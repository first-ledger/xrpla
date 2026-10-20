#!/usr/bin/env python3
"""Shared helpers for the publisher, piece client and live publisher.

Ed25519 manifest signing and verification (libsodium), safe archive paths,
size- and SHA-256-checked object reads, durable (fsync'd) writes, canonical
JSON encoding, and the ledger-header check for rippled `ledger` results.

Also the v2 chunk archive's pack, load and read commands. Artifact integrity
is distinct from independently reconstructed ledger roots. Python stdlib,
libzstd and libsodium. No network access.
"""
import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED
import ctypes as c
import ctypes.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import time

from compression_probe import Zstd


def digest(data): return hashlib.sha256(data).hexdigest()
def encoded(value): return json.dumps(value, separators=(',', ':')).encode()
def stamp(): return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


class IntegrityError(ValueError):
    pass


def check_header(result, sequence, expected=None, complete=True):
    """Checks a rippled `ledger` result: validated, the right sequence, a canonical 118-byte
    header hashing to its ledger_hash (and to `expected` when given). With `complete`, also
    that it carries binary transactions and object diffs. Returns (hash, parent hash)."""
    if result.get('validated') is not True or int(result.get('ledger_index', 0)) != sequence:
        raise IntegrityError('Unvalidated or wrong ledger sequence')
    ledger = result['ledger']; raw = bytes.fromhex(ledger['ledger_data'])
    if len(raw) != 118 or int.from_bytes(raw[:4], 'big') != sequence:
        raise IntegrityError('Invalid canonical ledger header')
    digest = hashlib.sha512(b'LWR\0' + raw).digest()[:32].hex()
    if digest != result['ledger_hash'].lower() or (expected and digest != expected.lower()):
        raise IntegrityError('Canonical ledger hash differs from source or pinned anchor')
    if complete:
        if not isinstance(ledger.get('transactions'), list) or not isinstance(ledger.get('diff'), list):
            raise IntegrityError('Source omitted transactions or complete object diffs')
        keys = set()
        for item in ledger['diff']:
            key = bytes.fromhex(item['object_id']); bytes.fromhex(item['object'])
            if len(key) != 32 or key in keys: raise IntegrityError('Invalid or repeated object change')
            keys.add(key)
        for tx in ledger['transactions']:
            if not bytes.fromhex(tx.get('tx_blob', tx.get('tx', ''))) or not bytes.fromhex(tx['meta']):
                raise IntegrityError('Missing binary transaction or metadata')
    return digest, raw[12:44].hex()


def durable(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.part')
    with temporary.open('wb') as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


class Signer:
    def __init__(self):
        self.lib = c.CDLL(os.environ.get('XRPLA_SODIUM_LIBRARY') or ctypes.util.find_library('sodium'))
        if self.lib.sodium_init() < 0: raise ValueError('Cannot initialize libsodium')
        self.lib.crypto_sign_seed_keypair.argtypes = [c.c_void_p, c.c_void_p, c.c_void_p]
        self.lib.crypto_sign_detached.argtypes = [c.c_void_p, c.c_void_p, c.c_void_p, c.c_ulonglong, c.c_void_p]
        self.lib.crypto_sign_verify_detached.argtypes = [c.c_void_p, c.c_void_p, c.c_ulonglong, c.c_void_p]

    def verify(self, data, signature, key):
        if len(signature) != 64 or len(key) != 32 or self.lib.crypto_sign_verify_detached(signature, data, len(data), key):
            raise ValueError('Manifest signature differs from pinned publisher')

    def sign(self, data, seed_path):
        seed = seed_path.read_bytes()
        if len(seed) != 32: seed = bytes.fromhex(seed.decode().strip())
        if len(seed) != 32: raise ValueError('Invalid signing seed length')
        pk, sk, signature = c.create_string_buffer(32), c.create_string_buffer(64), c.create_string_buffer(64)
        if self.lib.crypto_sign_seed_keypair(pk, sk, seed) or self.lib.crypto_sign_detached(signature, None, data, len(data), sk):
            raise ValueError('Cannot sign manifest')
        c.memset(sk, 0, 64)
        return pk.raw, signature.raw


def safe(root, name):
    if not isinstance(name, str) or '\\' in name: raise ValueError('Unsafe archive path')
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()) or path == root.resolve(): raise ValueError('Unsafe archive path')
    return path


def checked(root, item):
    path = safe(root, item['object'])
    if path.stat().st_size != item['size']: raise ValueError('Object size mismatch')
    data = path.read_bytes()
    if digest(data) != item['sha256']: raise ValueError('Object checksum mismatch: ' + item['object'])
    return data


def authenticate(root, key_path):
    raw = (root / 'manifest.json').read_bytes()
    if len(raw) > 16*1024**2: raise ValueError('Manifest exceeds reader limit')
    key = key_path.read_bytes()
    if len(key) != 32: key = bytes.fromhex(key.decode().strip())
    sig_name = 'signatures/' + digest(raw) + '.sig'
    signature = (root / sig_name).read_bytes()
    Signer().verify(raw, signature, key)
    def unique(pairs):
        result = {}
        for k, v in pairs:
            if k in result: raise ValueError('Duplicate JSON field')
            result[k] = v
        return result
    manifest = json.loads(raw, object_pairs_hook=unique)
    if manifest['format_version'] != 2 or manifest['dicts'] or not manifest['chunks']:
        raise ValueError('Requires dictionary-free v2 archive')
    previous = manifest['chunks'][0]['first'] - 1
    for chunk in manifest['chunks']:
        if chunk['first'] != previous + 1 or not chunk['first'] <= chunk['last'] <= chunk['first'] + 65535:
            raise ValueError('Noncontiguous archive chunks')
        previous = chunk['last']
    if previous != manifest['tip']['seq']: raise ValueError('Archive tip differs')
    return manifest, raw, signature, key


def descriptors(root, manifest):
    items = list(manifest['dicts'])
    for chunk in manifest['chunks']:
        items.append(chunk)
        if 'time_index' in chunk: items.append(chunk['time_index'])
    for snapshot in manifest['snapshots']:
        items.append(snapshot['index'])
        index = json.loads(checked(root, snapshot['index']))
        items.extend(index['parts'])
    names = [item['object'] for item in items]
    if len(names) != len(set(names)): raise ValueError('Duplicate archive object')
    return items


def record(raw):
    if not raw or len(raw) > 16*1024**2: raise ValueError('Invalid record size')
    return struct.pack('<I', len(raw)) + raw


def convert(result, sequence):
    header_hash, parent = check_header(result, sequence, complete=False)
    doc = result['ledger']; header = bytes.fromhex(doc['ledger_data'])
    if not isinstance(doc['transactions'],list) or not isinstance(doc['diff'],list):
        raise ValueError('Complete binary transactions and diffs are required')
    transactions = {}
    for tx in doc['transactions']:
        blob = bytes.fromhex(tx.get('tx_blob', tx.get('tx', '')))
        meta = bytes.fromhex(tx['meta'])
        key = hashlib.sha512(b'TXN\0' + blob).digest()[:32]
        if key in transactions: raise ValueError('Duplicate transaction hash')
        transactions[key] = (blob, meta)
    out = bytearray(struct.pack('<I', sequence) + record(header) + struct.pack('<I', len(transactions)))
    for key in sorted(transactions):
        blob, meta = transactions[key]; out.extend(record(blob)); out.extend(record(meta))
    patches = sorted((bytes.fromhex(x['object_id']), bytes.fromhex(x['object'])) for x in doc['diff'])
    out.extend(struct.pack('<I', len(patches)))
    previous_key = None
    for key, obj in patches:
        if len(key)!=32 or key==previous_key: raise ValueError('Invalid or duplicate object key')
        if len(obj) > 16*1024**2: raise ValueError('Oversized object')
        out.extend(key); out.extend(struct.pack('<I', len(obj))); out.extend(obj)
        previous_key=key
    if len(out) > 64*1024**2: raise ValueError('Fast pack ledger exceeds 64 MiB bound')
    return out, header_hash, parent, int.from_bytes(header[112:116], 'big'), len(transactions), len(patches)


def pack_job(parameters):
    spool, output, first, last, level, reserve = parameters
    output = Path(output)
    if shutil.disk_usage(output).free < reserve: raise RuntimeError('Protected disk reserve reached')
    codec = Zstd(); db = sqlite3.connect(Path(spool).resolve().as_uri() + '?mode=ro', uri=True)
    db.execute('PRAGMA query_only=ON')
    body, offsets, times, chunks = bytearray(), [], bytearray(), []
    txs = patches = raw_bytes = 0; first_parent = prior = None; start = first
    def flush(end):
        nonlocal body, offsets, times, start, raw_bytes
        table = len(body)
        for i, offset in enumerate(offsets): body.extend(struct.pack('<IQ', start + i, offset))
        raw_bytes += len(body)
        data = b'XRPA' + struct.pack('<HIII', 2, start, end, len(offsets)) + bytes(33) + codec.compress(bytes(body), level)
        data += struct.pack('<Q', table) + hashlib.sha256(data).digest()
        name = f'chunks/{start}-{end}.{digest(data)[:16]}.xrpa'
        time_name = f'times/{start}-{end}.{digest(times)[:16]}.xrpt'
        durable(output / name, data); durable(output / time_name, times)
        chunks.append({'object': name, 'size': len(data), 'sha256': digest(data), 'first': start, 'last': end,
                       'ledger_hash_of_last': prior, 'amendments': [],
                       'time_index': {'object': time_name, 'size': len(times), 'sha256': digest(times)},
                       'first_close_time': int.from_bytes(times[4:8], 'big'),
                       'last_close_time': int.from_bytes(times[-4:], 'big')})
        body, offsets, times = bytearray(), [], bytearray(); start = end + 1
    expected = first
    try:
        for seq, blob, count_tx, count_patch in db.execute('SELECT seq,payload,txs,patches FROM ledgers WHERE seq BETWEEN ? AND ? ORDER BY seq', (first,last)):
            if seq != expected: raise ValueError('Missing captured ledger')
            data = json.loads(codec.decompress(blob))
            packed, hash_value, parent, close, nt, np = convert(data, seq)
            if nt != count_tx or np != count_patch: raise ValueError('Capture counters differ')
            if prior is not None and prior != parent: raise ValueError('Parent hash differs')
            if first_parent is None: first_parent = parent
            if body and len(body) + len(packed) > 64*1024**2: flush(seq-1)
            offsets.append(len(body)); body.extend(packed); times.extend(struct.pack('>II',seq,close))
            prior = hash_value; txs += nt; patches += np; expected += 1
        if expected != last + 1: raise ValueError('Capture range is incomplete')
        flush(last)
    finally: db.close()
    return {'first': first, 'last': last, 'parent': first_parent, 'tip': prior, 'chunks': chunks,
            'transactions': txs, 'patches': patches, 'raw_binary_bytes': raw_bytes}


def pack(args):
    import fcntl
    root = args.out.resolve(); root.mkdir(parents=True, exist_ok=True)
    lock = (root / 'pack.lock').open('a'); fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    capture = args.capture.resolve(); spool = capture / 'capture.sqlite'
    source = sqlite3.connect(spool.as_uri() + '?mode=ro', uri=True)
    identity = json.loads(source.execute("SELECT value FROM meta WHERE key='identity'").fetchone()[0])
    baseline = args.baseline.resolve()
    manifest, _, _, publisher = authenticate(baseline, args.publisher_key)
    snapshot = next(s for s in manifest['snapshots'] if s['seq'] == identity['first'] - 1)
    index = json.loads(checked(baseline, snapshot['index']))
    if hashlib.sha512(b'LWR\0' + bytes.fromhex(index['ledger_header'])).digest()[:32].hex() != identity['baseline_hash'].lower():
        raise ValueError('Snapshot baseline differs from capture')
    end = args.last or identity['last']
    if not identity['first'] <= end <= identity['last']: raise ValueError('Invalid ending ledger')
    receipt_identity = {'capture': identity, 'last': end, 'level': args.level, 'batch_ledgers': 256,
                        'publisher_sha256': digest(publisher), 'packer_sha256': digest(Path(__file__).read_bytes())}
    work = sqlite3.connect(root / 'pack.sqlite'); work.execute('PRAGMA synchronous=FULL')
    work.executescript('CREATE TABLE IF NOT EXISTS meta(id INTEGER PRIMARY KEY, json TEXT); CREATE TABLE IF NOT EXISTS jobs(first INTEGER PRIMARY KEY,json TEXT);')
    old = work.execute('SELECT json FROM meta WHERE id=1').fetchone()
    if old and json.loads(old[0]) != receipt_identity: raise ValueError('Pack destination identity differs')
    with work: work.execute('INSERT OR REPLACE INTO meta VALUES(1,?)',(json.dumps(receipt_identity),))
    for item in [snapshot['index'], *index['parts']]:
        checked(baseline, item)
        target = safe(root,item['object']); target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists(): checked(root,item)
        else: os.link(safe(baseline,item['object']),target)
    complete = {n:json.loads(value) for n,value in work.execute('SELECT first,json FROM jobs')}
    # Recheck every resumed output before treating its job as committed.
    for result in complete.values():
        for chunk in result['chunks']:
            checked(root,chunk); checked(root,chunk['time_index'])
    first = identity['first']; total = end-first+1
    initial = sum(v['last']-v['first']+1 for v in complete.values()); started = time.monotonic()
    tasks = iter((str(spool),str(root),n,min(n+255,end),args.level,args.reserve_gib*1024**3)
                 for n in range(first,end+1,256) if n not in complete)
    def progress(state, error=None):
        done = sum(v['last']-v['first']+1 for v in complete.values()); seconds = time.monotonic()-started
        value = {'format':'xrpla-fast-pack-progress-v1','state':state,'updated_utc':stamp(),'first':first,'last':end,
                 'target':total,'packed':done,'session_ledgers':done-initial,'seconds':seconds,
                 'ledgers_per_second':(done-initial)/max(.001,seconds),'workers':args.workers,'compression_level':args.level,
                 'chunk_bytes':sum(c['size'] for v in complete.values() for c in v['chunks']),
                 'snapshot_bytes':snapshot['total_size'],'reserve_disk_bytes':args.reserve_gib*1024**3,
                 'free_disk_bytes':shutil.disk_usage(root).free,
                 'error':error,'integrity':'signed hashes and checked header chain; historical state roots not replayed'}
        durable(root/'progress.json',encoded(value)); return value
    try:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            pending = {}
            def fill():
                while len(pending) < args.workers*2:
                    task = next(tasks,None)
                    if task is None: break
                    pending[pool.submit(pack_job,task)] = task[2]
            fill(); progress('packing')
            while pending:
                finished,_ = wait(pending,timeout=2,return_when=FIRST_COMPLETED)
                for future in finished:
                    n = pending.pop(future); result = future.result()
                    with work: work.execute('INSERT INTO jobs VALUES(?,?)',(n,json.dumps(result)))
                    complete[n] = result
                fill(); progress('packing')
        ordered = [complete[n] for n in sorted(complete)]
        previous = identity['baseline_hash'].lower(); expected = first
        for result in ordered:
            if result['first'] != expected or result['parent'] != previous: raise ValueError('Cross-chunk chain mismatch')
            previous = result['tip']; expected = result['last']+1
        if expected != end+1: raise ValueError('Incomplete archive')
        if end == identity['last'] and previous != identity['tip_hash'].lower(): raise ValueError('Final anchor differs')
        document = {'format_version':2,'chunk_width':65536,'dicts':[],'chunks':[c for v in ordered for c in v['chunks']],
                    'snapshots':[snapshot],'published_at':stamp(),'tip':{'seq':end,'ledger_hash':previous},
                    'signature_format':'sha256-path-v1','integrity':{'mode':'source-attested-objects',
                    'header_chain_checked':True,'historical_state_roots_recomputed':False,
                    'transaction_roots_recomputed':False,'baseline_manifest_sha256':digest((baseline/'manifest.json').read_bytes())}}
        raw = encoded(document)
        if len(raw) > 16*1024**2: raise ValueError('Manifest exceeds reader limit')
        pk,signature = Signer().sign(raw,args.signing_key)
        if pk != publisher: raise ValueError('Signing key differs from pinned publisher')
        durable(root/'publisher.pub',pk); durable(root/'signatures'/(digest(raw)+'.sig'),signature)
        durable(root/'manifest.json',raw); durable(root/'manifest.json.sig',signature)
        status = progress('complete')
        status.update({'manifest_sha256':digest(raw),'transactions':sum(v['transactions'] for v in ordered),
                       'patches':sum(v['patches'] for v in ordered),'raw_binary_bytes':sum(v['raw_binary_bytes'] for v in ordered)})
        items = descriptors(root,document)
        status['retained_archive_bytes'] = sum(x['size'] for x in items)+len(raw)+32+64
        status['allocated_archive_bytes'] = sum(safe(root,x['object']).stat().st_blocks*512 for x in items) + sum((root/p).stat().st_blocks*512 for p in ('manifest.json','publisher.pub','signatures/'+digest(raw)+'.sig'))
        durable(root/'receipt.json',encoded(status)); print(json.dumps(status))
    except BaseException as error:
        progress('failed',str(error)); raise
    finally: work.close();source.close();lock.close()


def load(args):
    import fcntl
    root = args.archive.resolve(); output = args.out.resolve(); output.mkdir(parents=True,exist_ok=True)
    lock = (output/'load.lock').open('a'); fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    started = time.monotonic()
    manifest,raw,signature,key = authenticate(root,args.publisher_key)
    if (output/'manifest.json').exists() and (output/'manifest.json').read_bytes()!=raw: raise ValueError('Destination already contains a different archive')
    items = descriptors(root,manifest)
    def copy(item):
        data = checked(root,item)
        target = safe(output,item['object'])
        if target.exists(): checked(output,item)
        else: durable(target,data)
        return len(data)
    with ThreadPoolExecutor(max_workers=args.workers) as pool: size = sum(pool.map(copy,items))
    durable(output/'publisher.pub',key); durable(output/'signatures'/(digest(raw)+'.sig'),signature)
    durable(output/'manifest.json',raw)
    seconds = time.monotonic()-started; count = manifest['tip']['seq']-manifest['chunks'][0]['first']+1
    result = {'format':'xrpla-authenticated-load-v1','utc':stamp(),'ledgers':count,'seconds':seconds,
              'ledgers_per_second':count/seconds,'bytes':size+len(raw)+32+64,'workers':args.workers,
              'manifest_sha256':digest(raw),'integrity':'pinned publisher signature and every object SHA-256',
              'state_roots_recomputed':False,'query_support':'binary ledger/transaction/patch records by ledger number; no account index or materialized historical state'}
    durable(output/'load-receipt.json',encoded(result)); print(json.dumps(result));lock.close()


def read_ledger(root, key, sequence):
    manifest,_,_,_ = authenticate(root,key)
    chunk = next((x for x in manifest['chunks'] if x['first'] <= sequence <= x['last']),None)
    if chunk is None: raise ValueError('Ledger is outside archive')
    data = checked(root,chunk)
    if data[:6] != b'XRPA\x02\x00' or hashlib.sha256(data[:-40]).digest()!=data[-32:]: raise ValueError('Invalid chunk container')
    body = Zstd().decompress(data[51:-40]); table = struct.unpack_from('<Q',data,len(data)-40)[0]
    count=chunk['last']-chunk['first']+1
    if table+12*count!=len(body): raise ValueError('Invalid offset table')
    seq,pos=struct.unpack_from('<IQ',body,table+12*(sequence-chunk['first']))
    if seq!=sequence or pos>=table: raise ValueError('Invalid ledger offset')
    end=struct.unpack_from('<IQ',body,table+12*(sequence-chunk['first']+1))[1] if sequence<chunk['last'] else table
    def number():
        nonlocal pos
        if pos+4>end: raise ValueError('Truncated record')
        n=struct.unpack_from('<I',body,pos)[0];pos+=4;return n
    def take(n):
        nonlocal pos
        if n>16*1024**2 or pos+n>end: raise ValueError('Invalid record length')
        result=body[pos:pos+n];pos+=n;return result
    if number()!=sequence: raise ValueError('Wrong ledger sequence')
    header=take(number()); transactions=[];patches=[]
    n=number()
    if n>1000000: raise ValueError('Too many transactions')
    for _ in range(n): transactions.append({'tx_blob':take(number()).hex(),'meta':take(number()).hex()})
    n=number()
    if n>1000000: raise ValueError('Too many patches')
    for _ in range(n): patches.append({'object_id':take(32).hex(),'object':take(number()).hex()})
    if pos!=end: raise ValueError('Trailing ledger data')
    return {'ledger_index':sequence,'ledger_hash':hashlib.sha512(b'LWR\0'+header).digest()[:32].hex(),
            'integrity':manifest.get('integrity',{'mode':'publisher-attested'}),
            'ledger':{'ledger_data':header.hex(),'transactions':transactions,'diff':patches}}


def main():
    parser=argparse.ArgumentParser(description=__doc__); commands=parser.add_subparsers(dest='command',required=True)
    p=commands.add_parser('pack')
    for name in ('capture','baseline','publisher-key','signing-key','out'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--workers',type=int,default=4);p.add_argument('--level',type=int,default=3)
    p.add_argument('--reserve-gib',type=float,default=50);p.add_argument('--last',type=int)
    p=commands.add_parser('load')
    for name in ('archive','publisher-key','out'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--workers',type=int,default=4)
    p=commands.add_parser('read')
    for name in ('archive','publisher-key'):p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--ledger',type=int,required=True)
    args=parser.parse_args()
    if hasattr(args,'workers') and not 1<=args.workers<=16:parser.error('workers must be 1..16')
    if args.command=='pack':
        if not 1<=args.level<=19 or args.reserve_gib<1:parser.error('Invalid compression or disk reserve')
        pack(args)
    elif args.command=='load':load(args)
    else:print(json.dumps(read_ledger(args.archive.resolve(),args.publisher_key,args.ledger)))


if __name__=='__main__':main()
