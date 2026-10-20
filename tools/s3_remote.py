#!/usr/bin/env python3
"""Mirror a local publication bucket to R2 (or any S3-compatible store), and read it back.

The publisher's local bucket stays the source of truth. `push` uploads every new
immutable object (multipart above 64 MiB), reads each one back and checks its
SHA-256 against the catalog before uploading the signed catalog generation, and
moves `catalog/current.json` last, so remote readers only ever see complete,
verified generations. The pointer only moves forward: a push refuses a remote
generation that is newer or differs, and the pointer write is conditional on the
pointer read at the start, so two publishers on one prefix cannot overwrite each
other, and a remote publisher key that differs is refused. An object the remote already
holds under the same name is read back once (unless a push here recorded it) and replaced
when it differs, unless the generation the remote pointer names uses it: its readers would
find other bytes, so the push is refused (replacing a publication takes a fresh prefix).
Objects the generation does not use that are gone locally (reclaimed pieces, and the bundles
and snapshots of a publication this one replaced) are then deleted remotely, unless the
generation the remote pointer names still uses them, a pointer move left them less than their
grace period ago (the bucket's `.remote-retired` record, written before the move), the catalog
retired them less than their grace period ago, or a merge that has not finished copied them
(the bucket's `.merging` record). Every object a push reads back intact is recorded in the
bucket's `.pushed` file, so `reclaim` can delete local copies the remote is known to hold, and so
is every pointer a push moves the remote to. These records live in the local bucket alone: a push
refuses a remote pointer that no push from this bucket recorded (a publisher moved to another
machine without them, or another publisher on the prefix), unless told to adopt the remote as it
is (`--adopt-remote`), which keeps every object the local catalog does not name through the
longest grace period. `push` holds the bucket's publisher lock, as the publishers and merges do.
Credentials come from the environment: XRPLA_S3_ENDPOINT,
XRPLA_S3_BUCKET, XRPLA_S3_ACCESS_KEY_ID, XRPLA_S3_SECRET_ACCESS_KEY,
XRPLA_S3_REGION (default "auto", as R2 expects) and optionally XRPLA_S3_PREFIX, which
places a whole publication under one key prefix so several can share a bucket.

Standard library only: SigV4 signing, multipart upload, ranged GET, listing.
"""
import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

PART = 64 * 1024 * 1024
COPY_PART = 1024 ** 3  # a server-side copy's part
STEP = 8 * 1024 * 1024
NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'
# In the local bucket: the objects read back from the remote and found intact, and the remote
# pointers pushes moved it to (each recorded before the move), as JSON lines.
PUSHED = '.pushed'
# PUSHED is compacted once it is larger than this and twice what it keeps (compact_pushed),
# keeping this many of its latest pointers.
PUSHED_COMPACT_BYTES = 16 * 1024 * 1024
PUSHED_POINTERS_KEPT = 16
# In the local bucket: the objects a merge (publication_merge.py) copies under this prefix before
# a catalog generation names them, with the prefix they come from and when the merge started, as
# JSON. A push keeps them remotely while it is there, and reports it.
MERGING = '.merging'
# In the local bucket: the objects a generation the remote pointer named depends on that the one
# a push moved it to does not, each with the time until which pushes keep it remotely (the move's
# time plus its grace period: readers that started from the generation before may still fetch
# it), as JSON. Written before the pointer moves, so a push that fails after the move, or one
# run again, keeps them as well.
REMOTE_RETIRED = '.remote-retired'
# The grace period of an object the pointer leaves that the catalog does not list as retired (a
# replaced publication's, or a piece the catalog retired so long ago that its entry is gone):
# the longest the catalog gives, and never less than this (piece_publisher's --grace-seconds
# default).
DEFAULT_GRACE = 7 * 86400
# The remote objects a push deletes when neither named nor held locally.
DATA_PREFIXES = ('pieces/', 'fragments/', 'bundles/', 'snapshots/')


class S3:
    def __init__(self, endpoint, bucket, access_key, secret_key, region='auto', part_size=PART, prefix='', jobs=16):
        self.jobs = jobs  # concurrent part/range transfers: 1 stream ~10 MB/s to R2, 16 ~56 MB/s up and ~99 MB/s down
        if prefix and not prefix.endswith('/'): prefix += '/'
        self.prefix = prefix; self.part_size = part_size; self.endpoint = endpoint.rstrip('/'); self.bucket = bucket; self.host = urllib.parse.urlparse(self.endpoint).netloc
        self.access_key, self.secret_key, self.region = access_key, secret_key, region
        # Parts held in memory at once across every upload (a push sends a few objects at a
        # time, each with up to `jobs` parts in flight): about 1 GiB at 16 parts of 64 MiB.
        self.parts = threading.BoundedSemaphore(jobs)

    @classmethod
    def from_environment(cls, prefix=None):
        """The store the XRPLA_S3_* variables name, under their prefix or, when given, `prefix`."""
        e = os.environ
        return cls(e['XRPLA_S3_ENDPOINT'], e['XRPLA_S3_BUCKET'], e['XRPLA_S3_ACCESS_KEY_ID'],
                   e['XRPLA_S3_SECRET_ACCESS_KEY'], e.get('XRPLA_S3_REGION', 'auto'), prefix=e.get('XRPLA_S3_PREFIX', '') if prefix is None else prefix)

    # Reads (GET, HEAD) are tried this often when the store or the connection fails part way (a
    # 5xx or 429, a dropped connection, a body cut short); writes are tried once, since some are
    # conditional or not repeatable, and their callers decide. A multipart upload's part PUTs
    # are repeatable (a part sent again replaces the one before), so they are tried as reads are:
    # one failing part must not end an upload of tens of GB.
    READ_ATTEMPTS = 5

    def request(self, method, key='', query=None, body=b'', headers=None, stream=False, repeatable=False):
        """One signed request (path-style URLs). Returns the response; raises on HTTP errors."""
        attempts = self.READ_ATTEMPTS if (method in ('GET', 'HEAD') or repeatable) and not stream else 1
        for attempt in range(attempts):
            try: return self.attempt(method, key, query, body, headers, stream)
            except urllib.error.HTTPError as e:
                if e.code < 500 and e.code != 429 or attempt + 1 == attempts: raise
            except (urllib.error.URLError, http.client.HTTPException, OSError):
                if attempt + 1 == attempts: raise
            time.sleep(min(2 ** attempt, 30))

    def attempt(self, method, key, query, body, headers, stream):
        query = sorted((query or {}).items())
        path = '/' + self.bucket + ('/' + urllib.parse.quote(self.prefix + key, safe='/~') if key else '')
        canonical_query = '&'.join(f'{urllib.parse.quote(k, safe="~")}={urllib.parse.quote(str(v), safe="~")}' for k, v in query)
        now = datetime.datetime.now(datetime.timezone.utc); stamp = now.strftime('%Y%m%dT%H%M%SZ'); day = stamp[:8]
        payload = hashlib.sha256(body).hexdigest()
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        headers.update({'host': self.host, 'x-amz-date': stamp, 'x-amz-content-sha256': payload})
        signed = ';'.join(sorted(headers))
        canonical = '\n'.join([method, path, canonical_query, ''.join(f'{k}:{str(headers[k]).strip()}\n' for k in sorted(headers)), signed, payload])
        scope = f'{day}/{self.region}/s3/aws4_request'
        to_sign = '\n'.join(['AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()])
        key_ = ('AWS4' + self.secret_key).encode()
        for part in (day, self.region, 's3', 'aws4_request'): key_ = hmac.new(key_, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key_, to_sign.encode(), hashlib.sha256).hexdigest()
        headers['authorization'] = f'AWS4-HMAC-SHA256 Credential={self.access_key}/{scope}, SignedHeaders={signed}, Signature={signature}'
        url = self.endpoint + path + ('?' + canonical_query if canonical_query else '')
        req = urllib.request.Request(url, data=body if method in ('PUT', 'POST') else None, method=method, headers=headers)
        response = urllib.request.urlopen(req, timeout=300)
        return response if stream else (response.status, dict(response.headers), response.read())

    def exists(self, key):
        try: self.request('HEAD', key); return True
        except urllib.error.HTTPError as e:
            if e.code == 404: return False
            raise

    def get(self, key, start=None, end=None):
        headers = {'range': f'bytes={start}-{end}'} if start is not None else None
        return self.request('GET', key, headers=headers)[2]

    def put(self, key, data, if_none_match=False, if_match=None):
        headers = {'content-type': 'application/octet-stream'}
        if if_none_match: headers['if-none-match'] = '*'
        if if_match: headers['if-match'] = if_match
        return self.request('PUT', key, body=data, headers=headers)[1].get('ETag')

    def put_file(self, key, path):
        """Upload a file: single PUT when small, multipart otherwise (aborted on failure)."""
        size = os.path.getsize(path)
        if size <= self.part_size: return self.put(key, Path(path).read_bytes())
        upload = ET.fromstring(self.request('POST', key, {'uploads': ''})[2]).find(NS + 'UploadId').text
        try:
            def part(number):
                with self.parts:
                    with open(path, 'rb') as f:
                        f.seek((number - 1) * self.part_size); data = f.read(self.part_size)
                    return number, self.request('PUT', key, {'partNumber': number, 'uploadId': upload}, body=data, repeatable=True)[1]['ETag']
            count = -(-size // self.part_size)
            parts = list(self.window(part, range(1, count + 1)))
            xml = '<CompleteMultipartUpload>' + ''.join(f'<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>' for n, e in parts) + '</CompleteMultipartUpload>'
            self.request('POST', key, {'uploadId': upload}, body=xml.encode())
        except BaseException:
            try: self.request('DELETE', key, {'uploadId': upload})
            finally: raise

    def copy_from(self, source_prefix, key, size):
        """Copy `key` (`size` bytes) from another prefix of this bucket to this one, server side:
        one CopyObject when it fits a copy part, else a multipart upload of part copies (a
        single CopyObject stops at 5 GiB)."""
        source = '/' + self.bucket + '/' + urllib.parse.quote(source_prefix + key, safe='/~')
        if size <= COPY_PART: self.request('PUT', key, headers={'x-amz-copy-source': source}); return
        upload = ET.fromstring(self.request('POST', key, {'uploads': ''})[2]).find(NS + 'UploadId').text
        try:
            def part(number):
                start = (number - 1) * COPY_PART
                body = self.request('PUT', key, {'partNumber': number, 'uploadId': upload}, repeatable=True,
                                    headers={'x-amz-copy-source': source, 'x-amz-copy-source-range': f'bytes={start}-{min(start + COPY_PART, size) - 1}'})[2]
                return number, next(e.text for e in ET.fromstring(body).iter() if e.tag.endswith('ETag'))
            parts = list(self.window(part, range(1, -(-size // COPY_PART) + 1)))
            xml = '<CompleteMultipartUpload>' + ''.join(f'<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>' for n, e in parts) + '</CompleteMultipartUpload>'
            self.request('POST', key, {'uploadId': upload}, body=xml.encode())
        except BaseException:
            try: self.request('DELETE', key, {'uploadId': upload})
            finally: raise

    def window(self, work, items):
        """Run `work` on items with up to `jobs` in flight, yielding results in order (bounded memory)."""
        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            pending = deque()
            for item in items:
                pending.append(pool.submit(work, item))
                if len(pending) >= self.jobs: yield pending.popleft().result()
            while pending: yield pending.popleft().result()

    def ranges(self, key, size=None):
        """An object's bytes in order, fetched as concurrent ranged GETs (its size asked with a
        HEAD unless given, as the catalog gives it: one request fewer per object)."""
        if size is None: size = int(self.request('HEAD', key)[1]['Content-Length'])
        return self.window(lambda at: self.get(key, at, min(at + self.part_size, size) - 1), range(0, size, self.part_size))

    def sha256(self, key):
        """Read an object back in full and hash it."""
        digest = hashlib.sha256()
        for data in self.ranges(key): digest.update(data)
        return digest.hexdigest()

    def download(self, key, target, expected_sha256, expected_size=None):
        target = Path(target); target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + '.part'); digest = hashlib.sha256(); size = 0
        with temporary.open('wb') as out:
            for data in self.ranges(key, expected_size): digest.update(data); out.write(data); size += len(data)
            out.flush(); os.fsync(out.fileno())
        if digest.hexdigest() != expected_sha256: temporary.unlink(); raise ValueError('Object differs from catalog: ' + key)
        os.replace(temporary, target); return size

    def list(self, prefix=''):
        keys, token = [], None
        while True:
            query = {'list-type': 2, 'prefix': self.prefix + prefix}
            if token: query['continuation-token'] = token
            root = ET.fromstring(self.request('GET', query=query)[2])
            keys += [c.find(NS + 'Key').text[len(self.prefix):] for c in root.findall(NS + 'Contents')]
            if root.findtext(NS + 'IsTruncated') != 'true': return keys
            token = root.findtext(NS + 'NextContinuationToken')

    def delete(self, key): self.request('DELETE', key)

    def abort_incomplete_uploads(self):
        """Abort multipart uploads left by an interrupted push (one publisher per prefix), every
        page of them, and only of a publication's own objects: with no prefix, the bucket may
        hold other uploads."""
        aborted = 0; marker = {}
        while True:
            root = ET.fromstring(self.request('GET', query={'uploads': '', 'prefix': self.prefix, **marker})[2])
            for upload in root.findall(NS + 'Upload'):
                key = upload.findtext(NS + 'Key')[len(self.prefix):]
                if not key.startswith(('pieces/', 'fragments/', 'bundles/', 'snapshots/', 'catalog/')): continue
                self.request('DELETE', key, {'uploadId': upload.findtext(NS + 'UploadId')}); aborted += 1
            if root.findtext(NS + 'IsTruncated') != 'true': return aborted
            following = {'key-marker': root.findtext(NS + 'NextKeyMarker') or '', 'upload-id-marker': root.findtext(NS + 'NextUploadIdMarker') or ''}
            # A truncated page whose markers do not move on would be asked for again forever.
            if following == marker or not any(following.values()): raise ValueError('The multipart upload listing does not advance')
            marker = following


def catalog_objects(bucket_root, catalog):
    """(name, sha256) of every immutable object a catalog generation depends on."""
    root = Path(bucket_root); objects = [('publisher.pub', hashlib.sha256((root / 'publisher.pub').read_bytes()).hexdigest())]
    for s in catalog['snapshots']:
        objects.append((s['index']['object'], s['index']['sha256']))
        objects += [(p['object'], p['sha256']) for p in json.loads((root / s['index']['object']).read_bytes())['parts']]
    objects += [(r['object'], r['sha256']) for r in catalog['ranges'] + catalog.get('tail', {}).get('ranges', [])]
    return objects


def remote_catalog_objects(s3, pointer, remote):
    """(name, sha256) of every object the remote generation `pointer` names depends on, read
    from the remote (its catalog and snapshot indexes; `remote`, the keys it holds): what its
    readers may still fetch. Nothing when its catalog is not there (no reader can use it)."""
    if pointer['object'] not in remote: return []
    catalog = json.loads(s3.get(pointer['object'])); objects = []
    for s in catalog['snapshots']:
        objects.append((s['index']['object'], s['index']['sha256']))
        if s['index']['object'] in remote: objects += [(p['object'], p['sha256']) for p in json.loads(s3.get(s['index']['object']))['parts']]
    objects += [(r['object'], r['sha256']) for r in catalog['ranges'] + catalog.get('tail', {}).get('ranges', [])]
    return objects


def verified_remotely(bucket_root):
    """{name: sha256} of the objects pushes read back from the remote and found intact."""
    path = Path(bucket_root) / PUSHED; verified = {}
    if path.exists():
        for line in path.read_bytes().decode('utf-8').splitlines():
            try: entry = json.loads(line)
            except ValueError: continue  # a line cut short by a crash
            if 'object' in entry: verified[entry['object']] = entry['sha256']
    return verified


def pushed_pointers(bucket_root):
    """The remote pointers pushes from this bucket recorded in PUSHED, as (generation, sha256)
    pairs: an empty set when PUSHED records objects alone (written before pushes recorded
    pointers), None when it records nothing (no push from this bucket)."""
    path = Path(bucket_root) / PUSHED; pointers = set(); recorded = False
    if path.exists():
        for line in path.read_bytes().decode('utf-8').splitlines():
            try: entry = json.loads(line)
            except ValueError: continue
            recorded = True
            if 'pointer' in entry: pointers.add((entry['pointer']['generation'], entry['pointer']['sha256']))
    return pointers if recorded else None


def compact_pushed(bucket_root, before, remote, limit=PUSHED_COMPACT_BYTES):
    """Rewrite PUSHED (durably) once it is larger than `limit` with what is read from it that is
    still of use: the last line for each object the remote holds (`remote`, the keys a push listed
    under the lock: an object gone remotely is uploaded again by a push or a merge whatever PUSHED
    records, and reclaim asks the remote whether it holds what PUSHED records, so a line for one is
    of no use to any reader of verified_remotely), and the remote pointer `before` with the last
    PUSHED_POINTERS_KEPT pointers recorded (pushed_pointers: a push asks only whether the remote
    pointer is among them, and it is `before`, or one recorded before a move that failed). It is
    rewritten only when that at least halves it, so one whose kept lines alone pass `limit` is not
    rewritten by every push, only once it has doubled again. A bucket whose PUSHED records objects
    alone keeps them alone. Under the bucket's publisher lock, which every writer of PUSHED holds."""
    path = Path(bucket_root) / PUSHED
    if not path.exists(): return
    size = path.stat().st_size
    if size <= limit: return
    objects = {}; pointers = {}
    for line in path.read_bytes().decode('utf-8').splitlines():
        try: entry = json.loads(line)
        except ValueError: continue
        if 'object' in entry: objects[entry['object']] = entry['sha256']
        elif 'pointer' in entry:
            pair = (entry['pointer']['generation'], entry['pointer']['sha256'])
            pointers.pop(pair, None); pointers[pair] = True  # in the order last recorded
    kept = list(pointers)[-PUSHED_POINTERS_KEPT:]
    if before and (before['generation'], before['sha256']) in pointers and (before['generation'], before['sha256']) not in kept:
        kept.insert(0, (before['generation'], before['sha256']))
    lines = [json.dumps({'object': name, 'sha256': sha}) for name, sha in sorted(objects.items()) if name in remote]
    lines += [json.dumps({'pointer': {'generation': generation, 'sha256': sha}}) for generation, sha in kept]
    data = ''.join(line + '\n' for line in lines).encode()
    if 2 * len(data) <= size: write_durably(path, data)


def check_recorded(bucket_root, before, adopt=False):
    """The pointers PUSHED records (pushed_pointers), once the remote pointer `before` is found
    to be one of them. What keeps objects readers may still use remotely (REMOTE_RETIRED, MERGING)
    is recorded in this bucket alone: a remote pointer no push from here recorded means it may
    lack them (a publisher moved without them, a bucket rebuilt) or another publisher uses the
    prefix, and deleting by its records would delete what they keep, so it is refused unless
    `adopt`. A bucket whose PUSHED records objects alone predates the pointer records and is taken
    at its word, but not while a merge is under way (MERGING): a merge records its copies in
    PUSHED too, after checking the remote pointer (publication_merge.py), so beside its record
    objects alone are no older push's."""
    pointers = pushed_pointers(bucket_root)
    older = pointers == set() and not (Path(bucket_root) / MERGING).exists()
    if before and not adopt and not older and (before['generation'], before['sha256']) not in (pointers or set()):
        raise ValueError(f"The remote pointer names generation {before['generation']}, which no push from this bucket recorded in {PUSHED}: "
                         f"copy {PUSHED}, {REMOTE_RETIRED} and {MERGING} from the bucket that pushed it (docs/PUBLICATION.md), or, if they are "
                         "lost, push with --adopt-remote, which keeps every remote object this catalog does not name through the longest grace period")
    return pointers


def record_pointer(bucket_root, pointer):
    """Record in PUSHED, synced, that a push moves the remote pointer to `pointer` (or found it
    there): written before the move, so a push that fails after it finds it recorded."""
    with open(Path(bucket_root) / PUSHED, 'a') as ledger:
        ledger.write(json.dumps({'pointer': {'generation': pointer['generation'], 'sha256': pointer['sha256']}}) + '\n')
        ledger.flush(); os.fsync(ledger.fileno())


def merging_record(bucket_root):
    """The record of a merge that has not finished (MERGING): the names it copies under this
    prefix (`objects`), the prefix they come from (`source`) and when it started (`started`, Unix
    seconds; a record written before these were kept has neither). None when no merge is under way."""
    path = Path(bucket_root) / MERGING
    return json.loads(path.read_bytes()) if path.exists() else None


def merging(bucket_root):
    """The names a merge that has not finished copies under this prefix (MERGING), or none."""
    record = merging_record(bucket_root)
    return set(record['objects']) if record else set()


def write_durably(path, data):
    """Replace a small local file with `data`, synced before and after its rename."""
    temporary = path.with_name(path.name + '.part')
    with temporary.open('wb') as stream: stream.write(data); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def remotely_retired(bucket_root):
    """The REMOTE_RETIRED record: {name: delete_after} of the objects pushes keep remotely after
    the pointer left the generations that named them."""
    path = Path(bucket_root) / REMOTE_RETIRED
    if not path.exists(): return {}
    return {e['object']: e['delete_after'] for e in json.loads(path.read_bytes())['objects']}


def retire_remotely(bucket_root, catalog, named, previous, generation, now):
    """Before the remote pointer moves from `generation` (whose objects are `previous`, name ->
    sha256) to `catalog` (whose objects are `named`), record (REMOTE_RETIRED) each object the
    former uses and the latter does not, kept until `now` plus its grace period: the one the
    catalog retired it with (piece_publisher's --grace-seconds, or --tip-grace-seconds for a
    small fragment), else the longest the catalog gives or DEFAULT_GRACE, whichever is longer (a
    catalog whose retired entries are all tip fragments gives only the short tip grace). An
    object whose bytes are not known (one adopted, see push) has None for its sha256. Recorded
    entries already past their time are dropped; one recorded again keeps the later time."""
    graces = {r['object']: r['delete_after'] - r['retired_at'] for r in catalog.get('retired', []) if 'retired_at' in r}
    longest = max([DEFAULT_GRACE, *graces.values()])
    path = Path(bucket_root) / REMOTE_RETIRED
    entries = {e['object']: e for e in json.loads(path.read_bytes())['objects']} if path.exists() else {}
    kept = {name: e for name, e in entries.items() if e['delete_after'] > now}
    for name, sha in previous.items():
        if name in named: continue
        delete_after = int(now + graces.get(name, longest))
        if kept.get(name, {}).get('delete_after', 0) < delete_after:
            kept[name] = {'object': name, 'sha256': sha, 'generation': generation, 'delete_after': delete_after}
    if kept != entries:
        write_durably(path, json.dumps({'objects': sorted(kept.values(), key=lambda e: e['object'])}, indent=1).encode())
    return {name: e['delete_after'] for name, e in kept.items()}


def reclaim(bucket_root, s3, objects):
    """Delete the local copies of `objects` ((name, sha256) pairs) the remote holds: read back
    intact by a push (or, when none recorded it, now), and still there. Anything the remote
    does not hold stays. Returns the files and bytes deleted."""
    root = Path(bucket_root); verified = verified_remotely(root); files = size = 0
    with open(root / PUSHED, 'a') as ledger:
        for name, sha in objects:
            path = root / name
            if not path.exists(): continue
            if verified.get(name) == sha:
                if not s3.exists(name): continue
            elif s3.exists(name) and s3.sha256(name) == sha:
                ledger.write(json.dumps({'object': name, 'sha256': sha}) + '\n'); ledger.flush()
            else: continue
            size += path.stat().st_size; path.unlink(); files += 1
    return files, size


def remote_object(s3, key):
    """An object's bytes and ETag, or (None, None) when the remote does not hold it."""
    try: _, headers, body = s3.request('GET', key)
    except urllib.error.HTTPError as e:
        if e.code == 404: return None, None
        raise
    return body, {k.lower(): v for k, v in headers.items()}.get('etag')


def remote_pointer(s3):
    """The remote catalog pointer and its ETag, or (None, None) before the first push."""
    body, etag = remote_object(s3, 'catalog/current.json')
    return (None, None) if body is None else (json.loads(body), etag)


def conditional_on(name, held, tag):
    """The ETag a write over the remote `name` (whose bytes `held` were read, None when absent) is
    made conditional on. An object read without one is refused: a write that cannot be made
    conditional would replace whatever another writer put there meanwhile, unnoticed."""
    if held is not None and not tag:
        raise ValueError(f'The remote {name} came back without an ETag, so it cannot be replaced only if unchanged: not pushing')
    return tag


def push(bucket_root, s3, adopt=False):
    """Upload and verify everything the current local generation needs, then publish it remotely.
    With `adopt`, a remote pointer no push from this bucket recorded is taken as it is: every
    remote object the local catalog does not name is kept through the longest grace period."""
    root = Path(bucket_root); pointer = json.loads((root / 'catalog/current.json').read_bytes())
    before, etag = remote_pointer(s3)
    if before and before['generation'] > pointer['generation']:
        raise ValueError(f"Remote generation {before['generation']} is newer than local {pointer['generation']}; is another publisher using this prefix?")
    if before and before['generation'] == pointer['generation'] and before['sha256'] != pointer['sha256']:
        raise ValueError(f"Remote generation {before['generation']} differs from the local one")
    # The pointer this push moves must be replaceable only if unchanged: refused before anything is written.
    if before != pointer: etag = conditional_on('catalog/current.json', before, etag)
    # A remote pointer no push from this bucket recorded is refused unless adopted (check_recorded).
    pointers = check_recorded(root, before, adopt)
    catalog = json.loads((root / pointer['object']).read_bytes())
    aborted = s3.abort_incomplete_uploads()
    remote = set(s3.list()); known = verified_remotely(root)
    key = hashlib.sha256((root / 'publisher.pub').read_bytes()).hexdigest()
    if 'publisher.pub' in remote and known.get('publisher.pub') != key and s3.sha256('publisher.pub') != key:
        raise ValueError('The remote publisher key differs from the local one; is another publisher using this prefix?')
    # What the generation the remote pointer names depends on: its readers may be fetching it now.
    previous = dict(remote_catalog_objects(s3, before, remote)) if before and before != pointer else {}
    objects = catalog_objects(root, catalog); named = {name for name, _ in objects}
    # Adopted: what the remote holds that this catalog does not name, whatever generation used it.
    # Recorded kept (REMOTE_RETIRED, synced) before the remote pointer is recorded as found by a
    # push from here: a push that stopped between the two would leave the next one, which no
    # longer needs to adopt, to delete them.
    adopted = {k for k in remote if k.startswith(DATA_PREFIXES) and k not in named} if adopt else set()
    if adopted: retire_remotely(root, catalog, named, dict.fromkeys(adopted), before and before['generation'], time.time())
    if before and (adopt or not pointers): record_pointer(root, before)
    # Compacted once the remote pointer is recorded in it, so it keeps it.
    compact_pushed(root, before, remote)
    ledger = open(root / PUSHED, 'a'); recording = threading.Lock(); uploaded = 0
    def upload(item):
        nonlocal uploaded
        name, sha = item
        # Named remotely, but no push here read it back (a replaced publication's, or pushed
        # before pushes kept a record): kept when intact, replaced otherwise. Never by bytes other
        # than those the remote generation names for it: its readers would find them differing.
        if name not in remote or s3.sha256(name) != sha:
            if previous.get(name, sha) != sha:
                raise ValueError(f"Remote generation {before['generation']} names {name} with other bytes than this publication's, and its "
                                 "readers may be fetching them: an object it uses is never replaced. To replace a publication, push it to a fresh prefix")
            s3.put_file(name, root / name)
            if s3.sha256(name) != sha: raise ValueError('Remote copy differs: ' + name)
            with recording: uploaded += 1
        with recording: ledger.write(json.dumps({'object': name, 'sha256': sha}) + '\n'); ledger.flush()
    todo = [item for item in objects if item[0] not in remote or known.get(item[0]) != item[1]]
    # A few objects at a time; each large object also moves its parts concurrently.
    try:
        with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(upload, todo))
    finally: ledger.close()
    verified = len(todo)
    # A generation the remote pointer has not reached may be there from a push that stopped
    # before moving it, signed differently since (the publisher signs an unreferenced generation
    # again): what the remote holds must be this one. The pointer is read again first: another
    # publisher's push that reached this generation meanwhile owns its objects, and replacing
    # them would leave its pointer naming bytes it did not sign.
    current, _ = remote_pointer(s3)
    if current != before and current and current['generation'] >= pointer['generation'] and current != pointer:
        raise ValueError('The remote catalog pointer changed during this push; is another publisher using this prefix?')
    # Each write is conditional on what was read just before it (none there, or that ETag), so one
    # racing another publisher's fails instead of replacing its files.
    for name in (pointer['object'] + '.sig', pointer['object']):
        data = (root / name).read_bytes()
        held, tag = remote_object(s3, name) if name in remote else (None, None)
        if held != data:
            tag = conditional_on(name, held, tag)
            try: s3.put(name, data, if_none_match=held is None, if_match=tag if held is not None else None)
            except urllib.error.HTTPError as e:
                if e.code == 412: raise ValueError(f'The remote {name} changed during this push; is another publisher using this prefix?') from e
                raise
            if hashlib.sha256(s3.get(name)).digest() != hashlib.sha256(data).digest(): raise ValueError('Remote copy differs: ' + name)
    # Readers follow the pointer, so it moves only after every dependency is verified remotely,
    # and only if nobody else moved it while this push ran. What the generation it leaves uses
    # and this one does not is recorded first, kept from now through its grace period: a reader
    # that started from that generation may still be fetching it, however long ago it left
    # locally (pushes that failed for days would otherwise delete it minutes after the move),
    # and a push that fails after the move, or one run again, still finds it recorded.
    now = time.time()
    if before != pointer or adopted:
        retire_remotely(root, catalog, named, {**dict.fromkeys(adopted), **previous}, before and before['generation'], now)
    if before != pointer:
        record_pointer(root, pointer)
        try: s3.put('catalog/current.json', (root / 'catalog/current.json').read_bytes(), if_none_match=etag is None, if_match=etag)
        except urllib.error.HTTPError as e:
            if e.code == 412: raise ValueError('The remote catalog pointer changed during this push; is another publisher using this prefix?') from e
            raise
    # Objects the current generation does not use and that are gone locally: reclaimed pieces, and
    # the bundles and snapshots of a publication this one replaced on the prefix. Those a pointer
    # move left stay through their grace period from the move (REMOTE_RETIRED). The pieces and
    # fragments the catalog retired stay through their grace period (readers of an older
    # generation may still use them), however soon they leave locally. So do the objects a merge
    # copied before signing the catalog that names them (MERGING: read after the listing, and a
    # merge writes it before its first copy, so whatever the listing saw of its copies is in it).
    live = named | set(previous) | {k for k, delete_after in remotely_retired(root).items() if delete_after > now}
    live |= {r['object'] for r in catalog.get('retired', []) if r['delete_after'] > now}
    record = merging_record(root); pending = set(record['objects']) if record else set(); live |= pending
    removed = [k for k in remote if k.startswith(DATA_PREFIXES) and k not in live and not (root / k).exists()]
    for k in removed: s3.delete(k)
    result = {'generation': catalog['generation'], 'uploaded': uploaded, 'verified': verified, 'removed': len(removed), 'aborted_uploads': aborted}
    if adopt: result['adopted'] = len(adopted)
    # Once the pushed generation names everything a merge copied, its record has done its work.
    # Until then each push reports it: a merge left unfinished keeps its copies remotely until it
    # is run again or abandoned (publication_merge.py --abandon).
    if pending and pending <= named: (root / MERGING).unlink(missing_ok=True)
    elif pending:
        result['merging'] = {'source': record.get('source'), 'objects': len(pending),
                             'age_seconds': int(now - record['started']) if 'started' in record else None}
        print(json.dumps({'warning': 'an unfinished merge keeps its copies remotely; run it again, or abandon it (publication_merge.py --abandon)',
                          **result['merging']}), file=sys.stderr)
    print(json.dumps(result)); return result


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('command', choices=('push',)); p.add_argument('--bucket', type=Path, required=True, help='local publication bucket')
    p.add_argument('--adopt-remote', action='store_true', help='take a remote pointer no push from this bucket recorded as it is (its records '
                   'lost): every remote object the local catalog does not name is kept through the longest grace period')
    a = p.parse_args()
    # The bucket's publisher lock, as live_publisher.py's pushes take it: a publish run, a merge or
    # another push holding it is not pushed over. (fcntl is imported here alone: the store's client
    # above, which readers use too, needs none.)
    import fcntl
    with open(a.bucket / '.publisher.lock', 'w') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise SystemExit('A publisher, merge or push holds this bucket\'s lock: push once it is done') from None
        push(a.bucket, S3.from_environment(), adopt=a.adopt_remote)


if __name__ == '__main__':
    main()
