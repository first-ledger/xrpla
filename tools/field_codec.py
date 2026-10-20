"""XRPL binary field parsing and reversible field columns, shared across
transactions, metadata and object patches.

The library under the XRPH history codec (history_codec.py) and its transaction
columns (transaction_columns.py); core/src/wire.cpp follows the same rules.
Every block is self-contained. Unknown types fall back to the original record.
No transaction execution or inferred state.
"""
from collections import Counter
import hashlib
import struct

from delta_codec import Reader, split_v2

MAX = 256 * 1024**2
MAX_RECORD = 16 * 1024**2
FIXED = {1:2, 2:4, 3:8, 4:16, 5:32, 9:12, 10:4, 11:8,
         16:1, 17:20, 20:12, 21:24, 22:48, 23:64, 26:20}


def vu(n):
    if not 0 <= n < 1 << 129: raise ValueError('Integer out of bounds')
    out = bytearray()
    while n >= 128: out.append((n & 127) | 128); n >>= 7
    out.append(n)
    return bytes(out)


class R(Reader):
    def v(self, limit=MAX):
        n = 0
        for i in range(19):
            b = self.take(1)[0]; n |= (b & 127) << (7*i)
            if not b & 128:
                if n > limit or (i and b == 0): raise ValueError('Invalid integer')
                return n
        raise ValueError('Oversized integer')
    def blob(self): return self.take(self.v())


def blob(value): return vu(len(value)) + value


def fields(data):
    """Return exact literal prefixes plus typed payloads; raises on unknown type.

    Types follow xrpld 3.3.0 SField/STAmount serialization. This tokenizer is
    deliberately not a protocol-validity checker; authenticated SDK validation
    and exact byte round trips establish the benchmark's source identity.
    """
    r = R(data); result = []; stack = []
    def emit(literal, family='', width=0, value=b'', numeric=False):
        result.append((literal, family, width, value, numeric))
    def length():
        start = r.pos; a = r.take(1)[0]
        if a <= 192: n = a
        elif a <= 240: n = 193 + (a-193)*256 + r.take(1)[0]
        elif a <= 254:
            b,c = r.take(2); n = 12481 + (a-241)*65536 + b*256 + c
        else: raise ValueError('Invalid VL prefix')
        return data[start:r.pos], n
    while r.pos < len(data):
        start = r.pos; h = r.take(1)[0]; t, f = h >> 4, h & 15
        if not t: t = r.take(1)[0]
        if not f: f = r.take(1)[0]
        if not t or not f: raise ValueError('Invalid field ID')
        literal = data[start:r.pos]
        element = bool(stack) and stack[-1] == 15  # an array's element is an object whatever field names it (core/src/wire.cpp)
        if t in (14,15) or element:
            if t in (14,15) and f == 1:
                if not stack or stack.pop() != t: raise ValueError('Unbalanced container')
            else:
                stack.append(14 if element else t)
                if len(stack) > 64: raise ValueError('Nesting limit')
            emit(literal); continue
        if t in FIXED:
            w = FIXED[t]; numeric = t in (1,2,3,10,11,16)
            family = f'number:{t}:{f}' if numeric else {5:'hash256',26:'currency'}.get(t,f'type:{t}')
            emit(literal, family, w, r.take(w), numeric)
        elif t in (7,8,19):
            prefix,n = length(); value = r.take(n)
            if t == 8 and n == 20:
                emit(literal+prefix, 'account',20,value)
            elif t == 19 and n % 32 == 0:
                emit(literal+prefix)
                for i in range(0,n,32): emit(b'', 'hash256',32,value[i:i+32])
            else: emit(literal, f'vl:{t}:{f}',0,prefix+value)
        elif t == 6:
            amount = r.take(8)
            if amount[0] & 128:
                emit(literal,f'amount:{f}',8,amount,True)
                emit(b'','currency',20,r.take(20)); emit(b'','account',20,r.take(20))
            elif amount[0] in (0x60, 0x20):  # MPT; 2013 XRP amounts can have the 0x20 bit (core/src/wire.cpp)
                emit(literal,f'mpt-amount:{f}',9,amount+r.take(1))
                emit(b'','mpt-id',24,r.take(24))
            else: emit(literal,f'amount:{f}',8,amount,True)
        elif t == 18:
            emit(literal)
            while True:
                flag = r.take(1); emit(flag)
                if flag == b'\x00': break
                if flag == b'\xff': continue
                if flag[0] & ~0x31 or not flag[0]: raise ValueError('Unknown path flag')
                for mask,family in ((1,'account'),(16,'currency'),(32,'account')):
                    if flag[0] & mask: emit(b'',family,20,r.take(20))
        else: raise ValueError(f'Unsupported type {t}')
    if stack: raise ValueError('Missing container end')
    return result


def pack_values(values, width):
    return b''.join(values) if width else b''.join(blob(x) for x in values)


def pack_column(values, width, numeric, sharing):
    candidates = [(0, pack_values(values,width))]
    if numeric:
        out = bytearray(); last = 0
        for value in values:
            n = int.from_bytes(value,'big'); d = n-last; last=n
            out.extend(vu(2*d if d >= 0 else -2*d-1))
        candidates.append((2,bytes(out)))
    if sharing:
        ids = {}; dictionary = []; indexes = bytearray()
        for value in values:
            if value not in ids: ids[value]=len(ids); dictionary.append(value)
            indexes.extend(vu(ids[value]))
        candidates.append((1,vu(len(dictionary))+pack_values(dictionary,width)+indexes))
    # A deliberately simple initial heuristic. Benchmark compressed outcomes;
    # smaller transformed bytes do not imply a smaller final zstd frame.
    mode,payload = min(candidates,key=lambda x:len(x[1]))
    return bytes([mode])+vu(len(values))+blob(payload), mode


def unpack_column(data, width, budget):
    r = R(data); mode = r.take(1)[0]; count = r.v(); q = R(r.blob()); r.end()
    if count > MAX or (width and count*width > budget): raise ValueError('Column expansion limit')
    def value(): return q.take(width) if width else q.blob()
    result = []; used=0
    if mode == 0:
        for _ in range(count):
            v=value(); used+=len(v)
            if used>budget: raise ValueError('Column expansion limit')
            result.append(v)
    elif mode == 1:
        n=q.v()
        if n>count: raise ValueError('Dictionary limit')
        dictionary=[]
        for _ in range(n):
            v=value();used+=len(v)
            if used>budget: raise ValueError('Dictionary expansion limit')
            dictionary.append(v)
        used=0
        for _ in range(count):
            i=q.v()
            if i>=n: raise ValueError('Dictionary reference out of range')
            v=dictionary[i]; used+=len(v)
            if used>budget: raise ValueError('Column expansion limit')
            result.append(v)
    elif mode == 2:
        if not 1<=width<=8: raise ValueError('Invalid numeric width')
        last=0
        for _ in range(count):
            d=q.v((1<<65)-1);last+=d//2 if not d&1 else -(d//2)-1
            if not 0<=last<1<<(width*8): raise ValueError('Numeric overflow')
            result.append(last.to_bytes(width,'big'))
        used=count*width
    else: raise ValueError('Unknown column mode')
    q.end(); return result,used


def encode_block(rows, sharing=True):
    columns=[]; col_ids={}; descriptors=[]; desc_ids={}; control=bytearray();stats=Counter()
    def token(literal, family='', width=0, value=b'', numeric=False, role=0):
        col=0
        if family:
            # Ablation keeps roles separate; combined encoding shares them.
            key=(family,width,numeric,0 if sharing else role)
            if key not in col_ids:
                col_ids[key]=len(columns);columns.append([key,[],{}])
            col=col_ids[key]+1; column=columns[col-1];column[1].append(value)
            if sharing:
                old=column[2].get(value,0)
                if (role==2 and old&4) or (role==4 and old&2):
                    stats['metadata_patch_shared_occurrences']+=1
                    stats['metadata_patch_shared_payload_bytes']+=len(value)
                column[2][value]=old|role
        key=(literal,col)
        if key not in desc_ids: desc_ids[key]=len(descriptors);descriptors.append(key)
        return vu(desc_ids[key])
    def record(value,role):
        stats['records']+=1
        try: items=fields(value); stats['tokenized_records']+=1
        except ValueError:
            items=[(b'','opaque',0,value,False)] if value else []
            stats['literal_fallback_records']+=1;stats['literal_fallback_bytes']+=len(value)
        control.extend(vu(len(value))+vu(len(items)))
        for item in items: control.extend(token(*item,role=role))
    for seq,prefix,patches in rows:
        p=R(prefix); p.u32();header=p.record();n=p.u32()
        control.extend(struct.pack('<I',seq)+blob(header)+vu(n))
        for _ in range(n): record(p.record(),1);record(p.record(),2)
        p.end(); control.extend(vu(len(patches)))
        for key,value in patches:
            control.extend(token(b'','hash256',32,key,False,4));record(value,4)
    out=bytearray(b'XFC1'+vu(len(rows))+vu(len(descriptors)))
    for literal,col in descriptors:out.extend(blob(literal)+vu(col))
    out.extend(vu(len(columns)))
    for (family,width,numeric,role),values,_ in columns:
        packed,mode=pack_column(values,width,numeric,sharing)
        out.extend(vu(width)+blob(packed));stats[f'column_mode_{mode}']+=1
    out.extend(blob(control))
    stats['columns']=len(columns);stats['descriptors']=len(descriptors)
    if len(out)>MAX: raise ValueError('Encoded block too large')
    return bytes(out),dict(stats)


def decode_block(data, expected_size):
    if len(data)>MAX or not 0<expected_size<=MAX: raise ValueError('Block limit')
    r=R(data)
    if r.take(4)!=b'XFC1': raise ValueError('Invalid field block')
    count=r.v(256); n=r.v()
    if not count or n>len(data): raise ValueError('Descriptor limit')
    desc=[(r.blob(),r.v()) for _ in range(n)];n=r.v(65536)
    columns=[];positions=[];used=0
    for _ in range(n):
        width=r.v(64);values,size=unpack_column(r.blob(),width,expected_size-used)
        used+=size;columns.append(values);positions.append(0)
    q=R(r.blob());r.end()
    def token():
        i=q.v()
        if i>=len(desc): raise ValueError('Descriptor reference out of range')
        literal,col=desc[i]
        if not col:return literal
        col-=1
        if col>=len(columns) or positions[col]>=len(columns[col]): raise ValueError('Column reference out of range')
        value=columns[col][positions[col]];positions[col]+=1
        return literal+value
    def record():
        length=q.v(MAX_RECORD); nt=q.v()
        if nt>len(data) or nt>MAX_RECORD: raise ValueError('Record token limit')
        out=bytearray()
        for _ in range(nt):
            out.extend(token())
            if len(out)>length: raise ValueError('Record expansion limit')
        if len(out)!=length: raise ValueError('Record length differs')
        return struct.pack('<I',length)+out
    rows=[];total=0;prior=None
    for _ in range(count):
        seq=q.u32()
        if prior is not None and seq!=prior+1: raise ValueError('Noncontiguous sequence')
        prior=seq;header=q.blob();n=q.v(1000000)
        row=bytearray(struct.pack('<II',seq,len(header))+header+struct.pack('<I',n))
        for _ in range(n): row.extend(record());row.extend(record())
        n=q.v(1000000);row.extend(struct.pack('<I',n));last_key=None
        for _ in range(n):
            key=token()
            if len(key)!=32 or (last_key is not None and key<=last_key): raise ValueError('Invalid patch key')
            last_key=key;row.extend(key);row.extend(record())
        total+=len(row)
        if total>expected_size: raise ValueError('Block expansion limit')
        rows.append((seq,bytes(row)))
    q.end()
    if positions!=[len(x) for x in columns]: raise ValueError('Unused column values')
    if total+12*count!=expected_size: raise ValueError('Decoded size differs')
    return rows


def body_from_records(records):
    out=bytearray();table=bytearray()
    for seq,value in records:table.extend(struct.pack('<IQ',seq,len(out)));out.extend(value)
    return bytes(out+table)


HEADER=struct.Struct('<4sHIII32s')
ENTRY=struct.Struct('<IIIII32s')


def encode(body,count,zstd,block_ledgers=256,sharing=True,level=9):
    if not 1<=block_ledgers<=256: raise ValueError('Block size limit')
    rows=split_v2(body,count);blocks=[];stats=Counter()
    for at in range(0,count,block_ledgers):
        group=rows[at:at+block_ledgers]; raw,part=encode_block(group,sharing);stats.update(part)
        size=sum(len(prefix)+4+sum(36+len(v) for k,v in patches) for seq,prefix,patches in group)+12*len(group)
        frame=zstd.compress(raw,level);blocks.append((group[0][0],len(group),size,len(raw),frame))
    out=bytearray(HEADER.pack(b'XRPF',1,rows[0][0],count,len(blocks),hashlib.sha256(body).digest()))
    for first,n,size,rawsize,frame in blocks:
        out.extend(ENTRY.pack(first,n,size,rawsize,len(frame),hashlib.sha256(frame).digest()))
    for *_,frame in blocks:out.extend(frame)
    out.extend(hashlib.sha256(out).digest())
    stats['encoded_body_bytes']=sum(b[3] for b in blocks)
    return bytes(out),dict(stats)


def index(data):
    if len(data)>MAX or len(data)<HEADER.size+32: raise ValueError('Envelope size')
    if hashlib.sha256(data[:-32]).digest()!=data[-32:]: raise ValueError('Envelope digest differs')
    magic,version,first,count,n,digest=HEADER.unpack_from(data)
    if magic!=b'XRPF' or version!=1 or not 1<=count<=65536 or not 1<=n<=count: raise ValueError('Invalid envelope')
    start=HEADER.size+n*ENTRY.size;entries=[];seq=first
    if start>len(data)-32: raise ValueError('Truncated table')
    total=0
    for i in range(n):
        f,c,size,rawsize,length,sha=ENTRY.unpack_from(data,HEADER.size+i*ENTRY.size)
        if f!=seq or not 1<=c<=256 or not 0<size<=MAX or not 0<rawsize<=MAX or start+length>len(data)-32: raise ValueError('Invalid block entry')
        entries.append((f,c,size,rawsize,start,length,sha));seq+=c;start+=length;total+=size
    if seq!=first+count or start!=len(data)-32 or total>MAX: raise ValueError('Envelope coverage differs')
    return entries,digest


def restore(data,zstd,target=None):
    entries,digest=index(data);records=[]
    for first,count,size,rawsize,offset,length,sha in entries:
        if target is not None and not first<=target<first+count:continue
        frame=data[offset:offset+length]
        if hashlib.sha256(frame).digest()!=sha:raise ValueError('Block digest differs')
        raw=zstd.decompress(frame)
        if len(raw)!=rawsize:raise ValueError('Field body size differs')
        rows=decode_block(raw,size)
        if len(rows)!=count or rows[0][0]!=first:raise ValueError('Block coverage differs')
        if target is not None:return dict(rows)[target]
        records.extend(rows)
    if target is not None:raise ValueError('Ledger outside chunk')
    result=body_from_records(records)
    if hashlib.sha256(result).digest()!=digest:raise ValueError('Restored body digest differs')
    return result
