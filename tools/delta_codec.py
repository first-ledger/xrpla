"""v2 chunk bodies: the record `Reader` and `split_v2`, which the publisher,
piece client, live publisher and history codec use to cut a body into
per-ledger rows (header and transactions, then ordered object patches).

Also a lossless block codec (local key dictionaries, at most 16 delta links per
object) that is not a published XRPLA format. Transactions and metadata stay
byte-identical. No XRPL state replay.
"""
import hashlib
import re
import struct

MAX_RECORD = 16 * 1024**2
MAX_BODY = 256 * 1024**2
HEADER = struct.Struct('<4sBBHHIII')
ENTRY = struct.Struct('<IIQQ')
NONZERO = re.compile(rb'[^\x00]+')


def varint(n):
    if n < 0 or n > 0xffffffff: raise ValueError('Integer out of bounds')
    out = bytearray()
    while n >= 128: out.append((n & 127) | 128); n >>= 7
    out.append(n); return bytes(out)


class Reader:
    def __init__(self, data): self.data, self.pos = data, 0
    def take(self, n):
        if n < 0 or self.pos+n > len(self.data): raise ValueError('Truncated record')
        result=self.data[self.pos:self.pos+n];self.pos+=n;return result
    def u32(self): return struct.unpack('<I',self.take(4))[0]
    def varint(self):
        result=0
        for i in range(5):
            b=self.take(1)[0];result|=(b&127) << (7*i)
            if not b&128:
                if result>0xffffffff or (i and b==0): raise ValueError('Invalid integer')
                return result
        raise ValueError('Oversized integer')
    def record(self):
        n=self.u32()
        if n>MAX_RECORD:raise ValueError('Oversized record')
        return self.take(n)
    def end(self):
        if self.pos!=len(self.data):raise ValueError('Trailing bytes')


def split_v2(body,count):
    if not 1<=count<=65536 or len(body)>MAX_BODY:raise ValueError('Invalid body limits')
    r=Reader(body);rows=[];offsets=[]
    for i in range(count):
        start=r.pos;seq=r.u32();r.record();nt=r.u32()
        if nt>1000000:raise ValueError('Transaction limit')
        for _ in range(nt):r.record();r.record()
        prefix=body[start:r.pos];n=r.u32()
        if n>1000000:raise ValueError('Patch limit')
        patches=[];previous=None
        for _ in range(n):
            key=r.take(32);value=r.record()
            if previous is not None and key<=previous:raise ValueError('Unordered keys')
            patches.append((key,value));previous=key
        if rows and seq!=rows[-1][0]+1:raise ValueError('Noncontiguous ledgers')
        offsets.append((seq,start));rows.append((seq,prefix,patches))
    for expected in offsets:
        if struct.unpack('<IQ',r.take(12))!=expected:raise ValueError('Offset mismatch')
    r.end();return rows


def delta(old,new):
    full=b'\x00'+varint(len(new))+new
    if not old or not new:return full
    if old==new:return b'\x03'
    candidates=[full]
    if len(old)==len(new):
        changed=(int.from_bytes(old,'little')^int.from_bytes(new,'little')).to_bytes(len(new),'little')
        runs=[(m.start(),m.end()) for m in NONZERO.finditer(changed)]
        prefix=runs[0][0];suffix=len(new)-runs[-1][1]
        sparse=bytearray(b'\x02'+varint(len(runs)));end=0
        for start,stop in runs:
            sparse.extend(varint(start-end));sparse.extend(varint(stop-start));sparse.extend(new[start:stop]);end=stop
        candidates.append(bytes(sparse))
    else:
        n=min(len(old),len(new))
        x=int.from_bytes(old[:n],'little')^int.from_bytes(new[:n],'little')
        prefix=n if x==0 else ((x&-x).bit_length()-1)//8
        n-=prefix
        if n:
            x=int.from_bytes(old[-n:],'big')^int.from_bytes(new[-n:],'big')
            suffix=n if x==0 else ((x&-x).bit_length()-1)//8
        else:suffix=0
    middle=new[prefix:len(new)-suffix if suffix else len(new)]
    candidates.append(b'\x01'+varint(prefix)+varint(suffix)+varint(len(middle))+middle)
    return min(candidates,key=len)


def apply_delta(r,old):
    tag=r.take(1)[0]
    if tag==0:
        n=r.varint()
        if n>MAX_RECORD:raise ValueError('Oversized full value')
        return r.take(n),False
    if not old:raise ValueError('Missing delta base')
    if tag==1:
        prefix,suffix,n=r.varint(),r.varint(),r.varint()
        if prefix+suffix>len(old) or prefix+suffix+n>MAX_RECORD:raise ValueError('Invalid splice')
        middle=r.take(n);return old[:prefix]+middle+(old[-suffix:] if suffix else b''),True
    if tag==2:
        value=bytearray(old);end=0;n=r.varint()
        if n>len(old):raise ValueError('Too many edits')
        for _ in range(n):
            start=end+r.varint();length=r.varint();end=start+length
            if not length or end>len(value):raise ValueError('Invalid edit')
            value[start:end]=r.take(length)
        return bytes(value),True
    if tag==3:return old,True
    raise ValueError('Unknown delta operation')


def encode_rows(rows,block_ledgers=32,max_chain=16,keys=True,deltas=True,inline_keys=False):
    if not 1<=block_ledgers<=256 or not 1<=max_chain<=16:raise ValueError('Invalid dependency bounds')
    blocks=[];stats={'patches':0,'full':0,'splice':0,'sparse':0,'same':0,'dictionary_keys':0,'max_chain_observed':0}
    for at in range(0,len(rows),block_ledgers):
        group=rows[at:at+block_ledgers]
        dictionary=sorted({key for _,_,patches in group for key,_ in patches}) if keys and not inline_keys else []
        key_ids={key:i for i,key in enumerate(dictionary)};stats['dictionary_keys']+=len(dictionary)
        out=bytearray(struct.pack('<I',len(dictionary))+b''.join(dictionary));previous={}
        for seq,prefix,patches in group:
            out.extend(struct.pack('<I',len(prefix)));out.extend(prefix);out.extend(struct.pack('<I',len(patches)))
            for key,value in patches:
                old,depth=previous.get(key,(None,0))
                change=delta(old,value) if deltas and old and depth<max_chain else b'\x00'+varint(len(value))+value
                if inline_keys:
                    if key not in key_ids:
                        key_ids[key]=len(key_ids);out.extend(b'\x00'+key);stats['dictionary_keys']+=1
                    else:out.extend(varint(key_ids[key]+1))
                else:out.extend(varint(key_ids[key]) if keys else key)
                out.extend(change)
                tag=change[0];depth=depth+1 if tag else 0
                if deltas:
                    if value:previous[key]=(value,depth)
                    else:previous.pop(key,None)
                stats['patches']+=1;stats[['full','splice','sparse','same'][tag]]+=1
                stats['max_chain_observed']=max(stats['max_chain_observed'],depth)
        blocks.append((group[0][0],len(group),bytes(out)))
    flags=int(keys)|int(deltas)<<1|int(inline_keys)<<2
    head=HEADER.pack(b'XBD1',1,flags,block_ledgers,max_chain,rows[0][0],len(rows),len(blocks))
    offset=len(head)+ENTRY.size*len(blocks);table=bytearray();bodies=bytearray()
    for first,count,data in blocks:
        table.extend(ENTRY.pack(first,count,offset,len(data)));bodies.extend(data);offset+=len(data)
    return head+table+bodies,stats


def index(data):
    if len(data)>MAX_BODY or len(data)<HEADER.size:raise ValueError('Body size limit')
    magic,version,flags,block_size,max_chain,first,count,n=HEADER.unpack_from(data)
    if magic!=b'XBD1' or version!=1 or flags&~7 or (flags&4 and not flags&1) or not 1<=block_size<=256 or not 1<=max_chain<=16 or not 1<=count<=65536:
        raise ValueError('Invalid format header')
    if n!=(count+block_size-1)//block_size:raise ValueError('Invalid block count')
    end=HEADER.size+n*ENTRY.size;seq=first;entries=[]
    for i in range(n):
        if HEADER.size+(i+1)*ENTRY.size>len(data):raise ValueError('Truncated block table')
        start,size,offset,length=ENTRY.unpack_from(data,HEADER.size+i*ENTRY.size)
        if start!=seq or size!=min(block_size,first+count-seq) or offset!=end or offset+length>len(data):
            raise ValueError('Invalid block offset')
        entries.append((start,size,offset,length));end=offset+length;seq+=size
    if end!=len(data):raise ValueError('Trailing block data')
    return flags,max_chain,entries


def decode_block(data,entry,flags,max_chain,target=None):
    first,count,offset,length=entry;r=Reader(data[offset:offset+length]);n=r.u32()
    if n>1000000 or ((not flags&1 or flags&4) and n):raise ValueError('Invalid key table')
    dictionary=[r.take(32) for _ in range(n)]
    if dictionary!=sorted(set(dictionary)):raise ValueError('Unordered dictionary')
    prior={};records=[];inline_seen=set()
    for seq in range(first,first+count):
        length=r.u32()
        if length>MAX_BODY:raise ValueError('Oversized ledger prefix')
        prefix=r.take(length)
        if len(prefix)<4 or struct.unpack_from('<I',prefix)[0]!=seq:raise ValueError('Invalid ledger prefix')
        n=r.u32()
        if n>1000000:raise ValueError('Patch count limit')
        record=bytearray(prefix+struct.pack('<I',n));previous_key=None
        for _ in range(n):
            if flags&4:
                identifier=r.varint()
                if identifier==0:
                    key=r.take(32)
                    if key in inline_seen:raise ValueError('Duplicate inline key')
                    inline_seen.add(key);dictionary.append(key)
                elif identifier>len(dictionary):raise ValueError('Invalid inline reference')
                else:key=dictionary[identifier-1]
            elif flags&1:
                identifier=r.varint()
                if identifier>=len(dictionary):raise ValueError('Invalid key reference')
                key=dictionary[identifier]
            else:key=r.take(32)
            if previous_key is not None and key<=previous_key:raise ValueError('Unordered patch')
            previous_key=key;old,depth=prior.get(key,(None,0));value,is_delta=apply_delta(r,old)
            if is_delta and (not flags&2 or depth>=max_chain):raise ValueError('Delta chain exceeds bound')
            if flags&2:
                if value:prior[key]=(value,depth+1 if is_delta else 0)
                else:prior.pop(key,None)
            record.extend(key);record.extend(struct.pack('<I',len(value)));record.extend(value)
        records.append((seq,bytes(record)))
        if seq==target:return records[-1][1]
    r.end();return records


def decode(data):
    flags,chain,entries=index(data);body=bytearray();table=bytearray()
    for entry in entries:
        for seq,record in decode_block(data,entry,flags,chain):
            table.extend(struct.pack('<IQ',seq,len(body)));body.extend(record)
            if len(body)>MAX_BODY:raise ValueError('Decoded body exceeds limit')
    return bytes(body+table)


def read_ledger(data,sequence):
    flags,chain,entries=index(data)
    for entry in entries:
        if entry[0]<=sequence<entry[0]+entry[1]:return decode_block(data,entry,flags,chain,sequence)
    raise ValueError('Ledger outside chunk')
