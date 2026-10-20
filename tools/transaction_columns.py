"""Exact transaction-page columns for the experimental XRPH v3 envelope.

Independent of history/hash recipes, so transaction-hash references cannot cycle.
Signatures and unrecognized records are retained literally.
"""
from collections import Counter
import struct
from field_codec import R,blob,vu,fields,pack_column,unpack_column

LIMIT=128*1024**2
RECORD=16*1024**2


def encode(raw):
    r=R(raw);n=r.v(256);headers=bytearray(vu(n));layout=bytearray()
    shapes=[];shape_ids={};columns=[];column_ids={};stats=Counter()
    for _ in range(n):
        seq=r.u32();header=r.blob();count=r.v(1_000_000)
        headers.extend(struct.pack('<I',seq)+blob(header)+vu(count))
        for _ in range(count):
            tx=r.blob();layout.extend(vu(len(tx)));stats['transactions']+=1
            try:tokens=fields(tx)
            except ValueError:
                layout.extend(vu(0)+blob(tx));stats['literal_fallbacks']+=1;continue
            shape=[]
            for literal,family,width,value,numeric in tokens:
                cid=0
                if family:
                    key=(family,width,numeric)
                    if key not in column_ids:
                        column_ids[key]=len(columns);columns.append((width,numeric,[]))
                    at=column_ids[key];columns[at][2].append(value);cid=at+1
                shape.append((literal,cid))
            shape=tuple(shape)
            if shape not in shape_ids:shape_ids[shape]=len(shapes);shapes.append(shape)
            layout.extend(vu(shape_ids[shape]+1))
    r.end();out=bytearray(b'XTC1'+blob(bytes(headers))+vu(len(shapes)))
    for shape in shapes:
        out.extend(vu(len(shape)))
        for literal,cid in shape:out.extend(blob(literal)+vu(cid))
    out.extend(blob(bytes(layout))+vu(len(columns)))
    for width,numeric,values in columns:
        payload,mode=pack_column(values,width,numeric,True)
        out.extend(vu(width)+blob(payload));stats['column_mode_'+str(mode)]+=1
    stats.update(shapes=len(shapes),columns=len(columns))
    if len(out)>LIMIT:raise ValueError('Transaction columns bound')
    return bytes(out),dict(stats)


def decode(data):
    if len(data)>LIMIT:raise ValueError('Transaction columns bound')
    r=R(data)
    if r.take(4)!=b'XTC1':raise ValueError('Transaction columns magic')
    headers=r.blob();shapes=[]
    for _ in range(r.v(min(1_000_000,len(data)))):
        shapes.append([(r.blob(),r.v(65536)) for _ in range(r.v(65536))])
    layout=R(r.blob());columns=[];used=0
    for _ in range(r.v(65536)):
        width=r.v(64);values,expanded=unpack_column(r.blob(),width,LIMIT-used)
        used+=expanded;columns.append(values)
    r.end();positions=[0]*len(columns);h=R(headers);n=h.v(256);out=bytearray(vu(n))
    for _ in range(n):
        seq=h.u32();header=h.blob();count=h.v(1_000_000)
        out.extend(struct.pack('<I',seq)+blob(header)+vu(count))
        for _ in range(count):
            size=layout.v(RECORD);sid=layout.v(len(shapes))
            if not sid:tx=layout.blob()
            else:
                tx=bytearray()
                for literal,cid in shapes[sid-1]:
                    tx.extend(literal)
                    if cid:
                        if cid>len(columns) or positions[cid-1]>=len(columns[cid-1]):raise ValueError('Transaction column reference')
                        tx.extend(columns[cid-1][positions[cid-1]]);positions[cid-1]+=1
                    if len(tx)>size:raise ValueError('Transaction expansion bound')
            if len(tx)!=size:raise ValueError('Transaction size differs')
            out.extend(blob(tx))
            if len(out)>LIMIT:raise ValueError('Transaction page expansion bound')
    h.end();layout.end()
    if positions!=list(map(len,columns)):raise ValueError('Unconsumed transaction column values')
    return bytes(out)
