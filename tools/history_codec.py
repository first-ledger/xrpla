"""Experimental object histories + derived identities + segment dictionaries.

Canonical bytes are preserved, including unknown-record fallbacks. Dictionaries
span up to 4096 ledgers; each page starts with an empty object-field cache.
Hash recipes depend only on raw transactions, headers or dictionary literals,
never on metadata/state replay. This is not a production archive format.
"""
from bisect import bisect_right
from collections import Counter
import hashlib
import struct

from field_codec import FIXED,R,vu,blob,body_from_records
from delta_codec import split_v2

MAX_RECORD=16*1024**2
MAX_SECTION=128*1024**2
MAX_SEGMENT=512*1024**2
MAX_ITEMS=4_000_000
# Dictionary-reference arguments per hash recipe. 1 transaction hash; 2 AccountRoot,
# 3 Offer, 4 RippleState keys; 5 Ticket; 6 Check; 7 owner directory; 8 book
# directory; 9 directory page (root hash, then an unbounded page number); 10 ledger
# hash (offset of a ledger in this segment).
RECIPE_ARGS={1:1,2:1,3:2,4:3,5:2,6:2,7:1,8:5,9:1,10:1}
HEAD=struct.Struct('<4sHIIHH32s')
SECTION=struct.Struct('<II32s')
PAGE=struct.Struct('<IIII32s')


def half(data):return hashlib.sha512(data).digest()[:32]


def parse(data):
    """Exact tree of (header,type,field,payload-or-children,end-marker)."""
    r=R(data)
    def fields(end_type=None,depth=0):
        if depth>64:raise ValueError('Nesting bound')
        nodes=[]
        while r.pos<len(data):
            start=r.pos;b=r.take(1)[0];t,f=b>>4,b&15
            if not t:t=r.take(1)[0]
            if not f:f=r.take(1)[0]
            if not t or not f:raise ValueError('Invalid field')
            h=data[start:r.pos]
            if t in (14,15) and f==1:
                if t!=end_type:raise ValueError('Unexpected end')
                return nodes,h
            if t in (14,15) or end_type==15:  # an array's element is an object whatever field names it (core/src/wire.cpp)
                children,end=fields(14 if end_type==15 else t,depth+1);nodes.append((h,t,f,children,end));continue
            start=r.pos
            if t in FIXED:r.take(FIXED[t])
            elif t in (7,8,19):
                a=r.take(1)[0]
                if a<=192:n=a
                elif a<=240:n=193+(a-193)*256+r.take(1)[0]
                elif a<=254:
                    b,c=r.take(2);n=12481+(a-241)*65536+b*256+c
                else:raise ValueError('Invalid VL')
                r.take(n)
            elif t==6:
                a=r.take(8)[0]
                if a&128:r.take(40)
                elif a in (0x60,0x20):r.take(25)  # MPT; 2013 XRP amounts can have the 0x20 bit (core/src/wire.cpp)
            elif t==18:
                while True:
                    flag=r.take(1)[0]
                    if flag==0:break
                    if flag==255:continue
                    if flag&~0x31:raise ValueError('Invalid path')
                    for mask in (1,16,32):
                        if flag&mask:r.take(20)
            else:raise ValueError('Unsupported type')
            nodes.append((h,t,f,data[start:r.pos],None))
        if end_type is not None:raise ValueError('Missing end')
        return nodes,b''
    nodes,end=fields();r.end();return nodes


def serialize(node):
    h,t,f,value,end=node
    return h+(b''.join(map(serialize,value))+end if end is not None else value)


def raw_record(row):
    seq,prefix,patches=row
    return prefix+struct.pack('<I',len(patches))+b''.join(k+struct.pack('<I',len(v))+v for k,v in patches)


def original_body(rows):return body_from_records([(row[0],raw_record(row)) for row in rows])


class Pool:
    def __init__(self,recipes=True,extra=True):
        self.headers=[];self.hmap={};self.values=[];self.vmap={};self.atoms=[];self.amap={}
        self.shapes=[];self.smap={};self.hashes=[];self.hashmap={};self.recipes={};self.derive=recipes;self.stats=Counter()
        self.extra=recipes and extra;self.ledger_hashes={};self.tried=set()
    def intern(self,items,index,value):
        if value not in index:index[value]=len(items);items.append(value)
        return index[value]
    def header(self,h):return self.intern(self.headers,self.hmap,h)
    def value(self,v):return self.intern(self.values,self.vmap,v)
    def hash(self,h):
        if len(h)!=32:raise ValueError('Hash length')
        return self.intern(self.hashes,self.hashmap,h)
    def key_recipe(self,key,nodes):
        if not self.derive:return
        v={(t,f):value for h,t,f,value,end in nodes if end is None}
        kind=v.get((1,1));recipe=None;got=None
        if kind==b'\x00a' and len(v.get((8,1),b''))==21 and v[(8,1)][0]==20:
            account=v[(8,1)][1:];got=half(b'\x00a'+account);recipe=(2,self.value(account))
        elif kind==b'\x00o' and len(v.get((8,1),b''))==21 and v[(8,1)][0]==20 and len(v.get((2,4),b''))==4:
            account,seq=v[(8,1)][1:],v[(2,4)];got=half(b'\x00o'+account+seq);recipe=(3,self.value(account),self.value(seq))
        elif kind==b'\x00r':
            low,high=v.get((6,6),b''),v.get((6,7),b'')
            if len(low)==len(high)==48 and low[8:28]==high[8:28]:
                a,b=sorted((low[28:],high[28:]));currency=low[8:28]
                got=half(b'\x00r'+a+b+currency);recipe=(4,self.value(a),self.value(b),self.value(currency))
        if got==key:self.recipes[key]=recipe
        elif self.extra and key not in self.tried:self.extra_recipe(key,v,kind)
    def extra_recipe(self,key,v,kind):
        """Modes 5-9: Ticket, Check, owner/book/page directories. Accepted only on an exact match."""
        acct=lambda k:v[k][1:] if len(v.get(k,b''))==21 and v[k][0]==20 else None;recipe=None
        if kind==b'\x00T' and acct((8,1)) and len(v.get((2,41),b''))==4:
            if half(b'\x00T'+acct((8,1))+v[(2,41)])==key:recipe=(5,self.value(acct((8,1))),self.value(v[(2,41)]))
        elif kind==b'\x00C' and acct((8,1)) and len(v.get((2,4),b''))==4:
            if half(b'\x00C'+acct((8,1))+v[(2,4)])==key:recipe=(6,self.value(acct((8,1))),self.value(v[(2,4)]))
        elif kind==b'\x00d':
            owner=acct((8,2));root=v.get((5,8))
            # Metadata omits default values such as XRP's all-zero currency and issuer.
            book=[v.get((17,i),bytes(20)) for i in (1,3,2,4)]+[v.get((3,6))]
            if owner and half(b'\x00O'+owner)==key:recipe=(7,self.value(owner))
            elif (5,34) not in v and book[4] and len(book[4])==8 and half(b'\x00B'+b''.join(book[:4]))[:24]+book[4]==key:
                recipe=(8,*(self.value(x) for x in book))
            elif root and len(root)==32 and root!=key:
                hints=set(range(1,65))
                for f,d in (((3,2),1),((3,1),-1)):
                    if len(v.get(f,b''))==8:hints.add(int.from_bytes(v[f],'big')+d)
                for p in sorted(hints):
                    if 0<p<1<<64 and half(b'\x00d'+root+p.to_bytes(8,'big'))==key:recipe=(9,self.hash(root),p);break
                else:self.tried.add(key)
        if recipe:self.recipes[key]=recipe
    def atom(self,node):
        raw=serialize(node)
        if raw in self.amap:return self.amap[raw]
        h,t,f,value,end=node;hid=self.header(h)
        if end is not None:payload=(0,self.value(raw[len(h):]))
        elif t==5:payload=(1,self.hash(value))
        elif t==8 and len(value)==21 and value[0]==20:payload=(2,self.value(value[1:]))
        elif t==6 and len(value)==48:payload=(3,self.value(value[:8]),self.value(value[8:28]),self.value(value[28:]))
        elif t==19:
            prefix=1 if value[0]<=192 else 2 if value[0]<=240 else 3
            parts=value[prefix:]
            payload=(4,self.value(value[:prefix]),*(self.hash(parts[i:i+32]) for i in range(0,len(parts),32))) if len(parts)%32==0 else (0,self.value(value))
        else:payload=(0,self.value(value))
        i=len(self.atoms);self.amap[raw]=i;self.atoms.append((hid,*payload));return i
    def view(self,key,nodes,cache):
        ids=[self.atom(n) for n in nodes];shape=tuple(self.atoms[i][0] for i in ids)
        sid=self.intern(self.shapes,self.smap,shape);mask=bytearray((len(ids)+7)//8);changes=bytearray()
        if key not in cache:self.stats['object_page_anchors']+=1
        old=cache.setdefault(key,{})
        for at,(hid,i) in enumerate(zip(shape,ids)):
            if old.get(hid)!=i:
                mask[at//8]|=1<<(at%8);changes.extend(vu(i));old[hid]=i;self.stats['field_changes']+=1
            else:self.stats['unchanged_field_references']+=1
        self.stats['object_views']+=1
        return vu(sid)+mask+changes
    def node(self,node,cache,context=None):
        h,t,f,value,end=node
        if end is None:return b'\x00'+vu(self.atom(node))
        if t==14 and f in (3,4,5):
            keys=[n[3] for n in value if n[1:3]==(5,6) and n[4] is None]
            context=self.hash(keys[0]) if len(keys)==1 and len(keys[0])==32 else None
            types=[n for n in value if n[1:3]==(1,1) and n[4] is None]
            if context is not None and len(types)==1:
                for part in value:
                    if part[1:3] in ((14,7),(14,8)) and part[4] is not None:
                        self.key_recipe(keys[0],[types[0],*part[3]])
        if t==14 and f in (6,7,8) and context is not None:
            if f==8:cache.pop(context,None)
            self.stats['metadata_object_views']+=1
            return b'\x02'+vu(self.header(h))+blob(end)+vu(context)+bytes([f==8])+self.view(context,value,cache)
        return b'\x01'+vu(self.header(h))+blob(end)+vu(len(value))+b''.join(self.node(n,cache,context) for n in value)
    def metadata(self,raw,nodes,cache):
        if nodes is None:self.stats['metadata_literal_fallbacks']+=1;return b'\x00'+blob(raw)
        return b'\x01'+vu(len(nodes))+b''.join(self.node(n,cache) for n in nodes)
    def finish(self):
        if self.extra:
            for h in self.hashes:
                if h not in self.recipes and h in self.ledger_hashes:self.recipes[h]=(10,self.ledger_hashes[h])
        out=bytearray(b'XHD2'+vu(len(self.headers)))
        for h in self.headers:out.extend(blob(h))
        out.extend(vu(len(self.values)))
        for v in self.values:out.extend(blob(v))
        out.extend(vu(len(self.hashes)))
        for h in self.hashes:
            recipe=self.recipes.get(h) if self.derive else None
            if recipe:
                mode,*args=recipe;out.append(mode)
                for a in args:out.extend(vu(a))
                self.stats['hash_recipe_'+str(mode)]+=1
            else:out.extend(b'\x00'+h);self.stats['literal_hashes']+=1
        out.extend(vu(len(self.atoms)))
        for a in self.atoms:
            if a[1]==4:
                hid,kind,prefix,*hashes=a
                out.extend(vu(hid)+vu(kind)+vu(prefix)+vu(len(hashes)))
                for n in hashes:out.extend(vu(n))
                self.stats['hash_vector_atoms']+=1;self.stats['hash_vector_references']+=len(hashes)
            else:
                for n in a:out.extend(vu(n))
        out.extend(vu(len(self.shapes)))
        for shape in self.shapes:
            out.extend(vu(len(shape)))
            for h in shape:out.extend(vu(h))
        self.stats.update(headers=len(self.headers),values=len(self.values),atoms=len(self.atoms),hashes=len(self.hashes),shapes=len(self.shapes))
        return bytes(out)


def encode(rows,zstd,page_ledgers=64,recipes=True,level=19,transaction_columns=False,extra_recipes=True):
    """Level 19 is the measured publication setting (docs/internal/SIZE-AND-FOOTPRINT.md, E1)."""
    if not rows or len(rows)>4096 or not 1<=page_ledgers<=256:raise ValueError('Segment/page bound')
    if any(r[0]!=rows[0][0]+i for i,r in enumerate(rows)):raise ValueError('Noncontiguous segment')
    pool=Pool(recipes,extra_recipes);pages=[];raw_total=0;all_hasher=hashlib.sha256();table=bytearray();position=0;tx_total=0
    for start in range(0,len(rows),page_ledgers):
        group=rows[start:start+page_ledgers];tx_out=bytearray(vu(len(group)));control=bytearray(vu(len(group)));cache={};page_tx_first=tx_total
        page_records=[]
        for seq,prefix,patches in group:
            r=R(prefix);assert r.u32()==seq;header=r.record();n=r.u32();transactions=[];metas=[]
            if pool.extra:pool.ledger_hashes.setdefault(half(b'LWR\0'+header),seq-rows[0][0])
            tx_out.extend(struct.pack('<I',seq)+blob(header)+vu(n))
            for idx in range(n):
                tx,meta=r.record(),r.record();tx_out.extend(blob(tx));transactions.append(tx)
                if recipes:pool.recipes[half(b'TXN\0'+tx)]=(1,tx_total)
                tx_total+=1
                try:nodes=parse(meta)
                except ValueError:nodes=None
                if len(meta)<6 or meta[:2]!=b'\x20\x1c':raise ValueError('Missing canonical transaction index')
                metas.append((int.from_bytes(meta[2:6],'big'),idx,meta,nodes))
            r.end()
            if sorted(x[0] for x in metas)!=list(range(n)):raise ValueError('Invalid transaction execution order')
            control.extend(vu(n))
            for execution,idx,meta,nodes in sorted(metas):
                control.extend(vu(idx)+vu(len(meta))+pool.metadata(meta,nodes,cache))
            control.extend(vu(len(patches)))
            for key,value in patches:
                kid=pool.hash(key);control.extend(vu(kid)+vu(len(value)))
                if not value:cache.pop(kid,None);pool.stats['deletions']+=1;continue
                try:nodes=parse(value)
                except ValueError:nodes=None
                if nodes is None:
                    control.extend(b'\x00'+blob(value));pool.stats['object_literal_fallbacks']+=1
                else:
                    pool.key_recipe(key,nodes);control.extend(b'\x01'+pool.view(kid,nodes,cache));pool.stats['patch_object_views']+=1
            original=raw_record((seq,prefix,patches));page_records.append((seq,original));all_hasher.update(original)
            table.extend(struct.pack('<IQ',seq,position));position+=len(original)
        original=body_from_records(page_records);raw_total+=len(original)
        pages.append((group[0][0],len(group),page_tx_first,tx_total-page_tx_first,hashlib.sha256(original).digest(),bytes(tx_out),bytes(control)))
    all_hasher.update(table)
    if raw_total>MAX_SEGMENT:raise ValueError('Segment decoded bound')
    dictionary=pool.finish();sections=[dictionary]
    for first,count,txfirst,nt,digest,tx,control in pages:sections.extend([tx,control])
    if any(len(s)>MAX_SECTION for s in sections):raise ValueError('Section bound')
    frames=[]
    for i,s in enumerate(sections):
        if transaction_columns and i%2==1:
            from transaction_columns import encode as encode_transactions
            transformed,tx_stats=encode_transactions(s)
            plain=b'\x00'+s;columnar=b'\x01'+transformed
            plain_frame=zstd.compress(plain,level);column_frame=zstd.compress(columnar,level)
            if len(column_frame)<len(plain_frame):
                sections[i]=columnar;frames.append(column_frame);pool.stats['tx_column_pages']+=1
            else:sections[i]=plain;frames.append(plain_frame);pool.stats['tx_raw_pages']+=1
            for name,value in tx_stats.items():pool.stats['tx_'+name]+=value
        else:frames.append(zstd.compress(s,level))
    out=bytearray(HEAD.pack(b'XRPH',3 if transaction_columns else 2,rows[0][0],len(rows),page_ledgers,len(pages),all_hasher.digest()))
    for first,count,txfirst,nt,digest,*_ in pages:out.extend(PAGE.pack(first,count,txfirst,nt,digest))
    for raw,frame in zip(sections,frames):out.extend(SECTION.pack(len(raw),len(frame),hashlib.sha256(frame).digest()))
    for frame in frames:out.extend(frame)
    out.extend(hashlib.sha256(out).digest())
    pool.stats.update(dictionary_raw_bytes=len(dictionary),dictionary_compressed_bytes=len(frames[0]),
                      transaction_compressed_bytes=sum(len(f) for f in frames[1::2]),
                      history_compressed_bytes=sum(len(f) for f in frames[2::2]),transactions=tx_total,
                      raw_bytes=raw_total,envelope_and_indexes_bytes=HEAD.size+len(pages)*PAGE.size+len(sections)*SECTION.size+32)
    if len(out)>MAX_SEGMENT:raise ValueError('Encoded segment bound')
    return bytes(out),dict(pool.stats)


class Archive:
    def __init__(self,data,zstd):
        if len(data)>MAX_SEGMENT or len(data)<HEAD.size+32:raise ValueError('Segment bound')
        if hashlib.sha256(data[:-32]).digest()!=data[-32:]:raise ValueError('Segment checksum differs')
        magic,version,self.first,self.count,block,n,self.digest=HEAD.unpack_from(data)
        if magic!=b'XRPH' or version not in (2,3) or not 1<=self.count<=4096 or not 1<=block<=256 or n!=(self.count+block-1)//block:raise ValueError('Invalid segment header')
        self.version=version
        self.data,self.zstd=data,zstd;self.pages=[];self.sections=[];self.tx_cache={};self.tx_starts=[];self.hash_cache={};self.atom_cache={};self.resolving=set()
        pos=HEAD.size;seq=self.first;txfirst=0
        if pos+n*PAGE.size+(2*n+1)*SECTION.size>len(data)-32:raise ValueError('Truncated index')
        for i in range(n):
            page=PAGE.unpack_from(data,pos);pos+=PAGE.size;f,c,t,nt,digest=page
            if f!=seq or c!=min(block,self.first+self.count-seq) or t!=txfirst or nt>1_000_000:raise ValueError('Invalid page index')
            self.pages.append(page);self.tx_starts.append(t);seq+=c;txfirst+=nt
        start=pos+(2*n+1)*SECTION.size;raw_total=0
        for i in range(2*n+1):
            size,length,sha=SECTION.unpack_from(data,pos);pos+=SECTION.size
            if size>MAX_SECTION or start+length>len(data)-32:raise ValueError('Invalid section')
            self.sections.append((start,length,size,sha));start+=length;raw_total+=size
        if start!=len(data)-32 or raw_total>MAX_SEGMENT:raise ValueError('Segment coverage')
        self._dictionary(self.section(0))
    def section(self,i):
        at,length,size,sha=self.sections[i];frame=self.data[at:at+length]
        if hashlib.sha256(frame).digest()!=sha:raise ValueError('Section checksum differs')
        raw=self.zstd.decompress(frame)
        if len(raw)!=size:raise ValueError('Section size differs')
        return raw
    def _dictionary(self,data):
        r=R(data)
        if r.take(4)!=b'XHD2':raise ValueError('Dictionary magic')
        def count():return r.v(min(MAX_ITEMS,len(data)))
        self.headers=[r.blob() for _ in range(count())]
        if any(not 1<=len(h)<=3 for h in self.headers):raise ValueError('Invalid field header')
        self.values=[r.blob() for _ in range(count())];self.hashes=[]
        for _ in range(count()):
            mode=r.take(1)[0]
            if mode==0:item=(0,r.take(32))
            elif mode in RECIPE_ARGS:
                item=(mode,*(r.v(MAX_ITEMS) for _ in range(RECIPE_ARGS[mode])))
                if mode==9:
                    item=(*item,r.v((1<<64)-1))  # directory page number
                    if not item[2]:raise ValueError('Directory page number')
            else:raise ValueError('Invalid hash recipe')
            self.hashes.append(item)
        self.atoms=[]
        for _ in range(count()):
            hid,kind=r.v(),r.v(4)
            if kind==4:
                prefix=r.v(MAX_ITEMS);n=r.v(MAX_RECORD//32)
                self.atoms.append((hid,kind,prefix,*(r.v(MAX_ITEMS) for _ in range(n))))
            else:self.atoms.append((hid,kind,*(r.v(MAX_ITEMS) for _ in range(3 if kind==3 else 1))))
        self.shapes=[]
        for _ in range(count()):
            self.shapes.append(tuple(r.v() for _ in range(r.v(65536))))
        r.end()
    @staticmethod
    def get(values,i):
        if not 0<=i<len(values):raise ValueError('Dictionary reference out of range')
        return values[i]
    def tx_page(self,i):
        if i in self.tx_cache:return self.tx_cache[i]
        raw=self.section(1+2*i)
        if self.version==3:
            from transaction_columns import decode as decode_transactions
            if raw[:1]==b'\x00':raw=raw[1:]
            elif raw[:1]==b'\x01':raw=decode_transactions(raw[1:])
            else:raise ValueError('Transaction page mode')
        r=R(raw);first,count,txfirst,nt,_=self.pages[i]
        if r.v(256)!=count:raise ValueError('Transaction page count')
        rows=[];flat=[]
        for seq in range(first,first+count):
            if r.u32()!=seq:raise ValueError('Transaction sequence')
            header=r.blob();n=r.v(1_000_000);txs=[]
            for _ in range(n):
                tx=r.blob()
                if len(tx)>MAX_RECORD:raise ValueError('Transaction bound')
                txs.append(tx)
            rows.append((seq,header,txs));flat.extend(txs)
        r.end()
        if len(flat)!=nt:raise ValueError('Transaction index differs')
        self.tx_cache[i]=(rows,flat);return rows,flat
    def hash(self,i):
        if i in self.hash_cache:return self.hash_cache[i]
        mode,*args=self.get(self.hashes,i)
        if mode==0:h=args[0]
        elif mode==1:
            t=args[0];page=bisect_right(self.tx_starts,t)-1
            if page<0:raise ValueError('Transaction hash recipe range')
            _,txs=self.tx_page(page);index=t-self.pages[page][2]
            h=half(b'TXN\0'+self.get(txs,index))
        elif mode==9:
            if i in self.resolving:raise ValueError('Cyclic hash recipe')
            if not 0<args[1]:raise ValueError('Directory page number')
            self.resolving.add(i)
            try:h=half(b'\x00d'+self.hash(args[0])+args[1].to_bytes(8,'big'))
            finally:self.resolving.discard(i)
        elif mode==10:
            if not 0<=args[0]<self.count:raise ValueError('Ledger hash recipe range')
            seq=self.first+args[0];page=bisect_right([p[0] for p in self.pages],seq)-1
            rows,_=self.tx_page(page);h=half(b'LWR\0'+rows[seq-self.pages[page][0]][1])
        else:
            values=[self.get(self.values,a) for a in args]
            expected={2:[20],3:[20,4],4:[20,20,20],5:[20,4],6:[20,4],7:[20],8:[20,20,20,20,8]}[mode]
            if list(map(len,values))!=expected:raise ValueError('Object hash recipe size')
            if mode==8:h=half(b'\x00B'+b''.join(values[:4]))[:24]+values[4]
            else:h=half({2:b'\x00a',3:b'\x00o',4:b'\x00r',5:b'\x00T',6:b'\x00C',7:b'\x00O'}[mode]+b''.join(values))
        self.hash_cache[i]=h;return h
    def atom(self,i):
        if i in self.atom_cache:return self.atom_cache[i]
        hid,kind,*args=self.get(self.atoms,i);h=self.get(self.headers,hid)
        if kind==0:v=self.get(self.values,args[0])
        elif kind==1:v=self.hash(args[0])
        elif kind==2:
            account=self.get(self.values,args[0])
            if len(account)!=20:raise ValueError('Account size')
            v=b'\x14'+account
        elif kind==3:
            parts=[self.get(self.values,a) for a in args]
            if list(map(len,parts))!=[8,20,20]:raise ValueError('Amount size')
            v=b''.join(parts)
        else:
            prefix=self.get(self.values,args[0]);q=R(prefix);a=q.take(1)[0]
            if a<=192:n=a
            elif a<=240:n=193+(a-193)*256+q.take(1)[0]
            elif a<=254:
                b,c=q.take(2);n=12481+(a-241)*65536+b*256+c
            else:raise ValueError('Invalid vector length')
            q.end()
            if n!=32*(len(args)-1):raise ValueError('Vector length differs')
            v=prefix+b''.join(self.hash(i) for i in args[1:])
        value=h+v
        if len(value)>MAX_RECORD:raise ValueError('Atom bound')
        self.atom_cache[i]=value;return value
    def view(self,r,key,cache,budget):
        shape=self.get(self.shapes,r.v());mask=r.take((len(shape)+7)//8);old=cache.setdefault(key,{});out=bytearray()
        if shape and len(shape)%8 and mask[-1]>>(len(shape)%8):raise ValueError('Unused view bits')
        for j,hid in enumerate(shape):
            if mask[j//8]&(1<<(j%8)):
                aid=r.v()
                if self.get(self.atoms,aid)[0]!=hid:raise ValueError('Field shape differs')
                old[hid]=aid
            if hid not in old:raise ValueError('Missing page anchor')
            out.extend(self.atom(old[hid]))
            if len(out)>budget:raise ValueError('View expansion bound')
        return bytes(out)
    def node(self,r,cache,budget,depth=0):
        if depth>64:raise ValueError('Node depth')
        tag=r.take(1)[0]
        if tag==0:
            value=self.atom(r.v())
            if len(value)>budget:raise ValueError('Node bound')
            return value
        if tag not in (1,2):raise ValueError('Invalid node tag')
        h=self.get(self.headers,r.v());end=r.blob()
        if not 1<=len(end)<=3 or len(h)+len(end)>budget:raise ValueError('Container bound')
        if tag==2:
            key=r.v();self.get(self.hashes,key);reset=r.take(1)[0]
            if reset not in (0,1):raise ValueError('Reset flag')
            if reset:cache.pop(key,None)
            return h+self.view(r,key,cache,budget-len(h)-len(end))+end
        out=bytearray(h)
        for _ in range(r.v(min(1_000_000,len(r.data)))):
            out.extend(self.node(r,cache,budget-len(out)-len(end),depth+1))
        return bytes(out)+end
    def metadata(self,r,cache,length):
        tag=r.take(1)[0]
        if tag==0:value=r.blob()
        elif tag==1:
            value=bytearray()
            for _ in range(r.v(65536)):value.extend(self.node(r,cache,length-len(value)))
            value=bytes(value)
        else:raise ValueError('Metadata tag')
        if len(value)!=length:raise ValueError('Metadata byte length')
        return value
    def page(self,i):
        tx_rows,_=self.tx_page(i);r=R(self.section(2+2*i));first,count,_,_,digest=self.pages[i];cache={};records=[];total=0
        if r.v(256)!=count:raise ValueError('History page count')
        for seq,header,txs in tx_rows:
            if r.v(1_000_000)!=len(txs):raise ValueError('Metadata count')
            metas=[None]*len(txs)
            for _ in txs:
                at=r.v();length=r.v(MAX_RECORD)
                if at>=len(txs) or metas[at] is not None:raise ValueError('Transaction permutation')
                metas[at]=self.metadata(r,cache,length)
            row=bytearray(struct.pack('<II',seq,len(header))+header+struct.pack('<I',len(txs)))
            for tx,meta in zip(txs,metas):row.extend(struct.pack('<I',len(tx))+tx+struct.pack('<I',len(meta))+meta)
            n=r.v(min(1_000_000,len(r.data)));row.extend(struct.pack('<I',n));prior=None
            for _ in range(n):
                kid=r.v();key=self.hash(kid);length=r.v(MAX_RECORD)
                if prior is not None and key<=prior:raise ValueError('Patch order')
                prior=key
                if not length:cache.pop(kid,None);value=b''
                else:
                    tag=r.take(1)[0]
                    if tag==0:value=r.blob()
                    elif tag==1:value=self.view(r,kid,cache,length)
                    else:raise ValueError('Object tag')
                    if len(value)!=length:raise ValueError('Object byte length')
                row.extend(key+struct.pack('<I',length)+value)
                if len(row)>MAX_SECTION:raise ValueError('Ledger bound')
            total+=len(row)
            if total>MAX_SECTION:raise ValueError('Page decoded bound')
            records.append((seq,bytes(row)))
        r.end()
        if hashlib.sha256(body_from_records(records)).digest()!=digest:raise ValueError('Canonical page digest differs')
        return records
    def read(self,sequence):
        if not self.first<=sequence<self.first+self.count:raise ValueError('Ledger outside segment')
        i=bisect_right([p[0] for p in self.pages],sequence)-1
        return dict(self.page(i))[sequence]
    def restore(self):
        rows=[]
        for i in range(len(self.pages)):rows.extend(self.page(i))
        result=body_from_records(rows)
        if hashlib.sha256(result).digest()!=self.digest:raise ValueError('Canonical segment digest differs')
        return rows


def transcode_transactions(data,zstd,level=19):
    """Keep history frames unchanged and adaptively replace only tx pages."""
    archive=Archive(data,zstd)
    if archive.version!=2:raise ValueError('Transaction transform requires v2 prototype')
    from transaction_columns import encode as encode_transactions
    frames=[];sizes=[];stats=Counter()
    for i,(at,length,size,sha) in enumerate(archive.sections):
        frame=data[at:at+length]
        if i%2==1:
            raw=archive.section(i);columns,counts=encode_transactions(raw)
            candidates=[b'\x00'+raw,b'\x01'+columns]
            compressed=[zstd.compress(x,level) for x in candidates]
            stats['tx_raw_candidate_bytes']+=len(compressed[0]);stats['tx_columns_candidate_bytes']+=len(compressed[1])
            mode=min(range(2),key=lambda j:len(compressed[j]));frame=compressed[mode];size=len(candidates[mode])
            stats['tx_column_pages' if mode else 'tx_raw_pages']+=1
            for name,value in counts.items():stats['tx_'+name]+=value
        frames.append(frame);sizes.append(size)
    head=list(HEAD.unpack_from(data));head[1]=3
    out=bytearray(HEAD.pack(*head)+data[HEAD.size:HEAD.size+len(archive.pages)*PAGE.size])
    for size,frame in zip(sizes,frames):out.extend(SECTION.pack(size,len(frame),hashlib.sha256(frame).digest()))
    for frame in frames:out.extend(frame)
    out.extend(hashlib.sha256(out).digest())
    stats['transaction_compressed_bytes']=sum(len(frame) for frame in frames[1::2])
    return bytes(out),dict(stats)
