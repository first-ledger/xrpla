"""Combined history prototype: exact originals, cross-page recipes and bounds."""
import hashlib
import json
from pathlib import Path
import struct
import sys
import unittest
import subprocess
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import history_codec as hc
from compression_probe import Zstd
from delta_codec import split_v2
import transaction_columns as tc

CPP=sys.argv.pop(1) if len(sys.argv)>1 else None


def record(data):return struct.pack('<I',len(data))+data


def fixtures(n=18):
    account=b'a'*20;key=hc.half(b'\x00a'+account);rows=[]
    for seq in range(1,n+1):
        txs=[];final=None
        for idx in (1,0):
            number=seq*2+idx
            tx=b'\x12\x00\x00\x24'+number.to_bytes(4,'big')+b'\x81\x14'+account
            fields=b'\x24'+number.to_bytes(4,'big')+b'\x62'+(2**62+number).to_bytes(8,'big')+b'\x81\x14'+account
            meta=b'\x20\x1c'+idx.to_bytes(4,'big')+b'\xf8\xe5\x11\x00a\x56'+key+b'\xe6\x24'+(number-1).to_bytes(4,'big')+b'\xe1\xe7'+fields+b'\xe1\xe1\xf1\x03\x10\x00'
            txs.append((tx,meta))
            if idx==1:final=b'\x11\x00a\x24'+number.to_bytes(4,'big')+b'\x25'+seq.to_bytes(4,'big')+b'\x55'+hc.half(b'TXN\0'+tx)+fields[5:]
        prefix=struct.pack('<I',seq)+record(b'h'*118)+struct.pack('<I',2)+b''.join(record(t)+record(m) for t,m in txs)
        patches=sorted([(key,b'' if seq%5==0 else final),(bytes(32),b'\x0c\xffunknown')])
        rows.append((seq,prefix,patches))
    return rows


def keylet_fixtures():
    """Ticket, Check, owner/book/page directories and a skip list naming ledger 1."""
    a,b=b'A'*20,b'B'*20;header=lambda seq:bytes([seq])*118
    ticket=b'\x11\x00T\x20\x29'+(7).to_bytes(4,'big')+b'\x81\x14'+a
    check=b'\x11\x00C\x24'+(9).to_bytes(4,'big')+b'\x81\x14'+a
    owner_key=hc.half(b'\x00O'+b);owner=b'\x11\x00d\x58'+owner_key+b'\x82\x14'+b
    rate=(5).to_bytes(8,'big');book_key=hc.half(b'\x00B'+bytes(20)+b'USD'.ljust(20,b'\0')+bytes(20)+a)[:24]+rate
    book=b'\x11\x00d\x36'+rate+b'\x58'+book_key+b'\x01\x11'+bytes(20)+b'\x02\x11'+bytes(20)+b'\x03\x11'+b'USD'.ljust(20,b'\0')+b'\x04\x11'+a
    page=b'\x11\x00d\x32'+(2).to_bytes(8,'big')+b'\x58'+owner_key+b'\x82\x14'+b
    skip=b'\x11\x00h\x02\x13\x20'+hc.half(b'LWR\0'+header(1))
    objects=[(hc.half(b'\x00T'+a+(7).to_bytes(4,'big')),ticket),(hc.half(b'\x00C'+a+(9).to_bytes(4,'big')),check),
             (owner_key,owner),(book_key,book),(hc.half(b'\x00d'+owner_key+(3).to_bytes(8,'big')),page),(hc.half(b'\x00s'),skip)]
    rows=[]
    for seq in (1,2,3):
        prefix=struct.pack('<I',seq)+record(header(seq))+struct.pack('<I',0)
        rows.append((seq,prefix,sorted(objects) if seq==2 else []))
    return rows


class History(unittest.TestCase):
    def test_extra_recipes_derive_keylets_and_ledger_hashes(self):
        z=Zstd();rows=keylet_fixtures();data,stats=hc.encode(rows,z,2)
        for mode in range(5,11):self.assertEqual(stats.get(f'hash_recipe_{mode}'),1,mode)
        self.assertEqual(hc.Archive(data,z).restore(),[(r[0],hc.raw_record(r)) for r in rows])
        plain,counts=hc.encode(rows,z,2,extra_recipes=False)
        self.assertFalse([k for k in counts if k.startswith('hash_recipe_') and int(k[12:])>4])
        self.assertLess(len(data),len(plain))
        if CPP:
            with tempfile.TemporaryDirectory() as d:
                path,out=Path(d)/'s.xrph',Path(d)/'s.canonical';path.write_bytes(data)
                subprocess.run([CPP,str(path),'--output',str(out)],check=True,capture_output=True)
                self.assertEqual(out.read_bytes(),hc.original_body(rows))
        # An unknown recipe mode or a zero directory page is rejected rather than misread.
        archive=hc.Archive(data,z)
        for entry in (b'\x0b\x00',b'\x09\x00\x00'):
            with self.assertRaises(ValueError):archive._dictionary(b'XHD2'+hc.vu(0)+hc.vu(0)+hc.vu(1)+entry+hc.vu(0)+hc.vu(0))
    def test_transaction_columns_and_original_page_selection(self):
        txs=[b'\x12\x00\x00\x31'+n.to_bytes(8,'big')+b'\x74\x46'+b's'*70 for n in [*range(100),2**64-1,*range(100,200)]]
        txs.append(b'\x0c\xfffuture')
        raw=hc.vu(1)+struct.pack('<I',5)+hc.blob(b'h'*118)+hc.vu(len(txs))+b''.join(hc.blob(t) for t in txs)
        packed,stats=tc.encode(raw)
        self.assertEqual(tc.decode(packed),raw);self.assertEqual(stats['literal_fallbacks'],1)
        for bad in (packed[:10],packed+b'bad',b'bad!'+packed[4:]):
            with self.assertRaises(ValueError):tc.decode(bad)
        z=Zstd();rows=fixtures(30);data,stats=hc.encode(rows,z,8,transaction_columns=True)
        self.assertEqual(hc.HEAD.unpack_from(data)[1],3)
        self.assertEqual(stats.get('tx_column_pages',0)+stats.get('tx_raw_pages',0),4)
        self.assertEqual(hc.Archive(data,z).restore(),[(r[0],hc.raw_record(r)) for r in rows])
        base,_=hc.encode(rows,z,8)
        self.assertEqual(data,hc.transcode_transactions(base,z)[0])
    @unittest.skipUnless(CPP,'Compiled reader supplied by history_cpp CTest')
    def test_compiled_reader_all_bytes_versions_parallel_and_corruption(self):
        root=Path(__file__).parent/'fixtures/mainnet-transactions';m=json.loads((root/'manifest.json').read_text());z=Zstd();rows=[]
        for c in m['chunks']:rows.extend(split_v2(z.decompress((root/c['object']).read_bytes()[51:-40]),c['last']-c['first']+1))
        with tempfile.TemporaryDirectory() as temporary:
            d=Path(temporary);paths=[]
            for version in (False,True):
                for page in (1,2,5):
                    data,_=hc.encode(rows,z,page,transaction_columns=version);p=d/f'{version}-{page}.xrph';p.write_bytes(data);paths.append(str(p));out=d/'body'
                    run=subprocess.run([CPP,str(p),'--output',str(out)],capture_output=True,text=True,check=True)
                    self.assertTrue(json.loads(run.stdout)['passed']);self.assertEqual(out.read_bytes(),hc.original_body(rows))
                    subprocess.run([CPP,str(p),'--read',str(rows[-1][0]),'--output',str(out)],capture_output=True,check=True)
                    self.assertEqual(out.read_bytes(),hc.raw_record(rows[-1]))
                    # A ledger decoded alone, after the page's earlier ledgers are only parsed, is the same ledger.
                    for row in rows:
                        subprocess.run([CPP,str(p),'--ledger',str(row[0]),'--output',str(out)],capture_output=True,check=True)
                        self.assertEqual(out.read_bytes(),hc.raw_record(row))
                    # Each ledger alone as its transactions only (shuffled), and interleaved with whole and patches-only decodes
                    # on the same reader (ascending, shuffled): the parts resume from each other's cursors.
                    run=subprocess.run([CPP,str(p),'--parts'],capture_output=True,text=True,check=True)
                    self.assertEqual(json.loads(run.stdout),{'passed':True,'checks':3*len(rows)})
            run=subprocess.run([CPP,*paths,'--jobs','3','--repeat','2'],capture_output=True,text=True,check=True)
            self.assertEqual(json.loads(run.stdout)['ledgers'],len(rows)*6)
            data=bytearray(data);data[18]^=1;data[-32:]=hashlib.sha256(data[:-32]).digest();p=d/'bad';p.write_bytes(data)
            run=subprocess.run([CPP,str(p)],capture_output=True,text=True);self.assertNotEqual(run.returncode,0);self.assertIn('Canonical segment digest differs',run.stderr)
    def test_exact_order_views_recipes_and_page_anchors(self):
        rows=fixtures();z=Zstd();original=hc.original_body(rows)
        for page in (1,4,16):
            for recipes in (False,True):
                data,stats=hc.encode(rows,z,page,recipes)
                decoder=hc.Archive(data,z)
                self.assertEqual(hc.body_from_records(decoder.restore()),original)
                self.assertGreater(stats['unchanged_field_references'],0)
                self.assertEqual(stats['object_literal_fallbacks'],18)
                self.assertEqual(stats['deletions'],3)
                if recipes:self.assertGreater(stats['hash_recipe_1'],0);self.assertGreater(stats['hash_recipe_2'],0)
                for at in (1,4,5,18):self.assertEqual(hc.Archive(data,z).read(at),hc.raw_record(rows[at-1]))
                with self.assertRaises(ValueError):decoder.read(100)
    def test_real_fixture_and_recipe_equivalence(self):
        root=Path(__file__).parent/'fixtures/mainnet-transactions';m=json.loads((root/'manifest.json').read_text());z=Zstd();rows=[]
        for c in m['chunks']:rows.extend(split_v2(z.decompress((root/c['object']).read_bytes()[51:-40]),c['last']-c['first']+1))
        data,stats=hc.encode(rows,z,2,True);actual=hc.Archive(data,z).restore()
        self.assertEqual(actual,[(r[0],hc.raw_record(r)) for r in rows])
        self.assertEqual(stats.get('object_literal_fallbacks',0),0)
        self.assertGreater(stats['hash_recipe_4'],0)
        for seq in reversed([r[0] for r in rows]):self.assertEqual(hc.Archive(data,z).read(seq),dict(actual)[seq])
    def test_corruption_and_rehashed_bad_indexes(self):
        z=Zstd();data,_=hc.encode(fixtures(),z,4)
        for bad in (data[:20],data[:-1],data+b'x',b'bad!'+data[4:]):
            with self.assertRaises(ValueError):hc.Archive(bad,z)
        for offset,value in ((hc.HEAD.size,99),(hc.HEAD.size+4,1000),(hc.HEAD.size+8,99)):
            bad=bytearray(data);struct.pack_into('<I',bad,offset,value);bad[-32:]=hashlib.sha256(bad[:-32]).digest()
            with self.assertRaises(ValueError):hc.Archive(bytes(bad),z)
    def test_parser_preserves_binary_tree_and_fallback(self):
        for row in fixtures(1):
            for key,value in row[2]:
                if value.startswith(b'\x0c'):continue
                self.assertEqual(b''.join(map(hc.serialize,hc.parse(value))),value)
        for bad in (b'\xe1',b'\xe5\x24\x00',b'\x0c\xffunknown'):
            with self.assertRaises(ValueError):hc.parse(bad)
    def test_segment_limits(self):
        z=Zstd()
        for rows,page in (([],1),(fixtures(2),0),(fixtures(2),257)):
            with self.assertRaises(ValueError):hc.encode(rows,z,page)
        rows=fixtures(2);rows[1]=(99,*rows[1][1:])
        with self.assertRaises(ValueError):hc.encode(rows,z)
    def test_vector_recipes_read_without_other_history_pages(self):
        rows=fixtures(4);r=hc.R(rows[0][1]);r.u32();r.record();r.u32();first_tx=r.record()
        account_key=hc.half(b'\x00a'+b'a'*20);tx_hash=hc.half(b'TXN\0'+first_tx)
        # Two known recipes and an unknown literal in a later page's Vector256.
        vector=b'\x11\x00d\x01\x13\x60'+account_key+tx_hash+b'z'*32
        seq,prefix,patches=rows[-1];rows[-1]=(seq,prefix,sorted(patches+[(b'y'*32,vector)]))
        z=Zstd();data,stats=hc.encode(rows,z,1,True)
        self.assertGreater(stats['hash_vector_references'],0)
        reader=hc.Archive(data,z);section=reader.section;loaded=[]
        def tracked(i):loaded.append(i);return section(i)
        reader.section=tracked
        self.assertEqual(reader.read(4),hc.raw_record(rows[-1]))
        self.assertIn(1,loaded)  # Raw transactions of page one supply its hash.
        self.assertEqual([i for i in loaded if i%2==0],[8])  # Only page four history.
        # A modified source transaction with freshly recomputed transport hashes
        # must still fail the retained original canonical digest.
        raw=list(reader.sections);frames=[]
        for i,(at,length,size,sha) in enumerate(raw):
            frame=data[at:at+length]
            if i==7:
                decoded=bytearray(z.decompress(frame));decoded[-1]^=1;frame=z.compress(bytes(decoded),9)
            frames.append(frame)
        index_start=hc.HEAD.size+len(reader.pages)*hc.PAGE.size
        altered=bytearray(data[:index_start])
        for entry,frame in zip(raw,frames):altered.extend(hc.SECTION.pack(entry[2],len(frame),hashlib.sha256(frame).digest()))
        for frame in frames:altered.extend(frame)
        altered.extend(hashlib.sha256(altered).digest())
        with self.assertRaisesRegex(ValueError,'Canonical page digest differs'):hc.Archive(bytes(altered),z).read(4)
    def test_recreation_and_unknown_metadata_preserve_every_byte(self):
        rows=fixtures(8);changed=[]
        for seq,prefix,patches in rows:
            r=hc.R(prefix);r.u32();header=r.record();n=r.u32();txs=[]
            for i in range(n):
                tx,meta=r.record(),r.record()
                if seq==2 and i==0:meta+=b'\x0c\xfffuture-type'
                if seq==6:
                    nodes=hc.parse(meta);rebuilt=[]
                    for h,t,f,value,end in nodes:
                        if (t,f)==(15,8):
                            created=[]
                            for nh,nt,nf,children,ne in value:
                                fields=[]
                                for ch,ct,cf,cv,ce in children:
                                    if (ct,cf)==(14,6):continue
                                    if (ct,cf)==(14,7):ch,cf=b'\xe8',8
                                    fields.append((ch,ct,cf,cv,ce))
                                created.append((b'\xe3',nt,3,fields,ne))
                            value=created
                        rebuilt.append((h,t,f,value,end))
                    meta=b''.join(map(hc.serialize,rebuilt))
                txs.append((tx,meta))
            prefix=struct.pack('<I',seq)+record(header)+struct.pack('<I',n)+b''.join(record(t)+record(m) for t,m in txs)
            changed.append((seq,prefix,patches))
        z=Zstd();data,stats=hc.encode(changed,z,8)
        self.assertEqual(stats['metadata_literal_fallbacks'],1)
        self.assertEqual(hc.Archive(data,z).restore(),[(r[0],hc.raw_record(r)) for r in changed])


if __name__=='__main__':unittest.main()
