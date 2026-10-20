"""Lossless experimental codec, independent blocks and bounded delta links."""
import random
from pathlib import Path
import struct
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import delta_codec as codec


def body(rows):
    result=bytearray();table=bytearray()
    for seq,prefix,patches in rows:
        table.extend(struct.pack('<IQ',seq,len(result)));result.extend(prefix);result.extend(struct.pack('<I',len(patches)))
        for key,value in patches:result.extend(key+struct.pack('<I',len(value))+value)
    return bytes(result+table)


def fixtures():
    rng=random.Random(917);keys=[i.to_bytes(32,'big') for i in range(20)];values={}
    rows=[]
    for seq in range(100,180):
        patches=[]
        for key in keys:
            prior=values.get(key,b'account'+bytes(32)+bytes(80)+b'unchanged-suffix')
            kind=rng.randrange(6)
            if key==keys[0]:value=b'constant value' # forces chain reset
            elif kind==0:value=b''
            elif kind==1:value=prior+b'x'
            elif kind==2:value=prior[:10]
            elif kind==3 and len(prior)>40:value=prior[:12]+rng.randbytes(8)+prior[20:]
            elif kind==4:value=rng.randbytes(63)
            else:value=prior
            values[key]=value;patches.append((key,value))
        tx=b'transaction';meta=b'metadata'
        prefix=struct.pack('<II',seq,4)+b'head'+struct.pack('<II',1,len(tx))+tx+struct.pack('<I',len(meta))+meta
        rows.append((seq,prefix,patches))
    return rows


class Codec(unittest.TestCase):
    def test_roundtrip_and_random_access_all_modes(self):
        rows=fixtures();original=body(rows)
        self.assertEqual(codec.split_v2(original,len(rows)),rows)
        for block in (1,8,32,64):
            for keys,deltas,inline in ((True,False,False),(False,True,False),(True,True,False),(True,False,True),(True,True,True)):
                encoded,stats=codec.encode_rows(rows,block,16,keys,deltas,inline)
                self.assertEqual(codec.decode(encoded),original)
                self.assertLessEqual(stats['max_chain_observed'],16)
                for row in rows:
                    expected=body([row])[:-12]
                    self.assertEqual(codec.read_ledger(encoded,row[0]),expected)
                with self.assertRaises(ValueError):codec.read_ledger(encoded,999)
    def test_long_same_value_chain_is_reanchored(self):
        encoded,stats=codec.encode_rows(fixtures(),64,4)
        self.assertEqual(stats['max_chain_observed'],4)
        self.assertEqual(codec.decode(encoded),body(fixtures()))
    def test_256_ledger_boundaries_and_enforced_chain_limit(self):
        base=fixtures();rows=[]
        for i in range(520):
            _,prefix,patches=base[i%len(base)];seq=100+i
            rows.append((seq,struct.pack('<I',seq)+prefix[4:],patches))
        encoded,stats=codec.encode_rows(rows,256,16,True,True,True)
        self.assertEqual(codec.decode(encoded),body(rows))
        for i in (0,254,255,256,257,511,512,519):
            self.assertEqual(codec.read_ledger(encoded,100+i),body([rows[i]])[:-12])
        # The decoder enforces the declared chain length, not just its header range.
        altered=bytearray(encoded);altered[8:10]=struct.pack('<H',1)
        with self.assertRaises(ValueError):codec.decode(bytes(altered))
    def test_corrupt_structure_is_rejected(self):
        encoded,_=codec.encode_rows(fixtures())
        for bad in (encoded[:10],encoded[:-1],encoded+b'extra',b'bad!'+encoded[4:]):
            with self.assertRaises(ValueError):codec.decode(bad)
        bad=bytearray(encoded);bad[8:10]=struct.pack('<H',17)
        with self.assertRaises(ValueError):codec.decode(bytes(bad))
    def test_operations_and_malformed_edits(self):
        for old,new in ((b'1234',b'1234'),(b'abcXXdef',b'abcYYdef'),(b'abc123xyz',b'abc456789xyz'),(b'a'*100,b'a'*15+b'X'+b'a'*60+b'Y'+b'a'*23),(b'data',b'')):
            r=codec.Reader(codec.delta(old,new));self.assertEqual(codec.apply_delta(r,old)[0],new);r.end()
        for data in (b'\x03',b'\x01\x20\x00\x00',b'\x02\x01\x04\x03abc',b'\x07'):
            with self.assertRaises(ValueError):codec.apply_delta(codec.Reader(data),None if data==b'\x03' else b'abcd')
        with self.assertRaises(ValueError):codec.Reader(b'\x80\x00').varint()


if __name__=='__main__':unittest.main()
