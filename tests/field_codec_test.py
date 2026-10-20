"""Exact bytes, shared metadata/object fields, fallbacks, bounds and corruption."""
import hashlib
import json
from pathlib import Path
import struct
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import field_codec as fc
from compression_probe import Zstd
from delta_codec_test import body


class Codec(unittest.TestCase):
    def test_known_types_and_nested_fields(self):
        examples=[bytes.fromhex('24000000018114')+b'a'*20,
                  bytes.fromhex('f8e562')+bytes.fromhex('4000000000000064e1f1'),
                  b'\x61'+b'\x80'+bytes(7)+b'c'*20+b'a'*20,
                  b'\x61'+b'\x20'+bytes(8)+b'm'*24,
                  bytes.fromhex('011231')+b'a'*20+b'c'*20+b'i'*20+b'\x00',
                  bytes.fromhex('011340')+bytes(64)]
        for data in examples:
            self.assertEqual(b''.join(x[0]+x[3] for x in fc.fields(data)),data)
    def test_byte_equivalence_shared_fields_and_unknown_fallback(self):
        rows=[];account=b'\x81\x14'+b'a'*20
        for seq in range(1,68):
            record=b'\x24'+seq.to_bytes(4,'big')+account
            meta=b'\xf8\xe5'+record+b'\xe1\xf1'
            prefix=struct.pack('<II',seq,4)+b'head'+struct.pack('<II',1,len(record))+record+struct.pack('<I',len(meta))+meta
            rows.append((seq,prefix,[(bytes(32),record),(bytes(31)+b'\x01',b'\x0c\xffunknown')]))
        original=body(rows);z=Zstd()
        for sharing in (False,True):
            for block in (1,32,256):
                data,stats=fc.encode(original,len(rows),z,block,sharing)
                self.assertEqual(fc.restore(data,z),original)
                self.assertEqual(stats['literal_fallback_records'],67)
                if sharing:self.assertGreater(stats['metadata_patch_shared_occurrences'],0)
                for at in (1,32,33,67):self.assertEqual(fc.restore(data,z,at),body([rows[at-1]])[:-12])
                for bad in (data[:-1],data+b'x',data[:20],bytes([data[0]^1])+data[1:]):
                    with self.assertRaises(ValueError):fc.restore(bad,z)
                with self.assertRaises(ValueError):fc.restore(data,z,999)
    def test_real_mainnet_fixture_all_bytes(self):
        root=Path(__file__).parent/'fixtures/mainnet-transactions';m=json.loads((root/'manifest.json').read_text());z=Zstd()
        for c in m['chunks']:
            raw=z.decompress((root/c['object']).read_bytes()[51:-40]);n=c['last']-c['first']+1
            encoded,stats=fc.encode(raw,n,z,256,True)
            self.assertEqual(fc.restore(encoded,z),raw)
            self.assertEqual(stats.get('literal_fallback_records',0),0)
    def test_invalid_column_and_expansion(self):
        with self.assertRaises(ValueError):fc.unpack_column(b'\x01\x01'+fc.blob(b'\x01abcd\x02'),4,100)
        with self.assertRaises(ValueError):fc.unpack_column(b'\x00\x05'+fc.blob(b'abcd'*5),4,10)
        with self.assertRaises(ValueError):fc.unpack_column(b'\x02\x01'+fc.blob(fc.vu(512)),1,10)
        with self.assertRaises(ValueError):fc.fields(b'\xf8\xe5\x24\x00')
        with self.assertRaises(ValueError):fc.fields(b'\xe1')
    def test_rehashed_corrupt_block_table(self):
        z=Zstd();prefix=struct.pack('<II',1,1)+b'h'+struct.pack('<I',0)
        original=body([(1,prefix,[])])
        data,_=fc.encode(original,1,z)
        for offset,new in ((fc.HEADER.size,2),(fc.HEADER.size+4,257),(fc.HEADER.size+8,1)):
            damaged=bytearray(data);struct.pack_into('<I',damaged,offset,new)
            damaged[-32:]=hashlib.sha256(damaged[:-32]).digest()
            with self.assertRaises(ValueError):fc.restore(bytes(damaged),z)


if __name__=='__main__':unittest.main()
