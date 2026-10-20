import hashlib
import struct
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from native_benchmark_fixture import extract_tokens,qualified_fixture

IDENTITY={name:bytes([index])*32 for index,name in enumerate(('weights','tokenizer','template','layout'),1)}


def checkpoint(logical=2,tokens=(0,1),vocab=4,version=2,tail=b''):
    payload=struct.pack('<QIIII',logical,len(tokens),vocab,1,1)+struct.pack('<'+'I'*len(tokens),*tokens)
    payload+=struct.pack('<4fI',0.,1.,2.,3.,0)
    payload+=struct.pack('<III',0,0,1)+struct.pack('<IIQ',2,2,0)
    payload+=struct.pack('<4Q',0,1,1,2)+struct.pack('<10f',*([0.]*10))
    header=struct.pack('<8sIIQ',b'ANTFLYKV',version,0,len(payload))+b''.join(IDENTITY.values())
    prefix=header+payload
    return prefix+hashlib.sha256(prefix).digest()+tail


class FixtureTests(unittest.TestCase):
    def extract(self,data,expected=None,identity=None):
        # Tiny data fixture only; this is not a compiler/build cache.
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'state.kv';path.write_bytes(data)
            return extract_tokens(path,expected or hashlib.sha256(data).hexdigest(),
                                  identity or {k:v.hex() for k,v in IDENTITY.items()})

    def test_valid_tokens_are_recovered_exactly(self):
        value=self.extract(checkpoint())
        self.assertEqual(value['token_ids'],[0,1])
        self.assertEqual(value['token_bytes_sha256'],hashlib.sha256(struct.pack('<II',0,1)).hexdigest())

    def test_integrity_and_qualified_file_digest_are_independent(self):
        data=bytearray(checkpoint());data[-33]^=1
        with self.assertRaisesRegex(ValueError,'integrity'):
            self.extract(bytes(data))
        with self.assertRaisesRegex(ValueError,'qualified digest'):
            self.extract(checkpoint(),expected='0'*64)

    def test_version_identity_and_framing_must_match(self):
        for data in [checkpoint(version=1),checkpoint()[:-1],checkpoint(tail=b'x')]:
            with self.subTest(size=len(data)),self.assertRaises(ValueError):self.extract(data)
        with self.assertRaisesRegex(ValueError,'identity'):
            self.extract(checkpoint(),identity={k:'00'*32 for k in IDENTITY})

    def test_invalid_counts_and_vocabulary_ids_fail(self):
        for data in [checkpoint(logical=3),checkpoint(logical=0,tokens=()),
                     checkpoint(vocab=262145),checkpoint(tokens=(0,4))]:
            with self.subTest(size=len(data)),self.assertRaises(ValueError):self.extract(data)

    def test_failed_campaign_cannot_supply_qualified_fixture(self):
        with patch('native_benchmark_fixture.Path.read_text',return_value='{"passed":false}'):
            with self.assertRaisesRegex(ValueError,'did not pass'):
                qualified_fixture('reference.json','state.kv','8k')


if __name__=='__main__':unittest.main()
