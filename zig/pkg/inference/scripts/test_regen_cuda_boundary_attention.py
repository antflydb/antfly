#!/usr/bin/env python3
"""Compiler-free identity checks; numerical and regeneration gates are separate."""
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import unittest

spec = importlib.util.spec_from_file_location('boundary_attention_regen', Path(__file__).with_name('regen-cuda-boundary-attention.py'))
regen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(regen)


class BoundaryAttentionArtifactsTest(unittest.TestCase):
    def test_manifest_binds_both_architectures_sources_and_dependencies(self):
        metadata = json.loads((regen.ARTIFACTS/'gliner25_boundary_attention.json').read_text())
        expected = {f'gliner25_boundary_attention_{direction}.{suffix}'
                    for direction in regen.DIRECTIONS for suffix in ('cubin','sm80.cubin')}
        self.assertEqual(set(metadata['artifacts']), expected)
        self.assertEqual(metadata['compiler'], regen.COMPILER)
        self.assertEqual(metadata['options'], regen.OPTIONS)
        self.assertEqual(metadata['dependency_lock_sha256'], hashlib.sha256(regen.LOCK.read_bytes()).hexdigest())
        self.assertEqual(set(metadata['source_sha256']), {f'gliner25_boundary_attention_{d}.cu' for d in regen.DIRECTIONS})
        for name, expected_hash in {**metadata['artifacts'], **metadata['source_sha256']}.items():
            with self.subTest(name=name):
                data = (regen.ARTIFACTS/name).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), expected_hash)
                if name.endswith('.cubin'):
                    self.assertEqual(data[:6], b'\x7fELF\x02\x01')
                    flags = struct.unpack_from('<I', data, 48)[0]
                    self.assertEqual((flags >> 8) & 0xff, 80 if '.sm80.' in name else 89)
                    entry = b'boundary_backward' if '_backward.' in name else b'boundary_forward'
                    self.assertIn(entry+b'\x00', data)


if __name__ == '__main__':
    unittest.main()
