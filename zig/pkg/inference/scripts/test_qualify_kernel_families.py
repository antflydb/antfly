#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exercise qualification failure gates without a model fixture or GPU."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

spec = importlib.util.spec_from_file_location('qualification', Path(__file__).with_name('qualify_kernel_families.py'))
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = self.root / 'config.json'
        self.config.write_text(json.dumps({'arm_order': 'ABBA', 'cases': [
            {'name': 'model.with.dots', 'args': []}]}))
        self.baseline = self.executable('baseline', '1.0')
        self.candidate = self.executable('candidate', '1.0')
        self.args = SimpleNamespace(config=self.config, baseline=self.baseline,
                                    candidate=self.candidate, results=self.root)

    def executable(self, name, value, output=True):
        import sys
        path = self.root / name
        path.write_text('#!' + sys.executable + '\nimport sys, struct\n'
                        + ("open(sys.argv[sys.argv.index('--output')+1], 'wb').write(struct.pack('f', "
                           + value + '))\n' if output else '')
                        + "print('unified_execution p50_ns=100 p95_ns=110')\n")
        path.chmod(0o755)
        return path

    def run_models(self):
        with contextlib.redirect_stdout(io.StringIO()):
            return qualification.models(self.args)

    def test_balanced_arms_keep_distinct_outputs_and_raw_timings(self):
        self.assertTrue(self.run_models())
        receipt = json.loads((self.root / 'receipt.json').read_text())
        self.assertEqual(len(receipt['cases'][0]['arms']), 4)
        self.assertEqual(len(list(self.root.glob('*.f32'))), 4)
        self.assertEqual(receipt['cases'][0]['summary']['p50_ns']['ratio'], 1)

    def test_output_change_is_a_failed_gate(self):
        self.executable('candidate', '2.0')
        self.assertFalse(self.run_models())

    def test_identical_nonfinite_values_are_a_failed_gate(self):
        self.executable('baseline', "float('nan')")
        self.executable('candidate', "float('nan')")
        self.assertFalse(self.run_models())

    def test_missing_output_is_a_failed_gate(self):
        self.executable('candidate', '1.0', output=False)
        self.assertFalse(self.run_models())

    def test_stale_outputs_cannot_make_a_missing_write_pass(self):
        self.assertTrue(self.run_models())
        self.executable('candidate', '1.0', output=False)
        self.assertFalse(self.run_models())

    def test_unbalanced_protocol_is_rejected(self):
        config = json.loads(self.config.read_text())
        config['arm_order'] = 'AAB'
        self.config.write_text(json.dumps(config))
        with self.assertRaises(ValueError):
            self.run_models()

    def test_reference_output_uses_versioned_protocol(self):
        import struct
        (self.root / 'reference.bin').write_bytes(struct.pack('<f', 1) + b'guard')
        case = dict(name='packed.rows', kernel='packed_rows', grid=[1, 1, 1], threads=[32, 1, 1],
                    arguments=[dict(kind='reference_output', file='reference.bin', written_bytes=4)])
        self.config.write_text(json.dumps(dict(cases=[case])))
        self.assertTrue(qualification.pack_cuda(self.args))
        raw = (self.root / 'cuda-cases.bin').read_bytes()
        self.assertEqual(struct.unpack_from('<II', raw), (0x4b464332, 1))
        role_offset = 8 + 4 + len(case['name']) + 4 + len(case['kernel']) + 32
        self.assertEqual(struct.unpack_from('<IQQ', raw, role_offset), (3, 4, 9))
        self.assertEqual(raw[role_offset + 20:], (self.root / 'reference.bin').read_bytes())
        case['arguments'][0]['kind'] = 'output'
        self.config.write_text(json.dumps(dict(cases=[case])))
        self.assertTrue(qualification.pack_cuda(self.args))
        self.assertEqual(struct.unpack_from('<I', (self.root / 'cuda-cases.bin').read_bytes())[0], 0x4b464331)


if __name__ == '__main__':
    unittest.main()
