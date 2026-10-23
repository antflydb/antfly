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


if __name__ == '__main__':
    unittest.main()
