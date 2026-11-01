#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

    def model_reference(self, value=1.0, tolerance=.002):
        import struct
        path = self.root / 'oracle.bin'
        raw = struct.pack('<f', value)
        path.write_bytes(raw)
        config = json.loads(self.config.read_text())
        config['cases'][0]['output_reference'] = dict(path=str(path), sha256=qualification.digest(raw), atol=tolerance)
        self.config.write_text(json.dumps(config))
        return path

    def test_model_reference_preserves_default_bitwise_gate_and_checks_both_arms(self):
        self.executable('candidate', '1.001')
        self.assertFalse(self.run_models())
        self.model_reference()
        self.assertTrue(self.run_models())
        receipt = json.loads((self.root / 'receipt.json').read_text())
        self.assertTrue(all(x['reference_pass'] for x in receipt['cases'][0]['arms']))
        self.assertFalse(receipt['cases'][0]['arms'][1]['byte_identical'])
        self.executable('baseline', '1.01')
        self.assertFalse(self.run_models())
        self.executable('baseline', '1.0')
        self.executable('candidate', '1.01')
        self.assertFalse(self.run_models())

    def test_model_reference_rejects_stale_nonfinite_and_invalid_budgets(self):
        for tolerance in (-1, float('nan'), float('inf'), True):
            self.model_reference(tolerance=tolerance)
            with self.assertRaises(ValueError):
                self.run_models()
        self.model_reference(value=float('nan'))
        with self.assertRaises(ValueError):
            self.run_models()
        path = self.model_reference()
        path.write_bytes(b'changed')
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

    def test_float_reference_precision_is_explicit_and_old_protocol_stays_strict(self):
        import struct
        (self.root / 'reference.bin').write_bytes(struct.pack('<f', 1) + b'guard')
        arg = dict(kind='reference_output_f32', file='reference.bin', written_bytes=4,
                   absolute_tolerance=.001, relative_tolerance=.002)
        case = dict(name='float.rows', kernel='float_rows', grid=[1, 1, 1], threads=[32, 1, 1], arguments=[arg])
        self.config.write_text(json.dumps(dict(cases=[case])))
        self.assertTrue(qualification.pack_cuda(self.args))
        raw = (self.root / 'cuda-cases.bin').read_bytes()
        self.assertEqual(struct.unpack_from('<II', raw), (0x4b464333, 1))
        offset = 8 + 4 + len(case['name']) + 4 + len(case['kernel']) + 32
        self.assertEqual(struct.unpack_from('<IQQdd', raw, offset), (4, 4, 9, .001, .002))
        for invalid in (-1, float('nan'), float('inf')):
            arg['absolute_tolerance'] = invalid
            self.config.write_text(json.dumps(dict(cases=[case])))
            with self.assertRaises(ValueError):
                qualification.pack_cuda(self.args)
        arg['absolute_tolerance'] = 0
        arg['written_bytes'] = 3
        self.config.write_text(json.dumps(dict(cases=[case])))
        with self.assertRaises(ValueError):
            qualification.pack_cuda(self.args)


class MetalPairingTests(unittest.TestCase):
    @staticmethod
    def arms(effect=1.0):
        # A common frequency transition falls inside one AB/BA pair. There is
        # no source effect when effect=1; independent medians still disagree.
        return [dict(arm=int(version == 'B'), ns=(15 if index < 15 else 11)
                     * (effect if version == 'B' else 1))
                for index, version in enumerate('ABBABAABABBAABBAABBABAABABBAABBA')]

    def test_common_clock_transition_does_not_create_a_source_regression(self):
        import statistics
        arms = self.arms()
        raw = [statistics.median(row['ns'] for row in arms if row['arm'] == arm)
               for arm in (0, 1)]
        self.assertGreater(raw[1] / raw[0], 1.05)
        self.assertEqual(qualification.matched_metal_arm_ratio(arms), 1)

    def test_real_six_percent_regression_survives_clock_transition(self):
        self.assertAlmostEqual(qualification.matched_metal_arm_ratio(self.arms(1.06)), 1.06)
        self.assertGreater(qualification.matched_metal_arm_ratio(self.arms(1.06)), 1.05)

    def test_invalid_or_incomplete_pairs_cannot_be_dropped(self):
        for bad in (self.arms()[:-1], [dict(row, arm=0) for row in self.arms()]):
            with self.assertRaises(ValueError):
                qualification.matched_metal_arm_ratio(bad)
        for value in (0, -1, float('nan'), float('inf')):
            arms = self.arms()
            arms[0]['ns'] = value
            with self.assertRaises(ValueError):
                qualification.matched_metal_arm_ratio(arms)

    def test_failed_reference_blocks_good_pairing_in_both_orders(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / 'ab.json', root / 'ba.json']
            for path, order in zip(paths, ('AB', 'BA')):
                row = dict(name='case', kernel='kernel', pipeline_order=order,
                           arms=self.arms(), exact=True, guards=True,
                           resources_pass=True, reference_outputs=1, reference_pass=order != 'BA')
                path.write_text(json.dumps([row]))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertFalse(qualification.metal_pair(SimpleNamespace(ab=paths[0], ba=paths[1], results=root)))
            saved = json.loads((root / 'metal-paired.json').read_text())
            self.assertEqual(saved[0]['ratio'], 1)
            self.assertFalse(saved[0]['pass_'])


    def test_absent_reference_is_legal_only_with_zero_reference_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / 'ab.json', root / 'ba.json']
            for outputs, flag, accepted in [(0, None, True), (1, None, False), (0, False, False), (1, True, True)]:
                for path, order in zip(paths, ('AB', 'BA')):
                    row = dict(name='case', kernel='kernel', pipeline_order=order,
                               arms=self.arms(), exact=True, guards=True,
                               resources_pass=True, reference_outputs=outputs, reference_pass=flag)
                    path.write_text(json.dumps([row]))
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(qualification.metal_pair(SimpleNamespace(ab=paths[0], ba=paths[1], results=root)), accepted)




if __name__ == '__main__':
    unittest.main()
