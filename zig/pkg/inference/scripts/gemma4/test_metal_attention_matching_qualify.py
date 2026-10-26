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

"""Independent-oracle and binary-boundary tests; no synthetic serving claims."""
import argparse
import copy
import json
from pathlib import Path
import tempfile
import unittest
import numpy as np
import attention_matching_calibrate as am
import metal_attention_matching_qualify as metal
import release_build_fixture
import test_attention_matching_calibrate as fixtures


class MetalQualificationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CalibrationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        _, self.meta, self.arrays = self.fixture.capture(values=[1, 1, 1, 1])
        self.result = am.fit_head(self.meta, self.arrays, 0, 2, 'relative_cd_ridge_rms')
        self.keys = np.zeros((2, 1), dtype='<f2')
        self.values = np.ones((2, 1), dtype='<f2')
        self.bias = np.full(2, np.log(2), dtype='<f4')
        self.report = {'failure_flags': 0, 'iterations': 128, 'evaluated': 7, 'validation_groups': 1,
                       'training_mass_relative_max_error': 0., 'mass_relative_max_error': 0.,
                       'output_relative_rmse': 0., 'max_consumer_output_relative_rmse': 0.}

    def evaluate(self):
        return metal.evaluate_candidate(self.meta, self.arrays, self.result, self.keys, self.values, self.bias, self.report)

    def test_valid_candidate_is_checked_by_independent_oracle(self):
        result = self.evaluate()
        self.assertLess(result['validation']['mass_max'], 1e-7)
        self.assertEqual(len(result['consumers']), 1)

    def test_export_preserves_actual_f16_kv_f32_query_bytes(self):
        path = self.fixture.root / 'input.bin'
        metal.export_input(path, self.meta, self.arrays, self.result)
        data = path.read_bytes()
        header = metal.HEADER.unpack(data[:metal.HEADER.size])
        self.assertEqual(header, (b'AMFIT001', 4, 1, 3, 2, 2, 2, 1.))
        self.assertEqual(data[metal.HEADER.size:], b''.join(np.ascontiguousarray(x).tobytes() for x in (
            self.arrays['keys'][:, 0], self.arrays['values'][:, 0], self.arrays['queries'][0, :3], self.arrays['queries'][0, 3:])))
        with self.assertRaises(FileExistsError):
            metal.export_input(path, self.meta, self.arrays, self.result)

    def test_teacher_queries_never_enter_gpu_fitting_and_have_separate_gates(self):
        _, self.meta, self.arrays = self.fixture.capture("teacher", "evaluation", schema=2,
                                                       values=[1, 1, 1, 1], evaluation=[-10, 10])
        self.result = am.fit_head(self.meta, self.arrays, 0, 2, 'relative_cd_ridge_rms')
        path = self.fixture.root / 'teacher-input.bin'
        metal.export_input(path, self.meta, self.arrays, self.result)
        data = path.read_bytes()
        self.assertEqual(len(data), metal.HEADER.size + 4 * 2 * 2 + 5 * 4)
        self.assertEqual(data[-2 * 4:], np.ascontiguousarray(self.arrays['queries'][0, 3:5]).tobytes())
        measured = self.evaluate()
        self.assertLess(measured['evaluation']['mass_max'], 1e-7)
        self.assertEqual(len(measured['evaluation_consumers']), 1)
        self.arrays['exact_outputs'][0, 5:] = 2
        with self.assertRaisesRegex(ValueError, 'independent teacher evaluation'):
            self.evaluate()

    def test_export_rejects_exact_rejected_and_unsupported_recipes(self):
        for change in ({'exact': True}, {'accepted': False}, {'recipe': 'max_shift_qr'}):
            result = dict(self.result, **change)
            with self.assertRaisesRegex(ValueError, 'unsupported or rejected'):
                metal.export_input(self.fixture.root / 'invalid.bin', self.meta, self.arrays, result)

    def test_offline_query_envelope_does_not_expand_runtime_fitter_limits(self):
        meta = copy.deepcopy(self.meta)
        meta['capture'].update(training_rows=4095, validation_rows=2)
        path = self.fixture.root / 'too-many-queries.bin'
        with self.assertRaisesRegex(ValueError, 'Metal fitter row limit'):
            metal.export_input(path, meta, self.arrays, self.result)
        self.assertFalse(path.exists())

    def test_nonfinite_and_wrong_selected_keys_fail(self):
        self.values[0, 0] = np.inf
        with self.assertRaisesRegex(ValueError, 'nonfinite stored'):
            self.evaluate()
        self.values[0, 0] = 1
        self.keys[0, 0] = 1
        with self.assertRaisesRegex(ValueError, 'selected keys'):
            self.evaluate()

    def test_actual_mass_and_output_fail_even_if_device_report_claims_pass(self):
        self.bias += .2
        with self.assertRaisesRegex(ValueError, 'consumer gate'):
            self.evaluate()
        self.bias = np.full(2, np.log(2), dtype='<f4')
        self.values[:] = 2
        with self.assertRaisesRegex(ValueError, 'consumer gate'):
            self.evaluate()

    def test_gpu_report_fields_and_metrics_cannot_override_oracle(self):
        for name, value in (('failure_flags', 4), ('evaluated', 1), ('iterations', 1),
                            ('validation_groups', 2), ('mass_relative_max_error', float('nan')),
                            ('max_consumer_output_relative_rmse', .01)):
            old = self.report[name]
            self.report[name] = value
            with self.assertRaises(ValueError):
                self.evaluate()
            self.report[name] = old

    def test_stale_fp64_metrics_and_consumer_mapping_fail(self):
        original = copy.deepcopy(self.result)
        self.result['validation']['mass_max'] = .01
        with self.assertRaisesRegex(ValueError, 'fit metric mismatch'):
            self.evaluate()
        self.result = original
        self.result['consumers'][0]['query_head'] = 99
        with self.assertRaisesRegex(ValueError, 'consumer ordering'):
            self.evaluate()

    def test_full_release_build_receipt_passes_build_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, receipt = root / 'context-tests', root / 'build.json'
            binary.write_bytes(b'fixture executable')
            release_build_fixture.write(root, receipt, binary)
            numerical = root / 'numerical.json'; numerical.write_text(json.dumps({}))
            args = argparse.Namespace(timeout=1, max_rss_mib=32, binary=binary,
                                      build_receipt=receipt, numerical=numerical,
                                      output_dir=root / 'output')
            with self.assertRaisesRegex(ValueError, 'unsupported or stale numerical receipt'):
                metal.qualify(args)
            self.assertFalse(args.output_dir.exists())


if __name__ == '__main__':
    unittest.main()
