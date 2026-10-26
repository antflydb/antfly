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

import importlib.util
from pathlib import Path
import unittest
from unittest import mock

import numpy as np


SPEC = importlib.util.spec_from_file_location(
    "attention_matching_calibrate",
    Path(__file__).with_name("attention_matching_calibrate.py"))
am = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(am)


def normalized(rows):
    values = np.asarray(rows, dtype=np.float64)
    return values / values.sum(axis=1, keepdims=True)


def group(start, count, layer, head):
    return {"consumer_layer": layer, "query_head": head,
            "rows": np.arange(start, start + count, dtype=np.int64)}


class MinimaxSelectorTests(unittest.TestCase):
    def test_fit_head_keeps_runtime_gates_and_reports_payload_only_savings(self):
        eps = 1e-8
        common = np.tile(normalized([[.9, .1, eps]]), (200, 1))
        rare = normalized([[eps, eps, 1.]])
        validation = np.vstack((common[:1], rare))
        probabilities = np.vstack((common, rare, validation))
        keys = np.eye(3, dtype=np.float16).reshape(3, 1, 3)
        values = np.asarray([[1., 0., 0.], [1., 0., 0.], [0., 1., 0.]],
                            dtype=np.float16).reshape(3, 1, 3)
        queries = np.log(probabilities).astype(np.float32).reshape(1, len(probabilities), 3)
        _, _, exact_outputs, partitions = am.exact_attention(
            keys[:, 0], values[:, 0], queries[0], 1.)
        arrays = {"keys": keys, "values": values, "queries": queries,
                  "exact_outputs": exact_outputs.reshape(1, *exact_outputs.shape),
                  "exact_log_partition": partitions.reshape(1, -1)}
        capture = {"rows": 3, "heads": 1, "dim": 3, "training_rows": 201,
                   "validation_rows": 2, "owner_layer": 4, "scale": 1.,
                   "strata": [
                       {"kv_head": 0, "consumer_layer": 4, "query_head": 0,
                        "training_start": 0, "training_count": 200,
                        "validation_start": 0, "validation_count": 1},
                       {"kv_head": 0, "consumer_layer": 4, "query_head": 1,
                        "training_start": 200, "training_count": 1,
                        "validation_start": 1, "validation_count": 1},
                   ]}
        report = am.fit_head({"schema_version": 1, "capture": capture}, arrays, 0, 2,
                             "relative_minimax_ridge_peak")
        self.assertTrue(report["accepted"], report)
        self.assertLessEqual(report["training"]["mass_max"], .05)
        self.assertLessEqual(report["validation"]["mass_max"], .05)
        self.assertLessEqual(report["validation"]["output_relative_rmse"], .10)
        self.assertFalse(report["kv_payload_savings"]["includes_session_or_fitter_overhead"])
        self.assertEqual(report["kv_payload_savings"]["scope"],
                         "per_head_key_value_and_mass_bias_only")
        self.assertFalse(report["solver_identity"]["validation_visible_to_selection"])

    def test_rare_training_stratum_defeats_rms_energy(self):
        eps = 1e-8
        common = np.tile(normalized([[.9, .1, eps]]), (200, 1))
        rare = normalized([[eps, eps, 1.]])
        training = np.vstack((common, rare))
        groups = [group(0, 200, 4, 0), group(200, 1, 4, 1)]

        rms = np.sort(np.argsort(-(training ** 2).sum(axis=0), kind="stable")[:2])
        selected, weights, receipt = am.minimax_peak_selection(training, groups, 2)

        self.assertEqual(rms.tolist(), [0, 1])
        self.assertEqual(selected.tolist(), [0, 2])
        self.assertTrue(np.isfinite(weights).all())
        self.assertTrue(np.all(weights > 0))
        self.assertLessEqual(np.max(np.abs(training[:, selected] @ weights - 1)), am.MASS_LIMIT)
        stored_weights = np.exp(np.log(weights).astype(np.float32).astype(np.float64))
        self.assertLessEqual(np.max(np.abs(training[:, selected] @ stored_weights - 1)), am.MASS_LIMIT)
        self.assertEqual(receipt["information_boundary"],
                         "training_rows_and_predeclared_training_strata_only")
        self.assertTrue(all(a + 32 * am.MINIMAX_PRIMAL_TOLERANCE >= b
                            for a, b in zip(receipt["objective_history"],
                                            receipt["objective_history"][1:])))
        self.assertEqual(receipt["lp_solve_limit"], am.MINIMAX_MAX_LP_SOLVES)

    def test_unseen_validation_row_cannot_change_frozen_selection(self):
        eps = 1e-8
        training = normalized([[.9, .1, eps], [.8, .2, eps]] * 10)
        groups = [group(0, len(training), 4, 0)]
        first = am.minimax_peak_selection(training.copy(), groups, 2)
        # These mutually incompatible held-out sets never enter the selector.
        held_a = normalized([[eps, eps, 1.]])
        held_b = normalized([[.45, .55, eps]])
        second = am.minimax_peak_selection(training.copy(), groups, 2)
        self.assertEqual(first[0].tolist(), [0, 1])
        self.assertTrue(np.array_equal(first[0], second[0]))
        self.assertTrue(np.array_equal(first[1], second[1]))
        mass_a = held_a[:, first[0]] @ first[1]
        mass_b = held_b[:, first[0]] @ first[1]
        self.assertGreater(float(np.max(np.abs(mass_a - 1))), am.MASS_LIMIT)
        self.assertLessEqual(float(np.max(np.abs(mass_b - 1))), am.MASS_LIMIT)

    def test_ties_are_low_index_stable_and_receipt_is_bounded(self):
        training = normalized([[1., 1., .2, .2], [1., 1., .2, .2]])
        groups = [group(0, 2, 9, 0)]
        selected, weights, receipt = am.minimax_peak_selection(training, groups, 2)
        self.assertEqual(selected.tolist(), [0, 2])
        self.assertEqual(len(weights), 2)
        self.assertLessEqual(receipt["lp_solves"], receipt["lp_solve_limit"])
        self.assertEqual(receipt["rows"], 2)
        self.assertEqual(receipt["groups"], 1)

    def test_single_stratum_batches_keep_lp_calls_within_declared_bound(self):
        training = np.random.default_rng(7).dirichlet(np.ones(32), size=12)
        selected, _, receipt = am.minimax_peak_selection(
            training, [group(0, len(training), 9, 0)], 16)
        self.assertEqual(len(selected), 16)
        self.assertLessEqual(receipt["lp_solves"], 2 + int(np.ceil((16 - 1) / am.MINIMAX_BATCH)))

    def test_invalid_training_partitions_and_columns_fail_closed(self):
        valid = normalized([[.8, .2, 0.], [.2, .8, 0.]])
        cases = [
            (np.asarray([[np.nan, 0., 1.], [.2, .8, 0.]]), [group(0, 2, 1, 0)]),
            (np.asarray([[.8, -.1, .3], [.2, .8, 0.]]), [group(0, 2, 1, 0)]),
            (np.asarray([[.8, .3, 0.], [.2, .8, 0.]]), [group(0, 2, 1, 0)]),
            (valid, [group(0, 1, 1, 0)]),
            (valid, [group(0, 2, 1, 0), group(1, 1, 1, 1)]),
        ]
        for probabilities, groups in cases:
            with self.subTest(probabilities=probabilities, groups=groups):
                with self.assertRaises(ValueError):
                    am.minimax_peak_selection(probabilities, groups, 1)

        duplicates = normalized([[1., 1., 0.], [1., 1., 0.]])
        with self.assertRaisesRegex(ValueError, "unique columns exhausted"):
            am.minimax_peak_selection(duplicates, [group(0, 2, 1, 0)], 2)

    def test_solver_failure_and_nonfinite_result_are_rejected(self):
        matrix = normalized([[.8, .2], [.2, .8]])
        failed = mock.Mock(success=False, status=2, x=None)
        with mock.patch.object(am, "linprog", return_value=failed):
            with self.assertRaisesRegex(am.NumericalFailure, "LP failed"):
                am.chebyshev_mass_fit(matrix)

        bad = mock.Mock(success=True, status=0, x=np.asarray([np.nan, 1., 0.]), nit=1,
                        message="bad")
        with mock.patch.object(am, "linprog", return_value=bad):
            with self.assertRaisesRegex(am.NumericalFailure, "LP failed"):
                am.chebyshev_mass_fit(matrix)

    def test_group_builder_uses_training_ranges_only(self):
        capture = {"training_rows": 3, "validation_rows": 99,
                   "strata": [
                       {"kv_head": 0, "consumer_layer": 4, "query_head": 0,
                        "training_start": 0, "training_count": 2,
                        "validation_start": 98, "validation_count": 1},
                       {"kv_head": 0, "consumer_layer": 8, "query_head": 0,
                        "training_start": 2, "training_count": 1,
                        "validation_start": 0, "validation_count": 98},
                   ]}
        groups = am.training_strata_groups(capture, 0)
        self.assertEqual([x["rows"].tolist() for x in groups], [[0, 1], [2]])


if __name__ == "__main__":
    unittest.main()
