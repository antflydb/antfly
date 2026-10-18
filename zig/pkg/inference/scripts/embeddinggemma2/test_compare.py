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

import math
import io
import sys
from pathlib import Path
from unittest import mock
import copy
import unittest

if __package__:
    from . import compare
else:
    import compare


class ComparisonContracts(unittest.TestCase):
    def test_post_declares_bounded_inference_budget_and_transport_timeout(self):
        with mock.patch.object(
            compare.urllib.request, "urlopen", return_value=io.BytesIO(b'{"ok":true}')
        ) as opened:
            self.assertEqual(
                compare.post(
                    "http://localhost", "/embed", {"input": ["x"]}, timeout=6.3278
                ),
                {"ok": True},
            )
        request = opened.call_args.args[0]
        self.assertEqual(request.get_header("X-antfly-inference-timeout-ms"), "6327")
        self.assertEqual(opened.call_args.kwargs["timeout"], 6.3278)
        self.assertEqual(request.get_header("Content-type"), "application/json")

    def test_post_rejects_invalid_budget_before_network_io(self):
        with mock.patch.object(compare.urllib.request, "urlopen") as opened:
            for budget in (0, -1, math.inf, math.nan, 3600.001):
                with self.subTest(budget=budget):
                    with self.assertRaises(ValueError):
                        compare.post("http://localhost", "/embed", {}, timeout=budget)
            opened.assert_not_called()

    def test_deadline_probe_rejects_unverified_worker_disconnect(self):
        with mock.patch("sys.path", [str(Path(__file__).resolve().parent), *sys.path]):
            import video_deadline_qualification as probe

        self.assertEqual(
            probe.restart_generation(
                "inference worker stopped unexpectedly; restarting generation=2 delay_ms=100 term=.{ .exited = 86 }"
            ),
            2,
        )
        for text in (
            "",
            "restarting generation=2 exited = 86",
            "inference worker stopped unexpectedly; restarting generation=2 exited = 1",
            "inference worker stopped unexpectedly; restarting generation=1 exited = 86",
        ):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    probe.restart_generation(text)

    def test_case_selection_and_long_sample_controls(self):
        short = {"name": "short", "text": "x", "token_ids": [1] * 512}
        long = {"name": "long", "text": "x", "token_ids": [1] * 8192}
        media = {"name": "image", "token_ids": [1]}
        suite = {"cases": [short, long, media]}
        self.assertEqual(
            compare.select_cases(suite, None, True)["cases"], [short, long]
        )
        self.assertEqual(compare.select_cases(suite, ["long"], True)["cases"], [long])
        for names in (["missing"], ["short", "short"], ["image"]):
            with self.assertRaises(ValueError):
                compare.select_cases(suite, names, True)
        self.assertEqual(compare.iterations(long, 20), 1)
        self.assertEqual(compare.iterations(long, 20, 5), 5)
        self.assertEqual(compare.iterations(short, 20, 5), 20)
        self.assertEqual(compare.case_warmups(long, 2), 2)
        self.assertEqual(compare.case_warmups(long, None), 0)

    def test_strict_vector_gate_and_encoder_scope(self):
        actual = [math.sqrt(1 - 2e-5**2), 2e-5]
        compare.assert_agreement(actual, [1.0, 0.0])
        with self.assertRaises(ValueError):
            compare.assert_agreement(actual, [1.0, 0.0], 1e-5)
        result = {
            "suite_sha256": "s",
            "checkpoint_receipt_sha256": "w",
            "precision": "float32",
            "status": "pass",
            "measurement_mode": "encoder",
            "cases": {"x": {"vector": [1.0, 0.0]}},
            "decision": {"status": "not_run"},
        }
        self.assertEqual(
            compare.compare_reference(result, result, 1e-5)["decision_status"],
            "not_run",
        )
        bad = copy.deepcopy(result)
        bad["measurement_mode"] = "default"
        with self.assertRaises(ValueError):
            compare.compare_reference(result, bad)

    def test_percentiles_retain_sample_count_and_nearest_rank_tail(self):
        result = compare.timing([4.0, 1.0, 3.0, 2.0])
        self.assertEqual(result["count"], 4)
        self.assertEqual(result["p50_seconds"], 2.5)
        self.assertEqual(result["p95_seconds"], 4.0)

    def test_bad_timings_cannot_be_reported_as_measurements(self):
        for values in ([], [0.0], [-1.0], [math.inf], [math.nan]):
            with self.assertRaises(ValueError):
                compare.timing(values)

    def test_numerical_gate_checks_error_norm_and_direction(self):
        compare.assert_agreement([1.0, 0.0], [1.0, 0.0])
        for actual in ([2.0, 0.0], [-1.0, 0.0], [1.0, 0.1], [math.nan, 0.0]):
            with self.assertRaises(ValueError):
                compare.assert_agreement(actual, [1.0, 0.0])

    def test_shape_and_zero_vectors_fail_closed(self):
        for actual, expected in (([], []), ([1.0], [1.0, 0.0]), ([0.0], [1.0])):
            with self.assertRaises(ValueError):
                compare.agreement(actual, expected)

    def test_retrieval_preserves_full_rankings_and_cosine_scores(self):
        suite = {
            "retrieval": {
                "queries": ["q"],
                "documents": ["b", "a", "c"],
                "purpose": "parity",
            }
        }
        cases = {
            "q": {"vector": [1.0, 0.0]},
            "a": {"vector": [1.0, 0.0]},
            "b": {"vector": [0.0, 1.0]},
            "c": {"vector": [-1.0, 0.0]},
        }
        result = compare.retrieval_result(suite, cases)
        self.assertEqual(result["rankings"]["q"], ["a", "b", "c"])
        self.assertEqual(result["scores"]["q"], {"a": 1.0, "b": 0.0, "c": -1.0})
        self.assertIsNone(compare.retrieval_result({}, cases))

    def test_matched_reference_rejects_identity_vector_rank_and_route_changes(self):
        expected = {
            "suite_sha256": "suite",
            "checkpoint_receipt_sha256": "weights",
            "precision": "float32",
            "status": "pass",
            "cases": {"a": {"vector": [1.0, 0.0]}},
            "decision": {
                "choice": "a",
                "similarities": {"a": 0.8, "b": 0.2},
                "margin": 0.6,
            },
            "retrieval": {
                "rankings": {"q": ["a", "b"]},
                "scores": {"q": {"a": 0.8, "b": 0.2}},
            },
        }
        actual = copy.deepcopy(expected)
        actual["decision"] = {"answer": actual["decision"]}
        self.assertEqual(
            compare.compare_reference(actual, expected)["max_retrieval_score_error"], 0
        )
        for field, value in (
            ("suite_sha256", "other"),
            ("checkpoint_receipt_sha256", "other"),
            ("precision", "float16"),
        ):
            bad = copy.deepcopy(actual)
            bad[field] = value
            with self.assertRaises(ValueError):
                compare.compare_reference(bad, expected)
        bads = [copy.deepcopy(actual) for _ in range(4)]
        bads[0]["cases"]["a"]["vector"] = [-1.0, 0.0]
        bads[1]["decision"]["answer"]["choice"] = "b"
        bads[2]["decision"]["answer"]["similarities"]["a"] = 0.9
        bads[3]["retrieval"]["rankings"]["q"] = ["b", "a"]
        for bad in bads:
            with self.assertRaises(ValueError):
                compare.compare_reference(bad, expected)


class CudaRouteTests(unittest.TestCase):
    def test_qualification_requires_executed_routes(self):
        routes = compare.cuda_routes(
            "extended_flash_launches_total=3 extended_flash_launches_total=9 embedding_graph_replays_total=4 native_vision_launches_total=2 media_batches_total=1 fused_backbone_launches_total=96 packed_ple_groups_total=2 packed_head_launches_total=24 packed_gelu_launches_total=24 tuned_gemm_calls_total=100 vision_residual_launches_total=32"
        )
        self.assertEqual(routes["extended_flash"], 9)
        with self.assertRaisesRegex(ValueError, "tuned_gemm"):
            compare.require_cuda_routes(
                compare.cuda_routes(
                    "gemm_tuning_declines_total=9 cublaslt_matmuls_total=9"
                ),
                ["tuned_gemm"],
            )
        compare.require_cuda_routes(
            routes,
            [
                "extended_flash",
                "graph_replay",
                "native_vision",
                "media_batch",
                "fused_backbone",
                "packed_ple",
                "packed_heads",
                "packed_gelu",
                "tuned_gemm",
                "vision_residual",
            ],
        )
        with self.assertRaisesRegex(ValueError, "fused_vision"):
            compare.require_cuda_routes(routes, ["fused_vision"])
        self.assertEqual(
            compare.cuda_routes("extended_flash_launches_total=9garbage")[
                "extended_flash"
            ],
            0,
        )


class PrecisionTests(unittest.TestCase):
    def test_bf16_reference_requires_declared_pooling(self):
        result = {
            "suite_sha256": "suite",
            "checkpoint_receipt_sha256": "weights",
            "precision": "bfloat16",
            "status": "pass",
            "cases": {},
            "measurement_mode": "encoder",
            "decision": {"status": "not_run"},
        }
        with self.assertRaisesRegex(ValueError, "F32 pooling"):
            compare.compare_reference(result, result)
        result["pooling_precision"] = "float32"
        self.assertEqual(
            compare.compare_reference(result, result)["decision_status"], "not_run"
        )

    def test_backend_precision_is_explicit_and_cannot_cross_recipes(self):
        for backend in ("cuda", "torch-cuda"):
            self.assertEqual(compare.execution_precision(backend), "bfloat16")
            with self.assertRaises(ValueError):
                compare.execution_precision(backend, "float32")
        for backend in ("native", "metal", "cpu", "mps"):
            self.assertEqual(compare.execution_precision(backend), "float32")
            with self.assertRaises(ValueError):
                compare.execution_precision(backend, "bfloat16")


if __name__ == "__main__":
    unittest.main()
