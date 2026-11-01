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

import copy
import json
import unittest

from compare_reader_parity import compare


def capture(native, concurrency, seconds=10):
    pages = ["1" * 64, "2" * 64] * (2 if concurrency > 1 else 1)
    rows = [
        {
            "http_status": 200,
            "items": 1,
            "tail_pass": True,
            "statuses": ["completed"],
            "image_sha256": digest,
            "text_sha256": ("3" if digest.startswith("1") else "4") * 64,
            "execution": {"native_items": 1, "max_native_step_items": concurrency},
        }
        for digest in pages
    ]
    return {
        "passed": True,
        "api_shape": "antfly-reader" if native else "openai-chat",
        "concurrency": concurrency,
        "model_revision": "c" * 40,
        "benchmark_script_sha256": "d" * 64,
        "max_tokens": 2048,
        "warmup_rounds": 1,
        "warmup_observations": [{"seconds": 1000, "requests": copy.deepcopy(rows)}],
        "observations": [
            {"seconds": seconds, "requests": copy.deepcopy(rows)} for _ in range(2)
        ],
    }


class ReaderParityTests(unittest.TestCase):
    def setUp(self):
        self.captures = [
            capture(True, 1, 9),
            capture(True, 4, 8),
            capture(False, 1),
            capture(False, 4),
        ]

    def result(self):
        return compare(*self.captures)

    def test_exact_parity_and_throughput_exclude_cold_warmup_time(self):
        result = self.result()
        self.assertTrue(result["passed"])
        self.assertTrue(all(result["correctness"].values()))
        self.assertEqual(result["timings"]["concurrent"]["native_median_seconds"], 8)
        self.assertEqual(
            result["timings"]["concurrent"]["native_pages_per_second"], 0.5
        )

    def test_matching_reference_batch_does_not_hide_serial_batch_drift(self):
        for report in (self.captures[1], self.captures[3]):
            for observation in report["warmup_observations"] + report["observations"]:
                observation["requests"][0]["text_sha256"] = "a" * 64
                observation["requests"][2]["text_sha256"] = "a" * 64
        result = self.result()
        self.assertFalse(result["passed"])
        self.assertTrue(result["correctness"]["concurrent_exact"])
        self.assertTrue(result["correctness"]["serial_exact"])
        self.assertFalse(result["correctness"]["reference_batch_stable"])
        self.assertFalse(result["correctness"]["native_batch_stable"])

    def test_one_divergent_duplicate_or_round_is_not_an_exact_match(self):
        self.captures[3]["observations"][1]["requests"][0]["text_sha256"] = "a" * 64
        self.assertFalse(self.result()["correctness"]["concurrent_exact"])

    def test_warmup_output_drift_is_not_excluded_with_its_timing(self):
        self.captures[1]["warmup_observations"][0]["requests"][0]["text_sha256"] = (
            "a" * 64
        )
        self.assertFalse(self.result()["correctness"]["concurrent_exact"])
        self.assertFalse(self.result()["passed"])

    def test_mismatched_warmup_work_is_rejected(self):
        self.captures[1]["warmup_rounds"] = 2
        self.captures[1]["warmup_observations"] *= 2
        with self.assertRaisesRegex(ValueError, "warmup_rounds"):
            self.result()

    def test_speed_alone_cannot_qualify(self):
        for observation in self.captures[1]["observations"]:
            observation["requests"][0]["text_sha256"] = "a" * 64
        self.assertFalse(self.result()["passed"])

    def test_correctness_alone_cannot_qualify(self):
        for observation in self.captures[1]["observations"]:
            observation["seconds"] = 11
        result = self.result()
        self.assertTrue(all(result["correctness"].values()))
        self.assertFalse(result["passed"])
        self.assertAlmostEqual(
            result["timings"]["concurrent"]["native_over_reference_latency"], 1.1
        )

    def test_missing_warmup_and_mismatched_workloads_are_rejected(self):
        mutations = [
            ("warmup_rounds", 0),
            ("model_revision", "b" * 40),
            ("max_tokens", 1024),
            ("benchmark_script_sha256", "b" * 64),
            ("concurrency", 2),
            ("observations", []),
            ("passed", False),
        ]
        for key, value in mutations:
            with self.subTest(key=key):
                altered = copy.deepcopy(self.captures)
                altered[1][key] = value
                with self.assertRaises(ValueError):
                    compare(*altered)
        altered = copy.deepcopy(self.captures)
        for observation in (
            altered[3]["observations"] + altered[3]["warmup_observations"]
        ):
            observation["requests"].reverse()
        with self.assertRaisesRegex(ValueError, "ordered"):
            compare(*altered)

    def test_incomplete_results_and_singleton_execution_fail_closed(self):
        for key, value in (
            ("statuses", ["truncated"]),
            ("tail_pass", False),
            ("http_status", 503),
            ("execution", {"native_items": 1, "max_native_step_items": 1}),
        ):
            with self.subTest(key=key):
                altered = copy.deepcopy(self.captures)
                altered[1]["observations"][0]["requests"][0][key] = value
                with self.assertRaises(ValueError):
                    compare(*altered)
        self.captures[0]["warmup_observations"][0]["requests"][0]["statuses"] = [
            "unresolved"
        ]
        with self.assertRaises(ValueError):
            self.result()

    def test_hash_only_result_does_not_copy_optional_document_text(self):
        self.captures[0]["observations"][0]["requests"][0]["text"] = (
            "PRIVATE OCR CONTENT"
        )
        self.assertNotIn("PRIVATE OCR CONTENT", json.dumps(self.result()))

    def test_nonfinite_or_boolean_timings_are_rejected(self):
        for seconds in (False, float("inf"), float("nan"), -1, 0):
            altered = copy.deepcopy(self.captures)
            altered[0]["observations"][0]["seconds"] = seconds
            with self.assertRaises(ValueError):
                compare(*altered)


if __name__ == "__main__":
    unittest.main()
