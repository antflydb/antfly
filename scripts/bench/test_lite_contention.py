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

import queue
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import lite_contention as bench


def admission(*samples):
    histogram = [0] * 64
    for value in samples:
        histogram[max(0, value.bit_length() - 1)] += 1
    return dict(
        count=len(samples),
        ns=sum(samples),
        max_ns=max(samples, default=0),
        log2_ns=histogram,
    )


class ContentionReportTests(unittest.TestCase):
    def test_unequal_worker_windows_and_busy_calls_are_not_successful_throughput(self):
        workers = [
            dict(
                started=1,
                ended=3,
                success_latency_ns=[10, 20],
                admission=admission(10, 20),
                busy_latency_ns=[100],
                proofs=dict(
                    allocator_proof_walks=1,
                    allocator_proof_failures=0,
                    allocator_proof_ns=1000,
                ),
                cpu_seconds=0.25,
                voluntary_context_switches=4,
                involuntary_context_switches=2,
            ),
            dict(
                started=2,
                ended=4,
                success_latency_ns=[30],
                admission=admission(30),
                busy_latency_ns=[200],
                proofs=dict(
                    allocator_proof_walks=2,
                    allocator_proof_failures=1,
                    allocator_proof_ns=2000,
                ),
                cpu_seconds=0.5,
                voluntary_context_switches=6,
                involuntary_context_switches=3,
            ),
        ]
        result = bench.aggregate(workers)
        self.assertEqual(result["elapsed_seconds"], 3)
        self.assertEqual(result["successful_calls_per_second"], 1)
        self.assertEqual(result["busy_fraction"], 0.4)
        self.assertEqual(result["success_p95_ns"], 30)
        self.assertEqual(result["busy_p99_ns"], 200)
        self.assertEqual(result["proof_walks_per_second"], 1)
        self.assertEqual(result["proof_walks_per_success"], 1)
        self.assertEqual(result["allocator_proof_ns"], 3000)
        self.assertEqual(result["allocator_proof_failures"], 1)
        self.assertEqual(result["cpu_seconds"], 0.75)
        self.assertEqual(result["involuntary_context_switches"], 5)
        self.assertEqual(result["admission_count"], 3)
        self.assertEqual(result["admission_ns"], 60)
        self.assertEqual(result["admission_p95_lower_ns"], 16)
        self.assertEqual(result["admission_p95_upper_ns"], 31)

    def test_no_successful_calls_has_no_success_latency(self):
        self.assertIsNone(bench.percentile([], 99))
        self.assertEqual(bench.percentile(list(range(1, 101)), 95), 95)

    def test_worker_reports_the_last_successful_write_and_proof_delta(self):
        class Database:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def batch_json(self, payload):
                self.payload = payload

        database = Database()
        api = SimpleNamespace(open=lambda *a, **k: database, BusyError=RuntimeError)
        ready, results = queue.Queue(), queue.Queue()
        zero = dict.fromkeys(bench.PROOF_FIELDS, 0)
        zero.update(
            mutation_admission_count=0,
            mutation_admission_ns=0,
            mutation_admission_max_ns=0,
            mutation_admission_log2_ns=[0] * 64,
        )
        final = dict(
            allocator_proof_walks=1, allocator_proof_failures=0, allocator_proof_ns=500
        )
        final.update(
            mutation_admission_count=1,
            mutation_admission_ns=20,
            mutation_admission_max_ns=20,
            mutation_admission_log2_ns=admission(20)["log2_ns"],
        )
        config = dict(
            library="fake",
            database="fresh",
            no_sync=True,
            timeout_ms=5,
            watchdog=1,
            seconds=1,
            workload="churn",
        )
        with (
            patch.object(bench, "sdk", return_value=api),
            patch.object(bench, "measurement_stats", side_effect=[zero, final]),
            patch.object(bench.time, "monotonic", side_effect=[1, 1.1, 2.1, 2.2]),
            patch.object(bench.time, "perf_counter_ns", side_effect=[100, 150]),
        ):
            bench.native_worker(
                config, 4, ready, SimpleNamespace(wait=lambda _: True), results
            )
        result = results.get_nowait()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["success_latency_ns"], [50])
        self.assertEqual(result["last_write"], dict(key="worker-4-0", sequence=0))
        self.assertEqual(result["proofs"]["allocator_proof_ns"], 500)
        self.assertIn("worker-4-0", database.payload["inserts"])

    def test_worker_setup_failure_notifies_parent_before_start_barrier(self):
        ready, results = queue.Queue(), queue.Queue()
        with patch.object(bench, "sdk", side_effect=RuntimeError("missing library")):
            bench.native_worker({"library": "missing"}, 7, ready, None, results)
        notice = ready.get_nowait()
        self.assertEqual(notice["index"], 7)
        self.assertEqual(notice["status"], "error")
        self.assertIn("missing library", notice["diagnostic"])
        self.assertEqual(results.get_nowait(), notice)


if __name__ == "__main__":
    unittest.main()
