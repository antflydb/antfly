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

import unittest
from unittest.mock import patch

import benchmark_cpu as bench


class ThreadBudgetTests(unittest.TestCase):
    def test_blas_retains_requested_threads_with_a_valid_native_fallback_budget(self):
        for threads in (1, 2, 8, 9, 16, 32):
            with self.subTest(threads=threads):
                env = bench.thread_environment(threads)
                for name in bench.THREAD_ENV:
                    self.assertEqual(int(env[name]), threads)
                native_threads = int(env["ANTFLY_INFERENCE_CPU_THREADS"])
                self.assertTrue(1 <= native_threads <= 8)
                if threads <= 8:
                    self.assertEqual(native_threads, threads)

    def test_blas_readiness_accepts_a_separate_native_fallback_budget(self):
        bundle = {"model_id": "model", "revision": "revision", "files": {}}
        ready = {
            "event": "ready",
            "arm": "native",
            "scope": bench.SCOPE,
            "timing_boundary": bench.TIMING_BOUNDARY,
            "model_id": "model",
            "revision": "revision",
            "model_files": {},
            "dtype": "float32",
            "threads": 32,
            "qualification": False,
            "build_mode": "fast",
            "scheduler": "serial_io",
            "cases_sha256": "hash",
            "system_blas": True,
            "effective_cpu_threads": 8,
        }
        with patch.object(bench.oracle, "sha256_file", return_value="hash"):
            bench.checked_ready("native", ready, bundle, None, 32)


if __name__ == "__main__":
    unittest.main()
