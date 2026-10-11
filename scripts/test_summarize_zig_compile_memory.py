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
import tempfile
from pathlib import Path
from summarize_zig_compile_memory import GIB, summarize


class SummaryTests(unittest.TestCase):
    def test_units_and_concurrent_peak_keep_polls_distinct(self):
        trace = "header\n"
        trace += f"now\t0\t10\t{8 * GIB // 1024}\t8192\tzig build-lib --name antfly-storage-kernel -OReleaseFast\t0\n"
        trace += f"now\t0\t11\t{4 * GIB // 1024}\t4096\tzig build-lib --name antfly-runtime-inference\t0\n"
        trace += f"now\t0\t10\t{9 * GIB // 1024}\t9216\tzig build-lib --name antfly-storage-kernel\t1\n"
        report = summarize(trace)
        self.assertEqual(report["sampled_compiler_aggregate_peak_bytes"], 12 * GIB)
        self.assertEqual(
            report["units"]["storage_kernel"]["minimum_reservation_gib"], 12
        )
        self.assertEqual(report["units"]["inference"]["minimum_reservation_gib"], 5)
        self.assertTrue(report["aggregate_is_same_poll"])
        self.assertFalse(report["qualification"])

    def test_legacy_trace_marks_aggregate_as_approximate(self):
        report = summarize(
            "header\nnow\t0\t10\t1024\t1\tzig build-lib --name antfly-runtime-cli\n"
        )
        self.assertFalse(report["aggregate_is_same_poll"])
        self.assertEqual(report["units"]["cli"]["minimum_reservation_gib"], 1)

    def test_response_files_survive_cache_removal_and_attribute_large_and_embedded_units(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = root / "args"
            args.write_text(
                '--name antfly-runtime-api_kernel -Ofast -Mroot="path with spaces/root.zig"'
            )
            trace = f"header\nnow\t0\t10\t1024\t1\tzig build-lib @{args}\t0\n"
            retained = root / "retained"
            report = summarize(trace, capture_dir=retained)
            self.assertEqual(
                report["units"]["api_kernel"]["peak_rss_bytes"], 1024 * 1024
            )
            args.unlink()
            self.assertEqual(summarize(trace, response_dir=retained), report)
            args.write_text("--name antfly-embedded-inference -Ofast")
            self.assertIn("embedded_inference", summarize(trace)["units"])
            args.write_text("--name antfly-capi-objects -Ofast")
            self.assertIn("capi_objects", summarize(trace)["units"])

    def test_missing_and_recursive_response_files_are_not_silently_unattributed(self):
        with tempfile.TemporaryDirectory() as directory:
            args = Path(directory) / "args"
            trace = f"header\nnow\t0\t10\t1024\t1\tzig build-lib @{args}\t0\n"
            with self.assertRaisesRegex(ValueError, "response file unavailable"):
                summarize(trace)
            args.write_text(f"@{args}")
            with self.assertRaisesRegex(ValueError, "recursive compiler response file"):
                summarize(trace)


if __name__ == "__main__":
    unittest.main()
