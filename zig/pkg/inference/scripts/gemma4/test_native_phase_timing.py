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
from native_phase_timing import generation_timing, prepare_timing

GENERATION = "generate_timing_ms: prompt_format=0 tokenize=1 multimodal_prepare=0 runtime_prepare=1 prefill=10 decode=3 text_decode=1 total=16\n"
GENERATION_WITH_MAINTENANCE = "generate_timing_ms: prompt_format=0 tokenize=1 multimodal_prepare=0 runtime_prepare=1 prefill=10 maintenance=4 decode=3 text_decode=1 total=20\n"
PREPARE = "info: native_checkpoint_prepare_work source_tokens=8189 outcome=compacted query_ns=100 snapshot_ns=200 fit_ns=300 export_write_ns=400 export_clock_valid=1 checkpoint_bytes=201728532 retention_retries=2 terminal_complete=0\n"


class PhaseTimingTests(unittest.TestCase):
    def test_generation_reports_exclusive_additive_phases(self):
        row = generation_timing("other logs\n" + GENERATION, 0.020)
        self.assertEqual(row["maintenance"], 0)
        self.assertEqual(row["decode"], 3)
        self.assertEqual(row["text_decode"], 1)
        self.assertEqual(row["total"], 16)

    def test_generation_keeps_current_maintenance_out_of_decode(self):
        row = generation_timing(GENERATION_WITH_MAINTENANCE, 0.025)
        self.assertEqual(row["maintenance"], 4)
        self.assertEqual(row["decode"], 3)
        self.assertEqual(row["total"], 20)

    def test_generation_rejects_ambiguous_or_missing_requests(self):
        for text in ["", GENERATION * 2]:
            with self.assertRaises(ValueError):
                generation_timing(text, 1)

    def test_generation_rejects_corrupt_phases_and_impossible_wall_times(self):
        for text in [
            GENERATION.replace("total=16", "total=17"),
            GENERATION.replace("decode=3", "decode=-3"),
            GENERATION.replace("total=16", "total=16 total=16"),
            GENERATION.replace("prefill=10 ", ""),
            GENERATION_WITH_MAINTENANCE.replace("maintenance=4", "maintenance=-4"),
            GENERATION_WITH_MAINTENANCE.replace("total=20", "total=16"),
        ]:
            with self.assertRaises(ValueError):
                generation_timing(text, 1)
        for wall in [0.001, float("nan"), float("inf"), 0, True]:
            with self.assertRaises(ValueError):
                generation_timing(GENERATION, wall)

    def test_prepare_reports_work_without_claiming_terminal_completion(self):
        row = prepare_timing(PREPARE, 8189, 201728532, 1)
        self.assertEqual(row["measured_work_sum_ns"], 1000)
        self.assertEqual(row["terminal_complete"], 0)
        self.assertEqual(row["retention_retries"], 2)

    def test_prepare_separates_fitted_and_encoded_savings(self):
        text = PREPARE.replace(
            "terminal_complete=0",
            "fitted_savings_ratio=1.4 candidate_savings_ratio=1.7 terminal_complete=0",
        )
        text = text.replace(
            "outcome=compacted", "outcome=unchanged_insufficient_savings"
        )
        row = prepare_timing(text, 8189, 201728532, 1, "unchanged_insufficient_savings")
        self.assertEqual(row["fitted_savings_ratio"], 1.4)
        self.assertEqual(row["candidate_savings_ratio"], 1.7)
        self.assertEqual(row["measured_work_sum_ns"], 1000)
        for corrupt in [
            text.replace("candidate_savings_ratio=1.7 ", ""),
            text.replace("fitted_savings_ratio=1.4 ", ""),
            text.replace("1.4", "nan"),
            text.replace("1.7", "inf"),
            text.replace("1.4", "0"),
            text.replace("1.7", "-1"),
            text.replace("terminal_complete=0", "unrecognized=1 terminal_complete=0"),
        ]:
            with self.subTest(text=corrupt), self.assertRaises(ValueError):
                prepare_timing(
                    corrupt, 8189, 201728532, 1, "unchanged_insufficient_savings"
                )

    def test_prepare_rejects_wrong_export_and_missing_clock(self):
        for old, new in [
            ("source_tokens=8189", "source_tokens=32752"),
            ("checkpoint_bytes=201728532", "checkpoint_bytes=2"),
            ("outcome=compacted", "outcome=unchanged_insufficient_savings"),
            ("export_clock_valid=1", "export_clock_valid=0"),
            ("terminal_complete=0", "terminal_complete=1"),
            ("fit_ns=300", "fit_ns=0"),
            ("retention_retries=2", "retention_retries=-1"),
        ]:
            with self.assertRaises(ValueError):
                prepare_timing(PREPARE.replace(old, new), 8189, 201728532, 1)

    def test_prepare_rejects_duplicate_and_impossible_intervals(self):
        for text in ["", PREPARE * 2]:
            with self.assertRaises(ValueError):
                prepare_timing(text, 8189, 201728532, 1)
        with self.assertRaises(ValueError):
            prepare_timing(PREPARE, 8189, 201728532, 1e-9)


if __name__ == "__main__":
    unittest.main()
