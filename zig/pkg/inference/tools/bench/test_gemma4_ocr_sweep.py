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

"""Qualification gates and opt-in controls for the OCR sweep runner."""

import unittest
import gemma4_ocr_sweep as sweep


def report(text="complete", tokens=2, finish="stop", page=1):
    return {
        "iterations": 1,
        "waves": [
            {
                "iteration": 0,
                "outputs": [
                    {
                        "document": "test",
                        "page": page,
                        "text": text,
                        "usage": {"completion_tokens": tokens},
                        "finish_reason": finish,
                    }
                ],
            }
        ],
    }


class SweepTests(unittest.TestCase):
    def test_gate_rejects_omissions_count_changes_caps_and_missing_pages(self):
        reference = report()
        self.assertTrue(sweep.qualification(reference, reference)["passes_output_gate"])
        for candidate in [
            report(text="missing"),
            report(tokens=1),
            report(finish="length"),
            report(page=2),
        ]:
            self.assertFalse(
                sweep.qualification(candidate, reference)["passes_output_gate"]
            )

    def test_reference_uses_first_pass_to_detect_instability(self):
        reference = report()
        second = report(text="different")["waves"][0]
        second["iteration"] = 1
        reference["waves"].append(second)
        reference["iterations"] = 2
        self.assertFalse(
            sweep.qualification(reference, reference)["passes_output_gate"]
        )

    def test_diagnostic_and_ffn_rollback_are_explicit(self):
        flags, batch, _ = sweep.flags_for("differential")
        self.assertEqual(batch, 8)
        self.assertEqual(flags["ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL"], "1")
        flags, _, _ = sweep.flags_for("ffnoff")
        self.assertEqual(flags["TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN"], "1")
        flags, batch, extra = sweep.flags_for("mtp3")
        self.assertEqual(batch, 1)
        self.assertEqual(extra[-1], "3")
        self.assertEqual(flags["ANTFLY_GEMMA4_MTP_SEED_CACHED_TARGET"], "0")
        self.assertNotIn("TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME", flags)
        flags, _, _ = sweep.flags_for("mtp2frame")
        self.assertEqual(flags["TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME"], "1")

    def test_post_attention_isolation_and_free_running_trace(self):
        flags, batch, _ = sweep.flags_for("cohort4w4strict")
        self.assertEqual(batch, 4)
        for stage in ["QKV", "BLOCK", "PLE", "TAIL"]:
            self.assertEqual(flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_" + stage], "1")
        flags, _, _ = sweep.flags_for("cohort4w8attn")
        self.assertEqual(flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_ATTN_OUTPUT"], "1")
        for case in ["singletontrace", "cohort4w4qkvtrace"]:
            flags, _, _ = sweep.flags_for(case)
            self.assertEqual(flags["ANTFLY_GEMMA4_METAL_OCR_TRACE_CHOICES"], "1")
            self.assertNotIn("ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL_STEP", flags)

    def test_recommendation_excludes_failed_quality_and_overlapping_ranges(self):
        ordinary = {
            "case": "ordinary",
            "eligible_for_throughput_comparison": True,
            "seconds": [10, 11],
            "seconds_per_committed_token": [0.10, 0.11],
        }
        bad = {
            "case": "bad",
            "eligible_for_throughput_comparison": False,
            "seconds": [1, 1],
            "seconds_per_committed_token": [0.01, 0.01],
        }
        overlap = {
            "case": "mtp2",
            "eligible_for_throughput_comparison": True,
            "seconds": [9, 11],
            "seconds_per_committed_token": [0.09, 0.11],
        }
        self.assertFalse(sweep.recommend([ordinary, bad, overlap])["qualified_speedup"])
        faster = {
            "case": "cohort4w8",
            "eligible_for_throughput_comparison": True,
            "seconds": [5, 6],
            "seconds_per_committed_token": [0.05, 0.06],
        }
        self.assertTrue(
            sweep.recommend([ordinary, bad, faster, ordinary])["qualified_speedup"]
        )

    def test_incomplete_or_unbracketed_campaign_cannot_qualify(self):
        ordinary = {
            "case": "ordinary",
            "eligible_for_throughput_comparison": True,
            "seconds": [10, 11],
            "seconds_per_committed_token": [0.10, 0.11],
        }
        faster = {
            "case": "cohort2w4",
            "eligible_for_throughput_comparison": True,
            "seconds": [5, 6],
            "seconds_per_committed_token": [0.05, 0.06],
        }
        self.assertFalse(sweep.recommend([ordinary, faster])["qualified_speedup"])
        self.assertFalse(
            sweep.recommend([ordinary, faster, ordinary], complete=False)[
                "qualified_speedup"
            ]
        )

    def test_rss_counts_worker_descendants_and_excludes_unrelated_processes(self):
        table = "10 1 100\n11 10 200\n12 11 300\n13 1 999\n"
        self.assertEqual(sweep.process_tree_rss(table, 10), 600 * 1024)

    def test_profile_retains_first_divergence_and_changed_choices(self):
        log = "ocr-parity stage=attn-norm layer=0 row=1 seq=300 input=5 max_abs=1e-7 mean_abs=2e-8 nonfinite=0 matrix_hash=ab canonical_hash=cd\nocr-parity-choice row=1 seq=300 input=5 matrix=1 canonical=2 match=false matrix_margin=1e-6 canonical_margin=2e-6\n"
        result = sweep.profile(log)["parity"]
        self.assertEqual(result["first_nonidentical_stage"]["stage"], "attn-norm")
        self.assertEqual(len(result["changed_choices"]), 1)


if __name__ == "__main__":
    unittest.main()
