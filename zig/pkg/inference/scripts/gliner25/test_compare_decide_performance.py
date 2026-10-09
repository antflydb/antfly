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
import unittest
import compare_decide_performance as compare


class AcceptanceTests(unittest.TestCase):
    def pairs(self):
        context = {
            "swap": "vm.swapusage: total = 1024.00M used = 10.00M free = 1014.00M",
            "thermal": "No thermal warning level has been recorded",
            "power": "Now drawing from 'AC Power'",
        }
        cases = [
            *compare.PRIMARY,
            "binary_short",
            "described_four_labels",
            "score_and_noul",
            "mixed_longer",
        ]
        antfly = dict(
            passed=True,
            source_unchanged=True,
            binary_unchanged=True,
            binary_sha256="abc",
            source={"head": "abc"},
            host_before=context,
            host_after=context,
            reports=[
                dict(
                    case_id=c,
                    path="http_handler",
                    backend="metal",
                    model={"sha256": "model"},
                    capture_sha256="capture" if c in compare.PRIMARY else "holdout",
                    prepared_tokens=40,
                    samples_ns=[80_000_000] * 20,
                )
                for c in cases
            ],
        )
        python = dict(
            passed=True,
            source_unchanged=True,
            source={"head": "abc"},
            host_before=context,
            host_after=context,
            report=dict(
                device="mps",
                model={"model_sha256": "model"},
                source={"commit": "upstream"},
                runtime={"version": "pinned"},
                capture={"sha256": "capture"},
                short_holdout_sha256="holdout",
                cases=[
                    dict(case_id=c, prepared_tokens=40, samples_ns=[100_000_000] * 20)
                    for c in cases
                ],
            ),
        )
        return [copy.deepcopy((antfly, python)) for _ in range(6)]

    def test_repeatable_win(self):
        self.assertTrue(compare.evaluate(self.pairs())["passed"])

    def test_one_losing_block_is_not_hidden_by_pooled_mean(self):
        pairs = self.pairs()
        pairs[0][0]["reports"][0]["samples_ns"] = [101_000_000] * 20
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_failed_process_is_not_dropped(self):
        pairs = self.pairs()
        pairs[0][0]["passed"] = False
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_swap_growth_disqualifies(self):
        pairs = self.pairs()
        pairs[0][0]["host_after"] = {
            "swap": "used = 11.00M",
            "thermal": "No thermal warning",
        }
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_heldout_regression_disqualifies(self):
        pairs = self.pairs()
        for antfly, _ in pairs:
            antfly["reports"][-1]["samples_ns"] = [106_000_000] * 20
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_incomplete_campaign_disqualifies(self):
        self.assertFalse(compare.evaluate(self.pairs()[:5])["passed"])

    def test_diagnostic_run_cannot_qualify(self):
        pairs = self.pairs()
        pairs[0][0]["diagnostic_only"] = True
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_missing_source_verification_cannot_qualify(self):
        pairs = self.pairs()
        del pairs[0][1]["source_unchanged"]
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_power_source_change_cannot_qualify(self):
        pairs = self.pairs()
        pairs[0][0]["host_after"] = {
            **pairs[0][0]["host_after"],
            "power": "Now drawing from 'Battery Power'",
        }
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_mixed_backends_cannot_qualify(self):
        pairs = self.pairs()
        for row in pairs[0][0]["reports"]:
            row["backend"] = "native"
        pairs[0][1]["report"]["device"] = "cpu"
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_duplicate_and_missing_case_cannot_balance_across_blocks(self):
        pairs = self.pairs()
        pairs[0][0]["reports"].append(copy.deepcopy(pairs[0][0]["reports"][0]))
        del pairs[1][0]["reports"][0]
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_substitute_holdout_cannot_qualify(self):
        pairs = self.pairs()
        for antfly, python in pairs:
            antfly["reports"][-1]["case_id"] = "unreviewed"
            python["report"]["cases"][-1]["case_id"] = "unreviewed"
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_missing_model_or_capture_identity_cannot_qualify(self):
        for key in ("model", "capture_sha256"):
            pairs = self.pairs()
            del pairs[0][0]["reports"][0][key]
            self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_python_runtime_change_cannot_qualify(self):
        pairs = self.pairs()
        pairs[0][1]["report"]["runtime"] = {"version": "different"}
        self.assertFalse(compare.evaluate(pairs)["passed"])

    def test_mismatched_paired_source_cannot_qualify(self):
        pairs = self.pairs()
        pairs[0][1]["source"] = {"head": "different"}
        self.assertFalse(compare.evaluate(pairs)["passed"])


if __name__ == "__main__":
    unittest.main()
