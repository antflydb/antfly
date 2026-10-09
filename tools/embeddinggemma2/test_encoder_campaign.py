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

import copy
import unittest

if __package__:
    from . import compare
    from . import encoder_campaign as campaign
else:
    import compare
    import encoder_campaign as campaign


class EncoderCampaignContracts(unittest.TestCase):
    def runs(self):
        result = {}
        for role, samples in {
            "baseline": (0.10, 4.5),
            "candidate": (0.08, 3.7),
            "mps": (0.08, 3.4),
        }.items():
            result[role] = {
                "status": "pass",
                "measurement_mode": "encoder",
                "suite_sha256": "s",
                "checkpoint_receipt_sha256": "w",
                "precision": "float32",
                "decision": {"status": "not_run"},
                "cases": {},
            }
            for name, sample in zip(campaign.CASES, samples):
                n = 20 if name == "document_512" else 5
                result[role]["cases"][name] = {
                    "vector": [1.0, 0.0],
                    "timing": compare.timing([sample] * n),
                    "vm_measured_before": [2, 3, 4],
                    "vm_measured_after": [2, 3, 4],
                    "host_live_after_release": 0,
                    "host_peak_bytes": 100,
                    "gpu_memory": [
                        {"frame_retained_bytes": 0, "scratch_pool_pending_slots": 0}
                    ],
                }
        return result

    def test_both_lengths_must_pass_in_the_same_rotation(self):
        runs = self.runs()
        self.assertTrue(campaign.assess(runs)["within_target"])
        runs["candidate"]["cases"]["document_512"]["timing"] = compare.timing(
            [0.09] * 20
        )
        assessment = campaign.assess(runs)
        self.assertFalse(assessment["within_target"])
        self.assertTrue(assessment["cases"]["document_8192"]["within_target"])

    def test_measured_paging_disqualifies_fast_results(self):
        runs = self.runs()
        runs["mps"]["cases"]["document_8192"]["vm_measured_after"][0] += 1
        assessment = campaign.assess(runs)
        self.assertFalse(assessment["timing_eligible"])
        self.assertFalse(assessment["within_target"])
        bad = copy.deepcopy(runs)
        bad["mps"]["cases"]["document_8192"]["vm_measured_after"][0] = 0
        with self.assertRaises(ValueError):
            campaign.assess(bad)

    def test_archived_baseline_requires_a_clean_entire_process(self):
        runs = self.runs()
        baseline = runs["baseline"]
        for case in baseline["cases"].values():
            del case["vm_measured_before"], case["vm_measured_after"]
        baseline["before"] = {"vm_stat": "Pageouts: 2.\nSwapins: 3.\nSwapouts: 4.\n"}
        baseline["after"] = {"vm_stat": "Pageouts: 2.\nSwapins: 3.\nSwapouts: 4.\n"}
        self.assertTrue(campaign.assess(runs)["within_target"])
        baseline["after"]["vm_stat"] = "Pageouts: 3.\nSwapins: 3.\nSwapouts: 4.\n"
        self.assertFalse(campaign.assess(runs)["timing_eligible"])

    def test_archived_gpu_telemetry_is_optional_candidate_is_required(self):
        runs = self.runs()
        for case in runs["baseline"]["cases"].values():
            del case["gpu_memory"]
        assessment = campaign.assess(runs)
        self.assertTrue(assessment["within_target"])
        self.assertFalse(
            assessment["cases"]["document_512"][
                "archived_baseline_gpu_telemetry_available"
            ]
        )
        del runs["candidate"]["cases"]["document_512"]["gpu_memory"]
        with self.assertRaises(ValueError):
            campaign.assess(runs)

    def test_parity_and_complete_case_selection_cannot_be_waived(self):
        runs = self.runs()
        runs["candidate"]["cases"]["document_512"]["vector"] = [1.0, 2e-5]
        with self.assertRaises(ValueError):
            campaign.assess(runs)

    def test_samples_and_memory_release_are_required(self):
        for key, value in (
            ("host_live_after_release", 1),
            ("host_peak_bytes", 513 * 1024 * 1024),
        ):
            runs = self.runs()
            runs["candidate"]["cases"]["document_512"][key] = value
            with self.assertRaises(ValueError):
                campaign.assess(runs)
        runs = self.runs()
        runs["candidate"]["cases"]["document_8192"]["timing"] = compare.timing([2.0])
        with self.assertRaises(ValueError):
            campaign.assess(runs)
        runs = self.runs()
        runs["candidate"]["cases"]["document_512"]["gpu_memory"][0][
            "frame_retained_bytes"
        ] = 4
        with self.assertRaises(ValueError):
            campaign.assess(runs)
        del runs["candidate"]["cases"]["document_512"]
        with self.assertRaises(ValueError):
            campaign.assess(runs)


if __name__ == "__main__":
    unittest.main()
