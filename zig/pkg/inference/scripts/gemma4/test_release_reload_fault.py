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
import hashlib
import struct
import unittest

import release_attention_matching as release
import test_release_failure_collector as fixture


class ReloadFaultContractTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.FailureCollectorTests(methodName="test_raw_fault_collection_covers_sites_and_reload_generation")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        report = copy.deepcopy(self.fixture.report)
        tokens = report["exact_output_tokens"]
        digest = hashlib.sha256(b"".join(struct.pack("<I", token) for token in tokens)).hexdigest()
        report.update(
            fault="reload",
            loaded_generation_before=7,
            loaded_generation_after=8,
            request_generation=8,
            stale_selection_rejected=True,
            pre_reload_exact_generation=7,
            attention_matching_generation=8,
            exact_after_generation=8,
            context_disposition="model_generation_reloaded",
            original_context=None,
            partial_context=None,
            pre_reload_exact_output_tokens=list(tokens),
            pre_reload_exact_output_sha256=digest,
            attention_matching_output_tokens=list(tokens),
            attention_matching_output_sha256=digest,
            exact_after_output_tokens=list(tokens),
            exact_after_output_sha256=digest,
        )
        report["loaded_identity"]["load_generation"] = 8
        report["injection"] = {
            "kind": "reload",
            "stage": "reload",
            "mechanism": "model_manager_generation_transition",
            "completed_units": 1,
            "total_units": 1,
        }
        names = (
            "request_started",
            "fault_injected",
            "model_generation_reloaded",
            "stale_selection_rejected",
            "fresh_attention_matching_published",
            "observer_verified",
            "request_released",
        )
        report["events"] = [
            {
                "name": name,
                "monotonic_ns": index,
                "live_bytes": 100,
                **({"fault": "reload"} if name == "fault_injected" else {}),
            }
            for index, name in enumerate(names)
        ]
        self.report = report

    def test_genuine_reload_sequence_passes(self):
        release.failure_report_contract(self.report, self.fixture.identity, self.fixture.origin)

    def test_old_generation_request_rejected(self):
        self.report["request_generation"] = 7
        with self.assertRaisesRegex(ValueError, "loaded generation"):
            release.failure_report_contract(self.report, self.fixture.identity, self.fixture.origin)

    def test_missing_stale_selection_proof_rejected(self):
        self.report["stale_selection_rejected"] = False
        with self.assertRaisesRegex(ValueError, "did not reload cleanly"):
            release.failure_report_contract(self.report, self.fixture.identity, self.fixture.origin)

    def test_missing_fresh_am_publication_rejected(self):
        self.report["events"] = [
            event for event in self.report["events"]
            if event["name"] != "fresh_attention_matching_published"
        ]
        with self.assertRaisesRegex(ValueError, "event sequence"):
            release.failure_report_contract(self.report, self.fixture.identity, self.fixture.origin)

    def test_pre_reload_output_mismatch_rejected(self):
        self.report["pre_reload_exact_output_tokens"][0] += 1
        with self.assertRaisesRegex(ValueError, "pre-transition exact output"):
            release.failure_report_contract(self.report, self.fixture.identity, self.fixture.origin)


if __name__ == "__main__":
    unittest.main()
