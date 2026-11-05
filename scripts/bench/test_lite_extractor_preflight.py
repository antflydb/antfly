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

from lite_extractor_preflight import require_completed_projection


class DurableCompletionTests(unittest.TestCase):
    def setUp(self):
        # A reopened observer has no request counters, while coverage and the
        # projection checkpoint survive. This was the failed final preflight.
        self.stats = {
            "enrichment": {"processed_requests": 0, "error_count": 0},
            "indexes_available": True,
            "indexes": [
                {
                    "name": "knowledge",
                    "coverage_summary_ready": True,
                    "coverage_produced_count": 3,
                    "coverage_skipped_count": 0,
                    "coverage_terminal_failed_count": 0,
                    "replay_catch_up_required": False,
                    "replay_applied_sequence": 4,
                    "replay_target_sequence": 4,
                }
            ],
        }
        self.pending = {
            "derived_target_sequence": 4,
            "enrichment": {
                "error_count": 0,
                "retrying": False,
                "worker_failed": False,
                "stalled": False,
                "projection_checkpoint_status": "clean",
                "projection_checkpoint_identity_consistent": True,
                "applied_sequence": 4,
                "target_sequence": 4,
                "projection_checkpoint_applied_sequence": 4,
            },
        }

    def test_fresh_runtime_counters_do_not_hide_durable_completion(self):
        require_completed_projection(self.stats, self.pending, 3)

    def test_request_counters_cannot_hide_missing_or_failed_coverage(self):
        self.stats["enrichment"]["processed_requests"] = 100
        for key, value in (
            ("coverage_produced_count", 2),
            ("coverage_summary_ready", False),
            ("coverage_skipped_count", 1),
            ("coverage_terminal_failed_count", 1),
        ):
            with self.subTest(key=key):
                stats = copy.deepcopy(self.stats)
                stats["indexes"][0][key] = value
                with self.assertRaises(RuntimeError):
                    require_completed_projection(stats, self.pending, 3)

    def test_coverage_cannot_hide_incomplete_or_unhealthy_checkpoint(self):
        for key, value in (
            ("projection_checkpoint_status", "retrying"),
            ("projection_checkpoint_identity_consistent", False),
            ("projection_checkpoint_applied_sequence", 3),
            ("applied_sequence", 3),
            ("error_count", 1),
            ("retrying", True),
            ("worker_failed", True),
            ("stalled", True),
        ):
            with self.subTest(key=key):
                pending = copy.deepcopy(self.pending)
                pending["enrichment"][key] = value
                with self.assertRaises(RuntimeError):
                    require_completed_projection(self.stats, pending, 3)

    def test_replay_and_unavailable_index_evidence_remain_fail_closed(self):
        for key, value in (
            ("replay_catch_up_required", True),
            ("replay_applied_sequence", 3),
        ):
            with self.subTest(key=key):
                stats = copy.deepcopy(self.stats)
                stats["indexes"][0][key] = value
                with self.assertRaises(RuntimeError):
                    require_completed_projection(stats, self.pending, 3)
        self.stats["indexes_available"] = False
        with self.assertRaises(RuntimeError):
            require_completed_projection(self.stats, self.pending, 3)


if __name__ == "__main__":
    unittest.main()
