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

import unittest

import lite_namespace_replay as bench


def sample(seconds, applied, target):
    return {
        "seconds": seconds,
        "indexes": [
            {
                "name": "text",
                "replay_applied_sequence": applied,
                "replay_target_sequence": target,
            }
        ],
    }


class ActiveVisibilityTests(unittest.TestCase):
    def test_follows_an_earlier_prefix_while_targets_keep_moving(self):
        report = bench.active_progress(
            [sample(1, 1, 10), sample(3, 10, 30), sample(5, 30, 50)], 3
        )
        self.assertTrue(report["passed"])
        self.assertEqual(report["completed_prefixes"], 2)

    def test_rejects_primary_progress_without_index_progress(self):
        report = bench.active_progress(
            [sample(1, 1, 10), sample(5, 1, 50), sample(12, 1, 120)], 3
        )
        self.assertFalse(report["passed"])

    def test_late_completion_cannot_hide_an_exceeded_deadline(self):
        report = bench.active_progress([sample(1, 1, 10), sample(5, 10, 50)], 3)
        self.assertFalse(report["passed"])

    def test_checks_the_next_prefix_after_an_early_success(self):
        report = bench.active_progress(
            [sample(1, 1, 10), sample(2, 10, 20), sample(6, 10, 60)], 3
        )
        self.assertFalse(report["passed"])

    def test_missing_or_insufficient_evidence_does_not_pass(self):
        for samples in ([], [sample(1, 1, 10)], [{"seconds": 1, "indexes": []}]):
            self.assertFalse(bench.active_progress(samples, 3)["passed"])

    def test_stale_targets_without_index_advancement_do_not_pass(self):
        self.assertFalse(
            bench.active_progress(
                [sample(1, 10, 10), sample(2, 10, 10), sample(3, 10, 10)], 3
            )["passed"]
        )

    def test_one_healthy_index_cannot_hide_missing_sibling_evidence(self):
        samples = [sample(1, 1, 10), sample(2, 10, 20)]
        for row in samples:
            row["indexes"].append({"name": "copied_text"})
        self.assertFalse(bench.active_progress(samples, 3)["passed"])

    def test_explicit_empty_graph_fixture_does_not_mask_its_text_sink(self):
        samples = [sample(1, 1, 10), sample(2, 10, 20)]
        for row in samples:
            row["indexes"].append(
                {
                    "name": "knowledge",
                    "replay_applied_sequence": 0,
                    "replay_target_sequence": 0,
                }
            )
        self.assertFalse(bench.active_progress(samples, 3)["passed"])
        self.assertTrue(
            bench.active_progress(samples, 3, excluded_indexes=("knowledge",))["passed"]
        )


class BackfillVisibilityTests(unittest.TestCase):
    @staticmethod
    def sample(seconds, revision=2, pending=False, applied=10):
        return {
            "seconds": seconds,
            "resolver_progress": [
                {
                    "name": "bench_concepts",
                    "namespace_curation_generation": revision,
                    "backfill_pending": pending,
                    "resolution": {"applied_sequence": applied},
                }
            ],
        }

    def test_requires_accepted_revision_backfill_and_replay_prefix(self):
        self.assertTrue(bench.backfill_progress([self.sample(2)], 2, 10, 3)["passed"])
        for row in (
            self.sample(2, revision=1),
            self.sample(2, pending=True),
            self.sample(2, applied=9),
            {"seconds": 2},
        ):
            self.assertFalse(bench.backfill_progress([row], 2, 10, 3)["passed"])

    def test_late_backfill_does_not_erase_deadline_failure(self):
        report = bench.backfill_progress(
            [self.sample(1, pending=True), self.sample(5)], 2, 10, 3
        )
        self.assertFalse(report["passed"])
        self.assertEqual(report["completion_seconds"], 5)


if __name__ == "__main__":
    unittest.main()
