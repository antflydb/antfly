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

"""A speed comparison must reject unequal work and unstable greedy output."""

import copy
import unittest

from compare_gemma4 import (
    check_cpu_pins,
    check_decisions,
    check_native_activity,
    check_native_speculation,
    check_rows,
    native_campaign_suite,
    native_child_environment,
    native_profile,
)


class ComparisonCoverageTests(unittest.TestCase):
    def setUp(self):
        self.cases = [{"id": "text", "expected_prompt_tokens": 4, "max_tokens": 3}]
        self.rows = [
            {
                "id": "text",
                "repeat": i,
                "prompt_tokens": 4,
                "completion_tokens": 3,
                "tokens": [5, 6, 7],
                "ms": 1.0,
            }
            for i in range(2)
        ]

    def test_equal_work_passes(self):
        check_rows(self.rows, self.cases, 2)

    def test_baseline_does_not_inherit_candidate_optimizations_or_assistant(self):
        ambient = {
            "PATH": "/bin",
            "ANTFLY_INFERENCE_CUDA_GEMMA4_SMALL_DECODE_TILES": "1",
            "ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE": "1",
        }
        baseline = native_child_environment(ambient, native_profile({}))
        self.assertEqual(baseline["PATH"], "/bin")
        self.assertNotIn("ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE", baseline)
        self.assertNotIn("ANTFLY_INFERENCE_CUDA_GEMMA4_SMALL_DECODE_TILES", baseline)
        suite = {"cases": self.cases, "draft_model": "assistant", "speculative_k": 2}
        self.assertEqual(native_campaign_suite(suite, True), {"cases": self.cases})
        self.assertEqual(native_campaign_suite(suite), suite)
        self.assertEqual(suite["draft_model"], "assistant")

    def test_explicit_baseline_profile_is_independent(self):
        candidate = native_profile({"ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE": "1"})
        baseline = native_profile(
            {"ANTFLY_INFERENCE_CUDA_GEMMA4_SMALL_DECODE_TILES": "1"}
        )
        env = native_child_environment(candidate, baseline)
        self.assertNotIn("ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE", env)
        self.assertEqual(env["ANTFLY_INFERENCE_CUDA_GEMMA4_SMALL_DECODE_TILES"], "1")

    def test_profiles_reject_unrelated_or_non_string_environment(self):
        for profile in (
            {"LD_PRELOAD": "override.so"},
            {"ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE": True},
            [],
        ):
            with (
                self.subTest(profile=profile),
                self.assertRaisesRegex(ValueError, "Native profile"),
            ):
                native_profile(profile)

    def test_every_measured_request_must_run_speculation(self):
        active = {
            "mtp_enabled": True,
            "rounds": 2,
            "drafted_tokens": 4,
            "matched_draft_tokens": 2,
        }
        for row in self.rows:
            row["speculative"] = copy.deepcopy(active)
        check_native_speculation(self.rows)
        for bad in (
            None,
            {},
            {**active, "mtp_enabled": False},
            {**active, "rounds": 0},
            {**active, "matched_draft_tokens": True},
        ):
            with self.subTest(stats=bad):
                self.rows[1]["speculative"] = bad
                with self.assertRaisesRegex(ValueError, "Inactive measured"):
                    check_native_speculation(self.rows)
        with self.assertRaisesRegex(ValueError, "Missing measured"):
            check_native_speculation([])

    def test_short_output_cannot_win(self):
        self.rows[0].update(completion_tokens=2, tokens=[5, 6])
        with self.assertRaisesRegex(ValueError, "fixed-budget"):
            check_rows(self.rows, self.cases, 2)

    def test_duplicate_repeat_cannot_fill_missing_sample(self):
        self.rows[1] = copy.deepcopy(self.rows[0])
        with self.assertRaisesRegex(ValueError, "coverage"):
            check_rows(self.rows, self.cases, 2)

    def test_unstable_greedy_output_is_rejected(self):
        self.rows[1]["tokens"] = [5, 6, 8]
        with self.assertRaisesRegex(ValueError, "Nondeterministic"):
            check_rows(self.rows, self.cases, 2)

    def test_different_prompt_is_rejected(self):
        self.rows[0]["prompt_tokens"] = 5
        with self.assertRaisesRegex(ValueError, "fixed-budget"):
            check_rows(self.rows, self.cases, 2)

    def test_changed_verified_prefix_cannot_win(self):
        self.rows[0]["tokens"][1] = 8
        with self.assertRaisesRegex(ValueError, "Transformer decision differs"):
            check_decisions(self.rows, {"text": [5, 6]})

    def test_post_eos_budget_is_outside_saved_cpu_prefix(self):
        check_decisions(self.rows, {"text": [5, 6]})

    def test_missing_reference_case_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "coverage"):
            check_decisions(self.rows, {"text": [5, 6], "audio": [8]})

    def test_truncated_verified_prefix_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            check_decisions(self.rows, {"text": [5, 6, 7, 8]})

    def test_inactive_precision_route_cannot_be_qualified(self):
        for stats in ({}, {"residual_calls": 0}):
            with self.assertRaisesRegex(ValueError, "Inactive native optimization"):
                check_native_activity({"cuda": stats}, ["residual_calls"])
        check_native_activity({"cuda": {"residual_calls": 12}}, ["residual_calls"])

    def test_warmup_only_activity_cannot_qualify_measured_media(self):
        report = {
            "cuda": {"residual_calls": 247},
            "rows": [{"id": "image", "residual_calls": 0}],
        }
        with self.assertRaisesRegex(ValueError, "Inactive measured media"):
            check_native_activity(
                report, [], ["residual_calls"], [{"id": "image", "kind": "image"}]
            )
        report["rows"][0]["residual_calls"] = 72
        check_native_activity(
            report, [], ["residual_calls"], [{"id": "image", "kind": "image"}]
        )

    def test_audio_activity_is_required_on_each_measured_audio_request(self):
        cases = [{"id": "audio", "kind": "audio"}, {"id": "image", "kind": "image"}]
        report = {
            "cuda": {"layouts": 6},
            "rows": [{"id": "audio", "layouts": 0}, {"id": "image", "layouts": 0}],
        }
        with self.assertRaisesRegex(ValueError, "Inactive measured"):
            check_native_activity(
                report, [], cases=cases, kind_required=["audio=layouts"]
            )
        report["rows"][0]["layouts"] = 3
        check_native_activity(report, [], cases=cases, kind_required=["audio=layouts"])

    def test_missing_required_modality_cannot_vacuously_pass(self):
        with self.assertRaisesRegex(ValueError, "no cases"):
            check_native_activity(
                {}, [], cases=self.cases, kind_required=["audio=layouts"]
            )

    def test_changed_oracle_bytes_cannot_qualify_a_candidate(self):
        qualification = {
            "source_truth": {
                "identity": {"version": "official"},
                "vectors": {"text": "original"},
            },
            "native_reference_profile": {
                "validation_provenance": {"models": {"gguf": "original"}}
            },
        }
        check_cpu_pins(
            qualification,
            {"gguf": "original"},
            [{"version": "official"}],
            {"text": "original"},
        )
        for models, identities, vectors in [
            ({"gguf": "changed"}, [{"version": "official"}], {"text": "original"}),
            ({"gguf": "original"}, [{"version": "adapter"}], {"text": "original"}),
            ({"gguf": "original"}, [{"version": "official"}], {"text": "changed"}),
        ]:
            with self.assertRaisesRegex(ValueError, "differs"):
                check_cpu_pins(qualification, models, identities, vectors)
