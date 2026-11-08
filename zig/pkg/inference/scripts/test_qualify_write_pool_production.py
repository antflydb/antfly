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

from qualify_write_pool_production import require_extraction_observations

import copy
import unittest

import qualify_read_pool_40g as q
from qualify_write_pool_production import (
    assert_equivalent,
    frozen_boundary_output,
    latency_gate,
    require_observed_batch,
)


class QualificationTests(unittest.TestCase):
    def test_cohort_success_requires_caller_observations(self):
        prefix = "antfly_inference_extract_v2_"
        before = {
            prefix + key + "_total": 10
            for key in ("decoded_items", "returned_items", "decoded_prompt_tokens")
        }
        after = dict(before)
        for key, increment in (
            ("decoded_items", 2),
            ("returned_items", 2),
            ("decoded_prompt_tokens", 34),
        ):
            after[prefix + key + "_total"] += increment
        responses = [{"data": [{}], "usage": {"prompt_tokens": 17}}] * 2
        self.assertEqual(
            require_extraction_observations(before, after, responses)["returned_items"],
            2,
        )
        for key in before:
            changed = dict(after)
            changed[key] = before[key]
            with self.assertRaises(AssertionError):
                require_extraction_observations(before, changed, responses)
        with self.assertRaises(q.EvidenceUnavailable):
            require_extraction_observations({}, after, responses)

    def test_queue_turnover_does_not_establish_physical_batching(self):
        for kind in ("embed", "extract", "whisper"):
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch(
                    kind,
                    {},
                    {"antfly_inference_executor_microbatch_groups_total": 10},
                    [{}],
                )
            prefix = "antfly_inference_executor_microbatch_"
            zero = {
                prefix + "native_batches_total": 0,
                prefix + "native_items_total": 0,
            }
            positive = {
                prefix + "native_batches_total": 1,
                prefix + "native_items_total": 2,
            }
            self.assertEqual(
                require_observed_batch(kind, zero, positive, [{}])["native_items"], 2
            )
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch(kind, {}, positive, [{}])
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch(kind, positive, zero, [{}])

    def test_native_attribution_without_measured_ocr_width_is_incomplete(self):
        for response in (
            {},
            {
                "execution": {
                    "native_items": 1,
                    "requested_items": 1,
                    "rejected_items": 0,
                }
            },
            {"execution": {"max_native_step_items": 1}},
        ):
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch("ocr", {}, {}, [response])
        self.assertEqual(
            require_observed_batch(
                "ocr", {}, {}, [{"execution": {"max_native_step_items": 2}}]
            )["batched_requests"],
            1,
        )

    def test_usage_or_source_identity_changes_fail_parity(self):
        reference = {
            "data": [{"id": "first", "entities": [{"score": 0.9}]}],
            "usage": {"prompt_tokens": 17},
        }
        assert_equivalent("extract", reference, reference)
        for mutate in ("id", "tokens", "score"):
            observed = copy.deepcopy(reference)
            if mutate == "id":
                observed["data"][0]["id"] = "other"
            elif mutate == "tokens":
                observed["usage"]["prompt_tokens"] = 18
            else:
                observed["data"][0]["entities"][0]["score"] = 0.8
            with self.assertRaises(AssertionError):
                assert_equivalent("extract", reference, observed)

    def test_recognition_parity_preserves_usage_and_identity(self):
        for kind in ("ocr", "whisper"):
            reference = {
                "model": "m",
                "object": "list",
                "data": [
                    {
                        "id": "a",
                        "index": 0,
                        "text": "hello",
                        "status": "complete",
                        "audio": {"duration_ms": 20},
                    }
                ],
                "usage": {
                    "prompt_tokens": 17,
                    "completion_tokens": 3,
                    "total_tokens": 20,
                },
                "execution": {
                    "native_items": 1,
                    "requested_items": 1,
                    "rejected_items": 0,
                },
            }
            candidate = copy.deepcopy(reference)
            candidate["execution"]["native_items"] = 8
            assert_equivalent(kind, reference, candidate)
            for field in (
                "model",
                "object",
                "usage",
                "id",
                "index",
                "audio",
                "requested_items",
                "rejected_items",
            ):
                changed = copy.deepcopy(candidate)
                if field in ("model", "object"):
                    changed[field] = "other"
                elif field == "usage":
                    changed[field]["prompt_tokens"] = 999
                elif field in ("requested_items", "rejected_items"):
                    changed["execution"][field] += 1
                else:
                    changed["data"][0][field] = "other"
                with (
                    self.subTest(kind=kind, field=field),
                    self.assertRaises(AssertionError),
                ):
                    assert_equivalent(kind, reference, changed)

    def test_regression_and_promotion_are_separate_gates(self):
        self.assertFalse(latency_gate(1, 1, [0.9])["promotion_10_percent_pass"])
        self.assertTrue(latency_gate(1, 0.8, [0.7])["promotion_10_percent_pass"])
        with self.assertRaises(AssertionError):
            latency_gate(1, 1.2, [1.2])
        with self.assertRaises(q.EvidenceUnavailable):
            latency_gate(0, 1, [])

    def test_frozen_oracle_rejects_wrong_spans_scores_and_non_numeric_confidence(self):
        expected = {
            "entities": [
                {
                    "name": "person",
                    "values": [
                        {
                            "text": "Ada",
                            "confidence": 0.9,
                            "source": {"start": 1, "end": 4},
                        }
                    ],
                }
            ],
            "classifications": [],
            "relations": [],
            "structures": [],
        }
        actual = {
            "entities": [
                {"label": "person", "text": "Ada", "score": 0.9, "start": 1, "end": 4}
            ]
        }
        frozen_boundary_output(expected, actual)
        for field, value in (
            ("start", 2),
            ("label", "place"),
            ("score", True),
            ("score", float("nan")),
            ("score", 0.8),
        ):
            changed = copy.deepcopy(actual)
            changed["entities"][0][field] = value
            with self.assertRaises(AssertionError):
                frozen_boundary_output(expected, changed)


if __name__ == "__main__":
    unittest.main()
