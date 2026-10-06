# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
import json
import unittest

from prepare_laya_finetune import convert


class LayaConversionTests(unittest.TestCase):
    def setUp(self):
        self.case = {
            "id": "case-17",
            "state": json.dumps({"text": "hello"}),
            "questions": {
                "tool": {
                    "type": "choice",
                    "instructions": "which?",
                    "criteria": {"search": "topic", "none": "answer"},
                },
                "urgency": {
                    "type": "score",
                    "instructions": "urgency?",
                    "criteria": ["low", "high"],
                },
                "needed": {"type": "noul", "instructions": "needed?"},
            },
            "gold": {
                "tool": {"probabilities": {"none": 0.2, "search": 0.8}},
                "urgency": {"probabilities": {"1": 0.7, "0": 0.3}},
                "needed": {"probabilities": {"true": 0.6, "false": 0.4}},
            },
        }

    def test_preserves_option_order_and_case_group(self):
        records = convert(self.case)
        self.assertEqual(
            [r["target"] for r in records], [[0.8, 0.2], [0.3, 0.7], [0.4, 0.6]]
        )
        self.assertEqual({r["group_id"] for r in records}, {"case-17"})
        self.assertEqual(records[1]["descriptions"], ["low", "high"])
        self.assertEqual(records[2]["labels"], ["false", "true"])

    def test_dataset_encoded_columns_match_objects(self):
        encoded = copy.deepcopy(self.case)
        for key in ("questions", "gold"):
            encoded[key] = json.dumps(encoded[key])
        self.assertEqual(convert(encoded), convert(self.case))

    def test_missing_target_does_not_become_uniform(self):
        del self.case["gold"]["needed"]["probabilities"]["false"]
        with self.assertRaises(ValueError):
            convert(self.case)

    def test_invalid_distributions_fail(self):
        for value in (float("nan"), float("inf"), -0.1, 1.1, 0.1, True):
            with self.subTest(value=value):
                candidate = copy.deepcopy(self.case)
                candidate["gold"]["tool"]["probabilities"]["search"] = value
                with self.assertRaises(ValueError):
                    convert(candidate)

    def test_missing_question_gold_fails(self):
        del self.case["gold"]["urgency"]
        with self.assertRaises(ValueError):
            convert(self.case)

    def test_fallback_group_binds_source_state(self):
        del self.case["id"]
        records = convert(self.case)
        self.assertEqual(len(records[0]["group_id"]), 64)
        self.assertEqual(len({r["group_id"] for r in records}), 1)


if __name__ == "__main__":
    unittest.main()
