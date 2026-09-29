# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import unittest

from prepare_laya_packed_distillation import blend, temperature, upstream_question


class LayaDistillationTests(unittest.TestCase):
    def test_upstream_questions_preserve_label_order_and_descriptions(self):
        base = {"id": "c/q", "instruction": "which?"}
        choice = upstream_question({**base, "kind": "choice", "labels": ["search", "none"], "descriptions": ["topic", ""]})
        self.assertEqual(choice["crit"], {"search": "topic", "none": None})
        self.assertEqual(list(choice["crit"]), ["search", "none"])
        score = upstream_question({**base, "kind": "score", "labels": ["low", "high"], "descriptions": ["", "urgent"]})
        self.assertEqual(score["crit"], ["low", "urgent"])
        noul = upstream_question({**base, "kind": "noul", "labels": ["false", "true"]})
        self.assertEqual(noul["crit"], {"false": "", "true": ""})
        with self.assertRaises(ValueError):
            upstream_question({**base, "kind": "noul", "labels": ["true", "false"]})

    def test_calibration_bucket_precedes_type_temperature(self):
        decision = {"temperature": [2, 3, 4], "temperature_by_options": {"choice:3-5": 1.5}}
        self.assertEqual(temperature(decision, "choice", 4), 1.5)
        self.assertEqual(temperature(decision, "choice", 2), 2)
        self.assertEqual(temperature(decision, "noul", 2), 4)

    def test_blend_weights_gold_and_stays_a_distribution(self):
        self.assertEqual(blend([1, 0], [0.5, 0.5], 1), [1, 0])
        self.assertEqual(blend([1, 0], [0.5, 0.5], 0), [0.5, 0.5])
        mixed = blend([0.2, 0.8, 0], [0.6, 0.2, 0.2], 0.5)
        self.assertAlmostEqual(sum(mixed), 1)
        self.assertAlmostEqual(mixed[0], 0.4)
        with self.assertRaises(ValueError):
            blend([1, 0], [1, 0, 0], 0.5)


if __name__ == "__main__":
    unittest.main()
