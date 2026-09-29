# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import unittest

from prepare_laya_openjev import convert


def row(kind, options, target, state="s"):
    return {"id": "g/q", "group_id": "g", "kind": kind, "question": "which?", "options": options, "target": target, "state": state}


class LayaOpenJevTests(unittest.TestCase):
    def test_choice_splits_label_and_description_only_when_every_option_does(self):
        r = convert(row("choice", ["red: RGB [255, 0, 0]", "blue: RGB [0, 0, 255]"], [0, 1]))
        self.assertEqual(r["labels"], ["red", "blue"])
        self.assertEqual(r["descriptions"], ["RGB [255, 0, 0]", "RGB [0, 0, 255]"])
        r = convert(row("choice", ["up", "left: turn"], [1, 0]))
        self.assertEqual(r["labels"], ["up", "left: turn"])
        self.assertEqual(r["descriptions"], ["", ""])

    def test_score_uses_indices_with_options_as_descriptions(self):
        r = convert(row("score", ["low", "mid", "high"], [0, 0.5, 0.5]))
        self.assertEqual(r["labels"], ["0", "1", "2"])
        self.assertEqual(r["descriptions"], ["low", "mid", "high"])

    def test_noul_maps_to_false_true_and_reorders_yes_no(self):
        self.assertEqual(convert(row("noul", ["no", "yes"], [0.2, 0.8]))["target"], [0.2, 0.8])
        r = convert(row("noul", ["yes", "no"], [0.2, 0.8]))
        self.assertEqual(r["labels"], ["false", "true"])
        self.assertEqual(r["target"], [0.8, 0.2])
        with self.assertRaises(ValueError):
            convert(row("noul", ["maybe", "yes"], [0.5, 0.5]))

    def test_structured_state_is_serialized_and_bad_targets_rejected(self):
        self.assertEqual(convert(row("noul", ["no", "yes"], [1, 0], state={"a": 1}))["text"], '{"a": 1}')
        with self.assertRaises(ValueError):
            convert(row("choice", ["a", "b"], [0.5, 0.6]))
        with self.assertRaises(ValueError):
            convert(row("choice", ["a", "b"], [1.0]))


if __name__ == "__main__":
    unittest.main()
