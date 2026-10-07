from __future__ import annotations

import hashlib
import json
import re
import unittest

import capture_family_references as capture


HERE = capture.HERE
REQUESTS = HERE / "family_source_word_floor_reference_requests.json"
REQUESTS_SHA256 = "97c978fff4aa205a6f2d1d201bd473458926884550e3a670f0624cc1ec381852"
CAPTURE_DIR = HERE.parent.parent / "testdata" / "gliner25" / "family"
CAPTURES = {
    "multi_v1_source_word_floor_capture.json": {
        "size": 16839,
        "sha256": "f56c5dd28f0e677050f48167e20c70ca361fc42a78f1a8a3d9b192bb4be3e9e4",
        "semantic": [True, True],
        "value_counts": [1, 1],
    },
    "multi_decide_source_word_floor_capture.json": {
        "size": 16423,
        "sha256": "0e5cd459c394713e90d0ce19b2106da18c9b3a5b851f0f989bae440979b18903",
        "semantic": [True, False],
        "value_counts": [1, 0],
    },
}


class CaptureFamilySourceWordFloorReferencesTest(unittest.TestCase):
    def test_corpus_is_one_original_source_word(self) -> None:
        payload = REQUESTS.read_bytes()
        self.assertEqual(REQUESTS_SHA256, hashlib.sha256(payload).hexdigest())
        document = capture.validate_requests(REQUESTS)
        self.assertEqual(
            ["bare_source_word_floor", "described_source_word_floor"],
            [row["id"] for row in document["requests"]],
        )
        for row in document["requests"]:
            self.assertEqual("Alice", row["text"])
            self.assertIsNotNone(re.fullmatch(r"\w+", row["text"]))

    def test_checked_in_captures_pin_geometry_and_original_spans(self) -> None:
        for name, expected in CAPTURES.items():
            payload = (CAPTURE_DIR / name).read_bytes()
            self.assertEqual(expected["size"], len(payload))
            self.assertEqual(expected["sha256"], hashlib.sha256(payload).hexdigest())
            report = json.loads(payload)
            self.assertFalse(report["qualification"])
            self.assertEqual(REQUESTS_SHA256, report["artifacts"]["requests_sha256"])
            self.assertEqual(
                "4e9fee278e8e4757f1ee03aec87208dfc7cbb58eb7a009b30e8f525c8eb9cde5",
                report["artifacts"]["generator_sha256"],
            )
            self.assertEqual(
                expected["semantic"],
                [row["semantic"]["pass"] for row in report["requests"]],
            )
            for index, row in enumerate(report["requests"]):
                self.assertEqual(17 if index == 0 else 22, len(row["encoded"]["input_ids"]))
                self.assertEqual(["alice", "."], row["encoded"]["text_tokens"])
                self.assertEqual([0, 5], row["encoded"]["start_mappings"])
                self.assertEqual([5, 6], row["encoded"]["end_mappings"])
                values = row["native_expected"]["entities"][0]["values"]
                self.assertEqual(expected["value_counts"][index], len(values))
                for value in values:
                    self.assertEqual("Alice", value["text"])
                    self.assertGreaterEqual(value["source"]["start"], 0)
                    self.assertLessEqual(value["source"]["end"], len(row["text"]))


if __name__ == "__main__":
    unittest.main()
