from __future__ import annotations

import hashlib
import json
import unittest

import capture_family_references as capture


HERE = capture.HERE
REQUESTS = HERE / "family_clean_entity_short_reference_requests.json"
REQUESTS_SHA256 = "407fb1be410c7200c8daed0246a3ffb93be4d6b905ad770f915a5f21ceaaba6c"
CAPTURE_DIR = HERE.parent.parent / "testdata" / "gliner25" / "family"
CAPTURES = {
    "multi_v1_clean_entity_short_capture.json": {
        "size": 16376,
        "sha256": "f21f373a3c647ab4387dd5334c8420e5acc536ebc7ab715fec3522b616f655d5",
        "semantic": [True, False],
        "value_counts": [1, 0],
    },
    "multi_decide_clean_entity_short_capture.json": {
        "size": 15967,
        "sha256": "a6cfc792d13b50ab9fdf2a218cfa2eadf5157cb95dd6dff6375ffc0291068897",
        "semantic": [False, False],
        "value_counts": [0, 0],
    },
}


class CaptureFamilyCleanEntityShortReferencesTest(unittest.TestCase):
    def test_clean_corpus_keeps_offsets_inside_original_text(self) -> None:
        payload = REQUESTS.read_bytes()
        self.assertEqual(REQUESTS_SHA256, hashlib.sha256(payload).hexdigest())
        document = capture.validate_requests(REQUESTS)
        self.assertEqual(
            ["bare_clean_short", "described_clean_short"],
            [row["id"] for row in document["requests"]],
        )
        self.assertEqual(["a.", "a."], [row["text"] for row in document["requests"]])
        self.assertEqual(
            {
                "bare_clean_short": {
                    "input_tokens": 18,
                    "processor_text_tokens": 2,
                    "schema_token_lengths": [8],
                },
                "described_clean_short": {
                    "input_tokens": 24,
                    "processor_text_tokens": 2,
                    "schema_token_lengths": [8],
                },
            },
            document["prepared_geometry"],
        )

    def test_checked_in_clean_captures_are_byte_and_geometry_pinned(self) -> None:
        for name, expected in CAPTURES.items():
            payload = (CAPTURE_DIR / name).read_bytes()
            self.assertEqual(expected["size"], len(payload))
            self.assertEqual(expected["sha256"], hashlib.sha256(payload).hexdigest())
            report = json.loads(payload)
            self.assertFalse(report["semantic_pass"])
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
                self.assertEqual(18 if index == 0 else 24, len(row["encoded"]["input_ids"]))
                self.assertEqual([0, 1], row["encoded"]["start_mappings"])
                self.assertEqual([1, 2], row["encoded"]["end_mappings"])
                values = row["native_expected"]["entities"][0]["values"]
                self.assertEqual(expected["value_counts"][index], len(values))
                for value in values:
                    self.assertLessEqual(value["source"]["end"], len(row["text"]))


if __name__ == "__main__":
    unittest.main()
