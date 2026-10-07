from __future__ import annotations

import hashlib
import json
import unittest

import capture_family_references as capture


HERE = capture.HERE
REQUESTS = HERE / "family_general_entity_reference_requests.json"
REQUESTS_SHA256 = "0362626845854c6f3893c9640b4a8c207b0463168dd2083cedcbeeda5c2e98dc"
EXPECTED_GEOMETRY = {
    "bare_short": {"input_tokens": 18, "processor_text_tokens": 2, "schema_token_lengths": [8]},
    "bare_long": {"input_tokens": 181, "processor_text_tokens": 114, "schema_token_lengths": [12]},
    "described_short": {"input_tokens": 24, "processor_text_tokens": 2, "schema_token_lengths": [8]},
    "described_long": {"input_tokens": 202, "processor_text_tokens": 114, "schema_token_lengths": [12]},
}
CAPTURE_DIR = HERE.parent.parent / "testdata" / "gliner25" / "family"
CAPTURES = {
    "multi_v1_general_entity_capture.json": {
        "sha256": "9a3ded225a5aa5825b2a5acba7e27fe136abd7fe1f889ff9601a450f61582f9a",
        "size": 52321,
        "semantic": [False, True, False, True],
        "long_location_values": [2, 2],
    },
    "multi_decide_general_entity_capture.json": {
        "sha256": "71858130c7ef31ac60c7c30c51b772f6a22089adde5ea46e186cb3047aeb7dc4",
        "size": 48056,
        "semantic": [False, False, False, False],
        "long_location_values": [0, 0],
    },
}


class CaptureFamilyGeneralEntityReferencesTest(unittest.TestCase):
    def test_corpus_pins_bare_and_described_short_long_bounds(self) -> None:
        payload = REQUESTS.read_bytes()
        self.assertEqual(REQUESTS_SHA256, hashlib.sha256(payload).hexdigest())
        document = capture.validate_requests(REQUESTS)
        self.assertEqual("general_entity_endpoint_bounds_reference", document["scope"])
        self.assertEqual(EXPECTED_GEOMETRY, document["prepared_geometry"])
        self.assertEqual(list(EXPECTED_GEOMETRY), [row["id"] for row in document["requests"]])
        self.assertTrue(all(row["kind"] == "extract" for row in document["requests"]))
        self.assertEqual((1, 1), self._text_bounds(document["requests"][0]))
        self.assertEqual((663, 100), self._text_bounds(document["requests"][1]))
        self.assertEqual((1, 1), self._text_bounds(document["requests"][2]))
        self.assertEqual((663, 100), self._text_bounds(document["requests"][3]))

    def test_native_schema_preserves_bare_and_described_entity_contracts(self) -> None:
        rows = capture.validate_requests(REQUESTS)["requests"]
        bare_short, bare_long, described_short, described_long = [
            capture.native_schema_for(row) for row in rows
        ]
        self.assertEqual({"entities": ["x"]}, bare_short)
        self.assertEqual(["person", "organization", "location"], bare_long["entities"])
        self.assertNotIn("entity_definitions", bare_long)
        self.assertEqual({"description": "x"}, described_short["entity_definitions"]["x"])
        self.assertEqual(
            ["person", "organization", "location"], described_long["entities"]
        )
        self.assertEqual(
            {"person", "organization", "location"},
            set(described_long["entity_definitions"]),
        )

    def test_checked_in_captures_pin_geometry_and_source_quality(self) -> None:
        for name, expected in CAPTURES.items():
            payload = (CAPTURE_DIR / name).read_bytes()
            self.assertEqual(expected["size"], len(payload))
            self.assertEqual(expected["sha256"], hashlib.sha256(payload).hexdigest())
            report = json.loads(payload)
            self.assertFalse(report["semantic_pass"])
            self.assertFalse(report["qualification"])
            self.assertEqual(4, len(report["requests"]))
            self.assertEqual(REQUESTS_SHA256, report["artifacts"]["requests_sha256"])
            self.assertEqual(
                "4e9fee278e8e4757f1ee03aec87208dfc7cbb58eb7a009b30e8f525c8eb9cde5",
                report["artifacts"]["generator_sha256"],
            )
            self.assertEqual(
                expected["semantic"],
                [row["semantic"]["pass"] for row in report["requests"]],
            )
            for row in report["requests"]:
                geometry = EXPECTED_GEOMETRY[row["id"]]
                self.assertEqual(geometry["input_tokens"], len(row["encoded"]["input_ids"]))
                self.assertEqual(
                    geometry["processor_text_tokens"], len(row["encoded"]["text_tokens"])
                )
                self.assertEqual(
                    geometry["schema_token_lengths"],
                    [len(tokens) for tokens in row["encoded"]["schema_tokens"]],
                )
            long_rows = [report["requests"][1], report["requests"][3]]
            self.assertEqual(
                expected["long_location_values"],
                [self._entity_value_count(row, "location") for row in long_rows],
            )

    @staticmethod
    def _entity_value_count(row: dict, entity: str) -> int:
        item = next(value for value in row["native_expected"]["entities"] if value["name"] == entity)
        return len(item["values"])

    @staticmethod
    def _text_bounds(row: dict) -> tuple[int, int]:
        return len(row["text"].encode()), len(row["text"].split())


if __name__ == "__main__":
    unittest.main()
