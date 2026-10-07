from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

import capture_family_references as capture


HERE = Path(__file__).resolve().parent
REQUESTS = HERE / "family_classification_floor_reference_requests.json"
CAPTURE_DIR = HERE.parent.parent / "testdata" / "gliner25" / "family"
CAPTURES = {
    "multi_v1_classification_floor_capture.json": (
        "ca2bc9e223ec7ecf8bddfef463a5fbdf9eacd3d7cecbefe4c4d10b19fefa7d1a",
        "a",
    ),
    "multi_decide_classification_floor_capture.json": (
        "fa22eb5c7af39e23f5c58f3f237f81365c4daf6cfc1e280436d4760862bbf85a",
        "other",
    ),
}


class CaptureFamilyClassificationFloorReferencesTest(unittest.TestCase):
    def test_request_is_the_exact_one_character_two_label_floor(self) -> None:
        document = capture.validate_requests(REQUESTS)
        self.assertEqual(1, len(document["requests"]))
        request = document["requests"][0]
        self.assertEqual("a", request["text"])
        task = request["schema"]["tasks"]["answer"]
        self.assertEqual(["a", "other"], task["labels"])
        self.assertEqual((1, 1), (task["min_labels"], task["max_labels"]))

    def test_checked_in_floor_captures_are_byte_pinned(self) -> None:
        for name, (expected_sha, selected) in CAPTURES.items():
            payload = (CAPTURE_DIR / name).read_bytes()
            self.assertEqual(expected_sha, hashlib.sha256(payload).hexdigest())
            report = json.loads(payload)
            self.assertFalse(report["qualification"])
            self.assertEqual(
                "4e9fee278e8e4757f1ee03aec87208dfc7cbb58eb7a009b30e8f525c8eb9cde5",
                report["artifacts"]["generator_sha256"],
            )
            self.assertEqual(
                "c5fa0407f71a1f2e62e6ba5b0cfb7aecdeda2a08298116ad4eb73ec0b9898314",
                report["artifacts"]["requests_sha256"],
            )
            row = report["requests"][0]
            self.assertEqual(18, len(row["encoded"]["input_ids"]))
            self.assertEqual([selected], row["selected"]["answer"])


if __name__ == "__main__":
    unittest.main()
