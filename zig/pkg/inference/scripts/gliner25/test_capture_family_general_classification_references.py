from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

import capture_family_references as capture


HERE = Path(__file__).resolve().parent
REQUESTS = HERE / "family_general_classification_reference_requests.json"
CAPTURE_DIR = HERE.parent.parent / "testdata" / "gliner25" / "family"
REQUESTS_SHA256 = "b4c3fe4d67f5f092bb9c9d5781d524011499dd954d348f2ff4fd8e6ee54520ab"
CAPTURES = {
    "multi_v1_general_classification_capture.json": (
        "51b76c1cfeee1a444df6f6ab423ac6a41ad1f85dbe675d21edc7c7ed8fc8ff7a",
        {
            "bare_long": (183, "employment", True),
            "described_short": (55, "other", False),
            "described_long": (218, "employment", True),
            "prompt_described_short": (66, "other", False),
            "prompt_described_long": (229, "employment", True),
            "prompt_short": (31, "product", False),
            "prompt_long": (194, "other", False),
        },
    ),
    "multi_decide_general_classification_capture.json": (
        "2236d50308a00649ae0e134189b94f0cd06289f1e9aa86db5cc7781219c1cbc2",
        {
            "bare_long": (183, "product", False),
            "described_short": (55, "other", False),
            "described_long": (218, "employment", True),
            "prompt_described_short": (66, "other", False),
            "prompt_described_long": (229, "employment", True),
            "prompt_short": (31, "other", False),
            "prompt_long": (194, "product", False),
        },
    ),
}


class CaptureFamilyGeneralClassificationReferencesTest(unittest.TestCase):
    def test_request_matrix_has_exact_independent_feature_bounds(self) -> None:
        document = capture.validate_requests(REQUESTS)
        self.assertEqual(
            [
                "bare_long",
                "described_short",
                "described_long",
                "prompt_described_short",
                "prompt_described_long",
                "prompt_short",
                "prompt_long",
            ],
            [request["id"] for request in document["requests"]],
        )
        for request in document["requests"]:
            task = request["schema"]["tasks"]["topic"]
            self.assertEqual((1, 1), (task["min_labels"], task["max_labels"]))
            if request["id"].endswith("short"):
                self.assertEqual((1, 1), (len(request["text"].encode()), len(request["text"].split())))
            else:
                self.assertEqual((658, 100), (len(request["text"].encode()), len(request["text"].split())))

    def test_checked_in_captures_pin_geometry_and_observed_semantics(self) -> None:
        for name, (expected_sha, expected_rows) in CAPTURES.items():
            payload = (CAPTURE_DIR / name).read_bytes()
            self.assertEqual(expected_sha, hashlib.sha256(payload).hexdigest())
            report = json.loads(payload)
            self.assertFalse(report["qualification"])
            self.assertFalse(report["semantic_pass"])
            self.assertEqual(REQUESTS_SHA256, report["artifacts"]["requests_sha256"])
            self.assertEqual(
                "4e9fee278e8e4757f1ee03aec87208dfc7cbb58eb7a009b30e8f525c8eb9cde5",
                report["artifacts"]["generator_sha256"],
            )
            self.assertEqual(7, len(report["requests"]))
            for row in report["requests"]:
                input_tokens, selected, semantic_pass = expected_rows[row["id"]]
                self.assertEqual(input_tokens, len(row["encoded"]["input_ids"]))
                self.assertEqual([selected], row["selected"]["topic"])
                self.assertEqual(semantic_pass, row["semantic"]["pass"])


if __name__ == "__main__":
    unittest.main()
