from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import capture_family_endpoint_references as capture


CAPTURE_DIR = capture.HERE.parent.parent / "testdata" / "gliner25" / "family"
CAPTURES = {
    "multi_v1_endpoint_capture.json": "db0a4bb64f8d8dd3d76b443adb6c46c6e4bf1a19a0e42064798fd021b31acfba",
    "multi_decide_endpoint_capture.json": "ec6c438800d1ce63fccc592825af601befa628c9f46679a41b0250b8725a94e1",
}


class CaptureFamilyEndpointReferencesTest(unittest.TestCase):
    def test_single_mixed_schema_and_endpoint_bounds_are_fixed(self) -> None:
        document = capture.validate_requests(capture.DEFAULT_REQUESTS)
        self.assertEqual(1, len(document["requests"][0]["text"].encode()))
        self.assertEqual(658, len(document["requests"][1]["text"].encode()))
        self.assertEqual(100, len(document["requests"][1]["text"].split()))
        native = document["native_schema"]
        self.assertTrue(all(key in native for key in ("entities", "relations", "structures", "classifications")))
        task = native["classifications"][0]
        self.assertEqual((1, 1), (task["min_labels"], task["max_labels"]))
        self.assertEqual(set(task["labels"]), set(task["label_definitions"]))
        self.assertIn("prompt", task)
        native_json = json.dumps(native, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
        self.assertLess(native_json.index('"person"'), native_json.index('"organization"'))
        self.assertLess(native_json.index('"organization"'), native_json.index('"role"'))

    def test_structured_selection_is_the_native_classification_oracle(self) -> None:
        document = capture.validate_requests(capture.DEFAULT_REQUESTS)
        structured = {
            "topic": {
                "value": "employment",
                "confidence": 0.7,
                "probabilities": {"employment": 0.7, "product": 0.2, "other": 0.1},
            }
        }
        expected = capture.common.canonical_expected(
            {"kind": "extract"}, document["native_schema"], structured
        )
        self.assertEqual(
            [{"label": "employment", "confidence": 0.7}],
            expected["classifications"][0]["labels"],
        )

    def test_bound_fixture_rejects_split_or_drifted_schema(self) -> None:
        document = json.loads(capture.DEFAULT_REQUESTS.read_text())
        document["extract_schema"].pop("relations")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "requests.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(capture.common.CaptureError, "entities, relations, and structures"):
                capture.validate_requests(path)

    def test_checked_in_captures_are_byte_pinned_and_unqualified(self) -> None:
        for name, expected_sha in CAPTURES.items():
            payload = (CAPTURE_DIR / name).read_bytes()
            self.assertEqual(expected_sha, hashlib.sha256(payload).hexdigest())
            report = json.loads(payload)
            self.assertFalse(report["qualification"])
            self.assertFalse(report["native_runtime_qualified"])
            self.assertFalse(report["production_qualified"])
            self.assertTrue(report["capture_pass"])
            self.assertEqual(2, len(report["requests"]))
            self.assertEqual(capture.FROZEN_COMMON_SHA256, report["artifacts"]["shared_helper_sha256"])
            self.assertEqual(
                "5fb775edf439803b75ed7d131c330e98e6ae9a355caa7ce2fb1695f0431f4de7",
                report["artifacts"]["generator_sha256"],
            )
            self.assertEqual(
                "de6ba26cd1d11ab598be5b8e6120d4fc81718fe9fd2e6419ab612e3926e480ff",
                report["artifacts"]["requests_sha256"],
            )
            self.assertEqual(
                "55656fbfa01d3d4a77485e1a1eeeaf682990ccdf",
                report["source"]["revision"],
            )
            disclosure = report["source"]["advanced_classification_composition"]
            self.assertIn("min_labels", disclosure["joint_schema_parser_limit"])
            self.assertIn("same joint Schema request", disclosure["model_wire"])
            self.assertIn("exact unscaled", disclosure["structured_decode"])


if __name__ == "__main__":
    unittest.main()
