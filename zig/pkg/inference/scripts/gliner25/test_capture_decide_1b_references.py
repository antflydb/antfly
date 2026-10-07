from __future__ import annotations

import json
import base64
import hashlib
import tempfile
import unittest
from pathlib import Path

import capture_decide_1b_references as capture
import verify_family_contract as family


CAPTURE_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "testdata"
    / "gliner25"
    / "family"
    / "decide_1b_capture.json"
)
CAPTURE_SHA256 = "45828bb5e2d00812299d2a2b778d37a215bef231b821d74791a1d9d40335d34b"


class CaptureDecide1BReferencesTest(unittest.TestCase):
    def test_request_matrix_is_classification_only_and_multilingual(self) -> None:
        document = capture.validate_requests(capture.DEFAULT_REQUESTS)
        self.assertEqual(8, len(document["requests"]))
        self.assertTrue(all(row["kind"] == "classification" for row in document["requests"]))
        text = " ".join(row["text"] for row in document["requests"])
        for marker in ("refund", "cancelar", "キャンセル", "تغيير"):
            self.assertIn(marker, text + json.dumps(document, ensure_ascii=False))

    def test_long_context_row_targets_the_local_attention_cutoff(self) -> None:
        document = capture.validate_requests(capture.DEFAULT_REQUESTS)
        row = next(row for row in document["requests"] if row["id"] == "long_context_cutoff")
        self.assertEqual((658, 100), (len(row["text"].encode()), len(row["text"].split())))
        task = row["schema"]["tasks"]["topic"]
        self.assertEqual((1, 1), (task["min_labels"], task["max_labels"]))
        self.assertIsInstance(task["labels"], dict)
        self.assertIn("instruction", task)
        self.assertEqual(
            {
                "input_tokens": 198,
                "processor_text_tokens": 114,
                "schema_token_lengths": [12],
            },
            document["prepared_geometry"]["long_context_cutoff"],
        )

    def test_final_capture_matrix_is_exactly_eight_plus_two(self) -> None:
        regular = capture.validate_requests(capture.DEFAULT_REQUESTS)
        public = capture.public_decide.validate_requests(capture.public_decide.DEFAULT_REQUESTS)
        self.assertEqual(8, len(regular["requests"]))
        self.assertEqual(2, len(public["requests"]))

    def test_capture_rejects_prepared_geometry_drift(self) -> None:
        expected = capture.validate_requests(capture.DEFAULT_REQUESTS)["prepared_geometry"]
        row = {
            "id": "long_context_cutoff",
            "encoded": {
                "input_ids": list(range(198)),
                "text_tokens": ["word"] * 114,
                "schema_tokens": [["schema"] * 12],
            },
        }
        capture.verify_prepared_geometry([row], expected)
        row["encoded"]["input_ids"].pop()
        with self.assertRaisesRegex(capture.common.CaptureError, "geometry differs"):
            capture.verify_prepared_geometry([row], expected)

    def test_request_matrix_rejects_extraction(self) -> None:
        document = json.loads(capture.DEFAULT_REQUESTS.read_text())
        document["requests"][0]["kind"] = "extract"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "requests.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(capture.common.CaptureError, "classification"):
                capture.validate_requests(path)

    def test_isolated_runtime_tree_rejects_recorded_code_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            code = root / "package.py"
            code.write_bytes(b"trusted\n")
            info = root / "package-1.0.dist-info"
            info.mkdir()
            digest = base64.urlsafe_b64encode(hashlib.sha256(code.read_bytes()).digest()).decode().rstrip("=")
            record = info / "RECORD"
            record.write_text(
                f"package.py,sha256={digest},{code.stat().st_size}\n"
                "package-1.0.dist-info/RECORD,,\n",
                encoding="utf-8",
            )
            identity = capture.verify_record_tree(root, None)
            self.assertEqual(2, identity["files"])
            code.write_bytes(b"tampered\n")
            with self.assertRaisesRegex(capture.common.CaptureError, "differs"):
                capture.verify_record_tree(root, identity["tree_sha256"])

    def test_contract_pins_checkpoint_runtime_and_both_rope_layer_types(self) -> None:
        contract = family.strict_json(family.CONTRACT_PATH)
        runtime = family.strict_json(capture.DEFAULT_RUNTIME_CONTRACT)["oracle_runtime_decide_1b"]
        self.assertEqual(
            capture.sha256(Path(capture.public_decide.__file__)),
            runtime["public_decide_helper_sha256"],
        )
        self.assertEqual(
            2,
            len(capture.public_decide.validate_requests(capture.public_decide.DEFAULT_REQUESTS)["requests"]),
        )
        self.assertEqual("5.17.0", runtime["isolated_packages"]["transformers"])
        self.assertEqual(
            runtime["compatibility"]["checkpoint_transformers_version"],
            runtime["isolated_packages"]["transformers"],
        )
        self.assertEqual(
            ["full_attention", "sliding_attention"], runtime["rope_contract"]["layer_types"]
        )
        self.assertEqual(160000.0, runtime["rope_contract"]["rope_theta"])
        self.assertEqual(
            "checkpoint_runtime_outside_upstream_declared_dependency_range",
            runtime["compatibility"]["status"],
        )

    def test_checked_in_capture_is_byte_pinned_and_truthfully_unqualified(self) -> None:
        payload = CAPTURE_PATH.read_bytes()
        self.assertEqual(100296, len(payload))
        self.assertEqual(CAPTURE_SHA256, hashlib.sha256(payload).hexdigest())
        report = json.loads(payload)
        self.assertEqual(8, len(report["requests"]))
        self.assertEqual(2, len(report["public_decide_requests"]))
        self.assertFalse(report["semantic_pass"])
        self.assertFalse(report["qualification"])
        self.assertFalse(report["native_runtime_qualified"])
        self.assertFalse(report["production_qualified"])
        self.assertEqual(
            "02c567d791aed26550d300064c7f0c0094fd65291503c65969b45b30786e33b3",
            report["model"]["model_sha256"],
        )
        self.assertEqual(
            "ad272a7ec1aec50c0156c824d8ea67d4e7d8a186201b3c1a768a1264075512dd",
            report["artifacts"]["generator_sha256"],
        )
        self.assertEqual(
            "b97d2c69860e3496b3f4d01df055c54a42b6b3b78e95fb33cbc83dbb371e8271",
            report["artifacts"]["requests_sha256"],
        )
        self.assertEqual(
            198,
            max(
                len(row["encoded"]["input_ids"])
                for row in report["requests"] + report["public_decide_requests"]
            ),
        )
        failed = [row["id"] for row in report["requests"] if not row["semantic"]["pass"]]
        self.assertEqual(["instruction_prompt"], failed)


if __name__ == "__main__":
    unittest.main()
