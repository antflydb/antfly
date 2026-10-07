from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import capture_family_references as capture


class CaptureFamilyReferencesTest(unittest.TestCase):
    def write_requests(self, root: Path, requests: list[dict]) -> Path:
        path = root / "requests.json"
        path.write_text(
            json.dumps({"format_version": 1, "requests": requests}), encoding="utf-8"
        )
        return path

    def test_checked_in_multilingual_request_matrix_is_bounded(self) -> None:
        document = capture.validate_requests(capture.DEFAULT_REQUESTS)
        self.assertEqual(11, len(document["requests"]))
        self.assertEqual(
            {"extract", "classification"},
            {request["kind"] for request in document["requests"]},
        )
        combined = " ".join(request["text"] for request in document["requests"])
        for text in ("María", "田中", "ليلى"):
            self.assertIn(text, combined)

    def test_request_contract_rejects_duplicates_oversize_and_missing_expectation(self) -> None:
        base = {
            "id": "valid",
            "kind": "classification",
            "text": "hello",
            "schema": {"tasks": {}},
            "expect": {"selected": {}},
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write_requests(root, [base, dict(base)])
            with self.assertRaisesRegex(capture.CaptureError, "unique"):
                capture.validate_requests(path)
            path = self.write_requests(root, [{**base, "text": "x" * 4097}])
            with self.assertRaisesRegex(capture.CaptureError, "oversized text"):
                capture.validate_requests(path)
            missing = dict(base)
            missing.pop("expect")
            path = self.write_requests(root, [missing])
            with self.assertRaisesRegex(capture.CaptureError, "expectation"):
                capture.validate_requests(path)

    def test_semantic_checks_cover_exact_subset_and_extracted_text(self) -> None:
        output = {"entities": {"person": [{"text": "María González"}]}}
        result = capture.semantic_result(
            {"contains_text": ["María González"]}, output, None
        )
        self.assertTrue(result["pass"])

    def test_inference_peft_shim_exposes_only_inactive_type_imports(self) -> None:
        names = ("peft", "peft.tuners", "peft.tuners.lora", "peft.tuners.lora.layer")
        saved = {name: sys.modules.get(name) for name in names}
        try:
            for name in names:
                sys.modules.pop(name, None)
            identity = capture.install_inference_peft_shim()
            from peft import PeftModel
            from peft.tuners.lora.layer import LoraLayer

            self.assertFalse(identity["executed_peft_code"])
            self.assertFalse(isinstance(object(), PeftModel))
            self.assertFalse(isinstance(object(), LoraLayer))
        finally:
            for name in names:
                sys.modules.pop(name, None)
                if saved[name] is not None:
                    sys.modules[name] = saved[name]

    def test_native_schema_preserves_descriptions_prompts_and_structure_fields(self) -> None:
        requests = capture.validate_requests(capture.DEFAULT_REQUESTS)["requests"]
        by_id = {request["id"]: request for request in requests}
        described = capture.native_schema_for(by_id["described_labels"])
        task = described["classifications"][0]
        self.assertEqual("intent", task["name"])
        self.assertEqual(
            "The customer needs a new PIN or did not receive it",
            task["label_definitions"]["card_pin_change"]["description"],
        )
        prompted = capture.native_schema_for(by_id["instruction_prompt"])
        self.assertIn("instruction", prompted["classifications"][0])
        full = capture.native_schema_for(by_id["english_full_task"])
        self.assertEqual("str", full["structures"]["employment"]["fields"]["person"]["type"])
        self.assertEqual(
            ["person", "organization", "title"],
            list(full["structures"]["employment"]["fields"]),
        )
        self.assertEqual("A named individual", full["entity_definitions"]["person"]["description"])

    def test_native_expected_adapts_exclusive_classification_and_offsets(self) -> None:
        request = {
            "kind": "classification",
            "schema": {"tasks": {"intent": {"labels": ["refund", "sales"]}}},
        }
        schema = capture.native_schema_for(request)
        expected = capture.canonical_expected(
            request,
            schema,
            {
                "intent": {
                    "value": "refund",
                    "confidence": 0.8,
                    "probabilities": {"refund": 0.8, "sales": 0.2},
                }
            },
        )
        self.assertEqual(
            [{"label": "refund", "confidence": 0.8}],
            expected["classifications"][0]["labels"],
        )

        entity_request = {"kind": "extract", "schema": {"entities": ["person"]}}
        entity_schema = capture.native_schema_for(entity_request)
        expected = capture.canonical_expected(
            entity_request,
            entity_schema,
            {"entities": {"person": [{"text": "María", "confidence": 0.9, "start": 0, "end": 5}]}},
        )
        self.assertEqual(
            {"start": 0, "end": 5}, expected["entities"][0]["values"][0]["source"]
        )
        result = capture.semantic_result(
            {"selected": {"intent": ["refund"]}}, {}, {"intent": ["sales"]}
        )
        self.assertFalse(result["pass"])
        result = capture.semantic_result(
            {"selected_contains": {"aspects": ["battery", "screen"]}},
            {},
            {"aspects": ["screen", "battery", "keyboard"]},
        )
        self.assertTrue(result["pass"])


if __name__ == "__main__":
    unittest.main()
