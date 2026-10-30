#!/usr/bin/env python3
"""Offline contract checks for retained GLiNER2 specialty fixtures."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "zig/pkg/inference/testdata/gliner_specialty"
QUALIFICATION = ROOT / "zig/pkg/inference/src/models/gliner_specialty_qualification.zig"
LOCKED_PACKAGES = {
    "PyYAML":"6.0.3", "accelerate":"1.10.1", "huggingface-hub":"0.36.0",
    "numpy":"2.3.5", "packaging":"25.0", "peft":"0.17.1",
    "psutil":"7.1.3", "pydantic":"2.12.3", "requests":"2.32.5",
    "safetensors":"0.7.0", "scipy":"1.16.3", "tokenizers":"0.21.4",
    "torch":"2.9.1", "transformers":"4.55.4", "urllib3":"2.5.0",
}

def load(path: Path): return json.loads(path.read_text())
def digest(path: Path): return hashlib.sha256(path.read_bytes()).hexdigest()

def load_capture_module():
    path = ROOT / "scripts/gliner_specialty/capture_oracle.py"
    spec = importlib.util.spec_from_file_location("gliner_specialty_capture", path)
    if spec is None or spec.loader is None: raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module

class FixtureContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = load(FIXTURES / "manifest.json")
        cls.cases = load(FIXTURES / "cases.json")
        cls.pii = load(FIXTURES / "pii_multi_oracle.json")
        cls.guard = load(FIXTURES / "gliguard_oracle.json")

    def test_manifest_file_receipts(self):
        cases = self.manifest["cases"]
        self.assertEqual(cases["sha256"], digest(FIXTURES / cases["path"]))
        for capture in self.manifest["captures"].values():
            path = FIXTURES / capture["path"]
            self.assertEqual(capture["size_bytes"], path.stat().st_size)
            self.assertEqual(capture["sha256"], digest(path))
        for profile, model in self.manifest["models"].items():
            for relative, receipt in model["metadata_fixtures"].items():
                path = FIXTURES / profile / relative
                self.assertEqual(receipt["size_bytes"], path.stat().st_size)
                self.assertEqual(receipt["sha256"], digest(path))

    def test_model_pins_match_qualification_source(self):
        source = QUALIFICATION.read_text()
        for profile, prefix in (("pii_multi","pii"),("gliguard","gliguard")):
            model = self.manifest["models"][profile]
            revision = re.search(rf'pub const {prefix}_revision = "([0-9a-f]{{40}})";', source)
            pin = re.search(rf'pub const {prefix}_weight = FilePin\{{[^}}]*\.size_bytes = ([0-9_]+), \.sha256 = "([0-9a-f]{{64}})"', source)
            tokenizer = re.search(rf'pub const {prefix}_tokenizer = FilePin\{{[^}}]*\.size_bytes = ([0-9_]+), \.sha256 = "([0-9a-f]{{64}})"', source)
            inventory = re.search(rf'\.count = if \(pii\) 227 else 255, \.all_f32 = true', source)
            self.assertIsNotNone(revision); self.assertIsNotNone(pin); self.assertIsNotNone(tokenizer); self.assertIsNotNone(inventory)
            self.assertEqual(model["revision"], revision.group(1))
            self.assertEqual(model["model_safetensors_size"], int(pin.group(1).replace("_","")))
            self.assertEqual(model["model_safetensors_sha256"], pin.group(2))
            self.assertEqual(model["tokenizer_json_size"], int(tokenizer.group(1).replace("_","")))
            self.assertEqual(model["tokenizer_json_sha256"], tokenizer.group(2))
            self.assertEqual({"count":227 if profile == "pii_multi" else 255,"all_f32":True}, model["tensor_inventory"])

    def test_case_coverage(self):
        self.assertEqual(42, len(self.cases["pii_labels"]))
        self.assertTrue({"en","fr","es","de","it","pt","nl"}.issubset({case["language"] for case in self.cases["pii_cases"]}))
        self.assertEqual(6, len(self.cases["gliguard_tasks"]))
        seen = {name for case in self.cases["gliguard_cases"] for name in case["tasks"]}
        self.assertEqual(set(self.cases["gliguard_tasks"]), seen)
        self.assertTrue(any(len(case["tasks"]) == 6 for case in self.cases["gliguard_cases"]))
        pii_by_id = {case["id"]:case for case in self.cases["pii_cases"]}
        self.assertIn("no_pii_empty_output", pii_by_id)
        self.assertTrue(any("labels" in case and len(case["labels"]) < 42 for case in self.cases["pii_cases"]))
        self.assertTrue(any("entity_thresholds" in case for case in self.cases["pii_cases"]))
        self.assertTrue(any(case.get("request_options", {}).get("overlap") == "allow" for case in self.cases["pii_cases"]))
        self.assertTrue(any(case.get("request_options", {}).get("offset_unit") == "unicode_codepoints" for case in self.cases["pii_cases"]))

    def test_bounded_geometry_and_head_shapes(self):
        self.assertEqual(2, self.pii["format_version"])
        self.assertEqual(1, self.guard["format_version"])
        for case in self.pii["cases"]:
            prep = case["preprocessing"]
            self.assertTrue(0 < len(prep["active_input_ids"]) <= 512)
            self.assertEqual(len(case["labels"]), len(prep["query_marker_positions"]))
            self.assertEqual([1,20], case["heads"][0]["count_logits"]["shape"])
            head = case["heads"][0]
            if head["predicted_count"] == 0:
                self.assertNotIn("raw_span_logits", head)
            else:
                shape = head["raw_span_logits"]["shape"]
                words = len(prep["text_tokens"])
                valid_spans = sum(min(8, words - word) for word in range(words))
                self.assertEqual([head["predicted_count"],len(case["labels"]),valid_spans], shape)
                self.assertEqual(shape[0] * shape[1] * shape[2], len(head["raw_span_logits"]["values"]))
        for case in self.guard["cases"]:
            prep = case["preprocessing"]
            self.assertTrue(0 < len(prep["active_input_ids"]) <= 512)
            for head in case["heads"]:
                width = len(head["labels"])
                self.assertEqual([width], head["raw_logits"]["shape"])
                self.assertEqual([width], head["probabilities"]["shape"])
                self.assertEqual(width, len(head["raw_logits"]["values"]))
                self.assertEqual(width, len(head["probabilities"]["values"]))

    def test_valid_span_packing_order_is_slot_label_word_width(self):
        torch_was_loaded = "torch" in sys.modules
        capture = load_capture_module()
        self.assertEqual(torch_was_loaded, "torch" in sys.modules)
        values = list(range(2 * 2 * 3 * 8))
        packed = capture.pack_valid_span_tensor(
            {"shape":[2,2,3,8],"values":values}
        )
        self.assertEqual([2,2,6], packed["shape"])
        expected = []
        for slot in range(2):
            for label in range(2):
                base = (slot * 2 + label) * 3 * 8
                expected.extend(values[base:base + 3])
                expected.extend(values[base + 8:base + 10])
                expected.extend(values[base + 16:base + 17])
        self.assertEqual(expected, packed["values"])

    def test_pii_option_semantics_and_offsets(self):
        cases = {case["id"]:case for case in self.pii["cases"]}
        empty = cases["no_pii_empty_output"]
        # The pinned PII checkpoint's entity count head selects one even for
        # benign input: upstream training excludes entity tasks from count loss.
        # Keep this observed prior explicit; count-zero gating needs a focused
        # synthetic head test rather than a fabricated model oracle.
        self.assertEqual(1, empty["heads"][0]["predicted_count"])
        self.assertFalse(any(empty["presented_output"]["entities"].values()))
        self.assertEqual(["email","phone_number","government_id"], empty["labels"])
        self.assertEqual({"person":0.99999,"email":0.1}, cases["per_label_threshold"]["entity_thresholds"])
        self.assertEqual("allow", cases["explicit_overlap_allow"]["request_options"]["overlap"])
        unicode = cases["unicode_codepoint_offsets"]
        self.assertEqual("unicode_codepoints", unicode["preprocessing"]["offset_unit"])
        for values in unicode["presented_output"]["entities"].values():
            for entity in values:
                self.assertEqual(entity["text"], unicode["text"][entity["start"]:entity["end"]])

    def test_canonical_unshimmed_runtime_receipt(self):
        self.assertEqual("captured_locked_cpu_fp32", self.manifest["status"])
        for capture in (self.pii, self.guard):
            runtime = capture["runtime"]
            self.assertEqual(LOCKED_PACKAGES, runtime["packages"])
            self.assertEqual("2.0.0", runtime["gliner2_version"])
            self.assertNotIn("compatibility_shim", runtime)
            self.assertEqual({"accelerate","peft"}, set(runtime["worktree_local_modules"]))
            self.assertEqual("gliner2/__init__.py", runtime["gliner2_module"])

if __name__ == "__main__": unittest.main()
