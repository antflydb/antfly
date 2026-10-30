# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Model-independent checks on the pinned specialty oracle contract."""
import hashlib
import json
from pathlib import Path
import unittest

import capture_oracle as oracle
from benchmark_oracle import trace_counters


class SpecialtyContractTests(unittest.TestCase):
    def test_pii_cases_cover_documented_languages_and_all_labels(self):
        cases = oracle.cases("pii")
        self.assertEqual({"en", "fr", "es", "de", "it", "pt", "nl"},
                         {case["language"] for case in cases if "language" in case})
        self.assertEqual(42, len(oracle.PII_LABELS))
        self.assertEqual(42, len(set(oracle.PII_LABELS)))
        full = next(case for case in cases if case["id"] == "all_42_labels")
        self.assertEqual(oracle.PII_LABELS, full["schema"]["entities"])
        for case in cases:
            self.assertEqual({"entities"}, set(case["schema"]))

    def test_gliguard_six_tasks_are_individual_and_composed(self):
        cases = oracle.cases("gliguard")
        names = [task["name"] for task in oracle.GUARD_TASKS]
        self.assertEqual(6, len(set(names)))
        for name in names:
            case = next(case for case in cases if case["id"] == name)
            self.assertEqual([name], [task["name"] for task in case["schema"]["classifications"]])
        composed = next(case for case in cases if case["id"] == "composed_all_six")
        self.assertEqual(names, [task["name"] for task in composed["schema"]["classifications"]])
        for task in oracle.GUARD_TASKS:
            if "toxicity" in task["name"] or task["name"] == "jailbreak_detection":
                self.assertTrue(task["multi_label"])
                self.assertEqual(0.4, task["threshold"])
            else:
                self.assertFalse(task.get("multi_label", False))

    def test_frozen_oracles_bind_the_model_and_upstream_source(self):
        manifest = json.loads((oracle.FIXTURES / "manifest.json").read_text())
        for name, model in manifest["models"].items():
            if "oracle" not in model:
                continue
            pin = model["oracle"]
            path = oracle.FIXTURES / pin["path"]
            self.assertEqual(pin["size_bytes"], path.stat().st_size)
            self.assertEqual(pin["sha256"], oracle.digest_file(path))
            capture = json.loads(path.read_text())
            self.assertEqual(model["files"], capture["model"]["files"])
            self.assertEqual(manifest["upstream"], capture["upstream"])
            self.assertEqual("unicode_codepoints", capture["coordinate_unit"])
            self.assertEqual([case["id"] for case in oracle.cases(name)], [case["id"] for case in capture["cases"]])
            for case in capture["cases"]:
                self.assertLessEqual(len(case["input_ids"]), 512)
                for entity in case["expected_entities"]:
                    self.assertEqual(entity["text"], case["text"][entity["start"]:entity["end"]])

    def test_upstream_decoder_edges_are_frozen_independently_of_model_outputs(self):
        manifest = json.loads((oracle.FIXTURES / "manifest.json").read_text())
        pin = manifest["decoder_oracle"]
        path = oracle.FIXTURES / pin["path"]
        self.assertEqual(pin["sha256"], oracle.digest_file(path))
        self.assertEqual(pin["size_bytes"], path.stat().st_size)
        capture = json.loads(path.read_text())
        greedy = capture["overlap_cases"][0]
        self.assertEqual(["long"], [span[0] for span in greedy["omitted_overlap"]])
        self.assertEqual(["left", "right"], [span[0] for span in greedy["explicit_flat"]])
        tied = capture["overlap_cases"][1]
        self.assertEqual(["first", "third"], [span[0] for span in tied["omitted_overlap"]])
        gates = capture["count_threshold_unicode_cases"]
        self.assertEqual({"entities": {}}, gates[0]["expected"])
        for gate in gates[1:]:
            self.assertEqual(gates[1]["expected"], gate["expected"])
            for label in ["person", "alias"]:
                values = gate["expected"]["entities"][label]
                self.assertEqual(3, len(values))
                self.assertTrue(all(value["confidence"] == .5 for value in values))
                for value in values:
                    self.assertEqual(value["text"], gate["text"][value["start"]:value["end"]])

    def test_profile_counters_count_positive_allocations_and_transfer_bytes(self):
        counters = trace_counters({"traceEvents": [
            {"name": "[memory]", "args": {"Bytes": 64, "Device Type": 0}},
            {"name": "[memory]", "args": {"Bytes": -64, "Device Type": 0}},
            {"name": "[memory]", "args": {"Bytes": 128, "Device Type": 1}},
            {"name": "Memcpy HtoD (Pageable -> Device)", "cat": "gpu_memcpy", "args": {"bytes": 256}},
            {"name": "Memcpy DtoH (Device -> Pageable)", "cat": "gpu_memcpy", "args": {"bytes": 32}},
            {"name": "aten::empty", "cat": "cpu_op", "args": {}},
        ]})
        self.assertEqual({"cpu_count": 1, "cpu_bytes": 64, "device_count": 1, "device_bytes": 128}, counters["allocations"])
        self.assertEqual({"count": 1, "bytes": 256}, counters["transfers"]["h2d"])
        self.assertEqual({"count": 1, "bytes": 32}, counters["transfers"]["d2h"])

    def test_sidecars_inventory_and_marker_pins_are_consistent(self):
        manifest = json.loads((oracle.FIXTURES / "manifest.json").read_text())
        for name, model in manifest["models"].items():
            root = oracle.FIXTURES / name
            for filename in ["config.json", "encoder_config/config.json", "tokenizer_config.json"]:
                path = root / filename
                self.assertEqual(model["files"][filename]["sha256"], oracle.digest_file(path))
                self.assertEqual(model["files"][filename]["size_bytes"], path.stat().st_size)
            inventory = root / "tensor_inventory.json"
            self.assertEqual(model["tensor_inventory_sha256"], oracle.digest_file(inventory))
            tensors = json.loads(inventory.read_text())
            self.assertEqual(model["tensor_count"], len(tensors))
            self.assertEqual({"F32"}, {tensor["dtype"] for tensor in tensors.values()})
            self.assertEqual(7, len(set(model["marker_ids"].values())))
            geometry = json.loads((root / "encoder_config/config.json").read_text())
            for token in model["marker_ids"].values():
                self.assertLess(token, geometry["vocab_size"])


if __name__ == "__main__":
    unittest.main()
