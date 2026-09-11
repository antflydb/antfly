"""Adversarial contracts for saved intent evidence; no model execution."""
import copy
from pathlib import Path
import tempfile
import unittest

import audit_massive_intent_execution as audit


class MassiveIntentAuditTest(unittest.TestCase):
    def setUp(self):
        self.case = {"id": "request", "request_sha256": "digest"}
        self.response = {"event": "result", "case_id": "request", "request_sha256": "digest", "input_ids": [1, 2],
            "output": {"entities": [], "classifications": [{"name": "intent", "multi_label": False,
                "labels": [{"label": "a", "confidence": .25}]}], "relations": [], "structures": [],
                "classification_solver": None, "joint_solver": None, "record_solver": None, "long_document": None}}

    def test_single_argmax_below_threshold_is_preserved(self):
        self.assertEqual({"label": "a", "confidence": .25}, audit.selected(self.response, "native", self.case, ["a", "b"]))
        source = {**self.response, "output": {"intent": {"label": "a", "confidence": .25}}}
        self.assertEqual(audit.selected(source, "python", self.case, ["a", "b"]),
                         audit.selected(self.response, "native", self.case, ["a", "b"]))

    def test_extra_labels_tasks_and_probability_maps_are_not_ignored(self):
        for field in ("label", "task", "probabilities", "entities"):
            value = copy.deepcopy(self.response)
            if field == "label":
                value["output"]["classifications"][0]["labels"].append({"label": "b", "confidence": .2})
            elif field == "task":
                value["output"]["classifications"].append(value["output"]["classifications"][0])
            elif field == "probabilities":
                value["output"]["classifications"][0]["labels"][0]["probabilities"] = {"a": .25, "b": .75}
            else:
                value["output"]["entities"] = [{"name": "unrequested", "values": []}]
            with self.subTest(field=field), self.assertRaises(ValueError):
                audit.selected(value, "native", self.case, ["a", "b"])

    def test_reordered_request_invalid_probability_and_large_tokens_fail(self):
        for field in ("case_id", "nan", "boolean", "tokens"):
            value = copy.deepcopy(self.response)
            if field == "case_id":
                value["case_id"] = "other"
            elif field == "tokens":
                value["input_ids"] = [1] * 513
            else:
                value["output"]["classifications"][0]["labels"][0]["confidence"] = float("nan") if field == "nan" else True
            with self.subTest(field=field), self.assertRaises(ValueError):
                audit.selected(value, "native", self.case, ["a", "b"])

    def test_fixed_ontology_retains_absent_label_false_positives(self):
        result = audit.metrics(["a", "b", "a"], ["a", "absent", "b"], ["a", "b", "absent"])
        self.assertEqual((1, 2, 2), tuple(result["intent_exact"][name] for name in ("tp", "fp", "fn")))
        self.assertEqual(1 / 3, result["intent_exact"]["document_exact_match"])
        self.assertEqual(0, result["intent_type/absent"]["support"])
        self.assertEqual(1, result["intent_type/absent"]["fp"])
        self.assertEqual(1 / 3, result["intent_type/absent"]["absent_query_false_positive_rate"])
        self.assertEqual(1, result["intent_type/b"]["exact_documents"])

    def test_confidence_parity_cannot_hide_wrong_tokens_or_wrong_label(self):
        label = {"label": "a", "confidence": .5}
        good = audit.compare(label, label, True)
        self.assertEqual(1, audit.summarize([good])["exact_selected_labels"])
        for row in (audit.compare(label, label, False), audit.compare(label, {"label": "b", "confidence": .5}, True),
                    audit.compare(label, {"label": "a", "confidence": .501}, True)):
            with self.assertRaises(ValueError):
                audit.summarize([row])

    def test_changed_report_bytes_and_incomplete_rows_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_bytes(b"one")
            expected = audit.digest(b"one")
            path.write_bytes(b"two")
            with self.assertRaises(ValueError):
                audit.read(path, expected)
        for raw in (b'{}', b'{}\n{}\n', b'{"a":1,"a":2}\n'):
            with self.assertRaises(ValueError):
                audit.rows(raw, 1)

    def test_foreign_model_build_and_resource_identity_are_rejected(self):
        owner = object.__new__(audit.Audit)
        owner.helpers = audit.execution.contract_files()
        owner.entry = audit.execution.profile_entry(audit.PROFILE)
        owner.artifact = {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None,
                          **audit.driver.evaluate.source_identity("small")}
        report = {"status": "complete", "qualification": False, "backend": "native", "model": "small",
            "artifact": owner.artifact, "contract_files": owner.helpers, "profile": audit.PROFILE,
            "registry_sha256": audit.oracle.sha256_file(audit.execution.REGISTRY),
            "lock_sha256": owner.entry["lock_sha256"], "prepared_sha256": owner.entry["prepared_sha256"],
            "policy": audit.execution.POLICY, "limits": audit.execution.LIMITS, "resource_policy": audit.RESOURCE_POLICY,
            "errors": 0, "binary_sha256": audit.WORKER["sha256"]}
        owner.identity(report, "native")
        for field, changed in (("model", "base"), ("binary_sha256", "0" * 64), ("artifact", {}),
                               ("resource_policy", {**audit.RESOURCE_POLICY, "max_worker_rss_bytes": 12 * 1024**3}),
                               ("prepared_sha256", "0" * 64), ("qualification", True)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                owner.identity({**report, field: changed}, "native")


if __name__ == "__main__":
    unittest.main()
