import copy
import json
from pathlib import Path
import tempfile
import unicodedata
import unittest

import evaluation_contract as evaluation
import oracle


def pin(root, name, value):
    path = root / name
    path.write_bytes(value)
    return {"path": name, "size_bytes": len(value), "sha256": evaluation.digest(value)}


def make_lock(root):
    metrics = {"entities": {"counting": "set", "definition": "type and exact immutable UTF-8 span"},
               "records": {"counting": "multiset", "definition": "whole semantic record preserving multiplicity"}}
    entity = {"type": "symbol", "span": {"start": 3, "end": 7, "text": "😀"}}
    record = {"structure": "items", "fields": {"name": ["same"]}}
    common = {"language": "en", "schema_id": "all_types"}
    training = {**common, "id": "train-1", "family_id": "train-family", "text": "different text", "gold": {"entities": [], "records": []}}
    test = {**common, "id": "test-1", "family_id": "test-family", "text": "Α 😀", "gold": {"entities": [entity], "records": [record, record]}}
    schema = pin(root, "schema.json", evaluation.encoded({"entities": ["symbol", "absent_type"]}))
    adapter = pin(root, "adapter.py", b"# Synthetic test adapter; never executed.\n")
    metric_file = pin(root, "metrics.json", evaluation.encoded(metrics))
    source = pin(root, "source.txt", b"independently retained source corpus evidence\n")
    train_file = pin(root, "train.jsonl", evaluation.encoded(training) + b"\n")
    test_file = pin(root, "test.jsonl", evaluation.encoded(test) + b"\n")
    value = {"scope": evaluation.SCOPE, "status": "locked", "qualification": False,
             "upstream_commit": oracle.UPSTREAM_COMMIT, "unicode_version": unicodedata.unidata_version,
             "harness_sha256": oracle.sha256_file(Path(evaluation.__file__)),
             "schema_selection": "fixed_before_test", "test_used_for_tuning": False,
             "adapter_sha256": adapter["sha256"], "adapter_file": adapter,
             "metric_contract_sha256": metric_file["sha256"], "metric_contract_file": metric_file,
             "schemas": [{"id": "all_types", "origin": "public_ontology", "file": schema}],
             "request_options": {"threshold": 0.5, "best_effort": False}, "offset_unit": "utf8_bytes",
             "source_files": [source], "metrics": metrics,
             "splits": [{"split": "train", "records": 1, "file": train_file}, {"split": "test", "records": 1, "file": test_file}]}
    path = root / "lock.json"
    path.write_bytes(evaluation.encoded(value))
    return path, value, training, test


class EvaluationContractTests(unittest.TestCase):
    def test_catalog_is_explicitly_not_an_execution_lock(self):
        path = Path(evaluation.__file__).with_name("evaluation_catalog.json")
        catalog = oracle.read_json(path)
        self.assertIs(catalog["qualification"], False)
        self.assertEqual(catalog["status"], "metadata_only")
        self.assertEqual(len(catalog["datasets"]), 10)
        self.assertTrue(all(entry["data_files"] == [] and entry["metadata_only"] for entry in catalog["datasets"]))
        with self.assertRaises(evaluation.EvaluationError):
            evaluation.audit(path)

    def test_blinded_input_keeps_absent_types_and_never_contains_gold(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, lock, _, test = make_lock(root)
            first = evaluation.prepare(path, root / "first")
            first_request = next(evaluation.rows(root / "first" / "requests.jsonl"))
            self.assertEqual(first_request["request"]["schema"]["entities"], ["symbol", "absent_type"])
            self.assertEqual(set(first_request), {"request_id", "request_sha256", "request"})
            self.assertEqual(set(first_request["request"]), {"text", "schema", "options", "offset_unit"})
            self.assertIs(first["qualification"], False)
            test["gold"] = {"entities": [], "records": []}
            lock["splits"][1]["file"] = pin(root, "test.jsonl", evaluation.encoded(test) + b"\n")
            path.write_bytes(evaluation.encoded(lock))
            evaluation.prepare(path, root / "second")
            second = next(evaluation.rows(root / "second" / "requests.jsonl"))
            self.assertEqual(first_request["request"], second["request"])
            self.assertEqual(first_request["request_sha256"], second["request_sha256"])
            # Run IDs bind the changed gold lock but are opaque to inference.
            self.assertNotEqual(first_request["request_id"], second["request_id"])

    def test_family_duplicates_and_schema_from_gold_fail_closed(self):
        for corrupt in ("family", "text", "schema", "per_document_schema", "hash", "adapter"):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path, lock, training, test = make_lock(root)
                if corrupt == "family":
                    test["family_id"] = training["family_id"]
                elif corrupt == "text":
                    test["text"] = " DIFFERENT  TEXT "
                    test["gold"] = {"entities": [], "records": []}
                elif corrupt == "schema":
                    lock["schemas"][0]["origin"] = "test_gold"
                elif corrupt == "per_document_schema":
                    test["schema"] = {"entities": ["symbol"]}
                elif corrupt == "hash":
                    lock["source_files"][0]["sha256"] = "0" * 64
                else:
                    lock["adapter_sha256"] = "0" * 64
                lock["splits"][1]["file"] = pin(root, "test.jsonl", evaluation.encoded(test) + b"\n")
                path.write_bytes(evaluation.encoded(lock))
                with self.assertRaises(evaluation.EvaluationError):
                    evaluation.audit(path)

    def test_duplicate_records_are_counted_and_missing_outputs_cannot_disappear(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, _, _, _ = make_lock(root)
            prepared = root / "prepared"
            evaluation.prepare(path, prepared)
            gold = next(evaluation.rows(prepared / "gold.jsonl"))
            prediction = {key: copy.deepcopy(gold[key]) for key in ("request_id", "request_sha256", "metrics")}
            prediction["metrics"]["records"].pop()
            predictions = root / "predictions.jsonl"
            predictions.write_bytes(evaluation.encoded(prediction) + b"\n")
            result = evaluation.score(prepared, predictions)
            self.assertEqual(result["metrics"]["records"]["tp"], 1)
            self.assertEqual(result["metrics"]["records"]["fn"], 1)
            self.assertEqual(result["metrics"]["records"]["micro_f1"], 2 / 3)
            self.assertEqual(result["metrics"]["entities"]["micro_f1"], 1)
            self.assertIs(result["qualification"], False)
            predictions.write_bytes(b"")
            with self.assertRaisesRegex(evaluation.EvaluationError, "coverage"):
                evaluation.score(prepared, predictions)

    def test_span_interiors_drift_and_undeclared_metrics_are_rejected(self):
        for corrupt in ("span", "surface", "metric", "floating", "duplicate"):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path, lock, _, test = make_lock(root)
                entity = test["gold"]["entities"][0]
                if corrupt == "span":
                    entity["span"]["start"] = 4
                elif corrupt == "surface":
                    entity["span"]["text"] = "x"
                elif corrupt == "metric":
                    test["gold"]["unscored"] = []
                elif corrupt == "floating":
                    entity["confidence"] = 0.9
                else:
                    test["gold"]["entities"].append(copy.deepcopy(entity))
                lock["splits"][1]["file"] = pin(root, "test.jsonl", evaluation.encoded(test) + b"\n")
                path.write_bytes(evaluation.encoded(lock))
                with self.assertRaises(evaluation.EvaluationError):
                    evaluation.audit(path)

    def test_empty_support_does_not_receive_perfect_f1(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, lock, _, test = make_lock(root)
            test["gold"] = {"entities": [], "records": []}
            lock["splits"][1]["file"] = pin(root, "test.jsonl", evaluation.encoded(test) + b"\n")
            path.write_bytes(evaluation.encoded(lock))
            prepared = root / "prepared"
            evaluation.prepare(path, prepared)
            gold = next(evaluation.rows(prepared / "gold.jsonl"))
            predictions = root / "predictions.jsonl"
            predictions.write_bytes(evaluation.encoded({key: gold[key] for key in ("request_id", "request_sha256", "metrics")}) + b"\n")
            result = evaluation.score(prepared, predictions)
            self.assertEqual(result["metrics"]["entities"]["support"], 0)
            self.assertEqual(result["metrics"]["entities"]["micro_f1"], 0)
            self.assertEqual(result["metrics"]["entities"]["document_exact_match"], 1)


if __name__ == "__main__":
    unittest.main()
