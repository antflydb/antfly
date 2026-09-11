from __future__ import annotations

import copy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import evaluation_contract as evaluation
import oracle
import prepare_crossner_ai as crossner


class CrossNERTests(unittest.TestCase):
    def test_bio_preserves_utf8_occurrences_and_adjacent_entities(self):
        parsed = crossner.parse_bio("İ\tB-person\n李\tI-person\n李\tB-person\n.\tO", ["person"])
        self.assertEqual(parsed, [("İ 李 李 .", [
            {"type": "person", "span": {"start": 0, "end": 6, "text": "İ 李"}},
            {"type": "person", "span": {"start": 7, "end": 10, "text": "李"}},
        ], 4)])

    def test_invalid_bio_never_silently_drops_labels(self):
        for raw in ("x\tI-person", "x\tB-person\ny\tI-other", "x\tB-missing", "x O", "\tO"):
            with self.subTest(raw=raw), self.assertRaises(evaluation.EvaluationError):
                crossner.parse_bio(raw, ["person", "other"])

    def test_split_policy_uses_text_not_gold_and_keeps_test_multiplicity(self):
        row = ("İ  Test", [], 2)
        normalized_equivalent = ("i̇ test", [{"type": "person"}], 2)
        parsed = {"train": [row, ("only train", [], 2)], "dev": [normalized_equivalent, ("only dev", [], 2)],
                  "test": [row, row]}
        retained, audit = crossner.separate_splits(parsed)
        self.assertEqual(retained["train"], [(1, parsed["train"][1])])
        self.assertEqual(retained["dev"], [(1, parsed["dev"][1])])
        self.assertEqual(retained["test"], [(0, row), (1, row)])
        self.assertEqual(audit["cross_split_groups"], 1)
        self.assertEqual(audit["duplicate_groups"][0]["gold_variants"], 2)
        changed_gold = copy.deepcopy(parsed)
        changed_gold["test"][0] = (row[0], [{"unused": "different gold"}], row[2])
        self.assertEqual(crossner.separate_splits(changed_gold)[1]["excluded_ids"], audit["excluded_ids"])

    def test_prediction_conversion_keeps_absent_queries_and_exact_coordinates(self):
        names = oracle.read_json(crossner.MANIFEST)["entity_types"]
        request = {"schema": {"entities": names}, "text": "İ 李"}
        python = {"entities": {name: [] for name in names}}
        python["entities"]["person"] = [{"text": "李", "start": 2, "end": 3, "confidence": .9}]
        native = {"entities": [{"name": name, "values": []} for name in names]}
        native["entities"][names.index("person")]["values"] = [
            {"text": "李", "source": {"unit": "utf8_bytes", "start": 3, "end": 6}, "confidence": .9}]
        expected = crossner.prediction_facts(request, python, "python")
        self.assertEqual(crossner.prediction_facts(request, native, "native"), expected)
        self.assertEqual(len(expected), len(names) + 1)
        self.assertEqual(expected["entity_type/algorithm"], [])
        python["entities"].pop("algorithm")
        with self.assertRaisesRegex(evaluation.EvaluationError, "coverage"):
            crossner.prediction_facts(request, python, "python")
        native["entities"][names.index("person")]["values"][0]["source"]["start"] = 4
        with self.assertRaisesRegex(evaluation.EvaluationError, "span"):
            crossner.prediction_facts(request, native, "metal")

    def test_preparation_pins_source_audit_and_blinds_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = root / "corpus"
            files = {"ner_data/ai/train.txt": b"Train\tB-person\n", "ner_data/ai/dev.txt": b"Dev\tO\n",
                     "ner_data/ai/test.txt": "李\tB-person\n".encode(),
                     "src/dataloader.py": b"ai_labels = ['O', 'B-person', 'I-person']\n",
                     "LICENSE": b"Synthetic test only\n", "README.md": b"Synthetic test corpus\n"}
            manifest = oracle.read_json(crossner.MANIFEST)
            manifest.update(entity_types=["person"], source_documents={"train": 1, "dev": 1, "test": 1}, excluded_ids=[])
            manifest["files"] = []
            for name, data in files.items():
                path = corpus / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                manifest["files"].append({"path": name, "size_bytes": len(data), "sha256": evaluation.digest(data),
                                          "git_blob_sha1": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()})
            manifest_path = root / "manifest.json"
            oracle.write_json(manifest_path, manifest)
            with mock.patch.object(crossner, "MANIFEST", manifest_path):
                summary = crossner.prepare(corpus, root / "ready")
                self.assertEqual(summary["prepared"]["records"], 1)
                request = next(evaluation.rows(root / "ready/prepared/requests.jsonl"))
                self.assertNotIn("gold", request["request"])
                self.assertEqual(request["request"]["schema"], {"entities": ["person"]})
                self.assertFalse(summary["qualification"])
                with (root / "ready/split_audit.json").open("ab") as target:
                    target.write(b" ")
                with self.assertRaisesRegex(evaluation.EvaluationError, "content"):
                    evaluation.audit(root / "ready/lock.json")
                (corpus / "LICENSE").write_bytes(b"Synthetic test evil\n")
                with self.assertRaises(evaluation.EvaluationError):
                    crossner.prepare(corpus, root / "not-ready")
                self.assertFalse((root / "not-ready").exists())


if __name__ == "__main__":
    unittest.main()
