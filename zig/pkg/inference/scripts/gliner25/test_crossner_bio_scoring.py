"""Independent official BIO metric checks; no model or network dependencies."""
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import audit_crossner_bio_scoring as audit


def fact(text, start, end, kind="person"):
    return {"type": kind, "span": {"start": start, "end": end, "text": text.encode()[start:end].decode()}}


class OfficialBIOTests(unittest.TestCase):
    def test_reference_identity_and_license(self):
        receipt = json.loads((audit.REFERENCE / "source.json").read_bytes())
        self.assertEqual(receipt["revision"], "2e7ba2a7798c961e3f29fbc51252c5a8d40224bf")
        self.assertEqual(receipt["license"], "MIT")
        self.assertEqual(receipt["modifications"], [])
        for entry in receipt["files"]:
            self.assertEqual(audit.pin((audit.REFERENCE / entry["path"]).read_bytes()),
                             {key: entry[key] for key in ("size_bytes", "sha256")})
        self.assertIn("Copyright (c) 2020 Zihan Liu", (audit.REFERENCE / "LICENSE").read_text())
        self.assertIsNotNone(audit.load_scorer())
        with mock.patch.object(audit, "SCORER_SHA256", "0" * 64):
            with self.assertRaisesRegex(audit.evaluation.EvaluationError, "scorer pin"):
                audit.load_scorer()

    def test_exhaustive_flat_span_counts_with_adjacent_and_partial_word_entities(self):
        text = "abc"
        ontology = ["person", "other"]

        def inventories(start):
            if start == len(text):
                yield []
                return
            yield from inventories(start + 1)
            for end in range(start + 1, len(text) + 1):
                for kind in ontology:
                    for tail in inventories(end):
                        yield [fact(text, start, end, kind), *tail]

        choices = list(inventories(0))
        scorer = audit.load_scorer()
        for gold, predicted in itertools.product(choices, repeat=2):
            with self.subTest(gold=gold, predicted=predicted):
                actual = audit.score_documents([(text, gold, predicted)], ontology, scorer)
                for name in [None, *ontology]:
                    def keys(rows):
                        return {(row["span"]["start"], row["span"]["end"], row["type"])
                                for row in rows if name is None or row["type"] == name}
                    wanted, found = keys(gold), keys(predicted)
                    counts = actual["entity_exact" if name is None else "entity_type/" + name]
                    self.assertEqual((counts["tp"], counts["fp"], counts["fn"]),
                                     (len(wanted & found), len(found - wanted), len(wanted - found)))

    def test_utf8_empty_documents_duplicate_sets_and_explicit_boundaries(self):
        text = "é🙂x"
        gold = [fact(text, 0, 2), fact(text, 2, 7)]
        predicted = [fact(text, 0, 2), fact(text, 2, 6)]
        documents = [("", [], []), (text, gold, predicted + predicted), ("x", [fact("x", 0, 1)], [])]
        counts = audit.score_documents(documents, ["person", "absent"])
        self.assertEqual({key: counts["entity_exact"][key] for key in ("tp", "fp", "fn")},
                         {"tp": 1, "fp": 1, "fn": 2})
        self.assertEqual(counts["entity_type/absent"]["micro_f1"], 0)
        # Explicit B tags must preserve adjacent entities of the same type.
        adjacent = [fact("ab", 0, 1), fact("ab", 1, 2)]
        self.assertEqual(audit.score_documents([("ab", adjacent, adjacent)], ["person"])["entity_exact"]["tp"], 2)

    def test_invalid_spans_and_overlaps_fail_without_projection(self):
        bad = [
            {"type": "person", "span": {"start": 0, "end": 1, "text": "é"}},
            {"type": "person", "span": {"start": False, "end": 2, "text": "é"}},
            {"type": "person", "span": {"start": 0, "end": 2, "text": "wrong"}},
            {"type": "person", "span": {"start": 2, "end": 2, "text": ""}},
            {"type": "unknown", "span": {"start": 0, "end": 2, "text": "é"}},
        ]
        for value in bad:
            with self.assertRaises(audit.evaluation.EvaluationError):
                audit.score_documents([("éx", [], [value])], ["person"])
        with self.assertRaisesRegex(audit.evaluation.EvaluationError, "overlapping"):
            audit.score_documents([("abc", [], [fact("abc", 0, 2), fact("abc", 1, 3)])], ["person"])
        with self.assertRaises(audit.evaluation.EvaluationError):
            audit.score_documents([("", [], [])], ["bad type"])

    def test_cross_type_overlap_preserves_every_prediction(self):
        text = "Ada"
        gold = [fact(text, 0, 3)]
        predicted = [fact(text, 0, 3), fact(text, 0, 3, "other")]
        actual = audit.score_documents([(text, gold, predicted)], ["person", "other"])
        self.assertEqual((actual["entity_exact"]["tp"], actual["entity_exact"]["fp"], actual["entity_exact"]["fn"]), (1, 1, 0))
        self.assertEqual(actual["entity_type/other"]["fp"], 1)

    def test_input_admission_and_duplicate_json_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            path.write_bytes(b"{}")
            with self.assertRaises(audit.evaluation.EvaluationError):
                audit.read(path, maximum=1)
            link = Path(directory) / "link.json"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                audit.read(link)
            fifo = Path(directory) / "fifo"
            os.mkfifo(fifo)
            # A regression to blocking open must fail within a deadline instead
            # of hanging the parent contract suite. subprocess.run reaps on timeout.
            worker = """import pathlib,sys
import audit_crossner_bio_scoring as audit
try:
    audit.read(pathlib.Path(sys.argv[1]))
except audit.evaluation.EvaluationError:
    sys.exit(0)
sys.exit(1)
"""
            child = subprocess.run([sys.executable, "-c", worker, str(fifo)], cwd=audit.HERE,
                                   env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                                   capture_output=True, timeout=2, check=False)
            self.assertEqual(child.returncode, 0, child.stderr.decode())
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}'):
            with self.assertRaises(audit.evaluation.EvaluationError):
                audit.decode(raw)

    def test_foreign_report_and_embedded_metric_substitution_rejected(self):
        receipt = {"lock_sha256": "a" * 64, "records": 431}
        metric = {"entity_exact": {"tp": 1, "fp": 2, "fn": 3}}
        report = {"scope": "gliner25_heldout_execution/v1", "qualification": False, "status": "complete",
                  "lock_sha256": receipt["lock_sha256"], "denominator": 431, "completed_results": 431,
                  "errors": 0, "unprocessed": 0, "metrics": metric}
        stored = {"scope": "gliner25_heldout_exact_fact_metrics/v1", "qualification": False,
                  "lock_sha256": receipt["lock_sha256"], "records": 431, "metrics": metric}
        audit.check_report_identity(report, receipt, stored)
        for key, value in {"scope": "unrelated-report/v100", "qualification": True, "lock_sha256": "0" * 64,
                           "errors": 1, "completed_results": 430, "metrics": {"entity_exact": {"tp": 100}}}.items():
            with self.subTest(key=key), self.assertRaises(audit.evaluation.EvaluationError):
                audit.check_report_identity({**report, key: value}, receipt, stored)
        with self.assertRaises(audit.evaluation.EvaluationError):
            audit.check_report_identity(report, receipt, {**stored, "lock_sha256": "0" * 64})


if __name__ == "__main__":
    unittest.main()
