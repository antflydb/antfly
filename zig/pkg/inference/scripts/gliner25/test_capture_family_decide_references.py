from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import json

import capture_family_decide_references as capture


class CaptureFamilyDecideReferencesTest(unittest.TestCase):
    def test_exact_public_corpus_covers_combined_and_typed_questions(self) -> None:
        document = capture.validate_requests(capture.DEFAULT_REQUESTS)
        rows = {row["id"]: row for row in document["requests"]}
        first = rows["described_prompt_choice"]
        task = first["native_schema"]["classifications"][0]
        self.assertIn("prompt", task)
        self.assertEqual(set(task["labels"]), set(task["label_definitions"]))
        second = rows["choice_score_noul"]
        self.assertEqual(
            ["choice", "score", "noul"],
            [question["type"] for question in second["decide_request"]["questions"].values()],
        )

    def test_fixture_rejects_adapter_order_or_description_drift(self) -> None:
        document = json.loads(capture.DEFAULT_REQUESTS.read_text())
        document["requests"][1]["native_schema"]["classifications"][0]["labels"].reverse()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "requests.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(capture.common.CaptureError, "translation"):
                capture.validate_requests(path)

    def test_decide_presenter_uses_full_distribution(self) -> None:
        request = {
            "questions": {
                "choice": {"type": "choice", "instructions": "x", "criteria": {"a": "A", "b": "B"}},
                "score": {"type": "score", "instructions": "x", "criteria": ["low", "high"]},
                "noul": {"type": "noul", "instructions": "x"},
            }
        }
        answer = capture.decide_answers(request, {
            "choice": {"a": 0.25, "b": 0.75},
            "score": {"0": 0.4, "1": 0.6},
            "noul": {"false": 0.2, "true": 0.8},
        })
        self.assertEqual("b", answer["choice"]["choice"])
        self.assertAlmostEqual(0.6, answer["score"]["score"])
        self.assertAlmostEqual(0.8, answer["noul"]["noul"])


if __name__ == "__main__":
    unittest.main()
