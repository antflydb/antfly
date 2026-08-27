from __future__ import annotations

import unittest

from evaluate_gliner2_full_task import (
    REQUIRED_MINIMA,
    Scores,
    normalized_text,
    parse_minima,
    schema_and_gold,
    score_prediction,
)


class FullTaskEvaluationTest(unittest.TestCase):
    def test_scoring_uses_canonical_release_normalization(self) -> None:
        self.assertEqual("café worker", normalized_text(" CAFE\u0301\tWORKER "))

    def test_perfect_raw_upstream_prediction_scores_every_task_and_count(self) -> None:
        record = {
            "input": "Alice founded Acme in Paris and marked it urgent.",
            "output": {
                "entities": {"person": ["Alice"], "organization": ["Acme"]},
                "classifications": [
                    {"task": "priority", "labels": ["normal", "urgent"], "true_label": ["urgent"]}
                ],
                "json_structures": [
                    {"company": {"name": "Acme", "place": "Paris", "status": {"value": "active", "choices": ["active", "closed"]}}}
                ],
                "relations": [{"founded": {"head": "Alice", "tail": "Acme"}}],
            },
        }
        _, schema, gold = schema_and_gold(record)
        self.assertEqual(schema["entities"], {"person": "", "organization": ""})
        self.assertNotIn("Alice", str(schema))
        scores = Scores()
        score_prediction(
            scores,
            gold,
            {
                "entities": [{"person": ["Alice"], "organization": ["Acme"]}],
                "priority": ("urgent", 0.9),
                "company": [{"name": ["Acme"], "place": ["Paris"], "status": ["active"]}],
                "founded": [("Alice", "Acme")],
            },
        )
        metrics = scores.result()
        for task in ("entities", "classifications", "json_structures", "relations"):
            self.assertEqual(metrics[task]["micro_f1"], 1.0)
            self.assertEqual(metrics[task]["exact_match"], 1.0)
        self.assertEqual(metrics["count"]["accuracy"], 1.0)
        self.assertEqual(metrics["json_structures"]["count"]["accuracy"], 1.0)
        self.assertEqual(metrics["relations"]["count"]["accuracy"], 1.0)

    def test_unrequested_entity_task_does_not_inflate_exact_match(self) -> None:
        _, _, gold = schema_and_gold(
            {
                "input": "Urgent.",
                "output": {
                    "classifications": [
                        {"task": "priority", "labels": ["normal", "urgent"], "true_label": ["urgent"]}
                    ]
                },
            }
        )
        scores = Scores()
        score_prediction(scores, gold, {"priority": ("urgent", 0.9)})
        self.assertEqual(scores.result()["entities"]["cases"], 0)

    def test_preserves_all_upstream_schema_conditioning_metadata(self) -> None:
        _, schema, gold = schema_and_gold(
            {
                "input": "Alice filed an urgent Acme report.",
                "output": {
                    "entities": {"person": ["Alice"]},
                    "entity_descriptions": {"person": "A human name"},
                    "classifications": [
                        {
                            "task": "priority",
                            "labels": ["normal", "urgent"],
                            "true_label": ["urgent"],
                            "prompt": "Choose the report priority",
                            "examples": [["Act now", "urgent"]],
                            "label_descriptions": {
                                "normal": "Routine work",
                                "urgent": "Needs immediate attention",
                            },
                        }
                    ],
                    "json_structures": [{"report": {"company": "Acme"}}],
                    "json_descriptions": {"report": {"company": "Named company"}},
                },
            }
        )
        self.assertEqual(schema["entity_descriptions"], {"person": "A human name"})
        self.assertEqual(schema["json_descriptions"], {"report": {"company": "Named company"}})
        self.assertEqual(
            schema["classifications"][0],
            {
                "task": "priority",
                "labels": ["normal", "urgent"],
                "multi_label": False,
                "prompt": "Choose the report priority",
                "examples": [["Act now", "urgent"]],
                "label_descriptions": {
                    "normal": "Routine work",
                    "urgent": "Needs immediate attention",
                },
            },
        )
        scores = Scores()
        score_prediction(
            scores,
            gold,
            {"priority: Choose the report priority": ("urgent", 0.99)},
        )
        self.assertEqual(scores.result()["classifications"]["micro_f1"], 1.0)

    def test_rejects_metadata_that_upstream_would_silently_ignore(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown key"):
            schema_and_gold(
                {
                    "input": "Alice",
                    "output": {
                        "entities": {"person": ["Alice"]},
                        "entity_descriptions": {"organization": "A company"},
                    },
                }
            )

    def test_examples_without_descriptions_are_part_of_upstream_raw_key(self) -> None:
        _, _, gold = schema_and_gold(
            {
                "input": "Urgent.",
                "output": {
                    "classifications": [
                        {
                            "task": "priority",
                            "labels": ["normal", "urgent"],
                            "true_label": ["urgent"],
                            "examples": [["Act now", "urgent"]],
                        }
                    ]
                },
            }
        )
        scores = Scores()
        score_prediction(
            scores,
            gold,
            {"priority [EXAMPLE] Act now [OUTPUT] urgent": ("urgent", 0.99)},
        )
        self.assertEqual(scores.result()["classifications"]["micro_f1"], 1.0)
        with self.assertRaisesRegex(ValueError, "outputs must belong"):
            schema_and_gold(
                {
                    "input": "Urgent",
                    "output": {
                        "classifications": [
                            {
                                "task": "priority",
                                "labels": ["normal", "urgent"],
                                "true_label": ["urgent"],
                                "examples": [["Later", "deferred"]],
                            }
                        ]
                    },
                }
            )

    def test_ignores_empty_optional_task_containers(self) -> None:
        _, schema, _ = schema_and_gold(
            {
                "input": "Urgent.",
                "output": {
                    "entities": {},
                    "classifications": [
                        {"task": "priority", "labels": ["normal", "urgent"], "true_label": ["urgent"]}
                    ],
                    "json_structures": [],
                    "relations": [],
                },
            }
        )
        self.assertEqual(set(schema), {"classifications"})

    def test_negative_classification_scores_false_positives(self) -> None:
        _, _, gold = schema_and_gold(
            {
                "input": "This record has no assigned priority.",
                "output": {
                    "classifications": [
                        {"task": "priority", "labels": ["normal", "urgent"], "true_label": []}
                    ]
                },
            }
        )
        scores = Scores()
        score_prediction(scores, gold, {"priority": ("urgent", 0.9)})
        metrics = scores.result()["classifications"]
        self.assertEqual(metrics["gold_atoms"], 0)
        self.assertEqual(metrics["false_positive"], 1)
        self.assertEqual(metrics["exact_match"], 0.0)

    def test_rejects_relation_fields_the_upstream_public_decoder_cannot_return(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-head/tail"):
            schema_and_gold(
                {
                    "input": "Alice founded Acme in Paris.",
                    "output": {"relations": [{"founded": {"head": "Alice", "tail": "Acme", "place": "Paris"}}]},
                }
            )

    def test_rejects_incomplete_empty_or_multivalued_relations(self) -> None:
        invalid_fields = (
            {"head": "Alice"},
            {"head": "", "tail": "Acme"},
            {"head": ["Alice", "Bob"], "tail": "Acme"},
        )
        for fields in invalid_fields:
            with self.subTest(fields=fields), self.assertRaisesRegex(
                ValueError, "must contain exactly|exactly one non-empty"
            ):
                schema_and_gold(
                    {
                        "input": "Alice founded Acme.",
                        "output": {"relations": [{"founded": fields}]},
                    }
                )

    def test_minima_are_complete_unique_and_bounded(self) -> None:
        items = [f"{key}=0.5" for key in sorted(REQUIRED_MINIMA)]
        self.assertEqual(set(parse_minima(items)), {item.split("=", 1)[0] for item in items})
        with self.assertRaisesRegex(ValueError, "missing"):
            parse_minima(items[:-1])
        with self.assertRaisesRegex(ValueError, "invalid or duplicate"):
            parse_minima(items + [items[0]])


if __name__ == "__main__":
    unittest.main()
