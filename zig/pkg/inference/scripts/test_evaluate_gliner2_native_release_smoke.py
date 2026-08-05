from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path

from evaluate_gliner2_native_release_smoke import (
    FULL_TASK_MINIMUM_KEYS,
    build_command,
    parse_full_task_minima,
    release_entity_schema,
    release_threshold,
)


class NativeReleaseQualityTest(unittest.TestCase):
    minima = {key: 0.6 for key in FULL_TASK_MINIMUM_KEYS}

    def test_release_threshold_rejects_zero(self) -> None:
        with self.assertRaisesRegex(argparse.ArgumentTypeError, "greater than 0"):
            release_threshold("0")

    def test_release_schema_is_limited_to_eval_entity_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter_dir = root / "adapter"
            adapter_dir.mkdir()
            (adapter_dir / "training_manifest.json").write_text(
                json.dumps(
                    {
                        "entity_labels": [
                            "person",
                            "organization",
                            "@gliner2:c:9:sentiment:8:positive",
                        ]
                    }
                ),
                encoding="utf-8",
            )
            eval_data = root / "eval.jsonl"
            eval_data.write_text(
                json.dumps(
                    {
                        "input": "Alice joined Acme.",
                        "output": {
                            "entities": {"person": ["Alice"]},
                            "classifications": [
                                {
                                    "task": "sentiment",
                                    "labels": ["positive", "negative"],
                                    "true_label": ["positive"],
                                }
                            ],
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            labels, _ = release_entity_schema(adapter_dir, eval_data)
            self.assertEqual(["person"], labels)

    def test_native_command_scores_the_full_dataset_with_both_release_floors(self) -> None:
        command = build_command(
            Path("/model"),
            Path("/adapter"),
            Path("/eval.jsonl"),
            {"seq_len": 128, "max_span_width": 8},
            0.5,
            0.8,
            0.7,
            self.minima,
            Path("/report.json"),
        )
        self.assertIn("eval-gliner2-autodiff-adapter-dataset", command)
        self.assertEqual("0.8", command[command.index("--min-f1") + 1])
        self.assertEqual("0.7", command[command.index("--min-exact-match") + 1])
        self.assertEqual("/eval.jsonl", command[command.index("/adapter") + 1])
        self.assertEqual("-", command[command.index("/eval.jsonl") + 1])
        self.assertEqual(7, command.count("--min-task-metric"))

    def test_full_task_minima_are_required_and_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing required"):
            parse_full_task_minima([])
        values = [f"{key}=0.5" for key in FULL_TASK_MINIMUM_KEYS]
        self.assertEqual(set(FULL_TASK_MINIMUM_KEYS), parse_full_task_minima(values).keys())
        values[-1] = "count.accuracy=0"
        with self.assertRaisesRegex(ValueError, "greater than 0"):
            parse_full_task_minima(values)

    def test_structured_schema_allows_comma_labels_and_more_than_256_globally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter_dir = root / "adapter"
            adapter_dir.mkdir()
            labels = ["person, primary", *[f"entity-{index}" for index in range(256)]]
            (adapter_dir / "training_manifest.json").write_text(
                json.dumps({"entity_labels": labels}),
                encoding="utf-8",
            )
            eval_data = root / "eval.jsonl"
            eval_data.write_text(
                "".join(
                    json.dumps(
                        {
                            "input": f"value-{index}",
                            "output": {"entities": {label: [f"value-{index}"]}},
                        }
                    )
                    + "\n"
                    for index, label in enumerate(labels)
                ),
                encoding="utf-8",
            )

            actual, _ = release_entity_schema(adapter_dir, eval_data)

            self.assertEqual(labels, actual)


if __name__ == "__main__":
    unittest.main()
