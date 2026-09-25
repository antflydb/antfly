#!/usr/bin/env python3
"""Build Antenna distillation rows: GLiNER2.5 boundary training JSONL with
teacher soft targets.

Classification rows carry `probabilities` per declared label:
``w * gold + (1 - w) * sigmoid(teacher logit)``, with GLiNER2.5-Decide as the
teacher; the student's classification loss is per-label BCE, so the sigmoid
keeps teacher and student on one scale. Large label sets are sampled per row
(the gold label plus shuffled distractors) so the student sees varied
schemas. Entity rows carry complete gold spans, or with ``--teacher-entities``
the extraction teacher's spans above a threshold (pure distillation).

Datasets and label names come from ``antenna_datasets.py`` so training rows use
exactly the evaluation harness's text, labels and splits. Runs on the pinned
GLiNER2.5 oracle (``scripts/gliner25/oracle.py``) with local teacher
checkpoints.

    PYTHONDONTWRITEBYTECODE=1 <oracle venv>/bin/python teacher_targets.py \\
        --upstream <GLiNER2 checkout> --classifier <GLiNER2.5-Decide dir> \\
        --output <dir outside Git>
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "gliner25"))
sys.path.insert(0, str(HERE))

import oracle  # noqa: E402

# Train splits only; the evaluation harness holds out clinc150, sst5,
# typed_decisions, crossner_politics, crossner_science and mit_movie.
CLASSIFICATION = {"banking77": "intent", "ag_news": "topic"}
NER = ["crossner_ai", "crossner_literature", "crossner_music", "mit_restaurant"]


def sigmoid(value: float) -> float:
    return 1 / (1 + math.exp(-value)) if value >= 0 else math.exp(value) / (1 + math.exp(value))


def byte_offsets(text: str, start: int, end: int) -> tuple[int, int]:
    return len(text[:start].encode()), len(text[:end].encode())


def fits(args, splitter, text: str) -> bool:
    """Upstream truncates past max_len words; the native job rejects such
    rows instead. Keep only texts both see whole."""
    return len(list(splitter(text, lower=False))) <= args.max_words


def classification_rows(args, datasets, classifier, split: str, rng: random.Random) -> list[dict[str, Any]]:
    from gliner2.classification import ClassificationConfig, ClassificationSchema

    rows = []
    for name, task in CLASSIFICATION.items():
        records = [record for record in datasets.load_classification(name, split) if fits(args, classifier.model.processor.word_splitter, record["text"])]
        rng.shuffle(records)
        names = list(datasets.label_names(name))
        for record in records[: args.per_dataset]:
            gold = names[record["label"]] if isinstance(record["label"], int) else record["label"]
            if len(names) > args.max_labels:
                count = rng.randint(args.min_labels, args.max_labels)
                labels = [gold] + rng.sample([label for label in names if label != gold], count - 1)
                rng.shuffle(labels)
            else:
                labels = list(names)
            schema = ClassificationSchema.from_dict({"version": 3, "tasks": {task: {"labels": labels, "min_labels": 1, "max_labels": 1}}})
            scores = classifier.score(record["text"], schema, config=ClassificationConfig(max_len=args.max_words))
            probabilities = [round(args.gold_weight * (label == gold) + (1 - args.gold_weight) * sigmoid(scores.tasks[task][label]), 6)
                             for label in labels]
            rows.append({"version": 1, "id": record["id"], "text": record["text"],
                         "schema": {"classifications": [{"name": task, "labels": labels}]},
                         "classifications": [{"task": task, "labels": [gold], "probabilities": probabilities}]})
    return rows


def entity_rows(args, datasets, extractor, splitter, split: str, rng: random.Random) -> list[dict[str, Any]]:
    rows = []
    for name in NER:
        records = [record for record in datasets.load_ner(name, split) if fits(args, splitter, record["text"])]
        rng.shuffle(records)
        types = list(datasets.entity_types(name))
        for record in records[: args.per_dataset]:
            text = record["text"]
            if extractor is None:
                spans = [(entity["start"], entity["end"], entity["type"]) for entity in record["entities"]]
            else:
                result = extractor.extract(text, {"entities": types}, threshold=args.entity_threshold,
                                           include_spans=True, max_len=args.max_words)
                spans = []
                for label, mentions in result.get("entities", {}).items():
                    for mention in mentions:
                        spans.append((*byte_offsets(text, mention["start"], mention["end"]), label))
            entities = [{"id": f"e{index}", "type": kind, "span": {"start": start, "end": end, "unit": "utf8_bytes"}}
                        for index, (start, end, kind) in enumerate(sorted(set(spans)))]
            rows.append({"version": 1, "id": record["id"], "text": text,
                         "schema": {"entities": types}, "entities": entities})
    return rows


def build(args: argparse.Namespace) -> dict[str, Any]:
    import antenna_datasets as datasets

    provenance, torch = oracle.prepare_runtime(args.upstream)
    from gliner2 import AutoExtractor
    from gliner2.classification import Classifier

    teacher = AutoExtractor.from_pretrained(str(args.classifier), local_files_only=True, map_location="cpu",
                                            use_flashdeberta=False).float().eval()
    classifier = Classifier(teacher)
    extractor = None
    if args.teacher_entities:
        extractor = AutoExtractor.from_pretrained(str(args.teacher_entities), local_files_only=True, map_location="cpu",
                                                  use_flashdeberta=False).float().eval()
    rng = random.Random(args.seed)
    rows = classification_rows(args, datasets, classifier, "train", rng) + entity_rows(args, datasets, extractor, teacher.processor.word_splitter, "train", rng)
    # The native job requires disjoint splits by text; datasets repeat texts.
    seen: set[str] = set()
    rows = [row for row in rows if not (row["text"] in seen or seen.add(row["text"]))]
    rng.shuffle(rows)
    validation_count = max(1, int(len(rows) * args.validation_fraction))
    splits = {"validation": rows[:validation_count], "train": rows[validation_count:]}
    with oracle.atomic_output_directory(args.output) as directory:
        files = {}
        for split, items in splits.items():
            path = directory / f"{split}.jsonl"
            path.write_text("".join(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n" for item in items), encoding="utf-8")
            files[split] = {"path": path.name, "records": len(items), "sha256": oracle.sha256_file(path),
                            "size_bytes": path.stat().st_size}
        oracle.write_json(directory / "manifest.json", {
            "dataset_format": "gliner_boundary_dataset.Row/version=1", "files": files,
            "generator_sha256": oracle.sha256_file(Path(__file__)),
            "datasets_module_sha256": oracle.sha256_file(Path(datasets.__file__)),
            "classification_teacher": str(args.classifier), "entity_teacher": str(args.teacher_entities) if args.teacher_entities else "gold",
            "gold_weight": args.gold_weight, "seed": args.seed, "per_dataset": args.per_dataset,
            "label_sampling": [args.min_labels, args.max_labels], "classification_sets": CLASSIFICATION, "entity_sets": NER,
            "provenance": provenance, "heldout_quality_claim": False,
        })
        oracle.verify_upstream_checkout(args.upstream)
    return {"status": "built", "output": str(args.output.resolve()), **{k: v["records"] for k, v in files.items()}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--classifier", type=Path, required=True, help="GLiNER2.5-Decide checkpoint directory")
    parser.add_argument("--teacher-entities", type=Path, help="extraction teacher (e.g. gliner2.5-base); gold spans if omitted")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--per-dataset", type=int, default=1000)
    parser.add_argument("--min-labels", type=int, default=8)
    parser.add_argument("--max-labels", type=int, default=24)
    parser.add_argument("--gold-weight", type=float, default=0.5)
    parser.add_argument("--entity-threshold", type=float, default=0.5)
    parser.add_argument("--validation-fraction", type=float, default=0.05)
    parser.add_argument("--max-words", type=int, default=128)
    parser.add_argument("--seed", type=int, default=20260925)
    print(json.dumps(build(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
