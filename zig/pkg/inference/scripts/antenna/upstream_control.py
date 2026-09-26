#!/usr/bin/env python3
"""Train an Antenna student with upstream GLiNER2's own trainer, as a control
for the native trainer on the same rows.

Reads boundary training rows (``teacher_targets.py`` output) and converts them
to upstream ``InputExample``s with hard labels: the classification gold label
and entity surface strings (upstream matches every occurrence). Settings mirror
a native job: FP32, the same learning rates, epochs and effective batch.

    PYTHONDONTWRITEBYTECODE=1 <oracle venv>/bin/python upstream_control.py \\
        --upstream <GLiNER2 checkout> --student <init_student.py output> \\
        --train <rows.jsonl> --output <dir outside Git>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "gliner25"))

import oracle  # noqa: E402


def examples(path: Path) -> list[Any]:
    from gliner2.training.data import Classification, InputExample

    result = []
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        text = row["text"]
        raw = text.encode()
        if "classifications" in row:
            task = row["schema"]["classifications"][0]
            result.append(InputExample(text=text, classifications=[Classification(
                task=task["name"], labels=task["labels"], true_label=row["classifications"][0]["labels"][0])]))
        else:
            entities: dict[str, list[str]] = {kind: [] for kind in row["schema"]["entities"]}
            for entity in row.get("entities", []):
                surface = raw[entity["span"]["start"]:entity["span"]["end"]].decode()
                if surface not in entities[entity["type"]]:
                    entities[entity["type"]].append(surface)
            result.append(InputExample(text=text, entities=entities))
    return result


def run(args: argparse.Namespace) -> dict[str, Any]:
    provenance, torch = oracle.prepare_runtime(args.upstream)
    from gliner2 import AutoExtractor
    from gliner2.training.trainer import ExtractorTrainer, TrainingConfig

    torch.manual_seed(args.seed)
    model = AutoExtractor.from_pretrained(str(args.student), local_files_only=True, map_location=args.device,
                                          use_flashdeberta=False).float()
    train = examples(args.train)
    config = TrainingConfig(output_dir=str(args.output / "trainer"), num_epochs=args.epochs, batch_size=args.batch_size,
                            gradient_accumulation_steps=args.accumulation, encoder_lr=args.encoder_lr,
                            task_lr=args.task_lr, warmup_ratio=0.1, fp16=False, bf16=False, eval_strategy="no",
                            save_best=False, logging_steps=50, num_workers=0, pin_memory=False, seed=args.seed,
                            max_len=128)
    trainer = ExtractorTrainer(model, config, train_data=train)
    trainer.train()
    model.save_pretrained(str(args.output / "model"))
    oracle.write_json(args.output / "control.json", {
        "format_version": 1, "student": str(args.student), "train": str(args.train),
        "train_sha256": oracle.sha256_file(args.train), "examples": len(train),
        "settings": {k: getattr(config, k) for k in ("num_epochs", "batch_size", "gradient_accumulation_steps",
                                                        "encoder_lr", "task_lr", "warmup_ratio", "seed")},
        "device": args.device, "provenance": provenance, "generator_sha256": oracle.sha256_file(Path(__file__)),
    })
    return {"status": "trained", "output": str(args.output.resolve()), "examples": len(train)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--student", type=Path, required=True)
    parser.add_argument("--train", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--accumulation", type=int, default=2)
    parser.add_argument("--encoder-lr", type=float, default=3e-5)
    parser.add_argument("--task-lr", type=float, default=5e-4)
    parser.add_argument("--seed", type=int, default=2509)
    parser.add_argument("--device", default="mps")
    print(json.dumps(run(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
