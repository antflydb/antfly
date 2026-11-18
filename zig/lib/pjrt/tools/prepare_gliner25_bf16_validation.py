#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Prepare CPU-tokenized BF16 classification checks without exporting HLO.

Includes the release benchmark's seeded hotel texts, heterogeneous batches up
through 32, empty/Unicode/negated text and five custom schemas. No GPU is used.
Run benchmark_gliner25_torch.py separately to capture the CUDA BF16 reference.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import random

os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

from prepare_gliner25 import TASKS


def hotel_texts(count=32):
    rng = random.Random(11)
    parts = [
        "Guest in room {r} says the AC has been out since yesterday and they want to move tonight.",
        "The wifi in {r} keeps dropping during video calls; can someone take a look before 9am?",
        "I was charged twice for the minibar on my final bill for room {r}. Please refund one charge.",
        "Loud music from the room next to {r} until 2am again. This is the third night.",
        "What time is checkout? We'd like a late checkout for room {r} if possible.",
        "The bathroom in {r} wasn't cleaned today and there are no fresh towels.",
        "Front desk staff were incredibly kind when our flight was cancelled. Thank you!",
        "Water is leaking from the ceiling in {r} onto the bed. Please send someone immediately.",
    ]
    return [" ".join(rng.choice(parts).format(r=rng.randint(100, 1899))
                     for _ in range(rng.randint(1, 3))) for _ in range(count)]


def requests():
    texts = hotel_texts() + [
        "José visited München with Zoë. 東京 is in Japan.", "", "Please help immediately!",
        "The bill is wrong. The room is fine.", "No maintenance or room change is needed.",
    ]
    for i, text in enumerate(texts):
        yield f"classification_{i}", [text], TASKS
    for start in range(0, len(texts), 8):
        yield f"classification_{start}", texts[start:start + 8], TASKS
    yield "classification_0", texts[:32], TASKS
    yield "classification_32_repeat", texts[32:], TASKS
    schemas = [
        [{"task": "sentiment", "labels": ["positive", "negative", "neutral"]}],
        [{"task": "is_urgent", "labels": ["yes", "no"]}],
        [{"task": "flags", "labels": ["unsafe", "private", "spam", "urgent"],
          "multi_label": True, "cls_threshold": 0.7}],
        [{"task": "topic", "labels": ["biology", "geology", "history", "business", "politics", "culture"]}],
        [{"task": "route", "labels": ["finance", "technical support", "legal", "human resources",
                                     "sales", "marketing", "operations", "security", "product", "billing", "account", "other"]},
         {"task": "language", "labels": ["English", "French", "Spanish"]},
         {"task": "needs_reply", "labels": ["yes", "no"], "multi_label": True, "cls_threshold": 0.3}],
    ]
    texts = ["", "No maintenance or room change is needed.",
             "We love the new service. Please refund the duplicate charge immediately.",
             "José visited München with Zoë. 東京 is in Japan.",
             "Photosynthesis converts light into chemical energy.",
             "Please help! " + "A storm damaged the building and staff need assistance. " * 35]
    for si, tasks in enumerate(schemas):
        for ti, text in enumerate(texts):
            for batch in ([1, 8] if ti in (0, 2, 5) else [1]):
                yield f"schema{si}_text{ti}", [text] * batch, tasks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        parser.error("Use a fresh output file")
    import torch
    from gliner2 import AutoExtractor, Schema

    torch.set_num_threads(2)
    model = AutoExtractor.from_pretrained(str(args.model), local_files_only=True).eval()
    marker = model.processor.tokenizer.convert_tokens_to_ids("[L]")
    cases = []
    for name, texts, tasks in requests():
        schema = Schema.from_dict({"classifications": tasks}).build()
        batch = model.processor.collate_fn_inference(
            [(t, schema) for t in texts], max_len=512, architecture=model.architecture,
            error_policy="raise", build_targets=False)
        positions = torch.stack([torch.where(ids == marker)[0] for ids in batch.input_ids])
        tensors = [batch.input_ids.long(), batch.attention_mask.long(), positions, torch.ones_like(positions)]
        repeat = name.endswith("_repeat")
        if repeat:
            name = name.removesuffix("_repeat")
        case_id = f"{name}_b{len(texts)}_s{batch.input_ids.shape[1]}" + ("_repeat" if repeat else "")
        public = [{("name" if k == "task" else "threshold" if k == "cls_threshold" else k): v
                   for k, v in t.items()} for t in tasks]
        cases.append({
            "id": case_id, "text": texts[0], "texts": texts,
            "schema_json": json.dumps({"classifications": public}),
            "classification_tasks": [{"name": t["task"], "size": len(t["labels"]),
                                      "multi_label": t.get("multi_label", False),
                                      "threshold": t.get("cls_threshold", 0.5)} for t in tasks],
            "inputs": [{"name": n, "shape": list(t.shape), "data": t.flatten().tolist(), "dtype": "int64"}
                       for n, t in zip(("input_ids", "attention_mask", "decision_marker_positions", "decision_marker_mask"), tensors, strict=True)],
            "outputs": [{"name": "logits", "shape": [positions.numel()], "dtype": "float32"}],
        })
    if len(cases) != 89 or len({c["id"] for c in cases}) != 89:
        raise ValueError("Incomplete or duplicate validation cases")
    with (args.model / "model.safetensors").open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    result = {"architecture": model.architecture, "precision": "bf16", "model_sha256": digest,
              "preparation": {"producer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}, "cases": cases}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, allow_nan=False) + "\n")
    print(f"Prepared {len(cases)} classification cases")


if __name__ == "__main__":
    main()
