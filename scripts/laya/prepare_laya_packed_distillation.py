#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<5", "safetensors>=0.5", "numpy>=2"]
# ///
"""Blend gold targets with an unpacked Laya teacher for tree-packed fine-tuning.

    uv run --script prepare_laya_packed_distillation.py records.jsonl \\
        --teacher ./models/extractors/laya --common common.py \\
        --gold-weight 0.5 --output distilled.jsonl

Input and output are native Antfly Laya records (prepare_laya_finetune.py).
Each output target is `w * gold + (1 - w) * teacher`, where the teacher is
the released, unpacked checkpoint's calibrated distribution for the same
question. The teacher only sees upstream's 512-token sequence; when upstream
would truncate the state, the record keeps its gold target so the student is
never taught a distribution computed from a different state. Provenance is
written to `<output>.json`. See zig/pkg/inference/models/laya/LAYA.md.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path

QTYPES = {"choice": 0, "score": 1, "noul": 2}


def upstream_question(record: dict) -> dict:
    """Map a native record to common.build_sequence's question dictionary."""
    kind, labels = record["kind"], record["labels"]
    descriptions = record.get("descriptions") or [""] * len(labels)
    if kind == "choice":
        crit = {label: (desc or None) for label, desc in zip(labels, descriptions)}
    elif kind == "score":
        crit = [desc or label for label, desc in zip(labels, descriptions)]
    elif kind == "noul":
        if labels != ["false", "true"]:
            raise ValueError(f"Boolean labels must be false, true: {record['id']}")
        crit = {"false": descriptions[0], "true": descriptions[1]}
    else:
        raise ValueError(f"Unknown question kind: {record['id']}")
    return {"t": kind, "ins": record["instruction"], "crit": crit}


def temperature(decision: dict, kind: str, count: int) -> float:
    """Inference calibration: option-count bucket, then per-type temperature."""
    bucket = "2" if count <= 2 else "3-5" if count <= 5 else "6-10" if count <= 10 else "11+"
    buckets = decision.get("temperature_by_options", {})
    value = buckets.get(f"{kind}:{bucket}")
    if value is None:
        value = decision.get("temperature", [1, 1, 1])[QTYPES[kind]]
    return max(0.001, float(value))


def blend(gold: list[float], teacher: list[float], gold_weight: float) -> list[float]:
    if len(gold) != len(teacher) or not 0 <= gold_weight <= 1:
        raise ValueError("Cannot blend mismatched targets")
    mixed = [gold_weight * g + (1 - gold_weight) * t for g, t in zip(gold, teacher)]
    total = sum(mixed)
    if not math.isfinite(total) or total <= 0:
        raise ValueError("Blended target is not a distribution")
    return [p / total for p in mixed]


def state_fits(tok, record: dict, decision: dict, build_sequence) -> bool:
    """True when upstream keeps every state token for this question."""
    ids, _ = build_sequence(
        tok, record["text"], upstream_question(record), decision["max_len"], decision["head_max_len"]
    )
    state = tok(record["text"].replace(tok.mask_token, " "), add_special_tokens=False)["input_ids"]
    head_and_options = len(ids) - len(state) - 1
    return head_and_options >= 0 and ids[head_and_options : head_and_options + len(state)] == state


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("records", type=Path)
    parser.add_argument("--teacher", type=Path, required=True, help="Prepared unpacked Laya directory")
    parser.add_argument("--common", type=Path, required=True, help="Upstream laya/common.py")
    parser.add_argument("--gold-weight", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"Output already exists: {args.output}")
    if not 0 <= args.gold_weight <= 1:
        parser.error("--gold-weight must be in [0, 1]")

    import importlib.util

    import torch
    from safetensors.torch import load_file
    from transformers import ModernBertConfig, ModernBertModel, PreTrainedTokenizerFast

    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    raw = json.loads((args.teacher / "config.json").read_text())
    decision = raw.pop("laya")
    if decision.get("packing", {}).get("mode", "none") != "none":
        parser.error("The teacher must be an unpacked Laya checkpoint")
    cfg = ModernBertConfig.from_dict(raw)
    cfg._attn_implementation = "eager"
    model = common.DecisionModel(
        ModernBertModel(cfg),
        head_layers=decision["head_layers"],
        n_act=len(decision.get("act_costs", {})) + 1,
    ).eval()
    model.load_state_dict(load_file(args.teacher / "model.safetensors"), strict=True)
    tok = PreTrainedTokenizerFast.from_pretrained(args.teacher)

    records = [json.loads(line) for line in args.records.read_text().splitlines() if line.strip()]
    if not records:
        raise ValueError("Empty dataset")
    teacher = [None] * len(records)
    eligible = [
        i for i, r in enumerate(records)
        if len(r["labels"]) <= 20 and state_fits(tok, r, decision, common.build_sequence)
    ]
    for start in range(0, len(eligible), args.batch_size):
        chunk = eligible[start : start + args.batch_size]
        items = []
        for i in chunk:
            ids, markers = common.build_sequence(
                tok, records[i]["text"], upstream_question(records[i]), decision["max_len"], decision["head_max_len"]
            )
            items.append({"ids": ids, "markers": markers, "qtype": QTYPES[records[i]["kind"]]})
        batch = common.collate_items([items], tok.pad_token_id)
        with torch.no_grad():
            logits, _ = model(*(batch[k] for k in ("input_ids", "attention_mask", "marker_pos", "marker_mask", "qtype")))
        for row, i in enumerate(chunk):
            n = len(records[i]["labels"])
            scale = temperature(decision, records[i]["kind"], n)
            teacher[i] = torch.softmax(logits[row, :n].double() / scale, -1).tolist()

    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=args.output.parent, delete=False) as out:
            temporary = Path(out.name)
            for record, soft in zip(records, teacher):
                if soft is not None:
                    record = {**record, "target": blend(record["target"], soft, args.gold_weight)}
                out.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
            out.flush()
            os.fsync(out.fileno())
        os.link(temporary, args.output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

    def digest(path: Path) -> str:
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()

    provenance = {
        "format": "antfly-laya-packed-distillation/v1",
        "records": len(records),
        "distilled": len(eligible),
        "gold_only": len(records) - len(eligible),
        "gold_weight": args.gold_weight,
        "source_sha256": digest(args.records),
        "teacher_weights_sha256": digest(args.teacher / "model.safetensors"),
        "teacher_config_sha256": digest(args.teacher / "config.json"),
        "common_sha256": digest(args.common),
        "output_sha256": digest(args.output),
        "torch_version": torch.__version__,
    }
    Path(f"{args.output}.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps(provenance))


if __name__ == "__main__":
    main()
