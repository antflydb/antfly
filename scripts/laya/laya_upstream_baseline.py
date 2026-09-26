#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<5", "safetensors>=0.5", "numpy>=2"]
# ///
"""Score native Laya records with upstream's own unpacked model code.

    uv run --script laya_upstream_baseline.py records.jsonl \\
        --model ./models/extractors/laya --common common.py --output baseline.json

Unlike Antfly's serving pipeline, upstream `common.build_sequence` accepts any
number of options and squeezes them into the fixed `head_max_len` budget.
This gives the unpacked reference number for questions Antfly serves only in
candidate mode (Banking77). Accuracy is argmax against the argmax of the gold
target; soft CE uses the checkpoint's calibration.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
from pathlib import Path

from prepare_laya_packed_distillation import QTYPES, temperature, upstream_question


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("records", type=Path)
    parser.add_argument("--model", type=Path, required=True, help="Prepared unpacked Laya directory")
    parser.add_argument("--common", type=Path, required=True, help="Upstream laya/common.py")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    import torch
    from safetensors.torch import load_file
    from transformers import ModernBertConfig, ModernBertModel, PreTrainedTokenizerFast

    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    raw = json.loads((args.model / "config.json").read_text())
    decision = raw.pop("laya")
    if decision.get("packing", {}).get("mode", "none") != "none":
        parser.error("The model must be an unpacked Laya checkpoint")
    cfg = ModernBertConfig.from_dict(raw)
    cfg._attn_implementation = "eager"
    model = common.DecisionModel(
        ModernBertModel(cfg), head_layers=decision["head_layers"], n_act=len(decision.get("act_costs", {})) + 1
    ).eval()
    model.load_state_dict(load_file(args.model / "model.safetensors"), strict=True)
    tok = PreTrainedTokenizerFast.from_pretrained(args.model)

    records = [json.loads(line) for line in args.records.read_text().splitlines() if line.strip()]
    correct, ce = 0, 0.0
    for start in range(0, len(records), args.batch_size):
        chunk = records[start : start + args.batch_size]
        items = []
        for r in chunk:
            ids, markers = common.build_sequence(
                tok, r["text"], upstream_question(r), decision["max_len"], decision["head_max_len"]
            )
            items.append({"ids": ids, "markers": markers, "qtype": QTYPES[r["kind"]]})
        batch = common.collate_items([items], tok.pad_token_id)
        with torch.no_grad():
            logits, _ = model(*(batch[k] for k in ("input_ids", "attention_mask", "marker_pos", "marker_mask", "qtype")))
        for row, r in enumerate(chunk):
            n = len(r["labels"])
            probs = torch.softmax(logits[row, :n].double() / temperature(decision, r["kind"], n), -1)
            gold = max(range(n), key=lambda k: r["target"][k])
            correct += int(int(torch.argmax(probs)) == gold)
            ce -= sum(t * math.log(max(float(p), 1e-12)) for t, p in zip(r["target"], probs.tolist()))
    report = {"records": len(records), "accuracy": correct / len(records), "soft_ce": ce / len(records)}
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
