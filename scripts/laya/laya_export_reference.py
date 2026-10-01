#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<6", "safetensors>=0.5", "numpy>=2"]
# ///
"""Generate a PyTorch inference oracle for an exported Laya training checkpoint.

Run Antfly's 'laya finetuned export' test with ANTFLY_LAYA_EXPORT_REFERENCE set
to --output. The fixture links the supplied model without copying its weights.
"""

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

import torch
from safetensors.torch import load_file
from transformers import AutoTokenizer, ModernBertConfig, ModernBertModel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--common", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--records", type=Path, help="Native JSONL decisions for serving qualification"
    )
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output directory must be new")
    torch.set_num_threads(8)
    spec = importlib.util.spec_from_file_location("laya_common", args.common)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    raw = json.loads((args.model / "config.json").read_text())
    cfg = raw.pop("laya")
    encoder = ModernBertConfig.from_dict(raw)
    encoder.reference_compile = False
    encoder._attn_implementation = "eager"
    model = common.DecisionModel(
        ModernBertModel(encoder), cfg["head_layers"], len(cfg["act_costs"]) + 1
    ).eval()
    model.load_state_dict(load_file(args.model / "model.safetensors"), strict=True)
    tok = AutoTokenizer.from_pretrained(args.model, local_files_only=True)
    questions = [
        {
            "t": "choice",
            "ins": "which tool is needed?",
            "crit": {"search": None, "fetch": None, "none": None},
        },
        {"t": "score", "ins": "urgency?", "crit": ["low", "medium", "high"]},
        {"t": "noul", "ins": "is search needed?", "crit": None},
    ]
    states = ["please find the document", "urgent", "hello [MASK] world"]
    records = None
    if args.records:
        records = [json.loads(line) for line in args.records.read_text().splitlines()]
        states = [r["text"] for r in records]
        questions = [
            {
                "t": r["kind"],
                "ins": r["instruction"],
                "crit": r.get("descriptions", r["labels"])
                if r["kind"] == "score"
                else dict(
                    zip(r["labels"], r.get("descriptions", [None] * len(r["labels"])))
                ),
            }
            for r in records
        ]
    sequences = []
    for text, question in zip(states, questions):
        ids, markers = common.build_sequence(
            tok, text, question, cfg["max_len"], cfg["head_max_len"]
        )
        sequences.append(
            {"ids": ids, "markers": markers, "qtype": common.QTYPES[question["t"]]}
        )
    all_logits, all_actions = [], []
    with torch.no_grad():
        # Single-example references make padding/batch invariance independent
        # of the serving test's batch sizes.
        for sequence in sequences:
            batch = common.collate_items([[sequence]], tok.pad_token_id)
            logits, actions = model(
                *(
                    batch[key]
                    for key in (
                        "input_ids",
                        "attention_mask",
                        "marker_pos",
                        "marker_mask",
                        "qtype",
                    )
                )
            )
            all_logits.append(logits[0])
            all_actions.append(actions[0])
    probabilities = []
    for row, sequence in enumerate(sequences):
        kind = questions[row]["t"]
        count = len(sequence["markers"])
        bucket = (
            "2"
            if count <= 2
            else "3-5"
            if count <= 5
            else "6-10"
            if count <= 10
            else "11+"
        )
        temperature = cfg.get("temperature_by_options", {}).get(
            f"{kind}:{bucket}", cfg.get("temperature", [1, 1, 1])[sequence["qtype"]]
        )
        probabilities.append(
            torch.softmax(all_logits[row][:count] / temperature, -1).tolist()
        )
    with (args.model / "model.safetensors").open("rb") as weights:
        digest = hashlib.file_digest(weights, "sha256").hexdigest()
    args.output.mkdir(parents=True)
    (args.output / "model").symlink_to(args.model.resolve(), target_is_directory=True)
    actions = torch.stack(all_actions)
    (args.output / "reference.json").write_text(
        json.dumps(
            {
                "states": states,
                "questions": questions,
                "records": records,
                "sequences": sequences,
                "logits": [row.tolist() for row in all_logits],
                "action_logits": actions.tolist(),
                "probabilities": probabilities,
                "act_probabilities": torch.softmax(actions, -1)[:, 0].tolist(),
                "weights_sha256": digest,
            },
            indent=2,
        )
        + "\n"
    )
    print(json.dumps({"output": str(args.output), "weights_sha256": digest}))


if __name__ == "__main__":
    main()
