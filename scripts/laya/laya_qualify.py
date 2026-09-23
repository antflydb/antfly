#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# /// script
# requires-python = ">=3.11"
# dependencies = ["torch>=2.6,<3", "transformers>=4.51,<5", "safetensors>=0.5", "numpy>=2", "pyarrow>=20"]
# ///
"""Capture released-checkpoint accuracy and numerical references on pinned data.

Consumes the prepared Antfly model, upstream common.py, and local dataset files.
Dataset revisions and downloads are documented in docs/design/laya-qualification.md.
No generated labels, model downloads, or remote code loading occur here.
"""

import argparse
import hashlib
import importlib.util
import json
import random
import time
from pathlib import Path

import pyarrow.parquet as pq
import torch
from safetensors.torch import load_file
from transformers import AutoTokenizer, ModernBertConfig, ModernBertModel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--common", type=Path, required=True)
    parser.add_argument("--ag-news", type=Path, required=True)
    parser.add_argument("--boolq", type=Path, required=True)
    parser.add_argument("--sst5", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples-per-task", type=int, default=64)
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("--threads must be positive")
    torch.set_num_threads(args.threads)
    torch.manual_seed(714)
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
    data = {
        "ag_news": [json.loads(line) for line in args.ag_news.read_text().splitlines()],
        "boolq": pq.read_table(args.boolq).to_pylist(),
        "sst5": [json.loads(line) for line in args.sst5.read_text().splitlines()],
    }
    groups = []
    excluded = {}
    for dataset, records in data.items():
        random.Random(714).shuffle(records)
        selected = []
        excluded[dataset] = 0
        for record in records:
            if dataset == "ag_news":
                labels = ["world", "sports", "business", "science and technology"]
                text = record["title"] + "\n" + record["description"]
                kind, instruction = (
                    "choice",
                    "Which topic best describes this news article?",
                )
                target = record["label"] - 1
            elif dataset == "boolq":
                labels = ["false", "true"]
                text = record["passage"]
                kind, instruction = "noul", record["question"] + "?"
                target = int(record["answer"])
            else:
                labels = [
                    "very negative",
                    "negative",
                    "neutral",
                    "positive",
                    "very positive",
                ]
                text = record["text"]
                kind, instruction = (
                    "score",
                    "What is the sentiment of this movie review?",
                )
                target = record["label"]
            q = {
                "t": kind,
                "ins": instruction,
                "crit": dict.fromkeys(labels)
                if kind == "choice"
                else labels
                if kind == "score"
                else None,
            }
            # Select whole examples; do not let upstream silently truncate state.
            full, _ = common.build_sequence(tok, text, q, 100000, cfg["head_max_len"])
            if len(full) > cfg["max_len"]:
                excluded[dataset] += 1
                continue
            ids, markers = common.build_sequence(
                tok, text, q, cfg["max_len"], cfg["head_max_len"]
            )
            selected.append(
                {
                    "dataset": dataset,
                    "target": target,
                    "task": {
                        "text": text,
                        "question": {
                            "name": dataset,
                            "kind": kind,
                            "instruction": instruction,
                            "labels": labels,
                            "descriptions": [""] * len(labels),
                        },
                    },
                    "ids": ids,
                    "markers": markers,
                    "qtype": common.QTYPES[kind],
                }
            )
            if len(selected) == args.samples_per_task:
                break
        if len(selected) != args.samples_per_task:
            raise ValueError(f"Not enough eligible {dataset} examples")
        groups.append(selected)
    rows = [row for group in zip(*groups) for row in group]
    elapsed = 0
    with torch.inference_mode():
        for start in range(0, len(rows), 8):
            chunk = rows[start : start + 8]
            batch = common.collate_items(
                [
                    [
                        {
                            "ids": row["ids"],
                            "markers": row["markers"],
                            "qtype": row["qtype"],
                        }
                        for row in chunk
                    ]
                ],
                tok.pad_token_id,
            )
            began = time.perf_counter()
            logits, acts = model(
                batch["input_ids"],
                batch["attention_mask"],
                batch["marker_pos"],
                batch["marker_mask"],
                batch["qtype"],
            )
            elapsed += time.perf_counter() - began
            for row, z, act in zip(chunk, logits, acts):
                k = len(row["markers"])
                qt = row["qtype"]
                temperature = cfg["temperature_by_options"].get(
                    common.temp_bucket(qt, k), cfg["temperature"][qt]
                )
                p = torch.softmax(z[:k] / max(temperature, 0.001), -1)
                row.update(
                    logits=z[:k].tolist(),
                    action_logits=act.tolist(),
                    probabilities=p.tolist(),
                    act_probability=act.softmax(-1)[0].item(),
                )
            print(f"reference {min(start + 8, len(rows))}/{len(rows)}", flush=True)
    metrics = {}
    for dataset in data:
        subset = [row for row in rows if row["dataset"] == dataset]
        metrics[dataset] = {
            "count": len(subset),
            "accuracy": sum(
                max(range(len(r["probabilities"])), key=r["probabilities"].__getitem__)
                == r["target"]
                for r in subset
            )
            / len(subset),
        }
        if dataset == "sst5":
            metrics[dataset]["expected_level_mae"] = sum(
                abs(sum(i * p for i, p in enumerate(r["probabilities"])) - r["target"])
                for r in subset
            ) / len(subset)
    output = {
        "source": json.loads((args.model / "model_manifest.json").read_text())[
            "source"
        ],
        "seed": 714,
        "dataset_revisions": {
            "sh0416/ag_news": "70e3fa1915be9a8daebec5e840f20df9a8e18793",
            "google/boolq": "35b264d03638db9f4ce671b711558bf7ff0f80d5",
            "SetFit/sst5": "e51bdcd8cd3a30da231967c1a249ba59361279a3",
        },
        "file_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (args.common, args.ag_news, args.boolq, args.sst5)
        },
        "excluded_overlength": excluded,
        "torch_version": torch.__version__,
        "transformers_attention": "eager",
        "threads": torch.get_num_threads(),
        "forward_seconds": elapsed,
        "metrics": metrics,
        "rows": rows,
    }
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps({k: v for k, v in output.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    main()
