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
# dependencies = ["torch==2.6.0", "transformers==4.57.6", "safetensors>=0.5", "numpy>=2"]
# ///
"""Measure eager float32 PyTorch CUDA against the saved Laya CPU oracle."""

import argparse
import importlib.util
import json
import math
import time
from pathlib import Path

import torch
from safetensors.torch import load_file
from transformers import AutoTokenizer, ModernBertConfig, ModernBertModel


def prepare(common, tokenizer, head, chunk):
    items = []
    for row in chunk:
        question = row["task"]["question"]
        kind = question["kind"]
        criteria = (
            dict(zip(question["labels"], question["descriptions"]))
            if kind == "choice"
            else question["labels"]
            if kind == "score"
            else None
        )
        ids, markers = common.build_sequence(
            tokenizer,
            row["task"]["text"],
            {"t": kind, "ins": question["instruction"], "crit": criteria},
            head["max_len"],
            head["head_max_len"],
        )
        items.append({"ids": ids, "markers": markers, "qtype": common.QTYPES[kind]})
    return common.collate_items([items], tokenizer.pad_token_id)


def decode(common, head, chunk, logits, actions):
    decisions = []
    for row, scores, acts in zip(chunk, logits.tolist(), actions.tolist()):
        question = row["task"]["question"]
        labels = question["labels"]
        temperature = head["temperature_by_options"].get(
            common.temp_bucket(row["qtype"], len(labels)),
            head["temperature"][row["qtype"]],
        )
        scores = scores[: len(labels)]
        maximum = max(scores)
        probabilities = [
            math.exp((score - maximum) / max(temperature, 0.001)) for score in scores
        ]
        total = sum(probabilities)
        probabilities = [p / total for p in probabilities]
        maximum = max(acts)
        action = [math.exp(x - maximum) for x in acts]
        entropy = -sum(p * math.log(max(p, 1e-12)) for p in probabilities)
        decisions.append(
            {
                "name": question["name"],
                "kind": question["kind"],
                "labels": labels,
                "label": labels[max(range(len(labels)), key=probabilities.__getitem__)],
                "probabilities": probabilities,
                "act_probability": action[0] / sum(action),
                "confidence": max(probabilities)
                if question["kind"] == "noul"
                else max(0, min(1, 1 - entropy / math.log(len(labels)))),
                "expected_value": sum(i * p for i, p in enumerate(probabilities))
                if question["kind"] == "score"
                else None,
                "true_probability": probabilities[1]
                if question["kind"] == "noul"
                else None,
            }
        )
    return decisions


def validate(chunk, decisions):
    for row, decision in zip(chunk, decisions, strict=True):
        if (
            decision["label"]
            != row["task"]["question"]["labels"][
                max(
                    range(len(row["probabilities"])),
                    key=row["probabilities"].__getitem__,
                )
            ]
        ):
            raise ValueError("PyTorch decision differs from CPU oracle")
        for got, want in zip(
            decision["probabilities"] + [decision["act_probability"]],
            row["probabilities"] + [row["act_probability"]],
            strict=True,
        ):
            if not math.isfinite(got) or abs(got - want) > 5e-5:
                raise ValueError("PyTorch CUDA comparison fails CPU oracle parity")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument(
        "--scopes",
        nargs="+",
        choices=("pipeline", "prepared", "resident"),
        default=["pipeline", "prepared", "resident"],
    )
    args = parser.parse_args()
    if args.samples < 2 or args.warmups < 0:
        parser.error("At least two samples and nonnegative warmups are required")
    if not torch.cuda.is_available():
        raise RuntimeError("PyTorch CUDA is required for comparison")
    torch.set_num_threads(4)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_float32_matmul_precision("highest")
    spec = importlib.util.spec_from_file_location(
        "laya_common", args.work_dir / "common.py"
    )
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    model_path = args.work_dir / "released/model"
    config = json.loads((model_path / "config.json").read_text())
    head = config.pop("laya")
    encoder = ModernBertConfig.from_dict(config)
    encoder.reference_compile = False
    encoder._attn_implementation = "eager"
    began = time.perf_counter()
    model = common.DecisionModel(
        ModernBertModel(encoder), head["head_layers"], len(head["act_costs"]) + 1
    ).eval()
    model.load_state_dict(load_file(model_path / "model.safetensors"), strict=True)
    model = model.float().cuda()
    torch.cuda.synchronize()
    load_ms = (time.perf_counter() - began) * 1000
    tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
    rows = json.loads((args.work_dir / "released/qualification.json").read_text())[
        "rows"
    ]
    results = []
    with torch.inference_mode():
        for profile in ("fixed", "mixed"):
            for size in (1, 8):
                chunk = (
                    [rows[0]] * size
                    if profile == "fixed"
                    else list(reversed(rows[:size]))
                )
                host = prepare(common, tokenizer, head, chunk)
                for row, ids, markers in zip(
                    chunk,
                    host["input_ids"].tolist(),
                    host["marker_pos"].tolist(),
                    strict=True,
                ):
                    if (
                        ids[: len(row["ids"])] != row["ids"]
                        or markers[: len(row["markers"])] != row["markers"]
                    ):
                        raise ValueError(
                            "Benchmark preprocessing differs from pinned oracle"
                        )

                def upload(batch):
                    return {
                        key: batch[key].cuda()
                        for key in (
                            "input_ids",
                            "attention_mask",
                            "marker_pos",
                            "marker_mask",
                            "qtype",
                        )
                    }

                resident = upload(host)
                for scope in args.scopes:
                    samples = []
                    for iteration in range(args.warmups + args.samples):
                        torch.cuda.synchronize()
                        began = time.perf_counter()
                        batch = (
                            resident
                            if scope == "resident"
                            else upload(
                                prepare(common, tokenizer, head, chunk)
                                if scope == "pipeline"
                                else host
                            )
                        )
                        logits, actions = model(
                            batch["input_ids"],
                            batch["attention_mask"],
                            batch["marker_pos"],
                            batch["marker_mask"],
                            batch["qtype"],
                        )
                        if scope != "resident":
                            logits, actions = logits.cpu(), actions.cpu()
                        decisions = (
                            decode(common, head, chunk, logits, actions)
                            if scope == "pipeline"
                            else None
                        )
                        torch.cuda.synchronize()
                        elapsed = (time.perf_counter() - began) * 1000
                        if iteration >= args.warmups:
                            samples.append(elapsed)
                        if decisions is None:
                            decisions = decode(
                                common, head, chunk, logits.cpu(), actions.cpu()
                            )
                        validate(chunk, decisions)
                    samples.sort()
                    results.append(
                        {
                            "profile": profile,
                            "scope": scope,
                            "batch": size,
                            "p50_ms": samples[len(samples) // 2],
                            "p95_ms": samples[math.ceil(len(samples) * 0.95) - 1],
                            "questions_per_second": size
                            * 1000
                            / samples[len(samples) // 2],
                            "useful_tokens": sum(len(r["ids"]) for r in chunk),
                            "padded_tokens": size * max(len(r["ids"]) for r in chunk),
                            "samples": args.samples,
                            "warmups": args.warmups,
                        }
                    )
                    print(json.dumps(results[-1]), flush=True)
    import transformers

    args.output.write_text(
        json.dumps(
            {
                "torch_version": torch.__version__,
                "transformers_version": transformers.__version__,
                "cuda_version": torch.version.cuda,
                "dtype": "float32",
                "tf32": False,
                "threads": 4,
                "load_ms": load_ms,
                "scopes": {
                    "pipeline": "text/questions through decoded host decisions; excludes model load and transport",
                    "prepared": "prepared host tensors through host logits; includes transfers",
                    "resident": "resident GPU forward only",
                },
                "warmups": args.warmups,
                "samples": args.samples,
                "measurements": results,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
