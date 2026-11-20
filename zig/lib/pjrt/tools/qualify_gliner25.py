#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Prepare upstream CPU FP32 oracles, then qualify native CUDA independently."""
import argparse
import importlib.metadata
import json
import os
import statistics
import subprocess
from pathlib import Path

import numpy as np
from benchmark_gliner25 import compare_to_cpu, digest, native_run


def prepare(args):
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    import torch
    from gliner2 import AutoExtractor, Schema
    from prepare_gliner25 import Core, EntityCore, TASKS

    torch.set_float32_matmul_precision("highest")
    torch.set_num_threads(args.threads)
    texts = json.loads(args.texts.read_text())
    if not texts or any(not isinstance(text, str) for text in texts):
        raise ValueError("Text fixtures must be a nonempty JSON array of strings")
    model = AutoExtractor.from_pretrained(str(args.model), local_files_only=True).float().eval()
    model.requires_grad_(False)
    if model.architecture == "boundary" and max(args.batches) > 8:
        raise ValueError("Boundary core runner supports physical batches up to eight")
    entities = args.kind == "entities"
    if entities and model.architecture != "boundary":
        raise ValueError("Entity qualification requires a boundary checkpoint")
    core = (EntityCore(model) if entities else Core(model)).eval()
    schema = Schema.from_dict(
        {"entities": ["person", "location", "organization", "species", "date"]}
        if entities else {"classifications": TASKS}
    ).build()
    cases = []
    for size in args.batches:
        for start in range(0, len(texts), size):
            group = texts[start:start + size]
            batch = model.processor.collate_fn_inference([(text, schema) for text in group], max_len=512)
            if entities:
                tensors = [value.long() for value in (batch.input_ids, batch.attention_mask, batch.text_word_indices, batch.text_word_mask, batch.query_marker_indices, batch.query_marker_mask)]
                names = ("input_ids", "attention_mask", "text_word_indices", "text_word_mask", "query_marker_indices", "query_marker_mask")
                if not tensors[2].shape[1]:
                    raise ValueError("Entity fixtures must contain at least one word")
            else:
                marker = model.processor.tokenizer.convert_tokens_to_ids("[L]")
                positions = [torch.where(row == marker)[0] for row in batch.input_ids]
                if any(row.numel() != 14 for row in positions):
                    raise ValueError("Qualification schema must encode fourteen labels")
                positions = torch.stack(positions)
                tensors = [batch.input_ids.long(), batch.attention_mask.long(), positions, torch.ones_like(positions)]
                names = ("input_ids", "attention_mask", "decision_marker_positions", "decision_marker_mask")
            with torch.inference_mode():
                logits = core(*tensors).flatten().tolist()
            if not np.isfinite(logits).all():
                raise ValueError("CPU oracle returned nonfinite output")
            case = {
                "id": f"{args.kind}_{start}_b{len(group)}_s{tensors[0].shape[1]}",
                "kind": args.kind, "text": group[0], "texts": group,
                "inputs": [{"name": name, "shape": list(value.shape), "dtype": "int64", "data": value.flatten().tolist()} for name, value in zip(names, tensors, strict=True)],
                "outputs": [{"name": "logits", "shape": [len(logits)], "dtype": "float32"}],
                "cpu_logits": logits,
            }
            if entities:
                b, words, queries = len(group), tensors[2].shape[1], tensors[4].shape[1]
                case["candidate_indices_offset"] = b * queries * (2 * (words + 1) + words + 2 + model.boundary_head.settings.candidate_budget)
            cases.append(case)
            print(case["id"], flush=True)
    result = {
        "architecture": "boundary" if model.architecture == "boundary" else "span",
        "precision": "fp32", "oracle": {"device": "cpu", "dtype": "torch.float32"},
        "model_sha256": digest(args.model / "model.safetensors"),
        "model_sidecars": {name: digest(args.model / name) for name in ("config.json", "encoder_config/config.json", "tokenizer.json")},
        "texts_sha256": digest(args.texts),
        "producer_sha256": digest(Path(__file__)),
        "versions": {name: importlib.metadata.version(name) for name in ("torch", "gliner2", "transformers", "numpy")},
        "cases": cases,
    }
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out / "producer.py").write_bytes(Path(__file__).read_bytes())
    (args.out / "core.py").write_bytes(Path(__file__).with_name("prepare_gliner25.py").read_bytes())
    (args.out / "model.json").write_text(json.dumps(result, allow_nan=False) + "\n")


def run(args):
    manifest = json.loads(args.manifest.read_text())
    if manifest.get("precision") != "fp32" or manifest.get("oracle") != {"device": "cpu", "dtype": "torch.float32"}:
        raise ValueError("Qualification requires a recorded CPU FP32 oracle")
    if not manifest.get("cases") or any("cpu_logits" not in case for case in manifest["cases"]):
        raise ValueError("Every case requires CPU FP32 output")
    if digest(args.model / "model.safetensors") != manifest["model_sha256"]:
        raise ValueError("Oracle checkpoint differs")
    for name, sha in manifest["model_sidecars"].items():
        if digest(args.model / name) != sha:
            raise ValueError("Oracle model sidecar differs: " + name)
    artifacts = [args.native, args.control, args.manifest, args.model / "model.safetensors"]
    artifacts += [args.model / name for name in manifest["model_sidecars"]]
    hashes = {str(path.resolve()): digest(path) for path in artifacts}
    args.out.mkdir(parents=True, exist_ok=False)
    gpu = subprocess.check_output(["nvidia-smi", "--query-gpu=name,compute_cap,driver_version", "--format=csv,noheader"], text=True).strip()
    (args.out / "provenance.json").write_text(json.dumps({"hashes": hashes, "gpu": gpu, "native_precision": args.native_precision, "rounds": args.rounds, "iterations": args.iterations, "raw_error_limits": {"control": 0.002, "native": 0.002}}, indent=2) + "\n")
    reports = {"control": [], "native": []}
    for round_index in range(args.rounds):
        for arm in (("control", "native") if round_index % 2 == 0 else ("native", "control")):
            if subprocess.check_output(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True).strip():
                raise RuntimeError("GPU occupied; qualification timing must be isolated")
            output = args.out / f"r{round_index}-{arm}.json"
            report = native_run(args, output, output.with_suffix(".log"), args.control if arm == "control" else args.native, "fp32" if arm == "control" else args.native_precision)
            if not json.loads(output.with_suffix(".memory.json").read_text())["valid"]:
                raise RuntimeError("Qualification memory sampling failed")
            if [case["id"] for case in report["cases"]] != [case["id"] for case in manifest["cases"]]:
                raise ValueError("Qualification workload coverage differs")
            if arm == "native" and args.native_precision == "auto" and report["cuda"].get("gliner25_fast_f32_calls", 0) == 0:
                raise ValueError("Accurate Tensor Core dispatch was not exercised")
            checks = []
            for actual, fixture in zip(report["cases"], manifest["cases"], strict=True):
                if len(actual["logits"]) != args.iterations or len(actual["samples_seconds"]) != args.iterations:
                    raise ValueError("Missing qualification samples")
                if not np.isfinite(actual["samples_seconds"]).all() or min(actual["samples_seconds"]) <= 0:
                    raise ValueError("Invalid qualification latency")
                expected_forwards = (fixture["inputs"][0]["shape"][0] + 7) // 8
                if actual["physical_forwards"] != expected_forwards:
                    raise ValueError("Qualified physical batch limit was not applied")
                checks.append({"id": fixture["id"], "samples": [compare_to_cpu(vector, fixture, 0.002) for vector in actual["logits"]], "median_ms": statistics.median(actual["samples_seconds"]) * 1000})
            reports[arm].append(checks)
            print(round_index, arm, "failures", sum(not sample["passed"] for case in checks for sample in case["samples"]), flush=True)
    if any(digest(Path(path)) != sha for path, sha in hashes.items()):
        raise ValueError("Qualification artifacts changed during the run")
    passed = all(sample["passed"] for arm in reports.values() for round_ in arm for case in round_ for sample in case["samples"])
    (args.out / "summary.json").write_text(json.dumps({"passed": passed, "reports": reports}, indent=2, allow_nan=False) + "\n")
    if not passed:
        raise RuntimeError("Numerical qualification failed; all measurements retained")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("--model", type=Path, required=True)
    prep.add_argument("--texts", type=Path, required=True)
    prep.add_argument("--kind", choices=("classification", "entities"), default="classification")
    prep.add_argument("--batches", type=int, choices=(1, 8, 32), nargs="+", default=[1, 8])
    prep.add_argument("--threads", type=int, choices=range(1, 9), default=2)
    prep.add_argument("--out", type=Path, required=True)
    check = commands.add_parser("run")
    for name in ("native", "control", "model", "manifest", "out"):
        check.add_argument("--" + name, type=Path, required=True)
    check.add_argument("--native-precision", choices=("auto", "bf16", "fp32"), default="auto")
    check.add_argument("--rounds", type=int, choices=range(1, 11), default=3)
    check.add_argument("--iterations", type=int, choices=range(1, 101), default=4)
    args = parser.parse_args()
    (prepare if args.command == "prepare" else run)(args)


if __name__ == "__main__":
    main()
