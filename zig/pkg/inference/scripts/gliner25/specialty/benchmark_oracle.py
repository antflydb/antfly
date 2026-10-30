#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Benchmark frozen-source FP32 upstream with no numerical-capture hooks.

Latency trials and allocation/transfer profiling are separate executions.
Inputs and schemas are shared with capture_oracle.py and the public HTTP gate.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import time

from capture_oracle import FIXTURES, cases, digest_file, verify_model, verify_source


def quantile(values, q):
    ordered = sorted(values)
    index = (len(ordered) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def trace_counters(trace):
    allocations = {"cpu_count": 0, "device_count": 0, "cpu_bytes": 0, "device_bytes": 0}
    transfers = {name: {"count": 0, "bytes": 0} for name in ["h2d", "d2h", "d2d"]}
    gpu_events = 0
    for event in trace.get("traceEvents", []):
        name, category, args = event.get("name", ""), event.get("cat", ""), event.get("args", {})
        if name == "[memory]" and args.get("Bytes", 0) > 0:
            device = "cpu" if args.get("Device Type", 0) == 0 else "device"
            allocations[device + "_count"] += 1
            allocations[device + "_bytes"] += args["Bytes"]
        if category == "gpu_memcpy":
            gpu_events += 1
            direction = next((key for token, key in [("HtoD", "h2d"), ("DtoH", "d2h"), ("DtoD", "d2d")] if token in name), None)
            if direction:
                transfers[direction]["count"] += 1
                transfers[direction]["bytes"] += args.get("bytes", args.get("Bytes", 0))
    return {"allocations": allocations, "transfers": transfers, "gpu_memcpy_trace_events": gpu_events}


def benchmark(args):
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    model_pin = manifest["models"][args.profile]
    verify_model(args.model_dir, model_pin)
    verify_source(args.source_root, manifest["upstream"]["source_sha256"])
    sys.path.insert(0, str(args.source_root.resolve()))
    os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")
    import torch
    import gliner2
    from gliner2 import AutoExtractor, Schema
    if gliner2.__version__ != manifest["upstream"]["package_version"]:
        raise ValueError("unexpected frozen source version")
    versions = {package: importlib.metadata.version(package) for package in manifest["runtime"]["packages"]}
    runtime_matches = all(versions[key].split("+")[0] == value for key, value in manifest["runtime"]["packages"].items())
    torch.set_num_threads(args.threads)
    torch.manual_seed(0)
    torch.use_deterministic_algorithms(True)
    torch.set_float32_matmul_precision("highest")
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    model = AutoExtractor.from_pretrained(str(args.model_dir.resolve()), map_location=args.device).eval()
    physical_batch = min(8, args.batch_size) if args.profile == "gliguard" else 1
    output = []
    selected = [case for case in cases(args.profile) if not args.case or case["id"] in args.case]
    if not selected:
        raise ValueError("no benchmark cases selected")
    for case in selected:
        schema = Schema()
        if args.profile == "pii":
            schema.entities(case["schema"]["entities"])
        else:
            for task in case["schema"]["classifications"]:
                schema.classification(task["name"], task["labels"], multi_label=task.get("multi_label", False),
                                      cls_threshold=task.get("threshold", 0.5))
        prepared = model.processor.collate_fn_inference([(case["text"], schema.build())], max_len=None, error_policy="raise")
        token_count = int(prepared.attention_mask[0].sum().item())
        if token_count > 512:
            raise ValueError("oversized benchmark case")
        texts = [case["text"]] * args.batch_size

        def synchronize():
            if args.device == "cuda":
                torch.cuda.synchronize()
            elif args.device == "mps":
                torch.mps.synchronize()

        def run():
            return model.batch_extract(texts, schema, batch_size=physical_batch,
                threshold=case.get("threshold", 0.5), include_confidence=True, include_spans=True)

        with torch.inference_mode():
            for _ in range(args.warmups):
                run()
            synchronize()
            times = []
            for _ in range(args.iterations):
                synchronize()
                start = time.perf_counter_ns()
                result = run()
                synchronize()
                times.append((time.perf_counter_ns() - start) / 1e6)
                if len(result) != args.batch_size:
                    raise ValueError("public upstream batch changed output count")
            counters = None
            peak = None
            if args.device == "cuda":
                baseline = torch.cuda.memory_allocated()
                torch.cuda.reset_peak_memory_stats()
                before = torch.cuda.memory_stats()
            if args.profile_counters:
                activities = [torch.profiler.ProfilerActivity.CPU]
                if args.device == "cuda":
                    activities.append(torch.profiler.ProfilerActivity.CUDA)
                trace_path = args.output.with_name(args.output.stem + "." + case["id"] + ".trace.json")
                with torch.profiler.profile(activities=activities, profile_memory=True) as profile:
                    run()
                    synchronize()
                profile.export_chrome_trace(str(trace_path))
                counters = trace_counters(json.loads(trace_path.read_text()))
                counters["separate_profiled_requests"] = 1
                if args.device == "cuda" and counters["gpu_memcpy_trace_events"] == 0:
                    counters["transfer_counters_available"] = False
                else:
                    counters["transfer_counters_available"] = True
                if not args.keep_traces:
                    trace_path.unlink()
            elif args.device == "cuda":
                run()
                synchronize()
            if args.device == "cuda":
                after = torch.cuda.memory_stats()
                peak = {"baseline_allocated_bytes": baseline,
                        "peak_allocated_bytes": torch.cuda.max_memory_allocated(),
                        "peak_reserved_bytes": torch.cuda.max_memory_reserved(),
                        "peak_activation_bytes": torch.cuda.max_memory_allocated() - baseline,
                        "allocation_requests": after.get("allocation.all.allocated", 0) - before.get("allocation.all.allocated", 0),
                        "allocation_bytes": after.get("allocated_bytes.all.allocated", 0) - before.get("allocated_bytes.all.allocated", 0)}
        median = statistics.median(times)
        row = {"id": case["id"], "request_items": args.batch_size, "physical_batch_items": physical_batch,
               "prepared_tokens_per_item": token_count, "warmups": args.warmups, "iterations": args.iterations,
               "latency_ms": {"median": median, "p95": quantile(times, .95), "samples": times},
               "items_per_second": args.batch_size * 1000 / median,
               "prepared_tokens_per_second": token_count * args.batch_size * 1000 / median,
               "profile_counters": counters, "cuda_memory": peak}
        output.append(row)
        print(f"{args.profile} {case['id']} b{args.batch_size}: {median:.3f}ms {row['items_per_second']:.2f}items/s", flush=True)
    return {"format_version": 1, "profile": args.profile, "model": model_pin, "upstream": manifest["upstream"],
            "benchmark_script_sha256": digest_file(Path(__file__)),
            "timing_scope": "Public upstream preprocessing, model, decoder and formatting; excludes model load",
            "allocation_counter_scope": "PyTorch allocator events; excludes Python heap allocations",
            "runtime": {"packages": versions, "matches_frozen_oracle_runtime": runtime_matches,
                        "device": args.device, "dtype": "float32", "threads": args.threads,
                        "python": sys.version, "platform": platform.platform(), "torch_config": torch.__config__.show(),
                        "interop_threads": torch.get_num_interop_threads(),
                        "cuda_device": torch.cuda.get_device_name() if args.device == "cuda" else None,
                        "tf32": False, "float32_matmul_precision": torch.get_float32_matmul_precision(),
                        "cudnn_allow_tf32": torch.backends.cudnn.allow_tf32,
                        "cublas_workspace_config": os.environ.get("CUBLAS_WORKSPACE_CONFIG"),
                        "pytorch_jit": os.environ.get("PYTORCH_JIT", "1"), "capture_hooks": False}, "cases": output}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["pii", "gliguard"], required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--device", choices=["cpu", "cuda", "mps"], default="cpu")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--case", action="append")
    parser.add_argument("--profile-counters", action="store_true")
    parser.add_argument("--keep-traces", action="store_true")
    args = parser.parse_args()
    if args.iterations < 1 or args.warmups < 0 or args.batch_size < 1 or args.threads < 1:
        parser.error("iterations, batch size and threads must be positive; warmups must be nonnegative")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = benchmark(args)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
