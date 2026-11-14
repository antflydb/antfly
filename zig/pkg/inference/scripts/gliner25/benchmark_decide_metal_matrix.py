#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Offline Decide/Metal investigation with identical inputs and fresh-process pairs.

FP32 checkpoint, model-scoped Metal arithmetic, loaded public batch APIs including
preprocessing and decoding. Each report records the effective Metal policy. This is a
performance investigation, not production serving or model-quality qualification.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def sha(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def model_identity(model):
    names = ("model.safetensors",) if (model / "model.safetensors").exists() else (
        "gliner2-encoder.Q8_0.gguf", "gliner2-head.Q8_0.gguf")
    return {name: dict(sha256=sha(model / name), bytes=(model / name).stat().st_size)
            for name in names}


def sidecar_identity(model):
    return {name: sha(model / name) for name in ("config.json", "encoder_config/config.json",
        "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json")}


def source_diff(cwd=None):
    """Keep source identity stable when unchanged files are staged for commit."""
    return subprocess.check_output(["git", "diff", "HEAD"], cwd=cwd, text=True)


def activate(upstream):
    sys.path.insert(0, str(Path(upstream).resolve()))


def fixture(args):
    activate(args.upstream)
    from transformers import AutoTokenizer
    from gliner2.processor import SchemaTransformer
    from gliner2.inference.schema import Schema

    processor = SchemaTransformer(tokenizer=AutoTokenizer.from_pretrained(
        args.model_dir, local_files_only=True))
    tasks = {"sentiment": ["positive", "negative", "neutral"],
             "service": ["excellent", "poor", "average"]}
    schema = Schema()
    for name, labels in tasks.items():
        schema.classification(name, labels)
    built = schema.build()

    def ids(text):
        b = processor.collate_fn_inference([(text, built)], max_len=512,
                                           architecture="span", error_policy="raise")
        return b.input_ids[0].tolist()

    starts = [
        "The hotel room was spotless and quiet. The friendly staff helped with our bags.",
        "Our hotel room was dirty and noisy. The receptionist refused to help us.",
        "The hotel was adequate for one night. The room and service were average.",
        "Breakfast was cold but the hotel staff quickly arranged a fresh meal.",
    ]
    cases = []
    for workload in ("short64", "review128", "long384", "mixed64_384"):
        for batch in (1, 8, 32):
            texts, tokens = [], []
            for i in range(batch):
                target = {"short64": 64, "review128": 128, "long384": 384,
                          "mixed64_384": (64, 128, 256, 384)[i % 4]}[workload]
                base = (f"Guest {i + 1}: " + starts[i % len(starts)]).rstrip(".")
                text = base + " room" * (target - len(ids(base + "."))) + "."
                actual = ids(text)
                if len(actual) != target:
                    raise ValueError((workload, i, target, len(actual)))
                texts.append(text)
                tokens.append(actual)
            assert len(set(texts)) == batch
            cases.append(dict(id=f"{workload}_b{batch}", texts=texts, tasks=tasks))
    write(args.output, dict(format_version=1, cases=cases))


def python_benchmark(args):
    activate(args.upstream)
    import torch
    import gliner2
    from gliner2 import AutoExtractor
    from gliner2.classification.scoring import ClassificationScorer
    from gliner2.classification.schema import ClassificationSchema
    from gliner2.classification.compiler import compile_schema

    if not torch.backends.mps.is_available():
        raise RuntimeError("Physical MPS unavailable")
    torch.set_num_threads(2)
    torch.set_num_interop_threads(1)
    torch.manual_seed(0)
    case = json.loads(Path(args.cases).read_text())["cases"][args.case]
    start = time.perf_counter()
    model = AutoExtractor.from_pretrained(args.model_dir, local_files_only=True,
        map_location="cpu", use_flashdeberta=False).float().eval().to("mps")
    torch.mps.synchronize()
    load_ms = (time.perf_counter() - start) * 1000
    assert all(p.device.type == "mps" and p.dtype == torch.float32
               for p in model.parameters())
    schema = ClassificationSchema()
    for name, labels in case["tasks"].items():
        schema.single(name, labels)
    compiled = compile_schema(schema)
    scorer = ClassificationScorer(model, device="mps", dtype=torch.float32).eval()
    texts = case["texts"]
    physical_batch_limit = args.python_microbatch or len(texts)
    with torch.inference_mode():
        scores = scorer.batch_score(texts, compiled, batch_size=physical_batch_limit, max_len=512)
        raw = [[s.logit(task, label) for task, labels in case["tasks"].items()
                for label in labels] for s in scores]
        probabilities = [[s.probability(task, label) for task, labels in case["tasks"].items()
                          for label in labels] for s in scores]
        prepared = [model.processor.collate_fn_inference([(text, compiled.build())],
                    max_len=512, architecture="span").input_ids[0].tolist() for text in texts]
        samples = []
        for i in range(args.warmup + args.reps):
            torch.mps.synchronize()
            start = time.perf_counter_ns()
            result = model.batch_classify_text(texts, case["tasks"], batch_size=physical_batch_limit,
                        include_confidence=True, max_len=512)
            torch.mps.synchronize()
            elapsed = (time.perf_counter_ns() - start) / 1e6
            assert len(result) == len(texts)
            if i >= args.warmup:
                samples.append(elapsed)
    for decoded, scores_row in zip(result, scores, strict=True):
        for task, labels in case["tasks"].items():
            expected = max(labels, key=lambda label: scores_row.logit(task, label))
            if decoded[task]["label"] != expected:
                raise RuntimeError("Public Python API and raw scorer winners differ")
    output = dict(implementation="fastino_gliner2_batch_classify_text", backend="mps",
        case_index=args.case, batch_size=len(texts), load_ms=load_ms,
        physical_batch_limit=physical_batch_limit,
        physical_batch_size=min(physical_batch_limit, len(texts)),
        prepared_input_ids=prepared, raw_logits=raw, probabilities=probabilities,
        decoded=result, samples_ms=samples, median_ms=statistics.median(samples),
        p95_ms=sorted(samples)[math.ceil(.95 * len(samples))-1],
        mps_allocated_bytes=torch.mps.current_allocated_memory(),
        mps_driver_allocated_bytes=torch.mps.driver_allocated_memory(),
        runtime={name: importlib.metadata.version(name) for name in
                 ("torch", "transformers", "numpy", "tokenizers", "peft", "safetensors")},
        gliner2_version=gliner2.__version__, gliner2_path=gliner2.__file__)
    write(args.output, output)


class Usage(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [(name, ctypes.c_uint64) for name in
        ("user_time", "system_time", "pkg_idle_wkups", "interrupt_wkups", "pageins",
         "wired_size", "resident_size", "phys_footprint", "proc_start", "proc_exit")]


def memory(pid):
    info = Usage()
    lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    if lib.proc_pid_rusage(pid, 0, ctypes.byref(info)) != 0:
        return None
    return dict(rss_bytes=info.resident_size, footprint_bytes=info.phys_footprint,
                cpu_ns=info.user_time + info.system_time, pageins=info.pageins)


def context():
    result = {}
    for name, command in (("swap", ["sysctl", "vm.swapusage"]),
                          ("thermal", ["pmset", "-g", "therm"]),
                          ("power", ["pmset", "-g", "batt"])):
        r = subprocess.run(command, text=True, capture_output=True)
        result[name] = r.stdout.strip() or r.stderr.strip()
    return result


def run_child(command, folder, env, timeout, max_footprint_bytes):
    folder.mkdir(parents=True, exist_ok=False)
    receipt = dict(command=command, host_before=context(), status="running")
    write(folder / "receipt.json", receipt)
    samples = []
    started = time.monotonic()
    with (folder / "stdout.log").open("w") as out, (folder / "stderr.log").open("w") as err:
        child = subprocess.Popen(command, stdout=out, stderr=err, env=env)
        try:
            while child.poll() is None:
                usage = memory(child.pid)
                if usage:
                    samples.append(dict(seconds=time.monotonic()-started, **usage))
                    if usage["footprint_bytes"] > max_footprint_bytes:
                        receipt["footprint_limit_bytes"] = max_footprint_bytes
                        receipt["resource_limit_exceeded"] = True
                        child.kill()
                        break
                if time.monotonic() - started > timeout:
                    child.kill()
                    receipt["timeout"] = True
                    break
                time.sleep(.2)
        finally:
            receipt["exit_code"] = child.wait()
    receipt.update(status="finished", wall_seconds=time.monotonic()-started,
                   host_after=context(), memory_samples=samples,
                   peak_rss_bytes=max((s["rss_bytes"] for s in samples), default=None),
                   peak_footprint_bytes=max((s["footprint_bytes"] for s in samples), default=None))
    report = folder / "report.json"
    if receipt["exit_code"] == 0 and not report.exists():
        lines = (folder / "stdout.log").read_text().splitlines()
        write(report, json.loads(lines[-1]))
    receipt["passed"] = receipt["exit_code"] == 0 and report.exists()
    write(folder / "receipt.json", receipt)
    return receipt


def compare(antfly, python):
    tokens_equal = antfly["prepared_input_ids"] == python["prepared_input_ids"]
    a = [r["raw_logits"] for r in antfly["decisions"]]
    p = python["raw_logits"]
    if (len(a) != len(p) or any(len(x) != len(y) for x, y in zip(a, p)) or
        any(not math.isfinite(value) for rows in (a, p, python["probabilities"])
            for row in rows for value in row)):
        raise ValueError("Reference and runtime logits must have matching shapes and finite values")
    error = max(abs(x-y) for xs,ys in zip(a,p) for x,y in zip(xs,ys))
    winners_equal = all(
        max(range(r["start"],r["end"]),key=lambda i: x[i]) ==
        max(range(r["start"],r["end"]),key=lambda i: y[i])
        for x,y,d in zip(a,p,antfly["decisions"]) for r in d["task_ranges"])
    probabilities = []
    for logits, decision in zip(a, antfly["decisions"]):
        row = []
        for r in decision["task_ranges"]:
            task = logits[r["start"]:r["end"]]
            exps = [math.exp(x-max(task)) for x in task]
            row.extend(x/sum(exps) for x in exps)
        probabilities.append(row)
    probability_error = max(abs(x-y) for xs,ys in zip(probabilities,python["probabilities"])
                            for x,y in zip(xs,ys))
    decoded_equal = True
    decoded_probability_error = 0
    for decision_index, (decision, reference) in enumerate(zip(antfly["decisions"], python["raw_logits"], strict=True)):
        for task in decision["tasks"]:
            r = decision["task_ranges"][task["task_index"]]
            expected = max(range(r["start"], r["end"]), key=lambda i:reference[i]) - r["start"]
            selections = task["selections"]
            decoded_equal &= len(selections) == 1 and selections[0]["label_index"] == expected
            if len(selections) == 1:
                index = r["start"] + selections[0]["label_index"]
                decoded_probability_error = max(decoded_probability_error,
                    abs(selections[0]["probability"] - python["probabilities"][decision_index][index]))
    return dict(tokens_equal=tokens_equal, max_logit_error=error,
                max_probability_error=probability_error,
                winners_equal=winners_equal, decoded_equal=decoded_equal,
                max_decoded_probability_error=decoded_probability_error,
                passed=tokens_equal and winners_equal and decoded_equal and error <= .002
                    and probability_error <= .0005 and decoded_probability_error <= .0005)


def campaign(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    model = Path(args.model_dir).resolve()
    python_model = Path(args.python_model_dir or model).resolve()
    reference_model = Path(args.reference_model_dir).resolve() if args.reference_model_dir else None
    binary = Path(args.binary).resolve()
    cases_path = Path(args.cases).resolve()
    cases = json.loads(cases_path.read_text())["cases"]
    inherited = {k:v for k,v in os.environ.items() if not k.startswith(
        ("TERMITE_", "ANTFLY_INFERENCE_", "ANTFLY_METAL_", "PYTORCH_MPS_", "PYTORCH_ENABLE_MPS_"))}
    env = {**inherited, "HF_HUB_OFFLINE": "1", "TOKENIZERS_PARALLELISM": "false",
           "PYTORCH_ENABLE_MPS_FALLBACK": "0", "PYTHONDONTWRITEBYTECODE": "1",
           "ANTFLY_INFERENCE_CPU_THREADS": "2", "VECLIB_MAXIMUM_THREADS": "2",
           "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2"}
    for override in args.env:
        name, value = override.split("=", 1)
        env[name] = value
    model_weights = model_identity(model)
    python_weights = model_identity(python_model)
    sidecars = {str(path): sidecar_identity(path) for path in
                (model, python_model, reference_model) if path is not None}
    if any(value != sidecars[str(model)] for value in sidecars.values()):
        raise ValueError("Antfly, Python and reference must use identical configuration/tokenizer sidecars")
    if args.antfly_quantization == "q8_0":
        from prepare_decide_metal_q8_reference import PINS
        if {name: entry["sha256"] for name, entry in model_weights.items()} != PINS:
            raise ValueError("Q8 campaign requires the exact published bundle")
        if python_weights.get("model.safetensors", {}).get("sha256") != "40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997":
            raise ValueError("Q8 timing baseline must use the original Fastino FP32 checkpoint")
        reference_receipt = json.loads((reference_model / "q8_reference.json").read_text())
        reference_weights = model_identity(reference_model)
        if (reference_receipt["source_bundle"] != PINS or
            reference_receipt["sha256"] != reference_weights["model.safetensors"]["sha256"]):
            raise ValueError("Decoded Q8 reference identity changed")
    else:
        reference_weights = None
    identity = dict(model_dir=str(model), model_sha256=next(iter(model_weights.values()))["sha256"],
        model_bytes=sum(entry["bytes"] for entry in model_weights.values()),
        model_weights=model_weights, python_model_dir=str(python_model), python_weights=python_weights,
        antfly_quantization=args.antfly_quantization,
        reference_model_dir=str(reference_model) if reference_model else None,
        reference_weights=reference_weights,
        model_sidecars=sidecars,
        reference_tools={name: sha(Path(__file__).with_name(name)) for name in
            ("prepare_decide_metal_q8_reference.py", "benchmark_decide_quant.py")} if reference_model else {},
        model_files={str(p.relative_to(model)):sha(p) for p in sorted(model.rglob("*.json"))},
        binary=str(binary), binary_sha256=sha(binary), cases_sha256=sha(cases_path),
        script_sha256=sha(__file__), upstream_commit=subprocess.check_output(
            ["git","-C", args.upstream,"rev-parse","HEAD"],text=True).strip(),
        antfly_commit=subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
        antfly_diff=source_diff(),
        python=sys.version, platform=platform.platform(), warmup=args.warmup, reps=args.reps,
        blocks=args.blocks, overrides=args.env, memory_interval_seconds=.2,
        antfly_controlled=args.antfly_controlled, antfly_microbatch=args.antfly_microbatch,
        antfly_batch_tokens=args.antfly_batch_tokens,
        python_microbatch=args.python_microbatch,
        max_footprint_gb=args.max_footprint_gb,
        execution_environment={k:v for k,v in env.items() if k.startswith(
            ("TERMITE_", "ANTFLY_INFERENCE_", "ANTFLY_METAL_", "PYTORCH_", "OMP_", "VECLIB_", "OPENBLAS_"))},
        timing_scope="loaded public batch API: preprocess+encoder+head+decode; no HTTP/socket",
        arithmetic=("Q8_0 encoder with FP32 head and FP32 Metal mirrors; decoded-Q8 parity reference; original FP32 Python timing baseline"
                    if reference_model else "FP32 source checkpoint; Python FP32; Metal policy recorded in each report; binary precision field selects CUDA only"),
        memory_scope="whole fresh process including load; RSS and macOS phys_footprint are separate")
    write(output / "identity.json", identity)
    summary = []
    selected = range(len(cases)) if args.case is None else [args.case]
    for index in selected:
        case = cases[index]
        reference = None
        if reference_model:
            dest = output / f'{case["id"]}-q8-reference'
            command = [sys.executable, str(Path(__file__).resolve()), "python",
                "--upstream", str(Path(args.upstream).resolve()), "--model-dir", str(reference_model),
                "--cases", str(cases_path), "--case", str(index), "--warmup", "0", "--reps", "3",
                "--python-microbatch", str(args.python_microbatch), "--output", str(dest / "report.json")]
            receipt = run_child(command, dest, env, args.timeout, int(args.max_footprint_gb * 1e9))
            if not receipt["passed"]:
                raise RuntimeError(f"Decoded Q8 reference failed: {dest}")
            reference = json.loads((dest / "report.json").read_text())
        pairs = []
        for block in range(args.blocks):
            reports, receipts = {}, {}
            for runtime in (("antfly", "python") if block % 2 == 0 else ("python", "antfly")):
                dest = output / f'{case["id"]}-{block}-{runtime}'
                if runtime == "antfly":
                    command = [str(binary), "--model-dir", str(model), "--backend", "metal",
                        "--cases", str(cases_path), "--case", str(index),
                        "--warmup", str(args.warmup), "--reps", str(args.reps),
                        "--controlled", str(int(args.antfly_controlled)),
                        "--microbatch", str(args.antfly_microbatch),
                        "--batch-tokens", str(args.antfly_batch_tokens)]
                else:
                    command = [sys.executable, str(Path(__file__).resolve()), "python",
                        "--upstream", str(Path(args.upstream).resolve()), "--model-dir", str(python_model),
                        "--cases", str(cases_path), "--case", str(index),
                        "--warmup", str(args.warmup), "--reps", str(args.reps),
                        "--python-microbatch", str(args.python_microbatch),
                        "--output", str(dest / "report.json")]
                print(f'{case["id"]} block={block} {runtime}', flush=True)
                receipts[runtime] = run_child(command, dest, env, args.timeout,
                    int(args.max_footprint_gb * 1e9))
                if not receipts[runtime]["passed"]:
                    print(f"FAILED: {dest}", flush=True)
                    return 1
                reports[runtime] = json.loads((dest / "report.json").read_text())
            parity = compare(reports["antfly"], reference or reports["python"])
            pair = dict(block=block, parity=parity,
                antfly_ms=reports["antfly"]["median_ms"], python_ms=reports["python"]["median_ms"],
                antfly_p95_ms=reports["antfly"]["p95_ms"], python_p95_ms=reports["python"]["p95_ms"],
                **{f'{r}_{k}': receipts[r][f'peak_{k}_bytes'] for r in receipts for k in ("rss","footprint")})
            if reference:
                pair["original_fp32_comparison"] = compare(reports["antfly"], reports["python"])
            pairs.append(pair)
            write(output / f'{case["id"]}-pairs.json', pairs)
            print(json.dumps(pair), flush=True)
        row = dict(id=case["id"], batch=len(case["texts"]), pairs=pairs,
            **{key:statistics.median(p[key] for p in pairs) for key in pairs[0]
               if key not in ("block","parity","original_fp32_comparison")})
        row.update(speedup_python_over_antfly=row["python_ms"]/row["antfly_ms"],
            antfly_items_per_second=1000*row["batch"]/row["antfly_ms"],
            python_items_per_second=1000*row["batch"]/row["python_ms"],
            parity_passed=all(p["parity"]["passed"] for p in pairs))
        summary.append(row)
        write(output / "summary.json", summary)
    if model_identity(model) != model_weights or model_identity(python_model) != python_weights:
        raise RuntimeError("Model weights changed during campaign")
    if reference_model and model_identity(reference_model) != reference_weights:
        raise RuntimeError("Decoded reference weights changed during campaign")
    for name, digest in identity["reference_tools"].items():
        if sha(Path(__file__).with_name(name)) != digest:
            raise RuntimeError("Q8 reference decoder changed during campaign")
    if any(sidecar_identity(Path(path)) != value for path, value in sidecars.items()):
        raise RuntimeError("Configuration or tokenizer changed during campaign")
    write(output / "completion.json", dict(completed=True,
        source_unchanged=source_diff() == identity["antfly_diff"],
        binary_unchanged=sha(binary) == identity["binary_sha256"],
        script_unchanged=sha(__file__) == identity["script_sha256"]))
    if sha(binary) != identity["binary_sha256"]:
        raise RuntimeError("Binary changed during campaign")
    return 0 if all(row["parity_passed"] for row in summary) else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("fixture", "python", "campaign"))
    p.add_argument("--upstream", required=True)
    p.add_argument("--model-dir", required=True)
    p.add_argument("--python-model-dir", help="Original FP32 timing baseline when Antfly loads Q8")
    p.add_argument("--reference-model-dir", help="Decoded Q8 correctness reference, run separately from timed processes")
    p.add_argument("--antfly-quantization", choices=("fp32", "q8_0"), default="fp32")
    p.add_argument("--output", required=True)
    p.add_argument("--binary")
    p.add_argument("--cases")
    p.add_argument("--case", type=int)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--reps", type=int, default=20)
    p.add_argument("--blocks", type=int, default=3)
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--env", action="append", default=[])
    p.add_argument("--antfly-controlled", action="store_true")
    p.add_argument("--antfly-microbatch", type=int, default=64)
    p.add_argument("--antfly-batch-tokens", type=int, default=16384)
    p.add_argument("--python-microbatch", type=int, default=0,
                   help="0 uses the whole logical request; 8 matches Fastino's default")
    p.add_argument("--max-footprint-gb", type=float, default=12)
    args = p.parse_args()
    if not 0 <= args.warmup <= 100 or not 3 <= args.reps <= 1000 or not 1 <= args.blocks <= 6:
        p.error("warmup must be 0..100, reps 3..1000, blocks 1..6")
    if not 0 < args.max_footprint_gb < 100:
        p.error("--max-footprint-gb must be in (0, 100)")
    if not 0 <= args.python_microbatch <= 64:
        p.error("--python-microbatch must be in 0..64")
    if args.mode in ("python", "campaign") and not args.cases:
        p.error("--cases is required")
    if args.mode == "campaign" and not args.binary:
        p.error("--binary is required for a campaign")
    if args.mode == "campaign" and (args.antfly_quantization == "q8_0") != bool(args.reference_model_dir):
        p.error("Q8 campaigns require --reference-model-dir; FP32 campaigns must omit it")
    if args.mode == "python" and args.case is None:
        p.error("--case is required for a Python child")
    return {"fixture":fixture,"python":python_benchmark,"campaign":campaign}[args.mode](args)


if __name__ == "__main__":
    raise SystemExit(main())
