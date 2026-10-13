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

"""Source-pinned OCR cohort/MTP sweeps and canonical layer diagnostics.

Use a fresh server for each case. Timings exclude PDF rendering/server startup.
Diagnostic output is never eligible for throughput qualification. RSS is host
process resident memory, not total unified GPU memory or admission capacity.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import statistics
import subprocess
import sys
import threading
import time

DEFAULT_CASES = [
    "ordinary",
    "cohort2w4",
    "cohort4w4",
    "cohort4w8",
    "cohort4w16",
    "ordinary",
    "mtp1",
    "mtp2",
    "mtp3",
    "mtp4",
    "mtp2frame",
    "ordinary",
]


def flags_for(case):
    flags = {
        "TERMITE_SERVER_GENERATE_TIMING": "1",
        "ANTFLY_GEMMA4_OCR_PROFILE": "1",
        "ANTFLY_GEMMA4_MTP_PROFILE": "1",
        "ANTFLY_GEMMA4_METAL_BATCHED_Q4_FFN": "1",
        "ANTFLY_GEMMA4_METAL_OCR_MULTIROW": "1",
        "TERMITE_METAL_ENABLE_DONATED_SLOT_ATTENTION_ON_FRAME": "1",
        "TERMITE_METAL_ENABLE_Q4_0_SHORT_ROWS_SPECIALIZED": "0",
        "ANTFLY_GEMMA4_MTP_SEED_CACHED_TARGET": "0",
        "ANTFLY_GEMMA4_MTP_ADAPTIVE_K": "0",
    }
    batch = 1
    extra = []
    cohort = re.fullmatch(
        r"cohort([24])w(4|8|16)(qkv|tail|ple|canonical|reduce|reduceall|attn|block|strict|trace|qkvtrace|stable|invariant|invariantprofile|share|tails|pool|fused|reuse|optimized|optimizedprofile)?",
        case,
    )
    if cohort or case in {
        "normoff",
        "ffnoff",
        "differential",
        "stepdiff",
        "qkvoff",
        "pleoff",
        "tailoff",
        "canonical",
        "reduce",
        "reduceall",
    }:
        batch = int(cohort[2]) if cohort else 8
        flags.update(
            ANTFLY_GEMMA4_METAL_OCR_BATCH="1",
            ANTFLY_GEMMA4_METAL_OCR_CONTINUOUS="1",
            ANTFLY_GEMMA4_METAL_OCR_BATCH_ROWS=cohort[1] if cohort else "4",
            ANTFLY_GEMMA4_METAL_OCR_WINDOW=str(batch),
        )
        if case == "normoff":
            flags["TERMITE_METAL_DISABLE_SMALL_ROWS_NORM_REDUCE"] = "1"
        variant = cohort[3] if cohort else None
        flags["ANTFLY_GEMMA4_METAL_OCR_MATRIX_ARITHMETIC"] = (
            "0" if variant == "stable" else "1"
        )
        if variant in {
            "invariant",
            "invariantprofile",
            "share",
            "tails",
            "pool",
            "fused",
            "reuse",
            "optimized",
            "optimizedprofile",
        }:
            flags["TERMITE_METAL_Q4_0_BATCH_INVARIANT"] = "1"
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_QKV"] = "1"
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_PLE"] = "1"
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_TAIL"] = "1"
        if variant in {"invariantprofile", "optimizedprofile"}:
            flags["TERMITE_METAL_STAGE_TIMING"] = "1"
            flags["TERMITE_METAL_STAGE_TIMING_ROOFLINE"] = "0"
        if variant in {"share", "fused", "reuse", "optimized", "optimizedprofile"}:
            flags["TERMITE_METAL_Q4_0_BATCH_SHARED"] = "1"
        if variant in {
            "tails",
            "pool",
            "fused",
            "reuse",
            "optimized",
            "optimizedprofile",
        }:
            flags["TERMITE_METAL_Q4_0_BATCH_FUSED_TAILS"] = "1"
            flags["ANTFLY_GEMMA4_METAL_OCR_BATCH_QKV"] = "1"
        if variant in {"pool", "reuse", "optimized", "optimizedprofile"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_REUSE_CARRIERS"] = "1"
        if variant in {"qkv", "canonical", "strict", "qkvtrace"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_QKV"] = "1"
        if variant in {"ple", "canonical", "reduceall", "strict"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_PLE"] = "1"
        if variant in {"tail", "canonical", "reduceall", "strict"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_TAIL"] = "1"
        if variant in {"canonical", "reduce", "reduceall"}:
            flags["TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN"] = "1"
        if variant in {"reduce", "reduceall"}:
            flags["TERMITE_METAL_DISABLE_Q4_0_SMALL_BATCH"] = "1"
            flags["TERMITE_METAL_DISABLE_ANTFLY_Q4_0_SMALL_BATCH"] = "1"
        if variant == "attn":
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_ATTN_OUTPUT"] = "1"
        if variant in {"block", "strict"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_BLOCK"] = "1"
        if variant in {"trace", "qkvtrace"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_TRACE_CHOICES"] = "1"
        if case in {"reduce", "reduceall"}:
            flags["TERMITE_METAL_DISABLE_Q4_0_SMALL_BATCH"] = "1"
            flags["TERMITE_METAL_DISABLE_ANTFLY_Q4_0_SMALL_BATCH"] = "1"
            flags["TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN"] = "1"
        if case in {"qkvoff", "canonical"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_QKV"] = "1"
        if case in {"pleoff", "canonical", "reduceall"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_PLE"] = "1"
        if case in {"tailoff", "canonical", "reduceall"}:
            flags["ANTFLY_GEMMA4_METAL_OCR_CANONICAL_TAIL"] = "1"
        if case in {"ffnoff", "canonical"}:
            flags["TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN"] = "1"
        if case == "differential":
            flags["ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL"] = "1"
        if case == "stepdiff":
            flags["ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL_STEP"] = "1"
    elif case == "singletontrace":
        flags["ANTFLY_GEMMA4_METAL_OCR_TRACE_CHOICES"] = "1"
    elif re.fullmatch(r"mtp[1-4](frame|verify|all|seedreduce)?", case):
        flags.update(
            ANTFLY_GEMMA4_MTP_MULTIMODAL="1",
            ANTFLY_GEMMA4_MTP_ACCEPT_BONUS="1",
            ANTFLY_GEMMA4_MTP_LONG_CONTEXT_FRAME="1",
            ANTFLY_GEMMA4_MTP_ENABLE_MASKED_EMBEDDING="1",
            TERMITE_METAL_ENABLE_GEMMA4_MTP_CLUSTER_HEAD="1",
            TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_REPLAY="1",
            ANTFLY_GEMMA4_MTP_RECOVERY_CHOICE_ONLY="1",
            TERMITE_METAL_ENABLE_GEMMA4_DECODE_SCRATCH_ENVELOPE="1",
        )
        if case.endswith("seedreduce"):
            # Recheck the rejected seed schedule only with target Q4 matrix
            # reductions/FFN rolled back. Complete-output gates still apply.
            flags["ANTFLY_GEMMA4_MTP_SEED_CACHED_TARGET"] = "1"
            flags["TERMITE_METAL_DISABLE_Q4_0_SMALL_BATCH"] = "1"
            flags["TERMITE_METAL_DISABLE_ANTFLY_Q4_0_SMALL_BATCH"] = "1"
            flags["TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN"] = "1"
            flags["TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME"] = "1"
        if case.endswith(("frame", "all")):
            flags["TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME"] = "1"
        if case.endswith(("verify", "all")):
            flags["TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_INPUT_FRAME"] = "1"
            flags["TERMITE_METAL_ENABLE_GEMMA4_MTP_VERIFY_TAIL_FRAME"] = "1"
        extra = ["--draft-model", "assistant", "--speculative-k", case[3]]
    elif case != "ordinary":
        raise ValueError("Unknown case: " + case)
    return flags, batch, extra


def outputs(report):
    return [x for wave in report["waves"] for x in wave["outputs"]]


def qualification(report, reference):
    refs = {}
    for x in outputs(reference):
        refs.setdefault((x["document"], x["page"]), x)
    changed = []
    missing = []
    counts = []
    for iteration in range(report["iterations"]):
        xs = [
            x
            for wave in report["waves"]
            if wave["iteration"] == iteration
            for x in wave["outputs"]
        ]
        seen = set()
        counts.append(sum(x["usage"]["completion_tokens"] for x in xs))
        for x in xs:
            key = (x["document"], x["page"])
            seen.add(key)
            ref = refs.get(key)
            if (
                ref is None
                or x["text"] != ref["text"]
                or x["usage"]["completion_tokens"] != ref["usage"]["completion_tokens"]
            ):
                changed.append(
                    {
                        "iteration": iteration,
                        "document": key[0],
                        "page": key[1],
                        "completion_tokens": x["usage"]["completion_tokens"],
                    }
                )
        missing.extend(
            {"iteration": iteration, "document": key[0], "page": key[1]}
            for key in refs.keys() - seen
        )
    truncated = sum(x["finish_reason"] == "length" for x in outputs(report))
    return {
        "completion_tokens_per_pass": counts,
        "changed_pages": changed,
        "missing_pages": missing,
        "truncated_at_cap": truncated,
        "passes_output_gate": not changed and not missing and not truncated,
    }


def profile(log):
    # Server preload performs a dummy generation before accepting traffic.
    # Keep that out of request latency/work counters; explicit warmups are
    # removed by run() afterward.
    if "listening on 127.0.0.1:" in log:
        log = log.split("listening on 127.0.0.1:", 1)[1]
    result = {}
    for label, key in [
        ("generate_timing_ms:", "total"),
        ("metal_ocr_profile:", "decode_hits"),
        ("metal_ocr_mtp_profile:", "rounds"),
        ("mtp_phase_profile:", "draft_steps"),
        ("mtp_draft_profile:", "resident_steps"),
        ("metal_ocr_matrix_frame:", "rows"),
        ("metal_ocr_gpu_stages:", "frames"),
        ("metal_ocr_cohort_overhead:", "rows"),
    ]:
        rows = []
        for line in log.splitlines():
            if label in line:
                values = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", line)}
                if values.get(key):
                    rows.append(values)
        result[label.rstrip(":")] = rows
    parity = []
    choices = []
    for line in log.splitlines():
        if "ocr-parity stage=" in line:
            parity.append(dict(re.findall(r"(\w+)=([^\s]+)", line)))
        elif "ocr-parity-choice " in line or "ocr-parity-step " in line:
            choices.append(dict(re.findall(r"(\w+)=([^\s]+)", line)))
    result["parity"] = {
        "stage_samples": len(parity),
        "nonfinite_samples": sum(int(x["nonfinite"]) > 0 for x in parity),
        "first_nonidentical_stage": next(
            (x for x in parity if x["matrix_hash"] != x["canonical_hash"]), None
        ),
        "choice_samples": len(choices),
        "changed_choices": [x for x in choices if x["match"] != "true"],
    }
    return result


def process_tree_rss(ps_rows, root_pid):
    rows = []
    for line in ps_rows.splitlines():
        fields = line.split()
        if len(fields) == 3:
            try:
                rows.append(tuple(int(x) for x in fields))
            except ValueError:
                continue
    owned = {root_pid}
    while True:
        children = {pid for pid, parent, _ in rows if parent in owned}
        expanded = owned | children
        if expanded == owned:
            break
        owned = expanded
    return sum(rss * 1024 for pid, _, rss in rows if pid in owned)


def recommend(runs, complete=True):
    eligible = [
        r
        for r in runs
        if r.get("eligible_for_throughput_comparison") and len(r["seconds"]) >= 2
    ]
    controls = [
        value
        for r in eligible
        if r["case"] == "ordinary"
        for value in r["seconds_per_committed_token"]
    ]
    if not eligible:
        return {
            "qualified_speedup": False,
            "reason": "No repeated complete-output eligible cases",
        }
    best = min(
        eligible, key=lambda r: statistics.median(r["seconds_per_committed_token"])
    )
    values = best["seconds_per_committed_token"]
    ordinary_indices = [
        i for i, r in enumerate(runs) if r in eligible and r["case"] == "ordinary"
    ]
    best_index = runs.index(best)
    bracketed = len(ordinary_indices) >= 2 and min(ordinary_indices) < best_index < max(
        ordinary_indices
    )
    return {
        "measured_fastest_case": best["case"],
        "median_seconds_per_committed_token": statistics.median(values),
        "qualified_speedup": bool(
            complete
            and bracketed
            and controls
            and best["case"] != "ordinary"
            and max(values) < min(controls)
        ),
        "scope": "This campaign only; passing transcripts and disjoint repeated ranges against all ordinary controls. Shared-machine timing still needs independent replication.",
    }


def run(args):
    args.output_dir.mkdir(parents=True, exist_ok=False)
    corpus = json.loads(args.corpus.read_text())
    if args.reverse:
        corpus["pages"].reverse()
    corpus_path = args.output_dir / "corpus.json"
    corpus_path.write_text(json.dumps(corpus, indent=2) + "\n")
    reference = json.loads(args.reference.read_text()) if args.reference else None
    binary_hash = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    summary = {
        "runtime_source_revision": args.source_revision,
        "binary_sha256": binary_hash,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "corpus": str(args.corpus.resolve()),
        "reverse_rows": args.reverse,
        "recognition_scope": "image prep/prefill/decode/HTTP; PDF rendering/server startup excluded",
        "memory_scope": "sum of sampled server/worker process-tree RSS; shared mappings may be double-counted; not total unified GPU memory",
        "memory_probe_sha256": hashlib.sha256(
            args.memory_probe.read_bytes()
        ).hexdigest()
        if args.memory_probe
        else None,
        "runs": [],
    }
    for index, case in enumerate(args.cases):
        flags, batch, extra = flags_for(case)
        directory = args.output_dir / (str(index) + "-" + case)
        directory.mkdir()
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.startswith(
                ("ANTFLY_GEMMA4_", "TERMITE_METAL_", "TERMITE_SERVER_GENERATE_TIMING")
            )
        }
        env.update(flags)
        if args.memory_probe:
            env["DYLD_INSERT_LIBRARIES"] = str(args.memory_probe.resolve())
        command = [
            str(args.binary.resolve()),
            "run",
            "--port",
            str(args.port),
            "--models-dir",
            str(args.models_dir.resolve()),
            "--max-concurrent-requests",
            "512",
            "--host-budget-mb",
            "8192",
            "--backend-budget-mb",
            "16384",
            "--combined-budget-mb",
            "24576",
            "--scratch-budget-mb",
            "4096",
            "--kv-budget-mb",
            "8192",
            "--preload-model",
            "generator:metal:" + args.model,
        ]
        if args.http_max_body_mb is not None:
            command.extend(["--http-max-body-mb", str(args.http_max_body_mb)])
        entry = {
            "case": case,
            "http_max_body_mb": args.http_max_body_mb
            if args.http_max_body_mb is not None
            else 10,
            "batch": batch,
            "server_flags": flags,
            "server_command": command,
            "diagnostic": case in {"differential", "stepdiff"}
            or case.endswith(("trace", "profile"))
            or args.memory_probe is not None,
            "directory": directory.name,
        }
        samples = []
        stop = threading.Event()
        with (directory / "server.log").open("w") as log:
            server = subprocess.Popen(
                command, env=env, stdout=log, stderr=subprocess.STDOUT
            )

            def sample_rss():
                while not stop.wait(0.5):
                    try:
                        table = subprocess.check_output(
                            ["ps", "-axo", "pid=,ppid=,rss="], text=True
                        )
                        samples.append(process_tree_rss(table, server.pid))
                    except (OSError, subprocess.CalledProcessError, ValueError):
                        return

            sampler = None
            try:
                deadline = time.monotonic() + 180
                while True:
                    if server.poll() is not None:
                        raise RuntimeError(
                            "Server exited: " + str(directory / "server.log")
                        )
                    try:
                        with socket.create_connection(
                            ("127.0.0.1", args.port), timeout=1
                        ):
                            if (
                                "listening on 127.0.0.1:"
                                in (directory / "server.log").read_text()
                            ):
                                break
                    except OSError:
                        pass
                    if time.monotonic() > deadline:
                        raise TimeoutError("Server did not preload")
                    time.sleep(0.25)
                sampler = threading.Thread(target=sample_rss, daemon=True)
                sampler.start()
                bench = [
                    sys.executable,
                    str(Path(__file__).with_name("pdf_ocr.py")),
                    "--base-url",
                    "http://127.0.0.1:" + str(args.port),
                    "--corpus",
                    str(corpus_path),
                    "--model",
                    args.model,
                    "--output-dir",
                    str(directory),
                    "--batches",
                    str(batch),
                    "--iterations",
                    str(args.iterations),
                    "--warmups",
                    str(args.warmups),
                    "--max-tokens",
                    str(args.max_tokens),
                ] + extra
                entry["benchmark_command"] = bench
                subprocess.run(bench, check=True)
            except Exception as error:
                entry["error"] = {"type": type(error).__name__, "message": str(error)}
                entry["eligible_for_throughput_comparison"] = False
                summary["runs"].append(entry)
                summary["completed"] = index == len(args.cases) - 1
                summary["recommendation"] = recommend(
                    summary["runs"], summary["completed"]
                )
                (args.output_dir / "summary.json").write_text(
                    json.dumps(summary, indent=2) + "\n"
                )
                if not args.keep_going:
                    raise
            finally:
                stop.set()
                if sampler:
                    sampler.join(timeout=2)
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
        if "error" in entry:
            print(case, "FAILED", entry["error"], flush=True)
            continue
        report = json.loads(
            (directory / ("gemma4-b" + str(batch) + ".json")).read_text()
        )
        if reference is None and case == "ordinary":
            reference = report
        entry.update(
            seconds=report["totals"],
            sampled_peak_process_tree_rss_bytes=max(samples, default=None),
            profile=profile((directory / "server.log").read_text()),
        )
        # Canonical cohorts intentionally have no matrix-frame samples. Record
        # scheduler occupancy separately so their real batching remains visible.
        cohort_log = (directory / "server.log").read_text()
        if "listening on 127.0.0.1:" in cohort_log:
            cohort_log = cohort_log.split("listening on 127.0.0.1:", 1)[1]
        entry["native_cohort_rows_counts"] = {
            str(rows): len(matches)
            for rows in range(1, 5)
            if (
                matches := re.findall(
                    r"metal_ocr_native_decode_rows=" + str(rows) + r"\b",
                    cohort_log,
                )
            )
        }
        entry["native_cohort_rows_counts_scope"] = (
            "Serving scheduler occupancy after preload; may include explicit warmups. Not committed token counts."
        )
        # Request profiles contain the singleton warm pages; strip them before
        # computing decode work/rates. Matrix frame samples have no such warm.
        for name in [
            "generate_timing_ms",
            "metal_ocr_profile",
            "metal_ocr_mtp_profile",
            "mtp_phase_profile",
            "mtp_draft_profile",
        ]:
            entry["profile"][name] = entry["profile"][name][args.warmups :]
        durations = sorted(x["total"] for x in entry["profile"]["generate_timing_ms"])
        entry["generation_pipeline_wall_ms"] = {
            "samples": len(durations),
            "p50": statistics.median(durations) if durations else None,
            "p95": durations[max(0, (len(durations) * 95 + 99) // 100 - 1)]
            if durations
            else None,
            "max": max(durations, default=None),
            "scope": "Native generation pipeline, including phase waits; excludes queueing before pipeline start and HTTP batch return. Per-page provider counter deltas may overlap and must not be summed.",
        }
        entry["http_wave_seconds"] = [wave["seconds"] for wave in report["waves"]]
        mtp = entry["profile"]["metal_ocr_mtp_profile"]
        drafted = sum(x["drafted"] for x in mtp)
        entry["mtp_work"] = {
            "rounds": sum(x["rounds"] for x in mtp),
            "drafted": drafted,
            "matched": sum(x["matched"] for x in mtp),
            "acceptance_rate": sum(x["matched"] for x in mtp) / drafted
            if drafted
            else None,
            "phase_wall_ns": {
                key: sum(x.get(key, 0) for x in entry["profile"]["mtp_phase_profile"])
                for key in ["draft_ns", "verify_ns", "materialize_ns"]
            },
        }
        if args.memory_probe:
            probe_rows = []
            for line in (directory / "server.log").read_text().splitlines():
                if "metal-memory-probe " in line:
                    probe_rows.append(
                        {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", line)}
                    )
            entry["sampled_max_metal_allocated_bytes"] = max(
                (x["bytes"] for x in probe_rows), default=None
            )
            entry["metal_memory_samples"] = len(probe_rows)
            entry["metal_memory_scope"] = (
                "MTLDevice currentAllocatedSize sampled after command-buffer waits in the owning inference process; sampled maximum, not a transient peak. Instrumented timings excluded."
            )
        if reference:
            entry.update(qualification(report, reference))
            entry["eligible_for_throughput_comparison"] = (
                entry["passes_output_gate"] and not entry["diagnostic"]
            )
            entry["seconds_per_committed_token"] = [
                seconds / tokens if tokens else None
                for seconds, tokens in zip(
                    entry["seconds"], entry["completion_tokens_per_pass"]
                )
            ]
        summary["runs"].append(entry)
        summary["completed"] = index == len(args.cases) - 1
        summary["recommendation"] = recommend(summary["runs"], summary["completed"])
        (args.output_dir / "summary.json").write_text(
            json.dumps(summary, indent=2) + "\n"
        )
        print(case, entry["seconds"], entry.get("passes_output_gate"), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--source-revision",
        required=True,
        help="Revision used to compile --binary; never inferred from current HEAD",
    )
    parser.add_argument("--models-dir", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cases", nargs="+", default=DEFAULT_CASES)
    parser.add_argument("--iterations", type=int, default=2)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--max-tokens", type=int, default=2048)
    parser.add_argument("--port", type=int, default=18097)
    parser.add_argument(
        "--http-max-body-mb",
        type=int,
        help="Explicit encoded media payload limit; requires a server with this run option",
    )
    parser.add_argument(
        "--reverse",
        action="store_true",
        help="Reverse the page order without changing inputs",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Record failed cases and continue; failed/partial cases never qualify",
    )
    parser.add_argument(
        "--memory-probe",
        type=Path,
        help="Inject the optional local Metal allocation probe; instrumentation makes timings diagnostic only",
    )
    run(parser.parse_args())
