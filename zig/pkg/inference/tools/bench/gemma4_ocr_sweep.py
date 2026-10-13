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
    cohort = re.fullmatch(r"cohort([24])w(4|8|16)", case)
    if cohort or case in {"normoff", "ffnoff", "differential"}:
        batch = int(cohort[2]) if cohort else 8
        flags.update(
            ANTFLY_GEMMA4_METAL_OCR_BATCH="1",
            ANTFLY_GEMMA4_METAL_OCR_CONTINUOUS="1",
            ANTFLY_GEMMA4_METAL_OCR_BATCH_ROWS=cohort[1] if cohort else "4",
            ANTFLY_GEMMA4_METAL_OCR_WINDOW=str(batch),
        )
        if case == "normoff":
            flags["TERMITE_METAL_DISABLE_SMALL_ROWS_NORM_REDUCE"] = "1"
        if case == "ffnoff":
            flags["TERMITE_METAL_DISABLE_GEMMA4_BATCHED_Q4_FFN"] = "1"
        if case == "differential":
            flags["ANTFLY_GEMMA4_METAL_OCR_DIFFERENTIAL"] = "1"
    elif re.fullmatch(r"mtp[1-4](frame)?", case):
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
        if case.endswith("frame"):
            flags["TERMITE_METAL_ENABLE_GEMMA4_MTP_HEAD_FRAME"] = "1"
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
    result = {}
    for label, key in [
        ("mtp_phase_profile:", "draft_steps"),
        ("mtp_draft_profile:", "resident_steps"),
        ("metal_ocr_matrix_frame:", "rows"),
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
        elif "ocr-parity-choice " in line:
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
        "corpus": str(args.corpus.resolve()),
        "reverse_rows": args.reverse,
        "recognition_scope": "image prep/prefill/decode/HTTP; PDF rendering/server startup excluded",
        "memory_scope": "sampled process RSS, not total unified GPU memory",
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
        entry = {
            "case": case,
            "batch": batch,
            "server_flags": flags,
            "server_command": command,
            "diagnostic": case == "differential",
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
                        rss = subprocess.check_output(
                            ["ps", "-o", "rss=", "-p", str(server.pid)], text=True
                        ).strip()
                        if rss:
                            samples.append(int(rss) * 1024)
                    except (subprocess.CalledProcessError, ValueError):
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
        report = json.loads(
            (directory / ("gemma4-b" + str(batch) + ".json")).read_text()
        )
        if reference is None and case == "ordinary":
            reference = report
        entry.update(
            seconds=report["totals"],
            sampled_peak_rss_bytes=max(samples, default=None),
            profile=profile((directory / "server.log").read_text()),
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
        "--reverse",
        action="store_true",
        help="Reverse the page order without changing inputs",
    )
    run(parser.parse_args())
