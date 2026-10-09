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

"""Run exact-artifact GLiNER2.5 service performance fixtures serially on macOS.

The compiled fixtures own correctness, backend, cache and resource assertions.
This supervisor records process-level memory, host context and source identity.
HTTP measurements use in-process handler dispatch; they do not traverse TCP.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import statistics
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[5]
INFERENCE = ROOT / "zig/pkg/inference"
THREAD_VARIABLES = (
    "ANTFLY_INFERENCE_CPU_THREADS",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "BLIS_NUM_THREADS",
)
SOURCE_PATHS = (
    "zig/lib",
    "zig/pkg/inference/src",
    "zig/pkg/inference/build",
    "zig/pkg/inference/scripts/gliner25",
    "zig/pkg/inference/models/gliner2",
    "zig/pkg/inference/testdata/gliner25/family",
)
DIRECT_1B_NAME = "decide_1b-metal-direct-pipeline"
DIRECT_1B_PROFILE = "decide_1b_direct_kernel"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_text(*args: str) -> str:
    result = subprocess.run(args, text=True, capture_output=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else result.stderr.strip()


def host_context() -> dict:
    return {
        "utc": utc_now(),
        "vm_stat": command_text("/usr/bin/vm_stat"),
        "swap": command_text("/usr/sbin/sysctl", "vm.swapusage"),
        "thermal": command_text("/usr/bin/pmset", "-g", "therm"),
        "power": command_text("/usr/bin/pmset", "-g", "batt"),
    }


def source_identity() -> dict:
    diff = subprocess.check_output(
        ["git", "diff", "--binary", "HEAD", "--", *SOURCE_PATHS],
        cwd=ROOT,
    )
    untracked = subprocess.check_output(
        [
            "git",
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            *SOURCE_PATHS,
        ],
        cwd=ROOT,
    )
    files = {
        p.decode(): sha256(ROOT / p.decode())
        for p in sorted(untracked.split(b"\0"))
        if p
    }
    return {
        "head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
        "untracked_files_sha256": files,
        "scope": list(SOURCE_PATHS),
    }


def parse_time(text: str) -> dict:
    fields = {
        "maximum resident set size": "whole_process_max_rss_bytes",
        "peak memory footprint": "whole_process_peak_footprint_bytes",
        "page reclaims": "page_reclaims",
        "page faults": "page_faults",
        "swaps": "swaps",
        "voluntary context switches": "voluntary_context_switches",
        "involuntary context switches": "involuntary_context_switches",
    }
    parsed = {}
    for label, key in fields.items():
        match = re.search(
            r"^\s*(\d+)\s+" + re.escape(label) + r"\s*$", text, re.MULTILINE
        )
        if match:
            parsed[key] = int(match.group(1))
    if "whole_process_max_rss_bytes" not in parsed:
        raise ValueError("macOS time output lacks maximum resident set size")
    return parsed


def validate_reports(
    directory: Path,
    profile: str,
    backend: str,
    task: str,
    head: str,
    threads: int = 2,
    source_diff_sha256: str | None = None,
    binary_sha256: str | None = None,
) -> list[str]:
    direct_pipeline = task == "direct_pipeline"
    if direct_pipeline and (
        profile != DIRECT_1B_PROFILE
        or backend != "metal"
        or any(
            not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in (source_diff_sha256, binary_sha256)
        )
    ):
        raise ValueError(
            "direct pipeline requires Metal, its diagnostic profile, and explicit source/binary SHA-256 receipts"
        )
    case_ids = (
        ("spanish_entities",)
        if task == "extract"
        else ("described_prompt_choice", "choice_score_noul")
    )
    paths = ("direct_kernel",) if direct_pipeline else ("direct", "http_handler")
    expected = {(case, path) for case in case_ids for path in paths}
    contract_profile = "decide_1b" if direct_pipeline else profile
    contract = json.loads((Path(__file__).parent / "family_contract.json").read_text())[
        "models"
    ][contract_profile]
    capture_name = (
        "decide_1b_capture.json"
        if contract_profile == "decide_1b"
        else f"{profile}_{'decide_' if task == 'decide' else ''}capture.json"
    )
    capture_path = INFERENCE / "testdata/gliner25/family" / capture_name
    capture_pin = sha256(capture_path)
    capture = json.loads(capture_path.read_text())
    requests = {
        row["id"]: row
        for row in capture[
            "public_decide_requests" if contract_profile == "decide_1b" else "requests"
        ]
    }
    expected_scope = (
        "validated_direct_loaded_session_pipeline_latency"
        if direct_pipeline
        else "validated_test_fixture_latency"
    )
    expected_preflights = {"direct": 1, "http_handler": 0 if direct_pipeline else 1}
    direct_tokens = {"described_prompt_choice": 87, "choice_score_noul": 158}
    seen = set()
    files = sorted(directory.glob("*.json"))
    for file in files:
        report = json.loads(file.read_text())
        key = (report["case_id"], report["path"])
        if key not in expected or key in seen:
            raise ValueError(f"unexpected or duplicate request/path in {file.name}")
        seen.add(key)
        samples = report["samples_ns"]
        if (
            report["schema"] != "antfly.gliner25_family_service_perf.v1"
            or report["qualification"] is not False
            or report["profile"] != profile
            or report["backend"] != backend
            or report["source_head"] != head
            or report["build_mode"] != "fast"
            or report["warmups"] != 3
            or report["measured_samples"] != 20
            or len(samples) != 20
            or any(type(value) is not int or value <= 0 for value in samples)
            or report["prepared_tokens"] <= 0
            or report["input_bytes"] <= 0
            or report["model"]["sha256"] != contract["model_sha256"]
            or report["model"]["revision"] != contract["revision"]
            or report["model"]["size_bytes"] != contract["model_size_bytes"]
            or report["model"]["sidecars"] != contract["sidecars"]
            or report["capture_sha256"] != capture_pin
            or report["prepared_tokens"]
            != len(requests[report["case_id"]]["encoded"]["input_ids"])
            or report["input_bytes"]
            != len(requests[report["case_id"]]["text"].encode())
            or report["runtime_cpu_thread_budget"] != threads
            or report["sync_pool_parallelism"] is not False
            or report["fixture_allocator"] != "std.testing.allocator"
            or "smp_allocator" not in report["production_allocator"]
            or report["measurement_scope"] != expected_scope
            or report["validation_preflights"] != expected_preflights
            or report["warmups_follow_validation_preflight"] is not True
        ):
            raise ValueError(f"invalid provenance or sample inventory in {file.name}")
        if direct_pipeline and (
            report.get("source_diff_sha256") != source_diff_sha256
            or report.get("binary_sha256") != binary_sha256
            or report["prepared_tokens"] != direct_tokens[report["case_id"]]
            or report["cold_first_direct_ns"] is not None
        ):
            raise ValueError(f"invalid direct-pipeline receipt in {file.name}")
        memory = report["memory"]
        if any(
            type(memory[name]) is not int or memory[name] <= 0
            for name in ("process_footprint_bytes", "process_rss_bytes")
        ):
            raise ValueError(f"missing process memory snapshot in {file.name}")
        ledger_fields = {
            "host_weight_bytes",
            "backend_weight_bytes",
            "host_kv_bytes",
            "backend_kv_bytes",
            "host_scratch_bytes",
            "backend_scratch_bytes",
        }
        ledger = memory["idle_ledger"]
        if set(ledger) != ledger_fields or any(
            type(value) is not int or value < 0 for value in ledger.values()
        ):
            raise ValueError(f"invalid idle admission ledger in {file.name}")
        owners = memory["ownership"]
        if set(owners) != {
            "model",
            "tokenizer_load",
            "tokenizer_cache",
            "weight_cache",
            "workspace",
        }:
            raise ValueError(f"missing idle ownership breakdown in {file.name}")
        if any(set(owner) != ledger_fields for owner in owners.values()):
            raise ValueError(f"invalid idle ownership shape in {file.name}")
        if any(
            sum(owner[name] for owner in owners.values()) != ledger[name]
            for name in ledger_fields
        ):
            raise ValueError(
                f"idle ownership does not conserve admission in {file.name}"
            )
        ordered = sorted(samples)
        calculated = {
            "median_ms": statistics.median(samples) / 1e6,
            "mean_ms": statistics.mean(samples) / 1e6,
            "p95_ms": ordered[math.ceil(0.95 * len(samples)) - 1] / 1e6,
            "serial_rps": 1e9 / statistics.mean(samples),
        }
        if any(
            not math.isclose(report[key], value, rel_tol=1e-9, abs_tol=1e-9)
            for key, value in calculated.items()
        ):
            raise ValueError(
                f"summary does not reproduce from raw samples in {file.name}"
            )
    if seen != expected:
        raise ValueError(f"missing request/path reports: {sorted(expected - seen)}")
    return [path.name for path in files]


def run_child(command: list[str], env: dict, log: Path, timeout: int) -> int:
    with log.open("w") as stream:
        process = subprocess.Popen(
            command,
            cwd=INFERENCE,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            return process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise


def campaigns(only: str | None = None) -> list[tuple[str, str, str]]:
    result = []
    for backend, spelling in (("native", "native"), ("metal", "Metal")):
        for profile in ("multi_v1", "multi_decide"):
            result.append(
                (
                    profile,
                    backend,
                    f"GLiNER2.5 multilingual family pinned {spelling} extraction direct and HTTP service parity",
                )
            )
        result.append(
            (
                "multi_decide",
                backend,
                f"GLiNER2.5 multilingual Decide pinned {spelling} direct and HTTP distributions",
            )
        )
        result.append(
            (
                "decide_1b",
                backend,
                f"GLiNER2.5 Decide-1B exact {spelling} direct and HTTP service parity",
            )
        )
    # The diagnostic direct lane is opt-in only. It must never enlarge or
    # replace the default eight direct-plus-HTTP qualification campaigns.
    if only == DIRECT_1B_NAME:
        result.append(
            (
                DIRECT_1B_PROFILE,
                "metal",
                "GLiNER2.5 Decide-1B Metal direct-only kernel performance",
            )
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", required=True, type=Path)
    parser.add_argument("--models-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument(
        "--only", help="Run one campaign name from the generated campaign list"
    )
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("this supervisor requires macOS /usr/bin/time -l byte units")
    if not 1 <= args.threads <= 8 or args.timeout <= 0:
        parser.error("threads must be1..8 and timeout must be positive")
    binary = args.test_binary.resolve(strict=True)
    models_root = args.models_root.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    cleared_overrides = {
        name: env.pop(name)
        for name in list(env)
        if name.startswith(
            (
                "TERMITE_",
                "ANTFLY_TEST_",
                "ANTFLY_METAL_",
                "ANTFLY_GLINER_",
                "ANTFLY_GLINER25_",
                "ANTFLY_INFERENCE_",
            )
        )
    }
    env.update({name: str(args.threads) for name in THREAD_VARIABLES})
    model_directories = (
        ("ANTFLY_GLINER25_MULTI_V1_MODEL_DIR", "multi"),
        ("ANTFLY_GLINER25_MULTI_DECIDE_MODEL_DIR", "multi-decide"),
        ("ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR", "decide-1b"),
    )
    if args.only == DIRECT_1B_NAME:
        model_directories = model_directories[-1:]
    for name, directory in model_directories:
        env[name] = str((models_root / directory).resolve(strict=True))
    identity = source_identity()
    binary_sha256 = sha256(binary)
    metadata = {
        "format_version": 1,
        "started_utc": utc_now(),
        "source": identity,
        "test_binary": str(binary),
        "test_binary_sha256": binary_sha256,
        "models_root": str(models_root),
        "platform": platform.platform(),
        "chip": command_text("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"),
        "physical_memory_bytes": command_text("/usr/sbin/sysctl", "-n", "hw.memsize"),
        "requested_thread_caps": {name: env[name] for name in THREAD_VARIABLES},
        "cleared_inherited_runtime_override_names": sorted(cleared_overrides),
        "scope": "serial cached Node direct calls and in-process HTTP handler dispatch, exact FP32 artifacts",
        "memory_scope": "time -l whole-process peaks include pin checks, cold loading and every request; never add to logical admission bytes",
        "cold_scope": "first unloaded-model direct request; checkpoint files already verified and filesystem cache warm",
        "campaigns": [],
    }
    if args.only == DIRECT_1B_NAME:
        metadata["scope"] = (
            "serial cached direct loaded-session pipeline diagnostic; no HTTP serving qualification"
        )
        metadata["cold_scope"] = (
            "model loading excluded; no cold-request latency reported"
        )
    receipt = output / "campaigns.json"
    receipt.write_text(json.dumps(metadata, indent=2) + "\n")
    matched = False
    for profile, backend, test_filter in campaigns(args.only):
        if profile == DIRECT_1B_PROFILE:
            task = "direct_pipeline"
            name = DIRECT_1B_NAME
        else:
            task = "extract" if "extraction" in test_filter else "decide"
            name = f"{profile}-{backend}-{task}"
        if args.only and args.only != name:
            continue
        matched = True
        directory = output / name
        directory.mkdir()
        child_env = env | {
            "ANTFLY_GLINER25_PERF_OUTPUT_DIR": str(directory),
            "ANTFLY_GLINER25_PERF_PROFILE": profile,
            "ANTFLY_GLINER25_PERF_SOURCE_HEAD": identity["head"],
        }
        if task == "direct_pipeline":
            child_env |= {
                "ANTFLY_GLINER25_DECIDE_1B_DIRECT_PERF": "1",
                "ANTFLY_GLINER25_PERF_SOURCE_DIFF_SHA256": identity[
                    "tracked_diff_sha256"
                ],
                "ANTFLY_GLINER25_PERF_BINARY_SHA256": binary_sha256,
            }
        log = directory / "test.log"
        time_output = directory / "time.txt"
        command = [
            "/usr/bin/time",
            "-l",
            "-o",
            str(time_output),
            str(binary),
            "--test-filter",
            test_filter,
        ]
        record = {
            "name": name,
            "profile": profile,
            "backend": backend,
            "task": task,
            "command": command,
            "before": host_context(),
        }
        if task == "direct_pipeline":
            record["fixture_identifier_note"] = (
                "Legacy direct_kernel identifiers name the loaded-session pipeline fixture; measurement_scope and timing_boundary define the clock."
            )
        print(f"Starting {name}", flush=True)
        start = time.monotonic()
        try:
            record["exit_code"] = run_child(command, child_env, log, args.timeout)
        except subprocess.TimeoutExpired:
            record["exit_code"] = 124
            record["error"] = "campaign timed out; owned process group terminated"
        record["whole_process_wall_seconds"] = time.monotonic() - start
        record["after"] = host_context()
        try:
            record["process_resources"] = parse_time(time_output.read_text())
        except (OSError, ValueError) as error:
            record["resource_error"] = str(error)
        test_log = log.read_text()
        record["test_summary"] = next(
            (line for line in reversed(test_log.splitlines()) if "selected;" in line),
            None,
        )
        try:
            record["performance_files"] = validate_reports(
                directory,
                profile,
                backend,
                task,
                identity["head"],
                args.threads,
                identity["tracked_diff_sha256"],
                binary_sha256,
            )
        except (ValueError, KeyError, TypeError, OSError) as error:
            record["performance_files"] = sorted(
                path.name for path in directory.glob("*.json")
            )
            record["performance_error"] = str(error)
        record["passed"] = (
            record["exit_code"] == 0
            and record["test_summary"] == "1 selected; 1 passed; 0 skipped."
            and bool(record["performance_files"])
            and "performance_error" not in record
            and "process_resources" in record
        )
        metadata["campaigns"].append(record)
        receipt.write_text(json.dumps(metadata, indent=2) + "\n")
        print(
            f"Finished {name}: {'PASS' if record['passed'] else 'FAIL'}; {record['test_summary']}; {record['whole_process_wall_seconds']:.2f}s whole process",
            flush=True,
        )
    if not matched:
        parser.error("--only did not match any campaign")
    metadata["finished_utc"] = utc_now()
    metadata["source_unchanged_during_run"] = identity == source_identity()
    metadata["passed"] = (
        bool(metadata["campaigns"])
        and all(row["passed"] for row in metadata["campaigns"])
        and metadata["source_unchanged_during_run"]
    )
    receipt.write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Receipt: {receipt}", flush=True)
    return 0 if metadata["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
