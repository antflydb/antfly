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

"""Serial, source-bound Gemma4 Metal native-checkpoint campaign launcher.

The launcher freezes authority before loading a model and runs exactly one
phase per invocation.  It never enables production serving and never treats a
smoke or quality wall clock as matched performance evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

BASE = Path(__file__).resolve().parent
REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(BASE))

from context_release_build_adapter import validate as validate_build
import native_repeat_campaign_resources as resources

SCHEMA = "antfly.metal-native-checkpoint.campaign-spec.v2"
PHASE_SCHEMA = "antfly.metal-native-checkpoint.phase.v2"
HEX = re.compile(r"[0-9a-f]{64}")
HEADROOM = {"required_free_percent": 76, "samples": 2, "interval_seconds": 10}
LIMITS = {"max_rss_mib": 8192, "min_free_percent": 15,
          "max_swap_growth_mib": 0,
          "min_disk_free_mib": 4096}
SIZES = (("8k", 742), ("32k", 2975))
ENCODINGS = ("f16", "fp8")
KINDS = ("smoke", "quality", "performance")
CAMPAIGN_PROFILES = {
    "bounded": {
        "quality_profile": "bounded", "quality_cases_per_size": 16,
        "critical_cases_per_category": 4,
        "categories": ["code", "fact", "instruction", "tool_call"],
        "performance_warmups": 5, "performance_measured_triplets": 20,
        "performance_schedule": "balanced_round_robin_all_six_orders",
        "extended_profile": False, "reliability_gates_in_campaign": False,
        "production_ready_claim": False,
    },
    "extended": {
        "quality_profile": "extended", "quality_cases_per_size": 64,
        "critical_cases_per_category": None,
        "categories": ["code", "fact", "instruction", "tool_call"],
        "performance_warmups": 5, "performance_measured_triplets": 30,
        "performance_schedule": "balanced_round_robin_all_six_orders",
        "extended_profile": True, "reliability_gates_in_campaign": False,
        "production_ready_claim": False,
    },
}
PHASES = tuple(f"{kind}-{size}-{encoding}" for size, _ in SIZES
               for kind in KINDS for encoding in ENCODINGS)
PHASE_TIMEOUTS = {
    phase: (1800 if phase.startswith("smoke-8k-") else
            7200 if phase.startswith("smoke-32k-") else
            14400 if phase.startswith("performance-8k-") else
            43200 if phase.startswith("performance-32k-") else
            43200 if "-8k-" in phase else 172800)
    for phase in PHASES
}
REQUEST_TIMEOUTS = {"8k": 1800, "32k": 7200}
SERVER_LIFETIME_MARGIN_SECONDS = 300
SERVER_LIFETIME_MAX_SECONDS = 173100
RUNTIME_PREFIXES = ("ANTFLY_", "TERMITE_", "MTL_", "METAL_", "CUDA_",
                    "GGML_", "ZIG_")


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_new(path, value):
    path = Path(path)
    with path.open("xb") as stream:
        stream.write((json.dumps(value, indent=2, sort_keys=True) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())
    return value


def checked_ref(value, label):
    require(type(value) is dict and set(value) == {"path", "sha256"} and
            isinstance(value["path"], str) and Path(value["path"]).is_absolute() and
            HEX.fullmatch(value["sha256"] or ""), "invalid " + label)
    path = Path(value["path"])
    require(path.is_file() and not path.is_symlink() and sha(path) == value["sha256"],
            "changed " + label)
    return path


def suite_contract(path, campaign_profile):
    require(campaign_profile in CAMPAIGN_PROFILES, "unknown campaign profile")
    contract = CAMPAIGN_PROFILES[campaign_profile]
    raw = path.read_bytes()
    require(len(raw) <= 128 * 1024 * 1024, "suite exceeds byte limit")
    rows = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        require(type(value) is dict, "suite row must be an object")
        rows.append(value)
    expected_cases = contract["quality_cases_per_size"]
    require(len(rows) == expected_cases,
            f"frozen {campaign_profile} suite must contain exactly {expected_cases} cases")
    identifiers = [row.get("id", row.get("case_id")) for row in rows]
    require(all(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value)
                for value in identifiers) and len(set(identifiers)) == expected_cases,
            "suite case IDs must be unique and canonical")
    categories = {category: 0 for category in contract["categories"]}
    critical = 0
    for row in rows:
        require(row.get("category") in categories, "suite has unknown category")
        categories[row["category"]] += 1
        critical += row.get("critical") is True
    if campaign_profile == "bounded":
        require(critical == expected_cases and
                all(count == contract["critical_cases_per_category"]
                    for count in categories.values()),
                "bounded suite must have four preselected critical cases per category")
    else:
        require(critical >= 16 and all(count > 0 for count in categories.values()),
                "extended suite must retain critical and category coverage")
    return {"cases": expected_cases, "bytes": len(raw),
            "critical_cases": critical, "categories": categories}


def performance_request_contract(path, size, model):
    value = json.loads(path.read_bytes())
    reserved = 64 if size == "8k" else 256
    require(type(value) is dict and value.get("model") == model and
            value.get("max_tokens") == reserved and value.get("temperature") == 0 and
            value.get("prompt_cache") in (None, False) and
            value.get("cache_dtype") in (None, "f16") and
            value.get("stream") is True and
            value.get("mode") in (None, "eager") and
            "context_management" not in value and
            isinstance(value.get("messages"), list) and value["messages"],
            "invalid frozen " + size + " performance request")
    return value


def phase_parts(name):
    match = re.fullmatch(r"(smoke|quality|performance)-(8k|32k)-(f16|fp8)", name)
    require(match is not None, "invalid phase")
    return match.groups()


def predecessors(name):
    return PHASES[:PHASES.index(name)]


def load_spec(root):
    root = Path(root).resolve(strict=True)
    require(root.is_dir() and not root.is_symlink(), "campaign root must be real")
    path = root / "campaign-spec.json"
    value = json.loads(path.read_bytes())
    profile = value.get("campaign_profile") if type(value) is dict else None
    require(type(value) is dict and value.get("schema") == SCHEMA and
            value.get("backend") == "metal" and
            value.get("server_kind") == "metal-test" and
            profile in CAMPAIGN_PROFILES and
            value.get("qualification_contract") == CAMPAIGN_PROFILES[profile] and
            value.get("headroom") == HEADROOM and value.get("limits") == LIMITS and
            value.get("phase_timeouts_seconds") == PHASE_TIMEOUTS and
            value.get("request_timeouts_seconds") == REQUEST_TIMEOUTS and
            value.get("phases") == list(PHASES), "invalid campaign spec")
    python = Path(value.get("python", ""))
    require(python.is_absolute() and python.is_file() and os.access(python, os.X_OK),
            "frozen Python interpreter changed")
    checked_ref(value["build_receipt"], "build receipt")
    binary = checked_ref(value["binary"], "test binary")
    build = validate_build(Path(value["build_receipt"]["path"]), binary)
    require(build["receipt_sha256"] == value["build_receipt"]["sha256"] and
            build["binary_sha256"] == value["binary"]["sha256"],
            "build closure changed")
    for label, ref in value["runners"].items():
        checked_ref(ref, label + " runner")
    for size, ref in value["suites"].items():
        observed = suite_contract(checked_ref(ref, size + " suite"), profile)
        require(value.get("suite_contracts", {}).get(size) == observed,
                "frozen suite contract changed")
    for size, ref in value["performance_requests"].items():
        performance_request_contract(checked_ref(ref, size + " performance request"),
                                     size, value["model"]["request_model"])
    model = value["model"]
    require(type(model) is dict and set(model) == {"models_root", "request_model",
            "directory", "artifact", "artifact_sha256", "manifest_sha256"},
            "invalid model binding")
    models_root, directory = Path(model["models_root"]), Path(model["directory"])
    require(models_root.is_absolute() and directory.is_absolute() and
            models_root.is_dir() and directory.is_dir() and
            not models_root.is_symlink() and not directory.is_symlink() and
            directory.is_relative_to(models_root),
            "model registry binding changed")
    manifest = directory / ".antfly-download-complete.json"
    require(manifest.is_file() and not manifest.is_symlink() and
            sha(manifest) == model["manifest_sha256"], "model manifest changed")
    entries = json.loads(manifest.read_text()).get("artifacts", [])
    require(any(row.get("path") == model["artifact"] and
                row.get("sha256") == model["artifact_sha256"]
                for row in entries), "model artifact identity changed")
    require(sha(__file__) == value["launcher_sha256"], "launcher changed after freeze")
    return root, path, value


def init(args):
    root = args.campaign_root.resolve(strict=True)
    require(root.is_dir() and not root.is_symlink() and not any(root.iterdir()),
            "init requires an empty real campaign root")
    build = validate_build(args.build_receipt.resolve(strict=True))
    require(build["binary_path"] == args.binary.resolve(strict=True),
            "receipt does not bind selected test binary")
    models_root = args.models_dir.resolve(strict=True)
    require(args.model == "google/gemma-4-E4B-it-qat-q4_0-gguf",
            "campaign request model must be the pinned Gemma4 E4B ID")
    directory = args.model_directory.resolve(strict=True)
    require(directory.is_dir() and not directory.is_symlink() and
            directory.is_relative_to(models_root), "invalid model registry entry")
    manifest = directory / ".antfly-download-complete.json"
    require(manifest.is_file() and not manifest.is_symlink(), "missing model manifest")
    manifest_value = json.loads(manifest.read_text())
    artifact_rows = [row for row in manifest_value.get("artifacts", [])
                     if row.get("path") == args.artifact]
    require(len(artifact_rows) == 1 and HEX.fullmatch(
            artifact_rows[0].get("sha256", "")), "missing model artifact binding")
    artifact_path = directory / args.artifact
    require(artifact_path.is_file() and not artifact_path.is_symlink() and
            artifact_path.stat().st_size == artifact_rows[0].get("size"),
            "model artifact size or path changed")
    suite_refs = {}
    suite_contracts = {}
    for size, path in (("8k", args.suite_8k), ("32k", args.suite_32k)):
        path = path.resolve(strict=True)
        info = suite_contract(path, args.campaign_profile)
        suite_refs[size] = {"path": str(path), "sha256": sha(path)}
        suite_contracts[size] = info
    performance_refs = {}
    for size, path in (("8k", args.performance_request_8k),
                       ("32k", args.performance_request_32k)):
        path = path.resolve(strict=True)
        performance_request_contract(path, size, args.model)
        performance_refs[size] = {"path": str(path), "sha256": sha(path)}
    runner_paths = {"http": args.http_runner, "quality": args.quality_runner,
                    "performance": args.performance_runner}
    runners = {}
    for label, path in runner_paths.items():
        path = path.resolve(strict=True)
        require(path.is_file() and not path.is_symlink(), "missing " + label + " runner")
        runners[label] = {"path": str(path), "sha256": sha(path)}
    spec = {"schema": SCHEMA, "backend": "metal", "server_kind": "metal-test",
            "diagnostic_only": True, "production_ready": False,
            "campaign_profile": args.campaign_profile,
            "qualification_contract": CAMPAIGN_PROFILES[args.campaign_profile],
            "launcher_sha256": sha(__file__),
            "python": str(Path(sys.executable).resolve(strict=True)),
            "build_receipt": {"path": str(build["receipt_path"]),
                              "sha256": build["receipt_sha256"]},
            "binary": {"path": str(build["binary_path"]),
                       "sha256": build["binary_sha256"]},
            "model": {"models_root": str(models_root), "request_model": args.model,
                      "directory": str(directory), "artifact": args.artifact,
                      "artifact_sha256": artifact_rows[0]["sha256"],
                      "manifest_sha256": sha(manifest)},
            "runners": runners, "suites": suite_refs,
            "suite_contracts": suite_contracts,
            "performance_requests": performance_refs,
            "headroom": HEADROOM, "limits": LIMITS,
            "phase_timeouts_seconds": PHASE_TIMEOUTS,
            "request_timeouts_seconds": REQUEST_TIMEOUTS,
            "base_port": args.base_port, "phases": list(PHASES)}
    write_new(root / "campaign-spec.json", spec)
    return write_new(root / "init.json", {
        "schema": "antfly.metal-native-checkpoint.campaign-init.v2",
        "campaign_spec_sha256": sha(root / "campaign-spec.json"),
        "serving_qualified": False, "performance_qualified": False,
        "production_ready": False})


def headroom(root, phase):
    path = root / (phase + "-headroom.json")
    rows = []
    try:
        for index in range(HEADROOM["samples"]):
            raw = subprocess.check_output(["/usr/bin/memory_pressure", "-Q"],
                                          text=True, timeout=30)
            match = re.search(r"free percentage:\s*(\d+)%", raw)
            require(match is not None, "unparseable headroom probe")
            free = int(match.group(1))
            rows.append({"free_percent": free, "unix_seconds": time.time()})
            require(free >= HEADROOM["required_free_percent"],
                    f"loader headroom below {HEADROOM['required_free_percent']}%")
            if index + 1 < HEADROOM["samples"]:
                time.sleep(HEADROOM["interval_seconds"])
    except Exception:
        write_new(path, {**HEADROOM, "pass": False, "samples_observed": rows})
        raise
    return write_new(path, {**HEADROOM, "pass": True, "samples_observed": rows})


def command(spec, phase, root):
    kind, size, encoding = phase_parts(phase)
    padding = dict(SIZES)[size]
    port = spec["base_port"] + PHASES.index(phase)
    common = ["--backend", "metal", "--server-kind", "metal-test",
              "--server", spec["binary"]["path"], "--models-dir",
              spec["model"]["models_root"], "--model", spec["model"]["request_model"],
              "--port", str(port), "--encoding", encoding, "--timeout",
              str(spec["request_timeouts_seconds"][size])]
    output = root / (phase + ".json")
    checkpoint = root / (phase + ".kv")
    if kind == "smoke":
        return [spec["python"], spec["runners"]["http"]["path"], *common,
                "--padding-lines", str(padding), "--fixture", "fact",
                "--expected-outcome", "compacted", "--minimum-savings", "1.5",
                "--output", str(output), "--checkpoint", str(checkpoint)]
    if kind == "quality":
        return [spec["python"], spec["runners"]["quality"]["path"], *common,
                "--suite", spec["suites"][size]["path"], "--quality-profile",
                spec["qualification_contract"]["quality_profile"],
                "--size", size, "--output", str(output),
                "--checkpoint", str(checkpoint)]
    return [spec["python"], spec["runners"]["performance"]["path"], *common,
            "--request", spec["performance_requests"][size]["path"],
            "--declared-context-tokens", "8192" if size == "8k" else "32768",
            "--warmups", str(spec["qualification_contract"]["performance_warmups"]),
            "--pairs", str(spec["qualification_contract"][
                "performance_measured_triplets"]),
            "--minimum-savings", "1.5", "--output", str(output),
            "--checkpoint", str(checkpoint)]


def clean_environment():
    return {key: value for key, value in os.environ.items()
            if not key.startswith(RUNTIME_PREFIXES)}


def validate_metal_resources(evidence):
    require(evidence.get("qualification_profile") ==
            "metal_test_native_checkpoint_v1", "wrong qualification profile")
    records = [value for value in evidence.values()
               if isinstance(value, dict) and isinstance(value.get("memory"), dict)]
    require(records, "missing unified Metal resource evidence")
    for record in records:
        memory = record["memory"]
        require(memory.get("scope") == "unified_process_group" and
                memory.get("max_combined_rss_bytes") == LIMITS["max_rss_mib"] * 1024 * 1024 and
                type(memory.get("samples")) is int and memory["samples"] >= 2 and
                memory.get("errors") == [] and
                memory.get("swapout_growth_bytes") == 0 and
                type(memory.get("minimum_free_memory_percent")) is int and
                memory["minimum_free_memory_percent"] >= LIMITS["min_free_percent"] and
                type(memory.get("minimum_disk_free_mib")) in (int, float) and
                memory["minimum_disk_free_mib"] >= LIMITS["min_disk_free_mib"] and
                type(memory.get("peak_group_rss_bytes")) is int and
                0 < memory["peak_group_rss_bytes"] <= LIMITS["max_rss_mib"] * 1024 * 1024 and
                record.get("graceful_stop") is True and
                record.get("remaining_group_members") == [] and
                record.get("stopped") is True,
                "incomplete Metal server resource evidence")


def run_phase(args):
    root, spec_path, spec = load_spec(args.campaign_root)
    phase = args.phase
    require(phase in PHASES, "unknown phase")
    # Every earlier phase must be explicitly sealed.  This makes 8K precede
    # 32K and smoke precede quality/performance without auto-running anything.
    for previous in predecessors(phase):
        path = root / (previous + "-phase.json")
        require(path.is_file() and not path.is_symlink(),
                "missing predecessor phase: " + previous)
        value = json.loads(path.read_text())
        require(value.get("schema") == PHASE_SCHEMA and value.get("passed") is True,
                "predecessor did not pass: " + previous)
    output = root / (phase + ".json")
    checkpoint = root / (phase + ".kv")
    resource = root / (phase + "-resource.json")
    phase_path = root / (phase + "-phase.json")
    require(not any(path.exists() or path.is_symlink()
                    for path in (output, checkpoint, resource, phase_path)),
            "phase output already exists")
    headroom(root, phase)
    argv = command(spec, phase, root)
    phase_timeout = spec["phase_timeouts_seconds"][phase]
    server_lifetime = min(
        phase_timeout + SERVER_LIFETIME_MARGIN_SECONDS,
        SERVER_LIFETIME_MAX_SECONDS,
    )
    saved = os.environ.copy()
    sanitized = clean_environment()
    sanitized["ANTFLY_METAL_CHECKPOINT_MAX_LIFETIME_SECONDS"] = str(server_lifetime)
    os.environ.clear(); os.environ.update(sanitized)
    try:
        guarded = resources.run_guarded_combined(argv, resource, **{
            "timeout": phase_timeout,
            "max_rss_mib": LIMITS["max_rss_mib"],
            "min_free_percent": LIMITS["min_free_percent"],
            "max_swap_growth_mib": LIMITS["max_swap_growth_mib"],
            "min_disk_free_mib": LIMITS["min_disk_free_mib"]})
    finally:
        os.environ.clear(); os.environ.update(saved)
    require(guarded.get("pass") is True and guarded.get("returncode") == 0 and
            guarded.get("swapout_growth_mib") == 0 and
            guarded.get("remaining_child_group_live_processes") == 0 and
            guarded.get("remaining_child_group_rss_mib") == 0,
            "phase resource guard failed")
    evidence = json.loads(output.read_text())
    require(evidence.get("passed") is True and evidence.get("backend") == "metal" and
            evidence.get("server_kind") == "metal-test" and
            evidence.get("production_ready") is False,
            "phase lacks Metal test-ingress evidence")
    kind, size, _ = phase_parts(phase)
    contract = spec["qualification_contract"]
    if kind == "quality":
        require(evidence.get("quality_profile") == contract["quality_profile"] and
                evidence.get("size") == size and
                evidence.get("suite_sha256") == spec["suites"][size]["sha256"] and
                isinstance(evidence.get("case_ids"), list) and
                len(evidence["case_ids"]) == contract["quality_cases_per_size"],
                "quality evidence differs from frozen campaign profile")
    if kind == "performance":
        require(evidence.get("warmups") == contract["performance_warmups"] and
                evidence.get("pairs_requested") ==
                    contract["performance_measured_triplets"] and
                evidence.get("qualification_sized") is True,
                "performance evidence differs from frozen campaign profile")
    validate_metal_resources(evidence)
    if phase.startswith("performance-"):
        require(evidence.get("performance_qualified") is True,
                "matched performance or physical-savings gate failed")
    return write_new(phase_path, {"schema": PHASE_SCHEMA, "phase": phase,
        "passed": True, "diagnostic_only": True, "production_ready": False,
        "campaign_profile": spec["campaign_profile"],
        "qualification_contract": contract,
        "phase_wall_budget_seconds": phase_timeout,
        "test_server_max_lifetime_seconds": server_lifetime,
        "campaign_spec_sha256": sha(spec_path), "command": argv,
        "headroom_sha256": sha(root / (phase + "-headroom.json")),
        "resource_sha256": sha(resource), "output_sha256": sha(output),
        "checkpoint_sha256": sha(root / (phase + ".kv"))})


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    setup = sub.add_parser("init")
    setup.add_argument("--campaign-root", type=Path, required=True)
    setup.add_argument("--build-receipt", type=Path, required=True)
    setup.add_argument("--binary", type=Path, required=True)
    setup.add_argument("--models-dir", type=Path, required=True)
    setup.add_argument("--model", required=True)
    setup.add_argument("--model-directory", type=Path, required=True)
    setup.add_argument("--artifact", default="gemma-4-E4B_q4_0-it.gguf")
    setup.add_argument("--campaign-profile", choices=tuple(CAMPAIGN_PROFILES),
                       default="bounded")
    setup.add_argument("--suite-8k", type=Path, required=True)
    setup.add_argument("--suite-32k", type=Path, required=True)
    setup.add_argument("--performance-request-8k", type=Path, required=True)
    setup.add_argument("--performance-request-32k", type=Path, required=True)
    setup.add_argument("--http-runner", type=Path,
                       default=BASE / "qualify_native_checkpoint_http.py")
    setup.add_argument("--quality-runner", type=Path,
                       default=BASE / "qualify_native_checkpoint_quality.py")
    setup.add_argument("--performance-runner", type=Path,
                       default=BASE / "qualify_metal_native_checkpoint_performance.py")
    setup.add_argument("--base-port", type=int, default=18600)
    run = sub.add_parser("run")
    run.add_argument("--campaign-root", type=Path, required=True)
    run.add_argument("--phase", choices=PHASES, required=True)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    require(1024 <= getattr(args, "base_port", 1024) <= 65535 - len(PHASES),
            "invalid campaign port range")
    value = init(args) if args.command == "init" else run_phase(args)
    print(json.dumps(value, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"Metal checkpoint campaign: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2)
