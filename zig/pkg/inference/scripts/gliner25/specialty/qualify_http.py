#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify both pinned specialty profiles through an owned production CUDA server.

Validation, warmups, and timed requests are separate phases. CUDA transfer and
allocation counters are deliberately left to the accompanying Zig service gate;
they are not exposed by the public HTTP API.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import socket
import statistics
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request

PACKAGE = Path(__file__).resolve().parents[3]
REPO = PACKAGE.parents[2]
MANIFEST = PACKAGE / "testdata/gliner25/specialty/manifest.json"
SCORE_TOLERANCE = 5e-4
PROFILES = ("pii", "gliguard")
ATTENTION_MODES = ("f32", "auto", "streaming-f16", "materialized-f16", "generated-tc")
CUDA_POLICY_MARKER = "gliner_specialty_cuda_policy"
CUDA_POLICY_LINE = re.compile(r".*?\bgliner_specialty_cuda_policy\s+profile=(\S+)\s+weights=(\S+)\s+attention=(\S+)\s+diagnostic_overrides=(\S+)\s*$")


def backend_environment(attention_mode, generated_tc_auto, inherited=None):
    if attention_mode not in ATTENTION_MODES or generated_tc_auto not in ("0", "1"):
        raise ValueError("unsupported attention diagnostic option")
    environment = dict(os.environ if inherited is None else inherited)
    environment["ANTFLY_INFERENCE_REQUIRED_BACKEND"] = "cuda"
    environment["ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE"] = attention_mode
    environment["ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO"] = generated_tc_auto
    return environment


def backend_environment_receipt(environment):
    global_controls = {"CUDA_VISIBLE_DEVICES", "CUDA_DEVICE_ORDER", "NVIDIA_TF32_OVERRIDE",
                       "CUBLAS_WORKSPACE_CONFIG", "CUDA_MODULE_LOADING", "ANTFLY_INFERENCE_REQUIRED_BACKEND"}
    return {key: value for key, value in environment.items()
        if (key in global_controls or key.startswith(("ANTFLY_CUDA_", "ANTFLY_INFERENCE_CUDA_", "TERMITE_CUDA_")))
        and not any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))}


def parse_cuda_policy(log_text):
    receipts = {}
    for line in log_text.splitlines():
        if CUDA_POLICY_MARKER not in line:
            continue
        message = line
        if line.lstrip().startswith("{"):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"malformed structured specialty CUDA policy log: {line}") from error
            if not isinstance(record, dict) or not isinstance(record.get("msg"), str):
                raise ValueError(f"structured specialty CUDA policy log has no message: {line}")
            message = record["msg"]
        matched = CUDA_POLICY_LINE.fullmatch(message)
        if not matched:
            raise ValueError(f"malformed specialty CUDA policy marker: {line}")
        profile, weights, attention, overrides = matched.groups()
        if profile not in PROFILES or (weights, attention, overrides) != ("f32", "f32", "ignored"):
            raise ValueError(f"specialty CUDA policy violates the FP32 contract: {line}")
        receipt = receipts.setdefault(profile, {"weights": weights, "attention": attention,
            "diagnostic_overrides": overrides, "log_lines": []})
        receipt["log_lines"].append(line)
    if set(receipts) != set(PROFILES):
        raise ValueError(f"missing specialty CUDA policy marker(s): {sorted(set(PROFILES) - set(receipts))}")
    return receipts


def running_binary_receipt(process, expected_sha256):
    """Bind the observed candidate SHA to this live Linux server leader image."""
    if process.poll() is not None:
        raise ValueError(f"server exited before running-image verification: {process.returncode}")
    image = Path("/proc") / str(process.pid) / "exe"
    try:
        resolved = os.readlink(image)
        actual = digest_file(image)
        group = os.getpgid(process.pid)
    except (FileNotFoundError, ProcessLookupError) as error:
        raise ValueError("server exited during running-image verification") from error
    if process.poll() is not None:
        raise ValueError(f"server exited during running-image verification: {process.returncode}")
    if actual != expected_sha256:
        raise ValueError(f"running server image SHA differs from candidate: {actual} != {expected_sha256}")
    if group != process.pid:
        raise ValueError(f"server leader escaped its owned process group: {group} != {process.pid}")
    return {"pid": process.pid, "process_group": group, "image": str(image),
            "resolved_executable": resolved, "sha256": actual}


def assert_server_shutdown(was_running, returncode):
    if not was_running or returncode not in (0, -signal.SIGTERM):
        raise ValueError(f"owned server exited unexpectedly or required forced shutdown: {returncode}")


def digest_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_file(path, pin):
    if path.stat().st_size != pin["size_bytes"] or digest_file(path) != pin["sha256"]:
        raise ValueError(f"pinned artifact drift: {path}")


def verify_inputs(models_root):
    manifest = json.loads(MANIFEST.read_text())
    references = {}
    for profile in PROFILES:
        pin = manifest["models"][profile]
        for name, expected in pin["files"].items():
            verify_file(models_root / profile / name, expected)
        oracle_path = models_root / pin["oracle"]["path"]
        verify_file(oracle_path, pin["oracle"])
        oracle = json.loads(oracle_path.read_text())
        if oracle["profile"] != profile or oracle["coordinate_unit"] != "unicode_codepoints":
            raise ValueError(f"unexpected oracle identity or coordinate unit: {profile}")
        if oracle["upstream"] != manifest["upstream"]:
            raise ValueError(f"unexpected oracle upstream: {profile}")
        if oracle["model"] != {key: value for key, value in pin.items() if key != "oracle"}:
            raise ValueError(f"unexpected oracle model pins: {profile}")
        if not oracle["cases"] or len({case["id"] for case in oracle["cases"]}) != len(oracle["cases"]):
            raise ValueError(f"empty or duplicate oracle cases: {profile}")
        if any(not 0 < len(case["input_ids"]) <= 512 for case in oracle["cases"]):
            raise ValueError(f"oracle exceeds prepared-token contract: {profile}")
        references[profile] = oracle
    return manifest, references


def request_body(profile, case, count=1):
    return json.dumps({
        "model": profile,
        "schema_version": 2,
        "schema": json.loads(case["native_schema_json"]),
        "inputs": [{"id": case["id"], "content": case["text"]} for _ in range(count)],
        "options": {"threshold": case.get("threshold", 0.5), "offset_unit": "unicode_codepoints",
                    "include_spans": True, "include_confidence": True},
    }, ensure_ascii=False, separators=(",", ":")).encode()


def check_response(profile, case, response, count=1):
    if response.get("schema_version") != 2 or len(response.get("data", [])) != count:
        raise ValueError(f"{profile}/{case['id']}: unexpected response version or item count")
    worst = 0.0
    for sample in response["data"]:
        if sample.get("id") != case["id"] or sample.get("offset_unit") != "unicode_codepoints":
            raise ValueError(f"{profile}/{case['id']}: changed item identity or coordinate unit")
        for key, fields in [("entities", ("label", "text", "start", "end")),
                            ("classifications", ("name", "label"))]:
            expected = case["expected_" + key]
            actual = sample.get(key, [])
            if len(expected) != len(actual):
                raise ValueError(f"{profile}/{case['id']}: {key} count differs: {len(actual)} != {len(expected)}")
            for index, (want, got) in enumerate(zip(expected, actual)):
                for field in fields:
                    if want[field] != got.get(field):
                        raise ValueError(f"{profile}/{case['id']}: {key}[{index}].{field}: {got.get(field)!r} != {want[field]!r}")
                value = got.get("score")
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError(f"{profile}/{case['id']}: missing or non-finite confidence")
                difference = abs(value - want["score"])
                if difference > SCORE_TOLERANCE:
                    raise ValueError(f"{profile}/{case['id']}: confidence error {difference} exceeds {SCORE_TOLERANCE}")
                worst = max(worst, difference)
                if key == "entities" and (type(got["start"]) is not int or type(got["end"]) is not int
                        or case["text"][got["start"]:got["end"]] != got["text"]):
                    raise ValueError(f"{profile}/{case['id']}: entity offsets do not address the original text")
        if sample.get("relations") or sample.get("structures"):
            raise ValueError(f"{profile}/{case['id']}: unsupported task output appeared")
    return worst


def http_json(base_url, path, body=None, timeout=120):
    request = urllib.request.Request(base_url + path, data=body,
        headers={"Content-Type": "application/json"} if body is not None else {},
        method="POST" if body is not None else "GET")
    started = time.perf_counter_ns()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        raise ValueError(f"HTTP {error.code} {path}: {error.read().decode(errors='replace')}") from error
    elapsed_ms = (time.perf_counter_ns() - started) / 1e6
    if status != 200:
        raise ValueError(f"HTTP {status} {path}")
    return json.loads(raw), elapsed_ms


def check_instances(response):
    if response.get("object") != "list" or not isinstance(response.get("data"), list):
        raise ValueError("model-instance endpoint did not return the production list schema")
    by_profile = {}
    for profile in PROFILES:
        matches = [row for row in response["data"] if row.get("model") == profile]
        if len(matches) != 1:
            raise ValueError(f"expected one loaded runtime for {profile}: {matches!r}")
        row = matches[0]
        if row.get("backend") != "cuda" or row.get("state") != "loaded" or "extractor" not in row.get("tasks", []):
            raise ValueError(f"{profile} is not a loaded CUDA extractor: {row!r}")
        if not row.get("startup_managed"):
            raise ValueError(f"{profile} runtime is not the requested startup preload")
        by_profile[profile] = row
    return by_profile


def wait_ready(process, base_url, timeout):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited before readiness: {process.returncode}")
        try:
            response, _ = http_json(base_url, "/ai/v1/models/instances", timeout=2)
            check_instances(response)
            return response
        except (OSError, ValueError) as error:
            last_error = str(error)
        time.sleep(0.25)
    raise TimeoutError(f"server startup timeout ({timeout}s): {last_error}")


class ProcessGroupCleanupError(RuntimeError):
    def __init__(self, group, remaining_pids, returncode):
        self.process_group = group
        self.remaining_pids = remaining_pids
        self.returncode = returncode
        super().__init__(f"owned process group {group} required SIGKILL; "
                         f"remaining live members={remaining_pids}, leader returncode={returncode}")


def process_group_members(group):
    """Live Linux members of the same owned PGID sampled by the GB10 PSS gate.

    Zombies have released CUDA contexts and mappings; their eventual reaping
    belongs to their parent. The directly owned Popen leader is always waited.
    """
    if not Path("/proc").is_dir():
        raise RuntimeError("GB10 process-group cleanup requires Linux /proc")
    members = []
    for directory in Path("/proc").iterdir():
        if not directory.name.isdigit():
            continue
        try:
            stat = (directory / "stat").read_text()
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Linux comm can contain spaces and parentheses. Fields after its last
        # closing parenthesis begin with state, ppid, pgrp.
        fields = stat[stat.rfind(")") + 1:].split()
        if len(fields) < 3:
            raise RuntimeError(f"malformed process stat for {directory.name}")
        if int(fields[2]) == group and fields[0] not in ("Z", "X", "x"):
            members.append(int(directory.name))
    return sorted(members)


def _wait_process_group(process, deadline):
    while True:
        process.poll()  # Reap a completed leader while draining its children.
        members = process_group_members(process.pid)
        if not members:
            try:
                process.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                return [process.pid]
            return []
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return members
        time.sleep(min(.025, remaining))


def terminate_server(process, grace_seconds=10):
    # Workers inherit this PGID in the production supervisor. Waiting only for
    # its leader can otherwise leave a child that ignores SIGTERM using CUDA.
    if not math.isfinite(grace_seconds) or grace_seconds < 0:
        raise ValueError("cleanup grace must be finite and nonnegative")
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    remaining = _wait_process_group(process, time.monotonic() + grace_seconds)
    if not remaining:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if process.poll() is None:
        process.kill()
    remaining = _wait_process_group(process, time.monotonic() + max(1, grace_seconds))
    # A forced cleanup fails qualification even when it successfully drained
    # all children. If a kernel/OS cannot reap a live member, report its PID and
    # stop; never publish a passing receipt with outstanding GPU work.
    raise ProcessGroupCleanupError(process.pid, remaining, process.returncode)


def quantile(values, q):
    ordered = sorted(values)
    index = (len(ordered) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def hardware():
    result = subprocess.run(["nvidia-smi", "--query-gpu=name,uuid,compute_cap,driver_version,memory.total",
                             "--format=csv,noheader"], capture_output=True, text=True, timeout=20, check=True)
    if "GB10" not in result.stdout:
        raise ValueError(f"this qualification campaign requires NVIDIA GB10; found {result.stdout.strip()}")
    return {"nvidia_smi": result.stdout.strip(), "platform": platform.platform(),
            "machine": platform.machine(), "python": platform.python_version()}


def qualify(args, report):
    manifest, references = verify_inputs(args.models_root)
    known_cases = {case["id"] for reference in references.values() for case in reference["cases"]}
    if args.case and set(args.case) - known_cases:
        raise ValueError(f"unknown timing cases: {sorted(set(args.case) - known_cases)}")
    report.update({"artifact_pins": manifest, "manifest_sha256": digest_file(MANIFEST),
                   "hardware": hardware(), "server_binary": {"path": str(args.server), "sha256": digest_file(args.server)},
                   "harness_sha256": digest_file(Path(__file__))})
    artifact_directory = PACKAGE / "src/ops/cuda/artifacts"
    report["cuda_artifact_files"] = {name: digest_file(artifact_directory / name) for name in (
        "inference_cuda_kernels.cu", "inference_cuda_kernels.ptx", "inference_cuda_kernels.fatbin",
        "inference_cuda_kernels_sm89.cubin")}
    report["cuda_artifact_receipt_scope"] = "Checked-in files accompanying this binary; compile flags and separate artifact freshness receipt identify the embedded bundle"
    # Reject occupied ports before spawning so an existing server can never
    # accidentally produce a passing qualification receipt.
    with socket.socket() as probe:
        # Match the server's restart policy: recently closed HTTP connections
        # may retain TIME_WAIT sockets after the owned listener has exited.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", args.port))
    command = [str(args.server), "run", "--host", "127.0.0.1", "--port", str(args.port),
               "--models-dir", str(args.models_root), "--max-loaded-models", "2",
               "--max-concurrent-requests", "8", "--kernel-jit-mode", "off",
               "--host-budget-mb", "8192", "--backend-budget-mb", "12288",
               "--combined-budget-mb", "20480", "--scratch-budget-mb", "4096",
               "--preload-model", "extractor:cuda:pii", "--preload-model", "extractor:cuda:gliguard"]
    environment = backend_environment(args.attention_mode, args.generated_tc_auto)
    report["server_command"] = command
    report["requested_attention_diagnostics"] = {"attention_mode": args.attention_mode,
        "generated_tc_auto": args.generated_tc_auto}
    # Diagnostic globals are intentionally configurable so the production
    # specialty profile must prove in its logs that it ignored conflicting modes.
    report["backend_environment"] = backend_environment_receipt(environment)
    log_path = args.output.with_suffix(".server.log")
    report["server_log"] = str(log_path)
    base_url = f"http://127.0.0.1:{args.port}"
    with log_path.open("wb") as log:
        process = subprocess.Popen(command, cwd=REPO, env=environment, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        report["server_pid"] = process.pid
        try:
            report["running_binary_before"] = running_binary_receipt(process, report["server_binary"]["sha256"])
            report["backend_instances_before"] = wait_ready(process, base_url, args.startup_timeout)
            report["cuda_policy_before"] = parse_cuda_policy(log_path.read_text(errors="replace"))
            report["validation"] = []
            # Every frozen case is validated regardless of selected timing cases.
            for profile in PROFILES:
                for case in references[profile]["cases"]:
                    response, _ = http_json(base_url, "/ai/v1/extract", request_body(profile, case))
                    worst = check_response(profile, case, response)
                    report["validation"].append({"profile": profile, "id": case["id"],
                        "max_confidence_abs_error": worst, "response": response})
            report["timings"] = []
            for profile in PROFILES:
                for case in references[profile]["cases"]:
                    if args.case and case["id"] not in args.case:
                        continue
                    body = request_body(profile, case, args.batch_size)
                    for _ in range(args.warmups):
                        response, _ = http_json(base_url, "/ai/v1/extract", body)
                        check_response(profile, case, response, args.batch_size)
                    samples = []
                    for _ in range(args.iterations):
                        response, elapsed = http_json(base_url, "/ai/v1/extract", body)
                        samples.append(elapsed)
                        check_response(profile, case, response, args.batch_size)
                    median = statistics.median(samples)
                    row = {"profile": profile, "id": case["id"], "request_items": args.batch_size,
                        "physical_batch_items": min(8, args.batch_size) if profile == "gliguard" else 1,
                        "prepared_tokens_per_item": len(case["input_ids"]), "warmups": args.warmups,
                        "iterations": args.iterations, "latency_ms": {"median": median,
                        "p95": quantile(samples, .95), "samples": samples},
                        "items_per_second": args.batch_size * 1000 / median,
                        "prepared_tokens_per_second": len(case["input_ids"]) * args.batch_size * 1000 / median}
                    report["timings"].append(row)
                    print(f"{profile}/{case['id']} b{args.batch_size}: {median:.3f}ms", flush=True)
            if not report["timings"]:
                raise ValueError("no timing cases selected")
            report["backend_instances_after"], _ = http_json(base_url, "/ai/v1/models/instances")
            before = check_instances(report["backend_instances_before"])
            after = check_instances(report["backend_instances_after"])
            if any(before[name]["id"] != after[name]["id"] for name in PROFILES):
                raise ValueError("model runtime incarnation changed during qualification")
            report["running_binary_after"] = running_binary_receipt(process, report["server_binary"]["sha256"])
        finally:
            was_running = process.poll() is None
            terminate_server(process)
            report["server_returncode"] = process.returncode
            assert_server_shutdown(was_running, process.returncode)
    report["server_log_sha256"] = digest_file(log_path)
    report["cuda_policy_after"] = parse_cuda_policy(log_path.read_text(errors="replace"))


def main():
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"received signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, default=PACKAGE / "zig-out/bin/antfly-inference-http-server")
    parser.add_argument("--models-root", type=Path, default=REPO / ".specialty-artifacts")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--port", type=int, default=18984)
    parser.add_argument("--startup-timeout", type=float, default=300)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--case", action="append", help="Limit timing cases only; validation always covers every frozen case")
    parser.add_argument("--attention-mode", choices=ATTENTION_MODES, default="f32",
                        help="Global attention diagnostic; specialty profiles must log that they retain FP32")
    parser.add_argument("--generated-tc-auto", choices=("0", "1"), default="0",
                        help="Global generated-TC diagnostic; specialty profiles must ignore this override")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.warmups < 0 or args.iterations < 1 or args.batch_size < 1:
        parser.error("invalid port, warmup count, iteration count, or batch size")
    for key in ("server", "models_root", "output"):
        setattr(args, key, getattr(args, key).resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"format_version": 1, "status": "failed", "campaign": "GB10 CUDA specialty public HTTP",
        "confidence_abs_tolerance": SCORE_TOLERANCE, "started_unix_seconds": time.time(),
        "timing_scope": "Loopback HTTP request send through complete response read; separate TCP request per sample; excludes model load, validation, warmups, and client JSON decode/checks",
        "cuda_transfers": {"available": False, "reason": "Public HTTP does not expose CUDA transfers; see separately measured Zig service receipts"},
        "cuda_allocations": {"available": False, "reason": "Public HTTP does not expose allocation counters or peaks; see separately measured Zig service receipts"}}
    try:
        qualify(args, report)
        report["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = str(error)
        report["traceback"] = traceback.format_exc()
        print(report["traceback"], file=sys.stderr)
    finally:
        if report.get("server_log") and Path(report["server_log"]).exists():
            report["server_log_sha256"] = digest_file(Path(report["server_log"]))
        report["finished_unix_seconds"] = time.time()
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
