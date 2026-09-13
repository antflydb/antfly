#!/usr/bin/env python3
"""Matched, supervised GLiNER2.5 Metal versus pinned Fastino MPS/CPU benchmark.

Only completed, output-validated requests enter statistics. Model loading and
transport are excluded. Production request ownership and CPU decoding remain
inside each worker's clock. This is a diagnostic, not serving qualification.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time
from typing import Any

import benchmark_cpu as cpu
import extract_token_evidence as token_contract
import generate_pipeline_cases as adaptation
import oracle
import paired_benchmark
from metal_benchmark_supervisor import BenchmarkError, ResourceGuard, Worker

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[4]
SCOPE = "gliner25_direct_core_metal_comparison_fp32_v1"
TIMING_BOUNDARY = cpu.TIMING_BOUNDARY
NATIVE = "antfly_metal"
VARIANTS = ("small", "base", "multi")
NATIVE_KNOBS = (
    "TERMITE_METAL_DISABLE_DOT_GENERAL_2D_MPS", "TERMITE_METAL_FORCE_DEBERTA_SCALAR",
    "TERMITE_METAL_FORCE_DEBERTA_TG", "TERMITE_METAL_DISABLE_DEBERTA_MPS_ATTENTION",
    "TERMITE_METAL_DEBERTA_MPS_ATTENTION_MAX_MB",
    "TERMITE_METAL_DISABLE_GLINER_DEBERTA_DIRECT_FFN",
)
MPS_KNOBS = (
    "PYTORCH_ENABLE_MPS_FALLBACK", "PYTORCH_MPS_FAST_MATH",
    "PYTORCH_MPS_PREFER_METAL", "PYTORCH_MPS_HIGH_WATERMARK_RATIO",
    "PYTORCH_MPS_LOW_WATERMARK_RATIO", "PYTORCH_MPS_LOG_PROFILE_INFO",
    "PYTORCH_MPS_TRACE_SIGNPOSTS", "PYTORCH_DEBUG_MPS_ALLOCATOR",
)
BOOTSTRAP_SAMPLES = 10_000
SEED = 20260730


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n")
    temporary.replace(path)


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def rotate(values: list[str], offset: int) -> list[str]:
    if not values:
        return []
    at = offset % len(values)
    return values[at:] + values[:at]


def worker_environment() -> tuple[dict[str, str], dict[str, Any]]:
    env = dict(os.environ)
    controlled = (*cpu.THREAD_ENV, *NATIVE_KNOBS, *MPS_KNOBS, "USE_FLASHDEBERTA")
    inherited = {name: env.get(name) for name in controlled}
    for name in (*NATIVE_KNOBS, *MPS_KNOBS, "USE_FLASHDEBERTA"):
        env.pop(name, None)
    env.update({name: "1" for name in cpu.THREAD_ENV})
    env.update(PYTHONDONTWRITEBYTECODE="1", TOKENIZERS_PARALLELISM="false",
               HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
               PYTORCH_ENABLE_MPS_FALLBACK="0", PYTORCH_MPS_FAST_MATH="0")
    return env, {"inherited": inherited, "worker": {name: env.get(name) for name in controlled}}


def command_output(command: list[str]) -> str | None:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10, check=False)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def hardware_receipt() -> dict[str, Any]:
    import psutil
    power = command_output(["pmset", "-g", "batt"])
    power_source = ("ac" if power and "Now drawing from 'AC Power'" in power else
                    "battery" if power and "Now drawing from 'Battery Power'" in power else "unknown")
    power_settings = command_output(["pmset", "-g", "custom"])
    low_power_mode = None
    selected_section = "AC Power:" if power_source == "ac" else "Battery Power:"
    active = False
    for line in (power_settings or "").splitlines():
        if line and not line[0].isspace():
            active = line.strip() == selected_section
        elif active and line.strip().startswith("lowpowermode"):
            low_power_mode = line.split()[-1]
    return {
        "system": platform.system(), "macos": platform.mac_ver()[0],
        "machine": platform.machine(), "model_identifier": command_output(["sysctl", "-n", "hw.model"]),
        "chip": command_output(["sysctl", "-n", "machdep.cpu.brand_string"]),
        "physical_memory_bytes": psutil.virtual_memory().total,
        "logical_cpu_count": psutil.cpu_count(),
        "power": power, "power_source": power_source, "low_power_mode": low_power_mode,
        "power_settings": power_settings,
        "thermal": command_output(["pmset", "-g", "therm"]),
        "free_disk_bytes": __import__("shutil").disk_usage(HERE).free,
    }


def source_snapshot() -> dict[str, Any]:
    """Hash repository source/build inputs, including uncommitted additions.

    External dependency identity is carried by Zig's lockfiles and the pinned
    Python manifest. Models and oracle tensor fixtures have separate manifests.
    Logs, binaries, caches and documentation are deliberately not source inputs.
    """
    listing = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "zig"],
        cwd=REPO, capture_output=True, check=True, timeout=30,
    ).stdout
    suffixes = {".zig", ".zon", ".c", ".h", ".cpp", ".hpp", ".m", ".mm", ".metal",
                ".inc", ".py", ".json", ".txt"}
    files = {}
    for raw in sorted(set(listing.split(b"\0")) - {b""}):
        relative = os.fsdecode(raw)
        path = REPO / relative
        if path.suffix not in suffixes or any(p in (".zig-cache", "zig-out", "__pycache__") for p in path.parts):
            continue
        if path.is_symlink():
            files[relative] = {"symlink": os.readlink(path)}
        elif path.is_file():
            files[relative] = {"size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path)}
        else:
            files[relative] = {"deleted": True}
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return {
        "head": command_output(["git", "-C", str(REPO), "rev-parse", "HEAD"]),
        "source_files_sha256": hashlib.sha256(encoded).hexdigest(), "files": files,
        "scope": "repository_zig_source_build_scripts_and_json_inputs_including_dirty_files",
    }


def checked_ready(arm: str, ready: dict[str, Any], bundle: dict[str, Any],
                  variant: str, case_path: Path) -> None:
    required = {
        "event": "ready", "arm": arm, "scope": SCOPE, "timing_boundary": TIMING_BOUNDARY,
        "model": variant, "model_id": bundle["model_id"], "revision": bundle["revision"],
        "model_files": bundle["files"], "dtype": "float32", "threads": 1, "qualification": False,
    }
    for key, value in required.items():
        if ready.get(key) != value or (type(value) in (bool, int) and type(ready.get(key)) is not type(value)):
            raise BenchmarkError(f"{arm} readiness differs: {key}")
    if arm == NATIVE:
        required = {"backend": "metal", "device": "metal", "build_mode": "ReleaseFast",
                    "scheduler": "serial_requests", "runtime_ready": True, "external_frame": False,
                    "cases_sha256": oracle.sha256_file(case_path), "math_policy": "production_default",
                    "host_fallback_allowed": False, "native_environment": {name: None for name in NATIVE_KNOBS}}
    else:
        device = "mps" if arm == "fastino_mps" else "cpu"
        actual_device = ready.get("device", "").split(":")[0]
        if actual_device != device or ready.get("provenance", {}).get("device", "").split(":")[0] != device:
            raise BenchmarkError(f"{arm} actual device/provenance mismatch")
        required = {
            "interop_threads": 1, "parameter_device": device, "floating_dtype": "float32",
            "deterministic_algorithms": True, "strict_extraction": True,
            "requests_sha256": oracle.sha256_file(oracle.FIXTURES / "requests.json"),
            "math_policy": "pytorch_fp32_deterministic_no_mps_fallback_no_fast_math_v1",
            "synchronization_policy": ("torch_mps_synchronize_before_start_and_after_extract_v1"
                                       if device == "mps" else "synchronous_cpu_v1"),
        }
    for key, value in required.items():
        if ready.get(key) != value or (type(value) in (bool, int) and type(ready.get(key)) is not type(value)):
            raise BenchmarkError(f"{arm} backend/timing contract differs: {key}")


def checked_result(response: dict[str, Any], expected: dict[str, Any], arm: str,
                   *, validation: bool) -> dict[str, Any]:
    if response.get("event") == "error":
        raise cpu.BenchmarkError(f"{response.get('category', 'worker_error')}: {response.get('message', '')}")
    duration = response.get("duration_ns")
    if type(duration) is not int or duration <= 0:
        raise BenchmarkError(f"{arm}: invalid completed-request duration")
    actual = cpu.canonical_result(response["output"])
    cpu.require_equal(expected, actual, arm)
    if arm == NATIVE and response.get("gpu_work_submitted") is not True:
        raise BenchmarkError("native response has no strict device dispatch evidence")
    if arm == NATIVE:
        if response.get("external_frame") is not False:
            raise BenchmarkError("native response used an external command frame")
        stats = response.get("request_stats", {})
        dispatches = stats.get("encoder", {}).get("device_dispatches", 0)
        if type(dispatches) is not int or dispatches <= 0:
            raise BenchmarkError("native response lacks encoder device dispatches")
        fallback = response.get("host_fallback_evidence", {})
        if fallback.get("strict_device_dispatch") is not True or any(
            type(fallback.get(key)) is not int or fallback[key] != 0 for key in (
                "host_mirror_allocations_delta", "host_mirror_download_bytes_delta", "to_host_device_calls_delta")):
            raise BenchmarkError("native request used an unexpected host tensor fallback")
        memory = response.get("owned_memory", {})
        before, after = memory.get("before", {}), memory.get("after", {})
        for key in ("device_owned_live_bytes", "host_mirror_live_bytes"):
            if (any(type(snapshot.get(key)) is not int or snapshot[key] < 0 for snapshot in (before, after))
                    or before[key] != after[key]):
                raise BenchmarkError("native request retained owned device or host-mirror memory")
        for created, released in (("device_owned_buffers_created", "device_owned_buffers_released"),
                                  ("device_owned_bytes_created", "device_owned_bytes_released"),
                                  ("host_mirror_allocations", "host_mirror_frees")):
            if any(type(snapshot.get(key)) is not int or snapshot[key] < 0
                   for snapshot in (before, after) for key in (created, released)):
                raise BenchmarkError("native request memory receipt is malformed")
            allocated, freed = after[created] - before[created], after[released] - before[released]
            if allocated < 0 or allocated != freed:
                raise BenchmarkError("native request owned allocations did not balance")
    if validation:
        ids = response.get("input_ids")
        if (not isinstance(ids, list) or not ids or len(ids) > oracle.MAX_ENCODED_TOKENS
                or any(type(token) is not int for token in ids)):
            raise cpu.BenchmarkError(f"{arm}: invalid encoder tokens")
        if arm != NATIVE and response.get("input_device", "").split(":")[0] != arm.removeprefix("fastino_"):
            raise cpu.BenchmarkError(f"{arm}: encoder executed on a different device")
    return actual


def reference_input_ids(variant: str, fixture: dict[str, Any], case_path: Path) -> dict[str, list[int]]:
    """Read the existing derived CPU evidence without loading tensor diagnostics."""
    path = oracle.FIXTURES / "token_evidence.json"
    pin = adaptation.reference_inventory().get(path.name)
    if pin is None:
        raise BenchmarkError("reference manifest must retain encoder token evidence")
    oracle.verify_file(path, pin)
    report = oracle.read_json(path)
    if (report.get("format_version") != 1
            or report.get("scope") != "completed_cpu_benchmark_encoder_token_evidence"
            or report.get("fresh_model_execution") is not False
            or report.get("native_runtime_qualified") is not False
            or report.get("source_commit") != oracle.UPSTREAM_COMMIT
            or report.get("runtime") != oracle.load_manifest()["runtime"]
            or report.get("report_sha256") != token_contract.REPORT_SHA256
            or report.get("benchmark_driver_sha256") != token_contract.DRIVER_SHA256
            or report.get("native_binary_sha256") != token_contract.NATIVE_SHA256
            or report.get("generator_sha256") != oracle.sha256_file(Path(token_contract.__file__))):
        raise BenchmarkError("encoder token evidence source profile differs")
    models = report.get("models", [])
    if (len(models) != len(VARIANTS) or {row.get("model") for row in models} != set(VARIANTS)):
        raise BenchmarkError("encoder token evidence must cover all three model variants")
    evidence = next(row for row in models if row["model"] == variant)
    if (any(evidence.get(key) != fixture[key] for key in
            ("model", "model_id", "revision", "model_files", "reference_sha256", "requests_sha256"))
            or evidence.get("cases_sha256") != oracle.sha256_file(case_path)):
        raise BenchmarkError("encoder token evidence model or request identity differs")
    validation = evidence.get("validation", {})
    cases = fixture["cases"]
    if (len(cases) != len(validation) or set(validation) != {case["id"] for case in cases}):
        raise BenchmarkError("encoder token evidence case coverage differs")
    result = {}
    for case in cases:
        packet = validation[case["id"]]
        ids = packet.get("input_ids")
        if (not isinstance(ids, list) or not 0 < len(ids) <= oracle.MAX_ENCODED_TOKENS
                or any(type(token) is not int or not 0 <= token < 2**32 for token in ids)):
            raise BenchmarkError("invalid bounded reference encoder token sequence")
        request = json.dumps({"text": case["text"], "schema": case["schema"]}, ensure_ascii=False,
                             allow_nan=False, separators=(",", ":")).encode()
        token_bytes = b"".join(token.to_bytes(4, "little") for token in ids)
        if (hashlib.sha256(request).hexdigest() != packet.get("canonical_request_sha256")
                or hashlib.sha256(token_bytes).hexdigest() != packet.get("input_ids_u32_le_sha256")):
            raise BenchmarkError("encoder token evidence ordered request or token values differ")
        result[case["id"]] = ids
    return result


def load_contract(variant: str, model_root: Path, requested: list[str] | None) -> dict[str, Any]:
    case_path = cpu.case_fixture(variant)
    fixture = oracle.read_json(case_path)
    regenerated = adaptation.generate(variant, oracle.FIXTURES / f"{variant}_reference",
                                      oracle.FIXTURES / "requests.json")
    if json.dumps(fixture, ensure_ascii=False) != json.dumps(regenerated, ensure_ascii=False):
        raise BenchmarkError(f"{variant}: canonical fixture differs from pinned oracle adaptation")
    all_cases = {case["id"]: case for case in fixture["cases"]}
    selected = requested if requested is not None else list(all_cases)
    if not selected or len(set(selected)) != len(selected) or any(name not in all_cases for name in selected):
        raise BenchmarkError("case selection must be unique captured requests")
    reference_ids = reference_input_ids(variant, fixture, case_path)
    return {"model": variant, "bundle": oracle.verify_model_dir(variant, model_root / variant),
            "case_path": case_path, "cases": selected,
            "reference_input_ids": {name: reference_ids[name] for name in selected},
            "expected": {name: cpu.canonical_result(all_cases[name]["expected"]) for name in selected}}


def sample_receipt(response: dict[str, Any]) -> dict[str, Any]:
    # Full responses are in events.jsonl; keep the top-level report compact.
    receipt = {key: value for key, value in response.items()
               if key not in ("output", "input_ids", "input_device", "event", "arm", "request_id", "case_id")}
    receipt["output_sha256"] = hashlib.sha256(json.dumps(
        response.get("output"), sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode()).hexdigest()
    return receipt


def run_pair(args: argparse.Namespace, contract: dict[str, Any], baseline: str,
             directory: Path, *, repetition: int, phase: str, selected: list[str]) -> dict[str, Any]:
    directory.mkdir(parents=True)
    variant, reference = contract["model"], f"fastino_{baseline}"
    arms = (NATIVE, reference)
    env, environment = worker_environment()
    guard = ResourceGuard(args.max_rss_mib * 1024**2)
    command_count = len(selected) * (1 + (args.warmup + args.pairs if phase == "measurement" else 0)) + 1
    model_dir = args.model_root / variant
    commands = {
        NATIVE: [str(args.native_bin), "--model-dir", str(model_dir), "--cases", str(contract["case_path"]),
                 "--threads", "1", "--timeout-ms", str(args.timeout_ms), "--max-commands", str(command_count)],
        reference: [sys.executable, str(HERE / "metal_python_worker.py"), "--device", baseline,
                    "--model", variant, "--model-dir", str(model_dir), "--upstream", str(args.upstream),
                    "--max-commands", str(command_count)],
    }
    run = {"model": variant, "baseline": baseline, "phase": phase, "repetition": repetition,
           "status": "running", "started_at": utc_now(), "workers": {}, "commands": commands,
           "hardware_start": hardware_receipt(),
           "environment": environment, "validation": {}, "pairs": [], "comparisons": {},
           "case_status": {name: {"status": "pending"} for name in selected},
           "failures": [], "directory": str(directory)}
    workers: dict[str, Worker] = {}
    journal = (directory / "events.jsonl").open("w", encoding="utf-8")

    def record(value: dict[str, Any]) -> None:
        line = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
        if journal.tell() + len(line.encode()) > 64 * 1024**2:
            raise BenchmarkError("request evidence exceeded 64 MiB")
        journal.write(line)
        journal.flush()

    def call(arm: str, operation: str, name: str, stage: str, iteration: int = 0) -> dict[str, Any]:
        response = workers[arm].request(operation, name, args.timeout_ms / 1000 + 5)
        record({"stage": stage, "iteration": iteration, "arm": arm, "response": response})
        if response.get("event") == "error" and (response.get("recoverable") is not True or response.get("device_unsafe")):
            raise BenchmarkError(f"{arm}: unsafe worker error: {response}")
        return response

    def block(name: str, error: Exception, stage: str) -> None:
        failure = {"case_id": name, "stage": stage, "error": f"{type(error).__name__}: {error}"}
        run["failures"].append(failure)
        run["case_status"][name] = {"status": "blocked", **failure}

    try:
        for arm in paired_benchmark.balanced_pair_order(repetition + 1, reference, NATIVE):
            workers[arm] = Worker(arm, commands[arm], env, directory, guard)
            ready = workers[arm].receive(args.startup_timeout)
            record({"stage": "startup", "arm": arm, "response": ready})
            checked_ready(arm, ready, contract["bundle"], variant, contract["case_path"])
            run["workers"][arm] = ready
        for name in selected:
            responses = {arm: call(arm, "validate", name, "validation") for arm in arms}
            try:
                normalized = {arm: checked_result(responses[arm], contract["expected"][name], arm, validation=True)
                              for arm in arms}
                if responses[NATIVE]["input_ids"] != responses[reference]["input_ids"]:
                    raise cpu.BenchmarkError("encoder token identity differs between implementations")
                captured_ids = contract.get("reference_input_ids", {}).get(name)
                if captured_ids is not None and responses[NATIVE]["input_ids"] != captured_ids:
                    raise cpu.BenchmarkError("encoder tokens differ from the frozen oracle capture")
                run["validation"][name] = {
                    "input_ids": responses[NATIVE]["input_ids"], "outputs": normalized,
                    "expected": contract["expected"][name], "confidence_absolute_tolerance": 5e-4,
                    "outputs_match_oracle": True,
                    "tokens_match_frozen_capture": True if captured_ids is not None else None,
                }
                run["case_status"][name] = {"status": "validated"}
            except (cpu.BenchmarkError, KeyError, TypeError) as error:
                block(name, error, "validation")
        eligible = [name for name in selected if run["case_status"][name]["status"] == "validated"]
        if phase == "measurement":
            for iteration in range(args.warmup):
                for name in rotate(eligible.copy(), repetition + iteration):
                    try:
                        for arm in paired_benchmark.balanced_pair_order(iteration + repetition + 1, *arms):
                            response = call(arm, "run", name, "warmup", iteration + 1)
                            checked_result(response, contract["expected"][name], arm, validation=False)
                    except (cpu.BenchmarkError, KeyError, TypeError) as error:
                        block(name, error, "warmup")
                        eligible.remove(name)
            for pair in range(1, args.pairs + 1):
                for name in rotate(eligible.copy(), repetition + pair - 1):
                    row = {"pair": pair, "case_id": name,
                           "order": paired_benchmark.balanced_pair_order(pair + repetition, *arms),
                           "valid": False}
                    run["pairs"].append(row)
                    try:
                        for arm in row["order"]:
                            response = call(arm, "run", name, "measurement", pair)
                            row[arm] = sample_receipt(response)
                            checked_result(response, contract["expected"][name], arm, validation=False)
                        row["valid"] = True
                    except (cpu.BenchmarkError, KeyError, TypeError) as error:
                        block(name, error, "measurement")
                        eligible.remove(name)
        for arm, worker in workers.items():
            response = worker.request("stop", timeout=args.startup_timeout)
            record({"stage": "shutdown", "arm": arm, "response": response})
            if worker.process.wait(timeout=5) != 0:
                raise BenchmarkError(f"{arm}: worker did not exit successfully after stop")
        if oracle.verify_model_dir(variant, model_dir) != contract["bundle"]:
            raise BenchmarkError("model identity changed during comparison")
        oracle.verify_upstream_checkout(args.upstream)
        if phase == "measurement":
            for name in eligible:
                rows = [row for row in run["pairs"] if row["case_id"] == name and row["valid"]]
                if len(rows) != args.pairs:
                    raise BenchmarkError("incomplete pair count admitted for statistics")
                pairs = [(row[reference]["duration_ns"], row[NATIVE]["duration_ns"]) for row in rows]
                run["comparisons"][name] = {
                    "sample_pairs": len(pairs),
                    "metal_ns": paired_benchmark.distribution(native for _, native in pairs),
                    "python_ns": paired_benchmark.distribution(python for python, _ in pairs),
                    "python_over_metal_speedup": paired_benchmark.paired_log_ratio_ci(
                        pairs, samples=BOOTSTRAP_SAMPLES, seed=SEED),
                }
                run["case_status"][name] = {"status": "complete"}
        run["status"] = "partial" if run["failures"] else "complete"
    except (KeyboardInterrupt, SystemExit):
        run["status"] = "interrupted"
        raise
    except Exception as error:
        run["status"] = "failed"
        run["error"] = f"{type(error).__name__}: {error}"
        for name in selected:
            if run["case_status"][name]["status"] != "blocked":
                block(name, error, "worker_lifecycle")
        run["comparisons"] = {}
    finally:
        cleanup_errors = []
        for arm, worker in workers.items():
            try:
                worker.close()
            except Exception as error:
                cleanup_errors.append(f"{arm}: {type(error).__name__}: {error}")
        journal.close()
        run["resource_guard"] = guard.receipt()
        run["cleanup"] = {arm: getattr(worker, "cleanup", None) for arm, worker in workers.items()}
        cleanup_receipts = list(run["cleanup"].values()) + [
            entry.get("cleanup") for entry in run["resource_guard"].get("completed_workers", [])]
        if any(isinstance(receipt, dict) and receipt.get("complete") is not True
               for receipt in cleanup_receipts):
            cleanup_errors.append("an owned worker has an incomplete cleanup receipt")
        run["finished_at"] = utc_now()
        run["hardware_end"] = hardware_receipt()
        run["power_profile_stable"] = all(
            run["hardware_start"].get(key) == run["hardware_end"].get(key)
            for key in ("power_source", "low_power_mode"))
        if phase == "measurement" and not run["power_profile_stable"]:
            run["status"] = "failed"
            run["error"] = "power source or low-power mode changed within the repetition"
            run["comparisons"] = {}
            for name in selected:
                block(name, BenchmarkError(run["error"]), "power_profile")
        if cleanup_errors:
            run["status"] = "failed"
            run["cleanup_errors"] = cleanup_errors
            run["comparisons"] = {}
        write_json(directory / "run.json", run)
    return run


def aggregate(report: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for contract in report["contracts"]:
        for name in contract["cases"]:
            for baseline in report["baselines"]:
                observations = [
                    {"repetition": run["repetition"], **run["comparisons"][name],
                     "power_profile": {key: run.get("hardware_start", {}).get(key)
                                       for key in ("power_source", "low_power_mode")}}
                    for run in report["runs"]
                    if run["phase"] == "measurement" and run["model"] == contract["model"]
                    and run["baseline"] == baseline and name in run["comparisons"]
                ]
                row = {"model": contract["model"], "case_id": name, "baseline": baseline,
                       "status": "blocked", "repetitions": observations}
                same_power_profile = not observations or all(
                    obs["power_profile"] == observations[0]["power_profile"] for obs in observations)
                if not same_power_profile:
                    row["error"] = "power profile differs between repetitions"
                if len(observations) == report["repetitions"] and same_power_profile:
                    cis = [obs["python_over_metal_speedup"] for obs in observations]
                    row.update(
                        status="complete",
                        metal_median_ms=statistics.median(obs["metal_ns"]["median"] for obs in observations) / 1e6,
                        python_median_ms=statistics.median(obs["python_ns"]["median"] for obs in observations) / 1e6,
                        median_speedup=statistics.median(ci["median"] for ci in cis),
                        repeatability=("insufficient_repetitions" if len(cis) < 3 else
                                       "metal_faster" if all(ci["lower_95"] > 1 for ci in cis) else
                                       "python_faster" if all(ci["upper_95"] < 1 for ci in cis) else
                                       "inconclusive"),
                    )
                result.append(row)
    return result


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# GLiNER2.5 Metal comparison", "",
        f"Status: **{report['status']}**. Batch 1, FP32, one CPU math thread per arm.",
        "Speedup is Python latency / Metal latency; values above 1 favor Metal.",
        "Latencies are medians of the fresh-process repetition medians. Intervals describe each repetition separately.",
        "Sample p95 is descriptive; this report does not qualify serving latency or release readiness.", "",
    ]
    if report.get("error"):
        lines.extend([f"Campaign error: {report['error']}", ""])
    for baseline in report["baselines"]:
        lines.extend([f"## Fastino {baseline.upper()} reference", "",
                      "| Model | Request | Metal ms | Python ms | Speedup | 95% intervals by repetition | Result |",
                      "| --- | --- | ---: | ---: | ---: | --- | --- |"])
        for row in report["summary"]:
            if row["baseline"] != baseline:
                continue
            if row["status"] != "complete":
                lines.append(f"| {row['model']} | {row['case_id']} | — | — | — | — | blocked |")
                continue
            intervals = "; ".join(
                f"{obs['repetition'] + 1}: [{obs['python_over_metal_speedup']['lower_95']:.3f}, "
                f"{obs['python_over_metal_speedup']['upper_95']:.3f}]" for obs in row["repetitions"])
            lines.append(
                f"| {row['model']} | {row['case_id']} | {row['metal_median_ms']:.3f} | "
                f"{row['python_median_ms']:.3f} | {row['median_speedup']:.3f}× | {intervals} | {row['repeatability']} |")
        lines.append("")
    failures = [run for run in report["runs"] if run["status"] != "complete"]
    if failures:
        lines.extend(["## Preserved failures", ""])
        for run in failures:
            description = run.get("error") or "; ".join(
                f"{entry['case_id']}: {entry['error']}" for entry in run["failures"])
            lines.append(f"- {run['phase']} / {run['model']} / {run['baseline']} / "
                         f"repetition {run['repetition'] + 1}: {description}")
        lines.append("")
    lines.extend(["Full outputs, timings, identities, memory observations and diagnostics accompany report.json.",
                  "Native Metal uses production default math, including eligible Apple MPSMatrix operations. "
                  "PyTorch MPS uses its pinned deterministic FP32 profile. Host decoding and native request uploads are timed.",
                  "The MPS and CPU comparisons use separate pairs; their Metal measurements must not be mixed.", ""])
    return "\n".join(lines)


def driver(args: argparse.Namespace) -> int:
    if (not 1 <= args.repetitions <= 5 or not 0 <= args.warmup <= 8
            or not 2 <= args.pairs <= 64 or args.pairs % 2
            or not 1 <= args.timeout_ms <= 60_000 or not 1 <= args.startup_timeout <= 300
            or not 256 <= args.max_rss_mib <= 8192):
        raise BenchmarkError("invalid resource or sampling limits; pair count must be even")
    if args.output.exists() or not args.native_bin.is_file():
        raise BenchmarkError("requires a new output directory and an existing Metal executable")
    oracle.verify_config_fixtures()
    oracle.verify_reference_fixtures()
    dependencies = oracle.verify_dependencies()
    upstream = oracle.verify_upstream_checkout(args.upstream)
    variants = list(VARIANTS) if args.model == "all" else [args.model]
    baselines = ["mps", "cpu"] if args.baseline == "both" else [args.baseline]
    contracts = [load_contract(variant, args.model_root, args.cases) for variant in variants]
    args.output.mkdir(parents=True)
    source = source_snapshot()
    write_json(args.output / "source_manifest.json", source)
    report = {
        "format_version": 1, "status": "running", "scope": SCOPE, "timing_boundary": TIMING_BOUNDARY,
        "started_at": utc_now(), "threads": 1, "batch_size": 1, "precision": "float32",
        "serving_qualified": False, "performance_release_qualified": False,
        "repetitions": args.repetitions, "warmup_per_case": args.warmup, "pairs_per_case": args.pairs,
        "bootstrap_samples": BOOTSTRAP_SAMPLES, "bootstrap_seed": SEED,
        "baselines": baselines, "hardware_start": hardware_receipt(),
        "upstream": upstream, "dependencies": dependencies,
        "source_head": source["head"], "source_files_sha256": source["source_files_sha256"],
        "native_binary": {"path": str(args.native_bin), "sha256": oracle.sha256_file(args.native_bin)},
        "excluded": ["process_startup", "model_loading", "HTTP", "model_resolution",
                     "wire_JSON_serialization", "returned_result_destruction", "validation_hooks", "result_comparison"],
        "contracts": [{key: value for key, value in contract.items() if key not in ("case_path", "expected")}
                      for contract in contracts],
        "runs": [], "summary": [],
    }
    write_json(args.output / "report.json", report)

    def execute(contract: dict[str, Any], baseline: str, *, repetition: int, phase: str,
                selected: list[str]) -> dict[str, Any]:
        name = f"{phase}-{repetition + 1}-{contract['model']}-{baseline}"
        print(json.dumps({"event": "comparison_start", "run": name, "cases": selected}), flush=True)
        run = run_pair(args, contract, baseline, args.output / name,
                       repetition=repetition, phase=phase, selected=selected)
        report["runs"].append(run)
        write_json(args.output / "report.json", report)
        print(json.dumps({"event": "comparison_end", "run": name, "status": run["status"],
                          "error": run.get("error"), "blocked_cases": [
                              key for key, value in run["case_status"].items() if value["status"] == "blocked"]}), flush=True)
        if run.get("cleanup_errors"):
            raise BenchmarkError("worker cleanup failed; no further model processes will be started")
        return run

    try:
        eligible = {}
        # Complete the entire requested preflight matrix before timing any row.
        for contract in contracts:
            for baseline in baselines:
                run = execute(contract, baseline, repetition=0, phase="preflight", selected=contract["cases"])
                eligible[(contract["model"], baseline)] = [
                    name for name, value in run["case_status"].items()
                    if value["status"] == "validated" and run["status"] in ("complete", "partial")]
        by_variant = {contract["model"]: contract for contract in contracts}
        for repetition in range(args.repetitions):
            for variant in rotate(variants, repetition):
                for baseline in baselines:
                    selected = eligible[(variant, baseline)]
                    if selected:
                        execute(by_variant[variant], baseline, repetition=repetition,
                                phase="measurement", selected=selected)
        if oracle.sha256_file(args.native_bin) != report["native_binary"]["sha256"]:
            raise BenchmarkError("native executable changed during the campaign")
        if source_snapshot() != source:
            raise BenchmarkError("repository source identity changed during the campaign")
        oracle.verify_config_fixtures()
        oracle.verify_reference_fixtures()
        oracle.verify_upstream_checkout(args.upstream)
        if oracle.verify_dependencies() != dependencies:
            raise BenchmarkError("Python dependencies changed during the campaign")
        report["summary"] = aggregate(report)
        report["status"] = "complete" if all(row["status"] == "complete" for row in report["summary"]) else "partial"
        report["parity_validated"] = report["status"] == "complete"
    except BaseException as error:
        report["status"] = "interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        report["parity_validated"] = False
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
    finally:
        report["hardware_end"] = hardware_receipt()
        report["finished_at"] = utc_now()
        write_json(args.output / "report.json", report)
        (args.output / "summary.md").write_text(markdown_report(report), encoding="utf-8")
        paired_benchmark.write_evidence_manifest(args.output)
    print(json.dumps({"status": report["status"], "report": str(args.output / "report.json")}), flush=True)
    return 0 if report["status"] == "complete" else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--model-root", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", choices=(*VARIANTS, "all"), default="all")
    parser.add_argument("--baseline", choices=("mps", "cpu", "both"), default="both")
    parser.add_argument("--cases", nargs="+")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--pairs", type=int, default=30)
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--max-rss-mib", type=int, default=8192)
    args = parser.parse_args()
    for name in ("native_bin", "model_root", "upstream", "output"):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    try:
        return driver(args)
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
