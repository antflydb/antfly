#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run a local Laya job with durable timing/resource evidence and its real exit code."""

import argparse
import hashlib
import json
import math
import platform
import plistlib
import re
import shutil
import subprocess
import time
from pathlib import Path


def output(command):
    return subprocess.check_output(command, text=True).strip()


def sample(pid=None):
    swap = output(["sysctl", "vm.swapusage"])
    used = re.search(r"used = ([0-9.]+)M", swap)
    vm = output(["vm_stat"])
    counters = {
        name: int(value)
        for name, value in re.findall(
            r"^(Swapouts|Pageouts):\s+(\d+)\.", vm, re.MULTILINE
        )
    }
    rss = 0
    if pid:
        p = subprocess.run(
            ["ps", "-p", str(pid), "-o", "rss="],
            text=True,
            capture_output=True,
            check=False,
        )
        if p.returncode == 0 and p.stdout.strip():
            rss = int(p.stdout.strip()) * 1024
    gpu = {}
    try:
        devices = plistlib.loads(
            subprocess.check_output(
                ["/usr/sbin/ioreg", "-r", "-c", "IOAccelerator", "-a"]
            )
        )
        for source, name in (
            ("Alloc system memory", "system_gpu_allocated_bytes"),
            ("In use system memory", "system_gpu_in_use_bytes"),
        ):
            values = [
                device.get("PerformanceStatistics", {}).get(source)
                for device in devices
            ]
            values = [value for value in values if isinstance(value, int)]
            if values:
                gpu[name] = sum(values)
    except (OSError, subprocess.CalledProcessError, plistlib.InvalidFileException):
        pass  # Optional driver counters; RSS and paging evidence remain required.
    return {
        "swap_used_mib": float(used[1]) if used else None,
        "process_rss_bytes": rss,
        **counters,
        **gpu,
    }


def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def required_disk_bytes(model_dir):
    # An atomic replacement temporarily keeps both optimizer checkpoints.
    # Each trainable FP32 parameter has weight, accumulation, and two moments.
    with (model_dir / "model.safetensors").open("rb") as stream:
        prefix = stream.read(8)
        if len(prefix) != 8:
            raise ValueError("Missing safetensors header")
        length = int.from_bytes(prefix, "little")
        if length > 16 * 1024**2:
            raise ValueError("Oversized safetensors header")
        header = json.loads(stream.read(length))
    count = sum(
        math.prod(value["shape"])
        for name, value in header.items()
        if name not in ("__metadata__", "temperature")
        and not name.startswith("act_head.")
    )
    return max(12 * 1024**3, 2 * (16 * count + 8 * 1024**2) + 1024**3)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--job", type=Path, required=True)
    p.add_argument("--evidence", type=Path, required=True)
    p.add_argument("--timeout-seconds", type=int, default=21600)
    args = p.parse_args(argv)
    if args.timeout_seconds <= 0:
        raise ValueError("Timeout must be positive")
    args.evidence.mkdir(parents=True, exist_ok=False)
    admitted_job = args.job.read_bytes()
    job = json.loads(admitted_job)
    if Path(job["output_dir"]).exists():
        raise ValueError("Job output directory must be new")
    output_parent = Path(job["output_dir"]).parent
    while not output_parent.exists():
        output_parent = output_parent.parent
    disk = shutil.disk_usage(output_parent)
    required_disk = required_disk_bytes(Path(job["model_dir"]))
    if disk.free < required_disk:
        raise ValueError(
            f"At least {required_disk / 1024**3:.2f} GiB free disk is required for atomic checkpoint replacement"
        )
    baseline = sample()
    binary = args.evidence / "train-laya"
    shutil.copyfile(args.binary, binary)
    binary.chmod(0o500)
    job_snapshot = args.evidence / "job.json"
    job_snapshot.write_bytes(admitted_job)
    manifest = {
        "command": [str(binary.resolve()), str(job_snapshot.resolve())],
        "binary_sha256": sha(binary),
        "job_sha256": sha(job_snapshot),
        "platform": platform.platform(),
        "physical_memory_bytes": int(output(["sysctl", "-n", "hw.memsize"])),
        "disk_free_bytes": disk.free,
        "required_disk_bytes": required_disk,
        "baseline": baseline,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    manifest["cpu"] = output(["sysctl", "-n", "machdep.cpu.brand_string"])
    manifest["page_size_bytes"] = int(output(["sysctl", "-n", "hw.pagesize"]))
    (args.evidence / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    started = time.monotonic()
    peak = 0
    timed_out = False
    failure = None
    code = None
    with (
        (args.evidence / "process.log").open("w") as log,
        (args.evidence / "resources.jsonl").open("w") as resources,
        (args.evidence / "timeline.jsonl").open("w") as timeline,
    ):
        child = None
        observed = 0
        try:
            child = subprocess.Popen(
                manifest["command"], stdout=log, stderr=subprocess.STDOUT
            )
            while child.poll() is None:
                current = sample(child.pid)
                peak = max(peak, current["process_rss_bytes"])
                elapsed = time.monotonic() - started
                resources.write(
                    json.dumps({"elapsed_seconds": elapsed, **current}) + "\n"
                )
                resources.flush()
                metrics = Path(job["output_dir"]) / "metrics.jsonl"
                if metrics.exists():
                    lines = metrics.read_text().splitlines()
                    for line in lines[observed:]:
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            break
                        timeline.write(
                            json.dumps({"observed_seconds": elapsed, "event": event})
                            + "\n"
                        )
                        observed += 1
                    timeline.flush()
                if elapsed > args.timeout_seconds:
                    timed_out = True
                    child.terminate()
                    break
                time.sleep(5)
            try:
                code = child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                timed_out = True
                child.kill()
                code = child.wait()
        except Exception as exc:  # noqa: BLE001 -- preserve durable failure evidence before exiting nonzero
            failure = f"{type(exc).__name__}: {exc}"
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                code = child.wait()
    try:
        final = sample()
    except Exception as exc:  # noqa: BLE001 -- preserve durable failure evidence before exiting nonzero
        failure = failure or f"{type(exc).__name__}: {exc}"
        final = {}
    report_path = Path(job["output_dir"]) / "report.json"
    report = None
    try:
        report = json.loads(report_path.read_text()) if report_path.exists() else None
        if report is not None and not isinstance(report, dict):
            raise ValueError("Job report must be an object")
    except (OSError, ValueError) as exc:
        failure = failure or f"{type(exc).__name__}: {exc}"
        report = None
    if (
        sha(binary) != manifest["binary_sha256"]
        or sha(job_snapshot) != manifest["job_sha256"]
    ):
        failure = failure or "Executed inputs changed during qualification"
    result = {
        "exit_code": code,
        "timed_out": timed_out,
        "elapsed_seconds": time.monotonic() - started,
        "sampled_peak_rss_bytes": peak,
        "final": final,
        "swap_used_delta_mib": final["swap_used_mib"] - baseline["swap_used_mib"]
        if final.get("swap_used_mib") is not None
        and baseline.get("swap_used_mib") is not None
        else None,
        "global_swapouts_delta": final.get("Swapouts", 0) - baseline.get("Swapouts", 0),
        "global_pageouts_delta": final.get("Pageouts", 0) - baseline.get("Pageouts", 0),
        "job_report": report,
        "failure": failure,
        "complete": failure is None
        and not timed_out
        and code == 0
        and report is not None
        and report.get("status") == "complete",
    }
    (args.evidence / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    raise SystemExit(0 if result["complete"] else (code or 1))


if __name__ == "__main__":
    main()
