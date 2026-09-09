#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# Licensed under the Apache License, Version 2.0 (the "License").
"""Run a local benchmark and its workers inside a bounded process group.

Wrap the process that launches both servers and the benchmark client. This
accounts for worker/compiler children, including children whose parent exits.
The command's stdout/stderr and a report survive failures. Resource probes are
macOS-specific; an unavailable probe fails closed before starting work.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import time

from qwen3vl.qualify_qwen3vl_metal import (
    memory_free_percent,
    swapout_bytes,
    write_json_atomic,
)


def group_rss_mib(group: int, listing: str | None = None) -> float:
    if listing is None:
        listing = subprocess.check_output(
            ["/bin/ps", "-axo", "pgid=,rss="], text=True, timeout=10
        )
    rss_kib = 0
    for line in listing.splitlines():
        fields = line.split()
        if len(fields) != 2:
            raise ValueError("malformed process-group RSS probe")
        pgid, rss = map(int, fields)
        if rss < 0:
            raise ValueError("negative process RSS")
        if pgid == group:
            rss_kib += rss
    return rss_kib / 1024.0


def terminate_group(process: subprocess.Popen) -> None:
    # The leader may have exited while a worker still belongs to this group.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            break
        except PermissionError:
            # Some macOS launch environments reject signals to a reaped group.
            # Verify that no live address space remains before accepting this.
            if process.poll() is not None and group_rss_mib(process.pid) == 0:
                break
            raise
        if sig == signal.SIGTERM:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
    process.wait(timeout=5)


def run_guarded(
    command: list[str],
    output: Path,
    *,
    timeout: float = 180,
    max_rss_mib: float = 8192,
    min_free_percent: int = 15,
    max_swap_growth_mib: float = 0,
    min_disk_free_mib: float = 1024,
    interval: float = 0.25,
) -> dict:
    import shutil

    if not command or not all(
        math.isfinite(x)
        for x in (
            timeout,
            max_rss_mib,
            max_swap_growth_mib,
            min_disk_free_mib,
            interval,
        )
    ):
        raise ValueError("command and finite resource limits are required")
    if timeout <= 0 or max_rss_mib <= 0 or not 0 < interval <= 1:
        raise ValueError("timeout/RSS must be positive and interval in (0, 1]")
    if (
        not 0 < min_free_percent <= 100
        or min(max_swap_growth_mib, min_disk_free_mib) < 0
    ):
        raise ValueError("invalid free-memory, swap, or disk limit")
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "schema": "antfly.benchmark_resources.v1",
        "command": command,
        "pass": False,
        "returncode": None,
        "violation": None,
        "limits": dict(
            timeout_seconds=timeout,
            max_rss_mib=max_rss_mib,
            min_free_percent=min_free_percent,
            max_swap_growth_mib=max_swap_growth_mib,
            min_disk_free_mib=min_disk_free_mib,
        ),
        "samples": 0,
        "peak_group_rss_mib": 0.0,
    }
    started = time.monotonic()
    process = None
    baseline_swap = None

    def sample_system() -> None:
        nonlocal baseline_swap
        free = memory_free_percent()
        swap = swapout_bytes()
        disk = shutil.disk_usage(output.parent).free / 1048576
        if baseline_swap is None:
            baseline_swap = swap
            report["initial_free_percent"] = free
        report["min_free_percent"] = min(report.get("min_free_percent", 100), free)
        report["swapout_growth_mib"] = max(swap - baseline_swap, 0) / 1048576
        report["min_disk_free_mib"] = min(report.get("min_disk_free_mib", disk), disk)
        if free < min_free_percent:
            raise RuntimeError(f"free memory {free}% below {min_free_percent}%")
        if report["swapout_growth_mib"] > max_swap_growth_mib:
            raise RuntimeError("swapout growth exceeded budget")
        if disk < min_disk_free_mib:
            raise RuntimeError("free disk space below budget")

    try:
        sample_system()
        group_rss_mib(os.getpgrp())  # Verify process visibility before launch.
        with (
            output.with_suffix(".stdout").open("wb") as stdout,
            output.with_suffix(".stderr").open("wb") as stderr,
        ):
            process = subprocess.Popen(
                command, stdout=stdout, stderr=stderr, start_new_session=True
            )
            report["pid"] = process.pid
            next_system = 0.0
            next_report = 0.0
            while process.poll() is None:
                now = time.monotonic()
                if now - started > timeout:
                    raise RuntimeError(f"command exceeded {timeout}s timeout")
                rss = group_rss_mib(process.pid)
                report["samples"] += 1
                report["peak_group_rss_mib"] = max(report["peak_group_rss_mib"], rss)
                if rss > max_rss_mib:
                    raise RuntimeError(
                        f"process-group RSS {rss:.1f} MiB exceeded budget"
                    )
                if now >= next_system:
                    sample_system()
                    next_system = now + 1
                if now >= next_report:
                    report["elapsed_seconds"] = now - started
                    write_json_atomic(output, report)
                    next_report = now + 10
                time.sleep(interval)
            report["returncode"] = process.returncode
            sample_system()
            report["pass"] = process.returncode == 0
    except (Exception, KeyboardInterrupt) as exc:
        report["pass"] = False
        report["violation"] = f"{type(exc).__name__}: {exc}"
    finally:
        if process is not None:
            try:
                terminate_group(process)
            except Exception as exc:
                report["cleanup_error"] = f"{type(exc).__name__}: {exc}"
                report["pass"] = False
            report["returncode"] = process.returncode
        report["elapsed_seconds"] = time.monotonic() - started
        write_json_atomic(output, report)
    return report


def main() -> int:
    def interrupted(signum, _frame):
        raise InterruptedError(f"benchmark interrupted by signal {signum}")

    # Ensure termination of the launcher also reaches workers that belong to
    # its separately owned process group.
    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--max-rss-mib", type=float, default=8192)
    parser.add_argument("--min-free-percent", type=int, default=15)
    parser.add_argument("--max-swap-growth-mib", type=float, default=0)
    parser.add_argument("--min-disk-free-mib", type=float, default=1024)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    result = run_guarded(
        command,
        args.output,
        timeout=args.timeout,
        max_rss_mib=args.max_rss_mib,
        min_free_percent=args.min_free_percent,
        max_swap_growth_mib=args.max_swap_growth_mib,
        min_disk_free_mib=args.min_disk_free_mib,
    )
    print(json.dumps(result, indent=2))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
