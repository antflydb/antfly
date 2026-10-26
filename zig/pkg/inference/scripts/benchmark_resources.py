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
import shutil
import stat
import subprocess
import tempfile
import time

from qwen3vl.qualify_qwen3vl_metal import (
    memory_free_percent,
    swapout_bytes,
    write_json_atomic,
)


GUARD_FD_ENV = "ANTFLY_BENCHMARK_GUARD_FD"
GUARD_TOKEN_ENV = "ANTFLY_BENCHMARK_GUARD_TOKEN"
GUARD_OWNER_PID_ENV = "ANTFLY_BENCHMARK_GUARD_OWNER_PID"
GUARD_WITNESS_BYTES = 32


class DiskSpaceGuard:
    """Keep artifact, work and temp volumes bound for the whole run."""

    def __init__(self, output_directory: Path):
        paths = dict.fromkeys((Path(output_directory).resolve(strict=True),
                               Path.cwd().resolve(strict=True),
                               Path(tempfile.gettempdir()).resolve(strict=True)))
        self.volumes = tuple((path, path.stat().st_dev) for path in paths)

    def sample_mib(self) -> float:
        available = []
        for path, device in self.volumes:
            if path.stat().st_dev != device:
                raise RuntimeError("disk guard filesystem changed: " + str(path))
            available.append(shutil.disk_usage(path).free / 1048576)
        if not all(math.isfinite(value) and value >= 0 for value in available):
            raise ValueError("invalid free-disk probe")
        return min(available)


def group_rss_mib(group: int, listing: str | None = None) -> float:
    if listing is None:
        listing = subprocess.check_output(
            ["/bin/ps", "-axo", "pgid=,rss=,stat="], text=True, timeout=10
        )
    rss_kib = 0
    for line in listing.splitlines():
        fields = line.split()
        if len(fields) not in (2, 3):
            raise ValueError("malformed process-group RSS probe")
        pgid, rss = map(int, fields[:2])
        if rss < 0:
            raise ValueError("negative process RSS")
        # A zombie has no live address space. It can remain in ps briefly after
        # the group has otherwise drained and must not make cleanup fail.
        if pgid == group and (len(fields) == 2 or not fields[2].startswith("Z")):
            rss_kib += rss
    return rss_kib / 1024.0


def group_live_processes(group: int, listing: str | None = None) -> int:
    if listing is None:
        listing = subprocess.check_output(
            ["/bin/ps", "-axo", "pgid=,stat="], text=True, timeout=10
        )
    live = 0
    for line in listing.splitlines():
        fields = line.split()
        if len(fields) != 2:
            raise ValueError("malformed process-group liveness probe")
        pgid = int(fields[0])
        if pgid == group and not fields[1].startswith("Z"):
            live += 1
    return live


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


def wait_group_drained(group: int, timeout: float = 5) -> tuple[float, int]:
    deadline = time.monotonic() + timeout
    while True:
        remaining = group_rss_mib(group)
        live = group_live_processes(group)
        if remaining == 0 and live == 0:
            return 0.0, 0
        if time.monotonic() >= deadline:
            return remaining, live
        time.sleep(0.05)


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
        "peak_group_live_processes": 0,
    }
    started = time.monotonic()
    process = None
    baseline_swap = None
    witness_read = witness_write = None

    def sample_system() -> None:
        nonlocal baseline_swap
        free = memory_free_percent()
        swap = swapout_bytes()
        disk = disk_guard.sample_mib()
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

    def sample_group() -> float:
        rss = group_rss_mib(process.pid)
        live = group_live_processes(process.pid)
        report["samples"] += 1
        report["peak_group_rss_mib"] = max(report["peak_group_rss_mib"], rss)
        report["peak_group_live_processes"] = max(
            report["peak_group_live_processes"], live
        )
        if rss > max_rss_mib:
            raise RuntimeError(f"process-group RSS {rss:.1f} MiB exceeded budget")
        return rss

    try:
        disk_guard = DiskSpaceGuard(output.parent)
        sample_system()
        group_rss_mib(os.getpgrp())  # Verify process visibility before launch.
        with (
            output.with_suffix(".stdout").open("wb") as stdout,
            output.with_suffix(".stderr").open("wb") as stderr,
        ):
            witness_read, witness_write = os.pipe()
            token = os.urandom(GUARD_WITNESS_BYTES)
            if os.write(witness_write, token) != len(token):
                raise RuntimeError("failed to issue complete resource-guard witness")
            os.close(witness_write)
            witness_write = None
            child_env = os.environ.copy()
            child_env.update(
                {
                    GUARD_FD_ENV: str(witness_read),
                    GUARD_TOKEN_ENV: token.hex(),
                    GUARD_OWNER_PID_ENV: str(os.getpid()),
                }
            )
            process = subprocess.Popen(
                command,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
                env=child_env,
                pass_fds=(witness_read,),
            )
            os.close(witness_read)
            witness_read = None
            report["pid"] = process.pid
            next_system = 0.0
            next_report = 0.0
            while process.poll() is None:
                now = time.monotonic()
                if now - started > timeout:
                    raise RuntimeError(f"command exceeded {timeout}s timeout")
                sample_group()
                if now >= next_system:
                    sample_system()
                    next_system = now + 1
                if now >= next_report:
                    report["elapsed_seconds"] = now - started
                    write_json_atomic(output, report)
                    next_report = now + 10
                time.sleep(interval)
            report["returncode"] = process.returncode
            report["post_exit_group_rss_mib"] = sample_group()
            report["post_exit_group_live_processes"] = group_live_processes(process.pid)
            sample_system()
            report["pass"] = process.returncode == 0
    except (Exception, KeyboardInterrupt) as exc:
        report["pass"] = False
        report["violation"] = f"{type(exc).__name__}: {exc}"
    finally:
        for fd in (witness_read, witness_write):
            if fd is not None:
                os.close(fd)
        if process is not None:
            try:
                terminate_group(process)
            except Exception as exc:
                report["cleanup_error"] = f"{type(exc).__name__}: {exc}"
                report["pass"] = False
            try:
                (report["remaining_group_rss_mib"],
                 report["remaining_group_live_processes"]) = wait_group_drained(process.pid)
                if (report["remaining_group_rss_mib"] != 0 or
                        report["remaining_group_live_processes"] != 0):
                    report["cleanup_error"] = "live process-group address space remains"
                    report["pass"] = False
            except Exception as exc:
                report["cleanup_error"] = f"{type(exc).__name__}: {exc}"
                report["pass"] = False
            try:
                # Cleanup can itself page memory or cause swapout. A passing
                # receipt covers that interval too, not only command execution.
                sample_system()
            except Exception as exc:
                if report["violation"] is None:
                    report["violation"] = f"{type(exc).__name__}: {exc}"
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
