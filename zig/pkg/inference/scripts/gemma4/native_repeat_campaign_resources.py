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

"""Bound a campaign parent and one active child process group together.

This is the reusable in-repository form of the native-repeat campaign guard.
It deliberately delegates platform probes, disk-volume pinning, process-group
termination, and atomic receipt writes to :mod:`benchmark_resources`.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

SCRIPTS = Path(__file__).resolve().parent.parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import benchmark_resources as base


SCHEMA = "antfly.attention-matching.combined-resource-guard.v1"
RSS_SCOPE = "campaign_parent_plus_active_child_pgid"


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def process_rss(parent_pid, child_group, listing=None):
    """Return parent, child-group, combined MiB and live child count once."""
    require(type(parent_pid) is int and parent_pid > 0 and
            (child_group is None or type(child_group) is int and child_group > 0),
            "positive process identities required")
    if listing is None:
        listing = subprocess.check_output(
            ["/bin/ps", "-axo", "pid=,pgid=,rss=,stat="],
            text=True, timeout=10)
    parent_kib = child_kib = child_live = 0
    seen = set()
    for line in listing.splitlines():
        fields = line.split()
        require(len(fields) in (3, 4), "malformed combined RSS probe")
        pid, pgid, rss = map(int, fields[:3])
        require(pid > 0 and pgid > 0 and rss >= 0,
                "invalid combined RSS probe")
        zombie = len(fields) == 4 and fields[3].startswith("Z")
        if zombie or pid in seen:
            continue
        if pid == parent_pid:
            parent_kib += rss
            seen.add(pid)
        elif child_group is not None and pgid == child_group:
            child_kib += rss
            child_live += 1
            seen.add(pid)
    parent = parent_kib / 1024.0
    child = child_kib / 1024.0
    return parent, child, parent + child, child_live


def _claim(path, value):
    with Path(path).open("xb") as stream:
        stream.write((json.dumps(value, indent=2, sort_keys=True) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())


def run_guarded_combined(command, output, *, timeout, max_rss_mib,
                         min_free_percent, max_swap_growth_mib,
                         min_disk_free_mib, interval=0.25,
                         _rss_probe=process_rss):
    """Run ``command`` with exact combined-parent/child resource accounting."""
    numbers = (timeout, max_rss_mib, max_swap_growth_mib,
               min_disk_free_mib, interval)
    require(type(command) is list and command and
            all(isinstance(value, str) and value for value in command) and
            all(type(value) in (int, float) and math.isfinite(value)
                for value in numbers) and timeout > 0 and max_rss_mib > 0 and
            0 < interval <= 1 and type(min_free_percent) is int and
            0 < min_free_percent <= 100 and
            min(max_swap_growth_mib, min_disk_free_mib) >= 0,
            "invalid command or combined resource limits")
    output = Path(output)
    require(output.parent.is_dir() and not output.parent.is_symlink(),
            "combined resource output parent must be an existing directory")
    stdout_path, stderr_path = output.with_suffix(".stdout"), output.with_suffix(".stderr")
    require(all(not path.exists() and not path.is_symlink()
                for path in (output, stdout_path, stderr_path)),
            "combined resource output already exists")
    report = {
        "schema": SCHEMA, "command": command, "pass": False,
        "returncode": None, "violation": None, "rss_scope": RSS_SCOPE,
        "campaign_parent_pid": None, "child_pgid": None,
        "limits": {"timeout_seconds": timeout, "max_rss_mib": max_rss_mib,
                   "min_free_percent": min_free_percent,
                   "max_swap_growth_mib": max_swap_growth_mib,
                   "min_disk_free_mib": min_disk_free_mib},
        "samples": 0, "peak_parent_rss_mib": 0.0,
        "peak_child_group_rss_mib": 0.0, "peak_combined_rss_mib": 0.0,
        "peak_child_group_live_processes": 0, "rss_samples": [],
    }
    # Claim the receipt before any probes or child launch. Later atomic writes
    # update only this runner-owned path; logs remain exclusive throughout.
    _claim(output, report)
    started = time.monotonic()
    process = None
    baseline_swap = None
    witness_read = witness_write = None
    parent_pid = os.getpid()

    def sample_system():
        nonlocal baseline_swap
        free = base.memory_free_percent()
        swap = base.swapout_bytes()
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

    def sample_combined(child_group=None):
        parent, child, combined, live = _rss_probe(parent_pid, child_group)
        report["samples"] += 1
        report["peak_parent_rss_mib"] = max(report["peak_parent_rss_mib"], parent)
        report["peak_child_group_rss_mib"] = max(
            report["peak_child_group_rss_mib"], child)
        report["peak_combined_rss_mib"] = max(
            report["peak_combined_rss_mib"], combined)
        report["peak_child_group_live_processes"] = max(
            report["peak_child_group_live_processes"], live)
        report["rss_samples"].append({
            "elapsed_seconds": time.monotonic() - started,
            "parent_rss_mib": parent, "child_group_rss_mib": child,
            "combined_rss_mib": combined,
            "child_group_live_processes": live,
        })
        if parent <= 0:
            raise RuntimeError("campaign parent absent from RSS probe")
        if combined > max_rss_mib:
            raise RuntimeError(
                f"combined parent+child RSS {combined:.1f} MiB exceeded budget")

    try:
        disk_guard = base.DiskSpaceGuard(output.parent)
        sample_system()
        report["campaign_parent_pid"] = parent_pid
        sample_combined()
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            witness_read, witness_write = os.pipe()
            token = os.urandom(base.GUARD_WITNESS_BYTES)
            require(os.write(witness_write, token) == len(token),
                    "failed to issue complete resource-guard witness")
            os.close(witness_write)
            witness_write = None
            child_env = os.environ.copy()
            child_env.update({base.GUARD_FD_ENV: str(witness_read),
                base.GUARD_TOKEN_ENV: token.hex(),
                base.GUARD_OWNER_PID_ENV: str(parent_pid)})
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr,
                start_new_session=True, env=child_env, pass_fds=(witness_read,))
            os.close(witness_read)
            witness_read = None
            report["child_pgid"] = process.pid
            next_system = next_report = 0.0
            while process.poll() is None:
                now = time.monotonic()
                if now - started > timeout:
                    raise RuntimeError(f"command exceeded {timeout}s timeout")
                sample_combined(process.pid)
                if now >= next_system:
                    sample_system()
                    next_system = now + 1
                if now >= next_report:
                    report["elapsed_seconds"] = now - started
                    base.write_json_atomic(output, report)
                    next_report = now + 10
                time.sleep(interval)
            report["returncode"] = process.returncode
            sample_combined(process.pid)
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
                base.terminate_group(process)
            except Exception as exc:
                report["cleanup_error"] = f"{type(exc).__name__}: {exc}"
                report["pass"] = False
            try:
                remaining, live = base.wait_group_drained(process.pid)
                report["remaining_child_group_rss_mib"] = remaining
                report["remaining_child_group_live_processes"] = live
                if remaining != 0 or live != 0:
                    report["cleanup_error"] = "live child process-group address space remains"
                    report["pass"] = False
            except Exception as exc:
                report["cleanup_error"] = f"{type(exc).__name__}: {exc}"
                report["pass"] = False
            try:
                sample_system()
                sample_combined(None)
            except Exception as exc:
                if report["violation"] is None:
                    report["violation"] = f"{type(exc).__name__}: {exc}"
                report["pass"] = False
            report["returncode"] = process.returncode
        report["elapsed_seconds"] = time.monotonic() - started
        base.write_json_atomic(output, report)
    return report
