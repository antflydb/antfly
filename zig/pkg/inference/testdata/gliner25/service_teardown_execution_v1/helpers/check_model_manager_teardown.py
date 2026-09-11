#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""No-model process proof for the manager-owned close watchdog.

Build the repository's inference test executable with the filter
`model manager teardown`, then pass its frozen path and exact SHA-256.
This runner never builds, downloads, loads models, or retries a failed case.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time

MODES = ("cache", "ttl", "admission", "retired", "shutdown", "rollback", "escaped")
TEST = "model manager teardown supervised child fixture"
TIMEOUT_SECONDS = 5
STREAM_BYTES = 1024 * 1024


def digest(path):
    value = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            size += len(block)
            value.update(block)
    return {"size_bytes": size, "sha256": value.hexdigest()}


def run_case(binary, output, mode):
    environment = os.environ.copy()
    for key in tuple(environment):
        if key.startswith("ANTFLY_INFERENCE_TEST_") or key == "ANTFLY_TEST_MANAGER_TEARDOWN_CHILD":
            del environment[key]
    environment["ANTFLY_TEST_MANAGER_TEARDOWN_CHILD"] = mode
    argv = [str(binary), "--test-filter", TEST]
    stdout_path = output / f"{mode}.stdout.log"
    stderr_path = output / f"{mode}.stderr.log"
    process = None
    failure = None
    started = time.monotonic()
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        try:
            process = subprocess.Popen(argv, env=environment, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
            while process.poll() is None:
                if time.monotonic() - started >= TIMEOUT_SECONDS:
                    failure = "outer_timeout"
                    break
                if max(os.fstat(stdout.fileno()).st_size, os.fstat(stderr.fileno()).st_size) > STREAM_BYTES:
                    failure = "stream_limit"
                    break
                time.sleep(0.01)
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
    elapsed = time.monotonic() - started
    if max(stdout_path.stat().st_size, stderr_path.stat().st_size) > STREAM_BYTES:
        failure = failure or "stream_limit"
    # Read only a bounded prefix even on an excessive-output failure.
    with stderr_path.open("rb") as source:
        error_bytes = source.read(STREAM_BYTES)
    close_lease_entries = error_bytes.count(b"teardown-fixture close-entered lease-held64\n")
    close_no_lease_entries = error_bytes.count(b"teardown-fixture close-entered no-lease\n")
    close_entries = close_lease_entries + close_no_lease_entries
    cache_entries = error_bytes.count(b"teardown-fixture cache-entered primary-and-optional-active lease-held64\n")
    if mode == "cache":
        entered = cache_entries == 1 and close_entries == 0
    elif mode == "escaped":
        entered = close_no_lease_entries == 1 and close_lease_entries == 0 and cache_entries == 0
    else:
        entered = close_lease_entries == 1 and close_no_lease_entries == 0 and cache_entries == 0
    started_expected_operation = error_bytes.count(f"teardown-fixture operation-start:{mode}\n".encode()) == 1
    operation_error = b"teardown-fixture operation-error:" in error_bytes
    cleanup_returned = b"teardown-fixture cleanup-returned" in error_bytes
    watchdog = b"uninterruptible inference request expired" in error_bytes and b"err=Timeout" in error_bytes
    success = failure is None and process.returncode == 86 and entered and started_expected_operation and not operation_error and watchdog and not cleanup_returned
    return {
        "mode": mode, "argv": argv, "elapsed_seconds": elapsed,
        "exit_code": process.returncode, "outer_failure": failure,
        "expected_destructor_entered": entered,
        "expected_admission_state": "no_lease" if mode == "escaped" else "lease_held64",
        "lease_order_evidence": success and mode != "escaped",
        "session_close_lease_held64_entries": close_lease_entries,
        "session_close_no_lease_entries": close_no_lease_entries,
        "session_close_entries": close_entries, "cache_destruction_entries": cache_entries,
        "cleanup_returned": cleanup_returned,
        "started_expected_operation": started_expected_operation, "operation_error": operation_error,
        "watchdog_timeout_reported": watchdog, "reaped": process.poll() is not None,
        "stdout": digest(stdout_path), "stderr": digest(stderr_path), "pass": success,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.absolute()
    if not stat.S_ISREG(binary.lstat().st_mode):
        parser.error("binary must be a regular frozen file")
    initial = digest(binary)
    if initial["sha256"] != args.binary_sha256:
        parser.error("binary SHA-256 mismatch")
    args.output_dir.mkdir(mode=0o700)
    report = {
        "version": 1, "scope": "model_manager_bounded_teardown/no_model_v1",
        "qualification": False, "binary": initial,
        "driver": digest(Path(__file__)),
        "limits": {"child_seconds": TIMEOUT_SECONDS, "stream_bytes": STREAM_BYTES, "reap_seconds": 5},
        "expected_watchdog_exit": 86, "cases": [], "pass": False,
    }
    try:
        for mode in MODES:
            result = run_case(binary, args.output_dir, mode)
            report["cases"].append(result)
            if not result["pass"]:
                break
        report["binary_unchanged"] = digest(binary) == initial
        report["pass"] = report["binary_unchanged"] and len(report["cases"]) == len(MODES) and all(case["pass"] for case in report["cases"])
    except BaseException as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        with (args.output_dir / "report.json").open("x") as target:
            json.dump(report, target, indent=2, sort_keys=True)
            target.write("\n")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
