#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Build the matching Debug CLI and require all four real lifetime campaigns."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {"sql-0362", "sql-0363", "sql-0368", "sql-0569"}
TEST = "test_unlogged_originals_preserve_clean_rows_and_reset_real_process_loss"


def require_four_passes(report):
    root = ET.parse(report).getroot()
    cases = list(root.iter("testcase"))
    if len(cases) != 4:
        raise ValueError(f"expected exactly four process campaigns, found {len(cases)}")
    observed = set()
    for case in cases:
        match = re.fullmatch(
            re.escape(TEST) + r"\[(sql-[0-9]{4})\]", case.get("name", "")
        )
        if match is None or match[1] not in EXPECTED or match[1] in observed:
            raise ValueError("unexpected or duplicate process campaign identity")
        if any(case.find(tag) is not None for tag in ("failure", "error", "skipped")):
            raise ValueError(f"process campaign did not pass: {match[1]}")
        observed.add(match[1])
    if observed != EXPECTED:
        raise ValueError("missing required process campaign")
    return sorted(observed)


def run_bounded(command, *, cwd, env, timeout):
    if os.name != "posix":
        raise ValueError("lifetime qualification requires POSIX process-group cleanup")
    child = subprocess.Popen(command, cwd=cwd, env=env, start_new_session=True)
    try:
        code = child.wait(timeout=timeout)
        if code:
            raise subprocess.CalledProcessError(code, command)
    finally:
        # Own group only, including descendants left after a failed runner exits.
        # SIGTERM from the enclosing ledger also unwinds through this cleanup.
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    compiler = parser.add_mutually_exclusive_group(required=True)
    compiler.add_argument(
        "--zig", help="Explicit Zig executable/path for the matching Debug build"
    )
    compiler.add_argument(
        "--reuse-built-cli",
        type=Path,
        help="Explicit already-qualified matching Debug CLI; caller retains build provenance",
    )
    args = parser.parse_args()
    env = dict(os.environ, TZ="UTC")
    # Never inherit user-supplied selection or parallelism into this exact gate.
    env.pop("PYTEST_ADDOPTS", None)
    # Both stages are bounded; the ledger adds an enclosing 3600-second limit.
    if args.zig:
        found = shutil.which(args.zig)
        if found is None:
            parser.error("explicit Zig executable was not found")
        zig = Path(found).resolve(strict=True)
        print(f"Building matching Debug CLI with explicit Zig: {zig}", flush=True)
        run_bounded(
            [str(zig), "build", "antfly", "-Doptimize=Debug", "-j1"],
            cwd=ROOT / "zig",
            env=env,
            timeout=1500,
        )
        binary = (ROOT / "zig/zig-out/bin/antfly").resolve(strict=True)
    else:
        binary = args.reuse_built_cli.resolve(strict=True)
        print(f"Reusing explicitly supplied matching Debug CLI: {binary}", flush=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("matching CLI is not an executable file")
    env["ANTFLY_BIN"] = str(binary)
    # A fresh private report cannot accidentally certify an earlier run.
    with tempfile.TemporaryDirectory(prefix="antfly-native-lifetime-") as directory:
        report = Path(directory) / "processes.xml"
        run_bounded(
            [
                "uv",
                "run",
                "--offline",
                "--project",
                "e2e/antfly",
                "--locked",
                "python",
                "-m",
                "pytest",
                "e2e/antfly/test_sql_unlogged_lifecycle.py",
                "-q",
                "-s",
                "-n",
                "0",
                "--maxfail=1",
                "--junitxml=" + str(report),
            ],
            cwd=ROOT / "zig",
            env=env,
            timeout=1800,
        )
        observed = require_four_passes(report)
        # Preserve the actual checked report digest and identities in the
        # enclosing qualification log before the private report is removed.
        print(
            json.dumps(
                {
                    "native_lifetime_process_provenance": {
                        "junit_sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
                        "case_ids": observed,
                        "tests": len(observed),
                        "failures": 0,
                        "errors": 0,
                        "skipped": 0,
                    }
                },
                sort_keys=True,
            ),
            flush=True,
        )
    print(
        "Matching Debug CLI: four real native lifetime process campaigns passed, no skips."
    )


if __name__ == "__main__":

    def interrupt(signum, _frame):
        raise KeyboardInterrupt(signum)

    previous = {
        signum: signal.signal(signum, interrupt)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        main()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
