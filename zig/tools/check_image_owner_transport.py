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

"""Compare source and compiled-owner image decoding and VP8 reference tests.

Routine checks use Debug. This checks image transport equivalence, including
fixture pixels and retained VP8 reference assertions; it measures neither GPU
execution nor runtime performance. --build-log analyzes an existing complete
build log without starting another build.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
ZIG = REPO / "zig"
MODES = ("source", "native")
REQUIRED_VP8 = frozenset(
    ("vp8-inter", "vp8-version2", "vp8-version3", "vp8-partitions")
)
MIN_FIXTURES = 30
FIXTURE = re.compile(
    r"IMAGE_FIXTURE mode=(source|native) path=(\S+) hash=([a-f0-9]{64})\s*$"
)
VP8 = re.compile(
    r"IMAGE_VP8 mode=(source|native) fixture=(\S+) packets=(\d+) bytes=(\d+)\s*$"
)
SUMMARY = re.compile(r"Build Summary: (\d+)/(\d+) steps succeeded([^\n]*)")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes():
    paths = set((ZIG / "lib/image").rglob("*.zig"))
    paths.update((ZIG / "lib/video/src").glob("*.zig"))
    paths.update((ZIG / "build_support").rglob("*.zig"))
    paths.update((ZIG / "pkg/inference/build").rglob("*.zig"))
    paths.update((ZIG / "pkg/antfly/build").rglob("*.zig"))
    paths.update((ZIG / "pkg/antfly-embedded/src/storage").glob("*image*.zig"))
    paths.update((ZIG / "pkg/antfly-embedded/src/storage").glob("media_native*.zig"))
    paths.update(
        ZIG / relative
        for relative in (
            "build.zig",
            "build_support/antfly/dependencies.zig",
            "build_support/antfly/imports.zig",
            "pkg/antfly/build/storage_owner_tests.zig",
            "pkg/antfly/src/image_native_owner_test_root.zig",
            "pkg/antfly-embedded/src/storage/enrichment_compute_provider.zig",
            "testdata/image/manifest.zon",
        )
    )
    return {
        str(path.relative_to(REPO)): sha256(path)
        for path in sorted(paths)
        if path.is_file()
    }


def analyze(output):
    """Reject partial, duplicated or differing oracle observations."""
    fixtures = {}
    vp8 = {}
    errors = []
    for line_number, line in enumerate(output.splitlines(), 1):
        if "IMAGE_FIXTURE" in line:
            match = FIXTURE.search(line)
            if match is None:
                errors.append(f"Malformed image observation at line {line_number}")
                continue
            mode, name, digest = match.groups()
            row = fixtures.setdefault(name, {})
            if mode in row:
                errors.append(f"Duplicate {mode} image observation: {name}")
            else:
                row[mode] = digest
        if "IMAGE_VP8" in line:
            match = VP8.search(line)
            if match is None:
                errors.append(f"Malformed VP8 observation at line {line_number}")
                continue
            mode, name, packets, byte_count = match.groups()
            row = vp8.setdefault(name, {})
            value = dict(packets=int(packets), bytes=int(byte_count))
            if mode in row:
                errors.append(f"Duplicate {mode} VP8 observation: {name}")
            else:
                row[mode] = value
            if value["packets"] < 1 or value["bytes"] < 1:
                errors.append(f"Empty {mode} VP8 reference observation: {name}")

    fixture_counts = {
        mode: sum(mode in values for values in fixtures.values()) for mode in MODES
    }
    for mode, count in fixture_counts.items():
        if count < MIN_FIXTURES:
            errors.append(f"{mode} has {count} image fixtures; need {MIN_FIXTURES}")
    for name, values in fixtures.items():
        if any(mode not in values for mode in MODES):
            errors.append(f"Missing source/native image counterpart: {name}")
        elif values["source"] != values["native"]:
            errors.append(f"Image pixels differ: {name}")
    for mode in MODES:
        observed = {name for name, values in vp8.items() if mode in values}
        for missing in sorted(REQUIRED_VP8 - observed):
            errors.append(f"Missing {mode} VP8 reference sequence: {missing}")
    for name, values in vp8.items():
        if any(mode not in values for mode in MODES):
            errors.append(f"Missing source/native VP8 counterpart: {name}")
        elif values["source"] != values["native"]:
            errors.append(f"VP8 packet/checked-byte counts differ: {name}")

    summaries = list(SUMMARY.finditer(output))
    if not summaries:
        errors.append("Missing completed build summary")
    elif len(summaries) != 1:
        errors.append("Multiple build summaries; provide one qualification run")
    elif summaries[0][1] != summaries[0][2] or "failed" in summaries[0][3]:
        errors.append("Build/check steps did not all succeed")
    if re.search(
        r"\bFAIL\s*\(|error: process exited|error: \d+ compilation errors", output
    ):
        errors.append("Build log contains a test or compiler failure")
    return dict(
        fixtures=fixtures,
        fixture_counts=fixture_counts,
        vp8=vp8,
        vp8_counts={mode: sum(mode in row for row in vp8.values()) for mode in MODES},
        errors=errors,
        passed=not errors,
    )


def command(optimize):
    return [
        sys.executable,
        "tools/run_bounded_zig_build.py",
        "--",
        "build",
        "compiled-image-native-test",
        "-Dshared-image-operations=true",
        f"-Doptimize={optimize}",
        f"-Dstrip={str(optimize != 'debug').lower()}",
        "-Dcpu=baseline",
        "-Dmetal=false",
        "-Dcuda=false",
        "-Dsystem-blas=false",
        "--summary",
        "all",
    ]


def run_build(args, log_path, timeout):
    started = time.monotonic()
    failure = None
    process = None
    with log_path.open("wb") as log:
        try:
            process = subprocess.Popen(
                args,
                cwd=ZIG,
                env=dict(
                    os.environ,
                    ZIG_LOCAL_CACHE_DIR=str(ZIG / ".zig-cache"),
                    ZIG_GLOBAL_CACHE_DIR=str(ZIG / ".zig-cache/global"),
                ),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
            failure = type(exc).__name__
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
        except OSError as exc:
            failure = f"{type(exc).__name__}: {exc}"
    return dict(
        returncode=process.returncode if process is not None else None,
        elapsed_s=time.monotonic() - started,
        interruption=failure,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--build-log", type=Path, help="Analyze one existing build log")
    parser.add_argument(
        "--build-exit-code", type=int, help="Known exit code of --build-log's build"
    )
    parser.add_argument(
        "--optimize", choices=("debug", "fast", "safe"), default="debug"
    )
    parser.add_argument("--timeout", type=int, default=1200)
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("timeout must be positive")
    if args.build_exit_code is not None and args.build_log is None:
        parser.error("--build-exit-code requires --build-log")
    report_path = args.report.resolve()
    if args.build_log is not None and report_path == args.build_log.resolve():
        parser.error("--report must differ from --build-log")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = dict(
        format_version=1,
        scope="Exact source/native decoded-image pixels and retained VP8 reference assertions; GPU execution and runtime performance are not measured",
        base_git_revision=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip(),
        script_sha256=sha256(Path(__file__)),
        optimize=args.optimize,
        passed=False,
    )
    before = source_hashes()
    if args.build_log is not None:
        log_path = args.build_log.resolve()
        report.update(
            execution="existing_log",
            source_hashes_at_analysis=before,
            source_provenance="Current tree at analysis time; this does not establish the source snapshot used by the supplied log",
            optimize_provenance="argument; not independently verified in supplied log",
            returncode=args.build_exit_code,
            returncode_provenance="provided"
            if args.build_exit_code is not None
            else "not supplied; completed build summary is required",
        )
    else:
        log_path = report_path.with_suffix(".build.log")
        cmd = command(args.optimize)
        report.update(execution="build", command=cmd, initial_sources_sha256=before)
        report.update(run_build(cmd, log_path, args.timeout))
    try:
        output = log_path.read_text(errors="replace")
        report.update(analyze(output))
        report.update(build_log=str(log_path), build_log_sha256=sha256(log_path))
    except OSError as exc:
        report.update(errors=[f"Cannot read build log: {exc}"], passed=False)
    if report.get("returncode") not in (None, 0):
        report["errors"].append(f"Build exited with {report['returncode']}")
    if report.get("interruption"):
        report["errors"].append(report["interruption"])
    if args.build_log is None:
        after = source_hashes()
        report["final_sources_sha256"] = after
        if before != after:
            report["errors"].append(
                "Relevant source files changed during qualification"
            )
    report["passed"] = not report["errors"]
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                key: report.get(key)
                for key in (
                    "passed",
                    "fixture_counts",
                    "vp8_counts",
                    "returncode",
                    "errors",
                )
            },
            sort_keys=True,
        )
    )
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
