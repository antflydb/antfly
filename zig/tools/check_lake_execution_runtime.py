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

"""Measure source/native lake attempts in a verified compilation mode."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import time

ZIG = Path(__file__).resolve().parents[1]
FAMILIES = (
    "identity",
    "fragments",
    "spool",
    "aggregate",
    "ordered",
    "text",
    "sparse",
    "dense",
)
PHASES = ("cold", "checkpoint_resume", "incremental_append")


def observations(
    log,
    rounds,
    rows,
    quota_layers=2,
    io_dispatch="linear",
    headroom="auto",
    optimize="debug",
):
    found = [
        json.loads(line.split("LAKE_RUNTIME ", 1)[1])
        for line in log.splitlines()
        if "LAKE_RUNTIME " in line
    ]
    expected = {
        (mode, phase, run)
        for mode in ("source", "native")
        for phase in PHASES
        for run in range(rounds)
    }
    keys = [(x["mode"], x["phase"], x["round"]) for x in found]
    if len(keys) != len(set(keys)) or set(keys) != expected:
        raise ValueError("incomplete or duplicate runtime observations")
    for item in found:
        provider_mode = {"debug": 0, "fast": 1, "safe": 2}[optimize]
        if item["source_optimize"] != optimize or (
            item["mode"] == "native" and item["provider_optimize"] != provider_mode
        ):
            raise ValueError("source/provider optimization mode mismatch")
        if item["headroom_mode"] != headroom or (
            item["mode"] == "native" and item["headroom_queries"] <= 0
        ):
            raise ValueError("missing creator headroom query evidence")
        if item["family_names"] != list(FAMILIES) or any(
            len(item[key]) != len(FAMILIES)
            for key in ("family_ns", "family_calls", "family_declaration_root_bytes")
        ):
            raise ValueError("incomplete physical family evidence")
        if any(item["family_calls"][index] <= 0 for index in range(3, len(FAMILIES))):
            raise ValueError("missing physical writer family")
        if item["io_dispatch"] != io_dispatch:
            raise ValueError("incorrect IO dispatch mode")
        if item["mode"] == "native" and item["caller_io_dispatch_calls"] <= 0:
            raise ValueError("no foreign executor dispatch evidence")
        if item["working_quota_layers"] != (
            quota_layers if item["mode"] == "native" else 1
        ):
            raise ValueError("incorrect quota layer mode")
        if item["source_is_test"] or item["provider_is_test"]:
            raise ValueError(
                "performance evidence requires non-test source and provider builds"
            )
        if item["declarations"] != 5 or item["rows"] != rows * (
            5 if item["phase"] == "incremental_append" else 4
        ):
            raise ValueError("incorrect probe workload")
        if item["api_source_open_ns"] + item["api_admission_ns"] != item["control_ns"]:
            raise ValueError("inconsistent admission time")
        if (
            item["mode"] == "native"
            and min(
                item[key]
                for key in (
                    "provider_store_open_ns",
                    "provider_source_open_ns",
                    "provider_physical_ns",
                )
            )
            <= 0
        ):
            raise ValueError("missing provider stage timing")
        if item["total_attempt_ns"] != item["control_ns"] + item["execution_ns"]:
            raise ValueError("inconsistent total attempt time")
        if item["execution_ns"] <= 0 or item["peak_working_set_bytes"] <= 0:
            raise ValueError("empty timing/allocation evidence")
    for phase in PHASES:
        for run in range(rounds):
            pair = {
                x["mode"]: x for x in found if x["phase"] == phase and x["round"] == run
            }
            if headroom in ("unknown", "low") and pair["native"]["parallel_jobs"]:
                raise ValueError("unproved headroom admitted parallel work")
            if (
                headroom == "auto"
                and pair["source"]["parallel_jobs"] > 1
                and pair["native"]["parallel_jobs"] <= 1
            ):
                raise ValueError(
                    "creator headroom failed to preserve parallel admission"
                )
    return found


def authored_inputs():
    files = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "zig"],
        cwd=ZIG.parent,
        text=True,
    ).splitlines()
    digest = hashlib.sha256()
    count = 0
    for relative in sorted(set(files)):
        path = ZIG.parent / relative
        # Qualification/report tool edits do not alter the measured program.
        if (
            path.suffix == ".py"
            and not path.name.startswith(("generate_", "patch_"))
            and path.name != "run_bounded_zig_build.py"
        ):
            continue
        if path.suffix == ".md":
            continue
        if not path.is_file():
            continue
        digest.update(relative.encode() + b"\0")
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        digest.update(b"\0")
        count += 1
    return {"sha256": digest.hexdigest(), "files": count}


def run_logged(command, env, log, timeout):
    started = time.monotonic()
    with log.open("w") as stream:
        with subprocess.Popen(
            command,
            cwd=ZIG,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        ) as process:
            try:
                process.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
                finally:
                    # The wrapper may exit before a compiler child that ignored
                    # SIGTERM. Reap the whole session even in that case.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                raise
    if process.returncode:
        raise RuntimeError(f"command exited {process.returncode}; inspect {log}")
    return time.monotonic() - started


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--native-quota-layers", type=int, choices=(1, 2), default=2)
    parser.add_argument("--io-dispatch", choices=("linear", "switch"), default="linear")
    parser.add_argument(
        "--headroom", choices=("auto", "unknown", "low"), default="auto"
    )
    parser.add_argument(
        "--optimize", choices=("debug", "fast", "safe"), default="debug"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1800,
        help="Probe execution timeout in seconds, excluding compilation",
    )
    parser.add_argument(
        "--build-timeout",
        type=int,
        default=5400,
        help="Cold compilation timeout in seconds",
    )
    args = parser.parse_args()
    if not 1 <= args.rows <= 4096 or not 1 <= args.rounds <= 8:
        parser.error("rows must be 1..4096 and rounds 1..8")
    if args.timeout <= 0 or args.build_timeout <= 0:
        parser.error("timeouts must be positive")
    args.report = args.report.resolve()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    log = args.report.with_suffix(".log")
    command = [
        sys.executable,
        "tools/run_bounded_zig_build.py",
        "--",
        "build",
        "lake-execution-runtime-probe-build",
        f"-Doptimize={args.optimize}",
        f"-Dstrip={str(args.optimize != 'debug').lower()}",
        "-Dcpu=baseline",
        "-Dshared-image-operations=true",
        "-Dshared-lake-execution=true",
        "-Dmetal=false",
        "-Dcuda=false",
        "-Dsystem-blas=false",
        "--summary",
        "all",
        "-j1",
    ]
    env = dict(
        os.environ,
        ANTFLY_LAKE_PROBE_ROWS=str(args.rows),
        ANTFLY_LAKE_PROBE_ROUNDS=str(args.rounds),
        ANTFLY_LAKE_PROBE_HEADROOM=args.headroom,
        ANTFLY_LAKE_PROBE_LINEAR_IO="1" if args.io_dispatch == "linear" else "0",
        ANTFLY_LAKE_PROBE_SINGLE_QUOTA="1" if args.native_quota_layers == 1 else "0",
        ZIG_LOCAL_CACHE_DIR=str(ZIG / ".zig-cache"),
        ZIG_GLOBAL_CACHE_DIR=str(ZIG / ".zig-cache/global"),
    )
    report = {
        "format_version": 2,
        "optimize": args.optimize,
        "passed": False,
        "host": platform.platform(),
        "build_command": command,
        "command": [str(ZIG / "zig-out/bin/lake-execution-runtime-probe")],
        "rows_per_file": args.rows,
        "rounds": args.rounds,
        "native_quota_layers": args.native_quota_layers,
        "io_dispatch": args.io_dispatch,
        "headroom": args.headroom,
        "scope": "Verified-mode CPU-only non-test executable and provider archive; attempt wall time with SMP backing allocator (no allocation stack tracing), alternating paired order, and charged quota peaks; API cache counters exclude provider-private cache; declaration roots do not count internal file reuse; no release SLA/default approval",
    }
    try:
        report["authored_inputs_before"] = authored_inputs()
        report["zig_version"] = subprocess.check_output(
            ["zig", "version"], text=True
        ).strip()
        report["inputs_sha256"] = {
            str(path.relative_to(ZIG)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                ZIG / "pkg/antfly/src/api/lake_execution_runtime_probe.zig",
                ZIG / "pkg/antfly/src/api/lake_execution_native_provider.zig",
                ZIG / "pkg/antfly/src/api/lake_execution_build_impl.zig",
                ZIG / "pkg/antfly/build/lake_execution_tests.zig",
                ZIG / "pkg/antfly/src/lake_execution_provider_probe_root.zig",
                ZIG / "pkg/antfly/src/lake_execution_runtime_probe_root.zig",
                ZIG / "lib/runtime/src/runtime_io_abi.zig",
            )
        }
        report["build_seconds"] = run_logged(
            command, env, args.report.with_suffix(".build.log"), args.build_timeout
        )
        report["run_seconds"] = run_logged(report["command"], env, log, args.timeout)
        report["authored_inputs_after"] = authored_inputs()
        report["returncode"] = 0
        if report["authored_inputs_before"] != report["authored_inputs_after"]:
            raise ValueError(
                "authored Zig inputs changed during measurement; rerun at a coherent checkpoint"
            )
        contents = log.read_text()
        executable_headers = [
            json.loads(line.split("LAKE_RUNTIME_EXECUTABLE ", 1)[1])
            for line in contents.splitlines()
            if "LAKE_RUNTIME_EXECUTABLE " in line
        ]
        if len(executable_headers) != 1 or executable_headers[0]["builtin_is_test"]:
            raise ValueError("missing normal executable provenance")
        if executable_headers[0]["optimize"] != args.optimize:
            raise ValueError("executable optimization mode mismatch")
        binary = Path(executable_headers[0]["path"]).resolve()
        if binary != Path(report["command"][0]).resolve():
            raise ValueError("probe provenance does not match the installed executable")
        binary_hash = hashlib.sha256()
        with binary.open("rb") as compiled:
            for block in iter(lambda: compiled.read(1024 * 1024), b""):
                binary_hash.update(block)
        report["executable"] = dict(
            path=str(binary),
            bytes=binary.stat().st_size,
            sha256=binary_hash.hexdigest(),
        )
        retained = args.report.parent / "lake-runtime-probes" / binary_hash.hexdigest()
        retained.mkdir(parents=True, exist_ok=True)
        retained_binary = retained / "lake-execution-runtime-probe"
        if not retained_binary.exists():
            shutil.copy2(binary, retained_binary)
        if (
            hashlib.sha256(retained_binary.read_bytes()).hexdigest()
            != binary_hash.hexdigest()
        ):
            raise ValueError("retained executable does not match measured program")
        report["executable"]["cache_path"] = str(binary)
        report["executable"]["path"] = str(retained_binary)
        data = observations(
            contents,
            args.rounds,
            args.rows,
            args.native_quota_layers,
            args.io_dispatch,
            args.headroom,
            args.optimize,
        )
        report["observations"] = data
        medians = {}
        for phase in PHASES:
            medians[phase] = {
                mode: {
                    metric: statistics.median(
                        x[metric]
                        for x in data
                        if x["mode"] == mode and x["phase"] == phase
                    )
                    for metric in (
                        "api_source_open_ns",
                        "api_admission_ns",
                        "provider_store_open_ns",
                        "provider_source_open_ns",
                        "provider_physical_ns",
                        "provider_cache_hits",
                        "provider_cache_reads",
                        "provider_cache_bytes",
                        "provider_quota_peak_bytes",
                        "caller_io_dispatch_calls",
                        "provider_io_dispatch_calls",
                        "api_decoded_hits",
                        "provider_decoded_hits",
                        "headroom_queries",
                        "parallel_jobs",
                        "control_ns",
                        "execution_ns",
                        "total_attempt_ns",
                        "peak_working_set_bytes",
                    )
                }
                for mode in ("source", "native")
            }
            for mode in ("source", "native"):
                medians[phase][mode]["families"] = {
                    name: {
                        metric: statistics.median(
                            x[metric][index]
                            for x in data
                            if x["mode"] == mode and x["phase"] == phase
                        )
                        for metric in (
                            "family_ns",
                            "family_calls",
                            "family_declaration_root_bytes",
                        )
                    }
                    for index, name in enumerate(FAMILIES)
                }
            medians[phase]["native_source_family_ratios"] = {
                name: medians[phase]["native"]["families"][name]["family_ns"]
                / medians[phase]["source"]["families"][name]["family_ns"]
                if medians[phase]["source"]["families"][name]["family_ns"]
                else None
                for name in FAMILIES
            }
            medians[phase]["native_source_total_attempt_ratio"] = (
                medians[phase]["native"]["total_attempt_ns"]
                / medians[phase]["source"]["total_attempt_ns"]
            )
            medians[phase]["native_source_execution_ratio"] = (
                medians[phase]["native"]["execution_ns"]
                / medians[phase]["source"]["execution_ns"]
            )
        report.update(passed=True, medians=medians)
    except (
        RuntimeError,
        ValueError,
        KeyError,
        TypeError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        report["error"] = str(error)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "passed": report["passed"],
                "report": str(args.report),
                "error": report.get("error"),
            }
        )
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
