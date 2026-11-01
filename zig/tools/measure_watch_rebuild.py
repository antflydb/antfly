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

"""Measure an actual source edit in persistent Zig watch, in a private overlay.

Run ordinary and incremental modes separately with the same toolchain and step.
This changes the aggregation budget in the overlay, executes the real focused
planner tests, and retains logs. It never edits authored sources. A build failure
or missing successful rebuild is an error, not a zero-second measurement.
"""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import selectors
import signal
import subprocess
import tempfile
import time

from check_architecture_probe_cache import link_children, own

ZIG_ROOT = Path(__file__).resolve().parents[1]
SUMMARY = re.compile(r"Build Summary: (\d+)/(\d+) steps succeeded[^\n]*")


def successful_summary(output):
    matches = list(SUMMARY.finditer(output))
    if not matches:
        return False
    last = matches[-1]
    if last[1] != last[2] or re.search(r"\b[1-9]\d* failed\b", last[0]):
        raise RuntimeError("watch build failed: " + last[0])
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("ordinary", "incremental"), required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--build-file", default="build.focused.zig")
    parser.add_argument("--family", choices=("paging", "aggregation"), default="paging")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--zig", default="zig")
    args = parser.parse_args()
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = dict(
        mode=args.mode,
        platform=platform.platform(),
        zig=subprocess.check_output([args.zig, "version"], text=True).strip(),
        passed=False,
        phases=[],
        scope="query plan source edit; not whole-product cold time",
        family=args.family,
    )
    base = ZIG_ROOT / ".zig-cache" / "watch-overlays"
    base.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(dir=base) as directory:
            workspace = Path(directory) / "workspace"
            link_children(ZIG_ROOT.parent, workspace)
            # A symlinked build entry can canonicalize b.path() to the authored
            # tree instead of this overlay. Own both entry files explicitly.
            own(workspace, "zig/build.zig")
            own(
                workspace,
                args.build_file
                if args.build_file.startswith("zig/")
                else "zig/" + args.build_file,
            )
            root_name = (
                "query_paging_test_root"
                if args.family == "paging"
                else "aggregation_plan_test_root"
            )
            own(workspace, "zig/pkg/antfly-embedded/src/" + root_name + ".zig")
            source = own(
                workspace,
                "zig/pkg/antfly-embedded/src/storage/db/query/paging_plan.zig",
            )
            qualification = (
                "query paging plan" if args.family == "paging" else "aggregation domain"
            )
            source.write_text(
                source.read_text()
                + '\ntest "'
                + qualification
                + ' watch qualification" { @import("std").debug.print("\\nANTFLY_WATCH_BUDGET={d}\\n", .{default_aggregation_full_result_budget}); }\n'
            )
            root = workspace / "zig"
            flags = [
                "build",
                "--build-file",
                args.build_file,
                "query-paging-test"
                if args.family == "paging"
                else "aggregation-plan-test",
                "-fincremental" if args.mode == "incremental" else "-fno-incremental",
                "--watch",
                "--summary",
                "all",
                "-Doptimize=debug",
            ]
            command = [
                "python3",
                "tools/run_bounded_zig_build.py",
                "--zig",
                args.zig,
                "--",
                *flags,
            ]
            report["command"] = command
            global_cache = Path(
                os.environ.get(
                    "ZIG_GLOBAL_CACHE_DIR", ZIG_ROOT / ".zig-cache" / "global"
                )
            ).resolve()
            if not global_cache.is_relative_to(ZIG_ROOT / ".zig-cache"):
                raise RuntimeError(
                    "watch benchmark global cache must be inside the worktree .zig-cache"
                )
            process = subprocess.Popen(
                command,
                cwd=root,
                env=dict(
                    os.environ,
                    ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
                    ZIG_GLOBAL_CACHE_DIR=str(global_cache),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            log = bytearray()
            try:
                for phase in ("initial", "edited"):
                    started = time.monotonic()
                    if phase == "edited":
                        text = source.read_text()
                        original = "pub const default_aggregation_full_result_budget: u32 = 100_000;"
                        if text.count(original) != 1:
                            raise RuntimeError(
                                "planner budget marker changed; refusing an unmeasured edit"
                            )
                        source.write_text(
                            text.replace(
                                original, original.replace("100_000", "100_001")
                            )
                        )
                    phase_start = len(log)
                    while True:
                        events = selector.select(timeout=0.25)
                        for key, _ in events:
                            block = os.read(key.fileobj.fileno(), 65536)
                            if block:
                                log.extend(block)
                        output = re.sub(
                            r"\x1b\[[0-9;]*[A-Za-z]",
                            "",
                            log[phase_start:].decode(errors="replace"),
                        )
                        marker = "ANTFLY_WATCH_BUDGET=" + (
                            "100000" if phase == "initial" else "100001"
                        )
                        position = output.rfind(marker)
                        if position >= 0 and successful_summary(output[position:]):
                            break
                        if process.poll() is not None:
                            raise RuntimeError(
                                f"watch process exited {process.returncode}"
                            )
                        if time.monotonic() - started > args.timeout:
                            raise RuntimeError(f"watch phase {phase} timed out")
                    elapsed = time.monotonic() - started
                    report["phases"].append(dict(phase=phase, elapsed_s=elapsed))
                    print(phase, f"{elapsed:.3f}s", flush=True)
                    # The summary is emitted before the runner finishes arming
                    # filesystem watches. Synchronize via a short settling window.
                    if phase == "initial":
                        time.sleep(1)
                report["passed"] = True
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                selector.close()
                log_path = report_path.with_suffix(".log")
                log_path.write_bytes(log)
                report["log"] = str(log_path)
    except BaseException as error:
        report["error"] = str(error)
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
