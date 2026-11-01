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

"""Prove provider/consumer cache isolation with real production-operation probes.

Source mutations live in a private overlay inside the worktree's Zig cache.
This checks artifact reuse, not whole-product release performance.
"""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time

ZIG_ROOT = Path(__file__).resolve().parents[1]


def link_children(source, destination):
    destination.mkdir()
    for child in source.iterdir():
        if child.name not in {
            ".git",
            ".worktrees",
            ".zig-cache",
            ".zig-global-cache",
            "zig-out",
        }:
            (destination / child.name).symlink_to(
                child, target_is_directory=child.is_dir()
            )


def own(root, relative):
    path = root
    for part in Path(relative).parts[:-1]:
        path /= part
        if path.is_symlink():
            original = path.resolve()
            path.unlink()
            link_children(original, path)
    path = root / relative
    if path.is_symlink():
        original = path.resolve()
        path.unlink()
        shutil.copyfile(original, path)
    return path


def compile_state(output, kind, name):
    match = re.search(
        rf"compile {kind} {re.escape(name)} debug \S+ (cached|success)\b", output
    )
    if not match:
        raise RuntimeError(f"missing compiler result for {name}; inspect retained log")
    return match[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family", choices=("index", "inference"), default="inference")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    name = "index" if args.family == "index" else "fused_cpu"
    step = (
        "index-owner-probe-test" if name == "index" else "inference-kernel-probe-test"
    )
    provider_path = (
        "pkg/antfly/src/index_owner_probe.zig"
        if name == "index"
        else "pkg/inference/src/fused_cpu_probe.zig"
    )
    consumer_path = (
        "tools/fixtures/index_owner_probe_client.zig"
        if name == "index"
        else "tools/fixtures/fused_cpu_probe_client.zig"
    )
    cache = ZIG_ROOT / ".zig-cache" / "architecture-probe-overlays"
    cache.mkdir(parents=True, exist_ok=True)
    report = {
        "family": args.family,
        "scope": "production-operation probe cache isolation",
        "passed": False,
        "phases": [],
    }
    with tempfile.TemporaryDirectory(dir=cache) as directory:
        root = Path(directory) / "zig"
        link_children(ZIG_ROOT, root)
        # Build caches must not resolve through overlay symlinks.
        own(root, provider_path)
        implementation = own(
            root,
            (
                "pkg/antfly-embedded/src/storage/db/algebraic/index.zig"
                if name == "index"
                else "pkg/inference/src/ops/rope_table.zig"
            ),
        )
        consumer = own(root, consumer_path)
        contract = own(root, "build_support/architecture_probe_abi.zig")
        for phase in ("initial", "warm", "consumer", "provider", "contract"):
            if phase == "consumer":
                consumer.write_text(
                    consumer.read_text() + '\ntest "isolated consumer mutation" {}\n'
                )
            elif phase == "provider":
                implementation.write_text(
                    implementation.read_text()
                    + "\n// Isolated provider cache mutation.\n"
                )
            elif phase == "contract":
                contract.write_text(
                    contract.read_text() + "\n// Isolated ABI cache mutation.\n"
                )
            print(f"{args.family}: {phase}", flush=True)
            started = time.monotonic()
            command = [
                "python3",
                "tools/run_bounded_zig_build.py",
                "--",
                "build",
                step,
                "-Doptimize=debug",
                "-Dmetal=false",
                "-Dcuda=false",
                "--summary",
                "all",
            ]
            env = dict(
                os.environ,
                ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
                ZIG_GLOBAL_CACHE_DIR=str(ZIG_ROOT / ".zig-cache" / "global"),
            )
            process = subprocess.Popen(
                command,
                cwd=root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            try:
                output, _ = process.communicate(timeout=900)
            except BaseException:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate()
                raise
            log = report_path.with_name(report_path.stem + f"-{phase}.log")
            log.write_text(output)
            row = {
                "phase": phase,
                "elapsed_s": time.monotonic() - started,
                "returncode": process.returncode,
                "log": str(log),
            }
            report["phases"].append(row)
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            if process.returncode:
                raise RuntimeError(f"build failed: {log}")
            if re.search(
                r"compile lib antfly-(storage-engine|inference-core|runtime-enrichment_compute)\b",
                output,
            ):
                raise RuntimeError(
                    f"probe silently pulled in a whole product owner: {log}"
                )
            row["provider"] = compile_state(output, "lib", "antfly-probe-" + name)
            row["consumer"] = compile_state(
                output, "test_obj", name + "-owner-probe-tests"
            )
            expected = {
                "warm": ("cached", "cached"),
                "consumer": ("cached", "success"),
                "provider": ("success", "cached"),
                "contract": ("success", "success"),
            }.get(phase)
            if expected and (row["provider"], row["consumer"]) != expected:
                raise RuntimeError(f"cache isolation failed in {phase}: {row}")
            report_path.write_text(json.dumps(report, indent=2) + "\n")
    report["passed"] = True
    report_path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
