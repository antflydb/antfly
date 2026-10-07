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

"""Build hashed Windows qualification executables using an unmodified Zig library."""

from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[2]
PLATFORM = ["-Mantfly_platform=lib/platform/src/root.zig"]
HASH = ["-Mantfly_hash=lib/hash/src/mod.zig"]
FS = ["--dep", "antfly_platform", "-Mantfly_runtime_fs=lib/runtime/src/fs.zig"]
BUDGET = [
    "--dep",
    "antfly_platform",
    "-Mantfly_cache_budget=lib/runtime/src/cache_budget.zig",
]
EMBEDDED = "pkg/antfly-embedded/src/"
RUNNER = EMBEDDED + "test_runner.zig"


def module(root, dependencies, tail):
    result = []
    for dependency in dependencies:
        result += ["--dep", dependency]
    return result + ["-Mroot=" + root] + tail


SUITES = {
    "io-namespace": (
        ["platform Io "],
        module(
            "lib/platform/tests/io_namespace_test.zig",
            ["antfly_platform", "platform_dependency"],
            PLATFORM
            + [
                "--dep",
                "antfly_platform",
                "-Mplatform_dependency=lib/platform/tests/platform_dependency_fixture.zig",
            ],
        ),
    ),
    "socket-errors": (
        ["Windows socket "],
        module("tools/windows/socket_error_test.zig", ["antfly_platform"], PLATFORM),
    ),
    "model-file": (
        ["file readers preserve positional mode"],
        module("pkg/inference/src/util/c_file.zig", ["antfly_platform"], PLATFORM),
    ),
    "compat": (
        ["Windows "],
        module("tools/windows/compat_test.zig", ["antfly_platform"], PLATFORM),
    ),
    "hardlink": (
        ["Windows "],
        module("tools/windows/hardlink_test.zig", ["antfly_platform"], PLATFORM),
    ),
    "backup": (
        ["storage.db.native_backup", "storage.db.snapshot_staging"],
        module(
            EMBEDDED + "windows_backup_test.zig",
            [
                "antfly_source_root=root",
                "antfly_test_error_logs",
                "antfly_hash",
                "antfly_platform",
                "antfly_runtime_fs",
                "antfly_cancellation",
                "antfly_cache_budget",
            ],
            HASH
            + PLATFORM
            + FS
            + [
                "--dep",
                "antfly_platform",
                "-Mantfly_cancellation=lib/runtime/src/cancellation.zig",
            ]
            + BUDGET
            + ["-Mantfly_test_error_logs=" + EMBEDDED + "test_error_logs.zig"],
        ),
    ),
    "filesystem": (
        ["filesystem"],
        module("lib/objectstore/src/filesystem.zig", ["antfly_platform"], PLATFORM),
    ),
    "lite": (
        [
            "storage.lite.index_storage.",
            "lite native streaming vacuum rejects corrupt values before publication",
        ],
        module(
            EMBEDDED + "windows_lite_index_test.zig",
            [
                "antfly_hash",
                "antfly_platform",
                "antfly_runtime_fs",
                "antfly_cache_budget",
            ],
            HASH + PLATFORM + FS + BUDGET,
        ),
    ),
    "storage": (
        ["storage_io."],
        module(
            EMBEDDED + "windows_storage_test.zig",
            ["antfly_hash", "antfly_platform", "antfly_runtime_fs"],
            HASH + PLATFORM + FS,
        ),
    ),
    "bridge": (
        ["borrowed executor wakes idle owning workers across archives"],
        module(
            "tools/windows/executor_bridge_host.zig",
            ["antfly_platform", "antfly_executor_abi"],
            PLATFORM + ["-Mantfly_executor_abi=lib/runtime/src/runtime_io_abi.zig"],
        ),
    ),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--target",
        choices=["native", "x86_64-windows-gnu"],
        default="x86_64-windows-gnu",
    )
    parser.add_argument("--mode", choices=["Debug", "ReleaseFast"], action="append")
    parser.add_argument("--suite", choices=list(SUITES), action="append")
    args = parser.parse_args()
    executable = shutil.which(args.zig)
    if executable is None:
        parser.error("Zig executable not found")
    zig = Path(executable).resolve()
    library = zig.parent / "lib"
    if not (library / "std" / "std.zig").is_file():
        parser.error(
            "expected the unmodified lib directory alongside the Zig executable"
        )
    env = os.environ.copy()
    env.pop("ZIG_LIB_DIR", None)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    env["ZIG_LOCAL_CACHE_DIR"] = str(out / "cache")
    env["ZIG_GLOBAL_CACHE_DIR"] = str(out / "global-cache")
    manifest = {
        "target": args.target,
        "zig": str(zig),
        "zig_version": subprocess.check_output(
            [str(zig), "version"], text=True
        ).strip(),
        "zig_lib": str(library),
        "stock_threaded_sha256": hashlib.sha256(
            (library / "std/Io/Threaded.zig").read_bytes()
        ).hexdigest(),
        "executables": [],
    }
    for mode in args.mode or ["Debug", "ReleaseFast"]:
        common = ["-lc", "-O", mode, "--zig-lib-dir", str(library)]
        if args.target != "native":
            common += ["-target", args.target]
        for suite in args.suite or list(SUITES):
            if args.target == "native" and suite in ("compat", "hardlink"):
                continue
            filters, dependencies = SUITES[suite]
            output = out / f"{suite}-{mode}.exe"
            command = (
                [str(zig), "test"]
                + common
                + [
                    "--test-no-exec",
                    "--test-runner",
                    RUNNER,
                    "-femit-bin=" + str(output),
                ]
            )
            if args.target == "native":
                command += ["lib/platform/src/filesystem_capacity.c"]
            for item in filters:
                command += ["--test-filter", item]
            if suite == "bridge":
                worker = out / f"executor-worker-{mode}.lib"
                subprocess.run(
                    [str(zig), "build-lib", "-static"]
                    + common
                    + [
                        "-femit-bin=" + str(worker),
                        "--dep",
                        "antfly_executor_abi",
                        "-Mroot=tools/windows/executor_bridge_worker.zig",
                        "-Mantfly_executor_abi=lib/runtime/src/runtime_io_abi.zig",
                    ],
                    cwd=ROOT,
                    env=env,
                    check=True,
                )
                command += [str(worker)]
            print("Building", output.name, flush=True)
            subprocess.run(command + dependencies, cwd=ROOT, env=env, check=True)
            manifest["executables"].append(
                {
                    "name": output.name,
                    "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                    "suite": suite,
                    "mode": mode,
                }
            )
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    with zipfile.ZipFile(out / "tests.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(out / "manifest.json", "manifest.json")
        for item in manifest["executables"]:
            archive.write(out / item["name"], item["name"])
    print("Qualification archive:", out / "tests.zip", flush=True)


if __name__ == "__main__":
    main()
