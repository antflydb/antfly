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

"""Fetch and compile the published Apache package shape outside the repository."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/packaging"))
from package_embedded_zig_source import build  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--working-tree", action="store_true")
    parser.add_argument("--native-only", action="store_true")
    parser.add_argument("flags", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    flags = args.flags[1:] if args.flags[:1] == ["--"] else args.flags
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    with tempfile.TemporaryDirectory(prefix="antfly-zig-consumer-") as raw:
        workspace = Path(raw)
        artifacts = workspace / "artifacts"
        identity = build(ROOT, commit, "0.1.0", artifacts, args.zig, args.working_tree)
        archive = artifacts / identity["archive"]
        with tarfile.open(archive) as source:
            unpacked = sum(member.size for member in source if member.isfile())
        print(
            f"Fetched source archive: {archive.stat().st_size / 1024**2:.1f} MiB compressed, {unpacked / 1024**2:.1f} MiB unpacked",
            flush=True,
        )
        consumer = workspace / "consumer"
        shutil.copytree(ROOT / "zig/pkg/antfly-embedded/tests/zig-consumer", consumer)
        url = (
            "https://github.com/antflydb/antfly/releases/download/v0.1.0/"
            + identity["archive"]
        )
        (consumer / "build.zig.zon").write_text(
            '.{ .name = .consumer, .version = "0.1.0", .fingerprint = 0x705b37271f25a1cb, '
            ".dependencies = .{ .embedded = .{ .url = "
            + json.dumps(url)
            + ", .hash = "
            + json.dumps(identity["zig_hash"])
            + ' } }, .paths = .{ "build.zig", "build.zig.zon", "main.zig" } }\n'
        )
        steps = (
            ["test", "native"]
            if args.native_only
            else ["test", "native", "wasm", "inference-wasm32", "inference-wasm64"]
        )
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "zig/tools/run_bounded_zig_build.py"),
                "--zig",
                args.zig,
                "--",
                "build",
                *steps,
                *flags,
            ],
            cwd=consumer,
            check=True,
        )
        print(
            "Fetched Apache Zig package: native DB, SQL, inference"
            + ("" if args.native_only else " and WASM")
            + " verified"
        )


if __name__ == "__main__":
    main()
