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

"""Regenerate/check every standalone CUDA image from the shared native catalog.

Run tile-kernel-codegen first. Compilation is offline; no CUDA context is used.
Scratch and compiler caches stay inside the selected worktree.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def check_images(specs, generated, installed, write=False):
    results = []
    pending = []
    for spec in specs:
        name = spec["file"]
        if Path(name).name != name or not name.endswith(".ptx"):
            raise ValueError("Invalid native image path: " + name)
        data = (generated / name).read_bytes()
        expected = set(spec.get("exports") or [spec["entry"]])
        entries = set(re.findall(rb"\.visible\s+\.entry\s+(\w+)\s*\(", data))
        if entries != {name.encode() for name in expected}:
            raise ValueError("Native image exports changed: " + name)
        current = installed / name
        identical = current.is_file() and current.read_bytes() == data
        if write and not identical:
            pending.append((current, data))
        results.append(dict(file=name, identical=identical, sha256=hashlib.sha256(data).hexdigest()))
    for current, data in pending:
        temporary = current.with_suffix(".ptx.tmp")
        temporary.write_bytes(data)
        temporary.replace(current)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument("--nvcc", default="/usr/local/cuda-13.0/bin/nvcc")
    parser.add_argument("--libdevice", default="/usr/local/cuda-13.0/nvvm/libdevice/libdevice.10.bc")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--family", action="append", help="Check only these exact operation families")
    args = parser.parse_args()
    root = args.root.resolve()
    cuda = root / "zig/pkg/inference/src/ops/cuda"
    specs = json.loads((cuda / "generated/standalone_programs.json").read_text())
    if args.family:
        unknown = set(args.family) - {spec["family"] for spec in specs}
        if unknown:
            parser.error("Unknown native program families: " + ", ".join(sorted(unknown)))
        specs = [spec for spec in specs if spec["family"] in args.family]
    version = subprocess.check_output([args.nvcc, "--version"], text=True)
    if "V13.0.88" not in version:
        raise RuntimeError("Standalone regeneration requires NVCC 13.0.88")
    cache = root / ".zig-cache/cuda-standalone"
    cache.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="generate-", dir=cache) as temporary:
        output = Path(temporary)
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", TMPDIR=str(output),
                   TRITON_CACHE_DIR=str(cache / "triton"), CUDA_CACHE_PATH=str(cache / "cuda"))
        producers = {}
        for spec in specs:
            producers.setdefault(spec["producer"], []).append(spec)
        for producer, profiles in producers.items():
            source = cuda / "prototypes" / producer
            if source.suffix == ".cu":
                spec = profiles[0]
                subprocess.run([args.nvcc, "--ptx", "-arch=compute_" + str(spec["target"]), "-O3",
                                *spec["nvcc_flags"], str(source), "-o", str(output / spec["file"])],
                               env=env, check=True)
                p = output / spec["file"]
                p.write_bytes(p.read_bytes().rstrip() + b"\n")
            else:
                command = [sys.executable, "-B", str(source), "--output", str(output)]
                if producer == "projection_gelu_bf16.py":
                    command += ["--libdevice", args.libdevice]
                subprocess.run(command, env=env, check=True)
        rows = check_images(specs, output, cuda, args.write)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(rows, indent=2) + "\n")
        for row in rows:
            print(row["file"], "identical" if row["identical"] else "updated" if args.write else "STALE")
        return 0 if args.write or all(row["identical"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
