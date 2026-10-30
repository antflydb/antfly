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

"""Freeze the original raw-weight Metal ternary FFN owner and wrappers.

Run from the worktree root: --output <evidence directory>/baseline.h.
Compile check_metal_host_ffn.m using clang -O3 -fobjc-arc with
-DANTFLY_TERNARY_FFN_BASELINE_HEADER='"<absolute output path>"', linking Foundation,
Metal, MetalPerformanceShaders and Accelerate. No arguments runs parity checks;
--benchmark-ternary-gpu and --benchmark-ternary-host emit separate paired samples.
Declare the shared-host/quiet-host protocol and acceptance limits before timing.
This script does not run or retry measurements.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def prepare(root, revision):
    revision = subprocess.check_output(["git", "rev-parse", "--verify", revision + "^{commit}"], cwd=root, text=True).strip()
    source = subprocess.check_output(["git", "show", revision + ":zig/pkg/inference/src/backends/metal_kernels.m"], cwd=root).decode()
    names = ["termite_metal_apply_host_ternary_ffn"] + [
        "termite_metal_decode_runtime_apply_gated_ffn_residual_" + codec for codec in ("tl1", "tl2")]
    renamed = ("aw_before_owner", "at_before_tl1", "at_before_tl2")
    functions, hashes = [], {}
    for name in names:
        start = source.index("int " + name + "(")
        start = source.rfind("\n", 0, start) + 1
        end = source.index("\n}\n", start) + 3
        body = source[start:end]
        hashes[name] = hashlib.sha256(body.encode()).hexdigest()
        body = body.replace("__func__", '"' + name + '"')
        for old, new in zip(names, renamed):
            body = body.replace(old + "(", new + "(")
        functions.append(body)
    header = "\n".join(functions)
    return header, dict(baseline=revision, source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                        functions=hashes, header_sha256=hashlib.sha256(header.encode()).hexdigest(),
                        corrections="none; only names and explicit original profile labels")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--baseline", default="52e76654fe")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    header, receipt = prepare(args.root, args.baseline)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(header)
    args.output.with_suffix(".json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
