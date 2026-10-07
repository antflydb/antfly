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

"""Compile the actual standalone objectstore graph with cross-target dependencies."""

import pathlib
import subprocess
import sys

zig = sys.argv[1]
root = pathlib.Path(__file__).resolve().parents[1]
# A separate build invocation exercises this package's own dependency graph.
# test-compile never invokes this script, so this remains nonrecursive and
# retains stable source paths for Zig's compilation cache.
for target, optimize in (
    ("x86_64-windows-gnu", "Debug"),
    ("x86_64-linux-gnu", "ReleaseFast"),
):
    subprocess.run(
        [
            zig,
            "build",
            "test-compile",
            "-Dtarget=" + target,
            "-Doptimize=" + optimize,
            "--summary",
            "all",
        ],
        cwd=root,
        check=True,
        timeout=180,
    )
