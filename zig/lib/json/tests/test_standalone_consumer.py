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

"""Compile a real standalone JSON consumer without inheriting libc."""

import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

zig = sys.argv[1]
json_path = pathlib.Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="antfly-json-nolibc-") as temporary:
    root = pathlib.Path(temporary).resolve()
    # Copy the real packages so the fixture is independent of checkout layout,
    # symlinked macOS temp paths and Windows drive letters.
    ignored = shutil.ignore_patterns(".zig-cache", "zig-out", "__pycache__")
    shutil.copytree(json_path, root / "lib/json", ignore=ignored)
    shutil.copytree(
        json_path.parent / "platform", root / "lib/platform", ignore=ignored
    )
    dependency_path = json.dumps("lib/json")
    (root / "build.zig.zon").write_text(
        '.{ .name = .json_nolibc_consumer, .version = "0.0.0", .fingerprint = 0x17d625d9162dcc50, '
        ".dependencies = .{ .json = .{ .path = " + dependency_path + " } }, "
        '.paths = .{ "" } }\n'
    )
    (root / "build.zig").write_text("""
const std = @import("std");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const json = b.dependency("json", .{ .target = target, .optimize = .Debug });
    const module = b.createModule(.{
        .root_source_file = b.path("root.zig"), .target = target,
        .optimize = .Debug, .link_libc = false,
        .imports = &.{.{ .name = "json", .module = json.module("antfly-json") }},
    });
    if (target.result.os.tag == .freestanding) {
        const exe = b.addExecutable(.{ .name = "json-consumer", .root_module = module });
        exe.entry = .disabled;
        b.installArtifact(exe);
    } else {
        b.installArtifact(b.addLibrary(.{ .name = "json-consumer", .root_module = module, .linkage = .static }));
    }
}
""")
    (root / "root.zig").write_text("""
const builtin = @import("builtin");
const json = @import("json");
comptime { if (builtin.link_libc) @compileError("JSON consumer unexpectedly requires libc"); }
export fn sentinel() usize { return @sizeOf(json.Value); }
""")
    for target in ("x86_64-linux-gnu", "wasm32-freestanding"):
        subprocess.run(
            [zig, "build", "-Dtarget=" + target, "--summary", "all"],
            cwd=root,
            check=True,
            timeout=180,
        )
