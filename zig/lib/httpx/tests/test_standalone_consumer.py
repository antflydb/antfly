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

"""Compile an HTTP runtime consumer sharing its platform module on Windows."""

import pathlib
import shutil
import subprocess
import sys
import tempfile

zig = sys.argv[1]
httpx_path = pathlib.Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix="antfly-httpx-consumer-") as temporary:
    root = pathlib.Path(temporary).resolve()
    ignored = shutil.ignore_patterns(".zig-cache", "zig-out", "__pycache__")
    for name in ("httpx", "json", "platform"):
        shutil.copytree(httpx_path.parent / name, root / "lib" / name, ignore=ignored)
    (root / "build.zig.zon").write_text(
        '.{ .name = .httpx_platform_consumer, .version = "0.0.0", .fingerprint = 0x18bdcde2cac9ebec, '
        '.dependencies = .{ .httpx = .{ .path = "lib/httpx" }, '
        '.antfly_platform = .{ .path = "lib/platform" } }, .paths = .{ "" } }\n'
    )
    (root / "build.zig").write_text("""
const std = @import("std");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const httpx = b.dependency("httpx", .{ .target = target, .optimize = .Debug }).module("httpx");
    const platform = b.dependency("antfly_platform", .{ .target = target, .optimize = .Debug, .link_libc = true }).module("antfly_platform");
    const module = b.createModule(.{
        .root_source_file = b.path("root.zig"), .target = target,
        .optimize = .Debug, .link_libc = true,
        .imports = &.{.{ .name = "httpx", .module = httpx }},
    });
    @import("antfly_platform").bindPlatform(module, platform);
    b.installArtifact(b.addExecutable(.{ .name = "httpx-consumer", .root_module = module }));
}
""")
    (root / "root.zig").write_text("""
const std = @import("std");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
pub fn main(init: std.process.Init) void {
    var executor = platform.Io.Threaded.init(init.gpa, .{});
    defer executor.deinit();
    var runtime = httpx.HttpRuntime.init(init.gpa, .{ .observer_io = executor.io() });
    defer runtime.deinit();
    comptime {
        const Executor = @typeInfo(@FieldType(httpx.HttpRuntime, "listener_io_impl")).optional.child;
        if (Executor != platform.Io.Threaded) @compileError("HTTP runtime must share the owning platform executor type");
    }
}
""")
    subprocess.run(
        [zig, "build", "-Dtarget=x86_64-windows-gnu", "--summary", "all"],
        cwd=root,
        check=True,
        timeout=180,
    )
