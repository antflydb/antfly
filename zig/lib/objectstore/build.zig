// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const httpx_dep = b.dependency("httpx", .{ .target = target, .optimize = optimize });
    const httpx_mod = httpx_dep.module("httpx");
    const json_dep = b.dependency("antfly_json", .{ .target = target, .optimize = optimize });
    const json_mod = json_dep.module("antfly-json");
    const platform_dep = b.dependency("antfly_platform", .{ .target = target, .optimize = optimize, .link_libc = true });
    const platform_mod = platform_dep.module("antfly_platform");
    httpx_mod.addImport("antfly-json", json_mod);
    const credentials_mod = b.createModule(.{
        .root_source_file = b.path("../credentials/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const google_mod = b.createModule(.{
        .root_source_file = b.path("../google/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    google_mod.addImport("httpx", httpx_mod);
    google_mod.addImport("antfly_credentials", credentials_mod);
    google_mod.addImport("antfly_platform", platform_mod);

    const mod = b.addModule("objectstore", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    mod.addImport("httpx", httpx_mod);
    mod.addImport("antfly_platform", platform_mod);
    mod.addImport("antfly_google", google_mod);
    // Share executor types and TLS with dependencies that bind their own
    // standalone platform module. Bind after assembling the full import graph.
    @import("antfly_platform").bindPlatform(mod, platform_mod);

    const tests = b.addTest(.{
        .root_module = mod,
    });
    const run_tests = b.addRunArtifact(tests);
    b.step("test-compile", "Compile standalone unit tests for the requested target").dependOn(&tests.step);

    const test_step = b.step("test", "Run objectstore unit tests");
    test_step.dependOn(&run_tests.step);
    const standalone_build = b.addSystemCommand(&.{"python3"});
    standalone_build.addFileArg(b.path("tests/test_standalone_build.py"));
    standalone_build.addArg(b.graph.zig_exe);
    b.step("test-standalone-build", "Compile the standalone dependency graph for Windows and Linux")
        .dependOn(&standalone_build.step);
    test_step.dependOn(&standalone_build.step);
}
