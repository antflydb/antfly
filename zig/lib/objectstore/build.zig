// Copyright 2026 Antfly, Inc.
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

    const httpx_dep = b.dependency("httpx", .{});
    const httpx_mod = httpx_dep.module("httpx");
    const json_dep = b.dependency("antfly_json", .{});
    const json_mod = json_dep.module("antfly-json");
    const platform_dep = b.dependency("antfly_platform", .{});
    const platform_mod = platform_dep.module("antfly_platform");
    httpx_mod.addImport("antfly-json", json_mod);

    const mod = b.addModule("objectstore", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("httpx", httpx_mod);
    mod.addImport("antfly_platform", platform_mod);

    const tests = b.addTest(.{
        .root_module = mod,
    });
    const run_tests = b.addRunArtifact(tests);

    const test_step = b.step("test", "Run objectstore unit tests");
    test_step.dependOn(&run_tests.step);
}
