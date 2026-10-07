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
    defer @import("antfly_platform").bindBuild(b);

    const module = b.addModule("vopr", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{ .root_module = module });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run VOPR deterministic simulation and replay tests");
    test_step.dependOn(&run_unit_tests.step);

    const benchmark_module = b.createModule(.{
        .root_source_file = b.path("src/benchmark_main.zig"),
        .target = target,
        .optimize = .safe,
    });
    benchmark_module.addImport("vopr", if (optimize == .safe) module else b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .safe,
    }));
    const benchmark_exe = b.addExecutable(.{ .name = "vopr-benchmark", .root_module = benchmark_module });
    const benchmark_step = b.step("benchmark", "Run deterministic VOPR search-efficiency benchmarks");
    benchmark_step.dependOn(&b.addRunArtifact(benchmark_exe).step);
}
