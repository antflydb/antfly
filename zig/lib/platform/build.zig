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
const platform_build = @import("build_support.zig");
pub const ModuleOptions = platform_build.ModuleOptions;
pub const createModule = platform_build.createModule;
pub const addModule = platform_build.addModule;
pub const addFilesystemCapacitySource = platform_build.addFilesystemCapacitySource;
pub const addTests = platform_build.addTests;
pub const canRunNativeProcess = platform_build.canRunNativeProcess;
pub const addNativeProcessTest = platform_build.addNativeProcessTest;
pub const addMacosSdkPaths = platform_build.addMacosSdkPaths;

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const link_libc = b.option(bool, "link_libc", "Link the platform module against libc") orelse true;

    const platform_mod = platform_build.addModule(b, "antfly_platform", .{
        .root_source_file = b.path("src/root.zig"),
        .filesystem_capacity_source_file = b.path("src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("src/io_bench.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    bench_mod.addImport("antfly_platform", platform_mod);
    const bench = b.addExecutable(.{ .name = "io-backend-bench", .root_module = bench_mod });
    const install_bench = b.addInstallArtifact(bench, .{});
    b.step("io-backend-bench", "Build Threaded versus Evented positional I/O comparison")
        .dependOn(&install_bench.step);

    const tests = platform_build.addTests(b, .{
        .root = b.path("."),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const test_step = b.step("test", "Run supervisor unit and process-lifecycle tests (Python 3 on POSIX)");
    test_step.dependOn(&tests.unit.step);
    if (tests.process) |process| test_step.dependOn(process);
    const command_test_step = b.step("test-one-shot", "Run disposable command worker unit and process tests");
    command_test_step.dependOn(&tests.one_shot_unit.step);
    if (tests.one_shot_process) |process| command_test_step.dependOn(process);
    test_step.dependOn(command_test_step);
}
