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
pub const addNativeProcessTest = platform_build.addNativeProcessTest;

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const link_libc = b.option(bool, "link_libc", "Link the platform module against libc") orelse true;

    _ = platform_build.addModule(b, "antfly_platform", .{
        .root_source_file = b.path("src/root.zig"),
        .filesystem_capacity_source_file = b.path("src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });

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
