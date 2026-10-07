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

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    // Compile the actual standalone library tests without libc even on hosts
    // that cannot execute Linux binaries. bindBuild supplies this artifact's
    // platform dependency after the complete graph is declared.
    const no_libc_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = b.resolveTargetQuery(.{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .gnu }),
        .optimize = .Debug,
        .link_libc = false,
    }) });
    b.step("test-nolibc", "Compile standalone Linux unit tests without libc").dependOn(&no_libc_tests.step);
    defer @import("antfly_platform").bindBuild(b);

    const structlog_module = b.addModule("structlog", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    {
        const lib_test = b.addTest(.{
            .root_module = structlog_module,
        });
        lib_test.step.dependOn(&no_libc_tests.step);
        b.step("test-compile", "Compile standalone unit tests for the requested target").dependOn(&lib_test.step);
        const run_test = b.addRunArtifact(lib_test);
        run_test.has_side_effects = true;

        const test_step = b.step("test", "Run tests");
        test_step.dependOn(&run_test.step);
    }
}
