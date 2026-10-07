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

    const lib_mod = b.addModule("antfly-raft", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "antfly-raft",
        .root_module = lib_mod,
    });
    b.installArtifact(lib);

    const heartbeat_bench = b.addExecutable(.{
        .name = "raft-heartbeat-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/heartbeats.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "raft", .module = lib_mod }},
        }),
    });
    b.step("heartbeat-bench", "Compare per-group and bounded peer heartbeat encoding").dependOn(&b.addRunArtifact(heartbeat_bench).step);

    const retry_bench = b.addExecutable(.{
        .name = "raft-retry-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/retries.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{.{ .name = "raft", .module = lib_mod }},
        }),
    });
    b.step("retry-bench", "Measure bounded retry draining after a node reconnects").dependOn(&b.addRunArtifact(retry_bench).step);

    const lib_unit_tests = b.addTest(.{
        .root_module = lib_mod,
    });
    lib_unit_tests.step.dependOn(&no_libc_tests.step);
    b.step("test-compile", "Compile standalone unit tests for the requested target").dependOn(&lib_unit_tests.step);
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}
