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

    const supervisor = b.createModule(.{
        .root_source_file = b.path("src/inference_process_supervisor.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const unit = b.addTest(.{ .root_module = supervisor });
    const test_step = b.step("test", "Run supervisor unit and process-lifecycle tests (Python 3 on POSIX)");
    test_step.dependOn(&b.addRunArtifact(unit).step);
    if (target.result.os.tag == .linux or target.result.os.tag == .macos) {
        const fixture = b.addExecutable(.{
            .name = "inference-supervisor-fixture",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/inference_supervisor_fixture.zig"),
                .target = target,
                .optimize = optimize,
                .link_libc = link_libc,
                .imports = &.{.{ .name = "supervisor", .module = supervisor }},
            }),
        });
        const integration = b.addSystemCommand(&.{"python3"});
        integration.addFileArg(b.path("tests/test_inference_supervisor.py"));
        integration.addArtifactArg(fixture);
        test_step.dependOn(&integration.step);
    }
}
