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

    const platform_mod = platform_build.addModule(b, "antfly_platform", .{
        .root_source_file = b.path("src/root.zig"),
        .filesystem_capacity_source_file = b.path("src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });

    const compile_check = b.addTest(.{
        .root_module = platform_mod,
    });
    const check_step = b.step("check", "Compile the platform module without running target binaries");
    check_step.dependOn(&compile_check.step);
}
