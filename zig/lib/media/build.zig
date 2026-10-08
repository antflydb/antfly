// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub const support = @import("build_support.zig");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    _ = b.addModule("antfly_media", .{ .root_source_file = b.path("src/mod.zig"), .target = target, .optimize = optimize });
    support.addTests(b, b.path("."), target, optimize);
}
