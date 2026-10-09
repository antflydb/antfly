// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub const support = @import("build_support.zig");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    _ = b.addModule("antfly_media", .{ .root_source_file = b.path("src/mod.zig"), .target = target, .optimize = optimize });
    const objectstore = b.dependency("objectstore", .{ .target = target, .optimize = optimize });
    const integration = b.createModule(.{ .root_source_file = b.path("objectstore_test_root.zig"), .target = target, .optimize = optimize });
    integration.addImport("objectstore", objectstore.module("objectstore"));
    integration.addImport("httpx", objectstore.module("objectstore").import_table.get("httpx").?);
    integration.addImport("antfly_media", b.modules.get("antfly_media").?);
    const tests = b.addTest(.{ .root_module = integration });
    b.step("test-media-objectstore", "Run authenticated object-store range tests").dependOn(&b.addRunArtifact(tests).step);
    b.step("check-media-objectstore", "Compile authenticated object-store range tests").dependOn(&tests.step);
    support.addTests(b, b.path("."), target, optimize);
}
