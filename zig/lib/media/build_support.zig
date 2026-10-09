// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
/// Attach the shared portable container module, inheriting the consumer target.
pub fn attach(b: *std.Build, consumer: *std.Build.Module, root: std.Build.LazyPath) void {
    consumer.addImport("antfly_media", b.createModule(.{
        .root_source_file = root.path(b, "src/mod.zig"),
        .target = consumer.resolved_target,
        .optimize = consumer.optimize,
        .single_threaded = consumer.single_threaded,
    }));
}

pub fn addTests(b: *std.Build, root: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.Optimize) void {
    const tests = b.addTest(.{ .root_module = b.createModule(.{ .root_source_file = root.path(b, "media_test_root.zig"), .target = target, .optimize = optimize }) });
    b.step("test-media", "Run shared container, source and timeline tests").dependOn(&b.addRunArtifact(tests).step);
    b.step("check-media", "Compile shared media tests without executing them").dependOn(&tests.step);
}
