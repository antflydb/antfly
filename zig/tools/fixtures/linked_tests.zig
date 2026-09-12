// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const linked = @import("pkg/antfly/build/linked_tests.zig");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const filters = @import("build_test_filters.zig").select(b.allocator, b.args orelse &.{}, &.{"fixture"});
    const provider = b.addLibrary(.{
        .name = "fixture-provider",
        .root_module = b.createModule(.{
            .root_source_file = b.path("provider.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    const consumer_module = b.createModule(.{
        .root_source_file = b.path("consumer.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    consumer_module.addCSourceFile(.{ .file = b.path("consumer.c"), .flags = &.{} });
    const consumer = linked.add(b, .{
        .name = "fixture-consumer",
        .root_module = consumer_module,
        .filters = filters,
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    consumer.executable.root_module.linkLibrary(provider);
    const implementation = b.addTest(.{
        .name = "fixture-implementation",
        .root_module = b.createModule(.{
            .root_source_file = b.path("implementation.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = filters,
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    const compile_step = b.step("compile", "Compile and link without foreign execution");
    compile_step.dependOn(&consumer.executable.step);
    const run_step = b.step("test", "Run the audited pair");
    run_step.dependOn(&linked.runPair(b, consumer, implementation).step);
}
