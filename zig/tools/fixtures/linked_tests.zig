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
    const support = @import("pkg/antfly/build/test_support.zig");
    var owners: [2]support.OwnerTests = undefined;
    for ([_][]const u8{ "a", "b" }, &owners) |name, *owner| {
        owner.* = .{
            .artifact = b.addTest(.{
                .name = b.fmt("fixture-owner-{s}", .{name}),
                .root_module = b.createModule(.{
                    .root_source_file = b.path(b.fmt("owner_{s}.zig", .{name})),
                    .target = target,
                    .optimize = optimize,
                }),
                .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
            }),
            .filters = &.{"owned"},
        };
    }
    if (b.option(bool, "duplicate-owner", "Deliberately overlap owner inventories") orelse false) owners[1] = owners[0];
    support.addOwnerTestRuns(b, b.step("owner", "Run stable owner shards"), &owners, &.{});
    const concurrent = b.step("concurrency", "Verify test execution overlaps and is never cached");
    for ([_][]const u8{ "first", "second" }) |label| {
        const child = b.addSystemCommand(&.{"python3"});
        child.addFileArg(b.path("barrier.py"));
        child.addArg(label);
        @import("pkg/antfly/build/test_support.zig").configureTestRun(child);
        concurrent.dependOn(&child.step);
    }
}
