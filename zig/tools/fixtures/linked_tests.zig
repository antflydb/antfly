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
const linked = @import("pkg/antfly/build/linked_tests.zig");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const filters = @import("build_test_filters.zig").select(b.allocator, buildArguments(b) orelse &.{}, &.{"fixture"});
    const error_logs = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/test_error_logs.zig"),
        .target = target,
        .optimize = optimize,
    });
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
    consumer_module.addImport("antfly_test_error_logs", error_logs);
    consumer_module.addCSourceFile(.{ .file = b.path("consumer.c"), .flags = &.{} });
    const consumer = linked.add(b, .{
        .name = "fixture-consumer",
        .root_module = consumer_module,
        .filters = filters,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"), .mode = .simple },
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
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"), .mode = .simple },
    });
    implementation.root_module.addImport("antfly_test_error_logs", error_logs);
    const compile_step = b.step("compile", "Compile and link without foreign execution");
    compile_step.dependOn(&consumer.executable.step);
    const run_step = b.step("test", "Run the audited pair");
    run_step.dependOn(&linked.runPair(b, consumer, implementation).step);
    const support = @import("build_support/antfly/test_support.zig");
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
                .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"), .mode = .simple },
            }),
            .filters = &.{"owned"},
        };
    }
    for (owners) |owner| owner.artifact.root_module.addImport("antfly_test_error_logs", error_logs);
    if (b.option(bool, "duplicate-owner", "Deliberately overlap owner inventories") orelse false) owners[1] = owners[0];
    support.addOwnerTestRuns(b, b.step("owner", "Run stable owner shards"), &owners, &.{});
    const concurrent = b.step("concurrency", "Verify test execution overlaps and is never cached");
    for ([_][]const u8{ "first", "second" }) |label| {
        const child = b.addSystemCommand(&.{"python3"});
        child.addFileArg2(b.path("barrier.py"), .{ .make_absolute = true });
        child.addArg(label);
        @import("build_support/antfly/test_support.zig").configureTestRun(child);
        concurrent.dependOn(&child.step);
    }
}

fn buildArguments(b: *std.Build) ?[]const []const u8 {
    if (!b.available_options_map.contains("test-filter"))
        return b.option([]const []const u8, "test-filter", "Compile-time test filters (runtime filters follow --)");
    const input = b.user_input_options.get("test-filter") orelse return null;
    return switch (input) {
        .scalar => |value| blk: {
            const values = b.allocator.alloc([]const u8, 1) catch @panic("OOM");
            values[0] = value;
            break :blk values;
        },
        .list => |values| values.items,
        else => null,
    };
}
