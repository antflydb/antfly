// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Installed as build.zig only in the source-overlay regression checkout.
const std = @import("std");
const project = @import("project_build.zig");
const profiles = @import("tools/fixtures/build_profiles.zig");

// This bounded cache audit measures these owners and consumers. Other owner
// fixtures are covered by their normal test targets; adding one must not grow
// this compilation matrix or invalidate its discovery contract.
const audited_tests = [_][]const u8{
    "storage-owner-tests",
    "storage-owner-source-tests",
    "storage-owner-enrichment-tests",
    "api-table-read-tests",
    "api-table-write-tests",
    "api-table-write-lifecycle-tests",
    "data-runtime-tests",
};

pub fn build(b: *std.Build) void {
    _ = project.create(b) orelse return;
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| profiles.collectSteps(&top.step, &steps, &modules);
    const check = b.step("check-storage-compilation", "Compile real runtime archives and their owner tests");
    var found: [audited_tests.len]bool = @splat(false);
    var iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        if (artifact.kind != .exe) continue;
        for (audited_tests, 0..) |name, index| {
            if (std.mem.eql(u8, artifact.name, name)) {
                if (found[index]) std.debug.panic("duplicate audited storage test artifact: {s}", .{name});
                check.dependOn(&artifact.step);
                found[index] = true;
            }
        }
    }
    for (audited_tests, found) |name, present| {
        if (!present) std.debug.panic("missing audited storage test artifact: {s}", .{name});
    }
    inline for (.{ "cli", "distributed", "storage_kernel", "enrichment_compute", "serverless", "inference", "api_kernel" }) |unit| {
        check.dependOn(&b.top_level_steps.get("runtime-unit-" ++ unit).?.step);
    }
}
