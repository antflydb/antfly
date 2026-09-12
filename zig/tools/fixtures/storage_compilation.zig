// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Installed as build.zig only in the source-overlay regression checkout.
const std = @import("std");
const project = @import("project_build.zig");
const profiles = @import("tools/fixtures/build_profiles.zig");

pub fn build(b: *std.Build) void {
    _ = project.create(b) orelse return;
    var steps = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var modules = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
    for (b.top_level_steps.values()) |top| profiles.collectSteps(&top.step, &steps, &modules);
    const check = b.step("check-storage-compilation", "Compile real runtime archives and their owner tests");
    var found: usize = 0;
    var consumers: usize = 0;
    var iterator = steps.keyIterator();
    while (iterator.next()) |entry| {
        const artifact = entry.*.cast(std.Build.Step.Compile) orelse continue;
        if (artifact.kind == .exe and std.mem.startsWith(u8, artifact.name, "storage-owner-")) {
            check.dependOn(&artifact.step);
            found += 1;
        }
        if (artifact.kind == .exe) {
            inline for (.{ "api-table-read-tests", "api-table-write-tests", "api-table-write-lifecycle-tests", "data-runtime-tests" }) |name| {
                if (std.mem.eql(u8, artifact.name, name)) {
                    check.dependOn(&artifact.step);
                    consumers += 1;
                }
            }
        }
    }
    if (found != 3) @panic("expected all production-linked storage owner test artifacts");
    if (consumers != 4) @panic("expected all migrated consumer test artifacts");
    inline for (.{ "cli", "distributed", "storage_kernel", "enrichment_compute", "serverless", "inference", "api_kernel" }) |unit| {
        check.dependOn(&b.top_level_steps.get("runtime-unit-" ++ unit).?.step);
    }
}
