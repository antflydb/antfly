// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const project = @import("project_build.zig");

pub fn build(b: *std.Build) void {
    _ = project.create(b);
    _ = b.step("cache-vopr-memory", "Check VOPR workflow admission claims without compiling production");
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    for ([_][]const u8{
        "antfly-raft-transport-test",  "standby-vopr-test",                "vopr-runtime-test",
        "restore-admission-vopr-test", "vopr-determinism-audit",           "vopr-build",
        "antfly",                      "antfly-storage-owner-source-test",
    }) |name| {
        check(&b.top_level_steps.get(name).?.step, &visited);
    }
}

fn check(step: *std.Build.Step, visited: *std.AutoHashMap(*std.Build.Step, void)) void {
    if ((visited.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.id == .compile or step.id == .run) {
        if (step.max_rss == 0) std.debug.panic("unbudgeted VOPR work: {s}", .{step.name});
        if (step.max_rss > 22 * 1024 * 1024 * 1024)
            std.debug.panic("VOPR work exceeds workflow memory cap: {s}", .{step.name});
    }
    for (step.dependencies.items) |dependency| check(dependency, visited);
}
