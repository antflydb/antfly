// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const project = @import("project_build.zig");

pub fn build(b: *std.Build) void {
    project.build(b);
    _ = b.step("cache-finetune-standalone", "Inspect standalone finetuning test ownership");
    const unit = &b.top_level_steps.get("test-finetune-unit").?.step;
    const owner = unit.dependencies.items[0].cast(std.Build.Step.Run).?;
    const executable = owner.argv.items[0].artifact.artifact;
    for ([_][]const u8{ "test", "test-finetune" }) |name| {
        var seen = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
        var count: usize = 0;
        inspect(&b.top_level_steps.get(name).?.step, owner, executable, &seen, &count);
        if (count != 1) @panic("standalone gate must reach shared finetuning owner once");
    }
    for (@import("build/finetune/tests.zig").specs) |spec| {
        if (spec.covered_by_inference) continue;
        const step = &b.top_level_steps.get(spec.step_name).?.step;
        const run = step.dependencies.items[0].cast(std.Build.Step.Run).?;
        if (run.argv.items[0].artifact.artifact != executable)
            @panic("focused finetuning target recompiles the shared executable");
        const filters = if (spec.focused_filters.len != 0) spec.focused_filters else &.{b.fmt("{s}.test", .{std.fs.path.stem(spec.root_source_file)})};
        if (run.argv.items.len != 1 + 2 * filters.len)
            @panic("focused finetuning target lost its runtime selection");
        for (filters, 0..) |filter, index| {
            if (!std.mem.eql(u8, run.argv.items[1 + 2 * index].bytes, "--test-filter") or
                !std.mem.eql(u8, run.argv.items[2 + 2 * index].bytes, filter))
                @panic("focused finetuning target lost its runtime selection");
        }
    }
}

fn inspect(step: *std.Build.Step, owner: *std.Build.Step.Run, executable: *std.Build.Step.Compile, seen: *std.AutoHashMap(*std.Build.Step, void), count: *usize) void {
    if ((seen.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Run)) |run| {
        for (run.argv.items) |arg| {
            if (arg != .artifact or arg.artifact.artifact != executable) continue;
            if (run != owner) @panic("standalone gate repeats shared finetuning executable");
            count.* += 1;
        }
    }
    for (step.dependencies.items) |dependency| inspect(dependency, owner, executable, seen, count);
}
