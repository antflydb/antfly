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
        if (spec.isolated) {
            const isolated = run.argv.items[0].artifact.artifact;
            if (isolated == executable or isolated.kind != .@"test" or
                !std.mem.eql(u8, sourcePath(b, isolated.root_module.root_source_file.?), sourcePath(b, b.path(spec.root_source_file))))
                @panic("isolated finetuning target must own its declared test root");
            for ([_][]const u8{ "test", "test-finetune" }) |name| {
                var seen = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
                var count: usize = 0;
                inspect(&b.top_level_steps.get(name).?.step, run, isolated, &seen, &count);
                if (count != 1) @panic("standalone gate must reach isolated finetuning owner once");
            }
            continue;
        }
        if (run.argv.items[0].artifact.artifact != executable)
            @panic("focused finetuning target recompiles the shared executable");
        const filters = if (spec.focused_filters.len != 0) spec.focused_filters else &.{b.fmt("{s}.test", .{std.fs.path.stem(spec.root_source_file)})};
        if (run.argv.items.len < 1 + 2 * filters.len)
            @panic("focused finetuning target lost its runtime selection");
        for (run.argv.items[1 + 2 * filters.len ..]) |arg| {
            if (arg == .passthru) continue;
            if (arg != .bytes or (!std.mem.eql(u8, arg.bytes, "--allow-empty-test-filter") and
                !std.mem.eql(u8, arg.bytes, "--allow-empty-owner")))
                std.debug.panic("{s}: unexpected runtime argument {any}", .{ spec.step_name, arg });
        }
        for (filters, 0..) |filter, index| {
            if (!std.mem.eql(u8, run.argv.items[1 + 2 * index].bytes, "--test-filter") or
                !std.mem.eql(u8, run.argv.items[2 + 2 * index].bytes, filter))
                @panic("focused finetuning target lost its runtime selection");
        }
    }
}

fn sourcePath(b: *std.Build, path: std.Build.LazyPath) []const u8 {
    return switch (path) {
        .src_path => |source| source.owner.root.joinString(b.allocator, source.sub_path) catch @panic("OOM"),
        .dependency => |dependency| dependency.dependency.builder.root.joinString(b.allocator, dependency.sub_path) catch @panic("OOM"),
        .cwd_relative => |relative| relative,
        else => @panic("fixture expected a source file path"),
    };
}

fn inspect(step: *std.Build.Step, owner: *std.Build.Step.Run, executable: *std.Build.Step.Compile, seen: *std.AutoHashMap(*std.Build.Step, void), count: *usize) void {
    if ((seen.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Run)) |run| {
        for (run.argv.items) |arg| {
            if (arg != .artifact or arg.artifact.artifact != executable) continue;
            if (run != owner) @panic("standalone gate repeats a finetuning executable");
            count.* += 1;
        }
    }
    for (step.dependencies.items) |dependency| inspect(dependency, owner, executable, seen, count);
}
