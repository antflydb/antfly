// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const build_test_filters = @import("../../../build_test_filters.zig");

fn addProgressBanner(b: *std.Build, label: []const u8) *std.Build.Step.Run {
    return b.addSystemCommand(&.{
        "sh",
        "-c",
        b.fmt("printf '\\n==== {s} ====\\n'", .{label}),
    });
}

pub fn chainLabeledRun(
    b: *std.Build,
    artifact: *std.Build.Step.Compile,
    label: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    return chainLabeledRunStep(b, b.addRunArtifact(artifact), label, previous);
}

/// Add a progress banner without discarding arguments, environment, or other
/// policy already attached to a run artifact.
pub fn chainLabeledRunStep(
    b: *std.Build,
    run: *std.Build.Step.Run,
    label: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const banner = addProgressBanner(b, label);
    if (previous) |step| banner.step.dependOn(step);
    run.step.dependOn(&banner.step);
    return &run.step;
}

fn chainLabeledFilteredRun(
    b: *std.Build,
    artifact: *std.Build.Step.Compile,
    phase: []const u8,
    filter: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const banner = addProgressBanner(b, b.fmt("{s}: {s}", .{ phase, filter }));
    if (previous) |step| banner.step.dependOn(step);
    const run = b.addRunArtifact(artifact);
    run.addArgs(&.{ "--test-filter", filter });
    run.step.dependOn(&banner.step);
    return &run.step;
}

pub fn chainLabeledFilteredTests(
    b: *std.Build,
    root_module: *std.Build.Module,
    phase: []const u8,
    filters: []const []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    var tail = previous;
    for (filters) |filter| {
        tail = chainLabeledFilteredRun(b, tests, phase, filter, tail);
    }
    return tail.?;
}

pub fn selectTestFilters(
    b: *std.Build,
    default_filters: []const []const u8,
) []const []const u8 {
    return build_test_filters.select(
        b.allocator,
        b.args orelse &.{},
        default_filters,
    );
}

/// Name the existing run nodes; do not add dependencies or duplicate suites.
pub fn labelTestRuns(b: *std.Build, root: *std.Build.Step) void {
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    defer visited.deinit();
    labelTestRunsRecursive(b, root, &visited);
}

fn labelTestRunsRecursive(b: *std.Build, step: *std.Build.Step, visited: *std.AutoHashMap(*std.Build.Step, void)) void {
    const entry = visited.getOrPut(step) catch @panic("OOM");
    if (entry.found_existing) return;
    if (step.cast(std.Build.Step.Run)) |run| {
        for (run.argv.items) |arg| {
            if (arg != .artifact or arg.artifact.artifact.kind != .@"test") continue;
            const artifact = arg.artifact.artifact;
            const path = if (artifact.root_module.root_source_file) |source| switch (source) {
                .src_path => |v| v.sub_path,
                else => artifact.name,
            } else artifact.name;
            const selection = if (artifact.filters.len != 0) artifact.filters[0] else "all";
            run.setName(b.fmt("test {s} [{s}]", .{ path, selection }));
            break;
        }
    }
    for (step.dependencies.items) |dependency| labelTestRunsRecursive(b, dependency, visited);
}
