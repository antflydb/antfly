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
    const banner = addProgressBanner(b, label);
    if (previous) |step| banner.step.dependOn(step);
    const run = b.addRunArtifact(artifact);
    run.step.dependOn(&banner.step);
    return &run.step;
}

fn singleTestFilter(b: *std.Build, filter: []const u8) []const []const u8 {
    const filters = b.allocator.alloc([]const u8, 1) catch @panic("OOM");
    filters[0] = filter;
    return filters;
}

fn chainLabeledFilteredTest(
    b: *std.Build,
    root_module: *std.Build.Module,
    phase: []const u8,
    filter: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = singleTestFilter(b, filter),
    });
    return chainLabeledRun(b, tests, b.fmt("{s}: {s}", .{ phase, filter }), previous);
}

pub fn chainLabeledFilteredTests(
    b: *std.Build,
    root_module: *std.Build.Module,
    phase: []const u8,
    filters: []const []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    var tail = previous;
    for (filters) |filter| {
        tail = chainLabeledFilteredTest(b, root_module, phase, filter, tail);
    }
    return tail.?;
}

/// Zig 0.17's split build system removed the ability to observe the post-`--`
/// passthru args (`b.args`) at configure time, so compile-time test filtering
/// moves to a repeatable `-Dtest-filter=...` option instead of
/// `zig build test -- [--test-filter] <name>`. The option is declared once by
/// the caller (`b.option` panics if declared twice) and the resolved value is
/// threaded in here.
pub fn selectTestFilters(
    override: ?[]const []const u8,
    default_filters: []const []const u8,
) []const []const u8 {
    return override orelse default_filters;
}
