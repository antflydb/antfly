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

pub fn dependOnAll(step: *std.Build.Step, dependencies: []const *std.Build.Step) void {
    for (dependencies) |dependency| {
        step.dependOn(dependency);
    }
}

pub fn assignDefaultAggregateMaxRss(
    b: *std.Build,
    root: *std.Build.Step,
    compile_max_rss: usize,
    run_max_rss: usize,
) void {
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    defer visited.deinit();
    assignDefaultAggregateMaxRssRecursive(root, compile_max_rss, run_max_rss, &visited);
}

pub fn assignDefaultAggregateMaxRssRecursive(
    step: *std.Build.Step,
    compile_max_rss: usize,
    run_max_rss: usize,
    visited: *std.AutoHashMap(*std.Build.Step, void),
) void {
    const entry = visited.getOrPut(step) catch @panic("OOM");
    if (entry.found_existing) return;
    if (step.max_rss == 0) switch (step.id) {
        .compile => step.max_rss = compile_max_rss,
        .run => step.max_rss = run_max_rss,
        else => {},
    };
    for (step.dependencies.items) |dependency| {
        assignDefaultAggregateMaxRssRecursive(
            dependency,
            compile_max_rss,
            run_max_rss,
            visited,
        );
    }
}

pub fn addRuntimeTestFilters(
    b: *std.Build,
    run: *std.Build.Step.Run,
    filters: []const []const u8,
) void {
    for (filters) |filter| {
        run.addArgs(&.{ "--test-filter", filter });
    }
    build_test_filters.addRuntimeControls(run, b.args orelse &.{});
}

pub fn addRuntimeSkipTestFilters(run: *std.Build.Step.Run, filters: []const []const u8) void {
    for (filters) |filter| {
        run.addArgs(&.{ "--skip-test-filter", filter });
    }
}

pub fn configureUnitStorageTestRun(
    b: *std.Build,
    run: *std.Build.Step.Run,
    runtime_filters: []const []const u8,
    allow_empty_filter: bool,
    unit_skip_filters: []const []const u8,
    root_skip_filters: []const []const u8,
    extra_skip_filters: []const []const u8,
    is_ha_shard: bool,
) void {
    addRuntimeTestFilters(b, run, runtime_filters);
    if (allow_empty_filter) run.addArg("--allow-empty-test-filter");
    addRuntimeSkipTestFilters(run, unit_skip_filters);
    for (root_skip_filters) |filter| {
        // `storage.ha` keeps the HA suite out of broad root-module test runs.
        // Applying it to the dedicated shard would select zero tests.
        if (is_ha_shard and std.mem.eql(u8, filter, "storage.ha")) continue;
        run.addArgs(&.{ "--skip-test-filter", filter });
    }
    addRuntimeSkipTestFilters(run, extra_skip_filters);
    addRuntimeSkipTestFilters(run, &release_scale_test_filters);
}

pub fn compileFiltersWithAnchors(
    b: *std.Build,
    anchors: []const []const u8,
    runtime_filters: []const []const u8,
) []const []const u8 {
    const filters = b.allocator.alloc([]const u8, anchors.len + runtime_filters.len) catch @panic("OOM");
    var count: usize = 0;
    for (anchors) |anchor| {
        filters[count] = anchor;
        count += 1;
    }
    for (runtime_filters) |filter| {
        var duplicate = false;
        for (filters[0..count]) |existing| {
            if (std.mem.eql(u8, existing, filter)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        filters[count] = filter;
        count += 1;
    }
    return filters[0..count];
}

pub fn addAntflyTestRunArtifact(
    b: *std.Build,
    tests: *std.Build.Step.Compile,
) *std.Build.Step.Run {
    if (tests.test_runner == null) {
        const runner_path = b.path("pkg/antfly/src/test_runner.zig");
        tests.test_runner = .{ .path = runner_path, .mode = .simple };
        runner_path.addStepDependencies(&tests.step);
    }
    return b.addRunArtifact(tests);
}

/// Zig's compile-time filters can retain imported anonymous tests needed for
/// semantic analysis. Give every filtered artifact the exact-filter runner and
/// apply the caller's independently selected runtime filters so compile-only
/// reachability anchors never become executed tests.
pub fn addFilteredTestRunArtifactWithRuntimeFilters(
    b: *std.Build,
    tests: *std.Build.Step.Compile,
    runtime_filters: []const []const u8,
) *std.Build.Step.Run {
    const run = addAntflyTestRunArtifact(b, tests);
    addRuntimeTestFilters(b, run, runtime_filters);
    return run;
}

pub fn addFilteredTestRunArtifact(b: *std.Build, tests: *std.Build.Step.Compile) *std.Build.Step.Run {
    return addFilteredTestRunArtifactWithRuntimeFilters(b, tests, tests.filters);
}

/// Compile the curated suite once; caller filters may only narrow it.
pub fn addCuratedTestRunArtifact(
    b: *std.Build,
    tests: *std.Build.Step.Compile,
    suite_filters: []const []const u8,
) *std.Build.Step.Run {
    const run = addAntflyTestRunArtifact(b, tests);
    for (suite_filters) |filter| run.addArgs(&.{ "--suite-filter", filter });
    addRuntimeTestFilters(b, run, selectTestFilters(b, suite_filters));
    return run;
}

pub fn expectQuietSuccess(run: *std.Build.Step.Run) *std.Build.Step {
    run.has_side_effects = true;
    run.expectExitCode(0);
    run.expectStdErrMatch("");
    return &run.step;
}

pub const release_scale_test_filters = [_][]const u8{
    "db dense default dynamic 0.2 percent numeric filter exact scores bounded candidates",
    "one percent native filter routes through integrated dense search exactly",
    "db one real delete keeps filtered full text on complement path across restart",
    "db production ingest preserves high-frequency keyword recall across clean restarts",
};

pub fn productionVoprCompileMaxRss(target: std.Build.ResolvedTarget) usize {
    return @as(usize, if (target.result.os.tag == .macos) 18 else 7) * 1024 * 1024 * 1024;
}
