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

pub const DBTestFilters = struct {
    // Keep these buckets at module/category granularity. New DB tests should
    // normally join an owning module or stable prefix instead of adding a
    // one-off title here or in build.zig.
    pub const root = [_][]const u8{
        "storage.db.db.test.",
    };

    pub const enrichment = [_][]const u8{
        "storage.db.enrichment.enrichment_runtime.test.db enrichment runtime ",
        "storage.db.derived_async.test.db derived async ",
        "storage.db.split_restore_test.test.db split cutover",
        "storage.db.split_restore_test.test.db merge-style cutover",
    };

    pub const enrichment_worker = [_][]const u8{
        "storage.db.enrichment.enrichment_runtime.test.db enrichment runtime ",
    };

    pub const enrichment_replay = [_][]const u8{
        "storage.db.derived_async.test.db derived async ",
    };

    pub const enrichment_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover",
        "storage.db.split_restore_test.test.db merge-style cutover",
    };

    pub const enrichment_split_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover enrichment ",
    };

    pub const enrichment_merge_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db merge-style cutover enrichment ",
    };

    pub const enrichment_split_cutover_reopen = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover enrichment resume ",
    };

    pub const enrichment_merge_cutover_reopen = [_][]const u8{
        "storage.db.split_restore_test.test.db merge-style cutover enrichment resume ",
    };

    pub const query = [_][]const u8{
        "storage.db.db.test.db full-text",
        "storage.db.db.test.db dense ",
        "storage.db.db.test.db sparse ",
        "storage.db.db.test.db search ",
        "storage.db.db.test.db document _edges",
        "storage.db.db.test.db document _embeddings",
        "storage.db.graph_runtime.test.",
        "storage.db.search_runtime.test.db search runtime indexing ",
        "storage.db.search_runtime.test.db search runtime graph composition ",
        "storage.db.search_runtime.test.db search runtime text schema ",
        "storage.db.search_runtime.test.db search runtime identity ",
    };

    pub const result_shape = [_][]const u8{
        "db query result shape ",
    };

    pub const txn = [_][]const u8{
        "storage.db.transactions.test.",
        "storage.db.relational_integrity.test.db relational integrity transaction ",
        "storage.db.relational_integrity.test.db relational integrity constraints ",
        "storage.db.write_path.test.db write path ",
        "storage.db.maintenance.ttl_runtime.test.",
    };

    pub const sim = [_][]const u8{
        "storage.db.db_sim_test.test.",
    };

    pub const split_restore_lifecycle = [_][]const u8{
        "storage.db.split_restore_test.test.",
    };

    pub const schema = [_][]const u8{
        "storage.db.schema_runtime.test.",
        "storage.db.relational_integrity.test.",
    };

    pub const write_path = [_][]const u8{
        "storage.db.write_path.test.",
    };

    pub const foreign_key = [_][]const u8{
        "foreign key",
    };

    pub const temporal = [_][]const u8{
        "storage.db.relational_integrity.test.db relational temporal",
    };

    pub const relational_rows = [_][]const u8{
        "storage.db.relational_rows.test.",
    };

    pub const search_runtime = [_][]const u8{
        "storage.db.search_runtime.test.",
    };

    pub const graph_runtime = [_][]const u8{
        "storage.db.graph_runtime.test.",
    };

    pub const resolution_runtime = [_][]const u8{
        "storage.db.resolution_runtime.test.db resolution runtime ",
    };

    pub const derived_async = [_][]const u8{
        "storage.db.derived_async.test.",
    };

    pub const lifecycle = [_][]const u8{
        "storage.db.lifecycle.test.",
    };

    pub const reopen = [_][]const u8{
        "storage.db.search_runtime.test.db search runtime reopen ",
    };

    pub const ttl_runtime = [_][]const u8{
        "storage.db.maintenance.ttl_runtime.test.",
    };

    pub const enrichment_any = [_][]const u8{
        "enrichment",
    };
};

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

pub fn selectTestFilters(
    b: *std.Build,
    default_filters: []const []const u8,
) []const []const u8 {
    const args = b.args orelse return default_filters;
    if (args.len == 0) return default_filters;

    if (std.mem.eql(u8, args[0], "--test-filter")) {
        if (args.len <= 1) return default_filters;
        return args[1..];
    }
    return args;
}
