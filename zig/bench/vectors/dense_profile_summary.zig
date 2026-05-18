// Copyright 2026 Antfly, Inc.
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

const Allocator = std.mem.Allocator;

const FieldKind = enum {
    timing_ns,
    counter,
};

const FieldSpec = struct {
    name: []const u8,
    kind: FieldKind,
};

const fields = [_]FieldSpec{
    .{ .name = "wall_ns", .kind = .timing_ns },
    .{ .name = "total_ns", .kind = .timing_ns },
    .{ .name = "extract_writes_ns", .kind = .timing_ns },
    .{ .name = "delete_artifacts_ns", .kind = .timing_ns },
    .{ .name = "precompute_generated_ns", .kind = .timing_ns },
    .{ .name = "store_write_ns", .kind = .timing_ns },
    .{ .name = "append_derived_log_ns", .kind = .timing_ns },
    .{ .name = "wait_sync_ns", .kind = .timing_ns },
    .{ .name = "backlog_pressure_ns", .kind = .timing_ns },
    .{ .name = "executor_notify_ns", .kind = .timing_ns },
    .{ .name = "derived_apply_ns", .kind = .timing_ns },
    .{ .name = "sync_wait_ns", .kind = .timing_ns },
    .{ .name = "dense_apply_ns", .kind = .timing_ns },
    .{ .name = "dense_delete_ns", .kind = .timing_ns },
    .{ .name = "dense_doc_index_ns", .kind = .timing_ns },
    .{ .name = "dense_embedding_apply_ns", .kind = .timing_ns },
    .{ .name = "index_sync_ns", .kind = .timing_ns },
    .{ .name = "applied_sequence_save_ns", .kind = .timing_ns },
    .{ .name = "derived_log_truncate_ns", .kind = .timing_ns },
    .{ .name = "hbc_insert_commit_ns", .kind = .timing_ns },
    .{ .name = "hbc_bulk_build_store_ns", .kind = .timing_ns },
    .{ .name = "hbc_bulk_build_tree_ns", .kind = .timing_ns },
    .{ .name = "hbc_save_node_ns", .kind = .timing_ns },
    .{ .name = "hbc_save_split_range_ns", .kind = .timing_ns },
    .{ .name = "hbc_update_parent_ns", .kind = .timing_ns },
    .{ .name = "hbc_split_leaf_ns", .kind = .timing_ns },
    .{ .name = "hbc_split_internal_ns", .kind = .timing_ns },
    .{ .name = "hbc_refresh_quantized_ns", .kind = .timing_ns },
    .{ .name = "hbc_quantized_vector_load_ns", .kind = .timing_ns },
    .{ .name = "hbc_quantized_compute_ns", .kind = .timing_ns },
    .{ .name = "hbc_quantized_store_ns", .kind = .timing_ns },
    .{ .name = "hbc_quantized_encode_ns", .kind = .timing_ns },
    .{ .name = "hbc_quantized_put_ns", .kind = .timing_ns },
    .{ .name = "hbc_insert_calls", .kind = .counter },
    .{ .name = "hbc_grouped_items", .kind = .counter },
    .{ .name = "hbc_grouped_fallback_items", .kind = .counter },
    .{ .name = "hbc_grouped_leaf_groups", .kind = .counter },
    .{ .name = "hbc_grouped_split_candidates", .kind = .counter },
    .{ .name = "hbc_grouped_recursive_splits", .kind = .counter },
    .{ .name = "hbc_grouped_leaf_range_writes", .kind = .counter },
    .{ .name = "hbc_grouped_ancestor_range_refreshes", .kind = .counter },
    .{ .name = "hbc_grouped_ancestor_range_nodes", .kind = .counter },
    .{ .name = "hbc_grouped_node_body_writes", .kind = .counter },
    .{ .name = "hbc_grouped_vec_leaf_writes", .kind = .counter },
    .{ .name = "hbc_save_node_calls", .kind = .counter },
    .{ .name = "hbc_split_leaf_calls", .kind = .counter },
    .{ .name = "hbc_split_internal_calls", .kind = .counter },
    .{ .name = "hbc_range_put_calls", .kind = .counter },
    .{ .name = "hbc_range_delete_calls", .kind = .counter },
    .{ .name = "hbc_nodes_put_calls", .kind = .counter },
    .{ .name = "hbc_nodes_append_calls", .kind = .counter },
    .{ .name = "hbc_nodes_delete_calls", .kind = .counter },
    .{ .name = "hbc_meta_put_calls", .kind = .counter },
    .{ .name = "hbc_meta_append_calls", .kind = .counter },
    .{ .name = "hbc_meta_delete_calls", .kind = .counter },
    .{ .name = "hbc_quant_put_calls", .kind = .counter },
    .{ .name = "hbc_quant_append_calls", .kind = .counter },
    .{ .name = "hbc_quant_delete_calls", .kind = .counter },
    .{ .name = "hbc_vecs_put_calls", .kind = .counter },
    .{ .name = "hbc_vecs_append_calls", .kind = .counter },
    .{ .name = "hbc_vecs_delete_calls", .kind = .counter },
};

const FieldStats = struct {
    spec: FieldSpec,
    values: std.ArrayListUnmanaged(u64) = .empty,
    total: u128 = 0,

    fn deinit(self: *FieldStats, alloc: Allocator) void {
        self.values.deinit(alloc);
        self.* = undefined;
    }

    fn add(self: *FieldStats, alloc: Allocator, value: u64) !void {
        try self.values.append(alloc, value);
        self.total += value;
    }
};

const IngestSummaryRow = struct {
    docs: u64 = 0,
    dims: u64 = 0,
    batch_size: u64 = 0,
    batches: u64 = 0,
    write_ns: u64 = 0,
    write_ns_per_doc: u64 = 0,
    max_batch_ns: u64 = 0,
    final_drain_ns: u64 = 0,
    maintenance_ns: u64 = 0,
    maintenance_steps: u64 = 0,
    status_probe_count: u64 = 0,
    status_probe_ns: u64 = 0,
    status_probe_max_ns: u64 = 0,
    dense_lsm_total_runs: u64 = 0,
    dense_lsm_total_run_bytes: u64 = 0,
    dense_lsm_l0_runs: u64 = 0,
    dense_lsm_l0_bytes: u64 = 0,
    dense_lsm_obsolete_paths: u64 = 0,
    hbc_insert_calls: u64 = 0,
    hbc_grouped_items: u64 = 0,
    hbc_grouped_fallback_items: u64 = 0,
    hbc_grouped_leaf_groups: u64 = 0,
    hbc_grouped_recursive_splits: u64 = 0,
    hbc_quant_value_bytes: u64 = 0,
    hbc_vecs_value_bytes: u64 = 0,
    hbc_nodes_value_bytes: u64 = 0,
    hbc_meta_value_bytes: u64 = 0,
    hbc_insert_find_leaf_ns: u64 = 0,
    hbc_insert_mutate_leaf_ns: u64 = 0,
    hbc_insert_commit_ns: u64 = 0,
    hbc_refresh_quantized_ns: u64 = 0,
};

const FileSummary = struct {
    path: []const u8,
    rows: usize = 0,
    write_rows: usize = 0,
    docs: u64 = 0,
    inline_derived: ?bool = null,
    bulk_session: ?bool = null,
    ingest_summary: ?IngestSummaryRow = null,
    stats: []FieldStats,

    fn deinit(self: *FileSummary, alloc: Allocator) void {
        for (self.stats) |*stat| stat.deinit(alloc);
        alloc.free(self.stats);
    }
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    var saw_path = false;
    while (args.next()) |path| {
        saw_path = true;
        var summary = try loadSummary(alloc, init.io, path);
        defer summary.deinit(alloc);
        try printSummary(out, summary);
    }

    if (!saw_path) {
        try out.writeAll("usage: zig build dense-profile-summary -- <profile.jsonl> [more.jsonl...]\n");
        try stdout_writer.flush();
        return error.InvalidArgument;
    }

    try stdout_writer.flush();
}

fn loadSummary(alloc: Allocator, io: std.Io, path: []const u8) !FileSummary {
    const raw = try readFileAlloc(alloc, io, path);
    defer alloc.free(raw);

    const stats = try alloc.alloc(FieldStats, fields.len);
    for (fields, 0..) |field, i| {
        stats[i] = .{ .spec = field };
    }
    errdefer {
        for (stats) |*stat| stat.deinit(alloc);
        alloc.free(stats);
    }

    var summary = FileSummary{
        .path = path,
        .stats = stats,
    };

    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0) continue;

        const parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{}) catch |err| switch (err) {
            error.UnexpectedEndOfInput => continue,
            else => return err,
        };
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidProfileRow;

        summary.rows += 1;
        if (summary.inline_derived == null) {
            summary.inline_derived = valueObjectBool(parsed.value, "inline_derived");
        }
        if (summary.bulk_session == null) {
            summary.bulk_session = valueObjectBool(parsed.value, "bulk_session");
        }

        const phase = valueObjectString(parsed.value, "phase");
        if (phase) |p| {
            if (std.mem.eql(u8, p, "ingest_summary")) {
                summary.ingest_summary = loadIngestSummaryRow(parsed.value);
                continue;
            }
            if (!std.mem.eql(u8, p, "write_batch")) continue;
        }

        summary.write_rows += 1;
        summary.docs += valueObjectU64(parsed.value, "docs") orelse 0;
        for (summary.stats) |*stat| {
            const value = valueObjectU64(parsed.value, stat.spec.name) orelse 0;
            try stat.add(alloc, value);
        }
    }

    return summary;
}

fn readFileAlloc(alloc: Allocator, io: std.Io, path: []const u8) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(256 * 1024 * 1024));
}

fn valueObjectU64(value: std.json.Value, key: []const u8) ?u64 {
    const raw = value.object.get(key) orelse return null;
    return valueToU64(raw);
}

fn valueObjectBool(value: std.json.Value, key: []const u8) ?bool {
    const raw = value.object.get(key) orelse return null;
    return switch (raw) {
        .bool => |v| v,
        else => null,
    };
}

fn valueObjectString(value: std.json.Value, key: []const u8) ?[]const u8 {
    const raw = value.object.get(key) orelse return null;
    return switch (raw) {
        .string => |v| v,
        else => null,
    };
}

fn valueToU64(value: std.json.Value) ?u64 {
    return switch (value) {
        .integer => |v| if (v >= 0) @as(u64, @intCast(v)) else null,
        .float => |v| if (v >= 0) @as(u64, @intFromFloat(v)) else null,
        .number_string => |raw| std.fmt.parseUnsigned(u64, raw, 10) catch null,
        else => null,
    };
}

fn loadIngestSummaryRow(value: std.json.Value) IngestSummaryRow {
    return .{
        .docs = valueObjectU64(value, "docs") orelse 0,
        .dims = valueObjectU64(value, "dims") orelse 0,
        .batch_size = valueObjectU64(value, "batch_size") orelse 0,
        .batches = valueObjectU64(value, "batches") orelse 0,
        .write_ns = valueObjectU64(value, "write_ns") orelse 0,
        .write_ns_per_doc = valueObjectU64(value, "write_ns_per_doc") orelse 0,
        .max_batch_ns = valueObjectU64(value, "max_batch_ns") orelse 0,
        .final_drain_ns = valueObjectU64(value, "final_drain_ns") orelse 0,
        .maintenance_ns = valueObjectU64(value, "maintenance_ns") orelse 0,
        .maintenance_steps = valueObjectU64(value, "maintenance_steps") orelse 0,
        .status_probe_count = valueObjectU64(value, "status_probe_count") orelse 0,
        .status_probe_ns = valueObjectU64(value, "status_probe_ns") orelse 0,
        .status_probe_max_ns = valueObjectU64(value, "status_probe_max_ns") orelse 0,
        .dense_lsm_total_runs = valueObjectU64(value, "dense_lsm_total_runs") orelse 0,
        .dense_lsm_total_run_bytes = valueObjectU64(value, "dense_lsm_total_run_bytes") orelse 0,
        .dense_lsm_l0_runs = valueObjectU64(value, "dense_lsm_l0_runs") orelse 0,
        .dense_lsm_l0_bytes = valueObjectU64(value, "dense_lsm_l0_bytes") orelse 0,
        .dense_lsm_obsolete_paths = valueObjectU64(value, "dense_lsm_obsolete_paths") orelse 0,
        .hbc_insert_calls = valueObjectU64(value, "hbc_insert_calls") orelse 0,
        .hbc_grouped_items = valueObjectU64(value, "hbc_grouped_items") orelse 0,
        .hbc_grouped_fallback_items = valueObjectU64(value, "hbc_grouped_fallback_items") orelse 0,
        .hbc_grouped_leaf_groups = valueObjectU64(value, "hbc_grouped_leaf_groups") orelse 0,
        .hbc_grouped_recursive_splits = valueObjectU64(value, "hbc_grouped_recursive_splits") orelse 0,
        .hbc_quant_value_bytes = valueObjectU64(value, "hbc_quant_value_bytes") orelse 0,
        .hbc_vecs_value_bytes = valueObjectU64(value, "hbc_vecs_value_bytes") orelse 0,
        .hbc_nodes_value_bytes = valueObjectU64(value, "hbc_nodes_value_bytes") orelse 0,
        .hbc_meta_value_bytes = valueObjectU64(value, "hbc_meta_value_bytes") orelse 0,
        .hbc_insert_find_leaf_ns = valueObjectU64(value, "hbc_insert_find_leaf_ns") orelse 0,
        .hbc_insert_mutate_leaf_ns = valueObjectU64(value, "hbc_insert_mutate_leaf_ns") orelse 0,
        .hbc_insert_commit_ns = valueObjectU64(value, "hbc_insert_commit_ns") orelse 0,
        .hbc_refresh_quantized_ns = valueObjectU64(value, "hbc_refresh_quantized_ns") orelse 0,
    };
}

const RollupSpec = struct {
    name: []const u8,
    members: []const []const u8,
};

const rollups = [_]RollupSpec{
    .{
        .name = "request_path_ns",
        .members = &.{
            "resolve_transforms_ns",
            "merge_effective_req_ns",
            "predicates_ns",
            "validate_range_ns",
            "extract_writes_ns",
            "delete_artifacts_ns",
            "precompute_generated_ns",
            "store_write_ns",
            "split_delta_ns",
            "build_derived_ns",
            "apply_shadow_ns",
            "collect_sync_targets_ns",
            "append_derived_log_ns",
        },
    },
    .{
        .name = "wait_and_pressure_ns",
        .members = &.{
            "wait_sync_ns",
            "backlog_pressure_ns",
            "executor_notify_ns",
            "sync_wait_ns",
        },
    },
    .{
        .name = "derived_apply_ns",
        .members = &.{
            "derived_apply_ns",
            "full_text_apply_ns",
            "dense_apply_ns",
            "dense_delete_ns",
            "dense_doc_index_ns",
            "dense_embedding_apply_ns",
            "sparse_apply_ns",
            "graph_apply_ns",
            "index_sync_ns",
            "applied_sequence_save_ns",
            "derived_log_truncate_ns",
            "notify_enrichment_ns",
        },
    },
    .{
        .name = "hbc_core_ns",
        .members = &.{
            "hbc_insert_transform_ns",
            "hbc_insert_store_vector_ns",
            "hbc_insert_find_leaf_ns",
            "hbc_insert_mutate_leaf_ns",
            "hbc_insert_flush_metadata_ns",
            "hbc_insert_commit_ns",
            "hbc_save_node_ns",
            "hbc_save_split_range_ns",
            "hbc_update_parent_ns",
            "hbc_split_leaf_ns",
            "hbc_split_internal_ns",
            "hbc_refresh_quantized_ns",
        },
    },
    .{
        .name = "hbc_quantized_detail_ns",
        .members = &.{
            "hbc_quantized_vector_load_ns",
            "hbc_quantized_compute_ns",
            "hbc_quantized_store_ns",
            "hbc_quantized_encode_ns",
            "hbc_quantized_put_ns",
        },
    },
};

fn printSummary(writer: *std.Io.Writer, summary: FileSummary) !void {
    try writer.print(
        "dense_profile path={s} rows={d} write_rows={d} docs={d} inline_derived={s} bulk_session={s}\n",
        .{
            summary.path,
            summary.rows,
            summary.write_rows,
            summary.docs,
            if (summary.inline_derived) |enabled| if (enabled) "true" else "false" else "unknown",
            if (summary.bulk_session) |enabled| if (enabled) "true" else "false" else "unknown",
        },
    );

    if (summary.ingest_summary) |ingest| {
        try writer.print(
            "  ingest_summary: docs={d} batches={d} write_ms={d:.3} write_ns_per_doc={d} max_batch_ms={d:.3} final_drain_ms={d:.3} dense_runs={d} dense_run_mb={d:.2} dense_l0_runs={d} hbc_insert_calls={d} hbc_leaf_groups={d} hbc_recursive_splits={d}\n",
            .{
                ingest.docs,
                ingest.batches,
                nsToMs(@floatFromInt(ingest.write_ns)),
                ingest.write_ns_per_doc,
                nsToMs(@floatFromInt(ingest.max_batch_ns)),
                nsToMs(@floatFromInt(ingest.final_drain_ns)),
                ingest.dense_lsm_total_runs,
                bytesToMiB(ingest.dense_lsm_total_run_bytes),
                ingest.dense_lsm_l0_runs,
                ingest.hbc_insert_calls,
                ingest.hbc_grouped_leaf_groups,
                ingest.hbc_grouped_recursive_splits,
            },
        );
    }

    for (rollups) |rollup| {
        const total = rollupTotal(summary, rollup.members);
        if (total == 0) continue;
        try writer.print("  {s}: total_ms={d:.3}\n", .{ rollup.name, nsToMs(@floatFromInt(total)) });
    }

    for (summary.stats) |stat| {
        if (stat.values.items.len == 0) continue;
        if (stat.total == 0 and stat.spec.kind == .timing_ns) continue;
        std.mem.sort(u64, stat.values.items, {}, comptime std.sort.asc(u64));

        switch (stat.spec.kind) {
            .timing_ns => try writer.print(
                "  {s}: total_ms={d:.3} avg_ms={d:.3} p50_ms={d:.3} p95_ms={d:.3} max_ms={d:.3}\n",
                .{
                    stat.spec.name,
                    nsToMs(@floatFromInt(stat.total)),
                    nsToMs(@as(f64, @floatFromInt(stat.total)) / @as(f64, @floatFromInt(stat.values.items.len))),
                    nsToMs(@floatFromInt(percentile(stat.values.items, 50))),
                    nsToMs(@floatFromInt(percentile(stat.values.items, 95))),
                    nsToMs(@floatFromInt(stat.values.items[stat.values.items.len - 1])),
                },
            ),
            .counter => if (stat.total != 0) try writer.print(
                "  {s}: total={d} avg={d:.2} p50={d} p95={d} max={d}\n",
                .{
                    stat.spec.name,
                    stat.total,
                    @as(f64, @floatFromInt(stat.total)) / @as(f64, @floatFromInt(stat.values.items.len)),
                    percentile(stat.values.items, 50),
                    percentile(stat.values.items, 95),
                    stat.values.items[stat.values.items.len - 1],
                },
            ),
        }
    }
}

fn rollupTotal(summary: FileSummary, names: []const []const u8) u128 {
    var total: u128 = 0;
    for (names) |name| {
        total += fieldTotal(summary, name);
    }
    return total;
}

fn fieldTotal(summary: FileSummary, name: []const u8) u128 {
    for (summary.stats) |stat| {
        if (std.mem.eql(u8, stat.spec.name, name)) return stat.total;
    }
    return 0;
}

fn bytesToMiB(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

fn percentile(sorted: []const u64, pct: u64) u64 {
    if (sorted.len == 0) return 0;
    const idx = @min(sorted.len - 1, ((sorted.len - 1) * @as(usize, @intCast(pct)) + 50) / 100);
    return sorted[idx];
}

fn nsToMs(ns: f64) f64 {
    return ns / @as(f64, @floatFromInt(std.time.ns_per_ms));
}
