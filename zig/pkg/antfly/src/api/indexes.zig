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
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const tables_api = @import("tables.zig");
const runtime_status = @import("runtime_status.zig");
const indexes_openapi = @import("antfly_indexes_openapi");

pub fn parseCreateIndexRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    if (body.len == 0) return error.InvalidCreateIndexRequest;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .object => {},
        else => return error.InvalidCreateIndexRequest,
    }
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

pub fn addIndexToTableIndexesJson(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    index_name: []const u8,
    index_json: []const u8,
) ![]u8 {
    var current = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer current.deinit();
    var config = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer config.deinit();

    const root = switch (current.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    if (config.value != .object) return error.InvalidCreateIndexRequest;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, index_name)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    if (!first) try out.append(alloc, ',');
    try appendJsonString(alloc, &out, index_name);
    try out.append(alloc, ':');
    const encoded_config = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(config.value, .{})});
    defer alloc.free(encoded_config);
    try out.appendSlice(alloc, encoded_config);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn removeIndexFromTableIndexesJson(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    index_name: []const u8,
) !?[]u8 {
    var current = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer current.deinit();

    const root = switch (current.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    if (!root.contains(index_name)) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, index_name)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeIndexList(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    const table = tables_api.findTableByName(snapshot, table_name) orelse return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(table.indexes_json), .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const expected_group_ids = try expectedTableGroupIds(alloc, snapshot, table.table_id);
    defer if (expected_group_ids.len > 0) alloc.free(expected_group_ids);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '[');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendIndexStatus(alloc, &out, entry.key_ptr.*, entry.value_ptr.*, expected_group_ids, local_statuses);
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeSingleIndex(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    index_name: []const u8,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    const table = tables_api.findTableByName(snapshot, table_name) orelse return null;
    const expected_group_ids = try expectedTableGroupIds(alloc, snapshot, table.table_id);
    defer if (expected_group_ids.len > 0) alloc.free(expected_group_ids);
    return try encodeSingleIndexForTableWithTopology(alloc, table, index_name, expected_group_ids, local_statuses);
}

pub fn encodeSingleIndexForTable(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    return try encodeSingleIndexForTableWithTopology(alloc, table, index_name, &.{}, local_statuses);
}

fn encodeSingleIndexForTableWithTopology(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    var lookup = (try lookupSingleIndexConfig(alloc, table.indexes_json, index_name)) orelse return null;
    defer lookup.deinit();
    return try encodeSingleIndexLookupWithTopology(alloc, index_name, lookup.config, expected_group_ids, local_statuses);
}

pub fn encodeSingleIndexLookup(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) ![]u8 {
    return try encodeSingleIndexLookupWithTopology(alloc, index_name, config, &.{}, local_statuses);
}

fn encodeSingleIndexLookupWithTopology(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) ![]u8 {
    if (config != .object) return error.InvalidTableIndexMetadata;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendIndexStatus(alloc, &out, index_name, config, expected_group_ids, local_statuses);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeIndexConfigMap(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(indexes_json), .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendIndexConfig(alloc, &out, entry.key_ptr.*, entry.value_ptr.*);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeSingleIndexConfig(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !?[]u8 {
    var lookup = (try lookupSingleIndexConfig(alloc, indexes_json, index_name)) orelse return null;
    defer lookup.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendIndexConfig(alloc, &out, index_name, lookup.config);
    return try out.toOwnedSlice(alloc);
}

pub fn hasIndexConfig(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !bool {
    var lookup = (try lookupSingleIndexConfig(alloc, indexes_json, index_name)) orelse return false;
    defer lookup.deinit();
    return true;
}

pub const SingleIndexConfigLookup = struct {
    parsed: std.json.Parsed(std.json.Value),
    config: std.json.Value,

    pub fn deinit(self: *SingleIndexConfigLookup) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub fn lookupSingleIndexConfig(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !?SingleIndexConfigLookup {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(indexes_json), .{});
    errdefer parsed.deinit();

    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const config = object.get(index_name) orelse {
        parsed.deinit();
        return null;
    };
    return .{
        .parsed = parsed,
        .config = config,
    };
}

pub fn equivalentIndexConfigJson(
    alloc: std.mem.Allocator,
    lhs_json: []const u8,
    rhs_json: []const u8,
) !bool {
    if (std.mem.eql(u8, lhs_json, rhs_json)) return true;

    var lhs = try std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{});
    defer lhs.deinit();
    var rhs = try std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{});
    defer rhs.deinit();

    const lhs_object = switch (lhs.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const rhs_object = switch (rhs.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    if (lhs_object.count() != rhs_object.count()) return false;

    var it = lhs_object.iterator();
    while (it.next()) |entry| {
        const rhs_value = rhs_object.get(entry.key_ptr.*) orelse return false;
        const lhs_config = try canonicalIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*);
        defer alloc.free(lhs_config);
        const rhs_config = try canonicalIndexConfigJson(alloc, entry.key_ptr.*, rhs_value);
        defer alloc.free(rhs_config);
        if (!std.mem.eql(u8, lhs_config, rhs_config)) return false;
    }
    return true;
}

const ApiIndexType = enum {
    full_text,
    embeddings,
    graph,
    algebraic,
};

fn indexesJsonSource(indexes_json: []const u8) []const u8 {
    return if (indexes_json.len > 0) indexes_json else tables_api.default_indexes_json;
}

fn expectedTableGroupIds(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
) ![]u64 {
    var count: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id == table_id) count += 1;
    }
    if (count == 0) return &.{};

    const group_ids = try alloc.alloc(u64, count);
    var i: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        group_ids[i] = range.group_id;
        i += 1;
    }
    return group_ids;
}

fn appendIndexStatus(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !void {
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;
    const embeddings_require_table_coverage = if (index_type == .embeddings)
        embeddingsRequiresTableCoverage(config)
    else
        false;
    const embeddings_sparse = if (index_type == .embeddings)
        embeddingsIsSparse(config)
    else
        false;
    const graph_source_status = if (index_type == .graph)
        graphSourceStatus(config)
    else
        null;
    try out.appendSlice(alloc, "{\"config\":");
    try appendIndexConfig(alloc, out, index_name, config);
    try out.appendSlice(alloc, ",\"status\":");
    try appendIndexRuntimeStatus(alloc, out, index_name, index_type, embeddings_require_table_coverage, embeddings_sparse, graph_source_status, expected_group_ids, local_statuses, false);
    try out.appendSlice(alloc, ",\"shard_status\":");
    try appendIndexRuntimeStatus(alloc, out, index_name, index_type, embeddings_require_table_coverage, embeddings_sparse, graph_source_status, expected_group_ids, local_statuses, true);
    try out.append(alloc, '}');
}

fn appendIndexConfig(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
) !void {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;

    try out.append(alloc, '{');
    try appendJsonString(alloc, out, "name");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, index_name);
    if (config.object.get("type") == null) {
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, switch (index_type) {
            .full_text => "full_text",
            .embeddings => "embeddings",
            .graph => "graph",
            .algebraic => "algebraic",
        });
    }

    var it = config.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "name")) continue;
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
}

fn canonicalIndexConfigJson(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendIndexConfig(alloc, &out, index_name, config);
    return try out.toOwnedSlice(alloc);
}

fn embeddingsRequiresTableCoverage(config: std.json.Value) bool {
    if (config != .object) return true;
    const external = config.object.get("external") orelse return true;
    return switch (external) {
        .bool => |value| !value,
        else => true,
    };
}

fn embeddingsIsSparse(config: std.json.Value) bool {
    if (config != .object) return false;
    const sparse = config.object.get("sparse") orelse return false;
    return switch (sparse) {
        .bool => |value| value,
        else => false,
    };
}

const GraphSourceStatus = struct {
    artifact: []const u8,
    path: []const u8 = "",
    format: []const u8 = "extraction_relation",
};

fn graphSourceStatus(config: std.json.Value) ?GraphSourceStatus {
    if (config != .object) return null;
    const source = config.object.get("source") orelse return null;
    if (source != .object) return null;
    const kind = source.object.get("kind") orelse return null;
    if (kind != .string or !std.mem.eql(u8, kind.string, "artifact")) return null;
    const artifact = source.object.get("artifact") orelse return null;
    if (artifact != .string or artifact.string.len == 0) return null;
    return .{
        .artifact = artifact.string,
        .path = if (source.object.get("path")) |value| switch (value) {
            .string => value.string,
            else => "",
        } else "",
        .format = if (source.object.get("format")) |value| switch (value) {
            .string => value.string,
            else => "extraction_relation",
        } else "extraction_relation",
    };
}

fn indexTypeName(index_type: ApiIndexType) []const u8 {
    return switch (index_type) {
        .full_text => "full_text",
        .embeddings => "embeddings",
        .graph => "graph",
        .algebraic => "algebraic",
    };
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn appendAlgebraicIndexStatsFields(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    item: anytype,
) !void {
    var stats = indexes_openapi.AlgebraicIndexStats{
        .index_type = .algebraic,
        .healthy = item.algebraic_parse_error_count == 0,
        .parse_error_count = saturatingI64(item.algebraic_parse_error_count),
        .schema_version = saturatingI64(item.algebraic_schema_version),
        .capability_lifecycle_status = item.algebraic_capability_lifecycle_status orelse "current",
        .planner_selected = saturatingI64(item.algebraic_planner_selected),
        .planner_fallback_count = saturatingI64(item.algebraic_planner_fallback_count),
        .planner_last_decision = item.algebraic_planner_last_decision,
        .planner_last_fallback_reason = item.algebraic_planner_last_fallback_reason,
        .planner_last_estimated_scan_rows = if (item.algebraic_planner_last_estimated_scan_rows) |value| saturatingI64(value) else null,
        .planner_last_estimated_result_buckets = if (item.algebraic_planner_last_estimated_result_buckets) |value| saturatingI64(value) else null,
        .planner_lifecycle_ready = item.algebraic_planner_lifecycle_ready,
        .planner_lifecycle_blocking_reason = item.algebraic_planner_lifecycle_blocking_reason,
        .adaptive_progress_count = saturatingI64(item.algebraic_adaptive_progress_count),
        .recommendation_count = saturatingI64(item.algebraic_recommendation_count),
        .adaptive_backfilling_count = saturatingI64(item.algebraic_adaptive_backfilling_count),
        .adaptive_ready_count = saturatingI64(item.algebraic_adaptive_ready_count),
        .adaptive_stale_count = saturatingI64(item.algebraic_adaptive_stale_count),
        .adaptive_cleanup_recommended_count = saturatingI64(item.algebraic_adaptive_dematerialize_recommended_count),
        .last_error_reason = item.algebraic_last_error_reason,
    };
    if (item.algebraic_active_progress) |progress_status| {
        stats.active_progress_lifecycle = progress_status.lifecycle;
        stats.active_progress_rows_processed = saturatingI64(progress_status.rows_processed);
        stats.active_progress_target_rows = saturatingI64(progress_status.target_rows);
    }

    const encoded = try std.json.Stringify.valueAlloc(alloc, stats, .{ .emit_null_optional_fields = false });
    defer alloc.free(encoded);
    if (encoded.len <= 2) return;
    try out.append(alloc, ',');
    try out.appendSlice(alloc, encoded[1 .. encoded.len - 1]);
}

fn saturatingI64(value: u64) i64 {
    return std.math.cast(i64, value) orelse std.math.maxInt(i64);
}

fn appendIndexRuntimeStatus(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    index_type: ApiIndexType,
    embeddings_require_table_coverage: bool,
    embeddings_sparse: bool,
    graph_source_status: ?GraphSourceStatus,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
    shard_view: bool,
) !void {
    if (shard_view) {
        try out.append(alloc, '{');
        var emitted = false;
        var emitted_expected: []bool = &.{};
        if (expected_group_ids.len > 0) {
            emitted_expected = try alloc.alloc(bool, expected_group_ids.len);
            @memset(emitted_expected, false);
        }
        defer if (emitted_expected.len > 0) alloc.free(emitted_expected);

        if (local_statuses) |runtime| {
            for (runtime.items) |item_runtime| {
                const expected_index = if (expected_group_ids.len > 0)
                    expectedGroupIndex(expected_group_ids, item_runtime.group_id) orelse continue
                else
                    null;
                const item = findIndexStatus(item_runtime.stats.indexes, index_name) orelse continue;
                if (expected_index) |i| emitted_expected[i] = true;
                if (emitted) try out.append(alloc, ',');
                emitted = true;
                const key = if (item_runtime.group_id != 0)
                    try std.fmt.allocPrint(alloc, "{d}", .{item_runtime.group_id})
                else
                    try alloc.dupe(u8, "local");
                defer alloc.free(key);
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendSingleIndexRuntimeStatus(alloc, out, index_type, item, item_runtime.stats.doc_count, embeddings_require_table_coverage, embeddings_sparse, graph_source_status, item_runtime.stats.async_indexing, if (index_type == .embeddings) item_runtime.stats.enrichment else null, item_runtime.metadata, runtime_status.statusHasRuntimeFacts(item_runtime));
            }
        }
        if (expected_group_ids.len > 0) {
            const missing = missingAggregateIndexStatus(1);
            for (expected_group_ids, 0..) |group_id, i| {
                if (emitted_expected[i]) continue;
                if (emitted) try out.append(alloc, ',');
                emitted = true;
                const key = try std.fmt.allocPrint(alloc, "{d}", .{group_id});
                defer alloc.free(key);
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendSingleIndexRuntimeStatus(alloc, out, index_type, missing, 0, embeddings_require_table_coverage, embeddings_sparse, graph_source_status, .{}, null, .{
                    .source = .synthetic_config,
                    .freshness = .missing,
                }, false);
            }
        }
        try out.append(alloc, '}');
        return;
    }

    const aggregate = if (local_statuses) |runtime|
        aggregateIndexStatus(runtime.items, index_name, expected_group_ids)
    else if (expected_group_ids.len > 0)
        missingAggregateIndexStatus(expected_group_ids.len)
    else
        null;
    const item = aggregate orelse {
        try out.appendSlice(alloc, "{}");
        return;
    };
    try appendSingleIndexRuntimeStatus(alloc, out, index_type, item, item.table_doc_count, embeddings_require_table_coverage, embeddings_sparse, graph_source_status, item.async_indexing, if (index_type == .embeddings) item.enrichment else null, null, item.runtime_present);
}

const AggregatedIndexStatus = struct {
    backfill_active: bool = false,
    backfill_progress: f64 = 0.0,
    table_doc_count: u64 = 0,
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    replay_applied_sequence: u64 = 0,
    replay_target_sequence: u64 = 0,
    replay_catch_up_required: bool = false,
    catch_up_active: bool = false,
    catch_up_phase: db_mod.types.DenseCatchUpStats.Phase = .idle,
    catch_up_applied_sequence: u64 = 0,
    catch_up_target_sequence: u64 = 0,
    text_merge: db_mod.types.TextMergeStats = .{},
    hbc_cache: db_mod.types.HbcCacheStats = .{},
    hbc_posting: db_mod.types.HbcPostingStats = .{},
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
    enrichment: db_mod.types.EnrichmentStats = .{},
    expected_group_count: u64 = 0,
    reported_group_count: u64 = 0,
    fresh_group_count: u64 = 0,
    stale_group_count: u64 = 0,
    missing_group_count: u64 = 0,
    remote_unknown_group_count: u64 = 0,
    runtime_present: bool = false,
    runtime_fresh: bool = false,
    algebraic_parse_error_count: u64 = 0,
    algebraic_planner_selected: u64 = 0,
    algebraic_planner_fallback_count: u64 = 0,
    algebraic_planner_last_decision: ?[]const u8 = null,
    algebraic_planner_last_fallback_reason: ?[]const u8 = null,
    algebraic_planner_last_estimated_scan_rows: ?u64 = null,
    algebraic_planner_last_estimated_result_buckets: ?u64 = null,
    algebraic_planner_lifecycle_ready: bool = true,
    algebraic_planner_lifecycle_blocking_reason: ?[]const u8 = null,
    algebraic_graph_traversal_attempt_count: u64 = 0,
    algebraic_graph_traversal_proven_count: u64 = 0,
    algebraic_graph_traversal_rejected_count: u64 = 0,
    algebraic_graph_traversal_fallback_count: u64 = 0,
    algebraic_graph_traversal_result_node_count: u64 = 0,
    algebraic_recommendation_count: u64 = 0,
    algebraic_adaptive_progress_count: u64 = 0,
    algebraic_adaptive_backfilling_count: u64 = 0,
    algebraic_adaptive_ready_count: u64 = 0,
    algebraic_adaptive_stale_count: u64 = 0,
    algebraic_adaptive_dematerialize_recommended_count: u64 = 0,
    algebraic_last_error_reason: ?[]const u8 = null,
    algebraic_schema_version: u32 = 0,
    algebraic_capability_lifecycle_status: ?[]const u8 = null,
    algebraic_active_progress: ?db_mod.types.AlgebraicProgressStatus = null,
};

fn expectedGroupIndex(expected_group_ids: []const u64, group_id: u64) ?usize {
    for (expected_group_ids, 0..) |expected, i| {
        if (expected == group_id) return i;
    }
    return null;
}

fn expectedGroupAllowsStatus(expected_group_ids: []const u64, group_id: u64) bool {
    if (expected_group_ids.len == 0) return true;
    return expectedGroupIndex(expected_group_ids, group_id) != null;
}

fn missingAggregateIndexStatus(expected_group_count: usize) AggregatedIndexStatus {
    return .{
        .backfill_active = true,
        .replay_catch_up_required = true,
        .expected_group_count = @intCast(expected_group_count),
        .missing_group_count = @intCast(expected_group_count),
    };
}

fn statusFreshnessCountsAsFresh(metadata: runtime_status.RuntimeStatusMetadata) bool {
    return switch (metadata.freshness) {
        .fresh, .unknown => true,
        else => false,
    };
}

fn statusFreshnessName(freshness: runtime_status.RuntimeStatusFreshness) []const u8 {
    return @tagName(freshness);
}

fn statusSourceName(source: runtime_status.RuntimeStatusSource) []const u8 {
    return @tagName(source);
}

fn statusFreshnessCountsAsRemoteUnknown(metadata: runtime_status.RuntimeStatusMetadata) bool {
    return metadata.freshness == .remote_unknown;
}

fn aggregateIndexStatus(
    runtimes: []const runtime_status.LocalTableRuntimeStatus,
    index_name: []const u8,
    expected_group_ids: []const u64,
) ?AggregatedIndexStatus {
    var aggregate: AggregatedIndexStatus = .{};
    var found = false;
    var runtime_count: usize = 0;
    var active_count: usize = 0;
    var active_progress_sum: f64 = 0.0;

    for (runtimes) |runtime| {
        if (!expectedGroupAllowsStatus(expected_group_ids, runtime.group_id)) continue;
        const item = findIndexStatus(runtime.stats.indexes, index_name) orelse continue;
        found = true;
        const runtime_present = runtime_status.statusHasRuntimeFacts(runtime);
        if (!runtime_present) continue;
        runtime_count += 1;
        aggregate.reported_group_count += 1;
        aggregate.runtime_present = true;
        if (statusFreshnessCountsAsFresh(runtime.metadata)) {
            aggregate.fresh_group_count += 1;
            aggregate.runtime_fresh = true;
        } else if (statusFreshnessCountsAsRemoteUnknown(runtime.metadata)) {
            aggregate.remote_unknown_group_count += 1;
        } else {
            aggregate.stale_group_count += 1;
        }
        // DBStats.doc_count is intentionally query-visible cardinality, not a
        // primary-store scan count. Managed embedding coverage uses it as an
        // efficient approximation until table cardinality is maintained as a
        // durable counter.
        aggregate.table_doc_count += runtime.stats.doc_count;
        aggregate.doc_count += item.doc_count;
        aggregate.term_count += item.term_count;
        aggregate.edge_count += item.edge_count;
        aggregate.node_count += item.node_count;
        aggregate.root_node = if (runtime_count == 1) item.root_node else 0;
        aggregate.replay_applied_sequence += item.replay_applied_sequence;
        aggregate.replay_target_sequence += item.replay_target_sequence;
        if (item.replay_catch_up_required) aggregate.replay_catch_up_required = true;
        aggregate.catch_up_applied_sequence += item.catch_up_applied_sequence;
        aggregate.catch_up_target_sequence += item.catch_up_target_sequence;
        if (item.catch_up_active) aggregate.catch_up_active = true;
        if (@intFromEnum(item.catch_up_phase) > @intFromEnum(aggregate.catch_up_phase)) aggregate.catch_up_phase = item.catch_up_phase;
        aggregateTextMergeStats(&aggregate.text_merge, item.text_merge);
        aggregateHbcCacheStats(&aggregate.hbc_cache, item.hbc_cache);
        aggregateHbcPostingStats(&aggregate.hbc_posting, item.hbc_posting);
        aggregate.algebraic_parse_error_count += item.algebraic_parse_error_count;
        aggregate.algebraic_planner_selected += item.algebraic_planner_selected;
        aggregate.algebraic_planner_fallback_count += item.algebraic_planner_fallback_count;
        if (item.algebraic_planner_last_decision != null) aggregate.algebraic_planner_last_decision = item.algebraic_planner_last_decision;
        if (item.algebraic_planner_last_fallback_reason != null) aggregate.algebraic_planner_last_fallback_reason = item.algebraic_planner_last_fallback_reason;
        if (item.algebraic_planner_last_estimated_scan_rows != null) aggregate.algebraic_planner_last_estimated_scan_rows = item.algebraic_planner_last_estimated_scan_rows;
        if (item.algebraic_planner_last_estimated_result_buckets != null) aggregate.algebraic_planner_last_estimated_result_buckets = item.algebraic_planner_last_estimated_result_buckets;
        if (!item.algebraic_planner_lifecycle_ready) aggregate.algebraic_planner_lifecycle_ready = false;
        if (item.algebraic_planner_lifecycle_blocking_reason != null) aggregate.algebraic_planner_lifecycle_blocking_reason = item.algebraic_planner_lifecycle_blocking_reason;
        aggregate.algebraic_graph_traversal_attempt_count += item.algebraic_graph_traversal_attempt_count;
        aggregate.algebraic_graph_traversal_proven_count += item.algebraic_graph_traversal_proven_count;
        aggregate.algebraic_graph_traversal_rejected_count += item.algebraic_graph_traversal_rejected_count;
        aggregate.algebraic_graph_traversal_fallback_count += item.algebraic_graph_traversal_fallback_count;
        aggregate.algebraic_graph_traversal_result_node_count += item.algebraic_graph_traversal_result_node_count;
        aggregate.algebraic_recommendation_count += item.algebraic_recommendation_count;
        aggregate.algebraic_adaptive_progress_count += item.algebraic_adaptive_progress_count;
        aggregate.algebraic_adaptive_backfilling_count += item.algebraic_adaptive_backfilling_count;
        aggregate.algebraic_adaptive_ready_count += item.algebraic_adaptive_ready_count;
        aggregate.algebraic_adaptive_stale_count += item.algebraic_adaptive_stale_count;
        aggregate.algebraic_adaptive_dematerialize_recommended_count += item.algebraic_adaptive_dematerialize_recommended_count;
        if (item.algebraic_last_error_reason != null) aggregate.algebraic_last_error_reason = item.algebraic_last_error_reason;
        aggregate.algebraic_schema_version = @max(aggregate.algebraic_schema_version, item.algebraic_schema_version);
        if (item.algebraic_capability_lifecycle_status) |status| {
            if (aggregate.algebraic_capability_lifecycle_status == null or algebraicCapabilityLifecycleRanksHigher(status, aggregate.algebraic_capability_lifecycle_status.?)) {
                aggregate.algebraic_capability_lifecycle_status = status;
            }
        }
        if (item.algebraic_active_progress) |progress| {
            if (aggregate.algebraic_active_progress == null or algebraicProgressSummaryRanksHigher(progress, aggregate.algebraic_active_progress.?)) {
                aggregate.algebraic_active_progress = progress;
            }
        }
        db_mod.types.accumulateAsyncIndexingStats(&aggregate.async_indexing, runtime.stats.async_indexing);
        aggregateEnrichmentStats(&aggregate.enrichment, runtime.stats.enrichment);
        if (item.backfill_active) {
            aggregate.backfill_active = true;
            active_count += 1;
            active_progress_sum += item.backfill_progress;
        }
    }

    aggregate.expected_group_count = if (expected_group_ids.len > 0)
        @intCast(expected_group_ids.len)
    else
        aggregate.reported_group_count;
    aggregate.missing_group_count = aggregate.expected_group_count -| aggregate.reported_group_count;
    if (aggregate.missing_group_count > 0 or aggregate.stale_group_count > 0 or aggregate.remote_unknown_group_count > 0) {
        aggregate.backfill_active = true;
        aggregate.replay_catch_up_required = true;
        if (active_count == 0) aggregate.backfill_progress = 0.0;
    }
    if (!found and expected_group_ids.len == 0) return null;
    if (active_count > 0) aggregate.backfill_progress = active_progress_sum / @as(f64, @floatFromInt(active_count));
    return aggregate;
}

fn algebraicProgressSummaryRanksHigher(
    progress: db_mod.types.AlgebraicProgressStatus,
    selected: db_mod.types.AlgebraicProgressStatus,
) bool {
    if (std.mem.eql(u8, progress.lifecycle, "backfilling") and !std.mem.eql(u8, selected.lifecycle, "backfilling")) return true;
    if (!std.mem.eql(u8, progress.lifecycle, selected.lifecycle)) return false;
    if (progress.target_sequence != selected.target_sequence) return progress.target_sequence > selected.target_sequence;
    return progress.rows_processed > selected.rows_processed;
}

fn algebraicCapabilityLifecycleRanksHigher(status: []const u8, selected: []const u8) bool {
    return algebraicCapabilityLifecycleRank(status) > algebraicCapabilityLifecycleRank(selected);
}

fn algebraicCapabilityLifecycleRank(status: []const u8) u8 {
    if (std.mem.eql(u8, status, "rebuild_required")) return 30;
    if (std.mem.eql(u8, status, "stale")) return 20;
    if (std.mem.eql(u8, status, "backfilling")) return 10;
    if (std.mem.eql(u8, status, "current")) return 0;
    return 5;
}

fn aggregateEnrichmentStats(dst: *db_mod.types.EnrichmentStats, src: db_mod.types.EnrichmentStats) void {
    dst.enabled = dst.enabled or src.enabled;
    dst.lease_owned = dst.lease_owned and src.lease_owned;
    dst.has_lease = dst.has_lease or src.has_lease;
    dst.acquisition_count += src.acquisition_count;
    dst.lease_acquire_failures += src.lease_acquire_failures;
    dst.lost_leases += src.lost_leases;
    dst.last_acquired_ms = @max(dst.last_acquired_ms, src.last_acquired_ms);
    dst.target_sequence += src.target_sequence;
    dst.applied_sequence += src.applied_sequence;
    dst.processed_requests += src.processed_requests;
    dst.error_count += src.error_count;
    dst.retryable_error_count += src.retryable_error_count;
    dst.fatal_error_count += src.fatal_error_count;
    dst.retrying = dst.retrying or src.retrying;
    dst.worker_failed = dst.worker_failed or src.worker_failed;
    dst.skip_by_hash_count += src.skip_by_hash_count;
    dst.codec_decode_failures += src.codec_decode_failures;
    dst.dense_artifact_bytes_written += src.dense_artifact_bytes_written;
    dst.sparse_artifact_bytes_written += src.sparse_artifact_bytes_written;
    dst.chunk_artifact_bytes_written += src.chunk_artifact_bytes_written;
    dst.artifact_bytes_written += src.artifact_bytes_written;
}

fn aggregateTextMergeStats(dst: *db_mod.types.TextMergeStats, src: db_mod.types.TextMergeStats) void {
    dst.pending_indexes += src.pending_indexes;
    dst.pending_segments += src.pending_segments;
    dst.pending_bytes += src.pending_bytes;
    dst.in_flight_merges += src.in_flight_merges;
    dst.in_flight_segments += src.in_flight_segments;
    dst.completed_merges += src.completed_merges;
    dst.skipped_stale_merges += src.skipped_stale_merges;
    dst.failed_merges += src.failed_merges;
    dst.quarantined_merges += src.quarantined_merges;
    dst.quarantined_segments += src.quarantined_segments;
    dst.deferred_for_pressure += src.deferred_for_pressure;
    if (dst.last_merge_error.len == 0 and src.last_merge_error.len > 0) dst.last_merge_error = src.last_merge_error;
    if (src.retry_after_ns > 0 and (dst.retry_after_ns == 0 or src.retry_after_ns < dst.retry_after_ns)) dst.retry_after_ns = src.retry_after_ns;
}

fn aggregateHbcCacheKindStats(dst: *db_mod.types.HbcCacheKindStats, src: db_mod.types.HbcCacheKindStats) void {
    dst.used_bytes += src.used_bytes;
    dst.peak_bytes += src.peak_bytes;
    dst.insertions += src.insertions;
    dst.admission_skips += src.admission_skips;
    dst.evictions += src.evictions;
}

fn aggregateHbcCacheStats(dst: *db_mod.types.HbcCacheStats, src: db_mod.types.HbcCacheStats) void {
    dst.total_bytes += src.total_bytes;
    dst.accounted_bytes += src.accounted_bytes;
    aggregateHbcCacheKindStats(&dst.node, src.node);
    aggregateHbcCacheKindStats(&dst.quantized, src.quantized);
    aggregateHbcCacheKindStats(&dst.vector, src.vector);
    aggregateHbcCacheKindStats(&dst.metadata, src.metadata);
}

fn aggregateHbcPostingStats(dst: *db_mod.types.HbcPostingStats, src: db_mod.types.HbcPostingStats) void {
    dst.scanned_nodes += src.scanned_nodes;
    dst.scanned_postings += src.scanned_postings;
    dst.dirty_postings += src.dirty_postings;
    dst.centroid_dirty_postings += src.centroid_dirty_postings;
    dst.payload_dirty_postings += src.payload_dirty_postings;
    dst.max_centroid_version_lag = @max(dst.max_centroid_version_lag, src.max_centroid_version_lag);
    dst.max_payload_version_lag = @max(dst.max_payload_version_lag, src.max_payload_version_lag);
    dst.max_mutation_version = @max(dst.max_mutation_version, src.max_mutation_version);
    dst.skipped_missing += src.skipped_missing;
    dst.maintenance_scanned_nodes += src.maintenance_scanned_nodes;
    dst.maintenance_scanned_postings += src.maintenance_scanned_postings;
    dst.maintenance_dirty_postings += src.maintenance_dirty_postings;
    dst.maintenance_repaired_postings += src.maintenance_repaired_postings;
    dst.maintenance_centroid_refreshed += src.maintenance_centroid_refreshed;
    dst.maintenance_payload_refreshed += src.maintenance_payload_refreshed;
    dst.maintenance_ancestor_refresh_roots += src.maintenance_ancestor_refresh_roots;
    dst.maintenance_split_postings += src.maintenance_split_postings;
    dst.maintenance_merged_postings += src.maintenance_merged_postings;
    dst.maintenance_boundary_reassigned_vectors += src.maintenance_boundary_reassigned_vectors;
    dst.lazy_centroid_deferrals += src.lazy_centroid_deferrals;
    dst.lazy_payload_deferrals += src.lazy_payload_deferrals;
    dst.lazy_ancestor_deferrals += src.lazy_ancestor_deferrals;
}

const EmbeddingsRuntimeView = struct {
    backfill_active: bool,
    backfill_progress: f64,
    replay_applied_sequence: u64,
    replay_target_sequence: u64,
    replay_catch_up_required: bool,
};

fn embeddingsRuntimeView(item: anytype, table_doc_count: u64, require_table_coverage: bool, sparse: bool, enrichment: ?db_mod.types.EnrichmentStats) EmbeddingsRuntimeView {
    var view: EmbeddingsRuntimeView = .{
        .backfill_active = item.backfill_active,
        .backfill_progress = item.backfill_progress,
        .replay_applied_sequence = item.replay_applied_sequence,
        .replay_target_sequence = item.replay_target_sequence,
        .replay_catch_up_required = item.replay_catch_up_required,
    };
    const coverage_incomplete = aggregateRuntimeCoverageIncomplete(item);
    const dense_coverage_complete = !require_table_coverage or (table_doc_count > 0 and item.doc_count >= table_doc_count);
    if (enrichment) |stats| {
        const index_applied_sequence = view.replay_applied_sequence;
        const index_target_sequence = view.replay_target_sequence;
        view.replay_target_sequence = @max(index_target_sequence, stats.target_sequence);
        view.replay_applied_sequence = if (index_target_sequence == 0)
            stats.applied_sequence
        else if (stats.target_sequence == 0)
            index_applied_sequence
        else
            @min(index_applied_sequence, stats.applied_sequence);
        if (view.replay_applied_sequence < view.replay_target_sequence) {
            view.replay_catch_up_required = true;
            view.backfill_active = true;
            view.backfill_progress = @min(
                1.0,
                @as(f64, @floatFromInt(view.replay_applied_sequence)) /
                    @as(f64, @floatFromInt(view.replay_target_sequence)),
            );
        } else if (stats.retrying) {
            view.backfill_active = true;
            if (view.backfill_progress >= 1.0) view.backfill_progress = 0.999;
        }
    }
    const enrichment_blocked = if (enrichment) |stats|
        stats.enabled and (stats.retrying or stats.worker_failed)
    else
        false;
    if (!coverage_incomplete and dense_coverage_complete and item.doc_count > 0 and !enrichment_blocked) {
        view.replay_applied_sequence = @max(view.replay_applied_sequence, view.replay_target_sequence);
        view.replay_catch_up_required = false;
        view.backfill_active = false;
        view.backfill_progress = 1.0;
        return view;
    }
    const replay_ready = view.replay_target_sequence > 0 and
        view.replay_target_sequence <= view.replay_applied_sequence and
        !(if (enrichment) |stats| stats.retrying or stats.worker_failed else false);
    const enrichment_pending = if (enrichment) |stats|
        stats.enabled and (stats.worker_failed or stats.retrying or stats.applied_sequence < stats.target_sequence)
    else
        false;
    const artifact_visible = embeddingsArtifactVisible(item, sparse);
    if (replay_ready and !artifact_visible and view.replay_target_sequence > 0) {
        view.backfill_active = true;
        view.backfill_progress = 0.0;
        return view;
    }
    if (!coverage_incomplete and dense_coverage_complete and item.doc_count > 0 and !enrichment_pending) {
        view.backfill_active = false;
        view.backfill_progress = 1.0;
    } else if (!coverage_incomplete and replay_ready and item.doc_count > 0 and (!require_table_coverage or table_doc_count == 0)) {
        view.backfill_active = false;
        view.backfill_progress = 1.0;
    } else if (require_table_coverage and table_doc_count > 0 and item.doc_count < table_doc_count) {
        view.backfill_active = true;
        if (view.replay_target_sequence > 0 and view.replay_applied_sequence >= view.replay_target_sequence) {
            view.replay_applied_sequence = view.replay_target_sequence - 1;
            view.replay_catch_up_required = true;
        }
        view.backfill_progress = @min(
            1.0,
            @as(f64, @floatFromInt(item.doc_count)) /
                @as(f64, @floatFromInt(table_doc_count)),
        );
    }
    return view;
}

fn aggregateRuntimeCoverageIncomplete(item: anytype) bool {
    const Item = @TypeOf(item);
    if (@hasField(Item, "missing_group_count") and item.missing_group_count > 0) return true;
    if (@hasField(Item, "stale_group_count") and item.stale_group_count > 0) return true;
    if (@hasField(Item, "remote_unknown_group_count") and item.remote_unknown_group_count > 0) return true;
    return false;
}

fn embeddingsArtifactVisible(item: anytype, sparse: bool) bool {
    if (sparse) return item.doc_count > 0;
    return item.doc_count > 0 and (item.node_count > 0 or item.root_node > 0);
}

fn backfillState(index_type: ApiIndexType, active: bool, replay_applied_sequence: u64, replay_target_sequence: u64, enrichment: ?db_mod.types.EnrichmentStats) []const u8 {
    if (index_type == .embeddings) {
        _ = replay_applied_sequence;
        _ = replay_target_sequence;
        if (active) {
            if (enrichment) |stats| {
                if (stats.worker_failed) return "failed";
                if (stats.retrying) return "retrying";
            }
            return "running";
        }
        if (enrichment) |stats| {
            if (stats.worker_failed) return "failed";
        }
        return "ready";
    }
    return if (active) "running" else "ready";
}

fn appendEnrichmentRuntimeStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.EnrichmentStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"enabled\":");
    try out.appendSlice(alloc, if (stats.enabled) "true" else "false");
    try out.appendSlice(alloc, ",\"target_sequence\":");
    try appendIntValue(alloc, out, stats.target_sequence);
    try out.appendSlice(alloc, ",\"applied_sequence\":");
    try appendIntValue(alloc, out, stats.applied_sequence);
    try out.appendSlice(alloc, ",\"pending_sequence_count\":");
    try appendIntValue(alloc, out, stats.target_sequence -| stats.applied_sequence);
    try out.appendSlice(alloc, ",\"processed_requests\":");
    try appendIntValue(alloc, out, stats.processed_requests);
    try out.appendSlice(alloc, ",\"error_count\":");
    try appendIntValue(alloc, out, stats.error_count);
    try out.appendSlice(alloc, ",\"retryable_error_count\":");
    try appendIntValue(alloc, out, stats.retryable_error_count);
    try out.appendSlice(alloc, ",\"fatal_error_count\":");
    try appendIntValue(alloc, out, stats.fatal_error_count);
    try out.appendSlice(alloc, ",\"retrying\":");
    try out.appendSlice(alloc, if (stats.retrying) "true" else "false");
    try out.appendSlice(alloc, ",\"worker_failed\":");
    try out.appendSlice(alloc, if (stats.worker_failed) "true" else "false");
    try out.appendSlice(alloc, ",\"skip_by_hash_count\":");
    try appendIntValue(alloc, out, stats.skip_by_hash_count);
    try out.appendSlice(alloc, ",\"codec_decode_failures\":");
    try appendIntValue(alloc, out, stats.codec_decode_failures);
    try out.appendSlice(alloc, ",\"embed_batches_started\":");
    try appendIntValue(alloc, out, stats.embed_batches_started);
    try out.appendSlice(alloc, ",\"embed_batches_completed\":");
    try appendIntValue(alloc, out, stats.embed_batches_completed);
    try out.appendSlice(alloc, ",\"embed_items_started\":");
    try appendIntValue(alloc, out, stats.embed_items_started);
    try out.appendSlice(alloc, ",\"embed_items_completed\":");
    try appendIntValue(alloc, out, stats.embed_items_completed);
    try out.appendSlice(alloc, ",\"active_embed_batch_items\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_items);
    try out.appendSlice(alloc, ",\"active_embed_batch_bytes\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_bytes);
    try out.appendSlice(alloc, ",\"active_embed_batch_max_bytes\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_max_bytes);
    try out.appendSlice(alloc, ",\"active_embed_batch_started_ms\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_started_ms);
    try out.appendSlice(alloc, ",\"last_embed_batch_items\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_items);
    try out.appendSlice(alloc, ",\"last_embed_batch_bytes\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_bytes);
    try out.appendSlice(alloc, ",\"last_embed_batch_max_bytes\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_max_bytes);
    try out.appendSlice(alloc, ",\"last_embed_batch_ns\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_ns);
    try out.appendSlice(alloc, ",\"total_embed_ns\":");
    try appendIntValue(alloc, out, stats.total_embed_ns);
    try out.append(alloc, '}');
}

fn appendSingleIndexRuntimeStatus(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_type: ApiIndexType,
    item: anytype,
    table_doc_count: u64,
    embeddings_require_table_coverage: bool,
    embeddings_sparse: bool,
    graph_source_status: ?GraphSourceStatus,
    async_indexing: db_mod.types.AsyncIndexingStats,
    enrichment: ?db_mod.types.EnrichmentStats,
    metadata: ?runtime_status.RuntimeStatusMetadata,
    runtime_present: bool,
) !void {
    const embeddings_view = if (index_type == .embeddings)
        embeddingsRuntimeView(item, table_doc_count, embeddings_require_table_coverage, embeddings_sparse, enrichment)
    else
        null;
    var backfill_active = if (embeddings_view) |view| view.backfill_active else item.backfill_active;
    var backfill_progress = if (embeddings_view) |view| view.backfill_progress else item.backfill_progress;
    var replay_applied_sequence = if (embeddings_view) |view| view.replay_applied_sequence else item.replay_applied_sequence;
    var replay_target_sequence = if (embeddings_view) |view| view.replay_target_sequence else item.replay_target_sequence;
    var replay_catch_up_required = if (embeddings_view) |view| view.replay_catch_up_required else item.replay_catch_up_required;
    const dense_catch_up = async_indexing.dense_catch_up;
    var catch_up_active = item.catch_up_active;
    var catch_up_phase = item.catch_up_phase;
    var catch_up_applied_sequence = item.catch_up_applied_sequence;
    var catch_up_target_sequence = item.catch_up_target_sequence;
    if (index_type == .embeddings) {
        if (dense_catch_up.active) {
            catch_up_active = true;
            catch_up_phase = dense_catch_up.phase;
            catch_up_applied_sequence = @max(catch_up_applied_sequence, dense_catch_up.current_sequence);
            catch_up_target_sequence = @max(catch_up_target_sequence, dense_catch_up.current_target_sequence);
        }
    }
    if (catch_up_active or catch_up_target_sequence > catch_up_applied_sequence) {
        replay_catch_up_required = true;
        backfill_active = true;
        replay_target_sequence = @max(replay_target_sequence, catch_up_target_sequence);
        if (catch_up_applied_sequence != 0) {
            replay_applied_sequence = if (replay_applied_sequence == 0)
                catch_up_applied_sequence
            else
                @min(replay_applied_sequence, catch_up_applied_sequence);
        }
        if (replay_target_sequence > 0) {
            backfill_progress = @min(
                0.999,
                @as(f64, @floatFromInt(replay_applied_sequence)) /
                    @as(f64, @floatFromInt(replay_target_sequence)),
            );
        }
    }
    if (catch_up_active and catch_up_phase == .idle and replay_catch_up_required) catch_up_phase = .replay;

    try out.append(alloc, '{');
    if (index_type != .algebraic) {
        try appendJsonString(alloc, out, "index_type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, indexTypeName(index_type));
        try out.append(alloc, ',');
    }
    try out.appendSlice(alloc, "\"rebuilding\":");
    try out.appendSlice(alloc, if (backfill_active) "true" else "false");
    switch (index_type) {
        .full_text, .embeddings, .algebraic => {
            try out.appendSlice(alloc, ",\"total_indexed\":");
            try appendIntValue(alloc, out, item.doc_count);
        },
        .graph => {
            try out.appendSlice(alloc, ",\"total_edges\":");
            try appendIntValue(alloc, out, item.edge_count);
        },
    }
    if (index_type == .embeddings) {
        try out.appendSlice(alloc, ",\"total_terms\":");
        try appendIntValue(alloc, out, item.term_count);
        try out.appendSlice(alloc, ",\"total_nodes\":");
        try appendIntValue(alloc, out, item.node_count);
    }
    try out.append(alloc, ',');
    try appendJsonString(alloc, out, "backfill_active");
    try out.appendSlice(alloc, if (backfill_active) ":true" else ":false");
    try out.appendSlice(alloc, ",\"backfill_progress\":");
    const progress = try std.fmt.allocPrint(alloc, "{d:.3}", .{backfill_progress});
    defer alloc.free(progress);
    try out.appendSlice(alloc, progress);
    try out.appendSlice(alloc, ",\"backfill_state\":");
    try appendJsonString(alloc, out, backfillState(index_type, backfill_active, replay_applied_sequence, replay_target_sequence, enrichment));
    try out.appendSlice(alloc, ",\"doc_count\":");
    try appendIntValue(alloc, out, item.doc_count);
    try out.appendSlice(alloc, ",\"term_count\":");
    try appendIntValue(alloc, out, item.term_count);
    try out.appendSlice(alloc, ",\"edge_count\":");
    try appendIntValue(alloc, out, item.edge_count);
    try out.appendSlice(alloc, ",\"node_count\":");
    try appendIntValue(alloc, out, item.node_count);
    if (index_type == .embeddings) {
        const artifact_publish_pending = replay_target_sequence > 0 and !embeddingsArtifactVisible(item, embeddings_sparse);
        try out.appendSlice(alloc, ",\"query_visible_doc_count\":");
        try appendIntValue(alloc, out, item.doc_count);
        try out.appendSlice(alloc, ",\"published_doc_count\":");
        try appendIntValue(alloc, out, item.doc_count);
        try out.appendSlice(alloc, ",\"published_node_count\":");
        try appendIntValue(alloc, out, item.node_count);
        try out.appendSlice(alloc, ",\"root_node\":");
        try appendIntValue(alloc, out, item.root_node);
        try out.appendSlice(alloc, ",\"published_root_node\":");
        try appendIntValue(alloc, out, item.root_node);
        try out.appendSlice(alloc, ",\"dense_replay_applied_sequence\":");
        try appendIntValue(alloc, out, replay_applied_sequence);
        try out.appendSlice(alloc, ",\"dense_replay_target_sequence\":");
        try appendIntValue(alloc, out, replay_target_sequence);
        try out.appendSlice(alloc, ",\"dense_publish_pending\":");
        try out.appendSlice(alloc, if (catch_up_active or replay_catch_up_required or artifact_publish_pending) "true" else "false");
    }
    if (index_type == .graph) {
        try out.appendSlice(alloc, ",\"algebraic_graph\":{\"traversal\":{\"attempted\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_attempt_count);
        try out.appendSlice(alloc, ",\"proven\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_proven_count);
        try out.appendSlice(alloc, ",\"rejected\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_rejected_count);
        try out.appendSlice(alloc, ",\"fallback\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_fallback_count);
        try out.appendSlice(alloc, ",\"result_nodes\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_result_node_count);
        try out.appendSlice(alloc, "}}");
        if (graph_source_status) |source| {
            try out.appendSlice(alloc, ",\"source_artifact\":{");
            try appendJsonString(alloc, out, "name");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, source.artifact);
            try out.append(alloc, ',');
            try appendJsonString(alloc, out, "path");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, source.path);
            try out.append(alloc, ',');
            try appendJsonString(alloc, out, "format");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, source.format);
            try out.appendSlice(alloc, ",\"materialization_pending\":");
            try out.appendSlice(alloc, if (catch_up_active or replay_catch_up_required) "true" else "false");
            try out.append(alloc, '}');
        }
    }
    if (index_type == .algebraic) try appendAlgebraicIndexStatsFields(alloc, out, item);
    try out.appendSlice(alloc, ",\"replay_applied_sequence\":");
    try appendIntValue(alloc, out, replay_applied_sequence);
    try out.appendSlice(alloc, ",\"replay_target_sequence\":");
    try appendIntValue(alloc, out, replay_target_sequence);
    try out.appendSlice(alloc, ",\"replay_catch_up_required\":");
    try out.appendSlice(alloc, if (replay_catch_up_required) "true" else "false");
    try out.appendSlice(alloc, ",\"runtime_present\":");
    try out.appendSlice(alloc, if (runtime_present) "true" else "false");
    const runtime_fresh = if (@hasField(@TypeOf(item), "runtime_fresh"))
        item.runtime_fresh
    else if (metadata) |md|
        runtime_present and md.freshness == .fresh
    else
        false;
    try out.appendSlice(alloc, ",\"runtime_fresh\":");
    try out.appendSlice(alloc, if (runtime_fresh) "true" else "false");
    if (metadata) |md| {
        try out.appendSlice(alloc, ",\"runtime_source\":");
        try appendJsonString(alloc, out, statusSourceName(md.source));
        try out.appendSlice(alloc, ",\"runtime_freshness\":");
        try appendJsonString(alloc, out, statusFreshnessName(md.freshness));
    }
    try out.appendSlice(alloc, ",\"catch_up_active\":");
    try out.appendSlice(alloc, if (catch_up_active) "true" else "false");
    try out.appendSlice(alloc, ",\"catch_up_phase\":\"");
    try out.appendSlice(alloc, @tagName(catch_up_phase));
    try out.append(alloc, '"');
    try out.appendSlice(alloc, ",\"catch_up_applied_sequence\":");
    try appendIntValue(alloc, out, catch_up_applied_sequence);
    try out.appendSlice(alloc, ",\"catch_up_target_sequence\":");
    try appendIntValue(alloc, out, catch_up_target_sequence);
    if (@hasField(@TypeOf(item), "expected_group_count")) {
        try out.appendSlice(alloc, ",\"expected_groups\":");
        try appendIntValue(alloc, out, item.expected_group_count);
        try out.appendSlice(alloc, ",\"reported_groups\":");
        try appendIntValue(alloc, out, item.reported_group_count);
        try out.appendSlice(alloc, ",\"fresh_groups\":");
        try appendIntValue(alloc, out, item.fresh_group_count);
        try out.appendSlice(alloc, ",\"stale_groups\":");
        try appendIntValue(alloc, out, item.stale_group_count);
        try out.appendSlice(alloc, ",\"missing_groups\":");
        try appendIntValue(alloc, out, item.missing_group_count);
        try out.appendSlice(alloc, ",\"unknown_remote_groups\":");
        try appendIntValue(alloc, out, item.remote_unknown_group_count);
    }
    if (index_type == .full_text) {
        try out.appendSlice(alloc, ",\"text_merge\":");
        try appendTextMergeStatus(alloc, out, item.text_merge);
    }
    if (index_type == .embeddings) {
        try out.appendSlice(alloc, ",\"hbc_cache\":");
        try appendHbcCacheStatus(alloc, out, item.hbc_cache);
        try out.appendSlice(alloc, ",\"hbc_posting\":");
        try appendHbcPostingStatus(alloc, out, item.hbc_posting);
        try out.appendSlice(alloc, ",\"enrichment_runtime\":");
        try appendEnrichmentRuntimeStatus(alloc, out, enrichment orelse .{});
    }
    try out.appendSlice(alloc, ",\"async_indexing\":");
    try appendAsyncIndexingStatus(alloc, out, async_indexing);
    try out.append(alloc, '}');
}

fn appendDbMutexStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.DBMutexStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"lock_calls\":");
    try appendIntValue(alloc, out, stats.lock_calls);
    try out.appendSlice(alloc, ",\"contended_calls\":");
    try appendIntValue(alloc, out, stats.contended_calls);
    try out.appendSlice(alloc, ",\"max_waiters\":");
    try appendIntValue(alloc, out, stats.max_waiters);
    try out.appendSlice(alloc, ",\"spin_loops\":");
    try appendIntValue(alloc, out, stats.spin_loops);
    try out.appendSlice(alloc, ",\"yield_loops\":");
    try appendIntValue(alloc, out, stats.yield_loops);
    try out.appendSlice(alloc, ",\"sleep_loops\":");
    try appendIntValue(alloc, out, stats.sleep_loops);
    try out.appendSlice(alloc, ",\"wait_ns\":");
    try appendIntValue(alloc, out, stats.wait_ns);
    try out.appendSlice(alloc, ",\"max_wait_ns\":");
    try appendIntValue(alloc, out, stats.max_wait_ns);
    try out.appendSlice(alloc, ",\"hold_ns\":");
    try appendIntValue(alloc, out, stats.hold_ns);
    try out.appendSlice(alloc, ",\"max_hold_ns\":");
    try appendIntValue(alloc, out, stats.max_hold_ns);
    try out.append(alloc, '}');
}

fn appendAppliedSequenceStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.AppliedSequenceStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"note_calls\":");
    try appendIntValue(alloc, out, stats.note_calls);
    try out.appendSlice(alloc, ",\"forced_flush_calls\":");
    try appendIntValue(alloc, out, stats.forced_flush_calls);
    try out.appendSlice(alloc, ",\"skipped_flush_calls\":");
    try appendIntValue(alloc, out, stats.skipped_flush_calls);
    try out.appendSlice(alloc, ",\"flush_calls\":");
    try appendIntValue(alloc, out, stats.flush_calls);
    try out.appendSlice(alloc, ",\"flushed_indexes\":");
    try appendIntValue(alloc, out, stats.flushed_indexes);
    try out.appendSlice(alloc, ",\"sync_ns\":");
    try appendIntValue(alloc, out, stats.sync_ns);
    try out.appendSlice(alloc, ",\"save_ns\":");
    try appendIntValue(alloc, out, stats.save_ns);
    try out.appendSlice(alloc, ",\"flush_ns\":");
    try appendIntValue(alloc, out, stats.flush_ns);
    try out.appendSlice(alloc, ",\"max_flush_ns\":");
    try appendIntValue(alloc, out, stats.max_flush_ns);
    try out.append(alloc, '}');
}

fn appendDenseCatchUpStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.DenseCatchUpStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"begin_calls\":");
    try appendIntValue(alloc, out, stats.begin_calls);
    try out.appendSlice(alloc, ",\"finish_calls\":");
    try appendIntValue(alloc, out, stats.finish_calls);
    try out.appendSlice(alloc, ",\"abort_calls\":");
    try appendIntValue(alloc, out, stats.abort_calls);
    try out.appendSlice(alloc, ",\"active\":");
    try out.appendSlice(alloc, if (stats.active) "true" else "false");
    try out.appendSlice(alloc, ",\"phase\":\"");
    try out.appendSlice(alloc, @tagName(stats.phase));
    try out.append(alloc, '"');
    try out.appendSlice(alloc, ",\"current_sequence\":");
    try appendIntValue(alloc, out, stats.current_sequence);
    try out.appendSlice(alloc, ",\"current_target_sequence\":");
    try appendIntValue(alloc, out, stats.current_target_sequence);
    try out.appendSlice(alloc, ",\"current_scanned_entries\":");
    try appendIntValue(alloc, out, stats.current_scanned_entries);
    try out.appendSlice(alloc, ",\"current_applied_entries\":");
    try appendIntValue(alloc, out, stats.current_applied_entries);
    try out.appendSlice(alloc, ",\"progress_updates\":");
    try appendIntValue(alloc, out, stats.progress_updates);
    try out.appendSlice(alloc, ",\"bulk_finish_windows\":");
    try appendIntValue(alloc, out, stats.bulk_finish_windows);
    try out.appendSlice(alloc, ",\"bulk_finish_split_steps\":");
    try appendIntValue(alloc, out, stats.bulk_finish_split_steps);
    try out.appendSlice(alloc, ",\"bulk_finish_deferred_leaf_splits\":");
    try appendIntValue(alloc, out, stats.bulk_finish_deferred_leaf_splits);
    try out.appendSlice(alloc, ",\"bulk_finish_current_window\":");
    try appendIntValue(alloc, out, stats.bulk_finish_current_window);
    try out.appendSlice(alloc, ",\"bulk_finish_current_window_split_steps\":");
    try appendIntValue(alloc, out, stats.bulk_finish_current_window_split_steps);
    try out.appendSlice(alloc, ",\"bulk_finish_current_window_ns\":");
    try appendIntValue(alloc, out, stats.bulk_finish_current_window_ns);
    try out.appendSlice(alloc, ",\"bulk_finish_max_window_ns\":");
    try appendIntValue(alloc, out, stats.bulk_finish_max_window_ns);
    try out.appendSlice(alloc, ",\"finish_ns\":");
    try appendIntValue(alloc, out, stats.finish_ns);
    try out.appendSlice(alloc, ",\"max_finish_ns\":");
    try appendIntValue(alloc, out, stats.max_finish_ns);
    try out.appendSlice(alloc, ",\"finalize_ns\":");
    try appendIntValue(alloc, out, stats.finalize_ns);
    try out.appendSlice(alloc, ",\"max_finalize_ns\":");
    try appendIntValue(alloc, out, stats.max_finalize_ns);
    try out.appendSlice(alloc, ",\"maintenance_calls\":");
    try appendIntValue(alloc, out, stats.maintenance_calls);
    try out.appendSlice(alloc, ",\"maintenance_steps\":");
    try appendIntValue(alloc, out, stats.maintenance_steps);
    try out.appendSlice(alloc, ",\"maintenance_ns\":");
    try appendIntValue(alloc, out, stats.maintenance_ns);
    try out.appendSlice(alloc, ",\"max_maintenance_ns\":");
    try appendIntValue(alloc, out, stats.max_maintenance_ns);
    try out.appendSlice(alloc, ",\"manifest_writes\":");
    try appendIntValue(alloc, out, stats.manifest_writes);
    try out.appendSlice(alloc, ",\"manifest_ns\":");
    try appendIntValue(alloc, out, stats.manifest_ns);
    try out.appendSlice(alloc, ",\"write_pressure_compactions\":");
    try appendIntValue(alloc, out, stats.write_pressure_compactions);
    try out.appendSlice(alloc, ",\"write_pressure_ns\":");
    try appendIntValue(alloc, out, stats.write_pressure_ns);
    try out.append(alloc, '}');
}

fn appendAsyncIndexingStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.AsyncIndexingStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"apply_mutex\":");
    try appendDbMutexStatus(alloc, out, stats.apply_mutex);
    try out.appendSlice(alloc, ",\"applied_sequence_mutex\":");
    try appendDbMutexStatus(alloc, out, stats.applied_sequence_mutex);
    try out.appendSlice(alloc, ",\"dense_finish_mutex\":");
    try appendDbMutexStatus(alloc, out, stats.dense_finish_mutex);
    try out.appendSlice(alloc, ",\"applied_sequence\":");
    try appendAppliedSequenceStatus(alloc, out, stats.applied_sequence);
    try out.appendSlice(alloc, ",\"startup\":");
    try appendStartupCatchUpStatus(alloc, out, stats.startup);
    try out.appendSlice(alloc, ",\"dense_catch_up\":");
    try appendDenseCatchUpStatus(alloc, out, stats.dense_catch_up);
    try out.append(alloc, '}');
}

fn appendStartupCatchUpStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.StartupCatchUpStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"active\":");
    try out.appendSlice(alloc, if (stats.active) "true" else "false");
    try out.appendSlice(alloc, ",\"phase\":");
    try appendJsonString(alloc, out, switch (stats.phase) {
        .idle => "idle",
        .opening_db => "opening_db",
        .artifact_rebuild => "artifact_rebuild",
        .startup_catch_up => "startup_catch_up",
    });
    try out.appendSlice(alloc, ",\"wal_retained_segments\":");
    try appendIntValue(alloc, out, stats.wal_retained_segments);
    try out.appendSlice(alloc, ",\"wal_retained_bytes\":");
    try appendIntValue(alloc, out, stats.wal_retained_bytes);
    try out.appendSlice(alloc, ",\"configured_indexes\":");
    try appendIntValue(alloc, out, stats.configured_indexes);
    try out.appendSlice(alloc, ",\"configured_dense_indexes\":");
    try appendIntValue(alloc, out, stats.configured_dense_indexes);
    try out.appendSlice(alloc, ",\"configured_sparse_indexes\":");
    try appendIntValue(alloc, out, stats.configured_sparse_indexes);
    try out.appendSlice(alloc, ",\"configured_full_text_indexes\":");
    try appendIntValue(alloc, out, stats.configured_full_text_indexes);
    try out.appendSlice(alloc, ",\"configured_graph_indexes\":");
    try appendIntValue(alloc, out, stats.configured_graph_indexes);
    try out.appendSlice(alloc, ",\"opened_indexes\":");
    try appendIntValue(alloc, out, stats.opened_indexes);
    try out.appendSlice(alloc, ",\"db_open_ns\":");
    try appendIntValue(alloc, out, stats.db_open_ns);
    try out.appendSlice(alloc, ",\"load_indexes_ns\":");
    try appendIntValue(alloc, out, stats.load_indexes_ns);
    try out.appendSlice(alloc, ",\"wal_replay_records\":");
    try appendIntValue(alloc, out, stats.wal_replay_records);
    try out.appendSlice(alloc, ",\"wal_replay_entries\":");
    try appendIntValue(alloc, out, stats.wal_replay_entries);
    try out.appendSlice(alloc, ",\"wal_replay_bytes\":");
    try appendIntValue(alloc, out, stats.wal_replay_bytes);
    try out.appendSlice(alloc, ",\"wal_replay_ns\":");
    try appendIntValue(alloc, out, stats.wal_replay_ns);
    try out.append(alloc, '}');
}

fn appendHbcCacheKindStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.HbcCacheKindStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"used_bytes\":");
    try appendIntValue(alloc, out, stats.used_bytes);
    try out.appendSlice(alloc, ",\"peak_bytes\":");
    try appendIntValue(alloc, out, stats.peak_bytes);
    try out.appendSlice(alloc, ",\"insertions\":");
    try appendIntValue(alloc, out, stats.insertions);
    try out.appendSlice(alloc, ",\"admission_skips\":");
    try appendIntValue(alloc, out, stats.admission_skips);
    try out.appendSlice(alloc, ",\"evictions\":");
    try appendIntValue(alloc, out, stats.evictions);
    try out.append(alloc, '}');
}

fn appendHbcCacheStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.HbcCacheStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"total_bytes\":");
    try appendIntValue(alloc, out, stats.total_bytes);
    try out.appendSlice(alloc, ",\"accounted_bytes\":");
    try appendIntValue(alloc, out, stats.accounted_bytes);
    try out.appendSlice(alloc, ",\"node\":");
    try appendHbcCacheKindStatus(alloc, out, stats.node);
    try out.appendSlice(alloc, ",\"quantized\":");
    try appendHbcCacheKindStatus(alloc, out, stats.quantized);
    try out.appendSlice(alloc, ",\"vector\":");
    try appendHbcCacheKindStatus(alloc, out, stats.vector);
    try out.appendSlice(alloc, ",\"metadata\":");
    try appendHbcCacheKindStatus(alloc, out, stats.metadata);
    try out.append(alloc, '}');
}

fn appendHbcPostingStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.HbcPostingStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"scanned_nodes\":");
    try appendIntValue(alloc, out, stats.scanned_nodes);
    try out.appendSlice(alloc, ",\"scanned_postings\":");
    try appendIntValue(alloc, out, stats.scanned_postings);
    try out.appendSlice(alloc, ",\"dirty_postings\":");
    try appendIntValue(alloc, out, stats.dirty_postings);
    try out.appendSlice(alloc, ",\"centroid_dirty_postings\":");
    try appendIntValue(alloc, out, stats.centroid_dirty_postings);
    try out.appendSlice(alloc, ",\"payload_dirty_postings\":");
    try appendIntValue(alloc, out, stats.payload_dirty_postings);
    try out.appendSlice(alloc, ",\"max_centroid_version_lag\":");
    try appendIntValue(alloc, out, stats.max_centroid_version_lag);
    try out.appendSlice(alloc, ",\"max_payload_version_lag\":");
    try appendIntValue(alloc, out, stats.max_payload_version_lag);
    try out.appendSlice(alloc, ",\"max_mutation_version\":");
    try appendIntValue(alloc, out, stats.max_mutation_version);
    try out.appendSlice(alloc, ",\"skipped_missing\":");
    try appendIntValue(alloc, out, stats.skipped_missing);
    try out.appendSlice(alloc, ",\"maintenance_scanned_nodes\":");
    try appendIntValue(alloc, out, stats.maintenance_scanned_nodes);
    try out.appendSlice(alloc, ",\"maintenance_scanned_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_scanned_postings);
    try out.appendSlice(alloc, ",\"maintenance_dirty_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_dirty_postings);
    try out.appendSlice(alloc, ",\"maintenance_repaired_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_repaired_postings);
    try out.appendSlice(alloc, ",\"maintenance_centroid_refreshed\":");
    try appendIntValue(alloc, out, stats.maintenance_centroid_refreshed);
    try out.appendSlice(alloc, ",\"maintenance_payload_refreshed\":");
    try appendIntValue(alloc, out, stats.maintenance_payload_refreshed);
    try out.appendSlice(alloc, ",\"maintenance_ancestor_refresh_roots\":");
    try appendIntValue(alloc, out, stats.maintenance_ancestor_refresh_roots);
    try out.appendSlice(alloc, ",\"maintenance_split_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_split_postings);
    try out.appendSlice(alloc, ",\"maintenance_merged_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_merged_postings);
    try out.appendSlice(alloc, ",\"maintenance_boundary_reassigned_vectors\":");
    try appendIntValue(alloc, out, stats.maintenance_boundary_reassigned_vectors);
    try out.appendSlice(alloc, ",\"lazy_centroid_deferrals\":");
    try appendIntValue(alloc, out, stats.lazy_centroid_deferrals);
    try out.appendSlice(alloc, ",\"lazy_payload_deferrals\":");
    try appendIntValue(alloc, out, stats.lazy_payload_deferrals);
    try out.appendSlice(alloc, ",\"lazy_ancestor_deferrals\":");
    try appendIntValue(alloc, out, stats.lazy_ancestor_deferrals);
    try out.append(alloc, '}');
}

fn appendTextMergeStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.TextMergeStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"pending_segments\":");
    try appendIntValue(alloc, out, stats.pending_segments);
    try out.appendSlice(alloc, ",\"pending_bytes\":");
    try appendIntValue(alloc, out, stats.pending_bytes);
    try out.appendSlice(alloc, ",\"in_flight_merges\":");
    try appendIntValue(alloc, out, stats.in_flight_merges);
    try out.appendSlice(alloc, ",\"failed_merges\":");
    try appendIntValue(alloc, out, stats.failed_merges);
    try out.appendSlice(alloc, ",\"quarantined_merges\":");
    try appendIntValue(alloc, out, stats.quarantined_merges);
    try out.appendSlice(alloc, ",\"quarantined_segments\":");
    try appendIntValue(alloc, out, stats.quarantined_segments);
    try out.appendSlice(alloc, ",\"retry_after_ns\":");
    try appendIntValue(alloc, out, stats.retry_after_ns);
    try out.appendSlice(alloc, ",\"deferred_for_pressure\":");
    try appendIntValue(alloc, out, stats.deferred_for_pressure);
    try out.appendSlice(alloc, ",\"last_merge_error\":");
    try appendJsonString(alloc, out, stats.last_merge_error);
    try out.append(alloc, '}');
}

pub fn inferIndexType(index_name: []const u8, config: std.json.Value) ?ApiIndexType {
    if (config != .object) return null;
    if (config.object.get("type")) |type_value| {
        if (type_value != .string) return null;
        if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
        if (std.mem.eql(u8, type_value.string, "embeddings")) return .embeddings;
        if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
        if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
        return null;
    }
    if (config.object.get("dimension") != null or
        config.object.get("sparse") != null or
        config.object.get("chunker") != null or
        config.object.get("embedder") != null or
        config.object.get("generator") != null)
    {
        return .embeddings;
    }
    if (config.object.get("edge_type_configs") != null or
        config.object.get("store_reverse_edges") != null)
    {
        return .graph;
    }
    if (std.mem.eql(u8, index_name, tables_api.default_full_text_index_name)) return .full_text;
    if (std.mem.startsWith(u8, index_name, "full_text_index_v")) return .full_text;
    if (std.mem.eql(u8, index_name, "default")) return .full_text;
    return null;
}

fn appendIntValue(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn findIndexStatus(indexes: []const db_mod.types.DBIndexStats, index_name: []const u8) ?db_mod.types.DBIndexStats {
    for (indexes) |item| {
        if (std.mem.eql(u8, item.name, index_name)) return item;
    }
    return null;
}

test "index encoders expose metadata-backed configs" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded_list = (try encodeIndexList(std.testing.allocator, &snapshot, "docs", null)).?;
    defer std.testing.allocator.free(encoded_list);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"type\":\"full_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"dimension\":384") != null);

    const encoded_single = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "embed_idx", null)).?;
    defer std.testing.allocator.free(encoded_single);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"name\":\"embed_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"type\":\"embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"status\":{\"index_type\":\"embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"runtime_present\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"shard_status\":{}") != null);
}

test "index config map encoder injects canonical name and type" {
    const encoded = try encodeIndexConfigMap(std.testing.allocator, "{\"full_text_index_v0\":{},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}");
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"embed_idx\":{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384}") != null);
}

test "single index config encoder isolates requested index" {
    const encoded = (try encodeSingleIndexConfig(
        std.testing.allocator,
        "{\"full_text_index_v0\":{},\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3}}",
        "full_text_index_v0",
    )).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualStrings(
        "{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}",
        encoded,
    );
}

test "single index config encoder infers shorthand embeddings type" {
    const encoded = (try encodeSingleIndexConfig(
        std.testing.allocator,
        "{\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\"}}}",
        "semantic_chunked_idx",
    )).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"name\":\"semantic_chunked_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"type\":\"embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dimension\":3") != null);
}

test "single index helpers use default index metadata when indexes_json is empty" {
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 0, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded_status = (try encodeSingleIndex(
        std.testing.allocator,
        &snapshot,
        "docs",
        "full_text_index_v0",
        null,
    )).?;
    defer std.testing.allocator.free(encoded_status);
    try std.testing.expect(std.mem.indexOf(u8, encoded_status, "\"name\":\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_status, "\"type\":\"full_text\"") != null);

    const encoded_list = (try encodeIndexList(std.testing.allocator, &snapshot, "docs", null)).?;
    defer std.testing.allocator.free(encoded_list);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"name\":\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"type\":\"full_text\"") != null);

    const encoded_config = (try encodeSingleIndexConfig(
        std.testing.allocator,
        "",
        "full_text_index_v0",
    )).?;
    defer std.testing.allocator.free(encoded_config);
    try std.testing.expectEqualStrings(
        "{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}",
        encoded_config,
    );
}

test "index metadata helpers add and remove entries" {
    const added = try addIndexToTableIndexesJson(std.testing.allocator, "{\"default\":{\"type\":\"full_text\"}}", "embed_idx", "{\"type\":\"embeddings\",\"dimension\":384}");
    defer std.testing.allocator.free(added);
    try std.testing.expect(std.mem.indexOf(u8, added, "\"embed_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, added, "\"dimension\":384") != null);

    const removed = (try removeIndexFromTableIndexesJson(std.testing.allocator, added, "default")).?;
    defer std.testing.allocator.free(removed);
    try std.testing.expect(std.mem.indexOf(u8, removed, "\"default\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, removed, "\"embed_idx\"") != null);
}

test "index encoders expose local shard runtime status" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 12,
        .term_count = 34,
        .backfill_active = true,
        .backfill_progress = 0.5,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
        .catch_up_active = true,
        .catch_up_applied_sequence = 3,
        .catch_up_target_sequence = 5,
        .text_merge = .{
            .pending_segments = 3,
            .quarantined_segments = 2,
            .last_merge_error = "InvalidChunk",
            .deferred_for_pressure = 1,
        },
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 12,
            .index_count = 1,
            .indexes = indexes,
            .async_indexing = .{
                .apply_mutex = .{
                    .lock_calls = 11,
                    .contended_calls = 3,
                },
                .applied_sequence = .{
                    .flush_calls = 5,
                },
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retained_segments = 4,
                    .wal_retained_bytes = 99,
                },
                .dense_catch_up = .{
                    .active = true,
                    .current_sequence = 41,
                    .current_target_sequence = 77,
                    .current_scanned_entries = 1024,
                    .current_applied_entries = 768,
                    .progress_updates = 9,
                },
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_indexed\":12") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.500") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_applied_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"text_merge\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"quarantined_segments\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"last_merge_error\":\"InvalidChunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"async_indexing\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lock_calls\":11") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"flush_calls\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"startup\":{\"active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"phase\":\"opening_db\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_retained_segments\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_retained_bytes\":99") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_sequence\":41") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_target_sequence\":77") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_scanned_entries\":1024") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_applied_entries\":768") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"progress_updates\":9") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
}

test "index encoders expose algebraic graph traversal health" {
    const alloc = std.testing.allocator;
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    defer alloc.free(indexes);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "graph_idx"),
        .kind = .graph,
        .edge_count = 12,
        .node_count = 7,
        .doc_count = 7,
        .algebraic_graph_traversal_attempt_count = 3,
        .algebraic_graph_traversal_proven_count = 2,
        .algebraic_graph_traversal_rejected_count = 1,
        .algebraic_graph_traversal_fallback_count = 4,
        .algebraic_graph_traversal_result_node_count = 9,
    };
    defer alloc.free(indexes[0].name);

    const local_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer alloc.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 7,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"graph_idx\":{\"type\":\"graph\",\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "graph_idx", &local_status)).?;
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_edges\":12") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"algebraic_graph\":{\"traversal\":{\"attempted\":3,\"proven\":2,\"rejected\":1,\"fallback\":4,\"result_nodes\":9}}") != null);
}

test "index encoders expose graph artifact source materialization status" {
    const alloc = std.testing.allocator;
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    defer alloc.free(indexes);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "relations_graph"),
        .kind = .graph,
        .edge_count = 4,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
    };
    defer alloc.free(indexes[0].name);

    const local_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer alloc.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 2,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"relations_graph\":{\"type\":\"graph\",\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\"}}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "relations_graph", &local_status)).?;
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"source_artifact\":{\"name\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\",\"materialization_pending\":true}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
}

test "index encoders expose compact algebraic public status" {
    const alloc = std.testing.allocator;
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    defer alloc.free(indexes);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "alg"),
        .kind = .algebraic,
        .algebraic_parse_error_count = 2,
        .algebraic_last_error_reason = try alloc.dupe(u8, "invalid_json"),
        .algebraic_schema_version = 42,
        .algebraic_capability_lifecycle_status = try alloc.dupe(u8, "stale"),
        .algebraic_planner_selected = 3,
        .algebraic_planner_fallback_count = 1,
        .algebraic_planner_last_decision = try alloc.dupe(u8, "fallback"),
        .algebraic_planner_last_fallback_reason = try alloc.dupe(u8, "schema_lifecycle_not_ready"),
        .algebraic_planner_last_estimated_scan_rows = 61,
        .algebraic_planner_last_estimated_result_buckets = 8,
        .algebraic_planner_lifecycle_ready = false,
        .algebraic_planner_lifecycle_blocking_reason = try alloc.dupe(u8, "capability_lifecycle_not_ready"),
        .algebraic_recommendation_count = 4,
        .algebraic_adaptive_progress_count = 2,
        .algebraic_adaptive_backfilling_count = 1,
        .algebraic_adaptive_ready_count = 1,
        .algebraic_adaptive_stale_count = 0,
        .algebraic_adaptive_dematerialize_recommended_count = 1,
        .algebraic_active_progress = .{
            .recommendation = try alloc.dupe(u8, "recommendation:v2"),
            .materialization_id = try alloc.dupe(u8, "adaptive:v2"),
            .lifecycle = try alloc.dupe(u8, "backfilling"),
            .target_sequence = 50,
            .applied_sequence = 25,
            .rows_processed = 30,
            .target_rows = 60,
        },
    };
    defer {
        alloc.free(indexes[0].name);
        alloc.free(indexes[0].algebraic_last_error_reason.?);
        alloc.free(indexes[0].algebraic_capability_lifecycle_status.?);
        alloc.free(indexes[0].algebraic_planner_last_decision.?);
        alloc.free(indexes[0].algebraic_planner_last_fallback_reason.?);
        alloc.free(indexes[0].algebraic_planner_lifecycle_blocking_reason.?);
        alloc.free(indexes[0].algebraic_active_progress.?.recommendation);
        alloc.free(indexes[0].algebraic_active_progress.?.materialization_id);
        alloc.free(indexes[0].algebraic_active_progress.?.lifecycle);
    }

    const local_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer alloc.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"alg\":{\"type\":\"algebraic\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "alg", &local_status)).?;
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_type\":\"algebraic\"") != null);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, encoded, "\"index_type\""));
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"healthy\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"parse_error_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"schema_version\":42") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"capability_lifecycle_status\":\"stale\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_selected\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_fallback_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_decision\":\"fallback\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_fallback_reason\":\"schema_lifecycle_not_ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_estimated_scan_rows\":61") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_estimated_result_buckets\":8") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_lifecycle_ready\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_lifecycle_blocking_reason\":\"capability_lifecycle_not_ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"recommendation_count\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_progress_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_backfilling_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_ready_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_cleanup_recommended_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"last_error_reason\":\"invalid_json\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_progress_lifecycle\":\"backfilling\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_progress_rows_processed\":30") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_progress_target_rows\":60") != null);
}

test "index status aggregation preserves most severe algebraic capability lifecycle" {
    var rebuild_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_capability_lifecycle_status = "rebuild_required",
    }};
    var current_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_capability_lifecycle_status = "current",
    }};
    var stale_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_capability_lifecycle_status = "stale",
    }};
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{
        .{
            .group_id = 1,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = rebuild_indexes[0..] },
        },
        .{
            .group_id = 2,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = current_indexes[0..] },
        },
        .{
            .group_id = 3,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = stale_indexes[0..] },
        },
    };

    const aggregate = aggregateIndexStatus(runtimes[0..], "alg", &.{}) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("rebuild_required", aggregate.algebraic_capability_lifecycle_status.?);
}

test "index status aggregation reports selected algebraic progress summary shard" {
    const low_progress = [_]db_mod.types.AlgebraicProgressStatus{.{
        .recommendation = "recommendation:low-progress",
        .materialization_id = "adaptive:low-progress",
        .lifecycle = "backfilling",
        .target_sequence = 10,
        .applied_sequence = 2,
        .rows_processed = 4,
        .target_rows = 20,
    }};
    const high_progress = [_]db_mod.types.AlgebraicProgressStatus{.{
        .recommendation = "recommendation:high-progress",
        .materialization_id = "adaptive:high-progress",
        .lifecycle = "backfilling",
        .target_sequence = 50,
        .applied_sequence = 10,
        .rows_processed = 30,
        .target_rows = 60,
    }};
    var low_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_active_progress = low_progress[0],
    }};
    var high_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_active_progress = high_progress[0],
    }};
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{
        .{
            .group_id = 1,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = low_indexes[0..] },
        },
        .{
            .group_id = 2,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = high_indexes[0..] },
        },
    };

    const aggregate = aggregateIndexStatus(runtimes[0..], "alg", &.{}) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("recommendation:high-progress", aggregate.algebraic_active_progress.?.recommendation);
}

test "index encoders aggregate replay debt across local shards" {
    const shard_a_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(shard_a_indexes);
    shard_a_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 4,
        .term_count = 10,
        .backfill_active = true,
        .backfill_progress = 0.4,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
    };
    defer std.testing.allocator.free(shard_a_indexes[0].name);

    const shard_b_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(shard_b_indexes);
    shard_b_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 6,
        .term_count = 14,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 5,
        .replay_catch_up_required = false,
    };
    defer std.testing.allocator.free(shard_b_indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 2);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 4,
            .index_count = 1,
            .indexes = shard_a_indexes,
        },
    };
    local_items[1] = .{
        .group_id = 8,
        .stats = .{
            .doc_count = 6,
            .index_count = 1,
            .indexes = shard_b_indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.400") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"8\":{") != null);
}

test "index status keeps generic catch-up lag pending when replay sequence is equal" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 42,
        .term_count = 128,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 100,
        .replay_catch_up_required = false,
        .catch_up_active = false,
        .catch_up_applied_sequence = 40,
        .catch_up_target_sequence = 100,
        .backfill_active = false,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{ .doc_count = 42, .index_count = 1, .indexes = indexes },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":40") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":100") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
}

test "index encoders aggregate preserved synthetic shard counters" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 1_000_000,
        .node_count = 8_837,
        .root_node = 1,
        .replay_applied_sequence = 10_002,
        .replay_target_sequence = 10_002,
        .catch_up_applied_sequence = 10_002,
        .catch_up_target_sequence = 10_002,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 1_000_000,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":1024,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "dense_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"status\":{\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_indexed\":1000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":1000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"published_node_count\":8837") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"runtime_present\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
}

test "index encoders report missing and stale topology groups without probing databases" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 4,
        .term_count = 10,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 5,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .freshness = .stale },
        .stats = .{
            .doc_count = 4,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
            .{ .table_id = 7, .group_id = 7, .start_key = "" },
            .{ .table_id = 7, .group_id = 8, .start_key = "m" },
        })[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"expected_groups\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"reported_groups\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"fresh_groups\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"stale_groups\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"missing_groups\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"unknown_remote_groups\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"8\":{") != null);
}

test "single embeddings index encoder exposes replay and enrichment runtime state" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .backfill_active = true,
        .backfill_progress = 0.2,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 1,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 5,
                .applied_sequence = 1,
                .processed_requests = 1,
                .error_count = 2,
                .retryable_error_count = 2,
                .retrying = true,
                .skip_by_hash_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.200") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"enrichment_runtime\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"pending_sequence_count\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retryable_error_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retrying\":true") != null);
}

test "single embeddings index encoder synthesizes replay state from enrichment runtime" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 1,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 5,
                .applied_sequence = 1,
                .processed_requests = 1,
                .error_count = 2,
                .retryable_error_count = 2,
                .retrying = true,
                .skip_by_hash_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.200") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"pending_sequence_count\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retryable_error_count\":2") != null);
}

test "single embeddings index encoder keeps published visibility separate from replay debt" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .node_count = 1,
        .replay_applied_sequence = 0,
        .replay_target_sequence = 3,
        .replay_catch_up_required = true,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
}

test "single embeddings index encoder keeps backfill active while enrichment replay lags" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .node_count = 1,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 3,
        .replay_catch_up_required = false,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 5,
                .applied_sequence = 3,
                .retrying = true,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.600") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retrying\":true") != null);
}

test "single embeddings index encoder keeps retrying coverage gaps catch-up coherent" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .replay_applied_sequence = 34,
        .replay_target_sequence = 34,
        .replay_catch_up_required = false,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 34,
                .applied_sequence = 34,
                .retrying = true,
                .retryable_error_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.333") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":33") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":34") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
}

test "external embeddings index readiness does not require table doc coverage" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 50_000,
        .node_count = 24,
        .replay_applied_sequence = 502,
        .replay_target_sequence = 502,
        .replay_catch_up_required = false,
        .backfill_active = true,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 50_001,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":1536,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "vec", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_indexed\":50000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":50000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"published_doc_count\":50000") != null);
}

test "embeddings index status reports dense catch-up phase separately from published visibility" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .node_count = 128,
        .root_node = 9,
        .replay_applied_sequence = 700,
        .replay_target_sequence = 701,
        .replay_catch_up_required = true,
        .catch_up_active = true,
        .catch_up_phase = .idle,
        .catch_up_applied_sequence = 700,
        .catch_up_target_sequence = 701,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = indexes,
            .async_indexing = .{
                .dense_catch_up = .{
                    .active = true,
                    .phase = .replay,
                    .current_sequence = 700,
                    .current_target_sequence = 701,
                },
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":384,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "dense_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"doc_count\":25000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":25000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"published_doc_count\":25000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_replay_applied_sequence\":700") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_replay_target_sequence\":701") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_publish_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_phase\":\"replay\"") != null);
}

test "embeddings index status keeps replay pending when catch-up progress lags replay target" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 217_500,
        .node_count = 3_300,
        .root_node = 1,
        .replay_applied_sequence = 325,
        .replay_target_sequence = 325,
        .replay_catch_up_required = false,
        .catch_up_active = false,
        .catch_up_applied_sequence = 77,
        .catch_up_target_sequence = 325,
        .backfill_active = false,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 217_500,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":512,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "dense_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_publish_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":77") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":325") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
}

test "managed embeddings readiness prefers replay completion once docs are indexed" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
        .replay_catch_up_required = false,
        .backfill_active = true,
        .backfill_progress = 0.5,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 2,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .processed_requests = 2,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
}

test "managed embeddings readiness does not require table doc count once replay is complete" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
        .replay_catch_up_required = false,
        .backfill_active = true,
        .backfill_progress = 0.5,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
}

test "embeddings index replay completion without artifact visibility is not ready" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 0,
        .root_node = 0,
        .replay_applied_sequence = 4000,
        .replay_target_sequence = 4000,
        .replay_catch_up_required = false,
        .backfill_active = false,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .background_refresh,
            .freshness = .fresh,
        },
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":512,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "vec", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_publish_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":4000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":4000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
}

test "single embeddings index encoder keeps partial backfill active while indexed docs lag table docs" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 1,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 1,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
}
