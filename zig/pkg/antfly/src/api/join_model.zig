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
const db_mod = @import("../storage/db/mod.zig");
const json_helpers = @import("json_helpers.zig");

pub const Allocator = std.mem.Allocator;
pub const join_broadcast_threshold_bytes: u64 = 10 * 1024 * 1024;
pub const join_lookup_selectivity_threshold: f64 = 0.1;
pub const join_estimated_row_bytes: u64 = 200;
pub const join_lookup_batch_size: u64 = 1000;
pub const internal_doc_identity_key_field = "__antfly_doc_identity_key";

pub const JoinResponseMetadata = struct {
    total_hits: usize,
    max_score: f64,
};

pub const JoinProfilePayload = struct {
    strategy_used: []const u8,
    left_rows_scanned: i64,
    right_rows_scanned: i64,
    rows_matched: i64,
    rows_unmatched_left: i64,
    rows_unmatched_right: i64,
    estimated_cost: f64,
    estimated_rows: u64,
    estimated_memory_bytes: u64,
    planner_used_stats: bool,
    distributed_execution: bool,
    groups_queried: usize,
    shuffle_partitions: usize,
    shuffle_candidate: bool,
    forced_broadcast_fallback: bool,
    duration_ms: i64 = 0,
};

pub const JoinWorkerAttemptPayload = struct {
    partition_index: usize,
    worker_group_id: u64,
    succeeded: bool,
};

pub const JoinFinalizerAttemptPayload = struct {
    worker_group_id: u64,
    succeeded: bool,
};

pub const JoinWorkerExecutionPayload = struct {
    execution_mode: []const u8,
    job_id: ?u64 = null,
    job_phase: ?[]const u8 = null,
    total_partitions: usize = 0,
    completed_partitions: usize = 0,
    expires_at_millis: u64 = 0,
    worker_retries: usize = 0,
    finalizer_retries: usize = 0,
    finalizer_group_id: ?u64 = null,
    coordinator_finalized: bool = false,
    imported_owner_group_id: ?u64 = null,
    imported_partial_state: bool = false,
    imported_cached_result: bool = false,
    worker_attempts: []const JoinWorkerAttemptPayload = &.{},
    finalizer_attempts: []const JoinFinalizerAttemptPayload = &.{},
};

pub fn JoinOwnedShell(comptime Stats: type) type {
    return struct {
        hits: []std.json.Value,
        stats: Stats,
        matched_right_ids: [][]u8 = &.{},

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            for (self.hits) |*hit| json_helpers.deinitJsonValue(alloc, hit);
            if (self.hits.len > 0) alloc.free(self.hits);
            for (self.matched_right_ids) |id| alloc.free(id);
            if (self.matched_right_ids.len > 0) alloc.free(self.matched_right_ids);
            self.* = undefined;
        }
    };
}

/// Takes ownership of `hits`. The caller must eventually deinit the returned shell.
pub fn adoptOwnedJoinShellAlloc(
    comptime Stats: type,
    alloc: Allocator,
    hits: []std.json.Value,
    stats: Stats,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
) !JoinOwnedShell(Stats) {
    errdefer {
        for (hits) |*hit| json_helpers.deinitJsonValue(alloc, hit);
        if (hits.len > 0) alloc.free(hits);
    }
    const owned_matched_right_ids = try ownedMatchedRightIdsAlloc(alloc, matched_right_ids);
    errdefer {
        for (owned_matched_right_ids) |id| alloc.free(id);
        if (owned_matched_right_ids.len > 0) alloc.free(owned_matched_right_ids);
    }
    return .{
        .hits = hits,
        .stats = stats,
        .matched_right_ids = owned_matched_right_ids,
    };
}

/// Clones `hits` into an owned shell. The caller must eventually deinit the returned shell.
pub fn cloneJoinShellAlloc(
    comptime Stats: type,
    alloc: Allocator,
    hits: []const std.json.Value,
    stats: Stats,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
) !JoinOwnedShell(Stats) {
    const owned_hits = try cloneJsonHitSliceAlloc(alloc, hits);
    errdefer {
        for (owned_hits) |*hit| json_helpers.deinitJsonValue(alloc, hit);
        if (owned_hits.len > 0) alloc.free(owned_hits);
    }
    return try adoptOwnedJoinShellAlloc(Stats, alloc, owned_hits, stats, matched_right_ids);
}

pub fn computeJsonHitMaxScore(hits: []const std.json.Value) f64 {
    var max_score: f64 = 0;
    for (hits) |hit| {
        if (hit != .object) continue;
        const score = hit.object.get("_score") orelse continue;
        const numeric: f64 = switch (score) {
            .integer => |value| @floatFromInt(value),
            .float => |value| value,
            else => continue,
        };
        if (numeric > max_score) max_score = numeric;
    }
    return max_score;
}

pub fn makeJoinResponseMetadata(hits: []const std.json.Value) JoinResponseMetadata {
    return .{
        .total_hits = hits.len,
        .max_score = computeJsonHitMaxScore(hits),
    };
}

pub fn applyJoinResponseMetadata(
    alloc: Allocator,
    hits_obj: *std.json.ObjectMap,
    metadata: JoinResponseMetadata,
) !void {
    try setObjectFieldOwned(alloc, hits_obj, "total", .{ .integer = @intCast(metadata.total_hits) });
    try setObjectFieldOwned(alloc, hits_obj, "max_score", .{ .float = metadata.max_score });
}

pub fn applyJoinProfilePayload(
    alloc: Allocator,
    join_obj: *std.json.ObjectMap,
    payload: JoinProfilePayload,
) !void {
    try setObjectFieldOwned(alloc, join_obj, "strategy_used", .{ .string = try alloc.dupe(u8, payload.strategy_used) });
    try setObjectFieldOwned(alloc, join_obj, "left_rows_scanned", .{ .integer = payload.left_rows_scanned });
    try setObjectFieldOwned(alloc, join_obj, "right_rows_scanned", .{ .integer = payload.right_rows_scanned });
    try setObjectFieldOwned(alloc, join_obj, "rows_matched", .{ .integer = payload.rows_matched });
    try setObjectFieldOwned(alloc, join_obj, "rows_unmatched_left", .{ .integer = payload.rows_unmatched_left });
    try setObjectFieldOwned(alloc, join_obj, "rows_unmatched_right", .{ .integer = payload.rows_unmatched_right });
    try setObjectFieldOwned(alloc, join_obj, "estimated_cost", .{ .float = payload.estimated_cost });
    try setObjectFieldOwned(alloc, join_obj, "estimated_rows", .{ .integer = @intCast(payload.estimated_rows) });
    try setObjectFieldOwned(alloc, join_obj, "estimated_memory_bytes", .{ .integer = @intCast(payload.estimated_memory_bytes) });
    try setObjectFieldOwned(alloc, join_obj, "planner_used_stats", .{ .bool = payload.planner_used_stats });
    try setObjectFieldOwned(alloc, join_obj, "distributed_execution", .{ .bool = payload.distributed_execution });
    try setObjectFieldOwned(alloc, join_obj, "groups_queried", .{ .integer = @intCast(payload.groups_queried) });
    try setObjectFieldOwned(alloc, join_obj, "shuffle_partitions", .{ .integer = @intCast(payload.shuffle_partitions) });
    try setObjectFieldOwned(alloc, join_obj, "shuffle_candidate", .{ .bool = payload.shuffle_candidate });
    try setObjectFieldOwned(alloc, join_obj, "forced_broadcast_fallback", .{ .bool = payload.forced_broadcast_fallback });
    try setObjectFieldOwned(alloc, join_obj, "duration_ms", .{ .integer = payload.duration_ms });
}

pub fn applyJoinWorkerExecutionPayload(
    alloc: Allocator,
    join_obj: *std.json.ObjectMap,
    payload: JoinWorkerExecutionPayload,
) !void {
    try setObjectFieldOwned(alloc, join_obj, "execution_mode", .{ .string = try alloc.dupe(u8, payload.execution_mode) });
    if (payload.job_id) |job_id| try setObjectFieldOwned(alloc, join_obj, "job_id", try jsonU64ValueAlloc(alloc, job_id));
    if (payload.job_phase) |job_phase| try setObjectFieldOwned(alloc, join_obj, "job_phase", .{ .string = try alloc.dupe(u8, job_phase) });
    try setObjectFieldOwned(alloc, join_obj, "total_partitions", .{ .integer = @intCast(payload.total_partitions) });
    try setObjectFieldOwned(alloc, join_obj, "completed_partitions", .{ .integer = @intCast(payload.completed_partitions) });
    try setObjectFieldOwned(alloc, join_obj, "expires_at_millis", .{ .integer = @intCast(payload.expires_at_millis) });
    try setObjectFieldOwned(alloc, join_obj, "worker_retries", .{ .integer = @intCast(payload.worker_retries) });
    try setObjectFieldOwned(alloc, join_obj, "finalizer_retries", .{ .integer = @intCast(payload.finalizer_retries) });
    if (payload.finalizer_group_id) |group_id| try setObjectFieldOwned(alloc, join_obj, "finalizer_group_id", try jsonU64ValueAlloc(alloc, group_id));
    try setObjectFieldOwned(alloc, join_obj, "coordinator_finalized", .{ .bool = payload.coordinator_finalized });
    if (payload.imported_owner_group_id) |group_id| try setObjectFieldOwned(alloc, join_obj, "imported_owner_group_id", try jsonU64ValueAlloc(alloc, group_id));
    try setObjectFieldOwned(alloc, join_obj, "imported_partial_state", .{ .bool = payload.imported_partial_state });
    try setObjectFieldOwned(alloc, join_obj, "imported_cached_result", .{ .bool = payload.imported_cached_result });
    try setObjectFieldOwned(alloc, join_obj, "worker_attempts", .{ .array = try joinWorkerAttemptsValue(alloc, payload.worker_attempts) });
    try setObjectFieldOwned(alloc, join_obj, "finalizer_attempts", .{ .array = try joinFinalizerAttemptsValue(alloc, payload.finalizer_attempts) });
}

pub fn firstResponseObjectPtr(root: *std.json.Value) !*std.json.Value {
    if (root.* != .object) return error.InvalidQueryRequest;
    const responses = root.object.getPtr("responses") orelse return error.InvalidQueryRequest;
    if (responses.* != .array or responses.array.items.len == 0) return error.InvalidQueryRequest;
    const response = &responses.array.items[0];
    if (response.* != .object) return error.InvalidQueryRequest;
    return response;
}

pub fn queryHitsObjectPtr(root: *std.json.Value) !*std.json.ObjectMap {
    const response = try firstResponseObjectPtr(root);
    const hits = response.object.getPtr("hits") orelse return error.InvalidQueryRequest;
    if (hits.* != .object) return error.InvalidQueryRequest;
    return &hits.object;
}

pub fn queryHitsArrayPtr(root: *std.json.Value) !*std.json.Array {
    const hits_obj = try queryHitsObjectPtr(root);
    const hit_items = hits_obj.getPtr("hits") orelse return error.InvalidQueryRequest;
    if (hit_items.* != .array) return error.InvalidQueryRequest;
    return &hit_items.array;
}

pub fn replaceQueryResponseHitsAlloc(
    alloc: Allocator,
    root: *std.json.Value,
    hits: []const std.json.Value,
) !void {
    const hits_ptr = try queryHitsArrayPtr(root);
    for (hits_ptr.items) |*item| json_helpers.deinitJsonValue(alloc, item);
    hits_ptr.deinit();
    hits_ptr.* = std.json.Array.init(alloc);
    for (hits) |item| {
        var cloned = try json_helpers.cloneJsonValue(alloc, item);
        errdefer json_helpers.deinitJsonValue(alloc, &cloned);
        stripInternalDocIdentityKey(alloc, &cloned);
        try hits_ptr.append(cloned);
    }
    try applyJoinResponseMetadata(alloc, try queryHitsObjectPtr(root), makeJoinResponseMetadata(hits_ptr.items));
}

pub fn applyJoinShellToResponse(
    alloc: Allocator,
    root: *std.json.Value,
    shell: anytype,
) !void {
    try replaceQueryResponseHitsAlloc(alloc, root, shell.hits);
}

pub fn responseProfileObjectPtr(root: *std.json.Value) ?*std.json.Value {
    const response = firstResponseObjectPtr(root) catch return null;
    const profile_ptr = response.object.getPtr("profile") orelse return null;
    if (profile_ptr.* == .null or profile_ptr.* != .object) return null;
    return profile_ptr;
}

pub fn applyJoinProfileToResponse(
    alloc: Allocator,
    root: *std.json.Value,
    payload: JoinProfilePayload,
) !void {
    const profile_ptr = responseProfileObjectPtr(root) orelse return;
    var join_obj = std.json.ObjectMap.empty;
    try applyJoinProfilePayload(alloc, &join_obj, payload);
    if (profile_ptr.object.getPtr("join")) |join_ptr| {
        json_helpers.deinitJsonValue(alloc, join_ptr);
        join_ptr.* = .{ .object = join_obj };
    } else {
        try setObjectFieldOwned(alloc, &profile_ptr.object, "join", .{ .object = join_obj });
    }
}

fn mergeRightSourceValueIntoSourceAlloc(
    alloc: Allocator,
    source_value: *std.json.Value,
    join: anytype,
    right_source: std.json.Value,
) !void {
    if (right_source != .object or source_value.* != .object) return;
    var it = right_source.object.iterator();
    while (it.next()) |entry| {
        if (join.right_fields.len > 0 and !containsString(join.right_fields, entry.key_ptr.*)) continue;
        const prefixed_key = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ join.right_table, entry.key_ptr.* });
        if (source_value.object.getPtr(prefixed_key)) |value_ptr| {
            alloc.free(prefixed_key);
            json_helpers.deinitJsonValue(alloc, value_ptr);
            value_ptr.* = try json_helpers.cloneJsonValue(alloc, entry.value_ptr.*);
        } else {
            try source_value.object.put(alloc, prefixed_key, try json_helpers.cloneJsonValue(alloc, entry.value_ptr.*));
        }
    }
}

fn mergeOwnedRightSourceValueIntoSourceAlloc(
    alloc: Allocator,
    source_value: *std.json.Value,
    join: anytype,
    right_source: *std.json.Value,
) !void {
    if (right_source.* != .object or source_value.* != .object) return;
    var it = right_source.object.iterator();
    while (it.next()) |entry| {
        if (join.right_fields.len > 0 and !containsString(join.right_fields, entry.key_ptr.*)) continue;
        const prefixed_key = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ join.right_table, entry.key_ptr.* });
        if (source_value.object.getPtr(prefixed_key)) |value_ptr| {
            alloc.free(prefixed_key);
            json_helpers.deinitJsonValue(alloc, value_ptr);
            value_ptr.* = try json_helpers.cloneJsonValue(alloc, entry.value_ptr.*);
        } else {
            try source_value.object.put(alloc, prefixed_key, try json_helpers.cloneJsonValue(alloc, entry.value_ptr.*));
        }
    }
}

pub fn mergeRightHitIntoSourceAlloc(
    alloc: Allocator,
    source_value: *std.json.Value,
    join: anytype,
    right_hit: std.json.Value,
) !void {
    const right_source = right_hit.object.get("_source") orelse return;
    try mergeRightSourceValueIntoSourceAlloc(alloc, source_value, join, right_source);
}

pub fn removeFieldFromSourceObject(
    alloc: Allocator,
    source_value: *std.json.Value,
    field_name: []const u8,
) void {
    if (std.mem.indexOfScalar(u8, field_name, '.') != null) return;
    if (source_value.* != .object) return;
    if (source_value.object.fetchOrderedRemove(field_name)) |kv| {
        alloc.free(@constCast(kv.key));
        var val = kv.value;
        json_helpers.deinitJsonValue(alloc, &val);
    }
}

pub fn buildUnmatchedRightJoinHitAlloc(
    alloc: Allocator,
    right_hit: std.json.Value,
    join: anytype,
    left_fields: anytype,
    appended_left_field: bool,
) !std.json.Value {
    if (right_hit != .object) return error.InvalidQueryRequest;
    var source_value = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &source_value);

    for (left_fields) |field_value| {
        const field_name = try requestedFieldName(field_value);
        if (appended_left_field and std.mem.eql(u8, field_name, join.left_field)) continue;
        try source_value.object.put(alloc, try alloc.dupe(u8, field_name), .null);
    }
    try mergeRightHitIntoSourceAlloc(alloc, &source_value, join, right_hit);

    var hit_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = hit_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            json_helpers.deinitJsonValue(alloc, entry.value_ptr);
        }
        hit_obj.deinit(alloc);
    }
    if (right_hit.object.get("_id")) |id_value| {
        try setObjectFieldOwned(alloc, &hit_obj, "_id", try json_helpers.cloneJsonValue(alloc, id_value));
    }
    if (right_hit.object.get(internal_doc_identity_key_field)) |identity_key| {
        try setObjectFieldOwned(alloc, &hit_obj, internal_doc_identity_key_field, try json_helpers.cloneJsonValue(alloc, identity_key));
    }
    if (right_hit.object.get("_score")) |score_value| {
        try setObjectFieldOwned(alloc, &hit_obj, "_score", try json_helpers.cloneJsonValue(alloc, score_value));
    } else {
        try setObjectFieldOwned(alloc, &hit_obj, "_score", .{ .float = 0 });
    }
    try setObjectFieldOwned(alloc, &hit_obj, "_source", source_value);
    source_value = undefined;
    return .{ .object = hit_obj };
}

pub fn appendUnmatchedRightJoinHitsAlloc(
    alloc: Allocator,
    out: anytype,
    right_hits: []const std.json.Value,
    join: anytype,
    left_fields: anytype,
    appended_left_field: bool,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
) !usize {
    var appended: usize = 0;
    for (right_hits) |right_hit| {
        const right_id = rightHitIdentityKey(right_hit) orelse continue;
        if (matched_right_ids.contains(right_id)) continue;
        try appendJsonHit(out, alloc, try buildUnmatchedRightJoinHitAlloc(alloc, right_hit, join, left_fields, appended_left_field));
        appended += 1;
    }
    return appended;
}

pub fn buildUnmatchedRightJoinHitFromSearchHitAlloc(
    alloc: Allocator,
    right_hit: db_mod.types.SearchHit,
    join: anytype,
    left_fields: anytype,
    appended_left_field: bool,
) !std.json.Value {
    const stored_data = right_hit.stored_data orelse return error.InvalidQueryRequest;
    var parsed_source = try json_helpers.parseJsonValueAlloc(alloc, stored_data);
    defer parsed_source.deinit();

    var source_value = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &source_value);

    for (left_fields) |field_value| {
        const field_name = try requestedFieldName(field_value);
        if (appended_left_field and std.mem.eql(u8, field_name, join.left_field)) continue;
        try source_value.object.put(alloc, try alloc.dupe(u8, field_name), .null);
    }
    try mergeOwnedRightSourceValueIntoSourceAlloc(alloc, &source_value, join, &parsed_source.value);

    var hit_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = hit_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            json_helpers.deinitJsonValue(alloc, entry.value_ptr);
        }
        hit_obj.deinit(alloc);
    }
    try setObjectFieldOwned(alloc, &hit_obj, "_id", .{ .string = try alloc.dupe(u8, right_hit.id) });
    if (right_hit.doc_ordinal) |ordinal| {
        try setObjectFieldOwned(alloc, &hit_obj, internal_doc_identity_key_field, .{ .string = try std.fmt.allocPrint(alloc, "o:{d}", .{ordinal}) });
    }
    try setObjectFieldOwned(alloc, &hit_obj, "_score", if (right_hit.score) |score| .{ .float = score } else .{ .float = 0 });
    try setObjectFieldOwned(alloc, &hit_obj, "_source", source_value);
    source_value = undefined;
    return .{ .object = hit_obj };
}

pub fn appendUnmatchedRightJoinSearchHitsAlloc(
    alloc: Allocator,
    out: anytype,
    right_hits: []const db_mod.types.SearchHit,
    join: anytype,
    left_fields: anytype,
    appended_left_field: bool,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
) !usize {
    var appended: usize = 0;
    for (right_hits) |right_hit| {
        if (right_hit.doc_ordinal) |ordinal| {
            var key_buf: [32]u8 = undefined;
            const identity_key = try std.fmt.bufPrint(&key_buf, "o:{d}", .{ordinal});
            if (matched_right_ids.contains(identity_key)) continue;
        }
        if (matched_right_ids.contains(right_hit.id)) continue;
        try appendJsonHit(out, alloc, try buildUnmatchedRightJoinHitFromSearchHitAlloc(alloc, right_hit, join, left_fields, appended_left_field));
        appended += 1;
    }
    return appended;
}

pub fn findMatchingRightHit(
    right_hits: []const std.json.Value,
    right_field: []const u8,
    left_value: std.json.Value,
    match_ctx: anytype,
    extract_join_value_from_hit: anytype,
    values_match: anytype,
) ?std.json.Value {
    for (right_hits) |hit_value| {
        const right_value = extract_join_value_from_hit(hit_value, right_field) orelse continue;
        if (values_match(match_ctx, left_value, right_value)) return hit_value;
    }
    return null;
}

pub fn joinSupportsIndexLookup(join: anytype) bool {
    return join.join_type != .right and
        join.right_filters == null and
        join.nested_join == null and
        std.mem.eql(u8, join.right_field, "_id");
}

pub fn chooseJoinExecutionStrategyWithoutStats(
    comptime Strategy: type,
    join: anytype,
    supported_index_lookup: bool,
) Strategy {
    if (join.strategy_hint) |hint| {
        if (std.mem.eql(u8, hint, "broadcast")) return .broadcast;
        if (std.mem.eql(u8, hint, "index_lookup") and supported_index_lookup) {
            return .index_lookup;
        }
        if (std.mem.eql(u8, hint, "shuffle")) return .broadcast;
        return .broadcast;
    }

    if (supported_index_lookup) return .index_lookup;
    return .broadcast;
}

pub fn resolveJoinStrategyHint(
    comptime Strategy: type,
    join: anytype,
    supported_index_lookup: bool,
) ?struct {
    strategy: Strategy,
    shuffle_requested: bool,
    forced_broadcast_fallback: bool,
} {
    const hint = join.strategy_hint orelse return null;

    if (std.mem.eql(u8, hint, "index_lookup") and supported_index_lookup) {
        return .{
            .strategy = .index_lookup,
            .shuffle_requested = false,
            .forced_broadcast_fallback = false,
        };
    }
    if (std.mem.eql(u8, hint, "shuffle")) {
        return .{
            .strategy = .broadcast,
            .shuffle_requested = true,
            .forced_broadcast_fallback = true,
        };
    }
    return .{
        .strategy = .broadcast,
        .shuffle_requested = false,
        .forced_broadcast_fallback = std.mem.eql(u8, hint, "index_lookup") and !supported_index_lookup,
    };
}

pub fn countDistinctJoinKeys(
    alloc: Allocator,
    hits: []const std.json.Value,
    field_name: []const u8,
    extract_join_value_from_hit: anytype,
) !u64 {
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);
    var numeric_count: u64 = 0;
    for (hits) |hit| {
        const value = extract_join_value_from_hit(hit, field_name) orelse continue;
        switch (value) {
            .string => |text| {
                if (seen.contains(text)) continue;
                try seen.put(alloc, text, {});
            },
            .integer, .float, .bool => numeric_count += 1,
            else => {},
        }
    }
    return @as(u64, seen.count()) + numeric_count;
}

pub fn chooseBroadcastOrIndexLookupWithStats(
    comptime Strategy: type,
    supported_index_lookup: bool,
    distinct_left_keys: u64,
    right_row_count: u64,
    right_size_bytes: u64,
) Strategy {
    if (right_size_bytes > 0 and right_size_bytes < join_broadcast_threshold_bytes) {
        return .broadcast;
    }
    if (supported_index_lookup and right_row_count > 0) {
        const selectivity = @as(f64, @floatFromInt(distinct_left_keys)) / @as(f64, @floatFromInt(right_row_count));
        return if (selectivity < join_lookup_selectivity_threshold) .index_lookup else .broadcast;
    }
    return .broadcast;
}

pub fn joinSideBelowBroadcastThreshold(size_bytes: u64) bool {
    return size_bytes > 0 and size_bytes < join_broadcast_threshold_bytes;
}

pub fn joinSidesNeedShuffleCandidate(left_size_bytes: u64, right_size_bytes: u64) bool {
    return left_size_bytes > join_broadcast_threshold_bytes and right_size_bytes > join_broadcast_threshold_bytes;
}

pub fn applyBroadcastJoinPlanCost(
    estimated_cost: *f64,
    estimated_memory_bytes: *u64,
    left_rows: u64,
    right_rows: u64,
    right_size_bytes: u64,
) void {
    estimated_cost.* = @as(f64, @floatFromInt(right_rows)) + @as(f64, @floatFromInt(left_rows)) * 0.001;
    estimated_memory_bytes.* = if (right_size_bytes > 0) right_size_bytes else right_rows * join_estimated_row_bytes;
}

pub fn applyIndexLookupJoinPlanCostSimple(
    estimated_cost: *f64,
    estimated_memory_bytes: *u64,
    left_rows: u64,
    distinct_left_keys: u64,
) void {
    const lookup_count = if (distinct_left_keys == 0) left_rows else distinct_left_keys;
    estimated_cost.* = @as(f64, @floatFromInt(lookup_count)) * 0.25;
    estimated_memory_bytes.* = @max(@as(u64, 1), lookup_count) * 128;
}

pub fn applyIndexLookupJoinPlanCostBatched(
    estimated_cost: *f64,
    estimated_memory_bytes: *u64,
    left_rows: u64,
) void {
    const batches = if (left_rows == 0) 0 else (left_rows + join_lookup_batch_size - 1) / join_lookup_batch_size;
    estimated_cost.* = @as(f64, @floatFromInt(batches)) * 10 + @as(f64, @floatFromInt(left_rows)) * 0.01;
    estimated_memory_bytes.* = join_lookup_batch_size * join_estimated_row_bytes;
}

fn setObjectFieldOwned(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    if (obj.getPtr(key)) |value_ptr| {
        json_helpers.deinitJsonValue(alloc, value_ptr);
        value_ptr.* = value;
        return;
    }
    try obj.put(alloc, try alloc.dupe(u8, key), value);
}

fn jsonU64ValueAlloc(
    alloc: Allocator,
    value: u64,
) !std.json.Value {
    if (std.math.cast(i64, value)) |signed| {
        return .{ .integer = signed };
    }
    return .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{value}) };
}

fn containsString(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn requestedFieldName(field_value: anytype) ![]const u8 {
    const T = @TypeOf(field_value);
    if (T == std.json.Value) {
        if (field_value != .string) return error.InvalidQueryRequest;
        return field_value.string;
    }
    if (T == []const u8) return field_value;
    @compileError("unsupported requested field type");
}

pub fn rightHitId(right_hit: std.json.Value) ?[]const u8 {
    return switch (right_hit) {
        .object => |obj| switch (obj.get("_id") orelse return null) {
            .string => |text| text,
            else => null,
        },
        else => null,
    };
}

pub fn rightHitIdentityKey(right_hit: std.json.Value) ?[]const u8 {
    return switch (right_hit) {
        .object => |obj| blk: {
            if (obj.get(internal_doc_identity_key_field)) |identity_key| {
                if (identity_key == .string) break :blk identity_key.string;
            }
            break :blk rightHitId(right_hit);
        },
        else => null,
    };
}

pub fn stripInternalDocIdentityKey(alloc: Allocator, hit: *std.json.Value) void {
    if (hit.* != .object) return;
    if (hit.object.fetchOrderedRemove(internal_doc_identity_key_field)) |kv| {
        alloc.free(@constCast(kv.key));
        var value = kv.value;
        json_helpers.deinitJsonValue(alloc, &value);
    }
}

fn appendJsonHit(out: anytype, alloc: Allocator, hit: std.json.Value) !void {
    const T = @TypeOf(out.*);
    if (T == std.json.Array) {
        try out.append(hit);
        return;
    }
    if (T == std.ArrayListUnmanaged(std.json.Value)) {
        try out.append(alloc, hit);
        return;
    }
    @compileError("unsupported JSON hit sink");
}

fn ownedMatchedRightIdsAlloc(
    alloc: Allocator,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
) ![][]u8 {
    const out = try alloc.alloc([]u8, matched_right_ids.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |id| alloc.free(id);
        alloc.free(out);
    }
    var it = matched_right_ids.iterator();
    var idx: usize = 0;
    while (it.next()) |entry| : (idx += 1) {
        out[idx] = try alloc.dupe(u8, entry.key_ptr.*);
        initialized += 1;
    }
    return out;
}

fn cloneJsonHitSliceAlloc(
    alloc: Allocator,
    hits: []const std.json.Value,
) ![]std.json.Value {
    const out = try alloc.alloc(std.json.Value, hits.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| json_helpers.deinitJsonValue(alloc, item);
        alloc.free(out);
    }
    for (hits, 0..) |item, i| {
        out[i] = try json_helpers.cloneJsonValue(alloc, item);
        initialized += 1;
    }
    return out;
}

fn joinWorkerAttemptsValue(
    alloc: Allocator,
    attempts: []const JoinWorkerAttemptPayload,
) !std.json.Array {
    var attempted_workers = std.json.Array.init(alloc);
    errdefer {
        for (attempted_workers.items) |*item| json_helpers.deinitJsonValue(alloc, item);
        attempted_workers.deinit();
    }
    for (attempts) |attempt| {
        var obj = std.json.ObjectMap.empty;
        try setObjectFieldOwned(alloc, &obj, "partition_index", .{ .integer = @intCast(attempt.partition_index) });
        try setObjectFieldOwned(alloc, &obj, "worker_group_id", try jsonU64ValueAlloc(alloc, attempt.worker_group_id));
        try setObjectFieldOwned(alloc, &obj, "succeeded", .{ .bool = attempt.succeeded });
        try attempted_workers.append(.{ .object = obj });
    }
    return attempted_workers;
}

fn joinFinalizerAttemptsValue(
    alloc: Allocator,
    attempts: []const JoinFinalizerAttemptPayload,
) !std.json.Array {
    var finalizer_attempts = std.json.Array.init(alloc);
    errdefer {
        for (finalizer_attempts.items) |*item| json_helpers.deinitJsonValue(alloc, item);
        finalizer_attempts.deinit();
    }
    for (attempts) |attempt| {
        var obj = std.json.ObjectMap.empty;
        try setObjectFieldOwned(alloc, &obj, "worker_group_id", try jsonU64ValueAlloc(alloc, attempt.worker_group_id));
        try setObjectFieldOwned(alloc, &obj, "succeeded", .{ .bool = attempt.succeeded });
        try finalizer_attempts.append(.{ .object = obj });
    }
    return finalizer_attempts;
}

test "join model applies shell to response and refreshes hit metadata" {
    const alloc = std.testing.allocator;

    var root = try testQueryResponseRootAlloc(alloc, "old", 1.0, "old");
    defer json_helpers.deinitJsonValue(alloc, &root);

    const hits = try alloc.alloc(std.json.Value, 2);
    errdefer {
        for (hits) |*hit| json_helpers.deinitJsonValue(alloc, hit);
        alloc.free(hits);
    }
    hits[0] = try testJoinHitAlloc(alloc, "doc:1", 1.5, "1");
    hits[1] = try testJoinHitAlloc(alloc, "doc:2", 2.25, "2");
    try setObjectFieldOwned(alloc, &hits[0].object, internal_doc_identity_key_field, .{ .string = try alloc.dupe(u8, "o:17") });

    var matched_right_ids = std.StringHashMapUnmanaged(void){};
    defer matched_right_ids.deinit(alloc);
    try matched_right_ids.put(alloc, "right:1", {});

    var shell = try adoptOwnedJoinShellAlloc(struct {}, alloc, hits, .{}, &matched_right_ids);
    defer shell.deinit(alloc);

    try applyJoinShellToResponse(alloc, &root, shell);

    const hits_obj = try queryHitsObjectPtr(&root);
    const hits_ptr = try queryHitsArrayPtr(&root);

    try std.testing.expectEqual(@as(usize, 2), hits_ptr.items.len);
    try std.testing.expectEqual(@as(i64, 2), hits_obj.get("total").?.integer);
    try std.testing.expectApproxEqAbs(@as(f64, 2.25), hits_obj.get("max_score").?.float, 0.000001);
    try std.testing.expectEqualStrings("doc:1", hits_ptr.items[0].object.get("_id").?.string);
    try std.testing.expect(hits_ptr.items[0].object.get(internal_doc_identity_key_field) == null);
    try std.testing.expectEqualStrings("2", hits_ptr.items[1].object.get("_source").?.object.get("id").?.string);
}

test "join model resolves strategy hints with fallback bookkeeping" {
    const Strategy = enum { broadcast, index_lookup, shuffle };
    const Join = struct {
        strategy_hint: ?[]const u8 = null,
    };

    const shuffle_hint = resolveJoinStrategyHint(Strategy, Join{ .strategy_hint = "shuffle" }, false).?;
    try std.testing.expectEqual(Strategy.broadcast, shuffle_hint.strategy);
    try std.testing.expect(shuffle_hint.shuffle_requested);
    try std.testing.expect(shuffle_hint.forced_broadcast_fallback);

    const unsupported_lookup = resolveJoinStrategyHint(Strategy, Join{ .strategy_hint = "index_lookup" }, false).?;
    try std.testing.expectEqual(Strategy.broadcast, unsupported_lookup.strategy);
    try std.testing.expect(!unsupported_lookup.shuffle_requested);
    try std.testing.expect(unsupported_lookup.forced_broadcast_fallback);

    const supported_lookup = resolveJoinStrategyHint(Strategy, Join{ .strategy_hint = "index_lookup" }, true).?;
    try std.testing.expectEqual(Strategy.index_lookup, supported_lookup.strategy);
    try std.testing.expect(!supported_lookup.shuffle_requested);
    try std.testing.expect(!supported_lookup.forced_broadcast_fallback);
}

test "join model attaches join profile to response profile root" {
    const alloc = std.testing.allocator;

    var root = try testQueryResponseRootAlloc(alloc, "old", 1.0, "old");
    defer json_helpers.deinitJsonValue(alloc, &root);

    try applyJoinProfileToResponse(alloc, &root, .{
        .strategy_used = "shuffle",
        .left_rows_scanned = 10,
        .right_rows_scanned = 20,
        .rows_matched = 3,
        .rows_unmatched_left = 1,
        .rows_unmatched_right = 2,
        .estimated_cost = 42.5,
        .estimated_rows = 123,
        .estimated_memory_bytes = 4096,
        .planner_used_stats = true,
        .distributed_execution = true,
        .groups_queried = 4,
        .shuffle_partitions = 8,
        .shuffle_candidate = true,
        .forced_broadcast_fallback = false,
        .duration_ms = 77,
    });

    const profile = responseProfileObjectPtr(&root).?;
    const join = profile.object.get("join").?.object;
    try std.testing.expectEqualStrings("shuffle", join.get("strategy_used").?.string);
    try std.testing.expectEqual(@as(i64, 10), join.get("left_rows_scanned").?.integer);
    try std.testing.expectEqual(@as(i64, 20), join.get("right_rows_scanned").?.integer);
    try std.testing.expectEqual(@as(i64, 3), join.get("rows_matched").?.integer);
    try std.testing.expect(join.get("planner_used_stats").?.bool);
    try std.testing.expect(join.get("distributed_execution").?.bool);
    try std.testing.expectEqual(@as(i64, 8), join.get("shuffle_partitions").?.integer);
    try std.testing.expect(join.get("shuffle_candidate").?.bool);
    try std.testing.expect(!join.get("forced_broadcast_fallback").?.bool);
}

test "join model builds unmatched right hit with projected right fields" {
    const alloc = std.testing.allocator;

    var right_source = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer if (right_source != .null) json_helpers.deinitJsonValue(alloc, &right_source);
    try setObjectFieldOwned(alloc, &right_source.object, "id", .{ .string = try alloc.dupe(u8, "r1") });
    try setObjectFieldOwned(alloc, &right_source.object, "name", .{ .string = try alloc.dupe(u8, "Ada") });
    try setObjectFieldOwned(alloc, &right_source.object, "ignored", .{ .string = try alloc.dupe(u8, "skip") });

    var right_hit = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer if (right_hit != .null) json_helpers.deinitJsonValue(alloc, &right_hit);
    try setObjectFieldOwned(alloc, &right_hit.object, "_id", .{ .string = try alloc.dupe(u8, "doc:right") });
    try setObjectFieldOwned(alloc, &right_hit.object, "_score", .{ .float = 1.75 });
    try setObjectFieldOwned(alloc, &right_hit.object, "_source", right_source);
    right_source = .null;

    const join = struct {
        right_table: []const u8,
        left_field: []const u8,
        right_fields: []const []const u8,
    }{
        .right_table = "authors",
        .left_field = "author_id",
        .right_fields = &.{ "id", "name" },
    };
    const left_fields = [_][]const u8{ "title", "author_id" };

    var unmatched = try buildUnmatchedRightJoinHitAlloc(alloc, right_hit, join, &left_fields, true);
    defer json_helpers.deinitJsonValue(alloc, &unmatched);

    try std.testing.expectEqualStrings("doc:right", unmatched.object.get("_id").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 1.75), unmatched.object.get("_score").?.float, 0.000001);

    const source = unmatched.object.get("_source").?.object;
    try std.testing.expect(source.get("title").? == .null);
    try std.testing.expect(source.get("author_id") == null);
    try std.testing.expectEqualStrings("r1", source.get("authors.id").?.string);
    try std.testing.expectEqualStrings("Ada", source.get("authors.name").?.string);
    try std.testing.expect(source.get("authors.ignored") == null);
}

test "join model counts distinct join keys across string and numeric values" {
    const alloc = std.testing.allocator;

    const hits = try alloc.alloc(std.json.Value, 5);
    defer {
        for (hits) |*hit| json_helpers.deinitJsonValue(alloc, hit);
        alloc.free(hits);
    }
    hits[0] = try testJoinHitWithFieldAlloc(alloc, "doc:1", "k", .{ .string = try alloc.dupe(u8, "a") });
    hits[1] = try testJoinHitWithFieldAlloc(alloc, "doc:2", "k", .{ .string = try alloc.dupe(u8, "a") });
    hits[2] = try testJoinHitWithFieldAlloc(alloc, "doc:3", "k", .{ .string = try alloc.dupe(u8, "b") });
    hits[3] = try testJoinHitWithFieldAlloc(alloc, "doc:4", "k", .{ .integer = 7 });
    hits[4] = try testJoinHitWithFieldAlloc(alloc, "doc:5", "k", .{ .integer = 7 });

    const distinct = try countDistinctJoinKeys(alloc, hits, "k", testExtractJoinValueFromHit);
    try std.testing.expectEqual(@as(u64, 4), distinct);
}

test "join model chooses broadcast or index lookup with stats" {
    const Strategy = enum { broadcast, index_lookup };

    try std.testing.expectEqual(
        Strategy.broadcast,
        chooseBroadcastOrIndexLookupWithStats(
            Strategy,
            true,
            10,
            1_000,
            join_broadcast_threshold_bytes - 1,
        ),
    );

    try std.testing.expectEqual(
        Strategy.index_lookup,
        chooseBroadcastOrIndexLookupWithStats(
            Strategy,
            true,
            10,
            1_000,
            join_broadcast_threshold_bytes * 2,
        ),
    );

    try std.testing.expectEqual(
        Strategy.broadcast,
        chooseBroadcastOrIndexLookupWithStats(
            Strategy,
            true,
            500,
            1_000,
            join_broadcast_threshold_bytes * 2,
        ),
    );
}

fn testJoinHitAlloc(
    alloc: Allocator,
    id: []const u8,
    score: f64,
    source_id: []const u8,
) !std.json.Value {
    var source = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &source);
    try setObjectFieldOwned(alloc, &source.object, "id", .{ .string = try alloc.dupe(u8, source_id) });

    var hit = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &hit);
    try setObjectFieldOwned(alloc, &hit.object, "_id", .{ .string = try alloc.dupe(u8, id) });
    try setObjectFieldOwned(alloc, &hit.object, "_score", .{ .float = score });
    try setObjectFieldOwned(alloc, &hit.object, "_source", source);
    source = undefined;
    return hit;
}

fn testJoinHitWithFieldAlloc(
    alloc: Allocator,
    id: []const u8,
    field_name: []const u8,
    field_value: std.json.Value,
) !std.json.Value {
    var source = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &source);
    try setObjectFieldOwned(alloc, &source.object, field_name, field_value);

    var hit = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &hit);
    try setObjectFieldOwned(alloc, &hit.object, "_id", .{ .string = try alloc.dupe(u8, id) });
    try setObjectFieldOwned(alloc, &hit.object, "_source", source);
    source = undefined;
    return hit;
}

fn testExtractJoinValueFromHit(hit: std.json.Value, field_name: []const u8) ?std.json.Value {
    if (hit != .object) return null;
    const source = hit.object.get("_source") orelse return null;
    if (source != .object) return null;
    return source.object.get(field_name);
}

fn testQueryResponseRootAlloc(
    alloc: Allocator,
    id: []const u8,
    score: f64,
    source_id: []const u8,
) !std.json.Value {
    var hits = std.json.Array.init(alloc);
    errdefer {
        for (hits.items) |*item| json_helpers.deinitJsonValue(alloc, item);
        hits.deinit();
    }
    try hits.append(try testJoinHitAlloc(alloc, id, score, source_id));

    var hits_obj = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &hits_obj);
    try setObjectFieldOwned(alloc, &hits_obj.object, "hits", .{ .array = hits });
    hits = undefined;

    var profile = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &profile);

    var response = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &response);
    try setObjectFieldOwned(alloc, &response.object, "hits", hits_obj);
    hits_obj = undefined;
    try setObjectFieldOwned(alloc, &response.object, "profile", profile);
    profile = undefined;

    var responses = std.json.Array.init(alloc);
    errdefer {
        for (responses.items) |*item| json_helpers.deinitJsonValue(alloc, item);
        responses.deinit();
    }
    try responses.append(response);
    response = undefined;

    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer json_helpers.deinitJsonValue(alloc, &root);
    try setObjectFieldOwned(alloc, &root.object, "responses", .{ .array = responses });
    responses = undefined;
    return root;
}
