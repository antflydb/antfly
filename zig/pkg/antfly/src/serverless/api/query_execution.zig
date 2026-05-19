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
const Allocator = std.mem.Allocator;
const metadata_openapi = @import("antfly_metadata_openapi");
const foreign_mod = @import("../../foreign/mod.zig");
const foreign_sources_api = @import("../../api/foreign_sources.zig");
const public_text_query = @import("../../api/public_text_query.zig");
const json_helpers = @import("../../api/json_helpers.zig");
const join_model = @import("../../api/join_model.zig");

pub const SupportedJoinRequest = struct {
    pub const JoinType = enum {
        inner,
        left,
        right,
    };

    right_table: []u8,
    join_type: JoinType = .inner,
    left_field: []u8,
    right_field: []u8,
    right_filters: ?SupportedJoinFilters = null,
    right_fields: [][]const u8 = &.{},
    strategy_hint: ?[]u8 = null,
    nested_join: ?*SupportedJoinRequest = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.right_table);
        alloc.free(self.left_field);
        alloc.free(self.right_field);
        if (self.right_filters) |*filters| filters.deinit(alloc);
        for (self.right_fields) |field| alloc.free(@constCast(field));
        if (self.right_fields.len > 0) alloc.free(self.right_fields);
        if (self.strategy_hint) |hint| alloc.free(hint);
        if (self.nested_join) |nested| {
            nested.deinit(alloc);
            alloc.destroy(nested);
        }
        self.* = undefined;
    }
};

pub const SupportedJoinFilters = struct {
    filter_query: ?std.json.Value = null,
    filter_prefix: ?[]u8 = null,
    limit: ?usize = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.filter_query) |*query| deinitJsonValue(alloc, query);
        if (self.filter_prefix) |prefix| alloc.free(prefix);
        self.* = undefined;
    }
};

pub const JoinedQueryStats = struct {
    left_rows_scanned: i64 = 0,
    right_rows_scanned: i64 = 0,
    rows_matched: i64 = 0,
    rows_unmatched_left: i64 = 0,
    rows_unmatched_right: i64 = 0,
};

pub const JoinTableStats = struct {
    row_count: u64 = 0,
    size_bytes: u64 = 0,
    has_stats: bool = false,
};

pub const PlannedJoinExecution = struct {
    pub const StrategyUsed = enum {
        index_lookup,
        broadcast,
    };

    strategy: StrategyUsed = .broadcast,
    estimated_cost: f64 = 0,
    estimated_rows: u64 = 0,
    estimated_memory_bytes: u64 = 0,
    used_stats: bool = false,
    shuffle_partitions: usize = 0,
    shuffle_candidate: bool = false,
    forced_broadcast_fallback: bool = false,
};

pub const RightJoinQueryResult = struct {
    hits: []std.json.Value = &.{},
    strategy_used: PlannedJoinExecution.StrategyUsed = .broadcast,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.hits) |*hit| deinitJsonValue(alloc, hit);
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
};

pub const ParsedSupportedJoinRequest = struct {
    join: SupportedJoinRequest,
    foreign_sources: foreign_mod.PostgresSourceMap = .{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        freeSupportedJoinRequest(alloc, &self.join);
        self.foreign_sources.deinit(alloc);
        self.* = undefined;
    }
};

pub fn freeSupportedJoinRequest(alloc: Allocator, join: *const SupportedJoinRequest) void {
    var owned = join.*;
    owned.deinit(alloc);
}

pub fn joinUsesForeignSource(join: SupportedJoinRequest, foreign_sources: foreign_mod.PostgresSourceMap) bool {
    if (foreign_sources.contains(join.right_table)) return true;
    if (join.nested_join) |nested| return joinUsesForeignSource(nested.*, foreign_sources);
    return false;
}

pub fn parseSupportedJoinRequest(
    alloc: Allocator,
    body: []const u8,
) !?ParsedSupportedJoinRequest {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    const join_value = parsed.value.object.get("join") orelse return null;
    if (join_value == .null) return null;
    var parsed_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidQueryRequest;
    defer parsed_request.deinit();
    return .{
        .join = try parseSupportedJoinClauseValue(alloc, join_value),
        .foreign_sources = foreign_sources_api.postgresSourceMapFromMetadataOpenApiResolved(alloc, parsed_request.value.foreign_sources) catch |err| switch (err) {
            error.UnsupportedSourceKind => return error.UnsupportedQueryRequest,
            else => return err,
        },
    };
}

pub fn parseSupportedJoinClauseValue(
    alloc: Allocator,
    join_value: std.json.Value,
) !SupportedJoinRequest {
    if (join_value != .object) return error.InvalidQueryRequest;

    var join_type: SupportedJoinRequest.JoinType = .inner;
    if (join_value.object.get("join_type")) |value| {
        if (value != .string) return error.InvalidQueryRequest;
        if (std.mem.eql(u8, value.string, "inner")) {
            join_type = .inner;
        } else if (std.mem.eql(u8, value.string, "left")) {
            join_type = .left;
        } else if (std.mem.eql(u8, value.string, "right")) {
            join_type = .right;
        } else {
            return error.InvalidQueryRequest;
        }
    }

    var strategy_hint: ?[]u8 = null;
    errdefer if (strategy_hint) |hint| alloc.free(hint);
    if (join_value.object.get("strategy_hint")) |value| {
        if (value != .null) {
            if (value != .string) return error.InvalidQueryRequest;
            if (!std.mem.eql(u8, value.string, "index_lookup") and
                !std.mem.eql(u8, value.string, "broadcast") and
                !std.mem.eql(u8, value.string, "shuffle"))
                return error.InvalidQueryRequest;
            strategy_hint = try alloc.dupe(u8, value.string);
        }
    }

    const on_value = join_value.object.get("on") orelse return error.InvalidQueryRequest;
    if (on_value != .object) return error.InvalidQueryRequest;
    if (on_value.object.get("operator")) |value| {
        if (value != .string) return error.InvalidQueryRequest;
        if (!std.mem.eql(u8, value.string, "eq")) return error.UnsupportedQueryRequest;
    }

    const right_table = join_value.object.get("right_table") orelse return error.InvalidQueryRequest;
    const left_field = on_value.object.get("left_field") orelse return error.InvalidQueryRequest;
    const right_field = on_value.object.get("right_field") orelse return error.InvalidQueryRequest;
    if (right_table != .string or left_field != .string or right_field != .string) return error.InvalidQueryRequest;
    if (right_table.string.len == 0 or left_field.string.len == 0 or right_field.string.len == 0) return error.InvalidQueryRequest;

    var right_filters: ?SupportedJoinFilters = null;
    errdefer if (right_filters) |*filters| filters.deinit(alloc);
    if (join_value.object.get("right_filters")) |value| {
        if (value != .null) {
            if (value != .object) return error.InvalidQueryRequest;
            var filters = SupportedJoinFilters{};
            if (value.object.get("filter_query")) |filter_query| {
                if (filter_query != .null) filters.filter_query = try cloneJsonValue(alloc, filter_query);
            }
            if (value.object.get("filter_prefix")) |filter_prefix| {
                if (filter_prefix != .null) {
                    if (filter_prefix != .string) return error.InvalidQueryRequest;
                    filters.filter_prefix = try alloc.dupe(u8, filter_prefix.string);
                }
            }
            if (value.object.get("limit")) |limit| {
                if (limit != .null) {
                    if (limit != .integer or limit.integer < 0) return error.InvalidQueryRequest;
                    filters.limit = @intCast(limit.integer);
                }
            }
            right_filters = filters;
        }
    }

    var right_fields: [][]const u8 = &.{};
    if (join_value.object.get("right_fields")) |value| {
        if (value != .null) {
            if (value != .array) return error.InvalidQueryRequest;
            right_fields = try alloc.alloc([]const u8, value.array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (right_fields[0..initialized]) |field| alloc.free(@constCast(field));
                alloc.free(right_fields);
            }
            for (value.array.items, 0..) |item, idx| {
                if (item != .string) return error.InvalidQueryRequest;
                right_fields[idx] = try alloc.dupe(u8, item.string);
                initialized += 1;
            }
        }
    }

    var nested_join: ?*SupportedJoinRequest = null;
    errdefer if (nested_join) |nested| {
        nested.deinit(alloc);
        alloc.destroy(nested);
    };
    if (join_value.object.get("nested_join")) |value| {
        if (value != .null) {
            nested_join = try alloc.create(SupportedJoinRequest);
            nested_join.?.* = try parseSupportedJoinClauseValue(alloc, value);
        }
    }

    return .{
        .right_table = try alloc.dupe(u8, right_table.string),
        .join_type = join_type,
        .left_field = try alloc.dupe(u8, left_field.string),
        .right_field = try alloc.dupe(u8, right_field.string),
        .right_filters = right_filters,
        .right_fields = right_fields,
        .strategy_hint = strategy_hint,
        .nested_join = nested_join,
    };
}

pub fn planSupportedJoinExecution(
    foreign_registry: ?*const foreign_mod.Registry,
    alloc: Allocator,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    foreign_sources: foreign_mod.PostgresSourceMap,
) PlannedJoinExecution {
    const left_rows: u64 = @intCast(left_hits.len);
    const distinct_left_keys = join_model.countDistinctJoinKeys(alloc, left_hits, join.left_field, extractJoinValueFromHit) catch 0;
    const supported_index_lookup = join_model.joinSupportsIndexLookup(join);

    var plan: PlannedJoinExecution = .{
        .estimated_rows = left_rows,
    };
    const right_stats = if (foreign_sources.get(join.right_table)) |foreign_source|
        estimateForeignJoinTableStats(foreign_registry, alloc, foreign_source) orelse JoinTableStats{}
    else
        JoinTableStats{};
    plan.used_stats = right_stats.has_stats;

    if (join_model.resolveJoinStrategyHint(PlannedJoinExecution.StrategyUsed, join, supported_index_lookup)) |hint| {
        plan.strategy = hint.strategy;
        plan.shuffle_candidate = hint.shuffle_requested;
        plan.forced_broadcast_fallback = hint.forced_broadcast_fallback;
        estimateJoinPlanCosts(&plan, left_rows, distinct_left_keys, right_stats);
        return plan;
    }

    if (plan.used_stats) {
        plan.strategy = join_model.chooseBroadcastOrIndexLookupWithStats(
            PlannedJoinExecution.StrategyUsed,
            supported_index_lookup,
            distinct_left_keys,
            right_stats.row_count,
            right_stats.size_bytes,
        );
    } else {
        plan.strategy = join_model.chooseJoinExecutionStrategyWithoutStats(PlannedJoinExecution.StrategyUsed, join, supported_index_lookup);
    }
    estimateJoinPlanCosts(&plan, left_rows, distinct_left_keys, right_stats);
    return plan;
}

pub fn estimateJoinPlanCosts(
    plan: *PlannedJoinExecution,
    left_rows: u64,
    distinct_left_keys: u64,
    right_stats: JoinTableStats,
) void {
    switch (plan.strategy) {
        .broadcast => {
            const right_rows = if (right_stats.row_count > 0) right_stats.row_count else left_rows;
            join_model.applyBroadcastJoinPlanCost(
                &plan.estimated_cost,
                &plan.estimated_memory_bytes,
                left_rows,
                right_rows,
                right_stats.size_bytes,
            );
        },
        .index_lookup => {
            join_model.applyIndexLookupJoinPlanCostSimple(
                &plan.estimated_cost,
                &plan.estimated_memory_bytes,
                left_rows,
                distinct_left_keys,
            );
        },
    }
}

pub fn estimateForeignJoinTableStats(foreign_registry: ?*const foreign_mod.Registry, alloc: Allocator, foreign_source: foreign_mod.PostgresConfig) ?JoinTableStats {
    const registry = foreign_registry orelse return null;
    const source_config = foreign_source.toSourceConfig(alloc) catch return null;

    var source = registry.create(alloc, source_config) catch return null;
    defer source.deinit(alloc);

    const stats = source.statistics(foreign_source.postgres_table) catch return null;
    return .{
        .row_count = @intCast(@max(stats.row_count, 0)),
        .size_bytes = @intCast(@max(stats.size_bytes, 0)),
        .has_stats = true,
    };
}

pub fn buildRightJoinQueryValue(
    alloc: Allocator,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
) !std.json.Value {
    const filter_query_value = blk: {
        if (join.join_type == .right) {
            if (join.right_filters) |filters| {
                if (filters.filter_query) |filter_query| break :blk try cloneJsonValue(alloc, filter_query);
            }
            break :blk null;
        }

        var disjuncts = std.json.Array.init(alloc);
        errdefer {
            for (disjuncts.items) |*item| deinitJsonValue(alloc, item);
            disjuncts.deinit();
        }

        for (left_hits) |hit_value| {
            const left_value = extractJoinValueFromHit(hit_value, join.left_field) orelse continue;
            try disjuncts.append(try buildJoinEqualityQuery(alloc, join.right_field, left_value));
        }

        var join_filter_obj = std.json.ObjectMap.empty;
        try putJsonObjectFieldOwned(alloc, &join_filter_obj, "disjuncts", .{ .array = disjuncts });
        if (join.right_filters) |filters| {
            break :blk try buildCombinedRightFilterQueryValue(alloc, filters, .{ .object = join_filter_obj });
        }
        break :blk std.json.Value{ .object = join_filter_obj };
    };

    var root = std.json.ObjectMap.empty;
    if (filter_query_value) |filter_query| {
        try putJsonObjectFieldOwned(alloc, &root, "filter_query", filter_query);
    }
    const requested_limit = if (join.right_filters) |filters| filters.limit else null;
    try putJsonObjectFieldOwned(alloc, &root, "limit", .{ .integer = @intCast(requested_limit orelse @max(@as(usize, 10), left_hits.len)) });
    if (join.right_filters) |filters| {
        if (filters.filter_prefix) |prefix| {
            try putJsonObjectFieldOwned(alloc, &root, "filter_prefix", .{ .string = try alloc.dupe(u8, prefix) });
        }
    }
    if (join.right_fields.len > 0 or !std.mem.eql(u8, join.right_field, "_id")) {
        var fields = std.json.Array.init(alloc);
        var saw_join_field = false;
        for (join.right_fields) |field| {
            try fields.append(.{ .string = try alloc.dupe(u8, field) });
            if (std.mem.eql(u8, field, join.right_field)) saw_join_field = true;
        }
        if (!std.mem.eql(u8, join.right_field, "_id") and !saw_join_field) {
            try fields.append(.{ .string = try alloc.dupe(u8, join.right_field) });
        }
        try putJsonObjectFieldOwned(alloc, &root, "fields", .{ .array = fields });
    }
    if (join.strategy_hint) |hint| {
        try putJsonObjectFieldOwned(alloc, &root, "strategy_hint", .{ .string = try alloc.dupe(u8, hint) });
    }
    if (join.nested_join) |nested| {
        try putJsonObjectFieldOwned(alloc, &root, "join", try buildSupportedJoinClauseValue(alloc, nested.*));
    }
    return .{ .object = root };
}

pub fn buildCombinedRightFilterQueryValue(
    alloc: Allocator,
    filters: SupportedJoinFilters,
    join_filter: std.json.Value,
) !std.json.Value {
    if (filters.filter_query) |filter_query| {
        var conjuncts = std.json.Array.init(alloc);
        errdefer {
            for (conjuncts.items) |*item| deinitJsonValue(alloc, item);
            conjuncts.deinit();
        }
        try conjuncts.append(try cloneJsonValue(alloc, filter_query));
        try conjuncts.append(join_filter);
        var obj = std.json.ObjectMap.empty;
        try putJsonObjectFieldOwned(alloc, &obj, "conjuncts", .{ .array = conjuncts });
        return .{ .object = obj };
    }
    return join_filter;
}

pub fn buildSupportedJoinClauseValue(
    alloc: Allocator,
    join: SupportedJoinRequest,
) !std.json.Value {
    var join_obj = std.json.ObjectMap.empty;
    try putJsonObjectFieldOwned(alloc, &join_obj, "right_table", .{ .string = try alloc.dupe(u8, join.right_table) });
    try putJsonObjectFieldOwned(alloc, &join_obj, "join_type", .{ .string = try alloc.dupe(u8, switch (join.join_type) {
        .inner => "inner",
        .left => "left",
        .right => "right",
    }) });

    var on_obj = std.json.ObjectMap.empty;
    try putJsonObjectFieldOwned(alloc, &on_obj, "left_field", .{ .string = try alloc.dupe(u8, join.left_field) });
    try putJsonObjectFieldOwned(alloc, &on_obj, "right_field", .{ .string = try alloc.dupe(u8, join.right_field) });
    try putJsonObjectFieldOwned(alloc, &on_obj, "operator", .{ .string = try alloc.dupe(u8, "eq") });
    try putJsonObjectFieldOwned(alloc, &join_obj, "on", .{ .object = on_obj });

    if (join.right_filters) |filters| {
        var filters_obj = std.json.ObjectMap.empty;
        if (filters.filter_query) |filter_query| {
            try putJsonObjectFieldOwned(alloc, &filters_obj, "filter_query", try cloneJsonValue(alloc, filter_query));
        }
        if (filters.filter_prefix) |prefix| {
            try putJsonObjectFieldOwned(alloc, &filters_obj, "filter_prefix", .{ .string = try alloc.dupe(u8, prefix) });
        }
        if (filters.limit) |limit| {
            try putJsonObjectFieldOwned(alloc, &filters_obj, "limit", .{ .integer = @intCast(limit) });
        }
        try putJsonObjectFieldOwned(alloc, &join_obj, "right_filters", .{ .object = filters_obj });
    }

    if (join.right_fields.len > 0) {
        var fields = std.json.Array.init(alloc);
        for (join.right_fields) |field| {
            try fields.append(.{ .string = try alloc.dupe(u8, field) });
        }
        try putJsonObjectFieldOwned(alloc, &join_obj, "right_fields", .{ .array = fields });
    }

    if (join.strategy_hint) |hint| {
        try putJsonObjectFieldOwned(alloc, &join_obj, "strategy_hint", .{ .string = try alloc.dupe(u8, hint) });
    }

    if (join.nested_join) |nested| {
        try putJsonObjectFieldOwned(alloc, &join_obj, "nested_join", try buildSupportedJoinClauseValue(alloc, nested.*));
    }

    return .{ .object = join_obj };
}

pub fn buildJoinEqualityQuery(
    alloc: Allocator,
    field_name: []const u8,
    value: std.json.Value,
) !std.json.Value {
    var query_obj = std.json.ObjectMap.empty;
    switch (value) {
        .string => |text| {
            try putJsonObjectFieldOwned(alloc, &query_obj, "term", .{ .string = try alloc.dupe(u8, text) });
            try putJsonObjectFieldOwned(alloc, &query_obj, "field", .{ .string = try alloc.dupe(u8, field_name) });
        },
        .integer => |number| {
            try putJsonObjectFieldOwned(alloc, &query_obj, "field", .{ .string = try alloc.dupe(u8, field_name) });
            try putJsonObjectFieldOwned(alloc, &query_obj, "min", .{ .integer = number });
            try putJsonObjectFieldOwned(alloc, &query_obj, "max", .{ .integer = number });
            try putJsonObjectFieldOwned(alloc, &query_obj, "inclusive_min", .{ .bool = true });
            try putJsonObjectFieldOwned(alloc, &query_obj, "inclusive_max", .{ .bool = true });
        },
        .float => |number| {
            try putJsonObjectFieldOwned(alloc, &query_obj, "field", .{ .string = try alloc.dupe(u8, field_name) });
            try putJsonObjectFieldOwned(alloc, &query_obj, "min", .{ .float = number });
            try putJsonObjectFieldOwned(alloc, &query_obj, "max", .{ .float = number });
            try putJsonObjectFieldOwned(alloc, &query_obj, "inclusive_min", .{ .bool = true });
            try putJsonObjectFieldOwned(alloc, &query_obj, "inclusive_max", .{ .bool = true });
        },
        .bool => |flag| {
            try putJsonObjectFieldOwned(alloc, &query_obj, "field", .{ .string = try alloc.dupe(u8, field_name) });
            try putJsonObjectFieldOwned(alloc, &query_obj, "bool", .{ .bool = flag });
        },
        else => return error.UnsupportedQueryRequest,
    }
    return .{ .object = query_obj };
}

pub fn ensureQueryFieldsContains(
    alloc: Allocator,
    request: *std.json.Value,
    field_name: []const u8,
) !bool {
    if (std.mem.eql(u8, field_name, "_id")) return false;
    if (request.* != .object) return error.InvalidQueryRequest;
    const fields_ptr = request.object.getPtr("fields") orelse return false;
    if (fields_ptr.* == .null) return false;
    if (fields_ptr.* != .array) return error.InvalidQueryRequest;
    for (fields_ptr.array.items) |item| {
        if (item != .string) return error.InvalidQueryRequest;
        if (std.mem.eql(u8, item.string, field_name)) return false;
    }
    try fields_ptr.array.append(.{ .string = try alloc.dupe(u8, field_name) });
    return true;
}

pub fn queryHitsArrayPtr(root: *std.json.Value) !*std.json.Array {
    return try join_model.queryHitsArrayPtr(root);
}

pub fn queryTotalHits(root: std.json.Value) !usize {
    if (root != .object) return error.InvalidQueryRequest;
    const responses = root.object.get("responses") orelse return error.InvalidQueryRequest;
    if (responses != .array or responses.array.items.len == 0) return error.InvalidQueryRequest;
    const response = responses.array.items[0];
    if (response != .object) return error.InvalidQueryRequest;
    const hits = response.object.get("hits") orelse return error.InvalidQueryRequest;
    if (hits != .object) return error.InvalidQueryRequest;
    const total = hits.object.get("total") orelse return error.InvalidQueryRequest;
    return switch (total) {
        .integer => |value| @intCast(value),
        else => error.InvalidQueryRequest,
    };
}

pub fn queryRequestedFields(root: std.json.Value) []const std.json.Value {
    if (root != .object) return &.{};
    const fields = root.object.get("fields") orelse return &.{};
    return switch (fields) {
        .array => |arr| arr.items,
        else => &.{},
    };
}

pub fn extractJoinValueFromHit(hit: std.json.Value, field_name: []const u8) ?std.json.Value {
    if (hit != .object) return null;
    if (std.mem.eql(u8, field_name, "_id")) return hit.object.get("_id");
    const source = hit.object.get("_source") orelse return null;
    return extractJsonPathValue(source, field_name);
}

pub fn extractJoinValueFromDocument(doc_id: []const u8, source: std.json.Value, field_name: []const u8) ?std.json.Value {
    if (std.mem.eql(u8, field_name, "_id")) return .{ .string = doc_id };
    return json_helpers.extractJsonPathValue(source, field_name);
}

pub fn extractJsonPathValue(value: std.json.Value, path: []const u8) ?std.json.Value {
    return json_helpers.extractJsonPathValue(value, path);
}

pub fn freeOwnedStringSlice(alloc: Allocator, values: [][]u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

pub fn buildForeignJoinFieldListAlloc(
    alloc: Allocator,
    join: SupportedJoinRequest,
) ![][]u8 {
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |value| alloc.free(value);
        out.deinit(alloc);
    }

    if (join.right_fields.len > 0) {
        for (join.right_fields) |field| {
            if (join.nested_join != null and std.mem.indexOfScalar(u8, field, '.') != null) continue;
            try out.append(alloc, try alloc.dupe(u8, field));
        }
        var has_match_field = false;
        var has_nested_left_field = false;
        for (join.right_fields) |field| {
            if (std.mem.eql(u8, field, join.right_field)) {
                has_match_field = true;
            }
            if (join.nested_join) |nested| {
                if (std.mem.eql(u8, field, nested.left_field)) has_nested_left_field = true;
            }
        }
        if (!has_match_field) try out.append(alloc, try alloc.dupe(u8, join.right_field));
        if (join.nested_join) |nested| {
            if (!has_nested_left_field) try out.append(alloc, try alloc.dupe(u8, nested.left_field));
        }
    }

    return try out.toOwnedSlice(alloc);
}

pub fn scalarJsonValueStringAlloc(alloc: Allocator, value: std.json.Value) !?[]u8 {
    return try json_helpers.scalarJsonValueStringAlloc(alloc, value);
}

pub fn buildForeignRightJoinHit(
    alloc: Allocator,
    foreign_source: foreign_mod.PostgresConfig,
    row: std.json.Value,
    match_value: std.json.Value,
) !std.json.Value {
    var hit_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = hit_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        hit_obj.deinit(alloc);
    }
    if ((try foreign_sources_api.deriveSearchIdAlloc(alloc, foreign_source, row)) orelse try scalarJsonValueStringAlloc(alloc, match_value)) |text| {
        try putJsonObjectFieldOwned(alloc, &hit_obj, "_id", .{ .string = text });
    }
    try putJsonObjectFieldOwned(alloc, &hit_obj, "_score", .{ .float = 0 });
    try putJsonObjectFieldOwned(alloc, &hit_obj, "_source", try cloneJsonValue(alloc, row));
    return .{ .object = hit_obj };
}

pub fn buildRightJoinHitFromDocument(
    alloc: Allocator,
    doc_id: []const u8,
    source: std.json.Value,
) !std.json.Value {
    var hit_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = hit_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        hit_obj.deinit(alloc);
    }
    try putJsonObjectFieldOwned(alloc, &hit_obj, "_id", .{ .string = try alloc.dupe(u8, doc_id) });
    try putJsonObjectFieldOwned(alloc, &hit_obj, "_score", .{ .float = 0 });
    try putJsonObjectFieldOwned(alloc, &hit_obj, "_source", try cloneJsonValue(alloc, source));
    return .{ .object = hit_obj };
}

pub fn findFirstMatchingRightHit(
    join: SupportedJoinRequest,
    left_value: std.json.Value,
    right_hits: []const std.json.Value,
) ?std.json.Value {
    return join_model.findMatchingRightHit(
        right_hits,
        join.right_field,
        left_value,
        {},
        extractJoinValueFromHit,
        struct {
            fn call(_: void, left: std.json.Value, right: std.json.Value) bool {
                return jsonValuesEqual(left, right);
            }
        }.call,
    );
}

pub fn maybeAttachJoinProfile(
    alloc: Allocator,
    root: *std.json.Value,
    stats: JoinedQueryStats,
    plan: PlannedJoinExecution,
    strategy_used: PlannedJoinExecution.StrategyUsed,
) !void {
    const payload: join_model.JoinProfilePayload = .{
        .strategy_used = switch (strategy_used) {
            .index_lookup => "index_lookup",
            .broadcast => "broadcast",
        },
        .left_rows_scanned = stats.left_rows_scanned,
        .right_rows_scanned = stats.right_rows_scanned,
        .rows_matched = stats.rows_matched,
        .rows_unmatched_left = stats.rows_unmatched_left,
        .rows_unmatched_right = stats.rows_unmatched_right,
        .estimated_cost = plan.estimated_cost,
        .estimated_rows = plan.estimated_rows,
        .estimated_memory_bytes = plan.estimated_memory_bytes,
        .planner_used_stats = false,
        .distributed_execution = false,
        .groups_queried = 1,
        .shuffle_partitions = 0,
        .shuffle_candidate = plan.shuffle_candidate,
        .forced_broadcast_fallback = plan.forced_broadcast_fallback,
    };
    try join_model.applyJoinProfileToResponse(alloc, root, payload);
}

pub fn allocRequestedFieldsFromHits(
    alloc: Allocator,
    hits: []const std.json.Value,
) ![]std.json.Value {
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);

    var out = std.ArrayListUnmanaged(std.json.Value).empty;
    errdefer {
        freeJsonStringArray(alloc, out.items);
        out.deinit(alloc);
    }

    for (hits) |hit| {
        if (hit != .object) continue;
        const source = hit.object.get("_source") orelse continue;
        if (source != .object) continue;
        var it = source.object.iterator();
        while (it.next()) |entry| {
            if (seen.contains(entry.key_ptr.*)) continue;
            try seen.put(alloc, entry.key_ptr.*, {});
            try out.append(alloc, .{ .string = try alloc.dupe(u8, entry.key_ptr.*) });
        }
    }

    return try out.toOwnedSlice(alloc);
}

pub fn freeJsonStringArray(alloc: Allocator, items: []std.json.Value) void {
    for (items) |*item| deinitJsonValue(alloc, item);
    if (items.len > 0) alloc.free(items);
}

pub fn documentMatchesPublicTextSpec(
    alloc: Allocator,
    doc_id: []const u8,
    source: std.json.Value,
    spec: public_text_query.PublicTextSpec,
) !bool {
    _ = doc_id;
    const text_value = extractJsonPathValue(source, "text") orelse extractJsonPathValue(source, "body") orelse return false;
    if (text_value != .string) return false;
    const haystack = try std.ascii.allocLowerString(alloc, text_value.string);
    defer alloc.free(haystack);
    const needle = try std.ascii.allocLowerString(alloc, spec.text);
    defer alloc.free(needle);

    switch (spec.operator) {
        .phrase => return std.mem.indexOf(u8, haystack, needle) != null,
        .all_terms => {
            var parts = std.mem.tokenizeAny(u8, needle, &std.ascii.whitespace);
            while (parts.next()) |part| {
                if (std.mem.indexOf(u8, haystack, part) == null) return false;
            }
            return true;
        },
        .any_terms => {
            var parts = std.mem.tokenizeAny(u8, needle, &std.ascii.whitespace);
            while (parts.next()) |part| {
                if (std.mem.indexOf(u8, haystack, part) != null) return true;
            }
            return false;
        },
        .prefix_any_term => {
            var prefixes = std.mem.tokenizeAny(u8, needle, &std.ascii.whitespace);
            while (prefixes.next()) |prefix| {
                var words = std.mem.tokenizeAny(u8, haystack, &std.ascii.whitespace);
                while (words.next()) |word| {
                    if (std.mem.startsWith(u8, word, prefix)) return true;
                }
            }
            return false;
        },
    }
}

pub fn putJsonObjectFieldOwned(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    try obj.put(alloc, try alloc.dupe(u8, key), value);
}

pub fn cloneJsonValue(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return try json_helpers.cloneJsonValue(alloc, value);
}

pub fn deinitJsonValue(alloc: Allocator, value: *std.json.Value) void {
    json_helpers.deinitJsonValue(alloc, value);
}

pub fn computeJsonHitMaxScore(hits: []const std.json.Value) f64 {
    return join_model.computeJsonHitMaxScore(hits);
}

pub fn jsonValuesEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
    return json_helpers.jsonValuesEqual(lhs, rhs);
}
