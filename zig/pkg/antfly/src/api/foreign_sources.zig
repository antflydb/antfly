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
const foreign_mod = @import("../foreign/mod.zig");
const secrets = @import("../common/secrets.zig");
const aggregations_mod = @import("../storage/db/aggregations.zig");
const json_helpers = @import("json_helpers.zig");

const Allocator = std.mem.Allocator;

pub fn postgresSourceMapFromPublicOpenApiResolved(alloc: Allocator, foreign_sources: anytype) !foreign_mod.PostgresSourceMap {
    var source_map = try foreign_mod.postgresSourceMapFromPublicOpenApi(alloc, foreign_sources);
    errdefer source_map.deinit(alloc);
    try resolvePostgresSourceMapAlloc(alloc, &source_map);
    return source_map;
}

pub fn postgresSourceMapFromMetadataOpenApiResolved(alloc: Allocator, foreign_sources: anytype) !foreign_mod.PostgresSourceMap {
    var source_map = try foreign_mod.postgresSourceMapFromMetadataOpenApi(alloc, foreign_sources);
    errdefer source_map.deinit(alloc);
    try resolvePostgresSourceMapAlloc(alloc, &source_map);
    return source_map;
}

pub fn resolvePostgresSourceMapAlloc(alloc: Allocator, source_map: *foreign_mod.PostgresSourceMap) !void {
    for (source_map.entries) |*entry| {
        const resolved = try secrets.resolveReferenceOwned(alloc, null, entry.config.dsn);
        alloc.free(entry.config.dsn);
        entry.config.dsn = resolved;
    }
}

pub fn buildPostgresAggregateParamsAlloc(
    alloc: Allocator,
    foreign_source: foreign_mod.PostgresConfig,
    requests: []const aggregations_mod.SearchAggregationRequest,
    filter_query_json: ?[]const u8,
) !foreign_mod.AggregateParams {
    return .{
        .table = try alloc.dupe(u8, foreign_source.postgres_table),
        .filter_query_json = if (filter_query_json) |query| try alloc.dupe(u8, query) else null,
        .columns = try cloneForeignColumnsAlloc(alloc, foreign_source.columns),
        .aggregations = try cloneNamedAggregationsAlloc(alloc, requests),
    };
}

pub fn deriveSearchIdField(foreign_source: foreign_mod.PostgresConfig) ?[]const u8 {
    for (foreign_source.columns) |column| {
        if (std.mem.eql(u8, column.name, "id")) return column.name;
    }
    for (foreign_source.columns) |column| {
        if (std.mem.eql(u8, column.name, "_id")) return column.name;
    }
    for (foreign_source.columns) |column| {
        if (std.mem.endsWith(u8, column.name, "_id")) return column.name;
    }
    return null;
}

pub fn buildEffectiveFilterQueryJsonAlloc(
    alloc: Allocator,
    foreign_source: foreign_mod.PostgresConfig,
    filter_query_json: ?[]const u8,
    filter_prefix: ?[]const u8,
) !?[]u8 {
    if (filter_prefix == null) {
        return if (filter_query_json) |query| try alloc.dupe(u8, query) else null;
    }

    const id_field = deriveSearchIdField(foreign_source) orelse return error.UnsupportedQueryRequest;

    var prefix_obj = std.json.ObjectMap.empty;
    errdefer prefix_obj.deinit(alloc);
    try prefix_obj.put(alloc, try alloc.dupe(u8, "prefix"), .{ .string = try alloc.dupe(u8, filter_prefix.?) });
    try prefix_obj.put(alloc, try alloc.dupe(u8, "field"), .{ .string = try alloc.dupe(u8, id_field) });
    var prefix_value = std.json.Value{ .object = prefix_obj };
    errdefer json_helpers.deinitJsonValue(alloc, &prefix_value);

    if (filter_query_json) |query| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, query, .{});
        defer parsed.deinit();

        var conjuncts = std.json.Array.init(alloc);
        errdefer {
            for (conjuncts.items) |*item| json_helpers.deinitJsonValue(alloc, item);
            conjuncts.deinit();
        }
        try conjuncts.append(try json_helpers.cloneJsonValue(alloc, parsed.value));
        try conjuncts.append(prefix_value);
        prefix_value = undefined;

        var root = std.json.ObjectMap.empty;
        errdefer root.deinit(alloc);
        try root.put(alloc, try alloc.dupe(u8, "conjuncts"), .{ .array = conjuncts });
        return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = root }, .{});
    }

    defer json_helpers.deinitJsonValue(alloc, &prefix_value);
    return try std.json.Stringify.valueAlloc(alloc, prefix_value, .{});
}

pub fn deriveSearchIdAlloc(
    alloc: Allocator,
    foreign_source: foreign_mod.PostgresConfig,
    row: std.json.Value,
) !?[]u8 {
    if (row != .object) return error.InvalidQueryRequest;
    if (deriveSearchIdField(foreign_source)) |field_name| {
        if (row.object.get(field_name)) |value| return try scalarJsonValueStringAlloc(alloc, value);
    }
    if (row.object.get("id")) |value| return try scalarJsonValueStringAlloc(alloc, value);
    if (row.object.get("_id")) |value| return try scalarJsonValueStringAlloc(alloc, value);
    return null;
}

pub fn foreignAggregateResultsToSearchResultsAlloc(
    alloc: Allocator,
    requests: []const aggregations_mod.SearchAggregationRequest,
    aggregate_result: foreign_mod.AggregateResult,
) ![]aggregations_mod.SearchAggregationResult {
    const out = try alloc.alloc(aggregations_mod.SearchAggregationResult, aggregate_result.results.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }

    for (aggregate_result.results) |result| {
        const request = findAggregationRequest(requests, result.name) orelse return error.InvalidQueryRequest;
        out[initialized] = try switchAggregationResultAlloc(alloc, request, result);
        initialized += 1;
    }
    return out[0..initialized];
}

fn findAggregationRequest(
    requests: []const aggregations_mod.SearchAggregationRequest,
    name: []const u8,
) ?aggregations_mod.SearchAggregationRequest {
    for (requests) |request| {
        if (std.mem.eql(u8, request.name, name)) return request;
    }
    return null;
}

fn switchAggregationResultAlloc(
    alloc: Allocator,
    request: aggregations_mod.SearchAggregationRequest,
    result: foreign_mod.NamedValue,
) !aggregations_mod.SearchAggregationResult {
    if (std.mem.eql(u8, request.type, "terms")) {
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = try aggregationBucketsFromValueAlloc(alloc, result.value),
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = try std.json.Stringify.valueAlloc(alloc, result.value, .{}),
    };
}

fn aggregationBucketsFromValueAlloc(
    alloc: Allocator,
    value: std.json.Value,
) ![]aggregations_mod.SearchAggregationBucket {
    if (value != .array) return &.{};
    const out = try alloc.alloc(aggregations_mod.SearchAggregationBucket, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*bucket| bucket.deinit(alloc);
        alloc.free(out);
    }
    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidQueryRequest;
        const key_value = item.object.get("key") orelse return error.InvalidQueryRequest;
        const count_value = item.object.get("doc_count") orelse return error.InvalidQueryRequest;
        out[i] = .{
            .key_json = try std.json.Stringify.valueAlloc(alloc, key_value, .{}),
            .count = try jsonValueToI64(count_value),
        };
        initialized += 1;
    }
    return out;
}

fn jsonValueToI64(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| number,
        .float => |number| @intFromFloat(number),
        .number_string => |text| try std.fmt.parseInt(i64, text, 10),
        else => error.InvalidQueryRequest,
    };
}

fn scalarJsonValueStringAlloc(alloc: Allocator, value: std.json.Value) !?[]u8 {
    return try json_helpers.scalarJsonValueStringAlloc(alloc, value);
}

fn cloneForeignColumnsAlloc(
    alloc: Allocator,
    columns: []const foreign_mod.Column,
) ![]foreign_mod.Column {
    if (columns.len == 0) return &.{};
    const out = try alloc.alloc(foreign_mod.Column, columns.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*column| column.deinit(alloc);
        alloc.free(out);
    }
    for (columns, 0..) |column, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, column.name),
            .data_type = try alloc.dupe(u8, column.data_type),
            .nullable = column.nullable,
        };
        initialized += 1;
    }
    return out;
}

fn cloneNamedAggregationsAlloc(
    alloc: Allocator,
    requests: []const aggregations_mod.SearchAggregationRequest,
) ![]foreign_mod.NamedAggregation {
    if (requests.len == 0) return &.{};
    const out = try alloc.alloc(foreign_mod.NamedAggregation, requests.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*aggregation| aggregation.deinit(alloc);
        alloc.free(out);
    }
    for (requests, 0..) |request, i| {
        if (request.aggregations.len > 0 or request.background_query != null) return error.UnsupportedAggregate;
        out[i] = .{
            .name = try alloc.dupe(u8, request.name),
            .definition = .{
                .type_name = try alloc.dupe(u8, request.type),
                .field = if (request.field.len > 0) try alloc.dupe(u8, request.field) else null,
                .size = if (request.size > 0) @intCast(request.size) else null,
            },
        };
        initialized += 1;
    }
    return out;
}

test "derive foreign search id field prefers id-like columns" {
    var source = foreign_mod.PostgresConfig{
        .dsn = undefined,
        .postgres_table = undefined,
        .columns = @constCast(&[_]foreign_mod.Column{
            .{ .name = @constCast("customer_id"), .data_type = @constCast("text"), .nullable = false },
            .{ .name = @constCast("address_id"), .data_type = @constCast("text"), .nullable = false },
        }),
    };
    try std.testing.expectEqualStrings("customer_id", deriveSearchIdField(source).?);

    source.columns = @constCast(&[_]foreign_mod.Column{
        .{ .name = @constCast("id"), .data_type = @constCast("text"), .nullable = false },
        .{ .name = @constCast("customer_id"), .data_type = @constCast("text"), .nullable = false },
    });
    try std.testing.expectEqualStrings("id", deriveSearchIdField(source).?);
}

test "build effective foreign filter query combines prefix with existing filter" {
    const alloc = std.testing.allocator;
    const source = foreign_mod.PostgresConfig{
        .dsn = undefined,
        .postgres_table = undefined,
        .columns = @constCast(&[_]foreign_mod.Column{
            .{ .name = @constCast("customer_id"), .data_type = @constCast("text"), .nullable = false },
        }),
    };

    const query = try buildEffectiveFilterQueryJsonAlloc(
        alloc,
        source,
        "{\"term\":\"gold\",\"field\":\"tier\"}",
        "cust-",
    );
    defer if (query) |value| alloc.free(value);

    try std.testing.expectEqualStrings(
        "{\"conjuncts\":[{\"term\":\"gold\",\"field\":\"tier\"},{\"prefix\":\"cust-\",\"field\":\"customer_id\"}]}",
        query.?,
    );
}
