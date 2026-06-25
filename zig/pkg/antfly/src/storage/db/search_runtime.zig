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
const platform = @import("antfly_platform");
const builtin = @import("builtin");

const aggregations_mod = @import("aggregations.zig");
const algebraic_mod = @import("algebraic/mod.zig");
const artifact_ids = @import("artifact_ids.zig");
const db_internal = @import("internal.zig");
const db_config = @import("config.zig");
const derived_types = @import("derived/derived_types.zig");
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const docstore_mod = @import("../docstore.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const planning_adapter_mod = @import("planning_adapter.zig");
const planning_bindings_mod = @import("planning_bindings.zig");
const planning_stats_mod = @import("planning_stats.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_rows = @import("relational_rows.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_mod = @import("../schema.zig");
const graph_mod = @import("../../graph/graph.zig");
const paths_mod = @import("../../graph/paths.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const graph_query_mod = @import("../../graph/query.zig");
const graph_pattern_mod = @import("../../graph/pattern.zig");
const internal_keys = @import("../internal_keys.zig");
const search_geo_mod = @import("../../search/geo.zig");
const search_mod = @import("../../search/search.zig");
const ttl_mod = @import("../ttl.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");
const db_query_graph = @import("query/graph_exec.zig");
const db_query_metrics = @import("query_metrics.zig");
const db_query_projection = @import("query/projection.zig");
const db_query_result_shape = @import("query/result_shape.zig");
const db_query_search = @import("query/search_exec.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const graph_metric_runtime_mod = @import("maintenance/graph_metric_runtime.zig");
const platform_clock = @import("../../platform/clock.zig");
const platform_time = @import("../../platform/time.zig");
const vectorindex_mod = @import("antfly_vectorindex");
const mapper = @import("document_mapper.zig");

const Allocator = std.mem.Allocator;
const NamedResultSet = db_query_graph.NamedResultSet;
const AlgebraicIndex = @import("algebraic/index.zig").Index;

pub const AlgebraicDocFilterRequest = struct {
    req: types.SearchRequest,
    index: ?*AlgebraicIndex = null,
    filter_doc_ids: [][]u8 = &.{},
    exclude_doc_ids: [][]u8 = &.{},
    resolved_doc_filter: ?*doc_set.ResolvedDocFilter = null,
    resolved_doc_filter_alloc: ?Allocator = null,

    pub fn deinit(self: *@This()) void {
        if (self.index) |index| {
            index.freeDocIds(self.filter_doc_ids);
            index.freeDocIds(self.exclude_doc_ids);
        }
        if (self.resolved_doc_filter) |filter| {
            const alloc = self.resolved_doc_filter_alloc.?;
            filter.deinit(alloc);
            alloc.destroy(filter);
        }
        self.* = undefined;
    }
};

fn freeConstDocIdsAlloc(alloc: Allocator, doc_ids: []const []const u8) void {
    for (doc_ids) |doc_id| alloc.free(@constCast(doc_id));
    if (doc_ids.len > 0) alloc.free(@constCast(doc_ids));
}

fn freeOptionalOwnedBytes(alloc: Allocator, values: []?[]u8) void {
    for (values) |value| {
        if (value) |bytes| alloc.free(bytes);
    }
    alloc.free(values);
}

fn freeJsonValue(alloc: Allocator, value: *std.json.Value) void {
    db_query_projection.freeJsonValue(alloc, value);
}

fn cloneJsonValue(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return try db_query_projection.cloneJsonValue(alloc, value);
}

fn putOwnedValue(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    try db_query_projection.putOwnedValue(alloc, obj, key, value);
}

fn appendArtifactProjectionValue(
    alloc: Allocator,
    artifacts_obj: *std.json.ObjectMap,
    artifact_name: []const u8,
    artifact_kind: types.ArtifactKind,
    artifact_value: std.json.Value,
) !void {
    if (artifacts_obj.getPtr(artifact_name)) |existing| {
        if (existing.* == .object) {
            if (existing.object.getPtr("items")) |items| {
                if (items.* == .array) {
                    try items.array.append(artifact_value);
                    return;
                }
            }
        }

        var first = try cloneJsonValue(alloc, existing.*);
        var first_moved = false;
        errdefer if (!first_moved) freeJsonValue(alloc, &first);
        var items = std.json.Array.init(alloc);
        errdefer {
            for (items.items) |*item| freeJsonValue(alloc, item);
            items.deinit();
        }
        try items.append(first);
        first_moved = true;
        try items.append(artifact_value);

        var grouped = std.json.ObjectMap.empty;
        errdefer {
            var value = std.json.Value{ .object = grouped };
            freeJsonValue(alloc, &value);
        }
        try putOwnedValue(alloc, &grouped, "kind", .{ .string = try alloc.dupe(u8, artifactSetKindText(artifact_kind)) });
        try putOwnedValue(alloc, &grouped, "status", .{ .string = try alloc.dupe(u8, "ready") });
        try putOwnedValue(alloc, &grouped, "items", .{ .array = items });

        freeJsonValue(alloc, existing);
        existing.* = .{ .object = grouped };
        return;
    }

    try artifacts_obj.put(alloc, try alloc.dupe(u8, artifact_name), artifact_value);
}

fn artifactRefJsonValue(alloc: Allocator, artifact_ref: types.ArtifactRef) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    errdefer {
        var value = std.json.Value{ .object = obj };
        freeJsonValue(alloc, &value);
    }
    try putOwnedValue(alloc, &obj, "document_id", .{ .string = try alloc.dupe(u8, artifact_ref.document_id) });
    try putOwnedValue(alloc, &obj, "name", .{ .string = try alloc.dupe(u8, artifact_ref.name) });
    try putOwnedValue(alloc, &obj, "kind", .{ .string = try alloc.dupe(u8, artifactKindText(artifact_ref.kind)) });
    if (artifact_ref.chunk_id) |chunk_id| {
        try putOwnedValue(alloc, &obj, "chunk_id", .{ .integer = @intCast(chunk_id) });
    }
    if (artifact_ref.source) |source| {
        var source_obj = std.json.ObjectMap.empty;
        errdefer {
            var value = std.json.Value{ .object = source_obj };
            freeJsonValue(alloc, &value);
        }
        try putOwnedValue(alloc, &source_obj, "kind", .{ .string = try alloc.dupe(u8, artifactKindText(source.kind)) });
        try putOwnedValue(alloc, &source_obj, "name", .{ .string = try alloc.dupe(u8, source.name) });
        if (source.chunk_id) |chunk_id| {
            try putOwnedValue(alloc, &source_obj, "chunk_id", .{ .integer = @intCast(chunk_id) });
        }
        try putOwnedValue(alloc, &obj, "source", .{ .object = source_obj });
    }
    return .{ .object = obj };
}

fn artifactKindText(kind: types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk",
        .asset => "asset",
        .embedding => "embedding",
    };
}

fn artifactSetKindText(kind: types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk_set",
        .asset => "asset_set",
        .embedding => "embedding_set",
    };
}

fn artifactContentType(kind: types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "application/json",
        .asset => "application/octet-stream",
        .embedding => "application/vnd.antfly.embedding+binary",
    };
}

fn assetPayloadJsonValue(alloc: Allocator, content_type: []const u8, raw: []const u8) !std.json.Value {
    if (assetContentTypeIsJson(content_type)) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        return try cloneJsonValue(alloc, parsed.value);
    }
    return .{ .string = try alloc.dupe(u8, raw) };
}

fn assetContentTypeIsJson(content_type: []const u8) bool {
    const media_type = std.mem.trim(u8, if (std.mem.indexOfScalar(u8, content_type, ';')) |semi| content_type[0..semi] else content_type, &std.ascii.whitespace);
    return std.mem.eql(u8, media_type, "application/json") or std.mem.endsWith(u8, media_type, "+json");
}

pub fn resolvedDocSetFromSearchHitOrdinalsAlloc(alloc: Allocator, hits: []const types.SearchHit) !?doc_set.ResolvedDocSet {
    const ordinals = try alloc.alloc(doc_set.DocOrdinal, hits.len);
    defer if (ordinals.len > 0) alloc.free(ordinals);

    for (hits, 0..) |hit, i| {
        ordinals[i] = hit.doc_ordinal orelse return null;
    }
    return try doc_set.fromOrdinalsAlloc(alloc, ordinals);
}

test "resolved doc set from search hits uses complete hit ordinals" {
    const alloc = std.testing.allocator;

    const hits = [_]types.SearchHit{
        .{ .id = @constCast("doc:a"), .doc_ordinal = 3 },
        .{ .id = @constCast("doc:b"), .doc_ordinal = 1 },
    };
    var resolved = (try resolvedDocSetFromSearchHitOrdinalsAlloc(alloc, &hits)) orelse return error.TestUnexpectedResult;
    defer resolved.deinit(alloc);
    try std.testing.expect(resolved.containsOrdinal(1));
    try std.testing.expect(resolved.containsOrdinal(3));
    try std.testing.expect(!resolved.containsOrdinal(2));

    const mixed = [_]types.SearchHit{
        .{ .id = @constCast("doc:a"), .doc_ordinal = 1 },
        .{ .id = @constCast("doc:b") },
    };
    try std.testing.expect((try resolvedDocSetFromSearchHitOrdinalsAlloc(alloc, &mixed)) == null);
}

test "relational table full-text search loads stored_data from base rows" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    // Relational schema: returned documents and structured filters should read
    // the relational base row, not duplicated segment typed_doc_values.
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"datetime"},"active":{"type":"boolean"},"location":{"type":"geopoint"},"attrs":{"type":"json"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "row:a",
            .value =
            \\{"title":"hello world","status":"open","amount":42.5,"created_at":100,"active":true,"location":{"lat":0,"lon":0},"attrs":{"version":"old"}}
            ,
        }},
        .sync_level = .full_index,
    });

    const text_entry = db.core.textIndexEntry("ft_v1") orelse return error.TestExpectedEqual;
    const snapshot = text_entry.persistent.snapshot();
    try std.testing.expect(snapshot.segments.len > 0);
    for (snapshot.segments) |*segment| {
        try std.testing.expect(segment.reader.getSection("status", .typed_doc_values) == null);
        try std.testing.expect(segment.reader.getSection("amount", .typed_doc_values) == null);
        try std.testing.expect(segment.reader.getSection("created_at", .typed_doc_values) == null);
        try std.testing.expect(segment.reader.getSection("active", .typed_doc_values) == null);
        try std.testing.expect(segment.reader.getSection("location", .typed_doc_values) == null);
        try std.testing.expect(segment.reader.getSection("attrs", .typed_doc_values) == null);
    }

    const replacement_json =
        \\{"title":"base row wins","status":"closed","amount":77.25,"created_at":200,"active":false,"location":{"lat":37.78,"lon":-122.42},"attrs":{"version":"new","nested":{"ok":true}}}
    ;
    const replacement_row = try mapper.buildRelationalRowValueAlloc(alloc, replacement_json, runtime_schema.relational_columns);
    defer alloc.free(replacement_row);
    var replacement_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (replacement_writes.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        replacement_writes.deinit(alloc);
    }
    var replacement_deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (replacement_deletes.items) |key| alloc.free(@constCast(key));
        replacement_deletes.deinit(alloc);
    }
    const replacement_row_copy = try alloc.dupe(u8, replacement_row);
    var replacement_row_copy_owned = true;
    errdefer if (replacement_row_copy_owned) alloc.free(replacement_row_copy);
    try relational_store_mod.appendUpsertOwnedBatch(
        alloc,
        db.core.store,
        &replacement_writes,
        &replacement_deletes,
        "row:a",
        replacement_row_copy,
    );
    replacement_row_copy_owned = false;
    try db.core.store.putBatch(replacement_writes.items, replacement_deletes.items);

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .include_stored = true,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expect(result.hits[0].stored_data != null);

    // Search still matches the full-text segment, but stored_data comes from the
    // relational base row. If this path reads segment typed_doc_values, these
    // assertions see the original 42.5/true row instead.
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.hits[0].stored_data.?, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    try std.testing.expectEqualStrings("base row wins", obj.get("title").?.string);
    const amount = obj.get("amount").?;
    const amount_num: f64 = switch (amount) {
        .float => |f| f,
        .integer => |n| @floatFromInt(n),
        else => unreachable,
    };
    try std.testing.expectEqual(@as(f64, 77.25), amount_num);
    try std.testing.expectEqualStrings("closed", obj.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 200), obj.get("created_at").?.integer);
    try std.testing.expect(!obj.get("active").?.bool);
    try std.testing.expectEqualStrings("new", obj.get("attrs").?.object.get("version").?.string);
    try std.testing.expect(obj.get("attrs").?.object.get("nested").?.object.get("ok").?.bool);

    const aggregation_requests = [_]aggregations_mod.SearchAggregationRequest{
        .{ .name = "amount_sum", .type = "sum", .field = "amount" },
        .{ .name = "by_status", .type = "terms", .field = "status", .size = 5 },
    };
    const aggregation_results = try aggregations_mod.computeSearchAggregations(alloc, aggregation_requests[0..], result, .{});
    defer aggregations_mod.deinitResults(alloc, aggregation_results);
    try std.testing.expectEqual(@as(usize, 2), aggregation_results.len);
    try std.testing.expectEqualStrings("77.25", aggregation_results[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), aggregation_results[1].buckets.len);
    try std.testing.expectEqualStrings("\"closed\"", aggregation_results[1].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation_results[1].buckets[0].count);

    var filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .filter_query_json = "{\"numeric_range\":{\"field\":\"amount\",\"min\":70.0}}",
        .include_stored = true,
        .limit = 10,
    });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", filtered.hits[0].id);

    var top_level_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .numeric_range = .{ .field = "amount", .min = 70.0 } },
        .include_stored = true,
        .limit = 10,
    });
    defer top_level_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), top_level_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", top_level_filtered.hits[0].id);

    var bool_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .filter_query_json = "{\"bool_field\":{\"field\":\"active\",\"value\":false}}",
        .limit = 10,
    });
    defer bool_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), bool_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", bool_filtered.hits[0].id);

    var keyword_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"closed\"}}",
        .limit = 10,
    });
    defer keyword_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), keyword_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", keyword_filtered.hits[0].id);

    var stale_keyword_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"open\"}}",
        .limit = 10,
    });
    defer stale_keyword_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_keyword_filtered.total_hits);

    var term_range_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .filter_query_json = "{\"term_range\":{\"field\":\"status\",\"min\":\"cl\",\"max\":\"cm\"}}",
        .limit = 10,
    });
    defer term_range_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), term_range_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", term_range_filtered.hits[0].id);

    var date_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .date_range = .{ .field = "created_at", .start_ns = 150 } },
        .include_stored = true,
        .limit = 10,
    });
    defer date_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), date_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", date_filtered.hits[0].id);

    var geo_distance_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .filter_query_json = "{\"geo_distance\":{\"field\":\"location\",\"lat\":37.78,\"lon\":-122.42,\"radius_meters\":1000}}",
        .limit = 10,
    });
    defer geo_distance_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), geo_distance_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", geo_distance_filtered.hits[0].id);

    var geo_bbox_filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "hello" } },
        .filter_query_json = "{\"geo_bbox\":{\"field\":\"location\",\"min_lat\":37.0,\"min_lon\":-123.0,\"max_lat\":38.0,\"max_lon\":-122.0}}",
        .limit = 10,
    });
    defer geo_bbox_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), geo_bbox_filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", geo_bbox_filtered.hits[0].id);
}

test "relational column filter pushdown declines stale identity generations" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{.{ .key = "row:a", .value = "{\"title\":\"alpha\",\"status\":\"active\"}" }},
    });
    const active_generation = db.core.nextDerivedSequence();

    try db.batch(.{
        .writes = &.{.{ .key = "row:a", .value = "{\"title\":\"alpha\",\"status\":\"archived\"}" }},
    });
    const current_generation = db.core.nextDerivedSequence();

    const stale = try db.searchRuntimeResolveRelationalFilterQueryDocSetAlloc(alloc, runtime_schema, .{
        .term = .{ .field = "status", .term = "active" },
    }, active_generation);
    try std.testing.expect(stale == null);

    var current = (try db.searchRuntimeResolveRelationalFilterQueryDocSetAlloc(alloc, runtime_schema, .{
        .term = .{ .field = "status", .term = "archived" },
    }, current_generation)) orelse return error.TestExpectedEqual;
    defer current.deinit(alloc);
    const ids = (try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &current, current_generation)) orelse return error.TestExpectedEqual;
    defer {
        for (ids) |id| alloc.free(@constCast(id));
        alloc.free(ids);
    }
    try std.testing.expectEqual(@as(usize, 1), ids.len);
    try std.testing.expectEqualStrings("row:a", ids[0]);
}

test "relational unindexed column filters fall back to base rows" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric","x-antfly-index":false}},"required":["title","amount"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"title\":\"alpha\",\"amount\":42.5}" },
            .{ .key = "row:b", .value = "{\"title\":\"alpha\",\"amount\":7.0}" },
        },
        .sync_level = .full_index,
    });

    const amount_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:a");
    defer alloc.free(amount_index_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, amount_index_key));

    var filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .filter_query_json = "{\"numeric_range\":{\"field\":\"amount\",\"min\":40.0}}",
        .limit = 10,
    });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", filtered.hits[0].id);
}

test "relational indexed array_any filters use array element indexes" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"title\":\"alpha\",\"tags\":[\"hot\",\"new\"]}" },
            .{ .key = "row:b", .value = "{\"title\":\"alpha\",\"tags\":[\"cold\"]}" },
            .{ .key = "row:c", .value = "{\"title\":\"alpha\"}" },
        },
        .sync_level = .full_index,
    });

    var parsed_hot = try std.json.parseFromSlice(std.json.Value, alloc, "\"hot\"", .{});
    defer parsed_hot.deinit();
    const hot_key = try relational_store_mod.arrayElementIndexKeyForValueAlloc(alloc, parsed_hot.value);
    defer alloc.free(hot_key);
    const hot_index_key = try internal_keys.relationalArrayElementIndexKeyAlloc(alloc, "tags", hot_key, "row:a");
    defer alloc.free(hot_index_key);
    const hot_index_value = try db.core.store.get(alloc, hot_index_key);
    defer alloc.free(hot_index_value);
    const indexed_hot_ids = try relational_store_mod.scanArrayElementDocKeysAlloc(alloc, db.core.store, "tags", hot_key, "", "");
    defer relational_store_mod.freeDocKeys(alloc, indexed_hot_ids);
    try std.testing.expectEqual(@as(usize, 1), indexed_hot_ids.len);
    try std.testing.expectEqualStrings("row:a", indexed_hot_ids[0]);

    var direct_resolved = (try db.searchRuntimeResolveRelationalFilterQueryDocSetAlloc(alloc, runtime_schema, .{
        .array_any = .{ .field = "tags", .value = parsed_hot.value },
    }, null)) orelse return error.TestExpectedEqual;
    defer direct_resolved.deinit(alloc);
    const direct_ids = (try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &direct_resolved, null)) orelse return error.TestExpectedEqual;
    defer {
        for (direct_ids) |id| alloc.free(@constCast(id));
        alloc.free(direct_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), direct_ids.len);
    try std.testing.expectEqualStrings("row:a", direct_ids[0]);

    var unfiltered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .limit = 10,
    });
    defer unfiltered.deinit();
    try std.testing.expectEqual(@as(u32, 3), unfiltered.total_hits);

    var filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .filter_query_json = "{\"array_any\":{\"field\":\"tags\",\"value\":\"hot\"}}",
        .limit = 10,
    });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", filtered.hits[0].id);

    var missing = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .filter_query_json = "{\"array_any\":{\"field\":\"tags\",\"value\":\"missing\"}}",
        .limit = 10,
    });
    defer missing.deinit();
    try std.testing.expectEqual(@as(u32, 0), missing.total_hits);
}

test "relational indexed json_contains filters use json value indexes" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"attrs":{"type":"json"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "row:a", .value = "{\"title\":\"alpha\",\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\",\"beta\"]}}" },
            .{ .key = "row:b", .value = "{\"title\":\"alpha\",\"attrs\":{\"billing\":{\"plan\":\"free\"},\"flags\":[\"active\"]}}" },
            .{ .key = "row:c", .value = "{\"title\":\"alpha\",\"attrs\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"archived\"]}}" },
        },
        .sync_level = .full_index,
    });

    var parsed_pro = try std.json.parseFromSlice(std.json.Value, alloc, "\"pro\"", .{});
    defer parsed_pro.deinit();
    const pro_key = try relational_store_mod.jsonValueIndexKeyForValueAlloc(alloc, parsed_pro.value);
    defer alloc.free(pro_key);
    const pro_index_key = try internal_keys.relationalJsonValueIndexKeyAlloc(alloc, "attrs", "billing.plan", pro_key, "row:a");
    defer alloc.free(pro_index_key);
    const pro_index_value = try db.core.store.get(alloc, pro_index_key);
    defer alloc.free(pro_index_value);

    var wanted = try std.json.parseFromSlice(std.json.Value, alloc, "{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]}", .{});
    defer wanted.deinit();
    const indexed_ids = try relational_store_mod.scanJsonContainmentDocKeysAlloc(alloc, db.core.store, "attrs", wanted.value, "", "");
    defer relational_store_mod.freeDocKeys(alloc, indexed_ids);
    try std.testing.expectEqual(@as(usize, 1), indexed_ids.len);
    try std.testing.expectEqualStrings("row:a", indexed_ids[0]);

    var direct_resolved = (try db.searchRuntimeResolveRelationalFilterQueryDocSetAlloc(alloc, runtime_schema, .{
        .json_contains = .{ .field = "attrs", .value = wanted.value },
    }, null)) orelse return error.TestExpectedEqual;
    defer direct_resolved.deinit(alloc);
    const direct_ids = (try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &direct_resolved, null)) orelse return error.TestExpectedEqual;
    defer {
        for (direct_ids) |id| alloc.free(@constCast(id));
        alloc.free(direct_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), direct_ids.len);
    try std.testing.expectEqualStrings("row:a", direct_ids[0]);

    var filtered = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "alpha" } },
        .filter_query_json = "{\"json_contains\":{\"field\":\"attrs\",\"value\":{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]}}}",
        .limit = 10,
    });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
    try std.testing.expectEqualStrings("row:a", filtered.hits[0].id);
}

test "relational embedded json search intersects with top-level relational filters" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"status":{"type":"keyword"},"attrs":{"type":"json","schema":{"type":"object","properties":{"title":{"type":"text"},"plan":{"type":"keyword"}},"additionalProperties":true}}},"required":["status"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_json",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{
            .{
                .key = "row:active",
                .value = "{\"status\":\"active\",\"attrs\":{\"title\":\"nebula plan\",\"plan\":\"pro\",\"notes\":\"alpha note\"}}",
            },
            .{
                .key = "row:archived",
                .value = "{\"status\":\"archived\",\"attrs\":{\"title\":\"nebula archive\",\"plan\":\"free\",\"notes\":\"beta note\"}}",
            },
        },
        .sync_level = .full_index,
    });

    var active = try db.search(alloc, .{
        .index_name = "ft_json",
        .query = .{ .match = .{ .field = "attrs.title", .text = "nebula" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"active\"}}",
        .include_stored = true,
        .limit = 10,
    });
    defer active.deinit();
    try std.testing.expectEqual(@as(u32, 1), active.total_hits);
    try std.testing.expectEqualStrings("row:active", active.hits[0].id);
    try std.testing.expect(active.hits[0].stored_data != null);
    try std.testing.expect(std.mem.indexOf(u8, active.hits[0].stored_data.?, "\"attrs\":{\"title\":\"nebula plan\"") != null);

    var archived = try db.search(alloc, .{
        .index_name = "ft_json",
        .query = .{ .term = .{ .field = "attrs.plan", .term = "free" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"archived\"}}",
        .include_stored = true,
        .limit = 10,
    });
    defer archived.deinit();
    try std.testing.expectEqual(@as(u32, 1), archived.total_hits);
    try std.testing.expectEqualStrings("row:archived", archived.hits[0].id);

    var mismatched = try db.search(alloc, .{
        .index_name = "ft_json",
        .query = .{ .term = .{ .field = "attrs.plan", .term = "free" } },
        .filter_query_json = "{\"term\":{\"field\":\"status\",\"term\":\"active\"}}",
        .limit = 10,
    });
    defer mismatched.deinit();
    try std.testing.expectEqual(@as(u32, 0), mismatched.total_hits);
}

test "relational text backfill ignores generic primary rows" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{.{
            .key = "row:a",
            .value = "{\"title\":\"base row\",\"amount\":10}",
        }},
    });

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
    defer alloc.free(primary_key);
    try db.core.store.put(primary_key, "{\"title\":\"stale primary\",\"amount\":999}");

    try db.addIndex(.{
        .name = "ft_backfill",
        .kind = .full_text,
        .config_json = "{}",
    });

    var stale = try db.search(alloc, .{
        .index_name = "ft_backfill",
        .query = .{ .match = .{ .field = "title", .text = "stale" } },
        .include_stored = true,
    });
    defer stale.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale.total_hits);

    var base = try db.search(alloc, .{
        .index_name = "ft_backfill",
        .query = .{ .match = .{ .field = "title", .text = "base" } },
        .include_stored = true,
    });
    defer base.deinit();
    try std.testing.expectEqual(@as(u32, 1), base.total_hits);
    try std.testing.expect(base.hits[0].stored_data != null);
    try std.testing.expect(std.mem.indexOf(u8, base.hits[0].stored_data.?, "\"title\":\"base row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, base.hits[0].stored_data.?, "stale primary") == null);
}

test "relational derived replay reads base row keyspace" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_replay_relational",
        .kind = .full_text,
        .config_json = "{}",
    });

    const doc_json = "{\"title\":\"async replay row\",\"amount\":9.5}";
    const row_value = try mapper.buildRelationalRowValueAlloc(alloc, doc_json, runtime_schema.relational_columns);
    defer alloc.free(row_value);
    const relational_key = try relational_store_mod.rowKeyAlloc(alloc, "row:async");
    defer alloc.free(relational_key);
    try db.core.store.put(relational_key, row_value);

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:async");
    defer alloc.free(primary_key);
    const maybe_primary = db.core.store.get(alloc, primary_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (maybe_primary) |primary_value| {
        defer alloc.free(primary_value);
        return error.TestExpectedEqual;
    }

    const targets = [_]derived_types.DerivedTargetRef{.{
        .kind = .full_text,
        .index_name = "ft_replay_relational",
    }};
    const docs = [_]derived_types.DerivedDocument{.{
        .key = "row:async",
        .targets = &targets,
    }};
    const batch = derived_types.DerivedBatch{
        .sequence = 1,
        .documents = &docs,
    };
    var replay_ctx = db_internal.ReplayApplyContext(DB){ .db = &db };
    try std.testing.expect(try DB.derivedAsyncApplyDerivedBatchToIndexReplay(&replay_ctx, batch, .{
        .name = "ft_replay_relational",
        .kind = .full_text,
    }));

    var result = try db.search(alloc, .{
        .index_name = "ft_replay_relational",
        .query = .{ .match = .{ .field = "title", .text = "async" } },
        .include_stored = true,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("row:async", result.hits[0].id);
    try std.testing.expect(result.hits[0].stored_data != null);
    try std.testing.expect(std.mem.indexOf(u8, result.hits[0].stored_data.?, "\"amount\":9.5") != null);
}

fn benchQueryProfileEnabled() bool {
    return platform.env.getenv("ANTFLY_BENCH_QUERY_PROFILE") != null;
}

fn cloneGraphMetricStatusFromGraph(
    alloc: Allocator,
    source: graph_mod.GraphIndex.GraphMetricStatus,
) !types.GraphMetricStatus {
    const name = try alloc.dupe(u8, source.name);
    var name_moved = false;
    errdefer if (!name_moved) alloc.free(name);
    var edge_filter = try source.edge_filter.cloneAlloc(alloc);
    var edge_filter_moved = false;
    errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
    const recent_events = if (source.recent_events.len > 0)
        try alloc.dupe(graph_mod.GraphIndex.GraphMetricEvent, source.recent_events)
    else
        @constCast((&[_]graph_mod.GraphIndex.GraphMetricEvent{})[0..]);
    var recent_events_moved = false;
    errdefer if (!recent_events_moved and recent_events.len > 0) alloc.free(recent_events);
    const last_error = if (source.last_error.len > 0) try alloc.dupe(u8, source.last_error) else "";
    var last_error_moved = false;
    errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
    const build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    var build_worker_id_moved = false;
    errdefer if (!build_worker_id_moved and build_worker_id.len > 0) alloc.free(build_worker_id);
    const build_cursor = if (source.build_cursor.len > 0) try alloc.dupe(u8, source.build_cursor) else "";
    var build_cursor_moved = false;
    errdefer if (!build_cursor_moved and build_cursor.len > 0) alloc.free(build_cursor);
    const build_pages = try cloneGraphMetricBuildPageStatusesFromGraph(alloc, source.build_pages);
    var build_pages_moved = false;
    errdefer if (!build_pages_moved) {
        for (build_pages) |*page| page.deinit(alloc);
        if (build_pages.len > 0) alloc.free(build_pages);
    };
    const out = types.GraphMetricStatus{
        .name = name,
        .state = source.state,
        .phase = source.phase,
        .edge_filter = edge_filter,
        .metadata_version = source.metadata_version,
        .maintenance_paused = source.maintenance_paused,
        .build_queued = source.build_queued,
        .published_generation = source.published_generation,
        .edge_generation = source.edge_generation,
        .target_edge_generation = source.target_edge_generation,
        .queued_generation = source.queued_generation,
        .building_generation = source.building_generation,
        .build_job_id = source.build_job_id,
        .build_started_at_ms = source.build_started_at_ms,
        .build_iteration = source.build_iteration,
        .build_lease_expires_at_ms = source.build_lease_expires_at_ms,
        .build_worker_id = build_worker_id,
        .build_cursor = build_cursor,
        .build_completed_units = source.build_completed_units,
        .build_total_units = source.build_total_units,
        .build_pages = build_pages,
        .build_pages_truncated = source.build_pages_truncated,
        .retry_count = source.retry_count,
        .last_error = last_error,
        .progress = source.progress,
        .converged = source.converged,
        .iterations_completed = source.iterations_completed,
        .delta = source.delta,
        .computed_at_ms = source.computed_at_ms,
        .last_event = source.last_event,
        .recent_events = recent_events,
    };
    name_moved = true;
    edge_filter_moved = true;
    recent_events_moved = true;
    last_error_moved = true;
    build_worker_id_moved = true;
    build_cursor_moved = true;
    build_pages_moved = true;
    return out;
}

fn cloneGraphMetricBuildPageStatusesFromGraph(
    alloc: Allocator,
    source: []const graph_mod.GraphIndex.GraphMetricBuildPageStatus,
) ![]types.GraphMetricBuildPageStatus {
    if (source.len == 0) return @constCast((&[_]types.GraphMetricBuildPageStatus{})[0..]);
    const out = try alloc.alloc(types.GraphMetricBuildPageStatus, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*page| page.deinit(alloc);
        alloc.free(out);
    }
    for (source, 0..) |page, i| {
        const worker_id = if (page.worker_id.len > 0) try alloc.dupe(u8, page.worker_id) else "";
        var worker_id_moved = false;
        errdefer if (!worker_id_moved and worker_id.len > 0) alloc.free(worker_id);
        const cursor = if (page.cursor.len > 0) try alloc.dupe(u8, page.cursor) else "";
        var cursor_moved = false;
        errdefer if (!cursor_moved and cursor.len > 0) alloc.free(cursor);
        const last_error = if (page.last_error.len > 0) try alloc.dupe(u8, page.last_error) else "";
        var last_error_moved = false;
        errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
        out[i] = .{
            .phase = page.phase,
            .iteration = page.iteration,
            .page_id = page.page_id,
            .state = page.state,
            .range_kind = page.range_kind,
            .worker_id = worker_id,
            .lease_expires_at_ms = page.lease_expires_at_ms,
            .attempt = page.attempt,
            .cursor = cursor,
            .completed_units = page.completed_units,
            .total_units = page.total_units,
            .last_error = last_error,
        };
        worker_id_moved = true;
        cursor_moved = true;
        last_error_moved = true;
        initialized += 1;
    }
    return out;
}

pub fn Impl(comptime DB: type) type {
    return struct {
        const Self = @This();

        pub fn search(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try Self.searchWithExecutionContext(self, alloc, req, .{});
        }

        pub fn searchWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) !types.SearchResult {
            if (req.row_claim != null) {
                return try Self.searchWithRowClaim(self, alloc, req, exec_ctx);
            }
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var generation_ns: u64 = 0;
            var lock_wait_ns: u64 = 0;
            var locked_search_ns: u64 = 0;
            if (self.searchRuntimeCanUsePublishedDenseSearch(req)) {
                const generation_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
                const snapshot_req = try Self.searchRequestAtCurrentIdentityGeneration(self, req);
                if (bench_profile) generation_ns = platform_time.monotonicNs() - generation_start_ns;
                const search_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
                const result = try Self.searchLockedWithExecutionContext(self, alloc, snapshot_req, exec_ctx);
                if (bench_profile) {
                    locked_search_ns = platform_time.monotonicNs() - search_start_ns;
                    std.log.info(
                        "antfly_bench_db_search_wrapper total_us={d} generation_us={d} lock_wait_us={d} locked_search_us={d} published_dense={}",
                        .{ (platform_time.monotonicNs() - total_start_ns) / 1000, generation_ns / 1000, lock_wait_ns / 1000, locked_search_ns / 1000, true },
                    );
                }
                return result;
            }
            const lock_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            self.core.lockApplyShared();
            if (bench_profile) lock_wait_ns = platform_time.monotonicNs() - lock_start_ns;
            defer self.core.unlockApplyShared();
            const generation_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const snapshot_req = try Self.searchRequestAtCurrentIdentityGeneration(self, req);
            if (bench_profile) generation_ns = platform_time.monotonicNs() - generation_start_ns;
            const search_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const result = try Self.searchLockedWithExecutionContext(self, alloc, snapshot_req, exec_ctx);
            if (bench_profile) {
                locked_search_ns = platform_time.monotonicNs() - search_start_ns;
                std.log.info(
                    "antfly_bench_db_search_wrapper total_us={d} generation_us={d} lock_wait_us={d} locked_search_us={d} published_dense={}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, generation_ns / 1000, lock_wait_ns / 1000, locked_search_ns / 1000, false },
                );
            }
            return result;
        }

        fn searchLocked(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try Self.searchLockedWithExecutionContext(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), .{});
        }

        fn searchWithRowClaim(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) anyerror!types.SearchResult {
            const claim = req.row_claim orelse return error.InvalidQueryRequest;
            const txn_id = claim.txn_id orelse return error.InvalidQueryRequest;
            if (!claim.mode.usesDurableIntent()) return error.InvalidQueryRequest;
            if (req.count_only) return error.UnsupportedQueryRequest;
            if (req.return_mode != .parent) return error.UnsupportedQueryRequest;
            if (req.graph_queries.len > 0) return error.UnsupportedQueryRequest;
            if (req.graph_metric_queries.len > 0) return error.UnsupportedQueryRequest;

            var search_req = req;
            search_req.row_claim = null;
            var result = try Self.searchWithExecutionContext(self, alloc, search_req, exec_ctx);
            errdefer result.deinit();
            try self.searchRuntimeApplyRowClaimToSearchResult(&result, txn_id, claim);
            return result;
        }

        pub fn applyRowClaimToSearchResult(
            self: *DB,
            result: *types.SearchResult,
            txn_id: types.TxnId,
            claim: types.RowClaimRequest,
        ) !void {
            if (result.graph_results.len > 0) return error.UnsupportedQueryRequest;
            if (!claim.effectiveSkipLocked()) {
                var row_keys = std.ArrayListUnmanaged([]const u8).empty;
                defer row_keys.deinit(self.alloc);
                for (result.hits) |hit| try row_keys.append(self.alloc, hit.id);
                try self.claimRowsForTransaction(txn_id, row_keys.items, claim);
                return;
            }

            var kept = std.ArrayListUnmanaged(types.SearchHit).empty;
            errdefer {
                for (kept.items) |*hit| hit.deinit(result.alloc);
                kept.deinit(result.alloc);
            }
            for (result.hits) |hit| {
                if (try Self.tryClaimRowForTransaction(self, txn_id, hit.id, claim)) {
                    try kept.append(result.alloc, try hit.clone(result.alloc));
                }
            }
            const old_hits = result.hits;
            result.hits = &.{};
            result.total_hits = 0;
            for (old_hits) |*hit| hit.deinit(result.alloc);
            if (old_hits.len > 0) result.alloc.free(old_hits);
            result.hits = try kept.toOwnedSlice(result.alloc);
            result.total_hits = @intCast(result.hits.len);
            result.total_hits_relation = .exact;
        }

        pub fn resolveSearchHitsToDocSet(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            hits: []const types.SearchHit,
        ) !doc_set.ResolvedDocSet {
            if (try resolvedDocSetFromSearchHitOrdinalsAlloc(alloc, hits)) |resolved| {
                self.internalRecordResolvedDocSet(&resolved, false);
                return resolved;
            }
            var doc_ids = try alloc.alloc([]const u8, hits.len);
            defer alloc.free(doc_ids);
            for (hits, 0..) |hit, i| doc_ids[i] = hit.id;
            return try self.internalResolveDocSetForIdsNoLockAtGenerationAlloc(alloc, doc_ids, req.identity_read_generation);
        }

        pub fn resolveGraphNodesToDocSet(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            nodes: []const graph_query_mod.GraphResultNode,
        ) !doc_set.ResolvedDocSet {
            var doc_ids = try alloc.alloc([]const u8, nodes.len);
            defer alloc.free(doc_ids);
            for (nodes, 0..) |node, i| doc_ids[i] = node.key;
            return try self.internalResolveDocSetForIdsNoLockAtGenerationAlloc(alloc, doc_ids, req.identity_read_generation);
        }

        pub fn liveFilterDocSet(
            self: *DB,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) !doc_set.ResolvedDocSet {
            if (try self.internalAllDocsVisibleAtGeneration(generation)) {
                return try doc_set.cloneAlloc(alloc, set);
            }
            return try doc_identity.visibleFilteredDocSetFromStoreAlloc(alloc, self.core.store, set, generation);
        }

        pub fn denseOrdinalsForVectorIds(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            vector_ids: []const u64,
            generation: ?u64,
        ) ![]?doc_set.DocOrdinal {
            const ordinals = try self.core.index_manager.lookupDenseOrdinalsForVectorIdsAlloc(
                alloc,
                self.core.store,
                index_name,
                vector_ids,
            );
            errdefer alloc.free(ordinals);
            const all_visible = try self.internalAllDocsVisibleSummaryFast(generation);
            if (all_visible) return ordinals;

            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            for (ordinals) |*maybe_ordinal| {
                const ordinal = maybe_ordinal.* orelse continue;
                const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse {
                    maybe_ordinal.* = null;
                    continue;
                };
                const visible = if (generation) |at| state.isVisibleAt(at) else state.isLive();
                if (!visible) maybe_ordinal.* = null;
            }
            return ordinals;
        }

        pub fn lookupLiveDocOrdinal(
            self: *DB,
            alloc: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) !?doc_set.DocOrdinal {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try self.internalLookupLiveDocOrdinalNoLock(alloc, doc_id, generation);
        }

        pub fn loadChunkFieldValue(self: *DB, alloc: Allocator, doc_key: []const u8) !?std.json.Value {
            const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "chunk");
            defer alloc.free(prefix);

            const artifacts = try self.core.scanStorePrefix(alloc, prefix);
            defer docstore_mod.DocStore.freeResults(alloc, artifacts);

            var chunks_obj = std.json.ObjectMap.empty;
            errdefer {
                var it = chunks_obj.iterator();
                while (it.next()) |entry| {
                    alloc.free(entry.key_ptr.*);
                    freeJsonValue(alloc, entry.value_ptr);
                }
                chunks_obj.deinit(alloc);
            }

            var chunk_count: usize = 0;
            for (artifacts) |entry| {
                if (!internal_keys.isChunkArtifactRecordKey(entry.key)) continue;

                var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, entry.key)) orelse continue;
                defer artifact_ref.deinit(alloc);
                if (artifact_ref.kind != .chunk or artifact_ref.chunk_id == null) continue;

                var parsed = try std.json.parseFromSlice(std.json.Value, alloc, entry.value, .{});
                defer parsed.deinit();
                var cloned = try cloneJsonValue(alloc, parsed.value);
                errdefer freeJsonValue(alloc, &cloned);
                try db_query_projection.normalizeChunkArtifactForQuery(alloc, &cloned);

                const chunk_name = artifact_ref.name;
                if (chunks_obj.getPtr(chunk_name)) |existing| {
                    if (existing.* != .array) {
                        freeJsonValue(alloc, existing);
                        existing.* = .{ .array = std.json.Array.init(alloc) };
                    }
                    try existing.array.append(cloned);
                } else {
                    var arr = std.json.Array.init(alloc);
                    errdefer {
                        var mutable = cloned;
                        freeJsonValue(alloc, &mutable);
                        arr.deinit();
                    }
                    try arr.append(cloned);
                    try chunks_obj.put(alloc, try alloc.dupe(u8, chunk_name), .{ .array = arr });
                }

                chunk_count += 1;
            }

            if (chunk_count == 0) {
                var empty = std.json.Value{ .object = chunks_obj };
                freeJsonValue(alloc, &empty);
                return null;
            }
            return .{ .object = chunks_obj };
        }

        pub fn loadEmbeddingFieldValue(self: *DB, alloc: Allocator, doc_key: []const u8) !?std.json.Value {
            const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "embedding");
            defer alloc.free(prefix);

            const artifacts = try self.core.scanStorePrefix(alloc, prefix);
            defer docstore_mod.DocStore.freeResults(alloc, artifacts);

            var embeddings_obj = std.json.ObjectMap.empty;
            errdefer {
                var it = embeddings_obj.iterator();
                while (it.next()) |entry| {
                    alloc.free(entry.key_ptr.*);
                    freeJsonValue(alloc, entry.value_ptr);
                }
                embeddings_obj.deinit(alloc);
            }

            var embedding_count: usize = 0;
            for (artifacts) |entry| {
                if (!internal_keys.isInternalUserKey(entry.key)) continue;

                var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, entry.key)) orelse continue;
                defer artifact_ref.deinit(alloc);
                if (artifact_ref.kind != .embedding or artifact_ref.source != null) continue;

                var vector = enrichment_artifact_codec.decodeDenseEmbeddingJsonVectorAlloc(alloc, entry.value) catch continue;
                errdefer freeJsonValue(alloc, &vector);
                try putOwnedValue(alloc, &embeddings_obj, artifact_ref.name, vector);
                embedding_count += 1;
            }

            if (embedding_count == 0) {
                var empty = std.json.Value{ .object = embeddings_obj };
                freeJsonValue(alloc, &empty);
                return null;
            }
            return .{ .object = embeddings_obj };
        }

        pub fn loadArtifactFieldValue(self: *DB, alloc: Allocator, doc_key: []const u8) !?std.json.Value {
            const prefix = try internal_keys.artifactRootPrefixAlloc(alloc, doc_key);
            defer alloc.free(prefix);

            const artifacts = try self.core.scanStorePrefix(alloc, prefix);
            defer docstore_mod.DocStore.freeResults(alloc, artifacts);

            var artifacts_obj = std.json.ObjectMap.empty;
            errdefer {
                var it = artifacts_obj.iterator();
                while (it.next()) |entry| {
                    alloc.free(entry.key_ptr.*);
                    freeJsonValue(alloc, entry.value_ptr);
                }
                artifacts_obj.deinit(alloc);
            }

            var artifact_count: usize = 0;
            for (artifacts) |entry| {
                var artifact_ref = (try artifact_ids.decodeArtifactRefAlloc(alloc, entry.key)) orelse continue;
                defer artifact_ref.deinit(alloc);

                var artifact_value = try Self.artifactProjectionValue(self, alloc, artifact_ref, entry.value);
                errdefer freeJsonValue(alloc, &artifact_value);

                try appendArtifactProjectionValue(alloc, &artifacts_obj, artifact_ref.name, artifact_ref.kind, artifact_value);
                artifact_count += 1;
            }

            if (artifact_count == 0) {
                var empty = std.json.Value{ .object = artifacts_obj };
                freeJsonValue(alloc, &empty);
                return null;
            }
            return .{ .object = artifacts_obj };
        }

        fn tryClaimRowForTransaction(self: *DB, txn_id: types.TxnId, row_key: []const u8, claim: types.RowClaimRequest) !bool {
            self.claimRowsForTransaction(txn_id, &.{row_key}, claim) catch |err| switch (err) {
                transactions_mod.TxnError.IntentConflict => return false,
                else => return err,
            };
            return true;
        }

        fn artifactProjectionValue(self: *DB, alloc: Allocator, artifact_ref: types.ArtifactRef, raw: []const u8) !std.json.Value {
            var obj = std.json.ObjectMap.empty;
            errdefer {
                var value = std.json.Value{ .object = obj };
                freeJsonValue(alloc, &value);
            }

            {
                const artifact_id = try artifact_ids.artifactPublicIdAlloc(alloc, artifact_ref);
                errdefer alloc.free(artifact_id);
                try putOwnedValue(alloc, &obj, "artifact_id", .{ .string = artifact_id });
            }

            {
                var ref_value = try artifactRefJsonValue(alloc, artifact_ref);
                errdefer freeJsonValue(alloc, &ref_value);
                try putOwnedValue(alloc, &obj, "artifact_ref", ref_value);
            }

            try putOwnedValue(alloc, &obj, "kind", .{ .string = try alloc.dupe(u8, artifactKindText(artifact_ref.kind)) });
            const content_type = try Self.artifactContentTypeAlloc(self, alloc, artifact_ref.kind, artifact_ref.name);
            try putOwnedValue(alloc, &obj, "content_type", .{ .string = content_type });
            try putOwnedValue(alloc, &obj, "status", .{ .string = try alloc.dupe(u8, "ready") });

            if (enrichment_artifact_codec.sourceHash(raw) catch null) |source_hash| {
                try putOwnedValue(alloc, &obj, "source_hash", .{ .string = try std.fmt.allocPrint(alloc, "xxh64:{x}", .{source_hash}) });
            }

            switch (artifact_ref.kind) {
                .chunk => {
                    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch null;
                    if (parsed) |*owned| {
                        defer owned.deinit();
                        var cloned = try cloneJsonValue(alloc, owned.value);
                        errdefer freeJsonValue(alloc, &cloned);
                        try db_query_projection.normalizeChunkArtifactForQuery(alloc, &cloned);
                        try putOwnedValue(alloc, &obj, "value", cloned);
                    } else {
                        try putOwnedValue(alloc, &obj, "value", .{ .string = try alloc.dupe(u8, raw) });
                    }
                },
                .asset => {
                    try putOwnedValue(alloc, &obj, "value", try assetPayloadJsonValue(alloc, content_type, raw));
                },
                .embedding => {
                    if (enrichment_artifact_codec.decodeDenseEmbeddingDims(raw) catch null) |dims| {
                        try putOwnedValue(alloc, &obj, "dims", .{ .integer = @intCast(dims) });
                    }
                    try putOwnedValue(alloc, &obj, "value", .null);
                },
            }

            return .{ .object = obj };
        }

        fn artifactContentTypeAlloc(self: *DB, alloc: Allocator, kind: types.ArtifactKind, artifact_name: []const u8) ![]u8 {
            if (kind == .asset) {
                if (self.core.index_manager.getEnrichment(.asset, artifact_name)) |cfg| {
                    if (cfg.content_type.len > 0) return try alloc.dupe(u8, cfg.content_type);
                    return try alloc.dupe(u8, "text/plain");
                }
            }
            return try alloc.dupe(u8, artifactContentType(kind));
        }

        fn searchLockedWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) !types.SearchResult {
            const execution_req = directSingleVectorRequest(req) orelse req;
            if (execution_req.full_text_queries.len > 0 or execution_req.dense_queries.len > 0 or execution_req.sparse_queries.len > 0 or execution_req.merge_config != null) {
                var composed = try Self.searchComposed(self, alloc, execution_req, exec_ctx);
                errdefer composed.deinit();
                try Self.applyGraphMetricRerank(self, &composed, execution_req);
                try db_query_result_shape.externalizeSearchResultArtifactIds(alloc, &composed);
                return composed;
            }

            const has_primary = execution_req.full_text != null or execution_req.dense != null or execution_req.sparse != null or !db_query_search.isDefaultMatchAll(execution_req.query) or (execution_req.graph_queries.len == 0 and execution_req.graph_metric_queries.len == 0);

            var base = if (!has_primary and (execution_req.graph_queries.len > 0 or execution_req.graph_metric_queries.len > 0))
                try db_query_search.emptySearchResult(alloc)
            else if (execution_req.full_text) |text|
                try Self.searchTextQuery(self, alloc, execution_req, text)
            else if (execution_req.dense) |dense|
                try Self.searchDense(self, alloc, execution_req, dense)
            else if (execution_req.sparse) |sparse|
                try Self.searchSparse(self, alloc, execution_req, sparse)
            else switch (execution_req.query) {
                .match_none,
                .match_all,
                .phrase,
                .multi_phrase,
                .term,
                .fuzzy,
                .numeric_range,
                .date_range,
                .doc_id,
                .bool_field,
                .geo_distance,
                .geo_bbox,
                .term_range,
                .ip_range,
                .geo_shape,
                .match,
                .match_phrase,
                .prefix,
                .wildcard,
                .regexp,
                => try Self.searchText(self, alloc, execution_req),
                .dense_knn => |dense| try Self.searchDense(self, alloc, execution_req, dense),
                .sparse_knn => |sparse| try Self.searchSparse(self, alloc, execution_req, sparse),
                .graph => |graph| try Self.searchGraph(self, alloc, execution_req, graph, null),
            };
            errdefer base.deinit();

            if (execution_req.graph_metric_queries.len > 0) {
                base.graph_metric_results = try Self.executeGraphMetricQueries(self, alloc, execution_req.graph_metric_queries);
            }

            try Self.applyGraphMetricRerank(self, &base, execution_req);

            if (execution_req.graph_queries.len == 0) {
                try db_query_result_shape.externalizeSearchResultArtifactIds(alloc, &base);
                return base;
            }

            base.graph_results = try Self.executeGraphQueries(self, alloc, execution_req, execution_req.graph_queries, base.hits, base.total_hits);
            try Self.applyGraphExpandStrategy(self, alloc, &base, execution_req.expand_strategy);
            try db_query_result_shape.externalizeSearchResultArtifactIds(alloc, &base);
            return base;
        }

        fn directSingleVectorRequest(req: types.SearchRequest) ?types.SearchRequest {
            if (req.merge_config != null or req.reranker != null or req.pruner != null) return null;
            if (req.full_text_queries.len != 0) return null;
            if (req.full_text) |text| switch (text) {
                .match_all => {},
                else => return null,
            };
            if (!db_query_search.isDefaultMatchAll(req.query)) return null;
            if (req.graph_queries.len != 0 or req.graph_metric_queries.len != 0 or req.expand_strategy != null) return null;
            if (req.dense != null or req.sparse != null) return null;
            if (req.dense_queries.len == 1 and req.sparse_queries.len == 0) {
                var next = req;
                next.index_name = req.dense_queries[0].index_name;
                next.full_text = null;
                next.dense = req.dense_queries[0].query;
                next.dense_queries = &.{};
                return next;
            }
            if (req.sparse_queries.len == 1 and req.dense_queries.len == 0) {
                var next = req;
                next.index_name = req.sparse_queries[0].index_name;
                next.full_text = null;
                next.sparse = req.sparse_queries[0].query;
                next.sparse_queries = &.{};
                return next;
            }
            return null;
        }

        fn executeGraphMetricQueries(
            self: *DB,
            alloc: Allocator,
            queries: []const types.NamedGraphMetricQuery,
        ) ![]types.GraphMetricResult {
            if (queries.len == 0) return &.{};
            const results = try alloc.alloc(types.GraphMetricResult, queries.len);
            var initialized: usize = 0;
            errdefer {
                for (results[0..initialized]) |*result| result.deinit(alloc);
                alloc.free(results);
            }

            for (queries, 0..) |named, i| {
                results[i] = try executeGraphMetricQuery(self, alloc, named);
                initialized += 1;
            }
            return results;
        }

        fn applyGraphMetricRerank(
            self: *DB,
            result: *types.SearchResult,
            req: types.SearchRequest,
        ) !void {
            const rerank = req.graph_metric_rerank orelse return;
            if (req.count_only) return error.UnsupportedQueryRequest;

            const entry = self.core.graphIndex(rerank.index_name) orelse return error.IndexNotFound;
            var status = try entry.index.graphMetricStatus(rerank.metric_name);
            defer status.deinit(entry.index.alloc);

            if (status.published_generation == 0) return error.MetricNotReady;
            if (rerank.freshness == .fresh and status.state != .fresh) return error.MetricStale;

            var result_status = try cloneGraphMetricStatusFromGraph(result.alloc, status);
            var result_status_moved = false;
            errdefer if (!result_status_moved) result_status.deinit(result.alloc);

            for (result.hits) |*hit| {
                const metric_score_opt = try entry.index.graphMetricScore(rerank.metric_name, hit.id);
                const metric_score = metric_score_opt orelse rerank.missing_score;
                const base_score: f64 = if (hit.score) |score| @floatCast(score) else 0.0;
                const final_score = rerank.base_weight * base_score + rerank.weight * metric_score;
                const clamped_final_score = clampF64ToF32(final_score);
                const detail_index_name = try result.alloc.dupe(u8, rerank.index_name);
                errdefer result.alloc.free(detail_index_name);
                const detail_metric_name = try result.alloc.dupe(u8, rerank.metric_name);
                errdefer result.alloc.free(detail_metric_name);
                var score_details = types.GraphMetricRerankScoreDetails{
                    .index_name = detail_index_name,
                    .metric_name = detail_metric_name,
                    .base_score = base_score,
                    .base_weight = rerank.base_weight,
                    .metric_score = metric_score_opt,
                    .metric_score_used = metric_score,
                    .metric_weight = rerank.weight,
                    .missing_score_used = metric_score_opt == null,
                    .final_score = clamped_final_score,
                    .published_generation = status.published_generation,
                };
                var score_details_moved = false;
                errdefer if (!score_details_moved) score_details.deinit(result.alloc);

                if (hit.score_details) |*old_details| old_details.deinit(result.alloc);
                hit.score_details = score_details;
                score_details_moved = true;
                hit.score = clamped_final_score;
            }

            std.mem.sort(types.SearchHit, result.hits, {}, struct {
                fn lessThan(_: void, a: types.SearchHit, b: types.SearchHit) bool {
                    const a_score = a.score orelse 0.0;
                    const b_score = b.score orelse 0.0;
                    if (a_score == b_score) return std.mem.lessThan(u8, a.id, b.id);
                    return a_score > b_score;
                }
            }.lessThan);

            if (result.graph_metric_rerank_status) |*old_status| old_status.deinit(result.alloc);
            result.graph_metric_rerank_status = result_status;
            result_status_moved = true;
        }

        fn clampF64ToF32(value: f64) f32 {
            const max = std.math.floatMax(f32);
            if (value > max) return max;
            if (value < -max) return -max;
            return @floatCast(value);
        }

        fn executeGraphMetricQuery(
            self: *DB,
            alloc: Allocator,
            named: types.NamedGraphMetricQuery,
        ) !types.GraphMetricResult {
            const entry = self.core.graphIndex(named.query.index_name) orelse return error.IndexNotFound;
            var status = try entry.index.graphMetricStatus(named.query.metric_name);
            defer status.deinit(entry.index.alloc);

            if (status.published_generation == 0) return error.MetricNotReady;
            if (named.query.freshness == .fresh and status.state != .fresh) return error.MetricStale;

            const raw_scores = try entry.index.graphMetricTopK(named.query.metric_name, named.query.top_k);
            defer {
                for (raw_scores) |*score| score.deinit(entry.index.alloc);
                if (raw_scores.len > 0) entry.index.alloc.free(raw_scores);
            }

            const scores = try alloc.alloc(types.GraphMetricScore, raw_scores.len);
            var initialized_scores: usize = 0;
            errdefer {
                for (scores[0..initialized_scores]) |*score| score.deinit(alloc);
                alloc.free(scores);
            }
            for (raw_scores, 0..) |score, i| {
                scores[i] = .{
                    .node = try alloc.dupe(u8, score.node),
                    .score = score.score,
                };
                initialized_scores += 1;
            }

            return .{
                .name = try alloc.dupe(u8, named.name),
                .index_name = try alloc.dupe(u8, named.query.index_name),
                .metric_name = try alloc.dupe(u8, named.query.metric_name),
                .scores = scores,
                .status = try cloneGraphMetricStatusFromGraph(alloc, status),
            };
        }

        pub fn searchRequestWithTextAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest) !AlgebraicDocFilterRequest {
            const needs_algebraic_doc_filter = req.doc_filter_bindings.len > 0 or req.require_algebraic_filter_resolution;
            return if (needs_algebraic_doc_filter)
                try Self.searchRequestWithAlgebraicDocFilterAlloc(self, req)
            else
                AlgebraicDocFilterRequest{ .req = req };
        }

        pub fn searchRequestWithAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest) !AlgebraicDocFilterRequest {
            if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return .{ .req = req };
            if (!req.require_algebraic_filter_resolution and req.doc_filter_bindings.len == 0) {
                if (try Self.searchRequestWithDynamicStructuredDocFilterAlloc(self, req)) |direct| return direct;
            }
            const entry = self.core.index_manager.algebraicIndex(null) orelse {
                Self.recordUnsupportedDocSetFilterShape(self);
                if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                return .{ .req = req };
            };
            entry.index.recordVectorFilterAttempt();
            if (entry.index.hasErrors() or !entry.index.plannerLifecycleReady()) {
                entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
                Self.recordUnsupportedDocSetFilterShape(self);
                if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                return .{ .req = req };
            }
            if (try Self.searchRequestWithDirectAlgebraicDocFilterAlloc(self, req, &entry.index)) |direct| return direct;
            var filter_doc_ids: [][]u8 = &.{};
            errdefer entry.index.freeDocIds(filter_doc_ids);
            var exclude_doc_ids: [][]u8 = &.{};
            errdefer entry.index.freeDocIds(exclude_doc_ids);
            var changed = false;
            var filter_json_resolved = false;
            var exclusion_json_resolved = false;
            var filter_supported = req.filter_doc_ids_positive or req.filter_doc_ids.len > 0;
            var filter_bindings = std.ArrayListUnmanaged(AlgebraicIndex.FilterBinding).empty;
            defer {
                for (filter_bindings.items) |*binding| binding.set.deinit(&entry.index);
                filter_bindings.deinit(entry.index.alloc);
            }

            if (filter_supported) filter_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, req.filter_doc_ids);
            if (req.exclude_doc_ids.len > 0) exclude_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, req.exclude_doc_ids);

            for (req.doc_filter_bindings) |binding| {
                if (binding.name.len == 0 or binding.filter_query_json.len == 0) return error.InvalidArgument;
                for (filter_bindings.items) |existing| {
                    if (std.mem.eql(u8, existing.name, binding.name)) return error.InvalidArgument;
                }
                var set = (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(
                    self.core.store,
                    binding.filter_query_json,
                    filter_bindings.items,
                )) orelse {
                    entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
                    Self.recordUnsupportedDocSetFilterShape(self);
                    if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                    return .{ .req = req };
                };
                errdefer set.deinit(&entry.index);
                try filter_bindings.append(entry.index.alloc, .{
                    .name = binding.name,
                    .set = set,
                });
                set = .{};
            }

            if (req.filter_query_json.len > 0) {
                if (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(self.core.store, req.filter_query_json, filter_bindings.items)) |set| {
                    var owned_set = set;
                    defer owned_set.deinit(&entry.index);
                    if (owned_set.include) |ids| {
                        if (filter_supported) {
                            const intersected = try intersectAlgebraicDocIds(entry.index.alloc, filter_doc_ids, ids);
                            entry.index.freeDocIds(filter_doc_ids);
                            filter_doc_ids = intersected;
                        } else {
                            filter_doc_ids = try dupeAlgebraicDocIds(entry.index.alloc, ids);
                        }
                        filter_supported = true;
                        changed = true;
                    }
                    if (owned_set.exclude.len > 0) {
                        const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, owned_set.exclude);
                        entry.index.freeDocIds(exclude_doc_ids);
                        exclude_doc_ids = merged;
                        changed = true;
                    }
                    filter_json_resolved = true;
                }
            }
            if (req.exclusion_query_json.len > 0) {
                if (try entry.index.docIdSetForFilterJsonWithBindingsAlloc(self.core.store, req.exclusion_query_json, filter_bindings.items)) |set| {
                    var owned_set = set;
                    defer owned_set.deinit(&entry.index);
                    if (owned_set.include) |ids| {
                        const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, ids);
                        entry.index.freeDocIds(exclude_doc_ids);
                        exclude_doc_ids = merged;
                        changed = true;
                    }
                    if (owned_set.exclude.len > 0) {
                        const merged = try unionAlgebraicDocIds(entry.index.alloc, exclude_doc_ids, owned_set.exclude);
                        entry.index.freeDocIds(exclude_doc_ids);
                        exclude_doc_ids = merged;
                        changed = true;
                    }
                    exclusion_json_resolved = true;
                }
            }
            if (!changed) {
                entry.index.recordVectorFilterUnsupported(req.require_algebraic_filter_resolution);
                Self.recordUnsupportedDocSetFilterShape(self);
                if (req.require_algebraic_filter_resolution) return error.UnsupportedQueryRequest;
                return .{ .req = req };
            }

            var next = req;
            if (filter_supported) {
                next.filter_doc_ids = filter_doc_ids;
                next.filter_doc_ids_positive = true;
            }
            if (exclude_doc_ids.len > 0) next.exclude_doc_ids = exclude_doc_ids;
            if (filter_json_resolved) next.filter_query_json = "";
            if (exclusion_json_resolved) next.exclusion_query_json = "";
            if (filter_bindings.items.len > 0) next.doc_filter_bindings = &.{};
            if (next.require_algebraic_filter_resolution and (next.filter_query_json.len > 0 or next.exclusion_query_json.len > 0)) {
                entry.index.recordVectorFilterUnsupported(true);
                Self.recordUnsupportedDocSetFilterShape(self);
                return error.UnsupportedQueryRequest;
            }
            const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
            errdefer self.alloc.destroy(resolved_filter);
            resolved_filter.* = try Self.resolvedDocFilterForIdsAlloc(self, filter_supported, filter_doc_ids, exclude_doc_ids, req.identity_read_generation);
            errdefer resolved_filter.deinit(self.alloc);
            next.resolved_doc_filter = resolved_filter;
            entry.index.recordVectorFilterResolved(filter_doc_ids.len, exclude_doc_ids.len);
            return .{
                .req = next,
                .index = &entry.index,
                .filter_doc_ids = filter_doc_ids,
                .exclude_doc_ids = exclude_doc_ids,
                .resolved_doc_filter = resolved_filter,
                .resolved_doc_filter_alloc = self.alloc,
            };
        }

        fn searchRequestWithDynamicStructuredDocFilterAlloc(self: *DB, req: types.SearchRequest) !?AlgebraicDocFilterRequest {
            var resolved = (try db_query_search.resolveStructuredDocFilterForComposedAlloc(self.alloc, req, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .project_ordinals_to_doc_ids = false,
                .identity_read_generation = req.identity_read_generation,
            })) orelse return null;
            errdefer resolved.deinit(self.alloc);

            const resolved_filter = try self.alloc.create(doc_set.ResolvedDocFilter);
            errdefer self.alloc.destroy(resolved_filter);
            resolved_filter.* = resolved;
            resolved = .{};

            var next = req;
            next.resolved_doc_filter = resolved_filter;
            next.filter_query_json = "";
            next.exclusion_query_json = "";
            return .{
                .req = next,
                .resolved_doc_filter = resolved_filter,
                .resolved_doc_filter_alloc = self.alloc,
            };
        }

        fn searchRequestWithDirectAlgebraicDocFilterAlloc(self: *DB, req: types.SearchRequest, index: *AlgebraicIndex) !?AlgebraicDocFilterRequest {
            const algebraic_filter_rows_are_visible = true;
            var resolved_bindings = std.ArrayListUnmanaged(AlgebraicIndex.ResolvedFilterBinding).empty;
            defer {
                for (resolved_bindings.items) |*binding| binding.filter.deinit(index.alloc);
                resolved_bindings.deinit(index.alloc);
            }
            for (req.doc_filter_bindings) |binding| {
                if (binding.name.len == 0 or binding.filter_query_json.len == 0) return error.InvalidArgument;
                for (resolved_bindings.items) |existing| {
                    if (std.mem.eql(u8, existing.name, binding.name)) return error.InvalidArgument;
                }
                var binding_filter = (if (algebraic_filter_rows_are_visible)
                    try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                        self.core.store,
                        binding.filter_query_json,
                        resolved_bindings.items,
                    )
                else
                    try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                        self.core.store,
                        binding.filter_query_json,
                        req.identity_read_generation,
                        resolved_bindings.items,
                    )) orelse return null;
                errdefer binding_filter.deinit(index.alloc);
                try resolved_bindings.append(index.alloc, .{
                    .name = binding.name,
                    .filter = binding_filter,
                });
                binding_filter = .{};
            }

            var filter = doc_set.ResolvedDocFilter{};
            errdefer filter.deinit(index.alloc);
            var changed = false;
            var request_constraints_resolved = false;
            var filter_json_resolved = false;
            var exclusion_json_resolved = false;

            if (try Self.resolvedDocFilterForRequestNativeConstraintsAlloc(self, index.alloc, req)) |initial| {
                filter = initial;
                changed = true;
                request_constraints_resolved = true;
            }

            if (req.filter_query_json.len > 0) {
                var query_filter = (if (algebraic_filter_rows_are_visible)
                    try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                        self.core.store,
                        req.filter_query_json,
                        resolved_bindings.items,
                    )
                else if (resolved_bindings.items.len > 0)
                    try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                        self.core.store,
                        req.filter_query_json,
                        req.identity_read_generation,
                        resolved_bindings.items,
                    )
                else
                    try index.resolvedDocFilterForFilterJsonAtGenerationAlloc(self.core.store, req.filter_query_json, req.identity_read_generation)) orelse {
                    filter.deinit(index.alloc);
                    return null;
                };
                defer query_filter.deinit(index.alloc);
                if (!(try applyResolvedFilterIncludeAlloc(index.alloc, &filter, &query_filter))) {
                    filter.deinit(index.alloc);
                    return null;
                }
                changed = true;
                filter_json_resolved = true;
            }
            if (req.exclusion_query_json.len > 0) {
                var exclusion = (if (algebraic_filter_rows_are_visible)
                    try index.resolvedDocFilterForFilterJsonUncheckedAlloc(
                        self.core.store,
                        req.exclusion_query_json,
                        resolved_bindings.items,
                    )
                else if (resolved_bindings.items.len > 0)
                    try index.resolvedDocFilterForFilterJsonWithBindingsAtGenerationAlloc(
                        self.core.store,
                        req.exclusion_query_json,
                        req.identity_read_generation,
                        resolved_bindings.items,
                    )
                else
                    try index.resolvedDocFilterForFilterJsonAtGenerationAlloc(self.core.store, req.exclusion_query_json, req.identity_read_generation)) orelse {
                    filter.deinit(index.alloc);
                    return null;
                };
                defer exclusion.deinit(index.alloc);

                if (!(try unionResolvedFilterExcludeAlloc(index.alloc, &filter, &exclusion.include))) {
                    filter.deinit(index.alloc);
                    return null;
                }
                if (!(try unionResolvedFilterExcludeAlloc(index.alloc, &filter, &exclusion.exclude))) {
                    filter.deinit(index.alloc);
                    return null;
                }
                changed = true;
                exclusion_json_resolved = true;
            }
            if (!changed) return null;

            const resolved_filter = try index.alloc.create(doc_set.ResolvedDocFilter);
            errdefer index.alloc.destroy(resolved_filter);
            resolved_filter.* = filter;
            filter = .{};

            var next = req;
            if (request_constraints_resolved) {
                next.filter_doc_ids = &.{};
                next.filter_doc_ids_positive = false;
                next.exclude_doc_ids = &.{};
            }
            if (filter_json_resolved) next.filter_query_json = "";
            if (exclusion_json_resolved) next.exclusion_query_json = "";
            if (resolved_bindings.items.len > 0) next.doc_filter_bindings = &.{};
            next.resolved_doc_filter = resolved_filter;

            index.recordVectorFilterResolved(
                resolvedDocSetStatCount(&resolved_filter.include),
                resolvedDocSetStatCount(&resolved_filter.exclude),
            );
            return .{
                .req = next,
                .resolved_doc_filter = resolved_filter,
                .resolved_doc_filter_alloc = index.alloc,
            };
        }

        pub fn resolvedDocFilterForRequestNativeConstraintsAlloc(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
        ) !?doc_set.ResolvedDocFilter {
            var filter = doc_set.ResolvedDocFilter{};
            errdefer filter.deinit(alloc);
            var changed = false;

            if (req.resolved_doc_filter) |ptr| {
                const existing: *const doc_set.ResolvedDocFilter = @ptrCast(@alignCast(ptr));
                filter = try Self.normalizeResolvedDocFilterDocKeysNoLockAtGenerationAlloc(self, alloc, existing, req.identity_read_generation);
                changed = true;
            }

            if (req.filter_doc_ids_positive or req.filter_doc_ids.len != 0) {
                var include = if (req.filter_doc_ids.len == 0)
                    doc_set.ResolvedDocSet.none
                else
                    try Self.resolveDocIdsToDocSet(self, alloc, req.filter_doc_ids, req.identity_read_generation);
                defer include.deinit(alloc);
                if (!(try intersectResolvedFilterIncludeAlloc(alloc, &filter, &include))) {
                    filter.deinit(alloc);
                    return null;
                }
                changed = true;
            }

            if (req.exclude_doc_ids.len != 0) {
                var exclude = try Self.resolveDocIdsToDocSet(self, alloc, req.exclude_doc_ids, req.identity_read_generation);
                defer exclude.deinit(alloc);
                if (!(try unionResolvedFilterExcludeAlloc(alloc, &filter, &exclude))) {
                    filter.deinit(alloc);
                    return null;
                }
                changed = true;
            }

            if (!changed) return null;
            return filter;
        }

        fn normalizeResolvedDocFilterDocKeysNoLockAtGenerationAlloc(
            self: *DB,
            alloc: Allocator,
            filter: *const doc_set.ResolvedDocFilter,
            generation: ?u64,
        ) !doc_set.ResolvedDocFilter {
            var out = doc_set.ResolvedDocFilter{
                .include = try Self.normalizeResolvedDocSetDocKeysNoLockAtGenerationAlloc(self, alloc, &filter.include, generation),
            };
            errdefer out.deinit(alloc);
            out.exclude = try Self.normalizeResolvedDocSetDocKeysNoLockAtGenerationAlloc(self, alloc, &filter.exclude, generation);
            return out;
        }

        fn normalizeResolvedDocSetDocKeysNoLockAtGenerationAlloc(
            self: *DB,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) !doc_set.ResolvedDocSet {
            return switch (set.*) {
                .doc_keys => |keys| try Self.resolveDocIdsToDocSet(self, alloc, keys, generation),
                else => try doc_set.cloneAlloc(alloc, set),
            };
        }

        fn applyResolvedFilterIncludeAlloc(
            alloc: Allocator,
            target: *doc_set.ResolvedDocFilter,
            include_filter: *const doc_set.ResolvedDocFilter,
        ) !bool {
            var merged_include = (try doc_set.intersectAlloc(alloc, &target.include, &include_filter.include)) orelse return false;
            errdefer merged_include.deinit(alloc);
            var merged_exclude = (try doc_set.unionAlloc(alloc, &target.exclude, &include_filter.exclude)) orelse return false;
            errdefer merged_exclude.deinit(alloc);

            target.include.deinit(alloc);
            target.exclude.deinit(alloc);
            target.include = merged_include;
            target.exclude = merged_exclude;
            return true;
        }

        fn intersectResolvedFilterIncludeAlloc(
            alloc: Allocator,
            target: *doc_set.ResolvedDocFilter,
            include: *const doc_set.ResolvedDocSet,
        ) !bool {
            var merged = (try doc_set.intersectAlloc(alloc, &target.include, include)) orelse return false;
            errdefer merged.deinit(alloc);
            target.include.deinit(alloc);
            target.include = merged;
            return true;
        }

        fn unionResolvedFilterExcludeAlloc(
            alloc: Allocator,
            target: *doc_set.ResolvedDocFilter,
            exclude: *const doc_set.ResolvedDocSet,
        ) !bool {
            var merged = (try doc_set.unionAlloc(alloc, &target.exclude, exclude)) orelse return false;
            errdefer merged.deinit(alloc);
            target.exclude.deinit(alloc);
            target.exclude = merged;
            return true;
        }

        fn resolvedDocSetStatCount(set: *const doc_set.ResolvedDocSet) usize {
            return set.estimatedCardinality() orelse 0;
        }

        fn unionResolvedDocSetsAlloc(
            alloc: Allocator,
            left: *const doc_set.ResolvedDocSet,
            right: *const doc_set.ResolvedDocSet,
        ) !?doc_set.ResolvedDocSet {
            return switch (left.*) {
                .all => .all,
                .none => try doc_set.cloneAlloc(alloc, right),
                .doc_keys => |left_keys| switch (right.*) {
                    .all => .all,
                    .none => try doc_set.cloneAlloc(alloc, left),
                    .doc_keys => |right_keys| .{ .doc_keys = try unionAlgebraicDocIds(alloc, left_keys, right_keys) },
                    .ordinals, .ordinal_bitmap => null,
                },
                .ordinals, .ordinal_bitmap => switch (right.*) {
                    .all => .all,
                    .none => try doc_set.cloneAlloc(alloc, left),
                    .doc_keys => null,
                    .ordinals, .ordinal_bitmap => try unionOrdinalDocSetsAlloc(alloc, left, right),
                },
            };
        }

        fn unionOrdinalDocSetsAlloc(
            alloc: Allocator,
            left: *const doc_set.ResolvedDocSet,
            right: *const doc_set.ResolvedDocSet,
        ) !doc_set.ResolvedDocSet {
            var ordinals = std.ArrayListUnmanaged(doc_set.DocOrdinal).empty;
            defer ordinals.deinit(alloc);
            try appendResolvedDocSetOrdinalsAlloc(alloc, &ordinals, left);
            try appendResolvedDocSetOrdinalsAlloc(alloc, &ordinals, right);
            return try doc_set.fromOrdinalsAlloc(alloc, ordinals.items);
        }

        fn appendResolvedDocSetOrdinalsAlloc(
            alloc: Allocator,
            out: *std.ArrayListUnmanaged(doc_set.DocOrdinal),
            set: *const doc_set.ResolvedDocSet,
        ) !void {
            switch (set.*) {
                .ordinals => |ordinals| try out.appendSlice(alloc, ordinals),
                .ordinal_bitmap => |*bitmap| {
                    var iter = bitmap.iterator();
                    while (iter.next()) |ordinal| try out.append(alloc, ordinal);
                },
                .all, .none, .doc_keys => {},
            }
        }

        fn dupeAlgebraicDocIds(alloc: Allocator, doc_ids: []const []const u8) ![][]u8 {
            var out = try alloc.alloc([]u8, doc_ids.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |item| alloc.free(item);
                if (out.len > 0) alloc.free(out);
            }
            for (doc_ids, 0..) |doc_id, i| {
                out[i] = try alloc.dupe(u8, doc_id);
                initialized += 1;
            }
            return out;
        }

        fn containsAlgebraicDocId(doc_ids: []const []const u8, candidate: []const u8) bool {
            for (doc_ids) |doc_id| {
                if (std.mem.eql(u8, doc_id, candidate)) return true;
            }
            return false;
        }

        fn intersectAlgebraicDocIds(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![][]u8 {
            var out = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (out.items) |item| alloc.free(item);
                out.deinit(alloc);
            }
            for (left) |doc_id| {
                if (!containsAlgebraicDocId(right, doc_id)) continue;
                try out.append(alloc, try alloc.dupe(u8, doc_id));
            }
            return try out.toOwnedSlice(alloc);
        }

        fn unionAlgebraicDocIds(alloc: Allocator, left: []const []const u8, right: []const []const u8) ![][]u8 {
            var out = std.ArrayListUnmanaged([]u8).empty;
            errdefer {
                for (out.items) |item| alloc.free(item);
                out.deinit(alloc);
            }
            for (left) |doc_id| try out.append(alloc, try alloc.dupe(u8, doc_id));
            for (right) |doc_id| {
                if (containsAlgebraicDocId(out.items, doc_id)) continue;
                try out.append(alloc, try alloc.dupe(u8, doc_id));
            }
            return try out.toOwnedSlice(alloc);
        }

        fn searchComposed(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            exec_ctx: types.ExecutionContext,
        ) !types.SearchResult {
            _ = exec_ctx;
            return try db_query_search.searchComposed(alloc, req, .{
                .ctx = self,
                .resolve_structured_doc_filter = Self.resolveStructuredDocFilterForComposedCallback,
                .resolve_structured_text_doc_filter = Self.resolveStructuredTextDocFilterForComposedCallback,
                .search_text_query = Self.searchTextQueryCallback,
                .search_text = Self.searchTextComposedCallback,
                .search_dense = Self.searchDenseComposedCallback,
                .search_sparse = Self.searchSparseComposedCallback,
                .clone_named_set = Self.cloneNamedSetCallback,
                .fuse_named_sets = Self.fuseNamedSetsCallback,
                .resolve_hits_to_doc_set = Self.resolveSearchHitsToDocSetCallback,
                .attach_graph_results = Self.attachGraphResultsCallback,
            });
        }

        fn searchText(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try db_query_search.searchText(alloc, req, .{
                .ctx = self,
                .func = Self.searchTextQueryCallback,
            });
        }

        fn searchMatchAll(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
            return try db_query_search.searchMatchAll(alloc, req, .{
                .ctx = self,
                .collect_candidates = Self.collectSearchMatchAllCandidatesCallback,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .load_projected_document = Self.loadRequiredProjectedSearchDocumentCallback,
                .load_stored = Self.loadStoredSearchDocumentCallback,
                .load_many_stored = Self.loadStoredSearchDocumentManyCallback,
            });
        }

        fn searchGraph(self: *DB, alloc: Allocator, req: types.SearchRequest, graph_query: graph_query_mod.GraphQuery, base_hits: ?[]const types.SearchHit) !types.SearchResult {
            _ = req.index_name;
            var raw = try db_query_graph.executeSearchGraph(alloc, req, graph_query, base_hits, .{
                .ctx = self,
                .execute_graph_query = Self.executeSearchGraphQueryCallback,
                .load_projected_document = Self.loadProjectedSearchDocumentCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
            });
            errdefer raw.deinit();
            try Self.annotateSearchHitOrdinalsNoLock(self, alloc, req, raw.hits);
            return try Self.filterExpiredSearchResult(self, alloc, raw);
        }

        fn fuseNamedSets(self: *DB, alloc: Allocator, req: types.SearchRequest, named_sets: []const NamedResultSet) !types.SearchResult {
            const raw = try db_query_graph.fuseNamedSets(alloc, req, named_sets, .{
                .ctx = self,
                .load_projected_document = Self.loadProjectedSearchDocumentCallback,
            });
            return try Self.filterExpiredSearchResult(self, alloc, raw);
        }

        fn executeGraphQueries(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            graph_queries: []const types.NamedGraphQuery,
            base_hits: []const types.SearchHit,
            base_total_hits: u32,
        ) ![]types.GraphSearchResult {
            return try db_query_graph.executeGraphQueries(alloc, req, graph_queries, base_hits, base_total_hits, .{
                .ctx = self,
                .func = Self.executeSingleGraphQueryWithSetsCallback,
                .resolve_hits_to_doc_set = Self.resolveSearchHitsToDocSetCallback,
                .resolve_nodes_to_doc_set = Self.resolveGraphNodesToDocSetCallback,
            });
        }

        pub fn executeGraphQueriesWithSets(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            graph_queries: []const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) ![]types.GraphSearchResult {
            return try db_query_graph.executeGraphQueriesWithSets(alloc, req, graph_queries, named_sets, .{
                .ctx = self,
                .func = Self.executeSingleGraphQueryWithSetsCallback,
                .resolve_hits_to_doc_set = Self.resolveSearchHitsToDocSetCallback,
                .resolve_nodes_to_doc_set = Self.resolveGraphNodesToDocSetCallback,
            });
        }

        fn executeSingleGraphQueryWithSets(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) !types.GraphSearchResult {
            var result = switch (named.query.query_type) {
                .pattern => try Self.executeSinglePatternQueryWithSets(self, alloc, req, named, named_sets),
                else => try db_query_graph.executeSingleNonPatternQueryWithSets(alloc, req, named, named_sets, .{
                    .ctx = self,
                    .find_shortest_path = Self.executeShortestPathCallback,
                    .find_k_shortest_paths = Self.executeKShortestPathsCallback,
                    .execute_graph_query = Self.executeGraphQueryCallback,
                    .load_projected_document = Self.loadProjectedSearchDocumentCallback,
                    .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsForGraphCallback,
                    .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
                }),
            };
            errdefer result.deinit(alloc);
            try Self.annotateSearchHitOrdinalsNoLock(self, alloc, req, result.hits);
            return result;
        }

        fn executeSinglePatternQueryWithSets(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) !types.GraphSearchResult {
            return try db_query_graph.executeSinglePatternQueryWithSets(alloc, req, named, named_sets, .{
                .ctx = self,
                .match_pattern = Self.executePatternMatchCallback,
                .load_projected_document = Self.loadPatternProjectedDocumentCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsForGraphCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
            });
        }

        fn searchTextQuery(self: *DB, alloc: Allocator, req: types.SearchRequest, text_query: types.TextQuery) !types.SearchResult {
            var algebraic_filter = try self.searchRuntimeSearchRequestWithTextAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            try Self.proveTextQueryAccessPaths(self, algebraic_filter.req.index_name, text_query);
            const metric_name = Self.textQueryMetricIndexName(self, algebraic_filter.req);
            const start_ns = platform_time.monotonicNs();
            defer db_query_metrics.observe(metric_name, .search, platform_time.monotonicNs() -| start_ns);
            return try db_query_search.searchTextQuery(alloc, algebraic_filter.req, text_query, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .text_index_is_chunk_backed = Self.textIndexIsChunkBackedCallback,
                .search_match_all = Self.searchMatchAllCallback,
                .project_stored_search = Self.projectStoredBytesForSearchCallback,
                .load_projected_document = Self.loadProjectedSearchDocumentCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .postprocess = Self.postprocessTextSearchResultCallback,
            });
        }

        fn proveTextQueryAccessPaths(self: *DB, index_name: ?[]const u8, text_query: types.TextQuery) !void {
            switch (text_query) {
                .term => |term| try Self.proveTextFieldAccessPath(self, index_name, term.field, null, .slice),
                .match => |match| try Self.proveTextFieldAccessPath(self, index_name, match.field, match.analyzer, .slice),
                .multi_match_bool_prefix => |multi_match| {
                    for (multi_match.fields) |field| try Self.proveMultiMatchBoolPrefixAccessPaths(self, index_name, field.field);
                },
                .prefix => |prefix| try Self.proveTextFieldAccessPath(self, index_name, prefix.field, null, .automaton_select),
                .wildcard => |wildcard| try Self.proveTextFieldAccessPath(self, index_name, wildcard.field, null, .automaton_select),
                .regexp => |regexp| try Self.proveTextFieldAccessPath(self, index_name, regexp.field, null, .automaton_select),
                .fuzzy => |fuzzy| try Self.proveTextFieldAccessPath(self, index_name, fuzzy.field, null, .automaton_select),
                .bool_query => |bool_query| {
                    for (bool_query.must) |child| try Self.proveTextQueryAccessPaths(self, index_name, child);
                    for (bool_query.should) |child| try Self.proveTextQueryAccessPaths(self, index_name, child);
                    for (bool_query.must_not) |child| try Self.proveTextQueryAccessPaths(self, index_name, child);
                },
                else => {},
            }
        }

        fn proveTextFieldAccessPath(self: *DB, index_name: ?[]const u8, field: []const u8, analyzer: ?[]const u8, fragment: algebraic_mod.ir.TensorFragment) !void {
            _ = try self.core.index_manager.planFullTextLexicalAccessPathAlloc(self.alloc, index_name, field, analyzer, fragment) orelse return error.IndexNotFound;
        }

        fn proveOptionalTextFieldAccessPath(self: *DB, index_name: ?[]const u8, field: []const u8, fragment: algebraic_mod.ir.TensorFragment) !bool {
            const plan = try self.core.index_manager.planFullTextLexicalAccessPathAlloc(self.alloc, index_name, field, null, fragment);
            return plan != null;
        }

        fn proveMultiMatchBoolPrefixAccessPaths(self: *DB, index_name: ?[]const u8, field: []const u8) !void {
            if (std.mem.endsWith(u8, field, "._index_prefix")) {
                try Self.proveTextFieldAccessPath(self, index_name, field, null, .slice);
                return;
            }

            try Self.proveTextFieldAccessPath(self, index_name, field, null, .slice);
            try Self.proveTextFieldAccessPath(self, index_name, field, null, .automaton_select);
            if (isSearchAsYouTypeGeneratedFieldName(field)) return;

            const two_gram = try std.fmt.allocPrint(self.alloc, "{s}._2gram", .{field});
            defer self.alloc.free(two_gram);
            const has_two_gram = try Self.proveOptionalTextFieldAccessPath(self, index_name, two_gram, .slice);
            if (has_two_gram) _ = try Self.proveOptionalTextFieldAccessPath(self, index_name, two_gram, .automaton_select);

            const three_gram = try std.fmt.allocPrint(self.alloc, "{s}._3gram", .{field});
            defer self.alloc.free(three_gram);
            const has_three_gram = try Self.proveOptionalTextFieldAccessPath(self, index_name, three_gram, .slice);
            if (has_three_gram) _ = try Self.proveOptionalTextFieldAccessPath(self, index_name, three_gram, .automaton_select);

            const index_prefix = try std.fmt.allocPrint(self.alloc, "{s}._index_prefix", .{field});
            defer self.alloc.free(index_prefix);
            _ = try Self.proveOptionalTextFieldAccessPath(self, index_name, index_prefix, .slice);
        }

        fn isSearchAsYouTypeGeneratedFieldName(field: []const u8) bool {
            return std.mem.endsWith(u8, field, "._2gram") or
                std.mem.endsWith(u8, field, "._3gram") or
                std.mem.endsWith(u8, field, "._index_prefix");
        }

        fn textQueryMetricIndexName(self: *DB, req: types.SearchRequest) ?[]const u8 {
            if (req.index_name) |name| return name;
            const entry = self.core.textIndexEntry(null) orelse return null;
            return entry.config.name;
        }

        fn searchDense(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !types.SearchResult {
            if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
            const metric_name = Self.denseQueryMetricIndexName(self, req);
            const start_ns = platform_time.monotonicNs();
            defer db_query_metrics.observe(metric_name, .vector, platform_time.monotonicNs() -| start_ns);
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_ns: u64 = 0;
            var prove_ns: u64 = 0;
            var inner_ns: u64 = 0;
            const algebraic_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_filter = try self.searchRuntimeSearchRequestWithAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            if (bench_profile) algebraic_ns = platform_time.monotonicNs() - algebraic_start_ns;
            const prove_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            try Self.proveVectorSearchAccessPath(self, algebraic_filter.req.index_name, .dense_vector, hasNativeDocIdConstraints(algebraic_filter.req));
            if (bench_profile) prove_ns = platform_time.monotonicNs() - prove_start_ns;
            const inner_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const result = try db_query_search.searchDense(alloc, algebraic_filter.req, dense, Self.denseSearchExecutor(self));
            if (bench_profile) {
                inner_ns = platform_time.monotonicNs() - inner_start_ns;
                std.log.info(
                    "antfly_bench_db_dense_wrapper total_us={d} algebraic_us={d} prove_us={d} inner_us={d}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, algebraic_ns / 1000, prove_ns / 1000, inner_ns / 1000 },
                );
            }
            return result;
        }

        fn searchSparse(self: *DB, alloc: Allocator, req: types.SearchRequest, sparse: types.SparseKnnQuery) !types.SearchResult {
            if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
            const metric_name = Self.sparseQueryMetricIndexName(self, req);
            const start_ns = platform_time.monotonicNs();
            defer db_query_metrics.observe(metric_name, .vector, platform_time.monotonicNs() -| start_ns);
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_ns: u64 = 0;
            var prove_ns: u64 = 0;
            var inner_ns: u64 = 0;
            const algebraic_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var algebraic_filter = try self.searchRuntimeSearchRequestWithAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            if (bench_profile) algebraic_ns = platform_time.monotonicNs() - algebraic_start_ns;
            const prove_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            try Self.proveVectorSearchAccessPath(self, algebraic_filter.req.index_name, .sparse_vector, hasNativeDocIdConstraints(algebraic_filter.req));
            if (bench_profile) prove_ns = platform_time.monotonicNs() - prove_start_ns;
            const inner_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const result = try db_query_search.searchSparse(alloc, algebraic_filter.req, sparse, Self.sparseSearchExecutor(self));
            if (bench_profile) {
                inner_ns = platform_time.monotonicNs() - inner_start_ns;
                std.log.info(
                    "antfly_bench_db_sparse_wrapper total_us={d} algebraic_us={d} prove_us={d} inner_us={d}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, algebraic_ns / 1000, prove_ns / 1000, inner_ns / 1000 },
                );
            }
            return result;
        }

        fn searchDenseProfiledAtSnapshot(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !db_query_search.ProfiledDenseSearchResult {
            if (builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
            var algebraic_filter = try self.searchRuntimeSearchRequestWithAlgebraicDocFilterAlloc(req);
            defer algebraic_filter.deinit();
            try Self.proveVectorSearchAccessPath(self, algebraic_filter.req.index_name, .dense_vector, hasNativeDocIdConstraints(algebraic_filter.req));
            const profiled = db_query_search.searchDenseProfiled(alloc, algebraic_filter.req, dense, Self.denseSearchExecutor(self));
            return profiled catch |err| {
                if (err == error.IndexNotFound) {
                    const index_configs = self.listIndexes(alloc) catch |list_err| {
                        std.log.err("dense profiled search missing index requested={s} list_err={s}", .{
                            req.index_name orelse "<null>",
                            @errorName(list_err),
                        });
                        return err;
                    };
                    defer types.freeIndexConfigs(alloc, index_configs);
                    std.log.err("dense profiled search missing index requested={s} configured_index_count={d}", .{
                        req.index_name orelse "<null>",
                        index_configs.len,
                    });
                    for (index_configs) |cfg| {
                        std.log.err("dense profiled search visible index name={s} kind={s}", .{
                            cfg.name,
                            @tagName(cfg.kind),
                        });
                    }
                }
                return err;
            };
        }

        fn proveVectorSearchAccessPath(self: *DB, index_name: ?[]const u8, layout: algebraic_mod.ir.PhysicalLayout, constrained: bool) !void {
            const selected_path = switch (layout) {
                .dense_vector => self.core.index_manager.denseVectorAccessPath(index_name),
                .sparse_vector => self.core.index_manager.sparseVectorAccessPath(index_name),
                else => null,
            };
            const access_path = selected_path orelse {
                if (index_name) |name| {
                    if (self.core.index_manager.loadFailure(name) != null) return error.IndexUnavailable;
                }
                return error.IndexNotFound;
            };
            var planned = (try algebraic_mod.planner.planVectorSearchTensorProgramAlloc(self.alloc, access_path.owner, layout, constrained)) orelse return error.InvalidIndexConfig;
            defer planned.deinit(self.alloc);
            if (planned.access_paths.len != 1 or
                planned.access_paths[0].layout != layout or
                !std.mem.eql(u8, planned.access_paths[0].owner, access_path.owner) or
                !std.mem.eql(algebraic_mod.ir.Dimension, planned.access_paths[0].output_dims, access_path.output_dims))
            {
                return error.InvalidIndexConfig;
            }
            if (!algebraic_mod.ir.vectorSearchProgramMatchesTarget(planned.asProgram(), access_path.owner, layout, constrained)) return error.InvalidIndexConfig;
        }

        fn hasNativeDocIdConstraints(req: types.SearchRequest) bool {
            return req.filter_doc_ids_positive or
                req.filter_doc_ids.len > 0 or
                req.exclude_doc_ids.len > 0 or
                req.resolved_doc_filter != null or
                req.doc_filter_bindings.len > 0;
        }

        fn denseQueryMetricIndexName(self: *DB, req: types.SearchRequest) ?[]const u8 {
            if (req.index_name) |name| return name;
            const entry = self.core.denseIndex(null) orelse return null;
            return entry.config.name;
        }

        fn sparseQueryMetricIndexName(self: *DB, req: types.SearchRequest) ?[]const u8 {
            if (req.index_name) |name| return name;
            const entry = self.core.sparseIndex(null) orelse return null;
            return entry.config.name;
        }

        fn denseSearchExecutor(self: *DB) db_query_search.DenseSearchExecutor {
            return .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .dense_index = Self.denseIndexCallback,
                .lookup_doc_key = Self.denseDocKeyCallback,
                .lookup_vector_id = Self.denseVectorIdCallback,
                .lookup_vector_ids_for_ordinals = Self.denseVectorIdsForOrdinalsCallback,
                .all_docs_visible_fast = Self.allDocsVisibleFastCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
                .lookup_doc_ordinals = Self.lookupLiveDocOrdinalsNoLockCallback,
                .lookup_doc_ordinals_for_vector_ids = Self.denseOrdinalsForVectorIdsCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .load_projected_document = Self.loadRequiredProjectedSearchDocumentCallback,
                .hbc_search = Self.hbcSearchCallback,
                .hbc_search_profiled = Self.hbcSearchProfiledCallback,
                .postprocess = Self.postprocessVectorSearchResultCallback,
            };
        }

        fn sparseSearchExecutor(self: *DB) db_query_search.SparseSearchExecutor {
            return .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .sparse_index = Self.sparseIndexCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .lookup_doc_nums_for_ordinals = Self.sparseDocNumsForOrdinalsCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalNoLockCallback,
                .lookup_doc_ordinals = Self.lookupLiveDocOrdinalsNoLockCallback,
                .load_projected_document = Self.loadRequiredProjectedSearchDocumentCallback,
                .load_projected_documents = Self.loadProjectedSearchDocumentManyCallback,
                .postprocess = Self.postprocessVectorSearchResultCallback,
            };
        }

        pub fn searchRequestAtCurrentIdentityGeneration(self: *DB, req: types.SearchRequest) !types.SearchRequest {
            var snapshot_req = req;
            snapshot_req.identity_read_generation = try self.currentIdentityReadGenerationForRequest(snapshot_req.identity_read_generation);
            try Self.validateResolvedDocFilterWireContext(self, snapshot_req);
            return snapshot_req;
        }

        fn validateResolvedDocFilterWireContext(self: *DB, req: types.SearchRequest) !void {
            const ctx = req.resolved_doc_filter_wire_context orelse return;
            if (req.resolved_doc_filter == null) return error.InvalidQueryRequest;
            if (!ctx.namespace.eql(self.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            if (req.identity_read_generation == null or req.identity_read_generation.? != ctx.identity_read_generation) {
                self.doc_set_planning_stats.recordStaleIdentityGenerationRejection();
                return error.UnsupportedQueryRequest;
            }
        }

        pub fn collectSearchRequestTextStats(self: *DB, alloc: Allocator, req: types.SearchRequest) ![]const distributed_stats_mod.TextFieldStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try db_query_search.collectSearchRequestTextStats(alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
            });
        }

        pub fn preflightSearchRequest(self: *DB, alloc: Allocator, req: types.SearchRequest, max_work: u32) !db_query_search.RuntimePreflightSummary {
            return try Self.preflightSearchRequestWithExecutionContext(self, alloc, req, max_work, .{});
        }

        pub fn preflightSearchRequestWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
            exec_ctx: types.ExecutionContext,
        ) !db_query_search.RuntimePreflightSummary {
            return try Self.collectPlanningStatsWithExecutionContext(self, alloc, req, max_work, exec_ctx);
        }

        pub fn collectPlanningStats(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
        ) !planning_stats_mod.PlanningStatsSummary {
            return try Self.collectPlanningStatsWithExecutionContext(self, alloc, req, max_work, .{});
        }

        pub fn collectPlanningStatsWithExecutionContext(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
            exec_ctx: types.ExecutionContext,
        ) !planning_stats_mod.PlanningStatsSummary {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try Self.collectPlanningStatsLocked(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), max_work, exec_ctx);
        }

        fn collectPlanningStatsLocked(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
            exec_ctx: types.ExecutionContext,
        ) !planning_stats_mod.PlanningStatsSummary {
            _ = exec_ctx;
            try planning_bindings_mod.validateSearchRequestBindings(&self.core, self.alloc, req);
            return try planning_adapter_mod.collectSearchRequestStatsAlloc(
                alloc,
                &self.core,
                self,
                planningStatsSearchRequestCallback,
                req,
                max_work,
            );
        }

        pub fn planningStatsProvider(self: *DB) planning_stats_mod.PlanningStatsProvider {
            return planning_stats_mod.PlanningStatsProvider.init(self, planningStatsProviderCollectSearchRequestStats);
        }

        fn planningStatsProviderCollectSearchRequestStats(
            ptr: *anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            max_work: u32,
        ) !planning_stats_mod.PlanningStatsSummary {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try Self.collectPlanningStats(self, alloc, req, max_work);
        }

        fn planningStatsSearchRequestCallback(
            ptr: *anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) !types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ptr));
            return try Self.searchLocked(self, alloc, req);
        }

        pub fn collectExplicitTextStats(self: *DB, alloc: Allocator, requests: []const db_query_search.ExplicitTextStatRequest) ![]const distributed_stats_mod.TextFieldStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try db_query_search.collectExplicitTextStats(alloc, requests, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
            });
        }

        pub fn collectExplicitBackgroundTextStats(
            self: *DB,
            alloc: Allocator,
            requests: []const db_query_search.ExplicitBackgroundTextStatRequest,
        ) ![]const aggregations_mod.DistributedBackgroundTextStats {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            return try db_query_search.collectExplicitBackgroundTextStats(alloc, requests, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
            });
        }

        pub fn searchDenseProfiled(self: *DB, alloc: Allocator, req: types.SearchRequest, dense: types.DenseKnnQuery) !db_query_search.ProfiledDenseSearchResult {
            if (Self.canUsePublishedDenseSearch(self, req)) {
                return try Self.searchDenseProfiledAtSnapshot(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), dense);
            }
            {
                self.core.lockApplyShared();
                defer self.core.unlockApplyShared();
                return try Self.searchDenseProfiledAtSnapshot(self, alloc, try Self.searchRequestAtCurrentIdentityGeneration(self, req), dense);
            }
        }

        pub fn canUsePublishedDenseSearch(self: *DB, req: types.SearchRequest) bool {
            if (req.graph_queries.len != 0) return false;
            if (req.graph_metric_queries.len != 0) return false;
            if (req.full_text != null or req.sparse != null) return false;
            if (req.full_text_queries.len != 0 or req.dense_queries.len != 0 or req.sparse_queries.len != 0) return false;
            if (req.merge_config != null) return false;
            if (req.doc_filter_bindings.len != 0) return false;
            if (req.filter_query_json.len != 0 or req.exclusion_query_json.len != 0) return false;
            if (req.resolved_doc_filter != null) return false;
            if (req.filter_doc_ids_positive or req.filter_doc_ids.len != 0 or req.exclude_doc_ids.len != 0) return false;
            if (!(req.dense != null or req.query == .dense_knn)) return false;
            const entry = Self.denseIndex(self, req.index_name) orelse return false;
            return !entry.index.hasExternalVectorLoader();
        }

        pub fn relationalFilterGenerationCanUseCurrentRows(self: *DB, generation: ?u64) bool {
            const requested = generation orelse return true;
            return requested == self.core.nextDerivedSequence();
        }

        pub fn relationalAllRowsDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            generation: ?u64,
        ) !doc_set.ResolvedDocSet {
            const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, "", "");
            defer relational_store_mod.freeRows(alloc, rows);

            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            defer doc_ids.deinit(alloc);
            try doc_ids.ensureUnusedCapacity(alloc, rows.len);
            for (rows) |row| doc_ids.appendAssumeCapacity(row.doc_key);
            return try Self.resolveDocIdsToDocSet(self, alloc, doc_ids.items, generation);
        }

        pub fn combineRelationalFilterSetAlloc(
            self: *DB,
            alloc: Allocator,
            current: *?doc_set.ResolvedDocSet,
            child: *const doc_set.ResolvedDocSet,
            generation: ?u64,
            mode: relational_rows.FilterCombineMode,
        ) !void {
            if (current.* == null) {
                current.* = try doc_set.cloneAlloc(alloc, child);
                return;
            }

            if (try relational_rows.combineFilterSetFastAlloc(alloc, &current.*.?, child, mode)) |next| {
                var owned_next = next;
                errdefer owned_next.deinit(alloc);
                current.*.?.deinit(alloc);
                current.* = owned_next;
                return;
            }

            var fallback = try Self.combineRelationalFilterSetByVisibleDocIdsAlloc(self, alloc, &current.*.?, child, generation, mode);
            errdefer fallback.deinit(alloc);
            current.*.?.deinit(alloc);
            current.* = fallback;
        }

        fn combineRelationalFilterSetByVisibleDocIdsAlloc(
            self: *DB,
            alloc: Allocator,
            left: *const doc_set.ResolvedDocSet,
            right: *const doc_set.ResolvedDocSet,
            generation: ?u64,
            mode: relational_rows.FilterCombineMode,
        ) !doc_set.ResolvedDocSet {
            const left_ids = try Self.relationalFilterDocIdsForSetAlloc(self, alloc, left, generation);
            defer freeConstDocIdsAlloc(alloc, left_ids);
            const right_ids = try Self.relationalFilterDocIdsForSetAlloc(self, alloc, right, generation);
            defer freeConstDocIdsAlloc(alloc, right_ids);

            var right_set = std.StringHashMapUnmanaged(void).empty;
            defer right_set.deinit(alloc);
            for (right_ids) |doc_id| try right_set.put(alloc, doc_id, {});

            var out = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (out.items) |doc_id| alloc.free(@constCast(doc_id));
                out.deinit(alloc);
            }

            switch (mode) {
                .intersect => {
                    for (left_ids) |doc_id| {
                        if (right_set.contains(doc_id)) try out.append(alloc, try alloc.dupe(u8, doc_id));
                    }
                },
                .difference => {
                    for (left_ids) |doc_id| {
                        if (!right_set.contains(doc_id)) try out.append(alloc, try alloc.dupe(u8, doc_id));
                    }
                },
                .union_set => {
                    var seen = std.StringHashMapUnmanaged(void).empty;
                    defer seen.deinit(alloc);
                    for (left_ids) |doc_id| {
                        const entry = try seen.getOrPut(alloc, doc_id);
                        if (!entry.found_existing) try out.append(alloc, try alloc.dupe(u8, doc_id));
                    }
                    for (right_ids) |doc_id| {
                        const entry = try seen.getOrPut(alloc, doc_id);
                        if (!entry.found_existing) try out.append(alloc, try alloc.dupe(u8, doc_id));
                    }
                },
            }

            return if (out.items.len == 0) .none else .{ .doc_keys = try out.toOwnedSlice(alloc) };
        }

        fn relationalFilterDocIdsForSetAlloc(
            self: *DB,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) ![]const []const u8 {
            if (set.* != .all) {
                return (try Self.resolveDocSetDocIds(self, alloc, set, generation)) orelse error.UnsupportedQueryRequest;
            }

            var all_rows = try Self.relationalAllRowsDocSetAlloc(self, alloc, generation);
            defer all_rows.deinit(alloc);
            return (try Self.resolveDocSetDocIds(self, alloc, &all_rows, generation)) orelse error.UnsupportedQueryRequest;
        }

        pub fn relationalColumnIndexUsableForQuery(
            self: *DB,
            alloc: Allocator,
            column: schema_mod.RelationalColumn,
            implications: relational_rows.PredicateImplications,
        ) !bool {
            _ = self;
            return try relational_rows.columnIndexUsableForQuery(alloc, column, implications, platform_time.realtimeNs());
        }

        pub fn resolveRelationalFilterQueryDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            query: search_mod.SearchQuery,
            generation: ?u64,
        ) anyerror!?doc_set.ResolvedDocSet {
            return try Self.resolveRelationalFilterQueryDocSetWithImplicationsAlloc(self, alloc, runtime_schema, query, .{}, generation);
        }

        pub fn resolveRelationalFilterQueryDocSetWithImplicationsAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            query: search_mod.SearchQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) anyerror!?doc_set.ResolvedDocSet {
            if (!Self.relationalFilterGenerationCanUseCurrentRows(self, generation)) return null;
            return switch (query) {
                .match_none => .none,
                .match_all => try Self.relationalAllRowsDocSetAlloc(self, alloc, generation),
                .doc_id => |doc_id| try Self.resolveDocIdsToDocSet(self, alloc, doc_id.ids, generation),
                .term => |term| try Self.resolveRelationalTermFilterDocSetAlloc(self, alloc, runtime_schema, term, implications, generation),
                .array_any => |array_any| try Self.resolveRelationalArrayAnyFilterDocSetAlloc(self, alloc, runtime_schema, array_any, implications, generation),
                .json_contains => |json_contains| try Self.resolveRelationalJsonContainsFilterDocSetAlloc(self, alloc, runtime_schema, json_contains, implications, generation),
                .term_range => |range| try Self.resolveRelationalTermRangeFilterDocSetAlloc(self, alloc, runtime_schema, range, implications, generation),
                .numeric_range => |range| try Self.resolveRelationalNumericFilterDocSetAlloc(self, alloc, runtime_schema, range, implications, generation),
                .date_range => |range| try Self.resolveRelationalDateFilterDocSetAlloc(self, alloc, runtime_schema, range, implications, generation),
                .bool_field => |bool_query| try Self.resolveRelationalBoolFieldFilterDocSetAlloc(self, alloc, runtime_schema, bool_query, implications, generation),
                .geo_distance => |geo_query| try Self.resolveRelationalGeoDistanceFilterDocSetAlloc(self, alloc, runtime_schema, geo_query, implications, generation),
                .geo_bbox => |geo_query| try Self.resolveRelationalGeoBBoxFilterDocSetAlloc(self, alloc, runtime_schema, geo_query, implications, generation),
                .bool_query => |bool_query| try Self.resolveRelationalBoolQueryDocSetAlloc(self, alloc, runtime_schema, bool_query, implications, generation),
                else => null,
            };
        }

        fn resolveRelationalTermFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            term: search_mod.TermQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, term.field) orelse return null;
            if (column.field_type != .keyword) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, column, implications, generation, struct {
                wanted: []const u8,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    return value.value_type == .bytes_val and !value.is_json and std.mem.eql(u8, value.value.bytes_val, ctx.wanted);
                }
            }{ .wanted = term.term });
        }

        fn resolveRelationalArrayAnyFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            array_any: search_mod.ArrayAnyQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, array_any.field) orelse return null;
            if (column.field_type != .array) return null;

            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (doc_ids.items) |doc_id| alloc.free(@constCast(doc_id));
                doc_ids.deinit(alloc);
            }

            if (try Self.relationalColumnIndexUsableForQuery(self, alloc, column, implications)) {
                const element_key = try relational_store_mod.arrayElementIndexKeyForValueAlloc(alloc, array_any.value);
                defer alloc.free(element_key);
                const indexed_doc_ids = try relational_store_mod.scanArrayElementDocKeysAlloc(alloc, self.core.store, column.path, element_key, "", "");
                defer relational_store_mod.freeDocKeys(alloc, indexed_doc_ids);
                for (indexed_doc_ids) |doc_id| {
                    const owned_doc_id = try alloc.dupe(u8, doc_id);
                    errdefer alloc.free(owned_doc_id);
                    try doc_ids.append(alloc, owned_doc_id);
                }
            } else {
                const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, "", "");
                defer relational_store_mod.freeRows(alloc, rows);
                for (rows) |row| {
                    const cell = (try relational_row_codec.findCellByPath(row.row_value, column.path)) orelse continue;
                    const value = relational_store_mod.OwnedColumnValue{
                        .doc_key = row.doc_key,
                        .value_type = cell.value_type,
                        .is_json = cell.is_json,
                        .value = cell.value,
                    };
                    if (!(try relational_rows.arrayColumnValueContains(alloc, value, array_any.value))) continue;
                    const owned_doc_id = try alloc.dupe(u8, row.doc_key);
                    errdefer alloc.free(owned_doc_id);
                    try doc_ids.append(alloc, owned_doc_id);
                }
            }

            _ = generation;
            if (doc_ids.items.len == 0) return .none;
            return .{ .doc_keys = try doc_ids.toOwnedSlice(alloc) };
        }

        fn resolveRelationalJsonContainsFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            json_contains: search_mod.JsonContainsQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, json_contains.field) orelse return null;
            if (column.field_type != .json) return null;

            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (doc_ids.items) |doc_id| alloc.free(@constCast(doc_id));
                doc_ids.deinit(alloc);
            }

            if ((try Self.relationalColumnIndexUsableForQuery(self, alloc, column, implications)) and relational_store_mod.jsonContainsHasIndexableLeaf(json_contains.value)) {
                const indexed_doc_ids = try relational_store_mod.scanJsonContainmentDocKeysAlloc(alloc, self.core.store, column.path, json_contains.value, "", "");
                defer relational_store_mod.freeDocKeys(alloc, indexed_doc_ids);
                for (indexed_doc_ids) |doc_id| {
                    const owned_doc_id = try alloc.dupe(u8, doc_id);
                    errdefer alloc.free(owned_doc_id);
                    try doc_ids.append(alloc, owned_doc_id);
                }
            } else {
                const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, "", "");
                defer relational_store_mod.freeRows(alloc, rows);
                for (rows) |row| {
                    const cell = (try relational_row_codec.findCellByPath(row.row_value, column.path)) orelse continue;
                    if (!(try relational_store_mod.jsonCellContains(alloc, cell, json_contains.value))) continue;
                    const owned_doc_id = try alloc.dupe(u8, row.doc_key);
                    errdefer alloc.free(owned_doc_id);
                    try doc_ids.append(alloc, owned_doc_id);
                }
            }

            _ = generation;
            if (doc_ids.items.len == 0) return .none;
            return .{ .doc_keys = try doc_ids.toOwnedSlice(alloc) };
        }

        fn resolveRelationalTermRangeFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            range: search_mod.TermRangeQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, range.field) orelse return null;
            if (column.field_type != .keyword) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, column, implications, generation, struct {
                min: ?[]const u8,
                max: ?[]const u8,
                inclusive_min: bool,
                inclusive_max: bool,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    if (value.value_type != .bytes_val or value.is_json) return false;
                    const bytes = value.value.bytes_val;
                    const above_min = if (ctx.min) |min| blk: {
                        const order = std.mem.order(u8, bytes, min);
                        break :blk order == .gt or (ctx.inclusive_min and order == .eq);
                    } else true;
                    const below_max = if (ctx.max) |max| blk: {
                        const order = std.mem.order(u8, bytes, max);
                        break :blk order == .lt or (ctx.inclusive_max and order == .eq);
                    } else true;
                    return above_min and below_max;
                }
            }{
                .min = range.min,
                .max = range.max,
                .inclusive_min = range.inclusive_min,
                .inclusive_max = range.inclusive_max,
            });
        }

        fn resolveRelationalNumericFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            range: search_mod.NumericRangeQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, range.field) orelse return null;
            if (column.field_type != .numeric) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, column, implications, generation, struct {
                min: ?f64,
                max: ?f64,
                inclusive_min: bool,
                inclusive_max: bool,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    if (value.value_type != .f64_val) return false;
                    const number = value.value.f64_val;
                    const above_min = if (ctx.min) |min| if (ctx.inclusive_min) number >= min else number > min else true;
                    const below_max = if (ctx.max) |max| if (ctx.inclusive_max) number <= max else number < max else true;
                    return above_min and below_max;
                }
            }{
                .min = range.min,
                .max = range.max,
                .inclusive_min = range.inclusive_min,
                .inclusive_max = range.inclusive_max,
            });
        }

        fn resolveRelationalDateFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            range: search_mod.DateRangeQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, range.field) orelse return null;
            if (column.field_type != .datetime) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, column, implications, generation, struct {
                start_ns: ?u64,
                end_ns: ?u64,
                inclusive_start: bool,
                inclusive_end: bool,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    if (value.value_type != .u64_val) return false;
                    const timestamp = value.value.u64_val;
                    const after_start = if (ctx.start_ns) |start| if (ctx.inclusive_start) timestamp >= start else timestamp > start else true;
                    const before_end = if (ctx.end_ns) |end| if (ctx.inclusive_end) timestamp <= end else timestamp < end else true;
                    return after_start and before_end;
                }
            }{
                .start_ns = range.start_ns,
                .end_ns = range.end_ns,
                .inclusive_start = range.inclusive_start,
                .inclusive_end = range.inclusive_end,
            });
        }

        fn resolveRelationalBoolFieldFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            bool_query: search_mod.BoolFieldQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, bool_query.field) orelse return null;
            if (column.field_type != .boolean) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, column, implications, generation, struct {
                wanted: bool,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    return value.value_type == .bool_val and value.value.bool_val == ctx.wanted;
                }
            }{ .wanted = bool_query.value });
        }

        fn resolveRelationalGeoDistanceFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            geo_query: search_mod.GeoDistanceQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, geo_query.field) orelse return null;
            if (column.field_type != .geopoint) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, column, implications, generation, struct {
                center: search_mod.GeoPoint,
                radius_meters: f64,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    if (value.value_type != .geo_point) return false;
                    const point = search_mod.GeoPoint{
                        .lat = value.value.geo_point.lat,
                        .lon = value.value.geo_point.lon,
                    };
                    return search_geo_mod.haversineDistance(ctx.center, point) <= ctx.radius_meters;
                }
            }{
                .center = geo_query.center,
                .radius_meters = geo_query.radius_meters,
            });
        }

        fn resolveRelationalGeoBBoxFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            geo_query: search_mod.GeoBBoxQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_rows.columnForField(runtime_schema, geo_query.field) orelse return null;
            if (column.field_type != .geopoint) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, column, implications, generation, struct {
                min_lat: f64,
                min_lon: f64,
                max_lat: f64,
                max_lon: f64,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    if (value.value_type != .geo_point) return false;
                    const point = value.value.geo_point;
                    return point.lat >= ctx.min_lat and point.lat <= ctx.max_lat and
                        point.lon >= ctx.min_lon and point.lon <= ctx.max_lon;
                }
            }{
                .min_lat = geo_query.min_lat,
                .min_lon = geo_query.min_lon,
                .max_lat = geo_query.max_lat,
                .max_lon = geo_query.max_lon,
            });
        }

        fn resolveRelationalBoolQueryDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            bool_query: search_mod.BoolQuery,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
        ) anyerror!?doc_set.ResolvedDocSet {
            if (bool_query.min_should > 1) return null;

            var current: ?doc_set.ResolvedDocSet = null;
            errdefer if (current) |*set| set.deinit(alloc);

            for (bool_query.must) |child_query| {
                var child = (try Self.resolveRelationalFilterQueryDocSetWithImplicationsAlloc(self, alloc, runtime_schema, child_query, implications, generation)) orelse {
                    if (current) |*set| set.deinit(alloc);
                    return null;
                };
                defer child.deinit(alloc);
                try Self.combineRelationalFilterSetAlloc(self, alloc, &current, &child, generation, .intersect);
            }

            if (bool_query.should.len > 0 and (bool_query.min_should > 0 or current == null)) {
                var should_set: ?doc_set.ResolvedDocSet = null;
                errdefer if (should_set) |*set| set.deinit(alloc);
                for (bool_query.should) |child_query| {
                    var child = (try Self.resolveRelationalFilterQueryDocSetWithImplicationsAlloc(self, alloc, runtime_schema, child_query, implications, generation)) orelse {
                        if (current) |*set| set.deinit(alloc);
                        if (should_set) |*set| set.deinit(alloc);
                        return null;
                    };
                    defer child.deinit(alloc);
                    try Self.combineRelationalFilterSetAlloc(self, alloc, &should_set, &child, generation, .union_set);
                }
                if (should_set) |*set| {
                    try Self.combineRelationalFilterSetAlloc(self, alloc, &current, set, generation, .intersect);
                    set.* = .none;
                }
            }

            if (current == null and bool_query.must_not.len > 0) {
                current = try Self.relationalAllRowsDocSetAlloc(self, alloc, generation);
            }

            for (bool_query.must_not) |child_query| {
                var child = (try Self.resolveRelationalFilterQueryDocSetWithImplicationsAlloc(self, alloc, runtime_schema, child_query, implications, generation)) orelse {
                    if (current) |*set| set.deinit(alloc);
                    return null;
                };
                defer child.deinit(alloc);
                try Self.combineRelationalFilterSetAlloc(self, alloc, &current, &child, generation, .difference);
            }

            return current orelse null;
        }

        fn scanRelationalColumnFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            column: schema_mod.RelationalColumn,
            implications: relational_rows.PredicateImplications,
            generation: ?u64,
            matcher: anytype,
        ) !doc_set.ResolvedDocSet {
            if (!(try Self.relationalColumnIndexUsableForQuery(self, alloc, column, implications))) {
                return try Self.scanRelationalBaseRowsFilterDocSetAlloc(self, alloc, column, generation, matcher);
            }

            const values = try relational_store_mod.scanColumnAlloc(alloc, self.core.store, column.path, "", "");
            defer relational_store_mod.freeColumnValues(alloc, values);

            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            defer doc_ids.deinit(alloc);
            for (values) |value| {
                if (!matcher.matches(value)) continue;
                try doc_ids.append(alloc, value.doc_key);
            }
            if (doc_ids.items.len == 0 and (column.index_where.len != 0 or column.index_where_expressions.len != 0)) {
                return try Self.scanRelationalBaseRowsFilterDocSetAlloc(self, alloc, column, generation, matcher);
            }
            return try Self.resolveDocIdsToDocSet(self, alloc, doc_ids.items, generation);
        }

        fn scanRelationalBaseRowsFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            column: schema_mod.RelationalColumn,
            generation: ?u64,
            matcher: anytype,
        ) !doc_set.ResolvedDocSet {
            const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, "", "");
            defer relational_store_mod.freeRows(alloc, rows);

            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            defer doc_ids.deinit(alloc);
            for (rows) |row| {
                const cell = (try relational_row_codec.findCellByPath(row.row_value, column.path)) orelse continue;
                const value = relational_store_mod.OwnedColumnValue{
                    .doc_key = row.doc_key,
                    .value_type = cell.value_type,
                    .is_json = cell.is_json,
                    .value = cell.value,
                };
                if (!matcher.matches(value)) continue;
                try doc_ids.append(alloc, row.doc_key);
            }
            return try Self.resolveDocIdsToDocSet(self, alloc, doc_ids.items, generation);
        }

        pub fn applyGraphExpandStrategy(self: *DB, alloc: Allocator, result: *types.SearchResult, strategy: ?graph_query_mod.ExpandStrategy) !void {
            _ = self;
            try db_query_graph.applyGraphExpandStrategy(alloc, result, strategy);
        }

        pub fn executeSearchGraphQuery(
            self: *DB,
            alloc: Allocator,
            graph_query: graph_query_mod.GraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) !graph_query_mod.GraphQueryResult {
            const entry = self.core.graphIndex(graph_query.index_name) orelse return error.IndexNotFound;
            if (entry.index.supportsAlgebraicSemiringTraversal()) {
                try Self.proveGraphTraversalProgram(self, graph_query.index_name, target_keys.len > 0);
            }

            var resolved_query = graph_query;
            if (graph_query.target_nodes != null) {
                resolved_query.target_nodes = .{ .keys = try Self.castOwnedKeysToConst(alloc, target_keys) };
            }
            defer if (graph_query.target_nodes != null) {
                switch (resolved_query.target_nodes.?) {
                    .keys => |owned| alloc.free(owned),
                    .result_ref => unreachable,
                }
            };

            var graph_engine = graph_query_mod.GraphQueryEngine{ .alloc = alloc };
            return try graph_engine.execute(&entry.index, resolved_query, start_key_refs);
        }

        pub fn proveGraphTraversalProgram(self: *DB, index_name: []const u8, constrained_targets: bool) !void {
            const graph_access_path = self.core.index_manager.graphTraversalAccessPath(index_name) orelse return error.InvalidIndexConfig;
            var tensor_program = (try algebraic_mod.planner.planGraphTraversalTensorProgramAlloc(self.alloc, index_name, constrained_targets)) orelse return error.InvalidIndexConfig;
            defer tensor_program.deinit(self.alloc);
            if (tensor_program.access_paths.len != 1 or
                tensor_program.access_paths[0].layout != .graph_edges or
                !std.mem.eql(u8, tensor_program.access_paths[0].owner, index_name) or
                !std.mem.eql(algebraic_mod.ir.Dimension, tensor_program.access_paths[0].output_dims, graph_access_path.output_dims))
            {
                return error.InvalidIndexConfig;
            }
            if (!algebraic_mod.ir.graphTraversalProgramMatchesTarget(tensor_program.asProgram(), index_name, constrained_targets)) {
                return error.InvalidIndexConfig;
            }
        }

        pub fn getEdges(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            key: []const u8,
            edge_type: []const u8,
            direction: graph_mod.EdgeDirection,
        ) ![]graph_mod.Edge {
            if (key.len == 0) return try alloc.alloc(graph_mod.Edge, 0);
            return try self.core.graphGetEdges(alloc, index_name, key, edge_type, direction);
        }

        pub fn traverseEdges(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            start_key: []const u8,
            rules: traversal_mod.TraversalRules,
        ) ![]traversal_mod.TraversalResult {
            if (start_key.len == 0) return try alloc.alloc(traversal_mod.TraversalResult, 0);
            return try self.core.graphTraverseEdges(alloc, index_name, start_key, rules);
        }

        pub fn getNeighbors(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            key: []const u8,
            edge_type: []const u8,
            direction: graph_mod.EdgeDirection,
        ) ![]traversal_mod.TraversalResult {
            var edge_types_storage: [1][]const u8 = undefined;
            const edge_types = if (edge_type.len > 0) blk: {
                edge_types_storage[0] = edge_type;
                break :blk edge_types_storage[0..1];
            } else &.{};
            return try Self.traverseEdges(self, alloc, index_name, key, .{
                .edge_types = edge_types,
                .direction = direction,
                .max_depth = 1,
            });
        }

        pub fn rewriteEntityEdges(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            old_key: []const u8,
            new_key: []const u8,
        ) !usize {
            if (std.mem.eql(u8, old_key, new_key)) return 0;
            const inbound = try Self.getEdges(self, alloc, index_name, old_key, "", .in);
            defer graph_mod.GraphIndex.freeEdges(alloc, inbound);
            if (inbound.len == 0) return 0;

            var writes = try alloc.alloc(types.GraphEdgeWrite, inbound.len);
            defer alloc.free(writes);
            var deletes = try alloc.alloc(types.GraphEdgeDelete, inbound.len);
            defer alloc.free(deletes);
            for (inbound, 0..) |edge, i| {
                deletes[i] = .{ .index_name = index_name, .source = edge.source, .target = old_key, .edge_type = edge.edge_type };
                writes[i] = .{
                    .index_name = index_name,
                    .source = edge.source,
                    .target = new_key,
                    .edge_type = edge.edge_type,
                    .weight = edge.weight,
                    .created_at = edge.created_at,
                    .updated_at = edge.updated_at,
                    .metadata_json = edge.metadata,
                };
            }
            try self.batch(.{ .graph_writes = writes, .graph_deletes = deletes, .sync_level = .write });
            return inbound.len;
        }

        pub fn findShortestPath(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            source: []const u8,
            target: []const u8,
            edge_types: []const []const u8,
            direction: graph_mod.EdgeDirection,
            weight_mode: paths_mod.PathWeightMode,
            max_depth: u32,
            min_weight: f64,
            max_weight: f64,
        ) !?paths_mod.Path {
            if (source.len == 0 or target.len == 0) return null;
            if (try Self.findAlgebraicShortestPath(self, alloc, index_name, source, target, edge_types, direction, weight_mode, max_depth, min_weight, max_weight)) |path| {
                return path;
            }
            return try self.core.graphFindShortestPath(
                alloc,
                index_name,
                source,
                target,
                edge_types,
                direction,
                weight_mode,
                max_depth,
                min_weight,
                max_weight,
            );
        }

        pub fn findKShortestPaths(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            source: []const u8,
            target: []const u8,
            k: u32,
            edge_types: []const []const u8,
            direction: graph_mod.EdgeDirection,
            weight_mode: paths_mod.PathWeightMode,
            max_depth: u32,
            min_weight: f64,
            max_weight: f64,
        ) ![]paths_mod.Path {
            if (source.len == 0 or target.len == 0 or k == 0) return try alloc.alloc(paths_mod.Path, 0);
            if (k == 1) {
                if (try Self.findShortestPath(self, alloc, index_name, source, target, edge_types, direction, weight_mode, max_depth, min_weight, max_weight)) |path| {
                    const paths = try alloc.alloc(paths_mod.Path, 1);
                    paths[0] = path;
                    return paths;
                }
                return try alloc.alloc(paths_mod.Path, 0);
            }
            return try self.core.graphFindKShortestPaths(
                alloc,
                index_name,
                source,
                target,
                k,
                edge_types,
                direction,
                weight_mode,
                max_depth,
                min_weight,
                max_weight,
            );
        }

        fn findAlgebraicShortestPath(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            source: []const u8,
            target: []const u8,
            edge_types: []const []const u8,
            direction: graph_mod.EdgeDirection,
            weight_mode: paths_mod.PathWeightMode,
            max_depth: u32,
            min_weight: f64,
            max_weight: f64,
        ) !?paths_mod.Path {
            const entry = self.core.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
            const params = graph_query_mod.QueryParams{
                .edge_types = edge_types,
                .direction = direction,
                .max_depth = max_depth,
                .max_results = 0,
                .min_weight = min_weight,
                .max_weight = max_weight,
                .deduplicate = true,
                .include_paths = false,
                .weight_mode = weight_mode,
            };
            if (!(graph_query_mod.algebraicTraversalProof(&entry.index, params).safe())) return null;
            try Self.proveGraphTraversalProgram(self, index_name, true);

            var graph_engine = graph_query_mod.GraphQueryEngine{ .alloc = alloc };
            var result = try graph_engine.execute(&entry.index, .{
                .query_type = .shortest_path,
                .index_name = index_name,
                .start_nodes = .{ .keys = &.{source} },
                .target_nodes = .{ .keys = &.{target} },
                .params = params,
            }, &.{source});
            defer result.deinit(alloc);
            if (result.nodes.len == 0) return null;
            return try Self.graphResultNodePathAlloc(alloc, result.nodes[0]);
        }

        fn graphResultNodePathAlloc(alloc: Allocator, node: graph_query_mod.GraphResultNode) !?paths_mod.Path {
            const node_path = node.path orelse return null;
            const edge_path = node.path_edges orelse return null;
            const nodes = try alloc.alloc([]const u8, node_path.len);
            var initialized_nodes: usize = 0;
            errdefer {
                for (nodes[0..initialized_nodes]) |item| alloc.free(item);
                alloc.free(nodes);
            }
            for (node_path, 0..) |item, i| {
                nodes[i] = try alloc.dupe(u8, item);
                initialized_nodes += 1;
            }

            const edges = try alloc.alloc(paths_mod.PathEdge, edge_path.len);
            var initialized_edges: usize = 0;
            errdefer {
                for (edges[0..initialized_edges]) |edge| {
                    alloc.free(edge.source);
                    alloc.free(edge.target);
                    alloc.free(edge.edge_type);
                }
                alloc.free(edges);
            }
            var total_weight: f64 = 0;
            for (edge_path, 0..) |item, i| {
                edges[i] = .{
                    .source = try alloc.dupe(u8, item.source),
                    .target = try alloc.dupe(u8, item.target),
                    .edge_type = try alloc.dupe(u8, item.edge_type),
                    .weight = item.weight,
                };
                initialized_edges += 1;
                total_weight += item.weight;
            }

            return .{
                .nodes = nodes,
                .edges = edges,
                .total_weight = total_weight,
                .length = @intCast(edges.len),
            };
        }

        fn castOwnedKeysToConst(alloc: Allocator, keys: [][]u8) ![]const []const u8 {
            const out = try alloc.alloc([]const u8, keys.len);
            for (keys, 0..) |key, i| out[i] = key;
            return out;
        }

        pub fn matchPattern(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            start_keys: []const []const u8,
            pattern: []const graph_pattern_mod.PatternStep,
            max_results: u32,
            return_aliases: []const []const u8,
        ) ![]graph_pattern_mod.PatternMatch {
            if (start_keys.len == 0) return try alloc.alloc(graph_pattern_mod.PatternMatch, 0);
            return try self.core.graphMatchPattern(alloc, index_name, start_keys, pattern, .{
                .max_results = max_results,
                .return_aliases = return_aliases,
                .evaluator = .{
                    .ctx = self,
                    .func = Self.patternNodeFilterEvaluator,
                },
            });
        }

        fn graphInputSetHitsAlloc(
            self: *DB,
            alloc: Allocator,
            hit_ids: []const []const u8,
            generation: ?u64,
        ) ![]types.SearchHit {
            const hits = try alloc.alloc(types.SearchHit, hit_ids.len);
            var initialized: usize = 0;
            errdefer {
                for (hits[0..initialized]) |*hit| hit.deinit(alloc);
                if (hits.len > 0) alloc.free(hits);
            }
            for (hit_ids, 0..) |hit_id, j| {
                hits[j] = .{
                    .id = try alloc.dupe(u8, hit_id),
                    .doc_ordinal = try self.searchRuntimeLookupLiveDocOrdinalNoLock(alloc, hit_id, generation),
                    .score = null,
                    .stored_data = null,
                    .chunk_hits = &.{},
                };
                initialized += 1;
            }
            return hits;
        }

        pub fn executeNamedGraphQueries(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            graph_queries: []const types.NamedGraphQuery,
            input_sets: []const types.NamedGraphInputSet,
        ) ![]types.GraphSearchResult {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();
            if (req.identity_read_generation == null) {
                for (input_sets) |input_set| {
                    if (input_set.hit_ids.len > 0) return error.UnsupportedQueryRequest;
                }
            }
            const snapshot_req = try Self.searchRequestAtCurrentIdentityGeneration(self, req);

            var named_sets = try alloc.alloc(NamedResultSet, input_sets.len);
            defer alloc.free(named_sets);
            var temp_hits = try alloc.alloc([]types.SearchHit, input_sets.len);
            defer alloc.free(temp_hits);
            var resolved_sets = try alloc.alloc(?doc_set.ResolvedDocSet, input_sets.len);
            defer {
                for (resolved_sets) |*maybe_set| {
                    if (maybe_set.*) |*set| set.deinit(alloc);
                }
                if (resolved_sets.len > 0) alloc.free(resolved_sets);
            }
            @memset(resolved_sets, null);
            var initialized_sets: usize = 0;

            for (input_sets, 0..) |input_set, i| {
                resolved_sets[i] = try Self.resolveDocIdsToDocSet(self, alloc, input_set.hit_ids, snapshot_req.identity_read_generation);
                temp_hits[i] = try Self.graphInputSetHitsAlloc(self, alloc, input_set.hit_ids, snapshot_req.identity_read_generation);
                const resolved_doc_set = if (resolved_sets[i]) |*set| set else null;
                named_sets[i] = .{
                    .name = input_set.name,
                    .hits = temp_hits[i],
                    .total_hits = if (input_set.total_hits > 0) input_set.total_hits else @intCast(input_set.hit_ids.len),
                    .resolved_doc_set = resolved_doc_set,
                    .resolved_doc_set_complete = input_set.total_hits == 0 or @as(u64, input_set.total_hits) <= input_set.hit_ids.len,
                };
                initialized_sets += 1;
            }
            defer {
                for (temp_hits[0..initialized_sets]) |hits| {
                    for (hits) |*hit| hit.deinit(alloc);
                    if (hits.len > 0) alloc.free(hits);
                }
            }

            return try Self.executeGraphQueriesWithSets(self, alloc, snapshot_req, graph_queries, named_sets);
        }

        pub fn runGraphMetricMaintenanceForIdle(self: *DB) !usize {
            self.core.lockApply();
            defer self.core.unlockApply();
            return switch (self.graph_metric_idle_maintenance) {
                .legacy => try self.core.index_manager.runGraphMetricMaintenance(),
                .planned => try Self.runGraphMetricPlannedMaintenanceForIdleLocked(self),
                .auto => if (try self.core.index_manager.shouldRunGraphMetricPlannedAutoIdle(self.graph_metric_idle_auto_options))
                    try Self.runGraphMetricPlannedAutoMaintenanceForIdleLocked(self)
                else
                    try self.core.index_manager.runGraphMetricMaintenance(),
                .degree_canary => try Self.runGraphMetricDegreeCanaryMaintenanceForIdleLocked(self),
            };
        }

        fn runGraphMetricPlannedMaintenanceForIdleLocked(self: *DB) !usize {
            const result = try self.core.index_manager.runGraphMetricPlannedMaintenance(self.graph_metric_idle_planned_options);
            if (result.budget_exhausted) return error.RunUntilIdleDidNotConverge;
            return result.builds_started + result.pages_completed + result.phases_advanced + result.published;
        }

        fn runGraphMetricPlannedAutoMaintenanceForIdleLocked(self: *DB) !usize {
            const result = try self.core.index_manager.runGraphMetricPlannedAutoMaintenance(
                self.graph_metric_idle_planned_options,
                self.graph_metric_idle_auto_options,
            );
            if (result.budget_exhausted) return error.RunUntilIdleDidNotConverge;
            const progressed = result.builds_started + result.pages_completed + result.phases_advanced + result.published;
            const after_decision = try self.core.index_manager.graphMetricPlannedAutoIdleDecision(self.graph_metric_idle_auto_options);
            if (!after_decision.shouldRunPlanned() and after_decision.ineligible_queued != 0) {
                return progressed + try self.core.index_manager.runGraphMetricMaintenance();
            }
            return progressed;
        }

        fn runGraphMetricDegreeCanaryMaintenanceForIdleLocked(self: *DB) !usize {
            const decision = try self.core.index_manager.graphMetricDegreeCanaryDecision(self.graph_metric_idle_degree_canary_options);
            if (decision.shouldRunPlanned()) return try Self.runGraphMetricPlannedMaintenanceForIdleLocked(self);
            if (decision.active_degree_builds != 0 or decision.blocked_active_non_degree != 0) {
                return error.RunUntilIdleDidNotConverge;
            }
            return try self.core.index_manager.runGraphMetricMaintenance();
        }

        pub fn runGraphMetricPlannedMaintenanceForIdle(
            self: *DB,
            options: index_manager_mod.IndexManager.GraphMetricPlannedMaintenanceOptions,
        ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.index_manager.runGraphMetricPlannedMaintenance(options);
        }

        const GraphMetricServiceMaintenanceAction = enum {
            tick,
            status,
            release,
        };

        const GraphMetricServiceMaintenanceRequest = struct {
            action: GraphMetricServiceMaintenanceAction = .tick,
            role: graph_metric_runtime_mod.Role,
            runtime_id: []const u8,
            owner_id: []const u8,
            lease_owned: bool = false,
            lease_ttl_ms: u64 = 30_000,
            worker_id: ?[]const u8 = null,
            worker_ids: ?[]const []const u8 = null,
            start_background_builds: bool = true,
            max_rounds: usize = 1,
            max_metrics_per_round: usize = 8,
            max_pages_per_round: usize = 1,
            preserve_lease_after_tick: bool = false,
            now_ms: ?u64 = null,
        };

        pub fn runGraphMetricServiceMaintenanceJsonAlloc(self: *DB, alloc: Allocator, body: []const u8) ![]u8 {
            var parsed = std.json.parseFromSlice(GraphMetricServiceMaintenanceRequest, alloc, if (body.len == 0) "{}" else body, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            }) catch return error.InvalidGraphMetricRuntimeConfig;
            defer parsed.deinit();

            var manual_clock = platform_clock.ManualClock{};
            if (parsed.value.now_ms) |now_ms| manual_clock.setRealtimeNs(now_ms * std.time.ns_per_ms);
            const clock = if (parsed.value.now_ms != null) manual_clock.clock() else platform_clock.Clock.real();
            const resources = self.core.asyncResources();
            const worker_ids = parsed.value.worker_ids orelse &.{};
            var runtime = try graph_metric_runtime_mod.GraphMetricRuntime.init(
                alloc,
                resources.store,
                resources.index_manager,
                resources.apply_mutex,
                self.backend_runtime,
                .{
                    .enabled = true,
                    .start_background_loop = false,
                    .role = parsed.value.role,
                    .runtime_id = parsed.value.runtime_id,
                    .lease_owned = parsed.value.lease_owned,
                    .owner_id = parsed.value.owner_id,
                    .lease_ttl_ms = parsed.value.lease_ttl_ms,
                    .coordinator_start_background_builds = parsed.value.start_background_builds,
                    .planned_options = .{
                        .worker_id = parsed.value.worker_id orelse "",
                        .worker_ids = worker_ids,
                        .max_rounds = parsed.value.max_rounds,
                        .max_metrics_per_round = parsed.value.max_metrics_per_round,
                        .max_pages_per_round = parsed.value.max_pages_per_round,
                    },
                    .clock = clock,
                },
            );
            var preserve_lease = false;
            defer if (preserve_lease) runtime.deinitPreserveLease() else runtime.deinit();

            if (parsed.value.action == .release) {
                runtime.ownership.releaseHeldLease();
                var runtime_stats = runtime.stats();
                runtime_stats.shutdown = true;
                return try std.json.Stringify.valueAlloc(alloc, .{
                    .released = true,
                    .stats = runtime_stats,
                }, .{ .emit_null_optional_fields = false });
            }

            const result: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult = if (parsed.value.action == .tick) try runtime.runOnceDetailed() else .{};
            const runtime_stats = runtime.stats();
            if (parsed.value.preserve_lease_after_tick and parsed.value.lease_owned and parsed.value.action == .tick and runtime_stats.has_lease) {
                preserve_lease = true;
            }
            return try std.json.Stringify.valueAlloc(alloc, .{
                .result = result,
                .stats = runtime_stats,
            }, .{ .emit_null_optional_fields = false });
        }

        pub fn refreshGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.refreshGraphMetric(index_name, metric_name);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn rebuildGraphMetric(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.rebuildGraphMetric(index_name, metric_name);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn deleteGraphMetricMaterialization(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.deleteGraphMetricMaterialization(index_name, metric_name);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn pauseGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.pauseGraphMetricMaintenance(index_name, metric_name);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn resumeGraphMetricMaintenance(self: *DB, alloc: Allocator, index_name: []const u8, metric_name: []const u8) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.resumeGraphMetricMaintenance(index_name, metric_name);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn ensureGraphMetricPlannedBuild(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            metric_name: []const u8,
            target_generation: u64,
        ) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.ensureGraphMetricPlannedBuild(index_name, metric_name, target_generation);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn runGraphMetricPlannedWorkerPageStep(
            self: *DB,
            index_name: []const u8,
            metric_name: []const u8,
            worker_id: []const u8,
        ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.index_manager.runGraphMetricPlannedWorkerPageStep(index_name, metric_name, worker_id);
        }

        pub fn runGraphMetricPlannedWorkerPageStepAt(
            self: *DB,
            index_name: []const u8,
            metric_name: []const u8,
            worker_id: []const u8,
            now_ms: u64,
        ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.index_manager.runGraphMetricPlannedWorkerPageStepAt(index_name, metric_name, worker_id, now_ms);
        }

        pub fn runGraphMetricPlannedCoordinatorStep(
            self: *DB,
            index_name: []const u8,
            metric_name: []const u8,
        ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.index_manager.runGraphMetricPlannedCoordinatorStep(index_name, metric_name);
        }

        pub fn runGraphMetricPlannedCoordinatorStepAt(
            self: *DB,
            index_name: []const u8,
            metric_name: []const u8,
            now_ms: u64,
        ) !graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.index_manager.runGraphMetricPlannedCoordinatorStepAt(index_name, metric_name, now_ms);
        }

        pub fn failGraphMetricPlannedBuild(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            metric_name: []const u8,
            err: anyerror,
        ) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.failGraphMetricPlannedBuild(index_name, metric_name, err);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn runGraphMetricPlannedDrain(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            metric_name: []const u8,
            target_generation: u64,
            options: graph_mod.GraphIndex.GraphMetricPlannedDrainOptions,
        ) !types.GraphMetricStatus {
            self.core.lockApply();
            defer self.core.unlockApply();
            var status = try self.core.index_manager.runGraphMetricPlannedDrain(index_name, metric_name, target_generation, options);
            defer status.deinit(self.core.index_manager.alloc);
            return try cloneGraphMetricStatusFromGraph(alloc, status);
        }

        pub fn runGraphMetricPlannedCoordinatorSweep(
            self: *DB,
            options: index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
        ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.index_manager.runGraphMetricPlannedCoordinatorSweep(options);
        }

        pub fn runGraphMetricPlannedWorkerSweep(
            self: *DB,
            options: index_manager_mod.IndexManager.GraphMetricPlannedWorkerSweepOptions,
        ) !index_manager_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.core.index_manager.runGraphMetricPlannedWorkerSweep(options);
        }

        pub fn matchNamedPattern(
            self: *DB,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
        ) ![]graph_pattern_mod.PatternMatch {
            return try Self.matchPattern(
                self,
                alloc,
                named.query.index_name,
                start_key_refs,
                named.query.pattern,
                named.query.params.max_results,
                named.query.return_aliases,
            );
        }

        pub fn loadPatternProjectedDocument(
            self: *DB,
            alloc: Allocator,
            query: graph_query_mod.GraphQuery,
            key: []const u8,
        ) !?[]u8 {
            return if (try Self.loadStoredSearchDocument(self, alloc, key)) |stored|
                try self.searchRuntimeProjectLookupStoredBytes(alloc, key, stored, .{
                    .fields = query.fields,
                    .include_all_fields = query.include_all_fields,
                })
            else
                null;
        }

        fn patternNodeFilterEvaluator(ctx: ?*anyopaque, key: []const u8, filter: graph_pattern_mod.NodeFilter) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.UnsupportedNodeFilterQuery));
            return try Self.matchesPatternNodeFilter(self, key, filter);
        }

        fn matchesPatternNodeFilter(self: *DB, key: []const u8, filter: graph_pattern_mod.NodeFilter) !bool {
            if (filter.filter_query_json == null) return true;
            const stored = (try Self.loadStoredSearchDocument(self, self.alloc, key)) orelse return false;
            defer self.alloc.free(stored);
            return try db_query_graph.storedDocMatchesPatternFilter(self.alloc, key, stored, filter.filter_query_json.?);
        }

        pub fn projectStoredBytesForSearch(self: *DB, alloc: Allocator, req: types.SearchRequest, doc_key: []const u8, raw: []const u8) ![]u8 {
            if (req.defer_stored_projection and db_query_projection.shouldProjectSearchStored(req)) {
                return try alloc.dupe(u8, raw);
            }
            return try db_query_projection.projectStoredBytesForSearch(alloc, req, doc_key, raw, .{
                .ctx = self,
                .load_chunks = Self.loadChunkFieldValueCallback,
                .load_embeddings = Self.loadEmbeddingFieldValueCallback,
                .load_artifacts = Self.loadArtifactFieldValueCallback,
            });
        }

        pub fn projectOwnedStoredBytesForSearch(self: *DB, alloc: Allocator, req: types.SearchRequest, doc_key: []const u8, raw: []u8) ![]u8 {
            if (req.defer_stored_projection and db_query_projection.shouldProjectSearchStored(req)) {
                return raw;
            }
            return try db_query_projection.projectOwnedStoredBytesForSearch(alloc, req, doc_key, raw, .{
                .ctx = self,
                .load_chunks = Self.loadChunkFieldValueCallback,
                .load_embeddings = Self.loadEmbeddingFieldValueCallback,
                .load_artifacts = Self.loadArtifactFieldValueCallback,
            });
        }

        pub fn loadProjectedSearchDocument(self: *DB, alloc: Allocator, req: types.SearchRequest, key: []const u8) !?[]u8 {
            return if (try Self.loadStoredSearchDocument(self, alloc, key)) |stored|
                try Self.projectOwnedStoredBytesForSearch(self, alloc, req, key, stored)
            else
                null;
        }

        pub fn loadRequiredProjectedSearchDocument(self: *DB, alloc: Allocator, req: types.SearchRequest, key: []const u8) ![]u8 {
            return try Self.projectOwnedStoredBytesForSearch(
                self,
                alloc,
                req,
                key,
                (try Self.loadStoredSearchDocument(self, alloc, key)) orelse return error.StoredDocMissing,
            );
        }

        pub fn loadProjectedSearchDocumentMany(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            keys: []const []const u8,
        ) ![]?[]u8 {
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const loaded = try Self.loadStoredSearchDocumentMany(self, alloc, keys);
            errdefer freeOptionalOwnedBytes(alloc, loaded);

            var project_ns: u64 = 0;
            for (loaded, 0..) |maybe_stored, i| {
                if (maybe_stored) |stored| {
                    const project_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
                    loaded[i] = try Self.projectOwnedStoredBytesForSearch(self, alloc, req, keys[i], stored);
                    if (bench_profile) project_ns += platform_time.monotonicNs() - project_start_ns;
                }
            }
            if (bench_profile) {
                std.log.info(
                    "antfly_bench_projected_many total_us={d} project_us={d} keys={d}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, project_ns / 1000, keys.len },
                );
            }
            return loaded;
        }

        pub fn loadParentStoredForSearch(self: *DB, alloc: Allocator, req: types.SearchRequest, parent_id: []const u8) !?[]u8 {
            return if (try Self.loadStoredSearchDocument(self, alloc, parent_id)) |stored|
                try Self.projectOwnedStoredBytesForSearch(self, alloc, req, parent_id, stored)
            else
                null;
        }

        pub fn loadParentStoredForSearchMany(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            parent_ids: []const []const u8,
        ) ![]?[]u8 {
            return try Self.loadProjectedSearchDocumentMany(self, alloc, req, parent_ids);
        }

        pub fn get(self: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
            if (Self.hasRelationalBaseRows(self) and !Self.isMetadataKey(self, key) and !internal_keys.isInternalPhysicalTableDataKey(key)) {
                return try relational_store_mod.getMaterializedAlloc(alloc, self.core.store, key);
            }
            const store_key = try Self.encodeStoreLookupKeyAlloc(self, alloc, key);
            defer alloc.free(store_key);
            const value = try self.core.getStoreValue(alloc, store_key) orelse return null;
            return try mapper.materializeOwnedDocumentValueAlloc(alloc, value);
        }

        pub fn getRawStoreValue(self: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
            return self.core.getStoreValue(alloc, key) catch |err| switch (err) {
                error.NotFound => null,
                else => err,
            };
        }

        pub fn getArtifact(self: *DB, alloc: Allocator, artifact_id: []const u8) !?types.ArtifactRecord {
            var artifact_ref = (try artifact_ids.decodeArtifactPublicIdAlloc(alloc, artifact_id)) orelse return error.InvalidArgument;
            errdefer artifact_ref.deinit(alloc);

            const internal_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, artifact_ref);
            defer alloc.free(internal_key);

            const value = try self.core.getStoreValue(alloc, internal_key) orelse return null;
            errdefer alloc.free(value);

            return .{
                .id = try alloc.dupe(u8, artifact_id),
                .value = value,
                .artifact_ref = artifact_ref,
            };
        }

        pub fn getDocument(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
            return try Self.lookup(self, alloc, key, opts);
        }

        pub fn lookup(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
            if (!internal_keys.isInternalPhysicalTableDataKey(key) and (try Self.isExpiredDocumentKey(self, alloc, key))) return null;
            const raw = try Self.get(self, alloc, key) orelse return null;
            defer alloc.free(raw);
            const stored = try Self.projectLookupStoredBytes(self, alloc, key, raw, opts);
            return .{ .json = stored };
        }

        pub fn getTimestamp(self: *DB, alloc: Allocator, key: []const u8) !u64 {
            if (internal_keys.isInternalPhysicalTableDataKey(key)) return 0;
            return try self.core.readTimestamp(alloc, key);
        }

        pub fn scan(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !types.ScanResult {
            self.core.lockApplyShared();
            defer self.core.unlockApplyShared();

            const byte_range = self.core.byteRange();
            const lower_raw = if (from_key.len == 0 or std.mem.order(u8, from_key, byte_range.start) == .lt) byte_range.start else from_key;
            const upper_raw = blk: {
                if (to_key.len == 0) break :blk byte_range.end;
                if (byte_range.end.len == 0 or std.mem.order(u8, to_key, byte_range.end) == .lt) break :blk to_key;
                break :blk byte_range.end;
            };
            const lower = try self.core.documentRangeLowerAlloc(lower_raw);
            defer self.core.alloc.free(lower);
            const upper = if (upper_raw.len > 0) try self.core.documentRangeUpperAlloc(upper_raw) else null;
            defer if (upper) |buf| self.core.alloc.free(buf);

            const docs = try self.core.scanStoreRange(alloc, lower, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(alloc, docs);

            var hashes = std.ArrayListUnmanaged(types.ScanHash).empty;
            errdefer {
                for (hashes.items) |*entry| entry.deinit(alloc);
                hashes.deinit(alloc);
            }
            var documents = std.ArrayListUnmanaged(types.ScanDocument).empty;
            errdefer {
                for (documents.items) |*doc| doc.deinit(alloc);
                documents.deinit(alloc);
            }

            var count: u32 = 0;
            const relational_base_rows = Self.hasRelationalBaseRows(self);
            for (docs) |doc| {
                if (!db_internal.isBaseDocumentStoreKeyForMode(relational_base_rows, doc.key)) continue;
                const raw_key = (try internal_keys.decodeStoredDocumentRowKeyAlloc(alloc, doc.key)) orelse continue;
                defer alloc.free(raw_key);

                if (!byte_range.contains(raw_key)) continue;
                if (from_key.len > 0 and !opts.inclusive_from and std.mem.eql(u8, raw_key, from_key)) continue;
                if (to_key.len > 0) {
                    const ord = std.mem.order(u8, raw_key, to_key);
                    if (opts.exclusive_to) {
                        if (ord != .lt) break;
                    } else {
                        if (ord == .gt) break;
                    }
                }
                if (try Self.isExpiredDocumentKey(self, alloc, raw_key)) continue;

                const doc_json = if (relational_base_rows)
                    try mapper.materializeRelationalRowValueAlloc(alloc, doc.value)
                else
                    try mapper.materializeDocumentValueAlloc(alloc, doc.value);
                defer alloc.free(doc_json);

                const hash = std.hash.Wyhash.hash(0, doc_json);
                try hashes.append(alloc, .{
                    .id = try alloc.dupe(u8, raw_key),
                    .hash = hash,
                });

                if (opts.include_documents) {
                    try documents.append(alloc, .{
                        .id = try alloc.dupe(u8, raw_key),
                        .json = try Self.projectLookupStoredBytes(self, alloc, raw_key, doc_json, .{
                            .fields = opts.fields,
                            .include_all_fields = opts.include_all_fields,
                        }),
                    });
                }

                count += 1;
                if (opts.limit > 0 and count >= opts.limit) break;
            }

            return .{
                .hashes = try hashes.toOwnedSlice(alloc),
                .documents = try documents.toOwnedSlice(alloc),
            };
        }

        pub fn loadStoredSearchDocument(self: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
            const relational_row = Self.hasRelationalBaseRows(self) and
                !Self.isMetadataKey(self, key) and
                !internal_keys.isInternalPhysicalTableDataKey(key);
            const store_key = try encodeBaseDocumentLookupKeyAlloc(self, alloc, key);
            defer alloc.free(store_key);
            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            const raw = txn.get(store_key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            const owned = try alloc.dupe(u8, raw);
            return if (relational_row)
                try mapper.materializeOwnedRelationalRowValueAlloc(alloc, owned)
            else
                try mapper.materializeOwnedDocumentValueAlloc(alloc, owned);
        }

        pub fn loadStoredSearchDocumentMany(self: *DB, alloc: Allocator, keys: []const []const u8) ![]?[]u8 {
            const bench_profile = benchQueryProfileEnabled();
            const total_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var key_ns: u64 = 0;
            var sort_ns: u64 = 0;
            var txn_ns: u64 = 0;
            var get_many_ns: u64 = 0;
            var dupe_ns: u64 = 0;
            const PendingStoredSearchLoad = struct {
                original_index: usize,
                store_key: []u8,
                relational_row: bool,
            };

            const loaded = try alloc.alloc(?[]u8, keys.len);
            errdefer alloc.free(loaded);
            @memset(loaded, null);
            if (keys.len == 0) return loaded;

            var pending = std.ArrayListUnmanaged(PendingStoredSearchLoad).empty;
            defer {
                for (pending.items) |item| alloc.free(item.store_key);
                pending.deinit(alloc);
            }

            errdefer freeOptionalOwnedBytes(alloc, loaded);

            const key_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            const relational_base_rows = Self.hasRelationalBaseRows(self);
            for (keys, 0..) |key, i| {
                try pending.append(alloc, .{
                    .original_index = i,
                    .store_key = try encodeBaseDocumentLookupKeyAlloc(self, alloc, key),
                    .relational_row = relational_base_rows and !Self.isMetadataKey(self, key) and !internal_keys.isInternalPhysicalTableDataKey(key),
                });
            }
            if (bench_profile) key_ns = platform_time.monotonicNs() - key_start_ns;

            const sort_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            std.mem.sort(PendingStoredSearchLoad, pending.items, {}, struct {
                fn lessThan(_: void, lhs: PendingStoredSearchLoad, rhs: PendingStoredSearchLoad) bool {
                    return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
                }
            }.lessThan);
            if (bench_profile) sort_ns = platform_time.monotonicNs() - sort_start_ns;

            const read_keys = try alloc.alloc([]const u8, pending.items.len);
            defer alloc.free(read_keys);
            const read_values = try alloc.alloc(?[]const u8, pending.items.len);
            defer alloc.free(read_values);
            @memset(read_values, null);

            for (pending.items, 0..) |item, i| read_keys[i] = item.store_key;

            const txn_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            if (bench_profile) txn_ns = platform_time.monotonicNs() - txn_start_ns;
            const get_many_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            try txn.getManySorted(read_keys, read_values);
            if (bench_profile) get_many_ns = platform_time.monotonicNs() - get_many_start_ns;

            const dupe_start_ns = if (bench_profile) platform_time.monotonicNs() else 0;
            for (pending.items, 0..) |item, i| {
                const value = read_values[i] orelse continue;
                const owned = try alloc.dupe(u8, value);
                loaded[item.original_index] = if (item.relational_row)
                    try mapper.materializeOwnedRelationalRowValueAlloc(alloc, owned)
                else
                    try mapper.materializeOwnedDocumentValueAlloc(alloc, owned);
            }
            if (bench_profile) dupe_ns = platform_time.monotonicNs() - dupe_start_ns;
            if (bench_profile) {
                std.log.info(
                    "antfly_bench_stored_many total_us={d} key_us={d} sort_us={d} txn_us={d} get_many_us={d} dupe_us={d} keys={d}",
                    .{ (platform_time.monotonicNs() - total_start_ns) / 1000, key_ns / 1000, sort_ns / 1000, txn_ns / 1000, get_many_ns / 1000, dupe_ns / 1000, keys.len },
                );
            }
            return loaded;
        }

        pub fn projectLookupStoredBytes(self: *DB, alloc: Allocator, doc_key: []const u8, raw: []const u8, opts: types.LookupOptions) ![]u8 {
            return try db_query_projection.projectLookupStoredBytes(alloc, doc_key, raw, opts, .{
                .ctx = self,
                .load_chunks = Self.loadChunkFieldValueCallback,
                .load_embeddings = Self.loadEmbeddingFieldValueCallback,
                .load_artifacts = Self.loadArtifactFieldValueCallback,
            });
        }

        pub fn cloneNamedSetAsResult(
            self: *DB,
            alloc: Allocator,
            set: NamedResultSet,
            include_stored: bool,
        ) !types.SearchResult {
            _ = self;
            return try db_query_graph.cloneNamedSetAsResult(alloc, set, include_stored);
        }

        pub fn resolveDocSetDocIds(
            self: *DB,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) !?[]const []const u8 {
            return try self.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, set, generation);
        }

        pub fn resolveDocIdsToDocSet(
            self: *DB,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) !doc_set.ResolvedDocSet {
            return try self.internalResolveDocSetForIdsNoLockAtGenerationAlloc(alloc, doc_ids, generation);
        }

        pub fn resolvedDocFilterForIdsAlloc(
            self: *DB,
            include_positive: bool,
            include_doc_ids: []const []const u8,
            exclude_doc_ids: []const []const u8,
            generation: ?u64,
        ) !doc_set.ResolvedDocFilter {
            return try self.internalResolvedDocFilterForIdsAlloc(include_positive, include_doc_ids, exclude_doc_ids, generation);
        }

        pub fn recordUnsupportedDocSetFilterShape(self: *DB) void {
            self.internalRecordUnsupportedDocSetFilterShape();
        }

        pub fn resolveRelationalFilterDocSet(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            query: search_mod.SearchQuery,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            if (runtime_schema.storage_mode != .relational or runtime_schema.relational_columns.len == 0) return null;
            return try Self.resolveRelationalFilterQueryDocSetAlloc(self, alloc, runtime_schema, query, generation);
        }

        pub fn allDocsVisible(self: *DB, generation: ?u64) !bool {
            return try self.internalAllDocsVisibleAtGeneration(generation);
        }

        pub fn allDocsVisibleFast(self: *DB, generation: ?u64) !bool {
            return try self.internalAllDocsVisibleSummaryFast(generation);
        }

        pub fn denseIndex(self: *DB, index_name: ?[]const u8) ?*index_manager_mod.IndexManager.DenseIndex {
            return self.core.denseIndex(index_name);
        }

        pub fn sparseIndex(self: *DB, index_name: ?[]const u8) ?*index_manager_mod.IndexManager.SparseIndex {
            return self.core.sparseIndex(index_name);
        }

        pub fn denseDocKey(self: *DB, index_name: []const u8, vector_id: u64) !?[]u8 {
            return try self.core.index_manager.lookupDenseDocKey(self.core.store, index_name, vector_id);
        }

        pub fn denseVectorId(self: *DB, index_name: []const u8, doc_key: []const u8) !?u64 {
            return try self.core.index_manager.lookupDenseVectorId(self.core.store, index_name, doc_key);
        }

        pub fn denseVectorIdsForOrdinals(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            ordinals: []const u32,
        ) ![]u64 {
            return try self.core.index_manager.lookupDenseVectorIdsForOrdinalsAlloc(
                alloc,
                self.core.store,
                index_name,
                ordinals,
            );
        }

        pub fn lookupLiveDocOrdinalNoLock(
            self: *DB,
            alloc: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) !?doc_set.DocOrdinal {
            return try self.internalLookupLiveDocOrdinalNoLock(alloc, doc_id, generation);
        }

        pub fn lookupLiveDocOrdinalsNoLock(
            self: *DB,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) ![]?doc_set.DocOrdinal {
            return try self.internalLookupLiveDocOrdinalsNoLock(alloc, doc_ids, generation);
        }

        pub fn sparseDocNumsForOrdinals(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            ordinals: []const u32,
        ) ![]const u32 {
            return try self.core.index_manager.lookupSparseDocNumsForOrdinalsAlloc(alloc, self.core.store, index_name, ordinals);
        }

        pub fn hbcSearch(
            self: *DB,
            entry: *index_manager_mod.IndexManager.DenseIndex,
            req: vectorindex_mod.SearchRequest,
        ) !vectorindex_mod.SearchResults {
            return try self.core.index_manager.searchDenseEntryWithRequest(entry, req);
        }

        pub fn hbcSearchProfiled(
            self: *DB,
            entry: *index_manager_mod.IndexManager.DenseIndex,
            req: vectorindex_mod.SearchRequest,
        ) !vectorindex_mod.ProfiledSearchResults {
            return try self.core.index_manager.searchDenseEntryProfiledWithRequest(entry, req);
        }

        pub fn hasRelationalBaseRows(self: *DB) bool {
            return self.relationalColumnsForStore() != null;
        }

        pub fn isMetadataKey(self: *DB, key: []const u8) bool {
            _ = self;
            return db_internal.isMetadataKey(key);
        }

        pub fn scanStoreRange(
            self: *DB,
            alloc: Allocator,
            lower: []const u8,
            upper: []const u8,
        ) ![]docstore_mod.OwnedKVPair {
            return try self.core.scanStoreRange(alloc, lower, upper);
        }

        pub fn ttlDurationNs(self: *DB) u64 {
            return if (self.core.schema) |schema| schema.ttl_duration_ns else 0;
        }

        pub fn isExpiredDocumentKey(self: *DB, alloc: Allocator, key: []const u8) !bool {
            const duration_ns = Self.ttlDurationNs(self);
            if (duration_ns == 0) return false;
            const ts = try self.getTimestamp(alloc, key);
            if (ts == 0) return false;
            return ttl_mod.isExpired(ts, duration_ns, platform_clock.Clock.real().nowRealtimeNs());
        }

        pub fn loadDocumentTimestampsMany(self: *DB, alloc: Allocator, keys: []const []const u8) ![]u64 {
            const timestamps = try alloc.alloc(u64, keys.len);
            errdefer alloc.free(timestamps);
            @memset(timestamps, 0);
            if (keys.len == 0) return timestamps;

            const PendingTimestampLoad = struct {
                original_index: usize,
                store_key: []u8,
            };

            var pending = std.ArrayListUnmanaged(PendingTimestampLoad).empty;
            defer {
                for (pending.items) |item| alloc.free(item.store_key);
                pending.deinit(alloc);
            }

            for (keys, 0..) |key, i| {
                if (internal_keys.isInternalPhysicalTableDataKey(key)) continue;
                try pending.append(alloc, .{
                    .original_index = i,
                    .store_key = try internal_keys.ttlKeyAlloc(alloc, key),
                });
            }
            if (pending.items.len == 0) return timestamps;

            std.mem.sort(PendingTimestampLoad, pending.items, {}, struct {
                fn lessThan(_: void, lhs: PendingTimestampLoad, rhs: PendingTimestampLoad) bool {
                    return std.mem.order(u8, lhs.store_key, rhs.store_key) == .lt;
                }
            }.lessThan);

            const read_keys = try alloc.alloc([]const u8, pending.items.len);
            defer alloc.free(read_keys);
            const read_values = try alloc.alloc(?[]const u8, pending.items.len);
            defer alloc.free(read_values);
            @memset(read_values, null);

            for (pending.items, 0..) |item, i| read_keys[i] = item.store_key;

            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();
            try txn.getManySorted(read_keys, read_values);

            for (pending.items, 0..) |item, i| {
                const raw = read_values[i] orelse continue;
                if (raw.len != 8) continue;
                timestamps[item.original_index] = std.mem.readInt(u64, raw[0..8], .little);
            }
            return timestamps;
        }

        pub fn annotateSearchHitOrdinalsNoLock(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            hits: []types.SearchHit,
        ) !void {
            try self.internalAnnotateSearchHitOrdinalsNoLock(alloc, req, hits);
        }

        fn encodeBaseDocumentLookupKeyAlloc(self: *DB, alloc: Allocator, key: []const u8) ![]u8 {
            if (Self.hasRelationalBaseRows(self) and !Self.isMetadataKey(self, key) and !internal_keys.isInternalPhysicalTableDataKey(key)) {
                return try relational_store_mod.rowKeyAlloc(alloc, key);
            }
            if (internal_keys.isInternalPhysicalTableDataKey(key) or Self.isMetadataKey(self, key)) {
                return try alloc.dupe(u8, key);
            }
            return try internal_keys.documentKeyAlloc(alloc, key);
        }

        fn encodeStoreLookupKeyAlloc(self: *DB, alloc: Allocator, key: []const u8) ![]u8 {
            if (internal_keys.isInternalPhysicalTableDataKey(key) or Self.isMetadataKey(self, key)) {
                return try alloc.dupe(u8, key);
            }
            return try internal_keys.documentKeyAlloc(alloc, key);
        }

        pub fn filterVisibleSearchHitsMany(self: *DB, alloc: Allocator, hits: []const types.SearchHit) ![]bool {
            const keep = try alloc.alloc(bool, hits.len);
            errdefer alloc.free(keep);
            @memset(keep, true);

            const duration_ns = Self.ttlDurationNs(self);
            if (duration_ns == 0 or hits.len == 0) return keep;

            const expiry_now = platform_time.realtimeNs();
            const parent_ids = try alloc.alloc([]const u8, hits.len);
            defer alloc.free(parent_ids);

            const needs_stored = try alloc.alloc(bool, hits.len);
            defer alloc.free(needs_stored);
            @memset(needs_stored, false);

            const fallback_to_hit = try alloc.alloc(bool, hits.len);
            defer alloc.free(fallback_to_hit);
            @memset(fallback_to_hit, false);

            for (hits, 0..) |hit, i| {
                if (internal_keys.isChunkArtifactRecordKey(hit.id)) {
                    const parent = (try internal_keys.decodeDocumentComponentAlloc(alloc, hit.id)) orelse {
                        fallback_to_hit[i] = true;
                        parent_ids[i] = hit.id;
                        continue;
                    };
                    parent_ids[i] = parent;
                    continue;
                }
                if (hit.stored_data != null) {
                    parent_ids[i] = Self.parseChunkParentIdFromStoredAlloc(alloc, hit.stored_data.?) catch {
                        fallback_to_hit[i] = true;
                        parent_ids[i] = hit.id;
                        continue;
                    };
                    continue;
                }
                needs_stored[i] = true;
                parent_ids[i] = hit.id;
            }
            defer {
                for (hits, 0..) |_, i| {
                    if (!needs_stored[i] and !std.mem.eql(u8, parent_ids[i], hits[i].id)) alloc.free(parent_ids[i]);
                }
            }

            var pending_indices = std.ArrayListUnmanaged(usize).empty;
            defer pending_indices.deinit(alloc);
            for (needs_stored, 0..) |needed, i| if (needed) try pending_indices.append(alloc, i);
            if (pending_indices.items.len > 0) {
                const pending_keys = try alloc.alloc([]const u8, pending_indices.items.len);
                defer alloc.free(pending_keys);
                for (pending_indices.items, 0..) |hit_index, i| pending_keys[i] = hits[hit_index].id;

                const loaded = try Self.loadStoredSearchDocumentMany(self, alloc, pending_keys);
                defer freeOptionalOwnedBytes(alloc, loaded);
                for (pending_indices.items, 0..) |hit_index, i| {
                    const stored = loaded[i] orelse {
                        fallback_to_hit[hit_index] = true;
                        continue;
                    };
                    parent_ids[hit_index] = Self.parseChunkParentIdFromStoredAlloc(alloc, stored) catch {
                        fallback_to_hit[hit_index] = true;
                        continue;
                    };
                }
            }

            const timestamps = try Self.loadDocumentTimestampsMany(self, alloc, parent_ids);
            defer alloc.free(timestamps);
            for (timestamps, 0..) |ts, i| {
                if (ts == 0) continue;
                keep[i] = !ttl_mod.isExpired(ts, duration_ns, expiry_now);
            }
            return keep;
        }

        pub fn filterExpiredSearchResult(self: *DB, alloc: Allocator, raw: types.SearchResult) !types.SearchResult {
            if (Self.ttlDurationNs(self) == 0) return raw;
            return try db_query_result_shape.filterVisibleSearchResult(alloc, raw, .{
                .ctx = self,
                .func = Self.isVisibleSearchHitCallback,
                .filter_many = Self.filterVisibleSearchHitsManyCallback,
            });
        }

        pub fn isVisibleSearchHit(self: *DB, alloc: Allocator, hit: types.SearchHit) !bool {
            return try db_query_result_shape.isVisibleSearchHit(alloc, hit, .{
                .ctx = self,
                .load_stored = Self.loadStoredSearchDocumentCallback,
                .is_expired_key = Self.isExpiredDocumentKeyCallback,
            });
        }

        pub fn isVisibleNonChunkSearchHit(self: *DB, alloc: Allocator, hit: types.SearchHit) !bool {
            return !(try Self.isExpiredDocumentKey(self, alloc, hit.id));
        }

        pub fn reshapeChunkBackedResult(self: *DB, alloc: Allocator, req: types.SearchRequest, raw: types.SearchResult) !types.SearchResult {
            return try db_query_result_shape.reshapeChunkBackedResult(alloc, req, raw, .{
                .ctx = self,
                .resolve_parent_id = Self.resolveChunkParentIdCallback,
                .load_parent_stored = Self.loadParentStoredForSearchCallback,
            });
        }

        pub fn resolveChunkParentId(self: *DB, alloc: Allocator, hit: types.SearchHit) ![]u8 {
            return try db_query_result_shape.resolveChunkParentId(alloc, hit, .{
                .ctx = self,
                .load_stored = Self.loadStoredSearchDocumentCallback,
            });
        }

        fn parseChunkParentIdFromStoredAlloc(alloc: Allocator, stored: []const u8) ![]u8 {
            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidChunkArtifact;
            const parent = parsed.value.object.get("_parent_doc_key") orelse parsed.value.object.get("parent_doc_key") orelse return error.InvalidChunkArtifact;
            if (parent != .string) return error.InvalidChunkArtifact;
            return try alloc.dupe(u8, parent.string);
        }

        pub fn postprocessTextSearchResult(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            raw: types.SearchResult,
            chunk_backed: bool,
        ) !types.SearchResult {
            return try db_query_result_shape.postprocessTextSearchResult(alloc, req, raw, chunk_backed, .{
                .ctx = self,
                .is_visible = Self.isVisibleSearchHitCallback,
                .filter_visible_many = Self.filterVisibleSearchHitsManyCallback,
                .resolve_parent_id = Self.resolveChunkParentIdCallback,
                .load_parent_stored = Self.loadParentStoredForSearchCallback,
                .load_stored = Self.loadStoredSearchDocumentCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .load_many_parent_stored = Self.loadParentStoredForSearchManyCallback,
                .load_many_stored = Self.loadStoredSearchDocumentManyCallback,
            });
        }

        pub fn postprocessVectorSearchResult(
            self: *DB,
            alloc: Allocator,
            req: types.SearchRequest,
            raw: types.SearchResult,
            chunk_backed: bool,
        ) !types.SearchResult {
            return try db_query_result_shape.postprocessVectorSearchResult(alloc, req, raw, chunk_backed, .{
                .ctx = self,
                .is_visible = if (chunk_backed) Self.isVisibleSearchHitCallback else Self.isVisibleNonChunkSearchHitCallback,
                .filter_visible_many = Self.filterVisibleSearchHitsManyCallback,
                .resolve_parent_id = Self.resolveChunkParentIdCallback,
                .load_parent_stored = Self.loadParentStoredForSearchCallback,
                .load_stored = Self.loadStoredSearchDocumentCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .load_many_parent_stored = Self.loadParentStoredForSearchManyCallback,
                .load_many_stored = Self.loadStoredSearchDocumentManyCallback,
            });
        }

        fn textIndexEntryCallback(
            ctx: ?*anyopaque,
            index_name: ?[]const u8,
        ) anyerror!?*index_manager_mod.IndexManager.TextIndex {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return self.core.textIndexEntry(index_name);
        }

        fn resolveStructuredDocFilterForComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!?doc_set.ResolvedDocFilter {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try db_query_search.resolveStructuredDocFilterForComposedAlloc(alloc, req, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .project_ordinals_to_doc_ids = false,
                .identity_read_generation = req.identity_read_generation,
            });
        }

        fn resolveStructuredTextDocFilterForComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!?db_query_search.ResolvedTextDocNumFilter {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try db_query_search.resolveStructuredTextDocNumFilterForComposedAlloc(alloc, req, .{
                .ctx = self,
                .text_index_entry = Self.textIndexEntryCallback,
                .resolve_doc_set_doc_ids = Self.resolveDocSetDocIdsCallback,
                .resolve_doc_ids_to_doc_set = Self.resolveDocIdsToDocSetCallback,
                .resolve_relational_filter_doc_set = Self.resolveRelationalFilterDocSetCallback,
                .live_filter_doc_set = Self.liveFilterDocSetCallback,
                .all_docs_visible = Self.allDocsVisibleCallback,
                .project_ordinals_to_doc_ids = false,
                .identity_read_generation = req.identity_read_generation,
            });
        }

        fn searchTextQueryCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            text_query: types.TextQuery,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchTextQuery(self, alloc, req, text_query);
        }

        fn searchTextComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchText(self, alloc, req);
        }

        fn searchDenseComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            dense: types.DenseKnnQuery,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchDense(self, alloc, req, dense);
        }

        fn searchSparseComposedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            sparse: types.SparseKnnQuery,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchSparse(self, alloc, req, sparse);
        }

        fn cloneNamedSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: NamedResultSet,
            include_stored: bool,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.cloneNamedSetAsResult(self, alloc, set, include_stored);
        }

        fn fuseNamedSetsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            named_sets: []const NamedResultSet,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.fuseNamedSets(self, alloc, req, named_sets);
        }

        fn resolveSearchHitsToDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            hits: []const types.SearchHit,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeResolveSearchHitsToDocSet(alloc, req, hits);
        }

        fn attachGraphResultsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            base: *types.SearchResult,
            named_sets: []const NamedResultSet,
        ) anyerror!void {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            base.graph_results = try Self.executeGraphQueriesWithSets(self, alloc, req, req.graph_queries, named_sets);
            try Self.applyGraphExpandStrategy(self, alloc, base, req.expand_strategy);
        }

        fn executeSingleGraphQueryWithSetsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            named: *const types.NamedGraphQuery,
            named_sets: []const NamedResultSet,
        ) anyerror!types.GraphSearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.executeSingleGraphQueryWithSets(self, alloc, req, named, named_sets);
        }

        fn resolveDocSetDocIdsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[]const []const u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.resolveDocSetDocIds(self, alloc, set, generation);
        }

        fn resolveDocSetDocIdsForGraphCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!?[][]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const ids = (try Self.resolveDocSetDocIds(self, alloc, set, generation)) orelse return null;
            defer alloc.free(@constCast(ids));
            errdefer for (ids) |id| alloc.free(@constCast(id));

            const out = try alloc.alloc([]u8, ids.len);
            for (ids, 0..) |id, i| out[i] = @constCast(id);
            return out;
        }

        fn resolveGraphNodesToDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            nodes: []const graph_query_mod.GraphResultNode,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeResolveGraphNodesToDocSet(alloc, req, nodes);
        }

        fn resolveDocIdsToDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.resolveDocIdsToDocSet(self, alloc, doc_ids, generation);
        }

        fn resolveRelationalFilterDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            query: search_mod.SearchQuery,
            generation: ?u64,
        ) anyerror!?doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.resolveRelationalFilterDocSet(self, alloc, runtime_schema, query, generation);
        }

        fn liveFilterDocSetCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            set: *const doc_set.ResolvedDocSet,
            generation: ?u64,
        ) anyerror!doc_set.ResolvedDocSet {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLiveFilterDocSet(alloc, set, generation);
        }

        fn allDocsVisibleCallback(
            ctx: ?*anyopaque,
            generation: ?u64,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.allDocsVisible(self, generation);
        }

        fn textIndexIsChunkBackedCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: ?[]const u8,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeTextIndexIsChunkBacked(alloc, index_name);
        }

        fn searchMatchAllCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.searchMatchAll(self, alloc, req);
        }

        fn projectStoredBytesForSearchCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            doc_key: []const u8,
            raw: []const u8,
        ) anyerror![]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.projectStoredBytesForSearch(self, alloc, req, doc_key, raw);
        }

        fn loadProjectedSearchDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            key: []const u8,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadProjectedSearchDocument(self, alloc, req, key);
        }

        fn postprocessTextSearchResultCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            raw: types.SearchResult,
            chunk_backed: bool,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.postprocessTextSearchResult(self, alloc, req, raw, chunk_backed);
        }

        fn denseIndexCallback(
            ctx: ?*anyopaque,
            index_name: ?[]const u8,
        ) anyerror!?*index_manager_mod.IndexManager.DenseIndex {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return Self.denseIndex(self, index_name);
        }

        fn sparseIndexCallback(
            ctx: ?*anyopaque,
            index_name: ?[]const u8,
        ) anyerror!?*index_manager_mod.IndexManager.SparseIndex {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return Self.sparseIndex(self, index_name);
        }

        fn denseDocKeyCallback(
            ctx: ?*anyopaque,
            index_name: []const u8,
            vector_id: u64,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.denseDocKey(self, index_name, vector_id);
        }

        fn denseVectorIdCallback(
            ctx: ?*anyopaque,
            index_name: []const u8,
            doc_key: []const u8,
        ) anyerror!?u64 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.denseVectorId(self, index_name, doc_key);
        }

        fn denseVectorIdsForOrdinalsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: []const u8,
            ordinals: []const u32,
        ) anyerror![]u64 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.denseVectorIdsForOrdinals(self, alloc, index_name, ordinals);
        }

        fn allDocsVisibleFastCallback(
            ctx: ?*anyopaque,
            generation: ?u64,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.allDocsVisibleFast(self, generation);
        }

        fn lookupLiveDocOrdinalNoLockCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLookupLiveDocOrdinalNoLock(alloc, doc_id, generation);
        }

        fn lookupLiveDocOrdinalsNoLockCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_ids: []const []const u8,
            generation: ?u64,
        ) anyerror![]?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLookupLiveDocOrdinalsNoLock(alloc, doc_ids, generation);
        }

        fn denseOrdinalsForVectorIdsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: []const u8,
            vector_ids: []const u64,
            generation: ?u64,
        ) anyerror![]?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeDenseOrdinalsForVectorIds(alloc, index_name, vector_ids, generation);
        }

        fn sparseDocNumsForOrdinalsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            index_name: []const u8,
            ordinals: []const u32,
        ) anyerror![]const u32 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.sparseDocNumsForOrdinals(self, alloc, index_name, ordinals);
        }

        fn loadRequiredProjectedSearchDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            key: []const u8,
        ) anyerror![]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadRequiredProjectedSearchDocument(self, alloc, req, key);
        }

        fn loadProjectedSearchDocumentManyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            keys: []const []const u8,
        ) anyerror![]?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadProjectedSearchDocumentMany(self, alloc, req, keys);
        }

        fn loadChunkFieldValueCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_key: []const u8,
        ) anyerror!?std.json.Value {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadChunkFieldValue(alloc, doc_key);
        }

        fn loadEmbeddingFieldValueCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_key: []const u8,
        ) anyerror!?std.json.Value {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadEmbeddingFieldValue(alloc, doc_key);
        }

        fn loadArtifactFieldValueCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_key: []const u8,
        ) anyerror!?std.json.Value {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLoadArtifactFieldValue(alloc, doc_key);
        }

        fn isVisibleSearchHitCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            hit: types.SearchHit,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.isVisibleSearchHit(self, alloc, hit);
        }

        fn filterVisibleSearchHitsManyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            hits: []const types.SearchHit,
        ) anyerror![]bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.filterVisibleSearchHitsMany(self, alloc, hits);
        }

        fn isVisibleNonChunkSearchHitCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            hit: types.SearchHit,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.isVisibleNonChunkSearchHit(self, alloc, hit);
        }

        fn resolveChunkParentIdCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            hit: types.SearchHit,
        ) anyerror![]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.resolveChunkParentId(self, alloc, hit);
        }

        fn loadParentStoredForSearchCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            parent_id: []const u8,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadParentStoredForSearch(self, alloc, req, parent_id);
        }

        fn loadParentStoredForSearchManyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            parent_ids: []const []const u8,
        ) anyerror![]?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadParentStoredForSearchMany(self, alloc, req, parent_ids);
        }

        fn postprocessVectorSearchResultCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
            raw: types.SearchResult,
            chunk_backed: bool,
        ) anyerror!types.SearchResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.postprocessVectorSearchResult(self, alloc, req, raw, chunk_backed);
        }

        fn hbcSearchCallback(
            ctx: ?*anyopaque,
            entry: *index_manager_mod.IndexManager.DenseIndex,
            req: vectorindex_mod.SearchRequest,
        ) anyerror!vectorindex_mod.SearchResults {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.hbcSearch(self, entry, req);
        }

        fn hbcSearchProfiledCallback(
            ctx: ?*anyopaque,
            entry: *index_manager_mod.IndexManager.DenseIndex,
            req: vectorindex_mod.SearchRequest,
        ) anyerror!vectorindex_mod.ProfiledSearchResults {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.hbcSearchProfiled(self, entry, req);
        }

        fn collectSearchMatchAllCandidatesCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            req: types.SearchRequest,
        ) anyerror!db_query_search.MatchAllCandidates {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try db_query_search.collectMatchAllCandidates(alloc, req, .{
                .ctx = self,
                .relational_base_rows = Self.hasRelationalBaseRows(self),
                .scan_store_range = Self.scanStoreRangeCallback,
                .is_expired_key = Self.isExpiredDocumentKeyCallback,
                .lookup_doc_ordinal = Self.lookupLiveDocOrdinalCallback,
            });
        }

        fn scanStoreRangeCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            lower: []const u8,
            upper: []const u8,
        ) anyerror![]docstore_mod.OwnedKVPair {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.scanStoreRange(self, alloc, lower, upper);
        }

        fn isExpiredDocumentKeyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            key: []const u8,
        ) anyerror!bool {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.isExpiredDocumentKey(self, alloc, key);
        }

        fn lookupLiveDocOrdinalCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            doc_id: []const u8,
            generation: ?u64,
        ) anyerror!?doc_set.DocOrdinal {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try self.searchRuntimeLookupLiveDocOrdinal(alloc, doc_id, generation);
        }

        fn loadStoredSearchDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            key: []const u8,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadStoredSearchDocument(self, alloc, key);
        }

        fn loadStoredSearchDocumentManyCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            keys: []const []const u8,
        ) anyerror![]?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadStoredSearchDocumentMany(self, alloc, keys);
        }

        fn executePatternMatchCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
        ) anyerror![]graph_pattern_mod.PatternMatch {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.matchNamedPattern(self, alloc, named, start_key_refs);
        }

        fn loadPatternProjectedDocumentCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            query: graph_query_mod.GraphQuery,
            key: []const u8,
        ) anyerror!?[]u8 {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.loadPatternProjectedDocument(self, alloc, query, key);
        }

        fn executeShortestPathCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            source: []const u8,
            target: []const u8,
        ) anyerror!?types.GraphPath {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.findShortestPath(
                self,
                alloc,
                named.query.index_name,
                source,
                target,
                named.query.params.edge_types,
                named.query.params.direction,
                named.query.params.weight_mode,
                named.query.params.max_depth,
                named.query.params.min_weight,
                named.query.params.max_weight,
            );
        }

        fn executeKShortestPathsCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            source: []const u8,
            target: []const u8,
        ) anyerror![]types.GraphPath {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.findKShortestPaths(
                self,
                alloc,
                named.query.index_name,
                source,
                target,
                named.query.k,
                named.query.params.edge_types,
                named.query.params.direction,
                named.query.params.weight_mode,
                named.query.params.max_depth,
                named.query.params.min_weight,
                named.query.params.max_weight,
            );
        }

        fn executeGraphQueryCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            named: *const types.NamedGraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.executeSearchGraphQuery(self, alloc, named.query, start_key_refs, target_keys);
        }

        fn executeSearchGraphQueryCallback(
            ctx: ?*anyopaque,
            alloc: Allocator,
            graph_query: graph_query_mod.GraphQuery,
            start_key_refs: []const []const u8,
            target_keys: [][]u8,
        ) anyerror!graph_query_mod.GraphQueryResult {
            const self: *DB = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            return try Self.executeSearchGraphQuery(self, alloc, graph_query, start_key_refs, target_keys);
        }
    };
}

test "db search runtime reopen reopens persisted index catalog and text index" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"second body\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try std.testing.expectEqual(@as(u32, 1), db.core.index_manager.count());
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        try std.testing.expectEqual(@as(u32, 1), reopened.core.index_manager.count());

        var result = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    }
}

test "db search runtime reopen delete index persists across reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try std.testing.expect(try db.deleteIndex("ft_v1"));
        try std.testing.expectEqual(@as(u32, 0), db.core.index_manager.count());
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        try std.testing.expectEqual(@as(u32, 0), reopened.core.index_manager.count());
        try std.testing.expectError(error.IndexNotFound, reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .term = .{ .field = "title", .term = "alpha" } },
            .limit = 10,
        }));
    }
}

test "db search runtime reopen delete index persists across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const primary_backend: db_config.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try std.testing.expect(try db.deleteIndex("ft_v1"));
        try std.testing.expectEqual(@as(u32, 0), db.core.index_manager.count());
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer reopened.close();

        try std.testing.expectEqual(@as(u32, 0), reopened.core.index_manager.count());
        try std.testing.expectError(error.IndexNotFound, reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .term = .{ .field = "title", .term = "alpha" } },
            .limit = 10,
        }));
    }
}

test "db search runtime reopen indexed delete removes hits across reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"second alpha\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .deletes = &.{"doc:a"},
            .sync_level = .full_index,
        });

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }
}

test "db search runtime reopen indexed delete removes hits across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const primary_backend: db_config.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"second alpha\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .deletes = &.{"doc:a"},
            .sync_level = .full_index,
        });

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }
}

test "db search runtime reopen indexed overwrite replaces old hits across reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"replaced body\"}" },
            },
            .sync_level = .full_index,
        });

        var alpha = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer alpha.deinit();
        try std.testing.expectEqual(@as(u32, 0), alpha.total_hits);

        var replaced = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "replaced" } },
            .limit = 10,
        });
        defer replaced.deinit();
        try std.testing.expectEqual(@as(u32, 1), replaced.total_hits);
        try std.testing.expectEqualStrings("doc:a", replaced.hits[0].id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        var alpha = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer alpha.deinit();
        try std.testing.expectEqual(@as(u32, 0), alpha.total_hits);

        var replaced = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "replaced" } },
            .limit = 10,
        });
        defer replaced.deinit();
        try std.testing.expectEqual(@as(u32, 1), replaced.total_hits);
        try std.testing.expectEqualStrings("doc:a", replaced.hits[0].id);
    }
}

test "db search runtime reopen indexed overwrite replaces old hits across reopen with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const primary_backend: db_config.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first alpha\"}" },
            },
        });

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"replaced body\"}" },
            },
            .sync_level = .full_index,
        });

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 0), result.total_hits);

        var replaced = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "replaced" } },
            .limit = 10,
        });
        defer replaced.deinit();
        try std.testing.expectEqual(@as(u32, 1), replaced.total_hits);
        try std.testing.expectEqualStrings("doc:a", replaced.hits[0].id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 0), result.total_hits);

        var replaced = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "replaced" } },
            .limit = 10,
        });
        defer replaced.deinit();
        try std.testing.expectEqual(@as(u32, 1), replaced.total_hits);
        try std.testing.expectEqualStrings("doc:a", replaced.hits[0].id);
    }
}

test "db search runtime reopen phrase query survives text compaction deletes and reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        for (0..6) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
            defer alloc.free(key);

            const body = switch (i) {
                0 => "alpha beta",
                1 => "alpha beta gamma",
                2 => "alpha only",
                3 => "beta only",
                4 => "alpha beta delta",
                else => "gamma delta",
            };
            const value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"both\",\"body\":\"{s}\"}}",
                .{body},
            );
            defer alloc.free(value);

            try db.batch(.{
                .writes = &.{.{
                    .key = key,
                    .value = value,
                }},
                .sync_level = .full_index,
            });
        }

        try db.batch(.{
            .deletes = &.{"doc:1"},
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expect(text_index.snapshot().segments.len < 6);

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match_phrase = .{ .field = "body", .text = "alpha beta" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 2), result.total_hits);
        try std.testing.expect(
            (std.mem.eql(u8, result.hits[0].id, "doc:0") and std.mem.eql(u8, result.hits[1].id, "doc:4")) or
                (std.mem.eql(u8, result.hits[0].id, "doc:4") and std.mem.eql(u8, result.hits[1].id, "doc:0")),
        );
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match_phrase = .{ .field = "body", .text = "alpha beta" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 2), result.total_hits);
        try std.testing.expect(
            (std.mem.eql(u8, result.hits[0].id, "doc:0") and std.mem.eql(u8, result.hits[1].id, "doc:4")) or
                (std.mem.eql(u8, result.hits[0].id, "doc:4") and std.mem.eql(u8, result.hits[1].id, "doc:0")),
        );
    }
}

test "db search runtime reopen prefix wildcard and regexp queries use text dictionary filters" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:0", .value = "{\"title\":\"small\",\"body\":\"small smart\"}" },
            .{ .key = "doc:1", .value = "{\"title\":\"smile\",\"body\":\"smile\"}" },
            .{ .key = "doc:2", .value = "{\"title\":\"alpha\",\"body\":\"alpha beta\"}" },
        },
        .sync_level = .full_index,
    });

    var prefix = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .prefix = .{ .field = "title", .prefix = "sm" } },
        .limit = 10,
    });
    defer prefix.deinit();
    try std.testing.expectEqual(@as(u32, 2), prefix.total_hits);

    var wildcard = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .wildcard = .{ .field = "title", .pattern = "smi*" } },
        .limit = 10,
    });
    defer wildcard.deinit();
    try std.testing.expectEqual(@as(u32, 1), wildcard.total_hits);
    try std.testing.expectEqualStrings("doc:1", wildcard.hits[0].id);

    var regexp = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .regexp = .{ .field = "title", .pattern = "sm(a|i).*" } },
        .limit = 10,
    });
    defer regexp.deinit();
    try std.testing.expectEqual(@as(u32, 2), regexp.total_hits);
}

test "db search runtime reopen typed and dictionary queries survive compaction delete and reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        const jan3 = (try db_internal.parseRfc3339ToNs("2026-01-03T00:00:00Z")).?;
        const jan5 = (try db_internal.parseRfc3339ToNs("2026-01-05T00:00:00Z")).?;

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        const docs = [_]struct { key: []const u8, value: []const u8 }{
            .{ .key = "doc:0", .value = "{\"title\":\"smal\",\"body\":\"alpha zero\",\"price\":10,\"published_at\":\"2026-01-01T00:00:00Z\",\"location\":{\"lat\":37.7749,\"lon\":-122.4194}}" },
            .{ .key = "doc:1", .value = "{\"title\":\"small\",\"body\":\"alpha one\",\"price\":20,\"published_at\":\"2026-01-02T00:00:00Z\",\"location\":{\"lat\":37.7750,\"lon\":-122.4195}}" },
            .{ .key = "doc:2", .value = "{\"title\":\"smile\",\"body\":\"alpha two\",\"price\":30,\"published_at\":\"2026-01-03T00:00:00Z\",\"location\":{\"lat\":40.7128,\"lon\":-74.0060}}" },
            .{ .key = "doc:3", .value = "{\"title\":\"beta\",\"body\":\"alpha three\",\"price\":40,\"published_at\":\"2026-01-04T00:00:00Z\",\"location\":{\"lat\":34.0522,\"lon\":-118.2437}}" },
            .{ .key = "doc:4", .value = "{\"title\":\"gamma\",\"body\":\"alpha four\",\"price\":50,\"published_at\":\"2026-01-05T00:00:00Z\",\"location\":{\"lat\":47.6062,\"lon\":-122.3321}}" },
            .{ .key = "doc:5", .value = "{\"title\":\"delta\",\"body\":\"alpha five\",\"price\":60,\"published_at\":\"2026-01-06T00:00:00Z\",\"location\":{\"lat\":41.8781,\"lon\":-87.6298}}" },
        };

        for (docs) |doc| {
            try db.batch(.{
                .writes = &.{.{
                    .key = doc.key,
                    .value = doc.value,
                }},
                .sync_level = .full_index,
            });
        }

        try db.batch(.{
            .deletes = &.{"doc:1"},
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expect(text_index.snapshot().segments.len < docs.len);

        var fuzzy = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .fuzzy = .{ .field = "title", .term = "small", .max_edits = 1 } },
            .limit = 10,
        });
        defer fuzzy.deinit();
        try std.testing.expectEqual(@as(u32, 1), fuzzy.total_hits);
        try std.testing.expectEqualStrings("doc:0", fuzzy.hits[0].id);

        var numeric = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .numeric_range = .{
                .field = "price",
                .min = 15,
                .max = 35,
                .inclusive_min = true,
                .inclusive_max = false,
            } },
            .limit = 10,
        });
        defer numeric.deinit();
        try std.testing.expectEqual(@as(u32, 1), numeric.total_hits);
        try std.testing.expectEqualStrings("doc:2", numeric.hits[0].id);

        var date = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .date_range = .{
                .field = "published_at",
                .start_ns = jan3,
                .end_ns = jan5,
                .inclusive_start = true,
                .inclusive_end = false,
            } },
            .limit = 10,
        });
        defer date.deinit();
        try std.testing.expectEqual(@as(u32, 2), date.total_hits);
        try std.testing.expect(
            (std.mem.eql(u8, date.hits[0].id, "doc:2") and std.mem.eql(u8, date.hits[1].id, "doc:3")) or
                (std.mem.eql(u8, date.hits[0].id, "doc:3") and std.mem.eql(u8, date.hits[1].id, "doc:2")),
        );

        var bbox = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .geo_bbox = .{
                .field = "location",
                .min_lat = 37.70,
                .min_lon = -122.50,
                .max_lat = 37.80,
                .max_lon = -122.40,
            } },
            .limit = 10,
        });
        defer bbox.deinit();
        try std.testing.expectEqual(@as(u32, 1), bbox.total_hits);
        try std.testing.expectEqualStrings("doc:0", bbox.hits[0].id);

        var ids = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .doc_id = .{ .ids = &.{ "doc:0", "doc:1", "doc:4" } } },
            .limit = 10,
        });
        defer ids.deinit();
        try std.testing.expectEqual(@as(u32, 2), ids.total_hits);
        try std.testing.expect(
            (std.mem.eql(u8, ids.hits[0].id, "doc:0") and std.mem.eql(u8, ids.hits[1].id, "doc:4")) or
                (std.mem.eql(u8, ids.hits[0].id, "doc:4") and std.mem.eql(u8, ids.hits[1].id, "doc:0")),
        );
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();
        const jan3 = (try db_internal.parseRfc3339ToNs("2026-01-03T00:00:00Z")).?;
        const jan5 = (try db_internal.parseRfc3339ToNs("2026-01-05T00:00:00Z")).?;

        var fuzzy = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .fuzzy = .{ .field = "title", .term = "small", .max_edits = 1 } },
            .limit = 10,
        });
        defer fuzzy.deinit();
        try std.testing.expectEqual(@as(u32, 1), fuzzy.total_hits);
        try std.testing.expectEqualStrings("doc:0", fuzzy.hits[0].id);

        var numeric = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .numeric_range = .{
                .field = "price",
                .min = 15,
                .max = 35,
                .inclusive_min = true,
                .inclusive_max = false,
            } },
            .limit = 10,
        });
        defer numeric.deinit();
        try std.testing.expectEqual(@as(u32, 1), numeric.total_hits);
        try std.testing.expectEqualStrings("doc:2", numeric.hits[0].id);

        var date = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .date_range = .{
                .field = "published_at",
                .start_ns = jan3,
                .end_ns = jan5,
                .inclusive_start = true,
                .inclusive_end = false,
            } },
            .limit = 10,
        });
        defer date.deinit();
        try std.testing.expectEqual(@as(u32, 2), date.total_hits);
        try std.testing.expect(
            (std.mem.eql(u8, date.hits[0].id, "doc:2") and std.mem.eql(u8, date.hits[1].id, "doc:3")) or
                (std.mem.eql(u8, date.hits[0].id, "doc:3") and std.mem.eql(u8, date.hits[1].id, "doc:2")),
        );

        var bbox = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .geo_bbox = .{
                .field = "location",
                .min_lat = 37.70,
                .min_lon = -122.50,
                .max_lat = 37.80,
                .max_lon = -122.40,
            } },
            .limit = 10,
        });
        defer bbox.deinit();
        try std.testing.expectEqual(@as(u32, 1), bbox.total_hits);
        try std.testing.expectEqualStrings("doc:0", bbox.hits[0].id);

        var ids = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .doc_id = .{ .ids = &.{ "doc:0", "doc:1", "doc:4" } } },
            .limit = 10,
        });
        defer ids.deinit();
        try std.testing.expectEqual(@as(u32, 2), ids.total_hits);
        try std.testing.expect(
            (std.mem.eql(u8, ids.hits[0].id, "doc:0") and std.mem.eql(u8, ids.hits[1].id, "doc:4")) or
                (std.mem.eql(u8, ids.hits[0].id, "doc:4") and std.mem.eql(u8, ids.hits[1].id, "doc:0")),
        );
    }
}

test "db search runtime reopen mixed-type stored fields survive text compaction and reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        const docs = [_]struct { key: []const u8, value: []const u8 }{
            .{ .key = "doc:0", .value = "{\"title\":\"alpha\",\"mixed_scalar\":\"zulu\"}" },
            .{ .key = "doc:1", .value = "{\"title\":\"alpha\",\"mixed_scalar\":5}" },
            .{ .key = "doc:2", .value = "{\"title\":\"alpha\",\"mixed_scalar\":true}" },
            .{ .key = "doc:3", .value = "{\"title\":\"alpha\",\"mixed_scalar\":[\"bravo\"]}" },
            .{ .key = "doc:4", .value = "{\"title\":\"alpha\",\"mixed_scalar\":{\"priority\":1}}" },
            .{ .key = "doc:5", .value = "{\"title\":\"alpha\",\"mixed_scalar\":\"charlie\"}" },
        };

        for (docs) |doc| {
            try db.batch(.{
                .writes = &.{.{
                    .key = doc.key,
                    .value = doc.value,
                }},
                .sync_level = .full_index,
            });
        }

        try db.batch(.{
            .deletes = &.{"doc:5"},
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expect(text_index.snapshot().segments.len < docs.len);

        var match = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "title", .text = "alpha", .analyzer = null } },
            .limit = 10,
        });
        defer match.deinit();
        try std.testing.expectEqual(@as(u32, 5), match.total_hits);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        var match = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "title", .text = "alpha", .analyzer = null } },
            .limit = 10,
        });
        defer match.deinit();
        try std.testing.expectEqual(@as(u32, 5), match.total_hits);
    }
}

test "db search runtime reopen persists byte range across reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.updateRange(.{ .start = "doc:b", .end = "doc:m" });
        const byte_range = db.getRange();
        try std.testing.expectEqualStrings("doc:b", byte_range.start);
        try std.testing.expectEqualStrings("doc:m", byte_range.end);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const byte_range = db.getRange();
        try std.testing.expectEqualStrings("doc:b", byte_range.start);
        try std.testing.expectEqualStrings("doc:m", byte_range.end);
        try std.testing.expectError(error.KeyOutOfRange, db.batch(.{
            .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"outside\"}" }},
        }));
        try db.batchWithoutRangeValidation(.{
            .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"outside\"}" }},
        });
        const outside = (try db.get(alloc, "doc:z")).?;
        defer alloc.free(outside);
        try std.testing.expectEqualStrings("{\"title\":\"outside\"}", outside);
    }
}

test "db search runtime reopen updateRange constrains index backfill" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const doc_a_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_key);
    const doc_b_key = try internal_keys.documentKeyAlloc(alloc, "doc:b");
    defer alloc.free(doc_b_key);
    const doc_c_key = try internal_keys.documentKeyAlloc(alloc, "doc:c");
    defer alloc.free(doc_c_key);
    try db.core.store.put(doc_a_key, "{\"title\":\"alpha\"}");
    try db.core.store.put(doc_b_key, "{\"title\":\"beta\"}");
    try db.core.store.put(doc_c_key, "{\"title\":\"gamma\"}");

    try db.updateRange(.{ .start = "doc:b", .end = "doc:c\xff" });
    try db.addIndex(.{
        .name = "ft_range",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });

    const text_index = db.core.index_manager.textIndex("ft_range").?;
    try std.testing.expectEqual(@as(u32, 2), text_index.snapshot().global_doc_count);
}

test "db search runtime projection lookup projects nested document fields" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"title":"alpha","author":{"name":"ann","age":42},"tags":[{"name":"db","score":1},{"name":"zig","score":2}],"body":"hello"}
            ,
        }},
    });

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{ "title", "author.name", "tags.name", "-body" },
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("alpha", parsed.value.object.get("title").?.string);
    try std.testing.expect(parsed.value.object.get("body") == null);
    try std.testing.expectEqualStrings("ann", parsed.value.object.get("author").?.object.get("name").?.string);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("tags").?.array.items.len);
}

test "db search runtime projection lookup returns full stored json by default" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const raw = "{\"title\":\"alpha\",\"body\":\"hello\"}";
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = raw }},
    });

    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings(raw, result.json);
}

test "db search runtime projection search projects stored fields for hydrated hits" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value =
            \\{"title":"alpha","author":{"name":"ann","age":42},"body":"hello world"}
            ,
        }},
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .fields = &.{ "author.name", "-body" },
        .include_all_fields = false,
        .include_stored = true,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expect(result.hits[0].stored_data != null);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.hits[0].stored_data.?, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("body") == null);
    try std.testing.expectEqualStrings("ann", parsed.value.object.get("author").?.object.get("name").?.string);
}

test "db search runtime projection lookup includes chunk artifacts when _chunks is requested" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
    });

    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0,\"_start_offset\":0,\"_end_offset\":5}");
    try db.core.store.put(chunk_one, "{\"body\":\"world\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":1,\"_start_offset\":6,\"_end_offset\":11}");

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{ "title", "_chunks", "-_chunks.*._artifact_name" },
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("alpha", parsed.value.object.get("title").?.string);
    const chunks = parsed.value.object.get("_chunks").?.object.get("body_chunks_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    try std.testing.expectEqual(@as(i64, 0), chunks[0].object.get("_chunk_id").?.integer);
    try std.testing.expectEqual(@as(i64, 0), chunks[0].object.get("_id").?.integer);
    try std.testing.expectEqual(@as(i64, 0), chunks[0].object.get("_start_char").?.integer);
    try std.testing.expectEqual(@as(i64, 5), chunks[0].object.get("_end_char").?.integer);
    try std.testing.expectEqualStrings("hello", chunks[0].object.get("body").?.string);
    try std.testing.expectEqualStrings("hello", chunks[0].object.get("_content").?.string);
    try std.testing.expect(chunks[0].object.get("_artifact_name") == null);
    try std.testing.expectEqual(@as(i64, 1), chunks[1].object.get("_chunk_id").?.integer);
}

test "db search runtime projection lookup includes unified artifact projection when _artifacts is requested" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const putDenseEmbeddingArtifactForTest = db_test_support.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
    });
    try db.addEnrichment(.{
        .name = "page_ocr_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "text/plain",
    });
    try db.addEnrichment(.{
        .name = "entities_v1",
        .kind = .asset,
        .field = "body",
        .content_type = "application/json",
    });

    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0,\"_start_offset\":0,\"_end_offset\":5}");
    try db.core.store.put(chunk_one, "{\"body\":\"world\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":1,\"_start_offset\":6,\"_end_offset\":11}");

    const dense_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 2, 3 });

    var ocr_ref = types.ArtifactRef{
        .document_id = try alloc.dupe(u8, "doc:a"),
        .name = try alloc.dupe(u8, "page_ocr_v1"),
        .kind = .asset,
    };
    defer ocr_ref.deinit(alloc);
    const ocr_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, ocr_ref);
    defer alloc.free(ocr_key);
    try db.core.store.put(ocr_key, "Revenue increased");

    var entities_ref = types.ArtifactRef{
        .document_id = try alloc.dupe(u8, "doc:a"),
        .name = try alloc.dupe(u8, "entities_v1"),
        .kind = .asset,
    };
    defer entities_ref.deinit(alloc);
    const entities_key = try artifact_ids.internalKeyForArtifactRefAlloc(alloc, entities_ref);
    defer alloc.free(entities_key);
    try db.core.store.put(entities_key, "{\"entities\":[{\"text\":\"Antfly\"}],\"relations\":[]}");

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{ "title", "_artifacts" },
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("alpha", parsed.value.object.get("title").?.string);
    const artifacts = parsed.value.object.get("_artifacts").?.object;

    const chunks = artifacts.get("body_chunks_v1").?.object;
    try std.testing.expectEqualStrings("chunk_set", chunks.get("kind").?.string);
    const items = chunks.get("items").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), items.len);
    try std.testing.expect(std.mem.startsWith(u8, items[0].object.get("artifact_id").?.string, "af1:chunk:"));
    try std.testing.expectEqualStrings("chunk", items[0].object.get("artifact_ref").?.object.get("kind").?.string);
    try std.testing.expectEqual(@as(i64, 0), items[0].object.get("artifact_ref").?.object.get("chunk_id").?.integer);
    try std.testing.expectEqualStrings("hello", items[0].object.get("value").?.object.get("_content").?.string);

    const embedding = artifacts.get("body_dense_v1").?.object;
    try std.testing.expect(std.mem.startsWith(u8, embedding.get("artifact_id").?.string, "af1:embedding:"));
    try std.testing.expectEqualStrings("embedding", embedding.get("kind").?.string);
    try std.testing.expectEqualStrings("application/vnd.antfly.embedding+binary", embedding.get("content_type").?.string);
    try std.testing.expectEqual(@as(i64, 3), embedding.get("dims").?.integer);
    try std.testing.expect(embedding.get("value").? == .null);

    const ocr = artifacts.get("page_ocr_v1").?.object;
    try std.testing.expect(std.mem.startsWith(u8, ocr.get("artifact_id").?.string, "af1:asset:"));
    try std.testing.expectEqualStrings("asset", ocr.get("artifact_ref").?.object.get("kind").?.string);
    try std.testing.expectEqualStrings("asset", ocr.get("kind").?.string);
    try std.testing.expectEqualStrings("text/plain", ocr.get("content_type").?.string);
    try std.testing.expectEqualStrings("Revenue increased", ocr.get("value").?.string);

    const entities = artifacts.get("entities_v1").?.object;
    try std.testing.expectEqualStrings("application/json", entities.get("content_type").?.string);
    try std.testing.expectEqualStrings("Antfly", entities.get("value").?.object.get("entities").?.array.items[0].object.get("text").?.string);
}

test "db search runtime projection search includes chunk artifacts on hydrated hits when _chunks is requested" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
    });

    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0}");

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .fields = &.{"_chunks"},
        .include_all_fields = false,
        .include_stored = true,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expect(result.hits[0].stored_data != null);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.hits[0].stored_data.?, .{});
    defer parsed.deinit();
    const chunks = parsed.value.object.get("_chunks").?.object.get("body_chunks_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), chunks.len);
    try std.testing.expectEqualStrings("hello", chunks[0].object.get("body").?.string);
}

test "db search runtime projection scan includes chunk artifacts when _chunks is requested" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0}");

    var scan = try db.scan(alloc, "", "", .{
        .include_documents = true,
        .fields = &.{ "_chunks", "-_chunks.*._artifact_name" },
        .include_all_fields = false,
    });
    defer scan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), scan.documents.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, scan.documents[0].json, .{});
    defer parsed.deinit();
    const chunks = parsed.value.object.get("_chunks").?.object.get("body_chunks_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), chunks.len);
    try std.testing.expect(chunks[0].object.get("_artifact_name") == null);
}

test "db search runtime projection lookup does not load chunks for nested _chunks projection without explicit include-all selector" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0}");

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{"_chunks.body_chunks_v1.0._content"},
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("_chunks") == null);
}

test "db search runtime projection lookup loads chunks for _chunks.* selector and allows nested projection" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const chunk_zero = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try db.core.store.put(chunk_zero, "{\"body\":\"hello\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0,\"_start_offset\":0,\"_end_offset\":5}");

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{ "_chunks.*", "_chunks.body_chunks_v1.0._content", "-_chunks.*._artifact_name" },
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();
    const chunks = parsed.value.object.get("_chunks").?.object.get("body_chunks_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), chunks.len);
    try std.testing.expectEqualStrings("hello", chunks[0].object.get("_content").?.string);
    try std.testing.expect(chunks[0].object.get("_artifact_name") == null);
}

test "db search runtime projection lookup includes embedding artifacts when _embeddings is requested" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const putDenseEmbeddingArtifactForTest = db_test_support.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const dense_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 2, 3 });

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{ "title", "_embeddings" },
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("alpha", parsed.value.object.get("title").?.string);
    const vector = parsed.value.object.get("_embeddings").?.object.get("body_dense_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), vector.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1), relational_rows.jsonNumberAsF64(vector[0]) orelse return error.TestUnexpectedResult, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 3), relational_rows.jsonNumberAsF64(vector[2]) orelse return error.TestUnexpectedResult, 0.000001);
}

test "db search runtime projection search includes embedding artifacts on hydrated hits when _embeddings is requested" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const putDenseEmbeddingArtifactForTest = db_test_support.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello world\"}" }},
        .sync_level = .full_index,
    });

    const dense_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 0 });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .fields = &.{"_embeddings"},
        .include_all_fields = false,
        .include_stored = true,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expect(result.hits[0].stored_data != null);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.hits[0].stored_data.?, .{});
    defer parsed.deinit();
    const vector = parsed.value.object.get("_embeddings").?.object.get("body_dense_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), vector.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1), relational_rows.jsonNumberAsF64(vector[0]) orelse return error.TestUnexpectedResult, 0.000001);
}

test "db search runtime projection scan includes embedding artifacts when _embeddings is requested" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const putDenseEmbeddingArtifactForTest = db_test_support.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const dense_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 0 });

    var scan = try db.scan(alloc, "", "", .{
        .include_documents = true,
        .fields = &.{ "_embeddings", "_embeddings.body_dense_v1" },
        .include_all_fields = false,
    });
    defer scan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), scan.documents.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, scan.documents[0].json, .{});
    defer parsed.deinit();
    const vector = parsed.value.object.get("_embeddings").?.object.get("body_dense_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), vector.len);
}

test "db search runtime projection lookup does not load embeddings for nested _embeddings projection without explicit include-all selector" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const putDenseEmbeddingArtifactForTest = db_test_support.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const dense_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 0 });

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{"_embeddings.body_dense_v1"},
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("_embeddings") == null);
}

test "db search runtime projection lookup loads embeddings for _embeddings.* selector and allows nested projection" {
    const DB = @import("mod.zig").DB;
    const db_test_support = @import("test_support.zig");
    const tempPath = db_test_support.tempPath;
    const cleanupTempDir = db_test_support.cleanupTempDir;
    const putDenseEmbeddingArtifactForTest = db_test_support.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const dense_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "body_dense_v1");
    defer alloc.free(dense_key);
    try putDenseEmbeddingArtifactForTest(&db, alloc, dense_key, null, &[_]f32{ 1, 0 });

    var result = (try db.lookup(alloc, "doc:a", .{
        .fields = &.{ "_embeddings.*", "_embeddings.body_dense_v1" },
        .include_all_fields = false,
    })).?;
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();
    const vector = parsed.value.object.get("_embeddings").?.object.get("body_dense_v1").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), vector.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1), relational_rows.jsonNumberAsF64(vector[0]) orelse return error.TestUnexpectedResult, 0.000001);
}

test "db search runtime projection field selection plan only enables chunk special field for explicit include-all selectors" {
    const none = db_query_projection.buildFieldSelectionPlan(&.{"_chunks.body_chunks_v1.0._content"}, false);
    try std.testing.expect(!none.special.all_artifacts);
    try std.testing.expect(!none.special.all_chunks);
    try std.testing.expect(!none.special.all_embeddings);

    const artifacts_none = db_query_projection.buildFieldSelectionPlan(&.{"_artifacts.body_chunks_v1.items.0.value"}, false);
    try std.testing.expect(!artifacts_none.special.all_artifacts);
    try std.testing.expect(!artifacts_none.special.all_chunks);
    try std.testing.expect(!artifacts_none.special.all_embeddings);

    const artifacts_plain = db_query_projection.buildFieldSelectionPlan(&.{"_artifacts"}, false);
    try std.testing.expect(artifacts_plain.special.all_artifacts);
    try std.testing.expect(!artifacts_plain.special.all_chunks);
    try std.testing.expect(!artifacts_plain.special.all_embeddings);

    const artifacts_wildcard = db_query_projection.buildFieldSelectionPlan(&.{ "_artifacts.*", "_artifacts.body_chunks_v1.items" }, false);
    try std.testing.expect(artifacts_wildcard.special.all_artifacts);
    try std.testing.expect(!artifacts_wildcard.special.all_chunks);
    try std.testing.expect(!artifacts_wildcard.special.all_embeddings);

    const all_plain = db_query_projection.buildFieldSelectionPlan(&.{"_chunks"}, false);
    try std.testing.expect(!all_plain.special.all_artifacts);
    try std.testing.expect(all_plain.special.all_chunks);
    try std.testing.expect(!all_plain.special.all_embeddings);

    const all_wildcard = db_query_projection.buildFieldSelectionPlan(&.{ "_chunks.*", "_chunks.body_chunks_v1.0._content" }, false);
    try std.testing.expect(!all_wildcard.special.all_artifacts);
    try std.testing.expect(all_wildcard.special.all_chunks);
    try std.testing.expect(!all_wildcard.special.all_embeddings);
    try std.testing.expectEqual(@as(usize, 2), all_wildcard.projection.fields.len);

    const embedding_none = db_query_projection.buildFieldSelectionPlan(&.{"_embeddings.body_dense_v1"}, false);
    try std.testing.expect(!embedding_none.special.all_artifacts);
    try std.testing.expect(!embedding_none.special.all_chunks);
    try std.testing.expect(!embedding_none.special.all_embeddings);

    const embedding_plain = db_query_projection.buildFieldSelectionPlan(&.{"_embeddings"}, false);
    try std.testing.expect(!embedding_plain.special.all_artifacts);
    try std.testing.expect(!embedding_plain.special.all_chunks);
    try std.testing.expect(embedding_plain.special.all_embeddings);

    const embedding_wildcard = db_query_projection.buildFieldSelectionPlan(&.{ "_embeddings.*", "_embeddings.body_dense_v1" }, false);
    try std.testing.expect(!embedding_wildcard.special.all_artifacts);
    try std.testing.expect(!embedding_wildcard.special.all_chunks);
    try std.testing.expect(embedding_wildcard.special.all_embeddings);
    try std.testing.expectEqual(@as(usize, 2), embedding_wildcard.projection.fields.len);
}
