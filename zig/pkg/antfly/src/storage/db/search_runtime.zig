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
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const lsm_backend_mod = @import("../lsm_backend/mod.zig");
const planning_adapter_mod = @import("planning_adapter.zig");
const planning_bindings_mod = @import("planning_bindings.zig");
const planning_stats_mod = @import("planning_stats.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const relational_collation = @import("relational_collation.zig");
const relational_store_mod = @import("relational_store.zig");
const resource_manager_mod = @import("../resource_manager.zig");
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
const applyGraphUnion = db_query_graph.applyGraphUnion;
const applyGraphIntersection = db_query_graph.applyGraphIntersection;
const AlgebraicIndex = @import("algebraic/index.zig").Index;

pub const Edge = graph_mod.Edge;
pub const EdgeDirection = graph_mod.EdgeDirection;
pub const TraversalRules = traversal_mod.TraversalRules;
pub const TraversalResult = traversal_mod.TraversalResult;
pub const PathWeightMode = paths_mod.PathWeightMode;
pub const Path = paths_mod.Path;
pub const PatternStep = graph_pattern_mod.PatternStep;
pub const PatternMatch = graph_pattern_mod.PatternMatch;
pub const ExpandStrategy = graph_query_mod.ExpandStrategy;
pub const GraphResultNode = graph_query_mod.GraphResultNode;
pub const GraphQuery = graph_query_mod.GraphQuery;
pub const GraphQueryResult = graph_query_mod.GraphQueryResult;
pub const GraphNamedResultSet = db_query_graph.NamedResultSet;
pub const GraphMetricBuildWorkerStepResult = graph_mod.GraphIndex.GraphMetricBuildWorkerStepResult;
pub const GraphMetricPlannedDrainOptions = graph_mod.GraphIndex.GraphMetricPlannedDrainOptions;
pub const SearchQuery = search_mod.SearchQuery;
pub const TextFieldStats = distributed_stats_mod.TextFieldStats;
pub const RuntimePreflightSummary = db_query_search.RuntimePreflightSummary;
pub const ExplicitTextStatRequest = db_query_search.ExplicitTextStatRequest;
pub const ExplicitBackgroundTextStatRequest = db_query_search.ExplicitBackgroundTextStatRequest;
pub const ProfiledDenseSearchResult = db_query_search.ProfiledDenseSearchResult;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

        pub fn textIndexIsChunkBacked(self: *DB, alloc: Allocator, index_name: ?[]const u8) !bool {
            return try self.core.textIndexIsChunkBacked(alloc, index_name);
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
            mode: relational_store_mod.FilterCombineMode,
        ) !void {
            if (current.* == null) {
                current.* = try doc_set.cloneAlloc(alloc, child);
                return;
            }

            if (try relational_store_mod.combineFilterSetFastAlloc(alloc, &current.*.?, child, mode)) |next| {
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
            mode: relational_store_mod.FilterCombineMode,
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
            implications: relational_store_mod.PredicateImplications,
        ) !bool {
            return try Self.relationalColumnIndexUsableForQueryWithColumns(self, alloc, column, implications, &.{column});
        }

        fn relationalColumnIndexUsableForQueryWithColumns(
            self: *DB,
            alloc: Allocator,
            column: schema_mod.RelationalColumn,
            implications: relational_store_mod.PredicateImplications,
            columns: []const schema_mod.RelationalColumn,
        ) !bool {
            if (!column.indexed) return false;
            if (column.index_lifecycle != .ready) return false;
            if (!(try relational_store_mod.predicatesImplyUniqueWhereWithColumns(alloc, implications.predicates, column.index_where, columns))) return false;
            return try self.relationalRowsExpressionPredicatesImply(alloc, implications, column.index_where_expressions);
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
            implications: relational_store_mod.PredicateImplications,
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
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, term.field) orelse return null;
            if (column.field_type != .keyword) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, runtime_schema.relational_columns, column, implications, generation, struct {
                wanted: []const u8,
                collation: ?[]const u8,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    return value.value_type == .bytes_val and !value.is_json and relational_collation.textEqual(value.value.bytes_val, ctx.wanted, ctx.collation);
                }
            }{ .wanted = term.term, .collation = term.collation });
        }

        fn resolveRelationalArrayAnyFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            array_any: search_mod.ArrayAnyQuery,
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, array_any.field) orelse return null;
            if (column.field_type != .array) return null;

            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (doc_ids.items) |doc_id| alloc.free(@constCast(doc_id));
                doc_ids.deinit(alloc);
            }

            if (try Self.relationalColumnIndexUsableForQueryWithColumns(self, alloc, column, implications, runtime_schema.relational_columns)) {
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
                    if (!(try relational_store_mod.arrayColumnValueContains(alloc, value, array_any.value))) continue;
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
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, json_contains.field) orelse return null;
            if (column.field_type != .json) return null;

            var doc_ids = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (doc_ids.items) |doc_id| alloc.free(@constCast(doc_id));
                doc_ids.deinit(alloc);
            }

            if ((try Self.relationalColumnIndexUsableForQueryWithColumns(self, alloc, column, implications, runtime_schema.relational_columns)) and relational_store_mod.jsonContainsHasIndexableLeaf(json_contains.value)) {
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
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, range.field) orelse return null;
            if (column.field_type != .keyword) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, runtime_schema.relational_columns, column, implications, generation, struct {
                min: ?[]const u8,
                max: ?[]const u8,
                inclusive_min: bool,
                inclusive_max: bool,
                collation: ?[]const u8,

                fn matches(ctx: @This(), value: relational_store_mod.OwnedColumnValue) bool {
                    if (value.value_type != .bytes_val or value.is_json) return false;
                    const bytes = value.value.bytes_val;
                    const above_min = if (ctx.min) |min| blk: {
                        const comparison = relational_collation.compareTextForScalar(bytes, min, ctx.collation);
                        break :blk comparison == .gt or (ctx.inclusive_min and comparison == .eq);
                    } else true;
                    const below_max = if (ctx.max) |max| blk: {
                        const comparison = relational_collation.compareTextForScalar(bytes, max, ctx.collation);
                        break :blk comparison == .lt or (ctx.inclusive_max and comparison == .eq);
                    } else true;
                    return above_min and below_max;
                }
            }{
                .min = range.min,
                .max = range.max,
                .inclusive_min = range.inclusive_min,
                .inclusive_max = range.inclusive_max,
                .collation = range.collation,
            });
        }

        fn resolveRelationalNumericFilterDocSetAlloc(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            range: search_mod.NumericRangeQuery,
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, range.field) orelse return null;
            if (column.field_type != .numeric) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, runtime_schema.relational_columns, column, implications, generation, struct {
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
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, range.field) orelse return null;
            if (column.field_type != .datetime) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, runtime_schema.relational_columns, column, implications, generation, struct {
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
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, bool_query.field) orelse return null;
            if (column.field_type != .boolean) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, runtime_schema.relational_columns, column, implications, generation, struct {
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
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, geo_query.field) orelse return null;
            if (column.field_type != .geopoint) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, runtime_schema.relational_columns, column, implications, generation, struct {
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
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
        ) !?doc_set.ResolvedDocSet {
            const column = relational_store_mod.columnForField(runtime_schema, geo_query.field) orelse return null;
            if (column.field_type != .geopoint) return null;
            return try Self.scanRelationalColumnFilterDocSetAlloc(self, alloc, runtime_schema.relational_columns, column, implications, generation, struct {
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
            implications: relational_store_mod.PredicateImplications,
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
            columns: []const schema_mod.RelationalColumn,
            column: schema_mod.RelationalColumn,
            implications: relational_store_mod.PredicateImplications,
            generation: ?u64,
            matcher: anytype,
        ) !doc_set.ResolvedDocSet {
            if (!(try Self.relationalColumnIndexUsableForQueryWithColumns(self, alloc, column, implications, columns))) {
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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

test "db search runtime projection scan returns hashes and projected documents" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"x\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"y\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"z\"}" },
        },
    });

    var result = try db.scan(alloc, "doc:a", "doc:c", .{
        .include_documents = true,
        .fields = &.{"title"},
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.hashes.len);
    try std.testing.expectEqualStrings("doc:b", result.hashes[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hashes[1].id);
    try std.testing.expectEqual(@as(usize, 2), result.documents.len);
    try std.testing.expect(std.mem.indexOf(u8, result.documents[0].json, "\"title\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.documents[0].json, "\"body\"") == null);
}

test "db search runtime projection search projects stored fields for hydrated hits" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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

test "db search runtime full-text chunk consumer returns parent and chunk modes" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .full_text,
    });

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.textIndex("ft_chunks").?.snapshot().global_doc_count > 0);

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();

    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    try std.testing.expectEqualStrings(chunk_zero, chunk_result.hits[0].id);
    const chunk_ref = chunk_result.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.ArtifactKind.chunk, chunk_ref.kind);
    try std.testing.expectEqualStrings("doc:a", chunk_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", chunk_ref.name);
    try std.testing.expectEqual(@as(?u32, 0), chunk_ref.chunk_id);

    var parent_result = try db.search(alloc, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .parent,
    });
    defer parent_result.deinit();

    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);
}

test "db search runtime full-text chunk default index searches template chunk text when chunker full text indexing is enabled" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "semantic_template_chunked_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"Alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
        },
        .sync_level = .full_text,
    });

    var result = try waitForSearchResult(alloc, &db, .{
        .index_name = "full_text_index_v0",
        .full_text = .{ .match = .{ .field = "body", .text = "routing" } },
        .return_mode = .parent,
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db search runtime full-text chunk full_text sync level covers template chunk routing" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "semantic_template_chunked_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"Alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
        },
        .sync_level = .full_text,
    });

    const pending = db.pendingWorkStats();
    try std.testing.expectEqual(pending.enrichment.target_sequence, pending.enrichment.applied_sequence);
    try std.testing.expect(pending.enrichment.applied_sequence >= 1);

    var result = try db.search(alloc, .{
        .index_name = "full_text_index_v0",
        .full_text = .{ .match = .{ .field = "body", .text = "routing" } },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db search runtime full-text chunk consumer filters expired parents under ttl" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    const ttl_duration_ns: u64 = 60 * std.time.ns_per_s;
    try db.setSchema(.{
        .version = 1,
        .default_type = "_default",
        .ttl_duration_ns = ttl_duration_ns,
    });

    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    const now_ns = db_internal.currentTimeNs();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:old", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .timestamp_ns = now_ns - 2 * ttl_duration_ns,
        .sync_level = .full_text,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:fresh", .value = "{\"body\":\"abcdefghijklmno\"}" }},
        .timestamp_ns = now_ns,
        .sync_level = .full_text,
    });

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .chunk,
    }, 1);
    defer chunk_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), chunk_result.total_hits);
    const fresh_chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:fresh", "body_chunks_v1", 0);
    defer alloc.free(fresh_chunk_zero);
    try std.testing.expectEqualStrings(fresh_chunk_zero, chunk_result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "ft_chunks",
        .full_text = .{ .match = .{ .field = "body", .text = "abcdefgh" } },
        .return_mode = .parent,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:fresh", parent_result.hits[0].id);
}

test "db search runtime getArtifact loads stored chunk artifacts by public id" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const internal_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(internal_key);
    try db.core.store.put(internal_key, "{\"body\":\"abcdefgh\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0}");

    var artifact = (try db.getArtifact(alloc, chunk_zero)) orelse return error.TestUnexpectedResult;
    defer artifact.deinit(alloc);

    try std.testing.expectEqualStrings(chunk_zero, artifact.id);
    try std.testing.expectEqual(types.ArtifactKind.chunk, artifact.artifact_ref.kind);
    try std.testing.expectEqualStrings("doc:a", artifact.artifact_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", artifact.artifact_ref.name);
    try std.testing.expectEqual(@as(?u32, 0), artifact.artifact_ref.chunk_id);
    try std.testing.expect(std.mem.indexOf(u8, artifact.value, "\"_chunk_id\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, artifact.value, "\"body\":\"abcdefgh\"") != null);
}

test "db search runtime full-text chunk parent paging applies after grouping" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_chunks",
        .kind = .full_text,
        .config_json = "{\"chunk_name\":\"body_chunks_v1\"}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":32,\"chunk_overlap\":0}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha alpha alpha alpha\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"alpha\"}" },
        },
        .sync_level = .full_text,
    });

    var result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_chunks",
        .full_text = .{ .term = .{ .field = "body", .term = "alpha" } },
        .return_mode = .parent,
        .limit = 1,
        .offset = 1,
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

test "db search runtime dense chunk consumer supports parent and parent_with_chunks modes" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
        .search_effort = 1.0,
    }, 1);
    defer chunk_result.deinit();
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    const chunk_two = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 2);
    defer alloc.free(chunk_two);
    try std.testing.expectEqual(@as(u32, 3), chunk_result.total_hits);
    try std.testing.expect(
        std.mem.eql(u8, chunk_result.hits[0].id, chunk_zero) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_one) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_two),
    );

    var include = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:a"});
    errdefer include.deinit(alloc);
    const ordinal = switch (include) {
        .ordinals => |ordinals| blk: {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            break :blk ordinals[0];
        },
        else => return error.ExpectedOrdinalDocSet,
    };
    const vector_ids = try db.core.index_manager.lookupDenseVectorIdsForOrdinalsAlloc(alloc, db.core.store, "dv_v1", &.{ordinal});
    defer alloc.free(vector_ids);
    try std.testing.expect(vector_ids.len >= 2);

    var filter = doc_set.ResolvedDocFilter{
        .include = include,
        .exclude = .none,
    };
    include = .all;
    defer filter.deinit(alloc);

    var filtered_chunk_result = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 3,
        .include_stored = false,
        .return_mode = .parent,
        .resolved_doc_filter = &filter,
        .search_effort = 1.0,
    }, .{ .vector = query_vec, .k = 3 });
    defer filtered_chunk_result.result.deinit();
    try std.testing.expect(filtered_chunk_result.profile.raw_hit_count >= 2);
    try std.testing.expectEqual(@as(u32, 1), filtered_chunk_result.result.total_hits);
    try std.testing.expectEqualStrings("doc:a", filtered_chunk_result.result.hits[0].id);

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
        .search_effort = 1.0,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);

    var parent_with_chunks = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 1,
        .search_effort = 1.0,
    }, 1);
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    try std.testing.expect(
        std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_zero) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_one) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_two),
    );

    const doc_a_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_store_key);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(doc_a_store_key);
        try doc_identity.markDeletedTxn(alloc, &txn, 2, "doc:a");
        try txn.commit();
    }
    db.identity_visibility_summary_cache = null;

    var stale_chunk_result = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 3,
        .include_stored = false,
        .return_mode = .chunk,
        .search_effort = 1.0,
    }, .{ .vector = query_vec, .k = 3 });
    defer stale_chunk_result.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_chunk_result.result.total_hits);
    try std.testing.expectEqual(@as(u32, 0), stale_chunk_result.profile.raw_hit_count);

    var stale_parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
        .search_effort = 1.0,
    });
    defer stale_parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 0), stale_parent_result.total_hits);
}

test "db search runtime dense chunk consumer supports parent and parent_with_chunks modes with durable lsm primary backend" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"chunk_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" },
        },
        .sync_level = .write,
    });

    try db.enrichment_runtime.?.waitForApplied(1);
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    try std.testing.expect(chunk_records.len > 0);
    try std.testing.expect(db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count > 0);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "abcdefgh", 3);
    defer alloc.free(query_vec);

    var chunk_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .chunk,
        .search_effort = 1.0,
    }, 1);
    defer chunk_result.deinit();
    const chunk_zero = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_zero);
    const chunk_one = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 1);
    defer alloc.free(chunk_one);
    const chunk_two = try artifact_ids.chunkArtifactPublicIdAlloc(alloc, "doc:a", "body_chunks_v1", 2);
    defer alloc.free(chunk_two);
    try std.testing.expectEqual(@as(u32, 3), chunk_result.total_hits);
    try std.testing.expect(
        std.mem.eql(u8, chunk_result.hits[0].id, chunk_zero) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_one) or
            std.mem.eql(u8, chunk_result.hits[0].id, chunk_two),
    );

    var parent_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent,
        .search_effort = 1.0,
    });
    defer parent_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_result.hits[0].id);

    var parent_with_chunks = try waitForSearchResult(alloc, &db, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = query_vec, .k = 3 },
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 1,
        .search_effort = 1.0,
    }, 1);
    defer parent_with_chunks.deinit();
    try std.testing.expectEqual(@as(u32, 1), parent_with_chunks.total_hits);
    try std.testing.expectEqualStrings("doc:a", parent_with_chunks.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), parent_with_chunks.hits[0].chunk_hits.len);
    try std.testing.expect(
        std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_zero) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_one) or
            std.mem.eql(u8, parent_with_chunks.hits[0].chunk_hits[0].id, chunk_two),
    );
}

test "db search runtime dense chunk full_index supports parent search for template chunked embeddings" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_template_chunked_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
        },
        .sync_level = .full_index,
    });

    const query_vec = try deterministic.interface().embedDense(alloc, "", "alpha routing only in template chunks", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "semantic_template_chunked_idx",
        .dense = .{ .vector = query_vec, .k = 5 },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db search runtime dense chunk full_index supports parent search when chunk artifacts are ephemeral" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "semantic_template_chunked_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
        },
        .sync_level = .full_index,
    });

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "semantic_template_chunked_idx_chunks");
    defer alloc.free(chunk_prefix);
    const chunk_records = try db.core.store.scanPrefix(alloc, chunk_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, chunk_records);
    var chunk_count: usize = 0;
    for (chunk_records) |entry| {
        if (internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 0), chunk_count);

    const query_vec = try deterministic.interface().embedDense(alloc, "", "alpha routing only in template chunks", 3);
    defer alloc.free(query_vec);

    var result = try db.search(alloc, .{
        .index_name = "semantic_template_chunked_idx",
        .dense = .{ .vector = query_vec, .k = 5 },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db search runtime dense chunk reopened full_index supports parent search when chunk artifacts are ephemeral" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .enrichment = .{
                .owner_id = "worker-a",
                .dense_embedder = deterministic.interface(),
            },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "semantic_template_chunked_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"source_template\":\"{{title}}\",\"artifact_name\":\"semantic_template_chunked_idx_chunks\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":4,\"overlap_tokens\":1,\"separator\":\" \"}}}}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha routing only in template chunks\",\"body\":\"body text without the keyword\"}" },
            },
            .sync_level = .full_index,
        });
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    const query_vec = try deterministic.interface().embedDense(alloc, "", "alpha routing only in template chunks", 3);
    defer alloc.free(query_vec);

    var result = try reopened.search(alloc, .{
        .index_name = "semantic_template_chunked_idx",
        .dense = .{ .vector = query_vec, .k = 5 },
        .return_mode = .parent,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db search runtime projection lookup includes unified artifact projection when _artifacts is requested" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    try std.testing.expectApproxEqAbs(@as(f64, 1), relational_store_mod.jsonNumberAsF64(vector[0]) orelse return error.TestUnexpectedResult, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 3), relational_store_mod.jsonNumberAsF64(vector[2]) orelse return error.TestUnexpectedResult, 0.000001);
}

test "db search runtime projection search includes embedding artifacts on hydrated hits when _embeddings is requested" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    try std.testing.expectApproxEqAbs(@as(f64, 1), relational_store_mod.jsonNumberAsF64(vector[0]) orelse return error.TestUnexpectedResult, 0.000001);
}

test "db search runtime projection scan includes embedding artifacts when _embeddings is requested" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
    try std.testing.expectApproxEqAbs(@as(f64, 1), relational_store_mod.jsonNumberAsF64(vector[0]) orelse return error.TestUnexpectedResult, 0.000001);
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

test "db search runtime indexing batch get match_all search and index registry" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    const value = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", value);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try std.testing.expectEqual(@as(u32, 1), db.core.index_manager.count());
    try std.testing.expect(try db.deleteIndex("ft_v1"));
    try std.testing.expectEqual(@as(u32, 0), db.core.index_manager.count());

    var result = try db.search(alloc, .{
        .query = .{ .match_all = {} },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 2), stats.doc_count);
}

test "db search runtime indexing full-text index backfill and search routing" {
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"first body\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"second alpha\"}" },
        },
    });

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    var term_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .term = .{ .field = "title", .term = "alpha" } },
        .limit = 10,
    });
    defer term_result.deinit();

    try std.testing.expectEqual(@as(u32, 1), term_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", term_result.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"alpha third\"}" },
        },
        .sync_level = .full_index,
    });

    var match_result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "alpha" } },
        .limit = 10,
    }, 2);
    defer match_result.deinit();

    try std.testing.expectEqual(@as(u32, 2), match_result.total_hits);

    var match_all_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match_all = {} },
        .limit = 10,
    });
    defer match_all_result.deinit();

    try std.testing.expectEqual(@as(u32, 3), match_all_result.total_hits);
}

test "db search runtime indexing dense and sparse vector searches apply stored symbolic filters before final paging" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"category\":\"reject\",\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"category\":\"keep\",\"embedding\":[10,0],\"sparse\":{\"indices\":[1],\"values\":[0.1]}}" },
        },
        .sync_level = .full_index,
    });

    var dense_result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    });
    defer dense_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_result.hits[0].id);
    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        try std.testing.expectEqual(try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b"), dense_result.hits[0].doc_ordinal);
    }

    var sparse_result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    });
    defer sparse_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_result.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_result.hits[0].id);
}

test "db search runtime identity vector symbolic filters fail closed when algebraic lifecycle is stale" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "capability_lifecycle_status": "rebuild_required",
        \\  "group_fields": [{"name":"category","path":"category","type":"string"}],
        \\  "materializations": [{"name":"count_by_category","op":"count","group_by":["category"]}]
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"category\":\"reject\",\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"category\":\"keep\",\"embedding\":[10,0],\"sparse\":{\"indices\":[1],\"values\":[0.1]}}" },
        },
        .sync_level = .full_index,
    });

    const alg_entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
    try std.testing.expect(!alg_entry.index.plannerLifecycleReady());

    try std.testing.expectError(error.UnsupportedQueryRequest, db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .require_algebraic_filter_resolution = true,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 }));
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 0), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_unsupported_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_fail_closed_count);
        try std.testing.expectEqualStrings("capability_lifecycle_not_ready", status_value.planner_lifecycle_blocking_reason.?);
    }

    try std.testing.expectError(error.UnsupportedQueryRequest, db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .require_algebraic_filter_resolution = true,
    }));
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 0), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_unsupported_count);
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_fail_closed_count);
    }
}

test "db search runtime identity algebraic doc facts feed native dense and sparse symbolic filters" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "alg",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "group_fields": [
        \\    {"name":"category","path":"category","type":"string"},
        \\    {"name":"published","path":"published","type":"boolean"},
        \\    {"name":"ip","path":"ip","type":"string"},
        \\    {"name":"score","path":"score","type":"number"}
        \\  ],
        \\  "measure_fields": [
        \\    {"name":"code","path":"code","type":"string"}
        \\  ],
        \\  "materializations": [{"name":"count_by_category","op":"count","group_by":["category"]}]
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"shared token\",\"category\":\"reject\",\"published\":false,\"ip\":\"192.168.1.10\",\"score\":1.0,\"code\":\"drop\",\"meta\":{\"tier\":\"bronze\"},\"location\":{\"lat\":40.7128,\"lon\":-74.0060},\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"shared token\",\"category\":\"keep\",\"published\":true,\"ip\":\"10.1.2.3\",\"score\":5.0,\"code\":\"keep-code\",\"meta\":{\"tier\":\"gold\"},\"location\":{\"lat\":37.7749,\"lon\":-122.4194},\"embedding\":[10,0],\"sparse\":{\"indices\":[1],\"values\":[0.1]}}" },
        },
        .sync_level = .full_index,
    });

    const alg_entry = db.core.index_manager.algebraicIndex("alg") orelse return error.TestUnexpectedResult;
    const keep_doc_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"term\":{\"category\":\"keep\"}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(keep_doc_ids);
    try std.testing.expectEqual(@as(usize, 1), keep_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", keep_doc_ids[0]);
    try std.testing.expect((try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:b")) != null);

    const location_lat_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"numeric_range\":{\"path\":\"/location/lat\",\"min\":37.0,\"max\":38.0}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(location_lat_ids);
    try std.testing.expectEqual(@as(usize, 1), location_lat_ids.len);
    try std.testing.expectEqualStrings("doc:b", location_lat_ids[0]);

    const geo_doc_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"geo_distance\":{\"path\":\"/location\",\"lat\":37.7749,\"lon\":-122.4194,\"radius_meters\":2000}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(geo_doc_ids);
    try std.testing.expectEqual(@as(usize, 1), geo_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", geo_doc_ids[0]);

    const geo_shape_doc_ids = (try alg_entry.index.docIdsForFilterJsonAlloc(db.core.store, "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"intersects\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}")) orelse return error.TestUnexpectedResult;
    defer alg_entry.index.freeDocIds(geo_shape_doc_ids);
    try std.testing.expectEqual(@as(usize, 1), geo_shape_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", geo_shape_doc_ids[0]);

    var dense_keep = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/meta/tier\",\"value\":\"bronze\"}}",
        .require_algebraic_filter_resolution = true,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_keep.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_keep.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_keep.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_keep.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_keep.profile.raw_hit_count);
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_include_doc_id_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_exclude_doc_id_count);
    }

    try std.testing.expectError(error.UnsupportedQueryRequest, db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .exclusion_query_json = "{\"wildcard\":{\"/meta/tier\":\"*old\"}}",
        .require_algebraic_filter_resolution = true,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 }));
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 2), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_unsupported_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_fail_closed_count);
    }
    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.doc_set_planning.unsupported_filter_shape_count >= 1);
    }

    const binding_defs = [_]types.NamedDocFilterBinding{
        .{ .name = "kept", .filter_query_json = "{\"term\":{\"category\":\"keep\"}}" },
        .{ .name = "published", .filter_query_json = "{\"bool_field\":{\"field\":\"published\",\"value\":true}}" },
    };
    var sparse_with_binding = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
        .include_stored = false,
        .doc_filter_bindings = binding_defs[0..],
        .filter_query_json = "{\"bool\":{\"must\":[{\"ref\":\"kept\"},{\"ref\":\"published\"}]}}",
        .require_algebraic_filter_resolution = true,
    });
    defer sparse_with_binding.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_with_binding.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_with_binding.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_with_binding.hits[0].id);

    var full_text_with_binding = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "shared" } },
        .limit = 2,
        .include_stored = false,
        .doc_filter_bindings = binding_defs[0..],
        .filter_query_json = "{\"bool\":{\"must\":[{\"ref\":\"kept\"},{\"ref\":\"published\"}]}}",
        .require_algebraic_filter_resolution = true,
    });
    defer full_text_with_binding.deinit();
    try std.testing.expectEqual(@as(u32, 1), full_text_with_binding.total_hits);
    try std.testing.expectEqual(@as(usize, 1), full_text_with_binding.hits.len);
    try std.testing.expectEqualStrings("doc:b", full_text_with_binding.hits[0].id);
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 4), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 3), status_value.vector_filter_resolved_count);
    }

    var full_text_direct_algebraic = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "shared" } },
        .limit = 2,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .require_algebraic_filter_resolution = true,
    });
    defer full_text_direct_algebraic.deinit();
    try std.testing.expectEqual(@as(u32, 1), full_text_direct_algebraic.total_hits);
    try std.testing.expectEqual(@as(usize, 1), full_text_direct_algebraic.hits.len);
    try std.testing.expectEqualStrings("doc:b", full_text_direct_algebraic.hits[0].id);
    {
        const status_value = alg_entry.index.status();
        try std.testing.expectEqual(@as(u64, 5), status_value.vector_filter_attempt_count);
        try std.testing.expectEqual(@as(u64, 4), status_value.vector_filter_resolved_count);
        try std.testing.expectEqual(@as(u64, 1), status_value.vector_filter_unsupported_count);
    }

    var dense_terms = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"terms\":{\"field\":\"category\",\"values\":[\"missing\",\"keep\"]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_terms.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_terms.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_terms.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_terms.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_terms.profile.raw_hit_count);

    var dense_score_range = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"numeric_range\":{\"field\":\"score\",\"min\":2.0}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_score_range.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_score_range.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_score_range.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_score_range.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_score_range.profile.raw_hit_count);

    var dense_standard_score_range = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"range\":{\"score\":{\"gte\":2.0}}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_standard_score_range.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_standard_score_range.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_standard_score_range.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_standard_score_range.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_standard_score_range.profile.raw_hit_count);

    var dense_bool_field = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool_field\":{\"field\":\"published\",\"value\":true}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_bool_field.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_bool_field.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_bool_field.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_bool_field.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_bool_field.profile.raw_hit_count);

    var dense_measure_prefix = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"field\":\"code\",\"role\":\"measure\",\"value\":\"keep\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_measure_prefix.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_measure_prefix.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_measure_prefix.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_measure_prefix.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_measure_prefix.profile.raw_hit_count);

    var dense_measure_wildcard = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"wildcard\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-*\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_measure_wildcard.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_measure_wildcard.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_measure_wildcard.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_measure_wildcard.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_measure_wildcard.profile.raw_hit_count);

    var dense_measure_regexp = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"regexp\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-.*\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_measure_regexp.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_measure_regexp.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_measure_regexp.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_measure_regexp.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_measure_regexp.profile.raw_hit_count);

    var dense_fuzzy = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"fuzzy\":{\"field\":\"code\",\"role\":\"measure\",\"query\":\"keep-cide\",\"max_edits\":1,\"prefix_length\":5}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_fuzzy.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_fuzzy.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_fuzzy.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_fuzzy.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_fuzzy.profile.raw_hit_count);

    var dense_match = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match\":{\"field\":\"code\",\"role\":\"measure\",\"text\":\"KEEP\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_match.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_match.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_match.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_match.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_match.profile.raw_hit_count);

    var dense_path_term = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"path\":\"/meta/tier\",\"value\":\"gold\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_path_term.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_path_term.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_path_term.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_path_term.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_path_term.profile.raw_hit_count);

    var dense_path_prefix = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"path\":\"/meta/tier\",\"value\":\"go\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_path_prefix.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_path_prefix.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_path_prefix.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_path_prefix.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_path_prefix.profile.raw_hit_count);

    var dense_ip_range = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"ip_range\":{\"field\":\"ip\",\"cidr\":\"10.0.0.0/8\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_ip_range.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_ip_range.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_ip_range.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_ip_range.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_ip_range.profile.raw_hit_count);

    var dense_geo_distance = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_distance\":{\"path\":\"/location\",\"lat\":37.7749,\"lon\":-122.4194,\"radius_meters\":2000}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_geo_distance.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_geo_distance.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_geo_distance.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_geo_distance.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_geo_distance.profile.raw_hit_count);

    var dense_geo_shape = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"intersects\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_geo_shape.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_geo_shape.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_geo_shape.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_geo_shape.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_geo_shape.profile.raw_hit_count);

    var dense_disjuncts = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"category\":\"missing\"}},{\"bool_field\":{\"field\":\"published\",\"value\":true}}]}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_disjuncts.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_disjuncts.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_disjuncts.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_disjuncts.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_disjuncts.profile.raw_hit_count);

    var dense_required_plus_optional_should = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool\":{\"must\":[{\"term\":{\"category\":\"keep\"}}],\"should\":[{\"term\":{\"category\":\"reject\"}}]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_required_plus_optional_should.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_required_plus_optional_should.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_required_plus_optional_should.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_required_plus_optional_should.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_required_plus_optional_should.profile.raw_hit_count);

    var dense_missing = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"missing\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_missing.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), dense_missing.result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), dense_missing.result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), dense_missing.profile.raw_hit_count);

    var dense_match_none = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match_none\":{}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_match_none.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), dense_match_none.result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), dense_match_none.result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), dense_match_none.profile.raw_hit_count);

    var dense_intersect = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_doc_ids = &.{ "doc:a", "doc:b" },
        .filter_doc_ids_positive = true,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_intersect.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_intersect.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_intersect.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_intersect.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_intersect.profile.raw_hit_count);

    var dense_doc_id_filter = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"doc_id\":{\"ids\":[\"doc:b\",\"missing\"]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_doc_id_filter.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_doc_id_filter.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_doc_id_filter.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_doc_id_filter.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_doc_id_filter.profile.raw_hit_count);

    var dense_must_not = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool\":{\"must_not\":[{\"term\":{\"category\":\"reject\"}}]}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 2 });
    defer dense_must_not.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_must_not.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_must_not.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_must_not.result.hits[0].id);

    var sparse_keep = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
    });
    defer sparse_keep.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_keep.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_keep.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_keep.hits[0].id);

    var sparse_terms = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"terms\":{\"field\":\"category\",\"values\":[\"missing\",\"keep\"]}}",
    });
    defer sparse_terms.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_terms.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_terms.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_terms.hits[0].id);

    var sparse_missing = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"category\":\"missing\"}}",
    });
    defer sparse_missing.deinit();
    try std.testing.expectEqual(@as(u32, 0), sparse_missing.total_hits);
    try std.testing.expectEqual(@as(usize, 0), sparse_missing.hits.len);

    var sparse_match_none = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match_none\":{}}",
    });
    defer sparse_match_none.deinit();
    try std.testing.expectEqual(@as(u32, 0), sparse_match_none.total_hits);
    try std.testing.expectEqual(@as(usize, 0), sparse_match_none.hits.len);

    var sparse_score_range = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"numeric_range\":{\"field\":\"score\",\"min\":2.0}}",
    });
    defer sparse_score_range.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_score_range.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_score_range.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_score_range.hits[0].id);

    var sparse_standard_score_range = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"range\":{\"score\":{\"gte\":2.0}}}",
    });
    defer sparse_standard_score_range.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_standard_score_range.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_standard_score_range.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_standard_score_range.hits[0].id);

    var sparse_bool_field = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool_field\":{\"field\":\"published\",\"value\":true}}",
    });
    defer sparse_bool_field.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_bool_field.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_bool_field.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_bool_field.hits[0].id);

    var sparse_measure_prefix = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"field\":\"code\",\"role\":\"measure\",\"value\":\"keep\"}}",
    });
    defer sparse_measure_prefix.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_measure_prefix.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_measure_prefix.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_measure_prefix.hits[0].id);

    var sparse_measure_wildcard = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"wildcard\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-*\"}}",
    });
    defer sparse_measure_wildcard.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_measure_wildcard.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_measure_wildcard.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_measure_wildcard.hits[0].id);

    var sparse_measure_regexp = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"regexp\":{\"field\":\"code\",\"role\":\"measure\",\"pattern\":\"keep-.*\"}}",
    });
    defer sparse_measure_regexp.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_measure_regexp.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_measure_regexp.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_measure_regexp.hits[0].id);

    var sparse_fuzzy = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"fuzzy\":{\"field\":\"code\",\"role\":\"measure\",\"query\":\"keep-cide\",\"max_edits\":1,\"prefix_length\":5}}",
    });
    defer sparse_fuzzy.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_fuzzy.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_fuzzy.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_fuzzy.hits[0].id);

    var sparse_match = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match\":{\"field\":\"code\",\"role\":\"measure\",\"text\":\"KEEP\"}}",
    });
    defer sparse_match.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_match.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_match.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_match.hits[0].id);

    var sparse_path_term = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"term\":{\"path\":\"/meta/tier\",\"value\":\"gold\"}}",
    });
    defer sparse_path_term.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_path_term.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_path_term.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_path_term.hits[0].id);

    var sparse_path_prefix = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"prefix\":{\"path\":\"/meta/tier\",\"value\":\"go\"}}",
    });
    defer sparse_path_prefix.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_path_prefix.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_path_prefix.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_path_prefix.hits[0].id);

    var sparse_ip_range = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"ip_range\":{\"field\":\"ip\",\"cidr\":\"10.0.0.0/8\"}}",
    });
    defer sparse_ip_range.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_ip_range.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_ip_range.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_ip_range.hits[0].id);

    var sparse_geo_bbox = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_bbox\":{\"field\":\"/location\",\"min_lat\":37.70,\"min_lon\":-122.50,\"max_lat\":37.80,\"max_lon\":-122.30}}",
    });
    defer sparse_geo_bbox.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_geo_bbox.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_geo_bbox.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_geo_bbox.hits[0].id);

    var sparse_geo_shape = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"geo_shape\":{\"path\":\"/location\",\"relation\":\"intersects\",\"polygons\":[[{\"lat\":37.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-123.0},{\"lat\":38.0,\"lon\":-122.0},{\"lat\":37.0,\"lon\":-122.0}]]}}",
    });
    defer sparse_geo_shape.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_geo_shape.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_geo_shape.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_geo_shape.hits[0].id);

    var sparse_disjuncts = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"disjuncts\":[{\"term\":{\"category\":\"missing\"}},{\"bool_field\":{\"field\":\"published\",\"value\":true}}]}",
    });
    defer sparse_disjuncts.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_disjuncts.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_disjuncts.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_disjuncts.hits[0].id);

    var sparse_required_plus_optional_should = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"bool\":{\"filter\":[{\"term\":{\"category\":\"keep\"}}],\"should\":[{\"term\":{\"category\":\"reject\"}}],\"minimum_should_match\":0}}",
    });
    defer sparse_required_plus_optional_should.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_required_plus_optional_should.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_required_plus_optional_should.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_required_plus_optional_should.hits[0].id);
}

test "db search runtime indexing lsm match-all query sees same latest value as point lookup" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "user-1", .value = "{\"name\":\"Alice\",\"tier\":\"gold\"}" },
            .{ .key = "user-2", .value = "{\"name\":\"Bob\",\"tier\":\"silver\"}" },
        },
        .sync_level = .write,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "user-1", .value = "{\"name\":\"Alice\",\"tier\":\"platinum\"}" },
        },
        .sync_level = .write,
    });

    const raw = try db.get(alloc, "user-1");
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"platinum\"") != null);

    var result = try db.search(alloc, .{
        .query = .match_all,
        .fields = &.{ "name", "tier" },
        .limit = 10,
    });
    defer result.deinit();

    var saw_user_1 = false;
    for (result.hits) |hit| {
        if (!std.mem.eql(u8, hit.id, "user-1")) continue;
        saw_user_1 = true;
        try std.testing.expect(hit.stored_data != null);
        try std.testing.expect(std.mem.indexOf(u8, hit.stored_data.?, "\"platinum\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, hit.stored_data.?, "\"gold\"") == null);
    }
    try std.testing.expect(saw_user_1);
}

test "db search runtime reopen full-text index and search survive reopen with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
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

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "alpha" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        });
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

test "db search runtime indexing full-text backfill resumes after interrupted reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }

        for (0..300) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"alpha\",\"n\":{d}}}", .{i}),
            });
        }

        try db.batch(.{ .writes = writes.items });
        try db.core.index_manager.addAllNoBackfill(db.core.store, &.{.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        }});
    }

    index_manager_mod.test_abort_text_backfill_after_batches = 1;
    defer index_manager_mod.test_abort_text_backfill_after_batches = null;
    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{});
        defer interrupted.close();
        try std.testing.expect(interrupted.core.index_manager.hasLoadFailures());
        try std.testing.expectEqualStrings(
            "TestInjectedBackfillFailure",
            interrupted.core.index_manager.loadFailure("ft_v1") orelse return error.TestExpectedEqual,
        );
    }

    const state_path = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/rebuild.state", .{std.mem.span(path)});
    defer alloc.free(state_path);
    const interrupted_state = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path, alloc, .limited(1024));
    defer alloc.free(interrupted_state);
    try std.testing.expect(interrupted_state.len > 0);

    index_manager_mod.test_abort_text_backfill_after_batches = null;

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path, alloc, .limited(1024)));

    var result = try reopened.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match_all = {} },
        .limit = 400,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 300), result.total_hits);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqual(false, stats.indexes[0].backfill_active);
}

test "db search runtime indexing sparse backfill resumes after interrupted reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }

        for (0..10) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try alloc.dupe(u8, "{\"sparse\":{\"indices\":[0,1],\"values\":[1.0,0.5]}}"),
            });
        }

        try db.batch(.{ .writes = writes.items });
        try db.core.index_manager.addAllNoBackfill(db.core.store, &.{.{
            .name = "sp_v1",
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse\"}",
        }});
    }

    index_manager_mod.test_sparse_backfill_batch_size = 4;
    defer index_manager_mod.test_sparse_backfill_batch_size = null;
    index_manager_mod.test_abort_sparse_backfill_after_batches = 1;
    defer index_manager_mod.test_abort_sparse_backfill_after_batches = null;
    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{});
        defer interrupted.close();
        try std.testing.expect(interrupted.core.index_manager.hasLoadFailures());
        try std.testing.expectEqualStrings(
            "TestInjectedBackfillFailure",
            interrupted.core.index_manager.loadFailure("sp_v1") orelse return error.TestExpectedEqual,
        );
    }

    const state_path = try std.fmt.allocPrint(alloc, "{s}/indexes/sp_v1/rebuild.state", .{std.mem.span(path)});
    defer alloc.free(state_path);
    const interrupted_state = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path, alloc, .limited(1024));
    defer alloc.free(interrupted_state);
    try std.testing.expect(interrupted_state.len > 0);

    index_manager_mod.test_abort_sparse_backfill_after_batches = null;

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path, alloc, .limited(1024)));

    var result = try reopened.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{0},
            .values = &.{1.0},
            .k = 10,
        } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expect(result.total_hits > 0);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    for (stats.indexes) |entry| {
        if (std.mem.eql(u8, entry.name, "sp_v1")) {
            try std.testing.expectEqual(false, entry.backfill_active);
            try std.testing.expectEqual(@as(u64, 10), entry.doc_count);
            return;
        }
    }
    return error.TestExpectedEqual;
}

test "db search runtime indexing dense vector index routes knn search" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0],\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[0,1,0],\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expect(result.hits[0].score.? <= result.hits[1].score.?);
}

test "db search runtime indexing dense vector index routes knn search with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0],\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[0,1,0],\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expect(result.hits[0].score.? <= result.hits[1].score.?);
}

test "db search runtime indexing full text conjunction query" {
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc1", .value = "{\"content\":\"alpha beta\",\"title\":\"both\"}" },
            .{ .key = "doc2", .value = "{\"content\":\"alpha only\",\"title\":\"alpha\"}" },
            .{ .key = "doc3", .value = "{\"content\":\"beta only\",\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    var result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_v1",
        .full_text = .{ .bool_query = .{
            .must = &.{
                .{ .match = .{ .field = "content", .text = "alpha" } },
                .{ .match = .{ .field = "content", .text = "beta" } },
            },
        } },
        .limit = 10,
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc1", result.hits[0].id);
}

test "db search runtime indexing full text conjunction query with incremental full_index batches" {
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc1", .value = "{\"content\":\"alpha beta\",\"title\":\"both\"}" }},
        .sync_level = .full_index,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc2", .value = "{\"content\":\"alpha only\",\"title\":\"alpha\"}" }},
        .sync_level = .full_index,
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc3", .value = "{\"content\":\"beta only\",\"title\":\"beta\"}" }},
        .sync_level = .full_index,
    });

    var result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_v1",
        .full_text = .{ .bool_query = .{
            .must = &.{
                .{ .match = .{ .field = "content", .text = "alpha" } },
                .{ .match = .{ .field = "content", .text = "beta" } },
            },
        } },
        .limit = 10,
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc1", result.hits[0].id);
}

test "db search runtime indexing full text count_only applies stored filters" {
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc1", .value = "{\"content\":\"alpha\",\"category\":\"keep\"}" },
            .{ .key = "doc2", .value = "{\"content\":\"alpha\",\"category\":\"skip\"}" },
            .{ .key = "doc3", .value = "{\"content\":\"beta\",\"category\":\"keep\"}" },
        },
    });

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    var result = try waitForSearchResult(alloc, &db, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "content", .text = "alpha" } },
        .filter_query_json = "{\"term\":{\"category\":\"keep\"}}",
        .count_only = true,
        .limit = 10,
    }, 1);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), result.hits.len);
}

test "db search runtime indexing full_index delete waits for full text visibility" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc1", .value = "{\"content\":\"alpha beta\"}" }},
        .sync_level = .full_index,
    });

    var before_delete = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "content", .text = "alpha" } },
        .limit = 10,
    });
    defer before_delete.deinit();

    try std.testing.expectEqual(@as(u32, 1), before_delete.total_hits);
    try std.testing.expectEqual(@as(usize, 1), before_delete.hits.len);
    try std.testing.expectEqualStrings("doc1", before_delete.hits[0].id);

    try db.batch(.{
        .deletes = &.{"doc1"},
        .sync_level = .full_index,
    });

    var after_delete = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "content", .text = "alpha" } },
        .limit = 10,
    });
    defer after_delete.deinit();

    try std.testing.expectEqual(@as(u32, 0), after_delete.total_hits);
    try std.testing.expectEqual(@as(usize, 0), after_delete.hits.len);
}

test "db search runtime indexing stats uses full text visible count when available" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"content\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"content\":\"beta\"}" },
        },
        .sync_level = .full_index,
    });

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 2), stats.doc_count);
        try std.testing.expectEqual(@as(u64, 2), stats.indexes[0].doc_count);
        try std.testing.expect(stats.indexes[0].term_count > 0);
    }

    try db.batch(.{
        .deletes = &.{"doc:b"},
        .sync_level = .full_index,
    });

    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_count);
        try std.testing.expectEqual(@as(u64, 1), stats.indexes[0].doc_count);
    }
}

test "db search runtime indexing schemaless full text indexes strings into _all for bare text search" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v0",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha beta\"}" },
            .{ .key = "doc:b", .value = "{\"nested\":{\"body\":\"gamma delta\"}}" },
        },
        .sync_level = .full_index,
    });

    var top_level = try db.search(alloc, .{
        .index_name = "ft_v0",
        .query = .{ .match = .{ .field = "_all", .text = "alpha" } },
        .limit = 10,
    });
    defer top_level.deinit();
    try std.testing.expectEqual(@as(u32, 1), top_level.total_hits);
    try std.testing.expectEqualStrings("doc:a", top_level.hits[0].id);

    var nested = try db.search(alloc, .{
        .index_name = "ft_v0",
        .query = .{ .match = .{ .field = "_all", .text = "gamma" } },
        .limit = 10,
    });
    defer nested.deinit();
    try std.testing.expectEqual(@as(u32, 1), nested.total_hits);
    try std.testing.expectEqualStrings("doc:b", nested.hits[0].id);
}

test "db search runtime indexing stats does not scan primary docs when full text count is available" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
        .text_merge = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"content\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"content\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(u64, 2), try db.primaryDocCount(alloc));

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_count);
    try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
    try std.testing.expectEqualStrings("ft_v1", stats.indexes[0].name);
    try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].doc_count);
}

test "db search runtime indexing sparse vector index routes knn search" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"sparse\":{\"indices\":[0,2],\"values\":[0.9,0.1]},\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"sparse\":{\"indices\":[1],\"values\":[0.9]},\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    var result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{0},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    var identity_txn = try db.core.store.beginProbeTxn();
    defer identity_txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &identity_txn, "doc:a")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, ordinal), result.hits[0].doc_ordinal);
}

test "db search runtime indexing sparse vector index routes knn search with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"sparse\":{\"indices\":[0,2],\"values\":[0.9,0.1]},\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"sparse\":{\"indices\":[1],\"values\":[0.9]},\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    var result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{0},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db search runtime indexing graph index routes neighbor queries and doc deletes clean edges" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
    });

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"parent\",\"topology\":\"tree\"}]}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:c", .edge_type = "links", .weight = 2.0 },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].id);

    try db.batch(.{
        .deletes = &.{"doc:b"},
        .sync_level = .full_index,
    });

    var after_delete = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer after_delete.deinit();

    try std.testing.expectEqual(@as(u32, 1), after_delete.total_hits);
    try std.testing.expectEqualStrings("doc:c", after_delete.hits[0].id);
}

test "db search runtime indexing direct graph writes persist graph artifacts and deletes remove them" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.25, .metadata_json = "{\"kind\":\"cite\"}" },
        },
        .sync_level = .full_index,
    });

    const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:a", "gr_v1");
    defer alloc.free(prefix);
    const after_write = try db.core.scanStorePrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, after_write);
    try std.testing.expectEqual(@as(usize, 1), after_write.len);
    try std.testing.expect(internal_keys.isGraphEdgeArtifactKey(after_write[0].key));
    var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, after_write[0].value);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(f64, 1.25), decoded.weight);
    try std.testing.expectEqualStrings("{\"kind\":\"cite\"}", decoded.metadata_json);

    try db.batch(.{
        .graph_deletes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links" },
        },
        .sync_level = .full_index,
    });

    const after_delete = try db.core.scanStorePrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, after_delete);
    try std.testing.expectEqual(@as(usize, 0), after_delete.len);
}

test "db search runtime indexing delete artifact cleanup isolates binary document id prefixes" {
    const DB = @import("mod.zig").DB;
    const putDenseEmbeddingArtifactForTest = TestHelpers.putDenseEmbeddingArtifactForTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const first_doc = "doc\x00a";
    const second_doc = "doc\x00a:child";

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = first_doc, .value = "{\"title\":\"first\"}" },
            .{ .key = second_doc, .value = "{\"title\":\"second\"}" },
        },
    });

    const first_artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, first_doc, "dense\x00idx");
    defer alloc.free(first_artifact);
    const second_artifact = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, second_doc, "dense\x00idx");
    defer alloc.free(second_artifact);
    try putDenseEmbeddingArtifactForTest(&db, alloc, first_artifact, null, &.{ 1.0, 0.0 });
    try putDenseEmbeddingArtifactForTest(&db, alloc, second_artifact, null, &.{ 0.0, 1.0 });

    try db.batch(.{ .deletes = &.{first_doc} });

    try std.testing.expect((try db.get(alloc, first_doc)) == null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, first_artifact));

    const remaining_doc = (try db.get(alloc, second_doc)) orelse return error.TestExpectedEqual;
    defer alloc.free(remaining_doc);
    try std.testing.expectEqualStrings("{\"title\":\"second\"}", remaining_doc);

    const remaining_artifact = try db.core.store.get(alloc, second_artifact);
    defer alloc.free(remaining_artifact);
    try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, remaining_artifact, null, 2);
}

test "db search runtime indexing graph index routes neighbor queries and doc deletes clean edges with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
    });

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"parent\",\"topology\":\"tree\"}]}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:c", .edge_type = "links", .weight = 2.0 },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:c", result.hits[1].id);

    try db.batch(.{
        .deletes = &.{"doc:b"},
        .sync_level = .full_index,
    });

    var after_delete = try db.search(alloc, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    });
    defer after_delete.deinit();

    try std.testing.expectEqual(@as(u32, 1), after_delete.total_hits);
    try std.testing.expectEqualStrings("doc:c", after_delete.hits[0].id);
}

test "db search runtime indexing document _edges reconcile graph state and preserve base document" {
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\",\"weight\":1.0},{\"target\":\"doc:c\",\"weight\":0.5}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_edges\"") == null);
    const graph_prefix_before = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:a", "gr_v1");
    defer alloc.free(graph_prefix_before);
    const graph_artifacts_before = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_before);
    try std.testing.expectEqual(@as(usize, 2), graph_artifacts_before.len);

    var before = try waitForSearchResult(alloc, &db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 2);
    defer before.deinit();

    try std.testing.expectEqual(@as(u32, 2), before.total_hits);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\",\"weight\":0.9}]}}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_edges\"") == null);
    const graph_artifacts_after = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_after);
    try std.testing.expectEqual(@as(usize, 1), graph_artifacts_after.len);

    var after = try waitForSearchResult(alloc, &db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 1);
    defer after.deinit();

    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:c", after.hits[0].id);
}

test "db search runtime indexing document _edges reconcile graph state and preserve base document with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const waitForSearchResult = TestHelpers.waitForSearchResult;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\",\"weight\":1.0},{\"target\":\"doc:c\",\"weight\":0.5}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_edges\"") == null);
    const graph_prefix_before = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:a", "gr_v1");
    defer alloc.free(graph_prefix_before);
    const graph_artifacts_before = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_before);
    try std.testing.expectEqual(@as(usize, 2), graph_artifacts_before.len);

    var before = try waitForSearchResult(alloc, &db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 2);
    defer before.deinit();

    try std.testing.expectEqual(@as(u32, 2), before.total_hits);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\",\"weight\":0.9}]}}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_edges\"") == null);
    const graph_artifacts_after = try db.core.scanStorePrefix(alloc, graph_prefix_before);
    defer docstore_mod.DocStore.freeResults(alloc, graph_artifacts_after);
    try std.testing.expectEqual(@as(usize, 1), graph_artifacts_after.len);

    var after = try waitForSearchResult(alloc, &db, .{
        .query = .{ .graph = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{ .edge_types = &.{"links"} },
        } },
        .limit = 10,
    }, 1);
    defer after.deinit();

    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:c", after.hits[0].id);
}

test "db search runtime indexing document _embeddings update vector index and strip stored special fields" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_embeddings\"") == null);

    var before = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer before.deinit();

    try std.testing.expectEqualStrings("doc:a", before.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dv_v1\":[0,0,1]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_embeddings\"") == null);

    var after = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer after.deinit();

    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db search runtime indexing dense and sparse field-backed vector indexes strip vector fields and persist embedding artifacts" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.addIndex(.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"body\",\"dims\":3,\"metric\":\"cosine\",\"embedding_name\":\"semantic_idx\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,0,0],\"sparse\":{\"indices\":[7],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"embedding\":[0,1,0],\"sparse\":{\"indices\":[3],\"values\":[1.0]}}" },
            .{ .key = "doc:c", .value = "{\"embedding\":[0,0,1],\"sparse\":{\"indices\":[11],\"values\":[1.0]}}" },
        },
        .sync_level = .full_index,
    });

    const stored = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"sparse\"") == null);

    const vector_only_stored = (try db.get(alloc, "doc:c")).?;
    defer alloc.free(vector_only_stored);
    try std.testing.expectEqualStrings("{}", vector_only_stored);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:managed", .value = "{\"body\":\"managed embedding source text\",\"embedding\":[1,0,0],\"sparse\":{\"indices\":[13],\"values\":[1.0]}}" },
        },
        .sync_level = .write,
    });
    const managed_stored = (try db.get(alloc, "doc:managed")).?;
    defer alloc.free(managed_stored);
    try std.testing.expect(std.mem.indexOf(u8, managed_stored, "\"body\":\"managed embedding source text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, managed_stored, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, managed_stored, "\"sparse\"") == null);

    const dense_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(dense_artifact_key);
    const dense_artifact = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(dense_artifact);
    const sparse_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(sparse_artifact_key);
    const sparse_artifact = try db.core.store.get(alloc, sparse_artifact_key);
    defer alloc.free(sparse_artifact);

    var dense = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer dense.deinit();
    try std.testing.expectEqualStrings("doc:a", dense.hits[0].id);

    var sparse = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{7},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
    });
    defer sparse.deinit();
    try std.testing.expectEqualStrings("doc:a", sparse.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[0,0,1],\"sparse\":{\"indices\":[11],\"values\":[1.0]}}" },
        },
        .sync_level = .full_index,
    });

    const updated = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(updated);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"embedding\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, updated, "\"sparse\"") == null);
    const updated_dense_artifact = try db.core.store.get(alloc, dense_artifact_key);
    defer alloc.free(updated_dense_artifact);

    var dense_after = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 3,
        } },
        .limit = 3,
    });
    defer dense_after.deinit();
    try std.testing.expectEqualStrings("doc:a", dense_after.hits[0].id);
}

test "db search runtime indexing document _embeddings update vector index and strip stored special fields with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dv_v1\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_before = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_before);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_before, "\"_embeddings\"") == null);

    var before = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer before.deinit();

    try std.testing.expectEqualStrings("doc:a", before.hits[0].id);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dv_v1\":[0,0,1]}}" },
        },
        .sync_level = .full_index,
    });

    const stored_after = (try db.get(alloc, "doc:a")).?;
    defer alloc.free(stored_after);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, stored_after, "\"_embeddings\"") == null);

    var after = try db.search(alloc, .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer after.deinit();

    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db search runtime indexing full_index persists explicit dense embeddings across reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var iter: usize = 0;
    while (iter < 4) : (iter += 1) {
        {
            var db = try DB.open(alloc, std.mem.span(path), .{});
            defer db.close();

            try db.addIndex(.{
                .name = "dense_idx",
                .kind = .dense_vector,
                .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
            });

            try db.batch(.{
                .writes = &.{
                    .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                    .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                    .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
                },
                .sync_level = .full_index,
            });

            const stats = try db.stats(alloc);
            defer types.freeDBStats(alloc, stats);
            try std.testing.expectEqual(@as(u64, 3), stats.indexes[0].doc_count);
        }

        {
            var reopened = try DB.open(alloc, std.mem.span(path), .{});
            defer reopened.close();

            const stats = try reopened.stats(alloc);
            defer types.freeDBStats(alloc, stats);
            try std.testing.expectEqual(@as(u64, 3), stats.indexes[0].doc_count);

            var result = try reopened.search(alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 3,
                } },
                .limit = 3,
            });
            defer result.deinit();

            try std.testing.expectEqual(@as(u32, 3), result.total_hits);
            try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
        }

        TestHelpers.cleanupTempDir(path);
    }
}

test "db search runtime graph composition search supports graph result_ref from full-text hits" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"title\":\"intro\",\"body\":\"machine learning systems\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:2\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"title\":\"followup\",\"body\":\"retrieval and ranking\"}" },
            .{ .key = "paper:3", .value = "{\"title\":\"other\",\"body\":\"graph traversal\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$full_text_results", .limit = 0 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("paper:1", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqualStrings("citations", result.graph_results[0].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("paper:2", result.graph_results[0].hits[0].id);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_count >= 1);
}

test "db search runtime graph composition search supports graph result_ref from dense hits without public id handoff" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"cosine\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"embedding\":[1,0],\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:2\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"embedding\":[0,1]}" },
            .{ .key = "paper:3", .value = "{\"embedding\":[-1,0]}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 1 },
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$embeddings_results", .limit = 0 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expect(result.hits.len >= 1);
    try std.testing.expectEqualStrings("paper:1", result.hits[0].id);
    var identity_txn = try db.core.store.beginProbeTxn();
    defer identity_txn.abort();
    try std.testing.expectEqual(try doc_identity.lookupOrdinalTxn(alloc, &identity_txn, "paper:1"), result.hits[0].doc_ordinal);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqualStrings("citations", result.graph_results[0].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("paper:2", result.graph_results[0].hits[0].id);
}

test "db search runtime graph composition search rejects unbounded graph result_ref when base result is paged" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"title\":\"intro\",\"body\":\"machine learning systems\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:3\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"title\":\"followup\",\"body\":\"machine learning ranking\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:3\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:3", .value = "{\"title\":\"target\",\"body\":\"graph traversal\"}" },
        },
        .sync_level = .full_index,
    });

    try std.testing.expectError(error.UnsupportedQueryRequest, db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine" } },
        .index_name = "ft_v1",
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$full_text_results", .limit = 0 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 1,
    }));
}

test "db search runtime graph composition search supports graph-only named queries" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "n:a", .value = "{\"title\":\"A\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:b\"}]}}}" },
            .{ .key = "n:b", .value = "{\"title\":\"B\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"n:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), result.hits.len);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("n:b", result.graph_results[0].hits[0].id);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_count >= 1);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_docs >= 1);
}

test "db search runtime graph composition named graph input sets carry resolved doc sets" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "n:a", .value = "{\"title\":\"A\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:b\"}]}}}" },
            .{ .key = "n:b", .value = "{\"title\":\"B\"}" },
        },
        .sync_level = .full_index,
    });

    const graph_queries = [_]types.NamedGraphQuery{.{
        .name = "neighbors",
        .query = .{
            .query_type = .neighbors,
            .index_name = "gr_v1",
            .start_nodes = .{ .result_ref = .{ .ref = "seed", .limit = 0 } },
            .params = .{ .direction = .out, .edge_types = &.{"links"} },
        },
    }};
    const input_sets = [_]types.NamedGraphInputSet{.{
        .name = "seed",
        .hit_ids = &.{"n:a"},
    }};

    try std.testing.expectError(error.UnsupportedQueryRequest, db.executeNamedGraphQueries(alloc, .{}, &graph_queries, &input_sets));

    const current_generation = db.core.nextDerivedSequence();
    const seed_ordinal = (try db.searchRuntimeLookupLiveDocOrdinalNoLock(alloc, "n:a", current_generation)) orelse return error.TestUnexpectedResult;
    var identity_txn = try db.core.store.beginProbeTxn();
    defer identity_txn.abort();
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, seed_ordinal), try doc_identity.lookupOrdinalTxn(alloc, &identity_txn, "n:a"));

    const results = try db.executeNamedGraphQueries(alloc, .{
        .identity_read_generation = current_generation,
    }, &graph_queries, &input_sets);
    defer {
        for (results) |*result| result.deinit(alloc);
        if (results.len > 0) alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(u32, 1), results[0].total_hits);
    try std.testing.expectEqualStrings("n:b", results[0].hits[0].id);
    try std.testing.expectEqual(try doc_identity.lookupOrdinalTxn(alloc, &identity_txn, "n:b"), results[0].hits[0].doc_ordinal);
    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_count >= 1);
    try std.testing.expect(stats.doc_set_planning.ordinal_list_docs >= 1);

    const paged_input_sets = [_]types.NamedGraphInputSet{.{
        .name = "seed",
        .hit_ids = &.{"n:a"},
        .total_hits = 2,
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, db.executeNamedGraphQueries(alloc, .{
        .identity_read_generation = current_generation,
    }, &graph_queries, &paged_input_sets));

    try std.testing.expectError(error.UnsupportedQueryRequest, db.executeNamedGraphQueries(alloc, .{
        .identity_read_generation = current_generation -| 1,
    }, &graph_queries, &input_sets));
}

test "db search runtime graph composition search supports fused graph selectors for single-lane full-text searches" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "paper:1", .value = "{\"title\":\"intro\",\"body\":\"machine learning systems\",\"_edges\":{\"gr_v1\":{\"cites\":[{\"target\":\"paper:2\",\"weight\":1.0}]}}}" },
            .{ .key = "paper:2", .value = "{\"title\":\"followup\",\"body\":\"retrieval and ranking\"}" },
            .{ .key = "paper:3", .value = "{\"title\":\"other\",\"body\":\"graph traversal\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .graph_queries = &.{
            .{
                .name = "citations",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$fused_results", .limit = 5 } },
                    .params = .{ .direction = .out, .edge_types = &.{"cites"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("paper:1", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqualStrings("citations", result.graph_results[0].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("paper:2", result.graph_results[0].hits[0].id);
}

test "db search runtime graph composition search fuses full_text and dense named searches before graph expansion" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0]},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"ranking systems\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "$full_text_results", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 1.0 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$fused_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search runtime graph composition hybrid search does not hard-filter dense leg with scoring full_text" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"semantic alpha concept\",\"_embeddings\":{\"dv_v1\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"keyword quickstart only\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"body\":\"plain body unrelated\",\"_embeddings\":{\"dv_v1\":[0,0,1]}}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "quickstart" } },
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 3,
                },
            },
        },
        .merge_config = .{
            .strategy = .rsf,
            .window_size = 10,
            .weights = &.{
                .{ .name = "$full_text_results", .weight = 0.4 },
                .{ .name = "dv_v1", .weight = 1.0 },
            },
        },
        .limit = 3,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 2);
    var saw_semantic_only_hit = false;
    var saw_text_hit = false;
    for (result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:a")) saw_semantic_only_hit = true;
        if (std.mem.eql(u8, hit.id, "doc:b")) saw_text_hit = true;
    }
    try std.testing.expect(saw_semantic_only_hit);
    try std.testing.expect(saw_text_hit);
}

test "db search runtime graph composition search fuses full_text and dense named searches before graph expansion with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0]},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"ranking systems\",\"_embeddings\":{\"dv_v1\":[0,1,0]}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .index_name = "ft_v1",
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "$full_text_results", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 1.0 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$fused_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search runtime graph composition search supports named full_text queries fused with dense and sparse before graph expansion" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sv_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0],\"sv_v1\":{\"indices\":[1],\"values\":[1.0]}},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"machine learning beta\",\"_embeddings\":{\"dv_v1\":[0.7,0.3,0],\"sv_v1\":{\"indices\":[1],\"values\":[0.4]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text_queries = &.{
            .{
                .name = "ft_body",
                .index_name = "ft_v1",
                .query = .{ .match = .{ .field = "body", .text = "machine learning" } },
            },
        },
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sv_v1",
                .index_name = "sv_v1",
                .query = .{
                    .indices = &.{1},
                    .values = &.{1.0},
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "ft_body", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 2.0 },
                .{ .name = "sv_v1", .weight = 1.5 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$fused_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search runtime graph composition search supports named full_text queries fused with dense and sparse before graph expansion with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sv_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse_embedding\"}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"machine learning\",\"_embeddings\":{\"dv_v1\":[1,0,0],\"sv_v1\":{\"indices\":[1],\"values\":[1.0]}},\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"machine learning beta\",\"_embeddings\":{\"dv_v1\":[0.7,0.3,0],\"sv_v1\":{\"indices\":[1],\"values\":[0.4]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"graph target\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text_queries = &.{
            .{
                .name = "ft_body",
                .index_name = "ft_v1",
                .query = .{ .match = .{ .field = "body", .text = "machine learning" } },
            },
        },
        .dense_queries = &.{
            .{
                .name = "dv_v1",
                .index_name = "dv_v1",
                .query = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 2,
                },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sv_v1",
                .index_name = "sv_v1",
                .query = .{
                    .indices = &.{1},
                    .values = &.{1.0},
                    .k = 2,
                },
            },
        },
        .merge_config = .{
            .strategy = .rrf,
            .weights = &.{
                .{ .name = "ft_body", .weight = 1.0 },
                .{ .name = "dv_v1", .weight = 2.0 },
                .{ .name = "sv_v1", .weight = 1.5 },
            },
        },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$fused_results", .limit = 1 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expect(result.total_hits >= 1);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[0].hits[0].id);
}

test "db search runtime graph composition search sorts graph queries by dependency and resolves prior graph results" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"A\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"B\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:c\"}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"C\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "second_hop",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.first_hop", .limit = 5 } },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
            .{
                .name = "first_hop",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.graph_results.len);
    try std.testing.expectEqualStrings("first_hop", result.graph_results[0].name);
    try std.testing.expectEqualStrings("doc:b", result.graph_results[0].hits[0].id);
    try std.testing.expectEqualStrings("second_hop", result.graph_results[1].name);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[1].total_hits);
    try std.testing.expectEqualStrings("doc:c", result.graph_results[1].hits[0].id);
}

test "db search runtime graph composition search expand_strategy union and intersection apply graph hits to top level results" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"machine learning\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"doc:b\"}]}}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"machine learning beta\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"other topic\"}" },
        },
        .sync_level = .full_index,
    });

    var union_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .expand_strategy = .@"union",
        .limit = 10,
    });
    defer union_result.deinit();

    try std.testing.expectEqual(@as(u32, 2), union_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", union_result.hits[0].id);
    try std.testing.expectEqualStrings("doc:b", union_result.hits[1].id);

    var intersection_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "machine learning" } },
        .graph_queries = &.{
            .{
                .name = "neighbors",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "gr_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"links"} },
                },
            },
        },
        .expand_strategy = .intersection,
        .limit = 10,
    });
    defer intersection_result.deinit();

    try std.testing.expectEqual(@as(u32, 1), intersection_result.total_hits);
    try std.testing.expectEqualStrings("doc:b", intersection_result.hits[0].id);
}

test "db search runtime graph composition graph result merging preserves artifact refs" {
    const alloc = std.testing.allocator;

    var result = types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
        .graph_results = try alloc.alloc(types.GraphSearchResult, 1),
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .score = 1.0,
    };

    result.graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
    };
    result.graph_results[0].hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:Ym9keV9jaHVua3NfdjE:0"),
        .score = 0.75,
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "body_chunks_v1"),
            .kind = .chunk,
            .chunk_id = 0,
        },
    };

    try applyGraphUnion(alloc, &result);

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    const merged_ref = result.hits[1].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.ArtifactKind.chunk, merged_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), merged_ref.chunk_id);
    try std.testing.expectEqualStrings("doc:a", merged_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", merged_ref.name);
}

test "db search runtime graph composition graph intersection preserves artifact refs" {
    const alloc = std.testing.allocator;

    var result = types.SearchResult{
        .alloc = alloc,
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
        .graph_results = try alloc.alloc(types.GraphSearchResult, 1),
    };
    defer result.deinit();

    result.hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:Ym9keV9jaHVua3NfdjE:0"),
        .score = 0.75,
        .artifact_ref = .{
            .document_id = try alloc.dupe(u8, "doc:a"),
            .name = try alloc.dupe(u8, "body_chunks_v1"),
            .kind = .chunk,
            .chunk_id = 0,
        },
    };

    result.graph_results[0] = .{
        .name = try alloc.dupe(u8, "neighbors"),
        .hits = try alloc.alloc(types.SearchHit, 1),
        .total_hits = 1,
    };
    result.graph_results[0].hits[0] = .{
        .id = try alloc.dupe(u8, "af1:chunk:ZG9jOmE:Ym9keV9jaHVua3NfdjE:0"),
        .score = 0.75,
    };

    try applyGraphIntersection(alloc, &result);

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    const kept_ref = result.hits[0].artifact_ref orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(types.ArtifactKind.chunk, kept_ref.kind);
    try std.testing.expectEqual(@as(?u32, 0), kept_ref.chunk_id);
    try std.testing.expectEqualStrings("doc:a", kept_ref.document_id);
    try std.testing.expectEqualStrings("body_chunks_v1", kept_ref.name);
}

test "db search runtime graph composition graph index reloads on reopen for neighbor queries" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "gr_v1",
            .kind = .graph,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
            },
        });

        try db.batch(.{
            .graph_writes = &.{
                .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
                .{ .index_name = "gr_v1", .source = "doc:b", .target = "doc:c", .edge_type = "links", .weight = 1.0 },
            },
        });
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .query = .{ .graph = .{
                .query_type = .neighbors,
                .index_name = "gr_v1",
                .start_nodes = .{ .keys = &.{"doc:a"} },
                .params = .{ .edge_types = &.{"links"} },
            } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }
}

test "db search runtime graph composition graph index reloads on reopen for neighbor queries with durable lsm primary backend" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const primary_backend: db_config.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "gr_v1",
            .kind = .graph,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
            },
        });

        try db.batch(.{
            .graph_writes = &.{
                .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "links", .weight = 1.0 },
                .{ .index_name = "gr_v1", .source = "doc:b", .target = "doc:c", .edge_type = "links", .weight = 1.0 },
            },
        });
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .primary_backend = primary_backend,
        });
        defer reopened.close();

        var result = try reopened.search(alloc, .{
            .query = .{ .graph = .{
                .query_type = .neighbors,
                .index_name = "gr_v1",
                .start_nodes = .{ .keys = &.{"doc:a"} },
                .params = .{ .edge_types = &.{"links"} },
            } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
    }
}

test "db search runtime graph composition graph reverse rebuild resumes after interrupted reopen" {
    const DB = @import("mod.zig").DB;
    const threadedIo = db_internal.threadedIo;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "gr_v1",
            .kind = .graph,
            .config_json = "{}",
        });

        var graph_writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
        defer {
            for (graph_writes.items) |item| {
                alloc.free(@constCast(item.source));
                alloc.free(@constCast(item.target));
                alloc.free(@constCast(item.edge_type));
            }
            graph_writes.deinit(alloc);
        }

        for (0..1500) |i| {
            try graph_writes.append(alloc, .{
                .index_name = "gr_v1",
                .source = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .target = try alloc.dupe(u8, "doc:target"),
                .edge_type = try alloc.dupe(u8, "links"),
            });
        }

        try db.batch(.{
            .graph_writes = graph_writes.items,
            .sync_level = .full_index,
        });
    }

    const reverse_path = try std.fmt.allocPrint(alloc, "{s}/indexes/gr_v1/reverse", .{std.mem.span(path)});
    defer alloc.free(reverse_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), reverse_path) catch |err| switch (err) {
        error.NotDir => try std.Io.Dir.cwd().deleteFile(io_impl.io(), reverse_path),
        else => return err,
    };
    var reverse_dir = std.Io.Dir.cwd().openDir(io_impl.io(), reverse_path, .{}) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (reverse_dir) |*dir| {
        dir.close(io_impl.io());
        return error.TestUnexpectedResult;
    }

    graph_mod.test_abort_reverse_rebuild_after_batches = 1;
    defer graph_mod.test_abort_reverse_rebuild_after_batches = null;
    {
        var interrupted = try DB.open(alloc, std.mem.span(path), .{});
        defer interrupted.close();
        try std.testing.expect(interrupted.core.index_manager.hasLoadFailures());
        try std.testing.expectEqualStrings(
            "TestInjectedBackfillFailure",
            interrupted.core.index_manager.loadFailure("gr_v1") orelse return error.TestExpectedEqual,
        );
    }

    const state_path = try std.fmt.allocPrint(alloc, "{s}/indexes/gr_v1/rebuild.state", .{std.mem.span(path)});
    defer alloc.free(state_path);
    const interrupted_state = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path, alloc, .limited(1024));
    defer alloc.free(interrupted_state);
    try std.testing.expect(interrupted_state.len > 0);

    graph_mod.test_abort_reverse_rebuild_after_batches = null;

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().readFileAlloc(std.testing.io, state_path, alloc, .limited(1024)));

    const incoming = try reopened.getEdges(alloc, "gr_v1", "doc:target", "links", .in);
    defer graph_mod.GraphIndex.freeEdges(alloc, incoming);
    try std.testing.expectEqual(@as(usize, 1500), incoming.len);

    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    for (stats.indexes) |entry| {
        if (std.mem.eql(u8, entry.name, "gr_v1")) {
            try std.testing.expectEqual(false, entry.backfill_active);
            try std.testing.expectEqual(@as(u64, 1500), entry.edge_count);
            return;
        }
    }
    return error.TestExpectedEqual;
}

test "db search runtime text schema runUntilIdle drains scheduled text merges after repeated writes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    for (0..12) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc {d}\",\"body\":\"common token {d}\"}}", .{ i, i });
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{
                .key = key,
                .value = value,
            }},
            .sync_level = .write,
        });
    }

    try db.runUntilIdle();

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expect(text_index.snapshot().segments.len <= index_manager_mod.default_text_merge_max_segments_per_tier);

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
        .limit = 20,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 12), result.total_hits);
}

test "db search runtime text schema runUntilIdle drains scheduled text merges without index workers" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expect(db.text_merge_runtime == null);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    for (0..6) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc {d}\",\"body\":\"common token {d}\"}}", .{ i, i });
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{
                .key = key,
                .value = value,
            }},
            .sync_level = .write,
        });
    }

    const before = db.pendingWorkStats().text_merge;

    try db.runUntilIdle();

    const after = db.pendingWorkStats().text_merge;
    try std.testing.expect(after.pending_segments <= before.pending_segments);

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expect(text_index.snapshot().segments.len < 6);
}

test "db search runtime text schema full_text sync does not require draining scheduled text merges" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

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
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_text,
        });
    }

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 6), result.total_hits);

    const before = db.pendingWorkStats().text_merge;
    try db.drainScheduledTextMerges();
    const after = db.pendingWorkStats().text_merge;
    try std.testing.expect(after.pending_segments <= before.pending_segments);
}

test "db search runtime text schema force compacts text index to searchable merge tier" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    for (0..12) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_index,
        });
    }

    try db.batch(.{
        .deletes = &.{"doc:3"},
        .sync_level = .full_index,
    });

    try db.forceCompactTextIndexes();

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expect(text_index.snapshot().segments.len <= index_manager_mod.default_text_merge_max_segments_per_tier);

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
        .limit = 20,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 11), result.total_hits);
    for (result.hits) |hit| {
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:3"));
    }
}

test "db search runtime text schema text compaction preserves ordinal filters across reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const expected_ordinal: doc_set.DocOrdinal = 9;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
        });
        defer db.close();

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        for (0..12) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
            defer alloc.free(key);
            const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
            defer alloc.free(value);

            try db.batch(.{
                .writes = &.{.{ .key = key, .value = value }},
                .sync_level = .full_index,
            });
        }

        var include = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:8"});
        errdefer include.deinit(alloc);
        const resolved_ordinal = switch (include) {
            .ordinals => |ordinals| blk: {
                try std.testing.expectEqual(@as(usize, 1), ordinals.len);
                break :blk ordinals[0];
            },
            else => return error.ExpectedOrdinalDocSet,
        };
        try std.testing.expectEqual(expected_ordinal, resolved_ordinal);

        var filter = doc_set.ResolvedDocFilter{
            .include = include,
            .exclude = .none,
        };
        include = .all;
        defer filter.deinit(alloc);

        try db.forceCompactTextIndexes();

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expect(text_index.snapshot().segments.len <= index_manager_mod.default_text_merge_max_segments_per_tier);

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "common" } },
            .limit = 20,
            .resolved_doc_filter = &filter,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("doc:8", result.hits[0].id);
        try std.testing.expectEqual(@as(?doc_set.DocOrdinal, expected_ordinal), result.hits[0].doc_ordinal);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
        });
        defer reopened.close();

        var include = try reopened.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:8"});
        errdefer include.deinit(alloc);
        const resolved_ordinal = switch (include) {
            .ordinals => |ordinals| blk: {
                try std.testing.expectEqual(@as(usize, 1), ordinals.len);
                break :blk ordinals[0];
            },
            else => return error.ExpectedOrdinalDocSet,
        };
        try std.testing.expectEqual(expected_ordinal, resolved_ordinal);

        var filter = doc_set.ResolvedDocFilter{
            .include = include,
            .exclude = .none,
        };
        include = .all;
        defer filter.deinit(alloc);

        const text_index = reopened.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expect(text_index.snapshot().segments.len <= index_manager_mod.default_text_merge_max_segments_per_tier);

        var result = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "body", .text = "common" } },
            .limit = 20,
            .resolved_doc_filter = &filter,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("doc:8", result.hits[0].id);
        try std.testing.expectEqual(@as(?doc_set.DocOrdinal, expected_ordinal), result.hits[0].doc_ordinal);
    }
}

test "db search runtime text schema best effort force compact leaves text merge debt under pressure" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1024 * 1024,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_action = .defer_background_work,
        .hard_action = .defer_background_work,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{
        .budgets = budgets,
        .policies = policies,
    });
    var tracked_usage: u64 = 0;
    resource_manager.observeUsage(.text_merge_buffers, &tracked_usage, 2);
    defer resource_manager.observeUsage(.text_merge_buffers, &tracked_usage, 0);

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .resource_manager = &resource_manager,
        .text_merge = .{ .enabled = false },
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "body": {
        \\            "type": "string",
        \\            "x-antfly-types": ["text"]
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
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

    for (0..12) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_text,
        });
    }

    const text_index_before = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expect(text_index_before.snapshot().segments.len > 1);

    try db.bestEffortForceCompactTextIndexes();

    const text_index_after = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expect(text_index_after.snapshot().segments.len > 1);

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
        .limit = 20,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 12), result.total_hits);
}

test "db search runtime text schema runUntilIdle defers full text merge pressure without failing" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1,
    };
    var policies = resource_manager_mod.Options.defaultPolicies();
    policies[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = .{
        .soft_action = .defer_background_work,
        .hard_action = .reject_work,
    };
    var resource_manager = resource_manager_mod.ResourceManager.init(.{
        .budgets = budgets,
        .policies = policies,
    });

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .resource_manager = &resource_manager,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    for (0..12) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_text,
        });
    }

    const before = db.pendingWorkStats().text_merge;
    try std.testing.expect(before.pending_segments > 0);

    try db.runUntilIdle();

    const after = db.pendingWorkStats().text_merge;
    try std.testing.expect(after.pending_segments > 0);
    try std.testing.expect(after.deferred_for_pressure >= before.deferred_for_pressure);

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expect(text_index.snapshot().segments.len > 1);
}

test "db search runtime text schema search_as_you_type schema emits Elasticsearch-style field variants" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{
        \\  "default_type": "product",
        \\  "document_schemas": {
        \\    "product": {
        \\      "schema": {
        \\        "type": "object",
        \\        "additionalProperties": true,
        \\        "properties": {
        \\          "name": {
        \\            "type": "string",
        \\            "x-antfly-types": ["search_as_you_type", "keyword"]
        \\          },
        \\          "description": {
        \\            "type": "string",
        \\            "x-antfly-types": ["text"]
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
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
            .{ .key = "doc:1", .value = "{\"name\":\"Smartphone Apple iPhone\",\"description\":\"Latest iPhone model\"}" },
            .{ .key = "doc:2", .value = "{\"name\":\"Smart Television Samsung\",\"description\":\"High-definition smart TV\"}" },
            .{ .key = "doc:3", .value = "{\"name\":\"Smartwatch Fitbit\",\"description\":\"Fitness tracker\"}" },
            .{ .key = "doc:4", .value = "{\"name\":\"Gaming Console PlayStation\",\"description\":\"Next-generation gaming console\"}" },
        },
        .sync_level = .full_index,
    });

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "name._2gram", "smartphone apple"));
    try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "name._3gram", "smartphone apple iphone"));
    try std.testing.expectEqual(@as(u32, 3), try text_index.snapshot().termDocFreq(alloc, "name._index_prefix", "sm"));
    try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "name._index_prefix", "iph"));
    try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "name._index_prefix", "apple ip"));
    try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "name._index_prefix", "smartphone apple ip"));

    var ng_results = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .term = .{ .field = "name._index_prefix", .term = "sm" } },
        .limit = 10,
    });
    defer ng_results.deinit();
    try std.testing.expectEqual(@as(u32, 3), ng_results.total_hits);

    var phrase_prefix_results = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .term = .{ .field = "name._index_prefix", .term = "apple ip" } },
        .limit = 10,
    });
    defer phrase_prefix_results.deinit();
    try std.testing.expectEqual(@as(u32, 1), phrase_prefix_results.total_hits);
    try std.testing.expectEqualStrings("doc:1", phrase_prefix_results.hits[0].id);

    var prefix_results = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .prefix = .{ .field = "name", .prefix = "sm" } },
        .limit = 10,
    });
    defer prefix_results.deinit();
    try std.testing.expectEqual(@as(u32, 3), prefix_results.total_hits);

    const multi_match_fields = [_]types.TextMultiMatchField{
        .{ .field = "name" },
    };
    var bool_prefix_results = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .multi_match_bool_prefix = .{
            .query = "smartphone apple ip",
            .fields = &multi_match_fields,
        } },
        .limit = 10,
    });
    defer bool_prefix_results.deinit();
    try std.testing.expectEqual(@as(u32, 1), bool_prefix_results.total_hits);
    try std.testing.expectEqualStrings("doc:1", bool_prefix_results.hits[0].id);
}

test "db search runtime text schema versioned full text indexes reload matching schema mappings after reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_v0_json =
        \\{
        \\  "version": 0,
        \\  "default_type": "product",
        \\  "document_schemas": {
        \\    "product": {
        \\      "schema": {
        \\        "type": "object",
        \\        "additionalProperties": true,
        \\        "properties": {
        \\          "name": {
        \\            "type": "string",
        \\            "x-antfly-types": ["search_as_you_type"]
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;
    const schema_v1_json =
        \\{
        \\  "version": 1,
        \\  "default_type": "product",
        \\  "document_schemas": {
        \\    "product": {
        \\      "schema": {
        \\        "type": "object",
        \\        "additionalProperties": true,
        \\        "properties": {
        \\          "title": {
        \\            "type": "string",
        \\            "x-antfly-types": ["search_as_you_type"]
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var parsed_v0 = try table_schema_api.parseValidatedTableSchema(alloc, schema_v0_json);
        defer parsed_v0.deinit(alloc);
        const runtime_v0 = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_v0);
        defer schema_mod.freeSchema(alloc, runtime_v0);
        try db.setSchema(runtime_v0);

        try db.addIndex(.{
            .name = "full_text_index_v0",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:1", .value = "{\"name\":\"Alpha\"}" },
            },
            .sync_level = .full_index,
        });

        var parsed_v1 = try table_schema_api.parseValidatedTableSchema(alloc, schema_v1_json);
        defer parsed_v1.deinit(alloc);
        const runtime_v1 = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_v1);
        defer schema_mod.freeSchema(alloc, runtime_v1);
        try db.setSchema(runtime_v1);

        try db.addIndex(.{
            .name = "full_text_index_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:2", .value = "{\"name\":\"Gadget\",\"title\":\"Beta\"}" },
            },
            .sync_level = .full_index,
        });
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:3", .value = "{\"name\":\"Gamut\",\"title\":\"Brisk\"}" },
            },
            .sync_level = .full_index,
        });

        const text_v0 = db.core.index_manager.textIndex("full_text_index_v0").?;
        const text_v1 = db.core.index_manager.textIndex("full_text_index_v1").?;

        try std.testing.expectEqual(@as(u32, 2), try text_v0.snapshot().termDocFreq(alloc, "name._index_prefix", "ga"));
        try std.testing.expectEqual(@as(u32, 0), try text_v0.snapshot().termDocFreq(alloc, "title._index_prefix", "br"));
        try std.testing.expectEqual(@as(u32, 1), try text_v1.snapshot().termDocFreq(alloc, "title._index_prefix", "br"));
        try std.testing.expectEqual(@as(u32, 0), try text_v1.snapshot().termDocFreq(alloc, "name._index_prefix", "ga"));

        var old_index_result = try db.search(alloc, .{
            .index_name = "full_text_index_v0",
            .query = .{ .term = .{ .field = "name._index_prefix", .term = "ga" } },
            .limit = 10,
        });
        defer old_index_result.deinit();
        try std.testing.expectEqual(@as(u32, 2), old_index_result.total_hits);

        var new_index_result = try db.search(alloc, .{
            .index_name = "full_text_index_v1",
            .query = .{ .term = .{ .field = "title._index_prefix", .term = "br" } },
            .limit = 10,
        });
        defer new_index_result.deinit();
        try std.testing.expectEqual(@as(u32, 1), new_index_result.total_hits);
        try std.testing.expectEqualStrings("doc:3", new_index_result.hits[0].id);
    }
}

test "db search runtime text schema additionalProperties true nested text fields survive reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{
        \\  "version": 0,
        \\  "default_type": "product",
        \\  "document_schemas": {
        \\    "product": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "meta": {
        \\            "type": "object",
        \\            "additionalProperties": true
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

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
                .{ .key = "doc:1", .value = "{\"_type\":\"product\",\"meta\":{\"foo\":{\"title\":\"Gamma\"}}}" },
            },
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.foo.title", "gamma"));
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:2", .value = "{\"_type\":\"product\",\"meta\":{\"bar\":{\"title\":\"Delta\"}}}" },
            },
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.foo.title", "gamma"));
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.bar.title", "delta"));

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .term = .{ .field = "meta.bar.title", .term = "delta" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:2", result.hits[0].id);
    }
}

test "db search runtime text schema explicit field analyzer override drives match queries" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "title": {
        \\            "type": "string",
        \\            "x-antfly-types": ["text"],
        \\            "x-antfly-analyzer": "french"
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

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
            .{ .key = "doc:1", .value = "{\"title\":\"les maisons sont grandes\"}" },
            .{ .key = "doc:2", .value = "{\"title\":\"les voitures rapides\"}" },
        },
        .sync_level = .full_index,
    });

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "title", .text = "maison" } },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:1", result.hits[0].id);
}

test "db search runtime text schema dynamic template match_mapping_type analyzer survives reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "dynamic_templates": [
        \\    {
        \\      "name": "meta_date",
        \\      "path_match": "meta.*",
        \\      "match_mapping_type": "date",
        \\      "mapping": {
        \\        "type": "keyword"
        \\      }
        \\    },
        \\    {
        \\      "name": "meta_text",
        \\      "path_match": "meta.*",
        \\      "match_mapping_type": "string",
        \\      "mapping": {
        \\        "type": "text",
        \\        "analyzer": "french"
        \\      }
        \\    }
        \\  ],
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "meta": {
        \\            "type": "object",
        \\            "additionalProperties": true
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

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
                .{ .key = "doc:1", .value = "{\"meta\":{\"body\":\"les maisons sont grandes\",\"published\":\"2025-01-02\"}}" },
            },
            .sync_level = .full_index,
        });
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "meta.body", .text = "maisons" } },
            .limit = 10,
        });
        defer result.deinit();

        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:1", result.hits[0].id);
    }
}

test "db search runtime text schema dynamic template precedence beats open additionalProperties fallback" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "dynamic_templates": [
        \\    {
        \\      "name": "skip_meta",
        \\      "path_match": "meta.skip.*",
        \\      "mapping": {
        \\        "type": "keyword",
        \\        "index": false
        \\      }
        \\    }
        \\  ],
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "meta": {
        \\            "type": "object",
        \\            "additionalProperties": true
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

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
            .{ .key = "doc:1", .value = "{\"meta\":{\"skip\":{\"title\":\"Gamma\"},\"keep\":{\"title\":\"Delta\"}}}" },
        },
        .sync_level = .full_index,
    });

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expectEqual(@as(u32, 0), try text_index.snapshot().termDocFreq(alloc, "meta.skip.title", "gamma"));
    try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.keep.title", "delta"));
}

test "db search runtime text schema patternProperties text fields survive reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "meta": {
        \\            "type": "object",
        \\            "patternProperties": {
        \\              "^tag_[a-z]+$": {
        \\                "type": "object",
        \\                "properties": {
        \\                  "title": {
        \\                    "type": "string",
        \\                    "x-antfly-types": ["search_as_you_type"]
        \\                  }
        \\                }
        \\              }
        \\            },
        \\            "additionalProperties": false
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

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
                .{ .key = "doc:1", .value = "{\"meta\":{\"tag_blue\":{\"title\":\"Gamma\"},\"skip\":{\"title\":\"Nope\"}}}" },
            },
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.tag_blue.title._index_prefix", "ga"));
        try std.testing.expectEqual(@as(u32, 0), try text_index.snapshot().termDocFreq(alloc, "meta.skip.title._index_prefix", "no"));
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:2", .value = "{\"meta\":{\"tag_green\":{\"title\":\"Delta\"},\"skip\":{\"title\":\"Nada\"}}}" },
            },
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.tag_blue.title._index_prefix", "ga"));
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.tag_green.title._index_prefix", "de"));
        try std.testing.expectEqual(@as(u32, 0), try text_index.snapshot().termDocFreq(alloc, "meta.skip.title._index_prefix", "na"));

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .term = .{ .field = "meta.tag_green.title._index_prefix", .term = "de" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:2", result.hits[0].id);
    }
}

test "db search runtime text schema root additionalProperties true text fields survive reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "additionalProperties": true
        \\      }
        \\    }
        \\  }
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

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
                .{ .key = "doc:1", .value = "{\"foo\":\"Gamma\"}" },
            },
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "foo", "gamma"));
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:2", .value = "{\"meta\":{\"title\":\"Delta\"}}" },
            },
            .sync_level = .full_index,
        });

        const text_index = db.core.index_manager.textIndex("ft_v1").?;
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "foo", "gamma"));
        try std.testing.expectEqual(@as(u32, 1), try text_index.snapshot().termDocFreq(alloc, "meta.title", "delta"));

        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .term = .{ .field = "meta.title", .term = "delta" } },
            .limit = 10,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 1), result.total_hits);
        try std.testing.expectEqualStrings("doc:2", result.hits[0].id);
    }
}

test "db search runtime text schema schema-driven text query matrix covers explicit dynamic template and fallback modes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "dynamic_templates": [
        \\    {
        \\      "name": "dyn_text",
        \\      "path_match": "dyn.tpl_*",
        \\      "mapping": {
        \\        "type": "text"
        \\      }
        \\    }
        \\  ],
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "explicit": {
        \\            "type": "string",
        \\            "x-antfly-types": ["search_as_you_type", "keyword"]
        \\          },
        \\          "patterns": {
        \\            "type": "object",
        \\            "patternProperties": {
        \\              "^tag_[a-z]+$": {
        \\                "type": "object",
        \\                "properties": {
        \\                  "title": {
        \\                    "type": "string",
        \\                    "x-antfly-types": ["search_as_you_type"]
        \\                  }
        \\                }
        \\              }
        \\            },
        \\            "additionalProperties": false
        \\          },
        \\          "typed": {
        \\            "type": "object",
        \\            "additionalProperties": {
        \\              "type": "object",
        \\              "properties": {
        \\                "body": {
        \\                  "type": "string",
        \\                  "x-antfly-types": ["text"]
        \\                }
        \\              }
        \\            }
        \\          },
        \\          "dyn": {
        \\            "type": "object",
        \\            "additionalProperties": true
        \\          },
        \\          "open": {
        \\            "type": "object",
        \\            "additionalProperties": true
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

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
            .{ .key = "doc:explicit", .value = "{\"explicit\":\"Smart Home\"}" },
            .{ .key = "doc:pattern", .value = "{\"patterns\":{\"tag_blue\":{\"title\":\"Blue Rocket\"}}}" },
            .{ .key = "doc:typed", .value = "{\"typed\":{\"foo\":{\"body\":\"alpha beta\"}}}" },
            .{ .key = "doc:template", .value = "{\"dyn\":{\"tpl_body\":\"gamma delta\"}}" },
            .{ .key = "doc:open", .value = "{\"open\":{\"title\":\"open galaxy\"}}" },
        },
        .sync_level = .full_index,
    });

    var explicit_ngram = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .term = .{ .field = "explicit._index_prefix", .term = "sm" } },
        .limit = 10,
    });
    defer explicit_ngram.deinit();
    try std.testing.expectEqual(@as(u32, 1), explicit_ngram.total_hits);
    try std.testing.expectEqualStrings("doc:explicit", explicit_ngram.hits[0].id);

    var explicit_prefix = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .prefix = .{ .field = "explicit", .prefix = "sm" } },
        .limit = 10,
    });
    defer explicit_prefix.deinit();
    try std.testing.expectEqual(@as(u32, 1), explicit_prefix.total_hits);
    try std.testing.expectEqualStrings("doc:explicit", explicit_prefix.hits[0].id);

    var pattern_ngram = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .term = .{ .field = "patterns.tag_blue.title._index_prefix", .term = "bl" } },
        .limit = 10,
    });
    defer pattern_ngram.deinit();
    try std.testing.expectEqual(@as(u32, 1), pattern_ngram.total_hits);
    try std.testing.expectEqualStrings("doc:pattern", pattern_ngram.hits[0].id);

    var typed_phrase = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match_phrase = .{ .field = "typed.foo.body", .text = "alpha beta" } },
        .limit = 10,
    });
    defer typed_phrase.deinit();
    try std.testing.expectEqual(@as(u32, 1), typed_phrase.total_hits);
    try std.testing.expectEqualStrings("doc:typed", typed_phrase.hits[0].id);

    var template_match = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "dyn.tpl_body", .text = "gamma" } },
        .limit = 10,
    });
    defer template_match.deinit();
    try std.testing.expectEqual(@as(u32, 1), template_match.total_hits);
    try std.testing.expectEqualStrings("doc:template", template_match.hits[0].id);

    var open_phrase = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match_phrase = .{ .field = "open.title", .text = "open galaxy" } },
        .limit = 10,
    });
    defer open_phrase.deinit();
    try std.testing.expectEqual(@as(u32, 1), open_phrase.total_hits);
    try std.testing.expectEqualStrings("doc:open", open_phrase.hits[0].id);
}

test "db search runtime text schema no-schema path recursively infers nested text and typed fields" {
    const DB = @import("mod.zig").DB;
    const parsePatternRfc3339ToNs = db_internal.parseRfc3339ToNs;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const jan2 = (try parsePatternRfc3339ToNs("2026-01-02T00:00:00Z")).?;
    const jan4 = (try parsePatternRfc3339ToNs("2026-01-04T00:00:00Z")).?;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:1", .value = "{\"meta\":{\"title\":\"alpha beta\",\"score\":10,\"published_at\":\"2026-01-01T00:00:00Z\",\"location\":{\"lat\":37.7749,\"lon\":-122.4194}}}" },
                .{ .key = "doc:2", .value = "{\"meta\":{\"title\":\"gamma delta\",\"score\":20,\"published_at\":\"2026-01-03T00:00:00Z\",\"location\":{\"lat\":40.7128,\"lon\":-74.0060}}}" },
            },
            .sync_level = .full_index,
        });
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var text = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "meta.title", .text = "alpha" } },
            .limit = 10,
        });
        defer text.deinit();
        try std.testing.expectEqual(@as(u32, 1), text.total_hits);
        try std.testing.expectEqualStrings("doc:1", text.hits[0].id);

        var numeric = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .numeric_range = .{
                .field = "meta.score",
                .min = 15,
                .max = 25,
                .inclusive_min = true,
                .inclusive_max = true,
            } },
            .limit = 10,
        });
        defer numeric.deinit();
        try std.testing.expectEqual(@as(u32, 1), numeric.total_hits);
        try std.testing.expectEqualStrings("doc:2", numeric.hits[0].id);

        var date = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .date_range = .{
                .field = "meta.published_at",
                .start_ns = jan2,
                .end_ns = jan4,
                .inclusive_start = true,
                .inclusive_end = false,
            } },
            .limit = 10,
        });
        defer date.deinit();
        try std.testing.expectEqual(@as(u32, 1), date.total_hits);
        try std.testing.expectEqualStrings("doc:2", date.hits[0].id);

        var bbox = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .geo_bbox = .{
                .field = "meta.location",
                .min_lat = 37.70,
                .min_lon = -122.50,
                .max_lat = 37.80,
                .max_lon = -122.40,
            } },
            .limit = 10,
        });
        defer bbox.deinit();
        try std.testing.expectEqual(@as(u32, 1), bbox.total_hits);
        try std.testing.expectEqualStrings("doc:1", bbox.hits[0].id);
    }
}

test "db search runtime text schema schema-present infer_types opt-in recursively infers nested fields after reopen" {
    const DB = @import("mod.zig").DB;
    const parsePatternRfc3339ToNs = db_internal.parseRfc3339ToNs;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const jan2 = (try parsePatternRfc3339ToNs("2026-01-02T00:00:00Z")).?;
    const jan4 = (try parsePatternRfc3339ToNs("2026-01-04T00:00:00Z")).?;

    const schema_json =
        \\{
        \\  "default_type": "doc",
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "meta": {
        \\            "type": "object",
        \\            "additionalProperties": true,
        \\            "x-antfly-dynamic-indexing": {
        \\              "mode": "infer_types"
        \\            }
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

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
                .{ .key = "doc:1", .value = "{\"meta\":{\"title\":\"alpha beta\"}}" },
                .{ .key = "doc:2", .value = "{\"meta\":{\"score\":20,\"published_at\":\"2026-01-03T00:00:00Z\",\"location\":{\"lat\":40.7128,\"lon\":-74.0060}}}" },
            },
            .sync_level = .full_index,
        });
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var text = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match = .{ .field = "meta.title", .text = "alpha" } },
            .limit = 10,
        });
        defer text.deinit();
        try std.testing.expectEqual(@as(u32, 1), text.total_hits);
        try std.testing.expectEqualStrings("doc:1", text.hits[0].id);

        var numeric = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .numeric_range = .{
                .field = "meta.score",
                .min = 15,
                .max = 25,
                .inclusive_min = true,
                .inclusive_max = true,
            } },
            .limit = 10,
        });
        defer numeric.deinit();
        try std.testing.expectEqual(@as(u32, 1), numeric.total_hits);
        try std.testing.expectEqualStrings("doc:2", numeric.hits[0].id);

        var date = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .date_range = .{
                .field = "meta.published_at",
                .start_ns = jan2,
                .end_ns = jan4,
                .inclusive_start = true,
                .inclusive_end = false,
            } },
            .limit = 10,
        });
        defer date.deinit();
        try std.testing.expectEqual(@as(u32, 1), date.total_hits);
        try std.testing.expectEqualStrings("doc:2", date.hits[0].id);

        var bbox = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .geo_bbox = .{
                .field = "meta.location",
                .min_lat = 40.70,
                .min_lon = -74.02,
                .max_lat = 40.72,
                .max_lon = -74.00,
            } },
            .limit = 10,
        });
        defer bbox.deinit();
        try std.testing.expectEqual(@as(u32, 1), bbox.total_hits);
        try std.testing.expectEqualStrings("doc:2", bbox.hits[0].id);
    }
}

test "db search runtime identity match_all consumes resolved ordinal filter" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\"}" },
        },
    });

    var include = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:b"});
    errdefer include.deinit(alloc);
    switch (include) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 2), ordinals[0]);
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    var filter = doc_set.ResolvedDocFilter{
        .include = include,
        .exclude = .none,
    };
    include = .all;
    defer filter.deinit(alloc);

    var result = try db.search(alloc, .{
        .query = .{ .match_all = {} },
        .resolved_doc_filter = &filter,
        .limit = 10,
        .include_stored = false,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

test "db search runtime identity treats reserved namespace bytes as user document ids" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const raw_id = "\x03raw\x00doc";
    try db.batch(.{
        .writes = &.{
            .{ .key = raw_id, .value = "{\"name\":\"binary\"}" },
        },
    });

    const raw = try db.get(alloc, raw_id);
    defer if (raw) |value| alloc.free(value);
    try std.testing.expect(raw != null);
    try std.testing.expect(std.mem.indexOf(u8, raw.?, "\"binary\"") != null);

    var resolved = try db.internalResolveDocSetForIdsAlloc(alloc, &.{raw_id});
    defer resolved.deinit(alloc);
    switch (resolved) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 1), ordinals[0]);
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    const resolved_doc_ids = (try db.internalDocIdsForResolvedDocSetAlloc(alloc, &resolved)).?;
    defer {
        for (resolved_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(resolved_doc_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), resolved_doc_ids.len);
    try std.testing.expectEqualSlices(u8, raw_id, resolved_doc_ids[0]);
}

test "db search runtime identity batch and scan round trip adversarial document ids" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const raw_ids = [_][]const u8{
        "",
        "\x00",
        "\x00\x00",
        "\x00a",
        ":",
        ":e:",
        ":i:",
        ":t",
        "abc\x00def",
        "abc:",
        "abc\xffdef",
        "\xff",
    };

    var writes: [raw_ids.len]types.BatchWrite = undefined;
    for (raw_ids, 0..) |raw_id, i| {
        writes[i] = .{
            .key = raw_id,
            .value = try std.fmt.allocPrint(alloc, "{{\"ordinal\":{d}}}", .{i}),
        };
    }
    defer for (writes) |write| alloc.free(@constCast(write.value));

    try db.batch(.{ .writes = writes[0..] });

    for (raw_ids, 0..) |raw_id, i| {
        const raw = (try db.get(alloc, raw_id)) orelse return error.TestExpectedEqual;
        defer alloc.free(raw);
        const expected = try std.fmt.allocPrint(alloc, "{{\"ordinal\":{d}}}", .{i});
        defer alloc.free(expected);
        try std.testing.expectEqualStrings(expected, raw);
    }

    var resolved = try db.internalResolveDocSetForIdsAlloc(alloc, raw_ids[0..]);
    defer resolved.deinit(alloc);
    switch (resolved) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(raw_ids.len, ordinals.len);
            for (ordinals, 0..) |ordinal, i| {
                try std.testing.expectEqual(@as(doc_set.DocOrdinal, @intCast(i + 1)), ordinal);
            }
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    const resolved_doc_ids = (try db.internalDocIdsForResolvedDocSetAlloc(alloc, &resolved)).?;
    defer {
        for (resolved_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(resolved_doc_ids);
    }
    try std.testing.expectEqual(raw_ids.len, resolved_doc_ids.len);
    for (raw_ids, 0..) |raw_id, i| {
        try std.testing.expectEqualSlices(u8, raw_id, resolved_doc_ids[i]);
    }

    var scanned = try db.scan(alloc, "", "", .{ .include_documents = true });
    defer scanned.deinit(alloc);
    try std.testing.expectEqual(raw_ids.len, scanned.hashes.len);
    try std.testing.expectEqual(raw_ids.len, scanned.documents.len);
    for (raw_ids, 0..) |raw_id, i| {
        try std.testing.expectEqualSlices(u8, raw_id, scanned.hashes[i].id);
        try std.testing.expectEqualSlices(u8, raw_id, scanned.documents[i].id);
    }
}

test "db search runtime identity resolved doc-set projection honors identity read generation" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    var create_identity = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (create_identity.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        create_identity.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        db.core.store,
        0,
        0,
        1,
        &create_identity,
        &.{"doc:a"},
        &.{},
    );
    try db.core.store.putBatchWithReplay(null, create_identity.items, &.{}, null);

    var delete_identity = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (delete_identity.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        delete_identity.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        db.core.store,
        0,
        0,
        2,
        &delete_identity,
        &.{},
        &.{"doc:a"},
    );
    try db.core.store.putBatchWithReplay(null, delete_identity.items, &.{}, null);

    var future_identity = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (future_identity.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        future_identity.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        db.core.store,
        0,
        0,
        3,
        &future_identity,
        &.{"doc:b"},
        &.{},
    );
    try db.core.store.putBatchWithReplay(null, future_identity.items, &.{}, null);

    var resolved = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 2 });
    defer resolved.deinit(alloc);

    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &resolved, 1));
    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &resolved, 2));
    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &resolved, 3));
    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &resolved, null));

    var created = try doc_set.fromOrdinalsAlloc(alloc, &.{1});
    defer created.deinit(alloc);
    const visible_doc_ids = (try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &created, 1)) orelse return error.TestUnexpectedResult;
    defer {
        for (visible_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(visible_doc_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), visible_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", visible_doc_ids[0]);
    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &created, 2));
    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &created, null));

    var future = try doc_set.fromOrdinalsAlloc(alloc, &.{2});
    defer future.deinit(alloc);
    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &future, 1));
    try std.testing.expectEqual(@as(?[]const []const u8, null), try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &future, 2));
    const future_doc_ids = (try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &future, 3)) orelse return error.TestUnexpectedResult;
    defer {
        for (future_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(future_doc_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), future_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", future_doc_ids[0]);

    const current_doc_ids = (try db.internalDocIdsForResolvedDocSetNoLockAtGenerationAlloc(alloc, &future, null)) orelse return error.TestUnexpectedResult;
    defer {
        for (current_doc_ids) |doc_id| alloc.free(@constCast(doc_id));
        alloc.free(current_doc_ids);
    }
    try std.testing.expectEqual(@as(usize, 1), current_doc_ids.len);
    try std.testing.expectEqualStrings("doc:b", current_doc_ids[0]);
}

test "db search runtime identity search requests default to current identity generation snapshot" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"name\":\"alpha\"}" }},
    });

    const current_generation = db.core.nextDerivedSequence();
    const current = try db.searchRequestAtCurrentIdentityGeneration(.{});
    try std.testing.expectEqual(@as(?u64, db.core.nextDerivedSequence()), current.identity_read_generation);

    const explicit = try db.searchRequestAtCurrentIdentityGeneration(.{ .identity_read_generation = current_generation });
    try std.testing.expectEqual(@as(?u64, current_generation), explicit.identity_read_generation);

    try std.testing.expectError(error.UnsupportedQueryRequest, db.searchRequestAtCurrentIdentityGeneration(.{
        .identity_read_generation = current_generation -| 1,
    }));
    try std.testing.expectError(error.UnsupportedQueryRequest, db.search(alloc, .{
        .identity_read_generation = current_generation + 1,
    }));

    var preflight = try db.preflightSearchRequest(alloc, .{ .identity_read_generation = current_generation }, 0);
    defer preflight.deinit(alloc);
    try std.testing.expectError(error.UnsupportedQueryRequest, db.preflightSearchRequest(alloc, .{
        .identity_read_generation = current_generation -| 1,
    }, 0));
    try std.testing.expectError(error.UnsupportedQueryRequest, db.collectPlanningStats(alloc, .{
        .identity_read_generation = current_generation + 1,
    }, 0));
    const text_stats = try db.collectSearchRequestTextStats(alloc, .{ .identity_read_generation = current_generation });
    defer alloc.free(text_stats);
    try std.testing.expectError(error.UnsupportedQueryRequest, db.collectSearchRequestTextStats(alloc, .{
        .identity_read_generation = current_generation + 1,
    }));

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 5), stats.doc_set_planning.stale_identity_generation_rejection_count);
}

test "db search runtime identity validates internal resolved doc filter wire namespace and generation" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const namespace = doc_identity.Namespace{ .table_id = 1, .shard_id = 2, .range_id = 3 };
    var db = try DB.open(alloc, std.mem.span(path), .{
        .identity_namespace = namespace,
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    const generation = try db.currentIdentityReadGenerationForRequest(null);
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}),
    };
    defer filter.deinit(alloc);

    var ok = try db.search(alloc, .{
        .limit = 10,
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = namespace,
            .identity_read_generation = generation,
        },
    });
    defer ok.deinit();
    try std.testing.expectEqual(@as(u32, 1), ok.total_hits);

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, db.search(alloc, .{
        .limit = 10,
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 99, .range_id = 3 },
            .identity_read_generation = generation,
        },
    }));

    try std.testing.expectError(error.UnsupportedQueryRequest, db.search(alloc, .{
        .limit = 10,
        .identity_read_generation = generation,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = namespace,
            .identity_read_generation = generation + 1,
        },
    }));
}

test "db search runtime identity explicit doc-id filter resolution honors identity generation" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    var create_identity = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (create_identity.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        create_identity.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        db.core.store,
        0,
        0,
        1,
        &create_identity,
        &.{"doc:a"},
        &.{},
    );
    try db.core.store.putBatchWithReplay(null, create_identity.items, &.{}, null);

    var delete_identity = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (delete_identity.items) |item| {
            alloc.free(@constCast(item.key));
            alloc.free(@constCast(item.value));
        }
        delete_identity.deinit(alloc);
    }
    try doc_identity.appendBatchIdentityMetadataAlloc(
        alloc,
        db.core.store,
        0,
        0,
        2,
        &delete_identity,
        &.{},
        &.{"doc:a"},
    );
    try db.core.store.putBatchWithReplay(null, delete_identity.items, &.{}, null);

    var visible_at_create = try db.internalResolvedDocFilterForIdsAlloc(true, &.{"doc:a"}, &.{}, 1);
    defer visible_at_create.deinit(alloc);
    switch (visible_at_create.include) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 1), ordinals[0]);
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    var hidden_at_delete = try db.internalResolvedDocFilterForIdsAlloc(true, &.{"doc:a"}, &.{}, 2);
    defer hidden_at_delete.deinit(alloc);
    try std.testing.expectEqual(@as(?usize, 0), hidden_at_delete.include.estimatedCardinality());
}

test "db search runtime identity sparse index uses identity ordinals as physical doc nums for primary docs" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"sparse\":{\"indices\":[1],\"values\":[0.5]}}" },
        },
        .sync_level = .full_index,
    });

    const sparse_entry = db.core.index_manager.sparseIndex("sp_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(?u32, 1), try sparse_entry.index.debugDocNumForDocId("doc:a"));
    try std.testing.expectEqual(@as(?u32, 2), try sparse_entry.index.debugDocNumForDocId("doc:b"));

    var include = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:b"});
    errdefer include.deinit(alloc);
    const ordinal = switch (include) {
        .ordinals => |ordinals| blk: {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 2), ordinals[0]);
            break :blk ordinals[0];
        },
        else => return error.ExpectedOrdinalDocSet,
    };

    const sparse_doc_nums = try db.core.index_manager.lookupSparseDocNumsForOrdinalsAlloc(alloc, db.core.store, "sp_v1", &.{ordinal});
    defer alloc.free(sparse_doc_nums);
    try std.testing.expectEqual(@as(usize, 1), sparse_doc_nums.len);
    try std.testing.expectEqual(@as(u32, ordinal), sparse_doc_nums[0]);

    var filter = doc_set.ResolvedDocFilter{
        .include = include,
        .exclude = .none,
    };
    include = .all;
    defer filter.deinit(alloc);

    var filtered = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 2,
        .resolved_doc_filter = &filter,
    });
    defer filtered.deinit();

    try std.testing.expectEqual(@as(u32, 1), filtered.total_hits);
    try std.testing.expectEqual(@as(usize, 1), filtered.hits.len);
    try std.testing.expectEqualStrings("doc:b", filtered.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, ordinal), filtered.hits[0].doc_ordinal);
}

test "db search runtime identity sparse hits resolve doc ordinals through identity not sparse doc nums" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" }},
        .sync_level = .full_index,
    });

    const sparse_entry = db.core.index_manager.sparseIndex("sp_v1") orelse return error.TestUnexpectedResult;
    try sparse_entry.index.batch(&.{.{ .doc_id = "doc:a", .doc_num = 42, .vec = .{ .indices = &.{1}, .values = &.{1.0} } }}, &.{});
    try std.testing.expectEqual(@as(?u32, 42), try sparse_entry.index.debugDocNumForDocId("doc:a"));

    var identity_txn = try db.core.store.beginProbeTxn();
    defer identity_txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &identity_txn, "doc:a")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(doc_identity.DocOrdinal, 1), ordinal);

    var result = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 1,
        } },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, ordinal), result.hits[0].doc_ordinal);
}

test "db search runtime identity dense index stores stable vector ids with ordinal filter mappings" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[0,0]}" },
            .{ .key = "doc:b", .value = "{\"embedding\":[10,0]}" },
        },
        .sync_level = .full_index,
    });

    var include = try db.internalResolveDocSetForIdsAlloc(alloc, &.{"doc:b"});
    errdefer include.deinit(alloc);
    const ordinal = switch (include) {
        .ordinals => |ordinals| blk: {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            break :blk ordinals[0];
        },
        else => return error.ExpectedOrdinalDocSet,
    };
    try std.testing.expectEqual(@as(doc_set.DocOrdinal, 2), ordinal);
    const expected_vector_id = index_manager_mod.deterministicDenseVectorId("doc:b");
    try std.testing.expect(expected_vector_id != @as(u64, ordinal));
    try std.testing.expectEqual(@as(?u64, expected_vector_id), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:b"));

    var filter = doc_set.ResolvedDocFilter{
        .include = include,
        .exclude = .none,
    };
    include = .all;
    defer filter.deinit(alloc);

    try std.testing.expect(!db.searchRuntimeCanUsePublishedDenseSearch(.{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .resolved_doc_filter = &filter,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.0, 0.0 }, .k = 1 } },
    }));

    var dense_filtered = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .resolved_doc_filter = &filter,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_filtered.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_filtered.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_filtered.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_filtered.result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, ordinal), dense_filtered.result.hits[0].doc_ordinal);
    try std.testing.expectEqual(@as(u32, 1), dense_filtered.profile.raw_hit_count);

    const reassigned_namespace = doc_identity.Namespace{ .table_id = 19, .shard_id = 1902, .range_id = 19002 };
    try db.reassignIdentityNamespaceForInternalTransition(reassigned_namespace);
    try std.testing.expectEqual(@as(?u64, expected_vector_id), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:b"));

    {
        var txn = try db.core.store.beginProbeTxn();
        defer txn.abort();
        const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(reassigned_namespace, "doc:b"), state.canonical_doc_id);
    }

    var dense_after_reassign = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
        .resolved_doc_filter = &filter,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_after_reassign.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_after_reassign.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_after_reassign.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_after_reassign.result.hits[0].id);
    try std.testing.expectEqual(@as(?doc_set.DocOrdinal, ordinal), dense_after_reassign.result.hits[0].doc_ordinal);
    try std.testing.expectEqual(@as(u32, 1), dense_after_reassign.profile.raw_hit_count);
}

test "db search runtime identity resolved doc filter normalizes doc keys before composing native ids" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"category\":\"keep\"}" },
            .{ .key = "doc:b", .value = "{\"category\":\"keep\"}" },
        },
        .sync_level = .write,
    });

    var doc_key_filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.cloneDocKeysAlloc(alloc, &.{ "doc:a", "doc:b" }),
    };
    defer doc_key_filter.deinit(alloc);

    var composed = (try db.searchRuntimeResolvedDocFilterForRequestNativeConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &doc_key_filter,
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{"doc:b"},
    })) orelse return error.TestUnexpectedResult;
    defer composed.deinit(alloc);

    switch (composed.include) {
        .ordinals => |ordinals| {
            try std.testing.expectEqual(@as(usize, 1), ordinals.len);
            try std.testing.expectEqual(@as(doc_set.DocOrdinal, 2), ordinals[0]);
        },
        else => return error.ExpectedOrdinalDocSet,
    }

    var partial_doc_key_filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.cloneDocKeysAlloc(alloc, &.{ "doc:a", "missing" }),
    };
    defer partial_doc_key_filter.deinit(alloc);
    try std.testing.expect((try db.searchRuntimeResolvedDocFilterForRequestNativeConstraintsAlloc(alloc, .{
        .resolved_doc_filter = &partial_doc_key_filter,
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{"doc:a"},
    })) == null);
}

test "db search runtime identity vector full text filters project through doc identity ordinals" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"reject winner\",\"status\":\"active\",\"tenant\":\"tenantb\",\"embedding\":[0,0],\"sparse\":{\"indices\":[1],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"keep winner\",\"status\":\"active\",\"tenant\":\"tenanta\",\"embedding\":[10,0],\"sparse\":{\"indices\":[1],\"values\":[0.1]}}" },
        },
        .sync_level = .full_index,
    });

    try std.testing.expectEqual(@as(?u64, index_manager_mod.deterministicDenseVectorId("doc:b")), try db.core.index_manager.lookupDenseVectorId(db.core.store, "dv_v1", "doc:b"));

    var dense_filtered = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .primary_text_index_name = "ft_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match\":{\"field\":\"body\",\"text\":\"keep\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_filtered.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_filtered.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_filtered.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_filtered.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_filtered.profile.raw_hit_count);

    var dense_term_conjunct_filter = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .primary_text_index_name = "ft_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"conjuncts\":[{\"term\":{\"status\":\"active\"}},{\"term\":{\"tenant\":\"tenanta\"}}]}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_term_conjunct_filter.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_term_conjunct_filter.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_term_conjunct_filter.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_term_conjunct_filter.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_term_conjunct_filter.profile.raw_hit_count);

    var sparse_filtered = try db.search(alloc, .{
        .index_name = "sp_v1",
        .primary_text_index_name = "ft_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{1},
            .values = &.{1.0},
            .k = 2,
        } },
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match\":{\"field\":\"body\",\"text\":\"keep\"}}",
    });
    defer sparse_filtered.deinit();
    try std.testing.expectEqual(@as(u32, 1), sparse_filtered.total_hits);
    try std.testing.expectEqual(@as(usize, 1), sparse_filtered.hits.len);
    try std.testing.expectEqualStrings("doc:b", sparse_filtered.hits[0].id);

    var dense_empty_text_filter = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .primary_text_index_name = "ft_v1",
        .limit = 1,
        .include_stored = false,
        .filter_query_json = "{\"match\":{\"field\":\"body\",\"text\":\"absent\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_empty_text_filter.result.deinit();
    try std.testing.expectEqual(@as(u32, 0), dense_empty_text_filter.result.total_hits);
    try std.testing.expectEqual(@as(usize, 0), dense_empty_text_filter.result.hits.len);
    try std.testing.expectEqual(@as(u32, 0), dense_empty_text_filter.profile.raw_hit_count);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_set_planning.missing_ordinal_coverage_count);
}

test "db search runtime identity default dynamic schema vector term filters project through doc identity ordinals" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const default_schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true,"x-antfly-dynamic-indexing":{"mode":"infer_types"}}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, default_schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha winner\",\"status\":\"active\",\"tenant\":\"tenantb\",\"embedding\":[0,0]}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta winner\",\"status\":\"active\",\"tenant\":\"tenanta\",\"embedding\":[10,0]}" },
            .{ .key = "doc:c", .value = "{\"body\":\"alpha winner\",\"status\":\"active\",\"tenant\":\"tenanta\",\"embedding\":[20,0]}" },
            .{ .key = "doc:d", .value = "{\"body\":\"alpha winner\",\"status\":\"draft\",\"tenant\":\"tenanta\",\"embedding\":[30,0]}" },
        },
        .sync_level = .full_index,
    });

    var dense_term_conjunct_filter = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .primary_text_index_name = "ft_v1",
        .limit = 3,
        .include_stored = false,
        .filter_query_json = "{\"conjuncts\":[{\"term\":{\"status\":\"active\"}},{\"term\":{\"tenant\":\"tenanta\"}}]}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 3 });
    defer dense_term_conjunct_filter.result.deinit();
    try std.testing.expectEqual(@as(u32, 2), dense_term_conjunct_filter.result.total_hits);
    try std.testing.expectEqual(@as(usize, 2), dense_term_conjunct_filter.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_term_conjunct_filter.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 2), dense_term_conjunct_filter.profile.raw_hit_count);

    var dense_text_and_term_filter = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .primary_text_index_name = "ft_v1",
        .limit = 3,
        .include_stored = false,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .filter_query_json = "{\"conjuncts\":[{\"term\":{\"status\":\"active\"}},{\"term\":{\"tenant\":\"tenanta\"}}]}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 3 });
    defer dense_text_and_term_filter.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_text_and_term_filter.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_text_and_term_filter.result.hits.len);
    try std.testing.expectEqualStrings("doc:c", dense_text_and_term_filter.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_text_and_term_filter.profile.raw_hit_count);

    var dense_text_exclusion_filter = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .primary_text_index_name = "ft_v1",
        .limit = 3,
        .include_stored = false,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        .filter_query_json = "{\"term\":{\"tenant\":\"tenanta\"}}",
        .exclusion_query_json = "{\"term\":{\"status\":\"draft\"}}",
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 3 });
    defer dense_text_exclusion_filter.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_text_exclusion_filter.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_text_exclusion_filter.result.hits.len);
    try std.testing.expectEqualStrings("doc:c", dense_text_exclusion_filter.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_text_exclusion_filter.profile.raw_hit_count);

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expectEqual(@as(u32, 3), try text_index.snapshot().termDocFreq(alloc, "status.keyword", "active"));
    try std.testing.expectEqual(@as(u32, 3), try text_index.snapshot().termDocFreq(alloc, "tenant.keyword", "tenanta"));
}

test "db search runtime identity non chunked search paths apply broad live doc filter" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha only\",\"embedding\":[0,0],\"sparse\":{\"indices\":[7],\"values\":[1.0]}}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta only\",\"embedding\":[10,0],\"sparse\":{\"indices\":[8],\"values\":[1.0]}}" },
        },
        .sync_level = .full_index,
    });

    const doc_a_store_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
    defer alloc.free(doc_a_store_key);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(doc_a_store_key);
        try doc_identity.markDeletedTxn(alloc, &txn, 2, "doc:a");
        try txn.commit();
    }
    db.identity_visibility_summary_cache = null;

    var dense_live = try db.searchDenseProfiled(alloc, .{
        .index_name = "dv_v1",
        .limit = 1,
        .include_stored = false,
    }, .{ .vector = &.{ 0.0, 0.0 }, .k = 1 });
    defer dense_live.result.deinit();
    try std.testing.expectEqual(@as(u32, 1), dense_live.result.total_hits);
    try std.testing.expectEqual(@as(usize, 1), dense_live.result.hits.len);
    try std.testing.expectEqualStrings("doc:b", dense_live.result.hits[0].id);
    try std.testing.expectEqual(@as(u32, 1), dense_live.profile.raw_hit_count);

    var sparse_live = try db.search(alloc, .{
        .index_name = "sp_v1",
        .query = .{ .sparse_knn = .{
            .indices = &.{7},
            .values = &.{1.0},
            .k = 2,
        } },
        .include_stored = false,
    });
    defer sparse_live.deinit();
    try std.testing.expectEqual(@as(u32, 0), sparse_live.total_hits);
    try std.testing.expectEqual(@as(usize, 0), sparse_live.hits.len);

    var text_live = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "alpha" } },
        .include_stored = false,
    });
    defer text_live.deinit();
    try std.testing.expectEqual(@as(u32, 0), text_live.total_hits);
    try std.testing.expectEqual(@as(usize, 0), text_live.hits.len);
}

test "db search runtime preflight validates live lane bindings" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expectError(error.IndexNotFound, db.preflightSearchRequest(alloc, .{
        .full_text = .{ .match_all = {} },
        .index_name = "missing_ft",
    }, 0));

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    var text_summary = try db.preflightSearchRequest(alloc, .{
        .full_text = .{ .match_all = {} },
    }, 0);
    defer text_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), text_summary.result_refs.len);
    try std.testing.expectEqualStrings("$full_text_results", text_summary.result_refs[0]);
    try std.testing.expectEqual(@as(u32, 1), text_summary.shard_count);
    try std.testing.expectEqual(@as(u32, 0), text_summary.remote_shard_count);
    try std.testing.expectEqual(@as(usize, 1), text_summary.text_indexes.len);
    try std.testing.expectEqualStrings("ft_v1", text_summary.text_indexes[0].name);
    try std.testing.expectEqual(@as(u64, 0), text_summary.text_indexes[0].doc_count);
    try std.testing.expect(!text_summary.text_indexes[0].chunk_backed);
    try std.testing.expect(!text_summary.text_indexes[0].group_chunk_parents);
    var provider_summary = try db.planningStatsProvider().collectSearchRequestStats(alloc, .{
        .full_text = .{ .match_all = {} },
    }, 0);
    defer provider_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), provider_summary.result_refs.len);
    try std.testing.expectEqualStrings("$full_text_results", provider_summary.result_refs[0]);
    try std.testing.expectEqual(@as(usize, 1), provider_summary.text_indexes.len);
    try std.testing.expectEqualStrings("ft_v1", provider_summary.text_indexes[0].name);

    try db.addIndex(.{
        .name = "ft_title_v1",
        .kind = .full_text,
        .config_json = "{\"analysis_config\":{\"field_analyzers\":{\"title\":\"standard\"}}}",
    });
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .full_text = .{ .bool_query = .{
            .must = &.{.{ .match = .{ .field = "title", .text = "alpha" } }},
            .should = &.{.{ .match = .{ .field = "body", .text = "beta" } }},
        } },
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .query = .{ .match = .{ .field = "body", .text = "alpha" } },
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .full_text = .{ .match_all = {} },
        .filter_query_json = "{\"term\":{\"body\":\"published\"}}",
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .full_text = .{ .match_all = {} },
        .exclusion_query_json = "{\"bool\":{\"must_not\":[{\"term\":{\"body\":\"draft\"}}]}}",
    }, 0));
    var titled_text_summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    }, 0);
    defer titled_text_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), titled_text_summary.result_refs.len);
    try std.testing.expectEqualStrings("$full_text_results", titled_text_summary.result_refs[0]);
    try std.testing.expectEqual(@as(usize, 1), titled_text_summary.text_query_stats.len);
    try std.testing.expectEqualStrings("title", titled_text_summary.text_query_stats[0].field);
    try std.testing.expectEqual(@as(u32, 0), titled_text_summary.text_query_stats[0].global_doc_count);
    try std.testing.expectEqual(@as(usize, 1), titled_text_summary.text_query_stats[0].term_doc_freqs.len);
    try std.testing.expectEqualStrings("alpha", titled_text_summary.text_query_stats[0].term_doc_freqs[0].term);
    try std.testing.expectEqual(@as(u32, 0), titled_text_summary.text_query_stats[0].term_doc_freqs[0].doc_freq);
    var filtered_text_summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .full_text = .{ .match_all = {} },
        .filter_query_json = "{\"term\":{\"title\":\"published\"}}",
        .exclusion_query_json = "{\"bool\":{\"must_not\":[{\"match\":{\"title\":\"draft\"}}]}}",
    }, 0);
    defer filtered_text_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), filtered_text_summary.text_query_stats.len);
    try std.testing.expectEqualStrings("title", filtered_text_summary.text_query_stats[0].field);
    try std.testing.expectEqual(@as(usize, 2), filtered_text_summary.text_query_stats[0].term_doc_freqs.len);
    var saw_published = false;
    var saw_draft = false;
    for (filtered_text_summary.text_query_stats[0].term_doc_freqs) |term| {
        if (std.mem.eql(u8, term.term, "published")) saw_published = true;
        if (std.mem.eql(u8, term.term, "draft")) saw_draft = true;
    }
    try std.testing.expect(saw_published);
    try std.testing.expect(saw_draft);
    var all_text_summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "ft_title_v1",
        .query = .{ .match = .{ .field = "_all", .text = "alpha" } },
    }, 0);
    defer all_text_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), all_text_summary.result_refs.len);

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0 }, .k = 10 },
    }, 0));
    var dense_summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "dv_v1",
        .dense = .{ .vector = &.{ 1.0, 2.0, 3.0 }, .k = 10 },
    }, 0);
    defer dense_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), dense_summary.result_refs.len);
    try std.testing.expectEqualStrings("$embeddings_results", dense_summary.result_refs[0]);
    try std.testing.expectEqual(@as(u32, 1), dense_summary.shard_count);
    try std.testing.expectEqual(@as(u32, 1), dense_summary.dense_query_count);
    try std.testing.expectEqual(@as(u64, 10), dense_summary.dense_effective_k_total);
    try std.testing.expect(dense_summary.dense_search_width_total >= dense_summary.dense_effective_k_total);
    try std.testing.expect(dense_summary.dense_search_width_max >= 64);
    try std.testing.expect(dense_summary.dense_epsilon_max >= 1.0);
    try std.testing.expectEqual(@as(usize, 1), dense_summary.embedding_indexes.len);
    try std.testing.expectEqualStrings("dv_v1", dense_summary.embedding_indexes[0].name);
    try std.testing.expectEqual(@as(u32, 3), dense_summary.embedding_indexes[0].dims);
    try std.testing.expect(!dense_summary.embedding_indexes[0].sparse);

    var structured_summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .doc_id = .{ .ids = &.{ "doc:a", "doc:b" } } },
        .filter_ids = &.{ 1, 2, 3 },
        .exclude_ids = &.{4},
        .filter_query_json = "{\"bool\":{\"must\":[{\"numeric_range\":{\"field\":\"score\",\"min\":1}},{\"doc_id\":[\"doc:c\"]}]}}",
        .exclusion_query_json = "{\"bool\":{\"must_not\":[{\"term_range\":{\"field\":\"tag\",\"min\":\"a\",\"max\":\"m\"}},{\"ip_range\":{\"field\":\"ip\",\"cidr\":\"10.0.0.0/8\"}}]}}",
    }, 0);
    defer structured_summary.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), structured_summary.doc_id_value_count);
    try std.testing.expectEqual(@as(u32, 3), structured_summary.filter_id_count);
    try std.testing.expectEqual(@as(u32, 1), structured_summary.exclude_id_count);
    try std.testing.expectEqual(@as(u32, 1), structured_summary.numeric_range_clause_count);
    try std.testing.expectEqual(@as(u32, 1), structured_summary.term_range_clause_count);
    try std.testing.expectEqual(@as(u32, 1), structured_summary.ip_range_clause_count);
    try std.testing.expectEqual(@as(?u32, 1), structured_summary.positive_id_result_upper_bound);
    try std.testing.expectEqual(@as(?u64, 0), structured_summary.structured_filter_doc_count_estimate);
    try std.testing.expect(structured_summary.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 0), structured_summary.result_doc_upper_bound);
    try std.testing.expectEqual(@as(?u32, 0), structured_summary.result_doc_estimate);
    if (structured_summary.selectivity_upper_bound_ratio) |ratio| {
        try std.testing.expectApproxEqAbs(@as(f32, 0.0), ratio, 0.0001);
    }

    var cost_summary = try db.preflightSearchRequest(alloc, .{
        .limit = 6,
        .offset = 2,
        .include_stored = true,
        .aggregations_json = "{}",
        .reranker = .{
            .provider = .antfly,
            .field = "body",
            .top_n = 4,
        },
    }, 0);
    defer cost_summary.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 8), cost_summary.shard_result_window);
    try std.testing.expectEqual(@as(u64, 8), cost_summary.shard_result_window_total);
    try std.testing.expectEqual(@as(u64, 8), cost_summary.stored_projection_doc_upper_bound_total);
    try std.testing.expectEqual(@as(u32, 4), cost_summary.rerank_doc_upper_bound);
    try std.testing.expect(cost_summary.aggregation_may_scan_full_results);
    try std.testing.expectEqual(@as(?u32, null), cost_summary.positive_id_result_upper_bound);
    try std.testing.expectEqual(@as(?u32, null), cost_summary.result_doc_estimate);
    try std.testing.expectEqual(@as(?u32, null), cost_summary.result_doc_upper_bound);
    try std.testing.expectEqual(@as(?u64, null), cost_summary.effective_stored_projection_doc_estimate_total);
    try std.testing.expectEqual(@as(u64, 8), cost_summary.effective_stored_projection_doc_upper_bound_total);
    try std.testing.expectEqual(@as(?u32, null), cost_summary.effective_rerank_doc_estimate);
    try std.testing.expectEqual(@as(u32, 4), cost_summary.effective_rerank_doc_upper_bound);
    try std.testing.expectEqual(@as(?u32, null), cost_summary.aggregation_second_pass_doc_estimate);
    try std.testing.expectEqual(@as(?u32, null), cost_summary.aggregation_second_pass_doc_upper_bound);

    try std.testing.expectError(error.IndexNotFound, db.preflightSearchRequest(alloc, .{
        .index_name = "missing_sparse",
        .sparse = .{ .indices = &.{1}, .values = &.{1.0}, .k = 10 },
    }, 0));

    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{\"edge_types\":[{\"name\":\"parent\",\"topology\":\"tree\"}]}",
    });
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .graph_queries = &.{
            .{
                .name = "related",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .edge_types = &.{"missing"}, .max_depth = 1 },
                },
            },
        },
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .graph_queries = &.{
            .{
                .name = "path_missing_target",
                .query = .{
                    .query_type = .shortest_path,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                },
            },
        },
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .graph_queries = &.{
            .{
                .name = "k_zero",
                .query = .{
                    .query_type = .k_shortest_paths,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .target_nodes = .{ .keys = &.{"doc:b"} },
                    .k = 0,
                },
            },
        },
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .graph_queries = &.{
            .{
                .name = "pattern_with_target",
                .query = .{
                    .query_type = .pattern,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .target_nodes = .{ .keys = &.{"doc:b"} },
                    .pattern = &.{
                        .{
                            .alias = "src",
                            .edge = .{},
                        },
                    },
                },
            },
        },
    }, 0));
    try std.testing.expectError(error.InvalidArgument, db.preflightSearchRequest(alloc, .{
        .graph_queries = &.{
            .{
                .name = "pattern_missing_type",
                .query = .{
                    .query_type = .pattern,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .pattern = &.{
                        .{
                            .alias = "src",
                            .edge = .{},
                        },
                        .{
                            .alias = "dst",
                            .edge = .{ .types = &.{"missing"} },
                        },
                    },
                },
            },
        },
    }, 0));
    var graph_summary = try db.preflightSearchRequest(alloc, .{
        .graph_queries = &.{
            .{
                .name = "related",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .edge_types = &.{"parent"}, .max_depth = 1 },
                },
            },
            .{
                .name = "pattern_related",
                .query = .{
                    .query_type = .pattern,
                    .index_name = "graph_v1",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .pattern = &.{
                        .{
                            .alias = "src",
                            .edge = .{},
                        },
                        .{
                            .alias = "dst",
                            .edge = .{ .types = &.{"parent"} },
                        },
                    },
                    .return_aliases = &.{ "src", "dst" },
                },
            },
        },
    }, 0);
    defer graph_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), graph_summary.graph_query_order.len);
    try std.testing.expectEqualStrings("related", graph_summary.graph_query_order[0]);
    try std.testing.expectEqualStrings("pattern_related", graph_summary.graph_query_order[1]);
    try std.testing.expectEqual(@as(usize, 1), graph_summary.graph_indexes.len);
    try std.testing.expectEqualStrings("graph_v1", graph_summary.graph_indexes[0].name);
    try std.testing.expectEqual(@as(u64, 0), graph_summary.graph_indexes[0].edge_count);
    try std.testing.expectEqual(@as(u64, 0), graph_summary.graph_indexes[0].node_count);
}

test "db search runtime preflight surfaces structured filter probe counts when count is budget limited" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    for (0..5) |i| {
        const doc_id = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(doc_id);
        const doc_key = try internal_keys.documentKeyAlloc(alloc, doc_id);
        defer alloc.free(doc_key);
        const lat = 37.70 + (@as(f64, @floatFromInt(i)) * 0.02);
        const lon = -122.50 + (@as(f64, @floatFromInt(i)) * 0.02);
        const json = try std.fmt.allocPrint(alloc, "{{\"published\":true,\"score\":{d},\"location\":{{\"lat\":{d},\"lon\":{d}}}}}", .{ i, lat, lon });
        defer alloc.free(json);
        try db.core.store.put(doc_key, json);
    }

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    var summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "ft_v1",
        .filter_query_json = "{\"bool_field\":{\"field\":\"published\",\"value\":true}}",
    }, 3);
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(?u64, 3), summary.structured_filter_count_budget_limit);
    try std.testing.expectEqual(@as(?u64, 5), summary.structured_filter_doc_count_sample_estimate);
    try std.testing.expectEqual(@as(u32, 3), summary.structured_filter_count_sample_size);
    try std.testing.expectEqual(@as(?u32, 5), summary.result_doc_estimate);
    if (summary.selectivity_sample_ratio) |ratio| {
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), ratio, 0.0001);
    }
    try std.testing.expect(summary.structured_filter_doc_count_lower_bound != null or summary.structured_filter_doc_count_estimate != null);

    var geo_summary = try db.preflightSearchRequest(alloc, .{
        .index_name = "ft_v1",
        .filter_query_json = "{\"geo_bbox\":{\"field\":\"location\",\"min_lat\":37.69,\"min_lon\":-122.51,\"max_lat\":37.75,\"max_lon\":-122.45}}",
    }, 2);
    defer geo_summary.deinit(alloc);

    try std.testing.expectEqual(@as(?u64, 2), geo_summary.structured_filter_count_budget_limit);
    try std.testing.expect(geo_summary.structured_filter_doc_count_sample_estimate != null);
    try std.testing.expectEqual(@as(u32, 2), geo_summary.structured_filter_count_sample_size);
    try std.testing.expect(geo_summary.structured_filter_doc_count_lower_bound != null or geo_summary.structured_filter_doc_count_estimate != null);
}

test "db search runtime writes and reads timestamp metadata" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1_700_000_000_000_000_000,
    });

    const ts = try db.getTimestamp(alloc, "doc:a");
    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_000), ts);

    const first_doc = "doc\x00ttl";
    const second_doc = "doc\x00ttl:child";
    try db.batch(.{
        .writes = &.{
            .{ .key = first_doc, .value = "{\"title\":\"first\"}" },
            .{ .key = second_doc, .value = "{\"title\":\"second\"}" },
        },
        .timestamp_ns = 1_700_000_000_000_000_101,
    });

    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_101), try db.getTimestamp(alloc, first_doc));
    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_101), try db.getTimestamp(alloc, second_doc));

    try db.batch(.{ .deletes = &.{first_doc} });

    try std.testing.expectEqual(@as(u64, 0), try db.getTimestamp(alloc, first_doc));
    try std.testing.expectEqual(@as(u64, 1_700_000_000_000_000_101), try db.getTimestamp(alloc, second_doc));
}

test "db search runtime full_text sync level does not wait for dense hbc visibility" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha routing\",\"embedding\":[1,0,0]}" },
        },
        .sync_level = .full_text,
    });

    const full_text_applied = try db.core.loadAppliedSequence(alloc, "ft_v1");
    const dense_applied = try db.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expectEqual(@as(u64, 1), full_text_applied);
    try std.testing.expectEqual(@as(u64, 0), dense_applied);
    try std.testing.expectEqual(@as(u64, 0), db.core.index_manager.denseIndex("dv_v1").?.index.metadata.active_count);

    var text_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "alpha" } },
    });
    defer text_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), text_result.total_hits);
}

test "db search runtime relational dense HBC loader reads committed base rows" {
    const DB = @import("mod.zig").DB;
    const schema_api_mod = @import("../../schema/mod.zig");
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"embedding":{"type":"array"}},"required":["title","embedding"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "row:a",
            .value = "{\"title\":\"alpha\",\"embedding\":[1,0,0]}",
        }},
        .sync_level = .full_index,
    });

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "row:a", "dv_v1");
    defer alloc.free(artifact_key);
    try db.core.store.delete(artifact_key);
    db.clearDenseHbcCaches();

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:a");
    defer alloc.free(primary_key);
    const maybe_primary = db.core.store.get(alloc, primary_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (maybe_primary) |primary_value| {
        defer alloc.free(primary_value);
        return error.TestExpectedEqual;
    }

    var result = try db.search(alloc, .{
        .index_name = "dv_v1",
        .dense = .{
            .vector = &[_]f32{ 1, 0, 0 },
            .k = 1,
        },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("row:a", result.hits[0].id);
}

test "db search runtime dense lsm cache profile benchmark" {
    const DB = @import("mod.zig").DB;
    const profileBenchTestsEnabled = TestHelpers.profileBenchTestsEnabled;
    const cacheBlockHitsForBench = TestHelpers.cacheBlockHitsForBench;
    const monotonicTimeNs = platform_time.monotonicNs;
    if (!profileBenchTestsEnabled()) return error.SkipZigTest;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var lsm_cache = lsm_backend_mod.Cache.init(alloc, 64 * 1024 * 1024);
    defer lsm_cache.deinit();

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .lsm_cache = &lsm_cache,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":32,\"metric\":\"cosine\"}",
    });

    const doc_count: usize = 8192;
    const dims: usize = 32;
    const reps: usize = 20;

    const writes = try alloc.alloc(types.BatchWrite, doc_count);
    defer {
        for (writes) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        alloc.free(writes);
    }

    const vector_buf = try alloc.alloc(f32, dims);
    defer alloc.free(vector_buf);

    for (writes, 0..) |*write, doc_id| {
        var norm_sq: f32 = 0;
        for (vector_buf, 0..) |*slot, dim| {
            const raw: u32 = @intCast((doc_id * 1315423911 + dim * 2654435761 + 17) % 1000);
            const centered = (@as(f32, @floatFromInt(raw)) / 500.0) - 1.0;
            slot.* = centered;
            norm_sq += centered * centered;
        }
        const inv_norm: f32 = 1.0 / @sqrt(norm_sq);
        for (vector_buf) |*slot| slot.* *= inv_norm;

        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d}", .{doc_id}),
            .value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"doc-{d}\",\"embedding\":{f}}}",
                .{ doc_id, std.json.fmt(vector_buf, .{}) },
            ),
        };
    }

    try db.batch(.{
        .writes = writes,
        .sync_level = .full_index,
    });

    const dense_entry = db.core.denseIndex("dv_v1").?;
    const query = vector_buf[0..dims];
    @memcpy(query, &[_]f32{
        0.31,  -0.09, 0.27,  -0.41, 0.12,  0.05,  -0.33, 0.44,
        -0.11, 0.22,  -0.18, 0.39,  -0.28, 0.07,  0.14,  -0.36,
        0.25,  -0.19, 0.08,  0.17,  -0.45, 0.29,  -0.04, 0.35,
        -0.23, 0.16,  0.03,  -0.27, 0.41,  -0.15, 0.21,  -0.32,
    });
    var query_norm_sq: f32 = 0;
    for (query) |value| query_norm_sq += value * value;
    const query_inv_norm: f32 = 1.0 / @sqrt(query_norm_sq);
    for (query) |*value| value.* *= query_inv_norm;

    const dense_query: types.DenseKnnQuery = .{
        .vector = query,
        .k = 100,
    };

    const req: types.SearchRequest = .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = dense_query },
        .dense = dense_query,
        .limit = 100,
        .include_stored = false,
    };

    const cache_before = lsm_cache.snapshotStats();
    const block_hits_before = cacheBlockHitsForBench(cache_before);
    const index_hits_before = cache_before.run_table_index.hits;

    const cold_hbc_start = monotonicTimeNs();
    var cold_profiled = try dense_entry.index.searchProfiledRequest(.{
        .query = query,
        .k = 100,
    });
    const cold_hbc_ns: u64 = monotonicTimeNs() - cold_hbc_start;
    defer cold_profiled.results.deinit();

    const cold_db_start = monotonicTimeNs();
    var cold_result = try db.search(alloc, req);
    const cold_db_ns: u64 = monotonicTimeNs() - cold_db_start;
    defer cold_result.deinit();

    var warm_hbc_total_ns: u64 = 0;
    var warm_hbc_rerank_load_ns: u64 = 0;
    var warm_hbc_rerank_ns: u64 = 0;
    var warm_hbc_total_reranked: u64 = 0;
    var warm_dense_total_ns: u64 = 0;
    const warm_dense_index_lookup_ns: u64 = 0;
    var warm_dense_hbc_search_ns: u64 = 0;
    var warm_dense_doc_key_resolve_ns: u64 = 0;
    var warm_dense_load_projected_ns: u64 = 0;
    var warm_dense_postprocess_ns: u64 = 0;
    var warm_dense_inline_metadata_hits: u64 = 0;
    var warm_dense_fetched_metadata_hits: u64 = 0;
    var warm_dense_lookup_doc_key_hits: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicTimeNs();
        var profiled = try dense_entry.index.searchProfiledRequest(.{
            .query = query,
            .k = 100,
        });
        warm_hbc_total_ns += monotonicTimeNs() - start;
        warm_hbc_rerank_load_ns += profiled.profile.rerank_vector_load_ns;
        warm_hbc_rerank_ns += profiled.profile.rerank_ns;
        warm_hbc_total_reranked += profiled.profile.reranked_vectors;
        profiled.results.deinit();
    }

    for (0..reps) |_| {
        const total_start = monotonicTimeNs();
        const profiled_entry = dense_entry;
        const chunk_backed = profiled_entry.chunk_name != null;
        const group_chunk_parents = db_query_search.shouldGroupChunkParents(req, chunk_backed);
        const effective_k: u32 = if (group_chunk_parents)
            @intCast(profiled_entry.index.metadata.active_count)
        else
            req.dense.?.k;
        const effort = db_query_search.resolvedSearchEffort(req.search_effort);

        const hbc_search_start = monotonicTimeNs();
        var dense_results = try profiled_entry.index.searchWithRequest(.{
            .query = req.dense.?.vector,
            .k = effective_k,
            .search_width = db_query_search.resolveSearchWidth(req.dense.?.k, effort, profiled_entry.index.stats()),
            .epsilon = db_query_search.resolveSearchEpsilon(effort),
            .filter_prefix = req.filter_prefix,
            .distance_over = req.distance_over,
            .distance_under = req.distance_under,
            .filter_ids = req.filter_ids,
            .exclude_ids = req.exclude_ids,
        });
        warm_dense_hbc_search_ns += monotonicTimeNs() - hbc_search_start;
        defer dense_results.deinit();

        const raw_hits = dense_results.getHits();
        const start: u32 = if (group_chunk_parents) 0 else @min(req.offset, @as(u32, @intCast(raw_hits.len)));
        const end: u32 = if (group_chunk_parents) @intCast(raw_hits.len) else @min(start + req.limit, @as(u32, @intCast(raw_hits.len)));

        var hits = std.ArrayListUnmanaged(types.SearchHit).empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(alloc);
            hits.deinit(alloc);
        }

        for (raw_hits[@intCast(start)..@intCast(end)], 0..) |hit, i| {
            const result_index: usize = @as(usize, @intCast(start)) + i;
            const resolve_start = monotonicTimeNs();
            const doc_key = if (dense_results.takeMetadata(result_index)) |metadata| blk: {
                warm_dense_inline_metadata_hits += 1;
                break :blk metadata;
            } else blk: {
                if (try profiled_entry.index.getMetadata(hit.vector_id)) |metadata| {
                    warm_dense_fetched_metadata_hits += 1;
                    break :blk metadata;
                }
                const looked_up = (try db.core.index_manager.lookupDenseDocKey(
                    db.core.store,
                    profiled_entry.config.name,
                    hit.vector_id,
                )) orelse {
                    warm_dense_doc_key_resolve_ns += monotonicTimeNs() - resolve_start;
                    continue;
                };
                warm_dense_lookup_doc_key_hits += 1;
                break :blk looked_up;
            };
            warm_dense_doc_key_resolve_ns += monotonicTimeNs() - resolve_start;

            const stored_data = if (req.include_stored and !(chunk_backed and group_chunk_parents)) blk: {
                const load_start = monotonicTimeNs();
                const loaded = try db.searchRuntimeProjectOwnedStoredBytesForSearch(
                    alloc,
                    req,
                    doc_key,
                    (try db.get(alloc, doc_key)) orelse return error.StoredDocMissing,
                );
                warm_dense_load_projected_ns += monotonicTimeNs() - load_start;
                break :blk loaded;
            } else null;

            try hits.append(alloc, .{
                .id = doc_key,
                .score = hit.distance,
                .stored_data = stored_data,
            });
        }

        const postprocess_start = monotonicTimeNs();
        var profiled_result = try db.searchRuntimePostprocessVectorSearchResult(
            alloc,
            req,
            .{
                .alloc = alloc,
                .hits = try hits.toOwnedSlice(alloc),
                .total_hits = @intCast(hits.items.len),
                .graph_results = &.{},
            },
            chunk_backed,
        );
        warm_dense_postprocess_ns += monotonicTimeNs() - postprocess_start;
        warm_dense_total_ns += monotonicTimeNs() - total_start;
        profiled_result.deinit();
    }

    var warm_db_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicTimeNs();
        var result = try db.search(alloc, req);
        warm_db_total_ns += monotonicTimeNs() - start;
        result.deinit();
    }

    const cache_after = lsm_cache.snapshotStats();
    const block_hits_after = cacheBlockHitsForBench(cache_after);
    const index_hits_after = cache_after.run_table_index.hits;
    const warm_db_overhead_total_ns = if (warm_db_total_ns > warm_dense_total_ns)
        warm_db_total_ns - warm_dense_total_ns
    else
        0;

    std.debug.print(
        "dense_lsm_cache_profile docs={d} reps={d} cold_hbc_ms={d} cold_db_ms={d} warm_hbc_avg_ms={d} warm_db_avg_ms={d} warm_dense_total_avg_ms={d} warm_dense_index_lookup_avg_ms={d} warm_dense_hbc_search_avg_ms={d} warm_dense_doc_key_avg_ms={d} warm_dense_load_projected_avg_ms={d} warm_dense_postprocess_avg_ms={d} warm_db_overhead_avg_ms={d} warm_rerank_load_avg_ms={d} warm_rerank_avg_ms={d} avg_reranked={d} avg_inline_metadata_hits={d} avg_fetched_metadata_hits={d} avg_lookup_doc_key_hits={d} cache_index_hits_delta={d} cache_block_hits_delta={d}\n",
        .{
            doc_count,
            reps,
            @divTrunc(cold_hbc_ns, std.time.ns_per_ms),
            @divTrunc(cold_db_ns, std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_hbc_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_db_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_index_lookup_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_hbc_search_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_doc_key_resolve_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_load_projected_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_dense_postprocess_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_db_overhead_total_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_hbc_rerank_load_ns, reps), std.time.ns_per_ms),
            @divTrunc(@divTrunc(warm_hbc_rerank_ns, reps), std.time.ns_per_ms),
            @divTrunc(warm_hbc_total_reranked, reps),
            @divTrunc(warm_dense_inline_metadata_hits, reps),
            @divTrunc(warm_dense_fetched_metadata_hits, reps),
            @divTrunc(warm_dense_lookup_doc_key_hits, reps),
            index_hits_after - index_hits_before,
            block_hits_after - block_hits_before,
        },
    );

    try std.testing.expectEqual(@as(u32, 100), cold_result.total_hits);
}
