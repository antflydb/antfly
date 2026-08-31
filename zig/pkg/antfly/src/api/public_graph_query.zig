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
const ant_json = @import("antfly-json");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const db_mod = @import("../storage/db/selected_root.zig").db;
const graph_mod = @import("../graph/graph.zig");
const graph_pattern_mod = @import("../graph/pattern.zig");
const graph_query_mod = @import("../graph/query.zig");
const query_contract = @import("query_contract.zig");

pub fn rejectInternalDocIdentityFields(alloc: std.mem.Allocator, body: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    try query_contract.validatePublicQueryEnvelopeValueAlloc(alloc, parsed.value);
}

/// Parse the canonical graph-query surface used by serverless runtimes. Legacy
/// compatibility belongs exclusively to the stateful Antfly ingress adapter.
pub fn parseCanonicalGraphQueriesAlloc(
    alloc: std.mem.Allocator,
    request: anytype,
) ![]const db_mod.types.NamedGraphQuery {
    const Request = @TypeOf(request);
    if (comptime @hasField(Request, "graph_searches")) {
        if (request.graph_searches != null) return error.UnsupportedQueryRequest;
    }
    if (comptime @hasField(Request, "expand_strategy")) {
        // Canonical graph results may contain table-qualified identities, while
        // the retrieval hit envelope is scoped to the queried table. Reject the
        // legacy retrieval merge control instead of erasing that provenance.
        if (request.graph_queries != null and request.expand_strategy != null)
            return error.InvalidQueryRequest;
    }
    const query_count = if (request.graph_queries) |queries| queries.map.count() else 0;
    if (request.graph_queries != null and query_count == 0) return error.InvalidQueryRequest;
    if (query_count > graph_query_mod.max_named_queries) return error.InvalidQueryRequest;

    var items = std.ArrayListUnmanaged(db_mod.types.NamedGraphQuery).empty;
    errdefer freeNamedGraphQueries(alloc, items.items);

    if (request.graph_queries) |graph_queries| {
        var it = graph_queries.map.iterator();
        while (it.next()) |entry| {
            if (!graph_query_mod.isValidQueryName(entry.key_ptr.*)) return error.InvalidQueryRequest;
            const name = try alloc.dupe(u8, entry.key_ptr.*);
            var name_owned = true;
            errdefer if (name_owned) alloc.free(name);
            const query = try parseSupportedGraphQuery(alloc, entry.value_ptr.*);
            var query_owned = true;
            errdefer if (query_owned) freeGraphQuery(alloc, query);
            try items.append(alloc, .{ .name = name, .query = query });
            name_owned = false;
            query_owned = false;
        }
    }
    return try items.toOwnedSlice(alloc);
}

test "canonical graph query parser distinguishes omission from an explicit empty map" {
    const alloc = std.testing.allocator;

    var omitted = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc, "{}", .{});
    defer omitted.deinit();
    const no_queries = try parseCanonicalGraphQueriesAlloc(alloc, omitted.value);
    defer freeNamedGraphQueries(alloc, no_queries);
    try std.testing.expectEqual(@as(usize, 0), no_queries.len);

    var empty = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc, "{\"graph_queries\":{}}", .{});
    defer empty.deinit();
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseCanonicalGraphQueriesAlloc(alloc, empty.value),
    );
}

test "canonical graph queries reject the legacy expand strategy" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(
        metadata_openapi.QueryRequest,
        alloc,
        \\{"graph_queries":{"walk":{"index":"graph","traverse":{"start":{"keys":["doc:a"]}}}},"expand_strategy":"union"}
    ,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseCanonicalGraphQueriesAlloc(alloc, parsed.value),
    );
}

pub fn freeNamedGraphQueries(
    alloc: std.mem.Allocator,
    items: []const db_mod.types.NamedGraphQuery,
) void {
    for (items) |item| {
        alloc.free(item.name);
        freeGraphQuery(alloc, item.query);
    }
    if (items.len > 0) alloc.free(items);
}

pub fn sortQueriesByDependencies(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.NamedGraphQuery,
) ![]usize {
    return try graph_query_mod.executionOrderAlloc(alloc, queries);
}

pub fn resolveGraphSelectorAlloc(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    selector: graph_query_mod.NodeSelector,
    available_sets: anytype,
) ![][]u8 {
    return switch (selector) {
        .keys => |keys| blk: {
            const duped = try alloc.alloc([]u8, keys.len);
            errdefer alloc.free(duped);
            var initialized: usize = 0;
            errdefer {
                for (duped[0..initialized]) |key| alloc.free(key);
            }
            for (keys, 0..) |key, i| {
                duped[i] = try alloc.dupe(u8, key);
                initialized += 1;
            }
            break :blk duped;
        },
        .identities => |identities| blk: {
            const duped = try alloc.alloc([]u8, identities.len);
            var initialized: usize = 0;
            errdefer {
                for (duped[0..initialized]) |key| alloc.free(key);
                alloc.free(duped);
            }
            for (identities, 0..) |identity, i| {
                if (identity.table) |table| {
                    if (!std.mem.eql(u8, table, source_table)) return error.UnsupportedQueryRequest;
                }
                duped[i] = try alloc.dupe(u8, identity.key);
                initialized += 1;
            }
            break :blk duped;
        },
        .result_ref => |result_ref| blk: {
            const set = findResultSetByRef(available_sets, result_ref.ref) orelse return error.GraphResultRefNotImplemented;
            if (result_ref.binding) |binding| {
                const Set = @TypeOf(set);
                if (!@hasField(Set, "graph_result")) return error.InvalidQueryRequest;
                const graph_result = set.graph_result orelse return error.InvalidQueryRequest;
                if (result_ref.limit == 0 and
                    (graph_result.truncated or @as(u64, graph_result.total_hits) > graph_result.matches.len))
                    return error.UnsupportedQueryRequest;
                break :blk try resolveMatchBindingKeysAlloc(alloc, graph_result.matches, binding, result_ref.limit);
            }
            const Set = @TypeOf(set);
            if (@hasField(Set, "graph_result") and std.mem.startsWith(u8, result_ref.ref, "$graph_results.")) {
                const graph_result = set.graph_result orelse return error.InvalidQueryRequest;
                // Canonical node/path operations expose identities through
                // nodes, never through optional hydrated hits. Legacy pattern
                // results retain their compatibility fallback below.
                if (graph_result.matches.len == 0 and graph_result.aggregates.len == 0) {
                    if (result_ref.limit == 0 and
                        (graph_result.truncated or @as(u64, graph_result.total_hits) > graph_result.nodes.len))
                        return error.UnsupportedQueryRequest;
                    break :blk try resolveTableLocalGraphNodeKeysAlloc(
                        alloc,
                        graph_result.nodes,
                        result_ref.limit,
                    );
                }
            }
            if (result_ref.limit == 0 and resultSetMayBeIncompleteForUnboundedRef(set)) return error.UnsupportedQueryRequest;
            const count: usize = if (result_ref.limit == 0) set.hits.len else @min(set.hits.len, result_ref.limit);
            const duped = try alloc.alloc([]u8, count);
            errdefer alloc.free(duped);
            var initialized: usize = 0;
            errdefer {
                for (duped[0..initialized]) |key| alloc.free(key);
            }
            for (set.hits[0..count], 0..) |hit, i| {
                duped[i] = try alloc.dupe(u8, hit.id);
                initialized += 1;
            }
            break :blk duped;
        },
    };
}

fn resolveTableLocalGraphNodeKeysAlloc(
    alloc: std.mem.Allocator,
    nodes: []const graph_query_mod.GraphResultNode,
    limit: u32,
) ![][]u8 {
    var keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(key);
        keys.deinit(alloc);
    }
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    for (nodes) |node| {
        // The serverless snapshot resolver is table-local. Validate identity
        // provenance before key-only deduplication so a qualified candidate
        // cannot hide behind a local key with the same spelling.
        if (node.table != null) return error.UnsupportedQueryRequest;
        if (seen.contains(node.key)) continue;
        const key = try alloc.dupe(u8, node.key);
        errdefer alloc.free(key);
        try seen.put(alloc, key, {});
        try keys.append(alloc, key);
        if (limit > 0 and keys.items.len >= limit) break;
    }
    return try keys.toOwnedSlice(alloc);
}

fn resolveMatchBindingKeysAlloc(
    alloc: std.mem.Allocator,
    matches: []const db_mod.types.GraphPatternMatch,
    alias: []const u8,
    limit: u32,
) ![][]u8 {
    var keys = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(key);
        keys.deinit(alloc);
    }
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    for (matches) |match| {
        for (match.bindings) |binding| {
            if (!std.mem.eql(u8, binding.alias, alias)) continue;
            // This selector feeds a table-local query. Validate provenance
            // before key-only deduplication so a qualified identity cannot be
            // hidden by an earlier local binding with the same key.
            if (binding.node.table != null) return error.UnsupportedQueryRequest;
            if (seen.contains(binding.node.key)) continue;
            const key = try alloc.dupe(u8, binding.node.key);
            errdefer alloc.free(key);
            try seen.put(alloc, key, {});
            try keys.append(alloc, key);
            if (limit > 0 and keys.items.len >= limit) return try keys.toOwnedSlice(alloc);
            break;
        }
    }
    return try keys.toOwnedSlice(alloc);
}

pub fn testResolveGraphSelectorFailClosedGuard(alloc: std.mem.Allocator) !void {
    const ResultSet = struct {
        name: []const u8,
        hits: []const db_mod.types.SearchHit,
        total_hits: u32,
    };
    const hit = db_mod.types.SearchHit{ .id = @constCast("doc:a") };
    const sets = [_]ResultSet{.{
        .name = "$query_results",
        .hits = @constCast((&[_]db_mod.types.SearchHit{hit})[0..]),
        .total_hits = 2,
    }};

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveGraphSelectorAlloc(
        alloc,
        "docs",
        .{ .result_ref = .{ .ref = "$query_results", .limit = 0 } },
        &sets,
    ));

    const limited = try resolveGraphSelectorAlloc(
        alloc,
        "docs",
        .{ .result_ref = .{ .ref = "$query_results", .limit = 1 } },
        &sets,
    );
    defer {
        for (limited) |key| alloc.free(key);
        alloc.free(limited);
    }
    try std.testing.expectEqual(@as(usize, 1), limited.len);
    try std.testing.expectEqualStrings("doc:a", limited[0]);

    const saturated_sets = [_]struct {
        name: []const u8,
        hits: []const db_mod.types.SearchHit,
        total_hits: u32,
        page_limit: u32,
    }{.{
        .name = "$query_results",
        .hits = @constCast((&[_]db_mod.types.SearchHit{hit})[0..]),
        .total_hits = 1,
        .page_limit = 1,
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, resolveGraphSelectorAlloc(
        alloc,
        "docs",
        .{ .result_ref = .{ .ref = "$query_results", .limit = 0 } },
        &saturated_sets,
    ));

    const same_table = try resolveGraphSelectorAlloc(
        alloc,
        "docs",
        .{ .identities = &.{.{ .key = "doc:a", .table = "docs" }} },
        &sets,
    );
    defer {
        for (same_table) |key| alloc.free(key);
        alloc.free(same_table);
    }
    try std.testing.expectEqualStrings("doc:a", same_table[0]);

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveGraphSelectorAlloc(
        alloc,
        "docs",
        .{ .identities = &.{.{ .key = "doc:a", .table = "other" }} },
        &sets,
    ));

    var result_nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "doc:path-target",
        .depth = 2,
        .distance = 2,
        .path = null,
        .path_edges = null,
    }};
    const graph_result = db_mod.types.GraphSearchResult{
        .name = @constCast("path"),
        .nodes = &result_nodes,
        .hits = &.{},
        .total_hits = 1,
    };
    const graph_sets = [_]struct {
        name: []const u8,
        hits: []const db_mod.types.SearchHit,
        total_hits: u32,
        graph_result: ?*const db_mod.types.GraphSearchResult,
    }{.{
        .name = "path",
        .hits = &.{},
        .total_hits = 1,
        .graph_result = &graph_result,
    }};
    const path_keys = try resolveGraphSelectorAlloc(
        alloc,
        "docs",
        .{ .result_ref = .{ .ref = "$graph_results.path" } },
        &graph_sets,
    );
    defer {
        for (path_keys) |key| alloc.free(key);
        if (path_keys.len > 0) alloc.free(path_keys);
    }
    try std.testing.expectEqual(@as(usize, 1), path_keys.len);
    try std.testing.expectEqualStrings("doc:path-target", path_keys[0]);

    result_nodes[0].table = "entities";
    try std.testing.expectError(error.UnsupportedQueryRequest, resolveGraphSelectorAlloc(
        alloc,
        "docs",
        .{ .result_ref = .{ .ref = "$graph_results.path" } },
        &graph_sets,
    ));

    var local_bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("n"),
        .node = .{
            .key = @constCast("shared"),
            .depth = 0,
            .distance = 0,
            .path = null,
            .path_edges = null,
        },
    }};
    var qualified_bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("n"),
        .node = .{
            .key = @constCast("shared"),
            .table = @constCast("entities"),
            .depth = 0,
            .distance = 0,
            .path = null,
            .path_edges = null,
        },
    }};
    const matches = [_]db_mod.types.GraphPatternMatch{
        .{ .bindings = &local_bindings, .path = &.{} },
        .{ .bindings = &qualified_bindings, .path = &.{} },
    };
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        resolveMatchBindingKeysAlloc(alloc, &matches, "n", 0),
    );
}

fn resultSetMayBeIncompleteForUnboundedRef(set: anytype) bool {
    if (@as(u64, set.total_hits) > set.hits.len) return true;
    const Set = @TypeOf(set);
    if (@hasField(Set, "page_limit")) {
        const page_limit = @field(set, "page_limit");
        return page_limit > 0 and set.hits.len >= page_limit;
    }
    return false;
}

fn parseSupportedGraphQuery(
    alloc: std.mem.Allocator,
    query: indexes_openapi.GraphQuery,
) !graph_query_mod.GraphQuery {
    return try query_contract.parseGraphQuery(alloc, query);
}

fn findResultSetByRef(available_sets: anytype, ref: []const u8) ?@TypeOf(available_sets[0]) {
    // Resolve the public namespace before consulting result names. Graph
    // operation names cannot begin with `$`, so reserved retrieval references
    // can never be shadowed by a graph result in one executor but not another.
    const name = if (std.mem.startsWith(u8, ref, "$graph_results."))
        ref["$graph_results.".len..]
    else if (std.mem.eql(u8, ref, "$query_results") or
        std.mem.eql(u8, ref, "$full_text_results") or
        std.mem.eql(u8, ref, "$embeddings_results") or
        std.mem.eql(u8, ref, "$fused_results"))
        ref
    else
        return null;
    for (available_sets) |set| {
        if (std.mem.eql(u8, set.name, name)) return set;
    }
    return null;
}

fn freeGraphQuery(alloc: std.mem.Allocator, query: graph_query_mod.GraphQuery) void {
    query_contract.freeGraphQuery(alloc, query);
}

test "parse supported graph queries alloc clones edge types and keys" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "neighbors": {
        \\      "index": "graph_idx",
        \\      "traverse": {
        \\        "start": {"keys": ["doc-a"]},
        \\        "edge_types": ["cites", "related"],
        \\        "limit": 7,
        \\        "filter": {"term": "visible", "path": "/tenant"}
        \\      }
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqualStrings("neighbors", items[0].name);
    try std.testing.expectEqual(graph_query_mod.QueryType.traverse, items[0].query.query_type);
    try std.testing.expectEqual(@as(usize, 2), items[0].query.params.edge_types.len);
    try std.testing.expectEqualStrings("cites", items[0].query.params.edge_types[0]);
    try std.testing.expectEqualStrings("related", items[0].query.params.edge_types[1]);
    try std.testing.expectEqual(@as(u32, 7), items[0].query.params.max_results);
    try std.testing.expectEqual(@as(u32, 1), items[0].query.params.max_depth);
    try std.testing.expect(items[0].query.start_nodes == .identities);
    try std.testing.expectEqualStrings("doc-a", items[0].query.start_nodes.identities[0].key);
    try std.testing.expect(items[0].query.start_nodes.identities[0].table == null);
    try std.testing.expect(items[0].query.params.node_filter.filter_query_json != null);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/tenant\",\"term\":\"visible\"}}",
        items[0].query.params.node_filter.filter_query_json.?,
    );
}

test "canonical graph parser rejects deprecated graph searches" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_searches":{"neighbors":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc-a"]},"params":{"edge_types":["cites"],"max_results":7}}}}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        parseCanonicalGraphQueriesAlloc(alloc, parsed.value),
    );
}

test "graph operation names cannot shadow reserved result namespaces" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_queries":{"$query_results":{"index":"graph_idx","traverse":{"start":{"keys":["doc-a"]}}}}}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, parsed.value));

    const ResultSet = struct { name: []const u8 };
    const sets = [_]ResultSet{.{ .name = "walk" }};
    try std.testing.expect(findResultSetByRef(&sets, "walk") == null);
    try std.testing.expect(findResultSetByRef(&sets, "$graph_results.walk") != null);
}

test "canonical graph edge filters and document projections are explicit" {
    const alloc = std.testing.allocator;

    var duplicate_types = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_queries":{"walk":{"index":"graph_idx","traverse":{"start":{"keys":["doc-a"]},"edge_types":["cites","cites"]}}}}
    , .{});
    defer duplicate_types.deinit();
    try std.testing.expectError(error.InvalidQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, duplicate_types.value));

    var traversal_fields = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_queries":{"walk":{"index":"graph_idx","traverse":{"start":{"keys":["doc-a"]},"fields":["title"]}}}}
    , .{});
    defer traversal_fields.deinit();
    try std.testing.expectError(error.InvalidQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, traversal_fields.value));

    var path_fields = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_queries":{"path":{"index":"graph_idx","shortest_path":{"from":{"key":"doc-a"},"to":{"key":"doc-b"},"fields":["title"]}}}}
    , .{});
    defer path_fields.deinit();
    try std.testing.expectError(error.InvalidQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, path_fields.value));
}

test "canonical graph parser rejects canonical and legacy fields together" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries":{"canonical":{"index":"graph_idx","traverse":{"start":{"keys":["doc-a"]}}}},
        \\  "graph_searches":{"legacy":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc-a"]}}}
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, parsed.value));
}

test "parse supported graph queries reject unbounded traversal limits" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_queries":{"neighbors":{"index":"graph_idx","traverse":{"start":{"keys":["doc-a"]},"limit":10001}}}}
    , .{});
    defer parsed.deinit();
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseCanonicalGraphQueriesAlloc(alloc, parsed.value),
    );
}

test "parse supported graph queries rejects unsupported result refs" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "neighbors": {
        \\      "index": "graph_idx",
        \\      "traverse": {"start": {"result_ref": "$hits"}}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, parsed.value));
}

test "parse supported graph queries accepts graph result refs" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "second_hop": {
        \\      "index": "graph_idx",
        \\      "traverse": {"start": {"result_ref": "$graph_results.first_hop", "limit": 5}}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqualStrings("$graph_results.first_hop", items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 5), items[0].query.start_nodes.result_ref.limit);
}

test "parse supported graph queries accepts ranked query result refs" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "neighbors": {
        \\      "index": "graph_idx",
        \\      "traverse": {"start": {"result_ref": "$query_results", "limit": 2}}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqualStrings("$query_results", items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 2), items[0].query.start_nodes.result_ref.limit);
}

test "parse supported graph queries accepts ranked query refs for traversal" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "graph_queries": {
        \\    "neighbors": {
        \\      "index": "graph_idx",
        \\      "traverse": {"start": {"result_ref": "$query_results", "limit": 3}}
        \\    }
        \\  }
        \\}
    ;

    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqualStrings("$query_results", items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 3), items[0].query.start_nodes.result_ref.limit);
}

test "parse supported graph queries accepts ranked query refs for vector retrieval" {
    const alloc = std.testing.allocator;

    var embeddings_parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "traverse": {"start": {"result_ref": "$query_results", "limit": 2}}
        \\    }
        \\  }
        \\}
    , .{});
    defer embeddings_parsed.deinit();

    const embeddings_items = try parseCanonicalGraphQueriesAlloc(alloc, embeddings_parsed.value);
    defer freeNamedGraphQueries(alloc, embeddings_items);
    try std.testing.expectEqualStrings("$query_results", embeddings_items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 2), embeddings_items[0].query.start_nodes.result_ref.limit);
}

test "resolve graph selector fails closed for unbounded paged result refs" {
    try testResolveGraphSelectorFailClosedGuard(std.testing.allocator);
}

test "table-local graph result refs deduplicate nodes before applying limit" {
    const alloc = std.testing.allocator;
    const nodes = [_]graph_query_mod.GraphResultNode{
        .{ .key = "doc:a", .depth = 0, .distance = 0, .path = null, .path_edges = null },
        .{ .key = "doc:a", .depth = 1, .distance = 1, .path = null, .path_edges = null },
        .{ .key = "doc:b", .depth = 1, .distance = 1, .path = null, .path_edges = null },
    };
    const keys = try resolveTableLocalGraphNodeKeysAlloc(alloc, &nodes, 2);
    defer {
        for (keys) |key| alloc.free(key);
        alloc.free(keys);
    }

    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("doc:a", keys[0]);
    try std.testing.expectEqualStrings("doc:b", keys[1]);
}

test "table-local graph result refs reject qualified nodes" {
    const nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "shared",
        .table = "people",
        .depth = 0,
        .distance = 0,
        .path = null,
        .path_edges = null,
    }};
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        resolveTableLocalGraphNodeKeysAlloc(std.testing.allocator, &nodes, 0),
    );
}

test "parse supported graph queries accepts pattern requests" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}, "b": {}},
        \\        "edges": [{"from": "a", "to": "b", "types": ["links"], "min_hops": 1, "max_hops": 2}]
        \\      },
        \\      "return": {"bindings": ["b"], "limit": 10}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqual(graph_query_mod.QueryType.pattern, items[0].query.query_type);
    try std.testing.expectEqual(@as(usize, 2), items[0].query.match_pattern.?.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.match_pattern.?.edges.len);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.return_aliases.len);
    try std.testing.expectEqualStrings("b", items[0].query.return_aliases[0]);
}

test "graph query dependencies require compatible explicit outputs" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "seed": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}, "b": {}},
        \\        "edges": [{"from": "a", "to": "b"}]
        \\      },
        \\      "return": {"bindings": ["b"]}
        \\    },
        \\    "next": {
        \\      "index": "graph_idx",
        \\      "traverse": {"start": {"result_ref": "$graph_results.seed", "binding": "b"}}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();
    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);
    const order = try sortQueriesByDependencies(alloc, items);
    defer alloc.free(order);
    try std.testing.expectEqualStrings("seed", items[order[0]].name);
    try std.testing.expectEqualStrings("next", items[order[1]].name);

    var missing_binding = items[order[1]].query;
    missing_binding.start_nodes.result_ref.binding = null;
    var invalid = [_]db_mod.types.NamedGraphQuery{
        items[order[0]],
        .{ .name = "next", .query = missing_binding },
    };
    try std.testing.expectError(error.InvalidQueryRequest, sortQueriesByDependencies(alloc, &invalid));

    invalid[1].query.start_nodes.result_ref.ref = "$graph_results.missing";
    try std.testing.expectError(error.InvalidQueryRequest, sortQueriesByDependencies(alloc, &invalid));

    const path_dependency = [_]db_mod.types.NamedGraphQuery{
        .{ .name = "path", .query = .{
            .query_type = .shortest_path,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"a"} },
            .target_nodes = .{ .keys = &.{"b"} },
        } },
        .{ .name = "after_path", .query = .{
            .query_type = .traverse,
            .index_name = "graph_idx",
            .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.path" } },
        } },
    };
    const path_order = try sortQueriesByDependencies(alloc, &path_dependency);
    defer alloc.free(path_order);
    try std.testing.expectEqualStrings("path", path_dependency[path_order[0]].name);
    try std.testing.expectEqualStrings("after_path", path_dependency[path_order[1]].name);
}

test "parse supported graph queries accepts pattern node filter queries" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}, "b": {"filter": {"term": "beta", "path": "/title"}}},
        \\        "edges": [{"from": "a", "to": "b", "types": ["links"]}]
        \\      },
        \\      "return": {"bindings": ["a", "b"], "include_documents": true, "fields": ["title"]}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqual(graph_query_mod.QueryType.pattern, items[0].query.query_type);
    try std.testing.expect(items[0].query.include_documents);
    try std.testing.expect(!items[0].query.include_all_fields);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.fields.len);
    try std.testing.expectEqualStrings("title", items[0].query.fields[0]);
    try std.testing.expect(items[0].query.match_pattern.?.nodes[1].filter.filter_query_json != null);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/title\",\"term\":\"beta\"}}",
        items[0].query.match_pattern.?.nodes[1].filter.filter_query_json.?,
    );

    try std.testing.expectError(error.UnexpectedToken, ant_json.parseFromSlice(
        metadata_openapi.QueryRequest,
        alloc,
        "{\"graph_queries\":{\"walk\":{\"index\":\"graph_idx\",\"match\":{\"anchor\":\"a\",\"nodes\":{\"a\":{\"filter\":{\"term\":\"beta\",\"field\":\"title\"}}},\"edges\":[]},\"return\":{\"bindings\":[\"a\"]}}}}",
        .{},
    ));

    var malformed_path = try ant_json.parseFromSlice(
        metadata_openapi.QueryRequest,
        alloc,
        "{\"graph_queries\":{\"walk\":{\"index\":\"graph_idx\",\"match\":{\"anchor\":\"a\",\"nodes\":{\"a\":{\"filter\":{\"term\":\"beta\",\"path\":\"/bad~escape\"}}},\"edges\":[]},\"return\":{\"bindings\":[\"a\"]}}}}",
        .{},
    );
    defer malformed_path.deinit();
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseCanonicalGraphQueriesAlloc(alloc, malformed_path.value),
    );
}

test "graph document filters preserve explicit boolean thresholds" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {"walk": {
        \\    "index": "graph_idx",
        \\    "match": {"anchor": "a", "nodes": {
        \\      "a": {"filter": {"should": {"disjuncts": [
        \\        {"term": "gold", "path": "/tier"}
        \\      ], "min": 0}}},
        \\      "b": {"filter": {"must_not": {"disjuncts": [
        \\        {"term": "blocked", "path": "/status"},
        \\        {"term": "private", "path": "/visibility"}
        \\      ], "min": 2}}}
        \\    }, "edges": [{"from": "a", "to": "b"}]},
        \\    "return": {"bindings": ["a", "b"]}
        \\  }}
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    var optional_should = try ant_json.parseFromSlice(
        std.json.Value,
        alloc,
        items[0].query.match_pattern.?.nodes[0].filter.filter_query_json.?,
        .{},
    );
    defer optional_should.deinit();
    const optional_bool = optional_should.value.object.get("bool").?.object;
    try std.testing.expectEqual(@as(i64, 0), optional_bool.get("minimum_should_match").?.integer);
    try std.testing.expectEqual(@as(usize, 1), optional_bool.get("must").?.array.items.len);
    try std.testing.expect(optional_bool.get("must").?.array.items[0].object.get("match_all") != null);

    var threshold_not = try ant_json.parseFromSlice(
        std.json.Value,
        alloc,
        items[0].query.match_pattern.?.nodes[1].filter.filter_query_json.?,
        .{},
    );
    defer threshold_not.deinit();
    const must_not = threshold_not.value.object.get("bool").?.object.get("must_not").?.array;
    try std.testing.expectEqual(@as(usize, 1), must_not.items.len);
    const nested_bool = must_not.items[0].object.get("bool").?.object;
    try std.testing.expectEqual(@as(i64, 2), nested_bool.get("minimum_should_match").?.integer);
    try std.testing.expectEqual(@as(usize, 2), nested_bool.get("should").?.array.items.len);
}

fn expectInvalidGraphDocumentFilter(filter_json: []const u8) !void {
    const alloc = std.testing.allocator;
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"graph_queries\":{{\"walk\":{{\"index\":\"graph_idx\",\"match\":{{\"anchor\":\"a\",\"nodes\":{{\"a\":{{\"filter\":{s}}}}},\"edges\":[]}},\"return\":{{\"bindings\":[\"a\"]}}}}}}}}",
        .{filter_json},
    );
    defer alloc.free(body);
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, .{});
    defer parsed.deinit();
    try std.testing.expectError(error.InvalidQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, parsed.value));
}

test "graph document filter admission enforces generated schema constraints" {
    try expectInvalidGraphDocumentFilter("{\"ids\":[]}");
    try expectInvalidGraphDocumentFilter("{\"ids\":[\"a\",\"a\"]}");
    try expectInvalidGraphDocumentFilter("{\"numeric_range\":{\"path\":\"/score\"}}");
    try expectInvalidGraphDocumentFilter("{\"numeric_range\":{\"path\":\"/score\",\"min\":2,\"max\":1}}");
    try expectInvalidGraphDocumentFilter("{\"date_range\":{\"path\":\"/at\",\"start\":\"not-a-date\"}}");
    try expectInvalidGraphDocumentFilter("{\"date_range\":{\"path\":\"/at\",\"start\":\"2026-01-02T00:00:00Z\",\"end\":\"2026-01-01T00:00:00Z\"}}");
    try expectInvalidGraphDocumentFilter("{\"disjuncts\":[]}");
    try expectInvalidGraphDocumentFilter("{\"disjuncts\":[{\"match_all\":{}}],\"min\":2}");
}

test "graph match edges preserve explicit direction" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_queries":{"walk":{"index":"graph_idx","match":{"anchor":"a","nodes":{"a":{},"b":{},"c":{}},"edges":[
        \\  {"from":"a","to":"b"},
        \\  {"from":"b","to":"c","direction":"in"},
        \\  {"from":"c","to":"a","direction":"both"}
        \\]},"return":{"bindings":["a"]}}}}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);
    const edges = items[0].query.match_pattern.?.edges;
    try std.testing.expectEqual(graph_mod.EdgeDirection.out, edges[0].step.direction);
    try std.testing.expectEqual(graph_mod.EdgeDirection.in, edges[1].step.direction);
    try std.testing.expectEqual(graph_mod.EdgeDirection.both, edges[2].step.direction);
}

test "graph node filters reject analyzer-backed text clauses" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnexpectedToken, ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}, "b": {"filter": {"match": "beta", "field": "title"}}},
        \\        "edges": [{"from": "a", "to": "b"}]
        \\      },
        \\      "return": {"bindings": ["b"]}
        \\    }
        \\  }
        \\}
    , .{}));
}

test "raw graph admission rejects recursive edge shapes above the contract budget" {
    const alloc = std.testing.allocator;
    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(alloc);
    try body.appendSlice(alloc,
        \\{"graph_queries":{"q":{"index":"graph","match":{"anchor":"a","nodes":{"a":{},"b":{}},"edges":[
    );
    for (0..graph_pattern_mod.max_conjunctive_edges + 1) |i| {
        if (i > 0) try body.append(alloc, ',');
        try body.appendSlice(alloc, "{\"from\":\"a\",\"to\":\"b\"}");
    }
    try body.appendSlice(alloc,
        \\]},"return":{"bindings":["a"]}}}}
    );

    try std.testing.expectError(error.InvalidQueryRequest, rejectInternalDocIdentityFields(alloc, body.items));

    body.clearRetainingCapacity();
    try body.appendSlice(alloc,
        \\{"graph_queries":{"q":{"index":"graph","match":{"anchor":"a","nodes":{"a":{}},"edges":[]},"return":{"aggregates":{
    );
    for (0..graph_pattern_mod.max_count_aggregates + 1) |i| {
        if (i > 0) try body.append(alloc, ',');
        try body.print(alloc, "\"count_{d}\":{{\"count\":\"*\"}}", .{i});
    }
    try body.appendSlice(alloc, "}}}}}");

    try std.testing.expectError(error.InvalidQueryRequest, rejectInternalDocIdentityFields(alloc, body.items));

    var parsed = try ant_json.parseFromSlice(std.json.Value, alloc,
        \\{"graph_queries":{"walk":{"index":"graph","traverse":{"start":{"keys":["doc:a"]}}}}}
    , .{});
    defer parsed.deinit();

    try query_contract.validatePublicQueryEnvelopeValueAlloc(alloc, parsed.value);
    try parsed.value.object.put(alloc, "identity_read_generation", .{ .integer = 1 });
    try std.testing.expectError(
        error.InvalidQueryRequest,
        query_contract.validatePublicQueryEnvelopeValueAlloc(alloc, parsed.value),
    );
    _ = parsed.value.object.orderedRemove("identity_read_generation");
    try parsed.value.object.put(alloc, "_distributed_text_stats", .null);
    try std.testing.expectError(
        error.InvalidQueryRequest,
        query_contract.validatePublicQueryEnvelopeValueAlloc(alloc, parsed.value),
    );
}

test "parse supported graph queries require document hydration for projected fields" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}, "b": {}},
        \\        "edges": [{"from": "a", "to": "b"}]
        \\      },
        \\      "return": {"bindings": ["b"], "fields": ["title"]}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, parseCanonicalGraphQueriesAlloc(alloc, parsed.value));
}

test "parse supported graph queries accepts branches predicates optional groups and counts" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "parity": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}, "b": {}, "c": {}},
        \\        "edges": [
        \\          {"from": "a", "to": "b", "types": ["links"]},
        \\          {"from": "a", "to": "c", "types": ["links"]}
        \\        ],
        \\        "where": {"and": [
        \\          {"not_equal": {"left": {"alias": "b"}, "right": {"alias": "c"}}},
        \\          {"not_exists": {"edges": [{"from": "b", "to": "c", "types": ["blocks"]}]}}
        \\        ]},
        \\        "optional": [{
        \\          "nodes": {"d": {}},
        \\          "edges": [{"from": "d", "to": "b", "types": ["likes"]}]
        \\        }]
        \\      },
        \\      "return": {"aggregates": {"count": {"count": "*"}}}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);
    const pattern = items[0].query.match_pattern.?;
    try std.testing.expectEqual(@as(usize, 2), pattern.edges.len);
    try std.testing.expectEqual(@as(usize, 2), pattern.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), pattern.optional.len);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.aggregates.len);
    try std.testing.expectEqualStrings("count", items[0].query.aggregates[0].name);
}

test "parse supported graph queries rejects distinct field on count all" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "invalid": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {"a": {}},
        \\        "edges": []
        \\      },
        \\      "return": {"aggregates": {"count": {"count": "*", "distinct": false}}}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseCanonicalGraphQueriesAlloc(alloc, parsed.value),
    );
}

test "parse supported graph queries accepts sibling bindings and exact aggregate matches" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "two_hop": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {
        \\          "a": {"filter": {"ids": ["doc-a"]}},
        \\          "b": {"filter": {"term": "beta", "path": "/title"}},
        \\          "c": {"filter": {"prefix": "ga", "path": "/title"}}
        \\        },
        \\        "edges": [
        \\          {"from": "a", "to": "b", "types": ["cites"]},
        \\          {"from": "b", "to": "c", "types": ["cites"]}
        \\        ]
        \\      },
        \\      "return": {"bindings": ["a", "b", "c"], "limit": 10}
        \\    },
        \\    "two_hop_count": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "anchor": "a",
        \\        "nodes": {
        \\          "a": {"filter": {"ids": ["doc-a"]}},
        \\          "b": {"filter": {"term": "beta", "path": "/title"}},
        \\          "c": {"filter": {"prefix": "ga", "path": "/title"}}
        \\        },
        \\        "edges": [
        \\          {"from": "a", "to": "b", "types": ["cites"]},
        \\          {"from": "b", "to": "c", "types": ["cites"]}
        \\        ]
        \\      },
        \\      "return": {"aggregates": {"rows": {"count": "*"}}}
        \\    }
        \\  },
        \\  "limit": 10
        \\}
    , .{
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    const items = try parseCanonicalGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);
    try std.testing.expectEqual(@as(usize, 2), items.len);
}
