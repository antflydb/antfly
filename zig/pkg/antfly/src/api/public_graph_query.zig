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
const db_mod = @import("../storage/db/mod.zig");
const graph_query_mod = @import("../graph/query.zig");
const query_contract = @import("query_contract.zig");

pub fn rejectInternalDocIdentityFields(alloc: std.mem.Allocator, body: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    if (objectHasInternalDocIdentityField(parsed.value.object)) return error.InvalidQueryRequest;
}

fn objectHasInternalDocIdentityField(object: std.json.ObjectMap) bool {
    const internal_fields = [_][]const u8{
        "identity_read_generation",
        "allow_doc_identity_reassignment",
        "_identity_read_generation",
        "native_doc_id_constraints",
        "_filter_doc_ids",
        "_filter_doc_ids_positive",
        "_exclude_doc_ids",
        "_resolved_doc_filter",
    };
    inline for (internal_fields) |field| {
        if (object.get(field) != null) return true;
    }
    return false;
}

pub fn parseSupportedGraphQueriesAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) ![]const db_mod.types.NamedGraphQuery {
    if (request.graph_queries != null and request.graph_searches != null)
        return error.InvalidQueryRequest;

    var items = std.ArrayListUnmanaged(db_mod.types.NamedGraphQuery).empty;
    errdefer freeNamedGraphQueries(alloc, items.items);

    if (request.graph_queries) |graph_queries| {
        var it = graph_queries.map.iterator();
        while (it.next()) |entry| {
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
    } else if (request.graph_searches) |graph_searches| {
        var it = graph_searches.map.iterator();
        while (it.next()) |entry| {
            const name = try alloc.dupe(u8, entry.key_ptr.*);
            var name_owned = true;
            errdefer if (name_owned) alloc.free(name);
            const query = try query_contract.parseLegacyGraphQuery(alloc, entry.value_ptr.*);
            var query_owned = true;
            errdefer if (query_owned) freeGraphQuery(alloc, query);
            try items.append(alloc, .{ .name = name, .query = query });
            name_owned = false;
            query_owned = false;
        }
    }
    return try items.toOwnedSlice(alloc);
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
    var by_name = std.StringHashMapUnmanaged(usize).empty;
    defer by_name.deinit(alloc);
    for (queries, 0..) |query, i| {
        const result = try by_name.getOrPut(alloc, query.name);
        if (result.found_existing) return error.InvalidQueryRequest;
        result.value_ptr.* = i;
    }

    var sorted = std.ArrayListUnmanaged(usize).empty;
    defer sorted.deinit(alloc);

    const VisitState = enum { unvisited, visiting, done };
    const states = try alloc.alloc(VisitState, queries.len);
    defer alloc.free(states);
    @memset(states, .unvisited);

    for (queries, 0..) |_, i| {
        try visitQuery(alloc, queries, &by_name, states, &sorted, i);
    }
    return try sorted.toOwnedSlice(alloc);
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
            if (!std.mem.eql(u8, binding.alias, alias) or seen.contains(binding.node.key)) continue;
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

fn visitQuery(
    alloc: std.mem.Allocator,
    queries: []const db_mod.types.NamedGraphQuery,
    by_name: *std.StringHashMapUnmanaged(usize),
    states: anytype,
    sorted: *std.ArrayListUnmanaged(usize),
    index: usize,
) !void {
    switch (states[index]) {
        .done => return,
        .visiting => return error.InvalidQueryRequest,
        .unvisited => {},
    }

    states[index] = .visiting;
    const query = queries[index];
    if (try dependencyIndex(queries, by_name, query.query.start_nodes)) |dep_index| {
        try visitQuery(alloc, queries, by_name, states, sorted, dep_index);
    }
    if (query.query.target_nodes) |target_nodes| {
        if (try dependencyIndex(queries, by_name, target_nodes)) |dep_index|
            try visitQuery(alloc, queries, by_name, states, sorted, dep_index);
    }
    states[index] = .done;
    try sorted.append(alloc, index);
}

fn dependencyIndex(
    queries: []const db_mod.types.NamedGraphQuery,
    by_name: *const std.StringHashMapUnmanaged(usize),
    selector: graph_query_mod.NodeSelector,
) !?usize {
    const result_ref = switch (selector) {
        .keys, .identities => return null,
        .result_ref => |value| value,
    };
    if (!std.mem.startsWith(u8, result_ref.ref, "$graph_results.")) return null;
    const dep_name = result_ref.ref["$graph_results.".len..];
    const dep_index = by_name.get(dep_name) orelse return error.InvalidQueryRequest;
    const dependency = queries[dep_index].query;
    switch (dependency.query_type) {
        .neighbors, .traverse => if (result_ref.binding != null) return error.InvalidQueryRequest,
        .pattern => {
            const binding = result_ref.binding orelse return error.InvalidQueryRequest;
            if (dependency.match_pattern == null or dependency.aggregates.len > 0)
                return error.InvalidQueryRequest;
            var returned = false;
            for (dependency.return_aliases) |alias| {
                if (std.mem.eql(u8, alias, binding)) {
                    returned = true;
                    break;
                }
            }
            if (!returned) return error.InvalidQueryRequest;
        },
        .shortest_path, .k_shortest_paths => return error.InvalidQueryRequest,
    }
    return dep_index;
}

fn findResultSetByRef(available_sets: anytype, ref: []const u8) ?@TypeOf(available_sets[0]) {
    for (available_sets) |set| {
        if (std.mem.eql(u8, set.name, ref)) return set;
    }
    const name = if (std.mem.startsWith(u8, ref, "$graph_results."))
        ref["$graph_results.".len..]
    else if (std.mem.eql(u8, ref, "$query_results"))
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
        \\        "filter": {"term": "visible", "field": "tenant"}
        \\      }
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
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
        "{\"term\":{\"path\":\"tenant\",\"term\":\"visible\"}}",
        items[0].query.params.node_filter.filter_query_json.?,
    );
}

test "parse supported graph queries accepts deprecated graph searches" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_searches":{"neighbors":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc-a"]},"params":{"edge_types":["cites"],"max_results":7}}}}
    , .{});
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expect(items[0].query.legacy_response);
    try std.testing.expect(items[0].query.start_nodes == .keys);
    try std.testing.expectEqual(graph_query_mod.QueryType.neighbors, items[0].query.query_type);
    try std.testing.expectEqualStrings("graph_idx", items[0].query.index_name);
    try std.testing.expectEqual(@as(u32, 7), items[0].query.params.max_results);
}

test "parse supported graph queries rejects canonical and legacy fields together" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries":{"canonical":{"index":"graph_idx","traverse":{"start":{"keys":["doc-a"]}}}},
        \\  "graph_searches":{"legacy":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"keys":["doc-a"]}}}
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, parseSupportedGraphQueriesAlloc(alloc, parsed.value));
}

test "legacy graph result refs normalize to the ranked query result" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "legacy": {
        \\      "type": "neighbors",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"result_ref": "$full_text_results", "limit": 2}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();
    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);
    try std.testing.expect(items[0].query.legacy_response);
    try std.testing.expectEqualStrings("$query_results", items[0].query.start_nodes.result_ref.ref);
}

test "parse supported graph queries reject unbounded traversal limits" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{"graph_queries":{"neighbors":{"index":"graph_idx","traverse":{"start":{"keys":["doc-a"]},"limit":10001}}}}
    , .{});
    defer parsed.deinit();
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseSupportedGraphQueriesAlloc(alloc, parsed.value),
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

    try std.testing.expectError(error.InvalidQueryRequest, parseSupportedGraphQueriesAlloc(alloc, parsed.value));
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

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
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

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
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

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
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

    const embeddings_items = try parseSupportedGraphQueriesAlloc(alloc, embeddings_parsed.value);
    defer freeNamedGraphQueries(alloc, embeddings_items);
    try std.testing.expectEqualStrings("$query_results", embeddings_items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 2), embeddings_items[0].query.start_nodes.result_ref.limit);
}

test "resolve graph selector fails closed for unbounded paged result refs" {
    try testResolveGraphSelectorFailClosedGuard(std.testing.allocator);
}

test "parse supported graph queries accepts pattern requests" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "nodes": {"a": {}, "b": {}},
        \\        "edges": [{"from": "a", "to": "b", "types": ["links"], "direction": "out", "min_hops": 1, "max_hops": 2}]
        \\      },
        \\      "return": {"bindings": ["b"], "limit": 10}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
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
    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
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
}

test "parse supported graph queries accepts pattern node filter queries" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "walk": {
        \\      "index": "graph_idx",
        \\      "match": {
        \\        "nodes": {"a": {}, "b": {"filter": {"term": "beta", "field": "title"}}},
        \\        "edges": [{"from": "a", "to": "b", "types": ["links"]}]
        \\      },
        \\      "return": {"bindings": ["a", "b"], "include_documents": true, "fields": ["title"]}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqual(graph_query_mod.QueryType.pattern, items[0].query.query_type);
    try std.testing.expect(items[0].query.include_documents);
    try std.testing.expect(!items[0].query.include_all_fields);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.fields.len);
    try std.testing.expectEqualStrings("title", items[0].query.fields[0]);
    try std.testing.expect(items[0].query.match_pattern.?.nodes[1].filter.filter_query_json != null);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"title\",\"term\":\"beta\"}}",
        items[0].query.match_pattern.?.nodes[1].filter.filter_query_json.?,
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
        \\        "nodes": {"a": {}, "b": {}},
        \\        "edges": [{"from": "a", "to": "b"}]
        \\      },
        \\      "return": {"bindings": ["b"], "fields": ["title"]}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.InvalidQueryRequest, parseSupportedGraphQueriesAlloc(alloc, parsed.value));
}

test "parse supported graph queries accepts branches predicates optional groups and counts" {
    const alloc = std.testing.allocator;
    var parsed = try ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_queries": {
        \\    "parity": {
        \\      "index": "graph_idx",
        \\      "match": {
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

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);
    const pattern = items[0].query.match_pattern.?;
    try std.testing.expectEqual(@as(usize, 2), pattern.edges.len);
    try std.testing.expectEqual(@as(usize, 2), pattern.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), pattern.optional.len);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.aggregates.len);
    try std.testing.expectEqualStrings("count", items[0].query.aggregates[0].name);
}
