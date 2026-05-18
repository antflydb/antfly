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
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const db_mod = @import("../storage/db/mod.zig");
const graph_pattern_mod = @import("../graph/pattern.zig");
const graph_query_mod = @import("../graph/query.zig");
const query_contract = @import("query_contract.zig");

pub fn parseSupportedGraphQueriesAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) ![]const db_mod.types.NamedGraphQuery {
    const graph_searches = request.graph_searches orelse return &.{};

    var items = std.ArrayListUnmanaged(db_mod.types.NamedGraphQuery).empty;
    errdefer freeNamedGraphQueries(alloc, items.items);

    var it = graph_searches.map.iterator();
    while (it.next()) |entry| {
        const name = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(name);
        const query = try parseSupportedGraphQuery(alloc, entry.value_ptr.*);
        errdefer freeGraphQuery(alloc, query);
        try items.append(alloc, .{
            .name = name,
            .query = query,
        });
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
    if (queries.len <= 1) {
        const indexes = try alloc.alloc(usize, queries.len);
        for (indexes, 0..) |*index, i| index.* = i;
        return indexes;
    }

    var by_name = std.StringHashMapUnmanaged(usize).empty;
    defer by_name.deinit(alloc);
    for (queries, 0..) |query, i| try by_name.put(alloc, query.name, i);

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
        .result_ref => |result_ref| blk: {
            const set = findResultSetByRef(available_sets, result_ref.ref) orelse return error.GraphResultRefNotImplemented;
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

fn parseSupportedGraphQuery(
    alloc: std.mem.Allocator,
    query: indexes_openapi.GraphQuery,
) !graph_query_mod.GraphQuery {
    const start_nodes = try parseSupportedNodeSelector(alloc, query.start_nodes orelse return error.UnsupportedQueryRequest);
    errdefer freeNodeSelector(alloc, start_nodes);
    const target_nodes = if (query.target_nodes) |target_selector|
        try parseSupportedNodeSelector(alloc, target_selector)
    else
        null;
    errdefer if (target_nodes) |value| freeNodeSelector(alloc, value);
    const params = try parseSupportedGraphQueryParams(alloc, query.params);
    errdefer freeGraphQueryParams(alloc, params);
    const pattern = if (query.pattern) |steps|
        try parseSupportedPatternSteps(alloc, steps)
    else
        @constCast((&[_]graph_pattern_mod.PatternStep{})[0..]);
    errdefer freePatternSteps(alloc, pattern);
    const return_aliases = if (query.return_aliases) |aliases|
        try cloneFields(alloc, aliases)
    else
        @constCast((&[_][]u8{})[0..]);
    errdefer {
        for (return_aliases) |alias| alloc.free(alias);
        if (return_aliases.len > 0) alloc.free(return_aliases);
    }
    const fields = if (query.fields) |requested_fields|
        try cloneFields(alloc, requested_fields)
    else
        @constCast((&[_][]u8{})[0..]);
    errdefer {
        for (fields) |field| alloc.free(field);
        if (fields.len > 0) alloc.free(fields);
    }
    if (query.include_edges == true) return error.UnsupportedQueryRequest;

    const query_type: graph_query_mod.QueryType = switch (query.type) {
        .neighbors => .neighbors,
        .traverse => .traverse,
        .shortest_path => .shortest_path,
        .pattern => .pattern,
        else => return error.UnsupportedQueryRequest,
    };

    switch (query_type) {
        .neighbors, .traverse => {
            if (target_nodes != null) return error.UnsupportedQueryRequest;
            if (pattern.len > 0 or return_aliases.len > 0 or query.include_documents == true or fields.len > 0) {
                return error.UnsupportedQueryRequest;
            }
        },
        .shortest_path => {
            if (target_nodes == null) return error.UnsupportedQueryRequest;
            if (pattern.len > 0 or return_aliases.len > 0 or query.include_documents == true or fields.len > 0) {
                return error.UnsupportedQueryRequest;
            }
        },
        .pattern => {
            if (pattern.len == 0) return error.UnsupportedQueryRequest;
            if (target_nodes != null) return error.UnsupportedQueryRequest;
        },
        else => unreachable,
    }

    return .{
        .query_type = query_type,
        .index_name = try alloc.dupe(u8, query.index_name),
        .start_nodes = start_nodes,
        .params = params,
        .target_nodes = target_nodes,
        .k = 1,
        .pattern = pattern,
        .return_aliases = return_aliases,
        .include_documents = query.include_documents orelse false,
        .fields = fields,
        .include_all_fields = fields.len == 0,
    };
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
        .visiting => return error.GraphQueryCycle,
        .unvisited => {},
    }

    states[index] = .visiting;
    const query = queries[index];
    if (dependencyName(query.query.start_nodes)) |dep_name| {
        if (by_name.get(dep_name)) |dep_index| try visitQuery(alloc, queries, by_name, states, sorted, dep_index);
    }
    if (query.query.target_nodes) |target_nodes| {
        if (dependencyName(target_nodes)) |dep_name| {
            if (by_name.get(dep_name)) |dep_index| try visitQuery(alloc, queries, by_name, states, sorted, dep_index);
        }
    }
    states[index] = .done;
    try sorted.append(alloc, index);
}

fn dependencyName(selector: graph_query_mod.NodeSelector) ?[]const u8 {
    return switch (selector) {
        .keys => null,
        .result_ref => |result_ref| blk: {
            if (std.mem.startsWith(u8, result_ref.ref, "$graph_results.")) {
                break :blk result_ref.ref["$graph_results.".len..];
            }
            break :blk null;
        },
    };
}

fn findResultSetByRef(available_sets: anytype, ref: []const u8) ?@TypeOf(available_sets[0]) {
    for (available_sets) |set| {
        if (std.mem.eql(u8, set.name, ref)) return set;
    }
    const name = if (std.mem.startsWith(u8, ref, "$graph_results."))
        ref["$graph_results.".len..]
    else if (std.mem.eql(u8, ref, "$full_text_results"))
        ref
    else if (std.mem.eql(u8, ref, "$fused_results"))
        ref
    else if (std.mem.eql(u8, ref, "$embeddings_results"))
        ref
    else
        return null;
    for (available_sets) |set| {
        if (std.mem.eql(u8, set.name, name)) return set;
    }
    return null;
}

fn parseSupportedNodeSelector(
    alloc: std.mem.Allocator,
    selector: indexes_openapi.GraphNodeSelector,
) !graph_query_mod.NodeSelector {
    if (selector.node_filter != null) return error.UnsupportedQueryRequest;
    if (selector.keys) |keys| {
        return .{
            .keys = try cloneFields(alloc, keys),
        };
    }
    if (selector.result_ref) |result_ref| {
        if (!std.mem.startsWith(u8, result_ref, "$graph_results.") and
            !std.mem.eql(u8, result_ref, "$full_text_results") and
            !std.mem.eql(u8, result_ref, "$fused_results") and
            !std.mem.eql(u8, result_ref, "$embeddings_results"))
        {
            return error.UnsupportedQueryRequest;
        }
        return .{
            .result_ref = .{
                .ref = try alloc.dupe(u8, result_ref),
                .limit = if (selector.limit) |limit|
                    std.math.cast(u32, limit) orelse return error.InvalidQueryRequest
                else
                    0,
            },
        };
    }
    return error.UnsupportedQueryRequest;
}

fn parseSupportedGraphQueryParams(
    alloc: std.mem.Allocator,
    params: ?indexes_openapi.GraphQueryParams,
) !graph_query_mod.QueryParams {
    if (params == null) return .{};
    const graph_params = params.?;
    if (graph_params.node_filter != null) return error.UnsupportedQueryRequest;
    if (graph_params.algorithm != null or graph_params.algorithm_params != null) return error.UnsupportedQueryRequest;
    if (graph_params.min_weight != null or graph_params.max_weight != null) return error.UnsupportedQueryRequest;
    if (graph_params.deduplicate_nodes) |deduplicate| {
        if (!deduplicate) return error.UnsupportedQueryRequest;
    }
    if (graph_params.weight_mode) |weight_mode| switch (weight_mode) {
        .min_hops => {},
        else => return error.UnsupportedQueryRequest,
    };

    return .{
        .edge_types = if (graph_params.edge_types) |edge_types| try cloneFields(alloc, edge_types) else &.{},
        .direction = if (graph_params.direction) |direction| switch (direction) {
            .out => .out,
            .in => .in,
            .both => .both,
        } else .out,
        .max_depth = if (graph_params.max_depth) |max_depth|
            std.math.cast(u32, max_depth) orelse return error.InvalidQueryRequest
        else
            3,
        .max_results = if (graph_params.max_results) |max_results|
            std.math.cast(u32, max_results) orelse return error.InvalidQueryRequest
        else
            100,
        .deduplicate = true,
        .include_paths = graph_params.include_paths orelse false,
        .weight_mode = .min_hops,
    };
}

fn parseSupportedPatternSteps(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PatternStep,
) ![]const graph_pattern_mod.PatternStep {
    const steps = try alloc.alloc(graph_pattern_mod.PatternStep, value.len);
    var initialized: usize = 0;
    errdefer {
        for (steps[0..initialized]) |step| {
            alloc.free(step.alias);
            freePatternNodeFilter(alloc, step.node_filter);
            for (step.edge.types) |edge_type| alloc.free(edge_type);
            if (step.edge.types.len > 0) alloc.free(step.edge.types);
        }
        alloc.free(steps);
    }

    for (value, 0..) |step, i| {
        const edge_types = if (step.edge) |edge|
            if (edge.types) |types|
                try cloneFields(alloc, types)
            else
                @constCast((&[_][]u8{})[0..])
        else
            @constCast((&[_][]u8{})[0..]);
        errdefer {
            for (edge_types) |edge_type| alloc.free(edge_type);
            if (edge_types.len > 0) alloc.free(edge_types);
        }

        steps[i] = .{
            .alias = try alloc.dupe(u8, step.alias orelse ""),
            .node_filter = try parsePatternNodeFilter(alloc, step.node_filter),
            .edge = if (step.edge) |edge| .{
                .direction = if (edge.direction) |direction| switch (direction) {
                    .out => .out,
                    .in => .in,
                    .both => .both,
                } else .out,
                .min_hops = if (edge.min_hops) |min_hops|
                    std.math.cast(u32, min_hops) orelse return error.InvalidQueryRequest
                else
                    1,
                .max_hops = if (edge.max_hops) |max_hops|
                    std.math.cast(u32, max_hops) orelse return error.InvalidQueryRequest
                else
                    1,
                .min_weight = edge.min_weight orelse 0.0,
                .max_weight = edge.max_weight orelse 0.0,
                .types = edge_types,
            } else .{
                .types = edge_types,
            },
        };
        initialized += 1;
    }
    return steps;
}

fn parsePatternNodeFilter(
    alloc: std.mem.Allocator,
    filter: ?indexes_openapi.NodeFilter,
) !graph_pattern_mod.NodeFilter {
    const value = filter orelse return .{};
    var out = graph_pattern_mod.NodeFilter{};
    errdefer freePatternNodeFilter(alloc, out);
    if (value.filter_prefix) |filter_prefix| out.filter_prefix = try alloc.dupe(u8, filter_prefix);
    if (value.filter_query) |filter_query| {
        out.filter_query_json = try query_contract.encodeSupportedPatternFilterQueryAlloc(alloc, filter_query);
    }
    return out;
}

fn freePatternNodeFilter(alloc: std.mem.Allocator, filter: graph_pattern_mod.NodeFilter) void {
    if (filter.filter_prefix.len > 0) alloc.free(filter.filter_prefix);
    if (filter.filter_query_json) |query_json| alloc.free(query_json);
}

fn freePatternSteps(alloc: std.mem.Allocator, steps: []const graph_pattern_mod.PatternStep) void {
    for (steps) |step| {
        alloc.free(step.alias);
        freePatternNodeFilter(alloc, step.node_filter);
        for (step.edge.types) |edge_type| alloc.free(edge_type);
        if (step.edge.types.len > 0) alloc.free(step.edge.types);
    }
    if (steps.len > 0) alloc.free(steps);
}

fn cloneFields(alloc: std.mem.Allocator, fields: []const []const u8) ![][]u8 {
    const out = try alloc.alloc([]u8, fields.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |field| alloc.free(field);
    }
    for (fields, 0..) |field, idx| {
        out[idx] = try alloc.dupe(u8, field);
        initialized += 1;
    }
    return out;
}

fn freeGraphQuery(alloc: std.mem.Allocator, query: graph_query_mod.GraphQuery) void {
    alloc.free(query.index_name);
    freeNodeSelector(alloc, query.start_nodes);
    if (query.target_nodes) |target_nodes| freeNodeSelector(alloc, target_nodes);
    freeGraphQueryParams(alloc, query.params);
    freePatternSteps(alloc, query.pattern);
    for (query.return_aliases) |alias| alloc.free(alias);
    if (query.return_aliases.len > 0) alloc.free(query.return_aliases);
    for (query.fields) |field| alloc.free(field);
    if (query.fields.len > 0) alloc.free(query.fields);
}

fn freeNodeSelector(alloc: std.mem.Allocator, selector: graph_query_mod.NodeSelector) void {
    switch (selector) {
        .keys => |keys| {
            for (keys) |key| alloc.free(key);
            if (keys.len > 0) alloc.free(keys);
        },
        .result_ref => |value| alloc.free(value.ref),
    }
}

fn freeGraphQueryParams(alloc: std.mem.Allocator, params: graph_query_mod.QueryParams) void {
    for (params.edge_types) |edge_type| alloc.free(edge_type);
    if (params.edge_types.len > 0) alloc.free(params.edge_types);
}

test "parse supported graph queries alloc clones edge types and keys" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "neighbors": {
        \\      "type": "neighbors",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"keys": ["doc-a"]},
        \\      "params": {"edge_types": ["cites", "related"], "max_results": 7}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqualStrings("neighbors", items[0].name);
    try std.testing.expectEqual(graph_query_mod.QueryType.neighbors, items[0].query.query_type);
    try std.testing.expectEqual(@as(usize, 2), items[0].query.params.edge_types.len);
    try std.testing.expectEqualStrings("cites", items[0].query.params.edge_types[0]);
    try std.testing.expectEqualStrings("related", items[0].query.params.edge_types[1]);
    try std.testing.expectEqual(@as(u32, 7), items[0].query.params.max_results);
}

test "parse supported graph queries rejects unsupported result refs" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "neighbors": {
        \\      "type": "neighbors",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"result_ref": "$hits"}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, parseSupportedGraphQueriesAlloc(alloc, parsed.value));
}

test "parse supported graph queries accepts graph result refs" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "second_hop": {
        \\      "type": "neighbors",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"result_ref": "$graph_results.first_hop", "limit": 5}
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

test "parse supported graph queries accepts full text result refs" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "neighbors": {
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

    try std.testing.expectEqualStrings("$full_text_results", items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 2), items[0].query.start_nodes.result_ref.limit);
}

test "parse supported graph queries accepts fused result refs" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "graph_searches": {
        \\    "neighbors": {
        \\      "type": "neighbors",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"result_ref": "$fused_results", "limit": 3}
        \\    }
        \\  }
        \\}
    ;

    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqualStrings("$fused_results", items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 3), items[0].query.start_nodes.result_ref.limit);
}

test "parse supported graph queries accepts embeddings result refs" {
    const alloc = std.testing.allocator;

    var embeddings_parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "walk": {
        \\      "type": "neighbors",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"result_ref": "$embeddings_results", "limit": 2}
        \\    }
        \\  }
        \\}
    , .{});
    defer embeddings_parsed.deinit();

    const embeddings_items = try parseSupportedGraphQueriesAlloc(alloc, embeddings_parsed.value);
    defer freeNamedGraphQueries(alloc, embeddings_items);
    try std.testing.expectEqualStrings("$embeddings_results", embeddings_items[0].query.start_nodes.result_ref.ref);
    try std.testing.expectEqual(@as(u32, 2), embeddings_items[0].query.start_nodes.result_ref.limit);
}

test "parse supported graph queries accepts pattern requests" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "walk": {
        \\      "type": "pattern",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"keys": ["doc-a"]},
        \\      "pattern": [
        \\        {"alias": "a"},
        \\        {"alias": "b", "edge": {"types": ["links"], "direction": "out", "min_hops": 1, "max_hops": 2}}
        \\      ],
        \\      "return_aliases": ["b"],
        \\      "include_documents": true,
        \\      "fields": ["title"]
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqual(graph_query_mod.QueryType.pattern, items[0].query.query_type);
    try std.testing.expectEqual(@as(usize, 2), items[0].query.pattern.len);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.return_aliases.len);
    try std.testing.expect(items[0].query.include_documents);
    try std.testing.expectEqual(@as(usize, 1), items[0].query.fields.len);
    try std.testing.expectEqualStrings("title", items[0].query.fields[0]);
}

test "parse supported graph queries accepts pattern node filter queries" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc,
        \\{
        \\  "graph_searches": {
        \\    "walk": {
        \\      "type": "pattern",
        \\      "index_name": "graph_idx",
        \\      "start_nodes": {"keys": ["doc-a"]},
        \\      "pattern": [
        \\        {"alias": "a"},
        \\        {"alias": "b", "node_filter": {"filter_query": {"term": "beta", "field": "title"}}, "edge": {"types": ["links"]}}
        \\      ]
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const items = try parseSupportedGraphQueriesAlloc(alloc, parsed.value);
    defer freeNamedGraphQueries(alloc, items);

    try std.testing.expectEqual(@as(usize, 1), items.len);
    try std.testing.expectEqual(graph_query_mod.QueryType.pattern, items[0].query.query_type);
    try std.testing.expect(items[0].query.pattern[1].node_filter.filter_query_json != null);
    try std.testing.expect(std.mem.indexOf(u8, items[0].query.pattern[1].node_filter.filter_query_json.?, "\"term\":{\"title\":\"beta\"}") != null);
}
