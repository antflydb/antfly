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
const db_core = @import("core.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const types = @import("types.zig");
const graph_query_mod = @import("../../graph/query.zig");
const query_search = @import("query/search_exec.zig");

pub fn validateSearchRequestBindings(
    core: *db_core.DBCore,
    alloc: Allocator,
    req: types.SearchRequest,
) !void {
    try validateTextBindings(core, alloc, req);
    try validateDenseBindings(core, req);
    try validateSparseBindings(core, req);
    switch (req.query) {
        .graph => |graph_query| try validateGraphQueryBinding(core, graph_query),
        else => {},
    }
    for (req.graph_queries) |graph_query| {
        try validateGraphQueryBinding(core, graph_query.query);
    }
}

fn preflightRequestNeedsPrimaryTextIndex(req: types.SearchRequest) bool {
    return req.full_text != null or
        req.filter_query_json.len > 0 or
        req.exclusion_query_json.len > 0 or
        (!query_search.isDefaultMatchAll(req.query) and query_search.isTextQuery(req.query));
}

fn validateTextBindings(
    core: *db_core.DBCore,
    alloc: Allocator,
    req: types.SearchRequest,
) !void {
    const needs_primary_text_index = preflightRequestNeedsPrimaryTextIndex(req);
    if (needs_primary_text_index) {
        const entry = core.textIndexEntry(req.index_name) orelse return error.IndexNotFound;
        if (req.full_text) |full_text| {
            try validateTextQueryAgainstIndex(entry, full_text);
        }
        if (!query_search.isDefaultMatchAll(req.query) and query_search.isTextQuery(req.query)) {
            try validateTopLevelQueryAgainstTextIndex(entry, req.query);
        }
        if (req.filter_query_json.len > 0) {
            try validateTextQueryJsonAgainstIndex(alloc, entry, req.filter_query_json);
        }
        if (req.exclusion_query_json.len > 0) {
            try validateTextQueryJsonAgainstIndex(alloc, entry, req.exclusion_query_json);
        }
    }
    for (req.full_text_queries) |query| {
        const entry = core.textIndexEntry(query.index_name) orelse return error.IndexNotFound;
        try validateTextQueryAgainstIndex(entry, query.query);
    }
}

fn validateDenseBindings(core: *db_core.DBCore, req: types.SearchRequest) !void {
    if (req.dense) |dense| try validateDenseQueryBinding(core, req.index_name, dense);
    for (req.dense_queries) |query| try validateDenseQueryBinding(core, query.index_name, query.query);
}

fn validateDenseQueryBinding(
    core: *db_core.DBCore,
    index_name: ?[]const u8,
    dense: types.DenseKnnQuery,
) !void {
    const entry = core.denseIndex(index_name) orelse return error.IndexNotFound;
    if (dense.vector.len > 0 and dense.vector.len != entry.dims) return error.InvalidArgument;
}

fn validateSparseBindings(core: *db_core.DBCore, req: types.SearchRequest) !void {
    if (req.sparse != null) {
        _ = core.sparseIndex(req.index_name) orelse return error.IndexNotFound;
    }
    for (req.sparse_queries) |query| {
        _ = core.sparseIndex(query.index_name) orelse return error.IndexNotFound;
    }
}

fn validateGraphQueryBinding(core: *db_core.DBCore, graph_query: graph_query_mod.GraphQuery) !void {
    const entry = core.graphIndex(graph_query.index_name) orelse return error.IndexNotFound;
    try validateGraphQueryShape(graph_query);
    for (graph_query.params.edge_types) |edge_type| {
        try validateGraphEdgeType(entry, edge_type);
    }
    if (graph_query.pattern.len > 1) {
        for (graph_query.pattern[1..]) |step| {
            for (step.edge.types) |edge_type| {
                try validateGraphEdgeType(entry, edge_type);
            }
        }
    }
}

fn validateGraphQueryShape(graph_query: graph_query_mod.GraphQuery) !void {
    switch (graph_query.query_type) {
        .shortest_path => {
            if (graph_query.target_nodes == null) return error.InvalidArgument;
            if (graph_query.pattern.len > 0) return error.InvalidArgument;
            if (graph_query.return_aliases.len > 0) return error.InvalidArgument;
        },
        .k_shortest_paths => {
            if (graph_query.target_nodes == null) return error.InvalidArgument;
            if (graph_query.k == 0) return error.InvalidArgument;
            if (graph_query.pattern.len > 0) return error.InvalidArgument;
            if (graph_query.return_aliases.len > 0) return error.InvalidArgument;
        },
        .pattern => {
            if (graph_query.pattern.len == 0) return error.InvalidArgument;
            if (graph_query.target_nodes != null) return error.InvalidArgument;
        },
        .traverse, .neighbors => {
            if (graph_query.pattern.len > 0) return error.InvalidArgument;
            if (graph_query.return_aliases.len > 0) return error.InvalidArgument;
        },
    }
}

fn validateGraphEdgeType(
    entry: *const index_manager_mod.IndexManager.GraphIndex,
    edge_type: []const u8,
) !void {
    if (entry.edge_type_configs.len == 0) return;
    if (!graphIndexHasEdgeType(entry, edge_type)) return error.InvalidArgument;
}

fn graphIndexHasEdgeType(entry: *const index_manager_mod.IndexManager.GraphIndex, edge_type: []const u8) bool {
    for (entry.edge_type_configs) |cfg| {
        if (std.mem.eql(u8, cfg.name, edge_type)) return true;
    }
    return false;
}

fn validateTopLevelQueryAgainstTextIndex(
    entry: *const index_manager_mod.IndexManager.TextIndex,
    query: types.Query,
) !void {
    switch (query) {
        .phrase => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .multi_phrase => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .term => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .match => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .match_phrase => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .fuzzy => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .numeric_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .date_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .bool_field => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .geo_distance => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .geo_bbox => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .prefix => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .wildcard => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .regexp => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .term_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .ip_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .geo_shape => |value| try validateTextFieldAgainstIndex(entry, value.field),
        else => {},
    }
}

fn validateTextQueryAgainstIndex(
    entry: *const index_manager_mod.IndexManager.TextIndex,
    query: types.TextQuery,
) !void {
    switch (query) {
        .phrase => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .multi_phrase => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .term => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .match => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .match_phrase => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .fuzzy => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .numeric_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .date_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .bool_field => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .geo_distance => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .geo_bbox => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .prefix => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .wildcard => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .regexp => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .term_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .ip_range => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .geo_shape => |value| try validateTextFieldAgainstIndex(entry, value.field),
        .bool_query => |value| {
            for (value.must) |child| try validateTextQueryAgainstIndex(entry, child);
            for (value.should) |child| try validateTextQueryAgainstIndex(entry, child);
            for (value.must_not) |child| try validateTextQueryAgainstIndex(entry, child);
        },
        else => {},
    }
}

fn validateTextFieldAgainstIndex(
    entry: *const index_manager_mod.IndexManager.TextIndex,
    field: []const u8,
) !void {
    if (textIndexSupportsField(entry, field)) return;
    return error.InvalidArgument;
}

fn textIndexSupportsField(
    entry: *const index_manager_mod.IndexManager.TextIndex,
    field: []const u8,
) bool {
    if (std.mem.eql(u8, field, "_all")) return true;
    if (entry.text_analysis.field_analyzers.len == 0) return true;
    for (entry.text_analysis.field_analyzers) |item| {
        if (std.mem.eql(u8, item.field_name, field)) return true;
    }
    return false;
}

fn validateTextQueryJsonAgainstIndex(
    alloc: Allocator,
    entry: *const index_manager_mod.IndexManager.TextIndex,
    raw: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    try validateTextQueryJsonValueAgainstIndex(entry, parsed.value);
}

fn validateTextQueryJsonValueAgainstIndex(
    entry: *const index_manager_mod.IndexManager.TextIndex,
    value: std.json.Value,
) !void {
    switch (value) {
        .object => |object| {
            if (object.get("field")) |field_value| {
                if (field_value == .string) {
                    try validateTextFieldAgainstIndex(entry, field_value.string);
                }
            }
            if (object.get("term")) |term_value| {
                if (term_value == .object and term_value.object.get("field") == null) {
                    var term_it = term_value.object.iterator();
                    while (term_it.next()) |item| {
                        try validateTextFieldAgainstIndex(entry, item.key_ptr.*);
                    }
                }
            }
            if (object.get("terms")) |terms_value| {
                if (terms_value == .object and terms_value.object.get("field") == null) {
                    var terms_it = terms_value.object.iterator();
                    while (terms_it.next()) |item| {
                        try validateTextFieldAgainstIndex(entry, item.key_ptr.*);
                    }
                }
            }
            var it = object.iterator();
            while (it.next()) |item| {
                try validateTextQueryJsonValueAgainstIndex(entry, item.value_ptr.*);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try validateTextQueryJsonValueAgainstIndex(entry, item);
            }
        },
        else => {},
    }
}
