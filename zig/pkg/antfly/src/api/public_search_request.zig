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
const metadata_openapi = @import("antfly_metadata_openapi");
const public_embedding_query_mod = @import("public_embedding_query.zig");
const public_text_query_mod = @import("public_text_query.zig");

pub const ParsedTextClauses = struct {
    full_text: ?public_text_query_mod.PublicTextSpec = null,
    filter_text: ?public_text_query_mod.PublicTextSpec = null,
    exclusion_text: ?public_text_query_mod.PublicTextSpec = null,

    pub fn deinit(self: *ParsedTextClauses, alloc: std.mem.Allocator) void {
        if (self.full_text) |*value| value.deinit(alloc);
        if (self.filter_text) |*value| value.deinit(alloc);
        if (self.exclusion_text) |*value| value.deinit(alloc);
        self.* = undefined;
    }
};

pub const ParsedEmbedding = struct {
    index_name: []u8,
    query: public_embedding_query_mod.EmbeddingQuery,

    pub fn deinit(self: *ParsedEmbedding, alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        self.query.deinit(alloc);
        self.* = undefined;
    }
};

pub const ParsedEmbeddings = struct {
    items: []ParsedEmbedding = &.{},

    pub fn deinit(self: *ParsedEmbeddings, alloc: std.mem.Allocator) void {
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }

    pub fn find(self: ParsedEmbeddings, index_name: []const u8) ?ParsedEmbedding {
        for (self.items) |item| {
            if (std.mem.eql(u8, item.index_name, index_name)) return item;
        }
        return null;
    }
};

pub fn looksLikePublicSearchRequest(value: std.json.Value) bool {
    if (value != .object) return false;
    const obj = value.object;
    return hasNonNullField(obj, "full_text_search") or
        hasNonNullField(obj, "embeddings") or
        hasNonNullField(obj, "filter_query") or
        hasNonNullField(obj, "exclusion_query") or
        hasNonNullField(obj, "profile") or
        hasNonNullField(obj, "aggregations") or
        hasNonNullField(obj, "analyses") or
        hasNonNullField(obj, "order_by") or
        hasNonNullField(obj, "search_after") or
        hasNonNullField(obj, "search_before") or
        hasNonNullField(obj, "document_renderer") or
        hasNonNullField(obj, "join") or
        hasNonNullField(obj, "foreign_sources") or
        hasNonNullField(obj, "merge_config") or
        hasNonNullField(obj, "pruner") or
        hasNonNullField(obj, "reranker") or
        hasNonNullField(obj, "graph_searches") or
        hasNonNullField(obj, "expand_strategy") or
        hasNonNullField(obj, "distance_over") or
        hasNonNullField(obj, "distance_under");
}

pub fn parseTextClausesAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) !ParsedTextClauses {
    return .{
        .full_text = if (request.full_text_search) |value|
            try public_text_query_mod.parseTextSpecAlloc(alloc, value)
        else
            null,
        .filter_text = if (request.filter_query) |value|
            try public_text_query_mod.parseTextSpecAlloc(alloc, value)
        else
            null,
        .exclusion_text = if (request.exclusion_query) |value|
            try public_text_query_mod.parseTextSpecAlloc(alloc, value)
        else
            null,
    };
}

pub fn parseEmbeddingsAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
    default_k: u32,
) !ParsedEmbeddings {
    const embeddings = request.embeddings orelse return .{};
    var items = std.ArrayListUnmanaged(ParsedEmbedding).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    var it = embeddings.map.iterator();
    while (it.next()) |entry| {
        try items.append(alloc, .{
            .index_name = try alloc.dupe(u8, entry.key_ptr.*),
            .query = try public_embedding_query_mod.parseEmbeddingValueAlloc(alloc, entry.value_ptr.*, default_k),
        });
    }
    return .{
        .items = try items.toOwnedSlice(alloc),
    };
}

pub fn cloneRequestedIndexesAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
    parsed_embeddings: ParsedEmbeddings,
) !?[][]u8 {
    if (request.indexes) |indexes| {
        return try cloneIndexNamesAlloc(alloc, indexes);
    }
    if (parsed_embeddings.items.len == 0) return null;

    const out = try alloc.alloc([]u8, parsed_embeddings.items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |index_name| alloc.free(index_name);
    }
    for (parsed_embeddings.items, 0..) |item, idx| {
        out[idx] = try alloc.dupe(u8, item.index_name);
        initialized += 1;
    }
    return out;
}

pub fn cloneRequestedFieldsAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) !?[][]u8 {
    const fields = request.fields orelse return null;
    return try cloneFieldNamesAlloc(alloc, fields);
}

pub fn cloneFieldNamesAlloc(
    alloc: std.mem.Allocator,
    fields: []const []const u8,
) ![][]u8 {
    const out = try alloc.alloc([]u8, fields.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |field| alloc.free(field);
    }

    for (fields, 0..) |field, idx| {
        try validateRequestedField(field);
        out[idx] = try alloc.dupe(u8, field);
        initialized += 1;
    }
    return out;
}

fn cloneIndexNamesAlloc(alloc: std.mem.Allocator, indexes: []const []const u8) ![][]u8 {
    const out = try alloc.alloc([]u8, indexes.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |index_name| alloc.free(index_name);
    }
    for (indexes, 0..) |index_name, idx| {
        out[idx] = try alloc.dupe(u8, index_name);
        initialized += 1;
    }
    return out;
}

fn hasNonNullField(obj: std.json.ObjectMap, key: []const u8) bool {
    const value = obj.get(key) orelse return false;
    return value != .null;
}

fn validateRequestedField(field: []const u8) !void {
    const trimmed = std.mem.trim(u8, field, &std.ascii.whitespace);
    if (trimmed.len == 0) return error.InvalidQueryRequest;
    if (trimmed[0] == '_' or trimmed[0] == '-') return error.UnsupportedQueryRequest;
    if (std.mem.indexOfScalar(u8, trimmed, '*') != null) return error.UnsupportedQueryRequest;

    var it = std.mem.splitScalar(u8, trimmed, '.');
    while (it.next()) |segment| {
        if (segment.len == 0) return error.InvalidQueryRequest;
    }
}

test "public search request detection ignores legacy text lane" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"text\":\"alpha\",\"limit\":5}", .{});
    defer parsed.deinit();
    try std.testing.expect(!looksLikePublicSearchRequest(parsed.value));
}

test "public search request detection ignores legacy search effort" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"vector\":[1.0,0.0,0.0],\"search_effort\":0.4}", .{});
    defer parsed.deinit();
    try std.testing.expect(!looksLikePublicSearchRequest(parsed.value));
}

test "public search request detection recognizes public full text shape" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"full_text_search\":{\"query\":\"body:alpha\"}}", .{});
    defer parsed.deinit();
    try std.testing.expect(looksLikePublicSearchRequest(parsed.value));
}
