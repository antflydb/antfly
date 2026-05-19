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

pub const QueryMode = enum {
    text,
    vector,
    hybrid,
    sparse,
};

pub const GraphQueryDirection = enum {
    out,
    in,
    both,
};

pub const QueryFusionStrategy = enum {
    weighted_rrf,
    weighted_sum,
};

pub const QueryOperator = enum {
    any_terms,
    all_terms,
    phrase,
    prefix_any_term,
};

pub const SparseTermWeight = struct {
    term: []u8,
    weight: f32,

    pub fn deinit(self: *SparseTermWeight, alloc: Allocator) void {
        alloc.free(self.term);
        self.* = undefined;
    }
};

pub const QueryRequest = struct {
    text: []u8,
    fields: ?[][]u8 = null,
    filter_prefix: ?[]u8 = null,
    filter_text: ?[]u8 = null,
    exclusion_text: ?[]u8 = null,
    vector: ?[]f32 = null,
    sparse: ?[]SparseTermWeight = null,
    semantic_search: ?[]u8 = null,
    embedding_template: ?[]u8 = null,
    indexes: ?[][]u8 = null,
    count_only: bool = false,
    limit: usize = 10,
    offset: usize = 0,
    min_score: u32 = 0,
    num_probes: u32 = 2,
    search_effort: ?f32 = null,
    mode: QueryMode = .text,
    operator: QueryOperator = .all_terms,
    filter_operator: QueryOperator = .all_terms,
    exclusion_operator: QueryOperator = .all_terms,
    fusion_strategy: QueryFusionStrategy = .weighted_rrf,
    text_weight: f32 = 0.5,
    vector_weight: f32 = 0.5,
    sparse_weight: f32 = 0.5,

    pub fn deinit(self: *QueryRequest, alloc: Allocator) void {
        alloc.free(self.text);
        if (self.fields) |fields| {
            for (fields) |field| alloc.free(field);
            alloc.free(fields);
        }
        if (self.filter_prefix) |value| alloc.free(value);
        if (self.filter_text) |value| alloc.free(value);
        if (self.exclusion_text) |value| alloc.free(value);
        if (self.vector) |vector| alloc.free(vector);
        if (self.sparse) |weights| {
            for (weights) |*weight| weight.deinit(alloc);
            alloc.free(weights);
        }
        if (self.semantic_search) |value| alloc.free(value);
        if (self.embedding_template) |value| alloc.free(value);
        if (self.indexes) |indexes| {
            for (indexes) |index_name| alloc.free(index_name);
            alloc.free(indexes);
        }
        self.* = undefined;
    }
};

pub const GraphNeighborsRequest = struct {
    index_name: []u8 = &.{},
    doc_id: []u8,
    direction: GraphQueryDirection = .out,
    edge_types: ?[]const []const u8 = null,
    limit: usize = 100,

    pub fn deinit(self: *GraphNeighborsRequest, alloc: Allocator) void {
        if (self.index_name.len > 0) alloc.free(self.index_name);
        alloc.free(self.doc_id);
        if (self.edge_types) |edge_types| {
            for (edge_types) |edge_type| alloc.free(edge_type);
            alloc.free(edge_types);
        }
        self.* = undefined;
    }
};

pub const GraphTraverseRequest = struct {
    index_name: []u8 = &.{},
    start_doc_id: []u8,
    direction: GraphQueryDirection = .out,
    edge_types: ?[]const []const u8 = null,
    max_depth: u32 = 3,
    limit: usize = 100,
    include_start: bool = false,

    pub fn deinit(self: *GraphTraverseRequest, alloc: Allocator) void {
        if (self.index_name.len > 0) alloc.free(self.index_name);
        alloc.free(self.start_doc_id);
        if (self.edge_types) |edge_types| {
            for (edge_types) |edge_type| alloc.free(edge_type);
            alloc.free(edge_types);
        }
        self.* = undefined;
    }
};

pub const GraphShortestPathRequest = struct {
    index_name: []u8 = &.{},
    start_doc_id: []u8,
    end_doc_id: []u8,
    direction: GraphQueryDirection = .out,
    edge_types: ?[]const []const u8 = null,
    max_depth: u32 = 6,

    pub fn deinit(self: *GraphShortestPathRequest, alloc: Allocator) void {
        if (self.index_name.len > 0) alloc.free(self.index_name);
        alloc.free(self.start_doc_id);
        alloc.free(self.end_doc_id);
        if (self.edge_types) |edge_types| {
            for (edge_types) |edge_type| alloc.free(edge_type);
            alloc.free(edge_types);
        }
        self.* = undefined;
    }
};

pub const SearchHit = struct {
    doc_id: []u8,
    body: []u8,
    score: u32,

    pub fn deinit(self: *SearchHit, alloc: Allocator) void {
        alloc.free(self.doc_id);
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub fn freeHits(alloc: Allocator, hits: []SearchHit) void {
    for (hits) |*hit| hit.deinit(alloc);
    alloc.free(hits);
}

test "query request defaults limit" {
    const req = QueryRequest{ .text = @constCast("alpha") };
    try std.testing.expectEqual(@as(usize, 10), req.limit);
    try std.testing.expectEqual(@as(usize, 0), req.offset);
    try std.testing.expectEqual(@as(u32, 0), req.min_score);
    try std.testing.expectEqual(@as(u32, 2), req.num_probes);
    try std.testing.expectEqual(QueryMode.text, req.mode);
    try std.testing.expectEqual(QueryOperator.all_terms, req.operator);
    try std.testing.expectEqual(QueryOperator.all_terms, req.filter_operator);
    try std.testing.expectEqual(QueryOperator.all_terms, req.exclusion_operator);
    try std.testing.expectEqual(QueryFusionStrategy.weighted_rrf, req.fusion_strategy);
    try std.testing.expectEqual(@as(?[][]u8, null), req.fields);
    try std.testing.expectEqual(@as(?[]u8, null), req.filter_prefix);
    try std.testing.expectEqual(@as(?[]u8, null), req.filter_text);
    try std.testing.expectEqual(@as(?[]u8, null), req.exclusion_text);
    try std.testing.expectEqual(@as(?[]f32, null), req.vector);
    try std.testing.expectEqual(@as(?[]SparseTermWeight, null), req.sparse);
    try std.testing.expectEqual(@as(?[]u8, null), req.semantic_search);
    try std.testing.expectEqual(@as(?[]u8, null), req.embedding_template);
    try std.testing.expectEqual(@as(?[][]u8, null), req.indexes);
    try std.testing.expect(!req.count_only);
    try std.testing.expectEqual(@as(f32, 0.5), req.sparse_weight);
}

test "graph neighbors request defaults direction and limit" {
    const req = GraphNeighborsRequest{ .doc_id = @constCast("doc-a") };
    try std.testing.expectEqual(GraphQueryDirection.out, req.direction);
    try std.testing.expectEqual(@as(usize, 100), req.limit);
    try std.testing.expectEqual(@as(?[]const []const u8, null), req.edge_types);
}

test "graph traverse request defaults depth and limit" {
    const req = GraphTraverseRequest{ .start_doc_id = @constCast("doc-a") };
    try std.testing.expectEqual(GraphQueryDirection.out, req.direction);
    try std.testing.expectEqual(@as(u32, 3), req.max_depth);
    try std.testing.expectEqual(@as(usize, 100), req.limit);
    try std.testing.expect(!req.include_start);
}

test "graph shortest path request defaults depth" {
    const req = GraphShortestPathRequest{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast("doc-z"),
    };
    try std.testing.expectEqual(GraphQueryDirection.out, req.direction);
    try std.testing.expectEqual(@as(u32, 6), req.max_depth);
    try std.testing.expectEqual(@as(?[]const []const u8, null), req.edge_types);
}
