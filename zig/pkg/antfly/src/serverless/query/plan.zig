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
const request = @import("request.zig");
const search_sources = @import("../search_sources.zig");
const public_embedding_query_mod = @import("../../api/public_embedding_query.zig");
const public_search_request_mod = @import("../../api/public_search_request.zig");

pub const SearchPlan = struct {
    request: request.QueryRequest,
    sources: search_sources.ResolvedSearchSources,

    pub fn deinit(self: *SearchPlan, alloc: Allocator) void {
        self.request.deinit(alloc);
        self.* = undefined;
    }

    pub fn vectorSource(self: *const SearchPlan) ?search_sources.VectorSourceDescriptor {
        return self.sources.findVector();
    }

    pub fn sparseSource(self: *const SearchPlan) ?search_sources.SparseSourceDescriptor {
        return self.sources.findSparse();
    }

    pub fn usesTextLane(self: *const SearchPlan) bool {
        return switch (self.request.mode) {
            .text => true,
            .hybrid => std.mem.trim(u8, self.request.text, &std.ascii.whitespace).len != 0,
            else => false,
        };
    }

    pub fn usesVectorLane(self: *const SearchPlan) bool {
        return switch (self.request.mode) {
            .vector => true,
            .hybrid => (self.request.vector != null and self.request.vector.?.len != 0) or self.request.semantic_search != null,
            else => false,
        };
    }

    pub fn usesSparseLane(self: *const SearchPlan) bool {
        return switch (self.request.mode) {
            .sparse => true,
            .hybrid => self.request.sparse != null and self.request.sparse.?.len != 0,
            else => false,
        };
    }
};

pub fn parseSearchPlanAlloc(
    alloc: Allocator,
    body: []const u8,
    published_search_sources: search_sources.PublishedSearchSources,
) !SearchPlan {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    if (queryBodyHasForbiddenDocIdentityControlFields(parsed.value.object)) return error.InvalidQueryRequest;

    if (public_search_request_mod.looksLikePublicSearchRequest(parsed.value)) {
        var public_parsed = parseOwnedPublicQueryRequestAlloc(alloc, body) catch return error.InvalidQueryRequest;
        defer public_parsed.deinit();
        return try parsePublicSearchPlanAlloc(alloc, public_parsed.value, published_search_sources);
    }

    return try parseLegacySearchPlanAlloc(alloc, body, published_search_sources);
}

fn queryBodyHasForbiddenDocIdentityControlFields(object: std.json.ObjectMap) bool {
    return object.get("identity_read_generation") != null or
        object.get("allow_doc_identity_reassignment") != null or
        object.get("_identity_read_generation") != null or
        object.get("native_doc_id_constraints") != null or
        object.get("_filter_doc_ids") != null or
        object.get("_filter_doc_ids_positive") != null or
        object.get("_exclude_doc_ids") != null;
}

fn rejectForbiddenDocIdentityControlFieldsAlloc(alloc: Allocator, body: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    if (queryBodyHasForbiddenDocIdentityControlFields(parsed.value.object)) return error.InvalidQueryRequest;
}

fn parseOwnedPublicQueryRequestAlloc(
    alloc: Allocator,
    body: []const u8,
) !std.json.Parsed(metadata_openapi.QueryRequest) {
    return std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
}

fn parsePublicSearchPlanAlloc(
    alloc: Allocator,
    query_request: metadata_openapi.QueryRequest,
    published_search_sources: search_sources.PublishedSearchSources,
) !SearchPlan {
    if (query_request.analyses != null or
        query_request.order_by != null or
        query_request.search_after != null or
        query_request.search_before != null or
        query_request.document_renderer != null or
        query_request.join != null or
        query_request.foreign_sources != null or
        query_request.merge_config != null or
        query_request.pruner != null or
        query_request.reranker != null or
        query_request.graph_searches != null or
        query_request.expand_strategy != null or
        query_request.distance_over != null or
        query_request.distance_under != null)
    {
        return error.UnsupportedQueryRequest;
    }

    var text_clauses = try public_search_request_mod.parseTextClausesAlloc(alloc, query_request);
    defer text_clauses.deinit(alloc);

    var embeddings = try public_search_request_mod.parseEmbeddingsAlloc(alloc, query_request, @intCast(query_request.limit orelse 10));
    defer embeddings.deinit(alloc);
    var requested_fields = try public_search_request_mod.cloneRequestedFieldsAlloc(alloc, query_request);
    errdefer if (requested_fields) |fields| {
        for (fields) |field| alloc.free(field);
        alloc.free(fields);
    };

    var public_vector: ?[]f32 = null;
    var public_sparse: ?[]request.SparseTermWeight = null;
    var public_indexes = try public_search_request_mod.cloneRequestedIndexesAlloc(alloc, query_request, embeddings);
    errdefer if (public_vector) |vector| alloc.free(vector);
    errdefer if (public_sparse) |weights| {
        for (weights) |*weight| weight.deinit(alloc);
        alloc.free(weights);
    };
    errdefer if (public_indexes) |indexes| {
        for (indexes) |index_name| alloc.free(index_name);
        alloc.free(indexes);
    };

    for (embeddings.items) |item| switch (item.query) {
        .dense => |dense| {
            if (public_vector != null) return error.UnsupportedQueryRequest;
            public_vector = try alloc.dupe(f32, dense.vector);
        },
        .sparse => |sparse| {
            if (public_sparse != null) return error.UnsupportedQueryRequest;
            public_sparse = try sparseEmbeddingTermsAlloc(alloc, sparse);
        },
    };

    const has_text = text_clauses.full_text != null and std.mem.trim(u8, text_clauses.full_text.?.text, &std.ascii.whitespace).len != 0;
    const has_semantic = query_request.semantic_search != null;
    const has_dense_vector = public_vector != null;
    const has_sparse = public_sparse != null and public_sparse.?.len != 0;
    const default_mode: request.QueryMode = if (has_semantic)
        if (has_text or has_sparse) .hybrid else .vector
    else if (has_dense_vector)
        if (has_text or has_sparse) .hybrid else .vector
    else if (has_sparse)
        .sparse
    else
        .text;

    const request_text = if (text_clauses.full_text) |value|
        value.text
    else if (query_request.semantic_search) |semantic_search|
        semantic_search
    else
        "";

    var req = request.QueryRequest{
        .text = try alloc.dupe(u8, request_text),
        .fields = requested_fields,
        .filter_prefix = if (query_request.filter_prefix) |value| try alloc.dupe(u8, value) else null,
        .filter_text = if (text_clauses.filter_text) |value| try alloc.dupe(u8, value.text) else null,
        .exclusion_text = if (text_clauses.exclusion_text) |value| try alloc.dupe(u8, value.text) else null,
        .vector = public_vector,
        .sparse = public_sparse,
        .semantic_search = if (query_request.semantic_search) |value| try alloc.dupe(u8, value) else null,
        .embedding_template = if (query_request.embedding_template) |value| try alloc.dupe(u8, value) else null,
        .indexes = public_indexes,
        .count_only = query_request.count orelse false,
        .limit = @intCast(query_request.limit orelse 10),
        .offset = @intCast(query_request.offset orelse 0),
        .min_score = 0,
        .num_probes = 2,
        .search_effort = query_request.search_effort,
        .mode = default_mode,
        .operator = if (text_clauses.full_text) |value| switch (value.operator) {
            .all_terms => .all_terms,
            .any_terms => .any_terms,
            .phrase => .phrase,
            .prefix_any_term => .prefix_any_term,
        } else .all_terms,
        .filter_operator = if (text_clauses.filter_text) |value| switch (value.operator) {
            .all_terms => .all_terms,
            .any_terms => .any_terms,
            .phrase => .phrase,
            .prefix_any_term => .prefix_any_term,
        } else .all_terms,
        .exclusion_operator = if (text_clauses.exclusion_text) |value| switch (value.operator) {
            .all_terms => .all_terms,
            .any_terms => .any_terms,
            .phrase => .phrase,
            .prefix_any_term => .prefix_any_term,
        } else .all_terms,
        .fusion_strategy = .weighted_rrf,
        .text_weight = 0.5,
        .vector_weight = 0.5,
        .sparse_weight = 0.5,
    };
    errdefer req.deinit(alloc);
    requested_fields = null;
    public_vector = null;
    public_sparse = null;
    public_indexes = null;

    if (req.embedding_template != null and req.semantic_search == null) return error.InvalidQueryRequest;
    if (req.semantic_search != null and req.vector != null) return error.InvalidQueryRequest;

    try validateSearchRequest(req);

    var sources = try resolveSearchSources(req, published_search_sources);
    _ = &sources;
    return .{
        .request = req,
        .sources = sources,
    };
}

fn parseLegacySearchPlanAlloc(
    alloc: Allocator,
    body: []const u8,
    published_search_sources: search_sources.PublishedSearchSources,
) !SearchPlan {
    const ParsedLegacyRequest = struct {
        text: ?[]const u8 = null,
        fields: ?[][]const u8 = null,
        count: ?bool = null,
        filter_prefix: ?[]const u8 = null,
        vector: ?[]f32 = null,
        sparse: ?[]request.SparseTermWeight = null,
        semantic_search: ?[]const u8 = null,
        embedding_template: ?[]const u8 = null,
        indexes: ?[][]const u8 = null,
        limit: ?usize = null,
        offset: ?usize = null,
        min_score: ?u32 = null,
        num_probes: ?u32 = null,
        search_effort: ?f32 = null,
        mode: ?request.QueryMode = null,
        operator: ?request.QueryOperator = null,
        fusion_strategy: ?request.QueryFusionStrategy = null,
        text_weight: ?f32 = null,
        vector_weight: ?f32 = null,
        sparse_weight: ?f32 = null,
    };

    var parsed = try std.json.parseFromSlice(ParsedLegacyRequest, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var requested_fields = if (parsed.value.fields) |fields|
        try public_search_request_mod.cloneFieldNamesAlloc(alloc, fields)
    else
        null;
    errdefer if (requested_fields) |fields| {
        for (fields) |field| alloc.free(field);
        alloc.free(fields);
    };

    const has_text = parsed.value.text != null and std.mem.trim(u8, parsed.value.text.?, &std.ascii.whitespace).len != 0;
    const has_semantic = parsed.value.semantic_search != null;
    const has_dense_vector = parsed.value.vector != null;
    const has_sparse = parsed.value.sparse != null and parsed.value.sparse.?.len != 0;
    const default_mode: request.QueryMode = if (parsed.value.mode) |mode| switch (mode) {
        .text => if (has_semantic and !has_text and !has_dense_vector and !has_sparse)
            .vector
        else
            .text,
        else => mode,
    } else if (has_semantic)
        if (has_text or has_sparse) .hybrid else .vector
    else if (has_dense_vector)
        if (has_text or has_sparse) .hybrid else .vector
    else
        .text;

    const request_text = if (parsed.value.text) |text|
        text
    else if (parsed.value.semantic_search) |semantic_search|
        semantic_search
    else
        "";

    var req = request.QueryRequest{
        .text = try alloc.dupe(u8, request_text),
        .fields = requested_fields,
        .filter_prefix = if (parsed.value.filter_prefix) |value| try alloc.dupe(u8, value) else null,
        .vector = if (parsed.value.vector) |vector| try alloc.dupe(f32, vector) else null,
        .sparse = if (parsed.value.sparse) |sparse| try cloneSparseTermsAlloc(alloc, sparse) else null,
        .semantic_search = if (parsed.value.semantic_search) |value| try alloc.dupe(u8, value) else null,
        .embedding_template = if (parsed.value.embedding_template) |value| try alloc.dupe(u8, value) else null,
        .indexes = if (parsed.value.indexes) |indexes| try cloneIndexNamesAlloc(alloc, indexes) else null,
        .count_only = parsed.value.count orelse false,
        .limit = parsed.value.limit orelse 10,
        .offset = parsed.value.offset orelse 0,
        .min_score = parsed.value.min_score orelse 0,
        .num_probes = parsed.value.num_probes orelse 2,
        .search_effort = parsed.value.search_effort,
        .mode = default_mode,
        .operator = parsed.value.operator orelse .all_terms,
        .fusion_strategy = parsed.value.fusion_strategy orelse .weighted_rrf,
        .text_weight = parsed.value.text_weight orelse 0.5,
        .vector_weight = parsed.value.vector_weight orelse 0.5,
        .sparse_weight = parsed.value.sparse_weight orelse 0.5,
    };
    errdefer req.deinit(alloc);
    requested_fields = null;

    if (req.embedding_template != null and req.semantic_search == null) return error.InvalidQueryRequest;
    if (req.semantic_search != null and req.vector != null) return error.InvalidQueryRequest;

    try validateSearchRequest(req);

    var sources = try resolveSearchSources(req, published_search_sources);
    _ = &sources;
    return .{
        .request = req,
        .sources = sources,
    };
}

fn validateSearchRequest(req: request.QueryRequest) !void {
    switch (req.mode) {
        .text => {
            if (std.mem.trim(u8, req.text, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
        },
        .vector => {
            if ((req.vector == null or req.vector.?.len == 0) and req.semantic_search == null) return error.InvalidQueryRequest;
        },
        .hybrid => {
            var mode_count: u8 = 0;
            if (std.mem.trim(u8, req.text, &std.ascii.whitespace).len != 0) mode_count += 1;
            if ((req.vector != null and req.vector.?.len != 0) or req.semantic_search != null) mode_count += 1;
            if (req.sparse != null and req.sparse.?.len != 0) mode_count += 1;
            if (mode_count < 2) return error.InvalidQueryRequest;
        },
        .sparse => {
            if (req.sparse == null or req.sparse.?.len == 0) return error.InvalidQueryRequest;
        },
    }
}

pub fn parseGraphNeighborsPlanAlloc(alloc: Allocator, body: []const u8) !request.GraphNeighborsRequest {
    try rejectForbiddenDocIdentityControlFieldsAlloc(alloc, body);
    const Parsed = struct {
        index_name: ?[]const u8 = null,
        doc_id: []const u8,
        direction: ?request.GraphQueryDirection = null,
        edge_type: ?[]const u8 = null,
        edge_types: ?[]const []const u8 = null,
        limit: ?usize = null,
    };
    var parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (std.mem.trim(u8, parsed.value.doc_id, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
    return .{
        .index_name = if (parsed.value.index_name) |value| try alloc.dupe(u8, value) else &.{},
        .doc_id = try alloc.dupe(u8, parsed.value.doc_id),
        .direction = parsed.value.direction orelse .out,
        .edge_types = try parseEdgeTypesAlloc(alloc, parsed.value.edge_type, parsed.value.edge_types),
        .limit = parsed.value.limit orelse 100,
    };
}

pub fn parseGraphTraversePlanAlloc(alloc: Allocator, body: []const u8) !request.GraphTraverseRequest {
    try rejectForbiddenDocIdentityControlFieldsAlloc(alloc, body);
    const Parsed = struct {
        index_name: ?[]const u8 = null,
        start_doc_id: []const u8,
        direction: ?request.GraphQueryDirection = null,
        edge_type: ?[]const u8 = null,
        edge_types: ?[]const []const u8 = null,
        max_depth: ?u32 = null,
        limit: ?usize = null,
        include_start: ?bool = null,
    };
    var parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (std.mem.trim(u8, parsed.value.start_doc_id, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
    return .{
        .index_name = if (parsed.value.index_name) |value| try alloc.dupe(u8, value) else &.{},
        .start_doc_id = try alloc.dupe(u8, parsed.value.start_doc_id),
        .direction = parsed.value.direction orelse .out,
        .edge_types = try parseEdgeTypesAlloc(alloc, parsed.value.edge_type, parsed.value.edge_types),
        .max_depth = parsed.value.max_depth orelse 3,
        .limit = parsed.value.limit orelse 100,
        .include_start = parsed.value.include_start orelse false,
    };
}

pub fn parseGraphShortestPathPlanAlloc(alloc: Allocator, body: []const u8) !request.GraphShortestPathRequest {
    try rejectForbiddenDocIdentityControlFieldsAlloc(alloc, body);
    const Parsed = struct {
        index_name: ?[]const u8 = null,
        start_doc_id: []const u8,
        end_doc_id: []const u8,
        direction: ?request.GraphQueryDirection = null,
        edge_type: ?[]const u8 = null,
        edge_types: ?[]const []const u8 = null,
        max_depth: ?u32 = null,
    };
    var parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (std.mem.trim(u8, parsed.value.start_doc_id, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
    if (std.mem.trim(u8, parsed.value.end_doc_id, &std.ascii.whitespace).len == 0) return error.InvalidQueryRequest;
    return .{
        .index_name = if (parsed.value.index_name) |value| try alloc.dupe(u8, value) else &.{},
        .start_doc_id = try alloc.dupe(u8, parsed.value.start_doc_id),
        .end_doc_id = try alloc.dupe(u8, parsed.value.end_doc_id),
        .direction = parsed.value.direction orelse .out,
        .edge_types = try parseEdgeTypesAlloc(alloc, parsed.value.edge_type, parsed.value.edge_types),
        .max_depth = parsed.value.max_depth orelse 6,
    };
}

fn parseEdgeTypesAlloc(
    alloc: Allocator,
    single: ?[]const u8,
    many: ?[]const []const u8,
) !?[][]u8 {
    if (many) |values| {
        const out = try alloc.alloc([]u8, values.len);
        errdefer alloc.free(out);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |edge_type| alloc.free(edge_type);
        }
        for (values, 0..) |value, idx| {
            out[idx] = try alloc.dupe(u8, value);
            initialized += 1;
        }
        return out;
    }
    if (single) |value| {
        const out = try alloc.alloc([]u8, 1);
        errdefer alloc.free(out);
        out[0] = try alloc.dupe(u8, value);
        return out;
    }
    return null;
}

fn resolveSearchSources(
    req: request.QueryRequest,
    published_search_sources: search_sources.PublishedSearchSources,
) !search_sources.ResolvedSearchSources {
    const needs_text =
        (req.mode == .text or req.mode == .hybrid) and
        std.mem.trim(u8, req.text, &std.ascii.whitespace).len != 0;
    const needs_vector =
        (req.mode == .vector or req.mode == .hybrid) and
        ((req.vector != null and req.vector.?.len != 0) or req.semantic_search != null);
    const needs_sparse =
        (req.mode == .sparse or req.mode == .hybrid) and
        (req.sparse != null and req.sparse.?.len != 0);
    var resolved = try published_search_sources.resolveRequested(req.indexes, needs_vector, needs_sparse);
    if (req.semantic_search != null and resolved.findVector() == null) {
        const fallback = published_search_sources.findVector() orelse return error.InvalidQueryRequest;
        try resolved.append(.{ .vector = fallback });
    }
    if (needs_text and resolved.findText() == null) {
        if (published_search_sources.findText()) |fallback| {
            try resolved.append(.{ .text = fallback });
        }
    }
    return resolved;
}

fn sparseEmbeddingTermsAlloc(
    alloc: Allocator,
    sparse: public_embedding_query_mod.SparseEmbeddingQuery,
) ![]request.SparseTermWeight {
    const out = try alloc.alloc(request.SparseTermWeight, sparse.indices.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*weight| weight.deinit(alloc);
    }
    for (sparse.indices, sparse.values, 0..) |index, value, idx| {
        out[idx] = .{
            .term = try std.fmt.allocPrint(alloc, "{d}", .{index}),
            .weight = value,
        };
        initialized += 1;
    }
    return out;
}

fn cloneIndexNamesAlloc(alloc: Allocator, indexes: []const []const u8) ![][]u8 {
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

fn cloneSparseTermsAlloc(alloc: Allocator, terms: []const request.SparseTermWeight) ![]request.SparseTermWeight {
    const out = try alloc.alloc(request.SparseTermWeight, terms.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*term| term.deinit(alloc);
    }
    for (terms, 0..) |term, idx| {
        out[idx] = .{
            .term = try alloc.dupe(u8, term.term),
            .weight = term.weight,
        };
        initialized += 1;
    }
    return out;
}

test "search plan defaults semantic-only requests to vector mode and resolves default vector source" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(alloc, "{\"semantic_search\":\"cat\"}", search_sources.defaultPublishedSearchSources());
    defer plan.deinit(alloc);
    try std.testing.expectEqual(request.QueryMode.vector, plan.request.mode);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, plan.vectorSource().?.index_name);
}

test "search plan accepts public dense embeddings map" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0]},\"limit\":5}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqual(request.QueryMode.vector, plan.request.mode);
    try std.testing.expectEqual(@as(usize, 3), plan.request.vector.?.len);
    try std.testing.expectEqual(@as(usize, 1), plan.request.indexes.?.len);
    try std.testing.expectEqualStrings("serverless_chunk", plan.request.indexes.?[0]);
    try std.testing.expectEqualStrings("serverless_chunk", plan.vectorSource().?.index_name);
}

test "search plan rejects internal doc identity controls" {
    const alloc = std.testing.allocator;
    const sources = search_sources.defaultPublishedSearchSources();

    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0]},\"identity_read_generation\":7}",
        sources,
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0]},\"allow_doc_identity_reassignment\":true}",
        sources,
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(
        alloc,
        "{\"text\":\"alpha\",\"_identity_read_generation\":7}",
        sources,
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(
        alloc,
        "{\"text\":\"alpha\",\"native_doc_id_constraints\":{\"include_doc_ids\":[\"doc:a\"]}}",
        sources,
    ));
}

test "serverless graph plans reject internal doc identity controls" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.InvalidQueryRequest, parseGraphNeighborsPlanAlloc(
        alloc,
        "{\"doc_id\":\"doc:a\",\"identity_read_generation\":7}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphTraversePlanAlloc(
        alloc,
        "{\"start_doc_id\":\"doc:a\",\"allow_doc_identity_reassignment\":true}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphShortestPathPlanAlloc(
        alloc,
        "{\"start_doc_id\":\"doc:a\",\"end_doc_id\":\"doc:b\",\"native_doc_id_constraints\":{\"include_doc_ids\":[\"doc:a\"]}}",
    ));
}

test "search plan preserves public search effort" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0]},\"limit\":5,\"search_effort\":0.3}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expect(plan.request.search_effort != null);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), plan.request.search_effort.?, 0.0001);
}

test "search plan accepts public count flag" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"full_text_search\":{\"query\":\"body:alpha\"},\"count\":true}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expect(plan.request.count_only);
    try std.testing.expectEqual(request.QueryMode.text, plan.request.mode);
}

test "search plan accepts public sparse embeddings map" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_sparse\":{\"indices\":[1,2],\"values\":[0.5,1.0]}},\"limit\":5}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqual(request.QueryMode.sparse, plan.request.mode);
    try std.testing.expectEqual(@as(usize, 2), plan.request.sparse.?.len);
    try std.testing.expectEqualStrings("1", plan.request.sparse.?[0].term);
    try std.testing.expectEqual(@as(f32, 0.5), plan.request.sparse.?[0].weight);
    try std.testing.expectEqualStrings("2", plan.request.sparse.?[1].term);
    try std.testing.expectEqual(@as(usize, 1), plan.request.indexes.?.len);
    try std.testing.expectEqualStrings("serverless_sparse", plan.request.indexes.?[0]);
    try std.testing.expectEqualStrings("serverless_sparse", plan.sparseSource().?.index_name);
}

test "search plan accepts public dense and sparse embeddings map together" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0],\"serverless_sparse\":{\"indices\":[1,2],\"values\":[0.5,1.0]}},\"limit\":5}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqual(request.QueryMode.hybrid, plan.request.mode);
    try std.testing.expectEqual(@as(usize, 3), plan.request.vector.?.len);
    try std.testing.expectEqual(@as(usize, 2), plan.request.sparse.?.len);
    try std.testing.expectEqual(@as(usize, 2), plan.request.indexes.?.len);
}

test "search plan rejects invalid hybrid requests with fewer than two lanes" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidQueryRequest, parseSearchPlanAlloc(alloc, "{\"mode\":\"hybrid\",\"text\":\"only-text\"}", search_sources.defaultPublishedSearchSources()));
}

test "search plan reports active lanes" {
    const alloc = std.testing.allocator;

    var semantic = try parseSearchPlanAlloc(alloc, "{\"semantic_search\":\"cat\"}", search_sources.defaultPublishedSearchSources());
    defer semantic.deinit(alloc);
    try std.testing.expect(!semantic.usesTextLane());
    try std.testing.expect(semantic.usesVectorLane());
    try std.testing.expect(!semantic.usesSparseLane());

    var hybrid = try parseSearchPlanAlloc(
        alloc,
        "{\"semantic_search\":\"cat\",\"sparse\":[{\"term\":\"fur\",\"weight\":1.0}],\"indexes\":[\"serverless_chunk\",\"serverless_sparse\"]}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer hybrid.deinit(alloc);
    try std.testing.expect(hybrid.usesTextLane());
    try std.testing.expect(hybrid.usesVectorLane());
    try std.testing.expect(hybrid.usesSparseLane());
}

test "search plan accepts public full text query strings for supported fields" {
    const alloc = std.testing.allocator;

    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"full_text_search\":{\"query\":\"body:alpha OR body:bravo\"},\"limit\":5}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqual(request.QueryMode.text, plan.request.mode);
    try std.testing.expectEqual(request.QueryOperator.any_terms, plan.request.operator);
    try std.testing.expectEqualStrings("alpha bravo", plan.request.text);
    try std.testing.expectEqual(@as(usize, 5), plan.request.limit);
}

test "search plan rejects unsupported public full text fields" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        parseSearchPlanAlloc(
            alloc,
            "{\"full_text_search\":{\"query\":\"title:alpha\"}}",
            search_sources.defaultPublishedSearchSources(),
        ),
    );
}

test "search plan accepts public filter and exclusion text subset" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"full_text_search\":{\"query\":\"body:alpha OR body:bravo\"},\"filter_query\":{\"match\":{\"field\":\"body\",\"text\":\"alpha\"}},\"exclusion_query\":{\"prefix\":{\"field\":\"body\",\"text\":\"bra\"}},\"limit\":5}",
        .{
            .items = &.{},
        },
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("alpha bravo", plan.request.text);
    try std.testing.expectEqualStrings("alpha", plan.request.filter_text.?);
    try std.testing.expectEqualStrings("bra", plan.request.exclusion_text.?);
    try std.testing.expectEqual(request.QueryOperator.any_terms, plan.request.operator);
    try std.testing.expectEqual(request.QueryOperator.all_terms, plan.request.filter_operator);
    try std.testing.expectEqual(request.QueryOperator.prefix_any_term, plan.request.exclusion_operator);
}

test "search plan accepts basic public fields projection" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"full_text_search\":{\"query\":\"body:alpha\"},\"fields\":[\"title\",\"metadata.author\"]}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), plan.request.fields.?.len);
    try std.testing.expectEqualStrings("title", plan.request.fields.?[0]);
    try std.testing.expectEqualStrings("metadata.author", plan.request.fields.?[1]);
}

test "search plan accepts legacy text with fields and count" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"text\":\"alpha\",\"fields\":[\"title\"],\"count\":true}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expect(plan.request.count_only);
    try std.testing.expectEqual(@as(usize, 1), plan.request.fields.?.len);
    try std.testing.expectEqualStrings("title", plan.request.fields.?[0]);
}

test "search plan preserves legacy search effort" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"vector\":[1.0,0.0,0.0],\"search_effort\":0.8}",
        search_sources.defaultPublishedSearchSources(),
    );
    defer plan.deinit(alloc);

    try std.testing.expect(plan.request.search_effort != null);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), plan.request.search_effort.?, 0.0001);
}

test "search plan rejects unsupported wildcard fields projection" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        parseSearchPlanAlloc(
            alloc,
            "{\"full_text_search\":{\"query\":\"body:alpha\"},\"fields\":[\"_chunks.*\"]}",
            search_sources.defaultPublishedSearchSources(),
        ),
    );
}

test "search plan carries filter prefix through public request" {
    const alloc = std.testing.allocator;
    var plan = try parseSearchPlanAlloc(
        alloc,
        "{\"text\":\"alpha\",\"filter_prefix\":\"tenant:docs:\",\"limit\":5}",
        .{
            .items = &.{},
        },
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("tenant:docs:", plan.request.filter_prefix.?);
}

test "search plan owns public filter prefix independent of body storage" {
    const alloc = std.testing.allocator;
    const body = try alloc.dupe(u8, "{\"embeddings\":{\"serverless_chunk\":[1.0,0.0,0.0]},\"filter_prefix\":\"tenant:docs:\",\"limit\":5}");
    defer alloc.free(body);

    var plan = try parseSearchPlanAlloc(alloc, body, search_sources.defaultPublishedSearchSources());
    defer plan.deinit(alloc);

    @memset(body, 'x');
    try std.testing.expectEqualStrings("tenant:docs:", plan.request.filter_prefix.?);
}

test "graph plans reject empty identifiers" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphNeighborsPlanAlloc(alloc, "{\"doc_id\":\"   \"}"));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphTraversePlanAlloc(alloc, "{\"start_doc_id\":\"\"}"));
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphShortestPathPlanAlloc(alloc, "{\"start_doc_id\":\"a\",\"end_doc_id\":\"  \"}"));
}
