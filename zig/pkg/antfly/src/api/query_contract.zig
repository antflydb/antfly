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
const db_mod = @import("../storage/db/mod.zig");
const document_query = @import("../storage/db/document_query.zig");
const graph_pattern_mod = @import("../graph/pattern.zig");
const graph_query_mod = @import("../graph/query.zig");
const fusion_mod = @import("../search/fusion.zig");
const aggregations_mod = @import("../storage/db/aggregations.zig");
const public_search_request_mod = @import("public_search_request.zig");
const public_text_query_mod = @import("public_text_query.zig");
const public_query_string_mod = @import("public_query_string.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const bleve_openapi = @import("antfly_bleve_query_openapi");
const reranking_mod = @import("antfly_reranking");
const vector_codec = @import("antfly_vector").codec;
const algebraic_ir = db_mod.algebraic.ir;
const algebraic_law = db_mod.algebraic.law;
const algebraic_lexical = db_mod.algebraic.lexical;

pub const QueryResponse = struct {
    json: []u8,

    pub fn deinit(self: *QueryResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const QueryResponseMeta = struct {
    pub const RerankerProfile = struct {
        model: []const u8 = "",
        documents_reranked: u32 = 0,
        duration_ms: i64 = 0,
    };

    pub const MergeProfile = struct {
        strategy: ?indexes_openapi.MergeStrategy = null,
        full_text_hits: u32 = 0,
        semantic_hits: u32 = 0,
        duration_ms: i64 = 0,
    };

    pub const DenseSearchProfile = struct {
        pub const DebugHit = struct {
            id: u64 = 0,
            distance: f32 = 0,
            error_bound: f32 = 0,
            lower_bound: f32 = 0,
            upper_bound: f32 = 0,
        };

        pub const DebugPair = struct {
            left: DebugHit = .{},
            right: DebugHit = .{},
            distance_gap: f32 = 0,
            interval_gap: f32 = 0,
            overlaps: bool = false,
        };

        total_ns: u64 = 0,
        index_lookup_ns: u64 = 0,
        hbc_search_ns: u64 = 0,
        hbc_runtime_txn_ns: u64 = 0,
        hbc_scratch_acquire_ns: u64 = 0,
        hbc_node_cache_lookup_ns: u64 = 0,
        hbc_quantized_cache_lookup_ns: u64 = 0,
        resolved_search_width: u32 = 0,
        resolved_epsilon: f32 = 0,
        hbc_nodes_visited: u64 = 0,
        hbc_leaves_explored: u64 = 0,
        hbc_approx_vectors_scored: u64 = 0,
        hbc_exact_vectors_scored: u64 = 0,
        hbc_reranked_vectors: u64 = 0,
        hbc_approx_candidate_count: u64 = 0,
        hbc_rerank_candidate_count: u64 = 0,
        hbc_ambiguous_top_k_pairs: u64 = 0,
        hbc_ambiguous_boundary_pairs: u64 = 0,
        hbc_ambiguous_distance_over_hits: u64 = 0,
        hbc_ambiguous_distance_under_hits: u64 = 0,
        hbc_full_rerank_due_to_threshold: bool = false,
        hbc_top_k_count: u64 = 0,
        hbc_min_distance_gap_top_k: f32 = 0,
        hbc_min_interval_gap_top_k: f32 = 0,
        hbc_closest_pair_top_k: ?DebugPair = null,
        hbc_boundary_pair: ?DebugPair = null,
        hbc_boundary_tail_error_avg: f32 = 0,
        hbc_boundary_tail_error_max: f32 = 0,
        hbc_boundary_tail_distance_gap_avg: f32 = 0,
        hbc_boundary_tail_distance_gap_min: f32 = 0,
        hbc_boundary_tail_distance_gap_max: f32 = 0,
        hbc_boundary_tail_interval_gap_avg: f32 = 0,
        hbc_boundary_tail_interval_gap_min: f32 = 0,
        hbc_boundary_tail_interval_gap_max: f32 = 0,
        hbc_approx_top_count: u64 = 0,
        hbc_approx_top: [5]DebugHit = .{ .{}, .{}, .{}, .{}, .{} },
        hbc_rerank_external_score_ns: u64 = 0,
        hbc_rerank_vector_load_ns: u64 = 0,
        hbc_rerank_metadata_lookup_ns: u64 = 0,
        hbc_rerank_artifact_key_ns: u64 = 0,
        hbc_rerank_artifact_read_ns: u64 = 0,
        hbc_rerank_artifact_decode_ns: u64 = 0,
        hbc_rerank_artifact_distance_ns: u64 = 0,
        hbc_rerank_lsm_cache_hits: u64 = 0,
        hbc_rerank_lsm_cache_misses: u64 = 0,
        hbc_rerank_distance_ns: u64 = 0,
        doc_key_resolve_ns: u64 = 0,
        load_projected_document_ns: u64 = 0,
        postprocess_ns: u64 = 0,
        raw_hit_count: u32 = 0,
        returned_hit_count: u32 = 0,
        inline_metadata_hits: u32 = 0,
        fetched_metadata_hits: u32 = 0,
        lookup_doc_key_hits: u32 = 0,
    };

    took_ms: i64 = 0,
    shard_count: u32 = 1,
    merged: bool = false,
    reranker: ?RerankerProfile = null,
    merge: ?MergeProfile = null,
    dense_search: ?DenseSearchProfile = null,
    aggregation_results: []aggregations_mod.SearchAggregationResult = &.{},

    pub fn deinit(self: *QueryResponseMeta, alloc: std.mem.Allocator) void {
        aggregations_mod.deinitResults(alloc, self.aggregation_results);
        self.* = undefined;
    }
};

fn appendJsonFieldName(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendJsonFieldString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

fn appendJsonFieldBool(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: bool,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

fn appendJsonFieldUsize(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: usize,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn freeOwnedStringItems(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
}

fn freeOwnedStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    freeOwnedStringItems(alloc, values);
    if (values.len > 0) alloc.free(@constCast(values));
}

pub const NativeDocIdConstraintEnvelope = struct {
    positive_filter: bool = false,
    include_doc_ids: []const []const u8 = &.{},
    exclude_doc_ids: []const []const u8 = &.{},

    pub fn hasConstraints(self: @This()) bool {
        return self.positive_filter or self.include_doc_ids.len > 0 or self.exclude_doc_ids.len > 0;
    }
};

pub const OwnedNativeDocIdConstraintEnvelope = struct {
    constraints: NativeDocIdConstraintEnvelope,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeOwnedStringSlice(alloc, self.constraints.include_doc_ids);
        freeOwnedStringSlice(alloc, self.constraints.exclude_doc_ids);
        self.* = undefined;
    }
};

pub const AlgebraicVectorWorkerRequestOptions = struct {
    fields: [][]const u8 = &.{},
    filter_prefix: []const u8 = "",
    filter_query_json: []const u8 = "",
    exclusion_query_json: []const u8 = "",
    filter_ids: []const u64 = &.{},
    exclude_ids: []const u64 = &.{},
    require_algebraic_filter_resolution: bool = false,
    include_all_fields: bool = true,
    defer_stored_projection: bool = false,
    limit: u32 = 10,
    offset: u32 = 0,
    count_only: bool = false,
    profile: bool = false,
    include_stored: bool = true,
    search_effort: ?f32 = null,
    distance_over: ?f32 = null,
    distance_under: ?f32 = null,
    return_mode: db_mod.types.ReturnMode = .parent,
    max_chunks_per_parent: u32 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.fields) |field| alloc.free(field);
        if (self.fields.len > 0) alloc.free(self.fields);
        if (self.filter_prefix.len > 0) alloc.free(@constCast(self.filter_prefix));
        if (self.filter_query_json.len > 0) alloc.free(@constCast(self.filter_query_json));
        if (self.exclusion_query_json.len > 0) alloc.free(@constCast(self.exclusion_query_json));
        if (self.filter_ids.len > 0) alloc.free(@constCast(self.filter_ids));
        if (self.exclude_ids.len > 0) alloc.free(@constCast(self.exclude_ids));
        self.* = undefined;
    }
};

pub const OwnedAlgebraicVectorWorkerRequestEnvelope = struct {
    index_name: []u8,
    layout: algebraic_ir.PhysicalLayout,
    query: OwnedAlgebraicVectorWorkerQuery,
    options: AlgebraicVectorWorkerRequestOptions = .{},
    native_doc_id_constraints: OwnedNativeDocIdConstraintEnvelope,
    tensor_access_paths: []OwnedAlgebraicTensorAccessPathEnvelope,
    tensor_program: OwnedAlgebraicTensorProgramEnvelope,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        self.query.deinit(alloc);
        self.options.deinit(alloc);
        self.native_doc_id_constraints.deinit(alloc);
        for (self.tensor_access_paths) |*path| path.deinit(alloc);
        if (self.tensor_access_paths.len > 0) alloc.free(self.tensor_access_paths);
        self.tensor_program.deinit(alloc);
        self.* = undefined;
    }

    pub fn proveTensorProgramAlloc(self: *const @This(), alloc: std.mem.Allocator) !algebraic_ir.TensorProgramProof {
        var paths = try alloc.alloc(algebraic_ir.PhysicalAccessPath, self.tensor_access_paths.len);
        defer if (paths.len > 0) alloc.free(paths);
        var found_target_path = false;
        for (self.tensor_access_paths, 0..) |*path, i| {
            paths[i] = path.asAccessPath();
            if (paths[i].layout == self.layout and
                std.mem.eql(u8, paths[i].owner, self.index_name) and
                algebraic_ir.accessPathCanSatisfy(paths[i], .{
                    .fragment = .vector_search,
                    .output_dims = &.{ .doc, .score },
                    .owner = self.index_name,
                    .layout = self.layout,
                }).safe())
            {
                found_target_path = true;
            }
        }
        if (!found_target_path) return error.InvalidQueryRequest;

        var view = try self.tensor_program.asProgramAlloc(alloc);
        defer view.deinit(alloc);
        if (!algebraic_ir.vectorSearchProgramMatchesTarget(
            view.program,
            self.index_name,
            self.layout,
            self.native_doc_id_constraints.constraints.hasConstraints(),
        )) return error.InvalidQueryRequest;
        return try algebraic_ir.tensorProgramProof(alloc, paths, view.program);
    }
};

pub const AlgebraicVectorWorkerQuery = union(enum) {
    dense: db_mod.types.DenseKnnQuery,
    sparse: db_mod.types.SparseKnnQuery,
};

pub const OwnedAlgebraicVectorWorkerQuery = union(enum) {
    dense: db_mod.types.DenseKnnQuery,
    sparse: db_mod.types.SparseKnnQuery,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .dense => |query| if (query.vector.len > 0) alloc.free(@constCast(query.vector)),
            .sparse => |query| {
                if (query.indices.len > 0) alloc.free(@constCast(query.indices));
                if (query.values.len > 0) alloc.free(@constCast(query.values));
            },
        }
        self.* = undefined;
    }
};

pub const AlgebraicTensorAccessPathEnvelopeInput = struct {
    owner: []const u8,
    layout: []const u8,
    dictionary: ?AlgebraicDictionaryIdentityInput = null,
    fragments: []const []const u8 = &.{},
    output_dims: []const []const u8 = &.{},
    law_ids: []const []const u8 = &.{},
};

pub const AlgebraicDictionaryIdentityInput = struct {
    scope: []const u8,
    field_or_path: []const u8,
    label_kind: []const u8,
    analyzer_or_canonicalization: []const u8,
    value_kind: []const u8,
    coercion_policy: []const u8,
};

pub const AlgebraicTensorExprEnvelopeInput = struct {
    expr_id: ?[]const u8 = null,
    fragment: []const u8,
    input_dims: []const []const u8 = &.{},
    output_dims: []const []const u8 = &.{},
    semantic_id: ?[]const u8 = null,
    owner: ?[]const u8 = null,
    layout: ?[]const u8 = null,
    dictionary: ?AlgebraicDictionaryIdentityInput = null,
    law_id: ?[]const u8 = null,
    metadata: ?[]const u8 = null,
};

pub const AlgebraicTensorProgramRefInput = struct {
    kind: []const u8,
    index: usize,
};

pub const AlgebraicTensorProgramStepEnvelopeInput = struct {
    expr: AlgebraicTensorExprEnvelopeInput,
    inputs: []const AlgebraicTensorProgramRefInput = &.{},
};

pub const AlgebraicTensorProgramEnvelopeInput = struct {
    program_id: ?[]const u8 = null,
    inputs: []const AlgebraicTensorExprEnvelopeInput = &.{},
    steps: []const AlgebraicTensorProgramStepEnvelopeInput = &.{},
    output: AlgebraicTensorProgramRefInput,
    outputs: []const AlgebraicTensorProgramRefInput = &.{},
};

pub const OwnedAlgebraicTensorAccessPathEnvelope = struct {
    owner: []u8,
    layout: algebraic_ir.PhysicalLayout,
    dictionary: ?OwnedAlgebraicDictionaryIdentity = null,
    fragments: []algebraic_ir.TensorFragment,
    output_dims: []algebraic_ir.Dimension,
    law_ids: []algebraic_law.Id,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.owner);
        if (self.dictionary) |*dictionary| dictionary.deinit(alloc);
        if (self.fragments.len > 0) alloc.free(self.fragments);
        if (self.output_dims.len > 0) alloc.free(self.output_dims);
        if (self.law_ids.len > 0) alloc.free(self.law_ids);
        self.* = undefined;
    }

    pub fn asAccessPath(self: *const @This()) algebraic_ir.PhysicalAccessPath {
        const dictionary = if (self.dictionary) |*value| value.asIdentity() else null;
        return .{
            .owner = self.owner,
            .layout = self.layout,
            .dictionary = dictionary,
            .fragments = self.fragments,
            .output_dims = self.output_dims,
            .law_ids = self.law_ids,
        };
    }
};

pub const OwnedAlgebraicDictionaryIdentity = struct {
    scope: []u8,
    field_or_path: []u8,
    label_kind: algebraic_lexical.LabelKind,
    analyzer_or_canonicalization: []u8,
    value_kind: []u8,
    coercion_policy: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.scope);
        alloc.free(self.field_or_path);
        alloc.free(self.analyzer_or_canonicalization);
        alloc.free(self.value_kind);
        alloc.free(self.coercion_policy);
        self.* = undefined;
    }

    pub fn asIdentity(self: *const @This()) algebraic_lexical.DictionaryIdentity {
        return .{
            .scope = self.scope,
            .field_or_path = self.field_or_path,
            .label_kind = self.label_kind,
            .analyzer_or_canonicalization = self.analyzer_or_canonicalization,
            .value_kind = self.value_kind,
            .coercion_policy = self.coercion_policy,
        };
    }
};

pub const OwnedAlgebraicTensorExprEnvelope = struct {
    expr_id: []u8,
    fragment: algebraic_ir.TensorFragment,
    input_dims: []algebraic_ir.Dimension,
    output_dims: []algebraic_ir.Dimension,
    semantic_id: ?[]u8,
    owner: ?[]u8,
    layout: ?algebraic_ir.PhysicalLayout,
    dictionary: ?OwnedAlgebraicDictionaryIdentity,
    law_id: ?algebraic_law.Id,
    metadata: ?[]u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.expr_id);
        if (self.input_dims.len > 0) alloc.free(self.input_dims);
        if (self.output_dims.len > 0) alloc.free(self.output_dims);
        if (self.semantic_id) |value| alloc.free(value);
        if (self.owner) |value| alloc.free(value);
        if (self.metadata) |value| alloc.free(value);
        if (self.dictionary) |*value| value.deinit(alloc);
        self.* = undefined;
    }

    pub fn asExpr(self: *const @This()) algebraic_ir.TensorExpr {
        const dictionary = if (self.dictionary) |*value| value.asIdentity() else null;
        return .{
            .fragment = self.fragment,
            .input_dims = self.input_dims,
            .output_dims = self.output_dims,
            .semantic_id = self.semantic_id,
            .owner = self.owner,
            .layout = self.layout,
            .dictionary = dictionary,
            .law_id = self.law_id,
            .metadata = self.metadata,
        };
    }
};

pub const OwnedAlgebraicTensorProgramStepEnvelope = struct {
    expr: OwnedAlgebraicTensorExprEnvelope,
    inputs: []algebraic_ir.TensorProgramRef,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.expr.deinit(alloc);
        if (self.inputs.len > 0) alloc.free(self.inputs);
        self.* = undefined;
    }
};

pub const OwnedAlgebraicTensorProgramEnvelope = struct {
    program_id: []u8,
    inputs: []OwnedAlgebraicTensorExprEnvelope,
    steps: []OwnedAlgebraicTensorProgramStepEnvelope,
    output: algebraic_ir.TensorProgramRef,
    outputs: []algebraic_ir.TensorProgramRef,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.program_id);
        for (self.inputs) |*input| input.deinit(alloc);
        if (self.inputs.len > 0) alloc.free(self.inputs);
        for (self.steps) |*step| step.deinit(alloc);
        if (self.steps.len > 0) alloc.free(self.steps);
        if (self.outputs.len > 0) alloc.free(self.outputs);
        self.* = undefined;
    }

    pub fn asProgramAlloc(self: *const @This(), alloc: std.mem.Allocator) !OwnedAlgebraicTensorProgramView {
        const inputs = try alloc.alloc(algebraic_ir.TensorExpr, self.inputs.len);
        errdefer if (inputs.len > 0) alloc.free(inputs);
        for (self.inputs, 0..) |*input, i| inputs[i] = input.asExpr();
        const steps = try alloc.alloc(algebraic_ir.TensorProgramStep, self.steps.len);
        errdefer if (steps.len > 0) alloc.free(steps);
        for (self.steps, 0..) |*step, i| {
            steps[i] = .{
                .expr = step.expr.asExpr(),
                .inputs = step.inputs,
            };
        }
        return .{
            .program = .{
                .inputs = inputs,
                .steps = steps,
                .output = self.output,
                .outputs = self.outputs,
            },
            .input_exprs = inputs,
            .steps = steps,
        };
    }
};

pub const OwnedAlgebraicTensorProgramView = struct {
    program: algebraic_ir.TensorProgram,
    input_exprs: []algebraic_ir.TensorExpr,
    steps: []algebraic_ir.TensorProgramStep,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.input_exprs.len > 0) alloc.free(self.input_exprs);
        if (self.steps.len > 0) alloc.free(self.steps);
        self.* = undefined;
    }
};

pub fn nativeDocIdConstraintEnvelopeFromSearchRequest(req: db_mod.types.SearchRequest) NativeDocIdConstraintEnvelope {
    return .{
        .positive_filter = req.filter_doc_ids_positive,
        .include_doc_ids = req.filter_doc_ids,
        .exclude_doc_ids = req.exclude_doc_ids,
    };
}

pub fn encodeAlgebraicTensorAccessPathEnvelopeAlloc(
    alloc: std.mem.Allocator,
    access_path: algebraic_ir.PhysicalAccessPath,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "owner", access_path.owner);
    try appendJsonFieldString(alloc, &out, &first, "layout", @tagName(access_path.layout));
    if (access_path.dictionary) |dictionary| try appendAlgebraicDictionaryIdentity(alloc, &out, &first, dictionary);
    try appendJsonFieldName(alloc, &out, &first, "fragments");
    try appendEnumNameArray(alloc, &out, algebraic_ir.TensorFragment, access_path.fragments);
    try appendJsonFieldName(alloc, &out, &first, "output_dims");
    try appendEnumNameArray(alloc, &out, algebraic_ir.Dimension, access_path.output_dims);
    try appendJsonFieldName(alloc, &out, &first, "law_ids");
    try appendEnumNameArray(alloc, &out, algebraic_law.Id, access_path.law_ids);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendAlgebraicDictionaryIdentity(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    dictionary: algebraic_lexical.DictionaryIdentity,
) !void {
    try appendJsonFieldName(alloc, out, first, "dictionary");
    try out.append(alloc, '{');
    var dictionary_first = true;
    try appendJsonFieldString(alloc, out, &dictionary_first, "scope", dictionary.scope);
    try appendJsonFieldString(alloc, out, &dictionary_first, "field_or_path", dictionary.field_or_path);
    try appendJsonFieldString(alloc, out, &dictionary_first, "label_kind", @tagName(dictionary.label_kind));
    try appendJsonFieldString(alloc, out, &dictionary_first, "analyzer_or_canonicalization", dictionary.analyzer_or_canonicalization);
    try appendJsonFieldString(alloc, out, &dictionary_first, "value_kind", dictionary.value_kind);
    try appendJsonFieldString(alloc, out, &dictionary_first, "coercion_policy", dictionary.coercion_policy);
    try out.append(alloc, '}');
}

pub fn parseAlgebraicTensorAccessPathEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicTensorAccessPathEnvelope {
    var parsed = std.json.parseFromSlice(AlgebraicTensorAccessPathEnvelopeInput, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try parseAlgebraicTensorAccessPathEnvelopeInputAlloc(alloc, parsed.value);
}

pub fn encodeAlgebraicTensorExprEnvelopeAlloc(
    alloc: std.mem.Allocator,
    expr: algebraic_ir.TensorExpr,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    const expr_id = try algebraic_ir.tensorExprIdAlloc(alloc, expr);
    defer alloc.free(expr_id);
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "expr_id", expr_id);
    try appendJsonFieldString(alloc, &out, &first, "fragment", @tagName(expr.fragment));
    try appendJsonFieldName(alloc, &out, &first, "input_dims");
    try appendEnumNameArray(alloc, &out, algebraic_ir.Dimension, expr.input_dims);
    try appendJsonFieldName(alloc, &out, &first, "output_dims");
    try appendEnumNameArray(alloc, &out, algebraic_ir.Dimension, expr.output_dims);
    if (expr.semantic_id) |semantic_id| try appendJsonFieldString(alloc, &out, &first, "semantic_id", semantic_id);
    if (expr.owner) |owner| try appendJsonFieldString(alloc, &out, &first, "owner", owner);
    if (expr.layout) |layout| try appendJsonFieldString(alloc, &out, &first, "layout", @tagName(layout));
    if (expr.dictionary) |dictionary| try appendAlgebraicDictionaryIdentity(alloc, &out, &first, dictionary);
    if (expr.law_id) |law_id| try appendJsonFieldString(alloc, &out, &first, "law_id", @tagName(law_id));
    if (expr.metadata) |metadata| try appendJsonFieldString(alloc, &out, &first, "metadata", metadata);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeAlgebraicTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    program: algebraic_ir.TensorProgram,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    const program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, program);
    defer alloc.free(program_id);
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "program_id", program_id);
    try appendJsonFieldName(alloc, &out, &first, "inputs");
    try out.append(alloc, '[');
    for (program.inputs, 0..) |expr, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeAlgebraicTensorExprEnvelopeAlloc(alloc, expr);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "steps");
    try out.append(alloc, '[');
    for (program.steps, 0..) |step, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendAlgebraicTensorProgramStep(alloc, &out, step);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "output");
    try appendAlgebraicTensorProgramRef(alloc, &out, program.output);
    try appendJsonFieldName(alloc, &out, &first, "outputs");
    try out.append(alloc, '[');
    for (program.outputs, 0..) |output_ref, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendAlgebraicTensorProgramRef(alloc, &out, output_ref);
    }
    try out.append(alloc, ']');
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendAlgebraicTensorProgramStep(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    step: algebraic_ir.TensorProgramStep,
) !void {
    try out.append(alloc, '{');
    var first = true;
    try appendJsonFieldName(alloc, out, &first, "expr");
    const encoded = try encodeAlgebraicTensorExprEnvelopeAlloc(alloc, step.expr);
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
    try appendJsonFieldName(alloc, out, &first, "inputs");
    try out.append(alloc, '[');
    for (step.inputs, 0..) |input_ref, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendAlgebraicTensorProgramRef(alloc, out, input_ref);
    }
    try out.append(alloc, ']');
    try out.append(alloc, '}');
}

fn appendAlgebraicTensorProgramRef(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    input_ref: algebraic_ir.TensorProgramRef,
) !void {
    try out.append(alloc, '{');
    var first = true;
    switch (input_ref) {
        .input => |idx| {
            try appendJsonFieldString(alloc, out, &first, "kind", "input");
            try appendJsonFieldUsize(alloc, out, &first, "index", idx);
        },
        .step => |idx| {
            try appendJsonFieldString(alloc, out, &first, "kind", "step");
            try appendJsonFieldUsize(alloc, out, &first, "index", idx);
        },
    }
    try out.append(alloc, '}');
}

pub fn parseAlgebraicTensorExprEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicTensorExprEnvelope {
    var parsed = std.json.parseFromSlice(AlgebraicTensorExprEnvelopeInput, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, parsed.value);
}

pub fn parseAlgebraicTensorExprEnvelopeInputAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorExprEnvelopeInput,
) !OwnedAlgebraicTensorExprEnvelope {
    const fragment = std.meta.stringToEnum(algebraic_ir.TensorFragment, input.fragment) orelse return error.InvalidQueryRequest;
    const input_dims = try parseAlgebraicDimensionArrayAlloc(alloc, input.input_dims);
    errdefer if (input_dims.len > 0) alloc.free(input_dims);
    const output_dims = try parseAlgebraicDimensionArrayAlloc(alloc, input.output_dims);
    errdefer if (output_dims.len > 0) alloc.free(output_dims);
    const semantic_id = if (input.semantic_id) |value| try alloc.dupe(u8, value) else null;
    errdefer if (semantic_id) |value| alloc.free(value);
    const owner = if (input.owner) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owner) |value| alloc.free(value);
    const layout = if (input.layout) |value| std.meta.stringToEnum(algebraic_ir.PhysicalLayout, value) orelse return error.InvalidQueryRequest else null;
    var dictionary = if (input.dictionary) |value| try parseAlgebraicDictionaryIdentityAlloc(alloc, value) else null;
    errdefer if (dictionary) |*value| value.deinit(alloc);
    const law_id = if (input.law_id) |value| algebraic_law.Id.parse(value) orelse return error.InvalidQueryRequest else null;
    const metadata = if (input.metadata) |value| try alloc.dupe(u8, value) else null;
    errdefer if (metadata) |value| alloc.free(value);
    const expr_for_id = algebraic_ir.TensorExpr{
        .fragment = fragment,
        .input_dims = input_dims,
        .output_dims = output_dims,
        .semantic_id = semantic_id,
        .owner = owner,
        .layout = layout,
        .dictionary = if (dictionary) |*value| value.asIdentity() else null,
        .law_id = law_id,
        .metadata = metadata,
    };
    const expr_id = try algebraic_ir.tensorExprIdAlloc(alloc, expr_for_id);
    errdefer alloc.free(expr_id);
    const claimed_id = input.expr_id orelse return error.InvalidQueryRequest;
    if (!std.mem.eql(u8, expr_id, claimed_id)) return error.InvalidQueryRequest;

    return .{
        .expr_id = expr_id,
        .fragment = fragment,
        .input_dims = input_dims,
        .output_dims = output_dims,
        .semantic_id = semantic_id,
        .owner = owner,
        .layout = layout,
        .dictionary = dictionary,
        .law_id = law_id,
        .metadata = metadata,
    };
}

pub fn parseAlgebraicTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicTensorProgramEnvelope {
    var parsed = std.json.parseFromSlice(AlgebraicTensorProgramEnvelopeInput, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try parseAlgebraicTensorProgramEnvelopeInputAlloc(alloc, parsed.value);
}

pub fn parseAlgebraicTensorProgramEnvelopeInputAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorProgramEnvelopeInput,
) !OwnedAlgebraicTensorProgramEnvelope {
    const inputs = try alloc.alloc(OwnedAlgebraicTensorExprEnvelope, input.inputs.len);
    var inputs_initialized: usize = 0;
    errdefer {
        for (inputs[0..inputs_initialized]) |*owned| owned.deinit(alloc);
        if (inputs.len > 0) alloc.free(inputs);
    }
    for (input.inputs, 0..) |expr_input, i| {
        inputs[i] = try parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, expr_input);
        inputs_initialized += 1;
    }

    const steps = try alloc.alloc(OwnedAlgebraicTensorProgramStepEnvelope, input.steps.len);
    var steps_initialized: usize = 0;
    errdefer {
        for (steps[0..steps_initialized]) |*step| step.deinit(alloc);
        if (steps.len > 0) alloc.free(steps);
    }
    for (input.steps, 0..) |step_input, i| {
        var expr = try parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, step_input.expr);
        var expr_moved = false;
        errdefer if (!expr_moved) expr.deinit(alloc);
        const refs = try parseAlgebraicTensorProgramRefsAlloc(alloc, step_input.inputs);
        var refs_moved = false;
        errdefer if (!refs_moved and refs.len > 0) alloc.free(refs);
        steps[i] = .{
            .expr = expr,
            .inputs = refs,
        };
        expr_moved = true;
        refs_moved = true;
        steps_initialized += 1;
    }

    const output = parseAlgebraicTensorProgramRef(input.output) orelse return error.InvalidQueryRequest;
    const outputs = try parseAlgebraicTensorProgramRefsAlloc(alloc, input.outputs);
    errdefer if (outputs.len > 0) alloc.free(outputs);
    var envelope = OwnedAlgebraicTensorProgramEnvelope{
        .program_id = undefined,
        .inputs = inputs,
        .steps = steps,
        .output = output,
        .outputs = outputs,
    };
    try validateAlgebraicTensorProgramEnvelopeStructure(envelope);
    var view = try envelope.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, view.program);
    errdefer alloc.free(program_id);
    if (input.program_id) |claimed_id| {
        if (!std.mem.eql(u8, program_id, claimed_id)) return error.InvalidQueryRequest;
    }
    envelope.program_id = program_id;
    return envelope;
}

fn validateAlgebraicTensorProgramEnvelopeStructure(envelope: OwnedAlgebraicTensorProgramEnvelope) !void {
    try validateAlgebraicTensorProgramOutputRef(envelope, envelope.output);
    for (envelope.outputs) |output_ref| try validateAlgebraicTensorProgramOutputRef(envelope, output_ref);
    for (envelope.steps, 0..) |step, step_index| {
        for (step.inputs) |input_ref| {
            switch (input_ref) {
                .input => |idx| if (idx >= envelope.inputs.len) return error.InvalidQueryRequest,
                .step => |idx| if (idx >= step_index) return error.InvalidQueryRequest,
            }
        }
    }
}

fn validateAlgebraicTensorProgramOutputRef(
    envelope: OwnedAlgebraicTensorProgramEnvelope,
    ref: algebraic_ir.TensorProgramRef,
) !void {
    switch (ref) {
        .input => |idx| if (idx >= envelope.inputs.len) return error.InvalidQueryRequest,
        .step => |idx| if (idx >= envelope.steps.len) return error.InvalidQueryRequest,
    }
}

pub fn encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    layout: algebraic_ir.PhysicalLayout,
    query: AlgebraicVectorWorkerQuery,
    options: AlgebraicVectorWorkerRequestOptions,
    native_doc_id_constraints: NativeDocIdConstraintEnvelope,
    tensor_access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_program: algebraic_ir.TensorProgram,
) ![]u8 {
    if (layout != .dense_vector and layout != .sparse_vector) return error.InvalidQueryRequest;
    switch (query) {
        .dense => if (layout != .dense_vector) return error.InvalidQueryRequest,
        .sparse => |sparse| {
            if (layout != .sparse_vector) return error.InvalidQueryRequest;
            if (sparse.indices.len != sparse.values.len) return error.InvalidQueryRequest;
        },
    }
    if (!algebraic_ir.vectorSearchProgramMatchesTarget(
        tensor_program,
        index_name,
        layout,
        native_doc_id_constraints.hasConstraints(),
    )) return error.InvalidQueryRequest;
    if ((options.filter_query_json.len > 0 or options.exclusion_query_json.len > 0) and !options.require_algebraic_filter_resolution) return error.InvalidQueryRequest;
    const proof = try algebraic_ir.tensorProgramProof(alloc, tensor_access_paths, tensor_program);
    if (!proof.safe()) return error.InvalidQueryRequest;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldString(alloc, &out, &first, "index_name", index_name);
    try appendJsonFieldString(alloc, &out, &first, "layout", @tagName(layout));
    try appendJsonFieldName(alloc, &out, &first, "query");
    try appendAlgebraicVectorWorkerQuery(alloc, &out, query);
    try appendJsonFieldName(alloc, &out, &first, "options");
    try appendAlgebraicVectorWorkerRequestOptions(alloc, &out, options);
    try appendJsonFieldName(alloc, &out, &first, "native_doc_id_constraints");
    const encoded_constraints = try encodeNativeDocIdConstraintEnvelopeAlloc(alloc, native_doc_id_constraints);
    defer alloc.free(encoded_constraints);
    try out.appendSlice(alloc, encoded_constraints);
    try appendJsonFieldName(alloc, &out, &first, "tensor_access_paths");
    try out.append(alloc, '[');
    for (tensor_access_paths, 0..) |access_path, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeAlgebraicTensorAccessPathEnvelopeAlloc(alloc, access_path);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, ']');
    try appendJsonFieldName(alloc, &out, &first, "tensor_program");
    const encoded_program = try encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, tensor_program);
    defer alloc.free(encoded_program);
    try out.appendSlice(alloc, encoded_program);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn parseAlgebraicVectorWorkerRequestEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedAlgebraicVectorWorkerRequestEnvelope {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    const root = parsed.value;
    if (root != .object) return error.InvalidQueryRequest;
    const index_name_value = root.object.get("index_name") orelse return error.InvalidQueryRequest;
    if (index_name_value != .string or index_name_value.string.len == 0) return error.InvalidQueryRequest;
    const layout_value = root.object.get("layout") orelse return error.InvalidQueryRequest;
    if (layout_value != .string) return error.InvalidQueryRequest;
    const layout = std.meta.stringToEnum(algebraic_ir.PhysicalLayout, layout_value.string) orelse return error.InvalidQueryRequest;
    if (layout != .dense_vector and layout != .sparse_vector) return error.InvalidQueryRequest;

    const index_name = try alloc.dupe(u8, index_name_value.string);
    errdefer alloc.free(index_name);

    var query = try parseAlgebraicVectorWorkerQueryAlloc(alloc, layout, root.object.get("query") orelse return error.InvalidQueryRequest);
    errdefer query.deinit(alloc);
    var options = if (root.object.get("options")) |value|
        try parseAlgebraicVectorWorkerRequestOptions(alloc, value)
    else
        AlgebraicVectorWorkerRequestOptions{};
    errdefer options.deinit(alloc);

    var native_doc_id_constraints = if (root.object.get("native_doc_id_constraints")) |value|
        try parseNativeDocIdConstraintEnvelopeValueAlloc(alloc, value)
    else
        OwnedNativeDocIdConstraintEnvelope{ .constraints = .{} };
    errdefer native_doc_id_constraints.deinit(alloc);

    const access_path_value = root.object.get("tensor_access_paths") orelse return error.InvalidQueryRequest;
    if (access_path_value != .array) return error.InvalidQueryRequest;
    const tensor_access_paths = try alloc.alloc(OwnedAlgebraicTensorAccessPathEnvelope, access_path_value.array.items.len);
    var paths_initialized: usize = 0;
    errdefer {
        for (tensor_access_paths[0..paths_initialized]) |*path| path.deinit(alloc);
        if (tensor_access_paths.len > 0) alloc.free(tensor_access_paths);
    }
    for (access_path_value.array.items, 0..) |item, i| {
        const encoded = try jsonStringifyAlloc(alloc, item);
        defer alloc.free(encoded);
        tensor_access_paths[i] = try parseAlgebraicTensorAccessPathEnvelopeAlloc(alloc, encoded);
        paths_initialized += 1;
    }

    const program_value = root.object.get("tensor_program") orelse return error.InvalidQueryRequest;
    const encoded_program = try jsonStringifyAlloc(alloc, program_value);
    defer alloc.free(encoded_program);
    var tensor_program = try parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded_program);
    errdefer tensor_program.deinit(alloc);

    const envelope = OwnedAlgebraicVectorWorkerRequestEnvelope{
        .index_name = index_name,
        .layout = layout,
        .query = query,
        .options = options,
        .native_doc_id_constraints = native_doc_id_constraints,
        .tensor_access_paths = tensor_access_paths,
        .tensor_program = tensor_program,
    };
    if (!(try envelope.proveTensorProgramAlloc(alloc)).safe()) return error.InvalidQueryRequest;
    return envelope;
}

fn appendAlgebraicVectorWorkerQuery(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: AlgebraicVectorWorkerQuery,
) !void {
    try out.append(alloc, '{');
    var first = true;
    switch (query) {
        .dense => |dense| {
            try appendJsonFieldString(alloc, out, &first, "kind", "dense");
            try appendJsonFieldUsize(alloc, out, &first, "k", dense.k);
            try appendJsonFieldName(alloc, out, &first, "vector");
            try appendF32Array(alloc, out, dense.vector);
        },
        .sparse => |sparse| {
            try appendJsonFieldString(alloc, out, &first, "kind", "sparse");
            try appendJsonFieldUsize(alloc, out, &first, "k", sparse.k);
            try appendJsonFieldName(alloc, out, &first, "indices");
            try appendU32Array(alloc, out, sparse.indices);
            try appendJsonFieldName(alloc, out, &first, "values");
            try appendF32Array(alloc, out, sparse.values);
        },
    }
    try out.append(alloc, '}');
}

fn appendAlgebraicVectorWorkerRequestOptions(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    options: AlgebraicVectorWorkerRequestOptions,
) !void {
    try out.append(alloc, '{');
    var first = true;
    if (options.fields.len > 0) try appendJsonFieldStringArray(alloc, out, &first, "fields", options.fields);
    if (options.filter_prefix.len > 0) try appendJsonFieldString(alloc, out, &first, "filter_prefix", options.filter_prefix);
    if (options.filter_query_json.len > 0) try appendJsonFieldString(alloc, out, &first, "filter_query_json", options.filter_query_json);
    if (options.exclusion_query_json.len > 0) try appendJsonFieldString(alloc, out, &first, "exclusion_query_json", options.exclusion_query_json);
    if (options.filter_ids.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "filter_ids");
        try appendU64Array(alloc, out, options.filter_ids);
    }
    if (options.exclude_ids.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "exclude_ids");
        try appendU64Array(alloc, out, options.exclude_ids);
    }
    if (options.require_algebraic_filter_resolution) try appendJsonFieldBool(alloc, out, &first, "require_algebraic_filter_resolution", true);
    if (!options.include_all_fields) try appendJsonFieldBool(alloc, out, &first, "include_all_fields", false);
    if (options.defer_stored_projection) try appendJsonFieldBool(alloc, out, &first, "defer_stored_projection", true);
    try appendJsonFieldUsize(alloc, out, &first, "limit", options.limit);
    if (options.offset != 0) try appendJsonFieldUsize(alloc, out, &first, "offset", options.offset);
    if (options.count_only) try appendJsonFieldBool(alloc, out, &first, "count_only", true);
    if (options.profile) try appendJsonFieldBool(alloc, out, &first, "profile", true);
    if (!options.include_stored) try appendJsonFieldBool(alloc, out, &first, "include_stored", false);
    if (options.search_effort) |value| try appendJsonFieldF32(alloc, out, &first, "search_effort", value);
    if (options.distance_over) |value| try appendJsonFieldF32(alloc, out, &first, "distance_over", value);
    if (options.distance_under) |value| try appendJsonFieldF32(alloc, out, &first, "distance_under", value);
    if (options.return_mode != .parent) try appendJsonFieldString(alloc, out, &first, "return_mode", @tagName(options.return_mode));
    if (options.max_chunks_per_parent != 0) try appendJsonFieldUsize(alloc, out, &first, "max_chunks_per_parent", options.max_chunks_per_parent);
    try out.append(alloc, '}');
}

fn parseAlgebraicVectorWorkerRequestOptions(alloc: std.mem.Allocator, value: std.json.Value) !AlgebraicVectorWorkerRequestOptions {
    if (value != .object) return error.InvalidQueryRequest;
    const fields = if (value.object.get("fields")) |fields_value|
        try parseStringArrayJsonAlloc(alloc, fields_value)
    else
        @constCast((&[_][]const u8{})[0..]);
    const filter_prefix = if (value.object.get("filter_prefix")) |prefix_value|
        try parseStringJsonAlloc(alloc, prefix_value)
    else
        "";
    const filter_query_json = if (value.object.get("filter_query_json")) |query_value|
        try parseStringJsonAlloc(alloc, query_value)
    else
        "";
    const exclusion_query_json = if (value.object.get("exclusion_query_json")) |query_value|
        try parseStringJsonAlloc(alloc, query_value)
    else
        "";
    const filter_ids = if (value.object.get("filter_ids")) |ids_value|
        try parseU64JsonArrayAlloc(alloc, ids_value)
    else
        @constCast((&[_]u64{})[0..]);
    const exclude_ids = if (value.object.get("exclude_ids")) |ids_value|
        try parseU64JsonArrayAlloc(alloc, ids_value)
    else
        @constCast((&[_]u64{})[0..]);
    errdefer {
        for (fields) |field| alloc.free(field);
        if (fields.len > 0) alloc.free(fields);
        if (filter_prefix.len > 0) alloc.free(filter_prefix);
        if (filter_query_json.len > 0) alloc.free(filter_query_json);
        if (exclusion_query_json.len > 0) alloc.free(exclusion_query_json);
        if (filter_ids.len > 0) alloc.free(filter_ids);
        if (exclude_ids.len > 0) alloc.free(exclude_ids);
    }
    if (filter_query_json.len > 0) {
        var parsed_filter = std.json.parseFromSlice(std.json.Value, alloc, filter_query_json, .{}) catch return error.InvalidQueryRequest;
        parsed_filter.deinit();
    }
    if (exclusion_query_json.len > 0) {
        var parsed_exclusion = std.json.parseFromSlice(std.json.Value, alloc, exclusion_query_json, .{}) catch return error.InvalidQueryRequest;
        parsed_exclusion.deinit();
    }
    const require_algebraic_filter_resolution = try parseOptionalBoolJson(value.object.get("require_algebraic_filter_resolution"), false);
    if ((filter_query_json.len > 0 or exclusion_query_json.len > 0) and !require_algebraic_filter_resolution) return error.InvalidQueryRequest;
    return .{
        .fields = fields,
        .filter_prefix = filter_prefix,
        .filter_query_json = filter_query_json,
        .exclusion_query_json = exclusion_query_json,
        .filter_ids = filter_ids,
        .exclude_ids = exclude_ids,
        .require_algebraic_filter_resolution = require_algebraic_filter_resolution,
        .include_all_fields = try parseOptionalBoolJson(value.object.get("include_all_fields"), true),
        .defer_stored_projection = try parseOptionalBoolJson(value.object.get("defer_stored_projection"), false),
        .limit = try parseOptionalU32Json(value.object.get("limit"), 10),
        .offset = try parseOptionalU32Json(value.object.get("offset"), 0),
        .count_only = try parseOptionalBoolJson(value.object.get("count_only"), false),
        .profile = try parseOptionalBoolJson(value.object.get("profile"), false),
        .include_stored = try parseOptionalBoolJson(value.object.get("include_stored"), true),
        .search_effort = try parseOptionalF32Json(value.object.get("search_effort")),
        .distance_over = try parseOptionalF32Json(value.object.get("distance_over")),
        .distance_under = try parseOptionalF32Json(value.object.get("distance_under")),
        .return_mode = try parseOptionalReturnModeJson(value.object.get("return_mode")),
        .max_chunks_per_parent = try parseOptionalU32Json(value.object.get("max_chunks_per_parent"), 0),
    };
}

fn appendJsonFieldF32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f32,
) !void {
    if (!std.math.isFinite(value)) return error.InvalidQueryRequest;
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendF32Array(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const f32,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (!std.math.isFinite(value)) return error.InvalidQueryRequest;
        if (i > 0) try out.append(alloc, ',');
        const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
    }
    try out.append(alloc, ']');
}

fn appendU32Array(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const u32,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
    }
    try out.append(alloc, ']');
}

fn appendU64Array(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    values: []const u64,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
    }
    try out.append(alloc, ']');
}

fn parseAlgebraicVectorWorkerQueryAlloc(
    alloc: std.mem.Allocator,
    layout: algebraic_ir.PhysicalLayout,
    value: std.json.Value,
) !OwnedAlgebraicVectorWorkerQuery {
    if (value != .object) return error.InvalidQueryRequest;
    const kind_value = value.object.get("kind") orelse return error.InvalidQueryRequest;
    if (kind_value != .string) return error.InvalidQueryRequest;
    const k = try parseOptionalU32Json(value.object.get("k"), 10);
    if (std.mem.eql(u8, kind_value.string, "dense")) {
        if (layout != .dense_vector) return error.InvalidQueryRequest;
        return .{ .dense = .{
            .vector = try parseF32JsonArrayAlloc(alloc, value.object.get("vector") orelse return error.InvalidQueryRequest),
            .k = k,
        } };
    }
    if (std.mem.eql(u8, kind_value.string, "sparse")) {
        if (layout != .sparse_vector) return error.InvalidQueryRequest;
        const indices = try parseU32JsonArrayAlloc(alloc, value.object.get("indices") orelse return error.InvalidQueryRequest);
        errdefer if (indices.len > 0) alloc.free(indices);
        const values = try parseF32JsonArrayAlloc(alloc, value.object.get("values") orelse return error.InvalidQueryRequest);
        errdefer if (values.len > 0) alloc.free(values);
        if (indices.len != values.len) return error.InvalidQueryRequest;
        return .{ .sparse = .{
            .indices = indices,
            .values = values,
            .k = k,
        } };
    }
    return error.InvalidQueryRequest;
}

fn parseOptionalU32Json(value_opt: ?std.json.Value, default_value: u32) !u32 {
    const value = value_opt orelse return default_value;
    if (value != .integer or value.integer < 0) return error.InvalidQueryRequest;
    return std.math.cast(u32, value.integer) orelse return error.InvalidQueryRequest;
}

fn parseOptionalBoolJson(value_opt: ?std.json.Value, default_value: bool) !bool {
    const value = value_opt orelse return default_value;
    if (value != .bool) return error.InvalidQueryRequest;
    return value.bool;
}

fn parseOptionalF32Json(value_opt: ?std.json.Value) !?f32 {
    const value = value_opt orelse return null;
    const parsed: f32 = switch (value) {
        .integer => |raw| @floatFromInt(raw),
        .float => |raw| @floatCast(raw),
        .number_string => |raw| std.fmt.parseFloat(f32, raw) catch return error.InvalidQueryRequest,
        else => return error.InvalidQueryRequest,
    };
    if (!std.math.isFinite(parsed)) return error.InvalidQueryRequest;
    return parsed;
}

fn parseOptionalReturnModeJson(value_opt: ?std.json.Value) !db_mod.types.ReturnMode {
    const value = value_opt orelse return .parent;
    if (value != .string) return error.InvalidQueryRequest;
    return std.meta.stringToEnum(db_mod.types.ReturnMode, value.string) orelse return error.InvalidQueryRequest;
}

fn parseStringJsonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    if (value != .string) return error.InvalidQueryRequest;
    if (value.string.len == 0) return "";
    return try alloc.dupe(u8, value.string);
}

fn parseStringArrayJsonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc([]const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        if (out.len > 0) alloc.free(out);
    }
    for (value.array.items) |item| {
        if (item != .string) return error.InvalidQueryRequest;
        out[initialized] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return out;
}

fn parseF32JsonArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const f32 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc(f32, value.array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (value.array.items, 0..) |item, i| {
        out[i] = switch (item) {
            .integer => |raw| @floatFromInt(raw),
            .float => |raw| @floatCast(raw),
            .number_string => |raw| std.fmt.parseFloat(f32, raw) catch return error.InvalidQueryRequest,
            else => return error.InvalidQueryRequest,
        };
        if (!std.math.isFinite(out[i])) return error.InvalidQueryRequest;
    }
    return out;
}

fn parseU32JsonArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]const u32 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc(u32, value.array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (value.array.items, 0..) |item, i| {
        if (item != .integer or item.integer < 0) return error.InvalidQueryRequest;
        out[i] = std.math.cast(u32, item.integer) orelse return error.InvalidQueryRequest;
    }
    return out;
}

fn parseU64JsonArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u64 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc(u64, value.array.items.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (value.array.items, 0..) |item, i| {
        if (item != .integer or item.integer < 0) return error.InvalidQueryRequest;
        out[i] = std.math.cast(u64, item.integer) orelse return error.InvalidQueryRequest;
    }
    return out;
}

fn parseAlgebraicTensorProgramRefsAlloc(
    alloc: std.mem.Allocator,
    refs: []const AlgebraicTensorProgramRefInput,
) ![]algebraic_ir.TensorProgramRef {
    const out = try alloc.alloc(algebraic_ir.TensorProgramRef, refs.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (refs, 0..) |input, i| {
        out[i] = parseAlgebraicTensorProgramRef(input) orelse return error.InvalidQueryRequest;
    }
    return out;
}

fn parseAlgebraicTensorProgramRef(input: AlgebraicTensorProgramRefInput) ?algebraic_ir.TensorProgramRef {
    if (std.mem.eql(u8, input.kind, "input")) return .{ .input = input.index };
    if (std.mem.eql(u8, input.kind, "step")) return .{ .step = input.index };
    return null;
}

pub fn parseAlgebraicTensorAccessPathEnvelopeInputAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorAccessPathEnvelopeInput,
) !OwnedAlgebraicTensorAccessPathEnvelope {
    if (input.owner.len == 0) return error.InvalidQueryRequest;
    const layout = std.meta.stringToEnum(algebraic_ir.PhysicalLayout, input.layout) orelse return error.InvalidQueryRequest;
    var dictionary = if (input.dictionary) |value| try parseAlgebraicDictionaryIdentityAlloc(alloc, value) else null;
    errdefer if (dictionary) |*value| value.deinit(alloc);

    const fragments = try alloc.alloc(algebraic_ir.TensorFragment, input.fragments.len);
    errdefer if (fragments.len > 0) alloc.free(fragments);
    for (input.fragments, 0..) |fragment, i| {
        fragments[i] = std.meta.stringToEnum(algebraic_ir.TensorFragment, fragment) orelse return error.InvalidQueryRequest;
    }

    const output_dims = try alloc.alloc(algebraic_ir.Dimension, input.output_dims.len);
    errdefer if (output_dims.len > 0) alloc.free(output_dims);
    for (input.output_dims, 0..) |dim, i| {
        output_dims[i] = std.meta.stringToEnum(algebraic_ir.Dimension, dim) orelse return error.InvalidQueryRequest;
    }

    const law_ids = try alloc.alloc(algebraic_law.Id, input.law_ids.len);
    errdefer if (law_ids.len > 0) alloc.free(law_ids);
    for (input.law_ids, 0..) |law_id, i| {
        law_ids[i] = algebraic_law.Id.parse(law_id) orelse return error.InvalidQueryRequest;
    }
    const owner = try alloc.dupe(u8, input.owner);

    return .{
        .owner = owner,
        .layout = layout,
        .dictionary = dictionary,
        .fragments = fragments,
        .output_dims = output_dims,
        .law_ids = law_ids,
    };
}

fn parseAlgebraicDictionaryIdentityAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicDictionaryIdentityInput,
) !OwnedAlgebraicDictionaryIdentity {
    const label_kind = std.meta.stringToEnum(algebraic_lexical.LabelKind, input.label_kind) orelse return error.InvalidQueryRequest;
    const scope = try alloc.dupe(u8, input.scope);
    errdefer alloc.free(scope);
    const field_or_path = try alloc.dupe(u8, input.field_or_path);
    errdefer alloc.free(field_or_path);
    const analyzer_or_canonicalization = try alloc.dupe(u8, input.analyzer_or_canonicalization);
    errdefer alloc.free(analyzer_or_canonicalization);
    const value_kind = try alloc.dupe(u8, input.value_kind);
    errdefer alloc.free(value_kind);
    const coercion_policy = try alloc.dupe(u8, input.coercion_policy);
    errdefer alloc.free(coercion_policy);
    return .{
        .scope = scope,
        .field_or_path = field_or_path,
        .label_kind = label_kind,
        .analyzer_or_canonicalization = analyzer_or_canonicalization,
        .value_kind = value_kind,
        .coercion_policy = coercion_policy,
    };
}

fn parseAlgebraicDimensionArrayAlloc(
    alloc: std.mem.Allocator,
    values: []const []const u8,
) ![]algebraic_ir.Dimension {
    const dims = try alloc.alloc(algebraic_ir.Dimension, values.len);
    errdefer if (dims.len > 0) alloc.free(dims);
    for (values, 0..) |value, i| {
        dims[i] = std.meta.stringToEnum(algebraic_ir.Dimension, value) orelse return error.InvalidQueryRequest;
    }
    return dims;
}

pub fn applyNativeDocIdConstraintEnvelope(req: *db_mod.types.SearchRequest, constraints: NativeDocIdConstraintEnvelope) void {
    if (constraints.positive_filter or constraints.include_doc_ids.len > 0) {
        req.filter_doc_ids_positive = true;
        req.filter_doc_ids = constraints.include_doc_ids;
    }
    if (constraints.exclude_doc_ids.len > 0) req.exclude_doc_ids = constraints.exclude_doc_ids;
}

pub fn encodeNativeDocIdConstraintEnvelopeAlloc(
    alloc: std.mem.Allocator,
    constraints: NativeDocIdConstraintEnvelope,
) ![]u8 {
    var include_doc_ids = try normalizedDocIdArrayAlloc(alloc, constraints.include_doc_ids);
    defer freeOwnedStringSlice(alloc, include_doc_ids);
    const exclude_doc_ids = try normalizedDocIdArrayAlloc(alloc, constraints.exclude_doc_ids);
    defer freeOwnedStringSlice(alloc, exclude_doc_ids);
    include_doc_ids = try subtractSortedOwnedStringsAlloc(alloc, include_doc_ids, exclude_doc_ids);
    const positive_filter = constraints.positive_filter or include_doc_ids.len > 0;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendJsonFieldBool(alloc, &out, &first, "positive_filter", positive_filter);
    try appendJsonFieldStringArray(alloc, &out, &first, "include_doc_ids", include_doc_ids);
    if (exclude_doc_ids.len > 0) {
        try appendJsonFieldStringArray(alloc, &out, &first, "exclude_doc_ids", exclude_doc_ids);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn normalizedDocIdArrayAlloc(
    alloc: std.mem.Allocator,
    values: []const []const u8,
) ![][]const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(@constCast(item));
        alloc.free(out);
    }
    for (values) |value| {
        out[initialized] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return try sortAndDedupeOwnedStringArrayAlloc(alloc, out);
}

fn sortAndDedupeOwnedStringArrayAlloc(
    alloc: std.mem.Allocator,
    values: [][]const u8,
) ![][]const u8 {
    if (values.len == 0) return &.{};
    std.mem.sort([]const u8, values, {}, stringLessThan);

    var write: usize = 0;
    for (values, 0..) |value, read| {
        if (read > 0 and std.mem.eql(u8, value, values[read - 1])) {
            alloc.free(@constCast(value));
            continue;
        }
        values[write] = value;
        write += 1;
    }
    if (write == values.len) return values;
    const resized = alloc.realloc(values, write) catch |err| {
        for (values[0..write]) |value| alloc.free(@constCast(value));
        alloc.free(values);
        return err;
    };
    return resized;
}

fn subtractSortedOwnedStringsAlloc(
    alloc: std.mem.Allocator,
    values: [][]const u8,
    excluded: []const []const u8,
) ![][]const u8 {
    if (values.len == 0 or excluded.len == 0) return values;

    var write: usize = 0;
    var exclude_idx: usize = 0;
    for (values) |value| {
        while (exclude_idx < excluded.len and std.mem.lessThan(u8, excluded[exclude_idx], value)) {
            exclude_idx += 1;
        }
        if (exclude_idx < excluded.len and std.mem.eql(u8, excluded[exclude_idx], value)) {
            alloc.free(@constCast(value));
            continue;
        }
        values[write] = value;
        write += 1;
    }
    if (write == values.len) return values;
    const resized = alloc.realloc(values, write) catch |err| {
        for (values[0..write]) |value| alloc.free(@constCast(value));
        alloc.free(values);
        return err;
    };
    return resized;
}

fn stringLessThan(_: void, left: []const u8, right: []const u8) bool {
    return std.mem.lessThan(u8, left, right);
}

fn appendJsonFieldStringArray(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    values: []const []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, value);
    }
    try out.append(alloc, ']');
}

fn appendEnumNameArray(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime T: type,
    values: []const T,
) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, @tagName(value));
    }
    try out.append(alloc, ']');
}

pub fn parseNativeDocIdConstraintEnvelopeAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !OwnedNativeDocIdConstraintEnvelope {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return parseNativeDocIdConstraintEnvelopeValueAlloc(alloc, parsed.value);
}

fn parseNativeDocIdConstraintEnvelopeValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !OwnedNativeDocIdConstraintEnvelope {
    if (value != .object) return error.InvalidQueryRequest;

    var out = OwnedNativeDocIdConstraintEnvelope{ .constraints = .{} };
    errdefer out.deinit(alloc);

    var include_doc_ids: [][]const u8 = &.{};
    errdefer freeOwnedStringSlice(alloc, include_doc_ids);
    var exclude_doc_ids: [][]const u8 = &.{};
    errdefer freeOwnedStringSlice(alloc, exclude_doc_ids);

    if (value.object.get("positive_filter")) |positive| {
        if (positive != .bool) return error.InvalidQueryRequest;
        out.constraints.positive_filter = positive.bool;
    }
    if (value.object.get("include_doc_ids")) |include| {
        include_doc_ids = try parseInternalDocIdArrayAlloc(alloc, include);
        if (include_doc_ids.len > 0) out.constraints.positive_filter = true;
    }
    if (value.object.get("exclude_doc_ids")) |exclude| {
        exclude_doc_ids = try parseInternalDocIdArrayAlloc(alloc, exclude);
    }
    include_doc_ids = try subtractSortedOwnedStringsAlloc(alloc, include_doc_ids, exclude_doc_ids);
    out.constraints.include_doc_ids = include_doc_ids;
    out.constraints.exclude_doc_ids = exclude_doc_ids;
    include_doc_ids = &.{};
    exclude_doc_ids = &.{};
    return out;
}

fn freeOwnedMutableStrings(alloc: std.mem.Allocator, values: [][]u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

pub const OwnedQueryRequest = struct {
    fields: [][]const u8 = &.{},
    req: db_mod.types.SearchRequest = .{},

    pub fn deinit(self: *OwnedQueryRequest, alloc: std.mem.Allocator) void {
        if (self.fields.len > 0) {
            for (self.fields) |field| alloc.free(field);
            alloc.free(self.fields);
        }
        freeSearchRequest(alloc, &self.req);
        self.* = undefined;
    }
};

pub const QueryPreflightSummary = struct {
    full_text_indexes: []const []const u8 = &.{},
    embedding_indexes: []const []const u8 = &.{},
    graph_indexes: []const []const u8 = &.{},
    result_refs: []const []const u8 = &.{},
    graph_query_order: []const []const u8 = &.{},
    requested_limit: u32 = 10,
    requested_offset: u32 = 0,
    base_result_set_count: u32 = 0,
    graph_query_count: u32 = 0,
    requires_fusion: bool = false,
    count_only: bool = false,
    profile_requested: bool = false,
    include_stored: bool = false,
    reranker_enabled: bool = false,
    aggregation_count: u32 = 0,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        freeOwnedStringSlice(alloc, self.full_text_indexes);
        freeOwnedStringSlice(alloc, self.embedding_indexes);
        freeOwnedStringSlice(alloc, self.graph_indexes);
        freeOwnedStringSlice(alloc, self.result_refs);
        freeOwnedStringSlice(alloc, self.graph_query_order);
    }
};

const NamedVectorQueries = struct {
    dense: []const db_mod.types.NamedDenseQuery = &.{},
    sparse: []const db_mod.types.NamedSparseQuery = &.{},

    fn deinit(self: *const NamedVectorQueries, alloc: std.mem.Allocator) void {
        freeNamedDenseQueries(alloc, self.dense);
        freeNamedSparseQueries(alloc, self.sparse);
    }
};

pub const SemanticResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_dense_query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            semantic_search: []const u8,
            embedding_template: ?[]const u8,
            limit: u32,
        ) anyerror!db_mod.types.DenseKnnQuery,
    };

    pub fn resolveDenseQuery(
        self: SemanticResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_mod.types.DenseKnnQuery {
        return try self.vtable.resolve_dense_query(self.ptr, alloc, table_name, index_name, semantic_search, embedding_template, limit);
    }
};

fn applyCommonSearchRequestOptions(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
    req: *db_mod.types.SearchRequest,
) !void {
    if (request.limit) |limit| req.limit = @intCast(limit);
    if (request.offset) |offset| req.offset = @intCast(offset);
    if (request.count) |count| req.count_only = count;
    if (request.profile) |profile| req.profile = profile;
    if (request.aggregations) |aggregations| {
        req.aggregations_json = try jsonStringifyAlloc(alloc, aggregations);
    }
    if (request.filter_prefix) |filter_prefix| req.filter_prefix = try alloc.dupe(u8, filter_prefix);
    if (request.distance_over) |distance_over| req.distance_over = distance_over;
    if (request.distance_under) |distance_under| req.distance_under = distance_under;
    req.search_effort = request.search_effort;
    if (request.merge_config) |merge_config| req.merge_config = try parseMergeConfig(alloc, merge_config);
    if (request.pruner) |pruner| req.pruner = try parsePruner(pruner);
    if (request.reranker) |reranker| {
        const encoded_reranker = try std.json.Stringify.valueAlloc(alloc, reranker, .{});
        defer alloc.free(encoded_reranker);
        req.reranker = reranking_mod.parseConfigFromSlice(alloc, encoded_reranker) catch |err| switch (err) {
            error.InvalidRerankerConfig => return error.InvalidQueryRequest,
            else => return err,
        };
    }

    const has_semantic = request.semantic_search != null or request.embeddings != null;
    if (has_semantic and req.offset > 0) return error.UnsupportedQueryRequest;
    if (request.embedding_template != null and request.semantic_search == null) return error.UnsupportedQueryRequest;
    if (request.embedding_template != null and request.embeddings != null) return error.UnsupportedQueryRequest;
    if (req.count_only and req.reranker != null) return error.UnsupportedQueryRequest;
}

fn applySearchRequestFields(
    alloc: std.mem.Allocator,
    generated_fields: ?[]const []const u8,
    req: *db_mod.types.SearchRequest,
) ![][]const u8 {
    const include_all_fields = generated_fields == null;
    const fields: [][]const u8 = if (generated_fields) |items|
        try cloneFields(alloc, items)
    else
        &.{};
    req.fields = fields;
    req.include_all_fields = include_all_fields;
    req.include_stored = include_all_fields or fields.len > 0 or req.reranker != null;
    req.defer_stored_projection = canDeferStoredProjection(fields);
    return fields;
}

fn freeClonedFields(alloc: std.mem.Allocator, fields: []const []const u8) void {
    for (fields) |field| alloc.free(field);
    if (fields.len > 0) alloc.free(fields);
}

pub fn parseQueryRequest(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    body: []const u8,
) !OwnedQueryRequest {
    if (body.len == 0) return error.InvalidQueryRequest;

    // Packed dense requests are benchmark-oriented and unusual in production.
    // Skip the extra JSON parse unless the request even mentions embeddings.
    if (std.mem.indexOf(u8, body, "\"embeddings\"") != null and fastDensePublicQueryMayApply(body)) {
        if (try tryParseFastDensePublicQueryRequest(alloc, body)) |fast| {
            return fast;
        }
    }

    var parse_options: std.json.ParseOptions = .{};
    if (queryBodyHasInternalShardFields(body)) {
        // Internal shard fanout forwards precomputed execution hints outside the
        // public OpenAPI contract; ignore just enough schema strictness to accept
        // those fields.
        parse_options.ignore_unknown_fields = true;
    }

    var parsed = ant_json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, parse_options) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    const request = parsed.value;

    if (request.analyses != null) return error.UnsupportedQueryRequest;
    if (request.order_by != null) return error.UnsupportedQueryRequest;
    if (request.search_after != null) return error.UnsupportedQueryRequest;
    if (request.search_before != null) return error.UnsupportedQueryRequest;
    if (request.document_renderer != null) return error.UnsupportedQueryRequest;
    if (request.join != null) return error.UnsupportedQueryRequest;
    if (request.foreign_sources != null) return error.UnsupportedQueryRequest;

    var req: db_mod.types.SearchRequest = .{};
    errdefer freeSearchRequest(alloc, &req);

    try applyCommonSearchRequestOptions(alloc, request, &req);
    req.distributed_text_stats = try parseDistributedTextStatsAlloc(alloc, body);
    try parseInternalDocIdConstraintsAlloc(alloc, body, &req);

    const fields = try applySearchRequestFields(alloc, request.fields, &req);
    errdefer freeClonedFields(alloc, fields);

    var normalized_query = try normalizePublicQueryBucketsAlloc(alloc, request, req.limit);
    errdefer normalized_query.deinit(alloc);

    if (req.reranker != null) {
        req.reranker_query_text = try buildRerankerQueryText(alloc, request);
    }

    if (normalized_query.full_text) |query| {
        req.full_text = query;
        normalized_query.full_text = null;
    } else if (normalized_query.filter_query_json.len > 0 or normalized_query.exclusion_query_json.len > 0) {
        req.full_text = .{ .match_all = {} };
    }

    req.filter_query_json = normalized_query.filter_query_json;
    normalized_query.filter_query_json = "";
    req.exclusion_query_json = normalized_query.exclusion_query_json;
    normalized_query.exclusion_query_json = "";
    try parseInternalFilterQueryJsonAlloc(alloc, body, &req);

    const vector_queries = try buildSemanticVectorQueries(alloc, semantic_resolver, table_name, request, req.limit);
    errdefer vector_queries.deinit(alloc);
    req.dense_queries = vector_queries.dense;
    req.sparse_queries = vector_queries.sparse;
    req.graph_queries = try buildGraphQueries(alloc, request);
    if (request.expand_strategy) |expand_strategy| {
        req.expand_strategy = try parseExpandStrategy(expand_strategy);
    }

    return .{
        .fields = fields,
        .req = req,
    };
}

pub fn parsePublicQueryRequest(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    body: []const u8,
) !OwnedQueryRequest {
    return try parseQueryRequest(alloc, semantic_resolver, table_name, body);
}

pub fn preflightGraphSearchesAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) !void {
    const graph_queries = try buildGraphQueries(alloc, request);
    defer freeNamedGraphQueries(alloc, graph_queries);
}

fn buildPreflightSearchRequestAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) !OwnedQueryRequest {
    var req: db_mod.types.SearchRequest = .{};
    errdefer freeSearchRequest(alloc, &req);

    try applyCommonSearchRequestOptions(alloc, request, &req);

    if (request.search_after != null and request.search_before != null) return error.UnsupportedQueryRequest;
    if ((request.search_after != null or request.search_before != null) and request.order_by == null) return error.UnsupportedQueryRequest;

    const fields = try applySearchRequestFields(alloc, request.fields, &req);
    errdefer freeClonedFields(alloc, fields);

    var normalized_query = try normalizePublicQueryBucketsAlloc(alloc, request, req.limit);
    errdefer normalized_query.deinit(alloc);

    if (normalized_query.full_text) |query| {
        req.full_text = query;
        normalized_query.full_text = null;
    } else if (normalized_query.filter_query_json.len > 0 or normalized_query.exclusion_query_json.len > 0 or request.order_by != null or request.search_after != null or request.search_before != null) {
        req.full_text = .{ .match_all = {} };
    }

    if (req.reranker != null) {
        req.reranker_query_text = try buildRerankerQueryText(alloc, request);
    }

    req.filter_query_json = normalized_query.filter_query_json;
    normalized_query.filter_query_json = "";
    req.exclusion_query_json = normalized_query.exclusion_query_json;
    normalized_query.exclusion_query_json = "";

    const vector_queries = try buildPreflightSemanticVectorQueries(alloc, request, req.limit);
    errdefer vector_queries.deinit(alloc);
    req.dense_queries = vector_queries.dense;
    req.sparse_queries = vector_queries.sparse;
    req.graph_queries = try buildGraphQueries(alloc, request);
    if (request.expand_strategy) |expand_strategy| {
        req.expand_strategy = try parseExpandStrategy(expand_strategy);
    }

    return .{
        .fields = fields,
        .req = req,
    };
}

fn preflightRequestHasFullTextResults(req: db_mod.types.SearchRequest) bool {
    if (req.full_text != null) return true;
    if (req.full_text_queries.len > 0) return true;
    return req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0;
}

fn preflightBaseResultSetCount(req: db_mod.types.SearchRequest) u32 {
    var count: u32 = 0;
    if (req.full_text_queries.len > 0) {
        count += @as(u32, @intCast(req.full_text_queries.len));
    } else if (preflightRequestHasFullTextResults(req)) {
        count += 1;
    }
    count += @as(u32, @intCast(req.dense_queries.len));
    count += @as(u32, @intCast(req.sparse_queries.len));
    if (req.dense_queries.len == 0 and req.sparse_queries.len == 0) {
        if (req.dense != null) count += 1;
        if (req.sparse != null) count += 1;
    }
    return count;
}

fn preflightRequiresFusion(req: db_mod.types.SearchRequest) bool {
    const base_result_sets = preflightBaseResultSetCount(req);
    return base_result_sets > 1 or (base_result_sets == 1 and req.merge_config != null);
}

fn countAggregationRequests(aggregations: ?std.json.ArrayHashMap(metadata_openapi.AggregationRequest)) u32 {
    const value = aggregations orelse return 0;
    var total: u32 = 0;
    for (value.map.values()) |aggregation| {
        total += 1;
        total += countAggregationRequests(aggregation.sub_aggregations);
    }
    return total;
}

pub fn preflightQueryRequestAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) !QueryPreflightSummary {
    var preflight_req = try buildPreflightSearchRequestAlloc(alloc, request);
    defer preflight_req.deinit(alloc);

    var runtime_preflight = try db_mod.preflightSearchRequestAlloc(alloc, preflight_req.req);
    defer runtime_preflight.deinit(alloc);

    var full_text_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, full_text_indexes.items);
    errdefer full_text_indexes.deinit(alloc);
    var embedding_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, embedding_indexes.items);
    errdefer embedding_indexes.deinit(alloc);
    var graph_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, graph_indexes.items);
    errdefer graph_indexes.deinit(alloc);
    var result_refs = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, result_refs.items);
    errdefer result_refs.deinit(alloc);
    var graph_query_order = std.ArrayListUnmanaged([]const u8).empty;
    errdefer freeOwnedStringItems(alloc, graph_query_order.items);
    errdefer graph_query_order.deinit(alloc);

    if (preflightRequestHasFullTextResults(preflight_req.req)) {
        try appendUniqueOwnedString(alloc, &full_text_indexes, "full_text");
    }
    for (preflight_req.req.dense_queries) |dense_query| {
        try appendUniqueOwnedString(alloc, &embedding_indexes, dense_query.index_name);
    }
    for (preflight_req.req.sparse_queries) |sparse_query| {
        try appendUniqueOwnedString(alloc, &embedding_indexes, sparse_query.index_name);
    }
    for (preflight_req.req.graph_queries) |graph_query| {
        try appendUniqueOwnedString(alloc, &graph_indexes, graph_query.query.index_name);
    }
    for (runtime_preflight.result_refs) |result_ref| try appendUniqueOwnedString(alloc, &result_refs, result_ref);
    for (runtime_preflight.graph_query_order) |name| try appendUniqueOwnedString(alloc, &graph_query_order, name);

    return .{
        .full_text_indexes = if (full_text_indexes.items.len == 0) &.{} else try full_text_indexes.toOwnedSlice(alloc),
        .embedding_indexes = if (embedding_indexes.items.len == 0) &.{} else try embedding_indexes.toOwnedSlice(alloc),
        .graph_indexes = if (graph_indexes.items.len == 0) &.{} else try graph_indexes.toOwnedSlice(alloc),
        .result_refs = if (result_refs.items.len == 0) &.{} else try result_refs.toOwnedSlice(alloc),
        .graph_query_order = if (graph_query_order.items.len == 0) &.{} else try graph_query_order.toOwnedSlice(alloc),
        .requested_limit = preflight_req.req.limit,
        .requested_offset = preflight_req.req.offset,
        .base_result_set_count = preflightBaseResultSetCount(preflight_req.req),
        .graph_query_count = @as(u32, @intCast(preflight_req.req.graph_queries.len)),
        .requires_fusion = preflightRequiresFusion(preflight_req.req),
        .count_only = preflight_req.req.count_only,
        .profile_requested = preflight_req.req.profile,
        .include_stored = preflight_req.req.include_stored,
        .reranker_enabled = preflight_req.req.reranker != null,
        .aggregation_count = countAggregationRequests(request.aggregations),
    };
}

const FastDenseEmbedding = union(enum) {
    @"packed": []const u8,
    dense: []const f32,

    pub fn jsonParse(
        allocator: std.mem.Allocator,
        source: anytype,
        options: std.json.ParseOptions,
    ) !@This() {
        switch (try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?)) {
            .string => |value| return .{ .@"packed" = value },
            .allocated_string => |value| return .{ .@"packed" = value },
            .array_begin => return .{ .dense = try parseDenseArrayAlloc(allocator, source, options) },
            else => return error.UnexpectedToken,
        }
    }
};

const FastDensePublicQueryRequest = struct {
    embeddings: ?std.json.ArrayHashMap(FastDenseEmbedding) = null,
    indexes: ?[]const []const u8 = null,
    fields: ?[]const []const u8 = null,
    limit: ?u32 = null,
    offset: ?u32 = null,
    count: ?bool = null,
    profile: ?bool = null,
    search_effort: ?f32 = null,
    filter_prefix: ?[]const u8 = null,
    distance_over: ?f32 = null,
    distance_under: ?f32 = null,
};

fn fastDensePublicQueryMayApply(body: []const u8) bool {
    const disallowed = [_][]const u8{
        "\"query\"",
        "\"full_text_search\"",
        "\"filter_query\"",
        "\"exclusion_query\"",
        "\"merge_config\"",
        "\"reranker\"",
        "\"pruner\"",
        "\"semantic_search\"",
        "\"sparse\"",
        "\"graph\"",
        "\"join\"",
        "\"with\"",
        "\"_filter_query_json\"",
        "\"_exclusion_query_json\"",
    };
    for (disallowed) |needle| {
        if (std.mem.indexOf(u8, body, needle) != null) return false;
    }
    return true;
}

fn tryParseFastDensePublicQueryRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !?OwnedQueryRequest {
    var parsed = ant_json.parseFromSlice(FastDensePublicQueryRequest, alloc, body, .{}) catch return null;
    defer parsed.deinit();

    const request = parsed.value;
    const embeddings = request.embeddings orelse return null;
    if (embeddings.map.count() == 0) return error.UnsupportedQueryRequest;
    if ((request.offset orelse 0) > 0) return error.UnsupportedQueryRequest;

    var req: db_mod.types.SearchRequest = .{};
    errdefer freeSearchRequest(alloc, &req);

    req.limit = request.limit orelse req.limit;
    req.count_only = request.count orelse false;
    req.profile = request.profile orelse false;
    req.search_effort = request.search_effort;
    if (request.filter_prefix) |filter_prefix| req.filter_prefix = try alloc.dupe(u8, filter_prefix);
    req.distance_over = request.distance_over;
    req.distance_under = request.distance_under;

    const fields = try applySearchRequestFields(alloc, request.fields, &req);
    errdefer freeClonedFields(alloc, fields);

    const index_names = request.indexes orelse blk: {
        const out = try alloc.alloc([]u8, embeddings.map.count());
        errdefer alloc.free(out);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |index_name| alloc.free(index_name);
        }
        var it = embeddings.map.iterator();
        while (it.next()) |entry| {
            out[initialized] = try alloc.dupe(u8, entry.key_ptr.*);
            initialized += 1;
        }
        break :blk out;
    };
    defer if (request.indexes == null) {
        for (index_names) |index_name| alloc.free(index_name);
        alloc.free(index_names);
    };
    if (index_names.len == 0) return error.UnsupportedQueryRequest;

    const dense_queries = try alloc.alloc(db_mod.types.NamedDenseQuery, index_names.len);
    var dense_queries_initialized: usize = 0;
    errdefer freeNamedDenseQueries(alloc, dense_queries[0..dense_queries_initialized]);

    for (index_names, 0..) |index_name, i| {
        const embedding = embeddings.map.get(index_name) orelse return error.UnsupportedQueryRequest;
        dense_queries[i] = .{
            .name = try alloc.dupe(u8, index_name),
            .index_name = try alloc.dupe(u8, index_name),
            .query = .{
                .vector = switch (embedding) {
                    .@"packed" => |encoded| vector_codec.decodePackedF32Base64Alloc(alloc, encoded) catch return error.InvalidQueryRequest,
                    .dense => |dense| try alloc.dupe(f32, dense),
                },
                .k = req.limit,
            },
        };
        dense_queries_initialized += 1;
    }
    req.dense_queries = dense_queries;

    return .{
        .fields = fields,
        .req = req,
    };
}

fn parseDenseArrayAlloc(
    allocator: std.mem.Allocator,
    source: anytype,
    options: std.json.ParseOptions,
) ![]const f32 {
    var values = std.ArrayListUnmanaged(f32).empty;
    errdefer values.deinit(allocator);

    while (true) {
        const token = try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?);
        switch (token) {
            .array_end => return try values.toOwnedSlice(allocator),
            .number => |value| try values.append(allocator, try parseJsonNumberF32(value)),
            .allocated_number => |value| {
                defer allocator.free(value);
                try values.append(allocator, try parseJsonNumberF32(value));
            },
            else => return error.UnexpectedToken,
        }
    }
}

fn parseJsonNumberF32(value: []const u8) !f32 {
    if (std.mem.indexOfAny(u8, value, ".eE") != null) {
        return try std.fmt.parseFloat(f32, value);
    }
    return @floatFromInt(try std.fmt.parseInt(i64, value, 10));
}

pub fn encodeQueryResponses(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    meta: QueryResponseMeta,
    result: db_mod.types.SearchResult,
) !QueryResponse {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const emitted_hits = if (req.count_only) &.{} else result.hits;
    const hits = try arena.alloc(metadata_openapi.QueryHit, emitted_hits.len);
    for (emitted_hits, 0..) |hit, i| {
        hits[i] = try toOpenApiHit(arena, req, hit);
    }

    const graph_results = if (result.graph_results.len > 0)
        try buildGraphQueryResults(arena, req, meta, result)
    else
        null;
    const aggregations = if (meta.aggregation_results.len > 0)
        try buildAggregationResults(arena, req, meta.aggregation_results)
    else
        null;

    const query_results = try arena.alloc(metadata_openapi.QueryResult, 1);
    query_results[0] = .{
        .hits = .{
            .total = result.total_hits,
            .hits = hits,
            .max_score = computeMaxScore(emitted_hits),
        },
        .aggregations = aggregations,
        .graph_results = graph_results,
        .profile = if (req.profile) try buildProfileValue(arena, req, meta, result) else null,
        .took = meta.took_ms,
        .status = 200,
        .table = table_name,
    };

    return .{
        .json = try jsonStringifyAlloc(alloc, metadata_openapi.QueryResponses{
            .responses = query_results,
        }),
    };
}

fn toOpenApiHit(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest, hit: db_mod.types.SearchHit) !metadata_openapi.QueryHit {
    return .{
        ._id = hit.id,
        ._score = hit.score orelse 0,
        ._source = if (hit.stored_data) |stored_data|
            if (req.defer_stored_projection)
                try document_query.projectLookupJsonValue(alloc, stored_data, .{
                    .fields = req.fields,
                    .include_all_fields = req.include_all_fields,
                })
            else
                try parseStoredSourceValue(alloc, stored_data)
        else
            null,
    };
}

fn parseStoredSourceValue(alloc: std.mem.Allocator, stored_data: []const u8) !std.json.Value {
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, stored_data, .{}) catch {
        return .{ .string = try alloc.dupe(u8, stored_data) };
    };
    return parsed.value;
}

fn computeMaxScore(hits: []const db_mod.types.SearchHit) f32 {
    var max_score: f32 = 0;
    for (hits) |hit| {
        const score = hit.score orelse 0;
        if (score > max_score) max_score = score;
    }
    return max_score;
}

pub fn parseAggregationRequestsJson(
    alloc: std.mem.Allocator,
    aggregations_json: []const u8,
) ![]aggregations_mod.SearchAggregationRequest {
    if (aggregations_json.len == 0) return &.{};

    var parsed = std.json.parseFromSlice(std.json.ArrayHashMap(metadata_openapi.AggregationRequest), alloc, aggregations_json, .{}) catch {
        return error.InvalidQueryRequest;
    };
    defer parsed.deinit();
    return try parseAggregationRequestsAlloc(alloc, parsed.value);
}

pub fn freeAggregationRequests(
    alloc: std.mem.Allocator,
    requests: []const aggregations_mod.SearchAggregationRequest,
) void {
    for (requests) |request| {
        alloc.free(request.name);
        alloc.free(request.type);
        alloc.free(request.field);
        for (request.fields) |field| alloc.free(@constCast(field));
        if (request.fields.len > 0) alloc.free(@constCast(request.fields));
        if (request.calendar_interval.len > 0) alloc.free(request.calendar_interval);
        if (request.fixed_interval.len > 0) alloc.free(request.fixed_interval);
        if (request.significance_algorithm.len > 0) alloc.free(request.significance_algorithm);
        if (request.bucket_path.len > 0) alloc.free(request.bucket_path);
        if (request.sort_order.len > 0) alloc.free(request.sort_order);
        if (request.gap_policy.len > 0) alloc.free(request.gap_policy);
        if (request.term_prefix.len > 0) alloc.free(request.term_prefix);
        if (request.term_pattern.len > 0) alloc.free(request.term_pattern);
        if (request.distance_unit.len > 0) alloc.free(request.distance_unit);
        if (request.algebraic_join) |join| {
            alloc.free(join.name);
            if (join.group_side) |side| alloc.free(side);
            if (join.measure_side) |side| alloc.free(side);
        }
        if (request.background_query) |background_query| switch (background_query) {
            .match => |match| {
                alloc.free(match.field);
                alloc.free(match.text);
            },
            .term => |term| {
                alloc.free(term.field);
                alloc.free(term.term);
            },
            .match_all => {},
        };
        for (request.ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
        }
        if (request.ranges.len > 0) alloc.free(request.ranges);
        for (request.date_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
            if (range.start) |value| alloc.free(value);
            if (range.end) |value| alloc.free(value);
        }
        if (request.date_ranges.len > 0) alloc.free(request.date_ranges);
        for (request.distance_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
        }
        if (request.distance_ranges.len > 0) alloc.free(request.distance_ranges);
        freeAggregationRequests(alloc, request.aggregations);
    }
    if (requests.len > 0) alloc.free(requests);
}

fn parseAggregationRequestsAlloc(
    alloc: std.mem.Allocator,
    aggregations: std.json.ArrayHashMap(metadata_openapi.AggregationRequest),
) anyerror![]aggregations_mod.SearchAggregationRequest {
    const out = try alloc.alloc(aggregations_mod.SearchAggregationRequest, aggregations.map.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |request| {
            alloc.free(request.name);
            alloc.free(request.type);
            alloc.free(request.field);
            for (request.fields) |field| alloc.free(@constCast(field));
            if (request.fields.len > 0) alloc.free(@constCast(request.fields));
            if (request.calendar_interval.len > 0) alloc.free(request.calendar_interval);
            if (request.fixed_interval.len > 0) alloc.free(request.fixed_interval);
            if (request.significance_algorithm.len > 0) alloc.free(request.significance_algorithm);
            if (request.bucket_path.len > 0) alloc.free(request.bucket_path);
            if (request.sort_order.len > 0) alloc.free(request.sort_order);
            if (request.gap_policy.len > 0) alloc.free(request.gap_policy);
            if (request.term_prefix.len > 0) alloc.free(request.term_prefix);
            if (request.term_pattern.len > 0) alloc.free(request.term_pattern);
            if (request.distance_unit.len > 0) alloc.free(request.distance_unit);
            if (request.algebraic_join) |join| {
                alloc.free(join.name);
                if (join.group_side) |side| alloc.free(side);
                if (join.measure_side) |side| alloc.free(side);
            }
            if (request.background_query) |background_query| switch (background_query) {
                .match => |match| {
                    alloc.free(match.field);
                    alloc.free(match.text);
                },
                .term => |term| {
                    alloc.free(term.field);
                    alloc.free(term.term);
                },
                .match_all => {},
            };
            for (request.ranges) |range| {
                if (range.name.len > 0) alloc.free(range.name);
            }
            if (request.ranges.len > 0) alloc.free(request.ranges);
            for (request.date_ranges) |range| {
                if (range.name.len > 0) alloc.free(range.name);
                if (range.start) |value| alloc.free(value);
                if (range.end) |value| alloc.free(value);
            }
            if (request.date_ranges.len > 0) alloc.free(request.date_ranges);
            for (request.distance_ranges) |range| {
                if (range.name.len > 0) alloc.free(range.name);
            }
            if (request.distance_ranges.len > 0) alloc.free(request.distance_ranges);
            freeAggregationRequests(alloc, request.aggregations);
        }
        alloc.free(out);
    }

    for (aggregations.map.keys(), aggregations.map.values()) |name, aggregation| {
        out[initialized] = try parseSingleAggregationRequestAlloc(alloc, name, aggregation);
        initialized += 1;
    }
    return out;
}

fn parseSingleAggregationRequestAlloc(
    alloc: std.mem.Allocator,
    name: []const u8,
    aggregation: metadata_openapi.AggregationRequest,
) anyerror!aggregations_mod.SearchAggregationRequest {
    const ranges = try alloc.alloc(aggregations_mod.NumericRangeRequest, if (aggregation.ranges) |ranges_value| ranges_value.len else 0);
    errdefer alloc.free(ranges);
    if (aggregation.ranges) |ranges_value| {
        for (ranges_value, 0..) |range, i| {
            ranges[i] = .{
                .name = if (range.name.len > 0) try alloc.dupe(u8, range.name) else "",
                .start = if (range.from) |value| @floatCast(value) else null,
                .end = if (range.to) |value| @floatCast(value) else null,
            };
        }
    }

    const date_ranges = try alloc.alloc(aggregations_mod.DateRangeRequest, if (aggregation.date_ranges) |ranges_value| ranges_value.len else 0);
    errdefer {
        for (date_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
            if (range.start) |value| alloc.free(value);
            if (range.end) |value| alloc.free(value);
        }
        alloc.free(date_ranges);
    }
    if (aggregation.date_ranges) |ranges_value| {
        for (ranges_value, 0..) |range, i| {
            date_ranges[i] = .{
                .name = if (range.name.len > 0) try alloc.dupe(u8, range.name) else "",
                .start = if (range.from) |value| try alloc.dupe(u8, value) else null,
                .end = if (range.to) |value| try alloc.dupe(u8, value) else null,
            };
        }
    }

    const distance_ranges = try alloc.alloc(aggregations_mod.DistanceRangeRequest, if (aggregation.distance_ranges) |ranges_value| ranges_value.len else 0);
    errdefer {
        for (distance_ranges) |range| {
            if (range.name.len > 0) alloc.free(range.name);
        }
        alloc.free(distance_ranges);
    }
    if (aggregation.distance_ranges) |ranges_value| {
        for (ranges_value, 0..) |range, i| {
            distance_ranges[i] = .{
                .name = if (range.name.len > 0) try alloc.dupe(u8, range.name) else "",
                .from = if (range.from) |value| @floatCast(value) else null,
                .to = if (range.to) |value| @floatCast(value) else null,
            };
        }
    }

    const nested = if (aggregation.sub_aggregations) |value|
        try parseAggregationRequestsAlloc(alloc, value)
    else
        &.{};
    errdefer freeAggregationRequests(alloc, nested);
    const fields = if (aggregation.fields) |values| blk: {
        const cloned = try alloc.alloc([]const u8, values.len);
        var initialized_fields: usize = 0;
        errdefer {
            for (cloned[0..initialized_fields]) |field| alloc.free(@constCast(field));
            alloc.free(cloned);
        }
        for (values, 0..) |field, i| {
            if (field.len == 0) return error.InvalidQueryRequest;
            cloned[i] = try alloc.dupe(u8, field);
            initialized_fields += 1;
        }
        break :blk cloned;
    } else &.{};
    errdefer {
        for (fields) |field| alloc.free(@constCast(field));
        if (fields.len > 0) alloc.free(@constCast(fields));
    }
    if (fields.len > 0) {
        if (aggregation.type != .terms) return error.InvalidQueryRequest;
        if (aggregation.field) |field| {
            if (!std.mem.eql(u8, field, fields[0])) return error.InvalidQueryRequest;
        }
    }
    const primary_field = if (fields.len > 0) fields[0] else aggregation.field orelse return error.InvalidQueryRequest;
    const algebraic_join = if (aggregation.algebraic_join) |join|
        try parseAlgebraicAggregationJoinAlloc(alloc, join)
    else
        null;
    errdefer if (algebraic_join) |join| {
        alloc.free(join.name);
        if (join.group_side) |side| alloc.free(side);
        if (join.measure_side) |side| alloc.free(side);
    };

    var center_lat: f64 = 0;
    var center_lon: f64 = 0;
    if (aggregation.origin) |origin| {
        var it = std.mem.splitScalar(u8, origin, ',');
        const lat_text = it.next() orelse return error.InvalidQueryRequest;
        const lon_text = it.next() orelse return error.InvalidQueryRequest;
        if (it.next() != null) return error.InvalidQueryRequest;
        center_lat = std.fmt.parseFloat(f64, std.mem.trim(u8, lat_text, &std.ascii.whitespace)) catch return error.InvalidQueryRequest;
        center_lon = std.fmt.parseFloat(f64, std.mem.trim(u8, lon_text, &std.ascii.whitespace)) catch return error.InvalidQueryRequest;
    }

    return .{
        .name = try alloc.dupe(u8, name),
        .type = try alloc.dupe(u8, @tagName(aggregation.type)),
        .field = try alloc.dupe(u8, primary_field),
        .fields = fields,
        .size = aggregation.size orelse 0,
        .interval = if (aggregation.interval) |value| value else 0,
        .calendar_interval = if (aggregation.calendar_interval) |value|
            try jsonValueToFlatStringAlloc(alloc, value)
        else
            "",
        .min_doc_count = aggregation.min_doc_count orelse 0,
        .significance_algorithm = if (aggregation.algorithm) |value|
            try jsonValueToFlatStringAlloc(alloc, value)
        else
            "",
        .background_query = if (aggregation.background_filter) |value|
            try parseAggregationBackgroundQueryAlloc(alloc, value)
        else
            null,
        .ranges = ranges,
        .date_ranges = date_ranges,
        .distance_ranges = distance_ranges,
        .center_lat = center_lat,
        .center_lon = center_lon,
        .distance_unit = if (aggregation.unit) |value|
            try jsonValueToFlatStringAlloc(alloc, value)
        else
            "",
        .geohash_precision = if (aggregation.precision) |value|
            std.math.cast(u8, value) orelse return error.InvalidQueryRequest
        else
            0,
        .algebraic_join = algebraic_join,
        .aggregations = nested,
    };
}

fn parseAlgebraicAggregationJoinAlloc(
    alloc: std.mem.Allocator,
    join: metadata_openapi.AlgebraicAggregationJoin,
) !db_mod.algebraic.ir.JoinRef {
    if (join.name.len == 0 or join.group_side.len == 0 or join.measure_side.len == 0) return error.InvalidQueryRequest;
    const kind: db_mod.algebraic.join.TemporalMode = if (join.kind) |value| blk: {
        if (std.mem.eql(u8, value, "none")) break :blk .none;
        if (std.mem.eql(u8, value, "bucket")) break :blk .bucket;
        if (std.mem.eql(u8, value, "window")) break :blk .window;
        if (std.mem.eql(u8, value, "bucket_window")) break :blk .bucket_window;
        return error.InvalidQueryRequest;
    } else .none;
    return .{
        .name = try alloc.dupe(u8, join.name),
        .kind = kind,
        .group_side = try alloc.dupe(u8, join.group_side),
        .measure_side = try alloc.dupe(u8, join.measure_side),
    };
}

fn jsonValueToFlatStringAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        else => return error.UnsupportedQueryRequest,
    };
}

fn parseAggregationBackgroundQueryAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !aggregations_mod.BackgroundQuery {
    if (value == .object) {
        if (value.object.get("match_all") != null) return .{ .match_all = {} };
        if (value.object.get("match")) |match| {
            if (match == .object and match.object.count() == 1) {
                var it = match.object.iterator();
                const entry = it.next() orelse return error.UnsupportedQueryRequest;
                if (entry.value_ptr.* != .string) return error.UnsupportedQueryRequest;
                return .{ .match = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .text = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
        if (value.object.get("term")) |term| {
            if (term == .object and term.object.count() == 1) {
                var it = term.object.iterator();
                const entry = it.next() orelse return error.UnsupportedQueryRequest;
                if (entry.value_ptr.* != .string) return error.UnsupportedQueryRequest;
                return .{ .term = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .term = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
    }
    return error.UnsupportedQueryRequest;
}

fn buildAggregationResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    results: []const aggregations_mod.SearchAggregationResult,
) !std.json.ArrayHashMap(metadata_openapi.AggregationResult) {
    var out: std.json.ArrayHashMap(metadata_openapi.AggregationResult) = .{};
    errdefer out.deinit(alloc);

    var parsed_request_map: ?std.json.Parsed(std.json.ArrayHashMap(metadata_openapi.AggregationRequest)) = null;
    defer if (parsed_request_map) |*parsed| parsed.deinit();
    if (req.aggregations_json.len > 0) {
        parsed_request_map = std.json.parseFromSlice(std.json.ArrayHashMap(metadata_openapi.AggregationRequest), alloc, req.aggregations_json, .{}) catch {
            return error.InvalidQueryRequest;
        };
    }

    for (results) |result| {
        const request = if (parsed_request_map) |*parsed|
            parsed.value.map.get(result.name)
        else
            null;
        try out.map.put(alloc, result.name, try toOpenApiAggregationResult(alloc, request, result));
    }
    return out;
}

fn toOpenApiAggregationResult(
    alloc: std.mem.Allocator,
    request: ?metadata_openapi.AggregationRequest,
    result: aggregations_mod.SearchAggregationResult,
) anyerror!metadata_openapi.AggregationResult {
    var out: metadata_openapi.AggregationResult = .{};

    if (result.value_json) |value_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
        defer parsed.deinit();
        switch (parsed.value) {
            .float => |value| out.value = @floatCast(value),
            .integer => |value| out.value = @floatFromInt(value),
            .object => |object| {
                if (object.get("value")) |value| out.value = try jsonValueToF32(value);
                if (object.get("count")) |value| out.count = try jsonValueToI64(value);
                if (object.get("min")) |value| out.min = try jsonValueToF32(value);
                if (object.get("max")) |value| out.max = try jsonValueToF32(value);
                if (object.get("sum")) |value| out.sum = try jsonValueToF32(value);
                if (object.get("sum_squares")) |value| out.sum_of_squares = try jsonValueToF32(value);
                if (object.get("avg")) |value| out.avg = try jsonValueToF32(value);
                if (object.get("variance")) |value| out.variance = try jsonValueToF32(value);
                if (object.get("std_dev")) |value| out.std_deviation = try jsonValueToF32(value);
            },
            else => {},
        }
    }

    if (result.buckets.len > 0) {
        const buckets = try alloc.alloc(metadata_openapi.AggregationBucket, result.buckets.len);
        for (result.buckets, 0..) |bucket, idx| {
            buckets[idx] = try toOpenApiAggregationBucket(alloc, request, idx, bucket);
        }
        out.buckets = buckets;
    }

    return out;
}

fn toOpenApiAggregationBucket(
    alloc: std.mem.Allocator,
    request: ?metadata_openapi.AggregationRequest,
    idx: usize,
    bucket: aggregations_mod.SearchAggregationBucket,
) anyerror!metadata_openapi.AggregationBucket {
    var parsed_key = try std.json.parseFromSlice(std.json.Value, alloc, bucket.key_json, .{});
    defer parsed_key.deinit();

    var out: metadata_openapi.AggregationBucket = .{
        .key = try jsonValueToBucketKeyAlloc(alloc, parsed_key.value),
        .doc_count = bucket.count,
        .score = if (bucket.score) |value| @floatCast(value) else null,
        .bg_count = bucket.bg_count,
    };

    switch (parsed_key.value) {
        .float, .integer => {
            out.key_as_string = out.key;
        },
        else => {},
    }

    if (request) |aggregation_request| {
        if (aggregation_request.ranges) |ranges| {
            if (idx < ranges.len) {
                out.from = if (ranges[idx].from) |value| @floatCast(value) else null;
                out.to = if (ranges[idx].to) |value| @floatCast(value) else null;
                if (ranges[idx].from) |value| out.from_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
                if (ranges[idx].to) |value| out.to_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
            }
        } else if (aggregation_request.date_ranges) |ranges| {
            if (idx < ranges.len) {
                out.from_as_string = if (ranges[idx].from) |value| try alloc.dupe(u8, value) else null;
                out.to_as_string = if (ranges[idx].to) |value| try alloc.dupe(u8, value) else null;
            }
        } else if (aggregation_request.distance_ranges) |ranges| {
            if (idx < ranges.len) {
                out.from = if (ranges[idx].from) |value| @floatCast(value) else null;
                out.to = if (ranges[idx].to) |value| @floatCast(value) else null;
                if (ranges[idx].from) |value| out.from_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
                if (ranges[idx].to) |value| out.to_as_string = try std.fmt.allocPrint(alloc, "{d}", .{value});
            }
        }
    }

    if (bucket.aggregations.len > 0) {
        const sub_requests = if (request) |aggregation_request|
            aggregation_request.sub_aggregations
        else
            null;
        var sub_out: std.json.ArrayHashMap(metadata_openapi.AggregationResult) = .{};
        errdefer sub_out.deinit(alloc);
        for (bucket.aggregations) |aggregation| {
            const sub_request = if (sub_requests) |requests| requests.map.get(aggregation.name) else null;
            try sub_out.map.put(alloc, aggregation.name, try toOpenApiAggregationResult(alloc, sub_request, aggregation));
        }
        out.sub_aggregations = sub_out;
    }

    return out;
}

fn jsonValueToBucketKeyAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        else => try jsonStringifyAlloc(alloc, value),
    };
}

fn jsonValueToF32(value: std.json.Value) !?f32 {
    return switch (value) {
        .null => null,
        .integer => @floatFromInt(value.integer),
        .float => @floatCast(value.float),
        else => error.InvalidQueryRequest,
    };
}

fn jsonValueToI64(value: std.json.Value) !?i64 {
    return switch (value) {
        .null => null,
        .integer => value.integer,
        .float => @intFromFloat(value.float),
        else => error.InvalidQueryRequest,
    };
}

fn buildGraphQueryResults(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    meta: QueryResponseMeta,
    result: db_mod.types.SearchResult,
) !std.json.ArrayHashMap(indexes_openapi.GraphQueryResult) {
    var out: std.json.ArrayHashMap(indexes_openapi.GraphQueryResult) = .{};
    errdefer out.deinit(alloc);

    for (result.graph_results) |graph_result| {
        const query_type = findGraphQueryType(req.graph_queries, graph_result.name) orelse continue;
        std.log.info(
            "encode graph result name={s} type={s} total={d} nodes={d} paths={d} matches={d} hits={d}",
            .{
                graph_result.name,
                @tagName(query_type),
                graph_result.total_hits,
                graph_result.nodes.len,
                graph_result.paths.len,
                graph_result.matches.len,
                graph_result.hits.len,
            },
        );
        try out.map.put(alloc, graph_result.name, try toOpenApiGraphQueryResult(alloc, query_type, meta, graph_result));
    }
    return out;
}

fn findGraphQueryType(
    graph_queries: []const db_mod.types.NamedGraphQuery,
    name: []const u8,
) ?indexes_openapi.GraphQueryType {
    for (graph_queries) |graph_query| {
        if (!std.mem.eql(u8, graph_query.name, name)) continue;
        return switch (graph_query.query.query_type) {
            .traverse => .traverse,
            .neighbors => .neighbors,
            .shortest_path => .shortest_path,
            .k_shortest_paths => .k_shortest_paths,
            .pattern => .pattern,
        };
    }
    return null;
}

fn toOpenApiGraphQueryResult(
    alloc: std.mem.Allocator,
    query_type: indexes_openapi.GraphQueryType,
    meta: QueryResponseMeta,
    graph_result: db_mod.types.GraphSearchResult,
) !indexes_openapi.GraphQueryResult {
    return .{
        .type = query_type,
        .nodes = try toOpenApiGraphNodes(alloc, graph_result),
        .paths = try toOpenApiPaths(alloc, graph_result.paths),
        .matches = try toOpenApiPatternMatches(alloc, graph_result),
        .total = @intCast(graph_result.total_hits),
        .took = meta.took_ms,
    };
}

fn toOpenApiPatternMatches(
    alloc: std.mem.Allocator,
    graph_result: db_mod.types.GraphSearchResult,
) !?[]const indexes_openapi.PatternMatch {
    if (graph_result.matches.len == 0) return null;
    const out = try alloc.alloc(indexes_openapi.PatternMatch, graph_result.matches.len);
    for (graph_result.matches, 0..) |match, i| {
        var bindings: std.json.ArrayHashMap(indexes_openapi.GraphResultNode) = .{};
        errdefer bindings.deinit(alloc);
        for (match.bindings) |binding| {
            try bindings.map.put(alloc, binding.alias, .{
                .key = binding.node.key,
                .depth = @intCast(binding.node.depth),
                .distance = binding.node.distance,
                .document = findGraphDocument(alloc, graph_result.hits, binding.node.key),
                .path = binding.node.path,
                .path_edges = try toOpenApiOptionalPathEdges(alloc, binding.node.path_edges),
                .provenance = binding.node.provenance,
                .edges = null,
            });
        }
        out[i] = .{
            .bindings = bindings,
            .path = try toOpenApiPathEdges(alloc, match.path),
        };
    }
    return out;
}

fn toOpenApiGraphNodes(
    alloc: std.mem.Allocator,
    graph_result: db_mod.types.GraphSearchResult,
) ![]const indexes_openapi.GraphResultNode {
    const nodes = try alloc.alloc(indexes_openapi.GraphResultNode, graph_result.nodes.len);
    for (graph_result.nodes, 0..) |node, i| {
        nodes[i] = .{
            .key = node.key,
            .depth = @intCast(node.depth),
            .distance = node.distance,
            .document = findGraphDocument(alloc, graph_result.hits, node.key),
            .path = node.path,
            .path_edges = try toOpenApiOptionalPathEdges(alloc, node.path_edges),
            .provenance = node.provenance,
            .edges = null,
        };
    }
    return nodes;
}

fn findGraphDocument(
    alloc: std.mem.Allocator,
    hits: []const db_mod.types.SearchHit,
    key: []const u8,
) ?std.json.Value {
    for (hits) |hit| {
        if (!std.mem.eql(u8, hit.id, key)) continue;
        const stored_data = hit.stored_data orelse return null;
        const parsed = std.json.parseFromSlice(std.json.Value, alloc, stored_data, .{}) catch return null;
        return parsed.value;
    }
    return null;
}

fn toOpenApiPaths(
    alloc: std.mem.Allocator,
    paths: []const db_mod.types.GraphPath,
) ![]const indexes_openapi.Path {
    const out = try alloc.alloc(indexes_openapi.Path, paths.len);
    for (paths, 0..) |path, i| {
        out[i] = .{
            .nodes = path.nodes,
            .edges = try toOpenApiPathEdges(alloc, path.edges),
            .total_weight = path.total_weight,
            .length = @intCast(path.length),
        };
    }
    return out;
}

fn toOpenApiPathEdges(
    alloc: std.mem.Allocator,
    edges: anytype,
) ![]const indexes_openapi.PathEdge {
    const out = try alloc.alloc(indexes_openapi.PathEdge, edges.len);
    for (edges, 0..) |edge, i| {
        out[i] = .{
            .source = edge.source,
            .target = edge.target,
            .type = edge.edge_type,
            .weight = edge.weight,
        };
    }
    return out;
}

fn toOpenApiOptionalPathEdges(
    alloc: std.mem.Allocator,
    edges: ?[]const graph_query_mod.PathEdgeInfo,
) !?[]const indexes_openapi.PathEdge {
    const value = edges orelse return null;
    return try toOpenApiPathEdges(alloc, value);
}

test "api query contract preserves algebraic graph path provenance" {
    const alloc = std.testing.allocator;
    const path_nodes: []const []const u8 = &.{ "A", "B", "C" };
    const path_edges: []const graph_query_mod.PathEdgeInfo = &.{
        .{ .source = "A", .target = "B", .edge_type = "e", .weight = 2.0 },
        .{ .source = "B", .target = "C", .edge_type = "e", .weight = 3.0 },
    };
    const provenance: []const []const u8 = &.{ "A\x1fe\x1fB", "B\x1fe\x1fC" };
    const nodes: []const graph_query_mod.GraphResultNode = &.{.{
        .key = "C",
        .depth = 2,
        .distance = 2.0,
        .path = path_nodes,
        .path_edges = path_edges,
        .provenance = provenance,
    }};
    const graph_result = db_mod.types.GraphSearchResult{
        .name = @constCast("shortest"),
        .nodes = @constCast(nodes),
        .paths = &.{},
        .matches = &.{},
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 1,
    };

    const encoded = try toOpenApiGraphNodes(alloc, graph_result);
    defer {
        if (encoded[0].path_edges) |items| alloc.free(items);
        alloc.free(encoded);
    }

    try std.testing.expectEqual(@as(usize, 1), encoded.len);
    try std.testing.expectEqualStrings("C", encoded[0].key);
    try std.testing.expectEqual(@as(i64, 2), encoded[0].depth.?);
    try std.testing.expectEqual(@as(f64, 2.0), encoded[0].distance.?);
    try std.testing.expectEqualStrings("A", encoded[0].path.?[0]);
    try std.testing.expectEqualStrings("C", encoded[0].path.?[2]);
    try std.testing.expectEqual(@as(usize, 2), encoded[0].path_edges.?.len);
    try std.testing.expectEqualStrings("e", encoded[0].path_edges.?[0].type.?);
    try std.testing.expectEqual(@as(f64, 3.0), encoded[0].path_edges.?[1].weight.?);
    try std.testing.expectEqual(@as(usize, 2), encoded[0].provenance.?.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", encoded[0].provenance.?[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", encoded[0].provenance.?[1]);
}

fn buildProfileValue(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    meta: QueryResponseMeta,
    result: db_mod.types.SearchResult,
) !std.json.Value {
    const profile = metadata_openapi.QueryProfile{
        .shards = .{
            .total = meta.shard_count,
            .successful = meta.shard_count,
            .failed = 0,
        },
        .reranker = if (meta.reranker) |reranker| .{
            .model = if (reranker.model.len > 0) reranker.model else null,
            .documents_reranked = reranker.documents_reranked,
            .duration_ms = reranker.duration_ms,
        } else null,
        .merge = if (meta.merge) |merge| .{
            .strategy = merge.strategy,
            .full_text_hits = merge.full_text_hits,
            .semantic_hits = merge.semantic_hits,
            .duration_ms = merge.duration_ms,
        } else if (req.merge_config != null or meta.merged) .{
            .strategy = if (req.merge_config) |merge_config| switch (merge_config.strategy) {
                .rrf => .rrf,
                .rsf => .rsf,
            } else .rrf,
            .full_text_hits = result.total_hits,
            .semantic_hits = if (req.dense_queries.len > 0 or req.sparse_queries.len > 0) result.total_hits else 0,
            .duration_ms = meta.took_ms,
        } else null,
    };
    const encoded = try jsonStringifyAlloc(alloc, profile);
    defer alloc.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    if (meta.dense_search) |dense_search| {
        if (parsed.value != .object) return error.InvalidQueryRequest;
        const dense_json = try jsonStringifyAlloc(alloc, dense_search);
        defer alloc.free(dense_json);
        const dense_parsed = try std.json.parseFromSlice(std.json.Value, alloc, dense_json, .{});
        try parsed.value.object.put(alloc, "dense_search", dense_parsed.value);
    }
    return parsed.value;
}

fn parseMergeConfig(alloc: std.mem.Allocator, generated: indexes_openapi.MergeConfig) !db_mod.types.MergeConfig {
    var config = db_mod.types.MergeConfig{};
    if (generated.strategy) |strategy| {
        config.strategy = switch (strategy) {
            .rrf => .rrf,
            .rsf => .rsf,
            .failover => return error.UnsupportedQueryRequest,
        };
    }
    if (generated.rank_constant) |rank_constant| config.rank_constant = rank_constant;
    if (generated.window_size) |window_size| {
        config.window_size = std.math.cast(u32, window_size) orelse return error.InvalidQueryRequest;
    }
    if (generated.weights) |weights| {
        var named = try alloc.alloc(fusion_mod.NamedWeight, weights.map.count());
        var initialized: usize = 0;
        errdefer {
            for (named[0..initialized]) |item| alloc.free(item.name);
            alloc.free(named);
        }
        for (weights.map.keys(), weights.map.values()) |name, weight| {
            named[initialized] = .{
                .name = try alloc.dupe(u8, name),
                .weight = weight,
            };
            initialized += 1;
        }
        config.weights = named;
    }
    return config;
}

fn parsePruner(generated: indexes_openapi.Pruner) !fusion_mod.Pruner {
    return .{
        .min_score_ratio = generated.min_score_ratio orelse 0.0,
        .max_score_gap_percent = generated.max_score_gap_percent orelse 0.0,
        .min_absolute_score = generated.min_absolute_score orelse 0.0,
        .require_multi_index = generated.require_multi_index orelse false,
        .std_dev_threshold = generated.std_dev_threshold orelse 0.0,
    };
}

fn buildRerankerQueryText(alloc: std.mem.Allocator, request: metadata_openapi.QueryRequest) ![]const u8 {
    if (request.semantic_search) |semantic_search| {
        return try alloc.dupe(u8, semantic_search);
    }
    if (request.full_text_search) |full_text_search| {
        return try buildRerankerQueryTextFromValue(alloc, full_text_search);
    }
    if (request.query) |query| {
        return try buildRerankerQueryTextFromValue(alloc, query);
    }
    return error.UnsupportedQueryRequest;
}

fn buildRerankerQueryTextFromValue(alloc: std.mem.Allocator, value: std.json.Value) ![]const u8 {
    if (value == .string) return try alloc.dupe(u8, value.string);
    if (value != .object) return try jsonStringifyAlloc(alloc, value);

    if (value.object.get("query")) |query| {
        if (query == .string) return try alloc.dupe(u8, query.string);
    }
    if (value.object.get("match")) |match| {
        if (match == .string) return try alloc.dupe(u8, match.string);
        if (match == .object) {
            if (match.object.get("text")) |text| {
                if (text == .string) return try alloc.dupe(u8, text.string);
            }
            if (match.object.get("match")) |text| {
                if (text == .string) return try alloc.dupe(u8, text.string);
            }
            if (match.object.count() == 1) {
                var it = match.object.iterator();
                if (it.next()) |entry| {
                    if (entry.value_ptr.* == .string) return try alloc.dupe(u8, entry.value_ptr.string);
                }
            }
        }
    }
    if (value.object.get("term")) |term| {
        if (term == .string) return try alloc.dupe(u8, term.string);
        if (term == .object and term.object.count() == 1) {
            var it = term.object.iterator();
            if (it.next()) |entry| {
                if (entry.value_ptr.* == .string) return try alloc.dupe(u8, entry.value_ptr.string);
            }
        }
    }
    return try jsonStringifyAlloc(alloc, value);
}

fn cloneFields(alloc: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, value.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (value) |item| {
        fields[initialized] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return fields;
}

fn canDeferStoredProjection(fields: []const []const u8) bool {
    if (fields.len == 0) return false;
    for (fields) |field| {
        if (std.mem.eql(u8, field, "_chunks") or std.mem.eql(u8, field, "_chunks.*")) return false;
        if (std.mem.eql(u8, field, "_embeddings") or std.mem.eql(u8, field, "_embeddings.*")) return false;
    }
    return true;
}

const NormalizedPublicQueryBuckets = struct {
    full_text: ?db_mod.types.TextQuery = null,
    filter_query_json: []const u8 = "",
    exclusion_query_json: []const u8 = "",

    fn deinit(self: *NormalizedPublicQueryBuckets, alloc: std.mem.Allocator) void {
        if (self.full_text) |query| freeTextQuery(alloc, query);
        if (self.filter_query_json.len > 0) alloc.free(@constCast(self.filter_query_json));
        if (self.exclusion_query_json.len > 0) alloc.free(@constCast(self.exclusion_query_json));
        self.* = .{};
    }
};

fn normalizePublicQueryBucketsAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
    limit: u32,
) !NormalizedPublicQueryBuckets {
    var scoring_must = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &scoring_must);
    var scoring_should = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &scoring_should);
    var filter_clauses = std.ArrayListUnmanaged([]u8).empty;
    errdefer deinitOwnedStringArrayList(alloc, &filter_clauses);
    var exclusion_clauses = std.ArrayListUnmanaged([]u8).empty;
    errdefer deinitOwnedStringArrayList(alloc, &exclusion_clauses);

    if (request.query) |query| {
        try appendCanonicalPublicQueryAlloc(alloc, query, limit, &scoring_must, &scoring_should, &filter_clauses, &exclusion_clauses);
    }
    if (request.full_text_search) |full_text_search| {
        try appendScoringQueryClausesAlloc(alloc, &scoring_must, full_text_search, limit);
    }
    if (request.filter_query) |filter_query| {
        try appendPublicFilterClausesAlloc(alloc, &filter_clauses, filter_query, limit);
    }
    if (request.exclusion_query) |exclusion_query| {
        try appendPublicFilterClausesAlloc(alloc, &exclusion_clauses, exclusion_query, limit);
    }

    var full_text = try buildScoringTextQueryAlloc(alloc, &scoring_must, &scoring_should);
    errdefer if (full_text) |query| freeTextQuery(alloc, query);
    deinitTextQueryArrayList(alloc, &scoring_must);
    deinitTextQueryArrayList(alloc, &scoring_should);

    const filter_query_json = try buildStructuredFilterClausesJsonAlloc(alloc, filter_clauses.items, .all);
    errdefer if (filter_query_json.len > 0) alloc.free(filter_query_json);
    const exclusion_query_json = try buildStructuredFilterClausesJsonAlloc(alloc, exclusion_clauses.items, .any);
    errdefer if (exclusion_query_json.len > 0) alloc.free(exclusion_query_json);

    deinitOwnedStringArrayList(alloc, &filter_clauses);
    deinitOwnedStringArrayList(alloc, &exclusion_clauses);

    const out = NormalizedPublicQueryBuckets{
        .full_text = full_text,
        .filter_query_json = filter_query_json,
        .exclusion_query_json = exclusion_query_json,
    };
    full_text = null;
    return out;
}

fn appendCanonicalPublicQueryAlloc(
    alloc: std.mem.Allocator,
    query: std.json.Value,
    limit: u32,
    scoring_must: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    scoring_should: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    filter_clauses: *std.ArrayListUnmanaged([]u8),
    exclusion_clauses: *std.ArrayListUnmanaged([]u8),
) !void {
    if (query == .object) {
        if (query.object.get("bool")) |bool_value| {
            if (bool_value != .object) return error.InvalidQueryRequest;
            if (bool_value.object.get("must")) |must_value| {
                try appendScoringQueryClausesAlloc(alloc, scoring_must, must_value, limit);
            }
            if (bool_value.object.get("should")) |should_value| {
                try appendScoringQueryClausesAlloc(alloc, scoring_should, should_value, limit);
            }
            if (bool_value.object.get("filter")) |filter_value| {
                try appendRawStructuredFilterClausesAlloc(alloc, filter_clauses, filter_value);
            }
            if (bool_value.object.get("must_not")) |must_not_value| {
                try appendRawStructuredFilterClausesAlloc(alloc, exclusion_clauses, must_not_value);
            }
            return;
        }
    }

    appendScoringQueryClausesAlloc(alloc, scoring_must, query, limit) catch |err| switch (err) {
        error.UnsupportedQueryRequest, error.InvalidQueryRequest => {
            if (!isStructuredFilterValue(query)) return err;
            try appendRawStructuredFilterClausesAlloc(alloc, filter_clauses, query);
        },
        else => return err,
    };
}

fn appendScoringQueryClausesAlloc(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    query_or_queries: std.json.Value,
    limit: u32,
) !void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.InvalidQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendScoringQueryClausesAlloc(alloc, list, item, limit);
        }
        return;
    }

    const parsed = try parseSupportedFullTextQuery(alloc, query_or_queries, limit);
    errdefer freeTextQuery(alloc, parsed);
    try list.append(alloc, parsed);
}

fn appendPublicFilterClausesAlloc(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]u8),
    query_or_queries: std.json.Value,
    limit: u32,
) !void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.InvalidQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendPublicFilterClausesAlloc(alloc, list, item, limit);
        }
        return;
    }
    if (isStructuredFilterValue(query_or_queries) and
        !isQueryStringValue(query_or_queries) and
        !isPublicScalarOperatorFilterValue(query_or_queries))
    {
        try appendRawStructuredFilterClausesAlloc(alloc, list, query_or_queries);
        return;
    }

    const parsed = try parseSupportedFullTextQuery(alloc, query_or_queries, limit);
    defer freeTextQuery(alloc, parsed);
    const encoded = try encodePatternFilterQuery(alloc, parsed);
    errdefer alloc.free(encoded);
    try list.append(alloc, encoded);
}

fn appendRawStructuredFilterClausesAlloc(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged([]u8),
    query_or_queries: std.json.Value,
) !void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.InvalidQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendRawStructuredFilterClausesAlloc(alloc, list, item);
        }
        return;
    }
    if (!isStructuredFilterValue(query_or_queries)) return error.UnsupportedQueryRequest;
    const encoded = try jsonStringifyAlloc(alloc, query_or_queries);
    errdefer alloc.free(encoded);
    try list.append(alloc, encoded);
}

fn isStructuredFilterValue(value: std.json.Value) bool {
    if (value != .object) return false;
    inline for ([_][]const u8{
        "match_all",
        "match_none",
        "term",
        "terms",
        "exists",
        "match",
        "prefix",
        "wildcard",
        "regexp",
        "fuzzy",
        "range",
        "numeric_range",
        "term_range",
        "date_range",
        "bool_field",
        "ip_range",
        "geo_distance",
        "geo_bbox",
        "geo_shape",
        "ids",
        "doc_id",
        "doc_ids",
        "docids",
        "conjuncts",
        "disjuncts",
        "bool",
    }) |key| {
        if (value.object.get(key) != null) return true;
    }
    return false;
}

fn isQueryStringValue(value: std.json.Value) bool {
    if (value != .object) return false;
    return value.object.get("query") != null;
}

fn isPublicScalarOperatorFilterValue(value: std.json.Value) bool {
    if (value != .object or directDslFieldValue(value.object) == null) return false;
    inline for ([_][]const u8{ "term", "match", "prefix", "wildcard", "regexp", "fuzzy" }) |key| {
        if (value.object.get(key)) |operator_value| {
            return operator_value == .string;
        }
    }
    return false;
}

fn buildScoringTextQueryAlloc(
    alloc: std.mem.Allocator,
    must: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    should: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
) !?db_mod.types.TextQuery {
    if (must.items.len == 0 and should.items.len == 0) return null;

    const owned_must = try alloc.dupe(db_mod.types.TextQuery, must.items);
    must.clearRetainingCapacity();
    errdefer {
        freeTextQueryList(alloc, owned_must);
    }
    const owned_should = try alloc.dupe(db_mod.types.TextQuery, should.items);
    should.clearRetainingCapacity();
    errdefer {
        freeTextQueryList(alloc, owned_should);
    }

    if (owned_must.len == 1 and owned_should.len == 0) {
        const out = owned_must[0];
        alloc.free(owned_must);
        return out;
    }

    return .{ .bool_query = .{
        .must = owned_must,
        .should = owned_should,
        .min_should = if (owned_should.len > 0 and owned_must.len == 0) 1 else 0,
    } };
}

const StructuredClauseMode = enum {
    all,
    any,
};

fn buildStructuredFilterClausesJsonAlloc(
    alloc: std.mem.Allocator,
    clauses: []const []const u8,
    mode: StructuredClauseMode,
) ![]u8 {
    if (clauses.len == 0) return "";
    if (clauses.len == 1) return try alloc.dupe(u8, clauses[0]);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    switch (mode) {
        .all => try out.appendSlice(alloc, "{\"bool\":{\"must\":["),
        .any => try out.appendSlice(alloc, "{\"bool\":{\"should\":["),
    }
    for (clauses, 0..) |clause, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, clause);
    }
    switch (mode) {
        .all => try out.appendSlice(alloc, "]}}"),
        .any => try out.appendSlice(alloc, "],\"minimum_should_match\":1}}"),
    }
    return try out.toOwnedSlice(alloc);
}

fn deinitOwnedStringArrayList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged([]u8)) void {
    for (list.items) |item| alloc.free(item);
    list.deinit(alloc);
    list.* = .empty;
}

fn parseSupportedFullTextQuery(alloc: std.mem.Allocator, query: std.json.Value, limit: u32) !db_mod.types.TextQuery {
    if (query != .object) return error.InvalidQueryRequest;
    if (query.object.get("dense_knn") != null) {
        return error.UnsupportedQueryRequest;
    } else if (query.object.get("sparse_knn") != null) {
        return error.UnsupportedQueryRequest;
    }
    _ = limit;
    if (try parseDirectDslTextQuery(alloc, query)) |direct| return direct;
    return try parseGeneratedBleveTextQuery(alloc, query);
}

fn parseDirectDslTextQuery(alloc: std.mem.Allocator, query: std.json.Value) anyerror!?db_mod.types.TextQuery {
    if (query != .object) return null;

    if (query.object.get("match_all") != null) {
        return .{ .match_all = {} };
    }

    if (query.object.get("conjuncts")) |conjuncts| {
        return .{ .bool_query = .{
            .must = try parseDirectDslTextQueryArrayAlloc(alloc, conjuncts),
        } };
    }

    if (query.object.get("disjuncts")) |disjuncts| {
        return .{ .bool_query = .{
            .should = try parseDirectDslTextQueryArrayAlloc(alloc, disjuncts),
        } };
    }

    if (query.object.get("must_not")) |must_not| {
        return .{ .bool_query = .{
            .must_not = try parseDirectDslTextQueryListAlloc(alloc, must_not),
        } };
    }

    if (query.object.get("bool")) |bool_query| {
        return try parseDirectDslBoolTextQuery(alloc, bool_query);
    }

    if (public_text_query_mod.parseStatefulDirectTextOperatorQueryAlloc(alloc, query, 1.0)) |maybe_direct| {
        if (maybe_direct) |direct| return direct;
    } else |err| return err;

    if (try parseDirectDslDateRangeQueryAlloc(alloc, query)) |date_range| return date_range;

    if (public_text_query_mod.parseStatefulDirectTextRangeQueryAlloc(alloc, query, 1.0)) |maybe_direct| {
        if (maybe_direct) |direct| return direct;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {},
        else => return err,
    }

    return null;
}

fn directDslFieldValue(object: std.json.ObjectMap) ?std.json.Value {
    return object.get("field") orelse object.get("path");
}

fn parseDirectDslDateRangeQueryAlloc(alloc: std.mem.Allocator, query: std.json.Value) !?db_mod.types.TextQuery {
    if (query != .object) return null;
    if (query.object.get("start") == null and query.object.get("end") == null) return null;
    if (query.object.get("min") != null or query.object.get("max") != null) return error.UnsupportedQueryRequest;
    const field = directDslFieldValue(query.object) orelse return error.UnsupportedQueryRequest;
    if (field != .string) return error.UnsupportedQueryRequest;
    const start_ns = if (query.object.get("start")) |start| blk: {
        if (start != .string) return error.UnsupportedQueryRequest;
        break :blk (try parseDateTimeOptionalToNs(start.string)) orelse return error.UnsupportedQueryRequest;
    } else null;
    const end_ns = if (query.object.get("end")) |end| blk: {
        if (end != .string) return error.UnsupportedQueryRequest;
        break :blk (try parseDateTimeOptionalToNs(end.string)) orelse return error.UnsupportedQueryRequest;
    } else null;
    return .{ .date_range = .{
        .field = try alloc.dupe(u8, field.string),
        .start_ns = start_ns,
        .end_ns = end_ns,
        .inclusive_start = if (query.object.get("inclusive_start")) |inclusive_start| switch (inclusive_start) {
            .bool => inclusive_start.bool,
            else => return error.UnsupportedQueryRequest,
        } else true,
        .inclusive_end = if (query.object.get("inclusive_end")) |inclusive_end| switch (inclusive_end) {
            .bool => inclusive_end.bool,
            else => return error.UnsupportedQueryRequest,
        } else false,
    } };
}

fn parseDirectDslBoolTextQuery(alloc: std.mem.Allocator, query: std.json.Value) anyerror!db_mod.types.TextQuery {
    if (query != .object) return error.UnsupportedQueryRequest;

    var must = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &must);
    var should = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &should);
    var must_not = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &must_not);

    if (query.object.get("filter")) |filter| {
        try appendDirectDslTextQueryList(alloc, &must, filter);
    }
    if (query.object.get("must")) |must_value| {
        try appendDirectDslTextQueryList(alloc, &must, must_value);
    }
    if (query.object.get("should")) |should_value| {
        try appendDirectDslTextQueryList(alloc, &should, should_value);
    }
    if (query.object.get("must_not")) |must_not_value| {
        try appendDirectDslTextQueryList(alloc, &must_not, must_not_value);
    }

    if (must.items.len == 0 and should.items.len == 0 and must_not.items.len == 0) {
        return error.UnsupportedQueryRequest;
    }

    const owned_must = try must.toOwnedSlice(alloc);
    errdefer freeTextQueryList(alloc, owned_must);
    const owned_should = try should.toOwnedSlice(alloc);
    errdefer freeTextQueryList(alloc, owned_should);
    const owned_must_not = try must_not.toOwnedSlice(alloc);
    errdefer freeTextQueryList(alloc, owned_must_not);

    return .{ .bool_query = .{
        .must = owned_must,
        .should = owned_should,
        .must_not = owned_must_not,
    } };
}

fn parseDirectDslTextQueryArrayAlloc(
    alloc: std.mem.Allocator,
    queries: std.json.Value,
) anyerror![]const db_mod.types.TextQuery {
    if (queries != .array) return error.UnsupportedQueryRequest;
    const out = try alloc.alloc(db_mod.types.TextQuery, queries.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeTextQueryList(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (queries.array.items, 0..) |item, i| {
        out[i] = (try parseDirectDslTextQuery(alloc, item)) orelse return error.UnsupportedQueryRequest;
        initialized += 1;
    }
    return out;
}

fn parseDirectDslTextQueryListAlloc(
    alloc: std.mem.Allocator,
    query_or_queries: std.json.Value,
) anyerror![]const db_mod.types.TextQuery {
    var list = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &list);
    try appendDirectDslTextQueryList(alloc, &list, query_or_queries);
    return try list.toOwnedSlice(alloc);
}

fn appendDirectDslTextQueryList(
    alloc: std.mem.Allocator,
    list: *std.ArrayListUnmanaged(db_mod.types.TextQuery),
    query_or_queries: std.json.Value,
) anyerror!void {
    if (query_or_queries == .array) {
        if (query_or_queries.array.items.len == 0) return error.UnsupportedQueryRequest;
        for (query_or_queries.array.items) |item| {
            try appendDirectDslTextQueryList(alloc, list, item);
        }
        return;
    }

    const parsed = (try parseDirectDslTextQuery(alloc, query_or_queries)) orelse return error.UnsupportedQueryRequest;
    errdefer freeTextQuery(alloc, parsed);
    try list.append(alloc, parsed);
}

fn parseGeneratedBleveTextQuery(alloc: std.mem.Allocator, query: std.json.Value) !db_mod.types.TextQuery {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const normalized = try normalizeGeneratedBleveQuery(arena, query);
    const parsed = try std.json.parseFromValue(bleve_openapi.Query, arena, normalized, .{});
    return try parseGeneratedBleveQueryValue(alloc, parsed.value);
}

fn normalizeGeneratedBleveQuery(alloc: std.mem.Allocator, query: std.json.Value) !std.json.Value {
    if (query != .object) return query;
    if (query.object.count() != 1) return query;

    if (query.object.get("match")) |wrapped| {
        if (wrapped == .object) {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, "match", wrapped.object.get("text") orelse wrapped.object.get("match") orelse return error.UnsupportedQueryRequest);
            if (directDslFieldValue(wrapped.object)) |field| try obj.put(alloc, "field", field);
            if (wrapped.object.get("analyzer")) |analyzer| try obj.put(alloc, "analyzer", analyzer);
            if (wrapped.object.get("fuzziness")) |fuzziness| try obj.put(alloc, "fuzziness", fuzziness);
            if (wrapped.object.get("prefix_length")) |prefix_length| try obj.put(alloc, "prefix_length", prefix_length);
            if (wrapped.object.get("operator")) |operator| try obj.put(alloc, "operator", operator);
            return .{ .object = obj };
        }
    }

    if (query.object.get("term")) |wrapped| {
        if (wrapped == .object) {
            var obj = std.json.ObjectMap.empty;
            if (wrapped.object.get("term")) |term| {
                try obj.put(alloc, "term", term);
            } else if (wrapped.object.count() == 1) {
                var it = wrapped.object.iterator();
                const entry = it.next() orelse return error.UnsupportedQueryRequest;
                try obj.put(alloc, "term", entry.value_ptr.*);
                try obj.put(alloc, "field", .{ .string = entry.key_ptr.* });
                return .{ .object = obj };
            } else {
                return error.UnsupportedQueryRequest;
            }
            if (directDslFieldValue(wrapped.object)) |field| try obj.put(alloc, "field", field);
            if (wrapped.object.get("fuzziness")) |fuzziness| try obj.put(alloc, "fuzziness", fuzziness);
            if (wrapped.object.get("prefix_length")) |prefix_length| try obj.put(alloc, "prefix_length", prefix_length);
            return .{ .object = obj };
        }
    }

    return query;
}

fn encodePatternFilterQuery(alloc: std.mem.Allocator, query: db_mod.types.TextQuery) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendPatternFilterQueryValue(alloc, &out, query);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeSupportedPatternFilterQueryAlloc(
    alloc: std.mem.Allocator,
    query: std.json.Value,
) ![]u8 {
    const parsed = try parseSupportedFullTextQuery(alloc, query, 10);
    defer freeTextQuery(alloc, parsed);
    return try encodePatternFilterQuery(alloc, parsed);
}

fn appendPatternFilterQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.types.TextQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match_none => try out.appendSlice(alloc, "{\"match_none\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":{");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, "}}");
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":{");
            try appendJsonString(alloc, out, match.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, "}}");
        },
        .prefix => |prefix| {
            try out.appendSlice(alloc, "{\"prefix\":{");
            try appendJsonString(alloc, out, prefix.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, prefix.prefix);
            try out.appendSlice(alloc, "}}");
        },
        .wildcard => |wildcard| {
            try out.appendSlice(alloc, "{\"wildcard\":{");
            try appendJsonString(alloc, out, wildcard.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, wildcard.pattern);
            try out.appendSlice(alloc, "}}");
        },
        .regexp => |regexp| {
            try out.appendSlice(alloc, "{\"regexp\":{");
            try appendJsonString(alloc, out, regexp.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, regexp.pattern);
            try out.appendSlice(alloc, "}}");
        },
        .numeric_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            try appendJsonFieldName(alloc, out, &first, "numeric_range");
            try out.append(alloc, '{');
            var inner_first = true;
            if (range_query.min) |min| {
                try appendJsonFieldName(alloc, out, &inner_first, "min");
                try out.print(alloc, "{d}", .{min});
            }
            if (range_query.max) |max| {
                try appendJsonFieldName(alloc, out, &inner_first, "max");
                try out.print(alloc, "{d}", .{max});
            }
            try appendJsonFieldString(alloc, out, &inner_first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &inner_first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &inner_first, "inclusive_max", true);
            try out.appendSlice(alloc, "}}");
        },
        .date_range => return error.UnsupportedQueryRequest,
        .doc_id => |doc_id| {
            try out.appendSlice(alloc, "{\"doc_id\":");
            try out.append(alloc, '[');
            for (doc_id.ids, 0..) |id, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, id);
            }
            try out.appendSlice(alloc, "]}");
        },
        .bool_query => |bool_query| {
            try out.appendSlice(alloc, "{\"bool\":{");
            var first = true;
            if (bool_query.must.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must");
                try out.append(alloc, '[');
                for (bool_query.must, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendPatternFilterQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
            }
            if (bool_query.should.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "should");
                try out.append(alloc, '[');
                for (bool_query.should, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendPatternFilterQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
            }
            if (bool_query.must_not.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must_not");
                try out.append(alloc, '[');
                for (bool_query.must_not, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendPatternFilterQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
            }
            try out.appendSlice(alloc, "}}");
        },
        else => return error.UnsupportedQueryRequest,
    }
}

fn parseGeneratedBleveBooleanQuery(
    alloc: std.mem.Allocator,
    boolean_query: *const bleve_openapi.BooleanQuery,
) anyerror!db_mod.types.TextBoolQuery {
    var must = std.ArrayListUnmanaged(db_mod.types.TextQuery).empty;
    errdefer deinitTextQueryArrayList(alloc, &must);

    if (boolean_query.filter) |filter| {
        try must.append(alloc, try parseGeneratedBleveQueryValue(alloc, filter));
    }
    if (boolean_query.must) |must_query| {
        const items = try parseGeneratedBleveQuerySlice(alloc, must_query.conjuncts);
        for (items) |item| try must.append(alloc, item);
        if (items.len > 0) alloc.free(items);
    }

    const should = if (boolean_query.should) |should_query|
        try parseGeneratedBleveQuerySlice(alloc, should_query.disjuncts)
    else
        &.{};
    errdefer if (should.len > 0) freeTextQueryList(alloc, should);

    const must_not = if (boolean_query.must_not) |must_not_query|
        try parseGeneratedBleveQuerySlice(alloc, must_not_query.disjuncts)
    else
        &.{};
    errdefer if (must_not.len > 0) freeTextQueryList(alloc, must_not);

    return .{
        .must = try must.toOwnedSlice(alloc),
        .should = should,
        .must_not = must_not,
        .min_should = if (boolean_query.should) |should_query|
            if (should_query.min) |min| @intFromFloat(min) else 0
        else
            0,
        .boost = if (boolean_query.boost) |boost| @floatCast(boost) else 1.0,
    };
}

fn parseGeneratedBleveQuerySlice(
    alloc: std.mem.Allocator,
    queries: []const bleve_openapi.Query,
) ![]const db_mod.types.TextQuery {
    if (queries.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.types.TextQuery, queries.len);
    var initialized: usize = 0;
    errdefer {
        freeTextQueryList(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (queries, 0..) |item, i| {
        out[i] = try parseGeneratedBleveQueryValue(alloc, item);
        initialized += 1;
    }
    return out;
}

fn parseGeneratedBleveQueryValue(alloc: std.mem.Allocator, query: bleve_openapi.Query) anyerror!db_mod.types.TextQuery {
    const query_string_has_default_operator = comptime blk: {
        const QueryStringType = @TypeOf((@as(bleve_openapi.Query, undefined)).query_string_query);
        break :blk switch (@typeInfo(QueryStringType)) {
            .pointer => |pointer| @hasField(pointer.child, "default_operator"),
            else => @hasField(QueryStringType, "default_operator"),
        };
    };

    return switch (query) {
        .match_all_query => .{ .match_all = {} },
        .match_none_query => .{ .match_none = {} },
        .query_string_query => |query_string| try parseQueryStringTextQuery(
            alloc,
            query_string.query,
            if (query_string.boost) |boost| @floatCast(boost) else 1.0,
            if (query_string_has_default_operator)
                try normalizeQueryStringDefaultOperator(query_string.default_operator)
            else
                null,
        ),
        .term_query => |term| .{ .term = .{
            .field = try alloc.dupe(u8, term.field orelse return error.UnsupportedQueryRequest),
            .term = try alloc.dupe(u8, term.term),
            .boost = if (term.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .match_query => |match| .{ .match = .{
            .field = try alloc.dupe(u8, match.field orelse return error.UnsupportedQueryRequest),
            .text = try alloc.dupe(u8, match.match),
            .analyzer = if (match.analyzer) |analyzer| try alloc.dupe(u8, analyzer) else null,
            .boost = if (match.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .match_phrase_query => |phrase| blk: {
            const fuzziness = try parseBleveFuzziness(phrase.fuzziness, 0);
            break :blk .{ .match_phrase = .{
                .field = try alloc.dupe(u8, phrase.field orelse return error.UnsupportedQueryRequest),
                .text = try alloc.dupe(u8, phrase.match_phrase),
                .analyzer = if (phrase.analyzer) |analyzer| try alloc.dupe(u8, analyzer) else null,
                .max_edits = fuzziness.max_edits,
                .auto_fuzzy = fuzziness.auto_fuzzy,
                .boost = if (phrase.boost) |boost| @floatCast(boost) else 1.0,
            } };
        },
        .fuzzy_query => |fuzzy| blk: {
            const fuzziness = try parseBleveFuzziness(fuzzy.fuzziness, 1);
            break :blk .{ .fuzzy = .{
                .field = try alloc.dupe(u8, fuzzy.field orelse return error.UnsupportedQueryRequest),
                .term = try alloc.dupe(u8, fuzzy.term),
                .max_edits = fuzziness.max_edits,
                .prefix_len = if (fuzzy.prefix_length) |prefix_length| @intCast(prefix_length) else 0,
                .auto_fuzzy = fuzziness.auto_fuzzy,
                .boost = if (fuzzy.boost) |boost| @floatCast(boost) else 1.0,
            } };
        },
        .prefix_query => |prefix| .{ .prefix = .{
            .field = try alloc.dupe(u8, prefix.field orelse return error.UnsupportedQueryRequest),
            .prefix = try alloc.dupe(u8, prefix.prefix),
            .boost = if (prefix.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .wildcard_query => |wildcard| .{ .wildcard = .{
            .field = try alloc.dupe(u8, wildcard.field orelse return error.UnsupportedQueryRequest),
            .pattern = try alloc.dupe(u8, wildcard.wildcard),
            .boost = if (wildcard.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .regexp_query => |regexp| .{ .regexp = .{
            .field = try alloc.dupe(u8, regexp.field orelse return error.UnsupportedQueryRequest),
            .pattern = try alloc.dupe(u8, regexp.regexp),
            .boost = if (regexp.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .numeric_range_query => |range_query| .{ .numeric_range = .{
            .field = try alloc.dupe(u8, range_query.field orelse return error.UnsupportedQueryRequest),
            .min = range_query.min,
            .max = range_query.max,
            .inclusive_min = range_query.inclusive_min orelse true,
            .inclusive_max = range_query.inclusive_max orelse false,
            .boost = if (range_query.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .term_range_query => |range_query| .{ .term_range = .{
            .field = try alloc.dupe(u8, range_query.field orelse return error.UnsupportedQueryRequest),
            .min = if (range_query.min) |min| try alloc.dupe(u8, min) else null,
            .max = if (range_query.max) |max| try alloc.dupe(u8, max) else null,
            .inclusive_min = range_query.inclusive_min orelse true,
            .inclusive_max = range_query.inclusive_max orelse false,
            .boost = if (range_query.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .date_range_string_query => |range_query| blk: {
            if (range_query.datetime_parser != null) return error.UnsupportedQueryRequest;
            const field = try alloc.dupe(u8, range_query.field orelse return error.UnsupportedQueryRequest);
            errdefer alloc.free(field);
            break :blk .{ .date_range = .{
                .field = field,
                .start_ns = if (range_query.start) |start|
                    (try parseDateTimeOptionalToNs(start)) orelse return error.UnsupportedQueryRequest
                else
                    null,
                .end_ns = if (range_query.end) |end|
                    (try parseDateTimeOptionalToNs(end)) orelse return error.UnsupportedQueryRequest
                else
                    null,
                .inclusive_start = range_query.inclusive_start orelse true,
                .inclusive_end = range_query.inclusive_end orelse false,
                .boost = if (range_query.boost) |boost| @floatCast(boost) else 1.0,
            } };
        },
        .doc_id_query => |doc_id| .{ .doc_id = .{
            .ids = try cloneFields(alloc, doc_id.ids),
            .boost = if (doc_id.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .bool_field_query => |bool_field| .{ .bool_field = .{
            .field = try alloc.dupe(u8, bool_field.field orelse return error.UnsupportedQueryRequest),
            .value = bool_field.bool,
            .boost = if (bool_field.boost) |boost| @floatCast(boost) else 1.0,
        } },
        .boolean_query => |boolean_query| .{
            .bool_query = try parseGeneratedBleveBooleanQuery(alloc, boolean_query),
        },
        .conjunction_query => |conjunction| .{
            .bool_query = .{
                .must = try parseGeneratedBleveQuerySlice(alloc, conjunction.conjuncts),
                .boost = if (conjunction.boost) |boost| @floatCast(boost) else 1.0,
            },
        },
        .disjunction_query => |disjunction| .{
            .bool_query = .{
                .should = try parseGeneratedBleveQuerySlice(alloc, disjunction.disjuncts),
                .min_should = if (disjunction.min) |min| @intFromFloat(min) else 0,
                .boost = if (disjunction.boost) |boost| @floatCast(boost) else 1.0,
            },
        },
        else => error.UnsupportedQueryRequest,
    };
}

fn parseQueryStringTextQuery(
    alloc: std.mem.Allocator,
    input: []const u8,
    boost: f32,
    default_operator: ?[]const u8,
) !db_mod.types.TextQuery {
    const parsed_default_operator = if (default_operator) |value|
        if (std.ascii.eqlIgnoreCase(value, "or"))
            public_query_string_mod.ParseOptions{ .default_operator = .or_ }
        else if (std.ascii.eqlIgnoreCase(value, "and"))
            public_query_string_mod.ParseOptions{ .default_operator = .and_ }
        else
            return error.UnsupportedQueryRequest
    else
        public_query_string_mod.ParseOptions{};

    var owned = try public_query_string_mod.parseFilterAllocWithOptions(alloc, input, parsed_default_operator);
    defer owned.deinit(alloc);
    return try public_query_string_mod.filterToStatefulTextQueryAlloc(alloc, owned.filter, boost);
}

fn normalizeQueryStringDefaultOperator(value: anytype) !?[]const u8 {
    const T = @TypeOf(value);
    return switch (@typeInfo(T)) {
        .optional => if (value) |inner| try normalizeQueryStringDefaultOperator(inner) else null,
        .pointer => |pointer| switch (pointer.size) {
            .slice => value,
            else => error.UnsupportedQueryRequest,
        },
        .@"enum" => @tagName(value),
        else => error.UnsupportedQueryRequest,
    };
}

const ParsedFuzziness = struct {
    max_edits: u8,
    auto_fuzzy: bool,
};

fn parseBleveFuzziness(value: ?bleve_openapi.Fuzziness, default_edits: u8) !ParsedFuzziness {
    if (value == null) return .{ .max_edits = default_edits, .auto_fuzzy = false };
    return switch (value.?) {
        .integer => |int_value| .{ .max_edits = @intCast(int_value), .auto_fuzzy = false },
        .string => |str_value| {
            if (!std.mem.eql(u8, str_value, "auto")) return error.UnsupportedQueryRequest;
            return .{ .max_edits = default_edits, .auto_fuzzy = true };
        },
        else => error.UnsupportedQueryRequest,
    };
}

fn parseDateTimeOptionalToNs(text: []const u8) !?u64 {
    if (try parseRfc3339ToNs(text)) |ts| return ts;
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return null;
    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    return civilDateTimeToNs(year, month, day, 0, 0, 0, 0);
}

fn parseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    return civilDateTimeToNs(year, month, day, hour, minute, second, nanos);
}

fn civilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn buildSemanticVectorQueries(
    alloc: std.mem.Allocator,
    semantic_resolver: ?SemanticResolver,
    table_name: []const u8,
    request: metadata_openapi.QueryRequest,
    limit: u32,
) !NamedVectorQueries {
    if (request.semantic_search == null and request.embeddings == null) return .{};

    var parsed_embeddings = try public_search_request_mod.parseEmbeddingsAlloc(alloc, request, limit);
    defer parsed_embeddings.deinit(alloc);
    const index_names = (try public_search_request_mod.cloneRequestedIndexesAlloc(alloc, request, parsed_embeddings)) orelse
        return error.UnsupportedQueryRequest;
    defer {
        for (index_names) |index_name| alloc.free(index_name);
        alloc.free(index_names);
    }

    if (index_names.len == 0) return error.UnsupportedQueryRequest;

    var dense_queries = std.ArrayListUnmanaged(db_mod.types.NamedDenseQuery).empty;
    errdefer freeNamedDenseQueries(alloc, dense_queries.items);
    var sparse_queries = std.ArrayListUnmanaged(db_mod.types.NamedSparseQuery).empty;
    errdefer freeNamedSparseQueries(alloc, sparse_queries.items);

    for (index_names) |index_name| {
        if (parsed_embeddings.find(index_name)) |embedding| {
            switch (embedding.query) {
                .dense => |dense_query| try dense_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .vector = try alloc.dupe(f32, dense_query.vector),
                        .k = dense_query.k,
                    },
                }),
                .sparse => |sparse_query| try sparse_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .indices = try alloc.dupe(u32, sparse_query.indices),
                        .values = try alloc.dupe(f32, sparse_query.values),
                        .k = sparse_query.k,
                    },
                }),
            }
            continue;
        }
        if (request.semantic_search) |semantic_search| {
            const resolver = semantic_resolver orelse return error.UnsupportedQueryRequest;
            try dense_queries.append(alloc, .{
                .name = try alloc.dupe(u8, index_name),
                .index_name = try alloc.dupe(u8, index_name),
                .query = try resolver.resolveDenseQuery(alloc, table_name, index_name, semantic_search, request.embedding_template, limit),
            });
            continue;
        }
        return error.UnsupportedQueryRequest;
    }

    return .{
        .dense = try dense_queries.toOwnedSlice(alloc),
        .sparse = try sparse_queries.toOwnedSlice(alloc),
    };
}

fn buildPreflightSemanticVectorQueries(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
    limit: u32,
) !NamedVectorQueries {
    if (request.semantic_search == null and request.embeddings == null) return .{};

    var parsed_embeddings = try public_search_request_mod.parseEmbeddingsAlloc(alloc, request, limit);
    defer parsed_embeddings.deinit(alloc);
    const index_names = (try public_search_request_mod.cloneRequestedIndexesAlloc(alloc, request, parsed_embeddings)) orelse
        return error.UnsupportedQueryRequest;
    defer {
        for (index_names) |index_name| alloc.free(index_name);
        alloc.free(index_names);
    }

    if (index_names.len == 0) return error.UnsupportedQueryRequest;

    var dense_queries = std.ArrayListUnmanaged(db_mod.types.NamedDenseQuery).empty;
    errdefer freeNamedDenseQueries(alloc, dense_queries.items);
    var sparse_queries = std.ArrayListUnmanaged(db_mod.types.NamedSparseQuery).empty;
    errdefer freeNamedSparseQueries(alloc, sparse_queries.items);

    for (index_names) |index_name| {
        if (parsed_embeddings.find(index_name)) |embedding| {
            switch (embedding.query) {
                .dense => |dense_query| try dense_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .vector = try alloc.dupe(f32, dense_query.vector),
                        .k = dense_query.k,
                    },
                }),
                .sparse => |sparse_query| try sparse_queries.append(alloc, .{
                    .name = try alloc.dupe(u8, index_name),
                    .index_name = try alloc.dupe(u8, index_name),
                    .query = .{
                        .indices = try alloc.dupe(u32, sparse_query.indices),
                        .values = try alloc.dupe(f32, sparse_query.values),
                        .k = sparse_query.k,
                    },
                }),
            }
            continue;
        }
        if (request.semantic_search != null) {
            try dense_queries.append(alloc, .{
                .name = try alloc.dupe(u8, index_name),
                .index_name = try alloc.dupe(u8, index_name),
                .query = .{
                    .vector = try alloc.alloc(f32, 0),
                    .k = limit,
                },
            });
            continue;
        }
        return error.UnsupportedQueryRequest;
    }

    return .{
        .dense = try dense_queries.toOwnedSlice(alloc),
        .sparse = try sparse_queries.toOwnedSlice(alloc),
    };
}

fn buildGraphQueries(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
) ![]const db_mod.types.NamedGraphQuery {
    const graph_searches = request.graph_searches orelse return &.{};

    var items = std.ArrayListUnmanaged(db_mod.types.NamedGraphQuery).empty;
    errdefer {
        for (items.items) |item| {
            alloc.free(item.name);
            freeGraphQuery(alloc, item.query);
        }
        items.deinit(alloc);
    }

    var it = graph_searches.map.iterator();
    while (it.next()) |entry| {
        try items.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .query = try parseGraphQuery(alloc, entry.value_ptr.*),
        });
    }
    return try items.toOwnedSlice(alloc);
}

fn parseGraphQuery(
    alloc: std.mem.Allocator,
    query: indexes_openapi.GraphQuery,
) !graph_query_mod.GraphQuery {
    const params = try parseGraphQueryParams(alloc, query.params);
    errdefer freeGraphQueryParams(alloc, params);
    const index_name = try alloc.dupe(u8, query.index_name);
    errdefer alloc.free(index_name);
    const start_nodes = try parseGraphNodeSelector(alloc, query.start_nodes orelse return error.UnsupportedQueryRequest);
    errdefer freeGraphNodeSelector(alloc, start_nodes);
    const target_nodes = if (query.target_nodes) |target_selector|
        try parseGraphNodeSelector(alloc, target_selector)
    else
        null;
    errdefer if (target_nodes) |selector| freeGraphNodeSelector(alloc, selector);
    const pattern = if (query.pattern) |steps|
        try parsePatternSteps(alloc, steps)
    else
        @constCast((&[_]graph_pattern_mod.PatternStep{})[0..]);
    errdefer freePatternSteps(alloc, pattern);
    const return_aliases = if (query.return_aliases) |aliases|
        try cloneFields(alloc, aliases)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer {
        for (return_aliases) |alias| alloc.free(alias);
        if (return_aliases.len > 0) alloc.free(return_aliases);
    }
    if (query.include_edges == true) return error.UnsupportedQueryRequest;
    const fields = if (query.fields) |requested_fields|
        try cloneFields(alloc, requested_fields)
    else
        @constCast((&[_][]const u8{})[0..]);
    errdefer {
        for (fields) |field| alloc.free(field);
        if (fields.len > 0) alloc.free(fields);
    }

    if (query.type == .pattern) {
        if (pattern.len == 0) return error.UnsupportedQueryRequest;
        if (target_nodes != null) return error.UnsupportedQueryRequest;
    } else {
        if (pattern.len > 0 or return_aliases.len > 0) return error.UnsupportedQueryRequest;
    }

    return .{
        .query_type = switch (query.type) {
            .traverse => .traverse,
            .neighbors => .neighbors,
            .shortest_path => .shortest_path,
            .k_shortest_paths => .k_shortest_paths,
            .pattern => .pattern,
        },
        .index_name = index_name,
        .start_nodes = start_nodes,
        .params = params,
        .target_nodes = target_nodes,
        .k = if (query.params) |graph_params|
            if (graph_params.k) |k|
                std.math.cast(u32, k) orelse return error.InvalidQueryRequest
            else
                1
        else
            1,
        .pattern = pattern,
        .return_aliases = return_aliases,
        .include_documents = query.include_documents orelse false,
        .fields = fields,
        .include_all_fields = false,
    };
}

fn parsePatternSteps(
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
                @constCast((&[_][]const u8{})[0..])
        else
            @constCast((&[_][]const u8{})[0..]);
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
        const query = try parseSupportedFullTextQuery(alloc, filter_query, 10);
        defer freeTextQuery(alloc, query);
        out.filter_query_json = try encodePatternFilterQuery(alloc, query);
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

fn parseGraphNodeSelector(
    alloc: std.mem.Allocator,
    selector: indexes_openapi.GraphNodeSelector,
) !graph_query_mod.NodeSelector {
    if (selector.node_filter != null) return error.UnsupportedQueryRequest;
    if (selector.keys) |keys| {
        const owned_keys = try cloneFields(alloc, keys);
        return .{ .keys = owned_keys };
    }
    if (selector.result_ref) |result_ref| {
        return .{ .result_ref = .{
            .ref = try alloc.dupe(u8, result_ref),
            .limit = if (selector.limit) |limit|
                std.math.cast(u32, limit) orelse return error.InvalidQueryRequest
            else
                0,
        } };
    }
    return error.UnsupportedQueryRequest;
}

fn parseGraphQueryParams(
    alloc: std.mem.Allocator,
    params: ?indexes_openapi.GraphQueryParams,
) !graph_query_mod.QueryParams {
    if (params == null) return .{};
    const graph_params = params.?;
    if (graph_params.node_filter != null) return error.UnsupportedQueryRequest;
    if (graph_params.algorithm != null or graph_params.algorithm_params != null) return error.UnsupportedQueryRequest;

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
        .min_weight = graph_params.min_weight orelse 0.0,
        .max_weight = graph_params.max_weight orelse 0.0,
        .deduplicate = graph_params.deduplicate_nodes orelse true,
        .include_paths = graph_params.include_paths orelse false,
        .weight_mode = if (graph_params.weight_mode) |weight_mode| switch (weight_mode) {
            .min_hops => .min_hops,
            .min_weight => .min_weight,
            .max_weight => .max_weight,
        } else .min_hops,
    };
}

fn parseExpandStrategy(text: []const u8) !graph_query_mod.ExpandStrategy {
    if (std.mem.eql(u8, text, "union")) return .@"union";
    if (std.mem.eql(u8, text, "intersection")) return .intersection;
    return error.UnsupportedQueryRequest;
}

fn appendUniqueOwnedString(
    alloc: std.mem.Allocator,
    values: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (values.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try values.append(alloc, try alloc.dupe(u8, value));
}

fn freeSearchRequest(alloc: std.mem.Allocator, req: *db_mod.types.SearchRequest) void {
    if (req.index_name) |index_name| alloc.free(index_name);
    if (req.primary_text_index_name) |index_name| alloc.free(index_name);
    if (req.aggregations_json.len > 0) alloc.free(req.aggregations_json);
    if (req.filter_prefix.len > 0) alloc.free(req.filter_prefix);
    if (req.reranker) |*reranker| reranker.deinit(alloc);
    if (req.reranker_query_text.len > 0) alloc.free(req.reranker_query_text);
    if (req.merge_config) |merge_config| {
        for (merge_config.weights) |item| alloc.free(item.name);
        if (merge_config.weights.len > 0) alloc.free(merge_config.weights);
    }
    if (req.full_text) |full_text| freeTextQuery(alloc, full_text);
    if (req.filter_query_json.len > 0) alloc.free(req.filter_query_json);
    if (req.exclusion_query_json.len > 0) alloc.free(req.exclusion_query_json);
    switch (req.query) {
        .term => |term| {
            alloc.free(term.field);
            alloc.free(term.term);
        },
        .match => |match| {
            alloc.free(match.field);
            alloc.free(match.text);
        },
        else => {},
    }
    if (req.dense) |dense| alloc.free(dense.vector);
    freeNamedDenseQueries(alloc, req.dense_queries);
    freeNamedSparseQueries(alloc, req.sparse_queries);
    freeNamedGraphQueries(alloc, req.graph_queries);
    if (req.sparse) |sparse| {
        alloc.free(sparse.indices);
        alloc.free(sparse.values);
    }
    if (req.filter_ids.len > 0) alloc.free(req.filter_ids);
    if (req.exclude_ids.len > 0) alloc.free(req.exclude_ids);
    if (req.filter_doc_ids.len > 0) {
        freeOwnedStringItems(alloc, req.filter_doc_ids);
        alloc.free(@constCast(req.filter_doc_ids));
    }
    if (req.exclude_doc_ids.len > 0) {
        freeOwnedStringItems(alloc, req.exclude_doc_ids);
        alloc.free(@constCast(req.exclude_doc_ids));
    }
    if (req.distributed_text_stats.len > 0) @import("../search/distributed_stats.zig").deinitTextFieldStats(alloc, req.distributed_text_stats);
    req.* = undefined;
}

fn queryBodyHasInternalShardFields(body: []const u8) bool {
    return std.mem.indexOf(u8, body, "\"_distributed_text_stats\"") != null or
        std.mem.indexOf(u8, body, "\"native_doc_id_constraints\"") != null or
        std.mem.indexOf(u8, body, "\"_filter_query_json\"") != null or
        std.mem.indexOf(u8, body, "\"_exclusion_query_json\"") != null or
        std.mem.indexOf(u8, body, "\"_filter_doc_ids\"") != null or
        std.mem.indexOf(u8, body, "\"_filter_doc_ids_positive\"") != null or
        std.mem.indexOf(u8, body, "\"_exclude_doc_ids\"") != null;
}

fn parseInternalFilterQueryJsonAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    req: *db_mod.types.SearchRequest,
) !void {
    if (std.mem.indexOf(u8, body, "\"_filter_query_json\"") == null and
        std.mem.indexOf(u8, body, "\"_exclusion_query_json\"") == null) return;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;

    if (parsed.value.object.get("_filter_query_json")) |value| {
        const query_json = try parseInternalFilterJsonStringAlloc(alloc, value);
        if (req.filter_query_json.len > 0) alloc.free(req.filter_query_json);
        req.filter_query_json = query_json;
    }
    if (parsed.value.object.get("_exclusion_query_json")) |value| {
        const query_json = try parseInternalFilterJsonStringAlloc(alloc, value);
        if (req.exclusion_query_json.len > 0) alloc.free(req.exclusion_query_json);
        req.exclusion_query_json = query_json;
    }
}

fn parseInternalFilterJsonStringAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .string) return error.InvalidQueryRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value.string, .{}) catch return error.InvalidQueryRequest;
    parsed.deinit();
    return try alloc.dupe(u8, value.string);
}

fn parseInternalDocIdConstraintsAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
    req: *db_mod.types.SearchRequest,
) !void {
    if (!queryBodyHasInternalShardFields(body)) return;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;

    const native_constraints = parsed.value.object.get("native_doc_id_constraints");
    const has_legacy_constraints = parsed.value.object.get("_filter_doc_ids_positive") != null or
        parsed.value.object.get("_filter_doc_ids") != null or
        parsed.value.object.get("_exclude_doc_ids") != null;
    if (has_legacy_constraints) return error.InvalidQueryRequest;

    if (native_constraints) |value| {
        var envelope = try parseNativeDocIdConstraintEnvelopeValueAlloc(alloc, value);
        applyNativeDocIdConstraintEnvelope(req, envelope.constraints);
        envelope.constraints.include_doc_ids = &.{};
        envelope.constraints.exclude_doc_ids = &.{};
        envelope.deinit(alloc);
    }
}

fn parseInternalDocIdArrayAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidQueryRequest;
    if (value.array.items.len == 0) return &.{};

    const out = try alloc.alloc([]const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(@constCast(item));
        alloc.free(out);
    }

    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidQueryRequest;
        out[i] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return try sortAndDedupeOwnedStringArrayAlloc(alloc, out);
}

fn parseDistributedTextStatsAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]const @import("../search/distributed_stats.zig").TextFieldStats {
    const distributed_stats_mod = @import("../search/distributed_stats.zig");

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return &.{};
    defer parsed.deinit();
    if (parsed.value != .object) return &.{};
    const encoded = parsed.value.object.get("_distributed_text_stats") orelse return &.{};
    if (encoded != .array) return error.InvalidQueryRequest;

    const stats = try alloc.alloc(distributed_stats_mod.TextFieldStats, encoded.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (stats[0..initialized]) |*item| item.deinit(alloc);
        if (stats.len > 0) alloc.free(stats);
    }

    for (encoded.array.items, 0..) |entry, i| {
        if (entry != .object) return error.InvalidQueryRequest;
        const field_value = entry.object.get("field") orelse return error.InvalidQueryRequest;
        const doc_count_value = entry.object.get("global_doc_count") orelse return error.InvalidQueryRequest;
        const total_field_len_value = entry.object.get("global_total_field_len") orelse return error.InvalidQueryRequest;
        const term_doc_freqs_value = entry.object.get("term_doc_freqs") orelse return error.InvalidQueryRequest;
        if (field_value != .string or term_doc_freqs_value != .array) return error.InvalidQueryRequest;

        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, term_doc_freqs_value.array.items.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (term_doc_freqs_value.array.items, 0..) |term_entry, term_idx| {
            if (term_entry != .object) return error.InvalidQueryRequest;
            const term_value = term_entry.object.get("term") orelse return error.InvalidQueryRequest;
            const freq_value = term_entry.object.get("doc_freq") orelse return error.InvalidQueryRequest;
            if (term_value != .string) return error.InvalidQueryRequest;
            term_doc_freqs[term_idx] = .{
                .term = try alloc.dupe(u8, term_value.string),
                .doc_freq = try jsonValueToU32(freq_value),
            };
            initialized_terms += 1;
        }

        stats[i] = .{
            .field = try alloc.dupe(u8, field_value.string),
            .global_doc_count = try jsonValueToU32(doc_count_value),
            .global_total_field_len = try jsonValueToU64(total_field_len_value),
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }

    return stats;
}

fn jsonValueToU32(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |v| std.math.cast(u32, v) orelse return error.InvalidQueryRequest,
        else => error.InvalidQueryRequest,
    };
}

fn jsonValueToU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |v| std.math.cast(u64, v) orelse return error.InvalidQueryRequest,
        else => error.InvalidQueryRequest,
    };
}

fn freeNamedDenseQueries(alloc: std.mem.Allocator, items: []const db_mod.types.NamedDenseQuery) void {
    for (items) |item| {
        alloc.free(item.name);
        alloc.free(item.index_name);
        alloc.free(item.query.vector);
    }
    if (items.len > 0) alloc.free(items);
}

fn freeNamedSparseQueries(alloc: std.mem.Allocator, items: []const db_mod.types.NamedSparseQuery) void {
    for (items) |item| {
        alloc.free(item.name);
        alloc.free(item.index_name);
        alloc.free(item.query.indices);
        alloc.free(item.query.values);
    }
    if (items.len > 0) alloc.free(items);
}

fn freeNamedGraphQueries(alloc: std.mem.Allocator, items: []const db_mod.types.NamedGraphQuery) void {
    for (items) |item| {
        alloc.free(item.name);
        freeGraphQuery(alloc, item.query);
    }
    if (items.len > 0) alloc.free(items);
}

fn freeGraphQuery(alloc: std.mem.Allocator, query: graph_query_mod.GraphQuery) void {
    alloc.free(query.index_name);
    freeGraphNodeSelector(alloc, query.start_nodes);
    if (query.target_nodes) |target_nodes| freeGraphNodeSelector(alloc, target_nodes);
    freeGraphQueryParams(alloc, query.params);
    freePatternSteps(alloc, query.pattern);
    for (query.return_aliases) |alias| alloc.free(alias);
    if (query.return_aliases.len > 0) alloc.free(query.return_aliases);
    for (query.fields) |field| alloc.free(field);
    if (query.fields.len > 0) alloc.free(query.fields);
}

fn freeGraphNodeSelector(alloc: std.mem.Allocator, selector: graph_query_mod.NodeSelector) void {
    switch (selector) {
        .keys => |keys| {
            for (keys) |key| alloc.free(key);
            if (keys.len > 0) alloc.free(keys);
        },
        .result_ref => |result_ref| {
            alloc.free(result_ref.ref);
        },
    }
}

fn freeGraphQueryParams(alloc: std.mem.Allocator, params: graph_query_mod.QueryParams) void {
    for (params.edge_types) |edge_type| alloc.free(edge_type);
    if (params.edge_types.len > 0) alloc.free(params.edge_types);
}

fn freeTextQueryList(alloc: std.mem.Allocator, items: []const db_mod.types.TextQuery) void {
    for (items) |item| freeTextQuery(alloc, item);
    if (items.len > 0) alloc.free(items);
}

fn deinitTextQueryArrayList(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(db_mod.types.TextQuery)) void {
    for (list.items) |item| freeTextQuery(alloc, item);
    list.deinit(alloc);
    list.* = .empty;
}

fn freeTextQuery(alloc: std.mem.Allocator, query: db_mod.types.TextQuery) void {
    switch (query) {
        .phrase => |phrase| {
            alloc.free(phrase.field);
            for (phrase.terms) |term| alloc.free(term);
            if (phrase.terms.len > 0) alloc.free(phrase.terms);
        },
        .match_phrase => |phrase| {
            alloc.free(phrase.field);
            alloc.free(phrase.text);
            if (phrase.analyzer) |analyzer| alloc.free(analyzer);
        },
        .fuzzy => |fuzzy| {
            alloc.free(fuzzy.field);
            alloc.free(fuzzy.term);
        },
        .term => |term| {
            alloc.free(term.field);
            alloc.free(term.term);
        },
        .match => |match| {
            alloc.free(match.field);
            alloc.free(match.text);
            if (match.analyzer) |analyzer| alloc.free(analyzer);
        },
        .doc_id => |doc_id| {
            for (doc_id.ids) |id| alloc.free(id);
            if (doc_id.ids.len > 0) alloc.free(doc_id.ids);
        },
        .bool_field => |bool_field| {
            alloc.free(bool_field.field);
        },
        .prefix => |prefix| {
            alloc.free(prefix.field);
            alloc.free(prefix.prefix);
        },
        .wildcard => |wildcard| {
            alloc.free(wildcard.field);
            alloc.free(wildcard.pattern);
        },
        .regexp => |regexp| {
            alloc.free(regexp.field);
            alloc.free(regexp.pattern);
        },
        .numeric_range => |range_query| {
            alloc.free(range_query.field);
        },
        .term_range => |range_query| {
            alloc.free(range_query.field);
            if (range_query.min) |min| alloc.free(min);
            if (range_query.max) |max| alloc.free(max);
        },
        .date_range => |range_query| {
            alloc.free(range_query.field);
        },
        .bool_query => |bool_query| {
            freeTextQueryList(alloc, bool_query.must);
            freeTextQueryList(alloc, bool_query.should);
            freeTextQueryList(alloc, bool_query.must_not);
        },
        else => {},
    }
}

fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try std.json.Stringify.value(value, .{}, &out.writer);
    return try alloc.dupe(u8, out.written());
}

test "api query contract parses direct structured boolean filters" {
    const alloc = std.testing.allocator;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"conjuncts":[{"term":{"status":"active"}},{"term":{"tenant":"tenant-a"}}]}
    , .{});
    defer parsed.deinit();

    const encoded = try encodeSupportedPatternFilterQueryAlloc(alloc, parsed.value);
    defer alloc.free(encoded);

    try std.testing.expectEqualStrings(
        "{\"bool\":{\"must\":[{\"term\":{\"status\":\"active\"}},{\"term\":{\"tenant\":\"tenant-a\"}}]}}",
        encoded,
    );
}

test "api query contract parses direct JSON-pointer path aliases" {
    const alloc = std.testing.allocator;

    var direct_term_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":"gold","path":"/tier"}
    , .{});
    defer direct_term_json.deinit();
    const direct_term = try parseSupportedFullTextQuery(alloc, direct_term_json.value, 10);
    defer freeTextQuery(alloc, direct_term);
    try std.testing.expect(direct_term == .term);
    try std.testing.expectEqualStrings("/tier", direct_term.term.field);
    try std.testing.expectEqualStrings("gold", direct_term.term.term);

    var wrapped_term_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":{"path":"/tier","value":"silver"}}
    , .{});
    defer wrapped_term_json.deinit();
    const wrapped_term = try parseSupportedFullTextQuery(alloc, wrapped_term_json.value, 10);
    defer freeTextQuery(alloc, wrapped_term);
    try std.testing.expect(wrapped_term == .term);
    try std.testing.expectEqualStrings("/tier", wrapped_term.term.field);
    try std.testing.expectEqualStrings("silver", wrapped_term.term.term);

    var match_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"match":{"path":"/tier","text":"gold"}}
    , .{});
    defer match_json.deinit();
    const match_query = try parseSupportedFullTextQuery(alloc, match_json.value, 10);
    defer freeTextQuery(alloc, match_query);
    try std.testing.expect(match_query == .match);
    try std.testing.expectEqualStrings("/tier", match_query.match.field);
    try std.testing.expectEqualStrings("gold", match_query.match.text);

    var prefix_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"prefix":"go","path":"/tier"}
    , .{});
    defer prefix_json.deinit();
    const prefix_query = try parseSupportedFullTextQuery(alloc, prefix_json.value, 10);
    defer freeTextQuery(alloc, prefix_query);
    try std.testing.expect(prefix_query == .prefix);
    try std.testing.expectEqualStrings("/tier", prefix_query.prefix.field);
    try std.testing.expectEqualStrings("go", prefix_query.prefix.prefix);

    var wrapped_prefix_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"prefix":{"path":"/tier","value":"si"}}
    , .{});
    defer wrapped_prefix_json.deinit();
    const wrapped_prefix_query = try parseSupportedFullTextQuery(alloc, wrapped_prefix_json.value, 10);
    defer freeTextQuery(alloc, wrapped_prefix_query);
    try std.testing.expect(wrapped_prefix_query == .prefix);
    try std.testing.expectEqualStrings("/tier", wrapped_prefix_query.prefix.field);
    try std.testing.expectEqualStrings("si", wrapped_prefix_query.prefix.prefix);

    var wildcard_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"wildcard":{"path":"/tier","pattern":"go*"}}
    , .{});
    defer wildcard_json.deinit();
    const wildcard_query = try parseSupportedFullTextQuery(alloc, wildcard_json.value, 10);
    defer freeTextQuery(alloc, wildcard_query);
    try std.testing.expect(wildcard_query == .wildcard);
    try std.testing.expectEqualStrings("/tier", wildcard_query.wildcard.field);
    try std.testing.expectEqualStrings("go*", wildcard_query.wildcard.pattern);

    var regexp_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"regexp":{"path":"/tier","value":"go.*"}}
    , .{});
    defer regexp_json.deinit();
    const regexp_query = try parseSupportedFullTextQuery(alloc, regexp_json.value, 10);
    defer freeTextQuery(alloc, regexp_query);
    try std.testing.expect(regexp_query == .regexp);
    try std.testing.expectEqualStrings("/tier", regexp_query.regexp.field);
    try std.testing.expectEqualStrings("go.*", regexp_query.regexp.pattern);

    var fuzzy_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"fuzzy":{"path":"/tier","query":"gild","prefix_length":1,"max_edits":1}}
    , .{});
    defer fuzzy_json.deinit();
    const fuzzy_query = try parseSupportedFullTextQuery(alloc, fuzzy_json.value, 10);
    defer freeTextQuery(alloc, fuzzy_query);
    try std.testing.expect(fuzzy_query == .fuzzy);
    try std.testing.expectEqualStrings("/tier", fuzzy_query.fuzzy.field);
    try std.testing.expectEqualStrings("gild", fuzzy_query.fuzzy.term);
    try std.testing.expectEqual(@as(u8, 1), fuzzy_query.fuzzy.prefix_len);
    try std.testing.expectEqual(@as(u8, 1), fuzzy_query.fuzzy.max_edits);

    var range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"path":"/amount","min":10,"max":20}
    , .{});
    defer range_json.deinit();
    const range_query = try parseSupportedFullTextQuery(alloc, range_json.value, 10);
    defer freeTextQuery(alloc, range_query);
    try std.testing.expect(range_query == .numeric_range);
    try std.testing.expectEqualStrings("/amount", range_query.numeric_range.field);
    try std.testing.expectEqual(@as(?f64, 10), range_query.numeric_range.min);
    try std.testing.expectEqual(@as(?f64, 20), range_query.numeric_range.max);

    var mixed_range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"path":"/created_at","start":"2026-01-01T00:00:00Z","min":10}
    , .{});
    defer mixed_range_json.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseSupportedFullTextQuery(alloc, mixed_range_json.value, 10));

    var malformed_operator_with_range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":42,"path":"/amount","min":10}
    , .{});
    defer malformed_operator_with_range_json.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseSupportedFullTextQuery(alloc, malformed_operator_with_range_json.value, 10));

    var malformed_operator_with_date_range_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"term":42,"path":"/created_at","start":"2026-01-01T00:00:00Z"}
    , .{});
    defer malformed_operator_with_date_range_json.deinit();
    try std.testing.expectError(error.UnsupportedQueryRequest, parseSupportedFullTextQuery(alloc, malformed_operator_with_date_range_json.value, 10));

    var disjuncts_json = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"disjuncts":[{"term":"gold","path":"/tier"},{"term":{"path":"/tier","term":"bronze"}}]}
    , .{});
    defer disjuncts_json.deinit();
    const disjuncts_query = try parseSupportedFullTextQuery(alloc, disjuncts_json.value, 10);
    defer freeTextQuery(alloc, disjuncts_query);
    try std.testing.expect(disjuncts_query == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), disjuncts_query.bool_query.should.len);
    try std.testing.expect(disjuncts_query.bool_query.should[0] == .term);
    try std.testing.expectEqualStrings("/tier", disjuncts_query.bool_query.should[0].term.field);
    try std.testing.expectEqualStrings("gold", disjuncts_query.bool_query.should[0].term.term);
    try std.testing.expect(disjuncts_query.bool_query.should[1] == .term);
    try std.testing.expectEqualStrings("/tier", disjuncts_query.bool_query.should[1].term.field);
    try std.testing.expectEqualStrings("bronze", disjuncts_query.bool_query.should[1].term.term);
}

test "api query contract normalizes canonical query with legacy shorthands" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {
        \\    "bool": {
        \\      "must": [{"match":{"field":"body","text":"raft"}}],
        \\      "filter": [
        \\        {"term":{"path":"/tenant","value":"acme"}},
        \\        {"terms":{"path":"/tier","values":["gold",2,true]}}
        \\      ],
        \\      "must_not": [
        \\        {"exists":{"path":"/deleted_at"}},
        \\        {"term":{"path":"/archived","value":true}}
        \\      ]
        \\    }
        \\  },
        \\  "full_text_search": {"term":{"body":"legacy"}},
        \\  "filter_query": {"term":{"status":"published"}},
        \\  "exclusion_query": {"term":{"status":"deleted"}}
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), parsed.req.full_text.?.bool_query.must.len);
    try std.testing.expect(parsed.req.full_text.?.bool_query.must[0] == .match);
    try std.testing.expect(parsed.req.full_text.?.bool_query.must[1] == .term);
    try std.testing.expectEqualStrings("raft", parsed.req.full_text.?.bool_query.must[0].match.text);
    try std.testing.expectEqualStrings("legacy", parsed.req.full_text.?.bool_query.must[1].term.term);

    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"must\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"path\":\"/tenant\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"values\":[\"gold\",2,true]") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.filter_query_json, "\"status\":\"published\"") != null);

    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"should\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"minimum_should_match\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"exists\":{\"path\":\"/deleted_at\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"path\":\"/archived\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, parsed.req.exclusion_query_json, "\"status\":\"deleted\"") != null);
}

test "api query contract treats canonical typed scalar term as structured filter" {
    const alloc = std.testing.allocator;
    const body =
        \\{"query":{"term":{"path":"/published","value":true}}}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.full_text.? == .match_all);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/published\",\"value\":true}}", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("", parsed.req.exclusion_query_json);
}

test "api query contract includes stored source when fields are omitted" {
    const alloc = std.testing.allocator;
    const body =
        \\{"full_text_search":{"match":"needle","field":"content"}}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.include_all_fields);
    try std.testing.expect(parsed.req.include_stored);
    try std.testing.expect(!parsed.req.defer_stored_projection);
    try std.testing.expectEqual(@as(usize, 0), parsed.req.fields.len);
}

test "api query contract projects stored source when explicit fields are supplied" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match": "needle", "field": "content"},
        \\  "fields": ["path", "filename"]
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(!parsed.req.include_all_fields);
    try std.testing.expect(parsed.req.include_stored);
    try std.testing.expect(parsed.req.defer_stored_projection);
    try std.testing.expectEqual(@as(usize, 2), parsed.req.fields.len);
    try std.testing.expectEqualStrings("path", parsed.req.fields[0]);
    try std.testing.expectEqualStrings("filename", parsed.req.fields[1]);
}

test "api query contract accepts internal normalized filter json on internal query route" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match": "hello", "field": "body"},
        \\  "_filter_query_json": "{\"term\":{\"path\":\"/status\",\"value\":\"published\"}}",
        \\  "_exclusion_query_json": "{\"term\":{\"path\":\"/deleted\",\"value\":true}}"
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"published\"}}", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", parsed.req.exclusion_query_json);
}

test "api query contract normalizes public scalar filters before forwarding" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "full_text_search": {"match": "hello", "field": "body"},
        \\  "filter_query": {"term": "published", "field": "status"},
        \\  "exclusion_query": {"term": "gamma", "field": "title"}
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("{\"term\":{\"status\":\"published\"}}", parsed.req.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"title\":\"gamma\"}}", parsed.req.exclusion_query_json);
}

test "api query contract preflight summarizes query lanes and result refs" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "embeddings": {"body_embedding": [1.0, 0.0, 0.0]},
        \\  "indexes": ["body_embedding"],
        \\  "limit": 7,
        \\  "count": true,
        \\  "profile": true,
        \\  "fields": ["title"],
        \\  "aggregations": {
        \\    "by_status": {
        \\      "type": "terms",
        \\      "field": "status",
        \\      "sub_aggregations": {
        \\        "doc_count": {
        \\          "type": "count",
        \\          "field": "status"
        \\        }
        \\      }
        \\    }
        \\  },
        \\  "graph_searches": {
        \\    "seeded": {
        \\      "type": "neighbors",
        \\      "index_name": "doc_graph",
        \\      "start_nodes": {"result_ref": "$fused_results", "limit": 3}
        \\    },
        \\    "related": {
        \\      "type": "neighbors",
        \\      "index_name": "doc_graph",
        \\      "start_nodes": {"result_ref": "$graph_results.seeded", "limit": 3}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    var summary = try preflightQueryRequestAlloc(std.testing.allocator, parsed.value);
    defer summary.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), summary.full_text_indexes.len);
    try std.testing.expectEqualStrings("full_text", summary.full_text_indexes[0]);
    try std.testing.expectEqual(@as(usize, 1), summary.embedding_indexes.len);
    try std.testing.expectEqualStrings("body_embedding", summary.embedding_indexes[0]);
    try std.testing.expectEqual(@as(usize, 1), summary.graph_indexes.len);
    try std.testing.expectEqualStrings("doc_graph", summary.graph_indexes[0]);
    try std.testing.expectEqual(@as(usize, 6), summary.result_refs.len);
    var saw_full_text = false;
    var saw_named_embedding = false;
    var saw_embeddings = false;
    var saw_fused = false;
    var saw_seeded = false;
    var saw_graph = false;
    for (summary.result_refs) |result_ref| {
        if (std.mem.eql(u8, result_ref, "$full_text_results")) saw_full_text = true;
        if (std.mem.eql(u8, result_ref, "body_embedding")) saw_named_embedding = true;
        if (std.mem.eql(u8, result_ref, "$embeddings_results")) saw_embeddings = true;
        if (std.mem.eql(u8, result_ref, "$fused_results")) saw_fused = true;
        if (std.mem.eql(u8, result_ref, "$graph_results.seeded")) saw_seeded = true;
        if (std.mem.eql(u8, result_ref, "$graph_results.related")) saw_graph = true;
    }
    try std.testing.expect(saw_full_text);
    try std.testing.expect(saw_named_embedding);
    try std.testing.expect(saw_embeddings);
    try std.testing.expect(saw_fused);
    try std.testing.expect(saw_seeded);
    try std.testing.expect(saw_graph);
    try std.testing.expectEqual(@as(usize, 2), summary.graph_query_order.len);
    try std.testing.expectEqualStrings("seeded", summary.graph_query_order[0]);
    try std.testing.expectEqualStrings("related", summary.graph_query_order[1]);
    try std.testing.expectEqual(@as(u32, 7), summary.requested_limit);
    try std.testing.expectEqual(@as(u32, 0), summary.requested_offset);
    try std.testing.expectEqual(@as(u32, 2), summary.base_result_set_count);
    try std.testing.expectEqual(@as(u32, 2), summary.graph_query_count);
    try std.testing.expect(summary.requires_fusion);
    try std.testing.expect(summary.count_only);
    try std.testing.expect(summary.profile_requested);
    try std.testing.expect(summary.include_stored);
    try std.testing.expectEqual(@as(u32, 2), summary.aggregation_count);
}

test "api query contract preflight rejects count with reranker" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "count": true,
        \\  "reranker": {"provider":"cohere","model":"rerank-english-v3.0","field":"body","top_n":5}
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed.value));
}

test "api query contract preflight rejects cursor pagination without sort" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "search_after": ["2025-01-01", "doc-9"]
        \\}
    , .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedQueryRequest, preflightQueryRequestAlloc(std.testing.allocator, parsed.value));
}

test "api query contract parses packed dense embeddings via antfly-json" {
    const alloc = std.testing.allocator;
    const body =
        \\{"embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"},"indexes":["dense_idx"],"limit":3}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.dense_queries.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.req.sparse_queries.len);
    try std.testing.expectEqual(@as(u32, 3), parsed.req.dense_queries[0].query.k);
    try std.testing.expectEqual(@as(usize, 3), parsed.req.dense_queries[0].query.vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), parsed.req.dense_queries[0].query.vector[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), parsed.req.dense_queries[0].query.vector[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), parsed.req.dense_queries[0].query.vector[2], 0.0001);
}

test "api query contract does not use dense fast path for composed vector requests" {
    const alloc = std.testing.allocator;
    const body =
        \\{"embeddings":{"dense_idx":"AACAPwAAAEAAAEBA"},"indexes":["dense_idx"],"full_text_search":{"match":"alpha","field":"body"},"filter_query":{"term":{"status":"active"}},"exclusion_query":{"term":{"category":"archived"}},"limit":3}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.dense_queries.len);
    try std.testing.expect(parsed.req.full_text != null);
    try std.testing.expect(parsed.req.filter_query_json.len > 0);
    try std.testing.expect(parsed.req.exclusion_query_json.len > 0);
}

test "api query contract parses packed sparse embeddings via antfly-json" {
    const alloc = std.testing.allocator;
    const body =
        \\{"embeddings":{"sparse_idx":{"packed_indices":"AQAAAAUAAAA=","packed_values":"AAAAPwAAQD8=","k":4}},"indexes":["sparse_idx"],"limit":9}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), parsed.req.dense_queries.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.sparse_queries.len);
    try std.testing.expectEqual(@as(u32, 4), parsed.req.sparse_queries[0].query.k);
    try std.testing.expectEqualSlices(u32, &.{ 1, 5 }, parsed.req.sparse_queries[0].query.indices);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), parsed.req.sparse_queries[0].query.values[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), parsed.req.sparse_queries[0].query.values[1], 0.0001);
}

test "api query contract parses explicit algebraic aggregation join" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "by_segment": {
        \\    "type": "terms",
        \\    "field": "segment",
        \\    "algebraic_join": {
        \\      "name": "orders_customers",
        \\      "kind": "bucket",
        \\      "group_side": "right",
        \\      "measure_side": "left"
        \\    },
        \\    "sub_aggregations": {
        \\      "amount": {"type": "sum", "field": "amount"}
        \\    }
        \\  }
        \\}
    ;
    const requests = try parseAggregationRequestsJson(alloc, aggregations_json);
    defer freeAggregationRequests(alloc, requests);

    try std.testing.expectEqual(@as(usize, 1), requests.len);
    const join = requests[0].algebraic_join.?;
    try std.testing.expectEqualStrings("orders_customers", join.name);
    try std.testing.expectEqual(db_mod.algebraic.join.TemporalMode.bucket, join.kind);
    try std.testing.expectEqualStrings("right", join.group_side.?);
    try std.testing.expectEqualStrings("left", join.measure_side.?);
    try std.testing.expectEqual(@as(usize, 1), requests[0].aggregations.len);
    try std.testing.expect(requests[0].aggregations[0].algebraic_join == null);
}

test "api query contract parses multi field terms aggregation" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "by_customer_product": {
        \\    "type": "terms",
        \\    "fields": ["customer", "product"],
        \\    "sub_aggregations": {
        \\      "amount": {"type": "sum", "field": "amount"}
        \\    }
        \\  }
        \\}
    ;
    const requests = try parseAggregationRequestsJson(alloc, aggregations_json);
    defer freeAggregationRequests(alloc, requests);

    try std.testing.expectEqual(@as(usize, 1), requests.len);
    try std.testing.expectEqualStrings("customer", requests[0].field);
    try std.testing.expectEqual(@as(usize, 2), requests[0].fields.len);
    try std.testing.expectEqualStrings("customer", requests[0].fields[0]);
    try std.testing.expectEqualStrings("product", requests[0].fields[1]);
    try std.testing.expectEqual(@as(usize, 1), requests[0].aggregations.len);
    try std.testing.expectEqualStrings("amount", requests[0].aggregations[0].field);
}

test "api query contract rejects multi field non terms aggregation" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "amount": {
        \\    "type": "sum",
        \\    "fields": ["amount", "tax"]
        \\  }
        \\}
    ;

    try std.testing.expectError(error.InvalidQueryRequest, parseAggregationRequestsJson(alloc, aggregations_json));
}

test "api query contract rejects conflicting terms field and fields" {
    const alloc = std.testing.allocator;
    const aggregations_json =
        \\{
        \\  "by_customer_product": {
        \\    "type": "terms",
        \\    "field": "tenant",
        \\    "fields": ["customer", "product"]
        \\  }
        \\}
    ;

    try std.testing.expectError(error.InvalidQueryRequest, parseAggregationRequestsJson(alloc, aggregations_json));
}

test "api query contract exposes native doc id constraint envelope for non-query worker protocols" {
    const alloc = std.testing.allocator;
    const source = db_mod.types.SearchRequest{
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{},
        .exclude_doc_ids = &.{"doc:c"},
    };
    const envelope = nativeDocIdConstraintEnvelopeFromSearchRequest(source);
    try std.testing.expect(envelope.hasConstraints());

    const encoded = try encodeNativeDocIdConstraintEnvelopeAlloc(alloc, envelope);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"positive_filter\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"include_doc_ids\":[]") != null);

    var parsed = try parseNativeDocIdConstraintEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 0), parsed.constraints.include_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.constraints.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.constraints.exclude_doc_ids[0]);
}

test "api query contract normalizes native include doc ids to a positive envelope" {
    const alloc = std.testing.allocator;
    const envelope = NativeDocIdConstraintEnvelope{
        .positive_filter = false,
        .include_doc_ids = &.{ "doc:b", "doc:a", "doc:b", "doc:c" },
        .exclude_doc_ids = &.{ "doc:d", "doc:c", "doc:c" },
    };

    const encoded = try encodeNativeDocIdConstraintEnvelopeAlloc(alloc, envelope);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"positive_filter\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"include_doc_ids\":[\"doc:a\",\"doc:b\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"exclude_doc_ids\":[\"doc:c\",\"doc:d\"]") != null);

    var parsed = try parseNativeDocIdConstraintEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), parsed.constraints.include_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", parsed.constraints.include_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", parsed.constraints.include_doc_ids[1]);
    try std.testing.expectEqual(@as(usize, 2), parsed.constraints.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.constraints.exclude_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:d", parsed.constraints.exclude_doc_ids[1]);
}

test "api query contract exposes typed tensor access path envelope for worker protocols" {
    const alloc = std.testing.allocator;
    const dictionary = algebraic_lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const path = algebraic_ir.PhysicalAccessPath{
        .owner = "body_terms",
        .layout = .full_text_postings,
        .dictionary = dictionary,
        .fragments = &.{ .slice, .automaton_select },
        .output_dims = &.{.doc},
    };

    const encoded = try encodeAlgebraicTensorAccessPathEnvelopeAlloc(alloc, path);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"owner\":\"body_terms\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"full_text_postings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dictionary\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"label_kind\":\"analyzed_term\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"fragments\":[\"slice\",\"automaton_select\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"output_dims\":[\"doc\"]") != null);

    var parsed = try parseAlgebraicTensorAccessPathEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    const parsed_path = parsed.asAccessPath();
    try std.testing.expectEqualStrings(path.owner, parsed_path.owner);
    try std.testing.expectEqual(path.layout, parsed_path.layout);
    try std.testing.expect(parsed_path.dictionary != null);
    try std.testing.expect(dictionary.eql(parsed_path.dictionary.?));
    try std.testing.expectEqualSlices(algebraic_ir.TensorFragment, path.fragments, parsed_path.fragments);
    try std.testing.expectEqualSlices(algebraic_ir.Dimension, path.output_dims, parsed_path.output_dims);
    try std.testing.expectEqualSlices(algebraic_law.Id, path.law_ids, parsed_path.law_ids);
}

test "api query contract exposes typed tensor expression envelope for worker protocols" {
    const alloc = std.testing.allocator;
    const dictionary = algebraic_lexical.DictionaryIdentity.canonicalScalar("docs", "/customer", .string, "json-scalar-v1", "kind-qualified");
    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "sum_by_customer",
        .layout = .materialized_expr,
        .dictionary = dictionary,
        .law_id = .sum,
    };

    const encoded = try encodeAlgebraicTensorExprEnvelopeAlloc(alloc, expr);
    defer alloc.free(encoded);
    const expected_expr_id = try algebraic_ir.tensorExprIdAlloc(alloc, expr);
    defer alloc.free(expected_expr_id);
    const expected_expr_id_json = try std.fmt.allocPrint(alloc, "\"expr_id\":\"{s}\"", .{expected_expr_id});
    defer alloc.free(expected_expr_id_json);
    try std.testing.expect(std.mem.indexOf(u8, encoded, expected_expr_id_json) != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"fragment\":\"reduce\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"input_dims\":[\"doc\",\"scalar\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"output_dims\":[\"bucket\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"semantic_id\":\"sum_by_customer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"materialized_expr\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"label_kind\":\"canonical_scalar\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"law_id\":\"sum\"") != null);

    var parsed = try parseAlgebraicTensorExprEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    const parsed_expr = parsed.asExpr();
    try std.testing.expectEqualStrings(expected_expr_id, parsed.expr_id);
    try std.testing.expectEqual(expr.fragment, parsed_expr.fragment);
    try std.testing.expectEqualSlices(algebraic_ir.Dimension, expr.input_dims, parsed_expr.input_dims);
    try std.testing.expectEqualSlices(algebraic_ir.Dimension, expr.output_dims, parsed_expr.output_dims);
    try std.testing.expectEqualStrings(expr.semantic_id.?, parsed_expr.semantic_id.?);
    try std.testing.expect(parsed_expr.owner == null);
    try std.testing.expectEqual(expr.layout.?, parsed_expr.layout.?);
    try std.testing.expect(parsed_expr.dictionary != null);
    try std.testing.expect(dictionary.eql(parsed_expr.dictionary.?));
    try std.testing.expectEqual(expr.law_id.?, parsed_expr.law_id.?);

    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, parsed_expr)).?;
    defer plan.deinit(alloc);
    try std.testing.expectEqualStrings(parsed.expr_id, plan.expr_id);
    try std.testing.expect(algebraic_ir.accessPathCanSatisfy(plan.access_path, parsed_expr).safe());

    var tampered = try std.ArrayListUnmanaged(u8).initCapacity(alloc, encoded.len + 16);
    defer tampered.deinit(alloc);
    try tampered.appendSlice(alloc, encoded);
    const id_pos = std.mem.indexOf(u8, tampered.items, expected_expr_id) orelse return error.TestUnexpectedResult;
    tampered.items[id_pos] = if (tampered.items[id_pos] == 'x') 'y' else 'x';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorExprEnvelopeAlloc(alloc, tampered.items));

    const missing_id =
        \\{
        \\  "fragment": "reduce",
        \\  "input_dims": ["doc", "scalar"],
        \\  "output_dims": ["bucket"],
        \\  "semantic_id": "sum_by_customer",
        \\  "owner": "expr:sum_by_customer",
        \\  "layout": "materialized_expr",
        \\  "law_id": "sum"
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorExprEnvelopeAlloc(alloc, missing_id));
}

test "api query contract exposes typed tensor program envelope for worker protocols" {
    const alloc = std.testing.allocator;
    const dictionary = algebraic_lexical.DictionaryIdentity.analyzedText("docs", "body", "default");
    const input_expr = algebraic_ir.TensorExpr{
        .fragment = .automaton_select,
        .output_dims = &.{.doc},
        .dictionary = dictionary,
    };
    const reduce_step = algebraic_ir.TensorProgramStep{
        .expr = .{
            .fragment = .reduce,
            .input_dims = &.{.doc},
            .output_dims = &.{.bucket},
            .law_id = .count,
            .metadata = "fold:v1:bucket-body-count",
        },
        .inputs = &.{.{ .input = 0 }},
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{input_expr},
        .steps = &.{reduce_step},
        .output = .{ .step = 0 },
        .outputs = &.{ .{ .input = 0 }, .{ .step = 0 } },
    };
    const encoded = try encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, program);
    defer alloc.free(encoded);
    const expected_program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, program);
    defer alloc.free(expected_program_id);
    const expected_program_id_json = try std.fmt.allocPrint(alloc, "\"program_id\":\"{s}\"", .{expected_program_id});
    defer alloc.free(expected_program_id_json);
    try std.testing.expect(std.mem.indexOf(u8, encoded, expected_program_id_json) != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"inputs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"steps\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"input\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"step\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"outputs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"label_kind\":\"analyzed_term\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"metadata\":\"fold:v1:bucket-body-count\"") != null);

    var parsed = try parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings(expected_program_id, parsed.program_id);
    var view = try parsed.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), view.program.outputs.len);
    try std.testing.expectEqualStrings("fold:v1:bucket-body-count", view.program.steps[0].expr.metadata.?);
    const reparsed_id = try algebraic_ir.tensorProgramIdAlloc(alloc, view.program);
    defer alloc.free(reparsed_id);
    try std.testing.expectEqualStrings(expected_program_id, reparsed_id);

    const paths = [_]algebraic_ir.PhysicalAccessPath{
        algebraic_ir.lexicalAccessPath("body_terms", .full_text_postings, dictionary, true),
    };
    try std.testing.expect((try algebraic_ir.tensorProgramProof(alloc, &paths, view.program)).safe());

    var tampered = try std.ArrayListUnmanaged(u8).initCapacity(alloc, encoded.len + 16);
    defer tampered.deinit(alloc);
    try tampered.appendSlice(alloc, encoded);
    const id_pos = std.mem.indexOf(u8, tampered.items, expected_program_id) orelse return error.TestUnexpectedResult;
    tampered.items[id_pos] = if (tampered.items[id_pos] == 'x') 'y' else 'x';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorProgramEnvelopeAlloc(alloc, tampered.items));

    var bad_output_ref = try std.ArrayListUnmanaged(u8).initCapacity(alloc, encoded.len + 16);
    defer bad_output_ref.deinit(alloc);
    try bad_output_ref.appendSlice(alloc, encoded);
    const output_ref_pos = std.mem.indexOf(u8, bad_output_ref.items, "\"output\":{\"kind\":\"step\",\"index\":0}") orelse return error.TestUnexpectedResult;
    bad_output_ref.items[output_ref_pos + "\"output\":{\"kind\":\"step\",\"index\":".len] = '9';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicTensorProgramEnvelopeAlloc(alloc, bad_output_ref.items));
}

test "api query contract carries vector worker tensor program and native constraints together" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const candidate_input = algebraic_ir.TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "native_doc_id_constraints",
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{candidate_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .input_dims = &.{.doc},
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .step = 0 },
    };
    const constraints = NativeDocIdConstraintEnvelope{
        .positive_filter = true,
        .include_doc_ids = &.{ "doc:a", "doc:b" },
        .exclude_doc_ids = &.{"doc:c"},
    };
    const encoded = try encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 0.25, 0.5, 1.0 }, .k = 7 } },
        .{
            .fields = @constCast((&[_][]const u8{ "title", "score" })[0..]),
            .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
            .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
            .filter_prefix = "tenant/a/",
            .filter_ids = &.{ 42, 99 },
            .exclude_ids = &.{7},
            .require_algebraic_filter_resolution = true,
            .include_all_fields = false,
            .defer_stored_projection = true,
            .limit = 9,
            .offset = 2,
            .count_only = true,
            .profile = true,
            .include_stored = false,
            .search_effort = 0.75,
            .distance_over = 0.1,
            .distance_under = 0.9,
            .return_mode = .parent_with_chunks,
            .max_chunks_per_parent = 2,
        },
        constraints,
        &.{access_path},
        program,
    );
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_name\":\"dense_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"dense_vector\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"dense\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"options\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"native_doc_id_constraints\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_access_paths\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);

    const tampered_target = try alloc.dupe(u8, encoded);
    defer alloc.free(tampered_target);
    const index_name_prefix = "\"index_name\":\"";
    const index_name_pos = std.mem.indexOf(u8, tampered_target, index_name_prefix) orelse return error.TestUnexpectedResult;
    const index_name_start = index_name_pos + index_name_prefix.len;
    tampered_target[index_name_start] = if (tampered_target[index_name_start] == 'd') 'x' else 'd';
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, tampered_target));

    var parsed = try parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("dense_idx", parsed.index_name);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.dense_vector, parsed.layout);
    try std.testing.expectEqual(@as(u32, 7), parsed.query.dense.k);
    try std.testing.expectEqual(@as(u32, 9), parsed.options.limit);
    try std.testing.expectEqual(@as(u32, 2), parsed.options.offset);
    try std.testing.expect(parsed.options.count_only);
    try std.testing.expect(parsed.options.profile);
    try std.testing.expect(!parsed.options.include_stored);
    try std.testing.expect(!parsed.options.include_all_fields);
    try std.testing.expect(parsed.options.defer_stored_projection);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", parsed.options.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", parsed.options.exclusion_query_json);
    try std.testing.expect(parsed.options.require_algebraic_filter_resolution);
    try std.testing.expectEqualStrings("tenant/a/", parsed.options.filter_prefix);
    try std.testing.expectEqual(@as(usize, 2), parsed.options.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 42), parsed.options.filter_ids[0]);
    try std.testing.expectEqual(@as(u64, 99), parsed.options.filter_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), parsed.options.exclude_ids.len);
    try std.testing.expectEqual(@as(u64, 7), parsed.options.exclude_ids[0]);
    try std.testing.expectEqual(@as(usize, 2), parsed.options.fields.len);
    try std.testing.expectEqualStrings("title", parsed.options.fields[0]);
    try std.testing.expectEqualStrings("score", parsed.options.fields[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), parsed.options.search_effort.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), parsed.options.distance_over.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), parsed.options.distance_under.?, 0.0001);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, parsed.options.return_mode);
    try std.testing.expectEqual(@as(u32, 2), parsed.options.max_chunks_per_parent);
    try std.testing.expectEqual(@as(usize, 3), parsed.query.dense.vector.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), parsed.query.dense.vector[0], 0.0001);
    try std.testing.expect(parsed.native_doc_id_constraints.constraints.positive_filter);
    try std.testing.expectEqual(@as(usize, 2), parsed.native_doc_id_constraints.constraints.include_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.native_doc_id_constraints.constraints.exclude_doc_ids[0]);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.dense_vector, parsed.tensor_access_paths[0].layout);
    try std.testing.expect((try parsed.proveTensorProgramAlloc(alloc)).safe());

    var program_view = try parsed.tensor_program.asProgramAlloc(alloc);
    defer program_view.deinit(alloc);
    const program_id = try algebraic_ir.tensorProgramIdAlloc(alloc, program_view.program);
    defer alloc.free(program_id);
    try std.testing.expectEqualStrings(parsed.tensor_program.program_id, program_id);
}

test "api query contract carries sparse vector worker payload and proof" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "sparse_idx",
                .layout = .sparse_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    const encoded = try encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{ 3, 9, 27 }, .values = &.{ 1.0, 0.5, 0.25 }, .k = 5 } },
        .{},
        .{},
        &.{access_path},
        program,
    );
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"kind\":\"sparse\"") != null);

    var parsed = try parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("sparse_idx", parsed.index_name);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.sparse_vector, parsed.layout);
    try std.testing.expectEqual(@as(u32, 5), parsed.query.sparse.k);
    try std.testing.expectEqual(@as(usize, 3), parsed.query.sparse.indices.len);
    try std.testing.expectEqual(@as(u32, 9), parsed.query.sparse.indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), parsed.query.sparse.values[2], 0.0001);
    try std.testing.expect((try parsed.proveTensorProgramAlloc(alloc)).safe());
}

test "api query contract rejects sparse vector worker payload with mismatched indices and values" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "sparse_idx",
                .layout = .sparse_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{ 3, 9 }, .values = &.{1.0}, .k = 5 } },
        .{},
        .{},
        &.{access_path},
        program,
    ));

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"kind":"sparse","k":5,"indices":[3,9],"values":[1.0]}
    , .{});
    defer parsed.deinit();
    try std.testing.expectError(
        error.InvalidQueryRequest,
        parseAlgebraicVectorWorkerQueryAlloc(alloc, .sparse_vector, parsed.value),
    );
}

test "api query contract rejects vector worker non-finite numeric payloads" {
    const alloc = std.testing.allocator;
    const dense_access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const dense_program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{std.math.inf(f32)}, .k = 1 } },
        .{},
        .{},
        &.{dense_access_path},
        dense_program,
    ));
    const sparse_access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const sparse_program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "sparse_idx",
                .layout = .sparse_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{1}, .values = &.{std.math.nan(f32)}, .k = 1 } },
        .{},
        .{},
        &.{sparse_access_path},
        sparse_program,
    ));
    {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
            \\{"kind":"dense","k":1,"vector":[1e9999]}
        , .{});
        defer parsed.deinit();
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parseAlgebraicVectorWorkerQueryAlloc(alloc, .dense_vector, parsed.value),
        );
    }
    {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc,
            \\{"kind":"sparse","k":1,"indices":[7],"values":[-1e9999]}
        , .{});
        defer parsed.deinit();
        try std.testing.expectError(
            error.InvalidQueryRequest,
            parseAlgebraicVectorWorkerQueryAlloc(alloc, .sparse_vector, parsed.value),
        );
    }
}

test "api query contract rejects vector worker envelope without matching tensor proof" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("sparse_idx", .sparse_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "sparse_idx",
        .sparse_vector,
        .{ .sparse = .{ .indices = &.{ 1, 5 }, .values = &.{ 1.0, 0.5 }, .k = 3 } },
        .{},
        .{},
        &.{access_path},
        program,
    ));
}

test "api query contract rejects vector worker envelope when target does not match access path" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("other_dense_idx", .dense_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "other_dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 2 } },
        .{},
        .{},
        &.{access_path},
        program,
    ));
}

test "api query contract rejects vector worker envelope when primary output is not vector search" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const candidate_input = algebraic_ir.TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "native_doc_id_constraints",
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{candidate_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .input_dims = &.{.doc},
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .input = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 2 } },
        .{},
        .{},
        &.{access_path},
        program,
    ));
}

test "api query contract rejects vector worker envelope when native constraints are not consumed" {
    const alloc = std.testing.allocator;
    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const program = algebraic_ir.TensorProgram{
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
        }},
        .output = .{ .step = 0 },
    };
    try std.testing.expectError(error.InvalidQueryRequest, encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 1.0, 0.0 }, .k = 2 } },
        .{},
        .{ .positive_filter = true, .include_doc_ids = &.{"doc:a"} },
        &.{access_path},
        program,
    ));
}

test "api query contract accepts native doc id constraint envelope on internal query route" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {"match_all": {}},
        \\  "native_doc_id_constraints": {
        \\    "positive_filter": true,
        \\    "include_doc_ids": [],
        \\    "exclude_doc_ids": ["doc:c"]
        \\  }
        \\}
    ;

    var parsed = try parseQueryRequest(alloc, null, "docs", body);
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 0), parsed.req.filter_doc_ids.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", parsed.req.exclude_doc_ids[0]);
}

test "api query contract rejects legacy native doc id constraint fields" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "query": {"match_all": {}},
        \\  "_filter_doc_ids_positive": true
        \\}
    ;

    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", body));

    const old_arrays =
        \\{
        \\  "query": {"match_all": {}},
        \\  "_filter_doc_ids": ["doc:a"],
        \\  "_exclude_doc_ids": ["doc:b"]
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseQueryRequest(alloc, null, "docs", old_arrays));
}
