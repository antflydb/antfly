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
const generating_openapi = @import("antfly_generating_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const indexes_openapi = @import("antfly_indexes_openapi");
const generating = @import("antfly_generating");
const platform_time = @import("../platform/time.zig");
const db_mod = @import("../storage/db/mod.zig");
const query_contract = @import("query_contract.zig");

const AgentQuestion = metadata_openapi.AgentQuestion;
const AgentStatus = metadata_openapi.AgentStatus;

pub const GenerationRunner = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute_chain: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            chain: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) anyerror!generating.GenerateResult,
    };

    pub fn executeChain(
        self: GenerationRunner,
        alloc: std.mem.Allocator,
        chain: []const generating.ChainLink,
        messages: []const generating.ChatMessage,
    ) !generating.GenerateResult {
        return try self.vtable.execute_chain(self.ptr, alloc, chain, messages);
    }
};

pub fn buildQueryBuilderResponse(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    table_schema_fields: ?[]const []const u8,
) !metadata_openapi.QueryBuilderResult {
    return try buildQueryBuilderResponseWithGeneration(alloc, request, table_schema_fields, null);
}

pub const QueryBuilderTableContext = struct {
    schema_fields: []const []const u8 = &.{},
    full_text_indexes: []const []const u8 = &.{},
    semantic_indexes: []const []const u8 = &.{},
    graph_indexes: []const []const u8 = &.{},
    full_text_index_metadata: []const QueryBuilderFullTextIndex = &.{},
    embedding_index_metadata: []const QueryBuilderEmbeddingIndex = &.{},
    graph_index_metadata: []const QueryBuilderGraphIndex = &.{},
    plan_validator: ?QueryBuilderPlanValidator = null,
    runtime_query_request_validator: ?QueryBuilderRuntimeQueryRequestValidator = null,
};

pub const QueryBuilderCollectedContext = struct {
    table_context: QueryBuilderTableContext = .{},
    metadata_loaded: bool = false,

    pub fn effectiveTableContext(self: *const @This()) QueryBuilderTableContext {
        var out = self.table_context;
        if (self.metadata_loaded) out.plan_validator = metadataBackedPlanValidator(&self.table_context);
        return out;
    }
};

pub const QueryBuilderFullTextIndex = struct {
    name: []const u8,
    fields: []const []const u8 = &.{},
};

pub const QueryBuilderEmbeddingIndex = struct {
    name: []const u8,
    sparse: bool = false,
    dimension: ?i64 = null,
    model: ?[]const u8 = null,
};

pub const QueryBuilderGraphIndex = struct {
    name: []const u8,
    edge_types: []const QueryBuilderGraphEdgeType = &.{},
};

pub const QueryBuilderGraphEdgeType = struct {
    name: []const u8,
    topology: ?[]const u8 = null,
};

pub const QueryBuilderPlanValidator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        validate_graph_searches: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            request: metadata_openapi.QueryBuilderRequest,
            graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
        ) anyerror!?[]const u8 = null,
        validate_bleve_query: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            request: metadata_openapi.QueryBuilderRequest,
            query: std.json.Value,
        ) anyerror!?[]const u8 = null,
        validate_query_request: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            request: metadata_openapi.QueryBuilderRequest,
            query_request: metadata_openapi.QueryRequest,
            retrieval_query_request: ?metadata_openapi.RetrievalQueryRequest,
            specialist: []const u8,
        ) anyerror!?[]const u8 = null,
    };

    pub fn validateGraphSearches(
        self: QueryBuilderPlanValidator,
        alloc: std.mem.Allocator,
        request: metadata_openapi.QueryBuilderRequest,
        graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
    ) !?[]const u8 {
        const func = self.vtable.validate_graph_searches orelse return null;
        return try func(self.ptr, alloc, request, graph_searches);
    }

    pub fn validateBleveQuery(
        self: QueryBuilderPlanValidator,
        alloc: std.mem.Allocator,
        request: metadata_openapi.QueryBuilderRequest,
        query: std.json.Value,
    ) !?[]const u8 {
        const func = self.vtable.validate_bleve_query orelse return null;
        return try func(self.ptr, alloc, request, query);
    }

    pub fn validateQueryRequest(
        self: QueryBuilderPlanValidator,
        alloc: std.mem.Allocator,
        request: metadata_openapi.QueryBuilderRequest,
        query_request: metadata_openapi.QueryRequest,
        retrieval_query_request: ?metadata_openapi.RetrievalQueryRequest,
        specialist: []const u8,
    ) !?[]const u8 {
        const func = self.vtable.validate_query_request orelse return null;
        return try func(self.ptr, alloc, request, query_request, retrieval_query_request, specialist);
    }
};

pub const QueryBuilderRuntimeQueryRequestValidator = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        validate_query_request: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            query_request: metadata_openapi.QueryRequest,
        ) anyerror!?[]const u8,
        preflight_query_request: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            query_request: metadata_openapi.QueryRequest,
            max_work: u32,
        ) anyerror!?db_mod.RuntimePreflightSummary = null,
    };

    pub fn validateQueryRequest(
        self: QueryBuilderRuntimeQueryRequestValidator,
        alloc: std.mem.Allocator,
        query_request: metadata_openapi.QueryRequest,
    ) !?[]const u8 {
        return try self.vtable.validate_query_request(self.ptr, alloc, query_request);
    }

    pub fn preflightQueryRequest(
        self: QueryBuilderRuntimeQueryRequestValidator,
        alloc: std.mem.Allocator,
        query_request: metadata_openapi.QueryRequest,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const func = self.vtable.preflight_query_request orelse return null;
        return try func(self.ptr, alloc, query_request, max_work);
    }
};

pub const QueryPreflightMode = enum { validate, plan, estimate };

pub const QueryPreflightOptions = struct {
    mode: QueryPreflightMode = .validate,
    require_executable: bool = false,
    max_work: u32 = 0,
};

pub const QueryPreflightDiagnosticSeverity = enum { warning, @"error" };

pub const QueryPreflightDiagnostic = struct {
    severity: QueryPreflightDiagnosticSeverity,
    code: []const u8,
    path: []const u8,
    message: []const u8,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.code);
        alloc.free(self.path);
        alloc.free(self.message);
    }
};

pub const QueryPreflightPlanSummary = struct {
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
        deinitOwnedStringSlice(alloc, self.full_text_indexes);
        deinitOwnedStringSlice(alloc, self.embedding_indexes);
        deinitOwnedStringSlice(alloc, self.graph_indexes);
        deinitOwnedStringSlice(alloc, self.result_refs);
        deinitOwnedStringSlice(alloc, self.graph_query_order);
    }
};

pub const QueryPreflightTextEstimate = struct {
    name: []const u8,
    doc_count: u64 = 0,
    chunk_backed: bool = false,
    group_chunk_parents: bool = false,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
    }
};

pub const QueryPreflightEmbeddingEstimate = struct {
    name: []const u8,
    sparse: bool = false,
    doc_count: u64 = 0,
    dims: u32 = 0,
    chunk_backed: bool = false,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
    }
};

pub const QueryPreflightGraphEstimate = struct {
    name: []const u8,
    edge_count: u64 = 0,
    node_count: u64 = 0,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
    }
};

pub const QueryPreflightTermEstimate = struct {
    term: []const u8,
    doc_freq: u32 = 0,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.term);
    }
};

pub const QueryPreflightTextQueryEstimate = struct {
    field: []const u8,
    global_doc_count: u32 = 0,
    avg_doc_len: f32 = 0,
    term_doc_freqs: []const QueryPreflightTermEstimate = &.{},

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.field);
        for (self.term_doc_freqs) |*item| item.deinit(alloc);
        if (self.term_doc_freqs.len > 0) alloc.free(@constCast(self.term_doc_freqs));
    }
};

pub const QueryPreflightEstimateSummary = struct {
    pub const LatencyHeuristic = enum {
        low,
        medium,
        high,
    };

    pub const EstimateKind = enum {
        unknown,
        lower_bound,
        sampled,
        upper_bound,
        heuristic,
    };

    pub const Confidence = enum {
        low,
        medium,
        high,
    };

    pub const ScoreComponent = struct {
        factor: []const u8,
        points: u32 = 0,

        pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
            alloc.free(self.factor);
        }
    };

    pub const SelectivityHeuristic = enum {
        unknown,
        broad,
        medium,
        narrow,
    };

    text_indexes: []const QueryPreflightTextEstimate = &.{},
    embedding_indexes: []const QueryPreflightEmbeddingEstimate = &.{},
    graph_indexes: []const QueryPreflightGraphEstimate = &.{},
    text_query_stats: []const QueryPreflightTextQueryEstimate = &.{},
    doc_id_value_count: u32 = 0,
    filter_id_count: u32 = 0,
    exclude_id_count: u32 = 0,
    numeric_range_clause_count: u32 = 0,
    term_range_clause_count: u32 = 0,
    ip_range_clause_count: u32 = 0,
    bool_field_clause_count: u32 = 0,
    geo_filter_clause_count: u32 = 0,
    positive_id_result_upper_bound: ?u32 = null,
    structured_filter_doc_count_estimate: ?u64 = null,
    structured_filter_doc_count_lower_bound: ?u64 = null,
    structured_filter_doc_count_sample_estimate: ?u64 = null,
    structured_filter_count_exact: bool = false,
    structured_filter_count_sample_size: u32 = 0,
    structured_filter_count_budget_limit: ?u64 = null,
    text_result_upper_bound: ?u32 = null,
    text_term_doc_freq_total: u64 = 0,
    corpus_doc_count_estimate: ?u64 = null,
    selectivity_estimate_kind: EstimateKind = .unknown,
    selectivity_confidence: Confidence = .low,
    selectivity_lower_bound_ratio: ?f32 = null,
    selectivity_sample_ratio: ?f32 = null,
    selectivity_upper_bound_ratio: ?f32 = null,
    selectivity_heuristic: SelectivityHeuristic = .unknown,
    selectivity_risk_factors: []const []const u8 = &.{},
    result_doc_estimate: ?u32 = null,
    result_doc_upper_bound: ?u32 = null,
    shard_result_window: u32 = 0,
    shard_result_window_total: u64 = 0,
    stored_projection_doc_upper_bound_total: u64 = 0,
    effective_stored_projection_doc_estimate_total: ?u64 = null,
    effective_stored_projection_doc_upper_bound_total: u64 = 0,
    rerank_doc_upper_bound: u32 = 0,
    effective_rerank_doc_estimate: ?u32 = null,
    effective_rerank_doc_upper_bound: u32 = 0,
    aggregation_may_scan_full_results: bool = false,
    aggregation_second_pass_doc_estimate: ?u32 = null,
    aggregation_second_pass_doc_upper_bound: ?u32 = null,
    latency_risk_factors: []const []const u8 = &.{},
    latency_score_components: []const ScoreComponent = &.{},
    latency_estimate_kind: EstimateKind = .heuristic,
    latency_confidence: Confidence = .low,
    latency_heuristic_score: u32 = 0,
    latency_heuristic: LatencyHeuristic = .low,
    shard_count: u32 = 0,
    remote_shard_count: u32 = 0,
    dense_query_count: u32 = 0,
    vector_worker_candidate_count: u32 = 0,
    vector_worker_fallback_count: u32 = 0,
    vector_worker_filter_constraint_count: u32 = 0,
    vector_worker_requires_algebraic_filter_resolution: bool = false,
    dense_effective_k_total: u64 = 0,
    dense_search_width_total: u64 = 0,
    dense_search_width_max: u32 = 0,
    dense_epsilon_max: f32 = 0,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        for (self.text_indexes) |*item| item.deinit(alloc);
        if (self.text_indexes.len > 0) alloc.free(@constCast(self.text_indexes));
        for (self.embedding_indexes) |*item| item.deinit(alloc);
        if (self.embedding_indexes.len > 0) alloc.free(@constCast(self.embedding_indexes));
        for (self.graph_indexes) |*item| item.deinit(alloc);
        if (self.graph_indexes.len > 0) alloc.free(@constCast(self.graph_indexes));
        for (self.text_query_stats) |*item| item.deinit(alloc);
        if (self.text_query_stats.len > 0) alloc.free(@constCast(self.text_query_stats));
        deinitOwnedStringSlice(alloc, self.selectivity_risk_factors);
        deinitOwnedStringSlice(alloc, self.latency_risk_factors);
        for (self.latency_score_components) |*item| item.deinit(alloc);
        if (self.latency_score_components.len > 0) alloc.free(@constCast(self.latency_score_components));
    }
};

pub const QueryPreflightResult = struct {
    diagnostics: []const QueryPreflightDiagnostic = &.{},
    plan_summary: ?QueryPreflightPlanSummary = null,
    estimate_summary: ?QueryPreflightEstimateSummary = null,

    pub fn deinit(self: *const @This(), alloc: std.mem.Allocator) void {
        for (self.diagnostics) |*diagnostic| diagnostic.deinit(alloc);
        if (self.diagnostics.len > 0) alloc.free(@constCast(self.diagnostics));
        if (self.plan_summary) |plan_summary| plan_summary.deinit(alloc);
        if (self.estimate_summary) |estimate_summary| estimate_summary.deinit(alloc);
    }
};

const QueryBuilderStepPreflightDetails = struct {
    mode: []const u8,
    diagnostics: []const QueryPreflightDiagnostic = &.{},
    plan_summary: ?QueryPreflightPlanSummary = null,
    estimate_summary: ?QueryPreflightEstimateSummary = null,
};

const QueryBuilderStepDetailsJson = struct {
    table: ?[]const u8 = null,
    schema_fields: []const []const u8 = &.{},
    example_document_count: usize = 0,
    mode: []const u8,
    output: []const u8,
    specialist: []const u8,
    preflight: ?QueryBuilderStepPreflightDetails = null,
};

pub fn collectQueryBuilderContext(table_context: ?QueryBuilderTableContext) QueryBuilderCollectedContext {
    return .{
        .table_context = table_context orelse .{},
        .metadata_loaded = table_context != null,
    };
}

pub fn metadataBackedPlanValidator(context: *const QueryBuilderTableContext) QueryBuilderPlanValidator {
    return .{
        .ptr = @ptrCast(@constCast(context)),
        .vtable = &.{
            .validate_graph_searches = metadataValidateGraphSearches,
            .validate_bleve_query = metadataValidateBleveQuery,
            .validate_query_request = metadataValidateQueryRequest,
        },
    };
}

fn metadataValidateGraphSearches(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
) !?[]const u8 {
    _ = request;
    const context: *const QueryBuilderTableContext = @ptrCast(@alignCast(ptr));
    return metadataValidateGraphSearchesAgainstContext(alloc, context, graph_searches);
}

fn metadataValidateBleveQuery(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    query: std.json.Value,
) !?[]const u8 {
    _ = request;
    const context: *const QueryBuilderTableContext = @ptrCast(@alignCast(ptr));
    if (!metadataContextHasFullTextIndex(context)) {
        return try alloc.dupe(u8, "table metadata has no full-text index for generated Bleve query");
    }
    return try metadataValidateBleveFieldsAgainstContext(alloc, context, query, "generated Bleve query");
}

fn metadataValidateBleveFieldsAgainstContext(
    alloc: std.mem.Allocator,
    context: *const QueryBuilderTableContext,
    value: std.json.Value,
    label: []const u8,
) !?[]const u8 {
    switch (value) {
        .object => |object| {
            if (object.get("field")) |field_value| {
                const field = queryBuilderStringValue(field_value) orelse return try std.fmt.allocPrint(alloc, "{s} has a non-string field", .{label});
                if (!metadataFullTextIndexesAllowField(context, field)) {
                    return try std.fmt.allocPrint(alloc, "{s} references field '{s}' that is not covered by table full-text indexes", .{ label, field });
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "field")) continue;
                if (isQueryBuilderTextOperatorKey(entry.key_ptr.*)) {
                    if (try metadataValidateBleveTextOperatorFields(alloc, context, entry.value_ptr.*, label)) |feedback| return feedback;
                }
                if (try metadataValidateBleveFieldsAgainstContext(alloc, context, entry.value_ptr.*, label)) |feedback| return feedback;
            }
            return null;
        },
        .array => |array| {
            for (array.items) |item| {
                if (try metadataValidateBleveFieldsAgainstContext(alloc, context, item, label)) |feedback| return feedback;
            }
            return null;
        },
        else => return null,
    }
}

fn metadataValidateBleveTextOperatorFields(
    alloc: std.mem.Allocator,
    context: *const QueryBuilderTableContext,
    value: std.json.Value,
    label: []const u8,
) !?[]const u8 {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get("field") != null) return null;
    if (object.count() != 1) return null;
    var it = object.iterator();
    const entry = it.next() orelse return null;
    const field = entry.key_ptr.*;
    if (isQueryBuilderScalarOptionKey(field) or
        isQueryBuilderTextPayloadKey(field) or
        isQueryBuilderRangeKey(field) or
        std.mem.eql(u8, field, "query"))
    {
        return null;
    }
    if (!metadataFullTextIndexesAllowField(context, field)) {
        return try std.fmt.allocPrint(alloc, "{s} references field '{s}' that is not covered by table full-text indexes", .{ label, field });
    }
    return null;
}

fn metadataValidateQueryRequest(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    query_request: metadata_openapi.QueryRequest,
    retrieval_query_request: ?metadata_openapi.RetrievalQueryRequest,
    specialist: []const u8,
) !?[]const u8 {
    const context: *const QueryBuilderTableContext = @ptrCast(@alignCast(ptr));
    var preflight = try preflightQueryRequestAgainstContext(alloc, context, request, query_request, retrieval_query_request, specialist, .{});
    defer preflight.deinit(alloc);
    for (preflight.diagnostics) |diagnostic| {
        if (diagnostic.severity == .@"error") return try alloc.dupe(u8, diagnostic.message);
    }
    return null;
}

fn metadataValidateQueryRequestAgainstContext(
    alloc: std.mem.Allocator,
    context: *const QueryBuilderTableContext,
    query_request: metadata_openapi.QueryRequest,
    retrieval_query_request: ?metadata_openapi.RetrievalQueryRequest,
) !?[]const u8 {
    var preflight = try preflightQueryRequestAgainstContext(alloc, context, .{ .intent = "" }, query_request, retrieval_query_request, "query_builder", .{});
    defer preflight.deinit(alloc);
    for (preflight.diagnostics) |diagnostic| {
        if (diagnostic.severity == .@"error") return try alloc.dupe(u8, diagnostic.message);
    }
    return null;
}

pub fn preflightQueryRequest(
    alloc: std.mem.Allocator,
    collected_context: *const QueryBuilderCollectedContext,
    request: metadata_openapi.QueryBuilderRequest,
    query_request: metadata_openapi.QueryRequest,
    retrieval_query_request: ?metadata_openapi.RetrievalQueryRequest,
    specialist: []const u8,
    opts: QueryPreflightOptions,
) !QueryPreflightResult {
    if (!collected_context.metadata_loaded) {
        var contract_preflight = query_contract.preflightQueryRequestAlloc(alloc, query_request) catch |err| {
            const message = try std.fmt.allocPrint(alloc, "query_request failed contract preflight: {s}", .{@errorName(err)});
            errdefer alloc.free(message);
            const diagnostics = try alloc.alloc(QueryPreflightDiagnostic, 1);
            diagnostics[0] = .{
                .severity = .@"error",
                .code = try alloc.dupe(u8, "query_contract_preflight_failed"),
                .path = try alloc.dupe(u8, "query_request"),
                .message = message,
            };
            return .{ .diagnostics = diagnostics };
        };
        defer contract_preflight.deinit(alloc);
        return .{
            .plan_summary = if (opts.mode == .validate) null else try buildQueryPreflightPlanSummary(alloc, null, contract_preflight),
        };
    }
    return try preflightQueryRequestAgainstContext(
        alloc,
        &collected_context.table_context,
        request,
        query_request,
        retrieval_query_request,
        specialist,
        opts,
    );
}

fn preflightQueryRequestAgainstContext(
    alloc: std.mem.Allocator,
    context: *const QueryBuilderTableContext,
    request: metadata_openapi.QueryBuilderRequest,
    query_request: metadata_openapi.QueryRequest,
    retrieval_query_request: ?metadata_openapi.RetrievalQueryRequest,
    specialist: []const u8,
    opts: QueryPreflightOptions,
) !QueryPreflightResult {
    _ = request;
    _ = specialist;
    _ = opts.require_executable;

    var diagnostics = std.ArrayListUnmanaged(QueryPreflightDiagnostic).empty;
    errdefer {
        for (diagnostics.items) |*diagnostic| diagnostic.deinit(alloc);
        diagnostics.deinit(alloc);
    }
    var runtime_preflight: ?db_mod.RuntimePreflightSummary = null;
    defer if (runtime_preflight) |*summary| summary.deinit(alloc);
    var contract_preflight = query_contract.preflightQueryRequestAlloc(alloc, query_request) catch |err| blk: {
        const feedback = if (query_request.graph_searches != null)
            try std.fmt.allocPrint(alloc, "query_request.graph_searches failed executor preflight: {s}", .{@errorName(err)})
        else
            try std.fmt.allocPrint(alloc, "query_request failed contract preflight: {s}", .{@errorName(err)});
        defer alloc.free(feedback);
        try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "query_contract_preflight_failed", "query_request", feedback);
        break :blk null;
    };
    defer if (contract_preflight) |*summary| summary.deinit(alloc);

    if (query_request.full_text_search != null and !metadataContextHasFullTextIndex(context)) {
        try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "missing_full_text_index", "query_request.full_text_search", "query_request.full_text_search requires a table full-text index");
    }
    if (query_request.order_by != null and !metadataContextHasFullTextIndex(context)) {
        try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "missing_full_text_index", "query_request.order_by", "query_request.order_by requires a table full-text index");
    }
    if (query_request.offset != null and !metadataContextHasFullTextIndex(context)) {
        try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "missing_full_text_index", "query_request.offset", "query_request.offset requires a table full-text index");
    }
    if ((query_request.search_after != null or query_request.search_before != null) and !metadataContextHasFullTextIndex(context)) {
        try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "missing_full_text_index", "query_request.search_after", "query_request cursor pagination requires a table full-text index");
    }
    if (query_request.full_text_search) |full_text_search| {
        if (try metadataValidateBleveFieldsAgainstContext(alloc, context, full_text_search, "query_request.full_text_search")) |feedback| {
            defer alloc.free(feedback);
            try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_full_text_field", "query_request.full_text_search", feedback);
        }
    }
    if (query_request.filter_query) |filter_query| {
        if (try metadataValidateBleveFieldsAgainstContext(alloc, context, filter_query, "query_request.filter_query")) |feedback| {
            defer alloc.free(feedback);
            try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_filter_field", "query_request.filter_query", feedback);
        }
    }
    if (query_request.exclusion_query) |exclusion_query| {
        if (try metadataValidateBleveFieldsAgainstContext(alloc, context, exclusion_query, "query_request.exclusion_query")) |feedback| {
            defer alloc.free(feedback);
            try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_exclusion_field", "query_request.exclusion_query", feedback);
        }
    }
    if (query_request.semantic_search != null) {
        const indexes = query_request.indexes orelse blk: {
            try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "missing_embedding_indexes", "query_request.indexes", "query_request.semantic_search requires semantic indexes");
            break :blk null;
        };
        if (indexes) |semantic_indexes| {
            if (semantic_indexes.len == 0) {
                try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "missing_embedding_indexes", "query_request.indexes", "query_request.semantic_search requires semantic indexes");
            } else {
                for (semantic_indexes) |index| {
                    if (try metadataDenseEmbeddingIndexFeedback(alloc, context, index)) |feedback| {
                        defer alloc.free(feedback);
                        try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_embedding_index", "query_request.indexes", feedback);
                    }
                }
            }
        }
    }
    if (query_request.fields) |fields| {
        for (fields) |field| {
            if (!metadataContextAllowsField(context, field)) {
                const feedback = try std.fmt.allocPrint(alloc, "query_request.fields references unknown field '{s}'", .{field});
                defer alloc.free(feedback);
                try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "unknown_field", "query_request.fields", feedback);
            }
        }
    }
    if (query_request.order_by) |sort_fields| {
        for (sort_fields) |sort_field| {
            if (!metadataContextAllowsField(context, sort_field.field)) {
                const feedback = try std.fmt.allocPrint(alloc, "query_request.order_by references unknown field '{s}'", .{sort_field.field});
                defer alloc.free(feedback);
                try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "unknown_sort_field", "query_request.order_by", feedback);
            }
            if (!metadataFullTextIndexesAllowField(context, sort_field.field)) {
                const feedback = try std.fmt.allocPrint(alloc, "query_request.order_by references field '{s}' that is not covered by table full-text indexes", .{sort_field.field});
                defer alloc.free(feedback);
                try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_sort_field", "query_request.order_by", feedback);
            }
        }
    }
    if (query_request.graph_searches) |graph_searches| {
        if (try metadataValidateGraphSearchesAgainstContext(alloc, context, graph_searches)) |feedback| {
            defer alloc.free(feedback);
            try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_graph_search", "query_request.graph_searches", feedback);
        }
        if (try metadataValidateGraphSearchResultRefs(alloc, query_request, graph_searches)) |feedback| {
            defer alloc.free(feedback);
            try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_graph_result_ref", "query_request.graph_searches", feedback);
        }
    }
    if (retrieval_query_request) |retrieval_query| {
        if (retrieval_query.tree_search) |tree_search| {
            if (!metadataContextHasGraphIndex(context, tree_search.index)) {
                const feedback = try std.fmt.allocPrint(alloc, "retrieval_query_request.tree_search references unknown graph index '{s}'", .{tree_search.index});
                defer alloc.free(feedback);
                try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "unknown_tree_index", "retrieval_query_request.tree_search.index", feedback);
            }
            if (!metadataGraphIndexSupportsTreeSearch(context, tree_search.index)) {
                const feedback = try std.fmt.allocPrint(alloc, "retrieval_query_request.tree_search references graph index '{s}' without tree topology metadata", .{tree_search.index});
                defer alloc.free(feedback);
                try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "invalid_tree_index", "retrieval_query_request.tree_search.index", feedback);
            }
        }
    }
    if (context.runtime_query_request_validator) |validator| {
        if (opts.mode == .estimate) {
            runtime_preflight = try validator.preflightQueryRequest(alloc, query_request, opts.max_work);
            if (runtime_preflight) |*summary| db_mod.deriveRuntimePreflightEstimates(summary);
        }
        if (try validator.validateQueryRequest(alloc, query_request)) |feedback| {
            defer alloc.free(feedback);
            try appendQueryPreflightDiagnostic(alloc, &diagnostics, .@"error", "runtime_query_request_invalid", "query_request", feedback);
        }
    }

    return .{
        .diagnostics = if (diagnostics.items.len == 0) &.{} else try diagnostics.toOwnedSlice(alloc),
        .plan_summary = if (opts.mode == .validate)
            null
        else if (contract_preflight) |summary|
            try buildQueryPreflightPlanSummary(alloc, context, summary)
        else
            null,
        .estimate_summary = if (opts.mode == .estimate)
            if (runtime_preflight) |summary|
                try buildQueryPreflightEstimateSummary(alloc, summary)
            else
                null
        else
            null,
    };
}

fn appendQueryPreflightDiagnostic(
    alloc: std.mem.Allocator,
    diagnostics: *std.ArrayListUnmanaged(QueryPreflightDiagnostic),
    severity: QueryPreflightDiagnosticSeverity,
    code: []const u8,
    path: []const u8,
    message: []const u8,
) !void {
    try diagnostics.append(alloc, .{
        .severity = severity,
        .code = try alloc.dupe(u8, code),
        .path = try alloc.dupe(u8, path),
        .message = try alloc.dupe(u8, message),
    });
}

fn buildQueryPreflightPlanSummary(
    alloc: std.mem.Allocator,
    context: ?*const QueryBuilderTableContext,
    summary: query_contract.QueryPreflightSummary,
) !QueryPreflightPlanSummary {
    var full_text_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitOwnedStringItems(alloc, full_text_indexes.items);
    errdefer full_text_indexes.deinit(alloc);
    if (summary.full_text_indexes.len > 0) {
        if (context) |table_context| {
            for (table_context.full_text_index_metadata) |metadata| try appendUniqueOwnedString(alloc, &full_text_indexes, metadata.name);
        }
        if (full_text_indexes.items.len == 0) {
            for (summary.full_text_indexes) |index_name| try appendUniqueOwnedString(alloc, &full_text_indexes, index_name);
        }
    }

    var embedding_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitOwnedStringItems(alloc, embedding_indexes.items);
    errdefer embedding_indexes.deinit(alloc);
    for (summary.embedding_indexes) |index_name| try appendUniqueOwnedString(alloc, &embedding_indexes, index_name);

    var graph_indexes = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitOwnedStringItems(alloc, graph_indexes.items);
    errdefer graph_indexes.deinit(alloc);
    for (summary.graph_indexes) |index_name| try appendUniqueOwnedString(alloc, &graph_indexes, index_name);

    var result_refs = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitOwnedStringItems(alloc, result_refs.items);
    errdefer result_refs.deinit(alloc);
    for (summary.result_refs) |result_ref| try appendUniqueOwnedString(alloc, &result_refs, result_ref);

    var graph_query_order = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitOwnedStringItems(alloc, graph_query_order.items);
    errdefer graph_query_order.deinit(alloc);
    for (summary.graph_query_order) |name| try appendUniqueOwnedString(alloc, &graph_query_order, name);

    return .{
        .full_text_indexes = if (full_text_indexes.items.len == 0) &.{} else try full_text_indexes.toOwnedSlice(alloc),
        .embedding_indexes = if (embedding_indexes.items.len == 0) &.{} else try embedding_indexes.toOwnedSlice(alloc),
        .graph_indexes = if (graph_indexes.items.len == 0) &.{} else try graph_indexes.toOwnedSlice(alloc),
        .result_refs = if (result_refs.items.len == 0) &.{} else try result_refs.toOwnedSlice(alloc),
        .graph_query_order = if (graph_query_order.items.len == 0) &.{} else try graph_query_order.toOwnedSlice(alloc),
        .requested_limit = summary.requested_limit,
        .requested_offset = summary.requested_offset,
        .base_result_set_count = summary.base_result_set_count,
        .graph_query_count = summary.graph_query_count,
        .requires_fusion = summary.requires_fusion,
        .count_only = summary.count_only,
        .profile_requested = summary.profile_requested,
        .include_stored = summary.include_stored,
        .reranker_enabled = summary.reranker_enabled,
        .aggregation_count = summary.aggregation_count,
    };
}

fn buildQueryPreflightEstimateSummary(
    alloc: std.mem.Allocator,
    summary: db_mod.RuntimePreflightSummary,
) !QueryPreflightEstimateSummary {
    var text_indexes = std.ArrayListUnmanaged(QueryPreflightTextEstimate).empty;
    errdefer {
        for (text_indexes.items) |*item| item.deinit(alloc);
        text_indexes.deinit(alloc);
    }
    for (summary.text_indexes) |item| {
        try text_indexes.append(alloc, .{
            .name = try alloc.dupe(u8, item.name),
            .doc_count = item.doc_count,
            .chunk_backed = item.chunk_backed,
            .group_chunk_parents = item.group_chunk_parents,
        });
    }

    var embedding_indexes = std.ArrayListUnmanaged(QueryPreflightEmbeddingEstimate).empty;
    errdefer {
        for (embedding_indexes.items) |*item| item.deinit(alloc);
        embedding_indexes.deinit(alloc);
    }
    for (summary.embedding_indexes) |item| {
        try embedding_indexes.append(alloc, .{
            .name = try alloc.dupe(u8, item.name),
            .sparse = item.sparse,
            .doc_count = item.doc_count,
            .dims = item.dims,
            .chunk_backed = item.chunk_backed,
        });
    }

    var graph_indexes = std.ArrayListUnmanaged(QueryPreflightGraphEstimate).empty;
    errdefer {
        for (graph_indexes.items) |*item| item.deinit(alloc);
        graph_indexes.deinit(alloc);
    }
    for (summary.graph_indexes) |item| {
        try graph_indexes.append(alloc, .{
            .name = try alloc.dupe(u8, item.name),
            .edge_count = item.edge_count,
            .node_count = item.node_count,
        });
    }

    var text_query_stats = std.ArrayListUnmanaged(QueryPreflightTextQueryEstimate).empty;
    errdefer {
        for (text_query_stats.items) |*item| item.deinit(alloc);
        text_query_stats.deinit(alloc);
    }
    for (summary.text_query_stats) |item| {
        var term_doc_freqs = std.ArrayListUnmanaged(QueryPreflightTermEstimate).empty;
        errdefer {
            for (term_doc_freqs.items) |*term| term.deinit(alloc);
            term_doc_freqs.deinit(alloc);
        }
        for (item.term_doc_freqs) |term| {
            try term_doc_freqs.append(alloc, .{
                .term = try alloc.dupe(u8, term.term),
                .doc_freq = term.doc_freq,
            });
        }
        try text_query_stats.append(alloc, .{
            .field = try alloc.dupe(u8, item.field),
            .global_doc_count = item.global_doc_count,
            .avg_doc_len = item.avgDocLen(),
            .term_doc_freqs = if (term_doc_freqs.items.len == 0) &.{} else try term_doc_freqs.toOwnedSlice(alloc),
        });
    }

    const text_result_upper_bound = summary.text_result_upper_bound;
    const text_term_doc_freq_total = summary.text_term_doc_freq_total;
    const result_doc_upper_bound = summary.result_doc_upper_bound;
    const result_doc_estimate = summary.result_doc_estimate;
    const corpus_doc_count_estimate = summary.corpus_doc_count_estimate;
    const selectivity_lower_bound_ratio = summary.selectivity_lower_bound_ratio;
    const selectivity_sample_ratio = summary.selectivity_sample_ratio;
    const selectivity_ratio = summary.selectivity_upper_bound_ratio;
    const effective_stored_projection_doc_estimate_total = summary.effective_stored_projection_doc_estimate_total;
    const effective_rerank_doc_estimate = summary.effective_rerank_doc_estimate;
    const effective_stored_projection_doc_upper_bound_total = summary.effective_stored_projection_doc_upper_bound_total;
    const effective_rerank_doc_upper_bound = summary.effective_rerank_doc_upper_bound;
    var selectivity_risk_factors = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitOwnedStringItems(alloc, selectivity_risk_factors.items);
    errdefer selectivity_risk_factors.deinit(alloc);
    try appendSelectivityRiskFactors(
        alloc,
        &selectivity_risk_factors,
        summary,
        result_doc_upper_bound,
        text_result_upper_bound,
        selectivity_lower_bound_ratio,
        selectivity_sample_ratio,
        selectivity_ratio,
    );

    var latency_score_components = std.ArrayListUnmanaged(QueryPreflightEstimateSummary.ScoreComponent).empty;
    errdefer {
        for (latency_score_components.items) |*item| item.deinit(alloc);
        latency_score_components.deinit(alloc);
    }
    try appendLatencyScoreComponents(
        alloc,
        &latency_score_components,
        summary,
        text_term_doc_freq_total,
        effective_stored_projection_doc_estimate_total,
        effective_stored_projection_doc_upper_bound_total,
        effective_rerank_doc_estimate,
        effective_rerank_doc_upper_bound,
    );
    const latency_score = latencyScoreFromComponents(latency_score_components.items);

    var latency_risk_factors = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitOwnedStringItems(alloc, latency_risk_factors.items);
    errdefer latency_risk_factors.deinit(alloc);
    try appendLatencyRiskFactors(
        alloc,
        &latency_risk_factors,
        summary,
        text_term_doc_freq_total,
        effective_stored_projection_doc_estimate_total,
        effective_stored_projection_doc_upper_bound_total,
        effective_rerank_doc_estimate,
        effective_rerank_doc_upper_bound,
    );

    return .{
        .text_indexes = if (text_indexes.items.len == 0) &.{} else try text_indexes.toOwnedSlice(alloc),
        .embedding_indexes = if (embedding_indexes.items.len == 0) &.{} else try embedding_indexes.toOwnedSlice(alloc),
        .graph_indexes = if (graph_indexes.items.len == 0) &.{} else try graph_indexes.toOwnedSlice(alloc),
        .text_query_stats = if (text_query_stats.items.len == 0) &.{} else try text_query_stats.toOwnedSlice(alloc),
        .doc_id_value_count = summary.doc_id_value_count,
        .filter_id_count = summary.filter_id_count,
        .exclude_id_count = summary.exclude_id_count,
        .numeric_range_clause_count = summary.numeric_range_clause_count,
        .term_range_clause_count = summary.term_range_clause_count,
        .ip_range_clause_count = summary.ip_range_clause_count,
        .bool_field_clause_count = summary.bool_field_clause_count,
        .geo_filter_clause_count = summary.geo_filter_clause_count,
        .positive_id_result_upper_bound = summary.positive_id_result_upper_bound,
        .structured_filter_doc_count_estimate = summary.structured_filter_doc_count_estimate,
        .structured_filter_doc_count_lower_bound = summary.structured_filter_doc_count_lower_bound,
        .structured_filter_doc_count_sample_estimate = summary.structured_filter_doc_count_sample_estimate,
        .structured_filter_count_exact = summary.structured_filter_count_exact,
        .structured_filter_count_sample_size = summary.structured_filter_count_sample_size,
        .structured_filter_count_budget_limit = summary.structured_filter_count_budget_limit,
        .text_result_upper_bound = text_result_upper_bound,
        .text_term_doc_freq_total = text_term_doc_freq_total,
        .corpus_doc_count_estimate = corpus_doc_count_estimate,
        .selectivity_estimate_kind = selectivityEstimateKind(selectivity_lower_bound_ratio, selectivity_sample_ratio, selectivity_ratio),
        .selectivity_confidence = selectivityConfidence(summary, result_doc_upper_bound, text_result_upper_bound, selectivity_lower_bound_ratio, selectivity_sample_ratio, selectivity_ratio),
        .result_doc_estimate = result_doc_estimate,
        .result_doc_upper_bound = result_doc_upper_bound,
        .selectivity_lower_bound_ratio = selectivity_lower_bound_ratio,
        .selectivity_sample_ratio = selectivity_sample_ratio,
        .selectivity_upper_bound_ratio = selectivity_ratio,
        .selectivity_heuristic = selectivityHeuristic(summary, selectivity_lower_bound_ratio, selectivity_ratio),
        .selectivity_risk_factors = if (selectivity_risk_factors.items.len == 0) &.{} else try selectivity_risk_factors.toOwnedSlice(alloc),
        .shard_result_window = summary.shard_result_window,
        .shard_result_window_total = summary.shard_result_window_total,
        .stored_projection_doc_upper_bound_total = summary.stored_projection_doc_upper_bound_total,
        .effective_stored_projection_doc_estimate_total = effective_stored_projection_doc_estimate_total,
        .effective_stored_projection_doc_upper_bound_total = effective_stored_projection_doc_upper_bound_total,
        .rerank_doc_upper_bound = summary.rerank_doc_upper_bound,
        .effective_rerank_doc_estimate = effective_rerank_doc_estimate,
        .effective_rerank_doc_upper_bound = effective_rerank_doc_upper_bound,
        .aggregation_may_scan_full_results = summary.aggregation_may_scan_full_results,
        .aggregation_second_pass_doc_estimate = summary.aggregation_second_pass_doc_estimate,
        .aggregation_second_pass_doc_upper_bound = summary.aggregation_second_pass_doc_upper_bound,
        .latency_risk_factors = if (latency_risk_factors.items.len == 0) &.{} else try latency_risk_factors.toOwnedSlice(alloc),
        .latency_score_components = if (latency_score_components.items.len == 0) &.{} else try latency_score_components.toOwnedSlice(alloc),
        .latency_estimate_kind = .heuristic,
        .latency_confidence = latencyConfidence(summary),
        .latency_heuristic_score = latency_score,
        .latency_heuristic = latencyHeuristicLevel(latency_score),
        .shard_count = summary.shard_count,
        .remote_shard_count = summary.remote_shard_count,
        .dense_query_count = summary.dense_query_count,
        .vector_worker_candidate_count = summary.vector_worker_candidate_count,
        .vector_worker_fallback_count = summary.vector_worker_fallback_count,
        .vector_worker_filter_constraint_count = summary.vector_worker_filter_constraint_count,
        .vector_worker_requires_algebraic_filter_resolution = summary.vector_worker_requires_algebraic_filter_resolution,
        .dense_effective_k_total = summary.dense_effective_k_total,
        .dense_search_width_total = summary.dense_search_width_total,
        .dense_search_width_max = summary.dense_search_width_max,
        .dense_epsilon_max = summary.dense_epsilon_max,
    };
}

fn estimatedCorpusDocCount(summary: db_mod.RuntimePreflightSummary) ?u64 {
    return summary.corpus_doc_count_estimate;
}

fn selectivityHeuristic(
    summary: db_mod.RuntimePreflightSummary,
    lower_bound_ratio: ?f32,
    ratio: ?f32,
) QueryPreflightEstimateSummary.SelectivityHeuristic {
    if (ratio) |value| {
        if (value <= 0.01) return .narrow;
        if (value <= 0.2) return .medium;
        return .broad;
    }
    if (summary.structured_filter_doc_count_sample_estimate) |sample_estimate| {
        if (estimatedCorpusDocCount(summary)) |corpus_docs| {
            if (corpus_docs > 0) {
                const sample_ratio = @as(f32, @floatFromInt(sample_estimate)) / @as(f32, @floatFromInt(corpus_docs));
                if (sample_ratio <= 0.01) return .narrow;
                if (sample_ratio <= 0.2) return .medium;
                return .broad;
            }
        }
    }
    const lower_value = lower_bound_ratio orelse return .unknown;
    return if (lower_value >= 0.2) .broad else .unknown;
}

fn selectivityEstimateKind(
    lower_bound_ratio: ?f32,
    sample_ratio: ?f32,
    ratio: ?f32,
) QueryPreflightEstimateSummary.EstimateKind {
    if (ratio != null) return .upper_bound;
    if (sample_ratio != null) return .sampled;
    if (lower_bound_ratio != null) return .lower_bound;
    return .unknown;
}

fn selectivityConfidence(
    summary: db_mod.RuntimePreflightSummary,
    result_doc_upper_bound: ?u32,
    text_result_upper_bound: ?u32,
    lower_bound_ratio: ?f32,
    sample_ratio: ?f32,
    ratio: ?f32,
) QueryPreflightEstimateSummary.Confidence {
    if (ratio != null and
        result_doc_upper_bound != null and
        estimatedCorpusDocCount(summary) != null and
        (summary.positive_id_result_upper_bound != null or
            summary.structured_filter_count_exact or
            summary.structured_filter_doc_count_estimate != null and text_result_upper_bound == null))
    {
        return .high;
    }
    if (ratio != null and result_doc_upper_bound != null and estimatedCorpusDocCount(summary) != null) return .medium;
    if (sample_ratio != null and estimatedCorpusDocCount(summary) != null) {
        return if (summary.structured_filter_count_sample_size >= 128) .medium else .low;
    }
    if (lower_bound_ratio != null and estimatedCorpusDocCount(summary) != null) return .medium;
    if (result_doc_upper_bound != null or summary.text_query_stats.len > 0) return .medium;
    return .low;
}

fn appendSelectivityRiskFactors(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    summary: db_mod.RuntimePreflightSummary,
    result_doc_upper_bound: ?u32,
    text_result_upper_bound: ?u32,
    lower_bound_ratio: ?f32,
    sample_ratio: ?f32,
    ratio: ?f32,
) !void {
    if (result_doc_upper_bound != null) {
        try appendUniqueOwnedString(alloc, items, "positive_id_bound");
    } else {
        try appendUniqueOwnedString(alloc, items, "no_positive_id_bound");
    }
    if (summary.structured_filter_count_exact) {
        try appendUniqueOwnedString(alloc, items, "exact_structured_filter_count");
    }
    if (!summary.structured_filter_count_exact and summary.structured_filter_doc_count_lower_bound != null) {
        try appendUniqueOwnedString(alloc, items, "structured_filter_probe_lower_bound");
    }
    if (!summary.structured_filter_count_exact and summary.structured_filter_count_budget_limit != null) {
        try appendUniqueOwnedString(alloc, items, "structured_filter_count_budget_limited");
    }
    if (summary.structured_filter_doc_count_sample_estimate != null) {
        try appendUniqueOwnedString(alloc, items, "structured_filter_sample_estimate");
    }
    if (estimatedCorpusDocCount(summary) != null) {
        try appendUniqueOwnedString(alloc, items, "corpus_size_available");
    } else {
        try appendUniqueOwnedString(alloc, items, "no_corpus_size");
    }
    if (summary.text_query_stats.len > 0) try appendUniqueOwnedString(alloc, items, "text_term_stats");
    if (text_result_upper_bound != null and (summary.positive_id_result_upper_bound == null or result_doc_upper_bound == text_result_upper_bound)) {
        try appendUniqueOwnedString(alloc, items, "text_term_bound");
    }
    if (summary.numeric_range_clause_count > 0 or
        summary.term_range_clause_count > 0 or
        summary.ip_range_clause_count > 0 or
        summary.bool_field_clause_count > 0 or
        summary.geo_filter_clause_count > 0)
    {
        try appendUniqueOwnedString(alloc, items, "non_text_filters_present");
    }
    if (ratio == null and sample_ratio == null and lower_bound_ratio == null) try appendUniqueOwnedString(alloc, items, "selectivity_ratio_unknown");
}

fn appendLatencyScoreComponents(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(QueryPreflightEstimateSummary.ScoreComponent),
    summary: db_mod.RuntimePreflightSummary,
    text_term_doc_freq_total: u64,
    effective_stored_projection_doc_estimate_total: ?u64,
    effective_stored_projection_doc_upper_bound_total: u64,
    effective_rerank_doc_estimate: ?u32,
    effective_rerank_doc_upper_bound: u32,
) !void {
    if (summary.remote_shard_count > 0) try appendLatencyScoreComponent(alloc, items, "remote_shards", 3);
    if (summary.aggregation_may_scan_full_results) try appendLatencyScoreComponent(alloc, items, "aggregation_second_pass", 3);
    if (summary.vector_worker_fallback_count > 0) try appendLatencyScoreComponent(alloc, items, "vector_worker_fallback", 2);
    const rerank_work = effective_rerank_doc_estimate orelse effective_rerank_doc_upper_bound;
    if (rerank_work >= 128)
        try appendLatencyScoreComponent(alloc, items, "reranking", 2)
    else if (rerank_work > 0)
        try appendLatencyScoreComponent(alloc, items, "reranking", 1);
    if (summary.dense_query_count > 0) try appendLatencyScoreComponent(alloc, items, "dense_search", 1);
    if (summary.dense_search_width_total >= 4096)
        try appendLatencyScoreComponent(alloc, items, "dense_search_width", 2)
    else if (summary.dense_search_width_total > 0)
        try appendLatencyScoreComponent(alloc, items, "dense_search_width", 1);
    if (text_term_doc_freq_total >= 4096)
        try appendLatencyScoreComponent(alloc, items, "text_postings", 2)
    else if (text_term_doc_freq_total >= 256)
        try appendLatencyScoreComponent(alloc, items, "text_postings", 1);
    if (summary.structured_filter_doc_count_lower_bound) |lower_bound| {
        if (lower_bound >= 256)
            try appendLatencyScoreComponent(alloc, items, "structured_filter_probe", 2)
        else if (lower_bound > 0)
            try appendLatencyScoreComponent(alloc, items, "structured_filter_probe", 1);
    }
    if (summary.structured_filter_doc_count_sample_estimate) |sample_estimate| {
        if (sample_estimate >= 1024)
            try appendLatencyScoreComponent(alloc, items, "structured_filter_sample", 2)
        else if (sample_estimate >= 64)
            try appendLatencyScoreComponent(alloc, items, "structured_filter_sample", 1);
    }
    const stored_projection_work = effective_stored_projection_doc_estimate_total orelse effective_stored_projection_doc_upper_bound_total;
    if (stored_projection_work > 256)
        try appendLatencyScoreComponent(alloc, items, "stored_projection", 2)
    else if (stored_projection_work > 32)
        try appendLatencyScoreComponent(alloc, items, "stored_projection", 1);
    if (summary.shard_result_window_total > 256)
        try appendLatencyScoreComponent(alloc, items, "wide_shard_window", 2)
    else if (summary.shard_result_window_total > 32)
        try appendLatencyScoreComponent(alloc, items, "wide_shard_window", 1);
}

fn appendLatencyScoreComponent(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(QueryPreflightEstimateSummary.ScoreComponent),
    factor: []const u8,
    points: u32,
) !void {
    if (points == 0) return;
    try items.append(alloc, .{
        .factor = try alloc.dupe(u8, factor),
        .points = points,
    });
}

fn latencyScoreFromComponents(items: []const QueryPreflightEstimateSummary.ScoreComponent) u32 {
    var score: u32 = 0;
    for (items) |item| score +|= item.points;
    return score;
}

fn latencyHeuristicLevel(score: u32) QueryPreflightEstimateSummary.LatencyHeuristic {
    if (score >= 7) return .high;
    if (score >= 3) return .medium;
    return .low;
}

fn latencyConfidence(summary: db_mod.RuntimePreflightSummary) QueryPreflightEstimateSummary.Confidence {
    if (summary.shard_count > 0 and
        (summary.remote_shard_count > 0 or
            summary.dense_query_count > 0 or
            summary.aggregation_may_scan_full_results or
            summary.rerank_doc_upper_bound > 0 or
            summary.stored_projection_doc_upper_bound_total > 0))
    {
        return .high;
    }
    if (summary.shard_count > 0 or summary.text_indexes.len > 0 or summary.embedding_indexes.len > 0) return .medium;
    return .low;
}

fn appendLatencyRiskFactors(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    summary: db_mod.RuntimePreflightSummary,
    text_term_doc_freq_total: u64,
    effective_stored_projection_doc_estimate_total: ?u64,
    effective_stored_projection_doc_upper_bound_total: u64,
    effective_rerank_doc_estimate: ?u32,
    effective_rerank_doc_upper_bound: u32,
) !void {
    if (summary.remote_shard_count > 0) try appendUniqueOwnedString(alloc, items, "remote_shards");
    if (summary.aggregation_may_scan_full_results) try appendUniqueOwnedString(alloc, items, "aggregation_second_pass");
    if (summary.vector_worker_fallback_count > 0) try appendUniqueOwnedString(alloc, items, "vector_worker_fallback");
    if ((effective_rerank_doc_estimate orelse effective_rerank_doc_upper_bound) > 0) try appendUniqueOwnedString(alloc, items, "reranking");
    if (summary.dense_query_count > 0) try appendUniqueOwnedString(alloc, items, "dense_search");
    if (text_term_doc_freq_total >= 256) try appendUniqueOwnedString(alloc, items, "text_postings");
    if (summary.structured_filter_doc_count_lower_bound != null) try appendUniqueOwnedString(alloc, items, "structured_filter_probe");
    if (summary.structured_filter_doc_count_sample_estimate != null) try appendUniqueOwnedString(alloc, items, "structured_filter_sample");
    if ((effective_stored_projection_doc_estimate_total orelse effective_stored_projection_doc_upper_bound_total) > 32) try appendUniqueOwnedString(alloc, items, "stored_projection");
    if (summary.shard_result_window_total > 32) try appendUniqueOwnedString(alloc, items, "wide_shard_window");
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

fn deinitOwnedStringItems(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
}

fn deinitOwnedStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    deinitOwnedStringItems(alloc, values);
    if (values.len > 0) alloc.free(@constCast(values));
}

fn metadataValidateGraphSearchesAgainstContext(
    alloc: std.mem.Allocator,
    context: *const QueryBuilderTableContext,
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
) !?[]const u8 {
    var it = graph_searches.map.iterator();
    while (it.next()) |entry| {
        const query = entry.value_ptr.*;
        if (!metadataContextHasGraphIndex(context, query.index_name)) {
            return try std.fmt.allocPrint(alloc, "graph_searches.{s} references unknown graph index '{s}'", .{ entry.key_ptr.*, query.index_name });
        }
        if (query.params) |params| {
            if (params.edge_types) |edge_types| {
                if (try metadataValidateGraphEdgeTypesForIndex(alloc, context, entry.key_ptr.*, query.index_name, edge_types)) |feedback| return feedback;
            }
        }
        if (query.pattern) |pattern| {
            for (pattern) |step| {
                if (step.edge) |edge| {
                    if (edge.types) |edge_types| {
                        if (try metadataValidateGraphEdgeTypesForIndex(alloc, context, entry.key_ptr.*, query.index_name, edge_types)) |feedback| return feedback;
                    }
                }
            }
        }
        if (query.fields) |fields| {
            for (fields) |field| {
                if (!metadataContextAllowsField(context, field)) {
                    return try std.fmt.allocPrint(alloc, "graph_searches.{s}.fields references unknown field '{s}'", .{ entry.key_ptr.*, field });
                }
            }
        }
    }
    if (try metadataPreflightGraphSearchesAgainstExecutorParser(alloc, graph_searches)) |feedback| return feedback;
    return null;
}

fn metadataValidateGraphSearchResultRefs(
    alloc: std.mem.Allocator,
    query_request: metadata_openapi.QueryRequest,
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
) !?[]const u8 {
    validateGeneratedGraphResultDependencies(graph_searches) catch {
        return try alloc.dupe(u8, "query_request.graph_searches contains an invalid graph result dependency");
    };
    var it = graph_searches.map.iterator();
    while (it.next()) |entry| {
        if (try metadataValidateGraphSelectorResultRef(alloc, query_request, graph_searches, entry.key_ptr.*, "start_nodes", entry.value_ptr.*.start_nodes)) |feedback| return feedback;
        if (try metadataValidateGraphSelectorResultRef(alloc, query_request, graph_searches, entry.key_ptr.*, "target_nodes", entry.value_ptr.*.target_nodes)) |feedback| return feedback;
    }
    return null;
}

fn metadataValidateGraphSelectorResultRef(
    alloc: std.mem.Allocator,
    query_request: metadata_openapi.QueryRequest,
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
    search_name: []const u8,
    selector_name: []const u8,
    selector: ?indexes_openapi.GraphNodeSelector,
) !?[]const u8 {
    const result_ref = (selector orelse return null).result_ref orelse return null;
    if (generatedGraphResultDependencyName(result_ref)) |dependency_name| {
        if (graph_searches.map.get(dependency_name) == null) {
            return try std.fmt.allocPrint(alloc, "graph_searches.{s}.{s} references missing graph result '{s}'", .{ search_name, selector_name, result_ref });
        }
        return null;
    }
    if (std.mem.eql(u8, result_ref, "$full_text_results")) {
        if (query_request.full_text_search == null) {
            return try std.fmt.allocPrint(alloc, "graph_searches.{s}.{s} references $full_text_results but query_request.full_text_search is absent", .{ search_name, selector_name });
        }
        return null;
    }
    if (std.mem.eql(u8, result_ref, "$fused_results")) {
        if (query_request.full_text_search == null or !metadataQueryRequestHasVectorResults(query_request)) {
            return try std.fmt.allocPrint(alloc, "graph_searches.{s}.{s} references $fused_results but the query_request cannot produce fused full-text and vector results", .{ search_name, selector_name });
        }
        return null;
    }
    if (std.mem.eql(u8, result_ref, "$embeddings_results")) {
        if (!metadataQueryRequestHasVectorResults(query_request)) {
            return try std.fmt.allocPrint(alloc, "graph_searches.{s}.{s} references {s} but query_request has no semantic_search or embeddings", .{ search_name, selector_name, result_ref });
        }
        return null;
    }
    return try std.fmt.allocPrint(alloc, "graph_searches.{s}.{s} references unsupported result ref '{s}'", .{ search_name, selector_name, result_ref });
}

fn metadataQueryRequestHasVectorResults(query_request: metadata_openapi.QueryRequest) bool {
    return query_request.semantic_search != null or query_request.embeddings != null;
}

fn metadataPreflightGraphSearchesAgainstExecutorParser(
    alloc: std.mem.Allocator,
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
) !?[]const u8 {
    const request = metadata_openapi.QueryRequest{
        .graph_searches = graph_searches,
    };
    query_contract.preflightGraphSearchesAlloc(alloc, request) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return try std.fmt.allocPrint(alloc, "query_request.graph_searches failed executor preflight: {s}", .{@errorName(err)}),
    };
    return null;
}

fn metadataContextAllowsField(context: *const QueryBuilderTableContext, field: []const u8) bool {
    return context.schema_fields.len == 0 or queryBuilderFieldInSlice(context.schema_fields, field);
}

fn metadataContextHasFullTextIndex(context: *const QueryBuilderTableContext) bool {
    return context.full_text_index_metadata.len > 0;
}

fn metadataFullTextIndexesAllowField(context: *const QueryBuilderTableContext, field: []const u8) bool {
    if (context.full_text_index_metadata.len == 0) return true;
    for (context.full_text_index_metadata) |metadata| {
        if (metadata.fields.len == 0) return true;
        if (queryBuilderFieldInSlice(metadata.fields, field)) return true;
    }
    return false;
}

fn metadataContextHasDenseEmbeddingIndex(context: *const QueryBuilderTableContext, index: []const u8) bool {
    for (context.embedding_index_metadata) |metadata| {
        if (std.mem.eql(u8, metadata.name, index)) return !metadata.sparse;
    }
    return false;
}

fn metadataDenseEmbeddingIndexFeedback(
    alloc: std.mem.Allocator,
    context: *const QueryBuilderTableContext,
    index: []const u8,
) !?[]const u8 {
    for (context.embedding_index_metadata) |metadata| {
        if (!std.mem.eql(u8, metadata.name, index)) continue;
        if (!metadata.sparse) return null;
        if (metadata.model) |model| {
            return try std.fmt.allocPrint(alloc, "query_request.semantic_search references sparse embedding index '{s}' with model '{s}'; dense semantic search requires a dense embedding index", .{ index, model });
        }
        return try std.fmt.allocPrint(alloc, "query_request.semantic_search references sparse embedding index '{s}'; dense semantic search requires a dense embedding index", .{index});
    }
    return try std.fmt.allocPrint(alloc, "query_request.semantic_search references unknown embedding index '{s}'", .{index});
}

fn metadataContextHasGraphIndex(context: *const QueryBuilderTableContext, index: []const u8) bool {
    for (context.graph_index_metadata) |metadata| {
        if (std.mem.eql(u8, metadata.name, index)) return true;
    }
    return false;
}

fn metadataGraphIndexSupportsTreeSearch(context: *const QueryBuilderTableContext, index_name: []const u8) bool {
    const graph_metadata = metadataGraphIndex(context, index_name) orelse return true;
    if (graph_metadata.edge_types.len == 0) return true;
    for (graph_metadata.edge_types) |edge_type| {
        const topology = edge_type.topology orelse return true;
        if (std.ascii.eqlIgnoreCase(topology, "tree")) return true;
    }
    return false;
}

fn metadataValidateGraphEdgeTypesForIndex(
    alloc: std.mem.Allocator,
    context: *const QueryBuilderTableContext,
    search_name: []const u8,
    index_name: []const u8,
    edge_types: []const []const u8,
) !?[]const u8 {
    const graph_metadata = metadataGraphIndex(context, index_name) orelse return null;
    if (graph_metadata.edge_types.len == 0) return null;
    for (edge_types) |edge_type| {
        if (!metadataGraphIndexHasEdgeType(graph_metadata, edge_type)) {
            return try std.fmt.allocPrint(alloc, "graph_searches.{s} references unknown edge type '{s}' for graph index '{s}'", .{ search_name, edge_type, index_name });
        }
    }
    return null;
}

fn metadataGraphIndex(context: *const QueryBuilderTableContext, index_name: []const u8) ?QueryBuilderGraphIndex {
    for (context.graph_index_metadata) |metadata| {
        if (std.mem.eql(u8, metadata.name, index_name)) return metadata;
    }
    return null;
}

fn metadataGraphIndexHasEdgeType(index: QueryBuilderGraphIndex, edge_type: []const u8) bool {
    for (index.edge_types) |candidate| {
        if (std.mem.eql(u8, candidate.name, edge_type)) return true;
    }
    return false;
}

fn queryBuilderDenseEmbeddingIndexNames(
    alloc: std.mem.Allocator,
    metadata: []const QueryBuilderEmbeddingIndex,
    fallback: []const []const u8,
) ![]const []const u8 {
    if (metadata.len == 0) return fallback;
    var count: usize = 0;
    for (metadata) |index| {
        if (!index.sparse) count += 1;
    }
    if (count == 0) return &.{};

    const names = try alloc.alloc([]const u8, count);
    var out: usize = 0;
    for (metadata) |index| {
        if (index.sparse) continue;
        names[out] = index.name;
        out += 1;
    }
    return names;
}

fn queryBuilderGraphIndexNames(
    alloc: std.mem.Allocator,
    metadata: []const QueryBuilderGraphIndex,
    fallback: []const []const u8,
) ![]const []const u8 {
    if (metadata.len == 0) return fallback;
    const names = try alloc.alloc([]const u8, metadata.len);
    for (metadata, 0..) |index, i| {
        names[i] = index.name;
    }
    return names;
}

pub fn buildQueryBuilderResponseWithGeneration(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    table_schema_fields: ?[]const []const u8,
    generation_runner: ?GenerationRunner,
) !metadata_openapi.QueryBuilderResult {
    return try buildQueryBuilderResponseWithContext(alloc, request, .{
        .schema_fields = table_schema_fields orelse &.{},
    }, generation_runner);
}

pub fn buildQueryBuilderResponseWithCollectedContext(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    collected_context: *const QueryBuilderCollectedContext,
    generation_runner: ?GenerationRunner,
) !metadata_openapi.QueryBuilderResult {
    return try buildQueryBuilderResponseWithContext(
        alloc,
        request,
        collected_context.effectiveTableContext(),
        generation_runner,
    );
}

pub fn buildQueryBuilderResponseWithContext(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    table_context: QueryBuilderTableContext,
    generation_runner: ?GenerationRunner,
) !metadata_openapi.QueryBuilderResult {
    if (request.intent.len == 0) return error.InvalidQueryBuilderRequest;

    const session_id = try ensureQueryBuilderSessionId(alloc, request.session_id);
    const effective_intent = try appendDecisionContext(alloc, request.intent, request.decisions orelse &.{});
    const effective_fields = try resolveQueryBuilderFields(
        alloc,
        request.schema_fields,
        if (table_context.schema_fields.len > 0) table_context.schema_fields else null,
        request.example_documents,
    );

    var warnings = std.ArrayListUnmanaged([]const u8).empty;
    defer warnings.deinit(alloc);

    const semantic_indexes = try queryBuilderDenseEmbeddingIndexNames(alloc, table_context.embedding_index_metadata, table_context.semantic_indexes);
    const graph_indexes = try queryBuilderGraphIndexNames(alloc, table_context.graph_index_metadata, table_context.graph_indexes);

    const built_query = buildQueryBuilderSpecialist(
        alloc,
        request,
        effective_intent,
        effective_fields,
        table_context.full_text_index_metadata,
        table_context.embedding_index_metadata,
        semantic_indexes,
        graph_indexes,
        table_context.graph_index_metadata,
        table_context.plan_validator,
        generation_runner,
        &warnings,
    ) catch |err| switch (err) {
        error.UnsupportedQueryBuilderGeneration, error.InvalidQueryBuilderGeneration => blk: {
            try warnings.append(alloc, "Generator-backed full-text query building failed, so the deterministic full-text builder was used.");
            break :blk try buildQueryBuilderQuery(alloc, request, effective_intent, effective_fields, &warnings);
        },
        else => return err,
    };
    const explanation = try buildQueryBuilderExplanation(alloc, effective_fields, built_query);
    const confidence = queryBuilderConfidence(effective_fields, built_query);
    const selectable_fields = try queryBuilderSelectableFields(alloc, request.constraints, effective_fields);
    const query_request = try buildQueryBuilderQueryRequest(alloc, request, built_query, selectable_fields, graph_indexes);
    const retrieval_query_request = try buildQueryBuilderRetrievalQueryRequest(alloc, request, query_request, graph_indexes);
    const artifact = if (retrieval_query_request != null) "retrieval_query_request" else "query_request";
    const specialist = pickQueryBuilderSpecialist(request.mode, built_query, query_request, retrieval_query_request != null);
    const plan = try buildQueryBuilderPlan(alloc, request.mode, request.output, specialist, built_query, artifact);
    const plan_validation_feedback = if (table_context.plan_validator) |validator|
        try validator.validateQueryRequest(alloc, request, query_request, retrieval_query_request, specialist)
    else
        null;
    const preflight_mode: QueryPreflightMode = if (table_context.runtime_query_request_validator != null) .estimate else .plan;
    var preflight = try preflightQueryRequestAgainstContext(
        alloc,
        &table_context,
        request,
        query_request,
        retrieval_query_request,
        specialist,
        .{ .mode = preflight_mode },
    );
    defer preflight.deinit(alloc);

    const example_document_count: usize = if (request.example_documents) |docs| docs.len else 0;
    const clarification_question = try buildQueryBuilderClarificationQuestion(
        alloc,
        request,
        effective_fields,
        semantic_indexes,
        graph_indexes,
        built_query,
        example_document_count,
    );
    if (plan_validation_feedback != null and queryBuilderConstraintBool(request.constraints, "require_executable") and clarification_question == null) {
        return error.InvalidQueryBuilderRequest;
    }
    try validateRequiredExecutableQueryBuilderRequest(request, built_query, query_request, retrieval_query_request, clarification_question != null);
    if (plan_validation_feedback) |feedback| {
        try warnings.append(alloc, feedback);
    }
    if (built_query.llm_warnings) |llm_warnings| {
        for (llm_warnings) |warning| try warnings.append(alloc, warning);
    }
    if (selectable_fields.len == 0) {
        try warnings.append(alloc, "No schema field context was available, so a generic Bleve query was generated.");
    }
    if (built_query.temporal_hint) {
        try warnings.append(alloc, "Temporal qualifiers were preserved in the text query and not converted into a structured date filter.");
    }
    if (unsupportedQueryBuilderMode(request.mode)) |mode| {
        try warnings.append(alloc, try std.fmt.allocPrint(alloc, "Query builder mode '{s}' is not implemented yet, so the deterministic full-text/filter builder was used.", .{mode}));
    } else if (request.mode) |mode| {
        if (std.ascii.eqlIgnoreCase(mode, "filter") and built_query.query_kind != .status_only) {
            try warnings.append(alloc, "Filter mode was requested, but no standalone structured predicate was detected, so a full-text query request was generated.");
        } else if (std.ascii.eqlIgnoreCase(mode, "tree") and retrieval_query_request == null) {
            try warnings.append(alloc, "Tree mode requires constraints.tree_search or constraints.tree_index, so only the seed query_request was generated.");
        } else if (std.ascii.eqlIgnoreCase(mode, "graph") and query_request.graph_searches == null) {
            try warnings.append(alloc, "Graph mode requires constraints.graph_searches, constraints.graph_index, or table graph index metadata, so the deterministic full-text/filter builder was used.");
        }
    }
    if (request.generator != null and generation_runner == null) {
        try warnings.append(alloc, "A generator was provided, but this execution path has no generation runner, so the deterministic query builder was used.");
    }
    if (request.table != null and table_context.schema_fields.len == 0 and example_document_count == 0 and (request.schema_fields == null or request.schema_fields.?.len == 0) and selectable_fields.len == 0) {
        try warnings.append(alloc, "The target table has no derived searchable fields, so field selection fell back to generic matching.");
    }

    return .{
        .session_id = session_id,
        .iteration = 1,
        .clarification_count = if (request.decisions) |decisions| @intCast(decisions.len) else 0,
        .status = if (clarification_question != null) .clarification_required else .completed,
        .steps = try alloc.dupe(metadata_openapi.AgentStep, &[_]metadata_openapi.AgentStep{.{
            .kind = if (clarification_question != null) .clarification else .generation,
            .name = if (clarification_question != null) "query_builder_clarification" else "query_builder",
            .action = if (clarification_question != null) "Generated a draft Antfly query request and asked for missing query-building context" else "Generated Antfly query request from natural language intent",
            .status = .success,
            .details = try buildQueryBuilderStepDetails(
                alloc,
                request.table,
                effective_fields,
                example_document_count,
                request.mode,
                request.output,
                specialist,
                &preflight,
            ),
        }}),
        .remaining_internal_iterations = @max(@as(i64, 0), (request.max_internal_iterations orelse 0) - 1),
        .remaining_user_clarifications = @max(
            @as(i64, 0),
            (request.max_user_clarifications orelse 0) - if (request.decisions) |decisions| @as(i64, @intCast(decisions.len)) else 0,
        ),
        .questions = if (clarification_question) |question| try alloc.dupe(AgentQuestion, &[_]AgentQuestion{question}) else null,
        .query = built_query.query,
        .query_request = query_request,
        .retrieval_query_request = retrieval_query_request,
        .specialist = specialist,
        .plan = plan,
        .explanation = explanation,
        .confidence = confidence,
        .warnings = if (warnings.items.len > 0) try warnings.toOwnedSlice(alloc) else null,
    };
}

pub fn executeQueryBuilder(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    table_schema_fields: ?[]const []const u8,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), request, table_schema_fields);
    return try std.json.Stringify.valueAlloc(alloc, result, .{});
}

const BuiltQueryBuilderQuery = struct {
    query: std.json.Value,
    temporal_hint: bool = false,
    query_kind: enum {
        generic,
        field_match,
        conjunction,
        status_only,
        llm_full_text,
        semantic,
        hybrid,
    },
    text_field: ?[]const u8 = null,
    text_value: ?[]const u8 = null,
    status_field: ?[]const u8 = null,
    status_value: ?[]const u8 = null,
    date_field: ?[]const u8 = null,
    date_start: ?[]const u8 = null,
    date_end: ?[]const u8 = null,
    date_inclusive_start: bool = false,
    date_inclusive_end: bool = false,
    term_filters: []const QueryBuilderTermFilter = &.{},
    indexes: ?[]const []const u8 = null,
    graph_searches: ?std.json.ArrayHashMap(indexes_openapi.GraphQuery) = null,
    llm_explanation: ?[]const u8 = null,
    llm_confidence: ?f64 = null,
    llm_warnings: ?[]const []const u8 = null,
};

const QueryBuilderTermFilter = struct {
    field: []const u8,
    value: []const u8,
    negated: bool = false,
};

fn ensureQueryBuilderSessionId(alloc: std.mem.Allocator, session_id: ?[]const u8) ![]const u8 {
    if (session_id) |value| {
        if (value.len > 0) return value;
    }
    return try std.fmt.allocPrint(alloc, "qbs_{x}", .{platform_time.monotonicNs()});
}

fn appendDecisionContext(
    alloc: std.mem.Allocator,
    base: []const u8,
    decisions: []const metadata_openapi.AgentDecision,
) ![]const u8 {
    if (decisions.len == 0) return base;

    var has_intent_decision = false;
    for (decisions) |decision| {
        if (queryBuilderDecisionAffectsIntent(decision.question_id)) {
            has_intent_decision = true;
            break;
        }
    }
    if (!has_intent_decision) return base;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, base);
    try out.appendSlice(alloc, "\n\nResolved user decisions:\n");
    for (decisions) |decision| {
        if (!queryBuilderDecisionAffectsIntent(decision.question_id)) continue;
        try out.appendSlice(alloc, "- ");
        if (decision.question_id.len > 0) {
            try out.appendSlice(alloc, decision.question_id);
            try out.appendSlice(alloc, ": ");
        }
        if (decision.approved) |approved| {
            try out.appendSlice(alloc, if (approved) "approved" else "rejected");
        } else if (decision.answer) |answer| {
            try appendJsonValueText(alloc, &out, answer);
        } else {
            try out.appendSlice(alloc, "answered");
        }
        try out.append(alloc, '\n');
    }
    return try out.toOwnedSlice(alloc);
}

fn queryBuilderDecisionAffectsIntent(question_id: []const u8) bool {
    if (std.mem.eql(u8, question_id, "select_semantic_index")) return false;
    if (std.mem.eql(u8, question_id, "select_tree_index")) return false;
    if (std.mem.eql(u8, question_id, "select_graph_index")) return false;
    if (std.mem.eql(u8, question_id, "select_text_field")) return false;
    if (std.mem.eql(u8, question_id, "select_query_strategy")) return false;
    if (std.mem.eql(u8, question_id, "select_query_table")) return false;
    return true;
}

fn appendJsonValueText(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
) !void {
    switch (value) {
        .null => try out.appendSlice(alloc, "null"),
        .bool => |bool_value| try out.appendSlice(alloc, if (bool_value) "true" else "false"),
        .integer => |integer_value| {
            const s = try std.fmt.allocPrint(alloc, "{d}", .{integer_value});
            defer alloc.free(s);
            try out.appendSlice(alloc, s);
        },
        .float => |float_value| {
            const s = try std.fmt.allocPrint(alloc, "{d}", .{float_value});
            defer alloc.free(s);
            try out.appendSlice(alloc, s);
        },
        .number_string => |number_value| try out.appendSlice(alloc, number_value),
        .string => |string_value| try out.appendSlice(alloc, string_value),
        else => {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        },
    }
}

fn resolveQueryBuilderFields(
    alloc: std.mem.Allocator,
    request_fields: ?[]const []const u8,
    table_schema_fields: ?[]const []const u8,
    example_documents: ?[]const std.json.Value,
) ![]const []const u8 {
    if (request_fields) |fields| {
        if (fields.len > 0) return fields;
    }
    if (table_schema_fields) |fields| {
        if (fields.len > 0) return fields;
    }
    return try collectQueryBuilderExampleFields(alloc, example_documents orelse &.{});
}

fn collectQueryBuilderExampleFields(
    alloc: std.mem.Allocator,
    example_documents: []const std.json.Value,
) ![]const []const u8 {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    defer fields.deinit(alloc);

    for (example_documents) |document| {
        if (document != .object) continue;
        var it = document.object.iterator();
        while (it.next()) |entry| {
            if (seen.contains(entry.key_ptr.*)) continue;
            try seen.put(alloc, entry.key_ptr.*, {});
            try fields.append(alloc, entry.key_ptr.*);
        }
    }

    if (fields.items.len == 0) return &.{};
    return try fields.toOwnedSlice(alloc);
}

fn buildQueryBuilderSpecialist(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    intent: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    embedding_index_metadata: []const QueryBuilderEmbeddingIndex,
    semantic_indexes: []const []const u8,
    graph_indexes: []const []const u8,
    graph_index_metadata: []const QueryBuilderGraphIndex,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: ?GenerationRunner,
    warnings: *std.ArrayListUnmanaged([]const u8),
) !BuiltQueryBuilderQuery {
    const effective_mode = try queryBuilderEffectiveMode(alloc, request, fields, semantic_indexes);
    if (shouldUseGeneratedSemanticBuilder(request, effective_mode, generation_runner)) {
        return buildGeneratedSemanticOrHybridQueryBuilder(alloc, request, effective_mode.?, intent, fields, full_text_index_metadata, embedding_index_metadata, semantic_indexes, plan_validator, generation_runner.?, warnings) catch {
            try warnings.append(alloc, "Generator-backed semantic or hybrid query building failed, so the deterministic semantic or hybrid builder was used.");
            if (try buildSemanticOrHybridQueryBuilder(alloc, request, effective_mode, intent, fields, semantic_indexes, warnings)) |built| {
                return built;
            }
            return try buildQueryBuilderQuery(alloc, request, intent, fields, warnings);
        };
    }
    if (try buildSemanticOrHybridQueryBuilder(alloc, request, effective_mode, intent, fields, semantic_indexes, warnings)) |built| {
        return built;
    }
    if (shouldUseGeneratedGraphBuilder(request, effective_mode, generation_runner)) {
        return buildGeneratedGraphQueryBuilder(alloc, request, intent, fields, graph_indexes, graph_index_metadata, plan_validator, generation_runner.?, warnings);
    }
    if (shouldUseGeneratedFullTextBuilder(request, effective_mode, generation_runner)) {
        return buildGeneratedFullTextQueryBuilder(alloc, request, intent, fields, full_text_index_metadata, plan_validator, generation_runner.?);
    }
    return try buildQueryBuilderQuery(alloc, request, intent, fields, warnings);
}

fn buildSemanticOrHybridQueryBuilder(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    effective_mode: ?[]const u8,
    intent: []const u8,
    fields: []const []const u8,
    semantic_indexes: []const []const u8,
    warnings: *std.ArrayListUnmanaged([]const u8),
) !?BuiltQueryBuilderQuery {
    const mode = effective_mode orelse return null;
    const semantic_mode = std.ascii.eqlIgnoreCase(mode, "semantic");
    const hybrid_mode = std.ascii.eqlIgnoreCase(mode, "hybrid");
    if (!semantic_mode and !hybrid_mode) return null;

    const preferred_indexes = try queryBuilderPreferredIndexSlice(alloc, request);
    const source_indexes = if (preferred_indexes.len > 0) preferred_indexes else semantic_indexes;
    if (source_indexes.len == 0) {
        try warnings.append(alloc, "Semantic or hybrid query builder mode requires constraints.prefer_indexes or table embedding indexes, so the deterministic full-text builder was used.");
        return null;
    }

    var base = try buildQueryBuilderQuery(alloc, request, intent, fields, warnings);
    base.query_kind = if (semantic_mode) .semantic else .hybrid;
    if (base.text_value == null) base.text_value = intent;
    base.indexes = try cloneStringSlice(alloc, source_indexes);
    return base;
}

fn shouldUseGeneratedGraphBuilder(
    request: metadata_openapi.QueryBuilderRequest,
    effective_mode: ?[]const u8,
    generation_runner: ?GenerationRunner,
) bool {
    if (request.generator == null or generation_runner == null) return false;
    if (unsupportedQueryBuilderMode(effective_mode) != null) return false;
    const mode = effective_mode orelse return false;
    return std.ascii.eqlIgnoreCase(mode, "graph");
}

fn shouldUseGeneratedFullTextBuilder(
    request: metadata_openapi.QueryBuilderRequest,
    effective_mode: ?[]const u8,
    generation_runner: ?GenerationRunner,
) bool {
    if (request.generator == null or generation_runner == null) return false;
    if (unsupportedQueryBuilderMode(effective_mode) != null) return false;
    if (effective_mode) |mode| {
        if (std.ascii.eqlIgnoreCase(mode, "filter")) return false;
        if (std.ascii.eqlIgnoreCase(mode, "semantic")) return false;
        if (std.ascii.eqlIgnoreCase(mode, "hybrid")) return false;
        if (std.ascii.eqlIgnoreCase(mode, "tree")) return false;
        if (std.ascii.eqlIgnoreCase(mode, "graph")) return false;
    }
    return true;
}

fn shouldUseGeneratedSemanticBuilder(
    request: metadata_openapi.QueryBuilderRequest,
    effective_mode: ?[]const u8,
    generation_runner: ?GenerationRunner,
) bool {
    if (request.generator == null or generation_runner == null) return false;
    if (unsupportedQueryBuilderMode(effective_mode) != null) return false;
    const mode = effective_mode orelse return false;
    return std.ascii.eqlIgnoreCase(mode, "semantic") or std.ascii.eqlIgnoreCase(mode, "hybrid");
}

const GeneratedQueryBuilderResponse = struct {
    query: std.json.Value,
    explanation: ?[]const u8 = null,
    confidence: ?f64 = null,
    warnings: ?[]const []const u8 = null,
};

const GeneratedSemanticQueryBuilderResponse = struct {
    semantic_search: ?[]const u8 = null,
    indexes: ?[]const []const u8 = null,
    full_text_search: ?std.json.Value = null,
    explanation: ?[]const u8 = null,
    confidence: ?f64 = null,
    warnings: ?[]const []const u8 = null,
};

const GeneratedFullTextQueryBuilderPlan = struct {
    query: std.json.Value,
    explanation: ?[]const u8 = null,
    confidence: ?f64 = null,
    warnings: ?[]const []const u8 = null,
};

const GeneratedFullTextQueryBuilderAttempt = union(enum) {
    valid: GeneratedFullTextQueryBuilderPlan,
    feedback: []const u8,
};

const GeneratedSemanticQueryBuilderPlan = struct {
    semantic_search: []const u8,
    indexes: []const []const u8,
    full_text_search: ?std.json.Value = null,
    explanation: ?[]const u8 = null,
    confidence: ?f64 = null,
    warnings: ?[]const []const u8 = null,
};

const GeneratedSemanticQueryBuilderAttempt = union(enum) {
    valid: GeneratedSemanticQueryBuilderPlan,
    feedback: []const u8,
};

const GeneratedGraphQueryBuilderResponse = struct {
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
    explanation: ?[]const u8 = null,
    confidence: ?f64 = null,
    warnings: ?[]const []const u8 = null,
};

const GeneratedGraphQueryBuilderPlan = struct {
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
    explanation: ?[]const u8 = null,
    confidence: ?f64 = null,
    warnings: ?[]const []const u8 = null,
};

const GeneratedGraphQueryBuilderAttempt = union(enum) {
    valid: GeneratedGraphQueryBuilderPlan,
    feedback: []const u8,
};

fn buildGeneratedSemanticOrHybridQueryBuilder(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    mode: []const u8,
    intent: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    embedding_index_metadata: []const QueryBuilderEmbeddingIndex,
    semantic_indexes: []const []const u8,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
    warnings: *std.ArrayListUnmanaged([]const u8),
) !BuiltQueryBuilderQuery {
    var base = try buildQueryBuilderQuery(alloc, request, intent, fields, warnings);
    const generated = try buildGeneratedSemanticOrHybridQueryBuilderPlan(
        alloc,
        request,
        mode,
        intent,
        fields,
        full_text_index_metadata,
        embedding_index_metadata,
        semantic_indexes,
        plan_validator,
        generation_runner,
    );
    const hybrid_mode = std.ascii.eqlIgnoreCase(mode, "hybrid");
    base.query_kind = if (hybrid_mode) .hybrid else .semantic;
    base.text_value = generated.semantic_search;
    base.indexes = generated.indexes;
    if (generated.full_text_search) |full_text_search| {
        base.query = full_text_search;
        base.text_field = null;
    }
    base.llm_explanation = generated.explanation;
    base.llm_confidence = normalizeQueryBuilderConfidence(generated.confidence);
    base.llm_warnings = generated.warnings;
    return base;
}

fn buildGeneratedSemanticOrHybridQueryBuilderPlan(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    mode: []const u8,
    intent: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    embedding_index_metadata: []const QueryBuilderEmbeddingIndex,
    semantic_indexes: []const []const u8,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
) !GeneratedSemanticQueryBuilderPlan {
    const generator_cfg = request.generator orelse return error.UnsupportedQueryBuilderGeneration;
    const chain = try buildQueryBuilderGenerationChain(alloc, generator_cfg);
    const preferred_indexes = try queryBuilderPreferredIndexSlice(alloc, request);
    const source_indexes = try queryBuilderGeneratedSemanticIndexNames(alloc, embedding_index_metadata, semantic_indexes, preferred_indexes);
    if (source_indexes.len == 0) return error.InvalidQueryBuilderGeneration;
    const messages = try buildSemanticQueryBuilderMessages(alloc, intent, mode, fields, full_text_index_metadata, embedding_index_metadata, source_indexes, request.example_documents);
    const first_attempt = buildGeneratedSemanticQueryBuilderAttemptFromMessages(alloc, request, mode, fields, source_indexes, plan_validator, generation_runner, chain, messages) catch |first_err| switch (first_err) {
        error.InvalidQueryBuilderGeneration => {
            const repair_messages = try buildSemanticQueryBuilderRepairMessages(alloc, messages, null);
            const second_attempt = buildGeneratedSemanticQueryBuilderAttemptFromMessages(alloc, request, mode, fields, source_indexes, plan_validator, generation_runner, chain, repair_messages) catch |second_err| return switch (second_err) {
                error.InvalidQueryBuilderGeneration => first_err,
                else => second_err,
            };
            return switch (second_attempt) {
                .valid => |plan| plan,
                .feedback => first_err,
            };
        },
        else => return first_err,
    };
    return switch (first_attempt) {
        .valid => |plan| plan,
        .feedback => |feedback| blk: {
            const repair_messages = try buildSemanticQueryBuilderRepairMessages(alloc, messages, feedback);
            const second_attempt = buildGeneratedSemanticQueryBuilderAttemptFromMessages(alloc, request, mode, fields, source_indexes, plan_validator, generation_runner, chain, repair_messages) catch |second_err| return switch (second_err) {
                error.InvalidQueryBuilderGeneration => error.InvalidQueryBuilderGeneration,
                else => second_err,
            };
            break :blk switch (second_attempt) {
                .valid => |plan| plan,
                .feedback => error.InvalidQueryBuilderGeneration,
            };
        },
    };
}

fn queryBuilderGeneratedSemanticIndexNames(
    alloc: std.mem.Allocator,
    embedding_index_metadata: []const QueryBuilderEmbeddingIndex,
    semantic_indexes: []const []const u8,
    preferred_indexes: []const []const u8,
) ![]const []const u8 {
    if (preferred_indexes.len == 0) return semantic_indexes;
    if (embedding_index_metadata.len == 0) return preferred_indexes;

    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);
    for (preferred_indexes) |preferred_index| {
        for (embedding_index_metadata) |metadata| {
            if (!std.mem.eql(u8, metadata.name, preferred_index)) continue;
            if (!metadata.sparse) try out.append(alloc, preferred_index);
            break;
        }
    }
    return if (out.items.len == 0) &.{} else try out.toOwnedSlice(alloc);
}

fn buildGeneratedSemanticQueryBuilderAttemptFromMessages(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    mode: []const u8,
    fields: []const []const u8,
    semantic_indexes: []const []const u8,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
    chain: []const generating.ChainLink,
    messages: []const generating.ChatMessage,
) !GeneratedSemanticQueryBuilderAttempt {
    var result = generation_runner.executeChain(alloc, chain, messages) catch return error.UnsupportedQueryBuilderGeneration;
    defer result.deinit();

    const json_text = extractJsonObjectSlice(result.content) orelse return error.InvalidQueryBuilderGeneration;
    var parsed = std.json.parseFromSlice(GeneratedSemanticQueryBuilderResponse, alloc, json_text, .{
        .ignore_unknown_fields = true,
    }) catch return error.InvalidQueryBuilderGeneration;
    defer parsed.deinit();
    try validateGeneratedSemanticQueryBuilderResponse(request, mode, fields, semantic_indexes, parsed.value);
    const full_text_search = queryBuilderNonNullJsonValue(parsed.value.full_text_search);
    if (plan_validator) |validator| {
        const query_request = metadata_openapi.QueryRequest{
            .table = queryBuilderEffectiveTable(request),
            .semantic_search = parsed.value.semantic_search,
            .indexes = parsed.value.indexes,
            .full_text_search = full_text_search,
        };
        if (try validator.validateQueryRequest(alloc, request, query_request, null, if (std.ascii.eqlIgnoreCase(mode, "hybrid")) "hybrid" else "semantic")) |feedback| {
            return .{ .feedback = feedback };
        }
    }

    return .{ .valid = .{
        .semantic_search = try alloc.dupe(u8, parsed.value.semantic_search.?),
        .indexes = try cloneStringSlice(alloc, parsed.value.indexes.?),
        .full_text_search = if (full_text_search) |query| try cloneJsonValueLeaky(alloc, query) else null,
        .explanation = if (parsed.value.explanation) |explanation| try alloc.dupe(u8, explanation) else null,
        .confidence = parsed.value.confidence,
        .warnings = if (parsed.value.warnings) |warning_values| try cloneStringSlice(alloc, warning_values) else null,
    } };
}

fn validateGeneratedSemanticQueryBuilderResponse(
    request: metadata_openapi.QueryBuilderRequest,
    mode: []const u8,
    fields: []const []const u8,
    semantic_indexes: []const []const u8,
    response: GeneratedSemanticQueryBuilderResponse,
) QueryBuilderValidationError!void {
    const semantic_search = response.semantic_search orelse return error.InvalidQueryBuilderGeneration;
    if (semantic_search.len == 0) return error.InvalidQueryBuilderGeneration;
    const indexes = response.indexes orelse return error.InvalidQueryBuilderGeneration;
    if (indexes.len == 0) return error.InvalidQueryBuilderGeneration;
    if (semantic_indexes.len == 0) return error.InvalidQueryBuilderGeneration;
    for (indexes) |index| {
        if (!queryBuilderFieldInSlice(semantic_indexes, index)) return error.InvalidQueryBuilderGeneration;
    }
    const hybrid_mode = std.ascii.eqlIgnoreCase(mode, "hybrid");
    if (queryBuilderNonNullJsonValue(response.full_text_search)) |full_text_search| {
        if (full_text_search != .object) return error.InvalidQueryBuilderGeneration;
        try validateGeneratedQueryBuilderBleveQuery(request, fields, full_text_search);
    } else if (hybrid_mode) {
        return error.InvalidQueryBuilderGeneration;
    }
}

fn queryBuilderNonNullJsonValue(value: ?std.json.Value) ?std.json.Value {
    const resolved = value orelse return null;
    if (resolved == .null) return null;
    return resolved;
}

fn buildGeneratedFullTextQueryBuilder(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    intent: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
) !BuiltQueryBuilderQuery {
    const generated = try buildGeneratedFullTextQueryBuilderPlan(alloc, request, intent, fields, full_text_index_metadata, plan_validator, generation_runner);
    return .{
        .query = generated.query,
        .temporal_hint = detectTemporalHint(intent),
        .query_kind = .llm_full_text,
        .text_value = intent,
        .llm_explanation = generated.explanation,
        .llm_confidence = normalizeQueryBuilderConfidence(generated.confidence),
        .llm_warnings = generated.warnings,
    };
}

fn buildGeneratedFullTextQueryBuilderPlan(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    intent: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
) !GeneratedFullTextQueryBuilderPlan {
    const generator_cfg = request.generator orelse return error.UnsupportedQueryBuilderGeneration;
    const chain = try buildQueryBuilderGenerationChain(alloc, generator_cfg);
    const messages = try buildBleveQueryBuilderMessages(alloc, intent, fields, full_text_index_metadata, request.example_documents);
    const first_attempt = buildGeneratedFullTextQueryBuilderAttemptFromMessages(alloc, request, fields, plan_validator, generation_runner, chain, messages) catch |first_err| switch (first_err) {
        error.InvalidQueryBuilderGeneration => {
            const repair_messages = try buildBleveQueryBuilderRepairMessages(alloc, messages, null);
            const second_attempt = buildGeneratedFullTextQueryBuilderAttemptFromMessages(alloc, request, fields, plan_validator, generation_runner, chain, repair_messages) catch |second_err| return switch (second_err) {
                error.InvalidQueryBuilderGeneration => first_err,
                else => second_err,
            };
            return switch (second_attempt) {
                .valid => |plan| plan,
                .feedback => first_err,
            };
        },
        else => return first_err,
    };
    return switch (first_attempt) {
        .valid => |plan| plan,
        .feedback => |feedback| blk: {
            const repair_messages = try buildBleveQueryBuilderRepairMessages(alloc, messages, feedback);
            const second_attempt = buildGeneratedFullTextQueryBuilderAttemptFromMessages(alloc, request, fields, plan_validator, generation_runner, chain, repair_messages) catch |second_err| return switch (second_err) {
                error.InvalidQueryBuilderGeneration => error.InvalidQueryBuilderGeneration,
                else => second_err,
            };
            break :blk switch (second_attempt) {
                .valid => |plan| plan,
                .feedback => error.InvalidQueryBuilderGeneration,
            };
        },
    };
}

fn buildGeneratedFullTextQueryBuilderAttemptFromMessages(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
    chain: []const generating.ChainLink,
    messages: []const generating.ChatMessage,
) !GeneratedFullTextQueryBuilderAttempt {
    var result = generation_runner.executeChain(alloc, chain, messages) catch return error.UnsupportedQueryBuilderGeneration;
    defer result.deinit();

    const json_text = extractJsonObjectSlice(result.content) orelse return error.InvalidQueryBuilderGeneration;
    var parsed = std.json.parseFromSlice(GeneratedQueryBuilderResponse, alloc, json_text, .{
        .ignore_unknown_fields = true,
    }) catch return error.InvalidQueryBuilderGeneration;
    defer parsed.deinit();
    if (parsed.value.query != .object) return error.InvalidQueryBuilderGeneration;
    try validateGeneratedQueryBuilderBleveQuery(request, fields, parsed.value.query);
    if (plan_validator) |validator| {
        if (try validator.validateBleveQuery(alloc, request, parsed.value.query)) |feedback| {
            return .{ .feedback = feedback };
        }
    }

    return .{ .valid = .{
        .query = try cloneJsonValueLeaky(alloc, parsed.value.query),
        .explanation = if (parsed.value.explanation) |explanation| try alloc.dupe(u8, explanation) else null,
        .confidence = parsed.value.confidence,
        .warnings = if (parsed.value.warnings) |warning_values| try cloneStringSlice(alloc, warning_values) else null,
    } };
}

fn buildGeneratedGraphQueryBuilder(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    intent: []const u8,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
    graph_index_metadata: []const QueryBuilderGraphIndex,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
    warnings: *std.ArrayListUnmanaged([]const u8),
) !BuiltQueryBuilderQuery {
    var base = try buildQueryBuilderQuery(alloc, request, intent, fields, warnings);
    const generated = buildGeneratedGraphQueryBuilderPlan(alloc, request, intent, fields, graph_indexes, graph_index_metadata, base, plan_validator, generation_runner) catch {
        try warnings.append(alloc, "Generator-backed graph query building failed, so the deterministic graph builder was used.");
        return base;
    };
    base.graph_searches = generated.graph_searches;
    base.llm_explanation = generated.explanation;
    base.llm_confidence = normalizeQueryBuilderConfidence(generated.confidence);
    base.llm_warnings = generated.warnings;
    return base;
}

fn buildGeneratedGraphQueryBuilderPlan(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    intent: []const u8,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
    graph_index_metadata: []const QueryBuilderGraphIndex,
    base: BuiltQueryBuilderQuery,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
) !GeneratedGraphQueryBuilderPlan {
    const generator_cfg = request.generator orelse return error.UnsupportedQueryBuilderGeneration;
    const chain = try buildQueryBuilderGenerationChain(alloc, generator_cfg);
    const messages = try buildGraphQueryBuilderMessages(alloc, intent, fields, graph_indexes, graph_index_metadata, request.example_documents);
    const seed_query_request = try buildQueryBuilderGraphSeedQueryRequest(alloc, request, base, fields);
    const first_attempt = buildGeneratedGraphQueryBuilderAttemptFromMessages(alloc, request, fields, graph_indexes, seed_query_request, plan_validator, generation_runner, chain, messages) catch |first_err| switch (first_err) {
        error.InvalidQueryBuilderGeneration => {
            const repair_messages = try buildGraphQueryBuilderRepairMessages(alloc, messages, null);
            const second_attempt = buildGeneratedGraphQueryBuilderAttemptFromMessages(alloc, request, fields, graph_indexes, seed_query_request, plan_validator, generation_runner, chain, repair_messages) catch |second_err| return switch (second_err) {
                error.InvalidQueryBuilderGeneration => first_err,
                else => second_err,
            };
            return switch (second_attempt) {
                .valid => |plan| plan,
                .feedback => first_err,
            };
        },
        else => return first_err,
    };
    return switch (first_attempt) {
        .valid => |plan| plan,
        .feedback => |feedback| blk: {
            const repair_messages = try buildGraphQueryBuilderRepairMessages(alloc, messages, feedback);
            const second_attempt = buildGeneratedGraphQueryBuilderAttemptFromMessages(alloc, request, fields, graph_indexes, seed_query_request, plan_validator, generation_runner, chain, repair_messages) catch |second_err| return switch (second_err) {
                error.InvalidQueryBuilderGeneration => error.InvalidQueryBuilderGeneration,
                else => second_err,
            };
            break :blk switch (second_attempt) {
                .valid => |plan| plan,
                .feedback => error.InvalidQueryBuilderGeneration,
            };
        },
    };
}

fn buildGeneratedGraphQueryBuilderAttemptFromMessages(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
    seed_query_request: metadata_openapi.QueryRequest,
    plan_validator: ?QueryBuilderPlanValidator,
    generation_runner: GenerationRunner,
    chain: []const generating.ChainLink,
    messages: []const generating.ChatMessage,
) !GeneratedGraphQueryBuilderAttempt {
    var result = generation_runner.executeChain(alloc, chain, messages) catch return error.UnsupportedQueryBuilderGeneration;
    defer result.deinit();

    const json_text = extractJsonObjectSlice(result.content) orelse return error.InvalidQueryBuilderGeneration;
    var parsed = std.json.parseFromSlice(GeneratedGraphQueryBuilderResponse, alloc, json_text, .{
        .ignore_unknown_fields = true,
    }) catch return error.InvalidQueryBuilderGeneration;
    defer parsed.deinit();
    try validateGeneratedQueryBuilderGraphSearches(request, fields, graph_indexes, parsed.value.graph_searches);
    if (try metadataValidateGraphSearchResultRefs(alloc, seed_query_request, parsed.value.graph_searches)) |feedback| {
        return .{ .feedback = feedback };
    }
    if (plan_validator) |validator| {
        if (try validator.validateGraphSearches(alloc, request, parsed.value.graph_searches)) |feedback| {
            return .{ .feedback = feedback };
        }
    }

    return .{ .valid = .{
        .graph_searches = try cloneGraphSearchesLeaky(alloc, parsed.value.graph_searches),
        .explanation = if (parsed.value.explanation) |explanation| try alloc.dupe(u8, explanation) else null,
        .confidence = parsed.value.confidence,
        .warnings = if (parsed.value.warnings) |warning_values| try cloneStringSlice(alloc, warning_values) else null,
    } };
}

fn buildQueryBuilderGraphSeedQueryRequest(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    built: BuiltQueryBuilderQuery,
    fields: []const []const u8,
) !metadata_openapi.QueryRequest {
    var out = metadata_openapi.QueryRequest{
        .table = queryBuilderEffectiveTable(request),
    };
    switch (built.query_kind) {
        .status_only => {},
        .conjunction => {
            out.full_text_search = try buildQueryBuilderMatchQueryValue(alloc, built.text_value.?, built.text_field.?);
        },
        .field_match, .generic, .llm_full_text => {
            out.full_text_search = built.query;
        },
        .semantic => {
            out.semantic_search = built.text_value;
            out.indexes = built.indexes;
        },
        .hybrid => {
            out.semantic_search = built.text_value;
            out.indexes = built.indexes;
            if (built.text_field != null and built.text_value != null) {
                out.full_text_search = try buildQueryBuilderMatchQueryValue(alloc, built.text_value.?, built.text_field.?);
            } else if (built.status_field == null or built.status_value == null) {
                out.full_text_search = built.query;
            }
        },
    }
    out.filter_query = try buildQueryBuilderFilterQueryValue(alloc, built);
    out.filter_query = try combineQueryBuilderFilterQueries(
        alloc,
        out.filter_query,
        try buildQueryBuilderConstraintFilterQueryValue(alloc, request.constraints, fields),
    );
    return out;
}

fn buildGraphQueryBuilderRepairMessages(
    alloc: std.mem.Allocator,
    messages: []const generating.ChatMessage,
    feedback: ?[]const u8,
) ![]const generating.ChatMessage {
    const base_prompt =
        \\The previous graph_searches response failed deterministic validation. Regenerate a valid response.
        \\
        \\Requirements:
        \\- Return only the corrected JSON object.
        \\- graph_searches must be non-empty.
        \\- Every GraphQuery must include index_name and start_nodes.
        \\- Use only listed graph indexes.
        \\- Use start_nodes/target_nodes keys or one of $full_text_results, $fused_results, $embeddings_results, or $graph_results.<search_name>.
        \\- $graph_results.<search_name> refs must point to another graph_searches entry and must not form a cycle.
        \\- Do not include node_filter, algorithm, algorithm_params, include_edges, or unknown fields.
        \\- shortest_path and k_shortest_paths require target_nodes.
        \\- pattern queries require unique aliases, an edge on every step after the first, and return_aliases that exist in the pattern.
    ;
    const repair_prompt = if (feedback) |value|
        try std.fmt.allocPrint(
            alloc,
            \\{s}
            \\
            \\Query-builder plan validation feedback:
            \\{s}
        ,
            .{ base_prompt, value },
        )
    else
        base_prompt;
    const out = try alloc.alloc(generating.ChatMessage, messages.len + 1);
    @memcpy(out[0..messages.len], messages);
    out[messages.len] = .{ .role = .user, .content = .{ .text = repair_prompt } };
    return out;
}

fn buildQueryBuilderGenerationChain(
    alloc: std.mem.Allocator,
    generator_cfg: generating_openapi.GeneratorConfig,
) ![]const generating.ChainLink {
    return try alloc.dupe(generating.ChainLink, &[_]generating.ChainLink{.{
        .generator = try generatorConfigFromPublic(generator_cfg),
    }});
}

fn generatorConfigFromPublic(cfg: generating_openapi.GeneratorConfig) !generating.GeneratorConfig {
    const provider: generating.Provider = switch (cfg.provider) {
        .gemini => .gemini,
        .vertex => .vertex,
        .openai => .openai,
        .ollama => .ollama,
        .antfly => .antfly,
        else => return error.UnsupportedQueryBuilderGeneration,
    };
    const model = cfg.model orelse return error.InvalidQueryBuilderGeneration;
    const url = switch (provider) {
        .antfly => cfg.api_url orelse "",
        .gemini, .vertex => cfg.url orelse "",
        .openai, .ollama => cfg.url orelse return error.InvalidQueryBuilderGeneration,
        else => return error.UnsupportedQueryBuilderGeneration,
    };
    return .{
        .provider = provider,
        .model = model,
        .url = url,
        .api_key = cfg.api_key,
        .project_id = cfg.project_id,
        .location = cfg.location,
        .credentials_path = cfg.credentials_path,
    };
}

fn buildSemanticQueryBuilderMessages(
    alloc: std.mem.Allocator,
    intent: []const u8,
    mode: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    embedding_index_metadata: []const QueryBuilderEmbeddingIndex,
    semantic_indexes: []const []const u8,
    example_documents: ?[]const std.json.Value,
) ![]const generating.ChatMessage {
    const system = try buildSemanticQueryBuilderSystemPrompt(alloc, mode, fields, full_text_index_metadata, embedding_index_metadata, semantic_indexes, example_documents orelse &.{});
    const hybrid_mode = std.ascii.eqlIgnoreCase(mode, "hybrid");
    const user = try std.fmt.allocPrint(
        alloc,
        \\User's retrieval intent: "{s}"
        \\
        \\Generate an Antfly {s} query plan that fulfills this intent.
        \\Return your response in JSON format:
        \\{{
        \\  "semantic_search": "dense embedding query text",
        \\  "indexes": ["dense_embedding_index"],
        \\  "full_text_search": {s},
        \\  "explanation": "brief explanation of the query plan",
        \\  "confidence": 0.0,
        \\  "warnings": []
        \\}}
        \\
        \\Return ONLY the JSON object, no additional text.
    ,
        .{ intent, if (hybrid_mode) "hybrid" else "semantic", if (hybrid_mode) "{ ... native Bleve query ... }" else "null" },
    );
    return try alloc.dupe(generating.ChatMessage, &[_]generating.ChatMessage{
        .{ .role = .system, .content = .{ .text = system } },
        .{ .role = .user, .content = .{ .text = user } },
    });
}

fn buildSemanticQueryBuilderRepairMessages(
    alloc: std.mem.Allocator,
    messages: []const generating.ChatMessage,
    feedback: ?[]const u8,
) ![]const generating.ChatMessage {
    const base_prompt =
        \\The previous semantic or hybrid query response failed deterministic validation. Regenerate a valid response.
        \\
        \\Requirements:
        \\- Return only the corrected JSON object.
        \\- semantic_search must be non-empty.
        \\- indexes must contain at least one listed dense embedding index.
        \\- Do not use sparse embedding indexes for semantic_search.
        \\- Hybrid mode must include full_text_search as a native Bleve query object.
        \\- Use only schema fields and full-text index fields listed in the prompt.
        \\- Do not invent fields, indexes, operators, or wrapper objects outside the requested response format.
    ;
    const repair_prompt = if (feedback) |value|
        try std.fmt.allocPrint(
            alloc,
            \\{s}
            \\
            \\Query-builder plan validation feedback:
            \\{s}
        ,
            .{ base_prompt, value },
        )
    else
        base_prompt;
    const out = try alloc.alloc(generating.ChatMessage, messages.len + 1);
    @memcpy(out[0..messages.len], messages);
    out[messages.len] = .{ .role = .user, .content = .{ .text = repair_prompt } };
    return out;
}

fn buildSemanticQueryBuilderSystemPrompt(
    alloc: std.mem.Allocator,
    mode: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    embedding_index_metadata: []const QueryBuilderEmbeddingIndex,
    semantic_indexes: []const []const u8,
    example_documents: []const std.json.Value,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const hybrid_mode = std.ascii.eqlIgnoreCase(mode, "hybrid");

    try out.appendSlice(alloc,
        \\You are an expert Antfly query builder. Translate natural-language retrieval intent into an Antfly semantic query plan.
        \\
        \\semantic_search is the text to embed for dense vector search. Keep it concise and preserve user-critical nouns, filters, and entities.
        \\Use only dense embedding indexes listed below. Sparse indexes are metadata hints only and must not be returned in indexes.
        \\Do not invent fields or indexes.
        \\
    );
    if (hybrid_mode) {
        try out.appendSlice(alloc,
            \\Hybrid mode must also include full_text_search as a native Bleve query object that complements semantic_search.
            \\
        );
    }

    try out.appendSlice(alloc, "\nAllowed dense embedding indexes:\n");
    if (semantic_indexes.len == 0) {
        try out.appendSlice(alloc, "- <none provided>\n");
    } else {
        for (semantic_indexes) |index| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, index);
            try out.append(alloc, '\n');
        }
    }

    try out.appendSlice(alloc, "\nEmbedding index metadata:\n");
    if (embedding_index_metadata.len == 0) {
        try out.appendSlice(alloc, "- <none provided>\n");
    } else {
        for (embedding_index_metadata) |index| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, index.name);
            try out.appendSlice(alloc, if (index.sparse) " sparse" else " dense");
            if (index.dimension) |dimension| try out.print(alloc, " dimension: {d}", .{dimension});
            if (index.model) |model| {
                try out.appendSlice(alloc, " model: ");
                try out.appendSlice(alloc, model);
            }
            try out.append(alloc, '\n');
        }
    }

    try out.appendSlice(alloc, "\nSchema fields:\n");
    if (fields.len == 0) {
        try out.appendSlice(alloc, "- <none provided>\n");
    } else {
        for (fields) |field| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, field);
            try out.append(alloc, '\n');
        }
    }

    if (hybrid_mode) {
        try out.appendSlice(alloc, "\nFull-text indexes:\n");
        if (full_text_index_metadata.len == 0) {
            try out.appendSlice(alloc, "- <none provided>\n");
        } else {
            for (full_text_index_metadata) |index| {
                try out.appendSlice(alloc, "- ");
                try out.appendSlice(alloc, index.name);
                if (index.fields.len > 0) {
                    try out.appendSlice(alloc, " fields: ");
                    for (index.fields, 0..) |field, i| {
                        if (i > 0) try out.appendSlice(alloc, ", ");
                        try out.appendSlice(alloc, field);
                    }
                }
                try out.append(alloc, '\n');
            }
        }
    }

    if (example_documents.len > 0) {
        try out.appendSlice(alloc, "\nExample documents:\n");
        for (example_documents[0..@min(example_documents.len, 3)], 0..) |document, i| {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})});
            defer alloc.free(encoded);
            try out.print(alloc, "{d}. {s}\n", .{ i + 1, encoded });
        }
    }

    try out.appendSlice(alloc,
        \\
        \\Return a JSON object with semantic_search, indexes, full_text_search, explanation, confidence, and warnings. Do not include markdown fences.
        \\
    );
    return try out.toOwnedSlice(alloc);
}

fn buildBleveQueryBuilderMessages(
    alloc: std.mem.Allocator,
    intent: []const u8,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    example_documents: ?[]const std.json.Value,
) ![]const generating.ChatMessage {
    const system = try buildBleveQueryBuilderSystemPrompt(alloc, fields, full_text_index_metadata, example_documents orelse &.{});
    const user = try std.fmt.allocPrint(
        alloc,
        \\User's search intent: "{s}"
        \\
        \\Generate a search query in NATIVE BLEVE FORMAT that fulfills this intent.
        \\Return your response in JSON format:
        \\{{
        \\  "query": {{ ... your native Bleve query ... }},
        \\  "explanation": "brief explanation of the query",
        \\  "confidence": 0.0,
        \\  "warnings": []
        \\}}
        \\
        \\Return ONLY the JSON object, no additional text.
    ,
        .{intent},
    );
    return try alloc.dupe(generating.ChatMessage, &[_]generating.ChatMessage{
        .{ .role = .system, .content = .{ .text = system } },
        .{ .role = .user, .content = .{ .text = user } },
    });
}

fn buildBleveQueryBuilderRepairMessages(
    alloc: std.mem.Allocator,
    messages: []const generating.ChatMessage,
    feedback: ?[]const u8,
) ![]const generating.ChatMessage {
    const base_prompt =
        \\The previous native Bleve query response failed validation. Regenerate a valid response.
        \\
        \\Requirements:
        \\- Return only the corrected JSON object.
        \\- query must be a non-empty native Bleve query object.
        \\- Use only fields listed in the schema context or explicitly allowed constraints.
        \\- Use supported Bleve shapes such as match, match_phrase, term, prefix, wildcard, regexp, range, conjuncts, disjuncts, and must_not.
        \\- Do not invent fields, indexes, operators, or wrapper objects outside the requested response format.
    ;
    const repair_prompt = if (feedback) |value|
        try std.fmt.allocPrint(
            alloc,
            \\{s}
            \\
            \\Query-builder plan validation feedback:
            \\{s}
        ,
            .{ base_prompt, value },
        )
    else
        base_prompt;
    const out = try alloc.alloc(generating.ChatMessage, messages.len + 1);
    @memcpy(out[0..messages.len], messages);
    out[messages.len] = .{ .role = .user, .content = .{ .text = repair_prompt } };
    return out;
}

fn buildBleveQueryBuilderSystemPrompt(
    alloc: std.mem.Allocator,
    fields: []const []const u8,
    full_text_index_metadata: []const QueryBuilderFullTextIndex,
    example_documents: []const std.json.Value,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc,
        \\You are an expert Antfly query builder. Translate natural-language search intent into native Bleve query JSON.
        \\
        \\Use raw Bleve query objects. Common forms:
        \\- Full text: {"match":"search terms","field":"content"}
        \\- Phrase: {"match_phrase":"exact phrase","field":"title"}
        \\- Exact value: {"term":"published","field":"status"}
        \\- Prefix/wildcard/regexp: {"prefix":"mach","field":"title"}, {"wildcard":"mach*","field":"title"}, {"regexp":"^mach.*","field":"title"}
        \\- Boolean: {"conjuncts":[...]}, {"disjuncts":[...]}, {"must_not":{...}}
        \\- Ranges: {"field":"published_at","start":"2025-01-01","end":"2025-12-31","inclusive_end":true}
        \\
        \\Prefer fields from the schema context. Use a simple query string object only when field context is not enough:
        \\{"query":"body:raft AND status:published"}
        \\
    );

    try out.appendSlice(alloc, "\nSchema fields:\n");
    if (fields.len == 0) {
        try out.appendSlice(alloc, "- <none provided>\n");
    } else {
        for (fields) |field| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, field);
            try out.append(alloc, '\n');
        }
    }

    try out.appendSlice(alloc, "\nFull-text indexes:\n");
    if (full_text_index_metadata.len == 0) {
        try out.appendSlice(alloc, "- <none provided>\n");
    } else {
        for (full_text_index_metadata) |index| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, index.name);
            if (index.fields.len > 0) {
                try out.appendSlice(alloc, " fields: ");
                for (index.fields, 0..) |field, i| {
                    if (i > 0) try out.appendSlice(alloc, ", ");
                    try out.appendSlice(alloc, field);
                }
            }
            try out.append(alloc, '\n');
        }
    }

    if (example_documents.len > 0) {
        try out.appendSlice(alloc, "\nExample documents:\n");
        for (example_documents[0..@min(example_documents.len, 3)], 0..) |document, i| {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})});
            defer alloc.free(encoded);
            try out.print(alloc, "{d}. {s}\n", .{ i + 1, encoded });
        }
    }

    try out.appendSlice(alloc,
        \\
        \\Return a JSON object with query, explanation, confidence, and warnings. Do not include markdown fences.
        \\
    );
    return try out.toOwnedSlice(alloc);
}

fn buildGraphQueryBuilderMessages(
    alloc: std.mem.Allocator,
    intent: []const u8,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
    graph_index_metadata: []const QueryBuilderGraphIndex,
    example_documents: ?[]const std.json.Value,
) ![]const generating.ChatMessage {
    const system = try buildGraphQueryBuilderSystemPrompt(alloc, fields, graph_indexes, graph_index_metadata, example_documents orelse &.{});
    const user = try std.fmt.allocPrint(
        alloc,
        \\User's graph retrieval intent: "{s}"
        \\
        \\Generate Antfly graph_searches JSON that fulfills this intent.
        \\Return your response in JSON format:
        \\{{
        \\  "graph_searches": {{
        \\    "graph_search": {{ ... GraphQuery ... }}
        \\  }},
        \\  "explanation": "brief explanation of the graph plan",
        \\  "confidence": 0.0,
        \\  "warnings": []
        \\}}
        \\
        \\Return ONLY the JSON object, no additional text.
    ,
        .{intent},
    );
    return try alloc.dupe(generating.ChatMessage, &[_]generating.ChatMessage{
        .{ .role = .system, .content = .{ .text = system } },
        .{ .role = .user, .content = .{ .text = user } },
    });
}

fn buildGraphQueryBuilderSystemPrompt(
    alloc: std.mem.Allocator,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
    graph_index_metadata: []const QueryBuilderGraphIndex,
    example_documents: []const std.json.Value,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc,
        \\You are an expert Antfly graph query builder. Translate natural-language graph retrieval intent into Antfly graph_searches JSON.
        \\
        \\GraphQuery forms:
        \\- Neighbors: {"type":"neighbors","index_name":"graph_idx","start_nodes":{"result_ref":"$full_text_results","limit":5},"params":{"edge_types":["links"],"max_depth":1}}
        \\- Traverse: {"type":"traverse","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"params":{"edge_types":["references"],"max_depth":2,"deduplicate_nodes":true}}
        \\- Shortest path: {"type":"shortest_path","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"target_nodes":{"keys":["doc:b"]},"params":{"edge_types":["depends_on"],"max_depth":6,"include_paths":true}}
        \\- Pattern: {"type":"pattern","index_name":"graph_idx","start_nodes":{"keys":["doc:a"]},"pattern":[{"alias":"a"},{"alias":"b","edge":{"types":["links"],"direction":"out","min_hops":1,"max_hops":1}}],"return_aliases":["b"]}
        \\
        \\Always include index_name and start_nodes. Use {"result_ref":"$full_text_results"} when node keys are not explicit. Prefer named graph indexes listed below.
        \\Use fields only from the schema context. Do not invent fields or indexes.
        \\
    );

    try out.appendSlice(alloc, "\nGraph indexes:\n");
    if (graph_index_metadata.len > 0) {
        for (graph_index_metadata) |index| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, index.name);
            try out.append(alloc, '\n');
            if (index.edge_types.len > 0) {
                try out.appendSlice(alloc, "  edge_types:");
                for (index.edge_types, 0..) |edge_type, i| {
                    if (i == 0) {
                        try out.append(alloc, ' ');
                    } else {
                        try out.appendSlice(alloc, ", ");
                    }
                    try out.appendSlice(alloc, edge_type.name);
                    if (edge_type.topology) |topology| {
                        try out.appendSlice(alloc, " (");
                        try out.appendSlice(alloc, topology);
                        try out.append(alloc, ')');
                    }
                }
                try out.append(alloc, '\n');
            }
        }
    } else if (graph_indexes.len == 0) {
        try out.appendSlice(alloc, "- <none provided; use constraints.graph_index only if present in intent>\n");
    } else {
        for (graph_indexes) |index| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, index);
            try out.append(alloc, '\n');
        }
    }

    try out.appendSlice(alloc, "\nDocument fields:\n");
    if (fields.len == 0) {
        try out.appendSlice(alloc, "- <none provided>\n");
    } else {
        for (fields) |field| {
            try out.appendSlice(alloc, "- ");
            try out.appendSlice(alloc, field);
            try out.append(alloc, '\n');
        }
    }

    if (example_documents.len > 0) {
        try out.appendSlice(alloc, "\nExample documents:\n");
        for (example_documents[0..@min(example_documents.len, 3)], 0..) |document, i| {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})});
            defer alloc.free(encoded);
            try out.print(alloc, "{d}. {s}\n", .{ i + 1, encoded });
        }
    }

    try out.appendSlice(alloc,
        \\
        \\Return a JSON object with graph_searches, explanation, confidence, and warnings. Do not include markdown fences.
        \\
    );
    return try out.toOwnedSlice(alloc);
}

fn extractJsonObjectSlice(text: []const u8) ?[]const u8 {
    const start = std.mem.indexOfScalar(u8, text, '{') orelse return null;
    const end = std.mem.lastIndexOfScalar(u8, text, '}') orelse return null;
    if (end <= start) return null;
    return text[start .. end + 1];
}

fn normalizeQueryBuilderConfidence(value: ?f64) ?f64 {
    const confidence = value orelse return null;
    if (confidence < 0 or confidence > 1) return 0.5;
    return confidence;
}

fn cloneStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, values.len);
    for (values, 0..) |value, i| out[i] = try alloc.dupe(u8, value);
    return out;
}

fn cloneJsonValueLeaky(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, encoded, .{});
}

fn cloneGraphSearchesLeaky(
    alloc: std.mem.Allocator,
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
) !std.json.ArrayHashMap(indexes_openapi.GraphQuery) {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(graph_searches, .{})});
    return try std.json.parseFromSliceLeaky(std.json.ArrayHashMap(indexes_openapi.GraphQuery), alloc, encoded, .{
        .ignore_unknown_fields = true,
    });
}

const QueryBuilderValidationError = error{InvalidQueryBuilderGeneration};

fn validateGeneratedQueryBuilderGraphSearches(
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
) QueryBuilderValidationError!void {
    if (graph_searches.map.count() == 0) return error.InvalidQueryBuilderGeneration;
    var it = graph_searches.map.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.*.len == 0) return error.InvalidQueryBuilderGeneration;
        try validateGeneratedQueryBuilderGraphQuery(request, fields, graph_indexes, entry.value_ptr.*);
    }
    try validateGeneratedGraphResultDependencies(graph_searches);
}

fn validateGeneratedQueryBuilderGraphQuery(
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
    query: indexes_openapi.GraphQuery,
) QueryBuilderValidationError!void {
    if (query.index_name.len == 0) return error.InvalidQueryBuilderGeneration;
    if (graph_indexes.len > 0 and !queryBuilderFieldInSlice(graph_indexes, query.index_name)) return error.InvalidQueryBuilderGeneration;
    if (query.include_edges == true) return error.InvalidQueryBuilderGeneration;
    try validateGeneratedGraphNodeSelector(query.start_nodes orelse return error.InvalidQueryBuilderGeneration);
    if (query.target_nodes) |target_nodes| try validateGeneratedGraphNodeSelector(target_nodes);
    if (query.params) |params| try validateGeneratedGraphQueryParams(query.type, params);
    if (query.fields) |query_fields| {
        for (query_fields) |field| {
            if (!queryBuilderFieldAllowed(request.constraints, fields, field)) return error.InvalidQueryBuilderGeneration;
        }
    }
    switch (query.type) {
        .pattern => {
            if (query.target_nodes != null) return error.InvalidQueryBuilderGeneration;
            const pattern = query.pattern orelse return error.InvalidQueryBuilderGeneration;
            try validateGeneratedGraphPattern(pattern, query.return_aliases);
        },
        .shortest_path, .k_shortest_paths => {
            if (query.target_nodes == null) return error.InvalidQueryBuilderGeneration;
            if (query.pattern != null or query.return_aliases != null) return error.InvalidQueryBuilderGeneration;
        },
        else => {
            if (query.pattern != null or query.return_aliases != null) return error.InvalidQueryBuilderGeneration;
        },
    }
}

fn validateGeneratedGraphNodeSelector(selector: indexes_openapi.GraphNodeSelector) QueryBuilderValidationError!void {
    const has_keys = selector.keys != null and selector.keys.?.len > 0;
    const has_ref = selector.result_ref != null and selector.result_ref.?.len > 0;
    if (selector.node_filter != null) return error.InvalidQueryBuilderGeneration;
    if (selector.limit) |limit| {
        if (limit <= 0) return error.InvalidQueryBuilderGeneration;
    }
    if (has_keys) {
        for (selector.keys.?) |key| {
            if (key.len == 0) return error.InvalidQueryBuilderGeneration;
        }
    }
    if (has_ref and !isAllowedGeneratedGraphResultRef(selector.result_ref.?)) return error.InvalidQueryBuilderGeneration;
    if (has_keys and has_ref) return error.InvalidQueryBuilderGeneration;
    if (!has_keys and !has_ref) return error.InvalidQueryBuilderGeneration;
}

fn validateGeneratedGraphResultDependencies(
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
) QueryBuilderValidationError!void {
    const max_depth = graph_searches.map.count();
    var it = graph_searches.map.iterator();
    while (it.next()) |entry| {
        try validateGeneratedGraphSelectorDependency(graph_searches, entry.value_ptr.*.start_nodes, max_depth);
        if (entry.value_ptr.*.target_nodes) |target_nodes| {
            try validateGeneratedGraphSelectorDependency(graph_searches, target_nodes, max_depth);
        }
    }
}

fn validateGeneratedGraphSelectorDependency(
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
    selector: ?indexes_openapi.GraphNodeSelector,
    max_depth: usize,
) QueryBuilderValidationError!void {
    const result_ref = (selector orelse return).result_ref orelse return;
    const dep_name = generatedGraphResultDependencyName(result_ref) orelse return;
    try validateGeneratedGraphDependencyChain(graph_searches, dep_name, max_depth);
}

fn validateGeneratedGraphDependencyChain(
    graph_searches: std.json.ArrayHashMap(indexes_openapi.GraphQuery),
    dep_name: []const u8,
    remaining_depth: usize,
) QueryBuilderValidationError!void {
    if (remaining_depth == 0) return error.InvalidQueryBuilderGeneration;
    const query = graph_searches.map.get(dep_name) orelse return error.InvalidQueryBuilderGeneration;
    try validateGeneratedGraphSelectorDependency(graph_searches, query.start_nodes, remaining_depth - 1);
    if (query.target_nodes) |target_nodes| {
        try validateGeneratedGraphSelectorDependency(graph_searches, target_nodes, remaining_depth - 1);
    }
}

fn validateGeneratedGraphQueryParams(
    query_type: indexes_openapi.GraphQueryType,
    params: indexes_openapi.GraphQueryParams,
) QueryBuilderValidationError!void {
    if (params.node_filter != null or params.algorithm != null or params.algorithm_params != null) return error.InvalidQueryBuilderGeneration;
    if (params.edge_types) |edge_types| try validateGeneratedGraphEdgeTypes(edge_types);
    if (params.max_depth) |max_depth| {
        if (max_depth <= 0) return error.InvalidQueryBuilderGeneration;
    }
    if (params.max_results) |max_results| {
        if (max_results <= 0) return error.InvalidQueryBuilderGeneration;
    }
    if (params.k) |k| {
        if (query_type != .k_shortest_paths or k <= 0) return error.InvalidQueryBuilderGeneration;
    }
    if (query_type == .pattern) {
        if (params.edge_types != null or params.direction != null or params.include_paths != null or params.weight_mode != null or params.k != null) {
            return error.InvalidQueryBuilderGeneration;
        }
    }
}

fn validateGeneratedGraphPattern(
    pattern: []const indexes_openapi.PatternStep,
    return_aliases: ?[]const []const u8,
) QueryBuilderValidationError!void {
    if (pattern.len < 2 or pattern.len > 8) return error.InvalidQueryBuilderGeneration;
    for (pattern, 0..) |step, i| {
        const alias = step.alias orelse return error.InvalidQueryBuilderGeneration;
        if (alias.len == 0) return error.InvalidQueryBuilderGeneration;
        for (pattern[0..i]) |previous| {
            if (previous.alias) |previous_alias| {
                if (std.mem.eql(u8, previous_alias, alias)) return error.InvalidQueryBuilderGeneration;
            }
        }
        if (step.node_filter != null) return error.InvalidQueryBuilderGeneration;
        if (i == 0) {
            if (step.edge != null) return error.InvalidQueryBuilderGeneration;
        } else {
            const edge = step.edge orelse return error.InvalidQueryBuilderGeneration;
            try validateGeneratedGraphPatternEdge(edge);
        }
    }
    if (return_aliases) |aliases| {
        if (aliases.len == 0) return error.InvalidQueryBuilderGeneration;
        for (aliases) |alias| {
            if (!generatedGraphPatternHasAlias(pattern, alias)) return error.InvalidQueryBuilderGeneration;
        }
    }
}

fn validateGeneratedGraphPatternEdge(edge: indexes_openapi.PatternEdgeStep) QueryBuilderValidationError!void {
    if (edge.types) |types| try validateGeneratedGraphEdgeTypes(types);
    if (edge.min_hops) |min_hops| {
        if (min_hops <= 0) return error.InvalidQueryBuilderGeneration;
    }
    if (edge.max_hops) |max_hops| {
        if (max_hops <= 0) return error.InvalidQueryBuilderGeneration;
        if (edge.min_hops) |min_hops| {
            if (max_hops < min_hops) return error.InvalidQueryBuilderGeneration;
        }
    }
}

fn validateGeneratedGraphEdgeTypes(edge_types: []const []const u8) QueryBuilderValidationError!void {
    if (edge_types.len == 0) return error.InvalidQueryBuilderGeneration;
    for (edge_types) |edge_type| {
        if (edge_type.len == 0) return error.InvalidQueryBuilderGeneration;
    }
}

fn generatedGraphPatternHasAlias(pattern: []const indexes_openapi.PatternStep, alias: []const u8) bool {
    for (pattern) |step| {
        if (step.alias) |candidate| {
            if (std.mem.eql(u8, candidate, alias)) return true;
        }
    }
    return false;
}

fn isAllowedGeneratedGraphResultRef(result_ref: []const u8) bool {
    return std.mem.eql(u8, result_ref, "$full_text_results") or
        std.mem.eql(u8, result_ref, "$fused_results") or
        std.mem.eql(u8, result_ref, "$embeddings_results") or
        generatedGraphResultDependencyName(result_ref) != null;
}

fn generatedGraphResultDependencyName(result_ref: []const u8) ?[]const u8 {
    const prefix = "$graph_results.";
    if (!std.mem.startsWith(u8, result_ref, prefix)) return null;
    const name = result_ref[prefix.len..];
    return if (name.len > 0) name else null;
}

fn validateGeneratedQueryBuilderBleveQuery(
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    query: std.json.Value,
) QueryBuilderValidationError!void {
    if (query != .object) return error.InvalidQueryBuilderGeneration;
    try validateGeneratedQueryBuilderBleveObject(request.constraints, fields, query.object, 0);
}

fn validateGeneratedQueryBuilderBleveObject(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    object: std.json.ObjectMap,
    depth: usize,
) QueryBuilderValidationError!void {
    if (depth > 8) return error.InvalidQueryBuilderGeneration;
    if (object.count() == 0) return error.InvalidQueryBuilderGeneration;

    if (isQueryBuilderRangeObject(object)) {
        try validateGeneratedQueryBuilderRangeObject(constraints, fields, object);
        return;
    }

    if (object.get("field")) |field_value| {
        const field = queryBuilderStringValue(field_value) orelse return error.InvalidQueryBuilderGeneration;
        if (!queryBuilderFieldAllowed(constraints, fields, field)) return error.InvalidQueryBuilderGeneration;
    }

    var saw_query_operator = false;
    var it = object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;

        if (isQueryBuilderScalarOptionKey(key)) continue;

        if (std.mem.eql(u8, key, "query")) {
            if (value != .string) return error.InvalidQueryBuilderGeneration;
            saw_query_operator = true;
            continue;
        }

        if (isQueryBuilderTextOperatorKey(key)) {
            try validateGeneratedQueryBuilderTextOperator(constraints, fields, key, value, depth + 1);
            saw_query_operator = true;
            continue;
        }

        if (std.mem.eql(u8, key, "conjuncts") or std.mem.eql(u8, key, "disjuncts")) {
            try validateGeneratedQueryBuilderQueryArray(constraints, fields, value, depth + 1);
            saw_query_operator = true;
            continue;
        }

        if (std.mem.eql(u8, key, "must_not") or std.mem.eql(u8, key, "filter") or std.mem.eql(u8, key, "must") or std.mem.eql(u8, key, "should")) {
            try validateGeneratedQueryBuilderQueryOrArray(constraints, fields, value, depth + 1);
            saw_query_operator = true;
            continue;
        }

        if (std.mem.eql(u8, key, "bool") or std.mem.eql(u8, key, "boolean")) {
            if (value != .object) return error.InvalidQueryBuilderGeneration;
            try validateGeneratedQueryBuilderBooleanObject(constraints, fields, value.object, depth + 1);
            saw_query_operator = true;
            continue;
        }

        if (std.mem.eql(u8, key, "match_all") or std.mem.eql(u8, key, "match_none")) {
            if (value != .object and value != .bool and value != .null) return error.InvalidQueryBuilderGeneration;
            saw_query_operator = true;
            continue;
        }

        if (std.mem.eql(u8, key, "docids") or std.mem.eql(u8, key, "doc_ids") or std.mem.eql(u8, key, "ids")) {
            try validateQueryBuilderStringArray(value);
            saw_query_operator = true;
            continue;
        }

        return error.InvalidQueryBuilderGeneration;
    }

    if (!saw_query_operator) return error.InvalidQueryBuilderGeneration;
}

fn validateGeneratedQueryBuilderRangeObject(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    object: std.json.ObjectMap,
) QueryBuilderValidationError!void {
    const field = queryBuilderStringValue(object.get("field") orelse return error.InvalidQueryBuilderGeneration) orelse return error.InvalidQueryBuilderGeneration;
    if (!queryBuilderFieldAllowed(constraints, fields, field)) return error.InvalidQueryBuilderGeneration;

    var saw_bound = false;
    var it = object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "field")) continue;
        if (isQueryBuilderRangeKey(key)) {
            if (value == .object or value == .array) return error.InvalidQueryBuilderGeneration;
            saw_bound = true;
            continue;
        }
        if (isQueryBuilderScalarOptionKey(key)) {
            if (value == .object or value == .array) return error.InvalidQueryBuilderGeneration;
            continue;
        }
        return error.InvalidQueryBuilderGeneration;
    }
    if (!saw_bound) return error.InvalidQueryBuilderGeneration;
}

fn validateGeneratedQueryBuilderBooleanObject(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    object: std.json.ObjectMap,
    depth: usize,
) QueryBuilderValidationError!void {
    if (object.count() == 0) return error.InvalidQueryBuilderGeneration;
    var saw_clause = false;
    var it = object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "must") or
            std.mem.eql(u8, key, "should") or
            std.mem.eql(u8, key, "must_not") or
            std.mem.eql(u8, key, "filter"))
        {
            try validateGeneratedQueryBuilderQueryOrArray(constraints, fields, entry.value_ptr.*, depth + 1);
            saw_clause = true;
        } else if (std.mem.eql(u8, key, "min") or std.mem.eql(u8, key, "min_should") or std.mem.eql(u8, key, "boost")) {
            if (!isQueryBuilderNumber(entry.value_ptr.*)) return error.InvalidQueryBuilderGeneration;
        } else {
            return error.InvalidQueryBuilderGeneration;
        }
    }
    if (!saw_clause) return error.InvalidQueryBuilderGeneration;
}

fn validateGeneratedQueryBuilderTextOperator(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    key: []const u8,
    value: std.json.Value,
    depth: usize,
) QueryBuilderValidationError!void {
    _ = key;
    switch (value) {
        .string, .integer, .float, .number_string, .bool => return,
        .object => |object| {
            if (object.get("field")) |field_value| {
                const field = queryBuilderStringValue(field_value) orelse return error.InvalidQueryBuilderGeneration;
                if (!queryBuilderFieldAllowed(constraints, fields, field)) return error.InvalidQueryBuilderGeneration;
                try validateGeneratedQueryBuilderOperatorOptions(constraints, fields, object, depth + 1);
                return;
            }
            if (object.count() == 1) {
                var it = object.iterator();
                const entry = it.next() orelse return error.InvalidQueryBuilderGeneration;
                if (!queryBuilderFieldAllowed(constraints, fields, entry.key_ptr.*)) return error.InvalidQueryBuilderGeneration;
                return;
            }
            return error.InvalidQueryBuilderGeneration;
        },
        else => return error.InvalidQueryBuilderGeneration,
    }
}

fn validateGeneratedQueryBuilderOperatorOptions(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    object: std.json.ObjectMap,
    depth: usize,
) QueryBuilderValidationError!void {
    if (depth > 8) return error.InvalidQueryBuilderGeneration;
    var it = object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "field")) {
            const field = queryBuilderStringValue(value) orelse return error.InvalidQueryBuilderGeneration;
            if (!queryBuilderFieldAllowed(constraints, fields, field)) return error.InvalidQueryBuilderGeneration;
        } else if (isQueryBuilderScalarOptionKey(key) or isQueryBuilderTextPayloadKey(key)) {
            if (value == .object or value == .array) return error.InvalidQueryBuilderGeneration;
        } else if (isQueryBuilderRangeKey(key)) {
            if (value == .object or value == .array) return error.InvalidQueryBuilderGeneration;
        } else {
            return error.InvalidQueryBuilderGeneration;
        }
    }
}

fn validateGeneratedQueryBuilderQueryOrArray(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    value: std.json.Value,
    depth: usize,
) QueryBuilderValidationError!void {
    if (value == .array) return try validateGeneratedQueryBuilderQueryArray(constraints, fields, value, depth);
    if (value != .object) return error.InvalidQueryBuilderGeneration;
    try validateGeneratedQueryBuilderBleveObject(constraints, fields, value.object, depth + 1);
}

fn validateGeneratedQueryBuilderQueryArray(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    value: std.json.Value,
    depth: usize,
) QueryBuilderValidationError!void {
    if (value != .array) return error.InvalidQueryBuilderGeneration;
    if (value.array.items.len == 0) return error.InvalidQueryBuilderGeneration;
    for (value.array.items) |item| {
        if (item != .object) return error.InvalidQueryBuilderGeneration;
        try validateGeneratedQueryBuilderBleveObject(constraints, fields, item.object, depth + 1);
    }
}

fn validateQueryBuilderStringArray(value: std.json.Value) QueryBuilderValidationError!void {
    if (value != .array) return error.InvalidQueryBuilderGeneration;
    for (value.array.items) |item| {
        if (item != .string) return error.InvalidQueryBuilderGeneration;
    }
}

fn queryBuilderFieldAllowed(
    constraints: ?std.json.Value,
    fields: []const []const u8,
    field: []const u8,
) bool {
    if (field.len == 0) return false;
    if (queryBuilderConstraintArray(constraints, "allowed_fields")) |allowed| {
        return queryBuilderFieldInList(allowed, field);
    }
    if (fields.len == 0) return true;
    return queryBuilderFieldInSlice(fields, field);
}

fn queryBuilderConstraintArray(constraints: ?std.json.Value, key: []const u8) ?std.json.Array {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const entry = value.object.get(key) orelse return null;
    if (entry != .array or entry.array.items.len == 0) return null;
    return entry.array;
}

fn queryBuilderSelectableFields(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
) ![]const []const u8 {
    const allowed = queryBuilderConstraintArray(constraints, "allowed_fields") orelse return fields;

    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);
    if (fields.len == 0) {
        for (allowed.items) |value| {
            const field = queryBuilderStringValue(value) orelse continue;
            if (field.len > 0) try out.append(alloc, field);
        }
    } else {
        for (fields) |field| {
            if (queryBuilderFieldInList(allowed, field)) try out.append(alloc, field);
        }
    }
    if (out.items.len == 0) return &.{};
    return try out.toOwnedSlice(alloc);
}

fn queryBuilderFieldInList(values: std.json.Array, field: []const u8) bool {
    for (values.items) |value| {
        const candidate = queryBuilderStringValue(value) orelse continue;
        if (queryBuilderFieldMatches(candidate, field)) return true;
    }
    return false;
}

fn queryBuilderFieldInSlice(values: []const []const u8, field: []const u8) bool {
    for (values) |candidate| {
        if (queryBuilderFieldMatches(candidate, field)) return true;
    }
    return false;
}

fn queryBuilderFieldMatches(candidate: []const u8, field: []const u8) bool {
    if (std.mem.eql(u8, candidate, field)) return true;
    if (std.mem.startsWith(u8, field, candidate) and field.len > candidate.len and field[candidate.len] == '.') return true;
    if (std.mem.startsWith(u8, candidate, field) and candidate.len > field.len and candidate[field.len] == '.') return true;
    return false;
}

fn queryBuilderStringValue(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |string| string,
        else => null,
    };
}

fn isQueryBuilderTextOperatorKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "match") or
        std.mem.eql(u8, key, "match_phrase") or
        std.mem.eql(u8, key, "term") or
        std.mem.eql(u8, key, "prefix") or
        std.mem.eql(u8, key, "wildcard") or
        std.mem.eql(u8, key, "regexp") or
        std.mem.eql(u8, key, "fuzzy");
}

fn isQueryBuilderTextPayloadKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "text") or
        std.mem.eql(u8, key, "match") or
        std.mem.eql(u8, key, "match_phrase") or
        std.mem.eql(u8, key, "term") or
        std.mem.eql(u8, key, "prefix") or
        std.mem.eql(u8, key, "wildcard") or
        std.mem.eql(u8, key, "regexp");
}

fn isQueryBuilderScalarOptionKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "field") or
        std.mem.eql(u8, key, "boost") or
        std.mem.eql(u8, key, "analyzer") or
        std.mem.eql(u8, key, "operator") or
        std.mem.eql(u8, key, "fuzziness") or
        std.mem.eql(u8, key, "prefix_length") or
        std.mem.eql(u8, key, "inclusive_min") or
        std.mem.eql(u8, key, "inclusive_max") or
        std.mem.eql(u8, key, "inclusive_start") or
        std.mem.eql(u8, key, "inclusive_end") or
        std.mem.eql(u8, key, "datetime_parser");
}

fn isQueryBuilderRangeKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "min") or
        std.mem.eql(u8, key, "max") or
        std.mem.eql(u8, key, "start") or
        std.mem.eql(u8, key, "end");
}

fn isQueryBuilderRangeObject(object: std.json.ObjectMap) bool {
    if (object.get("field") == null) return false;
    return object.get("min") != null or
        object.get("max") != null or
        object.get("start") != null or
        object.get("end") != null;
}

fn isQueryBuilderNumber(value: std.json.Value) bool {
    return value == .integer or value == .float or value == .number_string;
}

fn buildQueryBuilderQuery(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    intent: []const u8,
    fields: []const []const u8,
    warnings: *std.ArrayListUnmanaged([]const u8),
) !BuiltQueryBuilderQuery {
    const selectable_fields = try queryBuilderSelectableFields(alloc, request.constraints, fields);
    const status_field = pickQueryBuilderStatusField(selectable_fields);
    const status_value = detectQueryBuilderStatusValue(intent);
    const term_filters = try detectQueryBuilderTermFilters(alloc, intent, selectable_fields, status_field, status_value);
    const date_filter = detectQueryBuilderDateFilter(intent);
    const date_field = if (date_filter != null) pickQueryBuilderDateField(selectable_fields) else null;
    const temporal_hint = (detectTemporalHint(intent) or date_filter != null) and !(date_filter != null and date_field != null);
    const preferred_text_field = queryBuilderPreferredTextField(request, selectable_fields);
    const text_field = pickQueryBuilderTextField(selectable_fields, preferred_text_field);
    const search_text = trimQueryBuilderIntent(intent);

    if (status_field != null and status_value != null and search_text.len == 0) {
        const query_json = try std.fmt.allocPrint(
            alloc,
            "{{\"term\":{f},\"field\":{f}}}",
            .{ std.json.fmt(status_value.?, .{}), std.json.fmt(status_field.?, .{}) },
        );
        return .{
            .query = try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{}),
            .temporal_hint = temporal_hint,
            .query_kind = .status_only,
            .status_field = status_field,
            .status_value = status_value,
            .date_field = date_field,
            .date_start = if (date_filter) |filter| filter.start else null,
            .date_end = if (date_filter) |filter| filter.end else null,
            .date_inclusive_start = if (date_filter) |filter| filter.inclusive_start else false,
            .date_inclusive_end = if (date_filter) |filter| filter.inclusive_end else false,
            .term_filters = term_filters,
        };
    }

    if (text_field) |field| {
        if (status_field) |status_field_value| {
            if (status_value) |status| {
                const query_json = try std.fmt.allocPrint(
                    alloc,
                    "{{\"conjuncts\":[{{\"match\":{f},\"field\":{f}}},{{\"term\":{f},\"field\":{f}}}]}}",
                    .{
                        std.json.fmt(search_text, .{}),
                        std.json.fmt(field, .{}),
                        std.json.fmt(status, .{}),
                        std.json.fmt(status_field_value, .{}),
                    },
                );
                return .{
                    .query = try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{}),
                    .temporal_hint = temporal_hint,
                    .query_kind = .conjunction,
                    .text_field = field,
                    .text_value = search_text,
                    .status_field = status_field_value,
                    .status_value = status,
                    .date_field = date_field,
                    .date_start = if (date_filter) |filter| filter.start else null,
                    .date_end = if (date_filter) |filter| filter.end else null,
                    .date_inclusive_start = if (date_filter) |filter| filter.inclusive_start else false,
                    .date_inclusive_end = if (date_filter) |filter| filter.inclusive_end else false,
                    .term_filters = term_filters,
                };
            }
        }

        const query_json = try std.fmt.allocPrint(
            alloc,
            "{{\"match\":{f},\"field\":{f}}}",
            .{ std.json.fmt(search_text, .{}), std.json.fmt(field, .{}) },
        );
        return .{
            .query = try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{}),
            .temporal_hint = temporal_hint,
            .query_kind = .field_match,
            .text_field = field,
            .text_value = search_text,
            .status_field = status_field,
            .status_value = status_value,
            .date_field = date_field,
            .date_start = if (date_filter) |filter| filter.start else null,
            .date_end = if (date_filter) |filter| filter.end else null,
            .date_inclusive_start = if (date_filter) |filter| filter.inclusive_start else false,
            .date_inclusive_end = if (date_filter) |filter| filter.inclusive_end else false,
            .term_filters = term_filters,
        };
    }

    try warnings.append(alloc, "No preferred searchable field was identified, so the query builder emitted a generic Bleve query string.");
    const query_json = try std.fmt.allocPrint(
        alloc,
        "{{\"query\":{f}}}",
        .{std.json.fmt(search_text, .{})},
    );
    return .{
        .query = try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{}),
        .temporal_hint = temporal_hint,
        .query_kind = .generic,
        .text_value = search_text,
        .status_field = status_field,
        .status_value = status_value,
        .date_field = date_field,
        .date_start = if (date_filter) |filter| filter.start else null,
        .date_end = if (date_filter) |filter| filter.end else null,
        .date_inclusive_start = if (date_filter) |filter| filter.inclusive_start else false,
        .date_inclusive_end = if (date_filter) |filter| filter.inclusive_end else false,
        .term_filters = term_filters,
    };
}

fn buildQueryBuilderQueryRequest(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    built: BuiltQueryBuilderQuery,
    fields: []const []const u8,
    graph_indexes: []const []const u8,
) !metadata_openapi.QueryRequest {
    var out = metadata_openapi.QueryRequest{
        .table = queryBuilderEffectiveTable(request),
        .limit = queryBuilderConstraintLimit(request.constraints),
        .fields = try queryBuilderConstraintFieldSlice(alloc, request.constraints, fields, "fields"),
        .count = queryBuilderConstraintOptionalBool(request.constraints, "count"),
        .profile = queryBuilderConstraintOptionalBool(request.constraints, "profile"),
    };
    var filter_query = try buildQueryBuilderFilterQueryValue(alloc, built);
    filter_query = try combineQueryBuilderFilterQueries(
        alloc,
        filter_query,
        try buildQueryBuilderConstraintFilterQueryValue(alloc, request.constraints, fields),
    );
    out.filter_prefix = queryBuilderConstraintString(request.constraints, "filter_prefix");
    out.exclusion_query = try buildQueryBuilderConstraintExclusionQueryValue(alloc, request.constraints, fields);
    out.graph_searches = try queryBuilderGraphSearches(alloc, request, built, graph_indexes);
    out.expand_strategy = queryBuilderConstraintString(request.constraints, "expand_strategy");

    switch (built.query_kind) {
        .status_only => {
            out.filter_query = filter_query orelse built.query;
        },
        .conjunction => {
            out.full_text_search = try buildQueryBuilderMatchQueryValue(alloc, built.text_value.?, built.text_field.?);
            out.filter_query = filter_query;
        },
        .field_match, .generic, .llm_full_text => {
            out.full_text_search = built.query;
            out.filter_query = filter_query;
        },
        .semantic => {
            out.semantic_search = built.text_value;
            out.indexes = built.indexes;
            out.filter_query = filter_query;
        },
        .hybrid => {
            out.semantic_search = built.text_value;
            out.indexes = built.indexes;
            out.filter_query = filter_query;
            if (built.text_field != null and built.text_value != null) {
                out.full_text_search = try buildQueryBuilderMatchQueryValue(alloc, built.text_value.?, built.text_field.?);
            } else if (built.status_field == null or built.status_value == null) {
                out.full_text_search = built.query;
            }
        },
    }

    if (out.full_text_search != null) {
        out.order_by = try queryBuilderConstraintSortFields(alloc, request.constraints, fields);
        if (out.order_by != null) {
            const search_after = try queryBuilderConstraintStringSlice(alloc, request.constraints, "search_after");
            const search_before = try queryBuilderConstraintStringSlice(alloc, request.constraints, "search_before");
            if (search_after.len > 0) {
                out.search_after = search_after;
            } else if (search_before.len > 0) {
                out.search_before = search_before;
            } else {
                out.offset = queryBuilderConstraintInteger(request.constraints, "offset");
            }
        } else {
            out.offset = queryBuilderConstraintInteger(request.constraints, "offset");
        }
    }

    return out;
}

fn buildQueryBuilderRetrievalQueryRequest(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    query_request: metadata_openapi.QueryRequest,
    graph_indexes: []const []const u8,
) !?metadata_openapi.RetrievalQueryRequest {
    const tree_search = try queryBuilderConstraintTreeSearch(alloc, request, graph_indexes) orelse return null;
    const encoded = try std.json.Stringify.valueAlloc(alloc, query_request, .{});
    var out = try std.json.parseFromSliceLeaky(metadata_openapi.RetrievalQueryRequest, alloc, encoded, .{
        .ignore_unknown_fields = true,
    });
    out.tree_search = tree_search;
    return out;
}

fn queryBuilderConstraintTreeSearch(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    graph_indexes: []const []const u8,
) !?metadata_openapi.TreeSearchConfig {
    const constraints = request.constraints;
    if (constraints) |value| {
        if (value == .object) {
            if (value.object.get("tree_search")) |raw| {
                if (try queryBuilderTreeSearchFromValue(alloc, value, raw)) |tree_search| return tree_search;
            }
        }
    }
    const index = queryBuilderTreeIndex(request, graph_indexes) orelse return null;
    const trimmed_index = std.mem.trim(u8, index, " \t\r\n");
    if (trimmed_index.len == 0) return null;
    return .{
        .index = try alloc.dupe(u8, trimmed_index),
        .start_nodes = try queryBuilderTreeStartNodes(alloc, request, constraints),
        .max_depth = queryBuilderConstraintInteger(constraints, "tree_max_depth") orelse
            queryBuilderConstraintInteger(constraints, "max_depth"),
        .beam_width = queryBuilderConstraintInteger(constraints, "tree_beam_width") orelse
            queryBuilderConstraintInteger(constraints, "beam_width"),
    };
}

fn queryBuilderTreeStartNodes(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    constraints: ?std.json.Value,
) !?[]const u8 {
    if (try queryBuilderConstraintFirstStringDup(alloc, constraints, &.{ "tree_start_nodes", "start_nodes" })) |start_nodes| return start_nodes;
    const start_node = queryBuilderInferredIntentNodeText(request.intent, .start) orelse return null;
    return try alloc.dupe(u8, start_node);
}

fn queryBuilderTreeSearchFromValue(
    alloc: std.mem.Allocator,
    constraints: std.json.Value,
    value: std.json.Value,
) !?metadata_openapi.TreeSearchConfig {
    switch (value) {
        .string => |index| {
            const trimmed = std.mem.trim(u8, index, " \t\r\n");
            if (trimmed.len == 0) return null;
            return .{ .index = try alloc.dupe(u8, trimmed) };
        },
        .object => |object| {
            const raw_index = object.get("index") orelse object.get("tree_index") orelse return null;
            const index = queryBuilderStringValue(raw_index) orelse return null;
            const trimmed = std.mem.trim(u8, index, " \t\r\n");
            if (trimmed.len == 0) return null;
            return .{
                .index = try alloc.dupe(u8, trimmed),
                .start_nodes = try queryBuilderObjectOptionalStringDup(alloc, object, "start_nodes"),
                .max_depth = queryBuilderObjectInteger(object, "max_depth") orelse
                    queryBuilderConstraintInteger(constraints, "tree_max_depth"),
                .beam_width = queryBuilderObjectInteger(object, "beam_width") orelse
                    queryBuilderConstraintInteger(constraints, "tree_beam_width"),
            };
        },
        else => return null,
    }
}

fn queryBuilderTreeIndex(
    request: metadata_openapi.QueryBuilderRequest,
    graph_indexes: []const []const u8,
) ?[]const u8 {
    if (queryBuilderDecisionString(request.decisions, "select_tree_index")) |index| return index;
    if (queryBuilderConstraintString(request.constraints, "tree_index")) |index| return index;
    if (queryBuilderConstraintString(request.constraints, "prefer_tree_index")) |index| return index;
    if (queryBuilderConstraintString(request.constraints, "tree")) |index| return index;
    const mode = queryBuilderSelectedStrategy(request) orelse request.mode orelse return null;
    if (!std.ascii.eqlIgnoreCase(mode, "tree")) return null;
    return if (graph_indexes.len == 1) graph_indexes[0] else null;
}

fn queryBuilderConstraintFirstStringDup(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    keys: []const []const u8,
) !?[]const u8 {
    for (keys) |key| {
        if (try queryBuilderConstraintOptionalStringDup(alloc, constraints, key)) |value| return value;
    }
    return null;
}

fn queryBuilderConstraintOptionalStringDup(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    key: []const u8,
) !?[]const u8 {
    const value = queryBuilderConstraintString(constraints, key) orelse return null;
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) return null;
    return try alloc.dupe(u8, trimmed);
}

fn queryBuilderObjectOptionalStringDup(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    key: []const u8,
) !?[]const u8 {
    const raw = object.get(key) orelse return null;
    const value = queryBuilderStringValue(raw) orelse return null;
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) return null;
    return try alloc.dupe(u8, trimmed);
}

fn queryBuilderObjectInteger(object: std.json.ObjectMap, key: []const u8) ?i64 {
    const raw = object.get(key) orelse return null;
    return queryBuilderIntegerValue(raw);
}

fn combineQueryBuilderFilterQueries(
    alloc: std.mem.Allocator,
    left: ?std.json.Value,
    right: ?std.json.Value,
) !?std.json.Value {
    if (left == null) return right;
    if (right == null) return left;
    const query_json = try std.fmt.allocPrint(
        alloc,
        "{{\"conjuncts\":[{f},{f}]}}",
        .{ std.json.fmt(left.?, .{}), std.json.fmt(right.?, .{}) },
    );
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{});
}

fn buildQueryBuilderConstraintFilterQueryValue(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
) !?std.json.Value {
    var out = try queryBuilderConstraintBleveQuery(alloc, constraints, fields, "filter_query");
    out = try combineQueryBuilderFilterQueries(
        alloc,
        out,
        try buildQueryBuilderConstraintFieldFiltersValue(alloc, constraints, fields, "filters"),
    );
    out = try combineQueryBuilderFilterQueries(
        alloc,
        out,
        try buildQueryBuilderConstraintFieldFiltersValue(alloc, constraints, fields, "filter"),
    );
    return out;
}

fn buildQueryBuilderConstraintExclusionQueryValue(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
) !?std.json.Value {
    var out = try queryBuilderConstraintBleveQuery(alloc, constraints, fields, "exclusion_query");
    out = try combineQueryBuilderFilterQueries(
        alloc,
        out,
        try buildQueryBuilderConstraintFieldFiltersValue(alloc, constraints, fields, "exclude"),
    );
    out = try combineQueryBuilderFilterQueries(
        alloc,
        out,
        try buildQueryBuilderConstraintFieldFiltersValue(alloc, constraints, fields, "exclusions"),
    );
    return out;
}

fn queryBuilderConstraintBleveQuery(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
    key: []const u8,
) !?std.json.Value {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const raw = value.object.get(key) orelse return null;
    switch (raw) {
        .object => |object| {
            validateGeneratedQueryBuilderBleveObject(constraints, fields, object, 0) catch return null;
            return try cloneJsonValueLeaky(alloc, raw);
        },
        .string => |query| {
            const query_json = try std.fmt.allocPrint(alloc, "{{\"query\":{f}}}", .{std.json.fmt(query, .{})});
            return try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{});
        },
        else => return null,
    }
}

fn buildQueryBuilderConstraintFieldFiltersValue(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
    key: []const u8,
) !?std.json.Value {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const raw = value.object.get(key) orelse return null;
    if (raw != .object) return null;

    var query_json = std.ArrayListUnmanaged(u8).empty;
    errdefer query_json.deinit(alloc);
    var count: usize = 0;
    var it = raw.object.iterator();
    while (it.next()) |entry| {
        const field = entry.key_ptr.*;
        if (!queryBuilderFieldAllowed(constraints, fields, field)) continue;
        try appendQueryBuilderConstraintFieldFilterJson(alloc, &query_json, field, entry.value_ptr.*, &count);
    }
    if (count == 0) return null;
    if (count == 1) return try std.json.parseFromSliceLeaky(std.json.Value, alloc, try query_json.toOwnedSlice(alloc), .{});

    const wrapped_json = try std.fmt.allocPrint(alloc, "{{\"conjuncts\":[{s}]}}", .{query_json.items});
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, wrapped_json, .{});
}

fn appendQueryBuilderConstraintFieldFilterJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    field: []const u8,
    value: std.json.Value,
    count: *usize,
) !void {
    switch (value) {
        .array => |items| try appendQueryBuilderConstraintArrayFilterJson(alloc, out, field, items.items, count),
        .object => |object| {
            if (queryBuilderConstraintRangeObjectHasBounds(object)) {
                try appendQueryBuilderConstraintRangeFilterJson(alloc, out, field, object, count);
            } else if (object.get("value") orelse object.get("term")) |term| {
                try appendQueryBuilderConstraintTermFilterJson(alloc, out, field, term, count);
            }
        },
        else => try appendQueryBuilderConstraintTermFilterJson(alloc, out, field, value, count),
    }
}

fn appendQueryBuilderConstraintArrayFilterJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    field: []const u8,
    values: []const std.json.Value,
    count: *usize,
) !void {
    var item_json = std.ArrayListUnmanaged(u8).empty;
    defer item_json.deinit(alloc);
    var item_count: usize = 0;
    for (values) |value| {
        if (!isQueryBuilderScalarFilterValue(value)) continue;
        if (item_count > 0) try item_json.append(alloc, ',');
        const query_json = try std.fmt.allocPrint(
            alloc,
            "{{\"term\":{f},\"field\":{f}}}",
            .{ std.json.fmt(value, .{}), std.json.fmt(field, .{}) },
        );
        defer alloc.free(query_json);
        try item_json.appendSlice(alloc, query_json);
        item_count += 1;
    }
    if (item_count == 0) return;
    if (count.* > 0) try out.append(alloc, ',');
    if (item_count == 1) {
        try out.appendSlice(alloc, item_json.items);
    } else {
        try out.appendSlice(alloc, "{\"disjuncts\":[");
        try out.appendSlice(alloc, item_json.items);
        try out.appendSlice(alloc, "]}");
    }
    count.* += 1;
}

fn appendQueryBuilderConstraintTermFilterJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    field: []const u8,
    value: std.json.Value,
    count: *usize,
) !void {
    if (!isQueryBuilderScalarFilterValue(value)) return;
    if (count.* > 0) try out.append(alloc, ',');
    const query_json = try std.fmt.allocPrint(
        alloc,
        "{{\"term\":{f},\"field\":{f}}}",
        .{ std.json.fmt(value, .{}), std.json.fmt(field, .{}) },
    );
    defer alloc.free(query_json);
    try out.appendSlice(alloc, query_json);
    count.* += 1;
}

fn appendQueryBuilderConstraintRangeFilterJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    field: []const u8,
    object: std.json.ObjectMap,
    count: *usize,
) !void {
    var range_json = std.ArrayListUnmanaged(u8).empty;
    defer range_json.deinit(alloc);
    try range_json.appendSlice(alloc, "{\"field\":");
    try range_json.appendSlice(alloc, try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(field, .{})}));

    var saw_bound = false;
    _ = try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "start", "start", &saw_bound);
    _ = try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "end", "end", &saw_bound);
    _ = try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "min", "min", &saw_bound);
    _ = try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "max", "max", &saw_bound);
    if (try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "gte", "start", &saw_bound)) {
        try range_json.appendSlice(alloc, ",\"inclusive_start\":true");
    }
    if (try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "gt", "start", &saw_bound)) {
        try range_json.appendSlice(alloc, ",\"inclusive_start\":false");
    }
    if (try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "lte", "end", &saw_bound)) {
        try range_json.appendSlice(alloc, ",\"inclusive_end\":true");
    }
    if (try appendQueryBuilderConstraintRangeBoundJson(alloc, &range_json, object, "lt", "end", &saw_bound)) {
        try range_json.appendSlice(alloc, ",\"inclusive_end\":false");
    }

    if (object.get("inclusive_start")) |inclusive| {
        if (queryBuilderBoolValue(inclusive)) |bool_value| {
            try range_json.appendSlice(alloc, try std.fmt.allocPrint(alloc, ",\"inclusive_start\":{}", .{bool_value}));
        }
    }
    if (object.get("inclusive_end")) |inclusive| {
        if (queryBuilderBoolValue(inclusive)) |bool_value| {
            try range_json.appendSlice(alloc, try std.fmt.allocPrint(alloc, ",\"inclusive_end\":{}", .{bool_value}));
        }
    }

    if (!saw_bound) return;
    try range_json.append(alloc, '}');
    if (count.* > 0) try out.append(alloc, ',');
    try out.appendSlice(alloc, range_json.items);
    count.* += 1;
}

fn appendQueryBuilderConstraintRangeBoundJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    object: std.json.ObjectMap,
    source_key: []const u8,
    target_key: []const u8,
    saw_bound: *bool,
) !bool {
    const value = object.get(source_key) orelse return false;
    if (!isQueryBuilderScalarFilterValue(value)) return false;
    const query_json = try std.fmt.allocPrint(
        alloc,
        ",{f}:{f}",
        .{ std.json.fmt(target_key, .{}), std.json.fmt(value, .{}) },
    );
    defer alloc.free(query_json);
    try out.appendSlice(alloc, query_json);
    saw_bound.* = true;
    return true;
}

fn queryBuilderConstraintRangeObjectHasBounds(object: std.json.ObjectMap) bool {
    return object.get("start") != null or
        object.get("end") != null or
        object.get("min") != null or
        object.get("max") != null or
        object.get("gte") != null or
        object.get("gt") != null or
        object.get("lte") != null or
        object.get("lt") != null;
}

fn isQueryBuilderScalarFilterValue(value: std.json.Value) bool {
    return value == .string or
        value == .integer or
        value == .float or
        value == .number_string or
        value == .bool;
}

fn queryBuilderGraphSearches(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    built: BuiltQueryBuilderQuery,
    graph_indexes: []const []const u8,
) !?std.json.ArrayHashMap(indexes_openapi.GraphQuery) {
    if (try queryBuilderConstraintGraphSearches(alloc, request.constraints)) |graph_searches| return graph_searches;
    if (built.graph_searches) |graph_searches| return graph_searches;
    return try queryBuilderInferredGraphSearches(alloc, request, built, graph_indexes);
}

fn queryBuilderHasConstraintGraphSearches(constraints: ?std.json.Value) bool {
    const value = constraints orelse return false;
    if (value != .object) return false;
    return value.object.get("graph_searches") != null or value.object.get("graphs") != null;
}

fn queryBuilderConstraintGraphSearches(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
) !?std.json.ArrayHashMap(indexes_openapi.GraphQuery) {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const raw = value.object.get("graph_searches") orelse value.object.get("graphs") orelse return null;
    if (raw != .object) return null;

    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(raw, .{})});
    return std.json.parseFromSliceLeaky(std.json.ArrayHashMap(indexes_openapi.GraphQuery), alloc, encoded, .{
        .ignore_unknown_fields = true,
    }) catch null;
}

fn queryBuilderInferredGraphSearches(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    built: BuiltQueryBuilderQuery,
    graph_indexes: []const []const u8,
) !?std.json.ArrayHashMap(indexes_openapi.GraphQuery) {
    const index = queryBuilderGraphIndex(request, graph_indexes) orelse return null;
    const graph_mode = queryBuilderRequestedGraphMode(request) orelse return null;
    const start_nodes = try queryBuilderGraphStartNodes(alloc, request, built) orelse return null;
    const edge_types = try queryBuilderGraphEdgeTypes(alloc, request);

    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    errdefer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, "graph_search", .{
        .type = graph_mode.query_type,
        .index_name = try alloc.dupe(u8, std.mem.trim(u8, index, " \t\r\n")),
        .start_nodes = start_nodes,
        .target_nodes = try queryBuilderGraphTargetNodes(alloc, request, graph_mode),
        .params = queryBuilderGraphQueryParams(request, graph_mode, edge_types),
        .pattern = try queryBuilderGraphPatternSteps(alloc, graph_mode, edge_types),
        .return_aliases = try queryBuilderGraphReturnAliases(alloc, graph_mode),
        .include_documents = true,
        .include_edges = queryBuilderConstraintOptionalBool(request.constraints, "graph_include_edges") orelse graph_mode.include_edges,
        .fields = try queryBuilderConstraintStringSliceOrNull(alloc, request.constraints, "graph_fields"),
    });
    return graph_searches;
}

const QueryBuilderGraphMode = struct {
    query_type: indexes_openapi.GraphQueryType,
    max_depth: ?i64 = null,
    include_paths: ?bool = null,
    include_edges: ?bool = null,
    pattern_hops: ?i64 = null,
};

fn queryBuilderRequestedGraphMode(request: metadata_openapi.QueryBuilderRequest) ?QueryBuilderGraphMode {
    const mode = queryBuilderSelectedStrategy(request) orelse request.mode;
    if (mode) |value| {
        if (std.ascii.eqlIgnoreCase(value, "graph")) return queryBuilderGraphModeFromIntent(request.intent);
    }
    if (queryBuilderConstraintString(request.constraints, "graph_index") != null or
        queryBuilderConstraintString(request.constraints, "prefer_graph_index") != null or
        queryBuilderConstraintString(request.constraints, "graph") != null)
    {
        return queryBuilderGraphModeFromIntent(request.intent);
    }
    return null;
}

fn queryBuilderGraphModeFromIntent(intent: []const u8) QueryBuilderGraphMode {
    if (queryBuilderPatternHopCount(intent)) |hops| {
        return .{ .query_type = .pattern, .pattern_hops = hops };
    }
    if (containsWholeWordIgnoreCase(intent, "path") or
        containsWholeWordIgnoreCase(intent, "between") or
        containsWholeWordIgnoreCase(intent, "route"))
    {
        return .{ .query_type = .shortest_path, .max_depth = 6, .include_paths = true };
    }
    if (containsWholeWordIgnoreCase(intent, "neighbor") or
        containsWholeWordIgnoreCase(intent, "neighbors") or
        containsWholeWordIgnoreCase(intent, "adjacent") or
        containsWholeWordIgnoreCase(intent, "nearby"))
    {
        return .{ .query_type = .neighbors, .max_depth = 1 };
    }
    return .{ .query_type = .traverse, .max_depth = 2 };
}

fn queryBuilderPatternHopCount(intent: []const u8) ?i64 {
    if (std.ascii.indexOfIgnoreCase(intent, "two-hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "two hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "2-hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "2 hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "2hop") != null)
    {
        return 2;
    }
    if (std.ascii.indexOfIgnoreCase(intent, "three-hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "three hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "3-hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "3 hop") != null or
        std.ascii.indexOfIgnoreCase(intent, "3hop") != null)
    {
        return 3;
    }
    return null;
}

fn queryBuilderGraphIndex(
    request: metadata_openapi.QueryBuilderRequest,
    graph_indexes: []const []const u8,
) ?[]const u8 {
    if (queryBuilderDecisionString(request.decisions, "select_graph_index")) |index| return index;
    if (queryBuilderConstraintString(request.constraints, "graph_index")) |index| return index;
    if (queryBuilderConstraintString(request.constraints, "prefer_graph_index")) |index| return index;
    if (queryBuilderConstraintString(request.constraints, "graph")) |index| return index;
    const mode = queryBuilderSelectedStrategy(request) orelse request.mode orelse return null;
    if (!std.ascii.eqlIgnoreCase(mode, "graph")) return null;
    return if (graph_indexes.len == 1) graph_indexes[0] else null;
}

fn queryBuilderGraphStartNodes(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    built: BuiltQueryBuilderQuery,
) !?indexes_openapi.GraphNodeSelector {
    const value = queryBuilderConstraintString(request.constraints, "graph_start_nodes") orelse
        queryBuilderConstraintString(request.constraints, "start_nodes");
    if (value) |start_nodes| {
        const trimmed = std.mem.trim(u8, start_nodes, " \t\r\n");
        if (trimmed.len == 0) return null;
        if (trimmed[0] == '$') return .{ .result_ref = try alloc.dupe(u8, trimmed) };
        return try queryBuilderGraphNodeSelectorFromText(alloc, trimmed);
    }
    if (try queryBuilderInferredGraphNodeSelector(alloc, request.intent, .start)) |start_nodes| return start_nodes;
    const seed_ref = queryBuilderGraphSeedResultRef(built) orelse return null;
    return .{ .result_ref = seed_ref };
}

fn queryBuilderGraphTargetNodes(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    graph_mode: QueryBuilderGraphMode,
) !?indexes_openapi.GraphNodeSelector {
    const value = queryBuilderConstraintString(request.constraints, "graph_target_nodes") orelse
        queryBuilderConstraintString(request.constraints, "target_nodes");
    if (value) |target_nodes| {
        const trimmed = std.mem.trim(u8, target_nodes, " \t\r\n");
        if (trimmed.len == 0) return null;
        if (trimmed[0] == '$') return .{ .result_ref = try alloc.dupe(u8, trimmed) };
        return try queryBuilderGraphNodeSelectorFromText(alloc, trimmed);
    }
    if (graph_mode.query_type != .shortest_path and graph_mode.query_type != .k_shortest_paths) return null;
    return try queryBuilderInferredGraphNodeSelector(alloc, request.intent, .target);
}

fn queryBuilderGraphEdgeTypes(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
) !?[]const []const u8 {
    if (try queryBuilderConstraintStringSliceOrNull(alloc, request.constraints, "graph_edge_types")) |edge_types| return edge_types;
    return try queryBuilderInferredGraphEdgeTypes(alloc, request.intent);
}

fn queryBuilderGraphQueryParams(
    request: metadata_openapi.QueryBuilderRequest,
    graph_mode: QueryBuilderGraphMode,
    edge_types: ?[]const []const u8,
) ?indexes_openapi.GraphQueryParams {
    return if (graph_mode.query_type == .pattern)
        .{
            .max_results = queryBuilderConstraintInteger(request.constraints, "graph_max_results") orelse
                queryBuilderConstraintLimit(request.constraints),
            .deduplicate_nodes = true,
        }
    else
        .{
            .edge_types = edge_types,
            .max_depth = queryBuilderConstraintInteger(request.constraints, "graph_max_depth") orelse graph_mode.max_depth,
            .max_results = queryBuilderConstraintInteger(request.constraints, "graph_max_results") orelse
                queryBuilderConstraintLimit(request.constraints),
            .deduplicate_nodes = true,
            .include_paths = graph_mode.include_paths,
        };
}

fn queryBuilderGraphPatternSteps(
    alloc: std.mem.Allocator,
    graph_mode: QueryBuilderGraphMode,
    edge_types: ?[]const []const u8,
) !?[]const indexes_openapi.PatternStep {
    const hops = graph_mode.pattern_hops orelse return null;
    if (hops < 1 or hops > 3) return null;

    const step_count: usize = @intCast(hops + 1);
    const steps = try alloc.alloc(indexes_openapi.PatternStep, step_count);
    for (steps, 0..) |*step, i| {
        step.* = .{
            .alias = try queryBuilderPatternAlias(alloc, i),
            .edge = if (i == 0) null else .{
                .types = edge_types,
                .direction = .out,
                .min_hops = 1,
                .max_hops = 1,
            },
        };
    }
    return steps;
}

fn queryBuilderGraphReturnAliases(
    alloc: std.mem.Allocator,
    graph_mode: QueryBuilderGraphMode,
) !?[]const []const u8 {
    const hops = graph_mode.pattern_hops orelse return null;
    if (hops < 1 or hops > 3) return null;
    const alias = try queryBuilderPatternAlias(alloc, @intCast(hops));
    return try alloc.dupe([]const u8, &[_][]const u8{alias});
}

fn queryBuilderPatternAlias(
    alloc: std.mem.Allocator,
    index: usize,
) ![]const u8 {
    var alias = try alloc.alloc(u8, 1);
    alias[0] = 'a' + @as(u8, @intCast(index));
    return alias;
}

fn queryBuilderGraphSeedResultRef(built: BuiltQueryBuilderQuery) ?[]const u8 {
    return switch (built.query_kind) {
        .generic, .field_match, .conjunction, .llm_full_text => "$full_text_results",
        .hybrid => "$fused_results",
        .semantic => if (built.indexes != null and built.indexes.?.len > 0) "$embeddings_results" else null,
        .status_only => null,
    };
}

const QueryBuilderIntentNodeRole = enum {
    start,
    target,
};

fn queryBuilderInferredGraphNodeSelector(
    alloc: std.mem.Allocator,
    intent: []const u8,
    role: QueryBuilderIntentNodeRole,
) !?indexes_openapi.GraphNodeSelector {
    const node_text = queryBuilderInferredIntentNodeText(intent, role) orelse return null;
    return try queryBuilderGraphNodeSelectorFromText(alloc, node_text);
}

fn queryBuilderGraphNodeSelectorFromText(
    alloc: std.mem.Allocator,
    text: []const u8,
) !?indexes_openapi.GraphNodeSelector {
    const keys = try queryBuilderCsvStringSlice(alloc, text);
    if (keys.len == 0) return null;
    return .{ .keys = keys };
}

fn queryBuilderInferredIntentNodeText(
    intent: []const u8,
    role: QueryBuilderIntentNodeRole,
) ?[]const u8 {
    return switch (role) {
        .start => findIntentNodeAfterKeyword(intent, "from", &.{ "to", "toward", "through", "via", "over", "using", "with", "where", "," }) orelse
            findIntentNodeAfterKeyword(intent, "between", &.{ "and", "through", "via", "over", "using", "with", "where", "," }) orelse
            findIntentNodeAfterKeyword(intent, "starting at", &.{ "to", "through", "via", "over", "using", "with", "where", "," }) orelse
            findIntentNodeAfterKeyword(intent, "start at", &.{ "to", "through", "via", "over", "using", "with", "where", "," }) orelse
            findIntentNodeAfterKeyword(intent, "rooted at", &.{ "to", "through", "via", "over", "using", "with", "where", "," }),
        .target => findIntentBetweenTargetNode(intent) orelse
            findIntentNodeAfterKeyword(intent, "to", &.{ "through", "via", "over", "using", "with", "where", "," }) orelse
            findIntentNodeAfterKeyword(intent, "toward", &.{ "through", "via", "over", "using", "with", "where", "," }),
    };
}

fn findIntentBetweenTargetNode(intent: []const u8) ?[]const u8 {
    var start: usize = 0;
    while (std.ascii.indexOfIgnoreCasePos(intent, start, "between")) |idx| {
        const between_end = idx + "between".len;
        const left_ok = idx == 0 or !isWordByte(intent[idx - 1]);
        const right_ok = between_end >= intent.len or !isWordByte(intent[between_end]);
        if (left_ok and right_ok) {
            if (findIntentNodeAfterKeyword(intent[between_end..], "and", &.{ "through", "via", "over", "using", "with", "where", "," })) |target| return target;
        }
        start = idx + 1;
    }
    return null;
}

fn findIntentNodeAfterKeyword(
    intent: []const u8,
    keyword: []const u8,
    delimiters: []const []const u8,
) ?[]const u8 {
    var start: usize = 0;
    while (std.ascii.indexOfIgnoreCasePos(intent, start, keyword)) |idx| {
        const end = idx + keyword.len;
        const left_ok = idx == 0 or !isWordByte(intent[idx - 1]);
        const right_ok = end >= intent.len or !isWordByte(intent[end]);
        if (left_ok and right_ok) {
            var pos = end;
            while (pos < intent.len and (std.ascii.isWhitespace(intent[pos]) or intent[pos] == ':' or intent[pos] == '=')) : (pos += 1) {}
            const tail = intent[pos..];
            const delimiter = findIntentNodeDelimiter(tail, delimiters);
            const node = trimIntentNodeText(tail[0..delimiter]);
            if (queryBuilderLooksLikeNodeText(node)) return node;
        }
        start = idx + 1;
    }
    return null;
}

fn findIntentNodeDelimiter(text: []const u8, delimiters: []const []const u8) usize {
    var best = text.len;
    for (delimiters) |delimiter| {
        const idx = if (std.mem.eql(u8, delimiter, ","))
            std.mem.indexOfScalar(u8, text, ',')
        else
            findWholeWordIgnoreCase(text, delimiter);
        if (idx) |value| best = @min(best, value);
    }
    return best;
}

fn findWholeWordIgnoreCase(text: []const u8, needle: []const u8) ?usize {
    var start: usize = 0;
    while (std.ascii.indexOfIgnoreCasePos(text, start, needle)) |idx| {
        const end = idx + needle.len;
        const left_ok = idx == 0 or !isWordByte(text[idx - 1]);
        const right_ok = end >= text.len or !isWordByte(text[end]);
        if (left_ok and right_ok) return idx;
        start = idx + 1;
    }
    return null;
}

fn trimIntentNodeText(text: []const u8) []const u8 {
    return std.mem.trim(u8, text, " \t\r\n\"'`.,;()[]{}");
}

fn queryBuilderLooksLikeNodeText(text: []const u8) bool {
    if (text.len == 0) return false;
    for (text) |byte| {
        if (std.ascii.isWhitespace(byte)) return false;
    }
    return true;
}

fn queryBuilderInferredGraphEdgeTypes(
    alloc: std.mem.Allocator,
    intent: []const u8,
) !?[]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);

    if (containsWholeWordIgnoreCase(intent, "reference") or
        containsWholeWordIgnoreCase(intent, "references") or
        containsWholeWordIgnoreCase(intent, "referenced"))
    {
        try appendQueryBuilderEdgeType(alloc, &out, "references");
    }
    if (containsWholeWordIgnoreCase(intent, "cite") or
        containsWholeWordIgnoreCase(intent, "cites") or
        containsWholeWordIgnoreCase(intent, "citation") or
        containsWholeWordIgnoreCase(intent, "citations"))
    {
        try appendQueryBuilderEdgeType(alloc, &out, "cites");
    }
    if (containsWholeWordIgnoreCase(intent, "dependency") or
        containsWholeWordIgnoreCase(intent, "dependencies") or
        containsWholeWordIgnoreCase(intent, "depends"))
    {
        try appendQueryBuilderEdgeType(alloc, &out, "depends_on");
    }
    if (containsWholeWordIgnoreCase(intent, "link") or
        containsWholeWordIgnoreCase(intent, "links") or
        containsWholeWordIgnoreCase(intent, "linked"))
    {
        try appendQueryBuilderEdgeType(alloc, &out, "links");
    }
    if (containsWholeWordIgnoreCase(intent, "import") or
        containsWholeWordIgnoreCase(intent, "imports") or
        containsWholeWordIgnoreCase(intent, "imported"))
    {
        try appendQueryBuilderEdgeType(alloc, &out, "imports");
    }
    if (containsWholeWordIgnoreCase(intent, "call") or
        containsWholeWordIgnoreCase(intent, "calls") or
        containsWholeWordIgnoreCase(intent, "called"))
    {
        try appendQueryBuilderEdgeType(alloc, &out, "calls");
    }
    if (containsWholeWordIgnoreCase(intent, "parent") or
        containsWholeWordIgnoreCase(intent, "child") or
        containsWholeWordIgnoreCase(intent, "children"))
    {
        try appendQueryBuilderEdgeType(alloc, &out, "parent");
    }

    if (out.items.len == 0) return null;
    return try out.toOwnedSlice(alloc);
}

fn appendQueryBuilderEdgeType(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    edge_type: []const u8,
) !void {
    for (out.items) |existing| {
        if (std.mem.eql(u8, existing, edge_type)) return;
    }
    try out.append(alloc, try alloc.dupe(u8, edge_type));
}

fn queryBuilderCsvStringSlice(
    alloc: std.mem.Allocator,
    value: []const u8,
) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);
    var it = std.mem.splitScalar(u8, value, ',');
    while (it.next()) |raw| {
        const trimmed = std.mem.trim(u8, raw, " \t\r\n");
        if (trimmed.len == 0) continue;
        try out.append(alloc, try alloc.dupe(u8, trimmed));
    }
    if (out.items.len == 0) return &.{};
    return try out.toOwnedSlice(alloc);
}

fn buildQueryBuilderFilterQueryValue(
    alloc: std.mem.Allocator,
    built: BuiltQueryBuilderQuery,
) !?std.json.Value {
    const positive_count = countQueryBuilderPositiveFilters(built);
    const negative_count = countQueryBuilderNegativeFilters(built);
    if (positive_count == 0 and negative_count == 0) return null;

    var query_json = std.ArrayListUnmanaged(u8).empty;
    errdefer query_json.deinit(alloc);

    if (negative_count == 0 and positive_count == 1) {
        try appendQueryBuilderPositiveFiltersJson(alloc, &query_json, built);
    } else if (negative_count == 0) {
        try query_json.appendSlice(alloc, "{\"conjuncts\":[");
        try appendQueryBuilderPositiveFiltersJson(alloc, &query_json, built);
        try query_json.appendSlice(alloc, "]}");
    } else {
        try query_json.append(alloc, '{');
        var wrote_clause = false;
        if (positive_count > 0) {
            try query_json.appendSlice(alloc, "\"must\":[");
            try appendQueryBuilderPositiveFiltersJson(alloc, &query_json, built);
            try query_json.append(alloc, ']');
            wrote_clause = true;
        }
        if (negative_count > 0) {
            if (wrote_clause) try query_json.append(alloc, ',');
            try query_json.appendSlice(alloc, "\"must_not\":[");
            try appendQueryBuilderNegativeFiltersJson(alloc, &query_json, built);
            try query_json.append(alloc, ']');
        }
        try query_json.append(alloc, '}');
    }

    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, try query_json.toOwnedSlice(alloc), .{});
}

fn countQueryBuilderPositiveFilters(built: BuiltQueryBuilderQuery) usize {
    var count: usize = 0;
    if (built.status_field != null and built.status_value != null) count += 1;
    for (built.term_filters) |filter| {
        if (!filter.negated) count += 1;
    }
    if (built.date_field != null and (built.date_start != null or built.date_end != null)) count += 1;
    return count;
}

fn countQueryBuilderNegativeFilters(built: BuiltQueryBuilderQuery) usize {
    var count: usize = 0;
    for (built.term_filters) |filter| {
        if (filter.negated) count += 1;
    }
    return count;
}

fn appendQueryBuilderPositiveFiltersJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    built: BuiltQueryBuilderQuery,
) !void {
    var wrote = false;
    if (built.status_field != null and built.status_value != null) {
        try appendQueryBuilderTermFilterJson(alloc, out, built.status_value.?, built.status_field.?, &wrote);
    }
    for (built.term_filters) |filter| {
        if (filter.negated) continue;
        try appendQueryBuilderTermFilterJson(alloc, out, filter.value, filter.field, &wrote);
    }
    if (built.date_field != null and (built.date_start != null or built.date_end != null)) {
        try appendQueryBuilderRangeFilterJson(alloc, out, built, &wrote);
    }
}

fn appendQueryBuilderNegativeFiltersJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    built: BuiltQueryBuilderQuery,
) !void {
    var wrote = false;
    for (built.term_filters) |filter| {
        if (!filter.negated) continue;
        try appendQueryBuilderTermFilterJson(alloc, out, filter.value, filter.field, &wrote);
    }
}

fn appendQueryBuilderTermFilterJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
    field: []const u8,
    wrote: *bool,
) !void {
    if (wrote.*) try out.append(alloc, ',');
    const query_json = try std.fmt.allocPrint(
        alloc,
        "{{\"term\":{f},\"field\":{f}}}",
        .{ std.json.fmt(value, .{}), std.json.fmt(field, .{}) },
    );
    defer alloc.free(query_json);
    try out.appendSlice(alloc, query_json);
    wrote.* = true;
}

fn appendQueryBuilderRangeFilterJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    built: BuiltQueryBuilderQuery,
    wrote: *bool,
) !void {
    if (wrote.*) try out.append(alloc, ',');
    const query_json = try buildQueryBuilderRangeQueryJson(alloc, built);
    defer alloc.free(query_json);
    try out.appendSlice(alloc, query_json);
    wrote.* = true;
}

fn buildQueryBuilderMatchQueryValue(
    alloc: std.mem.Allocator,
    text: []const u8,
    field: []const u8,
) !std.json.Value {
    const query_json = try std.fmt.allocPrint(
        alloc,
        "{{\"match\":{f},\"field\":{f}}}",
        .{ std.json.fmt(text, .{}), std.json.fmt(field, .{}) },
    );
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{});
}

fn buildQueryBuilderRangeQueryValue(
    alloc: std.mem.Allocator,
    built: BuiltQueryBuilderQuery,
) !std.json.Value {
    const query_json = try buildQueryBuilderRangeQueryJson(alloc, built);
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{});
}

fn buildQueryBuilderRangeQueryJson(
    alloc: std.mem.Allocator,
    built: BuiltQueryBuilderQuery,
) ![]const u8 {
    const field = built.date_field.?;
    return if (built.date_start != null and built.date_end != null)
        std.fmt.allocPrint(
            alloc,
            "{{\"field\":{f},\"start\":{f},\"end\":{f},\"inclusive_start\":{},\"inclusive_end\":{}}}",
            .{
                std.json.fmt(field, .{}),
                std.json.fmt(built.date_start.?, .{}),
                std.json.fmt(built.date_end.?, .{}),
                built.date_inclusive_start,
                built.date_inclusive_end,
            },
        )
    else if (built.date_start != null)
        std.fmt.allocPrint(
            alloc,
            "{{\"field\":{f},\"start\":{f},\"inclusive_start\":{}}}",
            .{ std.json.fmt(field, .{}), std.json.fmt(built.date_start.?, .{}), built.date_inclusive_start },
        )
    else
        std.fmt.allocPrint(
            alloc,
            "{{\"field\":{f},\"end\":{f},\"inclusive_end\":{}}}",
            .{ std.json.fmt(field, .{}), std.json.fmt(built.date_end.?, .{}), built.date_inclusive_end },
        );
}

fn buildQueryBuilderTermQueryValue(
    alloc: std.mem.Allocator,
    value: []const u8,
    field: []const u8,
) !std.json.Value {
    const query_json = try std.fmt.allocPrint(
        alloc,
        "{{\"term\":{f},\"field\":{f}}}",
        .{ std.json.fmt(value, .{}), std.json.fmt(field, .{}) },
    );
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_json, .{});
}

fn queryBuilderConstraintLimit(constraints: ?std.json.Value) ?i64 {
    return queryBuilderConstraintInteger(constraints, "limit");
}

fn queryBuilderConstraintInteger(constraints: ?std.json.Value, key: []const u8) ?i64 {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const raw = value.object.get(key) orelse return null;
    return queryBuilderIntegerValue(raw);
}

fn queryBuilderIntegerValue(raw: std.json.Value) ?i64 {
    return switch (raw) {
        .integer => |int_value| if (int_value >= 0) int_value else null,
        .float => |float_value| if (float_value >= 0 and @floor(float_value) == float_value) @intFromFloat(float_value) else null,
        .number_string => |number_value| blk: {
            const parsed = std.fmt.parseInt(i64, number_value, 10) catch break :blk null;
            break :blk if (parsed >= 0) parsed else null;
        },
        else => null,
    };
}

fn queryBuilderConstraintOptionalBool(constraints: ?std.json.Value, key: []const u8) ?bool {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const raw = value.object.get(key) orelse return null;
    return queryBuilderBoolValue(raw);
}

fn queryBuilderConstraintBool(constraints: ?std.json.Value, key: []const u8) bool {
    const value = constraints orelse return false;
    if (value != .object) return false;
    const raw = value.object.get(key) orelse return false;
    return queryBuilderBoolValue(raw) orelse false;
}

fn queryBuilderBoolValue(raw: std.json.Value) ?bool {
    return switch (raw) {
        .bool => |bool_value| bool_value,
        .string => |string_value| if (std.ascii.eqlIgnoreCase(string_value, "true"))
            true
        else if (std.ascii.eqlIgnoreCase(string_value, "false"))
            false
        else
            null,
        else => null,
    };
}

fn validateRequiredExecutableQueryBuilderRequest(
    request: metadata_openapi.QueryBuilderRequest,
    built: BuiltQueryBuilderQuery,
    query_request: metadata_openapi.QueryRequest,
    retrieval_query_request: ?metadata_openapi.RetrievalQueryRequest,
    clarification_pending: bool,
) !void {
    if (!queryBuilderConstraintBool(request.constraints, "require_executable")) return;
    if (clarification_pending) return;
    if (query_request.table == null) return error.InvalidQueryBuilderRequest;
    if (unsupportedQueryBuilderMode(request.mode) != null) return error.InvalidQueryBuilderRequest;
    if (request.mode) |mode| {
        const semantic_mode = std.ascii.eqlIgnoreCase(mode, "semantic");
        const hybrid_mode = std.ascii.eqlIgnoreCase(mode, "hybrid");
        if ((semantic_mode and built.query_kind != .semantic) or (hybrid_mode and built.query_kind != .hybrid)) {
            return error.InvalidQueryBuilderRequest;
        }
        if (std.ascii.eqlIgnoreCase(mode, "tree") and (retrieval_query_request == null or retrieval_query_request.?.tree_search == null)) {
            return error.InvalidQueryBuilderRequest;
        }
        if (std.ascii.eqlIgnoreCase(mode, "graph") and query_request.graph_searches == null) {
            return error.InvalidQueryBuilderRequest;
        }
    }
    if (built.query_kind == .semantic or built.query_kind == .hybrid) {
        if (query_request.semantic_search == null) return error.InvalidQueryBuilderRequest;
        if (query_request.indexes == null or query_request.indexes.?.len == 0) return error.InvalidQueryBuilderRequest;
    }
}

fn queryBuilderConstraintStringSlice(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    key: []const u8,
) ![]const []const u8 {
    const value = constraints orelse return &.{};
    if (value != .object) return &.{};
    const raw = value.object.get(key) orelse return &.{};

    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);
    switch (raw) {
        .string => |string| {
            const trimmed = std.mem.trim(u8, string, " \t\r\n");
            if (trimmed.len > 0) try out.append(alloc, try alloc.dupe(u8, trimmed));
        },
        .array => |values| {
            for (values.items) |item| {
                const string = queryBuilderStringValue(item) orelse continue;
                const trimmed = std.mem.trim(u8, string, " \t\r\n");
                if (trimmed.len == 0) continue;
                try out.append(alloc, try alloc.dupe(u8, trimmed));
            }
        },
        else => {},
    }
    if (out.items.len == 0) return &.{};
    return try out.toOwnedSlice(alloc);
}

fn queryBuilderConstraintStringSliceOrNull(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    key: []const u8,
) !?[]const []const u8 {
    const values = try queryBuilderConstraintStringSlice(alloc, constraints, key);
    return if (values.len == 0) null else values;
}

fn queryBuilderConstraintFieldSlice(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
    key: []const u8,
) !?[]const []const u8 {
    const values = try queryBuilderConstraintStringSlice(alloc, constraints, key);
    if (values.len == 0) return null;

    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);
    for (values) |field| {
        if (!queryBuilderFieldAllowed(constraints, fields, field)) continue;
        try out.append(alloc, field);
    }
    if (out.items.len == 0) return null;
    return try out.toOwnedSlice(alloc);
}

fn queryBuilderConstraintSortFields(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
) !?[]const metadata_openapi.SortField {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const raw = value.object.get("order_by") orelse return null;

    var out = std.ArrayListUnmanaged(metadata_openapi.SortField).empty;
    defer out.deinit(alloc);
    switch (raw) {
        .array => |values| {
            for (values.items) |item| {
                if (try queryBuilderSortField(alloc, constraints, fields, item)) |sort| {
                    try out.append(alloc, sort);
                }
            }
        },
        .string, .object => {
            if (try queryBuilderSortField(alloc, constraints, fields, raw)) |sort| {
                try out.append(alloc, sort);
            }
        },
        else => {},
    }
    if (out.items.len == 0) return null;
    return try out.toOwnedSlice(alloc);
}

fn queryBuilderSortField(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
    value: std.json.Value,
) !?metadata_openapi.SortField {
    return switch (value) {
        .string => |raw| try queryBuilderSortFieldFromString(alloc, constraints, fields, raw),
        .object => |object| try queryBuilderSortFieldFromObject(alloc, constraints, fields, object),
        else => null,
    };
}

fn queryBuilderSortFieldFromString(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
    raw: []const u8,
) !?metadata_openapi.SortField {
    var desc: ?bool = false;
    var field = std.mem.trim(u8, raw, " \t\r\n");
    if (field.len == 0) return null;
    if (field[0] == '-' or field[0] == '+') {
        desc = field[0] == '-';
        field = std.mem.trim(u8, field[1..], " \t\r\n");
    }
    if (std.ascii.endsWithIgnoreCase(field, " desc")) {
        desc = true;
        field = std.mem.trim(u8, field[0 .. field.len - 5], " \t\r\n");
    } else if (std.ascii.endsWithIgnoreCase(field, " asc")) {
        desc = false;
        field = std.mem.trim(u8, field[0 .. field.len - 4], " \t\r\n");
    }
    if (!queryBuilderFieldAllowed(constraints, fields, field)) return null;
    return .{ .field = try alloc.dupe(u8, field), .desc = desc };
}

fn queryBuilderSortFieldFromObject(
    alloc: std.mem.Allocator,
    constraints: ?std.json.Value,
    fields: []const []const u8,
    object: std.json.ObjectMap,
) !?metadata_openapi.SortField {
    const raw_field = object.get("field") orelse object.get("name") orelse return null;
    const field = queryBuilderStringValue(raw_field) orelse return null;
    const trimmed = std.mem.trim(u8, field, " \t\r\n");
    if (!queryBuilderFieldAllowed(constraints, fields, trimmed)) return null;

    var desc: ?bool = null;
    if (object.get("desc")) |raw_desc| {
        desc = queryBuilderBoolValue(raw_desc);
    } else if (object.get("direction")) |raw_direction| {
        desc = queryBuilderSortDirection(raw_direction);
    } else if (object.get("order")) |raw_order| {
        desc = queryBuilderSortDirection(raw_order);
    }
    return .{ .field = try alloc.dupe(u8, trimmed), .desc = desc };
}

fn queryBuilderSortDirection(value: std.json.Value) ?bool {
    if (queryBuilderBoolValue(value)) |desc| return desc;
    const direction = queryBuilderStringValue(value) orelse return null;
    if (std.ascii.eqlIgnoreCase(direction, "desc") or std.ascii.eqlIgnoreCase(direction, "descending")) return true;
    if (std.ascii.eqlIgnoreCase(direction, "asc") or std.ascii.eqlIgnoreCase(direction, "ascending")) return false;
    return null;
}

fn queryBuilderConstraintString(constraints: ?std.json.Value, key: []const u8) ?[]const u8 {
    const value = constraints orelse return null;
    if (value != .object) return null;
    const raw = value.object.get(key) orelse return null;
    const string = queryBuilderStringValue(raw) orelse return null;
    return if (string.len > 0) string else null;
}

fn queryBuilderPreferredIndexSlice(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
) ![]const []const u8 {
    const constrained = try queryBuilderConstraintStringSlice(alloc, request.constraints, "prefer_indexes");
    if (constrained.len > 0) return constrained;
    return try queryBuilderDecisionStringSlice(alloc, request.decisions, "select_semantic_index");
}

fn queryBuilderEffectiveMode(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    semantic_indexes: []const []const u8,
) !?[]const u8 {
    if (queryBuilderSelectedStrategy(request)) |strategy| {
        return strategy;
    }

    const requested_mode = request.mode orelse return try queryBuilderAutoMode(alloc, request, fields, semantic_indexes);
    if (!std.ascii.eqlIgnoreCase(requested_mode, "auto")) return requested_mode;
    return try queryBuilderAutoMode(alloc, request, fields, semantic_indexes);
}

fn queryBuilderSelectedStrategy(request: metadata_openapi.QueryBuilderRequest) ?[]const u8 {
    if (queryBuilderDecisionString(request.decisions, "select_query_strategy")) |strategy| {
        return strategy;
    }
    return queryBuilderConstraintString(request.constraints, "prefer_strategy");
}

fn queryBuilderEffectiveTable(request: metadata_openapi.QueryBuilderRequest) ?[]const u8 {
    if (request.table) |table| {
        if (table.len > 0) return table;
    }
    if (queryBuilderDecisionString(request.decisions, "select_query_table")) |table| {
        return table;
    }
    return queryBuilderConstraintString(request.constraints, "table");
}

fn queryBuilderAutoMode(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    semantic_indexes: []const []const u8,
) !?[]const u8 {
    const preferred_indexes = try queryBuilderPreferredIndexSlice(alloc, request);
    const source_indexes = if (preferred_indexes.len > 0) preferred_indexes else semantic_indexes;
    if (source_indexes.len == 0) return request.mode;

    const selectable_fields = try queryBuilderSelectableFields(alloc, request.constraints, fields);
    const preferred_text_field = queryBuilderPreferredTextField(request, selectable_fields);
    if (pickQueryBuilderTextField(selectable_fields, preferred_text_field) != null) return "hybrid";
    return "semantic";
}

fn queryBuilderDecisionStringSlice(
    alloc: std.mem.Allocator,
    decisions: ?[]const metadata_openapi.AgentDecision,
    question_id: []const u8,
) ![]const []const u8 {
    const values = decisions orelse return &.{};
    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);
    for (values) |decision| {
        if (!std.mem.eql(u8, decision.question_id, question_id)) continue;
        const answer = decision.answer orelse continue;
        switch (answer) {
            .string => |value| {
                const trimmed = std.mem.trim(u8, value, " \t\r\n");
                if (trimmed.len > 0) try out.append(alloc, try alloc.dupe(u8, trimmed));
            },
            .array => |items| {
                for (items.items) |item| {
                    const value = queryBuilderStringValue(item) orelse continue;
                    const trimmed = std.mem.trim(u8, value, " \t\r\n");
                    if (trimmed.len > 0) try out.append(alloc, try alloc.dupe(u8, trimmed));
                }
            },
            else => {},
        }
    }
    if (out.items.len == 0) return &.{};
    return try out.toOwnedSlice(alloc);
}

fn queryBuilderDecisionString(
    decisions: ?[]const metadata_openapi.AgentDecision,
    question_id: []const u8,
) ?[]const u8 {
    const values = decisions orelse return null;
    for (values) |decision| {
        if (!std.mem.eql(u8, decision.question_id, question_id)) continue;
        const answer = decision.answer orelse continue;
        const value = queryBuilderStringValue(answer) orelse continue;
        const trimmed = std.mem.trim(u8, value, " \t\r\n");
        if (trimmed.len > 0) return trimmed;
    }
    return null;
}

fn queryBuilderPreferredTextField(
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
) ?[]const u8 {
    if (queryBuilderDecisionString(request.decisions, "select_text_field")) |field| {
        if (queryBuilderFieldInSlice(fields, field)) return field;
    }
    if (queryBuilderConstraintString(request.constraints, "prefer_field")) |field| {
        if (queryBuilderFieldInSlice(fields, field)) return field;
    }
    if (queryBuilderConstraintArray(request.constraints, "prefer_fields")) |values| {
        for (values.items) |value| {
            const field = queryBuilderStringValue(value) orelse continue;
            if (queryBuilderFieldInSlice(fields, field)) return field;
        }
    }
    return null;
}

fn buildQueryBuilderClarificationQuestion(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    fields: []const []const u8,
    semantic_indexes: []const []const u8,
    graph_indexes: []const []const u8,
    built: BuiltQueryBuilderQuery,
    example_document_count: usize,
) !?AgentQuestion {
    if (!queryBuilderCanAskClarification(request)) return null;

    const selectable_fields = try queryBuilderSelectableFields(alloc, request.constraints, fields);
    const selected_strategy = queryBuilderSelectedStrategy(request);
    const mode = selected_strategy orelse request.mode orelse "auto";
    const semantic_mode = std.ascii.eqlIgnoreCase(mode, "semantic");
    const hybrid_mode = std.ascii.eqlIgnoreCase(mode, "hybrid");
    const tree_mode = std.ascii.eqlIgnoreCase(mode, "tree");
    const graph_mode = std.ascii.eqlIgnoreCase(mode, "graph");
    if (tree_mode and queryBuilderTreeIndex(request, graph_indexes) == null) {
        if (graph_indexes.len == 0) {
            const options = try alloc.dupe([]const u8, &[_][]const u8{"constraints.tree_index"});
            const affects = try alloc.dupe([]const u8, &[_][]const u8{ "retrieval_query_request.tree_search.index", "specialist" });
            return .{
                .id = "select_tree_index",
                .kind = .free_text,
                .question = "Which graph index should tree search use?",
                .reason = "Tree mode was requested, but no tree index or table graph index was available.",
                .options = options,
                .default_answer = null,
                .affects = affects,
            };
        }
        if (graph_indexes.len > 1) {
            const options = try cloneStringSlice(alloc, graph_indexes);
            const affects = try alloc.dupe([]const u8, &[_][]const u8{ "retrieval_query_request.tree_search.index", "specialist" });
            return .{
                .id = "select_tree_index",
                .kind = .single_choice,
                .question = "Which graph index should tree search use?",
                .reason = "The target table exposes multiple graph indexes.",
                .options = options,
                .default_answer = options[0],
                .affects = affects,
            };
        }
    }

    if (graph_mode and !queryBuilderHasConstraintGraphSearches(request.constraints) and queryBuilderGraphIndex(request, graph_indexes) == null) {
        if (graph_indexes.len == 0) {
            const options = try alloc.dupe([]const u8, &[_][]const u8{"constraints.graph_index"});
            const affects = try alloc.dupe([]const u8, &[_][]const u8{ "query_request.graph_searches", "specialist" });
            return .{
                .id = "select_graph_index",
                .kind = .free_text,
                .question = "Which graph index should graph search use?",
                .reason = "Graph mode was requested, but no graph index or table graph index was available.",
                .options = options,
                .default_answer = null,
                .affects = affects,
            };
        }
        if (graph_indexes.len > 1) {
            const options = try cloneStringSlice(alloc, graph_indexes);
            const affects = try alloc.dupe([]const u8, &[_][]const u8{ "query_request.graph_searches", "specialist" });
            return .{
                .id = "select_graph_index",
                .kind = .single_choice,
                .question = "Which graph index should graph search use?",
                .reason = "The target table exposes multiple graph indexes.",
                .options = options,
                .default_answer = options[0],
                .affects = affects,
            };
        }
    }

    const preferred_indexes = try queryBuilderPreferredIndexSlice(alloc, request);
    if ((semantic_mode or hybrid_mode) and preferred_indexes.len == 0) {
        if (semantic_indexes.len == 0 and built.query_kind != .semantic and built.query_kind != .hybrid) {
            const options = try alloc.dupe([]const u8, &[_][]const u8{"constraints.prefer_indexes"});
            const affects = try alloc.dupe([]const u8, &[_][]const u8{ "query_request.indexes", "retrieval_strategy" });
            return .{
                .id = "select_semantic_index",
                .kind = .free_text,
                .question = "Which dense vector index should semantic search use?",
                .reason = "Semantic or hybrid mode was requested, but no preferred index or table embedding index was available.",
                .options = options,
                .default_answer = null,
                .affects = affects,
            };
        }
        if (semantic_indexes.len > 1) {
            const options = try cloneStringSlice(alloc, semantic_indexes);
            const affects = try alloc.dupe([]const u8, &[_][]const u8{ "query_request.indexes", "retrieval_strategy" });
            return .{
                .id = "select_semantic_index",
                .kind = .single_choice,
                .question = "Which dense vector index should semantic search prefer?",
                .reason = "The target table exposes multiple dense embedding indexes.",
                .options = options,
                .default_answer = options[0],
                .affects = affects,
            };
        }
    }

    if (queryBuilderEffectiveTable(request) == null and queryBuilderConstraintBool(request.constraints, "require_executable") and (selectable_fields.len > 0 or example_document_count > 0)) {
        const options = try alloc.dupe([]const u8, &[_][]const u8{"table"});
        const affects = try alloc.dupe([]const u8, &[_][]const u8{"query_request.table"});
        return .{
            .id = "select_query_table",
            .kind = .free_text,
            .question = "Which table should this query run against?",
            .reason = "An executable query request needs a target table.",
            .options = options,
            .default_answer = null,
            .affects = affects,
        };
    }

    if (queryBuilderEffectiveTable(request) == null and selectable_fields.len == 0 and example_document_count == 0) {
        const options = try alloc.dupe([]const u8, &[_][]const u8{ "table", "schema_fields", "example_documents" });
        const affects = try alloc.dupe([]const u8, &[_][]const u8{ "field_selection", "query_request.table" });
        return .{
            .id = "provide_query_context",
            .kind = .free_text,
            .question = "Which table or schema fields should this query target?",
            .reason = "No table, schema fields, or example documents were available for field selection.",
            .options = options,
            .default_answer = null,
            .affects = affects,
        };
    }

    const auto_mode = request.mode == null or std.ascii.eqlIgnoreCase(request.mode.?, "auto");
    const can_choose_strategy = selected_strategy == null and auto_mode and built.query_kind == .hybrid;
    if (can_choose_strategy) {
        const options = try alloc.dupe([]const u8, &[_][]const u8{ "hybrid", "full_text", "semantic" });
        const affects = try alloc.dupe([]const u8, &[_][]const u8{ "specialist", "query_request.full_text_search", "query_request.semantic_search", "query_request.indexes" });
        return .{
            .id = "select_query_strategy",
            .kind = .single_choice,
            .question = "Which retrieval strategy should this query use?",
            .reason = "The target table can support both lexical matching and dense vector search for this intent.",
            .options = options,
            .default_answer = "hybrid",
            .affects = affects,
        };
    }

    if (queryBuilderPreferredTextField(request, selectable_fields) == null) {
        const text_candidates = try queryBuilderTextFieldCandidates(alloc, selectable_fields);
        defer alloc.free(@constCast(text_candidates));
        if (text_candidates.len > 1) {
            const options = try cloneStringSlice(alloc, text_candidates);
            const affects = try alloc.dupe([]const u8, &[_][]const u8{ "query.field", "query_request.full_text_search" });
            return .{
                .id = "select_text_field",
                .kind = .single_choice,
                .question = "Which text field should the query target?",
                .reason = "Multiple searchable text fields are available and the query intent did not clearly choose one.",
                .options = options,
                .default_answer = pickQueryBuilderTextField(selectable_fields, null),
                .affects = affects,
            };
        }
    }

    return null;
}

fn queryBuilderCanAskClarification(request: metadata_openapi.QueryBuilderRequest) bool {
    if (!(request.interactive orelse false)) return false;
    const max_clarifications = request.max_user_clarifications orelse 0;
    if (max_clarifications <= 0) return false;
    const used: i64 = if (request.decisions) |decisions| @intCast(decisions.len) else 0;
    return used < max_clarifications;
}

fn pickQueryBuilderSpecialist(
    mode: ?[]const u8,
    built: BuiltQueryBuilderQuery,
    query_request: metadata_openapi.QueryRequest,
    has_retrieval_query_request: bool,
) []const u8 {
    if (mode) |value| {
        if (std.ascii.eqlIgnoreCase(value, "tree") and has_retrieval_query_request) return "tree";
        if (std.ascii.eqlIgnoreCase(value, "graph") and query_request.graph_searches != null) return "graph";
        if (std.ascii.eqlIgnoreCase(value, "full_text")) return "full_text";
        if (std.ascii.eqlIgnoreCase(value, "filter") and built.query_kind == .status_only) return "filter";
        if (std.ascii.eqlIgnoreCase(value, "semantic") and built.query_kind == .semantic) return "semantic";
        if (std.ascii.eqlIgnoreCase(value, "hybrid") and built.query_kind == .hybrid) return "hybrid";
    }
    if (has_retrieval_query_request) return "tree";
    if (query_request.graph_searches != null) return "graph";
    return switch (built.query_kind) {
        .status_only => "filter",
        .semantic => "semantic",
        .hybrid => "hybrid",
        else => "full_text",
    };
}

fn unsupportedQueryBuilderMode(mode: ?[]const u8) ?[]const u8 {
    const value = mode orelse return null;
    if (std.ascii.eqlIgnoreCase(value, "auto")) return null;
    if (std.ascii.eqlIgnoreCase(value, "full_text")) return null;
    if (std.ascii.eqlIgnoreCase(value, "filter")) return null;
    if (std.ascii.eqlIgnoreCase(value, "semantic")) return null;
    if (std.ascii.eqlIgnoreCase(value, "hybrid")) return null;
    if (std.ascii.eqlIgnoreCase(value, "tree")) return null;
    if (std.ascii.eqlIgnoreCase(value, "graph")) return null;
    return value;
}

fn buildQueryBuilderPlan(
    alloc: std.mem.Allocator,
    mode: ?[]const u8,
    output: ?[]const u8,
    specialist: []const u8,
    built: BuiltQueryBuilderQuery,
    artifact: []const u8,
) !std.json.Value {
    const query_kind = switch (built.query_kind) {
        .generic => "generic",
        .field_match => "field_match",
        .conjunction => "conjunction",
        .status_only => "status_only",
        .llm_full_text => "llm_full_text",
        .semantic => "semantic",
        .hybrid => "hybrid",
    };
    const plan_json = try std.fmt.allocPrint(
        alloc,
        "{{\"mode\":{f},\"output\":{f},\"specialist\":{f},\"query_kind\":{f},\"artifact\":{f}}}",
        .{
            std.json.fmt(mode orelse "auto", .{}),
            std.json.fmt(output orelse "query_request", .{}),
            std.json.fmt(specialist, .{}),
            std.json.fmt(query_kind, .{}),
            std.json.fmt(artifact, .{}),
        },
    );
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, plan_json, .{});
}

fn buildQueryBuilderExplanation(
    alloc: std.mem.Allocator,
    fields: []const []const u8,
    built: BuiltQueryBuilderQuery,
) ![]const u8 {
    if (built.graph_searches != null) {
        if (built.llm_explanation) |explanation| return explanation;
    }
    return switch (built.query_kind) {
        .semantic => if (built.indexes) |indexes|
            try std.fmt.allocPrint(
                alloc,
                "Builds a semantic Antfly query over {d} preferred vector index(es).",
                .{indexes.len},
            )
        else
            try alloc.dupe(u8, "Builds a semantic Antfly query from the user's intent."),
        .hybrid => if (built.indexes) |indexes|
            try std.fmt.allocPrint(
                alloc,
                "Builds a hybrid Antfly query by combining full-text matching with semantic search over {d} preferred vector index(es).",
                .{indexes.len},
            )
        else
            try alloc.dupe(u8, "Builds a hybrid Antfly query from the user's intent."),
        .llm_full_text => if (built.llm_explanation) |explanation|
            explanation
        else
            try alloc.dupe(u8, "Generated a native Bleve full-text query using the configured query-builder generator."),
        .conjunction => try std.fmt.allocPrint(
            alloc,
            "Searches {s} for the requested content and constrains {s} to {s}.",
            .{ built.text_field.?, built.status_field.?, built.status_value.? },
        ),
        .field_match => try std.fmt.allocPrint(
            alloc,
            "Searches the {s} field for the user's intent using a Bleve match query.",
            .{built.text_field.?},
        ),
        .status_only => try std.fmt.allocPrint(
            alloc,
            "Constrains {s} to {s} because the intent reads like a status-only filter.",
            .{ built.status_field.?, built.status_value.? },
        ),
        .generic => if (fields.len > 0)
            try std.fmt.allocPrint(
                alloc,
                "Builds a generic Bleve query string using the available field context: {s}.",
                .{fields[0]},
            )
        else
            try alloc.dupe(u8, "Builds a generic Bleve query string because no schema field context was available."),
    };
}

fn queryBuilderConfidence(fields: []const []const u8, built: BuiltQueryBuilderQuery) f64 {
    if (built.llm_confidence) |confidence| return confidence;
    const base: f64 = switch (built.query_kind) {
        .llm_full_text => 0.82,
        .hybrid => 0.8,
        .semantic => 0.74,
        .conjunction => 0.83,
        .field_match => 0.76,
        .status_only => 0.72,
        .generic => 0.58,
    };
    const field_bonus: f64 = if (fields.len >= 3) 0.04 else if (fields.len > 0) 0.02 else 0.0;
    return @min(1.0, base + field_bonus);
}

fn buildQueryBuilderStepDetails(
    alloc: std.mem.Allocator,
    table: ?[]const u8,
    fields: []const []const u8,
    example_document_count: usize,
    mode: ?[]const u8,
    output: ?[]const u8,
    specialist: []const u8,
    preflight: ?*const QueryPreflightResult,
) !std.json.Value {
    const preflight_details: ?QueryBuilderStepPreflightDetails = if (preflight) |summary|
        if (summary.diagnostics.len == 0 and summary.plan_summary == null and summary.estimate_summary == null)
            null
        else
            .{
                .mode = @tagName(if (summary.estimate_summary != null) QueryPreflightMode.estimate else QueryPreflightMode.plan),
                .diagnostics = summary.diagnostics,
                .plan_summary = summary.plan_summary,
                .estimate_summary = summary.estimate_summary,
            }
    else
        null;
    const encoded = try std.json.Stringify.valueAlloc(alloc, QueryBuilderStepDetailsJson{
        .table = table,
        .schema_fields = fields,
        .example_document_count = example_document_count,
        .mode = mode orelse "auto",
        .output = output orelse "query_request",
        .specialist = specialist,
        .preflight = preflight_details,
    }, .{});
    defer alloc.free(encoded);
    return try std.json.parseFromSliceLeaky(std.json.Value, alloc, encoded, .{});
}

fn pickQueryBuilderTextField(fields: []const []const u8, preferred: ?[]const u8) ?[]const u8 {
    if (preferred) |field| {
        if (queryBuilderFieldInSlice(fields, field)) return field;
    }
    for ([_][]const u8{ "body", "content", "text", "description", "summary", "title", "name" }) |candidate| {
        for (fields) |field| {
            if (std.ascii.eqlIgnoreCase(field, candidate)) return field;
        }
    }
    return if (fields.len > 0) fields[0] else null;
}

fn queryBuilderTextFieldCandidates(alloc: std.mem.Allocator, fields: []const []const u8) ![]const []const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    defer out.deinit(alloc);
    for (fields) |field| {
        if (queryBuilderFieldLooksStatusLike(field)) continue;
        if (!queryBuilderFieldLooksTextLike(field)) continue;
        try out.append(alloc, field);
    }
    if (out.items.len == 0) return try alloc.alloc([]const u8, 0);
    return try out.toOwnedSlice(alloc);
}

fn queryBuilderFieldLooksTextLike(field: []const u8) bool {
    for ([_][]const u8{ "body", "content", "text", "description", "summary", "title", "name", "message", "note" }) |candidate| {
        if (std.ascii.eqlIgnoreCase(field, candidate)) return true;
        if (std.ascii.endsWithIgnoreCase(field, candidate)) return true;
    }
    return false;
}

fn queryBuilderFieldLooksStatusLike(field: []const u8) bool {
    for ([_][]const u8{ "status", "state", "published", "visibility" }) |candidate| {
        if (std.ascii.eqlIgnoreCase(field, candidate)) return true;
        if (std.ascii.endsWithIgnoreCase(field, candidate)) return true;
    }
    return false;
}

fn pickQueryBuilderStatusField(fields: []const []const u8) ?[]const u8 {
    for ([_][]const u8{ "status", "state", "published", "visibility" }) |candidate| {
        for (fields) |field| {
            if (std.ascii.eqlIgnoreCase(field, candidate)) return field;
        }
    }
    return null;
}

fn pickQueryBuilderDateField(fields: []const []const u8) ?[]const u8 {
    for ([_][]const u8{ "published_at", "created_at", "updated_at", "timestamp", "date" }) |candidate| {
        for (fields) |field| {
            if (std.ascii.eqlIgnoreCase(field, candidate)) return field;
        }
    }
    for (fields) |field| {
        if (std.ascii.endsWithIgnoreCase(field, "_at")) return field;
        if (std.ascii.endsWithIgnoreCase(field, "_date")) return field;
    }
    return null;
}

fn detectQueryBuilderStatusValue(intent: []const u8) ?[]const u8 {
    for ([_][]const u8{ "published", "draft", "active", "inactive", "archived", "pending" }) |candidate| {
        if (containsStatusValueIgnoreCase(intent, candidate) and !containsExcludedQueryBuilderStatusValue(intent, candidate)) return candidate;
    }
    return null;
}

fn detectQueryBuilderTermFilters(
    alloc: std.mem.Allocator,
    intent: []const u8,
    fields: []const []const u8,
    status_field: ?[]const u8,
    status_value: ?[]const u8,
) ![]const QueryBuilderTermFilter {
    var out = std.ArrayListUnmanaged(QueryBuilderTermFilter).empty;
    defer out.deinit(alloc);

    for (fields) |field| {
        if (findQueryBuilderFieldConstraintValue(intent, field)) |value| {
            if (status_field != null and status_value != null and queryBuilderFieldMatches(status_field.?, field) and std.ascii.eqlIgnoreCase(status_value.?, value)) {
                continue;
            }
            try appendQueryBuilderTermFilter(alloc, &out, .{ .field = field, .value = value });
        }
    }

    if (status_field) |field| {
        for ([_][]const u8{ "published", "draft", "active", "inactive", "archived", "pending" }) |value| {
            if (containsExcludedQueryBuilderStatusValue(intent, value)) {
                try appendQueryBuilderTermFilter(alloc, &out, .{ .field = field, .value = value, .negated = true });
            }
        }
    }

    if (out.items.len == 0) return &.{};
    return try out.toOwnedSlice(alloc);
}

fn appendQueryBuilderTermFilter(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(QueryBuilderTermFilter),
    filter: QueryBuilderTermFilter,
) !void {
    if (filter.value.len == 0) return;
    for (out.items) |existing| {
        if (existing.negated == filter.negated and queryBuilderFieldMatches(existing.field, filter.field) and std.ascii.eqlIgnoreCase(existing.value, filter.value)) {
            return;
        }
    }
    try out.append(alloc, filter);
}

fn findQueryBuilderFieldConstraintValue(intent: []const u8, field: []const u8) ?[]const u8 {
    var start: usize = 0;
    while (std.ascii.indexOfIgnoreCasePos(intent, start, field)) |idx| {
        const end = idx + field.len;
        const left_ok = idx == 0 or !isFieldNameByte(intent[idx - 1]);
        const right_ok = end >= intent.len or !isFieldNameByte(intent[end]);
        if (left_ok and right_ok) {
            var pos = end;
            while (pos < intent.len and std.ascii.isWhitespace(intent[pos])) : (pos += 1) {}
            if (pos < intent.len and (intent[pos] == ':' or intent[pos] == '=')) {
                if (parseQueryBuilderConstraintValue(intent[pos + 1 ..])) |value| return value;
            }
            if (matchKeywordAt(intent, pos, "is")) |next| {
                if (parseQueryBuilderConstraintValue(intent[next..])) |value| return value;
            }
            if (matchKeywordAt(intent, pos, "equals")) |next| {
                if (parseQueryBuilderConstraintValue(intent[next..])) |value| return value;
            }
        }
        start = idx + 1;
    }
    return null;
}

fn parseQueryBuilderConstraintValue(text: []const u8) ?[]const u8 {
    var start: usize = 0;
    while (start < text.len and std.ascii.isWhitespace(text[start])) : (start += 1) {}
    if (start >= text.len) return null;

    if (text[start] == '"' or text[start] == '\'') {
        const quote = text[start];
        var end: usize = start + 1;
        while (end < text.len and text[end] != quote) : (end += 1) {}
        if (end <= start + 1) return null;
        return text[start + 1 .. end];
    }

    var end = start;
    while (end < text.len and !std.ascii.isWhitespace(text[end]) and text[end] != ',' and text[end] != ';') : (end += 1) {}
    while (end > start and (text[end - 1] == '.' or text[end - 1] == ')' or text[end - 1] == ']')) : (end -= 1) {}
    if (end <= start) return null;
    return text[start..end];
}

fn containsExcludedQueryBuilderStatusValue(intent: []const u8, value: []const u8) bool {
    var start: usize = 0;
    while (findStatusValueOccurrence(intent, value, start)) |occurrence| {
        if (previousWordIsExclusionKeyword(intent, occurrence.start)) return true;
        start = occurrence.start + 1;
    }
    return false;
}

fn containsStatusValueIgnoreCase(intent: []const u8, value: []const u8) bool {
    return findStatusValueOccurrence(intent, value, 0) != null;
}

const QueryBuilderStatusOccurrence = struct {
    start: usize,
    end: usize,
};

fn findStatusValueOccurrence(intent: []const u8, value: []const u8, start: usize) ?QueryBuilderStatusOccurrence {
    var cursor = start;
    while (std.ascii.indexOfIgnoreCasePos(intent, cursor, value)) |idx| {
        var end = idx + value.len;
        const left_ok = idx == 0 or !isWordByte(intent[idx - 1]);
        var right_ok = end >= intent.len or !isWordByte(intent[end]);
        if (!right_ok and end < intent.len and intent[end] == 's') {
            const plural_end = end + 1;
            if (plural_end >= intent.len or !isWordByte(intent[plural_end])) {
                end = plural_end;
                right_ok = true;
            }
        }
        if (left_ok and right_ok) return .{ .start = idx, .end = end };
        cursor = idx + 1;
    }
    return null;
}

fn previousWordIsExclusionKeyword(text: []const u8, pos: usize) bool {
    if (pos == 0) return false;
    var end = pos;
    while (end > 0 and std.ascii.isWhitespace(text[end - 1])) : (end -= 1) {}
    var start = end;
    while (start > 0 and isWordByte(text[start - 1])) : (start -= 1) {}
    if (start == end) return false;
    const word = text[start..end];
    return std.ascii.eqlIgnoreCase(word, "not") or
        std.ascii.eqlIgnoreCase(word, "exclude") or
        std.ascii.eqlIgnoreCase(word, "excluding") or
        std.ascii.eqlIgnoreCase(word, "without") or
        std.ascii.eqlIgnoreCase(word, "except");
}

fn matchKeywordAt(text: []const u8, pos: usize, keyword: []const u8) ?usize {
    if (pos + keyword.len > text.len) return null;
    if (!std.ascii.eqlIgnoreCase(text[pos .. pos + keyword.len], keyword)) return null;
    const end = pos + keyword.len;
    if (pos > 0 and isWordByte(text[pos - 1])) return null;
    if (end < text.len and isWordByte(text[end])) return null;
    return end;
}

const QueryBuilderDateFilter = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
    inclusive_start: bool = false,
    inclusive_end: bool = false,
};

fn detectQueryBuilderDateFilter(intent: []const u8) ?QueryBuilderDateFilter {
    var filter = QueryBuilderDateFilter{};
    if (findIsoDateAfterKeyword(intent, "after")) |date| {
        filter.start = date;
        filter.inclusive_start = false;
    } else if (findIsoDateAfterKeyword(intent, "since")) |date| {
        filter.start = date;
        filter.inclusive_start = true;
    } else if (findIsoDateAfterKeyword(intent, "from")) |date| {
        filter.start = date;
        filter.inclusive_start = true;
    }

    if (findIsoDateAfterKeyword(intent, "before")) |date| {
        filter.end = date;
        filter.inclusive_end = false;
    } else if (findIsoDateAfterKeyword(intent, "until")) |date| {
        filter.end = date;
        filter.inclusive_end = true;
    } else if (findIsoDateAfterKeyword(intent, "through")) |date| {
        filter.end = date;
        filter.inclusive_end = true;
    } else if (findIsoDateAfterKeyword(intent, "to")) |date| {
        filter.end = date;
        filter.inclusive_end = true;
    }

    if (filter.start == null and filter.end == null) return null;
    return filter;
}

fn findIsoDateAfterKeyword(text: []const u8, keyword: []const u8) ?[]const u8 {
    var start: usize = 0;
    while (std.ascii.indexOfIgnoreCasePos(text, start, keyword)) |idx| {
        const end = idx + keyword.len;
        const left_ok = idx == 0 or !isWordByte(text[idx - 1]);
        const right_ok = end >= text.len or !isWordByte(text[end]);
        if (left_ok and right_ok) {
            var pos = end;
            while (pos < text.len and std.ascii.isWhitespace(text[pos])) : (pos += 1) {}
            if (parseIsoDatePrefix(text[pos..])) |len| return text[pos .. pos + len];
        }
        start = idx + 1;
    }
    return null;
}

fn parseIsoDatePrefix(text: []const u8) ?usize {
    if (text.len < 10) return null;
    if (!std.ascii.isDigit(text[0]) or !std.ascii.isDigit(text[1]) or !std.ascii.isDigit(text[2]) or !std.ascii.isDigit(text[3])) return null;
    if (text[4] != '-') return null;
    if (!std.ascii.isDigit(text[5]) or !std.ascii.isDigit(text[6])) return null;
    if (text[7] != '-') return null;
    if (!std.ascii.isDigit(text[8]) or !std.ascii.isDigit(text[9])) return null;
    return 10;
}

fn detectTemporalHint(intent: []const u8) bool {
    return containsWholeWordIgnoreCase(intent, "recent") or
        containsWholeWordIgnoreCase(intent, "latest") or
        containsWholeWordIgnoreCase(intent, "today") or
        std.ascii.indexOfIgnoreCase(intent, "last year") != null or
        std.ascii.indexOfIgnoreCase(intent, "last month") != null or
        std.ascii.indexOfIgnoreCase(intent, "last week") != null;
}

fn trimQueryBuilderIntent(intent: []const u8) []const u8 {
    return std.mem.trim(u8, intent, " \t\r\n");
}

fn containsWholeWordIgnoreCase(text: []const u8, needle: []const u8) bool {
    var start: usize = 0;
    while (std.ascii.indexOfIgnoreCasePos(text, start, needle)) |idx| {
        const end = idx + needle.len;
        const left_ok = idx == 0 or !isWordByte(text[idx - 1]);
        const right_ok = end >= text.len or !isWordByte(text[end]);
        if (left_ok and right_ok) return true;
        start = idx + 1;
    }
    return false;
}

fn isWordByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_';
}

fn isFieldNameByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_' or byte == '.';
}

test "query builder uses generated full text specialist when runner is provided" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            chain: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            try std.testing.expectEqual(@as(usize, 1), chain.len);
            try std.testing.expectEqualStrings("local-generator", chain[0].generator.model);
            try std.testing.expectEqual(@as(usize, 2), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "Native Bleve") != null or std.mem.indexOf(u8, messages[0].content, "native Bleve") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "Full-text indexes:") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "search_idx fields: title, body") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "raft snapshots") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"query":{"match_phrase":"raft snapshots","field":"body"},"explanation":"Uses phrase search over body.","confidence":0.91,"warnings":["checked schema fields"]}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const full_text_fields = [_][]const u8{ "title", "body" };
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    const result = try buildQueryBuilderResponseWithContext(arena, .{
        .intent = "find raft snapshots",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "full_text",
        .output = "query_request",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .full_text_index_metadata = &full_text_metadata,
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("full_text", result.specialist.?);
    try std.testing.expect(result.query.object.get("match_phrase") != null);
    try std.testing.expect(result.query_request != null);
    try std.testing.expect(result.query_request.?.full_text_search != null);
    try std.testing.expect(result.query_request.?.filter_query == null);
    try std.testing.expectEqualStrings("Uses phrase search over body.", result.explanation.?);
    try std.testing.expectEqual(@as(f64, 0.91), result.confidence.?);
    try std.testing.expect(result.warnings != null);
    try std.testing.expectEqualStrings("checked schema fields", result.warnings.?[0]);
    try std.testing.expect(result.plan.?.object.get("query_kind") != null);
}

test "query builder uses generated semantic specialist with embedding metadata prompt" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            chain: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            try std.testing.expectEqual(@as(usize, 1), chain.len);
            try std.testing.expectEqualStrings("local-generator", chain[0].generator.model);
            try std.testing.expectEqual(@as(usize, 2), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "Allowed dense embedding indexes:") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "body_embedding dense dimension: 384 model: e5-small") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "sparse_idx sparse model: splade") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "raft snapshots") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"semantic_search":"raft snapshot architecture","indexes":["body_embedding"],"full_text_search":null,"explanation":"Uses dense semantic retrieval.","confidence":0.88,"warnings":["kept sparse index out"]}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const embedding_metadata = [_]QueryBuilderEmbeddingIndex{
        .{ .name = "body_embedding", .dimension = 384, .model = "e5-small" },
        .{ .name = "sparse_idx", .sparse = true, .model = "splade" },
    };
    const result = try buildQueryBuilderResponseWithContext(arena, .{
        .intent = "find raft snapshots",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .output = "query_request",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .embedding_index_metadata = &embedding_metadata,
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expectEqualStrings("raft snapshot architecture", result.query_request.?.semantic_search.?);
    try std.testing.expectEqualStrings("body_embedding", result.query_request.?.indexes.?[0]);
    try std.testing.expect(result.query_request.?.full_text_search == null);
    try std.testing.expectEqualStrings("Uses dense semantic retrieval.", result.explanation.?);
    try std.testing.expectEqual(@as(f64, 0.88), result.confidence.?);
    try std.testing.expectEqualStrings("kept sparse index out", result.warnings.?[0]);
}

test "query builder response step details include preflight summaries" {
    const RuntimeValidator = struct {
        fn iface() QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .validate_query_request = validateQueryRequest,
                    .preflight_query_request = runtimePreflightQueryRequest,
                },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            return null;
        }

        fn runtimePreflightQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
            _: u32,
        ) !?db_mod.RuntimePreflightSummary {
            return .{
                .text_indexes = try alloc.dupe(db_mod.TextIndexEstimate, &.{.{
                    .name = try alloc.dupe(u8, "search_idx"),
                    .doc_count = 42,
                    .chunk_backed = true,
                    .group_chunk_parents = true,
                }}),
                .positive_id_result_upper_bound = 2,
                .shard_count = 1,
                .remote_shard_count = 1,
                .shard_result_window = 5,
                .shard_result_window_total = 5,
                .stored_projection_doc_upper_bound_total = 5,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const full_text_fields = [_][]const u8{"body"};
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    const result = try buildQueryBuilderResponseWithContext(arena, .{
        .intent = "find raft snapshots",
        .schema_fields = &.{"body"},
        .mode = "full_text",
        .output = "query_request",
    }, .{
        .full_text_index_metadata = &full_text_metadata,
        .runtime_query_request_validator = RuntimeValidator.iface(),
    }, null);

    try std.testing.expectEqual(@as(usize, 1), result.steps.?.len);
    const details = result.steps.?[0].details;
    try std.testing.expect(details != null);
    const preflight = details.?.object.get("preflight");
    try std.testing.expect(preflight != null);
    try std.testing.expectEqualStrings("estimate", preflight.?.object.get("mode").?.string);
    try std.testing.expectEqual(@as(usize, 0), preflight.?.object.get("diagnostics").?.array.items.len);
    const estimate_summary = preflight.?.object.get("estimate_summary");
    try std.testing.expect(estimate_summary != null);
    try std.testing.expectEqual(@as(i64, 42), estimate_summary.?.object.get("corpus_doc_count_estimate").?.integer);
    try std.testing.expectEqual(std.json.Value{ .null = {} }, estimate_summary.?.object.get("result_doc_estimate").?);
    try std.testing.expectEqual(std.json.Value{ .null = {} }, estimate_summary.?.object.get("effective_stored_projection_doc_estimate_total").?);
    try std.testing.expectEqualStrings("upper_bound", estimate_summary.?.object.get("selectivity_estimate_kind").?.string);
    try std.testing.expectEqualStrings("heuristic", estimate_summary.?.object.get("latency_estimate_kind").?.string);
    try std.testing.expectEqual(@as(usize, 2), estimate_summary.?.object.get("latency_score_components").?.array.items.len);
}

test "query builder generated semantic path does not prompt with sparse preferred index" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_indexes":["sparse_idx"]}
    , .{});
    defer constraints_tree.deinit();

    const FakeGeneration = struct {
        fn iface(calls: *usize) GenerationRunner {
            return .{
                .ptr = calls,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            const calls: *usize = @ptrCast(@alignCast(ptr));
            calls.* += 1;
            return .{
                .content = try alloc.dupe(u8, "{}"),
                .allocator = alloc,
            };
        }
    };

    const embedding_metadata = [_]QueryBuilderEmbeddingIndex{
        .{ .name = "body_embedding", .dimension = 384, .model = "e5-small" },
        .{ .name = "sparse_idx", .sparse = true, .model = "splade" },
    };
    var context = QueryBuilderTableContext{
        .embedding_index_metadata = &embedding_metadata,
    };
    context.plan_validator = metadataBackedPlanValidator(&context);

    var calls: usize = 0;
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft snapshots",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .output = "query_request",
        .constraints = constraints_tree.value,
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, context, FakeGeneration.iface(&calls));

    try std.testing.expectEqual(@as(usize, 0), calls);
    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expectEqualStrings("sparse_idx", result.query_request.?.indexes.?[0]);
    try std.testing.expect(result.warnings != null);
    var saw_sparse_feedback = false;
    for (result.warnings.?) |warning| {
        if (std.mem.indexOf(u8, warning, "sparse embedding index 'sparse_idx'") != null) saw_sparse_feedback = true;
    }
    try std.testing.expect(saw_sparse_feedback);
}

test "query builder uses generated hybrid specialist with full text validation" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            chain: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            try std.testing.expectEqual(@as(usize, 1), chain.len);
            try std.testing.expectEqual(@as(usize, 2), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "Hybrid mode must also include full_text_search") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "search_idx fields: title, body") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"semantic_search":"raft snapshot architecture","indexes":["body_embedding"],"full_text_search":{"match_phrase":"raft snapshots","field":"body"},"explanation":"Combines phrase search with dense retrieval.","confidence":0.9,"warnings":[]}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const full_text_fields = [_][]const u8{ "title", "body" };
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    const embedding_metadata = [_]QueryBuilderEmbeddingIndex{.{
        .name = "body_embedding",
        .dimension = 384,
        .model = "e5-small",
    }};
    const result = try buildQueryBuilderResponseWithContext(arena, .{
        .intent = "find raft snapshots",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "hybrid",
        .output = "query_request",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .full_text_index_metadata = &full_text_metadata,
        .embedding_index_metadata = &embedding_metadata,
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("hybrid", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expectEqualStrings("raft snapshot architecture", result.query_request.?.semantic_search.?);
    try std.testing.expectEqualStrings("body_embedding", result.query_request.?.indexes.?[0]);
    try std.testing.expect(result.query_request.?.full_text_search != null);
    try std.testing.expect(result.query_request.?.full_text_search.?.object.get("match_phrase") != null);
    try std.testing.expectEqualStrings("Combines phrase search with dense retrieval.", result.explanation.?);
}

test "query builder metadata validator rejects generated full text fields outside index metadata" {
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"match\":\"published\",\"field\":\"status\"}", .{});
    defer parsed.deinit();

    const full_text_fields = [_][]const u8{ "title", "body" };
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    const context = QueryBuilderTableContext{
        .full_text_index_metadata = &full_text_metadata,
    };
    const validator = metadataBackedPlanValidator(&context);
    const feedback = try validator.validateBleveQuery(std.testing.allocator, .{
        .intent = "find published docs",
    }, parsed.value);
    defer if (feedback) |value| std.testing.allocator.free(value);

    try std.testing.expect(feedback != null);
    try std.testing.expect(std.mem.indexOf(u8, feedback.?, "status") != null);
}

test "query builder metadata validator preflights graph searches against executor parser" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "graph_searches": {
        \\    "graph_search": {
        \\      "type": "neighbors",
        \\      "index_name": "doc_graph",
        \\      "start_nodes": {"keys": ["doc:a"]},
        \\      "params": {
        \\        "node_filter": {"filter_prefix": "doc:"}
        \\      }
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const context = QueryBuilderTableContext{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    };
    const feedback = try metadataValidateQueryRequestAgainstContext(std.testing.allocator, &context, parsed.value, null);
    defer if (feedback) |value| std.testing.allocator.free(value);

    try std.testing.expect(feedback != null);
    try std.testing.expect(std.mem.indexOf(u8, feedback.?, "executor preflight") != null);
    try std.testing.expect(std.mem.indexOf(u8, feedback.?, "UnsupportedQueryRequest") != null);
}

test "query builder metadata validator rejects graph result refs without seed results" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "graph_searches": {
        \\    "graph_search": {
        \\      "type": "neighbors",
        \\      "index_name": "doc_graph",
        \\      "start_nodes": {"result_ref": "$full_text_results"}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const context = QueryBuilderTableContext{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    };
    const feedback = try metadataValidateQueryRequestAgainstContext(std.testing.allocator, &context, parsed.value, null);
    defer if (feedback) |value| std.testing.allocator.free(value);

    try std.testing.expect(feedback != null);
    try std.testing.expect(std.mem.indexOf(u8, feedback.?, "$full_text_results") != null);
    try std.testing.expect(std.mem.indexOf(u8, feedback.?, "full_text_search is absent") != null);
}

test "query builder preflight reports metadata diagnostics" {
    var parsed_query = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"match\":\"published\",\"field\":\"body\"}", .{});
    defer parsed_query.deinit();

    var collected = collectQueryBuilderContext(.{
        .schema_fields = &.{"body"},
    });
    var preflight = try preflightQueryRequest(std.testing.allocator, &collected, .{
        .intent = "find published docs",
    }, .{
        .full_text_search = parsed_query.value,
    }, null, "full_text", .{});
    defer preflight.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), preflight.diagnostics.len);
    try std.testing.expectEqual(QueryPreflightDiagnosticSeverity.@"error", preflight.diagnostics[0].severity);
    try std.testing.expectEqualStrings("missing_full_text_index", preflight.diagnostics[0].code);
    try std.testing.expectEqualStrings("query_request.full_text_search", preflight.diagnostics[0].path);
    try std.testing.expect(std.mem.indexOf(u8, preflight.diagnostics[0].message, "full-text index") != null);
}

test "query builder preflight plan mode summarizes bound indexes and result refs" {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryRequest, std.testing.allocator,
        \\{
        \\  "full_text_search": {"match":"raft","field":"body"},
        \\  "semantic_search": "raft",
        \\  "indexes": ["body_embedding"],
        \\  "limit": 5,
        \\  "fields": ["title"],
        \\  "aggregations": {
        \\    "by_status": {
        \\      "type": "terms",
        \\      "field": "status"
        \\    }
        \\  },
        \\  "graph_searches": {
        \\    "related": {
        \\      "type": "neighbors",
        \\      "index_name": "doc_graph",
        \\      "start_nodes": {"result_ref": "$fused_results", "limit": 3}
        \\    }
        \\  }
        \\}
    , .{});
    defer parsed.deinit();

    const full_text_fields = [_][]const u8{"body"};
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    var collected = collectQueryBuilderContext(.{
        .full_text_index_metadata = &full_text_metadata,
        .embedding_index_metadata = &.{.{ .name = "body_embedding" }},
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    });
    var preflight = try preflightQueryRequest(std.testing.allocator, &collected, .{
        .intent = "find related raft docs",
    }, parsed.value, null, "graph", .{
        .mode = .plan,
    });
    defer preflight.deinit(std.testing.allocator);

    try std.testing.expect(preflight.plan_summary != null);
    const plan_summary = preflight.plan_summary.?;
    try std.testing.expect(queryBuilderFieldInSlice(plan_summary.full_text_indexes, "search_idx"));
    try std.testing.expect(queryBuilderFieldInSlice(plan_summary.embedding_indexes, "body_embedding"));
    try std.testing.expect(queryBuilderFieldInSlice(plan_summary.graph_indexes, "doc_graph"));
    try std.testing.expect(queryBuilderFieldInSlice(plan_summary.result_refs, "$full_text_results"));
    try std.testing.expect(queryBuilderFieldInSlice(plan_summary.result_refs, "$embeddings_results"));
    try std.testing.expect(queryBuilderFieldInSlice(plan_summary.result_refs, "$fused_results"));
    try std.testing.expect(queryBuilderFieldInSlice(plan_summary.result_refs, "$graph_results.related"));
    try std.testing.expectEqual(@as(usize, 1), plan_summary.graph_query_order.len);
    try std.testing.expectEqualStrings("related", plan_summary.graph_query_order[0]);
    try std.testing.expectEqual(@as(u32, 5), plan_summary.requested_limit);
    try std.testing.expectEqual(@as(u32, 0), plan_summary.requested_offset);
    try std.testing.expectEqual(@as(u32, 2), plan_summary.base_result_set_count);
    try std.testing.expectEqual(@as(u32, 1), plan_summary.graph_query_count);
    try std.testing.expect(plan_summary.requires_fusion);
    try std.testing.expect(!plan_summary.count_only);
    try std.testing.expect(plan_summary.include_stored);
    try std.testing.expectEqual(@as(u32, 1), plan_summary.aggregation_count);
}

test "query builder collected context drives final validation" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_field":"status","require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    const full_text_fields = [_][]const u8{"body"};
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    var collected = collectQueryBuilderContext(.{
        .schema_fields = &.{ "body", "status" },
        .full_text_index_metadata = &full_text_metadata,
    });

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponseWithCollectedContext(arena_impl.allocator(), .{
        .intent = "find published documents",
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, &collected, null));
}

test "query builder preflight includes runtime validator diagnostics" {
    const RuntimeValidator = struct {
        fn iface() QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{ .validate_query_request = validateQueryRequest },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            return try alloc.dupe(u8, "runtime preflight rejected query");
        }
    };

    var parsed_query = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"match\":\"published\",\"field\":\"body\"}", .{});
    defer parsed_query.deinit();

    var collected = collectQueryBuilderContext(.{
        .schema_fields = &.{"body"},
        .full_text_index_metadata = &.{.{ .name = "search_idx", .fields = &.{"body"} }},
        .runtime_query_request_validator = RuntimeValidator.iface(),
    });
    var preflight = try preflightQueryRequest(std.testing.allocator, &collected, .{
        .intent = "find published docs",
    }, .{
        .full_text_search = parsed_query.value,
    }, null, "full_text", .{});
    defer preflight.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), preflight.diagnostics.len);
    try std.testing.expectEqualStrings("runtime_query_request_invalid", preflight.diagnostics[0].code);
    try std.testing.expectEqualStrings("query_request", preflight.diagnostics[0].path);
    try std.testing.expect(std.mem.indexOf(u8, preflight.diagnostics[0].message, "runtime preflight rejected query") != null);
}

test "query builder preflight estimate mode includes runtime summary" {
    const RuntimeValidator = struct {
        fn iface() QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .validate_query_request = validateQueryRequest,
                    .preflight_query_request = runtimePreflightQueryRequest,
                },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            return null;
        }

        fn runtimePreflightQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
            _: u32,
        ) !?db_mod.RuntimePreflightSummary {
            return .{
                .text_indexes = try alloc.dupe(db_mod.TextIndexEstimate, &.{.{
                    .name = try alloc.dupe(u8, "search_idx"),
                    .doc_count = 42,
                    .chunk_backed = true,
                    .group_chunk_parents = true,
                }}),
                .embedding_indexes = try alloc.dupe(db_mod.EmbeddingIndexEstimate, &.{.{
                    .name = try alloc.dupe(u8, "body_embedding"),
                    .doc_count = 42,
                    .dims = 384,
                    .chunk_backed = true,
                }}),
                .graph_indexes = try alloc.dupe(db_mod.GraphIndexEstimate, &.{.{
                    .name = try alloc.dupe(u8, "doc_graph"),
                    .edge_count = 12,
                    .node_count = 7,
                }}),
                .text_query_stats = try alloc.dupe(db_mod.TextFieldStats, &.{.{
                    .field = try alloc.dupe(u8, "body"),
                    .global_doc_count = 42,
                    .global_total_field_len = 420,
                    .term_doc_freqs = try alloc.dupe(db_mod.TermDocFreq, &.{.{
                        .term = try alloc.dupe(u8, "published"),
                        .doc_freq = 6,
                    }}),
                }}),
                .doc_id_value_count = 2,
                .filter_id_count = 3,
                .exclude_id_count = 1,
                .numeric_range_clause_count = 1,
                .term_range_clause_count = 1,
                .ip_range_clause_count = 1,
                .bool_field_clause_count = 1,
                .geo_filter_clause_count = 2,
                .positive_id_result_upper_bound = 2,
                .structured_filter_doc_count_estimate = 5,
                .structured_filter_count_exact = true,
                .shard_result_window = 6,
                .shard_result_window_total = 18,
                .stored_projection_doc_upper_bound_total = 18,
                .rerank_doc_upper_bound = 4,
                .aggregation_may_scan_full_results = true,
                .shard_count = 3,
                .remote_shard_count = 2,
                .dense_query_count = 1,
                .vector_worker_candidate_count = 1,
                .vector_worker_filter_constraint_count = 2,
                .vector_worker_requires_algebraic_filter_resolution = true,
                .dense_effective_k_total = 11,
                .dense_search_width_total = 2080,
                .dense_search_width_max = 2080,
                .dense_epsilon_max = 7.0,
            };
        }
    };

    var parsed_query = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"match\":\"published\",\"field\":\"body\"}", .{});
    defer parsed_query.deinit();

    var collected = collectQueryBuilderContext(.{
        .schema_fields = &.{"body"},
        .full_text_index_metadata = &.{.{ .name = "search_idx", .fields = &.{"body"} }},
        .runtime_query_request_validator = RuntimeValidator.iface(),
    });
    var preflight = try preflightQueryRequest(std.testing.allocator, &collected, .{
        .intent = "find published docs",
    }, .{
        .full_text_search = parsed_query.value,
    }, null, "full_text", .{ .mode = .estimate });
    defer preflight.deinit(std.testing.allocator);

    try std.testing.expect(preflight.estimate_summary != null);
    const estimate = preflight.estimate_summary.?;
    try std.testing.expectEqual(@as(usize, 1), estimate.text_indexes.len);
    try std.testing.expectEqualStrings("search_idx", estimate.text_indexes[0].name);
    try std.testing.expectEqual(@as(u64, 42), estimate.text_indexes[0].doc_count);
    try std.testing.expect(estimate.text_indexes[0].chunk_backed);
    try std.testing.expect(estimate.text_indexes[0].group_chunk_parents);
    try std.testing.expectEqual(@as(usize, 1), estimate.embedding_indexes.len);
    try std.testing.expectEqualStrings("body_embedding", estimate.embedding_indexes[0].name);
    try std.testing.expectEqual(@as(u32, 384), estimate.embedding_indexes[0].dims);
    try std.testing.expectEqual(@as(usize, 1), estimate.graph_indexes.len);
    try std.testing.expectEqualStrings("doc_graph", estimate.graph_indexes[0].name);
    try std.testing.expectEqual(@as(u64, 12), estimate.graph_indexes[0].edge_count);
    try std.testing.expectEqual(@as(u64, 7), estimate.graph_indexes[0].node_count);
    try std.testing.expectEqual(@as(usize, 1), estimate.text_query_stats.len);
    try std.testing.expectEqualStrings("body", estimate.text_query_stats[0].field);
    try std.testing.expectEqual(@as(u32, 42), estimate.text_query_stats[0].global_doc_count);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), estimate.text_query_stats[0].avg_doc_len, 0.001);
    try std.testing.expectEqual(@as(usize, 1), estimate.text_query_stats[0].term_doc_freqs.len);
    try std.testing.expectEqualStrings("published", estimate.text_query_stats[0].term_doc_freqs[0].term);
    try std.testing.expectEqual(@as(u32, 6), estimate.text_query_stats[0].term_doc_freqs[0].doc_freq);
    try std.testing.expectEqual(@as(u32, 2), estimate.doc_id_value_count);
    try std.testing.expectEqual(@as(u32, 3), estimate.filter_id_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.exclude_id_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.numeric_range_clause_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.term_range_clause_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.ip_range_clause_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.bool_field_clause_count);
    try std.testing.expectEqual(@as(u32, 2), estimate.geo_filter_clause_count);
    try std.testing.expectEqual(@as(?u32, 2), estimate.positive_id_result_upper_bound);
    try std.testing.expectEqual(@as(?u64, 5), estimate.structured_filter_doc_count_estimate);
    try std.testing.expect(estimate.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 6), estimate.text_result_upper_bound);
    try std.testing.expectEqual(@as(u64, 6), estimate.text_term_doc_freq_total);
    try std.testing.expectEqual(@as(?u64, 42), estimate.corpus_doc_count_estimate);
    try std.testing.expectEqual(@as(?u32, 2), estimate.result_doc_estimate);
    try std.testing.expectEqual(@as(?u32, 2), estimate.result_doc_upper_bound);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.EstimateKind.upper_bound, estimate.selectivity_estimate_kind);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.Confidence.high, estimate.selectivity_confidence);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0 / 42.0), estimate.selectivity_upper_bound_ratio.?, 0.001);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.SelectivityHeuristic.medium, estimate.selectivity_heuristic);
    try std.testing.expectEqual(@as(usize, 5), estimate.selectivity_risk_factors.len);
    try std.testing.expectEqualStrings("positive_id_bound", estimate.selectivity_risk_factors[0]);
    try std.testing.expectEqualStrings("exact_structured_filter_count", estimate.selectivity_risk_factors[1]);
    try std.testing.expectEqualStrings("corpus_size_available", estimate.selectivity_risk_factors[2]);
    try std.testing.expectEqualStrings("text_term_stats", estimate.selectivity_risk_factors[3]);
    try std.testing.expectEqualStrings("non_text_filters_present", estimate.selectivity_risk_factors[4]);
    try std.testing.expectEqual(@as(u32, 6), estimate.shard_result_window);
    try std.testing.expectEqual(@as(u64, 18), estimate.shard_result_window_total);
    try std.testing.expectEqual(@as(u64, 18), estimate.stored_projection_doc_upper_bound_total);
    try std.testing.expectEqual(@as(?u64, 2), estimate.effective_stored_projection_doc_estimate_total);
    try std.testing.expectEqual(@as(u64, 2), estimate.effective_stored_projection_doc_upper_bound_total);
    try std.testing.expectEqual(@as(u32, 4), estimate.rerank_doc_upper_bound);
    try std.testing.expectEqual(@as(?u32, 2), estimate.effective_rerank_doc_estimate);
    try std.testing.expectEqual(@as(u32, 2), estimate.effective_rerank_doc_upper_bound);
    try std.testing.expect(estimate.aggregation_may_scan_full_results);
    try std.testing.expectEqual(@as(?u32, 2), estimate.aggregation_second_pass_doc_estimate);
    try std.testing.expectEqual(@as(?u32, 2), estimate.aggregation_second_pass_doc_upper_bound);
    try std.testing.expectEqual(@as(usize, 4), estimate.latency_risk_factors.len);
    try std.testing.expectEqual(@as(usize, 4), estimate.latency_score_components.len);
    try std.testing.expectEqualStrings("remote_shards", estimate.latency_score_components[0].factor);
    try std.testing.expectEqual(@as(u32, 3), estimate.latency_score_components[0].points);
    try std.testing.expectEqualStrings("aggregation_second_pass", estimate.latency_score_components[1].factor);
    try std.testing.expectEqual(@as(u32, 3), estimate.latency_score_components[1].points);
    try std.testing.expectEqualStrings("reranking", estimate.latency_score_components[2].factor);
    try std.testing.expectEqual(@as(u32, 1), estimate.latency_score_components[2].points);
    try std.testing.expectEqualStrings("dense_search", estimate.latency_score_components[3].factor);
    try std.testing.expectEqual(@as(u32, 1), estimate.latency_score_components[3].points);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.EstimateKind.heuristic, estimate.latency_estimate_kind);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.Confidence.high, estimate.latency_confidence);
    try std.testing.expectEqual(@as(u32, 8), estimate.latency_heuristic_score);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.LatencyHeuristic.high, estimate.latency_heuristic);
    try std.testing.expectEqualStrings("remote_shards", estimate.latency_risk_factors[0]);
    try std.testing.expectEqualStrings("aggregation_second_pass", estimate.latency_risk_factors[1]);
    try std.testing.expectEqualStrings("reranking", estimate.latency_risk_factors[2]);
    try std.testing.expectEqualStrings("dense_search", estimate.latency_risk_factors[3]);
    try std.testing.expectEqual(@as(u32, 3), estimate.shard_count);
    try std.testing.expectEqual(@as(u32, 2), estimate.remote_shard_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.dense_query_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), estimate.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 2), estimate.vector_worker_filter_constraint_count);
    try std.testing.expect(estimate.vector_worker_requires_algebraic_filter_resolution);
    try std.testing.expectEqual(@as(u64, 11), estimate.dense_effective_k_total);
    try std.testing.expectEqual(@as(u64, 2080), estimate.dense_search_width_total);
    try std.testing.expectEqual(@as(u32, 2080), estimate.dense_search_width_max);
    try std.testing.expectApproxEqAbs(@as(f32, 7.0), estimate.dense_epsilon_max, 0.001);
}

test "query builder preflight estimate mode reports vector worker fallback risk" {
    const RuntimeValidator = struct {
        fn iface() QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .validate_query_request = validateQueryRequest,
                    .preflight_query_request = runtimePreflightQueryRequest,
                },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            return null;
        }

        fn runtimePreflightQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
            _: u32,
        ) !?db_mod.RuntimePreflightSummary {
            return .{
                .shard_count = 1,
                .dense_query_count = 1,
                .vector_worker_fallback_count = 1,
                .vector_worker_filter_constraint_count = 1,
                .vector_worker_requires_algebraic_filter_resolution = true,
                .dense_effective_k_total = 10,
                .dense_search_width_total = 640,
                .dense_search_width_max = 640,
                .dense_epsilon_max = 1.0,
            };
        }
    };

    var parsed_query = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"match":"raft","field":"body"}
    , .{});
    defer parsed_query.deinit();

    var collected = collectQueryBuilderContext(.{
        .runtime_query_request_validator = RuntimeValidator.iface(),
    });
    var preflight = try preflightQueryRequest(std.testing.allocator, &collected, .{
        .intent = "find docs with unsupported vector filter",
    }, .{
        .full_text_search = parsed_query.value,
    }, null, "full_text", .{ .mode = .estimate });
    defer preflight.deinit(std.testing.allocator);

    try std.testing.expect(preflight.estimate_summary != null);
    const estimate = preflight.estimate_summary.?;
    try std.testing.expectEqual(@as(u32, 0), estimate.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 1), estimate.vector_worker_filter_constraint_count);
    try std.testing.expect(estimate.vector_worker_requires_algebraic_filter_resolution);
    try std.testing.expectEqual(@as(usize, 3), estimate.latency_score_components.len);
    try std.testing.expectEqualStrings("vector_worker_fallback", estimate.latency_score_components[0].factor);
    try std.testing.expectEqual(@as(u32, 2), estimate.latency_score_components[0].points);
    try std.testing.expectEqualStrings("dense_search", estimate.latency_score_components[1].factor);
    try std.testing.expectEqualStrings("dense_search_width", estimate.latency_score_components[2].factor);
    try std.testing.expectEqual(@as(usize, 3), estimate.latency_risk_factors.len);
    try std.testing.expectEqualStrings("vector_worker_fallback", estimate.latency_risk_factors[0]);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.LatencyHeuristic.medium, estimate.latency_heuristic);
}

test "query builder preflight estimate mode derives text bounds and postings latency" {
    const RuntimeValidator = struct {
        fn iface() QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .validate_query_request = validateQueryRequest,
                    .preflight_query_request = runtimePreflightQueryRequest,
                },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            return null;
        }

        fn runtimePreflightQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
            _: u32,
        ) !?db_mod.RuntimePreflightSummary {
            return .{
                .text_indexes = try alloc.dupe(db_mod.TextIndexEstimate, &.{.{
                    .name = try alloc.dupe(u8, "search_idx"),
                    .doc_count = 1000,
                    .chunk_backed = false,
                    .group_chunk_parents = false,
                }}),
                .text_query_stats = try alloc.dupe(db_mod.TextFieldStats, &.{.{
                    .field = try alloc.dupe(u8, "body"),
                    .global_doc_count = 1000,
                    .global_total_field_len = 10_000,
                    .term_doc_freqs = try alloc.dupe(db_mod.TermDocFreq, &.{
                        .{
                            .term = try alloc.dupe(u8, "draft"),
                            .doc_freq = 180,
                        },
                        .{
                            .term = try alloc.dupe(u8, "published"),
                            .doc_freq = 120,
                        },
                    }),
                }}),
                .stored_projection_doc_upper_bound_total = 500,
                .shard_count = 1,
            };
        }
    };

    var parsed_query = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"match\":\"draft published\",\"field\":\"body\"}", .{});
    defer parsed_query.deinit();

    var collected = collectQueryBuilderContext(.{
        .schema_fields = &.{"body"},
        .full_text_index_metadata = &.{.{ .name = "search_idx", .fields = &.{"body"} }},
        .runtime_query_request_validator = RuntimeValidator.iface(),
    });
    var preflight = try preflightQueryRequest(std.testing.allocator, &collected, .{
        .intent = "find draft or published docs",
    }, .{
        .full_text_search = parsed_query.value,
    }, null, "full_text", .{ .mode = .estimate });
    defer preflight.deinit(std.testing.allocator);

    try std.testing.expect(preflight.estimate_summary != null);
    const estimate = preflight.estimate_summary.?;
    try std.testing.expectEqual(@as(?u32, 300), estimate.text_result_upper_bound);
    try std.testing.expectEqual(@as(u64, 300), estimate.text_term_doc_freq_total);
    try std.testing.expectEqual(@as(?u32, 300), estimate.result_doc_upper_bound);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.EstimateKind.upper_bound, estimate.selectivity_estimate_kind);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.Confidence.medium, estimate.selectivity_confidence);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), estimate.selectivity_upper_bound_ratio.?, 0.001);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.SelectivityHeuristic.broad, estimate.selectivity_heuristic);
    try std.testing.expectEqual(@as(usize, 4), estimate.selectivity_risk_factors.len);
    try std.testing.expectEqualStrings("no_positive_id_bound", estimate.selectivity_risk_factors[0]);
    try std.testing.expectEqualStrings("corpus_size_available", estimate.selectivity_risk_factors[1]);
    try std.testing.expectEqualStrings("text_term_stats", estimate.selectivity_risk_factors[2]);
    try std.testing.expectEqualStrings("text_term_bound", estimate.selectivity_risk_factors[3]);
    try std.testing.expectEqual(@as(?u32, null), estimate.result_doc_estimate);
    try std.testing.expectEqual(@as(?u64, null), estimate.effective_stored_projection_doc_estimate_total);
    try std.testing.expectEqual(@as(?u32, null), estimate.effective_rerank_doc_estimate);
    try std.testing.expectEqual(@as(u64, 300), estimate.effective_stored_projection_doc_upper_bound_total);
    try std.testing.expectEqual(@as(u32, 3), estimate.latency_heuristic_score);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.LatencyHeuristic.medium, estimate.latency_heuristic);
    try std.testing.expectEqual(@as(usize, 2), estimate.latency_score_components.len);
    try std.testing.expectEqualStrings("text_postings", estimate.latency_score_components[0].factor);
    try std.testing.expectEqual(@as(u32, 1), estimate.latency_score_components[0].points);
    try std.testing.expectEqualStrings("stored_projection", estimate.latency_score_components[1].factor);
    try std.testing.expectEqual(@as(u32, 2), estimate.latency_score_components[1].points);
    try std.testing.expectEqual(@as(usize, 2), estimate.latency_risk_factors.len);
    try std.testing.expectEqualStrings("text_postings", estimate.latency_risk_factors[0]);
    try std.testing.expectEqualStrings("stored_projection", estimate.latency_risk_factors[1]);
}

test "query builder preflight estimate mode reports structured count budget limits" {
    const RuntimeValidator = struct {
        fn iface() QueryBuilderRuntimeQueryRequestValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .validate_query_request = validateQueryRequest,
                    .preflight_query_request = runtimePreflightQueryRequest,
                },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
        ) !?[]const u8 {
            return null;
        }

        fn runtimePreflightQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryRequest,
            max_work: u32,
        ) !?db_mod.RuntimePreflightSummary {
            return .{
                .text_indexes = try alloc.dupe(db_mod.TextIndexEstimate, &.{.{
                    .name = try alloc.dupe(u8, "search_idx"),
                    .doc_count = 5000,
                    .chunk_backed = false,
                    .group_chunk_parents = false,
                }}),
                .numeric_range_clause_count = 1,
                .structured_filter_doc_count_lower_bound = max_work,
                .structured_filter_doc_count_sample_estimate = max_work + 250,
                .structured_filter_count_sample_size = @intCast(max_work),
                .structured_filter_count_budget_limit = max_work,
                .shard_count = 1,
            };
        }
    };

    var collected = collectQueryBuilderContext(.{
        .schema_fields = &.{"score"},
        .full_text_index_metadata = &.{.{ .name = "search_idx", .fields = &.{"score"} }},
        .runtime_query_request_validator = RuntimeValidator.iface(),
    });
    var preflight = try preflightQueryRequest(std.testing.allocator, &collected, .{
        .intent = "find high scores",
    }, .{
        .filter_query = try std.json.parseFromSliceLeaky(std.json.Value, std.testing.allocator, "{\"numeric_range\":{\"field\":\"score\",\"min\":10}}", .{}),
    }, null, "filter", .{ .mode = .estimate, .max_work = 1000 });
    defer preflight.deinit(std.testing.allocator);

    try std.testing.expect(preflight.estimate_summary != null);
    const estimate = preflight.estimate_summary.?;
    try std.testing.expectEqual(@as(?u64, 1000), estimate.structured_filter_doc_count_lower_bound);
    try std.testing.expectEqual(@as(?u64, 1250), estimate.structured_filter_doc_count_sample_estimate);
    try std.testing.expectEqual(@as(u32, 1000), estimate.structured_filter_count_sample_size);
    try std.testing.expectEqual(@as(?u64, 1000), estimate.structured_filter_count_budget_limit);
    try std.testing.expect(!estimate.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 1250), estimate.result_doc_estimate);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.EstimateKind.sampled, estimate.selectivity_estimate_kind);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.Confidence.medium, estimate.selectivity_confidence);
    try std.testing.expectApproxEqAbs(@as(f32, 0.2), estimate.selectivity_lower_bound_ratio.?, 0.001);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.SelectivityHeuristic.broad, estimate.selectivity_heuristic);
    try std.testing.expectEqual(@as(usize, 5), estimate.selectivity_risk_factors.len);
    try std.testing.expectEqualStrings("no_positive_id_bound", estimate.selectivity_risk_factors[0]);
    try std.testing.expectEqualStrings("structured_filter_probe_lower_bound", estimate.selectivity_risk_factors[1]);
    try std.testing.expectEqualStrings("structured_filter_count_budget_limited", estimate.selectivity_risk_factors[2]);
    try std.testing.expectEqualStrings("structured_filter_sample_estimate", estimate.selectivity_risk_factors[3]);
    try std.testing.expectEqualStrings("corpus_size_available", estimate.selectivity_risk_factors[4]);
    try std.testing.expectEqual(@as(u32, 4), estimate.latency_heuristic_score);
    try std.testing.expectEqual(QueryPreflightEstimateSummary.LatencyHeuristic.medium, estimate.latency_heuristic);
    try std.testing.expectEqual(@as(usize, 2), estimate.latency_score_components.len);
    try std.testing.expectEqualStrings("structured_filter_probe", estimate.latency_score_components[0].factor);
    try std.testing.expectEqual(@as(u32, 2), estimate.latency_score_components[0].points);
    try std.testing.expectEqualStrings("structured_filter_sample", estimate.latency_score_components[1].factor);
    try std.testing.expectEqual(@as(u32, 2), estimate.latency_score_components[1].points);
    try std.testing.expectEqual(@as(usize, 2), estimate.latency_risk_factors.len);
    try std.testing.expectEqualStrings("structured_filter_probe", estimate.latency_risk_factors[0]);
    try std.testing.expectEqualStrings("structured_filter_sample", estimate.latency_risk_factors[1]);
}

test "query builder require executable rejects assembled full text outside index metadata" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_field":"status","require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    const full_text_fields = [_][]const u8{"body"};
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    var context = QueryBuilderTableContext{
        .schema_fields = &.{ "body", "status" },
        .full_text_index_metadata = &full_text_metadata,
    };
    context.plan_validator = metadataBackedPlanValidator(&context);

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find published documents",
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, context, null));
}

test "query builder rejects generated fields outside schema and falls back" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8,
                    \\{"query":{"match":"raft","field":"missing_field"},"explanation":"bad field","confidence":0.99}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithGeneration(arena_impl.allocator(), .{
        .intent = "find raft",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "full_text",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, null, FakeGeneration.iface());

    try std.testing.expectEqualStrings("full_text", result.specialist.?);
    try std.testing.expect(result.query.object.get("match") != null);
    try std.testing.expectEqualStrings("body", result.query.object.get("field").?.string);
    try std.testing.expect(result.warnings != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "Generator-backed full-text query building failed") != null);
}

test "query builder generated full text honors allowed fields constraint" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8,
                    \\{"query":{"match":"raft","field":"title"},"explanation":"disallowed by constraints","confidence":0.99}
                ),
                .allocator = alloc,
            };
        }
    };

    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"allowed_fields":["body"]}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithGeneration(arena_impl.allocator(), .{
        .intent = "find raft",
        .schema_fields = &.{ "title", "body", "status" },
        .constraints = constraints_tree.value,
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, null, FakeGeneration.iface());

    try std.testing.expect(result.query.object.get("match") != null);
    try std.testing.expectEqualStrings("body", result.query.object.get("field").?.string);
    try std.testing.expect(result.warnings != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "Generator-backed full-text query building failed") != null);
}

test "query builder repairs invalid generated full text once" {
    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) GenerationRunner {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 2), messages.len);
                return .{
                    .content = try alloc.dupe(u8,
                        \\{"query":{"match":"raft","field":"missing_field"},"explanation":"bad field","confidence":0.99}
                    ),
                    .allocator = alloc,
                };
            }

            try std.testing.expectEqual(@as(usize, 3), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "failed validation") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "Return only the corrected JSON object") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"query":{"match_phrase":"raft consensus","field":"body"},"explanation":"Repaired to use a schema field.","confidence":0.88}
                ),
                .allocator = alloc,
            };
        }
    };

    var fake = FakeGeneration{};
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "full_text",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{}, fake.iface());

    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("full_text", result.specialist.?);
    try std.testing.expectEqualStrings("Repaired to use a schema field.", result.explanation.?);
    try std.testing.expect(result.query.object.get("match_phrase") != null);
    try std.testing.expectEqualStrings("body", result.query.object.get("field").?.string);
    if (result.warnings) |warnings| {
        for (warnings) |warning| {
            try std.testing.expect(std.mem.indexOf(u8, warning, "Generator-backed full-text query building failed") == null);
        }
    }
}

test "query builder repairs generated full text from plan validator feedback" {
    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) GenerationRunner {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 2), messages.len);
                return .{
                    .content = try alloc.dupe(u8,
                        \\{"query":{"match":"raft","field":"body"},"explanation":"validator will reject","confidence":0.99}
                    ),
                    .allocator = alloc,
                };
            }

            try std.testing.expectEqual(@as(usize, 3), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "Query-builder plan validation feedback") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "full-text validator rejected") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"query":{"match_phrase":"raft snapshot","field":"body"},"explanation":"Repaired from full-text validator feedback.","confidence":0.9}
                ),
                .allocator = alloc,
            };
        }
    };

    const FakeQueryBuilderPlanValidator = struct {
        calls: usize = 0,

        fn iface(self: *@This()) QueryBuilderPlanValidator {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &.{ .validate_bleve_query = validateBleveQuery },
            };
        }

        fn validateBleveQuery(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryBuilderRequest,
            query: std.json.Value,
        ) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (query.object.get("match") != null) {
                return try alloc.dupe(u8, "full-text validator rejected broad match query; use a phrase query");
            }
            try std.testing.expect(query.object.get("match_phrase") != null);
            return null;
        }
    };

    var fake_generation = FakeGeneration{};
    var fake_validator = FakeQueryBuilderPlanValidator{};
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "full_text",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .plan_validator = fake_validator.iface(),
    }, fake_generation.iface());

    try std.testing.expectEqual(@as(usize, 2), fake_generation.calls);
    try std.testing.expectEqual(@as(usize, 2), fake_validator.calls);
    try std.testing.expectEqualStrings("full_text", result.specialist.?);
    try std.testing.expectEqualStrings("Repaired from full-text validator feedback.", result.explanation.?);
    try std.testing.expect(result.query.object.get("match_phrase") != null);
    try std.testing.expectEqualStrings("body", result.query.object.get("field").?.string);
    if (result.warnings) |warnings| {
        for (warnings) |warning| {
            try std.testing.expect(std.mem.indexOf(u8, warning, "Generator-backed full-text query building failed") == null);
        }
    }
}

test "query builder uses generated graph specialist when runner is provided" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            chain: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            try std.testing.expectEqual(@as(usize, 1), chain.len);
            try std.testing.expectEqualStrings("local-generator", chain[0].generator.model);
            try std.testing.expectEqual(@as(usize, 2), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "GraphQuery") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "doc_graph") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "references (graph)") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[0].content, "parent (tree)") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "related raft documents") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"related":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$full_text_results","limit":5},"params":{"edge_types":["references"],"max_depth":1},"fields":["title"]}},"explanation":"Expands from lexical matches through reference edges.","confidence":0.87,"warnings":["used table graph index"]}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const graph_edge_types = [_]QueryBuilderGraphEdgeType{
        .{ .name = "references", .topology = "graph" },
        .{ .name = "parent", .topology = "tree" },
    };
    const graph_metadata = [_]QueryBuilderGraphIndex{.{
        .name = "doc_graph",
        .edge_types = &graph_edge_types,
    }};
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{ "title", "body" },
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &graph_metadata,
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    try std.testing.expectEqualStrings("Expands from lexical matches through reference edges.", result.explanation.?);
    try std.testing.expectEqual(@as(f64, 0.87), result.confidence.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("related").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, graph_query.type);
    try std.testing.expectEqualStrings("doc_graph", graph_query.index_name);
    try std.testing.expectEqualStrings("$full_text_results", graph_query.start_nodes.?.result_ref.?);
    try std.testing.expectEqualStrings("references", graph_query.params.?.edge_types.?[0]);
    try std.testing.expectEqualStrings("title", graph_query.fields.?[0]);
    try std.testing.expectEqualStrings("used table graph index", result.warnings.?[0]);
}

test "query builder repairs invalid generated graph plan once" {
    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) GenerationRunner {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 2), messages.len);
                return .{
                    .content = try alloc.dupe(u8,
                        \\{"graph_searches":{"bad":{"type":"neighbors","index_name":"missing_graph","start_nodes":{"result_ref":"$full_text_results"}}},"explanation":"bad index","confidence":0.99}
                    ),
                    .allocator = alloc,
                };
            }

            try std.testing.expectEqual(@as(usize, 3), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "failed deterministic validation") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "Regenerate a valid response") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"repaired":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$full_text_results","limit":3},"params":{"edge_types":["references"],"max_depth":1}}},"explanation":"Repaired to use the table graph index.","confidence":0.72}
                ),
                .allocator = alloc,
            };
        }
    };

    var fake = FakeGeneration{};
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, fake.iface());

    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("graph", result.specialist.?);
    try std.testing.expectEqualStrings("Repaired to use the table graph index.", result.explanation.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("repaired").?;
    try std.testing.expectEqualStrings("doc_graph", graph_query.index_name);
    try std.testing.expectEqualStrings("$full_text_results", graph_query.start_nodes.?.result_ref.?);
    try std.testing.expectEqualStrings("references", graph_query.params.?.edge_types.?[0]);
    if (result.warnings) |warnings| {
        for (warnings) |warning| {
            try std.testing.expect(std.mem.indexOf(u8, warning, "Generator-backed graph query building failed") == null);
        }
    }
}

test "query builder repairs generated graph plan with unavailable seed ref" {
    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) GenerationRunner {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 2), messages.len);
                return .{
                    .content = try alloc.dupe(u8,
                        \\{"graph_searches":{"bad_seed":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$embeddings_results"},"params":{"edge_types":["references"],"max_depth":1}}},"explanation":"bad seed","confidence":0.99}
                    ),
                    .allocator = alloc,
                };
            }

            try std.testing.expectEqual(@as(usize, 3), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "Query-builder plan validation feedback") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "$embeddings_results") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"related":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$full_text_results","limit":3},"params":{"edge_types":["references"],"max_depth":1}}},"explanation":"Repaired to use the available lexical seed.","confidence":0.76}
                ),
                .allocator = alloc,
            };
        }
    };

    var fake = FakeGeneration{};
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, fake.iface());

    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("graph", result.specialist.?);
    try std.testing.expectEqualStrings("Repaired to use the available lexical seed.", result.explanation.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("related").?;
    try std.testing.expectEqualStrings("$full_text_results", graph_query.start_nodes.?.result_ref.?);
}

test "query builder accepts generated graph result dependencies" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            try std.testing.expectEqual(@as(usize, 2), messages.len);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"seed":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$full_text_results","limit":4},"params":{"edge_types":["references"],"max_depth":1}},"expand_seed":{"type":"traverse","index_name":"doc_graph","start_nodes":{"result_ref":"$graph_results.seed","limit":6},"params":{"edge_types":["links"],"max_depth":2}}},"explanation":"Chains graph expansion from a generated seed set.","confidence":0.78}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents and expand them",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    try std.testing.expectEqualStrings("Chains graph expansion from a generated seed set.", result.explanation.?);
    const graph_searches = result.query_request.?.graph_searches.?;
    try std.testing.expect(graph_searches.map.get("seed") != null);
    const dependent = graph_searches.map.get("expand_seed").?;
    try std.testing.expectEqualStrings("$graph_results.seed", dependent.start_nodes.?.result_ref.?);
    try std.testing.expectEqualStrings("links", dependent.params.?.edge_types.?[0]);
}

test "query builder repairs generated graph plan from validator feedback" {
    const FakeGeneration = struct {
        calls: usize = 0,

        fn iface(self: *@This()) GenerationRunner {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 2), messages.len);
                return .{
                    .content = try alloc.dupe(u8,
                        \\{"graph_searches":{"related":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$full_text_results"},"params":{"edge_types":["missing_edge"],"max_depth":1}}},"explanation":"Uses an unavailable edge type.","confidence":0.8}
                    ),
                    .allocator = alloc,
                };
            }

            try std.testing.expectEqual(@as(usize, 3), messages.len);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "Query-builder plan validation feedback") != null);
            try std.testing.expect(std.mem.indexOf(u8, messages[2].content, "missing_edge") != null);
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"related":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$full_text_results"},"params":{"edge_types":["references"],"max_depth":1}}},"explanation":"Uses the available reference edge type.","confidence":0.84}
                ),
                .allocator = alloc,
            };
        }
    };

    var fake_generation = FakeGeneration{};
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const graph_edge_types = [_]QueryBuilderGraphEdgeType{.{
        .name = "references",
        .topology = "graph",
    }};
    const graph_metadata = [_]QueryBuilderGraphIndex{.{
        .name = "doc_graph",
        .edge_types = &graph_edge_types,
    }};
    var table_context = QueryBuilderTableContext{
        .graph_index_metadata = &graph_metadata,
    };
    table_context.plan_validator = metadataBackedPlanValidator(&table_context);
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, table_context, fake_generation.iface());

    try std.testing.expectEqual(@as(usize, 2), fake_generation.calls);
    try std.testing.expectEqualStrings("Uses the available reference edge type.", result.explanation.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("related").?;
    try std.testing.expectEqualStrings("references", graph_query.params.?.edge_types.?[0]);
}

test "query builder appends final plan validator feedback" {
    const FakeQueryBuilderPlanValidator = struct {
        fn iface() QueryBuilderPlanValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{ .validate_query_request = validateQueryRequest },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryBuilderRequest,
            query_request: metadata_openapi.QueryRequest,
            _: ?metadata_openapi.RetrievalQueryRequest,
            specialist: []const u8,
        ) !?[]const u8 {
            try std.testing.expectEqualStrings("full_text", specialist);
            try std.testing.expectEqualStrings("docs", query_request.table.?);
            return try alloc.dupe(u8, "query request validator checked full-text field/index compatibility");
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft documents",
        .schema_fields = &.{"body"},
    }, .{
        .plan_validator = FakeQueryBuilderPlanValidator.iface(),
    }, null);

    try std.testing.expect(result.warnings != null);
    try std.testing.expectEqualStrings("query request validator checked full-text field/index compatibility", result.warnings.?[0]);
}

test "query builder require executable rejects final plan validator feedback" {
    const FakeQueryBuilderPlanValidator = struct {
        fn iface() QueryBuilderPlanValidator {
            return .{
                .ptr = undefined,
                .vtable = &.{ .validate_query_request = validateQueryRequest },
            };
        }

        fn validateQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: metadata_openapi.QueryBuilderRequest,
            _: metadata_openapi.QueryRequest,
            _: ?metadata_openapi.RetrievalQueryRequest,
            _: []const u8,
        ) !?[]const u8 {
            return try alloc.dupe(u8, "query request validator rejected final plan");
        }
    };

    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft documents",
        .schema_fields = &.{"body"},
        .constraints = constraints_tree.value,
    }, .{
        .plan_validator = FakeQueryBuilderPlanValidator.iface(),
    }, null));
}

test "query builder rejects generated graph missing result dependency" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"expand_missing":{"type":"traverse","index_name":"doc_graph","start_nodes":{"result_ref":"$graph_results.missing"},"params":{"edge_types":["links"],"max_depth":2}}},"explanation":"bad dependency","confidence":0.99}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    try std.testing.expect(result.query_request.?.graph_searches.?.map.get("graph_search") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "Generator-backed graph query building failed") != null);
}

test "query builder rejects generated graph cyclic result dependencies" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"a":{"type":"traverse","index_name":"doc_graph","start_nodes":{"result_ref":"$graph_results.b"},"params":{"max_depth":1}},"b":{"type":"traverse","index_name":"doc_graph","start_nodes":{"result_ref":"$graph_results.a"},"params":{"max_depth":1}}},"explanation":"bad cycle","confidence":0.99}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    try std.testing.expect(result.query_request.?.graph_searches.?.map.get("graph_search") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "Generator-backed graph query building failed") != null);
}

test "query builder rejects generated graph indexes outside context and falls back" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"bad":{"type":"neighbors","index_name":"missing_graph","start_nodes":{"result_ref":"$full_text_results"}}},"explanation":"bad index","confidence":0.99}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqualStrings("doc_graph", graph_query.index_name);
    try std.testing.expect(result.warnings != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "Generator-backed graph query building failed") != null);
}

test "query builder rejects generated graph unsupported result refs" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"bad":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"result_ref":"$unknown_results"}}},"explanation":"bad ref","confidence":0.99}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    try std.testing.expect(result.query_request.?.graph_searches.?.map.get("graph_search") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "Generator-backed graph query building failed") != null);
}

test "query builder rejects generated graph malformed patterns" {
    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const generating.ChainLink,
            _: []const generating.ChatMessage,
        ) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8,
                    \\{"graph_searches":{"bad":{"type":"pattern","index_name":"doc_graph","start_nodes":{"keys":["doc:a"]},"pattern":[{"alias":"a"},{"alias":"a","edge":{"types":["links"],"min_hops":2,"max_hops":1}}],"return_aliases":["missing"]}},"explanation":"bad pattern","confidence":0.99}
                ),
                .allocator = alloc,
            };
        }
    };

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find two hop links from doc:a",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .generator = .{
            .provider = .antfly,
            .model = "local-generator",
            .api_url = "http://127.0.0.1:8082",
        },
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, FakeGeneration.iface());

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.pattern, graph_query.type);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "Generator-backed graph query building failed") != null);
}

test "query builder assembles semantic query from preferred indexes" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_indexes":["body_embedding"],"limit":4}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expectEqualStrings("find raft architecture", result.query_request.?.semantic_search.?);
    try std.testing.expectEqualStrings("body_embedding", result.query_request.?.indexes.?[0]);
    try std.testing.expectEqual(@as(i64, 4), result.query_request.?.limit.?);
    try std.testing.expect(result.query_request.?.full_text_search == null);
}

test "query builder infers semantic indexes from table context" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
    }, .{
        .embedding_index_metadata = &.{.{ .name = "body_embedding" }},
    }, null);

    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expectEqualStrings("find raft architecture", result.query_request.?.semantic_search.?);
    try std.testing.expectEqualStrings("body_embedding", result.query_request.?.indexes.?[0]);
    try std.testing.expect(result.query_request.?.full_text_search == null);
}

test "query builder preserves legacy flat semantic indexes without structured metadata" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
    }, .{
        .semantic_indexes = &.{"legacy_embedding"},
    }, null);

    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expectEqualStrings("legacy_embedding", result.query_request.?.indexes.?[0]);
}

test "query builder preferred indexes override table context" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_indexes":["preferred_embedding"]}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .constraints = constraints_tree.value,
    }, .{
        .embedding_index_metadata = &.{.{ .name = "table_embedding" }},
    }, null);

    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expectEqualStrings("preferred_embedding", result.query_request.?.indexes.?[0]);
}

test "query builder warns when preferred semantic index is sparse metadata" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_indexes":["sparse_idx"]}
    , .{});
    defer constraints_tree.deinit();

    var context = QueryBuilderTableContext{
        .embedding_index_metadata = &.{
            .{ .name = "dense_idx", .dimension = 384, .model = "e5-small" },
            .{ .name = "sparse_idx", .sparse = true, .model = "splade" },
        },
    };
    context.plan_validator = metadataBackedPlanValidator(&context);

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .constraints = constraints_tree.value,
    }, context, null);

    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expectEqualStrings("sparse_idx", result.query_request.?.indexes.?[0]);
    try std.testing.expect(result.warnings != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "sparse embedding index 'sparse_idx'") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "splade") != null);
}

test "query builder require executable rejects sparse semantic index metadata" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_indexes":["sparse_idx"],"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var context = QueryBuilderTableContext{
        .embedding_index_metadata = &.{
            .{ .name = "dense_idx", .dimension = 384, .model = "e5-small" },
            .{ .name = "sparse_idx", .sparse = true, .model = "splade" },
        },
    };
    context.plan_validator = metadataBackedPlanValidator(&context);

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .constraints = constraints_tree.value,
    }, context, null));
}

test "query builder assembles hybrid query with semantic indexes and status filter" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"prefer_indexes":["body_embedding"]}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find published raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "hybrid",
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqualStrings("hybrid", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expectEqualStrings("find published raft architecture", result.query_request.?.semantic_search.?);
    try std.testing.expectEqualStrings("body_embedding", result.query_request.?.indexes.?[0]);
    try std.testing.expect(result.query_request.?.full_text_search != null);
    try std.testing.expect(result.query_request.?.filter_query != null);
}

test "query builder converts explicit dates into range filter" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture after 2024-01-01 before 2025-01-01",
        .schema_fields = &.{ "body", "published_at", "status" },
        .mode = "full_text",
    }, null);

    try std.testing.expect(result.query_request.?.filter_query != null);
    const filter = result.query_request.?.filter_query.?.object;
    try std.testing.expectEqualStrings("published_at", filter.get("field").?.string);
    try std.testing.expectEqualStrings("2024-01-01", filter.get("start").?.string);
    try std.testing.expectEqualStrings("2025-01-01", filter.get("end").?.string);
    try std.testing.expectEqual(false, filter.get("inclusive_start").?.bool);
    try std.testing.expectEqual(false, filter.get("inclusive_end").?.bool);
}

test "query builder combines status and date filters" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find published raft architecture since 2024-01-01",
        .schema_fields = &.{ "body", "published_at", "status" },
        .mode = "full_text",
    }, null);

    try std.testing.expect(result.query_request.?.filter_query != null);
    const conjuncts = result.query_request.?.filter_query.?.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), conjuncts.len);
    try std.testing.expectEqualStrings("status", conjuncts[0].object.get("field").?.string);
    try std.testing.expectEqualStrings("published", conjuncts[0].object.get("term").?.string);
    try std.testing.expectEqualStrings("published_at", conjuncts[1].object.get("field").?.string);
    try std.testing.expectEqualStrings("2024-01-01", conjuncts[1].object.get("start").?.string);
    try std.testing.expectEqual(true, conjuncts[1].object.get("inclusive_start").?.bool);
}

test "query builder converts explicit field constraints into term filters" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture tenant_id:acme type=article",
        .schema_fields = &.{ "body", "tenant_id", "type", "status" },
        .mode = "full_text",
    }, null);

    try std.testing.expect(result.query_request.?.filter_query != null);
    const conjuncts = result.query_request.?.filter_query.?.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), conjuncts.len);
    try std.testing.expectEqualStrings("tenant_id", conjuncts[0].object.get("field").?.string);
    try std.testing.expectEqualStrings("acme", conjuncts[0].object.get("term").?.string);
    try std.testing.expectEqualStrings("type", conjuncts[1].object.get("field").?.string);
    try std.testing.expectEqualStrings("article", conjuncts[1].object.get("term").?.string);
}

test "query builder converts status exclusions into must not filters" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture not archived",
        .schema_fields = &.{ "body", "status" },
        .mode = "full_text",
    }, null);

    try std.testing.expect(result.query_request.?.filter_query != null);
    const must_not = result.query_request.?.filter_query.?.object.get("must_not").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), must_not.len);
    try std.testing.expectEqualStrings("status", must_not[0].object.get("field").?.string);
    try std.testing.expectEqualStrings("archived", must_not[0].object.get("term").?.string);
}

test "query builder maps structured constraint filters and exclusions" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"filter_prefix":"tenant:acme:","filters":{"tenant_id":"acme","category":["tech","ai"],"published_at":{"gte":"2024-01-01","lt":"2025-01-01"}},"exclude":{"status":"archived"}}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "tenant_id", "category", "published_at", "status" },
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, null);

    const query_request = result.query_request.?;
    try std.testing.expectEqualStrings("tenant:acme:", query_request.filter_prefix.?);
    try std.testing.expect(query_request.filter_query != null);
    const conjuncts = query_request.filter_query.?.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), conjuncts.len);
    try std.testing.expectEqualStrings("tenant_id", conjuncts[0].object.get("field").?.string);
    try std.testing.expectEqualStrings("acme", conjuncts[0].object.get("term").?.string);
    const category_disjuncts = conjuncts[1].object.get("disjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), category_disjuncts.len);
    try std.testing.expectEqualStrings("category", category_disjuncts[0].object.get("field").?.string);
    try std.testing.expectEqualStrings("tech", category_disjuncts[0].object.get("term").?.string);
    try std.testing.expectEqualStrings("published_at", conjuncts[2].object.get("field").?.string);
    try std.testing.expectEqualStrings("2024-01-01", conjuncts[2].object.get("start").?.string);
    try std.testing.expectEqualStrings("2025-01-01", conjuncts[2].object.get("end").?.string);
    try std.testing.expectEqual(true, conjuncts[2].object.get("inclusive_start").?.bool);
    try std.testing.expectEqual(false, conjuncts[2].object.get("inclusive_end").?.bool);
    try std.testing.expect(query_request.exclusion_query != null);
    try std.testing.expectEqualStrings("status", query_request.exclusion_query.?.object.get("field").?.string);
    try std.testing.expectEqualStrings("archived", query_request.exclusion_query.?.object.get("term").?.string);
}

test "query builder require executable rejects filters outside full text index metadata" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"filters":{"tenant_id":"acme"},"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    const full_text_fields = [_][]const u8{"body"};
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    var context = QueryBuilderTableContext{
        .schema_fields = &.{ "body", "tenant_id" },
        .full_text_index_metadata = &full_text_metadata,
    };
    context.plan_validator = metadataBackedPlanValidator(&context);

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, context, null));
}

test "query builder filters structured constraints by allowed fields" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"filters":{"tenant_id":"acme","secret":"hidden"},"exclude":{"secret":"hidden"},"allowed_fields":["tenant_id"]}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "tenant_id", "secret" },
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, null);

    const query_request = result.query_request.?;
    try std.testing.expect(query_request.filter_query != null);
    try std.testing.expectEqualStrings("tenant_id", query_request.filter_query.?.object.get("field").?.string);
    try std.testing.expectEqualStrings("acme", query_request.filter_query.?.object.get("term").?.string);
    try std.testing.expect(query_request.exclusion_query == null);
}

test "query builder auto asks for strategy when lexical and semantic are viable" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "auto",
        .interactive = true,
        .max_user_clarifications = 1,
    }, .{
        .embedding_index_metadata = &.{.{ .name = "body_embedding" }},
    }, null);

    try std.testing.expectEqual(AgentStatus.clarification_required, result.status.?);
    try std.testing.expect(result.questions != null);
    try std.testing.expectEqualStrings("select_query_strategy", result.questions.?[0].id);
    try std.testing.expectEqual(metadata_openapi.AgentQuestionKind.single_choice, result.questions.?[0].kind);
    try std.testing.expectEqualStrings("hybrid", result.questions.?[0].options.?[0]);
    try std.testing.expectEqualStrings("full_text", result.questions.?[0].options.?[1]);
    try std.testing.expectEqualStrings("semantic", result.questions.?[0].options.?[2]);
    try std.testing.expectEqualStrings("hybrid", result.questions.?[0].default_answer.?);
    try std.testing.expectEqualStrings("hybrid", result.specialist.?);
    try std.testing.expect(result.query_request.?.semantic_search != null);
}

test "query builder auto uses strategy decision answer" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "status" },
        .mode = "auto",
        .interactive = true,
        .max_user_clarifications = 1,
        .decisions = &.{.{
            .question_id = "select_query_strategy",
            .answer = .{ .string = "full_text" },
        }},
    }, .{
        .embedding_index_metadata = &.{.{ .name = "body_embedding" }},
    }, null);

    try std.testing.expectEqual(AgentStatus.completed, result.status.?);
    try std.testing.expect(result.questions == null);
    try std.testing.expectEqualStrings("full_text", result.specialist.?);
    try std.testing.expect(result.query_request.?.semantic_search == null);
    try std.testing.expect(result.query_request.?.full_text_search != null);
}

test "query builder semantic mode without preferred indexes falls back to full text" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
    }, null);

    try std.testing.expectEqualStrings("full_text", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expect(result.query_request.?.semantic_search == null);
    try std.testing.expect(result.query_request.?.full_text_search != null);
    try std.testing.expect(result.warnings != null);
    try std.testing.expect(std.mem.indexOf(u8, result.warnings.?[0], "prefer_indexes") != null);
}

test "query builder asks for semantic index when interactive context is missing" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .interactive = true,
        .max_user_clarifications = 1,
    }, null);

    try std.testing.expectEqual(AgentStatus.clarification_required, result.status.?);
    try std.testing.expect(result.questions != null);
    try std.testing.expectEqualStrings("select_semantic_index", result.questions.?[0].id);
    try std.testing.expectEqual(metadata_openapi.AgentQuestionKind.free_text, result.questions.?[0].kind);
    try std.testing.expectEqualStrings("full_text", result.specialist.?);
    try std.testing.expect(result.query_request.?.semantic_search == null);
}

test "query builder uses semantic index decision answer" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .interactive = true,
        .max_user_clarifications = 1,
        .decisions = &.{.{
            .question_id = "select_semantic_index",
            .answer = .{ .string = "body_embedding" },
        }},
    }, null);

    try std.testing.expectEqual(AgentStatus.completed, result.status.?);
    try std.testing.expect(result.questions == null);
    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expectEqualStrings("body_embedding", result.query_request.?.indexes.?[0]);
    try std.testing.expectEqualStrings("find raft architecture", result.query_request.?.semantic_search.?);
}

test "query builder asks for text field when full text target is ambiguous" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "full_text",
        .interactive = true,
        .max_user_clarifications = 1,
    }, null);

    try std.testing.expectEqual(AgentStatus.clarification_required, result.status.?);
    try std.testing.expect(result.questions != null);
    try std.testing.expectEqualStrings("select_text_field", result.questions.?[0].id);
    try std.testing.expectEqual(metadata_openapi.AgentQuestionKind.single_choice, result.questions.?[0].kind);
    try std.testing.expectEqualStrings("title", result.questions.?[0].options.?[0]);
    try std.testing.expectEqualStrings("body", result.questions.?[0].options.?[1]);
    try std.testing.expectEqualStrings("body", result.questions.?[0].default_answer.?);
}

test "query builder uses text field decision answer" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "full_text",
        .interactive = true,
        .max_user_clarifications = 1,
        .decisions = &.{.{
            .question_id = "select_text_field",
            .answer = .{ .string = "title" },
        }},
    }, null);

    try std.testing.expectEqual(AgentStatus.completed, result.status.?);
    try std.testing.expect(result.questions == null);
    try std.testing.expectEqualStrings("title", result.query.object.get("field").?.string);
    try std.testing.expectEqualStrings("title", result.query_request.?.full_text_search.?.object.get("field").?.string);
}

test "query builder deterministic field selection honors allowed fields" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"allowed_fields":["title"],"prefer_field":"body"}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "title", "status" },
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqualStrings("title", result.query.object.get("field").?.string);
    try std.testing.expectEqualStrings("title", result.query_request.?.full_text_search.?.object.get("field").?.string);
}

test "query builder applies projection sort and pagination constraints" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"fields":["title","status"],"order_by":["-published_at","title asc"],"offset":5,"limit":3,"count":true,"profile":"true"}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "title", "status", "published_at" },
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, null);

    const query_request = result.query_request.?;
    try std.testing.expectEqual(@as(usize, 2), query_request.fields.?.len);
    try std.testing.expectEqualStrings("title", query_request.fields.?[0]);
    try std.testing.expectEqualStrings("status", query_request.fields.?[1]);
    try std.testing.expectEqual(@as(i64, 3), query_request.limit.?);
    try std.testing.expectEqual(@as(i64, 5), query_request.offset.?);
    try std.testing.expectEqual(true, query_request.count.?);
    try std.testing.expectEqual(true, query_request.profile.?);
    try std.testing.expectEqual(@as(usize, 2), query_request.order_by.?.len);
    try std.testing.expectEqualStrings("published_at", query_request.order_by.?[0].field);
    try std.testing.expectEqual(true, query_request.order_by.?[0].desc.?);
    try std.testing.expectEqualStrings("title", query_request.order_by.?[1].field);
    try std.testing.expectEqual(false, query_request.order_by.?[1].desc.?);
}

test "query builder require executable rejects sort outside full text index metadata" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"order_by":["published_at"],"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    const full_text_fields = [_][]const u8{"body"};
    const full_text_metadata = [_]QueryBuilderFullTextIndex{.{
        .name = "search_idx",
        .fields = &full_text_fields,
    }};
    var context = QueryBuilderTableContext{
        .schema_fields = &.{ "body", "published_at" },
        .full_text_index_metadata = &full_text_metadata,
    };
    context.plan_validator = metadataBackedPlanValidator(&context);

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, context, null));
}

test "query builder filters projection and sort constraints by allowed fields" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"fields":["title","secret"],"order_by":[{"field":"secret","desc":true},{"field":"title","direction":"asc"}],"allowed_fields":["title"]}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "title", "secret" },
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, null);

    const query_request = result.query_request.?;
    try std.testing.expectEqual(@as(usize, 1), query_request.fields.?.len);
    try std.testing.expectEqualStrings("title", query_request.fields.?[0]);
    try std.testing.expectEqual(@as(usize, 1), query_request.order_by.?.len);
    try std.testing.expectEqualStrings("title", query_request.order_by.?[0].field);
    try std.testing.expectEqual(false, query_request.order_by.?[0].desc.?);
}

test "query builder applies search cursor only when sort is present" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"order_by":["published_at"],"offset":5,"search_after":["2025-01-01","doc-9"]}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "published_at" },
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, null);

    const query_request = result.query_request.?;
    try std.testing.expect(query_request.order_by != null);
    try std.testing.expect(query_request.offset == null);
    try std.testing.expectEqual(@as(usize, 2), query_request.search_after.?.len);
    try std.testing.expectEqualStrings("2025-01-01", query_request.search_after.?[0]);
    try std.testing.expectEqualStrings("doc-9", query_request.search_after.?[1]);
}

test "query builder maps graph searches from constraints" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"graph_searches":{"related":{"type":"neighbors","index_name":"doc_graph","start_nodes":{"keys":["doc-1"]},"params":{"edge_types":["references"],"max_depth":1},"include_edges":true}},"expand_strategy":"union"}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "title" },
        .mode = "full_text",
        .constraints = constraints_tree.value,
    }, null);

    const query_request = result.query_request.?;
    try std.testing.expectEqualStrings("union", query_request.expand_strategy.?);
    try std.testing.expect(query_request.graph_searches != null);
    try std.testing.expectEqual(@as(usize, 1), query_request.graph_searches.?.map.count());
    const graph_query = query_request.graph_searches.?.map.get("related").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, graph_query.type);
    try std.testing.expectEqualStrings("doc_graph", graph_query.index_name);
    try std.testing.expectEqualStrings("doc-1", graph_query.start_nodes.?.keys.?[0]);
    try std.testing.expectEqualStrings("references", graph_query.params.?.edge_types.?[0]);
    try std.testing.expectEqual(true, graph_query.include_edges.?);
}

test "query builder infers graph search from table context" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, null);

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.traverse, graph_query.type);
    try std.testing.expectEqualStrings("doc_graph", graph_query.index_name);
    try std.testing.expectEqualStrings("$full_text_results", graph_query.start_nodes.?.result_ref.?);
    try std.testing.expectEqual(@as(i64, 2), graph_query.params.?.max_depth.?);
    try std.testing.expect(graph_query.include_edges == null);
}

test "query builder infers dense graph seed from semantic results" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"graph_index":"doc_graph"}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "semantic",
        .constraints = constraints_tree.value,
    }, .{
        .embedding_index_metadata = &.{.{ .name = "body_embedding" }},
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, null);

    try std.testing.expectEqualStrings("semantic", result.specialist.?);
    try std.testing.expect(result.query_request.?.semantic_search != null);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqualStrings("$embeddings_results", graph_query.start_nodes.?.result_ref.?);
}

test "query builder preserves legacy flat graph indexes without structured metadata" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
    }, .{
        .graph_indexes = &.{"legacy_graph"},
    }, null);

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqualStrings("legacy_graph", graph_query.index_name);
}

test "query builder maps graph shorthand constraints" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"graph_index":"doc_graph","graph_start_nodes":"doc:a, doc:b","graph_edge_types":["references"],"graph_max_depth":1}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "show neighbor documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, graph_query.type);
    try std.testing.expectEqualStrings("doc_graph", graph_query.index_name);
    try std.testing.expectEqualStrings("doc:a", graph_query.start_nodes.?.keys.?[0]);
    try std.testing.expectEqualStrings("doc:b", graph_query.start_nodes.?.keys.?[1]);
    try std.testing.expectEqualStrings("references", graph_query.params.?.edge_types.?[0]);
    try std.testing.expectEqual(@as(i64, 1), graph_query.params.?.max_depth.?);
}

test "query builder infers graph path nodes and edge type from intent" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find reference path from doc:a to doc:b",
        .schema_fields = &.{"body"},
        .mode = "graph",
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, null);

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.shortest_path, graph_query.type);
    try std.testing.expectEqualStrings("doc:a", graph_query.start_nodes.?.keys.?[0]);
    try std.testing.expectEqualStrings("doc:b", graph_query.target_nodes.?.keys.?[0]);
    try std.testing.expectEqualStrings("references", graph_query.params.?.edge_types.?[0]);
    try std.testing.expectEqual(true, graph_query.params.?.include_paths.?);
}

test "query builder infers graph between nodes and dependency edge type" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "route between service:a and service:b through dependencies",
        .schema_fields = &.{"body"},
        .mode = "graph",
    }, .{
        .graph_index_metadata = &.{.{ .name = "service_graph" }},
    }, null);

    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.shortest_path, graph_query.type);
    try std.testing.expectEqualStrings("service:a", graph_query.start_nodes.?.keys.?[0]);
    try std.testing.expectEqualStrings("service:b", graph_query.target_nodes.?.keys.?[0]);
    try std.testing.expectEqualStrings("depends_on", graph_query.params.?.edge_types.?[0]);
}

test "query builder infers graph multi hop pattern from intent" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find two hop links from doc:a",
        .schema_fields = &.{"body"},
        .mode = "graph",
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_graph" }},
    }, null);

    try std.testing.expectEqualStrings("graph", result.specialist.?);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.pattern, graph_query.type);
    try std.testing.expectEqualStrings("doc:a", graph_query.start_nodes.?.keys.?[0]);
    try std.testing.expectEqual(@as(usize, 3), graph_query.pattern.?.len);
    try std.testing.expectEqualStrings("a", graph_query.pattern.?[0].alias.?);
    try std.testing.expect(graph_query.pattern.?[0].edge == null);
    try std.testing.expectEqualStrings("b", graph_query.pattern.?[1].alias.?);
    try std.testing.expectEqualStrings("links", graph_query.pattern.?[1].edge.?.types.?[0]);
    try std.testing.expectEqual(@as(i64, 1), graph_query.pattern.?[1].edge.?.min_hops.?);
    try std.testing.expectEqual(@as(i64, 1), graph_query.pattern.?[1].edge.?.max_hops.?);
    try std.testing.expectEqualStrings("c", graph_query.pattern.?[2].alias.?);
    try std.testing.expectEqualStrings("links", graph_query.pattern.?[2].edge.?.types.?[0]);
    try std.testing.expectEqualStrings("c", graph_query.return_aliases.?[0]);
}

test "query builder maps tree search into retrieval query request" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"tree_search":{"index":"doc_hierarchy","start_nodes":"$roots","max_depth":2,"beam_width":4},"limit":5}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "title" },
        .mode = "tree",
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqualStrings("tree", result.specialist.?);
    try std.testing.expect(result.query_request != null);
    try std.testing.expect(result.retrieval_query_request != null);
    try std.testing.expect(result.retrieval_query_request.?.full_text_search != null);
    try std.testing.expectEqualStrings("doc_hierarchy", result.retrieval_query_request.?.tree_search.?.index);
    try std.testing.expectEqualStrings("$roots", result.retrieval_query_request.?.tree_search.?.start_nodes.?);
    try std.testing.expectEqual(@as(i64, 2), result.retrieval_query_request.?.tree_search.?.max_depth.?);
    try std.testing.expectEqual(@as(i64, 4), result.retrieval_query_request.?.tree_search.?.beam_width.?);
    try std.testing.expectEqualStrings("retrieval_query_request", result.plan.?.object.get("artifact").?.string);
}

test "query builder maps tree shorthand constraints" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"tree_index":"doc_hierarchy","tree_start_nodes":"doc:a,doc:b","tree_max_depth":3}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{"body"},
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqualStrings("tree", result.specialist.?);
    try std.testing.expect(result.retrieval_query_request != null);
    try std.testing.expectEqualStrings("doc_hierarchy", result.retrieval_query_request.?.tree_search.?.index);
    try std.testing.expectEqualStrings("doc:a,doc:b", result.retrieval_query_request.?.tree_search.?.start_nodes.?);
    try std.testing.expectEqual(@as(i64, 3), result.retrieval_query_request.?.tree_search.?.max_depth.?);
}

test "query builder infers tree index from table context" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{"body"},
        .mode = "tree",
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_hierarchy" }},
    }, null);

    try std.testing.expectEqualStrings("tree", result.specialist.?);
    try std.testing.expect(result.retrieval_query_request != null);
    try std.testing.expectEqualStrings("doc_hierarchy", result.retrieval_query_request.?.tree_search.?.index);
}

test "query builder require executable rejects tree search without tree topology metadata" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    const graph_edge_types = [_]QueryBuilderGraphEdgeType{.{
        .name = "references",
        .topology = "graph",
    }};
    const graph_metadata = [_]QueryBuilderGraphIndex{.{
        .name = "doc_graph",
        .edge_types = &graph_edge_types,
    }};
    var context = QueryBuilderTableContext{
        .schema_fields = &.{"body"},
        .full_text_index_metadata = &.{.{ .name = "search_idx" }},
        .graph_index_metadata = &graph_metadata,
    };
    context.plan_validator = metadataBackedPlanValidator(&context);

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .mode = "tree",
        .constraints = constraints_tree.value,
    }, context, null));
}

test "query builder infers tree start node from intent" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "walk tree from doc:root",
        .schema_fields = &.{"body"},
        .mode = "tree",
    }, .{
        .graph_index_metadata = &.{.{ .name = "doc_hierarchy" }},
    }, null);

    try std.testing.expectEqualStrings("tree", result.specialist.?);
    try std.testing.expectEqualStrings("doc:root", result.retrieval_query_request.?.tree_search.?.start_nodes.?);
}

test "query builder asks for tree index when table has multiple graph indexes" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{"body"},
        .mode = "tree",
        .interactive = true,
        .max_user_clarifications = 1,
    }, .{
        .graph_index_metadata = &.{ .{ .name = "doc_hierarchy" }, .{ .name = "topic_graph" } },
    }, null);

    try std.testing.expectEqual(AgentStatus.clarification_required, result.status.?);
    try std.testing.expect(result.questions != null);
    try std.testing.expectEqualStrings("select_tree_index", result.questions.?[0].id);
    try std.testing.expectEqualStrings("doc_hierarchy", result.questions.?[0].options.?[0]);
    try std.testing.expectEqualStrings("topic_graph", result.questions.?[0].options.?[1]);
    try std.testing.expect(result.retrieval_query_request == null);
}

test "query builder uses tree index decision answer" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{"body"},
        .mode = "tree",
        .interactive = true,
        .max_user_clarifications = 1,
        .decisions = &.{.{
            .question_id = "select_tree_index",
            .answer = .{ .string = "topic_graph" },
        }},
    }, .{
        .graph_index_metadata = &.{ .{ .name = "doc_hierarchy" }, .{ .name = "topic_graph" } },
    }, null);

    try std.testing.expectEqual(AgentStatus.completed, result.status.?);
    try std.testing.expect(result.questions == null);
    try std.testing.expect(result.retrieval_query_request != null);
    try std.testing.expectEqualStrings("topic_graph", result.retrieval_query_request.?.tree_search.?.index);
}

test "query builder asks for graph index when table has multiple graph indexes" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .interactive = true,
        .max_user_clarifications = 1,
    }, .{
        .graph_index_metadata = &.{ .{ .name = "doc_hierarchy" }, .{ .name = "topic_graph" } },
    }, null);

    try std.testing.expectEqual(AgentStatus.clarification_required, result.status.?);
    try std.testing.expect(result.questions != null);
    try std.testing.expectEqualStrings("select_graph_index", result.questions.?[0].id);
    try std.testing.expectEqualStrings("doc_hierarchy", result.questions.?[0].options.?[0]);
    try std.testing.expectEqualStrings("topic_graph", result.questions.?[0].options.?[1]);
    try std.testing.expect(result.query_request.?.graph_searches == null);
}

test "query builder uses graph index decision answer" {
    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponseWithContext(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .interactive = true,
        .max_user_clarifications = 1,
        .decisions = &.{.{
            .question_id = "select_graph_index",
            .answer = .{ .string = "topic_graph" },
        }},
    }, .{
        .graph_index_metadata = &.{ .{ .name = "doc_hierarchy" }, .{ .name = "topic_graph" } },
    }, null);

    try std.testing.expectEqual(AgentStatus.completed, result.status.?);
    try std.testing.expect(result.questions == null);
    const graph_query = result.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqualStrings("topic_graph", graph_query.index_name);
}

test "query builder require executable rejects missing semantic index without clarification" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponse(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .constraints = constraints_tree.value,
    }, null));
}

test "query builder require executable rejects missing graph index without clarification" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponse(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find related raft documents",
        .schema_fields = &.{"body"},
        .mode = "graph",
        .constraints = constraints_tree.value,
    }, null));
}

test "query builder require executable allows pending clarification" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .mode = "semantic",
        .interactive = true,
        .max_user_clarifications = 1,
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqual(AgentStatus.clarification_required, result.status.?);
    try std.testing.expect(result.questions != null);
    try std.testing.expectEqualStrings("select_semantic_index", result.questions.?[0].id);
}

test "query builder require executable asks for missing table with field context" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "status" },
        .interactive = true,
        .max_user_clarifications = 1,
        .constraints = constraints_tree.value,
    }, null);

    try std.testing.expectEqual(AgentStatus.clarification_required, result.status.?);
    try std.testing.expect(result.questions != null);
    try std.testing.expectEqualStrings("select_query_table", result.questions.?[0].id);
    try std.testing.expect(result.query_request.?.table == null);
}

test "query builder uses table decision answer" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    const result = try buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "body", "status" },
        .interactive = true,
        .max_user_clarifications = 1,
        .constraints = constraints_tree.value,
        .decisions = &.{.{
            .question_id = "select_query_table",
            .answer = .{ .string = "docs" },
        }},
    }, null);

    try std.testing.expectEqual(AgentStatus.completed, result.status.?);
    try std.testing.expect(result.questions == null);
    try std.testing.expectEqualStrings("docs", result.query_request.?.table.?);
    try std.testing.expectEqualStrings("find raft architecture", result.query_request.?.full_text_search.?.object.get("match").?.string);
}

test "query builder require executable rejects missing table" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponse(arena_impl.allocator(), .{
        .intent = "find raft architecture",
        .schema_fields = &.{ "title", "body", "status" },
        .constraints = constraints_tree.value,
    }, null));
}

test "query builder require executable rejects missing tree search" {
    var constraints_tree = try std.json.parseFromSlice(std.json.Value, std.testing.allocator,
        \\{"require_executable":true}
    , .{});
    defer constraints_tree.deinit();

    var arena_impl = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_impl.deinit();
    try std.testing.expectError(error.InvalidQueryBuilderRequest, buildQueryBuilderResponse(arena_impl.allocator(), .{
        .table = "docs",
        .intent = "find raft architecture",
        .schema_fields = &.{"body"},
        .mode = "tree",
        .constraints = constraints_tree.value,
    }, null));
}
