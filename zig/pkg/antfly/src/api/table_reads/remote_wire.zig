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
const db_mod = @import("../../storage/db/mod.zig");
const doc_set = @import("../../storage/db/doc_set.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_paths = @import("../../graph/paths.zig");
const graph_query_mod = @import("../../graph/query.zig");
const document_sql_runtime = @import("../../sql/document_runtime.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const distributed_graph = @import("../distributed_graph.zig");
const http_client = @import("../http_client.zig");
const http_common = @import("../../raft/transport/http_common.zig");
const query_api = @import("../query.zig");
const query_contract = @import("../query_contract.zig");
const table_read_core = @import("core.zig");
const table_read_relational_rows = @import("relational_rows.zig");

const LookupResponse = table_read_core.LookupResponse;
const ParsedTextStatsHttpResponse = table_read_core.ParsedTextStatsHttpResponse;
const ScanResponse = table_read_core.ScanResponse;
const relationalUniqueOwnerKeyAlloc = table_read_relational_rows.relationalUniqueOwnerKeyAlloc;
const algebraic_ir = db_mod.algebraic.ir;
const algebraic_law = db_mod.algebraic.law;
const algebraic_planner = db_mod.algebraic.planner;

const TextStatsRequestMode = enum {
    query_request,
    explicit_fields,
    background_fields,
};

pub fn searchRequestFromVectorWorkerEnvelope(envelope: *const query_contract.OwnedAlgebraicVectorWorkerRequestEnvelope) db_mod.types.SearchRequest {
    var req = switch (envelope.query) {
        .dense => |dense| db_mod.types.SearchRequest{
            .index_name = envelope.index_name,
            .limit = envelope.options.limit,
            .offset = envelope.options.offset,
            .count_only = envelope.options.count_only,
            .profile = envelope.options.profile,
            .include_stored = envelope.options.include_stored,
            .fields = envelope.options.fields,
            .filter_query_json = envelope.options.filter_query_json,
            .exclusion_query_json = envelope.options.exclusion_query_json,
            .filter_prefix = envelope.options.filter_prefix,
            .filter_ids = envelope.options.filter_ids,
            .exclude_ids = envelope.options.exclude_ids,
            .require_algebraic_filter_resolution = envelope.options.require_algebraic_filter_resolution,
            .include_all_fields = envelope.options.include_all_fields,
            .defer_stored_projection = envelope.options.defer_stored_projection,
            .search_effort = envelope.options.search_effort,
            .distance_over = envelope.options.distance_over,
            .distance_under = envelope.options.distance_under,
            .return_mode = envelope.options.return_mode,
            .max_chunks_per_parent = envelope.options.max_chunks_per_parent,
            .identity_read_generation = envelope.options.identity_read_generation,
            .resolved_doc_filter = envelope.resolved_doc_filter,
            .resolved_doc_filter_wire_context = envelope.resolved_doc_filter_wire_context,
            .query = .{ .dense_knn = dense },
        },
        .sparse => |sparse| db_mod.types.SearchRequest{
            .index_name = envelope.index_name,
            .limit = envelope.options.limit,
            .offset = envelope.options.offset,
            .count_only = envelope.options.count_only,
            .profile = envelope.options.profile,
            .include_stored = envelope.options.include_stored,
            .fields = envelope.options.fields,
            .filter_query_json = envelope.options.filter_query_json,
            .exclusion_query_json = envelope.options.exclusion_query_json,
            .filter_prefix = envelope.options.filter_prefix,
            .filter_ids = envelope.options.filter_ids,
            .exclude_ids = envelope.options.exclude_ids,
            .require_algebraic_filter_resolution = envelope.options.require_algebraic_filter_resolution,
            .include_all_fields = envelope.options.include_all_fields,
            .defer_stored_projection = envelope.options.defer_stored_projection,
            .search_effort = envelope.options.search_effort,
            .distance_over = envelope.options.distance_over,
            .distance_under = envelope.options.distance_under,
            .return_mode = envelope.options.return_mode,
            .max_chunks_per_parent = envelope.options.max_chunks_per_parent,
            .identity_read_generation = envelope.options.identity_read_generation,
            .resolved_doc_filter = envelope.resolved_doc_filter,
            .resolved_doc_filter_wire_context = envelope.resolved_doc_filter_wire_context,
            .query = .{ .sparse_knn = sparse },
        },
    };
    query_contract.applyNativeDocIdConstraintEnvelope(&req, envelope.native_doc_id_constraints.constraints);
    return req;
}

pub const OwnedTextStatsFieldRequest = struct {
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: [][]const u8 = &.{},

    pub fn deinit(self: *OwnedTextStatsFieldRequest, alloc: std.mem.Allocator) void {
        if (self.index_name) |index_name| alloc.free(index_name);
        alloc.free(self.field);
        for (self.terms) |term| alloc.free(term);
        if (self.terms.len > 0) alloc.free(self.terms);
        self.* = undefined;
    }
};

pub const OwnedBackgroundTextStatsFieldRequest = struct {
    aggregation_name: []const u8,
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: [][]const u8 = &.{},
    background_query: db_mod.aggregations.BackgroundQuery,

    pub fn deinit(self: *OwnedBackgroundTextStatsFieldRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.aggregation_name);
        if (self.index_name) |index_name| alloc.free(index_name);
        alloc.free(self.field);
        for (self.terms) |term| alloc.free(term);
        if (self.terms.len > 0) alloc.free(self.terms);
        switch (self.background_query) {
            .match_all => {},
            .match => |query| {
                alloc.free(query.field);
                alloc.free(query.text);
            },
            .term => |query| {
                alloc.free(query.field);
                alloc.free(query.term);
            },
        }
        self.* = undefined;
    }
};

const TextStatsFieldRequestInput = struct {
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8,
};

const BackgroundTextStatsFieldRequestInput = struct {
    aggregation_name: []const u8,
    index_name: ?[]const u8 = null,
    field: []const u8,
    terms: []const []const u8,
    background_query: std.json.Value,
};

const TextStatsRequestInput = struct {
    _identity_read_generation: ?u64 = null,
    _resolved_doc_filter: ?std.json.Value = null,
    query_request: ?std.json.Value = null,
    fields: ?[]const TextStatsFieldRequestInput = null,
    background_fields: ?[]const BackgroundTextStatsFieldRequestInput = null,
};

const ParsedExplicitTextStatsRequest = struct {
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null,
    items: []OwnedTextStatsFieldRequest = &.{},

    fn deinit(self: *ParsedExplicitTextStatsRequest, alloc: std.mem.Allocator) void {
        if (self.resolved_doc_filter) |*filter| filter.deinit(alloc);
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

const ParsedBackgroundTextStatsRequest = struct {
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null,
    items: []OwnedBackgroundTextStatsFieldRequest = &.{},

    fn deinit(self: *ParsedBackgroundTextStatsRequest, alloc: std.mem.Allocator) void {
        if (self.resolved_doc_filter) |*filter| filter.deinit(alloc);
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

const TextStatsTermDocFreqInput = struct {
    term: []const u8,
    doc_freq: u32,
};

const TextStatsFieldResponseInput = struct {
    field: []const u8,
    global_doc_count: u32,
    global_total_field_len: u64,
    term_doc_freqs: []const TextStatsTermDocFreqInput,
};

const TextStatsResponseInput = struct {
    fields: []const TextStatsFieldResponseInput,
};

const BackgroundTextStatsFieldResponseInput = struct {
    aggregation_name: []const u8,
    field: []const u8,
    background_doc_count: u32,
    term_doc_freqs: []const TextStatsTermDocFreqInput,
};

const BackgroundTextStatsResponseInput = struct {
    background_fields: []const BackgroundTextStatsFieldResponseInput,
};

pub const ParsedTextStatsRequest = union(TextStatsRequestMode) {
    query_request: query_api.OwnedQueryRequest,
    explicit_fields: ParsedExplicitTextStatsRequest,
    background_fields: ParsedBackgroundTextStatsRequest,

    pub fn deinit(self: *ParsedTextStatsRequest, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .query_request => |*request| request.deinit(alloc),
            .explicit_fields => |*request| request.deinit(alloc),
            .background_fields => |*request| request.deinit(alloc),
        }
        self.* = undefined;
    }
};

const AlgebraicPartialsRequestInput = struct {
    index_name: ?[]const u8 = null,
    _identity_read_generation: ?u64 = null,
    tensor_access_paths: ?[]const AlgebraicTensorAccessPathInput = null,
    tensor_exprs: ?[]const AlgebraicTensorExprInput = null,
    tensor_program: ?AlgebraicTensorProgramInput = null,
    cardinality: ?std.json.Value = null,
    terms_cardinality: ?std.json.Value = null,
    range_cardinality: ?std.json.Value = null,
    histogram_cardinality: ?std.json.Value = null,
};

const AlgebraicTensorAccessPathInput = query_contract.AlgebraicTensorAccessPathEnvelopeInput;
const AlgebraicTensorExprInput = query_contract.AlgebraicTensorExprEnvelopeInput;
const AlgebraicTensorProgramInput = query_contract.AlgebraicTensorProgramEnvelopeInput;

const AlgebraicPartialResponseInput = struct {
    canonical_axis: []const u8,
    metric: []const u8 = "",
    law: []const u8,
    value: []const u8,
};

const AlgebraicPartialsResponseInput = struct {
    partials: []const AlgebraicPartialResponseInput,
};

pub const ParsedAlgebraicPartialsRequest = struct {
    index_name: ?[]u8 = null,
    identity_read_generation: ?u64 = null,
    tensor_access_paths: []OwnedAlgebraicTensorAccessPath = &.{},
    tensor_exprs: []OwnedAlgebraicTensorExpr = &.{},
    tensor_program: ?OwnedAlgebraicTensorProgram = null,

    pub fn deinit(self: *ParsedAlgebraicPartialsRequest, alloc: std.mem.Allocator) void {
        if (self.index_name) |value| alloc.free(value);
        for (self.tensor_access_paths) |*item| item.deinit(alloc);
        if (self.tensor_access_paths.len > 0) alloc.free(self.tensor_access_paths);
        for (self.tensor_exprs) |*item| item.deinit(alloc);
        if (self.tensor_exprs.len > 0) alloc.free(self.tensor_exprs);
        if (self.tensor_program) |*program| program.deinit(alloc);
        self.* = undefined;
    }
};

pub const OwnedAlgebraicTensorAccessPath = query_contract.OwnedAlgebraicTensorAccessPathEnvelope;
pub const OwnedAlgebraicTensorExpr = query_contract.OwnedAlgebraicTensorExprEnvelope;
pub const OwnedAlgebraicTensorProgram = query_contract.OwnedAlgebraicTensorProgramEnvelope;

pub fn searchRequestHasResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    if (comptime @hasField(db_mod.types.SearchRequest, "resolved_doc_filter")) {
        return req.resolved_doc_filter != null;
    }
    return false;
}

pub fn searchRequestHasUnserializableResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    return searchRequestHasResolvedDocFilter(req) and req.resolved_doc_filter_wire_context == null;
}

pub const AlgebraicVectorWorkerFilterSupportFn = *const fn (std.mem.Allocator, []const u8) bool;

const AlgebraicVectorWorkerCandidate = struct {
    index_name: []const u8,
    layout: algebraic_ir.PhysicalLayout,
    query: query_contract.AlgebraicVectorWorkerQuery,
    k: u32,
};

fn algebraicVectorWorkerCandidateForSearchRequest(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    filter_supported: AlgebraicVectorWorkerFilterSupportFn,
) ?AlgebraicVectorWorkerCandidate {
    if (req.aggregations_json.len != 0 or
        req.full_text != null or
        req.full_text_queries.len != 0 or
        req.dense_queries.len != 0 or
        req.sparse_queries.len != 0 or
        req.graph_queries.len != 0 or
        req.merge_config != null or
        req.reranker != null or
        req.pruner != null or
        req.expand_strategy != null or
        req.distributed_text_stats.len != 0 or
        searchRequestHasUnserializableResolvedDocFilter(req))
    {
        return null;
    }
    if (req.filter_query_json.len != 0 and !filter_supported(alloc, req.filter_query_json)) return null;
    if (req.exclusion_query_json.len != 0 and !filter_supported(alloc, req.exclusion_query_json)) return null;

    if (req.dense) |dense| {
        if (req.sparse != null) return null;
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
    }
    if (req.sparse) |sparse| {
        if (req.query != .match_all) return null;
        const index_name = req.index_name orelse return null;
        return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
    }

    switch (req.query) {
        .dense_knn => |dense| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .dense_vector, .query = .{ .dense = dense }, .k = dense.k };
        },
        .sparse_knn => |sparse| {
            const index_name = req.index_name orelse return null;
            return .{ .index_name = index_name, .layout = .sparse_vector, .query = .{ .sparse = sparse }, .k = sparse.k };
        },
        else => return null,
    }
}

pub fn annotateVectorWorkerPreflight(
    alloc: std.mem.Allocator,
    summary: *db_mod.RuntimePreflightSummary,
    req: db_mod.types.SearchRequest,
    filter_supported: AlgebraicVectorWorkerFilterSupportFn,
) void {
    if (!searchRequestHasSingleVectorWorkerKnn(req)) return;
    summary.vector_worker_filter_constraint_count +|= vectorWorkerFilterConstraintCount(req);
    if (req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0) {
        summary.vector_worker_requires_algebraic_filter_resolution = true;
    }
    if (algebraicVectorWorkerCandidateForSearchRequest(alloc, req, filter_supported) != null) {
        summary.vector_worker_candidate_count +|= 1;
    } else {
        summary.vector_worker_fallback_count +|= 1;
    }
}

fn searchRequestHasSingleVectorWorkerKnn(req: db_mod.types.SearchRequest) bool {
    var count: u32 = 0;
    if (req.dense != null) count += 1;
    if (req.sparse != null) count += 1;
    switch (req.query) {
        .dense_knn, .sparse_knn => count += 1,
        else => {},
    }
    return count == 1;
}

fn vectorWorkerFilterConstraintCount(req: db_mod.types.SearchRequest) u32 {
    var count: u32 = 0;
    if (req.filter_query_json.len > 0) count += 1;
    if (req.exclusion_query_json.len > 0) count += 1;
    if (req.filter_ids.len > 0) count += 1;
    if (req.exclude_ids.len > 0) count += 1;
    if (req.filter_doc_ids_positive or req.filter_doc_ids.len > 0) count += 1;
    if (req.exclude_doc_ids.len > 0) count += 1;
    if (searchRequestHasResolvedDocFilter(req)) count += 1;
    return count;
}

pub fn encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    filter_supported: AlgebraicVectorWorkerFilterSupportFn,
) !?[]u8 {
    const candidate = algebraicVectorWorkerCandidateForSearchRequest(alloc, req, filter_supported) orelse return null;
    const constraints = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest(req);
    var tensor_program = (try algebraic_planner.planVectorSearchTensorProgramAlloc(alloc, candidate.index_name, candidate.layout, constraints.hasConstraints())) orelse return null;
    defer tensor_program.deinit(alloc);
    return try query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        candidate.index_name,
        candidate.layout,
        candidate.query,
        .{
            .limit = req.limit,
            .offset = req.offset,
            .count_only = req.count_only,
            .profile = req.profile,
            .include_stored = req.include_stored,
            .fields = @constCast(req.fields),
            .filter_query_json = req.filter_query_json,
            .exclusion_query_json = req.exclusion_query_json,
            .filter_prefix = req.filter_prefix,
            .filter_ids = req.filter_ids,
            .exclude_ids = req.exclude_ids,
            .require_algebraic_filter_resolution = req.filter_query_json.len > 0 or req.exclusion_query_json.len > 0,
            .include_all_fields = req.include_all_fields,
            .defer_stored_projection = req.defer_stored_projection,
            .search_effort = req.search_effort,
            .distance_over = req.distance_over,
            .distance_under = req.distance_under,
            .return_mode = req.return_mode,
            .max_chunks_per_parent = req.max_chunks_per_parent,
            .identity_read_generation = req.identity_read_generation,
        },
        constraints,
        req.resolved_doc_filter,
        req.resolved_doc_filter_wire_context,
        tensor_program.access_paths,
        tensor_program.asProgram(),
    );
}

pub fn parseRemoteSearchResultForHostedQuery(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    return parseRemoteSearchResult(alloc, body) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.UnsupportedQueryRequest,
    };
}

pub fn lookupRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    key: []const u8,
    opts: db_mod.types.LookupOptions,
) !?LookupResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const fields = try encodeLookupFields(alloc, opts);
    defer if (fields) |value| alloc.free(value);
    var result = try client.fetchGroupLookup(base_uri, group_id, table_name, key, fields);
    defer result.deinit(alloc);
    return .{
        .json = try alloc.dupe(u8, result.body),
        .version = if (result.version) |version| try std.fmt.parseUnsigned(u64, version, 10) else 0,
    };
}

pub fn lookupRelationalUniqueOwnerRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
) !?[]u8 {
    const key = try relationalUniqueOwnerKeyAlloc(alloc, constraint_name, encoded_value);
    defer alloc.free(key);
    var lookup = (try lookupRemote(executor, alloc, base_uri, group_id, table_name, key, .{})) orelse return null;
    defer lookup.deinit(alloc);
    return try alloc.dupe(u8, lookup.json);
}

pub fn lookupRelationalTemporalUniqueOwnerRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_point: []const u8,
) !?[]u8 {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"encoded_value\":{f},\"encoded_point\":{f}}}",
        .{ std.json.fmt(constraint_name, .{}), std.json.fmt(encoded_value, .{}), std.json.fmt(encoded_point, .{}) },
    );
    defer alloc.free(body);
    var result = client.fetchGroupTemporalUniqueOwner(base_uri, group_id, table_name, body) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer result.deinit(alloc);
    var parsed = std.json.parseFromSlice(struct { owner_key: []const u8 }, alloc, result.body, .{}) catch return error.InvalidRemoteResponse;
    defer parsed.deinit();
    return try alloc.dupe(u8, parsed.value.owner_key);
}

pub fn lookupRelationalTemporalUniqueOverlapOwnerRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_start: []const u8,
    encoded_end: []const u8,
) !?[]u8 {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"encoded_value\":{f},\"encoded_start\":{f},\"encoded_end\":{f}}}",
        .{ std.json.fmt(constraint_name, .{}), std.json.fmt(encoded_value, .{}), std.json.fmt(encoded_start, .{}), std.json.fmt(encoded_end, .{}) },
    );
    defer alloc.free(body);
    var result = client.fetchGroupTemporalUniqueOverlapOwner(base_uri, group_id, table_name, body) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer result.deinit(alloc);
    var parsed = std.json.parseFromSlice(struct { owner_key: []const u8 }, alloc, result.body, .{}) catch return error.InvalidRemoteResponse;
    defer parsed.deinit();
    return try alloc.dupe(u8, parsed.value.owner_key);
}

pub fn scanRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
) !?ScanResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try encodeScanRequest(alloc, from_key, to_key, opts);
    defer alloc.free(body);
    var result = try client.fetchGroupScan(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .ndjson = try alloc.dupe(u8, result.body) };
}

pub fn queryRemoteWithVectorWorkerBody(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    vector_worker_body: ?[]const u8,
) !db_mod.types.SearchResult {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    if (vector_worker_body) |body| {
        var result = try client.fetchGroupVectorWorker(base_uri, group_id, table_name, body);
        defer result.deinit(alloc);
        var parsed = try parseRemoteSearchResultForHostedQuery(alloc, result.body);
        parsed.identity_read_generation = req.identity_read_generation;
        return parsed;
    }
    const body = try encodeQueryRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupQuery(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    var parsed = try parseRemoteSearchResultForHostedQuery(alloc, result.body);
    parsed.identity_read_generation = req.identity_read_generation;
    return parsed;
}

pub fn preflightRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    max_work: u32,
) !db_mod.RuntimePreflightSummary {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    const body = try encodeQueryRequest(alloc, req);
    defer alloc.free(body);
    var summary = try client.fetchGroupQueryPreflight(base_uri, group_id, table_name, body, max_work);
    summary.remote_shard_count = summary.shard_count;
    return summary;
}

pub fn encodeQueryTextStatsRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ![]u8 {
    const encoded_query = try encodeQueryRequest(alloc, req);
    defer alloc.free(encoded_query);
    return try std.fmt.allocPrint(alloc, "{{\"query_request\":{s}}}", .{encoded_query});
}

pub fn encodeExplicitTextStatsRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedTextStatsFieldRequest,
    identity_read_generation: ?u64,
) ![]u8 {
    return try encodeExplicitTextStatsRequestForSearchRequest(alloc, items, .{ .identity_read_generation = identity_read_generation });
}

pub fn encodeExplicitTextStatsRequestForSearchRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedTextStatsFieldRequest,
    req: db_mod.types.SearchRequest,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var top_first = true;
    if (req.identity_read_generation) |generation| try appendJsonFieldU64(alloc, &out, &top_first, "_identity_read_generation", generation);
    try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &top_first, req);
    try appendJsonFieldName(alloc, &out, &top_first, "fields");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        if (item.index_name) |index_name| {
            try appendJsonFieldString(alloc, &out, &first, "index_name", index_name);
        }
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldName(alloc, &out, &first, "terms");
        try out.append(alloc, '[');
        for (item.terms, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, term);
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn encodeBackgroundTextStatsRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedBackgroundTextStatsFieldRequest,
    identity_read_generation: ?u64,
) ![]u8 {
    return try encodeBackgroundTextStatsRequestForSearchRequest(alloc, items, .{ .identity_read_generation = identity_read_generation });
}

pub fn encodeBackgroundTextStatsRequestForSearchRequest(
    alloc: std.mem.Allocator,
    items: []const OwnedBackgroundTextStatsFieldRequest,
    req: db_mod.types.SearchRequest,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var top_first = true;
    if (req.identity_read_generation) |generation| try appendJsonFieldU64(alloc, &out, &top_first, "_identity_read_generation", generation);
    try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &top_first, req);
    try appendJsonFieldName(alloc, &out, &top_first, "background_fields");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "aggregation_name", item.aggregation_name);
        if (item.index_name) |index_name| {
            try appendJsonFieldString(alloc, &out, &first, "index_name", index_name);
        }
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldName(alloc, &out, &first, "terms");
        try out.append(alloc, '[');
        for (item.terms, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, term);
        }
        try out.append(alloc, ']');
        try appendJsonFieldName(alloc, &out, &first, "background_query");
        try appendBackgroundQueryJson(alloc, &out, item.background_query);
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn parseTextStatsRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
) !ParsedTextStatsRequest {
    var parsed = try std.json.parseFromSlice(TextStatsRequestInput, alloc, body, .{});
    defer parsed.deinit();
    if (parsed.value.query_request) |query_value| {
        if (parsed.value._resolved_doc_filter != null) return error.InvalidQueryRequest;
        const encoded_query = try std.json.Stringify.valueAlloc(alloc, query_value, .{});
        defer alloc.free(encoded_query);
        return .{ .query_request = try query_api.parseQueryRequest(alloc, null, table_name, encoded_query) };
    }
    if (parsed.value.fields) |fields_value| {
        var resolved_doc_filter = if (parsed.value._resolved_doc_filter) |filter_value|
            try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, filter_value)
        else
            null;
        errdefer if (resolved_doc_filter) |*filter| filter.deinit(alloc);
        const identity_read_generation = try identityGenerationFromTextStatsResolvedFilter(parsed.value._identity_read_generation, if (resolved_doc_filter) |*filter| filter else null);
        const items = try alloc.alloc(OwnedTextStatsFieldRequest, fields_value.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            if (items.len > 0) alloc.free(items);
        }
        for (fields_value, 0..) |field_value, i| {
            const terms = try alloc.alloc([]const u8, field_value.terms.len);
            var initialized_terms: usize = 0;
            errdefer {
                for (terms[0..initialized_terms]) |term| alloc.free(term);
                if (terms.len > 0) alloc.free(terms);
            }
            for (field_value.terms, 0..) |term_value, term_idx| {
                terms[term_idx] = try alloc.dupe(u8, term_value);
                initialized_terms += 1;
            }
            items[i] = .{
                .index_name = if (field_value.index_name) |index_name_value| try alloc.dupe(u8, index_name_value) else null,
                .field = try alloc.dupe(u8, field_value.field),
                .terms = terms,
            };
            initialized += 1;
        }
        return .{ .explicit_fields = .{
            .identity_read_generation = identity_read_generation,
            .resolved_doc_filter = resolved_doc_filter,
            .items = items,
        } };
    }
    if (parsed.value.background_fields) |fields_value| {
        var resolved_doc_filter = if (parsed.value._resolved_doc_filter) |filter_value|
            try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, filter_value)
        else
            null;
        errdefer if (resolved_doc_filter) |*filter| filter.deinit(alloc);
        const identity_read_generation = try identityGenerationFromTextStatsResolvedFilter(parsed.value._identity_read_generation, if (resolved_doc_filter) |*filter| filter else null);
        const items = try alloc.alloc(OwnedBackgroundTextStatsFieldRequest, fields_value.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |*item| item.deinit(alloc);
            if (items.len > 0) alloc.free(items);
        }
        for (fields_value, 0..) |field_value, i| {
            const terms = try alloc.alloc([]const u8, field_value.terms.len);
            var initialized_terms: usize = 0;
            errdefer {
                for (terms[0..initialized_terms]) |term| alloc.free(term);
                if (terms.len > 0) alloc.free(terms);
            }
            for (field_value.terms, 0..) |term_value, term_idx| {
                terms[term_idx] = try alloc.dupe(u8, term_value);
                initialized_terms += 1;
            }
            items[i] = .{
                .aggregation_name = try alloc.dupe(u8, field_value.aggregation_name),
                .index_name = if (field_value.index_name) |index_name_value| try alloc.dupe(u8, index_name_value) else null,
                .field = try alloc.dupe(u8, field_value.field),
                .terms = terms,
                .background_query = try parseBackgroundQueryRequestAlloc(alloc, field_value.background_query),
            };
            initialized += 1;
        }
        return .{ .background_fields = .{
            .identity_read_generation = identity_read_generation,
            .resolved_doc_filter = resolved_doc_filter,
            .items = items,
        } };
    }
    return error.InvalidQueryRequest;
}

fn identityGenerationFromTextStatsResolvedFilter(
    explicit_generation: ?u64,
    resolved_doc_filter: ?*const db_mod.doc_filter_wire.ParsedResolvedDocFilter,
) !?u64 {
    const filter = resolved_doc_filter orelse return explicit_generation;
    if (explicit_generation) |generation| {
        if (generation != filter.context.identity_read_generation) return error.InvalidQueryRequest;
        return generation;
    }
    return filter.context.identity_read_generation;
}

pub fn encodeTextStatsResponse(alloc: std.mem.Allocator, stats: []const distributed_stats_mod.TextFieldStats) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"fields\":[");
    for (stats, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldU32(alloc, &out, &first, "global_doc_count", item.global_doc_count);
        try appendJsonFieldU64(alloc, &out, &first, "global_total_field_len", item.global_total_field_len);
        try appendJsonFieldName(alloc, &out, &first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, &out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, &out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn parseTextStatsResponse(alloc: std.mem.Allocator, body: []const u8) ![]const distributed_stats_mod.TextFieldStats {
    var parsed = try std.json.parseFromSlice(TextStatsResponseInput, alloc, body, .{});
    defer parsed.deinit();
    const fields_value = parsed.value.fields;
    const stats = try alloc.alloc(distributed_stats_mod.TextFieldStats, fields_value.len);
    var initialized: usize = 0;
    errdefer {
        for (stats[0..initialized]) |*item| item.deinit(alloc);
        if (stats.len > 0) alloc.free(stats);
    }
    for (fields_value, 0..) |entry, i| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.term_doc_freqs.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (entry.term_doc_freqs, 0..) |term_entry, term_idx| {
            term_doc_freqs[term_idx] = .{
                .term = try alloc.dupe(u8, term_entry.term),
                .doc_freq = term_entry.doc_freq,
            };
            initialized_terms += 1;
        }
        stats[i] = .{
            .field = try alloc.dupe(u8, entry.field),
            .global_doc_count = entry.global_doc_count,
            .global_total_field_len = entry.global_total_field_len,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return stats;
}

pub fn encodeBackgroundTextStatsResponse(
    alloc: std.mem.Allocator,
    stats: []const db_mod.aggregations.DistributedBackgroundTextStats,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"background_fields\":[");
    for (stats, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "aggregation_name", item.aggregation_name);
        try appendJsonFieldString(alloc, &out, &first, "field", item.field);
        try appendJsonFieldU32(alloc, &out, &first, "background_doc_count", item.background_doc_count);
        try appendJsonFieldName(alloc, &out, &first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, &out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, &out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn parseBackgroundTextStatsResponse(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    var parsed = try std.json.parseFromSlice(BackgroundTextStatsResponseInput, alloc, body, .{});
    defer parsed.deinit();
    const fields_value = parsed.value.background_fields;
    const stats = try alloc.alloc(db_mod.aggregations.DistributedBackgroundTextStats, fields_value.len);
    var initialized: usize = 0;
    errdefer {
        for (stats[0..initialized]) |*item| item.deinit(alloc);
        if (stats.len > 0) alloc.free(stats);
    }
    for (fields_value, 0..) |entry, i| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.term_doc_freqs.len);
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (entry.term_doc_freqs, 0..) |term_entry, term_idx| {
            term_doc_freqs[term_idx] = .{
                .term = try alloc.dupe(u8, term_entry.term),
                .doc_freq = term_entry.doc_freq,
            };
            initialized_terms += 1;
        }
        stats[i] = .{
            .aggregation_name = try alloc.dupe(u8, entry.aggregation_name),
            .field = try alloc.dupe(u8, entry.field),
            .background_doc_count = entry.background_doc_count,
            .term_doc_freqs = term_doc_freqs,
        };
        initialized += 1;
    }
    return stats;
}

pub fn parseTextStatsHttpResponse(
    alloc: std.mem.Allocator,
    request_body: []const u8,
    response_body: []const u8,
) !ParsedTextStatsHttpResponse {
    var parsed = try std.json.parseFromSlice(TextStatsRequestInput, alloc, request_body, .{});
    defer parsed.deinit();

    if (parsed.value.background_fields != null) {
        return .{
            .background_fields = .{
                .background_fields = try parseBackgroundTextStatsResponse(alloc, response_body),
            },
        };
    }

    return .{
        .fields = .{
            .fields = try parseTextStatsResponse(alloc, response_body),
        },
    };
}

fn appendBackgroundQueryJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.aggregations.BackgroundQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":{");
            try appendJsonString(alloc, out, match.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, "}}");
        },
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":{");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, "}}");
        },
    }
}

fn parseBackgroundQueryRequestAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !db_mod.aggregations.BackgroundQuery {
    if (value == .object) {
        if (value.object.get("match_all") != null) return .{ .match_all = {} };
        if (value.object.get("match")) |match| {
            if (match == .object and match.object.count() == 1) {
                var it = match.object.iterator();
                const entry = it.next() orelse return error.InvalidQueryRequest;
                if (entry.value_ptr.* != .string) return error.InvalidQueryRequest;
                return .{ .match = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .text = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
        if (value.object.get("term")) |term| {
            if (term == .object and term.object.count() == 1) {
                var it = term.object.iterator();
                const entry = it.next() orelse return error.InvalidQueryRequest;
                if (entry.value_ptr.* != .string) return error.InvalidQueryRequest;
                return .{ .term = .{
                    .field = try alloc.dupe(u8, entry.key_ptr.*),
                    .term = try alloc.dupe(u8, entry.value_ptr.string),
                } };
            }
        }
    }
    return error.InvalidQueryRequest;
}

pub fn textStatsRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupTextStats(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

pub fn algebraicPartialsRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupAlgebraicPartials(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

pub fn encodeAlgebraicPartialsRequest(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
) ![]u8 {
    return try encodeAlgebraicPartialsRequestWithProgram(alloc, index_name, access_paths, tensor_exprs, null);
}

pub fn encodeAlgebraicPartialsRequestWithProgram(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
    tensor_program: ?algebraic_ir.TensorProgram,
) ![]u8 {
    return try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, index_name, null, access_paths, tensor_exprs, tensor_program);
}

pub fn encodeAlgebraicPartialsRequestWithProgramAtGeneration(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    identity_read_generation: ?u64,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
    tensor_program: ?algebraic_ir.TensorProgram,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    if (index_name) |name| try appendJsonFieldString(alloc, &out, &first, "index_name", name);
    if (identity_read_generation) |generation| try appendJsonFieldU64(alloc, &out, &first, "_identity_read_generation", generation);
    if (access_paths.len > 0) {
        try appendJsonFieldName(alloc, &out, &first, "tensor_access_paths");
        try out.append(alloc, '[');
        for (access_paths, 0..) |path, i| {
            if (i > 0) try out.append(alloc, ',');
            const encoded = try query_contract.encodeAlgebraicTensorAccessPathEnvelopeAlloc(alloc, path);
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
        try out.append(alloc, ']');
    }
    if (tensor_exprs.len > 0) {
        try appendJsonFieldName(alloc, &out, &first, "tensor_exprs");
        try out.append(alloc, '[');
        for (tensor_exprs, 0..) |expr, i| {
            if (i > 0) try out.append(alloc, ',');
            const encoded = try query_contract.encodeAlgebraicTensorExprEnvelopeAlloc(alloc, expr);
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
        try out.append(alloc, ']');
    }
    if (tensor_program) |program| {
        try appendJsonFieldName(alloc, &out, &first, "tensor_program");
        const encoded = try query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, program);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeAlgebraicExpressionPartialsRequest(
    alloc: std.mem.Allocator,
    index_name: ?[]const u8,
    access_paths: []const algebraic_ir.PhysicalAccessPath,
    tensor_exprs: []const algebraic_ir.TensorExpr,
) ![]u8 {
    return try encodeAlgebraicPartialsRequest(alloc, index_name, access_paths, tensor_exprs);
}

pub fn parseAlgebraicPartialsRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !ParsedAlgebraicPartialsRequest {
    var parsed = try std.json.parseFromSlice(AlgebraicPartialsRequestInput, alloc, body, .{});
    defer parsed.deinit();
    const exprs_value = parsed.value.tensor_exprs orelse &.{};
    const has_program = parsed.value.tensor_program != null;
    const has_legacy_request = parsed.value.cardinality != null or
        parsed.value.terms_cardinality != null or
        parsed.value.range_cardinality != null or
        parsed.value.histogram_cardinality != null;
    if (has_legacy_request) return error.InvalidQueryRequest;
    if (exprs_value.len == 0 and !has_program) return error.InvalidQueryRequest;
    if (has_program and exprs_value.len > 0) return error.InvalidQueryRequest;
    const paths_value = parsed.value.tensor_access_paths orelse return error.InvalidQueryRequest;
    const expected_proof_count = if (exprs_value.len > 0) exprs_value.len else paths_value.len;
    if (paths_value.len != expected_proof_count) return error.InvalidQueryRequest;
    const tensor_access_paths = blk: {
        const paths = try alloc.alloc(OwnedAlgebraicTensorAccessPath, paths_value.len);
        var paths_initialized: usize = 0;
        errdefer {
            for (paths[0..paths_initialized]) |*item| item.deinit(alloc);
            if (paths.len > 0) alloc.free(paths);
        }
        for (paths_value, 0..) |path_value, i| {
            paths[i] = try parseAlgebraicTensorAccessPathAlloc(alloc, path_value);
            paths_initialized += 1;
        }
        break :blk paths;
    };
    errdefer {
        for (tensor_access_paths) |*item| item.deinit(alloc);
        if (tensor_access_paths.len > 0) alloc.free(tensor_access_paths);
    }
    const tensor_exprs = blk: {
        const exprs = try alloc.alloc(OwnedAlgebraicTensorExpr, exprs_value.len);
        var exprs_initialized: usize = 0;
        errdefer {
            for (exprs[0..exprs_initialized]) |*item| item.deinit(alloc);
            if (exprs.len > 0) alloc.free(exprs);
        }
        for (exprs_value, 0..) |expr_value, i| {
            exprs[i] = try query_contract.parseAlgebraicTensorExprEnvelopeInputAlloc(alloc, expr_value);
            exprs_initialized += 1;
        }
        break :blk exprs;
    };
    errdefer {
        for (tensor_exprs) |*item| item.deinit(alloc);
        if (tensor_exprs.len > 0) alloc.free(tensor_exprs);
    }
    var tensor_program: ?OwnedAlgebraicTensorProgram = null;
    errdefer if (tensor_program) |*program| program.deinit(alloc);
    if (parsed.value.tensor_program) |program_value| {
        tensor_program = try query_contract.parseAlgebraicTensorProgramEnvelopeInputAlloc(alloc, program_value);
        try validateAlgebraicProgramPartialsProof(alloc, tensor_access_paths, &tensor_program.?);
    }
    return .{
        .index_name = if (parsed.value.index_name) |name| try alloc.dupe(u8, name) else null,
        .identity_read_generation = parsed.value._identity_read_generation,
        .tensor_access_paths = tensor_access_paths,
        .tensor_exprs = tensor_exprs,
        .tensor_program = tensor_program,
    };
}

fn parseAlgebraicTensorAccessPathAlloc(
    alloc: std.mem.Allocator,
    input: AlgebraicTensorAccessPathInput,
) !OwnedAlgebraicTensorAccessPath {
    return try query_contract.parseAlgebraicTensorAccessPathEnvelopeInputAlloc(alloc, input);
}

pub fn encodeAlgebraicPartialsResponse(
    alloc: std.mem.Allocator,
    partials: []const db_mod.algebraic.distributed.Partial,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"partials\":[");
    for (partials, 0..) |partial, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        try appendJsonFieldString(alloc, &out, &first, "canonical_axis", partial.canonical_axis);
        try appendJsonFieldString(alloc, &out, &first, "metric", partial.metric);
        try appendJsonFieldString(alloc, &out, &first, "law", @tagName(partial.law_id));
        try appendJsonFieldString(alloc, &out, &first, "value", partial.value);
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn parseAlgebraicPartialsResponse(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]db_mod.algebraic.distributed.Partial {
    var parsed = try std.json.parseFromSlice(AlgebraicPartialsResponseInput, alloc, body, .{});
    defer parsed.deinit();
    const partials = try alloc.alloc(db_mod.algebraic.distributed.Partial, parsed.value.partials.len);
    var initialized: usize = 0;
    errdefer {
        for (partials[0..initialized]) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        if (partials.len > 0) alloc.free(partials);
    }
    for (parsed.value.partials, 0..) |partial, i| {
        const law_id = std.meta.stringToEnum(db_mod.algebraic.law.Id, partial.law) orelse return error.InvalidQueryRequest;
        partials[i] = .{
            .canonical_axis = try alloc.dupe(u8, partial.canonical_axis),
            .metric = try alloc.dupe(u8, partial.metric),
            .law_id = law_id,
            .value = try alloc.dupe(u8, partial.value),
        };
        initialized += 1;
    }
    return partials;
}

pub fn parsedAlgebraicTensorExpressionsAlloc(
    alloc: std.mem.Allocator,
    items: []const OwnedAlgebraicTensorExpr,
) ![]algebraic_ir.TensorExpr {
    const exprs = try alloc.alloc(algebraic_ir.TensorExpr, items.len);
    errdefer if (exprs.len > 0) alloc.free(exprs);
    for (items, 0..) |*item, i| exprs[i] = item.asExpr();
    return exprs;
}

pub fn validateAlgebraicPartialsAccessPaths(
    alloc: std.mem.Allocator,
    access_paths: anytype,
    tensor_exprs: anytype,
) !void {
    if (access_paths.len == 0 or access_paths.len != tensor_exprs.len) return error.InvalidQueryRequest;
    for (access_paths, tensor_exprs) |access_path, tensor_expr| {
        const expr = algebraicTensorExprValue(tensor_expr);
        var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)) orelse return error.InvalidQueryRequest;
        defer plan.deinit(alloc);
        if (!algebraicTensorAccessPathMatches(plan.access_path, access_path)) return error.InvalidQueryRequest;
    }
}

pub fn validateAlgebraicProgramPartialsAccessPaths(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
    program: *const OwnedAlgebraicTensorProgram,
) !void {
    try validateAlgebraicProgramPartialsProof(alloc, access_paths, program);
    const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, null, access_paths, program);
    defer if (exprs.len > 0) alloc.free(exprs);
}

pub fn validateAlgebraicProgramPartialsProof(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
    program: *const OwnedAlgebraicTensorProgram,
) !void {
    const path_values = try algebraicTensorAccessPathValuesAlloc(alloc, access_paths);
    defer if (path_values.len > 0) alloc.free(path_values);
    var view = try program.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const proof = try algebraic_ir.tensorProgramProof(alloc, path_values, view.program);
    if (!proof.safe()) return error.InvalidQueryRequest;
}

pub fn algebraicTensorProgramOutputExpressionsForIndexAlloc(
    alloc: std.mem.Allocator,
    index: ?*const db_mod.algebraic.index.Index,
    access_paths: []OwnedAlgebraicTensorAccessPath,
    program: *const OwnedAlgebraicTensorProgram,
) ![]algebraic_ir.TensorExpr {
    const path_values = try algebraicTensorAccessPathValuesAlloc(alloc, access_paths);
    defer if (path_values.len > 0) alloc.free(path_values);
    var view = try program.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const proof = try algebraic_ir.tensorProgramProof(alloc, path_values, view.program);
    if (!proof.safe()) return error.InvalidQueryRequest;
    const single_output = [_]algebraic_ir.TensorProgramRef{view.program.output};
    const refs = if (view.program.outputs.len > 0) view.program.outputs else single_output[0..];
    const exprs = try alloc.alloc(algebraic_ir.TensorExpr, refs.len);
    errdefer if (exprs.len > 0) alloc.free(exprs);
    for (refs, 0..) |ref, i| {
        const step_idx = switch (ref) {
            .step => |idx| idx,
            .input => return error.InvalidQueryRequest,
        };
        if (step_idx >= view.program.steps.len) return error.InvalidQueryRequest;
        const expr = view.program.steps[step_idx].expr;
        exprs[i] = try algebraicTensorProgramOutputExpressionForStep(alloc, index, path_values, expr);
    }
    return exprs;
}

fn algebraicTensorProgramOutputExpressionForStep(
    alloc: std.mem.Allocator,
    index: ?*const db_mod.algebraic.index.Index,
    path_values: []const algebraic_ir.PhysicalAccessPath,
    expr: algebraic_ir.TensorExpr,
) !algebraic_ir.TensorExpr {
    if (expr.layout == .materialized_expr) {
        var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)) orelse return error.InvalidQueryRequest;
        defer plan.deinit(alloc);
        if (!algebraicTensorAccessPathListHas(path_values, plan.access_path)) return error.InvalidQueryRequest;
        return expr;
    }
    if (expr.layout == .materialized_tensor) {
        const concrete_index = index orelse return error.InvalidQueryRequest;
        const materialization = expr.semantic_id orelse expr.owner orelse return error.InvalidQueryRequest;
        const mat = findAlgebraicMaterialization(concrete_index, materialization) orelse return error.InvalidQueryRequest;
        const access_path = algebraic_planner.materializationAccessPath(mat) orelse return error.InvalidQueryRequest;
        if (!algebraicTensorAccessPathListHas(path_values, access_path)) return error.InvalidQueryRequest;
        const output_expr = algebraic_planner.materializationTensorExpression(mat) orelse return error.InvalidQueryRequest;
        if (expr.law_id != null and output_expr.law_id != expr.law_id) return error.InvalidQueryRequest;
        return output_expr;
    }
    return error.InvalidQueryRequest;
}

fn algebraicTensorAccessPathListHas(paths: []const algebraic_ir.PhysicalAccessPath, expected: algebraic_ir.PhysicalAccessPath) bool {
    for (paths) |path| {
        if (algebraicTensorAccessPathMatches(expected, path)) return true;
    }
    return false;
}

pub fn algebraicTensorAccessPathValuesAlloc(
    alloc: std.mem.Allocator,
    access_paths: []OwnedAlgebraicTensorAccessPath,
) ![]algebraic_ir.PhysicalAccessPath {
    const out = try alloc.alloc(algebraic_ir.PhysicalAccessPath, access_paths.len);
    errdefer if (out.len > 0) alloc.free(out);
    for (access_paths, 0..) |path, i| out[i] = path.asAccessPath();
    return out;
}

fn findAlgebraicMaterialization(
    index: *const db_mod.algebraic.index.Index,
    name: []const u8,
) ?db_mod.algebraic.index.MaterializationConfig {
    for (index.config().materializations) |mat| {
        if (std.mem.eql(u8, mat.name, name)) return mat;
    }
    return null;
}

fn algebraicTensorAccessPathMatches(
    expected: algebraic_ir.PhysicalAccessPath,
    actual: anytype,
) bool {
    const actual_path = algebraicTensorAccessPathValue(actual);
    return std.mem.eql(u8, expected.owner, actual_path.owner) and
        expected.layout == actual_path.layout and
        optionalDictionaryEqual(expected.dictionary, actual_path.dictionary) and
        tensorFragmentSlicesEqual(expected.fragments, actual_path.fragments) and
        tensorDimensionSlicesEqual(expected.output_dims, actual_path.output_dims) and
        lawIdSlicesEqual(expected.law_ids, actual_path.law_ids);
}

fn algebraicTensorAccessPathValue(actual: anytype) algebraic_ir.PhysicalAccessPath {
    if (@TypeOf(actual) == algebraic_ir.PhysicalAccessPath) return actual;
    return actual.asAccessPath();
}

fn optionalDictionaryEqual(
    left: ?db_mod.algebraic.lexical.DictionaryIdentity,
    right: ?db_mod.algebraic.lexical.DictionaryIdentity,
) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return left.?.eql(right.?);
}

fn algebraicTensorExprValue(actual: anytype) algebraic_ir.TensorExpr {
    if (@TypeOf(actual) == algebraic_ir.TensorExpr) return actual;
    return actual.asExpr();
}

fn algebraicTensorExprMatches(expected: algebraic_ir.TensorExpr, actual: algebraic_ir.TensorExpr) bool {
    return expected.fragment == actual.fragment and
        tensorDimensionSlicesEqual(expected.input_dims, actual.input_dims) and
        tensorDimensionSlicesEqual(expected.output_dims, actual.output_dims) and
        optionalStringEqual(expected.semantic_id, actual.semantic_id) and
        optionalStringEqual(expected.owner, actual.owner) and
        expected.layout == actual.layout and
        expected.law_id == actual.law_id;
}

fn optionalStringEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return std.mem.eql(u8, left.?, right.?);
}

fn tensorFragmentSlicesEqual(left: []const algebraic_ir.TensorFragment, right: []const algebraic_ir.TensorFragment) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn tensorDimensionSlicesEqual(left: []const algebraic_ir.Dimension, right: []const algebraic_ir.Dimension) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn lawIdSlicesEqual(left: []const algebraic_law.Id, right: []const algebraic_law.Id) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

test "algebraic partial request preserves planner-owned materialization tensor programs" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"customer","path":"customer","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}]}
    );
    defer index.close();

    const materializations = [_][]const u8{"sum_by_customer"};
    var program_plan = (try algebraic_planner.planMaterializationPartialsTensorProgramAlloc(alloc, &index, &materializations)) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);

    const encoded = try encodeAlgebraicPartialsRequestWithProgramAtGeneration(alloc, "alg", 91, program_plan.access_paths, &.{}, program_plan.asProgram());
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"_identity_read_generation\":91") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_access_paths\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_exprs\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"materialized_tensor\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"law_ids\":[\"sum\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"law_id\":\"sum\"") != null);

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 91), parsed.identity_read_generation);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.tensor_exprs.len);
    try std.testing.expect(parsed.tensor_program != null);
    try validateAlgebraicProgramPartialsProof(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?);

    parsed.tensor_access_paths[0].law_ids[0] = .count;
    try std.testing.expectError(error.InvalidQueryRequest, validateAlgebraicProgramPartialsProof(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?));
    parsed.tensor_access_paths[0].law_ids[0] = .sum;
    try std.testing.expectError(error.UnknownField, parseAlgebraicPartialsRequest(alloc, "{\"index_name\":\"alg\",\"materializations\":[\"sum_by_customer\"]}"));
}

test "algebraic partial request rejects legacy cardinality bodies" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"cardinality\":{\"aggregation_name\":\"x\",\"field\":\"y\"}}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"terms_cardinality\":{\"aggregation_name\":\"x\",\"bucket_field\":\"y\",\"children\":[]}}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"range_cardinality\":{\"aggregation_name\":\"x\",\"field\":\"amount\",\"kind\":\"numeric\",\"ranges\":[],\"children\":[]}}",
    ));
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(
        alloc,
        "{\"index_name\":\"alg\",\"histogram_cardinality\":{\"aggregation_name\":\"x\",\"field\":\"amount\",\"kind\":\"numeric\",\"interval\":10,\"children\":[]}}",
    ));
}

test "algebraic partial request accepts expression cache proofs without named materializations" {
    const alloc = std.testing.allocator;

    const expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "expr_sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, expr)).?;
    defer plan.deinit(alloc);

    const encoded = try encodeAlgebraicExpressionPartialsRequest(alloc, "alg", &.{plan.access_path}, &.{expr});
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_access_paths\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_exprs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"layout\":\"materialized_expr\"") != null);

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_exprs.len);
    try validateAlgebraicPartialsAccessPaths(alloc, parsed.tensor_access_paths, parsed.tensor_exprs);

    parsed.tensor_access_paths[0].owner[0] = if (parsed.tensor_access_paths[0].owner[0] == 'x') 'y' else 'x';
    try std.testing.expectError(error.InvalidQueryRequest, validateAlgebraicPartialsAccessPaths(alloc, parsed.tensor_access_paths, parsed.tensor_exprs));
}

test "algebraic partial request accepts tensor program expression outputs" {
    const alloc = std.testing.allocator;

    const count_expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{.doc},
        .output_dims = &.{.bucket},
        .semantic_id = "expr_count_by_customer",
        .layout = .materialized_expr,
        .law_id = .count,
    };
    const sum_expr = algebraic_ir.TensorExpr{
        .fragment = .reduce,
        .input_dims = &.{ .doc, .scalar },
        .output_dims = &.{.bucket},
        .semantic_id = "expr_sum_by_customer",
        .layout = .materialized_expr,
        .law_id = .sum,
    };
    var count_plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, count_expr)).?;
    defer count_plan.deinit(alloc);
    var sum_plan = (try algebraic_ir.planMaterializedExpressionAlloc(alloc, sum_expr)).?;
    defer sum_plan.deinit(alloc);
    const access_paths = [_]algebraic_ir.PhysicalAccessPath{ count_plan.access_path, sum_plan.access_path };
    const steps = [_]algebraic_ir.TensorProgramStep{ .{ .expr = count_expr }, .{ .expr = sum_expr } };
    const outputs = [_]algebraic_ir.TensorProgramRef{ .{ .step = 0 }, .{ .step = 1 } };
    const program = algebraic_ir.TensorProgram{
        .steps = &steps,
        .output = .{ .step = 0 },
        .outputs = &outputs,
    };
    try std.testing.expect((try algebraic_ir.tensorProgramProof(alloc, &access_paths, program)).safe());

    const encoded = try encodeAlgebraicPartialsRequestWithProgram(alloc, "alg", &access_paths, &.{}, program);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"outputs\"") != null);
    const mixed_encoded = try encodeAlgebraicPartialsRequestWithProgram(alloc, "alg", &access_paths, &.{count_expr}, program);
    defer alloc.free(mixed_encoded);
    try std.testing.expectError(error.InvalidQueryRequest, parseAlgebraicPartialsRequest(alloc, mixed_encoded));

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), parsed.tensor_access_paths.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.tensor_exprs.len);
    try std.testing.expect(parsed.tensor_program != null);
    try validateAlgebraicProgramPartialsAccessPaths(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?);
    const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, null, parsed.tensor_access_paths, &parsed.tensor_program.?);
    defer alloc.free(exprs);
    try std.testing.expectEqual(@as(usize, 2), exprs.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.reduce, exprs[0].fragment);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, exprs[0].law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, exprs[1].law_id.?);

    parsed.tensor_access_paths[1].law_ids[0] = .max;
    try std.testing.expectError(error.InvalidQueryRequest, validateAlgebraicProgramPartialsAccessPaths(alloc, parsed.tensor_access_paths, &parsed.tensor_program.?));
}

test "algebraic partial request derives expression outputs from materialized tensor program" {
    const alloc = std.testing.allocator;

    var index = try db_mod.algebraic.index.Index.open(alloc, "alg",
        \\{"version":1,"table":"docs","schema_version":1,
        \\ "group_fields":[{"name":"customer","path":"customer","type":"keyword"}],
        \\ "measure_fields":[{"name":"amount","path":"amount","type":"number"}],
        \\ "materializations":[
        \\   {"name":"customers","op":"count","group_by":["customer"]},
        \\   {"name":"amount_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}
        \\ ]}
    );
    defer index.close();

    var program_plan = (try algebraic_planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "customers",
        .bucket_field = "customer",
        .child_metrics = &.{.{ .name = "amount_by_customer", .op = .sum, .field = "amount" }},
    })).?;
    defer program_plan.deinit(alloc);
    const encoded = try encodeAlgebraicPartialsRequestWithProgram(alloc, "alg", program_plan.access_paths, &.{}, program_plan.asProgram());
    defer alloc.free(encoded);

    var parsed = try parseAlgebraicPartialsRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, &index, parsed.tensor_access_paths, &parsed.tensor_program.?);
    defer alloc.free(exprs);
    try std.testing.expectEqual(@as(usize, 2), exprs.len);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.materialized_expr, exprs[0].layout.?);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.materialized_expr, exprs[1].layout.?);
    try std.testing.expectEqualStrings("customers", exprs[0].semantic_id.?);
    try std.testing.expectEqualStrings("amount_by_customer", exprs[1].semantic_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.count, exprs[0].law_id.?);
    try std.testing.expectEqual(db_mod.algebraic.law.Id.sum, exprs[1].law_id.?);
}

const DocumentAlgebraicAggregateResponseWire = struct {
    const Row = struct {
        group_json: ?[]const u8 = null,
        value_json: []const u8,
        raw_value: ?[]const u8 = null,
    };

    rows: []const Row,
    total_groups: u32,
};

pub fn parseDocumentAlgebraicAggregateResponseAlloc(
    alloc: std.mem.Allocator,
    body: []const u8,
) !document_sql_runtime.AlgebraicAggregateResponse {
    var parsed = try std.json.parseFromSlice(DocumentAlgebraicAggregateResponseWire, alloc, body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    const rows = try alloc.alloc(document_sql_runtime.AlgebraicAggregateRow, parsed.value.rows.len);
    errdefer alloc.free(rows);
    var initialized: usize = 0;
    errdefer {
        for (rows[0..initialized]) |*row| row.deinit(alloc);
    }
    for (parsed.value.rows, rows) |row, *out| {
        out.* = .{
            .group_json = if (row.group_json) |value| try alloc.dupe(u8, value) else null,
            .value_json = try alloc.dupe(u8, row.value_json),
            .raw_value = if (row.raw_value) |value| try alloc.dupe(u8, value) else null,
        };
        initialized += 1;
    }
    return .{
        .rows = rows,
        .total_groups = parsed.value.total_groups,
    };
}

pub fn documentAlgebraicAggregateRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: document_sql_runtime.AlgebraicAggregateRequest,
) !document_sql_runtime.AlgebraicAggregateResponse {
    const body = try std.json.Stringify.valueAlloc(alloc, req, .{});
    defer alloc.free(body);
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupDocumentAlgebraicAggregate(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return try parseDocumentAlgebraicAggregateResponseAlloc(alloc, result.body);
}

test "remote document algebraic aggregate preserves typed unavailable and not found errors" {
    const alloc = std.testing.allocator;
    const FakeExecutor = struct {
        status: u16,
        body: []const u8,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, response_alloc: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = self.status,
                .body = try response_alloc.dupe(u8, self.body),
            };
        }
    };

    const req = document_sql_runtime.AlgebraicAggregateRequest{
        .index_name = "amount_alg",
        .materialization_name = "avg_by_status",
        .aggregate_op = .avg,
        .group_by = null,
        .limit = null,
    };

    var unavailable = FakeExecutor{ .status = 424, .body = "DocumentSqlIndexUnavailable" };
    try std.testing.expectError(error.DocumentSqlIndexUnavailable, documentAlgebraicAggregateRemote(
        unavailable.executor(),
        alloc,
        "http://127.0.0.1:1",
        7,
        "docs",
        req,
    ));

    var table_missing = FakeExecutor{ .status = 404, .body = "TableNotFound" };
    try std.testing.expectError(error.TableNotFound, documentAlgebraicAggregateRemote(
        table_missing.executor(),
        alloc,
        "http://127.0.0.1:1",
        7,
        "docs",
        req,
    ));

    var group_missing = FakeExecutor{ .status = 404, .body = "UnknownGroup" };
    try std.testing.expectError(error.UnknownGroup, documentAlgebraicAggregateRemote(
        group_missing.executor(),
        alloc,
        "http://127.0.0.1:1",
        7,
        "docs",
        req,
    ));
}

pub fn joinPartitionRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinPartition(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

pub fn joinRowsRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinRows(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

pub fn joinUnmatchedRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinUnmatched(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

pub fn joinFinalizeRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinFinalize(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

pub fn joinJobStateRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !?query_api.QueryResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    var result = try client.fetchGroupJoinJobState(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return .{ .json = try alloc.dupe(u8, result.body) };
}

pub fn graphExpandRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
) !distributed_graph.GraphExpandResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try distributed_graph.encodeGraphExpandRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupGraphExpand(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return try distributed_graph.parseGraphExpandResponse(alloc, result.body);
}

pub fn graphHydrateRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphHydrateRequest,
) !distributed_graph.GraphHydrateResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try distributed_graph.encodeGraphHydrateRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupGraphHydrate(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return try distributed_graph.parseGraphHydrateResponse(alloc, result.body);
}

pub fn graphEdgesRemote(
    executor: http_common.RequestExecutor,
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
    req: distributed_graph.GraphEdgesRequest,
) !distributed_graph.GraphEdgesResponse {
    var client = http_client.ApiHttpClient.init(alloc, executor);
    const body = try distributed_graph.encodeGraphEdgesRequest(alloc, req);
    defer alloc.free(body);
    var result = try client.fetchGroupGraphEdges(base_uri, group_id, table_name, body);
    defer result.deinit(alloc);
    return try distributed_graph.parseGraphEdgesResponse(alloc, result.body);
}

pub fn encodeLookupFields(alloc: std.mem.Allocator, opts: db_mod.types.LookupOptions) !?[]u8 {
    if (opts.include_all_fields or opts.fields.len == 0) return null;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (opts.fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, field);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn encodeScanRequest(
    alloc: std.mem.Allocator,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    if (from_key.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "from", from_key);
    }
    if (to_key.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "to", to_key);
    }
    if (opts.limit > 0) {
        try appendJsonFieldU32(alloc, &out, &first, "limit", opts.limit);
    }
    if (opts.fields.len > 0 and !opts.include_all_fields) {
        try appendJsonFieldNames(alloc, &out, &first, "fields", opts.fields);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeQueryRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ![]u8 {
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;

    if (req.fields.len > 0 and !req.include_all_fields) {
        try appendJsonFieldNames(alloc, &out, &first, "fields", req.fields);
    }
    if (req.limit != 10) {
        try appendJsonFieldU32(alloc, &out, &first, "limit", req.limit);
    }
    if (req.offset != 0) {
        try appendJsonFieldU32(alloc, &out, &first, "offset", req.offset);
    }
    if (req.count_only) {
        try appendJsonFieldBool(alloc, &out, &first, "count", true);
    }
    if (req.profile) {
        try appendJsonFieldBool(alloc, &out, &first, "profile", true);
    }
    if (req.filter_prefix.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "filter_prefix", req.filter_prefix);
    }
    if (req.distance_over) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_over", value);
    }
    if (req.distance_under) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_under", value);
    }
    if (req.merge_config) |merge_config| {
        try appendMergeConfigField(alloc, &out, &first, merge_config);
    }
    if (req.pruner) |pruner| {
        try appendPrunerField(alloc, &out, &first, pruner);
    }
    if (req.distributed_text_stats.len > 0) {
        try appendDistributedTextStatsField(alloc, &out, &first, req.distributed_text_stats);
    }
    if (req.identity_read_generation) |generation| {
        try appendJsonFieldU64(alloc, &out, &first, "_identity_read_generation", generation);
    }
    if (req.resolved_doc_filter != null) {
        try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &first, req);
    }
    const native_doc_id_constraints = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest(req);
    if (native_doc_id_constraints.hasConstraints()) {
        try appendNativeDocIdConstraintsField(alloc, &out, &first, native_doc_id_constraints);
    }
    if (req.filter_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_filter_query_json", req.filter_query_json);
    }
    if (req.exclusion_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_exclusion_query_json", req.exclusion_query_json);
    }
    if (req.graph_queries.len > 0) {
        try appendGraphQueriesField(alloc, &out, &first, req.graph_queries);
    }
    if (req.graph_metric_queries.len > 0) {
        try appendGraphMetricQueryField(alloc, &out, &first, req.graph_metric_queries);
    }
    if (req.graph_metric_rerank) |rerank| {
        try appendGraphMetricRerankField(alloc, &out, &first, rerank);
    }
    if (req.expand_strategy) |expand_strategy| {
        try appendJsonFieldString(alloc, &out, &first, "expand_strategy", switch (expand_strategy) {
            .@"union" => "union",
            .intersection => "intersection",
        });
    }
    if (req.dense_queries.len > 0 or req.sparse_queries.len > 0) {
        try appendEmbeddingsField(alloc, &out, &first, req.dense_queries, req.sparse_queries);
    }
    if (req.full_text) |full_text| {
        try appendTextQueryField(alloc, &out, &first, "full_text_search", full_text);
    } else {
        try appendQueryField(alloc, &out, &first, req.query, req.limit);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendNativeDocIdConstraintsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    constraints: query_contract.NativeDocIdConstraintEnvelope,
) !void {
    const encoded = try query_contract.encodeNativeDocIdConstraintEnvelopeAlloc(alloc, constraints);
    defer alloc.free(encoded);
    try appendJsonFieldName(alloc, out, first, "native_doc_id_constraints");
    try out.appendSlice(alloc, encoded);
}

fn appendDistributedTextStatsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    items: []const distributed_stats_mod.TextFieldStats,
) !void {
    try appendJsonFieldName(alloc, out, first, "_distributed_text_stats");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var field_first = true;
        try appendJsonFieldString(alloc, out, &field_first, "field", item.field);
        try appendJsonFieldU32(alloc, out, &field_first, "global_doc_count", item.global_doc_count);
        try appendJsonFieldU64(alloc, out, &field_first, "global_total_field_len", item.global_total_field_len);
        try appendJsonFieldName(alloc, out, &field_first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.append(alloc, ']');
}

pub fn appendJsonFieldU64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    var buf: [32]u8 = undefined;
    const rendered = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try out.appendSlice(alloc, rendered);
}

fn appendMergeConfigField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    merge_config: db_mod.types.MergeConfig,
) !void {
    try appendJsonFieldName(alloc, out, first, "merge_config");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendJsonFieldString(alloc, out, &merge_first, "strategy", switch (merge_config.strategy) {
        .rrf => "rrf",
        .rsf => "rsf",
    });
    if (merge_config.rank_constant != 60.0) {
        try appendJsonFieldF64(alloc, out, &merge_first, "rank_constant", merge_config.rank_constant);
    }
    if (merge_config.window_size != 0) {
        try appendJsonFieldU32(alloc, out, &merge_first, "window_size", merge_config.window_size);
    }
    if (merge_config.weights.len > 0) {
        try appendJsonFieldName(alloc, out, &merge_first, "weights");
        try out.append(alloc, '{');
        for (merge_config.weights, 0..) |weight, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, weight.name);
            try out.append(alloc, ':');
            var weight_buf: [32]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&weight_buf, "{d}", .{weight.weight});
            try out.appendSlice(alloc, rendered);
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
}

fn appendPrunerField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    pruner: @import("../../search/fusion.zig").Pruner,
) !void {
    try appendJsonFieldName(alloc, out, first, "pruner");
    try out.append(alloc, '{');
    var pruner_first = true;
    if (pruner.min_score_ratio > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_score_ratio", pruner.min_score_ratio);
    }
    if (pruner.max_score_gap_percent > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "max_score_gap_percent", pruner.max_score_gap_percent);
    }
    if (pruner.min_absolute_score > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_absolute_score", pruner.min_absolute_score);
    }
    if (pruner.require_multi_index) {
        try appendJsonFieldBool(alloc, out, &pruner_first, "require_multi_index", true);
    }
    if (pruner.std_dev_threshold > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "std_dev_threshold", pruner.std_dev_threshold);
    }
    try out.append(alloc, '}');
}

fn appendGraphQueriesField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    graph_queries: []const db_mod.types.NamedGraphQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, "graph_searches");
    try out.append(alloc, '{');
    for (graph_queries, 0..) |graph_query, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, graph_query.name);
        try out.append(alloc, ':');
        try appendGraphQueryValue(alloc, out, graph_query.query);
    }
    try out.append(alloc, '}');
}

fn appendGraphMetricQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    queries: []const db_mod.types.NamedGraphMetricQuery,
) !void {
    if (queries.len != 1) return;
    const named = queries[0];

    try appendJsonFieldName(alloc, out, first, "graph_metric");
    try out.append(alloc, '{');
    var metric_first = true;
    try appendJsonFieldString(alloc, out, &metric_first, "name", named.name);
    try appendJsonFieldString(alloc, out, &metric_first, "index", named.query.index_name);
    try appendJsonFieldString(alloc, out, &metric_first, "metric", named.query.metric_name);
    try appendJsonFieldU32(alloc, out, &metric_first, "top_k", named.query.top_k);
    try appendJsonFieldString(alloc, out, &metric_first, "metric_freshness", switch (named.query.freshness) {
        .published => "published",
        .fresh => "fresh",
    });
    try out.append(alloc, '}');
}

fn appendGraphMetricRerankField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    rerank: db_mod.types.GraphMetricRerank,
) !void {
    try appendJsonFieldName(alloc, out, first, "graph_metric_rerank");
    try out.append(alloc, '{');
    var rerank_first = true;
    try appendJsonFieldString(alloc, out, &rerank_first, "index", rerank.index_name);
    try appendJsonFieldString(alloc, out, &rerank_first, "metric", rerank.metric_name);
    try appendJsonFieldF64(alloc, out, &rerank_first, "base_weight", rerank.base_weight);
    try appendJsonFieldF64(alloc, out, &rerank_first, "weight", rerank.weight);
    try appendJsonFieldF64(alloc, out, &rerank_first, "missing_score", rerank.missing_score);
    try appendJsonFieldString(alloc, out, &rerank_first, "metric_freshness", switch (rerank.freshness) {
        .published => "published",
        .fresh => "fresh",
    });
    try out.append(alloc, '}');
}

fn appendGraphQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: graph_query_mod.GraphQuery,
) !void {
    try out.append(alloc, '{');
    var first = true;
    try appendJsonFieldString(alloc, out, &first, "type", switch (query.query_type) {
        .traverse => "traverse",
        .neighbors => "neighbors",
        .shortest_path => "shortest_path",
        .k_shortest_paths => "k_shortest_paths",
        .pattern => "pattern",
    });
    try appendJsonFieldString(alloc, out, &first, "index_name", query.index_name);
    try appendGraphNodeSelectorField(alloc, out, &first, "start_nodes", query.start_nodes);
    if (query.target_nodes) |target_nodes| {
        try appendGraphNodeSelectorField(alloc, out, &first, "target_nodes", target_nodes);
    }
    try appendGraphQueryParamsField(alloc, out, &first, query.params, query.k);
    if (query.metrics.len > 0) {
        var metric_names = try alloc.alloc([]const u8, query.metrics.len);
        defer alloc.free(metric_names);
        for (query.metrics, 0..) |metric, i| metric_names[i] = metric.name;
        try appendJsonFieldNames(alloc, out, &first, "metrics", metric_names);
        try appendJsonFieldString(alloc, out, &first, "metric_freshness", switch (query.metrics[0].freshness) {
            .published => "published",
            .fresh => "fresh",
        });
    }
    if (query.order_by.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "order_by");
        try out.append(alloc, '[');
        for (query.order_by, 0..) |order, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var order_first = true;
            try appendJsonFieldString(alloc, out, &order_first, "metric", order.name);
            try appendJsonFieldString(alloc, out, &order_first, "direction", switch (order.direction) {
                .asc => "asc",
                .desc => "desc",
            });
            try appendJsonFieldString(alloc, out, &order_first, "nulls", switch (order.nulls) {
                .first => "first",
                .last => "last",
            });
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
        if (query.metrics.len == 0) {
            try appendJsonFieldString(alloc, out, &first, "metric_freshness", switch (query.order_by[0].freshness) {
                .published => "published",
                .fresh => "fresh",
            });
        }
    }
    if (query.where_metric.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "where_metric");
        try out.append(alloc, '[');
        for (query.where_metric, 0..) |filter, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var filter_first = true;
            try appendJsonFieldString(alloc, out, &filter_first, "metric", filter.name);
            try appendJsonFieldString(alloc, out, &filter_first, "op", switch (filter.op) {
                .gt => ">",
                .gte => ">=",
                .lt => "<",
                .lte => "<=",
                .eq => "==",
                .neq => "!=",
            });
            try appendJsonFieldF64(alloc, out, &filter_first, "value", filter.value);
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
        if (query.metrics.len == 0 and query.order_by.len == 0) {
            try appendJsonFieldString(alloc, out, &first, "metric_freshness", switch (query.where_metric[0].freshness) {
                .published => "published",
                .fresh => "fresh",
            });
        }
    }
    if (query.include_metric_status) try appendJsonFieldBool(alloc, out, &first, "include_metric_status", true);
    try out.append(alloc, '}');
}

fn appendGraphNodeSelectorField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    selector: graph_query_mod.NodeSelector,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '{');
    var selector_first = true;
    switch (selector) {
        .keys => |keys| {
            try appendJsonFieldName(alloc, out, &selector_first, "keys");
            try out.append(alloc, '[');
            for (keys, 0..) |key, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, key);
            }
            try out.append(alloc, ']');
        },
        .result_ref => |result_ref| {
            try appendJsonFieldString(alloc, out, &selector_first, "result_ref", result_ref.ref);
            if (result_ref.limit > 0) try appendJsonFieldU32(alloc, out, &selector_first, "limit", result_ref.limit);
        },
    }
    try out.append(alloc, '}');
}

fn appendGraphQueryParamsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    params: graph_query_mod.QueryParams,
    k: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, "params");
    try out.append(alloc, '{');
    var params_first = true;
    if (params.edge_types.len > 0) try appendJsonFieldNames(alloc, out, &params_first, "edge_types", params.edge_types);
    if (params.direction != .out) try appendJsonFieldString(alloc, out, &params_first, "direction", switch (params.direction) {
        .out => "out",
        .in => "in",
        .both => "both",
    });
    if (params.max_depth != 3) try appendJsonFieldU32(alloc, out, &params_first, "max_depth", params.max_depth);
    if (params.min_weight != 0) try appendJsonFieldF64(alloc, out, &params_first, "min_weight", params.min_weight);
    if (params.max_weight != 0) try appendJsonFieldF64(alloc, out, &params_first, "max_weight", params.max_weight);
    if (params.max_results != 100) try appendJsonFieldU32(alloc, out, &params_first, "max_results", params.max_results);
    if (!params.deduplicate) try appendJsonFieldBool(alloc, out, &params_first, "deduplicate_nodes", false);
    if (params.include_paths) try appendJsonFieldBool(alloc, out, &params_first, "include_paths", true);
    if (params.weight_mode != .min_hops) try appendJsonFieldString(alloc, out, &params_first, "weight_mode", switch (params.weight_mode) {
        .min_hops => "min_hops",
        .min_weight => "min_weight",
        .max_weight => "max_weight",
    });
    if (k > 1) try appendJsonFieldU32(alloc, out, &params_first, "k", k);
    try out.append(alloc, '}');
}

fn appendEmbeddingsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    dense_queries: []const db_mod.types.NamedDenseQuery,
    sparse_queries: []const db_mod.types.NamedSparseQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, "embeddings");
    try out.append(alloc, '{');
    var entry_index: usize = 0;
    for (dense_queries) |dense_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, dense_query.index_name);
        try out.appendSlice(alloc, ":[");
        for (dense_query.query.vector, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.append(alloc, ']');
        entry_index += 1;
    }
    for (sparse_queries) |sparse_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, sparse_query.index_name);
        try out.appendSlice(alloc, ":{\"indices\":[");
        for (sparse_query.query.indices, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "],\"values\":[");
        for (sparse_query.query.values, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "]}");
        entry_index += 1;
    }
    try out.append(alloc, '}');
}

fn appendQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    query: db_mod.types.Query,
    default_k: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, "full_text_search");
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, '}');
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, match.field);
            try out.append(alloc, '}');
        },
        .dense_knn => |dense| {
            try out.appendSlice(alloc, "{\"dense_knn\":{\"vector\":[");
            for (dense.vector, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (dense.k == 0) default_k else dense.k});
            try out.appendSlice(alloc, "}}");
        },
        .sparse_knn => |sparse| {
            try out.appendSlice(alloc, "{\"sparse_knn\":{\"indices\":[");
            for (sparse.indices, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"values\":[");
            for (sparse.values, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (sparse.k == 0) default_k else sparse.k});
            try out.appendSlice(alloc, "}}");
        },
        else => return error.UnsupportedQueryRequest,
    }
}

fn appendTextQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    query: db_mod.types.TextQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendTextQueryValue(alloc, out, query);
}

fn appendTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.types.TextQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match_none => try out.appendSlice(alloc, "{\"match_none\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, '}');
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, match.field);
            if (match.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            try out.append(alloc, '}');
        },
        .multi_match_bool_prefix => |multi_match| {
            try out.appendSlice(alloc, "{\"multi_match\":{\"query\":");
            try appendJsonString(alloc, out, multi_match.query);
            try out.appendSlice(alloc, ",\"type\":\"bool_prefix\",\"fields\":[");
            for (multi_match.fields, 0..) |field, i| {
                if (i > 0) try out.append(alloc, ',');
                if (field.boost == 1.0) {
                    try appendJsonString(alloc, out, field.field);
                } else {
                    const boosted_field = try std.fmt.allocPrint(alloc, "{s}^{d}", .{ field.field, field.boost });
                    defer alloc.free(boosted_field);
                    try appendJsonString(alloc, out, boosted_field);
                }
            }
            try out.append(alloc, ']');
            if (multi_match.boost != 1.0) {
                try out.appendSlice(alloc, ",\"boost\":");
                try out.print(alloc, "{d}", .{multi_match.boost});
            }
            try out.appendSlice(alloc, "}}");
        },
        .match_phrase => |phrase| {
            try out.appendSlice(alloc, "{\"match_phrase\":");
            try appendJsonString(alloc, out, phrase.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, phrase.field);
            if (phrase.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            if (phrase.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else if (phrase.max_edits > 0) {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{phrase.max_edits});
            }
            try out.append(alloc, '}');
        },
        .fuzzy => |fuzzy| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, fuzzy.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, fuzzy.field);
            if (fuzzy.prefix_len > 0) {
                try out.appendSlice(alloc, ",\"prefix_length\":");
                try out.print(alloc, "{d}", .{fuzzy.prefix_len});
            }
            if (fuzzy.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{fuzzy.max_edits});
            }
            try out.append(alloc, '}');
        },
        .prefix => |prefix| {
            try out.appendSlice(alloc, "{\"prefix\":");
            try appendJsonString(alloc, out, prefix.prefix);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, prefix.field);
            try out.append(alloc, '}');
        },
        .wildcard => |wildcard| {
            try out.appendSlice(alloc, "{\"wildcard\":");
            try appendJsonString(alloc, out, wildcard.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, wildcard.field);
            try out.append(alloc, '}');
        },
        .regexp => |regexp| {
            try out.appendSlice(alloc, "{\"regexp\":");
            try appendJsonString(alloc, out, regexp.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, regexp.field);
            try out.append(alloc, '}');
        },
        .numeric_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| {
                try appendJsonFieldName(alloc, out, &first, "min");
                try out.print(alloc, "{d}", .{min});
            }
            if (range_query.max) |max| {
                try appendJsonFieldName(alloc, out, &first, "max");
                try out.print(alloc, "{d}", .{max});
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try out.append(alloc, '}');
        },
        .date_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.start_ns) |start_ns| {
                const text = try formatRfc3339Ns(alloc, start_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "start", text);
            }
            if (range_query.end_ns) |end_ns| {
                const text = try formatRfc3339Ns(alloc, end_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "end", text);
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_start) try appendJsonFieldBool(alloc, out, &first, "inclusive_start", false);
            if (range_query.inclusive_end) try appendJsonFieldBool(alloc, out, &first, "inclusive_end", true);
            try out.append(alloc, '}');
        },
        .term_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| try appendJsonFieldString(alloc, out, &first, "min", min);
            if (range_query.max) |max| try appendJsonFieldString(alloc, out, &first, "max", max);
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try out.append(alloc, '}');
        },
        .doc_id => |doc_id| {
            try out.appendSlice(alloc, "{\"ids\":[");
            for (doc_id.ids, 0..) |id, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, id);
            }
            try out.appendSlice(alloc, "]}");
        },
        .bool_field => |bool_field| {
            try out.appendSlice(alloc, "{\"bool\":");
            try out.appendSlice(alloc, if (bool_field.value) "true" else "false");
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, bool_field.field);
            try out.append(alloc, '}');
        },
        .bool_query => |bool_query| {
            try out.append(alloc, '{');
            var first = true;
            if (bool_query.must.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must");
                try out.appendSlice(alloc, "{\"conjuncts\":[");
                for (bool_query.must, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            if (bool_query.should.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "should");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.should, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
                if (bool_query.min_should > 0) {
                    try out.appendSlice(alloc, ",\"min\":");
                    try out.print(alloc, "{d}", .{bool_query.min_should});
                }
                try out.append(alloc, '}');
            }
            if (bool_query.must_not.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must_not");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.must_not, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            try out.append(alloc, '}');
        },
        else => return error.UnsupportedQueryRequest,
    }
}

pub fn parseRemoteSearchResult(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, body, .{});
    defer parsed.deinit();
    const responses = parsed.value.responses orelse return error.InvalidQueryRequest;
    if (responses.len == 0) return error.InvalidQueryRequest;
    const response = responses[0];
    const hits_obj = response.hits orelse return error.InvalidQueryRequest;
    const hits_value = hits_obj.hits orelse return error.InvalidQueryRequest;

    const hits = try alloc.alloc(db_mod.types.SearchHit, hits_value.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (hits_value, 0..) |item, i| {
        hits[i] = .{
            .id = try alloc.dupe(u8, item._id),
            .score = item._score,
            .stored_data = if (item._source) |value| try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})}) else null,
        };
        initialized += 1;
    }

    const graph_results: []db_mod.types.GraphSearchResult = if (response.graph_results) |graph_results_value|
        try parseRemoteGraphResults(alloc, graph_results_value)
    else
        @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);
    errdefer {
        for (graph_results) |*graph_result| graph_result.deinit(alloc);
        if (graph_results.len > 0) alloc.free(graph_results);
    }
    const graph_metric_results: []db_mod.types.GraphMetricResult = if (response.graph_metric_results) |graph_metric_results_value|
        try parseRemoteGraphMetricResults(alloc, graph_metric_results_value)
    else
        @constCast((&[_]db_mod.types.GraphMetricResult{})[0..]);
    errdefer {
        for (graph_metric_results) |*metric_result| metric_result.deinit(alloc);
        if (graph_metric_results.len > 0) alloc.free(graph_metric_results);
    }

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits_obj.total orelse 0),
        .graph_results = graph_results,
        .graph_metric_results = graph_metric_results,
    };
}

fn parseRemoteGraphMetricResults(
    alloc: std.mem.Allocator,
    value: std.json.ArrayHashMap(indexes_openapi.GraphMetricResult),
) ![]db_mod.types.GraphMetricResult {
    const results = try alloc.alloc(db_mod.types.GraphMetricResult, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*metric_result| metric_result.deinit(alloc);
        alloc.free(results);
    }

    var it = value.map.iterator();
    while (it.next()) |entry| {
        const result_value = entry.value_ptr.*;
        const scores = try alloc.alloc(db_mod.types.GraphMetricScore, result_value.scores.len);
        var initialized_scores: usize = 0;
        errdefer {
            for (scores[0..initialized_scores]) |*score| score.deinit(alloc);
            alloc.free(scores);
        }
        for (result_value.scores, 0..) |score, i| {
            scores[i] = .{
                .node = try alloc.dupe(u8, score.node),
                .score = score.score,
            };
            initialized_scores += 1;
        }

        results[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .index_name = try alloc.dupe(u8, result_value.index_name),
            .metric_name = try alloc.dupe(u8, result_value.metric),
            .scores = scores,
            .status = try parseRemoteGraphMetricStatusValue(alloc, result_value.metric, result_value.status),
        };
        initialized += 1;
    }

    return results;
}

fn parseRemoteGraphResults(
    alloc: std.mem.Allocator,
    value: std.json.ArrayHashMap(indexes_openapi.GraphQueryResult),
) ![]db_mod.types.GraphSearchResult {
    const results = try alloc.alloc(db_mod.types.GraphSearchResult, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*graph_result| graph_result.deinit(alloc);
        alloc.free(results);
    }

    var it = value.map.iterator();
    while (it.next()) |entry| {
        const result_value = entry.value_ptr.*;
        const parsed_nodes = if (result_value.nodes) |nodes_value|
            try parseRemoteGraphNodes(alloc, nodes_value)
        else
            ParsedRemoteGraphNodes{};
        errdefer parsed_nodes.deinit(alloc);
        const parsed_matches = if (result_value.matches) |matches_value|
            try parseRemoteGraphMatches(alloc, matches_value)
        else
            ParsedRemoteGraphMatches{};
        errdefer parsed_matches.deinit(alloc);
        const paths: []graph_paths.Path = if (result_value.paths) |paths_value|
            try parseRemoteGraphPaths(alloc, paths_value)
        else
            @constCast((&[_]graph_paths.Path{})[0..]);
        errdefer {
            for (paths) |path| graph_paths.freePath(alloc, path);
            if (paths.len > 0) alloc.free(paths);
        }

        results[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .nodes = parsed_nodes.nodes,
            .paths = paths,
            .matches = parsed_matches.matches,
            .hits = try concatGraphResultHits(alloc, parsed_nodes.hits, parsed_matches.hits),
            .total_hits = @intCast(result_value.total),
            .metric_status = try parseRemoteGraphMetricStatusMap(alloc, result_value.metric_status),
        };
        initialized += 1;
    }

    return results;
}

const ParsedRemoteGraphNodes = struct {
    nodes: []graph_query_mod.GraphResultNode = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    fn deinit(self: ParsedRemoteGraphNodes, alloc: std.mem.Allocator) void {
        for (self.nodes) |*node| node.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

const ParsedRemoteGraphMatches = struct {
    matches: []db_mod.types.GraphPatternMatch = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    fn deinit(self: ParsedRemoteGraphMatches, alloc: std.mem.Allocator) void {
        for (self.matches) |*match| match.deinit(alloc);
        if (self.matches.len > 0) alloc.free(self.matches);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

fn parseRemoteGraphNodes(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphResultNode,
) !ParsedRemoteGraphNodes {
    const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, value.len);
    var initialized: usize = 0;
    errdefer {
        for (nodes[0..initialized]) |*node| node.deinit(alloc);
        alloc.free(nodes);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (value, 0..) |item, i| {
        nodes[i] = .{
            .key = try alloc.dupe(u8, item.key),
            .depth = @intCast(item.depth orelse 0),
            .distance = item.distance orelse 0,
            .path = if (item.path) |path| try cloneRemoteGraphNodePath(alloc, path) else null,
            .path_edges = if (item.path_edges) |path_edges| try cloneRemoteGraphNodePathEdges(alloc, path_edges) else null,
            .provenance = if (item.provenance) |provenance| try cloneRemoteGraphNodePath(alloc, provenance) else null,
            .metrics = try parseRemoteGraphNodeMetrics(alloc, item.metrics),
        };
        if (item.document) |document| {
            try hits.append(alloc, .{
                .id = try alloc.dupe(u8, item.key),
                .score = null,
                .stored_data = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})}),
            });
        }
        initialized += 1;
    }
    return .{
        .nodes = nodes,
        .hits = try hits.toOwnedSlice(alloc),
    };
}

fn parseRemoteGraphNodeWithKey(
    alloc: std.mem.Allocator,
    key: []const u8,
    item: indexes_openapi.GraphResultNode,
) !graph_query_mod.GraphResultNode {
    return .{
        .key = try alloc.dupe(u8, key),
        .depth = @intCast(item.depth orelse 0),
        .distance = item.distance orelse 0,
        .path = if (item.path) |path| try cloneRemoteGraphNodePath(alloc, path) else null,
        .path_edges = if (item.path_edges) |path_edges| try cloneRemoteGraphNodePathEdges(alloc, path_edges) else null,
        .provenance = if (item.provenance) |provenance| try cloneRemoteGraphNodePath(alloc, provenance) else null,
        .metrics = try parseRemoteGraphNodeMetrics(alloc, item.metrics),
    };
}

fn parseRemoteGraphNodeMetrics(
    alloc: std.mem.Allocator,
    value: ?std.json.Value,
) ![]graph_query_mod.GraphMetricValue {
    const metrics_value = value orelse return &.{};
    if (metrics_value != .object) return error.InvalidQueryResponse;
    const metrics = metrics_value.object;
    const out = try alloc.alloc(graph_query_mod.GraphMetricValue, metrics.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*metric| metric.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = metrics.iterator();
    while (it.next()) |entry| {
        const score: ?f64 = switch (entry.value_ptr.*) {
            .null => null,
            .float => |score| score,
            .integer => |score| @floatFromInt(score),
            else => return error.InvalidQueryResponse,
        };
        out[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .score = score,
        };
        initialized += 1;
    }
    return out;
}

fn parseRemoteGraphMetricStatusMap(
    alloc: std.mem.Allocator,
    value: ?std.json.ArrayHashMap(indexes_openapi.GraphMetricStatus),
) ![]db_mod.types.GraphMetricStatus {
    const statuses = value orelse return &.{};
    const out = try alloc.alloc(db_mod.types.GraphMetricStatus, statuses.map.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*status| status.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = statuses.map.iterator();
    while (it.next()) |entry| {
        const status = entry.value_ptr.*;
        const name = try alloc.dupe(u8, entry.key_ptr.*);
        var name_moved = false;
        errdefer if (!name_moved) alloc.free(name);
        var edge_filter = try parseRemoteGraphMetricEdgeFilterStatus(alloc, status.edge_filter);
        var edge_filter_moved = false;
        errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
        out[initialized] = try parseRemoteGraphMetricStatusValueWithOwnedName(alloc, name, status, edge_filter);
        name_moved = true;
        edge_filter_moved = true;
        initialized += 1;
    }
    return out;
}

fn parseRemoteGraphMetricStatusValue(
    alloc: std.mem.Allocator,
    metric_name: []const u8,
    status: indexes_openapi.GraphMetricStatus,
) !db_mod.types.GraphMetricStatus {
    const name = try alloc.dupe(u8, metric_name);
    var name_moved = false;
    errdefer if (!name_moved) alloc.free(name);
    var edge_filter = try parseRemoteGraphMetricEdgeFilterStatus(alloc, status.edge_filter);
    var edge_filter_moved = false;
    errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
    const out = try parseRemoteGraphMetricStatusValueWithOwnedName(alloc, name, status, edge_filter);
    name_moved = true;
    edge_filter_moved = true;
    return out;
}

fn parseRemoteGraphMetricStatusValueWithOwnedName(
    alloc: std.mem.Allocator,
    owned_name: []u8,
    status: indexes_openapi.GraphMetricStatus,
    owned_edge_filter: graph_mod.GraphMetricEdgeFilter,
) !db_mod.types.GraphMetricStatus {
    const last_error = if (status.last_error) |last_error|
        try alloc.dupe(u8, last_error)
    else
        "";
    var last_error_moved = false;
    errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
    const build_worker_id = if (status.build_worker_id) |worker_id|
        try alloc.dupe(u8, worker_id)
    else
        "";
    var build_worker_id_moved = false;
    errdefer if (!build_worker_id_moved and build_worker_id.len > 0) alloc.free(build_worker_id);
    const out = db_mod.types.GraphMetricStatus{
        .name = owned_name,
        .state = graphMetricStateFromName(status.state) orelse return error.InvalidQueryRequest,
        .phase = graphMetricPhaseFromName(status.phase) orelse return error.InvalidQueryRequest,
        .edge_filter = owned_edge_filter,
        .metadata_version = @intCast(@max(status.metadata_version orelse 0, 0)),
        .maintenance_paused = status.maintenance_paused orelse false,
        .build_queued = status.build_queued,
        .published_generation = @intCast(@max(status.published_generation, 0)),
        .edge_generation = @intCast(@max(status.edge_generation, 0)),
        .target_edge_generation = @intCast(@max(status.target_edge_generation, 0)),
        .queued_generation = @intCast(@max(status.queued_generation orelse 0, 0)),
        .building_generation = @intCast(@max(status.building_generation orelse 0, 0)),
        .build_job_id = @intCast(@max(status.build_job_id orelse 0, 0)),
        .build_started_at_ms = @intCast(@max(status.build_started_at_ms orelse 0, 0)),
        .build_iteration = @intCast(@max(status.build_iteration orelse 0, 0)),
        .build_lease_expires_at_ms = @intCast(@max(status.build_lease_expires_at_ms orelse 0, 0)),
        .build_worker_id = build_worker_id,
        .retry_count = @intCast(@max(status.retry_count orelse 0, 0)),
        .last_error = last_error,
        .progress = status.progress,
        .converged = status.converged,
        .iterations_completed = @intCast(@max(status.iterations_completed, 0)),
        .delta = status.delta,
        .computed_at_ms = @intCast(@max(status.computed_at_ms, 0)),
        .last_event = try parseRemoteGraphMetricEvent(status.last_event),
        .recent_events = try parseRemoteGraphMetricEvents(alloc, status.recent_events),
    };
    last_error_moved = true;
    build_worker_id_moved = true;
    return out;
}

fn parseRemoteGraphMetricEdgeFilterStatus(
    alloc: std.mem.Allocator,
    maybe_filter: ?indexes_openapi.GraphMetricEdgeFilterStatus,
) !graph_mod.GraphMetricEdgeFilter {
    const filter = maybe_filter orelse return .{};
    if (std.mem.eql(u8, filter.mode, "all")) return .{};
    if (!std.mem.eql(u8, filter.mode, "types")) return error.InvalidQueryRequest;
    const raw_types = filter.types orelse return error.InvalidQueryRequest;
    if (raw_types.len == 0) return error.InvalidQueryRequest;
    const types = try alloc.alloc([]const u8, raw_types.len);
    var initialized: usize = 0;
    errdefer {
        for (types[0..initialized]) |edge_type| alloc.free(edge_type);
        alloc.free(types);
    }
    for (raw_types, 0..) |edge_type, i| {
        if (edge_type.len == 0) return error.InvalidQueryRequest;
        types[i] = try alloc.dupe(u8, edge_type);
        initialized += 1;
    }
    return .{ .mode = .types, .types = types };
}

fn parseRemoteGraphMetricEvent(
    maybe_event: ?indexes_openapi.GraphMetricEvent,
) !?graph_mod.GraphIndex.GraphMetricEvent {
    const event = maybe_event orelse return null;
    return try parseRemoteGraphMetricEventValue(event);
}

fn parseRemoteGraphMetricEventValue(
    event: indexes_openapi.GraphMetricEvent,
) !graph_mod.GraphIndex.GraphMetricEvent {
    return .{
        .sequence = @intCast(@max(event.sequence, 0)),
        .kind = graphMetricEventKindFromName(event.kind) orelse return error.InvalidQueryRequest,
        .at_ms = @intCast(@max(event.at_ms, 0)),
        .target_edge_generation = @intCast(@max(event.target_edge_generation, 0)),
        .published_generation = @intCast(@max(event.published_generation, 0)),
        .score_count = @intCast(@max(event.score_count, 0)),
    };
}

fn parseRemoteGraphMetricEvents(
    alloc: std.mem.Allocator,
    maybe_events: ?[]const indexes_openapi.GraphMetricEvent,
) ![]graph_mod.GraphIndex.GraphMetricEvent {
    const events = maybe_events orelse return &.{};
    const out = try alloc.alloc(graph_mod.GraphIndex.GraphMetricEvent, events.len);
    for (events, 0..) |event, i| {
        out[i] = try parseRemoteGraphMetricEventValue(event);
    }
    return out;
}

fn graphMetricEventKindFromName(name: []const u8) ?graph_mod.GraphIndex.GraphMetricEventKind {
    if (std.mem.eql(u8, name, "publish")) return .publish;
    if (std.mem.eql(u8, name, "delete")) return .delete;
    if (std.mem.eql(u8, name, "pause")) return .pause;
    if (std.mem.eql(u8, name, "resume")) return .@"resume";
    if (std.mem.eql(u8, name, "failed")) return .failed;
    return null;
}

fn graphMetricStateFromName(name: []const u8) ?graph_mod.GraphIndex.GraphMetricState {
    if (std.mem.eql(u8, name, "disabled")) return .disabled;
    if (std.mem.eql(u8, name, "not_ready")) return .not_ready;
    if (std.mem.eql(u8, name, "fresh")) return .fresh;
    if (std.mem.eql(u8, name, "stale")) return .stale;
    if (std.mem.eql(u8, name, "building")) return .building;
    if (std.mem.eql(u8, name, "failed")) return .failed;
    return null;
}

fn graphMetricPhaseFromName(name: []const u8) ?graph_mod.GraphIndex.GraphMetricBuildPhase {
    if (std.mem.eql(u8, name, "idle")) return .idle;
    if (std.mem.eql(u8, name, "computing")) return .computing;
    if (std.mem.eql(u8, name, "publishing")) return .publishing;
    if (std.mem.eql(u8, name, "complete")) return .complete;
    if (std.mem.eql(u8, name, "prepare_generation")) return .prepare_generation;
    if (std.mem.eql(u8, name, "scan_edges_and_out_degree")) return .scan_edges_and_out_degree;
    if (std.mem.eql(u8, name, "initialize_ranks")) return .initialize_ranks;
    if (std.mem.eql(u8, name, "iterate_contributions")) return .iterate_contributions;
    if (std.mem.eql(u8, name, "reduce_ranks")) return .reduce_ranks;
    if (std.mem.eql(u8, name, "check_convergence")) return .check_convergence;
    if (std.mem.eql(u8, name, "publish_generation")) return .publish_generation;
    if (std.mem.eql(u8, name, "cleanup_old_generations")) return .cleanup_old_generations;
    return null;
}

fn parseRemoteGraphMatches(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PatternMatch,
) !ParsedRemoteGraphMatches {
    const matches = try alloc.alloc(db_mod.types.GraphPatternMatch, value.len);
    var initialized_matches: usize = 0;
    errdefer {
        for (matches[0..initialized_matches]) |*match| match.deinit(alloc);
        alloc.free(matches);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (value, 0..) |item, i| {
        const bindings_value = item.bindings orelse return error.InvalidQueryRequest;

        const bindings = try alloc.alloc(db_mod.types.GraphPatternBinding, bindings_value.map.count());
        var initialized_bindings: usize = 0;
        errdefer {
            for (bindings[0..initialized_bindings]) |*binding| binding.deinit(alloc);
            if (bindings.len > 0) alloc.free(bindings);
        }

        var binding_it = bindings_value.map.iterator();
        while (binding_it.next()) |binding_entry| {
            const node_value = binding_entry.value_ptr.*;
            const node = try parseRemoteGraphNodeWithKey(alloc, node_value.key, node_value);
            bindings[initialized_bindings] = .{
                .alias = try alloc.dupe(u8, binding_entry.key_ptr.*),
                .node = node,
            };
            if (node_value.document) |document| {
                try hits.append(alloc, .{
                    .id = try alloc.dupe(u8, node_value.key),
                    .score = null,
                    .stored_data = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})}),
                });
            }
            initialized_bindings += 1;
        }

        matches[i] = .{
            .bindings = bindings,
            .path = if (item.path) |path_value| try cloneRemoteGraphNodePathEdges(alloc, path_value) else @constCast((&[_]graph_query_mod.PathEdgeInfo{})[0..]),
        };
        initialized_matches += 1;
    }

    return .{
        .matches = matches,
        .hits = try hits.toOwnedSlice(alloc),
    };
}

fn concatGraphResultHits(
    alloc: std.mem.Allocator,
    left: []db_mod.types.SearchHit,
    right: []db_mod.types.SearchHit,
) ![]db_mod.types.SearchHit {
    const out = try alloc.alloc(db_mod.types.SearchHit, left.len + right.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*hit| hit.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (left) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    for (right) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    return out;
}

fn cloneRemoteGraphNodePath(alloc: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, value.len);
    errdefer alloc.free(out);
    for (value, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
    }
    return out;
}

fn cloneRemoteGraphNodePathEdges(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PathEdge,
) ![]graph_query_mod.PathEdgeInfo {
    const edges = try alloc.alloc(graph_query_mod.PathEdgeInfo, value.len);
    errdefer alloc.free(edges);
    for (value, 0..) |item, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, item.source orelse return error.InvalidQueryRequest),
            .target = try alloc.dupe(u8, item.target orelse return error.InvalidQueryRequest),
            .edge_type = try alloc.dupe(u8, item.type orelse return error.InvalidQueryRequest),
            .weight = item.weight orelse return error.InvalidQueryRequest,
        };
    }
    return edges;
}

fn parseRemoteGraphPaths(alloc: std.mem.Allocator, value: []const indexes_openapi.Path) ![]graph_paths.Path {
    const paths = try alloc.alloc(graph_paths.Path, value.len);
    var initialized: usize = 0;
    errdefer {
        for (paths[0..initialized]) |path| graph_paths.freePath(alloc, path);
        alloc.free(paths);
    }
    for (value, 0..) |item, i| {
        paths[i] = .{
            .nodes = try cloneRemoteGraphNodePath(alloc, item.nodes orelse return error.InvalidQueryRequest),
            .edges = try parseRemotePathEdges(alloc, item.edges orelse return error.InvalidQueryRequest),
            .total_weight = item.total_weight orelse return error.InvalidQueryRequest,
            .length = @intCast(item.length orelse return error.InvalidQueryRequest),
        };
        initialized += 1;
    }
    return paths;
}

fn parseRemotePathEdges(alloc: std.mem.Allocator, value: []const indexes_openapi.PathEdge) ![]graph_paths.PathEdge {
    const edges = try alloc.alloc(graph_paths.PathEdge, value.len);
    errdefer alloc.free(edges);
    for (value, 0..) |item, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, item.source orelse return error.InvalidQueryRequest),
            .target = try alloc.dupe(u8, item.target orelse return error.InvalidQueryRequest),
            .edge_type = try alloc.dupe(u8, item.type orelse return error.InvalidQueryRequest),
            .weight = item.weight orelse return error.InvalidQueryRequest,
        };
    }
    return edges;
}

pub fn appendJsonFieldName(
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

pub fn appendJsonFieldString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

pub fn appendJsonFieldU32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldF32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldF64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldBool(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: bool,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

pub fn appendJsonFieldNames(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    fields: []const []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, field);
    }
    try out.append(alloc, ']');
}

pub fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

const CivilDate = struct {
    year: i64,
    month: i64,
    day: i64,
};

fn formatRfc3339Ns(alloc: std.mem.Allocator, value_ns: u64) ![]u8 {
    const secs_total: u64 = @divFloor(value_ns, std.time.ns_per_s);
    const nanos: u64 = @mod(value_ns, std.time.ns_per_s);
    const days: i64 = @intCast(@divFloor(secs_total, 86_400));
    const secs_of_day: u64 = @mod(secs_total, 86_400);
    const date = civilFromDays(days);
    const year: u64 = @intCast(date.year);
    const month: u64 = @intCast(date.month);
    const day: u64 = @intCast(date.day);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    if (nanos == 0) {
        return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            year, month, day, hour, minute, second,
        });
    }
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        year, month, day, hour, minute, second, nanos,
    });
}

fn civilFromDays(days_since_epoch: i64) CivilDate {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    const year = y + (if (month <= 2) @as(i64, 1) else @as(i64, 0));
    return .{ .year = year, .month = month, .day = day };
}

fn parseJsonTestBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
}

test "remote query parser preserves graph metric results" {
    const alloc = std.testing.allocator;
    var parsed = try parseRemoteSearchResult(alloc,
        \\{"responses":[{"hits":{"total":0,"hits":[]},"graph_metric_results":{"central":{"index_name":"graph_idx","metric":"pagerank","scores":[{"node":"doc:b","score":0.8},{"node":"doc:a","score":0.9}],"status":{"state":"fresh","phase":"complete","maintenance_paused":false,"build_queued":false,"published_generation":7,"edge_generation":7,"target_edge_generation":7,"queued_generation":0,"building_generation":0,"build_job_id":12345,"build_started_at_ms":1780000000123,"build_lease_expires_at_ms":0,"progress":1.0,"converged":true,"iterations_completed":12,"delta":0.0,"computed_at_ms":1780000000000}}},"took":0,"status":200}]}
    );
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 1), parsed.graph_metric_results.len);
    const result = parsed.graph_metric_results[0];
    try std.testing.expectEqualStrings("central", result.name);
    try std.testing.expectEqualStrings("graph_idx", result.index_name);
    try std.testing.expectEqualStrings("pagerank", result.metric_name);
    try std.testing.expectEqual(@as(usize, 2), result.scores.len);
    try std.testing.expectEqualStrings("doc:b", result.scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), result.scores[0].score, 0.001);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, result.status.state);
    try std.testing.expectEqual(@as(u32, 0), result.status.metadata_version);
    try std.testing.expectEqual(@as(u64, 7), result.status.published_generation);
    try std.testing.expectEqual(@as(u64, 12345), result.status.build_job_id);
    try std.testing.expectEqual(@as(u64, 1780000000123), result.status.build_started_at_ms);
}

test "encode query request round-trips composed bleve full_text queries" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{
            .bool_query = .{
                .must = &.{
                    .{ .match = .{ .field = "body", .text = "hello" } },
                    .{ .numeric_range = .{
                        .field = "score",
                        .min = 10,
                        .max = 20,
                        .inclusive_max = true,
                    } },
                },
                .must_not = &.{
                    .{ .date_range = .{
                        .field = "created_at",
                        .start_ns = 1_772_323_200 * std.time.ns_per_s,
                        .inclusive_end = true,
                    } },
                },
            },
        },
        .limit = 5,
        .identity_read_generation = 77,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const full_text = parsed.value.object.get("full_text_search").?.object;
    try std.testing.expectEqual(@as(i64, 77), parsed.value.object.get("_identity_read_generation").?.integer);
    const must = full_text.get("must").?.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), must.len);
    try std.testing.expectEqual(true, must[1].object.get("inclusive_max").?.bool);
    try std.testing.expectEqualStrings("2026-03-01T00:00:00Z", full_text.get("must_not").?.object.get("disjuncts").?.array.items[0].object.get("start").?.string);
    try std.testing.expect(full_text.get("fuzziness") == null);

    const fuzzy = try encodeQueryRequest(alloc, .{
        .full_text = .{
            .fuzzy = .{
                .field = "body",
                .term = "helo",
                .max_edits = 1,
            },
        },
    });
    defer alloc.free(fuzzy);
    var parsed_fuzzy = try parseJsonTestBody(std.json.Value, alloc, fuzzy);
    defer parsed_fuzzy.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_fuzzy.value.object.get("full_text_search").?.object.get("fuzziness").?.integer);
}

test "encode query request includes named vector embeddings for routed semantic search" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .dense_queries = &.{
            .{
                .name = "semantic_idx",
                .index_name = "semantic_idx",
                .query = .{
                    .vector = &.{ 0.25, 0.5, 0.75 },
                    .k = 4,
                },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sparse_idx",
                .index_name = "sparse_idx",
                .query = .{
                    .indices = &.{ 1, 7 },
                    .values = &.{ 0.4, 0.9 },
                    .k = 4,
                },
            },
        },
        .limit = 4,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const embeddings = parsed.value.object.get("embeddings").?.object;
    const dense = embeddings.get("semantic_idx").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), dense.len);
    try std.testing.expectEqual(@as(f64, 0.25), dense[0].float);
    try std.testing.expectEqual(@as(f64, 0.75), dense[2].float);
    const sparse = embeddings.get("sparse_idx").?.object;
    try std.testing.expectEqual(@as(i64, 1), sparse.get("indices").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 7), sparse.get("indices").?.array.items[1].integer);
    try std.testing.expectEqual(@as(f64, 0.4), sparse.get("values").?.array.items[0].float);
    try std.testing.expectEqual(@as(f64, 0.9), sparse.get("values").?.array.items[1].float);
}

test "encode query request includes graph metric read and rerank" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "pagerank",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 25,
            .freshness = .fresh,
        },
    }};
    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{ .match_all = {} },
        .graph_metric_queries = &graph_metric_queries,
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
            .base_weight = 0.5,
            .weight = 2.5,
            .missing_score = -0.25,
        },
        .limit = 25,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const graph_metric = parsed.value.object.get("graph_metric").?.object;
    try std.testing.expectEqualStrings("pagerank", graph_metric.get("name").?.string);
    try std.testing.expectEqualStrings("graph_idx", graph_metric.get("index").?.string);
    try std.testing.expectEqualStrings("pagerank", graph_metric.get("metric").?.string);
    try std.testing.expectEqual(@as(i64, 25), graph_metric.get("top_k").?.integer);
    try std.testing.expectEqualStrings("fresh", graph_metric.get("metric_freshness").?.string);
    const rerank = parsed.value.object.get("graph_metric_rerank").?.object;
    try std.testing.expectEqualStrings("graph_idx", rerank.get("index").?.string);
    try std.testing.expectEqualStrings("pagerank", rerank.get("metric").?.string);
    try std.testing.expectEqual(@as(f64, 0.5), rerank.get("base_weight").?.float);
    try std.testing.expectEqual(@as(f64, 2.5), rerank.get("weight").?.float);
    try std.testing.expectEqual(@as(f64, -0.25), rerank.get("missing_score").?.float);
    try std.testing.expectEqualStrings("fresh", rerank.get("metric_freshness").?.string);
}

test "encode query request includes merge config and pruner but omits reranker" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .merge_config = .{
            .strategy = .rsf,
            .window_size = 25,
            .rank_constant = 42.0,
            .weights = &.{
                .{ .name = "full_text", .weight = 0.5 },
                .{ .name = "semantic_idx", .weight = 1.5 },
            },
        },
        .pruner = .{
            .min_score_ratio = 0.5,
            .require_multi_index = true,
        },
        .reranker = .{
            .provider = .antfly,
            .model = "cross-encoder/ms-marco-MiniLM-L-6-v2",
            .field = "body",
        },
        .reranker_query_text = "hello",
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const merge_config = parsed.value.object.get("merge_config").?.object;
    try std.testing.expectEqualStrings("rsf", merge_config.get("strategy").?.string);
    try std.testing.expectEqual(@as(f64, 0.5), merge_config.get("weights").?.object.get("full_text").?.float);
    try std.testing.expectEqual(@as(f64, 1.5), merge_config.get("weights").?.object.get("semantic_idx").?.float);
    const pruner = parsed.value.object.get("pruner").?.object;
    try std.testing.expectEqual(@as(f64, 0.5), pruner.get("min_score_ratio").?.float);
    try std.testing.expectEqual(true, pruner.get("require_multi_index").?.bool);
    try std.testing.expect(parsed.value.object.get("reranker") == null);
}

test "encode query request includes distributed text stats for internal shard scoring" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello world" } },
        .distributed_text_stats = &.{.{
            .field = "body",
            .global_doc_count = 9,
            .global_total_field_len = 45,
            .term_doc_freqs = &.{
                .{ .term = "hello", .doc_freq = 4 },
                .{ .term = "world", .doc_freq = 2 },
            },
        }},
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const stats = parsed.value.object.get("_distributed_text_stats").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), stats.len);
    try std.testing.expectEqualStrings("body", stats[0].object.get("field").?.string);
    try std.testing.expectEqual(@as(i64, 9), stats[0].object.get("global_doc_count").?.integer);
    try std.testing.expectEqual(@as(i64, 45), stats[0].object.get("global_total_field_len").?.integer);
    const freqs = stats[0].object.get("term_doc_freqs").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), freqs.len);
    try std.testing.expectEqualStrings("hello", freqs[0].object.get("term").?.string);
    try std.testing.expectEqual(@as(i64, 4), freqs[0].object.get("doc_freq").?.integer);
}

test "encode query request with distributed text stats parses through query contract" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "hello world" } },
        .fields = &.{"title"},
        .include_all_fields = false,
        .limit = 7,
        .distributed_text_stats = &.{.{
            .field = "body",
            .global_doc_count = 9,
            .global_total_field_len = 45,
            .term_doc_freqs = &.{
                .{ .term = "hello", .doc_freq = 4 },
                .{ .term = "world", .doc_freq = 2 },
            },
        }},
    });
    defer alloc.free(encoded);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 7), owned.req.limit);
    try std.testing.expectEqual(@as(usize, 1), owned.fields.len);
    try std.testing.expectEqualStrings("title", owned.fields[0]);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expectEqual(@as(usize, 1), owned.req.distributed_text_stats.len);
    try std.testing.expectEqualStrings("body", owned.req.distributed_text_stats[0].field);
    try std.testing.expectEqual(@as(u32, 9), owned.req.distributed_text_stats[0].global_doc_count);
    try std.testing.expectEqual(@as(u64, 45), owned.req.distributed_text_stats[0].global_total_field_len);
    try std.testing.expectEqual(@as(usize, 2), owned.req.distributed_text_stats[0].term_doc_freqs.len);
    try std.testing.expectEqualStrings("hello", owned.req.distributed_text_stats[0].term_doc_freqs[0].term);
}

test "encode query request carries internal native doc id constraints through query contract" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .dense_queries = &.{
            .{
                .name = "semantic_idx",
                .index_name = "semantic_idx",
                .query = .{
                    .vector = &.{ 0.25, 0.5 },
                    .k = 5,
                },
            },
        },
        .filter_doc_ids = &.{ "doc:a", "doc:b" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:c"},
        .limit = 5,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const constraints = parsed.value.object.get("native_doc_id_constraints").?.object;
    try std.testing.expectEqual(true, constraints.get("positive_filter").?.bool);
    try std.testing.expectEqualStrings("doc:a", constraints.get("include_doc_ids").?.array.items[0].string);
    try std.testing.expectEqualStrings("doc:c", constraints.get("exclude_doc_ids").?.array.items[0].string);
    try std.testing.expect(parsed.value.object.get("_filter_doc_ids_positive") == null);
    try std.testing.expect(parsed.value.object.get("_filter_doc_ids") == null);
    try std.testing.expect(parsed.value.object.get("_exclude_doc_ids") == null);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expect(owned.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 2), owned.req.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", owned.req.filter_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", owned.req.filter_doc_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), owned.req.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", owned.req.exclude_doc_ids[0]);
}

test "encode query request rejects in-memory resolved doc filters" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}) };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.UnsupportedQueryRequest, encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .resolved_doc_filter = &filter,
    }));
}

test "encode query request preserves empty positive internal doc id filter" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids_positive = true,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const constraints = parsed.value.object.get("native_doc_id_constraints").?.object;
    try std.testing.expectEqual(true, constraints.get("positive_filter").?.bool);
    try std.testing.expectEqual(@as(usize, 0), constraints.get("include_doc_ids").?.array.items.len);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expect(owned.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 0), owned.req.filter_doc_ids.len);
}

fn vectorWorkerTestFilterSupported(_: std.mem.Allocator, filter_query_json: []const u8) bool {
    return std.mem.indexOf(u8, filter_query_json, "\"wildcard\"") == null;
}

test "vector worker request lowers search request to envelope" {
    const alloc = std.testing.allocator;
    const body = (try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 11,
        .offset = 3,
        .count_only = true,
        .profile = true,
        .include_stored = false,
        .fields = &.{ "title", "score" },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_prefix = "tenant/a/",
        .filter_ids = &.{ 99, 42 },
        .exclude_ids = &.{7},
        .include_all_fields = false,
        .defer_stored_projection = true,
        .search_effort = 0.5,
        .distance_under = 0.9,
        .return_mode = .parent_with_chunks,
        .max_chunks_per_parent = 2,
        .identity_read_generation = 54321,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{ "doc:b", "doc:a" },
        .exclude_doc_ids = &.{"doc:c"},
    }, vectorWorkerTestFilterSupported)).?;
    defer alloc.free(body);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, body);
    defer envelope.deinit(alloc);
    try std.testing.expectEqualStrings("dense_idx", envelope.index_name);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.dense_vector, envelope.layout);
    try std.testing.expectEqual(@as(u32, 11), envelope.options.limit);
    try std.testing.expectEqual(@as(u32, 3), envelope.options.offset);
    try std.testing.expect(envelope.options.count_only);
    try std.testing.expect(envelope.options.profile);
    try std.testing.expect(!envelope.options.include_stored);
    try std.testing.expect(!envelope.options.include_all_fields);
    try std.testing.expect(envelope.options.defer_stored_projection);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", envelope.options.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", envelope.options.exclusion_query_json);
    try std.testing.expect(envelope.options.require_algebraic_filter_resolution);
    try std.testing.expectEqualStrings("tenant/a/", envelope.options.filter_prefix);
    try std.testing.expectEqual(@as(usize, 2), envelope.options.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 99), envelope.options.filter_ids[0]);
    try std.testing.expectEqual(@as(u64, 42), envelope.options.filter_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), envelope.options.exclude_ids.len);
    try std.testing.expectEqual(@as(u64, 7), envelope.options.exclude_ids[0]);
    try std.testing.expectEqual(@as(usize, 2), envelope.options.fields.len);
    try std.testing.expectEqualStrings("title", envelope.options.fields[0]);
    try std.testing.expectEqualStrings("score", envelope.options.fields[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), envelope.options.search_effort.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), envelope.options.distance_under.?, 0.0001);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, envelope.options.return_mode);
    try std.testing.expectEqual(@as(u32, 2), envelope.options.max_chunks_per_parent);
    try std.testing.expectEqual(@as(?u64, 54321), envelope.options.identity_read_generation);
    try std.testing.expect(envelope.native_doc_id_constraints.constraints.positive_filter);
    try std.testing.expectEqualStrings("doc:a", envelope.native_doc_id_constraints.constraints.include_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", envelope.native_doc_id_constraints.constraints.include_doc_ids[1]);
    try std.testing.expectEqualStrings("doc:c", envelope.native_doc_id_constraints.constraints.exclude_doc_ids[0]);
    try std.testing.expect((try envelope.proveTensorProgramAlloc(alloc)).safe());

    const supported_filter = try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 7,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
    }, vectorWorkerTestFilterSupported);
    try std.testing.expect(supported_filter != null);
    if (supported_filter) |body_supported| alloc.free(body_supported);

    const unsupported = try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .limit = 7,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    }, vectorWorkerTestFilterSupported);
    try std.testing.expect(unsupported == null);
}

test "vector worker preflight annotation tracks eligibility and symbolic filters" {
    const alloc = std.testing.allocator;

    var supported: db_mod.RuntimePreflightSummary = .{};
    defer supported.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &supported, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
        .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
        .filter_ids = &.{42},
        .exclude_ids = &.{7},
        .filter_doc_ids_positive = true,
        .filter_doc_ids = &.{"doc:a"},
        .exclude_doc_ids = &.{"doc:b"},
    }, vectorWorkerTestFilterSupported);
    try std.testing.expectEqual(@as(u32, 1), supported.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), supported.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 6), supported.vector_worker_filter_constraint_count);
    try std.testing.expect(supported.vector_worker_requires_algebraic_filter_resolution);

    var unsupported: db_mod.RuntimePreflightSummary = .{};
    defer unsupported.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &unsupported, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .filter_query_json = "{\"wildcard\":{\"/tenant\":\"*ice\"}}",
    }, vectorWorkerTestFilterSupported);
    try std.testing.expectEqual(@as(u32, 0), unsupported.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), unsupported.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 1), unsupported.vector_worker_filter_constraint_count);
    try std.testing.expect(unsupported.vector_worker_requires_algebraic_filter_resolution);

    var sentinel: u8 = 0;
    var resolved_filter: db_mod.RuntimePreflightSummary = .{};
    defer resolved_filter.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &resolved_filter, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &sentinel,
    }, vectorWorkerTestFilterSupported);
    try std.testing.expectEqual(@as(u32, 0), resolved_filter.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), resolved_filter.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 1), resolved_filter.vector_worker_filter_constraint_count);

    var non_vector: db_mod.RuntimePreflightSummary = .{};
    defer non_vector.deinit(alloc);
    annotateVectorWorkerPreflight(alloc, &non_vector, .{ .query = .{ .match_all = {} } }, vectorWorkerTestFilterSupported);
    try std.testing.expectEqual(@as(u32, 0), non_vector.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 0), non_vector.vector_worker_fallback_count);
}

test "remote wire doc identity query request serializes internal resolved doc filters with wire context" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 3 }) };
    defer filter.deinit(alloc);

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .identity_read_generation = 42,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    });
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"_resolved_doc_filter\"") != null);

    var parsed = try query_contract.parseQueryRequest(alloc, null, "docs", encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.req.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), parsed.req.identity_read_generation);
    try std.testing.expect(parsed.req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));

    const stats_body = try encodeQueryTextStatsRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .identity_read_generation = 42,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    });
    defer alloc.free(stats_body);
    try std.testing.expect(std.mem.indexOf(u8, stats_body, "\"_resolved_doc_filter\"") != null);

    var stats_parsed = try parseTextStatsRequest(alloc, "docs", stats_body);
    defer stats_parsed.deinit(alloc);
    const stats_query = stats_parsed.query_request.req;
    try std.testing.expect(stats_query.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), stats_query.identity_read_generation);
    try std.testing.expect(stats_query.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
}

test "remote wire doc identity vector worker request carries serializable resolved doc filter" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 1, 3 }) };
    defer filter.deinit(alloc);

    const body = (try encodeAlgebraicVectorWorkerRequestForSearchRequestAlloc(alloc, .{
        .index_name = "dense_idx",
        .identity_read_generation = 42,
        .query = .{ .dense_knn = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 42,
        },
    }, vectorWorkerTestFilterSupported)) orelse return error.TestUnexpectedResult;
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"_resolved_doc_filter\"") != null);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, body);
    defer envelope.deinit(alloc);
    const req = searchRequestFromVectorWorkerEnvelope(&envelope);
    try std.testing.expect(req.resolved_doc_filter != null);
    try std.testing.expectEqual(@as(?u64, 42), req.identity_read_generation);
    try std.testing.expect(req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
}

test "vector worker envelope converts to constrained search request" {
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
    const encoded = try query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .{
            .fields = @constCast((&[_][]const u8{"title"})[0..]),
            .filter_query_json = "{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}",
            .exclusion_query_json = "{\"term\":{\"path\":\"/deleted\",\"value\":true}}",
            .filter_prefix = "tenant/a/",
            .filter_ids = &.{ 42, 99 },
            .exclude_ids = &.{7},
            .require_algebraic_filter_resolution = true,
            .include_all_fields = false,
            .defer_stored_projection = true,
            .limit = 8,
            .offset = 1,
            .profile = true,
            .include_stored = false,
            .search_effort = 0.5,
            .distance_over = 0.1,
            .distance_under = 0.9,
            .return_mode = .parent_with_chunks,
            .max_chunks_per_parent = 2,
            .identity_read_generation = 12345,
        },
        .{
            .positive_filter = true,
            .include_doc_ids = &.{ "doc:a", "doc:b" },
            .exclude_doc_ids = &.{"doc:c"},
        },
        null,
        null,
        &.{access_path},
        program,
    );
    defer alloc.free(encoded);

    var envelope = try query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, encoded);
    defer envelope.deinit(alloc);
    const req = searchRequestFromVectorWorkerEnvelope(&envelope);

    try std.testing.expectEqualStrings("dense_idx", req.index_name.?);
    try std.testing.expectEqual(@as(u32, 8), req.limit);
    try std.testing.expectEqual(@as(u32, 1), req.offset);
    try std.testing.expect(req.profile);
    try std.testing.expect(!req.include_stored);
    try std.testing.expect(!req.include_all_fields);
    try std.testing.expect(req.defer_stored_projection);
    try std.testing.expectEqual(@as(usize, 1), req.fields.len);
    try std.testing.expectEqualStrings("title", req.fields[0]);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/tenant\",\"value\":\"t1\"}}", req.filter_query_json);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/deleted\",\"value\":true}}", req.exclusion_query_json);
    try std.testing.expect(req.require_algebraic_filter_resolution);
    try std.testing.expectEqualStrings("tenant/a/", req.filter_prefix);
    try std.testing.expectEqual(@as(usize, 2), req.filter_ids.len);
    try std.testing.expectEqual(@as(u64, 42), req.filter_ids[0]);
    try std.testing.expectEqual(@as(u64, 99), req.filter_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), req.exclude_ids.len);
    try std.testing.expectEqual(@as(u64, 7), req.exclude_ids[0]);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), req.search_effort.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), req.distance_over.?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), req.distance_under.?, 0.0001);
    try std.testing.expectEqual(db_mod.types.ReturnMode.parent_with_chunks, req.return_mode);
    try std.testing.expectEqual(@as(u32, 2), req.max_chunks_per_parent);
    try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
    try std.testing.expect(req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 2), req.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", req.filter_doc_ids[0]);
    try std.testing.expectEqual(@as(usize, 1), req.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", req.exclude_doc_ids[0]);
    switch (req.query) {
        .dense_knn => |dense| {
            try std.testing.expectEqual(@as(u32, 7), dense.k);
            try std.testing.expectEqual(@as(usize, 2), dense.vector.len);
        },
        else => return error.TestUnexpectedResult,
    }
}
