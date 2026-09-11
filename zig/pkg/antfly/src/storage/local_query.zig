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

//! Physical local-query execution. Routing and owner acquisition belong to callers.

const std = @import("std");
const graph_mod = @import("../graph/graph.zig");
const query_api = @import("../api/query.zig");
const query_contract = @import("../api/query_contract.zig");
const distributed_graph = @import("../api/distributed_graph.zig");
const platform_time = @import("antfly_platform").time;
const distributed_stats_mod = @import("../search/distributed_stats.zig");
const db_mod = @import("antfly_source_root").antfly_sources.selected_db;
const db_query_search = @import("db/query/search_exec.zig");
const contract = @import("../api/local_query_contract.zig");
const AlgebraicPartialsRequestInput = contract.AlgebraicPartialsRequestInput;
const AlgebraicTensorAccessPathInput = contract.AlgebraicTensorAccessPathInput;
const AlgebraicTensorExprInput = contract.AlgebraicTensorExprInput;
const AlgebraicTensorProgramInput = contract.AlgebraicTensorProgramInput;
const BackgroundTextStatsFieldRequestInput = contract.BackgroundTextStatsFieldRequestInput;
const OwnedAlgebraicTensorAccessPath = contract.OwnedAlgebraicTensorAccessPath;
const OwnedAlgebraicTensorExpr = contract.OwnedAlgebraicTensorExpr;
const OwnedAlgebraicTensorProgram = contract.OwnedAlgebraicTensorProgram;
const OwnedBackgroundTextStatsFieldRequest = contract.OwnedBackgroundTextStatsFieldRequest;
const OwnedTextStatsFieldRequest = contract.OwnedTextStatsFieldRequest;
const ParsedAlgebraicPartialsRequest = contract.ParsedAlgebraicPartialsRequest;
const ParsedBackgroundTextStatsRequest = contract.ParsedBackgroundTextStatsRequest;
const ParsedExplicitTextStatsRequest = contract.ParsedExplicitTextStatsRequest;
const ParsedTextStatsRequest = contract.ParsedTextStatsRequest;
const StorageKernelPreflightWireRequest = contract.StorageKernelPreflightWireRequest;
const TextStatsFieldRequestInput = contract.TextStatsFieldRequestInput;
const TextStatsRequestInput = contract.TextStatsRequestInput;
const TextStatsRequestMode = contract.TextStatsRequestMode;
const algebraicTensorAccessPathListHas = contract.algebraicTensorAccessPathListHas;
const algebraicTensorAccessPathMatches = contract.algebraicTensorAccessPathMatches;
const algebraicTensorAccessPathValue = contract.algebraicTensorAccessPathValue;
const algebraicTensorAccessPathValuesAlloc = contract.algebraicTensorAccessPathValuesAlloc;
const algebraicTensorExprValue = contract.algebraicTensorExprValue;
const algebraic_ir = contract.algebraic_ir;
const algebraic_law = contract.algebraic_law;
const algebraic_planner = @import("db/algebraic/planner.zig");
const appendJsonFieldName = contract.appendJsonFieldName;
const appendJsonFieldString = contract.appendJsonFieldString;
const appendJsonFieldU32 = contract.appendJsonFieldU32;
const appendJsonFieldU64 = contract.appendJsonFieldU64;
const appendJsonString = contract.appendJsonString;
const checkQueryDeadline = contract.checkQueryDeadline;
const encodeAlgebraicPartialsResponse = contract.encodeAlgebraicPartialsResponse;
const encodeBackgroundTextStatsResponse = contract.encodeBackgroundTextStatsResponse;
const encodeTextStatsResponse = contract.encodeTextStatsResponse;
const graphHydrateRequestHasResolvedDocFilter = contract.graphHydrateRequestHasResolvedDocFilter;
const graphHydrateSearchRequest = contract.graphHydrateSearchRequest;
const identityGenerationFromTextStatsResolvedFilter = contract.identityGenerationFromTextStatsResolvedFilter;
const lawIdSlicesEqual = contract.lawIdSlicesEqual;
const optionalDictionaryEqual = contract.optionalDictionaryEqual;
const parseAlgebraicPartialsRequest = contract.parseAlgebraicPartialsRequest;
const parseAlgebraicTensorAccessPathAlloc = contract.parseAlgebraicTensorAccessPathAlloc;
const parseBackgroundQueryRequestAlloc = contract.parseBackgroundQueryRequestAlloc;
const parseTextStatsRequest = contract.parseTextStatsRequest;
const parsedAlgebraicTensorExpressionsAlloc = contract.parsedAlgebraicTensorExpressionsAlloc;
const tensorDimensionSlicesEqual = contract.tensorDimensionSlicesEqual;
const tensorFragmentSlicesEqual = contract.tensorFragmentSlicesEqual;
const validateAlgebraicPartialsAccessPaths = contract.validateAlgebraicPartialsAccessPaths;
const validateAlgebraicProgramPartialsProof = contract.validateAlgebraicProgramPartialsProof;

pub fn validateGraphHydrateResolvedDocFilterForDb(req: distributed_graph.GraphHydrateRequest, db: *db_mod.DB) !void {
    if (!graphHydrateRequestHasResolvedDocFilter(req)) return;
    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
    if (!ctx.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
    const generation = try db.currentIdentityReadGenerationForRequest(req.identity_read_generation);
    if (generation != ctx.identity_read_generation) return error.IdentityReadGenerationChanged;
}

pub fn executeStorageKernelGraphExpand(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    req: distributed_graph.GraphExpandRequest,
) !distributed_graph.GraphExpandResponse {
    if (req.topology_epoch != 0) return error.InvalidArgument;
    const expansions = try alloc.alloc(distributed_graph.GraphExpansion, req.frontier.len);
    var initialized: usize = 0;
    errdefer {
        for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc);
        alloc.free(expansions);
    }
    for (req.frontier, 0..) |item, i| {
        const frontier_key = try alloc.dupe(u8, item.key);
        errdefer alloc.free(frontier_key);
        const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
        defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
        var result = try db.search(alloc, search_req);
        defer result.deinit();
        var graph_result = if (result.graph_results.len > 0)
            try distributed_graph.filterGraphSearchResult(alloc, table_name, result.graph_results[0], req.exclude_nodes, req.exclude_edges)
        else
            try distributed_graph.emptyGraphSearchResult(alloc, req.name);
        errdefer graph_result.deinit(alloc);
        for (graph_result.hits) |*hit| hit.deinit(alloc);
        if (graph_result.hits.len > 0) alloc.free(graph_result.hits);
        graph_result.hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]);
        try checkQueryDeadline(search_req);
        expansions[i] = .{
            .frontier_id = item.id,
            .frontier_key = frontier_key,
            .graph_result = graph_result,
        };
        initialized += 1;
    }
    return .{ .expansions = expansions };
}

pub fn executeStorageKernelGraphHydrate(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    req: distributed_graph.GraphHydrateRequest,
) !distributed_graph.GraphHydrateResponse {
    if (req.topology_epoch != 0) return error.InvalidArgument;
    try validateGraphHydrateResolvedDocFilterForDb(req, db);
    try validateGraphHydrateIncomingIndexIdentity(req, db);
    const search_req = graphHydrateSearchRequest(req);
    try checkQueryDeadline(search_req);
    const hits = if (req.include_hits)
        try db.graphHydrateKeysForInternalRead(alloc, search_req, req.keys)
    else
        @constCast((&[_]db_mod.types.SearchHit{})[0..]);
    errdefer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }
    try checkQueryDeadline(search_req);
    const has_incoming = if (req.incoming_index_name.len > 0)
        try db.graphHasIncomingEdgesForInternalRead(alloc, req.incoming_index_name, req.keys)
    else
        @constCast((&[_]bool{})[0..]);
    errdefer if (has_incoming.len > 0) alloc.free(has_incoming);
    try checkQueryDeadline(search_req);
    return .{
        .hits = hits,
        .has_incoming = has_incoming,
        .incoming_index_identity = req.incoming_index_identity,
    };
}

pub fn executeStorageKernelGraphEdges(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    req: distributed_graph.GraphEdgesRequest,
) !distributed_graph.GraphEdgesResponse {
    if (req.topology_epoch != 0) return error.InvalidArgument;
    const control_req = db_mod.types.SearchRequest{
        .identity_read_generation = req.identity_read_generation,
        .execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms),
        .cancellation = req.cancellation,
    };
    try checkQueryDeadline(control_req);
    try distributed_graph.validateGraphEdgesTensorAccessPath(alloc, req);
    _ = try currentIdentityReadGenerationForDb(req.identity_read_generation, db);
    const graph_entry = db.core.graphIndex(req.index_name) orelse return error.IndexNotFound;
    const edges = try graph_entry.index.getEdges(alloc, req.key, "", req.direction);
    errdefer {
        for (edges) |edge| graph_mod.GraphIndex.freeEdge(alloc, edge);
        if (edges.len > 0) alloc.free(edges);
    }
    try checkQueryDeadline(control_req);
    return .{ .edges = edges };
}

pub fn validateGraphHydrateIncomingIndexIdentity(
    req: distributed_graph.GraphHydrateRequest,
    db: *db_mod.DB,
) !void {
    if (req.incoming_index_name.len > 0) {
        if (!req.incoming_index_identity.valid()) return error.IndexGenerationMismatch;
        const actual = db.core.index_manager.coverageIdentityForIndex(req.incoming_index_name) orelse
            return error.IndexGenerationMismatch;
        if (actual.generation != req.incoming_index_identity.incarnation or
            actual.config_fingerprint == null or
            actual.config_fingerprint.? != req.incoming_index_identity.config_hash)
        {
            std.log.warn(
                "graph incoming index identity mismatch index={s} expected_incarnation={d} actual_generation={d} expected_config_hash={d} actual_config_hash={?d}",
                .{
                    req.incoming_index_name,
                    req.incoming_index_identity.incarnation,
                    actual.generation,
                    req.incoming_index_identity.config_hash,
                    actual.config_fingerprint,
                },
            );
            return error.IndexGenerationMismatch;
        }
    }
}

pub fn currentIdentityReadGenerationForDb(requested: ?u64, db: *db_mod.DB) !u64 {
    return try db.currentIdentityReadGenerationForRequest(requested);
}

pub fn algebraicIndexFreshEnoughForName(
    alloc: std.mem.Allocator,
    index_name_opt: ?[]const u8,
    db: *db_mod.DB,
) !bool {
    const entry = if (index_name_opt) |index_name|
        db.core.index_manager.algebraicIndex(index_name) orelse return false
    else
        db.core.index_manager.algebraicIndex(null) orelse return false;
    if (entry.index.hasErrors()) return false;
    const target_sequence = db.core.nextDerivedSequence();
    var applied_sequence = try db.core.loadAppliedSequence(alloc, entry.config.name);
    if (db.executor.appliedSequence(entry.config.name)) |live_applied| {
        applied_sequence = @max(applied_sequence, live_applied);
    }
    return applied_sequence >= target_sequence;
}

pub fn collectTextStatsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedTextStatsRequest,
) ![]const distributed_stats_mod.TextFieldStats {
    return switch (request) {
        .query_request => |owned_query| try db.collectSearchRequestTextStats(alloc, owned_query.req),
        .explicit_fields => |parsed| blk: {
            const generation = try db.currentIdentityReadGenerationForRequest(parsed.identity_read_generation);
            if (parsed.resolved_doc_filter) |filter| {
                if (parsed.identity_read_generation == null or generation != filter.context.identity_read_generation) return error.UnsupportedQueryRequest;
                if (!filter.context.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            }
            const explicit = try alloc.alloc(db_query_search.ExplicitTextStatRequest, parsed.items.len);
            defer alloc.free(explicit);
            for (parsed.items, 0..) |item, i| {
                explicit[i] = .{
                    .index_name = item.index_name,
                    .field = item.field,
                    .terms = item.terms,
                    .resolved_doc_filter = if (parsed.resolved_doc_filter) |filter| filter.resolved_doc_filter else null,
                };
            }
            break :blk try db.collectExplicitTextStats(alloc, explicit);
        },
        .background_fields => return error.InvalidQueryRequest,
    };
}

pub fn collectBackgroundTextStatsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedTextStatsRequest,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    return switch (request) {
        .background_fields => |parsed| blk: {
            const generation = try db.currentIdentityReadGenerationForRequest(parsed.identity_read_generation);
            if (parsed.resolved_doc_filter) |filter| {
                if (parsed.identity_read_generation == null or generation != filter.context.identity_read_generation) return error.UnsupportedQueryRequest;
                if (!filter.context.namespace.eql(db.core.identity_namespace)) return error.DocIdentityNamespaceMismatch;
            }
            const explicit = try alloc.alloc(db_query_search.ExplicitBackgroundTextStatRequest, parsed.items.len);
            defer alloc.free(explicit);
            for (parsed.items, 0..) |item, i| {
                explicit[i] = .{
                    .aggregation_name = item.aggregation_name,
                    .index_name = item.index_name,
                    .field = item.field,
                    .terms = item.terms,
                    .background_query = item.background_query,
                    .resolved_doc_filter = if (parsed.resolved_doc_filter) |filter| filter.resolved_doc_filter else null,
                };
            }
            break :blk try db.collectExplicitBackgroundTextStats(alloc, explicit);
        },
        else => return error.InvalidQueryRequest,
    };
}

pub fn collectAlgebraicPartialsFromDbForRequest(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    request: ParsedAlgebraicPartialsRequest,
) ![]db_mod.algebraic.distributed.Partial {
    const generation = try db.currentIdentityReadGenerationForRequest(request.identity_read_generation);
    const entry = if (request.index_name) |index_name|
        db.core.index_manager.algebraicIndex(index_name) orelse return error.UnsupportedQueryRequest
    else
        db.core.index_manager.algebraicIndex(null) orelse return error.UnsupportedQueryRequest;
    if (entry.index.hasErrors() or !entry.index.plannerLifecycleReady()) return error.UnsupportedQueryRequest;
    if (!(try algebraicIndexFreshEnoughForName(alloc, request.index_name, db))) return error.UnsupportedQueryRequest;
    if (request.tensor_program) |*program| {
        const access_path_values = try algebraicTensorAccessPathValuesAlloc(alloc, request.tensor_access_paths);
        defer if (access_path_values.len > 0) alloc.free(access_path_values);
        var view = try program.asProgramAlloc(alloc);
        defer view.deinit(alloc);
        if (try entry.index.scanDistributedPartialsForTensorProgramAtGeneration(db.core.store, access_path_values, view.program, generation)) |partials| {
            return partials;
        }
        const exprs = try algebraicTensorProgramOutputExpressionsForIndexAlloc(alloc, entry.index, request.tensor_access_paths, program);
        defer if (exprs.len > 0) alloc.free(exprs);
        return try entry.index.scanDistributedPartialsForExpressions(db.core.store, exprs);
    }
    try validateAlgebraicPartialsAccessPaths(alloc, request.tensor_access_paths, request.tensor_exprs);
    const exprs = try parsedAlgebraicTensorExpressionsAlloc(alloc, request.tensor_exprs);
    defer if (exprs.len > 0) alloc.free(exprs);
    return try entry.index.scanDistributedPartialsForExpressions(db.core.store, exprs);
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

pub fn algebraicTensorProgramOutputExpressionForStep(
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

pub fn findAlgebraicMaterialization(
    index: *const db_mod.algebraic.index.Index,
    name: []const u8,
) ?db_mod.algebraic.index.MaterializationConfig {
    for (index.config().materializations) |mat| {
        if (std.mem.eql(u8, mat.name, name)) return mat;
    }
    return null;
}

pub fn executeStorageKernelTextStats(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    body: []const u8,
) ![]u8 {
    var parsed = try parseTextStatsRequest(alloc, table_name, body);
    defer parsed.deinit(alloc);
    return switch (parsed) {
        .background_fields => blk: {
            const stats = try collectBackgroundTextStatsFromDbForRequest(alloc, db, parsed);
            defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, stats);
            break :blk try encodeBackgroundTextStatsResponse(alloc, stats);
        },
        else => blk: {
            const stats = try collectTextStatsFromDbForRequest(alloc, db, parsed);
            defer distributed_stats_mod.deinitTextFieldStats(alloc, stats);
            break :blk try encodeTextStatsResponse(alloc, stats);
        },
    };
}

pub fn executeStorageKernelAlgebraicPartials(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    body: []const u8,
) ![]u8 {
    var parsed = try parseAlgebraicPartialsRequest(alloc, body);
    defer parsed.deinit(alloc);
    const partials = try collectAlgebraicPartialsFromDbForRequest(alloc, db, parsed);
    defer db_mod.algebraic.distributed.freePartials(alloc, partials);
    return try encodeAlgebraicPartialsResponse(alloc, partials);
}

pub fn executeStorageKernelPreflight(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    request_json: []const u8,
) ![]u8 {
    var wire = try std.json.parseFromSlice(StorageKernelPreflightWireRequest, alloc, request_json, .{});
    defer wire.deinit();
    var owned = try query_api.parseQueryRequest(alloc, null, table_name, wire.value.query_json);
    defer owned.deinit(alloc);
    var summary = try db.preflightSearchRequest(alloc, owned.req, wire.value.max_work);
    defer summary.deinit(alloc);
    return try std.json.Stringify.valueAlloc(alloc, summary, .{});
}
