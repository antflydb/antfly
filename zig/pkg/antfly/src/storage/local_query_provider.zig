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

//! Physical local-query provider. The storage archive owns and closes the DB;
//! this component borrows that opaque handle for one complete query and returns
//! one encoded response. Index internals never cross the ABI. The provider is
//! co-generated with the owning storage kernel in release builds so physical
//! storage and local query share one optimized compilation graph.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const db_mod = @import("antfly_source_root").antfly_sources.selected_db;
const query_api = @import("../api/query.zig");
const local_query = @import("antfly_source_root").antfly_sources.local_query;
const distributed_graph = @import("../api/distributed_graph.zig");
const aggregation_plan = @import("../api/aggregation_plan.zig");
const local_query_contract = @import("../api/local_query_contract.zig");

pub fn execute(
    request: *const abi.LocalQueryRequest,
    out_response: *abi.QueryOwnedResponse,
    out_failure: *abi.FailureIdentity,
) callconv(.c) abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != abi.abi_version)
        return fail(error.InvalidAbiVersion, .validate_request, out_failure);
    const db: *db_mod.DB = @ptrCast(@alignCast(request.db orelse
        return fail(error.InvalidArgument, .validate_request, out_failure)));
    const table_name = request.table_name.slice();
    if (table_name.len == 0 or request.request_json.len == 0)
        return fail(error.InvalidArgument, .validate_request, out_failure);

    return switch (request.kind) {
        .search => executeSearch(request, db, table_name, out_response, out_failure),
        .graph_expand => executeGraphExpand(request, db, table_name, &out_response.buffer, out_failure),
        .graph_hydrate => executeGraphHydrate(request, db, &out_response.buffer, out_failure),
        .graph_edges => executeGraphEdges(request, db, &out_response.buffer, out_failure),
        .text_stats => executeTextStats(request, db, table_name, &out_response.buffer, out_failure),
        .algebraic_partials => executeAlgebraicPartials(request, db, &out_response.buffer, out_failure),
        .preflight => executePreflight(request, db, table_name, &out_response.buffer, out_failure),
    };
}

fn executeSearch(
    request: *const abi.LocalQueryRequest,
    db: *db_mod.DB,
    table_name: []const u8,
    out_response: *abi.QueryOwnedResponse,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    const alloc = std.heap.c_allocator;
    var owned = switch (request.dialect) {
        .internal => query_api.parseQueryRequest(
            alloc,
            null,
            table_name,
            request.request_json.slice(),
        ),
        .public => query_api.parsePublicQueryRequest(
            alloc,
            null,
            table_name,
            request.request_json.slice(),
        ),
    } catch |err| return fail(err, parseOperation(request.dialect), out_failure);
    defer owned.deinit(alloc);

    @import("local_query_controls.zig").applyExecutionOptions(&owned.req, request.execution_options);
    if (request.has_execution_deadline != 0) {
        owned.req.execution_deadline_ns = if (owned.req.execution_deadline_ns) |parsed_deadline|
            @min(parsed_deadline, request.execution_deadline_ns)
        else
            request.execution_deadline_ns;
    }
    owned.req.cancellation = requestCancellationToken(request);
    @import("../api/local_query_contract.zig").checkQueryDeadline(owned.req) catch |err|
        return fail(err, executeOperation(request.dialect), out_failure);

    // The owner has already established read safety. Keep aggregation's page
    // selection and complete collection inside one physical read generation;
    // an ABI round trip between them cannot retain that ownership.
    const finalize_aggregations = owned.req.aggregations_json.len != 0 and
        !(request.execution_options.enabled != 0 and request.execution_options.raw_search_result != 0);
    var lease: ?db_mod.DB.QueryReadLease = if (finalize_aggregations)
        db.beginQueryReadLease() catch |err| return fail(err, executeOperation(request.dialect), out_failure)
    else
        null;
    defer if (lease) |*held| held.release();
    const captured: db_mod.SearchWithDenseProfileResult = if (lease) |held|
        held.search(alloc, owned.req) catch |err| return fail(err, executeOperation(request.dialect), out_failure)
    else if (owned.req.profile)
        db.searchWithDenseProfile(alloc, owned.req) catch |err|
            return fail(err, executeOperation(request.dialect), out_failure)
    else blk: {
        const unprofiled = db.searchWithCapturedRequest(alloc, owned.req) catch |err|
            return fail(err, executeOperation(request.dialect), out_failure);
        break :blk .{ .request = unprofiled.request, .result = unprofiled.result };
    };
    var result = captured.result;
    defer result.deinit();
    var meta: query_api.QueryResponseMeta = .{
        .dense_search = if (captured.dense_profile) |profile|
            @import("../api/dense_search_profile.zig").fromStorage(profile)
        else
            null,
    };
    defer meta.deinit(alloc);
    if (lease) |*held| {
        applyAggregations(alloc, db, held, captured.request, result, &meta) catch |err|
            return fail(err, executeOperation(request.dialect), out_failure);
        held.release();
        lease = null;
    }
    @import("../api/local_query_contract.zig").checkQueryDeadline(owned.req) catch |err|
        return fail(err, executeOperation(request.dialect), out_failure);

    var response = query_api.encodeQueryResponses(
        alloc,
        table_name,
        captured.request,
        meta,
        result,
    ) catch |err| return fail(err, encodeOperation(request.dialect), out_failure);
    @import("../api/local_query_contract.zig").checkQueryDeadline(owned.req) catch |err| {
        response.deinit(alloc);
        return fail(err, executeOperation(request.dialect), out_failure);
    };
    const bytes = response.json;
    out_response.* = .{
        .buffer = .{ .ptr = bytes.ptr, .len = @intCast(bytes.len) },
        .identity_read_generation = response.identity_read_generation orelse 0,
        .has_identity_read_generation = @intFromBool(response.identity_read_generation != null),
    };
    return .ok;
}

/// Aggregation sub-queries share the owner's read lease and generation.
const LeaseSearcher = struct {
    lease: *db_mod.DB.QueryReadLease,

    pub fn search(self: LeaseSearcher, alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) !db_mod.types.SearchResult {
        return (try self.lease.search(alloc, req)).result;
    }
};

fn applyAggregations(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    lease: *db_mod.DB.QueryReadLease,
    req: db_mod.types.SearchRequest,
    result: db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
) !void {
    var full: ?db_mod.types.SearchResult = null;
    defer if (full) |*value| value.deinit();
    var aggregation_req = req;
    const selected = if (aggregation_plan.aggregationCanUseCurrentResult(req, result)) result else blk: {
        aggregation_req = try aggregation_plan.aggregationFullResultRequest(req, result, "local-owner");
        full = try aggregation_plan.collectAggregationFullResult(alloc, req, aggregation_req, LeaseSearcher{ .lease = lease }, "local-owner");
        break :blk full.?;
    };
    const requests = try query_api.parseAggregationRequestsJson(alloc, aggregation_req.aggregations_json);
    defer query_api.freeAggregationRequests(alloc, requests);
    var ctx: db_mod.aggregations.Context = .{
        .index_manager = db.core.index_manager,
        .doc_store = db.core.store,
        .full_text_index_name = if (aggregation_req.full_text_queries.len == 1) aggregation_req.full_text_queries[0].index_name else aggregation_req.index_name,
        .algebraic_index_name = aggregation_req.index_name,
        .algebraic_available = try local_query.algebraicIndexFreshEnoughForName(alloc, aggregation_req.index_name, db),
        .identity_read_generation = aggregation_req.identity_read_generation,
    };
    const constraints = if (aggregation_plan.canConsiderAlgebraicAggregations(aggregation_req))
        try local_query_contract.algebraicConstraintsForRequestAlloc(alloc, aggregation_req)
    else
        null;
    defer if (constraints) |items| local_query_contract.freeAlgebraicConstraints(alloc, items);
    if (constraints) |items| if (ctx.algebraic_available) {
        ctx.algebraic_scope = .root;
        ctx.algebraic_constraints = items;
    };
    const results = try db_mod.aggregations.computeSearchAggregations(alloc, requests, selected, ctx);
    errdefer db_mod.aggregations.deinitResults(alloc, results);
    for (results) |*aggregation| try db_mod.aggregations.cloneSearchAggregationResultLabelsDeep(alloc, aggregation);
    meta.aggregation_results = results;
}

fn executeGraphExpand(
    request: *const abi.LocalQueryRequest,
    db: *db_mod.DB,
    table_name: []const u8,
    out_response: *abi.OwnedBytes,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    const alloc = std.heap.c_allocator;
    var parsed = distributed_graph.parseGraphExpandRequest(alloc, request.request_json.slice()) catch |err|
        return fail(err, .parse_graph_expand, out_failure);
    defer parsed.deinit(alloc);
    applyControls(request, &parsed);
    var result = local_query.executeStorageKernelGraphExpand(alloc, db, table_name, parsed) catch |err|
        return fail(err, .execute_graph_expand, out_failure);
    defer result.deinit(alloc);
    const response = distributed_graph.encodeGraphExpandResponse(alloc, result) catch |err|
        return fail(err, .encode_graph_expand, out_failure);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

fn executeGraphHydrate(
    request: *const abi.LocalQueryRequest,
    db: *db_mod.DB,
    out_response: *abi.OwnedBytes,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    const alloc = std.heap.c_allocator;
    var parsed = distributed_graph.parseGraphHydrateRequest(alloc, request.request_json.slice()) catch |err|
        return fail(err, .parse_graph_hydrate, out_failure);
    defer parsed.deinit(alloc);
    applyControls(request, &parsed);
    var result = local_query.executeStorageKernelGraphHydrate(alloc, db, parsed) catch |err|
        return fail(err, .execute_graph_hydrate, out_failure);
    defer result.deinit(alloc);
    const response = distributed_graph.encodeGraphHydrateResponse(alloc, result) catch |err|
        return fail(err, .encode_graph_hydrate, out_failure);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

fn executeGraphEdges(
    request: *const abi.LocalQueryRequest,
    db: *db_mod.DB,
    out_response: *abi.OwnedBytes,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    const alloc = std.heap.c_allocator;
    var parsed = distributed_graph.parseGraphEdgesRequest(alloc, request.request_json.slice()) catch |err|
        return fail(err, .parse_graph_edges, out_failure);
    defer parsed.deinit(alloc);
    applyControls(request, &parsed);
    var result = local_query.executeStorageKernelGraphEdges(alloc, db, parsed) catch |err|
        return fail(err, .execute_graph_edges, out_failure);
    defer result.deinit(alloc);
    const response = distributed_graph.encodeGraphEdgesResponse(alloc, result) catch |err|
        return fail(err, .encode_graph_edges, out_failure);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

fn executeTextStats(
    request: *const abi.LocalQueryRequest,
    db: *db_mod.DB,
    table_name: []const u8,
    out_response: *abi.OwnedBytes,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    const response = local_query.executeStorageKernelTextStats(
        std.heap.c_allocator,
        db,
        table_name,
        request.request_json.slice(),
    ) catch |err| return fail(err, .text_stats, out_failure);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

fn executeAlgebraicPartials(
    request: *const abi.LocalQueryRequest,
    db: *db_mod.DB,
    out_response: *abi.OwnedBytes,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    const response = local_query.executeStorageKernelAlgebraicPartials(
        std.heap.c_allocator,
        db,
        request.request_json.slice(),
    ) catch |err| return fail(err, .algebraic_partials, out_failure);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

fn executePreflight(
    request: *const abi.LocalQueryRequest,
    db: *db_mod.DB,
    table_name: []const u8,
    out_response: *abi.OwnedBytes,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    const response = local_query.executeStorageKernelPreflight(
        std.heap.c_allocator,
        db,
        table_name,
        request.request_json.slice(),
    ) catch |err| return fail(err, .preflight, out_failure);
    out_response.* = .{ .ptr = response.ptr, .len = @intCast(response.len) };
    return .ok;
}

fn applyControls(request: *const abi.LocalQueryRequest, graph_request: anytype) void {
    graph_request.execution_deadline_ns = if (request.has_execution_deadline != 0)
        request.execution_deadline_ns
    else
        null;
    graph_request.timeout_ms = null;
    graph_request.cancellation = requestCancellationToken(request);
}

fn requestCancellationToken(request: *const abi.LocalQueryRequest) db_mod.types.CancellationToken {
    if (request.cancellation_fn == null) return .none;
    return .{
        .ptr = request,
        .is_cancelled_fn = struct {
            fn requested(ptr: *const anyopaque) bool {
                const local_request: *const abi.LocalQueryRequest = @ptrCast(@alignCast(ptr));
                const callback = local_request.cancellation_fn orelse return false;
                return callback(local_request.cancellation_ctx) != 0;
            }
        }.requested,
    };
}

fn fail(
    err: anyerror,
    operation: abi.LocalQueryOperation,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    out_failure.* = error_identity.failureFromError(
        err,
        .local_query,
        abi.abi_version,
        @intFromEnum(operation),
    );
    return out_failure.status;
}

fn parseOperation(dialect: abi.LocalQueryDialect) abi.LocalQueryOperation {
    return switch (dialect) {
        .internal => .parse_internal_request,
        .public => .parse_public_request,
    };
}

fn executeOperation(dialect: abi.LocalQueryDialect) abi.LocalQueryOperation {
    return switch (dialect) {
        .internal => .execute_internal_query,
        .public => .execute_public_query,
    };
}

fn encodeOperation(dialect: abi.LocalQueryDialect) abi.LocalQueryOperation {
    return switch (dialect) {
        .internal => .encode_internal_response,
        .public => .encode_public_response,
    };
}

pub fn bufferDestroy(buffer: *abi.OwnedBytes) callconv(.c) void {
    if (buffer.ptr) |ptr| std.heap.c_allocator.free(ptr[0..@intCast(buffer.len)]);
    buffer.* = .{};
}
