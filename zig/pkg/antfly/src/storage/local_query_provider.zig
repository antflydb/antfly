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
const db_mod = @import("db/mod.zig");
const query_api = @import("../api/query.zig");
const table_reads_api = @import("../api/table_reads.zig");
const distributed_graph = @import("../api/distributed_graph.zig");

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

    if (request.execution_options.enabled != 0) {
        owned.req.include_stored = request.execution_options.include_stored != 0;
        owned.req.return_mode = switch (request.execution_options.return_mode) {
            .parent => .parent,
            .chunk => .chunk,
            .parent_with_chunks => .parent_with_chunks,
            .unit => .unit,
            .unit_with_chunks => .unit_with_chunks,
        };
        owned.req.max_chunks_per_parent = request.execution_options.max_chunks_per_parent;
        if (request.execution_options.dense_k != 0) {
            for (@constCast(owned.req.dense_queries)) |*query| query.query.k = request.execution_options.dense_k;
        }
        if (request.execution_options.sparse_k != 0) {
            for (@constCast(owned.req.sparse_queries)) |*query| query.query.k = request.execution_options.sparse_k;
        }
    }

    owned.req.cancellation = requestCancellationToken(request);

    // Preserve the generation actually used by DB.search in the encoded wire
    // response. This validation belongs to the provider because the DB and its
    // identity namespace do not cross the ABI as typed implementation state.
    owned.req = db.searchRequestAtCurrentIdentityGeneration(owned.req) catch |err|
        return fail(err, executeOperation(request.dialect), out_failure);

    var result = db.search(alloc, owned.req) catch |err|
        return fail(err, executeOperation(request.dialect), out_failure);
    defer result.deinit();

    var response = query_api.encodeQueryResponses(
        alloc,
        table_name,
        owned.req,
        .{},
        result,
    ) catch |err| return fail(err, encodeOperation(request.dialect), out_failure);
    defer response.deinit(alloc);

    const bytes = alloc.dupe(u8, response.json) catch |err|
        return fail(err, encodeOperation(request.dialect), out_failure);
    out_response.* = .{
        .buffer = .{ .ptr = bytes.ptr, .len = @intCast(bytes.len) },
        .identity_read_generation = response.identity_read_generation orelse 0,
        .has_identity_read_generation = @intFromBool(response.identity_read_generation != null),
    };
    return .ok;
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
    var result = table_reads_api.executeStorageKernelGraphExpand(alloc, db, table_name, parsed) catch |err|
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
    var result = table_reads_api.executeStorageKernelGraphHydrate(alloc, db, parsed) catch |err|
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
    var result = table_reads_api.executeStorageKernelGraphEdges(alloc, db, parsed) catch |err|
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
    const response = table_reads_api.executeStorageKernelTextStats(
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
    const response = table_reads_api.executeStorageKernelAlgebraicPartials(
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
    const response = table_reads_api.executeStorageKernelPreflight(
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
