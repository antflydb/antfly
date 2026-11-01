// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Contract-only client for the compiled local-query provider.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");

pub const QueryResponse = struct {
    json: []u8,
    identity_read_generation: ?u64,
    native_field_views: u8 = 0,
};

pub fn executeJsonAlloc(
    alloc: std.mem.Allocator,
    db: *anyopaque,
    table_name: []const u8,
    request_json: []const u8,
    dialect: abi.LocalQueryDialect,
    execution_options: abi.LocalQueryExecutionOptions,
    execution_deadline_ns: ?u64,
    cancellation_ctx: ?*anyopaque,
    cancellation_fn: ?abi.CancellationCheckFn,
    out_failure: *abi.FailureIdentity,
) !QueryResponse {
    return executeAlloc(alloc, .{
        .dialect = dialect,
        .kind = .search,
        .db = db,
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .execution_options = execution_options,
        .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
        .execution_deadline_ns = execution_deadline_ns orelse 0,
        .cancellation_ctx = cancellation_ctx,
        .cancellation_fn = cancellation_fn,
    }, .validate_provider_response, out_failure);
}

/// Adopt only an explicitly acknowledged process C-allocator buffer. Older
/// providers and generic allocators retain a copying/destructor adapter.
pub fn executeJsonCAlloc(
    db: *anyopaque,
    table_name: []const u8,
    request_json: []const u8,
    dialect: abi.LocalQueryDialect,
    execution_options: abi.LocalQueryExecutionOptions,
    execution_deadline_ns: ?u64,
    cancellation_ctx: ?*anyopaque,
    cancellation_fn: ?abi.CancellationCheckFn,
    out_failure: *abi.FailureIdentity,
) !QueryResponse {
    return executeImpl(std.heap.c_allocator, .{
        .dialect = dialect,
        .kind = .search,
        .db = db,
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .execution_options = execution_options,
        .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
        .execution_deadline_ns = execution_deadline_ns orelse 0,
        .cancellation_ctx = cancellation_ctx,
        .cancellation_fn = cancellation_fn,
    }, .validate_provider_response, out_failure, true);
}

pub fn executeControlledJsonAlloc(
    alloc: std.mem.Allocator,
    kind: abi.LocalQueryKind,
    db: *anyopaque,
    table_name: []const u8,
    request_json: []const u8,
    execution_deadline_ns: ?u64,
    cancellation_ctx: ?*anyopaque,
    cancellation_fn: ?abi.CancellationCheckFn,
    out_failure: *abi.FailureIdentity,
) ![]u8 {
    std.debug.assert(kind != .search);
    const response = try executeAlloc(alloc, .{
        .kind = kind,
        .db = db,
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
        .execution_deadline_ns = execution_deadline_ns orelse 0,
        .cancellation_ctx = cancellation_ctx,
        .cancellation_fn = cancellation_fn,
    }, validationOperation(kind), out_failure);
    return response.json;
}

fn executeAlloc(
    alloc: std.mem.Allocator,
    request: abi.LocalQueryRequest,
    validation_operation: abi.LocalQueryOperation,
    out_failure: *abi.FailureIdentity,
) !QueryResponse {
    return executeImpl(alloc, request, validation_operation, out_failure, false);
}
fn executeImpl(alloc: std.mem.Allocator, request: abi.LocalQueryRequest, validation_operation: abi.LocalQueryOperation, out_failure: *abi.FailureIdentity, take_c_owned: bool) !QueryResponse {
    out_failure.* = .{};
    var provider_response: abi.QueryOwnedResponse = .{};
    defer abi.antfly_local_query_buffer_destroy(&provider_response.buffer);
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_local_query_execute(&request, &provider_response, &failure);
    try acceptProviderFailure(status, failure, validation_operation, out_failure);
    error_identity.statusToError(status) catch |err| {
        logProviderFailure("local query", failure);
        return err;
    };
    if (provider_response.buffer_c_allocated > 1 or provider_response.native_field_views > 2 or
        (provider_response.native_field_views != 0 and (request.execution_options.enabled == 0 or request.execution_options.native_field_views < provider_response.native_field_views))) return error.InvalidQueryResponse;
    const bytes = if (take_c_owned and provider_response.buffer_c_allocated == 1) adopted: {
        const bytes = @constCast(provider_response.buffer.slice());
        provider_response.buffer = .{};
        break :adopted bytes;
    } else try alloc.dupe(u8, provider_response.buffer.slice());
    return .{
        .json = bytes,
        .native_field_views = provider_response.native_field_views,
        .identity_read_generation = if (provider_response.has_identity_read_generation != 0)
            provider_response.identity_read_generation
        else
            null,
    };
}

fn validationOperation(kind: abi.LocalQueryKind) abi.LocalQueryOperation {
    return switch (kind) {
        .search => .validate_provider_response,
        .graph_expand => .encode_graph_expand,
        .graph_hydrate => .encode_graph_hydrate,
        .graph_edges => .encode_graph_edges,
        .text_stats => .text_stats,
        .algebraic_partials => .algebraic_partials,
        .preflight => .preflight,
    };
}

pub fn acceptProviderFailure(
    status: abi.Status,
    failure: abi.FailureIdentity,
    validation_operation: abi.LocalQueryOperation,
    out_failure: *abi.FailureIdentity,
) !void {
    error_identity.validateFailureEnvelope(status, &failure, abi.abi_version) catch |err| {
        logMalformedProviderFailure(status, failure);
        out_failure.* = error_identity.failureFromError(
            err,
            .storage_owner,
            abi.abi_version,
            @backingInt(validation_operation),
        );
        return err;
    };
    if (status != .ok and failure.boundary != .local_query) {
        std.log.err("local query returned failure from unexpected boundary={s}", .{@tagName(failure.boundary)});
        out_failure.* = error_identity.failureFromError(
            error.InvalidBoundaryFailureIdentity,
            .storage_owner,
            abi.abi_version,
            @backingInt(validation_operation),
        );
        return error.InvalidBoundaryFailureIdentity;
    }
    out_failure.* = failure;
}

fn logMalformedProviderFailure(status: abi.Status, failure: abi.FailureIdentity) void {
    std.log.err(
        "local query returned inconsistent failure identity status={s} identity_status={s} identity_version={d} expected_version={d} identity_operation={d} identity_error={s} identity_hash={x}",
        .{
            @tagName(status),
            @tagName(failure.status),
            failure.boundary_version,
            abi.abi_version,
            failure.operation,
            failure.boundedErrorName(),
            failure.error_name_hash,
        },
    );
}

fn logProviderFailure(label: []const u8, failure: abi.FailureIdentity) void {
    if (failure.status != .internal or failure.error_name_len == 0) return;
    std.log.err("{s} failed operation={d} provider_error={s} hash={x}", .{
        label,
        failure.operation,
        failure.errorName(),
        failure.error_name_hash,
    });
}
