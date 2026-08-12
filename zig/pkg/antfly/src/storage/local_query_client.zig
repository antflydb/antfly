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

//! Contract-only client for the compiled local-query provider.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");

pub fn executeJsonAlloc(
    alloc: std.mem.Allocator,
    db: *anyopaque,
    table_name: []const u8,
    request_json: []const u8,
    dialect: abi.LocalQueryDialect,
    execution_options: abi.LocalQueryExecutionOptions,
    cancellation_flag: ?*const anyopaque,
    out_failure: *abi.FailureIdentity,
) ![]u8 {
    return executeAlloc(alloc, .{
        .dialect = dialect,
        .kind = .search,
        .db = db,
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .execution_options = execution_options,
        .cancellation_flag = cancellation_flag,
    }, .validate_provider_response, out_failure);
}

pub fn executeControlledJsonAlloc(
    alloc: std.mem.Allocator,
    kind: abi.LocalQueryKind,
    db: *anyopaque,
    table_name: []const u8,
    request_json: []const u8,
    execution_deadline_ns: ?u64,
    cancellation_flag: ?*const anyopaque,
    out_failure: *abi.FailureIdentity,
) ![]u8 {
    std.debug.assert(kind != .search);
    return executeAlloc(alloc, .{
        .kind = kind,
        .db = db,
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
        .execution_deadline_ns = execution_deadline_ns orelse 0,
        .cancellation_flag = cancellation_flag,
    }, validationOperation(kind), out_failure);
}

fn executeAlloc(
    alloc: std.mem.Allocator,
    request: abi.LocalQueryRequest,
    validation_operation: abi.LocalQueryOperation,
    out_failure: *abi.FailureIdentity,
) ![]u8 {
    out_failure.* = .{};
    var provider_response: abi.OwnedBytes = .{};
    defer abi.antfly_local_query_buffer_destroy(&provider_response);
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_local_query_execute(&request, &provider_response, &failure);
    try acceptProviderFailure(status, failure, validation_operation, out_failure);
    error_identity.statusToError(status) catch |err| {
        logProviderFailure("local query", failure);
        return err;
    };
    return try alloc.dupe(u8, provider_response.slice());
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
            @intFromEnum(validation_operation),
        );
        return err;
    };
    if (status != .ok and failure.boundary != .local_query) {
        std.log.err("local query returned failure from unexpected boundary={s}", .{@tagName(failure.boundary)});
        out_failure.* = error_identity.failureFromError(
            error.InvalidBoundaryFailureIdentity,
            .storage_owner,
            abi.abi_version,
            @intFromEnum(validation_operation),
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
