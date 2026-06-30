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
const http_common = @import("../common/http/http_common.zig");
const http_server = @import("http_server.zig");
const pgwire_backend = @import("../pgwire/sql_backend.zig");
const sql_adapter = @import("../sql/mod.zig");
const table_reads = @import("table_reads.zig");

pub fn backendFromApiServer(api_server: *http_server.ApiHttpServer) pgwire_backend.Backend {
    return .{
        .ptr = api_server,
        .vtable = &api_backend_vtable,
    };
}

const api_backend_vtable = pgwire_backend.Backend.VTable{
    .allocator = allocator,
    .authentication_required = authenticationRequired,
    .can_authenticate_password = canAuthenticatePassword,
    .authenticate_user_password = authenticateUserPassword,
    .describe_parsed_sql = describeParsedSql,
    .execute_parsed_sql = executeParsedSql,
    .free_param = freeParam,
};

fn apiServer(ptr: *anyopaque) *http_server.ApiHttpServer {
    return @ptrCast(@alignCast(ptr));
}

fn allocator(ptr: *anyopaque) std.mem.Allocator {
    return apiServer(ptr).alloc;
}

fn authenticationRequired(ptr: *anyopaque) bool {
    const server = apiServer(ptr);
    return server.cfg.auth_enabled or server.cfg.trusted_principal_secret != null;
}

fn canAuthenticatePassword(ptr: *anyopaque) bool {
    return apiServer(ptr).cfg.user_manager != null;
}

fn authenticateUserPassword(ptr: *anyopaque, username: []const u8, password: []const u8) !pgwire_backend.AuthenticatedIdentity {
    const server = apiServer(ptr);
    const owned_identity = try server.alloc.create(http_server.AuthenticatedIdentity);
    errdefer server.alloc.destroy(owned_identity);
    owned_identity.* = try server.authenticateUserPassword(username, password);
    return .{
        .ptr = owned_identity,
        .deinit_fn = deinitAuthenticatedIdentity,
    };
}

fn deinitAuthenticatedIdentity(ptr: *anyopaque, alloc: std.mem.Allocator) void {
    const identity: *http_server.AuthenticatedIdentity = @ptrCast(@alignCast(ptr));
    identity.deinit(alloc);
    alloc.destroy(identity);
}

fn apiIdentity(identity: ?pgwire_backend.AuthenticatedIdentity) ?http_server.AuthenticatedIdentity {
    const value = identity orelse return null;
    const typed: *http_server.AuthenticatedIdentity = @ptrCast(@alignCast(value.ptr));
    return typed.*;
}

fn describeParsedSql(
    ptr: *anyopaque,
    request: pgwire_backend.Request,
    authenticated_identity: ?pgwire_backend.AuthenticatedIdentity,
) !pgwire_backend.PublicSqlDescribeResultOrResponse {
    const server = apiServer(ptr);
    var outcome = try server.handlePublicParsedSqlExternalDescribeRequestResult(.{
        .parsed_sql = request.parsed_sql,
        .session_id = request.session_id,
        .database = request.database,
        .namespace = request.namespace,
        .read_only = request.read_only,
        .params = request.params,
    }, apiIdentity(authenticated_identity));
    return try takeDescribeOutcome(server.alloc, &outcome);
}

fn executeParsedSql(
    ptr: *anyopaque,
    request: pgwire_backend.Request,
    authenticated_identity: ?pgwire_backend.AuthenticatedIdentity,
) !pgwire_backend.PublicSqlResultOrResponse {
    const server = apiServer(ptr);
    var outcome = try server.executePublicParsedSqlExternalRequestResult(.{
        .parsed_sql = request.parsed_sql,
        .session_id = request.session_id,
        .database = request.database,
        .namespace = request.namespace,
        .read_only = request.read_only,
        .params = request.params,
    }, apiIdentity(authenticated_identity));
    return try takeExecutionOutcome(server.alloc, &outcome);
}

fn freeParam(_: *anyopaque, alloc: std.mem.Allocator, value: sql_adapter.SqlValue) void {
    pgwire_backend.freeParam(alloc, value);
}

fn takeDescribeOutcome(
    alloc: std.mem.Allocator,
    outcome: *http_server.ApiHttpServer.PublicSqlDescribeResultOrResponse,
) !pgwire_backend.PublicSqlDescribeResultOrResponse {
    switch (outcome.*) {
        .response => |*response| {
            const out = try errorResponseFromHttpResponseAlloc(alloc, response);
            response.deinit(alloc);
            outcome.* = undefined;
            return .{ .response = out };
        },
        .result => |*result| {
            const session_id = result.session_id;
            const statement_kind = result.statement_kind;
            const transaction_status_value = transactionStatus(result.transaction_status);
            const has_row_description = result.has_row_description;
            const columns = try pgwire_backend.cloneColumnsAlloc(alloc, result.columns);
            result.deinit(alloc);
            outcome.* = undefined;
            return .{ .result = .{
                .session_id = session_id,
                .statement_kind = statement_kind,
                .transaction_status = transaction_status_value,
                .has_row_description = has_row_description,
                .columns = columns,
            } };
        },
    }
}

fn takeExecutionOutcome(
    alloc: std.mem.Allocator,
    outcome: *http_server.ApiHttpServer.PublicSqlResultOrResponse,
) !pgwire_backend.PublicSqlResultOrResponse {
    switch (outcome.*) {
        .response => |*response| {
            const out = try errorResponseFromHttpResponseAlloc(alloc, response);
            response.deinit(alloc);
            outcome.* = undefined;
            return .{ .response = out };
        },
        .result => |*result| {
            return .{ .result = try takeExecutionResult(alloc, result) };
        },
    }
}

fn takeExecutionResult(
    alloc: std.mem.Allocator,
    result: *http_server.ApiHttpServer.PublicSqlResult,
) !pgwire_backend.PublicSqlResult {
    const session_id = result.session_id;
    const statement_kind = result.statement_kind;
    const txn_status = transactionStatus(result.transaction_status);
    switch (result.result) {
        .ddl => |*ddl| {
            const applied = ddl.applied;
            ddl.applied = undefined;
            result.* = undefined;
            return .{
                .session_id = session_id,
                .statement_kind = statement_kind,
                .transaction_status = txn_status,
                .result = .{ .ddl = .{
                    .applied = applied,
                    .command_tag = ddl.command_tag,
                } },
            };
        },
        .read => |*read| {
            const rows = try cloneReadRowsAlloc(alloc, read.result);
            errdefer pgwire_backend.freeRows(alloc, rows);
            const columns = try pgwire_backend.cloneColumnsAlloc(alloc, read.columns);
            errdefer pgwire_backend.freeColumns(alloc, columns);
            read.deinit(alloc);
            result.* = undefined;
            return .{
                .session_id = session_id,
                .statement_kind = statement_kind,
                .transaction_status = txn_status,
                .result = .{ .read = .{
                    .rows = rows,
                    .columns = columns,
                } },
            };
        },
        .rows_batch => |*rows_batch| {
            const returning_rows = try pgwire_backend.cloneRowsAlloc(alloc, rows_batch.result.returning_rows);
            errdefer pgwire_backend.freeRows(alloc, returning_rows);
            const columns = try pgwire_backend.cloneColumnsAlloc(alloc, rows_batch.columns);
            errdefer pgwire_backend.freeColumns(alloc, columns);
            const counts = pgwire_backend.PublicSqlResult.RowsBatchResult{
                .inserted = rows_batch.result.inserted,
                .deleted = rows_batch.result.deleted,
                .transformed = rows_batch.result.transformed,
                .returning_rows = returning_rows,
            };
            rows_batch.deinit(alloc);
            result.* = undefined;
            return .{
                .session_id = session_id,
                .statement_kind = statement_kind,
                .transaction_status = txn_status,
                .result = .{ .rows_batch = .{
                    .result = counts,
                    .columns = columns,
                } },
            };
        },
        .mutation_source => |*mutation_source| {
            const returning_rows = try pgwire_backend.cloneRowsAlloc(alloc, mutation_source.result.returning_rows);
            errdefer pgwire_backend.freeRows(alloc, returning_rows);
            const columns = try pgwire_backend.cloneColumnsAlloc(alloc, mutation_source.columns);
            errdefer pgwire_backend.freeColumns(alloc, columns);
            const mutation_result = pgwire_backend.PublicSqlResult.MutationSourceResult{
                .staged = mutation_source.result.staged,
                .returning_rows = returning_rows,
            };
            mutation_source.deinit(alloc);
            result.* = undefined;
            return .{
                .session_id = session_id,
                .statement_kind = statement_kind,
                .transaction_status = txn_status,
                .result = .{ .mutation_source = .{
                    .result = mutation_result,
                    .columns = columns,
                } },
            };
        },
        .bulk_io => |*bulk_io| {
            const out = pgwire_backend.PublicSqlResult.BulkIo{
                .operation = bulk_io.operation,
                .stream = bulk_io.stream,
                .row_count = bulk_io.row_count,
            };
            bulk_io.deinit(alloc);
            result.* = undefined;
            return .{
                .session_id = session_id,
                .statement_kind = statement_kind,
                .transaction_status = txn_status,
                .result = .{ .bulk_io = out },
            };
        },
    }
}

fn cloneReadRowsAlloc(
    alloc: std.mem.Allocator,
    result: table_reads.LoweredSqlReadPlanResult,
) ![]const []const u8 {
    return try pgwire_backend.cloneRowsAlloc(alloc, switch (result) {
        .query => |query| query.rows,
        .document_query => |query| query.rows,
        .set_operation => |query| query.rows,
        .recursive_cte => |query| query.rows,
        .aggregate => |aggregate| aggregate.rows,
        .window => |window| window.rows,
        .join => |join| join.rows,
        .lateral => |lateral| lateral.rows,
    });
}

fn errorResponseFromHttpResponseAlloc(
    alloc: std.mem.Allocator,
    response: *const http_common.HttpResponse,
) !pgwire_backend.PublicSqlErrorResponse {
    return .{
        .status = response.status,
        .body = try alloc.dupe(u8, response.body),
    };
}

fn transactionStatus(status: http_server.ApiHttpServer.PublicSqlTransactionStatus) pgwire_backend.PublicSqlTransactionStatus {
    return switch (status) {
        .idle => .idle,
        .in_transaction => .in_transaction,
        .failed_transaction => .failed_transaction,
    };
}
