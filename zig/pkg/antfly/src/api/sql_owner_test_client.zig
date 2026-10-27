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

//! Shared mounted-owner SQL transport. Never retry an admitted mutation.
const std = @import("std");
const platform = @import("antfly_platform");
const fixture = @import("sql_owner_test_fixture.zig");
const http = @import("../raft/transport/http_common.zig");

pub const Retry = enum {
    never,
    read,
    write_admission,
    /// Only pure fixture observations, never arbitrary SELECT expressions or
    /// mutations. An active session's read may contend with maintenance.
    session_observation,
};
/// Real scoped credentials for long Debug HTTP campaigns. Keep password
/// hashing unchanged and reserve Basic verification for authentication tests.
pub const ApiKeyHeaders = struct {
    authorization: [2][192]u8 = undefined,
    admin: [1]http.RequestHeader = undefined,
    reader: [1]http.RequestHeader = undefined,

    pub fn activate(self: *@This(), ctx: *fixture.Context) !void {
        return self.activateWithCatalog(ctx, false);
    }

    pub fn activateWithCatalog(self: *@This(), ctx: *fixture.Context, catalog_admin: bool) !void {
        const users = @import("../usermgr/mod.zig");
        const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const manager = api.cfg.user_manager orelse return error.MissingUserManager;
        for ([_][]const u8{ "pk-rewriter", "pk-reader" }, 0..) |username, i| {
            var permission = try users.Permission.initOwned(ctx.alloc, .table, "*", if (i == 0) .admin else .read);
            defer permission.deinit(ctx.alloc);
            var database = try users.Permission.initOwned(ctx.alloc, .database, "*", .admin);
            defer database.deinit(ctx.alloc);
            var namespace = try users.Permission.initOwned(ctx.alloc, .namespace, "*", .admin);
            defer namespace.deinit(ctx.alloc);
            const scopes = [_]users.Permission{ permission, database, namespace };
            var key = try manager.createApiKey(username, "sql-owner-campaign", if (catalog_admin and i == 0) &scopes else &.{permission}, &.{}, null);
            defer key.deinit(manager.alloc);
            const authorization = try std.fmt.bufPrint(&self.authorization[i], "ApiKey {s}", .{key.encoded});
            const header: http.RequestHeader = .{ .name = "authorization", .value = authorization };
            if (i == 0) self.admin[0] = header else self.reader[0] = header;
        }
        ctx.admin_headers = &self.admin;
        ctx.reader_headers = &self.reader;
    }
};
const Diagnostic = struct {
    code: []const u8,
    message: []const u8,
    retryable: bool = false,
    transaction_status: []const u8 = "",
    transaction_id: ?[]const u8 = null,
    mutation_outcome: ?[]const u8 = null,
    admission_outcome: ?[]const u8 = null,
    status: ?[]const u8 = null,
    ddl_receipt: ?std.json.Value = null,
    catalog_receipt: ?std.json.Value = null,
};

fn definiteReadAdmission(diagnostic: Diagnostic) bool {
    return diagnostic.retryable and std.mem.eql(u8, diagnostic.code, "53300") and
        std.mem.startsWith(u8, diagnostic.message, "A consistent SQL statement read is temporarily unavailable.") and
        std.mem.eql(u8, diagnostic.transaction_status, "idle") and diagnostic.transaction_id == null and
        diagnostic.mutation_outcome == null and diagnostic.admission_outcome == null and diagnostic.status == null and diagnostic.ddl_receipt == null and diagnostic.catalog_receipt == null;
}

fn definiteDdlNonadmission(diagnostic: Diagnostic) bool {
    return diagnostic.retryable and std.mem.eql(u8, diagnostic.code, "53300") and
        std.mem.startsWith(u8, diagnostic.message, "The relation catalog publication is not ready to authorize this DDL. No schema change was admitted.") and
        std.mem.eql(u8, diagnostic.transaction_status, "idle") and diagnostic.transaction_id == null and
        diagnostic.mutation_outcome == null and diagnostic.admission_outcome == null and diagnostic.status == null and diagnostic.ddl_receipt == null and diagnostic.catalog_receipt == null;
}

fn sessionObservationCapacity(diagnostic: Diagnostic) bool {
    return diagnostic.retryable and std.mem.eql(u8, diagnostic.code, "53300") and
        std.mem.startsWith(u8, diagnostic.message, "SQL execution capacity is temporarily exhausted.") and
        std.mem.eql(u8, diagnostic.transaction_status, "in_transaction") and diagnostic.transaction_id == null and
        diagnostic.mutation_outcome == null and diagnostic.admission_outcome == null and diagnostic.status == null and diagnostic.ddl_receipt == null and diagnostic.catalog_receipt == null;
}

test "SQL session observations retry only pure active reads without mutation outcomes" {
    const safe: Diagnostic = .{ .code = "53300", .message = "SQL execution capacity is temporarily exhausted.", .retryable = true, .transaction_status = "in_transaction" };
    try std.testing.expect(sessionObservationCapacity(safe));
    try std.testing.expect(!definiteReadAdmission(safe));
    var changed = safe;
    changed.catalog_receipt = .null;
    try std.testing.expect(!sessionObservationCapacity(changed));
    changed = safe;
    changed.transaction_status = "failed";
    try std.testing.expect(!sessionObservationCapacity(changed));
    changed = safe;
    changed.transaction_id = "reconciliation";
    try std.testing.expect(!sessionObservationCapacity(changed));
    changed = safe;
    changed.mutation_outcome = "committed_pending";
    try std.testing.expect(!sessionObservationCapacity(changed));
    changed = safe;
    changed.admission_outcome = "unknown";
    try std.testing.expect(!sessionObservationCapacity(changed));
    changed = safe;
    changed.message = "The requested write is not yet visible within the readiness budget.";
    try std.testing.expect(!sessionObservationCapacity(changed));
    changed = safe;
    changed.ddl_receipt = .null;
    try std.testing.expect(!sessionObservationCapacity(changed));
}

test "SQL catalog lifecycle retries only explicit definite DDL nonadmission" {
    const safe: Diagnostic = .{ .code = "53300", .message = "The relation catalog publication is not ready to authorize this DDL. No schema change was admitted.", .retryable = true, .transaction_status = "idle" };
    try std.testing.expect(definiteDdlNonadmission(safe));
    var changed = safe;
    changed.catalog_receipt = .null;
    try std.testing.expect(!definiteDdlNonadmission(changed));
    changed = safe;
    changed.ddl_receipt = .null;
    try std.testing.expect(!definiteDdlNonadmission(changed));
    changed = safe;
    changed.admission_outcome = "unknown";
    try std.testing.expect(!definiteDdlNonadmission(changed));
    changed = safe;
    changed.mutation_outcome = "committed_pending";
    try std.testing.expect(!definiteDdlNonadmission(changed));
    changed = safe;
    changed.message = "The declaration committed; activation is still pending.";
    try std.testing.expect(!definiteDdlNonadmission(changed));
}

test "SQL default lifecycle retries only definite read admission" {
    const safe: Diagnostic = .{ .code = "53300", .message = "A consistent SQL statement read is temporarily unavailable. Retry the complete statement after a bounded delay.", .retryable = true, .transaction_status = "idle" };
    try std.testing.expect(definiteReadAdmission(safe));
    var changed = safe;
    changed.catalog_receipt = .null;
    try std.testing.expect(!definiteReadAdmission(changed));
    changed = safe;
    changed.message = "The requested write is not yet visible within the readiness budget.";
    try std.testing.expect(!definiteReadAdmission(changed));
    changed = safe;
    changed.transaction_id = "invalid";
    try std.testing.expect(!definiteReadAdmission(changed));
    changed = safe;
    changed.status = "committed_pending";
    try std.testing.expect(!definiteReadAdmission(changed));
    changed = safe;
    changed.mutation_outcome = "committed";
    try std.testing.expect(!definiteReadAdmission(changed));
    changed = safe;
    changed.admission_outcome = "unknown";
    try std.testing.expect(!definiteReadAdmission(changed));
    changed = safe;
    changed.transaction_status = "failed";
    try std.testing.expect(!definiteReadAdmission(changed));
    changed = safe;
    changed.retryable = false;
    try std.testing.expect(!definiteReadAdmission(changed));
}

pub fn execute(ctx: *fixture.Context, statement: []const u8, retry: Retry) !http.HttpResponse {
    return executeIn(ctx, null, statement, retry);
}

pub fn executeIn(ctx: *fixture.Context, namespace: ?[]const u8, statement: []const u8, retry: Retry) !http.HttpResponse {
    return executeConnected(ctx, namespace, null, statement, retry);
}

/// Named scopes use the same precise nonadmission classifier and deadline as
/// default-scope campaigns. Transport/unknown or committed outcomes never replay.
pub fn executeInDatabase(ctx: *fixture.Context, database: []const u8, namespace: []const u8, statement: []const u8, retry: Retry) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .database = database, .namespace = namespace }, .{});
    defer ctx.alloc.free(body);
    return executeBody(ctx, null, body, retry);
}

/// Campaigns with large complete postimages must request their bounded read
/// budget explicitly; ordinary callers retain the production default.
pub fn executeBounded(ctx: *fixture.Context, statement: []const u8, limit: u32, retry: Retry) !http.HttpResponse {
    if (limit == 0 or limit > 4096) return error.InvalidSqlLimit;
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .limit = limit }, .{});
    defer ctx.alloc.free(body);
    return executeBody(ctx, null, body, retry);
}

pub fn executeParameters(ctx: *fixture.Context, statement: []const u8, parameters: []const std.json.Value, retry: Retry) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .parameters = parameters }, .{});
    defer ctx.alloc.free(body);
    return executeBody(ctx, null, body, retry);
}

pub fn executeConnectedParameters(ctx: *fixture.Context, connection: []const u8, statement: []const u8, parameters: []const std.json.Value, retry: Retry) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement, .parameters = parameters, .connection_id = connection }, .{});
    defer ctx.alloc.free(body);
    return executeBody(ctx, connection, body, retry);
}

pub fn executeConnected(
    ctx: *fixture.Context,
    namespace: ?[]const u8,
    connection: ?[]const u8,
    statement: []const u8,
    retry: Retry,
) !http.HttpResponse {
    const request = .{ .statement = statement, .namespace = namespace, .connection_id = connection };
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, request, .{ .emit_null_optional_fields = false });
    defer ctx.alloc.free(body);
    return executeBody(ctx, connection, body, retry);
}

fn executeBody(ctx: *fixture.Context, connection: ?[]const u8, body: []const u8, retry: Retry) !http.HttpResponse {
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (true) {
        try ctx.checkDrivers();
        const remaining = deadline -| platform.time.monotonicNs();
        if (remaining == 0) return error.SqlOwnerTestRequestTimeout;
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql", .POST, body, @intCast(@max(1, remaining / std.time.ns_per_ms)));
        // 53300 alone also covers post-commit visibility waits. Only the
        // specific no-admission statement-read failure can repeat preparation.
        if (retry != .never and response.status == 503) {
            var diagnostic = std.json.parseFromSlice(Diagnostic, ctx.alloc, response.body, .{ .ignore_unknown_fields = true }) catch |err| {
                response.deinit(ctx.alloc);
                return err;
            };
            defer diagnostic.deinit();
            if (definiteReadAdmission(diagnostic.value) or
                (retry == .write_admission and definiteDdlNonadmission(diagnostic.value)) or
                (retry == .session_observation and connection != null and sessionObservationCapacity(diagnostic.value)))
            {
                response.deinit(ctx.alloc);
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            }
        }
        return response;
    }
}
