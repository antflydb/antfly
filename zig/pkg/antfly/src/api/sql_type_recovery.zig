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

//! Read-only recovery of namespace-owned type mutations. A token is an exact
//! journal lookup identity, never a mutation replay or namespace authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const types = @import("../system_catalog/named_types.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;
const base64 = std.base64.url_safe_no_pad;
pub const Token = struct { version: u8 = 4, kind: common.Receipt.Kind = .type, observed_revision: u64, request: types.OutcomeRequest };
pub const authorize = @import("sql_routine_recovery.zig").authorize;

fn encode(a: A, scratch: A, request: types.OutcomeRequest, revision: u64) ![]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(scratch, Token{ .request = request, .observed_revision = revision }, .{});
    defer scratch.free(bytes);
    const size = base64.Encoder.calcSize(bytes.len);
    if (size > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    const result = try a.alloc(u8, size);
    _ = base64.Encoder.encode(result, bytes);
    return result;
}
fn withToken(a: A, request: types.OutcomeRequest, token: []const u8) !common.Receipt {
    const database = try a.dupe(u8, request.target.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, request.target.namespace);
    errdefer a.free(namespace);
    const name = try a.dupe(u8, request.target.table);
    const diagnostic = local.sql_errors.describe(error.SqlMutationOutcomeUnknown);
    return .{ .kind = .type, .operation = std.meta.stringToEnum(common.Receipt.Operation, @tagName(request.operation)).?, .database = database, .namespace = namespace, .name = name, .recovery_token = token, .diagnostic = .{ .code = diagnostic.code, .message = diagnostic.message, .retryable = diagnostic.retryable orelse false } };
}
pub fn newReceipt(a: A, scratch: A, request: types.OutcomeRequest, revision: u64) !common.Receipt {
    const token = try encode(a, scratch, request, revision);
    errdefer a.free(token);
    return withToken(a, request, token);
}
pub fn decodeOutcome(a: A, bytes: []const u8, request: types.OutcomeRequest, revision: u64) ?types.Outcome {
    const outcome = (std.json.parseFromSliceLeaky(?types.Outcome, a, bytes, .{}) catch return null) orelse return null;
    if (outcome == .success) {
        const value = outcome.success;
        value.validate() catch return null;
        if (value.revision < revision or (request.operation == .drop and value.identity != null) or (request.operation != .drop and (value.identity == null or value.removed != 0))) return null;
    }
    return outcome;
}
fn authorizeObservation(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: types.OutcomeRequest) !bool {
    try context.ensureActive();
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    const lookup: types.SnapshotRequest = .{ .targets = &.{request.target}, .namespace_id = request.namespace_id, .scope_only = true };
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_type_snapshot = lookup }) catch |err| switch (err) {
        error.CatalogGenerationChanged => {
            // A retired scope has no current name to authorize. Its old name
            // may belong to someone else; only a global administrator may
            // observe this retained decision without a live namespace.
            if (!try http.resourcePermissionCurrentlyAllowed(identity, .namespace, "*", .admin)) return error.Forbidden;
            return true;
        },
        else => return err,
    };
    const snapshot = try std.json.parseFromSliceLeaky(types.Snapshot, a, bytes, .{});
    try snapshot.validate(a, lookup);
    try authorize(server, identity, a, snapshot.scopes[0].target);
    return std.mem.eql(u8, &snapshot.incarnation, &request.incarnation);
}
fn observeAuthorized(server: *http.ApiHttpServer, context: Context, a: A, request: types.OutcomeRequest, revision: u64) ?types.Outcome {
    context.ensureActive() catch return null;
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_type_outcome = request }) catch return null;
    return decodeOutcome(a, bytes, request, revision);
}
pub fn observe(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: types.OutcomeRequest, revision: u64) ?types.Outcome {
    if (!(authorizeObservation(server, identity, context, a, request) catch return null)) return null;
    return observeAuthorized(server, context, a, request, revision);
}
pub fn recoverIfType(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, encoded: []const u8) !?common.Receipt {
    if (encoded.len == 0 or encoded.len > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try scratch.alloc(u8, size);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const header = std.json.parseFromSliceLeaky(struct { version: u8, kind: common.Receipt.Kind }, scratch, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidSqlRecoveryToken;
    if (header.version != 4 and header.kind != .type) return null;
    if (header.version != 4 or header.kind != .type) return error.InvalidSqlRecoveryToken;
    if (!server.source.vtable.supports_sql_types) return error.UnsupportedSqlExecution;
    const token = std.json.parseFromSliceLeaky(Token, scratch, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidSqlRecoveryToken;
    token.request.validate() catch return error.InvalidSqlRecoveryToken;
    const authorized = try authorizeObservation(server, identity, context, scratch, token.request);
    const owned = try a.dupe(u8, encoded);
    errdefer a.free(owned);
    var receipt = try withToken(a, token.request, owned);
    if (authorized) if (observeAuthorized(server, context, scratch, token.request, token.observed_revision)) |outcome| common.complete(&receipt, outcome);
    return receipt;
}
