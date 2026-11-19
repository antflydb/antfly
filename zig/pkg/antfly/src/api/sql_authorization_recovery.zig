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

//! Actor-bound read-only observation of the existing catalog decision ring.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const auth = @import("../system_catalog/authorization.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;
const base64 = std.base64.url_safe_no_pad;
pub const Token = struct { version: u8 = 5, kind: common.Receipt.Kind = .authorization, observed_revision: u64, request: auth.Observation };

pub fn actor(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity) !auth.Actor {
    if (!server.cfg.auth_enabled) return .{ .session_user = .bootstrap, .credential = "" };
    const user = identity orelse return error.Forbidden;
    if (user.is_internal_service) return error.Forbidden;
    const manager = user.live_user_manager orelse return error.Forbidden;
    const instance = user.user_instance_id orelse return error.Forbidden;
    if (!try manager.sqlCredentialPermits(user.username, instance, user.credential_principal, "", 0)) return error.Forbidden;
    return .{ .session_user = .{ .kind = .user, .instance = instance }, .credential = user.credential_principal };
}
/// Existing trusted-principal/native-service table creation remains native.
/// Only authenticated local instances participate in SQL creator defaults.
pub fn creator(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity) !?auth.Principal {
    if (!server.source.vtable.supports_sql_authorization) return null;
    if (!server.cfg.auth_enabled) return .bootstrap;
    const value = identity orelse return error.Forbidden;
    if (value.is_internal_service or value.live_user_manager == null or value.user_instance_id == null) return null;
    return (try actor(server, identity)).session_user;
}

fn authorize(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, request: auth.Observation) !void {
    const current = try actor(server, identity);
    if (!current.session_user.eql(request.actor.session_user) or current.current_role != null or request.actor.current_role != null or !std.mem.eql(u8, current.credential, request.actor.credential)) return error.Forbidden;
}
fn withToken(a: A, request: auth.Observation, encoded: []const u8) !common.Receipt {
    const database = try a.dupe(u8, "");
    errdefer a.free(database);
    const namespace = try a.dupe(u8, "");
    errdefer a.free(namespace);
    const name = try a.dupe(u8, request.label);
    const diagnostic = local.sql_errors.describe(error.SqlMutationOutcomeUnknown);
    return .{ .kind = .authorization, .operation = std.meta.stringToEnum(common.Receipt.Operation, @tagName(request.operation)).?, .database = database, .namespace = namespace, .name = name, .recovery_token = encoded, .diagnostic = .{ .code = diagnostic.code, .message = diagnostic.message, .retryable = diagnostic.retryable orelse false } };
}
pub fn newReceipt(a: A, scratch: A, request: auth.Observation, revision: u64) !common.Receipt {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(scratch, Token{ .request = request, .observed_revision = revision }, .{});
    defer scratch.free(bytes);
    const size = base64.Encoder.calcSize(bytes.len);
    if (size > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    const encoded = try a.alloc(u8, size);
    errdefer a.free(encoded);
    _ = base64.Encoder.encode(encoded, bytes);
    return withToken(a, request, encoded);
}
pub fn decodeOutcome(a: A, bytes: []const u8, request: auth.Observation, revision: u64) ?auth.outcomes.Outcome {
    const result = (std.json.parseFromSliceLeaky(?auth.outcomes.Outcome, a, bytes, .{}) catch return null) orelse return null;
    if (result == .success) {
        const value = result.success;
        value.validate() catch return null;
        if (value.revision < revision) return null;
        switch (request.operation) {
            .create => if (value.identity == null or value.removed != 0 or value.no_op) return null,
            .drop => if (value.identity != null or (!value.no_op and value.removed != 1)) return null,
            .alter => if (value.identity != null or value.removed != 0) return null,
        }
    }
    return result;
}
pub fn observe(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: auth.Observation, revision: u64) ?auth.outcomes.Outcome {
    context.ensureActive() catch return null;
    authorize(server, identity, request) catch return null;
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_authorization_outcome = request }) catch return null;
    return decodeOutcome(a, bytes, request, revision);
}
pub fn recoverIfAuthorization(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, encoded: []const u8) !?common.Receipt {
    if (encoded.len == 0 or encoded.len > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try scratch.alloc(u8, size);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const header = std.json.parseFromSliceLeaky(struct { version: u8, kind: common.Receipt.Kind }, scratch, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidSqlRecoveryToken;
    if (header.version != 5 and header.kind != .authorization) return null;
    if (header.version != 5 or header.kind != .authorization) return error.InvalidSqlRecoveryToken;
    if (!server.source.vtable.supports_sql_authorization) return error.UnsupportedSqlExecution;
    const token = std.json.parseFromSliceLeaky(Token, scratch, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidSqlRecoveryToken;
    token.request.validate() catch return error.InvalidSqlRecoveryToken;
    try context.ensureActive();
    try authorize(server, identity, token.request);
    const owned = try a.dupe(u8, encoded);
    errdefer a.free(owned);
    var receipt = try withToken(a, token.request, owned);
    if (observe(server, identity, context, scratch, token.request, token.observed_revision)) |outcome| common.complete(&receipt, outcome);
    return receipt;
}
