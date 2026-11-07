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

//! Compact recovery for callable mutations on the shared catalog outcome ring.
//! The token binds a nonce/digest and target, never function text or authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const casts = @import("../system_catalog/casts.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;
const base64 = std.base64.url_safe_no_pad;
pub const Token = struct { version: u8 = 3, kind: common.Receipt.Kind = .cast, observed_revision: u64, request: casts.OutcomeRequest };

pub fn authorize(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, database: []const u8) !void {
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    if (!try http.resourcePermissionCurrentlyAllowed(identity, .database, database, .admin)) return error.Forbidden;
}

fn encode(a: A, scratch: A, request: casts.OutcomeRequest, revision: u64) ![]u8 {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(scratch, Token{ .kind = .cast, .request = request, .observed_revision = revision }, .{});
    defer scratch.free(bytes);
    const size = base64.Encoder.calcSize(bytes.len);
    if (size > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    const result = try a.alloc(u8, size);
    _ = base64.Encoder.encode(result, bytes);
    return result;
}
fn withToken(a: A, request: casts.OutcomeRequest, token: []const u8) !common.Receipt {
    const database = try a.dupe(u8, request.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, "");
    errdefer a.free(namespace);
    const name = try request.pair.displayAlloc(a);
    const diagnostic = local.sql_errors.describe(error.SqlMutationOutcomeUnknown);
    return .{ .kind = .cast, .operation = std.meta.stringToEnum(common.Receipt.Operation, @tagName(request.operation)).?, .database = database, .namespace = namespace, .name = name, .recovery_token = token, .diagnostic = .{ .code = diagnostic.code, .message = diagnostic.message, .retryable = diagnostic.retryable orelse false } };
}
pub fn newReceipt(a: A, scratch: A, request: casts.OutcomeRequest, revision: u64) !common.Receipt {
    const token = try encode(a, scratch, request, revision);
    errdefer a.free(token);
    return withToken(a, request, token);
}
pub fn decodeOutcome(a: A, bytes: []const u8, request: casts.OutcomeRequest, revision: u64) ?casts.Outcome {
    const result = (std.json.parseFromSliceLeaky(?casts.Outcome, a, bytes, .{}) catch return null) orelse return null;
    if (result == .success) {
        const value = result.success;
        value.validate() catch return null;
        if (value.revision < revision or (request.operation == .drop and value.identity != null) or (request.operation != .drop and (value.identity == null or value.removed != 0))) return null;
    }
    return result;
}
/// Recovery follows the immutable database identity. A renamed database must
/// authorize its current name; a reused old name cannot inherit the token.
fn liveDatabase(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: casts.OutcomeRequest) !?[]const u8 {
    try context.ensureActive();
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    const lookup: casts.SnapshotRequest = .{ .database = request.database, .database_id = request.database_id, .pair = request.pair };
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_cast_snapshot = lookup }) catch |err| switch (err) {
        error.CatalogGenerationChanged => return null,
        else => return err,
    };
    const snapshot = try std.json.parseFromSliceLeaky(casts.Snapshot, a, bytes, .{});
    try snapshot.validate(lookup);
    if (!std.mem.eql(u8, &snapshot.incarnation, &request.incarnation)) return null;
    const database = snapshot.database orelse return null;
    try authorize(server, identity, database);
    return database;
}
pub fn observe(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: casts.OutcomeRequest, revision: u64) ?casts.Outcome {
    context.ensureActive() catch return null;
    _ = (liveDatabase(server, identity, context, a, request) catch return null) orelse return null;
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_cast_outcome = request }) catch return null;
    return decodeOutcome(a, bytes, request, revision);
}

/// Return null only for a different versioned token. Malformed callable tokens
/// cannot fall back to interpreting a sequence request or gaining its authority.
pub fn recoverIfCast(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, encoded: []const u8) !?common.Receipt {
    if (encoded.len == 0 or encoded.len > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try scratch.alloc(u8, size);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const header = std.json.parseFromSliceLeaky(struct { version: u8, kind: common.Receipt.Kind }, scratch, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidSqlRecoveryToken;
    if (header.version != 3 and header.kind != .cast) return null;
    if (header.version != 3 or header.kind != .cast) return error.InvalidSqlRecoveryToken;
    const token = std.json.parseFromSliceLeaky(Token, scratch, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidSqlRecoveryToken;
    token.request.validate() catch return error.InvalidSqlRecoveryToken;
    try context.ensureActive();
    _ = try liveDatabase(server, identity, context, scratch, token.request);
    const owned = try a.dupe(u8, encoded);
    errdefer a.free(owned);
    var receipt = try withToken(a, token.request, owned);
    if (observe(server, identity, context, scratch, token.request, token.observed_revision)) |outcome| common.complete(&receipt, outcome);
    return receipt;
}
