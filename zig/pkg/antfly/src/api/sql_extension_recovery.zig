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

//! Read-only exact recovery; a receipt never authorizes replaying a package.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const extensions = @import("../system_catalog/sql_extensions.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;
const base64 = std.base64.url_safe_no_pad;
pub const Token = struct { version: u8 = 9, kind: common.Receipt.Kind = .extension, observed_revision: u64, request: extensions.OutcomeRequest };
pub fn authorize(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, _: A, database: []const u8) !void {
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    if (!try http.resourcePermissionCurrentlyAllowed(identity, .database, database, .admin)) return error.Forbidden;
}
fn withToken(a: A, request: extensions.OutcomeRequest, token: []const u8) !common.Receipt {
    const database = try a.dupe(u8, request.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, "");
    errdefer a.free(namespace);
    const name = try a.dupe(u8, request.name);
    const diagnostic = local.sql_errors.describe(error.SqlMutationOutcomeUnknown);
    return .{ .kind = .extension, .operation = request.operation, .database = database, .namespace = namespace, .name = name, .recovery_token = token, .diagnostic = .{ .code = diagnostic.code, .message = diagnostic.message, .retryable = diagnostic.retryable orelse false } };
}
pub fn newReceipt(a: A, scratch: A, request: extensions.OutcomeRequest, revision: u64) !common.Receipt {
    try request.validate();
    const bytes = try std.json.Stringify.valueAlloc(scratch, Token{ .request = request, .observed_revision = revision }, .{});
    defer scratch.free(bytes);
    const size = base64.Encoder.calcSize(bytes.len);
    if (size > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    const token = try a.alloc(u8, size);
    errdefer a.free(token);
    _ = base64.Encoder.encode(token, bytes);
    return withToken(a, request, token);
}
pub fn decodeOutcome(a: A, bytes: []const u8, request: extensions.OutcomeRequest, revision: u64) ?extensions.Outcome {
    const result = (std.json.parseFromSliceLeaky(?extensions.Outcome, a, bytes, .{}) catch return null) orelse return null;
    if (result == .pending) return null;
    if (result == .success) {
        const value = result.success;
        value.validate() catch return null;
        if (value.revision < revision or (request.operation == .drop and value.identity != null) or (request.operation != .drop and (value.identity == null or value.removed != 0))) return null;
    }
    return result;
}
pub fn observe(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: extensions.OutcomeRequest, revision: u64) ?extensions.Outcome {
    context.ensureActive() catch return null;
    authorize(server, identity, a, request.database) catch return null;
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_extension_outcome = request }) catch return null;
    return decodeOutcome(a, bytes, request, revision);
}
pub fn recoverIfExtension(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, encoded: []const u8) !?common.Receipt {
    if (encoded.len == 0 or encoded.len > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try scratch.alloc(u8, size);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const header = std.json.parseFromSliceLeaky(struct { version: u8, kind: common.Receipt.Kind }, scratch, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidSqlRecoveryToken;
    if (header.kind != .extension) return null;
    if (header.version != 9) return error.InvalidSqlRecoveryToken;
    const token = std.json.parseFromSliceLeaky(Token, scratch, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidSqlRecoveryToken;
    try token.request.validate();
    try context.ensureActive();
    try authorize(server, identity, scratch, token.request.database);
    const owned = try a.dupe(u8, encoded);
    errdefer a.free(owned);
    var receipt = try withToken(a, token.request, owned);
    if (observe(server, identity, context, scratch, token.request, token.observed_revision)) |outcome| common.complete(&receipt, outcome);
    return receipt;
}
