// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Read-only recovery of the exact bound catalog operation. A token carries
//! identity, not authority or permission to resubmit a mutation.
const std = @import("std");
const sources = @import("antfly_local_sources");
const catalog = sources.sql_catalog;
const errors = sources.sql_errors;
const seq = @import("../system_catalog/sequence_catalog.zig");
const http = @import("http_server.zig");
const auth = @import("sql_catalog_auth.zig");
const A = std.mem.Allocator;
const Context = sources.api_operation.RequestContext;
pub const Receipt = catalog.CatalogOperationReceipt;
pub const max_token_bytes = 8192;
const base64 = std.base64.url_safe_no_pad;
pub const Token = struct { version: u8, kind: Receipt.Kind, observed_revision: u64, request: seq.Mutation };

pub fn encode(a: A, scratch: A, request: seq.Mutation, revision: u64) ![]u8 {
    try request.validate();
    const json = try std.json.Stringify.valueAlloc(scratch, Token{ .version = 1, .kind = .sequence, .observed_revision = revision, .request = request }, .{});
    defer scratch.free(json);
    const size = base64.Encoder.calcSize(json.len);
    if (size > max_token_bytes) return error.InvalidSqlRecoveryToken;
    const result = try a.alloc(u8, size);
    _ = base64.Encoder.encode(result, json);
    return result;
}

/// All variable data borrows caller scratch, never the public token bytes.
pub fn decode(a: A, encoded: []const u8) !Token {
    if (encoded.len == 0 or encoded.len > max_token_bytes) return error.InvalidSqlRecoveryToken;
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try a.alloc(u8, size);
    defer a.free(bytes);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const result = std.json.parseFromSliceLeaky(Token, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidSqlRecoveryToken,
    };
    if (result.version != 1) return error.InvalidSqlRecoveryToken;
    result.request.validate() catch return error.InvalidSqlRecoveryToken;
    return result;
}

pub fn newReceipt(a: A, scratch: A, request: seq.Mutation, revision: u64) !Receipt {
    const token = try encode(a, scratch, request, revision);
    errdefer a.free(token);
    return receiptWithToken(a, request, token);
}

fn receiptWithToken(a: A, request: seq.Mutation, token: []const u8) !Receipt {
    const database = try a.dupe(u8, request.target.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, request.target.namespace);
    errdefer a.free(namespace);
    const name = try a.dupe(u8, request.target.table);
    return .{ .operation = switch (request.operation) {
        .create => .create,
        .alter => .alter,
        .rename => .rename,
        .drop => .drop,
    }, .database = database, .namespace = namespace, .name = name, .recovery_token = token, .diagnostic = diagnostic(error.SqlMutationOutcomeUnknown) };
}

fn diagnostic(err: anyerror) Receipt.Diagnostic {
    const value = errors.describe(err);
    return .{ .code = value.code, .message = value.message, .retryable = value.retryable orelse false };
}

pub fn checkResult(request: seq.Mutation, revision: u64, result: seq.Result) !void {
    result.validate() catch return error.SqlMutationOutcomeUnknown;
    if (result.revision < revision) return error.SqlMutationOutcomeUnknown;
    switch (request.operation) {
        .create => |value| if (result.removed != 0 or (!value.if_not_exists and result.identity == null)) return error.SqlMutationOutcomeUnknown,
        .drop => |value| if (result.identity != null or (!value.if_exists and result.removed != 1)) return error.SqlMutationOutcomeUnknown,
        .alter => |value| {
            const id = result.identity orelse return error.SqlMutationOutcomeUnknown;
            if (result.removed != 0 or id.id != value.expected.id or id.generation < value.expected.generation or id.generation - value.expected.generation > 1) return error.SqlMutationOutcomeUnknown;
        },
        .rename => |value| if (result.removed != 0 or !std.meta.eql(result.identity, @as(?seq.Identity, value.expected))) return error.SqlMutationOutcomeUnknown,
    }
}

pub fn decodeOutcome(a: A, bytes: []const u8, request: seq.Mutation, revision: u64) ?seq.Outcome {
    const value = (std.json.parseFromSliceLeaky(?seq.Outcome, a, bytes, .{}) catch return null) orelse return null;
    if (value == .success) checkResult(request, revision, value.success) catch return null;
    return value;
}

/// Completion never allocates after admission. Decimal formatting belongs to
/// response encoding and cannot turn a committed operation into a retryable OOM.
pub fn complete(receipt: *Receipt, outcome: seq.Outcome) void {
    switch (outcome) {
        .success => |value| {
            receipt.state = .committed;
            receipt.catalog_revision = value.revision;
            if (value.identity) |id| {
                receipt.object_id = id.id;
                receipt.generation = id.generation;
            }
            receipt.diagnostic = null;
        },
        .rejected => {
            receipt.state = .rejected;
            _ = outcome.result() catch |err| {
                receipt.diagnostic = diagnostic(err);
                return;
            };
            unreachable;
        },
    }
}

pub fn observe(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: seq.Mutation, revision: u64) ?seq.Outcome {
    context.ensureActive() catch return null;
    auth.authorizeTarget(server, identity, a, request.target) catch return null;
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_sequence_outcome = request }) catch return null;
    return decodeOutcome(a, bytes, request, revision);
}

pub fn recover(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, encoded: []const u8) !Receipt {
    try context.ensureActive();
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const temporary = scratch.allocator();
    const token = try decode(temporary, encoded);
    // Readiness being disabled for NEW statements must not disable observing
    // already admitted operations. Authorization is live, never token-carried.
    try auth.authorizeTarget(server, identity, temporary, token.request.target);
    const owned = try a.dupe(u8, encoded);
    errdefer a.free(owned);
    var receipt = try receiptWithToken(a, token.request, owned);
    if (observe(server, identity, context, temporary, token.request, token.observed_revision)) |outcome| complete(&receipt, outcome);
    return receipt;
}
