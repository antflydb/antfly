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

//! Prepared ordinary DDL and authorization-bound recovery on the native journal.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const http = @import("http_server.zig");
const native = @import("../system_catalog/catalog_mutations.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;
const base64 = std.base64.url_safe_no_pad;
const Command = @import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand;
pub const Token = struct { version: u8 = 11, kind: common.Receipt.Kind, request: native.Observation };

fn kind(target: native.Target) common.Receipt.Kind {
    return switch (target.kind) {
        .table => .table,
        .database => .database,
        .namespace => .namespace,
        .tablespace => .tablespace,
        .query_source => .query_source,
        else => unreachable,
    };
}
fn operation(target: native.Target) common.Receipt.Operation {
    return switch (target.action) {
        .create => .create,
        .drop => .drop,
        .rename => .rename,
        .comment, .set_tablespace, .alter => .alter,
    };
}
pub fn authorize(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, a: A, target: native.Target) !void {
    try target.validateLabels();
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    const routes = @import("../system_catalog/routes.zig");
    const resource = try routes.resourceNameAlloc(a, .{ .kind = target.kind, .database = target.database, .namespace = target.namespace, .name = target.name });
    defer a.free(resource);
    if (!try http.resourcePermissionCurrentlyAllowed(identity, routes.permissionKind(target.kind), resource, .admin)) return error.Forbidden;
}
fn withToken(a: A, request: native.Observation, token: []const u8) !common.Receipt {
    const database = try a.dupe(u8, request.target.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, request.target.namespace);
    errdefer a.free(namespace);
    const name = try a.dupe(u8, request.target.name);
    const diagnostic = local.sql_errors.describe(error.SqlMutationOutcomeUnknown);
    return .{ .kind = kind(request.target), .operation = operation(request.target), .database = database, .namespace = namespace, .name = name, .recovery_token = token, .diagnostic = .{ .code = diagnostic.code, .message = diagnostic.message, .retryable = false } };
}
pub fn newReceipt(a: A, scratch: A, request: native.Observation) !common.Receipt {
    try request.validate();
    const json = try std.json.Stringify.valueAlloc(scratch, Token{ .kind = kind(request.target), .request = request }, .{});
    defer scratch.free(json);
    const size = base64.Encoder.calcSize(json.len);
    if (size > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    const token = try a.alloc(u8, size);
    errdefer a.free(token);
    _ = base64.Encoder.encode(token, json);
    return withToken(a, request, token);
}
pub fn decodeOutcome(a: A, bytes: []const u8, request: native.Observation) ?native.Outcome {
    request.validate() catch return null;
    var parsed = std.json.parseFromSlice(?native.Outcome, a, bytes, .{}) catch return null;
    defer parsed.deinit();
    const outcome = parsed.value orelse return null;
    if (outcome == .pending) return null;
    if (outcome == .success) {
        const result = outcome.success;
        result.validate() catch return null;
        if (result.revision != request.expected_revision + 1 or result.removed != @intFromBool(request.target.action == .drop) or result.no_op or result.lock_effect != null or result.cascade_notice != null) return null;
        if ((request.target.action == .drop or request.target.kind == .table) != (result.identity == null)) return null;
        if (result.identity) |identity| if (identity.generation != result.revision) return null;
    }
    return outcome;
}
fn complete(receipt: *common.Receipt, request: native.Observation, outcome: native.Outcome) void {
    common.complete(receipt, outcome);
    if (outcome == .success and request.target.kind == .table and request.target.action != .drop and request.target.table_id != 0) {
        receipt.object_id = request.target.table_id;
        receipt.generation = outcome.success.revision;
    }
}
pub fn observe(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, request: native.Observation) ?native.Outcome {
    context.ensureActive() catch return null;
    authorize(server, identity, a, request.target) catch return null;
    var private = context;
    private.setting_admin = true;
    const bytes = server.source.systemCatalog(a, private, .{ .sql_catalog_outcome = request }) catch return null;
    defer a.free(bytes);
    return decodeOutcome(a, bytes, request);
}
pub fn recoverIfOrdinary(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, a: A, encoded: []const u8) !?common.Receipt {
    if (encoded.len == 0 or encoded.len > common.max_token_bytes) return error.InvalidSqlRecoveryToken;
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const size = base64.Decoder.calcSizeForSlice(encoded) catch return error.InvalidSqlRecoveryToken;
    const bytes = try scratch.alloc(u8, size);
    base64.Decoder.decode(bytes, encoded) catch return error.InvalidSqlRecoveryToken;
    const header = std.json.parseFromSliceLeaky(struct { version: u8 }, scratch, bytes, .{ .ignore_unknown_fields = true }) catch return error.InvalidSqlRecoveryToken;
    if (header.version != 11) return null;
    const token = std.json.parseFromSliceLeaky(Token, scratch, bytes, .{ .allocate = .alloc_always }) catch return error.InvalidSqlRecoveryToken;
    token.request.validate() catch return error.InvalidSqlRecoveryToken;
    if (token.kind != kind(token.request.target)) return error.InvalidSqlRecoveryToken;
    try authorize(server, identity, scratch, token.request.target);
    const owned = try a.dupe(u8, encoded);
    errdefer a.free(owned);
    var receipt = try withToken(a, token.request, owned);
    if (observe(server, identity, context, scratch, token.request)) |outcome| complete(&receipt, token.request, outcome);
    return receipt;
}

pub const Reply = struct {
    legacy: ?[]u8 = null,
    receipt: ?common.Receipt = null,
    table_id: u64 = 0,
    table_id_text: ?[]u8 = null,
    unknown: bool = false,
    pub fn takeReceipt(self: *@This()) ?common.Receipt {
        const receipt = self.receipt;
        self.receipt = null;
        return receipt;
    }
    pub fn takeTableId(self: *@This()) ?[]u8 {
        const text = self.table_id_text;
        self.table_id_text = null;
        return text;
    }
    pub fn deinit(self: *@This(), a: A) void {
        if (self.table_id_text) |text| a.free(text);
        if (self.legacy) |bytes| a.free(bytes);
        if (self.receipt) |receipt| receipt.deinit(a);
        self.* = undefined;
    }
};
pub fn submit(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, allocators: local.sql_catalog.DdlAllocators, request: domain.Request) !Reply {
    if (context.sql_command_fence == null) return .{ .legacy = try server.source.systemCatalog(allocators.result, context, .{ .mutate = request }) };
    var region: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer region.deinit();
    const scratch = region.allocator();
    var private = context;
    private.setting_admin = true;
    try authorize(server, identity, scratch, .{ .kind = request.mutation.kind, .action = request.mutation.action, .database = request.mutation.database, .namespace = request.mutation.namespace, .name = request.mutation.name, .table_id = 0 });
    const io = server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const bytes = try server.source.systemCatalog(scratch, private, .{ .sql_catalog_prepare = .{ .request = request, .nonce = nonce, .command_fence = context.sql_command_fence.?.* } });
    const command = try std.json.parseFromSliceLeaky(Command, scratch, bytes, .{});
    const observation = try native.observation(scratch, command);
    // A faulty/foreign preparer cannot redirect permission or recovery labels.
    if (!try native.matchesIntent(scratch, command.mutation, request.mutation) or !std.mem.eql(u8, &observation.admission.request.nonce, &nonce) or !std.meta.eql(observation.admission.request.command_fence, @as(?@import("../system_catalog/command_fence.zig").Fence, context.sql_command_fence.?.*))) return error.InvalidCatalogMutation;
    if (request.physical_name) |name| if (!std.mem.eql(u8, name, command.mutation.storage_name)) return error.InvalidCatalogMutation;
    try authorize(server, identity, scratch, observation.target);
    var receipt = try newReceipt(allocators.result, scratch, observation);
    errdefer receipt.deinit(allocators.result);
    const table_id_text = if (request.mutation.kind == .table and request.mutation.action == .create) try std.fmt.allocPrint(allocators.result, "{d}", .{observation.target.table_id}) else null;
    errdefer if (table_id_text) |text| allocators.result.free(text);
    try context.ensureActive();
    const reply = server.source.systemCatalog(scratch, private, .{ .sql_catalog_mutate = command }) catch null;
    var outcome = if (reply) |value| decodeOutcome(scratch, value, observation) else null;
    if (outcome == null) outcome = observe(server, identity, context, scratch, observation);
    if (outcome) |known| {
        _ = try known.result();
        complete(&receipt, observation, known);
        return .{ .receipt = receipt, .table_id = observation.target.table_id, .table_id_text = table_id_text };
    }
    return .{ .receipt = receipt, .table_id = observation.target.table_id, .table_id_text = table_id_text, .unknown = true };
}
