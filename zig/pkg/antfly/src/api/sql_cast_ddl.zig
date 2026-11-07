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

//! Database-owned cast producer. Only the support function uses search_path;
//! publication and exact outcome observation use the immutable database ID.
const std = @import("std");
const local = @import("antfly_local_sources");
const casts = @import("../system_catalog/casts.zig");
const catalog = local.sql_catalog;
const execution = @import("sql_execution.zig");
const calls = @import("sql_routine_execution.zig");
const recovery = @import("sql_cast_recovery.zig");
const common = @import("sql_catalog_recovery.zig");

pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if (ddl.kind != .cast or (ddl.action != .create and ddl.action != .drop)) return error.UnsupportedSqlExecution;
    if (root.active_transaction != null or !root.server.source.vtable.supports_sql_casts) return error.UnsupportedSqlExecution;
    try root.context.ensureActive();
    try recovery.authorize(root.server, root.identity.*, root.database);
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const intent = ddl.cast_definition orelse return error.InvalidSqlSyntax;
    // Do not admit a declaration whose runtime coercion policy is unavailable.
    if (ddl.action == .create and (intent.method != .function or intent.context == .implicit)) return error.UnsupportedSqlShape;
    const pair: casts.records.Pair = .{ .source = try catalog.routines.storageType(intent.source), .target = try catalog.routines.storageType(intent.target) };
    if (ddl.action == .drop and catalog.builtin_casts.exists(pair)) return error.UnsupportedSqlShape;
    const snapshot_request: casts.SnapshotRequest = .{ .database = root.database, .pair = pair, .expected_revision = root.revision };
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_cast_snapshot = snapshot_request });
    const cut = try std.json.parseFromSliceLeaky(casts.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try cut.validate(snapshot_request);
    const database_id = cut.database_id orelse return error.DatabaseNotFound;
    const creating = ddl.action == .create;
    const definition: ?casts.records.Definition = if (creating) declared: {
        const name = intent.function orelse return error.InvalidSqlSyntax;
        const targets = try calls.targets(root, a, name);
        const signature = if (ddl.routine_signature) |types| encoded: {
            const arguments = try a.alloc(catalog.routines.records.Argument, types.len);
            for (types, arguments) |kind, *argument| argument.* = .{ .type = try catalog.routines.storageType(kind) };
            break :encoded try (catalog.routines.records.Definition{ .arguments = arguments, .result = pair.target, .body = "SELECT NULL", .owner = .auth_disabled }).signatureAlloc(a);
        } else null;
        const functions = try calls.snapshot(root, a, .{ .targets = targets, .signature = signature, .expected_revision = cut.revision });
        const found = functions.found orelse return error.SqlUndefinedFunction;
        const function = found.record;
        if (root.server.cfg.auth_enabled and function.definition.owner == .auth_disabled) return error.Forbidden;
        if (function.kind != .function or function.definition.result == null or !std.meta.eql(function.definition.result.?, pair.target) or function.definition.arguments.len == 0 or function.definition.arguments.len > 3) return error.SqlInvalidCastDefinition;
        const scope = functions.scopes[found.scope];
        if (!std.mem.eql(u8, scope.target.database, root.database)) return error.UnsupportedSqlShape;
        if (!std.meta.eql(function.definition.arguments[0].type, pair.source)) return error.SqlInvalidCastDefinition;
        if (function.definition.arguments.len >= 2 and !std.meta.eql(function.definition.arguments[1].type, casts.records.Type{ .scalar = .int32 })) return error.SqlInvalidCastDefinition;
        if (function.definition.arguments.len >= 3 and !std.meta.eql(function.definition.arguments[2].type, casts.records.Type{ .scalar = .boolean })) return error.SqlInvalidCastDefinition;
        if (function.definition.arguments.len == 1 and std.meta.eql(pair.source, pair.target)) return error.SqlInvalidCastDefinition;
        if (cut.record != null or catalog.builtin_casts.exists(pair)) return error.SqlDuplicateCast;
        const declared: casts.records.Definition = .{ .pair = pair, .context = intent.context, .function = .{ .database = root.database, .namespace = scope.target.namespace, .name = scope.target.table, .namespace_id = scope.namespace_id.?, .id = function.id }, .arguments = @intCast(function.definition.arguments.len), .owner = try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled) };
        try declared.validate();
        break :declared declared;
    } else null;
    if (!creating and cut.record == null) {
        if (!ddl.conditional) return error.SqlCastNotFound;
        if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .message = try std.fmt.allocPrint(a, "cast from type {s} to type {s} does not exist, skipping", .{ pair.source.sqlName(), pair.target.sqlName() }) });
        return .{};
    }
    var nonce: [16]u8 = undefined;
    try root.server.embedding_provider_runtime.io.randomSecure(&nonce);
    const request: casts.Mutation = .{ .admission = .{ .nonce = nonce, .head = cut.head, .command_fence = if (root.context.sql_command_fence) |fence| fence.* else null }, .database = root.database, .database_id = database_id, .incarnation = cut.incarnation, .operation = if (definition) |value| .{ .create = value } else .{ .drop = .{ .pair = pair, .expected = if (cut.record) |old| .{ .id = old.id, .generation = old.generation } else null, .if_exists = ddl.conditional } } };
    try request.validate();
    const observation = try casts.observation(a, request);
    var receipt = try recovery.newReceipt(allocators.result, a, observation, cut.revision);
    errdefer receipt.deinit(allocators.result);
    try root.context.ensureActive();
    const reply = root.server.source.systemCatalog(a, private, .{ .sql_cast_mutate = request }) catch null;
    var outcome = if (reply) |value| recovery.decodeOutcome(a, value, observation, cut.revision) else null;
    if (outcome == null) outcome = recovery.observe(root.server, root.identity.*, root.context, a, observation, cut.revision);
    if (outcome) |known| {
        _ = try known.result();
        common.complete(&receipt, known);
        return .{ .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}
