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

//! Public SQL producer for native callable catalog transactions. User SQL
//! supplies definitions; live authorization supplies immutable owner identity.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ast = local.sql_ast;
const routines = @import("../system_catalog/routines.zig");
const execution = @import("sql_execution.zig");
const calls = @import("sql_routine_execution.zig");
const recovery = @import("sql_routine_recovery.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;

pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    if ((ddl.kind != .function and ddl.kind != .procedure) or (ddl.action != .create and ddl.action != .drop)) return error.UnsupportedSqlExecution;
    if (root.active_transaction != null or !root.server.source.vtable.supports_sql_routines) return error.UnsupportedSqlExecution;
    try root.context.ensureActive();
    var region: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const creating = ddl.action == .create;
    const routine_kind: routines.records.Kind = if (ddl.kind == .procedure) .procedure else .function;
    const scope = try calls.targets(root, a, ddl.name);
    const declared = if (creating) try catalog.routines.definition(a, ddl.routine orelse return error.InvalidSqlSyntax, try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled)) else null;
    const signature = if (declared) |definition| try definition.signatureAlloc(a) else if (ddl.routine_signature) |types| signature: {
        const arguments = try a.alloc(routines.records.Argument, types.len);
        for (types, arguments) |kind, *argument| argument.* = .{ .type = try catalog.routines.storageType(kind) };
        const probe: routines.records.Definition = .{ .arguments = arguments, .result = .{ .scalar = .text }, .body = "SELECT NULL", .owner = .auth_disabled };
        break :signature try probe.signatureAlloc(a);
    } else null;
    const cut = try calls.snapshot(root, a, .{ .targets = scope, .signature = signature });
    var chosen: ?usize = null;
    if (creating) {
        for (cut.scopes, 0..) |entry, i| if (entry.namespace_id != null) {
            chosen = i;
            break;
        };
    } else if (cut.found) |found| chosen = found.scope;
    const index = chosen orelse {
        if (!creating and ddl.conditional) {
            if (root.notice_sink) |sink| {
                const message = try std.fmt.allocPrint(a, "{s} {s}({s}) does not exist, skipping", .{ if (routine_kind == .procedure) @as([]const u8, "procedure") else "function", ddl.name.table, signature orelse "" });
                try sink.emit(sink.ptr, .{ .message = message });
            }
            return .{};
        }
        if (!creating) return if (routine_kind == .procedure) error.SqlUndefinedProcedure else error.SqlUndefinedFunction;
        return if (cut.scopes[0].database_exists) error.NamespaceNotFound else error.DatabaseNotFound;
    };
    const target = cut.scopes[index].target;
    try recovery.authorize(root.server, root.identity.*, a, target);
    const prior: ?routines.records.Record = if (cut.found) |found| if (found.scope == index) found.record else null else null;
    const expected: ?routines.Identity = if (prior) |record| .{ .id = record.id, .generation = record.generation } else null;
    if (declared) |definition| {
        if (prior != null and !ddl.routine.?.replace) return error.SqlDuplicateFunction;
        if (prior) |before| if (before.kind != routine_kind) return error.SqlWrongObjectType;
        if (prior) |before| {
            if (!std.meta.eql(before.definition.result, definition.result)) return error.SqlFunctionReturnTypeChanged;
            for (before.definition.arguments, definition.arguments) |old, new| if (old.name) |name| {
                if (new.name == null or !std.mem.eql(u8, name, new.name.?)) return error.SqlFunctionReturnTypeChanged;
            };
        }
        try catalog.routines.validateBody(a, .{ .kind = routine_kind, .id = if (prior) |record| record.id else cut.next_id, .generation = if (prior) |record| record.generation else 1, .definition = definition });
    }
    const io = root.server.embedding_provider_runtime.io;
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const request: routines.Mutation = .{ .admission = .{ .nonce = nonce, .head = cut.head }, .target = target, .namespace_id = cut.scopes[index].namespace_id.?, .incarnation = cut.incarnation, .operation = if (declared) |definition| .{ .publish = .{ .kind = routine_kind, .definition = definition, .replace = ddl.routine.?.replace, .expected = expected } } else .{ .drop = .{ .kind = routine_kind, .signature = signature orelse try prior.?.definition.signatureAlloc(a), .expected = expected, .if_exists = ddl.conditional, .cascade = ddl.cascade } } };
    try request.validate();
    const observation = try routines.observation(a, request);
    var receipt = try recovery.newReceipt(allocators.result, a, observation, cut.revision);
    errdefer receipt.deinit(allocators.result);
    var private = root.context;
    private.setting_admin = true;
    try root.context.ensureActive();
    const bytes = root.server.source.systemCatalog(a, private, .{ .sql_routine_mutate = request }) catch null;
    var outcome = if (bytes) |value| recovery.decodeOutcome(a, value, observation, cut.revision) else null;
    if (outcome == null) outcome = recovery.observe(root.server, root.identity.*, root.context, a, observation, cut.revision);
    if (outcome) |known| {
        _ = try known.result();
        common.complete(&receipt, known);
        return .{ .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}
