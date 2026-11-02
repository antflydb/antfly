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
    if ((ddl.kind != .function and ddl.kind != .procedure and ddl.kind != .operator and ddl.kind != .aggregate) or (ddl.action != .create and ddl.action != .drop and ddl.action != .comment)) return error.UnsupportedSqlExecution;
    if (root.active_transaction != null or !root.server.source.vtable.supports_sql_routines) return error.UnsupportedSqlExecution;
    try root.context.ensureActive();
    var region: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const creating = ddl.action == .create;
    const routine_kind: routines.records.Kind = if (ddl.kind == .procedure) .procedure else if (ddl.kind == .operator) .operator else if (ddl.kind == .aggregate) .aggregate else .function;
    const scope = try calls.targets(root, a, ddl.name);
    var declared: ?routines.records.Definition = if (creating and ddl.kind != .operator and ddl.kind != .aggregate) try catalog.routines.definition(a, ddl.routine orelse return error.InvalidSqlSyntax, try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled)) else null;
    const signature = if (declared) |definition| try definition.signatureAlloc(a) else if (ddl.routine_signature) |types| signature: {
        const arguments = try a.alloc(routines.records.Argument, types.len);
        for (types, arguments) |kind, *argument| argument.* = .{ .type = try catalog.routines.storageType(kind) };
        const probe: routines.records.Definition = .{ .arguments = arguments, .result = .{ .scalar = .text }, .body = "SELECT NULL", .owner = .auth_disabled };
        break :signature try probe.signatureAlloc(a);
    } else null;
    const cut = try calls.snapshot(root, a, .{ .targets = scope, .signature = signature, .kind = if (routine_kind == .operator) .operator else null });
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
                const message = if (routine_kind == .operator) try std.fmt.allocPrint(a, "operator {s} does not exist, skipping", .{ddl.name.table}) else try std.fmt.allocPrint(a, "{s} {s}({s}) does not exist, skipping", .{ if (routine_kind == .procedure) @as([]const u8, "procedure") else if (routine_kind == .operator) "operator" else if (routine_kind == .aggregate) "aggregate" else "function", ddl.name.table, signature orelse "" });
                try sink.emit(sink.ptr, .{ .message = message });
            }
            return .{};
        }
        if (!creating) return if (routine_kind == .procedure) error.SqlUndefinedProcedure else if (routine_kind == .operator) error.SqlOperatorNotFound else error.SqlUndefinedFunction;
        return if (cut.scopes[0].database_exists) error.NamespaceNotFound else error.DatabaseNotFound;
    };
    const target = cut.scopes[index].target;
    try recovery.authorize(root.server, root.identity.*, a, target);
    const prior: ?routines.records.Record = if (cut.found) |found| if (found.scope == index) found.record else null else null;
    const expected: ?routines.Identity = if (prior) |record| .{ .id = record.id, .generation = record.generation } else null;
    if (creating and routine_kind == .operator) {
        if (prior != null) return error.SqlDuplicateOperator;
        const arguments = try a.alloc(routines.records.Argument, ddl.routine_signature.?.len);
        for (ddl.routine_signature.?, arguments) |kind, *argument| argument.* = .{ .type = try catalog.routines.storageType(kind) };
        const types = try a.alloc(routines.records.Input, arguments.len);
        for (arguments, types) |argument, *input| input.* = .{ .type = argument.type };
        const function_targets = try calls.targets(root, a, ddl.operator_function orelse return error.InvalidSqlSyntax);
        const function_cut = try calls.snapshot(root, a, .{ .targets = function_targets, .arguments = types, .expected_revision = cut.revision });
        const function = function_cut.found orelse return error.SqlUndefinedFunction;
        if (root.server.cfg.auth_enabled and function.record.definition.owner == .auth_disabled) return error.Forbidden;
        if (function.record.kind != .function or function.record.definition.result == null or function.record.definition.arguments.len != arguments.len) return error.SqlUndefinedFunction;
        for (arguments, function.record.definition.arguments) |requested, actual| if (!std.meta.eql(requested.type, actual.type)) return error.SqlUndefinedFunction;
        const function_scope = function_cut.scopes[function.scope];
        if (!std.mem.eql(u8, function_scope.target.database, target.database)) return error.UnsupportedSqlShape;
        declared = .{ .arguments = arguments, .result = function.record.definition.result, .body = "", .owner = try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled), .function_reference = .{ .database = function_scope.target.database, .namespace = function_scope.target.namespace, .name = function_scope.target.table, .namespace_id = function_scope.namespace_id.?, .id = function.record.id } };
    }
    if (creating and ddl.kind == .aggregate) {
        const intent = ddl.aggregate_definition orelse return error.InvalidSqlSyntax;
        const arguments = try a.alloc(routines.records.Argument, ddl.routine_signature.?.len);
        for (ddl.routine_signature.?, arguments) |kind, *argument| argument.* = .{ .type = try catalog.routines.storageType(kind) };
        const state = try catalog.routines.storageType(intent.state);
        const transition_types = try a.alloc(routines.records.Type, arguments.len + 1);
        transition_types[0] = state;
        for (arguments, transition_types[1..]) |argument, *kind| kind.* = argument.type;
        const transition = try resolveSupport(root, a, intent.transition, transition_types, target.database, cut.revision);
        if (!std.meta.eql(transition.record.definition.result.?, state)) return error.SqlInvalidFunctionDefinition;
        if (transition.record.definition.strict and intent.initial == null and !std.meta.eql(state, arguments[0].type)) return error.SqlInvalidFunctionDefinition;
        const final = if (intent.final) |name| try resolveSupport(root, a, name, &.{state}, target.database, cut.revision) else null;
        declared = .{ .arguments = arguments, .result = if (final) |function| function.record.definition.result else state, .body = "", .owner = try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled), .function_reference = transition.reference, .aggregate = .{ .state = state, .initial = intent.initial, .final = if (final) |function| function.reference else null } };
    }
    if (declared) |definition| {
        if (prior != null and !(if (ddl.routine) |intent| intent.replace else if (ddl.aggregate_definition) |intent| intent.replace else false)) return error.SqlDuplicateFunction;
        if (prior) |before| if (before.kind != routine_kind) return error.SqlWrongObjectType;
        if (prior) |before| {
            if (before.definition.returns_trigger != definition.returns_trigger or !std.meta.eql(before.definition.result, definition.result)) return error.SqlFunctionReturnTypeChanged;
            for (before.definition.arguments, definition.arguments) |old, new| if (old.name) |name| {
                if (new.name == null or !std.mem.eql(u8, name, new.name.?)) return error.SqlFunctionReturnTypeChanged;
            };
        }
        try catalog.routines.validateBody(a, .{ .kind = routine_kind, .id = if (prior) |record| record.id else cut.next_id, .generation = if (prior) |record| record.generation else 1, .definition = definition });
    }
    if (prior) |before| if (before.kind != routine_kind) return error.SqlWrongObjectType;
    if (ddl.action == .drop and ddl.cascade) if (root.notice_sink) |sink| if (sink.reserve) |reserve| try reserve(sink.ptr, 1, 1024);
    const io = root.server.embedding_provider_runtime.io;
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const request: routines.Mutation = .{ .admission = .{ .nonce = nonce, .head = cut.head }, .target = target, .namespace_id = cut.scopes[index].namespace_id.?, .incarnation = cut.incarnation, .operation = if (declared) |definition| .{ .publish = .{ .kind = routine_kind, .definition = definition, .replace = if (ddl.routine) |intent| intent.replace else if (ddl.aggregate_definition) |intent| intent.replace else false, .expected = expected } } else if (ddl.action == .comment) .{ .comment = .{ .kind = routine_kind, .signature = signature orelse try prior.?.definition.signatureAlloc(a), .expected = expected.?, .description = ddl.comment_value } } else .{ .drop = .{ .kind = routine_kind, .signature = signature orelse try prior.?.definition.signatureAlloc(a), .expected = expected, .if_exists = ddl.conditional, .cascade = ddl.cascade } } };
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
        const committed = try known.result();
        common.complete(&receipt, known);
        if (committed.cascade_notice) |notice| if (root.notice_sink) |sink| {
            // Durability remains visible even if an optional transport notice
            // cannot be delivered. Standard transports reserved before submit.
            sink.emit(sink.ptr, .{ .message = notice }) catch return .{ .catalog_receipt = receipt };
        };
        return .{ .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}

const Support = struct { record: routines.records.Record, reference: routines.records.FunctionReference };
fn resolveSupport(root: *execution.Adapter, a: A, name: ast.Name, types: []const routines.records.Type, database: []const u8, revision: u64) !Support {
    const inputs = try a.alloc(routines.records.Input, types.len);
    for (types, inputs) |kind, *input| input.* = .{ .type = kind };
    const scope = try calls.targets(root, a, name);
    const cut = try calls.snapshot(root, a, .{ .targets = scope, .arguments = inputs, .expected_revision = revision });
    const found = cut.found orelse return error.SqlUndefinedFunction;
    if (found.record.kind != .function or found.record.definition.result == null or found.record.definition.arguments.len != types.len) return error.SqlUndefinedFunction;
    for (found.record.definition.arguments, types) |actual, expected| if (!std.meta.eql(actual.type, expected)) return error.SqlUndefinedFunction;
    if (root.server.cfg.auth_enabled and found.record.definition.owner == .auth_disabled) return error.Forbidden;
    const target = cut.scopes[found.scope];
    if (!std.mem.eql(u8, target.target.database, database)) return error.UnsupportedSqlShape;
    return .{ .record = found.record, .reference = .{ .database = target.target.database, .namespace = target.target.namespace, .name = target.target.table, .namespace_id = target.namespace_id.?, .id = found.record.id } };
}
