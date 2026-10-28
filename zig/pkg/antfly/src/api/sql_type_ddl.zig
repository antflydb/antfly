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

//! Public SQL producer for immutable namespace-owned type publication. Native
//! catalog transactions retain the outcome; ambiguity only permits observation.
const std = @import("std");
const local = @import("antfly_local_sources");
const types = @import("../system_catalog/named_types.zig");
const catalog = local.sql_catalog;
const execution = @import("sql_execution.zig");
const calls = @import("sql_routine_execution.zig");
const recovery = @import("sql_type_recovery.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;

pub fn snapshot(root: *execution.Adapter, a: A, request: types.SnapshotRequest) !types.Snapshot {
    try root.context.ensureActive();
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    if (!root.server.source.vtable.supports_sql_types) return error.UnsupportedSqlExecution;
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_type_snapshot = request });
    defer a.free(bytes);
    const cut = try std.json.parseFromSliceLeaky(types.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try cut.validate(a, request);
    return cut;
}
pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if (ddl.kind != .named_type or (ddl.action != .create and ddl.action != .alter_schema and ddl.action != .drop)) return error.UnsupportedSqlExecution;
    if (root.active_transaction != null or !root.server.source.vtable.supports_sql_types) return error.UnsupportedSqlExecution;
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const targets = calls.targets(root, a, ddl.name) catch |err| switch (err) {
        error.SqlUndefinedFunction => return error.SqlNamedTypeNotFound,
        else => return err,
    };
    const cut = try snapshot(root, a, .{ .targets = targets, .expected_revision = root.revision });
    root.revision = cut.revision;
    const creating = ddl.action == .create;
    const selected: ?usize = if (creating) scope: {
        for (cut.scopes, 0..) |candidate, i| if (candidate.namespace_id != null) break :scope i;
        break :scope null;
    } else if (cut.found) |found| found.scope else null;
    const index = selected orelse {
        if (ddl.action == .drop and ddl.conditional) {
            if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .message = try std.fmt.allocPrint(a, "type \"{s}\" does not exist, skipping", .{ddl.name.table}) });
            return .{};
        }
        if (!creating) return error.SqlNamedTypeNotFound;
        return if (cut.scopes[0].database_exists) error.NamespaceNotFound else error.DatabaseNotFound;
    };
    const scope = cut.scopes[index];
    try recovery.authorize(root.server, root.identity.*, a, scope.target);
    const previous: ?types.records.Record = if (cut.found) |found| if (found.scope == index) found.record else null else null;
    if (creating and previous != null) return error.SqlDuplicateType;
    const expected: ?types.Identity = if (previous) |record| .{ .id = record.identity.id, .generation = record.identity.generation } else null;
    const operation: @FieldType(types.Mutation, "operation") = switch (ddl.action) {
        .create => .{ .create_enum = .{ .labels = (ddl.enum_definition orelse return error.InvalidSqlSyntax).create, .owner = try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled) } },
        .alter_schema => add: {
            const change = (ddl.enum_definition orelse return error.InvalidSqlSyntax).add;
            const record = previous orelse return error.SqlNamedTypeNotFound;
            var view = try types.records.View.init(a, record.definition);
            defer view.deinit(a);
            if (view.find(change.label) != null) {
                if (!change.if_not_exists) return error.SqlDuplicateEnumLabel;
                if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .code = "42710", .message = try std.fmt.allocPrint(a, "enum label \"{s}\" already exists, skipping", .{change.label}) });
                return .{};
            }
            break :add .{ .add_enum = .{ .expected = expected.?, .label = change.label, .placement = change.placement, .if_not_exists = change.if_not_exists } };
        },
        .drop => .{ .drop = .{ .expected = expected, .if_exists = ddl.conditional, .cascade = ddl.cascade } },
        else => unreachable,
    };
    const Notice = catalog.notices.Notice;
    const conditional_notice: ?Notice = switch (operation) {
        .add_enum => |add| if (add.if_not_exists) .{ .code = "42710", .message = try std.fmt.allocPrint(a, "enum label \"{s}\" already exists, skipping", .{add.label}) } else null,
        .drop => |drop| if (drop.if_exists) .{ .message = try std.fmt.allocPrint(a, "type \"{s}\" does not exist, skipping", .{ddl.name.table}) } else null,
        else => null,
    };
    if (conditional_notice) |notice| if (root.notice_sink) |sink| if (sink.reserve) |reserve| try reserve(sink.ptr, 1, notice.message.len);
    var nonce: [16]u8 = undefined;
    try root.server.embedding_provider_runtime.io.randomSecure(&nonce);
    const request: types.Mutation = .{ .admission = .{ .nonce = nonce, .head = cut.head }, .target = scope.target, .namespace_id = scope.namespace_id.?, .incarnation = cut.incarnation, .operation = operation };
    try request.validate();
    const observation = try types.observation(a, request);
    var receipt = try recovery.newReceipt(allocators.result, a, observation, cut.revision);
    errdefer receipt.deinit(allocators.result);
    var private = root.context;
    private.setting_admin = true;
    try root.context.ensureActive();
    const reply = root.server.source.systemCatalog(a, private, .{ .sql_type_mutate = request }) catch null;
    var outcome = if (reply) |bytes| recovery.decodeOutcome(a, bytes, observation, cut.revision) else null;
    if (outcome == null) outcome = recovery.observe(root.server, root.identity.*, root.context, a, observation, cut.revision);
    if (outcome) |known| {
        const result = try known.result();
        if (result.no_op) if (conditional_notice) |notice| if (root.notice_sink) |sink| try sink.emit(sink.ptr, notice);
        common.complete(&receipt, known);
        return .{ .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}
