// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Scoped name resolution and journaled ICU collation declarations.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const records = catalog.collations;
const execution = @import("../execution/execution.zig");
const ordinary = @import("ordinary_catalog.zig");

pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, input: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if (input.cascade or root.active_transaction != null) return error.UnsupportedSqlExecution;
    const action: local.system_catalog_domain.Action = switch (input.action) {
        .create => .create,
        .rename => .rename,
        .drop => .drop,
        else => return error.UnsupportedSqlExecution,
    };
    try root.context.ensureActive();
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const targets = @import("../execution/routine_execution.zig").targets(root, a, input.name) catch |err| return if (err == error.SqlUndefinedFunction) error.Forbidden else err;
    const request: records.SnapshotRequest = .{ .database = root.database, .targets = targets, .expected_revision = root.revision };
    try request.validate();
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_collation_snapshot = request });
    const cut = try std.json.parseFromSliceLeaky(records.Snapshot, a, bytes, .{});
    try cut.validate(request);

    if (input.name.namespace != null and cut.scopes[0].namespace_id == null) return error.NamespaceNotFound;
    const scope = if (action == .create) find: {
        for (cut.scopes) |scope| if (scope.namespace_id != null) break :find scope;
        return error.NamespaceNotFound;
    } else if (cut.found) |found| cut.scopes[found.scope.?] else {
        const target = targets[0];
        try ordinary.authorize(root.server, root.identity.*, a, .{ .kind = .collation, .action = action, .database = target.database, .namespace = target.namespace, .name = target.table, .table_id = 0 });
        if (action != .drop or !input.conditional) return error.SqlCollationNotFound;
        if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .message = try std.fmt.allocPrint(a, "collation \"{s}\" does not exist, skipping", .{input.name.table}) });
        return .{};
    };
    const target = scope.target;
    try ordinary.authorize(root.server, root.identity.*, a, .{ .kind = .collation, .action = action, .database = target.database, .namespace = target.namespace, .name = target.table, .table_id = 0 });
    var mutation: local.system_catalog_domain.Mutation = .{ .kind = .collation, .action = action, .database = target.database, .namespace = target.namespace, .name = target.table };
    if (action == .create) {
        if (cut.found) |found| if (found.record.reference.namespace_id == scope.namespace_id.?) {
            if (!input.conditional) return error.SqlDuplicateCollation;
            if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .code = "42710", .message = try std.fmt.allocPrint(a, "collation \"{s}\" already exists, skipping", .{input.name.table}) });
            return .{};
        };
        const definition = input.collation_definition orelse return error.InvalidSqlSyntax;
        if (!definition.deterministic) return error.UnsupportedSqlCollation;
        var provider = try catalog.collation_native.Provider.load(null);
        defer provider.deinit();
        var prepared = try provider.prepare(a, definition.locale, true, null);
        defer prepared.deinit();
        const io = root.server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
        var origin: [16]u8 = undefined;
        try io.randomSecure(&origin);
        const create: records.Create = .{
            .expected_database_id = scope.database_id.?,
            .expected_namespace_id = scope.namespace_id.?,
            .origin_id = origin,
            .definition = .{
                .locale = definition.locale,
                .version = .{ .collation = prepared.version.collation, .uca = prepared.version.uca },
                .owner = try @import("view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled),
            },
        };
        try create.validate();
        mutation.source_json = try std.json.Stringify.valueAlloc(a, create, .{});
    } else {
        const found = cut.found.?;
        if (root.server.cfg.auth_enabled and found.record.definition.owner == .auth_disabled) return error.Forbidden;
        mutation.expected_collation = found.record.reference;
        if (action == .rename) mutation.new_name = input.new_name orelse return error.InvalidSqlSyntax;
    }
    var reply = ordinary.submit(root.server, root.identity.*, root.context, allocators, .{ .mutation = mutation, .collation_revision = cut.revision }) catch |err| return if (err == error.CatalogAlreadyExists) error.SqlDuplicateCollation else err;
    defer reply.deinit(allocators.result);
    return .{ .mutation_outcome = if (reply.unknown) null else .committed, .catalog_receipt = reply.takeReceipt() };
}
