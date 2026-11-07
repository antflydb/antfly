// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Table-trigger declarations publish references through the ordinary captured
//! schema transaction. Routine bodies are never copied into table schemas.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ast = local.sql_ast;
const domain = local.system_catalog_domain;
const calls = @import("sql_routine_execution.zig");
const execution = @import("sql_execution.zig");
const schemas = @import("sql_catalog.zig");
const topology = @import("../metadata/topology_protocol.zig");

pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    const syntax = ddl.trigger orelse return error.InvalidSqlSyntax;
    try syntax.validate();
    if (syntax.operation == .create) try @import("sql_trigger_authority.zig").requireConfiguration(root.server.cfg);
    if (root.active_transaction != null) return error.UnsupportedSqlExecution;
    // Do not advertise writable trigger tables until every receiver installs
    // the same preparation/dispatch contract and rejects predecessor nodes.
    if (topology.current_version < topology.sql_table_triggers_version) return error.TableTopologyProtocolUpgradeRequired;
    var arena: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const requested: domain.Target = .{ .database = syntax.table.database orelse root.database, .namespace = syntax.table.namespace orelse root.namespace, .table = syntax.table.table };
    const captured = (try schemas.captureTableAdmission(root.server, root.identity.*, root.context, a, requested, syntax.table.namespace != null or syntax.table.database != null, root.ddl_search_path)) orelse return error.TableNotFound;
    const definition = captured.table.query_definition orelse return error.InvalidSqlBackendResponse;
    const schema = try std.json.parseFromSliceLeaky(std.json.Value, a, definition.schema_json, .{});
    if (schema != .object) return error.InvalidSqlBackendResponse;
    var before: catalog.triggers.State = .{ .logical_relation_id = captured.table.logical_relation_id, .metadata_incarnation = captured.guard.incarnation, .table_id = captured.table.table_id, .generation = 1 };
    if (schema.object.get("sql_triggers")) |value| {
        before = (try local.schema_mod.table_triggers.parseValue(a, value)).value;
        if (before.table_id != captured.table.table_id or before.effectiveLogicalRelationId() != captured.table.effectiveLogicalRelationId()) return error.InvalidSqlTriggerDefinition;
    }
    const trigger_operation: @FieldType(@import("../system_catalog/server_call.zig").RelationTriggerMutation, "operation") = switch (syntax.operation) {
        .drop => |drop| dropped: {
            if (before.find(syntax.name) == null and drop.if_exists) {
                if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .message = try std.fmt.allocPrint(a, "trigger {s} for relation {s} does not exist, skipping", .{ syntax.name, captured.guard.logical_table }) });
                return .{};
            }
            break :dropped .{ .drop = .{ .name = syntax.name, .if_exists = drop.if_exists } };
        },
        .create => |create| created: {
            const targets = try calls.targets(root, a, create.function);
            const cut = try calls.snapshot(root, a, .{ .targets = targets, .arguments = &.{}, .kind = .function });
            const found = cut.found orelse return error.SqlUndefinedFunction;
            const scope = cut.scopes[found.scope];
            if (!std.mem.eql(u8, scope.target.database, captured.guard.target.database)) return error.UnsupportedSqlShape;
            try @import("sql_trigger_function_admission.zig").authorize(a, root.server.cfg.auth_enabled, root.identity.*, found.record, scope.target);
            break :created .{ .create = .{
                .name = syntax.name,
                .timing = create.timing,
                .events = create.events,
                .level = create.level,
                .function = .{ .database = scope.target.database, .namespace = scope.target.namespace, .name = scope.target.table, .namespace_id = scope.namespace_id.?, .id = found.record.id },
                .arguments = create.arguments,
                .replace = create.replace,
                .update_columns = create.update_columns,
                .predicate = create.predicate_sql,
            } };
        },
    };
    // Same durable completion/nonce and schema-CAS lane used by column/index
    // evolution. A lost reply returns its receipt; no statement replay occurs.
    return schemas.publishCapturedSchema(root.server, root.identity.*, root.context, allocators, captured, trigger_operation);
}
