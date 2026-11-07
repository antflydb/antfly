// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native preparation of declarative trigger operations. The metadata owner
//! derives identity high water, schema generations and attribute references;
//! private ingress cannot supply an already fabricated attachment directory.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const calls = @import("../system_catalog/server_call.zig");
const routines = @import("../system_catalog/routines.zig");
const tables = @import("../api/tables.zig");
const references = @import("trigger_catalog.zig");
const Table = local.common_topology_records.TableRecord;
const A = std.mem.Allocator;

const PreparationPhase = enum { request, source, prior, routine, attributes, predicate, declaration, schema_update };
var diagnosed_preparation = std.atomic.Value(bool).init(false);
pub fn prepare(a: A, store: anytype, group: u64, request: calls.RelationTriggerMutation, before: Table) !Table {
    var phase: PreparationPhase = .request;
    return preparePhased(a, store, group, request, before, &phase) catch |failure| {
        if (@import("builtin").mode == .debug and failure == error.InvalidSqlTriggerDefinition and !diagnosed_preparation.swap(true, .monotonic)) std.debug.print("trigger declaration rejected phase={s} class={s} table={d}\n", .{ @tagName(phase), @errorName(failure), before.table_id });
        return failure;
    };
}
fn preparePhased(a: A, store: anytype, group: u64, request: calls.RelationTriggerMutation, before: Table, phase: *PreparationPhase) !Table {
    try request.validate();
    if (before.table_id != request.guard.owner.table_id) return error.CatalogGenerationChanged;
    phase.* = .source;
    const resolved = (try store.resolveSystemCatalogIdentity(a, group, .{ .database = request.guard.target.database, .namespace = request.guard.target.namespace, .table = request.guard.logical_table })) orelse return error.CatalogGenerationChanged;
    defer resolved.deinit(a);
    if (resolved.table_id != before.table_id or !std.mem.eql(u8, resolved.name, before.name)) return error.CatalogGenerationChanged;
    phase.* = .prior;
    var parsed_state = try references.schemaState(a, before);
    defer if (parsed_state) |*state| state.deinit();
    const oid = resolved.logical_relation_id orelse proposed: {
        // This scalar is a proposal, not an allocation or source authority.
        // The original guarded schema/owner is retained, and native apply
        // rejoins its actual Resource, next_id and directory before writing.
        break :proposed (try store.systemCatalogMeta(a, group)).next_id;
    };
    try local.system_catalog_domain.requireRelationOid(oid);
    const old: catalog.triggers.State = if (parsed_state) |state| state.value else .{ .logical_relation_id = if (oid == before.table_id) null else oid, .metadata_incarnation = request.guard.incarnation, .table_id = before.table_id, .generation = 1 };
    if (old.effectiveLogicalRelationId() != oid) return error.CatalogGenerationChanged;
    if (!std.mem.eql(u8, &old.metadata_incarnation, &request.guard.incarnation)) return error.CatalogGenerationChanged;
    const next = switch (request.operation) {
        .drop => |drop| block: {
            // Ingress already handled a missing IF EXISTS under its own cut.
            // A private stale operation must not mint an unfulfillable owner
            // completion for an unchanged schema epoch.
            if (old.find(drop.name) == null) return error.CatalogGenerationChanged;
            break :block try catalog.triggers.drop(a, old, drop.name, false);
        },
        .create => |create| block: {
            phase.* = .routine;
            const bytes = try store.sqlRoutineSnapshot(a, group, routines.SnapshotRequest{ .targets = &.{}, .reference = create.function, .kind = .function }, local.api_operation.RequestContext{});
            const snapshot = try std.json.parseFromSliceLeaky(routines.Snapshot, a, bytes, .{});
            try snapshot.validate(.{ .targets = &.{}, .reference = create.function, .kind = .function });
            if (!std.mem.eql(u8, &snapshot.incarnation, &request.guard.incarnation)) return error.CatalogGenerationChanged;
            const function = snapshot.found orelse return error.SqlUndefinedFunction;
            if (!function.record.definition.returns_trigger or function.record.definition.arguments.len != 0) return error.InvalidSqlTriggerDefinition;
            const scope = snapshot.scopes[function.scope];
            if (!std.mem.eql(u8, scope.target.database, request.guard.target.database)) return error.InvalidSqlTriggerDefinition;
            phase.* = .attributes;
            const update_ids = try a.alloc(u64, create.update_columns.len);
            if (create.update_columns.len != 0) {
                var columns = try store.getSqlColumnIdentities(a, group, request.guard);
                defer columns.deinit();
                for (create.update_columns, update_ids) |name, *id| id.* = for (columns.value.columns) |column| {
                    if (std.mem.eql(u8, name, column.name)) break column.id;
                } else return error.UndefinedColumn;
            }
            phase.* = .predicate;
            if (create.predicate) |sql| {
                var parsed = try local.schema_mod.parseValidatedTableSchema(a, before.schema_json);
                defer parsed.deinit(a);
                const native = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
                defer local.storage_schema.freeSchema(a, native);
                const columns = if (native.storage_mode == .relational)
                    try local.sql_document_row.deriveRelationalColumnsLeaky(a, a, parsed, native)
                else
                    try local.sql_document_row.deriveColumns(a, parsed);
                const layout = try @import("antfly_local_sources").sql_catalog.trigger_runtime.FrameLayout.init(a, columns, oid, request.guard.logical_table, request.guard.target.namespace);
                var expression = try local.sql_compiler.compileScalar(a, sql, .{});
                defer expression.deinit();
                const references_new = @import("antfly_local_sources").sql_catalog.trigger_runtime.referencesImage(expression.expression, "new");
                const references_old = @import("antfly_local_sources").sql_catalog.trigger_runtime.referencesImage(expression.expression, "old");
                if ((create.level == .statement and (references_new or references_old)) or (create.events.insert and references_old) or (create.events.delete and references_new)) return error.InvalidSqlTriggerDefinition;
                _ = local.sql_scalar.bindTypedExpectedWithSettings(a, expression.expression, if (create.level == .statement) &.{} else layout.columns[0 .. layout.width * 2], &.{}, .{ .kind = .boolean }, .{}, null) catch |err| return if (err == error.UnknownColumn) error.UndefinedColumn else err;
            }
            phase.* = .declaration;
            break :block try catalog.triggers.prepare(a, old, .{ .name = create.name, .timing = create.timing, .events = create.events, .level = create.level, .function = .{ .id = function.record.id, .namespace_id = scope.namespace_id.?, .database = scope.target.database, .namespace = scope.target.namespace, .name = scope.target.table }, .arguments = create.arguments, .update_columns = update_ids, .predicate = create.predicate }, create.replace);
        },
    };
    var schema = try std.json.parseFromSliceLeaky(std.json.Value, a, if (before.schema_json.len == 0) "{}" else before.schema_json, .{});
    const encoded = try next.encode(a);
    try schema.object.put(a, "sql_triggers", try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{}));
    phase.* = .schema_update;
    return tables.applyTriggerSchemaMutationRecord(a, &before, try std.json.Stringify.valueAlloc(a, schema, .{}));
}
