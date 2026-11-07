// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Capture the exact executable declarations of a table-trigger statement.
//! Metadata lookup belongs here, before serialized native row application.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const manifest = catalog.trigger_manifest;
const routines = @import("../system_catalog/routines.zig");
const routine_execution = @import("sql_routine_execution.zig");
const execution = @import("sql_execution.zig");
const A = std.mem.Allocator;

const Capture = struct {
    a: A,
    incarnation: [16]u8,
    revision: ?u64 = null,
    declarations: std.ArrayList(manifest.Routine) = .empty,
    lookups: std.ArrayList(manifest.Lookup) = .empty,

    fn observe(raw: *anyopaque, cut: routines.Snapshot, found: routines.Snapshot.Found, lookup_name: ?[]const u8) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        if (!std.mem.eql(u8, &cut.incarnation, &self.incarnation)) return error.CatalogGenerationChanged;
        if (self.revision) |revision| {
            if (cut.revision != revision) return error.CatalogGenerationChanged;
        } else self.revision = cut.revision;
        const scope = cut.scopes[found.scope];
        const reference: routines.records.RoutineReference = .{ .id = found.record.id, .namespace_id = scope.namespace_id orelse return error.CatalogGenerationChanged, .database = scope.target.database, .namespace = scope.target.namespace, .name = scope.target.table };
        for (self.declarations.items) |entry| {
            if (entry.record.id == found.record.id) {
                if (entry.record.generation != found.record.generation) return error.CatalogGenerationChanged;
                break;
            }
        } else {
            if (self.declarations.items.len == manifest.max_routines) return error.SqlProgramLimitExceeded;
            try self.declarations.append(self.a, .{ .reference = reference, .record = found.record });
        }
        if (lookup_name) |name| {
            for (self.lookups.items) |entry| {
                if (entry.routine_id == found.record.id and std.mem.eql(u8, entry.name, name)) return;
            }
            if (self.lookups.items.len == manifest.max_lookups) return error.SqlProgramLimitExceeded;
            try self.lookups.append(self.a, .{ .name = name, .routine_id = found.record.id, .namespace_id = reference.namespace_id });
        }
    }
};

/// Return a fully owned canonical manifest; no resolver, settings owner, or
/// metadata response storage is retained from this preparation call.
pub fn capture(root: *execution.Adapter, a: A, table: catalog.Table, validator: local.schema_mod.CompiledTableValidator, clock: manifest.Clock) !manifest.Manifest {
    const state = if (validator.schema.sql_triggers) |stored| stored.value else return error.SqlTriggerCoordinationRequired;
    if (state.table_id != table.id or state.effectiveLogicalRelationId() != table.effectiveLogicalRelationId() or validator.schema.version != table.schema_version) return error.CatalogGenerationChanged;
    const digest = validator.source_digest orelse return error.CatalogGenerationChanged;
    if (!std.mem.eql(u8, &digest, &local.storage_db_relational_schema_owner_contract.hash(table.schema_json))) return error.CatalogGenerationChanged;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const temporary = arena.allocator();
    var captured: Capture = .{ .a = temporary, .incarnation = state.metadata_incarnation, .revision = root.revision };
    const callables = try routine_execution.Owner.create(root);
    defer callables.deinit();
    callables.capture_observer = .{ .ptr = &captured, .observe = Capture.observe };
    const environment = try callables.triggerEnvironment();
    const frame_layout = try catalog.trigger_runtime.FrameLayout.init(temporary, table.columns, table.effectiveLogicalRelationId(), if (table.scope) |scope| scope.name else table.physical_name, if (table.scope) |scope| scope.namespace else "public");
    const frame = try frame_layout.frame(temporary, null, null);
    for (state.entries) |entry| {
        const cut = try routine_execution.snapshot(root, temporary, .{ .targets = &.{}, .immutable_id = .{ .id = entry.function.id, .incarnation = state.metadata_incarnation }, .kind = .function, .expected_revision = captured.revision });
        const found = cut.found orelse return error.CatalogGenerationChanged;
        if (!found.record.definition.returns_trigger or found.record.definition.arguments.len != 0) return error.InvalidSqlTriggerDefinition;
        try Capture.observe(&captured, cut, found, null);
        root.revision = cut.revision;
        const body = try catalog.routines.procedures.prepare(temporary, found.record, environment.resolver, null, environment.settings);
        if (entry.predicate) |predicate| {
            const syntax = try local.sql_compiler.compileScalar(temporary, predicate, .{});
            _ = try local.sql_scalar.bindTypedExpectedWithSettings(temporary, syntax.expression, if (entry.level == .statement) &.{} else frame.frame.columns[0 .. frame_layout.width * 2], &.{}, .{ .kind = .boolean }, .{ .invocation = body.invocation }, environment.settings);
        }
        try catalog.routines.procedures.captureTriggerFunctions(body, frame.frame);
    }
    std.mem.sort(manifest.Routine, captured.declarations.items, {}, struct {
        fn less(_: void, x: manifest.Routine, y: manifest.Routine) bool {
            return x.record.id < y.record.id;
        }
    }.less);
    std.mem.sort(manifest.Lookup, captured.lookups.items, {}, struct {
        fn less(_: void, x: manifest.Lookup, y: manifest.Lookup) bool {
            const order = std.mem.order(u8, x.name, y.name);
            return order == .lt or (order == .eq and x.routine_id < y.routine_id);
        }
    }.less);
    const result: manifest.Manifest = .{ .metadata_incarnation = state.metadata_incarnation, .catalog_revision = captured.revision orelse return error.CatalogGenerationChanged, .table = .{ .id = table.id, .schema_version = table.schema_version, .schema_digest = digest, .attachment_generation = state.generation, .attachment_digest = try manifest.attachmentDigest(state) }, .attachments = state, .routines = captured.declarations.items, .lookups = captured.lookups.items, .clock = clock };
    const encoded = try result.encode(temporary);
    return std.json.parseFromSliceLeaky(manifest.Manifest, a, encoded, .{ .allocate = .alloc_always });
}
