// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared coordinator-side row preparation. Capture/read/authorization and
//! commit are caller-owned; this stage only transforms unpublished candidates.
//! SQL, document ingress and FK actions use the same ordering: defaults,
//! BEFORE, generated/row checks, caller's final RLS/integrity validation, AFTER,
//! then atomic commit. Native replay must verify the separate handoff receipt.
const std = @import("std");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const schema = @import("../schema/mod.zig");
const registry = @import("../storage/db/schema_registry.zig");
const runtime = @import("trigger_runtime.zig");
const manifest = @import("trigger_manifest.zig");
const shape = @import("trigger_definition.zig");
const document = @import("document_row.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const Options = struct {
    execution: runtime.Limits = .{},
    expressions: ?@import("../schema/relational_expression.zig").PreparationRuntime = null,
    settings: ?*const @import("setting_catalog.zig").View = null,
};
pub const Input = struct {
    key: []const u8,
    event: shape.Event,
    old: ?*const catalog.Row = null,
    /// Owned mutable DOM, already parsed once by the ordinary ingress pipeline.
    after: ?*Json = null,
    json_null_fields: []const []const u8 = &.{},
    update_columns: []const u64 = &.{},
    /// Existing SQL normalization may already have filled defaults. This flag
    /// never grants trigger authority; the actual trigger still executes.
    candidate_prepared: bool = false,
    domain_admission: ?catalog.domain_rows.Admission = null,
};
pub const Row = struct {
    key: []const u8,
    event: shape.Event,
    old: ?*const catalog.Row,
    new: ?*const catalog.Row,
    value: ?Json,
    json_null_fields: []const []const u8,
    update_columns: []const u64,
    skipped: bool = false,
    notices: []const @import("procedure.zig").Notice = &.{},
    after_executed: bool = false,
    after_entries: std.bit_set.Static(catalog.triggers.max_entries) = .empty,
};

pub fn captureView(a: A, schema_json: []const u8) !registry.SchemaView {
    var validator = try schema.CompiledTableValidator.init(a, schema_json);
    errdefer validator.deinit(a);
    const native = try schema.deriveRuntimeTableSchema(a, validator.schema);
    errdefer @import("../storage/schema.zig").freeSchema(a, native);
    return .{ .epoch = try registry.Epoch.createOwnedValidated(a, native, validator) };
}
pub fn deriveColumns(a: A, view: registry.SchemaView) ![]const catalog.Column {
    const validator = view.validator() orelse return error.InvalidSqlBackendResponse;
    return if (view.storageMode() == .relational) try document.deriveRelationalColumnsLeaky(a, a, validator.schema, view.tableSchema().*) else try document.deriveColumns(a, validator.schema);
}

pub const Owner = struct {
    a: A,
    view: registry.SchemaView,
    validator: schema.CompiledTableValidator,
    columns: []const catalog.Column,
    layout: *const runtime.FrameLayout,
    document_projection: ?document.Projection = null,
    execution: ?*runtime.Statement,

    pub fn create(a: A, view: registry.SchemaView, binding: catalog.Table, captured: ?manifest.Manifest, options: Options) !*Owner {
        const actual = view.triggerState();
        const validator = view.validator() orelse return error.InvalidSqlBackendResponse;
        if (binding.schema_json.len == 0 or binding.schema_version != view.version()) return error.CatalogGenerationChanged;
        const source_digest = validator.source_digest orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &source_digest, &@import("../storage/db/relational_schema_owner_contract.zig").hash(binding.schema_json))) return error.CatalogGenerationChanged;
        if (actual) |state| if (state.table_id != binding.id or state.effectiveLogicalRelationId() != binding.effectiveLogicalRelationId()) return error.CatalogGenerationChanged;
        if (captured) |value| {
            if (value.absence) |absent| {
                try absent.requireView(view);
                if (absent.table_id != binding.id) return error.CatalogGenerationChanged;
                try value.requireFor(try absent.table(), absent.incarnation);
            } else {
                const state = actual orelse return error.SqlTriggerCoordinationRequired;
                if (state.table_id != binding.id or state.effectiveLogicalRelationId() != binding.effectiveLogicalRelationId()) return error.CatalogGenerationChanged;
                const expected: manifest.Table = .{ .id = binding.id, .schema_version = view.version(), .schema_digest = source_digest, .attachment_generation = state.generation, .attachment_digest = try manifest.attachmentDigest(state.*) };
                try value.requireFor(expected, state.metadata_incarnation);
            }
        } else if (actual) |state| if (state.entries.len != 0) return error.SqlTriggerCoordinationRequired;
        const columns = try deriveColumns(a, view);
        const layout = try runtime.FrameLayout.init(a, columns, binding.effectiveLogicalRelationId(), if (binding.scope) |scope| scope.name else binding.physical_name, if (binding.scope) |scope| scope.namespace else "public");
        const projection: ?document.Projection = if (binding.storage_mode == .document) projected: {
            // BEFORE may fill a missing required column. Preserve its SQL
            // domain while leaving final NOT NULL admission to selected-row
            // validation, as for the relational candidate path.
            const relaxed = try a.dupe(catalog.Column, columns);
            for (relaxed) |*column| column.nullable = true;
            var table = binding;
            table.columns = relaxed;
            table.column_index = null;
            break :projected try document.Projection.init(a, table, layout.row.names);
        } else null;
        errdefer if (projection) |value| value.deinit(a);
        const execution: ?*runtime.Statement = if (captured) |value| try runtime.Statement.init(a, value, options.settings, options.execution) else null;
        const owner = try a.create(Owner);
        owner.* = .{
            .a = a,
            .view = view.clone(),
            .validator = if (options.expressions) |expressions| validator.forPreparation(expressions) else validator,
            .columns = columns,
            .layout = layout,
            .document_projection = projection,
            .execution = execution,
        };
        return owner;
    }
    /// Reuse the ordinary statement/receipt owner for a genuine absent-state
    /// graph preparation. There are no captured routines and nothing to refire.
    pub fn enableGraphProof(self: *Owner, admission: @import("../storage/db/prepared_graph_effects.zig").Admission, revision: u64, clock: manifest.Clock, options: Options) !void {
        if (self.execution != null) return;
        const digest = self.view.validator().?.source_digest orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &digest, &admission.schema_digest)) return error.CatalogGenerationChanged;
        const captured: manifest.Manifest = if (self.view.triggerState()) |state| present: {
            if (state.entries.len != 0 or state.table_id != admission.table_id or !std.mem.eql(u8, &state.metadata_incarnation, &admission.incarnation)) return error.CatalogGenerationChanged;
            break :present .{ .metadata_incarnation = state.metadata_incarnation, .catalog_revision = revision, .table = .{ .id = state.table_id, .schema_version = self.view.version(), .schema_digest = digest, .attachment_generation = state.generation, .attachment_digest = try manifest.attachmentDigest(state.*) }, .attachments = state.*, .routines = &.{}, .clock = clock };
        } else absent: {
            const proof = try manifest.Absence.fromAdmission(admission, self.view.version());
            try proof.requireView(self.view);
            break :absent .{ .format = 2, .metadata_incarnation = proof.incarnation, .catalog_revision = revision, .table = try proof.table(), .absence = proof, .routines = &.{}, .clock = clock };
        };
        self.execution = try runtime.Statement.init(self.a, captured, options.settings, options.execution);
    }

    pub fn deinit(self: *Owner) void {
        if (self.document_projection) |projection| projection.deinit(self.a);
        self.view.release();
    }

    pub fn statement(self: *Owner, timing: shape.Timing, event: shape.Event, update_columns: []const u64) ![]const @import("procedure.zig").Notice {
        const execution = self.execution orelse return &.{};
        const frame = try self.layout.frame(self.a, null, null);
        return (try execution.fire(.{ .timing = timing, .level = .statement, .event = event, .updated_columns = update_columns, .frame = &frame.frame, .owned = frame })).notices;
    }

    pub fn sourceTypedRow(self: *Owner, source: catalog.Row) !*const catalog.Row {
        const values = try self.a.alloc(scalar.Datum, self.columns.len);
        for (self.columns, values) |column, *value| value.* = try source.cell(column.path);
        const row = try self.a.create(catalog.Row);
        row.* = try catalog.Row.fromDatums(self.a, source.id, self.layout.row, values);
        row.version = source.version;
        row.expected_content_digest = source.expected_content_digest;
        row.document_version = source.document_version;
        return row;
    }

    pub fn sourceRow(self: *Owner, key: []const u8, value: Json, json_null_fields: []const []const u8, version: u64, digest: ?[32]u8) !*const catalog.Row {
        const row = @constCast(try self.typedRow(key, value, json_null_fields));
        row.version = version;
        row.expected_content_digest = digest;
        return row;
    }

    fn typedRow(self: *Owner, key: []const u8, value: Json, json_null_fields: []const []const u8) !*const catalog.Row {
        if (value != .object) return error.InvalidSqlBackendResponse;
        if (self.document_projection) |projection| {
            // Document JSON already encodes NULL provenance: absence is SQL
            // NULL and a present JSON-column null is a JSON datum. Reuse the
            // normal SQL/document decoder, not relational transport bitmaps.
            const row = try self.a.create(catalog.Row);
            row.* = try projection.projectValue(self.a, key, 0, value);
            return row;
        }
        const flags = try self.a.alloc(bool, value.object.count());
        for (value.object.values(), flags) |field, *flag| flag.* = field == .null;
        for (json_null_fields) |name| {
            const index = value.object.getIndex(name) orelse return error.InvalidSqlBackendResponse;
            if (value.object.values()[index] != .null) return error.InvalidSqlBackendResponse;
            flags[index] = false;
        }
        const source: catalog.Row = .{ .id = key, .version = 0, .value = value, .sql_nulls = flags };
        const cells = try self.a.alloc(scalar.Datum, self.columns.len);
        for (self.columns, cells) |column, *cell| {
            const physical = if (column.document_path.len == 0) column.path else column.document_path[0];
            var raw = try source.cell(physical);
            if (column.document_path.len > 1) {
                const nested = @import("document_path.zig").get(raw.value, column.document_path[1..]);
                raw = .{ .value = nested orelse .null, .sql_null = nested == null or (nested.? == .null and column.type != .json) };
            }
            cell.* = try document.declaredCell(self.a, column, raw);
        }
        const result = try self.a.create(catalog.Row);
        result.* = try catalog.Row.fromDatums(self.a, key, self.layout.row, cells);
        return result;
    }

    fn writeCandidate(self: *Owner, value: *Json, row: catalog.Row) ![]const []const u8 {
        if (value.* != .object) return error.InvalidSqlBackendResponse;
        var nulls: std.StringHashMapUnmanaged(void) = .empty;
        for (self.columns) |column| {
            const cell = try row.cell(column.name);
            var assignable = column;
            // NOT NULL is checked after all BEFORE transforms, alongside the
            // generated row. Domain casts were already checked by the VM.
            assignable.nullable = true;
            const stored = try @import("runtime.zig").encodeStorageDatumTyped(self.a, cell, assignable, column.element_type, 16 * 1024 * 1024);
            try column.assignStorageValue(self.a, &value.object, stored, cell.sql_null);
            if (column.type == .json and !cell.sql_null and stored == .null and column.document_path.len <= 1) {
                const physical = if (column.document_path.len == 0) column.path else column.document_path[0];
                try nulls.put(self.a, physical, {});
            }
        }
        const names = try self.a.alloc([]const u8, nulls.count());
        var keys = nulls.keyIterator();
        var ordinal: usize = 0;
        while (keys.next()) |name| : (ordinal += 1) names[ordinal] = name.*;
        std.mem.sort([]const u8, names, {}, struct {
            fn less(_: void, x: []const u8, y: []const u8) bool {
                return std.mem.order(u8, x, y) == .lt;
            }
        }.less);
        return names;
    }

    pub fn before(self: *Owner, input: Input) !Row {
        if (input.event == .truncate or (input.event == .insert and input.old != null) or (input.event != .insert and input.old == null) or ((input.event == .delete) != (input.after == null))) return error.InvalidSqlTriggerManifest;
        if (input.old) |old| if (!std.mem.eql(u8, old.id, input.key)) return error.InvalidSqlTriggerManifest;
        const validator = self.validator.forDomainAdmission(input.domain_admission);
        var null_fields = input.json_null_fields;
        if (input.after) |value| {
            if (!input.candidate_prepared) null_fields = (try validator.prepareCandidateValueWithNulls(self.a, value, input.json_null_fields)).fields;
        }
        const candidate = if (input.after) |value| try self.typedRow(input.key, value.*, null_fields) else null;
        const transformed: runtime.Output = if (self.execution) |execution| triggered: {
            const frame = try self.layout.frame(self.a, candidate, input.old);
            break :triggered try execution.fire(.{ .timing = .before, .level = .row, .event = input.event, .updated_columns = input.update_columns, .frame = &frame.frame, .owned = frame });
        } else .{ .row = if (input.event == .delete) input.old else candidate, .notices = &.{}, .fired = 0 };
        var result: Row = .{ .key = input.key, .event = input.event, .old = input.old, .new = candidate, .value = if (input.after) |value| value.* else null, .json_null_fields = null_fields, .update_columns = input.update_columns, .notices = transformed.notices };
        if (transformed.row == null) {
            result.skipped = true;
            return result;
        }
        if (input.after) |value| {
            if (transformed.row != candidate) null_fields = try self.writeCandidate(value, transformed.row.?.*);
            null_fields = (try validator.prepareSelectedValueWithNulls(self.a, value, null_fields)).fields;
            result.new = try self.typedRow(input.key, value.*, null_fields);
            result.value = value.*;
            result.json_null_fields = if (self.document_projection != null) &.{} else null_fields;
        }
        return result;
    }

    /// Caller runs final RLS WITH CHECK and distributed integrity validation
    /// before invoking this, and commits only after every AFTER stage succeeds.
    pub fn after(self: *Owner, row: *Row) ![]const @import("procedure.zig").Notice {
        if (row.after_executed) return error.InvalidSqlTriggerManifest;
        if (row.skipped) {
            row.after_executed = true;
            return &.{};
        }
        const execution = self.execution orelse {
            row.after_executed = true;
            return &.{};
        };
        var notices: std.ArrayList(@import("procedure.zig").Notice) = .empty;
        for (execution.captured.attachmentEntries(), 0..) |entry, index| {
            if (!entry.matches(.after, .row, row.event, row.update_columns) or row.after_entries.isSet(index)) continue;
            try notices.appendSlice(self.a, try self.afterAttachment(row, entry.id));
        }
        row.after_executed = true;
        return notices.toOwnedSlice(self.a);
    }

    /// One bounded coordinator-owned event. Original OLD/NEW images are reset
    /// for each attachment, and duplicate delivery is rejected before execution.
    pub fn afterAttachment(self: *Owner, row: *Row, id: u64) ![]const @import("procedure.zig").Notice {
        if (row.after_executed or row.skipped) return error.InvalidSqlTriggerManifest;
        const execution = self.execution orelse return error.InvalidSqlTriggerManifest;
        const index = for (execution.captured.attachmentEntries(), 0..) |entry, ordinal| {
            if (entry.id == id and entry.matches(.after, .row, row.event, row.update_columns)) break ordinal;
        } else return error.InvalidSqlTriggerManifest;
        if (row.after_entries.isSet(index)) return error.InvalidSqlTriggerManifest;
        const frame = try self.layout.frame(self.a, row.new, row.old);
        const output = try execution.fire(.{ .attachment_id = id, .timing = .after, .level = .row, .event = row.event, .updated_columns = row.update_columns, .frame = &frame.frame, .owned = frame });
        row.after_entries.set(index);
        row.after_executed = for (execution.captured.attachmentEntries(), 0..) |entry, ordinal| {
            if (entry.matches(.after, .row, row.event, row.update_columns) and !row.after_entries.isSet(ordinal)) break false;
        } else true;
        return output.notices;
    }
};

test "SQL trigger frame uses stable logical identity from the actual pinned schema and binding" {
    var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const state: catalog.triggers.State = .{ .metadata_incarnation = @splat(1), .table_id = 11, .logical_relation_id = 7, .generation = 1 };
    const encoded = try state.encode(a);
    const declaration = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"document\",\"default_type\":\"doc\",\"document_schemas\":{{\"doc\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"n\":{{\"type\":\"integer\"}}}}}}}}}},\"sql_triggers\":{s}}}", .{encoded});
    var view = try captureView(a, declaration);
    defer view.release();
    const binding: catalog.Table = .{ .id = 11, .logical_relation_id = 7, .physical_name = "physical:11", .schema_version = 1, .schema_json = declaration, .columns = &.{}, .storage_mode = .document };
    const owner = try Owner.create(a, view, binding, null, .{});
    defer owner.deinit();
    try std.testing.expectEqual(@as(u64, 7), owner.layout.table_id);
    try std.testing.expectEqual(@as(u64, 11), view.triggerState().?.table_id);
    const frame = try owner.layout.frame(a, null, null);
    const entry: catalog.triggers.Entry = .{ .id = 1, .generation = 1, .name = "touch", .timing = .before, .events = .{ .update = true }, .level = .row, .function = .{ .database = "antfly", .namespace = "public", .name = "touch", .namespace_id = 2, .id = 9 } };
    try frame.setVariables(a, entry, .update);
    try std.testing.expectEqual(@as(i64, 7), frame.frame.values[owner.layout.width * 2 + 6].value.integer);
    var stale = binding;
    stale.logical_relation_id = null;
    try std.testing.expectError(error.CatalogGenerationChanged, Owner.create(a, view, stale, null, .{}));
}
