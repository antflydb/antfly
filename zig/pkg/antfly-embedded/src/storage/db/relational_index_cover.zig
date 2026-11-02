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

//! Schema-bound INCLUDE payloads. The payload is a compact ordinal row over
//! only key/INCLUDE columns; it never copies the unselected document. Logical
//! hash, timestamp and source schema identity belong to the complete primary
//! row. Physical addressing uses an immutable index-local layout.
const std = @import("std");
const schema = @import("../schema.zig");
const named_types = @import("../../system_catalog/named_types.zig");
const native = @import("../relational_index.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const Allocator = std.mem.Allocator;
pub const header_len = 40;

/// A single worker-owned cold source binding. Both epochs must outlive it;
/// callers replace it when source layout changes instead of growing a registry.
pub const Source = struct {
    alloc: Allocator,
    layout: *const codec.PhysicalLayout,
    ordinals: []?usize,
    fingerprint: [32]u8,
    pub fn deinit(self: *Source) void {
        self.alloc.free(self.ordinals);
        self.* = undefined;
    }
};

pub const Plan = struct {
    alloc: Allocator,
    columns: []schema.RelationalColumn,
    ordinals: []usize,
    source_layout: *const codec.PhysicalLayout,
    layout: codec.PhysicalLayout,
    fingerprint: [32]u8,
    /// Borrow executable definitions from the pinned source; own only this directory.
    named_types: []const named_types.Record,

    pub fn init(alloc: Allocator, source_table: schema.TableSchema, source_layout: *const codec.PhysicalLayout, keys: []const native.RelationalIndexKey, includes: []const []const u8) !Plan {
        if (includes.len == 0 or includes.len > 256 or keys.len + includes.len > 288) return error.InvalidRelationalIndexDefinition;
        // Expression results belong to tuple positions, not named row fields.
        // Cover only genuine key columns and explicitly requested INCLUDEs;
        // don't invent synthetic columns that could collide with user names.
        var names: [288][]const u8 = undefined;
        var count: usize = 0;
        for (keys) |key| if (key.expression_json == null) {
            names[count] = key.column;
            count += 1;
        };
        for (includes) |name| {
            names[count] = name;
            count += 1;
        }
        const columns = try alloc.alloc(schema.RelationalColumn, count);
        errdefer alloc.free(columns);
        const ordinals = try alloc.alloc(usize, columns.len);
        errdefer alloc.free(ordinals);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:index-cover:v1\x00");
        for (columns, ordinals, 0..) |*column, *ordinal, i| {
            const name = names[i];
            for (columns[0..i]) |prior| if (std.mem.eql(u8, prior.name, name)) return error.InvalidRelationalIndexDefinition;
            ordinal.* = source_layout.ordinalForName(source_table.relational_columns, name) orelse return error.RelationalIndexColumnNotFound;
            column.* = source_table.relational_columns[ordinal.*];
            var number: [8]u8 = undefined;
            for ([_][]const u8{ column.name, column.path }) |text| {
                std.mem.writeInt(u64, &number, text.len, .little);
                hash.update(&number);
                hash.update(text);
            }
            hash.update(&.{ @backingInt(column.column_type), @intFromBool(column.required), @intFromBool(column.allows_null), @intFromBool(column.is_json), @backingInt(column.json_kind) });
        }
        // Preserve existing cover identities when there is no precise SQL
        // declaration. Otherwise bind every column's descriptor, including
        // absent descriptors, without changing the physical row layout.
        const has_sql_types = for (columns) |column| {
            if (column.sql_element_type != null) break true;
        } else false;
        if (has_sql_types) {
            hash.update("antfly:index-cover:sql-types:v1\x00");
            for (columns) |column| hash.update(&.{if (column.sql_element_type) |kind| @backingInt(kind) + 1 else 0});
        }
        const has_modifiers = for (columns) |column| {
            if (column.numeric_modifier != null) break true;
        } else false;
        if (has_modifiers) {
            hash.update("antfly:index-cover:numeric-modifiers:v1\x00");
            for (columns) |column| {
                hash.update(&.{@intFromBool(column.numeric_modifier != null)});
                if (column.numeric_modifier) |modifier| {
                    var bytes: [4]u8 = undefined;
                    std.mem.writeInt(u16, bytes[0..2], modifier.precision, .little);
                    std.mem.writeInt(i16, bytes[2..4], modifier.scale, .little);
                    hash.update(&bytes);
                }
            }
        }
        var included = std.AutoHashMapUnmanaged(u64, void).empty;
        defer included.deinit(alloc);
        var type_records = std.ArrayList(named_types.Record).empty;
        defer type_records.deinit(alloc);
        for (columns) |column| if (column.named_type_id) |id| {
            const record = schema.namedType(source_table, id) orelse return error.InvalidSchema;
            var encoded_id: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded_id, id, .little);
            hash.update("named SQL type");
            hash.update(&encoded_id);
            hash.update(&record.identity.origin_id);
            if (!(try included.getOrPut(alloc, id)).found_existing) try type_records.append(alloc, record.*);
        };
        // A projected domain retains every base definition required to decode
        // its cells. Copy only immutable record headers; the source epoch owns
        // their bytes for this covering layout's lifetime.
        var type_cursor: usize = 0;
        while (type_cursor < type_records.items.len) : (type_cursor += 1) {
            const record = type_records.items[type_cursor];
            if (record.definition != .domain or record.definition.domain.base != .named) continue;
            const id = record.definition.domain.base.named.id;
            if ((try included.getOrPut(alloc, id)).found_existing) continue;
            if (type_records.items.len == 4096) return error.InvalidSchema;
            const parent = schema.namedType(source_table, id) orelse return error.InvalidSchema;
            try type_records.append(alloc, parent.*);
        }
        std.mem.sort(named_types.Record, type_records.items, {}, struct {
            fn less(_: void, left: named_types.Record, right: named_types.Record) bool {
                return left.identity.id < right.identity.id;
            }
        }.less);
        const registry = try type_records.toOwnedSlice(alloc);
        errdefer alloc.free(registry);
        const layout = try codec.PhysicalLayout.init(alloc, .{ .version = 1, .storage_mode = .relational, .requires_public_schema = registry.len != 0, .named_types = registry, .relational_columns = columns });
        var fingerprint: [32]u8 = undefined;
        hash.final(&fingerprint);
        return .{ .alloc = alloc, .columns = columns, .ordinals = ordinals, .source_layout = source_layout, .layout = layout, .fingerprint = fingerprint, .named_types = registry };
    }

    pub fn deinit(self: *Plan) void {
        self.layout.deinit();
        self.alloc.free(self.named_types);
        self.alloc.free(self.columns);
        self.alloc.free(self.ordinals);
        self.* = undefined;
    }

    pub fn table(self: *const Plan) schema.TableSchema {
        return .{ .version = 1, .storage_mode = .relational, .requires_public_schema = self.named_types.len != 0, .named_types = self.named_types, .relational_columns = self.columns };
    }

    pub fn contains(self: *const Plan, name: []const u8) bool {
        return self.layout.ordinalForName(self.columns, name) != null;
    }

    pub fn projectSource(self: *const Plan, alloc: Allocator, source_table: schema.TableSchema, layout: *const codec.PhysicalLayout) !Source {
        if (source_table.storage_mode != .relational or source_table.version != layout.schema_version or source_table.relational_columns.len != layout.column_count) return error.RelationalRowSchemaMismatch;
        const ordinals = try alloc.alloc(?usize, self.columns.len);
        errdefer alloc.free(ordinals);
        var checked_types = std.AutoHashMapUnmanaged(u64, void).empty;
        defer checked_types.deinit(alloc);
        for (self.columns, ordinals) |column, *ordinal| {
            ordinal.* = layout.ordinalForName(source_table.relational_columns, column.name);
            if (ordinal.*) |bound| {
                const source_column = source_table.relational_columns[bound];
                if (source_column.column_type != column.column_type or source_column.json_kind != column.json_kind or source_column.sql_element_type != column.sql_element_type or source_column.named_type_id != column.named_type_id or
                    !@import("../../common/sql_builtin_type.zig").NumericModifier.eql(source_column.numeric_modifier, column.numeric_modifier) or
                    !std.mem.eql(u8, source_column.path, column.path)) return error.RelationalIndexColumnTypeMismatch;
                if (column.named_type_id) |id| {
                    if ((try checked_types.getOrPut(alloc, id)).found_existing) continue;
                    if (!((schema.sameNamedPhysicalType(source_table, self.table(), id, false) catch return error.RelationalIndexColumnTypeMismatch))) return error.RelationalIndexColumnTypeMismatch;
                }
            } else if (column.required) return error.RelationalIndexColumnNotFound;
        }
        return .{ .alloc = alloc, .layout = layout, .ordinals = ordinals, .fingerprint = self.fingerprint };
    }

    pub fn encode(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView) ![]u8 {
        return self.encodeSource(alloc, row, null);
    }

    pub fn encodeSource(self: *const Plan, alloc: Allocator, row: codec.OrdinalRowView, source: ?*const Source) ![]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(alloc);
        try self.appendSource(alloc, &out, row, source);
        return out.toOwnedSlice(alloc);
    }

    pub fn append(self: *const Plan, alloc: Allocator, out: *std.ArrayList(u8), row: codec.OrdinalRowView) !void {
        return self.appendSource(alloc, out, row, null);
    }

    pub fn appendSource(self: *const Plan, alloc: Allocator, out: *std.ArrayList(u8), row: codec.OrdinalRowView, source: ?*const Source) !void {
        if (row.layout != (if (source) |bound| bound.layout else self.source_layout)) return error.RelationalRowSchemaMismatch;
        if (source) |bound| if (bound.ordinals.len != self.columns.len or !std.mem.eql(u8, &bound.fingerprint, &self.fingerprint)) return error.RelationalRowSchemaMismatch;
        var cells: [288]codec.Cell = undefined;
        var count: usize = 0;
        for (self.ordinals, 0..) |bound, i| {
            const ordinal = if (source) |binding| binding.ordinals[i] orelse continue else bound;
            if (try row.findCell(ordinal)) |cell| {
                cells[count] = cell;
                cells[count].ordinal = @intCast(i);
                count += 1;
            }
        }
        const encoded = try codec.serializePreparedOrdinalDeferredHash(alloc, 1, self.columns, cells[0..count], &self.layout);
        defer alloc.free(encoded);
        try codec.finalizeOrdinalMetadata(encoded, row.semanticHash(), row.writeTimestampNs());
        var header: [header_len]u8 = undefined;
        @memcpy(header[0..4], "AIC1");
        @memcpy(header[4..36], &self.fingerprint);
        std.mem.writeInt(u32, header[36..40], row.table_schema.version, .little);
        try out.ensureUnusedCapacity(alloc, header.len + encoded.len);
        out.appendSliceAssumeCapacity(&header);
        out.appendSliceAssumeCapacity(encoded);
    }

    pub fn decode(self: *const Plan, encoded: []const u8) !codec.OrdinalRowView {
        if (encoded.len <= header_len or !std.mem.eql(u8, encoded[0..4], "AIC1") or !std.mem.eql(u8, encoded[4..36], &self.fingerprint)) return error.InvalidRelationalIndexForwardValue;
        return codec.ordinalRowViewSelective(encoded[header_len..], self.table(), &self.layout);
    }

    pub fn sourceVersion(encoded: []const u8) !u32 {
        if (encoded.len <= header_len) return error.InvalidRelationalIndexForwardValue;
        return std.mem.readInt(u32, encoded[36..40], .little);
    }
};

test "relational index system NUMERIC modifiers fence cover payloads without changing logical keys" {
    const a = std.testing.allocator;
    var columns = [_]schema.RelationalColumn{
        .{ .name = "k", .path = "k", .column_type = .integer },
        .{ .name = "n", .path = "n", .column_type = .numeric, .sql_element_type = .numeric, .numeric_modifier = .{ .precision = 4, .scale = 2 } },
    };
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var layout = try codec.PhysicalLayout.init(a, table);
    defer layout.deinit();
    var first = try Plan.init(a, table, &layout, &.{.{ .column = "k" }}, &.{"n"});
    defer first.deinit();
    columns[1].numeric_modifier.?.scale = 1;
    var second = try Plan.init(a, table, &layout, &.{.{ .column = "k" }}, &.{"n"});
    defer second.deinit();
    try std.testing.expect(!std.mem.eql(u8, &first.fingerprint, &second.fingerprint));
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, first.projectSource(a, table, &layout));
    var source = try second.projectSource(a, table, &layout);
    defer source.deinit();
    const TuplePlan = @import("relational_index_keys.zig").TuplePlan;
    var first_key = try TuplePlan.init(a, first.table(), &first.layout, &.{.{ .column = "n" }});
    defer first_key.deinit();
    var second_key = try TuplePlan.init(a, second.table(), &second.layout, &.{.{ .column = "n" }});
    defer second_key.deinit();
    try std.testing.expect(first_key.sameEqualityKey(0, second_key, 0));
}

test "relational index system SQL cover identity and source binding retain precise types" {
    const alloc = std.testing.allocator;
    const columns = [_]schema.RelationalColumn{
        .{ .name = "k", .path = "k", .column_type = .integer },
        .{ .name = "v", .path = "v", .column_type = .integer },
    };
    const plain: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    var plain_layout = try codec.PhysicalLayout.init(alloc, plain);
    defer plain_layout.deinit();
    var plain_plan = try Plan.init(alloc, plain, &plain_layout, &.{.{ .column = "k" }}, &.{"v"});
    defer plain_plan.deinit();
    var typed_columns = columns;
    typed_columns[1].sql_element_type = .int32;
    const typed: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &typed_columns };
    var typed_layout = try codec.PhysicalLayout.init(alloc, typed);
    defer typed_layout.deinit();
    var typed_plan = try Plan.init(alloc, typed, &typed_layout, &.{.{ .column = "k" }}, &.{"v"});
    defer typed_plan.deinit();
    try std.testing.expect(!std.mem.eql(u8, &plain_plan.fingerprint, &typed_plan.fingerprint));
    try std.testing.expectEqual(@import("../../common/sql_builtin_type.zig").Type.int32, typed_plan.columns[1].sql_element_type.?);
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, typed_plan.projectSource(alloc, plain, &plain_layout));
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, plain_plan.projectSource(alloc, typed, &typed_layout));
    var source = try typed_plan.projectSource(alloc, typed, &typed_layout);
    defer source.deinit();
}
