// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Schema-bound INCLUDE payloads. The payload is a compact ordinal row over
//! only key/INCLUDE columns; it never copies the unselected document. Logical
//! hash, timestamp and source schema identity belong to the complete primary
//! row. Physical addressing uses an immutable index-local layout.
const std = @import("std");
const schema = @import("../schema.zig");
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
            hash.update(&.{ @intFromEnum(column.column_type), @intFromBool(column.required), @intFromBool(column.allows_null), @intFromBool(column.is_json), @intFromEnum(column.json_kind) });
        }
        const layout = try codec.PhysicalLayout.init(alloc, .{ .version = 1, .storage_mode = .relational, .relational_columns = columns });
        var fingerprint: [32]u8 = undefined;
        hash.final(&fingerprint);
        return .{ .alloc = alloc, .columns = columns, .ordinals = ordinals, .source_layout = source_layout, .layout = layout, .fingerprint = fingerprint };
    }

    pub fn deinit(self: *Plan) void {
        self.layout.deinit();
        self.alloc.free(self.columns);
        self.alloc.free(self.ordinals);
        self.* = undefined;
    }

    pub fn table(self: *const Plan) schema.TableSchema {
        return .{ .version = 1, .storage_mode = .relational, .relational_columns = self.columns };
    }

    pub fn contains(self: *const Plan, name: []const u8) bool {
        return self.layout.ordinalForName(self.columns, name) != null;
    }

    pub fn projectSource(self: *const Plan, alloc: Allocator, source_table: schema.TableSchema, layout: *const codec.PhysicalLayout) !Source {
        if (source_table.storage_mode != .relational or source_table.version != layout.schema_version or source_table.relational_columns.len != layout.column_count) return error.RelationalRowSchemaMismatch;
        const ordinals = try alloc.alloc(?usize, self.columns.len);
        errdefer alloc.free(ordinals);
        for (self.columns, ordinals) |column, *ordinal| {
            ordinal.* = layout.ordinalForName(source_table.relational_columns, column.name);
            if (ordinal.*) |bound| {
                const source_column = source_table.relational_columns[bound];
                if (source_column.column_type != column.column_type or source_column.json_kind != column.json_kind or !std.mem.eql(u8, source_column.path, column.path)) return error.RelationalIndexColumnTypeMismatch;
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
