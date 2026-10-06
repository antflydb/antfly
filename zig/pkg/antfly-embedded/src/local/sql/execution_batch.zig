// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Borrowed operator batches. Native scans retain physical vectors and a
//! selection; scalar operators may provide owned rows. A consumer must finish
//! the batch before pulling its producer again. Payloads are gathered lazily.
const std = @import("std");
const scalar = @import("scalar.zig");
const A = std.mem.Allocator;
pub const Batch = union(enum) {
    /// One expression over page-local dictionary entries, including a NULL lane.
    dictionary: struct { values: []const scalar.Datum, indices: []const u32 },
    /// Stable retained columns; ownership belongs to the enclosing lease.
    retained: struct { store: *const @import("typed_store.zig").Store, begin: usize = 0, count: usize },
    /// Operator-specific column access without constructing a row matrix.
    reader: struct { ptr: *anyopaque, read: *const fn (*anyopaque, A, usize, usize) anyerror!scalar.Datum, read_dictionary: ?*const fn (*anyopaque, A, usize) anyerror!?Batch = null, read_identity: ?*const fn (*anyopaque, usize, usize) anyerror!?u64 = null, count: usize, width: usize },
    columns: struct { page: @import("catalog.zig").ColumnPage, definitions: []const scalar.Column },
    rows: []const []const scalar.Datum,
    vectors: struct { values: []const []const scalar.Datum, count: usize },
    mapped: struct { source: *const Batch, ordinals: []const usize, kinds: []const @import("ast.zig").ColumnType, selection: []const usize },
    pub fn len(self: Batch) usize {
        return switch (self) {
            .dictionary => |v| v.indices.len,
            .retained => |v| v.count,
            .reader => |v| v.count,
            .columns => |v| v.page.selection.len,
            .rows => |v| v.len,
            .vectors => |v| v.count,
            .mapped => |v| v.selection.len,
        };
    }
    pub fn width(self: Batch) usize {
        return switch (self) {
            .dictionary => 1,
            .retained => |v| v.store.columns.len,
            .reader => |v| v.width,
            .columns => |v| v.definitions.len,
            .rows => |v| if (v.len == 0) 0 else v[0].len,
            .vectors => |v| v.values.len,
            .mapped => |v| v.ordinals.len,
        };
    }
    /// Optional physical representation, with the same cells and row order.
    /// Producers decline when normalization or effects require scalar access.
    pub fn dictionaryColumn(self: Batch, a: A, column: usize) !?Batch {
        if (column >= self.width()) return error.InvalidSqlBackendResponse;
        return switch (self) {
            .dictionary => self,
            .retained => |v| v.store.dictionaryBatch(a, column, v.begin, v.count),
            .reader => |v| if (v.read_dictionary) |read| read(v.ptr, a, column) else null,
            .columns, .mapped => self.selectedDictionary(a, column),
            else => null,
        };
    }
    /// Physical identity only: tracing selections must not normalize or
    /// evaluate rows that an outer mapping will never consume.
    pub fn dictionaryIdentity(self: Batch, index: usize, column: usize) anyerror!?u64 {
        if (index >= self.len() or column >= self.width()) return error.InvalidSqlBackendResponse;
        return switch (self) {
            .dictionary => |v| if (v.indices[index] < v.values.len) @as(u64, v.indices[index]) else error.InvalidSqlBackendResponse,
            .reader => |v| if (v.read_identity) |read| read(v.ptr, index, column) else null,
            .retained => |v| v.store.dictionaryId(v.begin + index, column),
            .mapped => |v| if (v.ordinals[column] == std.math.maxInt(usize)) null else v.source.dictionaryIdentity(v.selection[index], v.ordinals[column]),
            .columns => |v| blk: {
                const physical = v.page.selection[index];
                const name = v.definitions[column].name;
                if (v.page.native) |native| {
                    const ordinal = for (native.names, 0..) |candidate, position| {
                        if (std.mem.eql(u8, candidate, name)) break position;
                    } else return null;
                    break :blk native.values.dictionaryIdentity(physical, ordinal);
                }
                const source = v.page.batch.findColumn(name) orelse return null;
                switch (source.values) {
                    .dictionary_bytes, .dictionary_i64, .dictionary_f64 => {},
                    else => return null,
                }
                break :blk if (try source.dictionaryId(physical)) |id| @as(u64, id) + 1 else 0;
            },
            else => null,
        };
    }
    fn selectedDictionary(self: Batch, a: A, column: usize) !?Batch {
        if (self.len() == 0 or try self.dictionaryIdentity(0, column) == null) return null;
        var ids: std.AutoHashMapUnmanaged(u64, u32) = .empty;
        defer ids.deinit(a);
        var values: std.ArrayList(scalar.Datum) = .empty;
        defer values.deinit(a);
        const indices = try a.alloc(u32, self.len());
        errdefer a.free(indices);
        for (indices, 0..) |*id, row_index| {
            const identity = (try self.dictionaryIdentity(row_index, column)) orelse return error.InvalidSqlBackendResponse;
            const entry = try ids.getOrPut(a, identity);
            if (!entry.found_existing) {
                entry.value_ptr.* = @intCast(values.items.len);
                try values.append(a, try self.cell(a, row_index, column));
            }
            id.* = entry.value_ptr.*;
        }
        return .{ .dictionary = .{ .values = try values.toOwnedSlice(a), .indices = indices } };
    }
    pub fn cell(self: Batch, a: A, index: usize, column: usize) anyerror!scalar.Datum {
        if (index >= self.len() or column >= self.width()) return error.InvalidSqlBackendResponse;
        return switch (self) {
            .dictionary => |v| if (v.indices[index] < v.values.len) v.values[v.indices[index]] else error.InvalidSqlBackendResponse,
            .retained => |v| v.store.cell(a, v.begin + index, column),
            .reader => |v| v.read(v.ptr, a, index, column),
            .rows => |v| v[index][column],
            .vectors => |v| v.values[column][index],
            .mapped => |v| blk: {
                if (v.ordinals[column] == std.math.maxInt(usize)) break :blk .{};
                const value = try v.source.cell(a, v.selection[index], v.ordinals[column]);
                break :blk .{ .value = try @import("describe.zig").coerceAlloc(a, value.value, v.kinds[column]), .sql_null = value.sql_null, .patterns = value.patterns };
            },
            .columns => |v| blk: {
                const definition = v.definitions[column];
                const value = try v.page.cell(a, index, definition.name);
                break :blk .{ .value = try @import("describe.zig").coerceAlloc(a, value.value, definition.type), .sql_null = value.sql_null, .patterns = value.patterns };
            },
        };
    }
    pub fn row(self: Batch, a: A, index: usize) ![]const scalar.Datum {
        if (index >= self.len()) return error.InvalidSqlBackendResponse;
        if (self == .rows) return self.rows[index];
        const values = try a.alloc(scalar.Datum, self.width());
        for (values, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return values;
    }
};

fn selectedDictionaryScenario(a: A) !void {
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const source: Batch = .{ .columns = .{ .definitions = &definitions, .page = .{
        .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" }, .{ .relational_key = "d" } }, .columns = &.{.{ .name = "n", .values = .{ .dictionary_bytes = .{ .values = &.{ "9007199254740993", "-7", "invalid-unselected" }, .indices = &.{ 0, 1, 99, 0 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } }} },
        .selection = &.{ 3, 2, 1, 0 },
    } } };
    const invalid_definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const with_invalid: Batch = .{ .columns = .{ .definitions = &invalid_definitions, .page = .{
        .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } }, .columns = &.{.{ .name = "n", .values = .{ .dictionary_bytes = .{ .values = &.{ "41", "invalid" }, .indices = &.{ 0, 1 } } } }} },
        .selection = &.{ 0, 1 },
    } } };
    const only_valid: Batch = .{ .mapped = .{ .source = &with_invalid, .ordinals = &.{0}, .kinds = &.{.integer}, .selection = &.{ 0, 0, 0 } } };
    const selected = (try only_valid.dictionaryColumn(a, 0)).?;
    defer a.free(selected.dictionary.values);
    defer a.free(selected.dictionary.indices);
    try std.testing.expectEqual(@as(usize, 1), selected.dictionary.values.len);
    const encoded = (try source.dictionaryColumn(a, 0)).?;
    defer a.free(encoded.dictionary.values);
    defer a.free(encoded.dictionary.indices);
    try std.testing.expectEqual(@as(usize, 3), encoded.dictionary.values.len);
    for (0..source.len()) |row| try std.testing.expectEqualDeep(try source.cell(a, row, 0), try encoded.cell(a, row, 0));
    const mapped: Batch = .{ .mapped = .{ .source = &encoded, .ordinals = &.{0}, .kinds = &.{.integer}, .selection = &.{ 2, 1, 3, 0 } } };
    const reordered = (try mapped.dictionaryColumn(a, 0)).?;
    defer a.free(reordered.dictionary.values);
    defer a.free(reordered.dictionary.indices);
    for (0..mapped.len()) |row| try std.testing.expectEqualDeep(try mapped.cell(a, row, 0), try reordered.cell(a, row, 0));
}
test "SQL dictionary traits preserve selected coercion and mapping through allocation failures" {
    try selectedDictionaryScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectedDictionaryScenario, .{});
}
