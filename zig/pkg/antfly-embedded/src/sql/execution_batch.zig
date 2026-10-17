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

//! Borrowed operator batches. Native scans retain physical vectors and a
//! selection; scalar operators may provide owned rows. A consumer must finish
//! the batch before pulling its producer again. Payloads are gathered lazily.
const std = @import("std");
const scalar = @import("scalar.zig");
const A = std.mem.Allocator;
/// Validated frozen JSON tokens, distinct from canonical JSON text. V1 tags
/// preserve exact scalar representations. Bytes borrow the producer lease.
pub const FrozenJson = struct { version: enum { v1 } = .v1, bytes: []const u8, lossless_text: bool };
pub const Batch = union(enum) {
    /// One expression over page-local dictionary entries, including a NULL lane.
    dictionary: struct { values: []const scalar.Datum, indices: []const u32 },
    /// Stable retained columns; ownership belongs to the enclosing lease.
    retained: struct { store: *const @import("typed_store.zig").Store, begin: usize = 0, count: usize },
    /// Operator-specific column access without constructing a row matrix.
    reader: struct { ptr: *anyopaque, read: *const fn (*anyopaque, A, usize, usize) anyerror!scalar.Datum, read_dictionary: ?*const fn (*anyopaque, A, usize) anyerror!?Batch = null, read_frozen: ?*const fn (*anyopaque, usize, usize) anyerror!?FrozenJson = null, read_json: ?*const fn (*anyopaque, usize, usize) anyerror!?[]const u8 = null, read_identity: ?*const fn (*anyopaque, usize, usize) anyerror!?u64 = null, count: usize, width: usize },
    columns: struct { page: @import("catalog.zig").ColumnPage, definitions: []const scalar.Column },
    rows: []const []const scalar.Datum,
    vectors: struct { values: []const []const scalar.Datum, count: usize },
    mapped: struct { source: *const Batch, ordinals: []const usize, types: []const scalar.Type, selection: []const usize },
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
    /// Canonical, validated JSON bytes with this batch's lifetime. Declining
    /// this capability leaves the ordinary parsed cell accessor unchanged.
    pub fn encodedJson(self: Batch, row_index: usize, column: usize) anyerror!?[]const u8 {
        if (row_index >= self.len() or column >= self.width()) return error.InvalidSqlBackendResponse;
        return switch (self) {
            .retained => |v| v.store.encodedJson(v.begin + row_index, column),
            .reader => |v| if (v.read_json) |read| read(v.ptr, row_index, column) else null,
            .mapped => |v| if (column < v.types.len and v.types[column].kind == .json and v.ordinals[column] != std.math.maxInt(usize)) v.source.encodedJson(v.selection[row_index], v.ordinals[column]) else null,
            .columns => |v| if (v.definitions[column].type == .json) v.page.encodedJson(row_index, v.definitions[column].name) else null,
            else => null,
        };
    }
    pub fn frozenJson(self: Batch, row_index: usize, column: usize) anyerror!?FrozenJson {
        if (row_index >= self.len() or column >= self.width()) return error.InvalidSqlBackendResponse;
        return switch (self) {
            .retained => |v| v.store.frozenJson(v.begin + row_index, column),
            .reader => |v| if (v.read_frozen) |read| read(v.ptr, row_index, column) else null,
            .mapped => |v| if (column < v.types.len and v.types[column].kind == .json and v.ordinals[column] != std.math.maxInt(usize)) v.source.frozenJson(v.selection[row_index], v.ordinals[column]) else null,
            .columns => |v| if (v.definitions[column].type == .json) v.page.frozenJson(row_index, v.definitions[column].name) else null,
            else => null,
        };
    }
    /// Size admission must not force an encoded JSON column into a DOM.
    pub fn cellBytes(self: Batch, a: A, row_index: usize, column: usize) !usize {
        if (try self.frozenJson(row_index, column)) |value| return value.bytes.len;
        if (try self.encodedJson(row_index, column)) |bytes| return bytes.len;
        return @import("operators.zig").datumBytes(try self.cell(a, row_index, column));
    }
    /// A single borrowed column retains physical dictionary identities.
    pub fn project(self: *const Batch, a: A, column: usize) !Batch {
        if (column >= self.width()) return error.InvalidSqlBackendResponse;
        if (try self.dictionaryColumn(a, column)) |encoded| return encoded;
        const View = struct {
            source: *const Batch,
            column: usize,
            fn read(raw: *anyopaque, alloc: A, row_index: usize, _: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.cell(alloc, row_index, view.column);
            }
            fn json(raw: *anyopaque, row_index: usize, _: usize) !?[]const u8 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.encodedJson(row_index, view.column);
            }
            fn frozen(raw: *anyopaque, row_index: usize, _: usize) !?FrozenJson {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.frozenJson(row_index, view.column);
            }
            fn identity(raw: *anyopaque, row_index: usize, _: usize) !?u64 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.dictionaryIdentity(row_index, view.column);
            }
        };
        const view = try a.create(View);
        view.* = .{ .source = self, .column = column };
        return .{ .reader = .{ .ptr = view, .read = View.read, .read_identity = View.identity, .read_json = View.json, .read_frozen = View.frozen, .count = self.len(), .width = 1 } };
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
    /// Borrow a physical selection without evaluating unselected cells. The
    /// descriptor and dictionary ID maps belong to the caller's scratch arena.
    pub fn select(self: *const Batch, a: A, selection: []const usize) !Batch {
        const View = struct {
            source: *const Batch,
            selection: []const usize,
            fn read(raw: *anyopaque, alloc: A, row_index: usize, column: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.cell(alloc, view.selection[row_index], column);
            }
            fn dictionary(raw: *anyopaque, alloc: A, column: usize) !?Batch {
                const view: *@This() = @ptrCast(@alignCast(raw));
                var ids: std.AutoHashMapUnmanaged(u64, u32) = .empty;
                defer ids.deinit(alloc);
                var values: std.ArrayList(scalar.Datum) = .empty;
                defer values.deinit(alloc);
                const indices = try alloc.alloc(u32, view.selection.len);
                var transferred = false;
                defer if (!transferred) alloc.free(indices);
                for (view.selection, indices) |position, *id| {
                    const identity_ = (try view.source.dictionaryIdentity(position, column)) orelse return null;
                    const entry = try ids.getOrPut(alloc, identity_);
                    if (!entry.found_existing) {
                        entry.value_ptr.* = @intCast(values.items.len);
                        try values.append(alloc, try view.source.cell(alloc, position, column));
                    }
                    id.* = entry.value_ptr.*;
                }
                const dictionary_values = try values.toOwnedSlice(alloc);
                transferred = true;
                return .{ .dictionary = .{ .values = dictionary_values, .indices = indices } };
            }

            fn json(raw: *anyopaque, row_index: usize, column: usize) !?[]const u8 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.encodedJson(view.selection[row_index], column);
            }
            fn frozen(raw: *anyopaque, row_index: usize, column: usize) !?FrozenJson {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.frozenJson(view.selection[row_index], column);
            }
            fn identity(raw: *anyopaque, row_index: usize, column: usize) !?u64 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.dictionaryIdentity(view.selection[row_index], column);
            }
        };
        for (selection) |index| if (index >= self.len()) return error.InvalidSqlBackendResponse;
        const view = try a.create(View);
        view.* = .{ .source = self, .selection = selection };
        return .{ .reader = .{ .ptr = view, .count = selection.len, .width = self.width(), .read = View.read, .read_dictionary = View.dictionary, .read_identity = View.identity, .read_json = View.json, .read_frozen = View.frozen } };
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
                if (column >= v.types.len) return error.InvalidSqlBackendResponse;
                const type_ = v.types[column];
                break :blk try @import("describe.zig").coerceDatum(a, value, type_.kind orelse return error.InvalidSqlBackendResponse, type_.element_type);
            },
            .columns => |v| blk: {
                const definition = v.definitions[column];
                const value = try v.page.cell(a, index, definition.name);
                break :blk try @import("describe.zig").coerceDatum(a, value, definition.type, definition.element_type);
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
    const only_valid: Batch = .{ .mapped = .{ .source = &with_invalid, .ordinals = &.{0}, .types = &.{.{ .kind = .integer }}, .selection = &.{ 0, 0, 0 } } };
    const selected = (try only_valid.dictionaryColumn(a, 0)).?;
    defer a.free(selected.dictionary.values);
    defer a.free(selected.dictionary.indices);
    try std.testing.expectEqual(@as(usize, 1), selected.dictionary.values.len);
    const encoded = (try source.dictionaryColumn(a, 0)).?;
    defer a.free(encoded.dictionary.values);
    defer a.free(encoded.dictionary.indices);
    try std.testing.expectEqual(@as(usize, 3), encoded.dictionary.values.len);
    for (0..source.len()) |row| try std.testing.expectEqualDeep(try source.cell(a, row, 0), try encoded.cell(a, row, 0));
    const mapped: Batch = .{ .mapped = .{ .source = &encoded, .ordinals = &.{0}, .types = &.{.{ .kind = .integer }}, .selection = &.{ 2, 1, 3, 0 } } };
    const reordered = (try mapped.dictionaryColumn(a, 0)).?;
    defer a.free(reordered.dictionary.values);
    defer a.free(reordered.dictionary.indices);
    for (0..mapped.len()) |row| try std.testing.expectEqualDeep(try mapped.cell(a, row, 0), try reordered.cell(a, row, 0));
}
test "SQL NUMERIC batch mappings borrow exact cells without placeholder coercion" {
    var context: @import("numeric_value.zig").Context = .{ .alloc = std.testing.allocator };
    var number = try @import("numeric_value.zig").parse(&context, "9007199254740993.1200");
    defer number.deinit();
    const source: Batch = .{ .rows = &.{ &.{scalar.Datum.typedNumeric(&number.value)}, &.{.{}} } };
    var mapping: Batch = .{ .mapped = .{ .source = &source, .ordinals = &.{0}, .types = &.{.{ .kind = .number, .element_type = .numeric }}, .selection = &.{ 0, 1 } } };
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const cell_ = try mapping.cell(none.allocator(), 0, 0);
    try std.testing.expect(cell_.numeric == &number.value and !cell_.sql_null);
    try std.testing.expect((try mapping.cell(none.allocator(), 1, 0)).sql_null);
    mapping.mapped.types = &.{.{ .kind = .string }};
    try std.testing.expectError(error.SqlTypeMismatch, mapping.cell(none.allocator(), 0, 0));
}

test "SQL dictionary traits preserve selected coercion and mapping through allocation failures" {
    try selectedDictionaryScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectedDictionaryScenario, .{});
}

test "external lake mapped batches retain exact NUMERIC declarations before coercion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const source: Batch = .{ .rows = &.{ &.{scalar.Datum.json(.{ .integer = 9007199254740993 })}, &.{scalar.Datum.json(.{ .string = "9007199254740993.125" })}, &.{.{}} } };
    const mapped: Batch = .{ .mapped = .{ .source = &source, .ordinals = &.{0}, .types = &.{.{ .kind = .number, .element_type = .numeric }}, .selection = &.{ 0, 1, 2 } } };
    for ([_][]const u8{ "9007199254740993", "9007199254740993.125" }, 0..) |text, row| {
        const value = try mapped.cell(a, row, 0);
        var context: @import("numeric_value.zig").Context = .{ .alloc = a };
        try std.testing.expectEqualStrings(text, try @import("numeric_value.zig").format(&context, value.numeric.?.*));
    }
    try std.testing.expect((try mapped.cell(a, 2, 0)).sql_null);
}
