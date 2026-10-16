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
/// A borrowed physical column plus its logical row mapping. The declaration
/// proves scalar boundary coercion is an identity; unsupported coercions decline.
pub const PhysicalColumn = struct {
    vector: @import("../storage/rowsource/types.zig").ColumnVector,
    selection: []const usize,
    kind: @import("ast.zig").ColumnType,
    element: ?@import("array_value.zig").ElementType = null,
    pub fn select(self: @This(), a: A, selected: []const usize) !@This() {
        const rows = try a.alloc(usize, selected.len);
        errdefer a.free(rows);
        for (rows, selected) |*row, index| {
            if (index >= self.selection.len) return error.InvalidSqlBackendResponse;
            row.* = self.selection[index];
        }
        var view = self;
        view.selection = rows;
        return view;
    }
    /// Normalize only selected lanes; excluded payloads remain lazy. Narrow
    /// integers are checked without copying, and REAL values round once into
    /// a compact native vector using the scalar width/underflow contract.
    pub fn coerce(self: @This(), a: A, kind_: @import("ast.zig").ColumnType, element_: ?@import("array_value.zig").ElementType) !?@This() {
        if (self.kind != kind_) return null;
        const compatible = switch (kind_) {
            .integer => element_ == null or element_ == .int16 or element_ == .int32 or element_ == .int64,
            .number => element_ == null or element_ == .float32 or element_ == .float64,
            .boolean => element_ == null or element_ == .boolean,
            .string => element_ == null or element_ == .text,
            else => false,
        };
        if (!compatible) return null;
        var view = self;
        view.element = element_;
        try view.validate();
        if (element_ == .float32) {
            const values = try a.alloc(f64, self.selection.len);
            errdefer a.free(values);
            const nulls = try a.alloc(u8, values.len);
            errdefer a.free(nulls);
            const selection = try a.alloc(usize, values.len);
            errdefer a.free(selection);
            for (self.selection, values, nulls, selection, 0..) |row, *value, *flag, *index, i| {
                flag.* = @intFromBool(self.vector.nulls.isNull(row));
                index.* = i;
                const number = if (flag.* != 0) 0 else switch (self.vector.values) {
                    .f64 => |v| v[row],
                    .dictionary_f64 => |v| v.at(row),
                    else => return error.InvalidSqlBackendResponse,
                };
                value.* = try @import("builtin_cast.zig").floatValue(f32, .{ .float = number });
            }
            view.vector = .{ .name = self.vector.name, .values = .{ .f64 = values }, .nulls = .{ .bytes = nulls } };
            view.selection = selection;
        }
        return view;
    }
    pub fn cell(self: @This(), index: usize) !scalar.Datum {
        if (index >= self.selection.len) return error.InvalidSqlBackendResponse;
        const row = self.selection[index];
        if (self.vector.nulls.isNull(row)) return .{};
        return scalar.Datum.json(switch (self.vector.values) {
            .i64 => |v| .{ .integer = v[row] },
            .dictionary_i64 => |v| .{ .integer = v.at(row) },
            .f64 => |v| .{ .float = v[row] },
            .dictionary_f64 => |v| .{ .float = v.at(row) },
            .bool => |v| .{ .bool = v[row] },
            .bytes => |v| .{ .string = v[row] },
            .dictionary_bytes => |v| .{ .string = v.at(row) },
            else => return error.InvalidSqlBackendResponse,
        });
    }
    pub fn bytes(self: @This(), index: usize) !usize {
        const row_index = self.selection[index];
        if (self.vector.nulls.isNull(row_index)) return @sizeOf(scalar.Datum);
        return @sizeOf(scalar.Datum) + switch (self.vector.values) {
            .bytes => |v| v[row_index].len,
            .dictionary_bytes => |v| v.at(row_index).len,
            else => @as(usize, 0),
        };
    }
    pub fn dictionaryIdentity(self: @This(), index: usize) ?u64 {
        const row_index = self.selection[index];
        if (self.vector.nulls.isNull(row_index)) return 0;
        return switch (self.vector.values) {
            inline .dictionary_i64, .dictionary_f64, .dictionary_bytes => |v| @as(u64, v.indices[row_index]) + 1,
            else => null,
        };
    }
    pub fn validate(self: @This()) !void {
        const compatible = switch (self.vector.values) {
            .i64, .dictionary_i64 => self.kind == .integer,
            .f64, .dictionary_f64 => self.kind == .number,
            .bool => self.kind == .boolean,
            .bytes, .dictionary_bytes => self.kind == .string,
            else => false,
        };
        if (!compatible or (self.vector.nulls.bytes.len != 0 and self.vector.nulls.bytes.len != self.vector.rowCount())) return error.InvalidSqlBackendResponse;
        for (self.selection) |row| {
            if (row >= self.vector.rowCount()) return error.InvalidSqlBackendResponse;
            if (self.vector.nulls.isNull(row)) continue;
            switch (self.vector.values) {
                inline .dictionary_i64, .dictionary_f64, .dictionary_bytes => |values| if (values.indices[row] >= values.values.len) return error.InvalidSqlBackendResponse,
                else => {},
            }
            if (self.kind == .integer and (self.element == .int16 or self.element == .int32)) {
                const integer = switch (self.vector.values) {
                    .i64 => |values| values[row],
                    .dictionary_i64 => |values| values.at(row),
                    else => unreachable,
                };
                _ = try @import("builtin_cast.zig").checkedInteger(integer, self.element.?);
            }
            if (self.kind == .number and self.element == null) {
                const number = switch (self.vector.values) {
                    .f64 => |values| values[row],
                    .dictionary_f64 => |values| values.at(row),
                    else => unreachable,
                };
                if (!std.math.isFinite(number)) return error.SqlTypeMismatch;
            }
        }
    }
};
pub const Batch = union(enum) {
    /// One expression over page-local dictionary entries, including a NULL lane.
    dictionary: struct { values: []const scalar.Datum, indices: []const u32 },
    /// Stable retained columns; ownership belongs to the enclosing lease.
    retained: struct { store: *const @import("typed_store.zig").Store, begin: usize = 0, count: usize },
    /// Operator-specific column access without constructing a row matrix.
    reader: struct { ptr: *anyopaque, read: *const fn (*anyopaque, A, usize, usize) anyerror!scalar.Datum, free_owned: ?*const fn (*anyopaque, A) void = null, read_physical: ?*const fn (*anyopaque, A, usize) anyerror!?PhysicalColumn = null, read_dictionary: ?*const fn (*anyopaque, A, usize) anyerror!?Batch = null, read_identity: ?*const fn (*anyopaque, usize, usize) anyerror!?u64 = null, count: usize, width: usize },
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
    /// A single borrowed column retains physical dictionary identities.
    pub fn project(self: *const Batch, a: A, column: usize) !Batch {
        if (column >= self.width()) return error.InvalidSqlBackendResponse;
        const physical = try self.physicalColumn(a, column);
        if (physical == null) if (try self.dictionaryColumn(a, column)) |encoded| return encoded;
        const View = struct {
            source: *const Batch,
            column: usize,
            physical: ?PhysicalColumn,
            fn physicalView(raw: *anyopaque, _: A, _: usize) !?PhysicalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.physical;
            }
            fn read(raw: *anyopaque, alloc: A, row_index: usize, _: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.cell(alloc, row_index, view.column);
            }
            fn dictionary(raw: *anyopaque, alloc: A, _: usize) !?Batch {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.dictionaryColumn(alloc, view.column);
            }
            fn identity(raw: *anyopaque, row_index: usize, _: usize) !?u64 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.dictionaryIdentity(row_index, view.column);
            }
        };
        const view = try a.create(View);
        view.* = .{ .source = self, .column = column, .physical = physical };
        return .{ .reader = .{ .ptr = view, .read = View.read, .read_physical = View.physicalView, .read_dictionary = View.dictionary, .read_identity = View.identity, .count = self.len(), .width = 1 } };
    }
    /// Scatter a lazily evaluated FILTER cohort back into its parent batch.
    /// Missing lanes are SQL NULL and never touch excluded source payloads.
    pub fn mask(self: *const Batch, a: A, positions: []const usize, count: usize) !Batch {
        if (positions.len != self.len() or self.width() != 1) return error.InvalidSqlBackendResponse;
        const View = struct {
            source: *const Batch,
            indices: []usize,
            fn read(raw: *anyopaque, alloc: A, row_index: usize, _: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                const index = view.indices[row_index];
                return if (index == std.math.maxInt(usize)) .{} else view.source.cell(alloc, index, 0);
            }
            fn physical(raw: *anyopaque, alloc: A, _: usize) !?PhysicalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                const source = (try view.source.physicalColumn(alloc, 0)) orelse return null;
                try source.validate();
                const rows = try alloc.alloc(usize, view.indices.len);
                const nulls = try alloc.alloc(u8, rows.len);
                for (rows, nulls, view.indices, 0..) |*row_index, *flag, index, i| {
                    row_index.* = i;
                    flag.* = @intFromBool(index == std.math.maxInt(usize) or source.vector.nulls.isNull(source.selection[index]));
                }
                const values: @import("../storage/rowsource/types.zig").ColumnValues = switch (source.vector.values) {
                    inline .i64, .f64, .bool, .bytes => |v, tag| blk: {
                        const result = try alloc.alloc(@typeInfo(@TypeOf(v)).pointer.child, rows.len);
                        for (result, nulls, view.indices) |*value, flag, index| value.* = if (flag != 0) std.mem.zeroes(@TypeOf(value.*)) else v[source.selection[index]];
                        break :blk @unionInit(@import("../storage/rowsource/types.zig").ColumnValues, @tagName(tag), result);
                    },
                    inline .dictionary_i64, .dictionary_f64, .dictionary_bytes => |v, tag| blk: {
                        const indices = try alloc.alloc(u32, rows.len);
                        for (indices, nulls, view.indices) |*id, flag, index| id.* = if (flag != 0) 0 else v.indices[source.selection[index]];
                        break :blk @unionInit(@import("../storage/rowsource/types.zig").ColumnValues, @tagName(tag), .{ .values = v.values, .indices = indices });
                    },
                    else => return null,
                };
                return .{ .vector = .{ .name = "", .values = values, .nulls = .{ .bytes = nulls } }, .selection = rows, .kind = source.kind, .element = source.element };
            }
        };
        const indices = try a.alloc(usize, count);
        @memset(indices, std.math.maxInt(usize));
        for (positions, 0..) |position, index| {
            if (position >= count or indices[position] != std.math.maxInt(usize)) return error.InvalidSqlBackendResponse;
            indices[position] = index;
        }
        const view = try a.create(View);
        view.* = .{ .source = self, .indices = indices };
        return .{ .reader = .{ .ptr = view, .read = View.read, .read_physical = View.physical, .count = count, .width = 1 } };
    }
    pub fn physicalColumn(self: Batch, a: A, column: usize) anyerror!?PhysicalColumn {
        if (column >= self.width()) return error.InvalidSqlBackendResponse;
        // Empty derived producers may retain schema names without physical
        // columns. There is no payload to borrow or normalize at EOF.
        if (self.len() == 0) return null;
        return switch (self) {
            .reader => |v| if (v.read_physical) |read| try read(v.ptr, a, column) else null,
            .retained => |v| try v.store.physicalColumn(a, column, v.begin, v.count),
            .mapped => |v| blk: {
                if (v.ordinals[column] == std.math.maxInt(usize) or column >= v.types.len) break :blk null;
                const source = (try v.source.physicalColumn(a, v.ordinals[column])) orelse break :blk null;
                break :blk try (try source.select(a, v.selection)).coerce(a, v.types[column].kind orelse return null, v.types[column].element_type);
            },
            .columns => |v| blk: {
                const definition = v.definitions[column];
                if (v.page.native) |native| {
                    for (native.names, 0..) |name, ordinal| if (std.mem.eql(u8, name, definition.name)) {
                        const source = (try native.values.physicalColumn(a, ordinal)) orelse break :blk null;
                        break :blk try (try source.select(a, v.page.selection)).coerce(a, definition.type, definition.element_type);
                    };
                    break :blk null;
                }
                if (std.mem.eql(u8, definition.name, "_id")) break :blk null;
                const vector = v.page.batch.findColumn(definition.name) orelse break :blk null;
                const compatible = switch (vector.values) {
                    .i64, .dictionary_i64 => definition.type == .integer and (definition.element_type == null or definition.element_type == .int16 or definition.element_type == .int32 or definition.element_type == .int64),
                    .f64, .dictionary_f64 => definition.type == .number and (definition.element_type == null or definition.element_type == .float32 or definition.element_type == .float64),
                    .bool => definition.type == .boolean and (definition.element_type == null or definition.element_type == .boolean),
                    .bytes, .dictionary_bytes => definition.type == .string and (definition.element_type == null or definition.element_type == .text),
                    else => false,
                };
                if (!compatible) break :blk null;
                break :blk try (PhysicalColumn{ .vector = vector, .selection = v.page.selection, .kind = definition.type, .element = definition.element_type }).coerce(a, definition.type, definition.element_type);
            },
            else => null,
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
            fn physical(raw: *anyopaque, alloc: A, column: usize) !?PhysicalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                const source = (try view.source.physicalColumn(alloc, column)) orelse return null;
                return try source.select(alloc, view.selection);
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

            fn identity(raw: *anyopaque, row_index: usize, column: usize) !?u64 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.dictionaryIdentity(view.selection[row_index], column);
            }
        };
        for (selection) |index| if (index >= self.len()) return error.InvalidSqlBackendResponse;
        const view = try a.create(View);
        view.* = .{ .source = self, .selection = selection };
        return .{ .reader = .{ .ptr = view, .count = selection.len, .width = self.width(), .read = View.read, .read_physical = View.physical, .read_dictionary = View.dictionary, .read_identity = View.identity } };
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

test "SQL physical views decline empty derived producers with retained schema names" {
    const source: Batch = .{ .rows = &.{} };
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer }};
    const batch: Batch = .{ .columns = .{ .definitions = &definitions, .page = .{
        .native = .{ .values = &source, .names = &.{"n"} },
        .selection = &.{},
    } } };
    var none = std.heap.FixedBufferAllocator.init(&.{});
    try std.testing.expect((try batch.physicalColumn(none.allocator(), 0)) == null);
}

test "SQL physical retained and spilled batches expose native columns and exact NULL lanes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    _ = try store.append(&.{ scalar.Datum.json(.{ .integer = 7 }), scalar.Datum.json(.{ .string = "seven" }) });
    _ = try store.append(&.{ .{}, .{} });
    _ = try store.append(&.{ scalar.Datum.json(.{ .integer = -2 }), scalar.Datum.json(.{ .string = "two" }) });
    const retained: Batch = .{ .retained = .{ .store = &store, .begin = 1, .count = 2 } };
    for (0..2) |column| {
        const physical = (try retained.physicalColumn(ca, column)).?;
        try physical.validate();
        for (0..2) |index| try std.testing.expectEqualDeep(try retained.cell(ca, index, column), try physical.cell(index));
    }
    const spill = @import("spill.zig");
    const rows = [_]@import("operators.zig").Row{
        .{ .values = &.{ scalar.Datum.json(.{ .integer = 7 }), scalar.Datum.json(.{ .string = "seven" }) }, .keys = &.{scalar.Datum.json(.{ .integer = 3 })}, .ordinal = 0 },
        .{ .values = &.{ .{}, .{} }, .keys = &.{.{}}, .ordinal = 1 },
        .{ .values = &.{ scalar.Datum.json(.{ .integer = -2 }), scalar.Datum.json(.{ .string = "two" }) }, .keys = &.{scalar.Datum.json(.{ .integer = -1 })}, .ordinal = 2 },
    };
    const bytes = try spill.encodeColumnarBlockAlloc(a, &rows, 4096);
    defer a.free(bytes);
    var block: spill.Sequential.OwnedBlock = .{ .a = a, .arena = std.heap.ArenaAllocator.init(a), .encoded = try spill.decodeColumnarBlockInArena(ca, bytes, 4096) };
    defer block.arena.deinit();
    for ([_]bool{ false, true }) |keys| {
        const batch = try block.batch(ca, keys);
        for (0..batch.width()) |column| {
            const physical = (try batch.physicalColumn(ca, column)).?;
            try physical.validate();
            for (0..batch.len()) |index| try std.testing.expectEqualDeep(try batch.cell(ca, index, column), try physical.cell(index));
        }
    }
}

test "SQL physical narrow widths preserve selected coercion and REAL rounding" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const integers: PhysicalColumn = .{ .kind = .integer, .vector = .{ .name = "n", .values = .{ .i64 = &.{ 32767, 32768, std.math.maxInt(i64) } } }, .selection = &.{0} };
    const narrow = (try integers.coerce(ca, .integer, .int16)).?;
    try narrow.validate();
    var overflow = integers;
    overflow.selection = &.{1};
    try std.testing.expectError(error.SqlNumericOutOfRange, overflow.coerce(ca, .integer, .int16));
    const numbers: PhysicalColumn = .{ .kind = .number, .element = .float64, .vector = .{ .name = "r", .values = .{ .f64 = &.{ 16777217, 1e-50, -0.0, std.math.inf(f64), std.math.nan(f64) } } }, .selection = &.{ 0, 2, 3, 4 } };
    const real = (try numbers.coerce(ca, .number, .float32)).?;
    try std.testing.expectEqual(@as(f64, 16777216), (try real.cell(0)).value.float);
    try std.testing.expect(std.math.signbit((try real.cell(1)).value.float));
    try std.testing.expect(std.math.isInf((try real.cell(2)).value.float));
    try std.testing.expect(std.math.isNan((try real.cell(3)).value.float));
    var underflow = numbers;
    underflow.selection = &.{1};
    try std.testing.expectError(error.SqlNumericOutOfRange, underflow.coerce(ca, .number, .float32));
}
