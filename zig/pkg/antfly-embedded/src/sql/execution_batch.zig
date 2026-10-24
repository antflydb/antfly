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
const Work = @import("operator_work.zig");
pub const ReadControl = Work.Control;
/// A borrowed physical column plus its logical row mapping. The declaration
/// proves scalar boundary coercion is an identity; unsupported coercions decline.
pub const PhysicalColumn = struct {
    vector: @import("../storage/rowsource/types.zig").ColumnVector,
    selection: []const usize = &.{},
    /// Contiguous borrowed coordinates need no per-lane identity array.
    range: ?struct { begin: usize = 0, count: usize } = null,
    kind: @import("ast.zig").ColumnType,
    element: ?@import("array_value.zig").ElementType = null,
    pub fn len(self: @This()) usize {
        return if (self.range) |span| span.count else self.selection.len;
    }
    pub fn rowAt(self: @This(), index: usize) usize {
        std.debug.assert(index < self.len());
        return if (self.range) |span| span.begin + index else self.selection[index];
    }
    pub fn slice(self: @This(), begin: usize, end: usize) @This() {
        std.debug.assert(begin <= end and end <= self.len());
        var view = self;
        if (self.range) |span| view.range = .{ .begin = span.begin + begin, .count = end - begin } else view.selection = self.selection[begin..end];
        return view;
    }
    pub fn select(self: @This(), a: A, selected: []const usize) !@This() {
        for (selected) |index| if (index >= self.len()) return error.InvalidSqlBackendResponse;
        var view = self;
        view.range = null;
        if (selected.len == 0) {
            view.selection = &.{};
            return view;
        }
        const first = self.rowAt(selected[0]);
        const contiguous = for (selected, 0..) |index, lane| {
            if (self.rowAt(index) != first +| lane) break false;
        } else true;
        if (contiguous) {
            view.selection = &.{};
            view.range = .{ .begin = first, .count = selected.len };
        } else if (self.range != null and self.range.?.begin == 0) {
            view.selection = selected;
        } else {
            const rows = try a.alloc(usize, selected.len);
            for (rows, selected) |*out, index| out.* = self.rowAt(index);
            view.selection = rows;
        }
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
            const values = try a.alloc(f64, self.len());
            errdefer a.free(values);
            const nulls = try a.alloc(u8, values.len);
            errdefer a.free(nulls);
            for (values, nulls, 0..) |*value, *flag, i| {
                const row = self.rowAt(i);
                flag.* = @intFromBool(self.vector.nulls.isNull(row));
                const number = if (flag.* != 0) 0 else switch (self.vector.values) {
                    .f64 => |v| v[row],
                    .dictionary_f64 => |v| v.at(row),
                    else => return error.InvalidSqlBackendResponse,
                };
                value.* = try @import("builtin_cast.zig").floatValue(f32, .{ .float = number });
            }
            view.vector = .{ .name = self.vector.name, .values = .{ .f64 = values }, .nulls = .{ .bytes = nulls } };
            view.selection = &.{};
            view.range = .{ .count = values.len };
        }
        return view;
    }
    pub fn cell(self: @This(), index: usize) !scalar.Datum {
        if (index >= self.len()) return error.InvalidSqlBackendResponse;
        const row = self.rowAt(index);
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
        const row_index = self.rowAt(index);
        if (self.vector.nulls.isNull(row_index)) return @sizeOf(scalar.Datum);
        return @sizeOf(scalar.Datum) + switch (self.vector.values) {
            .bytes => |v| v[row_index].len,
            .dictionary_bytes => |v| v.at(row_index).len,
            else => @as(usize, 0),
        };
    }
    pub fn dictionaryIdentity(self: @This(), index: usize) ?u64 {
        const row_index = self.rowAt(index);
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
        if (!compatible or (self.range != null and self.selection.len != 0)) return error.InvalidSqlBackendResponse;
        self.vector.nulls.validate(self.vector.rowCount()) catch return error.InvalidSqlBackendResponse;
        if (self.range) |span| if (span.begin > self.vector.rowCount() or span.count > self.vector.rowCount() - span.begin) return error.InvalidSqlBackendResponse;
        for (0..self.len()) |lane| {
            const row = self.rowAt(lane);
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
/// Fixed decimal views are opt-in: primitive consumers keep their established
/// physical contract, while exact kernels borrow coefficients and selected rows.
pub const DecimalColumn = struct {
    physical: PhysicalColumn,
    /// Empty means the vector's uniform scale; otherwise indexed in the
    /// physical coordinate domain, including duplicate outer selections.
    scales: []const u16 = &.{},
    pub fn scaleAt(self: @This(), index: usize) !u16 {
        const row_index = self.physical.rowAt(index);
        const scale = if (self.scales.len == 0) (std.math.cast(u16, self.physical.vector.values.decimal128.scale) orelse return error.InvalidDecimal128) else scale: {
            if (row_index >= self.scales.len) return error.InvalidSqlBackendResponse;
            break :scale self.scales[row_index];
        };
        if (scale > 38) return error.InvalidDecimal128;
        return scale;
    }
    pub fn cell(self: @This(), index: usize, fixed: *@import("numeric_value.zig").ScaledInteger, value: *@import("numeric_value.zig").Value) !scalar.Datum {
        const row = self.physical.rowAt(index);
        if (self.physical.vector.nulls.isNull(row)) return .{};
        const decimal = self.physical.vector.values.decimal128;
        const scale = try self.scaleAt(index);
        try @import("../common/decimal128.zig").validateCoefficient(decimal.values[row], decimal.precision, scale);
        fixed.* = try @import("numeric_value.zig").ScaledInteger.init(decimal.values[row], scale);
        value.* = fixed.view();
        return scalar.Datum.typedNumeric(value);
    }
};
/// One stable scratch slot per key column, reused across a census/hash window.
/// Escaping keys are copied by the existing typed group/join stores.
pub const NumericScratch = struct {
    views: []?DecimalColumn,
    fixed: []@import("numeric_value.zig").ScaledInteger,
    numbers: []@import("numeric_value.zig").Value,
    pub fn init(a: A, columns: []const Batch) !NumericScratch {
        var present = false;
        for (columns) |column| if (try column.decimalColumnSelected(a, 0, null) != null) {
            present = true;
            break;
        };
        if (!present) return .{ .views = &.{}, .fixed = &.{}, .numbers = &.{} };
        const views = try a.alloc(?DecimalColumn, columns.len);
        errdefer a.free(views);
        const fixed = try a.alloc(@import("numeric_value.zig").ScaledInteger, columns.len);
        errdefer a.free(fixed);
        const numbers = try a.alloc(@import("numeric_value.zig").Value, columns.len);
        errdefer a.free(numbers);
        for (columns, views) |column, *view| view.* = try column.decimalColumnSelected(a, 0, null);
        return .{ .views = views, .fixed = fixed, .numbers = numbers };
    }
    pub fn cell(self: *NumericScratch, a: A, columns: []const Batch, row: usize, column: usize) !scalar.Datum {
        return if (self.views.len != 0 and self.views[column] != null) self.views[column].?.cell(row, &self.fixed[column], &self.numbers[column]) else columns[column].cell(a, row, 0);
    }
};
/// Validated frozen JSON tokens, distinct from canonical JSON text. V1 tags
/// preserve exact scalar representations. Bytes borrow the producer lease.
pub const FrozenJson = struct { version: enum { v1 } = .v1, bytes: []const u8, lossless_text: bool };
pub const Batch = union(enum) {
    /// One expression over page-local dictionary entries, including a NULL lane.
    dictionary: struct { values: []const scalar.Datum, indices: []const u32 },
    /// Stable retained columns; ownership belongs to the enclosing lease.
    retained: struct { store: *const @import("typed_store.zig").Store, begin: usize = 0, count: usize },
    /// Operator-specific column access without constructing a row matrix.
    reader: struct { ptr: *anyopaque, read_controlled: ?*const fn (*anyopaque, A, usize, usize, ?Work.Control) anyerror!scalar.Datum = null, read: *const fn (*anyopaque, A, usize, usize) anyerror!scalar.Datum, free_owned: ?*const fn (*anyopaque, A) void = null, read_decimal_selected: ?*const fn (*anyopaque, A, usize, ?[]const usize) anyerror!?DecimalColumn = null, read_physical: ?*const fn (*anyopaque, A, usize) anyerror!?PhysicalColumn = null, read_physical_selected: ?*const fn (*anyopaque, A, usize, ?[]const usize) anyerror!?PhysicalColumn = null, cohort: ?struct { source: *const Batch, positions: []const usize } = null, read_frozen: ?*const fn (*anyopaque, usize, usize) anyerror!?FrozenJson = null, read_json: ?*const fn (*anyopaque, usize, usize) anyerror!?[]const u8 = null, read_dictionary: ?*const fn (*anyopaque, A, usize) anyerror!?Batch = null, read_identity: ?*const fn (*anyopaque, usize, usize) anyerror!?u64 = null, count: usize, width: usize },
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
        const View = struct {
            source: *const Batch,
            column: usize,
            fn decimalSelected(raw: *anyopaque, alloc: A, _: usize, selection: ?[]const usize) !?DecimalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.decimalColumnSelected(alloc, view.column, selection);
            }
            fn physicalView(raw: *anyopaque, alloc: A, _: usize) !?PhysicalColumn {
                return physicalSelected(raw, alloc, 0, null);
            }
            fn physicalSelected(raw: *anyopaque, alloc: A, _: usize, selection: ?[]const usize) !?PhysicalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.physicalColumnSelected(alloc, view.column, selection);
            }
            fn read(raw: *anyopaque, alloc: A, row_index: usize, _: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.cell(alloc, row_index, view.column);
            }
            fn readControlled(raw: *anyopaque, alloc: A, row_index: usize, _: usize, control: ?Work.Control) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.cellControlled(alloc, row_index, view.column, control);
            }

            fn dictionary(raw: *anyopaque, alloc: A, _: usize) !?Batch {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.dictionaryColumn(alloc, view.column);
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
        return .{ .reader = .{ .ptr = view, .read = View.read, .read_controlled = View.readControlled, .read_physical = View.physicalView, .read_decimal_selected = View.decimalSelected, .read_physical_selected = View.physicalSelected, .read_dictionary = View.dictionary, .read_identity = View.identity, .read_json = View.json, .read_frozen = View.frozen, .count = self.len(), .width = 1 } };
    }
    /// Keep FILTER cohorts sparse. Aggregate consumers gather group IDs for
    /// accepted lanes directly; scalar fallback observes SQL NULL elsewhere.
    pub fn mask(self: *const Batch, a: A, positions: []const usize, count: usize) !Batch {
        if (positions.len != self.len() or self.width() != 1) return error.InvalidSqlBackendResponse;
        for (positions, 0..) |position, i| if (position >= count or (i != 0 and position <= positions[i - 1])) return error.InvalidSqlBackendResponse;
        const View = struct {
            source: *const Batch,
            positions: []const usize,
            fn read(raw: *anyopaque, alloc: A, row_index: usize, _: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                var lo: usize = 0;
                var hi = view.positions.len;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (view.positions[mid] < row_index) lo = mid + 1 else hi = mid;
                }
                return if (lo < view.positions.len and view.positions[lo] == row_index) view.source.cell(alloc, lo, 0) else .{};
            }
        };
        const view = try a.create(View);
        view.* = .{ .source = self, .positions = positions };
        return .{ .reader = .{ .ptr = view, .read = View.read, .count = count, .width = 1, .cohort = .{ .source = self, .positions = positions } } };
    }
    /// Borrow independent expression columns without a Datum row matrix.
    pub fn combine(a: A, columns: []const Batch, count: usize) !Batch {
        for (columns) |column| if (column.width() != 1 or column.len() != count) return error.InvalidSqlBackendResponse;
        const View = struct {
            columns: []const Batch,
            fn read(raw: *anyopaque, alloc: A, row_index: usize, column: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.columns[column].cell(alloc, row_index, 0);
            }
            fn decimalSelected(raw: *anyopaque, alloc: A, column: usize, selection: ?[]const usize) !?DecimalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.columns[column].decimalColumnSelected(alloc, 0, selection);
            }
            fn physical(raw: *anyopaque, alloc: A, column: usize, selection: ?[]const usize) !?PhysicalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.columns[column].physicalColumnSelected(alloc, 0, selection);
            }
            fn dictionary(raw: *anyopaque, alloc: A, column: usize) !?Batch {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.columns[column].dictionaryColumn(alloc, 0);
            }
            fn json(raw: *anyopaque, row_index: usize, column: usize) !?[]const u8 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.columns[column].encodedJson(row_index, 0);
            }
            fn frozen(raw: *anyopaque, row_index: usize, column: usize) !?FrozenJson {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.columns[column].frozenJson(row_index, 0);
            }
            fn identity(raw: *anyopaque, row_index: usize, column: usize) !?u64 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.columns[column].dictionaryIdentity(row_index, 0);
            }
        };
        const view = try a.create(View);
        view.* = .{ .columns = columns };
        return .{ .reader = .{ .ptr = view, .read = View.read, .read_decimal_selected = View.decimalSelected, .read_physical_selected = View.physical, .read_dictionary = View.dictionary, .read_identity = View.identity, .read_json = View.json, .read_frozen = View.frozen, .width = columns.len, .count = count } };
    }
    /// Normalize a stable reader once; borrowed views are reused by hashing,
    /// admission and reduction until the enclosing block/window is released.
    pub fn preparePhysical(self: *const Batch, a: A) !Batch {
        const View = struct {
            source: *const Batch,
            columns: []const ?PhysicalColumn,
            fn read(raw: *anyopaque, alloc: A, row_index: usize, column: usize) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.cell(alloc, row_index, column);
            }
            fn decimalSelected(raw: *anyopaque, alloc: A, column: usize, selection: ?[]const usize) !?DecimalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.decimalColumnSelected(alloc, column, selection);
            }
            fn physical(raw: *anyopaque, alloc: A, column: usize, selection: ?[]const usize) !?PhysicalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                const native = view.columns[column] orelse return null;
                return if (selection) |indices| try native.select(alloc, indices) else native;
            }
            fn json(raw: *anyopaque, row_index: usize, column: usize) !?[]const u8 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.encodedJson(row_index, column);
            }
            fn frozen(raw: *anyopaque, row_index: usize, column: usize) !?FrozenJson {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.frozenJson(row_index, column);
            }
            fn identity(raw: *anyopaque, row_index: usize, column: usize) !?u64 {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return if (view.columns[column]) |native| native.dictionaryIdentity(row_index) else view.source.dictionaryIdentity(row_index, column);
            }
        };
        const columns = try a.alloc(?PhysicalColumn, self.width());
        for (columns, 0..) |*column, index| column.* = try self.physicalColumn(a, index);
        const view = try a.create(View);
        view.* = .{ .source = self, .columns = columns };
        return .{ .reader = .{ .ptr = view, .count = self.len(), .width = self.width(), .read = View.read, .read_decimal_selected = View.decimalSelected, .read_physical_selected = View.physical, .read_identity = View.identity, .read_json = View.json, .read_frozen = View.frozen } };
    }
    fn selectedRows(a: A, rows: []const usize, selection: ?[]const usize) ![]const usize {
        const indices = selection orelse return rows;
        const selected = try a.alloc(usize, indices.len);
        errdefer a.free(selected);
        for (indices, selected) |index, *row_index| {
            if (index >= rows.len) return error.InvalidSqlBackendResponse;
            row_index.* = rows[index];
        }
        return selected;
    }
    /// Transfer owned boolean vectors into the existing native reader contract.
    pub fn fromBoolean(a: A, values: []bool, nulls: []u8) !Batch {
        if (values.len != nulls.len) return error.InvalidSqlBackendResponse;
        const View = struct {
            values: []bool,
            nulls: []u8,
            fn column(self: *@This()) PhysicalColumn {
                return .{ .vector = .{ .name = "", .values = .{ .bool = self.values }, .nulls = .{ .bytes = self.nulls } }, .range = .{ .count = self.values.len }, .kind = .boolean, .element = .boolean };
            }
            fn read(raw: *anyopaque, _: A, row_index: usize, _: usize) !scalar.Datum {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.column().cell(row_index);
            }
            fn physical(raw: *anyopaque, alloc: A, _: usize, selection: ?[]const usize) !?PhysicalColumn {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return if (selection) |rows| try self.column().select(alloc, rows) else self.column();
            }
            fn free(raw: *anyopaque, alloc: A) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                alloc.free(self.values);
                alloc.free(self.nulls);
                alloc.destroy(self);
            }
        };
        const view = try a.create(View);
        view.* = .{ .values = values, .nulls = nulls };
        return .{ .reader = .{ .ptr = view, .read = View.read, .read_physical_selected = View.physical, .free_owned = View.free, .count = values.len, .width = 1 } };
    }
    /// Transfer exact coefficient vectors; primitive/floating consumers do not
    /// implicitly acquire this trait. Scalar boundaries allocate canonical limbs
    /// only for values they actually consume.
    pub fn fromDecimal(a: A, values: []i128, nulls: []u8, scale: u16) !Batch {
        return fromDecimalScales(a, values, nulls, scale, &.{});
    }
    /// Per-lane display scales retain exact division results without limb cells.
    /// Uniform selected cohorts still expose a borrowed decimal vector.
    pub fn fromDecimalScales(a: A, values: []i128, nulls: []u8, scale: u16, scales: []const u16) !Batch {
        if (values.len != nulls.len or scale > 38 or (scales.len != 0 and scales.len != values.len)) return error.InvalidSqlBackendResponse;
        for (scales) |value| if (value > 38) return error.InvalidSqlBackendResponse;
        const View = struct {
            values: []i128,
            nulls: []u8,
            scale: u16,
            scales: []const u16,
            fn column(self: *@This()) DecimalColumn {
                return .{ .scales = self.scales, .physical = .{ .vector = .{ .name = "", .values = .{ .decimal128 = .{ .values = self.values, .precision = 38, .scale = self.scale } }, .nulls = .{ .bytes = self.nulls } }, .range = .{ .count = self.values.len }, .kind = .number, .element = .numeric } };
            }
            fn read(raw: *anyopaque, alloc: A, row_index: usize, _: usize) !scalar.Datum {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.nulls[row_index] != 0) return .{};
                const number = @import("numeric_value.zig");
                var context: number.Context = .{ .alloc = alloc };
                var value = try number.fromScaledInteger(&context, self.values[row_index], if (self.scales.len == 0) self.scale else self.scales[row_index]);
                errdefer value.deinit();
                const owned = try alloc.create(number.Value);
                owned.* = value.value;
                return scalar.Datum.typedNumeric(owned);
            }
            fn decimal(raw: *anyopaque, alloc: A, _: usize, selection: ?[]const usize) !?DecimalColumn {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var column_ = self.column();
                if (selection) |selected| column_.physical = try column_.physical.select(alloc, selected);
                return column_;
            }
            fn free(raw: *anyopaque, alloc: A) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                alloc.free(self.values);
                alloc.free(self.nulls);
                alloc.free(self.scales);
                alloc.destroy(self);
            }
        };
        const view = try a.create(View);
        view.* = .{ .values = values, .nulls = nulls, .scale = scale, .scales = scales };
        return .{ .reader = .{ .ptr = view, .read = View.read, .read_decimal_selected = View.decimal, .free_owned = View.free, .count = values.len, .width = 1 } };
    }
    pub fn decimalColumnSelected(self: Batch, a: A, column: usize, selection: ?[]const usize) anyerror!?DecimalColumn {
        if (column >= self.width()) return error.InvalidSqlBackendResponse;
        if (selection) |rows| for (rows) |row_index| if (row_index >= self.len()) return error.InvalidSqlBackendResponse;
        return switch (self) {
            .retained => |v| blk: {
                const source = (try v.store.decimalColumn(column, v.begin, v.count)) orelse break :blk null;
                break :blk if (selection) |rows| .{ .physical = try source.physical.select(a, rows), .scales = source.scales } else source;
            },
            .reader => |v| if (v.read_decimal_selected) |read| read(v.ptr, a, column, selection) else null,
            .mapped => |v| blk: {
                if (column >= v.types.len or v.types[column].kind != .number or v.types[column].element_type != .numeric or v.types[column].numeric_modifier != null or v.ordinals[column] == std.math.maxInt(usize)) break :blk null;
                break :blk try v.source.decimalColumnSelected(a, v.ordinals[column], try selectedRows(a, v.selection, selection));
            },
            .columns => |v| blk: {
                const definition = v.definitions[column];
                if (definition.type != .number or definition.element_type != .numeric or definition.numeric_modifier != null) break :blk null;
                const rows = try selectedRows(a, v.page.selection, selection);
                if (v.page.native) |native| {
                    for (native.names, 0..) |name, ordinal| if (std.mem.eql(u8, name, definition.name)) break :blk try native.values.decimalColumnSelected(a, ordinal, rows);
                    break :blk null;
                }
                const vector = v.page.batch.findColumn(definition.name) orelse break :blk null;
                if (vector.values != .decimal128) break :blk null;
                try vector.nulls.validate(vector.rowCount());
                for (rows) |row_index| if (row_index >= vector.rowCount()) return error.InvalidSqlBackendResponse;
                break :blk .{ .physical = .{ .vector = vector, .selection = rows, .kind = .number, .element = .numeric } };
            },
            else => null,
        };
    }
    pub fn physicalColumn(self: Batch, a: A, column: usize) anyerror!?PhysicalColumn {
        return self.physicalColumnSelected(a, column, null);
    }
    /// Push outer selections to the producer before any intermediate coercion.
    /// A reader exposing only read_physical promises already normalized values;
    /// coercing readers must implement read_physical_selected instead.
    pub fn physicalColumnSelected(self: Batch, a: A, column: usize, selection: ?[]const usize) anyerror!?PhysicalColumn {
        if (column >= self.width()) return error.InvalidSqlBackendResponse;
        if (selection) |indices| for (indices) |index| {
            if (index >= self.len()) return error.InvalidSqlBackendResponse;
        };
        if (self.len() == 0 or (selection != null and selection.?.len == 0)) return null;
        return switch (self) {
            .reader => |v| blk: {
                if (v.read_physical_selected) |read| break :blk try read(v.ptr, a, column, selection);
                const source = if (v.read_physical) |read| (try read(v.ptr, a, column)) orelse break :blk null else break :blk null;
                break :blk if (selection) |indices| try source.select(a, indices) else source;
            },
            .retained => |v| blk: {
                const source = (try v.store.physicalColumn(a, column, v.begin, v.count)) orelse break :blk null;
                break :blk if (selection) |indices| try source.select(a, indices) else source;
            },
            .mapped => |v| blk: {
                if (v.ordinals[column] == std.math.maxInt(usize) or column >= v.types.len) break :blk null;
                const selected = try selectedRows(a, v.selection, selection);
                const source = (try v.source.physicalColumnSelected(a, v.ordinals[column], selected)) orelse break :blk null;
                break :blk try source.coerce(a, v.types[column].kind orelse return null, v.types[column].element_type);
            },
            .columns => |v| blk: {
                const definition = v.definitions[column];
                const selected = try selectedRows(a, v.page.selection, selection);
                if (v.page.native) |native| {
                    for (native.names, 0..) |name, ordinal| if (std.mem.eql(u8, name, definition.name)) {
                        const source = (try native.values.physicalColumnSelected(a, ordinal, selected)) orelse break :blk null;
                        break :blk try source.coerce(a, definition.type, definition.element_type);
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
                break :blk try (PhysicalColumn{ .vector = vector, .selection = selected, .kind = definition.type, .element = definition.element_type }).coerce(a, definition.type, definition.element_type);
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
            fn readControlled(raw: *anyopaque, alloc: A, row_index: usize, column: usize, control: ?Work.Control) !scalar.Datum {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.cellControlled(alloc, view.selection[row_index], column, control);
            }

            fn decimalSelected(raw: *anyopaque, alloc: A, column: usize, outer_selection: ?[]const usize) !?DecimalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.decimalColumnSelected(alloc, column, try selectedRows(alloc, view.selection, outer_selection));
            }
            fn physical(raw: *anyopaque, alloc: A, column: usize) !?PhysicalColumn {
                return physicalSelected(raw, alloc, column, null);
            }
            fn physicalSelected(raw: *anyopaque, alloc: A, column: usize, outer_selection: ?[]const usize) !?PhysicalColumn {
                const view: *@This() = @ptrCast(@alignCast(raw));
                return view.source.physicalColumnSelected(alloc, column, try selectedRows(alloc, view.selection, outer_selection));
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
        return .{ .reader = .{ .ptr = view, .count = selection.len, .width = self.width(), .read = View.read, .read_controlled = View.readControlled, .read_physical = View.physical, .read_decimal_selected = View.decimalSelected, .read_physical_selected = View.physicalSelected, .read_dictionary = View.dictionary, .read_identity = View.identity, .read_json = View.json, .read_frozen = View.frozen } };
    }
    pub fn cell(self: Batch, a: A, index: usize, column: usize) anyerror!scalar.Datum {
        return self.cellControlled(a, index, column, null);
    }
    pub fn cellControlled(self: Batch, a: A, index: usize, column: usize, control: ?Work.Control) anyerror!scalar.Datum {
        try Work.poll(control);
        if (index >= self.len() or column >= self.width()) return error.InvalidSqlBackendResponse;
        return switch (self) {
            .dictionary => |v| if (v.indices[index] < v.values.len) v.values[v.indices[index]] else error.InvalidSqlBackendResponse,
            .retained => |v| v.store.cellControlled(a, v.begin + index, column, control),
            .reader => |v| blk: {
                if (v.read_controlled) |read| break :blk try read(v.ptr, a, index, column, control);
                // Representation capabilities cannot replace a reader's
                // validation, policy checks or precise row errors.
                const value = try v.read(v.ptr, a, index, column);
                try Work.poll(control);
                break :blk value;
            },
            .rows => |v| v[index][column],
            .vectors => |v| v.values[column][index],
            .mapped => |v| blk: {
                if (v.ordinals[column] == std.math.maxInt(usize)) break :blk .{};
                const value = try v.source.cellControlled(a, v.selection[index], v.ordinals[column], control);
                if (column >= v.types.len) return error.InvalidSqlBackendResponse;
                const type_ = v.types[column];
                break :blk try @import("describe.zig").coerceDatum(a, value, type_.kind orelse return error.InvalidSqlBackendResponse, type_.element_type);
            },
            .columns => |v| blk: {
                const definition = v.definitions[column];
                const value = try v.page.typedCellControlled(a, index, definition.name, definition.element_type == .numeric, control);
                break :blk try @import("describe.zig").coerceDatum(a, value, definition.type, definition.element_type);
            },
        };
    }
    pub fn row(self: Batch, a: A, index: usize) ![]const scalar.Datum {
        return self.rowControlled(a, index, null);
    }
    pub fn rowControlled(self: Batch, a: A, index: usize, control: ?Work.Control) ![]const scalar.Datum {
        try Work.poll(control);
        if (index >= self.len()) return error.InvalidSqlBackendResponse;
        if (self == .rows) return self.rows[index];
        const values = try a.alloc(scalar.Datum, self.width());
        for (values, 0..) |*value, column| value.* = try self.cellControlled(a, index, column, control);
        return values;
    }
};

/// Operator-owned descriptors; decoded payloads and expression results belong
/// to the caller's evaluation region. Never reset that region with the row.
/// Each operator retains at most one vector, bounded by its bound schema width.
pub const ScalarRow = struct {
    values: std.ArrayList(scalar.Datum) = .empty,

    pub fn deinit(self: *ScalarRow, a: A) void {
        self.values.deinit(a);
    }

    pub fn prepare(self: *ScalarRow, owner: A, width: usize) ![]scalar.Datum {
        if (self.values.items.len != width) {
            try self.values.resize(owner, width);
            @memset(self.values.items, .{});
        }
        return self.values.items;
    }

    pub fn fill(self: *ScalarRow, owner: A, payload: A, program: *const scalar.Program, batch: Batch, row_index: usize) ![]const scalar.Datum {
        _ = try self.prepare(owner, batch.width());
        // Binding already deduplicates dependencies. The evaluator reads only
        // these slots, so other slots need neither decoding nor clearing.
        for (program.required_columns) |column| {
            if (column >= self.values.items.len) return error.InvalidSqlBackendResponse;
            self.values.items[column] = try batch.cell(payload, row_index, column);
        }
        return self.values.items;
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

test "SQL physical views compose selections before coercion" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const definitions = [_]scalar.Column{.{ .name = "n", .type = .integer, .element_type = .int16 }};
    const source: Batch = .{ .columns = .{ .definitions = &definitions, .page = .{
        .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } }, .columns = &.{.{ .name = "n", .values = .{ .i64 = &.{ 1, 32768 } } }} },
        .selection = &.{ 0, 1 },
    } } };
    const views = [_]Batch{
        .{ .mapped = .{ .source = &source, .ordinals = &.{0}, .types = &.{.{ .kind = .integer, .element_type = .int16 }}, .selection = &.{0} } },
        try source.select(a, &.{0}),
        .{ .columns = .{ .definitions = &definitions, .page = .{ .native = .{ .values = &source, .names = &.{"n"} }, .selection = &.{0} } } },
    };
    for (views) |view| {
        try std.testing.expectEqual(@as(i64, 1), (try view.cell(a, 0, 0)).value.integer);
        const physical = (try view.physicalColumn(a, 0)).?;
        try physical.validate();
        try std.testing.expectEqual(@as(i64, 1), (try physical.cell(0)).value.integer);
    }
}

test "SQL physical retained views borrow packed nulls and compact selected ranges" {
    const a = std.testing.allocator;
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    for (0..130) |row_index| _ = try store.append(&.{if (row_index == 64) scalar.Datum{} else scalar.Datum.json(.{ .integer = @intCast(row_index) })});
    var no_memory: @import("memory_budget.zig") = .{ .backing = a, .limit = 0 };
    const view = (try store.physicalColumn(no_memory.allocator(), 0, 63, 4)).?;
    try view.validate();
    try std.testing.expect(view.range != null and view.selection.len == 0 and view.vector.nulls.bits != null);
    try std.testing.expect((try view.cell(1)).sql_null);
    const owned_nulls = try view.vector.nulls.dupeBytes(a, 4);
    defer a.free(owned_nulls);
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 0, 0 }, owned_nulls);
    const selected = try view.select(no_memory.allocator(), &.{ 3, 1, 0, 1 });
    try selected.validate();
    try std.testing.expect((try selected.cell(1)).sql_null and (try selected.cell(3)).sql_null);
    try std.testing.expectEqual(@as(i64, 66), (try selected.cell(0)).value.integer);
    const part = view.slice(1, 3);
    try std.testing.expect((try part.cell(0)).sql_null);
    try std.testing.expectEqual(@as(usize, 0), no_memory.live);
    var invalid = view;
    invalid.range = .{ .begin = 4, .count = 1 };
    try std.testing.expectError(error.InvalidSqlBackendResponse, invalid.validate());
    invalid = view;
    invalid.vector.nulls.bits.?.offset = 192;
    try std.testing.expectError(error.InvalidSqlBackendResponse, invalid.validate());
}

test "SQL scalar row descriptors reuse bounded storage without retiring expression results" {
    const Fixture = struct {
        fn read(_: *anyopaque, _: A, index: usize, column: usize) !scalar.Datum {
            if (column != 127) return error.UnexpectedColumn;
            return scalar.Datum.json(.{ .string = if (index % 2 == 0) "abcdef" else "ghijkl" });
        }
        fn run(a: A) !void {
            var names: [128][16]u8 = undefined;
            var columns: [128]scalar.Column = undefined;
            for (&columns, &names, 0..) |*column, *name, index| column.* = .{ .name = try std.fmt.bufPrint(name, "unused{d}", .{index}), .type = .string };
            columns[127] = .{ .name = "k", .type = .string };
            var compiled = try @import("compiler.zig").compileScalar(a, "reverse(k)", .{});
            defer compiled.deinit();
            var program = try scalar.bind(a, compiled.expression, &columns, &.{}, .{});
            defer program.deinit();
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 64 << 10 };
            var region = std.heap.ArenaAllocator.init(budget.allocator());
            defer region.deinit();
            var scratch: ScalarRow = .{};
            defer scratch.deinit(budget.allocator());
            var dummy: u8 = 0;
            const batch: Batch = .{ .reader = .{ .ptr = &dummy, .read = read, .count = 256, .width = 128 } };
            var first: ?scalar.Datum = null;
            for (0..batch.len()) |index| {
                const values = try scratch.fill(budget.allocator(), region.allocator(), &program, batch, index);
                const result = try program.evaluate(region.allocator(), values, &.{}, .{});
                if (first == null) first = result;
                try std.testing.expectEqualStrings(if (index % 2 == 0) "fedcba" else "lkjihg", result.value.string);
            }
            try std.testing.expectEqualStrings("fedcba", first.?.value.string);
            try std.testing.expect(budget.peak < 32 << 10);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
