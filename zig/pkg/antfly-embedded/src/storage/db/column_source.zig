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

//! Immutable selected column pages retained through final response encoding.
//! A row lease shares vectors and dictionaries; it never owns a JSON row tree.
const std = @import("std");
const A = std.mem.Allocator;
const rows = @import("../rowsource/types.zig");
const projection = @import("document_query.zig");

pub const Page = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    refs: std.atomic.Value(usize) = .init(1),
    columns: []const rows.ColumnVector,
    len: usize,
    selection: ?[]const usize = null,
    owner: ?rows.ColumnOwner = null,
    native_leases: std.ArrayListUnmanaged(rows.ColumnOwner) = .empty,
    native_builders: ?[]NativeBuilder = null,
    native_allocator: ?A = null,
    native_finished: bool = false,
    pub fn retainedBytes(self: *const Page) usize {
        var total = @sizeOf(Page) +| self.arena.queryCapacity() +| (if (self.owner) |owner| owner.retained_bytes else 0);
        for (self.native_leases.items) |lease| total +|= lease.retained_bytes;
        if (self.native_builders) |builders| {
            total +|= builders.len * @sizeOf(NativeBuilder);
            for (builders) |*builder| total +|= builder.bytes();
        }
        return total;
    }
    pub fn retain(self: *Page) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *Page) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.a;
        if (self.owner) |owner| owner.release();
        for (self.native_leases.items) |lease| lease.release();
        self.native_leases.deinit(a);
        if (self.native_builders) |builders| {
            for (builders) |*builder| builder.deinit(self.native_allocator.?);
            self.native_allocator.?.free(builders);
        }
        self.arena.deinit();
        a.destroy(self);
    }
    pub fn row(self: *Page, index: usize) Row {
        std.debug.assert(index < self.len);
        self.retain();
        return .{ .page = self, .index = if (self.selection) |selection| selection[index] else index };
    }
    /// Mutable only while a page is built. Scalar buffers become the final
    /// physical vectors; only genuinely heterogeneous values need JSON cells.
    const NativeBuilder = struct {
        const Values = union(enum) { none, integer: []i64, float: []f64, bool: []bool, string: [][]const u8, json: []std.json.Value };
        values: Values = .none,
        nulls: []u8 = &.{},
        fn deinit(self: *@This(), a: A) void {
            switch (self.values) {
                .none => {},
                inline else => |v| a.free(v),
            }
            a.free(self.nulls);
        }
        fn bytes(self: *const @This()) usize {
            return self.nulls.len + switch (self.values) {
                .none => @as(usize, 0),
                inline else => |v| v.len * @sizeOf(@typeInfo(@TypeOf(v)).pointer.child),
            };
        }
        fn at(self: *const @This(), cell_index: usize) std.json.Value {
            if (self.nulls[cell_index] != 0) return .null;
            return switch (self.values) {
                .none => .null,
                .json => |v| v[cell_index],
                inline else => |v, tag| @unionInit(std.json.Value, @tagName(tag), v[cell_index]),
            };
        }
        fn read(raw: *const anyopaque, index: usize) !std.json.Value {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (index >= self.nulls.len) return error.RowSourceColumnLengthMismatch;
            return self.at(index);
        }
        fn set(self: *@This(), a: A, cell_index: usize, value: std.json.Value, sql_null: bool) !void {
            if (sql_null) {
                self.nulls[cell_index] = 1;
                return;
            }
            if (self.values == .none) switch (value) {
                inline .integer, .float, .bool, .string => |_, tag| {
                    const T = @FieldType(std.json.Value, @tagName(tag));
                    const buffer = try a.alloc(T, self.nulls.len);
                    const zero: T = if (tag == .string) "" else if (tag == .bool) false else 0;
                    @memset(buffer, zero);
                    self.values = @unionInit(Values, @tagName(tag), buffer);
                },
                else => {
                    const buffer = try a.alloc(std.json.Value, self.nulls.len);
                    @memset(buffer, .null);
                    self.values = .{ .json = buffer };
                },
            };
            const compatible = switch (self.values) {
                .none => unreachable,
                .json => true,
                inline else => |_, tag| std.meta.activeTag(value) == @field(std.meta.Tag(std.json.Value), @tagName(tag)),
            };
            if (!compatible) {
                const buffer = try a.alloc(std.json.Value, self.nulls.len);
                for (buffer, 0..) |*out, i| out.* = self.at(i);
                switch (self.values) {
                    .none => unreachable,
                    inline else => |v| a.free(v),
                }
                self.values = .{ .json = buffer };
            }
            self.nulls[cell_index] = 0;
            switch (self.values) {
                .none => unreachable,
                .json => |v| v[cell_index] = value,
                inline else => |v, tag| v[cell_index] = @field(value, @tagName(tag)),
            }
        }
    };
    /// Build result vectors over retained native payloads. Buffers are admitted
    /// and reclaimable independently of the page's container/payload arena.
    pub fn native(a: A, names: []const []const u8, count: usize) !*Page {
        const self = try a.create(Page);
        self.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = count };
        errdefer self.release();
        const work = @import("../../sql/memory_budget.zig").reclaimingAllocator(a);
        self.native_allocator = work;
        const builders = try work.alloc(NativeBuilder, names.len);
        @memset(builders, .{});
        self.native_builders = builders;
        const pa = self.arena.allocator();
        const columns = try pa.alloc(rows.ColumnVector, names.len);
        for (columns, names, builders) |*column, name, *builder| {
            builder.nulls = try work.alloc(u8, count);
            @memset(builder.nulls, 1);
            column.* = .{ .name = try pa.dupe(u8, name), .values = .{ .encoded = .{ .ptr = builder, .begin = 0, .count = count, .kind = .json, .read = NativeBuilder.read } } };
        }
        self.columns = columns;
        return self;
    }
    pub fn setNative(self: *Page, row_index: usize, column_index: usize, value: std.json.Value) !void {
        std.debug.assert(!self.native_finished and row_index < self.len);
        try self.setNativeCell(row_index, column_index, value, value == .null);
    }
    /// SQL NULL is independent of a non-null JSON scalar containing null.
    pub fn setNativeCell(self: *Page, row_index: usize, column_index: usize, value: std.json.Value, sql_null: bool) !void {
        std.debug.assert(!self.native_finished and row_index < self.len);
        try self.native_builders.?[column_index].set(self.native_allocator.?, row_index, value, sql_null);
    }
    /// Consumes a lease even on allocation failure. No source reader, request
    /// context, or artifact pin is retained: only authenticated decoded bytes.
    pub fn adoptNativeLease(self: *Page, lease: rows.ColumnOwner) !void {
        errdefer lease.release();
        for (self.native_leases.items) |prior| if (prior.ptr == lease.ptr) {
            lease.release();
            return;
        };
        try self.native_leases.append(self.a, lease);
    }
    pub fn finishNative(self: *Page) !void {
        std.debug.assert(!self.native_finished);
        for (@constCast(self.columns), self.native_builders.?) |*column, *builder| {
            column.nulls = .{ .bytes = builder.nulls };
            switch (builder.values) {
                inline .integer, .float, .bool, .string => |values, tag| column.values = @unionInit(rows.ColumnValues, switch (tag) {
                    .integer => "i64",
                    .float => "f64",
                    .bool => "bool",
                    .string => "bytes",
                    else => unreachable,
                }, values),
                else => {},
            }
        }
        self.native_finished = true;
    }
    /// Copy only selected vector slots before the provider advances. Dictionary
    /// entries are gathered once per page, preserving shared string storage.
    pub fn copy(a: A, batch: rows.ColumnBatch, selection: []const usize) !*Page {
        try batch.validate();
        for (selection) |index| if (index >= batch.rowCount()) return error.InvalidSqlBackendResponse;
        return gather(a, batch.columns, selection);
    }
    pub const Position = struct { page: *const Page, row: usize };
    /// Assemble a bounded scatter from independently owned pages. Preserve
    /// native scalar types; page-local dictionaries normalize into compact slots.
    pub fn gatherPositions(a: A, positions: []const Position) !*Page {
        return gatherProjectedPositions(a, positions, null);
    }
    /// Project a scatter once; contiguous tiles borrow the immutable producer
    /// vectors through an explicit page lease instead of copying their payload.
    pub fn gatherProjectedPositions(a: A, positions: []const Position, projection_: ?[]const usize) !*Page {
        if (positions.len == 0 or positions.len > 4096) return error.InvalidSqlBackendResponse;
        const first = positions[0].page;
        const width = if (projection_) |indices| indices.len else first.columns.len;
        for (positions) |position| if (position.row >= position.page.len or position.page.columns.len != first.columns.len or position.page.selection != null) return error.InvalidSqlBackendResponse;
        if (projection_) |indices| for (indices) |ci| if (ci >= first.columns.len) return error.InvalidSqlBackendResponse;
        const contiguous = for (positions, 0..) |position, i| {
            if (position.page != first or position.row != positions[0].row + i) break false;
        } else true;
        if (contiguous and positions.len >= first.len / 4 and first.retainedBytes() <= 8 * 1024 * 1024)
            return borrowRange(a, first, positions[0].row, positions.len, projection_);
        const self = try a.create(Page);
        self.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = positions.len };
        errdefer self.release();
        const pa = self.arena.allocator();
        const columns = try pa.alloc(rows.ColumnVector, width);
        for (columns, 0..) |*out, projected_index| {
            const ci = if (projection_) |indices| indices[projected_index] else projected_index;
            const original = first.columns[ci];
            const nulls = try pa.alloc(u8, positions.len);
            out.* = .{ .name = try pa.dupe(u8, original.name), .values = undefined, .nulls = .{ .bytes = nulls } };
            for (positions, nulls) |position, *flag| {
                const column = position.page.columns[ci];
                if (!std.mem.eql(u8, original.name, column.name)) return error.InvalidSqlBackendResponse;
                flag.* = @intFromBool(column.nulls.isNull(position.row));
            }
            out.values = switch (original.values) {
                .i64, .dictionary_i64 => blk: {
                    const values = try pa.alloc(i64, positions.len);
                    for (positions, values, nulls) |position, *value, flag| value.* = if (flag != 0) 0 else try position.page.columns[ci].integerAt(position.row);
                    break :blk .{ .i64 = values };
                },
                .f64, .dictionary_f64 => blk: {
                    const values = try pa.alloc(f64, positions.len);
                    for (positions, values, nulls) |position, *value, flag| value.* = if (flag != 0) 0 else switch (position.page.columns[ci].values) {
                        .f64 => |v| v[position.row],
                        .dictionary_f64 => |v| v.at(position.row),
                        else => return error.RowSourceColumnKindMismatch,
                    };
                    break :blk .{ .f64 = values };
                },
                .bytes, .dictionary_bytes => blk: {
                    var dictionary: std.StringHashMapUnmanaged(u32) = .empty;
                    defer dictionary.deinit(pa);
                    var values: std.ArrayList([]const u8) = .empty;
                    try values.append(pa, "");
                    try dictionary.put(pa, "", 0);
                    const indices = try pa.alloc(u32, positions.len);
                    for (positions, indices, nulls) |position, *index, flag| {
                        if (flag != 0) {
                            index.* = 0;
                            continue;
                        }
                        const raw = try position.page.columns[ci].bytesAt(position.row);
                        const entry = try dictionary.getOrPut(pa, raw);
                        if (!entry.found_existing) {
                            const owned = try pa.dupe(u8, raw);
                            entry.key_ptr.* = owned;
                            entry.value_ptr.* = @intCast(values.items.len);
                            try values.append(pa, owned);
                        }
                        index.* = entry.value_ptr.*;
                    }
                    break :blk .{ .dictionary_bytes = .{ .values = values.items, .indices = indices } };
                },
                .decimal128 => |decimal| blk: {
                    var result = decimal;
                    const values = try pa.alloc(i128, positions.len);
                    for (positions, values, nulls) |position, *value, flag| {
                        const source = position.page.columns[ci].values;
                        if (source != .decimal128 or source.decimal128.scale != decimal.scale) return error.RowSourceColumnKindMismatch;
                        result.precision = @max(result.precision, source.decimal128.precision);
                        value.* = if (flag != 0) 0 else source.decimal128.values[position.row];
                    }
                    result.values = values;
                    break :blk .{ .decimal128 = result };
                },
                inline .bool, .json, .vector_f32 => |_, tag| blk: {
                    const T = @typeInfo(@FieldType(rows.ColumnValues, @tagName(tag))).pointer.child;
                    const values = try pa.alloc(T, positions.len);
                    for (positions, values, nulls) |position, *value, flag| {
                        const source = position.page.columns[ci].values;
                        if (std.meta.activeTag(source) != tag) return error.RowSourceColumnKindMismatch;
                        const raw = @field(source, @tagName(tag))[position.row];
                        value.* = if (comptime tag == .bool) flag == 0 and raw else if (flag != 0) &.{} else try pa.dupe(@typeInfo(T).pointer.child, raw);
                    }
                    break :blk @unionInit(rows.ColumnValues, @tagName(tag), values);
                },
                .encoded => return error.UnsupportedSqlExecution,
            };
        }
        self.columns = columns;
        return self;
    }
    fn releasePage(raw: *anyopaque) void {
        @as(*Page, @ptrCast(@alignCast(raw))).release();
    }
    fn borrowRange(a: A, source: *const Page, begin: usize, count: usize, projection_: ?[]const usize) !*Page {
        const self = try a.create(Page);
        self.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = count };
        errdefer self.release();
        const pa = self.arena.allocator();
        const columns = try pa.alloc(rows.ColumnVector, if (projection_) |indices| indices.len else source.columns.len);
        for (columns, 0..) |*column, i| {
            column.* = source.columns[if (projection_) |indices| indices[i] else i];
            column.nulls = column.nulls.slice(begin, count);
            column.values = switch (column.values) {
                .decimal128 => |v| .{ .decimal128 = v.slice(begin, count) },
                inline .dictionary_i64, .dictionary_f64, .dictionary_bytes => |v, tag| blk: {
                    var selected = v;
                    selected.indices = v.indices[begin..][0..count];
                    if (comptime @hasField(@TypeOf(selected), "owned")) selected.owned = false;
                    break :blk @unionInit(rows.ColumnValues, @tagName(tag), selected);
                },
                .encoded => return error.UnsupportedSqlExecution,
                inline else => |v, tag| @unionInit(rows.ColumnValues, @tagName(tag), v[begin..][0..count]),
            };
        }
        @constCast(source).retain();
        self.owner = .{ .ptr = @constCast(source), .release_fn = releasePage, .retained_bytes = source.retainedBytes() };
        self.columns = columns;
        return self;
    }
    /// Prefer retained payloads for dense selections; sparse selections gather
    /// compact slots so a few hits cannot pin a large decoded page/dictionary.
    pub fn retainOrCopy(a: A, batch: rows.ColumnBatch, selection: []const usize, capability: anytype) !*Page {
        try batch.validate();
        for (selection) |index| if (index >= batch.rowCount()) return error.InvalidSqlBackendResponse;
        if (selection.len != 0 and selection.len >= batch.rowCount() / 2 + batch.rowCount() % 2) if (capability) |source| {
            if (try source.retain_fn(source.ptr, a)) |owner| {
                if (owner.retained_bytes <= 4 * 1024 * 1024) return borrow(a, batch, selection, owner);
                owner.release();
            }
        };
        return copy(a, batch, selection);
    }
    /// Consumes owner on success and failure. Descriptor/name/selection storage
    /// is independent of the producer; scalar and dictionary payloads are shared.
    pub fn borrow(a: A, batch: rows.ColumnBatch, selection: []const usize, owner: rows.ColumnOwner) !*Page {
        var owns = true;
        errdefer if (owns) owner.release();
        try batch.validate();
        for (selection) |index| if (index >= batch.rowCount()) return error.InvalidSqlBackendResponse;
        const self = try a.create(Page);
        self.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = selection.len, .owner = owner };
        owns = false;
        errdefer self.release();
        const pa = self.arena.allocator();
        self.selection = try pa.dupe(usize, selection);
        const columns = try pa.dupe(rows.ColumnVector, batch.columns);
        for (columns) |*column| column.name = try pa.dupe(u8, column.name);
        self.columns = columns;
        return self;
    }
    fn gather(a: A, input: []const rows.ColumnVector, selection: []const usize) !*Page {
        const self = try a.create(Page);
        self.* = .{ .a = a, .arena = .init(a), .columns = &.{}, .len = selection.len };
        errdefer self.release();
        const pa = self.arena.allocator();
        const columns = try pa.alloc(rows.ColumnVector, input.len);
        for (input, columns) |column, *out| {
            out.* = .{ .name = try pa.dupe(u8, column.name), .values = undefined };
            const nulls = try pa.alloc(u8, selection.len);
            for (selection, nulls) |index, *flag| flag.* = @intFromBool(column.nulls.isNull(index));
            out.nulls = .{ .bytes = nulls };
            out.values = switch (column.values) {
                .encoded => |values| blk: {
                    const gathered = try pa.alloc([]const u8, selection.len);
                    for (selection, gathered, nulls) |index, *value, flag| value.* = if (flag != 0) "null" else try std.json.Stringify.valueAlloc(pa, try values.at(index), .{});
                    break :blk .{ .json = gathered };
                },
                .decimal128 => |values| blk: {
                    var result = values;
                    const gathered = try pa.alloc(i128, selection.len);
                    for (selection, gathered, nulls) |index, *value, flag| value.* = if (flag != 0) 0 else values.values[index];
                    result.values = gathered;
                    break :blk .{ .decimal128 = result };
                },

                inline .i64, .f64, .bool => |values, tag| blk: {
                    const gathered = try pa.alloc(@TypeOf(values[0]), selection.len);
                    for (selection, gathered) |index, *value| value.* = values[index];
                    break :blk @unionInit(rows.ColumnValues, @tagName(tag), gathered);
                },
                inline .bytes, .json => |values, tag| blk: {
                    const gathered = try pa.alloc([]const u8, selection.len);
                    for (selection, gathered, nulls) |index, *value, flag| value.* = if (flag != 0) "" else try pa.dupe(u8, values[index]);
                    break :blk @unionInit(rows.ColumnValues, @tagName(tag), gathered);
                },
                inline .dictionary_bytes, .dictionary_i64, .dictionary_f64 => |dictionary, tag| blk: {
                    const T = @TypeOf(dictionary.values[0]);
                    var values: std.ArrayList(T) = .empty;
                    var ids: std.AutoHashMapUnmanaged(u32, u32) = .empty;
                    const indices = try pa.alloc(u32, selection.len);
                    for (selection, indices, nulls) |index, *id, flag| {
                        if (flag != 0) {
                            id.* = 0;
                            continue;
                        }
                        const old = dictionary.indices[index];
                        const entry = try ids.getOrPut(pa, old);
                        if (!entry.found_existing) {
                            entry.value_ptr.* = @intCast(values.items.len);
                            try values.append(pa, if (tag == .dictionary_bytes) try pa.dupe(u8, dictionary.values[old]) else dictionary.values[old]);
                        }
                        id.* = entry.value_ptr.*;
                    }
                    break :blk @unionInit(rows.ColumnValues, @tagName(tag), .{ .values = values.items, .indices = indices });
                },
                .vector_f32 => return error.UnsupportedSqlExecution,
            };
        }
        self.columns = columns;
        return self;
    }
};

/// Compact projected wire values. Scalar strings borrow the retained column
/// page; numbers and complex JSON are canonicalized once. Temporary projection
/// trees are reset after each preparation and never survive into transport I/O.
pub const Prepared = struct {
    const Value = union(enum) {
        string: struct { bytes: []const u8, encoded_len: usize, plain: bool },
        raw: []const u8,
        scalar: struct { bytes: [24]u8, len: u8 },
        fn length(self: @This()) !usize {
            return switch (self) {
                .string => |v| v.encoded_len,
                .raw => |v| v.len,
                .scalar => |v| v.len,
            };
        }
        pub fn jsonStringify(self: @This(), w: *std.json.Stringify) std.json.Stringify.Error!void {
            switch (self) {
                .string => |v| {
                    if (v.plain and !w.options.escape_unicode and !w.options.emit_strings_as_arrays) {
                        try w.beginWriteRaw();
                        try w.writer.writeByte('"');
                        try w.writer.writeAll(v.bytes);
                        try w.writer.writeByte('"');
                        w.endWriteRaw();
                    } else try w.write(v.bytes);
                },
                .raw => |v| {
                    try w.beginWriteRaw();
                    try w.writer.writeAll(v);
                    w.endWriteRaw();
                },
                .scalar => |v| {
                    try w.beginWriteRaw();
                    try w.writer.writeAll(v.bytes[0..v.len]);
                    w.endWriteRaw();
                },
            }
        }
        fn prepare(a: A, value: std.json.Value, borrow_string: bool) !@This() {
            switch (value) {
                .integer, .float, .bool, .null => {
                    var bytes: [24]u8 = @splat(0);
                    var writer: std.Io.Writer = .fixed(&bytes);
                    if (std.json.Stringify.value(value, .{ .emit_null_optional_fields = false }, &writer)) |_| {
                        return .{ .scalar = .{ .bytes = bytes, .len = @intCast(writer.buffered().len) } };
                    } else |_| {} // Preserve canonical fallback if a scalar needs more room.
                },
                else => {},
            }
            if (borrow_string and value == .string) {
                const stats = try stringStats(value.string);
                if (stats.ascii or std.unicode.utf8ValidateSlice(value.string)) return .{ .string = .{ .bytes = value.string, .encoded_len = stats.length, .plain = stats.length == value.string.len + 2 } };
            }
            return .{ .raw = try std.json.Stringify.valueAlloc(a, value, .{ .emit_null_optional_fields = false }) };
        }
    };
    const Field = struct { name: []const u8, value: Value };
    fields: []const Field,
    encoded_len: usize,
    pub fn retainedBytes(self: Prepared) usize {
        var size = self.fields.len * @sizeOf(Field);
        for (self.fields) |field| {
            size +|= field.name.len;
            if (field.value == .raw) size +|= field.value.raw.len;
        }
        return size;
    }
    /// Raw fragments and names may belong to per-hit preparation scratch.
    /// Scalar strings continue borrowing the immutable result page.
    pub fn clone(self: Prepared, a: A) !Prepared {
        const fields = try a.dupe(Field, self.fields);
        for (fields) |*field| {
            field.name = try a.dupe(u8, field.name);
            if (field.value == .raw) field.value.raw = try a.dupe(u8, field.value.raw);
        }
        return .{ .fields = fields, .encoded_len = self.encoded_len };
    }
    fn stringLength(bytes: []const u8) !usize {
        return (try stringStats(bytes)).length;
    }
    /// Size and validate the common ASCII path in one vector pass. Plain
    /// validated strings can then be copied directly by the wire writer.
    fn stringStats(bytes: []const u8) !struct { length: usize, ascii: bool } {
        var extra: usize = 0;
        var ascii = true;
        var at: usize = 0;
        const V = @Vector(32, u8);
        while (bytes.len - at >= 32) : (at += 32) {
            const v: V = bytes[at..][0..32].*;
            ascii = ascii and !@reduce(.Or, v >= @as(V, @splat(0x80)));
            const controls = v < @as(V, @splat(32));
            const short = (v == @as(V, @splat(8))) | (v == @as(V, @splat(12))) | (v == @as(V, @splat(10))) | (v == @as(V, @splat(13))) | (v == @as(V, @splat(9)));
            const escaped = controls | (v == @as(V, @splat('"'))) | (v == @as(V, @splat('\\')));
            extra = try std.math.add(usize, extra, @popCount(@as(u32, @bitCast(escaped))) + 4 * @as(usize, @popCount(@as(u32, @bitCast(controls & !short)))));
        }
        for (bytes[at..]) |byte| {
            ascii = ascii and byte < 0x80;
            extra = try std.math.add(usize, extra, switch (byte) {
                '"', '\\', 8, 12, '\n', '\r', '\t' => @as(usize, 1),
                0...7, 11, 14...31 => @as(usize, 5),
                else => @as(usize, 0),
            });
        }
        return .{ .length = try std.math.add(usize, try std.math.add(usize, bytes.len, 2), extra), .ascii = ascii };
    }
    fn init(fields: []const Field) !Prepared {
        var length: usize = 2;
        for (fields, 0..) |field, i| {
            length = try std.math.add(usize, length, try stringLength(field.name));
            length = try std.math.add(usize, length, 1 + @as(usize, @intFromBool(i != 0)));
            length = try std.math.add(usize, length, try field.value.length());
        }
        return .{ .fields = fields, .encoded_len = length };
    }
    pub fn jsonStringify(self: Prepared, w: *std.json.Stringify) std.json.Stringify.Error!void {
        try w.beginObject();
        for (self.fields) |field| {
            try w.objectField(field.name);
            try w.write(field.value);
        }
        try w.endObject();
    }
};

/// Request-owned, bounded schema/projection plans. Entries own names and masks,
/// never result pages, payloads, readers or request callbacks.
pub const ProjectionCache = struct {
    const Entry = struct {
        arena: std.heap.ArenaAllocator,
        names: []const []const u8,
        fields: []const []const u8,
        include_all: bool,
        indices: []const usize,
        header_bytes: usize,
        fn matches(self: *const @This(), columns: []const rows.ColumnVector, options: anytype) bool {
            if (self.include_all != options.include_all_fields or self.fields.len != options.fields.len or self.names.len != columns.len) return false;
            for (self.fields, options.fields) |a, b| if (!std.mem.eql(u8, a, b)) return false;
            for (self.names, columns) |name, column| if (!std.mem.eql(u8, name, column.name)) return false;
            return true;
        }
    };
    a: A,
    entries: [4]?*Entry = @splat(null),
    builds: usize = 0,
    pub fn init(a: A) ProjectionCache {
        return .{ .a = @import("../../sql/memory_budget.zig").reclaimingAllocator(a) };
    }
    pub fn deinit(self: *ProjectionCache) void {
        for (self.entries) |entry| if (entry) |owned| {
            owned.arena.deinit();
            self.a.destroy(owned);
        };
    }
    fn get(self: *ProjectionCache, page: *const Page, options: anytype) !?*const Entry {
        for (options.fields) |field| if (std.mem.indexOfScalar(u8, field, '.') != null) return null;
        for (self.entries) |entry| if (entry) |held| {
            if (held.matches(page.columns, options)) return held;
        };
        const slot = for (&self.entries) |*entry| {
            if (entry.* == null) break entry;
        } else return null;
        const entry = try self.a.create(Entry);
        entry.* = .{ .arena = .init(self.a), .names = &.{}, .fields = &.{}, .include_all = options.include_all_fields, .indices = &.{}, .header_bytes = 2 };
        errdefer {
            entry.arena.deinit();
            self.a.destroy(entry);
        }
        const a = entry.arena.allocator();
        const names = try a.alloc([]const u8, page.columns.len);
        for (names, page.columns) |*name, column| name.* = try a.dupe(u8, column.name);
        entry.names = names;
        const fields = try a.alloc([]const u8, options.fields.len);
        for (fields, options.fields) |*field, value| field.* = try a.dupe(u8, value);
        entry.fields = fields;
        var indices: std.ArrayList(usize) = .empty;
        const includes = for (options.fields) |field| {
            if (field.len == 0 or field[0] != '-') break true;
        } else false;
        if (includes) {
            for (options.fields, 0..) |field, ordinal| for (page.columns, 0..) |column, index| {
                if (!std.mem.eql(u8, field, "*") and !std.mem.eql(u8, field, column.name)) continue;
                if (Row.firstInclude(options.fields, column.name) != ordinal or Row.excluded(options.fields, column.name) or Row.internal(column.name)) continue;
                try indices.append(a, index);
            };
        } else if (options.fields.len != 0 or options.include_all_fields) {
            for (page.columns, 0..) |column, index| if (!Row.excluded(options.fields, column.name) and !Row.internal(column.name)) try indices.append(a, index);
        }
        entry.indices = try indices.toOwnedSlice(a);
        for (entry.indices, 0..) |index, ordinal| {
            entry.header_bytes = try std.math.add(usize, entry.header_bytes, try Prepared.stringLength(page.columns[index].name));
            entry.header_bytes = try std.math.add(usize, entry.header_bytes, 1 + @as(usize, @intFromBool(ordinal != 0)));
        }
        slot.* = entry;
        self.builds += 1;
        return entry;
    }
    pub fn prepare(self: *ProjectionCache, row: Row, a: A, scratch: *std.heap.ArenaAllocator, options: anytype) !Prepared {
        const entry = (try self.get(row.page, options)) orelse return row.prepare(a, scratch, options);
        _ = scratch.reset(.retain_capacity);
        const fields = try a.alloc(Prepared.Field, entry.indices.len);
        var length = entry.header_bytes;
        for (fields, entry.indices) |*field, index| {
            const column = row.page.columns[index];
            const value = try Prepared.Value.prepare(a, try row.cell(scratch.allocator(), column), column.values != .json and column.values != .decimal128 and column.values != .encoded);
            field.* = .{ .name = column.name, .value = value };
            length = try std.math.add(usize, length, try value.length());
        }
        return .{ .fields = fields, .encoded_len = length };
    }
    pub fn write(self: *ProjectionCache, row: Row, scratch: *std.heap.ArenaAllocator, options: anytype, w: *std.json.Stringify) !void {
        const entry = (try self.get(row.page, options)) orelse return row.write(scratch, options, w);
        _ = scratch.reset(.retain_capacity);
        try w.beginObject();
        for (entry.indices) |index| {
            const column = row.page.columns[index];
            try w.objectField(column.name);
            try w.write(try row.cell(scratch.allocator(), column));
        }
        try w.endObject();
    }
};

pub const Row = struct {
    page: *Page,
    index: usize,
    // Generic internal hit codecs serialize values, never lease pointers.
    pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!Row {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var opts = options;
        opts.allocate = .alloc_always;
        opts.parse_numbers = false;
        const parsed = try std.json.innerParse(std.json.Value, scratch.allocator(), source, opts);
        return fromValue(a, parsed);
    }
    pub fn jsonParseFromValue(a: A, source: std.json.Value, _: std.json.ParseOptions) std.json.ParseFromValueError!Row {
        return fromValue(a, source);
    }
    fn fromValue(a: A, source: std.json.Value) error{ OutOfMemory, UnexpectedToken }!Row {
        if (source != .object) return error.UnexpectedToken;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const columns = try sa.alloc(rows.ColumnVector, source.object.count());
        var iterator = source.object.iterator();
        for (columns) |*column| {
            const entry = iterator.next().?;
            const encoded = try std.json.Stringify.valueAlloc(sa, entry.value_ptr.*, .{});
            const values = try sa.alloc([]const u8, 1);
            values[0] = encoded;
            column.* = .{ .name = entry.key_ptr.*, .values = .{ .json = values } };
        }
        const refs = [_]rows.RowRef{.{ .relational_key = "" }};
        const page = Page.copy(a, .{ .snapshot = .{ .table_id = "internal-hit", .snapshot_id = "retained" }, .row_refs = &refs, .columns = columns }, &.{0}) catch |err| return if (err == error.OutOfMemory) error.OutOfMemory else error.UnexpectedToken;
        return .{ .page = page, .index = 0 };
    }
    pub fn jsonStringify(self: Row, w: *std.json.Stringify) std.json.Stringify.Error!void {
        var scratch = std.heap.ArenaAllocator.init(self.page.a);
        defer scratch.deinit();
        try w.beginObject();
        for (self.page.columns) |column| {
            try w.objectField(column.name);
            const item = self.cell(scratch.allocator(), column) catch return error.WriteFailed;
            try w.write(item);
        }
        try w.endObject();
    }
    pub fn clone(self: Row) Row {
        self.page.retain();
        return self;
    }
    pub fn cloneInto(self: Row, a: A) !Row {
        // A refcount cannot extend the lifetime of a request arena. Preserve
        // SearchHit.clone's independence when moving to another allocator.
        if (a.ptr == self.page.a.ptr and a.vtable == self.page.a.vtable) return self.clone();
        return .{ .page = try Page.gather(a, self.page.columns, &.{self.index}), .index = 0 };
    }
    pub fn deinit(self: Row) void {
        self.page.release();
    }
    /// Temporary borrowed source for highlighting and complex projections.
    /// Containers and parsed JSON belong to arena; scalar bytes belong to the page.
    pub fn value(self: Row, arena: *std.heap.ArenaAllocator) !std.json.Value {
        return self.valueForProjection(arena, .{ .fields = @as([]const []const u8, &.{}), .include_all_fields = true });
    }
    /// Conservative root selection for a subsequent full projection. This is
    /// not a final field mask: nested exclusions still belong to the projector.
    /// Do not decode unrelated JSON columns merely to select one logical path.
    pub fn valueForProjection(self: Row, arena: *std.heap.ArenaAllocator, options: anytype) !std.json.Value {
        return self.valueForProjectionAlloc(arena.allocator(), options);
    }
    /// Containers belong to a; immutable scalar bytes remain leased by page.
    pub fn valueForProjectionAlloc(self: Row, a: A, options: anytype) !std.json.Value {
        var object: std.json.ObjectMap = .empty;
        const includes = for (options.fields) |field| {
            if (field.len == 0 or field[0] != '-') break true;
        } else false;
        if (options.fields.len == 0 and !options.include_all_fields) return .{ .object = object };
        for (self.page.columns) |column| {
            if (excluded(options.fields, column.name)) continue;
            if (includes) {
                const selected = for (options.fields) |field| {
                    if (field.len > 0 and field[0] == '-') continue;
                    const root = field[0 .. std.mem.indexOfScalar(u8, field, '.') orelse field.len];
                    if (std.mem.eql(u8, root, "*") or std.mem.eql(u8, root, column.name)) break true;
                } else false;
                if (!selected) continue;
            }
            try object.put(a, column.name, try self.cell(a, column));
        }
        return .{ .object = object };
    }
    fn cell(self: Row, a: A, column: rows.ColumnVector) !std.json.Value {
        if (column.nulls.isNull(self.index)) return .null;
        return switch (column.values) {
            .decimal128, .encoded => try column.values.jsonAt(a, self.index),
            .i64 => |values| .{ .integer = values[self.index] },
            .dictionary_i64 => |values| .{ .integer = values.at(self.index) },
            .f64 => |values| .{ .float = values[self.index] },
            .dictionary_f64 => |values| .{ .float = values.at(self.index) },
            .bool => |values| .{ .bool = values[self.index] },
            .bytes => |values| .{ .string = values[self.index] },
            .dictionary_bytes => |values| .{ .string = values.at(self.index) },
            .json => |values| try std.json.parseFromSliceLeaky(std.json.Value, a, values[self.index], .{ .parse_numbers = false }),
            .vector_f32 => error.UnsupportedSqlExecution,
        };
    }
    pub fn write(self: Row, scratch: *std.heap.ArenaAllocator, options: anytype, w: *std.json.Stringify) !void {
        // Reuse one arena across hits, including when its backing allocator is
        // itself an arena. Nested JSON/projection capacity cannot grow per hit.
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const simple = for (options.fields) |field| {
            if (std.mem.indexOfScalar(u8, field, '.') != null) break false;
        } else true;
        if (!simple) {
            const source = try self.valueForProjection(scratch, options);
            var view = try projection.projectLookupView(sa, source, options);
            stripInternal(&view);
            return w.write(view);
        }
        try w.beginObject();
        const includes = for (options.fields) |field| {
            if (field.len == 0 or field[0] != '-') break true;
        } else false;
        if (includes) {
            // Public include order and duplicate/wildcard semantics match the
            // owned projector without allocating a source object or key set.
            for (options.fields, 0..) |field, ordinal| for (self.page.columns) |column| {
                if (!std.mem.eql(u8, field, "*") and !std.mem.eql(u8, field, column.name)) continue;
                if (firstInclude(options.fields, column.name) != ordinal or excluded(options.fields, column.name) or internal(column.name)) continue;
                try w.objectField(column.name);
                try w.write(try self.cell(sa, column));
            };
        } else if (options.fields.len != 0 or options.include_all_fields) for (self.page.columns) |column| {
            if (internal(column.name) or excluded(options.fields, column.name)) continue;
            try w.objectField(column.name);
            // Validate JSON provider bytes before emitting public output.
            try w.write(try self.cell(sa, column));
        };
        try w.endObject();
    }
    pub fn prepare(self: Row, a: A, scratch: *std.heap.ArenaAllocator, options: anytype) !Prepared {
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        var fields: std.ArrayList(Prepared.Field) = .empty;
        const simple = for (options.fields) |field| {
            if (std.mem.indexOfScalar(u8, field, '.') != null) break false;
        } else true;
        if (!simple) {
            const source = try self.valueForProjection(scratch, options);
            var view = try projection.projectLookupView(sa, source, options);
            stripInternal(&view);
            var it = view.object.iterator();
            while (it.next()) |entry| try fields.append(a, .{ .name = try a.dupe(u8, entry.key_ptr.*), .value = try Prepared.Value.prepare(a, entry.value_ptr.*, false) });
        } else {
            const includes = for (options.fields) |field| {
                if (field.len == 0 or field[0] != '-') break true;
            } else false;
            if (includes) {
                for (options.fields, 0..) |field, ordinal| for (self.page.columns) |column| {
                    if (!std.mem.eql(u8, field, "*") and !std.mem.eql(u8, field, column.name)) continue;
                    if (firstInclude(options.fields, column.name) != ordinal or excluded(options.fields, column.name) or internal(column.name)) continue;
                    try fields.append(a, .{ .name = column.name, .value = try Prepared.Value.prepare(a, try self.cell(sa, column), column.values != .json and column.values != .decimal128 and column.values != .encoded) });
                };
            } else if (options.fields.len != 0 or options.include_all_fields) for (self.page.columns) |column| {
                if (internal(column.name) or excluded(options.fields, column.name)) continue;
                try fields.append(a, .{ .name = column.name, .value = try Prepared.Value.prepare(a, try self.cell(sa, column), column.values != .json and column.values != .decimal128 and column.values != .encoded) });
            };
        }
        return Prepared.init(fields.items);
    }
    fn firstInclude(fields: []const []const u8, name: []const u8) ?usize {
        for (fields, 0..) |field, index| {
            if (field.len != 0 and field[0] == '-') continue;
            if (std.mem.eql(u8, field, "*") or std.mem.eql(u8, field, name)) return index;
        }
        return null;
    }
    fn excluded(fields: []const []const u8, name: []const u8) bool {
        for (fields) |field| {
            if (field.len == 0 or field[0] != '-') continue;
            if (std.mem.eql(u8, field[1..], "*") or std.mem.eql(u8, field[1..], name)) return true;
        }
        return false;
    }
    fn internal(name: []const u8) bool {
        const hierarchy = @import("../hierarchy_navigation.zig");
        return std.mem.eql(u8, name, hierarchy.unit_fingerprint_field) or std.mem.eql(u8, name, hierarchy.grouped_unit_revision_envelope_field);
    }
    fn stripInternal(value_ptr: *std.json.Value) void {
        if (value_ptr.* != .object) return;
        var i: usize = 0;
        while (i < value_ptr.object.count()) {
            if (internal(value_ptr.object.keys()[i])) {
                _ = value_ptr.object.orderedRemove(value_ptr.object.keys()[i]);
            } else i += 1;
        }
    }
};

fn retainedSelectionScenario(a: A) !void {
    var text = [_]u8{ 'n', 'e', 'e', 'd', 'l', 'e' };
    const refs = [_]rows.RowRef{ .{ .relational_key = "zero" }, .{ .relational_key = "one" }, .{ .relational_key = "two" } };
    const columns = [_]rows.ColumnVector{
        .{ .name = "body", .values = .{ .dictionary_bytes = .{ .values = &.{ "unused", &text }, .indices = &.{ 1, std.math.maxInt(u32), 1 } } }, .nulls = .{ .bytes = &.{ 0, 1, 0 } } },
        .{ .name = "amount", .values = .{ .dictionary_i64 = .{ .values = &.{9007199254740993}, .indices = &.{ 0, 0, 0 } } } },
    };
    const page = try Page.copy(a, .{ .snapshot = .{ .table_id = "test", .snapshot_id = "one" }, .row_refs = &refs, .columns = &columns }, &.{ 2, 0, 2, 1 });
    var owns_page = true;
    defer if (owns_page) page.release();
    const source = page.row(0);
    defer source.deinit();
    const duplicate = source.clone();
    defer duplicate.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.columns[0].values.dictionary_bytes.values.len);
    try std.testing.expectEqualSlices(u32, &.{ 0, 0, 0, 0 }, page.columns[0].values.dictionary_bytes.indices);
    text[0] = 'X';
    page.release();
    owns_page = false;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const value = try duplicate.value(&scratch);
    try std.testing.expectEqualStrings("needle", value.object.get("body").?.string);
    try std.testing.expectEqual(@as(i64, 9007199254740993), value.object.get("amount").?.integer);
    const null_value = try (Row{ .page = page, .index = 3 }).value(&scratch);
    try std.testing.expect(null_value.object.get("body").? == .null);
    const encoded = try std.json.Stringify.valueAlloc(a, source, .{});
    defer a.free(encoded);
    var decoded = try std.json.parseFromSlice(Row, a, encoded, .{});
    defer decoded.deinit();
    const roundtrip = try decoded.value.value(&scratch);
    try std.testing.expectEqualStrings("needle", roundtrip.object.get("body").?.string);
}
test "external lake column page leases gather duplicates and nullable dictionaries across owner closure and OOM" {
    try retainedSelectionScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, retainedSelectionScenario, .{});
}

test "external lake column wire scratch stays bounded with an arena backing allocator" {
    const a = std.testing.allocator;
    const refs = [_]rows.RowRef{.{ .relational_key = "row" }};
    const columns = [_]rows.ColumnVector{.{ .name = "nested", .values = .{ .json = &.{"{\"value\":\"retained payload\",\"private\":\"omit\"}"} } }};
    const page = try Page.copy(a, .{ .snapshot = .{ .table_id = "docs", .snapshot_id = "one" }, .row_refs = &refs, .columns = &columns }, &.{0});
    defer page.release();
    var parent = std.heap.ArenaAllocator.init(a);
    defer parent.deinit();
    var scratch = std.heap.ArenaAllocator.init(parent.allocator());
    defer scratch.deinit();
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    var w: std.json.Stringify = .{ .writer = &output.writer };
    try w.beginArray();
    const source: Row = .{ .page = page, .index = 0 };
    const options = @import("types.zig").LookupOptions{ .fields = &.{ "nested.*", "-nested.private" } };
    // Initial resets consolidate the arena's small allocation chunks.
    for (0..8) |_| try source.write(&scratch, options, &w);
    const retained = parent.queryCapacity();
    for (0..100) |_| try source.write(&scratch, options, &w);
    try w.endArray();
    try std.testing.expectEqual(retained, parent.queryCapacity());
}

test "external lake column clones into another allocator outlive the original request arena" {
    const a = std.testing.allocator;
    var request = std.heap.ArenaAllocator.init(a);
    var request_open = true;
    defer if (request_open) request.deinit();
    const refs = [_]rows.RowRef{.{ .relational_key = "row" }};
    const columns = [_]rows.ColumnVector{.{ .name = "body", .values = .{ .bytes = &.{"survives request closure"} } }};
    const page = try Page.copy(request.allocator(), .{ .snapshot = .{ .table_id = "docs", .snapshot_id = "one" }, .row_refs = &refs, .columns = &columns }, &.{0});
    const original: Row = .{ .page = page, .index = 0 };
    const copied = try original.cloneInto(a);
    defer copied.deinit();
    original.deinit();
    request.deinit();
    request_open = false;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const value = try copied.value(&scratch);
    try std.testing.expectEqualStrings("survives request closure", value.object.get("body").?.string);
}

fn preparedProjectionScenario(a: A) !void {
    const refs = [_]rows.RowRef{.{ .relational_key = "row" }};
    const columns = [_]rows.ColumnVector{
        .{ .name = "escaped\nkey", .values = .{ .bytes = &.{"a\x00\x01\x08\x0b\x0c\n\r\t\\\"é😀"} } },
        .{ .name = "amount", .values = .{ .i64 = &.{9007199254740993} } },
        .{ .name = "nested", .values = .{ .json = &.{"{\"visible\":\"escaped\\nvalue\",\"private\":0,\"array\":[null,true,1.5]}"} } },
        .{ .name = "nullable", .values = .{ .bool = &.{false} }, .nulls = .{ .bytes = &.{1} } },
    };
    const page = try Page.copy(a, .{ .snapshot = .{ .table_id = "test", .snapshot_id = "one" }, .row_refs = &refs, .columns = &columns }, &.{0});
    defer page.release();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const source: Row = .{ .page = page, .index = 0 };
    const Options = @import("types.zig").LookupOptions;
    var cache = ProjectionCache.init(a);
    defer cache.deinit();
    for ([_]Options{ .{}, .{ .fields = &.{"*"} }, .{ .fields = &.{ "nested.*", "-nested.private", "amount" } }, .{ .fields = &.{ "escaped\nkey", "escaped\nkey", "nullable" } }, .{ .fields = &.{"-amount"} } }) |options| {
        var preparation = std.heap.ArenaAllocator.init(a);
        defer preparation.deinit();
        const prepared = try source.prepare(preparation.allocator(), &scratch, options);
        // Transient parsing/projection state must not be borrowed by the plan.
        _ = scratch.reset(.free_all);
        const encoded = try std.json.Stringify.valueAlloc(a, prepared, .{});
        defer a.free(encoded);
        try std.testing.expectEqual(encoded.len, prepared.encoded_len);
        var buffer: [4096]u8 = undefined;
        var output: std.Io.Writer = .fixed(&buffer);
        var w: std.json.Stringify = .{ .writer = &output };
        try source.write(&scratch, options, &w);
        try std.testing.expectEqualStrings(output.buffered(), encoded);
        const cached = try cache.prepare(source, preparation.allocator(), &scratch, options);
        _ = scratch.reset(.free_all);
        const cached_json = try std.json.Stringify.valueAlloc(a, cached, .{});
        defer a.free(cached_json);
        try std.testing.expectEqualStrings(encoded, cached_json);
        try std.testing.expectEqual(encoded.len, cached.encoded_len);
        output = .fixed(&buffer);
        w = .{ .writer = &output };
        try cache.write(source, &scratch, options, &w);
        try std.testing.expectEqualStrings(encoded, output.buffered());
    }
    try std.testing.expectEqual(@as(usize, 4), cache.builds);
}

test "external lake prepared projection has exact escaped length and survives scratch reset and OOM" {
    try preparedProjectionScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, preparedProjectionScenario, .{});
}

fn retainedPayloadScenario(a: A) !void {
    const refs = [_]rows.RowRef{ .{ .relational_key = "zero" }, .{ .relational_key = "one" }, .{ .relational_key = "two" } };
    const columns = [_]rows.ColumnVector{
        .{ .name = "body", .values = .{ .dictionary_bytes = .{ .values = &.{ "first", "last" }, .indices = &.{ 0, 0, 1 } } } },
        .{ .name = "amount", .values = .{ .i64 = &.{ 9007199254740993, 2, 3 } } },
    };
    const producer = try Page.copy(a, .{ .snapshot = .{ .table_id = "docs", .snapshot_id = "one" }, .row_refs = &refs, .columns = &columns }, &.{ 0, 1, 2 });
    var producer_open = true;
    defer if (producer_open) producer.release();
    const Capability = struct {
        fn release(raw: *anyopaque) void {
            const page: *Page = @ptrCast(@alignCast(raw));
            page.release();
        }
        fn retain(raw: *anyopaque, _: A) !?rows.ColumnOwner {
            const page: *Page = @ptrCast(@alignCast(raw));
            page.retain();
            return .{ .ptr = page, .release_fn = release, .retained_bytes = page.retainedBytes() };
        }
    };
    const batch: rows.ColumnBatch = .{ .snapshot = .{ .table_id = "docs", .snapshot_id = "one" }, .row_refs = &refs, .columns = producer.columns };
    const capability: ?struct { ptr: *anyopaque, retain_fn: *const fn (*anyopaque, A) anyerror!?rows.ColumnOwner } = .{ .ptr = producer, .retain_fn = Capability.retain };
    const dense = try Page.retainOrCopy(a, batch, &.{ 2, 0, 2 }, capability);
    defer dense.release();
    try std.testing.expect(dense.owner != null);
    try std.testing.expect(dense.columns[0].values.dictionary_bytes.values.ptr == producer.columns[0].values.dictionary_bytes.values.ptr);
    try std.testing.expect(dense.columns[1].values.i64.ptr == producer.columns[1].values.i64.ptr);
    const sparse = try Page.retainOrCopy(a, batch, &.{2}, capability);
    defer sparse.release();
    try std.testing.expect(sparse.owner == null);
    producer.release();
    producer_open = false;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const last = dense.row(0);
    defer last.deinit();
    const first = dense.row(1);
    defer first.deinit();
    try std.testing.expectEqualStrings("last", (try last.value(&scratch)).object.get("body").?.string);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try first.value(&scratch)).object.get("amount").?.integer);
}
test "external lake dense column leases share payloads across producer closure and sparse selections gather under OOM" {
    try retainedPayloadScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, retainedPayloadScenario, .{});
}

test "external lake typed decimal scatter widens precision without losing coefficient bits" {
    const a = std.testing.allocator;
    const refs = [_]rows.RowRef{.{ .relational_key = "row" }};
    const first = try Page.copy(a, .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &.{.{ .name = "price", .values = .{ .decimal128 = .{ .values = &.{9007199254740993}, .precision = 18, .scale = 2 } } }} }, &.{0});
    defer first.release();
    const second = try Page.copy(a, .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &refs, .columns = &.{.{ .name = "price", .values = .{ .decimal128 = .{ .values = &.{-123456789012345678901}, .precision = 24, .scale = 2 } } }} }, &.{0});
    defer second.release();
    const gathered = try Page.gatherPositions(a, &.{ .{ .page = first, .row = 0 }, .{ .page = second, .row = 0 }, .{ .page = first, .row = 0 } });
    defer gathered.release();
    const decimal = gathered.columns[0].values.decimal128;
    try std.testing.expectEqual(@as(i32, 24), decimal.precision);
    try std.testing.expectEqual(@as(i32, 2), decimal.scale);
    try std.testing.expectEqualSlices(i128, &.{ 9007199254740993, -123456789012345678901, 9007199254740993 }, decimal.values);
}

test "external lake projected contiguous leases outlive producers and sparse scatters own only requested columns" {
    const Run = struct {
        fn run(a: A) !void {
            const input: rows.ColumnBatch = .{
                .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
                .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" }, .{ .relational_key = "c" }, .{ .relational_key = "d" } },
                .columns = &.{
                    .{ .name = "id", .values = .{ .i64 = &.{ 1, 2, 3, 4 } } },
                    .{ .name = "payload", .values = .{ .json = &.{ "null", "[1,2]", "{\"n\":9007199254740993}", "false" } } },
                    .{ .name = "price", .values = .{ .decimal128 = .{ .values = &.{ 0, 9007199254740993123, -9007199254740993123, 0 }, .precision = 38, .scale = 3 } } },
                },
            };
            const source = try Page.copy(a, input, &.{ 0, 1, 2, 3 });
            var owns = true;
            defer if (owns) source.release();
            const borrowed = try Page.gatherProjectedPositions(a, &.{ .{ .page = source, .row = 1 }, .{ .page = source, .row = 2 } }, &.{ 2, 0 });
            defer borrowed.release();
            try std.testing.expect(borrowed.owner != null and borrowed.selection == null);
            try std.testing.expectEqual(@as(usize, 2), borrowed.columns.len);
            try std.testing.expect(borrowed.columns[1].values.i64.ptr == source.columns[0].values.i64[1..].ptr);
            const copied = try Page.gatherProjectedPositions(a, &.{ .{ .page = source, .row = 2 }, .{ .page = source, .row = 1 }, .{ .page = source, .row = 2 } }, &.{ 2, 0 });
            defer copied.release();
            try std.testing.expect(copied.owner == null);
            source.release();
            owns = false;
            try std.testing.expectEqual(@as(i128, 9007199254740993123), borrowed.columns[0].values.decimal128.values[0]);
            try std.testing.expectEqual(@as(i128, -9007199254740993123), copied.columns[0].values.decimal128.values[0]);
            try std.testing.expectEqual(@as(i64, 3), try copied.columns[1].integerAt(2));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

test "external lake bounded window profiles prepared string sizing and plain transport match canonical JSON" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "plain", "\"\\\x00\x01\x08\x0c\n\r\t", "é 😀", "" }) |text| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const prepared = try Prepared.Value.prepare(arena.allocator(), .{ .string = text }, true);
        const actual = try std.json.Stringify.valueAlloc(a, prepared, .{});
        defer a.free(actual);
        const expected = try std.json.Stringify.valueAlloc(a, text, .{});
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, actual);
        try std.testing.expectEqual(expected.len, try prepared.length());
        const escaped = try std.json.Stringify.valueAlloc(a, prepared, .{ .escape_unicode = true });
        defer a.free(escaped);
        const expected_escaped = try std.json.Stringify.valueAlloc(a, text, .{ .escape_unicode = true });
        defer a.free(expected_escaped);
        try std.testing.expectEqualStrings(expected_escaped, escaped);
    }
    // Exercise every byte lane and control escape on both sides of the SIMD
    // boundary. Invalid UTF-8 retains the canonical raw-value fallback.
    var bytes: [96]u8 = @splat('x');
    for (0..96) |position| for (0..256) |byte| {
        bytes[position] = @intCast(byte);
        const expected = try std.json.Stringify.valueAlloc(a, @as([]const u8, &bytes), .{});
        defer a.free(expected);
        if (std.unicode.utf8ValidateSlice(&bytes)) {
            try std.testing.expectEqual(expected.len, try Prepared.stringLength(&bytes));
        } else {
            const value = try Prepared.Value.prepare(a, .{ .string = &bytes }, true);
            defer if (value == .raw) a.free(value.raw);
            try std.testing.expectEqual(expected.len, try value.length());
        }
        bytes[position] = 'x';
    };
}

test "external lake native typed result construction retains only physical scalar buffers" {
    const a = std.testing.allocator;
    const count = 4096;
    const names = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" };
    var budget = @import("../../sql/memory_budget.zig"){ .backing = a, .limit = 512 * 1024 };
    {
        const page = try Page.native(budget.allocator(), &names, count);
        defer page.release();
        for (0..count) |row| for (0..names.len) |column| {
            try page.setNative(row, column, if (row % 11 == 0) .null else .{ .integer = @as(i64, @intCast(row)) + 9007199254740993 });
        };
        try page.finishNative();
        for (page.columns) |column| {
            try std.testing.expect(column.values == .i64);
            try std.testing.expectEqual(@as(i64, 9007199254740994), column.values.i64[1]);
            try std.testing.expect(column.nulls.isNull(0));
        }
        try std.testing.expect(page.retainedBytes() < count * names.len * 16);
        std.debug.print("NATIVE_RESULT_TYPED cells={d} generic_cell_bytes={d} retained_bytes={d} peak_bytes={d}\n", .{ count * names.len, @sizeOf(std.json.Value), page.retainedBytes(), budget.peak });
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "external lake native typed result construction widens heterogeneous values and unwinds allocation faults" {
    const Case = struct {
        fn run(a: A) !void {
            const page = try Page.native(a, &.{ "mixed", "integer", "string", "empty" }, 5);
            defer page.release();
            const values = [_]std.json.Value{ .{ .integer = 9007199254740993 }, .{ .float = 1.5 }, .{ .string = "borrowed" }, .null, .{ .bool = true } };
            for (values, 0..) |value, row| {
                try page.setNative(row, 0, value);
                try page.setNative(row, 1, .{ .integer = -9007199254740993 });
                try page.setNative(row, 2, .{ .string = "survives finish" });
            }
            try page.finishNative();
            try std.testing.expect(page.columns[0].values == .encoded);
            try std.testing.expect(page.columns[1].values == .i64);
            try std.testing.expect(page.columns[2].values == .bytes);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            for (values, 0..) |value, row| {
                const actual = try (Row{ .page = page, .index = row }).value(&scratch);
                try std.testing.expectEqualDeep(value, actual.object.get("mixed").?);
                try std.testing.expectEqual(@as(i64, -9007199254740993), actual.object.get("integer").?.integer);
                try std.testing.expect(actual.object.get("empty").? == .null);
            }
        }
    };
    try Case.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Case.run, .{});
}

test "external lake native result page keeps JSON null separate from SQL NULL under faults" {
    const Run = struct {
        fn run(a: A) !void {
            const page = try Page.native(a, &.{"json"}, 3);
            defer page.release();
            try page.setNativeCell(0, 0, .null, false);
            try page.setNativeCell(1, 0, .{ .integer = 42 }, false);
            try page.setNativeCell(2, 0, .null, true);
            try page.finishNative();
            try std.testing.expect(!page.columns[0].nulls.isNull(0));
            try std.testing.expect(!page.columns[0].nulls.isNull(1));
            try std.testing.expect(page.columns[0].nulls.isNull(2));
            try std.testing.expect(try page.columns[0].values.encoded.at(0) == .null);
            try std.testing.expectEqual(@as(i64, 42), (try page.columns[0].values.encoded.at(1)).integer);
        }
    };
    try Run.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

test "external lake projection cache reuses schema across closed pages without retaining leases" {
    const a = std.testing.allocator;
    var cache = ProjectionCache.init(a);
    defer cache.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const options = @import("types.zig").LookupOptions{ .fields = &.{ "amount", "*", "-unused", "amount" } };
    for (0..256) |i| {
        const values = [_]i64{9007199254740993 + @as(i64, @intCast(i))};
        const refs = [_]rows.RowRef{.{ .relational_key = "row" }};
        const columns = [_]rows.ColumnVector{
            .{ .name = "unused", .values = .{ .json = &.{"invalid JSON must remain unread"} } },
            .{ .name = "amount", .values = .{ .i64 = &values } },
            .{ .name = "flag", .values = .{ .bool = &.{true} } },
        };
        const page = try Page.copy(a, .{ .snapshot = .{ .table_id = "test", .snapshot_id = "one" }, .row_refs = &refs, .columns = &columns }, &.{0});
        defer page.release();
        const prepared = try cache.prepare(.{ .page = page, .index = 0 }, scratch.allocator(), &scratch, options);
        try std.testing.expectEqual(@as(usize, 2), prepared.fields.len);
        for (prepared.fields) |field| try std.testing.expect(field.value == .scalar);
        try std.testing.expectEqual(@as(usize, 1), page.refs.load(.monotonic));
        const encoded = try std.json.Stringify.valueAlloc(a, prepared, .{});
        defer a.free(encoded);
        try std.testing.expectEqual(encoded.len, prepared.encoded_len);
    }
    try std.testing.expectEqual(@as(usize, 1), cache.builds);
}

test "external lake prepared scalars preserve canonical extremes and inline fitting values without allocation" {
    const a = std.testing.allocator;
    for ([_]std.json.Value{
        .{ .integer = std.math.minInt(i64) },  .{ .integer = std.math.maxInt(i64) },
        .{ .float = -1.7976931348623157e308 }, .{ .float = 5e-324 },
        .{ .float = -0.0 },                    .{ .bool = true },
        .null,
    }) |value| {
        const canonical = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(canonical);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const fits = canonical.len <= 24;
        const prepared = try Prepared.Value.prepare(if (fits) std.testing.failing_allocator else arena.allocator(), value, false);
        try std.testing.expect(if (fits) prepared == .scalar else prepared == .raw);
        const encoded = try std.json.Stringify.valueAlloc(a, prepared, .{});
        defer a.free(encoded);
        try std.testing.expectEqualStrings(canonical, encoded);
    }
}
