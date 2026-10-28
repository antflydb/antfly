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
    pub fn retainedBytes(self: *const Page) usize {
        return @sizeOf(Page) +| self.arena.queryCapacity() +| (if (self.owner) |owner| owner.retained_bytes else 0);
    }
    pub fn retain(self: *Page) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *Page) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.a;
        if (self.owner) |owner| owner.release();
        self.arena.deinit();
        a.destroy(self);
    }
    pub fn row(self: *Page, index: usize) Row {
        std.debug.assert(index < self.len);
        self.retain();
        return .{ .page = self, .index = if (self.selection) |selection| selection[index] else index };
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
                .encoded => return error.UnsupportedSqlExecution,
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
        string: []const u8,
        raw: []const u8,
        fn length(self: @This()) !usize {
            return switch (self) {
                .string => |v| stringLength(v),
                .raw => |v| v.len,
            };
        }
        pub fn jsonStringify(self: @This(), w: *std.json.Stringify) std.json.Stringify.Error!void {
            switch (self) {
                .string => |v| try w.write(v),
                .raw => |v| {
                    try w.beginWriteRaw();
                    try w.writer.writeAll(v);
                    w.endWriteRaw();
                },
            }
        }
        fn prepare(a: A, value: std.json.Value, borrow_string: bool) !@This() {
            if (borrow_string and value == .string and std.unicode.utf8ValidateSlice(value.string)) return .{ .string = value.string };
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
        var length = try std.math.add(usize, bytes.len, 2);
        for (bytes) |byte| length = try std.math.add(usize, length, switch (byte) {
            '"', '\\', 8, 12, '\n', '\r', '\t' => @as(usize, 1),
            0...7, 11, 14...31 => @as(usize, 5),
            else => @as(usize, 0),
        });
        return length;
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
    }
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
