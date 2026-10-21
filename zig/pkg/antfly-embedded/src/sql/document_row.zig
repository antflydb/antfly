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

//! Schema-bound SQL projection over native document snapshots. Shape is derived
//! only from declarations, never from sampled rows. All returned values belong
//! to the supplied request/page arena; a backend must retain its native read
//! transaction across pages and must not use this decoder to manufacture one.
const std = @import("std");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");
const typed_json = @import("../storage/typed_json.zig");
const datetime = @import("../datetime.zig");
const Json = std.json.Value;

/// Generic over the parsed native schema to keep the SQL decoder independent
/// of native storage/build dependencies. Unspecified/dynamic fields are not
/// columns. Conflicting declarations have a stable JSON supertype.
pub fn deriveColumns(alloc: std.mem.Allocator, schema: anytype) ![]const catalog.Column {
    const Accumulator = struct { column: catalog.Column, required_count: usize };
    var entries: std.StringArrayHashMapUnmanaged(Accumulator) = .empty;
    defer entries.deinit(alloc);
    for (schema.document_schemas) |document| {
        for (document.properties) |property| {
            if (std.mem.eql(u8, property.name, "_id")) continue;
            const kind = propertyType(property);
            const element_type = if (comptime @hasField(@TypeOf(property), "sql_type"))
                if (property.sql_type) |identity| @import("../common/sql_builtin_type.zig").Type.fromWire(identity) else null
            else
                null;
            const required = for (document.required_fields) |name| {
                if (std.mem.eql(u8, name, property.name)) break true;
            } else false;
            const nullable = !required or property.allows_null or property.field_type == null;
            const entry = try entries.getOrPut(alloc, property.name);
            if (!entry.found_existing) entry.value_ptr.* = .{
                .column = .{ .name = property.name, .path = property.name, .type = kind, .element_type = element_type, .nullable = nullable },
                .required_count = @intFromBool(!nullable),
            } else {
                if (entry.value_ptr.column.type != kind or entry.value_ptr.column.element_type != element_type) {
                    entry.value_ptr.column.type = .json;
                    entry.value_ptr.column.element_type = null;
                    entry.value_ptr.column.nullable = true;
                }
                entry.value_ptr.column.nullable = entry.value_ptr.column.nullable or nullable;
                entry.value_ptr.required_count += @intFromBool(!nullable);
            }
        }
    }
    const columns = try alloc.alloc(catalog.Column, entries.count());
    for (entries.values(), columns) |entry, *column| {
        column.* = entry.column;
        column.nullable = entry.column.nullable or entry.required_count != schema.document_schemas.len;
        column.name = try alloc.dupe(u8, entry.column.name);
        column.path = column.name;
    }
    std.mem.sort(catalog.Column, columns, {}, struct {
        fn lessThan(_: void, a: catalog.Column, b: catalog.Column) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lessThan);
    return columns;
}

/// Recover logical UUID semantics without changing the physical keyword layout.
pub fn relationalType(schema: anytype, name: []const u8, physical: ast.ColumnType) ast.ColumnType {
    if (physical != .string) return physical;
    for (schema.document_schemas) |document_| for (document_.properties) |property| {
        if (std.mem.eql(u8, property.name, name) and propertyType(property) == .uuid) return .uuid;
    };
    return physical;
}

fn propertyType(property: anytype) ast.ColumnType {
    if (property.field_type) |name| if (std.mem.eql(u8, name, "sql_array")) return .array;
    if (comptime @hasField(@TypeOf(property), "sql_type")) {
        if (property.sql_type == .uuid) return .uuid;
    }
    const name = property.field_type orelse return .json;
    if (std.mem.eql(u8, name, "integer") or property.integer_only) return .integer;
    if (std.mem.eql(u8, name, "number") or std.mem.eql(u8, name, "numeric")) return .number;
    if (std.mem.eql(u8, name, "boolean")) return .boolean;
    if (std.mem.eql(u8, name, "datetime")) return .datetime;
    if (property.format != null and std.mem.eql(u8, property.format.?, "uuid") and
        (std.mem.eql(u8, name, "keyword") or std.mem.eql(u8, name, "string") or std.mem.eql(u8, name, "text"))) return .uuid;
    if (std.mem.eql(u8, name, "string") or std.mem.eql(u8, name, "text") or
        std.mem.eql(u8, name, "keyword") or std.mem.eql(u8, name, "html") or std.mem.eql(u8, name, "link")) return .string;
    return .json;
}

test "SQL UUID schema projection retains typed column and canonical cell" {
    const Property = struct { name: []const u8, field_type: ?[]const u8 = "keyword", format: ?[]const u8 = "uuid", integer_only: bool = false, allows_null: bool = false };
    const Document = struct { properties: []const Property, required_fields: []const []const u8 = &.{"id"} };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const columns = try deriveColumns(alloc, .{ .document_schemas = &[_]Document{.{ .properties = &.{.{ .name = "id" }} }} });
    try std.testing.expectEqual(ast.ColumnType.uuid, columns[0].type);
    const projected = try projectValue(alloc, .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = columns }, "key", 1, .{ .object = blk: {
        var value: std.json.ObjectMap = .empty;
        try value.put(alloc, "id", .{ .string = "A0EEBC999C0B4EF8BB6D6BB9BD380A11" });
        break :blk value;
    } }, &.{"id"});
    try std.testing.expectEqualStrings("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", projected.value.object.get("id").?.string);
}

pub fn decode(alloc: std.mem.Allocator, table: catalog.Table, id: []const u8, version: u64, bytes: []const u8, fields: []const []const u8) !catalog.Row {
    return (try Projection.init(alloc, table, fields)).decode(alloc, id, version, bytes);
}

/// Native document nulls have no SQL-null bitmap. A missing field is SQL NULL;
/// an explicit null in a JSON column is a JSON datum. Scalar explicit null is
/// SQL NULL. Names are literal root members, not dotted path expressions.
pub fn projectValue(alloc: std.mem.Allocator, table: catalog.Table, id: []const u8, version: u64, root: Json, fields: []const []const u8) !catalog.Row {
    return (try Projection.init(alloc, table, fields)).projectValue(alloc, id, version, root);
}

/// Decode a declared storage cell once while its native page or prepared
/// mutation remains pinned. Already typed cells stay borrowed; exact payloads
/// never pass through the scalar JSON placeholder coercion path.
pub fn declaredCell(alloc: std.mem.Allocator, column: catalog.Column, raw: catalog.Row.Cell) !catalog.Row.Cell {
    if (column.type == .array and !raw.sql_null and raw.array == null) {
        const kind = column.element_type orelse return error.InvalidSqlBackendResponse;
        const decoded = try @import("array_wire.zig").decodeBorrowed(alloc, kind, raw.value, .{});
        const array = try alloc.create(@import("array_value.zig").Value);
        array.* = decoded.value;
        return catalog.Row.Cell.typedArray(array);
    }
    return @import("describe.zig").coerceDatum(alloc, raw, column.type, column.element_type);
}

/// Compile once per scan, not once per row. Binding is O(schema + projection)
/// and decoding O(projected fields + selected JSON bytes), independent of the
/// width of the declared schema. The scan arena owns all names and slots.
pub const Projection = struct {
    columns: []const catalog.Column,
    has_typed_cells: bool = false,
    source_slots: std.StringHashMapUnmanaged(usize) = .empty,
    next_source: []usize = &.{},

    pub fn init(alloc: std.mem.Allocator, table: catalog.Table, fields: []const []const u8) !Projection {
        var declared: std.StringHashMapUnmanaged(catalog.Column) = .empty;
        defer declared.deinit(alloc);
        try declared.ensureTotalCapacity(alloc, @intCast(table.columns.len));
        for (table.columns) |column| try declared.put(alloc, column.name, column);
        var selected: std.StringArrayHashMapUnmanaged(catalog.Column) = .empty;
        defer selected.deinit(alloc);
        for (fields) |name| {
            if (std.mem.eql(u8, name, "_id")) continue;
            const column = declared.get(name) orelse return error.UndefinedColumn;
            try selected.put(alloc, name, column);
        }
        const columns = try alloc.alloc(catalog.Column, selected.count());
        var initialized: usize = 0;
        errdefer {
            for (columns[0..initialized]) |column| {
                if (column.path.ptr != column.name.ptr) alloc.free(column.path);
                alloc.free(column.name);
            }
            alloc.free(columns);
        }
        var has_typed_cells = false;
        for (selected.values(), columns) |column, *out| {
            out.* = column;
            out.name = try alloc.dupe(u8, column.name);
            errdefer alloc.free(out.name);
            out.path = if (std.mem.eql(u8, column.name, column.path)) out.name else try alloc.dupe(u8, column.path);
            initialized += 1;
            has_typed_cells = has_typed_cells or column.type == .array or column.element_type == .numeric;
        }
        return .{ .columns = columns, .has_typed_cells = has_typed_cells };
    }

    /// Ranked hits need a root-member routing plan; ordinary scan projections
    /// keep their existing initialization cost and do not build this directory.
    pub fn initSearch(alloc: std.mem.Allocator, table: catalog.Table, fields: []const []const u8) !Projection {
        var result = try init(alloc, table, fields);
        errdefer result.deinit(alloc);
        result.next_source = try alloc.alloc(usize, result.columns.len);
        for (result.columns, result.next_source, 0..) |column, *next, slot| {
            const entry = try result.source_slots.getOrPut(alloc, column.path);
            next.* = if (entry.found_existing) entry.value_ptr.* else std.math.maxInt(usize);
            entry.value_ptr.* = slot;
        }
        return result;
    }

    pub fn deinit(self: Projection, alloc: std.mem.Allocator) void {
        var slots = self.source_slots;
        slots.deinit(alloc);
        alloc.free(self.next_source);
        for (self.columns) |column| {
            if (column.path.ptr != column.name.ptr) alloc.free(column.path);
            alloc.free(column.name);
        }
        alloc.free(self.columns);
    }

    /// The returned directory belongs to the page, not this scan/cursor. Bind
    /// once per page, and reuse across every row regardless of array length.
    pub fn pageLayout(self: Projection, alloc: std.mem.Allocator) !?*const catalog.Row.TypedLayout {
        if (!self.has_typed_cells) return null;
        const names = try alloc.alloc([]const u8, self.columns.len);
        for (self.columns, names) |column, *name| name.* = try alloc.dupe(u8, column.name);
        return try catalog.Row.TypedLayout.init(alloc, names);
    }

    /// Adapt an already parsed native projection. Metadata is page-owned and
    /// payloads borrow that same page's pinned JSON tree. No JSON reparsing or
    /// nested JSONB/string cloning occurs. Legacy scalar-only pages stay cheap.
    pub fn adaptBorrowed(self: Projection, alloc: std.mem.Allocator, layout: ?*const catalog.Row.TypedLayout, row: catalog.Row) !catalog.Row {
        if (!self.has_typed_cells) return row;
        if (row.typed_cells) |native| {
            var direct = native.layout.names.len == self.columns.len;
            if (direct) for (self.columns, native.layout.names) |column, name| {
                if (!std.mem.eql(u8, column.name, name) or !std.mem.eql(u8, column.path, name)) {
                    direct = false;
                    break;
                }
                if (column.type == .json and (try row.frozenJson(name) != null or try row.encodedJson(name) != null)) continue;
                const cell = try row.cell(name);
                if (cell.sql_null and !column.nullable) return error.InvalidSqlBackendResponse;
                if (!cell.sql_null and ((column.type == .array and cell.array == null) or
                    (column.element_type == .numeric and column.type != .array and cell.numeric == null)))
                {
                    direct = false;
                    break;
                }
            };
            // Native storage already owns complete cells. Retain physical
            // column blocks and page directories through the projection seam.
            if (direct) return row;
        }
        const directory = layout orelse if (row.typed_cells) |cells| cells.layout else return error.InvalidSqlBackendResponse;
        if (directory.names.len != self.columns.len) return error.InvalidSqlBackendResponse;
        const cells = try alloc.alloc(catalog.Row.Cell, self.columns.len);
        const presence = try alloc.alloc(bool, self.columns.len);
        for (self.columns, cells, presence, directory.names) |column, *cell, *present, name| {
            if (!std.mem.eql(u8, column.name, name)) return error.InvalidSqlBackendResponse;
            present.* = try row.hasField(column.path);
            const raw = try row.cell(column.path);
            if (raw.sql_null and !column.nullable) return error.InvalidSqlBackendResponse;
            cell.* = try declaredCell(alloc, column, raw);
        }
        var result = row;
        result.value = .null;
        result.sql_nulls = null;
        result.pattern_sources = null;
        result.typed_cells = .{ .layout = directory, .values = cells, .presence = presence };
        return result;
    }

    pub const SearchSource = union(enum) { value: Json, bytes: []const u8, fields: []const @import("../storage/document_projection.zig").FieldValue };
    pub const SearchMetadata = struct { score: ?f64 = null, highlights: ?Json = null };

    /// Copy only selected payloads into their final native cells. Raw stored
    /// hits validate every subtree/duplicate key while materializing selected
    /// root members only. Metadata overrides never mutate or copy the hit tree.
    pub fn projectSearch(self: Projection, alloc: std.mem.Allocator, scratch: std.mem.Allocator, layout: *const catalog.Row.TypedLayout, id: []const u8, source: SearchSource, metadata: SearchMetadata) !catalog.Row {
        return self.projectSearchEncoded(alloc, scratch, layout, id, source, metadata, null);
    }
    /// Optional canonical JSON columns. Placeholder cells are private to the
    /// native reader; ordinary cell access must parse that column on demand.
    pub fn projectSearchEncoded(self: Projection, alloc: std.mem.Allocator, scratch: std.mem.Allocator, layout: *const catalog.Row.TypedLayout, id: []const u8, source: SearchSource, metadata: SearchMetadata, encoded: ?[]?[]const u8) !catalog.Row {
        if (encoded) |slots| {
            if (slots.len != self.columns.len) return error.InvalidSqlBackendResponse;
            @memset(slots, null);
        }

        if (layout.names.len != self.columns.len or self.next_source.len != self.columns.len) return error.InvalidSqlBackendResponse;
        const cells = try alloc.alloc(catalog.Row.Cell, self.columns.len);
        @memset(cells, .{});
        const presence = try alloc.alloc(bool, self.columns.len);
        @memset(presence, false);
        const Sink = struct {
            projection: Projection,
            alloc: std.mem.Allocator,
            scratch: std.mem.Allocator,
            cells: []catalog.Row.Cell,
            presence: []bool,
            encoded: ?[]?[]const u8,
            fn put(target: *@This(), name: []const u8, value: Json) !void {
                var slot = target.projection.source_slots.get(name) orelse return;
                while (slot != std.math.maxInt(usize)) {
                    const column = target.projection.columns[slot];
                    const sql_null = value == .null and column.type != .json;
                    if (sql_null and !column.nullable) return error.InvalidSqlBackendResponse;
                    const owned = if (sql_null) Json.null else if (column.type == .array) try typed_json.clone(target.alloc, value) else if (column.element_type == .numeric and column.type == .number) value else try coerce(target.alloc, value, column.type);
                    target.cells[slot] = try declaredCell(target.alloc, column, .{ .value = owned, .sql_null = sql_null });
                    target.presence[slot] = true;
                    slot = target.projection.next_source[slot];
                }
            }
            fn member(raw: *anyopaque, name: []const u8, bytes: []const u8) !void {
                const target: *@This() = @ptrCast(@alignCast(raw));
                if (std.mem.eql(u8, name, "score") or std.mem.eql(u8, name, "_highlights") or !target.projection.source_slots.contains(name)) return;
                const first = target.projection.source_slots.get(name).?;
                const column = target.projection.columns[first];
                const homogeneous = blk: {
                    var slot = first;
                    while (slot != std.math.maxInt(usize)) {
                        const definition = target.projection.columns[slot];
                        if (definition.type != column.type or definition.element_type != column.element_type) break :blk false;
                        slot = target.projection.next_source[slot];
                    }
                    break :blk true;
                };
                if (homogeneous and column.type == .json and target.encoded != null and std.mem.indexOfScalar(u8, "{[", std.mem.trim(u8, bytes, " \t\r\n")[0]) != null) {
                    // The root scanner (or digest-bound directory) already
                    // admitted this subtree, including duplicate keys. The raw
                    // codec normalizes tokens without building a DOM.
                    const owned = try @import("json_bytes.zig").fromValidated(target.alloc, target.scratch, bytes);
                    var slot = first;
                    while (slot != std.math.maxInt(usize)) {
                        target.encoded.?[slot] = owned;
                        target.cells[slot] = catalog.Row.Cell.json(.null);
                        target.presence[slot] = true;
                        slot = target.projection.next_source[slot];
                    }
                    return;
                }
                if (homogeneous and column.type == .array and !std.mem.eql(u8, std.mem.trim(u8, bytes, " \t\r\n"), "null")) {
                    const decoded = try @import("array_wire.zig").decodeBytesLeakyMeasured(target.alloc, column.element_type orelse return error.InvalidSqlBackendResponse, bytes, .{});
                    const value = try target.alloc.create(@import("array_value.zig").Value);
                    value.* = decoded.value;
                    var slot = first;
                    while (slot != std.math.maxInt(usize)) {
                        const definition = target.projection.columns[slot];
                        if (definition.type != .array or definition.element_type != column.element_type) return error.InvalidSqlBackendResponse;
                        target.cells[slot] = catalog.Row.Cell.typedArray(value);
                        target.presence[slot] = true;
                        slot = target.projection.next_source[slot];
                    }
                    return;
                }
                const work = @import("memory_budget.zig").reclaimingAllocator(target.scratch);
                var scalar = std.json.Scanner.initCompleteInput(work, bytes);
                defer scalar.deinit();
                const type_ = try scalar.peekNextTokenType();
                if (type_ != .object_begin and type_ != .array_begin) {
                    const token = try scalar.nextAllocMax(work, .alloc_if_needed, bytes.len);
                    defer switch (token) {
                        .allocated_string, .allocated_number => |text| work.free(text),
                        else => {},
                    };
                    const value: Json = switch (token) {
                        .string, .allocated_string => |text| .{ .string = text },
                        .number, .allocated_number => |text| .{ .number_string = text },
                        .true => .{ .bool = true },
                        .false => .{ .bool = false },
                        .null => .null,
                        else => return error.UnexpectedToken,
                    };
                    if (try scalar.next() != .end_of_document) return error.UnexpectedToken;
                    return target.put(name, value);
                }
                // The temporary DOM dies after this selected field; put owns
                // all returned payloads, including exact NUMERIC coefficients.
                var parsed = try std.json.parseFromSlice(Json, @import("memory_budget.zig").reclaimingAllocator(target.scratch), bytes, .{ .parse_numbers = false });
                defer parsed.deinit();
                try target.put(name, parsed.value);
            }
        };
        var sink: Sink = .{ .projection = self, .alloc = alloc, .scratch = scratch, .cells = cells, .presence = presence, .encoded = encoded };
        switch (source) {
            // A revision-bound directory already validated the complete root;
            // range digests bind these selected fields to that immutable cut.
            .fields => |fields| for (fields) |field| try Sink.member(&sink, field.name, field.value),
            .value => |root| {
                if (root != .object) return error.InvalidSqlBackendResponse;
                var fields = self.source_slots.iterator();
                while (fields.next()) |entry| {
                    const name = entry.key_ptr.*;
                    if (std.mem.eql(u8, name, "score") or std.mem.eql(u8, name, "_highlights")) continue;
                    if (root.object.get(name)) |value| try sink.put(name, value);
                }
            },
            .bytes => |bytes| {
                var scanner = std.json.Scanner.initCompleteInput(scratch, bytes);
                defer scanner.deinit();
                if (try scanner.peekNextTokenType() != .object_begin) return error.InvalidSqlBackendResponse;
                try @import("antfly-json").scanValue(@import("memory_budget.zig").reclaimingAllocator(scratch), &scanner, bytes.len, .{ .ctx = &sink, .member_fn = Sink.member });
                if (try scanner.next() != .end_of_document) return error.InvalidSqlBackendResponse;
            },
        }
        try sink.put("score", if (metadata.score) |score| .{ .float = score } else .null);
        if (metadata.highlights) |value| try sink.put("_highlights", value);
        for (self.columns, layout.names, cells, presence) |column, name, cell, present| {
            if (!std.mem.eql(u8, column.name, name) or (!present and !column.nullable) or (cell.sql_null and !column.nullable)) return error.InvalidSqlBackendResponse;
        }
        return .{ .id = try alloc.dupe(u8, id), .version = 0, .value = .null, .typed_cells = .{ .layout = layout, .values = cells, .presence = presence } };
    }

    pub fn decode(self: Projection, alloc: std.mem.Allocator, id: []const u8, version: u64, bytes: []const u8) !catalog.Row {
        var parsed = std.json.parseFromSlice(Json, alloc, bytes, .{ .parse_numbers = false }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSqlBackendResponse,
        };
        defer parsed.deinit();
        return self.projectValue(alloc, id, version, parsed.value);
    }

    pub fn projectValue(self: Projection, alloc: std.mem.Allocator, id: []const u8, version: u64, root: Json) !catalog.Row {
        if (root != .object) return error.InvalidSqlBackendResponse;
        if (self.has_typed_cells) {
            const layout = (try self.pageLayout(alloc)).?;
            const cells = try alloc.alloc(catalog.Row.Cell, self.columns.len);
            const presence = try alloc.alloc(bool, self.columns.len);
            for (self.columns, cells, presence) |column, *cell, *present| {
                const raw = root.object.get(column.path);
                present.* = raw != null;
                const sql_null = raw == null or (raw.? == .null and column.type != .json);
                if (sql_null and !column.nullable) return error.InvalidSqlBackendResponse;
                // Exact decimals parse their original lexeme directly into
                // owned coefficients. Arrays need an owned envelope because
                // primitive cells can borrow its strings/JSONB subtrees.
                const value: Json = if (sql_null) .null else if (column.element_type == .numeric and column.type == .number)
                    raw.?
                else if (column.type == .array)
                    try typed_json.clone(alloc, raw.?)
                else
                    try coerce(alloc, raw.?, column.type);
                cell.* = try declaredCell(alloc, column, .{ .value = value, .sql_null = sql_null });
            }
            return .{ .id = try alloc.dupe(u8, id), .version = version, .value = .null, .typed_cells = .{ .layout = layout, .values = cells, .presence = presence } };
        }
        var object: std.json.ObjectMap = .empty;
        try object.ensureTotalCapacity(alloc, @intCast(self.columns.len));
        const nulls = try alloc.alloc(bool, self.columns.len);
        for (self.columns, nulls) |column, *sql_null| {
            const raw = root.object.get(column.path);
            sql_null.* = raw == null or (raw.? == .null and column.type != .json);
            const value: Json = if (sql_null.*) .null else try coerce(alloc, raw.?, column.type);
            try object.put(alloc, try alloc.dupe(u8, column.name), value);
        }
        return .{ .id = try alloc.dupe(u8, id), .version = version, .value = .{ .object = object }, .sql_nulls = nulls };
    }
};

fn coerce(alloc: std.mem.Allocator, value: Json, kind: ast.ColumnType) !Json {
    return switch (kind) {
        .array => error.UnsupportedSqlShape,
        .json => typed_json.clone(alloc, value),
        .integer => switch (value) {
            .integer => value,
            .number_string => |text| .{ .integer = try exactInteger(text) },
            else => error.SqlTypeMismatch,
        },
        .number => blk: {
            const number: f64 = switch (value) {
                .integer => |integer| @floatFromInt(integer),
                .float => |number| number,
                .number_string => |text| std.fmt.parseFloat(f64, text) catch return error.SqlTypeMismatch,
                else => return error.SqlTypeMismatch,
            };
            if (!std.math.isFinite(number)) return error.SqlNumericOutOfRange;
            break :blk .{ .float = number };
        },
        .boolean => if (value == .bool) value else error.SqlTypeMismatch,
        .string => if (value == .string) typed_json.clone(alloc, value) else error.SqlTypeMismatch,
        .uuid => if (value == .string) .{ .string = @import("../common/uuid.zig").canonicalAlloc(alloc, value.string) catch |err| switch (err) {
            error.InvalidUuid => return error.SqlTypeMismatch,
            else => return err,
        } } else error.SqlTypeMismatch,
        .datetime => blk: {
            const ns: u64 = switch (value) {
                .integer => |integer| std.math.cast(u64, integer) orelse return error.SqlTypeMismatch,
                .number_string => |text| std.fmt.parseInt(u64, text, 10) catch return error.SqlTypeMismatch,
                .string => |text| datetime.parseDateTimeToNs(text) orelse (std.fmt.parseInt(u64, text, 10) catch return error.SqlTypeMismatch),
                else => return error.SqlTypeMismatch,
            };
            break :blk .{ .string = try datetime.formatDateTimeNsAlloc(alloc, ns) };
        },
    };
}

/// The input is an already validated JSON numeric token. Accept integral
/// decimal/exponent spellings without rounding through binary floating point.
fn exactInteger(text: []const u8) !i64 {
    if (text.len == 0) return error.SqlTypeMismatch;
    const negative = text[0] == '-';
    var at: usize = @intFromBool(negative);
    var digits: usize = 0;
    var before: ?usize = null;
    var first: ?usize = null;
    var last: usize = 0;
    var magnitude: u64 = 0;
    while (at < text.len and text[at] != 'e' and text[at] != 'E') : (at += 1) {
        if (text[at] == '.') {
            if (before != null) return error.SqlTypeMismatch;
            before = digits;
            continue;
        }
        if (!std.ascii.isDigit(text[at])) return error.SqlTypeMismatch;
        if (text[at] != '0') {
            if (first == null) first = digits;
            last = digits;
        }
        digits += 1;
    }
    if (digits == 0) return error.SqlTypeMismatch;
    if (first == null) return 0;
    const exponent: i64 = if (at < text.len) std.fmt.parseInt(i64, text[at + 1 ..], 10) catch return error.SqlNumericOutOfRange else 0;
    const scale = std.math.add(i64, exponent, @as(i64, @intCast(before orelse digits)) - @as(i64, @intCast(last + 1))) catch return error.SqlNumericOutOfRange;
    if (scale < 0) return error.SqlTypeMismatch;
    if (last - first.? + 1 > 19 or scale > 19 - (last - first.? + 1)) return error.SqlNumericOutOfRange;
    var position: usize = 0;
    for (text[@intFromBool(negative)..at]) |digit| {
        if (digit == '.') continue;
        if (position >= first.? and position <= last) magnitude = magnitude * 10 + digit - '0';
        position += 1;
    }
    for (0..@intCast(scale)) |_| magnitude *= 10;
    if (negative) {
        if (magnitude == @as(u64, 1) << 63) return std.math.minInt(i64);
        const signed = std.math.cast(i64, magnitude) orelse return error.SqlNumericOutOfRange;
        return -signed;
    }
    return std.math.cast(i64, magnitude) orelse error.SqlNumericOutOfRange;
}

test "SQL document projection preserves missing null exact JSON and literal names" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = &.{
        .{ .name = "id", .path = "id", .type = .integer },
        .{ .name = "payload", .path = "payload", .type = .json },
        .{ .name = "absent", .path = "absent", .type = .json },
        .{ .name = "nested", .path = "nested", .type = .json },
        .{ .name = "a.b", .path = "a.b", .type = .integer },
        .{ .name = "scalar", .path = "scalar", .type = .string },
    } };
    const row = try decode(arena.allocator(), table, "key", 42,
        \\{"id":9007199254740993.0,"payload":null,"nested":{"n":18446744073709551615},"a.b":7,"a":{"b":9},"scalar":null,"ignored":false}
    , &.{ "_id", "id", "payload", "absent", "nested", "a.b", "scalar", "id" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try row.cell("id")).value.integer);
    try std.testing.expect(!(try row.cell("payload")).sql_null);
    try std.testing.expect((try row.cell("absent")).sql_null);
    try std.testing.expect((try row.cell("scalar")).sql_null);
    try std.testing.expectEqualStrings("18446744073709551615", (try row.cell("nested")).value.object.get("n").?.number_string);
    try std.testing.expectEqual(@as(i64, 7), (try row.cell("a.b")).value.integer);
    try std.testing.expectEqualStrings("key", (try row.cell("_id")).value.string);
    try std.testing.expectEqual(@as(usize, 6), row.value.object.count());
    try std.testing.expectError(error.UndefinedColumn, decode(arena.allocator(), table, "key", 1, "{}", &.{"unknown"}));
    try std.testing.expectError(error.InvalidSqlBackendResponse, decode(arena.allocator(), table, "key", 1, "[]", &.{}));
}

test "SQL document integer decoding is exact and bounded" {
    for ([_]struct { text: []const u8, value: i64 }{
        .{ .text = "9007199254740993.0", .value = 9007199254740993 },
        .{ .text = "-9223372036854775808", .value = std.math.minInt(i64) },
        .{ .text = "9223372036854775807", .value = std.math.maxInt(i64) },
        .{ .text = "12.3400e2", .value = 1234 },
        .{ .text = "0e99999999999999999999999", .value = 0 },
    }) |case| try std.testing.expectEqual(case.value, try exactInteger(case.text));
    try std.testing.expectError(error.SqlTypeMismatch, exactInteger("1.0000000000000001"));
    try std.testing.expectError(error.SqlNumericOutOfRange, exactInteger("9223372036854775808"));
    try std.testing.expectError(error.SqlNumericOutOfRange, exactInteger("1e99999999999999999999999"));
}

test "SQL native array projection retains presence and page ownership" {
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    var retained = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer retained.deinit();
    const a = source.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{
        .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
        .{ .name = "j", .path = "j", .type = .json },
        .{ .name = "missing", .path = "missing", .type = .string },
    } };
    const projection = try Projection.init(std.testing.allocator, table, &.{ "a", "j", "missing" });
    defer projection.deinit(std.testing.allocator);
    const layout = try projection.pageLayout(a);
    const parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"a":{"dimensions":[{"length":2,"lower_bound":-3}],"values":["9223372036854775807",null],"sql_nulls":[false,true]},"j":null}
    , .{ .allocate = .alloc_always });
    const row = try projection.adaptBorrowed(a, layout, .{ .id = "key", .version = 42, .value = parsed.value, .sql_nulls = &.{ false, false } });
    const second = try projection.adaptBorrowed(a, layout, .{ .id = "other", .version = 43, .value = parsed.value, .sql_nulls = &.{ false, false } });
    try std.testing.expect(row.typed_cells.?.layout.names.ptr == second.typed_cells.?.layout.names.ptr);
    try std.testing.expect(try row.hasField("j"));
    try std.testing.expect(!try row.hasField("missing"));
    try std.testing.expect(!(try row.cell("j")).sql_null);
    try std.testing.expect((try row.cell("missing")).sql_null);
    const owned = try row.cloneOwned(retained.allocator());
    _ = source.reset(.free_all);
    try std.testing.expectEqualStrings("key", owned.id);
    try std.testing.expectEqual(@as(u64, 42), owned.version);
    try std.testing.expect(!try owned.hasField("missing"));
    const array = (try owned.cell("a")).array.?;
    try std.testing.expectEqual(@as(i32, -3), array.dimensions[0].lower);
    try std.testing.expectEqual(std.math.maxInt(i64), array.flatCell(0).value.integer);
    try std.testing.expect(array.flatCell(1).sql_null);
    var malformed = owned;
    malformed.typed_cells.?.presence = &.{false};
    try std.testing.expectError(error.InvalidSqlBackendResponse, malformed.hasField("a"));
}

test "SQL NUMERIC native projection matches PostgreSQL senders without floating conversion" {
    const fixture = try std.json.parseFromSlice(struct {
        senders: []const struct { input: []const u8, binary: []const u8 },
    }, std.testing.allocator, @embedFile("fixtures/sql_exact_numeric_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 65), fixture.value.senders.len);
    const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{
        .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric },
    } };
    const projection = try Projection.init(std.testing.allocator, table, &.{"n"});
    defer projection.deinit(std.testing.allocator);
    for (fixture.value.senders) |entry| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const json = try std.json.Stringify.valueAlloc(a, .{ .n = entry.input }, .{});
        const row = try projection.decode(a, "row", 9, json);
        const cell = try row.cell("n");
        try std.testing.expect(cell.numeric != null and !cell.sql_null and cell.value == .null);
        var ctx: @import("numeric_value.zig").Context = .{ .alloc = a };
        const wire = try @import("numeric_binary.zig").encodeAlloc(&ctx, cell.numeric.?.*);
        const expected = try a.alloc(u8, entry.binary.len / 2);
        _ = try std.fmt.hexToBytes(expected, entry.binary);
        try std.testing.expectEqualSlices(u8, expected, wire);
    }
}

test "SQL NUMERIC native projection owns exact coefficients presence and mixed typed fields" {
    const Run = struct {
        fn run(backing: std.mem.Allocator) !void {
            var source = std.heap.ArenaAllocator.init(backing);
            defer source.deinit();
            var retained = std.heap.ArenaAllocator.init(backing);
            defer retained.deinit();
            const a = source.allocator();
            const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{
                .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric },
                .{ .name = "zero", .path = "zero", .type = .number, .element_type = .numeric },
                .{ .name = "missing", .path = "missing", .type = .number, .element_type = .numeric },
                .{ .name = "j", .path = "j", .type = .json },
                .{ .name = "a", .path = "a", .type = .array, .element_type = .text },
                .{ .name = "label", .path = "label", .type = .string },
            } };
            const projection = try Projection.init(a, table, &.{ "n", "zero", "missing", "j", "a", "label" });
            const row = try projection.decode(a, "row", 42,
                \\{"n":9007199254740993.1200,"zero":null,"j":null,"a":{"dimensions":[{"length":1,"lower_bound":-3}],"values":["owned"],"sql_nulls":[false]},"label":"retained"}
            );
            const owned = try row.cloneOwned(retained.allocator());
            _ = source.reset(.free_all);
            try std.testing.expectEqual(@as(u64, 42), owned.version);
            try std.testing.expect(try owned.hasField("zero"));
            try std.testing.expect(!try owned.hasField("missing"));
            try std.testing.expect((try owned.cell("zero")).sql_null);
            try std.testing.expect((try owned.cell("missing")).sql_null);
            try std.testing.expect(!(try owned.cell("j")).sql_null);
            const number = (try owned.cell("n")).numeric.?;
            try std.testing.expectEqual(@as(u16, 4), number.scale);
            var ctx: @import("numeric_value.zig").Context = .{ .alloc = retained.allocator() };
            const text = try @import("numeric_value.zig").format(&ctx, number.*);
            try std.testing.expectEqualStrings("9007199254740993.1200", text);
            const array = (try owned.cell("a")).array.?;
            try std.testing.expectEqual(@as(i32, -3), array.dimensions[0].lower);
            try std.testing.expectEqualStrings("owned", array.flatCell(0).value.string);
            try std.testing.expectEqualStrings("retained", (try owned.cell("label")).value.string);
        }
    };
    try Run.run(std.testing.allocator);
    // Arena growth can use address-dependent heap remaps. Force the fallback
    // allocation path so the exhaustive failure indexes are reproducible.
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
}

test "SQL NUMERIC borrowed projection shares page layout and rejects rounded backend cells" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{
        .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric, .nullable = false },
        .{ .name = "cold", .path = "cold", .type = .json },
    } };
    const projection = try Projection.init(a, table, &.{"n"});
    const layout = try projection.pageLayout(a);
    const parsed = try std.json.parseFromSlice(Json, a, "{\"n\":1.2000}", .{ .parse_numbers = false });
    const first = try projection.adaptBorrowed(a, layout, .{ .id = "one", .version = 1, .value = parsed.value });
    const second = try projection.adaptBorrowed(a, layout, .{ .id = "two", .version = 2, .value = parsed.value });
    try std.testing.expect(first.typed_cells.?.layout.names.ptr == second.typed_cells.?.layout.names.ptr);
    try std.testing.expectEqual(@as(u16, 4), (try first.cell("n")).numeric.?.scale);
    try std.testing.expectEqual(@as(usize, 1), first.typed_cells.?.values.len);
    var object: std.json.ObjectMap = .empty;
    try object.put(a, "n", .{ .float = 1.2 });
    try std.testing.expectError(error.SqlTypeMismatch, projection.adaptBorrowed(a, layout, .{ .id = "bad", .version = 1, .value = .{ .object = object } }));
    try std.testing.expectError(error.InvalidSqlBackendResponse, projection.decode(a, "null", 1, "{\"n\":null}"));
}

test "SQL document declared shape and projection clean up allocation failures" {
    const Fixture = struct {
        const Property = struct { name: []const u8, field_type: ?[]const u8 = null, format: ?[]const u8 = null, integer_only: bool = false, allows_null: bool = false };
        const Document = struct { properties: []const Property, required_fields: []const []const u8 = &.{} };
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const alloc = arena.allocator();
            const columns = try deriveColumns(alloc, .{ .document_schemas = &[_]Document{
                .{ .properties = &.{ .{ .name = "id", .field_type = "integer" }, .{ .name = "mixed", .field_type = "integer" }, .{ .name = "nested", .field_type = "object" } }, .required_fields = &.{ "id", "mixed" } },
                .{ .properties = &.{ .{ .name = "id", .field_type = "integer" }, .{ .name = "mixed", .field_type = "string" } }, .required_fields = &.{ "id", "mixed" } },
            } });
            const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .storage_mode = .document, .columns = columns };
            try std.testing.expect(!(try table.column("id")).nullable);
            try std.testing.expectEqual(ast.ColumnType.json, (try table.column("mixed")).type);
            try std.testing.expect((try table.column("mixed")).nullable);
            try std.testing.expect((try table.column("nested")).nullable);
            const row = try decode(alloc, table, "one", 5, "{\"id\":1,\"mixed\":\"text\",\"nested\":{\"exact\":9007199254740993}}", &.{ "id", "mixed", "nested" });
            try std.testing.expectEqualStrings("9007199254740993", (try row.cell("nested")).value.object.get("exact").?.number_string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL document search projection owns only selected payloads with metadata and strict raw validation" {
    const Probe = struct {
        fn run(a: std.mem.Allocator, large: bool) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const ignored = try sa.alloc(u8, if (large) 1024 * 1024 else 16);
            @memset(ignored, 'x');
            const bytes = try std.fmt.allocPrint(sa, "{{\"ignored\":\"{s}\",\"i\":9007199254740993,\"n\":9007199254740993.1200,\"a\":{{\"dimensions\":[{{\"length\":1,\"lower_bound\":-3}}],\"values\":[\"owned\"],\"sql_nulls\":[false]}},\"j\":[null,{{\"s\":\"kept\"}}],\"z\":null,\"score\":100,\"_highlights\":\"ignored\"}}", .{ignored});
            const root = try std.json.parseFromSliceLeaky(Json, sa, bytes, .{ .parse_numbers = false });
            const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{
                .{ .name = "i", .path = "i", .type = .integer },
                .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric },
                .{ .name = "a", .path = "a", .type = .array, .element_type = .text },
                .{ .name = "j", .path = "j", .type = .json },
                .{ .name = "z", .path = "z", .type = .json },
                .{ .name = "missing", .path = "missing", .type = .string },
                .{ .name = "score", .path = "score", .type = .number },
                .{ .name = "_highlights", .path = "_highlights", .type = .json },
            } };
            const names = &[_][]const u8{ "i", "n", "a", "j", "z", "missing", "score", "_highlights" };
            const projection = try Projection.initSearch(a, table, names);
            defer projection.deinit(a);
            const FieldValue = @import("../storage/document_projection.zig").FieldValue;
            const Capture = struct {
                a: std.mem.Allocator,
                fields: std.ArrayList(FieldValue) = .empty,
                fn member(raw: *anyopaque, name: []const u8, value: []const u8) !void {
                    const capture: *@This() = @ptrCast(@alignCast(raw));
                    try capture.fields.append(capture.a, .{ .name = try capture.a.dupe(u8, name), .value = value });
                }
            };
            var capture: Capture = .{ .a = sa };
            var scanner = std.json.Scanner.initCompleteInput(sa, bytes);
            defer scanner.deinit();
            try @import("antfly-json").scanValue(sa, &scanner, bytes.len, .{ .ctx = &capture, .member_fn = Capture.member });
            for (0..3) |raw| {
                var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 64 * 1024 };
                var output = std.heap.ArenaAllocator.init(budget.allocator());
                defer output.deinit();
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                const oa = output.allocator();
                const layout = try catalog.Row.TypedLayout.init(oa, names);
                const row = try projection.projectSearch(oa, scratch.allocator(), layout, "row", if (raw == 1) .{ .bytes = bytes } else if (raw == 2) .{ .fields = capture.fields.items } else .{ .value = root }, .{ .score = 0.5, .highlights = .{ .string = "actual" } });
                _ = scratch.reset(.free_all);
                try std.testing.expect(row.typed_cells != null and row.value == .null);
                try std.testing.expectEqual(@as(i64, 9007199254740993), (try row.cell("i")).value.integer);
                try std.testing.expectEqualStrings("owned", (try row.cell("a")).array.?.flatCell(0).value.string);
                try std.testing.expectEqual(@as(u16, 4), (try row.cell("n")).numeric.?.scale);
                try std.testing.expectEqualStrings("kept", (try row.cell("j")).value.array.items[1].object.get("s").?.string);
                try std.testing.expect(!(try row.cell("z")).sql_null);
                try std.testing.expect(!try row.hasField("missing"));
                try std.testing.expectEqual(@as(f64, 0.5), (try row.cell("score")).value.float);
                try std.testing.expectEqualStrings("actual", (try row.cell("_highlights")).value.string);
                if (large) std.debug.print("document search {s}: output peak {d} bytes for {d}-byte hit\n", .{ switch (raw) {
                    0 => "parsed",
                    1 => "raw",
                    else => "ranges",
                }, budget.peak, bytes.len });
                for ([_][]const u8{ "{\"ignored\":{\"x\":0,\"\\u0078\":1}}", "{\"ignored\":[1,]}", "{} trailing", "[]" }) |invalid| {
                    if (projection.projectSearch(oa, scratch.allocator(), layout, "row", .{ .bytes = invalid }, .{})) |_| return error.ExpectedInvalidDocument else |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => {},
                    }
                }
            }
        }
    };
    try Probe.run(std.testing.allocator, true);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(stable.allocator(), Probe.run, .{false});
}

test "SQL projected document directory validates ranges identities corruption and allocation failures" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const module = @import("../storage/document_projection.zig");
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const temp = arena.allocator();
            const ignored = try temp.alloc(u8, module.minimum_bytes);
            @memset(ignored, 'x');
            const bytes = try std.fmt.allocPrint(temp, "{{\"ignored\":\"{s}\",\"\\u0062ody\":{{\"z\":[1,null]}},\"a\":null}}", .{ignored});
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            const proof = .{ .bytes = @as(u64, bytes.len), .kind = @as(u8, 16), .digest = digest };
            const encoded = (try module.build(temp, bytes, 16, digest)).?;
            const directory = (try module.Directory.open(temp, encoded, proof)).?;
            const field = directory.find("body").?;
            try std.testing.expectEqualStrings("{\"z\":[1,null]}", bytes[@intCast(field.offset)..][0..@intCast(field.length)]);
            try std.testing.expect(directory.find("missing") == null);
            for (0..encoded.len) |end| try std.testing.expect(try module.Directory.open(temp, encoded[0..end], proof) == null);
            const altered = try temp.dupe(u8, encoded);
            altered[12] ^= 1;
            try std.testing.expect(try module.Directory.open(temp, altered, proof) == null);
            try std.testing.expect(try module.build(temp, "{}", 16, digest) == null);
            const duplicate = try std.fmt.allocPrint(temp, "{{\"ignored\":\"{s}\",\"a\":1,\"\\u0061\":2}}", .{ignored});
            try std.testing.expect(try module.build(temp, duplicate, 16, digest) == null);
            const unsupported = try module.buildAuthority(temp, ignored, 16, digest);
            try std.testing.expect(unsupported != null);
            const unsupported_proof = .{ .bytes = @as(u64, ignored.len), .kind = @as(u8, 16), .digest = digest };
            try std.testing.expect(try module.isCurrent(temp, unsupported.?, unsupported_proof));
            try std.testing.expect(try module.Directory.open(temp, unsupported.?, unsupported_proof) == null);
            const trailing = try std.fmt.allocPrint(temp, "{s} trailing", .{bytes});
            try std.testing.expect(try module.build(temp, trailing, 16, digest) == null);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL document scanner scratch remains bounded across large sibling objects" {
    // This fixture measures retained scratch over hundreds of thousands of
    // escaped keys. Keep safety/leak checks without unwinding every arena
    // resize; ANTFLY_TEST_ALLOCATOR_TRACES restores allocation backtraces.
    var checked: @import("../storage/test_allocator.zig").TestAllocator = .{};
    defer checked.deinit();
    const a = checked.allocator();
    for ([_]usize{ 4, 32 }) |siblings| {
        var input: std.Io.Writer.Allocating = .init(a);
        defer input.deinit();
        try input.writer.writeAll("{\"body\":\"selected\",\"objects\":[");
        for (0..siblings) |n| {
            if (n != 0) try input.writer.writeByte(',');
            try input.writer.writeByte('{');
            for (0..8192) |i| {
                if (i != 0) try input.writer.writeByte(',');
                try input.writer.print("\"k\\u0065y{d}\":0", .{i});
            }
            try input.writer.writeByte('}');
        }
        try input.writer.writeAll("]}");
        var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 1024 * 1024 };
        const directory = (try @import("../storage/document_projection.zig").buildAuthority(budget.allocator(), input.written(), @import("../storage/internal_keys.zig").primary_kind, @splat(0))).?;
        budget.allocator().free(directory);
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        const directory_peak = budget.peak;
        budget.peak = 0;
        var scratch = std.heap.ArenaAllocator.init(budget.allocator());
        defer scratch.deinit();
        var output = std.heap.ArenaAllocator.init(a);
        defer output.deinit();
        const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{.{ .name = "body", .path = "body", .type = .string }} };
        const projection = try Projection.initSearch(a, table, &.{"body"});
        defer projection.deinit(a);
        const layout = try catalog.Row.TypedLayout.init(output.allocator(), &.{"body"});
        const row = try projection.projectSearch(output.allocator(), scratch.allocator(), layout, "doc", .{ .bytes = input.written() }, .{});
        try std.testing.expectEqualStrings("selected", (try row.cell("body")).value.string);
        std.debug.print("BOUNDED_DIRECTORY siblings={d} source={d} directory_peak={d} raw_peak={d}\n", .{ siblings, input.written().len, directory_peak, budget.peak });
    }
}

test "SQL projected range planner coalesces bounded spans and restores order" {
    const ranges = @import("../storage/value_ranges.zig");
    const Source = struct {
        calls: usize = 0,
        bytes: [ranges.maximum_span * 2]u8 = @splat('x'),
        pub fn read(self: *@This(), offset: usize, len: usize) ![]const u8 {
            self.calls += 1;
            if (len > ranges.maximum_span) return error.UnboundedRead;
            return self.bytes[offset..][0..len];
        }
    };
    var source: Source = .{};
    const requested = [_]ranges.Range{ .{ .offset = 100, .length = 5 }, .{ .offset = 2, .length = 7 }, .{ .offset = 100, .length = 5 }, .{ .offset = ranges.maximum_span + 10000, .length = 6 } };
    var output: [requested.len][]const u8 = undefined;
    try ranges.read(source.bytes.len, &requested, &output, &source);
    try std.testing.expectEqual(@as(usize, 2), source.calls);
    for (requested, output) |range, value| try std.testing.expectEqualSlices(u8, source.bytes[@intCast(range.offset)..][0..range.length], value);
    try std.testing.expectEqual(output[0].ptr, output[2].ptr);
    try std.testing.expectError(error.EndOfStream, ranges.read(source.bytes.len, &.{.{ .offset = source.bytes.len, .length = 1 }}, output[0..1], &source));
}

test "SQL document encoded JSON projection preserves exact cells aliases depth and strict validation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 1, .columns = &.{ .{ .name = "j", .path = "payload", .type = .json }, .{ .name = "alias", .path = "payload", .type = .json }, .{ .name = "n", .path = "n", .type = .json } } };
    const projection = try Projection.initSearch(a, table, &.{ "j", "alias", "n" });
    const layout = try catalog.Row.TypedLayout.init(a, &.{ "j", "alias", "n" });
    var encoded: [3]?[]const u8 = undefined;
    const input = "{\"payload\": { \"x\": [9007199254740993,1e-3,\"\\u00e9\"], \"n\":null },\"n\":null}";
    const result = try projection.projectSearchEncoded(a, a, layout, "key", .{ .bytes = input }, .{}, &encoded);
    const legacy = try projection.projectSearch(a, a, layout, "key", .{ .bytes = input }, .{});
    try std.testing.expectEqual(encoded[0].?.ptr, encoded[1].?.ptr);
    try std.testing.expect(encoded[2] == null and !(try result.cell("n")).sql_null);
    const expected = try std.json.Stringify.valueAlloc(a, (try legacy.cell("j")).value, .{});
    try std.testing.expectEqualStrings(expected, encoded[0].?);
    try std.testing.expectError(error.DuplicateField, projection.projectSearchEncoded(a, a, layout, "key", .{ .bytes = "{\"payload\":{\"x\":1,\"x\":2}}" }, .{}, &encoded));
    const deep = try a.alloc(u8, 133);
    @memset(deep[0..66], '[');
    deep[66] = '0';
    @memset(deep[67..], ']');
    try std.testing.expectError(error.SqlProgramLimitExceeded, @import("json_bytes.zig").fromValidated(a, a, deep));
}
