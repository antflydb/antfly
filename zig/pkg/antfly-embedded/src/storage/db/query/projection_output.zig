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

const std = @import("std");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;
const EmbeddingFields = @import("embedding_fields.zig").EmbeddingFields;
const View = @import("json_view.zig").View;
const native_array = @import("native_array.zig");
const plan_mod = @import("json_projection_plan.zig");

/// Owns independently reclaimable cursors, including primed selections which
/// are replaced or never serialized. Retire before the enclosing row arena.
pub const Owner = struct {
    alloc: Allocator,
    work: Allocator,
    cursors: ?*FanoutRows = null,
    resources: ?*Resources = null,
    owns_resources: bool = false,

    pub fn init(alloc: Allocator) Owner {
        return .{ .alloc = alloc, .work = @import("antfly_platform").allocator.reclaimingScratch(alloc) };
    }
    fn borrowed(alloc: Allocator, resources: *Resources) Owner {
        return .{ .alloc = alloc, .work = resources.work, .resources = resources };
    }
    fn scratch(self: *Owner) !*Resources {
        if (self.resources) |resources| return resources;
        const resources = try self.alloc.create(Resources);
        resources.* = .init(self.work);
        self.resources = resources;
        self.owns_resources = true;
        return resources;
    }
    fn retireCursors(self: *Owner) void {
        while (self.cursors) |rows| {
            self.cursors = rows.next_owned;
            rows.deinit();
        }
    }
    pub fn deinit(self: *Owner) void {
        self.retireCursors();
        if (self.owns_resources) {
            self.resources.?.deinit();
            self.alloc.destroy(self.resources.?);
            self.resources = null;
            self.owns_resources = false;
        }
    }
};

/// All preparation calls are synchronous and finish before child fanouts are
/// primed. One bounded workspace can therefore serve the entire projection.
/// Cursor records never enter this pool: their active bit belongs to the row,
/// so an old record cannot return storage leased to a subsequent cursor.
const Resources = struct {
    work: Allocator,
    preparation: View.PreparationWorkspace,
    idle: ?*CursorScratch = null,
    idle_bytes: usize = 0,
    active: usize = 0,
    // The first lease shares stable row-owned control storage. Additional
    // simultaneous leases retain independently reclaimable heap metadata.
    primary: CursorScratch = undefined,
    primary_in_use: bool = false,
    const reuse_limit = 256 * 1024;

    fn init(work: Allocator) Resources {
        return .{ .work = work, .preparation = .init(work) };
    }
    fn acquire(self: *Resources) !*CursorScratch {
        const slot = if (self.idle) |slot| reused: {
            self.idle = slot.next;
            self.idle_bytes -= slot.retainedBytes();
            break :reused slot;
        } else fresh: {
            const slot = if (!self.primary_in_use) primary: {
                self.primary_in_use = true;
                break :primary &self.primary;
            } else try self.work.create(CursorScratch);
            slot.* = .{ .inline_control = slot == &self.primary, .accounting = .{ .backing = self.work }, .arena = undefined, .scan_arena = undefined, .owner = .borrowed(self.work, self) };
            slot.arena = .init(slot.accounting.allocator());
            slot.scan_arena = .init(slot.accounting.allocator());
            break :fresh slot;
        };
        errdefer self.destroy(slot);
        try resetScratch(&slot.arena);
        try resetScratch(&slot.scan_arena);
        slot.owner = .borrowed(slot.arena.allocator(), self);
        slot.next = null;
        self.active += 1;
        return slot;
    }
    fn release(self: *Resources, slot: *CursorScratch) void {
        slot.owner.retireCursors();
        // Cleanup must not allocate to consolidate or shrink an arena.
        if (slot.accounting.bytes > 16 * 1024) {
            _ = slot.arena.reset(.free_all);
            _ = slot.scan_arena.reset(.free_all);
        }
        std.debug.assert(self.active != 0);
        self.active -= 1;
        const bytes = slot.retainedBytes();
        if (bytes > reuse_limit - self.idle_bytes) return self.destroy(slot);
        slot.next = self.idle;
        self.idle = slot;
        self.idle_bytes += bytes;
    }
    fn destroy(self: *Resources, slot: *CursorScratch) void {
        std.debug.assert(slot.owner.cursors == null);
        slot.arena.deinit();
        slot.scan_arena.deinit();
        std.debug.assert(slot.accounting.bytes == 0);
        if (slot == &self.primary) self.primary_in_use = false else self.work.destroy(slot);
    }
    fn deinit(self: *Resources) void {
        std.debug.assert(self.active == 0);
        while (self.idle) |slot| {
            self.idle = slot.next;
            self.destroy(slot);
        }
        self.idle_bytes = 0;
        self.preparation.deinit();
        std.debug.assert(!self.primary_in_use);
    }
};

const CursorScratch = struct {
    next: ?*CursorScratch = null,
    inline_control: bool = false,
    accounting: ArenaAccounting,
    arena: std.heap.ArenaAllocator,
    // Scanner state spans rows, so it cannot share the row-reset arena.
    scan_arena: std.heap.ArenaAllocator,
    owner: Owner,
    view: View = undefined,

    fn retainedBytes(self: *const CursorScratch) usize {
        return (if (self.inline_control) @as(usize, 0) else @sizeOf(CursorScratch)) + self.accounting.bytes;
    }
};

/// Account complete backing blocks, including arena headers. This adapter is
/// embedded in a stable heap slot and never bypasses the caller's quota.
const ArenaAccounting = struct {
    backing: Allocator,
    bytes: usize = 0,
    fn allocator(self: *ArenaAccounting) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *ArenaAccounting = @ptrCast(@alignCast(ctx));
        const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
        self.bytes += len;
        return result;
    }
    fn resize(ctx: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *ArenaAccounting = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(buffer, alignment, len, ra)) return false;
        self.bytes = self.bytes - buffer.len + len;
        return true;
    }
    fn remap(ctx: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *ArenaAccounting = @ptrCast(@alignCast(ctx));
        const result = self.backing.rawRemap(buffer, alignment, len, ra) orelse return null;
        self.bytes = self.bytes - buffer.len + len;
        return result;
    }
    fn free(ctx: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *ArenaAccounting = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(buffer, alignment, ra);
        self.bytes -= buffer.len;
    }
};

/// Borrows an authenticated cell and immutable schema during synchronous row
/// projection. Only structural access to composite SQL values materializes a
/// logical container; scalar strings and dense vectors never need a DOM.
pub const NativeCell = struct {
    cell: @import("../algebraic/relational_row_codec.zig").Cell,
    column: *const @import("../../schema.zig").RelationalColumn,
    alloc: Allocator,
    cached: ?*const Value = null,
    prepared_array: ?*native_array.Prepared = null,
    materialized_count: ?*u64 = null,
    fn kind(self: *const @This()) u8 {
        if (self.cell.is_null) return 0;
        return switch (self.column.column_type) {
            .dense_vector => '[',
            .geopoint, .sql_array => '{',
            else => 0,
        };
    }
    fn array(self: *@This()) !*native_array.Prepared {
        if (self.prepared_array) |prepared| return prepared;
        const prepared = try self.alloc.create(native_array.Prepared);
        errdefer self.alloc.destroy(prepared);
        prepared.* = try native_array.Prepared.init(self.alloc, self.column.sql_element_type orelse return error.InvalidRelationalRow, self.cell.value.bytes_val, true);
        self.prepared_array = prepared;
        return prepared;
    }
    pub fn write(self: *@This(), scratch: Allocator, writer: *std.Io.Writer) !void {
        return self.writeMode(scratch, writer, false);
    }
    pub fn writeMode(self: *@This(), scratch: Allocator, writer: *std.Io.Writer, measuring: bool) !void {
        if (self.column.column_type == .sql_array and !self.cell.is_null) return self.writeArray(scratch, writer, .whole, measuring);
        if (self.cached) |value| return std.json.Stringify.value(value.*, .{}, writer);
        try @import("../algebraic/relational_row_codec.zig").writeCellValue(scratch, writer, self.cell, .logical);
    }

    fn writeArray(self: *@This(), scratch: Allocator, writer: *std.Io.Writer, part: native_array.Part, measuring: bool) !void {
        const prepared = try self.array();
        const size = try prepared.size(scratch, part);
        if (measuring) try writer.splatByteAll(' ', size) else try prepared.write(scratch, writer, part);
    }
    fn logical(self: *@This()) !*const Value {
        if (self.cached == null) {
            const value = try self.alloc.create(Value);
            errdefer self.alloc.destroy(value);
            value.* = try @import("../algebraic/relational_row_codec.zig").borrowedJsonValueFromCellAlloc(self.alloc, self.column.*, self.cell);
            self.cached = value;
            if (self.materialized_count) |count| count.* += 1;
        }
        return self.cached.?;
    }
};

/// Row-owned projection structure. Terminal values borrow immutable sources;
/// containers become mutable only when an ordered selector needs to change one.
/// Neither the view nor a loaded source is ever mutated by projection.
pub const Source = union(enum) {
    sql_array: struct { cell: *NativeCell, part: native_array.Part },
    native: *NativeCell,
    span: struct { view: *View, node: *View.Node },
    logical: *const Value,
    embeddings: *const EmbeddingFields,
    embedding_stream: *@import("embedding_fields.zig").EmbeddingStream,
    vector: []const f32,
    component: f32,

    fn kind(self: Source) u8 {
        return switch (self) {
            .sql_array => '[',
            .native => |cell| cell.kind(),
            .embeddings, .embedding_stream => '{',
            .vector => '[',
            .component => 0,
            .span => |s| s.node.kind(),
            .logical => |v| switch (v.*) {
                .object => '{',
                .array => '[',
                else => 0,
            },
        };
    }
    fn field(self: Source, alloc: Allocator, name: []const u8) !?Source {
        return switch (self) {
            .sql_array => null,
            .native => |cell| blk: {
                if (cell.kind() != '{') break :blk null;
                if (cell.column.column_type == .sql_array) {
                    const part: native_array.Part = if (std.mem.eql(u8, name, "dimensions")) .dimensions else if (std.mem.eql(u8, name, "values")) .values else if (std.mem.eql(u8, name, "sql_nulls")) .sql_nulls else break :blk null;
                    break :blk .{ .sql_array = .{ .cell = cell, .part = part } };
                }
                break :blk if ((try cell.logical()).object.getPtr(name)) |value| .{ .logical = value } else null;
            },
            .embedding_stream => |stream| if (try stream.getField(alloc, name)) |v| .{ .vector = v } else null,
            .embeddings => |fields| if (fields.values.get(name)) |v| .{ .vector = v } else null,
            .vector, .component => null,
            .span => |s| if (try s.view.fieldSparse(s.node, name)) |node| .{ .span = .{ .view = s.view, .node = node } } else null,
            .logical => |v| if (v.* == .object) (if (v.object.getPtr(name)) |value| .{ .logical = value } else null) else null,
        };
    }
    fn element(self: Source, alloc: Allocator, index: usize) !?Source {
        return switch (self) {
            .sql_array => |source| blk: {
                const array = try source.cell.array();
                if (index >= array.length(source.part)) break :blk null;
                if (source.part == .values and array.view.kind == .jsonb) {
                    const cell = try array.view.cell(index);
                    if (!cell.sql_null) {
                        const view = try alloc.create(View);
                        view.* = .init(alloc, cell.bytes);
                        view.canonical_source = cell.bytes;
                        break :blk .{ .span = .{ .view = view, .node = &view.root } };
                    }
                }
                const value = try alloc.create(Value);
                value.* = switch (source.part) {
                    .whole => unreachable,
                    .values => try array.value(alloc, index),
                    .sql_nulls => .{ .bool = (try array.view.cell(index)).sql_null },
                    .dimensions => dimension: {
                        const d = try array.view.dimension(index);
                        var object = std.json.ObjectMap.empty;
                        try object.put(alloc, "length", .{ .integer = d.length });
                        try object.put(alloc, "lower_bound", .{ .integer = d.lower });
                        break :dimension .{ .object = object };
                    },
                };
                break :blk .{ .logical = value };
            },
            .native => |cell| blk: {
                if (cell.kind() != '[' or index >= cell.cell.value.bytes_val.len / 4) break :blk null;
                const bytes = cell.cell.value.bytes_val[index * 4 ..][0..4];
                break :blk .{ .component = @bitCast(std.mem.readInt(u32, bytes, .little)) };
            },
            .vector => |v| if (index < v.len) .{ .component = v[index] } else null,
            .embeddings, .embedding_stream, .component => null,
            .span => |s| if (try s.view.element(s.node, index)) |node| .{ .span = .{ .view = s.view, .node = node } } else null,
            .logical => |v| if (v.* == .array and index < v.array.items.len) .{ .logical = &v.array.items[index] } else null,
        };
    }
};

pub const Output = union(enum) {
    borrowed: Source,
    object: Object,
    overlay: *Overlay,
    array: std.ArrayListUnmanaged(Output),
    fanout: *Fanout,
    const Fanout = struct {
        source: Source,
        path: []const []const u8,
        selector: ?*const @import("json_projection_plan.zig").Selector = null,
        prepared: ?*FanoutRows = null,
        first: ?Object = null,
    };
    pub const Object = std.StringArrayHashMapUnmanaged(Output);
    // Keep this behind a pointer: ordinary fanout entries must not pay for a
    // base source plus an override map when no copy-on-write is needed.
    const Overlay = struct { base: Source, changed: Object = .empty };
    const Target = struct { map: *Object, base: ?Source = null };

    fn objectTarget(self: *Output, a: Allocator) !Target {
        switch (self.*) {
            .object => |*object| return .{ .map = object },
            .overlay => |overlay| return .{ .map = &overlay.changed, .base = overlay.base },
            .borrowed => |source| if (source.kind() == '{') {
                const overlay = try a.create(Overlay);
                overlay.* = .{ .base = source };
                self.* = .{ .overlay = overlay };
                return .{ .map = &overlay.changed, .base = source };
            },
            else => {},
        }
        self.retire();
        self.* = .{ .object = .empty };
        return .{ .map = &self.object };
    }

    /// Restore proven object ancestors without copying their retained leaves.
    /// A borrowed object acquires only a small overlay of missing children.
    pub fn ensureObjectPath(self: *Output, a: Allocator, parts: []const []const u8) !void {
        var current = self;
        for (parts) |name| {
            try current.requireObject();
            const target = try current.objectTarget(a);
            if (!target.map.contains(name)) {
                const inherited = if (target.base) |base| try base.field(a, name) else null;
                try put(a, target.map, name, if (inherited) |value| .{ .borrowed = value } else .{ .object = .empty });
            }
            current = target.map.getPtr(name).?;
        }
        try current.requireObject();
    }
    fn requireObject(self: Output) !void {
        switch (self) {
            .object, .overlay => {},
            .borrowed => |source| if (source.kind() != '{') return error.InvalidProjectedDocumentJson,
            else => return error.InvalidProjectedDocumentJson,
        }
    }

    fn put(a: Allocator, object: *Object, name: []const u8, value: Output) !void {
        if (object.getPtr(name)) |previous| {
            previous.retire();
            previous.* = value;
            return;
        }
        if (object.entries.capacity == 0) try object.entries.setCapacity(a, 1);
        try object.put(a, name, value);
    }

    /// Builders uniquely own their mutable output containers. A winning
    /// replacement retires the displaced cursor tree immediately; row-owner
    /// cleanup later sees closed records, even if their buffers were reused.
    fn retire(self: Output) void {
        switch (self) {
            .fanout => |fanout| {
                if (fanout.prepared) |rows| rows.deinit();
                fanout.prepared = null;
                fanout.first = null;
            },
            .object => |object| for (object.values()) |child| child.retire(),
            .overlay => |overlay| for (overlay.changed.values()) |child| child.retire(),
            .array => |array| for (array.items) |child| child.retire(),
            .borrowed => {},
        }
    }

    fn copyObject(a: Allocator, dst: *Object, source: Source) !void {
        switch (source) {
            .native => |cell| {
                if (cell.column.column_type == .sql_array) {
                    for ([_][]const u8{ "dimensions", "values", "sql_nulls" }) |name| try put(a, dst, name, .{ .borrowed = (try source.field(a, name)).? });
                    return;
                }
                const value = try cell.logical();
                for (value.object.keys(), value.object.values()) |name, *child| try put(a, dst, name, .{ .borrowed = .{ .logical = child } });
            },
            .embedding_stream => |stream| try copyObject(a, dst, .{ .embeddings = try stream.fields(a) }),
            .embeddings => |fields| for (fields.values.keys(), fields.values.values()) |name, vector| try put(a, dst, name, .{ .borrowed = .{ .vector = vector } }),
            .sql_array, .vector, .component => unreachable,
            .logical => |v| for (v.object.keys(), v.object.values()) |name, *value| {
                try put(a, dst, name, .{ .borrowed = .{ .logical = value } });
            },
            .span => |s| {
                // A native view may belong to a retained column page. Copy-on-
                // write navigation belongs to this output row, not that page.
                const shallow = try a.create(View);
                shallow.* = .init(a, s.node.raw);
                shallow.parse_numbers = s.view.parse_numbers;
                shallow.canonical_source = s.view.canonical_source;
                const children = try shallow.children(&shallow.root);
                const names = try a.alloc([]const u8, children.len);
                var it = shallow.root.object_index.iterator();
                while (it.next()) |entry| if (entry.value_ptr.* < names.len) {
                    names[entry.value_ptr.*] = entry.key_ptr.*;
                };
                for (children, names) |*child, name| try put(a, dst, name, .{ .borrowed = .{ .span = .{ .view = shallow, .node = &child.node } } });
            },
        }
    }

    /// Apply includes in caller order. Object selections merge; every array
    /// selection produces a fresh compact array, preserving legacy replacement.
    pub fn include(owner: *Owner, dst: *Object, name: []const u8, source: Source, path: []const []const u8) anyerror!void {
        const selected = if (source == .span and path.len > 1) try plan_mod.compile(owner.alloc, &.{.{ .parts = path }}) else null;
        return includePlanned(owner, dst, name, source, path, selected);
    }
    pub fn includePlanned(owner: *Owner, dst: *Object, name: []const u8, source: Source, path: []const []const u8, selected: ?*const plan_mod.Selector) anyerror!void {
        return includeWithBase(owner, .{ .map = dst }, name, source, path, selected);
    }
    /// A canonical prefix proof records complete terminal spans and establishes
    /// object ancestry. Apply it only to independent projected objects. Borrowed
    /// bases/overlays use normal navigation so ordered whole-parent selections
    /// retain every original sibling and their existing replacement semantics.
    fn includeCanonicalPath(owner: *Owner, target: Target, name: []const u8, source: Source, path: []const []const u8) !bool {
        if (target.base != null or source != .span or path.len == 0 or source.span.node != &source.span.view.root) return false;
        const raw = source.span.view.canonicalPath(path) orelse return false;
        var check = target.map;
        var key = name;
        for (0..path.len) |i| {
            const existing = check.getPtr(key) orelse break;
            if (existing.* != .object) return false;
            check = &existing.object;
            key = path[i];
        }
        const a = owner.alloc;
        var dst = target.map;
        key = name;
        for (path) |part| {
            if (!dst.contains(key)) try put(a, dst, key, .{ .object = .empty });
            dst = &dst.getPtr(key).?.object;
            key = part;
        }
        const node = try a.create(View.Node);
        node.* = .{ .raw = raw };
        try put(a, dst, key, .{ .borrowed = .{ .span = .{ .view = source.span.view, .node = node } } });
        return true;
    }
    fn includeWithBase(owner: *Owner, target: Target, name: []const u8, source: Source, path: []const []const u8, selected: ?*const plan_mod.Selector) anyerror!void {
        if (try includeCanonicalPath(owner, target, name, source, path)) return;
        const a = owner.alloc;
        const dst = target.map;
        if (path.len == 0 or (source == .embedding_stream and path.len == 1 and std.mem.eql(u8, path[0], "*"))) return put(a, dst, name, .{ .borrowed = source });
        switch (source.kind()) {
            '{' => {
                if (!dst.contains(name)) {
                    const previous = if (target.base) |base| try base.field(a, name) else null;
                    try put(a, dst, name, if (previous) |child| .{ .borrowed = child } else .{ .object = .empty });
                }
                const nested = try dst.getPtr(name).?.objectTarget(a);
                if (std.mem.eql(u8, path[0], "*")) {
                    var all: Object = .empty;
                    try copyObject(a, &all, source);
                    for (all.keys(), all.values()) |key, child| try includeWithBase(owner, nested, key, child.borrowed, path[1..], childSelector(selected, path[0]));
                } else if (try source.field(a, path[0])) |child| try includeWithBase(owner, nested, path[0], child, path[1..], childSelector(selected, path[0]));
            },
            '[' => {
                var result: std.ArrayListUnmanaged(Output) = .empty;
                if (View.projectionIndex(path[0])) |index| {
                    const item = (try source.element(a, index)) orelse return;
                    if (path.len == 1) {
                        try result.append(a, .{ .borrowed = item });
                    } else if (item.kind() == '{') {
                        var object: Object = .empty;
                        try includeObjectPath(owner, &object, item, path[1..], childSelector(selected, path[0]));
                        if (object.count() != 0) try result.append(a, .{ .object = object });
                    }
                } else {
                    // Cursor records stay stable in the row. Their navigation
                    // arenas use independent backing, never a parent arena.
                    // Register before priming so errors and abandoned outputs
                    // retire the complete cursor tree through its owner.
                    const fanout = try a.create(Fanout);
                    fanout.* = .{ .source = source, .path = path, .selector = if (path.len > 1) selected else null };
                    const rows = try a.create(FanoutRows);
                    rows.* = FanoutRows.init(try owner.scratch(), fanout.*);
                    rows.next_owned = owner.cursors;
                    owner.cursors = rows;
                    if (try rows.next()) |first| {
                        fanout.prepared = rows;
                        fanout.first = first;
                        try put(a, dst, name, .{ .fanout = fanout });
                    } else rows.deinit();
                    return;
                }
                if (result.items.len != 0) try put(a, dst, name, .{ .array = result });
            },
            else => {},
        }
    }
    fn childSelector(selected: ?*const plan_mod.Selector, name: []const u8) ?*const plan_mod.Selector {
        return if (selected) |s| s.fields.get(name) else null;
    }
    fn includeObjectPath(owner: *Owner, dst: *Object, source: Source, path: []const []const u8, selected: ?*const plan_mod.Selector) !void {
        const a = owner.alloc;
        if (std.mem.eql(u8, path[0], "*")) {
            var all: Object = .empty;
            try copyObject(a, &all, source);
            for (all.keys(), all.values()) |name, value| try includePlanned(owner, dst, name, value.borrowed, path[1..], childSelector(selected, path[0]));
        } else if (try source.field(a, path[0])) |child| try includePlanned(owner, dst, path[0], child, path[1..], childSelector(selected, path[0]));
    }

    pub fn includeRoot(owner: *Owner, dst: *Object, view: *View, path: plan_mod.Path) !void {
        if (path.parts.len == 0) return;
        const selected = childSelector(path.selector, path.parts[0]);
        for (view.root_sources) |*source| if (std.mem.eql(u8, source.name, path.parts[0])) {
            return includePlanned(owner, dst, source.name, source.outputSource(), path.parts[1..], selected);
        };
        if (try view.field(&view.root, path.parts[0])) |node| try includePlanned(owner, dst, path.parts[0], .{ .span = .{ .view = view, .node = node } }, path.parts[1..], selected);
    }

    pub fn includePreparedRootExcept(owner: *Owner, dst: *Object, view: *View, excluded: []const []const u8) !void {
        const a = owner.alloc;
        if (omitted(excluded, "*")) return;
        const children = view.root.children orelse &.{};
        const names = try a.alloc([]const u8, children.len);
        var it = view.root.object_index.iterator();
        while (it.next()) |entry| if (entry.value_ptr.* < names.len) {
            names[entry.value_ptr.*] = entry.key_ptr.*;
        };
        for (names) |name| if (!omitted(excluded, name)) try includeRoot(owner, dst, view, .{ .parts = &.{name} });
        for (view.root_sources) |*source| {
            if (omitted(excluded, source.name) or dst.contains(source.name)) continue;
            try put(a, dst, source.name, .{ .borrowed = source.outputSource() });
        }
    }
    fn omitted(excluded: []const []const u8, name: []const u8) bool {
        for (excluded) |entry| if (std.mem.eql(u8, name, entry)) return true;
        return false;
    }

    pub fn serialize(self: Output, scratch: Allocator, output: Allocator) ![]u8 {
        return self.serializeWithPolicy(scratch, output, false);
    }
    /// Share bounded exact sizing with whole-row and typed-cell serialization.
    pub fn serializeWithPolicy(self: Output, scratch: Allocator, output: Allocator, exact: bool) ![]u8 {
        var context = StreamContext.init(scratch);
        defer context.deinit();
        const Emission = struct {
            value: Output,
            context: *StreamContext,
            pub fn exactSize(emission: @This()) !?usize {
                if (!emission.value.hasNativeArray()) return null;
                var counter = std.Io.Writer.Discarding.init(&.{});
                emission.context.measuring = true;
                defer emission.context.measuring = false;
                try emission.value.write(emission.context, &counter.writer);
                return std.math.cast(usize, counter.fullCount()) orelse error.OutOfMemory;
            }
            pub fn write(emission: @This(), writer: *std.Io.Writer) anyerror!void {
                try emission.value.write(emission.context, writer);
            }
        };
        return @import("serialized_output.zig").alloc(output, exact, Emission{ .value = self, .context = &context });
    }

    fn hasNativeArray(self: Output) bool {
        return switch (self) {
            .borrowed => |source| source == .sql_array or (source == .native and source.native.column.column_type == .sql_array),
            .object => |object| blk: {
                for (object.values()) |child| if (child.hasNativeArray()) break :blk true;
                break :blk false;
            },
            .array => |array| blk: {
                for (array.items) |child| if (child.hasNativeArray()) break :blk true;
                break :blk false;
            },
            .overlay => |overlay| (Output{ .borrowed = overlay.base }).hasNativeArray() or (Output{ .object = overlay.changed }).hasNativeArray(),
            .fanout => |fanout| fanout.source == .sql_array,
        };
    }

    /// A transport with its own writer can stream the body without allocating
    /// an intermediate response. The source remains borrowed until completion.
    pub fn writeTo(self: Output, scratch: Allocator, writer: *std.Io.Writer) !void {
        var context = StreamContext.init(scratch);
        defer context.deinit();
        try self.write(&context, writer);
    }
    /// Emit flat scan members while the source owner is pinned. The transport
    /// owns the surrounding object and authoritative _id. Every member begins
    /// with a comma; no intermediate JSON document is allocated.
    pub fn writeFieldsTo(self: Output, scratch: Allocator, writer: *std.Io.Writer) !void {
        if (self != .object) return error.InvalidProjectedDocumentJson;
        var context = StreamContext.init(scratch);
        defer context.deinit();
        var count: usize = 1;
        for (self.object.keys(), self.object.values()) |name, child| {
            if (!std.mem.eql(u8, name, "_id")) try writeMember(&context, writer, &count, name, child);
        }
    }
    fn write(self: Output, context: *StreamContext, writer: *std.Io.Writer) anyerror!void {
        switch (self) {
            .borrowed => |source| switch (source) {
                .sql_array => |source_array| try source_array.cell.writeArray(context.work, writer, source_array.part, context.measuring),
                .native => |cell| try cell.writeMode(context.work, writer, context.measuring),
                .embedding_stream => |stream| try stream.write(writer),
                .span => |s| if (s.view.canCopyCanonical(s.node.raw)) try writer.writeAll(s.node.raw) else try context.writeSpanValidated(writer, s.node.raw, s.view.parse_numbers, s.view.validated_ranges),
                .logical => |v| try std.json.Stringify.value(v.*, .{}, writer),
                .component => |v| try std.json.Stringify.value(@as(f64, v), .{}, writer),
                .vector => |v| try writeVector(writer, v),
                .embeddings => |fields| {
                    try writer.writeByte('{');
                    for (fields.values.keys(), fields.values.values(), 0..) |name, vector, i| {
                        if (i != 0) try writer.writeByte(',');
                        try std.json.Stringify.value(name, .{}, writer);
                        try writer.writeByte(':');
                        try writeVector(writer, vector);
                    }
                    try writer.writeByte('}');
                },
            },
            .overlay => |overlay| try writeOverlay(context, writer, overlay),
            .object => |object| {
                try writer.writeByte('{');
                for (object.keys(), object.values(), 0..) |name, child, i| {
                    if (i != 0) try writer.writeByte(',');
                    try std.json.Stringify.value(name, .{}, writer);
                    try writer.writeByte(':');
                    try child.write(context, writer);
                }
                try writer.writeByte('}');
            },
            .fanout => |fanout| {
                var fallback = FanoutRows.init(&context.resources, fanout.*);
                const rows = fanout.prepared orelse &fallback;
                // Detach before writing: errors and repeated writes must never
                // reuse a partially consumed cursor or its retired first row.
                const first = fanout.first;
                fanout.prepared = null;
                fanout.first = null;
                defer rows.deinit();
                try writer.writeByte('[');
                var count: usize = 0;
                if (first) |object| {
                    try (Output{ .object = object }).write(context, writer);
                    count = 1;
                    // next() retires the first row and its nested cursors.
                }
                while (try rows.next()) |object| {
                    if (count != 0) try writer.writeByte(',');
                    try (Output{ .object = object }).write(context, writer);
                    count += 1;
                }
                try writer.writeByte(']');
            },
            .array => |array| {
                try writer.writeByte('[');
                for (array.items, 0..) |child, i| {
                    if (i != 0) try writer.writeByte(',');
                    try child.write(context, writer);
                }
                try writer.writeByte(']');
            },
        }
    }

    fn writeVector(writer: *std.Io.Writer, vector: []const f32) !void {
        try writer.writeByte('[');
        for (vector, 0..) |component, i| {
            if (i != 0) try writer.writeByte(',');
            try std.json.Stringify.value(@as(f64, component), .{}, writer);
        }
        try writer.writeByte(']');
    }
    fn writeMember(context: *StreamContext, writer: *std.Io.Writer, count: *usize, name: []const u8, value: Output) !void {
        if (count.* != 0) try writer.writeByte(',');
        try std.json.Stringify.value(name, .{}, writer);
        try writer.writeByte(':');
        try value.write(context, writer);
        count.* += 1;
    }
    fn writeBaseMember(context: *StreamContext, writer: *std.Io.Writer, overlay: *const Overlay, emitted: []bool, count: *usize, name: []const u8, source: Source) !void {
        if (overlay.changed.getIndex(name)) |index| {
            try writeMember(context, writer, count, name, overlay.changed.values()[index]);
            emitted[index] = true;
        } else try writeMember(context, writer, count, name, .{ .borrowed = source });
    }
    fn writeOverlay(context: *StreamContext, writer: *std.Io.Writer, overlay: *const Overlay) anyerror!void {
        const work = context.work;
        const emitted = try work.alloc(bool, overlay.changed.count());
        defer work.free(emitted);
        @memset(emitted, false);
        var count: usize = 0;
        try writer.writeByte('{');
        switch (overlay.base) {
            .native => |cell| {
                if (cell.column.column_type == .sql_array) {
                    for ([_][]const u8{ "dimensions", "values", "sql_nulls" }) |name| try writeBaseMember(context, writer, overlay, emitted, &count, name, (try overlay.base.field(work, name)).?);
                } else {
                    const value = try cell.logical();
                    for (value.object.keys(), value.object.values()) |name, *child| try writeBaseMember(context, writer, overlay, emitted, &count, name, .{ .logical = child });
                }
            },
            .sql_array, .vector, .component => unreachable,
            .embedding_stream => |stream| {
                if (stream.cached_complete) {
                    const fields = &stream.cached.?;
                    for (fields.values.keys(), fields.values.values()) |name, vector| try writeBaseMember(context, writer, overlay, emitted, &count, name, .{ .vector = vector });
                } else {
                    try stream.reset_fn(stream.ctx);
                    while (try stream.next_fn(stream.ctx)) |field| try writeBaseMember(context, writer, overlay, emitted, &count, field.name, .{ .vector = field.vector });
                }
            },
            .embeddings => |fields| for (fields.values.keys(), fields.values.values()) |name, vector| try writeBaseMember(context, writer, overlay, emitted, &count, name, .{ .vector = vector }),
            .logical => |value| for (value.object.keys(), value.object.values()) |name, *child| {
                try writeBaseMember(context, writer, overlay, emitted, &count, name, .{ .logical = child });
            },
            .span => |span| {
                var scanner = std.json.Scanner.initCompleteInput(work, span.node.raw);
                defer scanner.deinit();
                var keys = std.heap.ArenaAllocator.init(work);
                defer keys.deinit();
                var names: std.StringHashMapUnmanaged(void) = .empty;
                var token_arena = std.heap.ArenaAllocator.init(work);
                defer token_arena.deinit();
                if (try scanner.next() != .object_begin) return error.SyntaxError;
                while (try scanner.peekNextTokenType() != .object_end) {
                    try resetScratch(&token_arena);
                    const token = try scanner.nextAllocMax(token_arena.allocator(), .alloc_if_needed, span.node.raw.len);
                    const name = switch (token) {
                        .string, .allocated_string => |name| name,
                        else => return error.SyntaxError,
                    };
                    const owned_name = if (token == .allocated_string) try keys.allocator().dupe(u8, name) else name;
                    if ((try names.getOrPut(keys.allocator(), owned_name)).found_existing) return error.DuplicateField;
                    switch (try scanner.peekNextTokenType()) {
                        .object_end, .array_end, .end_of_document => return error.SyntaxError,
                        else => {},
                    }
                    if (overlay.changed.getIndex(name)) |index| {
                        // Discarded bytes still need syntax validation. An
                        // unchanged value is consumed and emitted in one pass.
                        try scanner.skipValue();
                        try writeMember(context, writer, &count, name, overlay.changed.values()[index]);
                        emitted[index] = true;
                    } else {
                        if (count != 0) try writer.writeByte(',');
                        try std.json.Stringify.value(name, .{}, writer);
                        try writer.writeByte(':');
                        try context.writeValue(writer, &scanner, span.node.raw.len, span.view.parse_numbers, span.view.validated_ranges);
                        count += 1;
                    }
                }
                _ = try scanner.next();
                if (try scanner.next() != .end_of_document) return error.SyntaxError;
            },
        }
        for (overlay.changed.keys(), overlay.changed.values(), emitted) |name, child, seen| {
            if (!seen) try writeMember(context, writer, &count, name, child);
        }
        try writer.writeByte('}');
    }
};

/// Retire large buffers without allocating a replacement just to shrink one.
/// Small arenas consolidate once and then reset in place across siblings.
fn resetScratch(arena: *std.heap.ArenaAllocator) Allocator.Error!void {
    if (arena.queryCapacity() > 16 * 1024) {
        _ = arena.reset(.free_all);
    } else if (!arena.reset(.retain_capacity)) return error.OutOfMemory;
}

/// One bounded pool for all terminals in a response. No borrowed token or
/// duplicate-key entry survives its container; completed depths are reusable.
const StreamContext = struct {
    measuring: bool = false,
    work: Allocator,
    scanners: std.heap.ArenaAllocator,
    keys: @import("antfly-json").ObjectKeyWorkspace(FrameState),
    resources: Resources,
    const FrameState = struct { object: bool, key: bool = true, count: usize = 0 };
    fn init(scratch: Allocator) StreamContext {
        const work = @import("antfly_platform").allocator.reclaimingScratch(scratch);
        return .{ .work = work, .scanners = .init(work), .keys = .init(work), .resources = .init(work) };
    }
    fn deinit(self: *StreamContext) void {
        self.resources.deinit();
        self.keys.deinit();
        self.scanners.deinit();
    }
    fn writeSpan(self: *StreamContext, writer: *std.Io.Writer, raw: []const u8, parse_numbers: bool) !void {
        return self.writeSpanValidated(writer, raw, parse_numbers, null);
    }
    fn writeSpanValidated(self: *StreamContext, writer: *std.Io.Writer, raw: []const u8, parse_numbers: bool, proof: ?*const View.ValidatedRanges.Ref) !void {
        try resetScratch(&self.scanners);
        var scanner = std.json.Scanner.initCompleteInput(self.scanners.allocator(), raw);
        defer scanner.deinit();
        errdefer self.keys.deinit();
        try self.writeValue(writer, &scanner, raw.len, parse_numbers, proof);
        if (try scanner.next() != .end_of_document) return error.SyntaxError;
    }
    /// The scanner owns escape and surrogate validation. Coalesce its decoded
    /// chunks into fixed scratch, then use the standard encoder for canonical
    /// escaping. No scalar-sized buffer or private scanner state is needed.
    fn writeString(writer: *std.Io.Writer, scanner: *std.json.Scanner) !void {
        var buffer: [4096]u8 = undefined;
        var used: usize = 0;
        try writer.writeByte('"');
        while (true) {
            const token = try scanner.next();
            switch (token) {
                .string => |chunk| {
                    try stringChunk(writer, &buffer, &used, chunk);
                    try std.json.Stringify.encodeJsonStringChars(buffer[0..used], .{}, writer);
                    try writer.writeByte('"');
                    return;
                },
                .partial_string => |chunk| try stringChunk(writer, &buffer, &used, chunk),
                .partial_string_escaped_1 => |chunk| try stringChunk(writer, &buffer, &used, &chunk),
                .partial_string_escaped_2 => |chunk| try stringChunk(writer, &buffer, &used, &chunk),
                .partial_string_escaped_3 => |chunk| try stringChunk(writer, &buffer, &used, &chunk),
                .partial_string_escaped_4 => |chunk| try stringChunk(writer, &buffer, &used, &chunk),
                else => return error.SyntaxError,
            }
        }
    }
    fn stringChunk(writer: *std.Io.Writer, buffer: []u8, used: *usize, chunk: []const u8) !void {
        if (chunk.len >= buffer.len or chunk.len > buffer.len - used.*) {
            try std.json.Stringify.encodeJsonStringChars(buffer[0..used.*], .{}, writer);
            used.* = 0;
        }
        if (chunk.len >= buffer.len) return std.json.Stringify.encodeJsonStringChars(chunk, .{}, writer);
        @memcpy(buffer[used.*..][0..chunk.len], chunk);
        used.* += chunk.len;
    }
    fn writeValue(self: *StreamContext, writer: *std.Io.Writer, scanner: *std.json.Scanner, max_len: usize, parse_numbers: bool, proof: ?*const View.ValidatedRanges.Ref) !void {
        errdefer self.keys.deinit();
        var root_seen = false;
        // Reuse arenas across siblings, but keep each live ancestor's duplicate
        // keys until its object ends. Retention is capped at every reuse.
        var depth: usize = 0;
        while (!root_seen or depth != 0) {
            const expecting_key = depth != 0 and self.keys.stack.items[depth - 1].state.object and self.keys.stack.items[depth - 1].state.key;
            // Keep decoded scalar strings out of the token arena. Object names
            // still need owned bytes until their duplicate-key frame ends.
            const string_value = !expecting_key and try scanner.peekNextTokenType() == .string;
            const token = if (string_value)
                std.json.Token{ .string = "" }
            else if (expecting_key)
                try self.keys.nextKey(depth - 1, scanner, max_len)
            else
                try scanner.next();
            if (token == .end_of_document) {
                if (!root_seen or depth != 0) return error.SyntaxError;
                break;
            }
            if (token == .object_end or token == .array_end) {
                try writer.writeByte(if (token == .object_end) '}' else ']');
                if (depth == 0) return error.SyntaxError;
                depth -= 1;
                try self.keys.finishFrame(depth);
                continue;
            }
            if (depth == 0) {
                if (root_seen) return error.SyntaxError;
                root_seen = true;
            }
            if (depth != 0) {
                const frame = &self.keys.stack.items[depth - 1].state;
                if (frame.object and frame.key) {
                    const name = switch (token) {
                        .string, .allocated_string => |s| s,
                        else => return error.SyntaxError,
                    };
                    try self.keys.remember(depth - 1, name, false);
                    if (frame.count != 0) try writer.writeByte(',');
                    try std.json.Stringify.value(name, .{}, writer);
                    try writer.writeByte(':');
                    frame.count += 1;
                    frame.key = false;
                    continue;
                }
                if (frame.object) {
                    frame.key = true;
                } else {
                    if (frame.count != 0) try writer.writeByte(',');
                    frame.count += 1;
                }
            }
            switch (token) {
                .object_begin, .array_begin => {
                    try writer.writeByte(if (token == .object_begin) '{' else '[');
                    try self.keys.enter(depth, .{ .object = token == .object_begin });
                    depth += 1;
                },
                .string, .allocated_string => |s| if (string_value) {
                    const copied = if (proof) |validated| try validated.copyCanonicalString(scanner, writer) else false;
                    if (!copied) try writeString(writer, scanner);
                } else try std.json.Stringify.value(s, .{}, writer),
                .number, .allocated_number => |s| try std.json.Stringify.value(if (parse_numbers) Value.parseFromNumberSlice(s) else Value{ .number_string = s }, .{}, writer),
                .true => try writer.writeAll("true"),
                .false => try writer.writeAll("false"),
                .null => try writer.writeAll("null"),
                else => return error.SyntaxError,
            }
        }
        self.keys.finishValue();
    }
};

/// Each returned object borrows this iterator's row arena and view until next.
/// In particular, navigation never enters a retained native column-page view.
const FanoutRows = struct {
    next_owned: ?*FanoutRows = null,
    active: bool = true,
    source: Source,
    path: []const []const u8,
    selector: ?*const @import("json_projection_plan.zig").Selector,
    resources: *Resources,
    scratch: ?*CursorScratch = null,
    scanner: ?std.json.Scanner = null,
    started: bool = false,
    index: usize = 0,
    fn init(resources: *Resources, fanout: Output.Fanout) FanoutRows {
        return .{ .source = fanout.source, .path = fanout.path, .selector = fanout.selector, .resources = resources };
    }
    fn deinit(self: *FanoutRows) void {
        if (!self.active) return;
        self.active = false;
        if (self.scanner) |*scanner| scanner.deinit();
        if (self.scratch) |slot| self.resources.release(slot);
        self.scratch = null;
    }
    fn next(self: *FanoutRows) anyerror!?Output.Object {
        if (self.source == .sql_array) {
            const source = self.source.sql_array;
            if (source.part == .sql_nulls or (source.part == .values and (try source.cell.array()).view.kind != .jsonb)) return null;
        }
        if (self.source == .vector or (self.source == .native and self.source.native.kind() == '[')) return null;
        var retire_row = self.scratch != null;
        const slot = self.scratch orelse acquired: {
            const slot = try self.resources.acquire();
            self.scratch = slot;
            break :acquired slot;
        };
        if (self.scanner == null and self.source == .span) self.scanner = std.json.Scanner.initCompleteInput(slot.scan_arena.allocator(), self.source.span.node.raw);
        while (true) {
            // acquire() has already reset a fresh lease. Later rows retire
            // their child cursors before resetting the same backing buffers.
            if (retire_row) {
                slot.owner.retireCursors();
                try resetScratch(&slot.arena);
            }
            retire_row = true;
            const a = slot.arena.allocator();
            const item: Source = switch (self.source) {
                .sql_array => |source_array| blk: {
                    if (self.index >= (try source_array.cell.array()).length(source_array.part)) return null;
                    const item = (try self.source.element(a, self.index)).?;
                    self.index += 1;
                    break :blk item;
                },
                .native => unreachable,
                .vector => return null,
                .embeddings, .embedding_stream, .component => unreachable,
                .logical => |value| blk: {
                    if (self.index == value.array.items.len) return null;
                    const item = &value.array.items[self.index];
                    self.index += 1;
                    break :blk .{ .logical = item };
                },
                .span => |span| blk: {
                    const scanner = &self.scanner.?;
                    if (!self.started) {
                        if (try scanner.next() != .array_begin) return error.SyntaxError;
                        self.started = true;
                    }
                    const token = try scanner.peekNextTokenType();
                    if (token == .array_end) {
                        _ = try scanner.next();
                        if (try scanner.next() != .end_of_document) return error.SyntaxError;
                        return null;
                    }
                    if (token == .object_end or token == .end_of_document) return error.SyntaxError;
                    const start = scanner.cursor;
                    if (token == .object_begin and self.selector != null) {
                        // Bound navigation to this item. Preparing the complete
                        // selected path once avoids rescanning every ancestor's
                        // remaining subtree for a deep field selection.
                        slot.view = .init(a, span.node.raw[start..]);
                        slot.view.validated_ranges = span.view.validated_ranges;
                        try slot.view.prepareProjectionValueWithWorkspace(self.selector.?, .visited, .object_only, .{ .defer_fanout = true }, scanner, span.node.raw, &self.resources.preparation);
                    } else if (token == .object_begin and !std.mem.eql(u8, self.path[0], "*")) {
                        // Discover the selected root member while advancing
                        // the array scanner, avoiding a second scan per item.
                        _ = try scanner.next();
                        var names: std.StringHashMapUnmanaged(void) = .empty;
                        var selected: ?[]const u8 = null;
                        while (try scanner.peekNextTokenType() != .object_end) {
                            const key = switch (try scanner.nextAllocMax(a, .alloc_if_needed, span.node.raw.len)) {
                                .string, .allocated_string => |key| key,
                                else => return error.SyntaxError,
                            };
                            if (!span.node.state.fanout_keys_validated) if ((try names.getOrPut(a, key)).found_existing) return error.DuplicateField;
                            switch (try scanner.peekNextTokenType()) {
                                .object_end, .array_end, .end_of_document => return error.SyntaxError,
                                else => {},
                            }
                            const value_start = scanner.cursor;
                            const reused = if (span.view.validated_ranges) |ranges| try ranges.skip(scanner, false) else false;
                            if (!reused) {
                                try scanner.skipValue();
                                if (scanner.cursor - value_start >= 256) if (span.view.validated_ranges) |ranges| ranges.refill(span.node.raw[value_start..scanner.cursor]);
                            }
                            if (std.mem.eql(u8, key, self.path[0])) selected = span.node.raw[value_start..scanner.cursor];
                        }
                        _ = try scanner.next();
                        slot.view = try View.initProjectedObject(a, span.node.raw[start..scanner.cursor], self.path[0], selected);
                    } else {
                        try scanner.skipValue();
                        slot.view = .init(a, span.node.raw[start..scanner.cursor]);
                    }
                    slot.view.validated_ranges = span.view.validated_ranges;
                    slot.view.parse_numbers = span.view.parse_numbers;
                    slot.view.canonical_source = span.view.canonical_source;
                    break :blk .{ .span = .{ .view = &slot.view, .node = &slot.view.root } };
                },
            };
            if (item.kind() != '{') continue;
            var object: Output.Object = .empty;
            try Output.includeObjectPath(&slot.owner, &object, item, self.path, self.selector);
            if (object.count() != 0) return object;
        }
    }
};

fn differentialScenario(a: Allocator) !void {
    const query = @import("../document_query.zig");
    const raw = "{\"a\":{\"numbers\":[1.00,1e2,-0,9223372036854775808,1e999],\"text\":\"escape\\n\\u0061\",\"other\":true},\"items\":[{\"x\":{\"m\":1,\"n\":2},\"z\":4},{\"x\":{\"m\":3},\"z\":5},null],\"omit\":null}";
    const selections = [_][]const []const u8{
        &.{"a"},                    &.{"a.numbers"},                                 &.{"a.text"},
        &.{ "a", "a.numbers.0" },   &.{ "a.numbers.0", "a" },                        &.{ "items", "items.x.m" },
        &.{ "items.x.m", "items" }, &.{ "items.0.x", "items.1.x.n", "items.0.x.m" }, &.{ "a", "a", "-omit" },
        &.{"-omit"},                &.{"*"},
    };
    const overlay_selections = [_][]const []const u8{
        &.{ "a", "a.numbers.0", "a.numbers.1" },
        &.{ "a.numbers.0", "a", "a.numbers.1" },
        &.{ "a", "a.missing.value" },
        &.{ "items.0.x", "items", "items.1.x.m" },
    };
    for (overlay_selections) |fields| {
        const opts: @import("../types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
        var expected = try query.lookupJson(a, raw, opts);
        defer expected.deinit(a);
        var actual = try query.lookupJsonWithOutput(a, a, raw, opts);
        defer actual.deinit(a);
        try std.testing.expectEqualStrings(expected.json, actual.json);
    }
    for (selections) |fields| {
        const opts: @import("../types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
        var expected = try query.lookupJson(a, raw, opts);
        defer expected.deinit(a);
        var actual = try query.lookupJsonWithOutput(a, a, raw, opts);
        defer actual.deinit(a);
        try std.testing.expectEqualStrings(expected.json, actual.json);
    }
    // Native JSON spans keep original numeric lexemes, but still reject
    // duplicate members inside a whole selected terminal as the DOM did.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var view = View.init(scratch, "{\"n\":[1.00,1e2],\"s\":\"a\\u0062\"}");
    const output = Output{ .borrowed = .{ .span = .{ .view = &view, .node = &view.root } } };
    const native = try output.serialize(scratch, a);
    defer a.free(native);
    try std.testing.expectEqualStrings("{\"n\":[1.00,1e2],\"s\":\"ab\"}", native);
}
test "public graph metric borrowed wire streaming projection preserves ordered copy on write and numeric contracts" {
    try differentialScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, differentialScenario, .{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var view = View.init(arena.allocator(), "{\"dup\":1,\"d\\u0075p\":2}");
    const value = Output{ .borrowed = .{ .span = .{ .view = &view, .node = &view.root } } };
    try std.testing.expectError(error.DuplicateField, value.serialize(arena.allocator(), std.testing.allocator));
    for ([_][]const u8{ "", "[1,]", "{\"x\":}", "1 2" }) |invalid| {
        var invalid_view = View.init(arena.allocator(), invalid);
        const invalid_value = Output{ .borrowed = .{ .span = .{ .view = &invalid_view, .node = &invalid_view.root } } };
        if (invalid_value.serialize(arena.allocator(), std.testing.allocator)) |bytes| {
            std.testing.allocator.free(bytes);
            return error.TestExpectedError;
        } else |_| {}
    }
}

test "public graph metric borrowed wire streaming whole terminal has bounded scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"values\":[");
    for (0..100000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("123456789");
    }
    try input.writer.writeAll("],\"omit\":true}");
    const opts: @import("../types.zig").LookupOptions = .{ .fields = &.{"values"}, .include_all_fields = false };
    var plan = (try query.SpanProjectionPlan.init(a, opts)).?;
    defer plan.deinit();
    var budget: Counter = .{ .backing = a, .limit = 6 * 1024 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 1000012), result.json.len);
    try std.testing.expectEqualSlices(u8, input.written()[0 .. input.written().len - 13], result.json[0 .. result.json.len - 1]);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("STREAMING_TERMINAL output_bytes={d} scratch_peak={d}\n", .{ result.json.len, budget.peak });
}

fn overlayAppendScenario(a: Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var base = View.init(scratch, "{\"a\":[1,2],\"b\":{\"x\":1,\"y\":2}}");
    base.parse_numbers = true;
    const selected = try @import("json_projection_plan.zig").compile(scratch, &.{.{ .parts = &.{ "a", "1" } }});
    try base.prepareProjection(selected, .visited, .any_container);
    var replacement = View.init(scratch, "{\"x\":9,\"z\":3}");
    replacement.parse_numbers = true;
    const extra: Value = .{ .integer = 7 };
    var root: Output.Object = .empty;
    var projection_owner = Owner.init(scratch);
    defer projection_owner.deinit();
    try Output.include(&projection_owner, &root, "v", .{ .span = .{ .view = &base, .node = &base.root } }, &.{});
    // Select from a different source into a borrowed base. Existing members
    // retain base order; new members append in first insertion order.
    try Output.include(&projection_owner, &root, "v", .{ .span = .{ .view = &replacement, .node = &replacement.root } }, &.{"z"});
    try Output.include(&projection_owner, &root, "v", .{ .span = .{ .view = &replacement, .node = &replacement.root } }, &.{"x"});
    try Output.include(&projection_owner, &root, "v", .{ .span = .{ .view = &base, .node = &base.root } }, &.{ "a", "1" });
    var extra_source: Value = .{ .object = .empty };
    try extra_source.object.put(scratch, "last", extra);
    try Output.include(&projection_owner, &root, "v", .{ .logical = &extra_source }, &.{"last"});
    const bytes = try (Output{ .object = root }).serialize(scratch, a);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"v\":{\"a\":[2],\"b\":{\"x\":1,\"y\":2},\"z\":3,\"x\":9,\"last\":7}}", bytes);
    try std.testing.expectEqual(@as(usize, 1), base.root.children.?.len);
}

test "public graph metric borrowed wire streaming sparse overlays preserve base order and appended members" {
    try overlayAppendScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, overlayAppendScenario, .{});
}

test "public graph metric borrowed wire streaming sparse wide object stays within bounded scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"wide\":{");
    for (0..20000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.print("\"k{d}\":{d}", .{ i, i });
    }
    try input.writer.writeAll("}}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{ "wide", "wide.k0" }, .include_all_fields = false })).?;
    defer plan.deinit();
    var budget: Counter = .{ .backing = a, .limit = 6 * 1024 * 1024 };
    var body = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer body.deinit(a);
    try std.testing.expectEqualStrings(input.written(), body.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("SPARSE_WIDE fields=20000 output_bytes={d} peak={d}\n", .{ body.json.len, budget.peak });
}

test "public graph metric borrowed wire streaming siblings reuse bounded key arenas" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll(if (i % 2 == 0) "{\"x\":1,\"y\":2}" else "{\"x\\u0061\":1,\"y\":2}");
    }
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{"items"}, .include_all_fields = false })).?;
    defer plan.deinit();
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 2 * 1024 * 1024 };
    var body = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer body.deinit(a);
    const expected = try query.lookupJson(a, input.written(), .{ .fields = &.{"items"}, .include_all_fields = false });
    defer a.free(expected.json);
    try std.testing.expectEqualStrings(expected.json, body.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.alloc_calls < 128);
    std.debug.print("STREAMING_SIBLINGS count=8000 backing_allocations={d} peak={d}\n", .{ budget.alloc_calls, budget.peak });
}

test "public graph metric borrowed wire streaming sparse overlays validate discarded base members" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var replacement: Value = .{ .object = .empty };
    try replacement.object.put(arena.allocator(), "dup", .{ .integer = 7 });
    for ([_][]const u8{ "{\"dup\":1,\"d\\u0075p\":2}", "{\"dup\":[1,]}", "{\"dup\":}" }) |raw| {
        var base = View.init(arena.allocator(), raw);
        var root: Output.Object = .empty;
        var projection_owner = Owner.init(arena.allocator());
        defer projection_owner.deinit();
        try Output.include(&projection_owner, &root, "v", .{ .span = .{ .view = &base, .node = &base.root } }, &.{});
        try Output.include(&projection_owner, &root, "v", .{ .logical = &replacement }, &.{"dup"});
        if ((Output{ .object = root }).serialize(arena.allocator(), a)) |bytes| {
            a.free(bytes);
            return error.TestExpectedError;
        } else |_| {}
    }
}

test "public graph metric borrowed wire streaming scratch retires oversized buffers without replacement allocation" {
    const counters = @import("../../lite/test_allocator.zig");
    var no_resize: counters.NoResizeAllocator = .{ .backing = std.testing.allocator };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator() };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    _ = try arena.allocator().alloc(u8, 64 * 1024);
    const allocated = budget.alloc_calls;
    try resetScratch(&arena);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(allocated, budget.alloc_calls);
    _ = try arena.allocator().alloc(u8, 128);
    const small_allocated = budget.alloc_calls;
    try resetScratch(&arena);
    _ = try arena.allocator().alloc(u8, 128);
    try std.testing.expectEqual(small_allocated, budget.alloc_calls);
}

fn fanoutOrderingScenario(a: Allocator) !void {
    const query = @import("../document_query.zig");
    const raw = "{\"items\":[{\"x\":{\"m\":1.00,\"n\":2},\"nested\":[{\"z\":3},{\"q\":4}]},null,{\"x\":{},\"nested\":[]},{\"other\":5},{\"x\":[{\"m\":6}]}],\"empty\":[]}";
    const selections = [_][]const []const u8{
        &.{"items.x"},
        &.{ "items", "items.missing" },
        &.{ "items.x.m", "items.missing" },
        &.{ "items.x", "items.0.x.n" },
        &.{ "items.0.x.n", "items.x" },
        &.{ "items.nested.z", "items.x" },
        &.{ "items.x", "items.nested.z" },
        &.{ "items.x.m", "items", "items.nested.missing" },
        &.{ "items.nested.z", "empty.x" },
    };
    for (selections) |fields| {
        const opts: @import("../types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
        var expected = try query.lookupJson(a, raw, opts);
        defer expected.deinit(a);
        var actual = try query.lookupJsonWithOutput(a, a, raw, opts);
        defer actual.deinit(a);
        try std.testing.expectEqualStrings(expected.json, actual.json);
    }
    // Loaded logical fields use the same lazy fanout and replacement contract.
    var parsed = try std.json.parseFromSlice(Value, a, raw, .{});
    defer parsed.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var object: Output.Object = .empty;
    var projection_owner = Owner.init(arena.allocator());
    defer projection_owner.deinit();
    try Output.include(&projection_owner, &object, "items", .{ .logical = parsed.value.object.getPtr("items").? }, &.{ "nested", "z" });
    const result = try (Output{ .object = object }).serialize(arena.allocator(), a);
    defer a.free(result);
    var expected = try query.lookupJson(a, raw, .{ .fields = &.{"items.nested.z"}, .include_all_fields = false });
    defer expected.deinit(a);
    try std.testing.expectEqualStrings(expected.json, result);
}

test "public graph metric borrowed wire streaming fanout preserves ordered arrays and allocation failure ownership" {
    try fanoutOrderingScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, fanoutOrderingScenario, .{});
}

test "public graph metric borrowed wire streaming fanout bounds navigation maps and terminal scanner scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":{\"m\":1,\"n\":2}}");
    }
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{"items.x"}, .include_all_fields = false })).?;
    defer plan.deinit();
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 1024 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqualStrings(input.written(), result.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.alloc_calls < 128);
    std.debug.print("STREAMING_FANOUT count=8000 allocations={d} peak={d}\n", .{ budget.alloc_calls, budget.peak });
}

test "public graph metric borrowed wire streaming fused overlay consumes exactly one value including escaped keys" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var view = View.init(arena.allocator(), "{\"a\\u0062\":{\"s\":\"\\n\\u0061\",\"n\":1e2},\"arr\":[1,true,null,{\"z\":2}],\"last\":false}");
    var replacement: Value = .{ .object = .empty };
    try replacement.object.put(arena.allocator(), "last", .{ .bool = true });
    var object: Output.Object = .empty;
    var projection_owner = Owner.init(arena.allocator());
    defer projection_owner.deinit();
    try Output.include(&projection_owner, &object, "v", .{ .span = .{ .view = &view, .node = &view.root } }, &.{});
    try Output.include(&projection_owner, &object, "v", .{ .logical = &replacement }, &.{"last"});
    const bytes = try (Output{ .object = object }).serialize(arena.allocator(), a);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"v\":{\"ab\":{\"s\":\"\\na\",\"n\":1e2},\"arr\":[1,true,null,{\"z\":2}],\"last\":true}}", bytes);
    // The transcode context is reused across object members, but never shares
    // duplicate-name sets between distinct objects or overlooks trailing data.
    for ([_][]const u8{ "{\"x\":1,\"x\":2}", "{\"x\":1} true", "[1,]" }) |raw| {
        view = View.init(arena.allocator(), raw);
        const invalid: Output = .{ .borrowed = .{ .span = .{ .view = &view, .node = &view.root } } };
        if (invalid.serialize(arena.allocator(), a)) |result| {
            a.free(result);
            return error.TestExpectedError;
        } else |_| {}
    }
}

test "public graph metric borrowed wire streaming fanout validates unprepared sibling keys before reusing proof" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var view = View.init(arena.allocator(), "[{\"x\":1},{\"x\":2,\"\\u0078\":3}]");
    var object: Output.Object = .empty;
    var projection_owner = Owner.init(arena.allocator());
    defer projection_owner.deinit();
    try Output.include(&projection_owner, &object, "items", .{ .span = .{ .view = &view, .node = &view.root } }, &.{"x"});
    try std.testing.expectError(error.DuplicateField, (Output{ .object = object }).serialize(arena.allocator(), a));
}

test "public graph metric borrowed wire deep streaming fanout retains only one prepared item" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    var field = std.Io.Writer.Allocating.init(a);
    defer field.deinit();
    try input.writer.writeAll("{\"items\":[");
    try field.writer.writeAll("items");
    for (0..32) |_| {
        try input.writer.writeAll("{\"x\":");
        try field.writer.writeAll(".x");
    }
    try input.writer.writeByte('[');
    for (0..100000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("123456789");
    }
    try input.writer.writeByte(']');
    for (0..32) |_| try input.writer.writeByte('}');
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{field.written()}, .include_all_fields = false })).?;
    defer plan.deinit();
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var budget: Counter = .{ .backing = a, .limit = 512 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    try std.testing.expectEqualStrings(input.written(), result.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("DEEP_FANOUT depth=32 output_bytes={d} scratch_peak={d}\n", .{ result.json.len, budget.peak });
}

test "public graph metric borrowed wire direct writer streams compact vectors with no scratch allocation" {
    const a = std.testing.allocator;
    const vector = [_]f32{ 0.1, -0.0, 1.25 };
    const value: Output = .{ .borrowed = .{ .vector = &vector } };
    var expected = std.Io.Writer.Allocating.init(a);
    defer expected.deinit();
    var logical = [_]Value{ .{ .float = @as(f64, vector[0]) }, .{ .float = @as(f64, vector[1]) }, .{ .float = @as(f64, vector[2]) } };
    const array: Value = .{ .array = .fromOwnedSlice(a, &logical) };
    try std.json.Stringify.value(array, .{}, &expected.writer);
    var buffer: [256]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var budget: Counter = .{ .backing = a, .limit = 0 };
    try value.writeTo(budget.allocator(), &writer);
    try std.testing.expectEqualStrings(expected.written(), writer.buffered());
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_attempts);
}

test "public graph metric borrowed wire multi component fanout reuses item preparation scratch" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[");
    for (0..8000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":{\"m\":1,\"n\":2}}");
    }
    try input.writer.writeAll("]}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{"items.x.m"}, .include_all_fields = false })).?;
    defer plan.deinit();
    const counters = @import("../../lite/test_allocator.zig");
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 1024 * 1024 };
    var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
    defer result.deinit(a);
    var expected = try query.lookupJson(a, input.written(), .{ .fields = &.{"items.x.m"}, .include_all_fields = false });
    defer expected.deinit(a);
    try std.testing.expectEqualStrings(expected.json, result.json);
    try std.testing.expect(budget.alloc_calls < 128);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("PREPARED_FANOUT count=8000 allocations={d} peak={d}\n", .{ budget.alloc_calls, budget.peak });
}

test "public graph metric borrowed wire primed nested fanout survives replacement failures and repeated output" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var arena: std.heap.ArenaAllocator = .init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const raw = "[{\"x\":[{\"x\":[{\"leaf\":[1,2,3]},{\"other\":1}]}]},{\"x\":[{\"x\":[{\"leaf\":[4]}]}]}]";
            var view: View = .init(scratch, raw);
            var root: Output.Object = .empty;
            const source: Source = .{ .span = .{ .view = &view, .node = &view.root } };
            var projection_owner = Owner.init(scratch);
            defer projection_owner.deinit();
            try Output.include(&projection_owner, &root, "items", source, &.{ "x", "x", "leaf" });
            // Abandon a live prepared chain, then preserve its replacement when
            // a later selector produces no surviving objects.
            try Output.include(&projection_owner, &root, "items", source, &.{ "x", "x", "leaf" });
            try Output.include(&projection_owner, &root, "items", source, &.{"absent"});
            const output: Output = .{ .object = root };
            var storage: [32]u8 = undefined;
            var failed: std.Io.Writer = .fixed(&storage);
            if (output.writeTo(scratch, &failed)) |_| return error.ExpectedWriteFailure else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expect(err == error.WriteFailed);
            }
            for (0..2) |_| {
                const bytes = try output.serialize(scratch, a);
                defer a.free(bytes);
                try std.testing.expectEqualStrings("{\"items\":[{\"x\":[{\"x\":[{\"leaf\":[1,2,3]}]}]},{\"x\":[{\"x\":[{\"leaf\":[4]}]}]}]}", bytes);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "public graph metric borrowed wire nested fanout ownership bounds priming and reuses immutable suffix plans" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    const fields: []const []const u8 = &.{"items.x.x.x.x.x.x.x.x.m"};
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = fields, .include_all_fields = false })).?;
    defer plan.deinit();
    const selected = plan.paths[0].selector.?;
    const capacity = plan.arena.queryCapacity();
    for ([_]bool{ false, true }) |survives| {
        for ([_]usize{ 1, 100 }) |count| {
            var input: std.Io.Writer.Allocating = .init(a);
            defer input.deinit();
            try input.writer.writeAll("{\"items\":[");
            for (0..count) |i| {
                if (i != 0) try input.writer.writeByte(',');
                for (0..8) |_| try input.writer.writeAll("{\"x\":[");
                try input.writer.writeAll(if (survives) "{\"m\":1,\"absent\":0}" else "{\"absent\":0}");
                for (0..8) |_| try input.writer.writeAll("]}");
            }
            try input.writer.writeAll("]}");
            var no_resize: counters.NoResizeAllocator = .{ .backing = a };
            var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 512 * 1024 };
            var actual = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
            defer actual.deinit(a);
            var expected = try query.lookupJson(a, input.written(), .{ .fields = fields, .include_all_fields = false });
            defer expected.deinit(a);
            try std.testing.expectEqualStrings(expected.json, actual.json);
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            try std.testing.expectEqual(capacity, plan.arena.queryCapacity());
            try std.testing.expectEqual(selected, plan.paths[0].selector.?);
            std.debug.print("NESTED_PRIMING survives={} count={d} peak={d} allocations={d}\n", .{ survives, count, budget.peak, budget.alloc_calls });
        }
    }
}

test "public graph metric borrowed wire replaced fanouts retire scratch under row quota" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var input: std.Io.Writer.Allocating = .init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"items\":[{\"x\":{\"m\":1");
    for (0..64) |i| try input.writer.print(",\"m{d}\":1", .{i});
    for (0..2000) |i| try input.writer.print(",\"key{d}\":0", .{i});
    try input.writer.writeAll("}}]}");
    var names: std.heap.ArenaAllocator = .init(a);
    defer names.deinit();
    var fields: [64][]const u8 = undefined;
    for (&fields, 0..) |*field, i| field.* = try std.fmt.allocPrint(names.allocator(), "items.x.m{d}", .{i});
    for ([_]usize{ 1, 16, 64 }) |count| {
        var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = fields[0..count], .include_all_fields = false })).?;
        defer plan.deinit();
        var no_resize: counters.NoResizeAllocator = .{ .backing = a };
        var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 1024 * 1024 };
        var result = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
        defer result.deinit(a);
        const expected = try std.fmt.allocPrint(a, "{{\"items\":[{{\"x\":{{\"m{d}\":1}}}}]}}", .{count - 1});
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, result.json);
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expect(budget.peak < 256 * 1024);
        std.debug.print("RETIRED_FANOUT selectors={d} peak={d} allocations={d}\n", .{ count, budget.peak, budget.alloc_calls });
    }
}

test "public graph metric borrowed wire nested cursor arenas reuse across parent rows" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    const fields: []const []const u8 = &.{"items.x.x.x.m"};
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = fields, .include_all_fields = false })).?;
    defer plan.deinit();
    var warm_calls: ?usize = null;
    for ([_]usize{ 1, 100, 1000 }) |count| {
        var input: std.Io.Writer.Allocating = .init(a);
        defer input.deinit();
        try input.writer.writeAll("{\"items\":[");
        for (0..count) |i| {
            if (i != 0) try input.writer.writeByte(',');
            try input.writer.writeAll("{\"x\":[{\"x\":[{\"x\":[{");
            try input.writer.print("\"{s}\":{d}", .{ if (i % 3 == 0) "missing" else "m", i });
            try input.writer.writeAll("}]}]}]}");
        }
        try input.writer.writeAll("]}");
        var no_resize: counters.NoResizeAllocator = .{ .backing = a };
        var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 512 * 1024 };
        var actual = try query.lookupJsonWithPlan(budget.allocator(), a, input.written(), &plan);
        defer actual.deinit(a);
        var expected = try query.lookupJson(a, input.written(), .{ .fields = fields, .include_all_fields = false });
        defer expected.deinit(a);
        try std.testing.expectEqualStrings(expected.json, actual.json);
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        std.debug.print("POOLED_FANOUT rows={d} peak={d} allocations={d}\n", .{ count, budget.peak, budget.alloc_calls });
        try std.testing.expect(budget.alloc_calls < 128);
        if (count >= 100) {
            if (warm_calls) |calls| try std.testing.expectEqual(calls, budget.alloc_calls);
            warm_calls = budget.alloc_calls;
        }
    }
}

test "public graph metric borrowed wire cursor pool accounts physical bytes and retires without allocation" {
    const a = std.testing.allocator;
    const counters = @import("../../lite/test_allocator.zig");
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 4 * 1024 * 1024 };
    var resources: Resources = .init(budget.allocator());
    defer resources.deinit();
    var slots: [64]*CursorScratch = undefined;
    var live: usize = 0;
    errdefer for (slots[0..live]) |slot| resources.release(slot);
    for (&slots) |*slot| {
        slot.* = try resources.acquire();
        live += 1;
        const buffer = try slot.*.arena.allocator().alloc(u8, 8192);
        @memset(buffer, 17);
    }
    for (slots) |slot| resources.release(slot);
    live = 0;
    try std.testing.expectEqual(@as(usize, 0), resources.active);
    try std.testing.expect(resources.idle_bytes <= Resources.reuse_limit);
    try std.testing.expectEqual(budget.live, resources.idle_bytes);
    const warm = try resources.acquire();
    resources.release(warm);
    const calls = budget.alloc_calls;
    for (0..16) |_| {
        const slot = try resources.acquire();
        resources.release(slot);
    }
    try std.testing.expectEqual(calls, budget.alloc_calls);
    const wide = try resources.acquire();
    var wide_live = true;
    defer if (wide_live) resources.release(wide);
    _ = try wide.arena.allocator().alloc(u8, 512 * 1024);
    const attempts = budget.alloc_attempts;
    budget.limit = 0;
    resources.release(wide);
    wide_live = false;
    try std.testing.expectEqual(wide, resources.idle.?);
    try std.testing.expectEqual(@as(usize, 0), wide.accounting.bytes);
    try std.testing.expectEqual(attempts, budget.alloc_attempts);
    try std.testing.expectEqual(budget.live, resources.idle_bytes);
    resources.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "public graph metric borrowed wire retired cursor cannot close a reused arena lease" {
    const a = std.testing.allocator;
    var resources: Resources = .init(a);
    defer resources.deinit();
    var first_view: View = .init(a, "[{\"x\":1}]");
    var second_view: View = .init(a, "[{\"x\":2}]");
    var first = FanoutRows.init(&resources, .{ .source = .{ .span = .{ .view = &first_view, .node = &first_view.root } }, .path = &.{"x"} });
    defer first.deinit();
    _ = (try first.next()).?;
    const slot = first.scratch.?;
    first.deinit();
    var second = FanoutRows.init(&resources, .{ .source = .{ .span = .{ .view = &second_view, .node = &second_view.root } }, .path = &.{"x"} });
    defer second.deinit();
    const row = (try second.next()).?;
    try std.testing.expectEqual(slot, second.scratch.?);
    first.deinit();
    try std.testing.expectEqual(@as(usize, 1), resources.active);
    const bytes = try (Output{ .object = row }).serialize(a, a);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"x\":2}", bytes);
}

test "public graph metric borrowed wire streaming writer reuses wide escaped key tables" {
    const a = std.testing.allocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    var expected = std.Io.Writer.Allocating.init(a);
    defer expected.deinit();
    try input.writer.writeByte('{');
    try expected.writer.writeByte('{');
    for (0..1000) |i| {
        if (i != 0) {
            try input.writer.writeByte(',');
            try expected.writer.writeByte(',');
        }
        try input.writer.print("\"\\u006b{d}\":0", .{i});
        try expected.writer.print("\"k{d}\":0", .{i});
    }
    try input.writer.writeByte('}');
    try expected.writer.writeByte('}');
    const Fixture = struct {
        fn run(alloc: Allocator, raw: []const u8, canonical: []const u8) !void {
            var counted: std.testing.FailingAllocator = .init(alloc, .{});
            var context = StreamContext.init(counted.allocator());
            defer context.deinit();
            for (0..4) |iteration| {
                var out = std.Io.Writer.Allocating.init(alloc);
                defer out.deinit();
                const calls = counted.allocations;
                context.writeSpan(&out.writer, raw, true) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
                try std.testing.expectEqualStrings(canonical, out.written());
                if (iteration != 0) try std.testing.expectEqual(calls, counted.allocations);
                try std.testing.expect(context.keys.retainedBytes() <= @TypeOf(context.keys).reuse_limit);
            }
        }
    };
    try Fixture.run(a, input.written(), expected.written());
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{ input.written(), expected.written() });
}

test "public graph metric borrowed wire streaming writer retires sparse tables and recovers from write failure" {
    const a = std.testing.allocator;
    var context = StreamContext.init(a);
    defer context.deinit();
    var wide = std.Io.Writer.Allocating.init(a);
    defer wide.deinit();
    try wide.writer.writeByte('{');
    for (0..5000) |i| {
        if (i != 0) try wide.writer.writeByte(',');
        try wide.writer.print("\"k{d}\":0", .{i});
    }
    try wide.writer.writeByte('}');
    var output = std.Io.Writer.Allocating.init(a);
    defer output.deinit();
    try context.writeSpan(&output.writer, wide.written(), true);
    try std.testing.expect(context.keys.table_bytes > 64 * 1024);
    try context.writeSpan(&output.writer, "{\"a\":0}", true);
    try context.writeSpan(&output.writer, "{\"a\":0}", true);
    try std.testing.expect(context.keys.table_bytes < 1024);
    var memory: [2]u8 = undefined;
    var fixed = std.Io.Writer.fixed(&memory);
    try std.testing.expectError(error.WriteFailed, context.writeSpan(&fixed, "{\"a\":{\"b\":0}}", true));
    try std.testing.expectEqual(@as(usize, 0), context.keys.retainedBytes());
    try std.testing.expectError(error.DuplicateField, context.writeSpan(&output.writer, "{\"x\":0,\"\\u0078\":1}", true));
    try std.testing.expectEqual(@as(usize, 0), context.keys.retainedBytes());
    var recovered = std.Io.Writer.Allocating.init(a);
    defer recovered.deinit();
    try context.writeSpan(&recovered.writer, "{\"\\u0061\":{\"a\":1}}", true);
    try std.testing.expectEqualStrings("{\"a\":{\"a\":1}}", recovered.written());
    const depth = 10000;
    const deep = try a.alloc(u8, depth * 2 + 1);
    defer a.free(deep);
    @memset(deep[0..depth], '[');
    deep[depth] = '0';
    @memset(deep[depth + 1 ..], ']');
    var deep_output = std.Io.Writer.Allocating.init(a);
    defer deep_output.deinit();
    try context.writeSpan(&deep_output.writer, deep, true);
    try std.testing.expectEqualStrings(deep, deep_output.written());
    try std.testing.expectEqual(@as(usize, 0), context.keys.retainedBytes());
}

test "public graph metric borrowed wire validated fanout extents preserve ordered projections and failure cleanup" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    var raw: std.Io.Writer.Allocating = .init(a);
    defer raw.deinit();
    try raw.writer.writeAll("{\"items\":[{\"x\":[{\"m\":1,\"huge\":\"");
    try raw.writer.splatByteAll('a', 4096);
    try raw.writer.writeAll("\"},{\"n\":2}]},{\"x\":[{\"m\":3}]}]}");
    const Fixture = struct {
        fn run(alloc: Allocator, input: []const u8) !void {
            const fields: []const []const u8 = &.{ "items.x.m", "items.x.0.n", "items.x.n" };
            const opts: @import("../types.zig").LookupOptions = .{ .fields = fields, .include_all_fields = false };
            var expected = try query.lookupJson(alloc, input, opts);
            defer expected.deinit(alloc);
            var actual = try query.lookupJsonWithOutput(alloc, alloc, input, opts);
            defer actual.deinit(alloc);
            try std.testing.expectEqualStrings(expected.json, actual.json);
        }
    };
    try Fixture.run(a, raw.written());
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{raw.written()});
    const invalid = "{\"items\":[{\"x\":[{\"m\":1,\"deep\":{\"d\":0,\"d\":1}}]}]}";
    try std.testing.expectError(error.DuplicateField, query.lookupJsonWithOutput(a, a, invalid, .{ .fields = &.{"items.x.m"}, .include_all_fields = false }));
}

test "public graph metric borrowed wire lossy validated fanout index bounds scratch across many arrays" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var raw: std.Io.Writer.Allocating = .init(a);
    defer raw.deinit();
    try raw.writer.writeAll("{\"items\":[");
    for (0..5000) |i| {
        if (i != 0) try raw.writer.writeByte(',');
        try raw.writer.print("{{\"x\":[{{\"{s}\":{d}}}]}}", .{ if (i % 3 == 0) "missing" else "m", i });
    }
    try raw.writer.writeAll("]}");
    const opts: @import("../types.zig").LookupOptions = .{ .fields = &.{"items.x.m"}, .include_all_fields = false };
    var expected = try query.lookupJson(a, raw.written(), opts);
    defer expected.deinit(a);
    var no_resize: counters.NoResizeAllocator = .{ .backing = a };
    var budget: counters.BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 256 * 1024 };
    var actual = try query.lookupJsonWithOutput(budget.allocator(), a, raw.written(), opts);
    defer actual.deinit(a);
    try std.testing.expectEqualStrings(expected.json, actual.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "public graph metric borrowed wire escaped scalar emission uses fixed scratch and standard Unicode semantics" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var raw = std.Io.Writer.Allocating.init(a);
    defer raw.deinit();
    try raw.writer.writeAll("{\"payload\":{\"text\":\"");
    // Exercise coalescing and all decoded-chunk widths, including surrogate pairs.
    for (0..8192) |_| try raw.writer.writeAll("a\\u0061\\u00e9\\u4e2d\\uD834\\uDD1E\\n\\t\\b\\f\\r\\u0000\\\"\\\\\\/");
    try raw.writer.writeAll("\"}}");
    const opts = @import("../types.zig").LookupOptions{ .fields = &.{"payload.text"}, .include_all_fields = false };
    var expected = try query.lookupJson(a, raw.written(), opts);
    defer expected.deinit(a);
    var plan = (try query.SpanProjectionPlan.init(a, opts)).?;
    defer plan.deinit();
    var budget: counters.BudgetAllocator = .{ .backing = a, .limit = 32 * 1024 };
    var actual = try query.lookupJsonWithPlan(budget.allocator(), a, raw.written(), &plan);
    defer actual.deinit(a);
    try std.testing.expectEqualStrings(expected.json, actual.json);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    var context = StreamContext.init(a);
    defer context.deinit();
    for ([_][]const u8{ "\"\"", "\"plain\"", "\"\\u0000\\u007f\\u0080\\udbff\\udfff\"", "\"é中𝄞\"" }) |input| {
        var parsed = try std.json.parseFromSlice(Value, a, input, .{});
        defer parsed.deinit();
        const canonical = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(canonical);
        var output = std.Io.Writer.Allocating.init(a);
        defer output.deinit();
        try context.writeSpan(&output.writer, input, true);
        try std.testing.expectEqualStrings(canonical, output.written());
    }
    var short: [2]u8 = undefined;
    var failed = std.Io.Writer.fixed(&short);
    try std.testing.expectError(error.WriteFailed, context.writeSpan(&failed, "\"\\u0061\\u0062\\u0063\"", true));
    for ([_][]const u8{ "\"\\ud800\"", "\"\\udc00\"", "\"\\uZZZZ\"", "\"unterminated", "\"\\q\"" }) |input| {
        var output = std.Io.Writer.Allocating.init(a);
        defer output.deinit();
        if (context.writeSpan(&output.writer, input, true)) |_| return error.ExpectedSyntaxFailure else |_| {}
    }
}

test "public graph metric borrowed wire exact escaped output stays within bounded scratch for arena owners" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const counters = @import("../../lite/test_allocator.zig");
    var raw = std.Io.Writer.Allocating.init(a);
    defer raw.deinit();
    try raw.writer.writeAll("{\"payload\":{\"text\":\"");
    for (0..262144) |_| try raw.writer.writeAll("\\u0061");
    try raw.writer.writeAll("\"}}");
    var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = &.{"payload.text"}, .include_all_fields = false })).?;
    defer plan.deinit();
    for ([_]bool{ false, true }) |exact| {
        var budget: counters.BudgetAllocator = .{ .backing = a, .limit = 32 * 1024 };
        var output_budget: counters.BudgetAllocator = .{ .backing = a };
        {
            var arena = std.heap.ArenaAllocator.init(output_budget.allocator());
            defer arena.deinit();
            const result = try query.lookupJsonWithSourcesPolicy(budget.allocator(), arena.allocator(), raw.written(), &plan, &.{}, exact);
            const prefix = "{\"payload\":{\"text\":\"";
            try std.testing.expectEqualStrings(prefix, result.json[0..prefix.len]);
            try std.testing.expectEqual(262144 + "{\"payload\":{\"text\":\"\"}}".len, result.json.len);
            for (result.json[prefix.len..][0..262144]) |byte| try std.testing.expectEqual(@as(u8, 'a'), byte);
            try std.testing.expectEqualStrings("\"}}", result.json[result.json.len - 3 ..]);
            try std.testing.expectEqual(@as(usize, 1), output_budget.alloc_calls);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expectEqual(@as(usize, 0), output_budget.live);
    }
}

test "public graph metric borrowed wire exact stream replay bounds allocation and rejects changed lengths" {
    const Stream = @import("embedding_fields.zig").EmbeddingStream;
    const Fixture = struct {
        resets: usize = 0,
        emitted: bool = false,
        delta: i8 = 0,
        fail_replay: bool = false,
        length: usize = 4096,
        vector: [4096]f32 = @splat(0),
        fn reset(ctx: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.resets += 1;
            self.emitted = false;
            if (self.fail_replay and self.resets == 2) return error.ReplayFailed;
        }
        fn next(ctx: *anyopaque) !?Stream.Field {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.emitted) return null;
            self.emitted = true;
            const len: usize = if (self.resets == 2 and self.delta < 0) self.length - 2 else if (self.resets == 1 and self.delta > 0) self.length - 2 else self.length;
            return .{ .name = "v", .vector = self.vector[0..len] };
        }
        fn close(_: *anyopaque) void {}
        fn run(alloc: Allocator, delta: i8, fail_replay: bool) !void {
            var fixture: @This() = .{ .delta = delta, .fail_replay = fail_replay };
            var stream: Stream = .{ .ctx = &fixture, .next_fn = next, .reset_fn = reset, .close_fn = close };
            defer stream.deinit();
            const value: Output = .{ .borrowed = .{ .embedding_stream = &stream } };
            var scratch: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = std.testing.allocator, .limit = 0 };
            if (delta != 0) {
                try std.testing.expectError(error.OutputSizeChanged, value.serializeWithPolicy(scratch.allocator(), alloc, true));
            } else if (fail_replay) {
                try std.testing.expectError(error.ReplayFailed, value.serializeWithPolicy(scratch.allocator(), alloc, true));
            } else {
                const bytes = try value.serializeWithPolicy(scratch.allocator(), alloc, true);
                defer alloc.free(bytes);
                try std.testing.expectEqualStrings("{\"v\":[0", bytes[0..7]);
                try std.testing.expectEqual(@as(usize, 8199), bytes.len);
            }
            try std.testing.expectEqual(@as(usize, 2), fixture.resets);
            try std.testing.expectEqual(@as(usize, 0), scratch.alloc_attempts);
        }
    };
    const a = std.testing.allocator;
    for ([_]i8{ -1, 0, 1 }) |delta| try Fixture.run(a, delta, false);
    try Fixture.run(a, 0, true);
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{ @as(i8, 0), false });
    var small: Fixture = .{ .length = 1 };
    var stream: Stream = .{ .ctx = &small, .next_fn = Fixture.next, .reset_fn = Fixture.reset, .close_fn = Fixture.close };
    defer stream.deinit();
    const bytes = try (Output{ .borrowed = .{ .embedding_stream = &stream } }).serializeWithPolicy(a, a, true);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("{\"v\":[0]}", bytes);
    try std.testing.expectEqual(@as(usize, 1), small.resets);
}

test "public graph metric borrowed wire exact nested fanout replay survives errors and repeated serialization" {
    const a = std.testing.allocator;
    var raw = std.Io.Writer.Allocating.init(a);
    defer raw.deinit();
    try raw.writer.writeByte('[');
    for (0..4) |i| {
        if (i != 0) try raw.writer.writeByte(',');
        try raw.writer.writeAll("{\"x\":[{\"m\":\"");
        try raw.writer.splatByteAll('a', 2048);
        try raw.writer.writeAll("\"}]}");
    }
    try raw.writer.writeByte(']');
    const Fixture = struct {
        fn run(alloc: Allocator, input: []const u8) !void {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var view = View.init(arena.allocator(), input);
            var owner = Owner.init(arena.allocator());
            defer owner.deinit();
            var root: Output.Object = .empty;
            try Output.include(&owner, &root, "items", .{ .span = .{ .view = &view, .node = &view.root } }, &.{ "x", "m" });
            const value: Output = .{ .object = root };
            for (0..2) |_| {
                const bytes = try value.serializeWithPolicy(alloc, alloc, true);
                defer alloc.free(bytes);
                try std.testing.expectEqualStrings("{\"items\":", bytes[0..9]);
                try std.testing.expectEqualStrings(input, bytes[9 .. bytes.len - 1]);
                try std.testing.expectEqual(@as(u8, '}'), bytes[bytes.len - 1]);
            }
        }
    };
    try Fixture.run(a, raw.written());
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{raw.written()});
}

test "public graph metric borrowed wire canonical span reuse respects source bounds and number mode" {
    const a = std.testing.allocator;
    const raw = "{\"n\":900719925474099312345}";
    var view = View.init(a, raw);
    view.canonical_source = raw;
    const value: Output = .{ .borrowed = .{ .span = .{ .view = &view, .node = &view.root } } };
    var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 0 };
    const exact = try value.serializeWithPolicy(budget.allocator(), a, true);
    defer a.free(exact);
    try std.testing.expectEqualStrings(raw, exact);
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_attempts);
    view.parse_numbers = true;
    var parsed = try std.json.parseFromSlice(Value, a, raw, .{});
    defer parsed.deinit();
    const expected = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(expected);
    const normalized = try value.serializeWithPolicy(a, a, true);
    defer a.free(normalized);
    try std.testing.expectEqualStrings(expected, normalized);
    view.parse_numbers = false;
    var foreign: View.Node = .{ .raw = "[0,}" };
    try std.testing.expect(!view.canCopyCanonical(foreign.raw));
    try std.testing.expectError(error.SyntaxError, (Output{ .borrowed = .{ .span = .{ .view = &view, .node = &foreign } } }).serializeWithPolicy(a, a, true));
}

test "logical value conditional validated canonical strings preserve normalization and scope" {
    const a = std.testing.allocator;
    const query = @import("../document_query.zig");
    const Ranges = View.ValidatedRanges;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const plain = try temp.alloc(u8, 4096);
    @memset(plain, 'x');
    var controls: std.Io.Writer.Allocating = .init(temp);
    defer controls.deinit();
    for (0..32) |_| {
        for (0..128) |c| try controls.writer.writeByte(@intCast(c));
        try controls.writer.writeAll("café λ 🚀 \u{2028}");
    }
    const canonical_plain = try std.json.Stringify.valueAlloc(temp, plain, .{});
    const canonical_controls = try std.json.Stringify.valueAlloc(temp, controls.written(), .{});
    const noncanonical = try std.fmt.allocPrint(temp, "\"{s}\\u0061\\u000A\\/\\uD834\\uDD1E\"", .{plain});
    for ([_][]const u8{ canonical_plain, canonical_controls, noncanonical }, 0..) |encoded, index| {
        // Publish only after actual strict JSON validation, just like View.
        var parsed = try std.json.parseFromSlice(Value, a, encoded, .{});
        defer parsed.deinit();
        var ranges = Ranges.init(a);
        defer ranges.deinit();
        try ranges.remember(encoded);
        const proof = ranges.validated(encoded, true).?;
        var scanner = std.json.Scanner.initCompleteInput(a, encoded);
        defer scanner.deinit();
        try std.testing.expectEqual(.string, try scanner.peekNextTokenType());
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        const start = scanner.cursor;
        const copied = try proof.copyCanonicalString(&scanner, &output.writer);
        try std.testing.expectEqual(index < 2, copied);
        if (copied) {
            try std.testing.expectEqualStrings(encoded, output.written());
            try std.testing.expectEqual(.end_of_document, try scanner.next());
        } else {
            try std.testing.expectEqual(start, scanner.cursor);
            try std.testing.expectEqual(@as(usize, 0), output.written().len);
        }
        const foreign = try temp.dupe(u8, encoded);
        var other = std.json.Scanner.initCompleteInput(a, foreign);
        defer other.deinit();
        _ = try other.peekNextTokenType();
        try std.testing.expect(!try proof.copyCanonicalString(&other, &output.writer));

        const raw = try std.fmt.allocPrint(temp, "{{\"payload\":{{\"s\":{s}}},\"number\":1.0000}}", .{encoded});
        const options: @import("../types.zig").LookupOptions = .{ .fields = &.{"*"}, .include_all_fields = false };
        // Ordinary object projection has no fanout extents. Its complete-input
        // receipt must still be published, or the large-string path stays idle.
        var object_ranges = Ranges.init(a);
        defer object_ranges.deinit();
        var view = View.init(temp, raw);
        var selector: plan_mod.Selector = .{};
        try view.prepareProjectionWithOptions(&selector, .all, .object_only, .{ .retain_root_fields = true, .ranges = &object_ranges });
        try std.testing.expectEqual(@as(usize, 0), object_ranges.entries.len);
        const receipt = view.validated_ranges orelse return error.MissingSyntaxReceipt;
        const payload = (try view.field(&view.root, "payload")).?;
        const node = (try view.field(payload, "s")).?;
        var nested = std.json.Scanner.initCompleteInput(a, node.raw);
        defer nested.deinit();
        _ = try nested.peekNextTokenType();
        var nested_output: std.Io.Writer.Allocating = .init(a);
        defer nested_output.deinit();
        try std.testing.expectEqual(index < 2, try receipt.copyCanonicalString(&nested, &nested_output.writer));
        const expected = try query.lookupJson(a, raw, options);
        defer a.free(expected.json);
        const actual = try query.lookupJsonWithOutput(a, a, raw, options);
        defer a.free(actual.json);
        try std.testing.expectEqualStrings(expected.json, actual.json);
        var plan = (try query.SpanProjectionPlan.init(a, options)).?;
        defer plan.deinit();
        var wire: std.Io.Writer.Allocating = .init(a);
        defer wire.deinit();
        try query.writeLookupObject(a, &wire.writer, raw, &plan);
        try std.testing.expectEqualStrings(expected.json, wire.written());
    }
    // A canonical payload cannot hide a later duplicate key from validation.
    const duplicate = try std.fmt.allocPrint(temp, "{{\"s\":{s},\"s\":0}}", .{canonical_plain});
    try std.testing.expectError(error.DuplicateField, query.lookupJsonWithOutput(a, a, duplicate, .{ .fields = &.{"*"}, .include_all_fields = false }));
}
