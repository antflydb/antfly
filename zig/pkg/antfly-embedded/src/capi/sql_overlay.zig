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

//! Ordered merge of a native statement snapshot and transaction postimages.
//! Owns one native page; staged rows and shadow keys are captured at open so
//! subsequent statements and savepoint rollback cannot alter an open cursor.
const h = @import("handles.zig");
const std = h.std;
const d = h.antfly.capi_dependencies;
const catalog = d.sql_catalog;
const Json = std.json.Value;
const Session = @import("sql_session.zig").Session;

const Overlay = struct {
    alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    native: catalog.Cursor,
    native_arena: std.heap.ArenaAllocator,
    page: ?catalog.Page = null,
    native_index: usize = 0,
    native_done: bool = false,
    staged: []const catalog.Row,
    staged_index: usize = 0,
    shadowed: std.StringHashMapUnmanaged(void) = .empty,

    fn peek(self: *Overlay) !?catalog.Row {
        while (!self.native_done) {
            if (self.page) |page| {
                while (self.native_index < page.rows.len) {
                    const row = page.rows[self.native_index];
                    if (self.shadowed.contains(row.id)) {
                        self.native_index += 1;
                        continue;
                    }
                    return row;
                }
                const done = page.after == null;
                self.page.?.deinit();
                self.page = null;
                _ = self.native_arena.reset(.retain_capacity);
                if (done) {
                    self.native_done = true;
                    return null;
                }
            }
            self.page = try self.native.next(self.native.ptr, self.native_arena.allocator(), 128);
            self.native_index = 0;
        }
        return null;
    }
    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
        const self: *Overlay = @ptrCast(@alignCast(ptr));
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var rows: std.ArrayList(catalog.Row) = .empty;
        var layout: ?catalog.Row.TypedLayout = null;
        while (rows.items.len < limit) {
            const native = try self.peek();
            const staged: ?catalog.Row = if (self.staged_index < self.staged.len) self.staged[self.staged_index] else null;
            const use_staged = if (staged) |row| native == null or std.mem.lessThan(u8, row.id, native.?.id) else false;
            const row = if (use_staged) staged.? else native orelse break;
            if (row.typed_cells) |cells| if (layout == null) {
                layout = try cells.layout.clone(a);
            };
            try rows.append(a, try row.cloneWithLayout(a, layout));
            if (use_staged) self.staged_index += 1 else self.native_index += 1;
        }
        const more = (try self.peek()) != null or self.staged_index < self.staged.len;
        return .{ .rows = rows.items, .after = if (more and rows.items.len != 0) rows.items[rows.items.len - 1].id else null, .owned_arena = arena };
    }
    fn close(ptr: *anyopaque) void {
        const self: *Overlay = @ptrCast(@alignCast(ptr));
        if (self.page) |*page| page.deinit();
        self.native.close(self.native.ptr);
        self.native_arena.deinit();
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};
fn cloneRow(a: std.mem.Allocator, row: catalog.Row) !catalog.Row {
    return row.cloneOwned(a);
}
fn matches(row: catalog.Row, conditions: []const catalog.Condition) !bool {
    for (conditions) |condition| {
        const cell = try row.cell(condition.column);
        if (condition.op == .is_null) {
            if (!cell.sql_null) return false;
            continue;
        }
        if (condition.op == .is_not_null) {
            if (cell.sql_null) return false;
            continue;
        }
        const operand = condition.datum();
        if (cell.sql_null or operand.sql_null) return false;
        const order = try d.sql_scalar.compareDatums(cell, operand);
        if (!switch (condition.op) {
            .eq => order == .eq,
            .neq => order != .eq,
            .lt => order == .lt,
            .lte => order != .gt,
            .gt => order == .gt,
            .gte => order != .lt,
            else => unreachable,
        }) return false;
    }
    return true;
}
pub fn open(alloc: std.mem.Allocator, native: catalog.Cursor, session: *Session, table: catalog.Table, request: catalog.Scan) !catalog.Cursor {
    const self = try alloc.create(Overlay);
    errdefer alloc.destroy(self);
    self.* = .{ .alloc = alloc, .arena = std.heap.ArenaAllocator.init(alloc), .native = native, .native_arena = std.heap.ArenaAllocator.init(alloc), .staged = &.{} };
    errdefer {
        self.arena.deinit();
        self.native_arena.deinit();
    }
    const a = self.arena.allocator();
    // Normalize only projected and predicate cells; untouched wide arrays
    // remain in the postimage envelope. Native and staged rows then share
    // exact NUMERIC, array and SQL/JSON-null comparison semantics.
    var names: std.ArrayList([]const u8) = .empty;
    try names.appendSlice(a, request.fields);
    for (request.conditions) |condition| try names.append(a, condition.column);
    const full_projection = try d.sql_document_row.Projection.init(a, table, names.items);
    const full_layout = try full_projection.pageLayout(a);
    const selected_projection = try d.sql_document_row.Projection.init(a, table, request.fields);
    const selected_layout = try selected_projection.pageLayout(a);
    const conditions = try a.dupe(catalog.Condition, request.conditions);
    for (conditions) |*condition| {
        if (condition.op == .is_null or condition.op == .is_not_null or condition.datum().sql_null) continue;
        condition.operand = try d.sql_document_row.declaredCell(a, try table.column(condition.column), condition.datum());
    }
    const entries = try session.merged(a);
    var rows: std.ArrayList(catalog.Row) = .empty;
    for (entries) |entry| {
        if (!std.mem.eql(u8, entry.table.physical_name, table.physical_name)) continue;
        if (entry.table.id != table.id or entry.table.schema_version != table.schema_version) return error.PreparedGenerationChanged;
        if (entry.mutation.predicate_only) continue;
        const mutation = entry.mutation;
        try self.shadowed.put(a, try a.dupe(u8, mutation.key), {});
        if (request.primary_key) |key| if (!std.mem.eql(u8, mutation.key, key)) continue;
        if (request.after) |after| if (std.mem.order(u8, mutation.key, after) != .gt) continue;
        const bytes = entry.encoded_row orelse continue;
        const value = try std.json.parseFromSliceLeaky(Json, a, bytes, .{ .parse_numbers = false, .allocate = .alloc_always });
        var object: std.json.ObjectMap = .empty;
        const nulls = try a.alloc(bool, full_projection.columns.len);
        for (full_projection.columns, nulls) |column, *sql_null| {
            const raw = value.object.get(column.path) orelse .null;
            const json_null = if (table.storage_mode == .document) column.type == .json and value.object.contains(column.path) else for (mutation.json_null_fields) |field| {
                if (std.mem.eql(u8, field, column.path)) break true;
            } else false;
            sql_null.* = raw == .null and !json_null;
            try object.put(a, column.path, raw);
        }
        const row = try full_projection.adaptBorrowed(a, full_layout, .{ .id = mutation.key, .value = .{ .object = object }, .version = mutation.expected_version, .sql_nulls = nulls, .expected_content_digest = mutation.expected_content_digest, .document = if (request.include_document) value else null });
        if (!try matches(row, conditions)) continue;
        if (selected_projection.has_typed_cells) {
            var projected = try selected_projection.adaptBorrowed(a, selected_layout, row);
            projected.id = try a.dupe(u8, projected.id);
            try rows.append(a, projected);
            continue;
        }
        var projected: std.json.ObjectMap = .empty;
        const projected_nulls = try a.alloc(bool, request.fields.len);
        for (request.fields, projected_nulls) |field, *sql_null| {
            const cell = try row.cell(field);
            try projected.put(a, field, cell.value);
            sql_null.* = cell.sql_null;
        }
        try rows.append(a, try cloneRow(a, .{ .id = row.id, .version = row.version, .value = .{ .object = projected }, .sql_nulls = projected_nulls, .expected_content_digest = row.expected_content_digest, .document = row.document }));
    }
    std.mem.sort(catalog.Row, rows.items, {}, struct {
        fn less(_: void, arow: catalog.Row, brow: catalog.Row) bool {
            return std.mem.lessThan(u8, arow.id, brow.id);
        }
    }.less);
    self.staged = rows.items;
    return .{ .ptr = self, .next = Overlay.next, .close = Overlay.close };
}
