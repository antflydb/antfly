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
        var layout: ?*const catalog.Row.TypedLayout = null;
        while (rows.items.len < limit) {
            const native = try self.peek();
            const staged: ?catalog.Row = if (self.staged_index < self.staged.len) self.staged[self.staged_index] else null;
            const use_staged = if (staged) |row| native == null or std.mem.lessThan(u8, row.id, native.?.id) else false;
            const row = if (use_staged) staged.? else native orelse break;
            // peek() can release the native page before this output page is
            // returned. Transfer complete cells, sharing one owned directory
            // across the emitted page rather than cloning it for every row.
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
        if (cell.sql_null or condition.value == .null) return false;
        const order = try d.sql_scalar.compare(cell.value, condition.value);
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
        const nulls = try a.alloc(bool, table.columns.len);
        for (table.columns, nulls) |column, *sql_null| {
            const raw = value.object.get(column.path) orelse .null;
            const json_null = if (table.storage_mode == .document) column.type == .json and value.object.contains(column.path) else for (mutation.json_null_fields) |field| {
                if (std.mem.eql(u8, field, column.path)) break true;
            } else false;
            sql_null.* = raw == .null and !json_null;
            try object.put(a, column.path, raw);
        }
        const row: catalog.Row = .{ .id = mutation.key, .value = .{ .object = object }, .version = mutation.expected_version, .sql_nulls = nulls, .expected_content_digest = mutation.expected_content_digest, .document = if (request.include_document) value else null };
        if (!try matches(row, request.conditions)) continue;
        var projected: std.json.ObjectMap = .empty;
        const projected_nulls = try a.alloc(bool, request.fields.len);
        for (request.fields, projected_nulls) |field, *sql_null| {
            const cell = try row.cell(field);
            try projected.put(a, field, cell.value);
            sql_null.* = cell.sql_null;
        }
        try rows.append(a, try (catalog.Row{ .id = row.id, .version = row.version, .value = .{ .object = projected }, .sql_nulls = projected_nulls, .expected_content_digest = row.expected_content_digest, .document = row.document }).cloneOwned(a));
    }
    std.mem.sort(catalog.Row, rows.items, {}, struct {
        fn less(_: void, arow: catalog.Row, brow: catalog.Row) bool {
            return std.mem.lessThan(u8, arow.id, brow.id);
        }
    }.less);
    self.staged = rows.items;
    return .{ .ptr = self, .next = Overlay.next, .close = Overlay.close };
}

test "SQL overlay native pages retain typed cells after source page release" {
    const a = std.testing.allocator;
    const Fixture = struct {
        sent: bool = false,
        fn next(raw: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.sent) return .{ .rows = &.{} };
            self.sent = true;
            // Independently owned native pages really free their payload when
            // peek() reaches EOF, even if the caller retains its own arena.
            var arena = std.heap.ArenaAllocator.init(a);
            errdefer arena.deinit();
            const owned = arena.allocator();
            const layout = try catalog.Row.TypedLayout.init(owned, &.{ "n", "a", "j", "sql_null" });
            const array = try @import("../sql/array_text.zig").decode(owned, .int64, "[-1:1]={9007199254740993,NULL,2}", .{});
            const rows = try owned.alloc(catalog.Row, 2);
            for (rows, 0..) |*row, i| {
                row.* = try catalog.Row.fromDatums(owned, if (i == 0) "a" else "b", layout, &.{
                    catalog.Row.Cell.json(.{ .integer = 9007199254740993 + @as(i64, @intCast(i)) }),
                    catalog.Row.Cell.typedArray(&array.value),
                    catalog.Row.Cell.json(.null),
                    .{},
                });
                row.version = 7;
            }
            return .{ .rows = rows, .owned_arena = arena };
        }
        fn close(_: *anyopaque) void {}
    };
    var fixture: Fixture = .{};
    var overlay: Overlay = .{
        .alloc = a,
        .arena = .init(a),
        .native = .{ .ptr = &fixture, .next = Fixture.next, .close = Fixture.close },
        .native_arena = .init(a),
        .staged = &.{},
    };
    defer {
        if (overlay.page) |*page| page.deinit();
        overlay.native_arena.deinit();
        overlay.arena.deinit();
    }
    var page = try Overlay.next(&overlay, a, 2);
    defer page.deinit();
    try std.testing.expect(overlay.native_done and overlay.page == null);
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expect(page.rows[0].typed_cells.?.layout == page.rows[1].typed_cells.?.layout);
    var expected = try @import("../sql/array_text.zig").decode(a, .int64, "[-1:1]={9007199254740993,NULL,2}", .{});
    defer expected.deinit();
    for (page.rows, 0..) |row, i| {
        try std.testing.expectEqualStrings(if (i == 0) "a" else "b", row.id);
        try std.testing.expectEqual(@as(u64, 7), row.version);
        try std.testing.expectEqual(9007199254740993 + @as(i64, @intCast(i)), (try row.cell("n")).value.integer);
        const cell = try row.cell("a");
        try std.testing.expect(!cell.sql_null and cell.value == .null);
        var work: @import("../sql/array_value.zig").Budget = .{};
        try std.testing.expectEqual(std.math.Order.eq, try expected.value.compare((cell.array orelse return error.ExpectedNativeArrayCell).*, &work));
        const json_null = try row.cell("j");
        try std.testing.expect(!json_null.sql_null and json_null.value == .null);
        try std.testing.expect((try row.cell("sql_null")).sql_null);
    }
}
