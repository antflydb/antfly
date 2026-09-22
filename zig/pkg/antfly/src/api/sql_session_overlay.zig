// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded merge of an immutable statement read view and staged primary rows.
//! Owns at most one native page; staged state is borrowed from the serialized
//! statement. Native rows shadowed by any staged write/delete never leak out.
const std = @import("std");
const catalog = @import("../sql/catalog.zig");
const transactions = @import("transactions.zig");
const Json = std.json.Value;

pub fn open(alloc: std.mem.Allocator, native: catalog.Cursor, staged: *const transactions.OwnedTransactionCommitRequest, table: catalog.Table, request: catalog.Scan, row_filter_json: ?[]const u8) !catalog.Cursor {
    const cursor = try alloc.create(Cursor);
    cursor.* = .{ .alloc = alloc, .native = native, .arena = std.heap.ArenaAllocator.init(alloc), .page_arena = std.heap.ArenaAllocator.init(alloc) };
    errdefer {
        cursor.arena.deinit();
        cursor.page_arena.deinit();
        alloc.destroy(cursor);
    }
    const arena = cursor.arena.allocator();
    var row_filter = if (row_filter_json) |filter| try @import("../search/pattern_filter.zig").PreparedPatternFilter.init(alloc, filter) else null;
    defer if (row_filter) |*filter| filter.deinit();
    var rows: std.ArrayList(catalog.Row) = .empty;
    for (staged.tables) |entry| {
        if (!std.mem.eql(u8, staged.physicalName(entry.table_name), table.physical_name)) continue;
        if (entry.relational_schema_version != null and entry.relational_schema_version != table.schema_version) return error.CatalogGenerationChanged;
        for (entry.batch.deletes) |key| try cursor.shadowed.put(arena, key, {});
        for (entry.batch.writes) |write| {
            try cursor.shadowed.put(arena, write.key, {});
            if (request.primary_key) |key| if (!std.mem.eql(u8, key, write.key)) continue;
            if (request.after) |after| if (std.mem.order(u8, write.key, after) != .gt) continue;
            const value = try std.json.parseFromSliceLeaky(Json, arena, write.value, .{ .parse_numbers = false });
            if (value != .object) return error.InvalidSqlBackendResponse;
            if (row_filter) |*filter| if (!try filter.matchesJson(arena, write.key, value)) continue;
            var object: std.json.ObjectMap = .empty;
            const nulls = try arena.alloc(bool, table.columns.len);
            for (table.columns, nulls) |column, *sql_null| {
                const raw = value.object.get(column.path) orelse .null;
                const json_null = for (write.json_null_fields) |field| {
                    if (std.mem.eql(u8, field, column.path)) break true;
                } else false;
                sql_null.* = raw == .null and !json_null;
                const typed = if (raw == .null) raw else try @import("../sql/describe.zig").coerce(raw, column.type);
                try object.put(arena, column.path, typed);
            }
            const version = for (entry.predicates.items) |predicate| {
                if (std.mem.eql(u8, predicate.key, write.key)) break predicate.expected_version;
            } else return error.InvalidSqlBackendResponse;
            const row = catalog.Row{ .id = write.key, .version = version, .value = .{ .object = object }, .sql_nulls = nulls };
            if (try matches(row, request.conditions)) {
                var projected: std.json.ObjectMap = .empty;
                const projected_nulls = try arena.alloc(bool, request.fields.len);
                for (request.fields, projected_nulls) |field, *sql_null| {
                    const cell = try row.cell(field);
                    try projected.put(arena, field, cell.value);
                    sql_null.* = cell.sql_null;
                }
                try rows.append(arena, .{ .id = row.id, .version = row.version, .value = .{ .object = projected }, .sql_nulls = projected_nulls });
            }
        }
    }
    std.mem.sort(catalog.Row, rows.items, {}, struct {
        fn less(_: void, left: catalog.Row, right: catalog.Row) bool {
            return std.mem.lessThan(u8, left.id, right.id);
        }
    }.less);
    cursor.staged = rows.items;
    return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.close };
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
        if (cell.sql_null or condition.value == .null) return false;
        const order = try @import("../sql/scalar.zig").compare(cell.value, condition.value);
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

const Cursor = struct {
    alloc: std.mem.Allocator,
    native: catalog.Cursor,
    arena: std.heap.ArenaAllocator,
    page_arena: std.heap.ArenaAllocator,
    shadowed: std.StringHashMapUnmanaged(void) = .empty,
    staged: []const catalog.Row = &.{},
    staged_index: usize = 0,
    page: ?catalog.Page = null,
    page_index: usize = 0,
    exhausted: bool = false,

    fn peek(self: *Cursor) !?catalog.Row {
        while (true) {
            if (self.page) |page| {
                while (self.page_index < page.rows.len) {
                    const row = page.rows[self.page_index];
                    if (!self.shadowed.contains(row.id)) return row;
                    self.page_index += 1;
                }
                self.exhausted = page.after == null;
                page.deinit();
                self.page = null;
                _ = self.page_arena.reset(.retain_capacity);
            }
            if (self.exhausted) return null;
            self.page = try self.native.next(self.native.ptr, self.page_arena.allocator(), 4096);
            self.page_index = 0;
        }
    }

    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        var rows: std.ArrayList(catalog.Row) = .empty;
        while (rows.items.len < limit) {
            const native = try self.peek();
            const staged: ?catalog.Row = if (self.staged_index < self.staged.len) self.staged[self.staged_index] else null;
            if (native == null and staged == null) break;
            const take_staged = staged != null and (native == null or std.mem.lessThan(u8, staged.?.id, native.?.id));
            const row = if (take_staged) staged.? else native.?;
            try rows.append(alloc, .{ .id = try alloc.dupe(u8, row.id), .version = row.version, .value = try @import("../storage/typed_json.zig").clone(alloc, row.value), .sql_nulls = if (row.sql_nulls) |flags| try alloc.dupe(bool, flags) else null });
            if (take_staged) self.staged_index += 1 else self.page_index += 1;
        }
        const more = self.staged_index < self.staged.len or (try self.peek()) != null;
        return .{ .rows = rows.items, .after = if (more and rows.items.len != 0) rows.items[rows.items.len - 1].id else null };
    }

    fn close(ptr: *anyopaque) void {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        if (self.page) |page| page.deinit();
        self.native.close(self.native.ptr);
        self.page_arena.deinit();
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

test "SQL session overlay merges pages and suppresses replaced and deleted rows" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const Test = struct {
        calls: usize = 0,
        closed: bool = false,
        fn next(ptr: *anyopaque, page_alloc: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls > 1) return .{ .rows = &.{} };
            const rows = try page_alloc.alloc(catalog.Row, 2);
            rows[0] = .{ .id = "a", .version = 1, .value = try std.json.parseFromSliceLeaky(Json, page_alloc, "{\"n\":1}", .{}) };
            rows[1] = .{ .id = "c", .version = 3, .value = try std.json.parseFromSliceLeaky(Json, page_alloc, "{\"n\":3}", .{}) };
            return .{ .rows = rows };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
        }
    };
    var test_native: Test = .{};
    var staged = try transactions.parseCommitRequest(alloc, "{\"read_set\":[{\"table\":\"t\",\"key\":\"a\",\"version\":\"1\"},{\"table\":\"t\",\"key\":\"b\",\"version\":\"0\"}],\"tables\":{\"t\":{\"inserts\":{\"a\":{\"n\":11},\"b\":{\"n\":2}},\"deletes\":[\"c\"]}}}");
    defer staged.deinit(alloc);
    const cursor = try open(alloc, .{ .ptr = &test_native, .next = Test.next, .close = Test.close }, &staged, .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} }, .{ .fields = &.{"n"}, .limit = 1 }, null);
    const first = try cursor.next(cursor.ptr, arena.allocator(), 1);
    try std.testing.expectEqualStrings("a", first.rows[0].id);
    try std.testing.expect(first.after != null);
    const second = try cursor.next(cursor.ptr, arena.allocator(), 1);
    try std.testing.expectEqualStrings("b", second.rows[0].id);
    try std.testing.expect(second.after == null);
    cursor.close(cursor.ptr);
    try std.testing.expect(test_native.closed);
    try std.testing.expectEqual(@as(usize, 1), test_native.calls);
    test_native = .{};
    const filtered = try open(alloc, .{ .ptr = &test_native, .next = Test.next, .close = Test.close }, &staged, .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} }, .{ .fields = &.{"n"}, .limit = 1 }, "{\"doc_id\":{\"ids\":[\"b\"]}}");
    defer filtered.close(filtered.ptr);
    const visible = try filtered.next(filtered.ptr, arena.allocator(), 1);
    try std.testing.expectEqual(@as(usize, 1), visible.rows.len);
    try std.testing.expectEqualStrings("b", visible.rows[0].id);
    try std.testing.expect(visible.after == null);
}

test "SQL session overlay preserves JSON null independently of SQL NULL" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const Empty = struct {
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn close(_: *anyopaque) void {}
    };
    var state: u8 = 0;
    var staged = try transactions.parseCommitRequest(alloc, "{\"read_set\":[{\"table\":\"t\",\"key\":\"a\",\"version\":\"0\"}],\"tables\":{\"t\":{\"inserts\":{\"a\":{\"j\":null,\"n\":null}}}}}");
    defer staged.deinit(alloc);
    const fields = try alloc.alloc([]const u8, 1);
    fields[0] = try alloc.dupe(u8, "j");
    staged.tables[0].batch.writes[0].json_null_fields = fields;
    const cursor = try open(alloc, .{ .ptr = &state, .next = Empty.next, .close = Empty.close }, &staged, .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{ .{ .name = "j", .path = "j", .type = .json }, .{ .name = "n", .path = "n", .type = .json } } }, .{ .fields = &.{ "j", "n" }, .limit = 1 }, null);
    defer cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, arena.allocator(), 1);
    try std.testing.expect(!(try page.rows[0].cell("j")).sql_null);
    try std.testing.expect((try page.rows[0].cell("n")).sql_null);
}
