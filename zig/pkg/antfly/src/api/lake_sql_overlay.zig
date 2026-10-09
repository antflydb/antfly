// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Typed accepted-WAL visibility. Resolve keys before predicates; the child
//! scan must include stable key columns and must not apply early limits.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ingestion = @import("../serverless/lake_ingestion.zig");
const A = std.mem.Allocator;
const V = std.json.Value;

pub const Cut = struct {
    pending: ingestion.Pending,
    changed: std.StringHashMapUnmanaged(void) = .empty,
    pub fn init(a: A, pending: ingestion.Pending) !Cut {
        var result: Cut = .{ .pending = pending };
        for (pending.changes) |change| try result.changed.put(a, try identity(a, pending.key_fields, change.row), {});
        return result;
    }
};
fn identity(a: A, keys: []const []const u8, row: V) ![]const u8 {
    if (row != .object) return error.InvalidWal;
    const values = try a.alloc(V, keys.len);
    defer a.free(values);
    for (keys, values) |key, *value| value.* = row.object.get(key) orelse return error.InvalidWal;
    return std.json.Stringify.valueAlloc(a, values, .{});
}
pub fn physicalRequest(a: A, request: catalog.Scan, cut: Cut) !catalog.Scan {
    // Ordered/physical selections cannot be translated to the new logical cut.
    if (request.index_range != null or request.primary_key != null or request.row_refs != null or request.after != null or request.before != null) return error.UnsupportedSqlExecution;
    var result = request;
    var fields: std.ArrayList([]const u8) = .empty;
    try fields.appendSlice(a, request.fields);
    var required: std.ArrayList([]const u8) = .empty;
    try required.appendSlice(a, cut.pending.key_fields);
    for (request.conditions) |condition| try required.append(a, condition.column);
    for (required.items) |key| {
        const present = for (fields.items) |field| {
            if (std.mem.eql(u8, field, key)) break true;
        } else false;
        if (!present) try fields.append(a, key);
    }
    result.fields = fields.items;
    // The complete changed-key set is already pinned, so base predicate
    // pushdown commutes with suppression; recent rows are filtered separately.
    result.order = &.{};
    result.primary_order = false;
    result.row_goal = null;
    result.index_equality = null;
    return result;
}
pub fn wrap(a: A, child: catalog.Cursor, table: catalog.Table, request: catalog.Scan, cut: *const Cut, context: local.api_operation.RequestContext) !catalog.Cursor {
    const owner = try a.create(Cursor);
    owner.* = .{ .a = a, .child = child, .table = table, .request = request, .cut = cut, .context = context };
    return .{ .ptr = owner, .next = Cursor.next, .close = Cursor.close };
}
const Cursor = struct {
    a: A,
    child: catalog.Cursor,
    table: catalog.Table,
    request: catalog.Scan,
    cut: *const Cut,
    context: local.api_operation.RequestContext,
    base_done: bool = false,
    change_at: usize = 0,
    page_sequence: u64 = 0,
    fn next(raw: *anyopaque, a: A, maximum: u32) !catalog.Page {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (maximum == 0) return error.InvalidSqlBackendResponse;
        try self.context.ensureActive();
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        var rows: std.ArrayList(catalog.Row) = .empty;
        // Consume one bounded child page. Short filtered pages retain a
        // continuation; they are never mistaken for exhaustion.
        if (!self.base_done) {
            const page = try self.child.next(self.child.ptr, a, maximum);
            defer page.deinit();
            self.base_done = page.after == null;
            for (page.rows) |row| {
                try self.context.ensureActive();
                const key = try identity(scratch, self.cut.pending.key_fields, row.value);
                if (self.cut.changed.contains(key)) continue;
                if (!try matchesTyped(scratch, self.table, row, self.request.conditions)) continue;
                try rows.append(scratch, try project(scratch, self.table, row, self.request.fields));
            }
        }
        if (self.base_done) while (self.change_at < self.cut.pending.changes.len and rows.items.len < maximum) {
            try self.context.ensureActive();
            const change = self.cut.pending.changes[self.change_at];
            self.change_at += 1;
            if (change.op == .delete) continue;
            const key = try identity(scratch, self.cut.pending.key_fields, change.row);
            const row: catalog.Row = .{ .id = try std.fmt.allocPrint(scratch, "wal1:{s}", .{local.serverless_external_source_mod.lake_catalog.types.digestHex(key)}), .version = self.cut.pending.lsn, .value = change.row };
            if (try matchesTyped(scratch, self.table, row, self.request.conditions)) try rows.append(scratch, try project(scratch, self.table, row, self.request.fields));
        };
        try self.context.ensureActive();
        self.page_sequence = try std.math.add(u64, self.page_sequence, 1);
        return .{ .rows = rows.items, .owned_arena = arena, .after = if (self.base_done and self.change_at == self.cut.pending.changes.len) null else try std.fmt.allocPrint(scratch, "accepted-wal-{d}", .{self.page_sequence}) };
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.child.close(self.child.ptr);
        self.a.destroy(self);
    }
};
fn project(a: A, table: catalog.Table, row: catalog.Row, fields: []const []const u8) !catalog.Row {
    var value: V = .{ .object = .empty };
    for (fields) |field| try value.object.put(a, field, try local.sql_describe.coerceAlloc(a, (try row.cell(field)).value, (try table.column(field)).type));
    return .{ .id = try a.dupe(u8, row.id), .version = row.version, .value = value };
}
fn matchesTyped(a: A, table: catalog.Table, row: catalog.Row, conditions: []const catalog.Condition) !bool {
    var normalized: V = .{ .object = .empty };
    const copied = try a.dupe(catalog.Condition, conditions);
    for (copied) |*condition| {
        const kind = (try table.column(condition.column)).type;
        try normalized.object.put(a, condition.column, try local.sql_lake_values.comparisonValue(a, (try row.cell(condition.column)).value, kind));
        condition.value = try local.sql_lake_values.comparisonValue(a, condition.value, kind);
    }
    return matches(.{ .id = row.id, .version = row.version, .value = normalized }, copied);
}
fn matches(row: catalog.Row, conditions: []const catalog.Condition) !bool {
    for (conditions) |condition| {
        const cell = try row.cell(condition.column);
        const match = switch (condition.op) {
            .is_null => cell.sql_null,
            .is_not_null => !cell.sql_null,
            else => blk: {
                if (cell.sql_null or condition.value == .null) break :blk false;
                const order = try local.sql_scalar.compare(cell.value, condition.value);
                break :blk switch (condition.op) {
                    .eq => order == .eq,
                    .neq => order != .eq,
                    .lt => order == .lt,
                    .lte => order != .gt,
                    .gt => order == .gt,
                    .gte => order != .lt,
                    else => unreachable,
                };
            },
        };
        if (!match) return false;
    }
    return true;
}

test "lake SQL typed WAL visibility suppresses replaced matches before filtering and retains delete keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const old = try std.json.parseFromSliceLeaky(V, a, "{\"id\":1,\"amount\":10}", .{});
    const newer = try std.json.parseFromSliceLeaky(V, a, "{\"id\":1,\"amount\":20}", .{});
    const deleted = try std.json.parseFromSliceLeaky(V, a, "{\"id\":2}", .{});
    const cut = try Cut.init(a, .{ .lsn = 3, .key_fields = &.{"id"}, .changes = &.{ .{ .op = .upsert, .row = newer }, .{ .op = .delete, .row = deleted } } });
    try std.testing.expect(cut.changed.contains(try identity(a, &.{"id"}, old)));
    try std.testing.expect(cut.changed.contains(try identity(a, &.{"id"}, deleted)));
    const condition = [_]catalog.Condition{.{ .column = "amount", .op = .eq, .value = .{ .integer = 10 } }};
    try std.testing.expect(try matches(.{ .id = "old", .version = 0, .value = old }, &condition));
    try std.testing.expect(!try matches(.{ .id = "new", .version = 3, .value = newer }, &condition));
    const physical = try physicalRequest(a, .{ .fields = &.{"amount"}, .conditions = &condition, .limit = 20, .order = &.{.{ .column = "amount" }} }, cut);
    try std.testing.expectEqual(@as(usize, 2), physical.fields.len);
    try std.testing.expectEqual(@as(usize, 1), physical.conditions.len);
    try std.testing.expectEqual(@as(usize, 0), physical.order.len);
}

test "lake SQL overlay cursor keeps filtered short pages alive and applies final upserts once" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const data = try std.json.parseFromSliceLeaky(V, scratch, "[{\"id\":1,\"amount\":10},{\"id\":2,\"amount\":10},{\"id\":3,\"amount\":10}]", .{});
    const Fixture = struct {
        rows: []const V,
        at: usize = 0,
        fn next(raw: *anyopaque, alloc: A, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var owned = std.heap.ArenaAllocator.init(alloc);
            const oa = owned.allocator();
            const rows = try oa.alloc(catalog.Row, 1);
            rows[0] = .{ .id = try std.fmt.allocPrint(oa, "base-{d}", .{self.at + 1}), .version = 0, .value = self.rows[self.at] };
            self.at += 1;
            return .{ .rows = rows, .owned_arena = owned, .after = if (self.at == self.rows.len) null else "child-more" };
        }
        fn close(_: *anyopaque) void {}
    };
    var fixture: Fixture = .{ .rows = data.array.items };
    const changes = try std.json.parseFromSliceLeaky(V, scratch, "[{\"id\":1,\"amount\":20},{\"id\":2},{\"id\":4,\"amount\":10}]", .{});
    const cut = try Cut.init(scratch, .{ .lsn = 3, .key_fields = &.{"id"}, .changes = &.{ .{ .op = .upsert, .row = changes.array.items[0] }, .{ .op = .delete, .row = changes.array.items[1] }, .{ .op = .upsert, .row = changes.array.items[2] } } });
    const table: catalog.Table = .{ .id = 1, .physical_name = "hn", .schema_version = 1, .columns = &.{.{ .name = "amount", .path = "amount", .type = .integer }} };
    const cursor = try wrap(a, .{ .ptr = &fixture, .next = Fixture.next, .close = Fixture.close }, table, .{ .fields = &.{"amount"}, .conditions = &.{.{ .column = "amount", .op = .eq, .value = .{ .integer = 10 } }}, .limit = 1 }, &cut, .{});
    defer cursor.close(cursor.ptr);
    var total: usize = 0;
    var pages: usize = 0;
    var previous: ?[]u8 = null;
    defer if (previous) |token| a.free(token);
    while (true) {
        const page = try cursor.next(cursor.ptr, a, 1);
        defer page.deinit();
        total += page.rows.len;
        pages += 1;
        if (pages == 1) {
            try std.testing.expectEqual(@as(usize, 0), page.rows.len);
            try std.testing.expect(page.after != null);
        }
        if (page.after == null) break;
        if (previous) |token| {
            try std.testing.expect(!std.mem.eql(u8, token, page.after.?));
            a.free(token);
            previous = null;
        }
        previous = try a.dupe(u8, page.after.?);
        if (pages > 10) return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, 2), total);
}
