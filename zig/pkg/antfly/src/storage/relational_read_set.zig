// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! A bounded streaming union of already coordinated immutable owner views.
//! Coordination happens before construction; this module never acquires a
//! mutation fence or reopens an owner between pages.
const std = @import("std");
const View = @import("relational_read_view.zig").View;
const typed_json = @import("typed_json.zig");
const types = @import("db/types.zig");

pub const Set = struct {
    alloc: std.mem.Allocator,
    owners: []Owner,
    heap: []usize,
    heap_len: usize = 0,
    initialized: bool = false,
    failed: bool = false,
    primary_order: bool,
    current: usize = 0,
    fetch_rows: u32,
    fetched_pages: usize = 0,

    const Owner = struct {
        view: View,
        page: ?View.Page = null,
        position: usize = 0,
        done: bool = false,
        last_after: ?[]const u8 = null,
    };

    /// Ownership of views transfers only on success; the input slice remains
    /// owned by the caller. All allocations use the caller's request budget.
    pub fn create(alloc: std.mem.Allocator, views: []const View, primary_order: bool) !View {
        if (views.len == 0 or views.len > 256) return error.SqlProgramLimitExceeded;
        const self = try alloc.create(Set);
        errdefer alloc.destroy(self);
        const owners = try alloc.alloc(Owner, views.len);
        errdefer alloc.free(owners);
        const heap = try alloc.alloc(usize, views.len);
        for (owners, views) |*owner, view| owner.* = .{ .view = view };
        self.* = .{ .alloc = alloc, .owners = owners, .heap = heap, .primary_order = primary_order, .fetch_rows = @intCast(@max(1, 256 / views.len)) };
        return .{ .ptr = self, .vtable = &.{ .next = next, .close = close, .normalize = normalize } };
    }

    fn ensure(self: *Set, index: usize) !bool {
        const owner = &self.owners[index];
        while (true) {
            if (owner.page) |*page| {
                if (owner.position < page.rows.len) return true;
                owner.done = page.after == null;
                const next_after = if (page.after) |after| try self.alloc.dupe(u8, after) else null;
                if (owner.last_after) |after| self.alloc.free(after);
                owner.last_after = next_after;
                page.deinit();
                owner.page = null;
            }
            if (owner.done) return false;
            self.fetched_pages += 1;
            if (self.fetched_pages > 4096) return error.SqlProgramLimitExceeded;
            owner.page = try owner.view.next(self.alloc, self.fetch_rows);
            owner.position = 0;
            if (owner.page.?.rows.len > self.fetch_rows) return error.InvalidSqlBackendResponse;
            // RLS/TTL filtering can produce an empty page with a physical
            // continuation. Keep advancing it, but reject a stalled cursor
            // and charge every native fetch to the statement's page budget.
            if (owner.last_after) |previous| if (owner.page.?.after) |after| if (std.mem.eql(u8, previous, after)) return error.InvalidSqlBackendResponse;
        }
    }

    fn row(self: *const Set, index: usize) View.Row {
        const owner = self.owners[index];
        return owner.page.?.rows[owner.position];
    }

    fn less(self: *const Set, left: usize, right: usize) bool {
        return std.mem.order(u8, self.row(left).id, self.row(right).id) == .lt;
    }

    fn siftDown(self: *Set, initial: usize) void {
        var index = initial;
        while (index * 2 + 1 < self.heap_len) {
            var child = index * 2 + 1;
            if (child + 1 < self.heap_len and self.less(self.heap[child + 1], self.heap[child])) child += 1;
            if (!self.less(self.heap[child], self.heap[index])) break;
            std.mem.swap(usize, &self.heap[child], &self.heap[index]);
            index = child;
        }
    }

    fn initialize(self: *Set) !void {
        if (self.initialized) return;
        self.initialized = true;
        if (!self.primary_order) return;
        for (self.owners, 0..) |_, index| if (try self.ensure(index)) {
            self.heap[self.heap_len] = index;
            self.heap_len += 1;
        };
        var index = self.heap_len / 2;
        while (index != 0) {
            index -= 1;
            self.siftDown(index);
        }
    }

    fn selected(self: *Set) !?usize {
        if (self.primary_order) return if (self.heap_len == 0) null else self.heap[0];
        while (self.current < self.owners.len) : (self.current += 1) {
            if (try self.ensure(self.current)) return self.current;
        }
        return null;
    }

    fn advance(self: *Set, index: usize, prior_key: []const u8) !void {
        self.owners[index].position += 1;
        if (!self.primary_order) return;
        if (try self.ensure(index)) {
            if (std.mem.order(u8, prior_key, self.row(index).id) != .lt) return error.InvalidSqlBackendResponse;
        } else {
            self.heap_len -= 1;
            if (self.heap_len == 0) return;
            self.heap[0] = self.heap[self.heap_len];
        }
        self.siftDown(0);
        // Overlapping owners are a routing integrity error, not duplicate
        // user rows to silently drop. Check across page boundaries as well.
        if (self.heap_len != 0 and std.mem.order(u8, prior_key, self.row(self.heap[0]).id) != .lt) return error.InvalidSqlBackendResponse;
    }

    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !View.Page {
        const self: *Set = @ptrCast(@alignCast(ptr));
        if (self.failed) return error.InvalidSqlCursor;
        if (limit == 0 or limit > 4096) return error.InvalidSqlLimit;
        // An error after consumption poisons the cursor; retrying must never
        // silently skip rows whose previous output page was discarded.
        self.failed = true;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var rows: std.ArrayList(View.Row) = .empty;
        try self.initialize();
        while (rows.items.len < limit) {
            const index = (try self.selected()) orelse break;
            const source = self.row(index);
            const id = try owned.dupe(u8, source.id);
            try rows.append(owned, .{ .id = id, .version = source.version, .schema_version = source.schema_version, .value = try typed_json.clone(owned, source.value), .sql_nulls = if (source.sql_nulls) |flags| try owned.dupe(bool, flags) else null });
            try self.advance(index, id);
        }
        const more = (try self.selected()) != null;
        self.failed = false;
        return .{ .arena = arena, .rows = rows.items, .after = if (more) rows.items[rows.items.len - 1].id else null };
    }

    fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        const self: *Set = @ptrCast(@alignCast(ptr));
        if (self.failed) return error.InvalidSqlCursor;
        // Each owner was opened with the same expected immutable schema.
        return self.owners[0].view.normalize(alloc, writes);
    }

    fn close(ptr: *anyopaque) void {
        const self: *Set = @ptrCast(@alignCast(ptr));
        for (self.owners) |*owner| {
            if (owner.page) |*page| page.deinit();
            if (owner.last_after) |after| self.alloc.free(after);
            owner.view.deinit();
        }
        self.alloc.free(self.owners);
        self.alloc.free(self.heap);
        self.alloc.destroy(self);
    }
};

const Fixture = struct {
    ids: []const []const u8,
    position: usize = 0,
    closed: bool = false,
    fetches: usize = 0,
    empty_pages: u8 = 0,

    fn view(self: *Fixture) View {
        return .{ .ptr = self, .vtable = &.{ .next = next, .close = close } };
    }
    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !View.Page {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        if (self.empty_pages != 0) {
            self.empty_pages -= 1;
            const after = try std.fmt.allocPrint(owned, "filtered-{d}", .{self.empty_pages});
            return .{ .arena = arena, .rows = &.{}, .after = after };
        }
        // Deliberately return one row per page to exercise boundary ordering,
        // refills and source-page release, not just a merge of two arrays.
        const count = @min(@min(limit, 1), self.ids.len - self.position);
        const result = try owned.alloc(View.Row, count);
        for (result, 0..) |*out, offset| out.* = .{ .id = try owned.dupe(u8, self.ids[self.position + offset]), .version = 9, .schema_version = 4, .value = .{ .number_string = try owned.dupe(u8, "9007199254740993") } };
        self.position += count;
        self.fetches += 1;
        return .{ .arena = arena, .rows = result, .after = if (self.position < self.ids.len) result[count - 1].id else null };
    }
    fn close(ptr: *anyopaque) void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        std.debug.assert(!self.closed);
        self.closed = true;
    }
};

fn mergeFixture(alloc: std.mem.Allocator) !void {
    var left: Fixture = .{ .ids = &.{ "a", "c", "e" } };
    var right: Fixture = .{ .ids = &.{ "b", "d", "f" } };
    const merged = try Set.create(alloc, &.{ left.view(), right.view() }, true);
    defer merged.deinit();
    for ([_][]const []const u8{ &.{ "a", "b" }, &.{ "c", "d" }, &.{ "e", "f" } }, 0..) |expected, index| {
        var page = try merged.next(alloc, 2);
        defer page.deinit();
        try std.testing.expectEqual(expected.len, page.rows.len);
        for (page.rows, expected) |actual, id| {
            try std.testing.expectEqualStrings(id, actual.id);
            try std.testing.expectEqualStrings("9007199254740993", actual.value.number_string);
        }
        try std.testing.expectEqual(index != 2, page.after != null);
    }
}

test "relational index system coordinated read set merges bounded exact typed pages" {
    try mergeFixture(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, mergeFixture, .{});
}

test "relational index system coordinated read set rejects overlapping or unordered owners" {
    const alloc = std.testing.allocator;
    var left: Fixture = .{ .ids = &.{ "a", "c" } };
    var right: Fixture = .{ .ids = &.{ "b", "c" } };
    const overlap = try Set.create(alloc, &.{ left.view(), right.view() }, true);
    defer overlap.deinit();
    try std.testing.expectError(error.InvalidSqlBackendResponse, overlap.next(alloc, 10));
    try std.testing.expectError(error.InvalidSqlCursor, overlap.next(alloc, 10));
    var descending: Fixture = .{ .ids = &.{ "b", "a" } };
    const wrong_order = try Set.create(alloc, &.{descending.view()}, true);
    defer wrong_order.deinit();
    try std.testing.expectError(error.InvalidSqlBackendResponse, wrong_order.next(alloc, 10));
}

test "relational index system coordinated unordered scans retain index access without fanout reads" {
    const alloc = std.testing.allocator;
    var first: Fixture = .{ .ids = &.{ "c", "b", "a" } };
    var second: Fixture = .{ .ids = &.{ "d", "e" } };
    const merged = try Set.create(alloc, &.{ first.view(), second.view() }, false);
    defer merged.deinit();
    var page = try merged.next(alloc, 1);
    defer page.deinit();
    try std.testing.expectEqualStrings("c", page.rows[0].id);
    try std.testing.expectEqual(@as(usize, 0), second.fetches);
}

test "relational index system coordinated read set follows empty filtered continuations" {
    const alloc = std.testing.allocator;
    var filtered: Fixture = .{ .ids = &.{ "a", "c" }, .empty_pages = 3 };
    var visible: Fixture = .{ .ids = &.{"b"} };
    const merged = try Set.create(alloc, &.{ filtered.view(), visible.view() }, true);
    defer merged.deinit();
    var page = try merged.next(alloc, 3);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 3), page.rows.len);
    try std.testing.expectEqualStrings("a", page.rows[0].id);
    try std.testing.expectEqualStrings("b", page.rows[1].id);
    try std.testing.expectEqualStrings("c", page.rows[2].id);
    try std.testing.expect(page.after == null);
}
