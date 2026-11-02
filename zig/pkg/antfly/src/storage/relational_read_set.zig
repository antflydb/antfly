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

//! A bounded streaming union of already coordinated immutable owner views.
//! Coordination happens before construction; this module never acquires a
//! mutation fence or reopens an owner between pages.
const std = @import("std");
const View = @import("antfly_local_sources").storage_relational_read_view.View;
const types = @import("antfly_local_sources").storage_db_types;
const Admission = View.Admission;

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
        return .{ .ptr = self, .vtable = &.{ .next = next, .next_with_admission = nextWithAdmission, .close = close, .normalize = normalize, .normalize_with_admission = normalizeWithAdmission } };
    }

    fn ensure(self: *Set, index: usize, control: ?Admission) !bool {
        const owner = &self.owners[index];
        while (true) {
            if (control) |port| try port.checkpoint();
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
            owner.page = if (control) |port| try owner.view.nextWithAdmission(self.alloc, self.fetch_rows, port) else try owner.view.next(self.alloc, self.fetch_rows);
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

    fn initialize(self: *Set, control: ?Admission) !void {
        if (self.initialized) return;
        self.initialized = true;
        if (!self.primary_order) return;
        for (self.owners, 0..) |_, index| if (try self.ensure(index, control)) {
            self.heap[self.heap_len] = index;
            self.heap_len += 1;
        };
        var index = self.heap_len / 2;
        while (index != 0) {
            index -= 1;
            self.siftDown(index);
        }
    }

    fn selected(self: *Set, control: ?Admission) !?usize {
        if (self.primary_order) return if (self.heap_len == 0) null else self.heap[0];
        while (self.current < self.owners.len) : (self.current += 1) {
            if (try self.ensure(self.current, control)) return self.current;
        }
        return null;
    }

    fn advance(self: *Set, index: usize, prior_key: []const u8, control: ?Admission) !void {
        self.owners[index].position += 1;
        if (!self.primary_order) return;
        if (try self.ensure(index, control)) {
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
        return nextControlled(ptr, alloc, limit, null);
    }

    fn nextWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32, control: Admission) !View.Page {
        return nextControlled(ptr, alloc, limit, control);
    }

    // The admission borrow belongs to this page call, never to the set or an
    // owner. Pass it through every refill so remote credit waits can park the
    // caller's CPU and retain its original cancellation and read authority.
    fn nextControlled(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32, control: ?Admission) !View.Page {
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
        var retained: std.ArrayList(View.Page) = .empty;
        errdefer for (retained.items) |*page| page.deinit();
        var retained_leases: std.AutoHashMapUnmanaged(usize, void) = .empty;
        try self.initialize(control);
        while (rows.items.len < limit) {
            if (control) |port| try port.checkpoint();
            const index = (try self.selected(control)) orelse break;
            const source = self.row(index);
            // Pin a source page once per delivery page. Rows, column blocks,
            // directories and large arrays remain borrowed instead of cloned.
            const page = &self.owners[index].page.?;
            const lease_id = if (page.lease) |lease| @intFromPtr(lease) else 0;
            if (lease_id == 0 or !retained_leases.contains(lease_id)) {
                try retained.ensureUnusedCapacity(owned, 1);
                try retained_leases.ensureUnusedCapacity(owned, 1);
                const held = try page.retain(self.alloc);
                retained.appendAssumeCapacity(held);
                retained_leases.putAssumeCapacity(@intFromPtr(held.lease.?), {});
            }
            try rows.append(owned, source);
            const id = source.id;
            try self.advance(index, id, control);
        }
        const more = (try self.selected(control)) != null;
        self.failed = false;
        return .{ .arena = arena, .rows = rows.items, .retained_pages = retained.items, .after = if (more) rows.items[rows.items.len - 1].id else null };
    }

    fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        return normalizeControlled(ptr, alloc, writes, null);
    }

    fn normalizeWithAdmission(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite, control: Admission) ![]types.BatchWrite {
        return normalizeControlled(ptr, alloc, writes, control);
    }

    fn normalizeControlled(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite, control: ?Admission) ![]types.BatchWrite {
        const self: *Set = @ptrCast(@alignCast(ptr));
        if (self.failed) return error.InvalidSqlCursor;
        // Each owner was opened with the same expected immutable schema.
        return if (control) |port| self.owners[0].view.normalizeWithAdmission(alloc, writes, port) else self.owners[0].view.normalize(alloc, writes);
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
    digest: ?[32]u8 = null,
    document: ?std.json.Value = null,
    position: usize = 0,
    closed: bool = false,
    fetches: usize = 0,
    empty_pages: u8 = 0,
    native_array: bool = false,

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
        for (result, 0..) |*out, offset| out.* = .{ .id = try owned.dupe(u8, self.ids[self.position + offset]), .version = 9, .schema_version = 4, .value = .{ .number_string = try owned.dupe(u8, "9007199254740993") }, .expected_content_digest = self.digest, .document = self.document };
        if (self.native_array) for (result) |*out| {
            const src = @import("antfly_local_sources");
            const names = try owned.alloc([]const u8, 2);
            names[0] = try owned.dupe(u8, "a");
            names[1] = try owned.dupe(u8, "missing");
            const elements = try owned.alloc(src.sql_catalog.Row.Cell, 1);
            elements[0] = .json(.{ .string = try owned.dupe(u8, out.id) });
            const array = try owned.create(src.sql_array_value.Value);
            array.* = try src.sql_array_value.Value.init(.text, &.{.{ .length = 1, .lower = 0 }}, elements, .{});
            const values = try owned.alloc(src.sql_catalog.Row.Cell, 2);
            values[0] = .typedArray(array);
            values[1] = .{};
            out.value = .null;
            out.typed_cells = .{ .layout = try src.sql_catalog.Row.TypedLayout.init(owned, names), .values = values, .presence = &.{ true, false } };
        };
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

test "relational index system coordinated read set retains mutation digest and document preimage across page release" {
    var source: Fixture = .{ .ids = &.{"row"}, .digest = @splat(9), .document = .{ .string = "before" } };
    const merged = try Set.create(std.testing.allocator, &.{source.view()}, false);
    defer merged.deinit();
    var page = try merged.next(std.testing.allocator, 1);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqual(@as(?[32]u8, @splat(9)), page.rows[0].expected_content_digest);
    try std.testing.expectEqualStrings("before", page.rows[0].document.?.string);
}

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

fn nativeMergeFixture(alloc: std.mem.Allocator) !void {
    var left: Fixture = .{ .ids = &.{ "a", "c" }, .native_array = true };
    var right: Fixture = .{ .ids = &.{ "b", "d" }, .native_array = true };
    const merged = try Set.create(alloc, &.{ left.view(), right.view() }, true);
    var live = true;
    defer if (live) merged.deinit();
    var first = try merged.next(alloc, 2);
    defer first.deinit();
    var second = try merged.next(alloc, 2);
    defer second.deinit();
    merged.deinit();
    live = false;
    try std.testing.expect(left.closed and right.closed);
    for ([_]View.Page{ first, second }) |page| {
        try std.testing.expectEqual(@as(usize, 2), page.rows.len);
        try std.testing.expect(page.retained_pages.len != 0);
        // The output pins source payloads; it does not deep-copy array cells.
        try std.testing.expect(page.rows[0].typed_cells.?.values.ptr == page.retained_pages[0].rows[0].typed_cells.?.values.ptr);
        for (page.rows) |out| {
            const row = out.sqlRow();
            try std.testing.expect(!try row.hasField("missing"));
            const array = (try row.cell("a")).array.?;
            try std.testing.expectEqual(@as(i32, 0), array.dimensions[0].lower);
            try std.testing.expectEqualStrings(out.id, array.elements[0].value.string);
        }
    }
}

test "relational index system coordinated read set owns native arrays after source pages and cursors close" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, nativeMergeFixture, .{});
}

test "relational index system coordinated read set forwards admission through refills and normalization" {
    const admission = @import("antfly_local_sources").common_read_admission;
    const Scope = struct {
        canceled: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
    };
    const AdmittedOwner = struct {
        source: Fixture,
        expected: Admission,
        scope: *Scope,
        cancel_after_fetch: bool,
        admitted_fetches: usize = 0,
        normalized: bool = false,
        fn view(self: *@This()) View {
            return .{ .ptr = self, .vtable = &.{ .next = rejectPlain, .next_with_admission = fetch, .normalize_with_admission = normalizeWrites, .close = release } };
        }
        fn rejectPlain(_: *anyopaque, _: std.mem.Allocator, _: u32) !View.Page {
            return error.ExecutionWaitUnsupported;
        }
        fn fetch(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32, control: Admission) !View.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.expected.payload, control.payload);
            try control.checkpoint();
            self.admitted_fetches += 1;
            const page = try Fixture.next(&self.source, alloc, limit);
            if (self.cancel_after_fetch) self.scope.canceled = true;
            return page;
        }
        fn normalizeWrites(raw: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite, control: Admission) ![]types.BatchWrite {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.expected.payload, control.payload);
            try control.checkpoint();
            self.normalized = true;
            return alloc.dupe(types.BatchWrite, writes);
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            Fixture.close(&self.source);
        }
    };
    for ([_]bool{ false, true }) |ordered| {
        for ([_]bool{ false, true }) |cancel_after_fetch| {
            var scope: Scope = .{};
            const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
            const port = Admission.init(&payload);
            var owner: AdmittedOwner = .{ .source = .{ .ids = &.{ "a", "b", "c" }, .empty_pages = 1 }, .expected = port, .scope = &scope, .cancel_after_fetch = cancel_after_fetch };
            const merged = try Set.create(std.testing.allocator, &.{owner.view()}, ordered);
            {
                defer merged.deinit();
                const writes = try merged.normalizeWithAdmission(std.testing.allocator, &.{}, port);
                defer std.testing.allocator.free(writes);
                try std.testing.expect(owner.normalized);
                if (cancel_after_fetch) {
                    try std.testing.expectError(error.Canceled, merged.nextWithAdmission(std.testing.allocator, 2, port));
                    try std.testing.expectEqual(@as(usize, 1), owner.admitted_fetches);
                    scope.canceled = false;
                    try std.testing.expectError(error.InvalidSqlCursor, merged.nextWithAdmission(std.testing.allocator, 2, port));
                } else {
                    var page = try merged.nextWithAdmission(std.testing.allocator, 2, port);
                    defer page.deinit();
                    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
                    try std.testing.expectEqualStrings("a", page.rows[0].id);
                    try std.testing.expectEqualStrings("b", page.rows[1].id);
                    try std.testing.expect(owner.admitted_fetches >= 3);
                }
            }
            try std.testing.expect(owner.source.closed);
        }
    }
}
