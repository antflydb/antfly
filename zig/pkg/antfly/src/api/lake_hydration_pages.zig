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

//! Bounded selected-page gather. Every lane owns its cursor and scratch;
//! workers join before the caller allocates or mutates result vectors.
const std = @import("std");
const local = @import("antfly_local_sources");
const sql = local.sql_catalog;
const scheduling = local.sql_parallel_scheduler;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
const AllocatorOwner = struct {
    locked: scheduling.LockedAllocator,
    refs: std.atomic.Value(usize) = .init(1),
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.refs.fetchSub(1, .acq_rel) == 1) std.heap.page_allocator.destroy(self);
    }
};
pub const Pages = struct {
    a: A,
    owner: *AllocatorOwner,
    context: Context,
    lanes: ?[]sql.Cursor = null,
    work: [4]Work = undefined,
    count: usize = 0,
    position: usize = 0,
    ready: bool = false,
    const Work = struct {
        cursor: sql.Cursor,
        arena: std.heap.ArenaAllocator,
        context: Context,
        done: bool = false,
        page: ?sql.ColumnPage = null,
        task: ?scheduling.Task(anyerror!sql.ColumnPage) = null,
        fn run(self: *@This()) anyerror!sql.ColumnPage {
            try self.context.ensureActive();
            const page = try self.cursor.next_columns.?(self.cursor.ptr, self.arena.allocator(), 4096);
            try self.context.ensureActive();
            return page;
        }
    };
    pub fn init(a: A, parent: sql.Cursor, context: Context, parallel: bool) !*Pages {
        try context.ensureActive();
        const owner = try std.heap.page_allocator.create(AllocatorOwner);
        owner.* = .{ .locked = .{ .backing = a } };
        errdefer AllocatorOwner.release(owner);
        const self = try a.create(Pages);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .owner = owner, .context = context };
        if (parallel and context.io != null) if (parent.split_ordered) |split| {
            self.lanes = try split(parent.ptr, owner.locked.allocator(), self.work.len);
        };
        if (self.lanes) |lanes| {
            std.debug.assert(lanes.len <= self.work.len);
            for (lanes, 0..) |cursor, i| self.work[i] = .{ .cursor = cursor, .arena = .init(owner.locked.allocator()), .context = context };
            self.count = lanes.len;
        } else {
            self.work[0] = .{ .cursor = parent, .arena = .init(a), .context = context };
            self.count = 1;
        }
        return self;
    }
    fn drain(self: *Pages) void {
        const io = self.context.io orelse return;
        for (self.work[0..self.count]) |*work| if (work.task) |*task| {
            _ = task.cancel(io) catch {};
            work.task = null;
        };
    }
    pub fn deinit(self: *Pages) void {
        self.drain();
        if (self.lanes) |lanes| {
            for (lanes) |cursor| cursor.close(cursor.ptr);
            self.owner.locked.allocator().free(lanes);
        }
        for (self.work[0..self.count]) |*work| work.arena.deinit();
        const owner = self.owner;
        const a = self.a;
        a.destroy(self);
        AllocatorOwner.release(owner);
    }
    /// Retained native columns can outlive this gatherer and its child cursors.
    /// Release the allocator only after all returned column owners have drained.
    pub fn retainAllocator(self: *Pages, page: *local.storage_db_column_source.Page) !void {
        if (self.lanes == null) return;
        _ = self.owner.refs.fetchAdd(1, .monotonic);
        try page.adoptNativeLease(.{ .ptr = self.owner, .release_fn = AllocatorOwner.release, .retained_bytes = @sizeOf(AllocatorOwner) });
    }
    pub fn next(self: *Pages) !?sql.ColumnPage {
        try self.context.ensureActive();
        while (true) {
            if (self.ready) {
                while (self.position < self.count) {
                    const index = self.position;
                    self.position += 1;
                    if (self.work[index].page) |page| {
                        self.work[index].page = null;
                        return page;
                    }
                }
                self.ready = false;
            }
            var active = false;
            errdefer self.drain();
            // Submit all admitted lanes before any inline fallback or wait.
            for (self.work[0..self.count]) |*work| {
                if (work.done) continue;
                active = true;
                _ = work.arena.reset(.retain_capacity);
                if (self.lanes != null) if (self.context.io) |io| {
                    const reservation: usize = @intCast(@min(16 * 1024 * 1024, @max(1024 * 1024, work.cursor.estimated_bytes orelse 8 * 1024 * 1024)));
                    work.task = scheduling.global().submitIoTransient(io, reservation, Work.run, .{work});
                    if (self.context.trace) |trace| {
                        if (work.task != null) trace.add("hydration_lane_submitted", 1) else trace.add("hydration_lane_declined", 1);
                    }
                };
            }
            if (!active) return null;
            for (self.work[0..self.count]) |*work| {
                if (work.done) continue;
                const page = if (work.task) |*task| block: {
                    const outcome = task.await(self.context.io.?);
                    work.task = null;
                    break :block try outcome;
                } else try work.run();
                try page.validate();
                work.done = page.after == null;
                work.page = page;
            }
            self.position = 0;
            self.ready = true;
        }
    }
};

test "external lake bounded window profiles selected hydration overlaps joins failures and retains allocator leases" {
    const a = std.testing.allocator;
    const Gate = struct {
        entered: std.atomic.Value(usize) = .init(0),
        closed: std.atomic.Value(usize) = .init(0),
        overlap: bool,
        fail: bool,
        fn nextRows(_: *anyopaque, _: A, _: u32) !sql.Page {
            return error.UnsupportedSqlExecution;
        }
        fn page(raw: *anyopaque, alloc: A, _: u32) !sql.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const index = self.entered.fetchAdd(1, .acq_rel);
            const bytes = try alloc.alloc(u8, 32);
            @memset(bytes, 0);
            if (self.overlap) {
                const limit = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
                while (self.entered.load(.acquire) < 2) {
                    if (@import("antfly_platform").time.monotonicNs() > limit) return error.TestNoOverlap;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                }
            }
            if (self.fail and index == 0) return error.TestReadFailure;
            return .{ .batch = .{ .snapshot = .{ .table_id = "table", .snapshot_id = "snapshot" }, .row_refs = &.{}, .columns = &.{} }, .selection = &.{} };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.closed.fetchAdd(1, .monotonic);
        }
        fn cursor(self: *@This()) sql.Cursor {
            return .{ .ptr = self, .next = nextRows, .next_columns = page, .close = close, .split_ordered = split };
        }
        fn split(raw: *anyopaque, alloc: A, maximum: usize) !?[]sql.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (maximum < 2) return null;
            const result = try alloc.alloc(sql.Cursor, 2);
            for (result) |*out| out.* = self.cursor();
            return result;
        }
    };
    for ([_]bool{ false, true }) |denied| for ([_]bool{ false, true }) |fail| {
        const scheduler = scheduling.global();
        const previous = scheduler.max_io_bytes;
        if (denied) scheduler.max_io_bytes = 0;
        defer scheduler.max_io_bytes = previous;
        var gate: Gate = .{ .overlap = !denied, .fail = fail };
        const pages = try Pages.init(a, gate.cursor(), .{ .io = std.testing.io }, true);
        var page_count: usize = 0;
        if (fail) try std.testing.expectError(error.TestReadFailure, pages.next()) else {
            while (try pages.next()) |_| page_count += 1;
            try std.testing.expectEqual(@as(usize, 2), page_count);
        }
        const retained = try local.storage_db_column_source.Page.native(a, &.{}, 0);
        defer retained.release();
        try pages.retainAllocator(retained);
        const owner = pages.owner;
        try std.testing.expectEqual(@as(usize, 2), owner.refs.load(.acquire));
        pages.deinit();
        try std.testing.expectEqual(@as(usize, 1), owner.refs.load(.acquire));
        const still_owned = try owner.locked.allocator().alloc(u8, 8);
        owner.locked.allocator().free(still_owned);
        try std.testing.expectEqual(@as(usize, 2), gate.closed.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
        try std.testing.expectEqual(@as(usize, 0), scheduler.io_bytes);
    };
}
