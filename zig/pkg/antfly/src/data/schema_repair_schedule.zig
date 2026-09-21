// Copyright 2026 Antfly, Inc.
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

//! Reconstructible node-local scheduling over durable per-index repair intents.
//! Only the maintenance owner mutates this queue. Completion is a cached proof
//! for an exact table/schema/root/ownership identity, never catalog authority.
const std = @import("std");

pub const Identity = struct {
    table_id: u64,
    schema_hash: u64,
    root_generation: u64,
    ownership_generation: u64,
};
pub const Route = struct { group_id: u64, range_index: usize, table_index: usize };
pub const Schedule = struct {
    entries: std.AutoHashMapUnmanaged(u64, Entry) = .empty,
    head: ?u64 = null,
    tail: ?u64 = null,
    pending: usize = 0,
    epoch: u64 = 0,
    now_ms: u64 = 0,
    const Entry = struct {
        identity: Identity,
        route: Route,
        seen: u64,
        complete: bool = false,
        retry_at_ms: u64 = 0,
        previous: ?u64 = null,
        next: ?u64 = null,
    };
    pub fn deinit(self: *Schedule, alloc: std.mem.Allocator) void {
        self.entries.deinit(alloc);
    }
    pub fn beginSync(self: *Schedule, now_ms: u64) void {
        self.now_ms = now_ms;
        self.epoch +%= 1;
    }
    pub fn observe(self: *Schedule, alloc: std.mem.Allocator, route: Route, identity: Identity) !void {
        const found = try self.entries.getOrPut(alloc, route.group_id);
        if (!found.found_existing) {
            found.value_ptr.* = .{ .identity = identity, .route = route, .seen = self.epoch };
            self.append(route.group_id);
            return;
        }
        const changed = !std.meta.eql(found.value_ptr.identity, identity);
        found.value_ptr.route = route;
        found.value_ptr.seen = self.epoch;
        found.value_ptr.identity = identity;
        if (changed or (found.value_ptr.complete and found.value_ptr.retry_at_ms <= self.now_ms)) {
            found.value_ptr.retry_at_ms = 0;
            if (found.value_ptr.complete) {
                found.value_ptr.complete = false;
                self.append(route.group_id);
            }
        }
    }
    pub fn endSync(self: *Schedule, alloc: std.mem.Allocator) !void {
        var stale: std.ArrayListUnmanaged(u64) = .empty;
        defer stale.deinit(alloc);
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.seen != self.epoch) try stale.append(alloc, entry.key_ptr.*);
        }
        for (stale.items) |id| {
            if (!self.entries.get(id).?.complete) self.unlink(id);
            _ = self.entries.remove(id);
        }
    }
    fn append(self: *Schedule, id: u64) void {
        const entry = self.entries.getPtr(id).?;
        entry.previous = self.tail;
        entry.next = null;
        if (self.tail) |tail| self.entries.getPtr(tail).?.next = id else self.head = id;
        self.tail = id;
        self.pending += 1;
    }
    fn unlink(self: *Schedule, id: u64) void {
        const entry = self.entries.getPtr(id).?;
        if (entry.previous) |previous| self.entries.getPtr(previous).?.next = entry.next else self.head = entry.next;
        if (entry.next) |next| self.entries.getPtr(next).?.previous = entry.previous else self.tail = entry.previous;
        entry.previous = null;
        entry.next = null;
        self.pending -= 1;
    }
    /// Selection does not spend a group's turn. A pass may exhaust its time
    /// budget before attempting every selected route.
    pub fn selectReady(self: *const Schedule, now_ms: u64, out: []Route) usize {
        var id = self.head;
        var count: usize = 0;
        while (id) |current| {
            if (count == out.len) break;
            const entry = self.entries.get(current).?;
            if (entry.retry_at_ms <= now_ms) {
                out[count] = entry.route;
                count += 1;
            }
            id = entry.next;
        }
        return count;
    }
    pub fn beginAttempt(self: *Schedule, id: u64) void {
        std.debug.assert(!self.entries.get(id).?.complete);
        self.unlink(id);
        self.append(id);
    }
    /// Convenience for callers that immediately execute the selected turn.
    pub fn take(self: *Schedule, now_ms: u64) ?Route {
        const id = self.head orelse return null;
        const entry = self.entries.get(id).?;
        self.beginAttempt(id);
        return if (entry.retry_at_ms <= now_ms) entry.route else null;
    }
    pub fn finish(self: *Schedule, id: u64, complete: bool, retry_at_ms: u64) void {
        const entry = self.entries.getPtr(id) orelse return;
        entry.retry_at_ms = if (complete) self.now_ms +| 60_000 else retry_at_ms;
        if (complete and !entry.complete) {
            self.unlink(id);
            entry.complete = true;
        }
    }
};

test "schema repair queue rotates debt and fences completed proofs" {
    const a = std.testing.allocator;
    var queue: Schedule = .{};
    defer queue.deinit(a);
    const identity: Identity = .{ .table_id = 1, .schema_hash = 2, .root_generation = 3, .ownership_generation = 4 };
    queue.beginSync(0);
    for (0..100) |i| try queue.observe(a, .{ .group_id = i, .range_index = i, .table_index = 0 }, identity);
    try queue.endSync(a);
    // A long-running first group cannot pin small groups behind it.
    for (0..100) |i| {
        const route = queue.take(0).?;
        try std.testing.expectEqual(i, route.group_id);
        queue.finish(route.group_id, i != 0, 0);
    }
    try std.testing.expectEqual(@as(usize, 1), queue.pending);
    queue.beginSync(0);
    for (0..100) |i| try queue.observe(a, .{ .group_id = i, .range_index = i + 10, .table_index = 1 }, identity);
    try queue.endSync(a);
    try std.testing.expectEqual(@as(usize, 1), queue.pending);
    try std.testing.expectEqual(@as(usize, 10), queue.take(0).?.range_index);
    queue.finish(0, false, 50);
    try std.testing.expect(queue.take(49) == null);
    try std.testing.expect(queue.take(50) != null);
    var replacement = identity;
    replacement.root_generation += 1;
    try queue.observe(a, .{ .group_id = 1, .range_index = 11, .table_index = 1 }, replacement);
    try std.testing.expectEqual(@as(usize, 2), queue.pending);
    queue.beginSync(0);
    try queue.observe(a, .{ .group_id = 1, .range_index = 0, .table_index = 0 }, replacement);
    try queue.endSync(a);
    try std.testing.expectEqual(@as(usize, 1), queue.entries.count());
    try std.testing.expectEqual(@as(u64, 1), queue.take(50).?.group_id);
    queue.finish(1, true, 0);
    try std.testing.expectEqual(@as(usize, 0), queue.pending);
    queue.beginSync(60_000);
    try queue.observe(a, .{ .group_id = 1, .range_index = 0, .table_index = 0 }, replacement);
    try queue.endSync(a);
    try std.testing.expectEqual(@as(usize, 1), queue.pending);
}

test "schema repair queue preserves unstarted turns across a truncated pass" {
    const a = std.testing.allocator;
    var queue: Schedule = .{};
    defer queue.deinit(a);
    const identity: Identity = .{ .table_id = 1, .schema_hash = 2, .root_generation = 3, .ownership_generation = 4 };
    queue.beginSync(0);
    for (0..16) |i| try queue.observe(a, .{ .group_id = i, .range_index = i, .table_index = 0 }, identity);
    try queue.endSync(a);
    var selected: [16]Route = undefined;
    for (0..32) |i| {
        try std.testing.expectEqual(@as(usize, 16), queue.selectReady(0, &selected));
        try std.testing.expectEqual(i % 16, selected[0].group_id);
        // Only the first selection fits in this pass. Every group still gets
        // a turn, even when the selection width equals the queue length.
        queue.beginAttempt(selected[0].group_id);
        queue.finish(selected[0].group_id, false, 0);
    }
    queue.finish(0, false, 50);
    try std.testing.expectEqual(@as(usize, 15), queue.selectReady(49, &selected));
    try std.testing.expectEqual(@as(u64, 1), selected[0].group_id);
}

test "schema repair scheduling workload benchmark" {
    if (std.c.getenv("ANTFLY_CATALOG_REPORT_BENCH") == null) return;
    const a = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const identity: Identity = .{ .table_id = 1, .schema_hash = 2, .root_generation = 3, .ownership_generation = 4 };
    for ([_]usize{ 1000, 10000 }) |count| {
        for ([_]bool{ false, true }) |queued| {
            var samples: [5]u64 = undefined;
            var inspections: u64 = 0;
            for (0..6) |sample| {
                inspections = 0;
                const started = std.Io.Clock.awake.now(io).nanoseconds;
                if (queued) {
                    var queue: Schedule = .{};
                    defer queue.deinit(a);
                    queue.beginSync(0);
                    for (0..count) |i| try queue.observe(a, .{ .group_id = i, .range_index = i, .table_index = 0 }, identity);
                    try queue.endSync(a);
                    while (queue.take(0)) |route| {
                        inspections += 1;
                        queue.finish(route.group_id, true, 0);
                    }
                } else {
                    const completed = try a.alloc(bool, count);
                    defer a.free(completed);
                    @memset(completed, false);
                    for (0..count) |_| {
                        for (completed) |*done| {
                            inspections += 1;
                            if (!done.*) {
                                done.* = true;
                                break;
                            }
                        }
                    }
                }
                const elapsed: u64 = @intCast(std.Io.Clock.awake.now(io).nanoseconds - started);
                if (sample != 0) samples[sample - 1] = elapsed;
            }
            try std.testing.expectEqual(if (queued) count else count * (count + 1) / 2, inspections);
            const json = try std.json.Stringify.valueAlloc(a, .{
                .scenario = "schema_repair_completed_prefix",
                .groups = count,
                .queued = queued,
                .owner_inspections = inspections,
                .warmups = 1,
                .samples_ns = samples,
            }, .{});
            defer a.free(json);
            std.debug.print("{s}\n", .{json});
        }
    }
}
