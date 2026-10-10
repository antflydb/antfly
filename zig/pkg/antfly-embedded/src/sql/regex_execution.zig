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

//! Move-only, stable-address statement/cursor owner. Matcher lanes never share
//! native scratch, and the aggregate quota includes idle caches and metadata.
const std = @import("std");
const native = @import("antfly_sql_regex");
const MemoryBudget = @import("memory_budget.zig");
const Owner = @This();

budget: MemoryBudget,
mutex: std.atomic.Mutex = .unlocked,
lanes: ?*Lane = null,
stats: Stats = .{},

const Lane = struct {
    next: ?*Lane = null,
    busy: bool = true,
    session: native.Session,
    compilations: u64 = 0,
    hits: u64 = 0,
    replacement_preparations: u64 = 0,
    replacement_hits: u64 = 0,
};
pub const Stats = struct { lanes: usize = 0, active: usize = 0, peak_active: usize = 0, compilations: u64 = 0, hits: u64 = 0, replacement_preparations: u64 = 0, replacement_hits: u64 = 0 };
pub const Lease = struct {
    owner: *Owner,
    lane: *Lane,
    pub fn session(self: Lease) *native.Session {
        return &self.lane.session;
    }
    pub fn deinit(self: Lease) void {
        self.owner.lock();
        defer self.owner.mutex.unlock();
        std.debug.assert(self.lane.busy and self.owner.stats.active != 0);
        self.owner.stats.compilations +|= self.lane.session.compilations - self.lane.compilations;
        self.owner.stats.hits +|= self.lane.session.hits - self.lane.hits;
        self.owner.stats.replacement_preparations +|= self.lane.session.replacement_preparations - self.lane.replacement_preparations;
        self.owner.stats.replacement_hits +|= self.lane.session.replacement_hits - self.lane.replacement_hits;
        self.lane.compilations = self.lane.session.compilations;
        self.lane.hits = self.lane.session.hits;
        self.lane.replacement_preparations = self.lane.session.replacement_preparations;
        self.lane.replacement_hits = self.lane.session.replacement_hits;
        self.lane.busy = false;
        self.owner.stats.active -= 1;
    }
};
pub fn init(backing: std.mem.Allocator, maximum: usize) Owner {
    return .{ .budget = .{ .backing = backing, .limit = maximum } };
}
fn lock(self: *Owner) void {
    while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
}
fn admitted(self: *Owner) void {
    self.stats.active += 1;
    self.stats.peak_active = @max(self.stats.peak_active, self.stats.active);
}
pub fn acquire(self: *Owner) !Lease {
    self.lock();
    var next = self.lanes;
    while (next) |lane| : (next = lane.next) {
        if (!lane.busy) {
            lane.busy = true;
            self.admitted();
            self.mutex.unlock();
            return .{ .owner = self, .lane = lane };
        }
    }
    self.mutex.unlock();
    // Allocation and native execution never hold the lane bookkeeping lock.
    // All lanes, including retained idle ones, share actual-byte admission.
    const a = self.budget.allocator();
    const lane = a.create(Lane) catch |err| return self.mapError(err);
    const heap = @min(8 * 1024 * 1024, self.budget.limit / 4);
    lane.* = .{ .session = native.Session.init(a, .{ .heap_bytes = heap }, @min(16 * 1024 * 1024, self.budget.limit / 2)) };
    self.lock();
    defer self.mutex.unlock();
    lane.next = self.lanes;
    self.lanes = lane;
    self.stats.lanes += 1;
    self.admitted();
    return .{ .owner = self, .lane = lane };
}
pub fn snapshot(self: *Owner) Stats {
    self.lock();
    defer self.mutex.unlock();
    return self.stats;
}
pub fn mapError(self: *Owner, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and self.budget.isExhausted()) error.SqlProgramLimitExceeded else err;
}
/// Quiesce all evaluation lanes before closing. No row/result bytes borrow a
/// session, and no native program retains request checkpoints or budgets.
pub fn deinit(self: *Owner) void {
    std.debug.assert(self.stats.active == 0);
    var next = self.lanes;
    while (next) |lane| {
        std.debug.assert(!lane.busy);
        next = lane.next;
        lane.session.deinit();
        self.budget.allocator().destroy(lane);
    }
    std.debug.assert(self.budget.live == 0);
    self.lanes = null;
}

test "SQL regex execution owns independent bounded lanes and reuses warm patterns" {
    var owner = init(std.testing.allocator, 16 * 1024 * 1024);
    defer owner.deinit();
    {
        const left = try owner.acquire();
        defer left.deinit();
        const right = try owner.acquire();
        defer right.deinit();
        try std.testing.expect(left.session() != right.session());
        var budget: native.Budget = .{};
        _ = try left.session().pattern("a+", 3, &budget);
        _ = try right.session().pattern("a+", 3, &budget);
    }
    for (0..1000) |_| {
        const lease = try owner.acquire();
        defer lease.deinit();
        var budget: native.Budget = .{};
        _ = try lease.session().pattern("a+", 3, &budget);
    }
    const stats = owner.snapshot();
    try std.testing.expectEqual(@as(usize, 2), stats.lanes);
    try std.testing.expectEqual(@as(usize, 2), stats.peak_active);
    try std.testing.expectEqual(@as(u64, 2), stats.compilations);
    try std.testing.expectEqual(@as(u64, 1000), stats.hits);
    try std.testing.expect(owner.budget.peak <= owner.budget.limit);
}

fn allocationCase(a: std.mem.Allocator) !void {
    var owner = init(a, 16 * 1024 * 1024);
    defer owner.deinit();
    const lease = try owner.acquire();
    defer lease.deinit();
    var budget: native.Budget = .{};
    _ = try lease.session().pattern("(a+)(b)?", 3, &budget);
}

test "SQL regex execution leases isolate concurrent Io workers" {
    if (comptime @import("builtin").single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var owner = init(a, 16 * 1024 * 1024);
    defer owner.deinit();
    var io_impl = std.Io.Threaded.init(a, .{ .concurrent_limit = .limited(2) });
    defer io_impl.deinit();
    const io = io_impl.io();
    var entered: std.atomic.Value(usize) = .init(0);
    var gate: std.Io.Event = .unset;
    const Worker = struct {
        fn run(pool: *Owner, active_io: std.Io, count: *std.atomic.Value(usize), start: *std.Io.Event, input: []const u8) !void {
            const lease = try pool.acquire();
            defer lease.deinit();
            _ = count.fetchAdd(1, .acq_rel);
            try start.wait(active_io);
            var subject = try native.Subject.init(std.testing.allocator, input);
            defer subject.deinit();
            var spans: [1]native.Span = undefined;
            for (0..100) |_| {
                var budget: native.Budget = .{};
                const program = try lease.session().pattern("[0-9]+", 3, &budget);
                try std.testing.expect(try lease.session().executor.find(program, subject, 0, &spans, &budget));
                try std.testing.expectEqualStrings("123", (try subject.slice(spans[0])).?);
            }
        }
    };
    var first = try io.concurrent(Worker.run, .{ &owner, io, &entered, &gate, "雪123" });
    defer first.cancel(io) catch {};
    var second = try io.concurrent(Worker.run, .{ &owner, io, &entered, &gate, "😀123" });
    defer second.cancel(io) catch {};
    defer gate.set(io);
    for (0..2000) |_| {
        if (entered.load(.acquire) == 2) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 2), entered.load(.acquire));
    gate.set(io);
    try first.await(io);
    try second.await(io);
    const stats = owner.snapshot();
    try std.testing.expectEqual(@as(usize, 2), stats.peak_active);
    try std.testing.expectEqual(@as(u64, 2), stats.compilations);
    try std.testing.expectEqual(@as(u64, 198), stats.hits);
    try std.testing.expectEqual(@as(usize, 0), stats.active);
}
test "SQL regex execution releases partially admitted lanes on allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
    var owner = init(std.testing.allocator, 0);
    defer owner.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, owner.acquire());
    try std.testing.expectEqual(@as(usize, 0), owner.snapshot().active);
}
