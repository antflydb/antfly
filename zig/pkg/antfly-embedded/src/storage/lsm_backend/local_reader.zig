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
const platform = @import("antfly_platform");
const resources = @import("../resource_manager.zig");
const read_admission = @import("../../common/read_admission.zig");
const bounded_mutex = @import("../../common/bounded_mutex.zig");
const SharedBytes = @import("shared_bytes.zig").SharedBytes;

/// A fixed number of decoder workspaces and flight records. Workspace storage
/// is independent of result storage; releasing scratch never invalidates a row.
pub const Pool = struct {
    pub const workspace_count = 4;
    pub const flight_count = 16;
    pub const retained_bytes_per_workspace = 64 * 1024;
    const Waiter = struct {
        next: ?*Waiter = null,
        io: ?std.Io,
        done: std.Io.Event = .unset,
        notified: std.atomic.Value(bool) = .init(false),
    };
    const Capped = struct {
        backing: std.mem.Allocator = undefined,
        live: usize = 0,
        limit: usize = 0,
        fn allocator(self: *Capped) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *Capped = @ptrCast(@alignCast(raw));
            if (len > self.limit -| self.live) return null;
            const ptr = self.backing.rawAlloc(len, alignment, ra) orelse return null;
            self.live += len;
            return ptr;
        }
        fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
            const self: *Capped = @ptrCast(@alignCast(raw));
            if (len > memory.len and len - memory.len > self.limit -| self.live) return false;
            if (!self.backing.rawResize(memory, alignment, len, ra)) return false;
            self.live = self.live - memory.len + len;
            return true;
        }
        fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
            const self: *Capped = @ptrCast(@alignCast(raw));
            if (len > memory.len and len - memory.len > self.limit -| self.live) return null;
            const ptr = self.backing.rawRemap(memory, alignment, len, ra) orelse return null;
            self.live = self.live - memory.len + len;
            return ptr;
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *Capped = @ptrCast(@alignCast(raw));
            self.backing.rawFree(memory, alignment, ra);
            self.live -= memory.len;
        }
    };
    const Slot = struct {
        busy: bool = false,
        initialized: bool = false,
        arena: std.heap.ArenaAllocator = undefined,
        budget: ?resources.BudgetedAllocator = null,
        cap: Capped = .{},
    };
    pub const Flight = struct {
        refs: usize = 0,
        path: []u8 = &.{},
        run_id: u64 = 0,
        offset: u64 = 0,
        len: u32 = 0,
        admit: bool = false,
        completed: bool = false,
        io: ?std.Io = null,
        done: std.Io.Event = .unset,
        result: ?*SharedBytes = null,
        failure: ?anyerror = null,
    };
    mutex: std.atomic.Mutex = .unlocked,
    slots: [workspace_count]Slot = @splat(.{}),
    flights: [flight_count]Flight = @splat(.{}),
    waiters: ?*Waiter = null,
    active_bytes: usize = 0,
    active: usize = 0,
    peak_active_bytes: usize = 0,
    joined: usize = 0,
    allocator: ?std.mem.Allocator = null,

    fn notifyLocked(self: *Pool) void {
        var next = self.waiters;
        self.waiters = null;
        while (next) |waiter| {
            next = waiter.next;
            waiter.notified.store(true, .release);
            if (waiter.io) |io| waiter.done.set(io);
        }
    }
    /// Caller owns mutex; this returns with it unlocked. The stack waiter is
    /// detached before notification. Reacquiring the mutex below fences the
    /// notifier's final access, including Event.set's wake operation.
    fn waitLocked(self: *Pool, io: ?std.Io) void {
        var waiter = Waiter{ .next = self.waiters, .io = io };
        self.waiters = &waiter;
        self.mutex.unlock();
        if (io) |owned| waiter.done.waitUncancelable(owned) else {
            while (!waiter.notified.load(.acquire)) platform.time.yieldBriefly();
        }
        // Waking is not proof that notifyLocked has finished accessing this
        // stack frame. Keep it alive until the notifier releases the mutex.
        platform.sync.lockYielding(&self.mutex);
        self.mutex.unlock();
    }

    pub const Workspace = struct {
        pool: *Pool,
        slot: *Slot,
        bytes: usize,
        pub fn allocator(self: *const Workspace) std.mem.Allocator {
            return self.slot.arena.allocator();
        }
        pub fn release(self: *Workspace) void {
            _ = self.slot.arena.reset(.{ .retain_with_limit = retained_bytes_per_workspace });
            if (self.slot.budget) |*budget| _ = budget.releaseUnusedCredit();
            platform.sync.lockYielding(&self.pool.mutex);
            self.slot.busy = false;
            self.pool.active_bytes -= self.bytes;
            self.pool.active -= 1;
            self.pool.notifyLocked();
            self.pool.mutex.unlock();
            self.* = undefined;
        }
    };

    /// Ordinary operations share the byte ceiling. One operation larger than
    /// the ceiling runs alone, preserving support for legitimate large rows.
    pub fn acquire(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize, output_bytes: usize) Workspace {
        return self.acquireWithAdmission(backing, manager, io, bytes, limit, output_bytes, null) catch unreachable;
    }

    fn lockWithAdmission(self: *Pool, control: ?read_admission.Port) !void {
        if (control) |budget| try bounded_mutex.lockAtomic(&self.mutex, budget) else platform.sync.lockYielding(&self.mutex);
    }

    /// Scoped waiters borrow the original operation only for this call. They
    /// register no stack event that could outlive cancellation.
    fn waitLockedWithAdmission(self: *Pool, io: ?std.Io, control: ?read_admission.Port) !void {
        if (control) |budget| {
            self.mutex.unlock();
            try budget.waitForProgress();
        } else self.waitLocked(io);
    }

    pub fn acquireWithAdmission(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize, output_bytes: usize, control: ?read_admission.Port) !Workspace {
        while (true) {
            try self.lockWithAdmission(control);
            if (self.active == 0 or bytes <= limit -| self.active_bytes) {
                for (&self.slots) |*slot| if (!slot.busy) {
                    slot.busy = true;
                    self.active += 1;
                    self.active_bytes += bytes;
                    self.peak_active_bytes = @max(self.peak_active_bytes, self.active_bytes);
                    if (self.allocator == null) self.allocator = backing;
                    if (!slot.initialized) {
                        if (manager) |host| {
                            slot.budget = resources.BudgetedAllocator.init(host, .lsm_read_working_set, backing, 1);
                            slot.budget.?.credit_quantum = 4096;
                        }
                        slot.cap.backing = if (slot.budget) |*budget| budget.allocator() else backing;
                        slot.arena = .init(slot.cap.allocator());
                        slot.initialized = true;
                    }
                    slot.cap.limit = @max(slot.cap.live, bytes -| output_bytes);
                    self.mutex.unlock();
                    return .{ .pool = self, .slot = slot, .bytes = bytes };
                };
            }
            try self.waitLockedWithAdmission(io, control);
        }
    }

    pub const Ticket = struct { flight: *Flight, leader: bool };
    /// Called without the writer mutex. Flight exhaustion waits for a bounded
    /// record instead of allocating an unbounded coordination structure.
    pub fn begin(self: *Pool, backing: std.mem.Allocator, io: ?std.Io, path: []const u8, run_id: u64, offset: u64, len: u32, admit: bool) !Ticket {
        return self.beginWithAdmission(backing, io, path, run_id, offset, len, admit, null);
    }

    pub fn beginWithAdmission(self: *Pool, backing: std.mem.Allocator, io: ?std.Io, path: []const u8, run_id: u64, offset: u64, len: u32, admit: bool, control: ?read_admission.Port) !Ticket {
        while (true) {
            try self.lockWithAdmission(control);
            for (&self.flights) |*flight| {
                if (flight.refs != 0 and flight.run_id == run_id and flight.offset == offset and flight.len == len and flight.admit == admit and std.mem.eql(u8, flight.path, path)) {
                    flight.refs += 1;
                    self.joined += 1;
                    self.mutex.unlock();
                    return .{ .flight = flight, .leader = false };
                }
            }
            for (&self.flights) |*flight| if (flight.refs == 0) {
                const owned = backing.dupe(u8, path) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
                if (self.allocator == null) self.allocator = backing;
                flight.* = .{ .refs = 1, .path = owned, .run_id = run_id, .offset = offset, .len = len, .admit = admit, .io = io };
                self.mutex.unlock();
                return .{ .flight = flight, .leader = true };
            };
            try self.waitLockedWithAdmission(io, control);
        }
    }
    pub fn join(self: *Pool, path: []const u8, run_id: u64, offset: u64, len: u32, admit: bool) ?*Flight {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (&self.flights) |*flight| {
            if (flight.refs != 0 and flight.run_id == run_id and flight.offset == offset and flight.len == len and flight.admit == admit and std.mem.eql(u8, flight.path, path)) {
                flight.refs += 1;
                self.joined += 1;
                return flight;
            }
        }
        return null;
    }
    pub fn finish(self: *Pool, flight: *Flight, result: anyerror!*SharedBytes) void {
        platform.sync.lockYielding(&self.mutex);
        if (result) |payload| flight.result = payload.retain() else |err| flight.failure = err;
        flight.completed = true;
        if (flight.io) |io| flight.done.set(io);
        self.mutex.unlock();
    }
    pub fn waitWithAdmission(self: *Pool, flight: *Flight, control: ?read_admission.Port) !*SharedBytes {
        const budget = control orelse return self.wait(flight);
        while (true) {
            try self.lockWithAdmission(budget);
            if (flight.completed) {
                defer self.mutex.unlock();
                if (flight.failure) |err| return err;
                // The caller's flight reference keeps the result alive. No
                // admission borrow escapes with the returned payload.
                return flight.result.?.retain();
            }
            self.mutex.unlock();
            try budget.waitForProgress();
        }
    }

    pub fn wait(self: *Pool, flight: *Flight) !*SharedBytes {
        if (flight.io) |io| flight.done.waitUncancelable(io) else {
            while (true) {
                platform.sync.lockYielding(&self.mutex);
                const completed = flight.completed;
                self.mutex.unlock();
                if (completed) break;
                platform.time.yieldBriefly();
            }
        }
        if (flight.failure) |err| return err;
        return flight.result.?.retain();
    }
    pub fn releaseFlight(self: *Pool, flight: *Flight) void {
        platform.sync.lockYielding(&self.mutex);
        flight.refs -= 1;
        if (flight.refs != 0) {
            self.mutex.unlock();
            return;
        }
        const result = flight.result;
        const path = flight.path;
        flight.result = null;
        flight.path = &.{};
        self.notifyLocked();
        self.mutex.unlock();
        // Final result/free callbacks never run under the coordination lock.
        if (result) |payload| payload.release();
        self.allocator.?.free(path);
    }
    pub fn deinit(self: *Pool) void {
        std.debug.assert(self.active == 0 and self.waiters == null);
        for (&self.flights) |*flight| std.debug.assert(flight.refs == 0);
        for (&self.slots) |*slot| if (slot.initialized) {
            slot.arena.deinit();
            if (slot.budget) |*budget| budget.deinit();
        };
        self.* = .{};
    }
};

test "lsm local decoder scratch reuse is bounded charged and failure-safe" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var manager = resources.ResourceManager.init(.{});
            defer manager.deinit(std.testing.allocator);
            var pool: Pool = .{};
            var open = true;
            defer if (open) pool.deinit();
            for (0..2) |_| {
                var work = pool.acquire(a, &manager, std.testing.io, 256 * 1024, 1024 * 1024, 0);
                defer work.release();
                const bytes = try work.allocator().alloc(u8, 1024);
                @memset(bytes, 123);
                try std.testing.expectEqual(@as(u8, 123), bytes[0]);
            }
            try std.testing.expectEqual(@as(u64, pool.slots[0].cap.live), manager.sliceStats(.lsm_read_working_set).used_bytes);
            try std.testing.expect(pool.slots[0].cap.live <= Pool.retained_bytes_per_workspace + @sizeOf(usize) * 4);
            {
                var work = pool.acquire(a, &manager, std.testing.io, 256 * 1024, 1024 * 1024, 0);
                defer work.release();
                try std.testing.expectError(error.OutOfMemory, work.allocator().alloc(u8, 512 * 1024));
            }
            pool.deinit();
            open = false;
            try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "lsm local decoder byte gate admits oversized work alone and wakes waiters" {
    const Worker = struct {
        pool: *Pool,
        entered: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            var work = self.pool.acquire(std.testing.allocator, null, std.testing.io, 64 * 1024, 64 * 1024, 0);
            self.entered.set(std.testing.io);
            work.release();
        }
    };
    var pool: Pool = .{};
    defer pool.deinit();
    var big = pool.acquire(std.testing.allocator, null, std.testing.io, 128 * 1024, 64 * 1024, 0);
    var released = false;
    defer if (!released) big.release();
    var worker = Worker{ .pool = &pool };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    var joined = false;
    defer if (!joined) {
        if (!released) {
            big.release();
            released = true;
        }
        thread.join();
    };
    const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(5));
    var waiting = false;
    while (std.Io.Clock.awake.now(std.testing.io).nanoseconds < deadline.nanoseconds) {
        platform.sync.lockYielding(&pool.mutex);
        waiting = pool.waiters != null;
        pool.mutex.unlock();
        if (waiting) break;
        platform.time.yieldBriefly();
    }
    big.release();
    released = true;
    thread.join();
    joined = true;
    try std.testing.expect(waiting);
    try std.testing.expectEqual(@as(usize, 128 * 1024), pool.peak_active_bytes);
    try std.testing.expectEqual(@as(usize, 0), pool.active);
}

test "lsm local waiter keeps stack alive until notifier unlocks with and without io" {
    const Worker = struct {
        pool: *Pool,
        io: ?std.Io,
        returned: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            platform.sync.lockYielding(&self.pool.mutex);
            self.pool.waitLocked(self.io);
            self.returned.set(std.testing.io);
        }
    };
    for ([_]?std.Io{ null, std.testing.io }) |io| {
        var pool: Pool = .{};
        defer pool.deinit();
        var worker = Worker{ .pool = &pool, .io = io };
        const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
        defer thread.join();
        const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(5));
        while (true) {
            platform.sync.lockYielding(&pool.mutex);
            if (pool.waiters != null) break;
            pool.mutex.unlock();
            if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= deadline.nanoseconds) return error.WaiterNotRegistered;
            platform.time.yieldBriefly();
        }
        pool.notifyLocked();
        const returned_early = blk: {
            defer pool.mutex.unlock();
            // Give the awakened thread an opportunity to return while the
            // notifier still owns the stack-lifetime fence.
            std.testing.io.sleep(.fromMilliseconds(100), .awake) catch {};
            break :blk worker.returned.isSet();
        };
        worker.returned.waitUncancelable(std.testing.io);
        try std.testing.expect(!returned_early);
    }
}

test "lsm local decoder admission cancellation preserves producer and healthy follower" {
    var pool: Pool = .{};
    defer pool.deinit();
    const leader = try pool.begin(std.testing.allocator, null, "block", 1, 0, 64, true);
    defer pool.releaseFlight(leader.flight);
    const follower = try pool.begin(std.testing.allocator, null, "block", 1, 0, 64, true);
    var follower_held = true;
    defer if (follower_held) pool.releaseFlight(follower.flight);
    const healthy = try pool.begin(std.testing.allocator, null, "block", 1, 0, 64, true);
    defer pool.releaseFlight(healthy.flight);
    const Budget = struct {
        waits: usize = 0,
        canceled: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.canceled = true;
        }
    };
    var budget = Budget{};
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.Canceled, pool.waitWithAdmission(follower.flight, .init(&payload)));
    pool.releaseFlight(follower.flight);
    follower_held = false;
    try std.testing.expectEqual(@as(usize, 2), leader.flight.refs);
    try std.testing.expect(!leader.flight.completed);
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    const result = try SharedBytes.create(std.testing.allocator, try std.testing.allocator.dupe(u8, "decoded"));
    defer result.release();
    pool.finish(leader.flight, result);
    const retained = try pool.wait(healthy.flight);
    defer retained.release();
    try std.testing.expectEqualStrings("decoded", retained.bytes);
}

test "lsm local decoder admission progress releases workspace before retry" {
    var pool: Pool = .{};
    defer pool.deinit();
    var held = pool.acquire(std.testing.allocator, null, null, 128, 64, 0);
    var held_live = true;
    defer if (held_live) held.release();
    const Budget = struct {
        held: *Pool.Workspace,
        held_live: *bool,
        waits: usize = 0,
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.held.release();
            self.held_live.* = false;
        }
    };
    var budget = Budget{ .held = &held, .held_live = &held_live };
    var adapter = read_admission.Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    var admitted = try pool.acquireWithAdmission(std.testing.allocator, null, null, 64, 64, 0, .init(&payload));
    defer admitted.release();
    try std.testing.expectEqual(@as(usize, 1), budget.waits);
    try std.testing.expectEqual(@as(usize, 1), pool.active);
    try std.testing.expect(pool.waiters == null);
}
