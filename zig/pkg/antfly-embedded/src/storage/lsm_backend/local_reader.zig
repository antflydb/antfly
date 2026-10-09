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
const RecyclingWorkspace = @import("recycling_workspace.zig").RecyclingWorkspace;
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
        bound_manager: ?*resources.ResourceManager = null,
        bound_backing: std.mem.Allocator = undefined,
        arena: std.heap.ArenaAllocator = undefined,
        recycled: RecyclingWorkspace = .{},
        recycled_mode: bool = false,
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
            return if (self.slot.recycled_mode) self.slot.recycled.allocator() else self.slot.arena.allocator();
        }
        pub fn reclaimIdle(self: *const Workspace) void {
            if (self.slot.recycled_mode) self.slot.recycled.trimIdle(0);
        }

        pub fn release(self: *Workspace) void {
            if (self.slot.recycled_mode) {
                std.debug.assert(self.slot.recycled.live_buffers == 0);
                self.slot.recycled.trimIdle(retained_bytes_per_workspace);
            } else if (!self.slot.arena.reset(.{ .retain_with_limit = retained_bytes_per_workspace })) _ = self.slot.arena.reset(.free_all);
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
        return self.acquireMode(backing, manager, io, bytes, limit, output_bytes, false);
    }

    pub fn acquireRecycled(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize) Workspace {
        return self.acquireMode(backing, manager, io, bytes, limit, 0, true);
    }

    fn acquireMode(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize, output_bytes: usize, recycled_mode: bool) Workspace {
        while (true) {
            platform.sync.lockYielding(&self.mutex);
            if (self.active == 0 or bytes <= limit -| self.active_bytes) {
                for (&self.slots) |*slot| if (!slot.busy) {
                    slot.busy = true;
                    self.active += 1;
                    self.active_bytes += bytes;
                    self.peak_active_bytes = @max(self.peak_active_bytes, self.active_bytes);
                    if (self.allocator == null) self.allocator = backing;
                    self.mutex.unlock();
                    // The busy slot is exclusively owned. Release retained
                    // credit before rebinding, outside the coordination lock.
                    if (slot.initialized and (slot.bound_manager != manager or slot.bound_backing.ptr != backing.ptr or slot.bound_backing.vtable != backing.vtable or slot.recycled_mode != recycled_mode)) {
                        slot.arena.deinit();
                        slot.recycled.deinit();
                        if (slot.budget) |*budget| budget.deinit();
                        slot.budget = null;
                        slot.cap = .{};
                        slot.initialized = false;
                    }
                    if (!slot.initialized) {
                        slot.bound_manager = manager;
                        slot.bound_backing = backing;
                        if (manager) |host| {
                            slot.budget = resources.BudgetedAllocator.init(host, .lsm_read_working_set, backing, 1);
                            slot.budget.?.credit_quantum = 4096;
                        }
                        slot.cap.backing = if (slot.budget) |*budget| budget.allocator() else backing;
                        slot.arena = .init(slot.cap.allocator());
                        slot.recycled = .{ .backing = slot.cap.allocator(), .budget = if (slot.budget) |*budget| budget else null };
                        slot.recycled_mode = recycled_mode;
                        slot.initialized = true;
                    }
                    slot.cap.limit = @max(slot.cap.live, bytes -| output_bytes);
                    return .{ .pool = self, .slot = slot, .bytes = bytes };
                };
            }
            self.waitLocked(io);
        }
    }

    pub const Ticket = struct { flight: *Flight, leader: bool };
    /// Called without the writer mutex. Flight exhaustion waits for a bounded
    /// record instead of allocating an unbounded coordination structure.
    pub fn begin(self: *Pool, backing: std.mem.Allocator, io: ?std.Io, path: []const u8, run_id: u64, offset: u64, len: u32, admit: bool) !Ticket {
        while (true) {
            platform.sync.lockYielding(&self.mutex);
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
            self.waitLocked(io);
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
            slot.recycled.deinit();
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

test "lsm local decoder frees oversized scratch when reset allocation fails" {
    const allocators = @import("../lite/test_allocator.zig");
    var backing = allocators.BudgetAllocator{ .backing = std.testing.allocator };
    var no_resize = allocators.NoResizeAllocator{ .backing = backing.allocator() };
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var pool: Pool = .{};
    defer pool.deinit();
    var work = pool.acquire(no_resize.allocator(), &manager, std.testing.io, 2 * 1024 * 1024, 4 * 1024 * 1024, 0);
    _ = try work.allocator().alloc(u8, 1024 * 1024);
    backing.limit = backing.live;
    work.release();
    try std.testing.expectEqual(@as(usize, 0), backing.live);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
    backing.limit = std.math.maxInt(usize);
    work = pool.acquire(no_resize.allocator(), &manager, std.testing.io, 256 * 1024, 4 * 1024 * 1024, 0);
    _ = try work.allocator().alloc(u8, 4096);
    work.release();
    try std.testing.expect(pool.slots[0].cap.live <= Pool.retained_bytes_per_workspace + 128);
}
