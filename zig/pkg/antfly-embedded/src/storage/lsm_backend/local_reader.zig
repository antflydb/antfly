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
const RecyclingWorkspace = @import("recycling_workspace.zig").RecyclingWorkspace;
const table_file = @import("../lsm/table_file.zig");
const read_generation = @import("read_generation.zig");
const SharedBytes = @import("shared_bytes.zig").SharedBytes;

/// A fixed number of decoder workspaces and flight records. Workspace storage
/// is independent of result storage; releasing scratch never invalidates a row.
pub const Pool = struct {
    pub const workspace_count = 4;
    pub const flight_count = 16;
    pub const retained_bytes_per_workspace = 64 * 1024;

    /// Streaming block readers make two arena allocations: path and input
    /// window. With the pinned arena's 1.5x node growth, retaining R bytes and
    /// failing in-place resize can require up to 4.75R + 3.75P + 1.5W, plus
    /// node headers. Fund backing capacity, including retained nodes, before
    /// decoding; a logical P+W grant alone can falsely refuse a valid read.
    pub fn streamingScratchBound(path_bytes: usize, window_bytes: usize) !usize {
        const input = std.math.add(usize, path_bytes, window_bytes) catch return error.InvalidTableFile;
        const total = std.math.add(usize, input, retained_bytes_per_workspace + 128) catch return error.InvalidTableFile;
        return std.math.mul(usize, total, 5) catch error.InvalidTableFile;
    }
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
    pub const Key = struct {
        path: []const u8,
        generation: read_generation.Identity,
        run_id: u64,
        offset: u64,
        physical_len: u32,
        logical_len: u32,
        compression: table_file.BlockCompression,
        checksum: u32,
        admit: bool,
        fn matches(self: Key, other: Key) bool {
            return std.meta.eql(self.generation, other.generation) and self.run_id == other.run_id and
                self.offset == other.offset and self.physical_len == other.physical_len and
                self.logical_len == other.logical_len and self.compression == other.compression and
                self.checksum == other.checksum and self.admit == other.admit and std.mem.eql(u8, self.path, other.path);
        }
    };
    pub const Flight = struct {
        refs: usize = 0,
        constructing: bool = false,
        key: Key = undefined,
        backing: std.mem.Allocator = undefined,
        completed: bool = false,
        result: ?*SharedBytes = null,
        failed: bool = false,
    };
    mutex: std.atomic.Mutex = .unlocked,
    slots: [workspace_count]Slot = @splat(.{}),
    flights: [flight_count]Flight = @splat(.{}),
    waiters: ?*Waiter = null,
    active_bytes: usize = 0,
    active: usize = 0,
    peak_active_bytes: usize = 0,
    joined: usize = 0,

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
    /// This compatibility entry requires no runnable ownership. Production reads
    /// use the fallible admission entry, including callers without a port.
    pub fn acquire(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize, output_bytes: usize) Workspace {
        return self.acquireMode(backing, manager, io, bytes, limit, output_bytes, false, null, true) catch unreachable;
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
        return self.acquireMode(backing, manager, io, bytes, limit, output_bytes, false, control, try canWait(control));
    }

    pub fn acquireRecycled(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize) Workspace {
        return self.acquireMode(backing, manager, io, bytes, limit, 0, true, null, true) catch unreachable;
    }

    pub fn acquireRecycledWithAdmission(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize, control: ?read_admission.Port) !Workspace {
        return self.acquireMode(backing, manager, io, bytes, limit, 0, true, control, try canWait(control));
    }

    fn acquireMode(self: *Pool, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, io: ?std.Io, bytes: usize, limit: usize, output_bytes: usize, recycled_mode: bool, control: ?read_admission.Port, allow_wait: bool) !Workspace {
        while (true) {
            try self.lockWithAdmission(control);
            if (self.active == 0 or bytes <= limit -| self.active_bytes) {
                for (&self.slots) |*slot| if (!slot.busy) {
                    slot.busy = true;
                    self.active += 1;
                    self.active_bytes += bytes;
                    self.peak_active_bytes = @max(self.peak_active_bytes, self.active_bytes);
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
            if (!allow_wait) {
                self.mutex.unlock();
                if (control) |budget| try budget.checkLifetime();
                return error.Backpressured;
            }
            try self.waitLockedWithAdmission(io, control);
        }
    }

    fn canWait(control: ?read_admission.Port) !bool {
        const budget = control orelse return false;
        try budget.checkLifetime();
        return budget.hasExecutionWait();
    }

    pub const Ticket = struct { flight: *Flight, leader: bool };
    /// Only explicitly lendable callers may share a flight. Null/coarse callers
    /// decode independently and try the same bounded workspace pool.
    pub fn beginWithAdmission(self: *Pool, backing: std.mem.Allocator, io: ?std.Io, key: Key, control: ?read_admission.Port) !?Ticket {
        if (!try canWait(control) or !key.generation.valid()) return null;
        while (true) {
            try self.lockWithAdmission(control);
            for (&self.flights) |*flight| {
                if (flight.refs != 0 and !flight.constructing and flight.key.matches(key)) {
                    flight.refs += 1;
                    self.joined += 1;
                    self.mutex.unlock();
                    return .{ .flight = flight, .leader = false };
                }
            }
            for (&self.flights) |*flight| if (flight.refs == 0) {
                // Reserve a finite record first; allocation and its destructor
                // run outside the coordination mutex. Unpublished keys cannot join.
                flight.* = .{ .refs = 1, .constructing = true, .key = key, .backing = backing };
                flight.key.path = &.{};
                self.mutex.unlock();
                errdefer self.releaseFlight(flight);
                const path = try backing.dupe(u8, key.path);
                errdefer backing.free(path);
                try self.lockWithAdmission(control);
                flight.key.path = path;
                flight.constructing = false;
                self.mutex.unlock();
                return .{ .flight = flight, .leader = true };
            };
            try self.waitLockedWithAdmission(io, control);
        }
    }

    pub fn joinWithAdmission(self: *Pool, key: Key, control: ?read_admission.Port) !?*Flight {
        if (!try canWait(control) or !key.generation.valid()) return null;
        try self.lockWithAdmission(control);
        defer self.mutex.unlock();
        for (&self.flights) |*flight| {
            if (flight.refs != 0 and !flight.constructing and flight.key.matches(key)) {
                flight.refs += 1;
                self.joined += 1;
                return flight;
            }
        }
        return null;
    }
    pub fn finish(self: *Pool, flight: *Flight, result: anyerror!*SharedBytes) void {
        platform.sync.lockYielding(&self.mutex);
        if (result) |payload| flight.result = payload.retain() else |_| flight.failed = true;
        flight.completed = true;
        self.mutex.unlock();
    }
    /// Peer failure is not the borrower's authority. The caller may perform one
    /// independent decode, under its original lifetime and workspace admission.
    pub fn waitWithAdmission(self: *Pool, flight: *Flight, control: ?read_admission.Port) !*SharedBytes {
        const lendable = try canWait(control);
        while (true) {
            try self.lockWithAdmission(control);
            if (flight.completed) {
                defer self.mutex.unlock();
                if (flight.failed) return error.LocalDecodeProducerFailed;
                return flight.result.?.retain();
            }
            self.mutex.unlock();
            if (!lendable) return error.Backpressured;
            try control.?.waitForProgress();
        }
    }
    pub fn releaseFlight(self: *Pool, flight: *Flight) void {
        platform.sync.lockYielding(&self.mutex);
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) {
            self.mutex.unlock();
            return;
        }
        const result = flight.result;
        const path = flight.key.path;
        const backing = flight.backing;
        flight.result = null;
        flight.key = undefined;
        self.notifyLocked();
        self.mutex.unlock();
        if (result) |payload| payload.release();
        backing.free(path);
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

test "lsm local decoder streaming scratch funds retained arena backing without in-place growth" {
    const allocators = @import("../lite/test_allocator.zig");
    const Fixture = struct {
        fn run(backing: std.mem.Allocator) !void {
            var no_resize: allocators.NoResizeAllocator = .{ .backing = backing };
            var manager = resources.ResourceManager.init(.{});
            defer manager.deinit(std.testing.allocator);
            var pool: Pool = .{};
            defer pool.deinit();
            {
                var warm = pool.acquire(no_resize.allocator(), &manager, std.testing.io, 256 * 1024, 1024 * 1024, 0);
                defer warm.release();
                _ = try warm.allocator().alloc(u8, 64 * 1024);
            }
            const bound = try Pool.streamingScratchBound(64, 64 * 1024);
            {
                var work = pool.acquire(no_resize.allocator(), &manager, std.testing.io, bound, 1024 * 1024, 0);
                defer work.release();
                const path = try work.allocator().alloc(u8, 64);
                @memset(path, 'p');
                const input = try work.allocator().alloc(u8, 64 * 1024);
                @memset(input, 'x');
                try std.testing.expectEqual(@as(u8, 'p'), path[0]);
                try std.testing.expect(pool.slots[0].cap.live <= bound);
            }
            try std.testing.expectEqual(@as(u64, pool.slots[0].cap.live), manager.sliceStats(.lsm_read_working_set).used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    try std.testing.expectError(error.InvalidTableFile, Pool.streamingScratchBound(std.math.maxInt(usize), 1));
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

fn testBoundary(raw: *anyopaque) @import("../../common/execution_wait.zig").Boundary {
    const Noop = struct {
        fn run(_: *anyopaque) !void {}
    };
    return .{ .ptr = raw, .begin = Noop.run, .end = Noop.run };
}
fn testKey() !Pool.Key {
    return .{ .path = "block", .generation = try read_generation.allocate(), .run_id = 1, .offset = 0, .physical_len = 64, .logical_len = 64, .compression = .none, .checksum = 0, .admit = true };
}

test "lsm local decoder admission cancellation preserves producer and healthy follower" {
    var pool: Pool = .{};
    defer pool.deinit();
    const Budget = struct {
        waits: usize = 0,
        canceled: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
        }
        pub const checkLifetime = checkpoint;
        pub fn executionBoundary(self: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
            return testBoundary(self);
        }
        pub fn waitForProgress(self: *@This()) !void {
            self.waits += 1;
            self.canceled = true;
        }
    };
    var healthy_budget: Budget = .{};
    var healthy_adapter = read_admission.Adapter(*Budget){ .budget = &healthy_budget };
    const healthy_payload = healthy_adapter.payload();
    const port: read_admission.Port = .init(&healthy_payload);
    const key = try testKey();
    const leader = (try pool.beginWithAdmission(std.testing.allocator, null, key, port)).?;
    defer pool.releaseFlight(leader.flight);
    const follower = (try pool.beginWithAdmission(std.testing.allocator, null, key, port)).?;
    var follower_held = true;
    defer if (follower_held) pool.releaseFlight(follower.flight);
    const healthy = (try pool.beginWithAdmission(std.testing.allocator, null, key, port)).?;
    defer pool.releaseFlight(healthy.flight);
    var budget: Budget = .{};
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
    const retained = try pool.waitWithAdmission(healthy.flight, port);
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
        pub const checkLifetime = checkpoint;
        pub fn executionBoundary(self: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
            return testBoundary(self);
        }
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

const TestBudget = struct {
    fail_at: usize = 0,
    checks: usize = 0,
    waits: usize = 0,
    lendable: bool = true,
    pub fn checkpoint(self: *@This()) !void {
        self.checks += 1;
        if (self.fail_at != 0 and self.checks >= self.fail_at) return error.CatalogGenerationChanged;
    }
    pub const checkLifetime = checkpoint;
    pub fn executionBoundary(self: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
        return if (self.lendable) testBoundary(self) else null;
    }
    pub fn waitForProgress(self: *@This()) !void {
        self.waits += 1;
        return error.UnexpectedDecoderWait;
    }
};

test "lsm local decoder admission shares only exact immutable decode contracts" {
    var pool: Pool = .{};
    defer pool.deinit();
    var budget: TestBudget = .{};
    var adapter: read_admission.Adapter(*TestBudget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const port: read_admission.Port = .init(&payload);
    const key = try testKey();
    const leader = (try pool.beginWithAdmission(std.testing.allocator, null, key, port)).?;
    defer pool.releaseFlight(leader.flight);
    for (0..11) |field| {
        var changed = key;
        switch (field) {
            0 => changed.path = "other",
            1 => changed.generation.issuer += 1,
            2 => changed.generation.sequence += 1,
            3 => changed.run_id += 1,
            4 => changed.offset += 1,
            5 => changed.physical_len += 1,
            6 => changed.logical_len += 1,
            7 => changed.compression = .snappy,
            8 => changed.checksum += 1,
            9 => changed.admit = false,
            10 => changed.generation = .{},
            else => unreachable,
        }
        try std.testing.expect(try pool.joinWithAdmission(changed, port) == null);
        if (try pool.beginWithAdmission(std.testing.allocator, null, changed, port)) |independent| {
            try std.testing.expect(independent.leader and independent.flight != leader.flight);
            pool.finish(independent.flight, error.TestIndependentDecode);
            pool.releaseFlight(independent.flight);
        } else try std.testing.expect(field == 10);
    }
    try std.testing.expect(try pool.beginWithAdmission(std.testing.allocator, null, key, null) == null);
    try std.testing.expect(try pool.joinWithAdmission(key, null) == null);
    budget.lendable = false;
    const coarse_payload = adapter.payload();
    const coarse: read_admission.Port = .init(&coarse_payload);
    try std.testing.expect(try pool.beginWithAdmission(std.testing.allocator, null, key, coarse) == null);
    try std.testing.expect(try pool.joinWithAdmission(key, coarse) == null);
    try std.testing.expectError(error.Backpressured, pool.waitWithAdmission(leader.flight, coarse));
    budget.lendable = true;
    const joined = (try pool.joinWithAdmission(key, port)).?;
    defer pool.releaseFlight(joined);
    pool.finish(leader.flight, error.Canceled);
    try std.testing.expectError(error.LocalDecodeProducerFailed, pool.waitWithAdmission(joined, port));
    try std.testing.expectEqual(@as(usize, 0), budget.waits);
}

test "lsm local decoder admission rejected flight construction retires all bounded state" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator, fail_at: usize) !void {
            var pool: Pool = .{};
            defer pool.deinit();
            var budget: TestBudget = .{ .fail_at = fail_at };
            var adapter: read_admission.Adapter(*TestBudget) = .{ .budget = &budget };
            const payload = adapter.payload();
            const result = pool.beginWithAdmission(a, null, try testKey(), .init(&payload));
            if (result) |ticket| {
                if (ticket) |owned| {
                    pool.finish(owned.flight, error.TestEnd);
                    pool.releaseFlight(owned.flight);
                }
                try std.testing.expect(fail_at == 0 or budget.checks < fail_at);
            } else |cause| {
                if (cause == error.OutOfMemory) return cause;
                try std.testing.expectEqual(error.CatalogGenerationChanged, cause);
            }
            for (&pool.flights) |*flight| try std.testing.expectEqual(@as(usize, 0), flight.refs);
            try std.testing.expect(pool.mutex.tryLock());
            pool.mutex.unlock();
        }
        fn allocations(a: std.mem.Allocator) !void {
            try run(a, 0);
        }
    };
    for (1..7) |fail_at| try Fixture.run(std.testing.allocator, fail_at);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.allocations, .{});
}

test "lsm local decoder admission native sole ordinary slot preserves producer and control progress" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const native = @import("../../common/runnable_scheduler.zig");
    const Mode = enum { legacy, coarse, legacy_workspace_full, coarse_workspace_full, coalesced, peer_canceled, producer_canceled, different_generation, flight_full, workspace_wait };
    const Fixture = struct {
        const Self = @This();
        mode: Mode,
        pool: Pool = .{},
        scheduler: *native.Scheduler,
        key: Pool.Key,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        peer_waiting: std.Io.Event = .unset,
        stop: std.atomic.Value(bool) = .init(false),
        fn independent(self: *@This()) bool {
            return switch (self.mode) {
                .legacy, .coarse, .legacy_workspace_full, .coarse_workspace_full, .different_generation, .flight_full => true,
                else => false,
            };
        }
        const Scope = struct {
            f: *Self,
            producer: bool,
            execution: native.Execution = .{},
            failure: ?anyerror = null,
            reject: ?anyerror = null,
            done: std.Io.Event = .unset,
            pub fn checkLifetime(self: *@This()) !void {
                if (self.reject) |cause| return cause;
                if (self.f.stop.load(.acquire)) return error.Canceled;
                try self.execution.checkLifetime();
            }
            pub const checkpoint = checkLifetime;
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.execution.park(.resource_blocked);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.execution.enter(if (self.producer) .background else .write);
                if (self.producer and self.f.mode == .producer_canceled) self.reject = error.Canceled;
                try self.checkLifetime();
            }
            pub fn executionBoundary(self: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
                if (!self.producer and (self.f.mode == .coarse or self.f.mode == .coarse_workspace_full)) return null;
                return .{ .ptr = self, .begin = begin, .end = end };
            }
            pub fn waitForProgress(self: *@This()) !void {
                if (self.producer) return error.UnexpectedDecoderWait;
                if (self.f.independent()) return error.UnexpectedCoarseDecoderWait;
                try begin(self);
                self.f.peer_waiting.set(std.testing.io);
                const result = std.testing.io.sleep(.fromMilliseconds(1), .awake);
                try end(self);
                if (self.f.mode == .peer_canceled) {
                    self.reject = error.Canceled;
                    return error.Canceled;
                }
                try result;
            }
            fn run(self: *@This()) void {
                self.execute() catch |cause| {
                    self.failure = cause;
                };
                self.done.set(std.testing.io);
            }
            fn execute(self: *@This()) !void {
                const now: u64 = @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds);
                try self.execution.start(self.f.scheduler, .{ .io = std.testing.io, .allow_wait = true, .deadline_ns = now + 10 * std.time.ns_per_s, .retained_bytes = if (self.producer) 17 else 23 }, if (self.producer) .background else .write);
                defer self.execution.finish();
                var adapter: read_admission.Adapter(*@This()) = .{ .budget = self };
                const payload = adapter.payload();
                const port: ?read_admission.Port = if (!self.producer and (self.f.mode == .legacy or self.f.mode == .legacy_workspace_full or self.f.mode == .flight_full)) null else .init(&payload);
                var key = self.f.key;
                if (!self.producer and (self.f.mode == .different_generation or self.f.mode == .workspace_wait)) key.generation = try read_generation.allocate();
                if (self.producer) {
                    var tickets: [Pool.flight_count]Pool.Ticket = undefined;
                    var ticket_count: usize = 0;
                    defer for (tickets[0..ticket_count]) |ticket| self.f.pool.releaseFlight(ticket.flight);
                    const n: usize = if (self.f.mode == .flight_full) Pool.flight_count else 1;
                    for (0..n) |i| {
                        var own_key = key;
                        own_key.offset += i;
                        tickets[i] = (try self.f.pool.beginWithAdmission(std.testing.allocator, std.testing.io, own_key, port)).?;
                        ticket_count += 1;
                    }
                    const result = self.produce(port);
                    for (tickets[0..ticket_count]) |ticket| self.f.pool.finish(ticket.flight, result);
                    const value = try result;
                    value.release();
                } else {
                    const ticket = try self.f.pool.beginWithAdmission(std.testing.allocator, std.testing.io, key, port);
                    if (ticket) |owned| {
                        defer self.f.pool.releaseFlight(owned.flight);
                        if (!owned.leader) {
                            const value = try self.f.pool.waitWithAdmission(owned.flight, port);
                            defer value.release();
                            try std.testing.expectEqualStrings("decoded", value.bytes);
                            return;
                        }
                        const result = self.peerDecode(port);
                        self.f.pool.finish(owned.flight, result);
                        const value = try result;
                        value.release();
                    } else {
                        const value = try self.peerDecode(port);
                        value.release();
                    }
                }
            }
            fn produce(self: *@This(), port: ?read_admission.Port) !*SharedBytes {
                var work: [Pool.workspace_count]Pool.Workspace = undefined;
                var count: usize = 0;
                defer for (work[0..count]) |*held| held.release();
                const n: usize = switch (self.f.mode) {
                    .legacy_workspace_full, .coarse_workspace_full, .workspace_wait => Pool.workspace_count,
                    else => 1,
                };
                for (work[0..n]) |*held| {
                    held.* = try self.f.pool.acquireWithAdmission(std.testing.allocator, null, std.testing.io, 64, 1024, 0, port);
                    count += 1;
                }
                try begin(self);
                self.f.entered.set(std.testing.io);
                self.f.release.waitUncancelable(std.testing.io);
                try end(self);
                return SharedBytes.create(std.testing.allocator, try std.testing.allocator.dupe(u8, "decoded"));
            }
            fn peerDecode(self: *@This(), port: ?read_admission.Port) !*SharedBytes {
                var work = try self.f.pool.acquireWithAdmission(std.testing.allocator, null, std.testing.io, 64, 1024, 0, port);
                defer work.release();
                return SharedBytes.create(std.testing.allocator, try std.testing.allocator.dupe(u8, "decoded"));
            }
        };
    };
    const io = std.testing.io;
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    for (std.enums.values(Mode)) |mode| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var f: Fixture = .{ .mode = mode, .scheduler = &scheduler, .key = try testKey() };
        defer f.pool.deinit();
        var producer: Fixture.Scope = .{ .f = &f, .producer = true };
        var peer: Fixture.Scope = .{ .f = &f, .producer = false };
        const producer_thread = try std.Thread.spawn(.{}, Fixture.Scope.run, .{&producer});
        var peer_thread: ?std.Thread = null;
        var joined = false;
        defer if (!joined) {
            f.stop.store(true, .release);
            f.release.set(io);
            producer_thread.join();
            if (peer_thread) |thread| thread.join();
        };
        try f.entered.waitTimeout(io, timeout);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        peer_thread = try std.Thread.spawn(.{}, Fixture.Scope.run, .{&peer});
        if (f.independent()) {
            try peer.done.waitTimeout(io, timeout);
        } else {
            try f.peer_waiting.waitTimeout(io, timeout);
            if (mode == .peer_canceled) try peer.done.waitTimeout(io, timeout) else try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
        }
        var health: native.Execution = .{};
        try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        health.finish();
        f.release.set(io);
        try producer.done.waitTimeout(io, timeout);
        try peer.done.waitTimeout(io, timeout);
        producer_thread.join();
        peer_thread.?.join();
        joined = true;
        try std.testing.expectEqual(@as(?anyerror, if (mode == .producer_canceled) error.Canceled else null), producer.failure);
        const expected: ?anyerror = switch (mode) {
            .legacy_workspace_full, .coarse_workspace_full => error.Backpressured,
            .producer_canceled => error.LocalDecodeProducerFailed,
            .peer_canceled => error.Canceled,
            else => null,
        };
        try std.testing.expectEqual(expected, peer.failure);
        try std.testing.expectEqual(@as(usize, 0), f.pool.active);
        for (&f.pool.flights) |*flight| try std.testing.expectEqual(@as(usize, 0), flight.refs);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}
