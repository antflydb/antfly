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

//! Physical allocation groups for read-lane ownership. Pool and groups remain
//! stable through every allocation, borrowed allocator view and physical join.
const std = @import("std");
const Accounting = @import("allocation_budget.zig").Accounting;

pub const Lane = enum { general, bounded, transition };
const lane_count = @typeInfo(Lane).@"enum".field_names.len;

pub const Limits = struct {
    bytes: [lane_count]usize,
    groups: [lane_count]usize,
};

/// Private read capacity is carved out of, never added to, foreground memory.
/// Half serves demoted reads; bounded and transition each receive one quarter.
pub fn startupLimits(foreground_bytes: usize, maximum_groups: usize) !Limits {
    const total = foreground_bytes / 4;
    if (total < 4 or maximum_groups == 0) return error.InvalidReadMemoryLimits;
    const private = total / 4;
    return .{ .bytes = .{ total - private * 2, private, private }, .groups = @splat(maximum_groups) };
}

pub fn startupReservedBytes(foreground_bytes: usize) usize {
    return foreground_bytes / 4;
}

pub const Pool = struct {
    child: std.mem.Allocator,
    accounting: ?Accounting,
    pinned: usize,
    limits: Limits,
    mutex: std.atomic.Mutex = .unlocked,
    used: [lane_count]usize = @splat(0),
    groups: [lane_count]usize = @splat(0),
    transition_reserved_bytes: usize = 0,
    transition_reserved_groups: usize = 0,
    rejected: u64 = 0,

    pub const Stats = struct {
        used: [lane_count]usize,
        groups: [lane_count]usize,
        transition_reserved_bytes: usize,
        transition_reserved_groups: usize,
        total_used: usize,
        rejected: u64,
    };

    /// Reserve this entire finite envelope once against the node, matching
    /// protected service heaps. Child allocations must not charge it again.
    pub fn init(child: std.mem.Allocator, accounting: ?Accounting, limits: Limits) !Pool {
        var total: usize = 0;
        for (limits.bytes, limits.groups) |bytes, groups| {
            if (bytes == 0 or groups == 0) return error.InvalidReadMemoryLimits;
            total = std.math.add(usize, total, bytes) catch return error.InvalidReadMemoryLimits;
        }
        if (accounting) |owner| if (!owner.reserve(total)) return error.OutOfMemory;
        return .{ .child = child, .accounting = accounting, .pinned = total, .limits = limits };
    }

    fn lock(self: *Pool) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn stats(self: *Pool) Stats {
        self.lock();
        defer self.mutex.unlock();
        var total: usize = 0;
        for (self.used) |bytes| total += bytes;
        return .{ .used = self.used, .groups = self.groups, .transition_reserved_bytes = self.transition_reserved_bytes, .transition_reserved_groups = self.transition_reserved_groups, .total_used = total, .rejected = self.rejected };
    }

    pub fn deinit(self: *Pool) void {
        const observed = self.stats();
        std.debug.assert(observed.total_used == 0 and observed.transition_reserved_bytes == 0 and observed.transition_reserved_groups == 0);
        for (observed.groups) |count| std.debug.assert(count == 0);
        if (self.accounting) |owner| owner.release(self.pinned);
        self.* = undefined;
    }
};

pub const Group = struct {
    pool: *Pool,
    lane: Lane,
    limit: usize,
    used: usize = 0,
    active_allocator_operations: usize = 0,
    transition_reserved: bool = false,
    closed: bool = false,
    allocation_started: bool = false,

    const Header = struct { group: *Group };

    pub fn overhead(alignment: std.mem.Alignment) usize {
        return alignment.max(.of(Header)).forward(@sizeOf(Header));
    }

    /// Creation alone publishes no allocator pointer. Move to its final address
    /// before obtaining allocator views or allocating the first resource.
    pub fn init(pool: *Pool, lane: Lane, limit: usize) !Group {
        if (lane == .transition or limit == 0 or limit > pool.limits.bytes[@backingInt(lane)]) return error.InvalidReadMemoryLimits;
        pool.lock();
        defer pool.mutex.unlock();
        const index = @backingInt(lane);
        if (pool.groups[index] == pool.limits.groups[index]) return error.ReadMemoryCapacityUnavailable;
        pool.groups[index] += 1;
        return .{ .pool = pool, .lane = lane, .limit = limit };
    }

    /// All continuation and cleanup scratch must fit this declared maximum.
    /// Reserve before the first allocation, not after ordinary memory fills.
    pub fn reserveTransition(self: *Group) !void {
        const pool = self.pool;
        pool.lock();
        defer pool.mutex.unlock();
        if (self.closed or self.lane != .bounded or self.allocation_started or self.used != 0 or self.active_allocator_operations != 0 or self.transition_reserved) return error.InvalidReadMemoryTransition;
        const index = @backingInt(Lane.transition);
        if (pool.transition_reserved_groups >= pool.limits.groups[index] or self.limit > pool.limits.bytes[index] - pool.transition_reserved_bytes) return error.ReadMemoryCapacityUnavailable;
        pool.transition_reserved_groups += 1;
        pool.transition_reserved_bytes += self.limit;
        self.transition_reserved = true;
    }

    pub fn beginTransition(self: *Group) !void {
        const pool = self.pool;
        pool.lock();
        defer pool.mutex.unlock();
        if (self.closed or self.lane != .bounded or !self.transition_reserved) return error.InvalidReadMemoryTransition;
        if (self.active_allocator_operations != 0) return error.ReadMemoryBusy;
        self.moveLocked(.transition);
    }

    /// Refusal changes no ownership or capacity counters. A successful transfer
    /// changes the allocator group's owner, never the live buffers themselves.
    pub fn tryEnterGeneral(self: *Group) !bool {
        const pool = self.pool;
        pool.lock();
        defer pool.mutex.unlock();
        if (self.closed or self.lane != .transition or !self.transition_reserved) return error.InvalidReadMemoryTransition;
        if (self.active_allocator_operations != 0) return error.ReadMemoryBusy;
        const index = @backingInt(Lane.general);
        if (pool.groups[index] >= pool.limits.groups[index] or self.used > pool.limits.bytes[index] - pool.used[index]) return false;
        self.moveLocked(.general);
        self.releaseTransitionLocked();
        return true;
    }

    fn moveLocked(self: *Group, target: Lane) void {
        const pool = self.pool;
        const from = @backingInt(self.lane);
        const to = @backingInt(target);
        pool.used[from] -= self.used;
        pool.groups[from] -= 1;
        pool.used[to] += self.used;
        pool.groups[to] += 1;
        self.lane = target;
    }

    fn releaseTransitionLocked(self: *Group) void {
        if (!self.transition_reserved) return;
        self.pool.transition_reserved_bytes -= self.limit;
        self.pool.transition_reserved_groups -= 1;
        self.transition_reserved = false;
    }

    pub fn deinit(self: *Group) void {
        const pool = self.pool;
        pool.lock();
        defer pool.mutex.unlock();
        std.debug.assert(!self.closed and self.used == 0 and self.active_allocator_operations == 0);
        self.releaseTransitionLocked();
        pool.groups[@backingInt(self.lane)] -= 1;
        self.closed = true;
    }

    /// Compatible views from this pool can free/resize buffers after ownership
    /// changes. Each allocation header names its original stable group.
    pub fn allocator(self: *Group) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn beginOperation(self: *Group, growth: usize) bool {
        const pool = self.pool;
        pool.lock();
        defer pool.mutex.unlock();
        const index = @backingInt(self.lane);
        // Transition is a suspended continuation, not another execution lane.
        // Cleanup scratch must have been allocated before publication.
        if (self.closed or (self.lane == .transition and growth != 0) or growth > self.limit - self.used or growth > pool.limits.bytes[index] - pool.used[index]) {
            pool.rejected +|= 1;
            return false;
        }
        if (growth != 0) self.allocation_started = true;
        self.active_allocator_operations += 1;
        self.used += growth;
        pool.used[index] += growth;
        return true;
    }

    fn endOperation(self: *Group, release_bytes: usize) void {
        const pool = self.pool;
        pool.lock();
        defer pool.mutex.unlock();
        std.debug.assert(self.active_allocator_operations != 0 and release_bytes <= self.used);
        self.used -= release_bytes;
        pool.used[@backingInt(self.lane)] -= release_bytes;
        self.active_allocator_operations -= 1;
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const group: *Group = @ptrCast(@alignCast(raw));
        const prefix = overhead(alignment);
        const size = std.math.add(usize, len, prefix) catch return null;
        if (!group.beginOperation(size)) return null;
        const base = group.pool.child.rawAlloc(size, alignment.max(.of(Header)), ra) orelse {
            group.endOperation(size);
            return null;
        };
        const header: *Header = @ptrCast(@alignCast(base));
        header.* = .{ .group = group };
        group.endOperation(0);
        return base + prefix;
    }

    fn original(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment) *Group {
        const requested: *Group = @ptrCast(@alignCast(raw));
        const header: *Header = @ptrCast(@alignCast(bytes.ptr - overhead(alignment)));
        std.debug.assert(header.group.pool == requested.pool);
        return header.group;
    }

    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const group = original(raw, bytes, alignment);
        const prefix = overhead(alignment);
        const size = std.math.add(usize, len, prefix) catch return false;
        const growth = len -| bytes.len;
        if (!group.beginOperation(growth)) return false;
        const base = bytes.ptr - prefix;
        const ok = group.pool.child.rawResize(base[0 .. bytes.len + prefix], alignment.max(.of(Header)), size, ra);
        group.endOperation(if (ok) bytes.len -| len else growth);
        return ok;
    }

    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const group = original(raw, bytes, alignment);
        const prefix = overhead(alignment);
        const size = std.math.add(usize, len, prefix) catch return null;
        const growth = len -| bytes.len;
        if (!group.beginOperation(growth)) return null;
        const base = bytes.ptr - prefix;
        const moved = group.pool.child.rawRemap(base[0 .. bytes.len + prefix], alignment.max(.of(Header)), size, ra) orelse {
            group.endOperation(growth);
            return null;
        };
        group.endOperation(bytes.len -| len);
        return moved + prefix;
    }

    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const group = original(raw, bytes, alignment);
        const prefix = overhead(alignment);
        std.debug.assert(group.beginOperation(0));
        group.pool.child.rawFree((bytes.ptr - prefix)[0 .. bytes.len + prefix], alignment.max(.of(Header)), ra);
        group.endOperation(bytes.len + prefix);
    }
};

test "workload read memory transfers live buffers without copying or duplicate node charges" {
    const Node = struct {
        available: usize = 192,
        fn reserve(raw: *anyopaque, bytes: usize) callconv(.c) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (bytes > self.available) return false;
            self.available -= bytes;
            return true;
        }
        fn release(raw: *anyopaque, bytes: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.available += bytes;
        }
    };
    var node: Node = .{};
    {
        var pool = try Pool.init(std.testing.allocator, .{ .ptr = &node, .reserve_fn = Node.reserve, .release_fn = Node.release }, .{ .bytes = .{ 64, 64, 64 }, .groups = .{ 2, 2, 1 } });
        defer pool.deinit();
        var general = try Group.init(&pool, .general, 64);
        defer general.deinit();
        var read = try Group.init(&pool, .bounded, 64);
        defer read.deinit();
        try read.reserveTransition();
        const ordinary = try general.allocator().alloc(u8, 64 - Group.overhead(.of(u8)));
        const bytes = try read.allocator().dupe(u8, "fixed retained continuation across scheduling lanes");
        const address = bytes.ptr;
        const before = pool.stats().total_used;
        try read.beginTransition();
        try std.testing.expectEqual(before, pool.stats().total_used);
        try std.testing.expect(!try read.tryEnterGeneral());
        try std.testing.expect(bytes.ptr == address);
        try std.testing.expectEqualStrings("fixed retained continuation across scheduling lanes", bytes);
        try std.testing.expectEqual(@as(usize, 0), node.available);
        var peer = try Group.init(&pool, .bounded, 64);
        defer peer.deinit();
        const peer_bytes = try peer.allocator().alloc(u8, 64 - Group.overhead(.of(u8)));
        // A compatible allocator view retires the original allocation group.
        read.allocator().free(ordinary);
        try std.testing.expect(try read.tryEnterGeneral());
        try std.testing.expect(bytes.ptr == address);
        try std.testing.expectEqual(@as(usize, 0), pool.stats().transition_reserved_bytes);
        general.allocator().free(bytes);
        read.allocator().free(peer_bytes);
        try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
        try std.testing.expectEqual(@as(usize, 0), node.available);
    }
    try std.testing.expectEqual(@as(usize, 192), node.available);
}

test "workload read memory reserves transition before resources and bounds every owner" {
    var pool = try Pool.init(std.testing.allocator, null, .{ .bytes = .{ 64, 128, 64 }, .groups = .{ 1, 2, 1 } });
    defer pool.deinit();
    var first = try Group.init(&pool, .bounded, 64);
    defer first.deinit();
    var second = try Group.init(&pool, .bounded, 64);
    defer second.deinit();
    try first.reserveTransition();
    try std.testing.expectError(error.ReadMemoryCapacityUnavailable, second.reserveTransition());
    const bytes = try second.allocator().alloc(u8, 8);
    second.allocator().free(bytes);
    try std.testing.expectError(error.InvalidReadMemoryTransition, second.reserveTransition());
    try std.testing.expectError(error.InvalidReadMemoryTransition, second.beginTransition());
    try std.testing.expectError(error.InvalidReadMemoryLimits, Group.init(&pool, .transition, 64));
    const held = try first.allocator().alloc(u8, 64 - Group.overhead(.of(u8)));
    try first.beginTransition();
    try std.testing.expectError(error.OutOfMemory, first.allocator().alloc(u8, 1));
    try std.testing.expect(!first.allocator().resize(held, held.len + 1));
    first.allocator().free(held);
    try std.testing.expect(try first.tryEnterGeneral());
    try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
}

test "workload read memory rolls back failed allocation resize and remap" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    var pool = try Pool.init(failing.allocator(), null, .{ .bytes = .{ 256, 128, 128 }, .groups = .{ 2, 2, 2 } });
    defer pool.deinit();
    var group = try Group.init(&pool, .bounded, 128);
    defer group.deinit();
    try group.reserveTransition();
    const bytes = try group.allocator().dupe(u8, "retained");
    const original_used = pool.stats().total_used;
    try std.testing.expect(!group.allocator().resize(bytes, 32));
    try std.testing.expect(group.allocator().remap(bytes, 32) == null);
    try std.testing.expectEqual(original_used, pool.stats().total_used);
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, group.allocator().alloc(u8, 1));
    try std.testing.expectEqual(original_used, pool.stats().total_used);
    try group.beginTransition();
    try std.testing.expect(try group.tryEnterGeneral());
    try std.testing.expectEqualStrings("retained", bytes);
    group.allocator().free(bytes);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
}

test "workload read memory preserves alignment and original owner through shrink and zero remap" {
    var pool = try Pool.init(std.testing.allocator, null, .{ .bytes = .{ 256, 128, 128 }, .groups = .{ 2, 2, 2 } });
    defer pool.deinit();
    var read = try Group.init(&pool, .bounded, 128);
    defer read.deinit();
    var other = try Group.init(&pool, .general, 128);
    defer other.deinit();
    try read.reserveTransition();
    var bytes = try read.allocator().alignedAlloc(u8, .fromByteUnits(64), 24);
    var live = true;
    defer if (live) read.allocator().free(bytes);
    @memset(bytes, 7);
    try std.testing.expect(@intFromPtr(bytes.ptr) % 64 == 0);
    try read.beginTransition();
    if (other.allocator().resize(bytes, 8)) bytes = bytes[0..8];
    try std.testing.expectEqual(bytes.len + 64, pool.stats().used[@backingInt(Lane.transition)]);
    try std.testing.expect(try read.tryEnterGeneral());
    try std.testing.expectEqual(@as(u8, 7), bytes[0]);
    try std.testing.expect(other.allocator().remap(bytes, 0) != null);
    live = false;
    try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
}

test "workload read memory transfer cannot race physical allocator operations" {
    const Gate = struct {
        io: std.Io,
        mode: enum { alloc, free },
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        fn pause(self: *@This()) void {
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) self.io.sleep(.fromMilliseconds(1), .awake) catch break;
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.mode == .alloc) self.pause();
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.mode == .free) self.pause();
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn awaitEntered(self: *@This()) !void {
            const until = std.Io.Clock.awake.now(self.io).addDuration(.fromSeconds(2));
            while (!self.entered.load(.acquire)) {
                if (std.Io.Clock.awake.now(self.io).nanoseconds >= until.nanoseconds) return error.AllocatorDidNotStart;
                try self.io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    const Job = struct {
        alloc: std.mem.Allocator,
        bytes: ?[]u8 = null,
        failure: ?anyerror = null,
        fn allocate(self: *@This()) void {
            self.bytes = self.alloc.alloc(u8, 24) catch |err| {
                self.failure = err;
                return;
            };
        }
        fn retire(self: *@This()) void {
            self.alloc.free(self.bytes.?);
            self.bytes = null;
        }
    };
    const io = std.testing.io;
    var gate: Gate = .{ .io = io, .mode = .alloc };
    var pool = try Pool.init(gate.allocator(), null, .{ .bytes = .{ 128, 128, 128 }, .groups = .{ 2, 2, 2 } });
    defer pool.deinit();
    var read = try Group.init(&pool, .bounded, 128);
    defer read.deinit();
    try read.reserveTransition();
    var job: Job = .{ .alloc = read.allocator() };
    {
        var future = try io.concurrent(Job.allocate, .{&job});
        defer {
            gate.release.store(true, .release);
            future.cancel(io);
        }
        try gate.awaitEntered();
        try std.testing.expectError(error.ReadMemoryBusy, read.beginTransition());
        try std.testing.expectEqual(Lane.bounded, read.lane);
        gate.release.store(true, .release);
        future.await(io);
    }
    defer if (job.bytes) |bytes| read.allocator().free(bytes);
    if (job.failure) |cause| return cause;
    try read.beginTransition();
    gate.mode = .free;
    gate.entered.store(false, .release);
    gate.release.store(false, .release);
    {
        var future = try io.concurrent(Job.retire, .{&job});
        defer {
            gate.release.store(true, .release);
            future.cancel(io);
        }
        try gate.awaitEntered();
        try std.testing.expectError(error.ReadMemoryBusy, read.tryEnterGeneral());
        try std.testing.expectEqual(@as(usize, 24 + Group.overhead(.of(u8))), pool.stats().total_used);
        gate.release.store(true, .release);
        future.await(io);
    }
    try std.testing.expect(try read.tryEnterGeneral());
    try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().transition_reserved_bytes);
}

test "workload read memory capacity refusal preserves counters and permits reuse" {
    var pool = try Pool.init(std.testing.allocator, null, .{ .bytes = .{ 128, 128, 128 }, .groups = .{ 1, 2, 2 } });
    defer pool.deinit();
    {
        var first = try Group.init(&pool, .general, 128);
        defer first.deinit();
        const before = pool.stats();
        try std.testing.expectError(error.ReadMemoryCapacityUnavailable, Group.init(&pool, .general, 128));
        try std.testing.expectEqualDeep(before, pool.stats());
    }
    var replacement = try Group.init(&pool, .general, 128);
    defer replacement.deinit();

    var peer = try Group.init(&pool, .bounded, 64);
    defer peer.deinit();
    {
        var bounded = try Group.init(&pool, .bounded, 96);
        defer bounded.deinit();
        try bounded.reserveTransition();
        const reserved = pool.stats();
        // A second transition group exists, but its byte reservation cannot fit.
        try std.testing.expectError(error.ReadMemoryCapacityUnavailable, peer.reserveTransition());
        try std.testing.expectEqualDeep(reserved, pool.stats());
    }
    try peer.reserveTransition();
    try std.testing.expectEqual(@as(usize, 64), pool.stats().transition_reserved_bytes);
}
