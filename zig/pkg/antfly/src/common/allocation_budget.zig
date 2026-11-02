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

//! Concurrent allocation accounting. Owners must remain at stable addresses
//! until all allocations and joined operations are retired.
const std = @import("std");

/// Borrowed node owner. Callbacks cross native runtime partitions without an
/// error union; the owner must outlive every charged allocation and join.
pub const Accounting = struct {
    pub const TryReserve = enum(c_int) { reserved, busy, denied, unsupported };
    ptr: *anyopaque,
    /// Bounded scheduler-safe attempt. Refusal leaves the node ledger unchanged.
    try_reserve_fn: ?*const fn (*anyopaque, usize) callconv(.c) TryReserve = null,
    reserve_fn: *const fn (*anyopaque, usize) callconv(.c) bool,
    release_fn: *const fn (*anyopaque, usize) callconv(.c) void,

    pub fn reserve(self: Accounting, bytes: usize) bool {
        return self.reserve_fn(self.ptr, bytes);
    }
    pub fn release(self: Accounting, bytes: usize) void {
        self.release_fn(self.ptr, bytes);
    }
};

pub const Shared = struct {
    child: std.mem.Allocator,
    accounting: ?Accounting = null,
    limit: usize,
    used: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),
    rejected: std.atomic.Value(u64) = .init(0),

    pub const Stats = struct { limit_bytes: usize, used_bytes: usize, peak_bytes: usize, rejected_total: u64 };

    pub fn stats(self: *const Shared) Stats {
        return .{ .limit_bytes = self.limit, .used_bytes = self.used.load(.acquire), .peak_bytes = self.peak.load(.acquire), .rejected_total = self.rejected.load(.acquire) };
    }

    pub fn allocator(self: *Shared) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn reserve(self: *Shared, bytes: usize) bool {
        var observed = self.used.load(.acquire);
        while (observed <= self.limit and bytes <= self.limit - observed) {
            if (self.used.cmpxchgWeak(observed, observed + bytes, .acq_rel, .acquire)) |changed| observed = changed else {
                if (self.accounting) |owner| if (!owner.reserve(bytes)) {
                    _ = self.used.fetchSub(bytes, .acq_rel);
                    _ = self.rejected.fetchAdd(1, .monotonic);
                    return false;
                };
                var peak = self.peak.load(.acquire);
                while (peak < observed + bytes) {
                    peak = self.peak.cmpxchgWeak(peak, observed + bytes, .acq_rel, .acquire) orelse break;
                }
                return true;
            }
        }
        _ = self.rejected.fetchAdd(1, .monotonic);
        return false;
    }

    /// One bounded local CAS followed by one bounded node attempt. Never uses
    /// the ordinary allocator reservation callback under a scheduler lock.
    pub fn tryReserve(self: *Shared, bytes: usize) Accounting.TryReserve {
        const attempt = if (self.accounting) |owner| owner.try_reserve_fn orelse return .unsupported else null;
        const observed = self.used.load(.acquire);
        if (observed > self.limit or bytes > self.limit - observed) return .denied;
        if (self.used.cmpxchgStrong(observed, observed + bytes, .acq_rel, .acquire) != null) return .busy;
        if (attempt) |callback| {
            const result = callback(self.accounting.?.ptr, bytes);
            if (result != .reserved) {
                _ = self.used.fetchSub(bytes, .acq_rel);
                return result;
            }
        }
        // Statistics are best effort here: a CAS retry loop is not bounded.
        const peak = self.peak.load(.acquire);
        if (peak < observed + bytes) _ = self.peak.cmpxchgStrong(peak, observed + bytes, .acq_rel, .acquire);
        return .reserved;
    }

    pub fn release(self: *Shared, bytes: usize) void {
        const previous = self.used.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
        if (self.accounting) |owner| owner.release(bytes);
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Shared = @ptrCast(@alignCast(raw));
        if (!self.reserve(len)) return null;
        return self.child.rawAlloc(len, alignment, ret_addr) orelse {
            self.release(len);
            return null;
        };
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Shared = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (!self.reserve(growth)) return false;
        if (!self.child.rawResize(memory, alignment, new_len, ret_addr)) {
            self.release(growth);
            return false;
        }
        self.release(memory.len -| new_len);
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Shared = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (!self.reserve(growth)) return null;
        const result = self.child.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            self.release(growth);
            return null;
        };
        self.release(memory.len -| new_len);
        return result;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Shared = @ptrCast(@alignCast(raw));
        self.child.rawFree(memory, alignment, ret_addr);
        self.release(memory.len);
    }
};

/// One optional fixed operator allocation. Prepare at a stable address before
/// dispatch. Acquisition charges partition and node once; physical allocation
/// consumes that credit directly. Retire only after the operator frees its
/// buffer and the scheduler has detached the participant. No concurrent users.
pub const Fixed = struct {
    budget: *Shared,
    bytes: usize,
    reserved: bool = false,
    owner: ?*Fixed = null,
    live: ?[]u8 = null,

    pub fn tryAcquire(self: *Fixed) bool {
        std.debug.assert(!self.reserved and (self.owner == null or self.owner == self));
        self.owner = self;
        self.reserved = self.budget.tryReserve(self.bytes) == .reserved;
        return self.reserved;
    }
    pub fn deinit(self: *Fixed) void {
        std.debug.assert(self.live == null and (self.owner == null or self.owner == self));
        if (self.reserved) self.budget.release(self.bytes);
        self.reserved = false;
    }
    pub fn allocator(self: *Fixed) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Fixed = @ptrCast(@alignCast(raw));
        if (self.owner != self or !self.reserved or self.live != null or len > self.bytes) return null;
        const result = self.budget.child.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.live = result[0..len];
        return result;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Fixed = @ptrCast(@alignCast(raw));
        const live = self.live orelse unreachable;
        std.debug.assert(memory.ptr == live.ptr and memory.len == live.len);
        self.budget.child.rawFree(memory, alignment, ret_addr);
        self.live = null;
    }
};

/// One envelope with reserved control capacity. Both views can free allocations
/// from either partition: transaction registries transfer objects across request
/// lifetimes, so allocator selection must never change their destruction contract.
pub const Partitioned = struct {
    foreground: Shared,
    control: Shared,
    retained: ?Shared = null,
    /// Trusted node consumers only. Its capacity is carved out of an existing
    /// prepaid control reserve, never added to the process envelope.
    cleanup: ?Shared = null,

    const Kind = enum(u8) { foreground, control, retained, cleanup };
    const Header = struct { kind: Kind };

    pub fn overhead(alignment: std.mem.Alignment) usize {
        return alignment.max(.of(Header)).forward(@sizeOf(Header));
    }

    pub fn allocator(self: *Partitioned, control: bool) std.mem.Allocator {
        return .{ .ptr = self, .vtable = if (control) &control_vtable else &foreground_vtable };
    }

    pub fn retainedAllocator(self: *Partitioned) std.mem.Allocator {
        std.debug.assert(self.retained != null);
        return .{ .ptr = self, .vtable = &retained_vtable };
    }
    pub fn cleanupAllocator(self: *Partitioned) std.mem.Allocator {
        std.debug.assert(self.cleanup != null);
        return .{ .ptr = self, .vtable = &cleanup_vtable };
    }
    fn partitionBudget(self: *Partitioned, kind: Kind) *Shared {
        return switch (kind) {
            .foreground => &self.foreground,
            .control => &self.control,
            .retained => &self.retained.?,
            .cleanup => &self.cleanup.?,
        };
    }
    const retained_vtable: std.mem.Allocator.VTable = .{ .alloc = allocRetained, .resize = resize, .remap = remap, .free = free };
    fn allocRetained(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocate(raw, len, alignment, ra, .retained);
    }

    const cleanup_vtable: std.mem.Allocator.VTable = .{ .alloc = allocCleanup, .resize = resize, .remap = remap, .free = free };
    fn allocCleanup(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocate(raw, len, alignment, ra, .cleanup);
    }

    const foreground_vtable: std.mem.Allocator.VTable = .{ .alloc = allocForeground, .resize = resize, .remap = remap, .free = free };
    const control_vtable: std.mem.Allocator.VTable = .{ .alloc = allocControl, .resize = resize, .remap = remap, .free = free };

    fn allocForeground(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocate(raw, len, alignment, ra, .foreground);
    }
    fn allocControl(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocate(raw, len, alignment, ra, .control);
    }
    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize, kind: Kind) ?[*]u8 {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const size = std.math.add(usize, len, overhead(alignment)) catch return null;
        const budget = self.partitionBudget(kind);
        const base = budget.allocator().rawAlloc(size, alignment.max(.of(Header)), ra) orelse return null;
        const header: *Header = @ptrCast(@alignCast(base));
        header.* = .{ .kind = kind };
        return base + overhead(alignment);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const prefix = overhead(alignment);
        const base = bytes.ptr - prefix;
        const header: *Header = @ptrCast(@alignCast(base));
        const budget = self.partitionBudget(header.kind);
        const size = std.math.add(usize, len, prefix) catch return false;
        return budget.allocator().rawResize(base[0 .. bytes.len + prefix], alignment.max(.of(Header)), size, ra);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const prefix = overhead(alignment);
        const base = bytes.ptr - prefix;
        const header: *Header = @ptrCast(@alignCast(base));
        const budget = self.partitionBudget(header.kind);
        const size = std.math.add(usize, len, prefix) catch return null;
        const moved = budget.allocator().rawRemap(base[0 .. bytes.len + prefix], alignment.max(.of(Header)), size, ra) orelse return null;
        return moved + prefix;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const prefix = overhead(alignment);
        const base = bytes.ptr - prefix;
        const header: *Header = @ptrCast(@alignCast(base));
        const budget = self.partitionBudget(header.kind);
        budget.allocator().rawFree(base[0 .. bytes.len + prefix], alignment.max(.of(Header)), ra);
    }
};

/// Service-owned heaps retain their allocator until every client and stream
/// joins. Foreground growth charges the node; control capacity is pinned once.
pub const ServiceMemory = struct {
    memory: Partitioned,
    owner: Accounting,
    pinned: usize,

    pub fn init(child: std.mem.Allocator, owner: Accounting, foreground_bytes: usize, control_bytes: usize) !ServiceMemory {
        if (!owner.reserve(control_bytes)) return error.OutOfMemory;
        return .{
            .memory = .{
                .foreground = .{ .child = child, .accounting = owner, .limit = foreground_bytes },
                .control = .{ .child = child, .limit = control_bytes },
            },
            .owner = owner,
            .pinned = control_bytes,
        };
    }

    pub fn deinit(self: *ServiceMemory) void {
        std.debug.assert(self.memory.foreground.used.load(.acquire) == 0);
        std.debug.assert(self.memory.control.used.load(.acquire) == 0);
        if (self.memory.cleanup) |*cleanup| std.debug.assert(cleanup.used.load(.acquire) == 0);
        self.owner.release(self.pinned);
        self.* = undefined;
    }
};

test "workload envelope preserves allocation provenance across control ownership transfers" {
    var memory: Partitioned = .{
        .foreground = .{ .child = std.testing.allocator, .limit = 32 },
        .control = .{ .child = std.testing.allocator, .limit = 32 },
    };
    const foreground = memory.allocator(false);
    const control = memory.allocator(true);
    const first = try foreground.alloc(u8, 32 - Partitioned.overhead(.of(u8)));
    try std.testing.expectError(error.OutOfMemory, foreground.alloc(u8, 1));
    const second = try control.alloc(u8, 16);
    // Registry cleanup can run through the other partition's allocator view.
    control.free(first);
    foreground.free(second);
    try std.testing.expectEqual(@as(usize, 0), memory.foreground.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.control.used.load(.acquire));
}

test "workload envelope node accounting rolls back allocation and growth failures" {
    const Owner = struct {
        remaining: usize = 20,
        fn reserve(raw: *anyopaque, bytes: usize) callconv(.c) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (bytes > self.remaining) return false;
            self.remaining -= bytes;
            return true;
        }
        fn release(raw: *anyopaque, bytes: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.remaining += bytes;
        }
    };
    var owner: Owner = .{};
    var memory: Shared = .{ .child = std.testing.allocator, .limit = 100, .accounting = .{ .ptr = &owner, .reserve_fn = Owner.reserve, .release_fn = Owner.release } };
    const alloc = memory.allocator();
    const bytes = try alloc.alloc(u8, 20);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 1));
    try std.testing.expect(!alloc.resize(bytes, 21));
    try std.testing.expectEqual(@as(usize, 20), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.remaining);
    alloc.free(bytes);
    try std.testing.expectEqual(@as(usize, 20), owner.remaining);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var denied: Shared = .{ .child = failing.allocator(), .limit = 100, .accounting = memory.accounting };
    try std.testing.expectError(error.OutOfMemory, denied.allocator().alloc(u8, 10));
    try std.testing.expectEqual(@as(usize, 20), owner.remaining);
    try std.testing.expectEqual(@as(usize, 0), denied.stats().used_bytes);
}

test "workload envelope service control survives node exhaustion and returns its pin after joins" {
    const Owner = struct {
        used: usize = 0,
        fn reserve(ptr: *anyopaque, bytes: usize) callconv(.c) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (bytes > 128 - self.used) return false;
            self.used += bytes;
            return true;
        }
        fn release(ptr: *anyopaque, bytes: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.used >= bytes);
            self.used -= bytes;
        }
    };
    var owner: Owner = .{};
    const port: Accounting = .{ .ptr = &owner, .reserve_fn = Owner.reserve, .release_fn = Owner.release };
    var service = try ServiceMemory.init(std.testing.allocator, port, 96, 32);
    const foreground = service.memory.allocator(false);
    const control = service.memory.allocator(true);
    const ordinary = try foreground.alloc(u8, 96 - Partitioned.overhead(.of(u8)));
    try std.testing.expectEqual(@as(usize, 128), owner.used);
    try std.testing.expectError(error.OutOfMemory, foreground.alloc(u8, 1));
    const acknowledgement = try control.alloc(u8, 16);
    try std.testing.expectEqual(@as(usize, 128), owner.used);
    try std.testing.expectError(error.OutOfMemory, control.alloc(u8, 32));
    // Returned responses may be destroyed through the opposite allocator view.
    foreground.free(acknowledgement);
    control.free(ordinary);
    try std.testing.expectEqual(@as(usize, 32), owner.used);
    service.deinit();
    try std.testing.expectEqual(@as(usize, 0), owner.used);
    try std.testing.expectError(error.OutOfMemory, ServiceMemory.init(std.testing.allocator, port, 96, 129));
    try std.testing.expectEqual(@as(usize, 0), owner.used);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var failed = try ServiceMemory.init(failing.allocator(), port, 96, 32);
    try std.testing.expectError(error.OutOfMemory, failed.memory.allocator(false).alloc(u8, 16));
    try std.testing.expectEqual(@as(usize, 32), owner.used);
    failed.deinit();
    try std.testing.expectEqual(@as(usize, 0), owner.used);
}

test "workload envelope optional fixed scratch consumes one local and node charge through physical free" {
    const Node = struct {
        used: usize = 0,
        result: Accounting.TryReserve = .reserved,
        fn reserve(_: *anyopaque, _: usize) callconv(.c) bool {
            @panic("bounded scratch called blocking accounting");
        }
        fn attempt(raw: *anyopaque, bytes: usize) callconv(.c) Accounting.TryReserve {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.result == .reserved) self.used += bytes;
            return self.result;
        }
        fn release(raw: *anyopaque, bytes: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.used -= bytes;
        }
    };
    var node: Node = .{};
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var budget: Shared = .{ .child = backing.allocator(), .limit = 20, .accounting = .{ .ptr = &node, .reserve_fn = Node.reserve, .try_reserve_fn = Node.attempt, .release_fn = Node.release } };
    for ([_]Accounting.TryReserve{ .busy, .denied, .unsupported }) |refusal| {
        node.result = refusal;
        try std.testing.expectEqual(refusal, budget.tryReserve(20));
        try std.testing.expectEqual(@as(usize, 0), node.used);
        try std.testing.expectEqual(@as(usize, 0), budget.stats().used_bytes);
    }
    node.result = .reserved;
    var ticket: Fixed = .{ .budget = &budget, .bytes = 20 };
    try std.testing.expect(ticket.tryAcquire());
    try std.testing.expectEqual(Accounting.TryReserve.denied, budget.tryReserve(1));
    const bytes = try ticket.allocator().alloc(u8, 20);
    try std.testing.expectError(error.OutOfMemory, ticket.allocator().alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 20), node.used);
    try std.testing.expectEqual(@as(usize, 20), budget.stats().used_bytes);
    ticket.allocator().free(bytes);
    // Credit remains owned until the whole optional stage is retired.
    try std.testing.expectEqual(@as(usize, 20), node.used);
    ticket.deinit();
    try std.testing.expectEqual(@as(usize, 0), node.used);
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    backing.fail_index = backing.alloc_index;
    try std.testing.expect(ticket.tryAcquire());
    try std.testing.expectError(error.OutOfMemory, ticket.allocator().alloc(u8, 20));
    ticket.deinit();
    try std.testing.expectEqual(@as(usize, 0), node.used);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().used_bytes);
    budget.accounting.?.try_reserve_fn = null;
    try std.testing.expectEqual(Accounting.TryReserve.unsupported, budget.tryReserve(1));
}

test "workload envelope cleanup reserve survives ordinary control and node exhaustion with original allocation provenance" {
    const Owner = struct {
        used: usize = 0,
        fn reserve(ptr: *anyopaque, bytes: usize) callconv(.c) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (bytes > 128 - self.used) return false;
            self.used += bytes;
            return true;
        }
        fn release(ptr: *anyopaque, bytes: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.used >= bytes);
            self.used -= bytes;
        }
    };
    var owner: Owner = .{};
    var service = try ServiceMemory.init(std.testing.allocator, .{ .ptr = &owner, .reserve_fn = Owner.reserve, .release_fn = Owner.release }, 96, 32);
    defer service.deinit();
    service.memory.control.limit = 16;
    service.memory.cleanup = .{ .child = std.testing.allocator, .limit = 16 };
    const foreground = service.memory.allocator(false);
    const control = service.memory.allocator(true);
    const cleanup = service.memory.cleanupAllocator();
    const prefix = Partitioned.overhead(.of(u8));
    const data = try foreground.alloc(u8, 96 - prefix);
    defer cleanup.free(data);
    const request = try control.alloc(u8, 16 - prefix);
    try std.testing.expectEqual(@as(usize, 128), owner.used);
    try std.testing.expectError(error.OutOfMemory, foreground.alloc(u8, 1));
    try std.testing.expectError(error.OutOfMemory, control.alloc(u8, 1));
    const retirement = try cleanup.alloc(u8, 16 - prefix);
    try std.testing.expectEqual(@as(usize, 128), owner.used);
    // Even a resize through an ordinary view stays charged to cleanup. It may
    // not consume ordinary control space after that request physically frees.
    foreground.free(request);
    try std.testing.expect(!control.resize(retirement, 17));
    try std.testing.expectError(error.OutOfMemory, cleanup.alloc(u8, 1));
    try std.testing.expectEqual(@as(usize, 0), service.memory.control.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 16), service.memory.cleanup.?.stats().used_bytes);
    control.free(retirement);
    try std.testing.expectEqual(@as(usize, 0), service.memory.cleanup.?.stats().used_bytes);
    // The shared pin remains until the service and every consumer join.
    try std.testing.expectEqual(@as(usize, 128), owner.used);
}
