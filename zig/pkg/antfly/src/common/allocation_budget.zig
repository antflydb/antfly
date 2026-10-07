// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Concurrent allocation accounting. Owners must remain at stable addresses
//! until all allocations and joined operations are retired.
const std = @import("std");

/// Borrowed node owner. Callbacks cross native runtime partitions without an
/// error union; the owner must outlive every charged allocation and join.
pub const Accounting = struct {
    ptr: *anyopaque,
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

/// One envelope with reserved control capacity. Both views can free allocations
/// from either partition: transaction registries transfer objects across request
/// lifetimes, so allocator selection must never change their destruction contract.
pub const Partitioned = struct {
    foreground: Shared,
    control: Shared,

    const Header = struct { control: bool };

    pub fn overhead(alignment: std.mem.Alignment) usize {
        return alignment.max(.of(Header)).forward(@sizeOf(Header));
    }

    pub fn allocator(self: *Partitioned, control: bool) std.mem.Allocator {
        return .{ .ptr = self, .vtable = if (control) &control_vtable else &foreground_vtable };
    }

    const foreground_vtable: std.mem.Allocator.VTable = .{ .alloc = allocForeground, .resize = resize, .remap = remap, .free = free };
    const control_vtable: std.mem.Allocator.VTable = .{ .alloc = allocControl, .resize = resize, .remap = remap, .free = free };

    fn allocForeground(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocate(raw, len, alignment, ra, false);
    }
    fn allocControl(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        return allocate(raw, len, alignment, ra, true);
    }
    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize, control: bool) ?[*]u8 {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const size = std.math.add(usize, len, overhead(alignment)) catch return null;
        const budget = if (control) &self.control else &self.foreground;
        const base = budget.allocator().rawAlloc(size, alignment.max(.of(Header)), ra) orelse return null;
        const header: *Header = @ptrCast(@alignCast(base));
        header.* = .{ .control = control };
        return base + overhead(alignment);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const prefix = overhead(alignment);
        const base = bytes.ptr - prefix;
        const header: *Header = @ptrCast(@alignCast(base));
        const budget = if (header.control) &self.control else &self.foreground;
        const size = std.math.add(usize, len, prefix) catch return false;
        return budget.allocator().rawResize(base[0 .. bytes.len + prefix], alignment.max(.of(Header)), size, ra);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const prefix = overhead(alignment);
        const base = bytes.ptr - prefix;
        const header: *Header = @ptrCast(@alignCast(base));
        const budget = if (header.control) &self.control else &self.foreground;
        const size = std.math.add(usize, len, prefix) catch return null;
        const moved = budget.allocator().rawRemap(base[0 .. bytes.len + prefix], alignment.max(.of(Header)), size, ra) orelse return null;
        return moved + prefix;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Partitioned = @ptrCast(@alignCast(raw));
        const prefix = overhead(alignment);
        const base = bytes.ptr - prefix;
        const header: *Header = @ptrCast(@alignCast(base));
        const budget = if (header.control) &self.control else &self.foreground;
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
