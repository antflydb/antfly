// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Concurrent allocation accounting. Owners must remain at stable addresses
//! until all allocations and joined operations are retired.
const std = @import("std");

pub const Shared = struct {
    child: std.mem.Allocator,
    limit: usize,
    used: std.atomic.Value(usize) = .init(0),

    pub fn allocator(self: *Shared) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn reserve(self: *Shared, bytes: usize) bool {
        var observed = self.used.load(.acquire);
        while (observed <= self.limit and bytes <= self.limit - observed) {
            if (self.used.cmpxchgWeak(observed, observed + bytes, .acq_rel, .acquire)) |changed| observed = changed else return true;
        }
        return false;
    }

    pub fn release(self: *Shared, bytes: usize) void {
        const previous = self.used.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
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
