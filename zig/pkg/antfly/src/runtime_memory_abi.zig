// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Process-local, language-neutral memory ownership for runtime ABIs.
//! Allocation and destruction always execute in the compilation unit that
//! supplied the allocator; consumers only borrow this C-layout callback table.

const std = @import("std");

pub const Bytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,

    pub fn fromSlice(value: []const u8) Bytes {
        return .{
            .ptr = if (value.len == 0) null else value.ptr,
            .len = value.len,
        };
    }

    pub fn slice(self: Bytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..self.len];
    }
};

pub const OwnedBytes = extern struct {
    ptr: ?[*]u8 = null,
    len: usize = 0,

    pub fn slice(self: OwnedBytes) []u8 {
        if (self.len == 0) return &.{};
        return self.ptr.?[0..self.len];
    }
};

pub const OptionalOwnedBytes = extern struct {
    bytes: OwnedBytes = .{},
    present: u8 = 0,
    _reserved: [7]u8 = @splat(0),
};

pub const Allocator = extern struct {
    version: u32 = abi_version,
    _reserved: u32 = 0,
    context: *anyopaque,
    allocate: *const fn (
        context: *anyopaque,
        len: usize,
        alignment: usize,
    ) callconv(.c) ?[*]u8,
    release: *const fn (
        context: *anyopaque,
        ptr: ?[*]u8,
        len: usize,
        alignment: usize,
    ) callconv(.c) void,

    pub const abi_version: u32 = 1;

    pub fn fromStd(allocator: *const std.mem.Allocator) Allocator {
        return .{
            .context = @ptrCast(@constCast(allocator)),
            .allocate = stdAllocate,
            .release = stdRelease,
        };
    }

    pub fn asStd(self: *const Allocator) std.mem.Allocator {
        return .{ .ptr = @ptrCast(@constCast(self)), .vtable = &std_vtable };
    }

    pub fn valid(self: *const Allocator) bool {
        return self.version == abi_version and self._reserved == 0;
    }

    fn stdAllocate(
        context: *anyopaque,
        len: usize,
        alignment: usize,
    ) callconv(.c) ?[*]u8 {
        if (alignment == 0 or !std.math.isPowerOfTwo(alignment)) return null;
        const allocator: *const std.mem.Allocator = @ptrCast(@alignCast(context));
        return allocator.rawAlloc(len, .fromByteUnits(alignment), @returnAddress());
    }

    fn stdRelease(
        context: *anyopaque,
        ptr: ?[*]u8,
        len: usize,
        alignment: usize,
    ) callconv(.c) void {
        if (len == 0) return;
        if (alignment == 0 or !std.math.isPowerOfTwo(alignment))
            @panic("runtime ABI supplied invalid allocation alignment");
        const allocator: *const std.mem.Allocator = @ptrCast(@alignCast(context));
        allocator.rawFree(ptr.?[0..len], .fromByteUnits(alignment), @returnAddress());
    }

    const std_vtable: std.mem.Allocator.VTable = .{
        .alloc = adapterAllocate,
        .resize = adapterResize,
        .remap = adapterRemap,
        .free = adapterRelease,
    };

    fn adapterAllocate(
        context: *anyopaque,
        len: usize,
        alignment: std.mem.Alignment,
        _: usize,
    ) ?[*]u8 {
        const self: *const Allocator = @ptrCast(@alignCast(context));
        if (!self.valid()) return null;
        return self.allocate(self.context, len, alignment.toByteUnits());
    }

    fn adapterResize(
        _: *anyopaque,
        _: []u8,
        _: std.mem.Alignment,
        _: usize,
        _: usize,
    ) bool {
        return false;
    }

    fn adapterRemap(
        _: *anyopaque,
        _: []u8,
        _: std.mem.Alignment,
        _: usize,
        _: usize,
    ) ?[*]u8 {
        return null;
    }

    fn adapterRelease(
        context: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        _: usize,
    ) void {
        const self: *const Allocator = @ptrCast(@alignCast(context));
        if (!self.valid())
            @panic("runtime ABI allocator version mismatch");
        self.release(self.context, memory.ptr, memory.len, alignment.toByteUnits());
    }
};

test "ABI allocator preserves allocation ownership" {
    var allocator = std.testing.allocator;
    var abi_allocator = Allocator.fromStd(&allocator);
    const foreign = abi_allocator.asStd();
    const bytes = try foreign.alloc(u8, 32);
    foreign.free(bytes);

    abi_allocator._reserved = 1;
    try std.testing.expect(!abi_allocator.valid());
}
