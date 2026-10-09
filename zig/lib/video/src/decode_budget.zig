// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
/// Tracks live bytes across alloc/resize/remap/free; exceeding the cap fails
/// before allocation and is distinguished from backing allocator exhaustion.
pub const Budget = struct {
    backing: std.mem.Allocator,
    limit: usize,
    live: usize = 0,
    peak: usize = 0,
    denied: bool = false,
    pub fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn admit(self: *Budget, old: usize, new: usize) bool {
        if (new > self.limit -| (self.live - old)) {
            self.denied = true;
            return false;
        }
        return true;
    }
    fn charge(self: *Budget, old: usize, new: usize) void {
        self.live = self.live - old + new;
        self.peak = @max(self.peak, self.live);
    }
    fn alloc(ctx: *anyopaque, len: usize, align_: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        if (!self.admit(0, len)) return null;
        const bytes = self.backing.rawAlloc(len, align_, ra) orelse return null;
        self.charge(0, len);
        return bytes;
    }
    fn resize(ctx: *anyopaque, bytes: []u8, align_: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        if (!self.admit(bytes.len, len) or !self.backing.rawResize(bytes, align_, len, ra)) return false;
        self.charge(bytes.len, len);
        return true;
    }
    fn remap(ctx: *anyopaque, bytes: []u8, align_: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        if (!self.admit(bytes.len, len)) return null;
        const out = self.backing.rawRemap(bytes, align_, len, ra) orelse return null;
        self.charge(bytes.len, len);
        return out;
    }
    fn free(ctx: *anyopaque, bytes: []u8, align_: std.mem.Alignment, ra: usize) void {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(bytes, align_, ra);
        self.live -= bytes.len;
    }
};
