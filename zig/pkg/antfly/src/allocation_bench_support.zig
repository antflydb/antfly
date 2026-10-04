//! Single-threaded allocation accounting for diagnostic benchmarks only.
const std = @import("std");
pub const Counter = struct {
    live: usize = 0,
    peak: usize = 0,
    calls: usize = 0,
    bytes: usize = 0,
    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn grow(self: *@This(), len: usize) void {
        self.live += len;
        self.peak = @max(self.peak, self.live);
        self.bytes += len;
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const child = std.heap.c_allocator;
        const result = child.vtable.alloc(child.ptr, len, alignment, ra) orelse return null;
        const self: *Counter = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        self.grow(len);
        return result;
    }
    fn resized(self: *@This(), old: usize, new: usize) void {
        if (new >= old) self.grow(new - old) else self.live -= old - new;
    }
    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const child = std.heap.c_allocator;
        if (!child.vtable.resize(child.ptr, memory, alignment, len, ra)) return false;
        const self: *Counter = @ptrCast(@alignCast(ctx));
        self.resized(memory.len, len);
        return true;
    }
    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const child = std.heap.c_allocator;
        const result = child.vtable.remap(child.ptr, memory, alignment, len, ra) orelse return null;
        const self: *Counter = @ptrCast(@alignCast(ctx));
        self.resized(memory.len, len);
        return result;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Counter = @ptrCast(@alignCast(ctx));
        self.live -= memory.len;
        const child = std.heap.c_allocator;
        child.vtable.free(child.ptr, memory, alignment, ra);
    }
};
