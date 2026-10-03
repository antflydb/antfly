const std = @import("std");

pub fn StackFallbackAllocator(comptime size: usize) type {
    return struct {
        const Self = @This();

        buffer: [size]u8 = undefined,
        fallback_allocator: std.mem.Allocator,
        fixed_buffer_allocator: std.heap.FixedBufferAllocator = undefined,

        pub fn get(self: *Self) std.mem.Allocator {
            self.fixed_buffer_allocator = std.heap.FixedBufferAllocator.init(&self.buffer);
            return .{
                .ptr = self,
                .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free },
            };
        }

        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, return_address: usize) ?[*]u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            return std.heap.FixedBufferAllocator.alloc(&self.fixed_buffer_allocator, len, alignment, return_address) orelse
                self.fallback_allocator.vtable.alloc(self.fallback_allocator.ptr, len, alignment, return_address);
        }

        fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, return_address: usize) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fixed_buffer_allocator.ownsPtr(buf.ptr))
                return std.heap.FixedBufferAllocator.resize(&self.fixed_buffer_allocator, buf, alignment, new_len, return_address);
            return self.fallback_allocator.vtable.resize(self.fallback_allocator.ptr, buf, alignment, new_len, return_address);
        }

        fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, return_address: usize) ?[*]u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fixed_buffer_allocator.ownsPtr(buf.ptr))
                return std.heap.FixedBufferAllocator.remap(&self.fixed_buffer_allocator, buf, alignment, new_len, return_address);
            return self.fallback_allocator.vtable.remap(self.fallback_allocator.ptr, buf, alignment, new_len, return_address);
        }

        fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, return_address: usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fixed_buffer_allocator.ownsPtr(buf.ptr))
                return std.heap.FixedBufferAllocator.free(&self.fixed_buffer_allocator, buf, alignment, return_address);
            return self.fallback_allocator.vtable.free(self.fallback_allocator.ptr, buf, alignment, return_address);
        }
    };
}

pub fn stackFallback(comptime size: usize, fallback_allocator: std.mem.Allocator) StackFallbackAllocator(size) {
    return .{ .fallback_allocator = fallback_allocator };
}

pub fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0) return 0;
    if (needle.len > haystack.len) return null;
    for (0..haystack.len - needle.len + 1) |i| {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return i;
    }
    return null;
}
