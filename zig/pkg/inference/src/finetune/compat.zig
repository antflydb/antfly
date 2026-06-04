// Copyright 2026 Antfly, Inc.
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

//! Small shims for std APIs removed in Zig 0.17, kept local to the finetune
//! subtree so call sites stay unchanged.

const std = @import("std");

/// Reimplementation of the removed `std.heap.StackFallbackAllocator`: serves
/// allocations from a fixed inline buffer and falls back to the wrapped
/// allocator when the buffer is exhausted. Mirrors the old API: construct with
/// `stackFallback(size, fallback)` and obtain an `Allocator` via `.get()`.
pub fn StackFallbackAllocator(comptime size: usize) type {
    return struct {
        const Self = @This();

        buffer: [size]u8 = undefined,
        fallback_allocator: std.mem.Allocator,
        fixed_buffer_allocator: std.heap.FixedBufferAllocator = undefined,

        pub fn get(self: *Self) std.mem.Allocator {
            self.fixed_buffer_allocator = std.heap.FixedBufferAllocator.init(self.buffer[0..]);
            return .{
                .ptr = self,
                .vtable = &.{
                    .alloc = alloc,
                    .resize = resize,
                    .remap = remap,
                    .free = free,
                },
            };
        }

        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            return std.heap.FixedBufferAllocator.alloc(&self.fixed_buffer_allocator, len, alignment, ra) orelse
                self.fallback_allocator.vtable.alloc(self.fallback_allocator.ptr, len, alignment, ra);
        }

        fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fixed_buffer_allocator.ownsPtr(buf.ptr)) {
                return std.heap.FixedBufferAllocator.resize(&self.fixed_buffer_allocator, buf, alignment, new_len, ra);
            }
            return self.fallback_allocator.vtable.resize(self.fallback_allocator.ptr, buf, alignment, new_len, ra);
        }

        fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fixed_buffer_allocator.ownsPtr(buf.ptr)) {
                return std.heap.FixedBufferAllocator.remap(&self.fixed_buffer_allocator, buf, alignment, new_len, ra);
            }
            return self.fallback_allocator.vtable.remap(self.fallback_allocator.ptr, buf, alignment, new_len, ra);
        }

        fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.fixed_buffer_allocator.ownsPtr(buf.ptr)) {
                return std.heap.FixedBufferAllocator.free(&self.fixed_buffer_allocator, buf, alignment, ra);
            }
            return self.fallback_allocator.vtable.free(self.fallback_allocator.ptr, buf, alignment, ra);
        }
    };
}

pub fn stackFallback(comptime size: usize, fallback_allocator: std.mem.Allocator) StackFallbackAllocator(size) {
    return .{
        .buffer = undefined,
        .fallback_allocator = fallback_allocator,
        .fixed_buffer_allocator = undefined,
    };
}

/// Reimplementation of the removed `std.ascii.indexOfIgnoreCase`.
pub fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0) return 0;
    if (needle.len > haystack.len) return null;
    var i: usize = 0;
    const end = haystack.len - needle.len;
    while (i <= end) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return i;
    }
    return null;
}
