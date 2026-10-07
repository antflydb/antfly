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

//! Leak-checked fixture allocations with opt-in ownership backtraces.
const std = @import("std");
const platform = @import("antfly_platform");

/// Keep leak and ownership checks in large fixtures without collecting a stack
/// trace on every allocation/free. Opt in to traces when diagnosing a failure.
pub const TestAllocator = struct {
    state: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0 }),

    pub fn allocator(self: *TestAllocator) std.mem.Allocator {
        return if (platform.env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else self.state.allocator();
    }

    pub fn deinit(self: *TestAllocator) void {
        std.debug.assert(self.state.deinit() == 0);
    }
};

/// Leak-checked, single-threaded requested-byte budget for focused fixtures.
/// SafeAllocator has no memory-limit option; keep the cap outside its metadata.
pub const BoundedAllocator = struct {
    state: std.heap.SafeAllocator,
    limit: usize,
    live: usize = 0,

    pub fn init(backing: std.mem.Allocator, limit: usize) BoundedAllocator {
        return .{ .state = .init(backing, .{ .stack_trace_frames = 0 }), .limit = limit };
    }

    pub fn allocator(self: *BoundedAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn deinit(self: *BoundedAllocator) usize {
        return self.state.deinit();
    }

    fn permits(self: *const BoundedAllocator, old_len: usize, new_len: usize) bool {
        return new_len <= old_len or new_len - old_len <= self.limit - self.live;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *BoundedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.permits(0, len)) return null;
        const result = self.state.allocator().rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        return result;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *BoundedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.permits(memory.len, len)) return false;
        if (!self.state.allocator().rawResize(memory, alignment, len, ra)) return false;
        self.live = self.live - memory.len + len;
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *BoundedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.permits(memory.len, len)) return null;
        const result = self.state.allocator().rawRemap(memory, alignment, len, ra) orelse return null;
        self.live = self.live - memory.len + len;
        return result;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *BoundedAllocator = @ptrCast(@alignCast(ctx));
        self.state.allocator().rawFree(memory, alignment, ra);
        self.live -= memory.len;
    }
};

test "fixture budget bounds requested bytes and releases capacity after free" {
    var bounded = BoundedAllocator.init(std.testing.allocator, 64);
    defer std.debug.assert(bounded.deinit() == 0);
    const alloc = bounded.allocator();
    const first = try alloc.alloc(u8, 48);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 17));
    const second = try alloc.alloc(u8, 16);
    alloc.free(first);
    const reused = try alloc.alloc(u8, 48);
    try std.testing.expectError(error.OutOfMemory, alloc.realloc(second, 17));
    try std.testing.expectEqual(@as(usize, 64), bounded.live);
    alloc.free(second);
    alloc.free(reused);
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
}
