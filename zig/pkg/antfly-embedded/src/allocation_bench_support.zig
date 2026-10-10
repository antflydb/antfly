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

//! Single-threaded allocation accounting for diagnostic benchmarks only.
//! calls counts successful raw allocs; resize/remap counters count attempts.
//! Moving remaps count successful pointer changes and their requested new size.
//! bytes/live/peak measure logical requested storage, excluding allocator
//! overhead, transient backend realloc overlap, and RSS.
const std = @import("std");
pub const Counter = struct {
    backing: std.mem.Allocator = std.heap.c_allocator,
    live: usize = 0,
    peak: usize = 0,
    calls: usize = 0,
    bytes: usize = 0,
    resize_calls: usize = 0,
    remap_calls: usize = 0,
    moving_remaps: usize = 0,
    moved_bytes: usize = 0,

    pub fn resetActivity(self: *@This()) void {
        self.* = .{ .backing = self.backing, .live = self.live, .peak = self.live };
    }
    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn grow(self: *@This(), len: usize) void {
        self.live += len;
        self.peak = @max(self.peak, self.live);
        self.bytes += len;
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Counter = @ptrCast(@alignCast(ctx));
        const child = self.backing;
        const result = child.vtable.alloc(child.ptr, len, alignment, ra) orelse return null;
        self.calls += 1;
        self.grow(len);
        return result;
    }
    fn resized(self: *@This(), old: usize, new: usize) void {
        if (new >= old) self.grow(new - old) else self.live -= old - new;
    }
    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Counter = @ptrCast(@alignCast(ctx));
        self.resize_calls += 1;
        const child = self.backing;
        if (!child.vtable.resize(child.ptr, memory, alignment, len, ra)) return false;
        self.resized(memory.len, len);
        return true;
    }
    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Counter = @ptrCast(@alignCast(ctx));
        self.remap_calls += 1;
        const child = self.backing;
        const result = child.vtable.remap(child.ptr, memory, alignment, len, ra) orelse return null;
        if (result != memory.ptr) {
            self.moving_remaps += 1;
            self.moved_bytes += len;
        }
        self.resized(memory.len, len);
        return result;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Counter = @ptrCast(@alignCast(ctx));
        self.live -= memory.len;
        const child = self.backing;
        child.vtable.free(child.ptr, memory, alignment, ra);
    }
};

test "allocation counter distinguishes moving remaps from raw allocs and resets activity" {
    const Moving = struct {
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
            const result = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
            @memcpy(result[0..@min(memory.len, len)], memory[0..@min(memory.len, len)]);
            std.testing.allocator.rawFree(memory, alignment, ra);
            return result;
        }
        fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            std.testing.allocator.rawFree(memory, alignment, ra);
        }
    };
    var context: u8 = 0;
    var counter: Counter = .{ .backing = .{ .ptr = &context, .vtable = &.{ .alloc = Moving.alloc, .resize = Moving.resize, .remap = Moving.remap, .free = Moving.free } } };
    const a = counter.allocator();
    const initial = try a.dupe(u8, "retained");
    const grown = try a.realloc(initial, 64);
    try std.testing.expectEqualStrings("retained", grown[0..8]);
    try std.testing.expectEqual(@as(usize, 1), counter.calls);
    try std.testing.expectEqual(@as(usize, 1), counter.remap_calls);
    try std.testing.expectEqual(@as(usize, 1), counter.moving_remaps);
    try std.testing.expectEqual(@as(usize, 64), counter.moved_bytes);
    try std.testing.expectEqual(@as(usize, 64), counter.live);
    counter.resetActivity();
    try std.testing.expectEqual(@as(usize, 64), counter.peak);
    try std.testing.expectEqual(@as(usize, 0), counter.moving_remaps);
    a.free(grown);
    try std.testing.expectEqual(@as(usize, 0), counter.live);
}

test "allocation counter includes refused resize and remap attempts without changing live storage" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1, .resize_fail_index = 0 });
    var counter: Counter = .{ .backing = failing.allocator() };
    const a = counter.allocator();
    const bytes = try a.alloc(u8, 8);
    defer a.free(bytes);
    try std.testing.expectError(error.OutOfMemory, a.realloc(bytes, 64));
    try std.testing.expect(counter.resize_calls + counter.remap_calls != 0);
    try std.testing.expectEqual(@as(usize, 0), counter.moving_remaps);
    try std.testing.expectEqual(@as(usize, 1), counter.calls);
    try std.testing.expectEqual(@as(usize, 8), counter.live);
}
