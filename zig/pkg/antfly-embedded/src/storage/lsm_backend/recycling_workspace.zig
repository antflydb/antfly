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

const std = @import("std");
const resources = @import("../resource_manager.zig");

/// An exclusively leased workspace with reusable size classes. Buffers carry
/// intrusive metadata, so reuse and arbitrary-order frees need no allocation.
/// The backing allocator bounds and charges physical bytes, including headers.
pub const RecyclingWorkspace = struct {
    const min_capacity = 256;
    const max_capacity = 64 * 1024;
    const bin_count = 33;
    const Header = struct {
        all_prev: ?*Header = null,
        all_next: ?*Header = null,
        free_prev: ?*Header = null,
        free_next: ?*Header = null,
        allocation: []u8,
        capacity: usize,
        alignment: std.mem.Alignment,
        in_use: bool = true,
        class_index: u8,
    };
    backing: std.mem.Allocator = undefined,
    budget: ?*resources.BudgetedAllocator = null,
    bins: [bin_count]?*Header = @splat(null),
    head: ?*Header = null,
    tail: ?*Header = null,
    physical_bytes: usize = 0,
    live_buffers: usize = 0,

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn sizeClass(len: usize) struct { capacity: usize, index: u8 } {
        const size = @max(len, min_capacity);
        const base = @as(usize, 1) << std.math.log2_int(usize, size);
        const capacity = std.mem.alignForward(usize, size, base / 4);
        const rounded_base = @as(usize, 1) << std.math.log2_int(usize, capacity);
        return .{ .capacity = capacity, .index = @intCast((std.math.log2_int(usize, rounded_base) - 8) * 4 + (capacity - rounded_base) / (rounded_base / 4)) };
    }
    fn header(memory: []u8) *Header {
        return @ptrCast(@alignCast(memory.ptr - @sizeOf(Header)));
    }
    fn payload(h: *Header) [*]u8 {
        return @as([*]u8, @ptrCast(h)) + @sizeOf(Header);
    }
    fn unlinkFree(self: *@This(), h: *Header) void {
        if (h.free_prev) |prev| prev.free_next = h.free_next else self.bins[h.class_index] = h.free_next;
        if (h.free_next) |next| next.free_prev = h.free_prev;
        h.free_prev = null;
        h.free_next = null;
    }
    fn destroy(self: *@This(), h: *Header) void {
        std.debug.assert(!h.in_use);
        self.unlinkFree(h);
        if (h.all_prev) |prev| prev.all_next = h.all_next else self.head = h.all_next;
        if (h.all_next) |next| next.all_prev = h.all_prev else self.tail = h.all_prev;
        const memory = h.allocation;
        const alignment = h.alignment;
        self.physical_bytes -= memory.len;
        self.backing.rawFree(memory, alignment, @returnAddress());
    }
    /// Drop idle buffers, oldest first. Live buffers never move. Return spare
    /// host credit too, so mandatory fallback is not charged for retired data.
    pub fn trimIdle(self: *@This(), target: usize) void {
        var current = self.tail;
        while (self.physical_bytes > target) {
            const h = current orelse break;
            current = h.all_prev;
            if (!h.in_use) self.destroy(h);
        }
        if (self.budget) |budget| _ = budget.releaseUnusedCredit();
    }
    pub fn deinit(self: *@This()) void {
        std.debug.assert(self.live_buffers == 0);
        self.trimIdle(0);
        std.debug.assert(self.physical_bytes == 0);
        self.* = .{};
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (len > max_capacity) {
            self.trimIdle(0);
            return null;
        }
        const class = sizeClass(len);
        var capacity = class.capacity;
        var current = self.bins[class.index];
        while (current) |h| {
            current = h.free_next;
            if (h.capacity < len or h.alignment.toByteUnits() < alignment.toByteUnits()) continue;
            self.unlinkFree(h);
            h.in_use = true;
            self.live_buffers += 1;
            return payload(h);
        }
        const actual_alignment: std.mem.Alignment = @fromBackingInt(@intCast(@max(@backingInt(alignment), @backingInt(std.mem.Alignment.of(Header)))));
        const offset = std.mem.alignForward(usize, @sizeOf(Header), actual_alignment.toByteUnits());
        var allocation_len = std.math.add(usize, capacity, offset) catch return null;
        var memory = self.backing.rawAlloc(allocation_len, actual_alignment, ra);
        if (memory == null) {
            const before = self.physical_bytes;
            self.trimIdle(0);
            if (self.physical_bytes != before) memory = self.backing.rawAlloc(allocation_len, actual_alignment, ra);
            // Rounded spare capacity is optional. Preserve readable buffers
            // when exact payload plus ownership metadata fits admission.
            if (memory == null and capacity != len) {
                capacity = len;
                allocation_len = std.math.add(usize, capacity, offset) catch return null;
                memory = self.backing.rawAlloc(allocation_len, actual_alignment, ra);
            }
            if (memory == null) {
                if (self.budget) |budget| _ = budget.releaseUnusedCredit();
                return null;
            }
        }
        const bytes = memory.?;
        const h: *Header = @ptrCast(@alignCast(bytes + offset - @sizeOf(Header)));
        h.* = .{ .all_next = self.head, .allocation = bytes[0..allocation_len], .capacity = capacity, .alignment = actual_alignment, .class_index = class.index };
        if (self.head) |first| first.all_prev = h else self.tail = h;
        self.head = h;
        self.physical_bytes += allocation_len;
        self.live_buffers += 1;
        return payload(h);
    }
    fn resize(_: *anyopaque, memory: []u8, _: std.mem.Alignment, len: usize, _: usize) bool {
        return len <= header(memory).capacity;
    }
    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        if (resize(raw, memory, alignment, len, ra)) return memory.ptr;
        return null;
    }
    fn free(raw: *anyopaque, memory: []u8, _: std.mem.Alignment, _: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const h = header(memory);
        std.debug.assert(h.in_use);
        h.in_use = false;
        self.live_buffers -= 1;
        const index = h.class_index;
        h.free_next = self.bins[index];
        if (h.free_next) |next| next.free_prev = h;
        self.bins[index] = h;
    }
};

test "recycling workspace preserves aligned live buffers and unwinds allocation failures" {
    const Fixture = struct {
        fn run(backing: std.mem.Allocator) !void {
            var workspace: RecyclingWorkspace = .{ .backing = backing };
            defer workspace.deinit();
            const a = workspace.allocator();
            const first = try a.alignedAlloc(u8, .@"64", 100);
            var first_live = true;
            defer if (first_live) a.free(first);
            const second = try a.alloc(u8, 400);
            defer a.free(second);
            @memset(second, 'b');
            a.free(first);
            first_live = false;
            var reused = try a.alignedAlloc(u8, .@"64", 96);
            defer a.free(reused);
            try std.testing.expectEqual(@intFromPtr(first.ptr), @intFromPtr(reused.ptr));
            try std.testing.expectEqual(@as(usize, 0), @intFromPtr(reused.ptr) % 64);
            try std.testing.expect(a.resize(reused, 128));
            reused = reused.ptr[0..128];
            @memset(reused, 'c');
            for (second) |byte| try std.testing.expectEqual(@as(u8, 'b'), byte);
            try std.testing.expect(!a.resize(reused, 300));
        }
    };
    var no_resize = @import("../lite/test_allocator.zig").NoResizeAllocator{ .backing = std.testing.allocator };
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fixture.run, .{});
}

test "recycling workspace exact size fallback and failed growth release idle storage" {
    var budget = @import("../lite/test_allocator.zig").BudgetAllocator{ .backing = std.testing.allocator, .limit = 300 + @sizeOf(RecyclingWorkspace.Header) };
    var workspace: RecyclingWorkspace = .{ .backing = budget.allocator() };
    defer workspace.deinit();
    const a = workspace.allocator();
    const bytes = try a.alloc(u8, 300);
    a.free(bytes);
    try std.testing.expectEqual(@as(usize, 300 + @sizeOf(RecyclingWorkspace.Header)), workspace.physical_bytes);
    const calls = budget.alloc_calls;
    const reused = try a.alloc(u8, 280);
    try std.testing.expectEqual(@intFromPtr(bytes.ptr), @intFromPtr(reused.ptr));
    try std.testing.expectEqual(calls, budget.alloc_calls);
    a.free(reused);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 350));
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(@as(usize, 0), workspace.physical_bytes);
    const mandatory = try budget.allocator().alloc(u8, 300);
    defer budget.allocator().free(mandatory);
}

test "recycling workspace declined optional allocation returns host credit for mandatory scratch" {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 * 1024 } });
    defer manager.deinit(a);
    var budget = resources.BudgetedAllocator.init(&manager, .lsm_read_working_set, a, 1);
    budget.credit_quantum = 4096;
    defer budget.deinit();
    var workspace: RecyclingWorkspace = .{ .backing = budget.allocator(), .budget = &budget };
    defer workspace.deinit();
    const recycled = workspace.allocator();
    const idle = try recycled.alloc(u8, 16 * 1024);
    recycled.free(idle);
    try std.testing.expect(manager.sliceStats(.lsm_read_working_set).used_bytes > 0);
    try std.testing.expectError(error.OutOfMemory, recycled.alloc(u8, 64 * 1024 + 1));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
    const mandatory = try budget.allocator().alloc(u8, 64 * 1024);
    defer budget.allocator().free(mandatory);
}
