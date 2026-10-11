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

//! Query-owned result chunks. Addresses survive growth; chunk geometry does
//! not depend on heap remapping or optional cache allocation history.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Scratch = struct {
    arena: Arena,
    retained_limit: usize,
    pub fn init(backing: Allocator, retained_limit: usize) Scratch {
        return .{ .arena = .{ .child_allocator = backing }, .retained_limit = retained_limit };
    }
    pub fn allocator(self: *Scratch) Allocator {
        return self.arena.allocator();
    }
    pub fn reset(self: *Scratch) void {
        self.arena.reset(self.retained_limit);
    }
    pub fn deinit(self: *Scratch) void {
        self.arena.reset(0);
    }
};

const Arena = struct {
    const Node = struct {
        next: ?*Node = null,
        capacity: usize,
        used: usize = 0,
        alignment: std.mem.Alignment,
        fn offset(self: *const Node) usize {
            return std.mem.alignForward(usize, @intFromPtr(self) + @sizeOf(Node), self.alignment.toByteUnits()) - @intFromPtr(self);
        }
        fn allocationLen(self: *const Node) usize {
            return @sizeOf(Node) + self.alignment.toByteUnits() - @alignOf(Node) + self.capacity;
        }
        fn payload(self: *Node) []u8 {
            const ptr: [*]u8 = @ptrCast(self);
            return ptr[self.offset()..][0..self.capacity];
        }
    };
    child_allocator: Allocator,
    // Speculative growth uses the original owner; only required allocation may
    // reclaim optional workspace residency. Both adapters must have the same
    // allocation owner and compatible free operations.
    preferred_allocator: ?Allocator = null,
    head: ?*Node = null,
    current: ?*Node = null,
    pub fn allocator(self: *Arena) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = Allocator.noRemap, .free = free } };
    }
    pub fn queryCapacity(self: *const Arena) usize {
        var total: usize = 0;
        var node = self.head;
        while (node) |n| : (node = n.next) total += n.capacity;
        return total;
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Arena = @ptrCast(@alignCast(ctx));
        var node = self.current;
        while (node) |n| {
            const bytes = n.payload();
            const start = std.mem.alignForward(usize, @intFromPtr(bytes.ptr) + n.used, alignment.toByteUnits()) - @intFromPtr(bytes.ptr);
            if (start <= bytes.len and len <= bytes.len - start) {
                n.used = start + len;
                self.current = n;
                return bytes.ptr + start;
            }
            if (n.next) |next| node = next else break;
        }
        const previous_capacity = if (node) |n| n.capacity else 0;
        const preferred = @max(len, @min(@as(usize, 64 * 1024), @max(@as(usize, 64), previous_capacity *| 2)));
        const node_alignment = std.mem.Alignment.fromByteUnits(@max(@alignOf(Node), alignment.toByteUnits()));
        const offset = @sizeOf(Node) + node_alignment.toByteUnits() - @alignOf(Node);
        // Payload alignment lives inside the allocation, so varying alignments
        // never leave unreclaimable gaps between chunks on a LIFO backing.
        const allocation_alignment = std.mem.Alignment.fromByteUnits(@alignOf(Node));
        const bytes_len = std.math.add(usize, offset, preferred) catch return null;
        var capacity = preferred;
        const preferred_allocator = if (preferred == len) self.child_allocator else self.preferred_allocator orelse self.child_allocator;
        const ptr = preferred_allocator.rawAlloc(bytes_len, allocation_alignment, ra) orelse fallback: {
            if (preferred == len) return null;
            capacity = len;
            break :fallback self.child_allocator.rawAlloc(std.math.add(usize, offset, len) catch return null, allocation_alignment, ra) orelse return null;
        };
        const created: *Node = @ptrCast(@alignCast(ptr));
        created.* = .{ .capacity = capacity, .used = len, .alignment = node_alignment };
        if (node) |last| last.next = created else self.head = created;
        self.current = created;
        return created.payload().ptr;
    }
    fn resize(ctx: *anyopaque, memory: []u8, _: std.mem.Alignment, len: usize, _: usize) bool {
        const self: *Arena = @ptrCast(@alignCast(ctx));
        const node = self.current orelse return false;
        const payload = node.payload();
        if (@intFromPtr(memory.ptr) + memory.len != @intFromPtr(payload.ptr) + node.used) return len <= memory.len;
        const start = @intFromPtr(memory.ptr) - @intFromPtr(payload.ptr);
        if (len > payload.len - start) return false;
        node.used = start + len;
        return true;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        _ = resize(ctx, memory, alignment, 0, ra);
    }
    fn destroy(self: *Arena, node: *Node) void {
        const ptr: [*]u8 = @ptrCast(node);
        self.child_allocator.rawFree(ptr[0..node.allocationLen()], .fromByteUnits(@alignOf(Node)), @returnAddress());
    }
    fn reset(self: *Arena, limit: usize) void {
        var retained: usize = 0;
        var head: ?*Node = null;
        var tail: ?*Node = null;
        var discarded: ?*Node = null;
        var retain_prefix = true;
        var next = self.head;
        while (next) |node| {
            next = node.next;
            const allocation_len = node.allocationLen();
            if (retain_prefix and allocation_len <= limit - retained) {
                retained += allocation_len;
                node.used = 0;
                node.next = null;
                if (tail) |last| last.next = node else head = node;
                tail = node;
            } else {
                retain_prefix = false;
                node.next = discarded;
                discarded = node;
            }
        }
        // LIFO allocators can reclaim each chunk only in reverse order.
        while (discarded) |node| {
            discarded = node.next;
            self.destroy(node);
        }
        self.head = head;
        self.current = head;
    }
};

test "lite result scratch preserves addresses alignment and bounded reuse" {
    var scratch = Scratch.init(std.testing.allocator, 8192);
    defer scratch.deinit();
    const a = scratch.allocator();
    const earlier = try a.dupe(u8, "earlier");
    const big = try a.alignedAlloc(u8, .fromByteUnits(64), 32768);
    @memset(big, 'x');
    try std.testing.expectEqualStrings("earlier", earlier);
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(big.ptr) % 64);
    scratch.reset();
    try std.testing.expect(scratch.arena.queryCapacity() <= 8192);
    const reused = try a.dupe(u8, "reused");
    try std.testing.expectEqualStrings("reused", reused);
}

test "lite result scratch minimum fallback fits denied growth" {
    // The preferred first chunk needs more than this backing can provide.
    // An exact-sized chunk must still make the required result available.
    var buffer: [48]u8 align(64) = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&buffer);
    // Six payload bytes fit this reset limit, but the whole chunk does not.
    var scratch = Scratch.init(backing.allocator(), @sizeOf(Arena.Node) + 5);
    defer scratch.deinit();
    const result = try scratch.allocator().dupe(u8, "result");
    try std.testing.expectEqualStrings("result", result);
    try std.testing.expectEqual(@as(usize, 6), scratch.arena.queryCapacity());
    scratch.reset();
    try std.testing.expectEqual(@as(usize, 0), backing.end_index);
}

test "lite result scratch resize and free preserve other chunks" {
    var scratch = Scratch.init(std.testing.allocator, 8192);
    defer scratch.deinit();
    const a = scratch.allocator();
    const first = try a.dupe(u8, "preserved");
    const last = try a.alloc(u8, 8);
    try std.testing.expect(a.resize(last, 16));
    a.free(last.ptr[0..16]);
    const reused = try a.alloc(u8, 8);
    try std.testing.expectEqual(last.ptr, reused.ptr);
    _ = try a.alloc(u8, 32768);
    try std.testing.expect(!a.resize(first, 32768));
    try std.testing.expect(a.resize(first, 4));
    try std.testing.expectEqualStrings("preserved", first);
    scratch.reset();
    try std.testing.expectEqual(@as(usize, 64), scratch.arena.queryCapacity());
}

test "lite result scratch closes repeatedly with LIFO backing" {
    var buffer: [1024]u8 align(64) = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&buffer);
    for (0..16) |_| {
        var scratch = Scratch.init(backing.allocator(), 256 * 1024);
        _ = try scratch.allocator().alloc(u8, 64);
        _ = try scratch.allocator().alloc(u8, 256);
        scratch.deinit();
        try std.testing.expectEqual(@as(usize, 0), backing.end_index);
    }
}

test "lite result scratch pressure fallback crosses growth boundaries" {
    for ([_]usize{ 2048, 4096, 8192, 65536 }) |size| {
        const buffer = try std.testing.allocator.alignedAlloc(u8, .fromByteUnits(64), 3 * (size + @sizeOf(Arena.Node)));
        defer std.testing.allocator.free(buffer);
        var backing = std.heap.FixedBufferAllocator.init(buffer);
        var scratch = Scratch.init(backing.allocator(), 256 * 1024);
        var values: [3][]u8 = undefined;
        for (&values, 0..) |*value, i| {
            value.* = try scratch.allocator().alloc(u8, size);
            @memset(value.*, @intCast(i));
        }
        for (values, 0..) |value, i| for (value) |byte| {
            try std.testing.expectEqual(@as(u8, @intCast(i)), byte);
        };
        scratch.deinit();
        try std.testing.expectEqual(@as(usize, 0), backing.end_index);
    }
}

test "lite result scratch reset keeps a prefix for LIFO reclaim" {
    var buffer: [65536]u8 align(64) = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&buffer);
    var scratch = Scratch.init(backing.allocator(), 256);
    _ = try scratch.allocator().alloc(u8, 8);
    _ = try scratch.allocator().alloc(u8, 16384);
    _ = try scratch.allocator().alloc(u8, 16384);
    scratch.reset();
    try std.testing.expectEqual(@as(usize, 64), scratch.arena.queryCapacity());
    try std.testing.expectEqual(@as(usize, @sizeOf(Arena.Node) + 64), backing.end_index);
    scratch.deinit();
    try std.testing.expectEqual(@as(usize, 0), backing.end_index);
}
test "lite result scratch mixed alignments reclaim LIFO padding" {
    var buffer: [4096]u8 align(64) = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&buffer);
    var scratch = Scratch.init(backing.allocator(), 256 * 1024);
    _ = try scratch.allocator().alloc(u8, 8);
    _ = try scratch.allocator().alignedAlloc(u8, .fromByteUnits(64), 256);
    scratch.deinit();
    try std.testing.expectEqual(@as(usize, 0), backing.end_index);
}
