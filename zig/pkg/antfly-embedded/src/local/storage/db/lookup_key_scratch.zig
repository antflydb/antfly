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

//! Batch-owned lookup keys. Internal keys borrow the input; encoded keys live
//! in a 2 KiB inline buffer and bounded blocks. Scratch must stay at its
//! original address until consumers have finished using returned keys.
const std = @import("std");
const keys = @import("../internal_keys.zig");

pub const Scratch = struct {
    alloc: std.mem.Allocator,
    head: ?*Block = null,
    remaining_keys: usize = std.math.maxInt(usize),
    inline_bytes: [2048]u8 align(key_alignment) = undefined,
    inline_used: usize = 0,
    const Block = struct { next: ?*Block, len: usize, used: usize };
    const key_alignment = 32;
    const block_header = std.mem.alignForward(usize, @sizeOf(Block), key_alignment);

    pub fn init(alloc: std.mem.Allocator, expected_keys: usize) Scratch {
        return .{ .alloc = alloc, .remaining_keys = expected_keys };
    }

    pub fn deinit(self: *Scratch) void {
        while (self.head) |block| {
            self.head = block.next;
            self.alloc.free(@as([*]align(key_alignment) u8, @ptrCast(@alignCast(block)))[0..block.len]);
        }
    }

    noinline fn grow(self: *Scratch, stride: usize) !*Block {
        // Keep allocation and overflow handling out of the per-document loop.
        const slots = @max(@as(usize, 1), (4096 - block_header) / stride);
        const count = @max(@as(usize, 1), @min(slots, self.remaining_keys));
        const bytes = try std.math.add(usize, try std.math.mul(usize, stride, count), block_header);
        const memory = try self.alloc.alignedAlloc(u8, .fromByteUnits(key_alignment), bytes);
        const block: *Block = @ptrCast(memory.ptr);
        block.* = .{ .next = self.head, .len = bytes, .used = 0 };
        self.head = block;
        return block;
    }

    inline fn encode(output: []u8, document: []const u8, relational: bool) void {
        output[0] = keys.user_namespace;
        const end = 1 + keys.encodeComponent(output[1..], document);
        output[end] = if (relational) keys.relational_row_kind else keys.primary_kind;
    }

    pub inline fn key(self: *Scratch, document: []const u8, relational: bool) ![]const u8 {
        if (keys.isInternalUserKey(document)) {
            self.remaining_keys -|= 1;
            return document;
        }
        const output = try self.buffer(try std.math.add(usize, keys.encodedComponentLen(document), 2), key_alignment);
        encode(output, document, relational);
        return output;
    }

    pub inline fn identityKey(self: *Scratch, document: []const u8) ![]const u8 {
        const output = try self.buffer(try std.math.add(usize, keys.encodedComponentLen(document), 2), 1);
        output[0] = keys.identity_namespace;
        output[1] = keys.identity_doc_to_ordinal_kind;
        _ = keys.encodeComponent(output[2..], document);
        return output;
    }

    inline fn buffer(self: *Scratch, len: usize, comptime slot_alignment: usize) ![]u8 {
        // Keep short encoded keys in aligned slots for repeated comparisons.
        const stride = (try std.math.add(usize, len, slot_alignment - 1)) & ~@as(usize, slot_alignment - 1);
        if (stride <= self.inline_bytes.len - self.inline_used) {
            const output = self.inline_bytes[self.inline_used..][0..len];
            self.inline_used += stride;
            self.remaining_keys -|= 1;
            return output;
        }
        const block = if (self.head) |current|
            if (current.len - block_header - current.used >= stride) current else try self.grow(stride)
        else
            try self.grow(stride);
        const output = @as([*]u8, @ptrCast(block))[block_header + block.used ..][0..len];
        block.used += stride;
        self.remaining_keys -|= 1;
        return output;
    }
};

fn exercise(alloc: std.mem.Allocator) !void {
    var scratch = Scratch.init(alloc, 3);
    defer scratch.deinit();
    const first = try scratch.key("a\x00b", false);
    const huge = @as([8192]u8, @splat('z'));
    const large = try scratch.key(&huge, true);
    const expected_large = try keys.relationalRowKeyAlloc(alloc, &huge);
    defer alloc.free(expected_large);
    try std.testing.expectEqualSlices(u8, expected_large, large);
    const expected = try keys.documentKeyAlloc(alloc, "a\x00b");
    defer alloc.free(expected);
    try std.testing.expectEqualSlices(u8, expected, first);
    const last = try scratch.key("tail", true);
    const expected_last = try keys.relationalRowKeyAlloc(alloc, "tail");
    defer alloc.free(expected_last);
    try std.testing.expectEqualSlices(u8, expected_last, last);
}

test "lookup scratch preserves escaped keys across blocks and allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exercise, .{});
}

test "lookup scratch borrows internal keys without allocating" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var scratch: Scratch = .{ .alloc = failing.allocator() };
    defer scratch.deinit();
    const internal = "\x01internal";
    const result = try scratch.key(internal, true);
    try std.testing.expectEqual(internal.ptr, result.ptr);
}

test "lookup scratch inline keys survive a failed spill without allocating" {
    const alloc = std.testing.allocator;
    const expected = try keys.documentKeyAlloc(alloc, "first");
    defer alloc.free(expected);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var scratch = Scratch.init(failing.allocator(), 3);
    defer scratch.deinit();
    const first = try scratch.key("first", false);
    const huge = @as([8192]u8, @splat('z'));
    try std.testing.expectError(error.OutOfMemory, scratch.key(&huge, false));
    try std.testing.expectEqualSlices(u8, expected, first);
    _ = try scratch.key("after", false);
    try std.testing.expectEqualSlices(u8, expected, first);
}
