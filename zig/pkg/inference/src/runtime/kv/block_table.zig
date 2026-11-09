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
const block = @import("block.zig");

pub const SequenceBlockTable = struct {
    blocks: std.ArrayListUnmanaged(block.KvBlockId) = .empty,
    tail_tokens: u16 = 0,
    /// Conservative end of shared pages; COW can leave unique holes inside.
    /// Physical refcounts remain the write authority.
    shared_prefix_blocks: u32 = 0,

    pub fn deinit(self: *SequenceBlockTable, allocator: std.mem.Allocator) void {
        self.blocks.deinit(allocator);
    }

    pub fn len(self: SequenceBlockTable) usize {
        return self.blocks.items.len;
    }

    pub fn last(self: SequenceBlockTable) ?block.KvBlockId {
        if (self.blocks.items.len == 0) return null;
        return self.blocks.items[self.blocks.items.len - 1];
    }

    pub fn append(self: *SequenceBlockTable, allocator: std.mem.Allocator, id: block.KvBlockId) !void {
        try self.blocks.ensureUnusedCapacity(allocator, 1);
        self.appendAssumeCapacity(id);
    }

    /// Caller reserved publication metadata before obtaining the owned page.
    pub fn appendAssumeCapacity(self: *SequenceBlockTable, id: block.KvBlockId) void {
        self.blocks.appendAssumeCapacity(id);
        self.tail_tokens = 0;
    }

    pub fn appendExisting(self: *SequenceBlockTable, allocator: std.mem.Allocator, id: block.KvBlockId) !void {
        try self.blocks.append(allocator, id);
    }

    pub fn markSharedPrefix(self: *SequenceBlockTable, count: u32) void {
        self.shared_prefix_blocks = count;
    }

    pub fn reset(self: *SequenceBlockTable) void {
        self.blocks.items.len = 0;
        self.tail_tokens = 0;
        self.shared_prefix_blocks = 0;
    }

    pub fn fullBlockCount(self: SequenceBlockTable, page_size_tokens: u16) usize {
        if (self.blocks.items.len == 0) return 0;
        if (self.tail_tokens == page_size_tokens) return self.blocks.items.len;
        return self.blocks.items.len - 1;
    }

    pub fn dropFrontBlocks(self: *SequenceBlockTable, count: usize) void {
        if (count == 0) return;
        if (count >= self.blocks.items.len) {
            self.reset();
            return;
        }
        std.mem.copyForwards(block.KvBlockId, self.blocks.items[0 .. self.blocks.items.len - count], self.blocks.items[count..]);
        self.blocks.items.len -= count;
        self.shared_prefix_blocks = @intCast(self.sharedPrefixBlocksAfterDrop(count));
    }

    fn sharedPrefixBlocksAfterDrop(self: SequenceBlockTable, count: usize) usize {
        if (count >= self.shared_prefix_blocks) return 0;
        return self.shared_prefix_blocks - count;
    }

    pub fn tokenCount(self: SequenceBlockTable, page_size_tokens: u16) usize {
        if (self.blocks.items.len == 0) return 0;
        return (self.blocks.items.len - 1) * page_size_tokens + self.tail_tokens;
    }

    /// Remove tokens and return the borrowed removed ID tail. The backing
    /// table must remain alive and unmodified until the caller finishes its
    /// release loop. Neither shrinking the table nor iterating allocates.
    pub fn dropTailTokens(self: *SequenceBlockTable, page_size_tokens: u16, count: usize) []const block.KvBlockId {
        const previous = self.blocks.items;
        if (count == 0) return previous[previous.len..];
        const current = self.tokenCount(page_size_tokens);
        if (count >= current) {
            self.reset();
            return previous;
        }
        const target = current - count;
        const needed_blocks = (target - 1) / page_size_tokens + 1;
        self.blocks.items.len = needed_blocks;
        const rem: u16 = @intCast(target % page_size_tokens);
        self.tail_tokens = if (rem == 0) page_size_tokens else rem;
        self.shared_prefix_blocks = @intCast(@min(self.shared_prefix_blocks, needed_blocks));
        return previous[needed_blocks..];
    }
};

test "block table append updates tail state" {
    const allocator = std.testing.allocator;
    var table = SequenceBlockTable{};
    defer table.deinit(allocator);

    table.tail_tokens = 7;
    try table.append(allocator, 42);
    try std.testing.expectEqual(@as(usize, 1), table.len());
    try std.testing.expectEqual(@as(block.KvBlockId, 42), table.last().?);
    try std.testing.expectEqual(@as(u16, 0), table.tail_tokens);
}

test "block table dropFrontBlocks updates shared prefix and tokens" {
    const allocator = std.testing.allocator;
    var table = SequenceBlockTable{};
    defer table.deinit(allocator);

    try table.appendExisting(allocator, 10);
    try table.appendExisting(allocator, 11);
    try table.appendExisting(allocator, 12);
    table.tail_tokens = 2;
    table.markSharedPrefix(2);

    table.dropFrontBlocks(1);

    try std.testing.expectEqual(@as(usize, 2), table.len());
    try std.testing.expectEqual(@as(block.KvBlockId, 11), table.blocks.items[0]);
    try std.testing.expectEqual(@as(block.KvBlockId, 12), table.blocks.items[1]);
    try std.testing.expectEqual(@as(u32, 1), table.shared_prefix_blocks);
    try std.testing.expectEqual(@as(u16, 2), table.tail_tokens);
}

test "block table tail removal borrows exact removed IDs and clamps shared prefix without allocation" {
    const a = std.testing.allocator;
    var table = SequenceBlockTable{};
    defer table.deinit(a);
    for ([_]block.KvBlockId{ 7, 9, 11, 13 }) |id| try table.appendExisting(a, id);
    table.tail_tokens = 2;
    table.markSharedPrefix(4);
    const original = table.blocks.items.ptr;
    const removed = table.dropTailTokens(4, 7);
    try std.testing.expectEqualSlices(block.KvBlockId, &.{ 11, 13 }, removed);
    try std.testing.expect(removed.ptr == original + 2);
    try std.testing.expectEqual(@as(usize, 7), table.tokenCount(4));
    try std.testing.expectEqual(@as(u32, 2), table.shared_prefix_blocks);
    const remainder = table.dropTailTokens(4, 99);
    try std.testing.expectEqualSlices(block.KvBlockId, &.{ 7, 9 }, remainder);
    try std.testing.expectEqual(@as(usize, 0), table.tokenCount(4));
    try std.testing.expectEqual(@as(u32, 0), table.shared_prefix_blocks);
}
