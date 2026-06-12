// Copyright 2026 Antfly, Inc.
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
        try self.blocks.append(allocator, id);
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

    /// Remove `count` tokens from the tail of the sequence. Returns block IDs
    /// of any fully emptied trailing blocks so the caller can release them.
    /// Does NOT deallocate blocks itself — the returned slice borrows internal storage.
    pub fn dropTailTokens(self: *SequenceBlockTable, page_size_tokens: u16, count: usize) usize {
        if (count == 0) return 0;
        const current = self.tokenCount(page_size_tokens);
        if (count >= current) {
            const dropped = self.blocks.items.len;
            self.reset();
            return dropped;
        }
        const target = current - count;
        // How many blocks are needed to hold `target` tokens?
        const needed_blocks = (target + page_size_tokens - 1) / page_size_tokens;
        const excess_blocks = if (self.blocks.items.len > needed_blocks) self.blocks.items.len - needed_blocks else 0;
        self.blocks.items.len -= excess_blocks;
        if (self.blocks.items.len > 0) {
            const rem: u16 = @intCast(target % page_size_tokens);
            self.tail_tokens = if (rem == 0) page_size_tokens else rem;
        } else {
            self.tail_tokens = 0;
        }
        return excess_blocks;
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
