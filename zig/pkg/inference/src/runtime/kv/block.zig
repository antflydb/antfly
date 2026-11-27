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

pub const KvPoolId = u32;
pub const KvBlockId = u32;
pub const invalid_block_id = std.math.maxInt(KvBlockId);

/// The logical prefix can span the active block table and its reserved tail.
pub inline fn physicalTokenCapacity(spans: []const []const KvBlockId, needed_blocks: usize, page_size: usize) !usize {
    if (needed_blocks == 0) return 0;
    if (page_size == 0) return error.InvalidBlockId;
    var remaining = needed_blocks;
    var largest: usize = 0;
    for (spans) |span| {
        const count = @min(span.len, remaining);
        for (span[0..count]) |id| {
            if (id == invalid_block_id) return error.InvalidBlockId;
            largest = @max(largest, id);
        }
        remaining -= count;
        if (remaining == 0)
            return std.math.mul(usize, try std.math.add(usize, largest, 1), page_size);
    }
    return error.InvalidBlockId;
}

test "physical KV capacity takes a sparse prefix across retained and reserved pages" {
    const spans = [_][]const KvBlockId{ &.{2}, &.{ 17, 4, 99 } };
    try std.testing.expectEqual(@as(usize, 18 * 16), try physicalTokenCapacity(&spans, 3, 16));
    try std.testing.expectEqual(@as(usize, 100 * 16), try physicalTokenCapacity(&spans, 4, 16));
    try std.testing.expectError(error.InvalidBlockId, physicalTokenCapacity(&spans, 5, 16));
}

pub const KvResidency = enum {
    gpu,
    ram,
    nvme,
};

pub const KvBlockMeta = struct {
    pool_id: KvPoolId,
    block_id: KvBlockId,
    token_capacity: u16,
    tokens_written: u16 = 0,
    refcount: u32 = 1,
    last_access_tick: u64 = 0,
    priority: i32 = 0,
    residency: KvResidency = .gpu,
    prefix_cacheable: bool = false,
    model_tag: u64 = 0,

    pub fn isFull(self: KvBlockMeta) bool {
        return self.tokens_written >= self.token_capacity;
    }

    pub fn hasWritableCapacity(self: KvBlockMeta) bool {
        return self.tokens_written < self.token_capacity;
    }
};

test "block meta capacity helpers" {
    var meta = KvBlockMeta{
        .pool_id = 1,
        .block_id = 2,
        .token_capacity = 16,
    };
    try std.testing.expect(meta.hasWritableCapacity());
    meta.tokens_written = 16;
    try std.testing.expect(meta.isFull());
}
