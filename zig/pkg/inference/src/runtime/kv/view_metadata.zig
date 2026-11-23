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

//! Checked growth and cold host bound for state-owned KV view metadata.
const std = @import("std");
const BlockId = @import("block.zig").KvBlockId;
pub const Table = std.ArrayListUnmanaged(BlockId);
pub const History = std.ArrayListUnmanaged(Table);

pub fn capacityForBlocks(blocks: usize) !usize {
    return if (blocks == 0) 0 else try std.math.ceilPowerOfTwo(usize, blocks);
}

fn allocationBytes(comptime T: type, capacity: usize, admitted: bool) !usize {
    const bytes = try std.math.mul(usize, capacity, @sizeOf(T));
    return if (admitted) try @import("../../backends/admitted_allocator.zig").AdmittedAllocator.allocationHostBytes(bytes, .of(T)) else bytes;
}

/// All geometrically retained table payloads through the maximum token budget,
/// plus owner-record backing and its possible resize overlap. These are actual
/// state-allocator allocations; an admitted allocator adds its literal Header
/// to each allocation. RunScope reservation metadata remains with that owner.
pub fn hostBytes(max_tokens: usize, page_size: usize, admitted: bool) !usize {
    if (page_size == 0) return error.InvalidPoolId;
    if (max_tokens == 0) return 0;
    // Existing replay reservations may carry one spare for a shared tail COW.
    const blocks = try std.math.add(usize, try std.math.divCeil(usize, max_tokens, page_size), 1);
    const max_capacity = try capacityForBlocks(blocks);
    var total: usize = 0;
    var capacity: usize = 1;
    var histories: usize = 0;
    while (true) {
        total = try std.math.add(usize, total, try allocationBytes(BlockId, capacity, admitted));
        if (capacity == max_capacity) break;
        capacity = try std.math.mul(usize, capacity, 2);
        histories += 1;
    }
    if (histories != 0) {
        const records = try capacityForBlocks(histories);
        total = try std.math.add(usize, total, try allocationBytes(Table, records, admitted));
        // Precise allocator remap may allocate the replacement before freeing
        // the previous records. Historical table payloads stay independently owned.
        if (records > 1) total = try std.math.add(usize, total, try allocationBytes(Table, records / 2, admitted));
    }
    return total;
}

test "KV view metadata cold bound covers actual precise growth and retained owners" {
    var allocator = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const a = allocator.allocator();
    var history: History = .empty;
    defer {
        for (history.items) |*table| table.deinit(a);
        history.deinit(a);
    }
    var table: Table = .empty;
    defer table.deinit(a);
    const bound = try hostBytes(257, 2, false);
    var capacity: usize = 1;
    while (capacity <= 256) : (capacity *= 2) {
        var next: Table = .empty;
        errdefer next.deinit(a);
        try next.ensureTotalCapacityPrecise(a, try capacityForBlocks(capacity));
        if (table.capacity != 0) {
            try history.ensureTotalCapacityPrecise(a, try capacityForBlocks(history.items.len + 1));
            history.appendAssumeCapacity(table);
        }
        table = next;
        next = .empty;
        try std.testing.expect(allocator.allocated_bytes - allocator.freed_bytes <= bound);
    }
    try std.testing.expect(try hostBytes(257, 2, true) > bound);
    try std.testing.expectError(error.InvalidPoolId, hostBytes(1, 0, false));
    try std.testing.expectError(error.Overflow, hostBytes(std.math.maxInt(usize), 1, false));
}
