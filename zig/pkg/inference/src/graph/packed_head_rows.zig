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

//! Vocabulary projection only needs decode and final-prefix rows. This
//! bounded mapping restores scheduler order after the smaller device argmax.
const std = @import("std");
pub const capacity = 128;
pub const PrefixLogitsLayout = enum { all_host_rows, final_host_rows };
/// SAME original packed packet peak: independent CPU logits/publication
/// copies, three-axis positions, head plan/selection metadata, and the
/// authored32/64KiB mixed-context allowance. Never subtract outer PAGE floors.
pub fn hostPacketBytes(items: usize, total_query_rows: usize, host_rows: usize, vocab: usize) !usize {
    if (items == 0 or items > capacity or total_query_rows == 0 or host_rows > items or vocab == 0) return error.InvalidPackedHostGeometry;
    const logits = try std.math.mul(usize, try std.math.mul(usize, host_rows, vocab), 2 * @sizeOf(f32));
    const positions = try std.math.mul(usize, total_query_rows, 3 * @sizeOf(u32));
    const head_metadata = try std.math.add(usize, @sizeOf(Plan) + @sizeOf([capacity]bool), try std.math.mul(usize, items, @sizeOf(u32)));
    return std.math.add(usize, try std.math.add(usize, logits, positions), try std.math.add(usize, (if (items > 64) @as(usize, 64) else 32) * 1024, head_metadata));
}

pub const Plan = struct {
    items: [capacity]usize = undefined,
    row_by_item: [capacity]usize = @splat(std.math.maxInt(usize)),
    count: usize = 0,
    full_rows: usize = 0,

    pub fn init(active: []const bool) !Plan {
        if (active.len == 0 or active.len > capacity) return error.InvalidBatchDecodeState;
        var plan = Plan{ .full_rows = active.len };
        for (active, 0..) |is_active, item| if (is_active) {
            plan.items[plan.count] = item;
            plan.row_by_item[item] = plan.count;
            plan.count += 1;
        };
        return plan;
    }

    pub fn scatter(self: *const Plan, allocator: std.mem.Allocator, compact: []const u32, vocab: usize) ![]u32 {
        if (self.full_rows == 0 or self.full_rows > capacity or self.count == 0 or self.count > self.full_rows or compact.len != self.count or vocab == 0) return error.InvalidBatchDecodeState;
        var seen: [capacity]bool = @splat(false);
        for (self.items[0..self.count], compact, 0..) |item, token, row| {
            if (item >= self.full_rows or seen[item] or token >= vocab or self.row_by_item[item] != row) return error.InvalidBatchDecodeState;
            seen[item] = true;
        }
        const result = try allocator.alloc(u32, self.full_rows);
        @memset(result, 0);
        for (self.items[0..self.count], compact) |item, token| result[item] = token;
        return result;
    }
};

test "LightOnOCR3 CUDA packed head rows preserve scheduler order through prefix holes and the 128 page boundary" {
    const a = std.testing.allocator;
    for ([_]usize{ 1, 7, 64, 65, 128 }) |rows| {
        var active: [capacity]bool = @splat(false);
        for (active[0..rows], 0..) |*value, i| value.* = i % 3 != 0 or i + 1 == rows;
        const plan = try Plan.init(active[0..rows]);
        var ids: [capacity]u32 = undefined;
        for (plan.items[0..plan.count], 0..) |item, row| ids[row] = @intCast(item + 1);
        const full = try plan.scatter(a, ids[0..plan.count], capacity + 1);
        defer a.free(full);
        for (full, active[0..rows], 0..) |id, selected, item| try std.testing.expectEqual(if (selected) @as(u32, @intCast(item + 1)) else 0, id);
    }
    try std.testing.expectError(error.InvalidBatchDecodeState, Plan.init(&.{}));
    const oversized: [capacity + 1]bool = @splat(true);
    try std.testing.expectError(error.InvalidBatchDecodeState, Plan.init(&oversized));
    const empty = try Plan.init(&.{ false, false });
    try std.testing.expectError(error.InvalidBatchDecodeState, empty.scatter(a, &.{}, 10));
}

test "LightOnOCR3 CUDA packed head rows reject malformed compact choices before publication" {
    const a = std.testing.allocator;
    const good = try Plan.init(&.{ false, true, false, true });
    try std.testing.expectError(error.InvalidBatchDecodeState, good.scatter(a, &.{1}, 10));
    try std.testing.expectError(error.InvalidBatchDecodeState, good.scatter(a, &.{ 1, 10 }, 10));
    var duplicate = good;
    duplicate.items[1] = duplicate.items[0];
    try std.testing.expectError(error.InvalidBatchDecodeState, duplicate.scatter(a, &.{ 1, 2 }, 10));
    var outside = good;
    outside.items[1] = 4;
    try std.testing.expectError(error.InvalidBatchDecodeState, outside.scatter(a, &.{ 1, 2 }, 10));
    var mismatched = good;
    mismatched.row_by_item[3] = 0;
    try std.testing.expectError(error.InvalidBatchDecodeState, mismatched.scatter(a, &.{ 1, 2 }, 10));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, good.scatter(failing.allocator(), &.{ 1, 2 }, 10));
}

test "packed host packet geometry preserves actual mixed 35028 floor and checked wide logits overlap" {
    const metadata2 = @sizeOf(Plan) + @sizeOf([capacity]bool) + 2 * @sizeOf(u32);
    try std.testing.expectEqual(32 * 1024 + metadata2 + 5 * 3 * @sizeOf(u32), try hostPacketBytes(2, 5, 0, 64));
    if (@sizeOf(usize) == 8) try std.testing.expectEqual(@as(usize, 35028), try hostPacketBytes(2, 5, 0, 64));
    try std.testing.expectEqual(try hostPacketBytes(2, 5, 0, 64) + 2 * 64 * @sizeOf(f32), try hostPacketBytes(2, 5, 1, 64));
    const wide = 64 * 1024 + @sizeOf(Plan) + @sizeOf([capacity]bool) + capacity * @sizeOf(u32) + capacity * 3 * @sizeOf(u32) + capacity * 64 * 2 * @sizeOf(f32);
    try std.testing.expectEqual(wide, try hostPacketBytes(capacity, capacity, capacity, 64));
    try std.testing.expectError(error.InvalidPackedHostGeometry, hostPacketBytes(0, 1, 0, 64));
    try std.testing.expectError(error.InvalidPackedHostGeometry, hostPacketBytes(capacity + 1, 1, 0, 64));
    try std.testing.expectError(error.InvalidPackedHostGeometry, hostPacketBytes(2, 0, 0, 64));
    try std.testing.expectError(error.InvalidPackedHostGeometry, hostPacketBytes(2, 5, 3, 64));
    try std.testing.expectError(error.Overflow, hostPacketBytes(2, std.math.maxInt(usize), 0, 64));
    try std.testing.expectError(error.Overflow, hostPacketBytes(2, 5, 2, std.math.maxInt(usize)));
}
