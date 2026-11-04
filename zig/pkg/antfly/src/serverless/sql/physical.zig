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

//! Durable skew layout. Row identity is (accepted map, partition, row ordinal),
//! independent of retries, queue order and worker placement.
const std = @import("std");
pub const Tile = struct { map: ?u32 = null, rows: u64 = 0 };
pub const page_tiles = 128;
pub const max_index_bytes = 1024 * 1024;
pub const TilePage = struct { first: u32, count: u16, output: @import("state.zig").Output };
pub const Part = struct {
    partition: u16,
    left: []const Tile = &.{},
    right: []const Tile = &.{},
    left_count: ?u32 = null,
    right_count: ?u32 = null,
    left_pages: []const TilePage = &.{},
    right_pages: []const TilePage = &.{},
    pub fn leftCount(self: Part) u32 {
        return self.left_count orelse @intCast(self.left.len);
    }
    pub fn rightCount(self: Part) u32 {
        return self.right_count orelse @intCast(self.right.len);
    }
};
pub const Index = struct { boundaries: []const u16, skew: []const Part };
pub const Pair = struct { part: u16, left: u32, right: u32 };
pub const Work = union(enum) { reduce: u16, pair: Pair };
pub fn pairCount(parts: []const Part, reducers: u32) !u32 {
    var count = try std.math.sub(u32, reducers, std.math.cast(u32, parts.len) orelse return error.SqlJoinBudgetExceeded);
    for (parts) |part| count = try std.math.add(u32, count, try std.math.mul(u32, part.leftCount(), part.rightCount()));
    return count;
}
pub fn work(parts: []const Part, boundaries: []const u16, reducers: u32, id: u32) !Work {
    var next = id;
    for (0..reducers) |reducer| {
        const partition: u16 = if (boundaries.len == 0) @intCast(reducer) else boundaries[reducer];
        const found = for (parts, 0..) |part, index| {
            if (part.partition == partition) break index;
        } else null;
        if (found) |index| {
            const part = parts[index];
            const count = try std.math.mul(u32, part.leftCount(), part.rightCount());
            if (next < count) return .{ .pair = .{ .part = @intCast(index), .left = next / @as(u32, part.rightCount()), .right = next % @as(u32, part.rightCount()) } };
            next -= count;
        } else {
            if (next == 0) return .{ .reduce = @intCast(reducer) };
            next -= 1;
        }
    }
    return error.InvalidSqlExecutionState;
}
pub fn pairId(parts: []const Part, boundaries: []const u16, reducers: u32, target: Pair) !u32 {
    if (target.part >= parts.len) return error.InvalidSqlExecutionState;
    const target_part = parts[target.part];
    if (target.left >= target_part.leftCount() or target.right >= target_part.rightCount()) return error.InvalidSqlExecutionState;
    var offset: u32 = 0;
    for (0..reducers) |reducer| {
        const partition: u16 = if (boundaries.len == 0) @intCast(reducer) else boundaries[reducer];
        const found = for (parts, 0..) |part, index| {
            if (part.partition == partition) break index;
        } else null;
        if (found) |index| {
            if (index == target.part) return std.math.add(u32, offset, try std.math.add(u32, try std.math.mul(u32, target.left, target_part.rightCount()), target.right));
            offset = try std.math.add(u32, offset, try std.math.mul(u32, parts[index].leftCount(), parts[index].rightCount()));
        } else offset = try std.math.add(u32, offset, 1);
    }
    return error.InvalidSqlExecutionState;
}
