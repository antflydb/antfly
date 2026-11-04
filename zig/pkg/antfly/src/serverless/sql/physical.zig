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
pub const Part = struct { partition: u16, left: []const Tile, right: []const Tile };
pub const Pair = struct { part: u16, left: u32, right: u32 };
pub const Work = union(enum) { reduce: u16, pair: Pair };
pub fn pairCount(parts: []const Part, reducers: u32) !u32 {
    var count = try std.math.sub(u32, reducers, std.math.cast(u32, parts.len) orelse return error.SqlJoinBudgetExceeded);
    for (parts) |part| count = try std.math.add(u32, count, try std.math.mul(u32, @intCast(part.left.len), @intCast(part.right.len)));
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
            const count = try std.math.mul(u32, @intCast(part.left.len), @intCast(part.right.len));
            if (next < count) return .{ .pair = .{ .part = @intCast(index), .left = next / @as(u32, @intCast(part.right.len)), .right = next % @as(u32, @intCast(part.right.len)) } };
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
    if (target.left >= target_part.left.len or target.right >= target_part.right.len) return error.InvalidSqlExecutionState;
    var offset: u32 = 0;
    for (0..reducers) |reducer| {
        const partition: u16 = if (boundaries.len == 0) @intCast(reducer) else boundaries[reducer];
        const found = for (parts, 0..) |part, index| {
            if (part.partition == partition) break index;
        } else null;
        if (found) |index| {
            if (index == target.part) return std.math.add(u32, offset, try std.math.add(u32, try std.math.mul(u32, target.left, @intCast(target_part.right.len)), target.right));
            offset = try std.math.add(u32, offset, try std.math.mul(u32, @intCast(parts[index].left.len), @intCast(parts[index].right.len)));
        } else offset = try std.math.add(u32, offset, 1);
    }
    return error.InvalidSqlExecutionState;
}
