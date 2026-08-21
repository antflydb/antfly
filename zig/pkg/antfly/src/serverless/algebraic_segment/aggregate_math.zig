// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Canonical checked arithmetic for algebraic aggregates. Query fallback and
//! persisted sidecars must use the same operations so optimization never changes
//! results and hostile numeric inputs cannot trap or silently wrap.

const std = @import("std");
const types = @import("types.zig");

pub fn combine(lhs: types.AggregateValue, rhs: types.AggregateValue) !types.AggregateValue {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return error.AlgebraicAggregateTypeMismatch;
    return switch (lhs) {
        .count => |left| .{ .count = std.math.add(u64, left, rhs.count) catch return error.AlgebraicAggregateOverflow },
        .sum_i64 => |left| .{ .sum_i64 = std.math.add(i64, left, rhs.sum_i64) catch return error.AlgebraicAggregateOverflow },
        .min_i64 => |left| .{ .min_i64 = @min(left, rhs.min_i64) },
        .max_i64 => |left| .{ .max_i64 = @max(left, rhs.max_i64) },
        .avg_i64 => |left| .{ .avg_i64 = .{
            .sum_i64 = std.math.add(i64, left.sum_i64, rhs.avg_i64.sum_i64) catch return error.AlgebraicAggregateOverflow,
            .count = std.math.add(u64, left.count, rhs.avg_i64.count) catch return error.AlgebraicAggregateOverflow,
        } },
    };
}

pub fn addCount(current: u64, additional: usize) !u64 {
    const value = std.math.cast(u64, additional) orelse return error.AlgebraicAggregateOverflow;
    return std.math.add(u64, current, value) catch return error.AlgebraicAggregateOverflow;
}

pub fn addI64(current: i64, value: i64) !i64 {
    return std.math.add(i64, current, value) catch return error.AlgebraicAggregateOverflow;
}

test "aggregate math rejects overflow instead of trapping or wrapping" {
    try std.testing.expectError(
        error.AlgebraicAggregateOverflow,
        combine(.{ .sum_i64 = std.math.maxInt(i64) }, .{ .sum_i64 = 1 }),
    );
    try std.testing.expectError(
        error.AlgebraicAggregateOverflow,
        combine(.{ .avg_i64 = .{ .sum_i64 = std.math.minInt(i64), .count = 1 } }, .{ .avg_i64 = .{ .sum_i64 = -1, .count = 1 } }),
    );
    try std.testing.expectError(error.AlgebraicAggregateOverflow, addCount(std.math.maxInt(u64), 1));
}
