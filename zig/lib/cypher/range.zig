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

//! A checked arithmetic sequence. Its cardinality can exceed machine allocation
//! sizes; only consumers that actually retain cells need an allocation budget.
const std = @import("std");
const v = @import("value.zig");
pub const Range = struct {
    start: i64,
    step: i64,
    length: u128,
    pub fn init(start: v.Value, end: v.Value, step: v.Value) !?Range {
        if (start == .null or end == .null or step == .null) return null;
        if (start != .integer or end != .integer or step != .integer) return error.CypherTypeMismatch;
        if (step.integer == 0) return error.CypherInvalidRange;
        const distance = @as(i128, end.integer) - start.integer;
        const length: u128 = if ((distance > 0 and step.integer < 0) or (distance < 0 and step.integer > 0)) 0 else @intCast(@divTrunc(distance, step.integer) + 1);
        return .{ .start = start.integer, .step = step.integer, .length = length };
    }
    pub fn at(self: Range, ordinal: u128) v.Value {
        std.debug.assert(ordinal < self.length);
        return .{ .integer = @intCast(@as(i128, self.start) + @as(i128, @intCast(ordinal)) * self.step) };
    }
    pub fn index(self: Range, position: i64) v.Value {
        const ordinal: i128 = if (position < 0) @as(i128, @intCast(self.length)) + position else position;
        return if (ordinal < 0 or ordinal >= self.length) .null else self.at(@intCast(ordinal));
    }
    pub fn slice(self: Range, first: ?i64, last: ?i64) struct { start: u128, length: u128 } {
        const length: i128 = @intCast(self.length);
        const begin: i128 = if (first) |n| if (n < 0) length + n else n else 0;
        const end: i128 = if (last) |n| if (n < 0) length + n else n else length;
        const bounded = std.math.clamp(begin, 0, length);
        return .{ .start = @intCast(bounded), .length = @intCast(@max(bounded, std.math.clamp(end, 0, length)) - bounded) };
    }
};

test "range descriptor preserves signs inclusive bounds and full signed-domain indices" {
    const range = (try Range.init(.{ .integer = std.math.minInt(i64) }, .{ .integer = std.math.maxInt(i64) }, .{ .integer = 1 })).?;
    try std.testing.expectEqual(@as(u128, 1) << 64, range.length);
    try std.testing.expectEqual(std.math.minInt(i64), range.index(0).integer);
    try std.testing.expectEqual(std.math.maxInt(i64), range.index(-1).integer);
    const descending = (try Range.init(.{ .integer = 5 }, .{ .integer = -5 }, .{ .integer = -3 })).?;
    try std.testing.expectEqual(@as(u128, 4), descending.length);
    try std.testing.expectEqual(@as(i64, -4), descending.index(-1).integer);
    try std.testing.expectEqual(@as(u128, 1), descending.slice(-2, -1).length);
    try std.testing.expect((try Range.init(.null, .{ .integer = 1 }, .{ .integer = 0 })) == null);
    try std.testing.expectError(error.CypherInvalidRange, Range.init(.{ .integer = 1 }, .{ .integer = 2 }, .{ .integer = 0 }));
}
