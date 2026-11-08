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

//! Integer bounds and producer geometry obligations for scalar statistics. Unknown
//! bounds decline compression; interval failures never become query errors.
const std = @import("std");
pub const Interval = struct {
    low: i64,
    high: i64,
    pub fn literal(value: i64) Interval {
        return .{ .low = value, .high = value };
    }
    pub fn negative(self: Interval) ?Interval {
        return .{ .low = std.math.negate(self.high) catch return null, .high = std.math.negate(self.low) catch return null };
    }
    pub fn combine(left: Interval, right: Interval, op: enum { add, subtract, multiply }) ?Interval {
        const low: i128 = switch (op) {
            .add => @as(i128, left.low) + right.low,
            .subtract => @as(i128, left.low) - right.high,
            .multiply => @min(@as(i128, left.low) * right.low, @as(i128, left.low) * right.high, @as(i128, left.high) * right.low, @as(i128, left.high) * right.high),
        };
        const high: i128 = switch (op) {
            .add => @as(i128, left.high) + right.high,
            .subtract => @as(i128, left.high) - right.low,
            .multiply => @max(@as(i128, left.low) * right.low, @as(i128, left.low) * right.high, @as(i128, left.high) * right.low, @as(i128, left.high) * right.high),
        };
        return .{ .low = std.math.cast(i64, low) orelse return null, .high = std.math.cast(i64, high) orelse return null };
    }
};
pub const Field = struct { name: []const u8, bounds: ?Interval, numeric: ?@import("linear_numeric.zig").Form = null, polynomial: ?@import("statistic.zig").Form = null };
pub const Check = struct {
    ptr: *const anyopaque,
    statistic: ?*const fn (*const anyopaque, std.mem.Allocator, []const Field) anyerror!?@import("statistic.zig").Form = null,
    statistic_kind: @import("statistic.zig").Kind = .avg,
    statistic_global: bool = false,

    total_only: bool = false,
    numeric_required: bool = false,
    scalars: []const ?@import("linear_numeric.zig").Lattice = &.{},
    /// Invocation-local known integer slots for composed proof programs.
    /// NULL and other types remain unknown; they never become zero.
    integers: []const ?i64 = &.{},
    validate: *const fn (*const anyopaque, std.mem.Allocator, []const Field) anyerror!bool,
    pub fn statisticProof(self: Check, a: std.mem.Allocator, fields: []const Field) !?@import("statistic.zig").Form {
        return if (self.statistic) |f| try f(self.ptr, a, fields) else null;
    }

    pub fn accepts(self: Check, a: std.mem.Allocator, fields: []const Field) !bool {
        return self.validate(self.ptr, a, fields);
    }
};
