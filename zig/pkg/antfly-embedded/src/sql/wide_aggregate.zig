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

//! Promoted exact bag reductions. Ordinary SQL rows retain the compact native
//! states. Only a wide contribution promotes a column to these owned states;
//! count and total stay exact through partial merges and spill.
const std = @import("std");
const numeric = @import("numeric_value.zig");
const operators = @import("operators.zig");
const Datum = @import("scalar.zig").Datum;
const Count = @import("catalog.zig").Count;
const A = std.mem.Allocator;
pub const State = struct {
    a: A,
    count: numeric.Owned,
    total: numeric.Owned,
    result: ?numeric.Owned = null,
    remaining: u64 = 8 * 1024 * 1024,
    pub fn create(a: A) !*State {
        const self = try a.create(State);
        self.* = .{ .a = a, .count = .{ .alloc = a, .value = .{} }, .total = .{ .alloc = a, .value = .{} } };
        return self;
    }
    pub fn deinit(self: *State) void {
        const a = self.a;
        self.count.deinit();
        self.total.deinit();
        if (self.result) |*result| result.deinit();
        a.destroy(self);
    }
    fn invalidate(self: *State) void {
        if (self.result) |*result| result.deinit();
        self.result = null;
    }
    pub fn clone(self: *const State, a: A) !*State {
        const result = try create(a);
        errdefer result.deinit();
        try result.merge(self);
        return result;
    }
    pub fn merge(self: *State, other: *const State) !void {
        var ctx: numeric.Context = .{ .alloc = self.a, .allow_wide_weight = true, .remaining = self.remaining };
        defer self.remaining = @min(self.remaining, ctx.remaining);
        var count = try numeric.add(&ctx, self.count.value, other.count.value);
        errdefer count.deinit();
        const total = try numeric.add(&ctx, self.total.value, other.total.value);
        self.count.deinit();
        self.total.deinit();
        self.count = count;
        self.total = total;
        self.invalidate();
    }
    pub fn add(self: *State, input: Datum, weight: Count) !void {
        if (input.sql_null) return;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        var ctx: numeric.Context = .{ .alloc = arena.allocator(), .allow_wide_weight = true, .remaining = self.remaining };
        defer self.remaining = @min(self.remaining, ctx.remaining);
        const count = try numeric.parse(&ctx, try weight.text(ctx.alloc));
        var storage: [5]u16 = undefined;
        const value = if (input.numeric) |value| value.* else if (input.value == .integer) numeric.integerView(input.value.integer, &storage) else return error.SqlTypeMismatch;
        const total = try numeric.multiply(&ctx, value, count.value);
        var other: State = .{ .a = ctx.alloc, .count = count, .total = total };
        self.remaining = ctx.remaining;
        try self.merge(&other);
    }
    pub fn fromPartial(a: A, state: operators.Aggregate) !*State {
        if (state.wide) |wide| return wide.clone(a);
        const result = try create(a);
        errdefer result.deinit();
        if (state.count == 0) return result;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var ctx: numeric.Context = .{ .alloc = arena.allocator(), .allow_wide_weight = true };
        const count = try numeric.parse(&ctx, try std.fmt.allocPrint(ctx.alloc, "{d}", .{state.count}));
        const total = if (state.numeric) |reducer| try reducer.state.total(&ctx, false) else if (state.kind == .sum and state.input_type == .integer) try numeric.parse(&ctx, try std.fmt.allocPrint(ctx.alloc, "{d}", .{state.integer_sum})) else if (state.kind == .avg) blk: {
            const mean = try numeric.fromFloat(&ctx, state.mean, false);
            break :blk try numeric.multiply(&ctx, mean.value, count.value);
        } else return error.InvalidSqlBackendResponse;
        var other: State = .{ .a = ctx.alloc, .count = count, .total = total };
        try result.merge(&other);
        return result;
    }
    pub fn finish(self: *State, kind: operators.Aggregate.Kind, integer: bool, exact: bool) !Datum {
        if (self.count.value.isZero()) return .{};
        var ctx: numeric.Context = .{ .alloc = self.a, .allow_wide_weight = true, .remaining = self.remaining };
        defer self.remaining = @min(self.remaining, ctx.remaining);
        if (kind == .sum and integer) return Datum.json(.{ .integer = numeric.toInteger(i64, &ctx, self.total.value) catch |err| return if (err == error.InvalidSqlNumber) error.SqlNumericOutOfRange else err });
        if (self.result == null) self.result = if (kind == .avg) try numeric.divide(&ctx, self.total.value, self.count.value) else try numeric.add(&ctx, self.total.value, .{});
        if (exact) {
            var public: numeric.Context = .{ .alloc = self.a };
            numeric.validateCanonical(&public, self.result.?.value) catch |err| return if (err == error.InvalidNumericRepresentation) error.SqlNumericOutOfRange else err;
            return Datum.typedNumeric(&self.result.?.value);
        }
        const text = try numeric.format(&ctx, self.result.?.value);
        defer self.a.free(text);
        const number = std.fmt.parseFloat(f64, text) catch return error.SqlNumericOutOfRange;
        if (self.result.?.value.kind == .finite and !std.math.isFinite(number)) return error.SqlNumericOutOfRange;
        return Datum.json(.{ .float = number });
    }
};

test "wide numeric averages and cancellation admit intermediates beyond result exponents" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var ctx: numeric.Context = .{ .alloc = arena.allocator() };
    const input = try numeric.parse(&ctx, "1e131068");
    const negative = try numeric.parse(&ctx, "-1e131068");
    const count = try Count.init(1 << 63).multiply(arena.allocator(), Count.init(1 << 40));
    const state = try State.create(a);
    defer state.deinit();
    try state.add(Datum.typedNumeric(&input.value), count);
    const average = try state.finish(.avg, false, true);
    try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, input.value, average.numeric.?.*));
    state.invalidate();
    try std.testing.expectError(error.SqlNumericOutOfRange, state.finish(.sum, false, true));
    try state.add(Datum.typedNumeric(&negative.value), count);
    try std.testing.expect((try state.finish(.sum, false, true)).numeric.?.isZero());
}
