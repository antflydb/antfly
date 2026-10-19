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
/// Internal exact binary64 payload. SQL's public float-to-NUMERIC cast rounds
/// to fifteen significant digits; using that cast would round weighted inputs
/// before reducing them. The shared allocation-free decomposition is exact.
fn binaryFloat(ctx: *numeric.Context, input: f64) !numeric.Owned {
    if (!std.math.isFinite(input)) return numeric.fromFloat(ctx, input, false);
    const parts = try @import("../common/json_float_decimal.zig").Parts.init(input);
    try ctx.charge(parts.work() + 1);
    var coefficient_buffer: [768]u8 = undefined;
    const coefficient = try parts.coefficientText(&coefficient_buffer);
    var text_buffer: [800]u8 = undefined;
    return numeric.parse(ctx, try std.fmt.bufPrint(&text_buffer, "{s}{s}e{d}", .{ if (parts.negative) "-" else "", coefficient, parts.decimalExponent() }));
}
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
        const value = if (input.numeric) |value| value.* else if (input.value == .integer) numeric.integerView(input.value.integer, &storage) else if (input.value == .float) (try binaryFloat(&ctx, input.value.float)).value else return error.SqlTypeMismatch;
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
            const mean = if (state.input_type == .number) try binaryFloat(&ctx, state.mean) else try numeric.fromFloat(&ctx, state.mean, false);
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
        if (self.result == null) {
            var total = self.total.value;
            // Keep guard digits for the final binary64 conversion, rather
            // than exposing NUMERIC's default sixteen-digit division policy.
            if (kind == .avg and !integer and !exact and total.kind == .finite) total.scale = @intCast(@min(1000, @as(u32, numeric.divisionScale(total, self.count.value)) + 16));
            self.result = if (kind == .avg) try numeric.divide(&ctx, total, self.count.value) else try numeric.add(&ctx, total, .{});
        }
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

test "wide binary64 averages retain exact inputs and arbitrary weights through spill" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const weight = try Count.init(1 << 63).multiply(arena.allocator(), Count.init(1 << 40));
    for ([_]f64{ 0.1, 1.2345678901234567, 0x1p-1074, std.math.floatMax(f64) }) |input| {
        const state = try State.create(a);
        defer state.deinit();
        try state.add(Datum.json(.{ .float = input }), weight);
        const result = try state.finish(.avg, false, false);
        try std.testing.expectEqual(@as(u64, @bitCast(input)), @as(u64, @bitCast(result.value.float)));
        const aggregate: operators.Aggregate = .{ .alloc = a, .kind = .avg, .input_type = .number, .input_element = .float64, .count = 1, .wide = state };
        const encoded = try @import("aggregate_partial.zig").cell(a, aggregate);
        defer a.free(encoded.value.string);
        var restored = try @import("aggregate_partial.zig").decode(a, encoded, .{ .kind = .avg, .input_type = .number, .input_element = .float64 });
        defer restored.deinit();
        try std.testing.expectEqual(@as(u64, @bitCast(input)), @as(u64, @bitCast((try restored.finish()).value.float)));
    }
    for ([_]f64{ std.math.inf(f64), -std.math.inf(f64), std.math.nan(f64) }) |input| {
        const state = try State.create(a);
        defer state.deinit();
        try state.add(Datum.json(.{ .float = input }), weight);
        const result = (try state.finish(.avg, false, false)).value.float;
        try std.testing.expect(if (std.math.isNan(input)) std.math.isNan(result) else input == result);
    }
}

test "weighted binary64 average grouped promotion preserves counts and allocation failures" {
    const Campaign = struct {
        fn run(a: A) !void {
            const group = try operators.Grouped.create(a, &.{ .{ .kind = .avg, .input_type = .number, .input_element = .float64 }, .{ .kind = .count, .input_type = .integer } }, .{});
            defer group.deinit();
            try group.addWeighted(&.{}, &.{ Datum.json(.{ .float = 0.5 }), Datum.json(.{ .integer = 1 }) }, 3);
            try group.addWeighted(&.{}, &.{ Datum.json(.{ .float = 1.5 }), Datum.json(.{ .integer = 1 }) }, 1);
            const rows = try group.finish(a);
            defer {
                for (rows) |row| a.free(row.aggregates);
                a.free(rows);
            }
            try std.testing.expectEqual(@as(usize, 1), rows.len);
            try std.testing.expectEqual(@as(f64, 0.75), rows[0].aggregates[0].value.float);
            try std.testing.expectEqual(@as(i64, 4), rows[0].aggregates[1].value.integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}
