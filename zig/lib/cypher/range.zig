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
    /// Equality against integer cells is exact, including integral DOUBLE
    /// needles near i64 boundaries. Empty lists precede NULL propagation.
    pub fn contains(self: Range, needle: v.Value) v.Value {
        if (self.length == 0) return .{ .boolean = false };
        const integer: i64 = switch (needle) {
            .null => return .null,
            .integer => |n| n,
            .number => |n| blk: {
                if (!std.math.isFinite(n) or n < -9223372036854775808.0 or n >= 9223372036854775808.0 or @trunc(n) != n) return .{ .boolean = false };
                break :blk @intFromFloat(n);
            },
            else => return .{ .boolean = false },
        };
        const distance = @as(i128, integer) - self.start;
        const ordinal = @divTrunc(distance, self.step);
        return .{ .boolean = @rem(distance, self.step) == 0 and ordinal >= 0 and ordinal < self.length };
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

/// A finite integer lattice interval. Direction and duplicate occurrences do
/// not affect set membership. Wider steps occur when intersecting lattices.
pub const Run = struct {
    low: i128,
    high: i128,
    step: i128,
    pub fn from(range: Range) Run {
        const last = range.at(range.length - 1).integer;
        return .{ .low = @min(range.start, last), .high = @max(range.start, last), .step = if (range.length == 1) 1 else @intCast(@abs(@as(i128, range.step))) };
    }
    fn length(self: Run) u128 {
        return @intCast(@divTrunc(self.high - self.low, self.step) + 1);
    }
    pub fn intersect(a: Run, b: Run) ?Run {
        const low = @max(a.low, b.low);
        const high = @min(a.high, b.high);
        if (low > high) return null;
        // Extended Euclid supplies the inverse for the Chinese remainder
        // equation. i256 covers products of full signed-domain strides.
        var r: i256 = a.step;
        var next_r: i256 = b.step;
        var coefficient: i256 = 1;
        var next_coefficient: i256 = 0;
        while (next_r != 0) {
            const q = @divTrunc(r, next_r);
            const rest = r - q * next_r;
            r = next_r;
            next_r = rest;
            const next = coefficient - q * next_coefficient;
            coefficient = next_coefficient;
            next_coefficient = next;
        }
        const delta: i256 = b.low - a.low;
        if (@mod(delta, r) != 0) return null;
        const modulus = @divTrunc(@as(i256, b.step), r);
        const ordinal = @mod(@divTrunc(delta, r) * coefficient, modulus);
        const stride = @as(i256, a.step) * modulus;
        const base = @as(i256, a.low) + @as(i256, a.step) * ordinal;
        const first = base + @divFloor(@as(i256, low) - base + stride - 1, stride) * stride;
        if (first > high) return null;
        const last = first + @divTrunc(@as(i256, high) - first, stride) * stride;
        return .{ .low = @intCast(first), .high = @intCast(last), .step = if (first == last) 1 else @intCast(stride) };
    }
};

/// Merge exact set support when two sequences form one lattice interval.
/// Bag cardinality and encounter order remain in the caller's source DAG.
pub fn mergeCompatible(a: Range, b: Range) ?Range {
    if (a.length == 0) return b;
    if (b.length == 0) return a;
    const left = Run.from(a);
    const right = Run.from(b);
    var step = left.step;
    if (a.length == 1 and b.length == 1) {
        if (left.low == right.low) return a;
        step = @intCast(@abs(left.low - right.low));
    } else if (a.length == 1) {
        step = right.step;
    } else if (b.length != 1 and left.step != right.step) return null;
    if (step > @as(i128, 1) << 63 or @mod(left.low - right.low, step) != 0) return null;
    const low = @min(left.low, right.low);
    const high = @max(left.high, right.high);
    if (@max(left.low, right.low) > @min(left.high, right.high) + step) return null;
    const length: u128 = @intCast(@divTrunc(high - low, step) + 1);
    return if (step <= std.math.maxInt(i64))
        .{ .start = @intCast(low), .step = @intCast(step), .length = length }
    else
        .{ .start = @intCast(high), .step = std.math.minInt(i64), .length = length };
}

/// Exact union cardinality without visiting values, including overlapping
/// ranges with different strides. Inclusion/exclusion terms are coalesced;
/// bounded certificate complexity declines to the ordinary streaming reducer.
/// The cap bounds planning work, never approximates a DISTINCT result.
pub fn distinctCardinality(a: std.mem.Allocator, ranges: []const Range) !?u128 {
    const NoWork = struct {
        fn charge(_: *anyopaque, _: usize) anyerror!void {}
    };
    var placeholder: u8 = 0;
    return distinctCardinalityWork(a, ranges, &placeholder, NoWork.charge);
}
pub fn distinctCardinalityWork(a: std.mem.Allocator, ranges: []const Range, ptr: *anyopaque, charge: *const fn (*anyopaque, usize) anyerror!void) !?u128 {
    return if (try unionMeasureWork(a, ranges, ptr, charge)) |measure| measure.count else null;
}

/// Exact integer set moments. Negative and positive totals also determine the
/// ordered prefix bounds of any monotone sequence with previously seen values
/// removed. All signed-i64 sets fit these i128 moments, even the full domain.
pub const Measure = struct {
    count: u128 = 0,
    negative: i128 = 0,
    positive: i128 = 0,
    pub fn of(range: Range) @This() {
        if (range.length == 0) return .{};
        var wide: WideMeasure = .{};
        wide.add(Run.from(range), 1);
        return .{ .count = @intCast(wide.count), .negative = @intCast(wide.negative), .positive = @intCast(wide.positive) };
    }
    pub fn total(self: @This()) i128 {
        return self.negative + self.positive;
    }
    pub fn difference(self: @This(), prior: @This()) @This() {
        return .{ .count = self.count - prior.count, .negative = self.negative - prior.negative, .positive = self.positive - prior.positive };
    }
};
const WideMeasure = struct {
    count: i256 = 0,
    negative: i256 = 0,
    positive: i256 = 0,
    fn add(self: *@This(), run: Run, coefficient: i128) void {
        const n: i256 = @intCast(run.length());
        const low: i256 = run.low;
        const step: i256 = run.step;
        const total = @divExact(n * (2 * low + (n - 1) * step), 2);
        const negatives = if (low >= 0) 0 else @min(n, @divFloor(-low + step - 1, step));
        const negative = @divExact(negatives * (2 * low + (negatives - 1) * step), 2);
        self.count += n * coefficient;
        self.negative += negative * coefficient;
        self.positive += (total - negative) * coefficient;
    }
};
pub fn unionMeasureWork(a: std.mem.Allocator, ranges: []const Range, ptr: *anyopaque, charge: *const fn (*anyopaque, usize) anyerror!void) !?Measure {
    const Term = struct { run: Run, coefficient: i128 };
    var runs: std.ArrayList(Run) = .empty;
    defer runs.deinit(a);
    var enumeration: u128 = 0;
    for (ranges) |range| {
        try charge(ptr, 1);
        enumeration +|= range.length;
        if (range.length != 0) try runs.append(a, Run.from(range));
    }
    // Compatible lattice intervals merge before the general CRT prover. This
    // covers ordinary ranges and fragmented singleton sources in O(n log n).
    const Order = struct {
        fn lattice(_: void, x: Run, y: Run) bool {
            if (x.step != y.step) return x.step < y.step;
            const xr = @mod(x.low, x.step);
            const yr = @mod(y.low, y.step);
            return if (xr != yr) xr < yr else x.low < y.low;
        }
        fn low(_: void, x: Run, y: Run) bool {
            return x.low < y.low;
        }
    };
    const sort_work = runs.items.len *| @as(usize, if (runs.items.len == 0) 0 else @bitSizeOf(usize) - @clz(runs.items.len));
    try charge(ptr, sort_work);
    std.mem.sort(Run, runs.items, {}, Order.lattice);
    var length: usize = 0;
    for (runs.items) |run| {
        if (length != 0) {
            const previous = &runs.items[length - 1];
            if (previous.step == run.step and @mod(previous.low, run.step) == @mod(run.low, run.step) and run.low <= previous.high + run.step) {
                previous.high = @max(previous.high, run.high);
                continue;
            }
        }
        runs.items[length] = run;
        length += 1;
    }
    runs.shrinkRetainingCapacity(length);
    try charge(ptr, length *| @as(usize, if (length == 0) 0 else @bitSizeOf(usize) - @clz(length)));
    std.mem.sort(Run, runs.items, {}, Order.low);
    var terms: std.ArrayList(Term) = .empty;
    defer terms.deinit(a);
    var pending: std.ArrayList(Term) = .empty;
    defer pending.deinit(a);
    // General intersections must earn their cost relative to enumeration.
    // Exhausting this proof budget declines; it never consumes the caller's
    // remaining state budget just to establish that a certificate is too hard.
    var remaining: usize = @intCast(@min(@as(u128, 65536), enumeration *| 2));
    var result: WideMeasure = .{};
    for (runs.items) |run| {
        pending.clearRetainingCapacity();
        try pending.append(a, .{ .run = run, .coefficient = 1 });
        var i: usize = 0;
        while (i < terms.items.len) {
            const term = terms.items[i];
            if (remaining == 0) return null;
            remaining -= 1;
            try charge(ptr, 1);
            if (term.run.high < run.low) {
                result.add(term.run, term.coefficient);
                _ = terms.swapRemove(i);
                continue;
            }
            if (Run.intersect(run, term.run)) |intersection| try pending.append(a, .{ .run = intersection, .coefficient = -term.coefficient });
            i += 1;
        }
        for (pending.items) |term| {
            var found = false;
            for (terms.items, 0..) |existing, position| {
                if (remaining == 0) return null;
                remaining -= 1;
                try charge(ptr, 1);
                if (std.meta.eql(term.run, existing.run)) {
                    terms.items[position].coefficient = std.math.add(i128, existing.coefficient, term.coefficient) catch return null;
                    if (std.math.cast(i64, terms.items[position].coefficient) == null) return null;
                    if (terms.items[position].coefficient == 0) _ = terms.swapRemove(position);
                    found = true;
                    break;
                }
            }
            if (!found) {
                if (terms.items.len >= 256) return null;
                try terms.append(a, term);
            }
        }
    }
    for (terms.items) |term| result.add(term.run, term.coefficient);
    return .{ .count = @intCast(result.count), .negative = @intCast(result.negative), .positive = @intCast(result.positive) };
}

test "arithmetic domains count overlaps strides descending and signed boundaries" {
    const a = std.testing.allocator;
    try std.testing.expectEqual(@as(?u128, 10), try distinctCardinality(a, &.{
        .{ .start = 1, .step = 2, .length = 5 },
        .{ .start = 10, .step = -2, .length = 5 },
        .{ .start = 3, .step = 3, .length = 3 },
    }));
    try std.testing.expectEqual(@as(?u128, @as(u128, 1) << 64), try distinctCardinality(a, &.{
        .{ .start = std.math.minInt(i64), .step = 1, .length = @as(u128, 1) << 64 },
        .{ .start = std.math.maxInt(i64), .step = std.math.minInt(i64), .length = 2 },
    }));
}

test "arithmetic domain certificates agree with enumeration for arbitrary strides" {
    var prng: std.Random.DefaultPrng = .init(1739);
    const random = prng.random();
    var proved: usize = 0;
    for (0..3000) |_| {
        var ranges: [5]Range = undefined;
        var seen: [61]bool = @splat(false);
        for (&ranges) |*range| {
            const start = random.intRangeAtMost(i64, -30, 30);
            const end = random.intRangeAtMost(i64, -30, 30);
            var step = random.intRangeAtMost(i64, 1, 8);
            if (random.boolean()) step = -step;
            range.* = (try Range.init(.{ .integer = start }, .{ .integer = end }, .{ .integer = step })).?;
            var i: u128 = 0;
            while (i < range.length) : (i += 1) seen[@intCast(range.at(i).integer + 30)] = true;
        }
        if (mergeCompatible(ranges[0], ranges[1])) |merged| {
            var expected: [61]bool = @splat(false);
            for (ranges[0..2]) |part| {
                var ordinal: u128 = 0;
                while (ordinal < part.length) : (ordinal += 1) expected[@intCast(part.at(ordinal).integer + 30)] = true;
            }
            var actual: [61]bool = @splat(false);
            var ordinal: u128 = 0;
            while (ordinal < merged.length) : (ordinal += 1) actual[@intCast(merged.at(ordinal).integer + 30)] = true;
            try std.testing.expectEqualSlices(bool, &expected, &actual);
        }
        var expected: Measure = .{};
        for (seen, 0..) |present, index| if (present) {
            const integer = @as(i128, @intCast(index)) - 30;
            expected.count += 1;
            if (integer < 0) expected.negative += integer else expected.positive += integer;
        };
        const NoWork = struct {
            fn charge(_: *anyopaque, _: usize) !void {}
        };
        var placeholder: u8 = 0;
        if (try unionMeasureWork(std.testing.allocator, &ranges, &placeholder, NoWork.charge)) |actual| {
            try std.testing.expectEqualDeep(expected, actual);
            proved += 1;
        }
    }
    try std.testing.expect(proved > 1000);
}
