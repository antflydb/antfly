// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact, mergeable NUMERIC reduction. Signed base-10000 buckets deliberately
//! defer carries until finalization. Each bucket is bounded by 9999 * count;
//! an i128 safely covers every legal i64 row count, independent of input order.
//! Updates allocate only when the exponent span grows. Checkpoints retain the
//! widened state, so an overflowing partial SUM can still cancel or form AVG.
const std = @import("std");
const numeric = @import("numeric_value.zig");
const A = std.mem.Allocator;
const base: i128 = 10000;
const header = 46;
const guard_groups = 5; // 10000^5 exceeds the largest legal row count.

pub const State = struct {
    buckets: std.ArrayList(i128) = .empty,
    weight: i32 = 0,
    count: u64 = 0,
    nan_count: u64 = 0,
    positive_infinity: u64 = 0,
    negative_infinity: u64 = 0,
    scale: u16 = 0,

    pub fn deinit(self: *State, a: A) void {
        self.buckets.deinit(a);
        self.* = undefined;
    }

    fn admitCount(left: u64, right: u64) !u64 {
        const combined = std.math.add(u64, left, right) catch return error.SqlNumericOutOfRange;
        if (combined > std.math.maxInt(i64)) return error.SqlNumericOutOfRange;
        return combined;
    }

    /// All fallible work precedes logical mutation. Capacity growth may be
    /// retained after failure; count, scale and bucket values never change.
    pub fn growthBytes(self: State, weight: i32, length: usize) usize {
        if (length == 0) return 0;
        const high: i64 = if (self.buckets.items.len == 0) weight else @max(weight, self.weight);
        const low = @min(@as(i64, weight) - @as(i64, @intCast(length)) + 1, if (self.buckets.items.len == 0) weight else @as(i64, self.weight) - @as(i64, @intCast(self.buckets.items.len)) + 1);
        const size: usize = @intCast(high - low + 1);
        if (size <= self.buckets.capacity) return 0;
        // Include the whole replacement allocation: an allocator may have to
        // keep the old buffer alive until its contents have been copied.
        return std.ArrayList(i128).growCapacity(size) *| @sizeOf(i128);
    }

    fn expand(self: *State, ctx: *numeric.Context, weight: i32, length: usize) !usize {
        if (length == 0) return 0;
        const high = if (self.buckets.items.len == 0) weight else @max(weight, self.weight);
        const low = @min(weight - @as(i32, @intCast(length)) + 1, if (self.buckets.items.len == 0) weight else self.weight - @as(i32, @intCast(self.buckets.items.len)) + 1);
        const size: usize = @intCast(high - low + 1);
        if (size + guard_groups > ctx.max_groups or size > ctx.max_output_bytes / @sizeOf(i128)) return ctx.limit();
        const old = self.buckets.items.len;
        const shift: usize = if (old == 0) 0 else @intCast(high - self.weight);
        try ctx.charge(length + if (size == old) @as(usize, 0) else size + old);
        try self.buckets.ensureTotalCapacity(ctx.alloc, size);
        self.buckets.items.len = size;
        if (shift != 0) std.mem.copyBackwards(i128, self.buckets.items[shift..][0..old], self.buckets.items[0..old]);
        @memset(self.buckets.items[0..shift], 0);
        @memset(self.buckets.items[shift + old ..], 0);
        self.weight = high;
        return @intCast(high - weight);
    }

    pub fn add(self: *State, ctx: *numeric.Context, value: numeric.Value) !void {
        try numeric.validateCanonical(ctx, value);
        const count = try admitCount(self.count, 1);
        const offset = if (value.kind == .finite) try self.expand(ctx, value.weight, value.digits.len) else 0;
        switch (value.kind) {
            .finite => {
                for (value.digits, self.buckets.items[offset..][0..value.digits.len]) |digit, *bucket| bucket.* += if (value.negative) -@as(i128, digit) else digit;
                self.scale = @max(self.scale, value.scale);
            },
            .nan => self.nan_count += 1,
            .positive_infinity => self.positive_infinity += 1,
            .negative_infinity => self.negative_infinity += 1,
        }
        self.count = count;
    }

    pub fn merge(self: *State, ctx: *numeric.Context, source: State) !void {
        try source.validate(ctx);
        const count = try admitCount(self.count, source.count);
        const offset = try self.expand(ctx, source.weight, source.buckets.items.len);
        for (source.buckets.items, self.buckets.items[offset..][0..source.buckets.items.len]) |bucket, *target| target.* += bucket;
        self.count = count;
        self.nan_count += source.nan_count;
        self.positive_infinity += source.positive_infinity;
        self.negative_infinity += source.negative_infinity;
        self.scale = @max(self.scale, source.scale);
    }

    pub fn clone(self: State, ctx: *numeric.Context) !State {
        try self.validate(ctx);
        if (self.buckets.items.len > ctx.max_output_bytes / @sizeOf(i128)) return ctx.limit();
        var result = self;
        result.buckets = .empty;
        try result.buckets.appendSlice(ctx.alloc, self.buckets.items);
        return result;
    }

    pub fn validate(self: State, ctx: *numeric.Context) !void {
        try ctx.charge(1);
        if (self.count > std.math.maxInt(i64) or self.scale > numeric.maximum_scale) return error.InvalidNumericAggregateState;
        const special = std.math.add(u64, self.nan_count, self.positive_infinity) catch return error.InvalidNumericAggregateState;
        const specials = std.math.add(u64, special, self.negative_infinity) catch return error.InvalidNumericAggregateState;
        if (specials > self.count) return error.InvalidNumericAggregateState;
        const finite = self.count - specials;
        if (finite == 0 and self.scale != 0) return error.InvalidNumericAggregateState;
        if (self.buckets.items.len + guard_groups > ctx.max_groups) return ctx.limit();
        const cut = @divFloor(-@as(i32, self.scale), 4);
        if (self.buckets.items.len != 0 and (self.weight > numeric.maximum_weight or self.weight < cut or self.weight - @as(i32, @intCast(self.buckets.items.len)) + 1 < cut)) return error.InvalidNumericAggregateState;
        const bound: i128 = @as(i128, finite) * (base - 1);
        for (self.buckets.items) |bucket| {
            try ctx.charge(1);
            if (bucket < -bound or bucket > bound) return error.InvalidNumericAggregateState;
        }
        if (self.buckets.items.len != 0 and self.weight - @as(i32, @intCast(self.buckets.items.len)) + 1 == cut) {
            const factors = [_]i128{ 1, 10, 100, 1000 };
            if (@mod(self.buckets.items[self.buckets.items.len - 1], factors[@intCast(@mod(-@as(i32, self.scale), 4))]) != 0) return error.InvalidNumericAggregateState;
        }
    }

    /// Produce an owned result only after complete reduction. AVG divides the
    /// widened sum before enforcing public NUMERIC range, unlike SUM.
    pub fn total(self: State, ctx: *numeric.Context, average: bool) !numeric.Owned {
        try self.validate(ctx);
        if (self.count == 0) return error.InvalidNumericAggregateState;
        const kind: numeric.Kind = if (self.nan_count != 0 or (self.positive_infinity != 0 and self.negative_infinity != 0)) .nan else if (self.positive_infinity != 0) .positive_infinity else if (self.negative_infinity != 0) .negative_infinity else .finite;
        if (kind != .finite) return .{ .alloc = ctx.alloc, .value = .{ .kind = kind } };
        const size = self.buckets.items.len + guard_groups;
        if (size > ctx.max_groups or size > ctx.max_output_bytes / 2) return ctx.limit();
        try ctx.charge(size);
        const digits = try ctx.alloc.alloc(u16, size);
        errdefer ctx.alloc.free(digits);
        var carry: i128 = 0;
        var index = size;
        while (index != 0) {
            index -= 1;
            try ctx.charge(1);
            const raw = carry + if (index >= guard_groups) self.buckets.items[index - guard_groups] else @as(i128, 0);
            digits[index] = @bitCast(@as(i16, @intCast(@rem(raw, base))));
            carry = @divTrunc(raw, base);
        }
        std.debug.assert(carry == 0);
        var first: usize = 0;
        while (first < size and digits[first] == 0) : (first += 1) {}
        const negative = first < size and @as(i16, @bitCast(digits[first])) < 0;
        index = size;
        while (index != 0) {
            index -= 1;
            try ctx.charge(1);
            const digit: i16 = @bitCast(digits[index]);
            const raw = carry + if (negative) -@as(i128, digit) else digit;
            digits[index] = @intCast(@mod(raw, base));
            carry = @divFloor(raw, base);
        }
        std.debug.assert(carry == 0);
        first = 0;
        var last = size;
        while (first < last and digits[first] == 0) : (first += 1) {}
        while (last > first and digits[last - 1] == 0) : (last -= 1) {}
        const value: numeric.Value = .{
            .digits = digits[first..last],
            .scale = self.scale,
            .weight = if (first == last) 0 else self.weight + guard_groups - @as(i32, @intCast(first)),
            .negative = negative and first != last,
        };
        if (!average) {
            if (value.weight > numeric.maximum_weight) return error.SqlNumericOutOfRange;
            return .{ .alloc = ctx.alloc, .allocation = digits, .value = value };
        }
        // The division kernel accepts private widened coefficients; only its
        // owned quotient is subject to public NUMERIC range validation.
        var denominator: [guard_groups]u16 = undefined;
        var at: usize = denominator.len;
        var count = self.count;
        while (count != 0) : (count /= 10000) {
            at -= 1;
            denominator[at] = @intCast(count % 10000);
        }
        const result = numeric.divide(ctx, value, .{ .digits = denominator[at..], .weight = @intCast(denominator.len - at - 1) }) catch |err| return switch (err) {
            error.InvalidSqlNumber => error.SqlNumericOutOfRange,
            else => err,
        };
        ctx.alloc.free(digits);
        return result;
    }

    fn active(self: State) struct { first: usize, last: usize } {
        var first: usize = 0;
        var last = self.buckets.items.len;
        while (first < last and self.buckets.items[first] == 0) : (first += 1) {}
        while (last > first and self.buckets.items[last - 1] == 0) : (last -= 1) {}
        return .{ .first = first, .last = last };
    }

    pub fn equivalent(self: State, ctx: *numeric.Context, other: State) !bool {
        try self.validate(ctx);
        try other.validate(ctx);
        if (self.count != other.count or self.nan_count != other.nan_count or self.positive_infinity != other.positive_infinity or self.negative_infinity != other.negative_infinity or self.scale != other.scale) return false;
        const left = self.active();
        const right = other.active();
        if (left.last - left.first != right.last - right.first) return false;
        if (left.first == left.last) return true;
        return self.weight - @as(i32, @intCast(left.first)) == other.weight - @as(i32, @intCast(right.first)) and std.mem.eql(i128, self.buckets.items[left.first..left.last], other.buckets.items[right.first..right.last]);
    }

    pub fn encodedSize(self: State, ctx: *numeric.Context) !usize {
        try self.validate(ctx);
        const span = self.active();
        const size = header + (span.last - span.first) * @sizeOf(i128);
        if (size > ctx.max_output_bytes) return ctx.limit();
        return size;
    }

    pub fn encode(self: State, ctx: *numeric.Context, writer: *std.Io.Writer) !void {
        _ = try self.encodedSize(ctx);
        const span = self.active();
        try writer.writeAll("NAG\x01");
        inline for (.{ self.count, self.nan_count, self.positive_infinity, self.negative_infinity }) |count| try writeWord(u64, writer, count);
        try writeWord(u16, writer, self.scale);
        try writeWord(i32, writer, if (span.first == span.last) 0 else self.weight - @as(i32, @intCast(span.first)));
        try writeWord(u32, writer, @intCast(span.last - span.first));
        for (self.buckets.items[span.first..span.last]) |bucket| {
            try ctx.charge(1);
            try writeWord(i128, writer, bucket);
        }
    }

    pub fn encodeAlloc(self: State, ctx: *numeric.Context) ![]u8 {
        const size = try self.encodedSize(ctx);
        const bytes = try ctx.alloc.alloc(u8, size);
        errdefer ctx.alloc.free(bytes);
        var writer: std.Io.Writer = .fixed(bytes);
        try self.encode(ctx, &writer);
        return bytes;
    }
};

/// Flat group-vector lane. The optional final result has a stable address and
/// is invalidated only after a successful update/merge. It is not partial state.
pub const Reducer = struct {
    alloc: A,
    state: State = .{},
    result: ?*numeric.Owned = null,
    result_average: bool = false,
    remaining: u64 = 8 * 1024 * 1024,

    fn invalidate(self: *Reducer) void {
        if (self.result) |result| {
            result.deinit();
            self.alloc.destroy(result);
            self.result = null;
        }
    }
    pub fn deinit(self: *Reducer) void {
        self.invalidate();
        self.state.deinit(self.alloc);
        self.* = undefined;
    }
    pub fn add(self: *Reducer, value: numeric.Value) !void {
        var ctx: numeric.Context = .{ .alloc = self.alloc, .remaining = self.remaining };
        defer self.remaining = ctx.remaining;
        try self.state.add(&ctx, value);
        self.invalidate();
    }
    pub fn merge(self: *Reducer, source: State) !void {
        var ctx: numeric.Context = .{ .alloc = self.alloc, .remaining = self.remaining };
        defer self.remaining = ctx.remaining;
        try self.state.merge(&ctx, source);
        self.invalidate();
    }
    pub fn clone(self: Reducer, a: A) !Reducer {
        var ctx: numeric.Context = .{ .alloc = a };
        return .{ .alloc = a, .state = try self.state.clone(&ctx), .remaining = self.remaining };
    }
    pub fn finish(self: *Reducer, average: bool) !?*const numeric.Value {
        if (self.state.count == 0) return null;
        if (self.result) |result| {
            if (self.result_average != average) return error.InvalidNumericAggregateState;
            return &result.value;
        }
        var ctx: numeric.Context = .{ .alloc = self.alloc, .remaining = self.remaining };
        defer self.remaining = ctx.remaining;
        var result = try self.state.total(&ctx, average);
        errdefer result.deinit();
        const owner = try self.alloc.create(numeric.Owned);
        owner.* = result;
        self.result = owner;
        self.result_average = average;
        return &owner.value;
    }
};

fn writeWord(comptime T: type, writer: *std.Io.Writer, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try writer.writeAll(&bytes);
}

fn word(comptime T: type, bytes: []const u8, offset: usize) T {
    return std.mem.readInt(T, bytes[offset..][0..@sizeOf(T)], .little);
}

/// Validate the complete bounded payload before allocating. No row values or
/// input bytes remain borrowed, and no public NUMERIC range truncates a partial.
pub fn decode(ctx: *numeric.Context, bytes: []const u8) !State {
    try ctx.charge(1);
    if (bytes.len > ctx.max_input_bytes) return ctx.limit();
    if (bytes.len < header or !std.mem.eql(u8, bytes[0..4], "NAG\x01")) return error.InvalidNumericAggregateState;
    const size = word(u32, bytes, 42);
    if (size > (bytes.len - header) / 16 or header + @as(usize, size) * 16 != bytes.len) return error.InvalidNumericAggregateState;
    if (@as(usize, size) + guard_groups > ctx.max_groups or size > ctx.max_output_bytes / 16) return ctx.limit();
    var state: State = .{
        .count = word(u64, bytes, 4),
        .nan_count = word(u64, bytes, 12),
        .positive_infinity = word(u64, bytes, 20),
        .negative_infinity = word(u64, bytes, 28),
        .scale = word(u16, bytes, 36),
        .weight = word(i32, bytes, 38),
    };
    // A stack-sized view is not enough for large coefficients. First validate
    // counts and the exponent domain, then inspect limbs directly in the input.
    try state.validate(ctx);
    const specials = state.nan_count + state.positive_infinity + state.negative_infinity;
    const bound: i128 = @as(i128, state.count - specials) * (base - 1);
    const cut = @divFloor(-@as(i32, state.scale), 4);
    if ((size == 0 and state.weight != 0) or (size != 0 and (state.weight > numeric.maximum_weight or state.weight < cut or @as(i64, state.weight) - size + 1 < cut))) return error.InvalidNumericAggregateState;
    for (0..size) |i| {
        try ctx.charge(1);
        const bucket = word(i128, bytes, header + i * 16);
        if (bucket < -bound or bucket > bound or ((i == 0 or i + 1 == size) and bucket == 0)) return error.InvalidNumericAggregateState;
    }
    if (size != 0 and @as(i64, state.weight) - size + 1 == cut) {
        const factors = [_]i128{ 1, 10, 100, 1000 };
        if (@mod(word(i128, bytes, bytes.len - 16), factors[@intCast(@mod(-@as(i32, state.scale), 4))]) != 0) return error.InvalidNumericAggregateState;
    }
    errdefer state.deinit(ctx.alloc);
    try state.buckets.resize(ctx.alloc, size);
    for (state.buckets.items, 0..) |*bucket, i| bucket.* = word(i128, bytes, header + i * 16);
    return state;
}

fn scenario(a: A, corruptions: bool) !void {
    var ctx: numeric.Context = .{ .alloc = a };
    const cases = [_]struct { inputs: []const []const u8, sum: []const u8, average: []const u8 }{
        .{ .inputs = &.{ "1.20", "2.300" }, .sum = "3.500", .average = "1.7500000000000000" },
        .{ .inputs = &.{ "0.01", "0.00" }, .sum = "0.01", .average = "0.00500000000000000000" },
        .{ .inputs = &.{ "10000", "-9999", "-0.50" }, .sum = "0.50", .average = "0.16666666666666666667" },
        .{ .inputs = &.{ "-10000", "9999", "0.50" }, .sum = "-0.50", .average = "-0.16666666666666666667" },
        .{ .inputs = &.{ "Infinity", "1.00" }, .sum = "Infinity", .average = "Infinity" },
        .{ .inputs = &.{ "Infinity", "-Infinity" }, .sum = "NaN", .average = "NaN" },
        .{ .inputs = &.{ "NaN", "1.00" }, .sum = "NaN", .average = "NaN" },
    };
    for (cases) |case| {
        var state: State = .{};
        defer state.deinit(a);
        var left: State = .{};
        defer left.deinit(a);
        var right: State = .{};
        defer right.deinit(a);
        for (case.inputs, 0..) |text, i| {
            var value = try numeric.parse(&ctx, text);
            defer value.deinit();
            try state.add(&ctx, value.value);
            try (if (i % 2 == 0) &left else &right).add(&ctx, value.value);
        }
        var copy = try left.clone(&ctx);
        defer copy.deinit(a);
        try copy.merge(&ctx, right);
        for ([_]bool{ false, true }) |average| {
            var result = try state.total(&ctx, average);
            defer result.deinit();
            var merged = try copy.total(&ctx, average);
            defer merged.deinit();
            const text = try numeric.format(&ctx, result.value);
            defer a.free(text);
            try std.testing.expectEqualStrings(if (average) case.average else case.sum, text);
            try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, result.value, merged.value));
            try std.testing.expectEqual(result.value.scale, merged.value.scale);
        }
        const encoded = try state.encodeAlloc(&ctx);
        defer a.free(encoded);
        var restored = try decode(&ctx, encoded);
        defer restored.deinit(a);
        var result = try restored.total(&ctx, false);
        defer result.deinit();
        const text = try numeric.format(&ctx, result.value);
        defer a.free(text);
        try std.testing.expectEqualStrings(case.sum, text);
        if (corruptions) for (0..encoded.len) |size| try std.testing.expectError(error.InvalidNumericAggregateState, decode(&ctx, encoded[0..size]));
    }
}

test "SQL NUMERIC accumulator preserves PostgreSQL SUM AVG scale specials and widened partials" {
    try scenario(std.testing.allocator, true);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, scenario, .{false});
    const a = std.testing.allocator;
    var ctx: numeric.Context = .{ .alloc = a };
    var huge = try numeric.parse(&ctx, "9e131071");
    defer huge.deinit();
    var state: State = .{};
    defer state.deinit(a);
    try state.add(&ctx, huge.value);
    try state.add(&ctx, huge.value);
    try std.testing.expectError(error.SqlNumericOutOfRange, state.total(&ctx, false));
    var average = try state.total(&ctx, true);
    defer average.deinit();
    try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, huge.value, average.value));
    var bytes: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&bytes);
    try state.encode(&ctx, &writer);
    var restored = try decode(&ctx, writer.buffered());
    defer restored.deinit(a);
    var negative = huge.value;
    negative.negative = true;
    try restored.add(&ctx, negative);
    var cancelled = try restored.total(&ctx, false);
    defer cancelled.deinit();
    try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, huge.value, cancelled.value));
}

test "SQL NUMERIC accumulator hot updates reuse flat buckets versus immutable scalar sums" {
    const a = std.testing.allocator;
    var ctx: numeric.Context = .{ .alloc = a };
    var input = try numeric.parse(&ctx, "1.2300");
    defer input.deinit();
    const rows = 10000;
    var state: State = .{};
    defer state.deinit(a);
    try state.add(&ctx, input.value);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    ctx.alloc = failing.allocator();
    const time = @import("antfly_platform").time;
    const flat_started = time.monotonicNs();
    for (1..rows) |_| try state.add(&ctx, input.value);
    const flat_ns = time.monotonicNs() -| flat_started;
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expect(!failing.has_induced_failure);
    ctx.alloc = a;
    var total = try state.total(&ctx, false);
    defer total.deinit();
    var observed = std.testing.FailingAllocator.init(a, .{});
    var scalar_ctx: numeric.Context = .{ .alloc = observed.allocator() };
    var sum = try numeric.parse(&scalar_ctx, "0.0000");
    defer sum.deinit();
    const scalar_started = time.monotonicNs();
    for (0..rows) |_| {
        const next = try numeric.add(&scalar_ctx, sum.value, input.value);
        sum.deinit();
        sum = next;
    }
    const scalar_ns = time.monotonicNs() -| scalar_started;
    try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, total.value, sum.value));
    try std.testing.expectEqual(total.value.scale, sum.value.scale);
    std.debug.print("NUMERIC SUM: rows={d} flat_ns={d} scalar_ns={d} hot_allocations=0 scalar_allocations={d} state_bytes={d}\n", .{ rows, flat_ns, scalar_ns, observed.alloc_index, state.buckets.capacity * @sizeOf(i128) });
}

test "SQL NUMERIC accumulator cancellation and quota failures preserve logical state" {
    const a = std.testing.allocator;
    var ctx: numeric.Context = .{ .alloc = a };
    var input = try numeric.parse(&ctx, "12345.6700");
    defer input.deinit();
    var state: State = .{};
    defer state.deinit(a);
    try state.add(&ctx, input.value);
    var before = try state.clone(&ctx);
    defer before.deinit(a);
    const Cancel = struct {
        fn poll(_: ?*anyopaque) anyerror!void {
            return error.Canceled;
        }
    };
    var cancelled: numeric.Context = .{ .alloc = a, .checkpoint = Cancel.poll };
    try std.testing.expectError(error.Canceled, state.add(&cancelled, input.value));
    try std.testing.expectError(error.Canceled, state.merge(&cancelled, before));
    try std.testing.expectError(error.Canceled, state.total(&cancelled, false));
    try std.testing.expect(try state.equivalent(&ctx, before));
    var limited: numeric.Context = .{ .alloc = a, .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, state.add(&limited, input.value));
    try std.testing.expect(try state.equivalent(&ctx, before));
    const encoded = try state.encodeAlloc(&ctx);
    defer a.free(encoded);
    try std.testing.expectError(error.Canceled, decode(&cancelled, encoded));
    try std.testing.expectError(error.Canceled, state.encodeAlloc(&cancelled));
}

test "SQL NUMERIC accumulator bounds counts work bytes and validates before allocating" {
    const a = std.testing.allocator;
    var state: State = .{};
    defer state.deinit(a);
    var limited: numeric.Context = .{ .alloc = a, .max_output_bytes = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, state.add(&limited, .{ .digits = &.{1} }));
    try std.testing.expectEqual(@as(u64, 0), state.count);
    var ctx: numeric.Context = .{ .alloc = a };
    state.count = std.math.maxInt(i64);
    try std.testing.expectError(error.SqlNumericOutOfRange, state.add(&ctx, .{}));
    state.count = 0;
    try state.add(&ctx, .{ .digits = &.{1} });
    var bytes: [256]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&bytes);
    try state.encode(&ctx, &writer);
    @memset(bytes[header..][0..16], 255);
    bytes[header] = 0;
    bytes[header + 15] = 127;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var verification: numeric.Context = .{ .alloc = failing.allocator() };
    try std.testing.expectError(error.InvalidNumericAggregateState, decode(&verification, writer.buffered()));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}
