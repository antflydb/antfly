// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical, self-delimiting NUMERIC identity keys. Byte order is PostgreSQL
//! numeric order, including infinities and NaN; display scale is not identity.
//! This is an index-key payload, not a row encoding: decode recovers the
//! smallest exact display scale, not the original presentation metadata.
const std = @import("std");
const numeric = @import("numeric_value.zig");
const Context = numeric.Context;
const Value = numeric.Value;

const Rank = enum(u8) { negative_infinity, negative, zero, positive, positive_infinity, nan };
fn rank(value: Value) Rank {
    return switch (value.kind) {
        .negative_infinity => .negative_infinity,
        .positive_infinity => .positive_infinity,
        .nan => .nan,
        .finite => if (value.isZero()) .zero else if (value.negative) .negative else .positive,
    };
}

pub fn encodedSize(ctx: *Context, value: Value) !usize {
    try numeric.validateCanonical(ctx, value);
    const size: usize = if (value.kind == .finite and !value.isZero()) 5 + value.digits.len * 2 else 1;
    if (size > ctx.max_output_bytes) return ctx.limit();
    return size;
}

fn writeValidated(ctx: *Context, value: Value, writer: *std.Io.Writer) !void {
    try writer.writeByte(@backingInt(rank(value)));
    if (value.kind != .finite or value.isZero()) return;
    const mask: u16 = if (value.negative) 0xffff else 0;
    const weight: u16 = @as(u16, @bitCast(@as(i16, @intCast(value.weight)))) ^ 0x8000;
    try writer.writeInt(u16, weight ^ mask, .big);
    // Reserve zero as the terminator. Interior zero groups remain significant;
    // canonical trailing groups are nonzero, so the terminator sorts before
    // every strictly larger positive suffix. Complement reverses negative keys.
    for (value.digits) |digit| {
        try ctx.charge(1);
        try writer.writeInt(u16, (digit + 1) ^ mask, .big);
    }
    try writer.writeInt(u16, mask, .big);
}

pub fn encode(ctx: *Context, value: Value, writer: *std.Io.Writer) !void {
    _ = try encodedSize(ctx, value);
    try writeValidated(ctx, value, writer);
}

pub fn encodeAlloc(ctx: *Context, value: Value) ![]u8 {
    const size = try encodedSize(ctx, value);
    const bytes = try ctx.alloc.alloc(u8, size);
    errdefer ctx.alloc.free(bytes);
    var writer: std.Io.Writer = .fixed(bytes);
    try writeValidated(ctx, value, &writer);
    return bytes;
}

const Groups = struct {
    bytes: []const u8,
    mask: u16,
    pub fn len(self: Groups) usize {
        return self.bytes.len / 2;
    }
    pub fn at(self: Groups, i: usize) u16 {
        return (std.mem.readInt(u16, self.bytes[i * 2 ..][0..2], .big) ^ self.mask) - 1;
    }
};

pub const Decoded = struct { value: numeric.Owned, consumed: usize };

const Shape = struct {
    kind: Rank,
    consumed: usize,
    weight: i16 = 0,
    scale: u16 = 0,
    groups: Groups = .{ .bytes = &.{}, .mask = 0 },

    fn materialize(self: Shape, ctx: *Context) !numeric.Owned {
        if (self.kind == .positive or self.kind == .negative)
            return numeric.fromGroups(ctx, self.groups, self.weight, self.scale, self.kind == .negative);
        return .{ .alloc = ctx.alloc, .value = .{ .kind = switch (self.kind) {
            .negative_infinity => .negative_infinity,
            .positive_infinity => .positive_infinity,
            .nan => .nan,
            else => .finite,
        } } };
    }
};

/// Decode one component without copying or examining the following component.
/// The returned owned limbs do not borrow the key. Malformed/noncanonical keys
/// are rejected rather than normalized to another unique-index identity.
pub fn decodePrefix(ctx: *Context, bytes: []const u8) !Decoded {
    const shape = try parsePrefix(ctx, bytes);
    return .{ .value = try shape.materialize(ctx), .consumed = shape.consumed };
}

fn parsePrefix(ctx: *Context, bytes: []const u8) !Shape {
    try ctx.charge(1);
    if (ctx.max_input_bytes < 1) return ctx.limit();
    if (bytes.len == 0) return error.InvalidSqlNumericKey;
    const kind = std.enums.fromInt(Rank, bytes[0]) orelse return error.InvalidSqlNumericKey;
    switch (kind) {
        .negative_infinity, .zero, .positive_infinity, .nan => return .{
            .consumed = 1,
            .kind = kind,
        },
        else => {},
    }
    if (ctx.max_input_bytes < 3) return ctx.limit();
    if (bytes.len < 3) return error.InvalidSqlNumericKey;
    const negative = kind == .negative;
    const mask: u16 = if (negative) 0xffff else 0;
    const weight: i16 = @bitCast(std.mem.readInt(u16, bytes[1..3], .big) ^ mask ^ 0x8000);
    var end: usize = 3;
    while (true) : (end += 2) {
        try ctx.charge(1);
        if (end > ctx.max_input_bytes or 2 > ctx.max_input_bytes - end) return ctx.limit();
        if (end > bytes.len or 2 > bytes.len - end) return error.InvalidSqlNumericKey;
        const word = std.mem.readInt(u16, bytes[end..][0..2], .big) ^ mask;
        if (word == 0) break;
        if (word > 10000 or (end - 3) / 2 >= std.math.maxInt(u16)) return error.InvalidSqlNumericKey;
    }
    const groups: Groups = .{ .bytes = bytes[3..end], .mask = mask };
    if (groups.len() == 0 or groups.at(0) == 0 or groups.at(groups.len() - 1) == 0) return error.InvalidSqlNumericKey;
    const low = @as(i32, weight) - @as(i32, @intCast(groups.len())) + 1;
    var scale: i32 = @max(0, -low * 4);
    if (scale != 0) {
        var last = groups.at(groups.len() - 1);
        while (last % 10 == 0) : (last /= 10) scale -= 1;
    }
    if (scale > numeric.maximum_scale) return error.InvalidSqlNumericKey;
    return .{ .kind = kind, .weight = weight, .scale = @intCast(scale), .groups = groups, .consumed = end + 2 };
}

pub fn decode(ctx: *Context, bytes: []const u8) !numeric.Owned {
    if (bytes.len > ctx.max_input_bytes) return ctx.limit();
    const shape = try parsePrefix(ctx, bytes);
    if (shape.consumed != bytes.len) return error.InvalidSqlNumericKey;
    return shape.materialize(ctx);
}

test "SQL exact NUMERIC identity keys preserve all PostgreSQL sender values and order" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        senders: []const struct { input: []const u8 },
    }, a, @embedFile("fixtures/sql_exact_numeric_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var ctx: Context = .{ .alloc = arena.allocator(), .remaining = 128 * 1024 * 1024 };
    const values = try ctx.alloc.alloc(numeric.Owned, fixture.value.senders.len);
    const keys = try ctx.alloc.alloc([]const u8, values.len);
    for (fixture.value.senders, values, keys) |entry, *value, *key| {
        value.* = try numeric.parse(&ctx, entry.input);
        key.* = try encodeAlloc(&ctx, value.value);
        var decoded = try decode(&ctx, key.*);
        defer decoded.deinit();
        try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, value.value, decoded.value));
        const canonical = try encodeAlloc(&ctx, decoded.value);
        try std.testing.expectEqualSlices(u8, key.*, canonical);
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var streaming: Context = .{ .alloc = failing.allocator() };
        const output = try ctx.alloc.alloc(u8, key.len);
        var writer: std.Io.Writer = .fixed(output);
        try encode(&streaming, value.value, &writer);
        try std.testing.expectEqualSlices(u8, key.*, writer.buffered());
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    }
    for (values, keys) |left, left_key| for (values, keys) |right, right_key| {
        try std.testing.expectEqual(try numeric.order(&ctx, left.value, right.value), std.mem.order(u8, left_key, right_key));
    };
}

test "SQL exact NUMERIC identity keys ignore scale and delimit composite components" {
    const a = std.testing.allocator;
    var ctx: Context = .{ .alloc = a };
    for ([_][]const u8{ "1", "1.0", "1.000000", "10e-1", "0.1e1" }) |text| {
        var value = try numeric.parse(&ctx, text);
        defer value.deinit();
        const key = try encodeAlloc(&ctx, value.value);
        defer a.free(key);
        try std.testing.expectEqualSlices(u8, &.{ 3, 0x80, 0, 0, 2, 0, 0 }, key);
        const composite = try std.mem.concat(a, u8, &.{ key, &.{ 0xff, 0xff, 0xff } });
        defer a.free(composite);
        var decoded = try decodePrefix(&ctx, composite);
        defer decoded.value.deinit();
        try std.testing.expectEqual(key.len, decoded.consumed);
        try std.testing.expectEqual(@as(u16, 0), decoded.value.value.scale);
        try std.testing.expectError(error.InvalidSqlNumericKey, decode(&ctx, composite));
    }
}

test "SQL exact NUMERIC index keys match independently ranked PostgreSQL values" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        reference: []const u8,
        entries: []const struct { input: []const u8, rank: usize },
    }, a, @embedFile("fixtures/sql_exact_numeric_key_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqualStrings("PostgreSQL NUMERIC index dense ranks", fixture.value.reference);
    try std.testing.expectEqual(@as(usize, 235), fixture.value.entries.len);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var ctx: Context = .{ .alloc = arena.allocator(), .remaining = 128 * 1024 * 1024 };
    const keys = try ctx.alloc.alloc([]const u8, fixture.value.entries.len);
    for (fixture.value.entries, keys) |entry, *key| {
        var value = try numeric.parse(&ctx, entry.input);
        defer value.deinit();
        key.* = try encodeAlloc(&ctx, value.value);
        var restored = try decode(&ctx, key.*);
        defer restored.deinit();
        const canonical = try encodeAlloc(&ctx, restored.value);
        try std.testing.expectEqualSlices(u8, key.*, canonical);
    }
    for (fixture.value.entries, keys) |left, left_key| for (fixture.value.entries, keys) |right, right_key| {
        const expected = std.math.order(left.rank, right.rank);
        try std.testing.expectEqual(expected, std.mem.order(u8, left_key, right_key));
        if (expected != .eq) {
            // A following composite-key component cannot change numeric order.
            try std.testing.expect(!std.mem.startsWith(u8, left_key, right_key));
            try std.testing.expect(!std.mem.startsWith(u8, right_key, left_key));
        }
    };
}

test "SQL exact NUMERIC identity keys reject malformed and noncanonical bytes before allocation" {
    const a = std.testing.allocator;
    const malformed = [_][]const u8{
        &.{}, &.{6}, &.{3}, &.{ 3, 0x80 }, &.{ 3, 0x80, 0 },
        &.{ 3, 0x80, 0, 0, 0 }, // Finite zero must use the dedicated rank.
        &.{ 3, 0x80, 0, 0, 1, 0, 0 }, // Leading zero group.
        &.{ 3, 0x80, 0, 0, 2, 0, 1, 0, 0 }, // Trailing zero group.
        &.{ 3, 0x80, 0, 0x27, 0x11, 0, 0 }, // Digit 10000 is invalid.
        &.{ 3, 0, 0, 0, 2, 0, 0 }, // Fractional domain overflow.
        &.{ 3, 0x70, 0, 0, 2, 0, 0 }, // Scale 16384 is outside the domain.
        &.{ 1, 0x7f, 0xff, 0xff, 0xff }, // Noncanonical negative zero.
        &.{ 1, 0x7f, 0xff, 0xff, 0xfe, 0xff, 0xff }, // Leading zero group.
        &.{ 3, 0x80, 0, 0, 2, 0, 0, 0xff }, // Trailing bytes before limb allocation.
    };
    for (malformed) |key| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var ctx: Context = .{ .alloc = failing.allocator() };
        try std.testing.expectError(error.InvalidSqlNumericKey, decode(&ctx, key));
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    }
    const valid = [_]u8{ 3, 0x80, 0, 0, 2, 0, 0 };
    for (0..valid.len) |length| {
        var ctx: Context = .{ .alloc = a };
        try std.testing.expectError(error.InvalidSqlNumericKey, decode(&ctx, valid[0..length]));
    }
    // Mutation fuzzing: any accepted byte sequence must already be canonical.
    for (0..valid.len) |i| for (0..256) |byte| {
        var changed = valid;
        changed[i] = @intCast(byte);
        var ctx: Context = .{ .alloc = a };
        var decoded = decode(&ctx, &changed) catch |err| {
            try std.testing.expectEqual(error.InvalidSqlNumericKey, err);
            continue;
        };
        defer decoded.deinit();
        const encoded = try encodeAlloc(&ctx, decoded.value);
        defer a.free(encoded);
        try std.testing.expectEqualSlices(u8, &changed, encoded);
    };
}

fn ownership(a: std.mem.Allocator, checkpoint: ?*const fn (?*anyopaque) anyerror!void, ptr: ?*anyopaque) !void {
    const text: [2048]u8 = @splat('9');
    var ctx: Context = .{ .alloc = a, .checkpoint = checkpoint, .ptr = ptr };
    var value = try numeric.parse(&ctx, &text);
    defer value.deinit();
    const key = try encodeAlloc(&ctx, value.value);
    defer a.free(key);
    var decoded = try decode(&ctx, key);
    defer decoded.deinit();
    try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, value.value, decoded.value));
    const canonical = try encodeAlloc(&ctx, decoded.value);
    defer a.free(canonical);
    try std.testing.expectEqualSlices(u8, key, canonical);
    @memset(key, 0xa5);
    try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, value.value, decoded.value));
    const rendered = try numeric.format(&ctx, decoded.value);
    defer a.free(rendered);
}

test "SQL exact NUMERIC identity key ownership unwinds all allocation failures" {
    const Harness = struct {
        fn run(a: std.mem.Allocator) !void {
            try ownership(a, null, null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL exact NUMERIC identity keys preserve quotas cancellation and zero-allocation streaming" {
    const a = std.testing.allocator;
    const value: Value = .{ .digits = &.{1} };
    var buffer: [64]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buffer);
    var ctx: Context = .{ .alloc = a, .max_output_bytes = 6 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, encode(&ctx, value, &writer));
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(&ctx, &.{2}));
    ctx = .{ .alloc = a, .max_output_bytes = 1 };
    try encode(&ctx, .{ .kind = .nan }, &writer);
    try std.testing.expectEqualSlices(u8, &.{5}, writer.buffered());
    ctx = .{ .alloc = a, .max_input_bytes = 6 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(&ctx, &.{ 3, 0x80, 0, 0, 2, 0, 0 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, encodedSize(&ctx, value));
    ctx = .{ .alloc = a, .max_groups = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(&ctx, &.{ 3, 0x80, 0, 0, 2, 0, 0 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, encodedSize(&ctx, value));
    ctx = .{ .alloc = a };
    writer = .fixed(buffer[0..6]);
    try std.testing.expectError(error.WriteFailed, encode(&ctx, value, &writer));
    writer = .fixed(&buffer);
    ctx = .{ .alloc = a };
    try std.testing.expectError(error.InvalidNumericRepresentation, encode(&ctx, .{ .digits = &.{ 1, 0 } }, &writer));
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    const Poll = struct {
        count: usize = 0,
        fail_at: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.count += 1;
            if (self.count == self.fail_at) return error.Canceled;
        }
    };
    var polls: Poll = .{};
    try ownership(a, Poll.check, &polls);
    const count = polls.count;
    try std.testing.expect(count > 8);
    for (1..count + 1) |fail_at| {
        polls = .{ .fail_at = fail_at };
        try std.testing.expectError(error.Canceled, ownership(a, Poll.check, &polls));
    }
    try ownership(a, null, null);
}

test "SQL exact NUMERIC identity key benchmark bounds physical bytes and allocations" {
    const a = std.testing.allocator;
    const text: [2048]u8 = @splat('9');
    for ([_]usize{ 64, 256, 2048 }) |digits| {
        var parse_ctx: Context = .{ .alloc = a };
        var value = try numeric.parse(&parse_ctx, text[0..digits]);
        defer value.deinit();
        var counted = std.testing.FailingAllocator.init(a, .{});
        var ctx: Context = .{ .alloc = counted.allocator() };
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        const key = try encodeAlloc(&ctx, value.value);
        defer counted.allocator().free(key);
        const encode_allocations = counted.alloc_index;
        var restored = try decode(&ctx, key);
        defer restored.deinit();
        const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
        try std.testing.expectEqual(@as(usize, 1), encode_allocations);
        try std.testing.expectEqual(@as(usize, 2), counted.alloc_index);
        try std.testing.expectEqual(5 + digits / 2, key.len);
        try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, value.value, restored.value));
        std.debug.print("NUMERIC key: decimal_digits={} key_bytes={} encode_allocations=1 decode_allocations=1 elapsed_ns={}\n", .{ digits, key.len, elapsed });
    }
    for ([_][]const u8{ "1e131071", "-1e131071", "1e-16383", "-1e-16383" }) |input| {
        var ctx: Context = .{ .alloc = a, .remaining = 64, .max_groups = 1, .max_output_bytes = 8, .max_input_bytes = 16 };
        var value = try numeric.parse(&ctx, input);
        defer value.deinit();
        const key = try encodeAlloc(&ctx, value.value);
        defer a.free(key);
        try std.testing.expectEqual(@as(usize, 7), key.len);
        var restored = try decode(&ctx, key);
        defer restored.deinit();
        try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, value.value, restored.value));
    }
}
