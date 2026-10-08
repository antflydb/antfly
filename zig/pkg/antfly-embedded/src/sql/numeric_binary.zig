// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL NUMERIC binary boundary, independent of the public transport.
//! Decode validates framing and all groups before publishing canonical owned
//! limbs. Encode streams directly without formatting decimal text.
const std = @import("std");
const numeric = @import("numeric_value.zig");
const Context = numeric.Context;
const Value = numeric.Value;
const A = std.mem.Allocator;
pub const Options = struct { modifier: ?numeric.TypeModifier = null };

fn sign(value: Value) u16 {
    return switch (value.kind) {
        .finite => if (value.negative) 0x4000 else 0,
        .nan => 0xc000,
        .positive_infinity => 0xd000,
        .negative_infinity => 0xf000,
    };
}

/// Validate the complete logical representation before exposing output bytes.
pub fn encodedSize(ctx: *Context, value: Value) !usize {
    try numeric.validateCanonical(ctx, value);
    const bytes = 8 + value.digits.len * 2;
    if (bytes > ctx.max_output_bytes) return ctx.limit();
    return bytes;
}

fn writeValidated(ctx: *Context, value: Value, writer: *std.Io.Writer) !void {
    try writer.writeInt(u16, @intCast(value.digits.len), .big);
    try writer.writeInt(i16, @intCast(value.weight), .big);
    try writer.writeInt(u16, sign(value), .big);
    // PostgreSQL's sender exposes 32 in the ignored infinity scale field.
    // Keep that wire convention out of canonical logical value identity.
    const wire_scale: u16 = if (value.kind == .positive_infinity or value.kind == .negative_infinity) 32 else value.scale;
    try writer.writeInt(u16, wire_scale, .big);
    for (value.digits) |digit| {
        try ctx.charge(1);
        try writer.writeInt(u16, digit, .big);
    }
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

/// Storage/restore admission is stricter than PostgreSQL parameter input.
/// Verify bytes directly against an owned logical value, without allocating a
/// second encoding or repairing noncanonical physical bytes.
pub fn verifyCanonical(ctx: *Context, bytes: []const u8, value: Value) !void {
    if (bytes.len != try encodedSize(ctx, value)) return error.InvalidSqlBinaryRepresentation;
    const scale: u16 = if (value.kind == .positive_infinity or value.kind == .negative_infinity) 32 else value.scale;
    if (std.mem.readInt(u16, bytes[0..2], .big) != value.digits.len or
        std.mem.readInt(i16, bytes[2..4], .big) != value.weight or
        std.mem.readInt(u16, bytes[4..6], .big) != sign(value) or
        std.mem.readInt(u16, bytes[6..8], .big) != scale) return error.InvalidSqlBinaryRepresentation;
    for (value.digits, 0..) |digit, i| {
        try ctx.charge(1);
        if (std.mem.readInt(u16, bytes[8 + i * 2 ..][0..2], .big) != digit) return error.InvalidSqlBinaryRepresentation;
    }
}

const Groups = struct {
    bytes: []const u8,
    pub fn len(self: Groups) usize {
        return self.bytes.len / 2;
    }
    pub fn at(self: Groups, i: usize) u16 {
        return std.mem.readInt(u16, self.bytes[i * 2 ..][0..2], .big);
    }
};

pub fn decode(ctx: *Context, bytes: []const u8, options: Options) !numeric.Owned {
    try ctx.charge(1);
    if (bytes.len > ctx.max_input_bytes) return ctx.limit();
    if (bytes.len < 8) return error.SqlProtocolViolation;
    const count = std.mem.readInt(u16, bytes[0..2], .big);
    const expected = 8 + @as(usize, count) * 2;
    if (bytes.len < expected) return error.SqlProtocolViolation;
    if (bytes.len != expected) return error.InvalidSqlBinaryRepresentation;
    const weight = std.mem.readInt(i16, bytes[2..4], .big);
    const sign_code = std.mem.readInt(u16, bytes[4..6], .big);
    const scale = std.mem.readInt(u16, bytes[6..8], .big);
    const kind: numeric.Kind = switch (sign_code) {
        0, 0x4000 => .finite,
        0xc000 => .nan,
        0xd000 => .positive_infinity,
        0xf000 => .negative_infinity,
        else => return error.InvalidSqlBinaryRepresentation,
    };
    if (scale > numeric.maximum_scale) return error.InvalidSqlBinaryRepresentation;
    const groups: Groups = .{ .bytes = bytes[8..] };
    var result: numeric.Owned = if (kind == .finite)
        numeric.fromGroups(ctx, groups, weight, scale, sign_code == 0x4000) catch |err| return switch (err) {
            error.InvalidNumericRepresentation => error.InvalidSqlBinaryRepresentation,
            else => err,
        }
    else special: {
        // PostgreSQL validates even the ignored digit payload of specials.
        for (0..groups.len()) |i| {
            try ctx.charge(1);
            if (groups.at(i) >= 10000) return error.InvalidSqlBinaryRepresentation;
        }
        break :special .{ .alloc = ctx.alloc, .value = .{ .kind = kind } };
    };
    if (options.modifier) |modifier| {
        defer result.deinit();
        return numeric.applyTypeModifier(ctx, result.value, modifier);
    }
    return result;
}

const Expected = struct { binary: []const u8, text: ?[]const u8, text_length: usize, sha256: []const u8 };

test "SQL exact NUMERIC storage rejects normalized receiver bytes without a second encoding" {
    const a = std.testing.allocator;
    var context: Context = .{ .alloc = a };
    const padded = [_]u8{ 0, 3, 0, 1, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0 };
    var number = try decode(&context, &padded, .{});
    defer number.deinit();
    try std.testing.expectError(error.InvalidSqlBinaryRepresentation, verifyCanonical(&context, &padded, number.value));
    const canonical = try encodeAlloc(&context, number.value);
    defer a.free(canonical);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var verification: Context = .{ .alloc = none.allocator() };
    try verifyCanonical(&verification, canonical, number.value);
    var copy: [10]u8 = undefined;
    @memcpy(&copy, canonical);
    copy[9] ^= 1;
    try std.testing.expectError(error.InvalidSqlBinaryRepresentation, verifyCanonical(&verification, &copy, number.value));
}

test "SQL exact NUMERIC binary validates before output and preserves admission and writer errors" {
    const a = std.testing.allocator;
    const invalid = [_]Value{
        .{ .negative = true },         .{ .weight = 1 },                      .{ .scale = 16384 },
        .{ .kind = .nan, .scale = 1 }, .{ .digits = &.{0} },                  .{ .digits = &.{ 1, 0 } },
        .{ .digits = &.{10000} },      .{ .digits = &.{1}, .weight = 32768 }, .{ .digits = &.{1234}, .weight = -1, .scale = 2 },
    };
    var buffer: [64]u8 = undefined;
    for (invalid) |value| {
        var ctx: Context = .{ .alloc = a };
        var writer: std.Io.Writer = .fixed(&buffer);
        try std.testing.expectError(error.InvalidNumericRepresentation, encode(&ctx, value, &writer));
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    }
    var ctx: Context = .{ .alloc = a, .max_output_bytes = 7 };
    var writer: std.Io.Writer = .fixed(&buffer);
    try std.testing.expectError(error.SqlProgramLimitExceeded, encode(&ctx, .{}, &writer));
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(&ctx, "", .{}));
    ctx = .{ .alloc = a, .max_input_bytes = 7 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(&ctx, &@as([8]u8, @splat(0)), .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, encodedSize(&ctx, .{}));
    ctx = .{ .alloc = a };
    writer = .fixed(buffer[0..7]);
    try std.testing.expectError(error.WriteFailed, encode(&ctx, .{}, &writer));
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    ctx = .{ .alloc = failing.allocator() };
    const malformed = [_]u8{ 0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10 };
    try std.testing.expectError(error.InvalidSqlBinaryRepresentation, decode(&ctx, &malformed, .{}));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}

test "SQL exact NUMERIC binary cancellation is sticky at every observed checkpoint" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 8 + 192 * 2);
    defer a.free(bytes);
    std.mem.writeInt(u16, bytes[0..2], 192, .big);
    std.mem.writeInt(i16, bytes[2..4], 191, .big);
    std.mem.writeInt(u16, bytes[4..6], 0, .big);
    std.mem.writeInt(u16, bytes[6..8], 0, .big);
    for (0..192) |i| std.mem.writeInt(u16, bytes[8 + i * 2 ..][0..2], 9999, .big);
    const Poll = struct {
        calls: usize = 0,
        fail_at: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.fail_at != 0 and self.calls == self.fail_at) return error.Canceled;
        }
    };
    const Harness = struct {
        fn run(ctx: *Context, input: []const u8) !void {
            var value = try decode(ctx, input, .{ .modifier = .{ .precision = 1000 } });
            defer value.deinit();
            const encoded = try encodeAlloc(ctx, value.value);
            defer ctx.alloc.free(encoded);
            try std.testing.expectEqualSlices(u8, input, encoded);
            const text = try numeric.format(ctx, value.value);
            defer ctx.alloc.free(text);
        }
    };
    var poll: Poll = .{};
    var ctx: Context = .{ .alloc = a, .checkpoint = Poll.check, .ptr = &poll };
    try Harness.run(&ctx, bytes);
    const checkpoints = poll.calls;
    try std.testing.expect(checkpoints > 1);
    for (1..checkpoints + 1) |at| {
        poll = .{ .fail_at = at };
        ctx = .{ .alloc = a, .checkpoint = Poll.check, .ptr = &poll };
        try std.testing.expectError(error.Canceled, Harness.run(&ctx, bytes));
        try std.testing.expectError(error.Canceled, ctx.charge(0));
    }
    ctx = .{ .alloc = a };
    try Harness.run(&ctx, bytes);
}

test "SQL exact NUMERIC binary ownership unwinds every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            const bytes = [_]u8{ 0, 4, 0, 1, 0, 0, 0, 3, 0, 0, 4, 210, 22, 46, 0, 0 };
            var ctx: Context = .{ .alloc = a };
            var decoded = try decode(&ctx, &bytes, .{ .modifier = .{ .precision = 8, .scale = 2 } });
            defer decoded.deinit();
            const encoded = try encodeAlloc(&ctx, decoded.value);
            defer a.free(encoded);
            var roundtrip = try decode(&ctx, encoded, .{});
            defer roundtrip.deinit();
            const text = try numeric.format(&ctx, roundtrip.value);
            defer a.free(text);
            try std.testing.expectEqualStrings("1234.57", text);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL exact NUMERIC binary maximum group count retains only owned significant limbs" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 8 + 65535 * 2);
    defer a.free(bytes);
    @memset(bytes, 0);
    std.mem.writeInt(u16, bytes[0..2], 65535, .big);
    std.mem.writeInt(i16, bytes[2..4], 32767, .big);
    std.mem.writeInt(u16, bytes[8 + 32767 * 2 ..][0..2], 1, .big);
    var tracked = std.testing.FailingAllocator.init(a, .{});
    var ctx: Context = .{ .alloc = tracked.allocator(), .max_groups = 1 };
    const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    var decoded = try decode(&ctx, bytes, .{});
    defer decoded.deinit();
    const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
    try std.testing.expectEqual(@as(usize, 1), tracked.alloc_index);
    try std.testing.expectEqual(@as(usize, 1), decoded.allocation.len);
    @memset(bytes, 0);
    try std.testing.expectEqual(@as(u16, 1), decoded.value.digits[0]);
    try std.testing.expectEqual(@as(i32, 0), decoded.value.weight);
    std.mem.writeInt(u16, bytes[0..2], 65535, .big);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    ctx = .{ .alloc = failing.allocator() };
    var zero_value = try decode(&ctx, bytes, .{});
    defer zero_value.deinit();
    try std.testing.expect(zero_value.value.isZero());
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    std.debug.print("NUMERIC binary decode: input_groups=65535 retained_groups=1 allocations=1 elapsed_ns={d}\n", .{elapsed});
}
const ReceiveCase = struct {
    name: []const u8,
    binary: []const u8,
    modifier: ?numeric.TypeModifier = null,
    expected: ?Expected = null,
    @"error": ?[]const u8 = null,
};

fn unhex(a: A, text: []const u8) ![]u8 {
    const bytes = try a.alloc(u8, text.len / 2);
    errdefer a.free(bytes);
    _ = try std.fmt.hexToBytes(bytes, text);
    return bytes;
}

test "SQL exact NUMERIC binary codec matches real PostgreSQL sender and receiver" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        reference: []const u8,
        senders: []const struct { input: []const u8, binary: []const u8 },
        receivers: []const ReceiveCase,
    }, a, @embedFile("fixtures/sql_exact_numeric_binary_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqualStrings("PostgreSQL NUMERIC binary boundary", fixture.value.reference);
    try std.testing.expectEqual(@as(usize, 65), fixture.value.senders.len);
    try std.testing.expectEqual(@as(usize, 47), fixture.value.receivers.len);
    for (fixture.value.senders) |entry| {
        var ctx: Context = .{ .alloc = a };
        var value = try numeric.parse(&ctx, entry.input);
        defer value.deinit();
        const expected = try unhex(a, entry.binary);
        defer a.free(expected);
        const encoded = try encodeAlloc(&ctx, value.value);
        defer a.free(encoded);
        try std.testing.expectEqualSlices(u8, expected, encoded);
        var decoded = try decode(&ctx, encoded, .{});
        defer decoded.deinit();
        try std.testing.expectEqual(std.math.Order.eq, try numeric.order(&ctx, value.value, decoded.value));
        try std.testing.expectEqual(value.value.scale, decoded.value.scale);
        const output = try a.alloc(u8, expected.len);
        defer a.free(output);
        var writer: std.Io.Writer = .fixed(output);
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        ctx = .{ .alloc = failing.allocator() };
        try encode(&ctx, value.value, &writer);
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
        try std.testing.expectEqualSlices(u8, expected, writer.buffered());
    }
    for (fixture.value.receivers) |entry| {
        var ctx: Context = .{ .alloc = a };
        const bytes = try unhex(a, entry.binary);
        defer a.free(bytes);
        if (entry.@"error") |state| {
            const expected: anyerror = if (std.mem.eql(u8, state, "08P01")) error.SqlProtocolViolation else if (std.mem.eql(u8, state, "22P03")) error.InvalidSqlBinaryRepresentation else if (std.mem.eql(u8, state, "22003")) error.InvalidSqlNumber else return error.UnexpectedNumericBinaryOracleError;
            try std.testing.expectError(expected, decode(&ctx, bytes, .{ .modifier = entry.modifier }));
            continue;
        }
        var result = try decode(&ctx, bytes, .{ .modifier = entry.modifier });
        defer result.deinit();
        const encoded = try encodeAlloc(&ctx, result.value);
        defer a.free(encoded);
        const expected = try unhex(a, entry.expected.?.binary);
        defer a.free(expected);
        try std.testing.expectEqualSlices(u8, expected, encoded);
        const text = try numeric.format(&ctx, result.value);
        defer a.free(text);
        try std.testing.expectEqual(entry.expected.?.text_length, text.len);
        if (entry.expected.?.text) |display| try std.testing.expectEqualStrings(display, text);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(text, &digest, .{});
        try std.testing.expectEqualStrings(entry.expected.?.sha256, &std.fmt.bytesToHex(digest, .lower));
    }
}
