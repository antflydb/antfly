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

//! Canonical, self-delimiting NUMERIC identity keys. Byte order is PostgreSQL
//! numeric order, including infinities and NaN; display scale is not identity.
//! This is an index-key payload, not a row encoding: decode recovers the
//! smallest exact display scale, not the original presentation metadata.
const std = @import("std");
const numeric = @import("numeric_value.zig");
const Context = numeric.Context;
const Value = numeric.Value;
pub const layout = @import("../common/sql_numeric_key_layout.zig");

fn source(value: Value) layout.Source {
    return .{ .kind = switch (value.kind) {
        .finite => .finite,
        .negative_infinity => .negative_infinity,
        .positive_infinity => .positive_infinity,
        .nan => .nan,
    }, .negative = value.negative, .weight = @intCast(value.weight), .groups = .{ .limbs = value.digits } };
}

pub fn encodedSize(ctx: *Context, value: Value) !usize {
    try numeric.validateCanonical(ctx, value);
    const size = source(value).encodedSize();
    if (size > ctx.max_output_bytes) return ctx.limit();
    return size;
}

fn writeValidated(ctx: *Context, value: Value, writer: *std.Io.Writer) !void {
    try source(value).write(writer, false, ctx);
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

/// Validate an untrusted stored row value once, then borrow its coefficients
/// directly for ordered-key output. The returned source pins no owner: callers
/// must retain immutable input bytes through writing. No decimal limb copy or
/// intermediate key allocation is needed for a native index tuple.
pub fn prepareStored(ctx: *Context, bytes: []const u8) !layout.Source {
    const view = try @import("../common/sql_numeric_layout.zig").View.openWithBudget(bytes, .{ .bytes = ctx.max_input_bytes, .groups = ctx.max_groups }, ctx);
    const prepared = layout.Source.fromCanonicalRow(view);
    if (prepared.encodedSize() > ctx.max_output_bytes) return ctx.limit();
    return prepared;
}

pub fn encodeStored(ctx: *Context, bytes: []const u8, writer: *std.Io.Writer, descending: bool) !void {
    const prepared = try prepareStored(ctx, bytes);
    try prepared.write(writer, descending, ctx);
}

pub const Decoded = struct { value: numeric.Owned, consumed: usize };

fn materialize(shape: layout.Shape, ctx: *Context) !numeric.Owned {
    if (shape.kind == .positive or shape.kind == .negative)
        return numeric.fromGroups(ctx, shape.groups, shape.weight, shape.scale, shape.kind == .negative);
    return .{ .alloc = ctx.alloc, .value = .{ .kind = switch (shape.kind) {
        .negative_infinity => .negative_infinity,
        .positive_infinity => .positive_infinity,
        .nan => .nan,
        else => .finite,
    } } };
}

/// Decode one component without copying or examining the following component.
/// The returned owned limbs do not borrow the key. Malformed/noncanonical keys
/// are rejected rather than normalized to another unique-index identity.
pub fn decodePrefix(ctx: *Context, bytes: []const u8) !Decoded {
    const shape = try parsePrefix(ctx, bytes);
    return .{ .value = try materialize(shape, ctx), .consumed = shape.consumed };
}

fn parsePrefix(ctx: *Context, bytes: []const u8) !layout.Shape {
    return layout.parsePrefix(bytes, false, .{ .bytes = ctx.max_input_bytes, .groups = ctx.max_groups }, ctx);
}

pub fn decode(ctx: *Context, bytes: []const u8) !numeric.Owned {
    if (bytes.len > ctx.max_input_bytes) return ctx.limit();
    const shape = try parsePrefix(ctx, bytes);
    if (shape.consumed != bytes.len) return error.InvalidSqlNumericKey;
    return materialize(shape, ctx);
}

test "SQL exact NUMERIC identity keys preserve all PostgreSQL sender values and order" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(struct {
        senders: []const struct { input: []const u8, binary: []const u8 },
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
        const wire = try ctx.alloc.alloc(u8, entry.binary.len / 2);
        _ = try std.fmt.hexToBytes(wire, entry.binary);
        const composite = try ctx.alloc.alloc(u8, key.len + 3);
        for ([_]bool{ false, true }) |desc| {
            streaming = .{ .alloc = failing.allocator() };
            writer = .fixed(composite[0..key.len]);
            try encodeStored(&streaming, wire, &writer, desc);
            for (writer.buffered(), key.*) |actual, expected| {
                try std.testing.expectEqual(expected ^ @as(u8, if (desc) 0xff else 0), actual);
            }
            @memset(composite[key.len..], 0xff);
            // The following tuple component is deliberately outside the
            // admitted byte budget. Only this component may be traversed.
            streaming.max_input_bytes = key.len;
            const shape = try layout.parsePrefix(composite, desc, .{ .bytes = key.len }, &streaming);
            try std.testing.expectEqual(key.len, shape.consumed);
            try std.testing.expectEqual(value.value.digits.len, shape.groups.len());
            for (value.value.digits, 0..) |digit, i| try std.testing.expectEqual(digit, shape.groups.at(i));
        }
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
    const descending_keys = try ctx.alloc.alloc([]u8, fixture.value.entries.len);
    const View = @import("../common/sql_numeric_layout.zig").View;
    const views = try ctx.alloc.alloc(View, fixture.value.entries.len);
    for (fixture.value.entries, keys, descending_keys, views) |entry, *key, *descending, *view| {
        var value = try numeric.parse(&ctx, entry.input);
        defer value.deinit();
        key.* = try encodeAlloc(&ctx, value.value);
        var restored = try decode(&ctx, key.*);
        defer restored.deinit();
        const canonical = try encodeAlloc(&ctx, restored.value);
        try std.testing.expectEqualSlices(u8, key.*, canonical);
        const wire = try @import("numeric_binary.zig").encodeAlloc(&ctx, value.value);
        view.* = try View.open(wire, .{});
        descending.* = try ctx.alloc.alloc(u8, key.len);
        var writer: std.Io.Writer = .fixed(descending.*);
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var borrowed: Context = .{ .alloc = failing.allocator() };
        try encodeStored(&borrowed, wire, &writer, true);
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    }
    for (fixture.value.entries, keys, descending_keys, views) |left, left_key, left_desc, left_view| for (fixture.value.entries, keys, descending_keys, views) |right, right_key, right_desc, right_view| {
        const expected = std.math.order(left.rank, right.rank);
        try std.testing.expectEqual(expected, try left_view.order(right_view, &ctx));
        try std.testing.expectEqual(expected, std.mem.order(u8, left_key, right_key));
        try std.testing.expectEqual(expected, std.mem.order(u8, right_desc, left_desc));
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
    for (malformed[0 .. malformed.len - 1]) |key| for ([_]bool{ false, true }) |descending| {
        var buffer: [32]u8 = undefined;
        @memcpy(buffer[0..key.len], key);
        if (descending) for (buffer[0..key.len]) |*byte| {
            byte.* ^= 0xff;
        };
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var ctx: Context = .{ .alloc = failing.allocator() };
        try std.testing.expectError(error.InvalidSqlNumericKey, layout.parsePrefix(buffer[0..key.len], descending, .{}, &ctx));
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    };
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

test "SQL exact NUMERIC stored key admission rejects malformed bytes before output" {
    const malformed = [_][]const u8{
        &.{},
        &.{ 0, 1, 0, 0, 0, 0, 0, 0 }, // Missing coefficient.
        &.{ 0, 1, 0, 0, 0, 0, 0, 0, 0, 0 }, // Noncanonical zero.
        &.{ 0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10 }, // Digit overflow.
        &.{ 0, 2, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0 }, // Trailing zero.
        &.{ 0, 1, 0xff, 0xff, 0, 0, 0, 0, 0, 1 }, // Hidden fraction.
        &.{ 0, 0, 0, 0, 0x40, 0, 0, 0 }, // Negative zero.
        &.{ 0, 0, 0, 0, 0xd0, 0, 0, 0 }, // Noncanonical infinity scale.
    };
    for (malformed) |wire| for ([_]bool{ false, true }) |descending| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        var ctx: Context = .{ .alloc = failing.allocator() };
        var buffer: [32]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&buffer);
        try std.testing.expectError(error.InvalidSqlBinaryRepresentation, encodeStored(&ctx, wire, &writer, descending));
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    };
    const valid = [_]u8{ 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    for ([_]Context{
        .{ .alloc = std.testing.allocator, .max_input_bytes = 9 },
        .{ .alloc = std.testing.allocator, .max_output_bytes = 6 },
        .{ .alloc = std.testing.allocator, .max_groups = 0 },
        .{ .alloc = std.testing.allocator, .remaining = 1 },
    }) |initial| {
        var ctx = initial;
        var buffer: [32]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&buffer);
        try std.testing.expectError(error.SqlProgramLimitExceeded, encodeStored(&ctx, &valid, &writer, false));
        try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
        try std.testing.expectError(error.SqlProgramLimitExceeded, prepareStored(&ctx, &valid));
    }
    // A zero-group budget must fail after the first coefficient. A malformed
    // later word must not be traversed or change the quota failure.
    const oversized = [_]u8{ 3, 0x80, 0, 0, 2, 0xff, 0xff };
    for ([_]bool{ false, true }) |descending| {
        var bytes = oversized;
        if (descending) for (&bytes) |*byte| {
            byte.* ^= 0xff;
        };
        var ctx: Context = .{ .alloc = std.testing.allocator, .remaining = 3 };
        try std.testing.expectError(error.SqlProgramLimitExceeded, layout.parsePrefix(&bytes, descending, .{ .groups = 0 }, &ctx));
        try std.testing.expectEqual(@as(u64, 1), ctx.remaining);
        try std.testing.expectError(error.SqlProgramLimitExceeded, ctx.charge(0));
    }
}

test "SQL exact NUMERIC stored key cancellation is sticky at every borrowed checkpoint" {
    const a = std.testing.allocator;
    var setup: Context = .{ .alloc = a };
    const text: [2048]u8 = @splat('9');
    var value = try numeric.parse(&setup, &text);
    defer value.deinit();
    const wire = try @import("numeric_binary.zig").encodeAlloc(&setup, value.value);
    defer a.free(wire);
    const key = try a.alloc(u8, 5 + value.value.digits.len * 2);
    defer a.free(key);
    const Poll = struct {
        count: usize = 0,
        fail_at: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.count += 1;
            if (self.count == self.fail_at) return error.Canceled;
        }
        fn run(self: *@This(), alloc: std.mem.Allocator, bytes: []const u8, output: []u8, descending: bool) !void {
            var ctx: Context = .{ .alloc = alloc, .checkpoint = check, .ptr = self };
            var writer: std.Io.Writer = .fixed(output);
            encodeStored(&ctx, bytes, &writer, descending) catch |err| {
                try std.testing.expectError(error.Canceled, ctx.charge(0));
                return err;
            };
            _ = layout.parsePrefix(writer.buffered(), descending, .{}, &ctx) catch |err| {
                try std.testing.expectError(error.Canceled, ctx.charge(0));
                return err;
            };
        }
    };
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    for ([_]bool{ false, true }) |descending| {
        var polls: Poll = .{};
        try polls.run(failing.allocator(), wire, key, descending);
        const count = polls.count;
        try std.testing.expect(count >= 6);
        for (1..count + 1) |fail_at| {
            polls = .{ .fail_at = fail_at };
            try std.testing.expectError(error.Canceled, polls.run(failing.allocator(), wire, key, descending));
        }
    }
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
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
    ctx = .{ .alloc = a, .max_groups = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, encodedSize(&ctx, value));
    ctx = .{ .alloc = a, .max_groups = 0 };
    writer = .fixed(&buffer);
    try std.testing.expectError(error.SqlProgramLimitExceeded, encode(&ctx, value, &writer));
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
    ctx = .{ .alloc = a, .max_groups = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, @import("numeric_binary.zig").encode(&ctx, value, &writer));
    try std.testing.expectEqual(@as(usize, 0), writer.buffered().len);
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

test "SQL exact NUMERIC stored key benchmark compares borrowed and owned admission" {
    const a = std.testing.allocator;
    const binary = @import("numeric_binary.zig");
    const text: [2048]u8 = @splat('9');
    const iterations = 256;
    for ([_]usize{ 64, 256, 2048 }) |digits| {
        var setup: Context = .{ .alloc = a };
        var value = try numeric.parse(&setup, text[0..digits]);
        defer value.deinit();
        const wire = try binary.encodeAlloc(&setup, value.value);
        defer a.free(wire);
        const expected = try encodeAlloc(&setup, value.value);
        defer a.free(expected);
        const output = try a.alloc(u8, expected.len);
        defer a.free(output);
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var borrowed: Context = .{ .alloc = failing.allocator() };
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..iterations) |_| {
            var writer: std.Io.Writer = .fixed(output);
            try encodeStored(&borrowed, wire, &writer, false);
        }
        const borrowed_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
        try std.testing.expectEqualSlices(u8, expected, output);
        try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
        var counted = std.testing.FailingAllocator.init(a, .{});
        var owned: Context = .{ .alloc = counted.allocator() };
        const owned_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..iterations) |_| {
            var restored = try binary.decodeCanonical(&owned, wire);
            defer restored.deinit();
            var writer: std.Io.Writer = .fixed(output);
            try encode(&owned, restored.value, &writer);
        }
        const owned_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - owned_start;
        try std.testing.expectEqualSlices(u8, expected, output);
        try std.testing.expectEqual(@as(usize, iterations), counted.alloc_index);
        std.debug.print("NUMERIC stored key: digits={} rows={} borrowed_ns={} owned_ns={} allocations=0/{}\n", .{ digits, iterations, borrowed_ns, owned_ns, counted.alloc_index });
    }
}
