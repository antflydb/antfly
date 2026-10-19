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

//! PostgreSQL builtin coercion primitives; no allocation or mutable global state.
const std = @import("std");
pub const WorkControl = @import("sql_parser").lexer.WorkControl;
const Kind = @import("../common/sql_builtin_type.zig").Type;
pub const whitespace = " \t\n\r\x0b\x0c";
pub fn integral(kind: Kind) bool {
    return kind == .int16 or kind == .int32 or kind == .int64;
}
pub fn floating(kind: Kind) bool {
    return kind == .float32 or kind == .float64;
}

/// PostgreSQL's implicit numeric promotion chain, including array elements.
/// Explicit casts such as text-to-integer must not participate in resolution.
pub fn commonNumeric(left: Kind, right: Kind) !Kind {
    if (!(integral(left) or floating(left) or left == .numeric) or !(integral(right) or floating(right) or right == .numeric)) return error.SqlTypeMismatch;
    return if (left == .float64 or right == .float64) .float64 else if (left == .float32 or right == .float32) .float32 else if (left == .numeric or right == .numeric) .numeric else if (left == .int64 or right == .int64) .int64 else if (left == .int32 or right == .int32) .int32 else .int16;
}

pub fn allowed(source: Kind, target: Kind) bool {
    if (source == target or source == .text or target == .text) return true;
    if ((integral(source) or floating(source) or source == .numeric) and (integral(target) or floating(target) or target == .numeric)) return true;
    if ((source == .boolean and target == .int32) or (source == .int32 and target == .boolean)) return true;
    return source == .jsonb and (integral(target) or floating(target) or target == .numeric or target == .boolean);
}

/// PostgreSQL assignment context is narrower than an explicit CAST. Numeric
/// narrowing is legal (and range checked at execution); text input is legal
/// only while still an unknown literal, not as an already typed text value.
pub fn assignmentAllowed(source: Kind, target: Kind) bool {
    return source == target or target == .text or
        ((integral(source) or floating(source) or source == .numeric) and (integral(target) or floating(target) or target == .numeric));
}

pub fn checkedInteger(value: i64, target: Kind) !i64 {
    if (target == .int16 and std.math.cast(i16, value) == null) return error.SqlNumericOutOfRange;
    if (target == .int32 and std.math.cast(i32, value) == null) return error.SqlNumericOutOfRange;
    return value;
}

const IntegerWork = struct {
    control: ?WorkControl,
    remaining: usize = 1024,
    fn check(self: *IntegerWork) error{SqlPreparationInterrupted}!void {
        if (self.control) |value| try value.checkpoint(value.ptr);
    }
    fn poll(self: *IntegerWork) error{SqlPreparationInterrupted}!void {
        if (self.control == null) return;
        self.remaining -= 1;
        if (self.remaining == 0) {
            self.remaining = 1024;
            try self.check();
        }
    }
};

pub fn unsignedTextControlled(input: []const u8, base: u8, control: ?WorkControl) !u64 {
    if (control == null) return std.fmt.parseInt(u64, input, base);
    var work: IntegerWork = .{ .control = control };
    try work.check();
    if (input.len == 0) return error.InvalidCharacter;
    const negative = input[0] == '-';
    var text = if (negative or input[0] == '+') input[1..] else input;
    if (text.len == 0) return error.InvalidCharacter;
    var radix: u8 = if (base == 0) 10 else base;
    if (base == 0 and text.len > 2 and text[0] == '0') switch (std.ascii.toLower(text[1])) {
        'b' => {
            radix = 2;
            text = text[2..];
        },
        'o' => {
            radix = 8;
            text = text[2..];
        },
        'x' => {
            radix = 16;
            text = text[2..];
        },
        else => {},
    };
    if (text[0] == '_' or text[text.len - 1] == '_') return error.InvalidCharacter;
    var magnitude: u64 = 0;
    for (text) |byte| {
        try work.poll();
        if (byte == '_') continue;
        const digit = try std.fmt.charToDigit(byte, radix);
        if (negative and digit != 0) return error.Overflow;
        magnitude = try std.math.mul(u64, magnitude, radix);
        magnitude = try std.math.add(u64, magnitude, digit);
    }
    try work.check();
    return magnitude;
}

pub fn integerText(input: []const u8, target: Kind) !i64 {
    return integerTextControlled(input, target, null);
}

pub fn integerTextControlled(input: []const u8, target: Kind, control: ?WorkControl) !i64 {
    var work: IntegerWork = .{ .control = control };
    try work.check();
    var start: usize = 0;
    var end = input.len;
    while (start < end and std.mem.indexOfScalar(u8, whitespace, input[start]) != null) {
        try work.poll();
        start += 1;
    }
    while (end > start and std.mem.indexOfScalar(u8, whitespace, input[end - 1]) != null) {
        try work.poll();
        end -= 1;
    }
    const text = input[start..end];
    var at: usize = 0;
    if (text.len != 0 and (text[0] == '+' or text[0] == '-')) at = 1;
    var base: u8 = 10;
    if (text.len > at + 2 and text[at] == '0') switch (std.ascii.toLower(text[at + 1])) {
        'x' => {
            base = 16;
            at += 2;
        },
        'o' => {
            base = 8;
            at += 2;
        },
        'b' => {
            base = 2;
            at += 2;
        },
        else => {},
    };
    var digit = false;
    // PostgreSQL allows one underscore immediately after a base prefix.
    if (base != 10 and at < text.len and text[at] == '_') at += 1;
    const first = at;
    for (text[at..]) |byte| {
        try work.poll();
        if (byte == '_') {
            if (!digit) return error.SqlInvalidTextRepresentation;
            digit = false;
        } else {
            _ = std.fmt.charToDigit(byte, base) catch return error.SqlInvalidTextRepresentation;
            digit = true;
        }
    }
    if (!digit or first == text.len) return error.SqlInvalidTextRepresentation;
    // std's parser rejects an underscore directly following the prefix; the
    // normalized magnitude below handles that one PostgreSQL extension.
    const negative = text.len != 0 and text[0] == '-';
    const magnitude = unsignedTextControlled(text[first..], base, control) catch |err| return switch (err) {
        error.SqlPreparationInterrupted => error.SqlPreparationInterrupted,
        error.Overflow => error.SqlNumericOutOfRange,
        else => error.SqlInvalidTextRepresentation,
    };
    if (magnitude > @as(u64, std.math.maxInt(i64)) + @intFromBool(negative)) return error.SqlNumericOutOfRange;
    const result: i64 = if (negative and magnitude == @as(u64, 1) << 63) std.math.minInt(i64) else if (negative) -@as(i64, @intCast(magnitude)) else @intCast(magnitude);
    return checkedInteger(result, target);
}

/// Bridge the preparation parser's narrow interruption error to an existing
/// execution context; original cancellation/deadline failures escape intact.
pub fn integerTextWithContext(input: []const u8, target: Kind, context: ?*@import("numeric_value.zig").Context) !i64 {
    const owner = context orelse return integerText(input, target);
    try owner.checkpointNow();
    const Bridge = struct {
        fn check(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const value: *@import("numeric_value.zig").Context = @ptrCast(@alignCast(raw));
            value.checkpointNow() catch return error.SqlPreparationInterrupted;
        }
    };
    const result = integerTextControlled(input, target, .{ .ptr = owner, .checkpoint = Bridge.check }) catch |err| return owner.failure orelse err;
    try owner.checkpointNow();
    return result;
}

pub fn floatingInteger(value: f64, target: Kind) !i64 {
    if (!std.math.isFinite(value)) return error.SqlNumericOutOfRange;
    const low = @floor(value);
    const fraction = value - low;
    // PostgreSQL float-to-integer casts use rint: ties to even, not @round's
    // ties away from zero. JSONB NUMERIC conversion has a different rule.
    const rounded = if (fraction < 0.5) low else if (fraction > 0.5 or @mod(low, 2) != 0) low + 1 else low;
    if (rounded < -9223372036854775808.0 or rounded >= 9223372036854775808.0) return error.SqlNumericOutOfRange;
    return checkedInteger(@intFromFloat(rounded), target);
}

pub fn booleanText(input: []const u8) !bool {
    return booleanTextControlled(input, null);
}
pub fn booleanTextControlled(input: []const u8, context: ?*@import("numeric_value.zig").Context) !bool {
    if (context) |owner| try owner.checkpointNow();
    const text = try trimTextControlled(input, context);
    if (context) |owner| try owner.checkpointNow();
    if (std.mem.eql(u8, text, "1")) return true;
    if (std.mem.eql(u8, text, "0")) return false;
    if (text.len == 0) return error.SqlInvalidTextRepresentation;
    for ([_][]const u8{ "true", "yes", "false", "no", "on", "off" }, [_]bool{ true, true, false, false, true, false }) |word, result| {
        if (text.len <= word.len and std.ascii.eqlIgnoreCase(text, word[0..text.len]) and !(text.len == 1 and std.ascii.toLower(text[0]) == 'o')) return result;
    }
    return error.SqlInvalidTextRepresentation;
}

pub fn floatValue(comptime T: type, value: std.json.Value) !f64 {
    return floatValueControlled(T, value, null);
}
fn floatPoll(context: ?*@import("numeric_value.zig").Context, index: usize) !void {
    if (index % 1024 == 0) if (context) |work| try work.checkpointNow();
}
fn trimTextControlled(input: []const u8, context: ?*@import("numeric_value.zig").Context) ![]const u8 {
    var first: usize = 0;
    var last = input.len;
    while (first < last and std.mem.indexOfScalar(u8, whitespace, input[first]) != null) : (first += 1) try floatPoll(context, first);
    while (last > first and std.mem.indexOfScalar(u8, whitespace, input[last - 1]) != null) : (last -= 1) try floatPoll(context, input.len - last);
    return input[first..last];
}
pub fn floatValueControlled(comptime T: type, value: std.json.Value, context: ?*@import("numeric_value.zig").Context) !f64 {
    if (context) |work| try work.checkpointNow();
    var nonzero = false;
    var special = false;
    const result: T = switch (value) {
        .integer => |integer| blk: {
            nonzero = integer != 0;
            break :blk @floatFromInt(integer);
        },
        .float => |number| return @import("../common/float_domain.zig").normalize(number, if (T == f32) .float32 else .float64),
        .string, .number_string => |input| blk: {
            const text = try trimTextControlled(input, context);
            const positive = if (text.len != 0 and (text[0] == '+' or text[0] == '-')) text[1..] else text;
            const nan_payload = positive.len >= 5 and std.ascii.eqlIgnoreCase(positive[0..4], "nan(") and positive[positive.len - 1] == ')' and payload: {
                for (positive[4 .. positive.len - 1], 0..) |byte, index| {
                    try floatPoll(context, index);
                    if (!std.ascii.isAlphanumeric(byte) and byte != '_') break :payload false;
                }
                break :payload true;
            };
            if (std.ascii.eqlIgnoreCase(positive, "nan") or nan_payload) {
                special = true;
                break :blk std.math.nan(T);
            }
            if (std.ascii.eqlIgnoreCase(positive, "infinity") or std.ascii.eqlIgnoreCase(positive, "inf")) {
                special = true;
                break :blk if (text.len != 0 and text[0] == '-') -std.math.inf(T) else std.math.inf(T);
            }
            // Zig accepts underscores in numbers; PostgreSQL float input does
            // not. Hex mantissas may contain e/E, unlike decimal exponents.
            for (text, 0..) |byte, index| {
                try floatPoll(context, index);
                if (byte == '_') return error.SqlInvalidTextRepresentation;
            }
            const hex = positive.len >= 2 and std.ascii.eqlIgnoreCase(positive[0..2], "0x");
            const mantissa = if (hex) positive[2..] else positive;
            for (mantissa, 0..) |byte, index| {
                try floatPoll(context, index);
                if ((hex and (byte == 'p' or byte == 'P')) or (!hex and (byte == 'e' or byte == 'E'))) break;
                nonzero = nonzero or (byte >= '1' and byte <= '9') or (hex and std.ascii.toLower(byte) >= 'a' and std.ascii.toLower(byte) <= 'f');
            }
            break :blk if (context) |work|
                @import("controlled_float.zig").parseFloatControlled(T, text, work) catch |err| return if (err == error.InvalidCharacter) error.SqlInvalidTextRepresentation else err
            else
                std.fmt.parseFloat(T, text) catch return error.SqlInvalidTextRepresentation;
        },
        else => return error.SqlTypeMismatch,
    };
    if (context) |work| try work.checkpointNow();
    if (!special and (!std.math.isFinite(result) or (result == 0 and nonzero))) return error.SqlNumericOutOfRange;
    return result;
}

pub fn floatText(comptime T: type, value: T, buffer: *[64]u8) ![]const u8 {
    if (std.math.isNan(value)) return "NaN";
    if (std.math.isInf(value)) return if (value < 0) "-Infinity" else "Infinity";
    const scientific = try std.fmt.float.render(buffer, value, .{ .mode = .scientific });
    const split = std.mem.indexOfScalar(u8, scientific, 'e') orelse return error.InvalidSqlProgram;
    const exponent = std.fmt.parseInt(i32, scientific[split + 1 ..], 10) catch return error.InvalidSqlProgram;
    if (exponent >= -4 and exponent < (if (T == f32) @as(i32, 6) else @as(i32, 15))) return std.fmt.float.render(buffer, value, .{ .mode = .decimal });
    buffer[split + 1] = if (exponent < 0) '-' else '+';
    const magnitude: u32 = @intCast(@abs(exponent));
    var end = split + 2;
    if (magnitude >= 100) {
        buffer[end] = '0' + @as(u8, @intCast(magnitude / 100));
        end += 1;
    }
    buffer[end] = '0' + @as(u8, @intCast(magnitude / 10 % 10));
    buffer[end + 1] = '0' + @as(u8, @intCast(magnitude % 10));
    return buffer[0 .. end + 2];
}

test "SQL builtin casts preserve integer boundaries and PostgreSQL float input syntax" {
    try std.testing.expectEqual(std.math.minInt(i64), try integerText("-0x8000000000000000", .int64));
    try std.testing.expectEqual(std.math.maxInt(i64), try integerText("9223372036854775807", .int64));
    try std.testing.expectError(error.SqlNumericOutOfRange, integerText("9223372036854775808", .int64));
    try std.testing.expectError(error.SqlNumericOutOfRange, integerText("-32769", .int16));
    try std.testing.expectError(error.SqlInvalidTextRepresentation, floatValue(f64, .{ .string = "1_0" }));
    try std.testing.expectError(error.SqlNumericOutOfRange, floatValue(f64, .{ .string = "0xAp-20000" }));
    try std.testing.expectEqual(@as(f64, 1), try floatValue(f64, .{ .string = "0x1p0" }));
    try std.testing.expect(std.math.isNan(try floatValue(f64, .{ .string = "nan(1)" })));
    for ([_]f64{ 2.5, 3.5, -2.5, -3.5 }, [_]i64{ 2, 4, -2, -4 }) |input, expected| try std.testing.expectEqual(expected, try floatingInteger(input, .int32));
}

test "controlled integer preparation preserves signs prefixes separators and range errors" {
    const Probe = struct {
        fn checkpoint(_: *anyopaque) error{SqlPreparationInterrupted}!void {}
    };
    var marker: u8 = 0;
    const control: WorkControl = .{ .ptr = &marker, .checkpoint = Probe.checkpoint };
    for ([_][]const u8{ "0", "00000001", "+42", "-0", "0b111", "0Xff", "0o77", "1_2_3", "1__2", "18446744073709551615" }) |text| {
        try std.testing.expectEqual(try std.fmt.parseInt(u64, text, 0), try unsignedTextControlled(text, 0, control));
    }
    for ([_][]const u8{ "18446744073709551616", "-1" }) |text|
        try std.testing.expectError(error.Overflow, unsignedTextControlled(text, 0, control));
    for ([_][]const u8{ "", "+", "_1", "1_", "0x_1", "0x1_", "1x" }) |text|
        try std.testing.expectError(error.InvalidCharacter, unsignedTextControlled(text, 0, control));
    try std.testing.expectEqual(std.math.minInt(i64), try integerTextControlled("-0x_8000000000000000", .int64, control));
    try std.testing.expectEqual(@as(i64, 32767), try integerTextControlled(" +32_767 ", .int16, control));
    try std.testing.expectError(error.SqlNumericOutOfRange, integerTextControlled("32768", .int16, control));
    try std.testing.expectError(error.SqlInvalidTextRepresentation, integerTextControlled("18446744073709551616x", .int64, control));
}

test "controlled integer preparation interrupts long zero and whitespace scans" {
    const Probe = struct {
        checks: usize = 0,
        fn checkpoint(raw: *anyopaque) error{SqlPreparationInterrupted}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == 3) return error.SqlPreparationInterrupted;
        }
    };
    const zeros: [8192]u8 = @splat('0');
    const spaces: [8192]u8 = @splat(' ');
    for ([_][]const u8{ &zeros, spaces ++ "1", "1" ++ spaces }) |text| {
        var probe: Probe = .{};
        try std.testing.expectError(error.SqlPreparationInterrupted, integerTextControlled(text, .int32, .{ .ptr = &probe, .checkpoint = Probe.checkpoint }));
        try std.testing.expectEqual(@as(usize, 3), probe.checks);
    }
}

test "controlled floating input preserves PostgreSQL grammar diagnostics and polling without work charges" {
    const Context = @import("numeric_value.zig").Context;
    const Probe = struct {
        calls: usize = 0,
        refuse: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.refuse) return error.QueryCanceled;
        }
    };
    const a = std.testing.allocator;
    const zeros = try a.alloc(u8, 16384);
    defer a.free(zeros);
    @memset(zeros, '0');
    for ([_]struct { prefix: []const u8, fill: u8, suffix: []const u8 }{
        .{ .prefix = "", .fill = ' ', .suffix = "-1.5 " },
        .{ .prefix = "nan(", .fill = 'x', .suffix = ")" },
        .{ .prefix = "0x", .fill = '0', .suffix = "1.fp2" },
        .{ .prefix = "1e", .fill = '0', .suffix = "1" },
    }) |case| {
        @memset(zeros, case.fill);
        const text = try std.mem.concat(a, u8, &.{ case.prefix, zeros, case.suffix });
        defer a.free(text);
        inline for (.{ f32, f64 }) |T| {
            var probe: Probe = .{};
            var context: Context = .{ .alloc = a, .remaining = 7, .checkpoint = Probe.check, .ptr = &probe };
            const actual = try floatValueControlled(T, .{ .string = text }, &context);
            const expected = try floatValue(T, .{ .string = text });
            if (std.math.isNan(expected)) {
                try std.testing.expect(std.math.isNan(actual));
            } else try std.testing.expectEqual(@as(u64, @bitCast(expected)), @as(u64, @bitCast(actual)));
            try std.testing.expect(probe.calls > 16);
            for (1..probe.calls + 1) |failure| {
                var interrupted: Probe = .{ .refuse = failure };
                var work: Context = .{ .alloc = a, .remaining = 7, .checkpoint = Probe.check, .ptr = &interrupted };
                try std.testing.expectError(error.QueryCanceled, floatValueControlled(T, .{ .string = text }, &work));
                try std.testing.expectError(error.QueryCanceled, work.charge(0));
                try std.testing.expectEqual(failure, interrupted.calls);
                try std.testing.expectEqual(@as(u64, 7), work.remaining);
            }
            try std.testing.expectEqual(@as(u64, 7), context.remaining);
        }
    }
    var context: Context = .{ .alloc = a };
    try std.testing.expectError(error.SqlInvalidTextRepresentation, floatValueControlled(f64, .{ .string = "1_2" }, &context));
    try std.testing.expectError(error.SqlNumericOutOfRange, floatValueControlled(f32, .{ .string = "1e-100" }, &context));
    try std.testing.expectEqual(-std.math.inf(f64), try floatValueControlled(f64, .{ .string = "-Infinity" }, &context));
}

test "controlled cast input integer and boolean preserve grammar diagnostics work and original failures" {
    const Context = @import("numeric_value.zig").Context;
    const Probe = struct {
        calls: usize = 0,
        refuse: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.refuse) return error.QueryCanceled;
        }
    };
    const a = std.testing.allocator;
    var healthy: Context = .{ .alloc = a, .remaining = 31 };
    for ([_][]const u8{ "", "+", "0x_ff", "-9223372036854775808", "9223372036854775808", "1__2", "  0b101  ", "1_2", "123x" }) |text| {
        if (integerText(text, .int64)) |expected| {
            try std.testing.expectEqual(expected, try integerTextWithContext(text, .int64, &healthy));
        } else |err| try std.testing.expectError(err, integerTextWithContext(text, .int64, &healthy));
    }
    for ([_][]const u8{ "", "true", "T", "yes", "off", "o", "fa", "  1 \t", "not", "0" }) |text| {
        if (booleanText(text)) |expected| {
            try std.testing.expectEqual(expected, try booleanTextControlled(text, &healthy));
        } else |err| try std.testing.expectError(err, booleanTextControlled(text, &healthy));
    }
    const padding = try a.alloc(u8, 16384);
    defer a.free(padding);
    for ([_]bool{ false, true }) |integer_mode| {
        @memset(padding, if (integer_mode) '0' else ' ');
        const text = try std.mem.concat(a, u8, &.{ padding, if (integer_mode) "7" else "true" });
        defer a.free(text);
        var probe: Probe = .{};
        var parent: Context = .{ .alloc = a, .remaining = 31, .checkpoint = Probe.check, .ptr = &probe };
        if (integer_mode) {
            try std.testing.expectEqual(@as(i64, 7), try integerTextWithContext(text, .int64, &parent));
        } else try std.testing.expect(try booleanTextControlled(text, &parent));
        try std.testing.expect(probe.calls >= 16);
        for (1..probe.calls + 1) |failure| {
            var interrupted: Probe = .{ .refuse = failure };
            var retained: Context = .{ .alloc = a, .remaining = 31, .checkpoint = Probe.check, .ptr = &interrupted };
            var child: Context = .{ .alloc = a, .remaining = 23, .parent = &retained };
            if (integer_mode) {
                try std.testing.expectError(error.QueryCanceled, integerTextWithContext(text, .int64, &child));
            } else try std.testing.expectError(error.QueryCanceled, booleanTextControlled(text, &child));
            try std.testing.expectError(error.QueryCanceled, retained.charge(0));
            try std.testing.expectEqual(@as(u64, 31), retained.remaining);
            try std.testing.expectEqual(@as(u64, 23), child.remaining);
            try std.testing.expectEqual(failure, interrupted.calls);
        }
        try std.testing.expectEqual(@as(u64, 31), parent.remaining);
    }
}
