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

// Derived from pinned Zig 0.17.0 std/fmt/parse_float.zig; see float_parse/LICENSE.
const std = @import("std");
const math = std.math;
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectError = testing.expectError;
const approxEqAbs = std.math.approxEqAbs;
const epsilon = 1e-7;
const parse = @import("float_parse/parse.zig");
const convertHex = @import("float_parse/convert_hex.zig").convertHex;
const convertFast = @import("float_parse/convert_fast.zig").convertFast;
const convertEiselLemire = @import("float_parse/convert_eisel_lemire.zig").convertEiselLemire;
const convertSlow = @import("float_parse/convert_slow.zig").convertSlowControlled;
const control = @import("float_parse/control.zig");
const Context = @import("numeric_value.zig").Context;

pub const ParseFloatError = error{
    InvalidCharacter,
};

pub fn parseFloat(comptime T: type, s: []const u8) ParseFloatError!T {
    return parseFloatWithWork(T, s, null);
}
pub fn parseFloatControlled(comptime T: type, s: []const u8, context: *Context) !T {
    try context.checkpointNow();
    var work: control.Work = .{ .context = context };
    const result = parseFloatWithWork(T, s, &work) catch |err| {
        if (work.failure) |failure| return failure;
        return err;
    };
    if (work.failure) |failure| return failure;
    try context.checkpointNow();
    return result;
}
/// Poll an already admitted owner without creating a scalar work allowance.
pub fn parseFloatWithAdmission(comptime T: type, s: []const u8, admission: @import("cooperative_json_output.zig").Control) !T {
    try admission.check(admission.ptr);
    var work: control.Work = .{ .admission = admission };
    const result = parseFloatWithWork(T, s, &work) catch |err| {
        if (work.failure) |failure| return failure;
        return err;
    };
    if (work.failure) |failure| return failure;
    try admission.check(admission.ptr);
    return result;
}

fn parseFloatWithWork(comptime T: type, s: []const u8, work: ?*control.Work) ParseFloatError!T {
    if (@typeInfo(T) != .float) {
        @compileError("Cannot parse a float into a non-floating point type.");
    }

    if (s.len == 0) {
        return error.InvalidCharacter;
    }

    var i: usize = 0;
    const negative = s[i] == '-';
    if (s[i] == '-' or s[i] == '+') {
        i += 1;
    }
    if (s.len == i) {
        return error.InvalidCharacter;
    }

    const n = parse.parseNumberControlled(T, s[i..], negative, work) orelse {
        return parse.parseInfOrNan(T, s[i..], negative) orelse error.InvalidCharacter;
    };

    if (n.hex) {
        return convertHex(T, n);
    }

    if (convertFast(T, n)) |f| {
        return f;
    }

    if (T == f16 or T == f32 or T == f64) {
        // If significant digits were truncated, then we can have rounding error
        // only if `mantissa + 1` produces a different result. We also avoid
        // redundantly using the Eisel-Lemire algorithm if it was unable to
        // correctly round on the first pass.
        if (convertEiselLemire(T, n.exponent, n.mantissa)) |bf| {
            if (!n.many_digits) {
                return bf.toFloat(T, n.negative);
            }
            if (convertEiselLemire(T, n.exponent, n.mantissa + 1)) |bf2| {
                if (bf.eql(bf2)) {
                    return bf.toFloat(T, n.negative);
                }
            }
        }
    }

    // Unable to correctly round the float using the Eisel-Lemire algorithm.
    // Fallback to a slower, but always correct algorithm.
    return convertSlow(T, s[i..], work).toFloat(T, negative);
}

// See https://github.com/tiehuis/parse-number-fxx-test-data for a wider-selection of test-data.

test parseFloat {
    inline for ([_]type{ f16, f32, f64, f80, f128 }) |T| {
        try testing.expectError(error.InvalidCharacter, parseFloat(T, ""));
        try testing.expectError(error.InvalidCharacter, parseFloat(T, "   1"));
        try testing.expectError(error.InvalidCharacter, parseFloat(T, "1abc"));
        try testing.expectError(error.InvalidCharacter, parseFloat(T, "+"));
        try testing.expectError(error.InvalidCharacter, parseFloat(T, "-"));

        try expectEqual(try parseFloat(T, "0"), 0.0);
        try expectEqual(try parseFloat(T, "0"), 0.0);
        try expectEqual(try parseFloat(T, "+0"), 0.0);
        try expectEqual(try parseFloat(T, "-0"), 0.0);

        try expectEqual(try parseFloat(T, "0e0"), 0);
        try expectEqual(try parseFloat(T, "2e3"), 2000.0);
        try expectEqual(try parseFloat(T, "1e0"), 1.0);
        try expectEqual(try parseFloat(T, "-2e3"), -2000.0);
        try expectEqual(try parseFloat(T, "-1e0"), -1.0);
        try expectEqual(try parseFloat(T, "1.234e3"), 1234);

        try expect(approxEqAbs(T, try parseFloat(T, "3.141"), 3.141, epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, "-3.141"), -3.141, epsilon));

        try expectEqual(try parseFloat(T, "1e-5000"), 0);
        try expectEqual(try parseFloat(T, "1e+5000"), std.math.inf(T));

        try expectEqual(try parseFloat(T, "0.4e0066999999999999999999999999999999999999999999999999999"), std.math.inf(T));
        try expect(approxEqAbs(T, try parseFloat(T, "0_1_2_3_4_5_6.7_8_9_0_0_0e0_0_1_0"), @as(T, 123456.789000e10), epsilon));

        // underscore rule is simple and reduces to "can only occur between two digits" and multiple are not supported.
        try expectError(error.InvalidCharacter, parseFloat(T, "0123456.789000e_0010")); // cannot occur immediately after exponent
        try expectError(error.InvalidCharacter, parseFloat(T, "_0123456.789000e0010")); // cannot occur before any digits
        try expectError(error.InvalidCharacter, parseFloat(T, "0__123456.789000e_0010")); // cannot occur twice in a row
        try expectError(error.InvalidCharacter, parseFloat(T, "0123456_.789000e0010")); // cannot occur before decimal point
        try expectError(error.InvalidCharacter, parseFloat(T, "0123456.789000e0010_")); // cannot occur at end of number

        try expect(approxEqAbs(T, try parseFloat(T, "1e-2"), 0.01, epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, "1234e-2"), 12.34, epsilon));

        try expect(approxEqAbs(T, try parseFloat(T, "1."), 1, epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, "0."), 0, epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, ".1"), 0.1, epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, ".0"), 0, epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, ".1e-1"), 0.01, epsilon));

        try expectError(error.InvalidCharacter, parseFloat(T, ".")); // At least one digit is required.
        try expectError(error.InvalidCharacter, parseFloat(T, ".e1")); // At least one digit is required.
        try expectError(error.InvalidCharacter, parseFloat(T, "0.e")); // At least one digit is required.

        try expect(approxEqAbs(T, try parseFloat(T, "123142.1"), 123142.1, epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, "-123142.1124"), @as(T, -123142.1124), epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, "0.7062146892655368"), @as(T, 0.7062146892655368), epsilon));
        try expect(approxEqAbs(T, try parseFloat(T, "2.71828182845904523536"), @as(T, 2.718281828459045), epsilon));
    }
}

test "nan and inf" {
    inline for ([_]type{ f16, f32, f64, f80, f128 }) |T| {
        const Z = @Int(.unsigned, @typeInfo(T).float.bits);

        try expectEqual(@as(Z, @bitCast(try parseFloat(T, "nAn"))), @as(Z, @bitCast(std.math.nan(T))));
        try expectEqual(try parseFloat(T, "inF"), std.math.inf(T));
        try expectEqual(try parseFloat(T, "-INF"), -std.math.inf(T));
    }
}

test "largest normals" {
    try expectEqual(@as(u16, @bitCast(try parseFloat(f16, "65504"))), 0x7bff);
    try expectEqual(@as(u32, @bitCast(try parseFloat(f32, "3.4028234664E38"))), 0x7f7f_ffff);
    try expectEqual(@as(u64, @bitCast(try parseFloat(f64, "1.7976931348623157E308"))), 0x7fef_ffff_ffff_ffff);
    try expectEqual(@as(u80, @bitCast(try parseFloat(f80, "1.189731495357231765E4932"))), 0x7ffe_ffff_ffff_ffff_ffff);
    try expectEqual(@as(u128, @bitCast(try parseFloat(f128, "1.1897314953572317650857593266280070162E4932"))), 0x7ffe_ffff_ffff_ffff_ffff_ffff_ffff_ffff);
}

test "#11169" {
    try expectEqual(try parseFloat(f128, "9007199254740993.0"), 9007199254740993.0);
}

test "many_digits hex" {
    const a: f32 = try parseFloat(f32, "0xffffffffffffffff.0p0");
    const b: f32 = @floatCast(try parseFloat(f128, "0xffffffffffffffff.0p0"));
    try std.testing.expectEqual(a, b);
}

test "hex.special" {
    try testing.expect(math.isNan(try parseFloat(f32, "nAn")));
    try testing.expect(math.isPositiveInf(try parseFloat(f32, "iNf")));
    try testing.expect(math.isPositiveInf(try parseFloat(f32, "+Inf")));
    try testing.expect(math.isNegativeInf(try parseFloat(f32, "-iNf")));

    try testing.expect(math.isPositiveInf(try parseFloat(f32, "0x9999p9999")));
    try testing.expect(math.isNegativeInf(try parseFloat(f32, "-0x9999p9999")));
}

test "hex.zero" {
    try testing.expectEqual(@as(f32, 0.0), try parseFloat(f32, "0x0"));
    try testing.expectEqual(@as(f32, 0.0), try parseFloat(f32, "-0x0"));
    try testing.expectEqual(@as(f32, 0.0), try parseFloat(f32, "0x0p42"));
    try testing.expectEqual(@as(f32, 0.0), try parseFloat(f32, "-0x0.00000p42"));
    try testing.expectEqual(@as(f32, 0.0), try parseFloat(f32, "0x0.00000p666"));
}

test "hex.f16" {
    try testing.expectEqual(try parseFloat(f16, "0x1p0"), 1.0);
    try testing.expectEqual(try parseFloat(f16, "-0x1p-1"), -0.5);
    try testing.expectEqual(try parseFloat(f16, "0x10p+10"), 16384.0);
    try testing.expectEqual(try parseFloat(f16, "0x10p-10"), 0.015625);
    // Max normalized value.
    try testing.expectEqual(try parseFloat(f16, "0x1.ffcp+15"), math.floatMax(f16));
    try testing.expectEqual(try parseFloat(f16, "-0x1.ffcp+15"), -math.floatMax(f16));
    // Min normalized value.
    try testing.expectEqual(try parseFloat(f16, "0x1p-14"), math.floatMin(f16));
    try testing.expectEqual(try parseFloat(f16, "-0x1p-14"), -math.floatMin(f16));
    // Min denormal value.
    try testing.expectEqual(try parseFloat(f16, "0x1p-24"), math.floatTrueMin(f16));
    try testing.expectEqual(try parseFloat(f16, "-0x1p-24"), -math.floatTrueMin(f16));
}

test "hex.f32" {
    try testing.expectError(error.InvalidCharacter, parseFloat(f32, "0x"));
    try testing.expectEqual(try parseFloat(f32, "0x1p0"), 1.0);
    try testing.expectEqual(try parseFloat(f32, "-0x1p-1"), -0.5);
    try testing.expectEqual(try parseFloat(f32, "0x10p+10"), 16384.0);
    try testing.expectEqual(try parseFloat(f32, "0x10p-10"), 0.015625);
    try testing.expectEqual(try parseFloat(f32, "0x0.ffffffp128"), 0x0.ffffffp128);
    try testing.expectEqual(try parseFloat(f32, "0x0.1234570p-125"), 0x0.1234570p-125);
    // Max normalized value.
    try testing.expectEqual(try parseFloat(f32, "0x1.fffffeP+127"), math.floatMax(f32));
    try testing.expectEqual(try parseFloat(f32, "-0x1.fffffeP+127"), -math.floatMax(f32));
    // Min normalized value.
    try testing.expectEqual(try parseFloat(f32, "0x1p-126"), math.floatMin(f32));
    try testing.expectEqual(try parseFloat(f32, "-0x1p-126"), -math.floatMin(f32));
    // Min denormal value.
    try testing.expectEqual(try parseFloat(f32, "0x1P-149"), math.floatTrueMin(f32));
    try testing.expectEqual(try parseFloat(f32, "-0x1P-149"), -math.floatTrueMin(f32));
}

test "hex.f64" {
    try testing.expectEqual(try parseFloat(f64, "0x1p0"), 1.0);
    try testing.expectEqual(try parseFloat(f64, "-0x1p-1"), -0.5);
    try testing.expectEqual(try parseFloat(f64, "0x10p+10"), 16384.0);
    try testing.expectEqual(try parseFloat(f64, "0x10p-10"), 0.015625);
    // Max normalized value.
    try testing.expectEqual(try parseFloat(f64, "0x1.fffffffffffffp+1023"), math.floatMax(f64));
    try testing.expectEqual(try parseFloat(f64, "-0x1.fffffffffffffp1023"), -math.floatMax(f64));
    // Min normalized value.
    try testing.expectEqual(try parseFloat(f64, "0x1p-1022"), math.floatMin(f64));
    try testing.expectEqual(try parseFloat(f64, "-0x1p-1022"), -math.floatMin(f64));
    // Min denormalized value.
    try testing.expectEqual(try parseFloat(f64, "0x1p-1074"), math.floatTrueMin(f64));
    try testing.expectEqual(try parseFloat(f64, "-0x1p-1074"), -math.floatTrueMin(f64));
}

test "hex.f80" {
    try testing.expectEqual(try parseFloat(f80, "0x1p0"), 1.0);
    try testing.expectEqual(try parseFloat(f80, "-0x1p-1"), -0.5);
    try testing.expectEqual(try parseFloat(f80, "0x10p+10"), 16384.0);
    try testing.expectEqual(try parseFloat(f80, "0x10p-10"), 0.015625);
    // Max normalized value.
    try testing.expectEqual(try parseFloat(f80, "0xf.fffffffffffffff7p+16380"), math.floatMax(f80));
    try testing.expectEqual(try parseFloat(f80, "-0xf.fffffffffffffff7p+16380"), -math.floatMax(f80));
    // Min normalized value.
    try testing.expectEqual(try parseFloat(f80, "0x1p-16382"), math.floatMin(f80));
    try testing.expectEqual(try parseFloat(f80, "-0x1p-16382"), -math.floatMin(f80));
    // Min denormalized value.
    try testing.expectEqual(try parseFloat(f80, "0x1p-16445"), math.floatTrueMin(f80));
    try testing.expectEqual(try parseFloat(f80, "-0x1p-16445"), -math.floatTrueMin(f80));
}

test "hex.f128" {
    try testing.expectEqual(try parseFloat(f128, "0x1p0"), 1.0);
    try testing.expectEqual(try parseFloat(f128, "-0x1p-1"), -0.5);
    try testing.expectEqual(try parseFloat(f128, "0x10p+10"), 16384.0);
    try testing.expectEqual(try parseFloat(f128, "0x10p-10"), 0.015625);
    // Max normalized value.
    try testing.expectEqual(try parseFloat(f128, "0xf.fffffffffffffffffffffffffff8p+16380"), math.floatMax(f128));
    try testing.expectEqual(try parseFloat(f128, "-0xf.fffffffffffffffffffffffffff8p+16380"), -math.floatMax(f128));
    // Min normalized value.
    try testing.expectEqual(try parseFloat(f128, "0x1p-16382"), math.floatMin(f128));
    try testing.expectEqual(try parseFloat(f128, "-0x1p-16382"), -math.floatMin(f128));
    // Min denormalized value.
    try testing.expectEqual(try parseFloat(f128, "0x1p-16494"), math.floatTrueMin(f128));
    try testing.expectEqual(try parseFloat(f128, "-0x1p-16494"), -math.floatTrueMin(f128));
    // ensure round-to-even
    try testing.expectEqual(try parseFloat(f128, "0x1.edcb34a235253948765432134674fp-1"), 0x1.edcb34a235253948765432134674fp-1);
}

const ScanProbe = struct {
    calls: usize = 0,
    refuse: usize = 0,
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls == self.refuse) return error.QueryCanceled;
    }
    fn context(self: *@This()) Context {
        return .{ .alloc = std.testing.allocator, .remaining = 23, .checkpoint = check, .ptr = self };
    }
};

fn expectStandard(text: []const u8, context: *Context) !void {
    if (std.fmt.parseFloat(f64, text)) |expected| {
        const actual = try parseFloatControlled(f64, text, context);
        if (std.math.isNan(expected)) {
            try testing.expect(std.math.isNan(actual));
        } else try testing.expectEqual(@as(u64, @bitCast(expected)), @as(u64, @bitCast(actual)));
    } else |_| try testing.expectError(error.InvalidCharacter, parseFloatControlled(f64, text, context));
}

test "controlled float matches standard grammar and exact rounding for corpus and generated tokens" {
    var probe: ScanProbe = .{};
    var context = probe.context();
    for ([_][]const u8{ "", "+", "-", "-0", "-0.0", "NaN", "-nan", "+INF", "infinity", "-Infinity", " 1", "1 ", "1_2.3_4e+0_1", "1__2", "1_.2", "1._2", "1e_2", "0x1.fp2", "0X1_0.A_Bp-1_0", "0x1e2", "0x_1", "0x1_p2", ".5", "1.", "5e-324", "2.2250738585072014e-308", "1.7976931348623157e308", "1e309", "1e-9999", "1.00000000000000011102230246251565404236316680908203125", "1.00000000000000011102230246251565404236316680908203125001" }) |text| try expectStandard(text, &context);
    var generator = std.Random.DefaultPrng.init(0x84c16ba32d);
    const random = generator.random();
    const alphabet = "0123456789abcdefABCDEFxXpPeE._+-";
    var text: [128]u8 = undefined;
    for (0..10000) |_| {
        const len = random.intRangeLessThan(usize, 0, text.len + 1);
        for (text[0..len]) |*byte| byte.* = alphabet[random.intRangeLessThan(usize, 0, alphabet.len)];
        try expectStandard(text[0..len], &context);
    }
    for (0..1000) |_| {
        const value: f64 = @bitCast(random.int(u64));
        if (!std.math.isFinite(value)) continue;
        const rendered = try std.fmt.bufPrint(&text, "{e}", .{value});
        try expectStandard(rendered, &context);
    }
    try testing.expectEqual(@as(u64, 23), context.remaining);
}

test "controlled float all long scan checkpoints retain parent failure and numerical bytes" {
    const a = testing.allocator;
    const zeros = try a.alloc(u8, 16384);
    defer a.free(zeros);
    @memset(zeros, '0');
    const underscores = try a.alloc(u8, zeros.len);
    defer a.free(underscores);
    for (underscores, 0..) |*byte, i| byte.* = if (i % 2 == 0) '0' else '_';
    const halfway = "1.00000000000000011102230246251565404236316680908203125";
    for ([_]struct { prefix: []const u8, middle: []const u8, suffix: []const u8 }{
        .{ .prefix = "", .middle = zeros, .suffix = "1.5" },
        .{ .prefix = "0x", .middle = zeros, .suffix = "1.0000000000000001p0" },
        .{ .prefix = halfway, .middle = zeros, .suffix = "" },
        .{ .prefix = halfway, .middle = zeros, .suffix = "1" },
        .{ .prefix = "", .middle = underscores, .suffix = "1.0" },
        .{ .prefix = "1e+", .middle = zeros, .suffix = "1" },
    }) |case| {
        const text = try std.mem.concat(a, u8, &.{ case.prefix, case.middle, case.suffix });
        defer a.free(text);
        var baseline: ScanProbe = .{};
        var parent = baseline.context();
        var child: Context = .{ .alloc = a, .remaining = 19, .parent = &parent };
        try expectStandard(text, &child);
        try testing.expect(baseline.calls >= 16);
        for (1..baseline.calls + 1) |failure| {
            var interrupted: ScanProbe = .{ .refuse = failure };
            var retained = interrupted.context();
            var nested: Context = .{ .alloc = a, .remaining = 19, .parent = &retained };
            try testing.expectError(error.QueryCanceled, parseFloatControlled(f64, text, &nested));
            try testing.expectError(error.QueryCanceled, retained.charge(0));
            try testing.expectError(error.QueryCanceled, nested.checkpointNow());
            try testing.expectEqual(failure, interrupted.calls);
            try testing.expectEqual(@as(u64, 23), retained.remaining);
            try testing.expectEqual(@as(u64, 19), nested.remaining);
        }
    }
}
