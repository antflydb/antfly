// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Exact signed DASH availability offsets, independent of presentation clocks.
const std = @import("std");
pub const Offset = union(enum) {
    finite: i64,
    infinite,
    pub fn add(self: Offset, other: Offset) !Offset {
        if (self == .infinite or other == .infinite) return .infinite;
        return .{ .finite = std.math.add(i64, self.finite, other.finite) catch return error.ResourceLimitExceeded };
    }
    pub fn text(self: Offset, a: std.mem.Allocator) ![]const u8 {
        if (self == .infinite) return "INF";
        const n: u64 = @intCast(if (self.finite < 0) -@as(i128, self.finite) else self.finite);
        return std.fmt.allocPrint(a, "{s}{d}.{d:0>9}", .{ if (self.finite < 0) "-" else "", n / 1_000_000_000, n % 1_000_000_000 });
    }
};
/// Nanosecond precision, checked signed arithmetic, and no floating-point
/// rounding of availability boundaries. INF requires finite advertised extent.
pub fn parse(bytes: []const u8) !Offset {
    if (std.mem.eql(u8, bytes, "INF") or std.mem.eql(u8, bytes, "+INF")) return .infinite;
    if (bytes.len == 0) return error.MalformedDash;
    var at: usize = 0;
    const negative = bytes[0] == '-';
    if (negative or bytes[0] == '+') at += 1;
    var mantissa: u128 = 0;
    var digits: usize = 0;
    var fraction: i64 = 0;
    var decimal = false;
    while (at < bytes.len and bytes[at] != 'e' and bytes[at] != 'E') : (at += 1) {
        const ch = bytes[at];
        if (ch == '.' and !decimal) {
            decimal = true;
            continue;
        }
        if (ch < '0' or ch > '9') return error.MalformedDash;
        mantissa = std.math.mul(u128, mantissa, 10) catch return error.ResourceLimitExceeded;
        mantissa = std.math.add(u128, mantissa, ch - '0') catch return error.ResourceLimitExceeded;
        digits += 1;
        if (decimal) fraction += 1;
    }
    if (digits == 0) return error.MalformedDash;
    var exponent: i64 = 0;
    if (at < bytes.len) {
        at += 1;
        const exponent_start = at;
        if (at < bytes.len and (bytes[at] == '-' or bytes[at] == '+')) at += 1;
        if (at == bytes.len) return error.MalformedDash;
        for (bytes[at..]) |digit| if (digit < '0' or digit > '9') return error.MalformedDash;
        exponent = std.fmt.parseInt(i32, bytes[exponent_start..], 10) catch return error.MalformedDash;
    }
    if (mantissa == 0) return .{ .finite = 0 };
    var power = 9 + exponent - fraction;
    while (power < 0 and mantissa % 10 == 0) {
        mantissa /= 10;
        power += 1;
    }
    if (power < 0) return error.UnsupportedDash;
    if (power > 19) return error.ResourceLimitExceeded;
    while (power > 0) : (power -= 1) mantissa = std.math.mul(u128, mantissa, 10) catch return error.ResourceLimitExceeded;
    const maximum: u128 = @as(u128, std.math.maxInt(i64)) + @intFromBool(negative);
    if (mantissa > maximum) return error.ResourceLimitExceeded;
    return .{ .finite = @intCast(if (negative) -@as(i128, @intCast(mantissa)) else @as(i128, @intCast(mantissa))) };
}
test "DASH availability offsets retain signed decimal scientific precision and reject unsafe values" {
    inline for (.{ .{ "0", @as(i64, 0) }, .{ "-1.25", @as(i64, -1250000000) }, .{ "+.5", @as(i64, 500000000) }, .{ "1e-9", @as(i64, 1) }, .{ "1.0000000000", @as(i64, 1000000000) } }) |case| try std.testing.expectEqual(case[1], (try parse(case[0])).finite);
    try std.testing.expect((try parse("INF")) == .infinite);
    for ([_][]const u8{ "", ".", "-", "NaN", "-INF", "1e", "1e+", "1e1_0", "1.2.3" }) |bad| try std.testing.expectError(error.MalformedDash, parse(bad));
    try std.testing.expectError(error.UnsupportedDash, parse("1e-10"));
    try std.testing.expectError(error.ResourceLimitExceeded, parse("1e100"));
    const value = try (try parse("-.25")).add(try parse("0.75"));
    const rendered = try value.text(std.testing.allocator);
    defer std.testing.allocator.free(rendered);
    try std.testing.expectEqualStrings("0.500000000", rendered);
    try std.testing.expectError(error.ResourceLimitExceeded, (Offset{ .finite = std.math.maxInt(i64) }).add(.{ .finite = 1 }));
}
