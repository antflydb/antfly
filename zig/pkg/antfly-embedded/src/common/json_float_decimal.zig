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

//! Exact interpretation of an already-owned JSON floating numeric value.
//! This is not SQL float-to-JSONB casting: that boundary owns its decimal
//! conversion policy. Parsing JSONB text must retain exact decimal tokens.
//! Comparison, hashing and canonical persistence share this allocation-free
//! kernel so persistence cannot round a value after its logical hash is fixed.
const std = @import("std");

pub const Parts = struct {
    negative: bool,
    mantissa: u64,
    binary_exponent: i32,

    pub fn init(number: f64) !Parts {
        if (!std.math.isFinite(number)) return error.InvalidJsonNumber;
        if (number == 0) return .{ .negative = false, .mantissa = 0, .binary_exponent = 0 };
        const bits: u64 = @bitCast(number);
        const raw_exponent = (bits >> 52) & 0x7ff;
        var mantissa: u64 = bits & 0xfffffffffffff;
        if (raw_exponent != 0) mantissa |= 1 << 52;
        var exponent: i32 = if (raw_exponent == 0) -1074 else @as(i32, @intCast(raw_exponent)) - 1023 - 52;
        while (exponent < 0 and mantissa & 1 == 0) {
            mantissa >>= 1;
            exponent += 1;
        }
        return .{ .negative = bits >> 63 != 0, .mantissa = mantissa, .binary_exponent = exponent };
    }

    /// Charge this before expansion when the caller has a work budget.
    pub fn work(self: Parts) usize {
        return @intCast(-@min(self.binary_exponent, 0));
    }

    pub fn decimalExponent(self: Parts) i32 {
        return @min(self.binary_exponent, 0);
    }

    /// At most 767 decimal digits, bounded by IEEE-754 binary64, not input
    /// text. The fixed integer avoids arbitrary-precision heap allocations.
    pub fn coefficient(self: Parts) u4096 {
        var exact: u4096 = self.mantissa;
        if (self.binary_exponent >= 0) exact <<= @intCast(self.binary_exponent) else {
            for (0..self.work()) |_| exact *= 5;
        }
        return exact;
    }

    /// Exact decimal expansion using bounded base-10^9 limbs. Formatting a
    /// u4096 through the generic integer writer repeatedly divides a wide
    /// integer; that dominates cold numeric comparison even for binary64.
    /// Here every multiply and carry fits u64, with no heap or wide division.
    pub fn coefficientText(self: Parts, buffer: []u8) ![]const u8 {
        const radix: u64 = 1_000_000_000;
        var limbs: [86]u32 = undefined; // ceil(767 / 9)
        var len: usize = 0;
        var initial = self.mantissa;
        while (true) {
            limbs[len] = @intCast(initial % radix);
            len += 1;
            initial /= radix;
            if (initial == 0) break;
        }
        var remaining: usize = @intCast(@abs(self.binary_exponent));
        while (remaining != 0) {
            // 5^13 and 2^29 both preserve limb*factor+carry within u64.
            const chunk = @min(remaining, if (self.binary_exponent < 0) @as(usize, 13) else 29);
            const factor: u64 = if (self.binary_exponent < 0) std.math.pow(u64, 5, chunk) else @as(u64, 1) << @intCast(chunk);
            var carry: u64 = 0;
            for (limbs[0..len]) |*limb| {
                const product = @as(u64, limb.*) * factor + carry;
                limb.* = @intCast(product % radix);
                carry = product / radix;
            }
            while (carry != 0) {
                std.debug.assert(len < limbs.len);
                limbs[len] = @intCast(carry % radix);
                len += 1;
                carry /= radix;
            }
            remaining -= chunk;
        }
        const leading = try std.fmt.bufPrint(buffer, "{d}", .{limbs[len - 1]});
        const size = leading.len + (len - 1) * 9;
        if (buffer.len < size) return error.NoSpaceLeft;
        var used = leading.len;
        var index = len - 1;
        while (index != 0) {
            index -= 1;
            _ = try std.fmt.bufPrint(buffer[used..][0..9], "{d:0>9}", .{limbs[index]});
            used += 9;
        }
        return buffer[0..used];
    }
};

test "exact float decimal limbs match the fixed integer coefficient across binary64 exponents" {
    var buffer: [768]u8 = undefined;
    var negative_buffer: [768]u8 = undefined;
    // Every finite exponent, both signs, and mantissa boundaries. The oracle
    // is independent wide multiplication; it never formats the wide integer.
    for (0..2047) |exponent| {
        for ([_]u64{ 0, 1, 0x5555555555555, 0xfffffffffffff }) |mantissa| {
            const bits = (@as(u64, @intCast(exponent)) << 52) | mantissa;
            const parts = try Parts.init(@bitCast(bits));
            const text = try parts.coefficientText(&buffer);
            try std.testing.expect(text.len <= 767);
            try std.testing.expect(text.len == 1 or text[0] != '0');
            // Parse native-sized chunks before widening: one checked wide
            // multiply per nine digits, not one per byte. Both signs still
            // exercise the codec; their shared magnitude needs one oracle.
            const first = (text.len - 1) % 9 + 1;
            var decoded: u4096 = try std.fmt.parseInt(u32, text[0..first], 10);
            var at = first;
            while (at < text.len) : (at += 9) {
                decoded = decoded * 1_000_000_000 + try std.fmt.parseInt(u32, text[at..][0..9], 10);
            }
            try std.testing.expectEqual(parts.coefficient(), decoded);
            try std.testing.expect(!parts.negative);
            const negative = try Parts.init(@bitCast(bits | (@as(u64, 1) << 63)));
            try std.testing.expectEqual(parts.mantissa, negative.mantissa);
            try std.testing.expectEqual(parts.binary_exponent, negative.binary_exponent);
            try std.testing.expectEqual(bits != 0, negative.negative);
            try std.testing.expectEqualStrings(text, try negative.coefficientText(&negative_buffer));
        }
    }
    const half = try Parts.init(0.5);
    try std.testing.expectEqualStrings("5", try half.coefficientText(&buffer));
    const zero = try Parts.init(-0.0);
    try std.testing.expectEqualStrings("0", try zero.coefficientText(&buffer));
    var short: [1]u8 = undefined;
    try std.testing.expectError(error.NoSpaceLeft, (try Parts.init(std.math.floatMax(f64))).coefficientText(&short));
    try std.testing.expectError(error.InvalidJsonNumber, Parts.init(std.math.inf(f64)));
    try std.testing.expectError(error.InvalidJsonNumber, Parts.init(std.math.nan(f64)));
}

test "exact float decimal benchmark bounded limbs against wide integer formatting" {
    if (@import("builtin").mode == .debug) return error.SkipZigTest;
    const count = 64;
    var values: [count]Parts = undefined;
    for (&values, 0..) |*parts, i| {
        const exponent: u64 = @intCast(i * 2046 / (count - 1));
        parts.* = try Parts.init(@bitCast((exponent << 52) | 0xaaaaaaaaaaaaa));
    }
    for (0..3) |sample| {
        var elapsed: [2]i96 = undefined;
        var checksums: [2]u64 = undefined;
        for (0..2) |pass| {
            const optimized = (sample + pass) % 2 != 0;
            const slot = @intFromBool(optimized);
            var buffer: [768]u8 = undefined;
            var checksum = std.hash.Wyhash.init(0);
            const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
            for (values) |parts| {
                const text = if (optimized) try parts.coefficientText(&buffer) else try std.fmt.bufPrint(&buffer, "{d}", .{parts.coefficient()});
                checksum.update(text);
            }
            elapsed[slot] = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
            checksums[slot] = checksum.final();
        }
        try std.testing.expectEqual(checksums[0], checksums[1]);
        std.debug.print("exact_float_decimal {{\"values\":{d},\"sample\":{d},\"wide_ns\":{d},\"limb_ns\":{d}}}\n", .{ count, sample, elapsed[0], elapsed[1] });
    }
}
