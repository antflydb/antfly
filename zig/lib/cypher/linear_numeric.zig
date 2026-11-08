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

//! Exact bounded linear arithmetic. Floating operations are admitted only when
//! the entire input lattice fits binary64; rounded expressions remain exact
//! distribution consumers. Coefficients retain producer segment provenance.
const std = @import("std");
const A = std.mem.Allocator;
pub const Op = enum { add, subtract, multiply, divide };
pub const Lattice = struct {
    low: i64,
    high: i64,
    shift: i16 = 0,
    floating: bool = false,
    pub fn integer(n: i64) Lattice {
        return .{ .low = n, .high = n };
    }
    pub fn number(n: f64) ?Lattice {
        if (!std.math.isFinite(n)) return null;
        if (n == 0) return .{ .low = 0, .high = 0, .floating = true };
        const bits: u64 = @bitCast(n);
        const exp: i16 = @intCast((bits >> 52) & 0x7ff);
        var mantissa: u64 = (bits & ((@as(u64, 1) << 52) - 1)) | (if (exp != 0) @as(u64, 1) << 52 else 0);
        const zeros: u6 = @intCast(@ctz(mantissa));
        mantissa >>= zeros;
        const signed: i64 = @intCast(mantissa);
        return .{ .low = if (bits >> 63 != 0) -signed else signed, .high = if (bits >> 63 != 0) -signed else signed, .shift = (if (exp == 0) @as(i16, -1074) else exp - 1023 - 52) + zeros, .floating = true };
    }
    /// Admit a decimal literal only if it denotes this exact binary lattice.
    /// Parsing a rounded binary64 approximation would certify the wrong SQL
    /// NUMERIC value (for example 0.1).
    pub fn decimal(text: []const u8) ?Lattice {
        if (text.len == 0 or text.len > 100) return null;
        var position: usize = @intFromBool(text[0] == '-' or text[0] == '+');
        const negative_value = text[0] == '-';
        var coefficient: i128 = 0;
        var fractional: i32 = 0;
        var dot = false;
        while (position < text.len and text[position] != 'e' and text[position] != 'E') : (position += 1) {
            const char = text[position];
            if (char == '.' and !dot) {
                dot = true;
                continue;
            }
            if (char < '0' or char > '9') return null;
            coefficient = std.math.add(i128, std.math.mul(i128, coefficient, 10) catch return null, char - '0') catch return null;
            if (dot) fractional += 1;
        }
        const exponent = if (position < text.len) std.fmt.parseInt(i32, text[position + 1 ..], 10) catch return null else 0;
        const power = std.math.sub(i32, exponent, fractional) catch return null;
        if (power < -1074 or power > 308) return null;
        if (coefficient == 0) return number(0);
        var shift: i16 = 0;
        if (power < 0) {
            for (0..@intCast(-power)) |_| {
                if (@rem(coefficient, 5) != 0) return null;
                coefficient = @divExact(coefficient, 5);
            }
            shift = @intCast(power);
        } else {
            for (0..@intCast(power)) |_| {
                coefficient = std.math.mul(i128, coefficient, 10) catch return null;
            }
        }
        while (@rem(coefficient, 2) == 0) {
            coefficient = @divExact(coefficient, 2);
            shift += 1;
        }
        const n = std.math.cast(i64, if (negative_value) -coefficient else coefficient) orelse return null;
        const result: Lattice = .{ .low = n, .high = n, .shift = shift, .floating = true };
        return if (result.exactFloat()) result else null;
    }
    fn magnitude(self: Lattice) i128 {
        return @intCast(@max(@abs(@as(i128, self.low)), @abs(@as(i128, self.high))));
    }
    pub fn exactFloat(self: Lattice) bool {
        if (self.magnitude() > (@as(i128, 1) << 53) or self.shift < -1074) return false;
        const bound: f128 = @as(f128, @floatFromInt(self.magnitude())) * @exp2(@as(f128, @floatFromInt(self.shift)));
        return bound <= std.math.floatMax(f64);
    }
    fn scale(n: i64, shift: i32) ?i128 {
        if (n == 0) return 0;
        if (shift < 0 or shift > 62) return null;
        return @as(i128, n) * (@as(i128, 1) << @as(u7, @intCast(shift)));
    }
    pub fn negative(self: Lattice) ?Lattice {
        return .{ .low = std.math.negate(self.high) catch return null, .high = std.math.negate(self.low) catch return null, .shift = self.shift, .floating = self.floating };
    }
    pub fn combine(l: Lattice, r: Lattice, op: Op) ?Lattice {
        const floating = l.floating or r.floating;
        if (floating and (!l.exactFloat() or !r.exactFloat())) return null;
        var shift: i16 = undefined;
        var low: i128 = undefined;
        var high: i128 = undefined;
        switch (op) {
            .add, .subtract => {
                shift = @min(l.shift, r.shift);
                const ll = scale(l.low, @as(i32, l.shift) - shift) orelse return null;
                const lh = scale(l.high, @as(i32, l.shift) - shift) orelse return null;
                const rl = scale(r.low, @as(i32, r.shift) - shift) orelse return null;
                const rh = scale(r.high, @as(i32, r.shift) - shift) orelse return null;
                low = if (op == .add) ll + rl else ll - rh;
                high = if (op == .add) lh + rh else lh - rl;
            },
            .multiply => {
                shift = std.math.add(i16, l.shift, r.shift) catch return null;
                low = @min(@as(i128, l.low) * r.low, @as(i128, l.low) * r.high, @as(i128, l.high) * r.low, @as(i128, l.high) * r.high);
                high = @max(@as(i128, l.low) * r.low, @as(i128, l.low) * r.high, @as(i128, l.high) * r.low, @as(i128, l.high) * r.high);
            },
            .divide => {
                // Integer division truncates. General floating reciprocals
                // round. A signed power of two has an exact reciprocal.
                if (!floating or r.low != r.high or (r.low != 1 and r.low != -1)) return null;
                shift = std.math.sub(i16, l.shift, r.shift) catch return null;
                low = if (r.low == 1) l.low else -@as(i128, l.high);
                high = if (r.low == 1) l.high else -@as(i128, l.low);
            },
        }
        const result: Lattice = .{ .low = std.math.cast(i64, low) orelse return null, .high = std.math.cast(i64, high) orelse return null, .shift = shift, .floating = floating };
        if (floating and !result.exactFloat()) return null;
        return result;
    }
    pub fn equal(l: Lattice, r: Lattice) bool {
        const shift = @min(l.shift, r.shift);
        return (scale(l.low, @as(i32, l.shift) - shift) orelse return false) == (scale(r.low, @as(i32, r.shift) - shift) orelse return false);
    }
    pub fn zero(self: Lattice) bool {
        return self.low == 0 and self.high == 0;
    }
};
pub const Form = struct {
    range: Lattice,
    terms: []const Lattice = &.{},
    offset: Lattice,
    // A bounded choice of total-axis expressions need not have one set of
    // coefficients. Retain dependence without inventing a linear formula.
    opaque_total: bool = false,
    pub fn literal(n: Lattice) Form {
        return .{ .range = n, .offset = n };
    }
    pub fn column(a: A, range: Lattice, dimensions: usize, segment: ?usize) !Form {
        const terms = try a.alloc(Lattice, dimensions);
        for (terms, 0..) |*coefficient, i| coefficient.* = Lattice.integer(@intFromBool(segment == null or segment.? == i));
        return .{ .range = range, .terms = terms, .offset = Lattice.integer(0) };
    }
    fn term(self: Form, i: usize) Lattice {
        return if (i < self.terms.len) self.terms[i] else Lattice.integer(0);
    }
    pub fn constant(self: Form) bool {
        if (self.opaque_total) return false;
        for (self.terms) |t| if (!t.zero()) return false;
        return true;
    }
    pub fn total(self: Form) bool {
        if (self.terms.len == 0) return true;
        for (self.terms) |t| if (!Lattice.equal(t, self.terms[0])) return false;
        return true;
    }
    pub fn choice(left: Form, right: Form) ?Form {
        if (left.range.floating != right.range.floating) return null;
        const shift = @min(left.range.shift, right.range.shift);
        const range: Lattice = .{
            .low = std.math.cast(i64, @min(Lattice.scale(left.range.low, @as(i32, left.range.shift) - shift) orelse return null, Lattice.scale(right.range.low, @as(i32, right.range.shift) - shift) orelse return null)) orelse return null,
            .high = std.math.cast(i64, @max(Lattice.scale(left.range.high, @as(i32, left.range.shift) - shift) orelse return null, Lattice.scale(right.range.high, @as(i32, right.range.shift) - shift) orelse return null)) orelse return null,
            .shift = shift,
            .floating = left.range.floating,
        };
        if (range.floating and !range.exactFloat()) return null;
        const same = same: {
            if (left.opaque_total or right.opaque_total or left.terms.len != right.terms.len or !Lattice.equal(left.offset, right.offset)) break :same false;
            for (left.terms, right.terms) |l, r| if (!Lattice.equal(l, r)) break :same false;
            break :same true;
        };
        if (same) {
            var result = left;
            result.range = range;
            return result;
        }
        if (!left.total() or !right.total()) return null;
        return .{ .range = range, .offset = Lattice.integer(0), .opaque_total = true };
    }
    pub fn negative(self: Form, a: A) !?Form {
        const range = self.range.negative() orelse return null;
        const offset = self.offset.negative() orelse return null;
        const terms = try a.alloc(Lattice, self.terms.len);
        for (self.terms, terms) |t, *out| out.* = t.negative() orelse return null;
        return .{ .range = range, .terms = terms, .offset = offset, .opaque_total = self.opaque_total };
    }
    pub fn combine(l: Form, r: Form, a: A, op: Op) !?Form {
        if ((op == .multiply and !l.constant() and !r.constant()) or (op == .divide and !r.constant())) return null;
        const range = Lattice.combine(l.range, r.range, op) orelse return null;
        if (l.opaque_total or r.opaque_total) {
            if (!l.total() or !r.total()) return null;
            return .{ .range = range, .offset = Lattice.integer(0), .opaque_total = true };
        }
        const offset = Lattice.combine(l.offset, r.offset, op) orelse return null;
        const terms = try a.alloc(Lattice, @max(l.terms.len, r.terms.len));
        for (terms, 0..) |*out, i| out.* = switch (op) {
            .add, .subtract => Lattice.combine(l.term(i), r.term(i), op) orelse return null,
            .multiply => if (l.constant()) Lattice.combine(l.offset, r.term(i), op) orelse return null else Lattice.combine(l.term(i), r.offset, op) orelse return null,
            .divide => Lattice.combine(l.term(i), r.offset, op) orelse return null,
        };
        return .{ .range = range, .terms = terms, .offset = offset };
    }
};

test "numeric lattices prove exact binary arithmetic and reject rounding and errors" {
    const expect = std.testing.expect;
    const input: Lattice = .{ .low = 0, .high = 1024 };
    const twice = Lattice.combine(input, Lattice.number(2.0).?, .multiply).?;
    try expect(twice.floating and twice.high == 1024 and twice.shift == 1);
    try expect(Lattice.combine(input, Lattice.number(2.0).?, .divide) != null);
    try expect(Lattice.combine(input, Lattice.number(0.1).?, .multiply) == null);
    try expect(Lattice.combine(input, Lattice.number(3.0).?, .divide) == null);
    try expect(Lattice.combine(input, Lattice.number(0.0).?, .divide) == null);
    try expect(Lattice.combine(input, .integer(2), .divide) == null);
    try expect(Lattice.combine(.{ .low = 0, .high = 9007199254740993 }, Lattice.number(1.0).?, .multiply) == null);
    try expect(Lattice.combine(input, Lattice.number(std.math.floatMax(f64)).?, .multiply) == null);
    try expect(Lattice.number(std.math.inf(f64)) == null);
    const subnormal = Lattice.number(0x1p-1074).?;
    try expect(Lattice.combine(input, subnormal, .multiply) != null);
    try expect(Lattice.combine(subnormal, Lattice.number(2).?, .divide) == null);
    try expect(Lattice.combine(.integer(std.math.maxInt(i64)), .integer(1), .add) == null);
}

test "segment lineage proves total length without inventing joint extrema" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const x = try Form.column(a, .{ .low = 0, .high = 1024 }, 2, 0);
    const y = try Form.column(a, .{ .low = 0, .high = 1024 }, 2, 1);
    const sum = (try Form.combine(x, y, a, .add)).?;
    try std.testing.expect(sum.total());
    try std.testing.expect(!(try Form.combine(x, y, a, .subtract)).?.total());
    try std.testing.expect((try Form.combine(sum, Form.literal(Lattice.number(0.5).?), a, .multiply)).?.total());
    try std.testing.expect(try Form.combine(x, y, a, .multiply) == null);
    const first_of_three = try Form.column(a, .{ .low = 0, .high = 1024 }, 3, 0);
    const second_of_three = try Form.column(a, .{ .low = 0, .high = 1024 }, 3, 1);
    try std.testing.expect(!(try Form.combine(first_of_three, second_of_three, a, .add)).?.total());
}

test "decimal literals must equal their binary lattice exactly" {
    try std.testing.expect(Lattice.equal(Lattice.decimal("2.0").?, Lattice.number(2).?));
    try std.testing.expect(Lattice.equal(Lattice.decimal("-0.125").?, Lattice.number(-0.125).?));
    try std.testing.expect(Lattice.equal(Lattice.decimal("1.25e2").?, Lattice.number(125).?));
    try std.testing.expect(Lattice.decimal("0.1") == null);
    try std.testing.expect(Lattice.decimal("9007199254740993.0") == null);
    try std.testing.expect(Lattice.decimal("1e999999999999999999999") == null);
}
