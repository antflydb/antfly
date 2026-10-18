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

//! Arena-owned, immutable native bag counts. Common counts stay inline; products promote
//! through the statement allocator only when u64 is insufficient. SQL cursors
//! continue to negotiate their existing bounded u64 protocol.
const std = @import("std");
const Big = std.math.big.int.Managed;
pub const Count = struct {
    small: u64,
    big: ?*const Big = null,
    pub const one: Count = .{ .small = 1 };
    pub fn init(value: u64) Count {
        return .{ .small = value };
    }
    pub fn cap(self: Count, bound: u64) u64 {
        return if (self.big != null) bound else @min(self.small, bound);
    }
    pub fn exact(self: Count) ?u64 {
        return if (self.big == null) self.small else null;
    }
    pub fn isOne(self: Count) bool {
        return self.big == null and self.small == 1;
    }
    pub fn multiply(self: Count, a: std.mem.Allocator, other: Count) !Count {
        if ((self.big == null and self.small == 0) or (other.big == null and other.small == 0)) return init(0);
        if (self.exact()) |left| if (other.exact()) |right| {
            if (left == 0 or right == 0) return init(0);
            if (std.math.mul(u64, left, right)) |product| return init(product) else |_| {}
        };
        if (self.isOne()) return other;
        if (other.isOne()) return self;
        var left = if (self.big) |p| p.* else try Big.initSet(a, self.small);
        defer if (self.big == null) left.deinit();
        var right = if (other.big) |p| p.* else try Big.initSet(a, other.small);
        defer if (other.big == null) right.deinit();
        const result = try a.create(Big);
        result.* = try Big.initSet(a, 0);
        errdefer result.deinit();
        try result.mul(&left, &right);
        return .{ .small = std.math.maxInt(u64), .big = result };
    }
    pub fn subtract(self: Count, a: std.mem.Allocator, amount: u64) !Count {
        if (amount == 0) return self;
        if (self.exact()) |value| return init(value - amount);
        var subtrahend = try Big.initSet(a, amount);
        defer subtrahend.deinit();
        const result = try a.create(Big);
        result.* = try Big.initSet(a, 0);
        errdefer result.deinit();
        try result.sub(self.big.?, &subtrahend);
        if (result.toInt(u64)) |value| {
            result.deinit();
            return init(value);
        } else |_| {}
        return .{ .small = std.math.maxInt(u64), .big = result };
    }
    pub const Scaled = struct {
        fraction: f128,
        exponent: i64,
        pub fn from(value: f128) Scaled {
            const parts = std.math.frexp(value);
            return .{ .fraction = parts.significand, .exponent = parts.exponent };
        }
        pub fn plus(self: Scaled, other: Scaled) Scaled {
            const high = @max(self.exponent, other.exponent);
            const left = std.math.ldexp(self.fraction, -@as(i32, @intCast(@min(high - self.exponent, 20000))));
            const right = std.math.ldexp(other.fraction, -@as(i32, @intCast(@min(high - other.exponent, 20000))));
            var result = from(left + right);
            result.exponent += high;
            return result;
        }
        pub fn ratio(self: Scaled, total: Scaled) f128 {
            return std.math.ldexp(self.fraction / total.fraction, -@as(i32, @intCast(@min(total.exponent - self.exponent, 20000))));
        }
        pub fn number(self: Scaled) f128 {
            return std.math.ldexp(self.fraction, @intCast(@min(self.exponent, std.math.maxInt(i32))));
        }
    };
    pub fn wideNumber(self: Count) f128 {
        if (self.exact()) |value| return @floatFromInt(value);
        return self.scaled().number();
    }
    pub fn scaled(self: Count) Scaled {
        if (self.big) |big| {
            const value = big.toConst();
            // Retain more than binary128 precision on both 32- and 64-bit
            // targets; native counts also serve the portable WASM runtime.
            const high_limbs = std.math.divCeil(usize, 128, @bitSizeOf(std.math.big.Limb)) catch unreachable;
            const dropped = value.limbs.len -| (high_limbs + 1);
            const high: std.math.big.int.Const = .{ .positive = true, .limbs = value.limbs[dropped..] };
            var result = Scaled.from(high.toFloat(f128, .nearest_even)[0]);
            result.exponent += @as(i64, @intCast(dropped)) * @bitSizeOf(std.math.big.Limb);
            return result;
        }
        return Scaled.from(@floatFromInt(self.small));
    }
};

test "promoted counts preserve products, windows and normalized ratios" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const large = try Count.init(@as(u64, 1) << 63).multiply(a, Count.init(2));
    try std.testing.expect(large.exact() == null);
    const clipped = try large.subtract(a, std.math.maxInt(i64));
    try std.testing.expectEqual(@as(u64, 1) << 63 | 1, clipped.exact().?);
    var enormous = Count.one;
    for (0..400) |_| enormous = try enormous.multiply(a, Count.init(std.math.maxInt(u64)));
    const doubled = try enormous.multiply(a, Count.init(2));
    try std.testing.expectApproxEqAbs(@as(f128, 1.0 / 3.0), enormous.scaled().ratio(enormous.scaled().plus(doubled.scaled())), 1e-16);
}

test "promoted count allocation failures leave arena ownership intact" {
    const Probe = struct {
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const large = try Count.init(std.math.maxInt(u64)).multiply(a, Count.init(2));
            const reduced = try large.subtract(a, std.math.maxInt(u64));
            try std.testing.expectEqual(std.math.maxInt(u64), reduced.exact().?);
            try std.testing.expectEqual(@as(u64, 0), (try large.multiply(a, Count.init(0))).exact().?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
