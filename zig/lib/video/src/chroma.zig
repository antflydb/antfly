// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Native SDR chroma reconstruction: centered 17-bit geometry, cubic C=0.6,
//! Q14 horizontal/Q12 vertical coefficients and residual-error quantization.
//! Reference tools supply independent pixels; no scaler library is linked.
const std = @import("std");
const media = @import("antfly_media");

pub const Tap = extern struct { indices: [5]u32, weights: [5]i32, rounding_bias: i32 };
comptime {
    if (@sizeOf(Tap) != 44) @compileError("Metal chroma coefficient ABI changed");
}
const Raw = struct { first: i32, weights: [5]i64 };
const unit: i64 = 1 << 30;
fn cubic(distance: i64) i64 {
    if (distance >= 2 * unit) return 0;
    const q: i64 = 1 << 24;
    const c: i64 = 10066329;
    const square = (distance * distance) >> 30;
    const cube = (square * distance) >> 30;
    return 6 * (if (distance < unit)
        (2 * q - c) * cube + (c - 3 * q) * square + q * unit
    else
        -c * cube + 5 * c * square - 8 * c * distance + 4 * c * unit);
}
fn rounded(value: i64, denominator: i64) i64 {
    return if (value < 0) -@divTrunc(-value + @divTrunc(denominator, 2), denominator) else @divTrunc(value + @divTrunc(denominator, 2), denominator);
}
pub const Axis = struct {
    allocator: std.mem.Allocator,
    taps: []Tap,
    pub fn init(a: std.mem.Allocator, source: u32, target: u32, bits: u5, control: media.source.Control) !Axis {
        if (source == 0 or target == 0 or source > target or source > 16384 or target > 16384) return error.InvalidVideoGeometry;
        const taps = try a.alloc(Tap, target);
        errdefer a.free(taps);
        if (source == target) {
            for (taps, 0..) |*tap, i| tap.* = .{ .indices = .{ @intCast(i), 0, 0, 0, 0 }, .weights = .{ @as(i32, 1) << bits, 0, 0, 0, 0 }, .rounding_bias = 0 };
            return .{ .allocator = a, .taps = taps };
        }
        const raw = try a.alloc(Raw, target);
        defer a.free(raw);
        const support: usize = @intCast(@max(1, @min(5, @as(i32, @intCast(source)) - 2)));
        const increment = @divTrunc((@as(i64, source) << 16) + target / 2, target);
        for (raw, 0..) |*row, i| {
            try control.check();
            // Retain the half-LSB when the 16-bit increment is odd.
            const center = increment - 65536 + 2 * @as(i64, @intCast(i)) * increment;
            row.* = .{ .first = @intCast(@divTrunc(center - (@as(i64, @intCast(support)) - 2) * 65536, 131072)), .weights = @splat(0) };
            for (0..support) |j| {
                const sample = @as(i64, row.first) + @as(i64, @intCast(j));
                row.weights[j] = cubic(@intCast(@abs(sample * 131072 - center) << 13));
            }
        }
        const cutoff: i64 = 36028797018963; // 0.002 * 2^54, coefficient scale is 6.
        var maximum: usize = 1;
        var i = raw.len;
        while (i != 0) {
            i -= 1;
            var discarded: i64 = 0;
            for (0..support) |_| {
                discarded += @intCast(@abs(raw[i].weights[0]));
                if (discarded > cutoff or (i + 1 < raw.len and raw[i].first >= raw[i + 1].first)) break;
                raw[i].first += 1;
                for (0..support - 1) |j| raw[i].weights[j] = raw[i].weights[j + 1];
                raw[i].weights[support - 1] = 0;
            }
            discarded = 0;
            var count = support;
            while (count > 1) {
                discarded += @intCast(@abs(raw[i].weights[count - 1]));
                if (discarded > cutoff) break;
                count -= 1;
            }
            maximum = @max(maximum, count);
        }
        for (raw, taps) |row, *tap| {
            try control.check();
            tap.* = .{ .indices = @splat(0), .weights = @splat(0), .rounding_bias = if (bits == 12 and maximum > 2) 512 else 0 };
            var merged: [5]i64 = @splat(0);
            var used: usize = 0;
            for (0..maximum) |j| {
                const sample: u32 = @intCast(std.math.clamp(@as(i64, row.first) + @as(i64, @intCast(j)), 0, source - 1));
                if (used != 0 and tap.indices[used - 1] == sample) {
                    merged[used - 1] += row.weights[j];
                } else {
                    tap.indices[used] = sample;
                    merged[used] = row.weights[j];
                    used += 1;
                }
            }
            var sum: i64 = 0;
            for (merged) |value| sum += value;
            const denominator = @divTrunc(sum + (@as(i64, 1) << (bits - 1)), @as(i64, 1) << bits);
            if (denominator <= 0) return error.InvalidVideoGeometry;
            var residual: i64 = 0;
            for (0..used) |j| {
                const value = merged[j] + residual;
                const weight = rounded(value, denominator);
                tap.weights[j] = std.math.cast(i32, weight) orelse return error.ResourceLimitExceeded;
                residual = value - weight * denominator;
            }
            var absolute: u64 = 0;
            for (tap.weights) |weight| absolute += @abs(@as(i64, weight));
            // Safe SIMD accumulation even for a full unsigned 16-bit sample.
            if (bits == 14 and absolute > 32767) return error.ResourceLimitExceeded;
        }
        return .{ .allocator = a, .taps = taps };
    }
    pub fn deinit(self: *Axis) void {
        self.allocator.free(self.taps);
    }
};
pub const Plan = struct {
    x: Axis,
    y: Axis,
    full: bool,
    width: u32,
    height: u32,
    pub fn init(a: std.mem.Allocator, width: u32, height: u32, chroma: u8, control: media.source.Control) !Plan {
        const full = width % 2 != 0 or chroma == 3;
        const cw = if (chroma == 3) width else (width + 1) / 2;
        const ch = if (chroma == 1) (height + 1) / 2 else height;
        var x = try Axis.init(a, cw, if (full) width else cw, 14, control);
        errdefer x.deinit();
        const y = try Axis.init(a, ch, height, 12, control);
        return .{ .x = x, .y = y, .full = full, .width = if (full) width else cw, .height = ch };
    }
    pub fn bytes(self: Plan) usize {
        return (self.x.taps.len + self.y.taps.len) * @sizeOf(Tap);
    }
    pub fn deinit(self: *Plan) void {
        self.x.deinit();
        self.y.deinit();
    }
};
