// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const Bits = @import("h264_bits.zig").Bits;
pub const Weight = struct { denominator: u3 = 0, weight: i16 = 1, offset: i16 = 0 };
pub const Table = [2][16][3]Weight;
pub fn parse(bits: *Bits, active: [2]usize, b_slice: bool) !Table {
    const luma = try bits.ue();
    const chroma = try bits.ue();
    if (luma > 7 or chroma > 7) return error.MalformedVideoPacket;
    var table: Table = @splat(@splat(@splat(.{})));
    for (0..if (b_slice) @as(usize, 2) else 1) |list| for (0..active[list]) |reference| {
        for (0..3) |p| {
            const denominator = if (p == 0) luma else chroma;
            table[list][reference][p] = .{ .denominator = @intCast(denominator), .weight = @as(i16, 1) << @as(u4, @intCast(denominator)) };
        }
        if (try bits.read(1) != 0) table[list][reference][0] = try read(bits, luma);
        if (try bits.read(1) != 0) for (1..3) |p| {
            table[list][reference][p] = try read(bits, chroma);
        };
    };
    return table;
}
fn read(bits: *Bits, denominator: u32) !Weight {
    const weight = try bits.se();
    const offset = try bits.se();
    if (weight < -128 or weight > 127 or offset < -128 or offset > 127) return error.MalformedVideoPacket;
    return .{ .denominator = @intCast(denominator), .weight = @intCast(weight), .offset = @intCast(offset) };
}
pub fn single(value: u8, weight: Weight) u8 {
    const rounding: i32 = if (weight.denominator == 0) 0 else @as(i32, 1) << @as(u5, weight.denominator - 1);
    return @intCast(std.math.clamp(((@as(i32, value) * weight.weight + rounding) >> weight.denominator) + weight.offset, 0, 255));
}
pub fn pair(a: u8, b: u8, wa: Weight, wb: Weight) u8 {
    const rounding: i32 = @as(i32, 1) << wa.denominator;
    return @intCast(std.math.clamp(((@as(i32, a) * wa.weight + @as(i32, b) * wb.weight + rounding) >> (@as(u5, wa.denominator) + 1)) + ((@as(i32, wa.offset) + wb.offset + 1) >> 1), 0, 255));
}
pub fn distance(current: i32, first: i32, second: i32) i32 {
    const td = std.math.clamp(second - first, -128, 127);
    if (td == 0) return 256;
    const tb = std.math.clamp(current - first, -128, 127);
    const tx = @divTrunc(16384 + @as(i32, @intCast(@abs(@divTrunc(td, 2)))), td);
    return std.math.clamp((tb * tx + 32) >> 6, -1024, 1023);
}
pub fn implicit(a: u8, b: u8, current: i32, first: i32, second: i32) u8 {
    if (first == second) return @intCast((@as(u16, a) + b + 1) / 2);
    const scale = distance(current, first, second) >> 2;
    const wb = if (scale < -64 or scale > 128) @as(i32, 32) else scale;
    return @intCast(std.math.clamp((@as(i32, a) * (64 - wb) + @as(i32, b) * wb + 32) >> 6, 0, 255));
}
