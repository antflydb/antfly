// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const h264 = @import("h264.zig");
const Bits = @import("h264_bits.zig").Bits;
const Cabac = @import("h264_cabac.zig").Decoder;
pub const Meta = struct { transform8: bool = false, direct: bool = false, kind: u8 = 255, chroma: u8 = 0, cbp: u8 = 0, dc: u8 = 0 };
pub const Syntax = struct {
    bits: *Bits,
    cabac: ?Cabac,
    meta: []Meta,
    counts: [3][]u8,
    width: usize,
    mb: usize = 0,
    previous_delta: i32 = 0,
    slice_type: u32 = 2,
    pub fn init(bits: *Bits, use_cabac: bool, qp: i32, slice_type: u32, init_idc: usize, meta: []Meta, counts: [3][]u8, width: usize) !Syntax {
        return .{ .bits = bits, .cabac = if (use_cabac) try Cabac.init(bits, qp, if (slice_type == 2) 0 else init_idc + 1) else null, .meta = meta, .counts = counts, .width = width, .slice_type = slice_type };
    }
    fn neighbor(self: *Syntax, top: bool) ?Meta {
        const mw = self.width / 16;
        if (if (top) self.mb < mw else self.mb % mw == 0) return null;
        return self.meta[self.mb - (if (top) mw else 1)];
    }
    pub fn kind(self: *Syntax) !u32 {
        const c = if (self.cabac) |*decoder| decoder else return self.bits.ue();
        if (self.slice_type == 0) {
            if (try c.bin(14) != 0) {
                if (try c.bin(17) == 0) return 5;
                if (try c.terminate()) return error.UnsupportedVideoProfile;
                var result: u32 = 6 + 12 * try c.bin(18);
                if (try c.bin(19) != 0) result += 4 + 4 * try c.bin(19);
                result += 2 * try c.bin(20) + try c.bin(20);
                return result;
            }
            if (try c.bin(15) != 0) return if (try c.bin(17) != 0) 1 else 2;
            return if (try c.bin(16) != 0) 3 else 0;
        }
        if (self.slice_type == 1) {
            var context: usize = 27;
            if (self.neighbor(false)) |m| context += @intFromBool(m.kind != 27 and !m.direct);
            if (self.neighbor(true)) |m| context += @intFromBool(m.kind != 27 and !m.direct);
            if (try c.bin(context) == 0) return 0;
            if (try c.bin(30) == 0) return 1 + try c.bin(32);
            var code: u32 = try c.bin(31) << 3;
            code |= try c.bin(32) << 2;
            code |= try c.bin(32) << 1;
            code |= try c.bin(32);
            if (code < 8) return code + 3;
            if (code == 14) return 11;
            if (code == 15) return 22;
            if (code != 13) return (code << 1 | try c.bin(32)) - 4;
            if (try c.bin(32) == 0) return 23;
            if (try c.terminate()) return error.UnsupportedVideoProfile;
            var result: u32 = 24 + 12 * try c.bin(33);
            if (try c.bin(34) != 0) result += 4 + 4 * try c.bin(34);
            result += 2 * try c.bin(35) + try c.bin(35);
            return result;
        }
        var context: usize = 3;
        if (self.neighbor(false)) |m| context += @intFromBool(m.kind != 0);
        if (self.neighbor(true)) |m| context += @intFromBool(m.kind != 0);
        if (try c.bin(context) == 0) return 0;
        if (try c.terminate()) return error.UnsupportedVideoProfile; // CABAC PCM restart is not yet qualified.
        var result: u32 = 1 + 12 * try c.bin(6);
        if (try c.bin(7) != 0) result += 4 + 4 * try c.bin(8);
        result += 2 * try c.bin(9) + try c.bin(10);
        return result;
    }
    pub fn mode(self: *Syntax, predicted: u8) !u8 {
        if (self.cabac) |*c| {
            if (try c.bin(68) != 0) return predicted;
            var value: u8 = 0;
            for (0..3) |i| value |= @as(u8, @intCast(try c.bin(69))) << @as(u3, @intCast(i));
            return if (value < predicted) value else value + 1;
        }
        if (try self.bits.read(1) != 0) return predicted;
        const value: u8 = @intCast(try self.bits.read(3));
        return if (value < predicted) value else value + 1;
    }
    pub fn chroma(self: *Syntax) !u32 {
        const c = if (self.cabac) |*decoder| decoder else return self.bits.ue();
        var context: usize = 64;
        if (self.neighbor(false)) |m| context += @intFromBool(m.kind != 25 and m.chroma != 0);
        if (self.neighbor(true)) |m| context += @intFromBool(m.kind != 25 and m.chroma != 0);
        if (try c.bin(context) == 0) return 0;
        if (try c.bin(67) == 0) return 1;
        return 2 + try c.bin(67);
    }
    pub fn cbp(self: *Syntax, intra: bool) !u32 {
        const c = if (self.cabac) |*decoder| decoder else {
            const code = try self.bits.ue();
            const mapped = [_]u8{ 47, 31, 15, 0, 23, 27, 29, 30, 7, 11, 13, 14, 39, 43, 45, 46, 16, 3, 5, 10, 12, 19, 21, 26, 28, 35, 37, 42, 44, 1, 2, 4, 8, 17, 18, 20, 24, 6, 9, 22, 25, 32, 33, 34, 36, 40, 38, 41 };
            if (code >= mapped.len) return error.MalformedVideoPacket;
            const inter = [_]u8{ 0, 16, 1, 2, 4, 8, 32, 3, 5, 10, 12, 15, 47, 7, 11, 13, 14, 6, 9, 31, 35, 37, 42, 44, 33, 34, 36, 40, 39, 43, 45, 46, 17, 18, 20, 24, 19, 21, 26, 28, 23, 27, 29, 30, 22, 25, 38, 41 };
            return if (intra) mapped[code] else inter[code];
        };
        const left = self.neighbor(false);
        const top = self.neighbor(true);
        var result: u32 = 0;
        for (0..4) |i| {
            const x = i % 2;
            const y = i / 2;
            const a: u32 = if (x == 0) @intFromBool(if (left) |m| m.kind != 25 and m.cbp & (@as(u8, 1) << @as(u3, @intCast(y * 2 + 1))) == 0 else false) else @intFromBool(result & (@as(u32, 1) << @as(u5, @intCast(i - 1))) == 0);
            const b: u32 = if (y == 0) @intFromBool(if (top) |m| m.kind != 25 and m.cbp & (@as(u8, 1) << @as(u3, @intCast(x + 2))) == 0 else false) else @intFromBool(result & (@as(u32, 1) << @as(u5, @intCast(i - 2))) == 0);
            result |= try c.bin(73 + a + 2 * b) << @as(u5, @intCast(i));
        }
        const a: u32 = @intFromBool(if (left) |m| m.kind == 25 or m.cbp >> 4 != 0 else false);
        const b: u32 = @intFromBool(if (top) |m| m.kind == 25 or m.cbp >> 4 != 0 else false);
        if (try c.bin(77 + a + 2 * b) != 0) {
            const a2: u32 = @intFromBool(if (left) |m| m.kind == 25 or m.cbp >> 4 == 2 else false);
            const b2: u32 = @intFromBool(if (top) |m| m.kind == 25 or m.cbp >> 4 == 2 else false);
            result |= (1 + try c.bin(81 + a2 + 2 * b2)) << 4;
        }
        return result;
    }
    pub fn delta(self: *Syntax) !i32 {
        const c = if (self.cabac) |*decoder| decoder else return self.bits.se();
        var code: u32 = 0;
        if (try c.bin(60 + @as(usize, @intFromBool(self.previous_delta != 0))) != 0) {
            code = 1;
            while (try c.bin(if (code == 1) 62 else 63) != 0) {
                code += 1;
                if (code > 52) return error.MalformedVideoPacket;
            }
        }
        const magnitude: i32 = @intCast((code + 1) / 2);
        self.previous_delta = if (code & 1 == 0) -magnitude else magnitude;
        return self.previous_delta;
    }
    /// Category: 0 luma DC, 1 I16 AC, 2 luma4, 3 chroma DC, 4 chroma AC.
    pub fn coeff(self: *Syntax, nc: usize, max: usize, category: usize, plane: usize, x: usize, y: usize) !h264.Residual {
        const c = if (self.cabac) |*decoder| decoder else return h264.cavlcResidual(self.bits, nc, max, category == 3);
        const is_dc = category == 0 or category == 3;
        var a: u32 = @intFromBool(self.meta[self.mb].kind <= 25);
        var b = a;
        if (is_dc) {
            const mask: u8 = @as(u8, 1) << @as(u3, @intCast(plane));
            if (self.neighbor(false)) |m| a = @intFromBool(m.kind == 25 or m.dc & mask != 0);
            if (self.neighbor(true)) |m| b = @intFromBool(m.kind == 25 or m.dc & mask != 0);
        } else {
            const stride = self.width / (if (plane == 0) @as(usize, 4) else 8);
            if (x != 0) a = @intFromBool(self.counts[plane][y * stride + x - 1] != 0);
            if (y != 0) b = @intFromBool(self.counts[plane][(y - 1) * stride + x] != 0);
        }
        if (try c.bin(85 + 4 * category + a + 2 * b) == 0) return .{ .total = 0 };
        if (is_dc) self.meta[self.mb].dc |= @as(u8, 1) << @as(u3, @intCast(plane));
        const sig = [_]usize{ 105, 120, 134, 149, 152 };
        const last = [_]usize{ 166, 181, 195, 210, 213 };
        const level = [_]usize{ 227, 237, 247, 257, 266 };
        var result = h264.Residual{ .total = 0 };
        var positions: [16]usize = undefined;
        for (0..max) |i| {
            if (i == max - 1 or try c.bin(sig[category] + i) != 0) {
                positions[result.total] = i;
                result.total += 1;
                if (i == max - 1 or try c.bin(last[category] + i) != 0) break;
            }
        }
        var c1: usize = 1;
        var c2: usize = 0;
        var i: usize = result.total;
        while (i != 0) {
            i -= 1;
            var value: i32 = 1;
            if (try c.bin(level[category] + c1) != 0) {
                value = 2 + try c.level(level[category] + 5 + c2);
                c2 = @min(c2 + 1, if (category == 3) @as(usize, 3) else 4);
                c1 = 0;
            } else if (c1 != 0) c1 = @min(c1 + 1, 4);
            if (try c.bypass() != 0) value = -value;
            result.values[positions[i]] = value;
        }
        return result;
    }
    pub fn skip(self: *Syntax) !bool {
        const c = if (self.cabac) |*decoder| decoder else unreachable;
        var context: usize = if (self.slice_type == 1) 24 else 11;
        if (self.neighbor(false)) |m| context += @intFromBool(m.kind != 27);
        if (self.neighbor(true)) |m| context += @intFromBool(m.kind != 27);
        return try c.bin(context) != 0;
    }
    pub fn subkind(self: *Syntax) !u32 {
        if (self.cabac) |*c| {
            if (self.slice_type == 1) {
                if (try c.bin(36) == 0) return 0;
                if (try c.bin(37) == 0) return 1 + try c.bin(39);
                var result: u32 = 3;
                if (try c.bin(38) != 0) {
                    if (try c.bin(39) != 0) return 11 + try c.bin(39);
                    result += 4;
                }
                return result + 2 * try c.bin(39) + try c.bin(39);
            }
            if (try c.bin(21) != 0) return 0;
            if (try c.bin(22) == 0) return 1;
            return 3 - try c.bin(23);
        }
        const value = try self.bits.ue();
        if (value > (if (self.slice_type == 1) @as(u32, 12) else 3)) return error.MalformedVideoPacket;
        return value;
    }
    pub fn reference(self: *Syntax, active: usize, left: i8, top: i8) !i8 {
        if (active == 1) return 0;
        var value: u32 = 0;
        if (self.cabac) |*c| {
            if (try c.bin(54 + @as(usize, @intFromBool(left > 0)) + 2 * @as(usize, @intFromBool(top > 0))) != 0) {
                value = 1;
                while (try c.bin(if (value == 1) 58 else 59) != 0) {
                    value += 1;
                    if (value >= active) return error.MalformedVideoPacket;
                }
            }
        } else value = if (active == 2) 1 - try self.bits.read(1) else try self.bits.ue();
        if (value >= active) return error.MalformedVideoPacket;
        return @intCast(value);
    }
    pub fn mvd(self: *Syntax, component: usize, neighbor_sum: u32) !i32 {
        if (self.cabac) |*c| {
            const base: usize = 40 + 7 * component;
            const context: usize = if (neighbor_sum < 3) @as(usize, 0) else if (neighbor_sum <= 32) 1 else 2;
            if (try c.bin(base + context) == 0) return 0;
            var value: u32 = 1;
            while (value < 9 and try c.bin(base + @min(3 + @as(usize, value - 1), 6)) != 0) value += 1;
            if (value == 9) {
                var k: usize = 3;
                while (try c.bypass() != 0) {
                    if (k >= 15) return error.MalformedVideoPacket;
                    value += @as(u32, 1) << @as(u5, @intCast(k));
                    k += 1;
                }
                var suffix: u32 = 0;
                for (0..k) |_| suffix = (suffix << 1) | try c.bypass();
                value += suffix;
            }
            if (value > 32767) return error.MalformedVideoPacket;
            return if (try c.bypass() != 0) -@as(i32, @intCast(value)) else @intCast(value);
        }
        const value = try self.bits.se();
        if (value < -32767 or value > 32767) return error.MalformedVideoPacket;
        return value;
    }
    pub fn transform8(self: *Syntax) !bool {
        if (self.cabac) |*c| {
            var context: usize = 399;
            if (self.neighbor(false)) |m| context += @intFromBool(m.transform8);
            if (self.neighbor(true)) |m| context += @intFromBool(m.transform8);
            return try c.bin(context) != 0;
        }
        return try self.bits.read(1) != 0;
    }
    pub fn coeff8(self: *Syntax, x: usize, y: usize) ![64]i32 {
        var values: [64]i32 = @splat(0);
        const scan = @import("h264_transform8.zig").scan;
        if (self.cabac) |*c| {
            const sig = [_]usize{ 0, 1, 2, 3, 4, 5, 5, 4, 4, 3, 3, 4, 4, 4, 5, 5, 4, 4, 4, 4, 3, 3, 6, 7, 7, 7, 8, 9, 10, 9, 8, 7, 7, 6, 11, 12, 13, 11, 6, 7, 8, 9, 14, 10, 9, 8, 6, 11, 12, 13, 11, 6, 9, 14, 10, 9, 11, 12, 13, 11, 14, 10, 12 };
            var positions: [64]usize = undefined;
            var total: usize = 0;
            for (0..64) |i| {
                if (i == 63 or try c.bin(402 + sig[i]) != 0) {
                    positions[total] = i;
                    total += 1;
                    const last: usize = if (i == 0) 0 else if (i < 16) 1 else if (i < 32) 2 else if (i < 40) 3 else if (i < 48) 4 else if (i < 52) 5 else if (i < 56) 6 else if (i < 60) 7 else 8;
                    if (i == 63 or try c.bin(417 + last) != 0) break;
                }
            }
            var c1: usize = 1;
            var c2: usize = 0;
            var i = total;
            while (i != 0) {
                i -= 1;
                var value: i32 = 1;
                if (try c.bin(426 + c1) != 0) {
                    value = 2 + try c.level(431 + c2);
                    c2 = @min(c2 + 1, 4);
                    c1 = 0;
                } else if (c1 != 0) c1 = @min(c1 + 1, 4);
                if (try c.bypass() != 0) value = -value;
                values[scan[positions[i]]] = value;
            }
            for (0..2) |row| @memset(self.counts[0][(y + row) * (self.width / 4) + x ..][0..2], 1);
        } else {
            for (0..4) |i| {
                const bx = x + i % 2;
                const by = y + i / 2;
                const stride = self.width / 4;
                const nc: usize = if (bx == 0 and by == 0) 0 else if (bx == 0) self.counts[0][(by - 1) * stride + bx] else if (by == 0) self.counts[0][by * stride + bx - 1] else (@as(usize, self.counts[0][(by - 1) * stride + bx]) + self.counts[0][by * stride + bx - 1] + 1) / 2;
                const block = try h264.cavlcResidual(self.bits, nc, 16, false);
                self.counts[0][by * stride + bx] = block.total;
                for (0..16) |j| values[scan[4 * j + i]] = block.values[j];
            }
        }
        return values;
    }
    pub fn endMb(self: *Syntax, last_mb: bool) !void {
        if (self.cabac) |*c| if (try c.terminate() != last_mb) return error.MalformedVideoPacket;
    }
    pub fn finish(self: *Syntax) !void {
        if (self.cabac == null) try self.bits.finish();
    }
};
