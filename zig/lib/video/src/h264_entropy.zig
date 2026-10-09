// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const layout = @import("h264_layout.zig");
const h264 = @import("h264.zig");
const Bits = @import("h264_bits.zig").Bits;
const Cabac = @import("h264_cabac.zig").Decoder;
pub const Meta = struct { si: bool = false, switching_slice: bool = false, field: bool = false, slice_id: usize = std.math.maxInt(usize), filter: u2 = 0, alpha: i8 = 0, beta: i8 = 0, transform8: bool = false, direct: bool = false, kind: u8 = 255, chroma: u8 = 0, cbp: u8 = 0, dc: u8 = 0 };
pub const Syntax = struct {
    bits: *Bits,
    partitioned: bool = false,
    intra_bits: ?*Bits = null,
    inter_bits: ?*Bits = null,
    cabac: ?Cabac,
    meta: []Meta,
    counts: [3][]u8,
    width: usize,
    mb: usize = 0,
    x: usize = 0,
    y: usize = 0,
    paired: bool = false,
    mbaff: bool = false,
    field: bool = false,
    parity: usize = 0,
    address: usize = 0,
    slice_id: usize = 0,
    constrained: bool = false,
    previous_delta: i32 = 0,
    slice_type: u32 = 2,
    chroma_format: u8 = 1,
    bit_depth: u8 = 8,
    pub fn init(bits: *Bits, use_cabac: bool, qp: i32, slice_type: u32, init_idc: usize, meta: []Meta, counts: [3][]u8, width: usize) !Syntax {
        return .{ .bits = bits, .cabac = if (use_cabac) try Cabac.init(bits, qp, if (slice_type == 2) 0 else init_idc + 1) else null, .meta = meta, .counts = counts, .width = width, .slice_type = slice_type };
    }
    pub fn residualBits(self: *Syntax) !*Bits {
        if (!self.partitioned) return self.bits;
        return (if (self.meta[self.mb].kind <= 25) self.intra_bits else self.inter_bits) orelse error.MissingVideoPartition;
    }
    pub fn location(self: *Syntax, plane: usize, x: i32, y: i32) ?layout.Cell {
        return layout.cellSample(self.meta, self.width, self.chroma_format, self.paired, self.field, self.parity, plane, x * 4, y * 4 + (if (y < @as(i32, @intCast(self.y / (if (plane != 0 and self.chroma_format == 1) @as(usize, 2) else 1)))) @as(i32, 3) else 0));
    }
    pub fn cellIndex(self: *Syntax, plane: usize, x: usize, y: usize) usize {
        return self.location(plane, @intCast(x), @intCast(y)).?.index;
    }
    fn neighbor(self: *Syntax, top: bool) ?Meta {
        const point = self.location(0, @as(i32, @intCast(self.x)) - @as(i32, @intFromBool(!top)), @as(i32, @intCast(self.y)) - @as(i32, @intFromBool(top))) orelse return null;
        const m = self.meta[point.mb];
        return if (m.slice_id == self.slice_id and m.kind != 255) m else null;
    }
    pub fn available(self: *Syntax, plane: usize, x: i32, y: i32, intra: bool) bool {
        const point = self.location(plane, x, y) orelse return false;
        const m = self.meta[point.mb];
        return m.kind != 255 and m.slice_id == self.slice_id and (!intra or !self.constrained or (m.kind <= 25 and (!m.si or self.meta[self.mb].si)));
    }
    pub fn coefficientContext(self: *Syntax, plane: usize, x: usize, y: usize) usize {
        const left = self.available(plane, @as(i32, @intCast(x)) - 1, @intCast(y), false);
        const top = self.available(plane, @intCast(x), @as(i32, @intCast(y)) - 1, false);
        const a = if (left) self.counts[plane][self.location(plane, @as(i32, @intCast(x)) - 1, @intCast(y)).?.index] else @as(u8, 0);
        const b = if (top) self.counts[plane][self.location(plane, @intCast(x), @as(i32, @intCast(y)) - 1).?.index] else @as(u8, 0);
        return if (left and top) (@as(usize, a) + b + 1) / 2 else if (left) a else b;
    }
    pub fn fieldFlag(self: *Syntax) !bool {
        if (self.cabac) |*c| {
            const mw = self.width / 16;
            const top = self.mb - self.parity * mw;
            var context: usize = 70;
            if (top % mw != 0 and self.meta[top - 1].slice_id == self.slice_id) context += @intFromBool(self.meta[top - 1].field);
            if (top >= 2 * mw and self.meta[top - 2 * mw].slice_id == self.slice_id) context += @intFromBool(self.meta[top - 2 * mw].field);
            return try c.bin(context) != 0;
        }
        return try self.bits.read(1) != 0;
    }
    pub fn kind(self: *Syntax) !u32 {
        const c = if (self.cabac) |*decoder| decoder else return self.bits.ue();
        if (self.slice_type == 0) {
            if (try c.bin(14) != 0) {
                if (try c.bin(17) == 0) return 5;
                if (try c.terminate()) return if (self.slice_type == 0) 30 else 48;
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
            if (try c.terminate()) return if (self.slice_type == 0) 30 else 48;
            var result: u32 = 24 + 12 * try c.bin(33);
            if (try c.bin(34) != 0) result += 4 + 4 * try c.bin(34);
            result += 2 * try c.bin(35) + try c.bin(35);
            return result;
        }
        var context: usize = 3;
        if (self.neighbor(false)) |m| context += @intFromBool(m.kind != 0);
        if (self.neighbor(true)) |m| context += @intFromBool(m.kind != 0);
        if (try c.bin(context) == 0) return 0;
        if (try c.terminate()) return 25;
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
            if (self.chroma_format == 0 or self.chroma_format == 3) {
                const intra_map = [_]u8{ 15, 0, 7, 11, 13, 14, 3, 5, 10, 12, 1, 2, 4, 8, 6, 9 };
                const inter_map = [_]u8{ 0, 1, 2, 4, 8, 3, 5, 10, 12, 15, 7, 11, 13, 14, 6, 9 };
                if (code >= 16) return error.MalformedVideoPacket;
                return if (intra) intra_map[code] else inter_map[code];
            }
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
            const bx: i32 = @intCast(self.x + x * 2);
            const by: i32 = @intCast(self.y + y * 2);
            const a = self.cbpNeighbor(bx - 1, by, false, result);
            const b = self.cbpNeighbor(bx, by - 1, true, result);
            result |= try c.bin(73 + a + 2 * b) << @as(u5, @intCast(i));
        }
        if (self.chroma_format == 0 or self.chroma_format == 3) return result;
        const a: u32 = @intFromBool(if (left) |m| m.kind == 25 or m.cbp >> 4 != 0 else false);
        const b: u32 = @intFromBool(if (top) |m| m.kind == 25 or m.cbp >> 4 != 0 else false);
        if (try c.bin(77 + a + 2 * b) != 0) {
            const a2: u32 = @intFromBool(if (left) |m| m.kind == 25 or m.cbp >> 4 == 2 else false);
            const b2: u32 = @intFromBool(if (top) |m| m.kind == 25 or m.cbp >> 4 == 2 else false);
            result |= (1 + try c.bin(81 + a2 + 2 * b2)) << 4;
        }
        return result;
    }
    fn cbpNeighbor(self: *Syntax, x: i32, y: i32, top: bool, current: u32) u32 {
        const point = layout.cellSample(self.meta, self.width, self.chroma_format, self.paired, self.field, self.parity, 0, x * 4, y * 4 + (if (top) @as(i32, 3) else 0)) orelse return 0;
        const m = self.meta[point.mb];
        if (m.kind == 255 or m.slice_id != self.slice_id or m.kind == 25) return 0;
        const stride = self.width / 4;
        const block = (point.index % stride % 4) / 2 + (point.index / stride % 4) / 2 * 2;
        const pattern = if (point.mb == self.mb) current else m.cbp;
        return @intFromBool(pattern & (@as(u32, 1) << @as(u5, @intCast(block))) == 0);
    }
    pub fn delta(self: *Syntax) !i32 {
        const c = if (self.cabac) |*decoder| decoder else return self.bits.se();
        var code: u32 = 0;
        if (try c.bin(60 + @as(usize, @intFromBool(self.previous_delta != 0))) != 0) {
            code = 1;
            while (try c.bin(if (code == 1) 62 else 63) != 0) {
                code += 1;
                if (code > 88) return error.MalformedVideoPacket;
            }
        }
        const magnitude: i32 = @intCast((code + 1) / 2);
        self.previous_delta = if (code & 1 == 0) -magnitude else magnitude;
        return self.previous_delta;
    }
    /// Category: 0 luma DC, 1 I16 AC, 2 luma4, 3 chroma DC, 4 chroma AC.
    pub fn coeff(self: *Syntax, nc: usize, max: usize, category: usize, plane: usize, x: usize, y: usize) !h264.Residual {
        const c = if (self.cabac) |*decoder| decoder else {
            const result = try h264.cavlcResidual(try self.residualBits(), nc, max, category == 3);
            try self.validateLevels(&result.values);
            return result;
        };
        const is_dc = category == 0 or category == 3 or category == 6 or category == 10;
        var a: u32 = @intFromBool(self.meta[self.mb].kind <= 25);
        var b = a;
        if (is_dc) {
            const mask: u8 = @as(u8, 1) << @as(u3, @intCast(plane));
            if (self.neighbor(false)) |m| a = @intFromBool(m.kind == 25 or m.dc & mask != 0);
            if (self.neighbor(true)) |m| b = @intFromBool(m.kind == 25 or m.dc & mask != 0);
        } else {
            if (self.available(plane, @as(i32, @intCast(x)) - 1, @intCast(y), false)) a = @intFromBool(self.counts[plane][self.location(plane, @as(i32, @intCast(x)) - 1, @intCast(y)).?.index] != 0);
            if (self.available(plane, @intCast(x), @as(i32, @intCast(y)) - 1, false)) b = @intFromBool(self.counts[plane][self.location(plane, @intCast(x), @as(i32, @intCast(y)) - 1).?.index] != 0);
        }
        if (try c.bin((if (category <= 4) 85 + 4 * category else if (category <= 8) 460 + 4 * (category - 6) else 472 + 4 * (category - 10)) + a + 2 * b) == 0) return .{ .total = 0 };
        if (is_dc) self.meta[self.mb].dc |= @as(u8, 1) << @as(u3, @intCast(plane));
        const sig = if (self.field) [_]usize{ 277, 292, 306, 321, 324, 436, 776, 791, 805, 675, 820, 835, 849, 733 } else [_]usize{ 105, 120, 134, 149, 152, 402, 484, 499, 513, 660, 528, 543, 557, 718 };
        const last = if (self.field) [_]usize{ 338, 353, 367, 382, 385, 451, 864, 879, 893, 699, 908, 923, 937, 757 } else [_]usize{ 166, 181, 195, 210, 213, 417, 572, 587, 601, 690, 616, 631, 645, 748 };
        const level = [_]usize{ 227, 237, 247, 257, 266, 426, 952, 962, 972, 708, 982, 992, 1002, 766 };
        var result = h264.Residual{ .total = 0 };
        var positions: [16]usize = undefined;
        for (0..max) |i| {
            if (i == max - 1 or try c.bin(sig[category] + (if (category == 3 and max == 8) @min(i / 2, 2) else i)) != 0) {
                positions[result.total] = i;
                result.total += 1;
                if (i == max - 1 or try c.bin(last[category] + (if (category == 3 and max == 8) @min(i / 2, 2) else i)) != 0) break;
            }
        }
        var c1: usize = 1;
        var c2: usize = 0;
        var i: usize = result.total;
        while (i != 0) {
            i -= 1;
            var value: i32 = 1;
            if (try c.bin(level[category] + c1) != 0) {
                value = 2 + try c.level(level[category] + 5 + @min(c2, if (category == 3 and max == 8) @as(usize, 3) else 4));
                c2 = @min(c2 + 1, if (category == 3) @as(usize, 3) else 4);
                c1 = 0;
            } else if (c1 != 0) c1 = @min(c1 + 1, 4);
            if (try c.bypass() != 0) value = -value;
            result.values[positions[i]] = value;
        }
        try self.validateLevels(&result.values);
        return result;
    }
    fn validateLevels(self: *Syntax, values: []const i32) !void {
        const bound = @as(i32, 1) << @as(u5, @intCast(7 + self.bit_depth));
        for (values) |v| if (v < -bound or v >= bound) return error.MalformedVideoPacket;
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
    pub fn coeff8(self: *Syntax, plane: usize, x: usize, y: usize) ![64]i32 {
        var values: [64]i32 = @splat(0);
        const scan = if (self.field) @import("h264_transform8.zig").field_scan else @import("h264_transform8.zig").scan;
        if (self.cabac) |*c| {
            if (self.chroma_format == 3) {
                var flags: [2]u32 = @splat(@intFromBool(self.meta[self.mb].kind <= 25));
                for (0..2) |side| {
                    const nx: i32 = @as(i32, @intCast(x)) - (if (side == 0) @as(i32, 1) else 0);
                    const ny: i32 = @as(i32, @intCast(y)) - (if (side == 1) @as(i32, 1) else 0);
                    if (self.available(plane, nx, ny, false)) {
                        const point = self.location(plane, nx, ny).?;
                        const m = self.meta[point.mb];
                        flags[side] = @intFromBool(m.kind == 25 or (m.transform8 and self.counts[plane][point.index] != 0));
                    }
                }
                if (try c.bin(1012 + 4 * plane + flags[0] + 2 * flags[1]) == 0) return values;
            }
            const sig_base: usize = if (plane == 0) (if (self.field) @as(usize, 436) else 402) else if (plane == 1) (if (self.field) @as(usize, 675) else 660) else (if (self.field) @as(usize, 733) else 718);
            const last_base: usize = if (plane == 0) (if (self.field) @as(usize, 451) else 417) else if (plane == 1) (if (self.field) @as(usize, 699) else 690) else (if (self.field) @as(usize, 757) else 748);
            const level_base: usize = if (plane == 0) 426 else if (plane == 1) 708 else 766;
            const sig = if (self.field) @import("h264_transform8.zig").field_significant else [_]usize{ 0, 1, 2, 3, 4, 5, 5, 4, 4, 3, 3, 4, 4, 4, 5, 5, 4, 4, 4, 4, 3, 3, 6, 7, 7, 7, 8, 9, 10, 9, 8, 7, 7, 6, 11, 12, 13, 11, 6, 7, 8, 9, 14, 10, 9, 8, 6, 11, 12, 13, 11, 6, 9, 14, 10, 9, 11, 12, 13, 11, 14, 10, 12 };
            var positions: [64]usize = undefined;
            var total: usize = 0;
            for (0..64) |i| {
                if (i == 63 or try c.bin(sig_base + sig[i]) != 0) {
                    positions[total] = i;
                    total += 1;
                    const last: usize = if (i == 0) 0 else if (i < 16) 1 else if (i < 32) 2 else if (i < 40) 3 else if (i < 48) 4 else if (i < 52) 5 else if (i < 56) 6 else if (i < 60) 7 else 8;
                    if (i == 63 or try c.bin(last_base + last) != 0) break;
                }
            }
            var c1: usize = 1;
            var c2: usize = 0;
            var i = total;
            while (i != 0) {
                i -= 1;
                var value: i32 = 1;
                if (try c.bin(level_base + c1) != 0) {
                    value = 2 + try c.level(level_base + 5 + c2);
                    c2 = @min(c2 + 1, 4);
                    c1 = 0;
                } else if (c1 != 0) c1 = @min(c1 + 1, 4);
                if (try c.bypass() != 0) value = -value;
                values[scan[positions[i]]] = value;
            }
            for (0..2) |row| for (0..2) |col| {
                self.counts[plane][self.cellIndex(plane, x + col, y + row)] = 1;
            };
        } else {
            for (0..4) |i| {
                const bx = x + i % 2;
                const by = y + i / 2;
                const nc = self.coefficientContext(plane, bx, by);
                const block = try h264.cavlcResidual(try self.residualBits(), nc, 16, false);
                self.counts[plane][self.cellIndex(plane, bx, by)] = block.total;
                for (0..16) |j| values[scan[4 * j + i]] = block.values[j];
            }
        }
        try self.validateLevels(&values);
        return values;
    }
    pub fn endMb(self: *Syntax, remaining_skips: usize) !bool {
        if (self.mbaff and self.address % 2 == 0) return false;
        if (self.cabac) |*c| return c.terminate();
        return remaining_skips == 0 and self.bits.position == self.bits.end;
    }
    pub fn finish(self: *Syntax) !void {
        if (self.cabac == null) try self.bits.finish();
    }
};
