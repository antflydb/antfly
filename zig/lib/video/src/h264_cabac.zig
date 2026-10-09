// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded CABAC arithmetic engine, H.264 clause 9.3.3.
const std = @import("std");
const Bits = @import("h264_bits.zig").Bits;
const tables = @import("h264_cabac_tables.zig");
pub const Decoder = struct {
    bits: *Bits,
    range: u32 = 510,
    offset: u32,
    state: [460]u8,
    pub fn init(bits: *Bits, qp: i32, init_idc: usize) !Decoder {
        if (init_idc > 3) return error.MalformedVideoPacket;
        while (bits.position % 8 != 0) if (try bits.read(1) != 1) return error.MalformedVideoPacket;
        var decoder = Decoder{ .bits = bits, .offset = 0, .state = undefined };
        for (0..460) |i| {
            const params = tables.initial[i][init_idc];
            const pre = std.math.clamp((@as(i32, params[0]) * qp >> 4) + params[1], 1, 126);
            decoder.state[i] = @intCast(if (pre <= 63) (63 - pre) * 2 else (pre - 64) * 2 + 1);
        }
        for (0..9) |_| decoder.offset = (decoder.offset << 1) | try decoder.bit();
        if (decoder.offset >= 510) return error.MalformedVideoPacket;
        return decoder;
    }
    fn bit(self: *Decoder) !u32 {
        if (self.bits.position >= self.bits.bytes.len * 8) return error.MalformedVideoPacket;
        const position = self.bits.position;
        self.bits.position += 1;
        return (self.bits.bytes[position / 8] >> @as(u3, @intCast(7 - position % 8))) & 1;
    }
    fn normalize(self: *Decoder) !void {
        while (self.range < 256) {
            self.range <<= 1;
            self.offset = (self.offset << 1) | try self.bit();
        }
    }
    pub fn bin(self: *Decoder, context: usize) !u32 {
        if (context >= self.state.len) return error.MalformedVideoPacket;
        const encoded = self.state[context];
        const state: usize = encoded >> 1;
        const mps: u32 = encoded & 1;
        const lps = tables.lps[state][(self.range >> 6) & 3];
        self.range -= lps;
        var value = mps;
        if (self.offset >= self.range) {
            self.offset -= self.range;
            self.range = lps;
            value ^= 1;
            self.state[context] = tables.transition[state][0] * 2 + @as(u8, @intCast(if (state == 0) mps ^ 1 else mps));
        } else self.state[context] = tables.transition[state][1] * 2 + @as(u8, @intCast(mps));
        try self.normalize();
        return value;
    }
    pub fn bypass(self: *Decoder) !u32 {
        self.offset = (self.offset << 1) | try self.bit();
        if (self.offset >= self.range) {
            self.offset -= self.range;
            return 1;
        }
        return 0;
    }
    pub fn terminate(self: *Decoder) !bool {
        self.range -= 2;
        if (self.offset >= self.range) return true;
        try self.normalize();
        return false;
    }
    pub fn level(self: *Decoder, context: usize) !i32 {
        var extra: u32 = 0;
        while (extra < 13 and try self.bin(context) != 0) extra += 1;
        if (extra == 13) {
            var k: usize = 0;
            while (try self.bypass() != 0) {
                if (k >= 14) return error.MalformedVideoPacket;
                extra += @as(u32, 1) << @as(u5, @intCast(k));
                k += 1;
            }
            var suffix: u32 = 0;
            for (0..k) |_| suffix = (suffix << 1) | try self.bypass();
            extra += suffix;
        }
        if (extra > 32766) return error.MalformedVideoPacket;
        return @intCast(extra);
    }
};
