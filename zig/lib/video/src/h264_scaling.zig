// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! H.264 7.4.2.1/7.4.2.2 scaling-list syntax and fallback rules for all supported chroma formats.
const Bits = @import("h264_bits.zig").Bits;
const scan4 = [_]usize{ 0, 1, 4, 8, 5, 2, 3, 6, 9, 12, 13, 10, 7, 11, 14, 15 };
const scan8 = @import("h264_transform8.zig").scan;
const default4 = [2][16]u8{ .{ 6, 13, 13, 20, 20, 20, 28, 28, 28, 28, 32, 32, 32, 37, 37, 42 }, .{ 10, 14, 14, 20, 20, 20, 24, 24, 24, 24, 27, 27, 27, 30, 30, 34 } };
const default8 = [2][64]u8{ .{ 6, 10, 10, 13, 11, 13, 16, 16, 16, 16, 18, 18, 18, 18, 18, 23, 23, 23, 23, 23, 23, 25, 25, 25, 25, 25, 25, 25, 27, 27, 27, 27, 27, 27, 27, 27, 29, 29, 29, 29, 29, 29, 29, 31, 31, 31, 31, 31, 31, 33, 33, 33, 33, 33, 36, 36, 36, 36, 38, 38, 38, 40, 40, 42 }, .{ 9, 13, 13, 15, 13, 15, 17, 17, 17, 17, 19, 19, 19, 19, 19, 21, 21, 21, 21, 21, 21, 22, 22, 22, 22, 22, 22, 22, 24, 24, 24, 24, 24, 24, 24, 24, 25, 25, 25, 25, 25, 25, 25, 27, 27, 27, 27, 27, 27, 28, 28, 28, 28, 28, 30, 30, 30, 30, 32, 32, 32, 33, 33, 35 } };
pub const Matrices = struct {
    four: [6][16]u8 = @splat(@splat(16)),
    eight: [6][64]u8 = @splat(@splat(16)),
    sequence_present: bool = false,
    pub fn parseSequence(self: *Matrices, bits: *Bits, chroma_format: u8) !void {
        self.sequence_present = try bits.read(1) != 0;
        if (self.sequence_present) try self.parse(bits, false, true, chroma_format);
    }
    pub fn parsePicture(self: *Matrices, bits: *Bits, transform8: bool, chroma_format: u8) !void {
        if (try bits.read(1) != 0) try self.parse(bits, true, transform8, chroma_format);
    }
    fn parse(self: *Matrices, bits: *Bits, picture: bool, transform8: bool, chroma_format: u8) !void {
        for (0..if (transform8) (if (chroma_format == 3) @as(usize, 12) else 8) else 6) |i| {
            if (i < 6) {
                if (try bits.read(1) != 0) {
                    try readList(bits, &self.four[i], &scan4, &default4[i / 3]);
                } else if (i == 0 or i == 3) {
                    if (!picture or !self.sequence_present) for (scan4, default4[i / 3]) |position, value| {
                        self.four[i][position] = value;
                    };
                } else self.four[i] = self.four[i - 1];
            } else {
                if (try bits.read(1) != 0) {
                    try readList(bits, &self.eight[i - 6], &scan8, &default8[(i - 6) % 2]);
                } else if (i >= 8) {
                    self.eight[i - 6] = self.eight[i - 8];
                } else if (!picture or !self.sequence_present) for (scan8, default8[i - 6]) |position, value| {
                    self.eight[i - 6][position] = value;
                };
            }
        }
    }
};
fn readList(bits: *Bits, out: []u8, scan: []const usize, fallback: []const u8) !void {
    var last: i32 = 8;
    var next: i32 = 8;
    for (scan, 0..) |position, i| {
        if (next != 0) {
            const delta = try bits.se();
            if (delta < -128 or delta > 127) return error.MalformedVideoConfig;
            next = @mod(last + delta, 256);
            if (i == 0 and next == 0) {
                for (scan, fallback) |p, value| out[p] = value;
                return;
            }
        }
        last = if (next == 0) last else next;
        out[position] = @intCast(last);
    }
}
