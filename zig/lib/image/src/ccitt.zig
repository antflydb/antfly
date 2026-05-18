// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const tables = @import("ccitt_tables.zig");

const Allocator = std.mem.Allocator;

pub const Order = enum {
    lsb,
    msb,
};

pub const SubFormat = enum {
    group3,
    group4,
};

pub const Options = struct {
    byte_align: bool = false,
    mixed_2d: bool = false,
};

const mode_pass: u32 = 0;
const mode_h: u32 = 1;
const mode_v0: u32 = 2;
const mode_vr1: u32 = 3;
const mode_vr2: u32 = 4;
const mode_vr3: u32 = 5;
const mode_vl1: u32 = 6;
const mode_vl2: u32 = 7;
const mode_vl3: u32 = 8;

const Decoder = struct {
    input: []const u8,
    bit_pos: usize = 0,
    order: Order,
    sub_format: SubFormat,
    width: usize,
    height: usize,
    curr: []u8,
    prev: []u8,
    wi: usize = 0,
    at_start_of_row: bool = true,
    pen_color_is_white: bool = true,
    byte_align: bool,
    mixed_2d: bool,

    fn decode2dRow(self: *Decoder) !void {
        while (self.wi < self.curr.len) : (self.at_start_of_row = false) {
            const mode = try self.decodeCode(tables.mode_decode_table[0..]);
            switch (mode) {
                mode_pass => try self.readerModePass(),
                mode_h => try self.readerModeH(),
                mode_v0 => try self.readerModeV(0),
                mode_vr1 => try self.readerModeV(1),
                mode_vr2 => try self.readerModeV(2),
                mode_vr3 => try self.readerModeV(3),
                mode_vl1 => try self.readerModeV(-1),
                mode_vl2 => try self.readerModeV(-2),
                mode_vl3 => try self.readerModeV(-3),
                else => return error.UnsupportedCcittMode,
            }
        }
    }

    fn nextBit(self: *Decoder) !u1 {
        if (self.bit_pos >= self.input.len * 8) return error.EndOfStream;
        const byte = self.input[self.bit_pos / 8];
        const shift: u3 = switch (self.order) {
            .msb => @intCast(7 - (self.bit_pos % 8)),
            .lsb => @intCast(self.bit_pos % 8),
        };
        self.bit_pos += 1;
        return @intCast((byte >> shift) & 1);
    }

    fn alignToByteBoundary(self: *Decoder) void {
        self.bit_pos = (self.bit_pos + 7) & ~@as(usize, 7);
    }

    fn decodeCode(self: *Decoder, table: []const [2]i16) !u32 {
        var n_bits_read: u32 = 0;
        var state: i32 = 1;
        while (true) {
            const bit = try self.nextBit();
            n_bits_read += 1;
            state = table[@intCast(state)][bit];
            if (state < 0) return @intCast(~state);
            if (state == 0) {
                self.bit_pos -= n_bits_read;
                return error.InvalidCcittCode;
            }
        }
    }

    fn decodeEol(self: *Decoder) !void {
        var n_bits_read: u32 = 0;
        while (true) {
            const bit = try self.nextBit();
            n_bits_read += 1;
            if (n_bits_read < 12) {
                if (bit == 0) continue;
            } else if (bit == 1) {
                return;
            }
            self.bit_pos -= n_bits_read;
            return error.MissingCcittEol;
        }
    }

    fn penColor(self: *const Decoder) u8 {
        return if (self.pen_color_is_white) 0xff else 0x00;
    }

    fn decodeRun(self: *Decoder) !void {
        const table = if (self.pen_color_is_white) tables.white_decode_table[0..] else tables.black_decode_table[0..];
        var total: usize = 0;
        while (true) {
            const n = try self.decodeCode(table);
            total += n;
            if (total > self.width) return error.CcittRunLengthTooLong;
            if (n <= 0x3f) break;
        }
        if (total > (self.width - self.wi)) return error.CcittRunLengthOverflowsWidth;
        @memset(self.curr[self.wi .. self.wi + total], self.penColor());
        self.wi += total;
        self.pen_color_is_white = !self.pen_color_is_white;
    }

    fn findB(self: *const Decoder, want_b2: bool) usize {
        if (self.prev.len != self.curr.len) return self.curr.len;
        var i = self.wi;
        if (self.at_start_of_row) {
            while (i < self.prev.len and self.prev[i] == 0xff) : (i += 1) {}
            if (want_b2) {
                while (i < self.prev.len and self.prev[i] == 0x00) : (i += 1) {}
            }
            return i;
        }
        const opposite = ~self.penColor();
        while (i < self.prev.len and self.prev[i] == opposite) : (i += 1) {}
        const pen = ~opposite;
        while (i < self.prev.len and self.prev[i] == pen) : (i += 1) {}
        if (want_b2) {
            const opposite2 = ~pen;
            while (i < self.prev.len and self.prev[i] == opposite2) : (i += 1) {}
        }
        return i;
    }

    fn readerModePass(self: *Decoder) !void {
        const b2 = self.findB(true);
        if (b2 < self.wi or b2 > self.curr.len) return error.InvalidCcittOffset;
        @memset(self.curr[self.wi..b2], self.penColor());
        self.wi = b2;
    }

    fn readerModeH(self: *Decoder) !void {
        try self.decodeRun();
        try self.decodeRun();
    }

    fn readerModeV(self: *Decoder, arg: i32) !void {
        const b1: i32 = @intCast(self.findB(false));
        const a1 = b1 + arg;
        if (a1 < @as(i32, @intCast(self.wi)) or a1 > @as(i32, @intCast(self.curr.len))) return error.InvalidCcittOffset;
        @memset(self.curr[self.wi..@intCast(a1)], self.penColor());
        self.wi = @intCast(a1);
        self.pen_color_is_white = !self.pen_color_is_white;
    }

    fn decodeRow(self: *Decoder) !void {
        self.wi = 0;
        self.at_start_of_row = true;
        self.pen_color_is_white = true;
        if (self.byte_align) self.alignToByteBoundary();
        switch (self.sub_format) {
            .group3 => {
                const is_1d = if (self.mixed_2d) (try self.nextBit()) == 1 else true;
                if (is_1d) {
                    while (self.wi < self.curr.len) : (self.at_start_of_row = false) {
                        try self.decodeRun();
                    }
                } else {
                    try self.decode2dRow();
                }
                try self.decodeEol();
            },
            .group4 => {
                try self.decode2dRow();
            },
        }
    }
};

pub fn decodeGrayAlloc(
    alloc: Allocator,
    input: []const u8,
    order: Order,
    sub_format: SubFormat,
    width: usize,
    height: usize,
    opts: Options,
) ![]u8 {
    if (width == 0 or height == 0) return try alloc.alloc(u8, 0);
    const pixels = try alloc.alloc(u8, width * height);
    errdefer alloc.free(pixels);
    const prev = try alloc.alloc(u8, width);
    defer alloc.free(prev);
    @memset(prev, 0xff);

    var decoder = Decoder{
        .input = input,
        .order = order,
        .sub_format = sub_format,
        .width = width,
        .height = height,
        .curr = undefined,
        .prev = prev,
        .byte_align = opts.byte_align,
        .mixed_2d = opts.mixed_2d,
    };

    if (sub_format == .group3) try decoder.decodeEol();

    var row: usize = 0;
    while (row < height) : (row += 1) {
        decoder.curr = pixels[row * width .. (row + 1) * width];
        try decoder.decodeRow();
        @memcpy(prev, decoder.curr);
    }
    return pixels;
}

fn packBitsAlloc(alloc: Allocator, bits: []const u8) ![]u8 {
    const out_len = @divFloor(bits.len + 7, 8);
    const out = try alloc.alloc(u8, out_len);
    @memset(out, 0);
    for (bits, 0..) |bit, i| {
        if (bit != '1') continue;
        out[i / 8] |= @as(u8, 1) << @intCast(7 - (i % 8));
    }
    return out;
}

test "ccitt decodes simple group3 row" {
    const alloc = std.testing.allocator;
    const bits =
        "000000000001" ++
        "0111" ++
        "11" ++
        "0111" ++
        "11" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001";
    const encoded = try packBitsAlloc(alloc, bits);
    defer alloc.free(encoded);

    const gray = try decodeGrayAlloc(alloc, encoded, .msb, .group3, 8, 1, .{});
    defer alloc.free(gray);
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00 }, gray);
}

test "ccitt decodes simple group4 row" {
    const alloc = std.testing.allocator;
    const bits =
        "001" ++
        "0111" ++
        "11" ++
        "001" ++
        "0111" ++
        "11" ++
        "000000000001" ++
        "000000000001";
    const encoded = try packBitsAlloc(alloc, bits);
    defer alloc.free(encoded);

    const gray = try decodeGrayAlloc(alloc, encoded, .msb, .group4, 8, 1, .{});
    defer alloc.free(gray);
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00 }, gray);
}

test "ccitt decodes simple mixed group3 rows" {
    const alloc = std.testing.allocator;
    const bits =
        "000000000001" ++
        "1" ++
        "0111" ++
        "11" ++
        "0111" ++
        "11" ++
        "000000000001" ++
        "0" ++
        "1" ++
        "1" ++
        "1" ++
        "1" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001" ++
        "000000000001";
    const encoded = try packBitsAlloc(alloc, bits);
    defer alloc.free(encoded);

    const gray = try decodeGrayAlloc(alloc, encoded, .msb, .group3, 8, 2, .{ .mixed_2d = true });
    defer alloc.free(gray);
    try std.testing.expectEqualSlices(
        u8,
        &.{
            0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00,
            0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00,
        },
        gray,
    );
}
