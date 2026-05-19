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
const bitstream = @import("bitstream.zig");

pub const BitSliceReader = struct {
    bytes: []const u8,
    start_bit: usize,
    bit_length: usize,
    bit_offset: usize = 0,

    pub fn init(bytes: []const u8, start_bit: usize, bit_length: usize) BitSliceReader {
        return .{
            .bytes = bytes,
            .start_bit = start_bit,
            .bit_length = bit_length,
        };
    }

    pub fn remainingBits(self: BitSliceReader) usize {
        return self.bit_length -| self.bit_offset;
    }

    pub fn peekBits(self: BitSliceReader, count: u5) !u32 {
        if (count == 0) return 0;
        if (self.bit_offset + count > self.bit_length) return error.EndOfStream;
        return try readBitWindow(self.bytes, self.start_bit + self.bit_offset, count);
    }

    pub fn readBits(self: *BitSliceReader, count: u5) !u32 {
        if (count == 0) return 0;
        const result = try self.peekBits(count);
        self.bit_offset += count;
        return result;
    }
};

fn readBitWindow(bytes: []const u8, absolute_bit: usize, count: u5) !u32 {
    if (absolute_bit + count > bytes.len * 8) return error.EndOfStream;

    const byte_index = absolute_bit / 8;
    const bit_index = absolute_bit % 8;
    const needed_bits = bit_index + @as(usize, count);
    const needed_bytes = (needed_bits + 7) / 8;

    var window: u64 = 0;
    for (0..needed_bytes) |offset| {
        window = (window << 8) | bytes[byte_index + offset];
    }

    const window_bits = needed_bytes * 8;
    const low_discard: u6 = @intCast(window_bits - bit_index - @as(usize, count));
    const mask = (@as(u64, 1) << @intCast(count)) - 1;
    return @intCast((window >> low_discard) & mask);
}

pub const RegionPlan = struct {
    big_values: usize,
    big_value_samples: usize,
    region_sample_bounds: [3]usize,
    region_pair_counts: [3]usize,
    table_select: [3]u8,
    count1_table_select: bool,

    pub fn totalBigValuePairs(self: RegionPlan) usize {
        return self.region_pair_counts[0] + self.region_pair_counts[1] + self.region_pair_counts[2];
    }

    pub fn regionPairStart(self: RegionPlan, region_index: usize) usize {
        return switch (region_index) {
            0 => 0,
            1 => self.region_pair_counts[0],
            2 => self.region_pair_counts[0] + self.region_pair_counts[1],
            else => unreachable,
        };
    }
};

pub const DecodedPair = struct {
    x: i16,
    y: i16,
};

pub const DecodedQuad = struct {
    v: i16,
    w: i16,
    x: i16,
    y: i16,
};

pub const BigValueDecodeProgress = struct {
    pairs_decoded: usize,
    unsupported_table: ?u8 = null,
};

pub const Count1DecodeProgress = struct {
    quads_decoded: usize,
    samples_decoded: usize,
};

pub const TableUsage = struct {
    counts: [34]usize = [_]usize{0} ** 34,

    pub fn note(self: *TableUsage, table_index: u8) void {
        if (table_index < self.counts.len) self.counts[table_index] += 1;
    }

    pub fn usedCount(self: TableUsage) usize {
        var total: usize = 0;
        for (self.counts) |count| {
            if (count > 0) total += 1;
        }
        return total;
    }

    pub fn wasUsed(self: TableUsage, table_index: u8) bool {
        return table_index < self.counts.len and self.counts[table_index] > 0;
    }
};

pub const RegionReader = struct {
    pair_start: usize,
    pair_count: usize,
    table_select: u8,
};

const HuffData = struct {
    vlc_table: u8,
    linbits: u8,
};

const huff_data = [_]HuffData{
    .{ .vlc_table = 0, .linbits = 0 },
    .{ .vlc_table = 1, .linbits = 0 },
    .{ .vlc_table = 2, .linbits = 0 },
    .{ .vlc_table = 3, .linbits = 0 },
    .{ .vlc_table = 0, .linbits = 0 },
    .{ .vlc_table = 4, .linbits = 0 },
    .{ .vlc_table = 5, .linbits = 0 },
    .{ .vlc_table = 6, .linbits = 0 },
    .{ .vlc_table = 7, .linbits = 0 },
    .{ .vlc_table = 8, .linbits = 0 },
    .{ .vlc_table = 9, .linbits = 0 },
    .{ .vlc_table = 10, .linbits = 0 },
    .{ .vlc_table = 11, .linbits = 0 },
    .{ .vlc_table = 12, .linbits = 0 },
    .{ .vlc_table = 0, .linbits = 0 },
    .{ .vlc_table = 13, .linbits = 0 },
    .{ .vlc_table = 14, .linbits = 1 },
    .{ .vlc_table = 14, .linbits = 2 },
    .{ .vlc_table = 14, .linbits = 3 },
    .{ .vlc_table = 14, .linbits = 4 },
    .{ .vlc_table = 14, .linbits = 6 },
    .{ .vlc_table = 14, .linbits = 8 },
    .{ .vlc_table = 14, .linbits = 10 },
    .{ .vlc_table = 14, .linbits = 13 },
    .{ .vlc_table = 15, .linbits = 4 },
    .{ .vlc_table = 15, .linbits = 5 },
    .{ .vlc_table = 15, .linbits = 6 },
    .{ .vlc_table = 15, .linbits = 7 },
    .{ .vlc_table = 15, .linbits = 8 },
    .{ .vlc_table = 15, .linbits = 9 },
    .{ .vlc_table = 15, .linbits = 11 },
    .{ .vlc_table = 15, .linbits = 13 },
};

const quad_codes = [2][16]u8{
    .{ 1, 5, 4, 5, 6, 5, 4, 4, 7, 3, 6, 0, 7, 2, 3, 1 },
    .{ 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 },
};

const quad_bits = [2][16]u8{
    .{ 1, 4, 4, 5, 4, 6, 5, 6, 4, 5, 5, 6, 5, 6, 6, 6 },
    .{ 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 },
};

const symbol_lookup_bits: u5 = 9;
const symbol_lookup_size = 1 << symbol_lookup_bits;
const quad_lookup_bits: u5 = 6;
const quad_lookup_size = 1 << quad_lookup_bits;

const SymbolLookupEntry = struct {
    bits: u8 = 0,
    symbol: u16 = 0,
};

fn buildSymbolLookup(comptime bits: []const u8, comptime codes: []const u16) [symbol_lookup_size]SymbolLookupEntry {
    var table = [_]SymbolLookupEntry{.{}} ** symbol_lookup_size;
    for (bits, codes, 0..) |entry_bits, entry_code, symbol| {
        if (entry_bits == 0 or entry_bits > symbol_lookup_bits) continue;
        const shift: u5 = @intCast(symbol_lookup_bits - entry_bits);
        const start = @as(usize, entry_code) << shift;
        const count = @as(usize, 1) << shift;
        for (start..start + count) |index| {
            table[index] = .{
                .bits = entry_bits,
                .symbol = @intCast(symbol),
            };
        }
    }
    return table;
}

fn buildQuadLookup(comptime bits: []const u8, comptime codes: []const u8) [quad_lookup_size]SymbolLookupEntry {
    var table = [_]SymbolLookupEntry{.{}} ** quad_lookup_size;
    for (bits, codes, 0..) |entry_bits, entry_code, symbol| {
        if (entry_bits == 0 or entry_bits > quad_lookup_bits) continue;
        const shift: u5 = @intCast(quad_lookup_bits - entry_bits);
        const start = @as(usize, entry_code) << shift;
        const count = @as(usize, 1) << shift;
        for (start..start + count) |index| {
            table[index] = .{
                .bits = entry_bits,
                .symbol = @intCast(symbol),
            };
        }
    }
    return table;
}

const codebook1_codes = [_]u16{
    0x0001, 0x0001, 0x0001, 0x0000,
};

const codebook1_bits = [_]u8{
    1, 3, 2, 3,
};

const codebook2_codes = [_]u16{
    0x0001, 0x0002, 0x0001, 0x0003, 0x0001, 0x0001, 0x0003, 0x0002,
    0x0000,
};

const codebook2_bits = [_]u8{
    1, 3, 6, 3, 3, 5, 5, 5,
    6,
};

const codebook3_codes = [_]u16{
    0x0003, 0x0002, 0x0001, 0x0001, 0x0001, 0x0001, 0x0003, 0x0002,
    0x0000,
};

const codebook3_bits = [_]u8{
    2, 2, 6, 3, 2, 5, 5, 5,
    6,
};

const codebook5_codes = [_]u16{
    0x0001, 0x0002, 0x0006, 0x0005, 0x0003, 0x0001, 0x0004, 0x0004,
    0x0007, 0x0005, 0x0007, 0x0001, 0x0006, 0x0001, 0x0001, 0x0000,
};

const codebook5_bits = [_]u8{
    1, 3, 6, 7, 3, 3, 6, 7,
    6, 6, 7, 8, 7, 6, 7, 8,
};

const codebook6_codes = [_]u16{
    0x0007, 0x0003, 0x0005, 0x0001, 0x0006, 0x0002, 0x0003, 0x0002,
    0x0005, 0x0004, 0x0004, 0x0001, 0x0003, 0x0003, 0x0002, 0x0000,
};

const codebook6_bits = [_]u8{
    3, 3, 5, 7, 3, 2, 4, 5,
    4, 4, 5, 6, 6, 5, 6, 7,
};

const codebook7_codes = [_]u16{
    0x0001, 0x0002, 0x000a, 0x0013, 0x0010, 0x000a, 0x0003, 0x0003,
    0x0007, 0x000a, 0x0005, 0x0003, 0x000b, 0x0004, 0x000d, 0x0011,
    0x0008, 0x0004, 0x000c, 0x000b, 0x0012, 0x000f, 0x000b, 0x0002,
    0x0007, 0x0006, 0x0009, 0x000e, 0x0003, 0x0001, 0x0006, 0x0004,
    0x0005, 0x0003, 0x0002, 0x0000,
};

const codebook7_bits = [_]u8{
    1, 3,  6,  8,  8, 9,  3, 4,
    6, 7,  7,  8,  6, 5,  7, 8,
    8, 9,  7,  7,  8, 9,  9, 9,
    7, 7,  8,  9,  9, 10, 8, 8,
    9, 10, 10, 10,
};

const codebook8_codes = [_]u16{
    0x0003, 0x0004, 0x0006, 0x0012, 0x000c, 0x0005, 0x0005, 0x0001,
    0x0002, 0x0010, 0x0009, 0x0003, 0x0007, 0x0003, 0x0005, 0x000e,
    0x0007, 0x0003, 0x0013, 0x0011, 0x000f, 0x000d, 0x000a, 0x0004,
    0x000d, 0x0005, 0x0008, 0x000b, 0x0005, 0x0001, 0x000c, 0x0004,
    0x0004, 0x0001, 0x0001, 0x0000,
};

const codebook8_bits = [_]u8{
    2, 3, 6,  8,  8,  9,  3, 2,
    4, 8, 8,  8,  6,  4,  6, 8,
    8, 9, 8,  8,  8,  9,  9, 10,
    8, 7, 8,  9,  10, 10, 9, 8,
    9, 9, 11, 11,
};

const codebook9_codes = [_]u16{
    0x0007, 0x0005, 0x0009, 0x000e, 0x000f, 0x0007, 0x0006, 0x0004,
    0x0005, 0x0005, 0x0006, 0x0007, 0x0007, 0x0006, 0x0008, 0x0008,
    0x0008, 0x0005, 0x000f, 0x0006, 0x0009, 0x000a, 0x0005, 0x0001,
    0x000b, 0x0007, 0x0009, 0x0006, 0x0004, 0x0001, 0x000e, 0x0004,
    0x0006, 0x0002, 0x0006, 0x0000,
};

const codebook9_bits = [_]u8{
    3, 3, 5, 6, 8, 9, 3, 3,
    4, 5, 6, 8, 4, 4, 5, 6,
    7, 8, 6, 5, 6, 7, 7, 8,
    7, 6, 7, 7, 8, 9, 8, 7,
    8, 8, 9, 9,
};

const codebook10_codes = [_]u16{
    0x0001, 0x0002, 0x000a, 0x0017, 0x0023, 0x001e, 0x000c, 0x0011,
    0x0003, 0x0003, 0x0008, 0x000c, 0x0012, 0x0015, 0x000c, 0x0007,
    0x000b, 0x0009, 0x000f, 0x0015, 0x0020, 0x0028, 0x0013, 0x0006,
    0x000e, 0x000d, 0x0016, 0x0022, 0x002e, 0x0017, 0x0012, 0x0007,
    0x0014, 0x0013, 0x0021, 0x002f, 0x001b, 0x0016, 0x0009, 0x0003,
    0x001f, 0x0016, 0x0029, 0x001a, 0x0015, 0x0014, 0x0005, 0x0003,
    0x000e, 0x000d, 0x000a, 0x000b, 0x0010, 0x0006, 0x0005, 0x0001,
    0x0009, 0x0008, 0x0007, 0x0008, 0x0004, 0x0004, 0x0002, 0x0000,
};

const codebook10_bits = [_]u8{
    1, 3, 6,  8,  9,  9,  9,  10,
    3, 4, 6,  7,  8,  9,  8,  8,
    6, 6, 7,  8,  9,  10, 9,  9,
    7, 7, 8,  9,  10, 10, 9,  10,
    8, 8, 9,  10, 10, 10, 10, 10,
    9, 9, 10, 10, 11, 11, 10, 11,
    8, 8, 9,  10, 10, 10, 11, 11,
    9, 8, 9,  10, 10, 11, 11, 11,
};

const codebook11_codes = [_]u16{
    0x0003, 0x0004, 0x000a, 0x0018, 0x0022, 0x0021, 0x0015, 0x000f,
    0x0005, 0x0003, 0x0004, 0x000a, 0x0020, 0x0011, 0x000b, 0x000a,
    0x000b, 0x0007, 0x000d, 0x0012, 0x001e, 0x001f, 0x0014, 0x0005,
    0x0019, 0x000b, 0x0013, 0x003b, 0x001b, 0x0012, 0x000c, 0x0005,
    0x0023, 0x0021, 0x001f, 0x003a, 0x001e, 0x0010, 0x0007, 0x0005,
    0x001c, 0x001a, 0x0020, 0x0013, 0x0011, 0x000f, 0x0008, 0x000e,
    0x000e, 0x000c, 0x0009, 0x000d, 0x000e, 0x0009, 0x0004, 0x0001,
    0x000b, 0x0004, 0x0006, 0x0006, 0x0006, 0x0003, 0x0002, 0x0000,
};

const codebook11_bits = [_]u8{
    2, 3, 5, 7,  8,  9,  8,  9,
    3, 3, 4, 6,  8,  8,  7,  8,
    5, 5, 6, 7,  8,  9,  8,  8,
    7, 6, 7, 9,  8,  10, 8,  9,
    8, 8, 8, 9,  9,  10, 9,  10,
    8, 8, 9, 10, 10, 11, 10, 11,
    8, 7, 7, 8,  9,  10, 10, 10,
    8, 7, 8, 9,  10, 10, 10, 10,
};

const codebook12_codes = [_]u16{
    0x0009, 0x0006, 0x0010, 0x0021, 0x0029, 0x0027, 0x0026, 0x001a,
    0x0007, 0x0005, 0x0006, 0x0009, 0x0017, 0x0010, 0x001a, 0x000b,
    0x0011, 0x0007, 0x000b, 0x000e, 0x0015, 0x001e, 0x000a, 0x0007,
    0x0011, 0x000a, 0x000f, 0x000c, 0x0012, 0x001c, 0x000e, 0x0005,
    0x0020, 0x000d, 0x0016, 0x0013, 0x0012, 0x0010, 0x0009, 0x0005,
    0x0028, 0x0011, 0x001f, 0x001d, 0x0011, 0x000d, 0x0004, 0x0002,
    0x001b, 0x000c, 0x000b, 0x000f, 0x000a, 0x0007, 0x0004, 0x0001,
    0x001b, 0x000c, 0x0008, 0x000c, 0x0006, 0x0003, 0x0001, 0x0000,
};

const codebook12_bits = [_]u8{
    4, 3, 5, 7, 8, 9, 9, 9,
    3, 3, 4, 5, 7, 7, 8, 8,
    5, 4, 5, 6, 7, 8, 7, 8,
    6, 5, 6, 6, 7, 8, 8, 8,
    7, 6, 7, 7, 8, 8, 8, 9,
    8, 7, 8, 8, 8, 9, 8, 9,
    8, 7, 7, 8, 8, 9, 9, 10,
    9, 8, 8, 9, 9, 9, 9, 10,
};

const codebook24_codes = [_]u16{
    0x000f, 0x000d, 0x002e, 0x0050, 0x0092, 0x0106, 0x00f8, 0x01b2, 0x01aa, 0x029d, 0x028d, 0x0289, 0x026d, 0x0205, 0x0408, 0x0058,
    0x000e, 0x000c, 0x0015, 0x0026, 0x0047, 0x0082, 0x007a, 0x00d8, 0x00d1, 0x00c6, 0x0147, 0x0159, 0x013f, 0x0129, 0x0117, 0x002a,
    0x002f, 0x0016, 0x0029, 0x004a, 0x0044, 0x0080, 0x0078, 0x00dd, 0x00cf, 0x00c2, 0x00b6, 0x0154, 0x013b, 0x0127, 0x021d, 0x0012,
    0x0051, 0x0027, 0x004b, 0x0046, 0x0086, 0x007d, 0x0074, 0x00dc, 0x00cc, 0x00be, 0x00b2, 0x0145, 0x0137, 0x0125, 0x010f, 0x0010,
    0x0093, 0x0048, 0x0045, 0x0087, 0x007f, 0x0076, 0x0070, 0x00d2, 0x00c8, 0x00bc, 0x0160, 0x0143, 0x0132, 0x011d, 0x021c, 0x000e,
    0x0107, 0x0042, 0x0081, 0x007e, 0x0077, 0x0072, 0x00d6, 0x00ca, 0x00c0, 0x00b4, 0x0155, 0x013d, 0x012d, 0x0119, 0x0106, 0x000c,
    0x00f9, 0x007b, 0x0079, 0x0075, 0x0071, 0x00d7, 0x00ce, 0x00c3, 0x00b9, 0x015b, 0x014a, 0x0134, 0x0123, 0x0110, 0x0208, 0x000a,
    0x01b3, 0x0073, 0x006f, 0x006d, 0x00d3, 0x00cb, 0x00c4, 0x00bb, 0x0161, 0x014c, 0x0139, 0x012a, 0x011b, 0x0213, 0x017d, 0x0011,
    0x01ab, 0x00d4, 0x00d0, 0x00cd, 0x00c9, 0x00c1, 0x00ba, 0x00b1, 0x00a9, 0x0140, 0x012f, 0x011e, 0x010c, 0x0202, 0x0179, 0x0010,
    0x014f, 0x00c7, 0x00c5, 0x00bf, 0x00bd, 0x00b5, 0x00ae, 0x014d, 0x0141, 0x0131, 0x0121, 0x0113, 0x0209, 0x017b, 0x0173, 0x000b,
    0x029c, 0x00b8, 0x00b7, 0x00b3, 0x00af, 0x0158, 0x014b, 0x013a, 0x0130, 0x0122, 0x0115, 0x0212, 0x017f, 0x0175, 0x016e, 0x000a,
    0x028c, 0x015a, 0x00ab, 0x00a8, 0x00a4, 0x013e, 0x0135, 0x012b, 0x011f, 0x0114, 0x0107, 0x0201, 0x0177, 0x0170, 0x016a, 0x0006,
    0x0288, 0x0142, 0x013c, 0x0138, 0x0133, 0x012e, 0x0124, 0x011c, 0x010d, 0x0105, 0x0200, 0x0178, 0x0172, 0x016c, 0x0167, 0x0004,
    0x026c, 0x012c, 0x0128, 0x0126, 0x0120, 0x011a, 0x0111, 0x010a, 0x0203, 0x017c, 0x0176, 0x0171, 0x016d, 0x0169, 0x0165, 0x0002,
    0x0409, 0x0118, 0x0116, 0x0112, 0x010b, 0x0108, 0x0103, 0x017e, 0x017a, 0x0174, 0x016f, 0x016b, 0x0168, 0x0166, 0x0164, 0x0000,
    0x002b, 0x0014, 0x0013, 0x0011, 0x000f, 0x000d, 0x000b, 0x0009, 0x0007, 0x0006, 0x0004, 0x0007, 0x0005, 0x0003, 0x0001, 0x0003,
};

const codebook24_bits = [_]u8{
    4,  4,  6,  7,  8,  9,  9,  10, 10, 11, 11, 11, 11, 11, 12, 9,
    4,  4,  5,  6,  7,  8,  8,  9,  9,  9,  10, 10, 10, 10, 10, 8,
    6,  5,  6,  7,  7,  8,  8,  9,  9,  9,  9,  10, 10, 10, 11, 7,
    7,  6,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 7,
    8,  7,  7,  8,  8,  8,  8,  9,  9,  9,  10, 10, 10, 10, 11, 7,
    9,  7,  8,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10, 7,
    9,  8,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10, 11, 7,
    10, 8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10, 11, 11, 8,
    10, 9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 11, 11, 8,
    10, 9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 11, 11, 11, 8,
    11, 9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 8,
    11, 10, 9,  9,  9,  10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 8,
    11, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 8,
    11, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 8,
    12, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 8,
    8,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  4,
};

const codebook15_codes = [_]u16{
    0x0007, 0x000c, 0x0012, 0x0035, 0x002f, 0x004c, 0x007c, 0x006c, 0x0059, 0x007b, 0x006c, 0x0077, 0x006b, 0x0051, 0x007a, 0x003f,
    0x000d, 0x0005, 0x0010, 0x001b, 0x002e, 0x0024, 0x003d, 0x0033, 0x002a, 0x0046, 0x0034, 0x0053, 0x0041, 0x0029, 0x003b, 0x0024,
    0x0013, 0x0011, 0x000f, 0x0018, 0x0029, 0x0022, 0x003b, 0x0030, 0x0028, 0x0040, 0x0032, 0x004e, 0x003e, 0x0050, 0x0038, 0x0021,
    0x001d, 0x001c, 0x0019, 0x002b, 0x0027, 0x003f, 0x0037, 0x005d, 0x004c, 0x003b, 0x005d, 0x0048, 0x0036, 0x004b, 0x0032, 0x001d,
    0x0034, 0x0016, 0x002a, 0x0028, 0x0043, 0x0039, 0x005f, 0x004f, 0x0048, 0x0039, 0x0059, 0x0045, 0x0031, 0x0042, 0x002e, 0x001b,
    0x004d, 0x0025, 0x0023, 0x0042, 0x003a, 0x0034, 0x005b, 0x004a, 0x003e, 0x0030, 0x004f, 0x003f, 0x005a, 0x003e, 0x0028, 0x0026,
    0x007d, 0x0020, 0x003c, 0x0038, 0x0032, 0x005c, 0x004e, 0x0041, 0x0037, 0x0057, 0x0047, 0x0033, 0x0049, 0x0033, 0x0046, 0x001e,
    0x006d, 0x0035, 0x0031, 0x005e, 0x0058, 0x004b, 0x0042, 0x007a, 0x005b, 0x0049, 0x0038, 0x002a, 0x0040, 0x002c, 0x0015, 0x0019,
    0x005a, 0x002b, 0x0029, 0x004d, 0x0049, 0x003f, 0x0038, 0x005c, 0x004d, 0x0042, 0x002f, 0x0043, 0x0030, 0x0035, 0x0024, 0x0014,
    0x0047, 0x0022, 0x0043, 0x003c, 0x003a, 0x0031, 0x0058, 0x004c, 0x0043, 0x006a, 0x0047, 0x0036, 0x0026, 0x0027, 0x0017, 0x000f,
    0x006d, 0x0035, 0x0033, 0x002f, 0x005a, 0x0052, 0x003a, 0x0039, 0x0030, 0x0048, 0x0039, 0x0029, 0x0017, 0x001b, 0x003e, 0x0009,
    0x0056, 0x002a, 0x0028, 0x0025, 0x0046, 0x0040, 0x0034, 0x002b, 0x0046, 0x0037, 0x002a, 0x0019, 0x001d, 0x0012, 0x000b, 0x000b,
    0x0076, 0x0044, 0x001e, 0x0037, 0x0032, 0x002e, 0x004a, 0x0041, 0x0031, 0x0027, 0x0018, 0x0010, 0x0016, 0x000d, 0x000e, 0x0007,
    0x005b, 0x002c, 0x0027, 0x0026, 0x0022, 0x003f, 0x0034, 0x002d, 0x001f, 0x0034, 0x001c, 0x0013, 0x000e, 0x0008, 0x0009, 0x0003,
    0x007b, 0x003c, 0x003a, 0x0035, 0x002f, 0x002b, 0x0020, 0x0016, 0x0025, 0x0018, 0x0011, 0x000c, 0x000f, 0x000a, 0x0002, 0x0001,
    0x0047, 0x0025, 0x0022, 0x001e, 0x001c, 0x0014, 0x0011, 0x001a, 0x0015, 0x0010, 0x000a, 0x0006, 0x0008, 0x0006, 0x0002, 0x0000,
};

const codebook15_bits = [_]u8{
    3,  4,  5,  7,  7,  8,  9,  9,  9,  10, 10, 11, 11, 11, 12, 13,
    4,  3,  5,  6,  7,  7,  8,  8,  8,  9,  9,  10, 10, 10, 11, 11,
    5,  5,  5,  6,  7,  7,  8,  8,  8,  9,  9,  10, 10, 11, 11, 11,
    6,  6,  6,  7,  7,  8,  8,  9,  9,  9,  10, 10, 10, 11, 11, 11,
    7,  6,  7,  7,  8,  8,  9,  9,  9,  9,  10, 10, 10, 11, 11, 11,
    8,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 11, 11, 11, 12,
    9,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 11, 11, 12, 12,
    9,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 10, 11, 11, 11, 12,
    9,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 11, 11, 12, 12, 12,
    9,  8,  9,  9,  9,  9,  10, 10, 10, 11, 11, 11, 11, 12, 12, 12,
    10, 9,  9,  9,  10, 10, 10, 10, 10, 11, 11, 11, 11, 12, 13, 12,
    10, 9,  9,  9,  10, 10, 10, 10, 11, 11, 11, 11, 12, 12, 12, 13,
    11, 10, 9,  10, 10, 10, 11, 11, 11, 11, 11, 11, 12, 12, 13, 13,
    11, 10, 10, 10, 10, 11, 11, 11, 11, 12, 12, 12, 12, 12, 13, 13,
    12, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 13, 13, 12, 13,
    12, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 13, 13, 13, 13,
};

const codebook13_codes = [_]u16{
    0x0001, 0x0005, 0x000e, 0x0015, 0x0022, 0x0033, 0x002e, 0x0047, 0x002a, 0x0034, 0x0044, 0x0034, 0x0043, 0x002c, 0x002b, 0x0013,
    0x0003, 0x0004, 0x000c, 0x0013, 0x001f, 0x001a, 0x002c, 0x0021, 0x001f, 0x0018, 0x0020, 0x0018, 0x001f, 0x0023, 0x0016, 0x000e,
    0x000f, 0x000d, 0x0017, 0x0024, 0x003b, 0x0031, 0x004d, 0x0041, 0x001d, 0x0028, 0x001e, 0x0028, 0x001b, 0x0021, 0x002a, 0x0010,
    0x0016, 0x0014, 0x0025, 0x003d, 0x0038, 0x004f, 0x0049, 0x0040, 0x002b, 0x004c, 0x0038, 0x0025, 0x001a, 0x001f, 0x0019, 0x000e,
    0x0023, 0x0010, 0x003c, 0x0039, 0x0061, 0x004b, 0x0072, 0x005b, 0x0036, 0x0049, 0x0037, 0x0029, 0x0030, 0x0035, 0x0017, 0x0018,
    0x003a, 0x001b, 0x0032, 0x0060, 0x004c, 0x0046, 0x005d, 0x0054, 0x004d, 0x003a, 0x004f, 0x001d, 0x004a, 0x0031, 0x0029, 0x0011,
    0x002f, 0x002d, 0x004e, 0x004a, 0x0073, 0x005e, 0x005a, 0x004f, 0x0045, 0x0053, 0x0047, 0x0032, 0x003b, 0x0026, 0x0024, 0x000f,
    0x0048, 0x0022, 0x0038, 0x005f, 0x005c, 0x0055, 0x005b, 0x005a, 0x0056, 0x0049, 0x004d, 0x0041, 0x0033, 0x002c, 0x002b, 0x002a,
    0x002b, 0x0014, 0x001e, 0x002c, 0x0037, 0x004e, 0x0048, 0x0057, 0x004e, 0x003d, 0x002e, 0x0036, 0x0025, 0x001e, 0x0014, 0x0010,
    0x0035, 0x0019, 0x0029, 0x0025, 0x002c, 0x003b, 0x0036, 0x0051, 0x0042, 0x004c, 0x0039, 0x0036, 0x0025, 0x0012, 0x0027, 0x000b,
    0x0023, 0x0021, 0x001f, 0x0039, 0x002a, 0x0052, 0x0048, 0x0050, 0x002f, 0x003a, 0x0037, 0x0015, 0x0016, 0x001a, 0x0026, 0x0016,
    0x0035, 0x0019, 0x0017, 0x0026, 0x0046, 0x003c, 0x0033, 0x0024, 0x0037, 0x001a, 0x0022, 0x0017, 0x001b, 0x000e, 0x0009, 0x0007,
    0x0022, 0x0020, 0x001c, 0x0027, 0x0031, 0x004b, 0x001e, 0x0034, 0x0030, 0x0028, 0x0034, 0x001c, 0x0012, 0x0011, 0x0009, 0x0005,
    0x002d, 0x0015, 0x0022, 0x0040, 0x0038, 0x0032, 0x0031, 0x002d, 0x001f, 0x0013, 0x000c, 0x000f, 0x000a, 0x0007, 0x0006, 0x0003,
    0x0030, 0x0017, 0x0014, 0x0027, 0x0024, 0x0023, 0x0035, 0x0015, 0x0010, 0x0017, 0x000d, 0x000a, 0x0006, 0x0001, 0x0004, 0x0002,
    0x0010, 0x000f, 0x0011, 0x001b, 0x0019, 0x0014, 0x001d, 0x000b, 0x0011, 0x000c, 0x0010, 0x0008, 0x0001, 0x0001, 0x0000, 0x0001,
};

const codebook13_bits = [_]u8{
    1,  4,  6,  7,  8,  9,  9,  10, 9,  10, 11, 11, 12, 12, 13, 13,
    3,  4,  6,  7,  8,  8,  9,  9,  9,  9,  10, 10, 11, 12, 12, 12,
    6,  6,  7,  8,  9,  9,  10, 10, 9,  10, 10, 11, 11, 12, 13, 13,
    7,  7,  8,  9,  9,  10, 10, 10, 10, 11, 11, 11, 11, 12, 13, 13,
    8,  7,  9,  9,  10, 10, 11, 11, 10, 11, 11, 12, 12, 13, 13, 14,
    9,  8,  9,  10, 10, 10, 11, 11, 11, 11, 12, 11, 13, 13, 14, 14,
    9,  9,  10, 10, 11, 11, 11, 11, 11, 12, 12, 12, 13, 13, 14, 14,
    10, 9,  10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 16, 16,
    9,  8,  9,  10, 10, 11, 11, 12, 12, 12, 12, 13, 13, 14, 15, 15,
    10, 9,  10, 10, 11, 11, 11, 13, 12, 13, 13, 14, 14, 14, 16, 15,
    10, 10, 10, 11, 11, 12, 12, 13, 12, 13, 14, 13, 14, 15, 16, 17,
    11, 10, 10, 11, 12, 12, 12, 12, 13, 13, 13, 14, 15, 15, 15, 16,
    11, 11, 11, 12, 12, 13, 12, 13, 14, 14, 15, 15, 15, 16, 16, 16,
    12, 11, 12, 13, 13, 13, 14, 14, 14, 14, 14, 15, 16, 15, 16, 16,
    13, 12, 12, 13, 13, 13, 15, 14, 14, 17, 15, 15, 15, 17, 16, 16,
    12, 12, 13, 14, 14, 14, 15, 14, 15, 15, 16, 16, 19, 18, 19, 16,
};

const codebook16_codes = [_]u16{
    0x0001, 0x0005, 0x000e, 0x002c, 0x004a, 0x003f, 0x006e, 0x005d, 0x00ac, 0x0095, 0x008a, 0x00f2, 0x00e1, 0x00c3, 0x0178, 0x0011,
    0x0003, 0x0004, 0x000c, 0x0014, 0x0023, 0x003e, 0x0035, 0x002f, 0x0053, 0x004b, 0x0044, 0x0077, 0x00c9, 0x006b, 0x00cf, 0x0009,
    0x000f, 0x000d, 0x0017, 0x0026, 0x0043, 0x003a, 0x0067, 0x005a, 0x00a1, 0x0048, 0x007f, 0x0075, 0x006e, 0x00d1, 0x00ce, 0x0010,
    0x002d, 0x0015, 0x0027, 0x0045, 0x0040, 0x0072, 0x0063, 0x0057, 0x009e, 0x008c, 0x00fc, 0x00d4, 0x00c7, 0x0183, 0x016d, 0x001a,
    0x004b, 0x0024, 0x0044, 0x0041, 0x0073, 0x0065, 0x00b3, 0x00a4, 0x009b, 0x0108, 0x00f6, 0x00e2, 0x018b, 0x017e, 0x016a, 0x0009,
    0x0042, 0x001e, 0x003b, 0x0038, 0x0066, 0x00b9, 0x00ad, 0x0109, 0x008e, 0x00fd, 0x00e8, 0x0190, 0x0184, 0x017a, 0x01bd, 0x0010,
    0x006f, 0x0036, 0x0034, 0x0064, 0x00b8, 0x00b2, 0x00a0, 0x0085, 0x0101, 0x00f4, 0x00e4, 0x00d9, 0x0181, 0x016e, 0x02cb, 0x000a,
    0x0062, 0x0030, 0x005b, 0x0058, 0x00a5, 0x009d, 0x0094, 0x0105, 0x00f8, 0x0197, 0x018d, 0x0174, 0x017c, 0x0379, 0x0374, 0x0008,
    0x0055, 0x0054, 0x0051, 0x009f, 0x009c, 0x008f, 0x0104, 0x00f9, 0x01ab, 0x0191, 0x0188, 0x017f, 0x02d7, 0x02c9, 0x02c4, 0x0007,
    0x009a, 0x004c, 0x0049, 0x008d, 0x0083, 0x0100, 0x00f5, 0x01aa, 0x0196, 0x018a, 0x0180, 0x02df, 0x0167, 0x02c6, 0x0160, 0x000b,
    0x008b, 0x0081, 0x0043, 0x007d, 0x00f7, 0x00e9, 0x00e5, 0x00db, 0x0189, 0x02e7, 0x02e1, 0x02d0, 0x0375, 0x0372, 0x01b7, 0x0004,
    0x00f3, 0x0078, 0x0076, 0x0073, 0x00e3, 0x00df, 0x018c, 0x02ea, 0x02e6, 0x02e0, 0x02d1, 0x02c8, 0x02c2, 0x00df, 0x01b4, 0x0006,
    0x00ca, 0x00e0, 0x00de, 0x00da, 0x00d8, 0x0185, 0x0182, 0x017d, 0x016c, 0x0378, 0x01bb, 0x02c3, 0x01b8, 0x01b5, 0x06c0, 0x0004,
    0x02eb, 0x00d3, 0x00d2, 0x00d0, 0x0172, 0x017b, 0x02de, 0x02d3, 0x02ca, 0x06c7, 0x0373, 0x036d, 0x036c, 0x0d83, 0x0361, 0x0002,
    0x0179, 0x0171, 0x0066, 0x00bb, 0x02d6, 0x02d2, 0x0166, 0x02c7, 0x02c5, 0x0362, 0x06c6, 0x0367, 0x0d82, 0x0366, 0x01b2, 0x0000,
    0x000c, 0x000a, 0x0007, 0x000b, 0x000a, 0x0011, 0x000b, 0x0009, 0x000d, 0x000c, 0x000a, 0x0007, 0x0005, 0x0003, 0x0001, 0x0003,
};

const codebook16_bits = [_]u8{
    1,  4,  6,  8,  9,  9,  10, 10, 11, 11, 11, 12, 12, 12, 13, 9,
    3,  4,  6,  7,  8,  9,  9,  9,  10, 10, 10, 11, 12, 11, 12, 8,
    6,  6,  7,  8,  9,  9,  10, 10, 11, 10, 11, 11, 11, 12, 12, 9,
    8,  7,  8,  9,  9,  10, 10, 10, 11, 11, 12, 12, 12, 13, 13, 10,
    9,  8,  9,  9,  10, 10, 11, 11, 11, 12, 12, 12, 13, 13, 13, 9,
    9,  8,  9,  9,  10, 11, 11, 12, 11, 12, 12, 13, 13, 13, 14, 10,
    10, 9,  9,  10, 11, 11, 11, 11, 12, 12, 12, 12, 13, 13, 14, 10,
    10, 9,  10, 10, 11, 11, 11, 12, 12, 13, 13, 13, 13, 15, 15, 10,
    10, 10, 10, 11, 11, 11, 12, 12, 13, 13, 13, 13, 14, 14, 14, 10,
    11, 10, 10, 11, 11, 12, 12, 13, 13, 13, 13, 14, 13, 14, 13, 11,
    11, 11, 10, 11, 12, 12, 12, 12, 13, 14, 14, 14, 15, 15, 14, 10,
    12, 11, 11, 11, 12, 12, 13, 14, 14, 14, 14, 14, 14, 13, 14, 11,
    12, 12, 12, 12, 12, 13, 13, 13, 13, 15, 14, 14, 14, 14, 16, 11,
    14, 12, 12, 12, 13, 13, 14, 14, 14, 16, 15, 15, 15, 17, 15, 11,
    13, 13, 11, 12, 14, 14, 13, 14, 14, 15, 16, 15, 17, 15, 14, 11,
    9,  8,  8,  9,  9,  10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 8,
};

const codebook13_symbols = [_]u8{
    0xFE, 0xFC, 0xFD, 0xED, 0xFF, 0xEF, 0xDF, 0xEE, 0xCF, 0xDE, 0xBF, 0xFB, 0xCE, 0xDC, 0xAF, 0xE9,
    0xEC, 0xDD, 0xFA, 0xCD, 0xBE, 0xEB, 0x9F, 0xF9, 0xEA, 0xBD, 0xDB, 0x8F, 0xF8, 0xCC, 0xAE, 0x9E,
    0x8E, 0x7F, 0x7E, 0xF7, 0xDA, 0xAD, 0xBC, 0xCB, 0xF6, 0x6F, 0xE8, 0x5F, 0x9D, 0xD9, 0xF5, 0xE7,
    0xAC, 0xBB, 0x4F, 0xF4, 0xCA, 0xE6, 0xF3, 0x3F, 0x8D, 0xD8, 0x2F, 0xF2, 0x6E, 0x9C, 0x0F, 0xC9,
    0x5E, 0xAB, 0x7D, 0xD7, 0x4E, 0xC8, 0xD6, 0x3E, 0xB9, 0x9B, 0xAA, 0x1F, 0xF1, 0xF0, 0xBA, 0xE5,
    0xE4, 0x8C, 0x6D, 0xE3, 0xE2, 0x2E, 0x0E, 0x1E, 0xE1, 0xE0, 0x5D, 0xD5, 0x7C, 0xC7, 0x4D, 0x8B,
    0xB8, 0xD4, 0x9A, 0xA9, 0x6C, 0xC6, 0x3D, 0xD3, 0x7B, 0x2D, 0xD2, 0x1D, 0xB7, 0x5C, 0xC5, 0x99,
    0x7A, 0xC3, 0xA7, 0x97, 0x4B, 0xD1, 0x0D, 0xD0, 0x8A, 0xA8, 0x4C, 0xC4, 0x6B, 0xB6, 0x3C, 0x2C,
    0xC2, 0x5B, 0xB5, 0x89, 0x1C, 0xC1, 0x98, 0x0C, 0xC0, 0xB4, 0x6A, 0xA6, 0x79, 0x3B, 0xB3, 0x88,
    0x5A, 0x2B, 0xA5, 0x69, 0xA4, 0x78, 0x87, 0x94, 0x77, 0x76, 0xB2, 0x1B, 0xB1, 0x0B, 0xB0, 0x96,
    0x4A, 0x3A, 0xA3, 0x59, 0x95, 0x2A, 0xA2, 0x1A, 0xA1, 0x0A, 0x68, 0xA0, 0x86, 0x49, 0x93, 0x39,
    0x58, 0x85, 0x67, 0x29, 0x92, 0x57, 0x75, 0x38, 0x83, 0x66, 0x47, 0x74, 0x56, 0x65, 0x73, 0x19,
    0x91, 0x09, 0x90, 0x48, 0x84, 0x72, 0x46, 0x64, 0x28, 0x82, 0x18, 0x37, 0x27, 0x17, 0x71, 0x55,
    0x07, 0x70, 0x36, 0x63, 0x45, 0x54, 0x26, 0x62, 0x35, 0x81, 0x08, 0x80, 0x16, 0x61, 0x06, 0x60,
    0x53, 0x44, 0x25, 0x52, 0x05, 0x15, 0x51, 0x34, 0x43, 0x50, 0x24, 0x42, 0x33, 0x14, 0x41, 0x04,
    0x40, 0x23, 0x32, 0x13, 0x31, 0x03, 0x30, 0x22, 0x12, 0x21, 0x02, 0x20, 0x11, 0x01, 0x10, 0x00,
};

const codebook15_symbols = [_]u8{
    0xFF, 0xEF, 0xFE, 0xDF, 0xEE, 0xFD, 0xCF, 0xFC, 0xDE, 0xED, 0xBF, 0xFB, 0xCE, 0xEC, 0xDD, 0xAF,
    0xFA, 0xBE, 0xEB, 0xCD, 0xDC, 0x9F, 0xF9, 0xEA, 0xBD, 0xDB, 0x8F, 0xF8, 0xCC, 0x9E, 0xE9, 0x7F,
    0xF7, 0xAD, 0xDA, 0xBC, 0x6F, 0xAE, 0x0F, 0xCB, 0xF6, 0x8E, 0xE8, 0x5F, 0x9D, 0xF5, 0x7E, 0xE7,
    0xAC, 0xCA, 0xBB, 0xD9, 0x8D, 0x4F, 0xF4, 0x3F, 0xF3, 0xD8, 0xE6, 0x2F, 0xF2, 0x6E, 0xF0, 0x1F,
    0xF1, 0x9C, 0xC9, 0x5E, 0xAB, 0xBA, 0xE5, 0x7D, 0xD7, 0x4E, 0xE4, 0x8C, 0xC8, 0x3E, 0x6D, 0xD6,
    0xE3, 0x9B, 0xB9, 0x2E, 0xAA, 0xE2, 0x1E, 0xE1, 0x0E, 0xE0, 0x5D, 0xD5, 0x7C, 0xC7, 0x4D, 0x8B,
    0xD4, 0xB8, 0x9A, 0xA9, 0x6C, 0xC6, 0x3D, 0xD3, 0xD2, 0x2D, 0x0D, 0x1D, 0x7B, 0xB7, 0xD1, 0x5C,
    0xD0, 0xC5, 0x8A, 0xA8, 0x4C, 0xC4, 0x6B, 0xB6, 0x99, 0x0C, 0x3C, 0xC3, 0x7A, 0xA7, 0xA6, 0xC0,
    0x0B, 0xC2, 0x2C, 0x5B, 0xB5, 0x1C, 0x89, 0x98, 0xC1, 0x4B, 0xB4, 0x6A, 0x3B, 0x79, 0xB3, 0x97,
    0x88, 0x2B, 0x5A, 0xB2, 0xA5, 0x1B, 0xB1, 0xB0, 0x69, 0x96, 0x4A, 0xA4, 0x78, 0x87, 0x3A, 0xA3,
    0x59, 0x95, 0x2A, 0xA2, 0x1A, 0xA1, 0x0A, 0xA0, 0x68, 0x86, 0x49, 0x94, 0x39, 0x93, 0x77, 0x09,
    0x58, 0x85, 0x29, 0x67, 0x76, 0x92, 0x91, 0x19, 0x90, 0x48, 0x84, 0x57, 0x75, 0x38, 0x83, 0x66,
    0x47, 0x28, 0x82, 0x18, 0x81, 0x74, 0x08, 0x80, 0x56, 0x65, 0x37, 0x73, 0x46, 0x27, 0x72, 0x64,
    0x17, 0x55, 0x71, 0x07, 0x70, 0x36, 0x63, 0x45, 0x54, 0x26, 0x62, 0x16, 0x06, 0x60, 0x35, 0x61,
    0x53, 0x44, 0x25, 0x52, 0x15, 0x51, 0x05, 0x50, 0x34, 0x43, 0x24, 0x42, 0x33, 0x41, 0x14, 0x04,
    0x23, 0x32, 0x40, 0x03, 0x13, 0x31, 0x30, 0x22, 0x12, 0x21, 0x02, 0x20, 0x11, 0x01, 0x10, 0x00,
};

const codebook16_symbols = [_]u8{
    0xEF, 0xFE, 0xDF, 0xFD, 0xCF, 0xFC, 0xBF, 0xFB, 0xAF, 0xFA, 0x9F, 0xF9, 0xF8, 0x8F, 0x7F, 0xF7,
    0x6F, 0xF6, 0xFF, 0x5F, 0xF5, 0x4F, 0xF4, 0xF3, 0xF0, 0x3F, 0xCE, 0xEC, 0xDD, 0xDE, 0xE9, 0xEA,
    0xD9, 0xEE, 0xED, 0xEB, 0xBE, 0xCD, 0xDC, 0xDB, 0xAE, 0xCC, 0xAD, 0xDA, 0x7E, 0xAC, 0xCA, 0xC9,
    0x7D, 0x5E, 0xBD, 0xF2, 0x2F, 0x0F, 0x1F, 0xF1, 0x9E, 0xBC, 0xCB, 0x8E, 0xE8, 0x9D, 0xE7, 0xBB,
    0x8D, 0xD8, 0x6E, 0xE6, 0x9C, 0xAB, 0xBA, 0xE5, 0xD7, 0x4E, 0xE4, 0x8C, 0xC8, 0x3E, 0x6D, 0xD6,
    0x9B, 0xB9, 0xAA, 0xE1, 0xD4, 0xB8, 0xA9, 0x7B, 0xB7, 0xD0, 0xE3, 0x0E, 0xE0, 0x5D, 0xD5, 0x7C,
    0xC7, 0x4D, 0x8B, 0x9A, 0x6C, 0xC6, 0x3D, 0x5C, 0xC5, 0x0D, 0x8A, 0xA8, 0x99, 0x4C, 0xB6, 0x7A,
    0x3C, 0x5B, 0x89, 0x1C, 0xC0, 0x98, 0x79, 0xE2, 0x2E, 0x1E, 0xD3, 0x2D, 0xD2, 0xD1, 0x3B, 0x97,
    0x88, 0x1D, 0xC4, 0x6B, 0xC3, 0xA7, 0x2C, 0xC2, 0xB5, 0xC1, 0x0C, 0x4B, 0xB4, 0x6A, 0xA6, 0xB3,
    0x5A, 0xA5, 0x2B, 0xB2, 0x1B, 0xB1, 0x0B, 0xB0, 0x69, 0x96, 0x4A, 0xA4, 0x78, 0x87, 0xA3, 0x3A,
    0x59, 0x2A, 0x95, 0x68, 0xA1, 0x86, 0x77, 0x94, 0x49, 0x57, 0x67, 0xA2, 0x1A, 0x0A, 0xA0, 0x39,
    0x93, 0x58, 0x85, 0x29, 0x92, 0x76, 0x09, 0x19, 0x91, 0x90, 0x48, 0x84, 0x75, 0x38, 0x83, 0x66,
    0x28, 0x82, 0x47, 0x74, 0x18, 0x81, 0x80, 0x08, 0x56, 0x37, 0x73, 0x65, 0x46, 0x27, 0x72, 0x64,
    0x55, 0x07, 0x17, 0x71, 0x70, 0x36, 0x63, 0x45, 0x54, 0x26, 0x62, 0x16, 0x61, 0x06, 0x60, 0x53,
    0x35, 0x44, 0x25, 0x52, 0x51, 0x15, 0x05, 0x34, 0x43, 0x50, 0x24, 0x42, 0x33, 0x14, 0x41, 0x04,
    0x40, 0x23, 0x32, 0x13, 0x31, 0x03, 0x30, 0x22, 0x12, 0x21, 0x02, 0x20, 0x11, 0x01, 0x10, 0x00,
};

const codebook24_symbols = [_]u8{
    0xEF, 0xFE, 0xDF, 0xFD, 0xCF, 0xFC, 0xBF, 0xFB, 0xFA, 0xAF, 0x9F, 0xF9, 0xF8, 0x8F, 0x7F, 0xF7,
    0x6F, 0xF6, 0x5F, 0xF5, 0x4F, 0xF4, 0x3F, 0xF3, 0x2F, 0xF2, 0xF1, 0x1F, 0xF0, 0x0F, 0xEE, 0xDE,
    0xED, 0xCE, 0xEC, 0xDD, 0xBE, 0xEB, 0xCD, 0xDC, 0xAE, 0xEA, 0xBD, 0xDB, 0xCC, 0x9E, 0xE9, 0xAD,
    0xDA, 0xBC, 0xCB, 0x8E, 0xE8, 0x9D, 0xD9, 0x7E, 0xE7, 0xAC, 0xFF, 0xCA, 0xBB, 0x8D, 0xD8, 0x0E,
    0xE0, 0x0D, 0xE6, 0x6E, 0x9C, 0xC9, 0x5E, 0xBA, 0xE5, 0xAB, 0x7D, 0xD7, 0xE4, 0x8C, 0xC8, 0x4E,
    0x2E, 0x3E, 0x6D, 0xD6, 0xE3, 0x9B, 0xB9, 0xAA, 0xE2, 0x1E, 0xE1, 0x5D, 0xD5, 0x7C, 0xC7, 0x4D,
    0x8B, 0xB8, 0xD4, 0x9A, 0xA9, 0x6C, 0xC6, 0x3D, 0xD3, 0x2D, 0xD2, 0x1D, 0x7B, 0xB7, 0xD1, 0x5C,
    0xC5, 0x8A, 0xA8, 0x99, 0x4C, 0xC4, 0x6B, 0xB6, 0xD0, 0x0C, 0x3C, 0xC3, 0x7A, 0xA7, 0x2C, 0xC2,
    0x5B, 0xB5, 0x1C, 0x89, 0x98, 0xC1, 0x4B, 0xC0, 0x0B, 0x3B, 0xB0, 0x0A, 0x1A, 0xB4, 0x6A, 0xA6,
    0x79, 0x97, 0xA0, 0x09, 0x90, 0xB3, 0x88, 0x2B, 0x5A, 0xB2, 0xA5, 0x1B, 0xB1, 0x69, 0x96, 0xA4,
    0x4A, 0x78, 0x87, 0x3A, 0xA3, 0x59, 0x95, 0x2A, 0xA2, 0xA1, 0x68, 0x86, 0x77, 0x49, 0x94, 0x39,
    0x93, 0x58, 0x85, 0x29, 0x67, 0x76, 0x92, 0x19, 0x91, 0x48, 0x84, 0x57, 0x75, 0x38, 0x83, 0x66,
    0x28, 0x82, 0x18, 0x47, 0x74, 0x81, 0x08, 0x80, 0x56, 0x65, 0x17, 0x07, 0x70, 0x73, 0x37, 0x27,
    0x72, 0x46, 0x64, 0x55, 0x71, 0x36, 0x63, 0x45, 0x54, 0x26, 0x62, 0x16, 0x61, 0x06, 0x60, 0x35,
    0x53, 0x44, 0x25, 0x52, 0x15, 0x05, 0x50, 0x51, 0x34, 0x43, 0x24, 0x42, 0x33, 0x14, 0x41, 0x04,
    0x40, 0x23, 0x32, 0x13, 0x31, 0x03, 0x30, 0x22, 0x12, 0x21, 0x02, 0x20, 0x11, 0x01, 0x10, 0x00,
};

const codebook1_lookup = buildSymbolLookup(&codebook1_bits, &codebook1_codes);
const codebook2_lookup = buildSymbolLookup(&codebook2_bits, &codebook2_codes);
const codebook3_lookup = buildSymbolLookup(&codebook3_bits, &codebook3_codes);
const codebook5_lookup = buildSymbolLookup(&codebook5_bits, &codebook5_codes);
const codebook6_lookup = buildSymbolLookup(&codebook6_bits, &codebook6_codes);
const codebook7_lookup = buildSymbolLookup(&codebook7_bits, &codebook7_codes);
const codebook8_lookup = buildSymbolLookup(&codebook8_bits, &codebook8_codes);
const codebook9_lookup = buildSymbolLookup(&codebook9_bits, &codebook9_codes);
const codebook10_lookup = buildSymbolLookup(&codebook10_bits, &codebook10_codes);
const codebook11_lookup = buildSymbolLookup(&codebook11_bits, &codebook11_codes);
const codebook12_lookup = buildSymbolLookup(&codebook12_bits, &codebook12_codes);
const codebook13_lookup = buildSymbolLookup(&codebook13_bits, &codebook13_codes);
const codebook15_lookup = buildSymbolLookup(&codebook15_bits, &codebook15_codes);
const codebook16_lookup = buildSymbolLookup(&codebook16_bits, &codebook16_codes);
const codebook24_lookup = buildSymbolLookup(&codebook24_bits, &codebook24_codes);
const quad0_lookup = buildQuadLookup(&quad_bits[0], &quad_codes[0]);
const quad1_lookup = buildQuadLookup(&quad_bits[1], &quad_codes[1]);

pub fn makeRegionPlan(header: bitstream.FrameHeader, info: bitstream.GranuleChannelInfo) RegionPlan {
    const big_values = @as(usize, info.big_values);
    const big_value_samples = big_values * 2;
    const sample_rate_index: usize = switch (header.sample_rate) {
        44100 => 0,
        48000 => 1,
        32000 => 2,
        22050 => 3,
        24000 => 4,
        16000 => 5,
        11025 => 6,
        12000 => 7,
        8000 => 8,
        else => 1,
    };

    var bounds = [_]usize{ big_values, big_values, big_values };
    if (info.window_switching_flag and info.block_type == 2) {
        const short_region_pairs: usize = if (sample_rate_index != 8) 18 else 36;
        bounds[0] = @min(big_values, short_region_pairs);
        bounds[1] = big_values;
        bounds[2] = big_values;
    } else {
        const long_bands = scalefactorBandLong(header.sample_rate);
        const region0_band = @min(@as(usize, info.region0_count) + 1, long_bands.len - 1);
        const region1_band = @min(region0_band + @as(usize, info.region1_count) + 1, long_bands.len - 1);
        bounds[0] = @min(big_values, long_bands[region0_band] / 2);
        bounds[1] = @min(big_values, long_bands[region1_band] / 2);
        bounds[2] = big_values;
    }

    const pair_count0 = bounds[0];
    const pair_count1 = bounds[1] - bounds[0];
    const pair_count2 = bounds[2] - bounds[1];

    return .{
        .big_values = big_values,
        .big_value_samples = big_value_samples,
        .region_sample_bounds = bounds,
        .region_pair_counts = .{ pair_count0, pair_count1, pair_count2 },
        .table_select = info.table_select,
        .count1_table_select = info.count1table_select,
    };
}

pub fn regionTasksForGranule(plan: RegionPlan) [3]RegionReader {
    var readers: [3]RegionReader = undefined;
    for (0..3) |region_index| {
        readers[region_index] = .{
            .pair_start = plan.regionPairStart(region_index),
            .pair_count = plan.region_pair_counts[region_index],
            .table_select = plan.table_select[region_index],
        };
    }

    return readers;
}

pub fn notePlanUsage(plan: RegionPlan, usage: *TableUsage) void {
    for (0..3) |region_index| {
        if (plan.region_pair_counts[region_index] == 0) continue;
        usage.note(plan.table_select[region_index]);
    }
}

pub fn decodeBigValuePairsPartial(
    reader: *BitSliceReader,
    plan: RegionPlan,
    out_pairs: []DecodedPair,
) !BigValueDecodeProgress {
    if (out_pairs.len < plan.big_values) return error.OutputTooSmall;

    const tasks = regionTasksForGranule(plan);
    var written: usize = 0;

    for (tasks) |task| {
        if (task.pair_count == 0) continue;
        switch (huff_data[task.table_select].vlc_table) {
            0 => {
                for (0..task.pair_count) |_| {
                    out_pairs[written] = .{ .x = 0, .y = 0 };
                    written += 1;
                }
            },
            1...15 => {
                for (0..task.pair_count) |_| {
                    out_pairs[written] = decodePair(reader, task.table_select) catch |err| switch (err) {
                        error.EndOfStream => return .{
                            .pairs_decoded = written,
                            .unsupported_table = null,
                        },
                        else => return err,
                    };
                    written += 1;
                }
            },
            else => return .{ .pairs_decoded = written, .unsupported_table = task.table_select },
        }
    }

    return .{
        .pairs_decoded = written,
        .unsupported_table = null,
    };
}

pub fn decodeCount1QuadsPartial(
    reader: *BitSliceReader,
    table_select: bool,
    out_quads: []DecodedQuad,
    max_samples: usize,
) !Count1DecodeProgress {
    const table_index: usize = if (table_select) 1 else 0;
    var written: usize = 0;
    var samples_written: usize = 0;

    while (written < out_quads.len and samples_written + 4 <= max_samples) {
        const quad = decodeQuad(reader, table_index) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        out_quads[written] = quad;
        written += 1;
        samples_written += 4;
    }

    return .{
        .quads_decoded = written,
        .samples_decoded = samples_written,
    };
}

pub fn decodePair(reader: *BitSliceReader, table_select: u8) !DecodedPair {
    if (table_select >= huff_data.len) return error.Mp3UnsupportedHuffmanTable;
    const desc = huff_data[table_select];
    return switch (desc.vlc_table) {
        0 => .{ .x = 0, .y = 0 },
        1 => decodeCodebookPair(reader, &codebook1_bits, &codebook1_codes, &codebook1_lookup, 2, desc.linbits),
        2 => decodeCodebookPair(reader, &codebook2_bits, &codebook2_codes, &codebook2_lookup, 3, desc.linbits),
        3 => decodeCodebookPair(reader, &codebook3_bits, &codebook3_codes, &codebook3_lookup, 3, desc.linbits),
        4 => decodeCodebookPair(reader, &codebook5_bits, &codebook5_codes, &codebook5_lookup, 4, desc.linbits),
        5 => decodeCodebookPair(reader, &codebook6_bits, &codebook6_codes, &codebook6_lookup, 4, desc.linbits),
        6 => decodeCodebookPair(reader, &codebook7_bits, &codebook7_codes, &codebook7_lookup, 6, desc.linbits),
        7 => decodeCodebookPair(reader, &codebook8_bits, &codebook8_codes, &codebook8_lookup, 6, desc.linbits),
        8 => decodeCodebookPair(reader, &codebook9_bits, &codebook9_codes, &codebook9_lookup, 6, desc.linbits),
        9 => decodeCodebookPair(reader, &codebook10_bits, &codebook10_codes, &codebook10_lookup, 8, desc.linbits),
        10 => decodeCodebookPair(reader, &codebook11_bits, &codebook11_codes, &codebook11_lookup, 8, desc.linbits),
        11 => decodeCodebookPair(reader, &codebook12_bits, &codebook12_codes, &codebook12_lookup, 8, desc.linbits),
        12 => decodeMappedCodebookPair(reader, &codebook13_bits, &codebook13_codes, &codebook13_lookup, desc.linbits),
        13 => decodeMappedCodebookPair(reader, &codebook15_bits, &codebook15_codes, &codebook15_lookup, desc.linbits),
        14 => decodeMappedCodebookPair(reader, &codebook16_bits, &codebook16_codes, &codebook16_lookup, desc.linbits),
        15 => decodeMappedCodebookPair(reader, &codebook24_bits, &codebook24_codes, &codebook24_lookup, desc.linbits),
        else => error.Mp3UnsupportedHuffmanTable,
    };
}

fn decodeCodebookPair(
    reader: *BitSliceReader,
    bits: []const u8,
    codes: []const u16,
    lookup: *const [symbol_lookup_size]SymbolLookupEntry,
    xsize: usize,
    linbits: u8,
) !DecodedPair {
    const symbol = try decodeCodebookSymbol(reader, bits, codes, lookup);
    var x: i16 = @intCast(symbol / xsize);
    var y: i16 = @intCast(symbol % xsize);

    if (x == xsize - 1 and linbits > 0) x += @intCast(try reader.readBits(@intCast(linbits)));
    if (x != 0 and (try reader.readBits(1)) != 0) x = -x;

    if (y == xsize - 1 and linbits > 0) y += @intCast(try reader.readBits(@intCast(linbits)));
    if (y != 0 and (try reader.readBits(1)) != 0) y = -y;

    return .{ .x = x, .y = y };
}

fn decodeMappedCodebookPair(
    reader: *BitSliceReader,
    bits: []const u8,
    codes: []const u16,
    lookup: *const [symbol_lookup_size]SymbolLookupEntry,
    linbits: u8,
) !DecodedPair {
    const symbol_index = try decodeCodebookSymbol(reader, bits, codes, lookup);
    var x: i16 = @intCast(symbol_index / 16);
    var y: i16 = @intCast(symbol_index % 16);

    if (x == 15 and linbits > 0) x += @intCast(try reader.readBits(@intCast(linbits)));
    if (x != 0 and (try reader.readBits(1)) != 0) x = -x;

    if (y == 15 and linbits > 0) y += @intCast(try reader.readBits(@intCast(linbits)));
    if (y != 0 and (try reader.readBits(1)) != 0) y = -y;

    return .{ .x = x, .y = y };
}

fn decodeCodebookSymbol(
    reader: *BitSliceReader,
    bits: []const u8,
    codes: []const u16,
    lookup: *const [symbol_lookup_size]SymbolLookupEntry,
) !usize {
    if (reader.remainingBits() >= symbol_lookup_bits) {
        const entry = lookup[@intCast(try reader.peekBits(symbol_lookup_bits))];
        if (entry.bits != 0) {
            _ = try reader.readBits(@intCast(entry.bits));
            return entry.symbol;
        }
    }

    var max_bits: u8 = 0;
    for (bits) |entry_bits| {
        if (entry_bits > max_bits) max_bits = entry_bits;
    }

    for (1..@as(usize, max_bits) + 1) |bit_len| {
        const peeked = try reader.peekBits(@intCast(bit_len));
        for (bits, codes, 0..) |entry_bits, entry_code, index| {
            if (entry_bits != bit_len) continue;
            if (peeked != entry_code) continue;
            _ = try reader.readBits(@intCast(bit_len));
            return index;
        }
    }
    return error.Mp3InvalidHuffmanCode;
}

fn decodeQuad(reader: *BitSliceReader, table_index: usize) !DecodedQuad {
    const lookup = switch (table_index) {
        0 => &quad0_lookup,
        1 => &quad1_lookup,
        else => unreachable,
    };
    const symbol = try decodeQuadSymbol(reader, quad_bits[table_index][0..], quad_codes[table_index][0..], lookup);

    var quad = DecodedQuad{
        .v = if ((symbol & 0b1000) != 0) 1 else 0,
        .w = if ((symbol & 0b0100) != 0) 1 else 0,
        .x = if ((symbol & 0b0010) != 0) 1 else 0,
        .y = if ((symbol & 0b0001) != 0) 1 else 0,
    };

    if (quad.v != 0 and (try reader.readBits(1)) != 0) quad.v = -1;
    if (quad.w != 0 and (try reader.readBits(1)) != 0) quad.w = -1;
    if (quad.x != 0 and (try reader.readBits(1)) != 0) quad.x = -1;
    if (quad.y != 0 and (try reader.readBits(1)) != 0) quad.y = -1;

    return quad;
}

fn decodeQuadSymbol(
    reader: *BitSliceReader,
    bits: []const u8,
    codes: []const u8,
    lookup: *const [quad_lookup_size]SymbolLookupEntry,
) !u8 {
    if (reader.remainingBits() >= quad_lookup_bits) {
        const entry = lookup[@intCast(try reader.peekBits(quad_lookup_bits))];
        if (entry.bits != 0) {
            _ = try reader.readBits(@intCast(entry.bits));
            return @intCast(entry.symbol);
        }
    }

    for (1..7) |bit_len| {
        const peeked = try reader.peekBits(@intCast(bit_len));
        for (bits, codes, 0..) |entry_bits, entry_code, index| {
            if (entry_bits != bit_len) continue;
            if (peeked != entry_code) continue;
            _ = try reader.readBits(@intCast(bit_len));
            return @intCast(index);
        }
    }
    return error.Mp3InvalidHuffmanCode;
}

fn scalefactorBandLong(sample_rate: u32) []const usize {
    return switch (sample_rate) {
        44100 => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 44, 52, 62, 74, 90, 110, 134, 162, 196, 238, 288, 342, 418, 576 },
        48000 => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 42, 50, 60, 72, 88, 106, 128, 156, 190, 230, 276, 330, 384, 576 },
        32000 => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 44, 54, 66, 82, 102, 126, 156, 194, 240, 296, 364, 448, 550, 576 },
        24000 => &.{ 0, 6, 12, 18, 24, 30, 36, 44, 54, 66, 80, 96, 114, 136, 162, 194, 232, 278, 330, 394, 464, 540, 576 },
        22050, 16000, 12000, 11025 => &.{ 0, 6, 12, 18, 24, 30, 36, 44, 54, 66, 80, 96, 116, 140, 168, 200, 238, 284, 336, 396, 464, 522, 576 },
        else => &.{ 0, 4, 8, 12, 16, 20, 24, 30, 36, 42, 50, 60, 72, 88, 106, 128, 156, 190, 230, 276, 330, 384, 576 },
    };
}

test "bit slice reader respects start bit and length" {
    var reader = BitSliceReader.init(&.{ 0b10110011, 0b01010101 }, 3, 9);
    try std.testing.expectEqual(@as(u32, 0b10011), try reader.readBits(5));
    try std.testing.expectEqual(@as(u32, 0b0101), try reader.readBits(4));
    try std.testing.expectError(error.EndOfStream, reader.readBits(1));
}

test "region plan for switched short blocks collapses to two regions" {
    const header = bitstream.FrameHeader{
        .version = .mpeg2,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 64,
        .sample_rate = 16000,
        .padding = false,
        .channel_mode = .mono,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 20,
        .global_gain = 0,
        .scalefac_compress = 0,
        .window_switching_flag = true,
        .block_type = 2,
        .mixed_block_flag = false,
        .table_select = .{ 7, 8, 0 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 8,
        .region1_count = 12,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    const plan = makeRegionPlan(header, info);
    try std.testing.expectEqual(@as(usize, 18), plan.region_sample_bounds[0]);
    try std.testing.expectEqual(@as(usize, 20), plan.region_sample_bounds[1]);
    try std.testing.expectEqual(@as(usize, 20), plan.region_sample_bounds[2]);
}

test "region plan for long blocks is monotonic" {
    const header = bitstream.FrameHeader{
        .version = .mpeg1,
        .layer = .layer3,
        .has_crc = false,
        .free_format = false,
        .bitrate_kbps = 128,
        .sample_rate = 44100,
        .padding = false,
        .channel_mode = .stereo,
    };
    const info = bitstream.GranuleChannelInfo{
        .part2_3_length = 100,
        .big_values = 100,
        .global_gain = 0,
        .scalefac_compress = 0,
        .window_switching_flag = false,
        .block_type = 0,
        .mixed_block_flag = false,
        .table_select = .{ 5, 6, 7 },
        .subblock_gain = .{ 0, 0, 0 },
        .region0_count = 7,
        .region1_count = 13,
        .preflag = false,
        .scalefac_scale = false,
        .count1table_select = false,
    };
    const plan = makeRegionPlan(header, info);
    try std.testing.expect(plan.region_sample_bounds[0] <= plan.region_sample_bounds[1]);
    try std.testing.expect(plan.region_sample_bounds[1] <= plan.region_sample_bounds[2]);
    try std.testing.expectEqual(plan.big_values, plan.totalBigValuePairs());
}

test "region tasks cover declared pair counts" {
    const plan = RegionPlan{
        .big_values = 20,
        .big_value_samples = 40,
        .region_sample_bounds = .{ 10, 26, 40 },
        .region_pair_counts = .{ 5, 8, 7 },
        .table_select = .{ 3, 5, 7 },
        .count1_table_select = false,
    };
    const tasks = regionTasksForGranule(plan);
    try std.testing.expectEqual(@as(usize, 0), tasks[0].pair_start);
    try std.testing.expectEqual(@as(usize, 5), tasks[0].pair_count);
    try std.testing.expectEqual(@as(usize, 5), tasks[1].pair_start);
    try std.testing.expectEqual(@as(usize, 8), tasks[1].pair_count);
    try std.testing.expectEqual(@as(usize, 13), tasks[2].pair_start);
    try std.testing.expectEqual(@as(usize, 7), tasks[2].pair_count);
}

test "big value partial decode supports table zero" {
    const plan = RegionPlan{
        .big_values = 4,
        .big_value_samples = 8,
        .region_sample_bounds = .{ 4, 8, 8 },
        .region_pair_counts = .{ 2, 2, 0 },
        .table_select = .{ 0, 0, 0 },
        .count1_table_select = false,
    };
    var reader = BitSliceReader.init(&.{ 0xFF, 0x00 }, 0, 16);
    var pairs = [_]DecodedPair{.{ .x = -1, .y = -1 }} ** 4;
    const progress = try decodeBigValuePairsPartial(&reader, plan, &pairs);
    try std.testing.expectEqual(@as(usize, 4), progress.pairs_decoded);
    try std.testing.expect(progress.unsupported_table == null);
    for (pairs) |pair| {
        try std.testing.expectEqual(@as(i16, 0), pair.x);
        try std.testing.expectEqual(@as(i16, 0), pair.y);
    }
    try std.testing.expectEqual(@as(usize, 0), reader.bit_offset);
}

test "big value partial decode reports first unsupported table" {
    const plan = RegionPlan{
        .big_values = 5,
        .big_value_samples = 10,
        .region_sample_bounds = .{ 4, 10, 10 },
        .region_pair_counts = .{ 2, 3, 0 },
        .table_select = .{ 0, 27, 0 },
        .count1_table_select = false,
    };
    var reader = BitSliceReader.init(&.{0}, 0, 8);
    var pairs = [_]DecodedPair{.{ .x = -1, .y = -1 }} ** 5;
    const progress = try decodeBigValuePairsPartial(&reader, plan, &pairs);
    try std.testing.expectEqual(@as(usize, 2), progress.pairs_decoded);
    try std.testing.expectEqual(@as(?u8, 27), progress.unsupported_table);
}

test "codebook24 family decodes zero pair without sign bits" {
    var reader = BitSliceReader.init(&.{0b11110000}, 0, 4);
    const pair = try decodePair(&reader, 24);
    try std.testing.expectEqual(DecodedPair{ .x = 0, .y = 0 }, pair);
    try std.testing.expectEqual(@as(usize, 4), reader.bit_offset);
}

test "codebook24 family decodes linbits and sign bits" {
    var reader = BitSliceReader.init(&.{ 0b00110010, 0b00011000 }, 0, 14);
    const pair = try decodePair(&reader, 24);
    try std.testing.expectEqual(DecodedPair{ .x = -17, .y = 16 }, pair);
    try std.testing.expectEqual(@as(usize, 14), reader.bit_offset);
}

test "table 15 codebook decodes zero pair without sign bits" {
    var reader = BitSliceReader.init(&.{0b11100000}, 0, 3);
    const pair = try decodePair(&reader, 15);
    try std.testing.expectEqual(DecodedPair{ .x = 0, .y = 0 }, pair);
    try std.testing.expectEqual(@as(usize, 3), reader.bit_offset);
}

test "table 15 codebook decodes signed pair" {
    var reader = BitSliceReader.init(&.{0b11011100}, 0, 6);
    const pair = try decodePair(&reader, 15);
    try std.testing.expectEqual(DecodedPair{ .x = -1, .y = 1 }, pair);
    try std.testing.expectEqual(@as(usize, 6), reader.bit_offset);
}

test "table 2 codebook decodes signed pair" {
    var reader = BitSliceReader.init(&.{0b10010000}, 0, 5);
    const pair = try decodePair(&reader, 2);
    try std.testing.expectEqual(DecodedPair{ .x = -1, .y = 1 }, pair);
    try std.testing.expectEqual(@as(usize, 5), reader.bit_offset);
}

test "table 1 codebook decodes signed pair" {
    var reader = BitSliceReader.init(&.{0b01100000}, 0, 4);
    const pair = try decodePair(&reader, 1);
    try std.testing.expectEqual(DecodedPair{ .x = 1, .y = -1 }, pair);
    try std.testing.expectEqual(@as(usize, 4), reader.bit_offset);
}

test "table 7 codebook decodes signed pair" {
    var reader = BitSliceReader.init(&.{0b01011100}, 0, 6);
    const pair = try decodePair(&reader, 7);
    try std.testing.expectEqual(DecodedPair{ .x = -1, .y = 1 }, pair);
    try std.testing.expectEqual(@as(usize, 6), reader.bit_offset);
}

test "table 10 codebook decodes zero pair" {
    var reader = BitSliceReader.init(&.{0b10000000}, 0, 1);
    const pair = try decodePair(&reader, 11);
    try std.testing.expectEqual(DecodedPair{ .x = 0, .y = 0 }, pair);
    try std.testing.expectEqual(@as(usize, 1), reader.bit_offset);
}

test "table 12 codebook decodes zero pair" {
    var reader = BitSliceReader.init(&.{0b10010000}, 0, 4);
    const pair = try decodePair(&reader, 12);
    try std.testing.expectEqual(DecodedPair{ .x = 0, .y = 0 }, pair);
    try std.testing.expectEqual(@as(usize, 4), reader.bit_offset);
}

test "table 13 codebook decodes zero pair" {
    var reader = BitSliceReader.init(&.{0b10000000}, 0, 1);
    const pair = try decodePair(&reader, 13);
    try std.testing.expectEqual(DecodedPair{ .x = 0, .y = 0 }, pair);
    try std.testing.expectEqual(@as(usize, 1), reader.bit_offset);
}

test "table 16 family decodes zero pair" {
    var reader = BitSliceReader.init(&.{0b10000000}, 0, 1);
    const pair = try decodePair(&reader, 16);
    try std.testing.expectEqual(DecodedPair{ .x = 0, .y = 0 }, pair);
    try std.testing.expectEqual(@as(usize, 1), reader.bit_offset);
}

test "count1 table A decodes signed quad" {
    var reader = BitSliceReader.init(&.{0b01111000}, 0, 5);
    var quads = [_]DecodedQuad{.{ .v = 0, .w = 0, .x = 0, .y = 0 }} ** 1;
    const progress = try decodeCount1QuadsPartial(&reader, false, &quads, 4);
    try std.testing.expectEqual(@as(usize, 1), progress.quads_decoded);
    try std.testing.expectEqual(@as(usize, 4), progress.samples_decoded);
    try std.testing.expectEqual(DecodedQuad{ .v = -1, .w = 0, .x = 0, .y = 0 }, quads[0]);
}

test "count1 table B decodes full quad" {
    var reader = BitSliceReader.init(&.{0b11110000}, 0, 4);
    var quads = [_]DecodedQuad{.{ .v = 0, .w = 0, .x = 0, .y = 0 }} ** 1;
    const progress = try decodeCount1QuadsPartial(&reader, true, &quads, 4);
    try std.testing.expectEqual(@as(usize, 1), progress.quads_decoded);
    try std.testing.expectEqual(@as(usize, 4), progress.samples_decoded);
    try std.testing.expectEqual(DecodedQuad{ .v = 0, .w = 0, .x = 0, .y = 0 }, quads[0]);
}
