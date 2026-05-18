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
const test_support = @import("test_support.zig");

const Allocator = std.mem.Allocator;
const max_components = 4;
const max_quant_tables = 4;
const max_huffman_tables = 4;
const max_arithmetic_tables = 4;
const max_scans = 32;
const max_huffman_symbols = 256;
const max_component_blocks = 16;
const arithmetic_dc_stat_bins = 64;
const arithmetic_ac_stat_bins = 256;

pub fn hasSignature(bytes: []const u8) bool {
    return bytes.len >= 3 and bytes[0] == 0xFF and bytes[1] == 0xD8 and bytes[2] == 0xFF;
}

pub const FrameKind = enum {
    baseline_dct,
    extended_sequential_dct,
    progressive_dct,
    lossless,
    differential_sequential_dct,
    differential_progressive_dct,
    differential_lossless,
    arithmetic_sequential_dct,
    arithmetic_progressive_dct,
    arithmetic_lossless,
};

pub const ComponentInfo = struct {
    id: u8,
    horizontal_sampling: u8,
    vertical_sampling: u8,
    quant_table_id: u8,
};

pub const Info = struct {
    width: u32,
    height: u32,
    bits_per_sample: u8,
    component_count: u8,
    frame_kind: FrameKind,
    components: [max_components]ComponentInfo,
};

pub const QuantTableInfo = struct {
    present: bool = false,
    precision_bits: u8 = 0,
    zig_zag_values: [64]u16 = std.mem.zeroes([64]u16),
};

pub const HuffmanTableInfo = struct {
    present: bool = false,
    symbol_count: u16 = 0,
    code_counts: [16]u8 = std.mem.zeroes([16]u8),
    symbols: [max_huffman_symbols]u8 = std.mem.zeroes([max_huffman_symbols]u8),
};

pub const CanonicalHuffmanTable = struct {
    symbol_count: u16,
    code_counts: [16]u8,
    first_code: [16]u16,
    first_symbol_index: [16]u16,
    symbols: [max_huffman_symbols]u8,
};

pub const EntropyBitReader = struct {
    bytes: []const u8,
    cursor: usize = 0,
    bit_buffer: u32 = 0,
    bit_count: u8 = 0,
    pending_marker: ?u8 = null,

    pub fn init(bytes: []const u8) EntropyBitReader {
        return .{ .bytes = bytes };
    }

    pub fn readBit(self: *EntropyBitReader) !u1 {
        return @intCast(try self.readBits(1));
    }

    pub fn readBits(self: *EntropyBitReader, bit_len: u8) !u16 {
        if (bit_len == 0 or bit_len > 16) return error.JpegDecodeFailed;
        try self.ensureBits(bit_len);

        self.bit_count -= bit_len;
        const shift: u5 = @intCast(self.bit_count);
        const mask = (@as(u32, 1) << @as(u5, @intCast(bit_len))) - 1;
        return @intCast((self.bit_buffer >> shift) & mask);
    }

    pub fn byteAlign(self: *EntropyBitReader) void {
        self.bit_count -= self.bit_count % 8;
    }

    pub fn peekPendingMarker(self: EntropyBitReader) ?u8 {
        return self.pending_marker;
    }

    pub fn consumeExpectedRestartMarker(self: *EntropyBitReader, expected_marker: u8) !void {
        self.bit_buffer = 0;
        self.bit_count = 0;

        const marker = if (self.pending_marker) |pending| blk: {
            self.pending_marker = null;
            break :blk pending;
        } else try self.readMarkerFromStream();

        if (marker != expected_marker) return error.JpegDecodeFailed;
    }

    fn ensureBits(self: *EntropyBitReader, bit_len: u8) !void {
        while (self.bit_count < bit_len) {
            const next_byte = try self.readEntropyByte();
            self.bit_buffer = (self.bit_buffer << 8) | next_byte;
            self.bit_count += 8;
        }
    }

    fn readEntropyByte(self: *EntropyBitReader) !u8 {
        if (self.pending_marker != null) return error.JpegMarkerReached;
        if (self.cursor >= self.bytes.len) return error.JpegDecodeFailed;

        const byte = self.bytes[self.cursor];
        self.cursor += 1;
        if (byte != 0xff) return byte;

        if (self.cursor >= self.bytes.len) return error.JpegDecodeFailed;
        var marker = self.bytes[self.cursor];
        self.cursor += 1;
        while (marker == 0xff) {
            if (self.cursor >= self.bytes.len) return error.JpegDecodeFailed;
            marker = self.bytes[self.cursor];
            self.cursor += 1;
        }

        if (marker == 0x00) return 0xff;
        self.pending_marker = marker;
        return error.JpegMarkerReached;
    }

    fn readMarkerFromStream(self: *EntropyBitReader) !u8 {
        if (self.cursor >= self.bytes.len) return error.JpegDecodeFailed;
        if (self.bytes[self.cursor] != 0xff) return error.JpegDecodeFailed;

        self.cursor += 1;
        while (self.cursor < self.bytes.len and self.bytes[self.cursor] == 0xff) {
            self.cursor += 1;
        }
        if (self.cursor >= self.bytes.len) return error.JpegDecodeFailed;

        const marker = self.bytes[self.cursor];
        self.cursor += 1;
        if (marker == 0x00) return error.JpegDecodeFailed;
        return marker;
    }
};

pub const ScanComponentInfo = struct {
    component_selector: u8,
    dc_table_id: u8,
    ac_table_id: u8,
};

pub const ScanInfo = struct {
    component_count: u8,
    components: [max_components]ScanComponentInfo,
    spectral_start: u8,
    spectral_end: u8,
    successive_approx_high: u8,
    successive_approx_low: u8,
    dc_tables: [max_huffman_tables]HuffmanTableInfo = std.mem.zeroes([max_huffman_tables]HuffmanTableInfo),
    ac_tables: [max_huffman_tables]HuffmanTableInfo = std.mem.zeroes([max_huffman_tables]HuffmanTableInfo),
    entropy_start: usize = 0,
    entropy_end: usize = 0,
};

pub const Structure = struct {
    info: Info,
    quant_tables: [max_quant_tables]QuantTableInfo,
    huffman_dc_tables: [max_huffman_tables]HuffmanTableInfo,
    huffman_ac_tables: [max_huffman_tables]HuffmanTableInfo,
    arithmetic_dc_l: [max_arithmetic_tables]u8,
    arithmetic_dc_u: [max_arithmetic_tables]u8,
    arithmetic_ac_k: [max_arithmetic_tables]u8,
    adobe_transform: ?u8,
    restart_interval: ?u16,
    scan_count: u8,
    scans: [max_scans]ScanInfo,
};

pub const DecodedImage = struct {
    rgba: []u8,
    width: u32,
    height: u32,
};

const SamplePlane = struct {
    width: usize = 0,
    height: usize = 0,
    samples: []u16 = &.{},
};

const ArithmeticDecoder = struct {
    bytes: []const u8,
    cursor: usize = 0,
    c: u64 = 0,
    a: u64 = 0,
    ct: i32 = -16,
    unread_marker: ?u8 = null,

    fn init(bytes: []const u8) ArithmeticDecoder {
        return .{ .bytes = bytes };
    }

    fn resetState(self: *ArithmeticDecoder) void {
        self.c = 0;
        self.a = 0;
        self.ct = -16;
        self.unread_marker = null;
    }

    fn consumeExpectedRestartMarker(self: *ArithmeticDecoder, expected_marker: u8) !void {
        const marker = if (self.unread_marker) |pending| blk: {
            self.unread_marker = null;
            break :blk pending;
        } else try self.readMarkerFromStream();

        if (marker != expected_marker) return error.JpegDecodeFailed;
        self.resetState();
    }

    fn readMarkerFromStream(self: *ArithmeticDecoder) !u8 {
        if (self.cursor >= self.bytes.len) return error.JpegDecodeFailed;

        while (self.cursor < self.bytes.len and self.bytes[self.cursor] == 0xff) {
            self.cursor += 1;
        }
        if (self.cursor >= self.bytes.len) return error.JpegDecodeFailed;

        const marker = self.bytes[self.cursor];
        self.cursor += 1;
        if (marker == 0x00) return error.JpegDecodeFailed;
        return marker;
    }
};

const ColorEncoding = enum {
    rgb,
    ycbcr,
    cmyk,
    ycck,
};

pub const BaselineBlockDecode = struct {
    coefficients: [64]i64,
    dc_predictor: i64,
};

pub const ProgressiveBlockState = struct {
    coefficients: [64]i16 = std.mem.zeroes([64]i16),
    dc_predictor: i16 = 0,
};

const ProgressiveDcScanComponent = struct {
    frame_index: usize,
    component: ComponentInfo,
    dc_table: CanonicalHuffmanTable,
    states: []ProgressiveBlockState,
    blocks_x: usize,
    blocks_y: usize,
};

const ArithmeticProgressiveDcScanComponent = struct {
    frame_index: usize,
    component: ComponentInfo,
    dc_table_id: u8,
    states: []ProgressiveBlockState,
    blocks_x: usize,
    blocks_y: usize,
};

const ProgressiveDecodeContext = struct {
    max_h: u8,
    max_v: u8,
    mcu_cols: usize,
    mcu_rows: usize,
    component_states: [max_components][]ProgressiveBlockState,
    component_blocks_x: [max_components]usize,
    component_blocks_y: [max_components]usize,
    component_actual_blocks_x: [max_components]usize,
    component_actual_blocks_y: [max_components]usize,
    component_quant_tables: [max_components][64]u16,
    allocated_components: usize,

    fn init(alloc: Allocator, structure: Structure) !ProgressiveDecodeContext {
        var ctx = ProgressiveDecodeContext{
            .max_h = 1,
            .max_v = 1,
            .mcu_cols = 0,
            .mcu_rows = 0,
            .component_states = std.mem.zeroes([max_components][]ProgressiveBlockState),
            .component_blocks_x = [_]usize{0} ** max_components,
            .component_blocks_y = [_]usize{0} ** max_components,
            .component_actual_blocks_x = [_]usize{0} ** max_components,
            .component_actual_blocks_y = [_]usize{0} ** max_components,
            .component_quant_tables = undefined,
            .allocated_components = 0,
        };

        for (0..structure.info.component_count) |frame_index| {
            const component = structure.info.components[frame_index];
            if (component.horizontal_sampling > ctx.max_h) ctx.max_h = component.horizontal_sampling;
            if (component.vertical_sampling > ctx.max_v) ctx.max_v = component.vertical_sampling;
        }

        ctx.mcu_cols = @divFloor(
            @as(usize, structure.info.width) + (@as(usize, ctx.max_h) * 8 - 1),
            @as(usize, ctx.max_h) * 8,
        );
        ctx.mcu_rows = @divFloor(
            @as(usize, structure.info.height) + (@as(usize, ctx.max_v) * 8 - 1),
            @as(usize, ctx.max_v) * 8,
        );

        for (0..structure.info.component_count) |frame_index| {
            const component = structure.info.components[frame_index];
            ctx.component_actual_blocks_x[frame_index] = @divFloor(
                @as(usize, structure.info.width) * @as(usize, component.horizontal_sampling) + (@as(usize, ctx.max_h) * 8 - 1),
                @as(usize, ctx.max_h) * 8,
            );
            ctx.component_actual_blocks_y[frame_index] = @divFloor(
                @as(usize, structure.info.height) * @as(usize, component.vertical_sampling) + (@as(usize, ctx.max_v) * 8 - 1),
                @as(usize, ctx.max_v) * 8,
            );
            // Progressive scans traverse blocks in padded MCU order, not just the
            // cropped visible image grid. Keep state for the full entropy-coded block
            // lattice so scan traversal stays aligned on small subsampled images.
            ctx.component_blocks_x[frame_index] = ctx.mcu_cols * @as(usize, component.horizontal_sampling);
            ctx.component_blocks_y[frame_index] = ctx.mcu_rows * @as(usize, component.vertical_sampling);
            ctx.component_quant_tables[frame_index] = quantTableNaturalOrder(structure.quant_tables[component.quant_table_id]);

            const total_blocks = ctx.component_blocks_x[frame_index] * ctx.component_blocks_y[frame_index];
            ctx.component_states[frame_index] = try alloc.alloc(ProgressiveBlockState, total_blocks);
            @memset(ctx.component_states[frame_index], ProgressiveBlockState{});
            ctx.allocated_components += 1;
        }

        return ctx;
    }

    fn deinit(self: *ProgressiveDecodeContext, alloc: Allocator) void {
        for (0..self.allocated_components) |i| {
            alloc.free(self.component_states[i]);
        }
    }
};

pub fn probe(jpeg_bytes: []const u8) !Info {
    return (try parseStructure(jpeg_bytes)).info;
}

pub fn parseStructure(jpeg_bytes: []const u8) !Structure {
    if (jpeg_bytes.len < 4) return error.JpegDecodeFailed;
    if (jpeg_bytes[0] != 0xff or jpeg_bytes[1] != 0xd8) return error.JpegDecodeFailed;

    var structure = Structure{
        .info = undefined,
        .quant_tables = std.mem.zeroes([max_quant_tables]QuantTableInfo),
        .huffman_dc_tables = std.mem.zeroes([max_huffman_tables]HuffmanTableInfo),
        .huffman_ac_tables = std.mem.zeroes([max_huffman_tables]HuffmanTableInfo),
        .arithmetic_dc_l = [_]u8{0} ** max_arithmetic_tables,
        .arithmetic_dc_u = [_]u8{1} ** max_arithmetic_tables,
        .arithmetic_ac_k = [_]u8{5} ** max_arithmetic_tables,
        .adobe_transform = null,
        .restart_interval = null,
        .scan_count = 0,
        .scans = std.mem.zeroes([max_scans]ScanInfo),
    };
    var have_frame = false;

    var cursor: usize = 2;
    while (cursor < jpeg_bytes.len) {
        while (cursor < jpeg_bytes.len and jpeg_bytes[cursor] == 0xff) {
            cursor += 1;
        }
        if (cursor >= jpeg_bytes.len) return error.JpegDecodeFailed;

        const marker = jpeg_bytes[cursor];
        cursor += 1;

        if (marker == 0x00) continue;
        if (marker == 0xd9) break;
        if (isStandaloneMarker(marker)) continue;

        if (cursor + 2 > jpeg_bytes.len) return error.JpegDecodeFailed;
        const segment_len = std.mem.readInt(u16, jpeg_bytes[cursor..][0..2], .big);
        if (segment_len < 2) return error.JpegDecodeFailed;
        const segment_end = cursor + segment_len;
        if (segment_end > jpeg_bytes.len) return error.JpegDecodeFailed;
        const segment = jpeg_bytes[cursor + 2 .. segment_end];

        if (frameKindForMarker(marker)) |frame_kind| {
            structure.info = try parseFrameSegment(frame_kind, segment);
            have_frame = true;
            cursor = segment_end;
            continue;
        }

        switch (marker) {
            0xdb => try parseQuantizationSegment(&structure.quant_tables, segment),
            0xc4 => try parseHuffmanSegment(&structure.huffman_dc_tables, &structure.huffman_ac_tables, segment),
            0xcc => try parseArithmeticConditioningSegment(&structure.arithmetic_dc_l, &structure.arithmetic_dc_u, &structure.arithmetic_ac_k, segment),
            0xdd => structure.restart_interval = try parseRestartIntervalSegment(segment),
            0xee => structure.adobe_transform = parseAdobeApp14Transform(segment),
            0xda => {
                if (!have_frame) return error.JpegDecodeFailed;
                if (structure.scan_count >= max_scans) return error.UnsupportedJpegFormat;
                const entropy_start = segment_end;
                const entropy_end = try skipEntropyData(jpeg_bytes, segment_end);
                var scan = try parseScanSegment(segment);
                scan.dc_tables = structure.huffman_dc_tables;
                scan.ac_tables = structure.huffman_ac_tables;
                scan.entropy_start = entropy_start;
                scan.entropy_end = entropy_end;
                structure.scans[structure.scan_count] = scan;
                structure.scan_count += 1;
                cursor = entropy_end;
                continue;
            },
            else => {},
        }

        cursor = segment_end;
    }

    if (!have_frame) return error.JpegDecodeFailed;
    return structure;
}

pub fn supportsPlannedBaselineDecode(structure: Structure) bool {
    if (structure.info.frame_kind != .baseline_dct and structure.info.frame_kind != .extended_sequential_dct) return false;
    if (structure.info.frame_kind == .baseline_dct) {
        if (structure.info.bits_per_sample != 8) return false;
    } else {
        if (structure.info.bits_per_sample < 8 or structure.info.bits_per_sample > 12) return false;
    }
    if (structure.info.component_count != 1 and structure.info.component_count != 3 and structure.info.component_count != 4) return false;
    if (structure.scan_count != 1) return false;

    const scan = structure.scans[0];
    if (scan.component_count != structure.info.component_count) return false;
    if (scan.spectral_start != 0 or scan.spectral_end != 63) return false;
    if (scan.successive_approx_high != 0 or scan.successive_approx_low != 0) return false;

    for (0..structure.info.component_count) |i| {
        const frame_component = structure.info.components[i];
        const scan_component = scan.components[i];

        if (frame_component.quant_table_id >= max_quant_tables) return false;
        if (!structure.quant_tables[frame_component.quant_table_id].present) return false;
        const precision_bits = structure.quant_tables[frame_component.quant_table_id].precision_bits;
        if (precision_bits != 8 and precision_bits != 16) return false;
        if (scan_component.component_selector != frame_component.id) return false;
        if (!scan.dc_tables[scan_component.dc_table_id].present) return false;
        if (!scan.ac_tables[scan_component.ac_table_id].present) return false;
    }

    return true;
}

fn adobeTransformImpliesColorEncoding(transform: u8) ?ColorEncoding {
    return switch (transform) {
        0 => .cmyk,
        2 => .ycck,
        else => null,
    };
}

fn hasCmykComponentIds(info: Info) bool {
    return info.component_count == 4 and
        info.components[0].id == 'C' and
        info.components[1].id == 'M' and
        info.components[2].id == 'Y' and
        info.components[3].id == 'K';
}

fn colorEncodingForStructure(structure: Structure) ?ColorEncoding {
    return switch (structure.info.component_count) {
        3 => if (structure.info.components[0].id == 1 and structure.info.components[1].id == 2 and structure.info.components[2].id == 3)
            .ycbcr
        else if (structure.info.components[0].id == 'R' and structure.info.components[1].id == 'G' and structure.info.components[2].id == 'B')
            .rgb
        else
            null,
        4 => if (structure.adobe_transform) |transform|
            adobeTransformImpliesColorEncoding(transform)
        else if (hasCmykComponentIds(structure.info))
            .cmyk
        else
            null,
        else => null,
    };
}

pub fn buildCanonicalHuffmanTable(info: HuffmanTableInfo) !CanonicalHuffmanTable {
    if (!info.present or info.symbol_count == 0) return error.JpegDecodeFailed;

    var table = CanonicalHuffmanTable{
        .symbol_count = info.symbol_count,
        .code_counts = info.code_counts,
        .first_code = std.mem.zeroes([16]u16),
        .first_symbol_index = std.mem.zeroes([16]u16),
        .symbols = info.symbols,
    };

    var next_code: u32 = 0;
    var symbol_index: u16 = 0;
    for (0..16) |i| {
        const code_count = table.code_counts[i];
        const code_width = @as(u5, @intCast(i + 1));
        const max_codes_for_width = @as(u32, 1) << code_width;

        table.first_code[i] = @intCast(next_code);
        table.first_symbol_index[i] = symbol_index;

        if (next_code + code_count > max_codes_for_width) return error.JpegDecodeFailed;
        symbol_index += code_count;
        next_code = (next_code + code_count) << 1;
    }

    if (symbol_index != info.symbol_count) return error.JpegDecodeFailed;
    return table;
}

pub fn quantTableNaturalOrder(table: QuantTableInfo) [64]u16 {
    var natural = std.mem.zeroes([64]u16);
    for (zig_zag_to_natural, 0..) |natural_index, zig_zag_index| {
        natural[natural_index] = table.zig_zag_values[zig_zag_index];
    }
    return natural;
}

pub fn decodeHuffmanSymbol(reader: *EntropyBitReader, table: CanonicalHuffmanTable) !u8 {
    var code: u16 = 0;
    for (0..16) |i| {
        code = (code << 1) | try reader.readBit();
        const code_count = table.code_counts[i];
        if (code_count == 0) continue;

        const first_code = table.first_code[i];
        if (code < first_code or code >= first_code + code_count) continue;

        const symbol_offset = code - first_code;
        const symbol_index = table.first_symbol_index[i] + symbol_offset;
        if (symbol_index >= table.symbol_count) return error.JpegDecodeFailed;
        return table.symbols[symbol_index];
    }

    return error.JpegDecodeFailed;
}

pub fn extendSign(value: u16, bit_len: u8) !i16 {
    if (bit_len == 0) return 0;
    const threshold = @as(u16, 1) << @intCast(bit_len - 1);
    if (value >= threshold) return checkedI16FromI32(value);
    const signed = @as(i32, @intCast(value)) + 1 - (@as(i32, 1) << @intCast(bit_len));
    return checkedI16FromI32(signed);
}

fn extendSignI32(value: u16, bit_len: u8) !i32 {
    if (bit_len == 0) return 0;
    const threshold = @as(u16, 1) << @intCast(bit_len - 1);
    if (value >= threshold) return value;
    return @as(i32, value) + 1 - (@as(i32, 1) << @intCast(bit_len));
}

pub fn scanEntropyBytes(structure: Structure, jpeg_bytes: []const u8, scan_index: usize) ![]const u8 {
    if (scan_index >= structure.scan_count) return error.JpegDecodeFailed;
    const scan = structure.scans[scan_index];
    if (scan.entropy_start > scan.entropy_end or scan.entropy_end > jpeg_bytes.len) return error.JpegDecodeFailed;
    return jpeg_bytes[scan.entropy_start..scan.entropy_end];
}

fn checkedAddI16(a: i16, b: i16) !i16 {
    const widened = @as(i32, a) + @as(i32, b);
    if (widened < std.math.minInt(i16) or widened > std.math.maxInt(i16)) {
        return error.JpegDecodeFailed;
    }
    return @intCast(widened);
}

fn checkedAddI32(a: i32, b: i32) !i32 {
    const widened = @as(i64, a) + @as(i64, b);
    if (widened < std.math.minInt(i32) or widened > std.math.maxInt(i32)) {
        return error.JpegDecodeFailed;
    }
    return @intCast(widened);
}

fn checkedAddI64(a: i64, b: i32) !i64 {
    const widened = @as(i128, a) + @as(i128, b);
    if (widened < std.math.minInt(i64) or widened > std.math.maxInt(i64)) {
        return error.JpegDecodeFailed;
    }
    return @intCast(widened);
}

fn checkedShiftLeftI16(value: i16, shift: u8) !i16 {
    if (shift >= @bitSizeOf(i16)) return error.JpegDecodeFailed;
    const widened = @as(i32, value) * (@as(i32, 1) << @intCast(shift));
    if (widened < std.math.minInt(i16) or widened > std.math.maxInt(i16)) {
        return error.JpegDecodeFailed;
    }
    return @intCast(widened);
}

fn checkedI16FromI32(value: i32) !i16 {
    if (value < std.math.minInt(i16) or value > std.math.maxInt(i16)) {
        return error.JpegDecodeFailed;
    }
    return @intCast(value);
}

fn wrapArithmeticDcPredictor(predictor: i32, diff: i32) i32 {
    const sum = predictor + diff;
    const wrapped: u16 = @truncate(@as(u32, @bitCast(sum)));
    return wrapped;
}

fn arithmeticPredictorToCoefficient(predictor: i32) i16 {
    const wrapped: u16 = @truncate(@as(u32, @bitCast(predictor)));
    return @bitCast(wrapped);
}

pub fn decodeBaselineBlock(
    reader: *EntropyBitReader,
    dc_table: CanonicalHuffmanTable,
    ac_table: CanonicalHuffmanTable,
    prev_dc: i64,
) !BaselineBlockDecode {
    var coefficients = std.mem.zeroes([64]i64);

    const dc_size = try decodeHuffmanSymbol(reader, dc_table);
    const dc_delta = if (dc_size == 0) 0 else try extendSignI32(try reader.readBits(dc_size), dc_size);
    const dc_value = try checkedAddI64(prev_dc, dc_delta);
    coefficients[0] = dc_value;

    var zig_zag_index: usize = 1;
    while (zig_zag_index < 64) {
        const symbol = try decodeHuffmanSymbol(reader, ac_table);
        if (symbol == 0x00) break;
        if (symbol == 0xf0) {
            zig_zag_index += 16;
            continue;
        }

        const run_length = symbol >> 4;
        const value_bits = symbol & 0x0f;
        if (value_bits == 0) return error.JpegDecodeFailed;

        zig_zag_index += run_length;
        if (zig_zag_index >= 64) return error.JpegDecodeFailed;

        const coeff_value = @as(i64, try extendSignI32(try reader.readBits(value_bits), value_bits));
        coefficients[zig_zag_to_natural[zig_zag_index]] = coeff_value;
        zig_zag_index += 1;
    }

    return .{
        .coefficients = coefficients,
        .dc_predictor = dc_value,
    };
}

fn decodeLosslessDifference(reader: *EntropyBitReader, dc_table: CanonicalHuffmanTable) !i32 {
    const bit_len = try decodeHuffmanSymbol(reader, dc_table);
    if (bit_len == 0) return 0;
    // In lossless Huffman JPEG, category 16 is a special fixed difference.
    if (bit_len == 16) return 32768;
    if (bit_len > 15) return error.JpegDecodeFailed;
    const bits = try reader.readBits(bit_len);
    return try extendSignI32(bits, bit_len);
}

pub fn decodeProgressiveDcFirst(
    reader: *EntropyBitReader,
    dc_table: CanonicalHuffmanTable,
    prev_dc: i16,
    successive_low: u8,
) !ProgressiveBlockState {
    const dc_size = try decodeHuffmanSymbol(reader, dc_table);
    const dc_delta = if (dc_size == 0) 0 else try extendSign(try reader.readBits(dc_size), dc_size);
    const dc_value = try checkedAddI16(prev_dc, dc_delta);

    var state = ProgressiveBlockState{};
    state.dc_predictor = dc_value;
    state.coefficients[0] = try checkedShiftLeftI16(dc_value, successive_low);
    return state;
}

pub fn refineProgressiveDc(
    reader: *EntropyBitReader,
    state: *ProgressiveBlockState,
    successive_low: u8,
) !void {
    if (try reader.readBit() == 1) {
        const refinement_bit = @as(i16, 1) << @as(u4, @intCast(successive_low));
        state.coefficients[0] |= refinement_bit;
    }
}

pub fn decodeProgressiveAcFirst(
    reader: *EntropyBitReader,
    ac_table: CanonicalHuffmanTable,
    state: *ProgressiveBlockState,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
) !void {
    var eob_run: u16 = 0;
    return decodeProgressiveAcFirstWithEobRun(
        reader,
        ac_table,
        state,
        spectral_start,
        spectral_end,
        successive_low,
        &eob_run,
    );
}

pub fn decodeProgressiveAcFirstWithEobRun(
    reader: *EntropyBitReader,
    ac_table: CanonicalHuffmanTable,
    state: *ProgressiveBlockState,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
    eob_run: *u16,
) !void {
    if (eob_run.* > 0) {
        eob_run.* -= 1;
        return;
    }

    var zig_zag_index: usize = spectral_start;
    const zig_zag_end: usize = spectral_end;

    while (zig_zag_index <= zig_zag_end) {
        const symbol = try decodeHuffmanSymbol(reader, ac_table);
        const run_length = symbol >> 4;
        const value_bits = symbol & 0x0f;
        if (value_bits == 0) {
            if (run_length == 0x0f) {
                zig_zag_index += 16;
                continue;
            }

            const run_bits = if (run_length == 0) 0 else try reader.readBits(run_length);
            eob_run.* = ((@as(u16, 1) << @as(u4, @intCast(run_length))) + run_bits) - 1;
            return;
        }

        zig_zag_index += run_length;
        if (zig_zag_index > zig_zag_end or zig_zag_index >= 64) return error.JpegDecodeFailed;

        const coeff = (try extendSign(try reader.readBits(value_bits), value_bits)) << @as(u4, @intCast(successive_low));
        state.coefficients[zig_zag_to_natural[zig_zag_index]] = coeff;
        zig_zag_index += 1;
    }
}

pub fn refineProgressiveAc(
    reader: *EntropyBitReader,
    ac_table: CanonicalHuffmanTable,
    state: *ProgressiveBlockState,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
    eob_run: *u16,
) !void {
    const refinement_bit = @as(i16, 1) << @as(u4, @intCast(successive_low));
    var zig_zag_index: usize = spectral_start;
    const zig_zag_end: usize = spectral_end;

    if (eob_run.* == 0) {
        while (zig_zag_index <= zig_zag_end) {
            const symbol = try decodeHuffmanSymbol(reader, ac_table);
            var zero_run_remaining: i32 = symbol >> 4;
            const value_bits = symbol & 0x0f;
            var new_coefficient: i16 = 0;
            const is_zrl = value_bits == 0 and zero_run_remaining == 15;

            if (value_bits != 0) {
                if (value_bits != 1) return error.UnsupportedJpegFormat;
                new_coefficient = if (try reader.readBit() == 1) refinement_bit else -refinement_bit;
            } else if (zero_run_remaining != 15) {
                const run_bits = if (zero_run_remaining == 0) 0 else try reader.readBits(@intCast(zero_run_remaining));
                eob_run.* = (@as(u16, 1) << @as(u4, @intCast(zero_run_remaining))) + run_bits;
                break;
            }

            while (zig_zag_index <= zig_zag_end) : (zig_zag_index += 1) {
                const coeff = &state.coefficients[zig_zag_to_natural[zig_zag_index]];
                if (coeff.* != 0) {
                    try refineProgressiveAcCoefficient(reader, coeff, refinement_bit);
                    continue;
                }

                zero_run_remaining -= 1;
                if (zero_run_remaining < 0) break;
            }

            if (new_coefficient != 0) {
                if (zig_zag_index > zig_zag_end) return error.JpegDecodeFailed;
                state.coefficients[zig_zag_to_natural[zig_zag_index]] = new_coefficient;
                zig_zag_index += 1;
            } else if (is_zrl) {
                zig_zag_index += 1;
            }
        }
    }

    if (eob_run.* > 0) {
        while (zig_zag_index <= zig_zag_end) : (zig_zag_index += 1) {
            try refineProgressiveAcCoefficient(
                reader,
                &state.coefficients[zig_zag_to_natural[zig_zag_index]],
                refinement_bit,
            );
        }
        eob_run.* -= 1;
    }
}

fn decodeProgressiveAcScan(
    reader: *EntropyBitReader,
    ac_table: CanonicalHuffmanTable,
    states: []ProgressiveBlockState,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
) !void {
    var eob_run: u16 = 0;
    for (states, 0..) |*state, block_index| {
        decodeProgressiveAcFirstWithEobRun(
            reader,
            ac_table,
            state,
            spectral_start,
            spectral_end,
            successive_low,
            &eob_run,
        ) catch |err| switch (err) {
            error.JpegDecodeFailed => {
                if (block_index + 1 == states.len) break;
                return err;
            },
            else => return err,
        };
    }
}

fn refineProgressiveAcScan(
    reader: *EntropyBitReader,
    ac_table: CanonicalHuffmanTable,
    states: []ProgressiveBlockState,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
) !void {
    var eob_run: u16 = 0;
    for (states, 0..) |*state, block_index| {
        refineProgressiveAc(
            reader,
            ac_table,
            state,
            spectral_start,
            spectral_end,
            successive_low,
            &eob_run,
        ) catch |err| switch (err) {
            error.JpegDecodeFailed => {
                if (block_index + 1 == states.len) break;
                return err;
            },
            else => return err,
        };
    }
}

fn decodeProgressiveDcInterleavedScan(
    reader: *EntropyBitReader,
    scan_components: []const ProgressiveDcScanComponent,
    dc_predictors: *[max_components]i16,
    mcu_cols: usize,
    mcu_rows: usize,
    successive_high: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var restart_index: u8 = 0;
    var mcus_decoded: usize = 0;
    const total_mcus = mcu_cols * mcu_rows;

    for (0..mcu_rows) |mcu_y| {
        for (0..mcu_cols) |mcu_x| {
            for (scan_components) |scan_component| {
                for (0..scan_component.component.vertical_sampling) |block_row| {
                    for (0..scan_component.component.horizontal_sampling) |block_col| {
                        const block_index = componentBlockIndexForMcu(
                            scan_component.blocks_x,
                            scan_component.blocks_y,
                            scan_component.component,
                            mcu_x,
                            mcu_y,
                            block_col,
                            block_row,
                        ) orelse continue;
                        if (successive_high == 0) {
                            const block = try decodeProgressiveDcFirst(
                                reader,
                                scan_component.dc_table,
                                dc_predictors[scan_component.frame_index],
                                successive_low,
                            );
                            dc_predictors[scan_component.frame_index] = block.dc_predictor;
                            scan_component.states[block_index] = block;
                        } else {
                            try refineProgressiveDc(
                                reader,
                                &scan_component.states[block_index],
                                successive_low,
                            );
                        }
                    }
                }
            }

            mcus_decoded += 1;
            if (restart_interval != 0 and mcus_decoded < total_mcus and mcus_decoded % restart_interval == 0) {
                try reader.consumeExpectedRestartMarker(0xd0 + restart_index);
                restart_index = (restart_index + 1) & 0x7;
                dc_predictors.* = [_]i16{0} ** max_components;
            }
        }
    }
}

fn decodeProgressiveDcSingleComponentScan(
    reader: *EntropyBitReader,
    dc_table: CanonicalHuffmanTable,
    states: []ProgressiveBlockState,
    dc_predictor: *i16,
    successive_high: u8,
    successive_low: u8,
) !void {
    for (states) |*state| {
        if (successive_high == 0) {
            const block = try decodeProgressiveDcFirst(
                reader,
                dc_table,
                dc_predictor.*,
                successive_low,
            );
            dc_predictor.* = block.dc_predictor;
            state.* = block;
        } else {
            try refineProgressiveDc(reader, state, successive_low);
        }
    }
}

fn decodeProgressiveDcSingleComponentScanMapped(
    reader: *EntropyBitReader,
    dc_table: CanonicalHuffmanTable,
    states: []ProgressiveBlockState,
    padded_blocks_x: usize,
    actual_blocks_x: usize,
    actual_blocks_y: usize,
    dc_predictor: *i16,
    successive_high: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var restart_index: u8 = 0;
    var blocks_decoded: usize = 0;
    const total_blocks = actual_blocks_x * actual_blocks_y;

    for (0..actual_blocks_y) |block_y| {
        for (0..actual_blocks_x) |block_x| {
            const block_index = block_y * padded_blocks_x + block_x;
            if (successive_high == 0) {
                const block = try decodeProgressiveDcFirst(
                    reader,
                    dc_table,
                    dc_predictor.*,
                    successive_low,
                );
                dc_predictor.* = block.dc_predictor;
                states[block_index] = block;
            } else {
                try refineProgressiveDc(reader, &states[block_index], successive_low);
            }

            blocks_decoded += 1;
            if (restart_interval != 0 and blocks_decoded < total_blocks and blocks_decoded % restart_interval == 0) {
                try reader.consumeExpectedRestartMarker(0xd0 + restart_index);
                restart_index = (restart_index + 1) & 0x7;
                dc_predictor.* = 0;
            }
        }
    }
}

fn decodeProgressiveAcScanMapped(
    reader: *EntropyBitReader,
    ac_table: CanonicalHuffmanTable,
    states: []ProgressiveBlockState,
    padded_blocks_x: usize,
    actual_blocks_x: usize,
    actual_blocks_y: usize,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var eob_run: u16 = 0;
    var processed_blocks: usize = 0;
    const total_blocks = actual_blocks_x * actual_blocks_y;
    var restart_index: u8 = 0;
    for (0..actual_blocks_y) |block_y| {
        for (0..actual_blocks_x) |block_x| {
            const block_index = block_y * padded_blocks_x + block_x;
            processed_blocks += 1;
            decodeProgressiveAcFirstWithEobRun(
                reader,
                ac_table,
                &states[block_index],
                spectral_start,
                spectral_end,
                successive_low,
                &eob_run,
            ) catch |err| switch (err) {
                error.JpegDecodeFailed => {
                    if (processed_blocks == total_blocks) break;
                    return err;
                },
                else => return err,
            };

            if (restart_interval != 0 and processed_blocks < total_blocks and processed_blocks % restart_interval == 0) {
                try reader.consumeExpectedRestartMarker(0xd0 + restart_index);
                restart_index = (restart_index + 1) & 0x7;
                eob_run = 0;
            }
        }
    }
}

fn refineProgressiveAcScanMapped(
    reader: *EntropyBitReader,
    ac_table: CanonicalHuffmanTable,
    states: []ProgressiveBlockState,
    padded_blocks_x: usize,
    actual_blocks_x: usize,
    actual_blocks_y: usize,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var eob_run: u16 = 0;
    var processed_blocks: usize = 0;
    const total_blocks = actual_blocks_x * actual_blocks_y;
    var restart_index: u8 = 0;
    for (0..actual_blocks_y) |block_y| {
        for (0..actual_blocks_x) |block_x| {
            const block_index = block_y * padded_blocks_x + block_x;
            processed_blocks += 1;
            refineProgressiveAc(
                reader,
                ac_table,
                &states[block_index],
                spectral_start,
                spectral_end,
                successive_low,
                &eob_run,
            ) catch |err| switch (err) {
                error.JpegDecodeFailed => {
                    if (processed_blocks == total_blocks) break;
                    return err;
                },
                else => return err,
            };

            if (restart_interval != 0 and processed_blocks < total_blocks and processed_blocks % restart_interval == 0) {
                try reader.consumeExpectedRestartMarker(0xd0 + restart_index);
                restart_index = (restart_index + 1) & 0x7;
                eob_run = 0;
            }
        }
    }
}

fn applyProgressiveScan(
    jpeg_bytes: []const u8,
    structure: Structure,
    scan_index: usize,
    progressive: *const ProgressiveDecodeContext,
    dc_predictors: *[max_components]i16,
) !void {
    const scan = structure.scans[scan_index];
    const entropy_bytes = try scanEntropyBytes(structure, jpeg_bytes, scan_index);
    var reader = EntropyBitReader.init(entropy_bytes);
    const restart_interval = structure.restart_interval orelse 0;

    if (scan.spectral_start == 0) {
        var scan_frame_indices = [_]usize{0} ** max_components;
        var scan_dc_tables: [max_components]CanonicalHuffmanTable = undefined;
        for (0..scan.component_count) |i| {
            const scan_component = scan.components[i];
            const frame_index = frameComponentIndex(structure.info, scan_component.component_selector) orelse return error.JpegDecodeFailed;
            scan_frame_indices[i] = frame_index;
            scan_dc_tables[i] = try buildCanonicalHuffmanTable(scan.dc_tables[scan_component.dc_table_id]);
        }

        if (scan.component_count > 1) {
            var progressive_components = [_]ProgressiveDcScanComponent{undefined} ** max_components;
            for (0..scan.component_count) |scan_component_index| {
                const frame_index = scan_frame_indices[scan_component_index];
                progressive_components[scan_component_index] = .{
                    .frame_index = frame_index,
                    .component = structure.info.components[frame_index],
                    .dc_table = scan_dc_tables[scan_component_index],
                    .states = progressive.component_states[frame_index],
                    .blocks_x = progressive.component_blocks_x[frame_index],
                    .blocks_y = progressive.component_blocks_y[frame_index],
                };
            }
            try decodeProgressiveDcInterleavedScan(
                &reader,
                progressive_components[0..scan.component_count],
                dc_predictors,
                progressive.mcu_cols,
                progressive.mcu_rows,
                scan.successive_approx_high,
                scan.successive_approx_low,
                restart_interval,
            );
        } else {
            const frame_index = scan_frame_indices[0];
            try decodeProgressiveDcSingleComponentScanMapped(
                &reader,
                scan_dc_tables[0],
                progressive.component_states[frame_index],
                progressive.component_blocks_x[frame_index],
                progressive.component_actual_blocks_x[frame_index],
                progressive.component_actual_blocks_y[frame_index],
                &dc_predictors[frame_index],
                scan.successive_approx_high,
                scan.successive_approx_low,
                restart_interval,
            );
        }
        return;
    }

    const frame_index = frameComponentIndex(structure.info, scan.components[0].component_selector) orelse return error.JpegDecodeFailed;
    const ac_table = try buildCanonicalHuffmanTable(scan.ac_tables[scan.components[0].ac_table_id]);
    if (scan.successive_approx_high == 0) {
        try decodeProgressiveAcScanMapped(
            &reader,
            ac_table,
            progressive.component_states[frame_index],
            progressive.component_blocks_x[frame_index],
            progressive.component_actual_blocks_x[frame_index],
            progressive.component_actual_blocks_y[frame_index],
            scan.spectral_start,
            scan.spectral_end,
            scan.successive_approx_low,
            restart_interval,
        );
    } else {
        try refineProgressiveAcScanMapped(
            &reader,
            ac_table,
            progressive.component_states[frame_index],
            progressive.component_blocks_x[frame_index],
            progressive.component_actual_blocks_x[frame_index],
            progressive.component_actual_blocks_y[frame_index],
            scan.spectral_start,
            scan.spectral_end,
            scan.successive_approx_low,
            restart_interval,
        );
    }
}

pub fn decodeRgba(alloc: Allocator, jpeg_bytes: []const u8) !DecodedImage {
    const structure = try parseStructure(jpeg_bytes);
    if (supportsPlannedLosslessDecode(structure)) {
        return decodeRgbaPureZigLossless(alloc, jpeg_bytes, structure);
    }
    if (canPureZigDecodeGrayscaleBaseline(structure)) {
        return decodeRgbaPureZigGrayscaleBaseline(alloc, jpeg_bytes, structure);
    }
    if (canPureZigDecodeColorBaseline(structure)) {
        return decodeRgbaPureZigColorBaseline(alloc, jpeg_bytes, structure);
    }
    if (canPureZigDecodeProgressive(structure)) {
        return decodeRgbaPureZigProgressive(alloc, jpeg_bytes, structure);
    }
    if (canPureZigDecodeArithmeticProgressive(structure)) {
        return decodeRgbaPureZigArithmeticProgressive(alloc, jpeg_bytes, structure);
    }
    if (supportsPlannedArithmeticDecode(structure)) {
        return decodeRgbaPureZigArithmeticSequential(alloc, jpeg_bytes, structure);
    }

    return error.JpegDecodeFailed;
}

pub fn supportsPlannedLosslessDecode(structure: Structure) bool {
    if (structure.info.frame_kind != .lossless) return false;
    if (structure.info.bits_per_sample == 0 or structure.info.bits_per_sample > 16) return false;
    if (structure.info.component_count != 1 and structure.info.component_count != 3) return false;
    if (structure.scan_count != 1) return false;

    const scan = structure.scans[0];
    if (scan.component_count != structure.info.component_count) return false;
    if (scan.spectral_start < 1 or scan.spectral_start > 7) return false;
    if (scan.spectral_end != 0) return false;
    if (scan.successive_approx_high != 0) return false;
    if (scan.successive_approx_low >= structure.info.bits_per_sample) return false;

    for (0..structure.info.component_count) |i| {
        const frame_component = structure.info.components[i];
        const scan_component = scan.components[i];
        if (frame_component.horizontal_sampling != 1 or frame_component.vertical_sampling != 1) return false;
        if (scan_component.component_selector != frame_component.id) return false;
        if (!scan.dc_tables[scan_component.dc_table_id].present) return false;
    }

    if (structure.info.component_count == 3 and colorEncodingForStructure(structure) == null) return false;
    return true;
}

fn canPureZigDecodeGrayscaleBaseline(structure: Structure) bool {
    if (!supportsPlannedBaselineDecode(structure)) return false;
    if (structure.info.component_count != 1) return false;

    const component = structure.info.components[0];
    return component.horizontal_sampling == 1 and component.vertical_sampling == 1;
}

fn canPureZigDecodeColorBaseline(structure: Structure) bool {
    if (!supportsPlannedBaselineDecode(structure)) return false;
    const color_encoding = colorEncodingForStructure(structure) orelse return false;
    for (0..structure.info.component_count) |i| {
        const component = structure.info.components[i];
        if (@as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling) > max_component_blocks) return false;
    }

    if (color_encoding != .ycbcr) return true;
    return true;
}

fn canPureZigDecodeProgressive(structure: Structure) bool {
    if (!supportsPlannedProgressiveDecode(structure)) return false;

    if (structure.info.component_count == 1) {
        const component = structure.info.components[0];
        return component.horizontal_sampling == 1 and component.vertical_sampling == 1;
    }

    const color_encoding = colorEncodingForStructure(structure) orelse return false;

    for (0..structure.info.component_count) |i| {
        const component = structure.info.components[i];
        if (@as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling) > max_component_blocks) return false;
    }

    if (color_encoding != .ycbcr) return true;

    const y = structure.info.components[0];
    const cb = structure.info.components[1];
    const cr = structure.info.components[2];
    if (cb.horizontal_sampling != 1 or cb.vertical_sampling != 1) return false;
    if (cr.horizontal_sampling != 1 or cr.vertical_sampling != 1) return false;
    if ((y.horizontal_sampling == 4 and y.vertical_sampling == 1) or
        (y.horizontal_sampling == 1 and y.vertical_sampling == 4)) return true;
    return (y.horizontal_sampling == 1 and y.vertical_sampling == 1) or
        (y.horizontal_sampling == 2 and y.vertical_sampling == 1) or
        (y.horizontal_sampling == 2 and y.vertical_sampling == 2);
}

fn canPureZigDecodeArithmeticProgressive(structure: Structure) bool {
    if (!supportsPlannedArithmeticProgressiveDecode(structure)) return false;

    if (structure.info.component_count == 1) {
        const component = structure.info.components[0];
        return component.horizontal_sampling == 1 and component.vertical_sampling == 1;
    }

    const color_encoding = colorEncodingForStructure(structure) orelse return false;

    for (0..structure.info.component_count) |i| {
        const component = structure.info.components[i];
        if (@as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling) > max_component_blocks) return false;
    }

    if (color_encoding != .ycbcr) return true;

    const y = structure.info.components[0];
    const cb = structure.info.components[1];
    const cr = structure.info.components[2];
    if (cb.horizontal_sampling != 1 or cb.vertical_sampling != 1) return false;
    if (cr.horizontal_sampling != 1 or cr.vertical_sampling != 1) return false;
    if ((y.horizontal_sampling == 4 and y.vertical_sampling == 1) or
        (y.horizontal_sampling == 1 and y.vertical_sampling == 4)) return true;
    return (y.horizontal_sampling == 1 and y.vertical_sampling == 1) or
        (y.horizontal_sampling == 2 and y.vertical_sampling == 1) or
        (y.horizontal_sampling == 2 and y.vertical_sampling == 2);
}

pub fn supportsPlannedProgressiveDecode(structure: Structure) bool {
    if (structure.info.frame_kind != .progressive_dct) return false;
    if (structure.info.bits_per_sample != 8) return false;
    if (structure.info.component_count != 1 and structure.info.component_count != 3 and structure.info.component_count != 4) return false;
    if (structure.scan_count < 2) return false;
    if (structure.info.component_count > 1) {
        if (colorEncodingForStructure(structure) == null) return false;
    }

    var saw_dc_first = [_]bool{false} ** max_components;
    var saw_ac_first = [_]bool{false} ** max_components;

    for (0..structure.info.component_count) |i| {
        const component = structure.info.components[i];
        if (component.quant_table_id >= max_quant_tables) return false;
        if (!structure.quant_tables[component.quant_table_id].present) return false;
        if (structure.quant_tables[component.quant_table_id].precision_bits != 8) return false;
    }

    for (structure.scans[0..structure.scan_count]) |scan| {
        if (scan.component_count == 0 or scan.component_count > structure.info.component_count) return false;
        if (scan.spectral_end < scan.spectral_start) return false;
        if (scan.successive_approx_high != 0 and scan.successive_approx_high != scan.successive_approx_low + 1) return false;

        if (scan.spectral_start == 0) {
            if (scan.spectral_end != 0) return false;
            for (scan.components[0..scan.component_count]) |scan_component| {
                const frame_index = frameComponentIndex(structure.info, scan_component.component_selector) orelse return false;
                if (!scan.dc_tables[scan_component.dc_table_id].present) return false;
                if (scan.successive_approx_high == 0) {
                    saw_dc_first[frame_index] = true;
                } else if (!saw_dc_first[frame_index]) {
                    return false;
                }
            }
            continue;
        }

        if (scan.component_count != 1) return false;
        const frame_index = frameComponentIndex(structure.info, scan.components[0].component_selector) orelse return false;
        const scan_component = scan.components[0];
        if (scan.successive_approx_high == 0 and !saw_dc_first[frame_index]) return false;
        if (!scan.ac_tables[scan_component.ac_table_id].present) return false;
        if (scan.successive_approx_high == 0) {
            saw_ac_first[frame_index] = true;
        } else if (!saw_ac_first[frame_index]) {
            return false;
        }
    }

    for (0..structure.info.component_count) |i| {
        if (!saw_dc_first[i]) return false;
    }

    return true;
}

pub fn supportsPlannedArithmeticDecode(structure: Structure) bool {
    if (structure.info.frame_kind != .arithmetic_sequential_dct) return false;
    if (structure.info.bits_per_sample != 8) return false;
    if (structure.info.component_count != 1 and structure.info.component_count != 3 and structure.info.component_count != 4) return false;
    if (structure.scan_count != 1) return false;

    const scan = structure.scans[0];
    if (scan.component_count != structure.info.component_count) return false;
    if (scan.spectral_start != 0 or scan.spectral_end != 63) return false;
    if (scan.successive_approx_high != 0 or scan.successive_approx_low != 0) return false;

    for (0..structure.info.component_count) |i| {
        const frame_component = structure.info.components[i];
        const scan_component = scan.components[i];
        if (frame_component.quant_table_id >= max_quant_tables) return false;
        if (!structure.quant_tables[frame_component.quant_table_id].present) return false;
        if (structure.quant_tables[frame_component.quant_table_id].precision_bits != 8) return false;
        if (scan_component.component_selector != frame_component.id) return false;
        if (scan_component.dc_table_id >= max_arithmetic_tables or scan_component.ac_table_id >= max_arithmetic_tables) return false;
    }

    if (structure.info.component_count == 1) {
        const component = structure.info.components[0];
        return component.horizontal_sampling == 1 and component.vertical_sampling == 1;
    }

    const color_encoding = colorEncodingForStructure(structure) orelse return false;
    for (0..structure.info.component_count) |i| {
        const component = structure.info.components[i];
        if (@as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling) > max_component_blocks) return false;
    }

    if (color_encoding == .ycbcr and structure.info.component_count != 3) return false;
    return true;
}

pub fn supportsPlannedArithmeticProgressiveDecode(structure: Structure) bool {
    if (structure.info.frame_kind != .arithmetic_progressive_dct) return false;
    if (structure.info.bits_per_sample != 8) return false;
    if (structure.info.component_count != 1 and structure.info.component_count != 3 and structure.info.component_count != 4) return false;
    if (structure.scan_count < 2) return false;
    if (structure.info.component_count > 1) {
        if (colorEncodingForStructure(structure) == null) return false;
    }

    var saw_dc_first = [_]bool{false} ** max_components;
    var saw_ac_first = [_]bool{false} ** max_components;

    for (0..structure.info.component_count) |i| {
        const component = structure.info.components[i];
        if (component.quant_table_id >= max_quant_tables) return false;
        if (!structure.quant_tables[component.quant_table_id].present) return false;
        if (structure.quant_tables[component.quant_table_id].precision_bits != 8) return false;
    }

    for (structure.scans[0..structure.scan_count]) |scan| {
        if (scan.component_count == 0 or scan.component_count > structure.info.component_count) return false;
        if (scan.spectral_end < scan.spectral_start) return false;
        if (scan.successive_approx_high != 0 and scan.successive_approx_high != scan.successive_approx_low + 1) return false;

        if (scan.spectral_start == 0) {
            if (scan.spectral_end != 0) return false;
            for (scan.components[0..scan.component_count]) |scan_component| {
                const frame_index = frameComponentIndex(structure.info, scan_component.component_selector) orelse return false;
                if (scan_component.dc_table_id >= max_arithmetic_tables or scan_component.ac_table_id >= max_arithmetic_tables) return false;
                if (scan.successive_approx_high == 0) {
                    saw_dc_first[frame_index] = true;
                } else if (!saw_dc_first[frame_index]) {
                    return false;
                }
            }
            continue;
        }

        if (scan.component_count != 1) return false;
        const frame_index = frameComponentIndex(structure.info, scan.components[0].component_selector) orelse return false;
        const scan_component = scan.components[0];
        if (scan_component.dc_table_id >= max_arithmetic_tables or scan_component.ac_table_id >= max_arithmetic_tables) return false;
        if (scan.successive_approx_high == 0 and !saw_dc_first[frame_index]) return false;
        if (scan.successive_approx_high == 0) {
            saw_ac_first[frame_index] = true;
        } else if (!saw_ac_first[frame_index]) {
            return false;
        }
    }

    for (0..structure.info.component_count) |i| {
        if (!saw_dc_first[i]) return false;
    }

    return true;
}

fn initArithmeticProgressiveScanState(
    structure: Structure,
    scan: ScanInfo,
    dc_stats: *[max_arithmetic_tables][arithmetic_dc_stat_bins]u8,
    ac_stats: *[max_arithmetic_tables][arithmetic_ac_stat_bins]u8,
    last_dc_values: *[max_components]i32,
    dc_contexts: *[max_components]u8,
) !void {
    if (scan.spectral_start == 0 and scan.successive_approx_high == 0) {
        for (scan.components[0..scan.component_count]) |scan_component| {
            const frame_index = frameComponentIndex(structure.info, scan_component.component_selector) orelse return error.JpegDecodeFailed;
            dc_stats[scan_component.dc_table_id] = std.mem.zeroes([arithmetic_dc_stat_bins]u8);
            last_dc_values[frame_index] = 0;
            dc_contexts[frame_index] = 0;
        }
    }

    if (scan.spectral_start != 0) {
        for (scan.components[0..scan.component_count]) |scan_component| {
            ac_stats[scan_component.ac_table_id] = std.mem.zeroes([arithmetic_ac_stat_bins]u8);
        }
    }
}

fn consumeArithmeticProgressiveRestartMarker(
    decoder: *ArithmeticDecoder,
    structure: Structure,
    scan: ScanInfo,
    dc_stats: *[max_arithmetic_tables][arithmetic_dc_stat_bins]u8,
    ac_stats: *[max_arithmetic_tables][arithmetic_ac_stat_bins]u8,
    last_dc_values: *[max_components]i32,
    dc_contexts: *[max_components]u8,
    expected_marker: u8,
) !void {
    try decoder.consumeExpectedRestartMarker(expected_marker);
    try initArithmeticProgressiveScanState(structure, scan, dc_stats, ac_stats, last_dc_values, dc_contexts);
}

fn decodeRgbaPureZigArithmeticProgressive(
    alloc: Allocator,
    jpeg_bytes: []const u8,
    structure: Structure,
) !DecodedImage {
    const width = structure.info.width;
    const height = structure.info.height;
    const pixel_count = @as(usize, width) * @as(usize, height);
    const rgba = try alloc.alloc(u8, pixel_count * 4);
    errdefer alloc.free(rgba);

    var progressive = try ProgressiveDecodeContext.init(alloc, structure);
    defer progressive.deinit(alloc);

    var dc_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_dc_stat_bins]u8);
    var ac_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_ac_stat_bins]u8);
    var fixed_bin = [_]u8{0} ** 4;
    fixed_bin[0] = 113;
    var last_dc_values = [_]i32{0} ** max_components;
    var dc_contexts = [_]u8{0} ** max_components;

    for (0..structure.scan_count) |scan_index| {
        try applyArithmeticProgressiveScan(
            jpeg_bytes,
            structure,
            scan_index,
            &progressive,
            &dc_stats,
            &ac_stats,
            &fixed_bin,
            &last_dc_values,
            &dc_contexts,
        );
    }

    if (structure.info.component_count == 1) {
        for (0..progressive.component_blocks_y[0]) |block_y| {
            for (0..progressive.component_blocks_x[0]) |block_x| {
                const block_index = block_y * progressive.component_blocks_x[0] + block_x;
                const spatial = dequantizeAndInverseDctWithSamplePrecision(
                    progressive.component_states[0][block_index].coefficients,
                    progressive.component_quant_tables[0],
                    structure.info.bits_per_sample,
                );
                writeGrayscaleBlockRgba(rgba, width, height, block_x, block_y, spatial);
            }
        }
    } else {
        const color_encoding = colorEncodingForStructure(structure) orelse return error.JpegDecodeFailed;
        var component_planes = try initComponentSamplePlanes(
            alloc,
            width,
            height,
            structure.info.components,
            structure.info.component_count,
            progressive.max_h,
            progressive.max_v,
        );
        defer freeComponentSamplePlanes(alloc, &component_planes);
        for (0..progressive.mcu_rows) |mcu_y| {
            for (0..progressive.mcu_cols) |mcu_x| {
                for (0..structure.info.component_count) |frame_index| {
                    const component = structure.info.components[frame_index];
                    const block_count = @as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling);
                    for (0..block_count) |block_index| {
                        const block_col = block_index % @as(usize, component.horizontal_sampling);
                        const block_row = block_index / @as(usize, component.horizontal_sampling);
                        const global_block_index = componentBlockIndexForMcu(
                            progressive.component_blocks_x[frame_index],
                            progressive.component_blocks_y[frame_index],
                            component,
                            mcu_x,
                            mcu_y,
                            block_col,
                            block_row,
                        ) orelse continue;
                        const spatial = dequantizeAndInverseDctNativeWithSamplePrecision(
                            progressive.component_states[frame_index][global_block_index].coefficients,
                            progressive.component_quant_tables[frame_index],
                            structure.info.bits_per_sample,
                        );
                        const plane_block_x = mcu_x * @as(usize, component.horizontal_sampling) + block_col;
                        const plane_block_y = mcu_y * @as(usize, component.vertical_sampling) + block_row;
                        writeSpatialBlockToPlane(component_planes[frame_index], plane_block_x, plane_block_y, spatial);
                    }
                }
            }
        }

        renderColorPlanesToRgba(rgba, width, height, structure.info.bits_per_sample, color_encoding, structure.info.components, progressive.max_h, progressive.max_v, component_planes);
    }

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn applyArithmeticProgressiveScan(
    jpeg_bytes: []const u8,
    structure: Structure,
    scan_index: usize,
    progressive: *const ProgressiveDecodeContext,
    dc_stats: *[max_arithmetic_tables][arithmetic_dc_stat_bins]u8,
    ac_stats: *[max_arithmetic_tables][arithmetic_ac_stat_bins]u8,
    fixed_bin: *[4]u8,
    last_dc_values: *[max_components]i32,
    dc_contexts: *[max_components]u8,
) !void {
    const scan = structure.scans[scan_index];
    try initArithmeticProgressiveScanState(structure, scan, dc_stats, ac_stats, last_dc_values, dc_contexts);
    const entropy_bytes = try scanEntropyBytes(structure, jpeg_bytes, scan_index);
    var decoder = ArithmeticDecoder.init(entropy_bytes);
    const restart_interval = structure.restart_interval orelse 0;

    if (scan.spectral_start == 0) {
        if (scan.component_count > 1) {
            var scan_components = [_]ArithmeticProgressiveDcScanComponent{undefined} ** max_components;
            for (0..scan.component_count) |scan_component_index| {
                const scan_component = scan.components[scan_component_index];
                const frame_index = frameComponentIndex(structure.info, scan_component.component_selector) orelse return error.JpegDecodeFailed;
                scan_components[scan_component_index] = .{
                    .frame_index = frame_index,
                    .component = structure.info.components[frame_index],
                    .dc_table_id = scan_component.dc_table_id,
                    .states = progressive.component_states[frame_index],
                    .blocks_x = progressive.component_blocks_x[frame_index],
                    .blocks_y = progressive.component_blocks_y[frame_index],
                };
            }
            try decodeArithmeticProgressiveDcInterleavedScan(
                &decoder,
                structure,
                scan,
                scan_components[0..scan.component_count],
                dc_stats,
                fixed_bin,
                last_dc_values,
                dc_contexts,
                progressive.mcu_cols,
                progressive.mcu_rows,
                scan.successive_approx_high,
                scan.successive_approx_low,
                restart_interval,
            );
        } else {
            const frame_index = frameComponentIndex(structure.info, scan.components[0].component_selector) orelse return error.JpegDecodeFailed;
            try decodeArithmeticProgressiveDcSingleComponentScanMapped(
                &decoder,
                structure,
                scan,
                frame_index,
                scan.components[0].dc_table_id,
                progressive.component_states[frame_index],
                progressive.component_blocks_x[frame_index],
                progressive.component_actual_blocks_x[frame_index],
                progressive.component_actual_blocks_y[frame_index],
                dc_stats,
                fixed_bin,
                last_dc_values,
                dc_contexts,
                scan.successive_approx_high,
                scan.successive_approx_low,
                restart_interval,
            );
        }
        return;
    }

    const frame_index = frameComponentIndex(structure.info, scan.components[0].component_selector) orelse return error.JpegDecodeFailed;
    if (scan.successive_approx_high == 0) {
        try decodeArithmeticProgressiveAcScanMapped(
            &decoder,
            structure,
            scan,
            ac_stats,
            scan.components[0].ac_table_id,
            fixed_bin,
            progressive.component_states[frame_index],
            progressive.component_blocks_x[frame_index],
            progressive.component_actual_blocks_x[frame_index],
            progressive.component_actual_blocks_y[frame_index],
            scan.spectral_start,
            scan.spectral_end,
            scan.successive_approx_low,
            restart_interval,
        );
    } else {
        try refineArithmeticProgressiveAcScanMapped(
            &decoder,
            structure,
            scan,
            ac_stats,
            scan.components[0].ac_table_id,
            fixed_bin,
            progressive.component_states[frame_index],
            progressive.component_blocks_x[frame_index],
            progressive.component_actual_blocks_x[frame_index],
            progressive.component_actual_blocks_y[frame_index],
            scan.spectral_start,
            scan.spectral_end,
            scan.successive_approx_low,
            restart_interval,
        );
    }
}

fn decodeArithmeticProgressiveDcFirst(
    decoder: *ArithmeticDecoder,
    stats: *[arithmetic_dc_stat_bins]u8,
    dc_l: u8,
    dc_u: u8,
    dc_context: *u8,
    last_dc_value: *i32,
    successive_low: u8,
) !ProgressiveBlockState {
    const dc_diff = try decodeArithmeticDc(decoder, stats, dc_l, dc_u, dc_context);
    last_dc_value.* = wrapArithmeticDcPredictor(last_dc_value.*, dc_diff);
    const dc_value = arithmeticPredictorToCoefficient(last_dc_value.*);

    var state = ProgressiveBlockState{};
    state.dc_predictor = dc_value;
    state.coefficients[0] = try checkedShiftLeftI16(dc_value, successive_low);
    return state;
}

fn refineArithmeticProgressiveDc(
    decoder: *ArithmeticDecoder,
    fixed_bin: *[4]u8,
    state: *ProgressiveBlockState,
    successive_low: u8,
) !void {
    if ((try arithDecode(decoder, &fixed_bin[0])) == 1) {
        const refinement_bit = @as(i16, 1) << @as(u4, @intCast(successive_low));
        state.coefficients[0] |= refinement_bit;
    }
}

fn decodeArithmeticProgressiveAcFirst(
    decoder: *ArithmeticDecoder,
    stats: *[arithmetic_ac_stat_bins]u8,
    fixed_bin: *[4]u8,
    arithmetic_k: u8,
    state: *ProgressiveBlockState,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
) !void {
    const spectral_end_usize: usize = spectral_end;
    var k: usize = spectral_start - 1;

    while (true) {
        var st_index = 3 * k;
        if ((try arithDecode(decoder, &stats[st_index])) != 0) break;

        while (true) {
            k += 1;
            if ((try arithDecode(decoder, &stats[st_index + 1])) != 0) break;
            st_index += 3;
            if (k >= spectral_end_usize) return error.JpegDecodeFailed;
        }

        const sign = try arithDecode(decoder, &fixed_bin[0]);
        st_index += 2;
        var magnitude: i32 = try arithDecode(decoder, &stats[st_index]);
        if (magnitude != 0 and (try arithDecode(decoder, &stats[st_index])) != 0) {
            magnitude <<= 1;
            st_index = if (k <= arithmetic_k) 189 else 217;
            while ((try arithDecode(decoder, &stats[st_index])) != 0) {
                magnitude <<= 1;
                if (magnitude == 0x8000) return error.JpegDecodeFailed;
                st_index += 1;
            }
        }

        var value = magnitude;
        st_index += 14;
        var mask = magnitude;
        while (true) {
            mask >>= 1;
            if (mask == 0) break;
            if ((try arithDecode(decoder, &stats[st_index])) != 0) value |= mask;
        }
        value += 1;
        if (sign != 0) value = -value;
        state.coefficients[zig_zag_to_natural[k]] = try checkedShiftLeftI16(try checkedI16FromI32(value), successive_low);

        if (k >= spectral_end_usize) break;
    }
}

fn refineArithmeticProgressiveAc(
    decoder: *ArithmeticDecoder,
    stats: *[arithmetic_ac_stat_bins]u8,
    fixed_bin: *[4]u8,
    state: *ProgressiveBlockState,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
) !void {
    const refinement_bit = @as(i16, 1) << @as(u4, @intCast(successive_low));
    const negative_refinement_bit = -refinement_bit;
    var kex: usize = spectral_end;
    while (kex > 0) : (kex -= 1) {
        if (state.coefficients[zig_zag_to_natural[kex]] != 0) break;
    }

    var k: usize = spectral_start - 1;
    const spectral_end_usize: usize = spectral_end;
    while (true) {
        var st_index = 3 * k;
        if (k >= kex and (try arithDecode(decoder, &stats[st_index])) != 0) break;

        while (true) {
            k += 1;
            const coeff = &state.coefficients[zig_zag_to_natural[k]];
            if (coeff.* != 0) {
                if ((try arithDecode(decoder, &stats[st_index + 2])) != 0) {
                    coeff.* = try checkedAddI16(coeff.*, if (coeff.* < 0) negative_refinement_bit else refinement_bit);
                }
                break;
            }

            if ((try arithDecode(decoder, &stats[st_index + 1])) != 0) {
                coeff.* = if ((try arithDecode(decoder, &fixed_bin[0])) != 0) negative_refinement_bit else refinement_bit;
                break;
            }

            st_index += 3;
            if (k >= spectral_end_usize) return error.JpegDecodeFailed;
        }

        if (k >= spectral_end_usize) break;
    }
}

fn decodeArithmeticProgressiveDcInterleavedScan(
    decoder: *ArithmeticDecoder,
    structure: Structure,
    scan: ScanInfo,
    scan_components: []const ArithmeticProgressiveDcScanComponent,
    dc_stats: *[max_arithmetic_tables][arithmetic_dc_stat_bins]u8,
    fixed_bin: *[4]u8,
    last_dc_values: *[max_components]i32,
    dc_contexts: *[max_components]u8,
    mcu_cols: usize,
    mcu_rows: usize,
    successive_high: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var restart_index: u8 = 0;
    var mcus_decoded: usize = 0;
    const total_mcus = mcu_cols * mcu_rows;

    for (0..mcu_rows) |mcu_y| {
        for (0..mcu_cols) |mcu_x| {
            for (scan_components) |scan_component| {
                for (0..scan_component.component.vertical_sampling) |block_row| {
                    for (0..scan_component.component.horizontal_sampling) |block_col| {
                        const block_index = componentBlockIndexForMcu(
                            scan_component.blocks_x,
                            scan_component.blocks_y,
                            scan_component.component,
                            mcu_x,
                            mcu_y,
                            block_col,
                            block_row,
                        ) orelse continue;
                        if (successive_high == 0) {
                            const block = try decodeArithmeticProgressiveDcFirst(
                                decoder,
                                &dc_stats[scan_component.dc_table_id],
                                structure.arithmetic_dc_l[scan_component.dc_table_id],
                                structure.arithmetic_dc_u[scan_component.dc_table_id],
                                &dc_contexts[scan_component.frame_index],
                                &last_dc_values[scan_component.frame_index],
                                successive_low,
                            );
                            scan_component.states[block_index] = block;
                        } else {
                            try refineArithmeticProgressiveDc(
                                decoder,
                                fixed_bin,
                                &scan_component.states[block_index],
                                successive_low,
                            );
                        }
                    }
                }
            }

            mcus_decoded += 1;
            if (restart_interval != 0 and mcus_decoded < total_mcus and mcus_decoded % restart_interval == 0) {
                var dummy_ac_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_ac_stat_bins]u8);
                restart_index = (restart_index + 1) & 0x7;
                try consumeArithmeticProgressiveRestartMarker(
                    decoder,
                    structure,
                    scan,
                    dc_stats,
                    &dummy_ac_stats,
                    last_dc_values,
                    dc_contexts,
                    0xcf + restart_index,
                );
            }
        }
    }
}

fn decodeArithmeticProgressiveDcSingleComponentScanMapped(
    decoder: *ArithmeticDecoder,
    structure: Structure,
    scan: ScanInfo,
    frame_index: usize,
    dc_table_id: u8,
    states: []ProgressiveBlockState,
    padded_blocks_x: usize,
    actual_blocks_x: usize,
    actual_blocks_y: usize,
    dc_stats: *[max_arithmetic_tables][arithmetic_dc_stat_bins]u8,
    fixed_bin: *[4]u8,
    last_dc_values: *[max_components]i32,
    dc_contexts: *[max_components]u8,
    successive_high: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var restart_index: u8 = 0;
    var blocks_decoded: usize = 0;
    const total_blocks = actual_blocks_x * actual_blocks_y;
    var dummy_ac_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_ac_stat_bins]u8);

    for (0..actual_blocks_y) |block_y| {
        for (0..actual_blocks_x) |block_x| {
            const block_index = block_y * padded_blocks_x + block_x;
            if (successive_high == 0) {
                const block = try decodeArithmeticProgressiveDcFirst(
                    decoder,
                    &dc_stats[dc_table_id],
                    structure.arithmetic_dc_l[dc_table_id],
                    structure.arithmetic_dc_u[dc_table_id],
                    &dc_contexts[frame_index],
                    &last_dc_values[frame_index],
                    successive_low,
                );
                states[block_index] = block;
            } else {
                try refineArithmeticProgressiveDc(decoder, fixed_bin, &states[block_index], successive_low);
            }

            blocks_decoded += 1;
            if (restart_interval != 0 and blocks_decoded < total_blocks and blocks_decoded % restart_interval == 0) {
                restart_index = (restart_index + 1) & 0x7;
                try consumeArithmeticProgressiveRestartMarker(
                    decoder,
                    structure,
                    scan,
                    dc_stats,
                    &dummy_ac_stats,
                    last_dc_values,
                    dc_contexts,
                    0xcf + restart_index,
                );
            }
        }
    }
}

fn decodeArithmeticProgressiveAcScanMapped(
    decoder: *ArithmeticDecoder,
    structure: Structure,
    scan: ScanInfo,
    ac_stats: *[max_arithmetic_tables][arithmetic_ac_stat_bins]u8,
    ac_table_id: u8,
    fixed_bin: *[4]u8,
    states: []ProgressiveBlockState,
    padded_blocks_x: usize,
    actual_blocks_x: usize,
    actual_blocks_y: usize,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var restart_index: u8 = 0;
    var processed_blocks: usize = 0;
    const total_blocks = actual_blocks_x * actual_blocks_y;
    var dc_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_dc_stat_bins]u8);
    var last_dc_values = [_]i32{0} ** max_components;
    var dc_contexts = [_]u8{0} ** max_components;

    for (0..actual_blocks_y) |block_y| {
        for (0..actual_blocks_x) |block_x| {
            const block_index = block_y * padded_blocks_x + block_x;
            try decodeArithmeticProgressiveAcFirst(
                decoder,
                &ac_stats[ac_table_id],
                fixed_bin,
                structure.arithmetic_ac_k[ac_table_id],
                &states[block_index],
                spectral_start,
                spectral_end,
                successive_low,
            );

            processed_blocks += 1;
            if (restart_interval != 0 and processed_blocks < total_blocks and processed_blocks % restart_interval == 0) {
                restart_index = (restart_index + 1) & 0x7;
                try consumeArithmeticProgressiveRestartMarker(
                    decoder,
                    structure,
                    scan,
                    &dc_stats,
                    ac_stats,
                    &last_dc_values,
                    &dc_contexts,
                    0xcf + restart_index,
                );
            }
        }
    }
}

fn refineArithmeticProgressiveAcScanMapped(
    decoder: *ArithmeticDecoder,
    structure: Structure,
    scan: ScanInfo,
    ac_stats: *[max_arithmetic_tables][arithmetic_ac_stat_bins]u8,
    ac_table_id: u8,
    fixed_bin: *[4]u8,
    states: []ProgressiveBlockState,
    padded_blocks_x: usize,
    actual_blocks_x: usize,
    actual_blocks_y: usize,
    spectral_start: u8,
    spectral_end: u8,
    successive_low: u8,
    restart_interval: u16,
) !void {
    var restart_index: u8 = 0;
    var processed_blocks: usize = 0;
    const total_blocks = actual_blocks_x * actual_blocks_y;
    var dc_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_dc_stat_bins]u8);
    var last_dc_values = [_]i32{0} ** max_components;
    var dc_contexts = [_]u8{0} ** max_components;

    for (0..actual_blocks_y) |block_y| {
        for (0..actual_blocks_x) |block_x| {
            const block_index = block_y * padded_blocks_x + block_x;
            try refineArithmeticProgressiveAc(
                decoder,
                &ac_stats[ac_table_id],
                fixed_bin,
                &states[block_index],
                spectral_start,
                spectral_end,
                successive_low,
            );

            processed_blocks += 1;
            if (restart_interval != 0 and processed_blocks < total_blocks and processed_blocks % restart_interval == 0) {
                restart_index = (restart_index + 1) & 0x7;
                try consumeArithmeticProgressiveRestartMarker(
                    decoder,
                    structure,
                    scan,
                    &dc_stats,
                    ac_stats,
                    &last_dc_values,
                    &dc_contexts,
                    0xcf + restart_index,
                );
            }
        }
    }
}

fn decodeRgbaPureZigArithmeticSequential(
    alloc: Allocator,
    jpeg_bytes: []const u8,
    structure: Structure,
) !DecodedImage {
    const width = structure.info.width;
    const height = structure.info.height;
    const pixel_count = @as(usize, width) * @as(usize, height);
    const rgba = try alloc.alloc(u8, pixel_count * 4);
    errdefer alloc.free(rgba);
    const color_encoding = colorEncodingForStructure(structure) orelse return error.JpegDecodeFailed;

    const scan = structure.scans[0];
    if (scan.entropy_start > jpeg_bytes.len) return error.JpegDecodeFailed;
    const entropy_bytes = jpeg_bytes[scan.entropy_start..];
    var decoder = ArithmeticDecoder.init(entropy_bytes);
    var dc_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_dc_stat_bins]u8);
    var ac_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_ac_stat_bins]u8);
    var fixed_bin = [_]u8{0} ** 4;
    fixed_bin[0] = 113;
    var last_dc_values = [_]i32{0} ** max_components;
    var dc_contexts = [_]u8{0} ** max_components;
    const restart_interval = structure.restart_interval orelse 0;
    var restart_index: u8 = 0;

    if (structure.info.component_count == 1) {
        const component = structure.info.components[0];
        const quant_table = quantTableNaturalOrder(structure.quant_tables[component.quant_table_id]);
        const blocks_x = @divFloor(@as(usize, width) + 7, 8);
        const blocks_y = @divFloor(@as(usize, height) + 7, 8);
        const total_mcus = blocks_x * blocks_y;
        var mcus_decoded: usize = 0;

        for (0..blocks_y) |block_y| {
            for (0..blocks_x) |block_x| {
                const coeffs = try decodeArithmeticSequentialBlock(
                    &decoder,
                    &dc_stats,
                    &ac_stats,
                    &fixed_bin,
                    structure.arithmetic_dc_l,
                    structure.arithmetic_dc_u,
                    structure.arithmetic_ac_k,
                    scan.components[0].dc_table_id,
                    scan.components[0].ac_table_id,
                    &last_dc_values[0],
                    &dc_contexts[0],
                );
                const spatial = dequantizeAndInverseDctWithSamplePrecision(coeffs, quant_table, structure.info.bits_per_sample);
                writeGrayscaleBlockRgba(rgba, width, height, block_x, block_y, spatial);

                mcus_decoded += 1;
                if (restart_interval != 0 and mcus_decoded < total_mcus and mcus_decoded % restart_interval == 0) {
                    try decoder.consumeExpectedRestartMarker(0xd0 + restart_index);
                    restart_index = (restart_index + 1) & 0x7;
                    dc_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_dc_stat_bins]u8);
                    ac_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_ac_stat_bins]u8);
                    fixed_bin = [_]u8{0} ** 4;
                    fixed_bin[0] = 113;
                    last_dc_values = [_]i32{0} ** max_components;
                    dc_contexts = [_]u8{0} ** max_components;
                }
            }
        }
    } else {
        var component_quant_tables: [max_components][64]u16 = undefined;
        var max_h: u8 = 1;
        var max_v: u8 = 1;
        for (0..structure.info.component_count) |frame_index| {
            const component = structure.info.components[frame_index];
            component_quant_tables[frame_index] = quantTableNaturalOrder(structure.quant_tables[component.quant_table_id]);
            if (component.horizontal_sampling > max_h) max_h = component.horizontal_sampling;
            if (component.vertical_sampling > max_v) max_v = component.vertical_sampling;
        }

        const mcu_cols = @divFloor(@as(usize, width) + (@as(usize, max_h) * 8 - 1), @as(usize, max_h) * 8);
        const mcu_rows = @divFloor(@as(usize, height) + (@as(usize, max_v) * 8 - 1), @as(usize, max_v) * 8);
        const total_mcus = mcu_cols * mcu_rows;
        var mcus_decoded: usize = 0;
        var component_planes = try initComponentSamplePlanes(
            alloc,
            width,
            height,
            structure.info.components,
            structure.info.component_count,
            max_h,
            max_v,
        );
        defer freeComponentSamplePlanes(alloc, &component_planes);

        for (0..mcu_rows) |mcu_y| {
            for (0..mcu_cols) |mcu_x| {
                for (0..structure.info.component_count) |frame_index| {
                    const component = structure.info.components[frame_index];
                    const scan_component = scan.components[frame_index];
                    const block_count = @as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling);
                    for (0..block_count) |block_index| {
                        const coeffs = try decodeArithmeticSequentialBlock(
                            &decoder,
                            &dc_stats,
                            &ac_stats,
                            &fixed_bin,
                            structure.arithmetic_dc_l,
                            structure.arithmetic_dc_u,
                            structure.arithmetic_ac_k,
                            scan_component.dc_table_id,
                            scan_component.ac_table_id,
                            &last_dc_values[frame_index],
                            &dc_contexts[frame_index],
                        );
                        const spatial = dequantizeAndInverseDctNativeWithSamplePrecision(
                            coeffs,
                            component_quant_tables[frame_index],
                            structure.info.bits_per_sample,
                        );
                        const block_col = block_index % @as(usize, component.horizontal_sampling);
                        const block_row = block_index / @as(usize, component.horizontal_sampling);
                        const plane_block_x = mcu_x * @as(usize, component.horizontal_sampling) + block_col;
                        const plane_block_y = mcu_y * @as(usize, component.vertical_sampling) + block_row;
                        writeSpatialBlockToPlane(component_planes[frame_index], plane_block_x, plane_block_y, spatial);
                    }
                }

                mcus_decoded += 1;
                if (restart_interval != 0 and mcus_decoded < total_mcus and mcus_decoded % restart_interval == 0) {
                    try decoder.consumeExpectedRestartMarker(0xd0 + restart_index);
                    restart_index = (restart_index + 1) & 0x7;
                    dc_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_dc_stat_bins]u8);
                    ac_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_ac_stat_bins]u8);
                    fixed_bin = [_]u8{0} ** 4;
                    fixed_bin[0] = 113;
                    last_dc_values = [_]i32{0} ** max_components;
                    dc_contexts = [_]u8{0} ** max_components;
                }
            }
        }

        renderColorPlanesToRgba(rgba, width, height, structure.info.bits_per_sample, color_encoding, structure.info.components, max_h, max_v, component_planes);
    }

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn decodeArithmeticSequentialBlock(
    decoder: *ArithmeticDecoder,
    dc_stats: *[max_arithmetic_tables][arithmetic_dc_stat_bins]u8,
    ac_stats: *[max_arithmetic_tables][arithmetic_ac_stat_bins]u8,
    fixed_bin: *[4]u8,
    arithmetic_dc_l: [max_arithmetic_tables]u8,
    arithmetic_dc_u: [max_arithmetic_tables]u8,
    arithmetic_ac_k: [max_arithmetic_tables]u8,
    dc_table_id: u8,
    ac_table_id: u8,
    last_dc_value: *i32,
    dc_context: *u8,
) ![64]i16 {
    var coefficients = std.mem.zeroes([64]i16);

    const dc_diff = try decodeArithmeticDc(
        decoder,
        &dc_stats[dc_table_id],
        arithmetic_dc_l[dc_table_id],
        arithmetic_dc_u[dc_table_id],
        dc_context,
    );
    last_dc_value.* = wrapArithmeticDcPredictor(last_dc_value.*, dc_diff);
    coefficients[0] = arithmeticPredictorToCoefficient(last_dc_value.*);

    try decodeArithmeticAc(
        decoder,
        &ac_stats[ac_table_id],
        fixed_bin,
        arithmetic_ac_k[ac_table_id],
        &coefficients,
    );

    return coefficients;
}

fn decodeArithmeticDc(
    decoder: *ArithmeticDecoder,
    stats: *[arithmetic_dc_stat_bins]u8,
    dc_l: u8,
    dc_u: u8,
    dc_context: *u8,
) !i32 {
    var st_index: usize = dc_context.*;
    if ((try arithDecode(decoder, &stats[st_index])) == 0) {
        dc_context.* = 0;
        return 0;
    }

    const sign = try arithDecode(decoder, &stats[st_index + 1]);
    st_index += 2 + sign;
    var magnitude: i32 = try arithDecode(decoder, &stats[st_index]);
    if (magnitude != 0) {
        st_index = 20;
        while ((try arithDecode(decoder, &stats[st_index])) != 0) {
            magnitude <<= 1;
            if (magnitude == 0x8000) return error.JpegDecodeFailed;
            st_index += 1;
        }
    }

    if (magnitude < ((@as(i32, 1) << @as(u5, @intCast(dc_l))) >> 1)) {
        dc_context.* = 0;
    } else if (magnitude > ((@as(i32, 1) << @as(u5, @intCast(dc_u))) >> 1)) {
        dc_context.* = @intCast(12 + sign * 4);
    } else {
        dc_context.* = @intCast(4 + sign * 4);
    }

    var value = magnitude;
    st_index += 14;
    var mask = magnitude;
    while (true) {
        mask >>= 1;
        if (mask == 0) break;
        if ((try arithDecode(decoder, &stats[st_index])) != 0) value |= mask;
    }
    value += 1;
    if (sign != 0) value = -value;
    return value;
}

fn decodeArithmeticAc(
    decoder: *ArithmeticDecoder,
    stats: *[arithmetic_ac_stat_bins]u8,
    fixed_bin: *[4]u8,
    arithmetic_k: u8,
    coefficients: *[64]i16,
) !void {
    var k: usize = 1;
    while (k <= 63) {
        var st_index = 3 * (k - 1);
        if ((try arithDecode(decoder, &stats[st_index])) != 0) break;
        while ((try arithDecode(decoder, &stats[st_index + 1])) == 0) {
            st_index += 3;
            k += 1;
            if (k > 63) return error.JpegDecodeFailed;
        }

        const sign = try arithDecode(decoder, &fixed_bin[0]);
        st_index += 2;
        var magnitude: i32 = try arithDecode(decoder, &stats[st_index]);
        if (magnitude != 0) {
            if ((try arithDecode(decoder, &stats[st_index])) != 0) {
                magnitude <<= 1;
                st_index = if (k <= arithmetic_k) 189 else 217;
                while ((try arithDecode(decoder, &stats[st_index])) != 0) {
                    magnitude <<= 1;
                    if (magnitude == 0x8000) return error.JpegDecodeFailed;
                    st_index += 1;
                }
            }
        }

        var value = magnitude;
        st_index += 14;
        var mask = magnitude;
        while (true) {
            mask >>= 1;
            if (mask == 0) break;
            if ((try arithDecode(decoder, &stats[st_index])) != 0) value |= mask;
        }
        value += 1;
        if (sign != 0) value = -value;
        coefficients[zig_zag_to_natural[k]] = @intCast(value);
        k += 1;
    }
}

fn arithDecode(decoder: *ArithmeticDecoder, st: *u8) !u8 {
    while (decoder.a < 0x8000) {
        decoder.ct -= 1;
        if (decoder.ct < 0) {
            const data = readArithmeticByte(decoder);
            decoder.c = (decoder.c << 8) | data;
            decoder.ct += 8;
            if (decoder.ct < 0) {
                decoder.ct += 1;
                if (decoder.ct == 0) decoder.a = 0x8000;
            }
        }
        decoder.a <<= 1;
    }

    const sv = st.*;
    const entry = jpeg_aritab[sv & 0x7f];
    const nl = @as(u8, @intCast(entry & 0xff));
    const nm = @as(u8, @intCast((entry >> 8) & 0xff));
    const qe = entry >> 16;

    var symbol = sv;
    const interval = decoder.a - qe;
    decoder.a = interval;
    const threshold = interval << @as(u5, @intCast(decoder.ct));
    if (decoder.c >= threshold) {
        decoder.c -= threshold;
        if (decoder.a < qe) {
            decoder.a = qe;
            st.* = (sv & 0x80) ^ nm;
        } else {
            decoder.a = qe;
            st.* = (sv & 0x80) ^ nl;
            symbol ^= 0x80;
        }
    } else if (decoder.a < 0x8000) {
        if (decoder.a < qe) {
            st.* = (sv & 0x80) ^ nl;
            symbol ^= 0x80;
        } else {
            st.* = (sv & 0x80) ^ nm;
        }
    }

    return symbol >> 7;
}

fn readArithmeticByte(decoder: *ArithmeticDecoder) u64 {
    if (decoder.unread_marker != null) return 0;
    if (decoder.cursor >= decoder.bytes.len) return 0;

    var data = decoder.bytes[decoder.cursor];
    decoder.cursor += 1;
    if (data != 0xff) return data;

    while (decoder.cursor < decoder.bytes.len and decoder.bytes[decoder.cursor] == 0xff) {
        decoder.cursor += 1;
    }
    if (decoder.cursor >= decoder.bytes.len) {
        decoder.unread_marker = 0xd9;
        return 0;
    }

    data = decoder.bytes[decoder.cursor];
    decoder.cursor += 1;
    if (data == 0x00) return 0xff;
    decoder.unread_marker = data;
    return 0;
}

fn decodeRgbaPureZigGrayscaleBaseline(
    alloc: Allocator,
    jpeg_bytes: []const u8,
    structure: Structure,
) !DecodedImage {
    const width = structure.info.width;
    const height = structure.info.height;
    const pixel_count = @as(usize, width) * @as(usize, height);
    const rgba = try alloc.alloc(u8, pixel_count * 4);
    errdefer alloc.free(rgba);

    const component = structure.info.components[0];
    const scan = structure.scans[0];
    const dc_table = try buildCanonicalHuffmanTable(scan.dc_tables[scan.components[0].dc_table_id]);
    const ac_table = try buildCanonicalHuffmanTable(scan.ac_tables[scan.components[0].ac_table_id]);
    const quant_table = quantTableNaturalOrder(structure.quant_tables[component.quant_table_id]);
    if (scan.entropy_start > jpeg_bytes.len) return error.JpegDecodeFailed;
    const entropy_bytes = jpeg_bytes[scan.entropy_start..];

    var reader = EntropyBitReader.init(entropy_bytes);
    var dc_predictor: i64 = 0;
    const blocks_x = @divFloor(@as(usize, width) + 7, 8);
    const blocks_y = @divFloor(@as(usize, height) + 7, 8);
    const restart_interval = structure.restart_interval orelse 0;
    var restart_index: u8 = 0;
    var mcus_decoded: usize = 0;
    const total_mcus = blocks_x * blocks_y;

    for (0..blocks_y) |block_y| {
        for (0..blocks_x) |block_x| {
            const block = try decodeBaselineBlock(&reader, dc_table, ac_table, dc_predictor);
            dc_predictor = block.dc_predictor;

            const spatial = dequantizeAndInverseDctWithSamplePrecision(block.coefficients, quant_table, structure.info.bits_per_sample);
            writeGrayscaleBlockRgba(rgba, width, height, block_x, block_y, spatial);

            mcus_decoded += 1;
            if (restart_interval != 0 and mcus_decoded < total_mcus and mcus_decoded % restart_interval == 0) {
                try reader.consumeExpectedRestartMarker(0xd0 + restart_index);
                restart_index = (restart_index + 1) & 0x7;
                dc_predictor = 0;
            }
        }
    }

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn decodeRgbaPureZigColorBaseline(
    alloc: Allocator,
    jpeg_bytes: []const u8,
    structure: Structure,
) !DecodedImage {
    const width = structure.info.width;
    const height = structure.info.height;
    const pixel_count = @as(usize, width) * @as(usize, height);
    const rgba = try alloc.alloc(u8, pixel_count * 4);
    errdefer alloc.free(rgba);
    const color_encoding = colorEncodingForStructure(structure) orelse return error.JpegDecodeFailed;

    const scan = structure.scans[0];
    const entropy_bytes = try scanEntropyBytes(structure, jpeg_bytes, 0);
    var reader = EntropyBitReader.init(entropy_bytes);
    var dc_predictors = [_]i64{0} ** max_components;
    const restart_interval = structure.restart_interval orelse 0;
    var restart_index: u8 = 0;

    var component_dc_tables: [max_components]CanonicalHuffmanTable = undefined;
    var component_ac_tables: [max_components]CanonicalHuffmanTable = undefined;
    var component_quant_tables: [max_components][64]u16 = undefined;
    var max_h: u8 = 1;
    var max_v: u8 = 1;

    for (0..structure.info.component_count) |frame_index| {
        const component = structure.info.components[frame_index];
        const scan_component = scan.components[frame_index];
        component_dc_tables[frame_index] = try buildCanonicalHuffmanTable(scan.dc_tables[scan_component.dc_table_id]);
        component_ac_tables[frame_index] = try buildCanonicalHuffmanTable(scan.ac_tables[scan_component.ac_table_id]);
        component_quant_tables[frame_index] = quantTableNaturalOrder(structure.quant_tables[component.quant_table_id]);
        if (component.horizontal_sampling > max_h) max_h = component.horizontal_sampling;
        if (component.vertical_sampling > max_v) max_v = component.vertical_sampling;
    }

    const blocks_x = @divFloor(@as(usize, width) + (@as(usize, max_h) * 8 - 1), @as(usize, max_h) * 8);
    const blocks_y = @divFloor(@as(usize, height) + (@as(usize, max_v) * 8 - 1), @as(usize, max_v) * 8);
    const total_mcus = blocks_x * blocks_y;
    var mcus_decoded: usize = 0;

    var component_planes = try initComponentSamplePlanes(
        alloc,
        width,
        height,
        structure.info.components,
        structure.info.component_count,
        max_h,
        max_v,
    );
    defer freeComponentSamplePlanes(alloc, &component_planes);

    for (0..blocks_y) |mcu_y| {
        for (0..blocks_x) |mcu_x| {
            for (0..structure.info.component_count) |frame_index| {
                const component = structure.info.components[frame_index];
                const block_count = @as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling);
                for (0..block_count) |block_index| {
                    const block = decodeBaselineBlock(
                        &reader,
                        component_dc_tables[frame_index],
                        component_ac_tables[frame_index],
                        dc_predictors[frame_index],
                    ) catch |err| return err;
                    dc_predictors[frame_index] = block.dc_predictor;
                    const spatial = dequantizeAndInverseDctNativeWithSamplePrecision(
                        block.coefficients,
                        component_quant_tables[frame_index],
                        structure.info.bits_per_sample,
                    );
                    const block_col = block_index % @as(usize, component.horizontal_sampling);
                    const block_row = block_index / @as(usize, component.horizontal_sampling);
                    const plane_block_x = mcu_x * @as(usize, component.horizontal_sampling) + block_col;
                    const plane_block_y = mcu_y * @as(usize, component.vertical_sampling) + block_row;
                    writeSpatialBlockToPlane(component_planes[frame_index], plane_block_x, plane_block_y, spatial);
                }
            }

            mcus_decoded += 1;
            if (restart_interval != 0 and mcus_decoded < total_mcus and mcus_decoded % restart_interval == 0) {
                try reader.consumeExpectedRestartMarker(0xd0 + restart_index);
                restart_index = (restart_index + 1) & 0x7;
                dc_predictors = [_]i64{0} ** max_components;
            }
        }
    }

    renderColorPlanesToRgba(rgba, width, height, structure.info.bits_per_sample, color_encoding, structure.info.components, max_h, max_v, component_planes);

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn decodeRgbaPureZigLossless(
    alloc: Allocator,
    jpeg_bytes: []const u8,
    structure: Structure,
) !DecodedImage {
    const width = structure.info.width;
    const height = structure.info.height;
    const pixel_count = @as(usize, width) * @as(usize, height);
    const rgba = try alloc.alloc(u8, pixel_count * 4);
    errdefer alloc.free(rgba);

    const scan = structure.scans[0];
    const entropy_bytes = try scanEntropyBytes(structure, jpeg_bytes, 0);
    var reader = EntropyBitReader.init(entropy_bytes);
    const restart_interval = structure.restart_interval orelse 0;
    var restart_index: u8 = 0;
    var mcus_decoded: usize = 0;
    const total_mcus = @as(usize, width) * @as(usize, height);

    const predictor_selection = scan.spectral_start;
    const point_transform = scan.successive_approx_low;
    const bits_per_sample = structure.info.bits_per_sample;
    const initial_predictor = @as(i32, 1) << @as(u5, @intCast(bits_per_sample - point_transform - 1));
    const reduced_sample_bits = bits_per_sample - point_transform;
    const reduced_sample_modulus = @as(i64, 1) << @as(u6, @intCast(reduced_sample_bits));

    var component_dc_tables: [max_components]CanonicalHuffmanTable = undefined;
    for (0..structure.info.component_count) |frame_index| {
        component_dc_tables[frame_index] = try buildCanonicalHuffmanTable(scan.dc_tables[scan.components[frame_index].dc_table_id]);
    }

    const row_samples = try alloc.alloc(i32, @as(usize, width) * @as(usize, structure.info.component_count) * 2);
    defer alloc.free(row_samples);
    @memset(row_samples, 0);

    var prev_rows: [max_components][]i32 = undefined;
    var curr_rows: [max_components][]i32 = undefined;
    for (0..structure.info.component_count) |frame_index| {
        const base = frame_index * @as(usize, width) * 2;
        prev_rows[frame_index] = row_samples[base .. base + @as(usize, width)];
        curr_rows[frame_index] = row_samples[base + @as(usize, width) .. base + (@as(usize, width) * 2)];
    }

    var restart_reset = true;
    for (0..height) |y| {
        for (0..structure.info.component_count) |frame_index| {
            @memset(curr_rows[frame_index], 0);
        }

        for (0..width) |x| {
            var rgb: [3]u8 = undefined;
            var gray: u8 = 0;

            for (0..structure.info.component_count) |frame_index| {
                const predictor = if (restart_reset)
                    initial_predictor
                else
                    losslessPredictor(
                        predictor_selection,
                        if (x > 0) curr_rows[frame_index][x - 1] else null,
                        if (y > 0) prev_rows[frame_index][x] else null,
                        if (x > 0 and y > 0) prev_rows[frame_index][x - 1] else null,
                        initial_predictor,
                    );

                const diff = try decodeLosslessDifference(&reader, component_dc_tables[frame_index]);
                const reduced_sample = @mod(@as(i64, predictor) + diff, reduced_sample_modulus);
                curr_rows[frame_index][x] = @intCast(reduced_sample);

                const restored_sample: i32 = @intCast(reduced_sample << @as(u5, @intCast(point_transform)));
                const sample_u8 = scaleSampleToByte(restored_sample, bits_per_sample);
                if (structure.info.component_count == 1) {
                    gray = sample_u8;
                } else {
                    rgb[frame_index] = sample_u8;
                }
            }

            const pixel_offset = (@as(usize, y) * @as(usize, width) + @as(usize, x)) * 4;
            if (structure.info.component_count == 1) {
                rgba[pixel_offset + 0] = gray;
                rgba[pixel_offset + 1] = gray;
                rgba[pixel_offset + 2] = gray;
            } else {
                const color_encoding = colorEncodingForStructure(structure) orelse return error.JpegDecodeFailed;
                const out_rgb = switch (color_encoding) {
                    .rgb => rgb,
                    .ycbcr => ycbcrToRgb(rgb[0], rgb[1], rgb[2]),
                    else => return error.JpegDecodeFailed,
                };
                rgba[pixel_offset + 0] = out_rgb[0];
                rgba[pixel_offset + 1] = out_rgb[1];
                rgba[pixel_offset + 2] = out_rgb[2];
            }
            rgba[pixel_offset + 3] = 0xff;

            restart_reset = false;
            mcus_decoded += 1;
            if (restart_interval != 0 and mcus_decoded < total_mcus and mcus_decoded % restart_interval == 0) {
                try reader.consumeExpectedRestartMarker(0xd0 + restart_index);
                restart_index = (restart_index + 1) & 0x7;
                restart_reset = true;
            }
        }

        for (0..structure.info.component_count) |frame_index| {
            @memcpy(prev_rows[frame_index], curr_rows[frame_index]);
        }
    }

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn decodeRgbaPureZigProgressive(
    alloc: Allocator,
    jpeg_bytes: []const u8,
    structure: Structure,
) !DecodedImage {
    const width = structure.info.width;
    const height = structure.info.height;
    const pixel_count = @as(usize, width) * @as(usize, height);
    const rgba = try alloc.alloc(u8, pixel_count * 4);
    errdefer alloc.free(rgba);

    var progressive = try ProgressiveDecodeContext.init(alloc, structure);
    defer progressive.deinit(alloc);

    var dc_predictors = [_]i16{0} ** max_components;

    for (0..structure.scan_count) |scan_index| {
        try applyProgressiveScan(jpeg_bytes, structure, scan_index, &progressive, &dc_predictors);
    }

    if (structure.info.component_count == 1) {
        for (0..progressive.component_blocks_y[0]) |block_y| {
            for (0..progressive.component_blocks_x[0]) |block_x| {
                const block_index = block_y * progressive.component_blocks_x[0] + block_x;
                const spatial = dequantizeAndInverseDctWithSamplePrecision(
                    progressive.component_states[0][block_index].coefficients,
                    progressive.component_quant_tables[0],
                    structure.info.bits_per_sample,
                );
                writeGrayscaleBlockRgba(rgba, width, height, block_x, block_y, spatial);
            }
        }
    } else {
        const color_encoding = colorEncodingForStructure(structure) orelse return error.JpegDecodeFailed;
        var component_planes = try initComponentSamplePlanes(
            alloc,
            width,
            height,
            structure.info.components,
            structure.info.component_count,
            progressive.max_h,
            progressive.max_v,
        );
        defer freeComponentSamplePlanes(alloc, &component_planes);
        for (0..progressive.mcu_rows) |mcu_y| {
            for (0..progressive.mcu_cols) |mcu_x| {
                for (0..structure.info.component_count) |frame_index| {
                    const component = structure.info.components[frame_index];
                    const block_count = @as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling);
                    for (0..block_count) |block_index| {
                        const block_col = block_index % @as(usize, component.horizontal_sampling);
                        const block_row = block_index / @as(usize, component.horizontal_sampling);
                        const global_block_index = componentBlockIndexForMcu(
                            progressive.component_blocks_x[frame_index],
                            progressive.component_blocks_y[frame_index],
                            component,
                            mcu_x,
                            mcu_y,
                            block_col,
                            block_row,
                        ) orelse continue;
                        const spatial = dequantizeAndInverseDctNativeWithSamplePrecision(
                            progressive.component_states[frame_index][global_block_index].coefficients,
                            progressive.component_quant_tables[frame_index],
                            structure.info.bits_per_sample,
                        );
                        const plane_block_x = mcu_x * @as(usize, component.horizontal_sampling) + block_col;
                        const plane_block_y = mcu_y * @as(usize, component.vertical_sampling) + block_row;
                        writeSpatialBlockToPlane(component_planes[frame_index], plane_block_x, plane_block_y, spatial);
                    }
                }
            }
        }

        renderColorPlanesToRgba(rgba, width, height, structure.info.bits_per_sample, color_encoding, structure.info.components, progressive.max_h, progressive.max_v, component_planes);
    }

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn dequantizeAndInverseDct(coefficients: anytype, quant_table: [64]u16) [64]u8 {
    return dequantizeAndInverseDctWithSamplePrecision(coefficients, quant_table, 8);
}

fn dequantizeAndInverseDctNativeWithSamplePrecision(
    coefficients: anytype,
    quant_table: [64]u16,
    bits_per_sample: u8,
) [64]u16 {
    var out = std.mem.zeroes([64]u16);
    const const_bits = 13;
    const pass1_bits: u8 = if (bits_per_sample <= 8) 2 else 1;
    const sample_center: i64 = @as(i64, 1) << @as(u6, @intCast(bits_per_sample - 1));

    const fix_0_298631336: i64 = 2446;
    const fix_0_390180644: i64 = 3196;
    const fix_0_541196100: i64 = 4433;
    const fix_0_765366865: i64 = 6270;
    const fix_0_899976223: i64 = 7373;
    const fix_1_175875602: i64 = 9633;
    const fix_1_501321110: i64 = 12299;
    const fix_1_847759065: i64 = 15137;
    const fix_1_961570560: i64 = 16069;
    const fix_2_053119869: i64 = 16819;
    const fix_2_562915447: i64 = 20995;
    const fix_3_072711026: i64 = 25172;

    const dequantized = dequantizeCoefficientsSimd(coefficients, quant_table);
    var workspace = std.mem.zeroes([64]i64);

    for (0..8) |col| {
        if (dequantized[col + 8] == 0 and
            dequantized[col + 16] == 0 and
            dequantized[col + 24] == 0 and
            dequantized[col + 32] == 0 and
            dequantized[col + 40] == 0 and
            dequantized[col + 48] == 0 and
            dequantized[col + 56] == 0)
        {
            const dc = @as(i64, dequantized[col]);
            const dcval = dc << @as(u6, @intCast(pass1_bits));
            for (0..8) |row| {
                workspace[row * 8 + col] = dcval;
            }
            continue;
        }

        const z2 = @as(i64, dequantized[col + 16]);
        const z3 = @as(i64, dequantized[col + 48]);
        const z1 = multiplyIjpeg(z2 + z3, fix_0_541196100);
        const tmp2 = z1 + multiplyIjpeg(z3, -fix_1_847759065);
        const tmp3 = z1 + multiplyIjpeg(z2, fix_0_765366865);
        const z2_dc = @as(i64, dequantized[col + 0]);
        const z3_dc = @as(i64, dequantized[col + 32]);
        const tmp0 = (z2_dc + z3_dc) << const_bits;
        const tmp1 = (z2_dc - z3_dc) << const_bits;
        const tmp10 = tmp0 + tmp3;
        const tmp13 = tmp0 - tmp3;
        const tmp11 = tmp1 + tmp2;
        const tmp12 = tmp1 - tmp2;

        var odd0 = @as(i64, dequantized[col + 56]);
        var odd1 = @as(i64, dequantized[col + 40]);
        var odd2 = @as(i64, dequantized[col + 24]);
        var odd3 = @as(i64, dequantized[col + 8]);
        const zz1 = odd0 + odd3;
        const zz2 = odd1 + odd2;
        const zz3 = odd0 + odd2;
        const zz4 = odd1 + odd3;
        const zz5 = multiplyIjpeg(zz3 + zz4, fix_1_175875602);

        odd0 = multiplyIjpeg(odd0, fix_0_298631336);
        odd1 = multiplyIjpeg(odd1, fix_2_053119869);
        odd2 = multiplyIjpeg(odd2, fix_3_072711026);
        odd3 = multiplyIjpeg(odd3, fix_1_501321110);

        const zz1_scaled = multiplyIjpeg(zz1, -fix_0_899976223);
        const zz2_scaled = multiplyIjpeg(zz2, -fix_2_562915447);
        const zz3_scaled = multiplyIjpeg(zz3, -fix_1_961570560) + zz5;
        const zz4_scaled = multiplyIjpeg(zz4, -fix_0_390180644) + zz5;

        odd0 += zz1_scaled + zz3_scaled;
        odd1 += zz2_scaled + zz4_scaled;
        odd2 += zz2_scaled + zz3_scaled;
        odd3 += zz1_scaled + zz4_scaled;

        workspace[0 * 8 + col] = @intCast(descaleIjpeg(tmp10 + odd3, const_bits - pass1_bits));
        workspace[7 * 8 + col] = @intCast(descaleIjpeg(tmp10 - odd3, const_bits - pass1_bits));
        workspace[1 * 8 + col] = @intCast(descaleIjpeg(tmp11 + odd2, const_bits - pass1_bits));
        workspace[6 * 8 + col] = @intCast(descaleIjpeg(tmp11 - odd2, const_bits - pass1_bits));
        workspace[2 * 8 + col] = @intCast(descaleIjpeg(tmp12 + odd1, const_bits - pass1_bits));
        workspace[5 * 8 + col] = @intCast(descaleIjpeg(tmp12 - odd1, const_bits - pass1_bits));
        workspace[3 * 8 + col] = @intCast(descaleIjpeg(tmp13 + odd0, const_bits - pass1_bits));
        workspace[4 * 8 + col] = @intCast(descaleIjpeg(tmp13 - odd0, const_bits - pass1_bits));
    }

    for (0..8) |row| {
        const row_offset = row * 8;
        if (workspace[row_offset + 1] == 0 and
            workspace[row_offset + 2] == 0 and
            workspace[row_offset + 3] == 0 and
            workspace[row_offset + 4] == 0 and
            workspace[row_offset + 5] == 0 and
            workspace[row_offset + 6] == 0 and
            workspace[row_offset + 7] == 0)
        {
            const dc = limitNativeIdctSample(descaleIjpeg(workspace[row_offset], pass1_bits + 3), sample_center, bits_per_sample);
            for (0..8) |col| {
                out[row_offset + col] = @intCast(dc);
            }
            continue;
        }

        const z2 = @as(i64, workspace[row_offset + 2]);
        const z3 = @as(i64, workspace[row_offset + 6]);
        const z1 = multiplyIjpeg(z2 + z3, fix_0_541196100);
        const tmp2 = z1 + multiplyIjpeg(z3, -fix_1_847759065);
        const tmp3 = z1 + multiplyIjpeg(z2, fix_0_765366865);
        const tmp0 = (@as(i64, workspace[row_offset + 0]) + @as(i64, workspace[row_offset + 4])) << const_bits;
        const tmp1 = (@as(i64, workspace[row_offset + 0]) - @as(i64, workspace[row_offset + 4])) << const_bits;
        const tmp10 = tmp0 + tmp3;
        const tmp13 = tmp0 - tmp3;
        const tmp11 = tmp1 + tmp2;
        const tmp12 = tmp1 - tmp2;

        var odd0 = @as(i64, workspace[row_offset + 7]);
        var odd1 = @as(i64, workspace[row_offset + 5]);
        var odd2 = @as(i64, workspace[row_offset + 3]);
        var odd3 = @as(i64, workspace[row_offset + 1]);
        const zz1 = odd0 + odd3;
        const zz2 = odd1 + odd2;
        const zz3 = odd0 + odd2;
        const zz4 = odd1 + odd3;
        const zz5 = multiplyIjpeg(zz3 + zz4, fix_1_175875602);

        odd0 = multiplyIjpeg(odd0, fix_0_298631336);
        odd1 = multiplyIjpeg(odd1, fix_2_053119869);
        odd2 = multiplyIjpeg(odd2, fix_3_072711026);
        odd3 = multiplyIjpeg(odd3, fix_1_501321110);

        const zz1_scaled = multiplyIjpeg(zz1, -fix_0_899976223);
        const zz2_scaled = multiplyIjpeg(zz2, -fix_2_562915447);
        const zz3_scaled = multiplyIjpeg(zz3, -fix_1_961570560) + zz5;
        const zz4_scaled = multiplyIjpeg(zz4, -fix_0_390180644) + zz5;

        odd0 += zz1_scaled + zz3_scaled;
        odd1 += zz2_scaled + zz4_scaled;
        odd2 += zz2_scaled + zz3_scaled;
        odd3 += zz1_scaled + zz4_scaled;

        out[row_offset + 0] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp10 + odd3, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
        out[row_offset + 7] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp10 - odd3, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
        out[row_offset + 1] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp11 + odd2, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
        out[row_offset + 6] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp11 - odd2, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
        out[row_offset + 2] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp12 + odd1, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
        out[row_offset + 5] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp12 - odd1, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
        out[row_offset + 3] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp13 + odd0, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
        out[row_offset + 4] = @intCast(limitNativeIdctSample(descaleIjpeg(tmp13 - odd0, const_bits + pass1_bits + 3), sample_center, bits_per_sample));
    }

    return out;
}

fn dequantizeAndInverseDctWithSamplePrecision(
    coefficients: anytype,
    quant_table: [64]u16,
    bits_per_sample: u8,
) [64]u8 {
    const native = dequantizeAndInverseDctNativeWithSamplePrecision(coefficients, quant_table, bits_per_sample);
    var out = std.mem.zeroes([64]u8);
    for (native, 0..) |sample, i| {
        out[i] = scaleSampleToByteWide(sample, bits_per_sample);
    }
    return out;
}

fn rangeLimitSampleIjpeg(value: i64, bits_per_sample: u8) i64 {
    const sample_count: i64 = @as(i64, 1) << @as(u6, @intCast(bits_per_sample));
    const center: i64 = sample_count >> 1;
    const range_mask: u64 = @intCast(sample_count * 4 - 1);
    const idx: i64 = @intCast(@as(u64, @bitCast(value)) & range_mask);
    if (idx < center) return idx + center;
    if (idx < sample_count * 2) return sample_count - 1;
    if (idx < sample_count * 4 - center) return 0;
    return idx - (sample_count * 4 - center);
}

fn limitNativeIdctSample(value: i64, sample_center: i64, bits_per_sample: u8) i64 {
    if (bits_per_sample == 8) return rangeLimitSampleIjpeg(value, bits_per_sample);

    const sample_max: i64 = (@as(i64, 1) << @as(u6, @intCast(bits_per_sample))) - 1;
    return std.math.clamp(value + sample_center, 0, sample_max);
}

fn dequantizeCoefficientsSimd(coefficients: anytype, quant_table: [64]u16) [64]i64 {
    var out: [64]i64 = undefined;
    for (0..64) |i| {
        out[i] = @as(i64, coefficients[i]) * @as(i64, quant_table[i]);
    }
    return out;
}

fn multiplyIjpeg(value: i64, constant: i64) i64 {
    return value * constant;
}

fn descaleIjpeg(value: i64, bits: anytype) i64 {
    const shift: u6 = @intCast(bits);
    return (value + (@as(i64, 1) << @intCast(shift - 1))) >> shift;
}

fn clampToByte(value: i32) u8 {
    if (value < 0) return 0;
    if (value > 255) return 255;
    return @intCast(value);
}

fn scaleSampleToByte(value: i32, bits_per_sample: u8) u8 {
    if (bits_per_sample == 8) return clampToByte(value);

    const sample_max = (@as(i32, 1) << @as(u5, @intCast(bits_per_sample))) - 1;
    const clamped = std.math.clamp(value, 0, sample_max);
    return @intCast(@divFloor(@as(i64, clamped) * 255 + @divFloor(sample_max, 2), sample_max));
}

fn scaleSampleToByteWide(value: i64, bits_per_sample: u8) u8 {
    if (bits_per_sample == 8) return clampToByte(std.math.lossyCast(i32, value));

    const sample_max = (@as(i64, 1) << @as(u6, @intCast(bits_per_sample))) - 1;
    const clamped = std.math.clamp(value, 0, sample_max);
    return @intCast(@divFloor(clamped * 255 + @divFloor(sample_max, 2), sample_max));
}

fn decodeEmbeddedBase64Alloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const decoder = std.base64.standard.Decoder;
    const out_len = try decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, out_len);
    errdefer alloc.free(out);
    try decoder.decode(out, encoded);
    return out;
}

fn initComponentSamplePlanes(
    alloc: Allocator,
    width: u32,
    height: u32,
    components: [max_components]ComponentInfo,
    component_count: u8,
    max_h: u8,
    max_v: u8,
) ![max_components]SamplePlane {
    var planes = std.mem.zeroes([max_components]SamplePlane);
    errdefer freeComponentSamplePlanes(alloc, &planes);

    for (0..component_count) |i| {
        const component = components[i];
        const plane_width = @divFloor(@as(usize, width) * @as(usize, component.horizontal_sampling) + @as(usize, max_h) - 1, @as(usize, max_h));
        const plane_height = @divFloor(@as(usize, height) * @as(usize, component.vertical_sampling) + @as(usize, max_v) - 1, @as(usize, max_v));
        planes[i] = .{
            .width = plane_width,
            .height = plane_height,
            .samples = try alloc.alloc(u16, plane_width * plane_height),
        };
    }

    return planes;
}

fn freeComponentSamplePlanes(alloc: Allocator, planes: *[max_components]SamplePlane) void {
    for (planes) |plane| {
        if (plane.samples.len != 0) alloc.free(plane.samples);
    }
    planes.* = std.mem.zeroes([max_components]SamplePlane);
}

fn writeSpatialBlockToPlane(
    plane: SamplePlane,
    block_x: usize,
    block_y: usize,
    block: anytype,
) void {
    const start_x = block_x * 8;
    const start_y = block_y * 8;
    if (start_x >= plane.width or start_y >= plane.height) return;

    for (0..8) |local_y| {
        const sample_y = start_y + local_y;
        if (sample_y >= plane.height) break;

        const src_off = local_y * 8;
        const dst_off = sample_y * plane.width + start_x;
        const copy_len = @min(@as(usize, 8), plane.width - start_x);
        for (0..copy_len) |i| {
            plane.samples[dst_off + i] = @intCast(block[src_off + i]);
        }
    }
}

fn renderColorPlanesToRgba(
    rgba: []u8,
    width: u32,
    height: u32,
    bits_per_sample: u8,
    color_encoding: ColorEncoding,
    components: [max_components]ComponentInfo,
    max_h: u8,
    max_v: u8,
    planes: [max_components]SamplePlane,
) void {
    const width_usize: usize = @intCast(width);
    const height_usize: usize = @intCast(height);

    if (bits_per_sample == 8 and color_encoding == .rgb and canRenderDirectRgb8(width_usize, height_usize, components, max_h, max_v, planes)) {
        renderDirectRgb8(rgba, width_usize, height_usize, planes);
        return;
    }

    if (bits_per_sample == 8 and color_encoding == .ycbcr) {
        if (ycbcrChromaMode8(width_usize, height_usize, components, max_h, max_v, planes)) |mode| {
            renderYcbcr8(rgba, width_usize, height_usize, components, max_h, max_v, planes, mode);
            return;
        }
    }

    for (0..height_usize) |y| {
        for (0..width_usize) |x| {
            const rgb = switch (color_encoding) {
                .rgb => blk: {
                    const r = samplePlaneValue(planes[0], components[0], max_h, max_v, bits_per_sample, x, y);
                    const g = samplePlaneValue(planes[1], components[1], max_h, max_v, bits_per_sample, x, y);
                    const b = samplePlaneValue(planes[2], components[2], max_h, max_v, bits_per_sample, x, y);
                    break :blk if (bits_per_sample == 8)
                        [3]u8{ @intCast(r), @intCast(g), @intCast(b) }
                    else
                        [3]u8{
                            scaleSampleToByteWide(r, bits_per_sample),
                            scaleSampleToByteWide(g, bits_per_sample),
                            scaleSampleToByteWide(b, bits_per_sample),
                        };
                },
                .ycbcr => blk: {
                    const yy = samplePlaneValue(planes[0], components[0], max_h, max_v, bits_per_sample, x, y);
                    const cb = samplePlaneValue(planes[1], components[1], max_h, max_v, bits_per_sample, x, y);
                    const cr = samplePlaneValue(planes[2], components[2], max_h, max_v, bits_per_sample, x, y);
                    break :blk if (bits_per_sample == 8)
                        ycbcrToRgb(@intCast(yy), @intCast(cb), @intCast(cr))
                    else
                        ycbcrToRgbWide(yy, cb, cr, bits_per_sample);
                },
                .cmyk => blk: {
                    const c = samplePlaneValue(planes[0], components[0], max_h, max_v, bits_per_sample, x, y);
                    const m = samplePlaneValue(planes[1], components[1], max_h, max_v, bits_per_sample, x, y);
                    const yy = samplePlaneValue(planes[2], components[2], max_h, max_v, bits_per_sample, x, y);
                    const k = samplePlaneValue(planes[3], components[3], max_h, max_v, bits_per_sample, x, y);
                    break :blk if (bits_per_sample == 8)
                        invertedCmykToRgb(@intCast(c), @intCast(m), @intCast(yy), @intCast(k))
                    else
                        invertedCmykToRgbWide(c, m, yy, k, bits_per_sample);
                },
                .ycck => blk: {
                    const yy = samplePlaneValue(planes[0], components[0], max_h, max_v, bits_per_sample, x, y);
                    const cb = samplePlaneValue(planes[1], components[1], max_h, max_v, bits_per_sample, x, y);
                    const cr = samplePlaneValue(planes[2], components[2], max_h, max_v, bits_per_sample, x, y);
                    const k = samplePlaneValue(planes[3], components[3], max_h, max_v, bits_per_sample, x, y);
                    break :blk if (bits_per_sample == 8)
                        ycckToRgb(@intCast(yy), @intCast(cb), @intCast(cr), @intCast(k))
                    else
                        ycckToRgbWide(yy, cb, cr, k, bits_per_sample);
                },
            };

            const pixel_index = (y * width_usize + x) * 4;
            rgba[pixel_index + 0] = rgb[0];
            rgba[pixel_index + 1] = rgb[1];
            rgba[pixel_index + 2] = rgb[2];
            rgba[pixel_index + 3] = 0xff;
        }
    }
}

fn canRenderDirectRgb8(
    width: usize,
    height: usize,
    components: [max_components]ComponentInfo,
    max_h: u8,
    max_v: u8,
    planes: [max_components]SamplePlane,
) bool {
    for (0..3) |i| {
        if (components[i].horizontal_sampling != max_h or components[i].vertical_sampling != max_v) return false;
        if (planes[i].width < width or planes[i].height < height) return false;
    }
    return true;
}

fn renderDirectRgb8(rgba: []u8, width: usize, height: usize, planes: [max_components]SamplePlane) void {
    for (0..height) |y| {
        var dst = y * width * 4;
        const row_r = y * planes[0].width;
        const row_g = y * planes[1].width;
        const row_b = y * planes[2].width;
        for (0..width) |x| {
            rgba[dst + 0] = @intCast(planes[0].samples[row_r + x]);
            rgba[dst + 1] = @intCast(planes[1].samples[row_g + x]);
            rgba[dst + 2] = @intCast(planes[2].samples[row_b + x]);
            rgba[dst + 3] = 0xff;
            dst += 4;
        }
    }
}

const YcbcrChromaMode = enum {
    full,
    h2v1,
    h1v2,
    h2v2,
    nearest,
};

fn ycbcrChromaMode8(
    width: usize,
    height: usize,
    components: [max_components]ComponentInfo,
    max_h: u8,
    max_v: u8,
    planes: [max_components]SamplePlane,
) ?YcbcrChromaMode {
    if (components[0].horizontal_sampling != max_h or components[0].vertical_sampling != max_v) return null;
    if (planes[0].width < width or planes[0].height < height) return null;

    const cb_component = components[1];
    const cr_component = components[2];
    if (cb_component.horizontal_sampling != cr_component.horizontal_sampling or
        cb_component.vertical_sampling != cr_component.vertical_sampling) return null;

    if (cb_component.horizontal_sampling == max_h and cb_component.vertical_sampling == max_v) return .full;
    if (cb_component.horizontal_sampling * 2 == max_h and cb_component.vertical_sampling == max_v and planes[1].width > 2 and planes[2].width > 2) return .h2v1;
    if (cb_component.horizontal_sampling == max_h and cb_component.vertical_sampling * 2 == max_v and planes[1].height > 1 and planes[2].height > 1) return .h1v2;
    if (cb_component.horizontal_sampling * 2 == max_h and cb_component.vertical_sampling * 2 == max_v and planes[1].width > 2 and planes[2].width > 2 and planes[1].height > 1 and planes[2].height > 1) return .h2v2;
    return .nearest;
}

fn renderYcbcr8(
    rgba: []u8,
    width: usize,
    height: usize,
    components: [max_components]ComponentInfo,
    max_h: u8,
    max_v: u8,
    planes: [max_components]SamplePlane,
    chroma_mode: YcbcrChromaMode,
) void {
    switch (chroma_mode) {
        .full => renderYcbcr8Full(rgba, width, height, planes),
        .h2v1 => renderYcbcr8H2V1(rgba, width, height, planes),
        .h1v2 => renderYcbcr8H1V2(rgba, width, height, planes),
        .h2v2 => renderYcbcr8H2V2(rgba, width, height, planes),
        .nearest => renderYcbcr8Nearest(rgba, width, height, components, max_h, max_v, planes),
    }
}

fn writeYcbcr8Pixel(rgba: []u8, dst: usize, yy: u8, cb: u8, cr: u8) void {
    const rgb = ycbcrToRgb(yy, cb, cr);
    rgba[dst + 0] = rgb[0];
    rgba[dst + 1] = rgb[1];
    rgba[dst + 2] = rgb[2];
    rgba[dst + 3] = 0xff;
}

fn renderYcbcr8Full(rgba: []u8, width: usize, height: usize, planes: [max_components]SamplePlane) void {
    for (0..height) |y| {
        var dst = y * width * 4;
        const row_y = y * planes[0].width;
        const row_cb = y * planes[1].width;
        const row_cr = y * planes[2].width;
        for (0..width) |x| {
            writeYcbcr8Pixel(rgba, dst, @intCast(planes[0].samples[row_y + x]), @intCast(planes[1].samples[row_cb + x]), @intCast(planes[2].samples[row_cr + x]));
            dst += 4;
        }
    }
}

fn renderYcbcr8H2V1(rgba: []u8, width: usize, height: usize, planes: [max_components]SamplePlane) void {
    for (0..height) |y| {
        var dst = y * width * 4;
        const row_y = y * planes[0].width;
        for (0..width) |x| {
            writeYcbcr8Pixel(
                rgba,
                dst,
                @intCast(planes[0].samples[row_y + x]),
                @intCast(fancyUpsamplePlaneH2V1(planes[1], x, y)),
                @intCast(fancyUpsamplePlaneH2V1(planes[2], x, y)),
            );
            dst += 4;
        }
    }
}

fn renderYcbcr8H1V2(rgba: []u8, width: usize, height: usize, planes: [max_components]SamplePlane) void {
    for (0..height) |y| {
        var dst = y * width * 4;
        const row_y = y * planes[0].width;
        for (0..width) |x| {
            writeYcbcr8Pixel(
                rgba,
                dst,
                @intCast(planes[0].samples[row_y + x]),
                @intCast(fancyUpsamplePlaneH1V2(planes[1], x, y)),
                @intCast(fancyUpsamplePlaneH1V2(planes[2], x, y)),
            );
            dst += 4;
        }
    }
}

fn renderYcbcr8H2V2(rgba: []u8, width: usize, height: usize, planes: [max_components]SamplePlane) void {
    for (0..height) |y| {
        var dst = y * width * 4;
        const row_y = y * planes[0].width;
        for (0..width) |x| {
            writeYcbcr8Pixel(
                rgba,
                dst,
                @intCast(planes[0].samples[row_y + x]),
                @intCast(fancyUpsamplePlaneH2V2(planes[1], x, y)),
                @intCast(fancyUpsamplePlaneH2V2(planes[2], x, y)),
            );
            dst += 4;
        }
    }
}

fn renderYcbcr8Nearest(
    rgba: []u8,
    width: usize,
    height: usize,
    components: [max_components]ComponentInfo,
    max_h: u8,
    max_v: u8,
    planes: [max_components]SamplePlane,
) void {
    for (0..height) |y| {
        var dst = y * width * 4;
        const row_y = y * planes[0].width;
        const sample_y = @divFloor(y * @as(usize, components[1].vertical_sampling), @as(usize, max_v));
        for (0..width) |x| {
            const sample_x = @divFloor(x * @as(usize, components[1].horizontal_sampling), @as(usize, max_h));
            writeYcbcr8Pixel(
                rgba,
                dst,
                @intCast(planes[0].samples[row_y + x]),
                @intCast(planeSample(planes[1], sample_x, sample_y)),
                @intCast(planeSample(planes[2], sample_x, sample_y)),
            );
            dst += 4;
        }
    }
}

fn samplePlaneValue(
    plane: SamplePlane,
    component: ComponentInfo,
    max_h: u8,
    max_v: u8,
    bits_per_sample: u8,
    x: usize,
    y: usize,
) u16 {
    _ = bits_per_sample;
    if (component.horizontal_sampling == max_h and component.vertical_sampling == max_v) {
        return planeSample(plane, x, y);
    }

    if (component.horizontal_sampling * 2 == max_h and component.vertical_sampling == max_v and plane.width > 2) {
        return fancyUpsamplePlaneH2V1(plane, x, y);
    }

    if (component.horizontal_sampling == max_h and component.vertical_sampling * 2 == max_v and plane.height > 1) {
        return fancyUpsamplePlaneH1V2(plane, x, y);
    }

    if (component.horizontal_sampling * 2 == max_h and component.vertical_sampling * 2 == max_v and plane.width > 2 and plane.height > 1) {
        return fancyUpsamplePlaneH2V2(plane, x, y);
    }

    const sample_x = @divFloor(x * @as(usize, component.horizontal_sampling), @as(usize, max_h));
    const sample_y = @divFloor(y * @as(usize, component.vertical_sampling), @as(usize, max_v));
    return planeSample(plane, sample_x, sample_y);
}

fn planeSample(plane: SamplePlane, sample_x: usize, sample_y: usize) u16 {
    const clamped_x = @min(sample_x, plane.width - 1);
    const clamped_y = @min(sample_y, plane.height - 1);
    return plane.samples[clamped_y * plane.width + clamped_x];
}

fn fancyUpsamplePlaneH2V1(plane: SamplePlane, x: usize, y: usize) u16 {
    const source_x = x / 2;
    const parity = x & 1;
    const center = planeSample(plane, source_x, y);

    if (parity == 0) {
        if (source_x == 0) return center;
        const prev = planeSample(plane, source_x - 1, y);
        return @intCast((@as(u32, center) * 3 + @as(u32, prev) + 1) >> 2);
    }

    if (source_x + 1 >= plane.width) return center;
    const next = planeSample(plane, source_x + 1, y);
    return @intCast((@as(u32, center) * 3 + @as(u32, next) + 2) >> 2);
}

fn fancyUpsamplePlaneH1V2(plane: SamplePlane, x: usize, y: usize) u16 {
    const source_y = y / 2;
    const parity = y & 1;
    const far_y = if (parity == 0)
        if (source_y == 0) 0 else source_y - 1
    else if (source_y + 1 >= plane.height) plane.height - 1 else source_y + 1;
    const bias: u32 = if (parity == 0) 1 else 2;
    const near_value = planeSample(plane, x, source_y);
    const far_value = planeSample(plane, x, far_y);
    return @intCast((@as(u32, near_value) * 3 + @as(u32, far_value) + bias) >> 2);
}

fn fancyUpsamplePlaneH2V2(plane: SamplePlane, x: usize, y: usize) u16 {
    const source_x = x / 2;
    const source_y = y / 2;
    const x_parity = x & 1;
    const y_parity = y & 1;
    const far_y = if (y_parity == 0)
        if (source_y == 0) 0 else source_y - 1
    else if (source_y + 1 >= plane.height) plane.height - 1 else source_y + 1;

    const this_col_sum = fancyUpsamplePlaneColumnSum(plane, source_x, source_y, far_y);
    if (x_parity == 0) {
        if (source_x == 0) return @intCast((this_col_sum * 4 + 8) >> 4);
        const prev_col_sum = fancyUpsamplePlaneColumnSum(plane, source_x - 1, source_y, far_y);
        return @intCast((this_col_sum * 3 + prev_col_sum + 8) >> 4);
    }

    if (source_x + 1 >= plane.width) return @intCast((this_col_sum * 4 + 7) >> 4);
    const next_col_sum = fancyUpsamplePlaneColumnSum(plane, source_x + 1, source_y, far_y);
    return @intCast((this_col_sum * 3 + next_col_sum + 7) >> 4);
}

fn fancyUpsamplePlaneColumnSum(plane: SamplePlane, sample_x: usize, near_y: usize, far_y: usize) u16 {
    const near_value = planeSample(plane, sample_x, near_y);
    const far_value = planeSample(plane, sample_x, far_y);
    return @intCast(@as(u32, near_value) * 3 + @as(u32, far_value));
}

fn writeGrayscaleBlockRgba(
    rgba: []u8,
    width: u32,
    height: u32,
    block_x: usize,
    block_y: usize,
    block: [64]u8,
) void {
    const width_usize: usize = @intCast(width);
    const height_usize: usize = @intCast(height);

    for (0..8) |local_y| {
        const y = block_y * 8 + local_y;
        if (y >= height_usize) break;

        for (0..8) |local_x| {
            const x = block_x * 8 + local_x;
            if (x >= width_usize) break;

            const gray = block[local_y * 8 + local_x];
            const pixel_index = (y * width_usize + x) * 4;
            rgba[pixel_index + 0] = gray;
            rgba[pixel_index + 1] = gray;
            rgba[pixel_index + 2] = gray;
            rgba[pixel_index + 3] = 0xff;
        }
    }
}

fn writeColorMcuRgba(
    rgba: []u8,
    width: u32,
    height: u32,
    mcu_x: usize,
    mcu_y: usize,
    color_encoding: ColorEncoding,
    components: [max_components]ComponentInfo,
    max_h: u8,
    max_v: u8,
    component_blocks: [max_components][max_component_blocks][64]u8,
) void {
    const width_usize: usize = @intCast(width);
    const height_usize: usize = @intCast(height);
    const mcu_width = @as(usize, max_h) * 8;
    const mcu_height = @as(usize, max_v) * 8;

    for (0..mcu_height) |local_y| {
        const y = mcu_y * mcu_height + local_y;
        if (y >= height_usize) break;

        for (0..mcu_width) |local_x| {
            const x = mcu_x * mcu_width + local_x;
            if (x >= width_usize) break;

            const rgb = switch (color_encoding) {
                .rgb => blk: {
                    const r = sampleComponentValue(component_blocks[0], components[0], max_h, max_v, local_x, local_y);
                    const g = sampleComponentValue(component_blocks[1], components[1], max_h, max_v, local_x, local_y);
                    const b = sampleComponentValue(component_blocks[2], components[2], max_h, max_v, local_x, local_y);
                    break :blk [3]u8{ r, g, b };
                },
                .ycbcr => blk: {
                    const yy = sampleComponentValue(component_blocks[0], components[0], max_h, max_v, local_x, local_y);
                    const cb = sampleComponentValue(component_blocks[1], components[1], max_h, max_v, local_x, local_y);
                    const cr = sampleComponentValue(component_blocks[2], components[2], max_h, max_v, local_x, local_y);
                    break :blk ycbcrToRgb(yy, cb, cr);
                },
                .cmyk => blk: {
                    const c = sampleComponentValue(component_blocks[0], components[0], max_h, max_v, local_x, local_y);
                    const m = sampleComponentValue(component_blocks[1], components[1], max_h, max_v, local_x, local_y);
                    const yy = sampleComponentValue(component_blocks[2], components[2], max_h, max_v, local_x, local_y);
                    const k = sampleComponentValue(component_blocks[3], components[3], max_h, max_v, local_x, local_y);
                    break :blk invertedCmykToRgb(c, m, yy, k);
                },
                .ycck => blk: {
                    const yy = sampleComponentValue(component_blocks[0], components[0], max_h, max_v, local_x, local_y);
                    const cb = sampleComponentValue(component_blocks[1], components[1], max_h, max_v, local_x, local_y);
                    const cr = sampleComponentValue(component_blocks[2], components[2], max_h, max_v, local_x, local_y);
                    const k = sampleComponentValue(component_blocks[3], components[3], max_h, max_v, local_x, local_y);
                    break :blk ycckToRgb(yy, cb, cr, k);
                },
            };

            const pixel_index = (y * width_usize + x) * 4;
            rgba[pixel_index + 0] = rgb[0];
            rgba[pixel_index + 1] = rgb[1];
            rgba[pixel_index + 2] = rgb[2];
            rgba[pixel_index + 3] = 0xff;
        }
    }
}

fn sampleComponentValue(
    blocks: [max_component_blocks][64]u8,
    component: ComponentInfo,
    max_h: u8,
    max_v: u8,
    local_x: usize,
    local_y: usize,
) u8 {
    const source_width = @as(usize, component.horizontal_sampling) * 8;
    const source_height = @as(usize, component.vertical_sampling) * 8;

    if (component.horizontal_sampling == max_h and component.vertical_sampling == max_v) {
        return sampleComponentSource(blocks, component, local_x, local_y);
    }

    if (component.horizontal_sampling * 2 == max_h and component.vertical_sampling == max_v and source_width > 2) {
        return fancyUpsampleH2V1(blocks, component, local_x, local_y);
    }

    if (component.horizontal_sampling == max_h and component.vertical_sampling * 2 == max_v and source_height > 1) {
        return fancyUpsampleH1V2(blocks, component, local_x, local_y);
    }

    if (component.horizontal_sampling * 2 == max_h and component.vertical_sampling * 2 == max_v and source_width > 2 and source_height > 1) {
        return fancyUpsampleH2V2(blocks, component, local_x, local_y);
    }

    const sample_x = @divFloor(local_x * @as(usize, component.horizontal_sampling), @as(usize, max_h));
    const sample_y = @divFloor(local_y * @as(usize, component.vertical_sampling), @as(usize, max_v));
    return sampleComponentSource(blocks, component, sample_x, sample_y);
}

fn fancyUpsampleH2V1(
    blocks: [max_component_blocks][64]u8,
    component: ComponentInfo,
    local_x: usize,
    local_y: usize,
) u8 {
    const source_width = @as(usize, component.horizontal_sampling) * 8;
    const source_x = local_x / 2;
    const parity = local_x & 1;
    const center = sampleComponentSource(blocks, component, source_x, local_y);

    if (parity == 0) {
        if (source_x == 0) return center;
        const prev = sampleComponentSource(blocks, component, source_x - 1, local_y);
        return @intCast((@as(u16, center) * 3 + @as(u16, prev) + 1) >> 2);
    }

    if (source_x + 1 >= source_width) return center;
    const next = sampleComponentSource(blocks, component, source_x + 1, local_y);
    return @intCast((@as(u16, center) * 3 + @as(u16, next) + 2) >> 2);
}

fn fancyUpsampleH1V2(
    blocks: [max_component_blocks][64]u8,
    component: ComponentInfo,
    local_x: usize,
    local_y: usize,
) u8 {
    const source_height = @as(usize, component.vertical_sampling) * 8;
    const source_y = local_y / 2;
    const parity = local_y & 1;
    const near_y = source_y;
    const far_y = if (parity == 0)
        if (source_y == 0) 0 else source_y - 1
    else if (source_y + 1 >= source_height) source_height - 1 else source_y + 1;

    const near_value = sampleComponentSource(blocks, component, local_x, near_y);
    const far_value = sampleComponentSource(blocks, component, local_x, far_y);
    return @intCast((@as(u16, near_value) * 3 + @as(u16, far_value) + 1) >> 2);
}

fn fancyUpsampleH2V2(
    blocks: [max_component_blocks][64]u8,
    component: ComponentInfo,
    local_x: usize,
    local_y: usize,
) u8 {
    const source_width = @as(usize, component.horizontal_sampling) * 8;
    const source_height = @as(usize, component.vertical_sampling) * 8;
    const source_x = local_x / 2;
    const source_y = local_y / 2;
    const x_parity = local_x & 1;
    const y_parity = local_y & 1;

    const far_y = if (y_parity == 0)
        if (source_y == 0) 0 else source_y - 1
    else if (source_y + 1 >= source_height) source_height - 1 else source_y + 1;

    const this_col_sum = fancyUpsampleVerticalColumnSum(blocks, component, source_x, source_y, far_y);

    if (x_parity == 0) {
        if (source_x == 0) return @intCast((this_col_sum * 4 + 8) >> 4);
        const prev_col_sum = fancyUpsampleVerticalColumnSum(blocks, component, source_x - 1, source_y, far_y);
        return @intCast((this_col_sum * 3 + prev_col_sum + 8) >> 4);
    }

    if (source_x + 1 >= source_width) return @intCast((this_col_sum * 4 + 7) >> 4);
    const next_col_sum = fancyUpsampleVerticalColumnSum(blocks, component, source_x + 1, source_y, far_y);
    return @intCast((this_col_sum * 3 + next_col_sum + 7) >> 4);
}

fn fancyUpsampleVerticalColumnSum(
    blocks: [max_component_blocks][64]u8,
    component: ComponentInfo,
    sample_x: usize,
    near_y: usize,
    far_y: usize,
) u16 {
    const near_value = sampleComponentSource(blocks, component, sample_x, near_y);
    const far_value = sampleComponentSource(blocks, component, sample_x, far_y);
    return @as(u16, near_value) * 3 + @as(u16, far_value);
}

fn sampleComponentSource(
    blocks: [max_component_blocks][64]u8,
    component: ComponentInfo,
    sample_x: usize,
    sample_y: usize,
) u8 {
    const block_col = sample_x / 8;
    const block_row = sample_y / 8;
    const block_index = block_row * @as(usize, component.horizontal_sampling) + block_col;
    return blocks[block_index][(sample_y % 8) * 8 + (sample_x % 8)];
}

fn ycbcrToRgb(y: u8, cb: u8, cr: u8) [3]u8 {
    const scale_bits = 16;
    const one_half = 1 << (scale_bits - 1);
    const fix_140200 = 91881;
    const fix_034414 = 22554;
    const fix_071414 = 46802;
    const fix_177200 = 116130;

    const yy: i32 = y;
    const cb_shifted: i32 = @as(i32, cb) - 128;
    const cr_shifted: i32 = @as(i32, cr) - 128;

    const r = yy + @divFloor(fix_140200 * cr_shifted + one_half, 1 << scale_bits);
    const g = yy + @divFloor(-fix_034414 * cb_shifted - fix_071414 * cr_shifted + one_half, 1 << scale_bits);
    const b = yy + @divFloor(fix_177200 * cb_shifted + one_half, 1 << scale_bits);

    return .{
        @intCast(std.math.clamp(r, 0, 255)),
        @intCast(std.math.clamp(g, 0, 255)),
        @intCast(std.math.clamp(b, 0, 255)),
    };
}

fn ycbcrToRgbNative(y: u16, cb: u16, cr: u16, bits_per_sample: u8) [3]u16 {
    const scale_bits = 16;
    const one_half = 1 << (scale_bits - 1);
    const fix_140200 = 91881;
    const fix_034414 = 22554;
    const fix_071414 = 46802;
    const fix_177200 = 116130;
    const sample_max: i64 = (@as(i64, 1) << @as(u6, @intCast(bits_per_sample))) - 1;
    const center: i64 = (@as(i64, 1) << @as(u6, @intCast(bits_per_sample - 1)));

    const yy: i64 = y;
    const cb_shifted: i64 = @as(i64, cb) - center;
    const cr_shifted: i64 = @as(i64, cr) - center;

    const r = yy + @divFloor(@as(i64, fix_140200) * cr_shifted + one_half, 1 << scale_bits);
    const g = yy + @divFloor(-@as(i64, fix_034414) * cb_shifted - @as(i64, fix_071414) * cr_shifted + one_half, 1 << scale_bits);
    const b = yy + @divFloor(@as(i64, fix_177200) * cb_shifted + one_half, 1 << scale_bits);

    return .{
        @intCast(std.math.clamp(r, 0, sample_max)),
        @intCast(std.math.clamp(g, 0, sample_max)),
        @intCast(std.math.clamp(b, 0, sample_max)),
    };
}

fn ycbcrToRgbWide(y: u16, cb: u16, cr: u16, bits_per_sample: u8) [3]u8 {
    const rgb = ycbcrToRgbNative(y, cb, cr, bits_per_sample);
    return .{
        scaleSampleToByteWide(rgb[0], bits_per_sample),
        scaleSampleToByteWide(rgb[1], bits_per_sample),
        scaleSampleToByteWide(rgb[2], bits_per_sample),
    };
}

fn invertedCmykToRgb(c: u8, m: u8, y: u8, k: u8) [3]u8 {
    return .{
        multiplyAndDivideBy255(c, k),
        multiplyAndDivideBy255(m, k),
        multiplyAndDivideBy255(y, k),
    };
}

fn multiplyAndDivideBySampleMax(lhs: u16, rhs: u16, bits_per_sample: u8) u16 {
    const sample_max: u32 = (@as(u32, 1) << @as(u5, @intCast(bits_per_sample))) - 1;
    const product = @as(u32, lhs) * @as(u32, rhs);
    return @intCast((product + sample_max / 2) / sample_max);
}

fn invertedCmykToRgbWide(c: u16, m: u16, y: u16, k: u16, bits_per_sample: u8) [3]u8 {
    return .{
        scaleSampleToByteWide(multiplyAndDivideBySampleMax(c, k, bits_per_sample), bits_per_sample),
        scaleSampleToByteWide(multiplyAndDivideBySampleMax(m, k, bits_per_sample), bits_per_sample),
        scaleSampleToByteWide(multiplyAndDivideBySampleMax(y, k, bits_per_sample), bits_per_sample),
    };
}

fn ycckToRgb(y: u8, cb: u8, cr: u8, k: u8) [3]u8 {
    const cmy = ycbcrToRgb(y, cb, cr);
    return invertedCmykToRgb(
        0xff - cmy[0],
        0xff - cmy[1],
        0xff - cmy[2],
        k,
    );
}

fn ycckToRgbWide(y: u16, cb: u16, cr: u16, k: u16, bits_per_sample: u8) [3]u8 {
    const sample_max: u16 = @intCast((@as(u32, 1) << @as(u5, @intCast(bits_per_sample))) - 1);
    const cmy = ycbcrToRgbNative(y, cb, cr, bits_per_sample);
    return invertedCmykToRgbWide(
        sample_max - cmy[0],
        sample_max - cmy[1],
        sample_max - cmy[2],
        k,
        bits_per_sample,
    );
}

fn multiplyAndDivideBy255(lhs: u8, rhs: u8) u8 {
    const product = @as(u32, lhs) * @as(u32, rhs);
    return @intCast((product + 127) / 255);
}

fn refineProgressiveAcCoefficient(
    reader: *EntropyBitReader,
    coeff: *i16,
    refinement_bit: i16,
) !void {
    if (coeff.* == 0) return;
    if (try reader.readBit() == 0) return;

    if ((coeff.* & refinement_bit) != 0) return;
    coeff.* += if (coeff.* < 0) -refinement_bit else refinement_bit;
}

fn frameComponentIndex(info: Info, component_selector: u8) ?usize {
    for (0..info.component_count) |i| {
        if (info.components[i].id == component_selector) return i;
    }
    return null;
}

fn componentBlockIndexForMcu(
    component_blocks_x: usize,
    component_blocks_y: usize,
    component: ComponentInfo,
    mcu_x: usize,
    mcu_y: usize,
    block_col: usize,
    block_row: usize,
) ?usize {
    const global_block_x = mcu_x * @as(usize, component.horizontal_sampling) + block_col;
    const global_block_y = mcu_y * @as(usize, component.vertical_sampling) + block_row;
    if (global_block_x >= component_blocks_x or global_block_y >= component_blocks_y) return null;
    return global_block_y * component_blocks_x + global_block_x;
}

fn losslessPredictor(
    predictor_selection: u8,
    left: ?i32,
    above: ?i32,
    upper_left: ?i32,
    initial_predictor: i32,
) i32 {
    if (left == null and above == null) return initial_predictor;
    if (above == null) return left.?;
    if (left == null) return above.?;

    const ra = left.?;
    const rb = above.?;
    const rc = upper_left orelse above.?;
    return switch (predictor_selection) {
        1 => ra,
        2 => rb,
        3 => rc,
        4 => ra + rb - rc,
        5 => ra + @divFloor(rb - rc, 2),
        6 => rb + @divFloor(ra - rc, 2),
        7 => @divFloor(ra + rb, 2),
        else => initial_predictor,
    };
}

fn isStandaloneMarker(marker: u8) bool {
    return marker == 0x01 or (marker >= 0xd0 and marker <= 0xd7);
}

fn frameKindForMarker(marker: u8) ?FrameKind {
    return switch (marker) {
        0xc0 => .baseline_dct,
        0xc1 => .extended_sequential_dct,
        0xc2 => .progressive_dct,
        0xc3 => .lossless,
        0xc5 => .differential_sequential_dct,
        0xc6 => .differential_progressive_dct,
        0xc7 => .differential_lossless,
        0xc9 => .arithmetic_sequential_dct,
        0xca => .arithmetic_progressive_dct,
        0xcb => .arithmetic_lossless,
        else => null,
    };
}

fn parseFrameSegment(frame_kind: FrameKind, segment: []const u8) !Info {
    if (segment.len < 6) return error.JpegDecodeFailed;

    const bits_per_sample = segment[0];
    const height = std.mem.readInt(u16, segment[1..3], .big);
    const width = std.mem.readInt(u16, segment[3..5], .big);
    const component_count = segment[5];

    if (bits_per_sample == 0) return error.JpegDecodeFailed;
    if (width == 0 or height == 0) return error.JpegDecodeFailed;
    if (component_count == 0 or component_count > 4) return error.UnsupportedJpegFormat;
    if (segment.len != 6 + (@as(usize, component_count) * 3)) return error.JpegDecodeFailed;

    var components = std.mem.zeroes([4]ComponentInfo);
    var component_offset: usize = 6;
    for (0..component_count) |i| {
        const component_id = segment[component_offset];
        const sampling = segment[component_offset + 1];
        components[i] = .{
            .id = component_id,
            .horizontal_sampling = sampling >> 4,
            .vertical_sampling = sampling & 0x0f,
            .quant_table_id = segment[component_offset + 2],
        };
        if (components[i].horizontal_sampling == 0 or components[i].vertical_sampling == 0) {
            return error.JpegDecodeFailed;
        }
        component_offset += 3;
    }

    return .{
        .width = width,
        .height = height,
        .bits_per_sample = bits_per_sample,
        .component_count = component_count,
        .frame_kind = frame_kind,
        .components = components,
    };
}

fn parseQuantizationSegment(quant_tables: *[max_quant_tables]QuantTableInfo, segment: []const u8) !void {
    var cursor: usize = 0;
    while (cursor < segment.len) {
        const table_info = segment[cursor];
        cursor += 1;

        const precision_nibble = table_info >> 4;
        const table_id = table_info & 0x0f;
        if (table_id >= max_quant_tables) return error.UnsupportedJpegFormat;

        const element_bytes: usize = switch (precision_nibble) {
            0 => 1,
            1 => 2,
            else => return error.UnsupportedJpegFormat,
        };
        const table_bytes = 64 * element_bytes;
        if (cursor + table_bytes > segment.len) return error.JpegDecodeFailed;

        var table = QuantTableInfo{
            .present = true,
            .precision_bits = if (element_bytes == 1) 8 else 16,
        };
        for (0..64) |i| {
            table.zig_zag_values[i] = if (element_bytes == 1)
                segment[cursor + i]
            else
                std.mem.readInt(u16, segment[cursor + i * 2 ..][0..2], .big);
        }
        quant_tables[table_id] = table;
        cursor += table_bytes;
    }
}

fn parseHuffmanSegment(
    dc_tables: *[max_huffman_tables]HuffmanTableInfo,
    ac_tables: *[max_huffman_tables]HuffmanTableInfo,
    segment: []const u8,
) !void {
    var cursor: usize = 0;
    while (cursor < segment.len) {
        const table_info = segment[cursor];
        cursor += 1;

        const table_class = table_info >> 4;
        const table_id = table_info & 0x0f;
        if (table_class > 1) return error.UnsupportedJpegFormat;
        if (table_id >= max_huffman_tables) return error.UnsupportedJpegFormat;
        if (cursor + 16 > segment.len) return error.JpegDecodeFailed;

        const counts = segment[cursor .. cursor + 16];
        cursor += 16;

        var symbol_count: u16 = 0;
        for (counts) |count| symbol_count += count;
        if (cursor + symbol_count > segment.len) return error.JpegDecodeFailed;

        const table_ptr = if (table_class == 0) &dc_tables[table_id] else &ac_tables[table_id];
        var table = HuffmanTableInfo{
            .present = true,
            .symbol_count = symbol_count,
        };
        @memcpy(table.code_counts[0..16], counts);
        @memcpy(table.symbols[0..symbol_count], segment[cursor .. cursor + symbol_count]);
        table_ptr.* = table;
        cursor += symbol_count;
    }
}

fn parseArithmeticConditioningSegment(
    dc_l: *[max_arithmetic_tables]u8,
    dc_u: *[max_arithmetic_tables]u8,
    ac_k: *[max_arithmetic_tables]u8,
    segment: []const u8,
) !void {
    if (segment.len == 0 or segment.len % 2 != 0) return error.JpegDecodeFailed;

    var cursor: usize = 0;
    while (cursor < segment.len) {
        const table_info = segment[cursor];
        const conditioning = segment[cursor + 1];
        cursor += 2;

        const table_class = table_info >> 4;
        const table_id = table_info & 0x0f;
        if (table_id >= max_arithmetic_tables) return error.UnsupportedJpegFormat;

        switch (table_class) {
            0 => {
                dc_u[table_id] = conditioning >> 4;
                dc_l[table_id] = conditioning & 0x0f;
            },
            1 => ac_k[table_id] = conditioning,
            else => return error.UnsupportedJpegFormat,
        }
    }
}

fn parseRestartIntervalSegment(segment: []const u8) !u16 {
    if (segment.len != 2) return error.JpegDecodeFailed;
    return std.mem.readInt(u16, segment[0..2], .big);
}

fn parseAdobeApp14Transform(segment: []const u8) ?u8 {
    if (segment.len < 12) return null;
    if (!std.mem.eql(u8, segment[0..5], "Adobe")) return null;
    if (segment[5] != 0x00) return null;
    return segment[11];
}

fn parseScanSegment(segment: []const u8) !ScanInfo {
    if (segment.len < 6) return error.JpegDecodeFailed;

    const component_count = segment[0];
    if (component_count == 0 or component_count > max_components) return error.UnsupportedJpegFormat;
    const header_len = 1 + (@as(usize, component_count) * 2) + 3;
    if (segment.len != header_len) return error.JpegDecodeFailed;

    var components = std.mem.zeroes([max_components]ScanComponentInfo);
    var cursor: usize = 1;
    for (0..component_count) |i| {
        const table_selector = segment[cursor + 1];
        components[i] = .{
            .component_selector = segment[cursor],
            .dc_table_id = table_selector >> 4,
            .ac_table_id = table_selector & 0x0f,
        };
        if (components[i].dc_table_id >= max_huffman_tables or components[i].ac_table_id >= max_huffman_tables) {
            return error.UnsupportedJpegFormat;
        }
        cursor += 2;
    }

    return .{
        .component_count = component_count,
        .components = components,
        .spectral_start = segment[cursor],
        .spectral_end = segment[cursor + 1],
        .successive_approx_high = segment[cursor + 2] >> 4,
        .successive_approx_low = segment[cursor + 2] & 0x0f,
    };
}

fn skipEntropyData(bytes: []const u8, start: usize) !usize {
    var cursor = start;
    while (cursor < bytes.len) {
        if (bytes[cursor] != 0xff) {
            cursor += 1;
            continue;
        }
        if (cursor + 1 >= bytes.len) return error.JpegDecodeFailed;

        var next = cursor + 1;
        while (next < bytes.len and bytes[next] == 0xff) {
            next += 1;
        }
        if (next >= bytes.len) return error.JpegDecodeFailed;

        const marker = bytes[next];
        if (marker == 0x00) {
            cursor = next + 1;
            continue;
        }
        if (marker >= 0xd0 and marker <= 0xd7) {
            cursor = next + 1;
            continue;
        }
        return cursor;
    }
    return error.JpegDecodeFailed;
}

fn fixtureHasTag(fixture: test_support.Manifest.Fixture, tag: []const u8) bool {
    for (fixture.tags) |fixture_tag| {
        if (std.mem.eql(u8, fixture_tag, tag)) return true;
    }
    return false;
}

const zig_zag_to_natural = [64]u8{
    0,  1,  8,  16, 9,  2,  3,  10,
    17, 24, 32, 25, 18, 11, 4,  5,
    12, 19, 26, 33, 40, 48, 41, 34,
    27, 20, 13, 6,  7,  14, 21, 28,
    35, 42, 49, 56, 57, 50, 43, 36,
    29, 22, 15, 23, 30, 37, 44, 51,
    58, 59, 52, 45, 38, 31, 39, 46,
    53, 60, 61, 54, 47, 55, 62, 63,
};

const jpeg_aritab = [114]u32{
    0x5a1d0181, 0x2586020e, 0x11140310, 0x080b0412, 0x03d80514, 0x01da0617, 0x00e50719, 0x006f081c,
    0x0036091e, 0x001a0a21, 0x000d0b23, 0x00060c09, 0x00030d0a, 0x00010d0c, 0x5a7f0f8f, 0x3f251024,
    0x2cf21126, 0x207c1227, 0x17b91328, 0x1182142a, 0x0cef152b, 0x09a1162d, 0x072f172e, 0x055c1830,
    0x04061931, 0x03031a33, 0x02401b34, 0x01b11c36, 0x01441d38, 0x00f51e39, 0x00b71f3b, 0x008a203c,
    0x0068213e, 0x004e223f, 0x003b2320, 0x002c0921, 0x5ae125a5, 0x484c2640, 0x3a0d2741, 0x2ef12843,
    0x261f2944, 0x1f332a45, 0x19a82b46, 0x15182c48, 0x11772d49, 0x0e742e4a, 0x0bfb2f4b, 0x09f8304d,
    0x0861314e, 0x0706324f, 0x05cd3330, 0x04de3432, 0x040f3532, 0x03633633, 0x02d43734, 0x025c3835,
    0x01f83936, 0x01a43a37, 0x01603b38, 0x01253c39, 0x00f63d3a, 0x00cb3e3b, 0x00ab3f3d, 0x008f203d,
    0x5b1241c1, 0x4d044250, 0x412c4351, 0x37d84452, 0x2fe84553, 0x293c4654, 0x23794756, 0x1edf4857,
    0x1aa94957, 0x174e4a48, 0x14244b48, 0x119c4c4a, 0x0f6b4d4a, 0x0d514e4b, 0x0bb64f4d, 0x0a40304d,
    0x583251d0, 0x4d1c5258, 0x438e5359, 0x3bdd545a, 0x34ee555b, 0x2eae565c, 0x299a575d, 0x25164756,
    0x557059d8, 0x4ca95a5f, 0x44d95b60, 0x3e225c61, 0x38245d63, 0x32b45e63, 0x2e17565d, 0x56a860df,
    0x4f466165, 0x47e56266, 0x41cf6367, 0x3c3d6468, 0x375e5d63, 0x52316669, 0x4c0f676a, 0x4639686b,
    0x415e6367, 0x56276ae9, 0x50e76b6c, 0x4b85676d, 0x55976d6e, 0x504f6b6f, 0x5a106fee, 0x55226d70,
    0x59eb6ff0, 0x5a1d7171,
};

const synthetic_progressive_header = [_]u8{
    0xff, 0xd8,
    0xff, 0xe0,
    0x00, 0x10,
    'J',  'F',
    'I',  'F',
    0x00, 0x01,
    0x02, 0x00,
    0x00, 0x01,
    0x00, 0x01,
    0x00, 0x00,
    0xff, 0xdb,
    0x00, 0x43,
    0x00, 0x08,
    0x06, 0x06,
    0x07, 0x06,
    0x05, 0x08,
    0x07, 0x07,
    0x07, 0x09,
    0x09, 0x08,
    0x0a, 0x0c,
    0x14, 0x0d,
    0x0c, 0x0b,
    0x0b, 0x0c,
    0x19, 0x12,
    0x13, 0x0f,
    0x14, 0x1d,
    0x1a, 0x1f,
    0x1e, 0x1d,
    0x1a, 0x1c,
    0x1c, 0x20,
    0x24, 0x2e,
    0x27, 0x20,
    0x22, 0x2c,
    0x23, 0x1c,
    0x1c, 0x28,
    0x37, 0x29,
    0x2c, 0x30,
    0x31, 0x34,
    0x34, 0x34,
    0x1f, 0x27,
    0x39, 0x3d,
    0x38, 0x32,
    0x3c, 0x2e,
    0x33, 0x34,
    0x32, 0xff,
    0xc2, 0x00,
    0x11, 0x08,
    0x00, 0x02,
    0x00, 0x03,
    0x03, 0x01,
    0x22, 0x00,
    0x02, 0x11,
    0x01, 0x03,
    0x11, 0x01,
    0xff, 0xd9,
};

const embedded_progressive_444_b64 =
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/2wBDAQMEBAUEBQkFBQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBT/wgARCAAEAAQDAREAAhEBAxEB/8QAFAABAAAAAAAAAAAAAAAAAAAABf/EABQBAQAAAAAAAAAAAAAAAAAAAAX/2gAMAwEAAhADEAAAAQAXP//EABYQAQEBAAAAAAAAAAAAAAAAAAQFA//aAAgBAQABBQKDT3WP/8QAHhEBAAEDBQEAAAAAAAAAAAAAAQIDBBETITFRcfH/2gAIAQMBAT8Bs7KndalapzKWX1B+9u7mSr//xAAdEQABAwUBAAAAAAAAAAAAAAABAgMhAAQRMUFh/9oACAECAQE/Ab155got2l4ShOBr09G5k9MmSTX/xAAaEAADAAMBAAAAAAAAAAAAAAABAgMABDFB/9oACAEBAAY/Anq1KSJfmvakV4PEYDP/xAAYEAEBAAMAAAAAAAAAAAAAAAABETFBgf/aAAgBAQABPyF8N73olqywDAB//9oADAMBAAIAAwAAABAf/8QAFxEBAQEBAAAAAAAAAAAAAAAAARExAP/aAAgBAwEBPxCsnIICmZJVats/Df/EABcRAQEBAQAAAAAAAAAAAAAAAAERITH/2gAIAQIBAT8Qc0cQLKiqKlPRGjH/xAAWEAEBAQAAAAAAAAAAAAAAAAABEQD/2gAIAQEAAT8QpCpoQBVAIEgGm5//2Q==";

const embedded_progressive_422_b64 =
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/2wBDAQMEBAUEBQkFBQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBT/wgARCAAEAAQDASEAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAX/xAAUAQEAAAAAAAAAAAAAAAAAAAAG/9oADAMBAAIQAxAAAAGAMJP/xAAWEAEBAQAAAAAAAAAAAAAAAAAEBQP/2gAIAQEAAQUCg091j//EABoRAAICAwAAAAAAAAAAAAAAAAEDABECBAX/2gAIAQMBAT8B1+clqwzK7M//xAAcEQABBAMBAAAAAAAAAAAAAAACAQQhQQADEVH/2gAIAQIBAT8BcOXAbVAT4iRVR5n/xAAaEAADAAMBAAAAAAAAAAAAAAABAgMABDFB/9oACAEBAAY/Anq1KSJfmvakV4PEYDP/xAAYEAEBAAMAAAAAAAAAAAAAAAABETFBgf/aAAgBAQABPyF8N73olqywDAB//9oADAMBAAIAAwAAABAf/8QAGREBAQEAAwAAAAAAAAAAAAAAAREhADFR/9oACAEDAQE/EEQoViSqrCYeBh0AAc//xAAYEQEAAwEAAAAAAAAAAAAAAAABABEh8f/aAAgBAgEBPxBhdMFZAA1OAcn/xAAWEAEBAQAAAAAAAAAAAAAAAAABEQD/2gAIAQEAAT8QpCpoQBVAIEgGm5//2Q==";

const embedded_arithmetic_cmyk_b64 =
    "/9j/7gAOQWRvYmUAZAAAAAAA/9sAQwABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEB/8kAFAgAAQADBEMRAE0RAFkRAEsRAP/MAAYAEBAF/9oADgRDAE0AWQBLAAA/AP8A+c9TW++G6jittqndr99uSJGzUdn2FhSvPzc4IVWmk9BKozoFhB5+hv5jsfIQVJtA/9k=";

const embedded_arithmetic_ycck_b64 =
    "/9j/7gAOQWRvYmUAZAAAAAAC/9sAQwABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEB/9sAQwEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEB/8kAFAgAAQADBAEiAAIRAQMRAQQiAP/MAAoAEBAFARARBf/aAA4EAQACEQMRBAAAPwDS6Brvrc6MyRH096P7bsEsD/qSo5lwFTUfsTjDItG0zAmN066n7fP7H+Dqx6kCLqAmcJiEwzGA/9k=";

test "decode rgba loads file-backed baseline jpeg" {
    const alloc = std.testing.allocator;
    const jpeg_bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "testdata/image/jpeg/baseline/white-2x1.jpg", alloc, .limited(64 * 1024));
    defer alloc.free(jpeg_bytes);

    const decoded = try decodeRgba(alloc, jpeg_bytes);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expect(decoded.rgba[0] > 200);
    try std.testing.expect(decoded.rgba[1] > 200);
    try std.testing.expect(decoded.rgba[2] > 200);
    try std.testing.expectEqual(@as(u8, 0xff), decoded.rgba[3]);
}

test "decode rgba matches manifest-backed white fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, "jpeg/baseline/white-2x1.jpg");
    defer alloc.free(fixture_bytes);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/white-2x1.jpg") orelse return error.MissingImageFixture;

    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);
    try std.testing.expectEqual(@as(?u32, 2), fixture.width);
    try std.testing.expectEqual(@as(?u32, 1), fixture.height);
    try std.testing.expectEqual(@as(?u32, 1), fixture.frames);
    try std.testing.expectEqual(@as(usize, 1), fixture.pixel_hashes.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.frame_delays_ms.len);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "probe matches manifest-backed baseline jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/white-2x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(fixture.width.?, info.width);
    try std.testing.expectEqual(fixture.height.?, info.height);
    try std.testing.expectEqual(@as(u8, 8), info.bits_per_sample);
    try std.testing.expectEqual(@as(u8, 3), info.component_count);
    try std.testing.expectEqual(FrameKind.baseline_dct, info.frame_kind);
    try std.testing.expect(fixtureHasTag(fixture, "baseline"));
}

test "parse structure matches baseline jpeg tables and scan" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/white-2x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(fixture.width.?, structure.info.width);
    try std.testing.expectEqual(fixture.height.?, structure.info.height);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 1), structure.scan_count);
    try std.testing.expect(structure.quant_tables[0].present);
    try std.testing.expect(structure.huffman_dc_tables[0].present);
    try std.testing.expect(structure.huffman_ac_tables[0].present);
    try std.testing.expect(structure.quant_tables[0].zig_zag_values[0] != 0);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 3), structure.scans[0].component_count);
    try std.testing.expectEqual(@as(u8, 0), structure.scans[0].spectral_start);
    try std.testing.expectEqual(@as(u8, 63), structure.scans[0].spectral_end);
    try std.testing.expect(structure.scans[0].entropy_end > structure.scans[0].entropy_start);
    try std.testing.expect(supportsPlannedBaselineDecode(structure));
    try std.testing.expect(!canPureZigDecodeGrayscaleBaseline(structure));
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));

    const dc_table = try buildCanonicalHuffmanTable(structure.huffman_dc_tables[0]);
    const ac_table = try buildCanonicalHuffmanTable(structure.huffman_ac_tables[0]);
    try std.testing.expect(dc_table.symbol_count > 0);
    try std.testing.expect(ac_table.symbol_count > 0);
    try std.testing.expectEqual(@as(u16, 0), dc_table.first_code[0]);
    try std.testing.expectEqual(@as(u16, 0), ac_table.first_code[0]);
}

test "decode rgba matches manifest-backed grayscale jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/gray-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, info.frame_kind);
    try std.testing.expectEqual(@as(u8, 1), info.component_count);
    try std.testing.expect(fixtureHasTag(fixture, "grayscale"));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "parse structure matches grayscale jpeg tables and scan" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/gray-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(@as(u8, 1), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 1), structure.scan_count);
    try std.testing.expect(structure.quant_tables[0].present);
    try std.testing.expect(structure.huffman_dc_tables[0].present);
    try std.testing.expect(structure.huffman_ac_tables[0].present);
    try std.testing.expectEqual(@as(u8, 1), structure.scans[0].component_count);
    try std.testing.expectEqual(@as(u8, 1), structure.scans[0].components[0].component_selector);
    try std.testing.expect(structure.scans[0].entropy_end > structure.scans[0].entropy_start);
    try std.testing.expect(supportsPlannedBaselineDecode(structure));
    try std.testing.expect(canPureZigDecodeGrayscaleBaseline(structure));
    try std.testing.expect(!canPureZigDecodeColorBaseline(structure));

    const natural_quant = quantTableNaturalOrder(structure.quant_tables[0]);
    try std.testing.expect(natural_quant[0] != 0);
    try std.testing.expect(natural_quant[63] != 0);
}

test "decode rgba matches manifest-backed 444 baseline color jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/pattern-4x4-444.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "pure zig baseline color decode matches libjpeg coefficients on 444 fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, "jpeg/baseline/pattern-4x4-444.jpg");
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    const scan = structure.scans[0];
    const entropy_bytes = fixture_bytes[scan.entropy_start..];
    var reader = EntropyBitReader.init(entropy_bytes);
    var dc_predictors = [_]i64{0} ** max_components;
    var component_dc_tables: [max_components]CanonicalHuffmanTable = undefined;
    var component_ac_tables: [max_components]CanonicalHuffmanTable = undefined;
    for (0..structure.info.component_count) |frame_index| {
        const scan_component = scan.components[frame_index];
        component_dc_tables[frame_index] = try buildCanonicalHuffmanTable(scan.dc_tables[scan_component.dc_table_id]);
        component_ac_tables[frame_index] = try buildCanonicalHuffmanTable(scan.ac_tables[scan_component.ac_table_id]);
        const block = try decodeBaselineBlock(
            &reader,
            component_dc_tables[frame_index],
            component_ac_tables[frame_index],
            dc_predictors[frame_index],
        );
        dc_predictors[frame_index] = block.dc_predictor;
        const expected = switch (frame_index) {
            0 => [_]i64{ -61, -115, -37, 18, 16, 1, -6, -6, 75, 46, 22, 7, -1, -3, -3, -2 },
            1 => [_]i64{ -57, 26, 1, -8, -4, -1, 2, 2, 14, -16, -18, -7, -3, 0, 2, 2 },
            2 => [_]i64{ 59, -29, -8, 4, 3, 3, 2, 1, -27, 18, 7, 2, 2, 4, 6, 4 },
            else => unreachable,
        };
        try std.testing.expectEqualSlices(i64, &expected, block.coefficients[0..16]);
    }
}

test "pure zig baseline color decode matches djpeg hash on 444 fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, "jpeg/baseline/pattern-4x4-444.jpg");
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    const decoded = try decodeRgbaPureZigColorBaseline(alloc, fixture_bytes, structure);
    defer alloc.free(decoded.rgba);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings("d3906b458b6635e5a09415aab75f0c6ea2ff19957ec112e3099a3df8c4d886ca", actual_hex);
}

test "parse structure matches 444 baseline color jpeg sampling" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/pattern-4x4-444.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));
    try std.testing.expect(!canPureZigDecodeGrayscaleBaseline(structure));
}

test "parse structure matches 422 baseline color jpeg sampling" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/pattern-4x4-422.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));
    try std.testing.expect(!canPureZigDecodeGrayscaleBaseline(structure));
}

test "decode rgba matches manifest-backed 422 baseline color jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/pattern-4x4-422.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "pure zig baseline color decode matches libjpeg coefficients on 422 fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, "jpeg/baseline/pattern-4x4-422.jpg");
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    const scan = structure.scans[0];
    const entropy_bytes = fixture_bytes[scan.entropy_start..];
    var reader = EntropyBitReader.init(entropy_bytes);
    var dc_predictors = [_]i64{0} ** max_components;
    var component_dc_tables: [max_components]CanonicalHuffmanTable = undefined;
    var component_ac_tables: [max_components]CanonicalHuffmanTable = undefined;
    for (0..structure.info.component_count) |frame_index| {
        const component = structure.info.components[frame_index];
        const scan_component = scan.components[frame_index];
        component_dc_tables[frame_index] = try buildCanonicalHuffmanTable(scan.dc_tables[scan_component.dc_table_id]);
        component_ac_tables[frame_index] = try buildCanonicalHuffmanTable(scan.ac_tables[scan_component.ac_table_id]);
        const block_count = @as(usize, component.horizontal_sampling) * @as(usize, component.vertical_sampling);
        for (0..block_count) |block_index| {
            const block = try decodeBaselineBlock(
                &reader,
                component_dc_tables[frame_index],
                component_ac_tables[frame_index],
                dc_predictors[frame_index],
            );
            if (block_index == 0) {
                const expected = switch (frame_index) {
                    0 => [_]i64{ -61, -115, -37, 18, 16, 1, -6, -6, 75, 46, 22, 7, -1, -3, -3, -2 },
                    1 => [_]i64{ -74, 16, 9, 2, 0, -1, -1, -1, 18, -7, -7, -3, -3, -2, -2, -1 },
                    2 => [_]i64{ 76, -17, -12, -5, -1, -1, 0, 0, -35, 10, 6, 1, 0, -1, -1, -1 },
                    else => unreachable,
                };
                try std.testing.expectEqualSlices(i64, &expected, block.coefficients[0..16]);
            }
            dc_predictors[frame_index] = block.dc_predictor;
        }
    }
}

test "pure zig baseline color decode matches djpeg hash on 422 fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, "jpeg/baseline/pattern-4x4-422.jpg");
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    const decoded = try decodeRgbaPureZigColorBaseline(alloc, fixture_bytes, structure);
    defer alloc.free(decoded.rgba);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings("bbd0e972d617a4f2cb3c3ac945ea71cb8a055ee000de10d5dfe351cd4301fba2", actual_hex);
}

test "parse structure matches 411 baseline color jpeg sampling" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/pattern-8x8-411.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));
    try std.testing.expect(!canPureZigDecodeGrayscaleBaseline(structure));
}

test "decode rgba matches manifest-backed 411 baseline color jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/pattern-8x8-411.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed grayscale restart jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/restart/gray-16x16-rst1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "parse structure captures grayscale restart jpeg interval" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/restart/gray-16x16-rst1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(@as(u8, 1), structure.info.component_count);
    try std.testing.expectEqual(@as(?u16, 1), structure.restart_interval);
    try std.testing.expect(canPureZigDecodeGrayscaleBaseline(structure));
    try std.testing.expect(!canPureZigDecodeColorBaseline(structure));
}

test "parse structure captures manifest-backed adobe cmyk jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/cmyk/adobe-cmyk-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expectEqual(@as(?u8, 0), structure.adobe_transform);
    try std.testing.expectEqual(@as(u8, 'C'), structure.info.components[0].id);
    try std.testing.expectEqual(@as(u8, 'M'), structure.info.components[1].id);
    try std.testing.expectEqual(@as(u8, 'Y'), structure.info.components[2].id);
    try std.testing.expectEqual(@as(u8, 'K'), structure.info.components[3].id);
    try std.testing.expect(supportsPlannedBaselineDecode(structure));
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));
}

test "decode rgba matches manifest-backed adobe cmyk jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/cmyk/adobe-cmyk-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    const expected_rgba = [_]u8{
        0xff, 0x00, 0x00, 0xff,
        0x00, 0xff, 0x00, 0xff,
        0x00, 0x00, 0xff, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, decoded.rgba);
}

test "parse structure captures manifest-backed adobe ycck jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/ycck/adobe-ycck-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expectEqual(@as(?u8, 2), structure.adobe_transform);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[3].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[3].vertical_sampling);
    try std.testing.expect(supportsPlannedBaselineDecode(structure));
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));
}

test "decode rgba matches manifest-backed adobe ycck jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/ycck/adobe-ycck-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    const expected_rgba = [_]u8{
        0x5b, 0x5a, 0x00, 0xff,
        0xa5, 0xa4, 0x26, 0xff,
        0x00, 0x00, 0xff, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, decoded.rgba);
}

test "decode rgba matches manifest-backed odd app13 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/white-2x1-app13.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed odd com-before-sof jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/white-2x1-com-before-sof.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed odd app2-before-sos jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/white-2x1-app2-before-sos.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed late dqt before sos jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/white-2x1-late-dqt-before-sos.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed late dht before sos jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/white-2x1-late-dht-before-sos.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed late dac before sos jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-late-dac-before-sos.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed late dri before sos jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/gray-16x16-rst1-late-dri-before-sos.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed late dht before scan2 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-late-dht-before-scan2.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed dac reordered jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-dac-reordered.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed com before scan7 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-com-before-scan7.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed app1 before sos arithmetic jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-app1-before-sos.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed com before scan8 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-com-before-scan8.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed app2 before dac arithmetic jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-app2-before-dac.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed app13 before scan9 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/valid_weird/red-3x2-app13-before-scan9.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba rejects manifest-backed truncated jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/truncated-64b.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed truncated jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/truncated-64b.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad restart marker jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-restart-marker.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed bad restart marker jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-restart-marker.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad sos table selector jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-sos-table-selector.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad sos table selector jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-sos-table-selector.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 2), info.width);
    try std.testing.expectEqual(@as(u32, 1), info.height);
    try std.testing.expectEqual(FrameKind.baseline_dct, info.frame_kind);
}

test "decode rgba rejects manifest-backed bad dqt length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dqt-length.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed bad dqt length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dqt-length.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad dht length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dht-length.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed bad dht length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dht-length.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad dri length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dri-length.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed bad dri length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dri-length.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad sof sampling jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-sof-sampling.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed bad sof sampling jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-sof-sampling.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad sof quant table jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-sof-quant-table.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad sof quant table jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-sof-quant-table.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 2), info.width);
    try std.testing.expectEqual(@as(u32, 1), info.height);
    try std.testing.expectEqual(FrameKind.baseline_dct, info.frame_kind);
}

test "decode rgba rejects manifest-backed bad progressive spectral range jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-spectral-range.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad progressive spectral range jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-spectral-range.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.progressive_dct, info.frame_kind);
}

test "planned progressive decode rejects manifest-backed bad progressive spectral range jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-spectral-range.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(!canPureZigDecodeProgressive(structure));
}

test "decode rgba rejects manifest-backed bad arithmetic spectral range jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-spectral-range.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad arithmetic spectral range jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-spectral-range.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, info.frame_kind);
}

test "planned arithmetic decode rejects manifest-backed bad arithmetic spectral range jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-spectral-range.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedArithmeticDecode(structure));
}

test "decode rgba rejects manifest-backed bad dac table class jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dac-table-class.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.UnsupportedJpegFormat, parseStructure(fixture_bytes));
    try std.testing.expectError(error.UnsupportedJpegFormat, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed bad dac table class jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dac-table-class.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.UnsupportedJpegFormat, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad dac length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dac-length.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe rejects manifest-backed bad dac length jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-dac-length.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, probe(fixture_bytes));
}

test "decode rgba rejects manifest-backed bad progressive successive approx jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-successive-approx.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad progressive successive approx jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-successive-approx.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.progressive_dct, info.frame_kind);
}

test "planned progressive decode rejects manifest-backed bad progressive successive approx jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-successive-approx.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(!canPureZigDecodeProgressive(structure));
}

test "decode rgba rejects manifest-backed bad arithmetic component order jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-component-order.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad arithmetic component order jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-component-order.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, info.frame_kind);
}

test "planned arithmetic decode rejects manifest-backed bad arithmetic component order jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-component-order.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedArithmeticDecode(structure));
}

test "decode rgba rejects manifest-backed bad arithmetic successive approx jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-successive-approx.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad arithmetic successive approx jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-successive-approx.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, info.frame_kind);
}

test "planned arithmetic decode rejects manifest-backed bad arithmetic successive approx jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-arithmetic-successive-approx.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedArithmeticDecode(structure));
}

test "decode rgba rejects manifest-backed bad progressive missing dc component jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-dc-component.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad progressive missing dc component jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-dc-component.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.progressive_dct, info.frame_kind);
}

test "planned progressive decode rejects manifest-backed bad progressive missing dc component jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-dc-component.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(!canPureZigDecodeProgressive(structure));
}

test "decode rgba rejects manifest-backed bad progressive missing ac table jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-ac-table.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad progressive missing ac table jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-ac-table.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.progressive_dct, info.frame_kind);
}

test "planned progressive decode rejects manifest-backed bad progressive missing ac table jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-ac-table.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(!canPureZigDecodeProgressive(structure));
}

test "decode rgba rejects manifest-backed bad progressive missing dc table jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-dc-table.jpg") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("jpeg", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.JpegDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "probe accepts manifest-backed bad progressive missing dc table jpeg fixture before decode fails" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-dc-table.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(FrameKind.progressive_dct, info.frame_kind);
}

test "planned progressive decode rejects manifest-backed bad progressive missing dc table jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/invalid/bad-progressive-missing-dc-table.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expect(!supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(!canPureZigDecodeProgressive(structure));
}

test "probe classifies synthetic progressive jpeg frame header" {
    const info = try probe(&synthetic_progressive_header);

    try std.testing.expectEqual(@as(u32, 3), info.width);
    try std.testing.expectEqual(@as(u32, 2), info.height);
    try std.testing.expectEqual(@as(u8, 8), info.bits_per_sample);
    try std.testing.expectEqual(@as(u8, 3), info.component_count);
    try std.testing.expectEqual(FrameKind.progressive_dct, info.frame_kind);
    try std.testing.expectEqual(@as(u8, 2), info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), info.components[2].vertical_sampling);
}

test "probe classifies manifest-backed progressive jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(fixture.width.?, info.width);
    try std.testing.expectEqual(fixture.height.?, info.height);
    try std.testing.expectEqual(FrameKind.progressive_dct, info.frame_kind);
    try std.testing.expect(fixtureHasTag(fixture, "progressive"));
}

test "parse structure captures manifest-backed progressive jpeg scans" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expect(structure.scan_count > 1);
    try std.testing.expect(structure.quant_tables[0].present);
    try std.testing.expect(structure.huffman_dc_tables[0].present);
    try std.testing.expect(structure.huffman_ac_tables[0].present);
    try std.testing.expect(structure.scans[0].entropy_end > structure.scans[0].entropy_start);
    try std.testing.expect(!supportsPlannedBaselineDecode(structure));
    try std.testing.expect(supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeProgressive(structure));

    const dc_table = try buildCanonicalHuffmanTable(structure.huffman_dc_tables[0]);
    try std.testing.expect(dc_table.symbol_count > 0);

    const expected_scans = [_]struct {
        component_count: u8,
        selectors: [max_components]u8,
        dc_tables: [max_components]u8,
        ac_tables: [max_components]u8,
        spectral_start: u8,
        spectral_end: u8,
        successive_high: u8,
        successive_low: u8,
    }{
        .{
            .component_count = 3,
            .selectors = .{ 1, 2, 3, 0 },
            .dc_tables = .{ 0, 1, 1, 0 },
            .ac_tables = .{ 0, 0, 0, 0 },
            .spectral_start = 0,
            .spectral_end = 0,
            .successive_high = 0,
            .successive_low = 1,
        },
        .{
            .component_count = 1,
            .selectors = .{ 1, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 0, 0, 0, 0 },
            .spectral_start = 1,
            .spectral_end = 5,
            .successive_high = 0,
            .successive_low = 2,
        },
        .{
            .component_count = 1,
            .selectors = .{ 3, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 1, 0, 0, 0 },
            .spectral_start = 1,
            .spectral_end = 63,
            .successive_high = 0,
            .successive_low = 1,
        },
        .{
            .component_count = 1,
            .selectors = .{ 2, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 1, 0, 0, 0 },
            .spectral_start = 1,
            .spectral_end = 63,
            .successive_high = 0,
            .successive_low = 1,
        },
        .{
            .component_count = 1,
            .selectors = .{ 1, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 0, 0, 0, 0 },
            .spectral_start = 6,
            .spectral_end = 63,
            .successive_high = 0,
            .successive_low = 2,
        },
        .{
            .component_count = 1,
            .selectors = .{ 1, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 0, 0, 0, 0 },
            .spectral_start = 1,
            .spectral_end = 63,
            .successive_high = 2,
            .successive_low = 1,
        },
        .{
            .component_count = 3,
            .selectors = .{ 1, 2, 3, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 0, 0, 0, 0 },
            .spectral_start = 0,
            .spectral_end = 0,
            .successive_high = 1,
            .successive_low = 0,
        },
        .{
            .component_count = 1,
            .selectors = .{ 3, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 1, 0, 0, 0 },
            .spectral_start = 1,
            .spectral_end = 63,
            .successive_high = 1,
            .successive_low = 0,
        },
        .{
            .component_count = 1,
            .selectors = .{ 2, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 1, 0, 0, 0 },
            .spectral_start = 1,
            .spectral_end = 63,
            .successive_high = 1,
            .successive_low = 0,
        },
        .{
            .component_count = 1,
            .selectors = .{ 1, 0, 0, 0 },
            .dc_tables = .{ 0, 0, 0, 0 },
            .ac_tables = .{ 0, 0, 0, 0 },
            .spectral_start = 1,
            .spectral_end = 63,
            .successive_high = 1,
            .successive_low = 0,
        },
    };
    try std.testing.expectEqual(@as(usize, expected_scans.len), structure.scan_count);
    for (expected_scans, 0..) |expected, scan_index| {
        const scan = structure.scans[scan_index];
        try std.testing.expectEqual(expected.component_count, scan.component_count);
        for (0..scan.component_count) |component_index| {
            try std.testing.expectEqual(expected.selectors[component_index], scan.components[component_index].component_selector);
            try std.testing.expectEqual(expected.dc_tables[component_index], scan.components[component_index].dc_table_id);
            try std.testing.expectEqual(expected.ac_tables[component_index], scan.components[component_index].ac_table_id);
        }
        try std.testing.expectEqual(expected.spectral_start, scan.spectral_start);
        try std.testing.expectEqual(expected.spectral_end, scan.spectral_end);
        try std.testing.expectEqual(expected.successive_high, scan.successive_approx_high);
        try std.testing.expectEqual(expected.successive_low, scan.successive_approx_low);
    }

    var saw_redefined_ac_table = false;
    for (1..structure.scan_count) |scan_index| {
        const scan = structure.scans[scan_index];
        if (scan.spectral_start == 0) continue;
        const table_id = scan.components[0].ac_table_id;
        if (!std.meta.eql(scan.ac_tables[table_id], structure.huffman_ac_tables[table_id])) {
            saw_redefined_ac_table = true;
            break;
        }
    }
    try std.testing.expect(saw_redefined_ac_table);
}

test "apply progressive scans populates manifest-backed progressive jpeg state" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expect(canPureZigDecodeProgressive(structure));

    var progressive = try ProgressiveDecodeContext.init(alloc, structure);
    defer progressive.deinit(alloc);

    var dc_predictors = [_]i16{0} ** max_components;
    for (0..structure.scan_count) |scan_index| {
        try applyProgressiveScan(fixture_bytes, structure, scan_index, &progressive, &dc_predictors);
    }

    for (0..structure.info.component_count) |frame_index| {
        try std.testing.expect(progressive.component_states[frame_index].len > 0);
        try std.testing.expect(progressive.component_states[frame_index][0].coefficients[0] != 0);
    }
}

test "apply progressive scans matches libjpeg coefficient reference for manifest-backed fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expect(canPureZigDecodeProgressive(structure));

    var progressive = try ProgressiveDecodeContext.init(alloc, structure);
    defer progressive.deinit(alloc);

    var dc_predictors = [_]i16{0} ** max_components;
    for (0..structure.scan_count) |scan_index| {
        try applyProgressiveScan(fixture_bytes, structure, scan_index, &progressive, &dc_predictors);
    }

    const expected_y = [_]i16{ -291, 96, 69, 23, 0, -6, -6, -3 };
    const expected_cb = [_]i16{ -14, -15, -11, -6, -2, -2, -1, -1 };
    const expected_cr = [_]i16{ 42, 44, 33, 16, 6, 5, 3, 2 };

    try std.testing.expectEqualSlices(i16, &expected_y, progressive.component_states[0][0].coefficients[0..8]);
    try std.testing.expectEqualSlices(i16, &expected_cb, progressive.component_states[1][0].coefficients[0..8]);
    try std.testing.expectEqualSlices(i16, &expected_cr, progressive.component_states[2][0].coefficients[0..8]);
}

test "pure zig progressive decode matches djpeg reference for manifest-backed fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expect(canPureZigDecodeProgressive(structure));

    const progressive = try decodeRgbaPureZigProgressive(alloc, fixture_bytes, structure);
    defer alloc.free(progressive.rgba);

    try std.testing.expectEqual(fixture.width.?, progressive.width);
    try std.testing.expectEqual(fixture.height.?, progressive.height);
    const expected_rgba = [_]u8{
        0xfa, 0x01, 0x00, 0xff,
        0xfb, 0x02, 0x00, 0xff,
        0x03, 0x00, 0x02, 0xff,
        0xfa, 0x01, 0x00, 0xff,
        0xfb, 0x02, 0x00, 0xff,
        0x03, 0x00, 0x02, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, progressive.rgba);
}

test "parse structure matches manifest-backed 411 progressive jpeg sampling" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/pattern-8x8-411.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].vertical_sampling);
    try std.testing.expect(supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeProgressive(structure));
}

test "decode rgba matches manifest-backed 411 progressive jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/pattern-8x8-411.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "parse structure captures manifest-backed progressive adobe cmyk jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/adobe-cmyk-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expectEqual(@as(?u8, 0), structure.adobe_transform);
    try std.testing.expect(supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeProgressive(structure));
}

test "decode rgba matches manifest-backed progressive adobe cmyk jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/adobe-cmyk-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    const expected_rgba = [_]u8{
        0xff, 0x00, 0x00, 0xff,
        0x00, 0xff, 0x00, 0xff,
        0x00, 0x00, 0xff, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, decoded.rgba);
}

test "parse structure captures manifest-backed progressive adobe ycck jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/adobe-ycck-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expectEqual(@as(?u8, 2), structure.adobe_transform);
    try std.testing.expect(supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeProgressive(structure));
}

test "decode rgba matches manifest-backed progressive adobe ycck jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/progressive/adobe-ycck-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    const expected_rgba = [_]u8{
        0x5b, 0x5a, 0x00, 0xff,
        0xa5, 0xa4, 0x26, 0xff,
        0x00, 0x00, 0xff, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, decoded.rgba);
}

test "pure zig progressive decode matches djpeg hash on embedded 444 fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try decodeEmbeddedBase64Alloc(alloc, embedded_progressive_444_b64);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expect(canPureZigDecodeProgressive(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings("9d2817b51a4431daf5d959f1bc97041981abaefebd5180e5821fb71a730bbd2d", actual_hex);
}

test "embedded 444 progressive fixture coefficient state matches libjpeg reference" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try decodeEmbeddedBase64Alloc(alloc, embedded_progressive_444_b64);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    var progressive = try ProgressiveDecodeContext.init(alloc, structure);
    defer progressive.deinit(alloc);

    var dc_predictors = [_]i16{0} ** max_components;
    for (0..structure.scan_count) |scan_index| {
        try applyProgressiveScan(fixture_bytes, structure, scan_index, &progressive, &dc_predictors);
    }

    const expected_y = [_]i16{ -62, -122, -48, 12, 14, 3, -4, -4, 78, 46, 20, 6, 0, -1, -1, 0 };
    const expected_cb = [_]i16{ -60, 27, 3, -7, -4, -1, 1, 2, 13, -14, -16, -7, -3, 0, 2, 2 };
    const expected_cr = [_]i16{ 56, -24, 0, 8, 4, 0, -3, -3, -27, 20, 13, 4, 2, 2, 2, 1 };

    try std.testing.expectEqualSlices(i16, &expected_y, progressive.component_states[0][0].coefficients[0..16]);
    try std.testing.expectEqualSlices(i16, &expected_cb, progressive.component_states[1][0].coefficients[0..16]);
    try std.testing.expectEqualSlices(i16, &expected_cr, progressive.component_states[2][0].coefficients[0..16]);
}

test "pure zig progressive decode matches djpeg hash on embedded 422 fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try decodeEmbeddedBase64Alloc(alloc, embedded_progressive_422_b64);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expect(canPureZigDecodeProgressive(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings("263ceb48620c413ffc902ebccb50f8e6ed27ad57979023132b61d873a4d6d39d", actual_hex);
}

test "probe classifies manifest-backed arithmetic jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const info = try probe(fixture_bytes);
    try std.testing.expectEqual(fixture.width.?, info.width);
    try std.testing.expectEqual(fixture.height.?, info.height);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, info.frame_kind);
    try std.testing.expect(fixtureHasTag(fixture, "arithmetic"));
}

test "parse structure matches manifest-backed arithmetic 411 jpeg sampling" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/pattern-8x8-411.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expect(supportsPlannedArithmeticDecode(structure));
}

test "decode rgba matches manifest-backed arithmetic 411 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/pattern-8x8-411.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "parse structure captures manifest-backed arithmetic adobe cmyk jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/adobe-cmyk-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(?u8, 0), structure.adobe_transform);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expect(supportsPlannedArithmeticDecode(structure));
}

test "decode rgba matches manifest-backed arithmetic adobe cmyk jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/adobe-cmyk-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "parse structure captures manifest-backed arithmetic adobe ycck jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/adobe-ycck-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(?u8, 2), structure.adobe_transform);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expect(supportsPlannedArithmeticDecode(structure));
}

test "decode rgba matches manifest-backed arithmetic adobe ycck jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/adobe-ycck-3x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "probe classifies manifest-backed upstream libjpeg-turbo jpeg fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const baseline_orig = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo/testorig.jpg") orelse return error.MissingImageFixture;
    const baseline_int = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo/testimgint.jpg") orelse return error.MissingImageFixture;
    const arithmetic = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo/testimgari.jpg") orelse return error.MissingImageFixture;

    const orig_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, baseline_orig.path);
    defer alloc.free(orig_bytes);
    const int_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, baseline_int.path);
    defer alloc.free(int_bytes);
    const ari_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, arithmetic.path);
    defer alloc.free(ari_bytes);

    const orig_info = try probe(orig_bytes);
    const int_info = try probe(int_bytes);
    const ari_info = try probe(ari_bytes);

    try std.testing.expectEqual(FrameKind.baseline_dct, orig_info.frame_kind);
    try std.testing.expectEqual(FrameKind.baseline_dct, int_info.frame_kind);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, ari_info.frame_kind);
    try std.testing.expectEqual(baseline_orig.width.?, orig_info.width);
    try std.testing.expectEqual(baseline_orig.height.?, orig_info.height);
    try std.testing.expectEqual(baseline_int.width.?, int_info.width);
    try std.testing.expectEqual(baseline_int.height.?, int_info.height);
    try std.testing.expectEqual(arithmetic.width.?, ari_info.width);
    try std.testing.expectEqual(arithmetic.height.?, ari_info.height);
    try std.testing.expect(fixtureHasTag(baseline_orig, "upstream_libjpeg_turbo"));
    try std.testing.expect(fixtureHasTag(baseline_int, "upstream_libjpeg_turbo"));
    try std.testing.expect(fixtureHasTag(arithmetic, "upstream_libjpeg_turbo"));
}

test "parse structure captures manifest-backed upstream libjpeg-turbo seed grayscale restart jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_grayscale_q80_rst8.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 1), structure.info.component_count);
    try std.testing.expect(structure.restart_interval != null);
    try std.testing.expect(structure.restart_interval.? > 0);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed kitty2 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/kitty2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed grayscale restart jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_grayscale_q80_rst8.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "parse structure captures manifest-backed upstream libjpeg-turbo seed progressive restart jpeg metadata" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_rot270_420_fastdct_q75_prog_rst100.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expect(structure.restart_interval != null);
    try std.testing.expect(structure.restart_interval.? > 0);
    try std.testing.expect(supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeProgressive(structure));
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed progressive restart jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_rot270_420_fastdct_q75_prog_rst100.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed arithmetic restart jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_transpose_440_q90_ari_rst1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expect(structure.restart_interval != null);
    try std.testing.expect(structure.restart_interval.? > 0);
    try std.testing.expect(supportsPlannedArithmeticDecode(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed arithmetic progressive jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_transverse_422_q95_prog_ari.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 10), structure.scan_count);
    try std.testing.expect(supportsPlannedArithmeticProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeArithmeticProgressive(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "parse structure matches manifest-backed arithmetic progressive 411 jpeg sampling" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/pattern-8x8-411-progressive.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expect(supportsPlannedArithmeticProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeArithmeticProgressive(structure));
}

test "decode rgba matches manifest-backed arithmetic progressive 411 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/pattern-8x8-411-progressive.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed 410 baseline jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_vflip_410_q10_baseline.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed tiny 410 baseline jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/2x2_410.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed tiny 420 and 444 baseline jpeg fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const cases = [_]struct {
        path: []const u8,
        width: u32,
        height: u32,
        y_h: u8,
        y_v: u8,
    }{
        .{ .path = "jpeg/upstream/libjpeg_turbo_seed_corpora/2x2_420.jpg", .width = 2, .height = 2, .y_h = 2, .y_v = 2 },
        .{ .path = "jpeg/upstream/libjpeg_turbo_seed_corpora/8x8_420.jpg", .width = 8, .height = 8, .y_h = 2, .y_v = 2 },
        .{ .path = "jpeg/upstream/libjpeg_turbo_seed_corpora/1x1_420.jpg", .width = 1, .height = 1, .y_h = 2, .y_v = 2 },
        .{ .path = "jpeg/upstream/libjpeg_turbo_seed_corpora/1x1_444.jpg", .width = 1, .height = 1, .y_h = 1, .y_v = 1 },
    };

    for (cases) |case| {
        const fixture = test_support.findFixture(manifest, case.path) orelse return error.MissingImageFixture;
        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const structure = try parseStructure(fixture_bytes);
        try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
        try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
        try std.testing.expectEqual(case.y_h, structure.info.components[0].horizontal_sampling);
        try std.testing.expectEqual(case.y_v, structure.info.components[0].vertical_sampling);
        try std.testing.expect(canPureZigDecodeColorBaseline(structure));

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);
        const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hash);

        try std.testing.expectEqual(case.width, decoded.width);
        try std.testing.expectEqual(case.height, decoded.height);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
    }
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed rgb baseline jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_rgb_444_floatdct_q100_test3icc.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 'R'), structure.info.components[0].id);
    try std.testing.expectEqual(@as(u8, 'G'), structure.info.components[1].id);
    try std.testing.expectEqual(@as(u8, 'B'), structure.info.components[2].id);
    try std.testing.expectEqual(ColorEncoding.rgb, colorEncodingForStructure(structure).?);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed ycck 411 jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_rot180_cmyk_411_q50_opt_rst1B_test1icc.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expectEqual(@as(?u8, 2), structure.adobe_transform);
    try std.testing.expectEqual(ColorEncoding.ycck, colorEncodingForStructure(structure).?);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed 120 baseline jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/test1-8.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].vertical_sampling);
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed 12-bit odd-sampling jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/random12_100x91_islow_4x1,2x2,1x2_Q100,99,98_rst2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.extended_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 12), structure.info.bits_per_sample);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[1].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[1].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[2].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[2].vertical_sampling);
    try std.testing.expect(supportsPlannedBaselineDecode(structure));
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));
    try std.testing.expectEqual(ColorEncoding.ycbcr, colorEncodingForStructure(structure).?);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed 12-bit rgb jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/random12_99x92_ifast_rgb_420_Q90,80,70_smooth50.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.extended_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 12), structure.info.bits_per_sample);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expect(supportsPlannedBaselineDecode(structure));
    try std.testing.expect(canPureZigDecodeColorBaseline(structure));
    try std.testing.expectEqual(ColorEncoding.rgb, colorEncodingForStructure(structure).?);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed lossless jpeg fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture_paths = [_][]const u8{
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random2_92x99_lossless_psv5_pt1.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random6_97x94_lossless_psv2_pt2.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random8_93x98_lossless_psv2_pt0.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random10_97x94_lossless_psv7_pt9.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random10_100x91_lossless_psv6_pt1.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random11_99x92_lossless_psv6_pt2.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random12_91x100_lossless_psv6_pt0.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random5_96x95_lossless_psv3_pt2.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random13_93x98_lossless_psv4_pt4.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random16_96x95_lossless_psv3_pt0.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random16_98x93_lossless_psv2_pt0.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random16_98x93_lossless_psv7_pt0.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random16_99x92_lossless_psv1_pt6.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random14_99x92_lossless_psv6_pt13.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random15_92x99_lossless_psv5_pt3.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/random16_92x99_lossless_psv5_pt0.jpg",
    };

    for (fixture_paths) |fixture_path| {
        const fixture = test_support.findFixture(manifest, fixture_path) orelse return error.MissingImageFixture;
        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const structure = try parseStructure(fixture_bytes);
        try std.testing.expectEqual(FrameKind.lossless, structure.info.frame_kind);
        try std.testing.expect(supportsPlannedLosslessDecode(structure));

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);
        const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hash);

        try std.testing.expectEqual(fixture.width.?, decoded.width);
        try std.testing.expectEqual(fixture.height.?, decoded.height);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
    }
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed 441 progressive jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_rot90_441_q25_scan_script.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.progressive_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 1), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].vertical_sampling);
    try std.testing.expect(supportsPlannedProgressiveDecode(structure));
    try std.testing.expect(canPureZigDecodeProgressive(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed extended sequential jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/upstream/libjpeg_turbo_seed_corpora/testorig_hflip_2x4_q1_rst16.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.extended_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
    try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
    try std.testing.expectEqual(@as(u8, 4), structure.info.components[0].vertical_sampling);
    try std.testing.expectEqual(@as(u8, 16), structure.quant_tables[structure.info.components[0].quant_table_id].precision_bits);
    try std.testing.expect(supportsPlannedBaselineDecode(structure));
    try std.testing.expectEqual(ColorEncoding.ycbcr, colorEncodingForStructure(structure).?);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);
    const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hash);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
}

test "decode rgba matches manifest-backed upstream libjpeg-turbo seed github_347 overflow jpeg fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture_paths = [_][]const u8{
        "jpeg/upstream/libjpeg_turbo_seed_corpora/github_347_overflow1.jpg",
        "jpeg/upstream/libjpeg_turbo_seed_corpora/github_347_overflow2.jpg",
    };

    for (fixture_paths) |fixture_path| {
        const fixture = test_support.findFixture(manifest, fixture_path) orelse return error.MissingImageFixture;
        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const structure = try parseStructure(fixture_bytes);
        try std.testing.expectEqual(FrameKind.baseline_dct, structure.info.frame_kind);
        try std.testing.expectEqual(@as(u8, 3), structure.info.component_count);
        try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].horizontal_sampling);
        try std.testing.expectEqual(@as(u8, 2), structure.info.components[0].vertical_sampling);
        try std.testing.expect(canPureZigDecodeColorBaseline(structure));

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);
        const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hash);

        try std.testing.expectEqual(fixture.width.?, decoded.width);
        try std.testing.expectEqual(fixture.height.?, decoded.height);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hash);
    }
}

test "parse structure captures manifest-backed arithmetic jpeg scans" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(u8, 1), structure.scan_count);
    try std.testing.expect(structure.quant_tables[0].present);
    try std.testing.expect(structure.scans[0].entropy_end > structure.scans[0].entropy_start);
    try std.testing.expect(!supportsPlannedBaselineDecode(structure));
    try std.testing.expect(!supportsPlannedProgressiveDecode(structure));
    try std.testing.expectEqual(@as(u16, 0), structure.huffman_dc_tables[0].symbol_count);
    try std.testing.expectEqual(@as(u16, 0), structure.huffman_ac_tables[0].symbol_count);
}

test "arithmetic sequential block decode matches libjpeg coefficient reference" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expect(supportsPlannedArithmeticDecode(structure));

    const scan = structure.scans[0];
    const entropy_bytes = fixture_bytes[scan.entropy_start..];
    var decoder = ArithmeticDecoder.init(entropy_bytes);
    var dc_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_dc_stat_bins]u8);
    var ac_stats = std.mem.zeroes([max_arithmetic_tables][arithmetic_ac_stat_bins]u8);
    var fixed_bin = [_]u8{0} ** 4;
    fixed_bin[0] = 113;
    var last_dc_values = [_]i32{0} ** max_components;
    var dc_contexts = [_]u8{0} ** max_components;

    var y = std.mem.zeroes([64]i16);
    for (0..4) |block_index| {
        const block = try decodeArithmeticSequentialBlock(
            &decoder,
            &dc_stats,
            &ac_stats,
            &fixed_bin,
            structure.arithmetic_dc_l,
            structure.arithmetic_dc_u,
            structure.arithmetic_ac_k,
            scan.components[0].dc_table_id,
            scan.components[0].ac_table_id,
            &last_dc_values[0],
            &dc_contexts[0],
        );
        if (block_index == 0) y = block;
    }
    const cb = try decodeArithmeticSequentialBlock(
        &decoder,
        &dc_stats,
        &ac_stats,
        &fixed_bin,
        structure.arithmetic_dc_l,
        structure.arithmetic_dc_u,
        structure.arithmetic_ac_k,
        scan.components[1].dc_table_id,
        scan.components[1].ac_table_id,
        &last_dc_values[1],
        &dc_contexts[1],
    );
    const cr = try decodeArithmeticSequentialBlock(
        &decoder,
        &dc_stats,
        &ac_stats,
        &fixed_bin,
        structure.arithmetic_dc_l,
        structure.arithmetic_dc_u,
        structure.arithmetic_ac_k,
        scan.components[2].dc_table_id,
        scan.components[2].ac_table_id,
        &last_dc_values[2],
        &dc_contexts[2],
    );

    const expected_y = [_]i16{ -291, 96, 69, 23, 0, -6, -6, -3 };
    const expected_cb = [_]i16{ -14, -15, -11, -6, -2, -2, -1, -1 };
    const expected_cr = [_]i16{ 42, 44, 33, 16, 6, 5, 3, 2 };
    try std.testing.expectEqualSlices(i16, &expected_y, y[0..8]);
    try std.testing.expectEqualSlices(i16, &expected_cb, cb[0..8]);
    try std.testing.expectEqualSlices(i16, &expected_cr, cr[0..8]);
}

test "decode rgba matches manifest-backed arithmetic jpeg fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 3), decoded.width);
    try std.testing.expectEqual(@as(u32, 2), decoded.height);
    const expected_rgba = [_]u8{
        0xfa, 0x01, 0x00, 0xff,
        0xfb, 0x02, 0x00, 0xff,
        0x03, 0x00, 0x02, 0xff,
        0xfa, 0x01, 0x00, 0xff,
        0xfb, 0x02, 0x00, 0xff,
        0x03, 0x00, 0x02, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, decoded.rgba);
}

test "libjpeg arithmetic coefficient reference reconstructs expected rgba" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/arithmetic/red-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(@as(u32, 3), structure.info.width);
    try std.testing.expectEqual(@as(u32, 2), structure.info.height);

    var component_quant_tables: [max_components][64]u16 = undefined;
    var max_h: u8 = 1;
    var max_v: u8 = 1;
    for (0..structure.info.component_count) |frame_index| {
        const component = structure.info.components[frame_index];
        component_quant_tables[frame_index] = quantTableNaturalOrder(structure.quant_tables[component.quant_table_id]);
        if (component.horizontal_sampling > max_h) max_h = component.horizontal_sampling;
        if (component.vertical_sampling > max_v) max_v = component.vertical_sampling;
    }
    const rgba = try alloc.alloc(u8, @as(usize, structure.info.width) * @as(usize, structure.info.height) * 4);
    defer alloc.free(rgba);
    var component_planes = try initComponentSamplePlanes(
        alloc,
        structure.info.width,
        structure.info.height,
        structure.info.components,
        structure.info.component_count,
        max_h,
        max_v,
    );
    defer freeComponentSamplePlanes(alloc, &component_planes);

    const y_coeffs = [_]i16{
        -291, 96, 69, 23, 0, -6, -6, -3,
    } ++ ([_]i16{0} ** 56);
    const cb_coeffs = [_]i16{
        -14, -15, -11, -6, -2, -2, -1, -1,
    } ++ ([_]i16{0} ** 56);
    const cr_coeffs = [_]i16{
        42, 44, 33, 16, 6, 5, 3, 2,
    } ++ ([_]i16{0} ** 56);

    writeSpatialBlockToPlane(component_planes[0], 0, 0, dequantizeAndInverseDct(y_coeffs, component_quant_tables[0]));
    writeSpatialBlockToPlane(component_planes[0], 1, 0, dequantizeAndInverseDct([_]i16{0} ** 64, component_quant_tables[0]));
    writeSpatialBlockToPlane(component_planes[0], 0, 1, dequantizeAndInverseDct([_]i16{0} ** 64, component_quant_tables[0]));
    writeSpatialBlockToPlane(component_planes[0], 1, 1, dequantizeAndInverseDct([_]i16{0} ** 64, component_quant_tables[0]));
    writeSpatialBlockToPlane(component_planes[1], 0, 0, dequantizeAndInverseDct(cb_coeffs, component_quant_tables[1]));
    writeSpatialBlockToPlane(component_planes[2], 0, 0, dequantizeAndInverseDct(cr_coeffs, component_quant_tables[2]));

    renderColorPlanesToRgba(
        rgba,
        structure.info.width,
        structure.info.height,
        structure.info.bits_per_sample,
        .ycbcr,
        structure.info.components,
        max_h,
        max_v,
        component_planes,
    );

    const expected_rgba = [_]u8{
        0xfa, 0x01, 0x00, 0xff,
        0xfb, 0x02, 0x00, 0xff,
        0x03, 0x00, 0x02, 0xff,
        0xfa, 0x01, 0x00, 0xff,
        0xfb, 0x02, 0x00, 0xff,
        0x03, 0x00, 0x02, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, rgba);
}

test "decode rgba matches djpeg reference on embedded arithmetic cmyk fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try decodeEmbeddedBase64Alloc(alloc, embedded_arithmetic_cmyk_b64);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(?u8, 0), structure.adobe_transform);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expect(supportsPlannedArithmeticDecode(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    const expected_rgba = [_]u8{
        0xff, 0x00, 0x00, 0xff,
        0x00, 0xff, 0x00, 0xff,
        0x00, 0x00, 0xff, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, decoded.rgba);
}

test "decode rgba matches djpeg reference on embedded arithmetic ycck fixture" {
    const alloc = std.testing.allocator;
    const fixture_bytes = try decodeEmbeddedBase64Alloc(alloc, embedded_arithmetic_ycck_b64);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    try std.testing.expectEqual(FrameKind.arithmetic_sequential_dct, structure.info.frame_kind);
    try std.testing.expectEqual(@as(?u8, 2), structure.adobe_transform);
    try std.testing.expectEqual(@as(u8, 4), structure.info.component_count);
    try std.testing.expect(supportsPlannedArithmeticDecode(structure));

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    const expected_rgba = [_]u8{
        0x5b, 0x5a, 0x00, 0xff,
        0xa5, 0xa4, 0x26, 0xff,
        0x00, 0x00, 0xff, 0xff,
    };
    try std.testing.expectEqualSlices(u8, &expected_rgba, decoded.rgba);
}

test "entropy bit reader handles stuffed ff byte" {
    var reader = EntropyBitReader.init(&.{ 0xff, 0x00, 0x80 });

    try std.testing.expectEqual(@as(u16, 0xff), try reader.readBits(8));
    try std.testing.expectEqual(@as(u16, 1), try reader.readBits(1));
    try std.testing.expectEqual(@as(?u8, null), reader.peekPendingMarker());
}

test "entropy bit reader surfaces pending marker" {
    var reader = EntropyBitReader.init(&.{ 0xaa, 0xff, 0xd9 });

    try std.testing.expectEqual(@as(u16, 0xaa), try reader.readBits(8));
    try std.testing.expectError(error.JpegMarkerReached, reader.readBit());
    try std.testing.expectEqual(@as(?u8, 0xd9), reader.peekPendingMarker());
}

test "entropy bit reader consumes expected restart marker" {
    var reader = EntropyBitReader.init(&.{ 0xaa, 0xff, 0xd0, 0xbb });

    try std.testing.expectEqual(@as(u16, 0xaa), try reader.readBits(8));
    try reader.consumeExpectedRestartMarker(0xd0);
    try std.testing.expectEqual(@as(u16, 0xbb), try reader.readBits(8));
}

test "decode huffman symbol uses canonical codes" {
    var info = HuffmanTableInfo{ .present = true, .symbol_count = 2 };
    info.code_counts[0] = 1;
    info.code_counts[1] = 1;
    info.symbols[0] = 0x0a;
    info.symbols[1] = 0x0b;

    const table = try buildCanonicalHuffmanTable(info);
    var reader = EntropyBitReader.init(&.{0b0100_0000});

    try std.testing.expectEqual(@as(u8, 0x0a), try decodeHuffmanSymbol(&reader, table));
    try std.testing.expectEqual(@as(u8, 0x0b), try decodeHuffmanSymbol(&reader, table));
}

test "decode lossless difference treats category 16 as fixed 32768" {
    var info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    info.code_counts[0] = 1;
    info.symbols[0] = 16;

    const table = try buildCanonicalHuffmanTable(info);
    var reader = EntropyBitReader.init(&.{0x00});

    try std.testing.expectEqual(@as(i32, 32768), try decodeLosslessDifference(&reader, table));
}

test "decode progressive dc first populates coefficient state" {
    var dc_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    dc_info.code_counts[0] = 1;
    dc_info.symbols[0] = 2;

    const dc_table = try buildCanonicalHuffmanTable(dc_info);
    var reader = EntropyBitReader.init(&.{0b0110_0000});
    const state = try decodeProgressiveDcFirst(&reader, dc_table, 0, 1);

    try std.testing.expectEqual(@as(i16, 3), state.dc_predictor);
    try std.testing.expectEqual(@as(i16, 6), state.coefficients[0]);
}

test "refine progressive dc increments coefficient bitplane" {
    var state = ProgressiveBlockState{ .coefficients = std.mem.zeroes([64]i16), .dc_predictor = 5 };
    state.coefficients[0] = 8;

    var reader = EntropyBitReader.init(&.{0b1000_0000});
    try refineProgressiveDc(&reader, &state, 1);
    try std.testing.expectEqual(@as(i16, 10), state.coefficients[0]);
}

test "refine progressive dc preserves negative coefficient sign" {
    var state = ProgressiveBlockState{ .coefficients = std.mem.zeroes([64]i16), .dc_predictor = -5 };
    state.coefficients[0] = -12;

    var reader = EntropyBitReader.init(&.{0b1000_0000});
    try refineProgressiveDc(&reader, &state, 1);
    try std.testing.expectEqual(@as(i16, -10), state.coefficients[0]);
}

test "decode progressive dc interleaved scan carries per-component predictors" {
    var dc_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    dc_info.code_counts[0] = 1;
    dc_info.symbols[0] = 2;

    const dc_table = try buildCanonicalHuffmanTable(dc_info);
    var reader = EntropyBitReader.init(&.{ 0x68, 0xb4, 0x40 });

    var y_states = [_]ProgressiveBlockState{.{}} ** 4;
    var cb_states = [_]ProgressiveBlockState{.{}} ** 1;
    var cr_states = [_]ProgressiveBlockState{.{}} ** 1;
    const scan_components = [_]ProgressiveDcScanComponent{
        .{
            .frame_index = 0,
            .component = .{ .id = 1, .horizontal_sampling = 2, .vertical_sampling = 2, .quant_table_id = 0 },
            .dc_table = dc_table,
            .states = y_states[0..],
            .blocks_x = 2,
            .blocks_y = 2,
        },
        .{
            .frame_index = 1,
            .component = .{ .id = 2, .horizontal_sampling = 1, .vertical_sampling = 1, .quant_table_id = 1 },
            .dc_table = dc_table,
            .states = cb_states[0..],
            .blocks_x = 1,
            .blocks_y = 1,
        },
        .{
            .frame_index = 2,
            .component = .{ .id = 3, .horizontal_sampling = 1, .vertical_sampling = 1, .quant_table_id = 1 },
            .dc_table = dc_table,
            .states = cr_states[0..],
            .blocks_x = 1,
            .blocks_y = 1,
        },
    };
    var dc_predictors = [_]i16{0} ** max_components;

    try decodeProgressiveDcInterleavedScan(&reader, &scan_components, &dc_predictors, 1, 1, 0, 1, 0);

    try std.testing.expectEqual(@as(i16, 6), y_states[0].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 10), y_states[1].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 6), y_states[2].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 12), y_states[3].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 4), cb_states[0].coefficients[0]);
    try std.testing.expectEqual(@as(i16, -4), cr_states[0].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 6), dc_predictors[0]);
    try std.testing.expectEqual(@as(i16, 2), dc_predictors[1]);
    try std.testing.expectEqual(@as(i16, -2), dc_predictors[2]);
}

test "refine progressive dc interleaved scan updates only marked blocks" {
    var reader = EntropyBitReader.init(&.{0b1010_1000});
    var y_states = [_]ProgressiveBlockState{.{}} ** 4;
    var cb_states = [_]ProgressiveBlockState{.{}} ** 1;
    var cr_states = [_]ProgressiveBlockState{.{}} ** 1;
    y_states[0].coefficients[0] = 6;
    y_states[1].coefficients[0] = 10;
    y_states[2].coefficients[0] = 6;
    y_states[3].coefficients[0] = 12;
    cb_states[0].coefficients[0] = 4;
    cr_states[0].coefficients[0] = -4;

    const scan_components = [_]ProgressiveDcScanComponent{
        .{
            .frame_index = 0,
            .component = .{ .id = 1, .horizontal_sampling = 2, .vertical_sampling = 2, .quant_table_id = 0 },
            .dc_table = undefined,
            .states = y_states[0..],
            .blocks_x = 2,
            .blocks_y = 2,
        },
        .{
            .frame_index = 1,
            .component = .{ .id = 2, .horizontal_sampling = 1, .vertical_sampling = 1, .quant_table_id = 1 },
            .dc_table = undefined,
            .states = cb_states[0..],
            .blocks_x = 1,
            .blocks_y = 1,
        },
        .{
            .frame_index = 2,
            .component = .{ .id = 3, .horizontal_sampling = 1, .vertical_sampling = 1, .quant_table_id = 1 },
            .dc_table = undefined,
            .states = cr_states[0..],
            .blocks_x = 1,
            .blocks_y = 1,
        },
    };
    var dc_predictors = [_]i16{ 6, 2, -2, 0 };

    try decodeProgressiveDcInterleavedScan(&reader, &scan_components, &dc_predictors, 1, 1, 1, 0, 0);

    try std.testing.expectEqual(@as(i16, 7), y_states[0].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 10), y_states[1].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 7), y_states[2].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 12), y_states[3].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 5), cb_states[0].coefficients[0]);
    try std.testing.expectEqual(@as(i16, -4), cr_states[0].coefficients[0]);
}

test "decode progressive dc single-component scan carries predictor across blocks" {
    var dc_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    dc_info.code_counts[0] = 1;
    dc_info.symbols[0] = 2;

    const dc_table = try buildCanonicalHuffmanTable(dc_info);
    var reader = EntropyBitReader.init(&.{0b0110_1100});
    var states = [_]ProgressiveBlockState{ .{}, .{} };
    var dc_predictor: i16 = 0;

    try decodeProgressiveDcSingleComponentScan(&reader, dc_table, &states, &dc_predictor, 0, 1);

    try std.testing.expectEqual(@as(i16, 6), states[0].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 12), states[1].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 6), dc_predictor);
}

test "refine progressive dc single-component scan updates marked blocks" {
    var reader = EntropyBitReader.init(&.{0b1010_0000});
    var states = [_]ProgressiveBlockState{ .{}, .{} };
    states[0].coefficients[0] = 6;
    states[1].coefficients[0] = 12;
    var dc_predictor: i16 = 6;

    try decodeProgressiveDcSingleComponentScan(&reader, undefined, &states, &dc_predictor, 1, 0);

    try std.testing.expectEqual(@as(i16, 7), states[0].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 12), states[1].coefficients[0]);
    try std.testing.expectEqual(@as(i16, 6), dc_predictor);
}

test "decode progressive ac first writes coefficient run" {
    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 2 };
    ac_info.code_counts[0] = 1;
    ac_info.code_counts[1] = 1;
    ac_info.symbols[0] = 0x11;
    ac_info.symbols[1] = 0x00;

    const ac_table = try buildCanonicalHuffmanTable(ac_info);
    var reader = EntropyBitReader.init(&.{0b0110_0000});
    var state = ProgressiveBlockState{};

    try decodeProgressiveAcFirst(&reader, ac_table, &state, 1, 63, 0);
    try std.testing.expectEqual(@as(i16, 1), state.coefficients[8]);
}

test "decode progressive ac first tracks end-of-band run" {
    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    ac_info.code_counts[0] = 1;
    ac_info.symbols[0] = 0x10;

    const ac_table = try buildCanonicalHuffmanTable(ac_info);
    var reader = EntropyBitReader.init(&.{0b0100_0000});
    var state = ProgressiveBlockState{};
    var eob_run: u16 = 0;

    try decodeProgressiveAcFirstWithEobRun(&reader, ac_table, &state, 1, 3, 0, &eob_run);
    try std.testing.expectEqual(@as(u16, 2), eob_run);

    try decodeProgressiveAcFirstWithEobRun(&reader, ac_table, &state, 1, 3, 0, &eob_run);
    try std.testing.expectEqual(@as(u16, 1), eob_run);
}

test "decode progressive ac scan carries end-of-band run across blocks" {
    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    ac_info.code_counts[0] = 1;
    ac_info.symbols[0] = 0x10;

    const ac_table = try buildCanonicalHuffmanTable(ac_info);
    var reader = EntropyBitReader.init(&.{0b0100_0000});
    var states = [_]ProgressiveBlockState{ .{}, .{}, .{} };

    try decodeProgressiveAcScan(&reader, ac_table, &states, 1, 3, 0);
    for (states) |state| {
        try std.testing.expectEqual(@as(i16, 0), state.coefficients[zig_zag_to_natural[1]]);
        try std.testing.expectEqual(@as(i16, 0), state.coefficients[zig_zag_to_natural[2]]);
        try std.testing.expectEqual(@as(i16, 0), state.coefficients[zig_zag_to_natural[3]]);
    }
}

test "refine progressive ac inserts new coefficient and refines existing one" {
    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    ac_info.code_counts[0] = 1;
    ac_info.symbols[0] = 0x01;

    const ac_table = try buildCanonicalHuffmanTable(ac_info);
    var reader = EntropyBitReader.init(&.{0b0110_0000});
    var state = ProgressiveBlockState{};
    state.coefficients[zig_zag_to_natural[1]] = 2;
    var eob_run: u16 = 0;

    try refineProgressiveAc(&reader, ac_table, &state, 1, 2, 0, &eob_run);
    try std.testing.expectEqual(@as(i16, 3), state.coefficients[zig_zag_to_natural[1]]);
    try std.testing.expectEqual(@as(i16, 1), state.coefficients[zig_zag_to_natural[2]]);
    try std.testing.expectEqual(@as(u16, 0), eob_run);
}

test "refine progressive ac honors eob run while refining existing coefficients" {
    var reader = EntropyBitReader.init(&.{0b1000_0000});
    var state = ProgressiveBlockState{};
    state.coefficients[zig_zag_to_natural[1]] = 2;
    var eob_run: u16 = 1;

    try refineProgressiveAc(&reader, undefined, &state, 1, 2, 0, &eob_run);
    try std.testing.expectEqual(@as(i16, 3), state.coefficients[zig_zag_to_natural[1]]);
    try std.testing.expectEqual(@as(u16, 0), eob_run);
}

test "refine progressive ac increments negative coefficient when bit not set" {
    var reader = EntropyBitReader.init(&.{0b1000_0000});
    var state = ProgressiveBlockState{};
    state.coefficients[zig_zag_to_natural[1]] = -3;
    var eob_run: u16 = 1;

    try refineProgressiveAc(&reader, undefined, &state, 1, 1, 1, &eob_run);
    try std.testing.expectEqual(@as(i16, -5), state.coefficients[zig_zag_to_natural[1]]);
    try std.testing.expectEqual(@as(u16, 0), eob_run);
}

test "refine progressive ac decodes end-of-band run" {
    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    ac_info.code_counts[0] = 1;
    ac_info.symbols[0] = 0x10;

    const ac_table = try buildCanonicalHuffmanTable(ac_info);
    var reader = EntropyBitReader.init(&.{0b0100_0000});
    var state = ProgressiveBlockState{};
    var eob_run: u16 = 0;

    try refineProgressiveAc(&reader, ac_table, &state, 1, 3, 0, &eob_run);
    try std.testing.expectEqual(@as(u16, 2), eob_run);
}

test "refine progressive ac scan carries end-of-band run across blocks" {
    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    ac_info.code_counts[0] = 1;
    ac_info.symbols[0] = 0x10;

    const ac_table = try buildCanonicalHuffmanTable(ac_info);
    var reader = EntropyBitReader.init(&.{0b0100_0000});
    var states = [_]ProgressiveBlockState{ .{}, .{}, .{} };
    states[1].coefficients[zig_zag_to_natural[1]] = 2;
    states[2].coefficients[zig_zag_to_natural[1]] = 2;

    try refineProgressiveAcScan(&reader, ac_table, &states, 1, 3, 0);
    try std.testing.expectEqual(@as(i16, 0), states[0].coefficients[zig_zag_to_natural[1]]);
    try std.testing.expectEqual(@as(i16, 2), states[1].coefficients[zig_zag_to_natural[1]]);
    try std.testing.expectEqual(@as(i16, 2), states[2].coefficients[zig_zag_to_natural[1]]);
}

test "refine progressive ac zrl advances to next zero slot" {
    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 2 };
    ac_info.code_counts[0] = 1;
    ac_info.code_counts[1] = 1;
    ac_info.symbols[0] = 0xf0;
    ac_info.symbols[1] = 0x01;

    const ac_table = try buildCanonicalHuffmanTable(ac_info);
    var reader = EntropyBitReader.init(&.{0b0101_0000});
    var state = ProgressiveBlockState{};
    var eob_run: u16 = 0;

    try refineProgressiveAc(&reader, ac_table, &state, 1, 20, 0, &eob_run);
    try std.testing.expectEqual(@as(i16, 0), state.coefficients[zig_zag_to_natural[16]]);
    try std.testing.expectEqual(@as(i16, 1), state.coefficients[zig_zag_to_natural[17]]);
    try std.testing.expectEqual(@as(u16, 0), eob_run);
}

test "extend sign matches jpeg coefficient semantics" {
    try std.testing.expectEqual(@as(i16, 5), try extendSign(0b101, 3));
    try std.testing.expectEqual(@as(i16, -2), try extendSign(0b01, 2));
    try std.testing.expectEqual(@as(i16, -7), try extendSign(0b000, 3));
    try std.testing.expectEqual(@as(i16, 0), try extendSign(0, 0));
}

test "baseline fixture huffman tables build canonical decode tables" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/white-2x1.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    const dc_table = try buildCanonicalHuffmanTable(structure.huffman_dc_tables[0]);
    const ac_table = try buildCanonicalHuffmanTable(structure.huffman_ac_tables[0]);

    try std.testing.expect(dc_table.symbol_count > 0);
    try std.testing.expect(ac_table.symbol_count > 0);
    try std.testing.expect(dc_table.first_symbol_index[0] == 0);
    try std.testing.expect(ac_table.first_symbol_index[0] == 0);
}

test "decode baseline block handles dc plus eob" {
    var dc_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    dc_info.code_counts[0] = 1;
    dc_info.symbols[0] = 2;

    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    ac_info.code_counts[0] = 1;
    ac_info.symbols[0] = 0x00;

    const dc_table = try buildCanonicalHuffmanTable(dc_info);
    const ac_table = try buildCanonicalHuffmanTable(ac_info);

    var reader = EntropyBitReader.init(&.{0b0110_0000});
    const block = try decodeBaselineBlock(&reader, dc_table, ac_table, 0);

    try std.testing.expectEqual(@as(i64, 3), block.coefficients[0]);
    try std.testing.expectEqual(@as(i64, 3), block.dc_predictor);
    for (block.coefficients[1..]) |coeff| try std.testing.expectEqual(@as(i64, 0), coeff);
}

test "decode baseline block handles run length and nonzero ac coefficient" {
    var dc_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    dc_info.code_counts[0] = 1;
    dc_info.symbols[0] = 2;

    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 2 };
    ac_info.code_counts[0] = 1;
    ac_info.code_counts[1] = 1;
    ac_info.symbols[0] = 0x11;
    ac_info.symbols[1] = 0x00;

    const dc_table = try buildCanonicalHuffmanTable(dc_info);
    const ac_table = try buildCanonicalHuffmanTable(ac_info);

    var reader = EntropyBitReader.init(&.{0b0110_1100});
    const block = try decodeBaselineBlock(&reader, dc_table, ac_table, 0);

    try std.testing.expectEqual(@as(i64, 3), block.coefficients[0]);
    try std.testing.expectEqual(@as(i64, 1), block.coefficients[8]);
}

test "decode baseline block rejects overflowing dc predictor" {
    var dc_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    dc_info.code_counts[0] = 1;
    dc_info.symbols[0] = 11;

    var ac_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    ac_info.code_counts[0] = 1;
    ac_info.symbols[0] = 0x00;

    const dc_table = try buildCanonicalHuffmanTable(dc_info);
    const ac_table = try buildCanonicalHuffmanTable(ac_info);

    var reader = EntropyBitReader.init(&.{ 0x7f, 0xf0 });
    try std.testing.expectError(error.JpegDecodeFailed, decodeBaselineBlock(&reader, dc_table, ac_table, std.math.maxInt(i64)));
}

test "decode progressive dc first rejects overflowing dc predictor" {
    var dc_info = HuffmanTableInfo{ .present = true, .symbol_count = 1 };
    dc_info.code_counts[0] = 1;
    dc_info.symbols[0] = 11;

    const dc_table = try buildCanonicalHuffmanTable(dc_info);

    var reader = EntropyBitReader.init(&.{ 0x7f, 0xf0 });
    try std.testing.expectError(error.JpegDecodeFailed, decodeProgressiveDcFirst(&reader, dc_table, std.math.maxInt(i16), 0));
}

test "arithmetic dc predictor wraps modulo 65536" {
    try std.testing.expectEqual(@as(i32, 0), wrapArithmeticDcPredictor(65535, 1));
    try std.testing.expectEqual(@as(i32, 65535), wrapArithmeticDcPredictor(0, -1));
    try std.testing.expectEqual(@as(i32, 32768), wrapArithmeticDcPredictor(32767, 1));
}

test "arithmetic predictor low 16 bits map back to signed coefficient" {
    try std.testing.expectEqual(@as(i16, 0), arithmeticPredictorToCoefficient(0));
    try std.testing.expectEqual(@as(i16, -1), arithmeticPredictorToCoefficient(65535));
    try std.testing.expectEqual(@as(i16, -32768), arithmeticPredictorToCoefficient(32768));
    try std.testing.expectEqual(@as(i16, 32767), arithmeticPredictorToCoefficient(32767));
}

test "grayscale baseline fixture first block decodes from scan entropy" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "jpeg/baseline/gray-3x2.jpg") orelse return error.MissingImageFixture;
    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const structure = try parseStructure(fixture_bytes);
    const scan = structure.scans[0];
    const dc_table = try buildCanonicalHuffmanTable(structure.huffman_dc_tables[scan.components[0].dc_table_id]);
    const ac_table = try buildCanonicalHuffmanTable(structure.huffman_ac_tables[scan.components[0].ac_table_id]);
    const entropy_bytes = try scanEntropyBytes(structure, fixture_bytes, 0);

    var reader = EntropyBitReader.init(entropy_bytes);
    const block = try decodeBaselineBlock(&reader, dc_table, ac_table, 0);

    try std.testing.expect(block.coefficients[0] != 0);
    try std.testing.expectEqual(block.coefficients[0], block.dc_predictor);
}
