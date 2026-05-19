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
const markers = @import("markers.zig");
const codestream = @import("codestream.zig");

pub const native_port_available = true;

/// Parameters for encoding a JPEG 2000 codestream.
pub const EncodeParams = struct {
    width: u32,
    height: u32,
    components: []const ComponentParam,
    tile_width: u32 = 0, // 0 = single tile
    tile_height: u32 = 0, // 0 = single tile
    decomposition_levels: u8 = 5,
    progression_order: u8 = 0, // LRCP
    num_layers: u16 = 1,
    wavelet_transform: u8 = 1, // 0 = 9/7, 1 = 5/3
    multiple_component_transform: bool = false,
    code_block_width_exponent: u8 = 4, // 2^(4+2) = 64
    code_block_height_exponent: u8 = 4, // 2^(4+2) = 64
    /// Emit SOP (Start Of Packet) markers. Sets Scod bit 1; marker emission
    /// is handled by the tile-data assembler, not this writer.
    emit_sop_markers: bool = false,
    /// Emit EPH (End of Packet Header) markers. Sets Scod bit 2.
    emit_eph_markers: bool = false,
};

pub const ComponentParam = struct {
    bits_per_component: u8,
    is_signed: bool,
    xrsiz: u8 = 1,
    yrsiz: u8 = 1,
};

const ByteList = std.ArrayListUnmanaged(u8);

/// Write a complete JPEG 2000 codestream.
/// Returns the serialized bytes.
pub fn writeCodestream(
    allocator: std.mem.Allocator,
    params: *const EncodeParams,
    tile_data: []const u8, // Pre-assembled tile data from tier2_encode
) ![]u8 {
    var out: ByteList = .empty;
    errdefer out.deinit(allocator);

    // SOC - Start of Codestream
    try writeMarker(allocator, &out, markers.soc);

    // SIZ - Image and Tile Size
    try writeSiz(allocator, &out, params);

    // COD - Coding Style Default
    try writeCod(allocator, &out, params);

    // QCD - Quantization Default
    try writeQcd(allocator, &out, params);

    // SOT - Start of Tile
    // Psot counts from the first byte of the SOT marker to the end of the
    // tile-part data: SOT segment (12) + SOD marker (2) + tile_data.len.
    const psot: u32 = @intCast(14 + tile_data.len);
    try writeSot(allocator, &out, 0, psot, 0, 1);

    // SOD - Start of Data
    try writeMarker(allocator, &out, markers.sod);

    // Tile data
    try out.appendSlice(allocator, tile_data);

    // EOC - End of Codestream
    try writeMarker(allocator, &out, markers.eoc);

    return out.toOwnedSlice(allocator);
}

fn writeMarker(allocator: std.mem.Allocator, out: *ByteList, marker: u16) !void {
    try out.append(allocator, @intCast(marker >> 8));
    try out.append(allocator, @intCast(marker & 0xff));
}

fn writeU16(allocator: std.mem.Allocator, out: *ByteList, value: u16) !void {
    try out.append(allocator, @intCast(value >> 8));
    try out.append(allocator, @intCast(value & 0xff));
}

fn writeU32(allocator: std.mem.Allocator, out: *ByteList, value: u32) !void {
    try out.append(allocator, @intCast((value >> 24) & 0xff));
    try out.append(allocator, @intCast((value >> 16) & 0xff));
    try out.append(allocator, @intCast((value >> 8) & 0xff));
    try out.append(allocator, @intCast(value & 0xff));
}

/// Write SIZ marker segment.
///
/// Layout (offsets relative to the marker):
///   marker(2) + Lsiz(2) + Rsiz(2) + Xsiz(4) + Ysiz(4)
///   + XOsiz(4) + YOsiz(4) + XTsiz(4) + YTsiz(4)
///   + XTOsiz(4) + YTOsiz(4) + Csiz(2)
///   + per-component: Ssiz(1) + XRsiz(1) + YRsiz(1)
///
/// Lsiz = 38 + 3 * Csiz.
fn writeSiz(allocator: std.mem.Allocator, out: *ByteList, params: *const EncodeParams) !void {
    try writeMarker(allocator, out, markers.siz);
    const csiz: u16 = @intCast(params.components.len);
    const lsiz: u16 = 38 + 3 * csiz;
    try writeU16(allocator, out, lsiz);
    // Rsiz = 0 (no profile)
    try writeU16(allocator, out, 0);
    // Xsiz, Ysiz (image size; offsets are zero so size equals reference grid)
    try writeU32(allocator, out, params.width);
    try writeU32(allocator, out, params.height);
    // XOsiz, YOsiz = 0
    try writeU32(allocator, out, 0);
    try writeU32(allocator, out, 0);
    // XTsiz, YTsiz (tile size; use image size for single tile)
    const tile_w = if (params.tile_width == 0) params.width else params.tile_width;
    const tile_h = if (params.tile_height == 0) params.height else params.tile_height;
    try writeU32(allocator, out, tile_w);
    try writeU32(allocator, out, tile_h);
    // XTOsiz, YTOsiz = 0
    try writeU32(allocator, out, 0);
    try writeU32(allocator, out, 0);
    // Csiz
    try writeU16(allocator, out, csiz);
    // Per-component: Ssiz, XRsiz, YRsiz
    for (params.components) |comp| {
        const ssiz: u8 = if (comp.is_signed) (comp.bits_per_component - 1) | 0x80 else comp.bits_per_component - 1;
        try out.append(allocator, ssiz);
        try out.append(allocator, comp.xrsiz);
        try out.append(allocator, comp.yrsiz);
    }
}

/// Write COD marker segment.
///
/// Layout: marker(2) + Lcod(2) + Scod(1) + SGcod(4) + SPcod(5) = 14.
/// Lcod = 12 (no precinct sizes appended).
fn writeCod(allocator: std.mem.Allocator, out: *ByteList, params: *const EncodeParams) !void {
    try writeMarker(allocator, out, markers.cod);
    const lcod: u16 = 12; // Fixed size when no precincts
    try writeU16(allocator, out, lcod);
    // Scod: coding style. Bit 1 = SOP present, bit 2 = EPH present.
    var scod: u8 = 0;
    if (params.emit_sop_markers) scod |= 0x02;
    if (params.emit_eph_markers) scod |= 0x04;
    try out.append(allocator, scod);
    // SGcod: progression order, number of layers, MCT
    try out.append(allocator, params.progression_order);
    try writeU16(allocator, out, params.num_layers);
    try out.append(allocator, if (params.multiple_component_transform) 1 else 0);
    // SPcod: decomposition levels, code-block size, style, transform.
    // Code-block exponent is stored raw; the parser adds 2 on read.
    try out.append(allocator, params.decomposition_levels);
    try out.append(allocator, params.code_block_width_exponent);
    try out.append(allocator, params.code_block_height_exponent);
    try out.append(allocator, 0); // code_block_style = 0
    try out.append(allocator, params.wavelet_transform);
}

/// Write QCD marker segment.
///
/// For the reversible (5/3) wavelet (transform=1), uses quantization style 0
/// with 1-byte step values.  For the irreversible (9/7) wavelet (transform=0),
/// uses quantization style 1 (scalar derived) with 2-byte step values.
fn writeQcd(allocator: std.mem.Allocator, out: *ByteList, params: *const EncodeParams) !void {
    try writeMarker(allocator, out, markers.qcd);
    // Number of step values = 3 * decomposition_levels + 1
    const num_steps: u16 = 3 * @as(u16, params.decomposition_levels) + 1;
    if (params.wavelet_transform == 1) {
        // Style 0 (no quantization): 1 byte per step value
        const lqcd: u16 = 3 + num_steps;
        try writeU16(allocator, out, lqcd);
        // Sqcd = (guard_bits << 5) | style
        const sqcd: u8 = (2 << 5) | 0; // 2 guard bits, style 0
        try out.append(allocator, sqcd);
        // Step values: exponent only, packed as (expn << 3).
        const bpc = params.components[0].bits_per_component;
        // LL subband
        try out.append(allocator, bpc << 3);
        // Three subbands per decomposition level (HL, LH, HH)
        var r: u16 = 0;
        while (r < params.decomposition_levels) : (r += 1) {
            try out.append(allocator, (bpc + 1) << 3);
            try out.append(allocator, (bpc + 1) << 3);
            try out.append(allocator, (bpc + 1) << 3);
        }
    } else {
        // Style 1 (scalar derived): 2 bytes per step value
        const lqcd: u16 = 3 + 2 * num_steps;
        try writeU16(allocator, out, lqcd);
        const sqcd: u8 = (2 << 5) | 1; // 2 guard bits, style 1
        try out.append(allocator, sqcd);
        const bpc = params.components[0].bits_per_component;
        var r: u16 = 0;
        while (r <= params.decomposition_levels) : (r += 1) {
            const num_subbands: u16 = if (r == 0) 1 else 3;
            var s: u16 = 0;
            while (s < num_subbands) : (s += 1) {
                const expn: u16 = bpc + 1;
                try writeU16(allocator, out, expn << 11); // mantissa = 0
            }
        }
    }
}

/// Write SOT marker segment.
///
/// Layout: marker(2) + Lsot(2) + Isot(2) + Psot(4) + TPsot(1) + TNsot(1).
/// Lsot is always 10.
fn writeSot(allocator: std.mem.Allocator, out: *ByteList, tile_index: u16, tile_part_length: u32, tile_part_index: u8, num_tile_parts: u8) !void {
    try writeMarker(allocator, out, markers.sot);
    try writeU16(allocator, out, 10); // Lsot
    try writeU16(allocator, out, tile_index);
    try writeU32(allocator, out, tile_part_length);
    try out.append(allocator, tile_part_index);
    try out.append(allocator, num_tile_parts);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "writeCodestream produces SOC header and EOC trailer" {
    const allocator = std.testing.allocator;
    const components = [_]ComponentParam{
        .{ .bits_per_component = 8, .is_signed = false },
    };
    const params = EncodeParams{
        .width = 64,
        .height = 64,
        .components = &components,
        .decomposition_levels = 0,
    };
    const tile_data: []const u8 = &.{ 0xAA, 0xBB };
    const result = try writeCodestream(allocator, &params, tile_data);
    defer allocator.free(result);

    // Starts with SOC marker (0xff4f)
    try std.testing.expectEqual(@as(u16, markers.soc), std.mem.readInt(u16, @ptrCast(result[0..2].ptr), .big));
    // Ends with EOC marker (0xffd9)
    try std.testing.expectEqual(@as(u16, markers.eoc), std.mem.readInt(u16, @ptrCast(result[result.len - 2 .. result.len].ptr), .big));
}

test "writeCodestream round-trips through parseState" {
    const allocator = std.testing.allocator;
    const components = [_]ComponentParam{
        .{ .bits_per_component = 8, .is_signed = false },
        .{ .bits_per_component = 8, .is_signed = false },
        .{ .bits_per_component = 8, .is_signed = false },
    };
    const params = EncodeParams{
        .width = 256,
        .height = 256,
        .components = &components,
        .decomposition_levels = 5,
        .wavelet_transform = 1,
    };
    const tile_data: []const u8 = &.{ 0x00, 0x01, 0x02, 0x03 };
    const result = try writeCodestream(allocator, &params, tile_data);
    defer allocator.free(result);

    // The serialized bytes must be parseable by the codestream reader.
    var state = try codestream.parseState(allocator, result);
    defer state.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 256), state.header.width);
    try std.testing.expectEqual(@as(u32, 256), state.header.height);
    try std.testing.expectEqual(@as(usize, 3), state.header.components.len);
    for (state.header.components) |comp| {
        try std.testing.expectEqual(@as(u8, 8), comp.bits_per_component);
        try std.testing.expect(!comp.is_signed);
    }
    try std.testing.expect(state.coding_style != null);
    try std.testing.expectEqual(@as(u8, 5), state.coding_style.?.decomposition_levels);
    try std.testing.expectEqual(@as(u8, 1), state.coding_style.?.wavelet_transform);
    try std.testing.expect(state.quantization_style != null);
    try std.testing.expectEqual(@as(u8, 0), state.quantization_style.?.style);
    try std.testing.expectEqual(@as(usize, 1), state.tile_parts.len);
    try std.testing.expect(state.has_start_of_data);
    try std.testing.expect(state.has_end_of_codestream);
}

test "writeSiz produces correct header for 3-component 8-bit 256x256 image" {
    const allocator = std.testing.allocator;
    const components = [_]ComponentParam{
        .{ .bits_per_component = 8, .is_signed = false },
        .{ .bits_per_component = 8, .is_signed = false },
        .{ .bits_per_component = 8, .is_signed = false },
    };
    const params = EncodeParams{
        .width = 256,
        .height = 256,
        .components = &components,
    };
    var out: ByteList = .empty;
    defer out.deinit(allocator);

    try writeSiz(allocator, &out, &params);

    const bytes = out.items;
    // Marker
    try std.testing.expectEqual(@as(u16, markers.siz), std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big));
    // Lsiz = 38 + 3*3 = 47
    try std.testing.expectEqual(@as(u16, 47), std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big));
    // Rsiz = 0
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, @ptrCast(bytes[4..6].ptr), .big));
    // Xsiz = 256
    try std.testing.expectEqual(@as(u32, 256), std.mem.readInt(u32, @ptrCast(bytes[6..10].ptr), .big));
    // Ysiz = 256
    try std.testing.expectEqual(@as(u32, 256), std.mem.readInt(u32, @ptrCast(bytes[10..14].ptr), .big));
    // XOsiz = 0
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, @ptrCast(bytes[14..18].ptr), .big));
    // YOsiz = 0
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, @ptrCast(bytes[18..22].ptr), .big));
    // XTsiz = 256 (single tile)
    try std.testing.expectEqual(@as(u32, 256), std.mem.readInt(u32, @ptrCast(bytes[22..26].ptr), .big));
    // YTsiz = 256 (single tile)
    try std.testing.expectEqual(@as(u32, 256), std.mem.readInt(u32, @ptrCast(bytes[26..30].ptr), .big));
    // XTOsiz = 0
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, @ptrCast(bytes[30..34].ptr), .big));
    // YTOsiz = 0
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, @ptrCast(bytes[34..38].ptr), .big));
    // Csiz = 3
    try std.testing.expectEqual(@as(u16, 3), std.mem.readInt(u16, @ptrCast(bytes[38..40].ptr), .big));
    // Component 0: Ssiz=7 (8-bit unsigned), XRsiz=1, YRsiz=1
    try std.testing.expectEqual(@as(u8, 7), bytes[40]);
    try std.testing.expectEqual(@as(u8, 1), bytes[41]);
    try std.testing.expectEqual(@as(u8, 1), bytes[42]);
    // Total segment length: marker(2) + Lsiz(47) = 49
    try std.testing.expectEqual(@as(usize, 49), bytes.len);
}
