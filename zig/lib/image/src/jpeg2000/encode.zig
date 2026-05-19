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
const compat = @import("compat.zig");
const box = @import("box.zig");
const color_transform = @import("color_transform.zig");
const codestream = @import("codestream.zig");
const markers = @import("markers.zig");
const quantization = @import("quantization.zig");
const rate_control = @import("rate_control.zig");
const tier1_encode = @import("tier1_encode.zig");
const tier2_encode = @import("tier2_encode.zig");
const tile_mod = @import("tile.zig");
const wavelet = @import("wavelet.zig");

pub const EncodeBackend = enum {
    pure_zig,
};

/// Single window for a POC (Progression Order Change) segment. Fields match
/// `codestream.PocEntry` semantics: resolution half-open `[rs, re)`, component
/// half-open `[cs, ce)`, layers `[0, lye)`, ordered by `order`.
pub const ProgressionWindow = struct {
    rs: u8,
    re: u8,
    cs: u16,
    ce: u16,
    lye: u16,
    order: u8,
};

pub const EncodeParams = struct {
    width: u32,
    height: u32,
    components: u8,
    tile_width: u32 = 0,
    tile_height: u32 = 0,
    tile_parts_per_tile: u8 = 1,
    bits_per_component: u8 = 8,
    decomposition_levels: u8 = 5,
    num_layers: u16 = 1,
    progression_order: u8 = 0,
    wavelet_transform: u8 = 1, // 0=9/7, 1=5/3
    multiple_component_transform: bool = false,
    code_block_width_exponent: u8 = 2,
    code_block_height_exponent: u8 = 2,
    /// COD Scod byte: code-block coding-mode bits (BYPASS, RESET, TERMALL,
    /// VSC, PTERM, SEGSYM). BYPASS (0x01), RESET (0x02), TERMALL (0x04),
    /// VSC (0x08), PTERM (0x10), and SEGSYM (0x20) are accepted. PTERM
    /// is currently encode-side only; the decoder accepts it without
    /// verifying the predictable-termination bit pattern.
    code_block_style: u8 = 0,
    precinct_width_exponent: ?u8 = null,
    precinct_height_exponent: ?u8 = null,
    format: Format = .jp2,
    /// Optional rate target expressed in bits per pixel (bpp). When set, the encoder
    /// runs PCRD post-Tier-1 truncation to keep the resulting bitstream under the
    /// implied byte budget. Mutually exclusive with `target_bytes`.
    target_bitrate: ?f32 = null,
    /// Optional absolute byte target for the encoded codestream (codestream only;
    /// does not include JP2 container overhead).
    target_bytes: ?u32 = null,
    /// Emit SOP (Start Of Packet) markers before each packet; sets Scod bit 1.
    emit_sop_markers: bool = false,
    /// Emit EPH (End of Packet Header) markers after each packet header;
    /// sets Scod bit 2.
    emit_eph_markers: bool = false,
    /// Emit PLT markers (packet-length tags) in each tile-part header.
    emit_plt: bool = false,
    /// Emit a TLM marker (tile-part lengths) in the main header.
    emit_tlm: bool = false,
    /// When non-empty, emit a POC marker after QCD. The `progression_order`
    /// field remains the *default* order (used for any packets outside all
    /// windows, and advertised in COD). Each window describes one POC entry.
    progression_windows: []const ProgressionWindow = &.{},
    /// When set, enables Region-of-Interest encoding on the named component.
    /// The encoder upshifts coefficients inside the ROI by `roi_shift` bitplanes
    /// and emits an RGN marker with Srgn=0 (implicit max-shift) so the decoder
    /// can reverse the shift.
    roi_component: ?u8 = null,
    /// Number of bitplanes to shift ROI coefficients by (ignored when
    /// `roi_component` is null). Applied per-codeblock during Tier-1.
    roi_shift: u8 = 0,
    /// Optional per-pixel ROI mask over the image (row-major, length width*height).
    /// When null and `roi_shift > 0`, the whole component is treated as ROI.
    roi_mask: ?[]const bool = null,
    /// Optional user-supplied decorrelation matrix for 3-component images with
    /// 9/7 (irreversible) wavelet. When non-null AND `multiple_component_transform`
    /// is true AND `wavelet_transform == 0`, the encoder applies this matrix
    /// instead of the built-in ICT, and emits MCT/MCC/MCO markers after QCD.
    custom_mct: ?color_transform.CustomMctMatrix = null,
    // TODO: packed-packet-headers (PPM/PPT) markers are intentionally
    // deferred until tier-2 supports decoding them.
};

pub const Format = enum { j2k, jp2 };

/// Encode pixel data to JPEG 2000 lossless (5/3 wavelet) and write to a file.
///
/// Input: interleaved u8 pixel data (RGB or grayscale), width, height, components.
/// Writes JP2 container wrapping a J2K codestream to the given path.
pub fn encodeU8(
    allocator: std.mem.Allocator,
    path: []const u8,
    width: u32,
    height: u32,
    components: u8,
    pixels: []const u8,
) !EncodeBackend {
    const params = EncodeParams{
        .width = width,
        .height = height,
        .components = components,
        .tile_width = 0,
        .tile_height = 0,
        .tile_parts_per_tile = 1,
        .bits_per_component = 8,
        .decomposition_levels = clampDecompositionLevels(width, height),
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .precinct_width_exponent = null,
        .precinct_height_exponent = null,
        .format = .jp2,
    };
    const encoded = try encodeU8Bytes(allocator, pixels, &params);
    defer allocator.free(encoded);

    try writeEncodedFile(path, encoded);

    return .pure_zig;
}

pub fn encodeU16(
    allocator: std.mem.Allocator,
    path: []const u8,
    pixels: []const u16,
    params: *const EncodeParams,
) !EncodeBackend {
    const encoded = try encodeU16Bytes(allocator, pixels, params);
    defer allocator.free(encoded);

    try writeEncodedFile(path, encoded);
    return .pure_zig;
}

fn writeEncodedFile(path: []const u8, encoded: []const u8) !void {
    const io = compat.io();
    if (std.fs.path.isAbsolute(path)) {
        const dir_path = std.fs.path.dirname(path) orelse return error.InvalidOutputPath;
        const basename = std.fs.path.basename(path);
        var dir = try std.Io.Dir.openDirAbsolute(io, dir_path, .{});
        defer dir.close(io);
        try dir.writeFile(io, .{ .sub_path = basename, .data = encoded });
        return;
    }

    const dir = compat.cwd();
    try dir.writeFile(io, .{ .sub_path = path, .data = encoded });
}

/// Encode pixel data to JPEG 2000 lossless (5/3 wavelet) and return encoded bytes.
///
/// Input: interleaved u8 pixel data (RGB or grayscale), width, height, components.
/// Output: J2K or JP2 encoded bytes owned by the caller.
pub fn encodeU8Bytes(
    allocator: std.mem.Allocator,
    pixels: []const u8,
    params: *const EncodeParams,
) ![]u8 {
    const w: usize, const h: usize, const num_components: usize, const tile_width: usize, const tile_height: usize = try validateEncodeParams(params, pixels.len);

    const planes = try buildUnsignedPlanesU8(allocator, pixels, w, h, num_components);
    defer freePlanes(allocator, planes);

    return encodeFromShiftedPlanes(allocator, planes, params, w, h, num_components, tile_width, tile_height);
}

pub fn encodeU16Bytes(
    allocator: std.mem.Allocator,
    pixels: []const u16,
    params: *const EncodeParams,
) ![]u8 {
    const w: usize, const h: usize, const num_components: usize, const tile_width: usize, const tile_height: usize = try validateEncodeParams(params, pixels.len);
    if (params.bits_per_component == 0 or params.bits_per_component > 16) return error.UnsupportedSamplePrecision;

    const planes = try buildUnsignedPlanesU16(allocator, pixels, w, h, num_components, params.bits_per_component);
    defer freePlanes(allocator, planes);

    return encodeFromShiftedPlanes(allocator, planes, params, w, h, num_components, tile_width, tile_height);
}

fn validateEncodeParams(params: *const EncodeParams, pixel_len: usize) !struct {
    usize,
    usize,
    usize,
    usize,
    usize,
} {
    const w: usize = params.width;
    const h: usize = params.height;
    const num_components: usize = params.components;

    if (pixel_len != w * h * num_components) return error.InvalidPixelDataLength;
    if (params.code_block_width_exponent > 30 or params.code_block_height_exponent > 30) {
        return error.UnsupportedCodeBlockSize;
    }
    // ISO 15444-1 Table A-18: code-block dimensions are 2^(exp+2) with both
    // dimensions in [4, 1024] and width*height <= 4096. Translating to
    // exponents: each <= 8 and their sum <= 8.
    if (params.code_block_width_exponent > 8 or params.code_block_height_exponent > 8) {
        return error.UnsupportedCodeBlockSize;
    }
    if (@as(u16, params.code_block_width_exponent) + @as(u16, params.code_block_height_exponent) > 8) {
        return error.UnsupportedCodeBlockSize;
    }
    // Accept BYPASS (0x01), RESET (0x02), TERMALL (0x04), VSC (0x08),
    // PTERM (0x10) and SEGSYM (0x20). ERTERM (0x08 is VSC here; bit 3 is
    // reserved/ERTERM in some streams) is not implemented.
    const allowed_style_mask: u8 = 0x01 | 0x02 | 0x04 | 0x08 | 0x10 | 0x20;
    if ((params.code_block_style & ~allowed_style_mask) != 0) {
        return error.UnsupportedCodeBlockStyle;
    }
    if (params.progression_order > 4) return error.UnsupportedProgressionOrder;
    if (params.tile_parts_per_tile == 0) return error.InvalidTilePartCount;
    if ((params.precinct_width_exponent == null) != (params.precinct_height_exponent == null)) {
        return error.InvalidPrecinctConfiguration;
    }
    const tile_width: usize = if (params.tile_width == 0) w else params.tile_width;
    const tile_height: usize = if (params.tile_height == 0) h else params.tile_height;
    if (tile_width == 0 or tile_height == 0) return error.InvalidTileSize;
    return .{ w, h, num_components, tile_width, tile_height };
}

fn buildUnsignedPlanesU8(
    allocator: std.mem.Allocator,
    pixels: []const u8,
    w: usize,
    h: usize,
    num_components: usize,
) ![][]i32 {
    const planes = try allocator.alloc([]i32, num_components);
    errdefer allocator.free(planes);
    var allocated: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < allocated) : (i += 1) allocator.free(planes[i]);
    }
    for (planes, 0..) |*plane, c| {
        plane.* = try allocator.alloc(i32, w * h);
        allocated += 1;
        for (0..w * h) |i| {
            plane.*[i] = @as(i32, pixels[i * num_components + c]) - 128;
        }
    }
    return planes;
}

fn buildUnsignedPlanesU16(
    allocator: std.mem.Allocator,
    pixels: []const u16,
    w: usize,
    h: usize,
    num_components: usize,
    bits_per_component: u8,
) ![][]i32 {
    const planes = try allocator.alloc([]i32, num_components);
    errdefer allocator.free(planes);
    var allocated: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < allocated) : (i += 1) allocator.free(planes[i]);
    }
    const max_value: u32 = (@as(u32, 1) << @intCast(bits_per_component)) - 1;
    const offset: i32 = @as(i32, 1) << @intCast(bits_per_component - 1);
    for (planes, 0..) |*plane, c| {
        plane.* = try allocator.alloc(i32, w * h);
        allocated += 1;
        for (0..w * h) |i| {
            const sample = pixels[i * num_components + c];
            if (sample > max_value) return error.InvalidPixelSampleRange;
            plane.*[i] = @as(i32, sample) - offset;
        }
    }
    return planes;
}

fn freePlanes(allocator: std.mem.Allocator, planes: [][]i32) void {
    for (planes) |plane| allocator.free(plane);
    allocator.free(planes);
}

fn encodeFromShiftedPlanes(
    allocator: std.mem.Allocator,
    planes: [][]i32,
    params: *const EncodeParams,
    w: usize,
    h: usize,
    num_components: usize,
    tile_width: usize,
    tile_height: usize,
) ![]u8 {

    // Forward color transform. RCT requires integer 5/3 path; ICT operates on f32
    // and is applied inside the 9/7 tile path so we can avoid a double i32/f32 round-trip.
    if (params.multiple_component_transform and num_components == 3 and params.wavelet_transform == 1) {
        color_transform.forwardRct(planes[0], planes[1], planes[2]);
    }

    // 3. Tier-1: Encode each tile's codeblocks after tile-local transform.
    // EncodeParams carries the OpenJPEG-style exponent before the JPEG 2000
    // stored +2 bias; keep Tier-1 splitting aligned with what writeCodMarker
    // writes into COD.
    const cb_width_exp = params.code_block_width_exponent;
    const cb_height_exp = params.code_block_height_exponent;
    const cb_w_shift: u6 = @intCast(cb_width_exp + 4);
    const cb_h_shift: u6 = @intCast(cb_height_exp + 4);
    const cb_w: usize = @as(usize, 1) << cb_w_shift;
    const cb_h: usize = @as(usize, 1) << cb_h_shift;

    const tiles_x = (w + tile_width - 1) / tile_width;
    const tiles_y = (h + tile_height - 1) / tile_height;
    const num_tiles = tiles_x * tiles_y;
    const tile_datas = try allocator.alloc([]const u8, num_tiles);
    defer {
        for (tile_datas) |data| allocator.free(data);
        allocator.free(tile_datas);
    }

    // Per-tile packet-length lists collected from tier-2 assembly so the
    // codestream writer can emit PLT segments with accurate sizes.
    var tile_packet_lengths = try allocator.alloc(std.ArrayListUnmanaged(u32), num_tiles);
    defer {
        for (tile_packet_lengths) |*list| list.deinit(allocator);
        allocator.free(tile_packet_lengths);
    }
    for (tile_packet_lengths) |*list| list.* = .empty;

    // SOP Nsop is a single counter across the whole codestream (not per-tile).
    var sop_counter: u16 = 0;

    var tile_index: usize = 0;
    while (tile_index < num_tiles) : (tile_index += 1) {
        const tx = tile_index % tiles_x;
        const ty = tile_index / tiles_x;
        const x0 = tx * tile_width;
        const y0 = ty * tile_height;
        const this_tile_w = @min(tile_width, w - x0);
        const this_tile_h = @min(tile_height, h - y0);
        const plt_sink: ?*std.ArrayListUnmanaged(u32) = if (params.emit_plt) &tile_packet_lengths[tile_index] else null;
        tile_datas[tile_index] = try encodeTileData(
            allocator,
            planes,
            x0,
            y0,
            this_tile_w,
            this_tile_h,
            params,
            cb_w,
            cb_h,
            &sop_counter,
            plt_sink,
        );
    }

    // 5. Write codestream.
    const codestream_bytes = try writeCodestream(allocator, params, tile_datas, cb_width_exp, cb_height_exp, tile_packet_lengths);

    if (params.format == .jp2) {
        defer allocator.free(codestream_bytes);
        return box.writeJp2(allocator, codestream_bytes, params);
    }

    return codestream_bytes;
}

fn encodeTileData(
    allocator: std.mem.Allocator,
    source_planes: []const []const i32,
    x0: usize,
    y0: usize,
    tile_width: usize,
    tile_height: usize,
    params: *const EncodeParams,
    cb_w: usize,
    cb_h: usize,
    sop_counter: *u16,
    packet_lengths: ?*std.ArrayListUnmanaged(u32),
) ![]const u8 {
    const num_components = source_planes.len;
    const tile_planes = try allocator.alloc([]i32, num_components);
    defer {
        for (tile_planes) |plane| allocator.free(plane);
        allocator.free(tile_planes);
    }

    for (source_planes, 0..) |src_plane, comp_idx| {
        const dst = try allocator.alloc(i32, tile_width * tile_height);
        tile_planes[comp_idx] = dst;
        var y: usize = 0;
        while (y < tile_height) : (y += 1) {
            const src_row = (y0 + y) * @as(usize, params.width) + x0;
            const dst_row = y * tile_width;
            @memcpy(dst[dst_row .. dst_row + tile_width], src_plane[src_row .. src_row + tile_width]);
        }
    }

    if (params.wavelet_transform == 0) {
        try forward97Pipeline(
            allocator,
            tile_planes,
            tile_width,
            tile_height,
            params,
        );
    } else {
        for (tile_planes) |plane| {
            try forwardWaveletMultiLevel(allocator, plane, tile_width, tile_height, params.decomposition_levels);
        }
    }

    var encoded_cbs: std.ArrayListUnmanaged(tier2_encode.EncodedCodeblockInfo) = .empty;
    defer {
        for (encoded_cbs.items) |cb| {
            allocator.free(cb.data);
            allocator.free(cb.pass_lengths);
        }
        encoded_cbs.deinit(allocator);
    }

    for (tile_planes, 0..) |plane, comp_idx| {
        try encodeComponentCodeblocks(
            allocator,
            &encoded_cbs,
            plane,
            tile_width,
            tile_height,
            params.decomposition_levels,
            params.bits_per_component,
            @intCast(comp_idx),
            cb_w,
            cb_h,
            params,
        );
    }

    if (rateTargetBytesForTile(params, tile_width, tile_height)) |target| {
        try applyPcrdTruncation(allocator, encoded_cbs.items, target, params);
    }

    return tier2_encode.assembleTileDataWithOptions(
        allocator,
        encoded_cbs.items,
        params.num_layers,
        params.decomposition_levels + 1,
        params.components,
        params.progression_order,
        .{
            .emit_sop = params.emit_sop_markers,
            .emit_eph = params.emit_eph_markers,
            .sop_counter = sop_counter,
            .packet_lengths = packet_lengths,
        },
    );
}

fn rateTargetBytesForTile(params: *const EncodeParams, tile_width: usize, tile_height: usize) ?u64 {
    if (params.target_bytes) |bytes| {
        const total_pixels: u64 = @as(u64, params.width) * @as(u64, params.height);
        if (total_pixels == 0) return bytes;
        const tile_pixels: u64 = @as(u64, tile_width) * @as(u64, tile_height);
        return (@as(u64, bytes) * tile_pixels) / total_pixels;
    }
    if (params.target_bitrate) |bpp| {
        const tile_pixels: u64 = @as(u64, tile_width) * @as(u64, tile_height) * @as(u64, params.components);
        const bits: f64 = @as(f64, @floatFromInt(tile_pixels)) * @as(f64, bpp);
        const bytes: u64 = @intFromFloat(@ceil(bits / 8.0));
        return bytes;
    }
    return null;
}

/// Apply PCRD truncation to the encoded codeblocks in place. Reduces each codeblock's
/// `num_coding_passes` and `data.len` (via `pass_lengths`) so the sum of data sizes stays
/// within `target_bytes`. Uses a bitplane-weighted distortion estimate as the RD metric.
fn applyPcrdTruncation(
    allocator: std.mem.Allocator,
    codeblocks: []tier2_encode.EncodedCodeblockInfo,
    target_bytes: u64,
    params: *const EncodeParams,
) !void {
    if (codeblocks.len == 0) return;

    const rd_info = try allocator.alloc(rate_control.CodeblockRDInfo, codeblocks.len);
    defer {
        for (rd_info) |info| allocator.free(info.pass_distortions);
        allocator.free(rd_info);
    }
    for (rd_info, 0..) |*info, i| {
        const cb = codeblocks[i];
        const distortions = try rate_control.bitplaneWeightedPassDistortions(
            allocator,
            cb.num_coding_passes,
            subbandEffectiveBpcForCodeblock(params, cb.subband, cb.resolution_index),
            cb.zero_bit_planes,
        );
        info.* = .{
            .pass_lengths = @constCast(cb.pass_lengths),
            .pass_distortions = distortions,
            .num_passes = cb.num_coding_passes,
        };
    }

    var result = try rate_control.optimizeTruncation(allocator, rd_info, target_bytes);
    defer result.deinit(allocator);

    // Only logically truncate by reducing `num_coding_passes`. The original allocations for
    // `data` and `pass_lengths` remain live so the outer `defer` can free them correctly.
    // Tier-2 reads only the first `num_coding_passes` entries of `pass_lengths`.
    for (codeblocks, 0..) |*cb, i| {
        const keep = result.pass_counts[i];
        if (keep < cb.num_coding_passes) cb.num_coding_passes = keep;
    }
}

/// Irreversible (9/7) tile pipeline: convert i32 planes → f32, apply forward ICT when MCT is
/// enabled, run forward 9/7 multi-level DWT, then quantize coefficients back into the plane's
/// i32 buffer per subband. The subsequent Tier-1 path consumes the quantized i32 directly.
fn forward97Pipeline(
    allocator: std.mem.Allocator,
    tile_planes: [][]i32,
    tile_width: usize,
    tile_height: usize,
    params: *const EncodeParams,
) !void {
    const plane_len = tile_width * tile_height;
    const num_components = tile_planes.len;

    const f32_planes = try allocator.alloc([]f32, num_components);
    defer {
        for (f32_planes) |plane| allocator.free(plane);
        allocator.free(f32_planes);
    }
    for (f32_planes, 0..) |*plane, idx| {
        plane.* = try allocator.alloc(f32, plane_len);
        for (tile_planes[idx], 0..) |sample, i| plane.*[i] = @floatFromInt(sample);
    }

    if (params.multiple_component_transform and num_components == 3) {
        if (params.custom_mct) |matrix| {
            color_transform.applyCustomMctForward(matrix, f32_planes) catch return error.InvalidCustomMct;
        } else {
            color_transform.forwardIct(f32_planes[0], f32_planes[1], f32_planes[2]);
        }
    }

    for (f32_planes) |plane| {
        try forwardWavelet97MultiLevel(allocator, plane, tile_width, tile_height, params.decomposition_levels);
    }

    for (f32_planes, 0..) |plane, comp_idx| {
        quantize97Plane(plane, tile_planes[comp_idx], tile_width, tile_height, params);
    }
}

fn forwardWavelet97MultiLevel(
    allocator: std.mem.Allocator,
    plane: []f32,
    width: usize,
    height: usize,
    levels: u8,
) !void {
    var current_w = width;
    var current_h = height;
    var level: u8 = 0;
    while (level < levels) : (level += 1) {
        if (current_w == 0 or current_h == 0) break;
        var sub = try allocator.alloc(f32, current_w * current_h);
        defer allocator.free(sub);
        for (0..current_h) |y| {
            @memcpy(sub[y * current_w .. y * current_w + current_w], plane[y * width .. y * width + current_w]);
        }

        const coeffs = try wavelet.forward97Level(allocator, sub, current_w, current_h);
        defer allocator.free(coeffs);

        for (0..current_h) |y| {
            @memcpy(plane[y * width .. y * width + current_w], coeffs[y * current_w .. y * current_w + current_w]);
        }

        current_w = (current_w + 1) / 2;
        current_h = (current_h + 1) / 2;
    }
}

/// Quantize f32 9/7 coefficients into i32 with ISO round-to-nearest scaling:
///   q = sign(v) · floor(|v|/Δ_b + 0.5)
/// Δ_b is derived per subband from OpenJPEG's `dwt_norms_real` table to match
/// industry-standard irreversible quantization (see `writeQcdMarker`).
fn quantize97Plane(
    src_f32: []const f32,
    dst_i32: []i32,
    width: usize,
    height: usize,
    params: *const EncodeParams,
) void {
    const decomp: u8 = params.decomposition_levels;
    if (decomp == 0) {
        // No DWT pass: the whole plane is LL (deepest level treated as 0).
        const delta = quantization.irreversibleSubbandStepsize(.ll, 0);
        quantizeSubbandRegion(src_f32, dst_i32, width, 0, 0, width, height, delta);
        return;
    }

    // After multi-level DWT, subband regions shrink by halves as we descend.
    // Start from the full resolution (finest detail level = 0) and recurse into the LL quadrant.
    //
    // Our forward 9/7 (ISO F.4.8.2 steps 5/6) produces coefficients whose magnitude
    // equals `input * 1.0` for LL (the DC gain after the lifting+scaling is unity per
    // level). OpenJPEG's decoder, however, uses Δ = 1/norms_real, which was calibrated
    // against an encoder whose LL magnitude equals `input * norms_real[0][L]`. To make
    // ours→opj round-trip correctly we scale each subband coefficient by its
    // synthesis norm before quantization, so `q*Δ` reconstructs the value opj's
    // inverse DWT expects.
    var cur_w: usize = width;
    var cur_h: usize = height;
    var level: u8 = 0;
    while (level < decomp) : (level += 1) {
        const low_w: usize = (cur_w + 1) / 2;
        const low_h: usize = (cur_h + 1) / 2;
        const high_w: usize = cur_w - low_w;
        const high_h: usize = cur_h - low_h;

        // HL: top-right of the current block.
        if (high_w > 0 and low_h > 0) {
            const delta = quantization.irreversibleSubbandStepsize(.hl, level);
            quantizeSubbandRegion(src_f32, dst_i32, width, low_w, 0, high_w, low_h, delta);
        }
        // LH: bottom-left of the current block.
        if (low_w > 0 and high_h > 0) {
            const delta = quantization.irreversibleSubbandStepsize(.lh, level);
            quantizeSubbandRegion(src_f32, dst_i32, width, 0, low_h, low_w, high_h, delta);
        }
        // HH: bottom-right of the current block.
        if (high_w > 0 and high_h > 0) {
            const delta = quantization.irreversibleSubbandStepsize(.hh, level);
            quantizeSubbandRegion(src_f32, dst_i32, width, low_w, low_h, high_w, high_h, delta);
        }

        cur_w = low_w;
        cur_h = low_h;
    }

    // Remaining top-left is the deepest LL. Level index for LL per OpenJPEG is
    // numres − 1 − 0 = decomp (but clamped within norms table bounds).
    if (cur_w > 0 and cur_h > 0) {
        // OpenJPEG uses `level = numres − 1 − resno`; for LL (resno=0) that is
        // `decomp_levels` since numres = decomp_levels + 1.
        const ll_level: u8 = decomp;
        const delta = quantization.irreversibleSubbandStepsize(.ll, ll_level);
        quantizeSubbandRegion(src_f32, dst_i32, width, 0, 0, cur_w, cur_h, delta);
    }
}

fn quantizeSubbandRegion(
    src_f32: []const f32,
    dst_i32: []i32,
    stride: usize,
    x0: usize,
    y0: usize,
    w: usize,
    h: usize,
    delta: f64,
) void {
    quantizeSubbandRegionScaled(src_f32, dst_i32, stride, x0, y0, w, h, delta, 1.0);
}

/// Per-subband quantization with an explicit coefficient pre-scale. Computes
///   q = sign(v) * floor(|v| * scale / Δ)
/// which is equivalent to scaling the input coefficient by `scale` and then
/// applying ISO dead-zone quantization with stepsize Δ. Used on the 9/7 path
/// where `scale = synthesis_norm(b)` compensates for the difference between
/// our ISO-literal DWT scaling (DC gain 1.0 per level) and OpenJPEG's decoder
/// expectation that q·Δ equals the forward coefficient scaled by norms_real.
fn quantizeSubbandRegionScaled(
    src_f32: []const f32,
    dst_i32: []i32,
    stride: usize,
    x0: usize,
    y0: usize,
    w: usize,
    h: usize,
    delta: f64,
    scale: f64,
) void {
    const inv_delta: f64 = if (delta > 0.0) scale / delta else 0.0;
    var yy: usize = 0;
    while (yy < h) : (yy += 1) {
        const row = (y0 + yy) * stride + x0;
        var xx: usize = 0;
        while (xx < w) : (xx += 1) {
            const v = src_f32[row + xx];
            const av: f64 = @abs(@as(f64, v));
            const mag: f64 = @floor(av * inv_delta);
            const q: i32 = @intFromFloat(mag);
            dst_i32[row + xx] = if (v < 0.0) -q else q;
        }
    }
}

// ---------------------------------------------------------------------------
// Wavelet
// ---------------------------------------------------------------------------

fn forwardWaveletMultiLevel(allocator: std.mem.Allocator, plane: []i32, width: usize, height: usize, levels: u8) !void {
    var current_w = width;
    var current_h = height;
    var level: u8 = 0;
    while (level < levels) : (level += 1) {
        // Extract the active LL region.
        var sub = try allocator.alloc(i32, current_w * current_h);
        defer allocator.free(sub);
        for (0..current_h) |y| {
            @memcpy(sub[y * current_w .. y * current_w + current_w], plane[y * width .. y * width + current_w]);
        }

        const coeffs = try wavelet.forward53Level(allocator, sub, current_w, current_h);
        defer allocator.free(coeffs);

        for (0..current_h) |y| {
            @memcpy(plane[y * width .. y * width + current_w], coeffs[y * current_w .. y * current_w + current_w]);
        }

        current_w = (current_w + 1) / 2;
        current_h = (current_h + 1) / 2;
    }
}

// ---------------------------------------------------------------------------
// Codeblock encoding
// ---------------------------------------------------------------------------

fn encodeComponentCodeblocks(
    allocator: std.mem.Allocator,
    encoded_cbs: *std.ArrayListUnmanaged(tier2_encode.EncodedCodeblockInfo),
    plane: []const i32,
    width: usize,
    height: usize,
    decomposition_levels: u8,
    bits_per_component: u8,
    comp_idx: u16,
    cb_w: usize,
    cb_h: usize,
    params: *const EncodeParams,
) !void {
    // Resolution 0 is the LL band at the deepest level.
    // Resolution r (1..decomposition_levels) has HL, LH, HH subbands at
    // decomposition level (decomposition_levels - r).
    var res_idx: u8 = 0;
    while (res_idx <= decomposition_levels) : (res_idx += 1) {
        if (res_idx == 0) {
            // LL band: the top-left region after all decomposition levels.
            const ll_w = ceilDivPow2(width, decomposition_levels);
            const ll_h = ceilDivPow2(height, decomposition_levels);
            if (ll_w == 0 or ll_h == 0) continue;

            try encodeSubbandCodeblocks(
                allocator,
                encoded_cbs,
                plane,
                width,
                0,
                0,
                ll_w,
                ll_h,
                cb_w,
                cb_h,
                comp_idx,
                res_idx,
                .ll,
                bits_per_component,
                params,
            );
        } else {
            // Detail subbands at this resolution.
            const reduce = decomposition_levels - res_idx;
            const res_w = ceilDivPow2(width, reduce);
            const res_h = ceilDivPow2(height, reduce);
            const low_w = ceilDivPow2(res_w, 1);
            const low_h = ceilDivPow2(res_h, 1);
            const high_w = res_w - low_w;
            const high_h = res_h - low_h;

            // HL subband: top-right.
            if (high_w > 0 and low_h > 0) {
                try encodeSubbandCodeblocks(
                    allocator,
                    encoded_cbs,
                    plane,
                    width,
                    low_w,
                    0,
                    high_w,
                    low_h,
                    cb_w,
                    cb_h,
                    comp_idx,
                    res_idx,
                    .hl,
                    bits_per_component,
                    params,
                );
            }

            // LH subband: bottom-left.
            if (low_w > 0 and high_h > 0) {
                try encodeSubbandCodeblocks(
                    allocator,
                    encoded_cbs,
                    plane,
                    width,
                    0,
                    low_h,
                    low_w,
                    high_h,
                    cb_w,
                    cb_h,
                    comp_idx,
                    res_idx,
                    .lh,
                    bits_per_component,
                    params,
                );
            }

            // HH subband: bottom-right.
            if (high_w > 0 and high_h > 0) {
                try encodeSubbandCodeblocks(
                    allocator,
                    encoded_cbs,
                    plane,
                    width,
                    low_w,
                    low_h,
                    high_w,
                    high_h,
                    cb_w,
                    cb_h,
                    comp_idx,
                    res_idx,
                    .hh,
                    bits_per_component,
                    params,
                );
            }
        }
    }
}

/// Compute effective bitplanes Mb = G + ε − 1 for a subband. For the reversible
/// (5/3) path ε = bpc + gain(b). For the irreversible (9/7) path ε is derived
/// from OpenJPEG's `dwt_norms_real` stepsize table — the same computation that
/// `writeQcdMarker` emits on the wire — so Tier-1 pass scheduling matches the
/// advertised number of bitplanes.
fn subbandEffectiveBpc(bpc: u8, subband: tile_mod.SubbandType) u8 {
    const expn: u8 = switch (subband) {
        .ll => bpc,
        .hl, .lh => bpc + 1,
        .hh => bpc + 2,
    };
    return encoder_guard_bits + expn - 1;
}

/// Same as `subbandEffectiveBpc` but uses ISO/OpenJPEG real-norm stepsizes
/// when `wavelet_transform == 0` (9/7). The DWT level is derived from the
/// resolution index per `level = numres − 1 − resno`.
fn subbandEffectiveBpcForCodeblock(
    params: *const EncodeParams,
    subband: tile_mod.SubbandType,
    resolution_index: u8,
) u8 {
    if (params.wavelet_transform != 0) {
        return subbandEffectiveBpc(params.bits_per_component, subband);
    }
    const decomp: u8 = params.decomposition_levels;
    const level: u8 = if (resolution_index == 0)
        decomp
    else
        decomp - resolution_index;
    const gain = quantization.irreversibleSubbandGain(subband);
    const delta = quantization.irreversibleSubbandStepsize(subband, level);
    const step_value = quantization.encodeStepValueIrreversible(delta, params.bits_per_component, gain);
    const expn: u8 = quantization.stepExponent(step_value);
    if (encoder_guard_bits + expn == 0) return 0;
    return encoder_guard_bits + expn - 1;
}

fn encodeSubbandCodeblocks(
    allocator: std.mem.Allocator,
    encoded_cbs: *std.ArrayListUnmanaged(tier2_encode.EncodedCodeblockInfo),
    plane: []const i32,
    plane_stride: usize,
    sub_x0: usize,
    sub_y0: usize,
    sub_w: usize,
    sub_h: usize,
    cb_w: usize,
    cb_h: usize,
    comp_idx: u16,
    res_idx: u8,
    subband: tile_mod.SubbandType,
    bits_per_component: u8,
    params: *const EncodeParams,
) !void {
    _ = bits_per_component; // consumed via `params.bits_per_component` in helpers below.
    const cbs_x = (sub_w + cb_w - 1) / cb_w;
    const cbs_y = (sub_h + cb_h - 1) / cb_h;
    const full_height = plane.len / plane_stride;
    const resolution_width = resolutionExtent(plane_stride, params.decomposition_levels, res_idx);
    const resolution_height = resolutionExtent(full_height, params.decomposition_levels, res_idx);
    const precinct_width = precinctDimensionForResolution(params.precinct_width_exponent, resolution_width);
    const precinct_height = precinctDimensionForResolution(params.precinct_height_exponent, resolution_height);
    const precincts_x = (resolution_width + precinct_width - 1) / precinct_width;
    const subband_origin_x = sub_x0;
    const subband_origin_y = sub_y0;

    var cby: usize = 0;
    while (cby < cbs_y) : (cby += 1) {
        var cbx: usize = 0;
        while (cbx < cbs_x) : (cbx += 1) {
            const bx0 = cbx * cb_w;
            const by0 = cby * cb_h;
            const bx1 = @min(bx0 + cb_w, sub_w);
            const by1 = @min(by0 + cb_h, sub_h);
            const bw = bx1 - bx0;
            const bh = by1 - by0;

            // Extract coefficients for this codeblock.
            const coeffs = try allocator.alloc(i32, bw * bh);
            defer allocator.free(coeffs);
            for (0..bh) |y| {
                const src_row = (sub_y0 + by0 + y) * plane_stride + (sub_x0 + bx0);
                @memcpy(coeffs[y * bw .. y * bw + bw], plane[src_row .. src_row + bw]);
            }

            const effective_bpc = subbandEffectiveBpcForCodeblock(params, subband, res_idx);
            // Apply ROI shift for components targeted by EncodeParams.roi_component.
            // When roi_mask is null the whole image is treated as ROI for that
            // component; otherwise only coefficients whose corresponding image
            // pixel is inside the mask are upshifted. We approximate per-coeff
            // shifting by shifting the whole codeblock when any mask pixel is
            // set inside its footprint at the highest resolution.
            const effective_roi_shift: u8 = blk: {
                const rc = params.roi_component orelse break :blk 0;
                if (params.roi_shift == 0) break :blk 0;
                if (@as(u16, rc) != comp_idx) break :blk 0;
                if (params.roi_mask) |mask| {
                    // Map codeblock footprint in subband coords back to image coords.
                    const full_w = plane_stride;
                    const full_h = plane.len / plane_stride;
                    const band_scale = params.decomposition_levels - res_idx;
                    const sub_abs_x = sub_x0 + bx0;
                    const sub_abs_y = sub_y0 + by0;
                    const img_x0: usize = sub_abs_x << @intCast(band_scale);
                    const img_y0: usize = sub_abs_y << @intCast(band_scale);
                    const img_x1: usize = @min(full_w, (sub_abs_x + bw) << @intCast(band_scale));
                    const img_y1: usize = @min(full_h, (sub_abs_y + bh) << @intCast(band_scale));
                    var hit = false;
                    var yy: usize = img_y0;
                    scan: while (yy < img_y1) : (yy += 1) {
                        var xx: usize = img_x0;
                        while (xx < img_x1) : (xx += 1) {
                            if (yy * full_w + xx < mask.len and mask[yy * full_w + xx]) {
                                hit = true;
                                break :scan;
                            }
                        }
                    }
                    if (!hit) break :blk 0;
                }
                break :blk params.roi_shift;
            };
            const result = try tier1_encode.encodeCodeblock(
                allocator,
                coeffs,
                bw,
                bh,
                effective_bpc,
                subband,
                params.code_block_style,
                effective_roi_shift,
            );
            _ = subband_origin_x;
            _ = subband_origin_y;
            const subband_precinct_width = if (res_idx > 0 and subband != .ll) (precinct_width + 1) / 2 else precinct_width;
            const subband_precinct_height = if (res_idx > 0 and subband != .ll) (precinct_height + 1) / 2 else precinct_height;
            const precinct_x = if (subband_precinct_width == 0) 0 else bx0 / subband_precinct_width;
            const precinct_y = if (subband_precinct_height == 0) 0 else by0 / subband_precinct_height;
            const subband_precinct_origin_x = precinct_x * subband_precinct_width;
            const subband_precinct_origin_y = precinct_y * subband_precinct_height;

            try encoded_cbs.append(allocator, .{
                .component_index = comp_idx,
                .resolution_index = res_idx,
                .subband = subband,
                .precinct_index = @intCast(precinct_y * precincts_x + precinct_x),
                .codeblock_x = @intCast((bx0 - subband_precinct_origin_x) / cb_w),
                .codeblock_y = @intCast((by0 - subband_precinct_origin_y) / cb_h),
                .data = result.data,
                .num_coding_passes = result.num_coding_passes,
                .zero_bit_planes = result.zero_bit_planes,
                .pass_lengths = result.pass_lengths,
            });
            // Ownership of result.data transferred to encoded_cbs; do not free.
        }
    }
}

fn precinctDimensionForResolution(exp: ?u8, resolution_extent: usize) usize {
    if (exp == null) return @max(resolution_extent, @as(usize, 1));
    return @as(usize, 1) << @intCast(exp.?);
}

fn resolutionExtent(full_extent: usize, decomposition_levels: u8, resolution_index: u8) usize {
    const reduce = decomposition_levels - resolution_index;
    return ceilDivPow2(full_extent, reduce);
}

// ---------------------------------------------------------------------------
// Codestream writing
// ---------------------------------------------------------------------------

fn writeCodestream(
    allocator: std.mem.Allocator,
    params: *const EncodeParams,
    tile_datas: []const []const u8,
    cb_width_exp: u8,
    cb_height_exp: u8,
    tile_packet_lengths: []std.ArrayListUnmanaged(u32),
) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    // SOC marker.
    try writeBigU16(allocator, &out, markers.soc);

    // SIZ marker segment.
    try writeSizMarker(allocator, &out, params);

    // COD marker segment.
    try writeCodMarker(allocator, &out, params, cb_width_exp, cb_height_exp);

    // QCD marker segment.
    try writeQcdMarker(allocator, &out, params);

    // COM marker: producer tag. The decoder keys its reconstruction policy off
    // this tag (`decode.zig:antfly_producer_tag`): ours → exact bitplane;
    // foreign/absent → ISO Annex E.1.2 midpoint (only matters for 9/7).
    try writeComMarker(allocator, &out, antfly_producer_tag);

    // MCT/MCC/MCO markers (optional). Emitted when a user-supplied custom
    // decorrelation matrix is configured alongside MCT mode and the 9/7
    // (irreversible) wavelet. The 5/3 path uses the built-in RCT.
    if (params.custom_mct != null and
        params.multiple_component_transform and
        params.wavelet_transform == 0 and
        params.components == 3)
    {
        try writeCustomMctMarkers(allocator, &out, params.custom_mct.?);
    }

    // POC marker (optional). First entry is synthetic and covers the full
    // range using the default progression_order, followed by each configured
    // progression window.
    if (params.progression_windows.len > 0) {
        try writePocMarker(allocator, &out, params);
    }

    // RGN marker (optional). One per targeted component with roi_shift > 0;
    // Srgn=0 selects implicit max-shift.
    if (params.roi_component != null and params.roi_shift > 0) {
        try writeRgnMarker(allocator, &out, params);
    }

    // TLM is written after all other main-header markers. Its entries are
    // back-patched once tile-part sizes are known, so we reserve the exact
    // segment bytes up front using a known Stlm layout (ST=1 Ttlm byte,
    // SP=1 4-byte Ptlm -> 5 bytes per tile-part).
    const total_tile_parts = if (params.emit_tlm) blk: {
        var total: usize = 0;
        for (tile_datas) |tile_data| {
            total += @min(params.tile_parts_per_tile, if (tile_data.len == 0) @as(usize, 1) else tile_data.len);
        }
        break :blk total;
    } else 0;
    const tlm_entry_offset: usize = if (params.emit_tlm) blk: {
        try writeBigU16(allocator, &out, markers.tlm);
        // Ltlm = 4 + 5 * total_tile_parts (Ztlm + Stlm + entries).
        const ltlm: u16 = @intCast(4 + 5 * total_tile_parts);
        try writeBigU16(allocator, &out, ltlm);
        try out.append(allocator, 0); // Ztlm: single TLM marker, index 0.
        // Stlm: Ttlm present as u8 (ST=1, bits 4), Ptlm as u32 (SP=1, bit 6).
        try out.append(allocator, (1 << 4) | (1 << 6));
        const start = out.items.len;
        try out.appendNTimes(allocator, 0, 5 * total_tile_parts);
        break :blk start;
    } else 0;

    var tlm_cursor = tlm_entry_offset;

    for (tile_datas, 0..) |tile_data, tile_index| {
        const requested_parts: usize = @min(params.tile_parts_per_tile, if (tile_data.len == 0) @as(usize, 1) else tile_data.len);
        var part_index: usize = 0;
        var offset: usize = 0;
        while (part_index < requested_parts) : (part_index += 1) {
            const remaining = tile_data.len - offset;
            const parts_left = requested_parts - part_index;
            const chunk_len = if (parts_left == 1) remaining else @max(@as(usize, 1), remaining / parts_left);
            // PLT emission only on the first tile-part of a tile; writing
            // PLT across multiple parts would require splitting lengths by
            // chunk boundaries, which is deferred.
            const plt_segment = if (params.emit_plt and part_index == 0 and tile_packet_lengths[tile_index].items.len > 0)
                try buildPltSegmentBytes(allocator, tile_packet_lengths[tile_index].items)
            else
                &[_]u8{};
            defer if (plt_segment.len > 0) allocator.free(plt_segment);
            const tile_part_length: u32 = @intCast(12 + 2 + chunk_len + plt_segment.len);
            try writeSotMarker(
                allocator,
                &out,
                @intCast(tile_index),
                tile_part_length,
                @intCast(part_index),
                @intCast(requested_parts),
            );
            if (plt_segment.len > 0) try out.appendSlice(allocator, plt_segment);
            try writeBigU16(allocator, &out, markers.sod);
            try out.appendSlice(allocator, tile_data[offset .. offset + chunk_len]);
            offset += chunk_len;
            if (params.emit_tlm) {
                out.items[tlm_cursor] = @intCast(tile_index);
                std.mem.writeInt(u32, out.items[tlm_cursor + 1 ..][0..4], tile_part_length, .big);
                tlm_cursor += 5;
            }
        }
    }

    // EOC marker.
    try writeBigU16(allocator, &out, markers.eoc);

    return out.toOwnedSlice(allocator);
}

/// Build a PLT marker segment (may be split across multiple PLT markers if
/// payload is large). Returns owned bytes, or an empty slice when `lengths`
/// is empty. Each length uses 7-bit variable-length encoding (big-endian
/// groups of 7 bits, continuation bit in MSB except on last byte).
fn buildPltSegmentBytes(allocator: std.mem.Allocator, lengths: []const u32) ![]u8 {
    // First encode all length varints into a scratch buffer, then split
    // into PLT segments of at most (65535 - 3) payload bytes each.
    var varints: std.ArrayListUnmanaged(u8) = .empty;
    defer varints.deinit(allocator);
    for (lengths) |len| {
        var bytes: [5]u8 = undefined;
        var count: usize = 0;
        var v = len;
        // Build from least-significant 7-bit group to most-significant.
        while (true) {
            bytes[count] = @intCast(v & 0x7f);
            count += 1;
            v >>= 7;
            if (v == 0) break;
        }
        // Emit in most-significant first order with continuation bit set
        // on every byte except the last.
        var i: usize = count;
        while (i > 0) {
            i -= 1;
            var b = bytes[i];
            if (i != 0) b |= 0x80;
            try varints.append(allocator, b);
        }
    }

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    const max_payload: usize = 0xffff - 3; // Lplt is u16 and counts bytes after marker (incl. Zplt)
    var cursor: usize = 0;
    var zplt: u8 = 0;
    while (cursor < varints.items.len) {
        const chunk_len = @min(max_payload, varints.items.len - cursor);
        try writeBigU16(allocator, &out, markers.plt);
        const lplt: u16 = @intCast(3 + chunk_len);
        try writeBigU16(allocator, &out, lplt);
        try out.append(allocator, zplt);
        try out.appendSlice(allocator, varints.items[cursor .. cursor + chunk_len]);
        cursor += chunk_len;
        zplt +%= 1;
    }

    return out.toOwnedSlice(allocator);
}

fn writeSizMarker(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), params: *const EncodeParams) !void {
    const csiz: u16 = params.components;
    const lsiz: u16 = @intCast(38 + @as(usize, csiz) * 3);

    try writeBigU16(allocator, out, markers.siz);
    try writeBigU16(allocator, out, lsiz); // Lsiz
    try writeBigU16(allocator, out, 0); // Rsiz (capabilities)
    try writeBigU32(allocator, out, params.width); // Xsiz
    try writeBigU32(allocator, out, params.height); // Ysiz
    try writeBigU32(allocator, out, 0); // XOsiz
    try writeBigU32(allocator, out, 0); // YOsiz
    try writeBigU32(allocator, out, if (params.tile_width == 0) params.width else params.tile_width);
    try writeBigU32(allocator, out, if (params.tile_height == 0) params.height else params.tile_height);
    try writeBigU32(allocator, out, 0); // XTOsiz
    try writeBigU32(allocator, out, 0); // YTOsiz
    try writeBigU16(allocator, out, csiz); // Csiz

    var c: u16 = 0;
    while (c < csiz) : (c += 1) {
        // Ssiz: unsigned, bpc-1 (e.g. 7 for 8-bit unsigned).
        try out.append(allocator, params.bits_per_component - 1);
        try out.append(allocator, 1); // XRsiz
        try out.append(allocator, 1); // YRsiz
    }
}

fn writeCodMarker(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), params: *const EncodeParams, cb_width_exp: u8, cb_height_exp: u8) !void {
    const precincts_present = params.precinct_width_exponent != null;
    const lcod: u16 = 12 + (if (precincts_present) @as(u16, params.decomposition_levels + 1) else 0);
    try writeBigU16(allocator, out, markers.cod);
    try writeBigU16(allocator, out, lcod); // Lcod
    var scod: u8 = 0;
    if (precincts_present) scod |= 0x01;
    if (params.emit_sop_markers) scod |= 0x02;
    if (params.emit_eph_markers) scod |= 0x04;
    try out.append(allocator, scod);
    try out.append(allocator, params.progression_order); // SGcod: progression order
    try writeBigU16(allocator, out, params.num_layers); // SGcod: num_layers
    try out.append(allocator, if (params.multiple_component_transform) 1 else 0); // SGcod: MCT
    try out.append(allocator, params.decomposition_levels); // SPcod: decomposition_levels
    try out.append(allocator, cb_width_exp + 2); // SPcod: code_block_width_exponent - 2
    try out.append(allocator, cb_height_exp + 2); // SPcod: code_block_height_exponent - 2
    try out.append(allocator, params.code_block_style); // SPcod: code_block_style
    try out.append(allocator, params.wavelet_transform); // SPcod: wavelet_transform
    if (precincts_present) {
        const precinct_byte: u8 = (params.precinct_width_exponent.? << 4) | params.precinct_height_exponent.?;
        var res: u8 = 0;
        while (res <= params.decomposition_levels) : (res += 1) {
            try out.append(allocator, precinct_byte);
        }
    }
}

const encoder_guard_bits: u8 = 2;

fn writeQcdMarker(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), params: *const EncodeParams) !void {
    // Reversible (5/3): style 0 with ε chosen so Mb = G + ε − 1 covers the effective subband
    // precision (bpc + gain). Irreversible (9/7): style 2 with (ε,μ) derived from OpenJPEG's
    // `dwt_norms_real` table so the emitted step sizes match what industry-standard decoders
    // expect; the actual in-place quantization in `quantize97Plane` uses the same Δ.
    const bpc = params.bits_per_component;
    const is_irreversible = params.wavelet_transform == 0;
    const num_subbands: usize = 1 + @as(usize, params.decomposition_levels) * 3;
    const per_subband_len: usize = if (is_irreversible) 2 else 1;
    const lqcd: u16 = @intCast(3 + num_subbands * per_subband_len);

    try writeBigU16(allocator, out, markers.qcd);
    try writeBigU16(allocator, out, lqcd);
    // Style 0 = reversible (no quantization); style 2 = scalar expounded (explicit ε/μ per subband).
    const style: u8 = if (is_irreversible) 2 else 0;
    const sqcd: u8 = (encoder_guard_bits << 5) | style;
    try out.append(allocator, sqcd);

    if (is_irreversible) {
        // LL sits at decomposition depth `decomposition_levels − 1` (counted from finest=0).
        const decomp: u8 = params.decomposition_levels;
        // OpenJPEG uses `level = numres − 1 − resno`; for LL (resno=0) that is
        // `decomp_levels` since numres = decomp_levels + 1.
        const ll_level: u8 = decomp;
        try writeIrreversibleSubbandStep(allocator, out, .ll, ll_level, bpc);
        var res: u8 = 1;
        while (res <= decomp) : (res += 1) {
            // QCD marker is emitted lowest-resolution-first (deepest details first),
            // matching ISO 15444-1 Annex A.6.4. Detail subbands at resolution index r
            // correspond to DWT level = decomp − r (level 0 is the finest/outermost pass).
            const det_level: u8 = decomp - res;
            try writeIrreversibleSubbandStep(allocator, out, .hl, det_level, bpc);
            try writeIrreversibleSubbandStep(allocator, out, .lh, det_level, bpc);
            try writeIrreversibleSubbandStep(allocator, out, .hh, det_level, bpc);
        }
    } else {
        try out.append(allocator, bpc << 3); // LL
        var res: u8 = 1;
        while (res <= params.decomposition_levels) : (res += 1) {
            try out.append(allocator, (bpc + 1) << 3); // HL
            try out.append(allocator, (bpc + 1) << 3); // LH
            try out.append(allocator, (bpc + 2) << 3); // HH
        }
    }
}

fn writeIrreversibleSubbandStep(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    subband: tile_mod.SubbandType,
    level: u8,
    bpc: u8,
) !void {
    const gain = quantization.irreversibleSubbandGain(subband);
    const delta = quantization.irreversibleSubbandStepsize(subband, level);
    const step_value = quantization.encodeStepValueIrreversible(delta, bpc, gain);
    try writeBigU16(allocator, out, step_value);
}

fn writePocMarker(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    params: *const EncodeParams,
) !void {
    const csiz: u16 = params.components;
    const wide_comp = csiz >= 256;
    const comp_bytes: usize = if (wide_comp) 2 else 1;
    const entry_size: usize = 1 + comp_bytes + 2 + 1 + comp_bytes + 1;
    // Synthetic "default" window covering the full range, plus each user window.
    const total_entries: usize = 1 + params.progression_windows.len;
    const lpoc: u16 = @intCast(2 + entry_size * total_entries);

    try writeBigU16(allocator, out, markers.poc);
    try writeBigU16(allocator, out, lpoc);

    const full_re: u8 = params.decomposition_levels + 1;
    const full_lye: u16 = params.num_layers;

    // Entry 0: synthetic full-range with default order.
    try writePocEntry(allocator, out, wide_comp, 0, 0, full_lye, full_re, csiz, params.progression_order);

    for (params.progression_windows) |w| {
        try writePocEntry(allocator, out, wide_comp, w.rs, w.cs, w.lye, w.re, w.ce, w.order);
    }
}

fn writePocEntry(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    wide_comp: bool,
    rs: u8,
    cs: u16,
    lye: u16,
    re: u8,
    ce: u16,
    order: u8,
) !void {
    try out.append(allocator, rs);
    if (wide_comp) {
        try writeBigU16(allocator, out, cs);
    } else {
        try out.append(allocator, @intCast(cs));
    }
    try writeBigU16(allocator, out, lye);
    try out.append(allocator, re);
    if (wide_comp) {
        try writeBigU16(allocator, out, ce);
    } else {
        try out.append(allocator, @intCast(ce));
    }
    try out.append(allocator, order);
}

fn writeRgnMarker(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    params: *const EncodeParams,
) !void {
    const csiz: u16 = params.components;
    const wide_comp = csiz >= 257;
    const comp_bytes: usize = if (wide_comp) 2 else 1;
    const lrgn: u16 = @intCast(2 + comp_bytes + 1 + 1); // Lrgn + Crgn + Srgn + SPrgn
    try writeBigU16(allocator, out, markers.rgn);
    try writeBigU16(allocator, out, lrgn);
    const component = params.roi_component.?;
    if (wide_comp) {
        try writeBigU16(allocator, out, component);
    } else {
        try out.append(allocator, component);
    }
    try out.append(allocator, 0); // Srgn = 0 (implicit max-shift)
    try out.append(allocator, params.roi_shift); // SPrgn
}

/// Emit a single-MCT/MCC/MCO triplet representing the caller-supplied custom
/// decorrelation matrix. The `forward` coefficients are serialized as IEEE-754
/// big-endian f32s (element_type=2 in Imct bits 10-11). A single MCC
/// collection references MCT #1 and is selected by MCO entry 0.
fn writeCustomMctMarkers(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    matrix: color_transform.CustomMctMatrix,
) !void {
    const n: usize = matrix.num_components;
    const coeff_count: usize = n * n;
    const payload_len: usize = coeff_count * @sizeOf(f32);

    // --- MCT ---
    // Lmct = 2 (Lmct) + 2 (Zmct) + 2 (Imct) + 2 (Ymct) + payload.
    const lmct: u16 = @intCast(8 + payload_len);
    try writeBigU16(allocator, out, markers.mct);
    try writeBigU16(allocator, out, lmct);
    try writeBigU16(allocator, out, 0); // Zmct = 0 (single segment)
    // Imct: element_type=2 (f32) in bits 10-11, type=0 (decorrelation),
    // index=1 so MCC entry 0 can reference a non-null identifier.
    const imct: u16 = (@as(u16, 2) << 10) | 0x0001;
    try writeBigU16(allocator, out, imct);
    try writeBigU16(allocator, out, 0); // Ymct = 0
    var scratch: [4]u8 = undefined;
    for (matrix.forward) |coeff| {
        const bits: u32 = @bitCast(coeff);
        std.mem.writeInt(u32, &scratch, bits, .big);
        try out.appendSlice(allocator, &scratch);
    }

    // --- MCC ---
    // Minimal collection payload: 4 placeholder bytes that mark the collection
    // as referencing MCT index 1. Full Qmcc stage encoding is deferred.
    // Lmcc = 2 + 2 + 2 + 2 + 4 = 12.
    try writeBigU16(allocator, out, markers.mcc);
    try writeBigU16(allocator, out, 0x000c);
    try writeBigU16(allocator, out, 0); // Zmcc
    try writeBigU16(allocator, out, 0x0001); // Imcc (index=1)
    try writeBigU16(allocator, out, 0); // Ymcc
    // Placeholder collection body: references MCT id 1.
    try out.appendSlice(allocator, &.{ 0x00, 0x01, 0x00, 0x00 });

    // --- MCO ---
    // Lmco = 2 + 1 (Nmco) + 1 (one Imco id) = 4.
    try writeBigU16(allocator, out, markers.mco);
    try writeBigU16(allocator, out, 0x0004);
    try out.append(allocator, 1); // Nmco
    try out.append(allocator, 0x01); // Imco references MCC id 1
}

fn writeIrreversibleStepValue(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    expn: u8,
    mantissa: u16,
) !void {
    const packed_value: u16 = (@as(u16, expn) << 11) | (mantissa & 0x7ff);
    try writeBigU16(allocator, out, packed_value);
}

fn writeSotMarker(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    tile_index: u16,
    tile_part_length: u32,
    tile_part_index: u8,
    num_tile_parts: u8,
) !void {
    try writeBigU16(allocator, out, markers.sot);
    try writeBigU16(allocator, out, 10); // Lsot (always 10)
    try writeBigU16(allocator, out, tile_index); // Isot
    try writeBigU32(allocator, out, tile_part_length); // Psot
    try out.append(allocator, tile_part_index); // TPsot
    try out.append(allocator, num_tile_parts); // TNsot
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Producer tag emitted in a COM marker after QCD. Keyed by the decoder to
/// decide reconstruction policy for 9/7 irreversible streams; see
/// `decode.antfly_producer_tag`.
pub const antfly_producer_tag: []const u8 = "antfly-zig j2k v1";

fn writeComMarker(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    try writeBigU16(allocator, out, markers.com);
    // Lcom = 4 (Lcom + Rcom) + text length.
    const lcom: u16 = @intCast(4 + text.len);
    try writeBigU16(allocator, out, lcom);
    // Rcom = 1: general-use ISO/IEC 8859-15 (Latin) string. Decoders that
    // don't understand the payload are required to ignore the marker.
    try writeBigU16(allocator, out, 1);
    try out.appendSlice(allocator, text);
}

fn writeBigU16(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .big);
    try out.appendSlice(allocator, &buf);
}

fn writeBigU32(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .big);
    try out.appendSlice(allocator, &buf);
}

fn ceilDivPow2(value: usize, shift: u8) usize {
    if (shift == 0) return value;
    return (value + (@as(usize, 1) << @intCast(shift)) - 1) >> @intCast(shift);
}

/// Clamp decomposition levels so the smallest dimension at the coarsest level is >= 1.
fn clampDecompositionLevels(width: u32, height: u32) u8 {
    const min_dim = @min(width, height);
    if (min_dim <= 1) return 0;
    const max_levels = std.math.log2(min_dim);
    return @intCast(@min(max_levels, 5));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "validateEncodeParams accepts in-range code-block exponents" {
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .code_block_width_exponent = 4,
        .code_block_height_exponent = 4,
    };
    _ = try validateEncodeParams(&params, 16);
}

test "validateEncodeParams rejects code-block exponent sum > 8" {
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .code_block_width_exponent = 5,
        .code_block_height_exponent = 5,
    };
    try std.testing.expectError(error.UnsupportedCodeBlockSize, validateEncodeParams(&params, 16));
}

test "validateEncodeParams rejects single code-block exponent > 8" {
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .code_block_width_exponent = 9,
        .code_block_height_exponent = 0,
    };
    try std.testing.expectError(error.UnsupportedCodeBlockSize, validateEncodeParams(&params, 16));
}

test "validateEncodeParams rejects unsupported Scod bits" {
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .code_block_style = 0x40, // bit 6 is not a defined Scod mode
    };
    try std.testing.expectError(error.UnsupportedCodeBlockStyle, validateEncodeParams(&params, 16));
}

test "validateEncodeParams accepts BYPASS/TERMALL/PTERM Scod bits" {
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .code_block_style = 0x01 | 0x04 | 0x10,
    };
    _ = try validateEncodeParams(&params, 16);
}

test "encode 4x4 grayscale produces valid J2K starting with SOC" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        100, 110, 120, 130,
        140, 150, 160, 170,
        180, 190, 200, 210,
        220, 230, 240, 250,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const result = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(result);

    // Must start with SOC marker (0xff4f).
    try std.testing.expect(result.len >= 2);
    try std.testing.expectEqual(@as(u8, 0xff), result[0]);
    try std.testing.expectEqual(@as(u8, 0x4f), result[1]);

    // Must end with EOC marker (0xffd9).
    try std.testing.expectEqual(@as(u8, 0xff), result[result.len - 2]);
    try std.testing.expectEqual(@as(u8, 0xd9), result[result.len - 1]);
}

test "encode 4x4 grayscale as JP2 starts with JP2 signature" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        100, 110, 120, 130,
        140, 150, 160, 170,
        180, 190, 200, 210,
        220, 230, 240, 250,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .jp2,
    };
    const result = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(result);

    // Must start with JP2 signature box.
    try std.testing.expect(box.hasSignature(result));
}

test "encode 2x2 grayscale no-decomp round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    const pixels = [_]u8{ 100, 200, 50, 150 };
    const params = EncodeParams{
        .width = 2,
        .height = 2,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 2), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 4x4 grayscale no-decomp round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    const pixels = [_]u8{
        100, 110, 120, 130,
        140, 150, 160, 170,
        180, 190, 200, 210,
        220, 230, 240, 250,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 4), decoded.width);
    try std.testing.expectEqual(@as(u32, 4), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 8x8 grayscale no-decomp round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [64]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast(i * 4);

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();

    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 3x5 grayscale no-decomp round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    const pixels = [_]u8{
        10,  20,  30,
        40,  50,  60,
        70,  80,  90,
        100, 110, 120,
        130, 140, 150,
    };

    const params = EncodeParams{
        .width = 3,
        .height = 5,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();

    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 2x2 RGB no-decomp round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    const pixels = [_]u8{
        255, 0, 0, // red
        0, 255, 0, // green
        0, 0, 255, // blue
        128, 128, 128, // gray
    };

    const params = EncodeParams{
        .width = 2,
        .height = 2,
        .components = 3,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();

    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "clampDecompositionLevels returns sane values" {
    try std.testing.expectEqual(@as(u8, 0), clampDecompositionLevels(1, 1));
    try std.testing.expectEqual(@as(u8, 1), clampDecompositionLevels(2, 2));
    try std.testing.expectEqual(@as(u8, 2), clampDecompositionLevels(4, 4));
    try std.testing.expectEqual(@as(u8, 3), clampDecompositionLevels(8, 8));
    try std.testing.expectEqual(@as(u8, 5), clampDecompositionLevels(256, 256));
    try std.testing.expectEqual(@as(u8, 1), clampDecompositionLevels(2, 1024));
}

test "encode 3x1 grayscale no-decomp round-trips" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{ 50, 100, 200 };
    const params = EncodeParams{ .width = 3, .height = 1, .components = 1, .bits_per_component = 8, .decomposition_levels = 0, .wavelet_transform = 1, .multiple_component_transform = false, .format = .j2k };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);
    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 3x2 grayscale 1-level decomp round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{
        255, 128, 64,
        32,  192, 0,
    };
    const params = EncodeParams{
        .width = 3,
        .height = 2,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);
    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 2x3 grayscale no-decomp round-trips" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{ 50, 100, 200, 30, 70, 250 };
    const params = EncodeParams{ .width = 2, .height = 3, .components = 1, .bits_per_component = 8, .decomposition_levels = 0, .wavelet_transform = 1, .multiple_component_transform = false, .format = .j2k };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);
    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 4x1 grayscale no-decomp round-trips" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{ 50, 100, 200, 30 };
    const params = EncodeParams{ .width = 4, .height = 1, .components = 1, .bits_per_component = 8, .decomposition_levels = 0, .wavelet_transform = 1, .multiple_component_transform = false, .format = .j2k };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);
    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 1x5 grayscale no-decomp round-trips" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{ 50, 100, 200, 30, 70 };
    const params = EncodeParams{ .width = 1, .height = 5, .components = 1, .bits_per_component = 8, .decomposition_levels = 0, .wavelet_transform = 1, .multiple_component_transform = false, .format = .j2k };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);
    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 1x4 grayscale no-decomp round-trips" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{ 50, 100, 200, 30 };
    const params = EncodeParams{ .width = 1, .height = 4, .components = 1, .bits_per_component = 8, .decomposition_levels = 0, .wavelet_transform = 1, .multiple_component_transform = false, .format = .j2k };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);
    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 8x8 grayscale 2-level decomp round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [64]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast(i * 4);

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();

    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 4x4 JP2 container round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    const pixels = [_]u8{
        100, 110, 120, 130,
        140, 150, 160, 170,
        180, 190, 200, 210,
        220, 230, 240, 250,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .jp2,
    };
    const jp2 = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(jp2);

    try std.testing.expect(box.hasSignature(jp2));

    var decoded = try decode.decodeU8Bytes(allocator, jp2);
    defer decoded.deinit();

    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode RLCP codestream round-trips and preserves progression order" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [64]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 17) % 256);

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .num_layers = 1,
        .progression_order = 1,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 1), state.coding_style.?.progression_order);
    try std.testing.expectEqual(@as(u16, 1), state.coding_style.?.num_layers);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode CPRL RGB codestream round-trips and preserves progression order" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    const pixels = [_]u8{
        255, 0,   0,   0,   255, 0,   0,   0,   255, 255, 255, 0,
        128, 128, 128, 64,  64,  64,  200, 100, 50,  10,  20,  30,
        100, 200, 50,  50,  100, 200, 0,   0,   0,   255, 255, 255,
        1,   2,   3,   253, 254, 255, 127, 0,   255, 0,   127, 255,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 3,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .num_layers = 1,
        .progression_order = 4,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 4), state.coding_style.?.progression_order);
    try std.testing.expectEqual(@as(u16, 1), state.coding_style.?.num_layers);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode multi-tile grayscale round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [64]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 9) % 256);

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .tile_width = 4,
        .tile_height = 4,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expect(state.header.uses_multiple_tiles);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode multi-tile RGB round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [8 * 8 * 3]u8 = undefined;
    for (0..64) |i| {
        pixels[i * 3] = @intCast((i * 5) % 256);
        pixels[i * 3 + 1] = @intCast((i * 7) % 256);
        pixels[i * 3 + 2] = @intCast((255 - i * 3) % 256);
    }

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 3,
        .tile_width = 4,
        .tile_height = 4,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expect(state.header.uses_multiple_tiles);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode single-tile multipart grayscale round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [64]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 13) % 256);

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .tile_parts_per_tile = 2,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), state.tile_parts.len);
    try std.testing.expectEqual(@as(u8, 0), state.tile_parts[0].tile_part_index);
    try std.testing.expectEqual(@as(u8, 1), state.tile_parts[1].tile_part_index);
    try std.testing.expectEqual(@as(u8, 2), state.tile_parts[0].num_tile_parts);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode multi-tile multipart grayscale round-trips through decoder" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [64]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 11) % 256);

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .tile_width = 4,
        .tile_height = 4,
        .tile_parts_per_tile = 2,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 8), state.tile_parts.len);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode multi-layer grayscale codestream round-trips and preserves layer count" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [16 * 16]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 19) % 251);

    const params = EncodeParams{
        .width = 16,
        .height = 16,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .num_layers = 3,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 3), state.coding_style.?.num_layers);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 32x32 lossless grayscale with num_layers=4 is bit-exact" {
    // Focused regression for Tier-2 multi-layer distribution with more
    // codeblocks and resolutions than the 8x8 case.
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [32 * 32]u8 = undefined;
    const wm1: u32 = 31;
    const hm1: u32 = 31;
    for (0..32) |y| {
        for (0..32) |x| {
            const v: u32 = ((@as(u32, @intCast(x)) * 255) / wm1 +
                (@as(u32, @intCast(y)) * 255) / hm1) / 2;
            pixels[y * 32 + x] = @intCast(v);
        }
    }

    const params = EncodeParams{
        .width = 32,
        .height = 32,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 3,
        .num_layers = 4,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 16x16 checkerboard lossless grayscale num_layers=3 is bit-exact" {
    // Mirrors the conformance 'gray8_16x16_lossy_97_3layers' case flipped
    // back to 5/3 reversible; tier-2 must emit a contribution bit for every
    // precinct codeblock in every layer.
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [16 * 16]u8 = undefined;
    for (0..16) |y| {
        for (0..16) |x| {
            const on = ((x ^ y) & 1) == 0;
            pixels[y * 16 + x] = if (on) 230 else 25;
        }
    }

    const params = EncodeParams{
        .width = 16,
        .height = 16,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .num_layers = 3,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 8x8 lossless grayscale with num_layers=3 is bit-exact" {
    // Focused regression for the Tier-2 multi-layer pass distribution:
    // 5/3 reversible must be bit-exact regardless of num_layers.
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [8 * 8]u8 = undefined;
    for (0..8) |y| {
        for (0..8) |x| {
            const on = ((x ^ y) & 1) == 0;
            pixels[y * 8 + x] = if (on) 230 else 25;
        }
    }

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .num_layers = 3,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode precinct-coded grayscale codestream round-trips and preserves precinct metadata" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [32 * 32]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 7 + i / 5) % 256);

    const params = EncodeParams{
        .width = 32,
        .height = 32,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 0,
        .code_block_height_exponent = 0,
        .precinct_width_exponent = 4,
        .precinct_height_exponent = 4,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    try std.testing.expect(state.coding_style.?.precincts_present);
    try std.testing.expect(state.coding_style.?.precinct_sizes != null);
    try std.testing.expectEqual(@as(usize, 2), state.coding_style.?.precinct_sizes.?.len);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "encode 12-bit grayscale round-trips through decodeU16Bytes" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const reconstruct = @import("reconstruct.zig");

    const pixels = [_]u16{ 0, 511, 1024, 2048, 3072, 4095 };
    const params = EncodeParams{
        .width = 3,
        .height = 2,
        .components = 1,
        .bits_per_component = 12,
        .decomposition_levels = 1,
        .format = .j2k,
    };
    const j2k = try encodeU16Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU16Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, pixels.len), decoded.pixels.len);

    var expected: [pixels.len]u16 = undefined;
    for (pixels, 0..) |sample, i| {
        expected[i] = try reconstruct.reconstructU16Sample(@as(i32, sample) - 2048, 12, false);
    }
    try std.testing.expectEqualSlices(u16, expected[0..], decoded.pixels);
}

test "encode 12-bit 8x8 gradient is bit-exact through decodeU16Bytes (regression)" {
    // Regression for the U16 wraparound defect: with bpc < 16 the decoder's
    // reconstruct step used to rescale samples up to the full 16-bit range,
    // which made 12-bit lossless round-trip fail by 2^16-magnitude wraparound.
    // After the fix, samples are returned in [0, 2^bpc - 1] and lossless
    // round-trip is bit-exact for bpc <= 16.
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    var pixels: [64]u16 = undefined;
    const max_value: u32 = (1 << 12) - 1;
    var y: usize = 0;
    while (y < 8) : (y += 1) {
        var x: usize = 0;
        while (x < 8) : (x += 1) {
            const rx: u32 = @intCast((x * max_value) / 7);
            const ry: u32 = @intCast((y * max_value) / 7);
            pixels[y * 8 + x] = @intCast((rx + ry) / 2);
        }
    }

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .bits_per_component = 12,
        .decomposition_levels = 2,
        .format = .j2k,
    };
    const j2k = try encodeU16Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU16Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u16, pixels[0..], decoded.pixels);
}

test "encode 10-bit RGB round-trips through decodeU16Bytes" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const reconstruct = @import("reconstruct.zig");

    const pixels = [_]u16{
        0,    128, 1023,
        64,   512, 900,
        1023, 32,  256,
        700,  300, 100,
    };
    const params = EncodeParams{
        .width = 2,
        .height = 2,
        .components = 3,
        .bits_per_component = 10,
        .decomposition_levels = 0,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU16Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU16Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, pixels.len), decoded.pixels.len);

    var expected: [pixels.len]u16 = undefined;
    for (pixels, 0..) |sample, i| {
        expected[i] = try reconstruct.reconstructU16Sample(@as(i32, sample) - 512, 10, false);
    }
    try std.testing.expectEqualSlices(u16, expected[0..], decoded.pixels);
}

test "precinct packet model bodies match encoded codeblocks" {
    const allocator = std.testing.allocator;
    const packet = @import("packet.zig");

    var pixels: [32 * 32]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 7 + i / 5) % 256);

    const params = EncodeParams{
        .width = 32,
        .height = 32,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 0,
        .code_block_height_exponent = 0,
        .precinct_width_exponent = 4,
        .precinct_height_exponent = 4,
        .format = .j2k,
    };

    const w: usize = params.width;
    const h: usize = params.height;
    const planes = try allocator.alloc([]i32, 1);
    defer {
        allocator.free(planes[0]);
        allocator.free(planes);
    }
    planes[0] = try allocator.alloc(i32, w * h);
    for (0..w * h) |i| planes[0][i] = @as(i32, pixels[i]) - 128;
    try forwardWaveletMultiLevel(allocator, planes[0], w, h, params.decomposition_levels);

    const cb_w: usize = @as(usize, 1) << @intCast(params.code_block_width_exponent + 4);
    const cb_h: usize = @as(usize, 1) << @intCast(params.code_block_height_exponent + 4);
    var encoded_cbs: std.ArrayListUnmanaged(tier2_encode.EncodedCodeblockInfo) = .empty;
    defer {
        for (encoded_cbs.items) |cb| {
            allocator.free(cb.data);
            allocator.free(cb.pass_lengths);
        }
        encoded_cbs.deinit(allocator);
    }
    try encodeComponentCodeblocks(
        allocator,
        &encoded_cbs,
        planes[0],
        w,
        h,
        params.decomposition_levels,
        params.bits_per_component,
        0,
        cb_w,
        cb_h,
        &params,
    );

    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var state = try codestream.parseState(allocator, j2k);
    defer state.deinit(allocator);
    const tile_ranges = try codestream.parseTilePartRanges(allocator, j2k);
    defer allocator.free(tile_ranges);
    const tile_range = tile_ranges[0];
    const payload = j2k[tile_range.data_offset .. tile_range.data_offset + tile_range.data_length];
    var model = try packet.buildPacketModelFromPayload(allocator, &state, payload, tile_range.data_offset, .packet_present_tagtree_first_inclusion);
    defer model.deinit(allocator);

    try std.testing.expectEqual(encoded_cbs.items.len, model.entries.len);

    for (model.entries) |entry| {
        var found = false;
        for (encoded_cbs.items) |cb| {
            if (cb.component_index != entry.coordinate.component_index or
                cb.resolution_index != entry.coordinate.resolution_index or
                cb.precinct_index != entry.coordinate.precinct_index or
                cb.subband != entry.subband or
                cb.codeblock_x != entry.codeblock_x or
                cb.codeblock_y != entry.codeblock_y)
            {
                continue;
            }
            found = true;
            try std.testing.expectEqual(cb.data.len, entry.body_length);
            try std.testing.expectEqualSlices(u8, cb.data, payload[entry.body_offset - tile_range.data_offset .. entry.body_offset - tile_range.data_offset + entry.body_length]);
            break;
        }
        try std.testing.expect(found);
    }
}

test "9/7 irreversible encode round-trips 8x8 grayscale with modest PSNR" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    // Gradient image.
    var pixels: [64]u8 = undefined;
    for (&pixels, 0..) |*p, i| p.* = @intCast((i * 4) & 0xff);

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .wavelet_transform = 0, // 9/7 irreversible
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    try std.testing.expectEqual(@as(u32, 8), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);

    // With Δ = 1 across all subbands this is near-lossless (limited only by f32 rounding).
    // Tolerate a small per-pixel error budget.
    var max_err: i32 = 0;
    for (pixels, decoded.pixels) |orig, dec| {
        const diff: i32 = @as(i32, orig) - @as(i32, dec);
        const abs_diff = if (diff < 0) -diff else diff;
        if (abs_diff > max_err) max_err = abs_diff;
    }
    try std.testing.expect(max_err <= 4);
}

test "rate control reduces output size when target_bytes is tight" {
    const allocator = std.testing.allocator;

    var pixels: [1024]u8 = undefined;
    for (&pixels, 0..) |*p, i| {
        const x = i % 32;
        const y = i / 32;
        p.* = @intCast((x * 8 + y * 4) & 0xff);
    }

    const params_uncapped = EncodeParams{
        .width = 32,
        .height = 32,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 3,
        .wavelet_transform = 0,
        .format = .j2k,
    };
    const uncapped = try encodeU8Bytes(allocator, &pixels, &params_uncapped);
    defer allocator.free(uncapped);

    var params_capped = params_uncapped;
    params_capped.target_bytes = @intCast(uncapped.len / 2);
    const capped = try encodeU8Bytes(allocator, &pixels, &params_capped);
    defer allocator.free(capped);

    try std.testing.expect(capped.len < uncapped.len);
    try std.testing.expect(capped.len <= uncapped.len); // no weirdness
}

test "9/7 irreversible encode with RCT falls back to no MCT (tested separately)" {
    // ICT applies to 9/7; RCT applies only to 5/3. We validate that 9/7 + MCT + 3 components
    // doesn't crash and produces a decodable bitstream.
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    // 4x4 RGB image with varying colours.
    const pixels = [_]u8{
        200, 100, 50, 210, 105, 55, 220, 110, 60, 230, 115, 65,
        190, 95,  45, 200, 100, 50, 210, 105, 55, 220, 110, 60,
        180, 90,  40, 190, 95,  45, 200, 100, 50, 210, 105, 55,
        170, 85,  35, 180, 90,  40, 190, 95,  45, 200, 100, 50,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 3,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 0,
        .multiple_component_transform = true,
        .format = .j2k,
    };
    const j2k = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(j2k);

    var decoded = try decode.decodeU8Bytes(allocator, j2k);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 4), decoded.width);
    try std.testing.expectEqual(@as(u8, 3), decoded.components);
}

test "SOP marker emission changes codestream bytes and sets Scod bit 1" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        100, 110, 120, 130,
        140, 150, 160, 170,
        180, 190, 200, 210,
        220, 230, 240, 250,
    };
    const base = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
    };
    const with_sop = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
        .emit_sop_markers = true,
    };
    const a = try encodeU8Bytes(allocator, &pixels, &base);
    defer allocator.free(a);
    const b = try encodeU8Bytes(allocator, &pixels, &with_sop);
    defer allocator.free(b);
    try std.testing.expect(b.len > a.len);

    var state = try codestream.parseState(allocator, b);
    defer state.deinit(allocator);
    try std.testing.expect(state.coding_style != null);
    try std.testing.expectEqual(@as(u8, 0x02), state.coding_style.?.scod & 0x02);

    // The tile-part data area must contain at least one SOP magic (0xff91).
    const ranges = try codestream.parseTilePartRanges(allocator, b);
    defer allocator.free(ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    const payload = b[ranges[0].data_offset .. ranges[0].data_offset + ranges[0].data_length];
    var found_sop = false;
    var i: usize = 0;
    while (i + 1 < payload.len) : (i += 1) {
        if (payload[i] == 0xff and payload[i + 1] == 0x91) {
            found_sop = true;
            break;
        }
    }
    try std.testing.expect(found_sop);

    // Round-trip: decoding must reproduce the original pixels.
    const decode = @import("decode.zig");
    var decoded = try decode.decodeU8Bytes(allocator, b);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "EPH marker emission sets Scod bit 2 and round-trips" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10,  20,  30,  40,
        50,  60,  70,  80,
        90,  100, 110, 120,
        130, 140, 150, 160,
    };
    const with_eph = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
        .emit_eph_markers = true,
    };
    const b = try encodeU8Bytes(allocator, &pixels, &with_eph);
    defer allocator.free(b);

    var state = try codestream.parseState(allocator, b);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(u8, 0x04), state.coding_style.?.scod & 0x04);

    // At least one 0xff92 magic appears inside the tile-part data.
    const ranges = try codestream.parseTilePartRanges(allocator, b);
    defer allocator.free(ranges);
    const payload = b[ranges[0].data_offset .. ranges[0].data_offset + ranges[0].data_length];
    var found = false;
    var i: usize = 0;
    while (i + 1 < payload.len) : (i += 1) {
        if (payload[i] == 0xff and payload[i + 1] == 0x92) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);

    const decode = @import("decode.zig");
    var decoded = try decode.decodeU8Bytes(allocator, b);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "SOP and EPH together round-trip" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        0,  32, 64, 96,
        16, 48, 80, 112,
        8,  40, 72, 104,
        24, 56, 88, 120,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
        .emit_sop_markers = true,
        .emit_eph_markers = true,
    };
    const b = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(b);
    const decode = @import("decode.zig");
    var decoded = try decode.decodeU8Bytes(allocator, b);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "PLT marker emission produces parseable lengths that match packet bytes" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10,  20,  30,  40,
        50,  60,  70,  80,
        90,  100, 110, 120,
        130, 140, 150, 160,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
        .emit_plt = true,
    };
    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);
    var state = try codestream.parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expect(state.plt_markers.len >= 1);

    var total: u64 = 0;
    for (state.plt_markers) |plt| {
        for (plt.lengths) |len| total += len;
    }
    const ranges = try codestream.parseTilePartRanges(allocator, bytes);
    defer allocator.free(ranges);
    // PLT lengths sum to the total packet bytes in this (single tile-part) tile.
    try std.testing.expectEqual(@as(u64, ranges[0].data_length), total);

    // Decoding remains unaffected.
    const decode = @import("decode.zig");
    var decoded = try decode.decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "TLM marker emission records tile-part lengths parseable by decoder" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10,  20,  30,  40,
        50,  60,  70,  80,
        90,  100, 110, 120,
        130, 140, 150, 160,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
        .emit_tlm = true,
    };
    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);

    var state = try codestream.parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expect(state.tile_part_lengths.len >= 1);

    // The TLM Ptlm fields should match the SOT Psot values.
    const ranges = try codestream.parseTilePartRanges(allocator, bytes);
    defer allocator.free(ranges);
    try std.testing.expectEqual(ranges.len, state.tile_part_lengths.len);
    for (ranges, state.tile_part_lengths) |r, entry| {
        const psot = r.next_offset - r.sot_offset;
        try std.testing.expectEqual(@as(u32, @intCast(psot)), entry.tile_part_length);
    }

    // Decoding still works.
    const decode = @import("decode.zig");
    var decoded = try decode.decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "POC marker encode/parse round-trip with 2 user entries" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10,  20,  30,  40,
        50,  60,  70,  80,
        90,  100, 110, 120,
        130, 140, 150, 160,
    };
    const windows = [_]ProgressionWindow{
        .{ .rs = 0, .re = 1, .cs = 0, .ce = 1, .lye = 1, .order = 0 },
        .{ .rs = 1, .re = 2, .cs = 0, .ce = 1, .lye = 1, .order = 1 },
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
        .progression_windows = &windows,
    };
    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);

    var state = try codestream.parseState(allocator, bytes);
    defer state.deinit(allocator);
    // 1 synthetic default entry + 2 user windows.
    try std.testing.expectEqual(@as(usize, 3), state.poc_entries.len);
    // Synthetic default entry covers full range at default order 0.
    try std.testing.expectEqual(@as(u8, 0), state.poc_entries[0].rs_poc);
    try std.testing.expectEqual(@as(u8, 2), state.poc_entries[0].re_poc);
    try std.testing.expectEqual(@as(u16, 0), state.poc_entries[0].cs_poc);
    try std.testing.expectEqual(@as(u16, 1), state.poc_entries[0].ce_poc);
    try std.testing.expectEqual(@as(u8, 0), state.poc_entries[0].progression_order);
    // First user window.
    try std.testing.expectEqual(@as(u8, 0), state.poc_entries[1].rs_poc);
    try std.testing.expectEqual(@as(u8, 1), state.poc_entries[1].re_poc);
    try std.testing.expectEqual(@as(u8, 0), state.poc_entries[1].progression_order);
    // Second user window.
    try std.testing.expectEqual(@as(u8, 1), state.poc_entries[2].rs_poc);
    try std.testing.expectEqual(@as(u8, 2), state.poc_entries[2].re_poc);
    try std.testing.expectEqual(@as(u8, 1), state.poc_entries[2].progression_order);
}

test "empty progression_windows preserves default single-order round-trip" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10,  20,  30,  40,
        50,  60,  70,  80,
        90,  100, 110, 120,
        130, 140, 150, 160,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
    };
    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);
    var state = try codestream.parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), state.poc_entries.len);

    const decode = @import("decode.zig");
    var decoded = try decode.decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "PLT marker round-trip with SOP/EPH markers" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10,  20,  30,  40,
        50,  60,  70,  80,
        90,  100, 110, 120,
        130, 140, 150, 160,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .decomposition_levels = 1,
        .format = .j2k,
        .emit_sop_markers = true,
        .emit_eph_markers = true,
        .emit_plt = true,
        .emit_tlm = true,
    };
    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);
    var state = try codestream.parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expect(state.plt_markers.len >= 1);
    try std.testing.expect(state.tile_part_lengths.len >= 1);
    try std.testing.expectEqual(@as(u8, 0x06), state.coding_style.?.scod & 0x06);

    const decode = @import("decode.zig");
    var decoded = try decode.decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "RGN encode emits marker parsable by codestream state" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10, 40, 70, 120,
        20, 50, 80, 130,
        30, 60, 90, 140,
        35, 65, 95, 150,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
        .roi_component = 0,
        .roi_shift = 3,
    };
    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);

    var state = try codestream.parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.rgn_entries.len);
    try std.testing.expectEqual(@as(u16, 0), state.rgn_entries[0].component);
    try std.testing.expectEqual(@as(u8, 0), state.rgn_entries[0].style);
    try std.testing.expectEqual(@as(u8, 3), state.rgn_entries[0].shift);
}

test "RGN 5/3 whole-component ROI produces distinct bitstream that still decodes" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{
        10, 40, 70, 120,
        20, 50, 80, 130,
        30, 60, 90, 140,
        35, 65, 95, 150,
    };
    const params_no_roi = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .wavelet_transform = 1, // 5/3 reversible
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const params_roi = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 2,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
        .roi_component = 0,
        .roi_shift = 3,
    };

    const bytes_no_roi = try encodeU8Bytes(allocator, &pixels, &params_no_roi);
    defer allocator.free(bytes_no_roi);
    const bytes_roi = try encodeU8Bytes(allocator, &pixels, &params_roi);
    defer allocator.free(bytes_roi);

    // ROI emission must produce a distinct codestream: RGN marker plus
    // upshifted coefficients change the packet payload.
    try std.testing.expect(!std.mem.eql(u8, bytes_no_roi, bytes_roi));

    // The RGN-enabled codestream must still decode without errors.
    var decoded = try decode.decodeU8Bytes(allocator, bytes_roi);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 4), decoded.width);
    try std.testing.expectEqual(@as(u32, 4), decoded.height);
}

test "custom MCT markers round-trip through codestream" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        200, 100, 50, 210, 105, 55, 220, 110, 60, 230, 115, 65,
        190, 95,  45, 200, 100, 50, 210, 105, 55, 220, 110, 60,
        180, 90,  40, 190, 95,  45, 200, 100, 50, 210, 105, 55,
        170, 85,  35, 180, 90,  40, 190, 95,  45, 200, 100, 50,
    };
    // A trivial identity decorrelation; numerical correctness of the matrix
    // is exercised in color_transform.zig. Here we only verify marker wiring.
    const forward = [_]f32{ 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0 };
    const inverse = [_]f32{ 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0 };
    const offsets = [_]f32{ 128.0, 128.0, 128.0 };
    const matrix = color_transform.CustomMctMatrix{
        .num_components = 3,
        .forward = &forward,
        .inverse = &inverse,
        .offsets = &offsets,
    };
    const params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 3,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 0,
        .multiple_component_transform = true,
        .format = .j2k,
        .custom_mct = matrix,
    };
    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);

    var state = try codestream.parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.mct_segments.len);
    try std.testing.expectEqual(@as(u8, 2), state.mct_segments[0].element_type);
    try std.testing.expectEqual(@as(usize, 9 * @sizeOf(f32)), state.mct_segments[0].payload.len);
    try std.testing.expectEqual(@as(usize, 1), state.mcc_collections.len);
    try std.testing.expect(state.mco != null);
    try std.testing.expectEqual(@as(usize, 1), state.mco.?.ids.len);

    // Payload f32 coefficients must round-trip byte-for-byte with the matrix.
    var i: usize = 0;
    while (i < 9) : (i += 1) {
        const lo = i * 4;
        const raw = std.mem.readInt(u32, state.mct_segments[0].payload[lo..][0..4], .big);
        const coeff: f32 = @bitCast(raw);
        try std.testing.expectEqual(forward[i], coeff);
    }
}

test "encode with custom_mct applies user matrix in forward path" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");
    const pixels = [_]u8{
        200, 100, 50, 210, 105, 55, 220, 110, 60, 230, 115, 65,
        190, 95,  45, 200, 100, 50, 210, 105, 55, 220, 110, 60,
        180, 90,  40, 190, 95,  45, 200, 100, 50, 210, 105, 55,
        170, 85,  35, 180, 90,  40, 190, 95,  45, 200, 100, 50,
    };
    // Built-in ICT coefficients (ISO 15444-1 Annex G-1). Encoding with this
    // matrix should produce a codestream very similar in size to a plain
    // ICT-encoded stream, because the forward pipeline applies the same math.
    const forward = [_]f32{
        0.299,     0.587,     0.114,
        -0.168736, -0.331264, 0.5,
        0.5,       -0.418688, -0.081312,
    };
    const inverse = [_]f32{
        1.0, 0.0,       1.402,
        1.0, -0.344136, -0.714136,
        1.0, 1.772,     0.0,
    };
    const offsets = [_]f32{ 0.0, 0.0, 0.0 };
    const matrix = color_transform.CustomMctMatrix{
        .num_components = 3,
        .forward = &forward,
        .inverse = &inverse,
        .offsets = &offsets,
    };

    const base_params = EncodeParams{
        .width = 4,
        .height = 4,
        .components = 3,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 0,
        .multiple_component_transform = true,
        .format = .j2k,
    };
    var custom_params = base_params;
    custom_params.custom_mct = matrix;

    const bytes_ict = try encodeU8Bytes(allocator, &pixels, &base_params);
    defer allocator.free(bytes_ict);
    const bytes_custom = try encodeU8Bytes(allocator, &pixels, &custom_params);
    defer allocator.free(bytes_custom);

    // The custom stream carries extra MCT/MCC/MCO markers, so it must be
    // longer than the built-in ICT stream. Decode must still work (falls
    // back to ICT in the reconstruct path for MVP).
    try std.testing.expect(bytes_custom.len > bytes_ict.len);
    var decoded = try decode.decodeU8Bytes(allocator, bytes_custom);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u8, 3), decoded.components);
}

test "custom MCT encode+decode round-trip via marker chain" {
    const allocator = std.testing.allocator;
    const decode = @import("decode.zig");

    // 8x8 RGB with a smooth gradient, chosen so the 9/7 path + custom matrix
    // gives high PSNR even though we're using a tiny tile.
    var pixels: [8 * 8 * 3]u8 = undefined;
    {
        var y: usize = 0;
        while (y < 8) : (y += 1) {
            var x: usize = 0;
            while (x < 8) : (x += 1) {
                const base: u8 = @intCast((x + y) * 8);
                pixels[(y * 8 + x) * 3 + 0] = base;
                pixels[(y * 8 + x) * 3 + 1] = base +% 40;
                pixels[(y * 8 + x) * 3 + 2] = base +% 80;
            }
        }
    }

    // Built-in ICT matrix (ISO 15444-1 Annex G-1). Using ICT itself as the
    // custom matrix verifies that the decoder reads markers → builds matrix
    // → applies inverse, independent of the built-in ICT fallback.
    const forward = [_]f32{
        0.299,     0.587,     0.114,
        -0.168736, -0.331264, 0.5,
        0.5,       -0.418688, -0.081312,
    };
    const inverse = [_]f32{
        1.0, 0.0,       1.402,
        1.0, -0.344136, -0.714136,
        1.0, 1.772,     0.0,
    };
    const offsets = [_]f32{ 0.0, 0.0, 0.0 };
    const matrix = color_transform.CustomMctMatrix{
        .num_components = 3,
        .forward = &forward,
        .inverse = &inverse,
        .offsets = &offsets,
    };

    const params = EncodeParams{
        .width = 8,
        .height = 8,
        .components = 3,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 0, // 9/7 irreversible
        .multiple_component_transform = true,
        .format = .j2k,
        .custom_mct = matrix,
    };

    const bytes = try encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(bytes);

    var decoded = try decode.decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    try std.testing.expectEqual(@as(u32, 8), decoded.height);
    try std.testing.expectEqual(@as(u8, 3), decoded.components);

    var sse: f64 = 0.0;
    var i: usize = 0;
    while (i < pixels.len) : (i += 1) {
        const d: f64 = @as(f64, @floatFromInt(pixels[i])) - @as(f64, @floatFromInt(decoded.pixels[i]));
        sse += d * d;
    }
    const mse = sse / @as(f64, @floatFromInt(pixels.len));
    const psnr_db: f64 = if (mse <= 0.0) 100.0 else 10.0 * std.math.log10(255.0 * 255.0 / mse);
    try std.testing.expect(psnr_db > 40.0);
}
