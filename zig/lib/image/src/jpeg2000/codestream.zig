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

pub const Component = struct {
    bits_per_component: u8,
    is_signed: bool,
    xrsiz: u8,
    yrsiz: u8,
};

pub const Header = struct {
    width: u32,
    height: u32,
    x_offset: u32 = 0,
    y_offset: u32 = 0,
    components: []Component,
    tile_width: u32,
    tile_height: u32,
    tile_x_offset: u32 = 0,
    tile_y_offset: u32 = 0,
    uses_multiple_tiles: bool,

    pub fn deinit(self: *Header, allocator: std.mem.Allocator) void {
        allocator.free(self.components);
        self.* = undefined;
    }
};

pub const CodingStyle = struct {
    progression_order: u8,
    num_layers: u16,
    multiple_component_transform: bool,
    decomposition_levels: u8,
    code_block_width_exponent: u8,
    code_block_height_exponent: u8,
    code_block_style: u8,
    wavelet_transform: u8,
    precincts_present: bool,
    /// Full Scod byte from the COD marker. Retained so error-resilience
    /// bits (SOP = 0x02, EPH = 0x04) are visible to the decoder without
    /// re-reading the marker segment.
    scod: u8 = 0,
    precinct_sizes: ?[]u8 = null,

    pub fn deinit(self: *CodingStyle, allocator: std.mem.Allocator) void {
        if (self.precinct_sizes) |ps| allocator.free(ps);
    }
};

pub const QuantizationStyle = struct {
    style: u8,
    guard_bits: u8,
    step_values: []u16,

    pub fn deinit(self: *QuantizationStyle, allocator: std.mem.Allocator) void {
        allocator.free(self.step_values);
        self.* = undefined;
    }
};

pub const Comment = struct {
    registration: u16,
    text: []u8,

    pub fn deinit(self: *Comment, allocator: std.mem.Allocator) void {
        allocator.free(self.text);
        self.* = undefined;
    }
};

pub const TilePart = struct {
    tile_index: u16,
    tile_part_length: u32,
    tile_part_index: u8,
    num_tile_parts: u8,
    quantization_style: ?QuantizationStyle = null,
    poc_entries: []PocEntry = &.{},
    plt_markers: []PltMarker = &.{},
    /// Tile-part-scoped packed packet header segments (PPT, ISO A.7.3). Raw
    /// payloads are preserved verbatim; tier-2 consumption is a follow-up.
    ppt_segments: []PptSegment = &.{},

    pub fn deinit(self: *TilePart, allocator: std.mem.Allocator) void {
        if (self.quantization_style) |*q| q.deinit(allocator);
        if (self.poc_entries.len > 0) allocator.free(self.poc_entries);
        for (self.plt_markers) |*p| p.deinit(allocator);
        if (self.plt_markers.len > 0) allocator.free(self.plt_markers);
        for (self.ppt_segments) |*s| s.deinit(allocator);
        if (self.ppt_segments.len > 0) allocator.free(self.ppt_segments);
        self.* = undefined;
    }
};

/// Packed packet headers, main header (PPM, ISO A.7.2). Raw payload preserved;
/// the interleaved (Nppm:u32, Ippm:Nppm) sub-structure is walked at consume
/// time, not parse time.
pub const PpmSegment = struct {
    zppm: u8,
    payload: []u8,

    pub fn deinit(self: *PpmSegment, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
        self.* = undefined;
    }
};

/// Packed packet headers, tile-part header (PPT, ISO A.7.3). Raw payload
/// preserved; tier-2 consumption is deferred.
pub const PptSegment = struct {
    zppt: u8,
    payload: []u8,

    pub fn deinit(self: *PptSegment, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
        self.* = undefined;
    }
};

/// Packet-length list from a single PLT marker. Parsed for round-trip parity
/// but not consumed by the decoder: the tier-2 parser still derives lengths
/// from packet-header bits. Kept so encoders can assert byte equality.
pub const PltMarker = struct {
    zplt: u8,
    lengths: []u32,

    pub fn deinit(self: *PltMarker, allocator: std.mem.Allocator) void {
        allocator.free(self.lengths);
        self.* = undefined;
    }
};

pub const TileTlmEntry = struct {
    tile_index: ?u16,
    tile_part_length: u32,
};

/// Progression Order Change entry (Annex A-6, POC marker).
/// Describes a single window of packets in resolution/component/layer space
/// plus the progression order to enumerate it with.
pub const PocEntry = struct {
    rs_poc: u8, // starting resolution (inclusive)
    ce_poc: u16, // ending component (exclusive)
    lye_poc: u16, // ending layer (exclusive)
    re_poc: u8, // ending resolution (exclusive)
    cs_poc: u16, // starting component (inclusive)
    progression_order: u8,
};

/// Region of Interest entry (Annex A-6.5, RGN marker).
/// Only style 0 (implicit max-shift) is defined in Part 1: coefficients whose
/// magnitude exceeds 2^(Mb - shift) are interpreted by the decoder as having
/// been left-shifted by `shift` bitplanes, and are shifted down post-Tier-1.
pub const RgnEntry = struct {
    component: u16,
    style: u8,
    shift: u8,
};

/// Multiple Component Transformation (MCT) marker (ISO 15444-1 A.3.8).
/// Stores the raw Dmct coefficient payload for a single MCT segment.
/// `index` is the Imct/Ymct-derived identifier (upper 14 bits of Imct are 0
/// for MVP; we store the raw Imct low bits). `element_type` encodes the
/// coefficient element type from Ymct (0 = 16-bit int, 1 = 32-bit int,
/// 2 = f32, 3 = f64).
pub const McTSegment = struct {
    index: u16,
    element_type: u8,
    payload: []u8,

    pub fn deinit(self: *McTSegment, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
        self.* = undefined;
    }
};

/// Multiple Component Collection (MCC) marker (ISO 15444-1 A.3.9).
/// Stores the raw Qmcc payload; full parsing of collection stages is deferred.
pub const McCCollection = struct {
    index: u16,
    payload: []u8,

    pub fn deinit(self: *McCCollection, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
        self.* = undefined;
    }
};

/// Multiple Component Ordering (MCO) marker (ISO 15444-1 A.3.10).
/// Lists the MCC indices to apply in order.
pub const McOOrdering = struct {
    ids: []u16,

    pub fn deinit(self: *McOOrdering, allocator: std.mem.Allocator) void {
        allocator.free(self.ids);
        self.* = undefined;
    }
};

pub const TilePartRange = struct {
    tile_index: u16,
    tile_part_index: u8,
    num_tile_parts: u8,
    sot_offset: usize,
    sod_offset: usize,
    data_offset: usize,
    data_length: usize,
    next_offset: usize,
};

pub const NativeDecodeSupport = enum {
    supported,
    unsupported_components,
    unsupported_precision,
    unsupported_signed_samples,
    multiple_tiles,
    missing_coding_style,
    missing_quantization_style,
    unsupported_progression_order,
    unsupported_precincts,
    unsupported_code_block_style,
    unsupported_wavelet_transform,
    unsupported_quantization_mode,
    unsupported_multi_component_transform,
    unsupported_tile_part_layout,
    missing_start_of_data,
    missing_end_of_codestream,
    unsupported_packed_packet_headers,
};

pub const State = struct {
    header: Header,
    coding_style: ?CodingStyle = null,
    quantization_style: ?QuantizationStyle = null,
    component_coding_styles: []?CodingStyle = &.{},
    component_quantization_styles: []?QuantizationStyle = &.{},
    comments: []Comment,
    tile_parts: []TilePart,
    plt_markers: []PltMarker = &.{},
    tile_part_lengths: []TileTlmEntry = &.{},
    poc_entries: []PocEntry = &.{},
    rgn_entries: []RgnEntry = &.{},
    mct_segments: []McTSegment = &.{},
    mcc_collections: []McCCollection = &.{},
    mco: ?McOOrdering = null,
    /// Main-header PPM segments (ISO A.7.2). Captured in the order encountered;
    /// Zppm must be monotonically non-decreasing starting at 0. When non-empty,
    /// every tile's packet headers are drawn from this concatenated stream and
    /// no tile-part may carry PPT (see PpmPptConflict).
    ppm_segments: []PpmSegment = &.{},
    has_start_of_data: bool = false,
    has_end_of_codestream: bool = false,

    pub fn deinit(self: *State, allocator: std.mem.Allocator) void {
        self.header.deinit(allocator);
        if (self.coding_style) |*cs| cs.deinit(allocator);
        if (self.quantization_style) |*q| q.deinit(allocator);
        for (self.component_coding_styles) |*cs| {
            if (cs.*) |*c| c.deinit(allocator);
        }
        if (self.component_coding_styles.len > 0) allocator.free(self.component_coding_styles);
        for (self.component_quantization_styles) |*qs| {
            if (qs.*) |*q| q.deinit(allocator);
        }
        if (self.component_quantization_styles.len > 0) allocator.free(self.component_quantization_styles);
        for (self.comments) |*c| c.deinit(allocator);
        allocator.free(self.comments);
        for (self.tile_parts) |*tp| tp.deinit(allocator);
        allocator.free(self.tile_parts);
        for (self.plt_markers) |*p| p.deinit(allocator);
        if (self.plt_markers.len > 0) allocator.free(self.plt_markers);
        if (self.tile_part_lengths.len > 0) allocator.free(self.tile_part_lengths);
        if (self.poc_entries.len > 0) allocator.free(self.poc_entries);
        if (self.rgn_entries.len > 0) allocator.free(self.rgn_entries);
        for (self.mct_segments) |*m| m.deinit(allocator);
        if (self.mct_segments.len > 0) allocator.free(self.mct_segments);
        for (self.mcc_collections) |*m| m.deinit(allocator);
        if (self.mcc_collections.len > 0) allocator.free(self.mcc_collections);
        if (self.mco) |*m| m.deinit(allocator);
        for (self.ppm_segments) |*p| p.deinit(allocator);
        if (self.ppm_segments.len > 0) allocator.free(self.ppm_segments);
        self.* = undefined;
    }

    /// Check support for the full (non-bounded) decode path.
    /// Allows arbitrary decomposition levels, wavelet transform 0 or 1,
    /// quantization styles 0/1/2, MCT, and all code block styles.
    /// Supports single-tile and multi-tile.
    /// Requires 1-16 bpc, 1/3 components, and standard progression orders.
    /// Component subsampling (XRsiz/YRsiz > 1) is allowed for the u8 decode
    /// path; subsampled components are upsampled to the image grid post-IDWT.
    pub fn fullNativeDecodeSupport(self: *const State) NativeDecodeSupport {
        if (self.header.components.len != 1 and self.header.components.len != 3) return .unsupported_components;
        for (self.header.components) |component| {
            if (component.bits_per_component == 0 or component.bits_per_component > 16) return .unsupported_precision;
            if (component.xrsiz == 0 or component.yrsiz == 0) return .unsupported_components;
        }
        const coding_style = self.coding_style orelse return .missing_coding_style;
        if (coding_style.progression_order > 4) return .unsupported_progression_order;
        // Allow both 5/3 (reversible, transform=1) and 9/7 (irreversible, transform=0)
        if (coding_style.wavelet_transform > 1) return .unsupported_wavelet_transform;
        // MCT allowed for 3-component images with both wavelet types
        // Quantization styles 0, 1, 2 all supported
        const quantization_style = self.quantization_style orelse return .missing_quantization_style;
        if (quantization_style.style > 2) return .unsupported_quantization_mode;

        if (!self.has_start_of_data) return .missing_start_of_data;
        if (!self.has_end_of_codestream) return .missing_end_of_codestream;
        if (self.tile_parts.len == 0) return .unsupported_tile_part_layout;
        return .supported;
    }
};

pub fn fullLlCodeblockBitplanes(state: *const State) !u8 {
    const qcd = state.quantization_style orelse return error.MissingQuantizationStyle;
    if (qcd.style != 0 or qcd.step_values.len == 0) return error.UnsupportedQuantizationMode;
    const expn: u8 = @intCast(qcd.step_values[0] >> 3);
    if (expn == 0) return error.InvalidBitplaneCount;
    return qcd.guard_bits + expn - 1;
}

pub fn hasSoc(bytes: []const u8) bool {
    return bytes.len >= 2 and std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) == markers.soc;
}

pub fn parseHeader(allocator: std.mem.Allocator, bytes: []const u8) !Header {
    var state = try parseState(allocator, bytes);
    const header = state.header;
    if (state.coding_style) |*cs| cs.deinit(allocator);
    if (state.quantization_style) |*q| q.deinit(allocator);
    for (state.component_coding_styles) |*cs| {
        if (cs.*) |*c| c.deinit(allocator);
    }
    if (state.component_coding_styles.len > 0) allocator.free(state.component_coding_styles);
    for (state.component_quantization_styles) |*qs| {
        if (qs.*) |*q| q.deinit(allocator);
    }
    if (state.component_quantization_styles.len > 0) allocator.free(state.component_quantization_styles);
    for (state.comments) |*c| c.deinit(allocator);
    allocator.free(state.comments);
    for (state.tile_parts) |*tp| tp.deinit(allocator);
    allocator.free(state.tile_parts);
    for (state.plt_markers) |*p| p.deinit(allocator);
    if (state.plt_markers.len > 0) allocator.free(state.plt_markers);
    if (state.tile_part_lengths.len > 0) allocator.free(state.tile_part_lengths);
    if (state.poc_entries.len > 0) allocator.free(state.poc_entries);
    if (state.rgn_entries.len > 0) allocator.free(state.rgn_entries);
    for (state.mct_segments) |*m| m.deinit(allocator);
    if (state.mct_segments.len > 0) allocator.free(state.mct_segments);
    for (state.mcc_collections) |*m| m.deinit(allocator);
    if (state.mcc_collections.len > 0) allocator.free(state.mcc_collections);
    if (state.mco) |*m| m.deinit(allocator);
    for (state.ppm_segments) |*p| p.deinit(allocator);
    if (state.ppm_segments.len > 0) allocator.free(state.ppm_segments);
    return header;
}

pub fn parseState(allocator: std.mem.Allocator, bytes: []const u8) !State {
    if (!hasSoc(bytes)) return error.MissingSocMarker;

    var offset: usize = 2;
    var parsed_header: ?Header = null;
    var coding_style: ?CodingStyle = null;
    var quantization_style: ?QuantizationStyle = null;
    var component_coding_styles: []?CodingStyle = &.{};
    var component_quantization_styles: []?QuantizationStyle = &.{};
    var comments: std.ArrayListUnmanaged(Comment) = .empty;
    errdefer {
        if (parsed_header) |*h| h.deinit(allocator);
        if (coding_style) |*cs| cs.deinit(allocator);
        if (quantization_style) |*q| q.deinit(allocator);
        for (component_coding_styles) |*cs| {
            if (cs.*) |*c| c.deinit(allocator);
        }
        if (component_coding_styles.len > 0) allocator.free(component_coding_styles);
        for (component_quantization_styles) |*qs| {
            if (qs.*) |*q| q.deinit(allocator);
        }
        if (component_quantization_styles.len > 0) allocator.free(component_quantization_styles);
        for (comments.items) |*c| c.deinit(allocator);
        comments.deinit(allocator);
    }
    var tile_parts: std.ArrayListUnmanaged(TilePart) = .empty;
    errdefer {
        for (tile_parts.items) |*tp| tp.deinit(allocator);
        tile_parts.deinit(allocator);
    }
    var plt_markers: std.ArrayListUnmanaged(PltMarker) = .empty;
    errdefer {
        for (plt_markers.items) |*p| p.deinit(allocator);
        plt_markers.deinit(allocator);
    }
    var current_plt: std.ArrayListUnmanaged(PltMarker) = .empty;
    errdefer {
        for (current_plt.items) |*p| p.deinit(allocator);
        current_plt.deinit(allocator);
    }
    var tile_part_lengths: std.ArrayListUnmanaged(TileTlmEntry) = .empty;
    errdefer tile_part_lengths.deinit(allocator);
    var poc_entries: std.ArrayListUnmanaged(PocEntry) = .empty;
    errdefer poc_entries.deinit(allocator);
    var rgn_entries: std.ArrayListUnmanaged(RgnEntry) = .empty;
    errdefer rgn_entries.deinit(allocator);
    var mct_segments: std.ArrayListUnmanaged(McTSegment) = .empty;
    errdefer {
        for (mct_segments.items) |*m| m.deinit(allocator);
        mct_segments.deinit(allocator);
    }
    var mcc_collections: std.ArrayListUnmanaged(McCCollection) = .empty;
    errdefer {
        for (mcc_collections.items) |*m| m.deinit(allocator);
        mcc_collections.deinit(allocator);
    }
    var mco_entry: ?McOOrdering = null;
    errdefer if (mco_entry) |*m| m.deinit(allocator);
    var ppm_segments: std.ArrayListUnmanaged(PpmSegment) = .empty;
    errdefer {
        for (ppm_segments.items) |*p| p.deinit(allocator);
        ppm_segments.deinit(allocator);
    }
    // PPT segments accumulate inside the current tile-part (between SOT and
    // next SOT / EOC). On close they are moved into tile_parts.items[idx].
    var current_ppt: std.ArrayListUnmanaged(PptSegment) = .empty;
    errdefer {
        for (current_ppt.items) |*p| p.deinit(allocator);
        current_ppt.deinit(allocator);
    }
    var current_poc: std.ArrayListUnmanaged(PocEntry) = .empty;
    errdefer current_poc.deinit(allocator);
    var has_sod = false;
    var has_eoc = false;
    var last_sot_offset: ?usize = null;
    var last_tile_part_length: u32 = 0;
    var in_tile_part: bool = false;
    var after_sod: bool = false;
    var expected_zppm: u16 = 0;
    var expected_zppt: u16 = 0;

    while (offset + 2 <= bytes.len) {
        const marker = std.mem.readInt(u16, @ptrCast(bytes[offset .. offset + 2].ptr), .big);
        if (marker == markers.siz) {
            parsed_header = try parseSiz(allocator, bytes[offset..]);
            const csiz = parsed_header.?.components.len;
            component_coding_styles = try allocator.alloc(?CodingStyle, csiz);
            @memset(component_coding_styles, null);
            component_quantization_styles = try allocator.alloc(?QuantizationStyle, csiz);
            @memset(component_quantization_styles, null);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.cod) {
            if (coding_style) |*old| old.deinit(allocator);
            coding_style = try parseCod(allocator, bytes[offset..]);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.coc) {
            const header = parsed_header orelse return error.MissingSizMarker;
            const result = try parseCoc(allocator, bytes[offset..], @intCast(header.components.len));
            if (result.index >= component_coding_styles.len) {
                var leaked = result.style;
                leaked.deinit(allocator);
                return error.InvalidComponentIndex;
            }
            if (component_coding_styles[result.index]) |*old| old.deinit(allocator);
            component_coding_styles[result.index] = result.style;
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.qcd) {
            if (in_tile_part and !after_sod) {
                if (tile_parts.items.len == 0) return error.UnexpectedTilePartMarker;
                const idx = tile_parts.items.len - 1;
                if (tile_parts.items[idx].quantization_style) |*old| old.deinit(allocator);
                tile_parts.items[idx].quantization_style = try parseQcd(allocator, bytes[offset..]);
            } else {
                if (quantization_style) |*old| old.deinit(allocator);
                quantization_style = try parseQcd(allocator, bytes[offset..]);
            }
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.qcc) {
            const header = parsed_header orelse return error.MissingSizMarker;
            const result = try parseQcc(allocator, bytes[offset..], @intCast(header.components.len));
            if (result.index >= component_quantization_styles.len) {
                var leaked = result.style;
                leaked.deinit(allocator);
                return error.InvalidComponentIndex;
            }
            if (component_quantization_styles[result.index]) |*old| old.deinit(allocator);
            component_quantization_styles[result.index] = result.style;
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.com) {
            try comments.append(allocator, try parseCom(allocator, bytes[offset..]));
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.plt) {
            const parsed_plt = try parsePlt(allocator, bytes[offset..]);
            errdefer {
                var owned = parsed_plt;
                owned.deinit(allocator);
            }
            if (in_tile_part and !after_sod) {
                const tile_lengths = try allocator.dupe(u32, parsed_plt.lengths);
                errdefer allocator.free(tile_lengths);
                try current_plt.append(allocator, .{
                    .zplt = parsed_plt.zplt,
                    .lengths = tile_lengths,
                });
            }
            try plt_markers.append(allocator, parsed_plt);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.tlm) {
            try parseTlm(allocator, bytes[offset..], &tile_part_lengths);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.poc) {
            const header = parsed_header orelse return error.MissingSizMarker;
            if (in_tile_part and !after_sod) {
                try parsePoc(allocator, bytes[offset..], @intCast(header.components.len), &current_poc);
            } else {
                try parsePoc(allocator, bytes[offset..], @intCast(header.components.len), &poc_entries);
            }
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.rgn) {
            const header = parsed_header orelse return error.MissingSizMarker;
            try parseRgn(allocator, bytes[offset..], @intCast(header.components.len), &rgn_entries);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.mct) {
            try parseMct(allocator, bytes[offset..], &mct_segments);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.mcc) {
            try parseMcc(allocator, bytes[offset..], &mcc_collections);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.mco) {
            if (mco_entry) |*old| old.deinit(allocator);
            mco_entry = try parseMco(allocator, bytes[offset..]);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.sot) {
            // Flush any PPT segments collected for the prior tile-part.
            if (in_tile_part and tile_parts.items.len > 0) {
                const idx = tile_parts.items.len - 1;
                tile_parts.items[idx].ppt_segments = try current_ppt.toOwnedSlice(allocator);
                tile_parts.items[idx].poc_entries = try current_poc.toOwnedSlice(allocator);
                tile_parts.items[idx].plt_markers = try current_plt.toOwnedSlice(allocator);
            }
            const tile_part = try parseSot(bytes[offset..]);
            last_sot_offset = offset;
            last_tile_part_length = tile_part.tile_part_length;
            try tile_parts.append(allocator, tile_part);
            in_tile_part = true;
            after_sod = false;
            expected_zppt = 0;
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.sod) {
            has_sod = true;
            after_sod = true;
            if (last_sot_offset != null and last_tile_part_length > 0) {
                const next_offset = last_sot_offset.? + last_tile_part_length;
                if (next_offset > offset and next_offset <= bytes.len) {
                    offset = next_offset;
                    continue;
                }
            }
            offset += 2;
            continue;
        }
        if (marker == markers.eoc) {
            has_eoc = true;
            offset += 2;
            break;
        }
        if (marker == markers.ppm) {
            // PPM is a main-header marker: must not appear after the first SOT.
            if (in_tile_part or last_sot_offset != null) return error.InvalidPpmLocation;
            const seg = try parsePpm(allocator, bytes[offset..]);
            if (seg.zppm != expected_zppm) {
                allocator.free(seg.payload);
                return error.InvalidPpmOrdering;
            }
            expected_zppm +%= 1;
            try ppm_segments.append(allocator, seg);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.ppt) {
            // PPT belongs inside a tile-part header: after SOT and before SOD.
            if (!in_tile_part or after_sod) return error.InvalidPptLocation;
            const seg = try parsePpt(allocator, bytes[offset..]);
            if (seg.zppt != expected_zppt) {
                allocator.free(seg.payload);
                return error.InvalidPptOrdering;
            }
            expected_zppt +%= 1;
            try current_ppt.append(allocator, seg);
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (markers.isStandalone(marker)) {
            offset += 2;
            continue;
        }
        if (offset + 4 > bytes.len) return error.TruncatedMarkerSegment;
        const segment_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
        if (segment_len < 2) return error.InvalidMarkerSegmentLength;
        if (offset + 2 + segment_len > bytes.len) return error.TruncatedMarkerSegment;
        offset += 2 + segment_len;
    }
    if (parsed_header == null) return error.MissingSizMarker;
    // PPM and PPT are mutually exclusive per tile (ISO A.7.3): if PPM exists
    // (covers all tiles) and any tile-part also carries PPT, reject. Check
    // before flushing trailing current_ppt (which moves ownership) and before
    // walking already-flushed tile_parts.
    if (ppm_segments.items.len > 0) {
        if (current_ppt.items.len > 0) return error.PpmPptConflict;
        for (tile_parts.items) |tp| {
            if (tp.ppt_segments.len > 0) return error.PpmPptConflict;
        }
    }
    // Flush trailing PPT segments into the final tile-part.
    if (in_tile_part and tile_parts.items.len > 0) {
        const idx = tile_parts.items.len - 1;
        tile_parts.items[idx].ppt_segments = try current_ppt.toOwnedSlice(allocator);
        tile_parts.items[idx].poc_entries = try current_poc.toOwnedSlice(allocator);
        tile_parts.items[idx].plt_markers = try current_plt.toOwnedSlice(allocator);
    }
    return .{
        .header = parsed_header.?,
        .coding_style = coding_style,
        .quantization_style = quantization_style,
        .component_coding_styles = component_coding_styles,
        .component_quantization_styles = component_quantization_styles,
        .comments = try comments.toOwnedSlice(allocator),
        .tile_parts = try tile_parts.toOwnedSlice(allocator),
        .plt_markers = try plt_markers.toOwnedSlice(allocator),
        .tile_part_lengths = try tile_part_lengths.toOwnedSlice(allocator),
        .poc_entries = try poc_entries.toOwnedSlice(allocator),
        .rgn_entries = try rgn_entries.toOwnedSlice(allocator),
        .mct_segments = try mct_segments.toOwnedSlice(allocator),
        .mcc_collections = try mcc_collections.toOwnedSlice(allocator),
        .mco = mco_entry,
        .ppm_segments = try ppm_segments.toOwnedSlice(allocator),
        .has_start_of_data = has_sod,
        .has_end_of_codestream = has_eoc,
    };
}

pub fn parseTilePartRanges(allocator: std.mem.Allocator, bytes: []const u8) ![]TilePartRange {
    if (!hasSoc(bytes)) return error.MissingSocMarker;

    var offset: usize = 2;
    var ranges: std.ArrayListUnmanaged(TilePartRange) = .empty;
    errdefer ranges.deinit(allocator);
    var current_sot_offset: ?usize = null;
    var current_tile_part: ?TilePart = null;

    while (offset + 2 <= bytes.len) {
        const marker = std.mem.readInt(u16, @ptrCast(bytes[offset .. offset + 2].ptr), .big);
        if (marker == markers.sot) {
            const tile_part = try parseSot(bytes[offset..]);
            current_sot_offset = offset;
            current_tile_part = tile_part;
            const seg_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
            offset += 2 + seg_len;
            continue;
        }
        if (marker == markers.sod) {
            const sot_offset = current_sot_offset orelse return error.UnexpectedStartOfData;
            const tile_part = current_tile_part orelse return error.UnexpectedStartOfData;
            const data_offset = offset + 2;
            const next_offset = if (tile_part.tile_part_length > 0)
                sot_offset + tile_part.tile_part_length
            else
                findNextTilePartOrEoc(bytes, data_offset);
            if (next_offset < data_offset or next_offset > bytes.len) return error.InvalidTilePartLength;
            try ranges.append(allocator, .{
                .tile_index = tile_part.tile_index,
                .tile_part_index = tile_part.tile_part_index,
                .num_tile_parts = tile_part.num_tile_parts,
                .sot_offset = sot_offset,
                .sod_offset = offset,
                .data_offset = data_offset,
                .data_length = next_offset - data_offset,
                .next_offset = next_offset,
            });
            offset = next_offset;
            current_sot_offset = null;
            current_tile_part = null;
            continue;
        }
        if (marker == markers.eoc) {
            break;
        }
        if (markers.isStandalone(marker)) {
            offset += 2;
            continue;
        }
        if (offset + 4 > bytes.len) return error.TruncatedMarkerSegment;
        const segment_len: usize = std.mem.readInt(u16, @ptrCast(bytes[offset + 2 .. offset + 4].ptr), .big);
        if (segment_len < 2) return error.InvalidMarkerSegmentLength;
        if (offset + 2 + segment_len > bytes.len) return error.TruncatedMarkerSegment;
        offset += 2 + segment_len;
    }

    return try ranges.toOwnedSlice(allocator);
}

fn parseSiz(allocator: std.mem.Allocator, bytes: []const u8) !Header {
    if (bytes.len < 43) return error.TruncatedSizMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.siz) return error.ExpectedSizMarker;
    const lsiz = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lsiz < 38 or bytes.len < 2 + lsiz) return error.TruncatedSizMarker;

    const xsiz = std.mem.readInt(u32, @ptrCast(bytes[6..10].ptr), .big);
    const ysiz = std.mem.readInt(u32, @ptrCast(bytes[10..14].ptr), .big);
    const xosiz = std.mem.readInt(u32, @ptrCast(bytes[14..18].ptr), .big);
    const yosiz = std.mem.readInt(u32, @ptrCast(bytes[18..22].ptr), .big);
    const xtsiz = std.mem.readInt(u32, @ptrCast(bytes[22..26].ptr), .big);
    const ytsiz = std.mem.readInt(u32, @ptrCast(bytes[26..30].ptr), .big);
    const xtosiz = std.mem.readInt(u32, @ptrCast(bytes[30..34].ptr), .big);
    const ytosiz = std.mem.readInt(u32, @ptrCast(bytes[34..38].ptr), .big);
    const csiz = std.mem.readInt(u16, @ptrCast(bytes[38..40].ptr), .big);
    if (csiz == 0) return error.InvalidComponentCount;

    const expected_len: usize = 38 + (@as(usize, csiz) * 3);
    if (lsiz < expected_len) return error.TruncatedSizMarker;

    const components = try allocator.alloc(Component, csiz);
    errdefer allocator.free(components);
    var i: usize = 0;
    var comp_offset: usize = 40;
    while (i < csiz) : (i += 1) {
        const ssiz = bytes[comp_offset];
        components[i] = .{
            .bits_per_component = (ssiz & 0x7f) + 1,
            .is_signed = (ssiz & 0x80) != 0,
            .xrsiz = bytes[comp_offset + 1],
            .yrsiz = bytes[comp_offset + 2],
        };
        comp_offset += 3;
    }

    if (xosiz > xsiz or yosiz > ysiz) return error.InvalidSizOffsets;
    const width = xsiz - xosiz;
    const height = ysiz - yosiz;
    const tile_cols = if (xtsiz == 0 or xtosiz >= xsiz) 1 else (xsiz - xtosiz + xtsiz - 1) / xtsiz;
    const tile_rows = if (ytsiz == 0 or ytosiz >= ysiz) 1 else (ysiz - ytosiz + ytsiz - 1) / ytsiz;
    return .{
        .width = width,
        .height = height,
        .x_offset = xosiz,
        .y_offset = yosiz,
        .components = components,
        .tile_width = xtsiz,
        .tile_height = ytsiz,
        .tile_x_offset = xtosiz,
        .tile_y_offset = ytosiz,
        .uses_multiple_tiles = tile_cols * tile_rows > 1,
    };
}

fn parseCod(allocator: std.mem.Allocator, bytes: []const u8) !CodingStyle {
    if (bytes.len < 14) return error.TruncatedCodMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.cod) return error.ExpectedCodMarker;
    const lcod = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lcod < 12 or bytes.len < 2 + lcod) return error.TruncatedCodMarker;
    const scod = bytes[4];
    const precincts_present = (scod & 0x01) != 0;
    const decomposition_levels = bytes[9];

    var precinct_sizes: ?[]u8 = null;
    errdefer if (precinct_sizes) |s| allocator.free(s);
    if (precincts_present) {
        const count: usize = @as(usize, decomposition_levels) + 1;
        if (2 + lcod < 14 + count) return error.TruncatedCodMarker;
        precinct_sizes = try allocator.alloc(u8, count);
        for (0..count) |i| {
            precinct_sizes.?[i] = bytes[14 + i];
        }
    }

    const cb_width_exp: u8 = bytes[10] & 0x0f;
    const cb_height_exp: u8 = bytes[11] & 0x0f;
    try validateCodeBlockExponents(cb_width_exp, cb_height_exp);

    return .{
        .progression_order = bytes[5],
        .num_layers = std.mem.readInt(u16, @ptrCast(bytes[6..8].ptr), .big),
        .multiple_component_transform = bytes[8] != 0,
        .decomposition_levels = decomposition_levels,
        .code_block_width_exponent = cb_width_exp,
        .code_block_height_exponent = cb_height_exp,
        .code_block_style = bytes[12],
        .wavelet_transform = bytes[13],
        .precincts_present = precincts_present,
        .scod = scod,
        .precinct_sizes = precinct_sizes,
    };
}

/// ISO 15444-1 Table A-18 stores code-block dimensions as 2^(exp + 2), with
/// stored exponents in [0, 8] and exp_w + exp_h <= 8.
fn validateCodeBlockExponents(width_exp: u8, height_exp: u8) !void {
    if (width_exp > 8 or height_exp > 8) return error.InvalidCodeBlockSize;
    if (@as(u16, width_exp) + @as(u16, height_exp) > 8) return error.InvalidCodeBlockSize;
}

fn parseCoc(allocator: std.mem.Allocator, bytes: []const u8, csiz: u16) !struct { index: u16, style: CodingStyle } {
    if (bytes.len < 4) return error.TruncatedCocMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.coc) return error.ExpectedCocMarker;
    const lcoc = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (bytes.len < 2 + lcoc) return error.TruncatedCocMarker;

    const wide_index = csiz > 256;
    const index_size: usize = if (wide_index) 2 else 1;
    // ISO 15444-1 Table A-16: Lcoc = 2 (self) + Ccoc (index_size) + Scoc (1) + SPcoc
    // SPcoc minimum = num_decomp + cbw + cbh + cbstyle + transform = 5 bytes.
    const min_len: usize = 2 + index_size + 1 + 5;
    if (lcoc < min_len) return error.TruncatedCocMarker;

    const comp_index: u16 = if (wide_index)
        std.mem.readInt(u16, @ptrCast(bytes[4..6].ptr), .big)
    else
        bytes[4];
    const base = 4 + index_size;
    const scoc = bytes[base];
    const precincts_present = (scoc & 0x01) != 0;
    const decomposition_levels = bytes[base + 1];

    var precinct_sizes: ?[]u8 = null;
    errdefer if (precinct_sizes) |s| allocator.free(s);
    if (precincts_present) {
        const count: usize = @as(usize, decomposition_levels) + 1;
        const needed = base + 6 + count;
        if (2 + lcoc < needed) return error.TruncatedCocMarker;
        precinct_sizes = try allocator.alloc(u8, count);
        for (0..count) |i| {
            precinct_sizes.?[i] = bytes[base + 6 + i];
        }
    }

    const cb_width_exp: u8 = bytes[base + 2] & 0x0f;
    const cb_height_exp: u8 = bytes[base + 3] & 0x0f;
    try validateCodeBlockExponents(cb_width_exp, cb_height_exp);

    return .{
        .index = comp_index,
        .style = .{
            .progression_order = 0,
            .num_layers = 0,
            .multiple_component_transform = false,
            .decomposition_levels = decomposition_levels,
            .code_block_width_exponent = cb_width_exp,
            .code_block_height_exponent = cb_height_exp,
            .code_block_style = bytes[base + 4],
            .wavelet_transform = bytes[base + 5],
            .precincts_present = precincts_present,
            .precinct_sizes = precinct_sizes,
        },
    };
}

fn parseQcc(allocator: std.mem.Allocator, bytes: []const u8, csiz: u16) !struct { index: u16, style: QuantizationStyle } {
    if (bytes.len < 4) return error.TruncatedQccMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.qcc) return error.ExpectedQccMarker;
    const lqcc = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (bytes.len < 2 + lqcc) return error.TruncatedQccMarker;

    const wide_index = csiz > 256;
    const index_size: usize = if (wide_index) 2 else 1;
    const min_len: usize = 2 + index_size + 1;
    if (lqcc < min_len) return error.TruncatedQccMarker;

    const comp_index: u16 = if (wide_index)
        std.mem.readInt(u16, @ptrCast(bytes[4..6].ptr), .big)
    else
        bytes[4];
    const base = 4 + index_size;
    const sqcc = bytes[base];
    const style = sqcc & 0x1f;
    const guard_bits = sqcc >> 5;
    const payload = bytes[base + 1 .. 2 + lqcc];

    const step_width: usize = switch (style) {
        0 => 1,
        1, 2 => 2,
        else => return error.InvalidQuantizationSegment,
    };
    if (payload.len == 0 or payload.len % step_width != 0) return error.InvalidQuantizationSegment;

    const steps = try allocator.alloc(u16, payload.len / step_width);
    errdefer allocator.free(steps);
    for (steps, 0..) |*step, idx| {
        const start = idx * step_width;
        step.* = switch (step_width) {
            1 => payload[start],
            2 => std.mem.readInt(u16, @ptrCast(payload[start .. start + 2].ptr), .big),
            else => unreachable,
        };
    }

    return .{
        .index = comp_index,
        .style = .{
            .style = style,
            .guard_bits = guard_bits,
            .step_values = steps,
        },
    };
}

fn parseQcd(allocator: std.mem.Allocator, bytes: []const u8) !QuantizationStyle {
    if (bytes.len < 5) return error.TruncatedQcdMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.qcd) return error.ExpectedQcdMarker;
    const lqcd = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lqcd < 3 or bytes.len < 2 + lqcd) return error.TruncatedQcdMarker;

    const sqcd = bytes[4];
    const style = sqcd & 0x1f;
    const guard_bits = sqcd >> 5;
    const payload = bytes[5 .. 2 + lqcd];

    const step_width: usize = switch (style) {
        0 => 1,
        1, 2 => 2,
        else => return error.InvalidQuantizationSegment,
    };
    if (payload.len == 0 or payload.len % step_width != 0) return error.InvalidQuantizationSegment;

    const steps = try allocator.alloc(u16, payload.len / step_width);
    errdefer allocator.free(steps);
    for (steps, 0..) |*step, idx| {
        const start = idx * step_width;
        step.* = switch (step_width) {
            1 => payload[start],
            2 => std.mem.readInt(u16, @ptrCast(payload[start .. start + 2].ptr), .big),
            else => unreachable,
        };
    }
    return .{
        .style = style,
        .guard_bits = guard_bits,
        .step_values = steps,
    };
}

fn parseCom(allocator: std.mem.Allocator, bytes: []const u8) !Comment {
    if (bytes.len < 6) return error.TruncatedComMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.com) return error.ExpectedComMarker;
    const lcom: usize = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lcom < 4 or bytes.len < 2 + lcom) return error.TruncatedComMarker;
    const registration = std.mem.readInt(u16, @ptrCast(bytes[4..6].ptr), .big);
    const text = try allocator.dupe(u8, bytes[6 .. 2 + lcom]);
    return .{
        .registration = registration,
        .text = text,
    };
}

fn parsePlt(allocator: std.mem.Allocator, bytes: []const u8) !PltMarker {
    if (bytes.len < 5) return error.TruncatedPltMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.plt) return error.ExpectedPltMarker;
    const lplt = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lplt < 3 or bytes.len < 2 + lplt) return error.TruncatedPltMarker;
    const zplt = bytes[4];
    const payload = bytes[5 .. 2 + lplt];

    var lengths: std.ArrayListUnmanaged(u32) = .empty;
    errdefer lengths.deinit(allocator);
    // Variable-length: each byte contributes 7 bits (MSB is continuation).
    var accumulator: u32 = 0;
    for (payload) |byte| {
        accumulator = (accumulator << 7) | (byte & 0x7f);
        if ((byte & 0x80) == 0) {
            try lengths.append(allocator, accumulator);
            accumulator = 0;
        }
    }
    if (accumulator != 0) return error.TruncatedPltMarker;
    return .{
        .zplt = zplt,
        .lengths = try lengths.toOwnedSlice(allocator),
    };
}

fn parseTlm(allocator: std.mem.Allocator, bytes: []const u8, out: *std.ArrayListUnmanaged(TileTlmEntry)) !void {
    if (bytes.len < 6) return error.TruncatedTlmMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.tlm) return error.ExpectedTlmMarker;
    const ltlm = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (ltlm < 4 or bytes.len < 2 + ltlm) return error.TruncatedTlmMarker;
    // bytes[4] = Ztlm (index, ignored for MVP reassembly)
    const stlm = bytes[5];
    const st: u8 = (stlm >> 4) & 0x03;
    const sp: u8 = (stlm >> 6) & 0x01;
    const tile_bytes: usize = switch (st) {
        0 => 0, // Ttlm absent (implied tile order)
        1 => 1,
        2 => 2,
        else => return error.InvalidTlmMarker,
    };
    const len_bytes: usize = if (sp == 0) 2 else 4;
    const entry_bytes = tile_bytes + len_bytes;
    if (entry_bytes == 0) return error.InvalidTlmMarker;
    const payload = bytes[6 .. 2 + ltlm];
    if (payload.len % entry_bytes != 0) return error.InvalidTlmMarker;
    var i: usize = 0;
    while (i < payload.len) : (i += entry_bytes) {
        var tile_index: ?u16 = null;
        var off: usize = i;
        if (tile_bytes == 1) {
            tile_index = payload[off];
            off += 1;
        } else if (tile_bytes == 2) {
            tile_index = std.mem.readInt(u16, @ptrCast(payload[off .. off + 2].ptr), .big);
            off += 2;
        }
        const length: u32 = if (len_bytes == 2)
            std.mem.readInt(u16, @ptrCast(payload[off .. off + 2].ptr), .big)
        else
            std.mem.readInt(u32, @ptrCast(payload[off .. off + 4].ptr), .big);
        try out.append(allocator, .{ .tile_index = tile_index, .tile_part_length = length });
    }
}

fn parsePoc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    csiz: u16,
    out: *std.ArrayListUnmanaged(PocEntry),
) !void {
    if (bytes.len < 4) return error.TruncatedPocMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.poc) return error.ExpectedPocMarker;
    const lpoc = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lpoc < 2 or bytes.len < 2 + lpoc) return error.TruncatedPocMarker;

    const wide_comp = csiz >= 256;
    const comp_bytes: usize = if (wide_comp) 2 else 1;
    // Per-entry size: RSpoc(1) + CSpoc(comp) + LYEpoc(2) + REpoc(1) + CEpoc(comp) + Ppoc(1)
    const entry_size: usize = 1 + comp_bytes + 2 + 1 + comp_bytes + 1;
    const payload_len: usize = lpoc - 2;
    if (payload_len == 0 or payload_len % entry_size != 0) return error.InvalidPocMarker;

    var cursor: usize = 4;
    const end = 2 + @as(usize, lpoc);
    while (cursor < end) {
        const rs_poc = bytes[cursor];
        cursor += 1;
        const cs_poc: u16 = if (wide_comp)
            std.mem.readInt(u16, @ptrCast(bytes[cursor .. cursor + 2].ptr), .big)
        else
            bytes[cursor];
        cursor += comp_bytes;
        const lye_poc = std.mem.readInt(u16, @ptrCast(bytes[cursor .. cursor + 2].ptr), .big);
        cursor += 2;
        const re_poc = bytes[cursor];
        cursor += 1;
        const ce_poc: u16 = if (wide_comp)
            std.mem.readInt(u16, @ptrCast(bytes[cursor .. cursor + 2].ptr), .big)
        else
            bytes[cursor];
        cursor += comp_bytes;
        const progression_order = bytes[cursor];
        cursor += 1;

        if (re_poc <= rs_poc) return error.InvalidPocMarker;
        if (ce_poc <= cs_poc) return error.InvalidPocMarker;
        if (lye_poc == 0) return error.InvalidPocMarker;
        if (progression_order > 4) return error.InvalidPocMarker;

        try out.append(allocator, .{
            .rs_poc = rs_poc,
            .cs_poc = cs_poc,
            .lye_poc = lye_poc,
            .re_poc = re_poc,
            .ce_poc = ce_poc,
            .progression_order = progression_order,
        });
    }
}

fn parseRgn(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    csiz: u16,
    out: *std.ArrayListUnmanaged(RgnEntry),
) !void {
    if (bytes.len < 4) return error.TruncatedRgnMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.rgn) return error.ExpectedRgnMarker;
    const lrgn = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lrgn < 5 or bytes.len < 2 + lrgn) return error.TruncatedRgnMarker;

    const wide_comp = csiz >= 257;
    const comp_bytes: usize = if (wide_comp) 2 else 1;
    const expected_len: usize = 2 + comp_bytes + 1 + 1; // Lrgn + Crgn + Srgn + SPrgn
    if (lrgn != expected_len) return error.InvalidRgnMarker;

    var cursor: usize = 4;
    const component: u16 = if (wide_comp)
        std.mem.readInt(u16, @ptrCast(bytes[cursor .. cursor + 2].ptr), .big)
    else
        bytes[cursor];
    cursor += comp_bytes;
    const style = bytes[cursor];
    cursor += 1;
    const shift = bytes[cursor];

    if (component >= csiz) return error.InvalidComponentIndex;
    // Only style 0 (implicit max-shift) is defined in Part 1.
    if (style != 0) return error.UnsupportedRgnStyle;

    try out.append(allocator, .{
        .component = component,
        .style = style,
        .shift = shift,
    });
}

/// Parse an MCT marker segment (ISO 15444-1 A.3.8). Segment layout:
///   FF74 Lmct Zmct Imct Ymct Dmct*
/// Minimum Lmct is 2 (Lmct) + 2 (Zmct) + 2 (Imct) + 2 (Ymct) = 8 bytes after the marker.
fn parseMct(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    out: *std.ArrayListUnmanaged(McTSegment),
) !void {
    if (bytes.len < 4) return error.TruncatedMctMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.mct) return error.ExpectedMctMarker;
    const lmct = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lmct < 8) return error.TruncatedMctMarker;
    if (bytes.len < 2 + @as(usize, lmct)) return error.TruncatedMctMarker;
    const imct = std.mem.readInt(u16, @ptrCast(bytes[6..8].ptr), .big);
    // Ymct at bytes[8..10] is the continuation/chain index; preserved implicitly
    // by storing the full Dmct payload. `element_type` pulls the 2-bit coefficient
    // type from Imct bits 10-11 (per ISO 15444-2 MCT extension).
    const element_type: u8 = @intCast((imct >> 10) & 0x3);
    const index: u16 = imct & 0xff;
    const payload_start: usize = 10;
    const payload_end: usize = 2 + @as(usize, lmct);
    const payload = try allocator.alloc(u8, payload_end - payload_start);
    errdefer allocator.free(payload);
    @memcpy(payload, bytes[payload_start..payload_end]);
    try out.append(allocator, .{
        .index = index,
        .element_type = element_type,
        .payload = payload,
    });
}

/// Parse an MCC marker segment (ISO 15444-1 A.3.9). Segment layout:
///   FF75 Lmcc Zmcc Imcc Ymcc Qmcc...
/// For MVP we persist the payload bytes after Zmcc/Imcc/Ymcc verbatim.
fn parseMcc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    out: *std.ArrayListUnmanaged(McCCollection),
) !void {
    if (bytes.len < 4) return error.TruncatedMccMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.mcc) return error.ExpectedMccMarker;
    const lmcc = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lmcc < 8) return error.TruncatedMccMarker;
    if (bytes.len < 2 + @as(usize, lmcc)) return error.TruncatedMccMarker;
    const imcc = std.mem.readInt(u16, @ptrCast(bytes[6..8].ptr), .big);
    const index: u16 = imcc & 0xff;
    const payload_start: usize = 10;
    const payload_end: usize = 2 + @as(usize, lmcc);
    const payload = try allocator.alloc(u8, payload_end - payload_start);
    errdefer allocator.free(payload);
    @memcpy(payload, bytes[payload_start..payload_end]);
    try out.append(allocator, .{
        .index = index,
        .payload = payload,
    });
}

/// Parse an MCO marker segment (ISO 15444-1 A.3.10). Layout:
///   FF77 Lmco Nmco Imco_0 Imco_1 ... Imco_{Nmco-1}
/// Each Imco is a 1-byte MCC identifier. Zero Nmco is accepted.
fn parseMco(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) !McOOrdering {
    if (bytes.len < 4) return error.TruncatedMcoMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.mco) return error.ExpectedMcoMarker;
    const lmco = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lmco < 3) return error.TruncatedMcoMarker;
    if (bytes.len < 2 + @as(usize, lmco)) return error.TruncatedMcoMarker;
    const nmco = bytes[4];
    const expected_len: usize = 3 + @as(usize, nmco);
    if (lmco != expected_len) return error.InvalidMcoMarker;
    const ids = try allocator.alloc(u16, nmco);
    errdefer allocator.free(ids);
    var i: usize = 0;
    while (i < nmco) : (i += 1) {
        ids[i] = bytes[5 + i];
    }
    return .{ .ids = ids };
}

/// Parse a PPM marker segment (ISO 15444-1 A.7.2). Layout:
///   FF60 Lppm Zppm Nppm_0 Ippm_0... Nppm_1 Ippm_1...
/// We capture the bytes after Zppm verbatim; the (Nppm,Ippm) pairs are walked
/// at consumption time, not parse time.
fn parsePpm(allocator: std.mem.Allocator, bytes: []const u8) !PpmSegment {
    if (bytes.len < 4) return error.InvalidPpmSegment;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.ppm) return error.InvalidPpmSegment;
    const lppm: usize = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    // Lppm covers Lppm(2) + Zppm(1) + payload, so minimum is 3 (zero payload).
    if (lppm < 3) return error.InvalidPpmSegment;
    if (bytes.len < 2 + lppm) return error.InvalidPpmSegment;
    const zppm = bytes[4];
    const payload_start: usize = 5;
    const payload_end: usize = 2 + lppm;
    const payload = try allocator.alloc(u8, payload_end - payload_start);
    errdefer allocator.free(payload);
    @memcpy(payload, bytes[payload_start..payload_end]);
    return .{ .zppm = zppm, .payload = payload };
}

/// Parse a PPT marker segment (ISO 15444-1 A.7.3). Layout:
///   FF61 Lppt Zppt Ippt...
/// Like PPM, Ippt is captured raw; tier-2 walks the packet-header bitstream
/// later.
fn parsePpt(allocator: std.mem.Allocator, bytes: []const u8) !PptSegment {
    if (bytes.len < 4) return error.InvalidPptSegment;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.ppt) return error.InvalidPptSegment;
    const lppt: usize = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lppt < 3) return error.InvalidPptSegment;
    if (bytes.len < 2 + lppt) return error.InvalidPptSegment;
    const zppt = bytes[4];
    const payload_start: usize = 5;
    const payload_end: usize = 2 + lppt;
    const payload = try allocator.alloc(u8, payload_end - payload_start);
    errdefer allocator.free(payload);
    @memcpy(payload, bytes[payload_start..payload_end]);
    return .{ .zppt = zppt, .payload = payload };
}

fn parseSot(bytes: []const u8) !TilePart {
    if (bytes.len < 12) return error.TruncatedSotMarker;
    if (std.mem.readInt(u16, @ptrCast(bytes[0..2].ptr), .big) != markers.sot) return error.ExpectedSotMarker;
    const lsot = std.mem.readInt(u16, @ptrCast(bytes[2..4].ptr), .big);
    if (lsot != 10 or bytes.len < 12) return error.TruncatedSotMarker;
    return .{
        .tile_index = std.mem.readInt(u16, @ptrCast(bytes[4..6].ptr), .big),
        .tile_part_length = std.mem.readInt(u32, @ptrCast(bytes[6..10].ptr), .big),
        .tile_part_index = bytes[10],
        .num_tile_parts = bytes[11],
    };
}

fn findNextTilePartOrEoc(bytes: []const u8, start: usize) usize {
    var offset = start;
    while (offset + 2 <= bytes.len) : (offset += 1) {
        if (bytes[offset] != 0xff) continue;
        const marker = std.mem.readInt(u16, @ptrCast(bytes[offset .. offset + 2].ptr), .big);
        if (marker == markers.sot or marker == markers.eoc) return offset;
    }
    return bytes.len;
}

test "parse raw codestream siz header" {
    const allocator = std.testing.allocator;
    const bytes = [_]u8{
        0xff, 0x4f,
        0xff, 0x51,
        0x00, 0x29,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x10,
        0x00, 0x00,
        0x00, 0x20,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x10,
        0x00, 0x00,
        0x00, 0x20,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x01,
        0x07, 0x01,
        0x01,
    };
    var header = try parseHeader(allocator, bytes[0..]);
    defer header.deinit(allocator);
    try std.testing.expectEqual(@as(u32, 16), header.width);
    try std.testing.expectEqual(@as(u32, 32), header.height);
    try std.testing.expectEqual(@as(usize, 1), header.components.len);
    try std.testing.expectEqual(@as(u8, 8), header.components[0].bits_per_component);
}

test "parse codestream state with cod qcd com sot markers" {
    const allocator = std.testing.allocator;
    const bytes = [_]u8{
        0xff, 0x4f,
        0xff, 0x51,
        0x00, 0x2f,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x02,
        0x00, 0x00,
        0x00, 0x01,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x02,
        0x00, 0x00,
        0x00, 0x01,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x03,
        0x07, 0x01,
        0x01, 0x07,
        0x01, 0x01,
        0x07, 0x01,
        0x01, 0xff,
        0x52, 0x00,
        0x0c, 0x00,
        0x00, 0x00,
        0x01, 0x00,
        0x00, 0x04,
        0x04, 0x00,
        0x01, 0xff,
        0x5c, 0x00,
        0x04, 0x40,
        0x40, 0xff,
        0x64, 0x00,
        0x08, 0x00,
        0x01, 't',
        'e',  's',
        't',  0xff,
        0x90, 0x00,
        0x0a, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x12, 0x00,
        0x01, 0xff,
        0x93, 0x00,
        0x01, 0x02,
        0x03, 0xff,
        0xd9,
    };
    var state = try parseState(allocator, bytes[0..]);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(u32, 2), state.header.width);
    try std.testing.expect(state.coding_style != null);
    try std.testing.expectEqual(@as(u16, 1), state.coding_style.?.num_layers);
    try std.testing.expect(state.quantization_style != null);
    try std.testing.expectEqual(@as(usize, 1), state.quantization_style.?.step_values.len);
    try std.testing.expectEqual(@as(usize, 1), state.comments.len);
    try std.testing.expectEqualStrings("test", state.comments[0].text);
    try std.testing.expectEqual(@as(usize, 1), state.tile_parts.len);
    try std.testing.expectEqual(@as(u32, 18), state.tile_parts[0].tile_part_length);
    try std.testing.expect(state.has_start_of_data);
    try std.testing.expect(state.has_end_of_codestream);
    try std.testing.expectEqual(.supported, state.fullNativeDecodeSupport());
}

// A minimal single-component header containing SOC, a SIZ segment, and a POC
// segment with `poc_payload` appended. Csiz = 1 so POC component fields are
// 1 byte each. The POC Lpoc field is set to `poc_payload.len + 2`.
fn buildCodestreamWithPoc(
    allocator: std.mem.Allocator,
    poc_payload: []const u8,
) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    // SOC
    try out.appendSlice(allocator, &.{ 0xff, 0x4f });
    // SIZ: 1 component, 4x4, 8bpc.
    const siz_body = [_]u8{
        0xff, 0x51, // marker
        0x00, 0x29, // Lsiz = 41 (38 + 3)
        0x00, 0x00, // Rsiz
        0x00, 0x00, 0x00, 0x04, // Xsiz
        0x00, 0x00, 0x00, 0x04, // Ysiz
        0x00, 0x00, 0x00, 0x00, // XOsiz
        0x00, 0x00, 0x00, 0x00, // YOsiz
        0x00, 0x00, 0x00, 0x04, // XTsiz
        0x00, 0x00, 0x00, 0x04, // YTsiz
        0x00, 0x00, 0x00, 0x00, // XTOsiz
        0x00, 0x00, 0x00, 0x00, // YTOsiz
        0x00, 0x01, // Csiz
        0x07, 0x01, 0x01, // Ssiz, XRsiz, YRsiz
    };
    try out.appendSlice(allocator, &siz_body);
    // POC
    try out.appendSlice(allocator, &.{ 0xff, 0x5f });
    const lpoc: u16 = @intCast(2 + poc_payload.len);
    var len_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &len_buf, lpoc, .big);
    try out.appendSlice(allocator, &len_buf);
    try out.appendSlice(allocator, poc_payload);
    // EOC
    try out.appendSlice(allocator, &.{ 0xff, 0xd9 });
    return out.toOwnedSlice(allocator);
}

test "POC segment with two entries parses" {
    const allocator = std.testing.allocator;
    // Two 7-byte entries (csiz=1, so comp fields are 1 byte).
    // Entry layout: RSpoc(1) CSpoc(1) LYEpoc(2) REpoc(1) CEpoc(1) Ppoc(1) = 7 bytes
    const payload = [_]u8{
        // Entry 0: rs=0 cs=0 lye=1 re=2 ce=1 order=0
        0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0x00,
        // Entry 1: rs=1 cs=0 lye=1 re=3 ce=1 order=2
        0x01, 0x00, 0x00, 0x01, 0x03, 0x01, 0x02,
    };
    const bytes = try buildCodestreamWithPoc(allocator, &payload);
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), state.poc_entries.len);
    try std.testing.expectEqual(@as(u8, 0), state.poc_entries[0].rs_poc);
    try std.testing.expectEqual(@as(u8, 2), state.poc_entries[0].re_poc);
    try std.testing.expectEqual(@as(u16, 1), state.poc_entries[0].lye_poc);
    try std.testing.expectEqual(@as(u8, 1), state.poc_entries[1].rs_poc);
    try std.testing.expectEqual(@as(u8, 2), state.poc_entries[1].progression_order);
}

test "POC entry with re <= rs errors at parse" {
    const allocator = std.testing.allocator;
    // One entry: rs=2 re=2 (invalid: re must be > rs).
    const payload = [_]u8{ 0x02, 0x00, 0x00, 0x01, 0x02, 0x01, 0x00 };
    const bytes = try buildCodestreamWithPoc(allocator, &payload);
    defer allocator.free(bytes);
    try std.testing.expectError(error.InvalidPocMarker, parseState(allocator, bytes));
}

test "multiple POC segments accumulate entries" {
    const allocator = std.testing.allocator;
    var buffer: std.ArrayListUnmanaged(u8) = .empty;
    defer buffer.deinit(allocator);
    const first_payload = [_]u8{ 0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0x00 };
    const base = try buildCodestreamWithPoc(allocator, &first_payload);
    defer allocator.free(base);
    // Drop the trailing EOC (last two bytes) and append a second POC + EOC.
    try buffer.appendSlice(allocator, base[0 .. base.len - 2]);
    // Second POC: one entry rs=1 re=2 order=1.
    try buffer.appendSlice(allocator, &.{ 0xff, 0x5f });
    try buffer.appendSlice(allocator, &.{ 0x00, 0x09 }); // Lpoc = 2 + 7
    try buffer.appendSlice(allocator, &.{ 0x01, 0x00, 0x00, 0x01, 0x02, 0x01, 0x01 });
    try buffer.appendSlice(allocator, &.{ 0xff, 0xd9 });

    var state = try parseState(allocator, buffer.items);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), state.poc_entries.len);
    try std.testing.expectEqual(@as(u8, 0), state.poc_entries[0].progression_order);
    try std.testing.expectEqual(@as(u8, 1), state.poc_entries[1].progression_order);
    try std.testing.expectEqual(@as(u8, 1), state.poc_entries[1].rs_poc);
}

// Minimal single-component header (SOC + SIZ) with an RGN segment appended
// before EOC. Csiz = 1 so Crgn is a single byte.
fn buildCodestreamWithRgn(
    allocator: std.mem.Allocator,
    rgn_payload: []const u8,
) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, &.{ 0xff, 0x4f });
    const siz_body = [_]u8{
        0xff, 0x51,
        0x00, 0x29,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x01,
        0x07, 0x01,
        0x01,
    };
    try out.appendSlice(allocator, &siz_body);
    try out.appendSlice(allocator, &.{ 0xff, 0x5e });
    const lrgn: u16 = @intCast(2 + rgn_payload.len);
    var len_buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &len_buf, lrgn, .big);
    try out.appendSlice(allocator, &len_buf);
    try out.appendSlice(allocator, rgn_payload);
    try out.appendSlice(allocator, &.{ 0xff, 0xd9 });
    return out.toOwnedSlice(allocator);
}

test "RGN marker implicit max-shift parses round-trip" {
    const allocator = std.testing.allocator;
    // Crgn=0, Srgn=0, SPrgn=5
    const payload = [_]u8{ 0x00, 0x00, 0x05 };
    const bytes = try buildCodestreamWithRgn(allocator, &payload);
    defer allocator.free(bytes);

    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.rgn_entries.len);
    try std.testing.expectEqual(@as(u16, 0), state.rgn_entries[0].component);
    try std.testing.expectEqual(@as(u8, 0), state.rgn_entries[0].style);
    try std.testing.expectEqual(@as(u8, 5), state.rgn_entries[0].shift);
}

test "RGN marker with unknown style is rejected" {
    const allocator = std.testing.allocator;
    // Crgn=0, Srgn=1 (Part 1 only defines style 0), SPrgn=3
    const payload = [_]u8{ 0x00, 0x01, 0x03 };
    const bytes = try buildCodestreamWithRgn(allocator, &payload);
    defer allocator.free(bytes);
    try std.testing.expectError(error.UnsupportedRgnStyle, parseState(allocator, bytes));
}

test "RGN marker with out-of-range component index is rejected" {
    const allocator = std.testing.allocator;
    // Csiz=1, request component index 5 -> invalid.
    const payload = [_]u8{ 0x05, 0x00, 0x03 };
    const bytes = try buildCodestreamWithRgn(allocator, &payload);
    defer allocator.free(bytes);
    try std.testing.expectError(error.InvalidComponentIndex, parseState(allocator, bytes));
}

fn buildMinimalCodestreamWithBytes(
    allocator: std.mem.Allocator,
    extra: []const u8,
) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, &.{ 0xff, 0x4f }); // SOC
    const siz_body = [_]u8{
        0xff, 0x51,
        0x00, 0x29,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x01,
        0x07, 0x01,
        0x01,
    };
    try out.appendSlice(allocator, &siz_body);
    try out.appendSlice(allocator, extra);
    try out.appendSlice(allocator, &.{ 0xff, 0xd9 }); // EOC
    return out.toOwnedSlice(allocator);
}

test "parseMct rejects truncated segment" {
    const allocator = std.testing.allocator;
    // Lmct claims 10 bytes (Lmct=2 + Zmct=2 + Imct=2 + Ymct=2 + 2 data),
    // but only 6 bytes follow the Lmct field -> should reject.
    // Build directly (bypass parseState) because parseState enforces outer length.
    const bytes = [_]u8{
        0xff, 0x74, // MCT marker
        0x00, 0x0a, // Lmct = 10
        0x00, 0x00, // Zmct
        0x00, 0x00, // Imct
        0x00, 0x00, // Ymct
        // (missing 2 bytes of Dmct)
    };
    var out: std.ArrayListUnmanaged(McTSegment) = .empty;
    defer {
        for (out.items) |*m| m.deinit(allocator);
        out.deinit(allocator);
    }
    try std.testing.expectError(error.TruncatedMctMarker, parseMct(allocator, &bytes, &out));
}

test "parseMco with zero entries accepted" {
    const allocator = std.testing.allocator;
    // MCO with Nmco=0: Lmco = 3 (Lmco(2) + Nmco(1)).
    const mco_bytes = [_]u8{
        0xff, 0x77,
        0x00, 0x03,
        0x00, // Nmco = 0
    };
    const bytes = try buildMinimalCodestreamWithBytes(allocator, &mco_bytes);
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expect(state.mco != null);
    try std.testing.expectEqual(@as(usize, 0), state.mco.?.ids.len);
}

test "parseMct/parseMcc/parseMco round-trip through parseState" {
    const allocator = std.testing.allocator;
    // MCT segment: 8 bytes of Dmct payload.
    // Lmct = 2 + 2 + 2 + 2 + 8 = 16.
    // Imct: type=0 (decorrelation in bits 8-9), element type=2 (f32) in bits 10-11 -> 0x0800, index=1.
    const mct_bytes = [_]u8{
        0xff, 0x74,
        0x00, 0x10,
        0x00, 0x00, // Zmct
        0x08, 0x01, // Imct (element_type=2 in bits 10-11, index=1)
        0x00, 0x00, // Ymct
        0xaa, 0xbb, 0xcc, 0xdd, 0x01, 0x02, 0x03, 0x04, // Dmct
    };
    // MCC segment: Lmcc = 2 + 2 + 2 + 2 + 4 = 12.
    const mcc_bytes = [_]u8{
        0xff, 0x75,
        0x00, 0x0c,
        0x00, 0x00, // Zmcc
        0x00, 0x02, // Imcc (index=2)
        0x00, 0x00, // Ymcc
        0xde, 0xad, 0xbe, 0xef, // Qmcc
    };
    // MCO: Nmco=1, id=2.
    const mco_bytes = [_]u8{
        0xff, 0x77,
        0x00, 0x04,
        0x01, 0x02,
    };
    var extra: std.ArrayListUnmanaged(u8) = .empty;
    defer extra.deinit(allocator);
    try extra.appendSlice(allocator, &mct_bytes);
    try extra.appendSlice(allocator, &mcc_bytes);
    try extra.appendSlice(allocator, &mco_bytes);

    const bytes = try buildMinimalCodestreamWithBytes(allocator, extra.items);
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.mct_segments.len);
    try std.testing.expectEqual(@as(u16, 1), state.mct_segments[0].index);
    try std.testing.expectEqual(@as(u8, 2), state.mct_segments[0].element_type);
    try std.testing.expectEqualSlices(u8, &.{ 0xaa, 0xbb, 0xcc, 0xdd, 0x01, 0x02, 0x03, 0x04 }, state.mct_segments[0].payload);
    try std.testing.expectEqual(@as(usize, 1), state.mcc_collections.len);
    try std.testing.expectEqual(@as(u16, 2), state.mcc_collections[0].index);
    try std.testing.expectEqualSlices(u8, &.{ 0xde, 0xad, 0xbe, 0xef }, state.mcc_collections[0].payload);
    try std.testing.expect(state.mco != null);
    try std.testing.expectEqualSlices(u16, &.{2}, state.mco.?.ids);
}

// Build a synthetic codestream:
//   SOC | SIZ | <main_extra> | SOT | <tile_extra> | SOD | EOC
// where `main_extra` is inserted in the main header and `tile_extra` is
// inserted inside the tile-part header (between SOT and SOD). The single
// tile-part has tile_part_length = SOT(12) + tile_extra.len + SOD(2), so the
// parser's SOD-skip-ahead path lands cleanly on EOC.
fn buildCodestreamWithPpmPpt(
    allocator: std.mem.Allocator,
    main_extra: []const u8,
    tile_extra: []const u8,
) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, &.{ 0xff, 0x4f }); // SOC
    const siz_body = [_]u8{
        0xff, 0x51,
        0x00, 0x29,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x04,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x00,
        0x00, 0x01,
        0x07, 0x01,
        0x01,
    };
    try out.appendSlice(allocator, &siz_body);
    try out.appendSlice(allocator, main_extra);
    // SOT
    try out.appendSlice(allocator, &.{ 0xff, 0x90, 0x00, 0x0a, 0x00, 0x00 });
    const tile_part_length: u32 = @intCast(12 + tile_extra.len + 2);
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, tile_part_length, .big);
    try out.appendSlice(allocator, &len_buf);
    try out.appendSlice(allocator, &.{ 0x00, 0x01 }); // TPsot=0, TNsot=1
    try out.appendSlice(allocator, tile_extra);
    try out.appendSlice(allocator, &.{ 0xff, 0x93 }); // SOD
    try out.appendSlice(allocator, &.{ 0xff, 0xd9 }); // EOC
    return out.toOwnedSlice(allocator);
}

fn ppmBytes(zppm: u8, payload: []const u8) [256]u8 {
    // Build a PPM marker into a 256-byte scratch buffer (caller slices to
    // 4 + 1 + payload.len). Lppm = 3 + payload.len.
    var buf: [256]u8 = undefined;
    buf[0] = 0xff;
    buf[1] = 0x60;
    const lppm: u16 = @intCast(3 + payload.len);
    std.mem.writeInt(u16, buf[2..4], lppm, .big);
    buf[4] = zppm;
    @memcpy(buf[5 .. 5 + payload.len], payload);
    return buf;
}

fn pptBytes(zppt: u8, payload: []const u8) [256]u8 {
    var buf: [256]u8 = undefined;
    buf[0] = 0xff;
    buf[1] = 0x61;
    const lppt: u16 = @intCast(3 + payload.len);
    std.mem.writeInt(u16, buf[2..4], lppt, .big);
    buf[4] = zppt;
    @memcpy(buf[5 .. 5 + payload.len], payload);
    return buf;
}

test "PPM segment parses and stores raw payload" {
    const allocator = std.testing.allocator;
    const ppm_payload = [_]u8{ 0xde, 0xad, 0xbe, 0xef };
    const ppm = ppmBytes(0, &ppm_payload);
    const ppm_total = 5 + ppm_payload.len;
    const bytes = try buildCodestreamWithPpmPpt(allocator, ppm[0..ppm_total], &.{});
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.ppm_segments.len);
    try std.testing.expectEqual(@as(u8, 0), state.ppm_segments[0].zppm);
    try std.testing.expectEqualSlices(u8, &ppm_payload, state.ppm_segments[0].payload);
}

test "Multiple PPM segments preserve order" {
    const allocator = std.testing.allocator;
    const p0 = [_]u8{ 0x11, 0x22 };
    const p1 = [_]u8{ 0x33, 0x44, 0x55 };
    const ppm0 = ppmBytes(0, &p0);
    const ppm1 = ppmBytes(1, &p1);
    var main_extra: std.ArrayListUnmanaged(u8) = .empty;
    defer main_extra.deinit(allocator);
    try main_extra.appendSlice(allocator, ppm0[0 .. 5 + p0.len]);
    try main_extra.appendSlice(allocator, ppm1[0 .. 5 + p1.len]);
    const bytes = try buildCodestreamWithPpmPpt(allocator, main_extra.items, &.{});
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), state.ppm_segments.len);
    try std.testing.expectEqual(@as(u8, 0), state.ppm_segments[0].zppm);
    try std.testing.expectEqual(@as(u8, 1), state.ppm_segments[1].zppm);
    try std.testing.expectEqualSlices(u8, &p0, state.ppm_segments[0].payload);
    try std.testing.expectEqualSlices(u8, &p1, state.ppm_segments[1].payload);
}

test "Out-of-order Zppm rejected" {
    const allocator = std.testing.allocator;
    const p = [_]u8{0xaa};
    const ppm0 = ppmBytes(1, &p); // first one has Zppm=1, must be 0
    const ppm1 = ppmBytes(0, &p);
    var main_extra: std.ArrayListUnmanaged(u8) = .empty;
    defer main_extra.deinit(allocator);
    try main_extra.appendSlice(allocator, ppm0[0 .. 5 + p.len]);
    try main_extra.appendSlice(allocator, ppm1[0 .. 5 + p.len]);
    const bytes = try buildCodestreamWithPpmPpt(allocator, main_extra.items, &.{});
    defer allocator.free(bytes);
    try std.testing.expectError(error.InvalidPpmOrdering, parseState(allocator, bytes));
}

test "PPM after SOT rejected" {
    const allocator = std.testing.allocator;
    const p = [_]u8{ 0x01, 0x02 };
    const ppm = ppmBytes(0, &p);
    // Place PPM in the tile-part header (after SOT). It must be rejected.
    const bytes = try buildCodestreamWithPpmPpt(allocator, &.{}, ppm[0 .. 5 + p.len]);
    defer allocator.free(bytes);
    try std.testing.expectError(error.InvalidPpmLocation, parseState(allocator, bytes));
}

test "PPT inside tile-part stores on correct tile-part" {
    const allocator = std.testing.allocator;
    const ppt_payload = [_]u8{ 0xca, 0xfe, 0xba, 0xbe };
    const ppt = pptBytes(0, &ppt_payload);
    const bytes = try buildCodestreamWithPpmPpt(allocator, &.{}, ppt[0 .. 5 + ppt_payload.len]);
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.tile_parts.len);
    try std.testing.expectEqual(@as(usize, 1), state.tile_parts[0].ppt_segments.len);
    try std.testing.expectEqual(@as(u8, 0), state.tile_parts[0].ppt_segments[0].zppt);
    try std.testing.expectEqualSlices(u8, &ppt_payload, state.tile_parts[0].ppt_segments[0].payload);
}

test "PPT outside tile-part rejected" {
    const allocator = std.testing.allocator;
    const p = [_]u8{ 0xaa, 0xbb };
    const ppt = pptBytes(0, &p);
    // Place PPT in the main header (before SOT) -> should be rejected.
    const bytes = try buildCodestreamWithPpmPpt(allocator, ppt[0 .. 5 + p.len], &.{});
    defer allocator.free(bytes);
    try std.testing.expectError(error.InvalidPptLocation, parseState(allocator, bytes));
}

test "PPM + PPT conflict rejected" {
    const allocator = std.testing.allocator;
    const ppm_payload = [_]u8{ 0x01, 0x02 };
    const ppm = ppmBytes(0, &ppm_payload);
    const ppt_payload = [_]u8{ 0x03, 0x04 };
    const ppt = pptBytes(0, &ppt_payload);
    const bytes = try buildCodestreamWithPpmPpt(
        allocator,
        ppm[0 .. 5 + ppm_payload.len],
        ppt[0 .. 5 + ppt_payload.len],
    );
    defer allocator.free(bytes);
    try std.testing.expectError(error.PpmPptConflict, parseState(allocator, bytes));
}

test "fullNativeDecodeSupport permits PPM past the packed-header support gate" {
    const allocator = std.testing.allocator;
    const ppm_payload = [_]u8{ 0xde, 0xad, 0xbe, 0xef };
    const ppm = ppmBytes(0, &ppm_payload);
    const bytes = try buildCodestreamWithPpmPpt(allocator, ppm[0 .. 5 + ppm_payload.len], &.{});
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.ppm_segments.len);
    try std.testing.expectEqual(
        NativeDecodeSupport.missing_coding_style,
        state.fullNativeDecodeSupport(),
    );
}

test "fullNativeDecodeSupport permits PPT past the packed-header support gate" {
    const allocator = std.testing.allocator;
    const ppt_payload = [_]u8{ 0xca, 0xfe, 0xba, 0xbe };
    const ppt = pptBytes(0, &ppt_payload);
    const bytes = try buildCodestreamWithPpmPpt(allocator, &.{}, ppt[0 .. 5 + ppt_payload.len]);
    defer allocator.free(bytes);
    var state = try parseState(allocator, bytes);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), state.tile_parts.len);
    try std.testing.expectEqual(@as(usize, 1), state.tile_parts[0].ppt_segments.len);
    try std.testing.expectEqual(
        NativeDecodeSupport.missing_coding_style,
        state.fullNativeDecodeSupport(),
    );
}
