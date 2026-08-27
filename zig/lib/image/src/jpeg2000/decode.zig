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
const codeblock = @import("codeblock.zig");
const codestream = @import("codestream.zig");
const color = @import("color.zig");
const color_transform = @import("color_transform.zig");
const packet = @import("packet.zig");
const reconstruct = @import("reconstruct.zig");
const tier1_encode = @import("tier1_encode.zig");
const tile = @import("tile.zig");
const wavelet = @import("wavelet.zig");

/// Producer tag the encoder emits in a COM segment. When this tag is present,
/// the decoder stays on symmetric exact_bitplane policies (preserves ours->ours
/// round-trip PSNR). When absent, 9/7 irreversible streams switch to ISO
/// 15444-1 Annex E.1.2 midpoint reconstruction.
pub const antfly_producer_tag: []const u8 = "antfly-zig j2k v1";

/// Compile-time rollback switch. Set to `false` to force symmetric
/// exact_bitplane for every stream, reverting the OpenJPEG midpoint behavior.
const USE_MIDPOINT_FOR_FOREIGN_IRREVERSIBLE: bool = true;

fn producedByUs(comments: []const codestream.Comment) bool {
    for (comments) |c| {
        if (c.registration != 1) continue;
        if (std.mem.startsWith(u8, c.text, antfly_producer_tag)) return true;
    }
    return false;
}

const Tier1Policy = struct {
    refinement: codeblock.RefinementPolicy,
    magnitude: codeblock.MagnitudePolicy,
};

fn policiesForState(state: *const codestream.State) Tier1Policy {
    const exact = Tier1Policy{ .refinement = .exact_bitplane, .magnitude = .exact_bitplane };
    const coding = state.coding_style orelse return exact;
    if (coding.wavelet_transform != 0) return exact; // 5/3 reversible stays exact.
    if (!USE_MIDPOINT_FOR_FOREIGN_IRREVERSIBLE) return exact;
    if (producedByUs(state.comments)) return exact;
    if (state.header.components.len == 0) return exact;
    return .{ .refinement = .openjpeg_midpoint_signed, .magnitude = .openjpeg_midpoint };
}

fn quantizationStyleForTile(state: *const codestream.State, tile_index: u16) ?codestream.QuantizationStyle {
    for (state.tile_parts) |tile_part| {
        if (tile_part.tile_index == tile_index) {
            if (tile_part.quantization_style) |q| return q;
        }
    }
    return state.quantization_style;
}

const PocSelection = struct {
    entries: []codestream.PocEntry,
    owned: bool = false,

    fn deinit(self: PocSelection, allocator: std.mem.Allocator) void {
        if (self.owned) allocator.free(self.entries);
    }
};

fn pocEntriesForTile(allocator: std.mem.Allocator, state: *const codestream.State, tile_index: u16) !PocSelection {
    var count: usize = 0;
    for (state.tile_parts) |tile_part| {
        if (tile_part.tile_index == tile_index) count += tile_part.poc_entries.len;
    }
    if (count == 0) return .{ .entries = state.poc_entries };

    const entries = try allocator.alloc(codestream.PocEntry, count);
    var out: usize = 0;
    for (state.tile_parts) |tile_part| {
        if (tile_part.tile_index != tile_index) continue;
        @memcpy(entries[out .. out + tile_part.poc_entries.len], tile_part.poc_entries);
        out += tile_part.poc_entries.len;
    }
    return .{ .entries = entries, .owned = true };
}

pub const Format = enum {
    jp2,
    j2k,
};

pub const Header = struct {
    format: Format,
    width: u32,
    height: u32,
    components: u16,
    bits_per_component: u8,
    is_signed: bool,
    uses_multiple_tiles: bool,

    pub fn supportsDecodeU8(self: Header) bool {
        return color.supportsOutputU8(self.components, self.bits_per_component, self.is_signed);
    }

    pub fn supportsDecodeU16(self: Header) bool {
        return self.components >= 1 and self.components <= 5 and self.bits_per_component >= 1 and self.bits_per_component <= 16;
    }
};

pub const Jp2ColorMetadata = struct {
    color_method: ?box.ColorMethod = null,
    enumerated_color_space: ?u32 = null,
    has_icc_profile: bool = false,
    /// Indexed by codestream component. Five is the maximum component count
    /// accepted by the U8 decoder, so metadata remains allocation-free after
    /// the JP2 box parser returns.
    channel_definitions: [5]?box.ChannelDefinition = .{null} ** 5,

    pub const AlphaLayout = struct {
        color_count: u8,
        color_offsets: [4]u8,
        alpha: u8,
        premultiplied: bool,
    };

    fn fromParsed(parsed: box.ParsedJp2) Jp2ColorMetadata {
        var metadata = Jp2ColorMetadata{};
        if (parsed.color_spec) |color_spec| {
            metadata.color_method = color_spec.method;
            if (color_spec.method == .enumerated) metadata.enumerated_color_space = color_spec.enum_cs;
            metadata.has_icc_profile = color_spec.method == .restricted_icc or color_spec.method == .any_icc;
        }
        if (parsed.cdef) |definitions| {
            for (definitions) |definition| {
                if (definition.channel < metadata.channel_definitions.len)
                    metadata.channel_definitions[definition.channel] = definition;
            }
        }
        return metadata;
    }

    /// Resolve JP2 channel definitions into color-plus-opacity component indexes.
    /// Partial color associations are completed in codestream order, but an
    /// explicit opacity channel is mandatory so CMYK cannot be mistaken for
    /// RGB plus alpha.
    pub fn alphaLayout(self: Jp2ColorMetadata, components: u8) ?AlphaLayout {
        if (components < 2 or components > 5) return null;
        const color_count: u8 = components - 1;
        if (color_count != 1 and color_count != 3 and color_count != 4) return null;
        var color_indexes: [4]?u8 = .{ null, null, null, null };
        var alpha: ?u8 = null;
        var premultiplied = false;

        for (self.channel_definitions, 0..) |maybe_definition, component_index| {
            const definition = maybe_definition orelse continue;
            if (definition.channel >= components or definition.channel != component_index) return null;
            switch (definition.kind) {
                .opacity, .premultiplied_opacity => {
                    if (alpha != null) return null;
                    alpha = @intCast(component_index);
                    premultiplied = definition.kind == .premultiplied_opacity;
                },
                .color => {
                    if (definition.association >= 1 and definition.association <= color_count) {
                        const association: usize = definition.association - 1;
                        if (color_indexes[association] != null) return null;
                        color_indexes[association] = @intCast(component_index);
                    }
                },
                .unspecified => {},
            }
        }
        const alpha_index = alpha orelse return null;

        var used = [_]bool{false} ** 5;
        used[alpha_index] = true;
        for (color_indexes[0..color_count]) |maybe_index| {
            if (maybe_index) |index| {
                if (used[index]) return null;
                used[index] = true;
            }
        }
        for (color_indexes[0..color_count]) |*maybe_index| {
            if (maybe_index.* != null) continue;
            for (0..components) |index| {
                if (used[index]) continue;
                maybe_index.* = @intCast(index);
                used[index] = true;
                break;
            }
            if (maybe_index.* == null) return null;
        }
        var offsets: [4]u8 = .{ 0, 1, 2, 3 };
        for (color_indexes[0..color_count], 0..) |maybe_index, index| offsets[index] = maybe_index.?;
        return .{
            .color_count = color_count,
            .color_offsets = offsets,
            .alpha = alpha_index,
            .premultiplied = premultiplied,
        };
    }
};

pub const DecodedImage = struct {
    allocator: std.mem.Allocator,
    width: u32,
    height: u32,
    components: u8,
    backend: DecodeBackend,
    pixels: []u8,
    jp2_color: Jp2ColorMetadata = .{},

    pub fn deinit(self: *DecodedImage) void {
        self.allocator.free(self.pixels);
        self.* = undefined;
    }
};

pub const DecodedImageU16 = struct {
    allocator: std.mem.Allocator,
    width: u32,
    height: u32,
    components: u8,
    backend: DecodeBackend,
    pixels: []u16,
    jp2_color: Jp2ColorMetadata = .{},

    pub fn deinit(self: *DecodedImageU16) void {
        self.allocator.free(self.pixels);
        self.* = undefined;
    }
};

pub const DecodedComponentPlanesU8 = struct {
    allocator: std.mem.Allocator,
    widths: []u32,
    heights: []u32,
    components: u16,
    planes: [][]u8,

    pub fn deinit(self: *DecodedComponentPlanesU8) void {
        for (self.planes) |plane| self.allocator.free(plane);
        self.allocator.free(self.planes);
        self.allocator.free(self.widths);
        self.allocator.free(self.heights);
        self.* = undefined;
    }
};

pub const DecodedComponentPlanesU16 = struct {
    allocator: std.mem.Allocator,
    widths: []u32,
    heights: []u32,
    components: u16,
    planes: [][]u16,

    pub fn deinit(self: *DecodedComponentPlanesU16) void {
        for (self.planes) |plane| self.allocator.free(plane);
        self.allocator.free(self.planes);
        self.allocator.free(self.widths);
        self.allocator.free(self.heights);
        self.* = undefined;
    }
};

pub const DecodeBackend = enum {
    pure_zig,
};

pub const NativeDecodeSupport = codestream.NativeDecodeSupport;

pub fn decodeHeader(allocator: std.mem.Allocator, path: []const u8) !Header {
    const bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(1024 * 1024));
    defer allocator.free(bytes);
    return decodeHeaderBytes(allocator, bytes);
}

pub fn decodeHeaderBytes(allocator: std.mem.Allocator, bytes: []const u8) !Header {
    if (box.hasSignature(bytes)) {
        const parsed = try box.parse(bytes);
        if (parsed.codestream_offset == null) return error.MissingCodestreamBox;
        var state = try codestream.parseState(allocator, bytes[parsed.codestream_offset.?..]);
        defer state.deinit(allocator);
        return fromCodestream(.jp2, &state.header);
    }
    if (codestream.hasSoc(bytes)) {
        var state = try codestream.parseState(allocator, bytes);
        defer state.deinit(allocator);
        return fromCodestream(.j2k, &state.header);
    }
    return error.UnsupportedImageFormat;
}

pub fn decodeU8(allocator: std.mem.Allocator, path: []const u8) !DecodedImage {
    const bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    defer allocator.free(bytes);
    return decodeU8Bytes(allocator, bytes);
}

pub fn decodeU8Bytes(allocator: std.mem.Allocator, bytes: []const u8) !DecodedImage {
    var jp2_color = Jp2ColorMetadata{};
    const codestream_bytes = if (box.hasSignature(bytes)) blk: {
        var parsed = try box.parseOwned(allocator, bytes);
        defer box.freeParsed(allocator, &parsed);
        jp2_color = Jp2ColorMetadata.fromParsed(parsed);
        const offset = parsed.codestream_offset orelse return error.MissingCodestreamBox;
        break :blk bytes[offset..];
    } else if (codestream.hasSoc(bytes))
        bytes
    else
        return error.UnsupportedImageFormat;

    var state = try codestream.parseState(allocator, codestream_bytes);
    defer state.deinit(allocator);

    _ = state.coding_style orelse return error.MissingCodingStyle;

    if (state.fullNativeDecodeSupport() != .supported) return error.UnsupportedNativeDecode;

    // The reconstruct paths below consult component[0] only for bpc/is_signed;
    // refuse heterogeneous SIZ component declarations before they corrupt output.
    try reconstruct.requireHomogeneousComponents(&state.header);

    // Standard JPEG 2000 decode path with canonical policies.
    if (state.header.uses_multiple_tiles) {
        const pixels = try decodeMultiTile(allocator, codestream_bytes, &state);
        return .{
            .allocator = allocator,
            .width = state.header.width,
            .height = state.header.height,
            .components = @intCast(state.header.components.len),
            .backend = .pure_zig,
            .pixels = pixels,
            .jp2_color = jp2_color,
        };
    }

    // JPEG 2000 reconstruction clips component samples to their declared
    // precision before presentation-grid resampling. Use the compact native
    // component APIs for subsampled single-tile streams so U8 and U16, and
    // single- and multi-tile decode, all perform those operations in the same
    // order. Keep the optimized raw-plane path below for the common 1:1 case.
    if (hasSubsampledComponents(&state)) {
        const pixels = if (state.header.components[0].bits_per_component == 8) blk: {
            var component_planes = try decodeSingleTileComponentPlanesU8AtResolution(allocator, codestream_bytes, &state, 0);
            defer component_planes.deinit(allocator);
            break :blk try reconstruct.interleaveComponentSamplesU8(allocator, &component_planes, &state);
        } else blk: {
            var component_planes = try decodeSingleTileComponentPlanesU16AtResolution(allocator, codestream_bytes, &state, 0);
            defer component_planes.deinit(allocator);
            break :blk try reconstruct.interleaveNativeComponentSamplesU8(allocator, &component_planes, &state);
        };
        return .{
            .allocator = allocator,
            .width = state.header.width,
            .height = state.header.height,
            .components = @intCast(state.header.components.len),
            .backend = .pure_zig,
            .pixels = pixels,
            .jp2_color = jp2_color,
        };
    }

    const packed_headers = try collectPackedPacketHeaders(allocator, &state, 1, null);
    defer freePackedPacketHeaders(allocator, packed_headers);

    const payload_info = try codestreamPayloadInfoWithPackedMode(allocator, codestream_bytes, packed_headers != null);
    defer allocator.free(payload_info.payload);

    var decode_state = state;
    decode_state.quantization_style = quantizationStyleForTile(&state, 0);

    var packet_model = if (packed_headers) |headers|
        try packet.buildPacketModelFromSplitPayload(
            allocator,
            &decode_state,
            headers[0],
            0,
            .packet_present_tagtree_first_inclusion,
        )
    else
        try packet.buildPacketModelFromPayload(
            allocator,
            &decode_state,
            payload_info.payload,
            0,
            .packet_present_tagtree_first_inclusion,
        );
    defer packet_model.deinit(allocator);

    const policy = policiesForState(&decode_state);
    const pixels = if (try reconstruct.StreamingPlaneAssembler.canAssemble(&decode_state)) blk: {
        const planes = try decodeStreamingRawPlanes(allocator, &packet_model, payload_info.payload, &decode_state, policy, 0);
        defer {
            for (planes) |plane| allocator.free(plane);
            allocator.free(planes);
        }
        break :blk try reconstruct.interleaveComponentPlanesU8(allocator, &decode_state, planes);
    } else blk: {
        var execution = try packet.executeTier1SegmentsForState(
            allocator,
            &packet_model,
            payload_info.payload,
            &decode_state,
            .standard,
            policy.refinement,
            policy.magnitude,
            0,
            .standard,
        );
        defer execution.deinit(allocator);
        const reconstruction = try reconstruct.reconstructTier1ExecutionReportWithOptions(
            allocator,
            &decode_state,
            &execution,
            false,
            false,
        );
        break :blk reconstruction.pixels;
    };
    return .{
        .allocator = allocator,
        .width = state.header.width,
        .height = state.header.height,
        .components = @intCast(state.header.components.len),
        .backend = .pure_zig,
        .pixels = pixels,
        .jp2_color = jp2_color,
    };
}

pub fn decodeComponentPlanesU8Bytes(allocator: std.mem.Allocator, bytes: []const u8) !DecodedComponentPlanesU8 {
    return decodeComponentPlanesU8BytesAtResolution(allocator, bytes, 0);
}

pub fn decodeComponentPlanesU8BytesAtResolution(allocator: std.mem.Allocator, bytes: []const u8, discard_levels: u8) !DecodedComponentPlanesU8 {
    const codestream_bytes = if (box.hasSignature(bytes)) blk: {
        const parsed = try box.parse(bytes);
        const offset = parsed.codestream_offset orelse return error.MissingCodestreamBox;
        break :blk bytes[offset..];
    } else if (codestream.hasSoc(bytes))
        bytes
    else
        return error.UnsupportedImageFormat;

    var state = try codestream.parseState(allocator, codestream_bytes);
    defer state.deinit(allocator);

    if (componentPlanesU8DecodeSupport(&state) != .supported) return error.UnsupportedNativeDecode;
    try reconstruct.requireHomogeneousComponents(&state.header);
    if (state.header.components.len == 0) return error.UnsupportedPlaneCount;

    if (discard_levels > 0 and state.header.uses_multiple_tiles) return error.UnsupportedReducedMultiTileDecode;

    var component_planes = if (state.header.uses_multiple_tiles)
        try reconstructComponentPlanesU8FromPlanes(allocator, try decodeMultiTileComponentPlanesU8(allocator, codestream_bytes, &state), &state)
    else
        try decodeSingleTileComponentPlanesU8AtResolution(allocator, codestream_bytes, &state, discard_levels);
    errdefer component_planes.deinit(allocator);

    const widths = try usizeDimensionsToU32(allocator, component_planes.widths);
    errdefer allocator.free(widths);
    const heights = try usizeDimensionsToU32(allocator, component_planes.heights);
    errdefer allocator.free(heights);

    allocator.free(component_planes.widths);
    allocator.free(component_planes.heights);

    return .{
        .allocator = allocator,
        .widths = widths,
        .heights = heights,
        .components = @intCast(state.header.components.len),
        .planes = component_planes.planes,
    };
}

pub fn decodeComponentPlanesU16Bytes(allocator: std.mem.Allocator, bytes: []const u8) !DecodedComponentPlanesU16 {
    return decodeComponentPlanesU16BytesAtResolution(allocator, bytes, 0);
}

pub fn decodeComponentPlanesU16BytesAtResolution(allocator: std.mem.Allocator, bytes: []const u8, discard_levels: u8) !DecodedComponentPlanesU16 {
    const codestream_bytes = if (box.hasSignature(bytes)) blk: {
        const parsed = try box.parse(bytes);
        const offset = parsed.codestream_offset orelse return error.MissingCodestreamBox;
        break :blk bytes[offset..];
    } else if (codestream.hasSoc(bytes))
        bytes
    else
        return error.UnsupportedImageFormat;

    var state = try codestream.parseState(allocator, codestream_bytes);
    defer state.deinit(allocator);

    if (componentPlanesU8DecodeSupport(&state) != .supported) return error.UnsupportedNativeDecode;
    try reconstruct.requireHomogeneousComponents(&state.header);
    if (state.header.components.len == 0) return error.UnsupportedPlaneCount;

    if (discard_levels > 0 and state.header.uses_multiple_tiles) return error.UnsupportedReducedMultiTileDecode;

    var component_planes = if (state.header.uses_multiple_tiles)
        try reconstructComponentPlanesU16FromPlanes(allocator, try decodeMultiTileComponentPlanesU16(allocator, codestream_bytes, &state), &state)
    else
        try decodeSingleTileComponentPlanesU16AtResolution(allocator, codestream_bytes, &state, discard_levels);
    errdefer component_planes.deinit(allocator);

    const widths = try usizeDimensionsToU32(allocator, component_planes.widths);
    errdefer allocator.free(widths);
    const heights = try usizeDimensionsToU32(allocator, component_planes.heights);
    errdefer allocator.free(heights);

    allocator.free(component_planes.widths);
    allocator.free(component_planes.heights);

    return .{
        .allocator = allocator,
        .widths = widths,
        .heights = heights,
        .components = @intCast(state.header.components.len),
        .planes = component_planes.planes,
    };
}

fn componentPlanesU8DecodeSupport(state: *const codestream.State) NativeDecodeSupport {
    if (state.header.components.len == 0) return .unsupported_components;
    for (state.header.components) |component| {
        if (component.bits_per_component == 0 or component.bits_per_component > 16) return .unsupported_precision;
        if (component.xrsiz == 0 or component.yrsiz == 0) return .unsupported_components;
    }
    const coding_style = state.coding_style orelse return .missing_coding_style;
    if (coding_style.progression_order > 4) return .unsupported_progression_order;
    if (coding_style.wavelet_transform > 1) return .unsupported_wavelet_transform;
    if (!packet.nativeDecodeSupportsCodeBlockStyle(coding_style.code_block_style)) return .unsupported_code_block_style;
    for (state.component_coding_styles) |component_style| {
        if (component_style) |style| {
            if (!packet.nativeDecodeSupportsCodeBlockStyle(style.code_block_style)) return .unsupported_code_block_style;
        }
    }
    const quantization_style = state.quantization_style orelse return .missing_quantization_style;
    if (quantization_style.style > 2) return .unsupported_quantization_mode;
    if (!state.has_start_of_data) return .missing_start_of_data;
    if (!state.has_end_of_codestream) return .missing_end_of_codestream;
    if (state.tile_parts.len == 0) return .unsupported_tile_part_layout;
    return .supported;
}

pub fn decodeU16Bytes(allocator: std.mem.Allocator, bytes: []const u8) !DecodedImageU16 {
    var jp2_color = Jp2ColorMetadata{};
    const codestream_bytes = if (box.hasSignature(bytes)) blk: {
        var parsed = try box.parseOwned(allocator, bytes);
        defer box.freeParsed(allocator, &parsed);
        jp2_color = Jp2ColorMetadata.fromParsed(parsed);
        const offset = parsed.codestream_offset orelse return error.MissingCodestreamBox;
        break :blk bytes[offset..];
    } else if (codestream.hasSoc(bytes))
        bytes
    else
        return error.UnsupportedImageFormat;

    var state = try codestream.parseState(allocator, codestream_bytes);
    defer state.deinit(allocator);

    const full_support = state.fullNativeDecodeSupport();
    if (full_support != .supported) return error.UnsupportedNativeDecode;

    // interleavePlanesU16 consults component[0] only for bpc/is_signed;
    // refuse heterogeneous SIZ component declarations before they corrupt output.
    try reconstruct.requireHomogeneousComponents(&state.header);

    _ = state.coding_style orelse return error.MissingCodingStyle;

    if (state.header.uses_multiple_tiles) {
        const pixels = try decodeMultiTileU16(allocator, codestream_bytes, &state);
        return .{
            .allocator = allocator,
            .width = state.header.width,
            .height = state.header.height,
            .components = @intCast(state.header.components.len),
            .backend = .pure_zig,
            .pixels = pixels,
            .jp2_color = jp2_color,
        };
    }

    const packed_headers = try collectPackedPacketHeaders(allocator, &state, 1, null);
    defer freePackedPacketHeaders(allocator, packed_headers);

    const payload_info = try codestreamPayloadInfoWithPackedMode(allocator, codestream_bytes, packed_headers != null);
    defer allocator.free(payload_info.payload);

    var decode_state = state;
    decode_state.quantization_style = quantizationStyleForTile(&state, 0);

    var packet_model = if (packed_headers) |headers|
        try packet.buildPacketModelFromSplitPayload(
            allocator,
            &decode_state,
            headers[0],
            0,
            .packet_present_tagtree_first_inclusion,
        )
    else
        try packet.buildPacketModelFromPayload(
            allocator,
            &decode_state,
            payload_info.payload,
            0,
            .packet_present_tagtree_first_inclusion,
        );
    defer packet_model.deinit(allocator);

    const policy = policiesForState(&decode_state);
    var execution = try packet.executeTier1SegmentsForState(
        allocator,
        &packet_model,
        payload_info.payload,
        &decode_state,
        .standard,
        policy.refinement,
        policy.magnitude,
        0,
        .standard,
    );
    defer execution.deinit(allocator);

    var component_planes = try reconstruct.reconstructTier1ComponentPlanesU16(
        allocator,
        &decode_state,
        &execution,
    );
    defer component_planes.deinit(allocator);
    const pixels = try reconstruct.interleaveComponentPlanesU16(
        allocator,
        &component_planes,
        &state,
    );
    return .{
        .allocator = allocator,
        .width = state.header.width,
        .height = state.header.height,
        .components = @intCast(state.header.components.len),
        .backend = .pure_zig,
        .pixels = pixels,
        .jp2_color = jp2_color,
    };
}

pub fn nativeDecodeSupport(allocator: std.mem.Allocator, path: []const u8) !NativeDecodeSupport {
    const bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
    defer allocator.free(bytes);
    return nativeDecodeSupportBytes(allocator, bytes);
}

pub fn nativeDecodeSupportBytes(allocator: std.mem.Allocator, bytes: []const u8) !NativeDecodeSupport {
    if (box.hasSignature(bytes)) {
        const parsed = try box.parse(bytes);
        if (parsed.codestream_offset == null) return error.MissingCodestreamBox;
        var state = try codestream.parseState(allocator, bytes[parsed.codestream_offset.?..]);
        defer state.deinit(allocator);
        return state.fullNativeDecodeSupport();
    }
    if (codestream.hasSoc(bytes)) {
        var state = try codestream.parseState(allocator, bytes);
        defer state.deinit(allocator);
        return state.fullNativeDecodeSupport();
    }
    return error.UnsupportedImageFormat;
}

fn fromCodestream(format: Format, stream_header: *const codestream.Header) Header {
    const first = stream_header.components[0];
    return .{
        .format = format,
        .width = stream_header.width,
        .height = stream_header.height,
        .components = @intCast(stream_header.components.len),
        .bits_per_component = first.bits_per_component,
        .is_signed = first.is_signed,
        .uses_multiple_tiles = stream_header.uses_multiple_tiles,
    };
}

fn ceilDivUsize(value: usize, denom: usize) usize {
    return if (value == 0) 0 else (value + denom - 1) / denom;
}

fn hasSubsampledComponents(state: *const codestream.State) bool {
    for (state.header.components) |component| {
        const dims = tile.componentDimensionsAt(
            state.header.x_offset,
            state.header.y_offset,
            state.header.width,
            state.header.height,
            component.xrsiz,
            component.yrsiz,
        );
        if (dims.width != state.header.width or dims.height != state.header.height) return true;
    }
    return false;
}

fn usizeDimensionsToU32(allocator: std.mem.Allocator, dims: []const usize) ![]u32 {
    const out = try allocator.alloc(u32, dims.len);
    errdefer allocator.free(out);
    for (dims, 0..) |dim, i| out[i] = @intCast(dim);
    return out;
}

fn componentPlaneFullDimensions(allocator: std.mem.Allocator, state: *const codestream.State) !struct { widths: []usize, heights: []usize } {
    const widths = try allocator.alloc(usize, state.header.components.len);
    errdefer allocator.free(widths);
    const heights = try allocator.alloc(usize, state.header.components.len);
    errdefer allocator.free(heights);
    for (state.header.components, 0..) |component, i| {
        const dims = tile.componentDimensionsAt(
            state.header.x_offset,
            state.header.y_offset,
            state.header.width,
            state.header.height,
            component.xrsiz,
            component.yrsiz,
        );
        widths[i] = @intCast(dims.width);
        heights[i] = @intCast(dims.height);
    }
    return .{ .widths = widths, .heights = heights };
}

fn reconstructComponentPlanesU8FromPlanes(allocator: std.mem.Allocator, planes: [][]u8, state: *const codestream.State) !reconstruct.ComponentPlanesU8 {
    errdefer {
        for (planes) |plane| allocator.free(plane);
        allocator.free(planes);
    }
    const dims = try componentPlaneFullDimensions(allocator, state);
    return .{ .widths = dims.widths, .heights = dims.heights, .planes = planes };
}

fn reconstructComponentPlanesU16FromPlanes(allocator: std.mem.Allocator, planes: [][]u16, state: *const codestream.State) !reconstruct.ComponentPlanesU16 {
    errdefer {
        for (planes) |plane| allocator.free(plane);
        allocator.free(planes);
    }
    const dims = try componentPlaneFullDimensions(allocator, state);
    return .{ .widths = dims.widths, .heights = dims.heights, .planes = planes };
}

/// Decode codeblocks directly into inverse-wavelet component planes. Each
/// coefficient grid is released immediately after the visitor consumes it,
/// bounding peak memory independently of the number of codeblocks.
fn decodeStreamingRawPlanes(
    allocator: std.mem.Allocator,
    packet_model: *const packet.PacketModel,
    payload: []const u8,
    state: *const codestream.State,
    policy: Tier1Policy,
    discard_levels: u8,
) ![][]i32 {
    var assembler = try reconstruct.StreamingPlaneAssembler.init(allocator, state);
    defer assembler.deinit();
    const Visitor = struct {
        fn visit(context: *anyopaque, codeblock_state: *const packet.Tier1CodeblockState) !void {
            const plane_assembler: *reconstruct.StreamingPlaneAssembler = @ptrCast(@alignCast(context));
            try plane_assembler.appendCodeblock(codeblock_state);
        }
    };
    try packet.visitTier1CodeblocksForState(
        allocator,
        packet_model,
        payload,
        state,
        .standard,
        policy.refinement,
        policy.magnitude,
        0,
        .standard,
        .{ .context = &assembler, .visit = Visitor.visit },
    );
    return assembler.finish(discard_levels);
}

fn decodeSingleTileComponentPlanesU8(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
) ![][]u8 {
    const component_planes = try decodeSingleTileComponentPlanesU8AtResolution(allocator, codestream_bytes, state, 0);
    allocator.free(component_planes.widths);
    allocator.free(component_planes.heights);
    return component_planes.planes;
}

fn decodeSingleTileComponentPlanesU8AtResolution(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
    discard_levels: u8,
) !reconstruct.ComponentPlanesU8 {
    const packed_headers = try collectPackedPacketHeaders(allocator, state, 1, null);
    defer freePackedPacketHeaders(allocator, packed_headers);

    const payload_info = try codestreamPayloadInfoWithPackedMode(allocator, codestream_bytes, packed_headers != null);
    defer allocator.free(payload_info.payload);

    var decode_state = state.*;
    decode_state.quantization_style = quantizationStyleForTile(state, 0);

    var packet_model = if (packed_headers) |headers|
        try packet.buildPacketModelFromSplitPayload(
            allocator,
            &decode_state,
            headers[0],
            0,
            .packet_present_tagtree_first_inclusion,
        )
    else
        try packet.buildPacketModelFromPayload(
            allocator,
            &decode_state,
            payload_info.payload,
            0,
            .packet_present_tagtree_first_inclusion,
        );
    defer packet_model.deinit(allocator);

    const policy = policiesForState(&decode_state);
    if (try reconstruct.StreamingPlaneAssembler.canAssemble(&decode_state)) {
        const raw_planes = try decodeStreamingRawPlanes(
            allocator,
            &packet_model,
            payload_info.payload,
            &decode_state,
            policy,
            discard_levels,
        );
        defer {
            for (raw_planes) |plane| allocator.free(plane);
            allocator.free(raw_planes);
        }
        return reconstruct.componentPlanesU8FromRawAtResolution(
            allocator,
            &decode_state,
            raw_planes,
            discard_levels,
        );
    }

    var execution = try packet.executeTier1SegmentsForState(
        allocator,
        &packet_model,
        payload_info.payload,
        &decode_state,
        .standard,
        policy.refinement,
        policy.magnitude,
        0,
        .standard,
    );
    defer execution.deinit(allocator);

    return reconstruct.reconstructTier1ComponentPlanesU8AtResolution(allocator, &decode_state, &execution, discard_levels);
}

fn decodeSingleTileComponentPlanesU16(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
) ![][]u16 {
    const component_planes = try decodeSingleTileComponentPlanesU16AtResolution(allocator, codestream_bytes, state, 0);
    allocator.free(component_planes.widths);
    allocator.free(component_planes.heights);
    return component_planes.planes;
}

fn decodeSingleTileComponentPlanesU16AtResolution(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
    discard_levels: u8,
) !reconstruct.ComponentPlanesU16 {
    const packed_headers = try collectPackedPacketHeaders(allocator, state, 1, null);
    defer freePackedPacketHeaders(allocator, packed_headers);

    const payload_info = try codestreamPayloadInfoWithPackedMode(allocator, codestream_bytes, packed_headers != null);
    defer allocator.free(payload_info.payload);

    var decode_state = state.*;
    decode_state.quantization_style = quantizationStyleForTile(state, 0);

    var packet_model = if (packed_headers) |headers|
        try packet.buildPacketModelFromSplitPayload(
            allocator,
            &decode_state,
            headers[0],
            0,
            .packet_present_tagtree_first_inclusion,
        )
    else
        try packet.buildPacketModelFromPayload(
            allocator,
            &decode_state,
            payload_info.payload,
            0,
            .packet_present_tagtree_first_inclusion,
        );
    defer packet_model.deinit(allocator);

    const policy = policiesForState(&decode_state);
    if (try reconstruct.StreamingPlaneAssembler.canAssemble(&decode_state)) {
        const raw_planes = try decodeStreamingRawPlanes(
            allocator,
            &packet_model,
            payload_info.payload,
            &decode_state,
            policy,
            discard_levels,
        );
        defer {
            for (raw_planes) |plane| allocator.free(plane);
            allocator.free(raw_planes);
        }
        return reconstruct.componentPlanesU16FromRawAtResolution(
            allocator,
            &decode_state,
            raw_planes,
            discard_levels,
        );
    }

    var execution = try packet.executeTier1SegmentsForState(
        allocator,
        &packet_model,
        payload_info.payload,
        &decode_state,
        .standard,
        policy.refinement,
        policy.magnitude,
        0,
        .standard,
    );
    defer execution.deinit(allocator);

    return reconstruct.reconstructTier1ComponentPlanesU16AtResolution(allocator, &decode_state, &execution, discard_levels);
}

fn decodeMultiTileComponentPlanesU8(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
) ![][]u8 {
    const num_components = state.header.components.len;

    const component_widths = try allocator.alloc(usize, num_components);
    defer allocator.free(component_widths);
    const component_heights = try allocator.alloc(usize, num_components);
    defer allocator.free(component_heights);
    const component_xrs = try allocator.alloc(usize, num_components);
    defer allocator.free(component_xrs);
    const component_yrs = try allocator.alloc(usize, num_components);
    defer allocator.free(component_yrs);
    for (state.header.components, 0..) |component, c| {
        const dims = tile.componentDimensionsAt(
            state.header.x_offset,
            state.header.y_offset,
            state.header.width,
            state.header.height,
            component.xrsiz,
            component.yrsiz,
        );
        component_widths[c] = @intCast(dims.width);
        component_heights[c] = @intCast(dims.height);
        component_xrs[c] = if (component.xrsiz == 0) 1 else @intCast(component.xrsiz);
        component_yrs[c] = if (component.yrsiz == 0) 1 else @intCast(component.yrsiz);
    }

    const tile_grid = tile.buildTileGrid(state);
    const num_tiles: usize = @intCast(tile_grid.tileCount());

    const has_packed_headers = hasPackedPacketHeaders(state);
    const tile_payloads = try collectTilePayloadsWithPackedMode(allocator, codestream_bytes, num_tiles, has_packed_headers);
    defer {
        for (tile_payloads) |payload| {
            allocator.free(payload.payload);
            if (payload.packet_lengths.len > 0) allocator.free(payload.packet_lengths);
        }
        allocator.free(tile_payloads);
    }

    const packed_headers = try collectPackedPacketHeaders(allocator, state, num_tiles, tile_payloads);
    defer freePackedPacketHeaders(allocator, packed_headers);

    const out_planes = try allocator.alloc([]u8, num_components);
    errdefer allocator.free(out_planes);
    var allocated: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < allocated) : (i += 1) allocator.free(out_planes[i]);
    }
    for (out_planes, 0..) |*plane, c| {
        plane.* = try allocator.alloc(u8, component_widths[c] * component_heights[c]);
        @memset(plane.*, 0);
        allocated += 1;
    }

    for (tile_payloads, 0..) |tr, tile_idx| {
        const tile_bounds = tile_grid.tileBounds(@intCast(tile_idx));
        const this_tile_w = tile_bounds.width;
        const this_tile_h = tile_bounds.height;
        const tile_poc = try pocEntriesForTile(allocator, state, @intCast(tile_idx));
        defer tile_poc.deinit(allocator);

        var tile_state = codestream.State{
            .header = .{
                .width = this_tile_w,
                .height = this_tile_h,
                .x_offset = tile_bounds.x0,
                .y_offset = tile_bounds.y0,
                .components = state.header.components,
                .tile_width = this_tile_w,
                .tile_height = this_tile_h,
                .tile_x_offset = tile_bounds.x0,
                .tile_y_offset = tile_bounds.y0,
                .uses_multiple_tiles = false,
            },
            .coding_style = state.coding_style,
            .quantization_style = quantizationStyleForTile(state, @intCast(tile_idx)),
            .component_coding_styles = state.component_coding_styles,
            .component_quantization_styles = state.component_quantization_styles,
            .poc_entries = if (packed_headers != null) &.{} else tile_poc.entries,
            .rgn_entries = state.rgn_entries,
            .mct_segments = state.mct_segments,
            .mcc_collections = state.mcc_collections,
            .mco = state.mco,
            .comments = state.comments,
            .tile_parts = &.{},
            .has_start_of_data = true,
            .has_end_of_codestream = true,
        };

        var packet_model = try buildPacketModelForTilePayload(allocator, &tile_state, tr, packed_headers, tile_idx);
        defer packet_model.deinit(allocator);

        const policy = policiesForState(&tile_state);
        var execution = try packet.executeTier1SegmentsForState(
            allocator,
            &packet_model,
            tr.payload,
            &tile_state,
            .standard,
            policy.refinement,
            policy.magnitude,
            0,
            .standard,
        );
        defer execution.deinit(allocator);

        var component_planes = try reconstruct.reconstructTier1ComponentPlanesU8(allocator, &tile_state, &execution);
        defer component_planes.deinit(allocator);

        for (component_planes.planes, 0..) |plane, c| {
            const comp_x0 = ceilDivUsize(tile_bounds.x0, component_xrs[c]) -
                ceilDivUsize(state.header.x_offset, component_xrs[c]);
            const comp_y0 = ceilDivUsize(tile_bounds.y0, component_yrs[c]) -
                ceilDivUsize(state.header.y_offset, component_yrs[c]);
            const copy_w = component_planes.widths[c];
            const copy_h = component_planes.heights[c];
            var y: usize = 0;
            while (y < copy_h) : (y += 1) {
                const dst_row = (comp_y0 + y) * component_widths[c] + comp_x0;
                const src_row = y * copy_w;
                @memcpy(out_planes[c][dst_row .. dst_row + copy_w], plane[src_row .. src_row + copy_w]);
            }
        }

        tile_state.header.components = &.{};
        tile_state.coding_style = null;
        tile_state.quantization_style = null;
        tile_state.comments = &.{};
    }

    return out_planes;
}

fn decodeMultiTileComponentPlanesU16(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
) ![][]u16 {
    const num_components = state.header.components.len;

    const component_widths = try allocator.alloc(usize, num_components);
    defer allocator.free(component_widths);
    const component_heights = try allocator.alloc(usize, num_components);
    defer allocator.free(component_heights);
    const component_xrs = try allocator.alloc(usize, num_components);
    defer allocator.free(component_xrs);
    const component_yrs = try allocator.alloc(usize, num_components);
    defer allocator.free(component_yrs);
    for (state.header.components, 0..) |component, c| {
        const dims = tile.componentDimensionsAt(
            state.header.x_offset,
            state.header.y_offset,
            state.header.width,
            state.header.height,
            component.xrsiz,
            component.yrsiz,
        );
        component_widths[c] = @intCast(dims.width);
        component_heights[c] = @intCast(dims.height);
        component_xrs[c] = if (component.xrsiz == 0) 1 else @intCast(component.xrsiz);
        component_yrs[c] = if (component.yrsiz == 0) 1 else @intCast(component.yrsiz);
    }

    const tile_grid = tile.buildTileGrid(state);
    const num_tiles: usize = @intCast(tile_grid.tileCount());

    const has_packed_headers = hasPackedPacketHeaders(state);
    const tile_payloads = try collectTilePayloadsWithPackedMode(allocator, codestream_bytes, num_tiles, has_packed_headers);
    defer {
        for (tile_payloads) |payload| {
            allocator.free(payload.payload);
            if (payload.packet_lengths.len > 0) allocator.free(payload.packet_lengths);
        }
        allocator.free(tile_payloads);
    }

    const packed_headers = try collectPackedPacketHeaders(allocator, state, num_tiles, tile_payloads);
    defer freePackedPacketHeaders(allocator, packed_headers);

    const out_planes = try allocator.alloc([]u16, num_components);
    errdefer allocator.free(out_planes);
    var allocated: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < allocated) : (i += 1) allocator.free(out_planes[i]);
    }
    for (out_planes, 0..) |*plane, c| {
        plane.* = try allocator.alloc(u16, component_widths[c] * component_heights[c]);
        @memset(plane.*, 0);
        allocated += 1;
    }

    for (tile_payloads, 0..) |tr, tile_idx| {
        const tile_bounds = tile_grid.tileBounds(@intCast(tile_idx));
        const this_tile_w = tile_bounds.width;
        const this_tile_h = tile_bounds.height;
        const tile_poc = try pocEntriesForTile(allocator, state, @intCast(tile_idx));
        defer tile_poc.deinit(allocator);

        var tile_state = codestream.State{
            .header = .{
                .width = this_tile_w,
                .height = this_tile_h,
                .x_offset = tile_bounds.x0,
                .y_offset = tile_bounds.y0,
                .components = state.header.components,
                .tile_width = this_tile_w,
                .tile_height = this_tile_h,
                .tile_x_offset = tile_bounds.x0,
                .tile_y_offset = tile_bounds.y0,
                .uses_multiple_tiles = false,
            },
            .coding_style = state.coding_style,
            .quantization_style = quantizationStyleForTile(state, @intCast(tile_idx)),
            .component_coding_styles = state.component_coding_styles,
            .component_quantization_styles = state.component_quantization_styles,
            .poc_entries = if (packed_headers != null) &.{} else tile_poc.entries,
            .rgn_entries = state.rgn_entries,
            .mct_segments = state.mct_segments,
            .mcc_collections = state.mcc_collections,
            .mco = state.mco,
            .comments = state.comments,
            .tile_parts = &.{},
            .has_start_of_data = true,
            .has_end_of_codestream = true,
        };

        var packet_model = try buildPacketModelForTilePayload(allocator, &tile_state, tr, packed_headers, tile_idx);
        defer packet_model.deinit(allocator);

        const policy = policiesForState(&tile_state);
        var execution = try packet.executeTier1SegmentsForState(
            allocator,
            &packet_model,
            tr.payload,
            &tile_state,
            .standard,
            policy.refinement,
            policy.magnitude,
            0,
            .standard,
        );
        defer execution.deinit(allocator);

        var component_planes = try reconstruct.reconstructTier1ComponentPlanesU16(allocator, &tile_state, &execution);
        defer component_planes.deinit(allocator);

        for (component_planes.planes, 0..) |plane, c| {
            const comp_x0 = ceilDivUsize(tile_bounds.x0, component_xrs[c]) -
                ceilDivUsize(state.header.x_offset, component_xrs[c]);
            const comp_y0 = ceilDivUsize(tile_bounds.y0, component_yrs[c]) -
                ceilDivUsize(state.header.y_offset, component_yrs[c]);
            const copy_w = component_planes.widths[c];
            const copy_h = component_planes.heights[c];
            var y: usize = 0;
            while (y < copy_h) : (y += 1) {
                const dst_row = (comp_y0 + y) * component_widths[c] + comp_x0;
                const src_row = y * copy_w;
                @memcpy(out_planes[c][dst_row .. dst_row + copy_w], plane[src_row .. src_row + copy_w]);
            }
        }

        tile_state.header.components = &.{};
        tile_state.coding_style = null;
        tile_state.quantization_style = null;
        tile_state.comments = &.{};
    }

    return out_planes;
}

/// Decode a multi-tile JPEG 2000 codestream by decoding each tile independently
/// and stitching component-grid samples before reference-grid upsampling.
fn decodeMultiTile(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
) ![]u8 {
    if (state.header.components[0].bits_per_component == 8) {
        var component_planes = try reconstructComponentPlanesU8FromPlanes(
            allocator,
            try decodeMultiTileComponentPlanesU8(allocator, codestream_bytes, state),
            state,
        );
        defer component_planes.deinit(allocator);
        return reconstruct.interleaveComponentSamplesU8(allocator, &component_planes, state);
    }

    var component_planes = try reconstructComponentPlanesU16FromPlanes(
        allocator,
        try decodeMultiTileComponentPlanesU16(allocator, codestream_bytes, state),
        state,
    );
    defer component_planes.deinit(allocator);
    return reconstruct.interleaveNativeComponentSamplesU8(allocator, &component_planes, state);
}

fn decodeMultiTileU16(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    state: *const codestream.State,
) ![]u16 {
    var component_planes = try reconstructComponentPlanesU16FromPlanes(
        allocator,
        try decodeMultiTileComponentPlanesU16(allocator, codestream_bytes, state),
        state,
    );
    defer component_planes.deinit(allocator);
    return reconstruct.interleaveComponentPlanesU16(allocator, &component_planes, state);
}

const TilePayload = struct {
    payload: []u8,
    base_offset: usize,
    packet_lengths: []u32 = &.{},
};

const PayloadInfo = struct {
    payload: []u8,
    base_offset: usize,
};

fn collectTilePayloads(allocator: std.mem.Allocator, codestream_bytes: []const u8, expected_tiles: usize) ![]TilePayload {
    return collectTilePayloadsWithPackedMode(allocator, codestream_bytes, expected_tiles, false);
}

fn collectTilePayloadsWithPackedMode(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    expected_tiles: usize,
    packed_headers: bool,
) ![]TilePayload {
    const tile_ranges = try codestream.parseTilePartRanges(allocator, codestream_bytes);
    defer allocator.free(tile_ranges);
    const payloads = try allocator.alloc(TilePayload, expected_tiles);
    var initialized_payloads: usize = 0;
    errdefer {
        for (payloads[0..initialized_payloads]) |payload| {
            allocator.free(payload.payload);
            if (payload.packet_lengths.len > 0) allocator.free(payload.packet_lengths);
        }
        allocator.free(payloads);
    }

    // Peek at COD to see whether SOP/EPH markers are embedded in the packet
    // payload. When set, strip them into scratch buffers before the tier-2
    // parser walks packet bits, which otherwise would mistake marker bytes
    // for header data.
    var strip_sop = false;
    var strip_eph = false;
    var parsed_state: ?codestream.State = null;
    defer if (parsed_state) |*ps| ps.deinit(allocator);
    if (codestream.parseState(allocator, codestream_bytes)) |parsed| {
        parsed_state = parsed;
        if (parsed_state.?.coding_style) |cs| {
            strip_sop = (cs.scod & 0x02) != 0;
            // With PPM/PPT, EPH markers are part of the packed header stream,
            // not the SOD packet-body byte stream.
            strip_eph = !packed_headers and (cs.scod & 0x04) != 0;
        }
    } else |_| {}

    var lists = try allocator.alloc(std.ArrayListUnmanaged(u8), expected_tiles);
    defer allocator.free(lists);
    for (lists) |*list| list.* = .empty;
    errdefer {
        for (lists) |*list| list.deinit(allocator);
    }
    var packet_length_lists = try allocator.alloc(std.ArrayListUnmanaged(u32), expected_tiles);
    defer allocator.free(packet_length_lists);
    for (packet_length_lists) |*list| list.* = .empty;
    errdefer {
        for (packet_length_lists) |*list| list.deinit(allocator);
    }
    const stripped_marker_bytes_per_packet: u32 =
        @as(u32, if (strip_sop) 6 else 0) + @as(u32, if (strip_eph) 2 else 0);
    if (stripped_marker_bytes_per_packet != 0) {
        if (parsed_state) |*ps| {
            for (ps.tile_parts) |tile_part| {
                if (tile_part.tile_index >= expected_tiles) return error.UnsupportedNativeDecode;
                const tile_index: usize = tile_part.tile_index;
                for (tile_part.plt_markers) |plt_marker| {
                    for (plt_marker.lengths) |length| {
                        if (length < stripped_marker_bytes_per_packet) return error.InvalidPacketSpanLayout;
                        try packet_length_lists[tile_index].append(allocator, length - stripped_marker_bytes_per_packet);
                    }
                }
            }
        }
    }

    const first_offsets = try allocator.alloc(?usize, expected_tiles);
    defer allocator.free(first_offsets);
    @memset(first_offsets, null);
    const expected_part_indices = try allocator.alloc(u8, expected_tiles);
    defer allocator.free(expected_part_indices);
    @memset(expected_part_indices, 0);
    const seen_any = try allocator.alloc(bool, expected_tiles);
    defer allocator.free(seen_any);
    @memset(seen_any, false);

    for (tile_ranges) |range| {
        if (range.tile_index >= expected_tiles) return error.UnsupportedNativeDecode;
        const tile_index: usize = range.tile_index;
        if (range.tile_part_index != expected_part_indices[tile_index]) return error.UnsupportedNativeDecode;
        expected_part_indices[tile_index] +%= 1;
        if (first_offsets[tile_index] == null) first_offsets[tile_index] = range.data_offset;
        const src = codestream_bytes[range.data_offset .. range.data_offset + range.data_length];
        if (strip_sop or strip_eph) {
            try appendStrippedPayload(allocator, &lists[tile_index], src, strip_sop, strip_eph);
        } else {
            try lists[tile_index].appendSlice(allocator, src);
        }
        seen_any[tile_index] = true;
    }

    for (0..expected_tiles) |tile_index| {
        if (!seen_any[tile_index]) return error.UnsupportedNativeDecode;
        const payload = try lists[tile_index].toOwnedSlice(allocator);
        errdefer allocator.free(payload);
        const packet_lengths: []u32 = if (packet_length_lists[tile_index].items.len > 0)
            try packet_length_lists[tile_index].toOwnedSlice(allocator)
        else
            &.{};
        errdefer if (packet_lengths.len > 0) allocator.free(packet_lengths);
        payloads[tile_index] = .{
            .payload = payload,
            .base_offset = first_offsets[tile_index] orelse 0,
            .packet_lengths = packet_lengths,
        };
        initialized_payloads += 1;
    }
    return payloads;
}

fn buildPacketModelForTilePayload(
    allocator: std.mem.Allocator,
    tile_state: *const codestream.State,
    tile_payload: TilePayload,
    packed_headers: ?[][]u8,
    tile_index: usize,
) !packet.PacketModel {
    if (packed_headers) |headers| {
        return buildSplitPacketModelMatchingBody(
            allocator,
            tile_state,
            headers[tile_index],
            tile_payload.payload.len,
        ) catch packet.buildPacketModelFromSplitPayload(
            allocator,
            tile_state,
            headers[tile_index],
            0,
            .packet_present_tagtree_first_inclusion,
        );
    }
    if (tile_payload.packet_lengths.len > 0) {
        if (packet.buildPacketModelFromPayloadWithPacketLengths(
            allocator,
            tile_state,
            tile_payload.payload,
            0,
            .packet_present_tagtree_first_inclusion,
            tile_payload.packet_lengths,
        )) |model| {
            return model;
        } else |err| switch (err) {
            error.InvalidPacketSpanLayout,
            error.TruncatedPacketBody,
            error.TruncatedPacketHeader,
            error.EndOfBitstream,
            => {},
            else => return err,
        }
    }
    return packet.buildPacketModelFromPayload(
        allocator,
        tile_state,
        tile_payload.payload,
        0,
        .packet_present_tagtree_first_inclusion,
    );
}

fn buildSplitPacketModelMatchingBody(
    allocator: std.mem.Allocator,
    tile_state: *const codestream.State,
    header_payload: []const u8,
    body_payload_length: usize,
) !packet.PacketModel {
    const modes = [_]packet.PacketHeaderMode{
        .packet_present_tagtree_first_inclusion,
        .packet_present_tagtree_one_based_inclusion,
        .packet_present_tagtree_one_based_zero_bitplanes_zero,
    };
    const prefer_empty_packets = if (tile_state.coding_style) |cs| (cs.scod & 0x06) != 0 else false;
    const include_empty_choices = [_]bool{ prefer_empty_packets, !prefer_empty_packets };

    var best: ?struct {
        model: packet.PacketModel,
        diff: usize,
    } = null;
    errdefer if (best) |*b| b.model.deinit(allocator);

    for (include_empty_choices) |include_empty| {
        for (modes) |mode| {
            var prefix = packet.buildPacketModelFromSplitPayloadPrefix(
                allocator,
                tile_state,
                header_payload,
                0,
                mode,
                include_empty,
            ) catch continue;
            var moved = false;
            defer if (!moved) prefix.deinit(allocator);

            var body_length: usize = 0;
            for (prefix.model.entries) |entry| body_length += entry.body_length;
            if (body_length > body_payload_length) {
                continue;
            }
            const diff = body_payload_length - body_length;
            if (best == null or diff < best.?.diff) {
                if (best) |*b| b.model.deinit(allocator);
                best = .{ .model = prefix.model, .diff = diff };
                moved = true;
                if (diff == 0) {
                    const model = best.?.model;
                    best = null;
                    return model;
                }
            }
        }
    }

    if (best) |b| return b.model;
    return error.InvalidPacketSpanLayout;
}

fn hasPackedPacketHeaders(state: *const codestream.State) bool {
    if (state.ppm_segments.len > 0) return true;
    for (state.tile_parts) |tp| {
        if (tp.ppt_segments.len > 0) return true;
    }
    return false;
}

fn collectPackedPacketHeaders(
    allocator: std.mem.Allocator,
    state: *const codestream.State,
    expected_tiles: usize,
    tile_payloads: ?[]const TilePayload,
) !?[][]u8 {
    var has_ppt = false;
    for (state.tile_parts) |tp| {
        if (tp.ppt_segments.len > 0) {
            has_ppt = true;
            break;
        }
    }
    if (state.ppm_segments.len == 0 and !has_ppt) return null;

    var lists = try allocator.alloc(std.ArrayListUnmanaged(u8), expected_tiles);
    defer allocator.free(lists);
    for (lists) |*list| list.* = .empty;
    errdefer {
        for (lists) |*list| list.deinit(allocator);
    }

    if (state.ppm_segments.len > 0) {
        _ = tile_payloads;
        var ppm = std.ArrayListUnmanaged(u8).empty;
        defer ppm.deinit(allocator);
        for (state.ppm_segments) |seg| try ppm.appendSlice(allocator, seg.payload);

        var cursor: usize = 0;
        for (state.tile_parts) |tp| {
            if (tp.tile_index >= expected_tiles) return error.UnsupportedNativeDecode;
            if (cursor + 4 > ppm.items.len) return error.TruncatedPacketHeader;
            const nppm: usize = std.mem.readInt(u32, @ptrCast(ppm.items[cursor .. cursor + 4].ptr), .big);
            cursor += 4;
            if (cursor + nppm > ppm.items.len) return error.TruncatedPacketHeader;
            try lists[tp.tile_index].appendSlice(allocator, ppm.items[cursor .. cursor + nppm]);
            cursor += nppm;
        }
        if (!packet.payloadHasOnlyZeroPadding(ppm.items[cursor..])) return error.InvalidPacketSpanLayout;
    } else {
        for (state.tile_parts) |tp| {
            if (tp.tile_index >= expected_tiles) return error.UnsupportedNativeDecode;
            for (tp.ppt_segments) |seg| {
                try lists[tp.tile_index].appendSlice(allocator, seg.payload);
            }
        }
    }

    const headers = try allocator.alloc([]u8, expected_tiles);
    errdefer allocator.free(headers);
    var initialized: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < initialized) : (i += 1) allocator.free(headers[i]);
    }
    for (lists, 0..) |*list, tile_index| {
        headers[tile_index] = try list.toOwnedSlice(allocator);
        initialized += 1;
    }

    return headers;
}

fn mergePpmPacketHeaderPayload(allocator: std.mem.Allocator, state: *const codestream.State) ![]u8 {
    var framed = std.ArrayListUnmanaged(u8).empty;
    defer framed.deinit(allocator);
    for (state.ppm_segments) |seg| try framed.appendSlice(allocator, seg.payload);

    var merged = std.ArrayListUnmanaged(u8).empty;
    errdefer merged.deinit(allocator);
    var cursor: usize = 0;
    while (cursor < framed.items.len) {
        if (cursor + 4 > framed.items.len) return error.TruncatedPacketHeader;
        const nppm: usize = std.mem.readInt(u32, @ptrCast(framed.items[cursor .. cursor + 4].ptr), .big);
        cursor += 4;
        if (cursor + nppm > framed.items.len) return error.TruncatedPacketHeader;
        try merged.appendSlice(allocator, framed.items[cursor .. cursor + nppm]);
        cursor += nppm;
    }
    return try merged.toOwnedSlice(allocator);
}

fn splitPpmHeaderLengthForTile(
    allocator: std.mem.Allocator,
    tile_state: *const codestream.State,
    header_payload: []const u8,
    body_payload_length: usize,
) !usize {
    const modes = [_]packet.PacketHeaderMode{
        .packet_present_tagtree_first_inclusion,
        .packet_present_tagtree_one_based_inclusion,
        .packet_present_tagtree_one_based_zero_bitplanes_zero,
    };
    const prefer_empty_packets = if (tile_state.coding_style) |cs| (cs.scod & 0x06) != 0 else false;
    const include_empty_choices = [_]bool{ prefer_empty_packets, !prefer_empty_packets };

    var best_length: ?usize = null;
    var best_diff: usize = std.math.maxInt(usize);

    for (include_empty_choices) |include_empty| {
        for (modes) |mode| {
            var prefix = packet.buildPacketModelFromSplitPayloadPrefix(
                allocator,
                tile_state,
                header_payload,
                0,
                mode,
                include_empty,
            ) catch continue;
            defer prefix.deinit(allocator);

            var body_length: usize = 0;
            for (prefix.model.entries) |entry| body_length += entry.body_length;
            if (body_length > body_payload_length) continue;
            const diff = body_payload_length - body_length;
            if (diff < best_diff) {
                best_diff = diff;
                best_length = prefix.header_length;
                if (diff == 0) return prefix.header_length;
            }
        }
    }

    return best_length orelse error.InvalidPacketSpanLayout;
}

fn freePackedPacketHeaders(allocator: std.mem.Allocator, headers: ?[][]u8) void {
    if (headers) |items| {
        for (items) |payload| allocator.free(payload);
        allocator.free(items);
    }
}

/// Copy bytes from `src` into `out`, skipping SOP (0xff91 + 4-byte payload)
/// and EPH (0xff92) markers in place. Markers only appear at packet-body
/// boundaries, so we scan byte-by-byte for 0xff and peek the next byte; a
/// matching magic advances past the marker and its fixed-size body.
fn appendStrippedPayload(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    src: []const u8,
    strip_sop: bool,
    strip_eph: bool,
) !void {
    var i: usize = 0;
    while (i < src.len) {
        if (src[i] == 0xff and i + 1 < src.len) {
            const next = src[i + 1];
            if (strip_sop and next == 0x91) {
                // SOP: 0xff91 Lsop(u16)=4 Nsop(u16). Skip 6 bytes total.
                if (i + 6 > src.len) return error.TruncatedPacketBody;
                i += 6;
                continue;
            }
            if (strip_eph and next == 0x92) {
                i += 2;
                continue;
            }
        }
        try out.append(allocator, src[i]);
        i += 1;
    }
}

fn codestreamPayloadInfo(allocator: std.mem.Allocator, codestream_bytes: []const u8) !PayloadInfo {
    return codestreamPayloadInfoWithPackedMode(allocator, codestream_bytes, false);
}

fn codestreamPayloadInfoWithPackedMode(
    allocator: std.mem.Allocator,
    codestream_bytes: []const u8,
    packed_headers: bool,
) !PayloadInfo {
    const payloads = try collectTilePayloadsWithPackedMode(allocator, codestream_bytes, 1, packed_headers);
    defer {
        if (payloads[0].packet_lengths.len > 0) allocator.free(payloads[0].packet_lengths);
        allocator.free(payloads);
    }
    return .{
        .payload = payloads[0].payload,
        .base_offset = payloads[0].base_offset,
    };
}

// ---------------------------------------------------------------------------
// OpenJPEG interoperability tests
// ---------------------------------------------------------------------------

fn readTestFile(allocator: std.mem.Allocator, sub_path: []const u8) ![]u8 {
    return compat.cwd().readFileAlloc(compat.io(), sub_path, allocator, .limited(1024 * 1024));
}
const TestPeakAllocator = struct {
    backing: std.mem.Allocator,
    live_bytes: usize = 0,
    peak_live_bytes: usize = 0,

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn recordGrowth(self: *@This(), bytes: usize) void {
        self.live_bytes += bytes;
        self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
    }

    fn alloc(context: *anyopaque, len: usize, alignment: std.mem.Alignment, return_address: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(context));
        const result = self.backing.rawAlloc(len, alignment, return_address) orelse return null;
        self.recordGrowth(len);
        return result;
    }

    fn resize(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, return_address: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(context));
        if (!self.backing.rawResize(memory, alignment, new_len, return_address)) return false;
        if (new_len > memory.len) {
            self.recordGrowth(new_len - memory.len);
        } else {
            self.live_bytes -= memory.len - new_len;
        }
        return true;
    }

    fn remap(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, return_address: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(context));
        const result = self.backing.rawRemap(memory, alignment, new_len, return_address) orelse return null;
        if (new_len > memory.len) {
            self.recordGrowth(new_len - memory.len);
        } else {
            self.live_bytes -= memory.len - new_len;
        }
        return result;
    }

    fn free(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, return_address: usize) void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.backing.rawFree(memory, alignment, return_address);
        self.live_bytes -= memory.len;
    }
};

test "OfficeQA JPEG 2000 page decodes natively within bounded memory" {
    const fixture = try readTestFile(
        std.testing.allocator,
        "testdata/image/jpeg2000/regression/officeqa-1985-page1-2319x3253.j2k",
    );
    defer std.testing.allocator.free(fixture);

    var peak = TestPeakAllocator{ .backing = std.testing.allocator };
    var decoded = try decodeU8Bytes(peak.allocator(), fixture);
    try std.testing.expectEqual(@as(u32, 2319), decoded.width);
    try std.testing.expectEqual(@as(u32, 3253), decoded.height);
    try std.testing.expectEqual(@as(u8, 3), decoded.components);
    try std.testing.expectEqual(@as(usize, 2319 * 3253 * 3), decoded.pixels.len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(decoded.pixels, &digest, .{});
    try std.testing.expectEqualStrings(
        "bd592833fd9c09b0501d9eb6855c50e557d4df9afaca7d34963edb5af5c1363f",
        &std.fmt.bytesToHex(digest, .lower),
    );
    try std.testing.expect(peak.peak_live_bytes <= 96 * 1024 * 1024);
    decoded.deinit();
    try std.testing.expectEqual(@as(usize, 0), peak.live_bytes);
}
test "larger OfficeQA JPEG 2000 page decodes within 128 MiB" {
    const fixture = try readTestFile(
        std.testing.allocator,
        "testdata/image/jpeg2000/regression/officeqa-1972-page204-2612x3564.j2k",
    );
    defer std.testing.allocator.free(fixture);

    var peak = TestPeakAllocator{ .backing = std.testing.allocator };
    var decoded = try decodeU8Bytes(peak.allocator(), fixture);
    try std.testing.expectEqual(@as(u32, 2612), decoded.width);
    try std.testing.expectEqual(@as(u32, 3564), decoded.height);
    try std.testing.expectEqual(@as(u8, 3), decoded.components);
    try std.testing.expectEqual(@as(usize, 2612 * 3564 * 3), decoded.pixels.len);
    try std.testing.expect(peak.peak_live_bytes <= 128 * 1024 * 1024);
    decoded.deinit();
    try std.testing.expectEqual(@as(usize, 0), peak.live_bytes);
}

test "decode OpenJPEG lossless 8x8 grayscale J2K" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/gray_8x8_lossless_1level.j2k") catch return;
    defer allocator.free(bytes);

    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    try std.testing.expectEqual(@as(u32, 8), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);
    // Verify pixel values match the original gradient (0,4,8,...,252)
    for (decoded.pixels, 0..) |pixel, i| {
        const expected: u8 = @intCast(i * 4);
        try std.testing.expectEqual(expected, pixel);
    }
}

test "decode OpenJPEG lossless 16x16 grayscale 3-level J2K" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/gray_16x16_lossless_3level.j2k") catch return;
    defer allocator.free(bytes);

    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16), decoded.width);
    try std.testing.expectEqual(@as(u32, 16), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);
    // Verify pixel values match the original gradient (0,1,2,...,255)
    for (decoded.pixels, 0..) |pixel, i| {
        try std.testing.expectEqual(@as(u8, @intCast(i)), pixel);
    }
}

test "decode OpenJPEG lossless 8x8 grayscale JP2 container" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/gray_8x8_lossless.jp2") catch return;
    defer allocator.free(bytes);

    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    try std.testing.expectEqual(@as(u32, 8), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);
    for (decoded.pixels, 0..) |pixel, i| {
        const expected: u8 = @intCast(i * 4);
        try std.testing.expectEqual(expected, pixel);
    }
}

test "decode OpenJPEG lossless 4x4 RGB J2K" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/rgb_4x4_lossless.j2k") catch return;
    defer allocator.free(bytes);

    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 4), decoded.width);
    try std.testing.expectEqual(@as(u32, 4), decoded.height);
    try std.testing.expectEqual(@as(u8, 3), decoded.components);
    const expected = [_]u8{
        255, 0,   0,   0,   255, 0,   0,   0,   255, 255, 255, 0,
        128, 128, 128, 64,  64,  64,  200, 100, 50,  10,  20,  30,
        100, 200, 50,  50,  100, 200, 0,   0,   0,   255, 255, 255,
        1,   2,   3,   253, 254, 255, 127, 0,   255, 0,   127, 255,
    };
    try std.testing.expectEqualSlices(u8, &expected, decoded.pixels);
}

test "JP2 channel definitions distinguish opacity from a fourth color component" {
    var metadata = Jp2ColorMetadata{};
    try std.testing.expect(metadata.alphaLayout(4) == null);

    metadata.channel_definitions[0] = .{ .channel = 0, .kind = .color, .association = 1 };
    metadata.channel_definitions[1] = .{ .channel = 1, .kind = .color, .association = 2 };
    metadata.channel_definitions[2] = .{ .channel = 2, .kind = .color, .association = 3 };
    metadata.channel_definitions[3] = .{ .channel = 3, .kind = .opacity, .association = 0 };
    const layout = metadata.alphaLayout(4) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u8, 3), layout.color_count);
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 2 }, layout.color_offsets[0..3]);
    try std.testing.expectEqual(@as(u8, 3), layout.alpha);
    try std.testing.expect(!layout.premultiplied);

    var gray_alpha = Jp2ColorMetadata{};
    gray_alpha.channel_definitions[1] = .{ .channel = 1, .kind = .opacity, .association = 0 };
    const gray_layout = gray_alpha.alphaLayout(2) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u8, 1), gray_layout.color_count);
    try std.testing.expectEqual(@as(u8, 0), gray_layout.color_offsets[0]);
    try std.testing.expectEqual(@as(u8, 1), gray_layout.alpha);

    var cmyk_alpha = Jp2ColorMetadata{};
    cmyk_alpha.channel_definitions[4] = .{ .channel = 4, .kind = .premultiplied_opacity, .association = 0 };
    const cmyk_layout = cmyk_alpha.alphaLayout(5) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u8, 4), cmyk_layout.color_count);
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 2, 3 }, &cmyk_layout.color_offsets);
    try std.testing.expect(cmyk_layout.premultiplied);
}

test "decode OpenJPEG header parsing for lossy J2K" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/gray_16x16_lossy.j2k") catch return;
    defer allocator.free(bytes);

    const header = try decodeHeaderBytes(allocator, bytes);
    try std.testing.expectEqual(@as(u32, 16), header.width);
    try std.testing.expectEqual(@as(u32, 16), header.height);
    try std.testing.expectEqual(@as(u16, 1), header.components);
}

test "decode OpenJPEG flat128 8x8 (all zero coefficients)" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/flat128.j2k") catch return;
    defer allocator.free(bytes);
    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    for (decoded.pixels) |p| try std.testing.expectEqual(@as(u8, 128), p);
}

test "decode OpenJPEG flat100 4x4 no-decomp" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/flat100.j2k") catch return;
    defer allocator.free(bytes);
    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 4), decoded.width);
    for (decoded.pixels) |p| try std.testing.expectEqual(@as(u8, 100), p);
}

test "decode OpenJPEG simple 2x2" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/simple_2x2.j2k") catch return;
    defer allocator.free(bytes);
    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u8, 0), decoded.pixels[0]);
    try std.testing.expectEqual(@as(u8, 255), decoded.pixels[1]);
    try std.testing.expectEqual(@as(u8, 128), decoded.pixels[2]);
    try std.testing.expectEqual(@as(u8, 64), decoded.pixels[3]);
}

test "decode OpenJPEG white 2x2 RGB no-decomp" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/white_2x2.j2k") catch return;
    defer allocator.free(bytes);
    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    for (decoded.pixels) |p| try std.testing.expectEqual(@as(u8, 255), p);
}

test "decode OpenJPEG RGB 2x2 no-decomp with MCT" {
    const allocator = std.testing.allocator;
    const bytes = readTestFile(allocator, "testdata/image/jpeg2000/rgb_2x2_nodecomp.j2k") catch return;
    defer allocator.free(bytes);
    var decoded = try decodeU8Bytes(allocator, bytes);
    defer decoded.deinit();
    // Expected: red(255,0,0), green(0,255,0), blue(0,0,255), gray(128,128,128)
    const expected = [_]u8{ 255, 0, 0, 0, 255, 0, 0, 0, 255, 128, 128, 128 };
    try std.testing.expectEqualSlices(u8, &expected, decoded.pixels);
}

// ---------------------------------------------------------------------------
// Component subsampling (XRsiz/YRsiz > 1) tests.
// ---------------------------------------------------------------------------

const encode = @import("encode.zig");
const markers = @import("markers.zig");

/// Rewrite the SIZ marker in a J2K codestream to declare subsampling factors
/// XRsiz/YRsiz (applied to every component) and replace the image/tile size
/// with `image_w x image_h`. The caller supplies a codestream whose
/// per-component coefficient data already matches `(image_w/xrsiz,
/// image_h/yrsiz)`; the subsampled declaration tells the decoder to upsample
/// the reconstructed plane back to `image_w x image_h`.
fn rewriteSizForSubsampling(
    codestream_bytes: []u8,
    image_w: u32,
    image_h: u32,
    tile_w: u32,
    tile_h: u32,
    xrsiz: u8,
    yrsiz: u8,
) !void {
    // Locate SIZ marker (0xff51) after SOC (0xff4f at offset 0).
    if (codestream_bytes.len < 4) return error.TruncatedCodestream;
    var i: usize = 0;
    var siz_off: usize = 0;
    while (i + 1 < codestream_bytes.len) : (i += 1) {
        if (codestream_bytes[i] == 0xff and codestream_bytes[i + 1] == 0x51) {
            siz_off = i;
            break;
        }
    }
    if (siz_off == 0) return error.SizNotFound;

    // Xsiz at +6..+9, Ysiz at +10..+13.
    std.mem.writeInt(u32, codestream_bytes[siz_off + 6 ..][0..4], image_w, .big);
    std.mem.writeInt(u32, codestream_bytes[siz_off + 10 ..][0..4], image_h, .big);
    // XTsiz at +22..+25, YTsiz at +26..+29.
    std.mem.writeInt(u32, codestream_bytes[siz_off + 22 ..][0..4], tile_w, .big);
    std.mem.writeInt(u32, codestream_bytes[siz_off + 26 ..][0..4], tile_h, .big);
    // Csiz at +38..+39, per-component triple starts at +40.
    const csiz = std.mem.readInt(u16, codestream_bytes[siz_off + 38 ..][0..2], .big);
    var c: usize = 0;
    while (c < csiz) : (c += 1) {
        const off = siz_off + 40 + 3 * c;
        // Ssiz at off, XRsiz at off+1, YRsiz at off+2.
        codestream_bytes[off + 1] = xrsiz;
        codestream_bytes[off + 2] = yrsiz;
    }
}

test "fullNativeDecodeSupport accepts XRsiz/YRsiz > 1" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        0,   16,  32,  48,
        64,  80,  96,  112,
        128, 144, 160, 176,
        192, 208, 224, 240,
    };
    const params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded_const = try encode.encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded_const);
    const encoded = try allocator.dupe(u8, encoded_const);
    defer allocator.free(encoded);

    try rewriteSizForSubsampling(encoded, 8, 8, 8, 8, 2, 2);

    const support = try nativeDecodeSupportBytes(allocator, encoded);
    try std.testing.expectEqual(NativeDecodeSupport.supported, support);
}

test "fullNativeDecodeSupport accepts signed samples" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        0,   16,  32,  48,
        64,  80,  96,  112,
        128, 144, 160, 176,
        192, 208, 224, 240,
    };
    const params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded_const = try encode.encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded_const);
    const encoded = try allocator.dupe(u8, encoded_const);
    defer allocator.free(encoded);

    // Locate SIZ (0xff51) and flip bit 7 of Ssiz on the single component to
    // advertise signed samples. Per-component triple starts at SIZ+40;
    // Ssiz is the first byte of each triple.
    var siz_off: usize = 0;
    {
        var i: usize = 0;
        while (i + 1 < encoded.len) : (i += 1) {
            if (encoded[i] == 0xff and encoded[i + 1] == 0x51) {
                siz_off = i;
                break;
            }
        }
    }
    try std.testing.expect(siz_off != 0);
    encoded[siz_off + 40] |= 0x80;

    const support = try nativeDecodeSupportBytes(allocator, encoded);
    try std.testing.expectEqual(NativeDecodeSupport.supported, support);

    var decoded = try decodeU8Bytes(allocator, encoded);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "decodeU8 upsamples a 2x2-subsampled single-component codestream" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        10,  20,  30,  40,
        50,  60,  70,  80,
        90,  100, 110, 120,
        130, 140, 150, 160,
    };
    const params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded_const = try encode.encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded_const);
    const encoded = try allocator.dupe(u8, encoded_const);
    defer allocator.free(encoded);

    // Rewrite SIZ to claim an 8x8 reference grid with 2:2 subsampling.
    try rewriteSizForSubsampling(encoded, 8, 8, 8, 8, 2, 2);

    var decoded = try decodeU8Bytes(allocator, encoded);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    try std.testing.expectEqual(@as(u32, 8), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);
    try std.testing.expectEqual(@as(usize, 64), decoded.pixels.len);

    // Corners of the upsampled 8x8 grid should equal the four corner samples
    // of the encoded 4x4 plane (bilinear maps corner pixel centers to corners).
    try std.testing.expectEqual(@as(u8, 10), decoded.pixels[0]);
    try std.testing.expectEqual(@as(u8, 40), decoded.pixels[7]);
    try std.testing.expectEqual(@as(u8, 130), decoded.pixels[56]);
    try std.testing.expectEqual(@as(u8, 160), decoded.pixels[63]);
}

test "decodeU16 preserves exact subsampled input" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        0,  32,  64,  96,
        32, 64,  96,  128,
        64, 96,  128, 160,
        96, 128, 160, 192,
    };
    const params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded_const = try encode.encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded_const);
    const encoded = try allocator.dupe(u8, encoded_const);
    defer allocator.free(encoded);
    try rewriteSizForSubsampling(encoded, 8, 8, 8, 8, 2, 2);

    var decoded = try decodeU16Bytes(allocator, encoded);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    try std.testing.expectEqual(@as(u32, 8), decoded.height);
    try std.testing.expectEqual(@as(u8, 1), decoded.components);
    try std.testing.expectEqual(@as(usize, 64), decoded.pixels.len);
    try std.testing.expectEqual(@as(u16, 0), decoded.pixels[0]);
    try std.testing.expectEqual(@as(u16, 192), decoded.pixels[63]);
}

test "decodeU8 and decodeU16 upsample stitched component planes without tile seams" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        0,  32,  64,  96,
        32, 64,  96,  128,
        64, 96,  128, 160,
        96, 128, 160, 192,
    };
    const single_params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    var tiled_params = single_params;
    tiled_params.tile_width = 2;
    tiled_params.tile_height = 2;

    const single_const = try encode.encodeU8Bytes(allocator, &pixels, &single_params);
    defer allocator.free(single_const);
    const tiled_const = try encode.encodeU8Bytes(allocator, &pixels, &tiled_params);
    defer allocator.free(tiled_const);
    const single = try allocator.dupe(u8, single_const);
    defer allocator.free(single);
    const tiled = try allocator.dupe(u8, tiled_const);
    defer allocator.free(tiled);
    try rewriteSizForSubsampling(single, 8, 8, 8, 8, 2, 2);
    try rewriteSizForSubsampling(tiled, 8, 8, 4, 4, 2, 2);

    var single_decoded = try decodeU16Bytes(allocator, single);
    defer single_decoded.deinit();
    var tiled_decoded = try decodeU16Bytes(allocator, tiled);
    defer tiled_decoded.deinit();
    try std.testing.expectEqualSlices(u16, single_decoded.pixels, tiled_decoded.pixels);

    var single_u8 = try decodeU8Bytes(allocator, single);
    defer single_u8.deinit();
    var tiled_u8 = try decodeU8Bytes(allocator, tiled);
    defer tiled_u8.deinit();
    try std.testing.expectEqualSlices(u8, single_u8.pixels, tiled_u8.pixels);
    for (single_u8.pixels, single_decoded.pixels) |sample_u8, sample_u16| {
        try std.testing.expectEqual(@as(u16, sample_u8), sample_u16);
    }

    const pixels_12 = [_]u16{
        0,    511,  1023, 1535,
        512,  1024, 1536, 2048,
        1024, 1537, 2049, 3071,
        1536, 2048, 3072, 4095,
    };
    var single_12_params = single_params;
    single_12_params.bits_per_component = 12;
    var tiled_12_params = single_12_params;
    tiled_12_params.tile_width = 2;
    tiled_12_params.tile_height = 2;
    const single_12_const = try encode.encodeU16Bytes(allocator, &pixels_12, &single_12_params);
    defer allocator.free(single_12_const);
    const tiled_12_const = try encode.encodeU16Bytes(allocator, &pixels_12, &tiled_12_params);
    defer allocator.free(tiled_12_const);
    const single_12 = try allocator.dupe(u8, single_12_const);
    defer allocator.free(single_12);
    const tiled_12 = try allocator.dupe(u8, tiled_12_const);
    defer allocator.free(tiled_12);
    try rewriteSizForSubsampling(single_12, 8, 8, 8, 8, 2, 2);
    try rewriteSizForSubsampling(tiled_12, 8, 8, 4, 4, 2, 2);

    var single_12_u8 = try decodeU8Bytes(allocator, single_12);
    defer single_12_u8.deinit();
    var tiled_12_u8 = try decodeU8Bytes(allocator, tiled_12);
    defer tiled_12_u8.deinit();
    try std.testing.expectEqualSlices(u8, single_12_u8.pixels, tiled_12_u8.pixels);
    var single_12_u16 = try decodeU16Bytes(allocator, single_12);
    defer single_12_u16.deinit();
    for (single_12_u8.pixels, single_12_u16.pixels) |sample_u8, sample_u16| {
        try std.testing.expectEqual(@as(u8, @intCast(sample_u16 >> 4)), sample_u8);
    }
}

test "subsampled irreversible U8 clips native samples before interpolation" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        0,   255, 0,   255,
        255, 0,   255, 0,
        0,   255, 0,   255,
        255, 0,   255, 0,
    };
    const single_params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 1,
        .wavelet_transform = 0,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    var tiled_params = single_params;
    tiled_params.tile_width = 2;
    tiled_params.tile_height = 2;

    const single_const = try encode.encodeU8Bytes(allocator, &pixels, &single_params);
    defer allocator.free(single_const);
    const tiled_const = try encode.encodeU8Bytes(allocator, &pixels, &tiled_params);
    defer allocator.free(tiled_const);
    const single = try allocator.dupe(u8, single_const);
    defer allocator.free(single);
    const tiled = try allocator.dupe(u8, tiled_const);
    defer allocator.free(tiled);
    try rewriteSizForSubsampling(single, 8, 8, 8, 8, 2, 2);
    try rewriteSizForSubsampling(tiled, 8, 8, 4, 4, 2, 2);

    var single_state = try codestream.parseState(allocator, single);
    defer single_state.deinit(allocator);
    try std.testing.expect(try reconstruct.StreamingPlaneAssembler.canAssemble(&single_state));

    var single_u8 = try decodeU8Bytes(allocator, single);
    defer single_u8.deinit();
    var single_u16 = try decodeU16Bytes(allocator, single);
    defer single_u16.deinit();
    for (single_u8.pixels, single_u16.pixels) |sample_u8, sample_u16| {
        try std.testing.expectEqual(@as(u16, sample_u8), sample_u16);
    }

    var tiled_u8 = try decodeU8Bytes(allocator, tiled);
    defer tiled_u8.deinit();
    var tiled_u16 = try decodeU16Bytes(allocator, tiled);
    defer tiled_u16.deinit();
    for (tiled_u8.pixels, tiled_u16.pixels) |sample_u8, sample_u16| {
        try std.testing.expectEqual(@as(u16, sample_u8), sample_u16);
    }
}

test "decode supports native gray-alpha and high-bit CMYK component layouts" {
    const allocator = std.testing.allocator;
    const gray_alpha = [_]u8{ 10, 20, 30, 40, 50, 60, 70, 80 };
    const u8_params = encode.EncodeParams{
        .width = 2,
        .height = 2,
        .components = 2,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .format = .j2k,
    };
    const gray_alpha_encoded = try encode.encodeU8Bytes(allocator, &gray_alpha, &u8_params);
    defer allocator.free(gray_alpha_encoded);
    var gray_alpha_decoded = try decodeU8Bytes(allocator, gray_alpha_encoded);
    defer gray_alpha_decoded.deinit();
    try std.testing.expectEqual(@as(u8, 2), gray_alpha_decoded.components);
    try std.testing.expectEqualSlices(u8, &gray_alpha, gray_alpha_decoded.pixels);

    const cmyk = [_]u16{
        0,   100, 200,  300,
        400, 500, 600,  700,
        800, 900, 1000, 1023,
        12,  34,  56,   78,
    };
    const u16_params = encode.EncodeParams{
        .width = 2,
        .height = 2,
        .components = 4,
        .bits_per_component = 10,
        .decomposition_levels = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = true,
        .format = .j2k,
    };
    const cmyk_encoded = try encode.encodeU16Bytes(allocator, &cmyk, &u16_params);
    defer allocator.free(cmyk_encoded);
    var cmyk_decoded = try decodeU16Bytes(allocator, cmyk_encoded);
    defer cmyk_decoded.deinit();
    try std.testing.expectEqual(@as(u8, 4), cmyk_decoded.components);
    try std.testing.expectEqualSlices(u16, &cmyk, cmyk_decoded.pixels);
}

const SplitTilePayload = struct {
    header: []u8,
    body: []u8,

    fn deinit(self: *SplitTilePayload, allocator: std.mem.Allocator) void {
        allocator.free(self.header);
        allocator.free(self.body);
        self.* = undefined;
    }
};

fn splitSingleTilePacketPayload(allocator: std.mem.Allocator, encoded: []const u8) !SplitTilePayload {
    var state = try codestream.parseState(allocator, encoded);
    defer state.deinit(allocator);

    const payload_info = try codestreamPayloadInfo(allocator, encoded);
    defer allocator.free(payload_info.payload);

    var packet_model = try packet.buildPacketModelFromPayload(
        allocator,
        &state,
        payload_info.payload,
        0,
        .packet_present_tagtree_first_inclusion,
    );
    defer packet_model.deinit(allocator);

    if (packet_model.entries.len == 0) return error.InvalidPacketSpanLayout;
    var header_len: usize = std.math.maxInt(usize);
    for (packet_model.entries) |entry| header_len = @min(header_len, entry.body_offset);
    if (header_len > payload_info.payload.len) return error.InvalidPacketSpanLayout;

    return .{
        .header = try allocator.dupe(u8, payload_info.payload[0..header_len]),
        .body = try allocator.dupe(u8, payload_info.payload[header_len..]),
    };
}

fn packedHeaderMarker(allocator: std.mem.Allocator, marker: u16, sequence: u8, payload: []const u8) ![]u8 {
    if (payload.len + 3 > std.math.maxInt(u16)) return error.PacketHeaderTooLarge;
    const out = try allocator.alloc(u8, payload.len + 5);
    std.mem.writeInt(u16, out[0..2], marker, .big);
    std.mem.writeInt(u16, out[2..4], @intCast(payload.len + 3), .big);
    out[4] = sequence;
    @memcpy(out[5..], payload);
    return out;
}

fn withPptPacketHeaders(allocator: std.mem.Allocator, encoded: []const u8, split: SplitTilePayload) ![]u8 {
    const ranges = try codestream.parseTilePartRanges(allocator, encoded);
    defer allocator.free(ranges);
    if (ranges.len != 1) return error.UnsupportedTileLayout;
    const range = ranges[0];

    const ppt = try packedHeaderMarker(allocator, markers.ppt, 0, split.header);
    defer allocator.free(ppt);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, encoded[0..range.sod_offset]);
    try out.appendSlice(allocator, ppt);
    try out.appendSlice(allocator, encoded[range.sod_offset..range.data_offset]);
    try out.appendSlice(allocator, split.body);
    try out.appendSlice(allocator, encoded[range.next_offset..]);

    const psot: u32 = @intCast(12 + ppt.len + 2 + split.body.len);
    std.mem.writeInt(u32, out.items[range.sot_offset + 6 ..][0..4], psot, .big);
    return try out.toOwnedSlice(allocator);
}

fn withPpmPacketHeaders(allocator: std.mem.Allocator, encoded: []const u8, split: SplitTilePayload) ![]u8 {
    const ranges = try codestream.parseTilePartRanges(allocator, encoded);
    defer allocator.free(ranges);
    if (ranges.len != 1) return error.UnsupportedTileLayout;
    const range = ranges[0];

    var ppm_payload = std.ArrayListUnmanaged(u8).empty;
    defer ppm_payload.deinit(allocator);
    try ppm_payload.resize(allocator, 4);
    std.mem.writeInt(u32, ppm_payload.items[0..4], @intCast(split.header.len), .big);
    try ppm_payload.appendSlice(allocator, split.header);

    const ppm = try packedHeaderMarker(allocator, markers.ppm, 0, ppm_payload.items);
    defer allocator.free(ppm);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, encoded[0..range.sot_offset]);
    try out.appendSlice(allocator, ppm);
    const new_sot_offset = out.items.len;
    try out.appendSlice(allocator, encoded[range.sot_offset..range.data_offset]);
    try out.appendSlice(allocator, split.body);
    try out.appendSlice(allocator, encoded[range.next_offset..]);

    const psot: u32 = @intCast(12 + 2 + split.body.len);
    std.mem.writeInt(u32, out.items[new_sot_offset + 6 ..][0..4], psot, .big);
    return try out.toOwnedSlice(allocator);
}

test "decode consumes PPT packed packet headers" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        0,   16,  32,  48,
        64,  80,  96,  112,
        128, 144, 160, 176,
        192, 208, 224, 240,
    };
    const params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded = try encode.encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded);
    var split = try splitSingleTilePacketPayload(allocator, encoded);
    defer split.deinit(allocator);

    const packed_bytes = try withPptPacketHeaders(allocator, encoded, split);
    defer allocator.free(packed_bytes);
    try std.testing.expectEqual(NativeDecodeSupport.supported, try nativeDecodeSupportBytes(allocator, packed_bytes));

    var decoded = try decodeU8Bytes(allocator, packed_bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

test "decode consumes PPM packed packet headers" {
    const allocator = std.testing.allocator;
    const pixels = [_]u8{
        3,   17,  31,  47,
        63,  79,  95,  111,
        127, 143, 159, 175,
        191, 207, 223, 239,
    };
    const params = encode.EncodeParams{
        .width = 4,
        .height = 4,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 0,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded = try encode.encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded);
    var split = try splitSingleTilePacketPayload(allocator, encoded);
    defer split.deinit(allocator);

    const packed_bytes = try withPpmPacketHeaders(allocator, encoded, split);
    defer allocator.free(packed_bytes);
    try std.testing.expectEqual(NativeDecodeSupport.supported, try nativeDecodeSupportBytes(allocator, packed_bytes));

    var decoded = try decodeU8Bytes(allocator, packed_bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded.pixels);
}

/// Excise the first COM segment (marker 0xFF64) from a codestream. Returns a
/// new buffer; caller frees. Used by the midpoint-discriminator test to
/// simulate a foreign producer (decoder then falls to midpoint reconstruction).
fn stripFirstComMarker(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    var i: usize = 0;
    while (i + 3 < bytes.len) : (i += 1) {
        if (bytes[i] == 0xff and bytes[i + 1] == 0x64) {
            const lcom = (@as(usize, bytes[i + 2]) << 8) | bytes[i + 3];
            const segment_end = i + 2 + lcom;
            if (segment_end > bytes.len) return error.MalformedComMarker;
            const out = try allocator.alloc(u8, bytes.len - (segment_end - i));
            @memcpy(out[0..i], bytes[0..i]);
            @memcpy(out[i..], bytes[segment_end..]);
            return out;
        }
    }
    return error.NoComMarkerFound;
}

test "producer COM tag is emitted after QCD" {
    const allocator = std.testing.allocator;

    var pixels: [16 * 16]u8 = undefined;
    for (0..pixels.len) |idx| pixels[idx] = @intCast((idx * 3) & 0xff);

    const params = encode.EncodeParams{
        .width = 16,
        .height = 16,
        .components = 1,
        .bits_per_component = 8,
        .decomposition_levels = 3,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 1,
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded = try encode.encodeU8Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded);

    try std.testing.expect(std.mem.indexOf(u8, encoded, antfly_producer_tag) != null);

    // Round-trip after stripping must still decode (COM is ignorable per ISO).
    const stripped = try stripFirstComMarker(allocator, encoded);
    defer allocator.free(stripped);
    try std.testing.expect(std.mem.indexOf(u8, stripped, antfly_producer_tag) == null);
    var decoded_stripped = try decodeU8Bytes(allocator, stripped);
    defer decoded_stripped.deinit();
    try std.testing.expectEqualSlices(u8, &pixels, decoded_stripped.pixels);
}

test "producer COM tag gates midpoint reconstruction for 12-bit 9/7 irreversible" {
    const allocator = std.testing.allocator;

    var pixels: [16 * 16]u16 = undefined;
    for (0..pixels.len) |idx| pixels[idx] = @intCast((idx * 23) & 0x0fff);

    const params = encode.EncodeParams{
        .width = 16,
        .height = 16,
        .components = 1,
        .bits_per_component = 12,
        .decomposition_levels = 3,
        .num_layers = 1,
        .progression_order = 0,
        .wavelet_transform = 0, // 9/7 irreversible
        .multiple_component_transform = false,
        .code_block_width_exponent = 2,
        .code_block_height_exponent = 2,
        .format = .j2k,
    };
    const encoded = try encode.encodeU16Bytes(allocator, &pixels, &params);
    defer allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, antfly_producer_tag) != null);

    var decoded_ours = try decodeU16Bytes(allocator, encoded);
    defer decoded_ours.deinit();

    const stripped = try stripFirstComMarker(allocator, encoded);
    defer allocator.free(stripped);
    try std.testing.expect(std.mem.indexOf(u8, stripped, antfly_producer_tag) == null);

    var decoded_foreign = try decodeU16Bytes(allocator, stripped);
    defer decoded_foreign.deinit();

    try std.testing.expectEqual(decoded_ours.pixels.len, decoded_foreign.pixels.len);

    // Under the policy flip (exact_bitplane → midpoint + standard_additive),
    // reconstruction is shifted by ~Δ/2 on nonzero coefficients. For 12-bit
    // 9/7 this must produce a measurable difference; zero diffs would mean
    // the COM tag isn't reaching the tier-1 policy selection site.
    var diff_count: usize = 0;
    for (decoded_ours.pixels, decoded_foreign.pixels) |a, b| {
        if (a != b) diff_count += 1;
    }
    try std.testing.expect(diff_count > 0);
}
