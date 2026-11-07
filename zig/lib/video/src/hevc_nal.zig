// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! NAL unit header (ITU-T H.265 clause 7.3.1.2), length-prefixed demuxing for
//! ISO/IEC 14496-15 `hvc1`/`hev1` samples, and the HEVCDecoderConfigurationRecord
//! (`hvcC`, ISO/IEC 14496-15 clause 8.3.3.1). Written from the specifications;
//! field order was additionally cross-checked against this repository's own
//! fixtures using FFmpeg's `trace_headers` bitstream filter as an independent
//! oracle, not against any decoder's source.
const std = @import("std");

/// clause 7.4.2. Values 24-31 are reserved IRAP types we also route as IRAP;
/// values not named below are either VCL (<=31) or non-VCL and handled by type.
pub const NalUnitType = enum(u6) {
    trail_n = 0,
    trail_r = 1,
    tsa_n = 2,
    tsa_r = 3,
    stsa_n = 4,
    stsa_r = 5,
    radl_n = 6,
    radl_r = 7,
    rasl_n = 8,
    rasl_r = 9,
    rsv_vcl_n10 = 10,
    rsv_vcl_r11 = 11,
    rsv_vcl_n12 = 12,
    rsv_vcl_r13 = 13,
    rsv_vcl_n14 = 14,
    rsv_vcl_r15 = 15,
    bla_w_lp = 16,
    bla_w_radl = 17,
    bla_n_lp = 18,
    idr_w_radl = 19,
    idr_n_lp = 20,
    cra_nut = 21,
    rsv_irap_vcl22 = 22,
    rsv_irap_vcl23 = 23,
    vps_nut = 32,
    sps_nut = 33,
    pps_nut = 34,
    aud_nut = 35,
    eos_nut = 36,
    eob_nut = 37,
    fd_nut = 38,
    prefix_sei_nut = 39,
    suffix_sei_nut = 40,
    _,

    pub fn raw(self: NalUnitType) u6 {
        return @backingInt(self);
    }
    pub fn isVcl(self: NalUnitType) bool {
        return self.raw() <= 31;
    }
    pub fn isIrap(self: NalUnitType) bool {
        const v = self.raw();
        return v >= 16 and v <= 23;
    }
    pub fn isIdr(self: NalUnitType) bool {
        const v = self.raw();
        return v == 19 or v == 20;
    }
    pub fn isBla(self: NalUnitType) bool {
        const v = self.raw();
        return v >= 16 and v <= 18;
    }
    pub fn isCra(self: NalUnitType) bool {
        return self.raw() == 21;
    }
    pub fn isRasl(self: NalUnitType) bool {
        const v = self.raw();
        return v == 8 or v == 9;
    }
    pub fn isRadl(self: NalUnitType) bool {
        const v = self.raw();
        return v == 6 or v == 7;
    }
    /// clause 3.139/7.4.3.1: picture produced by this VCL NAL is used as a
    /// reference by other pictures of the same temporal sub-layer or not.
    pub fn isSubLayerReference(self: NalUnitType) bool {
        const v = self.raw();
        return v <= 14 and v % 2 == 1;
    }
    pub fn isSubLayerNonReference(self: NalUnitType) bool {
        const v = self.raw();
        return v <= 14 and v % 2 == 0;
    }
};

pub const Header = struct {
    nal_unit_type: NalUnitType,
    layer_id: u6,
    temporal_id: u3,
};

/// Parses the 2-byte NAL unit header (clause 7.3.1.2) and returns it with the
/// remaining bytes (the RBSP, still containing emulation-prevention bytes).
pub fn parseHeader(nal: []const u8) !struct { header: Header, rbsp: []const u8 } {
    if (nal.len < 3) return error.MalformedVideoPacket;
    if (nal[0] & 0x80 != 0) return error.MalformedVideoPacket; // forbidden_zero_bit
    const temporal_id_plus1: u3 = @intCast(nal[1] & 0x07);
    if (temporal_id_plus1 == 0) return error.MalformedVideoPacket;
    return .{
        .header = .{
            .nal_unit_type = @fromBackingInt(@intCast(@as(u6, @intCast((nal[0] & 0x7f) >> 1)))),
            .layer_id = @intCast(((@as(u16, nal[0] & 0x01) << 5) | (nal[1] >> 3))),
            .temporal_id = temporal_id_plus1 - 1,
        },
        .rbsp = nal[2..],
    };
}

/// Splits a `nal_length_size`-byte-length-prefixed packet (ISO/IEC 14496-15
/// sample format) into NAL units, borrowing from `packet`. `nal_length_size`
/// must be 1-4 (hvcC `lengthSizeMinusOne` + 1); any other value is rejected by
/// the caller before constructing this iterator.
pub const LengthPrefixedIterator = struct {
    bytes: []const u8,
    length_size: u8,
    at: usize = 0,

    pub fn next(self: *LengthPrefixedIterator) !?[]const u8 {
        if (self.at == self.bytes.len) return null;
        if (self.bytes.len - self.at < self.length_size) return error.MalformedVideoPacket;
        var len: usize = 0;
        for (0..self.length_size) |i| len = (len << 8) | self.bytes[self.at + i];
        self.at += self.length_size;
        if (len == 0 or len > self.bytes.len - self.at) return error.MalformedVideoPacket;
        const nal = self.bytes[self.at..][0..len];
        self.at += len;
        return nal;
    }
};

/// Fixed-size fields of the HEVCDecoderConfigurationRecord preceding the
/// per-array NAL unit list (ISO/IEC 14496-15 clause 8.3.3.1.2), plus an
/// iterator over that list. Array NAL bytes are borrowed from `config`.
pub const ConfigurationRecord = struct {
    general_profile_space: u2,
    general_tier_flag: bool,
    general_profile_idc: u5,
    general_profile_compatibility_flags: u32,
    general_constraint_indicator_flags: u48,
    general_level_idc: u8,
    min_spatial_segmentation_idc: u12,
    parallelism_type: u2,
    chroma_format_idc: u2,
    bit_depth_luma: u8,
    bit_depth_chroma: u8,
    avg_frame_rate: u16,
    num_temporal_layers: u3,
    temporal_id_nested: bool,
    length_size: u8,
    num_arrays: u8 = 0,
    arrays_at: usize,
    bytes: []const u8,

    pub const Array = struct { nal_unit_type: u6, completeness: bool, nals: NalListIterator };
    pub const NalListIterator = struct {
        bytes: []const u8,
        at: usize = 0,
        remaining: u16,
        pub fn next(self: *NalListIterator) !?[]const u8 {
            if (self.remaining == 0) return null;
            if (self.bytes.len - self.at < 2) return error.MalformedVideoConfig;
            const len = std.mem.readInt(u16, self.bytes[self.at..][0..2], .big);
            self.at += 2;
            if (len == 0 or len > self.bytes.len - self.at) return error.MalformedVideoConfig;
            const nal = self.bytes[self.at..][0..len];
            self.at += len;
            self.remaining -= 1;
            return nal;
        }
    };
    pub const ArrayIterator = struct {
        bytes: []const u8,
        at: usize,
        remaining: u8,
        pub fn next(self: *ArrayIterator) !?Array {
            if (self.remaining == 0) return null;
            if (self.bytes.len - self.at < 3) return error.MalformedVideoConfig;
            const header = self.bytes[self.at];
            const num_nalus = std.mem.readInt(u16, self.bytes[self.at + 1 ..][0..2], .big);
            self.at += 3;
            var cursor: usize = self.at;
            var scanned: u16 = 0;
            while (scanned < num_nalus) : (scanned += 1) {
                if (self.bytes.len - cursor < 2) return error.MalformedVideoConfig;
                const len = std.mem.readInt(u16, self.bytes[cursor..][0..2], .big);
                cursor += 2;
                if (len > self.bytes.len - cursor) return error.MalformedVideoConfig;
                cursor += len;
            }
            const array = Array{
                .nal_unit_type = @intCast(header & 0x3f),
                .completeness = header & 0x80 != 0,
                .nals = .{ .bytes = self.bytes[self.at..cursor], .remaining = num_nalus },
            };
            self.at = cursor;
            self.remaining -= 1;
            return array;
        }
    };

    pub fn arrays(self: ConfigurationRecord) ArrayIterator {
        return .{ .bytes = self.bytes, .at = self.arrays_at, .remaining = self.num_arrays };
    }
};

/// Parses the fixed-size header fields of an HEVCDecoderConfigurationRecord
/// and returns an iterator positioned at the array list; `numOfArrays` is
/// read separately so callers can bound total array/NAL counts up front.
pub fn parseConfigurationRecord(config: []const u8) !ConfigurationRecord {
    if (config.len < 23) return error.MalformedVideoConfig;
    if (config[0] != 1) return error.MalformedVideoConfig; // configurationVersion
    const general_profile_space: u2 = @intCast(config[1] >> 6);
    const general_tier_flag = config[1] & 0x20 != 0;
    const general_profile_idc: u5 = @intCast(config[1] & 0x1f);
    const general_profile_compatibility_flags = std.mem.readInt(u32, config[2..6], .big);
    var constraint_bytes: [8]u8 = @splat(0);
    @memcpy(constraint_bytes[2..8], config[6..12]);
    const general_constraint_indicator_flags: u48 = @truncate(std.mem.readInt(u64, &constraint_bytes, .big));
    const general_level_idc = config[12];
    const min_spatial_segmentation_idc: u12 = @intCast((@as(u16, config[13] & 0x0f) << 8) | config[14]);
    const parallelism_type: u2 = @intCast(config[15] & 0x03);
    const chroma_format_idc: u2 = @intCast(config[16] & 0x03);
    const bit_depth_luma: u8 = @as(u8, @intCast(config[17] & 0x07)) + 8;
    const bit_depth_chroma: u8 = @as(u8, @intCast(config[18] & 0x07)) + 8;
    const avg_frame_rate = std.mem.readInt(u16, config[19..21], .big);
    const num_temporal_layers: u3 = @intCast((config[21] >> 3) & 0x07);
    const temporal_id_nested = config[21] & 0x04 != 0;
    const length_size: u8 = @as(u8, @intCast(config[21] & 0x03)) + 1;
    const num_arrays = config[22];
    var record = ConfigurationRecord{
        .general_profile_space = general_profile_space,
        .general_tier_flag = general_tier_flag,
        .general_profile_idc = general_profile_idc,
        .general_profile_compatibility_flags = general_profile_compatibility_flags,
        .general_constraint_indicator_flags = general_constraint_indicator_flags,
        .general_level_idc = general_level_idc,
        .min_spatial_segmentation_idc = min_spatial_segmentation_idc,
        .parallelism_type = parallelism_type,
        .chroma_format_idc = chroma_format_idc,
        .bit_depth_luma = bit_depth_luma,
        .bit_depth_chroma = bit_depth_chroma,
        .avg_frame_rate = avg_frame_rate,
        .num_temporal_layers = num_temporal_layers,
        .temporal_id_nested = temporal_id_nested,
        .length_size = length_size,
        .arrays_at = 23,
        .bytes = config,
    };
    // Validate the array list up front so `arrays()` below cannot be handed a
    // truncated count; store it out of band since ArrayIterator.remaining is
    // recomputed in `arrays()` from this validated scan.
    var it = ConfigurationRecord.ArrayIterator{ .bytes = config, .at = 23, .remaining = num_arrays };
    while (try it.next()) |_| {}
    record.arrays_at = 23;
    record.num_arrays = num_arrays;
    return record;
}

test "HEVC NAL header parses forbidden/type/layer/temporal fields" {
    const parsed = try parseHeader(&.{ 0x42, 0x01, 0x00 });
    try std.testing.expectEqual(NalUnitType.sps_nut, parsed.header.nal_unit_type);
    try std.testing.expectEqual(@as(u6, 0), parsed.header.layer_id);
    try std.testing.expectEqual(@as(u3, 0), parsed.header.temporal_id);
    try std.testing.expectError(error.MalformedVideoPacket, parseHeader(&.{ 0x80, 0x01, 0x00 }));
    try std.testing.expectError(error.MalformedVideoPacket, parseHeader(&.{ 0x42, 0x00, 0x00 }));
}

test "HEVC length-prefixed iterator rejects truncation" {
    var it = LengthPrefixedIterator{ .bytes = &.{ 0x00, 0x00, 0x00, 0x02, 0xaa, 0xbb }, .length_size = 4 };
    const first = (try it.next()).?;
    try std.testing.expectEqualSlices(u8, &.{ 0xaa, 0xbb }, first);
    try std.testing.expectEqual(@as(?[]const u8, null), try it.next());
    var bad = LengthPrefixedIterator{ .bytes = &.{ 0x00, 0x00, 0x00, 0x05, 0xaa }, .length_size = 4 };
    try std.testing.expectError(error.MalformedVideoPacket, bad.next());
}
