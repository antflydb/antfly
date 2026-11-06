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

//! What an hvcC's SPS declares about color and pixel aspect. Containers can omit
//! both, and a decoder surface doesn't report them, so routes read them from here.
const std = @import("std");
const bits_mod = @import("hevc_bits.zig");
const nal = @import("hevc_nal.zig");
const params = @import("hevc_params.zig");

pub const Signal = struct {
    primaries: ?u8 = null,
    transfer: ?u8 = null,
    matrix: ?u8 = null,
    full_range: bool = false,
    sar: Aspect = .{},
};
pub const Aspect = struct { horizontal: u32 = 1, vertical: u32 = 1 };

fn specified(value: ?u8) ?u8 {
    const code = value orelse return null;
    return if (code == 2) null else code;
}

/// The first SPS in the record. Null when parameter sets are only in-band; a record
/// that declares an SPS this parser refuses is an error, not an SDR default.
pub fn parse(a: std.mem.Allocator, config: []const u8) !?Signal {
    const record = try nal.parseConfigurationRecord(config);
    var arrays = record.arrays();
    while (try arrays.next()) |array| {
        if (array.nal_unit_type != 33) continue;
        var nals = array.nals;
        const unit = (try nals.next()) orelse continue;
        const parsed = try nal.parseHeader(unit);
        var bits = try bits_mod.Bits.init(a, parsed.rbsp);
        defer bits.deinit();
        const sps = try params.parseSps(&bits);
        const vui = sps.vui;
        return .{
            .primaries = specified(vui.colour_primaries),
            .transfer = specified(vui.transfer_characteristics),
            .matrix = specified(vui.matrix_coefficients),
            .full_range = vui.video_full_range_flag,
            .sar = if (vui.sar_width != 0 and vui.sar_height != 0) .{ .horizontal = vui.sar_width, .vertical = vui.sar_height } else .{},
        };
    }
    return null;
}

/// `hev1` clips may carry parameter sets only in their samples. A record built from
/// the first sample lets routes that need them up front treat the clip like any
/// other. Caller owns the result; null when the record has sets or the sample has none.
pub fn fromInband(a: std.mem.Allocator, config: []const u8, sample: []const u8, length_bytes: u3) !?[]u8 {
    if (config.len < 23) return error.MalformedVideoConfig;
    if (config[22] != 0) return null;
    var units: [3]std.ArrayList([]const u8) = @splat(.empty);
    defer for (&units) |*list| list.deinit(a);
    var cursor: usize = 0;
    while (cursor < sample.len) {
        if (sample.len - cursor < length_bytes) return error.MalformedVideoConfig;
        var size: usize = 0;
        for (sample[cursor..][0..length_bytes]) |byte| size = (size << 8) | byte;
        cursor += length_bytes;
        if (size < 2 or size > sample.len - cursor) return error.MalformedVideoConfig;
        const unit = sample[cursor..][0..size];
        cursor += size;
        const kind = (unit[0] >> 1) & 0x3f;
        if (kind < 32) break;
        if (kind <= 34) try units[kind - 32].append(a, unit);
    }
    if (units[1].items.len == 0 or units[2].items.len == 0) return null;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, config[0..22]);
    var arrays: u8 = 0;
    for (&units) |*list| arrays += @intFromBool(list.items.len != 0);
    try out.append(a, arrays);
    for (&units, 32..) |*list, kind| {
        if (list.items.len == 0) continue;
        try out.append(a, @intCast(kind));
        try out.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u16, @intCast(list.items.len))));
        for (list.items) |unit| {
            try out.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u16, std.math.cast(u16, unit.len) orelse return error.MalformedVideoConfig)));
            try out.appendSlice(a, unit);
        }
    }
    return try out.toOwnedSlice(a);
}

/// The container's color declaration when present, which must agree with the stream.
pub fn fullRange(signal: ?Signal, color_info: []const u8) !bool {
    const nclx = color_info.len >= 11 and std.mem.eql(u8, color_info[0..4], "nclx");
    const declared = nclx and color_info[10] & 0x80 != 0;
    if (signal) |value| {
        if (nclx and declared != value.full_range) return error.UnsupportedVideoColor;
        return value.full_range;
    }
    return declared;
}

test "SPS signalling survives without any container color" {
    const a = std.testing.allocator;
    const fixture = @embedFile("../testdata/hevc-hlg-config.bin");
    const signal = (try parse(a, fixture)).?;
    try std.testing.expectEqual(@as(?u8, 9), signal.primaries);
    try std.testing.expectEqual(@as(?u8, 18), signal.transfer);
    try std.testing.expectEqual(@as(?u8, 9), signal.matrix);
    try std.testing.expect(!signal.full_range);
    try std.testing.expect(!try fullRange(signal, ""));
    try std.testing.expectError(error.UnsupportedVideoColor, fullRange(.{ .full_range = false }, "nclx" ++ [_]u8{ 0, 9, 0, 18, 0, 9, 0x80 }));
    try std.testing.expect(try fullRange(null, "nclx" ++ [_]u8{ 0, 9, 0, 18, 0, 9, 0x80 }));
}

test "A record without parameter sets carries no signal" {
    var config = @as([23]u8, @splat(0));
    config[0] = 1;
    config[21] = 3;
    try std.testing.expect((try parse(std.testing.allocator, &config)) == null);
}

/// Writes `bits` of `source` from `start`, into a byte buffer being built.
const BitWriter = struct {
    bytes: []u8,
    position: usize = 0,
    fn put(self: *BitWriter, bit: u1) void {
        const byte = self.position / 8;
        const mask = @as(u8, 0x80) >> @intCast(self.position % 8);
        if (bit == 1) self.bytes[byte] |= mask else self.bytes[byte] &= ~mask;
        self.position += 1;
    }
};

/// A NAL payload can't hold a long run of zeros without emulation prevention.
fn escaped(a: std.mem.Allocator, raw: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    var zeros: usize = 0;
    for (raw) |byte| {
        if (zeros == 2 and byte <= 3) {
            try out.append(a, 3);
            zeros = 0;
        }
        try out.append(a, byte);
        zeros = if (byte == 0) zeros + 1 else 0;
    }
    return out.toOwnedSlice(a);
}

test "A huge Exp-Golomb value anywhere in real parameter sets is an error, not a panic" {
    const a = std.testing.allocator;
    const record = try nal.parseConfigurationRecord(@embedFile("../testdata/hevc-hlg-config.bin"));
    var sets: [2][]const u8 = undefined;
    var arrays = record.arrays();
    while (try arrays.next()) |array| {
        var nals = array.nals;
        const unit = (try nals.next()) orelse continue;
        if (array.nal_unit_type == 33) sets[0] = (try nal.parseHeader(unit)).rbsp;
        if (array.nal_unit_type == 34) sets[1] = (try nal.parseHeader(unit)).rbsp;
    }
    const buffer = try a.alloc(u8, @max(sets[0].len, sets[1].len) + 16);
    defer a.free(buffer);
    for (sets, 0..) |set, kind| {
        for (0..set.len * 8) |offset| {
            // Leading zeros then 32 one bits: the largest value ue(v) can return.
            var writer = BitWriter{ .bytes = buffer };
            for (0..offset) |bit| writer.put(@intCast((set[bit / 8] >> @intCast(7 - bit % 8)) & 1));
            for (0..31) |_| writer.put(0);
            for (0..32) |_| writer.put(1);
            for (offset..set.len * 8) |bit| writer.put(@intCast((set[bit / 8] >> @intCast(7 - bit % 8)) & 1));
            const payload = try escaped(a, buffer[0 .. (writer.position + 7) / 8]);
            defer a.free(payload);
            var bits = bits_mod.Bits.init(a, payload) catch continue;
            defer bits.deinit();
            if (kind == 0) {
                _ = params.parseSps(&bits) catch continue;
            } else {
                _ = params.parsePps(&bits) catch continue;
            }
        }
    }
}

test "An SPS depth of 0xfffffffe is refused" {
    const a = std.testing.allocator;
    var bytes = @as([64]u8, @splat(0));
    var writer = BitWriter{ .bytes = &bytes };
    const ue = struct {
        fn put(w: *BitWriter, value: u64) void {
            const coded = value + 1;
            const width: u7 = @intCast(64 - @clz(coded));
            for (0..width - 1) |_| w.put(0);
            var bit: u7 = width;
            while (bit != 0) {
                bit -= 1;
                w.put(@intCast((coded >> @intCast(bit)) & 1));
            }
        }
    }.put;
    for (0..4) |_| writer.put(0); // sps_video_parameter_set_id
    for (0..3) |_| writer.put(0); // sps_max_sub_layers_minus1
    writer.put(1); // sps_temporal_id_nesting_flag
    for (0..96) |_| writer.put(0); // profile_tier_level, general fields and level
    ue(&writer, 0); // sps_seq_parameter_set_id
    ue(&writer, 1); // chroma_format_idc
    ue(&writer, 64); // pic_width
    ue(&writer, 64); // pic_height
    writer.put(0); // conformance_window_flag
    ue(&writer, 0xfffffffe); // bit_depth_luma_minus8
    const payload = try escaped(a, bytes[0 .. (writer.position + 7) / 8]);
    defer a.free(payload);
    var bits = try bits_mod.Bits.init(a, payload);
    defer bits.deinit();
    try std.testing.expectError(error.MalformedVideoPacket, params.parseSps(&bits));
}

test "SPS predicted reference sets read delta_idx only in a slice header" {
    const a = std.testing.allocator;
    // inter_ref_pic_set_prediction_flag=1, delta_rps_sign=0, abs_delta_rps_minus1=ue(0),
    // used_by_curr_pic_flag=1 for the one candidate, then the RBSP stop bit.
    const sets = [_]params.ShortTermRefPicSet{.{}};
    var bits = try bits_mod.Bits.init(a, &.{0b1_0_1_1_1_000});
    defer bits.deinit();
    const inside_sps = try params.parseShortTermRefPicSet(&bits, &sets, 1, 2);
    try std.testing.expectEqual(@as(usize, 1), inside_sps.numDeltaPocs());
    try std.testing.expectEqual(@as(usize, 4), bits.position);
    // The same index in a slice header (index == count) also has delta_idx_minus1=ue(0).
    var slice = try bits_mod.Bits.init(a, &.{0b1_1_0_1_1_1_00});
    defer slice.deinit();
    const in_slice = try params.parseShortTermRefPicSet(&slice, &sets, 1, 1);
    try std.testing.expectEqual(@as(usize, 1), in_slice.numDeltaPocs());
    try std.testing.expectEqual(@as(usize, 5), slice.position);
}

test "A record built from in-band parameter sets parses like an out-of-band one" {
    const a = std.testing.allocator;
    const full = @embedFile("../testdata/hevc-hlg-config.bin");
    var empty: [23]u8 = undefined;
    @memcpy(empty[0..22], full[0..22]);
    empty[22] = 0;
    try std.testing.expect((try fromInband(a, full, "", 4)) == null);
    var sample: std.ArrayList(u8) = .empty;
    defer sample.deinit(a);
    const record = try nal.parseConfigurationRecord(full);
    var arrays = record.arrays();
    while (try arrays.next()) |array| {
        var nals = array.nals;
        while (try nals.next()) |unit| {
            try sample.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u32, @intCast(unit.len))));
            try sample.appendSlice(a, unit);
        }
    }
    try sample.appendSlice(a, &[_]u8{ 0, 0, 0, 3, 19 << 1, 1, 0 });
    const built = (try fromInband(a, &empty, sample.items, 4)).?;
    defer a.free(built);
    try std.testing.expectEqual(@as(?u8, 18), (try parse(a, built)).?.transfer);
    // Only VPS, SPS and PPS are kept; a sample with no SPS yields nothing.
    try std.testing.expect((try fromInband(a, &empty, &.{ 0, 0, 0, 3, 19 << 1, 1, 0 }, 4)) == null);
    try std.testing.expectError(error.MalformedVideoConfig, fromInband(a, &empty, &.{ 0, 0, 0, 9, 32 << 1 }, 4));
}
