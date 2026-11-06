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
const media = @import("antfly_media");

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

/// Parameter-set identities have codec-defined bounds (16 VPS, 16 SPS, 64 PPS).
/// Merge into fixed slots, replacing the same ID with the sample's newer value.
/// Bound parsing work too: repeated IDs must not turn a small request into an
/// unbounded number of parameter-set parses or allocations.
const max_parameter_units = 256;
const SetTable = struct {
    units: [3][64]?[]const u8 = @splat(@splat(null)),
    ids: [3][64]u8 = undefined,
    counts: [3]u8 = @splat(0),
    parsed: usize = 0,

    fn put(self: *SetTable, a: std.mem.Allocator, kind: u6, unit: []const u8, control: media.source.Control) !bool {
        try control.check();
        if (self.parsed == max_parameter_units) return error.ResourceLimitExceeded;
        self.parsed += 1;
        if (unit.len > std.math.maxInt(u16)) return error.MalformedVideoConfig;
        const header = try nal.parseHeader(unit);
        if (header.header.nal_unit_type.raw() != kind or header.header.layer_id != 0) return error.UnsupportedVideoProfile;
        var bits = try bits_mod.Bits.initControlled(a, header.rbsp, control);
        defer bits.deinit();
        const id = switch (kind) {
            32 => try bits.read(4),
            33 => try params.parseSpsId(&bits),
            34 => try bits.ueBounded(params.max_pps_count - 1),
            else => unreachable,
        };
        const slot = &self.units[kind - 32][id];
        if (slot.*) |previous| if (std.mem.eql(u8, previous, unit)) return false;
        if (slot.* == null) {
            const group = kind - 32;
            self.ids[group][self.counts[group]] = @intCast(id);
            self.counts[group] += 1;
        }
        slot.* = unit;
        return true;
    }
    fn count(self: *const SetTable, kind: usize) u16 {
        return self.counts[kind];
    }
};

/// Array count alone says nothing about which parameter sets are available.
pub fn hasParameterSets(config: []const u8) bool {
    const record = nal.parseConfigurationRecord(config) catch return false;
    var sps = false;
    var pps = false;
    var arrays = record.arrays();
    while (arrays.next() catch return false) |array| {
        var nals = array.nals;
        if ((nals.next() catch return false) == null) continue;
        sps = sps or array.nal_unit_type == 33;
        pps = pps or array.nal_unit_type == 34;
    }
    return sps and pps;
}

/// Own the normalized configuration alongside any reader copies borrowing it.
/// Only hev1 reads a sample; ordinary hvc1/AVC/AV1 inspection stays metadata-only.
pub fn normalizeReader(a: std.mem.Allocator, reader: *media.mp4.Reader) !?[]u8 {
    if (reader.track.codec != .hevc or !reader.track.inband_parameter_sets or reader.packets.len == 0) return null;
    var first = try reader.readPacket(0);
    defer first.deinit();
    const config = try fromInbandControlled(a, reader.track.codec_config, first.bytes, reader.track.nal_length_bytes, reader.input.control);
    if (config) |bytes| reader.track.codec_config = bytes;
    return config;
}

/// Merge first-sample parameter sets with hvcC, including partially populated or
/// empty arrays. Preserve non-parameter arrays. Null means no sample set changed
/// the record, or the combined record still lacks SPS/PPS. Caller owns the result.
pub fn fromInband(a: std.mem.Allocator, config: []const u8, sample: []const u8, length_bytes: u3) !?[]u8 {
    return fromInbandControlled(a, config, sample, length_bytes, .{});
}

pub fn fromInbandControlled(a: std.mem.Allocator, config: []const u8, sample: []const u8, length_bytes: u3, control: media.source.Control) !?[]u8 {
    try control.check();
    if (length_bytes == 0 or length_bytes > 4) return error.MalformedVideoConfig;
    const record = try nal.parseConfigurationRecord(config);
    if (record.length_size != length_bytes) return error.MalformedVideoConfig;
    var sets: SetTable = .{};
    var retained_arrays: u16 = 0;
    var arrays = record.arrays();
    while (try arrays.next()) |array| {
        if (array.nal_unit_type < 32 or array.nal_unit_type > 34) {
            retained_arrays += 1;
            continue;
        }
        var nals = array.nals;
        while (try nals.next()) |unit| _ = try sets.put(a, array.nal_unit_type, unit, control);
    }
    var changed = false;
    var packets = nal.LengthPrefixedIterator{ .bytes = sample, .length_size = length_bytes };
    while (packets.next() catch return error.MalformedVideoConfig) |unit| {
        try control.check();
        if (unit.len < 2) return error.MalformedVideoConfig;
        const kind: u6 = @intCast((unit[0] >> 1) & 0x3f);
        if (kind < 32) break;
        if (kind <= 34) changed = (try sets.put(a, kind, unit, control)) or changed;
    }
    if (!changed or sets.count(1) == 0 or sets.count(2) == 0) return null;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, config[0..22]);
    var total_arrays = retained_arrays;
    for (0..3) |kind| total_arrays += @intFromBool(sets.count(kind) != 0);
    try out.append(a, std.math.cast(u8, total_arrays) orelse return error.MalformedVideoConfig);
    for (0..3) |kind| {
        const count = sets.count(kind);
        if (count == 0) continue;
        // hev1 permits later updates: don't declare these arrays complete.
        try out.append(a, @intCast(kind + 32));
        try out.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u16, count)));
        // Keep container order stable; replacement updates an existing slot.
        for (sets.ids[kind][0..count]) |id| {
            const unit = sets.units[kind][id].?;
            try out.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u16, @intCast(unit.len))));
            try out.appendSlice(a, unit);
        }
    }
    arrays = record.arrays();
    while (try arrays.next()) |array| {
        if (array.nal_unit_type >= 32 and array.nal_unit_type <= 34) continue;
        try out.append(a, @as(u8, array.nal_unit_type) | (if (array.completeness) @as(u8, 0x80) else 0));
        try out.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u16, array.nals.remaining)));
        try out.appendSlice(a, array.nals.bytes);
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

fn testSample(a: std.mem.Allocator, config: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    var arrays = (try nal.parseConfigurationRecord(config)).arrays();
    while (try arrays.next()) |array| {
        var nals = array.nals;
        while (try nals.next()) |unit| {
            try out.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u32, @intCast(unit.len))));
            try out.appendSlice(a, unit);
        }
    }
    return out.toOwnedSlice(a);
}

test "HEVC normalization merges partial and empty arrays and preserves other arrays" {
    const a = std.testing.allocator;
    const full = @embedFile("../testdata/hevc-hlg-config.bin");
    const sample = try testSample(a, full);
    defer a.free(sample);
    var empty: [32]u8 = undefined;
    @memcpy(empty[0..22], full[0..22]);
    @memcpy(empty[22..], &[_]u8{ 3, 32, 0, 0, 33, 0, 0, 34, 0, 0 });
    try std.testing.expect(!hasParameterSets(&empty));
    const merged = (try fromInband(a, &empty, sample, 4)).?;
    defer a.free(merged);
    try std.testing.expect(hasParameterSets(merged));
    try std.testing.expectEqual(try parse(a, full), try parse(a, merged));
    // Retain VPS in hvcC and put SPS/PPS in the sample. Also retain a non-set
    // array verbatim so normalizing the sets doesn't discard decoder metadata.
    var partial: std.ArrayList(u8) = .empty;
    defer partial.deinit(a);
    try partial.appendSlice(a, full[0..22]);
    try partial.append(a, 2);
    var arrays = (try nal.parseConfigurationRecord(full)).arrays();
    const vps = (try arrays.next()).?;
    try std.testing.expectEqual(@as(u6, 32), vps.nal_unit_type);
    try partial.append(a, 32);
    try partial.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u16, vps.nals.remaining)));
    try partial.appendSlice(a, vps.nals.bytes);
    const extra = [_]u8{ 0x80 | 39, 0, 1, 0, 3, 39 << 1, 1, 0x80 };
    try partial.appendSlice(a, &extra);
    const completed = (try fromInband(a, partial.items, sample, 4)).?;
    defer a.free(completed);
    try std.testing.expect(hasParameterSets(completed));
    try std.testing.expectEqual(try parse(a, full), try parse(a, completed));
    try std.testing.expect(std.mem.endsWith(u8, completed, &extra));
    // Repeated sets don't grow the configuration or cause another allocation
    // for the serialized output when it already describes the first sample.
    try std.testing.expect((try fromInband(a, completed, sample, 4)) == null);
}

test "HEVC normalization bounds repeated parameter sets before serialization" {
    const a = std.testing.allocator;
    const full = @embedFile("../testdata/hevc-hlg-config.bin");
    var empty: [23]u8 = undefined;
    @memcpy(empty[0..22], full[0..22]);
    empty[22] = 0;
    var arrays = (try nal.parseConfigurationRecord(full)).arrays();
    _ = try arrays.next();
    var sps = (try arrays.next()).?.nals;
    const unit = (try sps.next()).?;
    var sample: std.ArrayList(u8) = .empty;
    defer sample.deinit(a);
    // This used to reach an unchecked u16 cast. Reject work well before 65536.
    for (0..65536) |_| {
        try sample.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u32, @intCast(unit.len))));
        try sample.appendSlice(a, unit);
    }
    try std.testing.expectError(error.ResourceLimitExceeded, fromInband(a, &empty, sample.items, 4));
    try std.testing.expectError(error.MalformedVideoConfig, fromInband(a, &empty, sample.items, 0));
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, fromInbandControlled(a, &empty, sample.items, 4, .{ .check_fn = Cancel.check }));
}

test "HEVC normalization releases all allocations on failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const full = @embedFile("../testdata/hevc-hlg-config.bin");
            const sample = try testSample(a, full);
            defer a.free(sample);
            var empty: [23]u8 = undefined;
            @memcpy(empty[0..22], full[0..22]);
            empty[22] = 0;
            const merged = (try fromInband(a, &empty, sample, 4)).?;
            defer a.free(merged);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "HEVC normalization replaces a matching identity rather than duplicating it" {
    const a = std.testing.allocator;
    const old = @embedFile("../testdata/hevc-hlg-config.bin");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/hevc-mp4.mp4") }, .identity = "sdr-config" };
    var reader = try media.mp4.Reader.init(a, &source, .{ .codecs = std.EnumSet(media.mp4.Codec).initOne(.hevc) });
    defer reader.deinit();
    const sample = try testSample(a, reader.track.codec_config);
    defer a.free(sample);
    const merged = (try fromInband(a, old, sample, 4)).?;
    defer a.free(merged);
    try std.testing.expectEqual(try parse(a, reader.track.codec_config), try parse(a, merged));
    var arrays = (try nal.parseConfigurationRecord(merged)).arrays();
    while (try arrays.next()) |array| {
        if (array.nal_unit_type >= 32 and array.nal_unit_type <= 34)
            try std.testing.expectEqual(@as(u16, 1), array.nals.remaining);
    }
}
