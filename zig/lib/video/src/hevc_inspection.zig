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

//! Resolve HEVC activation with bounded header probes, never frame reconstruction.
//! Sparse hardware restarts use the initial configuration. New/redefined sets
//! after sample zero are therefore refused before publishing any selected frame.
const std = @import("std");
const media = @import("antfly_media");
const config = @import("hevc_config.zig");
const nal = @import("hevc_nal.zig");
const Bits = @import("hevc_bits.zig").Bits;
const params = @import("hevc_params.zig");

const Picture = struct {
    width: u32,
    height: u32,
    display_width: u32,
    display_height: u32,
    depth: u8,
    chroma: u2,
    signal: config.Signal,
};
const PictureResult = anyerror!Picture;
const PpsResult = anyerror!u4;
const Selection = struct { sps: u4, picture: Picture };

fn picture(bits: *Bits) !Picture {
    const sps = try params.parseSps(bits);
    const vui = sps.vui;
    return .{
        .width = sps.width,
        .height = sps.height,
        .display_width = sps.displayWidth(),
        .display_height = sps.displayHeight(),
        .depth = sps.bit_depth_luma,
        .chroma = sps.chroma_format_idc,
        .signal = .{
            .primaries = specified(vui.colour_primaries),
            .transfer = specified(vui.transfer_characteristics),
            .matrix = specified(vui.matrix_coefficients),
            .full_range = vui.video_full_range_flag,
            .sar = if (vui.sar_width != 0 and vui.sar_height != 0) .{ .horizontal = vui.sar_width, .vertical = vui.sar_height } else .{},
        },
    };
}
fn specified(value: ?u8) ?u8 {
    return if (value == 2) null else value;
}

const State = struct {
    // No heap map, decoded pictures or borrowed packet data survives a probe.
    hashes: [3][64]?[32]u8 = @splat(@splat(null)),
    sps: [16]?PictureResult = @splat(null),
    pps: [64]?PpsResult = @splat(null),

    fn load(self: *State, a: std.mem.Allocator, bytes: []const u8, control: media.source.Control) !void {
        var arrays = (try nal.parseConfigurationRecord(bytes)).arrays();
        var count: usize = 0;
        while (try arrays.next()) |array| {
            if (array.nal_unit_type < 32 or array.nal_unit_type > 34) continue;
            var nals = array.nals;
            while (try nals.next()) |unit| {
                if (count == 256) return error.ResourceLimitExceeded;
                count += 1;
                var bits = try parameterBits(a, unit, array.nal_unit_type, control);
                defer bits.deinit();
                const id = try parameterId(&bits, array.nal_unit_type);
                self.hashes[array.nal_unit_type - 32][id] = digest(unit);
                if (array.nal_unit_type == 33) {
                    bits.position = 0;
                    // An unused SPS may describe unsupported tools. Save its
                    // failure, but only the active PPS->SPS chain decides policy.
                    self.sps[id] = picture(&bits);
                } else if (array.nal_unit_type == 34) {
                    self.pps[id] = ppsSps(&bits);
                }
            }
        }
    }
    fn select(self: *const State, pps: u6) !Selection {
        const sps = try (self.pps[pps] orelse return error.MissingVideoParameterSet);
        return .{ .sps = sps, .picture = try (self.sps[sps] orelse return error.MissingVideoParameterSet) };
    }
    fn checkParameter(self: *const State, a: std.mem.Allocator, kind: u6, unit: []const u8, control: media.source.Control) !void {
        var bits = try parameterBits(a, unit, kind, control);
        defer bits.deinit();
        const id = try parameterId(&bits, kind);
        const initial = self.hashes[kind - 32][id] orelse return error.UnsupportedDynamicVideoConfig;
        if (!std.mem.eql(u8, &initial, &digest(unit))) return error.UnsupportedDynamicVideoConfig;
    }
};
fn digest(unit: []const u8) [32]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(unit, &hash, .{});
    return hash;
}
fn ppsSps(bits: *Bits) !u4 {
    return @intCast(try bits.ueBounded(15));
}
fn parameterId(bits: *Bits, kind: u6) !u6 {
    return @intCast(switch (kind) {
        32 => try bits.read(4),
        33 => try params.parseSpsId(bits),
        34 => try bits.ueBounded(63),
        else => unreachable,
    });
}
fn parameterBits(a: std.mem.Allocator, unit: []const u8, kind: u6, control: media.source.Control) !Bits {
    if (unit.len > std.math.maxInt(u16)) return error.MalformedVideoConfig;
    const parsed = try nal.parseHeader(unit);
    if (parsed.header.nal_unit_type.raw() != kind or parsed.header.layer_id != 0) return error.UnsupportedVideoProfile;
    return Bits.initControlled(a, parsed.rbsp, control);
}

// Slice activation precedes all geometry-dependent syntax. At most fifteen bits
// identify a bounded PPS; de-escape only the small prefix that can contain them.
fn slicePps(prefix: []const u8, kind: u6) !struct { first: bool, pps: u6 } {
    if (prefix.len < 3) return error.MalformedVideoPacket;
    var bytes: [8]u8 = undefined;
    var count: usize = 0;
    var zeros: usize = 0;
    for (prefix[2..]) |byte| {
        if (count == bytes.len) break;
        if (zeros == 2 and byte == 3) {
            zeros = 0;
            continue;
        }
        bytes[count] = byte;
        count += 1;
        zeros = if (byte == 0) zeros + 1 else 0;
    }
    // This is a prefix, not a complete RBSP: trailing bits belong to the picture.
    var bits = Bits{ .allocator = undefined, .bytes = bytes[0..count], .end = count * 8 };
    const first = try bits.flag();
    if (kind >= 16 and kind <= 23) _ = try bits.flag();
    return .{ .first = first, .pps = @intCast(try bits.ueBounded(63)) };
}

fn range(reader: *media.mp4.Reader, packet: media.mp4.Packet, offset: usize, size: usize, borrowed: ?[]const u8) !media.source.Lease {
    if (borrowed) |bytes| return .{ .bytes = bytes[offset..][0..size], .source = null };
    return reader.input.read(packet.offset + offset, size);
}
fn inspectPacket(a: std.mem.Allocator, reader: *media.mp4.Reader, packet: media.mp4.Packet, state: *const State, expected: ?Picture, borrowed: ?[]const u8) !Selection {
    const length_bytes = reader.track.nal_length_bytes;
    if (length_bytes == 0 or length_bytes > 4) return error.MalformedVideoConfig;
    var cursor: usize = 0;
    var selected: ?Selection = null;
    var selected_pps: ?u6 = null;
    var parameters: usize = 0;
    while (cursor < packet.size) {
        try reader.input.control.check();
        const remaining = packet.size - cursor;
        if (remaining < length_bytes + @as(usize, 2)) return error.MalformedVideoPacket;
        var prefix = try range(reader, packet, cursor, @min(remaining, 32), borrowed);
        defer prefix.deinit();
        var size: usize = 0;
        for (prefix.bytes[0..length_bytes]) |byte| size = (size << 8) | byte;
        cursor += length_bytes;
        if (size < 2 or size > packet.size - cursor) return error.MalformedVideoPacket;
        const unit = prefix.bytes[length_bytes..][0..@min(size, prefix.bytes.len - length_bytes)];
        if (unit[0] & 0x80 != 0 or unit[1] & 7 == 0) return error.MalformedVideoPacket;
        if (unit[0] & 1 != 0 or unit[1] >> 3 != 0) return error.UnsupportedVideoProfile;
        const kind: u6 = @intCast((unit[0] >> 1) & 0x3f);
        if (kind >= 32 and kind <= 34) {
            if (parameters == 256) return error.ResourceLimitExceeded;
            parameters += 1;
            var parameter = try range(reader, packet, cursor, size, borrowed);
            defer parameter.deinit();
            try state.checkParameter(a, kind, parameter.bytes, reader.input.control);
        } else if (kind < 32) {
            const slice = try slicePps(unit, kind);
            if (selected_pps) |pps| {
                if (slice.first or pps != slice.pps) return error.MixedVideoPictures;
            } else {
                if (!slice.first) return error.IncompleteVideoPicture;
                selected_pps = slice.pps;
                selected = try state.select(slice.pps);
                if (expected) |policy| if (!std.meta.eql(policy, selected.?.picture)) return error.UnsupportedDynamicVideoConfig;
            }
        }
        cursor += size;
    }
    return selected orelse error.IncompleteVideoPicture;
}

pub fn resolveReader(a: std.mem.Allocator, reader: *media.mp4.Reader) !?[]u8 {
    if (reader.track.codec != .hevc or reader.track.hevc_policy_validated or reader.packets.len == 0) return null;
    try reader.input.control.check();
    var owned: ?[]u8 = null;
    errdefer if (owned) |bytes| a.free(bytes);
    var state: State = .{};
    const first = blk: {
        var sample = try reader.readPacket(0);
        defer sample.deinit();
        if (reader.track.inband_parameter_sets) owned = try config.fromInbandControlled(a, reader.track.codec_config, sample.bytes, reader.track.nal_length_bytes, reader.input.control);
        const initial = owned orelse reader.track.codec_config;
        try state.load(a, initial, reader.input.control);
        const selected = try inspectPacket(a, reader, reader.packets[0], &state, null, sample.bytes);
        if (try config.prioritizeSps(a, initial, selected.sps, reader.input.control)) |canonical| {
            if (owned) |previous| a.free(previous);
            owned = canonical;
        }
        break :blk selected;
    };
    // All later probes skip VCL payloads. Repeated parameter sets are bounded by
    // identity and hash, and no packet lease or decoded frame survives a probe.
    for (reader.packets[1..]) |packet| _ = try inspectPacket(a, reader, packet, &state, first.picture, null);
    if (owned) |bytes| reader.track.codec_config = bytes;
    reader.track.hevc_policy_validated = true;
    return owned;
}

test "HEVC activation selects the referenced SPS and reuses validated inspection" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/hevc-multi-sps.mp4") }, .identity = "multi-sps" };
    var reader = try media.mp4.Reader.init(a, &source, .{ .codecs = std.EnumSet(media.mp4.Codec).initOne(.hevc) });
    defer reader.deinit();
    // The first unused SPS is 1:1; every picture's PPS selects the second, 2:1 SPS.
    try std.testing.expectEqual(@as(u32, 1), (try config.parse(a, reader.track.codec_config)).?.sar.horizontal);
    const before = source.total_bytes;
    const owned = (try resolveReader(a, &reader)).?;
    defer a.free(owned);
    try std.testing.expectEqual(@as(u32, 2), (try config.parse(a, reader.track.codec_config)).?.sar.horizontal);
    try std.testing.expect(source.total_bytes - before <= reader.packets[0].size + (reader.packets.len - 1) * 64);
    const after = source.total_bytes;
    try std.testing.expect((try resolveReader(a, &reader)) == null);
    try std.testing.expectEqual(after, source.total_bytes);
    try std.testing.expect(reader.track.hevc_policy_validated);
}

fn syntheticPacket(offset: u64, size: u32) media.mp4.Packet {
    return .{ .offset = offset, .size = size, .dts = 0, .media_pts = 0, .pts = 0, .duration = 1, .sync = true };
}

test "HEVC activation permits equivalent PPS switches and refuses incompatible or missing references" {
    const a = std.testing.allocator;
    var metadata = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/hevc-multi-sps.mp4") }, .identity = "sets" };
    var owner = try media.mp4.Reader.init(a, &metadata, .{ .codecs = std.EnumSet(media.mp4.Codec).initOne(.hevc) });
    defer owner.deinit();
    // PPS 0 and 2 select SPS 0 (2:1). PPS 1 selects SPS 1 (1:1). No PPS 3 exists.
    var bytes = [_]u8{ 0, 0, 0, 3, 19 << 1, 1, 0xb0, 0, 0, 0, 3, 19 << 1, 1, 0x9c };
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = &bytes }, .identity = "switches" };
    var packets = [_]media.mp4.Packet{ syntheticPacket(0, 7), syntheticPacket(7, 7) };
    var reader = owner;
    reader.input = &source;
    reader.packets = &packets;
    const owned = try resolveReader(a, &reader);
    defer if (owned) |record| a.free(record);
    try std.testing.expectEqual(@as(u32, 2), (try config.parse(a, reader.track.codec_config)).?.sar.horizontal);
    reader.track = owner.track;
    bytes[13] = 0x94; // PPS 1: same coded size, different SAR.
    try std.testing.expectError(error.UnsupportedDynamicVideoConfig, resolveReader(a, &reader));
    try std.testing.expect(!reader.track.hevc_policy_validated);
    bytes[13] = 0x89; // PPS 3: missing.
    try std.testing.expectError(error.MissingVideoParameterSet, resolveReader(a, &reader));
}

test "HEVC activation rejects later redefinitions before publication" {
    const a = std.testing.allocator;
    var metadata = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/hevc-multi-sps.mp4") }, .identity = "sets" };
    var owner = try media.mp4.Reader.init(a, &metadata, .{ .codecs = std.EnumSet(media.mp4.Codec).initOne(.hevc) });
    defer owner.deinit();
    var arrays = (try nal.parseConfigurationRecord(@embedFile("../testdata/hevc-hlg-config.bin"))).arrays();
    var changed_sps: []const u8 = undefined;
    while (try arrays.next()) |array| if (array.nal_unit_type == 33) {
        var units = array.nals;
        changed_sps = (try units.next()).?;
        break;
    };
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    const idr = [_]u8{ 0, 0, 0, 3, 19 << 1, 1, 0xb0 };
    try bytes.appendSlice(a, &idr);
    try bytes.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(u32, @intCast(changed_sps.len))));
    try bytes.appendSlice(a, changed_sps);
    try bytes.appendSlice(a, &idr);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes.items }, .identity = "redefined-sps" };
    var packets = [_]media.mp4.Packet{ syntheticPacket(0, 7), syntheticPacket(7, @intCast(bytes.items.len - 7)) };
    var reader = owner;
    reader.input = &source;
    reader.packets = &packets;
    try std.testing.expectError(error.UnsupportedDynamicVideoConfig, resolveReader(a, &reader));
    try std.testing.expectEqual(owner.track.codec_config.ptr, reader.track.codec_config.ptr);
    try std.testing.expect(!reader.track.hevc_policy_validated);
    try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
}

test "HEVC activation allocation failures cancellation and read budgets release ownership" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/hevc-multi-sps.mp4") }, .identity = "allocations" };
            var reader = try media.mp4.Reader.init(a, &source, .{ .codecs = std.EnumSet(media.mp4.Codec).initOne(.hevc) });
            defer reader.deinit();
            const owned = try resolveReader(a, &reader);
            defer if (owned) |bytes| a.free(bytes);
        }
    };
    const a = std.testing.allocator;
    try std.testing.checkAllAllocationFailures(a, Check.run, .{});
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/hevc-multi-sps.mp4") }, .identity = "limits" };
    var reader = try media.mp4.Reader.init(a, &source, .{ .codecs = std.EnumSet(media.mp4.Codec).initOne(.hevc) });
    defer reader.deinit();
    const retained = source.retained_bytes;
    source.limits.max_total_bytes = source.total_bytes + reader.packets[0].size;
    try std.testing.expectError(error.ResourceLimitExceeded, resolveReader(a, &reader));
    try std.testing.expectEqual(retained, source.retained_bytes);
    try std.testing.expect(!reader.track.hevc_policy_validated);
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    source.control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, resolveReader(a, &reader));
}
