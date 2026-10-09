// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Portable static H.264 profile/tool subset: multi-slice I/P/B, CAVLC/CABAC,
//! 8–14-bit 4:2:0/4:2:2/4:4:4, MBAFF and bounded buffered PAFF field assembly.
//! Unsupported coding tools fail closed; no system codec or FFmpeg dependency.
const std = @import("std");
const layout = @import("h264_layout.zig");
const media = @import("antfly_media");
const preparation = @import("preparation.zig");
const Bits = @import("h264_bits.zig").Bits;
const tables = @import("h264_tables.zig");
pub const Options = struct {
    max_pixels: usize = 16 * 1024 * 1024,
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_decode_bytes: usize = 128 * 1024 * 1024,
    max_dependency_packets: usize = 256,
    /// Lazy reconstruction slots for field pairs separated by other pictures.
    max_pending_pictures: usize = 16,
    /// Per reconstructed picture, including both standalone PAFF field packets.
    max_slices: usize = 256,
};
const Config = struct {
    profile: u32,
    bit_depth: u8,
    bypass_allowed: bool,
    chroma_format: u8,
    groups: @import("h264_groups.zig").Groups,
    redundant: bool,
    scaling: @import("h264_scaling.zig").Matrices,
    chroma_offset1: i32,
    constrained: bool,
    frame_only: bool,
    mbaff: bool,
    max_refs: usize,
    active0: usize,
    active1: usize,
    weighted_p: bool,
    weighted_b: u2,
    direct8: bool,
    transform8: bool,
    cabac: bool,
    id: u32,
    pps: u32,
    frame_bits: usize,
    poc_bits: ?usize,
    poc_type: u32,
    poc_zero: bool,
    poc_nonref: i32,
    poc_bottom: i32,
    poc_cycle: usize,
    poc_offsets: [255]i32,

    coded_width: usize,
    coded_height: usize,
    width: usize,
    height: usize,
    left: usize,
    top: usize,
    full_range: bool,
    bottom_poc: bool,
    qp: i32,
    chroma_offset: i32,
    deblock_present: bool,
};
pub const Frame = struct {
    allocator: std.mem.Allocator,
    reservation: media.admission.Token = .{},
    /// Semiplanar Y then interleaved Cb/Cr, in the declared chroma format.
    /// Depths above 8 use right-aligned little-endian u16 samples.
    nv12: []u8,
    bit_depth: u8 = 8,
    chroma_format: u8 = 1,
    width: u32,
    height: u32,
    pts: i64,
    duration: u32,
    timescale: u32,
    full_range: bool,
    decode_high_water: usize,
    decoded_packets: usize = 1,
    field_macroblocks: usize = 0,
    interlaced: bool = false,
    payload_bytes: u64 = 0,
    pub fn host(self: *const Frame) preparation.HostSurface {
        const bytes: usize = if (self.bit_depth == 8) 1 else 2;
        const size = @as(usize, self.width) * self.height * bytes;
        const sub_x: u32 = if (self.chroma_format == 3) 1 else 2;
        return .{ .chroma_format = self.chroma_format, .bit_depth = self.bit_depth, .width = self.width, .height = self.height, .format = if (self.full_range) .nv12_full else .nv12_video, .planes = .{ .{ .bytes = self.nv12[0..size], .width = self.width, .height = self.height, .stride = self.width * bytes }, .{ .bytes = self.nv12[size..], .width = self.width / sub_x, .height = self.height / (if (self.chroma_format == 1) @as(u32, 2) else 1), .stride = self.width * 2 / sub_x * bytes } } };
    }
    pub fn deinit(self: *Frame) void {
        self.allocator.free(self.nv12);
        self.reservation.deinit();
        self.* = undefined;
    }
};
fn nalSet(config: []const u8, cursor: *usize) ![]const u8 {
    if (cursor.* > config.len or config.len - cursor.* < 2) return error.MalformedVideoConfig;
    const size = std.mem.readInt(u16, config[cursor.*..][0..2], .big);
    cursor.* += 2;
    if (size == 0 or size > config.len - cursor.*) return error.MalformedVideoConfig;
    const nal = config[cursor.*..][0..size];
    cursor.* += size;
    return nal;
}
fn configParse(allocator: std.mem.Allocator, config: []const u8) !Config {
    try @import("avc.zig").validatePortableConfig(config);
    if (config[5] & 31 != 1) return error.UnsupportedVideoProfile;
    var cursor: usize = 6;
    const sps = try nalSet(config, &cursor);
    var bits = try Bits.init(allocator, sps);
    defer bits.deinit();
    const profile = try bits.read(8);
    if (profile != config[1]) return error.MalformedVideoConfig;
    const constraints = try bits.read(8);
    if (constraints & 3 != 0) return error.MalformedVideoConfig;
    _ = try bits.read(8);
    const id = try bits.ue();
    if (id > 31) return error.MalformedVideoConfig;
    var scaling = @import("h264_scaling.zig").Matrices{};
    var bit_depth: u8 = 8;
    var bypass_allowed = false;
    var chroma_format: u8 = 1;
    if (profile == 100 or profile == 110 or profile == 122 or profile == 244) {
        const chroma = try bits.ue();
        if (chroma < 1 or chroma > 3 or (profile < 122 and chroma != 1) or (profile == 122 and chroma > 2)) return error.UnsupportedVideoProfile;
        chroma_format = @intCast(chroma);
        if (chroma == 3 and try bits.read(1) != 0) return error.UnsupportedVideoProfile;
        const depth_y = try bits.ue();
        const depth_c = try bits.ue();
        if (depth_y != depth_c or depth_y > 6 or (profile == 100 and depth_y != 0) or ((profile == 110 or profile == 122) and depth_y > 2)) return error.UnsupportedVideoProfile;
        bit_depth = @intCast(depth_y + 8);
        bypass_allowed = try bits.read(1) != 0;
        if (bypass_allowed and profile != 244) return error.MalformedVideoConfig;
        try scaling.parseSequence(&bits, chroma_format);
    }
    const frame_bits = try bits.ue() + 4;
    if (frame_bits > 16) return error.UnsupportedVideoProfile;
    const poc = try bits.ue();
    var poc_bits: ?usize = null;
    var poc_zero = false;
    var poc_nonref: i32 = 0;
    var poc_bottom: i32 = 0;
    var poc_cycle: usize = 0;
    var poc_offsets: [255]i32 = @splat(0);
    if (poc == 0) {
        poc_bits = try bits.ue() + 4;
        if (poc_bits.? > 16) return error.UnsupportedVideoProfile;
    } else if (poc == 1) {
        poc_zero = try bits.read(1) != 0;
        poc_nonref = try bits.se();
        poc_bottom = try bits.se();
        poc_cycle = try bits.ue();
        if (poc_cycle > poc_offsets.len) return error.MalformedVideoConfig;
        for (poc_offsets[0..poc_cycle]) |*offset| offset.* = try bits.se();
    } else if (poc != 2) return error.MalformedVideoConfig;
    const max_refs = try bits.ue();
    if (max_refs > 16 or try bits.read(1) != 0) return error.UnsupportedVideoProfile;
    const mbs_width = try bits.ue() + 1;
    const mbs_height = try bits.ue() + 1;
    if (mbs_width > 1024 or mbs_height > 1024) return error.UnsupportedVideoProfile;
    const frame_only = try bits.read(1) != 0;
    const mbaff = !frame_only and try bits.read(1) != 0;
    if (!frame_only and profile == 66) return error.MalformedVideoConfig;
    const direct8 = try bits.read(1) != 0;
    var left: usize = 0;
    var right: usize = 0;
    var top: usize = 0;
    var bottom: usize = 0;
    if (try bits.read(1) != 0) {
        left = try bits.ue();
        right = try bits.ue();
        top = try bits.ue();
        bottom = try bits.ue();
    }
    const coded_width: usize = mbs_width * 16;
    const coded_height: usize = mbs_height * 16 * (if (frame_only) @as(usize, 1) else 2);
    const sub_y: usize = (if (chroma_format == 1) @as(usize, 2) else 1) * (if (frame_only) @as(usize, 1) else 2);
    const sub_x: usize = if (chroma_format == 3) 1 else 2;
    if (left + right >= coded_width / sub_x or (top + bottom) * sub_y >= coded_height) return error.MalformedVideoConfig;
    var full_range = false;
    if (try bits.read(1) != 0) full_range = try vui(&bits);
    try bits.finish();
    if (cursor >= config.len or config[cursor] != 1) return error.UnsupportedVideoProfile;
    cursor += 1;
    const pps = try nalSet(config, &cursor);
    if (cursor != config.len and profile < 100) return error.UnsupportedVideoProfile;
    var p = try Bits.init(allocator, pps);
    defer p.deinit();
    const pps_id = try p.ue();
    if (pps_id > 255 or try p.ue() != id) return error.MalformedVideoConfig;
    const cabac = try p.read(1) != 0;
    if (cabac and profile == 66) return error.MalformedVideoConfig;
    const bottom_poc = try p.read(1) != 0;
    const groups = try @import("h264_groups.zig").Groups.parse(&p, allocator, mbs_width, mbs_height);
    errdefer groups.deinit(allocator);
    if (groups.count != 1 and profile != 66) return error.MalformedVideoConfig;
    const active0 = try p.ue() + 1;
    const active1 = try p.ue() + 1;
    if (active0 > 16 or active1 > 16) return error.UnsupportedVideoProfile;
    const weighted_p = try p.read(1) != 0;
    const weighted_b: u2 = @intCast(try p.read(2));
    if (weighted_b == 3 or (profile == 66 and (weighted_p or weighted_b != 0))) return error.MalformedVideoConfig;
    const qp = try p.se() + 26;
    if (qp < -6 * @as(i32, bit_depth - 8) or qp > 51) return error.MalformedVideoConfig;
    _ = try p.se();
    const chroma_offset = try p.se();
    if (chroma_offset < -12 or chroma_offset > 12) return error.MalformedVideoConfig;
    const deblock_present = try p.read(1) != 0;
    const constrained = try p.read(1) != 0;
    const redundant = try p.read(1) != 0;
    var transform8 = false;
    var chroma_offset1 = chroma_offset;
    if (p.position != p.end) {
        transform8 = try p.read(1) != 0;
        if (transform8 and profile < 100) return error.MalformedVideoConfig;
        try scaling.parsePicture(&p, transform8, chroma_format);
        chroma_offset1 = try p.se();
        if (chroma_offset1 < -12 or chroma_offset1 > 12) return error.MalformedVideoConfig;
    }
    try p.finish();
    return .{ .bypass_allowed = bypass_allowed, .frame_only = frame_only, .mbaff = mbaff, .chroma_format = chroma_format, .bit_depth = bit_depth, .groups = groups, .redundant = redundant, .constrained = constrained, .scaling = scaling, .chroma_offset1 = chroma_offset1, .profile = profile, .max_refs = max_refs, .active0 = active0, .active1 = active1, .weighted_p = weighted_p, .weighted_b = weighted_b, .direct8 = direct8, .transform8 = transform8, .cabac = cabac, .id = id, .pps = pps_id, .frame_bits = frame_bits, .poc_type = poc, .poc_zero = poc_zero, .poc_nonref = poc_nonref, .poc_bottom = poc_bottom, .poc_cycle = poc_cycle, .poc_offsets = poc_offsets, .poc_bits = poc_bits, .coded_width = coded_width, .coded_height = coded_height, .width = coded_width - sub_x * (left + right), .height = coded_height - sub_y * (top + bottom), .left = sub_x * left, .top = sub_y * top, .full_range = full_range, .bottom_poc = bottom_poc, .qp = qp, .chroma_offset = chroma_offset, .deblock_present = deblock_present };
}
fn hrd(bits: *Bits) !void {
    const count = try bits.ue() + 1;
    if (count > 32) return error.UnsupportedVideoProfile;
    _ = try bits.read(8);
    for (0..count) |_| {
        _ = try bits.ue();
        _ = try bits.ue();
        _ = try bits.read(1);
    }
    _ = try bits.read(20);
}
fn vui(bits: *Bits) !bool {
    if (try bits.read(1) != 0) {
        if (try bits.read(8) == 255) _ = try bits.read(32);
    }
    if (try bits.read(1) != 0) _ = try bits.read(1);
    var full = false;
    if (try bits.read(1) != 0) {
        _ = try bits.read(3);
        full = try bits.read(1) != 0;
        if (try bits.read(1) != 0) _ = try bits.read(24);
    }
    if (try bits.read(1) != 0) {
        _ = try bits.ue();
        _ = try bits.ue();
    }
    if (try bits.read(1) != 0) {
        _ = try bits.read(32);
        _ = try bits.read(32);
        _ = try bits.read(1);
    }
    const nal_hrd = try bits.read(1) != 0;
    if (nal_hrd) try hrd(bits);
    const vcl_hrd = try bits.read(1) != 0;
    if (vcl_hrd) try hrd(bits);
    if (nal_hrd or vcl_hrd) _ = try bits.read(1);
    _ = try bits.read(1);
    if (try bits.read(1) != 0) {
        _ = try bits.read(1);
        for (0..4) |_| _ = try bits.ue();
        if (try bits.ue() > 16 or try bits.ue() > 16) return error.UnsupportedVideoProfile;
    }
    return full;
}
const zigzag = [16]usize{ 0, 1, 4, 8, 5, 2, 3, 6, 9, 12, 13, 10, 7, 11, 14, 15 };
pub const Residual = struct { values: [16]i32 = @splat(0), total: u8 = 0 };
fn vlc(bits: *Bits, words: []const [2]u8) !usize {
    var code: u32 = 0;
    for (1..17) |length| {
        code = (code << 1) | try bits.read(1);
        for (words, 0..) |word, i| if (word[1] == length and word[0] == code) return i;
    }
    return error.MalformedVideoPacket;
}
pub fn cavlcResidual(bits: *Bits, nc: usize, max: usize, dc_chroma: bool) !Residual {
    const table: usize = if (dc_chroma) 4 else if (nc < 2) @as(usize, 0) else if (nc < 4) 1 else if (nc < 8) 2 else 3;
    var code: u32 = 0;
    var total: usize = 0;
    var trailing: usize = 0;
    var found = false;
    outer: for (1..17) |length| {
        code = (code << 1) | try bits.read(1);
        for (if (dc_chroma and max == 8) @import("h264_chroma422.zig").token else tables.coeff_token[table], 0..) |row, coeff| for (row, 0..) |word, ones| {
            if (word[1] == length and word[0] == code) {
                total = coeff;
                trailing = ones;
                found = true;
                break :outer;
            }
        };
    }
    if (!found or total > max or trailing > total) return error.MalformedVideoPacket;
    var result = Residual{ .total = @intCast(total) };
    if (total == 0) return result;
    var levels: [16]i32 = undefined;
    for (0..trailing) |i| levels[i] = if (try bits.read(1) == 0) 1 else -1;
    var suffix: usize = if (total > 10 and trailing < 3) 1 else 0;
    for (trailing..total) |i| {
        var prefix: usize = 0;
        while (try bits.read(1) == 0) {
            prefix += 1;
            if (prefix > 28) return error.MalformedVideoPacket;
        }
        const suffix_size = if (prefix == 14 and suffix == 0) @as(usize, 4) else if (prefix >= 15) prefix - 3 else suffix;
        var level_code: i64 = @as(i64, @intCast(@min(prefix, 15))) << @as(u6, @intCast(suffix));
        level_code += try bits.read(suffix_size);
        if (prefix >= 15 and suffix == 0) level_code += 15;
        if (prefix >= 16) level_code += (@as(i64, 1) << @as(u6, @intCast(prefix - 3))) - 4096;
        if (i == trailing and trailing < 3) level_code += 2;
        const level: i64 = if (level_code & 1 == 0) @divTrunc(level_code + 2, 2) else -@divTrunc(level_code + 1, 2);
        if (@abs(level) > (1 << 21)) return error.MalformedVideoPacket;
        levels[i] = @intCast(level);
        if (suffix == 0) suffix = 1;
        if (@abs(level) > (@as(u32, 3) << @as(u5, @intCast(suffix - 1))) and suffix < 6) suffix += 1;
    }
    var zeros: usize = if (total == max) 0 else try vlc(bits, if (dc_chroma and max == 8) &@import("h264_chroma422.zig").zeros[total] else if (dc_chroma) &tables.chroma_zeros[total] else &tables.total_zeros[total]);
    if (zeros > max - total) return error.MalformedVideoPacket;
    var position = total + zeros;
    for (0..total) |i| {
        if (position == 0) return error.MalformedVideoPacket;
        position -= 1;
        result.values[position] = levels[i];
        const run = if (i + 1 == total or zeros == 0) @as(usize, 0) else try vlc(bits, &tables.run_before[@min(zeros, 7)]);
        if (run > zeros or run > position) return error.MalformedVideoPacket;
        zeros -= run;
        position -= run;
    }
    if (position != zeros) return error.MalformedVideoPacket;
    return result;
}

fn clipSample(comptime Sample: type, value: i64, bit_depth: u8) Sample {
    return @intCast(std.math.clamp(value, 0, (@as(i64, 1) << @as(u6, @intCast(bit_depth))) - 1));
}
fn predict(plane: anytype, stride: usize, x: usize, y: usize, size: usize, mode: u32, chroma: bool, chroma_height: usize, bit_depth: u8, neighbors: @import("h264_intra.zig").Availability) !void {
    var top: [16]i64 = undefined;
    var left: [16]i64 = undefined;
    const has_top = neighbors.top;
    const has_left = neighbors.left;
    const height = if (chroma) chroma_height else size;
    for (0..size) |i| {
        top[i] = if (has_top) layout.get(plane, (y - 1) * stride + x + i) else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
    }
    for (0..height) |i| left[i] = if (has_left) layout.get(plane, (y + i) * stride + x - 1) else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
    // Luma modes: vertical, horizontal, DC, plane. Chroma: DC, horizontal,
    // vertical, plane. Neighbors exist only after preceding raster macroblocks.
    const resolved = if (chroma) ([_]u32{ 2, 1, 0, 3 })[@min(mode, 3)] else mode;
    if (mode > 3 or (resolved == 0 and !has_top) or (resolved == 1 and !has_left) or (resolved == 3 and (!has_top or !has_left or !neighbors.corner))) return error.MalformedVideoPacket;
    var a: i64 = 0;
    var b: i64 = 0;
    var c: i64 = 0;
    if (resolved == 3) {
        const corner: i64 = layout.get(plane, (y - 1) * stride + x - 1);
        const half = size / 2;
        var h: i64 = 0;
        var v: i64 = 0;
        for (1..half + 1) |i| {
            h += @as(i64, @intCast(i)) * (top[half - 1 + i] - (if (i == half) corner else top[half - 1 - i]));
            v += @as(i64, @intCast(i)) * (left[half - 1 + i] - (if (i == half) corner else left[half - 1 - i]));
        }
        a = 16 * (top[size - 1] + left[height - 1]);
        b = if (chroma) (17 * h + 16) >> 5 else (5 * h + 32) >> 6;
        if (height != size) {
            v = 0;
            for (1..height / 2 + 1) |i| v += @as(i64, @intCast(i)) * (left[height / 2 - 1 + i] - (if (i == height / 2) corner else left[height / 2 - 1 - i]));
        }
        c = if (height == 8) (17 * v + 16) >> 5 else (5 * v + 32) >> 6;
    }
    var dc: [8]i64 = @splat(@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
    if (resolved == 2) {
        if (chroma) {
            var t: [2]i64 = @splat(0);
            var l: [4]i64 = @splat(0);
            for (0..8) |i| {
                t[i / 4] += top[i];
            }
            for (0..height) |i| l[i / 4] += left[i];
            dc[0] = if (has_top and has_left) (t[0] + l[0] + 4) >> 3 else if (has_top) (t[0] + 2) >> 2 else if (has_left) (l[0] + 2) >> 2 else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
            dc[1] = if (has_top) (t[1] + 2) >> 2 else if (has_left) (l[0] + 2) >> 2 else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
            dc[2] = if (has_left) (l[1] + 2) >> 2 else if (has_top) (t[0] + 2) >> 2 else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
            dc[3] = if (has_top and has_left) (t[1] + l[1] + 4) >> 3 else if (has_top) (t[1] + 2) >> 2 else if (has_left) (l[1] + 2) >> 2 else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
            for (2..height / 4) |row| {
                dc[row * 2] = if (has_left) (l[row] + 2) >> 2 else if (has_top) (t[0] + 2) >> 2 else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
                dc[row * 2 + 1] = if (has_left and has_top) (l[row] + t[1] + 4) >> 3 else if (has_left) (l[row] + 2) >> 2 else if (has_top) (t[1] + 2) >> 2 else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
            }
        } else {
            var sum: i64 = 0;
            for (0..16) |i| {
                if (has_top) sum += top[i];
                if (has_left) sum += left[i];
            }
            dc[0] = if (has_top and has_left) (sum + 16) >> 5 else if (has_top or has_left) (sum + 8) >> 4 else (@as(i64, 1) << @as(u6, @intCast(bit_depth - 1)));
        }
    }
    for (0..height) |row| for (0..size) |column| {
        const value = switch (resolved) {
            0 => top[column],
            1 => left[row],
            2 => dc[if (chroma) (row / 4) * 2 + column / 4 else 0],
            3 => (a + b * (@as(i64, @intCast(column)) - @as(i64, @intCast(size / 2 - 1))) + c * (@as(i64, @intCast(row)) - @as(i64, @intCast(height / 2 - 1))) + 16) >> 5,
            else => unreachable,
        };
        layout.put(plane, (y + row) * stride + x + column, clipSample(layout.Sample(@TypeOf(plane)), value, bit_depth));
    };
}
const factors = [6][3]i64{ .{ 10, 13, 16 }, .{ 11, 14, 18 }, .{ 13, 16, 20 }, .{ 14, 18, 23 }, .{ 16, 20, 25 }, .{ 18, 23, 29 } };
fn dequant(value: i32, qp: usize, index: usize, weight: u8) i64 {
    const category: usize = (index % 4 & 1) + (index / 4 & 1);
    const scaled = @as(i64, value) * factors[qp % 6][category] * weight;
    return if (qp >= 24) scaled << @as(u6, @intCast(qp / 6 - 4)) else (scaled + (@as(i64, 1) << @as(u6, @intCast(3 - qp / 6)))) >> @as(u6, @intCast(4 - qp / 6));
}
fn hadamard4(input: [16]i64) [16]i64 {
    var temp: [16]i64 = undefined;
    var result: [16]i64 = undefined;
    for (0..4) |row| {
        const p = input[row * 4 ..][0..4];
        const a = p[0] + p[3];
        const b = p[1] + p[2];
        const c = p[1] - p[2];
        const d = p[0] - p[3];
        temp[row * 4 ..][0..4].* = .{ a + b, d + c, a - b, d - c };
    }
    for (0..4) |column| {
        const a = temp[column] + temp[12 + column];
        const b = temp[4 + column] + temp[8 + column];
        const c = temp[4 + column] - temp[8 + column];
        const d = temp[column] - temp[12 + column];
        result[column] = a + b;
        result[4 + column] = d + c;
        result[8 + column] = a - b;
        result[12 + column] = d - c;
    }
    return result;
}
fn bypassAdd(plane: anytype, stride: usize, x: usize, y: usize, width: usize, height: usize, residual: []i64, mode: ?u32, bit_depth: u8) void {
    if (mode) |direction| {
        if (direction == 0) for (1..height) |row| for (0..width) |col| {
            residual[row * width + col] += residual[(row - 1) * width + col];
        };
        if (direction == 1) for (0..height) |row| for (1..width) |col| {
            residual[row * width + col] += residual[row * width + col - 1];
        };
    }
    for (0..height) |row| for (0..width) |col| {
        const index = (y + row) * stride + x + col;
        layout.put(plane, index, clipSample(layout.Sample(@TypeOf(plane)), @as(i64, layout.get(plane, index)) + residual[row * width + col], bit_depth));
    };
}
fn inverseAdd(plane: anytype, stride: usize, x: usize, y: usize, coefficients: [16]i64, bit_depth: u8) void {
    var temp: [16]i64 = undefined;
    var result: [16]i64 = undefined;
    for (0..4) |row| {
        const p = coefficients[row * 4 ..][0..4];
        const a = p[0] + p[2];
        const b = p[0] - p[2];
        const c = (p[1] >> 1) - p[3];
        const d = p[1] + (p[3] >> 1);
        temp[row * 4 ..][0..4].* = .{ a + d, b + c, b - c, a - d };
    }
    for (0..4) |column| {
        const a = temp[column] + temp[8 + column];
        const b = temp[column] - temp[8 + column];
        const c = (temp[4 + column] >> 1) - temp[12 + column];
        const d = temp[4 + column] + (temp[12 + column] >> 1);
        result[column] = a + d;
        result[4 + column] = b + c;
        result[8 + column] = b - c;
        result[12 + column] = a - d;
    }
    for (0..4) |row| for (0..4) |column| {
        const offset = (y + row) * stride + x + column;
        layout.put(plane, offset, clipSample(layout.Sample(@TypeOf(plane)), @as(i64, layout.get(plane, offset)) + ((result[row * 4 + column] + 32) >> 6), bit_depth));
    };
}
fn blockPoint(index: usize) [2]usize {
    return .{ (index / 4 % 2) * 2 + index % 2, (index / 8) * 2 + index / 2 % 2 };
}
fn context(counts: []u8, stride: usize, x: usize, y: usize) usize {
    if (x == 0 and y == 0) return 0;
    if (x == 0) return counts[(y - 1) * stride + x];
    if (y == 0) return counts[y * stride + x - 1];
    return (@as(usize, counts[y * stride + x - 1]) + counts[(y - 1) * stride + x] + 1) / 2;
}
fn chromaQpDepth(qp: i32, offset: i32, depth_offset: i32) usize {
    const q = std.math.clamp(qp - depth_offset + offset, -depth_offset, 51);
    return @intCast((if (q < 0) q else @as(i32, @intCast(chromaQp(q, 0)))) + depth_offset);
}
fn chromaQp(qp: i32, offset: i32) usize {
    const index: usize = @intCast(std.math.clamp(qp + offset, 0, 51));
    return if (index < 30) index else ([_]usize{ 29, 30, 31, 32, 32, 33, 34, 34, 35, 35, 36, 36, 37, 37, 37, 38, 38, 38, 39, 39, 39, 39 })[index - 30];
}
fn reorder(bits: *Bits, state: anytype, frame_bits: usize, active: usize) !void {
    const maximum: u32 = @as(u32, 1) << @as(u5, @intCast(frame_bits));
    var predicted = state.current_num;
    var insertion: usize = 0;
    while (true) {
        const operation = try bits.ue();
        if (operation == 3) return;
        if (operation > 2 or insertion >= active or insertion >= state.list_count) return error.UnsupportedVideoProfile;
        const code = try bits.ue();
        if (operation != 2) {
            const distance = code + 1;
            if (distance > maximum) return error.MalformedVideoPacket;
            predicted = if (operation == 0) (predicted + maximum - distance) % maximum else (predicted + distance) % maximum;
        }
        var found: ?usize = null;
        for (state.pictures[0..state.count], 0..) |pic, i| if (if (operation == 2) pic.long_term == code else pic.long_term == null and pic.frame_num == predicted) {
            found = i;
            break;
        };
        const target = found orelse return error.MissingVideoReference;
        var updated: [16]usize = undefined;
        var count: usize = 0;
        for (state.list0[0..insertion]) |i| {
            updated[count] = i;
            count += 1;
        }
        updated[count] = target;
        count += 1;
        for (state.list0[insertion..state.list_count]) |i| if (i != target) {
            if (count < state.list_count) {
                updated[count] = i;
                count += 1;
            }
        };
        @memset(updated[count..], 16);
        state.list0 = updated;
        insertion += 1;
    }
}
const PictureHeader = struct {
    frame_num: u32,
    frame_offset: i32,
    poc: i32,
    idr: bool,
    reference: bool,
    idr_id: u32,
    group_cycle: usize,
    field_pic: bool,
    bottom: bool,
    adaptive: bool,
    current_long: ?u32,
    commands: [32]@import("h264_references.zig").State.Command,
    command_count: usize,
};
fn predictionNeighbors(syntax: *@import("h264_entropy.zig").Syntax, plane: usize, x: usize, y: usize, right: bool) @import("h264_intra.zig").Availability {
    const bx: i32 = @intCast(x / 4);
    const by: i32 = @intCast(y / 4);
    return .{ .top = syntax.available(plane, bx, by - 1, true), .left = syntax.available(plane, bx - 1, by, true), .corner = syntax.available(plane, bx - 1, by - 1, true), .top_right = right };
}
fn decodeSlice(allocator: std.mem.Allocator, nal: []const u8, cfg: Config, physical_planes: anytype, counts: [3][]u8, modes: []u8, qps: []u8, metadata: []@import("h264_entropy.zig").Meta, motions: []@import("h264_motion.zig").Motion, motions1: []@import("h264_motion.zig").Motion, references: anytype, headers: *[2]?PictureHeader, group_map: []u8, control: media.source.Control) !void {
    var bits = try Bits.initSlice(allocator, nal, control, cfg.cabac);
    defer bits.deinit();
    const first_mb = try bits.ue();
    const encoded_type = try bits.ue();
    if (encoded_type > 9) return error.MalformedVideoPacket;
    const slice_type = encoded_type % 5;
    if (slice_type != 2 and slice_type != 0 and slice_type != 1) return error.UnsupportedVideoProfile;
    if (cfg.profile == 66 and slice_type == 1) return error.MalformedVideoPacket;
    if (try bits.ue() != cfg.pps) return error.MalformedVideoPacket;
    const frame_num = try bits.read(cfg.frame_bits);
    const field_pic = !cfg.frame_only and try bits.read(1) != 0;
    const bottom = field_pic and try bits.read(1) != 0;
    const parity: usize = @intFromBool(bottom);
    const header = &headers[parity];
    const first_slice = header.* == null;
    const first_picture = headers[0] == null and headers[1] == null;
    if (headers[1 - parity]) |other| if (other.frame_num != frame_num or !other.field_pic or !field_pic) return error.MixedVideoPictures;
    const mbaff = cfg.mbaff and !field_pic;
    const first_address = first_mb * (if (mbaff) @as(usize, 2) else 1);
    const picture_mbs = metadata.len / (if (field_pic) @as(usize, 2) else 1);
    if (first_address >= picture_mbs) return error.MalformedVideoPacket;
    const idr = nal[0] & 31 == 5;
    const reference = nal[0] & 0x60 != 0;
    if (header.*) |previous| if (previous.frame_num != frame_num or previous.idr != idr or previous.reference != reference or previous.field_pic != field_pic or previous.bottom != bottom) {
        return error.MixedVideoPictures;
    };
    references.reference = reference;
    var idr_id: u32 = 0;
    if (idr) {
        idr_id = try bits.ue();
        if (header.*) |previous| if (previous.idr_id != idr_id) {
            return error.MixedVideoPictures;
        };
    }
    // A complementary reference pair's second field is not an IDR picture.
    if (headers[1 - parity]) |other| if (idr or other.reference != reference) return error.MixedVideoPictures;
    if (idr and first_picture) {
        if (frame_num != 0 or slice_type != 2) return error.MalformedVideoPacket;
        references.deinit(allocator);
        references.previous_lsb = 0;
        references.previous_msb = 0;
        references.previous_num = 0;
        references.frame_offset = 0;
    }
    references.frame_bits = cfg.frame_bits;
    const frame_offset = if (header.*) |previous| previous.frame_offset else if (headers[1 - parity]) |other| other.frame_offset else blk: {
        if (references.previous_num > frame_num) references.frame_offset = std.math.add(i32, references.frame_offset, @as(i32, 1) << @as(u5, @intCast(cfg.frame_bits))) catch return error.TimestampOverflow;
        references.previous_num = frame_num;
        break :blk references.frame_offset;
    };
    references.current_num = frame_num;
    if (cfg.poc_bits) |poc_bits| {
        const lsb: i32 = @intCast(try bits.read(poc_bits));
        const maximum: i32 = @as(i32, 1) << @as(u5, @intCast(poc_bits));
        var msb = references.previous_msb;
        if (lsb < references.previous_lsb and references.previous_lsb - lsb >= (maximum >> 1)) msb += maximum;
        if (lsb > references.previous_lsb and lsb - references.previous_lsb > (maximum >> 1)) msb -= maximum;
        references.current_poc = msb + lsb;
        if (references.reference) {
            references.previous_lsb = lsb;
            references.previous_msb = msb;
        }
        if (field_pic) {
            references.current_field_poc[@intFromBool(bottom)] = references.current_poc;
        } else {
            const bottom_order = std.math.add(i32, msb + lsb, if (cfg.bottom_poc) try bits.se() else 0) catch return error.TimestampOverflow;
            references.current_field_poc = .{ msb + lsb, bottom_order };
            references.current_poc = @min(msb + lsb, bottom_order);
        }
    } else if (cfg.poc_type == 1) {
        var absolute: i64 = if (cfg.poc_cycle == 0) 0 else @as(i64, frame_offset) + frame_num;
        if (!reference and absolute > 0) absolute -= 1;
        var expected: i64 = 0;
        if (absolute > 0) {
            var cycle_sum: i64 = 0;
            for (cfg.poc_offsets[0..cfg.poc_cycle]) |offset| cycle_sum += offset;
            expected = @divTrunc(absolute - 1, @as(i64, @intCast(cfg.poc_cycle))) * cycle_sum;
            for (cfg.poc_offsets[0 .. @as(usize, @intCast(@mod(absolute - 1, @as(i64, @intCast(cfg.poc_cycle))))) + 1]) |offset| expected += offset;
        }
        if (!reference) expected += cfg.poc_nonref;
        const delta0 = if (cfg.poc_zero) 0 else try bits.se();
        const delta1 = if (!cfg.poc_zero and cfg.bottom_poc and !field_pic) try bits.se() else 0;
        const top = expected + delta0;
        const bottom_poc = top + cfg.poc_bottom + delta1;
        references.current_poc = std.math.cast(i32, if (field_pic) (if (bottom) expected + cfg.poc_bottom + delta0 else top) else @min(top, bottom_poc)) orelse return error.TimestampOverflow;
        if (field_pic) references.current_field_poc[@intFromBool(bottom)] = references.current_poc else references.current_field_poc = .{ std.math.cast(i32, top) orelse return error.TimestampOverflow, std.math.cast(i32, bottom_poc) orelse return error.TimestampOverflow };
    } else {
        references.current_poc = (frame_offset + @as(i32, @intCast(frame_num))) * 2 - @as(i32, @intFromBool(!references.reference));
        if (field_pic) references.current_field_poc[@intFromBool(bottom)] = references.current_poc else references.current_field_poc = @splat(references.current_poc);
    }
    if (header.*) |previous| if (previous.poc != references.current_poc) {
        return error.MixedVideoPictures;
    };

    const redundant = if (cfg.redundant) try bits.ue() else 0;
    if (redundant > 127) return error.MalformedVideoPacket;
    if (redundant != 0) {
        if (first_slice) return error.MissingPrimaryVideoSlice;
        return;
    }
    references.field_picture = field_pic;
    references.order(cfg.frame_bits);
    if (slice_type == 1) references.orderB();
    if (field_pic) references.orderFields(parity, slice_type == 1);
    const spatial_direct = slice_type == 1 and try bits.read(1) != 0;
    var active0 = cfg.active0;
    var active1 = cfg.active1;
    if (slice_type != 2) {
        if (try bits.read(1) != 0) {
            active0 = try bits.ue() + 1;
            if (slice_type == 1) active1 = try bits.ue() + 1;
        }
        if (active0 > (if (field_pic) @as(usize, 32) else 16) or active1 > (if (field_pic) @as(usize, 32) else 16)) return error.UnsupportedVideoProfile;
        references.list_count = @max(references.list_count, @max(active0, if (slice_type == 1) active1 else 0) / (if (field_pic) @as(usize, 2) else 1));
        if (try bits.read(1) != 0) {
            if (field_pic) try references.reorderFields(&bits, 0, active0) else try reorder(&bits, references, cfg.frame_bits, active0);
        }
        if (slice_type == 1 and try bits.read(1) != 0) {
            if (field_pic) try references.reorderFields(&bits, 1, active1) else {
                std.mem.swap([16]usize, &references.list0, &references.list1);
                try reorder(&bits, references, cfg.frame_bits, active1);
                std.mem.swap([16]usize, &references.list0, &references.list1);
            }
        }
    }
    references.weight_mode = .none;
    if ((slice_type == 0 and cfg.weighted_p) or (slice_type == 1 and cfg.weighted_b == 1)) {
        references.weights = try @import("h264_weights.zig").parse(&bits, .{ active0, active1 }, slice_type == 1);
        references.weight_mode = .explicit;
    } else if (slice_type == 1 and cfg.weighted_b == 2) references.weight_mode = .implicit;
    try references.marking(&bits, idr);
    if (header.*) |previous| {
        if (previous.adaptive != references.adaptive or previous.current_long != references.current_long or previous.command_count != references.command_count) return error.MixedVideoPictures;
        for (previous.commands[0..previous.command_count], references.commands[0..references.command_count]) |before, after| if (!std.meta.eql(before, after)) return error.MixedVideoPictures;
    }
    var init_idc: usize = 0;
    if (cfg.cabac and slice_type != 2) {
        init_idc = try bits.ue();
        if (init_idc > 2) return error.MalformedVideoPacket;
    }
    const depth_offset: i32 = 6 * @as(i32, cfg.bit_depth - 8);
    var qp = cfg.qp + try bits.se() + depth_offset;
    if (qp < 0 or qp > 51 + depth_offset) return error.MalformedVideoPacket;
    const filter = if (cfg.deblock_present) try bits.ue() else 0;
    if (filter > 2) return error.MalformedVideoPacket;
    var alpha_offset: i32 = 0;
    var beta_offset: i32 = 0;
    if (filter != 1) {
        if (cfg.deblock_present) {
            alpha_offset = try bits.se();
            beta_offset = try bits.se();
            if (alpha_offset < -6 or alpha_offset > 6 or beta_offset < -6 or beta_offset > 6) return error.MalformedVideoPacket;
            alpha_offset *= 2;
            beta_offset *= 2;
        }
    }
    const group_cycle = try cfg.groups.cycle(&bits, metadata.len);
    if (header.*) |previous| if (previous.group_cycle != group_cycle) {
        return error.MixedVideoPictures;
    };
    if (first_slice and group_map.len != 0) cfg.groups.build(group_map, cfg.coded_width / 16, group_cycle);
    header.* = .{ .frame_num = frame_num, .frame_offset = frame_offset, .poc = references.current_poc, .idr = idr, .reference = reference, .idr_id = idr_id, .group_cycle = group_cycle, .field_pic = field_pic, .bottom = bottom, .adaptive = references.adaptive, .current_long = references.current_long, .commands = references.commands, .command_count = references.command_count };
    var syntax = try @import("h264_entropy.zig").Syntax.init(&bits, cfg.cabac, std.math.clamp(qp - depth_offset, 0, 51), slice_type, init_idc, metadata, counts, cfg.coded_width);
    syntax.slice_id = first_mb;
    syntax.constrained = cfg.constrained;
    syntax.chroma_format = cfg.chroma_format;
    syntax.bit_depth = cfg.bit_depth;
    syntax.paired = mbaff or field_pic;
    syntax.mbaff = mbaff;
    const sub_y: usize = if (cfg.chroma_format == 1) 2 else 1;
    const sub_x: usize = if (cfg.chroma_format == 3) 1 else 2;
    for (motions, motions1) |*m0, *m1| {
        m0.decoded = false;
        m1.decoded = false;
    }
    const mb_width = cfg.coded_width / 16;
    const Sample = std.meta.Child(@TypeOf(physical_planes[0]));
    if (field_pic) for (metadata) |*m| {
        m.field = true;
    };
    var cached_bottom_skip: ?bool = null;
    var ended = false;
    var skip_run: usize = 0;
    var after_skip = false;
    var address: usize = first_address;
    while (address < picture_mbs) : (address = @import("h264_groups.zig").next(group_map, address, picture_mbs)) {
        const mb = if (mbaff) layout.raster(address, mb_width) else if (field_pic) layout.raster(address * 2 + parity, mb_width) else address;
        if (metadata[mb].kind != 255) return error.OverlappingVideoSlices;
        metadata[mb].slice_id = first_mb;
        metadata[mb].filter = @intCast(filter);
        metadata[mb].alpha = @intCast(alpha_offset);
        metadata[mb].beta = @intCast(beta_offset);
        try control.check();
        const x = mb % mb_width * 16;
        var field = field_pic;
        if (mbaff) {
            const pair_top = mb - address % 2 * mb_width;
            field = if (address % 2 != 0) metadata[pair_top].field else if (pair_top % mb_width != 0 and metadata[pair_top - 1].slice_id == first_mb) metadata[pair_top - 1].field else if (pair_top >= 2 * mb_width and metadata[pair_top - 2 * mb_width].slice_id == first_mb) metadata[pair_top - 2 * mb_width].field else false;
            metadata[pair_top].field = field;
            metadata[pair_top + mb_width].field = field;
        }
        const current_parity = if (field_pic) parity else if (mbaff) address % 2 else 0;
        syntax.mb = mb;
        syntax.address = address;
        syntax.parity = current_parity;
        syntax.field = field;
        syntax.x = x / 4;
        syntax.y = if (field) mb / mb_width / 2 * 4 else mb / mb_width * 4;
        var skipped = false;
        if (slice_type != 2) {
            if (cfg.cabac) {
                if (cached_bottom_skip) |skip| {
                    skipped = skip;
                    cached_bottom_skip = null;
                } else skipped = try syntax.skip();
            } else {
                if (skip_run == 0 and !after_skip) {
                    skip_run = try bits.ue();
                    if (skip_run > picture_mbs - address) return error.MalformedVideoPacket;
                }
                if (skip_run != 0) {
                    skipped = true;
                    skip_run -= 1;
                    after_skip = true;
                } else after_skip = false;
            }
        }
        if (mbaff) {
            const top_mb = mb - current_parity * mb_width;
            if (address % 2 == 0) {
                if (skipped and cfg.cabac) {
                    metadata[mb].kind = 27;
                    syntax.mb = mb + mb_width;
                    syntax.parity = 1;
                    syntax.y += if (field) @as(usize, 0) else 4;
                    cached_bottom_skip = try syntax.skip();
                    syntax.mb = mb;
                    syntax.parity = 0;
                    syntax.y -= if (field) @as(usize, 0) else 4;
                }
                const both_skipped = skipped and (if (cfg.cabac) cached_bottom_skip.? else skip_run != 0);
                field = if (!both_skipped) try syntax.fieldFlag() else if (mb % mb_width != 0 and metadata[mb - 1].slice_id == syntax.slice_id) metadata[mb - 1].field else if (mb >= 2 * mb_width and metadata[mb - 2 * mb_width].slice_id == syntax.slice_id) metadata[mb - 2 * mb_width].field else false;
                metadata[top_mb].field = field;
                metadata[top_mb + mb_width].field = field;
            } else field = metadata[top_mb].field;
        }
        syntax.field = field;
        const y = if (field) mb / mb_width / 2 * 16 else mb / mb_width * 16;
        syntax.y = y / 4;
        var planes: [3]layout.View(Sample) = undefined;
        for (0..3) |plane| planes[plane] = .{ .data = physical_planes[plane], .width = cfg.coded_width / (if (plane == 0) @as(usize, 1) else sub_x), .height = cfg.coded_height / (if (plane == 0) @as(usize, 1) else sub_y) / (if (field) @as(usize, 2) else 1), .field = field, .parity = current_parity };
        references.field_mode = field;
        references.field_parity = current_parity;
        const scan4 = if (field) [_]usize{ 0, 4, 1, 8, 12, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15 } else zigzag;
        const encoded_kind: u32 = if (skipped) 0 else try syntax.kind();
        const inter = (slice_type == 0 and encoded_kind < 5) or (slice_type == 1 and encoded_kind < 23);
        const kind = if (inter) @as(u32, 26) else encoded_kind - @as(u32, if (slice_type == 0) 5 else if (slice_type == 1) 23 else 0);
        if ((!inter and kind > 25) or (skipped and !inter)) return error.UnsupportedVideoProfile;
        metadata[mb].kind = @intCast(if (skipped) 27 else kind);
        if (!inter) {
            for (0..4) |row| for (0..4) |column| {
                motions[syntax.cellIndex(0, x / 4 + column, y / 4 + row)] = .{ .decoded = true };
                motions1[syntax.cellIndex(0, x / 4 + column, y / 4 + row)] = .{ .decoded = true };
            };
        }
        const allow8 = if (inter) try @import("h264_inter.zig").predict(&syntax, references, planes, .{ motions, motions1 }, cfg.coded_width, cfg.coded_height, encoded_kind, .{ active0 * (if (field and mbaff) @as(usize, 2) else 1), active1 * (if (field and mbaff) @as(usize, 2) else 1) }, skipped, spatial_direct, cfg.direct8, cfg.bit_depth, cfg.chroma_format) else true;
        if (skipped) {
            qps[mb] = @intCast(qp);
            for (0..4) |row| for (0..4) |col| {
                modes[syntax.cellIndex(0, x / 4 + col, y / 4 + row)] = 2;
            };
            syntax.previous_delta = 0;
            if (try syntax.endMb(skip_run)) {
                ended = true;
                break;
            }
            continue;
        }
        if (kind == 25) {
            qps[mb] = @intCast(depth_offset);
            // CABAC flush completes the arithmetic byte before PCM samples;
            // its unused low bits can be nonzero (including x264's flush bit).
            // CAVLC carries explicit pcm_alignment_zero_bit syntax instead.
            while (bits.position % 8 != 0) {
                const padding = try bits.read(1);
                if (syntax.cabac == null and padding != 0) return error.MalformedVideoPacket;
            }
            for (0..3) |plane| {
                const size: usize = if (plane == 0) 16 else 16 / sub_x;
                const stride = if (plane == 0) cfg.coded_width else cfg.coded_width / sub_x;
                const px = if (plane == 0) x else x / sub_x;
                const py = if (plane == 0) y else y / sub_y;
                const height = if (plane == 0) 16 else 16 / sub_y;
                for (0..height) |row| for (0..size) |column| {
                    layout.put(planes[plane], (py + row) * stride + px + column, @intCast(try bits.read(cfg.bit_depth)));
                };
                for (0..height / 4) |row| for (0..size / 4) |col| {
                    counts[plane][syntax.cellIndex(plane, px / 4 + col, py / 4 + row)] = 16;
                };
            }
            for (0..4) |row| for (0..4) |col| {
                modes[syntax.cellIndex(0, x / 4 + col, y / 4 + row)] = 2;
            };
            metadata[mb].dc = 7;
            metadata[mb].cbp = 47;
            syntax.previous_delta = 0;
            if (syntax.cabac) |*c| try c.restart();
            if (try syntax.endMb(skip_run)) {
                ended = true;
                break;
            }
            continue;
        }
        if (!inter and kind > 24) return error.UnsupportedVideoProfile;
        const value = if (inter) @as(u32, 0) else kind -| 1;
        const mode = value % 4;
        var cbp_chroma = if (cfg.chroma_format == 3) @as(u32, 0) else value / 4 % 3;
        var cbp_luma: u32 = if (value / 12 != 0) 15 else 0;
        var use8 = kind == 0 and cfg.transform8 and try syntax.transform8();
        metadata[mb].transform8 = use8;
        if (kind == 0) {
            for (0..if (use8) @as(usize, 4) else 16) |block| {
                const i = if (use8) block * 4 else block;
                const point = blockPoint(i);
                const bx = x / 4 + point[0];
                const by = y / 4 + point[1];
                const left = if (!syntax.available(0, @as(i32, @intCast(bx)) - 1, @intCast(by), true)) @as(u8, 255) else modes[syntax.location(0, @as(i32, @intCast(bx)) - 1, @intCast(by)).?.index];
                const top = if (!syntax.available(0, @intCast(bx), @as(i32, @intCast(by)) - 1, true)) @as(u8, 255) else modes[syntax.location(0, @intCast(bx), @as(i32, @intCast(by)) - 1).?.index];
                const predicted: u8 = if (left == 255 or top == 255) 2 else @min(left, top);
                const resolved = try syntax.mode(predicted);
                if (use8) {
                    for (0..2) |row| for (0..2) |col| {
                        modes[syntax.cellIndex(0, bx + col, by + row)] = resolved;
                    };
                } else modes[syntax.cellIndex(0, bx, by)] = resolved;
            }
        } else for (0..4) |row| for (0..4) |col| {
            modes[syntax.cellIndex(0, x / 4 + col, y / 4 + row)] = 2;
        };
        const chroma_mode = if (inter or cfg.chroma_format == 3) @as(u32, 0) else try syntax.chroma();
        if (chroma_mode > 3) return error.MalformedVideoPacket;
        metadata[mb].chroma = @intCast(chroma_mode);
        if (kind == 0 or inter) {
            const mapped = try syntax.cbp(!inter);
            cbp_luma = mapped & 15;
            cbp_chroma = mapped >> 4;
        }
        metadata[mb].cbp = @intCast(cbp_luma | cbp_chroma << 4);
        if (inter and cfg.transform8 and cbp_luma != 0 and allow8) {
            use8 = try syntax.transform8();
            metadata[mb].transform8 = use8;
        }
        if ((!inter and kind != 0) or cbp_luma != 0 or cbp_chroma != 0) {
            const delta = try syntax.delta();
            if (delta < -(26 + @divTrunc(depth_offset, 2)) or delta > 25 + @divTrunc(depth_offset, 2)) return error.MalformedVideoPacket;
            qp = @mod(qp + delta + 52 + depth_offset, 52 + depth_offset);
        } else syntax.previous_delta = 0;
        qps[mb] = @intCast(qp);
        const bypass = cfg.bypass_allowed and qp == 0;
        const cqs = [2]usize{ chromaQpDepth(qp, cfg.chroma_offset, depth_offset), chromaQpDepth(qp, cfg.chroma_offset1, depth_offset) };
        if (cfg.chroma_format != 3 and !inter) {
            try predict(planes[1], cfg.coded_width / 2, x / 2, y / sub_y, 8, chroma_mode, true, 16 / sub_y, cfg.bit_depth, predictionNeighbors(&syntax, 1, x / 2, y / sub_y, false));
            try predict(planes[2], cfg.coded_width / 2, x / 2, y / sub_y, 8, chroma_mode, true, 16 / sub_y, cfg.bit_depth, predictionNeighbors(&syntax, 2, x / 2, y / sub_y, false));
        }
        for (0..if (cfg.chroma_format == 3) @as(usize, 3) else 1) |plane| {
            const q: usize = if (plane == 0) @intCast(qp) else cqs[plane - 1];
            const cat: usize = if (plane == 0) 0 else if (plane == 1) 6 else 10;
            if (!inter and kind != 0) try predict(planes[plane], cfg.coded_width, x, y, 16, mode, false, 16, cfg.bit_depth, predictionNeighbors(&syntax, plane, x, y, false));
            const luma_dc = if (!inter and kind != 0) try syntax.coeff(syntax.coefficientContext(plane, x / 4, y / 4), 16, cat, plane, x / 4, y / 4) else Residual{ .total = 0 };
            var dc_input: [16]i64 = @splat(0);
            for (scan4, luma_dc.values) |position, coefficient| dc_input[position] = coefficient;
            var dc = if (bypass) dc_input else hadamard4(dc_input);
            if (!bypass) for (&dc) |*coefficient| {
                const scaled = coefficient.* * factors[q % 6][0] * cfg.scaling.four[plane][0];
                coefficient.* = if (q >= 36) scaled << @as(u6, @intCast(q / 6 - 6)) else (scaled + (@as(i64, 1) << @as(u6, @intCast(5 - q / 6)))) >> @as(u6, @intCast(6 - q / 6));
            };
            var bypass_mb: [256]i64 = undefined;
            if (bypass and kind != 0 and !inter) @memset(&bypass_mb, 0);
            if (use8) {
                for (0..4) |i| {
                    const bx = x / 4 + (i % 2) * 2;
                    const by = y / 4 + (i / 2) * 2;
                    const right = bx + 2;
                    const above = by -| 1;
                    const right_mb = if (syntax.location(plane, @intCast(right), @intCast(above))) |neighbor_cell| neighbor_cell.mb else metadata.len;
                    const right_block = right % 4 / 2 + above % 4 / 2 * 2;
                    const available = by != 0 and right < cfg.coded_width / 4 and syntax.available(plane, @intCast(right), @intCast(above), true) and (right_mb != mb or right_block < i);
                    if (!inter) try @import("h264_intra.zig").predict8(planes[plane], cfg.coded_width, bx * 4, by * 4, modes[syntax.cellIndex(0, bx, by)], predictionNeighbors(&syntax, plane, bx * 4, by * 4, available), cfg.bit_depth);
                    if (cbp_luma & (@as(u32, 1) << @as(u5, @intCast(i))) != 0) {
                        const coefficients = try syntax.coeff8(plane, bx, by);
                        if (bypass) {
                            var raw: [64]i64 = undefined;
                            for (coefficients, 0..) |raw_coefficient, index| raw[index] = raw_coefficient;
                            bypassAdd(planes[plane], cfg.coded_width, bx * 4, by * 4, 8, 8, &raw, if (inter) null else modes[syntax.cellIndex(0, bx, by)], cfg.bit_depth);
                        } else @import("h264_transform8.zig").add(planes[plane], cfg.coded_width, bx * 4, by * 4, coefficients, q, cfg.scaling.eight[2 * plane + @intFromBool(inter)], cfg.bit_depth);
                    }
                }
            } else for (0..16) |i| {
                const point = blockPoint(i);
                const bx = x / 4 + point[0];
                const by = y / 4 + point[1];
                var coefficients: [16]i64 = @splat(0);
                coefficients[0] = dc[point[1] * 4 + point[0]];
                if (kind == 0) {
                    const right_bx = bx + 1;
                    const above_by = by -| 1;
                    const right_mb = if (syntax.location(plane, @intCast(right_bx), @intCast(above_by))) |neighbor_cell| neighbor_cell.mb else metadata.len;
                    const right_scan = (right_bx % 4 / 2 + above_by % 4 / 2 * 2) * 4 + right_bx % 2 + above_by % 2 * 2;
                    const available = by != 0 and right_bx < cfg.coded_width / 4 and syntax.available(plane, @intCast(right_bx), @intCast(above_by), true) and (right_mb != mb or right_scan < i);
                    try @import("h264_intra.zig").predict(planes[plane], cfg.coded_width, bx * 4, by * 4, modes[syntax.cellIndex(0, bx, by)], predictionNeighbors(&syntax, plane, bx * 4, by * 4, available), cfg.bit_depth);
                }
                if (cbp_luma & (@as(u32, 1) << @as(u5, @intCast(i / 4))) != 0) {
                    const ac = try syntax.coeff(syntax.coefficientContext(plane, bx, by), if (kind == 0 or inter) 16 else 15, cat + (if (kind == 0 or inter) @as(usize, 2) else 1), plane, bx, by);
                    counts[plane][syntax.cellIndex(plane, bx, by)] = ac.total;
                    for (0..if (kind == 0 or inter) @as(usize, 16) else 15) |j| {
                        const position = scan4[j + @as(usize, if (kind == 0 or inter) 0 else 1)];
                        coefficients[position] = if (bypass) ac.values[j] else dequant(ac.values[j], q, position, cfg.scaling.four[plane + (if (inter) @as(usize, 3) else 0)][position]);
                    }
                }
                if (bypass) {
                    if (kind != 0 and !inter) {
                        for (0..4) |row| for (0..4) |col| {
                            bypass_mb[(point[1] * 4 + row) * 16 + point[0] * 4 + col] = coefficients[row * 4 + col];
                        };
                    } else bypassAdd(planes[plane], cfg.coded_width, bx * 4, by * 4, 4, 4, &coefficients, if (inter) null else modes[syntax.cellIndex(0, bx, by)], cfg.bit_depth);
                } else inverseAdd(planes[plane], cfg.coded_width, bx * 4, by * 4, coefficients, cfg.bit_depth);
            }
            if (bypass and kind != 0 and !inter) bypassAdd(planes[plane], cfg.coded_width, x, y, 16, 16, &bypass_mb, mode, cfg.bit_depth);
        }
        if (cfg.chroma_format != 3) {
            var chroma_dc: [2][8]i64 = @splat(@splat(0));
            if (cbp_chroma != 0) for (0..2) |p| {
                const cq = cqs[p];
                const n: usize = 8 / sub_y;
                const r = try syntax.coeff(0, n, 3, p + 1, x / 8, y / (sub_y * 4));
                var d: [8]i64 = @splat(0);
                // DC 2x4 uses the ordinary zigzag restricted to a 2x4 matrix.
                const scan = [_]usize{ 0, 2, 1, 4, 6, 3, 5, 7 };
                for (0..n) |i| d[if (n == 4) i else scan[i]] = r.values[i];
                if (bypass) {
                    chroma_dc[p] = d;
                    continue;
                }
                const matrix = [4][4]i64{ .{ 1, 1, 1, 1 }, .{ 1, 1, -1, -1 }, .{ 1, -1, -1, 1 }, .{ 1, -1, 1, -1 } };
                for (0..n / 2) |row| for (0..2) |col| {
                    var f: i64 = 0;
                    for (0..n / 2) |k| f += (if (n == 4) (if (row == 0 or k == 0) @as(i64, 1) else -1) else matrix[row][k]) * (d[k * 2] + (if (col == 0) d[k * 2 + 1] else -d[k * 2 + 1]));
                    const qdc = cq + (if (n == 8) @as(usize, 3) else 0);
                    const scaled = f * factors[qdc % 6][0] * cfg.scaling.four[if (inter) p + 4 else p + 1][0];
                    chroma_dc[p][row * 2 + col] = if (n == 4) (scaled * (@as(i64, 1) << @as(u6, @intCast(qdc / 6)))) >> 5 else if (qdc >= 36) scaled << @as(u6, @intCast(qdc / 6 - 6)) else (scaled + (@as(i64, 1) << @as(u6, @intCast(5 - qdc / 6)))) >> @as(u6, @intCast(6 - qdc / 6));
                };
            };
            for (1..3) |p| {
                var raw_chroma: [128]i64 = undefined;
                if (bypass) @memset(&raw_chroma, 0);
                for (0..8 / sub_y) |i| {
                    const cq = cqs[p - 1];
                    const bx = x / 8 + i % 2;
                    const by = y / (sub_y * 4) + i / 2;
                    const stride = cfg.coded_width / 2;
                    var coefficients: [16]i64 = @splat(0);
                    coefficients[0] = chroma_dc[p - 1][i];
                    if (cbp_chroma == 2) {
                        const ac = try syntax.coeff(syntax.coefficientContext(p, bx, by), 15, 4, p, bx, by);
                        counts[p][syntax.cellIndex(p, bx, by)] = ac.total;
                        for (0..15) |j| coefficients[scan4[j + 1]] = if (bypass) ac.values[j] else dequant(ac.values[j], cq, scan4[j + 1], cfg.scaling.four[if (inter) p + 3 else p][scan4[j + 1]]);
                    }
                    if (bypass) {
                        for (0..4) |row| for (0..4) |col| {
                            raw_chroma[(i / 2 * 4 + row) * 8 + i % 2 * 4 + col] = coefficients[row * 4 + col];
                        };
                    } else inverseAdd(planes[p], stride, bx * 4, by * 4, coefficients, cfg.bit_depth);
                }
                if (bypass) bypassAdd(planes[p], cfg.coded_width / 2, x / 2, y / sub_y, 8, 16 / sub_y, raw_chroma[0 .. 128 / sub_y], if (inter) null else if (chroma_mode == 1) @as(u32, 1) else if (chroma_mode == 2) @as(u32, 0) else null, cfg.bit_depth);
            }
        }
        if (try syntax.endMb(skip_run)) {
            ended = true;
            break;
        }
    }
    if (!ended) return error.IncompleteVideoSlice;
    try syntax.finish();
}
/// Decode from a verified IDR through the requested picture, including a following
/// PAFF complement when the selected packet contains the first field. Output is owned;
/// reference state is bounded and local to the call. Calls are portable.
pub fn decodeFrame(allocator: std.mem.Allocator, reader: *media.mp4.Reader, index: usize, options: Options) !Frame {
    try reader.input.control.check();
    if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
    if (index >= reader.packets.len) return error.InvalidPacketIndex;
    const packet = reader.packets[index];
    if (packet.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
    var budget = @import("decode_budget.zig").Budget{ .backing = allocator, .limit = options.max_decode_bytes };
    return decodeBudget(&budget, reader, &.{index}, options, null, null) catch |err| {
        return if (err == error.OutOfMemory and budget.denied) error.ResourceLimitExceeded else err;
    };
}
pub const Statistics = struct { decoded_packets: usize, payload_bytes: u64, decode_high_water: usize };
pub const SelectionCallback = *const fn (*anyopaque, usize, *const Frame) anyerror!void;
/// Frames are borrowed only for the callback. Dependency pictures are decoded
/// once in packet order; slots retain the caller's selection/presentation order.
pub fn decodeSelected(allocator: std.mem.Allocator, reader: *media.mp4.Reader, indexes: []const usize, options: Options, callback_context: *anyopaque, callback: SelectionCallback) !Statistics {
    try reader.input.control.check();
    if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
    if (indexes.len == 0 or indexes.len > options.max_dependency_packets) return error.ResourceLimitExceeded;
    for (indexes) |index| if (index >= reader.packets.len) {
        return error.InvalidPacketIndex;
    };
    var budget = @import("decode_budget.zig").Budget{ .backing = allocator, .limit = options.max_decode_bytes };
    var frame = decodeBudget(&budget, reader, indexes, options, callback_context, callback) catch |err| {
        return if (err == error.OutOfMemory and budget.denied) error.ResourceLimitExceeded else err;
    };
    defer frame.deinit();
    return .{ .decoded_packets = frame.decoded_packets, .payload_bytes = frame.payload_bytes, .decode_high_water = frame.decode_high_water };
}
fn decodeBudget(budget: *@import("decode_budget.zig").Budget, reader: *media.mp4.Reader, indexes: []const usize, options: Options, callback_context: ?*anyopaque, callback: ?SelectionCallback) !Frame {
    var index: usize = 0;
    var first_index: usize = reader.packets.len;
    for (indexes) |selected| {
        index = @max(index, selected);
        first_index = @min(first_index, selected);
    }
    const allocator = budget.allocator();
    var config_reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = reader.track.avcc.len * 10 }) else media.admission.Token{};
    defer config_reservation.deinit();
    const cfg = try configParse(allocator, reader.track.avcc);
    defer cfg.groups.deinit(allocator);
    try config_reservation.resize(.{ .host_bytes = if (cfg.groups.explicit) |map| map.len else 0 });
    return if (cfg.bit_depth == 8) decodeConfigured(u8, cfg, &config_reservation, budget, reader, indexes, options, callback_context, callback, index, first_index) else decodeConfigured(u16, cfg, &config_reservation, budget, reader, indexes, options, callback_context, callback, index, first_index);
}
/// PAFF reset packets may contain an IDR first field and a non-IDR
/// complementary second field. Hardware seeking keeps the stricter avc.isIdr.
fn isDependencyReset(allocator: std.mem.Allocator, cfg: Config, bytes: []const u8, length_bytes: u3, input: *media.source.Source) !bool {
    if (try @import("avc.zig").isIdr(bytes, length_bytes)) {
        if (cfg.frame_only) return true;
        var reservation = if (input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = bytes.len }) else media.admission.Token{};
        defer reservation.deinit();
        var cursor: usize = 0;
        while (cursor < bytes.len) {
            var size: usize = 0;
            for (bytes[cursor..][0..length_bytes]) |byte| size = (size << 8) | byte;
            cursor += length_bytes;
            const nal = bytes[cursor..][0..size];
            cursor += size;
            if (nal[0] & 31 != 5) continue;
            // A sync flag on a continuation fragment is not a seek point.
            var bits = try Bits.initSlice(allocator, nal, input.control, cfg.cabac);
            defer bits.deinit();
            return try bits.ue() == 0;
        }
        return false;
    }
    if (cfg.frame_only) return false;
    var reservation = if (input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = bytes.len }) else media.admission.Token{};
    defer reservation.deinit();
    var cursor: usize = 0;
    var first_parity: ?u32 = null;
    var second = false;
    while (cursor < bytes.len) {
        var size: usize = 0;
        for (bytes[cursor..][0..length_bytes]) |byte| size = (size << 8) | byte;
        cursor += length_bytes;
        const nal = bytes[cursor..][0..size];
        cursor += size;
        const kind = nal[0] & 31;
        if (kind != 1 and kind != 5) continue;
        var bits = try Bits.initSlice(allocator, nal, input.control, cfg.cabac);
        defer bits.deinit();
        _ = try bits.ue();
        const slice_type = try bits.ue();
        if (try bits.ue() != cfg.pps or try bits.read(cfg.frame_bits) != 0 or try bits.read(1) != 1) return false;
        const parity = try bits.read(1);
        if (first_parity == null) {
            if (kind != 5 or slice_type % 5 != 2 or nal[0] & 0x60 == 0) return false;
            first_parity = parity;
        }
        if (parity == first_parity.?) {
            if (kind != 5 or second) return false;
        } else {
            if (kind != 1) return false;
            second = true;
        }
    }
    return first_parity != null and second;
}
/// Extend only while an assembled picture is incomplete; every traversed packet
/// remains covered by dependency, payload, allocator and shared admission limits.
fn extendAssembly(reader: *media.mp4.Reader, start: usize, current: usize, end: *usize, max_packet: *usize, peak: *usize, transient: *media.admission.Token, output_size: usize, config_bytes: u64, options: Options) !void {
    if (current + 1 != end.*) return;
    if (end.* >= reader.packets.len) return error.IncompleteVideoPicture;
    if (end.* - start >= options.max_dependency_packets) return error.ResourceLimitExceeded;
    const additional = reader.packets[end.*].size;
    if (additional > options.max_packet_bytes) return error.ResourceLimitExceeded;
    if (additional > max_packet.*) {
        const growth = try std.math.mul(usize, additional - max_packet.*, 2);
        peak.* = try std.math.add(usize, peak.*, growth);
        if (peak.* > options.max_decode_bytes) return error.ResourceLimitExceeded;
        try transient.resize(.{ .host_bytes = peak.* - output_size - config_bytes });
        max_packet.* = additional;
    }
    end.* += 1;
}
const Prefix = struct { frame_num: u32, field: bool, parity: usize, reference: bool, idr: bool, idr_id: u32 };
fn slicePrefix(allocator: std.mem.Allocator, cfg: Config, nal: []const u8, control: media.source.Control) !Prefix {
    var bits = try Bits.initSlice(allocator, nal, control, cfg.cabac);
    defer bits.deinit();
    _ = try bits.ue();
    _ = try bits.ue();
    if (try bits.ue() != cfg.pps) return error.MalformedVideoPacket;
    const frame_num = try bits.read(cfg.frame_bits);
    const field = !cfg.frame_only and try bits.read(1) != 0;
    const parity: usize = if (field) @intCast(try bits.read(1)) else 0;
    const idr = nal[0] & 31 == 5;
    return .{ .frame_num = frame_num, .field = field, .parity = parity, .reference = nal[0] & 0x60 != 0, .idr = idr, .idr_id = if (idr) try bits.ue() else 0 };
}
fn restoreMarking(references: anytype, workspace: anytype, header: PictureHeader, parity: usize, mbaff: bool) void {
    references.current_pair = workspace.id;
    references.reference = header.reference;
    references.field_picture = header.field_pic;
    references.field_parity = parity;
    references.current_num = header.frame_num;
    references.current_poc = header.poc;
    if (header.field_pic) references.current_field_poc[parity] = header.poc;
    references.current_fields = if (header.field_pic) .{ parity == 0, parity == 1 } else .{ true, true };
    references.current_meta = workspace.metadata;
    references.paired = mbaff or header.field_pic;
    references.adaptive = header.adaptive;
    references.current_long = header.current_long;
    references.commands = header.commands;
    references.command_count = header.command_count;
    // Motion identities, rather than list positions, retain prediction identity.
    references.list_count = 0;
}
fn decodeConfigured(comptime Sample: type, cfg: Config, config_reservation: *media.admission.Token, budget: *@import("decode_budget.zig").Budget, reader: *media.mp4.Reader, indexes: []const usize, options: Options, callback_context: ?*anyopaque, callback: ?SelectionCallback, index: usize, first_index: usize) !Frame {
    const allocator = budget.allocator();
    var output_packet = reader.packets[index];
    if (cfg.width != reader.track.width or cfg.height != reader.track.height) return error.UnsupportedDynamicGeometry;
    const coded_pixels = try std.math.mul(usize, cfg.coded_width, cfg.coded_height);
    if (coded_pixels > options.max_pixels) return error.ResourceLimitExceeded;
    var start = first_index;
    var dependency_count: usize = 0;
    while (true) {
        if (dependency_count >= options.max_dependency_packets) return error.ResourceLimitExceeded;
        dependency_count += 1;
        if (reader.packets[start].size > options.max_packet_bytes) return error.ResourceLimitExceeded;
        if (reader.packets[start].sync) {
            var probe = try reader.readPacket(start);
            defer probe.deinit();
            if (try isDependencyReset(allocator, cfg, probe.bytes, reader.track.nal_length_bytes, reader.input)) break;
        }
        if (start == 0) return error.MissingVideoReference;
        start -= 1;
    }
    if (index - start + 1 > options.max_dependency_packets) return error.ResourceLimitExceeded;
    var max_packet: usize = 0;
    for (reader.packets[start .. index + 1]) |dependency| {
        if (dependency.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
        max_packet = @max(max_packet, dependency.size);
    }
    const sub_y: usize = if (cfg.chroma_format == 1) 2 else 1;
    const sub_x: usize = if (cfg.chroma_format == 3) 1 else 2;
    const output_size = (cfg.width * cfg.height + 2 * (cfg.width / sub_x) * (cfg.height / sub_y)) * @sizeOf(Sample);
    const planar_size = (coded_pixels + 2 * coded_pixels / (sub_x * sub_y)) * @sizeOf(Sample);
    const count_size = (coded_pixels + 2 * coded_pixels / (sub_x * sub_y)) / 16;
    const mode_size = coded_pixels / 16;
    const qp_size = coded_pixels / 256;
    const motion_size = coded_pixels / 16 * @sizeOf(@import("h264_motion.zig").Motion);
    const reference_size = (planar_size + 2 * motion_size + qp_size * @sizeOf(@import("h264_entropy.zig").Meta) + @sizeOf(usize)) * cfg.max_refs;
    const group_size = if (cfg.groups.count > 1) qp_size else 0;
    const explicit_size = if (cfg.groups.explicit) |map| map.len else 0;
    const meta_size = group_size + explicit_size + qp_size * @sizeOf(@import("h264_entropy.zig").Meta);
    var peak = try std.math.add(usize, indexes.len, try std.math.add(usize, try std.math.add(usize, planar_size + mode_size + qp_size + meta_size + 2 * motion_size + reference_size, output_size), try std.math.add(usize, count_size, try std.math.add(usize, try std.math.mul(usize, max_packet, 2), reader.track.avcc.len * 2))));
    if (peak > options.max_decode_bytes) return error.ResourceLimitExceeded;
    var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = output_size }) else media.admission.Token{};
    errdefer reservation.deinit();
    var transient = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = peak - output_size - config_reservation.resources.host_bytes }) else media.admission.Token{};
    defer transient.deinit();
    const Workspace = @import("h264_assembly.zig").Workspace(Sample, PictureHeader);
    if (options.max_pending_pictures == 0 or options.max_pending_pictures > 16) return error.ResourceLimitExceeded;
    const workspace_size = planar_size + count_size + mode_size + qp_size + qp_size * @sizeOf(@import("h264_entropy.zig").Meta) + 2 * motion_size + group_size + indexes.len;
    const snapshot_size = reference_size + @sizeOf(Workspace.State);
    var workspaces: [16]?Workspace = @splat(null);
    defer for (&workspaces) |*slot| if (slot.*) |*workspace| workspace.deinit(allocator);
    workspaces[0] = try Workspace.init(allocator, coded_pixels, sub_x * sub_y, group_size, indexes.len);
    var references = @import("h264_references.zig").StateFor(Sample){ .chroma_format = cfg.chroma_format };
    defer references.deinit(allocator);
    const output = try allocator.alloc(u8, output_size);
    errdefer allocator.free(output);
    var decoded_packets: usize = 0;
    var field_macroblocks: usize = 0;
    var payload_bytes: u64 = 0;
    var next_picture_id: u32 = 0;
    var packet_index = start;
    var end = index + 1;
    while (packet_index < end) : (packet_index += 1) {
        if (packet_index - start >= options.max_dependency_packets) return error.ResourceLimitExceeded;
        var input_packet = try reader.readPacket(packet_index);
        defer input_packet.deinit();
        if (reader.packets[packet_index].size > options.max_packet_bytes) return error.ResourceLimitExceeded;
        try @import("avc.zig").validatePacket(input_packet.bytes, reader.track.nal_length_bytes);
        var cursor: usize = 0;
        var packet_picture: ?u32 = null;
        while (cursor < input_packet.bytes.len) {
            try reader.input.control.check();
            var size: usize = 0;
            for (input_packet.bytes[cursor..][0..reader.track.nal_length_bytes]) |byte| size = (size << 8) | byte;
            cursor += reader.track.nal_length_bytes;
            const nal = input_packet.bytes[cursor..][0..size];
            cursor += size;
            switch (nal[0] & 31) {
                1, 5 => {
                    if (nal[0] & 31 == 5 and nal[0] & 0x60 == 0) return error.MalformedVideoPacket;
                    const prefix = try slicePrefix(allocator, cfg, nal, reader.input.control);
                    var found: ?usize = null;
                    for (&workspaces, 0..) |*slot, i| if (slot.*) |*workspace| {
                        if (!workspace.active) continue;
                        const h = workspace.headers[0] orelse workspace.headers[1].?;
                        if (h.frame_num == prefix.frame_num and h.field_pic == prefix.field and h.reference == prefix.reference and (!prefix.idr or h.idr and h.idr_id == prefix.idr_id)) {
                            if (found != null) return error.MixedVideoPictures;
                            found = i;
                        }
                    };
                    if (found == null) {
                        // A fresh IDR starts a new pairing generation. Requested
                        // incomplete pictures cannot silently disappear at reset.
                        if (prefix.idr) for (&workspaces) |*slot| if (slot.*) |*workspace| {
                            if (workspace.active and workspace.wanted()) return error.IncompleteVideoPicture;
                            if (workspace.snapshot != null) {
                                workspace.releaseSnapshot(allocator);
                                peak -= snapshot_size;
                            }
                            workspace.active = false;
                        };
                        try transient.resize(.{ .host_bytes = peak - output_size - config_reservation.resources.host_bytes });
                        for (workspaces[0..options.max_pending_pictures], 0..) |slot, i| {
                            if (slot == null or !slot.?.active) {
                                found = i;
                                break;
                            }
                        }
                        const free = found orelse return error.ResourceLimitExceeded;
                        if (workspaces[free] == null) {
                            peak = try std.math.add(usize, peak, workspace_size);
                            if (peak > options.max_decode_bytes) return error.ResourceLimitExceeded;
                            try transient.resize(.{ .host_bytes = peak - output_size - config_reservation.resources.host_bytes });
                            workspaces[free] = try Workspace.init(allocator, coded_pixels, sub_x * sub_y, group_size, indexes.len);
                        }
                        workspaces[free].?.reset(next_picture_id);
                        next_picture_id += 1;
                    }
                    const workspace = &workspaces[found.?].?;
                    if (packet_picture) |id| if (id != workspace.id) return error.MixedVideoPictures;
                    packet_picture = workspace.id;
                    if (workspace.slices >= options.max_slices) return error.ResourceLimitExceeded;
                    workspace.slices += 1;
                    for (indexes, 0..) |wanted, slot| if (wanted == packet_index) {
                        workspace.selected[slot] = true;
                    };
                    const part = reader.packets[packet_index];
                    const part_end = std.math.add(i64, part.pts, part.duration) catch return error.TimestampOverflow;
                    const first_part = workspace.pts == null;
                    workspace.pts = if (workspace.pts) |pts| @min(pts, part.pts) else part.pts;
                    workspace.end = if (first_part) part_end else @max(workspace.end, part_end);
                    const planes = [3][]Sample{ workspace.planar[0..coded_pixels], workspace.planar[coded_pixels..][0 .. coded_pixels / (sub_x * sub_y)], workspace.planar[coded_pixels + coded_pixels / (sub_x * sub_y) ..] };
                    const counts = [3][]u8{ workspace.counts[0 .. coded_pixels / 16], workspace.counts[coded_pixels / 16 ..][0 .. coded_pixels / (16 * sub_x * sub_y)], workspace.counts[coded_pixels / 16 + coded_pixels / (16 * sub_x * sub_y) ..] };
                    const prediction = workspace.snapshot orelse &references;
                    prediction.current_pair = workspace.id;
                    try decodeSlice(allocator, nal, cfg, planes, counts, workspace.modes, workspace.qps, workspace.metadata, workspace.motions[0], workspace.motions[1], prediction, &workspace.headers, workspace.groups, reader.input.control);
                    const header = workspace.headers[prefix.parity].?;
                    const covered = workspace.covers(prefix.field, prefix.parity, cfg.coded_width);
                    if (covered and !workspace.committed[prefix.parity]) {
                        if (workspace.snapshot != null) {
                            workspace.releaseSnapshot(allocator);
                            peak -= snapshot_size;
                            try transient.resize(.{ .host_bytes = peak - output_size - config_reservation.resources.host_bytes });
                        }
                        try @import("h264_deblock.zig").picture(planes, cfg.coded_width, workspace.qps, workspace.metadata, counts, workspace.motions, .{ cfg.chroma_offset, cfg.chroma_offset1 }, cfg.bit_depth, cfg.chroma_format, cfg.mbaff or prefix.field, reader.input.control);
                        restoreMarking(&references, workspace, header, prefix.parity, cfg.mbaff);
                        try references.commit(allocator, workspace.planar, workspace.motions, cfg.max_refs);
                        workspace.committed[prefix.parity] = true;
                        if (prefix.field) {
                            for (workspace.metadata, 0..) |*m, i| if (i / (cfg.coded_width / 16) % 2 == prefix.parity) {
                                m.filter = 1;
                            };
                            workspace.complete = workspace.committed[0] and workspace.committed[1];
                        } else workspace.complete = true;
                    } else if (prefix.field and !covered and workspace.snapshot == null) {
                        peak = try std.math.add(usize, peak, snapshot_size);
                        if (peak > options.max_decode_bytes) return error.ResourceLimitExceeded;
                        try transient.resize(.{ .host_bytes = peak - output_size - config_reservation.resources.host_bytes });
                        const snapshot = try allocator.create(Workspace.State);
                        snapshot.* = references.clone();
                        workspace.snapshot = snapshot;
                    }
                },
                6, 9, 12 => {},
                else => return error.UnsupportedVideoProfile,
            }
        }
        decoded_packets += 1;
        payload_bytes += reader.packets[packet_index].size;
        if (packet_picture == null) for (indexes) |wanted| if (wanted == packet_index) return error.UnsupportedVideoProfile;
        for (&workspaces) |*slot| if (slot.*) |*workspace| {
            if (!workspace.active) continue;
            if (!workspace.complete) {
                const h = workspace.headers[0] orelse workspace.headers[1].?;
                if (!h.field_pic) return error.IncompleteVideoPicture;
                if (workspace.wanted()) try extendAssembly(reader, start, packet_index, &end, &max_packet, &peak, &transient, output_size, config_reservation.resources.host_bytes, options);
                continue;
            }
            for (workspace.metadata) |m| field_macroblocks += @intFromBool(m.field);
            const selected = workspace.wanted();
            if (selected) {
                output_packet.pts = workspace.pts.?;
                output_packet.duration = std.math.cast(u32, std.math.sub(i64, workspace.end, output_packet.pts) catch return error.TimestampOverflow) orelse return error.TimestampOverflow;
            }
            const planes = [3][]Sample{ workspace.planar[0..coded_pixels], workspace.planar[coded_pixels..][0 .. coded_pixels / (sub_x * sub_y)], workspace.planar[coded_pixels + coded_pixels / (sub_x * sub_y) ..] };
            if (selected) {
                for (0..cfg.height) |y| {
                    try reader.input.control.check();
                    for (0..cfg.width) |x| {
                        const value = planes[0][(y + cfg.top) * cfg.coded_width + cfg.left + x];
                        if (Sample == u8) output[y * cfg.width + x] = value else std.mem.writeInt(u16, output[(y * cfg.width + x) * 2 ..][0..2], value, .little);
                    }
                }
                for (0..cfg.height / sub_y) |y| {
                    try reader.input.control.check();
                    for (0..cfg.width / sub_x) |x| {
                        for (0..2) |p| {
                            const value = planes[p + 1][(y + cfg.top / sub_y) * (cfg.coded_width / sub_x) + x + cfg.left / sub_x];
                            const offset = cfg.width * cfg.height + y * (cfg.width / sub_x) * 2 + x * 2 + p;
                            if (Sample == u8) output[offset] = value else std.mem.writeInt(u16, output[offset * 2 ..][0..2], value, .little);
                        }
                    }
                }
                if (callback) |publish| {
                    const metadata_frame = Frame{ .interlaced = !cfg.frame_only, .field_macroblocks = field_macroblocks, .chroma_format = cfg.chroma_format, .bit_depth = cfg.bit_depth, .allocator = budget.backing, .nv12 = output, .width = @intCast(cfg.width), .height = @intCast(cfg.height), .pts = output_packet.pts, .duration = output_packet.duration, .timescale = reader.track.timescale, .full_range = cfg.full_range, .decode_high_water = budget.peak, .decoded_packets = decoded_packets, .payload_bytes = payload_bytes };
                    for (workspace.selected, 0..) |wanted, request_slot| if (wanted) {
                        try publish(callback_context.?, request_slot, &metadata_frame);
                    };
                }
            }
            workspace.active = false;
        };
    }
    return .{ .interlaced = !cfg.frame_only, .field_macroblocks = field_macroblocks, .chroma_format = cfg.chroma_format, .bit_depth = cfg.bit_depth, .reservation = reservation, .allocator = budget.backing, .nv12 = output, .width = @intCast(cfg.width), .height = @intCast(cfg.height), .pts = output_packet.pts, .duration = output_packet.duration, .timescale = reader.track.timescale, .full_range = cfg.full_range, .decode_high_water = budget.peak, .decoded_packets = decoded_packets, .payload_bytes = payload_bytes };
}
