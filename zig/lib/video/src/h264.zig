// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Portable H.264 subset: progressive 8-bit 4:2:0 Baseline/Main/High,
//! single-slice I/P/B pictures, CAVLC/CABAC and 4x4/8x8 reconstruction.
//! Unsupported coding tools fail closed; no system codec or FFmpeg dependency.
const std = @import("std");
const media = @import("antfly_media");
const preparation = @import("preparation.zig");
const Bits = @import("h264_bits.zig").Bits;
const tables = @import("h264_tables.zig");
pub const Options = struct {
    max_pixels: usize = 16 * 1024 * 1024,
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_decode_bytes: usize = 128 * 1024 * 1024,
    max_dependency_packets: usize = 256,
};
const Config = struct {
    profile: u32,
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
    nv12: []u8,
    width: u32,
    height: u32,
    pts: i64,
    duration: u32,
    timescale: u32,
    full_range: bool,
    decode_high_water: usize,
    decoded_packets: usize = 1,
    payload_bytes: u64 = 0,
    pub fn host(self: *const Frame) preparation.HostSurface {
        const size = @as(usize, self.width) * self.height;
        return .{ .width = self.width, .height = self.height, .format = if (self.full_range) .nv12_full else .nv12_video, .planes = .{ .{ .bytes = self.nv12[0..size], .width = self.width, .height = self.height, .stride = self.width }, .{ .bytes = self.nv12[size..], .width = self.width / 2, .height = self.height / 2, .stride = self.width } } };
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
    try @import("avc.zig").validateConfig(config);
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
    if (profile == 100) {
        if (try bits.ue() != 1 or try bits.ue() != 0 or try bits.ue() != 0 or try bits.read(1) != 0 or try bits.read(1) != 0) return error.UnsupportedVideoProfile;
    }
    const frame_bits = try bits.ue() + 4;
    if (frame_bits > 16) return error.UnsupportedVideoProfile;
    const poc = try bits.ue();
    var poc_bits: ?usize = null;
    if (poc == 0) {
        poc_bits = try bits.ue() + 4;
        if (poc_bits.? > 16) return error.UnsupportedVideoProfile;
    } else if (poc != 2) return error.UnsupportedVideoProfile;
    const max_refs = try bits.ue();
    if (max_refs > 16 or try bits.read(1) != 0) return error.UnsupportedVideoProfile;
    const mbs_width = try bits.ue() + 1;
    const mbs_height = try bits.ue() + 1;
    if (mbs_width > 1024 or mbs_height > 1024 or try bits.read(1) != 1) return error.UnsupportedVideoProfile;
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
    const coded_height: usize = mbs_height * 16;
    if (left + right >= coded_width / 2 or top + bottom >= coded_height / 2) return error.MalformedVideoConfig;
    var full_range = false;
    if (try bits.read(1) != 0) full_range = try vui(&bits);
    try bits.finish();
    if (cursor >= config.len or config[cursor] != 1) return error.UnsupportedVideoProfile;
    cursor += 1;
    const pps = try nalSet(config, &cursor);
    if (cursor != config.len and profile != 100) return error.UnsupportedVideoProfile;
    var p = try Bits.init(allocator, pps);
    defer p.deinit();
    const pps_id = try p.ue();
    if (pps_id > 255 or try p.ue() != id) return error.MalformedVideoConfig;
    const cabac = try p.read(1) != 0;
    if (cabac and profile == 66) return error.MalformedVideoConfig;
    const bottom_poc = try p.read(1) != 0;
    if (try p.ue() != 0) return error.UnsupportedVideoProfile;
    const active0 = try p.ue() + 1;
    const active1 = try p.ue() + 1;
    if (active0 > 16 or active1 > 16) return error.UnsupportedVideoProfile;
    const weighted_p = try p.read(1) != 0;
    const weighted_b: u2 = @intCast(try p.read(2));
    if (weighted_b == 3 or (profile == 66 and (weighted_p or weighted_b != 0))) return error.MalformedVideoConfig;
    const qp = try p.se() + 26;
    if (qp < 0 or qp > 51) return error.MalformedVideoConfig;
    _ = try p.se();
    const chroma_offset = try p.se();
    if (chroma_offset < -12 or chroma_offset > 12) return error.MalformedVideoConfig;
    const deblock_present = try p.read(1) != 0;
    if (try p.read(1) != 0) return error.UnsupportedVideoProfile;
    if (try p.read(1) != 0) return error.UnsupportedVideoProfile;
    var transform8 = false;
    if (p.position != p.end) {
        transform8 = try p.read(1) != 0;
        if (transform8 and profile != 100) return error.MalformedVideoConfig;
        if (try p.read(1) != 0 or try p.se() != chroma_offset) return error.UnsupportedVideoProfile;
    }
    try p.finish();
    return .{ .profile = profile, .max_refs = max_refs, .active0 = active0, .active1 = active1, .weighted_p = weighted_p, .weighted_b = weighted_b, .direct8 = direct8, .transform8 = transform8, .cabac = cabac, .id = id, .pps = pps_id, .frame_bits = frame_bits, .poc_bits = poc_bits, .coded_width = coded_width, .coded_height = coded_height, .width = coded_width - 2 * (left + right), .height = coded_height - 2 * (top + bottom), .left = 2 * left, .top = 2 * top, .full_range = full_range, .bottom_poc = bottom_poc, .qp = qp, .chroma_offset = chroma_offset, .deblock_present = deblock_present };
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
        for (tables.coeff_token[table], 0..) |row, coeff| for (row, 0..) |word, ones| {
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
        if (@abs(level) > 32768) return error.MalformedVideoPacket;
        levels[i] = @intCast(level);
        if (suffix == 0) suffix = 1;
        if (@abs(level) > (@as(u32, 3) << @as(u5, @intCast(suffix - 1))) and suffix < 6) suffix += 1;
    }
    var zeros: usize = if (total == max) 0 else try vlc(bits, if (dc_chroma) &tables.chroma_zeros[total] else &tables.total_zeros[total]);
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

fn clipByte(value: i64) u8 {
    return @intCast(std.math.clamp(value, 0, 255));
}
fn predict(plane: []u8, stride: usize, x: usize, y: usize, size: usize, mode: u32, chroma: bool) !void {
    var top: [16]i64 = undefined;
    var left: [16]i64 = undefined;
    const has_top = y != 0;
    const has_left = x != 0;
    for (0..size) |i| {
        top[i] = if (has_top) plane[(y - 1) * stride + x + i] else 128;
        left[i] = if (has_left) plane[(y + i) * stride + x - 1] else 128;
    }
    // Luma modes: vertical, horizontal, DC, plane. Chroma: DC, horizontal,
    // vertical, plane. Neighbors exist only after preceding raster macroblocks.
    const resolved = if (chroma) ([_]u32{ 2, 1, 0, 3 })[@min(mode, 3)] else mode;
    if (mode > 3 or (resolved == 0 and !has_top) or (resolved == 1 and !has_left) or (resolved == 3 and (!has_top or !has_left))) return error.MalformedVideoPacket;
    var a: i64 = 0;
    var b: i64 = 0;
    var c: i64 = 0;
    if (resolved == 3) {
        const corner: i64 = plane[(y - 1) * stride + x - 1];
        const half = size / 2;
        var h: i64 = 0;
        var v: i64 = 0;
        for (1..half + 1) |i| {
            h += @as(i64, @intCast(i)) * (top[half - 1 + i] - (if (i == half) corner else top[half - 1 - i]));
            v += @as(i64, @intCast(i)) * (left[half - 1 + i] - (if (i == half) corner else left[half - 1 - i]));
        }
        a = 16 * (top[size - 1] + left[size - 1]);
        b = if (chroma) (17 * h + 16) >> 5 else (5 * h + 32) >> 6;
        c = if (chroma) (17 * v + 16) >> 5 else (5 * v + 32) >> 6;
    }
    var dc: [4]i64 = @splat(128);
    if (resolved == 2) {
        if (chroma) {
            var t: [2]i64 = @splat(0);
            var l: [2]i64 = @splat(0);
            for (0..8) |i| {
                t[i / 4] += top[i];
                l[i / 4] += left[i];
            }
            dc[0] = if (has_top and has_left) (t[0] + l[0] + 4) >> 3 else if (has_top) (t[0] + 2) >> 2 else if (has_left) (l[0] + 2) >> 2 else 128;
            dc[1] = if (has_top) (t[1] + 2) >> 2 else if (has_left) (l[0] + 2) >> 2 else 128;
            dc[2] = if (has_left) (l[1] + 2) >> 2 else if (has_top) (t[0] + 2) >> 2 else 128;
            dc[3] = if (has_top and has_left) (t[1] + l[1] + 4) >> 3 else if (has_top) (t[1] + 2) >> 2 else if (has_left) (l[1] + 2) >> 2 else 128;
        } else {
            var sum: i64 = 0;
            for (0..16) |i| {
                if (has_top) sum += top[i];
                if (has_left) sum += left[i];
            }
            dc[0] = if (has_top and has_left) (sum + 16) >> 5 else if (has_top or has_left) (sum + 8) >> 4 else 128;
        }
    }
    for (0..size) |row| for (0..size) |column| {
        const value = switch (resolved) {
            0 => top[column],
            1 => left[row],
            2 => dc[if (chroma) (row / 4) * 2 + column / 4 else 0],
            3 => (a + b * (@as(i64, @intCast(column)) - @as(i64, @intCast(size / 2 - 1))) + c * (@as(i64, @intCast(row)) - @as(i64, @intCast(size / 2 - 1))) + 16) >> 5,
            else => unreachable,
        };
        plane[(y + row) * stride + x + column] = clipByte(value);
    };
}
const factors = [6][3]i64{ .{ 10, 13, 16 }, .{ 11, 14, 18 }, .{ 13, 16, 20 }, .{ 14, 18, 23 }, .{ 16, 20, 25 }, .{ 18, 23, 29 } };
fn dequant(value: i32, qp: usize, index: usize) i64 {
    const category: usize = (index % 4 & 1) + (index / 4 & 1);
    return @as(i64, value) * factors[qp % 6][category] * (@as(i64, 1) << @as(u6, @intCast(qp / 6)));
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
fn inverseAdd(plane: []u8, stride: usize, x: usize, y: usize, coefficients: [16]i64) void {
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
        plane[offset] = clipByte(@as(i64, plane[offset]) + ((result[row * 4 + column] + 32) >> 6));
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
fn chromaQp(qp: i32, offset: i32) usize {
    const index: usize = @intCast(std.math.clamp(qp + offset, 0, 51));
    return if (index < 30) index else ([_]usize{ 29, 30, 31, 32, 32, 33, 34, 34, 35, 35, 36, 36, 37, 37, 37, 38, 38, 38, 39, 39, 39, 39 })[index - 30];
}
fn reorder(bits: *Bits, state: *@import("h264_references.zig").State, frame_bits: usize, active: usize) !void {
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
fn decodeSlice(allocator: std.mem.Allocator, nal: []const u8, cfg: Config, planes: [3][]u8, counts: [3][]u8, modes: []u8, qps: []u8, metadata: []@import("h264_entropy.zig").Meta, motions: []@import("h264_motion.zig").Motion, motions1: []@import("h264_motion.zig").Motion, references: *@import("h264_references.zig").State, control: media.source.Control) !void {
    var bits = try Bits.initSlice(allocator, nal, control, cfg.cabac);
    defer bits.deinit();
    if (try bits.ue() != 0) return error.UnsupportedVideoProfile;
    const encoded_type = try bits.ue();
    if (encoded_type > 9) return error.MalformedVideoPacket;
    const slice_type = encoded_type % 5;
    if (slice_type != 2 and slice_type != 0 and slice_type != 1) return error.UnsupportedVideoProfile;
    if (cfg.profile == 66 and slice_type == 1) return error.MalformedVideoPacket;
    if (try bits.ue() != cfg.pps) return error.MalformedVideoPacket;
    const frame_num = try bits.read(cfg.frame_bits);
    const idr = nal[0] & 31 == 5;
    references.reference = nal[0] & 0x60 != 0;
    if (idr) {
        if (frame_num != 0 or slice_type != 2) return error.MalformedVideoPacket;
        references.deinit(allocator);
        references.previous_lsb = 0;
        references.previous_msb = 0;
        references.previous_num = 0;
        references.frame_offset = 0;
        _ = try bits.ue();
    }
    references.frame_bits = cfg.frame_bits;
    if (references.previous_num > frame_num) references.frame_offset += @as(i32, 1) << @as(u5, @intCast(cfg.frame_bits));
    references.previous_num = frame_num;
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
        if (cfg.bottom_poc and try bits.se() != 0) return error.UnsupportedVideoProfile;
    } else references.current_poc = (references.frame_offset + @as(i32, @intCast(frame_num))) * 2 - @as(i32, @intFromBool(!references.reference));
    references.order(cfg.frame_bits);
    if (slice_type == 1) references.orderB();
    const spatial_direct = slice_type == 1 and try bits.read(1) != 0;
    var active0 = cfg.active0;
    var active1 = cfg.active1;
    if (slice_type != 2) {
        if (try bits.read(1) != 0) {
            active0 = try bits.ue() + 1;
            if (slice_type == 1) active1 = try bits.ue() + 1;
        }
        if (active0 > 16 or active1 > 16) return error.UnsupportedVideoProfile;
        references.list_count = @max(references.list_count, @max(active0, if (slice_type == 1) active1 else 0));
        if (try bits.read(1) != 0) try reorder(&bits, references, cfg.frame_bits, active0);
        if (slice_type == 1 and try bits.read(1) != 0) {
            std.mem.swap([16]usize, &references.list0, &references.list1);
            try reorder(&bits, references, cfg.frame_bits, active1);
            std.mem.swap([16]usize, &references.list0, &references.list1);
        }
    }
    references.weight_mode = .none;
    if ((slice_type == 0 and cfg.weighted_p) or (slice_type == 1 and cfg.weighted_b == 1)) {
        references.weights = try @import("h264_weights.zig").parse(&bits, .{ active0, active1 }, slice_type == 1);
        references.weight_mode = .explicit;
    } else if (slice_type == 1 and cfg.weighted_b == 2) references.weight_mode = .implicit;
    try references.marking(&bits, idr);
    var init_idc: usize = 0;
    if (cfg.cabac and slice_type != 2) {
        init_idc = try bits.ue();
        if (init_idc > 2) return error.MalformedVideoPacket;
    }
    var qp = cfg.qp + try bits.se();
    if (qp < 0 or qp > 51) return error.MalformedVideoPacket;
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
    var syntax = try @import("h264_entropy.zig").Syntax.init(&bits, cfg.cabac, qp, slice_type, init_idc, metadata, counts, cfg.coded_width);
    const mb_width = cfg.coded_width / 16;
    const mb_height = cfg.coded_height / 16;
    var skip_run: usize = 0;
    var after_skip = false;
    for (0..mb_width * mb_height) |mb| {
        try control.check();
        const x = mb % mb_width * 16;
        const y = mb / mb_width * 16;
        syntax.mb = mb;
        var skipped = false;
        if (slice_type != 2) {
            if (cfg.cabac) skipped = try syntax.skip() else {
                if (skip_run == 0 and !after_skip) {
                    skip_run = try bits.ue();
                    if (skip_run > mb_width * mb_height - mb) return error.MalformedVideoPacket;
                }
                if (skip_run != 0) {
                    skipped = true;
                    skip_run -= 1;
                    after_skip = true;
                } else after_skip = false;
            }
        }
        const encoded_kind: u32 = if (skipped) 0 else try syntax.kind();
        const inter = (slice_type == 0 and encoded_kind < 5) or (slice_type == 1 and encoded_kind < 23);
        const kind = if (inter) @as(u32, 26) else encoded_kind - @as(u32, if (slice_type == 0) 5 else if (slice_type == 1) 23 else 0);
        if ((!inter and kind > 25) or (skipped and !inter)) return error.UnsupportedVideoProfile;
        metadata[mb].kind = @intCast(if (skipped) 27 else kind);
        if (!inter) {
            for (0..4) |row| for (0..4) |column| {
                motions[(y / 4 + row) * (cfg.coded_width / 4) + x / 4 + column] = .{ .decoded = true };
                motions1[(y / 4 + row) * (cfg.coded_width / 4) + x / 4 + column] = .{ .decoded = true };
            };
        }
        const allow8 = if (inter) try @import("h264_inter.zig").predict(&syntax, references, planes, .{ motions, motions1 }, cfg.coded_width, cfg.coded_height, encoded_kind, .{ active0, active1 }, skipped, spatial_direct, cfg.direct8) else true;
        if (skipped) {
            qps[mb] = @intCast(qp);
            for (0..4) |row| @memset(modes[(y / 4 + row) * (cfg.coded_width / 4) + x / 4 ..][0..4], 2);
            syntax.previous_delta = 0;
            try syntax.endMb(mb + 1 == mb_width * mb_height);
            continue;
        }
        if (kind == 25) {
            qps[mb] = 0;
            while (bits.position % 8 != 0) if (try bits.read(1) != 0) return error.MalformedVideoPacket;
            for (0..3) |plane| {
                const size: usize = if (plane == 0) 16 else 8;
                const stride = if (plane == 0) cfg.coded_width else cfg.coded_width / 2;
                const px = if (plane == 0) x else x / 2;
                const py = if (plane == 0) y else y / 2;
                for (0..size) |row| for (0..size) |column| {
                    planes[plane][(py + row) * stride + px + column] = @intCast(try bits.read(8));
                };
                for (0..size / 4) |row| @memset(counts[plane][(py / 4 + row) * (stride / 4) + px / 4 ..][0 .. size / 4], 16);
            }
            for (0..4) |row| @memset(modes[(y / 4 + row) * (cfg.coded_width / 4) + x / 4 ..][0..4], 2);
            metadata[mb].dc = 7;
            metadata[mb].cbp = 47;
            try syntax.endMb(mb + 1 == mb_width * mb_height);
            continue;
        }
        if (!inter and kind > 24) return error.UnsupportedVideoProfile;
        const value = if (inter) @as(u32, 0) else kind -| 1;
        const mode = value % 4;
        var cbp_chroma = value / 4 % 3;
        var cbp_luma: u32 = if (value / 12 != 0) 15 else 0;
        var use8 = kind == 0 and cfg.transform8 and try syntax.transform8();
        metadata[mb].transform8 = use8;
        if (kind == 0) {
            for (0..if (use8) @as(usize, 4) else 16) |block| {
                const i = if (use8) block * 4 else block;
                const point = blockPoint(i);
                const bx = x / 4 + point[0];
                const by = y / 4 + point[1];
                const stride = cfg.coded_width / 4;
                const left = if (bx == 0) @as(u8, 255) else modes[by * stride + bx - 1];
                const top = if (by == 0) @as(u8, 255) else modes[(by - 1) * stride + bx];
                const predicted: u8 = if (left == 255 or top == 255) 2 else @min(left, top);
                const resolved = try syntax.mode(predicted);
                if (use8) {
                    for (0..2) |row| @memset(modes[(by + row) * stride + bx ..][0..2], resolved);
                } else modes[by * stride + bx] = resolved;
            }
        } else for (0..4) |row| @memset(modes[(y / 4 + row) * (cfg.coded_width / 4) + x / 4 ..][0..4], 2);
        const chroma_mode = if (inter) @as(u32, 0) else try syntax.chroma();
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
            if (delta < -26 or delta > 25) return error.MalformedVideoPacket;
            qp = @mod(qp + delta + 52, 52);
        } else syntax.previous_delta = 0;
        qps[mb] = @intCast(qp);
        const q: usize = @intCast(qp);
        if (!inter and kind != 0) try predict(planes[0], cfg.coded_width, x, y, 16, mode, false);
        if (!inter) try predict(planes[1], cfg.coded_width / 2, x / 2, y / 2, 8, chroma_mode, true);
        if (!inter) try predict(planes[2], cfg.coded_width / 2, x / 2, y / 2, 8, chroma_mode, true);
        const luma_dc = if (!inter and kind != 0) try syntax.coeff(context(counts[0], cfg.coded_width / 4, x / 4, y / 4), 16, 0, 0, x / 4, y / 4) else Residual{ .total = 0 };
        var dc_input: [16]i64 = @splat(0);
        for (zigzag, luma_dc.values) |position, coefficient| dc_input[position] = coefficient;
        var dc = hadamard4(dc_input);
        for (&dc) |*coefficient| {
            const scaled = coefficient.* * factors[q % 6][0];
            coefficient.* = if (q >= 12) scaled << @as(u6, @intCast(q / 6 - 2)) else (scaled + (@as(i64, 1) << @as(u6, @intCast(1 - q / 6)))) >> @as(u6, @intCast(2 - q / 6));
        }
        if (use8) {
            for (0..4) |i| {
                const bx = x / 4 + (i % 2) * 2;
                const by = y / 4 + (i / 2) * 2;
                const right = bx + 2;
                const above = by -| 1;
                const right_mb = (above / 4) * mb_width + right / 4;
                const right_block = right % 4 / 2 + above % 4 / 2 * 2;
                const available = by != 0 and right < cfg.coded_width / 4 and (right_mb < mb or (right_mb == mb and right_block < i));
                if (!inter) try @import("h264_intra.zig").predict8(planes[0], cfg.coded_width, bx * 4, by * 4, modes[by * (cfg.coded_width / 4) + bx], available);
                if (cbp_luma & (@as(u32, 1) << @as(u5, @intCast(i))) != 0) {
                    const coefficients = try syntax.coeff8(bx, by);
                    @import("h264_transform8.zig").add(planes[0], cfg.coded_width, bx * 4, by * 4, coefficients, q);
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
                const right_mb = (above_by / 4) * mb_width + right_bx / 4;
                const right_scan = (right_bx % 4 / 2 + above_by % 4 / 2 * 2) * 4 + right_bx % 2 + above_by % 2 * 2;
                const available = by != 0 and right_bx < cfg.coded_width / 4 and (right_mb < mb or (right_mb == mb and right_scan < i));
                try @import("h264_intra.zig").predict(planes[0], cfg.coded_width, bx * 4, by * 4, modes[by * (cfg.coded_width / 4) + bx], available);
            }
            if (cbp_luma & (@as(u32, 1) << @as(u5, @intCast(i / 4))) != 0) {
                const ac = try syntax.coeff(context(counts[0], cfg.coded_width / 4, bx, by), if (kind == 0 or inter) 16 else 15, if (kind == 0 or inter) 2 else 1, 0, bx, by);
                counts[0][by * (cfg.coded_width / 4) + bx] = ac.total;
                for (0..if (kind == 0 or inter) @as(usize, 16) else 15) |j| {
                    const position = zigzag[j + @as(usize, if (kind == 0 or inter) 0 else 1)];
                    coefficients[position] = dequant(ac.values[j], q, position);
                }
            }
            inverseAdd(planes[0], cfg.coded_width, bx * 4, by * 4, coefficients);
        }
        const cq = chromaQp(qp, cfg.chroma_offset);
        var chroma_dc: [2][4]i64 = @splat(@splat(0));
        if (cbp_chroma != 0) for (0..2) |p| {
            const r = try syntax.coeff(0, 4, 3, p + 1, x / 8, y / 8);
            const d = r.values;
            chroma_dc[p] = .{ @as(i64, d[0]) + d[1] + d[2] + d[3], @as(i64, d[0]) - d[1] + d[2] - d[3], @as(i64, d[0]) + d[1] - d[2] - d[3], @as(i64, d[0]) - d[1] - d[2] + d[3] };
            for (&chroma_dc[p]) |*coefficient| coefficient.* = (coefficient.* * factors[cq % 6][0] * (@as(i64, 1) << @as(u6, @intCast(cq / 6)))) >> 1;
        };
        for (1..3) |p| for (0..4) |i| {
            const bx = x / 8 + i % 2;
            const by = y / 8 + i / 2;
            const stride = cfg.coded_width / 2;
            var coefficients: [16]i64 = @splat(0);
            coefficients[0] = chroma_dc[p - 1][i];
            if (cbp_chroma == 2) {
                const ac = try syntax.coeff(context(counts[p], stride / 4, bx, by), 15, 4, p, bx, by);
                counts[p][by * (stride / 4) + bx] = ac.total;
                for (0..15) |j| coefficients[zigzag[j + 1]] = dequant(ac.values[j], cq, zigzag[j + 1]);
            }
            inverseAdd(planes[p], stride, bx * 4, by * 4, coefficients);
        };
        try syntax.endMb(mb + 1 == mb_width * mb_height);
    }
    try syntax.finish();
    if (filter != 1) try @import("h264_deblock.zig").picture(planes, cfg.coded_width, qps, metadata, counts[0], .{ motions, motions1 }, references, cfg.chroma_offset, alpha_offset, beta_offset, control);
}
/// Decode from a verified IDR through the requested packet. Output is owned;
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
    const packet = reader.packets[index];
    var config_reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = reader.track.avcc.len * 2 }) else media.admission.Token{};
    defer config_reservation.deinit();
    const cfg = try configParse(allocator, reader.track.avcc);
    config_reservation.deinit();
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
            if (try @import("avc.zig").isIdr(probe.bytes, reader.track.nal_length_bytes)) break;
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
    const output_size = cfg.width * cfg.height * 3 / 2;
    const planar_size = coded_pixels * 3 / 2;
    const count_size = coded_pixels * 3 / 32;
    const mode_size = coded_pixels / 16;
    const qp_size = coded_pixels / 256;
    const motion_size = coded_pixels / 16 * @sizeOf(@import("h264_motion.zig").Motion);
    const reference_size = (planar_size + 2 * motion_size) * cfg.max_refs;
    const meta_size = qp_size * @sizeOf(@import("h264_entropy.zig").Meta);
    const peak = try std.math.add(usize, try std.math.add(usize, planar_size + mode_size + qp_size + meta_size + 2 * motion_size + reference_size, output_size), try std.math.add(usize, count_size, try std.math.add(usize, try std.math.mul(usize, max_packet, 2), reader.track.avcc.len * 2)));
    if (peak > options.max_decode_bytes) return error.ResourceLimitExceeded;
    var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = output_size }) else media.admission.Token{};
    errdefer reservation.deinit();
    var transient = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = peak - output_size }) else media.admission.Token{};
    defer transient.deinit();
    const planar = try allocator.alloc(u8, planar_size);
    defer allocator.free(planar);
    @memset(planar, 0);
    const count_bytes = try allocator.alloc(u8, count_size);
    defer allocator.free(count_bytes);
    @memset(count_bytes, 0);
    const planes = [3][]u8{ planar[0..coded_pixels], planar[coded_pixels..][0 .. coded_pixels / 4], planar[coded_pixels + coded_pixels / 4 ..] };
    const counts = [3][]u8{ count_bytes[0 .. coded_pixels / 16], count_bytes[coded_pixels / 16 ..][0 .. coded_pixels / 64], count_bytes[coded_pixels / 16 + coded_pixels / 64 ..] };
    const metadata = try allocator.alloc(@import("h264_entropy.zig").Meta, qp_size);
    defer allocator.free(metadata);
    @memset(metadata, .{});
    const qps = try allocator.alloc(u8, qp_size);
    defer allocator.free(qps);
    const modes = try allocator.alloc(u8, mode_size);
    defer allocator.free(modes);
    @memset(modes, 255);
    const motions = try allocator.alloc(@import("h264_motion.zig").Motion, coded_pixels / 16);
    defer allocator.free(motions);
    const motions1 = try allocator.alloc(@import("h264_motion.zig").Motion, coded_pixels / 16);
    defer allocator.free(motions1);
    var references = @import("h264_references.zig").State{};
    defer references.deinit(allocator);
    const output = try allocator.alloc(u8, output_size);
    errdefer allocator.free(output);
    var decoded_packets: usize = 0;
    var payload_bytes: u64 = 0;
    for (start..index + 1) |packet_index| {
        var input_packet = try reader.readPacket(packet_index);
        defer input_packet.deinit();
        if (reader.packets[packet_index].size > options.max_packet_bytes) return error.ResourceLimitExceeded;
        try @import("avc.zig").validatePacket(input_packet.bytes, reader.track.nal_length_bytes);
        @memset(count_bytes, 0);
        @memset(modes, 255);
        @memset(metadata, .{});
        @memset(motions, .{ .reference = -2 });
        @memset(motions1, .{ .reference = -2 });
        var cursor: usize = 0;
        var decoded = false;
        while (cursor < input_packet.bytes.len) {
            try reader.input.control.check();
            var size: usize = 0;
            for (input_packet.bytes[cursor..][0..reader.track.nal_length_bytes]) |byte| size = (size << 8) | byte;
            cursor += reader.track.nal_length_bytes;
            const nal = input_packet.bytes[cursor..][0..size];
            cursor += size;
            switch (nal[0] & 31) {
                1, 5 => {
                    if (decoded or (nal[0] & 31 == 5 and nal[0] & 0x60 == 0)) return error.UnsupportedVideoProfile;
                    try decodeSlice(allocator, nal, cfg, planes, counts, modes, qps, metadata, motions, motions1, &references, reader.input.control);
                    decoded = true;
                },
                6, 9, 12 => {},
                else => return error.UnsupportedVideoProfile,
            }
        }
        if (!decoded) return error.UnsupportedVideoProfile;
        try references.commit(allocator, planar, .{ motions, motions1 }, cfg.max_refs);
        decoded_packets += 1;
        payload_bytes += reader.packets[packet_index].size;
        var selected = false;
        for (indexes) |wanted| if (wanted == packet_index) {
            selected = true;
            break;
        };
        if (selected) {
            for (0..cfg.height) |y| {
                try reader.input.control.check();
                @memcpy(output[y * cfg.width ..][0..cfg.width], planes[0][(y + cfg.top) * cfg.coded_width + cfg.left ..][0..cfg.width]);
            }
            for (0..cfg.height / 2) |y| {
                try reader.input.control.check();
                for (0..cfg.width / 2) |x| {
                    for (0..2) |p| {
                        output[cfg.width * cfg.height + y * cfg.width + x * 2 + p] = planes[p + 1][(y + cfg.top / 2) * (cfg.coded_width / 2) + x + cfg.left / 2];
                    }
                }
            }
            if (callback) |publish| {
                const metadata_frame = Frame{ .allocator = budget.backing, .nv12 = output, .width = @intCast(cfg.width), .height = @intCast(cfg.height), .pts = reader.packets[packet_index].pts, .duration = reader.packets[packet_index].duration, .timescale = reader.track.timescale, .full_range = cfg.full_range, .decode_high_water = budget.peak, .decoded_packets = decoded_packets, .payload_bytes = payload_bytes };
                for (indexes, 0..) |wanted, slot| if (wanted == packet_index) {
                    try publish(callback_context.?, slot, &metadata_frame);
                };
            }
        }
    }
    return .{ .reservation = reservation, .allocator = budget.backing, .nv12 = output, .width = @intCast(cfg.width), .height = @intCast(cfg.height), .pts = packet.pts, .duration = packet.duration, .timescale = reader.track.timescale, .full_range = cfg.full_range, .decode_high_water = budget.peak, .decoded_packets = decoded_packets, .payload_bytes = payload_bytes };
}
