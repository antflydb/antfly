// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Portable H.264 subset: progressive 8-bit 4:2:0 Baseline, IDR I_PCM and
//! Intra16x16/CAVLC pictures, one slice, with deblocking explicitly disabled.
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
};
const Config = struct {
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
    if (config[1] != 66 or config[5] & 31 != 1) return error.UnsupportedVideoProfile;
    var cursor: usize = 6;
    const sps = try nalSet(config, &cursor);
    var bits = try Bits.init(allocator, sps);
    defer bits.deinit();
    if (try bits.read(8) != 66) return error.UnsupportedVideoProfile;
    const constraints = try bits.read(8);
    if (constraints & 3 != 0) return error.MalformedVideoConfig;
    _ = try bits.read(8);
    const id = try bits.ue();
    if (id > 31) return error.MalformedVideoConfig;
    const frame_bits = try bits.ue() + 4;
    if (frame_bits > 16) return error.UnsupportedVideoProfile;
    const poc = try bits.ue();
    var poc_bits: ?usize = null;
    if (poc == 0) {
        poc_bits = try bits.ue() + 4;
        if (poc_bits.? > 16) return error.UnsupportedVideoProfile;
    } else if (poc != 2) return error.UnsupportedVideoProfile;
    if (try bits.ue() > 16 or try bits.read(1) != 0) return error.UnsupportedVideoProfile;
    const mbs_width = try bits.ue() + 1;
    const mbs_height = try bits.ue() + 1;
    if (mbs_width > 1024 or mbs_height > 1024 or try bits.read(1) != 1) return error.UnsupportedVideoProfile;
    _ = try bits.read(1);
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
    if (cursor != config.len) return error.UnsupportedVideoProfile;
    var p = try Bits.init(allocator, pps);
    defer p.deinit();
    const pps_id = try p.ue();
    if (pps_id > 255 or try p.ue() != id) return error.MalformedVideoConfig;
    if (try p.read(1) != 0) return error.UnsupportedVideoProfile;
    const bottom_poc = try p.read(1) != 0;
    if (try p.ue() != 0 or try p.ue() != 0 or try p.ue() != 0 or try p.read(1) != 0 or try p.read(2) != 0) return error.UnsupportedVideoProfile;
    const qp = try p.se() + 26;
    if (qp < 0 or qp > 51) return error.MalformedVideoConfig;
    _ = try p.se();
    const chroma_offset = try p.se();
    if (chroma_offset < -12 or chroma_offset > 12) return error.MalformedVideoConfig;
    const deblock_present = try p.read(1) != 0;
    _ = try p.read(1);
    if (try p.read(1) != 0 or p.position != p.end) return error.UnsupportedVideoProfile;
    return .{ .id = id, .pps = pps_id, .frame_bits = frame_bits, .poc_bits = poc_bits, .coded_width = coded_width, .coded_height = coded_height, .width = coded_width - 2 * (left + right), .height = coded_height - 2 * (top + bottom), .left = 2 * left, .top = 2 * top, .full_range = full_range, .bottom_poc = bottom_poc, .qp = qp, .chroma_offset = chroma_offset, .deblock_present = deblock_present };
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
const Residual = struct { values: [16]i32 = @splat(0), total: u8 = 0 };
fn vlc(bits: *Bits, words: []const [2]u8) !usize {
    var code: u32 = 0;
    for (1..17) |length| {
        code = (code << 1) | try bits.read(1);
        for (words, 0..) |word, i| if (word[1] == length and word[0] == code) return i;
    }
    return error.MalformedVideoPacket;
}
fn residual(bits: *Bits, nc: usize, max: usize, dc_chroma: bool) !Residual {
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
fn decodeSlice(allocator: std.mem.Allocator, nal: []const u8, cfg: Config, planes: [3][]u8, counts: [3][]u8, control: media.source.Control) !void {
    var bits = try Bits.initControlled(allocator, nal, control);
    defer bits.deinit();
    if (try bits.ue() != 0) return error.UnsupportedVideoProfile;
    const slice_type = try bits.ue();
    if (slice_type != 2 and slice_type != 7) return error.UnsupportedVideoProfile;
    if (try bits.ue() != cfg.pps or try bits.read(cfg.frame_bits) != 0) return error.MalformedVideoPacket;
    _ = try bits.ue(); // idr_pic_id
    if (cfg.poc_bits) |poc_bits| {
        _ = try bits.read(poc_bits);
        if (cfg.bottom_poc) _ = try bits.se();
    }
    _ = try bits.read(1);
    _ = try bits.read(1); // IDR reference marking
    var qp = cfg.qp + try bits.se();
    if (qp < 0 or qp > 51) return error.MalformedVideoPacket;
    if (!cfg.deblock_present or try bits.ue() != 1) return error.UnsupportedVideoProfile;
    const mb_width = cfg.coded_width / 16;
    const mb_height = cfg.coded_height / 16;
    for (0..mb_width * mb_height) |mb| {
        try control.check();
        const x = mb % mb_width * 16;
        const y = mb / mb_width * 16;
        const kind = try bits.ue();
        if (kind == 25) {
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
            continue;
        }
        if (kind == 0 or kind > 24) return error.UnsupportedVideoProfile;
        const value = kind - 1;
        const mode = value % 4;
        const cbp_chroma = value / 4 % 3;
        const cbp_luma = value / 12 != 0;
        const chroma_mode = try bits.ue();
        if (chroma_mode > 3) return error.MalformedVideoPacket;
        const delta = try bits.se();
        if (delta < -26 or delta > 25) return error.MalformedVideoPacket;
        qp = @mod(qp + delta + 52, 52);
        const q: usize = @intCast(qp);
        try predict(planes[0], cfg.coded_width, x, y, 16, mode, false);
        try predict(planes[1], cfg.coded_width / 2, x / 2, y / 2, 8, chroma_mode, true);
        try predict(planes[2], cfg.coded_width / 2, x / 2, y / 2, 8, chroma_mode, true);
        const luma_dc = try residual(&bits, context(counts[0], cfg.coded_width / 4, x / 4, y / 4), 16, false);
        var dc_input: [16]i64 = @splat(0);
        for (zigzag, luma_dc.values) |position, coefficient| dc_input[position] = coefficient;
        var dc = hadamard4(dc_input);
        for (&dc) |*coefficient| {
            const scaled = coefficient.* * factors[q % 6][0];
            coefficient.* = if (q >= 12) scaled << @as(u6, @intCast(q / 6 - 2)) else (scaled + (@as(i64, 1) << @as(u6, @intCast(1 - q / 6)))) >> @as(u6, @intCast(2 - q / 6));
        }
        for (0..16) |i| {
            const point = blockPoint(i);
            const bx = x / 4 + point[0];
            const by = y / 4 + point[1];
            var coefficients: [16]i64 = @splat(0);
            coefficients[0] = dc[point[1] * 4 + point[0]];
            if (cbp_luma) {
                const ac = try residual(&bits, context(counts[0], cfg.coded_width / 4, bx, by), 15, false);
                counts[0][by * (cfg.coded_width / 4) + bx] = ac.total;
                for (0..15) |j| coefficients[zigzag[j + 1]] = dequant(ac.values[j], q, zigzag[j + 1]);
            }
            inverseAdd(planes[0], cfg.coded_width, bx * 4, by * 4, coefficients);
        }
        const cq = chromaQp(qp, cfg.chroma_offset);
        var chroma_dc: [2][4]i64 = @splat(@splat(0));
        if (cbp_chroma != 0) for (0..2) |p| {
            const r = try residual(&bits, 0, 4, true);
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
                const ac = try residual(&bits, context(counts[p], stride / 4, bx, by), 15, false);
                counts[p][by * (stride / 4) + bx] = ac.total;
                for (0..15) |j| coefficients[zigzag[j + 1]] = dequant(ac.values[j], cq, zigzag[j + 1]);
            }
            inverseAdd(planes[p], stride, bx * 4, by * 4, coefficients);
        };
    }
    try bits.finish();
}
/// Decode one complete IDR packet. Each output is independent and owned; no
/// decoder reference state is retained across calls. Calls are portable.
pub fn decodeFrame(allocator: std.mem.Allocator, reader: *media.mp4.Reader, index: usize, options: Options) !Frame {
    try reader.input.control.check();
    if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
    if (index >= reader.packets.len) return error.InvalidPacketIndex;
    const packet = reader.packets[index];
    if (packet.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
    var budget = @import("decode_budget.zig").Budget{ .backing = allocator, .limit = options.max_decode_bytes };
    return decodeBudget(&budget, reader, index, options) catch |err| {
        return if (err == error.OutOfMemory and budget.denied) error.ResourceLimitExceeded else err;
    };
}
fn decodeBudget(budget: *@import("decode_budget.zig").Budget, reader: *media.mp4.Reader, index: usize, options: Options) !Frame {
    const allocator = budget.allocator();
    const packet = reader.packets[index];
    var config_reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = reader.track.avcc.len * 2 }) else media.admission.Token{};
    defer config_reservation.deinit();
    const cfg = try configParse(allocator, reader.track.avcc);
    config_reservation.deinit();
    if (cfg.width != reader.track.width or cfg.height != reader.track.height) return error.UnsupportedDynamicGeometry;
    const coded_pixels = try std.math.mul(usize, cfg.coded_width, cfg.coded_height);
    if (coded_pixels > options.max_pixels) return error.ResourceLimitExceeded;
    const output_size = cfg.width * cfg.height * 3 / 2;
    const planar_size = coded_pixels * 3 / 2;
    const count_size = coded_pixels * 3 / 32;
    const peak = try std.math.add(usize, try std.math.add(usize, planar_size, output_size), try std.math.add(usize, count_size, try std.math.add(usize, try std.math.mul(usize, packet.size, 2), reader.track.avcc.len * 2)));
    if (peak > options.max_decode_bytes) return error.ResourceLimitExceeded;
    var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = output_size }) else media.admission.Token{};
    errdefer reservation.deinit();
    var transient = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = peak - output_size }) else media.admission.Token{};
    defer transient.deinit();
    var lease = try reader.readPacket(index);
    defer lease.deinit();
    try @import("avc.zig").validatePacket(lease.bytes, reader.track.nal_length_bytes);
    const planar = try allocator.alloc(u8, planar_size);
    defer allocator.free(planar);
    @memset(planar, 0);
    const count_bytes = try allocator.alloc(u8, count_size);
    defer allocator.free(count_bytes);
    @memset(count_bytes, 0);
    const planes = [3][]u8{ planar[0..coded_pixels], planar[coded_pixels..][0 .. coded_pixels / 4], planar[coded_pixels + coded_pixels / 4 ..] };
    const counts = [3][]u8{ count_bytes[0 .. coded_pixels / 16], count_bytes[coded_pixels / 16 ..][0 .. coded_pixels / 64], count_bytes[coded_pixels / 16 + coded_pixels / 64 ..] };
    var cursor: usize = 0;
    var decoded = false;
    while (cursor < lease.bytes.len) {
        try reader.input.control.check();
        var size: usize = 0;
        for (lease.bytes[cursor..][0..reader.track.nal_length_bytes]) |byte| size = (size << 8) | byte;
        cursor += reader.track.nal_length_bytes;
        const nal = lease.bytes[cursor..][0..size];
        cursor += size;
        switch (nal[0] & 31) {
            5 => {
                if (decoded or nal[0] & 0x60 == 0) return error.UnsupportedVideoProfile;
                try decodeSlice(allocator, nal, cfg, planes, counts, reader.input.control);
                decoded = true;
            },
            6, 9, 12 => {},
            else => return error.UnsupportedVideoProfile,
        }
    }
    if (!decoded) return error.UnsupportedVideoProfile;
    const output = try allocator.alloc(u8, output_size);
    errdefer allocator.free(output);
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
    return .{ .reservation = reservation, .allocator = budget.backing, .nv12 = output, .width = @intCast(cfg.width), .height = @intCast(cfg.height), .pts = packet.pts, .duration = packet.duration, .timescale = reader.track.timescale, .full_range = cfg.full_range, .decode_high_water = budget.peak };
}
