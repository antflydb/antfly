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
const builtin = @import("builtin");
const test_support = @import("test_support.zig");

const Allocator = std.mem.Allocator;
const cancellation_work_bytes: usize = 64 * 1024;
const adam7_x_starts = [_]usize{ 0, 4, 0, 2, 0, 1, 0 };
const adam7_y_starts = [_]usize{ 0, 0, 4, 0, 2, 0, 1 };
const adam7_x_steps = [_]usize{ 8, 8, 4, 4, 2, 2, 1 };
const adam7_y_steps = [_]usize{ 8, 8, 8, 4, 4, 2, 2 };

pub const DecodedImage = struct {
    rgba: []u8,
    width: u32,
    height: u32,
};

/// Borrowed, allocation-free cancellation source for bounded PNG work.
pub const CancellationProbe = struct {
    context: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (?*const anyopaque) bool = null,

    pub fn check(self: CancellationProbe) !void {
        if (self.is_cancelled_fn) |is_cancelled| if (is_cancelled(self.context)) return error.Canceled;
    }
};

pub fn hasSignature(bytes: []const u8) bool {
    return bytes.len >= 8 and std.mem.eql(u8, bytes[0..8], &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' });
}

pub fn encodeRgba(alloc: Allocator, width: u32, height: u32, rgba: []const u8) ![]u8 {
    return try encodeRgbaWithCancellation(alloc, width, height, rgba, .{});
}

pub fn encodeRgbaWithCancellation(alloc: Allocator, width: u32, height: u32, rgba: []const u8, cancellation: CancellationProbe) ![]u8 {
    try cancellation.check();
    if (width == 0 or height == 0) return error.InvalidRgbaSize;
    const pixel_count = std.math.mul(usize, width, height) catch return error.InvalidRgbaSize;
    const expected = std.math.mul(usize, pixel_count, 4) catch return error.InvalidRgbaSize;
    if (rgba.len != expected) return error.InvalidRgbaSize;

    const row_bytes = std.math.mul(usize, width, 4) catch return error.InvalidRgbaSize;
    // The flate writer requires an initial output buffer (and grows it through
    // Writer.Allocating as compressed bytes are emitted).
    var compressed = try std.Io.Writer.Allocating.initCapacity(alloc, 16 * 1024);
    defer compressed.deinit();
    {
        var history: [std.compress.flate.max_window_len]u8 = undefined;
        var compressor = try std.compress.flate.Compress.init(
            &compressed.writer,
            history[0..],
            .zlib,
            .default,
        );
        for (0..height) |row| {
            try compressor.writer.writeByte(0);
            const offset = row * row_bytes;
            const row_data = rgba[offset..][0..row_bytes];
            var chunk_offset: usize = 0;
            while (chunk_offset < row_data.len) {
                try cancellation.check();
                const chunk_end = chunk_offset + @min(row_data.len - chunk_offset, cancellation_work_bytes);
                try compressor.writer.writeAll(row_data[chunk_offset..chunk_end]);
                chunk_offset = chunk_end;
            }
        }
        try cancellation.check();
        try compressor.finish();
    }
    try cancellation.check();
    const zlib = compressed.writer.buffered();
    const zlib_len = zlib.len;
    if (zlib_len > std.math.maxInt(u32)) return error.PngEncodeFailed;

    const png_len = 8 + chunkTotalLen(13) + chunkTotalLen(zlib_len) + chunkTotalLen(0);
    try cancellation.check();
    const out = try alloc.alloc(u8, png_len);
    errdefer alloc.free(out);
    var cursor: usize = 0;
    @memcpy(out[cursor .. cursor + 8], &[_]u8{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' });
    cursor += 8;

    var ihdr: [13]u8 = undefined;
    std.mem.writeInt(u32, ihdr[0..4], width, .big);
    std.mem.writeInt(u32, ihdr[4..8], height, .big);
    ihdr[8] = 8;
    ihdr[9] = 6;
    ihdr[10] = 0;
    ihdr[11] = 0;
    ihdr[12] = 0;
    appendChunkToBuffer(out, &cursor, "IHDR", &ihdr);

    writeU32be(out[cursor .. cursor + 4], @intCast(zlib_len));
    cursor += 4;
    const idat_type_start = cursor;
    @memcpy(out[cursor .. cursor + 4], "IDAT");
    cursor += 4;
    var idat_crc = crc32FastUpdate(crc32FastInit(), out[idat_type_start..cursor]);
    var zlib_offset: usize = 0;
    while (zlib_offset < zlib.len) {
        try cancellation.check();
        const chunk_end = zlib_offset + @min(zlib.len - zlib_offset, cancellation_work_bytes);
        const chunk = zlib[zlib_offset..chunk_end];
        @memcpy(out[cursor .. cursor + chunk.len], chunk);
        idat_crc = crc32FastUpdate(idat_crc, chunk);
        cursor += chunk.len;
        zlib_offset = chunk_end;
    }
    writeU32be(out[cursor .. cursor + 4], crc32FastFinal(idat_crc));
    cursor += 4;

    try cancellation.check();
    appendChunkToBuffer(out, &cursor, "IEND", &.{});
    std.debug.assert(cursor == out.len);
    return out;
}

fn chunkTotalLen(data_len: usize) usize {
    return 4 + 4 + data_len + 4;
}

fn appendChunkToBuffer(out: []u8, cursor: *usize, name: []const u8, data: []const u8) void {
    writeU32be(out[cursor.* .. cursor.* + 4], @intCast(data.len));
    cursor.* += 4;
    @memcpy(out[cursor.* .. cursor.* + 4], name);
    cursor.* += 4;
    @memcpy(out[cursor.* .. cursor.* + data.len], data);
    cursor.* += data.len;
    writeU32be(out[cursor.* .. cursor.* + 4], chunkCrc(name, data));
    cursor.* += 4;
}

fn writeU32be(dest: []u8, value: u32) void {
    dest[0] = @truncate(value >> 24);
    dest[1] = @truncate(value >> 16);
    dest[2] = @truncate(value >> 8);
    dest[3] = @truncate(value);
}

fn chunkCrc(name: []const u8, data: []const u8) u32 {
    var crc = crc32FastInit();
    crc = crc32FastUpdate(crc, name);
    crc = crc32FastUpdate(crc, data);
    return crc32FastFinal(crc);
}

fn crc32Fast(bytes: []const u8) u32 {
    return crc32FastFinal(crc32FastUpdate(crc32FastInit(), bytes));
}

fn crc32FastInit() u32 {
    return 0xffffffff;
}

fn crc32FastFinal(crc: u32) u32 {
    return crc ^ 0xffffffff;
}

fn crc32FastUpdate(initial_crc: u32, bytes: []const u8) u32 {
    if (comptime builtin.cpu.arch == .aarch64 and std.Target.aarch64.featureSetHas(builtin.cpu.features, .crc)) {
        return crc32Arm64Update(initial_crc, bytes);
    }

    const tables = comptime crc32SlicingTables();
    var crc = initial_crc;
    var index: usize = 0;

    while (index + 8 <= bytes.len) : (index += 8) {
        crc ^= readU32le(bytes[index .. index + 4]);
        const next = readU32le(bytes[index + 4 .. index + 8]);
        crc =
            tables[7][@as(u8, @truncate(crc))] ^
            tables[6][@as(u8, @truncate(crc >> 8))] ^
            tables[5][@as(u8, @truncate(crc >> 16))] ^
            tables[4][@as(u8, @truncate(crc >> 24))] ^
            tables[3][@as(u8, @truncate(next))] ^
            tables[2][@as(u8, @truncate(next >> 8))] ^
            tables[1][@as(u8, @truncate(next >> 16))] ^
            tables[0][@as(u8, @truncate(next >> 24))];
    }

    while (index < bytes.len) : (index += 1) {
        crc = tables[0][@as(u8, @truncate(crc ^ bytes[index]))] ^ (crc >> 8);
    }
    return crc;
}

fn crc32SlicingTables() [8][256]u32 {
    @setEvalBranchQuota(30000);
    const polynomial: u32 = 0xedb88320;
    var tables: [8][256]u32 = undefined;
    for (0..256) |i| {
        var crc: u32 = @intCast(i);
        for (0..8) |_| {
            crc = if ((crc & 1) != 0) (crc >> 1) ^ polynomial else crc >> 1;
        }
        tables[0][i] = crc;
    }
    for (1..8) |table_index| {
        for (0..256) |i| {
            const previous = tables[table_index - 1][i];
            tables[table_index][i] = (previous >> 8) ^ tables[0][@as(u8, @truncate(previous))];
        }
    }
    return tables;
}

fn readU32le(bytes: []const u8) u32 {
    return @as(u32, bytes[0]) |
        (@as(u32, bytes[1]) << 8) |
        (@as(u32, bytes[2]) << 16) |
        (@as(u32, bytes[3]) << 24);
}

fn readU16le(bytes: []const u8) u16 {
    return @as(u16, bytes[0]) |
        (@as(u16, bytes[1]) << 8);
}

fn readU64le(bytes: []const u8) u64 {
    return @as(u64, bytes[0]) |
        (@as(u64, bytes[1]) << 8) |
        (@as(u64, bytes[2]) << 16) |
        (@as(u64, bytes[3]) << 24) |
        (@as(u64, bytes[4]) << 32) |
        (@as(u64, bytes[5]) << 40) |
        (@as(u64, bytes[6]) << 48) |
        (@as(u64, bytes[7]) << 56);
}

fn crc32Arm64Update(initial_crc: u32, bytes: []const u8) u32 {
    var crc = initial_crc;
    var index: usize = 0;
    while (index + 8 <= bytes.len) : (index += 8) {
        crc = crc32Arm64U64(crc, readU64le(bytes[index .. index + 8]));
    }
    if (index + 4 <= bytes.len) {
        crc = crc32Arm64U32(crc, readU32le(bytes[index .. index + 4]));
        index += 4;
    }
    if (index + 2 <= bytes.len) {
        crc = crc32Arm64U16(crc, readU16le(bytes[index .. index + 2]));
        index += 2;
    }
    if (index < bytes.len) {
        crc = crc32Arm64U8(crc, bytes[index]);
    }
    return crc;
}

fn crc32Arm64U64(crc: u32, value: u64) u32 {
    return asm ("crc32x %[out:w], %[crc:w], %[value]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

fn crc32Arm64U32(crc: u32, value: u32) u32 {
    return asm ("crc32w %[out:w], %[crc:w], %[value:w]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

fn crc32Arm64U16(crc: u32, value: u16) u32 {
    return asm ("crc32h %[out:w], %[crc:w], %[value:w]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

fn crc32Arm64U8(crc: u32, value: u8) u32 {
    return asm ("crc32b %[out:w], %[crc:w], %[value:w]"
        : [out] "=r" (-> u32),
        : [crc] "r" (crc),
          [value] "r" (value),
    );
}

fn adler32FastInit() u32 {
    return 1;
}

fn adler32FastUpdate(state: u32, bytes: []const u8) u32 {
    const base = 65521;
    const nmax = 5552;
    const weights: @Vector(16, u32) = .{ 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };

    var s1 = state & 0xffff;
    var s2 = state >> 16;
    var index: usize = 0;

    while (index < bytes.len) {
        const end = @min(index + nmax, bytes.len);
        while (index + 16 <= end) : (index += 16) {
            const byte_vec: @Vector(16, u8) = bytes[index..][0..16].*;
            const lanes: @Vector(16, u32) = @intCast(byte_vec);
            const sum = @reduce(.Add, lanes);
            const weighted_sum = @reduce(.Add, lanes * weights);
            s2 += 16 * s1 + weighted_sum;
            s1 += sum;
        }
        while (index < end) : (index += 1) {
            s1 += bytes[index];
            s2 += s1;
        }
        s1 %= base;
        s2 %= base;
    }

    return s1 | (s2 << 16);
}

pub fn decodeRgba(alloc: Allocator, png_bytes: []const u8) !DecodedImage {
    const signature = &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' };
    if (png_bytes.len < signature.len or !std.mem.eql(u8, png_bytes[0..signature.len], signature)) {
        return error.PngDecodeFailed;
    }

    var width: u32 = 0;
    var height: u32 = 0;
    var bit_depth: u8 = 0;
    var color_type: u8 = 0;
    var interlace_method: u8 = 0;
    var palette: [256][4]u8 = undefined;
    @memset(&palette, .{ 0, 0, 0, 0xff });
    var palette_entries: usize = 0;
    var palette_alpha_len: usize = 0;
    var transparent_gray: ?u16 = null;
    var transparent_rgb: ?[3]u16 = null;
    var saw_ihdr = false;
    var saw_iend = false;
    var idat = std.ArrayList(u8).empty;
    defer idat.deinit(alloc);

    var offset: usize = signature.len;
    while (offset + 12 <= png_bytes.len) {
        const chunk_len = std.mem.readInt(u32, @ptrCast(png_bytes[offset .. offset + 4]), .big);
        const chunk_len_usize: usize = @intCast(chunk_len);
        offset += 4;
        if (offset + 4 + chunk_len_usize + 4 > png_bytes.len) return error.PngDecodeFailed;

        const chunk_type = png_bytes[offset .. offset + 4];
        offset += 4;
        const chunk_data = png_bytes[offset .. offset + chunk_len_usize];
        offset += chunk_len_usize;
        const chunk_crc = std.mem.readInt(u32, @ptrCast(png_bytes[offset .. offset + 4]), .big);
        offset += 4;

        if (chunkCrc(chunk_type, chunk_data) != chunk_crc) return error.PngDecodeFailed;

        if (std.mem.eql(u8, chunk_type, "IHDR")) {
            if (saw_ihdr or chunk_data.len != 13) return error.PngDecodeFailed;
            saw_ihdr = true;
            width = std.mem.readInt(u32, @ptrCast(chunk_data[0..4]), .big);
            height = std.mem.readInt(u32, @ptrCast(chunk_data[4..8]), .big);
            bit_depth = chunk_data[8];
            color_type = chunk_data[9];
            if (chunk_data[10] != 0 or chunk_data[11] != 0) return error.PngDecodeFailed;
            interlace_method = chunk_data[12];
            continue;
        }

        if (!saw_ihdr) return error.PngDecodeFailed;

        if (std.mem.eql(u8, chunk_type, "IDAT")) {
            try idat.appendSlice(alloc, chunk_data);
            continue;
        }
        if (std.mem.eql(u8, chunk_type, "PLTE")) {
            if (chunk_data.len == 0 or (chunk_data.len % 3) != 0 or chunk_data.len > 256 * 3) return error.PngDecodeFailed;
            palette_entries = chunk_data.len / 3;
            for (0..palette_entries) |i| {
                const base = i * 3;
                palette[i][0] = chunk_data[base + 0];
                palette[i][1] = chunk_data[base + 1];
                palette[i][2] = chunk_data[base + 2];
                palette[i][3] = if (i < palette_alpha_len) palette[i][3] else 0xff;
            }
            continue;
        }
        if (std.mem.eql(u8, chunk_type, "tRNS")) {
            switch (color_type) {
                0 => {
                    if (chunk_data.len != 2) return error.PngDecodeFailed;
                    transparent_gray = if (bit_depth == 16)
                        std.mem.readInt(u16, @ptrCast(chunk_data[0..2]), .big)
                    else
                        chunk_data[1];
                },
                2 => {
                    if (chunk_data.len != 6) return error.PngDecodeFailed;
                    transparent_rgb = .{
                        if (bit_depth == 16) std.mem.readInt(u16, @ptrCast(chunk_data[0..2]), .big) else chunk_data[1],
                        if (bit_depth == 16) std.mem.readInt(u16, @ptrCast(chunk_data[2..4]), .big) else chunk_data[3],
                        if (bit_depth == 16) std.mem.readInt(u16, @ptrCast(chunk_data[4..6]), .big) else chunk_data[5],
                    };
                },
                3 => {
                    if (chunk_data.len > 256) return error.PngDecodeFailed;
                    palette_alpha_len = chunk_data.len;
                    for (chunk_data, 0..) |alpha, i| palette[i][3] = alpha;
                    for (chunk_data.len..256) |i| {
                        if (i >= palette_entries) palette[i][3] = 0xff;
                    }
                },
                else => return error.UnsupportedPngFormat,
            }
            continue;
        }
        if (std.mem.eql(u8, chunk_type, "IEND")) {
            saw_iend = true;
            break;
        }
    }

    if (!saw_ihdr or !saw_iend or width == 0 or height == 0 or idat.items.len == 0) return error.PngDecodeFailed;
    try validateBitDepth(color_type, bit_depth);
    if (interlace_method > 1) return error.PngDecodeFailed;

    const channels: usize = switch (color_type) {
        0 => 1,
        3 => 1,
        4 => 2,
        2 => 3,
        6 => 4,
        else => return error.UnsupportedPngFormat,
    };
    if (color_type == 3 and palette_entries == 0) return error.PngDecodeFailed;
    if (color_type == 3 and palette_alpha_len > palette_entries) return error.PngDecodeFailed;
    const stride = try rowByteLen(width, channels, bit_depth);
    const filter_bpp = filterBytesPerPixel(channels, bit_depth);
    const expected_raw_len = if (interlace_method == 0)
        (@as(usize, height) * (stride + 1))
    else
        try adam7ExpectedRawLen(width, height, channels, bit_depth);
    const raw = try alloc.alloc(u8, expected_raw_len);
    defer alloc.free(raw);

    var in: std.Io.Reader = .fixed(idat.items);
    var fw: std.Io.Writer = .fixed(raw);
    var decompress: std.compress.flate.Decompress = .init(&in, .zlib, &.{});
    const decompressed_len = decompress.reader.streamRemaining(&fw) catch return error.PngDecodeFailed;
    if (decompressed_len != expected_raw_len) return error.PngDecodeFailed;

    const rgba = try alloc.alloc(u8, @as(usize, width) * @as(usize, height) * 4);
    errdefer alloc.free(rgba);
    if (interlace_method == 0) {
        try decodeNonInterlacedRgba(alloc, rgba, raw, width, height, color_type, bit_depth, channels, stride, filter_bpp, palette[0..palette_entries], transparent_gray, transparent_rgb);
    } else {
        try decodeAdam7Rgba(alloc, rgba, raw, width, height, color_type, bit_depth, channels, filter_bpp, palette[0..palette_entries], transparent_gray, transparent_rgb);
    }

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn validateBitDepth(color_type: u8, bit_depth: u8) !void {
    switch (color_type) {
        0 => switch (bit_depth) {
            1, 2, 4, 8, 16 => {},
            else => return error.UnsupportedPngFormat,
        },
        3 => switch (bit_depth) {
            1, 2, 4, 8 => {},
            else => return error.UnsupportedPngFormat,
        },
        2, 4, 6 => switch (bit_depth) {
            8, 16 => {},
            else => return error.UnsupportedPngFormat,
        },
        else => return error.UnsupportedPngFormat,
    }
}

fn rowByteLen(width: u32, channels: usize, bit_depth: u8) !usize {
    const total_bits = try std.math.mul(usize, @as(usize, width), try std.math.mul(usize, channels, bit_depth));
    return (total_bits + 7) / 8;
}

fn filterBytesPerPixel(channels: usize, bit_depth: u8) usize {
    return @max(1, (channels * bit_depth + 7) / 8);
}

fn adam7ExpectedRawLen(width: u32, height: u32, channels: usize, bit_depth: u8) !usize {
    var total: usize = 0;
    for (0..adam7_x_starts.len) |pass| {
        const pass_width = adam7PassSpan(width, adam7_x_starts[pass], adam7_x_steps[pass]);
        const pass_height = adam7PassSpan(height, adam7_y_starts[pass], adam7_y_steps[pass]);
        if (pass_width == 0 or pass_height == 0) continue;
        const row_bytes = try rowByteLen(@intCast(pass_width), channels, bit_depth);
        total = try std.math.add(usize, total, try std.math.mul(usize, pass_height, row_bytes + 1));
    }
    return total;
}

fn adam7PassSpan(full: u32, start: usize, step: usize) usize {
    const full_usize: usize = @intCast(full);
    if (full_usize <= start) return 0;
    return 1 + ((full_usize - 1 - start) / step);
}

fn decodeNonInterlacedRgba(
    alloc: Allocator,
    rgba: []u8,
    raw: []const u8,
    width: u32,
    height: u32,
    color_type: u8,
    bit_depth: u8,
    channels: usize,
    stride: usize,
    filter_bpp: usize,
    palette: []const [4]u8,
    transparent_gray: ?u16,
    transparent_rgb: ?[3]u16,
) !void {
    const row_a = try alloc.alloc(u8, stride);
    defer alloc.free(row_a);
    const row_b = try alloc.alloc(u8, stride);
    defer alloc.free(row_b);

    var prev_row = row_a;
    var curr_row = row_b;
    @memset(prev_row, 0);

    for (0..@as(usize, height)) |row| {
        const raw_start = row * (stride + 1);
        const filter_type = raw[raw_start];
        const filtered = raw[raw_start + 1 .. raw_start + 1 + stride];
        try unfilterRow(curr_row, filtered, prev_row, filter_type, filter_bpp);
        const out_row = rgba[row * @as(usize, width) * 4 ..][0 .. @as(usize, width) * 4];
        try writeDecodedRowRgba(out_row, curr_row, @intCast(width), color_type, bit_depth, channels, palette, transparent_gray, transparent_rgb);
        const decoded_row = curr_row;
        curr_row = prev_row;
        prev_row = decoded_row;
    }
}

fn decodeAdam7Rgba(
    alloc: Allocator,
    rgba: []u8,
    raw: []const u8,
    width: u32,
    height: u32,
    color_type: u8,
    bit_depth: u8,
    channels: usize,
    filter_bpp: usize,
    palette: []const [4]u8,
    transparent_gray: ?u16,
    transparent_rgb: ?[3]u16,
) !void {
    @memset(rgba, 0);

    var raw_offset: usize = 0;
    for (0..adam7_x_starts.len) |pass| {
        const pass_width = adam7PassSpan(width, adam7_x_starts[pass], adam7_x_steps[pass]);
        const pass_height = adam7PassSpan(height, adam7_y_starts[pass], adam7_y_steps[pass]);
        if (pass_width == 0 or pass_height == 0) continue;

        const pass_stride = try rowByteLen(@intCast(pass_width), channels, bit_depth);
        const row_a = try alloc.alloc(u8, pass_stride);
        defer alloc.free(row_a);
        const row_b = try alloc.alloc(u8, pass_stride);
        defer alloc.free(row_b);

        var prev_row = row_a;
        var curr_row = row_b;
        @memset(prev_row, 0);

        const row_rgba = try alloc.alloc(u8, pass_width * 4);
        defer alloc.free(row_rgba);

        for (0..pass_height) |row| {
            if (raw_offset + 1 + pass_stride > raw.len) return error.PngDecodeFailed;
            const filter_type = raw[raw_offset];
            raw_offset += 1;
            const filtered = raw[raw_offset .. raw_offset + pass_stride];
            raw_offset += pass_stride;

            try unfilterRow(curr_row, filtered, prev_row, filter_type, filter_bpp);
            try writeDecodedRowRgba(row_rgba, curr_row, pass_width, color_type, bit_depth, channels, palette, transparent_gray, transparent_rgb);

            const dst_y = adam7_y_starts[pass] + row * adam7_y_steps[pass];
            if (dst_y >= height) return error.PngDecodeFailed;

            for (0..pass_width) |col| {
                const dst_x = adam7_x_starts[pass] + col * adam7_x_steps[pass];
                if (dst_x >= width) return error.PngDecodeFailed;
                const src = col * 4;
                const dst = ((dst_y * @as(usize, width)) + dst_x) * 4;
                @memcpy(rgba[dst .. dst + 4], row_rgba[src .. src + 4]);
            }

            const decoded_row = curr_row;
            curr_row = prev_row;
            prev_row = decoded_row;
        }
    }

    if (raw_offset != raw.len) return error.PngDecodeFailed;
}

fn unfilterRow(out: []u8, filtered: []const u8, prev: []const u8, filter_type: u8, bytes_per_pixel: usize) !void {
    if (out.len != filtered.len or prev.len != filtered.len) return error.PngDecodeFailed;

    switch (filter_type) {
        0 => {
            @memcpy(out, filtered);
        },
        1 => {
            for (filtered, 0..) |byte, i| {
                const left: u8 = if (i >= bytes_per_pixel) out[i - bytes_per_pixel] else 0;
                out[i] = byte +% left;
            }
        },
        2 => {
            addRowsModulo(out, filtered, prev);
        },
        3 => {
            for (filtered, 0..) |byte, i| {
                const left: u8 = if (i >= bytes_per_pixel) out[i - bytes_per_pixel] else 0;
                const up: u8 = prev[i];
                out[i] = byte +% @as(u8, @truncate((@as(u16, left) + @as(u16, up)) / 2));
            }
        },
        4 => {
            for (filtered, 0..) |byte, i| {
                const left: u8 = if (i >= bytes_per_pixel) out[i - bytes_per_pixel] else 0;
                const up: u8 = prev[i];
                const up_left: u8 = if (i >= bytes_per_pixel) prev[i - bytes_per_pixel] else 0;
                out[i] = byte +% paethPredictor(left, up, up_left);
            }
        },
        else => return error.UnsupportedPngFormat,
    }
}

fn addRowsModulo(out: []u8, a: []const u8, b: []const u8) void {
    const lane_count = 32;
    const Vec = @Vector(lane_count, u8);

    var i: usize = 0;
    while (i + lane_count <= a.len) : (i += lane_count) {
        const av: Vec = a[i..][0..lane_count].*;
        const bv: Vec = b[i..][0..lane_count].*;
        const sum: [lane_count]u8 = av +% bv;
        out[i..][0..lane_count].* = sum;
    }
    while (i < a.len) : (i += 1) {
        out[i] = a[i] +% b[i];
    }
}

fn paethPredictor(left: u8, up: u8, up_left: u8) u8 {
    const p = @as(i32, left) + @as(i32, up) - @as(i32, up_left);
    const pa = @abs(p - @as(i32, left));
    const pb = @abs(p - @as(i32, up));
    const pc = @abs(p - @as(i32, up_left));
    if (pa <= pb and pa <= pc) return left;
    if (pb <= pc) return up;
    return up_left;
}

fn writeRgbaRow(dst: []u8, row: []const u8, width: usize, bit_depth: u8, channels: usize, transparent_rgb: ?[3]u16) void {
    for (0..width) |x| {
        const src = x * channels * bytesPerSample(bit_depth);
        const out = x * 4;
        const r = directSampleRaw(row, src, bit_depth);
        const g = directSampleRaw(row, src + bytesPerSample(bit_depth), bit_depth);
        const b = directSampleRaw(row, src + bytesPerSample(bit_depth) * 2, bit_depth);
        dst[out + 0] = directSampleTo8(r, bit_depth);
        dst[out + 1] = directSampleTo8(g, bit_depth);
        dst[out + 2] = directSampleTo8(b, bit_depth);
        dst[out + 3] = if (channels == 4)
            directSampleTo8(directSampleRaw(row, src + bytesPerSample(bit_depth) * 3, bit_depth), bit_depth)
        else if (transparent_rgb) |key|
            if (r == key[0] and g == key[1] and b == key[2]) 0 else 0xff
        else
            0xff;
    }
}

fn writeDecodedRowRgba(
    dst: []u8,
    row: []const u8,
    width: usize,
    color_type: u8,
    bit_depth: u8,
    channels: usize,
    palette: []const [4]u8,
    transparent_gray: ?u16,
    transparent_rgb: ?[3]u16,
) !void {
    if (color_type == 6 and bit_depth == 8) {
        @memcpy(dst, row[0 .. width * 4]);
        return;
    }
    if (color_type == 2 and bit_depth == 8 and transparent_rgb == null) {
        writeRgb8OpaqueRow(dst, row, width);
        return;
    }

    switch (color_type) {
        0 => try writeGrayRgbaRow(dst, row, width, bit_depth, transparent_gray),
        3 => try writePaletteRgbaRow(dst, row, width, bit_depth, palette),
        4 => writeGrayAlphaRgbaRow(dst, row, width, bit_depth),
        else => writeRgbaRow(dst, row, width, bit_depth, channels, transparent_rgb),
    }
}

fn bytesPerSample(bit_depth: u8) usize {
    return if (bit_depth == 16) 2 else 1;
}

fn sample16To8(sample: u16) u8 {
    return @intCast(@min((@as(u32, sample) + 128) >> 8, 255));
}

fn directSampleTo8(sample: u16, bit_depth: u8) u8 {
    return switch (bit_depth) {
        8 => @as(u8, @truncate(sample)),
        16 => sample16To8(sample),
        else => unreachable,
    };
}

fn writeRgb8OpaqueRow(dst: []u8, row: []const u8, width: usize) void {
    for (0..width) |x| {
        const src = x * 3;
        const out = x * 4;
        dst[out + 0] = row[src + 0];
        dst[out + 1] = row[src + 1];
        dst[out + 2] = row[src + 2];
        dst[out + 3] = 0xff;
    }
}

fn expandSample(sample: u8, bit_depth: u8) u8 {
    return switch (bit_depth) {
        1 => if (sample == 0) 0 else 0xff,
        2 => sample * 0x55,
        4 => sample * 0x11,
        8 => sample,
        else => unreachable,
    };
}

fn packedSample(row: []const u8, x: usize, bit_depth: u8) u8 {
    if (bit_depth == 8) return row[x];

    const bit_index = x * bit_depth;
    const byte = row[bit_index / 8];
    const shift: u3 = @intCast(8 - bit_depth - @as(u8, @intCast(bit_index % 8)));
    const mask: u8 = (@as(u8, 1) << @intCast(bit_depth)) - 1;
    return (byte >> shift) & mask;
}

fn directSampleRaw(row: []const u8, offset: usize, bit_depth: u8) u16 {
    return switch (bit_depth) {
        8 => row[offset],
        16 => std.mem.readInt(u16, @ptrCast(row[offset .. offset + 2]), .big),
        else => unreachable,
    };
}

fn graySample16(row: []const u8, x: usize, bit_depth: u8) u16 {
    return switch (bit_depth) {
        1, 2, 4 => @as(u16, packedSample(row, x, bit_depth)),
        8 => row[x],
        16 => std.mem.readInt(u16, @ptrCast(row[x * 2 .. x * 2 + 2]), .big),
        else => unreachable,
    };
}

fn writeGrayRgbaRow(dst: []u8, row: []const u8, width: usize, bit_depth: u8, transparent_gray: ?u16) !void {
    for (0..width) |x| {
        const sample = graySample16(row, x, bit_depth);
        const gray = switch (bit_depth) {
            1, 2, 4 => expandSample(@truncate(sample), bit_depth),
            8 => @as(u8, @truncate(sample)),
            16 => sample16To8(sample),
            else => unreachable,
        };
        const out = x * 4;
        dst[out + 0] = gray;
        dst[out + 1] = gray;
        dst[out + 2] = gray;
        dst[out + 3] = if (transparent_gray != null and sample == transparent_gray.?) 0 else 0xff;
    }
}

fn writeGrayAlphaRgbaRow(dst: []u8, row: []const u8, width: usize, bit_depth: u8) void {
    for (0..width) |x| {
        const src = x * 2 * bytesPerSample(bit_depth);
        const gray = directSampleTo8(directSampleRaw(row, src, bit_depth), bit_depth);
        const alpha = directSampleTo8(directSampleRaw(row, src + bytesPerSample(bit_depth), bit_depth), bit_depth);
        const out = x * 4;
        dst[out + 0] = gray;
        dst[out + 1] = gray;
        dst[out + 2] = gray;
        dst[out + 3] = alpha;
    }
}

fn writePaletteRgbaRow(dst: []u8, row: []const u8, width: usize, bit_depth: u8, palette: []const [4]u8) !void {
    for (0..width) |x| {
        const index = packedSample(row, x, bit_depth);
        if (index >= palette.len) return error.PngDecodeFailed;
        const out = x * 4;
        @memcpy(dst[out .. out + 4], &palette[index]);
    }
}

test "encode rgba writes png signature" {
    const alloc = std.testing.allocator;
    const rgba = [_]u8{
        0xff, 0,    0,    0xff,
        0,    0xff, 0,    0xff,
        0,    0,    0xff, 0xff,
        0xff, 0xff, 0xff, 0xff,
    };
    const png = try encodeRgba(alloc, 2, 2, &rgba);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
    const decoded = try decodeRgba(alloc, png);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 2), decoded.height);
    try std.testing.expectEqualSlices(u8, &rgba, decoded.rgba);
}

test "encode rgba cancellation bounds compression and IDAT work" {
    const CancelAtCheck = struct {
        checks: usize = 0,
        cancel_at: usize,

        fn isCancelled(context: ?*const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(context.?)));
            self.checks += 1;
            return self.checks >= self.cancel_at;
        }

        fn probe(self: *@This()) CancellationProbe {
            return .{ .context = self, .is_cancelled_fn = isCancelled };
        }
    };

    // A single scanline wider than one work chunk must remain interruptible.
    const wide_rgba = [_]u8{0x5a} ** (20_000 * 4);
    var during_compression = CancelAtCheck{ .cancel_at = 3 };
    try std.testing.expectError(
        error.Canceled,
        encodeRgbaWithCancellation(std.testing.allocator, 20_000, 1, &wide_rgba, during_compression.probe()),
    );
    try std.testing.expectEqual(@as(usize, 3), during_compression.checks);

    // Cancellation remains active after Deflate while IDAT is copied and its
    // checksum is accumulated. The testing allocator verifies output cleanup.
    const small_rgba = [_]u8{0xa5} ** (8 * 8 * 4);
    var during_idat = CancelAtCheck{ .cancel_at = 13 };
    try std.testing.expectError(
        error.Canceled,
        encodeRgbaWithCancellation(std.testing.allocator, 8, 8, &small_rgba, during_idat.probe()),
    );
    try std.testing.expectEqual(@as(usize, 13), during_idat.checks);
}

test "encode rgba compresses repetitive pages" {
    const alloc = std.testing.allocator;
    const rgba = try alloc.alloc(u8, 256 * 256 * 4);
    defer alloc.free(rgba);
    @memset(rgba, 0xff);
    const png = try encodeRgba(alloc, 256, 256, rgba);
    defer alloc.free(png);
    try std.testing.expect(png.len < rgba.len / 16);
    const decoded = try decodeRgba(alloc, png);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqualSlices(u8, rgba, decoded.rgba);
}

test "png crc32 fast path matches std crc32" {
    const bytes = "IHDRabcdefghijklmnopqrstuvwxyz0123456789";
    try std.testing.expectEqual(std.hash.Crc32.hash(bytes), crc32Fast(bytes));
    var crc = crc32FastInit();
    crc = crc32FastUpdate(crc, bytes[0..4]);
    crc = crc32FastUpdate(crc, bytes[4..]);
    try std.testing.expectEqual(std.hash.Crc32.hash(bytes), crc32FastFinal(crc));
}

test "png adler32 fast path matches std adler32" {
    const bytes = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789";
    try std.testing.expectEqual(std.hash.Adler32.hash(bytes), adler32FastUpdate(adler32FastInit(), bytes));

    var chunked = adler32FastInit();
    chunked = adler32FastUpdate(chunked, bytes[0..7]);
    chunked = adler32FastUpdate(chunked, bytes[7..31]);
    chunked = adler32FastUpdate(chunked, bytes[31..]);
    try std.testing.expectEqual(std.hash.Adler32.hash(bytes), chunked);

    const long = [_]u8{0xf3} ** 7000;
    try std.testing.expectEqual(std.hash.Adler32.hash(&long), adler32FastUpdate(adler32FastInit(), &long));
}

test "decode rgba matches manifest-backed red fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/basic/red-2x2.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed alpha png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/basic/red-alpha-2x2.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed grayscale png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/grayscale/test-gray-4x4.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed grayscale alpha png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/grayscale/test-graya-2x2.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed grayscale trns png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/trns/transparent-black-gray-2x2.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed truecolor trns png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/trns/transparent-red-rgb-2x2.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed low-bit-depth grayscale png fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const paths = [_][]const u8{
        "png/lowbit/gray1-4x2.png",
        "png/lowbit/gray2-4x2.png",
        "png/lowbit/gray4-4x2.png",
    };

    for (paths) |path| {
        const fixture = test_support.findFixture(manifest, path) orelse return error.MissingImageFixture;
        try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
        try std.testing.expectEqualStrings("png", fixture.format);
        try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);

        try std.testing.expectEqual(fixture.width.?, decoded.width);
        try std.testing.expectEqual(fixture.height.?, decoded.height);

        const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
    }
}

test "decode rgba matches manifest-backed low-bit-depth indexed png fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const paths = [_][]const u8{
        "png/lowbit/palette1-4x2.png",
        "png/lowbit/palette2-4x2.png",
        "png/lowbit/palette4-4x2.png",
    };

    for (paths) |path| {
        const fixture = test_support.findFixture(manifest, path) orelse return error.MissingImageFixture;
        try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
        try std.testing.expectEqualStrings("png", fixture.format);
        try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);

        try std.testing.expectEqual(fixture.width.?, decoded.width);
        try std.testing.expectEqual(fixture.height.?, decoded.height);

        const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
    }
}

test "decode rgba matches manifest-backed high-bit-depth grayscale png fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const paths = [_][]const u8{
        "png/highbit/gray16-2x2.png",
        "png/highbit/gray16-trns-2x2.png",
        "png/highbit/graya16-2x2.png",
    };

    for (paths) |path| {
        const fixture = test_support.findFixture(manifest, path) orelse return error.MissingImageFixture;
        try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
        try std.testing.expectEqualStrings("png", fixture.format);
        try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);

        try std.testing.expectEqual(fixture.width.?, decoded.width);
        try std.testing.expectEqual(fixture.height.?, decoded.height);

        const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
    }
}

test "decode rgba matches manifest-backed high-bit-depth color png fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const paths = [_][]const u8{
        "png/highbit/rgb16-2x2.png",
        "png/highbit/rgb16-trns-2x2.png",
        "png/highbit/rgba16-2x2.png",
    };

    for (paths) |path| {
        const fixture = test_support.findFixture(manifest, path) orelse return error.MissingImageFixture;
        try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
        try std.testing.expectEqualStrings("png", fixture.format);
        try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        const decoded = try decodeRgba(alloc, fixture_bytes);
        defer alloc.free(decoded.rgba);

        try std.testing.expectEqual(fixture.width.?, decoded.width);
        try std.testing.expectEqual(fixture.height.?, decoded.height);

        const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
    }
}

test "decode rgba matches manifest-backed indexed png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/unsupported/testsrc-pal8-4x4.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed indexed trns png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/palette/transparent-red-pal8-trns-2x2.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed adam7 rgba png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/interlaced/pattern-4x4-rgba-adam7.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed ancillary text png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/valid_weird/red-2x2-text.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed ancillary gamma png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/valid_weird/red-2x2-gama.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba matches manifest-backed ancillary srgb png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/valid_weird/red-2x2-srgb.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);
    try std.testing.expectEqualStrings("rgba8", fixture.pixel_format.?);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const decoded = try decodeRgba(alloc, fixture_bytes);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(fixture.width.?, decoded.width);
    try std.testing.expectEqual(fixture.height.?, decoded.height);

    const actual_hex = try test_support.sha256HexAlloc(alloc, decoded.rgba);
    defer alloc.free(actual_hex);
    try std.testing.expectEqualStrings(fixture.pixel_hashes[0], actual_hex);
}

test "decode rgba rejects manifest-backed bad crc png fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "png/invalid/bad-crc-red-2x2.png") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("png", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.PngDecodeFailed, decodeRgba(alloc, fixture_bytes));
}

test "decode rgba rejects manifest-backed bad trns png fixtures" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const paths = [_][]const u8{
        "png/invalid/bad-trns-gray-length.png",
        "png/invalid/bad-trns-rgb-length.png",
    };

    for (paths) |path| {
        const fixture = test_support.findFixture(manifest, path) orelse return error.MissingImageFixture;
        try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
        try std.testing.expectEqualStrings("png", fixture.format);

        const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
        defer alloc.free(fixture_bytes);

        try std.testing.expectError(error.PngDecodeFailed, decodeRgba(alloc, fixture_bytes));
    }
}
