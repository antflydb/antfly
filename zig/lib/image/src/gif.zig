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
const test_support = @import("test_support.zig");

const Allocator = std.mem.Allocator;

pub fn hasSignature(bytes: []const u8) bool {
    return bytes.len >= 6 and (std.mem.eql(u8, bytes[0..6], "GIF87a") or std.mem.eql(u8, bytes[0..6], "GIF89a"));
}

pub const Frame = struct {
    rgba: []u8,
    width: u32,
    height: u32,
    delay_ms: u32,
};

const GraphicControl = struct {
    disposal_method: u3 = 0,
    delay_ms: u32 = 0,
    transparent_index: ?u8 = null,
};

const ImageDescriptor = struct {
    left: u16,
    top: u16,
    width: u16,
    height: u16,
    local_color_table_flag: bool,
    interlaced: bool,
    local_color_table_size: usize,
};

const Parser = struct {
    bytes: []const u8,
    offset: usize = 0,

    fn readByte(self: *Parser) !u8 {
        if (self.offset >= self.bytes.len) return error.GifDecodeFailed;
        defer self.offset += 1;
        return self.bytes[self.offset];
    }

    fn readU16(self: *Parser) !u16 {
        if (self.offset + 2 > self.bytes.len) return error.GifDecodeFailed;
        defer self.offset += 2;
        return std.mem.readInt(u16, @ptrCast(self.bytes[self.offset .. self.offset + 2]), .little);
    }

    fn readSlice(self: *Parser, len: usize) ![]const u8 {
        if (self.offset + len > self.bytes.len) return error.GifDecodeFailed;
        defer self.offset += len;
        return self.bytes[self.offset .. self.offset + len];
    }

    fn readSubBlocksAlloc(self: *Parser, alloc: Allocator) ![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(alloc);

        while (true) {
            const block_len = try self.readByte();
            if (block_len == 0) break;
            const data = try self.readSlice(block_len);
            try out.appendSlice(alloc, data);
        }
        return try out.toOwnedSlice(alloc);
    }

    fn skipSubBlocks(self: *Parser) !void {
        while (true) {
            const block_len = try self.readByte();
            if (block_len == 0) break;
            _ = try self.readSlice(block_len);
        }
    }
};

pub fn decodeFramesAlloc(alloc: Allocator, gif_bytes: []const u8) ![]Frame {
    var parser = Parser{ .bytes = gif_bytes };

    const header = try parser.readSlice(6);
    if (!std.mem.eql(u8, header, "GIF87a") and !std.mem.eql(u8, header, "GIF89a")) return error.GifDecodeFailed;

    const screen_width = try parser.readU16();
    const screen_height = try parser.readU16();
    if (screen_width == 0 or screen_height == 0) return error.GifDecodeFailed;

    const packed_fields = try parser.readByte();
    const global_color_table_flag = (packed_fields & 0x80) != 0;
    const global_color_table_size: usize = if (global_color_table_flag) @as(usize, 1) << @intCast((packed_fields & 0x07) + 1) else 0;
    const background_color_index = try parser.readByte();
    _ = try parser.readByte(); // pixel aspect ratio

    var global_palette: [256][4]u8 = undefined;
    var has_global_palette = false;
    if (global_color_table_flag) {
        try parseColorTable(&parser, &global_palette, global_color_table_size, null);
        has_global_palette = true;
    }

    var background_rgba = [_]u8{ 0, 0, 0, 0 };
    if (has_global_palette and background_color_index < global_color_table_size) {
        background_rgba = global_palette[background_color_index];
    }

    const canvas_len = @as(usize, screen_width) * @as(usize, screen_height) * 4;
    const canvas = try alloc.alloc(u8, canvas_len);
    defer alloc.free(canvas);
    fillCanvas(canvas, background_rgba);

    const previous_canvas = try alloc.alloc(u8, canvas_len);
    defer alloc.free(previous_canvas);

    var frames = std.ArrayList(Frame).empty;
    errdefer {
        for (frames.items) |frame| alloc.free(frame.rgba);
        frames.deinit(alloc);
    }

    var gce = GraphicControl{};
    while (true) {
        const sentinel = try parser.readByte();

        switch (sentinel) {
            0x3B => break, // trailer
            0x21 => {
                const label = try parser.readByte();
                switch (label) {
                    0xF9 => {
                        const block_size = try parser.readByte();
                        if (block_size != 4) return error.GifDecodeFailed;
                        const gce_packed = try parser.readByte();
                        const delay_cs = try parser.readU16();
                        const transparent_index = try parser.readByte();
                        const terminator = try parser.readByte();
                        if (terminator != 0) return error.GifDecodeFailed;
                        gce = .{
                            .disposal_method = @truncate((gce_packed >> 2) & 0x7),
                            .delay_ms = @as(u32, delay_cs) * 10,
                            .transparent_index = if ((gce_packed & 0x1) != 0) transparent_index else null,
                        };
                    },
                    else => try parser.skipSubBlocks(),
                }
            },
            0x2C => {
                const descriptor = try readImageDescriptor(&parser);
                if (@as(u32, descriptor.left) + @as(u32, descriptor.width) > screen_width) return error.GifDecodeFailed;
                if (@as(u32, descriptor.top) + @as(u32, descriptor.height) > screen_height) return error.GifDecodeFailed;

                var palette: [256][4]u8 = undefined;
                if (descriptor.local_color_table_flag) {
                    try parseColorTable(&parser, &palette, descriptor.local_color_table_size, gce.transparent_index);
                } else if (has_global_palette) {
                    palette = global_palette;
                    if (gce.transparent_index) |idx| palette[idx][3] = 0;
                } else {
                    return error.GifDecodeFailed;
                }

                const min_code_size = try parser.readByte();
                const compressed = try parser.readSubBlocksAlloc(alloc);
                defer alloc.free(compressed);

                const pixel_count = @as(usize, descriptor.width) * @as(usize, descriptor.height);
                const indices = try decodeImageIndicesAlloc(alloc, compressed, min_code_size, pixel_count);
                defer alloc.free(indices);

                @memcpy(previous_canvas, canvas);
                try drawFrame(canvas, screen_width, screen_height, descriptor, indices, &palette);

                const rgba = try alloc.dupe(u8, canvas);
                try frames.append(alloc, .{
                    .rgba = rgba,
                    .width = screen_width,
                    .height = screen_height,
                    .delay_ms = gce.delay_ms,
                });

                try applyDisposal(canvas, previous_canvas, screen_width, screen_height, descriptor, gce, background_rgba);
                gce = .{};
            },
            else => return error.GifDecodeFailed,
        }
    }

    return try frames.toOwnedSlice(alloc);
}

fn readImageDescriptor(parser: *Parser) !ImageDescriptor {
    const left = try parser.readU16();
    const top = try parser.readU16();
    const width = try parser.readU16();
    const height = try parser.readU16();
    if (width == 0 or height == 0) return error.GifDecodeFailed;

    const packed_fields = try parser.readByte();
    return .{
        .left = left,
        .top = top,
        .width = width,
        .height = height,
        .local_color_table_flag = (packed_fields & 0x80) != 0,
        .interlaced = (packed_fields & 0x40) != 0,
        .local_color_table_size = if ((packed_fields & 0x80) != 0) @as(usize, 1) << @intCast((packed_fields & 0x07) + 1) else 0,
    };
}

fn parseColorTable(parser: *Parser, palette: *[256][4]u8, size: usize, transparent_index: ?u8) !void {
    if (size == 0 or size > 256) return error.GifDecodeFailed;
    for (0..size) |i| {
        const rgb = try parser.readSlice(3);
        palette[i] = .{
            rgb[0],
            rgb[1],
            rgb[2],
            if (transparent_index != null and transparent_index.? == i) 0 else 0xff,
        };
    }
    for (size..256) |i| palette[i] = .{ 0, 0, 0, 0xff };
}

fn drawFrame(
    canvas: []u8,
    screen_width: u16,
    screen_height: u16,
    descriptor: ImageDescriptor,
    indices: []const u8,
    palette: *const [256][4]u8,
) !void {
    const frame_width = @as(usize, descriptor.width);
    const frame_height = @as(usize, descriptor.height);
    if (indices.len != frame_width * frame_height) return error.GifDecodeFailed;

    var src_index: usize = 0;
    if (!descriptor.interlaced) {
        for (0..frame_height) |row| {
            for (0..frame_width) |col| {
                const color = palette[indices[src_index]];
                src_index += 1;
                if (color[3] == 0) continue;
                const dst_x = @as(usize, descriptor.left) + col;
                const dst_y = @as(usize, descriptor.top) + row;
                if (dst_x >= screen_width or dst_y >= screen_height) return error.GifDecodeFailed;
                const dst = (dst_y * @as(usize, screen_width) + dst_x) * 4;
                @memcpy(canvas[dst .. dst + 4], &color);
            }
        }
        return;
    }

    const starts = [_]usize{ 0, 4, 2, 1 };
    const steps = [_]usize{ 8, 8, 4, 2 };
    for (starts, steps) |start, step| {
        var row = start;
        while (row < frame_height) : (row += step) {
            for (0..frame_width) |col| {
                const color = palette[indices[src_index]];
                src_index += 1;
                if (color[3] == 0) continue;
                const dst_x = @as(usize, descriptor.left) + col;
                const dst_y = @as(usize, descriptor.top) + row;
                if (dst_x >= screen_width or dst_y >= screen_height) return error.GifDecodeFailed;
                const dst = (dst_y * @as(usize, screen_width) + dst_x) * 4;
                @memcpy(canvas[dst .. dst + 4], &color);
            }
        }
    }
}

fn applyDisposal(
    canvas: []u8,
    previous_canvas: []const u8,
    screen_width: u16,
    screen_height: u16,
    descriptor: ImageDescriptor,
    gce: GraphicControl,
    background_rgba: [4]u8,
) !void {
    switch (gce.disposal_method) {
        0, 1 => return,
        2 => {
            const frame_width = @as(usize, descriptor.width);
            const frame_height = @as(usize, descriptor.height);
            for (0..frame_height) |row| {
                const dst_y = @as(usize, descriptor.top) + row;
                if (dst_y >= screen_height) return error.GifDecodeFailed;
                for (0..frame_width) |col| {
                    const dst_x = @as(usize, descriptor.left) + col;
                    if (dst_x >= screen_width) return error.GifDecodeFailed;
                    const dst = (dst_y * @as(usize, screen_width) + dst_x) * 4;
                    @memcpy(canvas[dst .. dst + 4], &background_rgba);
                }
            }
        },
        3 => @memcpy(canvas, previous_canvas),
        else => return error.UnsupportedGifFormat,
    }
}

fn fillCanvas(canvas: []u8, color: [4]u8) void {
    for (0..canvas.len / 4) |i| {
        @memcpy(canvas[i * 4 ..][0..4], &color);
    }
}

fn decodeImageIndicesAlloc(alloc: Allocator, compressed: []const u8, min_code_size: u8, pixel_count: usize) ![]u8 {
    if (min_code_size < 2 or min_code_size > 8) return error.GifDecodeFailed;

    const clear_code: u16 = @as(u16, 1) << @intCast(min_code_size);
    const end_code: u16 = clear_code + 1;
    var next_code: u16 = end_code + 1;
    var code_size: u8 = min_code_size + 1;

    var prefixes: [4096]u16 = undefined;
    var suffixes: [4096]u8 = undefined;
    var stack: [4096]u8 = undefined;
    var stack_len: usize = 0;

    for (0..clear_code) |i| suffixes[i] = @intCast(i);

    var out = try alloc.alloc(u8, pixel_count);
    errdefer alloc.free(out);
    var out_len: usize = 0;

    var bit_index: usize = 0;
    var old_code: ?u16 = null;
    var first_char: u8 = 0;

    while (true) {
        const code = try readCode(compressed, &bit_index, code_size);
        if (code == clear_code) {
            next_code = end_code + 1;
            code_size = min_code_size + 1;
            old_code = null;
            continue;
        }
        if (code == end_code) break;

        if (old_code == null) {
            if (code >= clear_code) return error.GifDecodeFailed;
            first_char = suffixes[code];
            if (out_len >= out.len) return error.GifDecodeFailed;
            out[out_len] = first_char;
            out_len += 1;
            old_code = code;
            continue;
        }

        const in_code = code;
        var cur = code;
        stack_len = 0;

        if (cur == next_code) {
            stack[stack_len] = first_char;
            stack_len += 1;
            cur = old_code.?;
        } else if (cur > next_code) {
            return error.GifDecodeFailed;
        }

        while (cur >= clear_code) {
            if (cur >= next_code or stack_len >= stack.len) return error.GifDecodeFailed;
            stack[stack_len] = suffixes[cur];
            stack_len += 1;
            cur = prefixes[cur];
        }

        first_char = suffixes[cur];
        stack[stack_len] = first_char;
        stack_len += 1;

        while (stack_len > 0) {
            stack_len -= 1;
            if (out_len >= out.len) return error.GifDecodeFailed;
            out[out_len] = stack[stack_len];
            out_len += 1;
        }

        if (next_code < 4096) {
            prefixes[next_code] = old_code.?;
            suffixes[next_code] = first_char;
            next_code += 1;
            if (next_code == (@as(u16, 1) << @intCast(code_size)) and code_size < 12) {
                code_size += 1;
            }
        }

        old_code = in_code;
        if (out_len == out.len) break;
    }

    if (out_len != out.len) return error.GifDecodeFailed;
    return out;
}

fn readCode(compressed: []const u8, bit_index: *usize, code_size: u8) !u16 {
    var value: u16 = 0;
    for (0..code_size) |bit| {
        const absolute_bit = bit_index.* + bit;
        const byte_index = absolute_bit / 8;
        if (byte_index >= compressed.len) return error.GifDecodeFailed;
        const shift: u3 = @intCast(absolute_bit % 8);
        const current_bit = (compressed[byte_index] >> shift) & 1;
        value |= @as(u16, current_bit) << @intCast(bit);
    }
    bit_index.* += code_size;
    return value;
}

test "decode frames matches manifest-backed animated gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/animated/two-frame-1x1.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames matches manifest-backed 2x2 animated gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/animated/two-frame-2x2.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames matches manifest-backed local-palette gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/conformance/local-palette-two-frame-2x2.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0x81), fixture_bytes[0x31]);
    try std.testing.expectEqual(@as(u8, 0x81), fixture_bytes[0x58]);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames matches manifest-backed interlaced local-palette gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/conformance/interlaced-local-palette-two-frame-2x2.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0xC1), fixture_bytes[0x31]);
    try std.testing.expectEqual(@as(u8, 0xC1), fixture_bytes[0x58]);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames matches manifest-backed disposal background gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/conformance/disposal-background-3x3.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0x09), fixture_bytes[0x33f]);
    try std.testing.expectEqual(@as(u8, 0x00), fixture_bytes[11]);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames matches manifest-backed disposal previous gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/conformance/disposal-previous-3x3.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0x0D), fixture_bytes[0x33f]);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames matches manifest-backed transparency overlay gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/conformance/transparency-overlay-3x3.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0x01), fixture_bytes[0x34]);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames matches manifest-backed comment extension gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/valid_weird/two-frame-2x2-comment.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.success, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0x21), fixture_bytes[781]);
    try std.testing.expectEqual(@as(u8, 0xFE), fixture_bytes[782]);

    const frames = try decodeFramesAlloc(alloc, fixture_bytes);
    defer {
        for (frames) |frame| alloc.free(frame.rgba);
        alloc.free(frames);
    }

    try std.testing.expectEqual(fixture.frames.?, frames.len);
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(fixture.width.?, frame.width);
        try std.testing.expectEqual(fixture.height.?, frame.height);
        try std.testing.expectEqual(fixture.frame_delays_ms[i], frame.delay_ms);
        const actual_hex = try test_support.sha256HexAlloc(alloc, frame.rgba);
        defer alloc.free(actual_hex);
        try std.testing.expectEqualStrings(fixture.pixel_hashes[i], actual_hex);
    }
}

test "decode frames rejects manifest-backed truncated gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/invalid/truncated-two-frame-2x2.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectError(error.GifDecodeFailed, decodeFramesAlloc(alloc, fixture_bytes));
}

test "decode frames rejects manifest-backed out of bounds gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/invalid/out-of-bounds-two-frame-2x2.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0x01), fixture_bytes[809]);
    try std.testing.expectEqual(@as(u8, 0x02), fixture_bytes[813]);
    try std.testing.expectError(error.GifDecodeFailed, decodeFramesAlloc(alloc, fixture_bytes));
}

test "decode frames rejects manifest-backed bad min code size gif fixture" {
    const alloc = std.testing.allocator;
    const manifest = try test_support.loadManifest(alloc, std.testing.io);
    defer test_support.freeManifest(alloc, manifest);

    const fixture = test_support.findFixture(manifest, "gif/invalid/bad-min-code-size-3x3.gif") orelse return error.MissingImageFixture;
    try std.testing.expectEqualStrings(manifest.results.invalid, fixture.result);
    try std.testing.expectEqualStrings("gif", fixture.format);

    const fixture_bytes = try test_support.readFixtureAlloc(alloc, std.testing.io, fixture.path);
    defer alloc.free(fixture_bytes);

    try std.testing.expectEqual(@as(u8, 0x01), fixture_bytes[0x2b]);
    try std.testing.expectError(error.GifDecodeFailed, decodeFramesAlloc(alloc, fixture_bytes));
}
