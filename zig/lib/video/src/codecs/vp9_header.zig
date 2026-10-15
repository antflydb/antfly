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

//! Geometry and superframe preflight before the reconstruction core allocates.
const std = @import("std");
pub const Geometry = struct {
    width: u32,
    height: u32,
    bit_depth: u8 = 8,
    chroma_format: u8 = 1,
    color: @import("frame.zig").Color = .{},
    profile: u8 = 0,
    av1_config: ?[4]u8 = null,
    apple_compatible: bool = true,
};
pub const References = [8]?Geometry;
pub const Picture = struct { geometry: Geometry, visible: bool, show_existing: bool = false, key: bool = false };
pub const Parts = struct { packets: [8][]const u8 = undefined, len: usize = 0 };
const Bits = struct {
    bytes: []const u8,
    position: usize = 0,
    fn read(self: *Bits, count: u5) !u32 {
        if (@as(usize, count) > self.bytes.len * 8 -| self.position) return error.InvalidVideoData;
        var value: u32 = 0;
        for (0..count) |_| {
            value = (value << 1) | ((self.bytes[self.position / 8] >> @intCast(7 - self.position % 8)) & 1);
            self.position += 1;
        }
        return value;
    }
    fn flag(self: *Bits) !bool {
        return try self.read(1) != 0;
    }
};
pub fn split(bytes: []const u8) !Parts {
    if (bytes.len == 0) return error.InvalidVideoData;
    var result: Parts = .{};
    const marker = bytes[bytes.len - 1];
    if (marker & 0xe0 != 0xc0) {
        result.packets[0] = bytes;
        result.len = 1;
        return result;
    }
    const count: usize = (marker & 7) + 1;
    const magnitude: usize = ((marker >> 3) & 3) + 1;
    const index_size = 2 + count * magnitude;
    // A marker-looking last payload byte without a matching index is payload.
    if (index_size > bytes.len or bytes[bytes.len - index_size] != marker) {
        result.packets[0] = bytes;
        result.len = 1;
        return result;
    }
    var offset: usize = 0;
    var index = bytes.len - index_size + 1;
    for (0..count) |i| {
        var packet_size: usize = 0;
        for (0..magnitude) |j| {
            packet_size |= @as(usize, bytes[index]) << @intCast(j * 8);
            index += 1;
        }
        if (packet_size == 0 or packet_size > bytes.len - index_size -| offset) return error.InvalidVideoData;
        result.packets[i] = bytes[offset..][0..packet_size];
        offset += packet_size;
    }
    if (offset != bytes.len - index_size) return error.InvalidVideoData;
    result.len = count;
    return result;
}
const ColorInfo = struct { depth: u8 = 8, chroma: u8 = 1, signal: @import("frame.zig").Color = .{} };
fn color(bits: *Bits, profile: u32) !ColorInfo {
    const depth: u8 = if (profile >= 2) (if (try bits.flag()) 12 else 10) else 8;
    const space = try bits.read(3);
    var full: bool = true;
    var chroma: u8 = 1;
    if (space != 7) {
        full = try bits.flag();
        if (profile == 1 or profile == 3) {
            const horizontal = try bits.flag();
            const vertical = try bits.flag();
            if (!horizontal and vertical) return error.UnsupportedVideoProfile;
            chroma = if (!horizontal) 3 else if (!vertical) 2 else 1;
            if (try bits.flag()) return error.InvalidVideoData;
        }
    } else {
        if (profile != 1 and profile != 3) return error.InvalidVideoData;
        chroma = 3;
        if (try bits.flag()) return error.InvalidVideoData;
    }
    return .{ .depth = depth, .chroma = chroma, .signal = .{
        .full_range = full,
        .primaries = if (space == 7) 1 else 2,
        .transfer = if (space == 7) 13 else 2,
        .matrix = switch (space) {
            1, 3 => 6,
            2 => 1,
            4 => 7,
            5 => 9,
            7 => 0,
            else => 2,
        },
    } };
}

fn size(bits: *Bits) !Geometry {
    return .{ .width = try bits.read(16) + 1, .height = try bits.read(16) + 1 };
}
fn render(bits: *Bits) !void {
    if (try bits.flag()) {
        _ = try bits.read(16);
        _ = try bits.read(16);
    }
}
fn sync(bits: *Bits) !void {
    if (try bits.read(24) != 0x498342) return error.InvalidVideoData;
}
pub fn inspect(bytes: []const u8, references: *References, max_pixels: usize) !Picture {
    var bits: Bits = .{ .bytes = bytes };
    if (try bits.read(2) != 2) return error.InvalidVideoData;
    const low = try bits.read(1);
    const profile = low + 2 * try bits.read(1);
    if (profile == 3 and try bits.flag()) return error.InvalidVideoData;
    if (try bits.flag()) {
        const geometry = references[try bits.read(3)] orelse return error.InvalidVideoData;
        try validate(geometry, max_pixels);
        return .{ .geometry = geometry, .visible = true, .show_existing = true };
    }
    const inter = try bits.flag();
    const show = try bits.flag();
    const resilient = try bits.flag();
    var refresh: u32 = 255;
    var geometry: Geometry = undefined;
    if (!inter) {
        try sync(&bits);
        const format = try color(&bits, profile);
        geometry = try size(&bits);
        geometry.bit_depth = format.depth;
        geometry.chroma_format = format.chroma;
        geometry.color = format.signal;
        try render(&bits);
    } else {
        const intra = if (!show) try bits.flag() else false;
        if (!resilient) _ = try bits.read(2);
        if (intra) {
            try sync(&bits);
            const format: ColorInfo = if (profile > 0) try color(&bits, profile) else .{ .signal = .{ .matrix = 6 } };
            refresh = try bits.read(8);
            geometry = try size(&bits);
            geometry.bit_depth = format.depth;
            geometry.chroma_format = format.chroma;
            geometry.color = format.signal;
            try render(&bits);
        } else {
            refresh = try bits.read(8);
            var refs: [3]u32 = undefined;
            for (&refs) |*ref| {
                ref.* = try bits.read(3);
                _ = try bits.read(1);
            }
            var found: ?Geometry = null;
            for (refs) |ref| {
                if (try bits.flag()) {
                    found = references[ref] orelse return error.InvalidVideoData;
                    break;
                }
            }
            geometry = found orelse try size(&bits);
            const format_reference = references[refs[0]] orelse return error.InvalidVideoData;
            geometry.bit_depth = format_reference.bit_depth;
            geometry.chroma_format = format_reference.chroma_format;
            geometry.color = format_reference.color;
            try render(&bits);
        }
    }
    try validate(geometry, max_pixels);
    geometry.profile = @intCast(profile);
    for (0..8) |i| {
        if (refresh & (@as(u32, 1) << @intCast(i)) != 0) references[i] = geometry;
    }
    return .{ .geometry = geometry, .visible = show, .key = !inter };
}
pub fn preflight(bytes: []const u8, references: *References, max_pixels: usize) !void {
    _ = try inspect(bytes, references, max_pixels);
}
fn validate(geometry: Geometry, max_pixels: usize) !void {
    if (geometry.width > 16_384 or geometry.height > 16_384 or @as(u64, geometry.width) * geometry.height > max_pixels) return error.ResourceLimitExceeded;
}

test "VP9 metadata visibility and geometry match reconstruction across profiles" {
    const a = std.testing.allocator;
    const decoder = @import("decoder.zig");
    inline for (.{ "../../testdata/vpx-av1/vp9.ivf", "../../testdata/vpx-av1/vp9-yuv420p.ivf", "../../testdata/vpx-av1/vp9-yuv420p10le.ivf", "../../testdata/vpx-av1/vp9-yuv420p12le.ivf", "../../testdata/vpx-av1/vp9-yuv422p.ivf", "../../testdata/vpx-av1/vp9-yuv422p12le.ivf", "../../testdata/vpx-av1/vp9-yuv444p.ivf", "../../testdata/vpx-av1/vp9-yuv444p10le.ivf" }) |path| {
        const bytes = @embedFile(path);
        const pixels = try decoder.Session.create(a, .vp9, .{});
        defer pixels.destroy();
        var references: References = @splat(null);
        var position: usize = 32;
        while (position < bytes.len) {
            const length = std.mem.readInt(u32, bytes[position..][0..4], .little);
            position += 12;
            const packet = bytes[position..][0..length];
            position += length;
            var batch = try pixels.decode(packet, 0, null);
            defer batch.deinit();
            const parts = try split(packet);
            var displayed: usize = 0;
            for (parts.packets[0..parts.len]) |part| {
                const metadata = try inspect(part, &references, 16 * 1024 * 1024);
                if (!metadata.visible) continue;
                try std.testing.expect(displayed < batch.frames.len);
                try std.testing.expectEqual(batch.frames[displayed].width, metadata.geometry.width);
                try std.testing.expectEqual(batch.frames[displayed].height, metadata.geometry.height);
                try std.testing.expectEqual(batch.frames[displayed].bit_depth, metadata.geometry.bit_depth);
                try std.testing.expectEqual(batch.frames[displayed].chroma_format, metadata.geometry.chroma_format);
                displayed += 1;
            }
            try std.testing.expectEqual(batch.frames.len, displayed);
        }
    }
}
