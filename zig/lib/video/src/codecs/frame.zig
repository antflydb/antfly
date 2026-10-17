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

const std = @import("std");
const media = @import("antfly_media");
const preparation = @import("../preparation.zig");
pub const Color = struct { full_range: bool = false, primaries: u8 = 2, transfer: u8 = 2, matrix: u8 = 2 };
/// Move-only, independently owned native-depth semiplanar samples. High-depth
/// values are right-aligned little-endian u16, rather than shifted P010.
pub const Frame = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    width: u32,
    height: u32,
    bit_depth: u8,
    chroma_format: u8,
    color: Color,
    pts_ns: i64,
    duration_ns: ?u64,
    decode_high_water: usize,
    /// Ordinal among this packet's native visible outputs, including skipped ones.
    output_index: usize = 0,
    reservation: media.admission.Token = .{},
    pub fn deinit(self: *Frame) void {
        self.allocator.free(self.bytes);
        self.reservation.deinit();
        self.* = undefined;
    }
    pub fn host(self: Frame) preparation.HostSurface {
        const sample: usize = if (self.bit_depth == 8) 1 else 2;
        const y_size = @as(usize, self.width) * self.height * sample;
        const cw: usize = if (self.chroma_format == 3) self.width else (self.width + 1) / 2;
        const ch: usize = if (self.chroma_format >= 2) self.height else (self.height + 1) / 2;
        return .{ .width = self.width, .height = self.height, .bit_depth = self.bit_depth, .chroma_format = self.chroma_format, .format = if (self.color.full_range) .nv12_full else .nv12_video, .planes = .{ .{ .bytes = self.bytes[0..y_size], .stride = self.width * sample, .width = self.width, .height = self.height }, if (self.chroma_format == 0) .{ .bytes = &.{}, .stride = 0, .width = 0, .height = 0 } else .{ .bytes = self.bytes[y_size..], .stride = cw * sample * 2, .width = cw, .height = ch } } };
    }
};
pub const Batch = struct {
    allocator: std.mem.Allocator,
    frames: []Frame,
    /// Native visible outputs drained, including outputs not materialized.
    output_count: usize = 0,
    reservation: media.admission.Token = .{},
    pub fn deinit(self: *Batch) void {
        for (self.frames) |*frame| frame.deinit();
        self.allocator.free(self.frames);
        self.reservation.deinit();
        self.* = undefined;
    }
};
/// Vectorize the planar-to-semiplanar boundary; scalar tails preserve odd widths.
fn interleave(comptime T: type, out: []u8, u: []const u8, v: []const u8, samples: usize) void {
    const n = @sizeOf(T);
    var i: usize = 0;
    while (i + 8 <= samples) : (i += 8) {
        var a: [8]T = undefined;
        var b: [8]T = undefined;
        @memcpy(std.mem.asBytes(&a), u[i * n ..][0 .. 8 * n]);
        @memcpy(std.mem.asBytes(&b), v[i * n ..][0 .. 8 * n]);
        const interleaved: [16]T = @shuffle(T, @as(@Vector(8, T), a), @as(@Vector(8, T), b), @Vector(16, i32){ 0, -1, 1, -2, 2, -3, 3, -4, 4, -5, 5, -6, 6, -7, 7, -8 });
        @memcpy(out[i * 2 * n ..][0 .. 16 * n], std.mem.asBytes(&interleaved));
    }
    while (i < samples) : (i += 1) {
        @memcpy(out[2 * i * n ..][0..n], u[i * n ..][0..n]);
        @memcpy(out[(2 * i + 1) * n ..][0..n], v[i * n ..][0..n]);
    }
}
pub fn validateLayout(width: u32, height: u32, depth: u8, chroma: u8, strides: [3]usize) !usize {
    if (width == 0 or height == 0 or width > 16_384 or height > 16_384) return error.ResourceLimitExceeded;
    if ((depth != 8 and depth != 10 and depth != 12) or chroma > 3) return error.UnsupportedVideoProfile;
    const sample: usize = if (depth == 8) 1 else 2;
    const cw: usize = if (chroma == 3) width else (width + 1) / 2;
    const ch: usize = if (chroma >= 2) height else (height + 1) / 2;
    if (strides[0] < width * sample or (chroma != 0 and (strides[1] < cw * sample or strides[2] < cw * sample))) return error.InvalidVideoData;
    return @as(usize, width) * height * sample + (if (chroma == 0) @as(usize, 0) else cw * ch * 2 * sample);
}
pub fn copy(allocator: std.mem.Allocator, pool: ?*media.admission.Pool, width: u32, height: u32, depth: u8, chroma: u8, color: Color, planes: [3][*]const u8, strides: [3]usize, limit: usize) !Frame {
    const total = try validateLayout(width, height, depth, chroma, strides);
    const sample: usize = if (depth == 8) 1 else 2;
    const cw: usize = if (chroma == 3) width else (width + 1) / 2;
    const ch: usize = if (chroma >= 2) height else (height + 1) / 2;
    const y_size = @as(usize, width) * height * sample;

    if (total > limit) return error.ResourceLimitExceeded;
    var reservation = if (pool) |p| try p.acquire(.{ .host_bytes = total }) else media.admission.Token{};
    errdefer reservation.deinit();
    const bytes = try allocator.alloc(u8, total);
    errdefer allocator.free(bytes);
    for (0..height) |y| @memcpy(bytes[y * width * sample ..][0 .. width * sample], planes[0][y * strides[0] ..][0 .. width * sample]);
    if (chroma != 0) for (0..ch) |y| {
        const out = bytes[y_size + y * cw * 2 * sample ..][0 .. cw * 2 * sample];
        const u = planes[1][y * strides[1] ..][0 .. cw * sample];
        const v = planes[2][y * strides[2] ..][0 .. cw * sample];
        if (sample == 1) interleave(u8, out, u, v, cw) else interleave(u16, out, u, v, cw);
    };
    return .{ .allocator = allocator, .bytes = bytes, .width = width, .height = height, .bit_depth = depth, .chroma_format = chroma, .color = color, .pts_ns = 0, .duration_ns = null, .decode_high_water = 0, .reservation = reservation };
}
