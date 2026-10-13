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
const image = @import("antfly_image").processing;
const apple = @import("backends/apple.zig");
const supported = @import("builtin").os.tag == .macos;
extern fn av_preparer_create(*anyopaque, *?*anyopaque) i32;
extern fn av_preparer_destroy(*anyopaque) void;
extern fn av_coefficients_create(*anyopaque, [*]const u32, usize, [*]const i32, usize, [*]const u32, usize, [*]const i32, usize, *?*anyopaque) i32;
extern fn av_coefficients_destroy(*anyopaque) void;
extern fn av_prepare_submit(*anyopaque, *anyopaque, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepare_native_submit(*anyopaque, *anyopaque, ?*anyopaque, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepare_host_submit(*anyopaque, [*]const u8, usize, [*]const u8, usize, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepare_rgba_submit(*anyopaque, [*]const u8, usize, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepared_gpu_seconds(*anyopaque) f64;
extern fn av_preparer_device_bytes(*anyopaque) u64;
extern fn av_prepared_poll(*anyopaque) c_int;
extern fn av_prepared_buffer(*anyopaque) *anyopaque;
extern fn av_prepared_copy(*anyopaque, [*]f32, usize) i32;
extern fn av_prepared_release_source(*anyopaque) c_int;
extern fn av_preparer_tone_mask(*anyopaque) u32;
extern fn av_prepared_destroy(*anyopaque) void;

pub const Rotation = enum(u32) { none, clockwise90, half_turn, clockwise270 };
pub const Matrix = enum(u32) { bt601, bt709, bt2020_ncl };
pub const ToneMapping = @import("tone.zig").Policy;
pub const RgbRounding = enum(u32) { nearest, q13_components, q16_luma, gray_samples };
pub const Options = struct {
    /// Geometry comes from the pinned processor adapter, never inferred from FPS.
    width: u32,
    height: u32,
    /// Required explicit SDR color matrix; range comes from the NV12 surface.
    matrix: Matrix,
    rotation: Rotation = .none,
    /// Remove coded pixels before rotation/resampling; chroma uses coded coordinates.
    crop: media.webm.Crop = .{},
    tone_mapping: ToneMapping = .none,
    /// Explicit integer RGB profiles: Q13 floors component products; Q16
    /// quantizes chroma in the luma domain before expansion. Model policy
    /// selects by reference row alignment, independently of host dispatch.
    /// Gray samples repeat the rounded unexpanded code value for SDR monochrome.
    rgb_rounding: RgbRounding = .nearest,
    /// Rescale to [0,1], then center for HF vision patch linear inputs if requested.
    centered: bool = true,
    /// Torchvision rounds intermediate RGB8 stages; Pillow truncates them.
    torchvision: bool = false,
    admission_pool: ?*media.admission.Pool = null,
    max_soft_tokens: usize = 140,
    max_source_pixels: usize = 16 * 1024 * 1024,
    max_scratch_bytes: usize = 128 * 1024 * 1024,
    /// Copied RGBA input only; independent of resize scratch/output admission.
    max_host_staging_bytes: usize = 128 * 1024 * 1024,
};
pub const Geometry = struct {
    width: u32,
    height: u32,
    patches: usize,
    soft_tokens: usize,
    scratch_bytes: usize = 0,
    pub fn values(self: Geometry) usize {
        return self.patches * 768;
    }
};
pub fn croppedSize(width: u32, height: u32, crop: media.webm.Crop) ![2]u32 {
    const horizontal = @as(u64, crop.left) + crop.right;
    const vertical = @as(u64, crop.top) + crop.bottom;
    if (horizontal >= width or vertical >= height) return error.InvalidVideoGeometry;
    return .{ width - @as(u32, @intCast(horizontal)), height - @as(u32, @intCast(vertical)) };
}
pub fn geometry(source_width: u32, source_height_u32: u32, options: Options) !Geometry {
    if (source_width == 0 or source_height_u32 == 0 or options.width == 0 or options.height == 0) return error.InvalidVideoGeometry;
    // Match the qualified texture/display bounds even when caller budgets grow.
    if (source_width > 16_384 or source_height_u32 > 16_384 or options.width > 16_384 or options.height > 16_384) return error.ResourceLimitExceeded;
    if (options.matrix == .bt2020_ncl and options.tone_mapping == .none) return error.UnsupportedVideoColor;
    if (options.width % 48 != 0 or options.height % 48 != 0) return error.InvalidVideoGeometry;
    const pixels = try std.math.mul(usize, options.width, options.height);
    const source_pixels = try std.math.mul(usize, source_width, source_height_u32);
    const patches = pixels / 256;
    if (patches / 9 > options.max_soft_tokens or source_pixels > options.max_source_pixels) return error.ResourceLimitExceeded;
    const cropped = try croppedSize(source_width, source_height_u32, options.crop);
    const source_height = if (options.rotation == .clockwise90 or options.rotation == .clockwise270) cropped[0] else cropped[1];
    const horizontal_bytes = try std.math.mul(usize, try std.math.mul(usize, options.width, source_height), 3);
    const output_bytes = try std.math.mul(usize, pixels, 3 * @sizeOf(f32));
    const rgb_bytes = try std.math.mul(usize, source_pixels, 3);
    // One conservative bound covers both reference and device paths, including
    // CPU result/patch repacking. Coefficient storage is checked separately.
    const scratch = try std.math.add(usize, try std.math.add(usize, horizontal_bytes, rgb_bytes), try std.math.mul(usize, output_bytes, 2));
    if (scratch > options.max_scratch_bytes / 2) return error.ResourceLimitExceeded;
    return .{ .width = options.width, .height = options.height, .patches = patches, .soft_tokens = patches / 9, .scratch_bytes = scratch * 2 };
}
fn sourcePoint(x: usize, y: usize, width: usize, height: usize, rotation: Rotation) [2]usize {
    return switch (rotation) {
        .none => .{ x, y },
        .clockwise90 => .{ y, height - 1 - x },
        .half_turn => .{ width - 1 - x, height - 1 - y },
        .clockwise270 => .{ width - 1 - y, x },
    };
}
fn hostSample(bytes: []const u8, offset: usize, bit_depth: u8) u16 {
    return if (bit_depth == 8) bytes[offset] else std.mem.readInt(u16, bytes[offset..][0..2], .little);
}
fn rgb(planes: [2]apple.Plane, x: usize, y: usize, format: apple.Format, matrix: Matrix, bit_depth: u8, chroma_format: u8, rounding: RgbRounding, tone_mapping: ToneMapping) [3]u8 {
    if (tone_mapping != .none) return rgbHdr(planes, x, y, format, matrix, bit_depth, chroma_format, tone_mapping);
    if (rounding == .gray_samples and chroma_format == 0) {
        const sample = hostSample(planes[0].bytes, y * planes[0].stride + x * @as(usize, if (bit_depth == 8) 1 else 2), bit_depth) >> format.sampleShift();
        const shift: u4 = @intCast(bit_depth - 8);
        const rounded = (@as(u32, sample) + (if (shift == 0) @as(u32, 0) else @as(u32, 1) << (shift - 1))) >> shift;
        const value: u8 = @intCast(@min(rounded, 255));
        return .{ value, value, value };
    }
    const bytes: usize = if (bit_depth == 8) 1 else 2;
    const scale: f32 = @floatFromInt(@as(u32, 1) << @as(u5, @intCast(bit_depth - 8)));
    var luma: f32 = @as(f32, @floatFromInt((hostSample(planes[0].bytes, y * planes[0].stride + x * bytes, bit_depth) >> format.sampleShift()))) / scale;
    const sub_x: usize = if (chroma_format == 3) 1 else 2;
    const uv = (y / (if (chroma_format == 1) @as(usize, 2) else 1)) * planes[1].stride + (x / sub_x) * 2 * bytes;
    var u: f32 = if (chroma_format == 0) 0 else @as(f32, @floatFromInt((hostSample(planes[1].bytes, uv, bit_depth) >> format.sampleShift()))) / scale - 128;
    var v: f32 = if (chroma_format == 0) 0 else @as(f32, @floatFromInt((hostSample(planes[1].bytes, uv + bytes, bit_depth) >> format.sampleShift()))) / scale - 128;
    if (rounding == .q16_luma and bit_depth == 8 and tone_mapping == .none) return rgbQ16(@intFromFloat(luma), @intFromFloat(u), @intFromFloat(v), format.fullRange(), matrix);
    if (rounding == .q13_components and bit_depth == 8 and tone_mapping == .none) return rgbQ13(@intFromFloat(luma), @intFromFloat(u), @intFromFloat(v), format.fullRange(), matrix);
    if (!format.fullRange()) {
        luma = (luma - 16) * (255.0 / 219.0);
        u *= 255.0 / 224.0;
        v *= 255.0 / 224.0;
    } else if (bit_depth != 8) {
        const maximum: f32 = @floatFromInt((@as(u32, 1) << @as(u5, @intCast(bit_depth))) - 1);
        const full_scale = 255 * scale / maximum;
        luma *= full_scale;
        u *= full_scale;
        v *= full_scale;
    }
    const values: [3]f32 = if (matrix == .bt709)
        .{ luma + 1.5748 * v, luma - 0.187324 * u - 0.468124 * v, luma + 1.8556 * u }
    else if (matrix == .bt2020_ncl)
        .{ luma + 1.4746 * v, luma - 0.164553 * u - 0.571353 * v, luma + 1.8814 * u }
    else
        .{ luma + 1.402 * v, luma - 0.344136 * u - 0.714136 * v, luma + 1.772 * u };
    if (tone_mapping != .none) return @import("tone.zig").render(values, matrix == .bt2020_ncl, tone_mapping);
    var output: [3]u8 = undefined;
    for (&output, values) |*out, value| out.* = @intFromFloat(std.math.clamp(@floor(value + 0.5), 0, 255));
    return output;
}
// Evaluate absolute-light transfer in f64 before RGB8 quantization; small PQ
// exponent errors can cross a code-value boundary and amplify in the model.
fn rgbHdr(planes: [2]apple.Plane, x: usize, y: usize, format: apple.Format, matrix: Matrix, depth: u8, chroma: u8, policy: ToneMapping) [3]u8 {
    const bytes: usize = if (depth == 8) 1 else 2;
    const scale: f64 = @floatFromInt(@as(u32, 1) << @as(u5, @intCast(depth - 8)));
    var luma = @as(f64, @floatFromInt(hostSample(planes[0].bytes, y * planes[0].stride + x * bytes, depth) >> format.sampleShift())) / scale;
    const uv = (y / (if (chroma == 1) @as(usize, 2) else 1)) * planes[1].stride + (x / (if (chroma == 3) @as(usize, 1) else 2)) * 2 * bytes;
    var u: f64 = if (chroma == 0) 0 else @as(f64, @floatFromInt(hostSample(planes[1].bytes, uv, depth) >> format.sampleShift())) / scale - 128;
    var v: f64 = if (chroma == 0) 0 else @as(f64, @floatFromInt(hostSample(planes[1].bytes, uv + bytes, depth) >> format.sampleShift())) / scale - 128;
    if (!format.fullRange()) {
        luma = (luma - 16) * (255.0 / 219.0);
        u *= 255.0 / 224.0;
        v *= 255.0 / 224.0;
    } else if (depth != 8) {
        const maximum: f64 = @floatFromInt((@as(u32, 1) << @as(u5, @intCast(depth))) - 1);
        const factor = 255 * scale / maximum;
        luma *= factor;
        u *= factor;
        v *= factor;
    }
    const values: [3]f64 = switch (matrix) {
        .bt2020_ncl => .{ luma + 1.4746 * v, luma - 0.164553 * u - 0.571353 * v, luma + 1.8814 * u },
        .bt709 => .{ luma + 1.5748 * v, luma - 0.187324 * u - 0.468124 * v, luma + 1.8556 * u },
        .bt601 => .{ luma + 1.402 * v, luma - 0.344136 * u - 0.714136 * v, luma + 1.772 * u },
    };
    return @import("tone.zig").render64(values, matrix == .bt2020_ncl, policy);
}
// Q16 luma-domain quantization for the scalar RGB24 reference profile.
// Chroma displacements are quantized before luma expansion. Evaluate the
// integer equations directly; no external conversion library or lookup cache.
fn q16Coefficients(full: bool, matrix: Matrix) [4]i32 {
    return switch (matrix) {
        .bt601 => if (full) .{ 91881, -22552, -46800, 116129 } else .{ 89830, -22049, -45756, 113537 },
        .bt709 => if (full) .{ 103206, -12275, -30678, 121608 } else .{ 100902, -12001, -29993, 118894 },
        .bt2020_ncl => if (full) .{ 96638, -10783, -37443, 123299 } else .{ 94482, -10543, -36607, 120547 },
    };
}
fn rgbQ16(y: i32, u_delta: i32, v_delta: i32, full: bool, matrix: Matrix) [3]u8 {
    const coefficients = q16Coefficients(full, matrix);
    const u = u_delta + 128;
    const v = v_delta + 128;
    const offsets = [4]i32{
        ((v * coefficients[0]) >> 16) - (coefficients[0] >> 9),
        ((u * coefficients[1]) >> 16) - (coefficients[1] >> 9),
        ((v * coefficients[2]) >> 16) - (coefficients[2] >> 9),
        ((u * coefficients[3]) >> 16) - (coefficients[3] >> 9),
    };
    const indexes = [3]i32{ y + offsets[0], y + offsets[1] + offsets[2], y + offsets[3] };
    var output: [3]u8 = undefined;
    for (indexes, &output) |index, *value| {
        const expanded = if (full) index else ((index + 326) * 76309 - 400 * 65536 + 32768) >> 16;
        value.* = @intCast(std.math.clamp(expanded, 0, 255));
    }
    return output;
}
// Coefficients are independently quantized from the BT.601/BT.709 matrix
// and range expansion. Signed shifts deliberately floor each component.
fn rgbQ13(y: i32, u: i32, v: i32, full: bool, matrix: Matrix) [3]u8 {
    const coefficients: [4]i32 = if (matrix == .bt709)
        (if (full) .{ 12901, -1534, -3835, 15201 } else .{ 14686, -1747, -4366, 17305 })
    else
        (if (full) .{ 11485, -2819, -5850, 14516 } else .{ 13075, -3209, -6660, 16525 });
    const luma = ((y - @as(i32, if (full) 0 else 16)) * @as(i32, if (full) 8192 else 9539)) >> 12;
    const values = [3]i32{
        (luma + ((v * coefficients[0]) >> 12) + 1) >> 1,
        (luma + ((u * coefficients[1]) >> 12) + ((v * coefficients[2]) >> 12) + 1) >> 1,
        (luma + ((u * coefficients[3]) >> 12) + 1) >> 1,
    };
    var out: [3]u8 = undefined;
    for (&out, values) |*channel, value| channel.* = @intCast(std.math.clamp(value, 0, 255));
    return out;
}
/// Reference path deliberately materializes host RGB. Production Metal path
/// imports NV12 and keeps both resize passes and patch packing on device.
pub const HostSurface = struct {
    /// Right-aligned little-endian u16 samples when depth exceeds 8.
    bit_depth: u8 = 8,
    chroma_format: u8 = 1,
    width: u32,
    height: u32,
    format: apple.Format,
    planes: [2]apple.Plane,
    pub fn validate(self: HostSurface) !void {
        if (self.bit_depth < 8 or self.bit_depth > 14 or self.chroma_format > 3) return error.UnsupportedSurfaceFormat;
        if (self.format.sampleShift() != 0 and (self.bit_depth != 10 or self.chroma_format != 1)) return error.UnsupportedSurfaceFormat;
        if (self.width == 0 or self.height == 0) return error.InvalidVideoGeometry;
        if (self.width > 16_384 or self.height > 16_384) return error.ResourceLimitExceeded;
        for (self.planes, 0..) |plane, i| {
            if (i == 1 and self.chroma_format == 0) {
                if (plane.width != 0 or plane.height != 0 or plane.stride != 0 or plane.bytes.len != 0) return error.UnsupportedSurfaceFormat;
                continue;
            }
            const width = if (i == 0 or self.chroma_format == 3) self.width else (self.width + 1) / 2;
            const height = if (i == 0 or self.chroma_format >= 2) self.height else (self.height + 1) / 2;
            const row = try std.math.mul(usize, width, (if (i == 0) @as(usize, 1) else 2) * (if (self.bit_depth == 8) @as(usize, 1) else 2));
            const size = try std.math.mul(usize, plane.stride, height);
            if (plane.width != width or plane.height != height or plane.stride < row or plane.bytes.len < size) return error.UnsupportedSurfaceFormat;
        }
    }
};
pub fn reference(allocator: std.mem.Allocator, surface: *const apple.Surface, options: Options, control: media.source.Control) ![]f32 {
    try control.check();
    _ = try geometry(surface.width, surface.height, options);
    var mapping = try surface.map();
    defer mapping.deinit();
    return referenceHost(allocator, .{ .width = surface.width, .height = surface.height, .format = surface.format, .bit_depth = surface.format.bitDepth(), .planes = mapping.planes }, options, control);
}
/// Portable borrowed NV12 boundary. Plane storage must survive the synchronous
/// call. No Apple framework or native device is needed for the reference path.
pub fn referenceHost(allocator: std.mem.Allocator, host: HostSurface, options: Options, control: media.source.Control) ![]f32 {
    return referenceHostImpl(allocator, host, options, control, true);
}
/// Scalar preparation oracle for numerical qualification and matched benchmarks.
pub fn referenceHostScalar(allocator: std.mem.Allocator, host: HostSurface, options: Options, control: media.source.Control) ![]f32 {
    return referenceHostImpl(allocator, host, options, control, false);
}
fn referenceHostImpl(allocator: std.mem.Allocator, host: HostSurface, options: Options, control: media.source.Control, simd: bool) ![]f32 {
    try control.check();
    const g = try geometry(host.width, host.height, options);
    const rotated = options.rotation == .clockwise90 or options.rotation == .clockwise270;
    const cropped = try croppedSize(host.width, host.height, options.crop);
    const width = if (rotated) cropped[1] else cropped[0];
    const height = if (rotated) cropped[0] else cropped[1];
    try host.validate();
    const bytes = try allocator.alloc(u8, @as(usize, width) * height * 3);
    defer allocator.free(bytes);
    for (0..height) |y| {
        try control.check();
        var x: usize = 0;
        if (simd and options.rotation == .none and (host.chroma_format == 0 or host.chroma_format == 3 or options.crop.left % 2 == 0) and host.bit_depth == 8 and options.tone_mapping == .none and (options.rgb_rounding == .q13_components or options.rgb_rounding == .q16_luma)) {
            while (x + 8 <= width) : (x += 8) integerRgbRow8(bytes[(y * width + x) * 3 ..][0..24], host, x + options.crop.left, y + options.crop.top, options);
        }
        while (x < width) : (x += 1) {
            const local = sourcePoint(x, y, cropped[0], cropped[1], options.rotation);
            const point = [2]usize{ local[0] + options.crop.left, local[1] + options.crop.top };
            @memcpy(bytes[(y * width + x) * 3 ..][0..3], &rgb(host.planes, point[0], point[1], host.format, options.matrix, host.bit_depth, host.chroma_format, options.rgb_rounding, options.tone_mapping));
        }
    }
    return prepareRgb(allocator, bytes, width, height, g, options, control);
}

fn integerRgbRow8(out: []u8, host: HostSurface, x: usize, y: usize, options: Options) void {
    const V = @Vector(8, i32);
    var luma_bytes: [8]u8 = undefined;
    @memcpy(&luma_bytes, host.planes[0].bytes[y * host.planes[0].stride + x ..][0..8]);
    const luma: V = @intCast(@as(@Vector(8, u8), luma_bytes));
    var u: V = @splat(0);
    var v: V = @splat(0);
    if (host.chroma_format != 0) {
        const cy = if (host.chroma_format == 1) y / 2 else y;
        const start = cy * host.planes[1].stride + (if (host.chroma_format == 3) x * 2 else x);
        if (host.chroma_format == 3) {
            var uv_bytes: [16]u8 = undefined;
            @memcpy(&uv_bytes, host.planes[1].bytes[start..][0..16]);
            const uv: @Vector(16, u8) = uv_bytes;
            u = @as(V, @intCast(@shuffle(u8, uv, undefined, @Vector(8, i32){ 0, 2, 4, 6, 8, 10, 12, 14 }))) - @as(V, @splat(128));
            v = @as(V, @intCast(@shuffle(u8, uv, undefined, @Vector(8, i32){ 1, 3, 5, 7, 9, 11, 13, 15 }))) - @as(V, @splat(128));
        } else {
            var uv_bytes: [8]u8 = undefined;
            @memcpy(&uv_bytes, host.planes[1].bytes[start..][0..8]);
            const uv: @Vector(8, u8) = uv_bytes;
            u = @as(V, @intCast(@shuffle(u8, uv, undefined, @Vector(8, i32){ 0, 0, 2, 2, 4, 4, 6, 6 }))) - @as(V, @splat(128));
            v = @as(V, @intCast(@shuffle(u8, uv, undefined, @Vector(8, i32){ 1, 1, 3, 3, 5, 5, 7, 7 }))) - @as(V, @splat(128));
        }
    }
    var values: [3]V = undefined;
    const full = host.format.fullRange();
    if (options.rgb_rounding == .q16_luma) {
        const c = q16Coefficients(full, options.matrix);
        const uu = u + @as(V, @splat(128));
        const vv = v + @as(V, @splat(128));
        const r = ((vv * @as(V, @splat(c[0]))) >> @as(@Vector(8, u5), @splat(16))) - @as(V, @splat(c[0] >> 9));
        const gu = ((uu * @as(V, @splat(c[1]))) >> @as(@Vector(8, u5), @splat(16))) - @as(V, @splat(c[1] >> 9));
        const gv = ((vv * @as(V, @splat(c[2]))) >> @as(@Vector(8, u5), @splat(16))) - @as(V, @splat(c[2] >> 9));
        const b = ((uu * @as(V, @splat(c[3]))) >> @as(@Vector(8, u5), @splat(16))) - @as(V, @splat(c[3] >> 9));
        values = .{ luma + r, luma + gu + gv, luma + b };
        if (!full) {
            for (&values) |*channel| channel.* = ((channel.* + @as(V, @splat(326))) * @as(V, @splat(76309)) - @as(V, @splat(400 * 65536)) + @as(V, @splat(32768))) >> @as(@Vector(8, u5), @splat(16));
        }
    } else {
        const c: [4]i32 = if (options.matrix == .bt709)
            (if (full) .{ 12901, -1534, -3835, 15201 } else .{ 14686, -1747, -4366, 17305 })
        else
            (if (full) .{ 11485, -2819, -5850, 14516 } else .{ 13075, -3209, -6660, 16525 });
        const yy = ((luma - @as(V, @splat(if (full) 0 else 16))) * @as(V, @splat(if (full) 8192 else 9539))) >> @as(@Vector(8, u5), @splat(12));
        values = .{
            yy + ((v * @as(V, @splat(c[0]))) >> @as(@Vector(8, u5), @splat(12))),
            yy + ((u * @as(V, @splat(c[1]))) >> @as(@Vector(8, u5), @splat(12))) + ((v * @as(V, @splat(c[2]))) >> @as(@Vector(8, u5), @splat(12))),
            yy + ((u * @as(V, @splat(c[3]))) >> @as(@Vector(8, u5), @splat(12))),
        };
        for (&values) |*channel| channel.* = (channel.* + @as(V, @splat(1))) >> @as(@Vector(8, u5), @splat(1));
    }
    var channels: [3]@Vector(8, u8) = undefined;
    for (values, &channels) |value, *channel| channel.* = @intCast(@min(@max(value, @as(V, @splat(0))), @as(V, @splat(255))));
    const red: [8]u8 = channels[0];
    const green: [8]u8 = channels[1];
    const blue: [8]u8 = channels[2];
    for (0..8) |lane| {
        out[lane * 3] = red[lane];
        out[lane * 3 + 1] = green[lane];
        out[lane * 3 + 2] = blue[lane];
    }
}
/// Portable RGBA input, including pure Zig MJPEG output. Uses the same resize
/// coefficients/patch packing as the NV12 reference after color conversion.
pub fn referenceRgba(allocator: std.mem.Allocator, bytes: []const u8, source_width: u32, source_height: u32, options: Options, control: media.source.Control) ![]f32 {
    try control.check();
    const g = try geometry(source_width, source_height, options);
    if (options.tone_mapping != .none) return error.UnsupportedVideoColor;
    if (bytes.len != @as(usize, source_width) * source_height * 4) return error.InvalidVideoGeometry;
    const rotated = options.rotation == .clockwise90 or options.rotation == .clockwise270;
    const cropped = try croppedSize(source_width, source_height, options.crop);
    const width = if (rotated) cropped[1] else cropped[0];
    const height = if (rotated) cropped[0] else cropped[1];
    const rgb_bytes = try allocator.alloc(u8, @as(usize, width) * height * 3);
    defer allocator.free(rgb_bytes);
    for (0..height) |y| {
        try control.check();
        for (0..width) |x| {
            const local = sourcePoint(x, y, cropped[0], cropped[1], options.rotation);
            const point = [2]usize{ local[0] + options.crop.left, local[1] + options.crop.top };
            const offset = (point[1] * source_width + point[0]) * 4;
            @memcpy(rgb_bytes[(y * width + x) * 3 ..][0..3], bytes[offset..][0..3]);
        }
    }
    return prepareRgb(allocator, rgb_bytes, width, height, g, options, control);
}
fn prepareRgb(allocator: std.mem.Allocator, bytes: []const u8, width: usize, height: usize, g: Geometry, options: Options, control: media.source.Control) ![]f32 {
    // Shared image control follows the same deadline through both resize passes.
    var scope = image.work_control.Scope.enter(.{ .context = control.context, .check_fn = control.check_fn });
    defer scope.deinit();
    const chw = try image.preprocessDecodedRectScaledWithResample(allocator, .{ .data = bytes, .width = @intCast(width), .height = @intCast(height), .format = .rgb8 }, g.width, g.height, .{ 0, 0, 0 }, .{ 1, 1, 1 }, 1.0 / 255.0, if (options.torchvision) .torchvision_bicubic else .pillow_bicubic);
    defer allocator.free(chw);
    const result = try allocator.alloc(f32, g.values());
    errdefer allocator.free(result);
    for (0..g.height) |y| {
        try control.check();
        for (0..g.width) |x| {
            const patch = (y / 16) * (g.width / 16) + x / 16;
            for (0..3) |c| {
                const value = chw[c * @as(usize, g.width) * g.height + y * g.width + x];
                result[patch * 768 + ((y % 16) * 16 + x % 16) * 3 + c] = if (options.centered) 2 * (value - 0.5) else value;
            }
        }
    }
    return result;
}
const Axis = struct {
    allocator: std.mem.Allocator,
    table: []u32,
    weights: []i32,
    fn bound(source: usize, target: usize) !usize {
        // Bound the coefficient builder before allocation. Each destination has
        // <= 4*ceil(scale)+2 taps, plus starts/offsets/float staging and packed table.
        const taps = try std.math.add(usize, try std.math.mul(usize, 4, std.math.divCeil(usize, source, target) catch return error.ResourceLimitExceeded), 2);
        return std.math.mul(usize, target, try std.math.add(usize, try std.math.mul(usize, taps, 32), 64));
    }
    fn init(allocator: std.mem.Allocator, source: usize, target: usize, max_bytes: usize, torchvision: bool) !Axis {
        if (try bound(source, target) > max_bytes) return error.ResourceLimitExceeded;
        var axis = try image.buildPillowBicubicAxis(allocator, source, target, torchvision);
        defer axis.deinit();
        // Lift Torchvision int16 coefficients into the shader's fixed 22-bit
        // scale without changing the two-pass integer rounding.
        for (axis.weights) |*weight| weight.* *= @as(i32, 1) << @intCast(22 - axis.precision_bits);
        const table = try allocator.alloc(u32, target * 3);
        errdefer allocator.free(table);
        for (0..target) |i| {
            table[i * 3] = std.math.cast(u32, axis.starts[i]) orelse return error.ResourceLimitExceeded;
            table[i * 3 + 1] = std.math.cast(u32, axis.offsets[i]) orelse return error.ResourceLimitExceeded;
            table[i * 3 + 2] = std.math.cast(u32, axis.offsets[i + 1] - axis.offsets[i]) orelse return error.ResourceLimitExceeded;
            var absolute: u64 = 0;
            for (axis.weights[axis.offsets[i]..axis.offsets[i + 1]]) |weight| absolute += @abs(@as(i64, weight));
            if (absolute * 255 + (1 << 21) > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
        }
        return .{ .allocator = allocator, .table = table, .weights = try allocator.dupe(i32, axis.weights) };
    }
    fn deinit(self: *Axis) void {
        self.allocator.free(self.table);
        self.allocator.free(self.weights);
    }
};
/// Owns a queue/pipelines on the caller's existing Metal device. Single-consumer.
pub const Metal = struct {
    handle: *anyopaque,
    /// One bounded resident geometry. Commands retain replaced buffers through
    /// completion; this owner is single-consumer even across concurrent jobs.
    coefficients: ?*anyopaque = null,
    coefficient_key: [5]u32 = .{ 0, 0, 0, 0, 0 },
    coefficient_bytes: usize = 0,
    coefficient_build_bound: usize = 0,
    coefficient_limit: usize = 16 * 1024 * 1024,
    cache_reservation: media.admission.Token = .{},
    pub fn init(device: *anyopaque) !Metal {
        if (!supported) return error.UnsupportedVideoBackend;
        var out: ?*anyopaque = null;
        if (av_preparer_create(device, &out) != 0) return error.MetalPreparationUnavailable;
        return .{ .handle = out.? };
    }
    pub fn deinit(self: *Metal) void {
        if (supported) {
            if (self.coefficients) |resident| av_coefficients_destroy(resident);
            av_preparer_destroy(self.handle);
            self.cache_reservation.deinit();
        }
        self.* = undefined;
    }
    /// Reserve a persistent coefficient budget on a shared pool. Call before
    /// submitting work. Destruction fences the queue before returning capacity.
    pub fn admit(self: *Metal, pool: *media.admission.Pool, max_coefficient_bytes: usize) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        if (self.coefficients != null or self.cache_reservation.pool != null) return error.MetalPreparerAlreadyUsed;
        self.cache_reservation = try pool.acquire(.{ .device_bytes = max_coefficient_bytes });
        self.coefficient_limit = max_coefficient_bytes;
    }
    /// Device-wide allocated size, including other work using this device.
    pub fn deviceAllocatedBytes(self: *const Metal) u64 {
        if (!supported) return 0;
        return av_preparer_device_bytes(self.handle);
    }
    /// Evict the resident geometry without invalidating submitted commands.
    pub fn clearCoefficients(self: *Metal) void {
        if (supported) if (self.coefficients) |resident| av_coefficients_destroy(resident);
        self.coefficients = null;
        self.coefficient_bytes = 0;
        self.coefficient_build_bound = 0;
    }
    fn ensureCoefficients(self: *Metal, allocator: std.mem.Allocator, width: u32, height: u32, g: Geometry, options: Options, control: media.source.Control) !usize {
        const mask = av_preparer_tone_mask(self.handle);
        const requested: u32 = switch (options.tone_mapping) {
            .none => 0,
            .pq_reinhard_100 => 1,
            .hlg_reinhard_100 => 2,
        };
        const color_bytes = @as(usize, @popCount(mask | requested)) * 16385 * 4;
        if (color_bytes > self.coefficient_limit) return error.ResourceLimitExceeded;
        const color_upload: usize = if (requested != 0 and mask & requested == 0) 16385 * 4 else 0;
        const rotated = options.rotation == .clockwise90 or options.rotation == .clockwise270;
        const cropped = try croppedSize(width, height, options.crop);
        const key: [5]u32 = .{ if (rotated) cropped[1] else cropped[0], if (rotated) cropped[0] else cropped[1], g.width, g.height, @intFromBool(options.torchvision) };
        if (self.coefficients != null and std.mem.eql(u32, &key, &self.coefficient_key)) {
            if (self.coefficient_build_bound > options.max_scratch_bytes / 4) return error.ResourceLimitExceeded;
            if (self.coefficient_bytes > self.coefficient_limit - color_bytes) return error.ResourceLimitExceeded;
            return color_upload;
        }
        var xaxis = try Axis.init(allocator, key[0], key[2], options.max_scratch_bytes / 4, options.torchvision);
        defer xaxis.deinit();
        var yaxis = try Axis.init(allocator, key[1], key[3], options.max_scratch_bytes / 4, options.torchvision);
        defer yaxis.deinit();
        try control.check();
        const byte_count = (xaxis.table.len + xaxis.weights.len + yaxis.table.len + yaxis.weights.len) * 4;
        if (byte_count > self.coefficient_limit - color_bytes) return error.ResourceLimitExceeded;
        var resident: ?*anyopaque = null;
        if (av_coefficients_create(self.handle, xaxis.table.ptr, xaxis.table.len, xaxis.weights.ptr, xaxis.weights.len, yaxis.table.ptr, yaxis.table.len, yaxis.weights.ptr, yaxis.weights.len, &resident) != 0) return error.MetalPreparationFailed;
        if (self.coefficients) |old| av_coefficients_destroy(old);
        self.coefficients = resident.?;
        self.coefficient_key = key;
        self.coefficient_bytes = byte_count;
        self.coefficient_build_bound = @max(try Axis.bound(key[0], key[2]), try Axis.bound(key[1], key[3]));
        return self.coefficient_bytes + color_upload;
    }
    pub fn submit(self: *Metal, allocator: std.mem.Allocator, surface: *const apple.Surface, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        var scope = image.work_control.Scope.enter(.{ .context = control.context, .check_fn = control.check_fn });
        defer scope.deinit();
        const g = try geometry(surface.width, surface.height, options);
        var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .device_bytes = g.scratch_bytes, .commands = 1 }) else media.admission.Token{};
        errdefer reservation.deinit();
        const uploaded = try self.ensureCoefficients(allocator, surface.width, surface.height, g, options, control);
        const params = [_]u32{ surface.width, surface.height, g.width, g.height, @backingInt(options.rotation), @backingInt(options.matrix), @intFromBool(surface.format.fullRange()), @intFromBool(options.centered), surface.format.bitDepth(), 1, @backingInt(options.rgb_rounding), surface.format.sampleShift(), @backingInt(options.tone_mapping), options.crop.left, options.crop.top, options.crop.right, options.crop.bottom };
        var out: ?*anyopaque = null;
        if (av_prepare_submit(self.handle, surface.handle, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .reservation = reservation, .handle = out.?, .geometry = g, .coefficient_staging_bytes = uploaded };
    }
    /// Caller-owned integer Metal plane textures on this preparer's device.
    /// R8/RG8Uint or R16/RG16Uint contain right-aligned native samples. The
    /// command retains textures until completion, without pixel readback/copy.
    pub const NativeSurface = struct {
        y: *anyopaque,
        /// Absent for monochrome; required for chroma formats 1 through 3.
        uv: ?*anyopaque = null,
        width: u32,
        height: u32,
        bit_depth: u8,
        sample_shift: u4 = 0,
        chroma_format: u8,
        full_range: bool = false,
    };
    pub fn submitNative(self: *Metal, allocator: std.mem.Allocator, surface: NativeSurface, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        if (surface.bit_depth < 8 or surface.bit_depth > 14 or surface.chroma_format > 3 or (surface.uv == null) != (surface.chroma_format == 0)) return error.UnsupportedSurfaceFormat;
        if (surface.sample_shift > 16 - surface.bit_depth or (surface.bit_depth == 8 and surface.sample_shift != 0)) return error.UnsupportedSurfaceFormat;
        const g = try geometry(surface.width, surface.height, options);
        var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .device_bytes = g.scratch_bytes, .commands = 1 }) else media.admission.Token{};
        errdefer reservation.deinit();
        const uploaded = try self.ensureCoefficients(allocator, surface.width, surface.height, g, options, control);
        const params = [_]u32{ surface.width, surface.height, g.width, g.height, @backingInt(options.rotation), @backingInt(options.matrix), @intFromBool(surface.full_range), @intFromBool(options.centered), surface.bit_depth, surface.chroma_format, @backingInt(options.rgb_rounding), surface.sample_shift, @backingInt(options.tone_mapping), options.crop.left, options.crop.top, options.crop.right, options.crop.bottom };
        var out: ?*anyopaque = null;
        if (av_prepare_native_submit(self.handle, surface.y, surface.uv, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .reservation = reservation, .handle = out.?, .geometry = g, .coefficient_staging_bytes = uploaded };
    }
    /// Upload native planes directly, without host RGB conversion. Metal owns
    /// its copies before return; padded byte strides remain explicit.
    pub fn submitHost(self: *Metal, allocator: std.mem.Allocator, surface: HostSurface, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        try surface.validate();
        const g = try geometry(surface.width, surface.height, options);
        const size = try std.math.add(usize, surface.planes[0].bytes.len, surface.planes[1].bytes.len);
        if (size > options.max_host_staging_bytes) return error.ResourceLimitExceeded;
        var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .device_bytes = try std.math.add(usize, g.scratch_bytes, size), .commands = 1 }) else media.admission.Token{};
        errdefer reservation.deinit();
        const uploaded = try self.ensureCoefficients(allocator, surface.width, surface.height, g, options, control);
        const params = [_]u32{ surface.width, surface.height, g.width, g.height, @backingInt(options.rotation), @backingInt(options.matrix), @intFromBool(surface.format.fullRange()), @intFromBool(options.centered), surface.bit_depth, surface.chroma_format, @backingInt(options.rgb_rounding), surface.format.sampleShift(), @backingInt(options.tone_mapping), options.crop.left, options.crop.top, options.crop.right, options.crop.bottom };
        var out: ?*anyopaque = null;
        if (av_prepare_host_submit(self.handle, surface.planes[0].bytes.ptr, surface.planes[0].stride, surface.planes[1].bytes.ptr, surface.planes[1].stride, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .reservation = reservation, .handle = out.?, .geometry = g, .native_staging_bytes = size, .coefficient_staging_bytes = uploaded };
    }
    /// Copies tightly packed RGBA into owned Metal storage before returning.
    /// The producer may free its input immediately. Alpha/matrix are ignored
    /// as in referenceRgba; explicit rotation and centering still apply.
    pub fn submitRgba(self: *Metal, allocator: std.mem.Allocator, bytes: []const u8, width: u32, height: u32, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        var scope = image.work_control.Scope.enter(.{ .context = control.context, .check_fn = control.check_fn });
        defer scope.deinit();
        const g = try geometry(width, height, options);
        const size = std.math.mul(usize, try std.math.mul(usize, width, height), 4) catch return error.ResourceLimitExceeded;
        if (bytes.len != size) return error.InvalidVideoGeometry;
        if (size > options.max_host_staging_bytes) return error.ResourceLimitExceeded;
        var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .device_bytes = try std.math.add(usize, g.scratch_bytes, size), .commands = 1 }) else media.admission.Token{};
        errdefer reservation.deinit();
        const uploaded = try self.ensureCoefficients(allocator, width, height, g, options, control);
        const params = [_]u32{ width, height, g.width, g.height, @backingInt(options.rotation), 0, 0, @intFromBool(options.centered), 8, 1, @backingInt(options.rgb_rounding), 0, 0, options.crop.left, options.crop.top, options.crop.right, options.crop.bottom };
        var out: ?*anyopaque = null;
        if (av_prepare_rgba_submit(self.handle, bytes.ptr, bytes.len, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .reservation = reservation, .handle = out.?, .geometry = g, .rgba_staging_bytes = bytes.len, .coefficient_staging_bytes = uploaded };
    }
};
/// Owns input import/staging, GPU command, and patch buffer. Destroy fences in-flight
/// work, including after deadline/cancellation. No pixel readback on submit.
pub const Prepared = struct {
    reservation: media.admission.Token = .{},
    handle: *anyopaque,
    geometry: Geometry,
    /// Logical bytes copied by newBufferWithBytes, not physical PCIe/DMA bytes.
    rgba_staging_bytes: usize = 0,
    native_staging_bytes: usize = 0,
    coefficient_staging_bytes: usize = 0,
    pub fn wait(self: *const Prepared, io: std.Io, control: media.source.Control) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        while (true) {
            try control.check();
            switch (av_prepared_poll(self.handle)) {
                1 => return,
                -1 => return error.MetalPreparationFailed,
                else => try std.Io.sleep(io, .fromMilliseconds(1), .awake),
            }
        }
    }
    /// After completion, discard input import/staging/command/intermediates while
    /// retaining the output buffer. Idempotent; wait before calling.
    pub fn releaseSource(self: *Prepared) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        if (av_prepared_release_source(self.handle) != 0) return error.MetalPreparationNotReady;
        try self.reservation.resize(.{ .device_bytes = self.geometry.values() * @sizeOf(f32) });
    }
    /// Borrowed id<MTLBuffer>; wait before consuming on another queue. For a
    /// resident inference consumer this is the handoff, not a host tensor copy.
    pub fn buffer(self: *const Prepared) !*anyopaque {
        if (!supported) return error.UnsupportedVideoBackend;
        if (av_prepared_poll(self.handle) != 1) return error.MetalPreparationNotReady;
        return av_prepared_buffer(self.handle);
    }
    /// Completed command execution duration, excluding queue wait and CPU setup.
    /// Preserved after releaseSource. Some drivers report zero if unavailable.
    pub fn gpuSeconds(self: *const Prepared) !f64 {
        if (!supported) return error.UnsupportedVideoBackend;
        const seconds = av_prepared_gpu_seconds(self.handle);
        if (seconds < 0) return error.MetalPreparationNotReady;
        return seconds;
    }
    /// Explicit oracle/debug readback. Model integration should use buffer().
    pub fn readback(self: *const Prepared, allocator: std.mem.Allocator) ![]f32 {
        if (!supported) return error.UnsupportedVideoBackend;
        const values = try allocator.alloc(f32, self.geometry.values());
        errdefer allocator.free(values);
        if (av_prepared_copy(self.handle, values.ptr, values.len) != 0) return error.MetalPreparationNotReady;
        return values;
    }
    /// Fences an outstanding command before releasing resources, including on
    /// cancellation, so the owner can safely release its admission afterwards.
    pub fn deinit(self: *Prepared) void {
        if (supported) av_prepared_destroy(self.handle);
        self.reservation.deinit();
        self.* = undefined;
    }
};

test "video Q13 RGB conversion matches independent reference across matrices ranges and clipping" {
    var y: [256]u8 = undefined;
    var uv: [128]u8 = undefined;
    for (&y, 0..) |*v, i| v.* = @intCast((i * 37 + 11) % 256);
    for (&uv, 0..) |*v, i| v.* = @intCast((i * 61 + 7) % 256);
    const planes = [2]apple.Plane{
        .{ .bytes = &y, .width = 16, .height = 16, .stride = 16 },
        .{ .bytes = &uv, .width = 8, .height = 8, .stride = 16 },
    };
    const expected = @embedFile("../testdata/rgb-q13-reference.bin");
    var offset: usize = 0;
    for ([_]Matrix{ .bt601, .bt709 }) |matrix| {
        for ([_]apple.Format{ .nv12_video, .nv12_full }) |format| {
            for (0..16) |row| for (0..16) |col| {
                const actual = rgb(planes, col, row, format, matrix, 8, 1, .q13_components, .none);
                try std.testing.expectEqualSlices(u8, expected[offset..][0..3], &actual);
                offset += 3;
            };
        }
    }
}

test "video Q16 luma-domain RGB matches independent nonaligned RGB24 reference" {
    var y: [26 * 16]u8 = undefined;
    var uv: [26 * 8]u8 = undefined;
    for (&y, 0..) |*value, i| value.* = @intCast((i * 37 + 11) % 256);
    for (&uv, 0..) |*value, i| value.* = @intCast((i * 61 + 7) % 256);
    const planes = [2]apple.Plane{
        .{ .bytes = &y, .width = 26, .height = 16, .stride = 26 },
        .{ .bytes = &uv, .width = 13, .height = 8, .stride = 26 },
    };
    const expected = @embedFile("../testdata/rgb-q16-reference.bin");
    var offset: usize = 0;
    for ([_]Matrix{ .bt601, .bt709, .bt2020_ncl }) |matrix| for ([_]apple.Format{ .nv12_video, .nv12_full }) |format| {
        for (0..16) |row| {
            var x: usize = 0;
            while (x + 8 <= 26) : (x += 8) {
                var vector_rgb: [24]u8 = undefined;
                integerRgbRow8(&vector_rgb, .{ .width = 26, .height = 16, .format = format, .planes = planes }, x, row, .{ .width = 96, .height = 48, .matrix = matrix, .rgb_rounding = .q16_luma });
                const position = offset + x * 3;
                try std.testing.expectEqualSlices(u8, expected[position..][0..24], &vector_rgb);
            }
            for (0..26) |col| {
                const actual = rgb(planes, col, row, format, matrix, 8, 1, .q16_luma, .none);
                try std.testing.expectEqualSlices(u8, expected[offset..][0..3], &actual);
                offset += 3;
            }
        }
    };
    try std.testing.expectEqual(expected.len, offset);
}

test "video portable integer RGB SIMD matches scalar across tails strides chroma and ranges" {
    const a = std.testing.allocator;
    var y: [1024]u8 = undefined;
    var uv: [1024]u8 = undefined;
    for (&y, 0..) |*value, i| value.* = @intCast((i * 37 + 11) % 256);
    for (&uv, 0..) |*value, i| value.* = @intCast((i * 61 + 7) % 256);
    for ([_]u32{ 1, 7, 8, 9, 15, 16, 17, 26, 33 }) |width| for (0..4) |chroma| for ([_]bool{ false, true }) |full| for ([_]Matrix{ .bt601, .bt709 }) |matrix| for ([_]RgbRounding{ .q13_components, .q16_luma }) |rounding| {
        const cw = if (chroma == 3) width else (width + 1) / 2;
        const ch: u32 = if (chroma == 1) 3 else 5;
        const ys: usize = width + 5;
        const us: usize = cw * 2 + 7;
        const host = HostSurface{ .width = width, .height = 5, .chroma_format = @intCast(chroma), .format = if (full) .nv12_full else .nv12_video, .planes = .{
            .{ .bytes = y[0 .. ys * 5], .width = width, .height = 5, .stride = ys },
            if (chroma == 0) .{ .bytes = &.{}, .width = 0, .height = 0, .stride = 0 } else .{ .bytes = uv[0 .. us * ch], .width = cw, .height = ch, .stride = us },
        } };
        const options = Options{ .width = 96, .height = 48, .matrix = matrix, .rgb_rounding = rounding, .torchvision = true };
        const expected = try referenceHostScalar(a, host, options, .{});
        defer a.free(expected);
        const actual = try referenceHost(a, host, options, .{});
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
    };
}

test "video cropped preparation preserves coded chroma coordinates before quarter turns" {
    var y: [26 * 16]u8 = undefined;
    var uv: [26 * 8]u8 = undefined;
    for (&y, 0..) |*v, i| v.* = @intCast((i * 37 + 11) % 256);
    for (&uv, 0..) |*v, i| v.* = @intCast((i * 61 + 7) % 256);
    const fixture = @embedFile("../testdata/rgb-q16-reference.bin");
    const host = HostSurface{ .width = 26, .height = 16, .format = .nv12_video, .planes = .{
        .{ .bytes = &y, .width = 26, .height = 16, .stride = 26 }, .{ .bytes = &uv, .width = 13, .height = 8, .stride = 26 },
    } };
    for ([_]u32{ 1, 2 }) |left| {
        const crop = media.webm.Crop{ .left = left, .right = 3, .top = 1, .bottom = 2 };
        const cw = 26 - left - 3;
        var cropped_rgba: [26 * 16 * 4]u8 = undefined;
        for (0..13) |row| for (0..cw) |col| {
            const src = ((row + 1) * 26 + col + left) * 3;
            const dst = (row * cw + col) * 4;
            @memcpy(cropped_rgba[dst..][0..3], fixture[src..][0..3]);
            cropped_rgba[dst + 3] = 255;
        };
        for (std.enums.values(Rotation)) |rotation| {
            const options = Options{ .width = 96, .height = 48, .matrix = .bt601, .rgb_rounding = .q16_luma, .torchvision = true, .crop = crop, .rotation = rotation };
            var oracle_options = options;
            oracle_options.crop = .{};
            const expected = try referenceRgba(std.testing.allocator, cropped_rgba[0 .. cw * 13 * 4], cw, 13, oracle_options, .{});
            defer std.testing.allocator.free(expected);
            const actual = try referenceHost(std.testing.allocator, host, options, .{});
            defer std.testing.allocator.free(actual);
            try std.testing.expectEqualSlices(f32, expected, actual);
        }
    }
    try std.testing.expectError(error.InvalidVideoGeometry, geometry(26, 16, .{ .width = 96, .height = 48, .matrix = .bt601, .crop = .{ .left = 26 } }));
    try std.testing.expectError(error.InvalidVideoGeometry, croppedSize(26, 16, .{ .left = std.math.maxInt(u32), .right = std.math.maxInt(u32) }));
}

test "video monochrome sample rounding matches exhaustive independent RGB oracle" {
    const expected = @embedFile("../testdata/gray-rgb-reference.bin");
    var at: usize = 0;
    for ([_]u8{ 8, 10, 12 }) |depth| {
        const count: usize = @as(usize, 1) << @as(u4, @intCast(depth));
        const bytes: usize = if (depth == 8) 1 else 2;
        const plane = try std.testing.allocator.alloc(u8, count * bytes);
        defer std.testing.allocator.free(plane);
        for (0..count) |i| {
            if (depth == 8) plane[i] = @intCast(i) else std.mem.writeInt(u16, plane[i * 2 ..][0..2], @intCast(i), .little);
        }
        const planes = [2]apple.Plane{ .{ .bytes = plane, .width = 64, .height = @intCast(count / 64), .stride = 64 * bytes }, .{ .bytes = &.{}, .width = 0, .height = 0, .stride = 0 } };
        for ([_]bool{ false, true }) |full| {
            for (0..count) |i| {
                const actual = rgb(planes, i % 64, i / 64, if (full) .nv12_full else .nv12_video, .bt601, depth, 0, .gray_samples, .none);
                try std.testing.expectEqualSlices(u8, expected[at..][0..3], &actual);
                at += 3;
            }
        }
    }
    try std.testing.expectEqual(expected.len, at);
}
