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

pub const Resample = enum(u32) {
    nearest = 0,
    lanczos = 1,
    bilinear = 2,
    bicubic = 3,
    /// Pillow's antialiased, quantized two-pass BICUBIC implementation. This
    /// is distinct from the legacy bicubic sampler used by existing model
    /// configurations so parity-sensitive callers must opt in explicitly.
    pillow_bicubic = 4,
};

pub const PixelFormat = enum(u8) {
    rgb8 = 3,
    rgba8 = 4,
};

pub const ImageU8 = struct {
    data: []const u8,
    width: u32,
    height: u32,
    format: PixelFormat,

    pub fn channels(self: ImageU8) usize {
        return @intFromEnum(self.format);
    }
};

/// Convert a decoded RGB/RGBA image into normalized CHW f32 layout.
pub fn preprocessDecoded(
    allocator: std.mem.Allocator,
    img: ImageU8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    return preprocessDecodedRectWithResample(allocator, img, target_size, target_size, mean, std_dev, .bilinear);
}

/// Convert a decoded RGB/RGBA image into normalized CHW f32 layout using the requested sampler.
pub fn preprocessDecodedWithResample(
    allocator: std.mem.Allocator,
    img: ImageU8,
    target_size: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
) ![]f32 {
    return preprocessDecodedRectWithResample(allocator, img, target_size, target_size, mean, std_dev, resample);
}

/// Convert a decoded RGB/RGBA image into normalized CHW f32 layout with explicit width and height.
pub fn preprocessDecodedRect(
    allocator: std.mem.Allocator,
    img: ImageU8,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
) ![]f32 {
    return preprocessDecodedRectWithResample(allocator, img, target_width, target_height, mean, std_dev, .bilinear);
}

/// Convert a decoded RGB/RGBA image into normalized CHW f32 layout with explicit width and height using the requested sampler.
pub fn preprocessDecodedRectWithResample(
    allocator: std.mem.Allocator,
    img: ImageU8,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
) ![]f32 {
    return preprocessDecodedRectScaledWithResample(
        allocator,
        img,
        target_width,
        target_height,
        mean,
        std_dev,
        1.0 / 255.0,
        resample,
    );
}

/// Convert a decoded RGB/RGBA image into normalized CHW f32 layout with an explicit pixel rescale factor.
pub fn preprocessDecodedRectScaledWithResample(
    allocator: std.mem.Allocator,
    img: ImageU8,
    target_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
    resample: Resample,
) ![]f32 {
    const source_pixels = std.math.mul(usize, img.width, img.height) catch return error.InvalidImageDimensions;
    const expected_min = std.math.mul(usize, source_pixels, img.channels()) catch return error.InvalidImageDimensions;
    if (img.data.len < expected_min) return error.InvalidImageBuffer;

    const tw: usize = target_width;
    const th: usize = target_height;
    if (tw == 0 or th == 0) return error.InvalidImageDimensions;
    const target_pixels = std.math.mul(usize, tw, th) catch return error.InvalidImageDimensions;
    const result_len = std.math.mul(usize, 3, target_pixels) catch return error.InvalidImageDimensions;
    const result = try allocator.alloc(f32, result_len);
    errdefer allocator.free(result);

    if (solidRgb(img)) |rgb| {
        fillSolidChw(result, tw, th, rgb, mean, std_dev, rescale_factor);
        return result;
    }

    const src_w: f32 = @floatFromInt(img.width);
    const src_h: f32 = @floatFromInt(img.height);
    const scale_x = src_w / @as(f32, @floatFromInt(tw));
    const scale_y = src_h / @as(f32, @floatFromInt(th));

    if (resample == .bilinear) {
        preprocessDecodedRectBilinearInterleaved(img, result, tw, th, mean, std_dev, rescale_factor, scale_x, scale_y);
        return result;
    }

    if (resample == .pillow_bicubic) {
        try preprocessDecodedRectPillowBicubic(
            allocator,
            img,
            result,
            tw,
            th,
            mean,
            std_dev,
            rescale_factor,
        );
        return result;
    }

    for (0..th) |y| {
        for (0..tw) |x| {
            for (0..3) |ch| {
                const val = sampleResized(img, x, y, ch, scale_x, scale_y, resample);
                result[ch * th * tw + y * tw + x] = normalizeSample(val, mean[ch], std_dev[ch], rescale_factor);
            }
        }
    }

    return result;
}

pub fn computeAspectFitWidth(src_width: u32, src_height: u32, target_height: u32, max_width: u32) u32 {
    if (src_width == 0 or src_height == 0 or target_height == 0 or max_width == 0) return 1;

    const scaled = @max(
        @as(u32, @intCast(@divFloor(
            @as(u64, src_width) * @as(u64, target_height) + @as(u64, src_height) - 1,
            @as(u64, src_height),
        ))),
        1,
    );
    return @min(scaled, max_width);
}

/// Convert a decoded RGB/RGBA image into normalized CHW f32 layout while preserving aspect ratio.
/// The resized content is left-aligned and the remaining columns are padded with the provided RGB value.
pub fn preprocessDecodedRectKeepAspectPadRightWithResample(
    allocator: std.mem.Allocator,
    img: ImageU8,
    max_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    resample: Resample,
    pad_rgb: [3]u8,
) ![]f32 {
    return preprocessDecodedRectKeepAspectPadRightScaledWithResample(
        allocator,
        img,
        max_width,
        target_height,
        mean,
        std_dev,
        1.0 / 255.0,
        resample,
        pad_rgb,
    );
}

/// Convert a decoded RGB/RGBA image into normalized CHW f32 layout while preserving aspect ratio and using an explicit pixel rescale factor.
pub fn preprocessDecodedRectKeepAspectPadRightScaledWithResample(
    allocator: std.mem.Allocator,
    img: ImageU8,
    max_width: u32,
    target_height: u32,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
    resample: Resample,
    pad_rgb: [3]u8,
) ![]f32 {
    const expected_min = @as(usize, img.width) * @as(usize, img.height) * img.channels();
    if (img.data.len < expected_min) return error.InvalidImageBuffer;

    const tw: usize = max_width;
    const th: usize = target_height;
    const result = try allocator.alloc(f32, 3 * tw * th);
    errdefer allocator.free(result);

    const content_width = computeAspectFitWidth(img.width, img.height, target_height, max_width);
    const cw: usize = content_width;

    for (0..3) |ch| {
        const pad_val = normalizeSample(@floatFromInt(pad_rgb[ch]), mean[ch], std_dev[ch], rescale_factor);
        @memset(result[ch * th * tw .. (ch + 1) * th * tw], pad_val);
    }

    const src_w: f32 = @floatFromInt(img.width);
    const src_h: f32 = @floatFromInt(img.height);
    const scale_x = src_w / @as(f32, @floatFromInt(cw));
    const scale_y = src_h / @as(f32, @floatFromInt(th));

    if (resample == .bilinear) {
        preprocessDecodedRectKeepAspectPadRightBilinearSimd(img, result, cw, tw, th, mean, std_dev, rescale_factor, scale_x, scale_y);
        return result;
    }

    for (0..th) |y| {
        for (0..cw) |x| {
            for (0..3) |ch| {
                const val = sampleResized(img, x, y, ch, scale_x, scale_y, resample);
                result[ch * th * tw + y * tw + x] = normalizeSample(val, mean[ch], std_dev[ch], rescale_factor);
            }
        }
    }

    return result;
}

fn preprocessDecodedRectKeepAspectPadRightBilinearSimd(
    img: ImageU8,
    result: []f32,
    content_width: usize,
    tw: usize,
    th: usize,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
    scale_x: f32,
    scale_y: f32,
) void {
    const lanes = 4;
    const VecF = @Vector(lanes, f32);
    const rescale_v: VecF = @splat(rescale_factor);

    for (0..3) |ch| {
        const mean_v: VecF = @splat(mean[ch]);
        const std_v: VecF = @splat(std_dev[ch]);
        const plane = result[ch * th * tw .. (ch + 1) * th * tw];

        for (0..th) |y| {
            const row_offset = y * tw;
            var x: usize = 0;
            while (x + lanes <= content_width) : (x += lanes) {
                const samples = sampleBilinear4(img, x, y, ch, scale_x, scale_y);
                const normalized = (samples * rescale_v - mean_v) / std_v;
                const out: [lanes]f32 = @bitCast(normalized);
                @memcpy(plane[row_offset + x .. row_offset + x + lanes], out[0..]);
            }
            while (x < content_width) : (x += 1) {
                const val = sampleBilinear(img, x, y, ch, scale_x, scale_y);
                plane[row_offset + x] = normalizeSample(val, mean[ch], std_dev[ch], rescale_factor);
            }
        }
    }
}

fn preprocessDecodedRectBilinearSimd(
    img: ImageU8,
    result: []f32,
    tw: usize,
    th: usize,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
    scale_x: f32,
    scale_y: f32,
) void {
    const lanes = 4;
    const VecF = @Vector(lanes, f32);
    const one: VecF = @splat(1.0);
    const rescale_v: VecF = @splat(rescale_factor);

    for (0..3) |ch| {
        const mean_v: VecF = @splat(mean[ch]);
        const std_v: VecF = @splat(std_dev[ch]);
        const plane = result[ch * th * tw .. (ch + 1) * th * tw];

        for (0..th) |y| {
            const row_offset = y * tw;
            var x: usize = 0;
            while (x + lanes <= tw) : (x += lanes) {
                const samples = sampleBilinear4(img, x, y, ch, scale_x, scale_y);
                const normalized = (samples * rescale_v - mean_v) / std_v;
                const out: [lanes]f32 = @bitCast(normalized);
                @memcpy(plane[row_offset + x .. row_offset + x + lanes], out[0..]);
            }
            while (x < tw) : (x += 1) {
                const val = sampleBilinear(img, x, y, ch, scale_x, scale_y);
                plane[row_offset + x] = normalizeSample(val, mean[ch], std_dev[ch], rescale_factor);
            }
        }
    }

    _ = one;
}

fn preprocessDecodedRectBilinearInterleaved(
    img: ImageU8,
    result: []f32,
    tw: usize,
    th: usize,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
    scale_x: f32,
    scale_y: f32,
) void {
    const channels = img.channels();
    const plane_size = tw * th;
    const norm_scale = [3]f32{
        rescale_factor / std_dev[0],
        rescale_factor / std_dev[1],
        rescale_factor / std_dev[2],
    };
    const norm_bias = [3]f32{
        -mean[0] / std_dev[0],
        -mean[1] / std_dev[1],
        -mean[2] / std_dev[2],
    };

    for (0..th) |y| {
        const src_y = (@as(f32, @floatFromInt(y)) + 0.5) * scale_y - 0.5;
        const y0 = clampIndex(@intFromFloat(@floor(src_y)), img.height);
        const y1 = clampIndex(@as(i32, @intCast(y0)) + 1, img.height);
        const fy = src_y - @as(f32, @floatFromInt(y0));
        const wy0 = 1.0 - fy;
        const row0 = @as(usize, y0) * @as(usize, img.width) * channels;
        const row1 = @as(usize, y1) * @as(usize, img.width) * channels;
        const dst_row = y * tw;

        for (0..tw) |x| {
            const src_x = (@as(f32, @floatFromInt(x)) + 0.5) * scale_x - 0.5;
            const x0 = clampIndex(@intFromFloat(@floor(src_x)), img.width);
            const x1 = clampIndex(@as(i32, @intCast(x0)) + 1, img.width);
            const fx = src_x - @as(f32, @floatFromInt(x0));
            const wx0 = 1.0 - fx;

            const idx00 = row0 + @as(usize, x0) * channels;
            const idx10 = row0 + @as(usize, x1) * channels;
            const idx01 = row1 + @as(usize, x0) * channels;
            const idx11 = row1 + @as(usize, x1) * channels;
            const dst_idx = dst_row + x;

            inline for (0..3) |ch| {
                const p00: f32 = @floatFromInt(img.data[idx00 + ch]);
                const p10: f32 = @floatFromInt(img.data[idx10 + ch]);
                const p01: f32 = @floatFromInt(img.data[idx01 + ch]);
                const p11: f32 = @floatFromInt(img.data[idx11 + ch]);
                const top = p00 * wx0 + p10 * fx;
                const bot = p01 * wx0 + p11 * fx;
                result[ch * plane_size + dst_idx] = (top * wy0 + bot * fy) * norm_scale[ch] + norm_bias[ch];
            }
        }
    }
}

fn solidRgb(img: ImageU8) ?[3]u8 {
    const channels = img.channels();
    const pixel_count = @as(usize, img.width) * @as(usize, img.height);
    if (pixel_count == 0 or img.data.len < pixel_count * channels) return null;

    const rgb = [3]u8{ img.data[0], img.data[1], img.data[2] };
    for (1..pixel_count) |i| {
        const idx = i * channels;
        if (img.data[idx] != rgb[0] or img.data[idx + 1] != rgb[1] or img.data[idx + 2] != rgb[2]) return null;
    }
    return rgb;
}

fn fillSolidChw(
    result: []f32,
    tw: usize,
    th: usize,
    rgb: [3]u8,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
) void {
    const plane_size = tw * th;
    for (0..3) |ch| {
        const value = normalizeSample(@floatFromInt(rgb[ch]), mean[ch], std_dev[ch], rescale_factor);
        @memset(result[ch * plane_size .. (ch + 1) * plane_size], value);
    }
}

fn sampleBilinear4(
    img: ImageU8,
    dst_x: usize,
    dst_y: usize,
    ch: usize,
    scale_x: f32,
    scale_y: f32,
) @Vector(4, f32) {
    const lanes = 4;
    const VecF = @Vector(lanes, f32);
    const VecI = @Vector(lanes, i32);

    const base_x: f32 = @floatFromInt(dst_x);
    const dst_xs = VecF{ base_x, base_x + 1.0, base_x + 2.0, base_x + 3.0 };
    const half: VecF = @splat(0.5);
    const src_x = (dst_xs + half) * @as(VecF, @splat(scale_x)) - half;

    const src_y = (@as(f32, @floatFromInt(dst_y)) + 0.5) * scale_y - 0.5;
    const y0 = clampIndex(@intFromFloat(@floor(src_y)), img.height);
    const y1 = clampIndex(@as(i32, @intCast(y0)) + 1, img.height);
    const fy: VecF = @splat(src_y - @as(f32, @floatFromInt(y0)));

    const x0_unclamped: VecI = @intFromFloat(@floor(src_x));
    const x0 = clampIndexVec(x0_unclamped, img.width);
    const x1 = clampIndexVec(x0 + @as(VecI, @splat(1)), img.width);
    const fx = src_x - @as(VecF, @floatFromInt(x0));

    var p00_arr: [lanes]f32 = undefined;
    var p10_arr: [lanes]f32 = undefined;
    var p01_arr: [lanes]f32 = undefined;
    var p11_arr: [lanes]f32 = undefined;
    const x0_arr: [lanes]i32 = @bitCast(x0);
    const x1_arr: [lanes]i32 = @bitCast(x1);

    inline for (0..lanes) |lane| {
        p00_arr[lane] = pixelAt(img, @intCast(x0_arr[lane]), y0, ch);
        p10_arr[lane] = pixelAt(img, @intCast(x1_arr[lane]), y0, ch);
        p01_arr[lane] = pixelAt(img, @intCast(x0_arr[lane]), y1, ch);
        p11_arr[lane] = pixelAt(img, @intCast(x1_arr[lane]), y1, ch);
    }

    const p00: VecF = @bitCast(p00_arr);
    const p10: VecF = @bitCast(p10_arr);
    const p01: VecF = @bitCast(p01_arr);
    const p11: VecF = @bitCast(p11_arr);
    const one: VecF = @splat(1.0);

    const top = p00 * (one - fx) + p10 * fx;
    const bot = p01 * (one - fx) + p11 * fx;
    return top * (one - fy) + bot * fy;
}

fn clampIndexVec(idx: @Vector(4, i32), dim: u32) @Vector(4, i32) {
    const zero: @Vector(4, i32) = @splat(0);
    const max_idx: @Vector(4, i32) = @splat(@as(i32, @intCast(dim - 1)));
    return @min(@max(idx, zero), max_idx);
}

fn pixelAt(img: ImageU8, x: u32, y: u32, ch: usize) f32 {
    const idx = (@as(usize, y) * @as(usize, img.width) + @as(usize, x)) * img.channels() + ch;
    return @floatFromInt(img.data[idx]);
}

fn sampleResized(
    img: ImageU8,
    dst_x: usize,
    dst_y: usize,
    ch: usize,
    scale_x: f32,
    scale_y: f32,
    resample: Resample,
) f32 {
    return switch (resample) {
        .bicubic => sampleBicubic(img, dst_x, dst_y, ch, scale_x, scale_y),
        else => sampleBilinear(img, dst_x, dst_y, ch, scale_x, scale_y),
    };
}

fn sampleBilinear(
    img: ImageU8,
    dst_x: usize,
    dst_y: usize,
    ch: usize,
    scale_x: f32,
    scale_y: f32,
) f32 {
    const src_x = (@as(f32, @floatFromInt(dst_x)) + 0.5) * scale_x - 0.5;
    const src_y = (@as(f32, @floatFromInt(dst_y)) + 0.5) * scale_y - 0.5;
    const x0 = clampIndex(@intFromFloat(@floor(src_x)), img.width);
    const y0 = clampIndex(@intFromFloat(@floor(src_y)), img.height);
    const x1 = clampIndex(@as(i32, @intCast(x0)) + 1, img.width);
    const y1 = clampIndex(@as(i32, @intCast(y0)) + 1, img.height);
    const fx = src_x - @as(f32, @floatFromInt(x0));
    const fy = src_y - @as(f32, @floatFromInt(y0));

    const p00 = pixelAt(img, x0, y0, ch);
    const p10 = pixelAt(img, x1, y0, ch);
    const p01 = pixelAt(img, x0, y1, ch);
    const p11 = pixelAt(img, x1, y1, ch);

    const top = p00 * (1.0 - fx) + p10 * fx;
    const bot = p01 * (1.0 - fx) + p11 * fx;
    return top * (1.0 - fy) + bot * fy;
}

fn sampleBicubic(
    img: ImageU8,
    dst_x: usize,
    dst_y: usize,
    ch: usize,
    scale_x: f32,
    scale_y: f32,
) f32 {
    const src_x = (@as(f32, @floatFromInt(dst_x)) + 0.5) * scale_x - 0.5;
    const src_y = (@as(f32, @floatFromInt(dst_y)) + 0.5) * scale_y - 0.5;
    const base_x: i32 = @intFromFloat(@floor(src_x));
    const base_y: i32 = @intFromFloat(@floor(src_y));
    var accum: f32 = 0.0;
    for (0..4) |ky| {
        const sy = clampIndex(base_y + @as(i32, @intCast(ky)) - 1, img.height);
        const wy = cubicWeight(src_y - @as(f32, @floatFromInt(base_y + @as(i32, @intCast(ky)) - 1)));
        for (0..4) |kx| {
            const sx = clampIndex(base_x + @as(i32, @intCast(kx)) - 1, img.width);
            const wx = cubicWeight(src_x - @as(f32, @floatFromInt(base_x + @as(i32, @intCast(kx)) - 1)));
            accum += pixelAt(img, sx, sy, ch) * wx * wy;
        }
    }
    return std.math.clamp(accum, 0.0, 255.0);
}

const BicubicAxis = struct {
    allocator: std.mem.Allocator,
    starts: []usize,
    offsets: []usize,
    weights: []i32,

    fn deinit(self: *@This()) void {
        self.allocator.free(self.starts);
        self.allocator.free(self.offsets);
        self.allocator.free(self.weights);
    }
};

/// Build Pillow-compatible antialiased bicubic coefficients. Downsampling
/// widens the cubic support by the reduction factor and renormalizes the
/// surviving in-bounds taps instead of clamping out-of-bounds samples.
fn buildPillowBicubicAxis(
    allocator: std.mem.Allocator,
    source_size: usize,
    target_size: usize,
) !BicubicAxis {
    if (source_size == 0 or target_size == 0) return error.InvalidImageDimensions;
    const starts = try allocator.alloc(usize, target_size);
    errdefer allocator.free(starts);
    const offsets = try allocator.alloc(usize, target_size + 1);
    errdefer allocator.free(offsets);
    var float_weights = std.ArrayListUnmanaged(f64).empty;
    defer float_weights.deinit(allocator);
    var weights = std.ArrayListUnmanaged(i32).empty;
    errdefer weights.deinit(allocator);

    const scale = @as(f64, @floatFromInt(source_size)) / @as(f64, @floatFromInt(target_size));
    const filter_scale = @max(scale, 1.0);
    const support = 2.0 * filter_scale;
    for (0..target_size) |target| {
        const center = (@as(f64, @floatFromInt(target)) + 0.5) * scale;
        var first: isize = @intFromFloat(center - support + 0.5);
        var last: isize = @intFromFloat(center + support + 0.5);
        first = @max(first, 0);
        last = @min(last, @as(isize, @intCast(source_size)));
        if (last <= first) return error.InvalidImageDimensions;
        starts[target] = @intCast(first);
        offsets[target] = weights.items.len;
        const float_start = float_weights.items.len;
        var total: f64 = 0.0;
        var source = first;
        while (source < last) : (source += 1) {
            const distance = (@as(f64, @floatFromInt(source)) - center + 0.5) / filter_scale;
            const weight = cubicWeightF64(distance);
            try float_weights.append(allocator, weight);
            total += weight;
        }
        if (total == 0.0 or !std.math.isFinite(total)) return error.InvalidImageDimensions;
        for (float_weights.items[float_start..]) |weight| {
            const normalized = weight / total;
            const scaled = normalized * @as(f64, 1 << pillow_precision_bits);
            const rounded = if (scaled < 0.0) scaled - 0.5 else scaled + 0.5;
            try weights.append(allocator, @intFromFloat(rounded));
        }
    }
    offsets[target_size] = weights.items.len;
    return .{
        .allocator = allocator,
        .starts = starts,
        .offsets = offsets,
        .weights = try weights.toOwnedSlice(allocator),
    };
}

const pillow_precision_bits = 22;

fn clipPillowAccumulator(value: i64) u8 {
    return @intCast(std.math.clamp(value >> pillow_precision_bits, 0, 255));
}

/// Pillow resizes 8-bit images in two passes, materializing an 8-bit
/// horizontal intermediate before the vertical pass. Preserving that
/// quantization boundary is required for Transformers pixel parity.
fn preprocessDecodedRectPillowBicubic(
    allocator: std.mem.Allocator,
    img: ImageU8,
    result: []f32,
    target_width: usize,
    target_height: usize,
    mean: [3]f32,
    std_dev: [3]f32,
    rescale_factor: f32,
) !void {
    var horizontal_axis = try buildPillowBicubicAxis(allocator, img.width, target_width);
    defer horizontal_axis.deinit();
    var vertical_axis = try buildPillowBicubicAxis(allocator, img.height, target_height);
    defer vertical_axis.deinit();

    const horizontal_pixels = std.math.mul(usize, target_width, img.height) catch
        return error.InvalidImageDimensions;
    const horizontal_len = std.math.mul(usize, horizontal_pixels, 3) catch
        return error.InvalidImageDimensions;
    const horizontal = try allocator.alloc(u8, horizontal_len);
    defer allocator.free(horizontal);
    for (0..img.height) |source_y| {
        for (0..target_width) |target_x| {
            const start = horizontal_axis.starts[target_x];
            const begin = horizontal_axis.offsets[target_x];
            const end = horizontal_axis.offsets[target_x + 1];
            for (0..3) |channel| {
                var value: i64 = 1 << (pillow_precision_bits - 1);
                for (horizontal_axis.weights[begin..end], 0..) |weight, offset| {
                    value += @as(i64, @intFromFloat(pixelAt(img, @intCast(start + offset), @intCast(source_y), channel))) * weight;
                }
                horizontal[(source_y * target_width + target_x) * 3 + channel] = clipPillowAccumulator(value);
            }
        }
    }

    for (0..target_height) |target_y| {
        const start = vertical_axis.starts[target_y];
        const begin = vertical_axis.offsets[target_y];
        const end = vertical_axis.offsets[target_y + 1];
        for (0..target_width) |target_x| {
            for (0..3) |channel| {
                var value: i64 = 1 << (pillow_precision_bits - 1);
                for (vertical_axis.weights[begin..end], 0..) |weight, offset| {
                    const source_y = start + offset;
                    value += @as(i64, horizontal[(source_y * target_width + target_x) * 3 + channel]) * weight;
                }
                const sample: f32 = @floatFromInt(clipPillowAccumulator(value));
                result[channel * target_height * target_width + target_y * target_width + target_x] =
                    normalizeSample(sample, mean[channel], std_dev[channel], rescale_factor);
            }
        }
    }
}

fn cubicWeight(x: f32) f32 {
    return @floatCast(cubicWeightF64(x));
}

fn cubicWeightF64(x: f64) f64 {
    const a: f64 = -0.5;
    const t = @abs(x);
    if (t <= 1.0) {
        return ((a + 2.0) * t - (a + 3.0)) * t * t + 1.0;
    }
    if (t < 2.0) {
        return (((a * t) - (5.0 * a)) * t + (8.0 * a)) * t - (4.0 * a);
    }
    return 0.0;
}

fn clampIndex(idx: i32, dim: u32) u32 {
    if (idx <= 0) return 0;
    const max: i32 = @intCast(dim - 1);
    if (idx >= max) return @intCast(max);
    return @intCast(idx);
}

fn normalizeSample(value: f32, mean: f32, std_dev: f32, rescale_factor: f32) f32 {
    return (value * rescale_factor - mean) / std_dev;
}

test "preprocess decoded rgb image returns chw output" {
    const alloc = std.testing.allocator;
    const rgb = [_]u8{
        255, 0,   0,
        0,   255, 0,
        0,   0,   255,
        255, 255, 255,
    };
    const out = try preprocessDecoded(alloc, .{
        .data = &rgb,
        .width = 2,
        .height = 2,
        .format = .rgb8,
    }, 2, .{ 0.0, 0.0, 0.0 }, .{ 1.0, 1.0, 1.0 });
    defer alloc.free(out);

    try std.testing.expectEqual(@as(usize, 12), out.len);
    try std.testing.expectEqual(@as(f32, 1.0), out[0]);
    try std.testing.expectEqual(@as(f32, 0.0), out[1]);
}

test "preprocess decoded rgba ignores alpha" {
    const alloc = std.testing.allocator;
    const rgba = [_]u8{ 10, 20, 30, 40 };
    const out = try preprocessDecoded(alloc, .{
        .data = &rgba,
        .width = 1,
        .height = 1,
        .format = .rgba8,
    }, 1, .{ 0.0, 0.0, 0.0 }, .{ 1.0, 1.0, 1.0 });
    defer alloc.free(out);

    try std.testing.expectEqual(@as(usize, 3), out.len);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0 / 255.0), out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 20.0 / 255.0), out[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 30.0 / 255.0), out[2], 1e-6);
}

test "preprocess decoded rect returns non-square chw output" {
    const alloc = std.testing.allocator;
    const rgb = [_]u8{
        255, 0,   0,
        0,   255, 0,
    };
    const out = try preprocessDecodedRect(alloc, .{
        .data = &rgb,
        .width = 2,
        .height = 1,
        .format = .rgb8,
    }, 4, 2, .{ 0.0, 0.0, 0.0 }, .{ 1.0, 1.0, 1.0 });
    defer alloc.free(out);

    try std.testing.expectEqual(@as(usize, 3 * 4 * 2), out.len);
}

test "computeAspectFitWidth preserves ratio and caps width" {
    try std.testing.expectEqual(@as(u32, 100), computeAspectFitWidth(200, 96, 48, 320));
    try std.testing.expectEqual(@as(u32, 320), computeAspectFitWidth(2000, 48, 48, 320));
    try std.testing.expectEqual(@as(u32, 1), computeAspectFitWidth(0, 48, 48, 320));
}

test "preprocess decoded keep aspect pads remaining width" {
    const alloc = std.testing.allocator;
    const rgb = [_]u8{
        255, 0,   0,
        0,   255, 0,
    };
    const out = try preprocessDecodedRectKeepAspectPadRightWithResample(
        alloc,
        .{
            .data = &rgb,
            .width = 2,
            .height = 1,
            .format = .rgb8,
        },
        6,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        .nearest,
        .{ 255, 255, 255 },
    );
    defer alloc.free(out);

    try std.testing.expectEqual(@as(usize, 3 * 6 * 2), out.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[5], 1e-6);
}

test "preprocess decoded rect scaled applies explicit rescale factor" {
    const alloc = std.testing.allocator;
    const rgb = [_]u8{ 128, 64, 32 };
    const out = try preprocessDecodedRectScaledWithResample(
        alloc,
        .{
            .data = &rgb,
            .width = 1,
            .height = 1,
            .format = .rgb8,
        },
        1,
        1,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        1.0,
        .nearest,
    );
    defer alloc.free(out);

    try std.testing.expectApproxEqAbs(@as(f32, 128.0), out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 64.0), out[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32.0), out[2], 1e-6);
}

test "bicubic downsampling matches Pillow RGB pixels" {
    const alloc = std.testing.allocator;
    var rgb: [8 * 6 * 3]u8 = undefined;
    for (0..6) |y| {
        for (0..8) |x| {
            const offset = (y * 8 + x) * 3;
            rgb[offset] = @intCast((y * 8 + x) * 5);
            rgb[offset + 1] = @intCast(x * 30);
            rgb[offset + 2] = @intCast(y * 40);
        }
    }

    const out = try preprocessDecodedRectScaledWithResample(
        alloc,
        .{
            .data = &rgb,
            .width = 8,
            .height = 6,
            .format = .rgb8,
        },
        3,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        1.0,
        .pillow_bicubic,
    );
    defer alloc.free(out);

    // Pillow 12.1.0: Image.fromarray(rgb).resize((3, 2), Image.Resampling.BICUBIC).
    const expected = [_]f32{
        49, 62,  76,  159, 172, 186,
        27, 105, 183, 27,  105, 183,
        45, 45,  45,  155, 155, 155,
    };
    try std.testing.expectEqualSlices(f32, &expected, out);

    const legacy = try preprocessDecodedRectScaledWithResample(
        alloc,
        .{
            .data = &rgb,
            .width = 8,
            .height = 6,
            .format = .rgb8,
        },
        3,
        2,
        .{ 0.0, 0.0, 0.0 },
        .{ 1.0, 1.0, 1.0 },
        1.0,
        .bicubic,
    );
    defer alloc.free(legacy);
    try std.testing.expect(!std.mem.eql(f32, legacy, out));
}
