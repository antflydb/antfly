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
const apple = @import("backends/apple.zig");
const prep = @import("preparation.zig");
extern fn av_test_device_create() ?*anyopaque;
extern fn av_test_device_destroy(*anyopaque) void;
extern fn av_test_p010_surface_create([*]const u8, u32, u32, c_int) ?*anyopaque;
const clip = @embedFile("../testdata/prepare-sdr.mp4");
const a = std.testing.allocator;
test "video Metal P010 CVPixelBuffer imports preserve ten-bit samples and ownership" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try prep.Metal.init(device);
    defer metal.deinit();
    const width = 32;
    const height = 24;
    var bytes: [width * height * 3]u8 = undefined;
    for (0..bytes.len / 2) |index| {
        // Independent right-aligned reference below includes clipping extremes
        // and values between the eight-bit grid, with non-neutral chroma.
        const value: u16 = @intCast((index * 37 + 13) % 1024);
        std.mem.writeInt(u16, bytes[index * 2 ..][0..2], value << 6, .little);
    }
    var aligned = bytes;
    for (0..aligned.len / 2) |index| {
        const value = std.mem.readInt(u16, aligned[index * 2 ..][0..2], .little) >> 6;
        std.mem.writeInt(u16, aligned[index * 2 ..][0..2], value, .little);
    }
    for ([_]bool{ false, true }) |full| for (std.enums.values(prep.Rotation)) |rotation| for ([_]media.webm.Crop{ .{}, .{ .left = 1, .top = 1, .right = 2, .bottom = 3 } }) |crop| {
        const pixel = av_test_p010_surface_create(&bytes, width, height, @intFromBool(full)) orelse return error.SurfaceMappingFailed;
        var surface = apple.Surface.fromBorrowed(pixel) catch |err| {
            av_test_surface_destroy(pixel);
            return err;
        };
        av_test_surface_destroy(pixel);
        var surface_live = true;
        defer if (surface_live) surface.deinit();
        const options = prep.Options{ .width = 48, .height = 48, .matrix = if (full) .bt601 else .bt709, .rotation = rotation, .crop = crop };
        const reference_host = prep.HostSurface{ .width = width, .height = height, .bit_depth = 10, .format = if (full) .nv12_full else .nv12_video, .planes = .{
            .{ .bytes = aligned[0 .. width * height * 2], .stride = width * 2, .width = width, .height = height },
            .{ .bytes = aligned[width * height * 2 ..], .stride = width * 2, .width = width / 2, .height = height / 2 },
        } };
        const expected = try prep.referenceHost(a, reference_host, options, .{});
        defer a.free(expected);
        const mapped = try prep.reference(a, &surface, options, .{});
        defer a.free(mapped);
        try std.testing.expectEqualSlices(f32, expected, mapped);
        var staged = blk: {
            var mapping = try surface.map();
            defer mapping.deinit();
            break :blk try metal.submitHost(a, .{ .width = width, .height = height, .format = surface.format, .bit_depth = 10, .planes = mapping.planes }, options, .{});
        };
        defer staged.deinit();
        var result = try metal.submit(a, &surface, options, .{});
        defer result.deinit();
        surface.deinit();
        surface_live = false;
        try result.wait(std.testing.io, .{});
        const actual = try result.readback(a);
        defer a.free(actual);
        for (actual, expected) |got, want| try std.testing.expectApproxEqAbs(want, got, 2.0 / 255.0 + 1e-6);
        try staged.wait(std.testing.io, .{});
        const staged_values = try staged.readback(a);
        defer a.free(staged_values);
        for (staged_values, expected) |got, want| try std.testing.expectApproxEqAbs(want, got, 2.0 / 255.0 + 1e-6);
    };
}
test "video Metal quantized bicubic patch preparation matches CPU with rotation and color policies" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try prep.Metal.init(device);
    defer metal.deinit();
    var source = media.source.Source{ .allocator = a, .identity = "original-sdr-prepare-v1", .storage = .{ .borrowed = clip } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var batch = try apple.decodeSelected(a, &reader, &.{0}, .{ .require_hardware = false });
    defer batch.deinit();
    inline for (std.meta.tags(prep.Rotation)) |rotation| {
        inline for ([_]prep.Matrix{ .bt601, .bt709 }) |matrix| {
            inline for (std.meta.tags(prep.RgbRounding)) |rounding| {
                const options = prep.Options{ .width = 96, .height = 48, .matrix = matrix, .rotation = rotation, .rgb_rounding = rounding };
                const expected = try prep.reference(a, &batch.frames[0].surface, options, .{});
                defer a.free(expected);
                var result = try metal.submit(a, &batch.frames[0].surface, options, .{});
                defer result.deinit();
                try std.testing.expectEqual(@as(usize, 0), result.rgba_staging_bytes);
                try result.wait(std.testing.io, .{});
                _ = try result.buffer();
                const actual = try result.readback(a);
                defer a.free(actual);
                try std.testing.expectEqual(expected.len, actual.len);
                try std.testing.expectEqual(@as(usize, 2), result.geometry.soft_tokens);
                for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 2.0 / 255.0 + 1e-6);
            }
        }
    }
}
test "video Metal imports and in-flight preparation outlive decoder batch and queue owner" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var source = media.source.Source{ .allocator = a, .identity = "original-sdr-prepare-v1", .storage = .{ .borrowed = clip } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var batch = try apple.decodeSelected(a, &reader, &.{0}, .{ .require_hardware = false });
    var cache = try apple.MetalCache.init(device);
    var imported = try cache.import(&batch.frames[0].surface);
    defer imported.deinit();
    cache.deinit();
    try std.testing.expectError(error.InvalidSurfacePlane, imported.texture(2));
    var metal = try prep.Metal.init(device);
    const options = prep.Options{ .width = 48, .height = 48, .matrix = .bt601 };
    var result = try metal.submit(a, &batch.frames[0].surface, options, .{});
    defer result.deinit();
    metal.deinit();
    batch.deinit();
    _ = try imported.texture(0);
    _ = try imported.texture(1);
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.DeadlineExceeded;
        }
    };
    try std.testing.expectError(error.DeadlineExceeded, result.wait(std.testing.io, .{ .check_fn = Cancel.check }));
    try result.wait(std.testing.io, .{});
    const values = try result.readback(a);
    defer a.free(values);
    try std.testing.expectEqual(@as(usize, 48 * 48 * 3), values.len);
}
test "video preparation validates budgets and cancellation before allocating" {
    const fake = apple.Surface{ .handle = @ptrFromInt(1), .width = 160, .height = 96, .format = .nv12_video };
    try std.testing.expectError(error.InvalidVideoGeometry, prep.reference(a, &fake, .{ .width = 47, .height = 48, .matrix = .bt601 }, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, prep.reference(a, &fake, .{ .width = 48, .height = 48, .matrix = .bt601, .max_scratch_bytes = 1 }, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, prep.reference(a, &fake, .{ .width = 96, .height = 48, .matrix = .bt601, .max_soft_tokens = 1 }, .{}));
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, prep.reference(a, &fake, .{ .width = 48, .height = 48, .matrix = .bt601 }, .{ .check_fn = Cancel.check }));
}

fn hostFixture() prep.HostSurface {
    const bytes = @embedFile("../testdata/decode-bframes.nv12")[0 .. 32 * 24 * 3 / 2];
    return .{ .width = 32, .height = 24, .format = .nv12_video, .planes = .{
        .{ .bytes = bytes[0 .. 32 * 24], .stride = 32, .width = 32, .height = 24 },
        .{ .bytes = bytes[32 * 24 ..], .stride = 32, .width = 16, .height = 12 },
    } };
}
fn hostAllocations(allocator: std.mem.Allocator) !void {
    const values = try prep.referenceHost(allocator, hostFixture(), .{ .width = 48, .height = 48, .matrix = .bt601 }, .{});
    allocator.free(values);
}
test "video portable borrowed host NV12 validates strides and allocation failures" {
    const values = try prep.referenceHost(a, hostFixture(), .{ .width = 48, .height = 48, .matrix = .bt601 }, .{});
    defer a.free(values);
    try std.testing.expectEqual(@as(usize, 48 * 48 * 3), values.len);
    for (values) |value| try std.testing.expect(std.math.isFinite(value) and value >= -1 and value <= 1);
    var invalid = hostFixture();
    invalid.planes[1].stride = 1;
    try std.testing.expectError(error.UnsupportedSurfaceFormat, prep.referenceHost(a, invalid, .{ .width = 48, .height = 48, .matrix = .bt601 }, .{}));
    invalid.width = std.math.maxInt(u32);
    try std.testing.expectError(error.ResourceLimitExceeded, invalid.validate());
    try std.testing.checkAllAllocationFailures(a, hostAllocations, .{});
}
test "video portable full-range NV12 neutral colors preserve patch layout" {
    var y: [4]u8 = .{ 0, 255, 0, 255 };
    var uv: [2]u8 = .{ 128, 128 };
    const host = prep.HostSurface{ .width = 2, .height = 2, .format = .nv12_full, .planes = .{
        .{ .bytes = &y, .stride = 2, .width = 2, .height = 2 },
        .{ .bytes = &uv, .stride = 2, .width = 1, .height = 1 },
    } };
    const values = try prep.referenceHost(a, host, .{ .width = 48, .height = 48, .matrix = .bt709, .centered = false }, .{});
    defer a.free(values);
    // Bicubic endpoint clipping preserves solid extremes and neutral channels.
    try std.testing.expectEqual(@as(f32, 0), values[0]);
    const last = (2 * 768) + (15 * 3);
    try std.testing.expectEqual(@as(f32, 1), values[last]);
    for (0..values.len / 3) |i| {
        try std.testing.expectEqual(values[i * 3], values[i * 3 + 1]);
        try std.testing.expectEqual(values[i * 3], values[i * 3 + 2]);
    }
}

extern fn av_test_surface_create([*]const u8, u32, u32, c_int) ?*anyopaque;
extern fn av_test_surface_destroy(*anyopaque) void;
test "video Metal borrowed full-range planes match portable reference without decoding" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try prep.Metal.init(device);
    defer metal.deinit();
    // Two colored chroma cells, not only neutral grayscale.
    const bytes = [_]u8{ 0, 30, 70, 100, 140, 170, 210, 255, 60, 200, 190, 80 };
    const pixel = av_test_surface_create(&bytes, 4, 2, 1) orelse return error.SurfaceMappingFailed;
    defer av_test_surface_destroy(pixel);
    var surface = try apple.Surface.fromBorrowed(pixel);
    defer surface.deinit();
    const host = prep.HostSurface{ .width = 4, .height = 2, .format = .nv12_full, .planes = .{
        .{ .bytes = bytes[0..8], .stride = 4, .width = 4, .height = 2 },
        .{ .bytes = bytes[8..], .stride = 4, .width = 2, .height = 1 },
    } };
    inline for ([_]prep.Matrix{ .bt601, .bt709 }) |matrix| {
        const options = prep.Options{ .width = 48, .height = 48, .matrix = matrix, .centered = false };
        const expected = try prep.referenceHost(a, host, options, .{});
        defer a.free(expected);
        var result = try metal.submit(a, &surface, options, .{});
        defer result.deinit();
        try result.wait(std.testing.io, .{});
        const actual = try result.readback(a);
        defer a.free(actual);
        for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 1.0 / 255.0 + 1e-6);
    }
}

test "HDR rendering matches independent float64 RGB8 reference on CPU and Metal" {
    const bytes = @embedFile("../testdata/hdr-10bit-nv12.bin");
    const fixtures = [_][]const u8{
        @embedFile("../testdata/hdr-pq-709-limited.rgba"),   @embedFile("../testdata/hdr-pq-709-full.rgba"),
        @embedFile("../testdata/hdr-pq-2020-limited.rgba"),  @embedFile("../testdata/hdr-pq-2020-full.rgba"),
        @embedFile("../testdata/hdr-hlg-709-limited.rgba"),  @embedFile("../testdata/hdr-hlg-709-full.rgba"),
        @embedFile("../testdata/hdr-hlg-2020-limited.rgba"), @embedFile("../testdata/hdr-hlg-2020-full.rgba"),
    };
    for (fixtures, 0..) |expected_rgba, i| {
        const host = prep.HostSurface{ .width = 48, .height = 48, .bit_depth = 10, .format = if (i % 2 == 0) .nv12_video else .nv12_full, .planes = .{
            .{ .bytes = bytes[0 .. 48 * 48 * 2], .stride = 96, .width = 48, .height = 48 },
            .{ .bytes = bytes[48 * 48 * 2 ..], .stride = 96, .width = 24, .height = 24 },
        } };
        const options = prep.Options{ .width = 48, .height = 48, .matrix = if ((i / 2) % 2 == 0) .bt709 else .bt2020_ncl, .tone_mapping = if (i < 4) .pq_reinhard_100 else .hlg_reinhard_100, .torchvision = true };
        var rgb_options = options;
        rgb_options.matrix = .bt709;
        rgb_options.tone_mapping = .none;
        const oracle = try prep.referenceRgba(a, expected_rgba, 48, 48, rgb_options, .{});
        defer a.free(oracle);
        const native = try prep.referenceHost(a, host, options, .{});
        defer a.free(native);
        try std.testing.expectEqualSlices(f32, oracle, native);
    }
    // CPU evidence remains valid on Linux. Device qualification requires a real
    // Metal device and is run explicitly on the GPU host.
    if (comptime @import("builtin").os.tag == .macos) {
        const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
        defer av_test_device_destroy(device);
        var metal = try prep.Metal.init(device);
        defer metal.deinit();
        for (fixtures, 0..) |expected_rgba, i| {
            const host = prep.HostSurface{ .width = 48, .height = 48, .bit_depth = 10, .format = if (i % 2 == 0) .nv12_video else .nv12_full, .planes = .{
                .{ .bytes = bytes[0 .. 48 * 48 * 2], .stride = 96, .width = 48, .height = 48 },
                .{ .bytes = bytes[48 * 48 * 2 ..], .stride = 96, .width = 24, .height = 24 },
            } };
            const options = prep.Options{ .width = 48, .height = 48, .matrix = if ((i / 2) % 2 == 0) .bt709 else .bt2020_ncl, .tone_mapping = if (i < 4) .pq_reinhard_100 else .hlg_reinhard_100, .torchvision = true };
            var rgb_options = options;
            rgb_options.matrix = .bt709;
            rgb_options.tone_mapping = .none;
            const oracle = try prep.referenceRgba(a, expected_rgba, 48, 48, rgb_options, .{});
            defer a.free(oracle);
            var prepared = try metal.submitHost(a, host, options, .{});
            defer prepared.deinit();
            try prepared.wait(std.testing.io, .{});
            const result = try prepared.readback(a);
            defer a.free(result);
            // GPU fused normalization may differ by a few float32 ULPs.
            // This tolerance is far smaller than one RGB8 code step.
            for (oracle, result) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 0.000001);
            if (i == 0) try std.testing.expect(prepared.coefficient_staging_bytes > 16385 * 4) else try std.testing.expectEqual(@as(usize, if (i == 4) 16385 * 4 else 0), prepared.coefficient_staging_bytes);
        }
    }
}

test "HDR Metal cache admission and submitted output survive preparer teardown" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    const bytes = @embedFile("../testdata/hdr-10bit-nv12.bin");
    const host = prep.HostSurface{ .width = 48, .height = 48, .bit_depth = 10, .format = .nv12_video, .planes = .{
        .{ .bytes = bytes[0 .. 48 * 48 * 2], .stride = 96, .width = 48, .height = 48 },
        .{ .bytes = bytes[48 * 48 * 2 ..], .stride = 96, .width = 24, .height = 24 },
    } };
    const options = prep.Options{ .width = 48, .height = 48, .matrix = .bt2020_ncl, .tone_mapping = .pq_reinhard_100, .torchvision = true };
    var pool = media.admission.Pool{ .limits = .{ .device_bytes = 4096 } };
    {
        var bounded = try prep.Metal.init(device);
        defer bounded.deinit();
        try bounded.admit(&pool, 4096);
        var sdr = options;
        sdr.tone_mapping = .none;
        sdr.matrix = .bt709;
        var warm = try bounded.submitHost(a, host, sdr, .{});
        defer warm.deinit();
        try warm.wait(std.testing.io, .{});
        const cached = bounded.coefficient_bytes;
        try std.testing.expectError(error.ResourceLimitExceeded, bounded.submitHost(a, host, options, .{}));
        try std.testing.expectEqual(cached, bounded.coefficient_bytes);
        try std.testing.expectEqual(@as(u64, 4096), pool.snapshot().device_bytes);
    }
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().device_bytes);
    var metal = try prep.Metal.init(device);
    var prepared = metal.submitHost(a, host, options, .{}) catch |err| {
        metal.deinit();
        return err;
    };
    defer prepared.deinit();
    metal.deinit();
    try prepared.wait(std.testing.io, .{});
    const actual = try prepared.readback(a);
    defer a.free(actual);
    const expected = try prep.referenceHost(a, host, options, .{});
    defer a.free(expected);
    for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.000001);
}

test "video Metal Q16 integer host preparation matches portable RGB across supported SDR matrices and ranges" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try prep.Metal.init(device);
    defer metal.deinit();
    var y: [26 * 16]u8 = undefined;
    var uv: [26 * 8]u8 = undefined;
    for (&y, 0..) |*value, i| value.* = @intCast((i * 37 + 11) % 256);
    for (&uv, 0..) |*value, i| value.* = @intCast((i * 61 + 7) % 256);
    for ([_]prep.Matrix{ .bt601, .bt709 }) |matrix| for ([_]bool{ false, true }) |full| {
        const host = prep.HostSurface{ .width = 26, .height = 16, .format = if (full) .nv12_full else .nv12_video, .planes = .{
            .{ .bytes = &y, .width = 26, .height = 16, .stride = 26 },
            .{ .bytes = &uv, .width = 13, .height = 8, .stride = 26 },
        } };
        const options = prep.Options{ .width = 96, .height = 48, .matrix = matrix, .rgb_rounding = .q16_luma, .torchvision = true };
        const expected = try prep.referenceHost(a, host, options, .{});
        defer a.free(expected);
        var result = try metal.submitHost(a, host, options, .{});
        defer result.deinit();
        try result.wait(std.testing.io, .{});
        const actual = try result.readback(a);
        defer a.free(actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
    };
}

test "video Metal crop precedes rotation for native host and RGBA staging" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try prep.Metal.init(device);
    defer metal.deinit();
    var y: [26 * 16]u8 = undefined;
    var uv: [26 * 8]u8 = undefined;
    var rgba: [26 * 16 * 4]u8 = undefined;
    for (&y, 0..) |*v, i| v.* = @intCast((i * 37 + 11) % 256);
    for (&uv, 0..) |*v, i| v.* = @intCast((i * 61 + 7) % 256);
    for (&rgba, 0..) |*v, i| v.* = @intCast((i * 19 + 7) % 256);
    const host = prep.HostSurface{ .width = 26, .height = 16, .format = .nv12_video, .planes = .{
        .{ .bytes = &y, .width = 26, .height = 16, .stride = 26 }, .{ .bytes = &uv, .width = 13, .height = 8, .stride = 26 },
    } };
    for (std.enums.values(prep.Rotation)) |rotation| {
        const options = prep.Options{ .width = 96, .height = 48, .matrix = .bt601, .rgb_rounding = .q16_luma, .torchvision = true, .rotation = rotation, .crop = .{ .left = 1, .top = 1, .right = 3, .bottom = 2 } };
        const expected = try prep.referenceHost(a, host, options, .{});
        defer a.free(expected);
        var result = try metal.submitHost(a, host, options, .{});
        defer result.deinit();
        try result.wait(std.testing.io, .{});
        const actual = try result.readback(a);
        defer a.free(actual);
        for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
        const rgba_expected = try prep.referenceRgba(a, &rgba, 26, 16, options, .{});
        defer a.free(rgba_expected);
        var rgba_result = try metal.submitRgba(a, &rgba, 26, 16, options, .{});
        defer rgba_result.deinit();
        try rgba_result.wait(std.testing.io, .{});
        try std.testing.expectEqual(@as(usize, 0), rgba_result.coefficient_staging_bytes);
        const rgba_actual = try rgba_result.readback(a);
        defer a.free(rgba_actual);
        for (rgba_expected, rgba_actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
    }
}

test "video Metal monochrome code samples preserve independent CPU RGB policy" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try prep.Metal.init(device);
    defer metal.deinit();
    for ([_]u8{ 8, 10, 12 }) |depth| {
        const bytes: usize = if (depth == 8) 1 else 2;
        const samples = try a.alloc(u8, 48 * 48 * bytes);
        defer a.free(samples);
        for (0..48 * 48) |i| {
            const value: u16 = @intCast((i * 37 + 11) % (@as(usize, 1) << @as(u4, @intCast(depth))));
            if (depth == 8) samples[i] = @intCast(value) else std.mem.writeInt(u16, samples[i * 2 ..][0..2], value, .little);
        }
        for ([_]bool{ false, true }) |full| {
            const host = prep.HostSurface{ .width = 48, .height = 48, .bit_depth = depth, .chroma_format = 0, .format = if (full) .nv12_full else .nv12_video, .planes = .{
                .{ .bytes = samples, .width = 48, .height = 48, .stride = 48 * bytes }, .{ .bytes = &.{}, .width = 0, .height = 0, .stride = 0 },
            } };
            for ([_]prep.Rotation{ .none, .clockwise90 }) |rotation| {
                const options = prep.Options{ .width = 96, .height = 48, .matrix = .bt601, .rgb_rounding = .gray_samples, .torchvision = true, .rotation = rotation };
                const expected = try prep.referenceHost(a, host, options, .{});
                defer a.free(expected);
                var prepared = try metal.submitHost(a, host, options, .{});
                defer prepared.deinit();
                try prepared.wait(std.testing.io, .{});
                const actual = try prepared.readback(a);
                defer a.free(actual);
                try std.testing.expectEqual(expected.len, actual.len);
                for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
            }
        }
    }
}
