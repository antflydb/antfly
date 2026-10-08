// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const apple = @import("backends/apple.zig");
const prep = @import("preparation.zig");
extern fn av_test_device_create() ?*anyopaque;
extern fn av_test_device_destroy(*anyopaque) void;
const clip = @embedFile("../testdata/prepare-sdr.mp4");
const a = std.testing.allocator;
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
        inline for (std.meta.tags(prep.Matrix)) |matrix| {
            const options = prep.Options{ .width = 96, .height = 48, .matrix = matrix, .rotation = rotation };
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
    inline for (std.meta.tags(prep.Matrix)) |matrix| {
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
