// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const video = @import("mod.zig");
const a = std.testing.allocator;
extern fn av_test_device_create() ?*anyopaque;
extern fn av_test_device_destroy(*anyopaque) void;
extern fn av_test_native_texture(*anyopaque, [*]const u8, u32, u32, usize, u32, u32) ?*anyopaque;
extern fn av_test_native_texture_destroy(*anyopaque) void;
const clip = @embedFile("../testdata/mjpeg.mov");
fn source() media.source.Source {
    return .{ .allocator = a, .identity = "mjpeg-metal-v1", .storage = .{ .borrowed = clip } };
}
const clips = [_]video.windows.Window{ .{ .interval = .{ .start = 0, .end = 16384 }, .step = 4096 }, .{ .interval = .{ .start = 8192, .end = 32768 }, .step = 4096 } };
const job_options = video.mjpeg_metal.Options{ .preparation = .{ .width = 48, .height = 48, .matrix = .bt709 } };
fn compare(expected: []const f32, actual: []const f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-6);
}
test "video Metal RGBA preparation matches CPU for all rotations and centering modes" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    const bytes = try a.alloc(u8, 160 * 96 * 4);
    defer a.free(bytes);
    for (0..160 * 96) |i| {
        bytes[i * 4] = @truncate(i * 7);
        bytes[i * 4 + 1] = @truncate(i * 13);
        bytes[i * 4 + 2] = @truncate(i * 23);
        bytes[i * 4 + 3] = @truncate(i);
    }
    inline for (std.meta.tags(video.preparation.Rotation)) |rotation| {
        inline for (.{ false, true }) |centered| {
            const options = video.preparation.Options{ .width = 96, .height = 48, .matrix = .bt601, .rotation = rotation, .centered = centered };
            const expected = try video.preparation.referenceRgba(a, bytes, 160, 96, options, .{});
            defer a.free(expected);
            var result = try metal.submitRgba(a, bytes, 160, 96, options, .{});
            defer result.deinit();
            try std.testing.expectEqual(bytes.len, result.rgba_staging_bytes);
            if (!centered) try std.testing.expect(result.coefficient_staging_bytes > 0) else try std.testing.expectEqual(@as(usize, 0), result.coefficient_staging_bytes);
            try result.wait(std.testing.io, .{});
            try result.releaseSource();
            try result.releaseSource();
            _ = try result.buffer();
            const actual = try result.readback(a);
            defer a.free(actual);
            try compare(expected, actual);
        }
    }
}
test "video RGBA submission owns pixels and completed buffers outlive preparation owner" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var frame = try video.mjpeg.decodeFrame(a, &reader, 0, .{});
    const expected = try video.preparation.referenceRgba(a, frame.rgba, frame.width, frame.height, job_options.preparation, .{});
    defer a.free(expected);
    var metal = try video.preparation.Metal.init(device);
    var prepared = try metal.submitRgba(a, frame.rgba, frame.width, frame.height, job_options.preparation, .{});
    defer prepared.deinit();
    // Overwrite then free producer storage immediately; the command owns its copy.
    @memset(frame.rgba, 0);
    frame.deinit();
    metal.deinit();
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.DeadlineExceeded;
        }
    };
    try std.testing.expectError(error.DeadlineExceeded, prepared.wait(std.testing.io, .{ .check_fn = Cancel.check }));
    try prepared.wait(std.testing.io, .{});
    try prepared.releaseSource();
    const actual = try prepared.readback(a);
    defer a.free(actual);
    try compare(expected, actual);
}
test "video MJPEG Metal windows reuse pictures bound queues and preserve CPU patches after teardown" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    inline for (.{ @as(usize, 1), @as(usize, 2), @as(usize, 8) }) |depth| {
        var src = source();
        var metal = try video.preparation.Metal.init(device);
        var jobs = blk: {
            var reader = try media.mp4.Reader.init(a, &src, .{});
            defer reader.deinit();
            var options = job_options;
            options.queue_depth = depth;
            var jobs = try video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options);
            errdefer jobs.deinit();
            var baseline_staging: u64 = 0;
            var baseline_packets: usize = 0;
            for (clips) |window| {
                var unshared = try video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &.{window}, &metal, options);
                defer unshared.deinit();
                baseline_staging += unshared.rgba_staging_bytes;
                baseline_packets += unshared.decoded_packets;
            }
            try std.testing.expectEqual(@as(usize, 10), baseline_packets);
            try std.testing.expectEqual(@as(u64, 122880), baseline_staging);
            var cpu = try video.mjpeg.prepareWindows(a, &reader, &clips, .{ .preparation = options.preparation });
            defer cpu.deinit();
            for (jobs.frames, 0..) |*frame, i| {
                const actual = try frame.readback(a);
                defer a.free(actual);
                try compare(try cpu.frame(i), actual);
            }
            try std.testing.expectEqualSlices(i64, cpu.plan.unique_pts, jobs.plan.unique_pts);
            try std.testing.expectEqualSlices(usize, cpu.plan.references, jobs.plan.references);
            break :blk jobs;
        };
        defer jobs.deinit();
        metal.deinit();
        try std.testing.expectEqual(@as(usize, 0), src.retained_bytes);
        try std.testing.expectEqual(@as(usize, 8), jobs.decoded_packets);
        try std.testing.expectEqual(@as(usize, 2), jobs.reusedSelections());
        try std.testing.expectEqual(depth, jobs.queue_high_water);
        try std.testing.expectEqual(@as(u64, 8 * 64 * 48 * 4), jobs.rgba_staging_bytes);
        try std.testing.expectEqual(@as(u64, depth * 64 * 48 * 4), jobs.inflight_staging_high_water);
        try std.testing.expectEqual(@as(usize, 8 * 48 * 48 * 3 * 4), jobs.output_bytes);
        try std.testing.expectEqual((try jobs.window(0))[2], (try jobs.window(1))[0]);
        for (jobs.frames) |*frame| {
            try frame.releaseSource();
            _ = try frame.buffer();
        }
    }
}
test "video RGBA admission and late MJPEG Metal cancellation release work and allow retry" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    var frame = try video.mjpeg.decodeFrame(a, &reader, 0, .{});
    defer frame.deinit();
    try std.testing.expectError(error.InvalidVideoGeometry, metal.submitRgba(a, frame.rgba[0..1], frame.width, frame.height, job_options.preparation, .{}));
    var prepare = job_options.preparation;
    prepare.max_host_staging_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, metal.submitRgba(a, frame.rgba, frame.width, frame.height, prepare, .{}));
    inline for (0..4) |case| {
        var options = job_options;
        switch (case) {
            0 => options.queue_depth = 9,
            1 => options.max_total_staging_bytes = 1,
            2 => options.max_inflight_staging_bytes = 1,
            3 => options.max_output_bytes = 1,
            else => unreachable,
        }
        const reads = src.reads;
        try std.testing.expectError(error.ResourceLimitExceeded, video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options));
        try std.testing.expectEqual(reads, src.reads);
    }
    const Cancel = struct {
        source: *const media.source.Source,
        cutoff: u64,
        fn check(ctx: ?*const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(ctx.?));
            if (self.source.total_bytes >= self.cutoff) return error.Cancelled;
        }
    };
    var cutoff = src.total_bytes;
    for (reader.packets[0..3]) |packet| cutoff += packet.size;
    const cancel = Cancel{ .source = &src, .cutoff = cutoff };
    src.control = .{ .context = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, job_options));
    try std.testing.expectEqual(retained, src.retained_bytes);
    src.control = .{};
    var options = job_options;
    options.max_inflight_staging_bytes = 64 * 48 * 4;
    var jobs = try video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options);
    defer jobs.deinit();
    try std.testing.expectEqual(@as(usize, 1), jobs.queue_high_water);
}
fn allocationCase(allocator: std.mem.Allocator, metal: *video.preparation.Metal) !void {
    metal.clearCoefficients();
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const retained = src.retained_bytes;
    defer std.debug.assert(retained == src.retained_bytes);
    var result = try video.mjpeg_metal.prepareWindows(allocator, std.testing.io, &reader, &clips, metal, job_options);
    defer result.deinit();
}
test "video MJPEG Metal allocation failures fence submitted buffers and release leases" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    var baseline = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try allocationCase(baseline.allocator(), &metal);
    try std.testing.expectEqual(baseline.allocated_bytes, baseline.freed_bytes);
    for (0..baseline.alloc_index) |index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0, .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, allocationCase(failing.allocator(), &metal));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}
test "video non-Apple targets fail closed for RGBA Metal and MJPEG Metal jobs" {
    if (@import("builtin").os.tag == .macos) return error.SkipZigTest;
    var metal: video.preparation.Metal = undefined;
    var reader: media.mp4.Reader = undefined;
    try std.testing.expectError(error.UnsupportedVideoBackend, metal.submitRgba(a, &.{}, 0, 0, job_options.preparation, .{}));
    try std.testing.expectError(error.UnsupportedVideoBackend, video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, job_options));
}

test "video shared output admission denies another request until result destruction" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    var src = source();
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 2 * 1024 * 1024, .device_bytes = 1024 * 1024, .commands = 2 } };
    try metal.admit(&pool, 4096);
    var options = job_options;
    options.decode.max_decode_bytes = 1024 * 1024;
    options.preparation.max_scratch_bytes = 256 * 1024;
    options.max_inflight_staging_bytes = 24576;
    options.admission_pool = &pool;
    var first = try video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options);
    try std.testing.expectEqual(@as(u64, first.output_bytes + 4096), pool.snapshot().device_bytes);
    // A caller retaining completed outputs consumes the same shared capacity.
    pool.limits.device_bytes = first.output_bytes;
    const reads = src.reads;
    try std.testing.expectError(error.SharedAdmissionExceeded, video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options));
    try std.testing.expectEqual(reads, src.reads);
    first.deinit();
    try std.testing.expectEqual(media.admission.Resources{ .device_bytes = 4096 }, pool.snapshot());
    pool.limits.device_bytes = 1024 * 1024;
    var retry = try video.mjpeg_metal.prepareWindows(a, std.testing.io, &reader, &clips, &metal, options);
    retry.deinit();
    try std.testing.expectEqual(media.admission.Resources{ .device_bytes = 4096 }, pool.snapshot());
}

test "video Metal native depth and chroma preparation owns planes and matches CPU" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const device = av_test_device_create() orelse return error.MetalPreparationUnavailable;
    defer av_test_device_destroy(device);
    var metal = try video.preparation.Metal.init(device);
    defer metal.deinit();
    const width = 80;
    const height = 64;
    for ([_]u8{ 8, 10, 14 }) |depth| {
        for ([_]u8{ 1, 2, 3 }) |chroma| {
            const sample_bytes: usize = if (depth == 8) 1 else 2;
            const cw: usize = if (chroma == 3) width else width / 2;
            const ch: usize = if (chroma == 1) height / 2 else height;
            const ys = width * sample_bytes + 8;
            const uvs = cw * 2 * sample_bytes + 8;
            const y = try a.alloc(u8, ys * height);
            defer a.free(y);
            const uv = try a.alloc(u8, uvs * ch);
            defer a.free(uv);
            @memset(y, 0);
            @memset(uv, 0);
            for (0..height) |row| for (0..width) |x| {
                const value: u16 = @intCast((row * 41 + x * 19) % (@as(usize, 1) << @as(u4, @intCast(depth))));
                if (depth == 8) y[row * ys + x] = @intCast(value) else std.mem.writeInt(u16, y[row * ys + x * 2 ..][0..2], value, .little);
            };
            for (0..ch) |row| for (0..cw * 2) |x| {
                const value: u16 = @intCast((row * 23 + x * 61 + 53) % (@as(usize, 1) << @as(u4, @intCast(depth))));
                if (depth == 8) uv[row * uvs + x] = @intCast(value) else std.mem.writeInt(u16, uv[row * uvs + x * 2 ..][0..2], value, .little);
            };
            for (std.enums.values(video.preparation.Rotation)) |rotation| for ([_]bool{ false, true }) |full| {
                const options = video.preparation.Options{ .width = 96, .height = 48, .rotation = rotation, .matrix = if (full) .bt601 else .bt709, .centered = full };
                const host = video.preparation.HostSurface{ .width = width, .height = height, .bit_depth = depth, .chroma_format = chroma, .format = if (full) .nv12_full else .nv12_video, .planes = .{ .{ .bytes = y, .width = width, .height = height, .stride = ys }, .{ .bytes = uv, .width = cw, .height = ch, .stride = uvs } } };
                const expected = try video.preparation.referenceHost(a, host, options, .{});
                defer a.free(expected);
                var result = try metal.submitHost(a, host, options, .{});
                defer result.deinit();
                try std.testing.expectEqual(y.len + uv.len, result.native_staging_bytes);
                const native_y = av_test_native_texture(device, y.ptr, width, height, ys, depth, 1) orelse return error.MetalPreparationUnavailable;
                const native_uv = av_test_native_texture(device, uv.ptr, @intCast(cw), @intCast(ch), uvs, depth, 2) orelse {
                    av_test_native_texture_destroy(native_y);
                    return error.MetalPreparationUnavailable;
                };
                var direct = metal.submitNative(a, .{ .y = native_y, .uv = native_uv, .width = width, .height = height, .bit_depth = depth, .chroma_format = chroma, .full_range = full }, options, .{}) catch |err| {
                    av_test_native_texture_destroy(native_y);
                    av_test_native_texture_destroy(native_uv);
                    return err;
                };
                defer direct.deinit();
                // The submitted command retains both producer textures.
                av_test_native_texture_destroy(native_y);
                av_test_native_texture_destroy(native_uv);
                try std.testing.expectEqual(@as(usize, 0), direct.native_staging_bytes);
                if (rotation == .clockwise270 and full) {
                    // Both submissions must own their source independently of
                    // the producer's host storage by the time submit returns.
                    @memset(y, 0);
                    @memset(uv, 0);
                }
                try direct.wait(std.testing.io, .{});
                try direct.releaseSource();
                const direct_values = try direct.readback(a);
                defer a.free(direct_values);
                try compare(expected, direct_values);
                try result.wait(std.testing.io, .{});
                try result.releaseSource();
                const actual = try result.readback(a);
                defer a.free(actual);
                compare(expected, actual) catch |err| {
                    std.debug.print("native Metal depth {d} chroma {d} rotation {s} full {any}\n", .{ depth, chroma, @tagName(rotation), full });
                    return err;
                };
            };
        }
    }
}
