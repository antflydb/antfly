// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Synchronized decode-to-patches benchmark. No model, no host GPU readback.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("mod.zig");
extern fn av_benchmark_device_create() ?*anyopaque;
extern fn av_benchmark_device_destroy(*anyopaque) void;
extern fn av_benchmark_peak_rss() u64;
const clip = @embedFile("../testdata/mjpeg.mov");
const requests = [_]video.windows.Window{ .{ .interval = .{ .start = 0, .end = 16384 }, .step = 4096 }, .{ .interval = .{ .start = 8192, .end = 32768 }, .step = 4096 } };
pub fn main(init: std.process.Init) !void {
    var output_buffer: [4096]u8 = undefined;
    var writer = std.Io.File.stdout().writer(init.io, &output_buffer);
    const output = &writer.interface;
    const a = init.gpa;
    var arguments = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer arguments.deinit();
    _ = arguments.skip();
    const path_arg = arguments.next();
    const path = if (path_arg) |value| (if (std.mem.eql(u8, value, "-")) null else value) else null;
    const backend = arguments.next() orelse "auto";
    const iterations = try std.fmt.parseInt(usize, arguments.next() orelse "21", 10);
    const target_size = try std.fmt.parseInt(u32, arguments.next() orelse "48", 10);
    if (target_size == 0 or target_size > 2048) return error.InvalidBenchmarkArguments;
    if (iterations < 2 or iterations > 100 or (!std.mem.eql(u8, backend, "auto") and !std.mem.eql(u8, backend, "cpu"))) return error.InvalidBenchmarkArguments;
    if (arguments.next() != null) return error.InvalidBenchmarkArguments;
    var file: ?std.Io.File = null;
    defer if (file) |handle| handle.close(init.io);
    var provider: media.source.FileRange = undefined;
    var input = media.source.Source{ .allocator = a, .identity = "benchmark-mjpeg", .storage = .{ .borrowed = clip } };
    if (path) |name| {
        file = try std.Io.Dir.cwd().openFile(init.io, name, .{});
        provider = .{ .file = file.?, .io = init.io };
        input.storage = .{ .range = .{ .context = &provider, .read_at = media.source.FileRange.readAt, .length = (try file.?.stat(init.io)).size } };
        input.identity = name;
        input.limits.max_total_bytes = 1024 * 1024 * 1024;
    }
    var reader = try media.mp4.Reader.init(a, &input, .{});
    defer reader.deinit();
    var selected = requests;
    if (path != null) {
        var start: i64 = std.math.maxInt(i64);
        var end: i64 = std.math.minInt(i64);
        for (reader.packets) |packet| {
            start = @min(start, packet.pts);
            end = @max(end, try std.math.add(i64, packet.pts, packet.duration));
        }
        const midpoint = start + @divFloor(end - start, 2);
        const step: u64 = @intCast(@max(@divFloor(end - start, 24), 1));
        selected = .{ .{ .interval = .{ .start = start, .end = end }, .step = step }, .{ .interval = .{ .start = midpoint, .end = end }, .step = step } };
    }
    const setup_start = now(init.io);
    var metal: ?video.preparation.Metal = null;
    var device: ?*anyopaque = null;
    if (@import("builtin").os.tag == .macos and !std.mem.eql(u8, backend, "cpu")) {
        device = av_benchmark_device_create() orelse return error.MetalPreparationUnavailable;
        metal = try video.preparation.Metal.init(device.?);
    }
    defer {
        if (metal) |*owner| owner.deinit();
        if (device) |handle| av_benchmark_device_destroy(handle);
    }
    const setup_ns = now(init.io) - setup_start;
    var warm_storage: [99]u64 = undefined;
    const warm = warm_storage[0 .. iterations - 1];
    var frames: usize = 0;
    for (0..iterations) |iteration| {
        const start = now(init.io);
        var gpu_seconds: f64 = 0;
        var decode_peak: usize = 0;
        var dependency_packets: usize = 0;
        var device_bytes: u64 = 0;
        var coefficients: u64 = 0;
        var rgba: u64 = 0;
        if (metal) |*owner| {
            if (reader.track.codec == .avc) {
                var job = try video.apple_jobs.prepareWindows(a, init.io, &reader, &selected, owner, .{ .preparation = .{ .width = target_size, .height = target_size, .matrix = .bt709 } });
                defer job.deinit();
                for (job.frames) |*frame| {
                    gpu_seconds += try frame.gpuSeconds();
                    coefficients += frame.coefficient_staging_bytes;
                }
                frames = job.frames.len;
                dependency_packets = job.decode.submitted_packets;
                device_bytes = owner.deviceAllocatedBytes();
            } else {
                var job = try video.mjpeg_metal.prepareWindows(a, init.io, &reader, &selected, owner, .{ .preparation = .{ .width = target_size, .height = target_size, .matrix = .bt709 } });
                defer job.deinit();
                // prepareWindows has fenced every command. Submission time alone
                // is deliberately not reported as completion latency.
                for (job.frames) |*frame| gpu_seconds += try frame.gpuSeconds();
                frames = job.frames.len;
                dependency_packets = job.decoded_packets;
                decode_peak = job.decode_high_water;
                device_bytes = owner.deviceAllocatedBytes();
                coefficients = job.coefficient_staging_bytes;
                rgba = job.rgba_staging_bytes;
            }
        } else {
            var job = try video.software.prepareWindows(a, &reader, &selected, .{ .h264 = .{ .max_dependency_packets = reader.packets.len, .max_decode_bytes = 512 * 1024 * 1024 }, .preparation = .{ .width = target_size, .height = target_size, .matrix = .bt709 } });
            defer job.deinit();
            frames = job.plan.unique_indexes.len;
            dependency_packets = job.decoded_packets;
            decode_peak = job.decode_high_water;
        }
        const elapsed = now(init.io) - start;
        if (iteration != 0) warm[iteration - 1] = elapsed;
        try output.print("{{\"kind\":\"iteration\",\"backend\":\"{s}\",\"state\":\"{s}\",\"iteration\":{d},\"completed_ns\":{d},\"frames\":{d},\"dependency_packets\":{d},\"gpu_execution_ns\":{d},\"decode_live_peak_bytes\":{d},\"device_allocated_bytes\":{d},\"process_peak_rss_bytes\":{d},\"coefficient_upload_bytes\":{d},\"rgba_upload_bytes\":{d}}}\n", .{ if (metal != null) (if (reader.track.codec == .avc) "videotoolbox-metal" else "mjpeg-metal") else (if (reader.track.codec == .avc) "h264-cpu" else "mjpeg-cpu"), if (iteration == 0) "cold" else "warm", iteration, elapsed, frames, dependency_packets, @as(u64, @intFromFloat(@max(gpu_seconds, 0) * 1e9)), decode_peak, device_bytes, av_benchmark_peak_rss(), coefficients, rgba });
    }
    std.mem.sort(u64, warm, {}, std.sort.asc(u64));
    const p50 = warm[(warm.len - 1) / 2];
    const p95 = warm[(warm.len * 95 + 99) / 100 - 1];
    try output.print("{{\"kind\":\"summary\",\"fixture\":\"{s}\",\"source_packets\":{d},\"preparation_size\":{d},\"setup_ns\":{d},\"warm_samples\":{d},\"warm_p50_ns\":{d},\"warm_p95_ns\":{d},\"frames_per_second_p50\":{d}}}\n", .{ if (path != null) "external-file" else "mjpeg-64x48-8-frames", reader.packets.len, target_size, setup_ns, warm.len, p50, p95, @as(f64, @floatFromInt(frames)) * 1e9 / @as(f64, @floatFromInt(@max(p50, 1))) });
    try output.flush();
}
fn now(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}
