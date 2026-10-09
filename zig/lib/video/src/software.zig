// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Portable window preparation, sharing selected pictures across clips.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const mjpeg = @import("mjpeg.zig");
const windows = @import("windows.zig");
const preparation = @import("preparation.zig");
pub const Options = struct {
    h264: h264.Options = .{},
    mjpeg: mjpeg.Options = .{},
    windows: windows.Limits = .{},
    preparation: preparation.Options,
    max_frames: usize = 64,
    max_output_bytes: usize = 128 * 1024 * 1024,
    admission_pool: ?*media.admission.Pool = null,
};
pub const PreparedWindows = mjpeg.PreparedWindows;
pub fn prepareWindows(allocator: std.mem.Allocator, reader: *media.mp4.Reader, requested: []const windows.Window, options: Options) !PreparedWindows {
    try reader.input.control.check();
    if (reader.track.codec == .mjpeg) {
        var decode = options.mjpeg;
        decode.max_frames = @min(decode.max_frames, options.max_frames);
        return mjpeg.prepareWindows(allocator, reader, requested, .{ .decode = decode, .windows = options.windows, .preparation = options.preparation, .max_output_bytes = options.max_output_bytes, .admission_pool = options.admission_pool });
    }
    var plan = try windows.create(allocator, reader, requested, options.windows);
    errdefer plan.deinit();
    if (plan.unique_indexes.len > options.max_frames) return error.ResourceLimitExceeded;
    const g = try preparation.geometry(reader.track.width, reader.track.height, options.preparation);
    const values = try std.math.mul(usize, g.values(), plan.unique_indexes.len);
    const bytes = try std.math.mul(usize, values, @sizeOf(f32));
    if (bytes > options.max_output_bytes) return error.ResourceLimitExceeded;
    var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = bytes }) else media.admission.Token{};
    errdefer reservation.deinit();
    var transient = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(u64, options.h264.max_decode_bytes, options.preparation.max_scratch_bytes) }) else media.admission.Token{};
    defer transient.deinit();
    const color_info = try allocator.dupe(u8, reader.track.color_info);
    errdefer allocator.free(color_info);
    const patches = try allocator.alloc(f32, values);
    errdefer allocator.free(patches);
    var payload_bytes: u64 = 0;
    var peak: usize = 0;
    for (plan.unique_indexes, 0..) |index, slot| {
        var frame = try h264.decodeFrame(allocator, reader, index, options.h264);
        defer frame.deinit();
        const prepared = try preparation.referenceHost(allocator, frame.host(), options.preparation, reader.input.control);
        defer allocator.free(prepared);
        @memcpy(patches[slot * g.values() ..][0..g.values()], prepared);
        peak = @max(peak, frame.decode_high_water);
        payload_bytes += reader.packets[index].size;
    }
    return .{ .output_reservation = reservation, .allocator = allocator, .plan = plan, .patches = patches, .geometry = g, .decoded_packets = plan.unique_indexes.len, .payload_bytes = payload_bytes, .decode_high_water = peak, .display_matrix = reader.track.display_matrix, .pixel_aspect = reader.track.pixel_aspect, .color_info = color_info };
}
