// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Pure Zig JPEG decode overlapped with bounded RGBA staging/Metal preparation.
//! No model dependency; results hand off completed buffers on the caller's device.
const std = @import("std");
const media = @import("antfly_media");
const mjpeg = @import("mjpeg.zig");
const windows = @import("windows.zig");
const preparation = @import("preparation.zig");
pub const Options = struct {
    decode: mjpeg.Options = .{},
    windows: windows.Limits = .{},
    preparation: preparation.Options,
    queue_depth: usize = 2,
    max_inflight_staging_bytes: u64 = 128 * 1024 * 1024,
    max_total_staging_bytes: u64 = 256 * 1024 * 1024,
    max_output_bytes: usize = 128 * 1024 * 1024,
};
/// Move-only. Completed command inputs have released; outputs, display metadata
/// and window mappings remain owned independently of Reader/Source/Metal.
pub const PreparedWindows = struct {
    allocator: std.mem.Allocator,
    plan: windows.Plan,
    frames: []preparation.Prepared,
    display_matrix: [9]i32,
    pixel_aspect: @FieldType(media.mp4.Track, "pixel_aspect"),
    color_info: []u8,
    output_bytes: usize,
    decoded_packets: usize,
    payload_bytes: u64,
    decode_high_water: usize,
    rgba_staging_bytes: u64,
    coefficient_staging_bytes: u64,
    queue_high_water: usize,
    inflight_staging_high_water: u64,
    pub fn window(self: *const PreparedWindows, index: usize) ![]const usize {
        return self.plan.window(index);
    }
    pub fn reusedSelections(self: *const PreparedWindows) usize {
        return self.plan.references.len - self.frames.len;
    }
    pub fn deinit(self: *PreparedWindows) void {
        for (self.frames) |*frame| frame.deinit();
        self.allocator.free(self.frames);
        self.allocator.free(self.color_info);
        self.plan.deinit();
        self.* = undefined;
    }
};
const Pending = struct { slot: usize, bytes: usize };
const Queue = struct {
    io: std.Io,
    control: media.source.Control,
    slots: []?preparation.Prepared,
    pending: [8]Pending = undefined,
    count: usize = 0,
    bytes: u64 = 0,
    fn finishOldest(self: *Queue) !void {
        const first = self.pending[0];
        const prepared = &self.slots[first.slot].?;
        try prepared.wait(self.io, self.control);
        try prepared.releaseSource();
        self.bytes -= first.bytes;
        self.count -= 1;
        std.mem.copyForwards(Pending, self.pending[0..self.count], self.pending[1 .. self.count + 1]);
    }
};
pub fn prepareWindows(allocator: std.mem.Allocator, io: std.Io, reader: *media.mp4.Reader, requested: []const windows.Window, metal: *preparation.Metal, options: Options) !PreparedWindows {
    if (@import("builtin").os.tag != .macos) return error.UnsupportedVideoBackend;
    try reader.input.control.check();
    if (reader.track.codec != .mjpeg) return error.UnsupportedVideoCodec;
    if (options.queue_depth == 0 or options.queue_depth > 8) return error.ResourceLimitExceeded;
    var plan = try windows.create(allocator, reader, requested, options.windows);
    errdefer plan.deinit();
    if (plan.unique_indexes.len > options.decode.max_frames) return error.ResourceLimitExceeded;
    const g = try preparation.geometry(reader.track.width, reader.track.height, options.preparation);
    const input_bytes = std.math.mul(usize, try std.math.mul(usize, reader.track.width, reader.track.height), 4) catch return error.ResourceLimitExceeded;
    const total_staging = std.math.mul(u64, input_bytes, plan.unique_indexes.len) catch return error.ResourceLimitExceeded;
    const output_bytes = std.math.mul(usize, try std.math.mul(usize, g.values(), @sizeOf(f32)), plan.unique_indexes.len) catch return error.ResourceLimitExceeded;
    if (input_bytes > options.max_inflight_staging_bytes or input_bytes > options.preparation.max_host_staging_bytes or total_staging > options.max_total_staging_bytes or output_bytes > options.max_output_bytes) return error.ResourceLimitExceeded;
    const color_info = try allocator.dupe(u8, reader.track.color_info);
    errdefer allocator.free(color_info);
    const slots = try allocator.alloc(?preparation.Prepared, plan.unique_indexes.len);
    defer allocator.free(slots);
    @memset(slots, null);
    // Deinit joins in-flight GPU work before freeing staging and output resources.
    errdefer for (slots) |*slot| if (slot.*) |*frame| frame.deinit();
    var queue = Queue{ .io = io, .control = reader.input.control, .slots = slots };
    var high_water: usize = 0;
    var staging_high_water: u64 = 0;
    var payload_bytes: u64 = 0;
    var decode_high_water: usize = 0;
    var coefficient_bytes: u64 = 0;
    var staging_bytes: u64 = 0;
    for (plan.unique_indexes, 0..) |index, slot| {
        // At most one decoded host frame exists while waiting for queue capacity.
        var frame = try mjpeg.decodeFrame(allocator, reader, index, options.decode);
        defer frame.deinit();
        while (queue.count >= options.queue_depth or input_bytes > options.max_inflight_staging_bytes - queue.bytes) try queue.finishOldest();
        try reader.input.control.check();
        slots[slot] = try metal.submitRgba(allocator, frame.rgba, frame.width, frame.height, options.preparation, reader.input.control);
        queue.pending[queue.count] = .{ .slot = slot, .bytes = input_bytes };
        queue.count += 1;
        queue.bytes += input_bytes;
        high_water = @max(high_water, queue.count);
        staging_high_water = @max(staging_high_water, queue.bytes);
        payload_bytes += reader.packets[index].size;
        decode_high_water = @max(decode_high_water, frame.decode_high_water);
        coefficient_bytes += slots[slot].?.coefficient_staging_bytes;
        staging_bytes += slots[slot].?.rgba_staging_bytes;
    }
    while (queue.count != 0) try queue.finishOldest();
    try reader.input.control.check();
    const frames = try allocator.alloc(preparation.Prepared, slots.len);
    errdefer allocator.free(frames);
    for (frames, slots) |*frame, slot| frame.* = slot orelse return error.MissingDecodedFrame;
    return .{ .allocator = allocator, .plan = plan, .frames = frames, .display_matrix = reader.track.display_matrix, .pixel_aspect = reader.track.pixel_aspect, .color_info = color_info, .output_bytes = output_bytes, .decoded_packets = frames.len, .payload_bytes = payload_bytes, .decode_high_water = decode_high_water, .rgba_staging_bytes = staging_bytes, .coefficient_staging_bytes = coefficient_bytes, .queue_high_water = high_water, .inflight_staging_high_water = staging_high_water };
}
