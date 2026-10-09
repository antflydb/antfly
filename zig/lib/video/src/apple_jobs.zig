// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Request-local reuse and bounded decode/Metal preparation overlap. Model
//! execution is a separate consumer of the returned completed Metal buffers.
const std = @import("std");
const media = @import("antfly_media");
const apple = @import("backends/apple.zig");
const preparation = @import("preparation.zig");
const windows = @import("windows.zig");
pub const Options = struct {
    admission_pool: ?*media.admission.Pool = null,
    windows: windows.Limits = .{},
    decode: apple.Options = .{ .seek_mode = .verified_idr, .max_frames = 64 },
    preparation: preparation.Options,
    /// Hard cap on outstanding imported surfaces/commands. One more surface may
    /// be borrowed by the synchronous decoder while the oldest command fences.
    queue_depth: usize = 2,
    max_inflight_surface_bytes: u64 = 128 * 1024 * 1024,
    max_output_bytes: usize = 128 * 1024 * 1024,
};
/// Move-only owned results. Buffers/PTS/window mappings outlive Reader/Source.
/// One output per unique picture; window() returns indexes into frames.
pub const PreparedWindows = struct {
    output_reservation: media.admission.Token = .{},
    allocator: std.mem.Allocator,
    plan: windows.Plan,
    frames: []preparation.Prepared,
    decode: apple.Batch,
    queue_high_water: usize,
    inflight_surface_high_water: u64,
    output_bytes: usize,
    pub fn window(self: *const PreparedWindows, index: usize) ![]const usize {
        return self.plan.window(index);
    }
    pub fn reusedSelections(self: *const PreparedWindows) usize {
        return self.plan.references.len - self.frames.len;
    }
    pub fn deinit(self: *PreparedWindows) void {
        for (self.frames) |*frame| frame.deinit();
        self.allocator.free(self.frames);
        self.decode.deinit();
        self.plan.deinit();
        self.output_reservation.deinit();
        self.* = undefined;
    }
};
const Pending = struct { slot: usize, bytes: u64 };
const Queue = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    control: media.source.Control,
    plan: *const windows.Plan,
    metal: *preparation.Metal,
    options: Options,
    slots: []?preparation.Prepared,
    pending: [8]Pending = undefined,
    count: usize = 0,
    bytes: u64 = 0,
    high_water: usize = 0,
    bytes_high_water: u64 = 0,
    fn finishOldest(self: *Queue) !void {
        const oldest = self.pending[0];
        const prepared = &self.slots[oldest.slot].?;
        try prepared.wait(self.io, self.control);
        try prepared.releaseSource();
        self.bytes -= oldest.bytes;
        self.count -= 1;
        std.mem.copyForwards(Pending, self.pending[0..self.count], self.pending[1 .. self.count + 1]);
    }
    fn accept(context: *anyopaque, frame: *const apple.Frame) !void {
        const self: *Queue = @ptrCast(@alignCast(context));
        try self.control.check();
        const bytes = frame.surface.storageBytes();
        if (bytes == 0 or bytes > self.options.max_inflight_surface_bytes) return error.ResourceLimitExceeded;
        while (self.count >= self.options.queue_depth or bytes > self.options.max_inflight_surface_bytes - self.bytes) try self.finishOldest();
        var slot: ?usize = null;
        for (self.plan.unique_indexes, 0..) |index, i| if (index == frame.decode_index) {
            slot = i;
            break;
        };
        const index = slot orelse return error.InvalidPacketIndex;
        if (self.slots[index] != null) return error.DuplicateDecodedFrame;
        self.slots[index] = try self.metal.submit(self.allocator, &frame.surface, self.options.preparation, self.control);
        self.pending[self.count] = .{ .slot = index, .bytes = bytes };
        self.count += 1;
        self.bytes += bytes;
        self.high_water = @max(self.high_water, self.count);
        self.bytes_high_water = @max(self.bytes_high_water, self.bytes);
    }
};
pub fn prepareWindows(allocator: std.mem.Allocator, io: std.Io, reader: *media.mp4.Reader, requested: []const windows.Window, metal: *preparation.Metal, options: Options) !PreparedWindows {
    if (@import("builtin").os.tag != .macos) return error.UnsupportedVideoBackend;
    try reader.input.control.check();
    if (options.admission_pool) |pool| if (metal.cache_reservation.pool != pool) return error.UnadmittedMetalPreparer;
    if (options.queue_depth == 0 or options.queue_depth > 8) return error.ResourceLimitExceeded;
    var plan = try windows.create(allocator, reader, requested, options.windows);
    errdefer plan.deinit();
    const geometry = try preparation.geometry(reader.track.width, reader.track.height, options.preparation);
    const per_frame = std.math.mul(usize, geometry.values(), @sizeOf(f32)) catch return error.ResourceLimitExceeded;
    const output_bytes = std.math.mul(usize, per_frame, plan.unique_indexes.len) catch return error.ResourceLimitExceeded;
    if (output_bytes > options.max_output_bytes or plan.unique_indexes.len > options.decode.max_frames) return error.ResourceLimitExceeded;
    // Reserve configured upper bounds before decoding. Keep output admission
    // until result destruction; release transient capacity after GPU completion.
    var output_reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .device_bytes = output_bytes }) else media.admission.Token{};
    errdefer output_reservation.deinit();
    var transient = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = options.preparation.max_scratch_bytes, .device_bytes = try std.math.add(u64, try std.math.add(u64, try std.math.mul(u64, options.preparation.max_scratch_bytes, options.queue_depth), options.max_inflight_surface_bytes), try apple.surfaceAdmissionBytes(reader.track.width, reader.track.height, plan.unique_indexes.len)), .commands = options.queue_depth }) else media.admission.Token{};
    defer transient.deinit();
    const slots = try allocator.alloc(?preparation.Prepared, plan.unique_indexes.len);
    defer allocator.free(slots);
    @memset(slots, null);
    errdefer for (slots) |*slot| if (slot.*) |*prepared| prepared.deinit();
    var queue = Queue{ .allocator = allocator, .io = io, .control = reader.input.control, .plan = &plan, .metal = metal, .options = options, .slots = slots };
    var receipt = try apple.decodeTo(allocator, reader, plan.unique_indexes, options.decode, .{ .context = &queue, .accept = Queue.accept });
    errdefer receipt.deinit();
    while (queue.count != 0) try queue.finishOldest();
    try reader.input.control.check();
    const frames = try allocator.alloc(preparation.Prepared, slots.len);
    errdefer allocator.free(frames);
    for (frames, slots) |*frame, slot| frame.* = slot orelse return error.MissingDecodedFrame;
    return .{ .output_reservation = output_reservation, .allocator = allocator, .plan = plan, .frames = frames, .decode = receipt, .queue_high_water = queue.high_water, .inflight_surface_high_water = queue.bytes_high_water, .output_bytes = output_bytes };
}
