// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Portable intra-picture decoder. Each qualified jpeg sample is a complete
//! baseline 8-bit JPEG; no reference frames or external abbreviated tables.
const std = @import("std");
const media = @import("antfly_media");
const image = @import("antfly_image");
const windows = @import("windows.zig");
const preparation = @import("preparation.zig");
pub const Options = struct {
    max_frames: usize = 64,
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_pixels: usize = 16 * 1024 * 1024,
    /// Actual live allocations during one JPEG decode, including output pixels.
    max_decode_bytes: usize = 128 * 1024 * 1024,
};
pub const Frame = struct {
    allocator: std.mem.Allocator,
    decode_index: usize,
    pts: i64,
    duration: u32,
    timescale: u32,
    width: u32,
    height: u32,
    rgba: []u8,
    decode_high_water: usize,
    pub fn deinit(self: *Frame) void {
        self.allocator.free(self.rgba);
        self.* = undefined;
    }
};
/// Tracks live bytes across alloc/resize/remap/free; exceeding the cap fails
/// before allocation and is distinguished from backing allocator exhaustion.
const Budget = struct {
    backing: std.mem.Allocator,
    limit: usize,
    live: usize = 0,
    peak: usize = 0,
    denied: bool = false,
    fn allocator(self: *Budget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn admit(self: *Budget, old: usize, new: usize) bool {
        if (new > self.limit -| (self.live - old)) {
            self.denied = true;
            return false;
        }
        return true;
    }
    fn charge(self: *Budget, old: usize, new: usize) void {
        self.live = self.live - old + new;
        self.peak = @max(self.peak, self.live);
    }
    fn alloc(ctx: *anyopaque, len: usize, align_: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        if (!self.admit(0, len)) return null;
        const bytes = self.backing.rawAlloc(len, align_, ra) orelse return null;
        self.charge(0, len);
        return bytes;
    }
    fn resize(ctx: *anyopaque, bytes: []u8, align_: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        if (!self.admit(bytes.len, len) or !self.backing.rawResize(bytes, align_, len, ra)) return false;
        self.charge(bytes.len, len);
        return true;
    }
    fn remap(ctx: *anyopaque, bytes: []u8, align_: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        if (!self.admit(bytes.len, len)) return null;
        const out = self.backing.rawRemap(bytes, align_, len, ra) orelse return null;
        self.charge(bytes.len, len);
        return out;
    }
    fn free(ctx: *anyopaque, bytes: []u8, align_: std.mem.Alignment, ra: usize) void {
        const self: *Budget = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(bytes, align_, ra);
        self.live -= bytes.len;
    }
};
pub fn decodeFrame(allocator: std.mem.Allocator, reader: *media.mp4.Reader, index: usize, options: Options) !Frame {
    try reader.input.control.check();
    if (reader.track.codec != .mjpeg) return error.UnsupportedVideoCodec;
    if (index >= reader.packets.len) return error.InvalidPacketIndex;
    const packet = reader.packets[index];
    if (packet.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
    var lease = try reader.readPacket(index);
    defer lease.deinit();
    var scope = image.work_control.Scope.enter(.{ .context = reader.input.control.context, .check_fn = reader.input.control.check_fn });
    defer scope.deinit();
    const structure = try image.jpeg.parseStructure(lease.bytes);
    const info = structure.info;
    if (info.width != reader.track.width or info.height != reader.track.height) return error.UnsupportedDynamicGeometry;
    if (info.frame_kind != .baseline_dct or info.bits_per_sample != 8 or (info.component_count != 1 and info.component_count != 3) or !image.jpeg.supportsPlannedBaselineDecode(structure)) return error.UnsupportedVideoProfile;
    (image.DecodeLimits{ .max_pixels = options.max_pixels, .max_rgba_bytes = options.max_decode_bytes }).validate(info.width, info.height) catch return error.ResourceLimitExceeded;
    // One complete JPEG per sample, no concatenated fields/trailing payload.
    if (lease.bytes.len < 4 or structure.scans[0].entropy_end != lease.bytes.len - 2 or !image.jpeg.hasSignature(lease.bytes) or !std.mem.eql(u8, lease.bytes[lease.bytes.len - 2 ..], &.{ 0xff, 0xd9 })) return error.MalformedVideoPacket;
    try reader.input.control.check();
    var budget = Budget{ .backing = allocator, .limit = options.max_decode_bytes };
    const decoded = image.jpeg.decodeRgba(budget.allocator(), lease.bytes) catch |err| {
        return if (err == error.OutOfMemory and budget.denied) error.ResourceLimitExceeded else err;
    };
    errdefer allocator.free(decoded.rgba);
    try reader.input.control.check();
    // Decoder scratch has released; transfer the sole remaining allocation to
    // the frame's original allocator, without retaining a stack allocator.
    std.debug.assert(budget.live == decoded.rgba.len);
    return .{ .allocator = allocator, .decode_index = index, .pts = packet.pts, .duration = packet.duration, .timescale = reader.track.timescale, .width = decoded.width, .height = decoded.height, .rgba = decoded.rgba, .decode_high_water = budget.peak };
}
pub const PreparedWindows = struct {
    allocator: std.mem.Allocator,
    plan: windows.Plan,
    /// Contiguous unique-picture patch arrays, window entries index this axis.
    patches: []f32,
    geometry: preparation.Geometry,
    decoded_packets: usize,
    payload_bytes: u64,
    decode_high_water: usize,
    display_matrix: [9]i32,
    pixel_aspect: @FieldType(media.mp4.Track, "pixel_aspect"),
    color_info: []u8,
    pub fn window(self: *const PreparedWindows, index: usize) ![]const usize {
        return self.plan.window(index);
    }
    pub fn reusedSelections(self: *const PreparedWindows) usize {
        return self.plan.references.len - self.decoded_packets;
    }
    pub fn frame(self: *const PreparedWindows, index: usize) ![]const f32 {
        if (index >= self.plan.unique_indexes.len) return error.InvalidPacketIndex;
        const count = self.geometry.values();
        return self.patches[index * count ..][0..count];
    }
    pub fn deinit(self: *PreparedWindows) void {
        self.allocator.free(self.patches);
        self.allocator.free(self.color_info);
        self.plan.deinit();
        self.* = undefined;
    }
};
pub const JobOptions = struct {
    decode: Options = .{},
    windows: windows.Limits = .{},
    preparation: preparation.Options,
    max_output_bytes: usize = 128 * 1024 * 1024,
};
/// Decode/prepare one unique picture at a time; bounded request-local reuse.
/// JPEG supplies RGB display values; preparation.matrix is unused for RGB input.
pub fn prepareWindows(allocator: std.mem.Allocator, reader: *media.mp4.Reader, requested: []const windows.Window, options: JobOptions) !PreparedWindows {
    try reader.input.control.check();
    if (reader.track.codec != .mjpeg) return error.UnsupportedVideoCodec;
    var plan = try windows.create(allocator, reader, requested, options.windows);
    errdefer plan.deinit();
    if (plan.unique_indexes.len > options.decode.max_frames) return error.ResourceLimitExceeded;
    const g = try preparation.geometry(reader.track.width, reader.track.height, options.preparation);
    const count = std.math.mul(usize, g.values(), plan.unique_indexes.len) catch return error.ResourceLimitExceeded;
    const bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.ResourceLimitExceeded;
    if (bytes > options.max_output_bytes) return error.ResourceLimitExceeded;
    const color_info = try allocator.dupe(u8, reader.track.color_info);
    errdefer allocator.free(color_info);
    const patches = try allocator.alloc(f32, count);
    errdefer allocator.free(patches);
    var payload_bytes: u64 = 0;
    var peak: usize = 0;
    for (plan.unique_indexes, 0..) |index, slot| {
        var decoded = try decodeFrame(allocator, reader, index, options.decode);
        defer decoded.deinit();
        const prepared = try preparation.referenceRgba(allocator, decoded.rgba, decoded.width, decoded.height, options.preparation, reader.input.control);
        defer allocator.free(prepared);
        @memcpy(patches[slot * g.values() ..][0..g.values()], prepared);
        peak = @max(peak, decoded.decode_high_water);
        payload_bytes += reader.packets[index].size;
    }
    return .{ .allocator = allocator, .plan = plan, .patches = patches, .geometry = g, .decoded_packets = plan.unique_indexes.len, .payload_bytes = payload_bytes, .decode_high_water = peak, .display_matrix = reader.track.display_matrix, .pixel_aspect = reader.track.pixel_aspect, .color_info = color_info };
}
