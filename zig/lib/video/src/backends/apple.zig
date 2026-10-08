// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const avc = @import("../avc.zig");
const supported = @import("builtin").os.tag == .macos;
extern fn av_decoder_create([*]const u8, usize, u32, u32, c_int, *const fn (*anyopaque, usize, i32, ?*anyopaque) callconv(.c) void, *anyopaque, *?*anyopaque) i32;
extern fn av_decoder_submit(*anyopaque, [*]const u8, usize, usize, i64, i64, u32, u32) i32;
extern fn av_decoder_drain(*anyopaque) i32;
extern fn av_decoder_destroy(*anyopaque) void;
extern fn av_decoder_hardware(*anyopaque) c_int;
extern fn av_surface_retain(*anyopaque) void;
extern fn av_surface_release(*anyopaque) void;
extern fn av_surface_width(*anyopaque) u32;
extern fn av_surface_height(*anyopaque) u32;
extern fn av_surface_format(*anyopaque) u32;
extern fn av_surface_storage_bytes(*anyopaque) u64;
extern fn av_surface_lock(*anyopaque) i32;
extern fn av_surface_unlock(*anyopaque) void;
extern fn av_surface_plane(*anyopaque, usize, *usize, *usize, *usize) ?[*]const u8;
extern fn av_metal_create(*anyopaque, *?*anyopaque) i32;
extern fn av_metal_destroy(*anyopaque) void;
extern fn av_metal_import(*anyopaque, *anyopaque, *?*anyopaque) i32;
extern fn av_metal_plane_texture(*anyopaque, usize) ?*anyopaque;
extern fn av_metal_import_release(*anyopaque) void;

pub const Format = enum(u32) { nv12_video = 0x34323076, nv12_full = 0x34323066 };
/// Move-only owned retain. Not locked for CPU access until map() is called.
pub const Surface = struct {
    handle: *anyopaque,
    width: u32,
    height: u32,
    format: Format,
    /// Retains a caller-owned CVPixelBuffer without copying its pixels.
    pub fn fromBorrowed(handle: *anyopaque) !Surface {
        if (!supported) return error.UnsupportedVideoBackend;
        const format = std.enums.fromInt(Format, av_surface_format(handle)) orelse return error.UnsupportedSurfaceFormat;
        av_surface_retain(handle);
        return .{ .handle = handle, .width = av_surface_width(handle), .height = av_surface_height(handle), .format = format };
    }
    pub fn deinit(self: *Surface) void {
        if (supported) av_surface_release(self.handle);
        self.* = undefined;
    }
    pub fn map(self: *const Surface) !Mapping {
        if (!supported) return error.UnsupportedVideoBackend;
        av_surface_retain(self.handle);
        errdefer av_surface_release(self.handle);
        if (av_surface_lock(self.handle) != 0) return error.SurfaceMappingFailed;
        errdefer av_surface_unlock(self.handle);
        var planes: [2]Plane = undefined;
        for (&planes, 0..) |*p, i| {
            var stride: usize = 0;
            var width: usize = 0;
            var height: usize = 0;
            const bytes = av_surface_plane(self.handle, i, &stride, &width, &height) orelse return error.UnsupportedSurfaceFormat;
            const size = std.math.mul(usize, stride, height) catch return error.ResourceLimitExceeded;
            const row = std.math.mul(usize, width, if (i == 0) @as(usize, 1) else 2) catch return error.ResourceLimitExceeded;
            if (stride < row) return error.UnsupportedSurfaceFormat;
            p.* = .{ .bytes = bytes[0..size], .stride = stride, .width = width, .height = height };
        }
        return .{ .handle = self.handle, .planes = planes };
    }
};
pub const Plane = struct { bytes: []const u8, stride: usize, width: usize, height: usize };
pub const Mapping = struct {
    /// Independently retains the locked pixel buffer, even after batch release.
    handle: *anyopaque,
    planes: [2]Plane,
    pub fn deinit(self: *Mapping) void {
        if (supported) {
            av_surface_unlock(self.handle);
            av_surface_release(self.handle);
        }
        self.* = undefined;
    }
};
pub const Frame = struct {
    decode_index: usize,
    pts: i64,
    duration: u32,
    timescale: u32,
    surface: Surface,
};
pub const Batch = struct {
    allocator: std.mem.Allocator,
    frames: []Frame,
    hardware: bool,
    submitted_packets: usize,
    display_matrix: [9]i32,
    pixel_aspect: @FieldType(media.mp4.Track, "pixel_aspect"),
    /// Owned copy: display metadata remains available after Reader.deinit().
    color_info: []u8,
    pub fn deinit(self: *Batch) void {
        for (self.frames) |*frame| frame.surface.deinit();
        self.allocator.free(self.frames);
        self.allocator.free(self.color_info);
        self.* = undefined;
    }
};
pub const Options = struct {
    /// Hardware-only promises must not silently fall back to platform software.
    require_hardware: bool = true,
    max_frames: usize = 32,
    /// Admission estimate includes a conservative native decoder picture pool;
    /// opaque OS decoder workspace is outside allocator-level accounting.
    max_surface_bytes: u64 = 512 * 1024 * 1024,
    max_retained_surface_bytes: u64 = 128 * 1024 * 1024,
    max_decode_packets: usize = 500_000,
    max_packet_bytes: usize = 16 * 1024 * 1024,
};
const Capture = struct {
    reader: *const media.mp4.Reader,
    indexes: []const usize,
    slots: []?Frame,
    control: media.source.Control,
    failure: ?anyerror = null,
    retained: u64 = 0,
    max_retained: u64,
    /// Defensive even though temporal/asynchronous decode flags are disabled.
    mutex: std.atomic.Mutex = .unlocked,
    fn callback(context: *anyopaque, index: usize, status: i32, image: ?*anyopaque) callconv(.c) void {
        const self: *Capture = @ptrCast(@alignCast(context));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.failure != null) return;
        self.control.check() catch |err| {
            self.failure = err;
            return;
        };
        if (status != 0) {
            self.failure = error.VideoDecodeFailed;
            return;
        }
        for (self.indexes, 0..) |selected, slot| {
            if (selected != index) continue;
            if (image == null) {
                self.failure = error.MissingDecodedFrame;
                return;
            }
            if (self.slots[slot] != null) {
                self.failure = error.DuplicateDecodedFrame;
                return;
            }
            const bytes = av_surface_storage_bytes(image.?);
            if (bytes == 0 or bytes > self.max_retained -| self.retained) {
                self.failure = error.ResourceLimitExceeded;
                return;
            }
            const surface = Surface.fromBorrowed(image.?) catch |err| {
                self.failure = err;
                return;
            };
            if (surface.width != self.reader.track.width or surface.height != self.reader.track.height) {
                var owned = surface;
                owned.deinit();
                self.failure = error.UnsupportedDynamicGeometry;
                return;
            }
            self.retained += bytes;
            const packet = self.reader.packets[index];
            self.slots[slot] = .{ .decode_index = index, .pts = packet.pts, .duration = packet.duration, .timescale = self.reader.track.timescale, .surface = surface };
            return;
        }
    }
};
/// Decodes forward from the beginning, retaining only requested pictures. The
/// returned order matches indexes (normally a presentation-order sampling plan).
/// No speculative sync seeking: open GOP dependency qualification is later work.
/// Each call owns its session; cancellation drains callbacks before freeing state.
pub fn decodeSelected(allocator: std.mem.Allocator, reader: *media.mp4.Reader, indexes: []const usize, options: Options) !Batch {
    if (!supported) return error.UnsupportedVideoBackend;
    try reader.input.control.check();
    try avc.validateConfig(reader.track.avcc);
    if (indexes.len == 0 or indexes.len > options.max_frames) return error.ResourceLimitExceeded;
    if (reader.track.timescale > std.math.maxInt(i32)) return error.UnsupportedTimeline;
    var end: usize = 0;
    for (indexes, 0..) |index, i| {
        if (index >= reader.packets.len) return error.InvalidPacketIndex;
        for (indexes[0..i]) |other| if (other == index) return error.DuplicateFrameSelection;
        end = @max(end, index);
    }
    if (end >= options.max_decode_packets) return error.ResourceLimitExceeded;
    // Conservative bound includes native row alignment, all selected pictures,
    // and a fixed extra decoder pool (16 reference pictures + 8 outputs).
    const row = std.mem.alignForward(u64, @as(u64, reader.track.width) + 256, 256);
    const per_frame = row * (@as(u64, reader.track.height) + 32) * 2;
    if (per_frame * (indexes.len + 24) > options.max_surface_bytes) return error.ResourceLimitExceeded;
    const slots = try allocator.alloc(?Frame, indexes.len);
    defer allocator.free(slots);
    @memset(slots, null);
    errdefer for (slots) |*slot| if (slot.*) |*f| f.surface.deinit();
    var capture = Capture{ .reader = reader, .indexes = indexes, .slots = slots, .control = reader.input.control, .max_retained = options.max_retained_surface_bytes };
    var handle: ?*anyopaque = null;
    if (av_decoder_create(reader.track.avcc.ptr, reader.track.avcc.len, reader.track.width, reader.track.height, @intFromBool(options.require_hardware), Capture.callback, &capture, &handle) != 0) return error.VideoDecoderUnavailable;
    // This defer runs before capture/slot cleanup on every failure path.
    defer av_decoder_destroy(handle.?);
    for (0..end + 1) |index| {
        try reader.input.control.check();
        const packet = reader.packets[index];
        if (packet.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
        var lease = try reader.readPacket(index);
        defer lease.deinit();
        try avc.validatePacket(lease.bytes, reader.track.nal_length_bytes);
        if (av_decoder_submit(handle.?, lease.bytes.ptr, lease.bytes.len, index, packet.pts, packet.dts, packet.duration, reader.track.timescale) != 0) return error.VideoDecodeFailed;
        if (capture.failure) |err| return err;
    }
    if (av_decoder_drain(handle.?) != 0) return error.VideoDecodeFailed;
    const hardware = av_decoder_hardware(handle.?) != 0;
    if (options.require_hardware and !hardware) return error.HardwareVideoDecoderRequired;
    if (capture.failure) |err| return err;
    try reader.input.control.check();
    for (slots) |slot| if (slot == null) return error.MissingDecodedFrame;
    const color_info = try allocator.dupe(u8, reader.track.color_info);
    errdefer allocator.free(color_info);
    const frames = try allocator.alloc(Frame, indexes.len);
    for (frames, slots) |*frame, slot| frame.* = slot.?;
    return .{ .allocator = allocator, .frames = frames, .hardware = hardware, .submitted_packets = end + 1, .display_matrix = reader.track.display_matrix, .pixel_aspect = reader.track.pixel_aspect, .color_info = color_info };
}
/// Device is a borrowed id<MTLDevice> from the consuming inference backend. The
/// cache owns no model runtime and does not silently pick a different GPU.
pub const MetalCache = struct {
    handle: *anyopaque,
    pub fn init(device: *anyopaque) !MetalCache {
        if (!supported) return error.UnsupportedVideoBackend;
        var handle: ?*anyopaque = null;
        if (av_metal_create(device, &handle) != 0) return error.MetalTextureImportFailed;
        return .{ .handle = handle.? };
    }
    pub fn deinit(self: *MetalCache) void {
        if (supported) av_metal_destroy(self.handle);
        self.* = undefined;
    }
    pub fn import(self: *MetalCache, surface: *const Surface) !MetalImport {
        if (!supported) return error.UnsupportedVideoBackend;
        var handle: ?*anyopaque = null;
        if (av_metal_import(self.handle, surface.handle, &handle) != 0) return error.MetalTextureImportFailed;
        return .{ .handle = handle.? };
    }
};
/// Retains the CVPixelBuffer and both CVMetalTexture wrappers independently of
/// the frame batch and cache. Consumer must fence GPU work before deinit.
pub const MetalImport = struct {
    handle: *anyopaque,
    pub fn texture(self: *const MetalImport, plane: usize) !*anyopaque {
        if (!supported) return error.UnsupportedVideoBackend;
        if (plane >= 2) return error.InvalidSurfacePlane;
        return av_metal_plane_texture(self.handle, plane) orelse error.MetalTextureImportFailed;
    }
    pub fn deinit(self: *MetalImport) void {
        if (supported) av_metal_import_release(self.handle);
        self.* = undefined;
    }
};
