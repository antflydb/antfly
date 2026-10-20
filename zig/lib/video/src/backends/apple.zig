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
const avc = @import("../avc.zig");
const decode_plan = @import("../decode_plan.zig");
const supported = @import("builtin").os.tag == .macos;
extern fn av_decoder_create([*]const u8, usize, u32, u32, c_int, *const fn (*anyopaque, usize, i32, ?*anyopaque) callconv(.c) void, *anyopaque, *?*anyopaque) i32;
extern fn av_decoder_submit(*anyopaque, [*]const u8, usize, usize, i64, i64, u32, u32) i32;
extern fn av_decoder_submit_coded(*anyopaque, [*]const u8, usize, usize, i64, i64, u32) i32;
extern fn av_decoder_drain(*anyopaque) i32;
extern fn av_decoder_destroy(*anyopaque) void;
extern fn av_decoder_hardware(*anyopaque) c_int;
extern fn av_decoder_supports(u32) c_int;
extern fn av_decoder_create_coded(u32, [*]const u8, usize, u32, u32, u32, c_int, *const fn (*anyopaque, usize, i32, ?*anyopaque) callconv(.c) void, *anyopaque, *?*anyopaque) i32;
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

pub const Format = enum(u32) {
    nv12_video = 0x34323076,
    nv12_full = 0x34323066,
    p010_video = 0x78343230,
    p010_full = 0x78663230,
    pub fn fullRange(self: Format) bool {
        return self == .nv12_full or self == .p010_full;
    }
    pub fn bitDepth(self: Format) u8 {
        return if (self == .p010_video or self == .p010_full) 10 else 8;
    }
    pub fn sampleShift(self: Format) u4 {
        return if (self.bitDepth() == 10) 6 else 0;
    }
};
pub const Codec = enum(u32) { h264 = 0x61766331, vp9 = 0x76703039, av1 = 0x61763031 };
pub fn hardwareSupported(codec: Codec) bool {
    return supported and av_decoder_supports(@backingInt(codec)) != 0;
}
/// Caller callback/context must stay at a stable address until deinit fences
/// all callbacks. No asynchronous/temporal processing flags are requested.
pub const CodedSession = struct {
    handle: *anyopaque,
    pub fn init(codec: Codec, config: []const u8, width: u32, height: u32, format: Format, require_hardware: bool, callback: *const fn (*anyopaque, usize, i32, ?*anyopaque) callconv(.c) void, context: *anyopaque) !CodedSession {
        if (!supported) return error.UnsupportedVideoBackend;
        if (codec == .h264) return error.UnsupportedVideoProfile;
        var handle: ?*anyopaque = null;
        const status = av_decoder_create_coded(@backingInt(codec), config.ptr, config.len, width, height, @backingInt(format), @intFromBool(require_hardware), callback, context, &handle);
        if (status == -108) return error.OutOfMemory; // memFullErr
        if (status != 0) return error.VideoDecoderUnavailable;
        return .{ .handle = handle.? };
    }
    pub fn submit(self: CodedSession, bytes: []const u8, id: usize, pts: i64, duration: ?u64, scale: u32) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        const ticks = if (duration) |d| std.math.cast(i64, d) orelse return error.UnsupportedTimeline else @as(i64, -1);
        if (av_decoder_submit_coded(self.handle, bytes.ptr, bytes.len, id, pts, ticks, scale) != 0) return error.VideoDecodeFailed;
    }
    pub fn drain(self: CodedSession) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        if (av_decoder_drain(self.handle) != 0) return error.VideoDecodeFailed;
    }
    pub fn hardware(self: CodedSession) bool {
        return supported and av_decoder_hardware(self.handle) != 0;
    }
    pub fn deinit(self: *CodedSession) void {
        if (supported) av_decoder_destroy(self.handle);
        self.* = undefined;
    }
};
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
    pub fn storageBytes(self: *const Surface) u64 {
        if (!supported) return 0;
        return av_surface_storage_bytes(self.handle);
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
            const row = std.math.mul(usize, width, (if (i == 0) @as(usize, 1) else 2) * (if (self.format.bitDepth() == 8) @as(usize, 1) else 2)) catch return error.ResourceLimitExceeded;
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
    skipped_packets: usize,
    probe_bytes: usize,
    peak_retained_surface_bytes: u64,
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
    seek_mode: decode_plan.Mode = .from_start,
    max_probe_candidates: usize = 256,
    max_probe_bytes: usize = 16 * 1024 * 1024,
    max_search_steps: usize = 1_000_000,
    merge_gap_packets: usize = 0,
};
const Capture = struct {
    reader: *const media.mp4.Reader,
    indexes: []const usize,
    slots: []?Frame,
    seen: []bool,
    control: media.source.Control,
    failure: ?anyerror = null,
    retained: u64 = 0,
    peak_retained: u64 = 0,
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
            if (self.seen[slot]) {
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
            self.peak_retained = @max(self.peak_retained, self.retained);
            self.seen[slot] = true;
            const packet = self.reader.packets[index];
            self.slots[slot] = .{ .decode_index = index, .pts = packet.pts, .duration = packet.duration, .timescale = self.reader.track.timescale, .surface = surface };
            return;
        }
    }
};
/// Sink receives a borrowed picture outside the native callback. Retain/import
/// it during accept if it must outlive that call; cancellation/errors drain the
/// native session before destroying callback state. Single-consumer.
pub const Sink = struct {
    context: *anyopaque,
    accept: *const fn (*anyopaque, *const Frame) anyerror!void,
};
/// Returned order matches indexes. from_start remains the reference/default;
/// verified_idr explicitly opts into conservative dependency-aware seeking.
pub fn decodeSelected(allocator: std.mem.Allocator, reader: *media.mp4.Reader, indexes: []const usize, options: Options) !Batch {
    return decode(allocator, reader, indexes, options, null);
}
/// Streams selected pictures into a consumer and returns owned decode metadata
/// and counters in a Batch with no retained frames.
pub fn decodeTo(allocator: std.mem.Allocator, reader: *media.mp4.Reader, indexes: []const usize, options: Options, sink: Sink) !Batch {
    return decode(allocator, reader, indexes, options, sink);
}
fn flush(capture: *Capture, sink: ?Sink) !void {
    if (capture.failure) |err| return err;
    if (sink) |consumer| for (capture.slots) |*slot| {
        if (slot.*) |*frame| {
            try capture.control.check();
            try consumer.accept(consumer.context, frame);
            capture.retained -= av_surface_storage_bytes(frame.surface.handle);
            frame.surface.deinit();
            slot.* = null;
        }
    };
}
/// Conservative platform picture-pool estimate shared by request admission
/// and session qualification. Opaque driver workspace remains unmeasured.
/// Stable VideoToolbox owner across forward windows. Every call drains native
/// callbacks before releasing borrowed selection/sink state; references persist.
pub const Continuation = struct {
    allocator: std.mem.Allocator,
    handle: ?*anyopaque = null,
    capture: Capture,
    slots: [32]?Frame = @splat(null),
    seen: [32]bool = @splat(false),
    next_packet: usize = 0,
    options: Options,
    pub fn create(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options) !*Continuation {
        if (!supported) return error.UnsupportedVideoBackend;
        const self = try a.create(Continuation);
        self.* = .{ .allocator = a, .capture = .{ .reader = reader, .indexes = &.{}, .slots = &.{}, .seen = &.{}, .control = reader.input.control, .max_retained = options.max_retained_surface_bytes }, .options = options };
        errdefer self.deinit();
        const status = av_decoder_create(reader.track.avcc.ptr, reader.track.avcc.len, reader.track.width, reader.track.height, @intFromBool(options.require_hardware), Capture.callback, &self.capture, &self.handle);
        if (status == -108) return error.OutOfMemory;
        if (status != 0) return error.VideoDecoderUnavailable;
        return self;
    }
    pub fn deinit(self: *Continuation) void {
        if (self.handle) |handle| if (supported) av_decoder_destroy(handle);
        for (&self.slots) |*slot| if (slot.*) |*frame| frame.surface.deinit();
        self.allocator.destroy(self);
    }
    pub fn decodeTo(self: *Continuation, indexes: []const usize, sink: Sink) !usize {
        return self.decodeToControlled(indexes, sink, self.capture.reader.input.control);
    }
    /// All submissions and callbacks drain before the scoped control returns.
    pub fn decodeToControlled(self: *Continuation, indexes: []const usize, sink: Sink, control: media.source.Control) !usize {
        if (!supported or indexes.len == 0 or indexes.len > self.slots.len) return error.InvalidPacketIndex;
        try control.check();
        var planning_reader = self.capture.reader.*;
        const PlanningControl = struct {
            original: media.source.Control,
            current: media.source.Control,
            fn check(raw: ?*const anyopaque) !void {
                const checks: *const @This() = @ptrCast(@alignCast(raw.?));
                try checks.original.check();
                try checks.current.check();
            }
        };
        var checks = PlanningControl{ .original = planning_reader.input.control, .current = control };
        var planning_source = planning_reader.input.*;
        planning_source.control = .{ .context = &checks, .check_fn = PlanningControl.check };
        planning_reader.input = &planning_source;
        var unique: [32]usize = undefined;
        var count: usize = 0;
        for (indexes) |index| {
            if (index < self.next_packet or index >= self.capture.reader.packets.len) return error.InvalidPacketIndex;
            if (std.mem.indexOfScalar(usize, unique[0..count], index) != null) return error.DuplicateFrameSelection;
            unique[count] = index;
            count += 1;
        }
        std.mem.sort(usize, unique[0..count], {}, std.sort.asc(usize));
        var nearby = true;
        for (unique[1..count], unique[0 .. count - 1]) |next, previous| if (next - previous > 32) {
            nearby = false;
            break;
        };
        const first = unique[0];
        const last = unique[count - 1];
        // Keep the common warm-window route free of repeated IDR probing. The
        // inner submission still enforces the complete dependency envelope.
        if (nearby and (self.next_packet != 0 and first <= self.next_packet +| 32 and last - self.next_packet < self.options.max_decode_packets or self.next_packet == 0 and first == 0)) return self.decodeRun(indexes, sink, control);
        // The planner retains verified restart payloads and bounds aggregate
        // submissions. Execute every run instead of widening the first run to
        // the final selection across arbitrarily large holes.
        var plan = try decode_plan.create(self.allocator, &planning_reader, unique[0..count], .{ .mode = .verified_idr, .max_run_packets = self.options.max_decode_packets, .max_decode_packets = self.options.max_decode_packets, .merge_gap_packets = 32 });
        defer plan.deinit();
        var submitted: usize = 0;
        for (plan.runs) |run| {
            var n: usize = 0;
            for (indexes) |index| if (index >= run.first and index <= run.last) {
                unique[n] = index;
                n += 1;
            };
            const limit = self.options.max_decode_packets;
            self.options.max_decode_packets = limit - submitted;
            defer self.options.max_decode_packets = limit;
            submitted += try self.decodeRun(unique[0..n], sink, control);
        }
        return submitted;
    }
    fn decodeRun(self: *Continuation, indexes: []const usize, sink: Sink, control: media.source.Control) !usize {
        var reader_view = self.capture.reader.*;
        const Controls = struct {
            original: media.source.Control,
            current: media.source.Control,
            fn check(raw: ?*const anyopaque) !void {
                const checks: *const @This() = @ptrCast(@alignCast(raw.?));
                try checks.original.check();
                try checks.current.check();
            }
        };
        var controls = Controls{ .original = reader_view.input.control, .current = control };
        var source = reader_view.input.*;
        source.control = .{ .context = &controls, .check_fn = Controls.check };
        reader_view.input = &source;
        const previous = self.capture.control;
        self.capture.control = source.control;
        defer self.capture.control = previous;
        const reader = &reader_view;
        var first = reader.packets.len;
        var last: usize = 0;
        for (indexes) |index| {
            if (index < self.next_packet or index >= reader.packets.len) return error.InvalidPacketIndex;
            first = @min(first, index);
            last = @max(last, index);
        }
        var start = self.next_packet;
        if (self.next_packet == 0 or first > self.next_packet +| 32 or last - self.next_packet >= self.options.max_decode_packets) {
            var plan = try decode_plan.create(self.allocator, reader, indexes, .{ .mode = .verified_idr, .max_decode_packets = self.options.max_decode_packets });
            defer plan.deinit();
            if (plan.runs.len == 0) return error.UnverifiedDecodeStart;
            const restart = plan.runs[0].first;
            if (self.next_packet == 0 or restart > self.next_packet +| 32 or last - self.next_packet >= self.options.max_decode_packets) {
                start = restart;
                if (self.next_packet != 0) {
                    // A fresh session prevents delayed pictures and reference
                    // state from the old GOP crossing this verified IDR.
                    var replacement: ?*anyopaque = null;
                    const status = av_decoder_create(reader.track.avcc.ptr, reader.track.avcc.len, reader.track.width, reader.track.height, @intFromBool(self.options.require_hardware), Capture.callback, &self.capture, &replacement);
                    if (status == -108) return error.OutOfMemory;
                    if (status != 0) return error.VideoDecoderUnavailable;
                    av_decoder_destroy(self.handle.?);
                    self.handle = replacement;
                }
            }
        }
        if (last - start + 1 > self.options.max_decode_packets) return error.ResourceLimitExceeded;
        if (try surfaceAdmissionBytes(reader.track.width, reader.track.height, indexes.len) > self.options.max_surface_bytes) return error.ResourceLimitExceeded;
        @memset(&self.seen, false);
        self.capture.indexes = indexes;
        self.capture.slots = self.slots[0..indexes.len];
        self.capture.seen = self.seen[0..indexes.len];
        var drained = false;
        defer if (!drained) {
            _ = av_decoder_drain(self.handle.?);
        };
        for (start..last + 1) |index| {
            try reader.input.control.check();
            var lease = try reader.readPacket(index);
            defer lease.deinit();
            const packet = reader.packets[index];
            if (lease.bytes.len > self.options.max_packet_bytes) return error.ResourceLimitExceeded;
            try avc.validatePacket(lease.bytes, reader.track.nal_length_bytes);
            if (av_decoder_submit(self.handle.?, lease.bytes.ptr, lease.bytes.len, index, packet.pts, packet.dts, packet.duration, reader.track.timescale) != 0) return error.VideoDecodeFailed;
            try flush(&self.capture, sink);
        }
        const drain_status = av_decoder_drain(self.handle.?);
        drained = true;
        if (drain_status != 0) return error.VideoDecodeFailed;
        try flush(&self.capture, sink);
        for (self.seen[0..indexes.len]) |seen| if (!seen) return error.MissingDecodedFrame;
        self.next_packet = last + 1;
        return last - start + 1;
    }
    pub fn hardware(self: *const Continuation) bool {
        return supported and av_decoder_hardware(self.handle.?) != 0;
    }
};

pub fn surfaceAdmissionBytes(width: u32, height: u32, selections: usize) !u64 {
    return surfaceAdmissionBytesForDepth(width, height, selections, 8);
}
pub fn surfaceAdmissionBytesForDepth(width: u32, height: u32, selections: usize, depth: u8) !u64 {
    const row = std.mem.alignForward(u64, @as(u64, width) + 256, 256);
    const per_frame = try std.math.mul(u64, row, (@as(u64, height) + 32) * (if (depth > 8) @as(u64, 4) else 2));
    return std.math.mul(u64, per_frame, try std.math.add(u64, selections, 24));
}
fn decode(allocator: std.mem.Allocator, reader: *media.mp4.Reader, indexes: []const usize, options: Options, sink: ?Sink) !Batch {
    if (!supported) return error.UnsupportedVideoBackend;
    try reader.input.control.check();
    if (reader.track.timescale > std.math.maxInt(i32)) return error.UnsupportedTimeline;
    var plan = try decode_plan.create(allocator, reader, indexes, .{
        .mode = options.seek_mode,
        .max_selections = options.max_frames,
        .max_decode_packets = options.max_decode_packets,
        .max_probe_candidates = options.max_probe_candidates,
        .max_probe_bytes = options.max_probe_bytes,
        .max_packet_bytes = options.max_packet_bytes,
        .max_search_steps = options.max_search_steps,
        .merge_gap_packets = options.merge_gap_packets,
    });
    defer plan.deinit();
    // Conservative bound includes native row alignment, all selected pictures,
    // and a fixed extra decoder pool (16 reference pictures + 8 outputs).
    if (try surfaceAdmissionBytes(reader.track.width, reader.track.height, indexes.len) > options.max_surface_bytes) return error.ResourceLimitExceeded;
    const slots = try allocator.alloc(?Frame, indexes.len);
    defer allocator.free(slots);
    @memset(slots, null);
    errdefer for (slots) |*slot| if (slot.*) |*f| f.surface.deinit();
    const seen = try allocator.alloc(bool, indexes.len);
    defer allocator.free(seen);
    @memset(seen, false);
    var capture = Capture{ .reader = reader, .indexes = indexes, .slots = slots, .seen = seen, .control = reader.input.control, .max_retained = options.max_retained_surface_bytes };
    var handle: ?*anyopaque = null;
    const creation = av_decoder_create(reader.track.avcc.ptr, reader.track.avcc.len, reader.track.width, reader.track.height, @intFromBool(options.require_hardware), Capture.callback, &capture, &handle);
    if (creation == -108) return error.OutOfMemory;
    if (creation != 0) return error.VideoDecoderUnavailable;
    // This defer runs before capture/slot cleanup on every failure path.
    defer av_decoder_destroy(handle.?);
    for (plan.runs) |run| {
        for (run.first..run.last + 1) |index| {
            try reader.input.control.check();
            const packet = reader.packets[index];
            if (packet.size > options.max_packet_bytes) return error.ResourceLimitExceeded;
            var lease: ?media.source.Lease = null;
            defer if (lease) |*owned| owned.deinit();
            const bytes = plan.anchorBytes(index) orelse blk: {
                lease = try reader.readPacket(index);
                break :blk lease.?.bytes;
            };
            try avc.validatePacket(bytes, reader.track.nal_length_bytes);
            if (index == run.first and index != 0 and !try avc.isIdr(bytes, reader.track.nal_length_bytes)) return error.UnverifiedDecodeStart;
            if (av_decoder_submit(handle.?, bytes.ptr, bytes.len, index, packet.pts, packet.dts, packet.duration, reader.track.timescale) != 0) return error.VideoDecodeFailed;
            try flush(&capture, sink);
        }
        // Fence before crossing a skipped dependency region. The next run starts
        // at an IDR picture, which resets reference pictures in the same session.
        if (av_decoder_drain(handle.?) != 0) return error.VideoDecodeFailed;
        try flush(&capture, sink);
    }
    const hardware = av_decoder_hardware(handle.?) != 0;
    if (options.require_hardware and !hardware) return error.HardwareVideoDecoderRequired;
    if (capture.failure) |err| return err;
    try reader.input.control.check();
    for (seen) |received| if (!received) return error.MissingDecodedFrame;
    const color_info = try allocator.dupe(u8, reader.track.color_info);
    errdefer allocator.free(color_info);
    const frames = try allocator.alloc(Frame, if (sink != null) 0 else indexes.len);
    if (sink == null) for (frames, slots) |*frame, slot| {
        frame.* = slot.?;
    };
    return .{ .allocator = allocator, .frames = frames, .hardware = hardware, .submitted_packets = plan.submitted_packets, .skipped_packets = plan.skipped_packets, .probe_bytes = plan.probe_bytes, .peak_retained_surface_bytes = capture.peak_retained, .display_matrix = reader.track.display_matrix, .pixel_aspect = reader.track.pixel_aspect, .color_info = color_info };
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
