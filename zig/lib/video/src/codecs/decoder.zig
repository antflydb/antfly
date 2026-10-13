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

//! Maintained pure Zig VP9/AV1 reconstruction; no codec library or C build.
const std = @import("std");
const builtin = @import("builtin");
const media = @import("antfly_media");
const runtime = @import("runtime.zig");
const av1 = @import("av1/lib.zig");
const vp9 = @import("vp9/vpx_decoder.zig");
const vp9_iface = @import("vp9/vp9_dx_iface.zig");
const header = @import("vp9_header.zig");
const frame = @import("frame.zig");
const annex_b = @import("annex_b.zig");
comptime {
    _ = @import("av1/root.zig");
    _ = @import("vp9/root.zig");
}
pub const Frame = frame.Frame;
pub const Batch = frame.Batch;
pub const Codec = enum { vp9, av1 };
pub const Av1Framing = enum { low_overhead, annex_b };
/// Selection applies to native visible-output ordinals within one coded packet.
/// Every coded part and output is still reconstructed, validated and drained.
pub const OutputSelection = union(enum) {
    all,
    none,
    indexes: []const usize,
    fn contains(self: @This(), index: usize) bool {
        return switch (self) {
            .all => true,
            .none => false,
            .indexes => |ids| std.mem.indexOfScalar(usize, ids, index) != null,
        };
    }
};
pub const OutputStats = struct { native_outputs: usize = 0, skipped_outputs: usize = 0, copied_bytes: u64 = 0 };
pub const Options = struct {
    av1_framing: Av1Framing = .low_overhead,
    av1_operating_point: u5 = 0,
    av1_all_layers: bool = true,
    max_annex_b_frame_units: usize = 256,
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_pixels: usize = 16 * 1024 * 1024,
    max_decode_bytes: usize = 256 * 1024 * 1024,
    max_output_bytes: usize = 128 * 1024 * 1024,
    max_output_frames: usize = 8,
    admission_pool: ?*media.admission.Pool = null,
    control: media.source.Control = .{},
};
/// One sequential owner per coded stream. Different sessions may run concurrently;
/// a single session must not be entered concurrently. Batch frames outlive it.
/// Preflight failures are retryable. Errors after core entry invalidate the
/// session: destroy it and replay from a synchronization point in a new session.
pub const Session = struct {
    allocator: std.mem.Allocator,
    codec: Codec,
    options: Options,
    owner: runtime.Owner,
    reservation: media.admission.Token,
    av1_context: [*c]av1.Dav1dContext = null,
    vp9_context: vp9.vpx_codec_ctx_t = .{},
    vp9_config: vp9.vpx_codec_dec_cfg_t = .{ .threads = 1 },
    references: header.References = @splat(null),
    output_stats: OutputStats = .{},
    failed: bool = false,
    webm_initialized: bool = false,
    pub fn create(allocator: std.mem.Allocator, codec: Codec, options: Options) !*Session {
        if ((builtin.os.tag != .linux and builtin.os.tag != .macos) or builtin.cpu.arch.endian() != .little) return error.UnsupportedVideoBackend;
        try options.control.check();
        if (options.max_pixels == 0 or options.max_pixels > 16 * 1024 * 1024 or options.max_output_frames == 0 or options.max_output_frames > 64) return error.InvalidDecodeOptions;
        var reservation = if (options.admission_pool) |pool| try pool.acquire(.{ .host_bytes = options.max_decode_bytes }) else media.admission.Token{};
        errdefer reservation.deinit();
        const self = try allocator.create(Session);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .codec = codec, .options = options, .reservation = reservation, .owner = .{ .budget = .{ .backing = allocator, .limit = options.max_decode_bytes }, .control = options.control } };
        errdefer self.owner.sweep();
        const scope = runtime.Scope.enter(&self.owner);
        defer scope.deinit();
        const result = switch (codec) {
            .av1 => block: {
                var settings: av1.Dav1dSettings = undefined;
                av1.antfly_av1_dav1d_default_settings(&settings);
                settings.operating_point = options.av1_operating_point;
                settings.all_layers = @intFromBool(options.av1_all_layers);
                settings.n_threads = 1;
                settings.max_frame_delay = 1;
                settings.strict_std_compliance = 1;
                settings.frame_size_limit = @intCast(options.max_pixels);
                break :block av1.antfly_av1_dav1d_open(&self.av1_context, &settings);
            },
            .vp9 => @as(c_int, @intCast(vp9.antfly_vp9_vpx_codec_dec_init_ver(&self.vp9_context, @ptrCast(vp9_iface.antfly_vp9_vpx_codec_vp9_dx()), &self.vp9_config, 0, 12))),
        };
        try self.owner.result();
        if (result != 0) return error.InvalidVideoData;
        return self;
    }
    pub fn destroy(self: *Session) void {
        // There are no worker threads, external buffers, OS handles or callbacks
        // in these configurations. Reclaim every allocation even after a parser
        // abort, without calling a destructor on partially initialized state.
        self.owner.sweep();
        self.reservation.deinit();
        self.allocator.destroy(self);
    }
    fn invalidate(self: *Session) void {
        self.failed = true;
        self.owner.sweep();
        self.reservation.deinit();
        self.av1_context = null;
        self.vp9_context = .{};
    }
    pub fn decode(self: *Session, packet: []const u8, pts_ns: i64, duration_ns: ?u64) !Batch {
        return self.decodeOutputs(packet, pts_ns, duration_ns, .all);
    }
    /// Skipping materialization never skips entropy decoding or reference updates.
    /// Output byte limits apply to retained frames; native-output count limits
    /// apply equally to selected and skipped pictures. Preflight is retryable.
    pub fn decodeOutputs(self: *Session, packet: []const u8, pts_ns: i64, duration_ns: ?u64, selection: OutputSelection) !Batch {
        try self.options.control.check();
        if (self.failed) return error.InvalidDecoderSession;
        if (packet.len == 0) return error.InvalidVideoData;
        if (packet.len > self.options.max_packet_bytes or packet.len > std.math.maxInt(c_uint)) return error.ResourceLimitExceeded;
        if (selection == .indexes) {
            if (selection.indexes.len > self.options.max_output_frames) return error.ResourceLimitExceeded;
            for (selection.indexes) |id| if (id >= self.options.max_output_frames) return error.InvalidPacketIndex;
        }
        var parts: header.Parts = .{};
        var references = self.references;
        var normalized: ?annex_b.Units = null;
        defer if (normalized) |*units| units.deinit();
        if (self.codec == .vp9) {
            parts = try header.split(packet);
            for (parts.packets[0..parts.len]) |part| try header.preflight(part, &references, self.options.max_pixels);
        } else {
            parts.packets[0] = packet;
            parts.len = 1;
        }
        if (self.codec == .av1 and self.options.av1_framing == .annex_b) {
            normalized = annex_b.normalize(self.owner.budget.allocator(), packet, self.options.max_packet_bytes, self.options.max_annex_b_frame_units, self.options.control) catch |err| {
                if (self.owner.budget.denied) {
                    self.owner.budget.denied = false;
                    return error.ResourceLimitExceeded;
                }
                return err;
            };
        }
        const coded_parts = if (normalized) |units| units.packets else parts.packets[0..parts.len];
        var frames: std.array_list.Managed(Frame) = .init(self.allocator);
        errdefer {
            for (frames.items) |*item| item.deinit();
            frames.deinit();
        }
        var output_bytes: usize = 0;
        var output_count: usize = 0;
        const scope = runtime.Scope.enter(&self.owner);
        defer scope.deinit();
        errdefer {
            // Drop normalization storage before releasing shared core admission.
            if (normalized) |*units| units.deinit();
            normalized = null;
            self.invalidate();
        }
        for (coded_parts) |part| switch (self.codec) {
            .av1 => {
                var input: av1.Dav1dData = .{};
                const buffer = av1.antfly_av1_dav1d_data_create(&input, part.len);
                if (buffer == null) {
                    try self.owner.result();
                    return error.OutOfMemory;
                }
                @memcpy(buffer[0..part.len], part);
                // A failed core call may leave input referenced. Its allocations
                // remain in owner and are swept by the failure path.
                const sent = av1.antfly_av1_dav1d_send_data(self.av1_context, &input);
                av1.antfly_av1_dav1d_data_unref(&input);
                try self.owner.result();
                if (sent != 0) return error.InvalidVideoData;
                while (true) {
                    var picture: av1.Dav1dPicture = .{};
                    const status = av1.antfly_av1_dav1d_get_picture(self.av1_context, &picture);
                    try self.owner.result();
                    if (status == -35) break; // private port's need-input status
                    if (status != 0) return error.InvalidVideoData;
                    defer av1.antfly_av1_dav1d_picture_unref(&picture);
                    if (picture.p.w <= 0 or picture.p.h <= 0 or picture.stride[0] < 0 or picture.stride[1] < 0) return error.InvalidVideoData;
                    if (picture.seq_hdr.*.profile > 2 or picture.p.layout < 0 or picture.p.layout > 3 or (picture.p.bpc != 8 and picture.p.bpc != 10 and picture.p.bpc != 12)) return error.UnsupportedVideoProfile;
                    const y = picture.data[0] orelse return error.InvalidVideoData;
                    const u = if (picture.p.layout == 0) y else picture.data[1] orelse return error.InvalidVideoData;
                    const v = if (picture.p.layout == 0) y else picture.data[2] orelse return error.InvalidVideoData;
                    const seq = picture.seq_hdr.*;
                    if (self.options.av1_operating_point >= seq.num_operating_points) return error.InvalidDecodeOptions;
                    _ = try frame.validateLayout(@intCast(picture.p.w), @intCast(picture.p.h), @intCast(picture.p.bpc), @intCast(picture.p.layout), .{ @intCast(picture.stride[0]), @intCast(picture.stride[1]), @intCast(picture.stride[1]) });
                    const output_index = try self.observeOutput(&output_count);
                    if (!selection.contains(output_index)) {
                        self.output_stats.skipped_outputs +|= 1;
                        continue;
                    }
                    var copy = try frame.copy(self.allocator, self.options.admission_pool, @intCast(picture.p.w), @intCast(picture.p.h), @intCast(picture.p.bpc), @intCast(picture.p.layout), .{ .full_range = seq.color_range != 0, .primaries = @intCast(seq.pri), .transfer = @intCast(seq.trc), .matrix = @intCast(seq.mtrx) }, .{ @ptrCast(y), @ptrCast(u), @ptrCast(v) }, .{ @intCast(picture.stride[0]), @intCast(picture.stride[1]), @intCast(picture.stride[1]) }, self.options.max_output_bytes - output_bytes);
                    errdefer copy.deinit();
                    copy.output_index = output_index;
                    self.output_stats.copied_bytes +|= copy.bytes.len;
                    try self.append(&frames, &copy, &output_bytes, pts_ns, duration_ns);
                }
            },
            .vp9 => {
                const status = vp9.antfly_vp9_vpx_codec_decode(&self.vp9_context, part.ptr, @intCast(part.len), null, 0);
                try self.owner.result();
                if (status != 0) return error.InvalidVideoData;
                var iterator: vp9.vpx_codec_iter_t = null;
                while (true) {
                    const picture = vp9.antfly_vp9_vpx_codec_get_frame(&self.vp9_context, &iterator);
                    try self.owner.result();
                    if (picture == null) break;
                    const p = picture.*;
                    if (p.stride[0] < 0 or p.stride[1] < 0 or p.stride[2] < 0 or p.x_chroma_shift > 1 or p.y_chroma_shift > 1 or (p.x_chroma_shift == 0 and p.y_chroma_shift != 0)) return error.UnsupportedVideoProfile;
                    _ = try frame.validateLayout(p.d_w, p.d_h, @intCast(p.bit_depth), if (p.x_chroma_shift == 0) 3 else if (p.y_chroma_shift == 0) 2 else 1, .{ @intCast(p.stride[0]), @intCast(p.stride[1]), @intCast(p.stride[2]) });
                    const output_index = try self.observeOutput(&output_count);
                    if (!selection.contains(output_index)) {
                        self.output_stats.skipped_outputs +|= 1;
                        continue;
                    }
                    var copy = try frame.copy(self.allocator, self.options.admission_pool, p.d_w, p.d_h, @intCast(p.bit_depth), if (p.x_chroma_shift == 0) 3 else if (p.y_chroma_shift == 0) 2 else 1, .{ .full_range = p.range != 0, .matrix = switch (p.cs) {
                        1, 3 => 6,
                        2 => 1,
                        4 => 7,
                        5 => 9,
                        7 => 0,
                        else => 2,
                    } }, .{ p.planes[0], p.planes[1], p.planes[2] }, .{ @intCast(p.stride[0]), @intCast(p.stride[1]), @intCast(p.stride[2]) }, self.options.max_output_bytes - output_bytes);
                    errdefer copy.deinit();
                    copy.output_index = output_index;
                    self.output_stats.copied_bytes +|= copy.bytes.len;
                    try self.append(&frames, &copy, &output_bytes, pts_ns, duration_ns);
                }
            },
        };
        try self.owner.result();
        try self.options.control.check();
        self.references = references;
        return .{ .allocator = self.allocator, .frames = try frames.toOwnedSlice(), .output_count = output_count };
    }
    fn observeOutput(self: *Session, count: *usize) !usize {
        if (count.* >= self.options.max_output_frames) return error.ResourceLimitExceeded;
        const index = count.*;
        count.* += 1;
        self.output_stats.native_outputs +|= 1;
        return index;
    }
    fn append(self: *Session, frames: *std.array_list.Managed(Frame), copy: *Frame, bytes: *usize, pts: i64, duration: ?u64) !void {
        if (frames.items.len >= self.options.max_output_frames) return error.ResourceLimitExceeded;
        copy.pts_ns = pts;
        copy.duration_ns = duration;
        copy.decode_high_water = self.owner.budget.peak;
        try frames.append(copy.*);
        bytes.* += copy.bytes.len;
    }
    /// Loads configuration OBUs before a seek-specific sequence header. No
    /// coded pictures may be produced by codec-private initialization.
    pub fn initializeWebm(self: *Session, reader: *media.webm.Reader) !void {
        try reader.input.control.check();
        if (self.failed) return error.InvalidDecoderSession;
        if (self.codec == .av1 and self.options.av1_framing != .low_overhead) return error.InvalidDecodeOptions;
        if ((self.codec == .av1 and reader.track.codec != .av1) or (self.codec == .vp9 and reader.track.codec != .vp9)) return error.UnsupportedVideoCodec;
        if (!self.webm_initialized and self.codec == .av1 and reader.track.codec_private.len != 0) {
            const config = reader.track.codec_private;
            if (config.len < 4 or config[0] != 0x81) return error.InvalidVideoData;
            if (config.len > 4) {
                var initialization = try self.decodeWebmBytes(reader, config[4..], 0, null);
                defer initialization.deinit();
                if (initialization.frames.len != 0) {
                    self.invalidate();
                    return error.InvalidVideoData;
                }
            }
        }
        self.webm_initialized = true;
    }
    /// Borrows bytes only during this call and joins source/decoder controls.
    /// Used for bounded sequence bootstrap as well as ordinary packet replay.
    pub fn decodeWebmBytes(self: *Session, reader: *media.webm.Reader, bytes: []const u8, pts: i64, duration: ?u64) !Batch {
        try reader.input.control.check();
        const Pair = struct {
            first: media.source.Control,
            second: media.source.Control,
            fn check(context: ?*const anyopaque) !void {
                const pair: *@This() = @ptrCast(@alignCast(@constCast(context.?)));
                try pair.first.check();
                try pair.second.check();
            }
        };
        var controls = Pair{ .first = self.options.control, .second = reader.input.control };
        const original = self.owner.control;
        self.owner.control = .{ .context = &controls, .check_fn = Pair.check };
        defer self.owner.control = original;
        var batch = try self.decode(bytes, pts, duration);
        errdefer {
            batch.deinit();
            self.invalidate();
        }
        try reader.input.control.check();
        return batch;
    }
    pub fn decodeWebmPacket(self: *Session, reader: *media.webm.Reader, index: usize) !Batch {
        try reader.input.control.check();
        if (self.codec == .av1 and self.options.av1_framing != .low_overhead) return error.InvalidDecodeOptions;
        if (index >= reader.packets.len) return error.InvalidPacketIndex;
        if ((self.codec == .av1 and reader.track.codec != .av1) or (self.codec == .vp9 and reader.track.codec != .vp9)) return error.UnsupportedVideoCodec;
        const packet = reader.packets[index];
        if (packet.size > self.options.max_packet_bytes) return error.ResourceLimitExceeded;
        try self.initializeWebm(reader);
        var lease = try reader.readPacket(index);
        defer lease.deinit();
        return self.decodeWebmBytes(reader, lease.bytes, packet.pts, packet.duration_ns);
    }
};

test "VP9 cancellation at a frame view boundary leaves the view valid until the next checkpoint" {
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    inline for (.{ @import("vp9/vp9_decodeframe.zig"), @import("vp9/vp9_decoder.zig") }) |core| {
        var pool: core.BufferPool = .{};
        var common = core.VP9_COMMON{ .buffer_pool = &pool, .new_fb_idx = 0 };
        var owner = runtime.Owner{ .budget = .{ .backing = std.testing.allocator, .limit = 1024 }, .control = .{ .check_fn = Cancel.check }, .polls = 254 };
        const scope = runtime.Scope.enter(&owner);
        defer scope.deinit();
        try std.testing.expectEqual(@as(c_int, 0), core.antfly_codec_afz_vp9_failed());
        try std.testing.expectEqual(@as(usize, 255), owner.polls);
        const view = core.get_frame_new_buffer(&common);
        try std.testing.expectEqual(@as(usize, 255), owner.polls);
        try std.testing.expect(view != null);
        view.*.render_width = 32;
        try std.testing.expect(runtime.antfly_codec_afz_vp9_failed() != 0);
        try std.testing.expectError(error.Cancelled, owner.result());
        try std.testing.expectEqual(@as(c_int, 32), pool.frame_bufs[0].buf.render_width);
    }
}
