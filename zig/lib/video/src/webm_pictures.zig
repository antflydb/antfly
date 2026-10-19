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

//! Logical displayed pictures for WebM model sampling. Indexing retains metadata
//! only; replay reconstructs dependencies once and publishes selected pictures.
const std = @import("std");
test {
    _ = @import("codecs/av1_headers.zig");
}
const media = @import("antfly_media");
const codecs = @import("codecs/decoder.zig");
const frames = @import("codecs/frame.zig");
const vp8 = @import("vp8.zig");
const sampling = @import("sampling.zig");
pub const Picture = struct {
    pts: i64,
    duration: ?u64,
    width: u32,
    height: u32,
    bit_depth: u8 = 8,
    chroma_format: u8 = 1,
    /// Present for header indexes; reconstructed indexes remain portable-only.
    packet_index: ?usize = null,
    color: frames.Color = .{},
    profile: u8 = 0,
    av1_config: ?[4]u8 = null,
    apple_compatible: bool = true,
};
pub const Restart = struct { packet: usize = 0, logical: usize = 0, sequence: ?struct { packet: usize, offset: usize, size: usize } = null };
pub const ReplayStats = struct { first_packet: usize = 0, packets_decoded: usize = 0, bootstrap_bytes: usize = 0, output_copy_bytes: u64 = 0, outputs_skipped: usize = 0 };
pub const Options = struct { max_packets: usize = 8192, max_pictures: usize = 65536, decoder: codecs.Options = .{}, strategy: enum { headers, reconstruct } = .headers, replay_stats: ?*ReplayStats = null, copy_unselected_outputs: bool = false };
pub const Index = struct {
    allocator: std.mem.Allocator,
    pictures: []Picture,
    restarts: []Restart = &.{},
    reader: ?*const media.webm.Reader = null,
    apple_packet_layout: bool = false,
    operating_point: u5 = 0,
    all_layers: bool = true,
    reservation: media.admission.Token = .{},
    duration_seconds: ?f64 = null,
    pub fn deinit(self: *Index) void {
        self.allocator.free(self.pictures);
        self.allocator.free(self.restarts);
        self.reservation.deinit();
        self.* = undefined;
    }
    pub fn select(self: Index) ![]usize {
        if (self.pictures.len == 0) return error.EmptyVideo;
        const order = try self.allocator.alloc(usize, self.pictures.len);
        defer self.allocator.free(order);
        for (order, 0..) |*id, i| id.* = i;
        const Less = struct {
            fn less(pictures: []const Picture, l: usize, r: usize) bool {
                return pictures[l].pts < pictures[r].pts or (pictures[l].pts == pictures[r].pts and l < r);
            }
        };
        std.mem.sort(usize, order, @as([]const Picture, self.pictures), Less.less);
        const first = self.pictures[order[0]].pts;
        const last = self.pictures[order[order.len - 1]];
        // Without an explicit last duration, infer the final positive PTS step.
        // If no rate can be established, upstream falls back to capped indices.
        var tail = last.duration;
        if (tail == null) {
            var i = order.len - 1;
            while (i > 0) : (i -= 1) {
                const delta = @as(i128, last.pts) - self.pictures[order[i - 1]].pts;
                if (delta > 0) {
                    tail = std.math.cast(u64, delta) orelse return error.TimestampOverflow;
                    break;
                }
            }
        }
        const span = @as(i128, last.pts) - first + (tail orelse 0);
        const seconds: ?f64 = self.duration_seconds orelse if (span > 0) @as(f64, @floatFromInt(span)) / 1e9 else null;
        const selected = try sampling.embeddingGemma2(self.allocator, .{ .total_frames = order.len, .duration_seconds = seconds, .fps = if (seconds) |s| @as(f64, @floatFromInt(order.len)) / s else null }, .{});
        for (selected) |*id| id.* = order[id.*];
        return selected;
    }
};
const Collect = struct {
    allocator: std.mem.Allocator,
    pictures: std.ArrayList(Picture) = .empty,
    limit: usize,
    restarts: std.ArrayList(Restart) = .empty,
    restart_limit: usize = 8192,
    apple_packet_layout: bool = true,
    fn restart(self: *Collect, value: Restart) !void {
        if (self.restarts.items.len >= self.restart_limit) return error.ResourceLimitExceeded;
        if (self.restarts.items.len == self.restarts.capacity) {
            const growth = self.restarts.capacity +| (self.restarts.capacity / 2 +| 8);
            try self.restarts.ensureTotalCapacityPrecise(self.allocator, @min(self.restart_limit, growth));
        }
        self.restarts.appendAssumeCapacity(value);
    }
    fn accept(raw: *anyopaque, _: usize, frame: *const codecs.Frame) !void {
        const self: *Collect = @ptrCast(@alignCast(raw));
        try self.push(.{ .pts = frame.pts_ns, .duration = frame.duration_ns, .width = frame.width, .height = frame.height, .bit_depth = frame.bit_depth, .chroma_format = frame.chroma_format, .color = frame.color });
    }
    fn push(self: *Collect, picture: Picture) !void {
        if (self.pictures.items.len >= self.limit) return error.ResourceLimitExceeded;
        if (self.pictures.items.len == self.pictures.capacity) {
            // Cap capacity at the declared picture limit. The admission budget
            // covers both old and replacement buffers during allocator growth.
            const growth = self.pictures.capacity +| (self.pictures.capacity / 2 +| 8);
            try self.pictures.ensureTotalCapacityPrecise(self.allocator, @min(self.limit, growth));
        }
        self.pictures.appendAssumeCapacity(picture);
    }
};
pub fn index(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options) !Index {
    const picture_budget = std.math.mul(usize, @max(options.max_pictures, 8), 2 * @sizeOf(Picture)) catch return error.ResourceLimitExceeded;
    const restart_budget = std.math.mul(usize, @max(options.max_packets, 8), 2 * @sizeOf(Restart)) catch return error.ResourceLimitExceeded;
    const array_budget = std.math.add(usize, picture_budget, restart_budget) catch return error.ResourceLimitExceeded;
    const metadata_bytes = std.math.add(usize, array_budget, @sizeOf(@import("codecs/av1_headers.zig").Session)) catch return error.ResourceLimitExceeded;
    var reservation = if (options.decoder.admission_pool) |pool| try pool.acquire(.{ .host_bytes = metadata_bytes }) else media.admission.Token{};
    errdefer reservation.deinit();
    var collector = Collect{ .allocator = a, .limit = options.max_pictures, .restart_limit = options.max_packets };
    defer collector.restarts.deinit(a);
    defer collector.pictures.deinit(a);
    if (options.strategy == .headers) {
        try indexHeaders(reader, options, &collector);
    } else try walk(a, reader, options, null, &collector, Collect.accept);
    const pictures = try collector.pictures.toOwnedSlice(a);
    errdefer a.free(pictures);
    const restarts = try collector.restarts.toOwnedSlice(a);
    errdefer a.free(restarts);
    try reservation.resize(.{ .host_bytes = pictures.len * @sizeOf(Picture) + restarts.len * @sizeOf(Restart) });
    return .{ .allocator = a, .pictures = pictures, .restarts = restarts, .reader = reader, .apple_packet_layout = options.strategy == .headers and collector.apple_packet_layout, .operating_point = options.decoder.av1_operating_point, .all_layers = options.decoder.av1_all_layers, .reservation = reservation, .duration_seconds = reader.duration_seconds };
}
/// Header indexing validates visible-picture geometry/reference metadata. Pixel
/// reconstruction and entropy validity remain the selected replay's responsibility.
fn indexHeaders(reader: *media.webm.Reader, options: Options, collector: *Collect) !void {
    if (reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
    var references: @import("codecs/vp9_header.zig").References = @splat(null);
    var vp8_geometry: ?@import("codecs/vp9_header.zig").Geometry = null;
    const av1 = if (reader.track.codec == .av1) try @import("codecs/av1_headers.zig").Session.create(collector.allocator, .{ .max_pixels = options.decoder.max_pixels, .operating_point = options.decoder.av1_operating_point, .all_layers = options.decoder.av1_all_layers, .control = options.decoder.control }) else null;
    defer if (av1) |session| session.deinit();
    const Av1Publish = struct {
        collector: *Collect,
        packet: media.webm.Packet,
        packet_id: usize,
        fn accept(raw: *anyopaque, geometry: @import("codecs/vp9_header.zig").Geometry) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.packet.invisible) try appendHeader(self.collector, self.packet, self.packet_id, geometry);
        }
    };
    if (av1) |session| {
        const config = reader.track.codec_private;
        if (config.len != 0) {
            if (config.len < 4 or config[0] != 0x81) return error.InvalidVideoData;
            const Reject = struct {
                fn picture(_: *anyopaque, _: @import("codecs/vp9_header.zig").Geometry) !void {
                    return error.InvalidVideoData;
                }
            };
            var dummy: u8 = 0;
            if (config.len > 4) try session.walk(config[4..], &dummy, Reject.picture);
        }
    }
    var sequence: @FieldType(Restart, "sequence") = null;
    for (reader.packets, 0..) |packet, packet_id| {
        const logical_before = collector.pictures.items.len;
        try options.decoder.control.check();
        try reader.input.control.check();
        if (packet.size > options.decoder.max_packet_bytes) return error.ResourceLimitExceeded;
        var lease = try reader.readPacket(packet_id);
        defer lease.deinit();
        if (reader.track.codec == .vp8) {
            const bytes = lease.bytes;
            if (bytes.len < 3) return error.InvalidVideoData;
            const tag = @as(u32, bytes[0]) | (@as(u32, bytes[1]) << 8) | (@as(u32, bytes[2]) << 16);
            const key = tag & 1 == 0;
            if ((tag >> 1) & 7 > 3) return error.UnsupportedVideoProfile;
            const start: usize = if (key) 10 else 3;
            if (bytes.len < start or tag >> 5 > bytes.len - start) return error.InvalidVideoData;
            if (key) {
                if (!std.mem.eql(u8, bytes[3..6], &.{ 0x9d, 0x01, 0x2a })) return error.InvalidVideoData;
                const width = std.mem.readInt(u16, bytes[6..8], .little);
                const height = std.mem.readInt(u16, bytes[8..10], .little);
                if (width >> 14 != 0 or height >> 14 != 0) return error.UnsupportedVideoProfile;
                vp8_geometry = .{ .width = width, .height = height, .color = .{ .matrix = 6 } };
                try collector.restart(.{ .packet = packet_id, .logical = logical_before });
                if (width != reader.track.width or height != reader.track.height) return error.UnsupportedDynamicGeometry;
            }
            const g = vp8_geometry orelse return error.InvalidVideoData;
            if (g.width == 0 or g.height == 0 or @as(u64, g.width) * g.height > options.decoder.max_pixels) return error.ResourceLimitExceeded;
            if (tag & 16 != 0 and !packet.invisible) try appendHeader(collector, packet, packet_id, g);
        } else if (av1) |session| {
            const sequence_before = sequence;
            var publish = Av1Publish{ .collector = collector, .packet = packet, .packet_id = packet_id };
            try session.walk(lease.bytes, &publish, Av1Publish.accept);
            if (session.packet_sequence) |span| sequence = .{ .packet = packet_id, .offset = span.offset, .size = span.size };
            if (session.packet_restart_key) {
                try collector.restart(.{ .packet = packet_id, .logical = logical_before, .sequence = if (session.packet_restart_needs_sequence) sequence_before else null });
            }
        } else {
            const parts = try @import("codecs/vp9_header.zig").split(lease.bytes);
            if (parts.len != 1) collector.apple_packet_layout = false;
            for (parts.packets[0..parts.len], 0..) |part, part_id| {
                const picture = try @import("codecs/vp9_header.zig").inspect(part, &references, options.decoder.max_pixels);
                if (part_id == 0 and picture.key) try collector.restart(.{ .packet = packet_id, .logical = logical_before });
                if (picture.visible and !packet.invisible) try appendHeader(collector, packet, packet_id, picture.geometry);
            }
        }
    }
    if (av1) |session| try session.finish();
}
fn appendHeader(collector: *Collect, packet: media.webm.Packet, packet_id: usize, geometry: @import("codecs/vp9_header.zig").Geometry) !void {
    try collector.push(.{ .pts = packet.pts, .duration = packet.duration_ns, .width = geometry.width, .height = geometry.height, .bit_depth = geometry.bit_depth, .chroma_format = geometry.chroma_format, .packet_index = packet_id, .color = geometry.color, .profile = geometry.profile, .av1_config = geometry.av1_config, .apple_compatible = geometry.apple_compatible });
}

/// IDs are visible decoder-output ordinals, never container packet ordinals.
/// The callback borrows the frame until it returns. Repeated selections publish
/// once; the model reuses that projection in each requested presentation slot.
pub fn walk(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options, selected: ?[]const usize, context: *anyopaque, accept: *const fn (*anyopaque, usize, *const codecs.Frame) anyerror!void) !void {
    return walkFrom(a, reader, options, .{}, selected, context, accept, null, null);
}
/// Restarts only at parsed codec key pictures, retaining logical output IDs.
/// AV1 reloads the most recent sequence OBU before replaying the key packet.
pub fn walkIndexed(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options, indexed: Index, selected: ?[]const usize, context: *anyopaque, accept: *const fn (*anyopaque, usize, *const codecs.Frame) anyerror!void) !void {
    if (indexed.reader != reader) return error.InvalidVideoIndex;
    if (reader.track.codec == .av1 and (indexed.operating_point != options.decoder.av1_operating_point or indexed.all_layers != options.decoder.av1_all_layers)) return error.InvalidDecodeOptions;
    var restart: Restart = .{};
    if (selected) |ids| {
        if (ids.len == 0) return;
        var first = ids[0];
        for (ids) |id| {
            if (id >= indexed.pictures.len) return error.InvalidPacketIndex;
            first = @min(first, id);
        }
        for (indexed.restarts) |point| {
            if (point.logical > first) break;
            restart = point;
        }
    }
    return walkFrom(a, reader, options, restart, selected, context, accept, null, null);
}
/// One request-owned reference decoder across forward logical selections.
/// The immutable reader/index and admission pool outlive this owner. Calls are
/// sequential; callbacks borrow each output. Backward/within-packet selections
/// replay independently without disturbing the forward cursor. Any failure
/// destroys retained entropy/reference state before retry.
pub const Continuation = struct {
    allocator: std.mem.Allocator,
    reader: *media.webm.Reader,
    packet: usize = 0,
    logical: usize = 0,
    vp8_session: ?*vp8.Session = null,
    session: ?*codecs.Session = null,
    configured: ?codecs.Options = null,
    packets_decoded: usize = 0,
    reused_windows: usize = 0,
    pub fn deinit(self: *@This()) void {
        if (self.vp8_session) |session| session.deinit();
        if (self.session) |session| session.destroy();
        self.vp8_session = null;
        self.session = null;
        self.configured = null;
        self.packet = 0;
        self.logical = 0;
    }
    pub fn walk(self: *@This(), indexed: Index, options: Options, selected: []const usize, context: *anyopaque, accept: *const fn (*anyopaque, usize, *const codecs.Frame) anyerror!void) !void {
        if (indexed.reader != self.reader) return error.InvalidVideoIndex;
        if (self.reader.track.codec == .av1 and (indexed.operating_point != options.decoder.av1_operating_point or indexed.all_layers != options.decoder.av1_all_layers)) return error.InvalidDecodeOptions;
        if (selected.len == 0) return;
        var first = selected[0];
        for (selected) |id| {
            if (id >= indexed.pictures.len) return error.InvalidPacketIndex;
            first = @min(first, id);
        }
        var config = options.decoder;
        // Reference reservations survive model invocations. Charge them to the
        // reader's request-lived pool, never a per-window preprocessing pool.
        config.admission_pool = self.reader.input.admission_pool;
        config.control = .{};
        if (self.configured) |previous| if (!std.meta.eql(previous, config)) return error.InvalidDecodeOptions;
        if (first < self.logical) return walkIndexed(self.allocator, self.reader, options, indexed, selected, context, accept);
        var restart = Restart{ .packet = self.packet, .logical = self.logical };
        const warm = self.vp8_session != null or self.session != null;
        if (!warm) {
            for (indexed.restarts) |point| {
                if (point.logical > first) break;
                restart = point;
            }
            self.packet = restart.packet;
            self.logical = restart.logical;
        }
        errdefer self.deinit();
        var stats = ReplayStats{};
        var measured = options;
        measured.decoder.admission_pool = self.reader.input.admission_pool;
        measured.replay_stats = &stats;
        defer {
            self.packets_decoded += stats.packets_decoded;
            if (options.replay_stats) |out| out.* = stats;
        }
        try walkFrom(self.allocator, self.reader, measured, restart, selected, context, accept, null, self);
        self.configured = config;
        self.reused_windows += @intFromBool(warm);
    }
};
pub const RecoveryOptions = struct {
    max_recoveries: usize = 8,
    max_skipped_packets: usize = 4096,
};
pub const RecoveryGap = struct {
    failed_packet: usize,
    first_lost_picture: usize,
    next_packet: usize,
    next_picture: usize,
    reason: anyerror,
};
pub const RecoveryStats = struct { recoveries: usize = 0, skipped_packets: usize = 0 };
fn corruptDecode(err: anyerror) bool {
    return switch (err) {
        error.InvalidVideoData, error.WebpDecodeFailed, error.MalformedVideoPacket, error.MissingVideoReference, error.InvalidVideoReference => true,
        else => false,
    };
}
/// Explicit entropy-corruption recovery using a successfully parsed header index.
/// Decode owners are destroyed before a gap is reported or a new owner starts.
/// Source, callback, cancellation and capacity errors remain terminal. Inference
/// uses strict walkIndexed; callers opting into recovery must handle every gap.
pub fn walkRecoveringIndexed(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options, indexed: Index, recovery: RecoveryOptions, selected: ?[]const usize, context: *anyopaque, accept: *const fn (*anyopaque, usize, *const codecs.Frame) anyerror!void, gap_context: *anyopaque, gap: *const fn (*anyopaque, RecoveryGap) anyerror!void) !RecoveryStats {
    if (indexed.reader != reader) return error.InvalidVideoIndex;
    if (reader.track.codec == .av1 and (indexed.operating_point != options.decoder.av1_operating_point or indexed.all_layers != options.decoder.av1_all_layers)) return error.InvalidDecodeOptions;
    var restart: Restart = .{};
    if (selected) |ids| {
        if (ids.len == 0) return .{};
        var first = ids[0];
        for (ids) |id| {
            if (id >= indexed.pictures.len) return error.InvalidPacketIndex;
            first = @min(first, id);
        }
        for (indexed.restarts) |point| {
            if (point.logical > first) break;
            restart = point;
        }
    }
    var stats = RecoveryStats{};
    while (true) {
        try options.decoder.control.check();
        var failure: ?DecodeFailure = null;
        walkFrom(a, reader, options, restart, selected, context, accept, &failure, null) catch |err| {
            const failed = failure orelse return err;
            if (!corruptDecode(err)) return err;
            if (stats.recoveries >= recovery.max_recoveries) return error.RecoveryLimitExceeded;
            var next: ?Restart = null;
            for (indexed.restarts) |point| {
                if (point.packet > failed.packet) {
                    next = point;
                    break;
                }
            }
            const point = next orelse return error.UnrecoverableVideoData;
            if (point.logical < failed.logical or point.packet > reader.packets.len) return error.InvalidVideoIndex;
            const skipped = point.packet - failed.packet;
            if (skipped > recovery.max_skipped_packets -| stats.skipped_packets) return error.RecoveryLimitExceeded;
            try options.decoder.control.check();
            try gap(gap_context, .{ .failed_packet = failed.packet, .first_lost_picture = failed.logical, .next_packet = point.packet, .next_picture = point.logical, .reason = err });
            stats.recoveries += 1;
            stats.skipped_packets += skipped;
            restart = point;
            continue;
        };
        return stats;
    }
}
const DecodeFailure = struct { packet: usize, logical: usize };
fn walkFrom(a: std.mem.Allocator, reader: *media.webm.Reader, options: Options, restart: Restart, selected: ?[]const usize, context: *anyopaque, accept: *const fn (*anyopaque, usize, *const codecs.Frame) anyerror!void, failure: ?*?DecodeFailure, retained: ?*Continuation) !void {
    if (reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
    if (reader.track.codec == .av1 and options.decoder.av1_framing != .low_overhead) return error.InvalidDecodeOptions;
    const Watch = struct {
        first: media.source.Control,
        second: media.source.Control,
        failed: bool = false,
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.first.check() catch |err| {
                self.failed = true;
                return err;
            };
            self.second.check() catch |err| {
                self.failed = true;
                return err;
            };
        }
    };
    var watch = Watch{ .first = options.decoder.control, .second = reader.input.control };
    const control = media.source.Control{ .context = &watch, .check_fn = Watch.check };
    var decoder_options = options.decoder;
    decoder_options.control = control;
    if (selected) |ids| if (ids.len == 0) return;
    var last: usize = 0;
    if (selected) |ids| for (ids) |id| {
        last = @max(last, id);
    };
    if (restart.packet > reader.packets.len or restart.logical > options.max_pictures) return error.InvalidPacketIndex;
    if (options.replay_stats) |stats| stats.* = .{ .first_packet = restart.packet };
    var logical: usize = restart.logical;
    defer if (retained) |owner| {
        owner.logical = logical;
    };
    if (reader.track.codec == .vp8) {
        const session = if (retained != null and retained.?.vp8_session != null) retained.?.vp8_session.? else try vp8.Session.init(a, reader, .{ .max_packet_bytes = options.decoder.max_packet_bytes, .max_pixels = options.decoder.max_pixels, .max_decode_bytes = options.decoder.max_decode_bytes, .admission_pool = options.decoder.admission_pool });
        if (retained) |owner| owner.vp8_session = session;
        defer if (retained == null) session.deinit();
        defer if (retained) |owner| {
            owner.packet = session.next_index;
        };
        session.next_index = restart.packet;
        while (session.next_index < reader.packets.len) {
            try options.decoder.control.check();
            if (options.replay_stats) |stats| stats.packets_decoded += 1;
            const packet_id = session.next_index;
            if (reader.packets[packet_id].size > options.decoder.max_packet_bytes) return error.ResourceLimitExceeded;
            var lease = try reader.readPacket(packet_id);
            defer lease.deinit();
            watch.failed = false;
            var decoded = (session.decodePacketBytes(lease.bytes, control) catch |err| {
                if (!watch.failed) {
                    if (failure) |out| out.* = .{ .packet = packet_id, .logical = logical };
                }
                return err;
            }) orelse continue;
            defer decoded.deinit();
            try control.check();
            if (logical >= options.max_pictures) return error.ResourceLimitExceeded;
            if (selected == null or std.mem.indexOfScalar(usize, selected.?, logical) != null) {
                const p = decoded.planes;
                var frame = try frames.copy(a, options.decoder.admission_pool, @intCast(p.width), @intCast(p.height), 8, 1, .{ .matrix = 6 }, .{ p.y.ptr, p.u.ptr, p.v.ptr }, .{ p.y_stride, p.uv_stride, p.uv_stride }, options.decoder.max_output_bytes);
                defer frame.deinit();
                frame.pts_ns = decoded.pts_ns;
                frame.duration_ns = decoded.duration_ns;
                try accept(context, logical, &frame);
            }
            logical += 1;
            if (selected != null and logical > last) return;
        }
    } else {
        const cold = retained == null or retained.?.session == null;
        const session = if (!cold) retained.?.session.? else try codecs.Session.create(a, if (reader.track.codec == .vp9) .vp9 else .av1, decoder_options);
        if (retained) |owner| owner.session = session;
        defer if (retained == null) session.destroy();
        session.setControl(control);
        // The composite control borrows this call's stack; never retain it.
        defer if (retained != null) {
            session.setControl(.{});
        };
        const previous_stats = session.output_stats;
        defer if (options.replay_stats) |stats| {
            stats.output_copy_bytes = session.output_stats.copied_bytes - previous_stats.copied_bytes;
            stats.outputs_skipped = session.output_stats.skipped_outputs - previous_stats.skipped_outputs;
        };
        try session.initializeWebm(reader);
        if (if (cold) restart.sequence else null) |span| {
            if (span.packet >= reader.packets.len) return error.InvalidPacketIndex;
            const packet = reader.packets[span.packet];
            if (span.offset > packet.size or span.size > packet.size - span.offset) return error.InvalidVideoData;
            if (span.size > options.decoder.max_packet_bytes) return error.ResourceLimitExceeded;
            const offset = std.math.add(u64, packet.offset, span.offset) catch return error.InvalidVideoData;
            var lease = try reader.input.read(offset, span.size);
            defer lease.deinit();
            if (options.replay_stats) |stats| stats.bootstrap_bytes += span.size;
            var initialization = try session.decodeWebmBytes(reader, lease.bytes, 0, null);
            defer initialization.deinit();
            if (initialization.frames.len != 0) return error.InvalidVideoData;
        }
        for (reader.packets[restart.packet..], restart.packet..) |packet, packet_id| {
            if (options.replay_stats) |stats| stats.packets_decoded += 1;
            try options.decoder.control.check();
            if (packet.size > options.decoder.max_packet_bytes) return error.ResourceLimitExceeded;
            var lease = try reader.readPacket(packet_id);
            defer lease.deinit();
            watch.failed = false;
            var output_ids: [64]usize = undefined;
            var output_id_count: usize = 0;
            if (selected) |ids| for (ids) |id| {
                if (id < logical or id - logical >= decoder_options.max_output_frames) continue;
                const local = id - logical;
                if (std.mem.indexOfScalar(usize, output_ids[0..output_id_count], local) != null) continue;
                output_ids[output_id_count] = local;
                output_id_count += 1;
            };
            const output_selection: codecs.OutputSelection = if (options.copy_unselected_outputs) .all else if (packet.invisible) .none else if (selected != null) .{ .indexes = output_ids[0..output_id_count] } else .all;
            var batch = session.decodeOutputs(lease.bytes, packet.pts, packet.duration_ns, output_selection) catch |err| {
                if (!watch.failed) {
                    if (failure) |out| out.* = .{ .packet = packet_id, .logical = logical };
                }
                return err;
            };
            defer batch.deinit();
            try control.check();
            if (retained) |owner| owner.packet = packet_id + 1;
            // Container-invisible packets still update codec references.
            if (packet.invisible) continue;
            // Skipped native pictures advance logical identity exactly as owned
            // outputs do; superframes/layers can publish several outputs per packet.
            const consumed = if (selected != null and logical <= last) @min(batch.output_count, last - logical +| 1) else batch.output_count;
            if (consumed > options.max_pictures -| logical) return error.ResourceLimitExceeded;
            for (batch.frames) |*frame| {
                const id = try std.math.add(usize, logical, frame.output_index);
                if (selected == null or std.mem.indexOfScalar(usize, selected.?, id) != null) try accept(context, id, frame);
            }
            logical = try std.math.add(usize, logical, batch.output_count);
            if (selected != null and logical > last) return;
        }
    }
    if (selected != null and logical <= last) return error.InvalidPacketIndex;
}

test "WebM logical pictures sample visible VP8 VP9 AV1 and replay selected pictures" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vp8-altref.webm", "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var pictures = try index(a, &reader, .{});
        defer pictures.deinit();
        try std.testing.expect(pictures.pictures.len > 0);
        const selected = try pictures.select();
        defer a.free(selected);
        var collector = Collect{ .allocator = a, .limit = 32 };
        defer collector.pictures.deinit(a);
        try walk(a, &reader, .{}, selected, &collector, Collect.accept);
        var unique: usize = 0;
        for (selected, 0..) |id, i| if (std.mem.indexOfScalar(usize, selected[0..i], id) == null) {
            unique += 1;
        };
        try std.testing.expectEqual(unique, collector.pictures.items.len);
        try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_packets = 0 }));
    }
}

test "WebM picture sampling orders timestamps and falls back when duration is absent" {
    const a = std.testing.allocator;
    var index_value = Index{ .allocator = a, .pictures = try a.dupe(Picture, &.{
        .{ .pts = 1_000_000_000, .duration = null, .width = 48, .height = 48 },
        .{ .pts = 0, .duration = null, .width = 48, .height = 48 },
        .{ .pts = 500_000_000, .duration = null, .width = 48, .height = 48 },
    }) };
    defer index_value.deinit();
    const selected = try index_value.select();
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, &.{1}, selected);
    for (index_value.pictures) |*picture| picture.pts = 0;
    const no_rate = try index_value.select();
    defer a.free(no_rate);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, no_rate);
}

test "WebM replay callback failures release sessions and permit a fresh replay" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vp8-inter.webm", "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        const retained_metadata = source.retained_bytes;
        const Fail = struct {
            fn accept(_: *anyopaque, _: usize, _: *const codecs.Frame) !void {
                return error.InjectedConsumerFailure;
            }
        };
        var collector = Collect{ .allocator = a, .limit = 32 };
        defer collector.pictures.deinit(a);
        try std.testing.expectError(error.InjectedConsumerFailure, walk(a, &reader, .{}, &.{0}, &collector, Fail.accept));
        try walk(a, &reader, .{}, &.{0}, &collector, Collect.accept);
        try std.testing.expectEqual(@as(usize, 1), collector.pictures.items.len);
        try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_pictures = 0 }));
        try std.testing.expectEqual(retained_metadata, source.retained_bytes);
    }
}

test "WebM declared duration preserves two-second FPS sampling at rounded PTS boundaries" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        try std.testing.expectEqual(@as(?f64, 2), reader.duration_seconds);
        var pictures = try index(a, &reader, .{});
        defer pictures.deinit();
        const selected = try pictures.select();
        defer a.free(selected);
        try std.testing.expectEqualSlices(usize, &.{ 0, 6 }, selected);
    }
}

test "WebM header indexes match fully reconstructed logical timelines" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vp8-altref.webm", "../testdata/vp8-inter.webm", "../testdata/vp8-true-motion-border.webm", "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm", "../testdata/vpx-av1/vp9-sintel.webm", "../testdata/vpx-av1/av1-sintel.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var headers = try index(a, &reader, .{});
        defer headers.deinit();
        var decoded = try index(a, &reader, .{ .strategy = .reconstruct });
        defer decoded.deinit();
        for (decoded.pictures, headers.pictures) |pixel, header| {
            var expected = pixel;
            expected.packet_index = header.packet_index;
            expected.profile = header.profile;
            expected.av1_config = header.av1_config;
            expected.apple_compatible = header.apple_compatible;
            try std.testing.expect(header.packet_index.? < reader.packets.len);
            try std.testing.expectEqualDeep(expected, header);
        }
        const expected = try decoded.select();
        defer a.free(expected);
        const actual = try headers.select();
        defer a.free(actual);
        try std.testing.expectEqualSlices(usize, expected, actual);
    }
}

test "WebM header metadata admission outlives indexing and allocation failures reclaim it" {
    const Campaign = struct {
        fn run(a: std.mem.Allocator) !void {
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 16 * 1024 * 1024 } };
            defer std.debug.assert(pool.snapshot().host_bytes == 0);
            var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/vpx-av1/av1.webm") }, .identity = "header-index-alloc" };
            var reader = try media.webm.Reader.init(a, &source, .{});
            defer reader.deinit();
            var headers = try index(a, &reader, .{ .decoder = .{ .admission_pool = &pool } });
            defer headers.deinit();
            try std.testing.expectEqual(@as(u64, headers.pictures.len * @sizeOf(Picture) + headers.restarts.len * @sizeOf(Restart)), pool.snapshot().host_bytes);
        }
    };
    // Refuse backing remaps so every campaign enumerates the same allocations.
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Campaign.run, .{});
}

test "WebM metadata indexing rejects overflowing and denied shared budgets" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/vpx-av1/av1.webm") }, .identity = "header-index-budget" };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_pictures = std.math.maxInt(usize) }));
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, index(a, &reader, .{ .decoder = .{ .admission_pool = &pool } }));
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
}

test "WebM indexed replay ignores sync hints and bootstraps AV1 sequence headers" {
    const a = std.testing.allocator;
    const Capture = struct {
        count: usize = 0,
        digest: [32]u8 = undefined,
        pts: i64 = 0,
        fn accept(raw: *anyopaque, id: usize, frame: *const codecs.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 3), id);
            std.crypto.hash.sha2.Sha256.hash(frame.bytes, &self.digest, .{});
            self.pts = frame.pts_ns;
            self.count += 1;
        }
        fn geometry(_: *anyopaque, _: @import("codecs/vp9_header.zig").Geometry) !void {}
    };
    inline for (.{ "../testdata/vp8-inter.webm", "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        const key = reader.packets[0];
        var sequence_end: usize = 0;
        if (reader.track.codec == .av1) {
            var lease = try reader.readPacket(0);
            defer lease.deinit();
            const headers = try @import("codecs/av1_headers.zig").Session.create(a, .{ .max_pixels = 1024 * 1024 });
            defer headers.deinit();
            var dummy: u8 = 0;
            try headers.walk(lease.bytes, &dummy, Capture.geometry);
            const span = headers.packet_sequence orelse return error.TestUnexpectedResult;
            sequence_end = span.offset + span.size;
            reader.track.codec_private = &.{};
            try std.testing.expect(sequence_end < lease.bytes.len);
        }
        const packets = try a.alloc(media.webm.Packet, 4);
        for (packets, 0..) |*packet, i| {
            packet.* = key;
            packet.sync = false;
            packet.pts = @intCast(i * 1_000_000_000);
            if (i != 0 and sequence_end != 0) {
                packet.offset += sequence_end;
                packet.size -= sequence_end;
            }
        }
        a.free(reader.packets);
        reader.packets = packets;
        var indexed = try index(a, &reader, .{});
        defer indexed.deinit();
        try std.testing.expectEqual(@as(usize, 4), indexed.restarts.len);
        var expected = Capture{};
        var actual = Capture{};
        var baseline_stats = ReplayStats{};
        var seek_stats = ReplayStats{};
        try walk(a, &reader, .{ .replay_stats = &baseline_stats }, &.{ 3, 3 }, &expected, Capture.accept);
        try walkIndexed(a, &reader, .{ .replay_stats = &seek_stats }, indexed, &.{ 3, 3 }, &actual, Capture.accept);
        try std.testing.expectEqual(@as(usize, 1), actual.count);
        try std.testing.expectEqual(expected.count, actual.count);
        try std.testing.expectEqualSlices(u8, &expected.digest, &actual.digest);
        try std.testing.expectEqual(expected.pts, actual.pts);
        try std.testing.expectEqual(@as(usize, 4), baseline_stats.packets_decoded);
        try std.testing.expectEqual(@as(usize, 1), seek_stats.packets_decoded);
        try std.testing.expectEqual(@as(usize, 3), seek_stats.first_packet);
        if (reader.track.codec == .av1) {
            try std.testing.expect(seek_stats.bootstrap_bytes > 0);
            try std.testing.expectError(error.InvalidDecodeOptions, walkIndexed(a, &reader, .{ .decoder = .{ .av1_all_layers = false } }, indexed, &.{3}, &actual, Capture.accept));
        }
        try std.testing.expectError(error.InvalidPacketIndex, walkIndexed(a, &reader, .{}, indexed, &.{4}, &actual, Capture.accept));
    }
}

test "WebM indexed replay allocation failures reclaim index bootstrap and decoder owners" {
    const Campaign = struct {
        fn accept(_: *anyopaque, _: usize, _: *const codecs.Frame) !void {}
        fn geometry(_: *anyopaque, _: @import("codecs/vp9_header.zig").Geometry) !void {}
        fn run(a: std.mem.Allocator) !void {
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 64 * 1024 * 1024 } };
            defer std.debug.assert(pool.snapshot().host_bytes == 0);
            var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/vpx-av1/av1.webm") }, .identity = "restart-allocation", .admission_pool = &pool };
            var reader = try media.webm.Reader.init(a, &source, .{});
            defer reader.deinit();
            const key = reader.packets[0];
            const sequence_end = blk: {
                var lease = try reader.readPacket(0);
                defer lease.deinit();
                const headers = try @import("codecs/av1_headers.zig").Session.create(a, .{ .max_pixels = 1024 * 1024 });
                defer headers.deinit();
                var unused: u8 = 0;
                try headers.walk(lease.bytes, &unused, geometry);
                const span = headers.packet_sequence orelse return error.TestUnexpectedResult;
                break :blk span.offset + span.size;
            };
            reader.track.codec_private = &.{};
            const packets = try a.alloc(media.webm.Packet, 4);
            for (packets, 0..) |*packet, i| {
                packet.* = key;
                packet.pts = @intCast(i * 1_000_000_000);
                if (i != 0) {
                    packet.offset += sequence_end;
                    packet.size -= sequence_end;
                }
            }
            a.free(reader.packets);
            reader.packets = packets;
            const options = Options{ .decoder = .{ .admission_pool = &pool, .max_decode_bytes = 16 * 1024 * 1024 } };
            var indexed = try index(a, &reader, options);
            defer indexed.deinit();
            var dummy: u8 = 0;
            try walkIndexed(a, &reader, options, indexed, &.{3}, &dummy, accept);
        }
    };
    // Refuse backing remaps so every campaign enumerates the same allocations.
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Campaign.run, .{});
}

test "WebM indexed replay preserves expanded VP9 AV1 depth chroma grain and layers" {
    const a = std.testing.allocator;
    const Capture = struct {
        target: usize,
        digest: [32]u8 = undefined,
        count: usize = 0,
        fn accept(raw: *anyopaque, id: usize, frame: *const codecs.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.target, id);
            std.crypto.hash.sha2.Sha256.hash(frame.bytes, &self.digest, .{});
            self.count += 1;
        }
    };
    inline for (.{ "vpx-av1/vp9.ivf", "vpx-av1/vp9-yuv420p10le.ivf", "vpx-av1/vp9-yuv420p12le.ivf", "vpx-av1/vp9-yuv422p.ivf", "vpx-av1/vp9-yuv422p12le.ivf", "vpx-av1/vp9-yuv444p.ivf", "vpx-av1/vp9-yuv444p10le.ivf", "vpx-av1/av1.ivf", "vpx-av1/av1-grain.ivf", "vpx-av1/av1-odd.ivf", "av1-extended/av1-c0-12.ivf", "av1-extended/av1-c1-12.ivf", "av1-extended/av1-c2-8.ivf", "av1-extended/av1-c2-10.ivf", "av1-extended/av1-c2-12.ivf", "av1-extended/av1-c3-8.ivf", "av1-extended/av1-c3-10.ivf", "av1-extended/av1-c3-12.ivf", "av1-extended/av1-svc.ivf" }) |path| {
        const bytes = @embedFile("../testdata/" ++ path);
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/vpx-av1/av1.webm") }, .identity = path };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        const packets = try a.alloc(media.webm.Packet, 4);
        const original = reader.packets[0];
        for (packets, 0..) |*packet, i| {
            packet.* = original;
            packet.offset = 44;
            packet.size = std.mem.readInt(u32, bytes[32..36], .little);
            packet.pts = @intCast(i * 1_000_000_000);
            packet.invisible = false;
        }
        a.free(reader.packets);
        reader.packets = packets;
        source.storage = .{ .borrowed = bytes };
        reader.track.codec = if (std.mem.startsWith(u8, path, "vpx-av1/vp9")) .vp9 else .av1;
        reader.track.width = std.mem.readInt(u16, bytes[12..14], .little);
        reader.track.height = std.mem.readInt(u16, bytes[14..16], .little);
        reader.track.codec_private = &.{};
        var indexed = try index(a, &reader, .{});
        defer indexed.deinit();
        try std.testing.expectEqual(@as(usize, 4), indexed.restarts.len);
        const selected = [_]usize{indexed.pictures.len - 1};
        var baseline = Capture{ .target = selected[0] };
        var replay = Capture{ .target = selected[0] };
        var stats = ReplayStats{};
        try walk(a, &reader, .{}, &selected, &baseline, Capture.accept);
        try walkIndexed(a, &reader, .{ .replay_stats = &stats }, indexed, &selected, &replay, Capture.accept);
        try std.testing.expectEqual(@as(usize, 1), replay.count);
        try std.testing.expectEqual(baseline.count, replay.count);
        try std.testing.expectEqualSlices(u8, &baseline.digest, &replay.digest);
        try std.testing.expectEqual(@as(usize, 1), stats.packets_decoded);
    }
}

const RecoveryCapture = struct {
    hashes: [64][32]u8 = undefined,
    seen: [64]bool = @splat(false),
    gaps: usize = 0,
    last_gap: ?RecoveryGap = null,
    callback_error: bool = false,
    gap_error: bool = false,
    hold_first: bool = false,
    pool: ?*media.admission.Pool = null,
    retained: ?codecs.Frame = null,
    fn accept(raw: *anyopaque, id: usize, frame: *const codecs.Frame) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.callback_error) return error.InvalidVideoData;
        if (id >= self.seen.len) return error.ResourceLimitExceeded;
        std.crypto.hash.sha2.Sha256.hash(frame.bytes, &self.hashes[id], .{});
        self.seen[id] = true;
        if (id == 0 and self.hold_first and self.retained == null) {
            var reservation = if (self.pool) |pool| try pool.acquire(.{ .host_bytes = frame.bytes.len }) else media.admission.Token{};
            errdefer reservation.deinit();
            var owned = frame.*;
            owned.allocator = std.testing.allocator;
            owned.bytes = try std.testing.allocator.dupe(u8, frame.bytes);
            owned.reservation = reservation;
            self.retained = owned;
        }
    }
    fn gap(raw: *anyopaque, value: RecoveryGap) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.gap_error) return error.InvalidVideoData;
        if (self.retained) |frame| {
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(frame.bytes, &hash, .{});
            try std.testing.expectEqualSlices(u8, &self.hashes[0], &hash);
        }
        self.gaps += 1;
        self.last_gap = value;
    }
};
test "WebM recovery restarts VP8 VP9 AV1 after detected entropy corruption" {
    const a = std.testing.allocator;
    inline for (.{ "../testdata/vp8-keyframes.webm", "../testdata/vp8-sintel.webm", "../testdata/vpx-av1/vp9.webm", "../testdata/vpx-av1/av1.webm" }) |path| {
        const fixture = @embedFile(path);
        const damaged = try a.dupe(u8, fixture);
        defer a.free(damaged);
        var source = media.source.Source{ .allocator = a, .identity = path, .storage = .{ .borrowed = damaged } };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var baseline = RecoveryCapture{};
        try walk(a, &reader, .{}, null, &baseline, RecoveryCapture.accept);
        const packet = reader.packets[1];
        var found = false;
        for (0..64) |attempt| {
            @memcpy(damaged, fixture);
            const offset: usize = @intCast(packet.offset);
            const first = @min(packet.size - 1, @max(@as(usize, 12), packet.size / 2));
            if (attempt == 0) {
                @memset(damaged[offset + first .. offset + packet.size], 0xff);
            } else {
                const at = first + (attempt - 1) % (packet.size - first);
                damaged[offset + at] ^= @as(u8, 1) << @intCast((attempt - 1) % 8);
            }
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
            const options = Options{ .decoder = .{ .max_decode_bytes = 64 * 1024 * 1024, .admission_pool = &pool } };
            // Container key hints must not authorize an interframe restart.
            for (reader.packets) |*item| item.sync = true;
            var indexed = index(a, &reader, options) catch continue;
            defer indexed.deinit();
            var strict = RecoveryCapture{};
            var decode_failed = false;
            walkIndexed(a, &reader, options, indexed, null, &strict, RecoveryCapture.accept) catch |err| {
                if (corruptDecode(err)) decode_failed = true else return err;
            };
            if (!decode_failed) continue;
            var recovered = RecoveryCapture{ .hold_first = true, .pool = &pool };
            defer if (recovered.retained) |*frame| frame.deinit();
            const stats = try walkRecoveringIndexed(a, &reader, options, indexed, .{}, null, &recovered, RecoveryCapture.accept, &recovered, RecoveryCapture.gap);
            try std.testing.expectEqual(@as(usize, 1), stats.recoveries);
            try std.testing.expectEqual(@as(usize, 1), recovered.gaps);
            try std.testing.expect(recovered.retained != null);
            var rejected = RecoveryCapture{ .gap_error = true };
            try std.testing.expectError(error.InvalidVideoData, walkRecoveringIndexed(a, &reader, options, indexed, .{}, null, &rejected, RecoveryCapture.accept, &rejected, RecoveryCapture.gap));
            try std.testing.expectEqual(@as(usize, 0), rejected.gaps);
            const gap = recovered.last_gap.?;
            try std.testing.expectEqual(@as(usize, 1), gap.failed_packet);
            try std.testing.expect(gap.next_packet > gap.failed_packet);
            for (baseline.seen, 0..) |seen, id| {
                const lost = id >= gap.first_lost_picture and id < gap.next_picture;
                try std.testing.expectEqual(seen and !lost, recovered.seen[id]);
                if (recovered.seen[id]) try std.testing.expectEqualSlices(u8, &baseline.hashes[id], &recovered.hashes[id]);
            }
            try std.testing.expectError(error.RecoveryLimitExceeded, walkRecoveringIndexed(a, &reader, options, indexed, .{ .max_recoveries = 0 }, null, &strict, RecoveryCapture.accept, &strict, RecoveryCapture.gap));
            try std.testing.expectError(error.RecoveryLimitExceeded, walkRecoveringIndexed(a, &reader, options, indexed, .{ .max_skipped_packets = 0 }, null, &strict, RecoveryCapture.accept, &strict, RecoveryCapture.gap));
            found = true;
            break;
        }
        try std.testing.expect(found);
    }
}
test "WebM recovery preserves callback control and admission failures" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .identity = "recovery-errors", .storage = .{ .borrowed = @embedFile("../testdata/vpx-av1/vp9.webm") } };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var indexed = try index(a, &reader, .{});
    defer indexed.deinit();
    var capture = RecoveryCapture{ .callback_error = true };
    try std.testing.expectError(error.InvalidVideoData, walkRecoveringIndexed(a, &reader, .{}, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap));
    try std.testing.expectEqual(@as(usize, 0), capture.gaps);
    capture.callback_error = false;
    const ControlError = struct {
        calls: usize = 0,
        reason: anyerror = error.InvalidVideoData,
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            if (self.calls >= 6) return self.reason;
        }
    };
    var control = ControlError{};
    try std.testing.expectError(error.InvalidVideoData, walkRecoveringIndexed(a, &reader, .{ .decoder = .{ .control = .{ .context = &control, .check_fn = ControlError.check } } }, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap));
    try std.testing.expectEqual(@as(usize, 0), capture.gaps);
    control.calls = 0;
    control.reason = error.Cancelled;
    try std.testing.expectError(error.Cancelled, walkRecoveringIndexed(a, &reader, .{ .decoder = .{ .control = .{ .context = &control, .check_fn = ControlError.check } } }, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap));
    try std.testing.expectEqual(@as(usize, 0), capture.gaps);
    const FailedRead = struct {
        fn read(_: *anyopaque, _: u64, _: []u8, _: media.source.Control) !usize {
            return error.InvalidVideoData;
        }
    };
    var unused: u8 = 0;
    const source_length = source.length();
    source.storage = .{ .range = .{ .context = &unused, .read_at = FailedRead.read, .length = source_length } };
    try std.testing.expectError(error.InvalidVideoData, walkRecoveringIndexed(a, &reader, .{}, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap));
    try std.testing.expectEqual(@as(usize, 0), capture.gaps);
    const reads_before = source.reads;
    try std.testing.expectError(error.ResourceLimitExceeded, walkRecoveringIndexed(a, &reader, .{ .decoder = .{ .max_packet_bytes = 0 } }, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap));
    try std.testing.expectEqual(reads_before, source.reads);
    var denied = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, walkRecoveringIndexed(a, &reader, .{ .decoder = .{ .admission_pool = &denied } }, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap));
    try std.testing.expectEqual(media.admission.Resources{}, denied.snapshot());
    try std.testing.expectEqual(@as(usize, 0), capture.gaps);
}

test "WebM recovery skips a corrupt restart and rejects unrecoverable tail" {
    const a = std.testing.allocator;
    const fixture = @embedFile("../testdata/vp8-keyframes.webm");
    const damaged = try a.dupe(u8, fixture);
    defer a.free(damaged);
    var source = media.source.Source{ .allocator = a, .identity = "repeated-corruption", .storage = .{ .borrowed = damaged } };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var baseline = RecoveryCapture{};
    try walk(a, &reader, .{}, null, &baseline, RecoveryCapture.accept);
    for (reader.packets[0..2]) |packet| {
        const offset: usize = @intCast(packet.offset);
        const first = @max(@as(usize, 12), packet.size / 2);
        @memset(damaged[offset + first .. offset + packet.size], 0xff);
    }
    var indexed = try index(a, &reader, .{});
    defer indexed.deinit();
    var capture = RecoveryCapture{};
    const stats = try walkRecoveringIndexed(a, &reader, .{}, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap);
    try std.testing.expectEqual(@as(usize, 2), stats.recoveries);
    try std.testing.expectEqual(@as(usize, 2), capture.gaps);
    try std.testing.expect(!capture.seen[0] and !capture.seen[1] and capture.seen[2]);
    try std.testing.expectEqualSlices(u8, &baseline.hashes[2], &capture.hashes[2]);
    const last = reader.packets[2];
    const offset: usize = @intCast(last.offset);
    @memset(damaged[offset + @max(@as(usize, 12), last.size / 2) .. offset + last.size], 0xff);
    try std.testing.expectError(error.UnrecoverableVideoData, walkRecoveringIndexed(a, &reader, .{}, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap));
}

fn recoveryAllocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    const fixture = @embedFile("../testdata/vp8-keyframes.webm");
    const damaged = try a.dupe(u8, fixture);
    defer a.free(damaged);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var source = media.source.Source{ .allocator = a, .identity = "recovery-allocations", .storage = .{ .borrowed = damaged }, .admission_pool = &pool };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    const packet = reader.packets[1];
    const offset: usize = @intCast(packet.offset);
    @memset(damaged[offset + @max(@as(usize, 12), packet.size / 2) .. offset + packet.size], 0xff);
    const options = Options{ .decoder = .{ .max_decode_bytes = 64 * 1024 * 1024, .admission_pool = &pool } };
    var indexed = try index(a, &reader, options);
    defer indexed.deinit();
    var capture = RecoveryCapture{};
    const stats = try walkRecoveringIndexed(a, &reader, options, indexed, .{}, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap);
    try std.testing.expectEqual(@as(usize, 1), stats.recoveries);
}
test "WebM recovery allocation failures release old new and partial owners" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, recoveryAllocationRun, .{});
}

const RecoveryFuzz = struct {
    fn seed(comptime program: []const u8) []const u8 {
        return comptime blk: {
            var data: [4 + program.len]u8 = undefined;
            std.mem.writeInt(u32, data[0..4], program.len, .little);
            @memcpy(data[4..], program);
            const frozen = data;
            break :blk &frozen;
        };
    }
    const seeds = .{ seed(&.{ 0, 0 }), seed(&.{ 0, 1 }), seed(&.{ 1, 1 }), seed(&.{ 2, 2, 0, 1 }) };
    fn run(program: []const u8) !RecoveryStats {
        const a = std.testing.allocator;
        const fixture = switch (if (program.len == 0) @as(u8, 0) else program[0] % 3) {
            0 => @embedFile("../testdata/vp8-keyframes.webm"),
            1 => @embedFile("../testdata/vpx-av1/vp9.webm"),
            else => @embedFile("../testdata/vpx-av1/av1.webm"),
        };
        const data = try a.dupe(u8, fixture);
        defer a.free(data);
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
        var source = media.source.Source{ .allocator = a, .identity = "recovery-fuzz", .storage = .{ .borrowed = data }, .admission_pool = &pool };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        if (program.len > 1 and program[1] != 0) {
            const packet = reader.packets[1];
            const offset: usize = @intCast(packet.offset);
            const first = @min(packet.size - 1, @max(@as(usize, 12), packet.size / 2));
            if (program[1] == 1) {
                @memset(data[offset + first .. offset + packet.size], 0xff);
                for (program[2..], 0..) |value, i| {
                    data[offset + first + i % (packet.size - first)] ^= value;
                }
            } else {
                const at = first + (if (program.len > 2) @as(usize, program[2]) else 0) % (packet.size - first);
                data[offset + at] ^= if (program.len > 3) program[3] else 1;
            }
        }
        const options = Options{ .max_packets = 64, .max_pictures = 64, .decoder = .{ .max_decode_bytes = 64 * 1024 * 1024, .admission_pool = &pool } };
        var indexed = try index(a, &reader, options);
        defer indexed.deinit();
        var capture = RecoveryCapture{};
        return walkRecoveringIndexed(a, &reader, options, indexed, .{ .max_recoveries = 3, .max_skipped_packets = 64 }, null, &capture, RecoveryCapture.accept, &capture, RecoveryCapture.gap);
    }
};
test "WebM recovery fuzz seeds reach clean and recovered output through Smith" {
    inline for (RecoveryFuzz.seeds, 0..) |seed, i| {
        var smith = std.testing.Smith{ .in = seed };
        var program: [20]u8 = undefined;
        const length = smith.sliceWithHash(&program, 0x57524301);
        const stats = try RecoveryFuzz.run(program[0..length]);
        try std.testing.expectEqual(@as(usize, if (i == 0) 0 else 1), stats.recoveries);
    }
}
test "WebM recovery fuzz bounded entropy mutations restart and admission cleanup" {
    const Harness = struct {
        fn fuzz(_: void, smith: *std.testing.Smith) !void {
            var program: [20]u8 = undefined;
            const length = smith.sliceWithHash(&program, 0x57524301);
            _ = RecoveryFuzz.run(program[0..length]) catch return;
        }
    };
    try std.testing.fuzz({}, Harness.fuzz, .{ .corpus = &RecoveryFuzz.seeds });
}

test "WebM recovery VP8 transform overflow crash regression" {
    var smith = std.testing.Smith{ .in = @embedFile("../testdata/recovery-validation/vp8-transform-crash.smith") };
    var program: [20]u8 = undefined;
    const length = smith.sliceWithHash(&program, 0x57524301);
    _ = RecoveryFuzz.run(program[0..length]) catch |err| switch (err) {
        error.InvalidVideoData, error.UnrecoverableVideoData, error.RecoveryLimitExceeded => {},
        else => return err,
    };
}

test "WebM recovery VP9 transform overflow crash regression" {
    var smith = std.testing.Smith{ .in = @embedFile("../testdata/recovery-validation/vp9-transform-crash.smith") };
    var program: [20]u8 = undefined;
    const length = smith.sliceWithHash(&program, 0x57524301);
    _ = RecoveryFuzz.run(program[0..length]) catch |err| switch (err) {
        error.InvalidVideoData, error.UnrecoverableVideoData, error.RecoveryLimitExceeded => {},
        else => return err,
    };
}

test "WebM filtered replay preserves invisible preroll and superframe output identities" {
    const a = std.testing.allocator;
    const Capture = struct {
        hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
        count: usize = 0,
        bytes: u64 = 0,
        fn accept(raw: *anyopaque, id: usize, frame: *const codecs.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.hash.update(std.mem.asBytes(&id));
            self.hash.update(frame.bytes);
            self.count += 1;
            self.bytes += frame.bytes.len;
        }
        fn digest(self: *@This()) [32]u8 {
            var out: [32]u8 = undefined;
            self.hash.final(&out);
            return out;
        }
    };
    inline for (.{ "vp9", "av1" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/vpx-av1/" ++ name ++ ".webm") } };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        reader.packets[0].invisible = true;
        var expected = Capture{};
        var actual = Capture{};
        var full_stats = ReplayStats{};
        var filtered_stats = ReplayStats{};
        try walk(a, &reader, .{ .copy_unselected_outputs = true, .replay_stats = &full_stats }, &.{ 1, 1 }, &expected, Capture.accept);
        try walk(a, &reader, .{ .replay_stats = &filtered_stats }, &.{ 1, 1 }, &actual, Capture.accept);
        try std.testing.expectEqual(@as(usize, 1), actual.count);
        try std.testing.expectEqualSlices(u8, &expected.digest(), &actual.digest());
        try std.testing.expectEqual(full_stats.packets_decoded, filtered_stats.packets_decoded);
        try std.testing.expectEqual(actual.bytes, filtered_stats.output_copy_bytes);
        try std.testing.expect(filtered_stats.output_copy_bytes < full_stats.output_copy_bytes);
        try std.testing.expect(filtered_stats.outputs_skipped >= 2);
    }
    const original = @embedFile("../testdata/vpx-av1/vp9.webm");
    var source = media.source.Source{ .allocator = a, .identity = "superframe", .storage = .{ .borrowed = original } };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var first = try reader.readPacket(0);
    defer first.deinit();
    var second = try reader.readPacket(1);
    defer second.deinit();
    const bytes = try a.alloc(u8, original.len + first.bytes.len + second.bytes.len + 6);
    defer a.free(bytes);
    @memcpy(bytes[0..original.len], original);
    const packet = bytes[original.len..];
    const n = first.bytes.len;
    const m = second.bytes.len;
    @memcpy(packet[0..n], first.bytes);
    @memcpy(packet[n..][0..m], second.bytes);
    packet[n + m] = 0xc9;
    std.mem.writeInt(u16, packet[n + m + 1 ..][0..2], @intCast(n), .little);
    std.mem.writeInt(u16, packet[n + m + 3 ..][0..2], @intCast(m), .little);
    packet[n + m + 5] = 0xc9;
    source.storage = .{ .borrowed = bytes };
    reader.packets[0].offset = original.len;
    reader.packets[0].size = @intCast(packet.len);
    var expected = Capture{};
    var actual = Capture{};
    var full_stats = ReplayStats{};
    var filtered_stats = ReplayStats{};
    try walk(a, &reader, .{ .copy_unselected_outputs = true, .replay_stats = &full_stats }, &.{ 1, 1 }, &expected, Capture.accept);
    try walk(a, &reader, .{ .replay_stats = &filtered_stats }, &.{ 1, 1 }, &actual, Capture.accept);
    try std.testing.expectEqual(@as(usize, 1), actual.count);
    try std.testing.expectEqualSlices(u8, &expected.digest(), &actual.digest());
    try std.testing.expectEqual(@as(usize, 1), filtered_stats.packets_decoded);
    try std.testing.expectEqual(actual.bytes, filtered_stats.output_copy_bytes);
    try std.testing.expectEqual(2 * actual.bytes, full_stats.output_copy_bytes);
    try std.testing.expectEqual(@as(usize, 1), filtered_stats.outputs_skipped);
}

test "WebM continuation retains VP8 VP9 AV1 references and preserves replay outputs" {
    const a = std.testing.allocator;
    const Capture = struct {
        digest: [32]u8 = undefined,
        pts: i64 = 0,
        duration: ?u64 = null,
        count: usize = 0,
        fail: bool = false,
        fn accept(raw: *anyopaque, _: usize, frame: *const codecs.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.Cancelled;
            std.crypto.hash.sha2.Sha256.hash(frame.bytes, &self.digest, .{});
            self.pts = frame.pts_ns;
            self.duration = frame.duration_ns;
            self.count += 1;
        }
    };
    inline for (.{ "../testdata/vp8-inter.webm", "../testdata/vp8-altref.webm", "../testdata/vpx-av1/vp9-sintel.webm", "../testdata/vpx-av1/av1-sintel.webm" }) |path| {
        var request_pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile(path) }, .identity = path, .admission_pool = &request_pool };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var indexed = try index(a, &reader, .{});
        defer indexed.deinit();
        const indexed_usage = request_pool.snapshot();
        var retained = Continuation{ .allocator = a, .reader = &reader };
        defer retained.deinit();
        var replay_packets: usize = 0;
        const count = @min(indexed.pictures.len, 8);
        try std.testing.expect(count > 2);
        for (0..count) |id| {
            var expected = Capture{};
            var actual = Capture{};
            var stats = ReplayStats{};
            try walkIndexed(a, &reader, .{ .replay_stats = &stats }, indexed, &.{id}, &expected, Capture.accept);
            replay_packets += stats.packets_decoded;
            var window_pool = media.admission.Pool{ .limits = .{ .host_bytes = 0 } };
            try retained.walk(indexed, .{ .decoder = .{ .admission_pool = &window_pool } }, &.{id}, &actual, Capture.accept);
            if (retained.session) |session| {
                try std.testing.expect(std.meta.eql(media.source.Control{}, session.options.control));
                try std.testing.expect(std.meta.eql(media.source.Control{}, session.owner.control));
            }
            try std.testing.expectEqual(media.admission.Resources{}, window_pool.snapshot());
            try std.testing.expectEqual(expected.count, actual.count);
            try std.testing.expectEqualSlices(u8, &expected.digest, &actual.digest);
            try std.testing.expectEqual(expected.pts, actual.pts);
            try std.testing.expectEqual(expected.duration, actual.duration);
        }
        try std.testing.expect(retained.reused_windows > 0);
        try std.testing.expect(retained.packets_decoded < replay_packets);
        // A backward request replays separately and leaves forward state intact.
        const next_packet = retained.packet;
        var backwards = Capture{};
        try retained.walk(indexed, .{}, &.{0}, &backwards, Capture.accept);
        try std.testing.expectEqual(@as(usize, 1), backwards.count);
        try std.testing.expectEqual(next_packet, retained.packet);
        // A consumer failure must not leave a partially advanced owner reusable.
        if (count < indexed.pictures.len) {
            var failed = Capture{ .fail = true };
            try std.testing.expectError(error.Cancelled, retained.walk(indexed, .{}, &.{count}, &failed, Capture.accept));
            try std.testing.expect(retained.session == null and retained.vp8_session == null);
            var retried = Capture{};
            try retained.walk(indexed, .{}, &.{count}, &retried, Capture.accept);
            try std.testing.expectEqual(@as(usize, 1), retried.count);
        }
        retained.deinit();
        try std.testing.expectEqual(indexed_usage, request_pool.snapshot());
    }
}
