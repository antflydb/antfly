// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Ordered, bounded video frames using the pinned EmbeddingGemma 2 processor.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const projector = @import("../architectures/gemma4_projector.zig");
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const build_options = @import("build_options");
const MetalCompute = @import("../ops/metal_compute.zig").MetalCompute;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Encoded = struct {
    embeddings: []f32,
    frame_tokens: []usize,
    pub fn deinit(self: Encoded, a: std.mem.Allocator) void {
        a.free(self.embeddings);
        a.free(self.frame_tokens);
    }
};

/// Sample logical presentation frames, then map them to decode packet indexes.
/// The FPS policy matches HF; timestamp selection remains a separate video API.
pub fn select(a: std.mem.Allocator, reader: *const media.mp4.Reader) ![]usize {
    if (reader.packets.len == 0 or reader.track.timescale == 0) return error.EmptyVideo;
    const order = try a.alloc(usize, reader.packets.len);
    defer a.free(order);
    for (order, 0..) |*index, i| index.* = i;
    const Less = struct {
        fn less(packets: []const media.mp4.Packet, l: usize, r: usize) bool {
            return packets[l].pts < packets[r].pts or (packets[l].pts == packets[r].pts and l < r);
        }
    };
    std.mem.sort(usize, order, @as([]const media.mp4.Packet, reader.packets), Less.less);
    const first = reader.packets[order[0]].pts;
    const last = reader.packets[order[order.len - 1]];
    const ticks = @as(i128, last.pts) + last.duration - first;
    if (ticks <= 0) return error.InvalidSamplingMetadata;
    const seconds = @as(f64, @floatFromInt(ticks)) / @as(f64, @floatFromInt(reader.track.timescale));
    const sampled = try video.sampling.embeddingGemma2(a, .{ .total_frames = order.len, .fps = @as(f64, @floatFromInt(order.len)) / seconds, .duration_seconds = seconds }, .{});
    for (sampled) |*index| index.* = order[index.*];
    return sampled;
}

fn policy(track: media.mp4.Track, width: u32, height: u32) !video.preparation.Options {
    return policySignal(track, width, height, .{});
}
fn policySignal(track: media.mp4.Track, width: u32, height: u32, signal: video.h264.Color) !video.preparation.Options {
    const identity = [9]i32{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 };
    if (!std.mem.eql(i32, &track.display_matrix, &identity) or track.pixel_aspect.horizontal != track.pixel_aspect.vertical) return error.UnsupportedVideoDisplay;
    if (signal.transfer) |transfer| if (transfer != 1 and transfer != 2 and transfer != 6 and transfer != 13) return error.UnsupportedVideoColor;
    var matrix: video.preparation.Matrix = if (signal.matrix) |value| switch (value) {
        1 => .bt709,
        5, 6 => .bt601,
        else => return error.UnsupportedVideoColor,
    } else .bt601;
    if (track.color_info.len != 0) {
        const c = track.color_info;
        if (c.len < 10 or (!std.mem.eql(u8, c[0..4], "nclx") and !std.mem.eql(u8, c[0..4], "nclc"))) return error.UnsupportedVideoColor;
        if (std.mem.eql(u8, c[0..4], "nclx") and c.len < 11) return error.UnsupportedVideoColor;
        const transfer = std.mem.readInt(u16, c[6..8], .big);
        if (transfer != 1 and transfer != 2 and transfer != 6 and transfer != 13) return error.UnsupportedVideoColor;
        const declared = std.mem.readInt(u16, c[8..10], .big);
        if (signal.matrix) |value| if (declared != 2 and value != declared) return error.UnsupportedVideoColor;
        matrix = switch (declared) {
            2 => matrix,
            1 => .bt709,
            5, 6 => .bt601,
            else => return error.UnsupportedVideoColor,
        };
    }
    const g = projector.embeddingGemma2VideoGeometry(width, height);
    return .{ .width = g.width, .height = g.height, .matrix = matrix, .torchvision = true };
}

// Model sampling assumes one progressive access-unit picture per MP4 sample.
// Field/partition transport packets need a logical-picture index before FPS parity.
fn validateFrameConfig(cfg: video.h264.Config) !void {
    if (!cfg.frame_only or cfg.profile == 88) return error.UnsupportedVideoProfile;
}
fn parameterSession(a: std.mem.Allocator, reader: *media.mp4.Reader) !?*video.h264_dynamic.Session {
    if (reader.track.codec != .avc) return null;
    if (reader.track.inband_parameter_sets) {
        const session = try video.h264_dynamic.Session.init(a, reader, .{});
        errdefer session.deinit();
        for (session.entries.items) |entry| {
            try validateFrameConfig(entry.config);
            _ = try policySignal(reader.track, @intCast(entry.config.width), @intCast(entry.config.height), entry.config.color);
        }
        return session;
    }
    const cfg = try video.h264.configParse(a, reader.track.avcc);
    defer cfg.groups.deinit(a);
    try validateFrameConfig(cfg);
    _ = try policySignal(reader.track, @intCast(cfg.width), @intCast(cfg.height), cfg.color);
    return null;
}

/// Conservative decoded-pixel charge used before admitted HTTP model execution.
pub fn inspectPixels(bytes: []const u8, control: Control) !u64 {
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = std.heap.page_allocator, .storage = .{ .borrowed = bytes }, .identity = "video-preflight", .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    var reader = try media.mp4.Reader.init(std.heap.page_allocator, &source, .{});
    defer reader.deinit();
    _ = try policy(reader.track, reader.track.width, reader.track.height);
    const selected = try select(std.heap.page_allocator, &reader);
    defer std.heap.page_allocator.free(selected);
    const configurations = try parameterSession(std.heap.page_allocator, &reader);
    defer if (configurations) |session| session.deinit();
    const static_config = if (reader.track.codec == .avc and configurations == null) try video.h264.configParse(std.heap.page_allocator, reader.track.avcc) else null;
    defer if (static_config) |cfg| cfg.groups.deinit(std.heap.page_allocator);
    var pixels: u64 = 0;
    for (selected) |index| {
        try control.check();
        const cfg = if (configurations) |session| try session.configAt(index) else static_config;
        const width = if (cfg) |value| value.width else reader.track.width;
        const height = if (cfg) |value| value.height else reader.track.height;
        pixels = try std.math.add(u64, pixels, try std.math.mul(u64, width, height));
    }
    return pixels;
}

pub fn encode(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control) !Encoded {
    if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "embedding-video", .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const selected = try select(a, &reader);
    defer a.free(selected);
    const configurations = try parameterSession(a, &reader);
    defer if (configurations) |session| session.deinit();
    const frame_tokens = try a.alloc(usize, selected.len);
    errdefer a.free(frame_tokens);
    @memset(frame_tokens, 0);
    const output = try a.alloc(f32, selected.len * 140 * 512);
    defer a.free(output);
    var signal = video.h264.Color{};
    if (reader.track.codec == .avc and !reader.track.inband_parameter_sets) {
        const cfg = try video.h264.configParse(a, reader.track.avcc);
        defer cfg.groups.deinit(a);
        signal = cfg.color;
    }
    var preparer: ?video.preparation.Metal = null;
    if (comptime build_options.enable_metal) {
        if (cb.kind() == .metal) preparer = try video.preparation.Metal.init(try MetalCompute.videoDevice(cb));
    }
    defer if (preparer) |*value| value.deinit();
    var capture = Capture{ .signal = signal, .selected = selected, .io = control.io orelse cb.getIo(), .metal = if (preparer) |*value| value else null, .a = a, .cb = cb, .track = reader.track, .control = source.control, .output = output, .frame_tokens = frame_tokens };
    if (reader.track.codec == .mjpeg) {
        for (selected, 0..) |index, slot| {
            try source.control.check();
            if (capture.copyDuplicate(slot)) continue;
            var frame = try video.mjpeg.decodeFrame(a, &reader, index, .{});
            defer frame.deinit();
            const options = try policy(reader.track, frame.width, frame.height);
            if (capture.metal) |metal| {
                const prepared = try metal.submitRgba(a, frame.rgba, frame.width, frame.height, options, source.control);
                try capture.projectGpu(slot, prepared, options);
                continue;
            }
            const patches = try video.preparation.referenceRgba(a, frame.rgba, frame.width, frame.height, options, source.control);
            defer a.free(patches);
            try capture.project(slot, patches, options);
        }
    } else if (capture.metal != null and !reader.track.inband_parameter_sets and compatibleApple(reader.track.avcc)) {
        var batch = try video.apple.decodeTo(a, &reader, selected, .{ .require_hardware = false, .seek_mode = .verified_idr }, .{ .context = &capture, .accept = Capture.publishApple });
        defer batch.deinit();
    } else {
        // Decode references once for the whole selection, never once per frame.
        if (configurations) |session| _ = try session.decodeSelected(selected, &capture, Capture.publish) else _ = try video.h264.decodeSelected(a, &reader, selected, .{}, &capture, Capture.publish);
    }
    // Callbacks arrive in decode order; compact in presentation slot order.
    var count: usize = 0;
    for (frame_tokens, 0..) |tokens, slot| {
        if (tokens == 0 or tokens > 140) return error.InvalidTensorShape;
        const n = tokens * 512;
        std.mem.copyForwards(f32, output[count..][0..n], output[slot * 140 * 512 ..][0..n]);
        count += n;
    }
    return .{ .embeddings = try a.dupe(f32, output[0..count]), .frame_tokens = frame_tokens };
}
fn compatibleApple(avcc: []const u8) bool {
    video.avc.validateConfig(avcc) catch return false;
    return true;
}
const Capture = struct {
    a: std.mem.Allocator,
    cb: *const ComputeBackend,
    track: media.mp4.Track,
    control: media.source.Control,
    output: []f32,
    frame_tokens: []usize,
    selected: []const usize,
    signal: video.h264.Color = .{},
    metal: ?*video.preparation.Metal = null,
    io: ?std.Io = null,
    fn copyDuplicate(self: *Capture, slot: usize) bool {
        const first = std.mem.indexOfScalar(usize, self.selected, self.selected[slot]).?;
        if (self.frame_tokens[first] == 0) return false;
        const n = self.frame_tokens[first] * 512;
        if (first != slot) @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
        self.frame_tokens[slot] = self.frame_tokens[first];
        return true;
    }
    fn project(self: *Capture, slot: usize, patches: []const f32, options: video.preparation.Options) !void {
        try self.control.check();
        const encoded = try projector.encodeEmbeddingGemma2VideoPatches(self.cb, self.a, patches, options.width, options.height);
        defer self.a.free(encoded.embeddings);
        try self.save(slot, encoded);
    }
    fn save(self: *Capture, slot: usize, encoded: anytype) !void {
        if (encoded.tokens == 0 or encoded.tokens > 140 or encoded.embeddings.len != encoded.tokens * 512) return error.InvalidTensorShape;
        @memcpy(self.output[slot * 140 * 512 ..][0..encoded.embeddings.len], encoded.embeddings);
        self.frame_tokens[slot] = encoded.tokens;
    }
    fn projectGpu(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options) !void {
        var prepared = value;
        var live = true;
        defer if (live) prepared.deinit();
        if (comptime build_options.enable_metal) {
            const io = self.io orelse return error.MissingVideoIo;
            try prepared.wait(io, self.control);
            const input = try MetalCompute.importVideoBuffer(self.cb, try prepared.buffer(), prepared.geometry.values());
            defer self.cb.free(input);
            // Give sole ownership to inference before runtime reuse can occur.
            prepared.deinit();
            live = false;
            const encoded = try projector.encodeEmbeddingGemma2VideoTensor(self.cb, self.a, input, options.width, options.height);
            defer self.a.free(encoded.embeddings);
            try self.save(slot, encoded);
        } else return error.UnsupportedVideoBackend;
    }
    fn validateRange(self: *const Capture, full: bool) !void {
        const c = self.track.color_info;
        if (c.len >= 11 and std.mem.eql(u8, c[0..4], "nclx") and (c[10] & 128 != 0) != full) return error.UnsupportedVideoColor;
    }
    fn publishApple(raw: *anyopaque, frame: *const video.apple.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        try self.validateRange(frame.surface.format == .nv12_full);
        const options = try policySignal(self.track, frame.surface.width, frame.surface.height, self.signal);
        // Duplicate logical samples must receive the same projection.
        const first = std.mem.indexOfScalar(usize, self.selected, frame.decode_index) orelse return error.InvalidPacketIndex;
        if (self.frame_tokens[first] != 0) return;
        const prepared = try self.metal.?.submit(self.a, &frame.surface, options, self.control);
        try self.projectGpu(first, prepared, options);
        const n = self.frame_tokens[first] * 512;
        for (self.selected, 0..) |index, slot| if (index == frame.decode_index and slot != first) {
            @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
            self.frame_tokens[slot] = self.frame_tokens[first];
        };
    }
    fn publish(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        if (self.copyDuplicate(slot)) return;
        try self.validateRange(frame.full_range);
        const options = try policySignal(self.track, frame.width, frame.height, frame.color);
        if (self.metal) |metal| {
            if (frame.chroma_format != 0) {
                const prepared = try metal.submitHost(self.a, frame.host(), options, self.control);
                return self.projectGpu(slot, prepared, options);
            }
        }
        const patches = try video.preparation.referenceHost(self.a, frame.host(), options, self.control);
        defer self.a.free(patches);
        try self.project(slot, patches, options);
    }
};

test "embeddinggemma2 video sampling maps presentation order and uniform long clips" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "original-mjpeg-fixture" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var packets: [100]media.mp4.Packet = undefined;
    for (&packets, 0..) |*packet, i| packet.* = .{ .offset = 0, .size = 1, .pts = @intCast(99 - i), .dts = @intCast(i), .media_pts = @intCast(99 - i), .duration = 1, .sync = true };
    var synthetic = reader;
    synthetic.packets = &packets;
    synthetic.track.timescale = 1;
    const indexes = try select(a, &synthetic);
    defer a.free(indexes);
    try std.testing.expectEqual(@as(usize, 32), indexes.len);
    try std.testing.expectEqual(@as(usize, 99), indexes[0]);
    try std.testing.expectEqual(@as(usize, 0), indexes[31]);
    for (indexes[1..], indexes[0 .. indexes.len - 1]) |next, previous| try std.testing.expect(next < previous);
    const g = projector.embeddingGemma2VideoGeometry(1920, 1080);
    try std.testing.expect(g.tokens <= 140 and g.width % 48 == 0 and g.height % 48 == 0);
    _ = try policy(reader.track, 1920, 1080);
    synthetic.track.pixel_aspect.horizontal = 2;
    try std.testing.expectError(error.UnsupportedVideoDisplay, policy(synthetic.track, 1920, 1080));
    try std.testing.expectError(error.Timeout, encode(a, undefined, "x", .{ .deadline_ns = 0 }));
}

test "embeddinggemma2 video repeated samples reuse projection and validate color declarations" {
    const a = std.testing.allocator;
    const output = try a.alloc(f32, 3 * 140 * 512);
    defer a.free(output);
    @memset(output, 0);
    @memset(output[0..512], 0.25);
    @memset(output[140 * 512 ..][0..512], 0.75);
    var counts = [_]usize{ 1, 1, 0 };
    var capture = Capture{ .a = a, .cb = undefined, .track = undefined, .control = .{}, .output = output, .frame_tokens = &counts, .selected = &.{ 2, 0, 2 } };
    try std.testing.expect(capture.copyDuplicate(2));
    try std.testing.expectEqualSlices(f32, output[0..512], output[2 * 140 * 512 ..][0..512]);
    try std.testing.expectEqual(@as(usize, 1), counts[2]);
    counts[0] = 0;
    try std.testing.expect(!capture.copyDuplicate(2));

    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "color-policy-fixture" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    try std.testing.expectEqual(video.preparation.Matrix.bt709, (try policySignal(reader.track, 640, 480, .{ .matrix = 1, .transfer = 1 })).matrix);
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(reader.track, 640, 480, .{ .transfer = 16 }));
    var track = reader.track;
    track.color_info = "nclx\x00\x01\x00\x01\x00\x06\x80";
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .matrix = 1 }));
    _ = try policy(track, 640, 480);
    capture.track = track;
    try capture.validateRange(true);
    try std.testing.expectError(error.UnsupportedVideoColor, capture.validateRange(false));
    track.color_info = track.color_info[0..10];
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}
