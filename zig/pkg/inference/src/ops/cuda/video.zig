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

//! Bounded H.264 MP4 decoding on the inference CUDA context. Compressed packets
//! and timestamp metadata use the CPU; selected NV12 surfaces and model patches
//! stay on the GPU. Successful clips reuse decoder surfaces; errors discard them.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const media = @import("antfly_media");
const context = @import("context.zig");
const driver = @import("driver.zig");
const ops = @import("../ops.zig");
const cuda = @import("cuda_compute.zig");

const Callback = *const fn (*anyopaque, u64, u32, u32, u32, i64, u32, u32) callconv(.c) c_int;
extern fn antfly_nvdec_open(*?*anyopaque, ?*anyopaque, *anyopaque, Callback) c_int;
extern fn antfly_nvdec_packet(*anyopaque, ?[*]const u8, u32, i64, c_int) c_int;
extern fn antfly_nvdec_close(*anyopaque) void;
extern fn antfly_nvdec_begin(*anyopaque, ?*anyopaque, *anyopaque, Callback, u32, u32, c_int, c_int) c_int;
extern fn antfly_nvdec_finish(*anyopaque) void;

extern fn antfly_nvdec_set_budget(*anyopaque, *anyopaque, *const fn (*anyopaque, u32, u32, u32) callconv(.c) c_int, *const fn (*anyopaque) callconv(.c) void) void;

fn reserveDecoder(raw: *anyopaque, width: u32, height: u32, surfaces: u32) callconv(.c) c_int {
    const ctx: *context.CudaContext = @ptrCast(@alignCast(raw));
    const bytes = @import("video_cache_budget.zig").envelope(width, height, surfaces) catch |err| {
        ctx.video_cache_budget.failure = err;
        return 0;
    };
    ctx.video_cache_budget.reserve(&ctx.device_allocations, bytes) catch |err| {
        ctx.video_cache_budget.failure = err;
        return 0;
    };
    return 1;
}
fn releaseDecoder(raw: *anyopaque) callconv(.c) void {
    const ctx: *context.CudaContext = @ptrCast(@alignCast(raw));
    ctx.video_cache_budget.release();
}

pub const max_frames = 60;
pub const max_duration_us = 60_000_000;
pub const Frame = struct {
    device: u64,
    pitch: u32,
    width: u32,
    height: u32,
    timestamp_us: i64,
    matrix: u32,
    full_range: u32,
    repetitions: usize = 1,
};
pub const Consumer = struct {
    context: *anyopaque,
    consume: *const fn (*anyopaque, Frame) anyerror!void,
    check: ?*const fn (*anyopaque) anyerror!void = null,
    /// Consumers opting in stage mapped pixels into owned storage and fence
    /// that staging here. Later model work need not delay surface unmapping.
    surface_ready: ?*const fn (*anyopaque) anyerror!void = null,
    coalesce_repetitions: bool = false,
};
pub const Stats = struct {
    decoded_frames: usize = 0,
    selected_frames: usize = 0,
    compressed_bytes: usize = 0,
};

/// Display-order sampling. Uses presentation time, never decode order or FPS.
pub const Sampler = struct {
    end_us: i64 = max_duration_us,
    next_us: i64 = 0,
    last_us: ?i64 = null,
    count: usize = 0,
    pub fn select(self: *Sampler, pts: i64) !bool {
        if (self.last_us) |last| if (pts < last) return error.NonMonotonicVideoPresentation;
        self.last_us = pts;
        if (pts < self.next_us or pts < 0) return false;
        if (pts >= self.end_us or self.count == max_frames) return false;
        self.next_us = (@divFloor(pts, 1_000_000) + 1) * 1_000_000;
        self.count += 1;
        return true;
    }
};

pub const Sampling = enum { presentation_seconds, embedding_gemma2 };

pub const EmbeddingInspection = struct { width: u32, height: u32, frames: usize };

test "embeddinggemma2 video container preflight shares six second sampling and cancellation" {
    const bytes = @embedFile("../../testdata/video/closed.mp4");
    const clip = try inspectEmbedding(std.testing.allocator, bytes, .{});
    try std.testing.expectEqual(@as(usize, 6), clip.frames);
    try std.testing.expect(clip.width > 0 and clip.height > 0);
    const Cancel = struct {
        fn check(_: ?*const anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, inspectEmbedding(std.testing.allocator, bytes, .{ .check_fn = Cancel.check }));
    if (inspectEmbedding(std.testing.allocator, bytes[0..32], .{})) |_| return error.TestUnexpectedResult else |_| {}
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator, source: []const u8) !void {
            _ = try inspectEmbedding(a, source, .{});
        }
    }.run, .{bytes});
}

/// Container-only inspection under the caller's host preprocessing permit.
/// Uses the decoder's validation and sampler, without opening NVDEC.
pub fn inspectEmbedding(allocator: std.mem.Allocator, bytes: []const u8, control: media.source.Control) !EmbeddingInspection {
    if (bytes.len > 256 * 1024 * 1024) return error.VideoInputTooLarge;
    var source = media.source.Source{ .allocator = allocator, .storage = .{ .borrowed = bytes }, .identity = "request-video", .limits = .{ .max_total_bytes = 512 * 1024 * 1024 }, .control = control };
    var reader = try media.mp4.Reader.init(allocator, &source, .{ .max_samples = 18_000, .max_dimension = 4096 });
    defer reader.deinit();
    try validateTrack(reader.track);
    const end_us = try validateTimeline(reader.track, reader.packets);
    const sampler = try embeddingSamplerForReader(&reader, end_us);
    return .{ .width = reader.track.width, .height = reader.track.height, .frames = sampler.length };
}

fn validateTrack(track: anytype) !void {
    if (track.codec != .avc or track.inband_parameter_sets or track.pixel_aspect.horizontal != track.pixel_aspect.vertical or
        !std.mem.eql(i32, &track.display_matrix, &.{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 })) return error.UnsupportedVideoFormat;
    _ = try containerColor(track.color_info);
}

fn embeddingSamplerForReader(reader: *const media.mp4.Reader, end_us: i64) !EmbeddingSampler {
    const track = reader.track;
    var frames: usize = 0;
    var duration_us: i64 = 0;
    for (reader.packets) |packet| {
        const pts = try timestampUs(packet.pts, track.timescale);
        if (pts < 0 or pts >= end_us) continue;
        frames += 1;
        const end = std.math.cast(i64, @as(i128, packet.pts) + packet.duration) orelse return error.InvalidVideoTimestamp;
        duration_us = @max(duration_us, @min(end_us, try timestampUs(end, track.timescale)));
    }
    // An empty edit shifts presentation time without adding content samples.
    const content_duration_us = try embeddingContentDurationUs(track, duration_us);
    return EmbeddingSampler.init(frames, content_duration_us, end_us);
}

/// EmbeddingGemma2: floor(duration * 1 FPS) samples, capped uniformly to 32.
/// Frame indices use the container's average frame rate, as the upstream processor does.
pub const EmbeddingSampler = struct {
    indices: [32]usize = undefined,
    length: usize = 0,
    next: usize = 0,
    frame: usize = 0,
    repetitions: usize = 1,
    end_us: i64,
    last_us: ?i64 = null,

    pub fn init(frame_count: usize, duration_us: i64, presentation_end_us: i64) !EmbeddingSampler {
        if (frame_count == 0 or duration_us <= 0 or presentation_end_us <= 0) return error.EmptyVideo;
        if (duration_us > max_duration_us) return error.VideoDurationExceeded;
        const duration: u64 = @intCast(duration_us);
        const samples: usize = @intCast(@max(1, duration / 1_000_000));
        var self = EmbeddingSampler{ .end_us = presentation_end_us, .length = @min(samples, 32) };
        for (0..self.length) |i| {
            const sample = if (samples > 32) i * (samples - 1) / 31 else i;
            self.indices[i] = @min(frame_count - 1, sample * frame_count * 1_000_000 / duration);
        }
        return self;
    }
    pub fn select(self: *EmbeddingSampler, pts: i64) !bool {
        if (self.last_us) |last| if (pts < last) return error.NonMonotonicVideoPresentation;
        self.last_us = pts;
        if (pts < 0 or pts >= self.end_us) return false;
        const index = self.frame;
        self.frame += 1;
        if (self.next == self.length or index != self.indices[self.next]) return false;
        self.repetitions = 0;
        while (self.next < self.length and self.indices[self.next] == index) : (self.next += 1) self.repetitions += 1;
        return true;
    }
};

const DecodeState = struct {
    ctx: *context.CudaContext,
    consumer: Consumer,
    sampler: Sampler = .{},
    embedding_sampler: ?EmbeddingSampler = null,
    stats: Stats = .{},
    failure: ?anyerror = null,
    fn checkStatus(self: *DecodeState, status: c_int) !void {
        if (self.failure) |err| return err;
        if (self.ctx.video_cache_budget.failure) |err| return err;
        try checkStatusCode(status);
    }
    fn callback(user_data: *anyopaque, device: u64, pitch: u32, width: u32, height: u32, pts: i64, matrix: u32, full_range: u32) callconv(.c) c_int {
        const self: *@This() = @ptrCast(@alignCast(user_data));
        if (self.failure != null) return -1;
        if (self.consumer.check) |check| check(self.consumer.context) catch |err| {
            self.failure = err;
            return -1;
        };
        if (device == 0) {
            self.stats.decoded_frames += 1;
            return if ((if (self.embedding_sampler) |*sampler| sampler.select(pts) else self.sampler.select(pts)) catch |err| {
                self.failure = err;
                return -1;
            }) 1 else 0;
        }
        const repetitions = if (self.embedding_sampler) |sampler| sampler.repetitions else 1;
        for (0..if (self.consumer.coalesce_repetitions) @as(usize, 1) else repetitions) |_| {
            self.consumer.consume(self.consumer.context, .{ .device = device, .pitch = pitch, .width = width, .height = height, .timestamp_us = pts, .matrix = matrix, .full_range = full_range, .repetitions = if (self.consumer.coalesce_repetitions) repetitions else 1 }) catch |err| {
                self.failure = err;
                break;
            };
        }
        // A staging fence may complete before queued vision execution. Error
        // paths always drain the stream before releasing a mapped surface.
        if (self.failure == null and self.consumer.surface_ready != null) {
            self.consumer.surface_ready.?(self.consumer.context) catch |err| {
                self.failure = err;
                self.ctx.synchronize() catch {};
            };
        } else self.ctx.synchronize() catch |err| {
            self.failure = err;
        };
        if (self.failure != null) return -1;
        self.stats.selected_frames += repetitions;
        return 1;
    }
};

fn checkStatusCode(status: c_int) !void {
    return switch (status) {
        0 => {},
        1 => error.NvdecUnavailable,
        2 => error.UnsupportedVideoFormat,
        4 => error.VideoConsumerFailed,
        5 => error.OutOfMemory,
        else => error.VideoDecodeFailed,
    };
}

pub fn decodeMp4(allocator: std.mem.Allocator, ctx: *context.CudaContext, bytes: []const u8, consumer: Consumer) !Stats {
    return decodeMp4Sampled(allocator, ctx, bytes, consumer, .presentation_seconds);
}

pub fn decodeMp4Sampled(allocator: std.mem.Allocator, ctx: *context.CudaContext, bytes: []const u8, consumer: Consumer, sampling: Sampling) !Stats {
    if (!build_options.enable_cuda or builtin.os.tag != .linux) return error.UnsupportedVideoBackend;
    if (bytes.len > 256 * 1024 * 1024) return error.VideoInputTooLarge;
    ctx.video_cache_budget.failure = null;
    var state = DecodeState{ .ctx = ctx, .consumer = consumer };
    var source = media.source.Source{ .allocator = allocator, .storage = .{ .borrowed = bytes }, .identity = "request-video", .limits = .{ .max_total_bytes = 512 * 1024 * 1024 }, .control = .{ .context = &state, .check_fn = sourceCheck } };
    var reader = try media.mp4.Reader.init(allocator, &source, .{ .max_samples = 18_000, .max_dimension = 4096 });
    defer reader.deinit();
    const track = reader.track;
    try validateTrack(track);
    const color = try containerColor(track.color_info);
    if (reader.packets.len == 0) return error.EmptyVideo;
    state.sampler.end_us = try validateTimeline(track, reader.packets);
    if (sampling == .embedding_gemma2) {
        state.embedding_sampler = try embeddingSamplerForReader(&reader, state.sampler.end_us);
    }
    try ctx.makeCurrent();
    if (ctx.video_decoder_cache == null) {
        try state.checkStatus(antfly_nvdec_open(&ctx.video_decoder_cache, ctx.stream, &state, DecodeState.callback));
        ctx.video_decoder_release = antfly_nvdec_close;
    }
    const decoder = ctx.video_decoder_cache;
    antfly_nvdec_set_budget(decoder.?, ctx, reserveDecoder, releaseDecoder);
    var succeeded = false;
    defer {
        if (succeeded) antfly_nvdec_finish(decoder.?) else {
            antfly_nvdec_close(decoder.?);
            ctx.video_decoder_cache = null;
        }
    }
    try state.checkStatus(antfly_nvdec_begin(decoder.?, ctx.stream, &state, DecodeState.callback, track.width, track.height, color.matrix, color.range));
    var annex: std.ArrayList(u8) = .empty;
    defer annex.deinit(allocator);
    try avccHeaders(allocator, &annex, track.avcc);
    try state.checkStatus(antfly_nvdec_packet(decoder.?, annex.items.ptr, @intCast(annex.items.len), 0, 0));
    for (reader.packets, 0..) |packet, index| {
        try source.control.check();
        var lease = try reader.readPacket(index);
        defer lease.deinit();
        annex.clearRetainingCapacity();
        try annexPacket(allocator, &annex, lease.bytes, track.nal_length_bytes);
        const pts = try timestampUs(packet.pts, track.timescale);
        const result = antfly_nvdec_packet(decoder.?, annex.items.ptr, @intCast(annex.items.len), pts, 0);
        if (state.failure) |err| return err;
        try state.checkStatus(result);
        state.stats.compressed_bytes += lease.bytes.len;
    }
    const result = antfly_nvdec_packet(decoder.?, null, 0, 0, 1);
    if (state.failure) |err| return err;
    try state.checkStatus(result);
    if (state.stats.selected_frames == 0) return error.EmptyVideo;
    if (state.embedding_sampler) |sampler| if (sampler.next != sampler.length) return error.VideoDecodeFailed;
    succeeded = true;
    return state.stats;
}

fn timestampUs(ticks: i64, timescale: u32) !i64 {
    if (timescale == 0) return error.InvalidVideoTimestamp;
    return std.math.cast(i64, @divFloor(@as(i128, ticks) * 1_000_000, timescale)) orelse error.InvalidVideoTimestamp;
}

fn embeddingContentDurationUs(track: media.mp4.Track, presentation_end_us: i64) !i64 {
    if (track.edit.source_duration) |duration| return timestampUs(duration, track.timescale);
    return presentation_end_us;
}

fn validateTimeline(track: media.mp4.Track, packets: []const media.mp4.Packet) !i64 {
    if (track.timescale == 0) return error.InvalidVideoTimestamp;
    const max_ticks = @as(i128, max_duration_us) * track.timescale;
    var end_us: i64 = max_duration_us;
    if (track.edit.source_duration) |duration| {
        const end_ticks = @as(i128, track.edit.source_start) + duration;
        if (end_ticks * 1_000_000 > max_ticks) return error.VideoDurationExceeded;
        if (end_ticks <= 0) return error.EmptyVideo;
        end_us = try timestampUs(std.math.cast(i64, end_ticks) orelse return error.InvalidVideoTimestamp, track.timescale);
    }
    for (packets) |packet| {
        // Validate even negative preroll before handing timestamps to the C ABI.
        _ = try timestampUs(packet.pts, track.timescale);
        if ((@as(i128, packet.pts) + packet.duration) * 1_000_000 > max_ticks) return error.VideoDurationExceeded;
    }
    return end_us;
}

test "video timeline rejects overflow and samples only the declared edit interval" {
    const track = media.mp4.Track{ .id = 1, .timescale = 1000, .width = 160, .height = 96, .avcc = &.{}, .nal_length_bytes = 4, .display_matrix = @splat(0), .edit = .{ .source_duration = 1500 } };
    const packets = [_]media.mp4.Packet{.{ .offset = 0, .size = 1, .dts = 0, .media_pts = 0, .pts = 0, .duration = 6000, .sync = true }};
    var sampler = Sampler{ .end_us = try validateTimeline(track, &packets) };
    try std.testing.expect(try sampler.select(0));
    try std.testing.expect(try sampler.select(1_000_000));
    try std.testing.expect(!try sampler.select(1_500_000));
    try std.testing.expect(!try sampler.select(2_000_000));
    try std.testing.expectError(error.InvalidVideoTimestamp, timestampUs(std.math.minInt(i64), 1));
    try std.testing.expectError(error.InvalidVideoTimestamp, timestampUs(0, 0));
    var bad = packets;
    bad[0].pts = std.math.minInt(i64);
    try std.testing.expectError(error.InvalidVideoTimestamp, validateTimeline(track, &bad));
    bad[0].pts = 60000;
    try std.testing.expectError(error.VideoDurationExceeded, validateTimeline(track, &bad));
}

test "video MP4 indexing releases leases on allocation failure and enforces limits" {
    const Runner = struct {
        fn run(a: std.mem.Allocator) !void {
            var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../../testdata/video/closed.mp4") }, .identity = "fixture" };
            defer std.debug.assert(source.retained_bytes == 0);
            var reader = try media.mp4.Reader.init(a, &source, .{});
            defer reader.deinit();
            try std.testing.expectEqual(@as(usize, 60), reader.packets.len);
            for (reader.packets, 0..) |packet, i| {
                var lease = try reader.readPacket(i);
                defer lease.deinit();
                try std.testing.expectEqual(packet.size, lease.bytes.len);
            }
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
    for ([_]media.mp4.Limits{ .{ .max_samples = 59 }, .{ .max_dimension = 32 }, .{ .max_metadata_bytes = 8 }, .{ .max_boxes = 1 }, .{ .max_index_bytes = 8 }, .{ .max_packet_bytes = 1 } }) |limits| {
        var source = media.source.Source{ .allocator = std.testing.allocator, .storage = .{ .borrowed = @embedFile("../../testdata/video/closed.mp4") }, .identity = "fixture" };
        try std.testing.expectError(error.ResourceLimitExceeded, media.mp4.Reader.init(std.testing.allocator, &source, limits));
        try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
    }
}

test "video MP4 truncated and mutated containers stay bounded and release leases" {
    const Runner = struct {
        fn probe(bytes: []const u8) !void {
            var source = media.source.Source{ .allocator = std.testing.allocator, .storage = .{ .borrowed = bytes }, .identity = "malformed" };
            defer std.debug.assert(source.retained_bytes == 0);
            var reader = media.mp4.Reader.init(std.testing.allocator, &source, .{ .max_samples = 128, .max_boxes = 256 }) catch return;
            defer reader.deinit();
            for (reader.packets) |packet| {
                try std.testing.expect(packet.offset <= bytes.len);
                try std.testing.expect(packet.size <= bytes.len - packet.offset);
            }
        }
    };
    const original = @embedFile("../../testdata/video/closed.mp4");
    // Every truncation point, then deterministic byte corruption across both
    // metadata and payload. Successful parses must still have bounded indexes.
    for (0..original.len) |len| try Runner.probe(original[0..len]);
    const mutated = try std.testing.allocator.dupe(u8, original);
    defer std.testing.allocator.free(mutated);
    for (0..original.len) |i| {
        mutated[i] ^= 0xff;
        try Runner.probe(mutated);
        mutated[i] = original[i];
    }
}
const Color = struct { matrix: c_int = -1, range: c_int = -1 };
fn containerColor(bytes: []const u8) !Color {
    if (bytes.len == 0) return .{};
    if (bytes.len < 10) return error.UnsupportedVideoContainerColor;
    const nclx = std.mem.eql(u8, bytes[0..4], "nclx");
    if ((!nclx and !std.mem.eql(u8, bytes[0..4], "nclc")) or (nclx and bytes.len != 11) or (!nclx and bytes.len != 10)) return error.UnsupportedVideoContainerColor;
    const primaries = std.mem.readInt(u16, bytes[4..6], .big);
    const transfer = std.mem.readInt(u16, bytes[6..8], .big);
    const matrix = std.mem.readInt(u16, bytes[8..10], .big);
    if ((primaries != 1 and primaries != 2 and primaries != 5 and primaries != 6) or
        (transfer != 1 and transfer != 2 and transfer != 6 and transfer != 13) or
        (matrix != 1 and matrix != 2 and matrix != 5 and matrix != 6)) return error.UnsupportedVideoContainerColor;
    if (nclx and bytes[10] & 127 != 0) return error.UnsupportedVideoContainerColor;
    return .{ .matrix = if (matrix == 2) -1 else matrix, .range = if (nclx) @intCast(bytes[10] >> 7) else -1 };
}

test "video container color accepts SDR and rejects HDR or malformed metadata" {
    try std.testing.expectEqual(Color{ .matrix = 1, .range = 1 }, try containerColor("nclx" ++ [_]u8{ 0, 1, 0, 1, 0, 1, 128 }));
    try std.testing.expectEqual(Color{ .matrix = 6 }, try containerColor("nclc" ++ [_]u8{ 0, 6, 0, 6, 0, 6 }));
    try std.testing.expectError(error.UnsupportedVideoContainerColor, containerColor("nclx" ++ [_]u8{ 0, 9, 0, 16, 0, 9, 0 }));
    try std.testing.expectError(error.UnsupportedVideoContainerColor, containerColor("nclx"));
}

fn sourceCheck(user_data: ?*const anyopaque) !void {
    const state: *const DecodeState = @ptrCast(@alignCast(user_data.?));
    if (state.consumer.check) |check| try check(state.consumer.context);
}
fn appendNal(a: std.mem.Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    if (bytes.len == 0 or bytes.len > 16 * 1024 * 1024 or out.items.len + bytes.len + 4 > 32 * 1024 * 1024) return error.InvalidVideoPacket;
    try out.appendSlice(a, &.{ 0, 0, 0, 1 });
    try out.appendSlice(a, bytes);
}
fn avccHeaders(a: std.mem.Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    if (bytes.len < 7 or bytes[0] != 1) return error.InvalidVideoConfiguration;
    var cursor: usize = 6;
    var count: usize = bytes[5] & 31;
    for (0..2) |group| {
        if (group == 1) {
            if (cursor == bytes.len) return error.InvalidVideoConfiguration;
            count = bytes[cursor];
            cursor += 1;
        }
        if (count == 0) return error.InvalidVideoConfiguration;
        for (0..count) |_| {
            if (bytes.len - cursor < 2) return error.InvalidVideoConfiguration;
            const len = std.mem.readInt(u16, bytes[cursor..][0..2], .big);
            cursor += 2;
            if (len > bytes.len - cursor) return error.InvalidVideoConfiguration;
            try appendNal(a, out, bytes[cursor..][0..len]);
            cursor += len;
        }
    }
}
fn annexPacket(a: std.mem.Allocator, out: *std.ArrayList(u8), bytes: []const u8, length_bytes: u3) !void {
    if (length_bytes != 1 and length_bytes != 2 and length_bytes != 4) return error.InvalidVideoPacket;
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        if (bytes.len - cursor < length_bytes) return error.InvalidVideoPacket;
        var len: usize = 0;
        for (bytes[cursor..][0..length_bytes]) |byte| len = (len << 8) | byte;
        cursor += length_bytes;
        if (len > bytes.len - cursor) return error.InvalidVideoPacket;
        try appendNal(a, out, bytes[cursor..][0..len]);
        cursor += len;
    }
}

pub const Preprocessor = struct {
    ctx: *context.CudaContext,
    module: driver.CUmodule,
    function: driver.CUfunction,
    rgb_function: driver.CUfunction,
    pub fn init(a: std.mem.Allocator, ctx: *context.CudaContext) !Preprocessor {
        _ = a;
        try ctx.makeCurrent();
        var module: driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, @embedFile("video_preprocess.ptx").ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var function: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "video_patches"));
        var rgb_function: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&rgb_function, module, "video_rgb8"));
        return .{ .ctx = ctx, .module = module, .function = function, .rgb_function = rgb_function };
    }
    pub fn deinit(self: *Preprocessor) void {
        self.ctx.makeCurrent() catch {};
        _ = self.ctx.driver.fns.cuModuleUnload(self.module);
    }
    pub fn rgb8(self: *Preprocessor, compute: *cuda.CudaCompute, frame: Frame) !ops.CT {
        if (frame.device == 0 or frame.width == 0 or frame.height == 0 or frame.width > 4096 or frame.height > 4096 or frame.pitch < frame.width or frame.width % 2 != 0 or frame.height % 2 != 0) return error.InvalidVideoGeometry;
        const output = try compute.createEmbeddingGemma2Tensor(&.{ frame.height, frame.width, 3 }, .u8);
        const tensor: *cuda.CudaTensor = @ptrCast(@alignCast(output));
        const cb = compute.computeBackend();
        errdefer cb.free(output);
        var src = frame.device;
        var dst = tensor.buffer.ptr;
        var sizes = [_]u32{ frame.pitch, frame.width, frame.height, frame.matrix, frame.full_range };
        var args: [7]?*anyopaque = undefined;
        args[0] = &src;
        args[1] = &dst;
        for (&sizes, 2..) |*value, i| args[i] = value;
        try self.ctx.driver.check(self.ctx.driver.fns.cuLaunchKernel(self.rgb_function, (frame.width * frame.height * 3 + 255) / 256, 1, 1, 256, 1, 1, 0, self.ctx.stream, &args, null));
        self.ctx.noteKernelLaunch();
        return output;
    }
    pub fn patches(self: *Preprocessor, cb: *const ops.ComputeBackend, frame: Frame, width: u32, height: u32, patch: u32, mean: [3]f32, stddev: [3]f32) !ops.CT {
        if (frame.device == 0 or frame.width == 0 or frame.height == 0 or frame.width > 4096 or frame.height > 4096 or frame.pitch < frame.width) return error.InvalidVideoGeometry;
        if (patch == 0 or width == 0 or height == 0 or width % patch != 0 or height % patch != 0 or width > 4096 or height > 4096) return error.InvalidVideoGeometry;
        const shape = [_]i32{ @intCast((width / patch) * (height / patch)), @intCast(patch * patch * 3) };
        const output = (try cb.allocUninitF32Shape(&shape)) orelse return error.UnsupportedVideoBackend;
        errdefer cb.free(output);
        const tensor: *cuda.CudaTensor = @ptrCast(@alignCast(output));
        var src = frame.device;
        var dst = tensor.buffer.ptr;
        var sizes = [_]u32{ frame.pitch, frame.width, frame.height, width, height, patch, frame.matrix, frame.full_range };
        var normalization = mean ++ stddev;
        var args: [16]?*anyopaque = undefined;
        args[0] = &src;
        args[1] = &dst;
        for (&sizes, 2..) |*value, i| args[i] = value;
        for (&normalization, 10..) |*value, i| args[i] = value;
        try self.ctx.driver.check(self.ctx.driver.fns.cuLaunchKernel(self.function, (width * height * 3 + 255) / 256, 1, 1, 256, 1, 1, 0, self.ctx.stream, &args, null));
        self.ctx.noteKernelLaunch();
        return output;
    }
};

test "video CUDA scalar clamps preserve bounds without host transfers" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var compute = try cuda.CudaCompute.init(a);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var values: [513]f32 = undefined;
    for (&values, 0..) |*value, i| value.* = (@as(f32, @floatFromInt(i)) - 256) / 32;
    values[0] = -std.math.inf(f32);
    values[1] = std.math.nan(f32);
    values[512] = std.math.inf(f32);
    const input = try cb.fromFloat32Shape(&values, &.{ 3, 171 });
    defer cb.free(input);
    for ([_]?f32{ null, -0.3 }) |lo| for ([_]?f32{ null, 0.7 }) |hi| {
        const before = cb.trainingRuntimeStats();
        const output = (try cb.clampScalar(input, lo, hi)) orelse return error.MissingCudaClamp;
        defer cb.free(output);
        try std.testing.expectEqual(before.d2h_bytes, cb.trainingRuntimeStats().d2h_bytes);
        try std.testing.expectEqual(before.h2d_bytes, cb.trainingRuntimeStats().h2d_bytes);
        const shape = try cb.tensorShape(output, a);
        defer a.free(shape);
        try std.testing.expectEqualSlices(i64, &.{ 3, 171 }, shape);
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        for (values, actual) |value, got| {
            var expected = value;
            if (lo) |bound| expected = @max(expected, bound);
            if (hi) |bound| expected = @min(expected, bound);
            if (std.math.isNan(expected)) try std.testing.expect(std.math.isNan(got)) else try std.testing.expectEqual(expected, got);
        }
    };
    const unchanged = try cb.toFloat32(input, a);
    defer a.free(unchanged);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&values), std.mem.sliceAsBytes(unchanged));
}

test "video CUDA vision attention matches CPU softmax across frames and long sequences" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var compute = try cuda.CudaCompute.init(a);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for ([_][3]usize{ .{ 2, 513, 64 }, .{ 1, 33, 128 } }) |size| {
        const batch, const seq, const dim = size;
        const heads = 2;
        const hidden = heads * dim;
        const n = batch * seq * hidden;
        const q = try a.alloc(f32, n);
        defer a.free(q);
        const k = try a.alloc(f32, n);
        defer a.free(k);
        const v = try a.alloc(f32, n);
        defer a.free(v);
        for (q, k, v, 0..) |*qq, *kk, *vv, i| {
            qq.* = @as(f32, @floatFromInt(i % 37)) / 11 - 1.5;
            kk.* = @as(f32, @floatFromInt((i * 13) % 53)) / 17 - 1.4;
            vv.* = @as(f32, @floatFromInt((i * 7) % 31)) / 9 - 1.6;
        }
        const shape = [_]i32{ @intCast(batch * seq), @intCast(hidden) };
        const qt = try cb.fromFloat32Shape(q, &shape);
        defer cb.free(qt);
        const kt = try cb.fromFloat32Shape(k, &shape);
        defer cb.free(kt);
        const vt = try cb.fromFloat32Shape(v, &shape);
        defer cb.free(vt);
        const before = cb.trainingRuntimeStats();
        const result = try cb.scaledDotProductAttentionGemma4Vision(qt, kt, vt, batch, seq, heads, dim);
        defer cb.free(result);
        try std.testing.expectEqual(before.d2h_bytes, cb.trainingRuntimeStats().d2h_bytes);
        try std.testing.expectEqual(before.h2d_bytes, cb.trainingRuntimeStats().h2d_bytes);
        const actual = try cb.toFloat32(result, a);
        defer a.free(actual);
        const scores = try a.alloc(f64, seq);
        defer a.free(scores);
        for (0..batch) |b| for (0..heads) |head| for ([_]usize{ 0, seq / 2, seq - 1 }) |row| {
            const base = (b * seq + row) * hidden + head * dim;
            var maximum: f64 = -std.math.inf(f64);
            for (scores, 0..) |*score, col| {
                score.* = 0;
                for (0..dim) |d| score.* += @as(f64, q[base + d]) * k[(b * seq + col) * hidden + head * dim + d];
                score.* /= @sqrt(@as(f64, @floatFromInt(dim)));
                maximum = @max(maximum, score.*);
            }
            var denominator: f64 = 0;
            for (scores) |*score| {
                score.* = @exp(score.* - maximum);
                denominator += score.*;
            }
            for (0..dim) |d| {
                var expected: f64 = 0;
                for (scores, 0..) |score, col| expected += score * v[(b * seq + col) * hidden + head * dim + d];
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected / denominator)), actual[base + d], 0.00001);
            }
        };
    }
}

test "video CUDA weight widening matches all FP16 and BF16 values and uploads compact bytes" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var compute = try cuda.CudaCompute.init(a);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const halves = try a.alloc(u16, 65536);
    defer a.free(halves);
    for (halves, 0..) |*bits, i| bits.* = @intCast(i);
    for ([_]@import("../../backends/tensor.zig").DType{ .f16, .bf16 }) |dtype| {
        const tensor = @import("../../backends/tensor.zig").Tensor{
            .name = "widening-test",
            .data = std.mem.sliceAsBytes(halves),
            .dtype = dtype,
            .shape = &.{65536},
            .allocator = a,
            .owns_data = false,
            .owns_shape = false,
        };
        const before = cb.trainingRuntimeStats();
        const output = try compute.upload16BitAsF32(&tensor);
        defer cb.free(output);
        try std.testing.expectEqual(@as(u64, 65536 * 2), cb.trainingRuntimeStats().h2d_bytes - before.h2d_bytes);
        try std.testing.expectEqual(before.d2h_bytes, cb.trainingRuntimeStats().d2h_bytes);
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        for (halves, actual) |bits, got| {
            const expected: f32 = if (dtype == .f16) @floatCast(@as(f16, @bitCast(bits))) else @bitCast(@as(u32, bits) << 16);
            if (std.math.isNan(expected)) try std.testing.expect(std.math.isNan(got)) else try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(got)));
        }
    }
}

test "video sampling uses display timestamps and bounds output" {
    var s = Sampler{};
    try std.testing.expect(!try s.select(-100));
    try std.testing.expect(try s.select(0));
    try std.testing.expect(!try s.select(33_333));
    try std.testing.expect(try s.select(1_050_000));
    try std.testing.expect(!try s.select(1_950_000));
    try std.testing.expect(try s.select(3_100_000));
    try std.testing.expectError(error.NonMonotonicVideoPresentation, s.select(3_000_000));
    try std.testing.expect(!try s.select(max_duration_us));
}
test "video packets reject truncated lengths and preserve NAL boundaries" {
    const a = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(a);
    try annexPacket(a, &out, &.{ 0, 0, 0, 2, 0x65, 0x42, 0, 0, 0, 1, 0x06 }, 4);
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 1, 0x65, 0x42, 0, 0, 0, 1, 0x06 }, out.items);
    try std.testing.expectError(error.InvalidVideoPacket, annexPacket(a, &out, &.{ 0, 0, 0, 2, 0x65 }, 4));
    try std.testing.expectError(error.InvalidVideoPacket, annexPacket(a, &out, &.{ 0, 0 }, 4));
    try std.testing.expectError(error.InvalidVideoConfiguration, avccHeaders(a, &out, &.{ 1, 0, 0, 0, 0, 1, 0 }));
}

test "video NVDEC CUDA decodes into resident patches" {
    if (comptime !build_options.enable_cuda or builtin.os.tag != .linux) return error.SkipZigTest;
    const a = std.testing.allocator;
    var compute = try cuda.CudaCompute.init(a);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var preprocess = try Preprocessor.init(a, &compute.ctx);
    defer preprocess.deinit();
    const TestConsumer = struct {
        cb: *const ops.ComputeBackend,
        preprocess: *Preprocessor,
        timestamps: [max_frames]i64 = undefined,
        count: usize = 0,
        last: ?ops.CT = null,
        fn consume(user_data: *anyopaque, frame: Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(user_data));
            const out = try self.preprocess.patches(self.cb, frame, 48, 48, 16, .{ 0, 0, 0 }, .{ 1, 1, 1 });
            if (self.last) |last| self.cb.free(last);
            self.last = out;
            self.timestamps[self.count] = frame.timestamp_us;
            self.count += 1;
        }
    };
    var consumer = TestConsumer{ .cb = &cb, .preprocess = &preprocess };
    defer if (consumer.last) |last| cb.free(last);
    const before = cb.trainingRuntimeStats();
    const stats = try decodeMp4(a, &compute.ctx, @embedFile("../../testdata/video/closed.mp4"), .{ .context = &consumer, .consume = TestConsumer.consume });
    const after = cb.trainingRuntimeStats();
    try std.testing.expectEqual(before.d2h_bytes, after.d2h_bytes);
    try std.testing.expectEqual(before.h2d_bytes, after.h2d_bytes);
    try std.testing.expect(stats.decoded_frames > stats.selected_frames);
    try std.testing.expect(stats.selected_frames > 0);
    try std.testing.expectEqual(stats.selected_frames, consumer.count);
    // Independent packet-index oracle: presentation order differs from DTS for
    // this closed-GOP fixture, and EOS must flush the final reordered pictures.
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../../testdata/video/closed.mp4") }, .identity = "oracle" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const times = try a.alloc(i64, reader.packets.len);
    defer a.free(times);
    var reordered = false;
    for (reader.packets, times) |packet, *time| {
        time.* = @intCast(@divFloor(@as(i128, packet.pts) * 1_000_000, reader.track.timescale));
        if (packet.pts != packet.dts) reordered = true;
    }
    try std.testing.expect(reordered);
    std.mem.sort(i64, times, {}, std.sort.asc(i64));
    var sampler = Sampler{};
    var selected: usize = 0;
    for (times) |time| if (try sampler.select(time)) {
        try std.testing.expectEqual(time, consumer.timestamps[selected]);
        selected += 1;
    };
    try std.testing.expectEqual(selected, consumer.count);
    const first_pixels = try cb.toFloat32(consumer.last.?, a);
    defer a.free(first_pixels);
    consumer.count = 0;
    const reused_before = cb.trainingRuntimeStats();
    const repeated = try decodeMp4(a, &compute.ctx, @embedFile("../../testdata/video/closed.mp4"), .{ .context = &consumer, .consume = TestConsumer.consume });
    try std.testing.expectEqual(stats, repeated);
    try std.testing.expectEqual(reused_before.d2h_bytes, cb.trainingRuntimeStats().d2h_bytes);
    try std.testing.expectEqual(reused_before.h2d_bytes, cb.trainingRuntimeStats().h2d_bytes);
    const repeated_pixels = try cb.toFloat32(consumer.last.?, a);
    defer a.free(repeated_pixels);
    try std.testing.expectEqualSlices(f32, first_pixels, repeated_pixels);
    const edited = try a.dupe(u8, @embedFile("../../testdata/video/closed.mp4"));
    defer a.free(edited);
    const elst = std.mem.indexOf(u8, edited, "elst").?;
    const mvhd = std.mem.indexOf(u8, edited, "mvhd").?;
    try std.testing.expectEqual(@as(u8, 0), edited[elst + 4]);
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, edited[elst + 8 ..][0..4], .big));
    const movie_scale = std.mem.readInt(u32, edited[mvhd + 16 ..][0..4], .big);
    std.mem.writeInt(u32, edited[elst + 12 ..][0..4], movie_scale * 3 / 2, .big);
    consumer.count = 0;
    const trimmed = try decodeMp4(a, &compute.ctx, edited, .{ .context = &consumer, .consume = TestConsumer.consume });
    try std.testing.expectEqual(@as(usize, 2), trimmed.selected_frames);
    try std.testing.expectEqual(stats.decoded_frames, trimmed.decoded_frames);
    const Stop = struct {
        fn consume(_: *anyopaque, _: Frame) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, decodeMp4(a, &compute.ctx, @embedFile("../../testdata/video/closed.mp4"), .{ .context = &consumer, .consume = Stop.consume }));
    try std.testing.expect(compute.ctx.video_decoder_cache == null);
    consumer.count = 0;
    const recovered = try decodeMp4(a, &compute.ctx, @embedFile("../../testdata/video/closed.mp4"), .{ .context = &consumer, .consume = TestConsumer.consume });
    try std.testing.expectEqual(stats, recovered);
    try std.testing.expectError(error.UnsupportedVideoFormat, decodeMp4(a, &compute.ctx, @embedFile("../../testdata/video/bframes.mp4"), .{ .context = &consumer, .consume = Stop.consume }));
    for (consumer.timestamps[1..consumer.count], consumer.timestamps[0 .. consumer.count - 1]) |next, prev| try std.testing.expect(next > prev);
    const pixels = try cb.toFloat32(consumer.last.?, a);
    defer a.free(pixels);
    for (pixels) |value| try std.testing.expect(std.math.isFinite(value) and value >= 0 and value <= 1.00001);
}

test "video NVDEC library unwind metadata survives consecutive CUDA contexts" {
    if (comptime !build_options.enable_cuda or builtin.os.tag != .linux) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Noop = struct {
        fn consume(_: *anyopaque, _: Frame) !void {}
        fn decodeOnce(allocator: std.mem.Allocator) !Stats {
            var compute = try cuda.CudaCompute.init(allocator);
            defer compute.deinit();
            var token: u8 = 0;
            return decodeMp4(allocator, &compute.ctx, @embedFile("../../testdata/video/closed.mp4"), .{ .context = &token, .consume = consume });
        }
    };
    const first = try Noop.decodeOnce(a);
    // SafeAllocator captures a stack on free. Before RTLD_NODELETE this walked
    // cached libnvcuvid EH metadata after its mapping had been unloaded.
    const probe = try a.alloc(u8, 1);
    a.free(probe);
    const second = try Noop.decodeOnce(a);
    try std.testing.expectEqual(first, second);
}

test "video CUDA preprocessing matches CPU RGB resize and planar patches" {
    if (comptime !build_options.enable_cuda or builtin.os.tag != .linux) return error.SkipZigTest;
    const a = std.testing.allocator;
    var compute = try cuda.CudaCompute.init(a);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var preprocess = try Preprocessor.init(a, &compute.ctx);
    defer preprocess.deinit();
    const width = 8;
    const height = 6;
    const pitch = 12;
    var nv12: [pitch * height * 3 / 2]u8 = undefined;
    for (&nv12, 0..) |*value, i| value.* = @intCast((i * 37 + 23) % 256);
    var buffer = try @import("buffer.zig").DeviceBuffer.alloc(&compute.ctx, nv12.len);
    defer buffer.free(&compute.ctx);
    try buffer.copyFromHost(&compute.ctx, &nv12);
    const mean = [3]f32{ 0.1, 0.2, 0.3 };
    const deviation = [3]f32{ 0.8, 0.7, 0.6 };
    for ([_]u32{ 1, 6 }) |matrix| for ([_]u32{ 0, 1 }) |full| {
        var rgb: [width * height * 3]u8 = undefined;
        const kr: f64 = if (matrix == 1) 0.2126 else 0.299;
        const kb: f64 = if (matrix == 1) 0.0722 else 0.114;
        for (0..height) |y| for (0..width) |x| {
            var yy: f64 = @floatFromInt(nv12[y * pitch + x]);
            const uv = pitch * height + (y / 2) * pitch + (x / 2) * 2;
            var u = @as(f64, @floatFromInt(nv12[uv])) - 128;
            var v = @as(f64, @floatFromInt(nv12[uv + 1])) - 128;
            if (full == 0) {
                yy = (yy - 16) * (255.0 / 219.0);
                u *= 255.0 / 224.0;
                v *= 255.0 / 224.0;
            }
            const values = [3]f64{ yy + 2 * (1 - kr) * v, yy - 2 * kb * (1 - kb) / (1 - kr - kb) * u - 2 * kr * (1 - kr) / (1 - kr - kb) * v, yy + 2 * (1 - kb) * u };
            for (values, 0..) |value, c| rgb[(y * width + x) * 3 + c] = @intFromFloat(@round(std.math.clamp(value, 0, 255)));
        };
        for ([_][2]u32{ .{ 6, 4 }, .{ 12, 8 } }) |size| {
            const reference = try @import("../../pipelines/image.zig").preprocessImageViewRectScaledWithResample(a, .{ .data = &rgb, .width = width, .height = height, .format = .rgb8 }, size[0], size[1], mean, deviation, 1.0 / 255.0, .bilinear);
            defer a.free(reference);
            const output = try preprocess.patches(&cb, .{ .device = buffer.ptr, .pitch = pitch, .width = width, .height = height, .timestamp_us = 0, .matrix = matrix, .full_range = full }, size[0], size[1], 2, mean, deviation);
            defer cb.free(output);
            const actual = try cb.toFloat32(output, a);
            defer a.free(actual);
            for (0..size[1] / 2) |gy| for (0..size[0] / 2) |gx| for (0..3) |c| for (0..2) |py| for (0..2) |px| {
                const index = (((gy * (size[0] / 2) + gx) * 3 + c) * 2 + py) * 2 + px;
                const expected = reference[c * size[0] * size[1] + (gy * 2 + py) * size[0] + gx * 2 + px];
                try std.testing.expectApproxEqAbs(expected, actual[index], 0.00002);
            };
        }
    };
}

test "video PTX matches its checked source" {
    const source = @embedFile("video_preprocess.cu");
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(source, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const prefix = "// source-sha256: ";
    try std.testing.expectEqualStrings(&hex, @embedFile("video_preprocess.ptx")[prefix.len..][0..64]);
}

test "embeddinggemma2 video sampling matches fractional FPS and uniformly caps the entire clip" {
    var short = try EmbeddingSampler.init(1, 300_000, 300_000);
    try std.testing.expectEqualSlices(usize, &.{0}, short.indices[0..short.length]);
    try std.testing.expect(!try short.select(-1));
    try std.testing.expect(try short.select(0));
    const fractional = try EmbeddingSampler.init(180, 6_006_000, 6_006_000);
    try std.testing.expectEqualSlices(usize, &.{ 0, 29, 59, 89, 119, 149 }, fractional.indices[0..fractional.length]);
    const capped = try EmbeddingSampler.init(1800, 60_000_000, 60_000_000);
    try std.testing.expectEqual(@as(usize, 32), capped.length);
    try std.testing.expectEqual(@as(usize, 1770), capped.indices[31]);
    var low_rate = try EmbeddingSampler.init(2, 4_000_000, 4_000_000);
    try std.testing.expect(try low_rate.select(0));
    try std.testing.expectEqual(@as(usize, 2), low_rate.repetitions);
    try std.testing.expect(try low_rate.select(2_000_000));
    try std.testing.expectEqual(@as(usize, 4), low_rate.next);
    try std.testing.expectError(error.NonMonotonicVideoPresentation, low_rate.select(1));
    try std.testing.expectError(error.VideoDurationExceeded, EmbeddingSampler.init(1801, 60_000_001, 60_000_001));
    try std.testing.expectError(error.EmptyVideo, EmbeddingSampler.init(0, 1, 1));
}

test "embeddinggemma2 sampling excludes a leading empty edit from content duration" {
    // Equivalent to the parsed timeline of a 60-frame, 10 FPS clip preceded
    // by a two-second empty edit. PyAV reports duration=6s and start_time=2s;
    // the pinned processor therefore selects [0, 10, 20, 30, 40, 50].
    const track = media.mp4.Track{
        .id = 1,
        .timescale = 10_240,
        .width = 160,
        .height = 96,
        .avcc = &.{},
        .nal_length_bytes = 4,
        .display_matrix = @splat(0),
        .edit = .{ .source_start = 20_480, .source_duration = 61_440 },
    };
    const presentation_end_us = try timestampUs(track.edit.source_start + track.edit.source_duration.?, track.timescale);
    const content_duration_us = try embeddingContentDurationUs(track, presentation_end_us);
    const sampler = try EmbeddingSampler.init(60, content_duration_us, presentation_end_us);
    try std.testing.expectEqual(@as(i64, 8_000_000), sampler.end_us);
    try std.testing.expectEqualSlices(usize, &.{ 0, 10, 20, 30, 40, 50 }, sampler.indices[0..sampler.length]);
}

test "embeddinggemma2 video CUDA RGB and bicubic patches match the CPU torchvision path" {
    if (comptime !build_options.enable_cuda or builtin.os.tag != .linux) return error.SkipZigTest;
    const a = std.testing.allocator;
    var compute = try cuda.CudaCompute.init(a);
    defer compute.deinit();
    compute.capability_profile = .embedding_gemma2;
    const cb = compute.computeBackend();
    var preprocess = try Preprocessor.init(a, &compute.ctx);
    defer preprocess.deinit();
    const width = 128;
    const height = 96;
    const pitch = 132;
    var nv12: [pitch * height * 3 / 2]u8 = undefined;
    for (&nv12, 0..) |*value, i| value.* = @intCast((i * 37 + 23) % 256);
    var buffer = try @import("buffer.zig").DeviceBuffer.alloc(&compute.ctx, nv12.len);
    defer buffer.free(&compute.ctx);
    try buffer.copyFromHost(&compute.ctx, &nv12);
    const mean = [3]f32{ 0, 0, 0 };
    const deviation = [3]f32{ 1, 1, 1 };
    for ([_]u32{ 1, 6 }) |matrix| for ([_]u32{ 0, 1 }) |full| {
        var rgb: [width * height * 3]u8 = undefined;
        const kr: f64 = if (matrix == 1) 0.2126 else 0.299;
        const kb: f64 = if (matrix == 1) 0.0722 else 0.114;
        for (0..height) |y| for (0..width) |x| {
            var yy: f64 = @floatFromInt(nv12[y * pitch + x]);
            const uv = pitch * height + (y / 2) * pitch + (x / 2) * 2;
            var u = @as(f64, @floatFromInt(nv12[uv])) - 128;
            var v = @as(f64, @floatFromInt(nv12[uv + 1])) - 128;
            if (full == 0) {
                yy = (yy - 16) * (255.0 / 219.0);
                u *= 255.0 / 224.0;
                v *= 255.0 / 224.0;
            }
            const values = [3]f64{ yy + 2 * (1 - kr) * v, yy - 2 * kb * (1 - kb) / (1 - kr - kb) * u - 2 * kr * (1 - kr) / (1 - kr - kb) * v, yy + 2 * (1 - kb) * u };
            for (values, 0..) |value, c| rgb[(y * width + x) * 3 + c] = @intFromFloat(@round(std.math.clamp(value, 0, 255)));
        };
        for ([_][2]u32{ .{ 48, 48 }, .{ 96, 48 }, .{ 192, 144 } }) |size| {
            const reference = try @import("../../pipelines/image.zig").preprocessImageViewRectScaledWithResample(a, .{ .data = &rgb, .width = width, .height = height, .format = .rgb8 }, size[0], size[1], mean, deviation, 1.0 / 255.0, .torchvision_bicubic);
            defer a.free(reference);
            const before = cb.trainingRuntimeStats();
            const resident_rgb = try preprocess.rgb8(&compute, .{ .device = buffer.ptr, .pitch = pitch, .width = width, .height = height, .timestamp_us = 0, .matrix = matrix, .full_range = full });
            defer cb.free(resident_rgb);
            const output = try compute.preprocessEmbeddingGemma2ResidentRgb(resident_rgb, width, height, size[0], size[1]);
            defer cb.free(output);
            const after = cb.trainingRuntimeStats();
            try std.testing.expectEqual(before.d2h_bytes, after.d2h_bytes);
            const actual = try cb.toFloat32(output, a);
            defer a.free(actual);
            for (0..size[1] / 16) |gy| for (0..size[0] / 16) |gx| for (0..16) |py| for (0..16) |px| for (0..3) |c| {
                const index = ((gy * (size[0] / 16) + gx) * 256 + py * 16 + px) * 3 + c;
                const expected = 2 * (reference[c * size[0] * size[1] + (gy * 16 + py) * size[0] + gx * 16 + px] - 0.5);
                try std.testing.expectEqual(expected, actual[index]);
            };
        }
    };
}

test {
    _ = @import("video_cache_budget.zig");
}

test "embeddinggemma2 video staging fences coalesce repeats and drain consumer errors" {
    const Mock = struct {
        var synchronizations: usize = 0;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            synchronizations += 1;
            return 0;
        }
        const State = struct {
            calls: usize = 0,
            copies: usize = 0,
            ready: usize = 0,
            fail: bool = false,
            fn consume(raw: *anyopaque, frame: Frame) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                self.copies += frame.repetitions;
                if (self.fail) return error.Cancelled;
            }
            fn surfaceReady(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ready += 1;
            }
        };
    };
    Mock.synchronizations = 0;
    var ctx: context.CudaContext = undefined;
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    var consumer = Mock.State{};
    var state = DecodeState{ .ctx = &ctx, .consumer = .{ .context = &consumer, .consume = Mock.State.consume, .surface_ready = Mock.State.surfaceReady, .coalesce_repetitions = true } };
    // Three model slots can refer to one coded picture. One staging/vision
    // call carries all repeats while retaining three logical sample slots.
    var sampler = try EmbeddingSampler.init(1, 1_000_000, 1_000_000);
    sampler.repetitions = 3;
    state.embedding_sampler = sampler;
    try std.testing.expectEqual(@as(c_int, 1), DecodeState.callback(&state, 1, 256, 160, 96, 0, 2, 0));
    try std.testing.expectEqual(@as(usize, 1), consumer.calls);
    try std.testing.expectEqual(@as(usize, 3), consumer.copies);
    try std.testing.expectEqual(@as(usize, 1), consumer.ready);
    try std.testing.expectEqual(@as(usize, 0), Mock.synchronizations);
    consumer.fail = true;
    try std.testing.expectEqual(@as(c_int, -1), DecodeState.callback(&state, 1, 256, 160, 96, 1, 2, 0));
    try std.testing.expectEqual(@as(usize, 1), consumer.ready);
    try std.testing.expectEqual(@as(usize, 1), Mock.synchronizations);
    try std.testing.expectEqual(error.Cancelled, state.failure.?);
}
