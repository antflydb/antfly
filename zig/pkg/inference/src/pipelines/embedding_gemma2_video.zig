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

//! Ordered, bounded video frames using the pinned EmbeddingGemma 2 processor.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const projector = @import("../architectures/gemma4_projector.zig");
const CT = @import("../ops/ops.zig").CT;
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const build_options = @import("build_options");
const MetalCompute = @import("../ops/metal_compute.zig").MetalCompute;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const FrameCache = @import("embedding_gemma2_video_cache.zig").Cache;
const CacheKey = @import("embedding_gemma2_video_cache.zig").Key;
const CacheTicket = @import("embedding_gemma2_video_cache.zig").Ticket;
pub const Encoded = struct {
    embeddings: []f32,
    tensor: ?CT = null,
    backend: ?*const ComputeBackend = null,
    frame_tokens: []usize,
    pub fn deinit(self: Encoded, a: std.mem.Allocator) void {
        a.free(self.embeddings);
        if (self.tensor) |tensor| self.backend.?.free(tensor);
        a.free(self.frame_tokens);
    }
};

/// Sample logical presentation frames, then map them to decode packet indexes.
/// The FPS policy matches HF; timestamp selection remains a separate video API.
pub const Sampling = union(enum) { reference, timestamps: video.sampling.TimestampPolicy, source_timestamps: video.sampling.TimestampPolicy };
/// Owned selection from a validated logical index. Input and policy are
/// content-bound; this owner must outlive all Prefetch workers borrowing it.
pub const ReaderSelection = struct {
    allocator: std.mem.Allocator,
    selected: []const usize,
    projection_keys: []const CacheKey,
    binding: [32]u8,
    reservation: media.admission.Token = .{},
    pub fn init(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, logical: *const video.h264_pictures.Index, pool: ?*media.admission.Pool, control: media.source.Control) !ReaderSelection {
        if (reader.track.codec != .avc or logical.packet_count != reader.packets.len or logical.timescale != reader.track.timescale or logical.pictures.len > 8192) return error.InvalidVideoReaderSelection;
        const bound = try std.math.add(usize, try std.math.mul(usize, logical.pictures.len, @sizeOf(video.sampling.TimedPicture)), 128 * @sizeOf(usize) + 32 * @sizeOf(CacheKey));
        var reservation = if (pool) |shared| try shared.acquire(.{ .host_bytes = bound }) else media.admission.Token{};
        errdefer reservation.deinit();
        const binding = try readerBinding(reader, sampling, control);
        const selected = try selectLogical(a, logical.*, sampling, control);
        errdefer a.free(selected);
        if (selected.len == 0 or selected.len > 32) return error.InvalidVideoReaderSelection;
        for (selected) |id| if (id >= reader.packets.len) return error.InvalidVideoReaderSelection;
        const keys = try readerProjectionKeys(a, reader, logical, selected, control);
        errdefer a.free(keys);
        try reservation.resize(.{ .host_bytes = selected.len * (@sizeOf(usize) + @sizeOf(CacheKey)) });
        return .{ .allocator = a, .selected = selected, .projection_keys = keys, .binding = binding, .reservation = reservation };
    }
    pub fn deinit(self: *ReaderSelection) void {
        self.allocator.free(self.selected);
        self.allocator.free(self.projection_keys);
        self.reservation.deinit();
        self.* = undefined;
    }
    pub fn select(self: *const ReaderSelection, a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, control: media.source.Control) ![]usize {
        const binding = try readerBinding(reader, sampling, control);
        if (!std.mem.eql(u8, &binding, &self.binding) or self.selected.len == 0 or self.selected.len > 32 or self.projection_keys.len != self.selected.len) return error.InvalidVideoReaderSelection;
        for (self.selected) |id| if (id >= reader.packets.len) return error.InvalidVideoReaderSelection;
        return a.dupe(usize, self.selected);
    }
};
/// Bind coded dependencies through completion, independent of byte placement.
/// A complete single-packet IDR with static parameter sets starts a new hash
/// chain. Dynamic configuration and split pictures retain the conservative chain.
fn readerProjectionKeys(a: std.mem.Allocator, reader: *const media.mp4.Reader, logical: *const video.h264_pictures.Index, selected: []const usize, control: media.source.Control) ![]CacheKey {
    const bytes = try packetBytes(reader);
    const keys = try a.alloc(CacheKey, selected.len);
    errdefer a.free(keys);
    var ends: [32]usize = undefined;
    var maximum: usize = 0;
    for (selected, 0..) |id, slot| {
        var found = false;
        for (logical.pictures) |picture| if (picture.first_packet == id) {
            const end = picture.last_packet orelse reader.packets.len - 1;
            if (end < id or end >= reader.packets.len) return error.InvalidVideoReaderSelection;
            ends[slot] = end;
            maximum = @max(maximum, end);
            found = true;
            break;
        };
        if (!found) return error.InvalidVideoReaderSelection;
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-reader-projection-dependencies-v2");
    hashValue(&hash, reader.track);
    const initial = hash;
    var base: usize = 0;
    for (reader.packets[0 .. maximum + 1], 0..) |packet, index| {
        try control.check();
        if (packet.offset > bytes.len or packet.size > bytes.len - packet.offset) return error.InvalidVideoReaderSelection;
        const payload = bytes[@intCast(packet.offset)..][0..packet.size];
        if (!reader.track.inband_parameter_sets and try video.avc.isPortableIdr(payload, reader.track.nal_length_bytes) and projectionReset(logical, index)) {
            hash = initial;
            base = index;
        }
        // Transport offsets and selector rebasing cannot change rendered pixels.
        // Keep every timing, size and sync value in the content identity.
        var descriptor = packet;
        descriptor.offset = 0;
        hashValue(&hash, descriptor);
        hashValue(&hash, payload);
        for (selected, 0..) |id, slot| if (ends[slot] == index) {
            var snapshot = hash;
            hashValue(&snapshot, index - base + 1);
            var digest: [32]u8 = undefined;
            snapshot.final(&digest);
            // Unknown completion metadata never permits a reset past its start.
            if (id < base) return error.InvalidVideoReaderSelection;
            keys[slot] = .{ .clip = digest, .picture = id - base };
        };
    }
    return keys;
}
fn projectionReset(logical: *const video.h264_pictures.Index, packet: usize) bool {
    if (logical.pending_first_packet) |pending| if (pending < packet) return false;
    var complete = false;
    for (logical.pictures) |picture| {
        const last = picture.last_packet orelse return false;
        if (picture.first_packet < packet and last >= packet) return false;
        if (picture.first_packet == packet and last == packet) complete = true;
    }
    return complete;
}
fn projectionKey(selection: ?*const ReaderSelection, clip: [32]u8, selected: []const usize, slot: usize) CacheKey {
    return if (selection) |owner| owner.projection_keys[slot] else .{ .clip = clip, .picture = selected[slot] };
}
fn hashValue(hash: *std.crypto.hash.sha2.Sha256, value: anytype) void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .int => |info| {
            var bytes: [8]u8 = undefined;
            const word: u64 = if (info.signedness == .signed) @bitCast(@as(i64, @intCast(value))) else @intCast(value);
            std.mem.writeInt(u64, &bytes, word, .little);
            hash.update(&bytes);
        },
        .bool => hashValue(hash, @as(u8, @intFromBool(value))),
        .@"enum" => hashValue(hash, @as(u64, @backingInt(value))),
        .@"struct" => inline for (@typeInfo(T).@"struct".field_names) |name| hashValue(hash, @field(value, name)),
        .array => for (value) |item| hashValue(hash, item),
        .pointer => |pointer| {
            if (pointer.size != .slice) @compileError("reader binding requires value metadata");
            hashValue(hash, @as(u64, value.len));
            if (pointer.child == u8) hash.update(value) else for (value) |item| hashValue(hash, item);
        },
        .optional => {
            hashValue(hash, value != null);
            if (value) |item| hashValue(hash, item);
        },
        else => @compileError("unsupported reader binding metadata"),
    }
}
fn readerBinding(reader: *const media.mp4.Reader, sampling: Sampling, control: media.source.Control) ![32]u8 {
    try control.check();
    const bytes = try packetBytes(reader);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-reader-selection-v1");
    hashValue(&hash, reader.track);
    hashValue(&hash, @as(u8, @backingInt(std.meta.activeTag(sampling))));
    switch (sampling) {
        .reference => {},
        .timestamps, .source_timestamps => |policy_value| hashValue(&hash, policy_value),
    }
    hashValue(&hash, @as(u64, reader.packets.len));
    for (reader.packets) |packet| {
        try control.check();
        hashValue(&hash, packet);
        hash.update(bytes[@intCast(packet.offset)..][0..packet.size]);
    }
    return hash.finalResult();
}
fn selectLogical(a: std.mem.Allocator, logical: video.h264_pictures.Index, sampling: Sampling, control: media.source.Control) ![]usize {
    if (sampling == .source_timestamps) if (logical.pending_pts) |pts| {
        const end = sampling.source_timestamps.end_ns orelse return error.InvalidLiveWatermark;
        if (@as(i128, pts) * std.time.ns_per_s < @as(i128, end) * logical.timescale) return error.InvalidLiveWatermark;
    };
    if (sampling == .reference) {
        const owned = try logical.select();
        defer logical.allocator.free(owned);
        return a.dupe(usize, owned);
    }
    const metadata = try a.alloc(video.sampling.TimedPicture, logical.pictures.len);
    defer a.free(metadata);
    for (logical.pictures, metadata) |picture, *item| {
        try control.check();
        item.* = .{ .pts = picture.pts, .duration = picture.duration };
    }
    const selected = try timestampSelect(a, metadata, logical.timescale, sampling, control);
    for (selected) |*id| id.* = logical.pictures[id.*].first_packet;
    return selected;
}
fn selectReader(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, control: media.source.Control, selection: ?*const ReaderSelection) ![]usize {
    return if (selection) |bound| bound.select(a, reader, sampling, control) else selectWithSampling(a, reader, sampling);
}
fn timestampSelect(a: std.mem.Allocator, metadata: []const video.sampling.TimedPicture, scale: u32, sampling: Sampling, control: media.source.Control) ![]usize {
    return switch (sampling) {
        .timestamps => |interval| video.sampling.cappedTimestamps(a, metadata, scale, interval, control),
        .source_timestamps => |interval| video.sampling.cappedSourceTimestamps(a, metadata, scale, interval, control),
        .reference => error.InvalidSamplingPolicy,
    };
}
pub fn select(a: std.mem.Allocator, reader: *const media.mp4.Reader) ![]usize {
    return selectWithSampling(a, reader, .reference);
}
pub fn selectWithSampling(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling) ![]usize {
    if (reader.track.codec == .avc) {
        const cfg = try video.h264.configParse(a, reader.track.avcc);
        defer cfg.groups.deinit(a);
        if (reader.track.inband_parameter_sets or !cfg.frame_only or cfg.profile == 88 or cfg.separate_planes) {
            // Index completed assemblies rather than transport members.
            var mutable = reader.*;
            var logical = if (sampling == .source_timestamps)
                try video.h264_pictures.streaming(a, &mutable, .{})
            else
                try video.h264_pictures.index(a, &mutable, .{});
            defer logical.deinit();
            if (sampling == .source_timestamps) {
                // A live window may carry an unfinished future field pair as
                // decoder context, but never sample inside its pending barrier.
                if (logical.pending_pts) |pts| {
                    const end = sampling.source_timestamps.end_ns orelse return error.InvalidLiveWatermark;
                    if (@as(i128, pts) * std.time.ns_per_s < @as(i128, end) * logical.timescale) return error.InvalidLiveWatermark;
                }
            }
            if (sampling == .reference) return logical.select();
            const metadata = try a.alloc(video.sampling.TimedPicture, logical.pictures.len);
            defer a.free(metadata);
            for (logical.pictures, metadata) |picture, *item| item.* = .{ .pts = picture.pts, .duration = picture.duration };
            const selected = try timestampSelect(a, metadata, logical.timescale, sampling, reader.input.control);
            for (selected) |*id| id.* = logical.pictures[id.*].first_packet;
            return selected;
        }
    }
    if (sampling != .reference) {
        const metadata = try a.alloc(video.sampling.TimedPicture, reader.packets.len);
        defer a.free(metadata);
        for (reader.packets, metadata) |packet, *item| item.* = .{ .pts = packet.pts, .duration = packet.duration };
        return timestampSelect(a, metadata, reader.track.timescale, sampling, reader.input.control);
    }
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
    return policyLayout(track, try video.display.layout(track, width, height, .presentation), width, signal);
}
fn policyLayout(track: media.mp4.Track, display: video.display.Layout, width: u32, signal: video.h264.Color) !video.preparation.Options {
    var transfer: u16 = signal.transfer orelse 2;
    var declared_matrix: u16 = signal.matrix orelse 6;
    var primaries: u16 = signal.primaries orelse 2;
    if (track.color_info.len != 0) {
        const c = track.color_info;
        if (c.len < 10 or (!std.mem.eql(u8, c[0..4], "nclx") and !std.mem.eql(u8, c[0..4], "nclc"))) return error.UnsupportedVideoColor;
        if (std.mem.eql(u8, c[0..4], "nclx") and c.len < 11) return error.UnsupportedVideoColor;
        const container_primaries = std.mem.readInt(u16, c[4..6], .big);
        const container_transfer = std.mem.readInt(u16, c[6..8], .big);
        const container_matrix = std.mem.readInt(u16, c[8..10], .big);
        if (signal.primaries) |value| if (container_primaries != 2 and value != container_primaries) return error.UnsupportedVideoColor;
        if (signal.transfer) |value| if (container_transfer != 2 and value != container_transfer) return error.UnsupportedVideoColor;
        if (signal.matrix) |value| if (container_matrix != 2 and value != container_matrix) return error.UnsupportedVideoColor;
        if (container_primaries != 2) primaries = container_primaries;
        if (container_transfer != 2) transfer = container_transfer;
        if (container_matrix != 2) declared_matrix = container_matrix;
    }
    const matrix: video.preparation.Matrix = switch (declared_matrix) {
        1 => .bt709,
        5, 6 => .bt601,
        9 => .bt2020_ncl,
        else => return error.UnsupportedVideoColor,
    };
    const tone: video.preparation.ToneMapping = switch (transfer) {
        1, 2, 6, 13 => .none,
        16 => .pq_reinhard_100,
        18 => .hlg_reinhard_100,
        else => return error.UnsupportedVideoColor,
    };
    if (tone != .none) {
        // Require explicit consistent primaries; never guess an HDR gamut.
        if (track.codec != .avc or !((matrix == .bt2020_ncl and primaries == 9) or (matrix == .bt709 and primaries == 1))) return error.UnsupportedVideoColor;
    } else if (matrix == .bt2020_ncl or (primaries != 2 and primaries != 1 and primaries != 5 and primaries != 6)) return error.UnsupportedVideoColor;
    const g = projector.embeddingGemma2VideoGeometry(display.width, display.height);
    // Apply SAR through target aspect; a single separable resample avoids an
    // intermediate stretched raster and its additional RGB8 rounding.
    return .{ .width = g.width, .height = g.height, .matrix = matrix, .rotation = display.rotation, .tone_mapping = tone, .torchvision = true, .rgb_rounding = if (width % 16 == 0) .q13_components else .q16_luma };
}

fn parameterSession(a: std.mem.Allocator, reader: *media.mp4.Reader) !?*video.h264_dynamic.Session {
    if (reader.track.codec != .avc) return null;
    if (reader.track.inband_parameter_sets) {
        const session = try video.h264_dynamic.Session.init(a, reader, .{});
        errdefer session.deinit();
        for (session.entries.items) |entry| {
            _ = try policySignal(reader.track, @intCast(entry.config.width), @intCast(entry.config.height), entry.config.color);
        }
        return session;
    }
    const cfg = try video.h264.configParse(a, reader.track.avcc);
    defer cfg.groups.deinit(a);
    _ = try policySignal(reader.track, @intCast(cfg.width), @intCast(cfg.height), cfg.color);
    return null;
}

fn isWebm(bytes: []const u8) bool {
    return bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "\x1a\x45\xdf\xa3");
}
// Own normalized TS storage alongside the packet reader. Returning this owner
// is safe: the snapshot's Source address is stable until deinit.
const PacketInput = struct {
    reader: media.mp4.Reader,
    snapshot: ?*media.stream.Snapshot = null,
    fn init(a: std.mem.Allocator, source: *media.source.Source, bytes: []const u8) !PacketInput {
        if (media.transport_stream.isTransportStream(bytes)) {
            const normalized = try video.transport_avc.Reader.init(a, source.identity, bytes, .{}, .{
                .transport = .{ .max_bytes = 64 * 1024 * 1024 },
                .stream = .{ .max_bytes = 64 * 1024 * 1024, .control = source.control, .admission_pool = source.admission_pool },
            });
            return .{ .reader = normalized.reader, .snapshot = normalized.snapshot };
        }
        return .{ .reader = try media.mp4.Reader.init(a, source, .{}) };
    }
    fn deinit(self: *PacketInput) void {
        self.reader.deinit();
        if (self.snapshot) |snapshot| snapshot.deinit();
    }
};
fn webmTrack(track: media.webm.Track) !media.mp4.Track {
    // Codec-independent display policy; codec reconstruction remains in WebM.
    return .{ .codec = .avc, .id = 1, .timescale = 1_000_000_000, .width = std.math.cast(u16, track.width) orelse return error.ResourceLimitExceeded, .height = std.math.cast(u16, track.height) orelse return error.ResourceLimitExceeded, .avcc = &.{}, .nal_length_bytes = 0, .display_matrix = .{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 } };
}

fn webmPolicy(track: media.webm.Track, width: u32, height: u32, signal: video.h264.Color) !video.preparation.Options {
    const layout = try video.display.webmLayout(track, width, height, .presentation);
    var common = try webmTrack(track);
    const c = track.colour;
    var declaration: [10]u8 = undefined;
    @memcpy(declaration[0..4], "nclc");
    std.mem.writeInt(u16, declaration[4..6], c.primaries, .big);
    std.mem.writeInt(u16, declaration[6..8], c.transfer, .big);
    std.mem.writeInt(u16, declaration[8..10], c.matrix, .big);
    common.color_info = &declaration;
    var options = try policyLayout(common, layout.display, width, signal);
    options.crop = layout.crop;
    return options;
}
fn webmHost(track: media.webm.Track, frame: *const video.codecs.Frame) !video.preparation.HostSurface {
    const c = track.colour;
    if (c.bits_per_channel != 0 and c.bits_per_channel != frame.bit_depth) return error.UnsupportedVideoColor;
    const horizontal: u8 = if (frame.chroma_format == 3) 0 else 1;
    const vertical: u8 = if (frame.chroma_format == 1) 1 else 0;
    if (c.subsampling_h) |v| if (frame.chroma_format == 0 or v != horizontal) return error.UnsupportedVideoColor;
    if (c.subsampling_v) |v| if (frame.chroma_format == 0 or v != vertical) return error.UnsupportedVideoColor;
    if (c.cb_subsampling_h) |v| if (v != 0) return error.UnsupportedVideoColor;
    if (c.cb_subsampling_v) |v| if (v != 0) return error.UnsupportedVideoColor;
    // Current RGB conversion replicates coded chroma samples. Explicit siting
    // requires a separate reconstruction filter rather than an ignored tag.
    if (c.siting_h != 0 or c.siting_v != 0) return error.UnsupportedVideoColor;
    var host = frame.host();
    if (c.range == 3) return error.UnsupportedVideoColor;
    if (c.range != 0) {
        const full = c.range == 2;
        // VP8 has no full/limited-range signal; container Range is authoritative.
        if (track.codec != .vp8 and full != frame.color.full_range) return error.UnsupportedVideoColor;
        host.format = if (full) .nv12_full else .nv12_video;
    }
    return host;
}

/// Conservative decoded-pixel charge used before admitted HTTP model execution.
pub fn inspectPixels(bytes: []const u8, control: Control) !u64 {
    return inspectPixelsWithSampling(bytes, control, .reference);
}
pub fn inspectPixelsWithSampling(bytes: []const u8, control: Control, sampling: Sampling) !u64 {
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.heap.page_allocator, .limit = 512 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const plan = inspectPlan(bounded.allocator(), bytes, control, sampling, &pool, false) catch |err| return if ((err == error.OutOfMemory and bounded.denied) or err == error.SharedAdmissionExceeded) error.MemoryBudgetExceeded else err;
    return plan.pixels;
}

/// Geometry-derived share of a prepaid execution reservation. Counts maximum
/// concurrent preparations rather than every sampled frame retained by the model.
pub const Plan = struct {
    pixels: u64 = 0,
    preparation_bytes: usize = 0,
    apple_surface_bytes: usize = 0,
    commands: u64 = 0,
    pub fn deviceBytes(self: Plan) !usize {
        return std.math.add(usize, 16 * 1024 * 1024, try std.math.add(usize, self.apple_surface_bytes, try std.math.mul(usize, self.preparation_bytes, @intCast(self.commands))));
    }
    fn picture(self: *Plan, width: u32, height: u32, options: video.preparation.Options) !void {
        const geometry = try video.preparation.geometry(width, height, options);
        // All native outputs are tightly packed; extra 64-pixel row/height
        // padding also covers the imported/copy paths without guessing depth.
        const staging = try std.math.mul(usize, try std.math.mul(usize, std.mem.alignForward(usize, width, 64), std.mem.alignForward(usize, height, 64)), 6);
        if (staging > options.max_host_staging_bytes) return error.ResourceLimitExceeded;
        self.preparation_bytes = @max(self.preparation_bytes, try std.math.add(usize, geometry.scratch_bytes, staging));
        self.pixels = try std.math.add(u64, self.pixels, try std.math.mul(u64, width, height));
    }
};
fn preparationSlots(selected: []const usize) u64 {
    var unique: u64 = 0;
    for (selected, 0..) |id, index| {
        if (std.mem.indexOfScalar(usize, selected[0..index], id) == null) unique += 1;
    }
    return @min(unique, 5);
}
/// Packet readers are borrowed until synchronous encoding or Prefetch.deinit.
/// The backing source must be immutable borrowed storage; metadata and packets
/// stay owned by the caller. Model reads use independent counters and controls.
pub fn packetBytes(reader: *const media.mp4.Reader) ![]const u8 {
    const bytes = switch (reader.input.storage) {
        .borrowed => |value| value,
        .range => return error.UnsupportedVideoPacketSource,
    };
    if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024 or reader.packets.len == 0 or reader.track.timescale == 0) return error.ResourceLimitExceeded;
    for (reader.packets) |packet| if (packet.offset > bytes.len or packet.size > bytes.len - packet.offset) return error.UnexpectedEndOfSource;
    return bytes;
}
pub fn inspectReaderPlan(a: std.mem.Allocator, borrowed: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, metal: bool) !Plan {
    return inspectReaderPlanWithSelection(a, borrowed, control, sampling, pool, metal, null);
}
pub fn inspectReaderPlanWithSelection(a: std.mem.Allocator, borrowed: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, metal: bool, selection: ?*const ReaderSelection) !Plan {
    const bytes = try packetBytes(borrowed);
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = borrowed.input.identity, .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    var reader = borrowed.*;
    reader.input = &source;
    return inspectPacketPlan(a, &reader, control, sampling, metal, selection);
}
pub fn inspectPlan(a: std.mem.Allocator, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, metal: bool) !Plan {
    if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "video-preflight", .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    if (isWebm(bytes)) {
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var index = try video.webm_pictures.index(a, &reader, .{ .decoder = .{ .control = source.control, .admission_pool = pool } });
        defer index.deinit();
        const selected = try selectWebm(a, index, sampling, source.control);
        defer a.free(selected);
        var plan = Plan{ .commands = preparationSlots(selected) };
        for (selected) |id| {
            const picture = index.pictures[id];
            var preparation = try webmPolicy(reader.track, picture.width, picture.height, .{});
            if (preparation.tone_mapping == .none) preparation.rgb_rounding = video.preparation.softwareSdrRounding(picture.width, picture.chroma_format, picture.bit_depth);
            try plan.picture(picture.width, picture.height, preparation);
        }
        return plan;
    }
    var input = try PacketInput.init(a, &source, bytes);
    defer input.deinit();
    return inspectPacketPlan(a, &input.reader, control, sampling, metal, null);
}
fn inspectPacketPlan(a: std.mem.Allocator, reader: *media.mp4.Reader, control: Control, sampling: Sampling, metal: bool, selection: ?*const ReaderSelection) !Plan {
    // AVC combines VUI and container declarations in parameterSession below.
    if (reader.track.codec != .avc) _ = try policy(reader.track, reader.track.width, reader.track.height);
    const selected = try selectReader(a, reader, sampling, reader.input.control, selection);
    defer a.free(selected);
    const configurations = try parameterSession(a, reader);
    defer if (configurations) |session| session.deinit();
    const static_config = if (reader.track.codec == .avc and configurations == null) try video.h264.configParse(a, reader.track.avcc) else null;
    defer if (static_config) |cfg| cfg.groups.deinit(a);
    var plan = Plan{ .commands = preparationSlots(selected) };
    if (metal and reader.track.codec == .avc and !reader.track.inband_parameter_sets and try compatibleApple(a, reader.track.avcc)) {
        plan.apple_surface_bytes = std.math.cast(usize, try video.apple.surfaceAdmissionBytes(reader.track.width, reader.track.height, selected.len)) orelse return error.ResourceLimitExceeded;
        if (plan.apple_surface_bytes > 512 * 1024 * 1024) return error.ResourceLimitExceeded;
    }
    for (selected) |index| {
        try control.check();
        const cfg = if (configurations) |session| try session.configAt(index) else static_config;
        const width = if (cfg) |value| value.width else reader.track.width;
        const height = if (cfg) |value| value.height else reader.track.height;
        try plan.picture(@intCast(width), @intCast(height), if (cfg) |value| try policySignal(reader.track, @intCast(width), @intCast(height), value.color) else try policy(reader.track, @intCast(width), @intCast(height)));
    }
    return plan;
}

fn selectWebm(a: std.mem.Allocator, index: video.webm_pictures.Index, sampling: Sampling, control: media.source.Control) ![]usize {
    if (sampling == .reference) return index.select();
    const metadata = try a.alloc(video.sampling.TimedPicture, index.pictures.len);
    defer a.free(metadata);
    for (index.pictures, metadata) |picture, *item| item.* = .{ .pts = picture.pts, .duration = picture.duration };
    return timestampSelect(a, metadata, 1_000_000_000, sampling, control);
}
/// A request-owned producer with two queued preparations. It never touches model
/// weights or a compute backend; start after admission and before the model gate.
/// The caller retains input, allocator, pool, Io and (for Metal) a process guard
/// until deinit has closed, joined and drained this owner.
pub const Prefetch = struct {
    const Job = union(enum) { header: []const usize, native: NativeProjectionJob, metal: ProjectionJob, cached: struct { slot: usize, ticket: CacheTicket } };
    a: std.mem.Allocator,
    io: std.Io,
    bytes: []const u8,
    packet_reader: ?*const media.mp4.Reader = null,
    reader_selection: ?*const ReaderSelection = null,
    control: Control,
    sampling: Sampling,
    pool: ?*media.admission.Pool,
    device: ?*anyopaque,
    cache: ?*FrameCache = null,
    clip_hash: [32]u8 = @splat(0),
    preparer: ?video.preparation.Metal = null,
    storage: [2]Job = undefined,
    queue: std.Io.Queue(Job) = undefined,
    thread: ?std.Thread = null,
    selected: ?[]usize = null,
    failure: ?anyerror = null, // Read only after joining the producer.
    aborted: std.atomic.Value(bool) = .init(false),
    published: std.atomic.Value(usize) = .init(0),

    pub fn start(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !*Prefetch {
        return startCached(a, io, device, bytes, control, sampling, pool, null);
    }
    pub fn startCached(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache) !*Prefetch {
        return startInput(a, io, device, bytes, null, control, sampling, pool, cache, null);
    }
    pub fn startReader(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !*Prefetch {
        return startReaderWithSelection(a, io, device, reader, control, sampling, pool, null);
    }
    pub fn startReaderWithSelection(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection) !*Prefetch {
        return startReaderCachedWithSelection(a, io, device, reader, control, sampling, pool, selection, null);
    }
    pub fn startReaderCachedWithSelection(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection, cache: ?*FrameCache) !*Prefetch {
        return startInput(a, io, device, try packetBytes(reader), reader, control, sampling, pool, cache, selection);
    }
    fn startInput(a: std.mem.Allocator, io: std.Io, device: ?*anyopaque, bytes: []const u8, reader: ?*const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache, selection: ?*const ReaderSelection) !*Prefetch {
        try control.check();
        const self = try a.create(Prefetch);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .io = io, .device = device, .bytes = bytes, .packet_reader = reader, .reader_selection = selection, .control = control, .sampling = sampling, .pool = pool, .cache = cache };
        if (cache != null and bytes.len <= 64 * 1024 * 1024) {
            if (reader) |input| {
                const adapter = control.imageWorkControl();
                self.clip_hash = try readerBinding(input, .reference, .{ .context = adapter.context, .check_fn = adapter.check_fn });
            } else std.crypto.hash.sha2.Sha256.hash(bytes, &self.clip_hash, .{});
        }
        self.queue = .init(&self.storage);
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        return self;
    }
    fn check(raw: ?*const anyopaque) !void {
        const self: *const Prefetch = @ptrCast(@alignCast(raw.?));
        try self.control.check();
        if (self.aborted.load(.acquire)) return error.Cancelled;
    }
    fn put(self: *Prefetch, job: Job) !void {
        try check(self);
        try self.queue.putOneUncancelable(self.io, job);
        _ = self.published.fetchAdd(1, .release);
    }
    fn run(self: *Prefetch) void {
        self.produce() catch |err| {
            self.failure = err;
        };
        self.queue.close(self.io);
    }
    fn join(self: *Prefetch) void {
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }
    fn discard(self: *Prefetch, job: *Job) void {
        switch (job.*) {
            .header => {},
            .native => |value| self.a.free(value.patches),
            .metal => |*value| value.prepared.deinit(),
            .cached => |value| self.cache.?.restore(value.ticket),
        }
    }
    pub fn deinit(self: *Prefetch) void {
        self.aborted.store(true, .release);
        self.queue.close(self.io);
        self.join();
        var job: [1]Job = undefined;
        while (true) {
            _ = self.queue.getUncancelable(self.io, &job, 1) catch break;
            self.discard(&job[0]);
        }
        self.releasePreparer();
        if (self.selected) |selected| self.a.free(selected);
        self.a.destroy(self);
    }
    fn releasePreparer(self: *Prefetch) void {
        // Called only after joining and fencing/draining every preparation. The
        // CoreVideo cache must survive imports held by queued/consumer jobs.
        if (self.preparer) |*preparer| preparer.deinit();
        self.preparer = null;
    }
    fn produce(self: *Prefetch) !void {
        const a = self.a;
        const bytes = self.bytes;
        const sampling = self.sampling;
        const pool = self.pool;
        if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "embedding-video", .admission_pool = pool, .control = .{ .context = self, .check_fn = check }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
        const webm = self.packet_reader == null and isWebm(bytes);
        var webm_reader: media.webm.Reader = undefined;
        var reader: media.mp4.Reader = undefined;
        var packet_input: ?PacketInput = null;
        if (webm) {
            webm_reader = try media.webm.Reader.init(a, &source, .{});
        } else if (self.packet_reader) |borrowed| {
            reader = borrowed.*;
            reader.input = &source;
        } else {
            packet_input = try PacketInput.init(a, &source, bytes);
            reader = packet_input.?.reader;
        }
        defer if (webm) webm_reader.deinit() else if (packet_input) |*input| input.deinit();
        var webm_index: ?video.webm_pictures.Index = if (webm) try video.webm_pictures.index(a, &webm_reader, .{ .decoder = .{ .control = source.control, .admission_pool = pool } }) else null;
        defer if (webm_index) |*index| index.deinit();
        const selected = if (webm_index) |index| try selectWebm(a, index, sampling, source.control) else try selectReader(a, &reader, sampling, source.control, self.reader_selection);
        self.selected = selected;
        const track = if (webm) try webmTrack(webm_reader.track) else reader.track;
        const configurations = if (webm) null else try parameterSession(a, &reader);
        defer if (configurations) |session| session.deinit();
        var signal = video.h264.Color{};
        if (!webm and reader.track.codec == .avc and !reader.track.inband_parameter_sets) {
            const cfg = try video.h264.configParse(a, reader.track.avcc);
            defer cfg.groups.deinit(a);
            signal = cfg.color;
        }
        if (self.device) |device| {
            self.preparer = try video.preparation.Metal.init(device);
            if (pool) |shared| try self.preparer.?.admit(shared, 16 * 1024 * 1024);
        }
        var capture = Capture{ .a = a, .cb = undefined, .track = track, .webm_track = if (webm) webm_reader.track else null, .pool = pool, .control = source.control, .output = &.{}, .frame_tokens = &.{}, .selected = selected, .signal = signal, .metal = if (self.preparer) |*value| value else null, .io = self.io, .prefetch = self };
        try self.put(.{ .header = selected });
        var misses: [32]usize = undefined;
        var miss_slots: [32]usize = undefined;
        var missing: usize = 0;
        for (selected, 0..) |id, slot| {
            if (std.mem.indexOfScalar(usize, selected[0..slot], id) != null) continue;
            if (self.cache) |cache| if (cache.take(projectionKey(self.reader_selection, self.clip_hash, selected, slot))) |ticket| {
                self.put(.{ .cached = .{ .slot = slot, .ticket = ticket } }) catch |err| {
                    cache.restore(ticket);
                    return err;
                };
                continue;
            };
            misses[missing] = id;
            miss_slots[missing] = slot;
            missing += 1;
        }
        if (missing == 0) return;
        capture.decode_slots = miss_slots[0..missing];
        if (webm) {
            try video.webm_pictures.walkIndexed(a, &webm_reader, .{ .decoder = .{ .control = source.control, .admission_pool = pool } }, webm_index.?, misses[0..missing], &capture, Capture.publishWebm);
        } else try decodeSelection(a, &reader, misses[0..missing], configurations, &capture);
    }

    /// Only the consumer runs model operations, while the producer continues
    /// decoding/preparing the next bounded pair. Returns independent output.
    pub fn encode(self: *Prefetch, cb: *const ComputeBackend) !Encoded {
        try self.control.check();
        const a = self.a;
        var header: [1]Job = undefined;
        _ = self.queue.getUncancelable(self.io, &header, 1) catch {
            self.join();
            return self.failure orelse error.InvalidTensorShape;
        };
        const selected = switch (header[0]) {
            .header => |value| value,
            else => {
                self.discard(&header[0]);
                return error.InvalidTensorShape;
            },
        };
        const frame_tokens = try a.alloc(usize, selected.len);
        errdefer a.free(frame_tokens);
        @memset(frame_tokens, 0);
        const output = try a.alloc(f32, if (cb.kind() == .metal) 0 else selected.len * 140 * 512);
        defer a.free(output);
        const resident = if (cb.kind() == .metal) try a.alloc(?CT, selected.len) else null;
        defer if (resident) |tensors| {
            for (tensors) |tensor| if (tensor) |value| cb.free(value);
            a.free(tensors);
        };
        if (resident) |tensors| @memset(tensors, null);
        const adapter = self.control.imageWorkControl();
        var capture = Capture{ .reader_selection = self.reader_selection, .a = a, .cb = cb, .track = undefined, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .output = output, .resident = resident, .frame_tokens = frame_tokens, .selected = selected, .io = self.io, .native_batching = cb.kind() == .native, .cache = self.cache, .clip_hash = self.clip_hash };
        defer capture.discardNativePending();
        var jobs: [2]Job = undefined;
        while (true) {
            const count = self.queue.getUncancelable(self.io, &jobs, 2) catch break;
            var owned: [2]bool = .{ true, true };
            defer for (jobs[0..count], 0..) |*job, i| if (owned[i]) {
                self.discard(job);
            };
            try self.control.check();
            if (jobs[0] == .metal and (count == 1 or jobs[1] == .metal)) {
                var gpu: [2]ProjectionJob = undefined;
                for (jobs[0..count], 0..) |job, i| {
                    if (job != .metal) return error.InvalidTensorShape;
                    gpu[i] = job.metal;
                }
                for (0..count) |i| owned[i] = false;
                try capture.projectGpuBatchNow(gpu[0..count]);
                for (gpu[0..count]) |job| for (selected, 0..) |index, slot| if (index == selected[job.slot]) {
                    frame_tokens[slot] = frame_tokens[job.slot];
                };
            } else {
                for (jobs[0..count], 0..) |job, i| {
                    switch (job) {
                        .native => |value| try capture.project(value.slot, value.patches, value.options),
                        .metal => |value| {
                            owned[i] = false;
                            try capture.projectGpuNow(value.slot, value.prepared, value.options);
                        },
                        .cached => |value| try capture.loadCached(value.slot, value.ticket),
                        .header => return error.InvalidTensorShape,
                    }
                }
            }
        }
        self.join();
        if (self.failure) |err| return err;
        self.releasePreparer();
        try capture.flushNativePending();
        try capture.finishDuplicates();
        if (resident) |tensors| {
            // Each unique packet owns exactly one projection. Presentation duplicates
            // refer to that projection and are appended in sampling order.
            const first = std.mem.indexOfScalar(usize, selected, selected[0]).?;
            var combined = try cb.ensureDeviceResidentOwned(try cb.primReshape(tensors[first] orelse return error.InvalidTensorShape, &.{ @intCast(frame_tokens[0]), 512 }));
            errdefer cb.free(combined);
            var rows = frame_tokens[0];
            for (selected[1..], 1..) |index, slot| {
                const owner = std.mem.indexOfScalar(usize, selected, index).?;
                if (frame_tokens[slot] == 0) return error.InvalidTensorShape;
                const next = try cb.primConcatPrim(combined, tensors[owner] orelse return error.InvalidTensorShape, 0, &.{ @intCast(rows), 512 }, &.{ @intCast(frame_tokens[slot]), 512 });
                cb.free(combined);
                combined = next;
                rows += frame_tokens[slot];
            }
            return .{ .embeddings = try a.alloc(f32, 0), .tensor = combined, .backend = cb, .frame_tokens = frame_tokens };
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
};

pub fn encode(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control) !Encoded {
    return encodeWithSampling(a, cb, bytes, control, .reference);
}
pub fn encodeWithSampling(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control, sampling: Sampling) !Encoded {
    return encodeWithBudget(a, cb, bytes, control, sampling, null);
}
pub fn encodeWithBudget(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !Encoded {
    return encodeCached(a, cb, bytes, control, sampling, pool, null);
}
pub fn encodeCached(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache) !Encoded {
    return encodeInput(a, cb, bytes, null, control, sampling, pool, cache, null);
}
pub fn encodeReader(a: std.mem.Allocator, cb: *const ComputeBackend, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool) !Encoded {
    return encodeReaderWithSelection(a, cb, reader, control, sampling, pool, null);
}
pub fn encodeReaderWithSelection(a: std.mem.Allocator, cb: *const ComputeBackend, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection) !Encoded {
    return encodeReaderCachedWithSelection(a, cb, reader, control, sampling, pool, selection, null);
}
pub fn encodeReaderCachedWithSelection(a: std.mem.Allocator, cb: *const ComputeBackend, reader: *const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, selection: ?*const ReaderSelection, cache: ?*FrameCache) !Encoded {
    return encodeInput(a, cb, try packetBytes(reader), reader, control, sampling, pool, cache, selection);
}
fn encodeInput(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, borrowed: ?*const media.mp4.Reader, control: Control, sampling: Sampling, pool: ?*media.admission.Pool, cache: ?*FrameCache, selection: ?*const ReaderSelection) !Encoded {
    if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "embedding-video", .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    const webm = borrowed == null and isWebm(bytes);
    var webm_reader: media.webm.Reader = undefined;
    var reader: media.mp4.Reader = undefined;
    var packet_input: ?PacketInput = null;
    if (webm) {
        webm_reader = try media.webm.Reader.init(a, &source, .{});
    } else if (borrowed) |input| {
        reader = input.*;
        reader.input = &source;
    } else {
        packet_input = try PacketInput.init(a, &source, bytes);
        reader = packet_input.?.reader;
    }
    defer if (webm) webm_reader.deinit() else if (packet_input) |*input| input.deinit();
    var webm_index: ?video.webm_pictures.Index = if (webm) try video.webm_pictures.index(a, &webm_reader, .{ .decoder = .{ .control = source.control, .admission_pool = pool } }) else null;
    defer if (webm_index) |*index| index.deinit();
    const selected = if (webm_index) |index| try selectWebm(a, index, sampling, source.control) else try selectReader(a, &reader, sampling, source.control, selection);
    defer a.free(selected);
    const track = if (webm) try webmTrack(webm_reader.track) else reader.track;
    const configurations = if (webm) null else try parameterSession(a, &reader);
    defer if (configurations) |session| session.deinit();
    const frame_tokens = try a.alloc(usize, selected.len);
    errdefer a.free(frame_tokens);
    @memset(frame_tokens, 0);
    const output = try a.alloc(f32, if (cb.kind() == .metal) 0 else selected.len * 140 * 512);
    defer a.free(output);
    var signal = video.h264.Color{};
    if (!webm and reader.track.codec == .avc and !reader.track.inband_parameter_sets) {
        const cfg = try video.h264.configParse(a, reader.track.avcc);
        defer cfg.groups.deinit(a);
        signal = cfg.color;
    }
    var preparer: ?video.preparation.Metal = null;
    if (comptime build_options.enable_metal) {
        if (cb.kind() == .metal) {
            preparer = try video.preparation.Metal.init(try MetalCompute.videoDevice(cb));
            errdefer preparer.?.deinit();
            if (pool) |shared| try preparer.?.admit(shared, 16 * 1024 * 1024);
        }
    }
    defer if (preparer) |*value| value.deinit();
    const resident = if (cb.kind() == .metal) try a.alloc(?CT, selected.len) else null;
    defer if (resident) |tensors| {
        for (tensors) |tensor| if (tensor) |value| cb.free(value);
        a.free(tensors);
    };
    if (resident) |tensors| @memset(tensors, null);
    var clip_hash: [32]u8 = @splat(0);
    if (cache != null) {
        if (borrowed) |input_reader| clip_hash = try readerBinding(input_reader, .reference, source.control) else std.crypto.hash.sha2.Sha256.hash(bytes, &clip_hash, .{});
    }
    var capture = Capture{ .reader_selection = selection, .cache = cache, .clip_hash = clip_hash, .pool = pool, .native_batching = cb.kind() == .native, .resident = resident, .signal = signal, .selected = selected, .io = control.io orelse cb.getIo(), .metal = if (preparer) |*value| value else null, .a = a, .cb = cb, .track = track, .webm_track = if (webm) webm_reader.track else null, .control = source.control, .output = output, .frame_tokens = frame_tokens };
    defer capture.discardNativePending();
    var jobs: [2]ProjectionJob = undefined;
    var scheduler = ProjectionScheduler{ .queue = .init(&jobs), .capture = &capture };
    var projection_thread: ?std.Thread = null;
    if (resident != null) {
        if (capture.io == null) return error.MissingVideoIo;
        capture.scheduler = &scheduler;
        projection_thread = try std.Thread.spawn(.{}, ProjectionScheduler.run, .{&scheduler});
    }
    defer if (projection_thread) |thread| {
        scheduler.aborted.store(true, .release);
        scheduler.queue.close(capture.io.?);
        thread.join();
    };
    var misses: [32]usize = undefined;
    var miss_slots: [32]usize = undefined;
    var missing: usize = 0;
    for (selected, 0..) |id, slot| {
        if (std.mem.indexOfScalar(usize, selected[0..slot], id) != null) continue;
        if (cache) |shared| if (shared.take(projectionKey(selection, clip_hash, selected, slot))) |ticket| {
            defer shared.restore(ticket);
            try capture.loadCached(slot, ticket);
            continue;
        };
        misses[missing] = id;
        miss_slots[missing] = slot;
        missing += 1;
    }
    capture.decode_slots = miss_slots[0..missing];
    const decoded = if (missing == 0) {} else if (webm)
        video.webm_pictures.walkIndexed(a, &webm_reader, .{ .decoder = .{ .control = source.control, .admission_pool = pool } }, webm_index.?, misses[0..missing], &capture, Capture.publishWebm)
    else
        decodeSelection(a, &reader, misses[0..missing], configurations, &capture);
    const decode_error: ?anyerror = if (decoded) |_| null else |err| err;
    if (decode_error != null) scheduler.aborted.store(true, .release);
    if (projection_thread) |thread| {
        scheduler.queue.close(capture.io.?);
        thread.join();
        projection_thread = null;
        if (scheduler.failure) |err| return err;
    }
    if (decode_error) |err| return err;
    try capture.flushNativePending();
    try capture.finishDuplicates();
    if (resident) |tensors| {
        // Each unique packet owns exactly one projection. Presentation duplicates
        // refer to that projection and are appended in sampling order.
        const first = std.mem.indexOfScalar(usize, selected, selected[0]).?;
        var combined = try cb.ensureDeviceResidentOwned(try cb.primReshape(tensors[first] orelse return error.InvalidTensorShape, &.{ @intCast(frame_tokens[0]), 512 }));
        errdefer cb.free(combined);
        var rows = frame_tokens[0];
        for (selected[1..], 1..) |index, slot| {
            const owner = std.mem.indexOfScalar(usize, selected, index).?;
            if (frame_tokens[slot] == 0) return error.InvalidTensorShape;
            const next = try cb.primConcatPrim(combined, tensors[owner] orelse return error.InvalidTensorShape, 0, &.{ @intCast(rows), 512 }, &.{ @intCast(frame_tokens[slot]), 512 });
            cb.free(combined);
            combined = next;
            rows += frame_tokens[slot];
        }
        return .{ .embeddings = try a.alloc(f32, 0), .tensor = combined, .backend = cb, .frame_tokens = frame_tokens };
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
fn decodeSelection(a: std.mem.Allocator, reader: *media.mp4.Reader, selected: []const usize, configurations: ?*video.h264_dynamic.Session, capture: *Capture) !void {
    const control = reader.input.control;
    if (reader.track.codec == .mjpeg) {
        for (selected) |index| {
            const slot = std.mem.indexOfScalar(usize, capture.selected, index) orelse return error.InvalidPacketIndex;
            try control.check();
            if (capture.copyDuplicate(slot)) continue;
            var frame = try video.mjpeg.decodeFrame(a, reader, index, .{});
            defer frame.deinit();
            var options = try policy(reader.track, frame.width, frame.height);
            options.admission_pool = capture.pool;
            if (capture.metal) |metal| {
                const prepared = try metal.submitRgba(a, frame.rgba, frame.width, frame.height, options, control);
                try capture.projectGpu(slot, prepared, options);
                continue;
            }
            const patches = try video.preparation.referenceRgba(a, frame.rgba, frame.width, frame.height, options, control);
            defer a.free(patches);
            try capture.project(slot, patches, options);
        }
    } else if (capture.metal != null and !reader.track.inband_parameter_sets and try compatibleApple(a, reader.track.avcc)) {
        // VideoToolbox's bounded decoder surface working set shares the same
        // prepaid device budget as queued preparation commands.
        const surface_bytes = try video.apple.surfaceAdmissionBytes(reader.track.width, reader.track.height, selected.len);
        var surfaces = if (capture.pool) |pool| try pool.acquire(.{ .device_bytes = surface_bytes }) else media.admission.Token{};
        defer surfaces.deinit();
        var batch = try video.apple.decodeTo(a, reader, selected, .{ .require_hardware = false, .seek_mode = .verified_idr }, .{ .context = capture, .accept = Capture.publishApple });
        defer batch.deinit();
    } else {
        // Decode references once for the whole selection, never once per frame.
        if (configurations) |session| _ = try session.decodeSelected(selected, capture, Capture.publishDecoded) else _ = try video.h264_sparse.decodeSelected(a, reader, selected, .{ .max_total_packets = 8192 }, capture, Capture.publishDecoded);
    }
}

fn compatibleApple(a: std.mem.Allocator, avcc: []const u8) !bool {
    video.avc.validateConfig(avcc) catch return false;
    const cfg = video.h264.configParse(a, avcc) catch |err| return if (err == error.OutOfMemory) err else false;
    defer cfg.groups.deinit(a);
    return cfg.frame_only and cfg.profile != 88 and !cfg.separate_planes;
}
const ProjectionJob = struct { slot: usize, prepared: video.preparation.Prepared, options: video.preparation.Options };

/// Two queued preparations plus one executing projection. The producer owns
/// decoding/preparation; this worker exclusively owns model backend operations.
/// Closing on failure wakes blocked producers; every queued GPU lease is drained.
const ProjectionScheduler = struct {
    queue: std.Io.Queue(ProjectionJob),
    capture: *Capture,
    aborted: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,
    fn run(self: *ProjectionScheduler) void {
        const capture = self.capture;
        const io = capture.io.?;
        var jobs: [2]ProjectionJob = undefined;
        while (true) {
            // A closed queue returns a final partial batch. Decode can continue
            // filling the bounded queue while this worker runs the previous pair.
            const count = self.queue.getUncancelable(io, &jobs, 2) catch break;
            if (self.aborted.load(.acquire) or self.failure != null) {
                for (jobs[0..count]) |*job| job.prepared.deinit();
                continue;
            }
            capture.projectGpuBatchNow(jobs[0..count]) catch |err| {
                self.failure = err;
                self.queue.close(io);
                continue;
            };
            for (jobs[0..count]) |job| {
                for (capture.selected, 0..) |index, slot| if (index == capture.selected[job.slot]) {
                    capture.frame_tokens[slot] = capture.frame_tokens[job.slot];
                };
            }
        }
    }
};

const NativeProjectionJob = struct { slot: usize, patches: []f32, options: video.preparation.Options };

const Capture = struct {
    a: std.mem.Allocator,
    cb: *const ComputeBackend,
    cache: ?*FrameCache = null,
    clip_hash: [32]u8 = @splat(0),
    reader_selection: ?*const ReaderSelection = null,
    decode_slots: ?[]const usize = null,
    track: media.mp4.Track,
    webm_track: ?media.webm.Track = null,
    pool: ?*media.admission.Pool = null,
    control: media.source.Control,
    output: []f32,
    resident: ?[]?CT = null,
    frame_tokens: []usize,
    selected: []const usize,
    signal: video.h264.Color = .{},
    metal: ?*video.preparation.Metal = null,
    io: ?std.Io = null,
    scheduler: ?*ProjectionScheduler = null,
    prefetch: ?*Prefetch = null,
    native_batching: bool = false,
    native_pending: ?NativeProjectionJob = null,
    claimed: [32]bool = @splat(false),
    fn loadCached(self: *Capture, slot: usize, ticket: CacheTicket) !void {
        try self.control.check();
        const tokens = ticket.entry.tokens;
        if (self.resident) |tensors| tensors[slot] = try self.cache.?.import(self.cb, ticket) else {
            switch (ticket.entry.value) {
                .cpu => |values| @memcpy(self.output[slot * 140 * 512 ..][0..values.len], values),
                .device => return error.UnsupportedVideoBackend,
            }
        }
        for (self.selected, 0..) |id, duplicate| if (id == self.selected[slot]) {
            if (self.resident == null and duplicate != slot) @memcpy(self.output[duplicate * 140 * 512 ..][0 .. tokens * 512], self.output[slot * 140 * 512 ..][0 .. tokens * 512]);
            self.frame_tokens[duplicate] = tokens;
            self.claimed[duplicate] = true;
        };
    }
    fn storeDevice(self: *Capture, slot: usize, tensor: CT, tokens: usize) void {
        if (self.cache) |cache| cache.storeDevice(projectionKey(self.reader_selection, self.clip_hash, self.selected, slot), tensor, tokens);
    }
    fn finishDuplicates(self: *Capture) !void {
        for (self.selected, 0..) |id, slot| {
            const first = std.mem.indexOfScalar(usize, self.selected, id).?;
            const tokens = self.frame_tokens[first];
            if (tokens == 0 or tokens > 140) return error.InvalidTensorShape;
            if (self.resident == null and slot != first) @memcpy(self.output[slot * 140 * 512 ..][0 .. tokens * 512], self.output[first * 140 * 512 ..][0 .. tokens * 512]);
            self.frame_tokens[slot] = tokens;
        }
    }
    fn copyDuplicate(self: *Capture, slot: usize) bool {
        const first = std.mem.indexOfScalar(usize, self.selected, self.selected[slot]).?;
        if (self.prefetch != null or self.scheduler != null or self.native_batching) return self.claimed[first];
        if (self.frame_tokens[first] == 0) return false;
        const n = self.frame_tokens[first] * 512;
        if (self.resident == null and first != slot) @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
        self.frame_tokens[slot] = self.frame_tokens[first];
        return true;
    }
    fn discardNativePending(self: *Capture) void {
        if (self.native_pending) |pending| self.a.free(pending.patches);
        self.native_pending = null;
    }
    fn commitNative(self: *Capture, slot: usize, embeddings: []const f32, tokens: usize) !void {
        try self.save(slot, .{ .embeddings = embeddings, .tokens = tokens });
        for (self.selected, 0..) |index, duplicate| if (index == self.selected[slot] and duplicate != slot) {
            @memcpy(self.output[duplicate * 140 * 512 ..][0..embeddings.len], embeddings);
            self.frame_tokens[duplicate] = tokens;
        };
    }
    fn flushNativePending(self: *Capture) !void {
        const pending = self.native_pending orelse return;
        self.native_pending = null;
        defer self.a.free(pending.patches);
        try self.control.check();
        const encoded = try projector.encodeEmbeddingGemma2VideoPatches(self.cb, self.a, pending.patches, pending.options.width, pending.options.height);
        defer self.a.free(encoded.embeddings);
        try self.commitNative(pending.slot, encoded.embeddings, encoded.tokens);
    }
    fn projectNativeBatch(self: *Capture, slot: usize, patches: []const f32, options: video.preparation.Options) !void {
        if (self.native_pending) |pending| {
            if (pending.options.width != options.width or pending.options.height != options.height) {
                try self.flushNativePending();
            } else {
                self.native_pending = null;
                defer self.a.free(pending.patches);
                if (pending.patches.len != patches.len) return error.InvalidTensorShape;
                const combined = try self.a.alloc(f32, patches.len * 2);
                defer self.a.free(combined);
                @memcpy(combined[0..patches.len], pending.patches);
                @memcpy(combined[patches.len..], patches);
                const input = try self.cb.fromFloat32Shape(combined, &.{ @intCast(combined.len / 768), 768 });
                defer self.cb.free(input);
                const encoded = try projector.encodeEmbeddingGemma2VideoBatchTensorResident(self.cb, self.a, input, options.width, options.height, 2);
                defer self.cb.free(encoded.tensor);
                const embeddings = try self.cb.toFloat32(encoded.tensor, self.a);
                defer self.a.free(embeddings);
                if (encoded.tokens % 2 != 0 or embeddings.len != encoded.tokens * 512) return error.InvalidTensorShape;
                const per_frame = encoded.tokens / 2;
                try self.commitNative(pending.slot, embeddings[0 .. per_frame * 512], per_frame);
                try self.commitNative(slot, embeddings[per_frame * 512 ..], per_frame);
                self.claimed[slot] = true;
                return;
            }
        }
        self.native_pending = .{ .slot = slot, .patches = try self.a.dupe(f32, patches), .options = options };
        self.claimed[slot] = true;
    }
    fn project(self: *Capture, slot: usize, patches: []const f32, options: video.preparation.Options) !void {
        try self.control.check();
        if (self.prefetch) |producer| {
            const owned = try self.a.dupe(f32, patches);
            errdefer self.a.free(owned);
            try producer.put(.{ .native = .{ .slot = slot, .patches = owned, .options = options } });
            self.claimed[slot] = true;
            return;
        }
        if (self.native_batching) return self.projectNativeBatch(slot, patches, options);
        if (self.resident) |tensors| {
            const input = try self.cb.fromFloat32Shape(patches, &.{ @intCast(patches.len / 768), 768 });
            defer self.cb.free(input);
            const encoded = try projector.encodeEmbeddingGemma2VideoTensorResident(self.cb, self.a, input, options.width, options.height);
            tensors[slot] = encoded.tensor;
            self.frame_tokens[slot] = encoded.tokens;
            self.storeDevice(slot, encoded.tensor, encoded.tokens);
            return;
        }
        const encoded = try projector.encodeEmbeddingGemma2VideoPatches(self.cb, self.a, patches, options.width, options.height);
        defer self.a.free(encoded.embeddings);
        try self.save(slot, encoded);
    }
    fn save(self: *Capture, slot: usize, encoded: anytype) !void {
        if (encoded.tokens == 0 or encoded.tokens > 140 or encoded.embeddings.len != encoded.tokens * 512) return error.InvalidTensorShape;
        if (self.cache) |cache| cache.storeCpu(projectionKey(self.reader_selection, self.clip_hash, self.selected, slot), encoded.embeddings, encoded.tokens);
        @memcpy(self.output[slot * 140 * 512 ..][0..encoded.embeddings.len], encoded.embeddings);
        self.frame_tokens[slot] = encoded.tokens;
    }
    fn projectGpu(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options) !void {
        if (self.prefetch) |producer| {
            var prepared = value;
            errdefer prepared.deinit();
            try producer.put(.{ .metal = .{ .slot = slot, .prepared = prepared, .options = options } });
            self.claimed[slot] = true;
            return;
        }
        if (self.scheduler) |scheduler| {
            const owner = std.mem.indexOfScalar(usize, self.selected, self.selected[slot]).?;
            var prepared = value;
            errdefer prepared.deinit();
            try self.control.check();
            try scheduler.queue.putOneUncancelable(self.io.?, .{ .slot = owner, .prepared = prepared, .options = options });
            self.claimed[owner] = true;
            return;
        }
        return self.projectGpuNow(slot, value, options);
    }
    /// Consumes every preparation even when imports or projection fail.
    fn projectGpuBatchNow(self: *Capture, jobs: []ProjectionJob) !void {
        if (jobs.len == 1) return self.projectGpuNow(jobs[0].slot, jobs[0].prepared, jobs[0].options);
        if (jobs.len != 2) return error.InvalidTensorShape;
        if (jobs[0].options.width != jobs[1].options.width or jobs[0].options.height != jobs[1].options.height) {
            self.projectGpuNow(jobs[0].slot, jobs[0].prepared, jobs[0].options) catch |err| {
                jobs[1].prepared.deinit();
                return err;
            };
            return self.projectGpuNow(jobs[1].slot, jobs[1].prepared, jobs[1].options);
        }
        var live = [_]bool{ true, true };
        defer for (jobs, 0..) |*job, i| {
            if (live[i]) job.prepared.deinit();
        };
        if (comptime build_options.enable_metal) {
            const io = self.io orelse return error.MissingVideoIo;
            var inputs: [2]CT = undefined;
            var imported: usize = 0;
            defer for (inputs[0..imported]) |input| self.cb.free(input);
            for (jobs, 0..) |*job, i| {
                try self.control.check();
                try job.prepared.wait(io, self.control);
                inputs[i] = try MetalCompute.importVideoBuffer(self.cb, try job.prepared.buffer(), job.prepared.geometry.values());
                imported += 1;
                job.prepared.deinit();
                live[i] = false;
            }
            const options = jobs[0].options;
            const rows = @as(usize, options.width) * options.height / 256;
            const input_shape = [_]i64{ @intCast(rows), 768 };
            const input = try self.cb.primConcatPrim(inputs[0], inputs[1], 0, &input_shape, &input_shape);
            defer self.cb.free(input);
            const encoded = try projector.encodeEmbeddingGemma2VideoBatchTensorResident(self.cb, self.a, input, options.width, options.height, 2);
            defer self.cb.free(encoded.tensor);
            if (encoded.tokens % 2 != 0) return error.InvalidTensorShape;
            const per_frame = encoded.tokens / 2;
            const shape = [_]i64{ @intCast(encoded.tokens), 512 };
            // Slice both before publishing either: an allocation failure cannot
            // leave one slot with a partially committed batched result.
            var outputs: [2]CT = undefined;
            var sliced: usize = 0;
            errdefer for (outputs[0..sliced]) |output| self.cb.free(output);
            for (0..2) |i| {
                outputs[i] = try self.cb.primSlice(encoded.tensor, &.{ @intCast(i * per_frame), 0 }, &.{ @intCast((i + 1) * per_frame), 512 }, &.{ 1, 1 }, &shape);
                sliced += 1;
            }
            const tensors = self.resident orelse return error.UnsupportedVideoBackend;
            for (jobs, 0..) |job, i| {
                tensors[job.slot] = outputs[i];
                self.frame_tokens[job.slot] = per_frame;
                self.storeDevice(job.slot, outputs[i], per_frame);
            }
        } else return error.UnsupportedVideoBackend;
    }
    fn projectGpuNow(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options) !void {
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
            const encoded = try projector.encodeEmbeddingGemma2VideoTensorResident(self.cb, self.a, input, options.width, options.height);
            if (self.resident) |tensors| {
                tensors[slot] = encoded.tensor;
                self.frame_tokens[slot] = encoded.tokens;
                self.storeDevice(slot, encoded.tensor, encoded.tokens);
            } else {
                defer self.cb.free(encoded.tensor);
                const embeddings = try self.cb.toFloat32(encoded.tensor, self.a);
                defer self.a.free(embeddings);
                try self.save(slot, .{ .embeddings = embeddings, .tokens = encoded.tokens });
            }
        } else return error.UnsupportedVideoBackend;
    }
    fn validateRange(self: *const Capture, full: bool) !void {
        const c = self.track.color_info;
        if (c.len >= 11 and std.mem.eql(u8, c[0..4], "nclx") and (c[10] & 128 != 0) != full) return error.UnsupportedVideoColor;
    }
    fn publishWebm(raw: *anyopaque, id: usize, frame: *const video.codecs.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        const slot = std.mem.indexOfScalar(usize, self.selected, id) orelse return error.InvalidPacketIndex;
        if (self.copyDuplicate(slot)) return;
        const track = self.webm_track orelse return error.InvalidSamplingMetadata;
        const host = try webmHost(track, frame);
        const color = frame.color;
        const signal = video.h264.Color{
            .primaries = if (color.primaries == 2) null else color.primaries,
            .transfer = if (color.transfer == 2) null else color.transfer,
            .matrix = if (color.matrix == 2 or track.codec == .vp8) null else color.matrix,
        };
        var options = try webmPolicy(track, frame.width, frame.height, signal);
        if (options.tone_mapping == .none) options.rgb_rounding = video.preparation.softwareSdrRounding(frame.width, frame.chroma_format, frame.bit_depth);
        options.admission_pool = self.pool;
        if (self.metal) |metal| {
            const prepared = try metal.submitHost(self.a, host, options, self.control);
            return self.projectGpu(slot, prepared, options);
        }
        const patches = try video.preparation.referenceHost(self.a, host, options, self.control);
        defer self.a.free(patches);
        try self.project(slot, patches, options);
    }
    fn publishApple(raw: *anyopaque, frame: *const video.apple.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        try self.validateRange(frame.surface.format.fullRange());
        var options = try policySignal(self.track, frame.surface.width, frame.surface.height, self.signal);
        options.admission_pool = self.pool;
        // Duplicate logical samples must receive the same projection.
        const first = std.mem.indexOfScalar(usize, self.selected, frame.decode_index) orelse return error.InvalidPacketIndex;
        if (self.copyDuplicate(first)) return;
        const prepared = try self.metal.?.submit(self.a, &frame.surface, options, self.control);
        try self.projectGpu(first, prepared, options);
        if (self.prefetch != null or self.scheduler != null) return;
        const n = self.frame_tokens[first] * 512;
        for (self.selected, 0..) |index, slot| if (index == frame.decode_index and slot != first) {
            if (self.resident == null) @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
            self.frame_tokens[slot] = self.frame_tokens[first];
        };
    }
    fn publishDecoded(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        return publish(raw, if (self.decode_slots) |slots| slots[slot] else slot, frame);
    }
    fn publish(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        if (self.copyDuplicate(slot)) return;
        try self.validateRange(frame.full_range);
        var options = try policySignal(self.track, frame.width, frame.height, frame.color);
        options.admission_pool = self.pool;
        if (self.metal) |metal| {
            const prepared = try metal.submitHost(self.a, frame.host(), options, self.control);
            return self.projectGpu(slot, prepared, options);
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
    _ = try policy(synthetic.track, 1920, 1080);
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

test "embeddinggemma2 model VFR qualification preserves pinned average FPS indices and compressed pixels" {
    const a = std.testing.allocator;
    var original_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "cfr" };
    var original = try media.mp4.Reader.init(a, &original_source, .{});
    defer original.deinit();
    var vfr_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg-vfr.mov") }, .identity = "vfr" };
    var vfr = try media.mp4.Reader.init(a, &vfr_source, .{});
    defer vfr.deinit();
    const indexes = try select(a, &vfr);
    defer a.free(indexes);
    try std.testing.expectEqualSlices(usize, &.{ 0, 4 }, indexes);
    try std.testing.expectEqual(@as(i64, 4096), vfr.packets[4].pts);
    try std.testing.expectEqual(@as(u32, 16384), vfr.track.timescale);
    try std.testing.expect(vfr.packets[0].duration != vfr.packets[4].duration);
    for (original.packets, 0..) |_, index| {
        var cfr_packet = try original.readPacket(index);
        defer cfr_packet.deinit();
        var vfr_packet = try vfr.readPacket(index);
        defer vfr_packet.deinit();
        try std.testing.expectEqualSlices(u8, cfr_packet.bytes, vfr_packet.bytes);
    }
    // Orthogonal display metadata now drives rotation and model target aspect.
    var track = vfr.track;
    track.display_matrix = .{ 0, 65536, 0, -65536, 0, 0, 0, 0, 1073741824 };
    try std.testing.expectEqual(video.preparation.Rotation.clockwise90, (try policy(track, 640, 480)).rotation);
    track = vfr.track;
    track.pixel_aspect = .{ .horizontal = 2, .vertical = 1 };
    const aspect = try policy(track, 640, 480);
    try std.testing.expect(aspect.width > aspect.height * 2);
    track = vfr.track;
    track.color_info = "nclx\x00\x01\x00\x10\x00\x06\x80";
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}

test "embeddinggemma2 video HDR rendering requires consistent gamut and transfer" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "HDR policy" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var track = reader.track;
    track.codec = .avc;
    track.color_info = "nclx\x00\x09\x00\x10\x00\x09\x00";
    const pq = try policySignal(track, 640, 480, .{ .matrix = 9, .transfer = 16, .primaries = 9 });
    try std.testing.expectEqual(video.preparation.Matrix.bt2020_ncl, pq.matrix);
    try std.testing.expectEqual(video.preparation.ToneMapping.pq_reinhard_100, pq.tone_mapping);
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .transfer = 18 }));
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .primaries = 1 }));
    track.color_info = "nclx\x00\x09\x00\x12\x00\x09\x00";
    try std.testing.expectEqual(video.preparation.ToneMapping.hlg_reinhard_100, (try policy(track, 640, 480)).tone_mapping);
    track.color_info = "nclx\x00\x02\x00\x10\x00\x09\x00";
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}

test "embeddinggemma2 video WebM inspection uses decoded visible picture sampling" {
    const a = std.testing.allocator;
    inline for (.{ "vp8.webm", "vp9.webm", "av1.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = name };
        var reader = try media.webm.Reader.init(a, &source, .{});
        defer reader.deinit();
        var pictures = try video.webm_pictures.index(a, &reader, .{});
        defer pictures.deinit();
        const selected = try pictures.select();
        defer a.free(selected);
        var expected: u64 = 0;
        for (selected) |id| expected += @as(u64, pictures.pictures[id].width) * pictures.pictures[id].height;
        try std.testing.expectEqual(expected, try inspectPixels(bytes, .{ .io = std.testing.io }));
        try std.testing.expectError(error.Timeout, inspectPixels(bytes, .{ .deadline_ns = 0 }));
    }
}

test "embeddinggemma2 video logical field partition selection and timestamp windows" {
    const a = std.testing.allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-jm-partitions" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name ++ ".mp4");
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = bytes } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        var logical = try video.h264_pictures.index(a, &reader, .{});
        defer logical.deinit();
        const expected = try logical.select();
        defer a.free(expected);
        const selected = try select(a, &reader);
        defer a.free(selected);
        try std.testing.expectEqualSlices(usize, expected, selected);
        try std.testing.expect(try inspectPixels(bytes, .{}) > 0);
        const native = try selectWithSampling(a, &reader, .{ .timestamps = .{ .max_frames = 1 } });
        defer a.free(native);
        try std.testing.expectEqual(@as(usize, 1), native.len);
        try std.testing.expect(try inspectPixelsWithSampling(bytes, .{}, .{ .timestamps = .{ .max_frames = 1 } }) > 0);
    }
}

test "embeddinggemma2 video admission rejects all container paths before model execution" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "vp8.webm", "vp9.webm", "av1.webm", "closed.ts", "h264-paff-adaptive-packets-8-1.mp4" }) |name| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
        var unopened: ComputeBackend = undefined;
        try std.testing.expectError(error.SharedAdmissionExceeded, encodeWithBudget(a, &unopened, @embedFile("../testdata/video/" ++ name), .{}, .reference, &pool));
        try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    }
}

test "embeddinggemma2 video preflight bounds temporary owners and plans geometry instead of maximum slots" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var denied = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, inspectPlan(a, bytes, .{}, .reference, &denied, true));
    try std.testing.expectEqual(media.admission.Resources{}, denied.snapshot());
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    const plan = try inspectPlan(a, bytes, .{}, .{ .timestamps = .{ .max_frames = 1 } }, &pool, true);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    try std.testing.expectEqual(@as(u64, 1), plan.commands);
    try std.testing.expect(plan.preparation_bytes > 0);
    try std.testing.expectEqual(@as(usize, 0), plan.apple_surface_bytes);
    try std.testing.expect(try plan.deviceBytes() < 64 * 1024 * 1024);
    var src = media.source.Source{ .allocator = a, .identity = "preflight-bound", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &src, .{});
    defer reader.deinit();
    const options = try policy(reader.track, reader.track.width, reader.track.height);
    const geometry = try video.preparation.geometry(reader.track.width, reader.track.height, options);
    try std.testing.expect(plan.preparation_bytes >= geometry.scratch_bytes + @as(usize, reader.track.width) * reader.track.height * 4);
    try std.testing.expectEqual(@as(u64, 1), preparationSlots(&.{ 2, 2, 2 }));
    try std.testing.expectEqual(@as(u64, 5), preparationSlots(&.{ 0, 1, 2, 3, 4, 5 }));
}

fn preflightAllocationRun(backing: std.mem.Allocator) !void {
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const a = no_resize.allocator();
    inline for (.{ @embedFile("../testdata/video/mjpeg.mov"), @embedFile("../testdata/video/vp9.webm"), @embedFile("../testdata/video/bframes.ts"), @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4") }) |bytes| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
        const plan = try inspectPlan(a, bytes, .{}, .reference, &pool, true);
        try std.testing.expect(plan.pixels > 0);
    }
}
test "embeddinggemma2 video preflight allocation failures release metadata decoder and admission owners" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, preflightAllocationRun, .{});
}

test "embeddinggemma2 video producer prepares before model gate and drains an abandoned full queue" {
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.testing.allocator, .limit = 512 * 1024 * 1024 };
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    const producer = try Prefetch.start(bounded.allocator(), std.testing.io, null, @embedFile("../testdata/video/mjpeg.mov"), .{}, .reference, &pool);
    var live = true;
    defer if (live) producer.deinit();
    // No compute backend or model lock exists. The producer publishes metadata
    // and one prepared frame, then backpressure stops it at the two-slot queue.
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (producer.published.load(.acquire) < 2) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 2), producer.published.load(.acquire));
    producer.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video producer failure preserves error and releases partial owners" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    const producer = try Prefetch.start(a, std.testing.io, null, @embedFile("../testdata/video/vp9.webm"), .{}, .reference, &pool);
    defer producer.deinit();
    var unopened: ComputeBackend = undefined;
    try std.testing.expectError(error.SharedAdmissionExceeded, producer.encode(&unopened));
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video producer allocation failure drains metadata and prepared patches" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            // Producer ArrayList resize depends on heap placement. Exercise
            // every allocation fallback deterministically on both hosts.
            var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
            const a = no_resize.allocator();
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
            var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = 512 * 1024 * 1024 };
            defer std.debug.assert(bounded.live == 0);
            const producer = try Prefetch.start(bounded.allocator(), std.testing.io, null, @embedFile("../testdata/video/mjpeg.mov"), .{}, .reference, &pool);
            defer producer.deinit();
            var jobs: [1]Prefetch.Job = undefined;
            while (true) {
                _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
                producer.discard(&jobs[0]);
            }
            producer.join();
            if (producer.failure) |err| return err;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "embeddinggemma2 video cancellation before consumer entry joins and drains producer" {
    const Cancel = struct {
        value: std.atomic.Value(bool) = .init(false),
        fn cancelled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.value.load(.acquire);
        }
    };
    var token = Cancel{};
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.testing.allocator, .limit = 512 * 1024 * 1024 };
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    const producer = try Prefetch.start(bounded.allocator(), std.testing.io, null, @embedFile("../testdata/video/mjpeg.mov"), .{ .cancellation = .{ .ptr = &token, .is_cancelled_fn = Cancel.cancelled } }, .reference, &pool);
    var live = true;
    defer if (live) producer.deinit();
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (producer.published.load(.acquire) < 2) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    token.value.store(true, .release);
    var unopened: ComputeBackend = undefined;
    try std.testing.expectError(error.Cancelled, producer.encode(&unopened));
    producer.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}

test "embeddinggemma2 video WebM presentation and container colour reach model preparation" {
    var track = media.webm.Track{ .number = 1, .codec = .vp9, .width = 26, .height = 16, .presentation = .{ .crop = .{ .left = 1, .top = 1, .right = 3, .bottom = 2 }, .display_width = 16, .display_height = 9, .display_unit = 3, .roll = 90 }, .colour = .{ .matrix = 1, .primaries = 1, .transfer = 1, .range = 1, .bits_per_channel = 8 } };
    const options = try webmPolicy(track, 26, 16, .{ .matrix = 1 });
    try std.testing.expectEqual(video.preparation.Rotation.clockwise270, options.rotation);
    try std.testing.expectEqual(track.presentation.crop, options.crop);
    try std.testing.expectEqual(video.preparation.Matrix.bt709, options.matrix);
    const g = projector.embeddingGemma2VideoGeometry(13, 23);
    try std.testing.expectEqual(g.width, options.width);
    try std.testing.expectEqual(g.height, options.height);
    try std.testing.expectError(error.UnsupportedVideoColor, webmPolicy(track, 26, 16, .{ .matrix = 6 }));
    track.colour = .{ .matrix = 9, .primaries = 9, .transfer = 16 };
    try std.testing.expectEqual(video.preparation.ToneMapping.pq_reinhard_100, (try webmPolicy(track, 26, 16, .{})).tone_mapping);
    var frame = video.codecs.Frame{ .allocator = std.testing.allocator, .bytes = undefined, .width = 26, .height = 16, .bit_depth = 8, .chroma_format = 1, .color = .{}, .pts_ns = 0, .duration_ns = null, .decode_high_water = 0 };
    var pixels: [26 * 16 * 3 / 2]u8 = @splat(128);
    frame.bytes = &pixels;
    track.colour = .{ .range = 2 };
    try std.testing.expectError(error.UnsupportedVideoColor, webmHost(track, &frame));
    track.codec = .vp8;
    try std.testing.expect((try webmHost(track, &frame)).format.fullRange());
    track.colour.bits_per_channel = 10;
    try std.testing.expectError(error.UnsupportedVideoColor, webmHost(track, &frame));
    track.colour = .{ .subsampling_h = 0 };
    try std.testing.expectError(error.UnsupportedVideoColor, webmHost(track, &frame));
}

test "embeddinggemma2 video native TS sampling and preflight preserve MP4 pictures" {
    const a = std.testing.allocator;
    inline for (.{ "closed", "bframes" }) |name| {
        const ts = @embedFile("../testdata/video/" ++ name ++ ".ts");
        const mp4 = @embedFile("../testdata/video/" ++ name ++ ".mp4");
        var ts_source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = ts } };
        var mp4_source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = mp4 } };
        var normalized = try PacketInput.init(a, &ts_source, ts);
        defer normalized.deinit();
        var original = try PacketInput.init(a, &mp4_source, mp4);
        defer original.deinit();
        const selected = try select(a, &normalized.reader);
        defer a.free(selected);
        const expected = try select(a, &original.reader);
        defer a.free(expected);
        try std.testing.expectEqualSlices(usize, expected, selected);
        const ts_plan = try inspectPlan(a, ts, .{}, .reference, null, false);
        const mp4_plan = try inspectPlan(a, mp4, .{}, .reference, null, false);
        try std.testing.expectEqualDeep(mp4_plan, ts_plan);
        try std.testing.expectError(error.Timeout, inspectPlan(a, ts, .{ .deadline_ns = 0 }, .reference, null, false));
    }
}

test "embeddinggemma2 video TS producer preserves prepared MP4 patches and drains owners" {
    const CapturePatches = struct {
        fn run(bytes: []const u8) ![][32]u8 {
            const a = std.testing.allocator;
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
            defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
            const producer = try Prefetch.start(a, std.testing.io, null, bytes, .{}, .reference, &pool);
            defer producer.deinit();
            var hashes: std.ArrayList([32]u8) = .empty;
            defer hashes.deinit(a);
            var jobs: [1]Prefetch.Job = undefined;
            while (true) {
                _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
                defer producer.discard(&jobs[0]);
                switch (jobs[0]) {
                    .header => {},
                    .native => |job| {
                        var hash: [32]u8 = undefined;
                        std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(job.patches), &hash, .{});
                        try hashes.append(a, hash);
                    },
                    .metal, .cached => return error.TestUnexpectedResult,
                }
            }
            producer.join();
            if (producer.failure) |err| return err;
            return hashes.toOwnedSlice(a);
        }
    };
    inline for (.{ "closed", "bframes" }) |name| {
        const original = try CapturePatches.run(@embedFile("../testdata/video/" ++ name ++ ".mp4"));
        defer std.testing.allocator.free(original);
        inline for (.{ ".ts", "-split.ts" }) |suffix| {
            const normalized = try CapturePatches.run(@embedFile("../testdata/video/" ++ name ++ suffix));
            defer std.testing.allocator.free(normalized);
            try std.testing.expect(original.len > 0);
            try std.testing.expectEqualSlices([32]u8, original, normalized);
        }
    }
}

test "embeddinggemma2 video cache producer skips fully cached decode and preparation for every container" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    inline for (.{ "mjpeg.mov", "vp8.webm", "vp9.webm", "av1.webm", "closed.ts", "h264-paff-adaptive-packets-8-1.mp4" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        const sampling = Sampling{ .timestamps = .{ .max_frames = 3 } };
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = bytes } };
        const selected = if (isWebm(bytes)) blk: {
            var reader = try media.webm.Reader.init(a, &source, .{});
            defer reader.deinit();
            var index = try video.webm_pictures.index(a, &reader, .{});
            defer index.deinit();
            break :blk try selectWebm(a, index, sampling, .{});
        } else blk: {
            var input = try PacketInput.init(a, &source, bytes);
            defer input.deinit();
            break :blk try selectWithSampling(a, &input.reader, sampling);
        };
        defer a.free(selected);
        var cache = FrameCache{ .allocator = a, .session = session };
        defer cache.deinit();
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        const values: [512]f32 = @splat(0.125);
        for (selected) |id| cache.storeCpu(.{ .clip = hash, .picture = id }, &values, 1);
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        const producer = try Prefetch.startCached(a, std.testing.io, null, bytes, .{}, sampling, &pool, &cache);
        var live = true;
        defer if (live) producer.deinit();
        var hits: usize = 0;
        var jobs: [1]Prefetch.Job = undefined;
        while (true) {
            _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
            defer producer.discard(&jobs[0]);
            switch (jobs[0]) {
                .header => |ids| try std.testing.expectEqualSlices(usize, selected, ids),
                .cached => |job| {
                    hits += 1;
                    try std.testing.expectEqualSlices(f32, &values, job.ticket.entry.value.cpu);
                },
                .native, .metal => return error.TestUnexpectedResult,
            }
        }
        producer.join();
        if (producer.failure) |err| return err;
        producer.deinit();
        live = false;
        try std.testing.expectEqual(cache.stats.stored, hits);
        try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
        const ticket = cache.take(.{ .clip = hash, .picture = selected[0] }).?;
        cache.restore(ticket);
    }
}
test "embeddinggemma2 video cache abandoned producer restores borrowed entries under backpressure" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var cache = FrameCache{ .allocator = a, .session = session };
    defer cache.deinit();
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    const values: [512]f32 = @splat(0.125);
    var source = media.source.Source{ .allocator = a, .identity = "cached-abort", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const selected = try select(a, &reader);
    defer a.free(selected);
    for (selected) |id| cache.storeCpu(.{ .clip = hash, .picture = id }, &values, 1);
    const producer = try Prefetch.startCached(a, std.testing.io, null, bytes, .{}, .reference, null, &cache);
    var live = true;
    defer if (live) producer.deinit();
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (producer.published.load(.acquire) < 2) {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    producer.deinit();
    live = false;
    for (selected) |id| {
        const ticket = cache.take(.{ .clip = hash, .picture = id }).?;
        cache.restore(ticket);
    }
}

fn preparedReaderHash(a: std.mem.Allocator, producer: *Prefetch) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var frames: usize = 0;
    var jobs: [1]Prefetch.Job = undefined;
    while (true) {
        _ = producer.queue.getUncancelable(std.testing.io, &jobs, 1) catch break;
        defer producer.discard(&jobs[0]);
        switch (jobs[0]) {
            .header => {},
            .native => |job| {
                hash.update(std.mem.sliceAsBytes(job.patches));
                frames += 1;
            },
            .metal => |job| {
                try job.prepared.wait(std.testing.io, .{});
                const patches = try job.prepared.readback(a);
                defer a.free(patches);
                hash.update(std.mem.sliceAsBytes(patches));
                frames += 1;
            },
            else => return error.TestUnexpectedResult,
        }
    }
    producer.join();
    if (producer.failure) |err| return err;
    try std.testing.expect(frames > 0);
    var output: [32]u8 = undefined;
    hash.final(&output);
    return output;
}
test "embeddinggemma2 video borrowed packet preparation matches container and leaves caller read state untouched" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "borrowed-window" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const original_reads = source.reads;
    const original_total = source.total_bytes;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const sampling = Sampling{ .timestamps = .{ .max_frames = 1 } };
    const container_plan = try inspectPlan(a, bytes, .{}, sampling, &pool, false);
    const reader_plan = try inspectReaderPlan(a, &reader, .{}, sampling, &pool, false);
    try std.testing.expectEqual(container_plan, reader_plan);
    const original = try Prefetch.start(a, std.testing.io, null, bytes, .{}, sampling, &pool);
    defer original.deinit();
    const original_hash = try preparedReaderHash(a, original);
    const borrowed = try Prefetch.startReader(a, std.testing.io, null, &reader, .{}, sampling, &pool);
    defer borrowed.deinit();
    const borrowed_hash = try preparedReaderHash(a, borrowed);
    try std.testing.expectEqual(original_hash, borrowed_hash);
    try std.testing.expectEqual(original_reads, source.reads);
    try std.testing.expectEqual(original_total, source.total_bytes);
    try std.testing.expectError(error.Timeout, inspectReaderPlan(a, &reader, .{ .deadline_ns = 0 }, sampling, &pool, false));
    try @import("embedding_gemma2.zig").validateGroup(.{ .content = &.{.{ .sampled_video = .{ .reader = &reader, .sampling = sampling } }} }, .{});
    try std.testing.expectError(error.InvalidEmbeddingGroup, @import("embedding_gemma2.zig").validateGroup(.{ .content = &.{.{ .sampled_video = .{ .bytes = bytes, .reader = &reader, .sampling = sampling } }} }, .{}));
}

test "embeddinggemma2 Metal video borrowed reader preparation matches container on completed device work" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../ops/metal_compute.zig");
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    const device = try MetalCompute.videoDevice(&cb);
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "Metal-borrowed-window" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024, .device_bytes = 256 * 1024 * 1024, .commands = 4 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const sampling = Sampling{ .timestamps = .{ .max_frames = 1 } };
    const original = try Prefetch.start(a, std.testing.io, device, bytes, .{}, sampling, &pool);
    const expected = preparedReaderHash(a, original) catch |err| {
        original.deinit();
        return err;
    };
    original.deinit();
    const borrowed = try Prefetch.startReader(a, std.testing.io, device, &reader, .{}, sampling, &pool);
    defer borrowed.deinit();
    try std.testing.expectEqual(expected, try preparedReaderHash(a, borrowed));
}

test "embeddinggemma2 video source sampling preserves VFR picture identity after preroll eviction" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/mjpeg.mov");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "source-sampling" };
    var original = try media.mp4.Reader.init(a, &source, .{});
    defer original.deinit();
    const packets = try a.dupe(media.mp4.Packet, original.packets[0..3]);
    defer a.free(packets);
    for (packets, [_]i64{ 1, 2, 4 }) |*packet, pts| {
        packet.pts = pts;
        packet.duration = 1;
    }
    var reader = original;
    reader.track.timescale = 3;
    reader.packets = packets;
    const sampling = Sampling{ .source_timestamps = .{ .start_ns = 1_333_333_333, .end_ns = 1_666_666_667 } };
    const all = try selectWithSampling(a, &reader, sampling);
    defer a.free(all);
    try std.testing.expectEqualSlices(usize, &.{2}, all);
    reader.packets = packets[1..];
    const tail = try selectWithSampling(a, &reader, sampling);
    defer a.free(tail);
    try std.testing.expectEqualSlices(usize, &.{1}, tail);
}

test "embeddinggemma2 video source sampling excludes unfinished future fields" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "future-fields" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const packets = reader.packets;
    defer reader.packets = packets;
    var checked = false;
    for (1..packets.len) |end| {
        reader.packets = packets[0..end];
        var logical = try video.h264_pictures.streaming(a, &reader, .{});
        defer logical.deinit();
        const pending = logical.pending_pts orelse continue;
        if (logical.pictures.len == 0) continue;
        const barrier = try media.timeline.rescale(pending, logical.timescale, std.time.ns_per_s);
        if (barrier <= 0) continue;
        const selected = try selectWithSampling(a, &reader, .{ .source_timestamps = .{ .end_ns = barrier } });
        defer a.free(selected);
        try std.testing.expect(selected.len > 0);
        for (selected) |id| try std.testing.expect(id < logical.pending_first_packet.?);
        try std.testing.expectError(error.InvalidLiveWatermark, selectWithSampling(a, &reader, .{ .source_timestamps = .{ .end_ns = barrier + 1 } }));
        checked = true;
    }
    try std.testing.expect(checked);
}

test "embeddinggemma2 video bound selection reuses logical metadata without decoder admission" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "bound-reader", .admission_pool = &pool };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    const expected = try selectWithSampling(a, &reader, sampling);
    defer a.free(expected);
    const before = pool.snapshot();
    pool.limits.host_bytes = before.host_bytes + 256 * 1024;
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, &pool, .{});
    defer selection.deinit();
    const reads = source.reads;
    const selected = try selection.select(a, &reader, sampling, .{});
    defer a.free(selected);
    try std.testing.expectEqualSlices(usize, expected, selected);
    const planned = try inspectReaderPlanWithSelection(a, &reader, .{}, sampling, &pool, false, &selection);
    try std.testing.expect(planned.pixels > 0);
    try std.testing.expectEqual(reads, source.reads);
    try std.testing.expectEqual(before.host_bytes + selected.len * (@sizeOf(usize) + @sizeOf(CacheKey)), pool.snapshot().host_bytes);
    try std.testing.expectError(error.SharedAdmissionExceeded, inspectReaderPlan(a, &reader, .{}, sampling, &pool, false));
}

test "embeddinggemma2 video bound selection rejects payload timing geometry and policy changes" {
    const a = std.testing.allocator;
    const bytes = try a.dupe(u8, @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4"));
    defer a.free(bytes);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "selection-binding" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, null, .{});
    defer selection.deinit();
    const at: usize = @intCast(reader.packets[0].offset + reader.packets[0].size - 1);
    bytes[at] ^= 1;
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, sampling, .{}));
    bytes[at] ^= 1;
    reader.packets[0].pts += 1;
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, sampling, .{}));
    reader.packets[0].pts -= 1;
    reader.track.pixel_aspect.horizontal += 1;
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, sampling, .{}));
    reader.track.pixel_aspect.horizontal -= 1;
    const other = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 2 } };
    try std.testing.expectError(error.InvalidVideoReaderSelection, selection.select(a, &reader, other, .{}));
    const restored = try selection.select(a, &reader, sampling, .{});
    defer a.free(restored);
    try std.testing.expect(restored.len > 0);
    try std.testing.expectError(error.Timeout, inspectReaderPlanWithSelection(a, &reader, .{ .deadline_ns = 0 }, sampling, null, false, &selection));
}

fn selectionPreparationParity(a: std.mem.Allocator, device: ?*anyopaque) !void {
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "selection-patches" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024, .device_bytes = 256 * 1024 * 1024, .commands = 4 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, &pool, .{});
    defer selection.deinit();
    const original = try Prefetch.startReader(a, std.testing.io, device, &reader, .{}, sampling, &pool);
    const expected = preparedReaderHash(a, original) catch |err| {
        original.deinit();
        return err;
    };
    original.deinit();
    const bound = try Prefetch.startReaderWithSelection(a, std.testing.io, device, &reader, .{}, sampling, &pool, &selection);
    defer bound.deinit();
    try std.testing.expectEqual(expected, try preparedReaderHash(a, bound));
}
test "embeddinggemma2 video source bound preparation preserves native field patches" {
    try selectionPreparationParity(std.testing.allocator, null);
}
test "embeddinggemma2 Metal video source bound preparation preserves field patches" {
    const metal = @import("../ops/metal_compute.zig");
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    try selectionPreparationParity(a, try MetalCompute.videoDevice(&cb));
}
fn selectionAllocationRun(a: std.mem.Allocator) !void {
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "selection-failure" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    var selection = try ReaderSelection.init(a, &reader, .{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } }, &logical, &pool, .{});
    defer selection.deinit();
}
test "embeddinggemma2 video source bound selection releases admission on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectionAllocationRun, .{});
}

test "embeddinggemma2 video reader projection prefixes survive append and bind field completion" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    const owned = try a.dupe(u8, bytes);
    defer a.free(owned);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = owned }, .identity = "prefix-fields" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const picture = logical.pictures[0];
    const complete = picture.last_packet orelse return error.TestUnexpectedResult;
    try std.testing.expect(complete > picture.first_packet and complete + 1 < reader.packets.len);
    var prefix = reader;
    prefix.packets = reader.packets[0 .. complete + 1];
    var prefix_index = logical;
    prefix_index.pictures = logical.pictures[0..1];
    prefix_index.packet_count = prefix.packets.len;
    prefix_index.pending_pts = null;
    prefix_index.pending_first_packet = null;
    const prefix_sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var early = try ReaderSelection.init(a, &prefix, prefix_sampling, &prefix_index, null, .{});
    defer early.deinit();
    var later = try ReaderSelection.init(a, &reader, prefix_sampling, &logical, null, .{});
    defer later.deinit();
    try std.testing.expectEqualSlices(usize, early.selected, later.selected);
    try std.testing.expectEqual(early.projection_keys[0], later.projection_keys[0]);
    try std.testing.expect(!std.mem.eql(u8, &early.binding, &later.binding));
    const at = @as(usize, @intCast(reader.packets[complete].offset));
    owned[at + reader.packets[complete].size - 1] ^= 1;
    var changed = try ReaderSelection.init(a, &reader, prefix_sampling, &logical, null, .{});
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &later.projection_keys[0].clip, &changed.projection_keys[0].clip));
    owned[at + reader.packets[complete].size - 1] ^= 1;
    reader.track.pixel_aspect.horizontal *= 2;
    var display = try ReaderSelection.init(a, &reader, prefix_sampling, &logical, null, .{});
    defer display.deinit();
    try std.testing.expect(!std.mem.eql(u8, &later.projection_keys[0].clip, &display.projection_keys[0].clip));
}
test "embeddinggemma2 video projection keys survive IDR preroll eviction and payload relocation" {
    const a = std.testing.allocator;
    const owned = try a.dupe(u8, @embedFile("../testdata/video/closed.mp4"));
    defer a.free(owned);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = owned }, .identity = "rolling-keys" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    var base: usize = 1;
    while (base < reader.packets.len) : (base += 1) {
        var lease = try reader.readPacket(base);
        defer lease.deinit();
        if (try video.avc.isPortableIdr(lease.bytes, reader.track.nal_length_bytes) and projectionReset(&logical, base)) break;
    }
    try std.testing.expect(base > 1 and base + 1 < reader.packets.len);
    const target = base + 1;
    const full = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(full);
    var suffix = reader;
    suffix.packets = reader.packets[base..];
    var suffix_index = try logical.cloneRebased(a, base, suffix.packets.len, null, .{});
    defer suffix_index.deinit();
    const retained = try readerProjectionKeys(a, &suffix, &suffix_index, &.{target - base}, .{});
    defer a.free(retained);
    try std.testing.expectEqual(full[0], retained[0]);
    // Repack the suffix into a different allocation with different offsets.
    const relocated_bytes = try a.alloc(u8, owned.len + 37);
    defer a.free(relocated_bytes);
    const descriptors = try a.dupe(media.mp4.Packet, suffix.packets);
    defer a.free(descriptors);
    var cursor: usize = 37;
    for (descriptors) |*packet| {
        @memcpy(relocated_bytes[cursor..][0..packet.size], owned[@intCast(packet.offset)..][0..packet.size]);
        packet.offset = cursor;
        cursor += packet.size;
    }
    var relocated_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = relocated_bytes[0..cursor] }, .identity = "relocated" };
    var relocated = suffix;
    relocated.input = &relocated_source;
    relocated.packets = descriptors;
    const moved = try readerProjectionKeys(a, &relocated, &suffix_index, &.{target - base}, .{});
    defer a.free(moved);
    try std.testing.expectEqual(full[0], moved[0]);
    // Earlier GOP changes cannot affect this IDR generation.
    const old_end: usize = @intCast(reader.packets[0].offset + reader.packets[0].size - 1);
    owned[old_end] ^= 1;
    const unrelated = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(unrelated);
    try std.testing.expectEqual(full[0], unrelated[0]);
    owned[old_end] ^= 1;
    const dependency_end: usize = @intCast(reader.packets[base].offset + reader.packets[base].size - 1);
    owned[dependency_end] ^= 1;
    const changed = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(changed);
    try std.testing.expect(!std.mem.eql(u8, &full[0].clip, &changed[0].clip));
    owned[dependency_end] ^= 1;
    // avc3 parameter state before an IDR remains part of the dependency chain.
    reader.track.inband_parameter_sets = true;
    suffix.track.inband_parameter_sets = true;
    const dynamic_full = try readerProjectionKeys(a, &reader, &logical, &.{target}, .{});
    defer a.free(dynamic_full);
    const dynamic_suffix = try readerProjectionKeys(a, &suffix, &suffix_index, &.{target - base}, .{});
    defer a.free(dynamic_suffix);
    try std.testing.expect(!std.mem.eql(u8, &dynamic_full[0].clip, &dynamic_suffix[0].clip));
}
test "embeddinggemma2 video projection reset requires complete metadata without crossing assemblies" {
    const a = std.testing.allocator;
    var entries = [_]video.h264.Picture{
        .{ .first_packet = 2, .last_packet = 2, .pts = 0, .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 },
        .{ .first_packet = 0, .last_packet = 1, .pts = 0, .duration = 1, .width = 16, .height = 16, .bit_depth = 8, .chroma_format = 1 },
    };
    var metadata = video.h264_pictures.Index{ .allocator = a, .pictures = &entries, .timescale = 1, .packet_count = 4 };
    try std.testing.expect(projectionReset(&metadata, 2));
    entries[1].last_packet = 3;
    try std.testing.expect(!projectionReset(&metadata, 2));
    entries[1].last_packet = null;
    try std.testing.expect(!projectionReset(&metadata, 2));
    entries[1].last_packet = 1;
    metadata.pending_first_packet = 0;
    try std.testing.expect(!projectionReset(&metadata, 2));
}
test "embeddinggemma2 video cached borrowed reader producer skips native reconstruction and restores tickets" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4");
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "cached-reader" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.streaming(a, &reader, .{});
    defer logical.deinit();
    const sampling = Sampling{ .source_timestamps = .{ .end_ns = std.time.ns_per_s, .max_frames = 1 } };
    var selection = try ReaderSelection.init(a, &reader, sampling, &logical, null, .{});
    defer selection.deinit();
    var marker: u8 = 0;
    const session = @import("../backends/session.zig").Session{ .ptr = &marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined } };
    var cache = FrameCache{ .allocator = a, .session = session };
    defer cache.deinit();
    const values: [512]f32 = @splat(0.125);
    cache.storeCpu(selection.projection_keys[0], &values, 1);
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 } };
    {
        const producer = try Prefetch.startReaderCachedWithSelection(a, std.testing.io, null, &reader, .{}, sampling, &pool, &selection, &cache);
        defer producer.deinit();
        var job: [1]Prefetch.Job = undefined;
        _ = try producer.queue.getUncancelable(std.testing.io, &job, 1);
        try std.testing.expect(job[0] == .header);
        _ = try producer.queue.getUncancelable(std.testing.io, &job, 1);
        try std.testing.expect(job[0] == .cached);
        cache.restore(job[0].cached.ticket);
        producer.join();
        try std.testing.expect(producer.failure == null);
    }
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.stats.hits);
    const ticket = cache.take(selection.projection_keys[0]) orelse return error.TestUnexpectedResult;
    defer cache.restore(ticket);
    try std.testing.expectEqualSlices(f32, &values, ticket.entry.value.cpu);
}

test "embeddinggemma2 video preflight includes native chroma reconstruction scratch" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/av1-chroma-12.webm");
    const plan = try inspectPlan(a, bytes, .{}, .reference, null, true);
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "native-depth-chroma-preflight" };
    var reader = try media.webm.Reader.init(a, &source, .{});
    defer reader.deinit();
    var old = try webmPolicy(reader.track, 127, 95, .{});
    old.rgb_rounding = .nearest;
    var actual = old;
    actual.rgb_rounding = .q13_reconstructed;
    const staging = std.mem.alignForward(usize, 127, 64) * std.mem.alignForward(usize, 95, 64) * 6;
    const required = (try video.preparation.geometry(127, 95, actual)).scratch_bytes + staging;
    const old_charge = (try video.preparation.geometry(127, 95, old)).scratch_bytes + staging;
    try std.testing.expect(plan.preparation_bytes >= required);
    try std.testing.expect(plan.preparation_bytes > old_charge);
    try std.testing.expect((try plan.deviceBytes()) >= required * plan.commands + 16 * 1024 * 1024);
}
