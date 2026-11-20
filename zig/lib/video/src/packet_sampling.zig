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

//! Select MP4 packet identities on the model’s presentation timeline.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const h264_pictures = @import("h264_pictures.zig");
const sampling_mod = @import("sampling.zig");

pub const Sampling = union(enum) { reference, timestamps: sampling_mod.TimestampPolicy, source_timestamps: sampling_mod.TimestampPolicy, gemma4 };
pub const gemma4_max_input_bytes = 256 * 1024 * 1024;
pub const gemma4_max_frames = (sampling_mod.TimestampPolicy{}).max_frames;

pub fn presentationUs(pts: i64, origin: i64, scale: u32) !i64 {
    if (scale == 0) return error.InvalidTimebase;
    return std.math.cast(i64, @divFloor((@as(i128, pts) - origin) * std.time.us_per_s, scale)) orelse error.TimestampOverflow;
}

/// Bucket pictures by their latest eligible one-second target. Filling empty
/// buckets backwards implements first-at-or-after for VFR and B-frame order.
/// Packet IDs break ties before conversion to microseconds at the API boundary.
pub fn gemma4Pictures(reader: *const media.mp4.Reader, pictures: anytype, control: media.source.Control) !Gemma4Plan {
    return planPictures(reader.track.timescale, reader.track.edit, pictures, null, null, control);
}

/// NVDEC returns source packet tags. Only samples producing a visible output
/// participate in its clock, matching the portable logical picture index.
pub fn gemma4Av1Packets(a: std.mem.Allocator, reader: *media.mp4.Reader) !Gemma4Plan {
    const headers = try @import("codecs/av1_headers.zig").Session.create(a, .{ .max_pixels = 4096 * 4096, .all_layers = false, .control = reader.input.control });
    defer headers.deinit();
    try headers.configuration(reader.track.codec_config);
    var visible = try std.DynamicBitSetUnmanaged.initEmpty(a, reader.packets.len);
    defer visible.deinit(a);
    const Display = struct {
        fn accept(raw: *anyopaque, _: @import("codecs/vp9_header.zig").Geometry) !void {
            const seen: *bool = @ptrCast(@alignCast(raw));
            // Several outputs sharing a parser timestamp cannot be independently
            // identified by NVDEC. Route these to the logical decoder up front.
            if (seen.*) return error.UnsupportedVideoFormat;
            seen.* = true;
        }
    };
    for (0..reader.packets.len) |i| {
        var lease = try reader.readPacket(i);
        defer lease.deinit();
        var seen = false;
        try headers.walk(lease.bytes, &seen, Display.accept);
        if (seen) visible.set(i);
    }
    try headers.finish();
    return planPictures(reader.track.timescale, reader.track.edit, reader.packets, null, visible, reader.input.control);
}

/// Select visible AV1 output ordinals in the original MP4 clock. Codec replay
/// still sees every packet, including references outside the presentation edit.
pub fn gemma4AdaptedPictures(timeline: media.webm.SourceTimeline, pictures: []const @import("webm_pictures.zig").Picture, control: media.source.Control) !Gemma4Plan {
    return planPictures(timeline.timescale, timeline.edit, pictures, timeline.samples, null, control);
}

fn pictureTime(pic: anytype, samples: ?[]const media.webm.SourceTimeline.Sample) !media.webm.SourceTimeline.Sample {
    if (comptime @hasField(@TypeOf(pic), "packet_index")) {
        const index = pic.packet_index orelse return error.InvalidVideoIndex;
        if (index >= samples.?.len) return error.InvalidVideoIndex;
        return samples.?[index];
    }
    return .{ .pts = pic.pts, .duration = pic.duration };
}

fn planPictures(scale: u32, edit: media.timeline.Edit, pictures: anytype, samples: ?[]const media.webm.SourceTimeline.Sample, visible: ?std.DynamicBitSetUnmanaged, control: media.source.Control) !Gemma4Plan {
    try control.check();
    if (scale == 0 or pictures.len == 0) return error.EmptyVideo;
    var origin = edit.source_start;
    const duration: i128 = edit.source_duration orelse infer: {
        origin = std.math.maxInt(i64);
        var end: i128 = std.math.minInt(i64);
        for (pictures, 0..) |pic, i| {
            if (i % 256 == 0) try control.check();
            if (visible) |mask| if (!mask.isSet(i)) continue;
            const time = try pictureTime(pic, samples);
            origin = @min(origin, time.pts);
            end = @max(end, @as(i128, time.pts) + time.duration);
        }
        break :infer end - origin;
    };
    if (duration <= 0) return error.EmptyVideo;
    const seconds = @divFloor(duration + scale - 1, scale);
    const count: usize = @intCast(@min(seconds, gemma4_max_frames));
    var best: [gemma4_max_frames]?usize = @splat(null);
    for (pictures, 0..) |pic, id| {
        if (id % 256 == 0) try control.check();
        if (visible) |mask| if (!mask.isSet(id)) continue;
        const pts = (try pictureTime(pic, samples)).pts;
        const tick = @as(i128, pts) - origin;
        if (tick < 0 or tick >= duration) continue;
        const second = @divFloor(tick, scale);
        // Target j occurs at floor(j * (seconds-1) / (count-1)).
        const bucket: usize = if (seconds <= count) @intCast(second) else @intCast(@divFloor((second + 1) * (count - 1) - 1, seconds - 1));
        if (best[bucket]) |previous| {
            const prior = pictures[previous];
            const prior_pts = (try pictureTime(prior, samples)).pts;
            if (pts > prior_pts or (pts == prior_pts and packetId(pic, id) >= packetId(prior, previous))) continue;
        }
        best[bucket] = id;
    }
    var next: ?usize = null;
    var i = count;
    while (i > 0) {
        i -= 1;
        next = best[i] orelse next;
        best[i] = next;
    }
    var plan = Gemma4Plan{};
    var previous: ?usize = null;
    for (best[0..count]) |candidate| if (candidate) |id| {
        if (previous == id) continue;
        previous = id;
        const pic = pictures[id];
        plan.packet_ids[plan.len] = packetId(pic, id);
        plan.timestamps_us[plan.len] = try presentationUs((try pictureTime(pic, samples)).pts, origin, scale);
        plan.len += 1;
    };
    if (plan.len == 0) return error.EmptyVideo;
    return plan;
}

fn packetId(picture: anytype, index: usize) usize {
    return if (@hasField(@TypeOf(picture), "first_packet")) picture.first_packet else index;
}

pub const Gemma4Plan = struct {
    packet_ids: [gemma4_max_frames]usize = undefined,
    timestamps_us: [gemma4_max_frames]i64 = undefined,
    len: usize = 0,

    pub fn ids(self: *const Gemma4Plan) []const usize {
        return self.packet_ids[0..self.len];
    }

    pub fn timestamps(self: *const Gemma4Plan) []const i64 {
        return self.timestamps_us[0..self.len];
    }
};

pub fn needsLogicalIndex(a: std.mem.Allocator, track: media.mp4.Track) !bool {
    if (track.codec != .avc) return false;
    if (track.requiresAvcRegistry()) return true;
    const cfg = try h264.configParse(a, track.avcc);
    defer cfg.groups.deinit(a);
    return !cfg.frame_only or cfg.profile == 88 or cfg.separate_planes or cfg.groups.count > 1 or cfg.redundant;
}

/// Immutable software inputs may need multiple coded members to assemble one picture.
/// The decoder validates entropy and pixels later, under its own admission.
pub fn gemma4PortablePlan(a: std.mem.Allocator, reader: *media.mp4.Reader, registry: ?*@import("h264_dynamic.zig").Session) !Gemma4Plan {
    if (registry != null or try needsLogicalIndex(a, reader.track)) {
        var logical = try h264_pictures.planning(a, reader, h264_pictures.Options.forTrack(reader.packets.len), registry);
        defer logical.deinit();
        return gemma4Pictures(reader, logical.pictures, reader.input.control);
    }
    return gemma4Pictures(reader, reader.packets, reader.input.control);
}

pub fn timestampSelect(a: std.mem.Allocator, metadata: []const sampling_mod.TimedPicture, scale: u32, sampling: Sampling, control: media.source.Control) ![]usize {
    return switch (sampling) {
        .timestamps => |interval| sampling_mod.cappedTimestamps(a, metadata, scale, interval, control),
        .source_timestamps => |interval| sampling_mod.cappedSourceTimestamps(a, metadata, scale, interval, control),
        .gemma4 => sampling_mod.cappedTimestamps(a, metadata, scale, .{}, control),
        .reference => error.InvalidSamplingPolicy,
    };
}
pub fn selectLogical(a: std.mem.Allocator, reader: *const media.mp4.Reader, logical: h264_pictures.Index, sampling: Sampling, control: media.source.Control) ![]usize {
    if (sampling == .gemma4) {
        const plan = try gemma4Pictures(reader, logical.pictures, control);
        return a.dupe(usize, plan.ids());
    }
    if (sampling == .source_timestamps) if (logical.pending_pts) |pts| {
        const end = sampling.source_timestamps.end_ns orelse return error.InvalidLiveWatermark;
        if (@as(i128, pts) * std.time.ns_per_s < @as(i128, end) * logical.timescale) return error.InvalidLiveWatermark;
    };
    if (sampling == .reference) {
        const owned = try logical.select();
        defer logical.allocator.free(owned);
        return a.dupe(usize, owned);
    }
    const metadata = try a.alloc(sampling_mod.TimedPicture, logical.pictures.len);
    defer a.free(metadata);
    for (logical.pictures, metadata) |picture, *item| {
        try control.check();
        item.* = .{ .pts = picture.pts, .duration = picture.duration };
    }
    const selected = try timestampSelect(a, metadata, logical.timescale, sampling, control);
    for (selected) |*id| id.* = logical.pictures[id.*].first_packet;
    return selected;
}

pub fn selectWithSampling(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling) ![]usize {
    return selectConfigured(a, reader, sampling, null);
}

pub fn selectConfigured(a: std.mem.Allocator, reader: *const media.mp4.Reader, sampling: Sampling, registry: ?*@import("h264_dynamic.zig").Session) ![]usize {
    var mutable = reader.*;
    if (sampling == .gemma4) {
        const plan = try gemma4PortablePlan(a, &mutable, registry);
        return a.dupe(usize, plan.ids());
    }
    if (try needsLogicalIndex(a, reader.track)) {
        var logical = try h264_pictures.indexMode(a, &mutable, .{}, sampling == .source_timestamps, registry);
        defer logical.deinit();
        return selectLogical(a, reader, logical, sampling, reader.input.control);
    }
    if (sampling != .reference) {
        const metadata = try a.alloc(sampling_mod.TimedPicture, reader.packets.len);
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
    const sampled = try sampling_mod.embeddingGemma2(a, .{ .total_frames = order.len, .fps = @as(f64, @floatFromInt(order.len)) / seconds, .duration_seconds = seconds }, .{});
    for (sampled) |*index| index.* = order[index.*];
    return sampled;
}
