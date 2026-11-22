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

pub const Av1FrameUnits = @import("codecs/av1_headers.zig").FrameUnits;
pub const Sampling = union(enum) { reference, timestamps: sampling_mod.TimestampPolicy, source_timestamps: sampling_mod.TimestampPolicy, gemma4 };
pub const reference_max_input_bytes = 256 * 1024 * 1024;
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
    const counts = try av1OutputCounts(a, reader);
    defer a.free(counts);
    return gemma4Av1FromCounts(a, reader, counts);
}

pub fn gemma4Av1FromCounts(_: std.mem.Allocator, reader: *media.mp4.Reader, counts: []const usize) !Gemma4Plan {
    if (counts.len != reader.packets.len) return error.InvalidVideoIndex;
    // Outputs in one sample share its clock. Stable tie breaking selects
    // its first visible ordinal, including show_existing_frame.
    return planPictures(reader.track.timescale, reader.track.edit, reader.packets, null, counts, reader.input.control);
}

pub fn av1OutputCounts(a: std.mem.Allocator, reader: *media.mp4.Reader) ![]usize {
    return av1OutputCountsLimited(a, reader, std.math.maxInt(usize));
}

pub fn av1OutputCountsLimited(a: std.mem.Allocator, reader: *media.mp4.Reader, max_units: usize) ![]usize {
    const headers = try @import("codecs/av1_headers.zig").Session.create(a, .{ .max_frame_units = max_units, .max_pixels = 4096 * 4096, .all_layers = false, .control = reader.input.control });
    defer headers.deinit();
    try headers.configuration(reader.track.codec_config);
    const counts = try a.alloc(usize, reader.packets.len);
    errdefer a.free(counts);
    const Display = struct {
        fn accept(raw: *anyopaque, _: @import("codecs/vp9_header.zig").Geometry) !void {
            const count: *usize = @ptrCast(@alignCast(raw));
            count.* += 1;
        }
    };
    for (counts, 0..) |*count, i| {
        var lease = try reader.readPacket(i);
        defer lease.deinit();
        count.* = 0;
        try headers.walk(lease.bytes, count, Display.accept);
    }
    try headers.finish();
    return counts;
}

/// Select visible AV1 output ordinals in the original MP4 clock. Codec replay
/// still sees every packet, including references outside the presentation edit.
pub fn gemma4AdaptedPictures(timeline: media.webm.SourceTimeline, pictures: []const @import("webm_pictures.zig").Picture, control: media.source.Control) !Gemma4Plan {
    return planPictures(timeline.timescale, timeline.edit, pictures, timeline.samples, null, control);
}

pub fn gemma4WebmPictures(index: @import("webm_pictures.zig").Index, control: media.source.Control) !Gemma4Plan {
    if (index.sourceTimeline()) |source| return gemma4AdaptedPictures(source, index.pictures, control);
    if (index.pictures.len == 0) return error.EmptyVideo;
    var bounds = sampling_mod.Bounds{};
    for (index.pictures, 0..) |picture, i| {
        if (i % 256 == 0) try control.check();
        bounds.include(picture.pts, picture.duration);
    }
    const declared: ?i64 = if (index.duration_seconds) |seconds| @intFromFloat(seconds * std.time.ns_per_s) else null;
    const duration = std.math.cast(i64, bounds.end(declared) - bounds.first) orelse return error.TimestampOverflow;
    if (duration == 0 and bounds.first == bounds.latest) {
        // All visible outputs share one instant. Target zero is decidable
        // without inventing a duration or frame rate.
        var plan = Gemma4Plan{};
        plan.packet_ids[0] = 0;
        plan.timestamps_us[0] = 0;
        plan.len = 1;
        return plan;
    }
    if (duration <= 0) return error.EmptyVideoWindow;
    return planPictures(std.time.ns_per_s, .{ .source_start = bounds.first, .source_duration = duration }, index.pictures, null, null, control);
}

fn pictureTime(pic: anytype, samples: ?[]const media.webm.SourceTimeline.Sample) !sampling_mod.TimedPicture {
    if (comptime @hasField(@TypeOf(pic), "packet_index")) {
        if (samples) |clock| {
            const index = pic.packet_index orelse return error.InvalidVideoIndex;
            if (index >= clock.len) return error.InvalidVideoIndex;
            return .{ .pts = clock[index].pts, .duration = clock[index].duration };
        }
    }
    return .{ .pts = pic.pts, .duration = pic.duration };
}

fn planPictures(scale: u32, edit: media.timeline.Edit, pictures: anytype, samples: ?[]const media.webm.SourceTimeline.Sample, visible: ?[]const usize, control: media.source.Control) !Gemma4Plan {
    try control.check();
    if (scale == 0 or pictures.len == 0) return error.EmptyVideo;
    var origin = edit.source_start;
    const duration: i128 = edit.source_duration orelse infer: {
        origin = std.math.maxInt(i64);
        var end: i128 = std.math.minInt(i64);
        for (pictures, 0..) |pic, i| {
            if (i % 256 == 0) try control.check();
            if (sampling_mod.hidden(pic)) continue;
            if (visible) |mask| if (mask[i] == 0) continue;
            const time = try pictureTime(pic, samples);
            origin = @min(origin, time.pts);
            end = @max(end, @as(i128, time.pts) + (time.duration orelse 0));
        }
        break :infer end - origin;
    };
    if (duration <= 0) return error.EmptyVideo;
    const seconds = @divFloor(duration + scale - 1, scale);
    const count: usize = @intCast(@min(seconds, gemma4_max_frames));
    var best: [gemma4_max_frames]?usize = @splat(null);
    var held: ?usize = null;
    var held_pts: i64 = std.math.minInt(i64);
    for (pictures, 0..) |pic, id| {
        if (id % 256 == 0) try control.check();
        if (sampling_mod.hidden(pic)) continue;
        if (visible) |mask| if (mask[id] == 0) continue;
        const time = try pictureTime(pic, samples);
        const pts = time.pts;
        if (pts < origin and @as(i128, pts) + (time.duration orelse 0) > origin and (held == null or pts > held_pts or (pts == held_pts and packetId(pic, id) < packetId(pictures[held.?], held.?)))) {
            held = id;
            held_pts = pts;
        }
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
    // Clipping makes the opening hold visible at presentation time zero.
    // A picture starting exactly on the boundary supersedes that hold.
    if (held) |id| {
        if (best[0] == null or (try pictureTime(pictures[best[0].?], samples)).pts > origin) best[0] = id;
    }
    var plan = Gemma4Plan{};
    var previous: ?usize = null;
    for (best[0..count]) |candidate| if (candidate) |id| {
        if (previous == id) continue;
        previous = id;
        const pic = pictures[id];
        plan.packet_ids[plan.len] = packetId(pic, id);
        plan.timestamps_us[plan.len] = try presentationUs(@max(origin, (try pictureTime(pic, samples)).pts), origin, scale);
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
fn selectTimestampEdit(a: std.mem.Allocator, metadata: []const sampling_mod.TimedPicture, track: media.mp4.Track, sampling: Sampling, control: media.source.Control) ![]usize {
    if (track.edit.source_duration == null) return timestampSelect(a, metadata, track.timescale, sampling, control);
    var timeline = try sampling_mod.Timeline.initEdited(a, metadata, control, track.edit);
    defer timeline.deinit();
    return switch (sampling) {
        .timestamps => |policy| timeline.select(a, track.timescale, policy, control, true),
        .source_timestamps => |policy| timeline.select(a, track.timescale, policy, control, false),
        else => error.InvalidSamplingPolicy,
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
        const ids = try sampling_mod.referencePictures(a, logical.pictures, logical.timescale, reader.track.edit, control);
        for (ids) |*id| id.* = logical.pictures[id.*].first_packet;
        return ids;
    }
    const metadata = try a.alloc(sampling_mod.TimedPicture, logical.pictures.len);
    defer a.free(metadata);
    for (logical.pictures, metadata) |picture, *item| {
        try control.check();
        item.* = .{ .pts = picture.pts, .duration = picture.duration };
    }
    const selected = try selectTimestampEdit(a, metadata, reader.track, sampling, control);
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
        var logical = if (sampling == .source_timestamps)
            try h264_pictures.indexMode(a, &mutable, h264_pictures.Options.forTrack(reader.packets.len), true, registry)
        else
            try h264_pictures.planning(a, &mutable, h264_pictures.Options.forTrack(reader.packets.len), registry);
        defer logical.deinit();
        return selectLogical(a, reader, logical, sampling, reader.input.control);
    }
    if (sampling != .reference) {
        const metadata = try a.alloc(sampling_mod.TimedPicture, reader.packets.len);
        defer a.free(metadata);
        for (reader.packets, metadata) |packet, *item| item.* = .{ .pts = packet.pts, .duration = packet.duration, .hidden = packet.hidden };
        return selectTimestampEdit(a, metadata, reader.track, sampling, reader.input.control);
    }
    return sampling_mod.referencePictures(a, reader.packets, reader.track.timescale, reader.track.edit, reader.input.control);
}

test "Gemma4 selects a solitary undated WebM output and a picture spanning the entire edit" {
    var visible = [_]@import("webm_pictures.zig").Picture{.{ .pts = 123, .duration = null, .width = 16, .height = 16 }};
    var single = try gemma4WebmPictures(.{ .allocator = std.testing.allocator, .pictures = &visible }, .{});
    try std.testing.expectEqualSlices(usize, &.{0}, single.ids());
    try std.testing.expectEqualSlices(i64, &.{0}, single.timestamps());
    visible[0].duration = 100;
    var held = try planPictures(10, .{ .source_start = 130, .source_duration = 20 }, &visible, null, null, .{});
    try std.testing.expectEqualSlices(usize, &.{0}, held.ids());
    try std.testing.expectEqualSlices(i64, &.{0}, held.timestamps());
}

test "reference pictures filter a trimmed MP4 edit before applying average FPS" {
    const samples = [_]media.webm.SourceTimeline.Sample{
        .{ .pts = 20, .duration = 10 }, .{ .pts = 10, .duration = 10 },
        .{ .pts = 30, .duration = 10 }, .{ .pts = 0, .duration = 10 },
    };
    const ids = try sampling_mod.referencePictures(std.testing.allocator, &samples, 10, .{ .source_start = 10, .source_duration = 20 }, .{});
    defer std.testing.allocator.free(ids);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0 }, ids);
}

test "edited opening hold precedes the next target and yields to an exact boundary" {
    var times = [_]media.webm.SourceTimeline.Sample{ .{ .pts = 0, .duration = 15 }, .{ .pts = 15, .duration = 10 } };
    const edit = media.timeline.Edit{ .source_start = 5, .source_duration = 20 };
    var plan = try planPictures(10, edit, &times, null, null, .{});
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, plan.ids());
    try std.testing.expectEqualSlices(i64, &.{ 0, 1_000_000 }, plan.timestamps());
    times[1].pts = 5;
    plan = try planPictures(10, edit, &times, null, null, .{});
    try std.testing.expectEqualSlices(usize, &.{1}, plan.ids());
}

test "explicit opening hold yields to exact edit and ignores hidden reference packets" {
    const a = std.testing.allocator;
    const pictures = [_]sampling_mod.TimedPicture{
        .{ .pts = 5, .duration = 10 },
        .{ .pts = 10, .duration = 10, .hidden = true },
        .{ .pts = 10, .duration = 10 },
    };
    const edit = media.timeline.Edit{ .source_start = 10, .source_duration = 10 };
    const reference = try sampling_mod.referencePictures(a, &pictures, 10, edit, .{});
    defer a.free(reference);
    try std.testing.expectEqualSlices(usize, &.{2}, reference);
    var plan = try planPictures(10, edit, &pictures, null, null, .{});
    try std.testing.expectEqualSlices(usize, &.{2}, plan.ids());
    const hold = try sampling_mod.referencePictures(a, pictures[0..2], 10, edit, .{});
    defer a.free(hold);
    try std.testing.expectEqualSlices(usize, &.{0}, hold);
}

test "reference sampling includes longest presentation even with earlier PTS" {
    const samples = [_]media.webm.SourceTimeline.Sample{ .{ .pts = 1, .duration = 1 }, .{ .pts = 0, .duration = 4 } };
    const ids = try sampling_mod.referencePictures(std.testing.allocator, &samples, 1, .{}, .{});
    defer std.testing.allocator.free(ids);
    try std.testing.expectEqualSlices(usize, &.{ 1, 1, 0, 0 }, ids);
}
