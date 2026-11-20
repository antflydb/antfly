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
pub const gemma4_max_frames = (sampling_mod.TimestampPolicy{}).max_frames;

pub fn gemma4Origin(reader: *const media.mp4.Reader) !i64 {
    if (reader.track.edit.source_duration != null) return reader.track.edit.source_start;
    if (reader.packets.len == 0) return error.EmptyVideo;
    var first = reader.packets[0].pts;
    for (reader.packets, 0..) |packet, i| {
        if (i % 256 == 0) try reader.input.control.check();
        first = @min(first, packet.pts);
    }
    return first;
}

pub fn presentationUs(pts: i64, origin: i64, scale: u32) !i64 {
    if (scale == 0) return error.InvalidTimebase;
    return std.math.cast(i64, @divFloor((@as(i128, pts) - origin) * std.time.us_per_s, scale)) orelse error.TimestampOverflow;
}

/// Bucket pictures by their latest eligible one-second target. Filling empty
/// buckets backwards implements first-at-or-after for VFR and B-frame order.
/// Packet IDs break ties before conversion to microseconds at the API boundary.
pub fn gemma4Pictures(a: std.mem.Allocator, reader: *const media.mp4.Reader, pictures: anytype) ![]usize {
    const scale = reader.track.timescale;
    if (scale == 0 or pictures.len == 0) return error.EmptyVideo;
    const origin = try gemma4Origin(reader);
    const duration: i128 = reader.track.edit.source_duration orelse infer: {
        var end: i128 = origin;
        for (pictures, 0..) |pic, i| {
            if (i % 256 == 0) try reader.input.control.check();
            end = @max(end, @as(i128, pic.pts) + pic.duration);
        }
        break :infer end - origin;
    };
    if (duration <= 0) return error.EmptyVideo;
    const seconds = @divFloor(duration + scale - 1, scale);
    const count: usize = @intCast(@min(seconds, gemma4_max_frames));
    var best: [gemma4_max_frames]?usize = @splat(null);
    for (pictures, 0..) |pic, id| {
        if (id % 256 == 0) try reader.input.control.check();
        const tick = @as(i128, pic.pts) - origin;
        if (tick < 0 or tick >= duration) continue;
        const second = @divFloor(tick, scale);
        // Target j occurs at floor(j * (seconds-1) / (count-1)).
        const bucket: usize = if (seconds <= count) @intCast(second) else @intCast(@divFloor((second + 1) * (count - 1) - 1, seconds - 1));
        if (best[bucket] == null or pic.pts < pictures[best[bucket].?].pts) best[bucket] = id;
    }
    var next: ?usize = null;
    var i = count;
    while (i > 0) {
        i -= 1;
        next = best[i] orelse next;
        best[i] = next;
    }
    var ids: std.ArrayList(usize) = .empty;
    errdefer ids.deinit(a);
    for (best[0..count]) |candidate| if (candidate) |id| {
        if (ids.items.len == 0 or ids.items[ids.items.len - 1] != id) try ids.append(a, id);
    };
    if (ids.items.len == 0) return error.EmptyVideo;
    return ids.toOwnedSlice(a);
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
        const ids = try gemma4Pictures(a, reader, logical.pictures);
        for (ids) |*id| id.* = logical.pictures[id.*].first_packet;
        return ids;
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
    if (reader.track.codec == .avc) {
        const cfg = try h264.configParse(a, reader.track.avcc);
        defer cfg.groups.deinit(a);
        if (reader.track.inband_parameter_sets or !cfg.frame_only or cfg.profile == 88 or cfg.separate_planes or cfg.groups.count > 1 or cfg.redundant) {
            // Index completed assemblies rather than transport members.
            var mutable = reader.*;
            var logical = if (sampling == .source_timestamps)
                try h264_pictures.streaming(a, &mutable, .{})
            else
                try h264_pictures.index(a, &mutable, .{});
            defer logical.deinit();
            return selectLogical(a, reader, logical, sampling, reader.input.control);
        }
    }
    if (sampling == .gemma4) return gemma4Pictures(a, reader, reader.packets);
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
