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

//! MP4 tracks whose codec reconstruction already lives on the WebM lane. Codec
//! packets are unchanged, so only container metadata needs translating.
const std = @import("std");
const media = @import("antfly_media");
const display = @import("display.zig");

/// True when the clip's first decodable track belongs on the WebM lane. A clip
/// the MP4 reader can't open isn't claimed; its own route reports the failure.
pub fn usesWebmLane(a: std.mem.Allocator, bytes: []const u8) bool {
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "video-route" };
    var reader = media.mp4.Reader.init(a, &source, limits) catch return false;
    defer reader.deinit();
    return reader.track.codec == .av1;
}

pub const limits: media.mp4.Limits = .{ .codecs = std.EnumSet(media.mp4.Codec).initMany(&.{ .avc, .mjpeg, .hevc, .av1 }) };

/// The returned reader takes over the MP4 reader's metadata and index admission.
pub fn openWebm(a: std.mem.Allocator, source: *media.source.Source) !media.webm.Reader {
    var reader = try media.mp4.Reader.init(a, source, limits);
    var moved = false;
    defer if (!moved) reader.deinit();
    if (reader.track.codec != .av1) return error.UnsupportedVideoCodec;
    if (reader.track.timescale == 0) return error.InvalidTimebase;
    const track = try webmTrack(reader.track);
    const packets = try a.alloc(media.webm.Packet, reader.packets.len);
    errdefer a.free(packets);
    var start: i128 = std.math.maxInt(i64);
    var end: i128 = std.math.minInt(i64);
    for (reader.packets, packets) |packet, *out| {
        start = @min(start, packet.pts);
        end = @max(end, @as(i128, packet.pts) + packet.duration);
        out.* = .{
            .cluster_offset = packet.offset,
            .offset = packet.offset,
            .size = packet.size,
            .pts = try nanoseconds(packet.pts, reader.track.timescale),
            .duration_ns = @intCast(try nanoseconds(packet.duration, reader.track.timescale)),
            .sync = packet.sync,
            .invisible = false,
            .discardable = false,
        };
    }
    const cues = try a.alloc(media.webm.Cue, 0);
    const span = if (end > start) @as(f64, @floatFromInt(end - start)) / @as(f64, @floatFromInt(reader.track.timescale)) else null;
    const result = media.webm.Reader{
        .allocator = a,
        .input = source,
        .metadata = reader.metadata,
        .index_reservation = reader.index_reservation,
        .track = track,
        .packets = packets,
        .cues = cues,
        .timestamp_scale_ns = 1,
        .duration_seconds = span,
    };
    reader.allocator.free(reader.packets);
    moved = true;
    return result;
}

fn nanoseconds(ticks: i64, timescale: u32) !i64 {
    return std.math.cast(i64, @divFloor(@as(i128, ticks) * std.time.ns_per_s, timescale)) orelse error.TimestampOverflow;
}

pub fn webmTrack(track: media.mp4.Track) !media.webm.Track {
    // Geometry is validated by the shared MP4 policy before it's re-expressed.
    const layout = try display.layout(track, track.width, track.height, .presentation);
    const sar = track.pixel_aspect;
    return .{
        .number = track.id,
        .codec = .av1,
        .width = track.width,
        .height = track.height,
        .codec_private = track.codec_config,
        .presentation = .{
            .display_width = @as(u32, track.width) * sar.horizontal,
            .display_height = @as(u32, track.height) * sar.vertical,
            .roll = switch (layout.rotation) {
                .none => 0,
                .clockwise90 => -90,
                .half_turn => 180,
                .clockwise270 => 90,
            },
        },
        .colour = try colour(track.color_info),
    };
}

/// Absent, ICC or unspecified declarations leave the bitstream's signal in charge.
fn colour(bytes: []const u8) !media.webm.Colour {
    if (bytes.len < 10) return .{};
    const nclx = std.mem.eql(u8, bytes[0..4], "nclx");
    if (!nclx and !std.mem.eql(u8, bytes[0..4], "nclc")) return .{};
    if (nclx and bytes.len < 11) return error.UnsupportedVideoColor;
    return .{
        .primaries = std.mem.readInt(u16, bytes[4..6], .big),
        .transfer = std.mem.readInt(u16, bytes[6..8], .big),
        .matrix = std.mem.readInt(u16, bytes[8..10], .big),
        .range = if (nclx) (if (bytes[10] & 0x80 != 0) @as(u8, 2) else 1) else 0,
    };
}

test "AV1 MP4 keeps its packets and display metadata on the WebM lane" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/av1-mp4.mp4");
    try std.testing.expect(usesWebmLane(a, bytes));
    try std.testing.expect(!usesWebmLane(a, @embedFile("../testdata/mjpeg.mov")));
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "av1" };
    var original = try media.mp4.Reader.init(a, &source, limits);
    defer original.deinit();
    var reader = try openWebm(a, &source);
    defer reader.deinit();
    try std.testing.expectEqual(original.packets.len, reader.packets.len);
    try std.testing.expectEqual(media.webm.Codec.av1, reader.track.codec);
    try std.testing.expectEqualSlices(u8, original.track.codec_config, reader.track.codec_private);
    var lease = try reader.readPacket(0);
    defer lease.deinit();
    var expected = try original.readPacket(0);
    defer expected.deinit();
    try std.testing.expectEqualSlices(u8, expected.bytes, lease.bytes);
}

test "MP4 display transforms and SAR survive the translation to WebM layout" {
    var track = media.mp4.Track{ .id = 1, .timescale = 1000, .width = 64, .height = 48, .avcc = &.{}, .nal_length_bytes = 4, .display_matrix = .{ 0, 65536, 0, -65536, 0, 0, 0, 0, 1073741824 } };
    track.pixel_aspect = .{ .horizontal = 2, .vertical = 1 };
    const expected = try display.layout(track, 64, 48, .presentation);
    const actual = (try display.webmLayout(try webmTrack(track), 64, 48, .presentation)).display;
    try std.testing.expectEqual(expected.rotation, actual.rotation);
    try std.testing.expectEqual(expected.width, actual.width);
    try std.testing.expectEqual(expected.height, actual.height);
}

test "MP4 color declarations map to WebM colour" {
    const hlg = try colour("nclx" ++ [_]u8{ 0, 9, 0, 18, 0, 9, 0 });
    try std.testing.expectEqual(@as(u16, 18), hlg.transfer);
    try std.testing.expectEqual(@as(u8, 1), hlg.range);
    try std.testing.expectEqual(@as(u16, 2), (try colour("prof-icc-profile")).matrix);
}
