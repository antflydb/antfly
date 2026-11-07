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
pub fn usesWebmLane(_: std.mem.Allocator, bytes: []const u8) bool {
    // Most clips aren't AV1. Neither this fast check nor the metadata probe
    // allocates an index; the chosen route owns full validation under admission.
    if (!moovMentionsAv1(bytes)) return false;
    const track = media.mp4.probeTrack(bytes, limits, .{}) catch return false;
    return track.codec == .av1;
}

/// Walks top-level boxes only. A false positive just costs the full parse below.
fn moovMentionsAv1(bytes: []const u8) bool {
    var cursor: usize = 0;
    while (bytes.len - cursor >= 8) {
        var size: u64 = std.mem.readInt(u32, bytes[cursor..][0..4], .big);
        var header: usize = 8;
        if (size == 1) {
            if (bytes.len - cursor < 16) return false;
            size = std.mem.readInt(u64, bytes[cursor + 8 ..][0..8], .big);
            header = 16;
        } else if (size == 0) {
            size = bytes.len - cursor;
        }
        if (size < header or size > bytes.len - cursor) return false;
        const box: usize = @intCast(size);
        if (std.mem.eql(u8, bytes[cursor + 4 ..][0..4], "moov")) {
            return std.mem.indexOf(u8, bytes[cursor + header .. cursor + box], "av01") != null;
        }
        cursor += box;
    }
    return false;
}

pub const limits: media.mp4.Limits = .{ .codecs = std.EnumSet(media.mp4.Codec).initMany(&.{ .avc, .mjpeg, .hevc, .av1 }) };

/// The returned reader owns MP4 metadata and exact WebM-index admission.
/// Conversion admits the overlap while both packet indexes are live.
pub fn openWebm(a: std.mem.Allocator, source: *media.source.Source) !media.webm.Reader {
    var reader = try media.mp4.Reader.init(a, source, limits);
    errdefer reader.deinit();
    return intoWebm(a, &reader);
}

/// Consumes the MP4 owner on success; errors preserve it for caller cleanup.
fn intoWebm(a: std.mem.Allocator, reader: *media.mp4.Reader) !media.webm.Reader {
    try reader.input.control.check();
    if (reader.track.codec != .av1) return error.UnsupportedVideoCodec;
    if (reader.track.timescale == 0) return error.InvalidTimebase;
    const track = try webmTrack(reader.track);
    if (reader.packets.len > limits.max_index_bytes / @sizeOf(media.webm.Packet)) return error.ResourceLimitExceeded;
    const index_bytes = reader.packets.len * @sizeOf(media.webm.Packet);
    // The old index remains live throughout translation. Admit this moving
    // overlap before allocation, and retain the replacement's exact charge.
    var replacement = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = index_bytes }) else media.admission.Token{};
    errdefer replacement.deinit();
    const packets = try a.alloc(media.webm.Packet, reader.packets.len);
    errdefer a.free(packets);
    var start: i128 = std.math.maxInt(i64);
    var end: i128 = std.math.minInt(i64);
    for (reader.packets, packets, 0..) |packet, *out, index| {
        if (index % 256 == 0) try reader.input.control.check();
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
    try reader.input.control.check();
    const cues = try a.alloc(media.webm.Cue, 0);
    const span = if (end > start) @as(f64, @floatFromInt(end - start)) / @as(f64, @floatFromInt(reader.track.timescale)) else null;
    const result = media.webm.Reader{
        .allocator = a,
        .input = reader.input,
        .metadata = reader.metadata,
        .index_reservation = replacement,
        .track = track,
        .packets = packets,
        .cues = cues,
        .timestamp_scale_ns = 1,
        .duration_seconds = span,
    };
    reader.allocator.free(reader.packets);
    reader.index_reservation.deinit();
    reader.* = undefined;
    return result;
}

fn nanoseconds(ticks: i64, timescale: u32) !i64 {
    return std.math.cast(i64, @divFloor(@as(i128, ticks) * std.time.ns_per_s, timescale)) orelse error.TimestampOverflow;
}

pub fn webmTrack(track: media.mp4.Track) !media.webm.Track {
    // Geometry is validated by the shared MP4 policy before it's re-expressed.
    const layout = try display.layout(track, track.width, track.height, .presentation);
    const display_aspect = try displayRatio(track);
    return .{
        .number = track.id,
        .codec = .av1,
        .width = track.width,
        .height = track.height,
        .codec_private = track.codec_config,
        .presentation = .{
            .display_width = display_aspect[0],
            .display_height = display_aspect[1],
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

/// Display ratio in lowest terms. SAR components can be as large as their box
/// allows while still describing a small ratio, so they can't simply be multiplied.
fn displayRatio(track: media.mp4.Track) ![2]u32 {
    const sar = track.pixel_aspect;
    const reduced = std.math.gcd(sar.horizontal, sar.vertical);
    const width = @as(u64, track.width) * (sar.horizontal / reduced);
    const height = @as(u64, track.height) * (sar.vertical / reduced);
    const common = std.math.gcd(width, height);
    return .{
        std.math.cast(u32, width / common) orelse return error.UnsupportedVideoDisplay,
        std.math.cast(u32, height / common) orelse return error.UnsupportedVideoDisplay,
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
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expect(usesWebmLane(denied.allocator(), bytes));
    try std.testing.expectEqual(@as(usize, 0), denied.allocated_bytes);
    try std.testing.expect(!usesWebmLane(a, @embedFile("../testdata/mjpeg.mov")));
    try std.testing.expect(!moovMentionsAv1(@embedFile("../testdata/hevc-mp4.mp4")));
    try std.testing.expect(!moovMentionsAv1("not an mp4 at all"));
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

test "Large equivalent SAR components don't overflow the display ratio" {
    var track = media.mp4.Track{ .id = 1, .timescale = 1000, .width = 64, .height = 48, .avcc = &.{}, .nal_length_bytes = 4, .display_matrix = .{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 } };
    track.pixel_aspect = .{ .horizontal = 100_000_000, .vertical = 100_000_000 };
    const presentation = (try webmTrack(track)).presentation;
    try std.testing.expectEqual(@as(?u32, 4), presentation.display_width);
    try std.testing.expectEqual(@as(?u32, 3), presentation.display_height);
    const layout = try display.webmLayout(try webmTrack(track), 64, 48, .presentation);
    try std.testing.expectEqual(@as(u32, 64), layout.display.width);
    // A ratio that truly can't be represented is refused rather than wrapped.
    track.pixel_aspect = .{ .horizontal = std.math.maxInt(u32), .vertical = 1 };
    try std.testing.expectError(error.ResourceLimitExceeded, display.layout(track, 64, 48, .presentation));
}

test "MP4 color declarations map to WebM colour" {
    const hlg = try colour("nclx" ++ [_]u8{ 0, 9, 0, 18, 0, 9, 0 });
    try std.testing.expectEqual(@as(u16, 18), hlg.transfer);
    try std.testing.expectEqual(@as(u8, 1), hlg.range);
    try std.testing.expectEqual(@as(u16, 2), (try colour("prof-icc-profile")).matrix);
}

// Synthetic prevalidated metadata keeps migration tests independent of encoding
// cost and fixture length; real packet preservation is covered above.
fn indexedTestReader(a: std.mem.Allocator, source: *media.source.Source, count: usize) !media.mp4.Reader {
    var metadata = try source.read(0, @intCast(source.length()));
    errdefer metadata.deinit();
    var reservation = try source.admission_pool.?.acquire(.{ .host_bytes = count * @sizeOf(media.mp4.Packet) });
    errdefer reservation.deinit();
    const packets = try a.alloc(media.mp4.Packet, count);
    for (packets, 0..) |*packet, index| packet.* = .{ .offset = 0, .size = 1, .dts = @intCast(index), .media_pts = @intCast(index), .pts = @intCast(index), .duration = 1, .sync = true };
    return .{
        .allocator = a,
        .input = source,
        .metadata = metadata,
        .index_reservation = reservation,
        .track = .{ .codec = .av1, .id = 1, .timescale = 1000, .width = 192, .height = 192, .avcc = &.{}, .codec_config = &.{ 0x81, 0, 0x0c, 0 }, .nal_length_bytes = 0, .display_matrix = .{ 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824 } },
        .packets = packets,
        .limits = limits,
    };
}

test "AV1 MP4 bridge admits index overlap before allocation and transfers exact ownership" {
    const a = std.testing.allocator;
    const count = 400;
    const old_bytes = count * @sizeOf(media.mp4.Packet);
    const new_bytes = count * @sizeOf(media.webm.Packet);
    const peak = "metadata".len + old_bytes + new_bytes;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = peak - 1 } };
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = "metadata" }, .identity = "index-overlap", .admission_pool = &pool };
    var reader = try indexedTestReader(a, &source, count);
    var owns_reader = true;
    defer if (owns_reader) reader.deinit();
    const original = reader.packets.ptr;
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.SharedAdmissionExceeded, intoWebm(denied.allocator(), &reader));
    try std.testing.expectEqual(@as(usize, 0), denied.allocated_bytes);
    try std.testing.expectEqual(original, reader.packets.ptr);
    try std.testing.expectEqual(@as(u64, "metadata".len + old_bytes), pool.snapshot().host_bytes);
    pool.limits.host_bytes = peak;
    var translated = try intoWebm(a, &reader);
    owns_reader = false;
    var owns_translated = true;
    defer if (owns_translated) translated.deinit();
    try std.testing.expectEqual(@as(u64, peak), pool.high_water.host_bytes);
    try std.testing.expectEqual(@as(u64, new_bytes), translated.index_reservation.resources.host_bytes);
    try std.testing.expectEqual(@as(u64, "metadata".len + new_bytes), pool.snapshot().host_bytes);
    try std.testing.expectEqual(@as(i64, 399_000_000), translated.packets[399].pts);
    try std.testing.expectEqual(@as(?u64, 1_000_000), translated.packets[0].duration_ns);
    translated.deinit();
    owns_translated = false;
    try std.testing.expectEqualDeep(media.admission.Resources{}, pool.snapshot());
    try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
}

test "AV1 MP4 bridge allocation failures release both index and metadata ownership" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
            var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = "metadata" }, .identity = "bridge-allocation", .admission_pool = &pool };
            defer {
                std.debug.assert(pool.snapshot().host_bytes == 0);
                std.debug.assert(source.retained_bytes == 0);
            }
            var reader = try indexedTestReader(a, &source, 400);
            var owns_reader = true;
            defer if (owns_reader) reader.deinit();
            var translated = try intoWebm(a, &reader);
            owns_reader = false;
            defer translated.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "AV1 MP4 bridge conversion errors and cancellation preserve the source owner" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |cancel| {
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = "metadata" }, .identity = "bridge-errors", .admission_pool = &pool };
        defer std.debug.assert(pool.snapshot().host_bytes == 0);
        var reader = try indexedTestReader(a, &source, 400);
        defer reader.deinit();
        const Cancel = struct {
            calls: usize = 0,
            fn check(raw: ?*const anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
                self.calls += 1;
                if (self.calls == 2) return error.Cancelled;
            }
        };
        var cancellation: Cancel = .{};
        if (cancel) source.control = .{ .context = &cancellation, .check_fn = Cancel.check } else reader.packets[0].pts = std.math.maxInt(i64);
        const before = pool.snapshot();
        const original = reader.packets.ptr;
        try std.testing.expectError(if (cancel) error.Cancelled else error.TimestampOverflow, intoWebm(a, &reader));
        try std.testing.expectEqualDeep(before, pool.snapshot());
        try std.testing.expectEqual(original, reader.packets.ptr);
        try std.testing.expectEqual(@as(usize, "metadata".len), source.retained_bytes);
    }
}

test "AV1 MP4 bridge retains only translated index admission on real input" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/av1-mp4.mp4") }, .identity = "bridge-integration", .admission_pool = &pool };
    {
        var reader = try openWebm(a, &source);
        defer reader.deinit();
        const new_bytes = reader.packets.len * @sizeOf(media.webm.Packet);
        const old_bytes = reader.packets.len * @sizeOf(media.mp4.Packet);
        try std.testing.expectEqual(@as(u64, new_bytes), reader.index_reservation.resources.host_bytes);
        try std.testing.expectEqual(@as(u64, reader.metadata.bytes.len + new_bytes), pool.snapshot().host_bytes);
        try std.testing.expect(pool.high_water.host_bytes >= reader.metadata.bytes.len + old_bytes + new_bytes);
    }
    try std.testing.expectEqualDeep(media.admission.Resources{}, pool.snapshot());
    try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
}
