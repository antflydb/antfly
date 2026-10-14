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

//! Immutable request-local window/frame reuse; no URL-keyed persistent cache.
//! Owned packet windows spanning finite published HLS/DASH units. No remuxing.
const std = @import("std");
const media = @import("antfly_media");
pub const Options = struct {
    stream: media.stream.Options = .{},
    max_packets: usize = 8192,
    max_units: usize = 256,
    max_identity_bytes: usize = 4096,
};
pub const Window = struct {
    snapshot: *media.stream.Snapshot,
    reader: media.mp4.Reader,
    epoch: usize,
    identity: []u8,
    identity_reservation: media.admission.Token,
    pub fn deinit(self: *Window) void {
        const a = self.reader.allocator;
        self.reader.deinit();
        self.snapshot.deinit();
        a.free(self.identity);
        self.identity_reservation.deinit();
        self.* = undefined;
    }
};
fn sameTrack(left: media.mp4.Track, right: media.mp4.Track) bool {
    var l = left;
    var r = right;
    l.avcc = &.{};
    r.avcc = &.{};
    l.color_info = &.{};
    r.color_info = &.{};
    return std.meta.eql(l, r) and std.mem.eql(u8, left.avcc, right.avcc) and std.mem.eql(u8, left.color_info, right.color_info);
}
/// Collect a finite set of published units in decode order. Include codec
/// preroll before the requested sampling interval and sufficient future packets
/// for reordered output. Epoch/config changes must form separate model windows.
/// Returned storage is independent of every input segment and packet lease.
/// A live producer must establish a presentation watermark before publishing a
/// final window; this function does not infer completeness from EXTINF or PART.
pub fn join(a: std.mem.Allocator, identity: []const u8, segments: []const media.live_mp4.Segment, options: Options) !Window {
    if (identity.len > options.max_identity_bytes) return error.ResourceLimitExceeded;
    if (segments.len == 0 or segments.len > options.max_units) return error.ResourceLimitExceeded;
    const first = segments[0];
    if (first.reader.track.timescale == 0) return error.InvalidSamplingMetadata;
    const metadata_size = try std.math.add(usize, first.reader.track.avcc.len, first.reader.track.color_info.len);
    var size = metadata_size;
    var count: usize = 0;
    var previous: ?media.mp4.Packet = null;
    for (segments) |segment| {
        try options.stream.control.check();
        if (segment.epoch != first.epoch or !sameTrack(first.reader.track, segment.reader.track)) return error.UnsupportedDynamicVideoConfig;
        if (first.dash_timing) |left| {
            const right = segment.dash_timing orelse return error.UnsupportedDynamicVideoConfig;
            if (!left.period.compatible(right.period)) return error.UnsupportedDynamicVideoConfig;
        } else if (segment.dash_timing != null) return error.UnsupportedDynamicVideoConfig;
        for (segment.reader.packets) |packet| {
            if (previous) |last| if (packet.media_dts < try std.math.add(i64, last.media_dts, last.duration)) return error.NonMonotonicMediaSegment;
            previous = packet;
            if (count >= options.max_packets) return error.ResourceLimitExceeded;
            count += 1;
            size = try std.math.add(usize, size, packet.size);
            if (size > options.stream.max_bytes) return error.ResourceLimitExceeded;
        }
    }
    if (count == 0 or size > options.stream.max_bytes) return error.EmptyVideo;
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, size, try std.math.mul(usize, count, @sizeOf(media.mp4.Packet))) }) else media.admission.Token{};
    errdefer reservation.deinit();
    var identity_reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = identity.len }) else media.admission.Token{};
    errdefer identity_reservation.deinit();
    const owned_identity = try a.dupe(u8, identity);
    errdefer a.free(owned_identity);
    const packets = try a.alloc(media.mp4.Packet, count);
    errdefer a.free(packets);
    const bytes = try a.alloc(u8, size);
    var bytes_live = true;
    defer if (bytes_live) a.free(bytes);
    @memcpy(bytes[0..first.reader.track.avcc.len], first.reader.track.avcc);
    @memcpy(bytes[first.reader.track.avcc.len..][0..first.reader.track.color_info.len], first.reader.track.color_info);
    var cursor = metadata_size;
    var index: usize = 0;
    for (segments) |segment| {
        var reader = segment.reader;
        for (reader.packets, 0..) |packet, id| {
            try options.stream.control.check();
            var payload = try reader.readPacket(id);
            defer payload.deinit();
            @memcpy(bytes[cursor..][0..payload.bytes.len], payload.bytes);
            packets[index] = packet;
            packets[index].offset = cursor;
            cursor += payload.bytes.len;
            index += 1;
        }
    }
    const snapshot = try media.stream.Snapshot.copy(a, owned_identity, &.{bytes}, options.stream);
    errdefer snapshot.deinit();
    var metadata = try snapshot.input.read(0, metadata_size);
    errdefer metadata.deinit();
    var track = first.reader.track;
    track.avcc = metadata.bytes[0..track.avcc.len];
    track.color_info = metadata.bytes[track.avcc.len..];
    a.free(bytes);
    bytes_live = false;
    try reservation.resize(.{ .host_bytes = packets.len * @sizeOf(media.mp4.Packet) });
    return .{ .snapshot = snapshot, .epoch = first.epoch, .identity = owned_identity, .identity_reservation = identity_reservation, .reader = .{ .allocator = a, .input = &snapshot.input, .metadata = metadata, .track = track, .packets = packets, .index_reservation = reservation, .limits = .{} } };
}

/// Own an equivalent reader with parameter NALs prepended to its first payload.
/// Packet selectors/clocks are unchanged. No input or bootstrap ownership escapes.
pub fn copyWithParameters(a: std.mem.Allocator, identity: []const u8, reader: *media.mp4.Reader, epoch: usize, parameters: []const u8, options: Options) !Window {
    if (identity.len > options.max_identity_bytes or reader.packets.len == 0 or reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
    if (parameters.len != 0 and (!reader.track.inband_parameter_sets or reader.track.codec != .avc)) return error.UnsupportedDynamicVideoConfig;
    const metadata_size = try std.math.add(usize, reader.track.avcc.len, reader.track.color_info.len);
    var size = try std.math.add(usize, metadata_size, parameters.len);
    for (reader.packets) |packet| size = try std.math.add(usize, size, packet.size);
    if (size > options.stream.max_bytes) return error.ResourceLimitExceeded;
    const count = reader.packets.len;
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, size, try std.math.mul(usize, count, @sizeOf(media.mp4.Packet))) }) else media.admission.Token{};
    errdefer reservation.deinit();
    var identity_reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = identity.len }) else media.admission.Token{};
    errdefer identity_reservation.deinit();
    const owned_identity = try a.dupe(u8, identity);
    errdefer a.free(owned_identity);
    const packets = try a.dupe(media.mp4.Packet, reader.packets);
    errdefer a.free(packets);
    const bytes = try a.alloc(u8, size);
    defer a.free(bytes);
    @memcpy(bytes[0..reader.track.avcc.len], reader.track.avcc);
    @memcpy(bytes[reader.track.avcc.len..][0..reader.track.color_info.len], reader.track.color_info);
    var cursor = metadata_size;
    for (packets, 0..) |*packet, i| {
        try options.stream.control.check();
        packet.offset = cursor;
        if (i == 0) {
            @memcpy(bytes[cursor..][0..parameters.len], parameters);
            cursor += parameters.len;
            packet.size = std.math.cast(u32, try std.math.add(usize, packet.size, parameters.len)) orelse return error.ResourceLimitExceeded;
        }
        var payload = try reader.readPacket(i);
        defer payload.deinit();
        @memcpy(bytes[cursor..][0..payload.bytes.len], payload.bytes);
        cursor += payload.bytes.len;
    }
    const snapshot = try media.stream.Snapshot.copy(a, owned_identity, &.{bytes}, options.stream);
    errdefer snapshot.deinit();
    var metadata = try snapshot.input.read(0, metadata_size);
    errdefer metadata.deinit();
    var track = reader.track;
    track.avcc = metadata.bytes[0..track.avcc.len];
    track.color_info = metadata.bytes[track.avcc.len..];
    try reservation.resize(.{ .host_bytes = packets.len * @sizeOf(media.mp4.Packet) });
    return .{ .snapshot = snapshot, .epoch = epoch, .identity = owned_identity, .identity_reservation = identity_reservation, .reader = .{ .allocator = a, .input = &snapshot.input, .metadata = metadata, .track = track, .packets = packets, .index_reservation = reservation, .limits = .{} } };
}
pub fn prefixParameters(window: *Window, parameters: []const u8, options: Options) !void {
    if (parameters.len == 0) return;
    const replacement = try copyWithParameters(window.reader.allocator, window.identity, &window.reader, window.epoch, parameters, options);
    window.deinit();
    window.* = replacement;
}

fn joinAllocationRun(a: std.mem.Allocator) !void {
    const file = @embedFile("../testdata/live/fragmented.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 128 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var ingest = try media.live_mp4.Ingest.init(a, "window", file[0..797], .{ .stream = .{ .admission_pool = &pool } });
    defer ingest.deinit();
    try ingest.push(file[797..11206]);
    var one = try ingest.finishSegment();
    defer one.deinit();
    try ingest.push(file[11206..20811]);
    var two = try ingest.finishSegment();
    defer two.deinit();
    var window = try join(a, "model-window", &.{ one, two }, .{ .stream = .{ .admission_pool = &pool } });
    defer window.deinit();
    try std.testing.expectEqual(one.reader.packets.len + two.reader.packets.len, window.reader.packets.len);
    var packet = try window.reader.readPacket(one.reader.packets.len);
    defer packet.deinit();
    var original = try two.reader.readPacket(0);
    defer original.deinit();
    try std.testing.expectEqualSlices(u8, original.bytes, packet.bytes);
}
test "live packet window joins units with independently owned source metadata and leases" {
    try joinAllocationRun(std.testing.allocator);
}
test "live packet window releases every failing allocation and shared reservation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, joinAllocationRun, .{});
}

test "live packet window rejects mixed epochs duplicate clocks and packet bounds" {
    const a = std.testing.allocator;
    const file = @embedFile("../testdata/live/fragmented.mp4");
    var ingest = try media.live_mp4.Ingest.init(a, "window", file[0..797], .{});
    defer ingest.deinit();
    try ingest.push(file[797..11206]);
    var one = try ingest.finishSegment();
    var one_live = true;
    defer if (one_live) one.deinit();
    try std.testing.expectError(error.NonMonotonicMediaSegment, join(a, "duplicate", &.{ one, one }, .{}));
    var changed = one;
    changed.epoch += 1;
    try std.testing.expectError(error.UnsupportedDynamicVideoConfig, join(a, "epoch", &.{ one, changed }, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, join(a, "bounded", &.{one}, .{ .max_packets = 1 }));
    var identity = "independent".*;
    var window = try join(a, &identity, &.{one}, .{});
    @memset(&identity, 'x');
    try std.testing.expectEqualStrings("independent", window.snapshot.input.identity);
    defer window.deinit();
    const configuration = try a.dupe(u8, one.reader.track.avcc);
    defer a.free(configuration);
    one.deinit();
    one_live = false;
    try std.testing.expectEqualSlices(u8, configuration, window.reader.track.avcc);
    var retained = try window.reader.readPacket(0);
    defer retained.deinit();
    try std.testing.expectEqualSlices(u8, window.snapshot.bytes[@intCast(window.reader.packets[0].offset)..][0..window.reader.packets[0].size], retained.bytes);
}
