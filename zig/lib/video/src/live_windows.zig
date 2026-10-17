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
    snapshot: ?*media.stream.Snapshot,
    overlay: ?*ParameterOverlay = null,
    reader: media.mp4.Reader,
    epoch: usize,
    identity: []u8,
    identity_reservation: media.admission.Token,
    pub fn deinit(self: *Window) void {
        const a = self.reader.allocator;
        self.reader.deinit();
        if (self.snapshot) |snapshot| snapshot.deinit();
        if (self.overlay) |overlay| overlay.deinit();
        a.free(self.identity);
        self.identity_reservation.deinit();
        self.* = undefined;
    }
};
/// Metadata-only view of an immutable request source. The source and its
/// metadata outlive this window. Range providers remain demand-read; no coded
/// bytes are materialized, and packet offsets keep their original meaning.
pub fn borrow(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options) !Window {
    if (reader.input.identity.len > options.max_identity_bytes or reader.packets.len == 0 or reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(usize, reader.packets.len, @sizeOf(media.mp4.Packet)) }) else media.admission.Token{};
    errdefer reservation.deinit();
    const packets = try a.dupe(media.mp4.Packet, reader.packets);
    errdefer a.free(packets);
    var identity_reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = reader.input.identity.len }) else media.admission.Token{};
    errdefer identity_reservation.deinit();
    const identity = try a.dupe(u8, reader.input.identity);
    errdefer a.free(identity);
    var view = reader.*;
    view.allocator = a;
    view.packets = packets;
    view.metadata = .{ .bytes = &.{}, .source = null };
    view.index_reservation = reservation;
    return .{ .snapshot = null, .reader = view, .epoch = 0, .identity = identity, .identity_reservation = identity_reservation };
}
const ParameterOverlay = struct {
    allocator: std.mem.Allocator,
    input: media.source.Source,
    original: *media.source.Source,
    first: media.mp4.Packet,
    prefix: []u8,
    reservation: media.admission.Token,
    fn read(raw: *anyopaque, offset: u64, destination: []u8, control: media.source.Control) !usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try control.check();
        const boundary = self.original.length();
        if (offset < boundary) {
            const count: usize = @intCast(@min(destination.len, boundary - offset));
            var lease = try self.original.read(offset, count);
            defer lease.deinit();
            @memcpy(destination[0..count], lease.bytes);
            return count;
        }
        const relative = offset - boundary;
        if (relative < self.prefix.len) {
            const count: usize = @intCast(@min(destination.len, self.prefix.len - relative));
            @memcpy(destination[0..count], self.prefix[@intCast(relative)..][0..count]);
            return count;
        }
        const payload_offset = relative - self.prefix.len;
        if (payload_offset > self.first.size) return error.UnexpectedEndOfSource;
        const count: usize = @intCast(@min(destination.len, self.first.size - payload_offset));
        var lease = try self.original.read(try std.math.add(u64, self.first.offset, payload_offset), count);
        defer lease.deinit();
        @memcpy(destination[0..count], lease.bytes);
        return count;
    }
    fn deinit(self: *@This()) void {
        std.debug.assert(self.input.retained_bytes == 0);
        self.allocator.free(self.prefix);
        self.reservation.deinit();
        self.allocator.destroy(self);
    }
};
/// Dynamic SPS/PPS bootstrap overlays only the first packet. Remaining coded
/// packets still borrow the original immutable source, including remote ranges.
pub fn borrowWithParameters(a: std.mem.Allocator, reader: *media.mp4.Reader, parameters: []const u8, options: Options) !Window {
    var window = try borrow(a, reader, options);
    errdefer window.deinit();
    if (parameters.len == 0) return window;
    if (!reader.track.inband_parameter_sets or reader.track.codec != .avc) return error.UnsupportedDynamicVideoConfig;
    const first = reader.packets[0];
    const size = try std.math.add(usize, parameters.len, first.size);
    if (size > options.stream.max_bytes or size > reader.input.limits.max_read_bytes) return error.ResourceLimitExceeded;
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = parameters.len + @sizeOf(ParameterOverlay) }) else media.admission.Token{};
    errdefer reservation.deinit();
    const prefix = try a.dupe(u8, parameters);
    errdefer a.free(prefix);
    const overlay = try a.create(ParameterOverlay);
    errdefer a.destroy(overlay);
    overlay.* = .{ .allocator = a, .original = reader.input, .first = first, .prefix = prefix, .reservation = reservation, .input = .{ .allocator = a, .identity = reader.input.identity, .limits = reader.input.limits, .control = options.stream.control, .admission_pool = options.stream.admission_pool, .storage = .{ .range = .{ .context = overlay, .read_at = ParameterOverlay.read, .length = try std.math.add(u64, reader.input.length(), size) } } } };
    window.reader.packets[0].size = std.math.cast(u32, size) orelse return error.ResourceLimitExceeded;
    window.overlay = overlay;
    window.reader.input = &overlay.input;
    window.reader.packets[0].offset = reader.input.length();
    return window;
}
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
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(usize, count, @sizeOf(media.mp4.Packet)) }) else media.admission.Token{};
    errdefer reservation.deinit();
    var identity_reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = identity.len }) else media.admission.Token{};
    errdefer identity_reservation.deinit();
    const owned_identity = try a.dupe(u8, identity);
    errdefer a.free(owned_identity);
    const packets = try a.alloc(media.mp4.Packet, count);
    errdefer a.free(packets);
    const snapshot = try media.stream.Snapshot.allocate(a, owned_identity, size, options.stream);
    errdefer snapshot.deinit();
    const bytes = snapshot.bytes;
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
    var metadata = try snapshot.input.read(0, metadata_size);
    errdefer metadata.deinit();
    var track = first.reader.track;
    track.avcc = metadata.bytes[0..track.avcc.len];
    track.color_info = metadata.bytes[track.avcc.len..];
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
    var reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.mul(usize, count, @sizeOf(media.mp4.Packet)) }) else media.admission.Token{};
    errdefer reservation.deinit();
    var identity_reservation = if (options.stream.admission_pool) |pool| try pool.acquire(.{ .host_bytes = identity.len }) else media.admission.Token{};
    errdefer identity_reservation.deinit();
    const owned_identity = try a.dupe(u8, identity);
    errdefer a.free(owned_identity);
    const packets = try a.dupe(media.mp4.Packet, reader.packets);
    errdefer a.free(packets);
    const snapshot = try media.stream.Snapshot.allocate(a, owned_identity, size, options.stream);
    errdefer snapshot.deinit();
    const bytes = snapshot.bytes;
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
    try std.testing.expectEqualStrings("independent", window.snapshot.?.input.identity);
    defer window.deinit();
    const configuration = try a.dupe(u8, one.reader.track.avcc);
    defer a.free(configuration);
    one.deinit();
    one_live = false;
    try std.testing.expectEqualSlices(u8, configuration, window.reader.track.avcc);
    var retained = try window.reader.readPacket(0);
    defer retained.deinit();
    try std.testing.expectEqualSlices(u8, window.snapshot.?.bytes[@intCast(window.reader.packets[0].offset)..][0..window.reader.packets[0].size], retained.bytes);
}

fn borrowedOverlayAllocationRun(a: std.mem.Allocator) !void {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 128 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "overlay", .storage = .{ .borrowed = @embedFile("../testdata/live/fragmented.mp4") }, .admission_pool = &pool };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, borrow(a, &reader, .{ .max_identity_bytes = 0 }));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var separate = try borrow(arena.allocator(), &reader, .{});
    separate.deinit();
    reader.track.inband_parameter_sets = true;
    const prefix = [_]u8{ 0, 0, 0, 1, 0x67 };
    var expected = try copyWithParameters(a, "overlay", &reader, 0, &prefix, .{ .stream = .{ .admission_pool = &pool } });
    defer expected.deinit();
    var borrowed = try borrowWithParameters(a, &reader, &prefix, .{ .stream = .{ .admission_pool = &pool } });
    defer borrowed.deinit();
    try std.testing.expect(borrowed.snapshot == null);
    for (reader.packets, 0..) |_, i| {
        var wanted = try expected.reader.readPacket(i);
        defer wanted.deinit();
        var actual = try borrowed.reader.readPacket(i);
        defer actual.deinit();
        try std.testing.expectEqualSlices(u8, wanted.bytes, actual.bytes);
    }
}
test "live packet window borrowed bootstrap matches materialization and releases every failed allocation" {
    try borrowedOverlayAllocationRun(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, borrowedOverlayAllocationRun, .{});
}

test "live packet window fills one final backing allocation within exact admission" {
    const a = std.testing.allocator;
    const file = @embedFile("../testdata/live/fragmented.mp4");
    var ingest = try media.live_mp4.Ingest.init(a, "window", file[0..797], .{});
    defer ingest.deinit();
    try ingest.push(file[797..11206]);
    var one = try ingest.finishSegment();
    defer one.deinit();
    const identity = "single-backing";
    var size: usize = one.reader.track.avcc.len + one.reader.track.color_info.len;
    for (one.reader.packets) |packet| size += packet.size;
    const exact = size + @sizeOf(media.stream.Snapshot) + identity.len + one.reader.packets.len * @sizeOf(media.mp4.Packet) + one.reader.track.avcc.len + one.reader.track.color_info.len;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = exact } };
    var window = try join(a, identity, &.{one}, .{ .stream = .{ .admission_pool = &pool } });
    try std.testing.expectEqual(@as(u64, @intCast(exact)), pool.high_water.host_bytes);
    window.deinit();
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
}
