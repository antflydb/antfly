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

//! Logical AVC presentation metadata, obtained from completed decoder assemblies.
const std = @import("std");
const media = @import("antfly_media");
const h264 = @import("h264.zig");
const dynamic = @import("h264_dynamic.zig");
const sampling = @import("sampling.zig");
pub const Options = struct { max_packets: usize = 8192, max_pictures: usize = 8192, decode: h264.Options = .{ .max_dependency_packets = 8192, .max_parameter_packets = 8192 } };
pub const Index = struct {
    allocator: std.mem.Allocator,
    pictures: []h264.Picture,
    timescale: u32,
    packet_count: usize = 0,
    pending_pts: ?i64 = null,
    pending_first_packet: ?usize = null,
    reservation: media.admission.Token = .{},
    pub fn deinit(self: *Index) void {
        self.allocator.free(self.pictures);
        self.reservation.deinit();
        self.* = undefined;
    }
    /// Own a metadata snapshot after whole-unit preroll eviction. The source
    /// owns its original allocation; this result can outlive it independently.
    pub fn cloneRebased(self: Index, a: std.mem.Allocator, base: usize, packet_count: usize, pool: ?*media.admission.Pool, control: media.source.Control) !Index {
        try control.check();
        const end = try std.math.add(usize, base, packet_count);
        if (end != self.packet_count) return error.InvalidLivePictureIndex;
        var count: usize = 0;
        for (self.pictures) |picture| {
            if (picture.first_packet >= end) return error.InvalidLivePictureIndex;
            if (picture.first_packet >= base) count += 1;
        }
        if (self.pending_first_packet) |pending| if (pending < base or pending >= end) return error.InvalidLivePictureIndex;
        const bytes = try std.math.mul(usize, count, @sizeOf(h264.Picture));
        var reservation = if (pool) |shared| try shared.acquire(.{ .host_bytes = bytes }) else media.admission.Token{};
        errdefer reservation.deinit();
        const owned = try a.alloc(h264.Picture, count);
        errdefer a.free(owned);
        var at: usize = 0;
        for (self.pictures) |picture| {
            try control.check();
            if (picture.first_packet < base) continue;
            owned[at] = picture;
            owned[at].first_packet -= base;
            at += 1;
        }
        return .{ .allocator = a, .pictures = owned, .timescale = self.timescale, .packet_count = packet_count, .pending_pts = self.pending_pts, .pending_first_packet = if (self.pending_first_packet) |pending| pending - base else null, .reservation = reservation };
    }
    /// Return transport selectors for logical pictures in presentation order.
    /// The first member selects the complete field/partition assembly on replay.
    pub fn select(self: Index) ![]usize {
        if (self.pictures.len == 0 or self.timescale == 0) return error.EmptyVideo;
        const order = try self.allocator.alloc(usize, self.pictures.len);
        defer self.allocator.free(order);
        for (order, 0..) |*id, i| id.* = i;
        const Less = struct {
            fn less(pictures: []const h264.Picture, l: usize, r: usize) bool {
                return pictures[l].pts < pictures[r].pts or (pictures[l].pts == pictures[r].pts and pictures[l].first_packet < pictures[r].first_packet);
            }
        };
        std.mem.sort(usize, order, @as([]const h264.Picture, self.pictures), Less.less);
        const first = self.pictures[order[0]].pts;
        const last = self.pictures[order[order.len - 1]];
        const span = @as(i128, last.pts) + last.duration - first;
        if (span <= 0) return error.InvalidSamplingMetadata;
        const seconds = @as(f64, @floatFromInt(span)) / @as(f64, @floatFromInt(self.timescale));
        const selected = try sampling.embeddingGemma2(self.allocator, .{ .total_frames = order.len, .fps = @as(f64, @floatFromInt(order.len)) / seconds, .duration_seconds = seconds }, .{});
        for (selected) |*id| id.* = self.pictures[order[id.*]].first_packet;
        return selected;
    }
};
const Collector = struct {
    allocator: std.mem.Allocator,
    pictures: std.ArrayList(h264.Picture) = .empty,
    planes: std.ArrayList(u8) = .empty,
    limit: usize,
    pending_pts: ?i64 = null,
    pending_first_packet: ?usize = null,
    fn pending(raw: *anyopaque, picture: h264.PendingPicture) void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        self.pending_pts = if (self.pending_pts) |old| @min(old, picture.pts) else picture.pts;
        self.pending_first_packet = if (self.pending_first_packet) |old| @min(old, picture.first_packet) else picture.first_packet;
    }
    fn accept(raw: *anyopaque, picture: h264.Picture) !void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        if (picture.independent_plane) |plane| {
            const mask = @as(u8, 1) << @intCast(plane);
            for (self.pictures.items, self.planes.items, 0..) |existing, seen, i| {
                if (existing.first_packet != picture.first_packet) continue;
                if (seen & mask != 0 or existing.pts != picture.pts or existing.duration != picture.duration or existing.width != picture.width or existing.height != picture.height or existing.bit_depth != picture.bit_depth) return error.MixedVideoPictures;
                self.planes.items[i] |= mask;
                return;
            }
        }
        if (self.pictures.items.len >= self.limit) return error.ResourceLimitExceeded;
        var result = picture;
        if (picture.independent_plane != null) result.chroma_format = 3;
        result.independent_plane = null;
        try self.pictures.append(self.allocator, result);
        try self.planes.append(self.allocator, if (picture.independent_plane) |plane| @as(u8, 1) << @intCast(plane) else 0);
    }
};
fn codedEnds(reader: *media.mp4.Reader, begin: usize, end: usize, max_packet: usize) !?[2]usize {
    var first: ?usize = null;
    var last: usize = 0;
    for (begin..end) |i| {
        try reader.input.control.check();
        if (reader.packets[i].size > max_packet) return error.ResourceLimitExceeded;
        var packet = try reader.readPacket(i);
        defer packet.deinit();
        try @import("avc.zig").validatePortablePacket(packet.bytes, reader.track.nal_length_bytes);
        var at: usize = 0;
        while (at < packet.bytes.len) {
            var size: usize = 0;
            for (packet.bytes[at..][0..reader.track.nal_length_bytes]) |byte| size = (size << 8) | byte;
            at += reader.track.nal_length_bytes;
            const kind = packet.bytes[at] & 31;
            if (kind == 1 or kind == 2 or kind == 5) {
                if (first == null) first = i;
                last = i;
            }
            at += size;
        }
    }
    return if (first) |packet_index| .{ packet_index, last } else null;
}
fn discard(_: *anyopaque, _: usize, _: *const h264.Frame) !void {}
pub fn index(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options) !Index {
    return indexMode(a, reader, options, false);
}
/// Completed logical pictures plus a conservative pending-picture barrier.
/// Corruption, resource failures and abandoned pictures at reset remain errors.
pub fn streaming(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options) !Index {
    return indexMode(a, reader, options, true);
}
fn indexMode(a: std.mem.Allocator, reader: *media.mp4.Reader, options: Options, partial: bool) !Index {
    if (reader.track.codec != .avc) return error.UnsupportedVideoCodec;
    if (reader.packets.len == 0) return error.EmptyVideo;
    if (options.max_pictures == 0) return error.ResourceLimitExceeded;
    if (reader.packets.len > options.max_packets) return error.ResourceLimitExceeded;
    // Account both growth buffers and transient old/new realloc owners. The
    // fixed initial growth allowance also covers very small configured limits.
    const slots = try std.math.mul(usize, try std.math.add(usize, options.max_pictures, 8), 3);
    const metadata_bound = try std.math.mul(usize, slots, @sizeOf(h264.Picture) + @sizeOf(u8));
    var reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = metadata_bound }) else media.admission.Token{};
    errdefer reservation.deinit();
    var collector = Collector{ .allocator = a, .limit = options.max_pictures };
    defer collector.pictures.deinit(a);
    defer collector.planes.deinit(a);
    var decode = options.decode;
    decode.picture_observer = .{ .context = &collector, .accept = Collector.accept, .pending = if (partial) Collector.pending else null };
    if (reader.track.inband_parameter_sets) {
        const session = try dynamic.Session.init(a, reader, decode);
        defer session.deinit();
        var selectors: std.ArrayList(usize) = .empty;
        defer selectors.deinit(a);
        for (session.epochs.items) |epoch| if (try codedEnds(reader, epoch.start, epoch.end, decode.max_packet_bytes)) |ends| {
            try selectors.appendSlice(a, &ends);
        };
        if (selectors.items.len == 0) return error.EmptyVideo;
        _ = session.decodeSelected(selectors.items, &collector, discard) catch |err| caught: {
            if (!partial or err != error.IncompleteVideoPicture or collector.pending_first_packet == null) return err;
            break :caught h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 };
        };
    } else {
        const ends = try codedEnds(reader, 0, reader.packets.len, decode.max_packet_bytes) orelse return error.EmptyVideo;
        _ = h264.decodeSelected(a, reader, &ends, decode, &collector, discard) catch |err| caught: {
            if (!partial or err != error.IncompleteVideoPicture or collector.pending_first_packet == null) return err;
            break :caught h264.Statistics{ .decoded_packets = 0, .payload_bytes = 0, .decode_high_water = 0 };
        };
    }
    var kept: usize = 0;
    for (collector.pictures.items, collector.planes.items) |picture, mask| {
        if (mask != 0 and mask != 7) {
            if (!partial) return error.IncompleteVideoPicture;
            Collector.pending(&collector, .{ .first_packet = picture.first_packet, .pts = picture.pts });
        } else {
            collector.pictures.items[kept] = picture;
            kept += 1;
        }
    }
    collector.pictures.items.len = kept;
    const owned = try collector.pictures.toOwnedSlice(a);
    errdefer a.free(owned);
    collector.planes.deinit(a);
    collector.planes = .empty;
    try reservation.resize(.{ .host_bytes = owned.len * @sizeOf(h264.Picture) });
    return .{ .allocator = a, .pictures = owned, .timescale = reader.track.timescale, .packet_count = reader.packets.len, .pending_pts = collector.pending_pts, .pending_first_packet = collector.pending_first_packet, .reservation = reservation };
}

test "AVC logical picture index counts field pairs partitions and configuration epochs" {
    const a = std.testing.allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-paff-interleaved-fragments-10-2", "h264-jm-partitions", "h264-dynamic-geometry", "h264-jm-separate" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        var pictures = try index(a, &reader, .{});
        defer pictures.deinit();
        try std.testing.expect(pictures.pictures.len > 0);
        var live = try streaming(a, &reader, .{});
        defer live.deinit();
        try std.testing.expectEqual(@as(?i64, null), live.pending_pts);
        try std.testing.expectEqualSlices(h264.Picture, pictures.pictures, live.pictures);
        if (std.mem.eql(u8, name, "h264-jm-partitions") or std.mem.eql(u8, name, "h264-jm-separate")) try std.testing.expectEqual(@as(usize, 4), pictures.pictures.len);
        if (std.mem.eql(u8, name, "h264-dynamic-geometry")) {
            try std.testing.expectEqual(@as(usize, 3), pictures.pictures.len);
            try std.testing.expectEqual(@as(u32, 64), pictures.pictures[0].width);
            try std.testing.expectEqual(@as(u32, 96), pictures.pictures[1].width);
            try std.testing.expectEqual(@as(u32, 80), pictures.pictures[2].width);
        }
        if (std.mem.startsWith(u8, name, "h264-paff")) try std.testing.expect(pictures.pictures.len < reader.packets.len);
        const selected = try pictures.select();
        defer a.free(selected);
        for (selected) |packet| try std.testing.expect(packet < reader.packets.len);
    }
}

test "AVC logical index bounds callback metadata and releases shared decode admission" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .identity = "logical-admission", .admission_pool = &pool, .storage = .{ .borrowed = @embedFile("../testdata/h264-paff-adaptive-packets-8-1.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const before = pool.snapshot().host_bytes;
    try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_pictures = 0 }));
    try std.testing.expectEqual(before, pool.snapshot().host_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, index(a, &reader, .{ .max_packets = 0 }));
    try std.testing.expectError(error.ResourceLimitExceeded, streaming(a, &reader, .{ .max_pictures = 0 }));
    try std.testing.expectEqual(before, pool.snapshot().host_bytes);
    var pictures = try index(a, &reader, .{});
    defer pictures.deinit();
    try std.testing.expectEqual(before + pictures.pictures.len * @sizeOf(h264.Picture), pool.snapshot().host_bytes);
}

test "AVC streaming index holds incomplete field pairs until their mate arrives" {
    const a = std.testing.allocator;
    inline for (.{ "h264-paff-adaptive-packets-8-1", "h264-paff-interleaved-fragments-10-2" }) |name| {
        var source = media.source.Source{ .allocator = a, .identity = name, .storage = .{ .borrowed = @embedFile("../testdata/" ++ name ++ ".mp4") } };
        var reader = try media.mp4.Reader.init(a, &source, .{});
        defer reader.deinit();
        const packets = reader.packets;
        defer reader.packets = packets;
        var saw_pending = false;
        var saw_completed = false;
        for (1..packets.len + 1) |end| {
            reader.packets = packets[0..end];
            var logical = try streaming(a, &reader, .{});
            defer logical.deinit();
            if (logical.pending_pts != null) {
                saw_pending = true;
                try std.testing.expect(logical.pending_first_packet.? < end);
                if (end == 1) try std.testing.expectError(error.IncompleteVideoPicture, index(a, &reader, .{}));
            }
            saw_completed = saw_completed or logical.pictures.len != 0;
            if (end == packets.len) try std.testing.expectEqual(@as(?i64, null), logical.pending_pts);
        }
        try std.testing.expect(saw_pending and saw_completed);
    }
}

test "AVC streaming index rejects corrupt NALs rather than manufacturing a pending tail" {
    const a = std.testing.allocator;
    const bytes = try a.dupe(u8, @embedFile("../testdata/h264-paff-adaptive-packets-8-1.mp4"));
    defer a.free(bytes);
    var source = media.source.Source{ .allocator = a, .identity = "corrupt-tail", .storage = .{ .borrowed = bytes } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const header = @as(usize, @intCast(reader.packets[0].offset)) + reader.track.nal_length_bytes;
    bytes[header] |= 128;
    try std.testing.expectError(error.MalformedVideoPacket, streaming(a, &reader, .{}));
}

test "AVC logical snapshots rebase retained suffix and keep independent admission" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 256 * 1024 * 1024 } };
    var source = media.source.Source{ .allocator = a, .identity = "snapshot", .admission_pool = &pool, .storage = .{ .borrowed = @embedFile("../testdata/h264-dynamic-geometry.mp4") } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const before = pool.snapshot().host_bytes;
    var original = try streaming(a, &reader, .{});
    var original_live = true;
    defer if (original_live) original.deinit();
    const base = original.pictures[1].first_packet;
    var copy = try original.cloneRebased(a, base, reader.packets.len - base, &pool, .{});
    defer copy.deinit();
    try std.testing.expectEqual(@as(usize, 2), copy.pictures.len);
    try std.testing.expectEqual(@as(usize, 0), copy.pictures[0].first_packet);
    try std.testing.expectEqual(@as(u32, 96), copy.pictures[0].width);
    try std.testing.expectError(error.InvalidLivePictureIndex, original.cloneRebased(a, 0, reader.packets.len - 1, &pool, .{}));
    original.deinit();
    original_live = false;
    try std.testing.expectEqual(before + copy.pictures.len * @sizeOf(h264.Picture), pool.snapshot().host_bytes);
    try std.testing.expectEqual(@as(u32, 80), copy.pictures[1].width);
}
