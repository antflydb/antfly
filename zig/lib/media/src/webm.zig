// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded VP8/VP9/AV1 WebM index. Payloads remain in Source; decoding is separate.
const std = @import("std");
const ebml = @import("ebml.zig");
const source = @import("source.zig");
pub const Codec = enum { vp8, vp9, av1 };
pub const Limits = struct {
    max_metadata_bytes: usize = 16 * 1024 * 1024,
    max_packets: usize = 500_000,
    max_index_bytes: usize = 32 * 1024 * 1024,
    max_elements: usize = 1_000_000,
    max_packet_bytes: usize = 16 * 1024 * 1024,
    max_dimension: u64 = 16_384,
};
pub const Track = struct {
    number: u64,
    codec: Codec,
    width: u32,
    height: u32,
    codec_private: []const u8 = &.{},
    default_duration_ns: ?u64 = null,
};
pub const Packet = struct {
    offset: u64,
    size: usize,
    /// Signed presentation nanoseconds; no invented decode timestamp.
    pts: i64,
    duration_ns: ?u64,
    /// Container hint. Codec-specific reference inspection remains separate.
    sync: bool,
    invisible: bool,
    discardable: bool,
};
const Element = struct { id: u32, payload: u64, end: u64, unknown: bool };
const Scan = struct {
    input: *source.Source,
    limits: Limits,
    elements: usize = 0,
    fn header(self: *Scan, offset: u64, parent_end: u64) !Element {
        try self.input.control.check();
        if (offset >= parent_end or parent_end > self.input.length()) return error.MalformedMedia;
        if (self.elements >= self.limits.max_elements) return error.ResourceLimitExceeded;
        self.elements += 1;
        var lease = try self.input.read(offset, @intCast(@min(12, parent_end - offset)));
        defer lease.deinit();
        const id = try ebml.readElementId(lease.bytes, 0);
        const size = try ebml.readVint(lease.bytes, id.len);
        const payload = offset + id.len + size.len;
        const end = if (size.is_unknown) parent_end else try std.math.add(u64, payload, size.value);
        if (end > parent_end) return error.MalformedMedia;
        return .{ .id = @intCast(id.value), .payload = payload, .end = end, .unknown = size.is_unknown };
    }
    fn uint(self: *Scan, element: Element) !u64 {
        if (element.unknown or element.end - element.payload > 8) return error.MalformedMedia;
        var lease = try self.input.read(element.payload, @intCast(element.end - element.payload));
        defer lease.deinit();
        return ebml.readUint(lease.bytes);
    }
};
pub const Reader = struct {
    allocator: std.mem.Allocator,
    input: *source.Source,
    metadata: source.Lease,
    index_reservation: @import("admission.zig").Token = .{},
    track: Track,
    packets: []Packet,
    timestamp_scale_ns: u64,
    pub fn init(allocator: std.mem.Allocator, input: *source.Source, limits: Limits) !Reader {
        var index_reservation = if (input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = limits.max_index_bytes }) else @import("admission.zig").Token{};
        errdefer index_reservation.deinit();
        var scan = Scan{ .input = input, .limits = limits };
        var cursor: u64 = 0;
        const file_header = try scan.header(cursor, input.length());
        if (file_header.id != 0x1a45dfa3 or file_header.unknown) return error.MalformedMedia;
        // This lane requires a WebM document, not arbitrary Matroska codecs.
        var doc_type = false;
        cursor = file_header.payload;
        while (cursor < file_header.end) {
            const child = try scan.header(cursor, file_header.end);
            cursor = child.end;
            if (child.id == 0x4282) {
                if (child.end - child.payload != 4) return error.UnsupportedContainer;
                var lease = try input.read(child.payload, 4);
                defer lease.deinit();
                if (!std.mem.eql(u8, lease.bytes, "webm")) return error.UnsupportedContainer;
                doc_type = true;
            }
        }
        if (!doc_type) return error.MalformedMedia;
        const segment = try scan.header(file_header.end, input.length());
        if (segment.id != 0x18538067 or segment.end != input.length()) return error.UnsupportedContainer;
        var metadata: ?source.Lease = null;
        errdefer if (metadata) |*lease| lease.deinit();
        var scale: u64 = 1_000_000;
        var info_seen = false;
        cursor = segment.payload;
        while (cursor < segment.end) {
            const child = try scan.header(cursor, segment.end);
            cursor = child.end;
            if (child.unknown) return error.UnsupportedUnknownClusterSize;
            if (child.id == 0x1654ae6b) {
                if (metadata != null or child.end - child.payload > limits.max_metadata_bytes) return error.ResourceLimitExceeded;
                metadata = try input.read(child.payload, @intCast(child.end - child.payload));
            } else if (child.id == 0x1549a966) {
                if (info_seen) return error.MalformedMedia;
                if (child.end - child.payload > limits.max_metadata_bytes) return error.ResourceLimitExceeded;
                info_seen = true;
                var nested = child.payload;
                while (nested < child.end) {
                    const value = try scan.header(nested, child.end);
                    nested = value.end;
                    if (value.id == 0x2ad7b1) scale = try scan.uint(value);
                }
            }
        }
        if (scale == 0 or scale > std.math.maxInt(i64)) return error.UnsupportedTimeline;
        const track = try parseTracks((metadata orelse return error.UnsupportedVideoCodec).bytes, limits, input.control);
        var packets: std.ArrayList(Packet) = .empty;
        errdefer packets.deinit(allocator);
        cursor = segment.payload;
        while (cursor < segment.end) {
            const cluster = try scan.header(cursor, segment.end);
            cursor = cluster.end;
            if (cluster.id != 0x1f43b675) continue;
            var timestamp: ?u64 = null;
            var nested = cluster.payload;
            while (nested < cluster.end) {
                const child = try scan.header(nested, cluster.end);
                nested = child.end;
                if (child.id == 0xe7) {
                    if (timestamp != null) return error.MalformedMedia;
                    timestamp = try scan.uint(child);
                }
            }
            const time = timestamp orelse return error.MalformedMedia;
            nested = cluster.payload;
            while (nested < cluster.end) {
                const child = try scan.header(nested, cluster.end);
                nested = child.end;
                if (child.id == 0xa3) try appendBlock(allocator, &scan, &packets, track, child, time, scale, null, true) else if (child.id == 0xa0) {
                    var block: ?Element = null;
                    var duration: ?u64 = null;
                    var referenced = false;
                    var group = child.payload;
                    while (group < child.end) {
                        const entry = try scan.header(group, child.end);
                        group = entry.end;
                        switch (entry.id) {
                            0xa1 => {
                                if (block != null) return error.MalformedMedia;
                                block = entry;
                            },
                            0x9b => duration = try std.math.mul(u64, try scan.uint(entry), scale),
                            0xfb => referenced = true,
                            0xa4 => return error.UnsupportedDynamicVideoConfig,
                            else => {},
                        }
                    }
                    try appendBlock(allocator, &scan, &packets, track, block orelse return error.MalformedMedia, time, scale, duration, !referenced);
                }
            }
        }
        if (packets.items.len == 0) return error.EmptyVideoTrack;
        return .{ .index_reservation = index_reservation, .allocator = allocator, .input = input, .metadata = metadata.?, .track = track, .packets = try packets.toOwnedSlice(allocator), .timestamp_scale_ns = scale };
    }
    pub fn deinit(self: *Reader) void {
        self.allocator.free(self.packets);
        self.metadata.deinit();
        self.index_reservation.deinit();
        self.* = undefined;
    }
    pub fn readPacket(self: *Reader, index: usize) !source.Lease {
        if (index >= self.packets.len) return error.InvalidPacketIndex;
        const packet = self.packets[index];
        return self.input.read(packet.offset, packet.size);
    }
};
fn parseTracks(bytes: []const u8, limits: Limits, control: source.Control) !Track {
    var elements: usize = 0;
    var cursor: usize = 0;
    var count: usize = 0;
    while (cursor < bytes.len) {
        try control.check();
        elements += 1;
        if (elements > limits.max_elements) return error.ResourceLimitExceeded;
        const entry = try ebml.readElementHeader(bytes, cursor);
        cursor = entry.data_end orelse return error.MalformedMedia;
        if (entry.id != 0xae) continue;
        count += 1;
        if (count > 64) return error.ResourceLimitExceeded;
        const fields = try ebml.elementPayload(bytes, entry);
        var number: u64 = 0;
        var kind: u64 = 0;
        var codec_id: []const u8 = &.{};
        var private: []const u8 = &.{};
        var width: u64 = 0;
        var height: u64 = 0;
        var duration: ?u64 = null;
        var encoded = false;
        var nested: usize = 0;
        while (nested < fields.len) {
            try control.check();
            elements += 1;
            if (elements > limits.max_elements) return error.ResourceLimitExceeded;
            const field = try ebml.readElementHeader(fields, nested);
            nested = field.data_end orelse return error.MalformedMedia;
            const value = try ebml.elementPayload(fields, field);
            switch (field.id) {
                0xd7 => number = try ebml.readUint(value),
                0x83 => kind = try ebml.readUint(value),
                0x86 => codec_id = value,
                0x63a2 => private = value,
                0x23e383 => duration = try ebml.readUint(value),
                0x6d80 => encoded = true,
                0x23314f => {
                    if (try ebml.readFloat(value) != 1) return error.UnsupportedTimeline;
                },
                0xe0 => {
                    var geometry: usize = 0;
                    while (geometry < value.len) {
                        try control.check();
                        elements += 1;
                        if (elements > limits.max_elements) return error.ResourceLimitExceeded;
                        const dimension = try ebml.readElementHeader(value, geometry);
                        geometry = dimension.data_end orelse return error.MalformedMedia;
                        const data = try ebml.elementPayload(value, dimension);
                        if (dimension.id == 0xb0) width = try ebml.readUint(data);
                        if (dimension.id == 0xba) height = try ebml.readUint(data);
                        if (dimension.id == 0x9a and try ebml.readUint(data) != 0 and try ebml.readUint(data) != 2) return error.UnsupportedInterlacedVideo;
                    }
                },
                else => {},
            }
        }
        if (kind != 1) continue;
        const codec: Codec = if (std.mem.eql(u8, codec_id, "V_VP8")) .vp8 else if (std.mem.eql(u8, codec_id, "V_VP9")) .vp9 else if (std.mem.eql(u8, codec_id, "V_AV1")) .av1 else continue;
        if (encoded) return error.UnsupportedEncryptedMedia;
        if (number == 0 or width == 0 or height == 0 or width > limits.max_dimension or height > limits.max_dimension) return error.ResourceLimitExceeded;
        if (duration != null and duration.? == 0) return error.UnsupportedTimeline;
        return .{ .number = number, .codec = codec, .width = std.math.cast(u32, width) orelse return error.ResourceLimitExceeded, .height = std.math.cast(u32, height) orelse return error.ResourceLimitExceeded, .codec_private = private, .default_duration_ns = duration };
    }
    return error.UnsupportedVideoCodec;
}
fn appendBlock(allocator: std.mem.Allocator, scan: *Scan, packets: *std.ArrayList(Packet), track: Track, element: Element, time: u64, scale: u64, duration: ?u64, group_sync: bool) !void {
    if (element.unknown or element.end - element.payload < 4) return error.MalformedMedia;
    var lease = try scan.input.read(element.payload, @intCast(@min(12, element.end - element.payload)));
    defer lease.deinit();
    const number = try ebml.readVint(lease.bytes, 0);
    if (number.is_unknown) return error.MalformedMedia;
    if (number.value != track.number) return;
    if (lease.bytes.len < number.len + 3) return error.MalformedMedia;
    const flags = lease.bytes[number.len + 2];
    if (flags & 6 != 0) return error.UnsupportedVideoLacing;
    const relative = std.mem.readInt(i16, lease.bytes[number.len..][0..2], .big);
    const ticks = std.math.add(i128, time, relative) catch return error.TimestampOverflow;
    const nanoseconds = std.math.mul(i128, ticks, scale) catch return error.TimestampOverflow;
    const pts = std.math.cast(i64, nanoseconds) orelse return error.TimestampOverflow;
    const offset = element.payload + number.len + 3;
    const size = std.math.cast(usize, element.end - offset) orelse return error.ResourceLimitExceeded;
    if (size == 0 or size > scan.limits.max_packet_bytes or packets.items.len >= scan.limits.max_packets or packets.items.len + 1 > scan.limits.max_index_bytes / @sizeOf(Packet)) return error.ResourceLimitExceeded;
    if (packets.items.len == packets.capacity) {
        const capacity = @min(scan.limits.max_index_bytes / @sizeOf(Packet), @min(scan.limits.max_packets, @max(@as(usize, 16), try std.math.mul(usize, packets.capacity, 2))));
        try packets.ensureTotalCapacityPrecise(allocator, capacity);
    }
    packets.appendAssumeCapacity(.{ .offset = offset, .size = size, .pts = pts, .duration_ns = duration orelse track.default_duration_ns, .sync = if (element.id == 0xa3) flags & 0x80 != 0 else group_sync, .invisible = flags & 8 != 0, .discardable = flags & 1 != 0 });
}

test "webm rejects timestamp multiplication overflow without trapping" {
    var input = source.Source{ .allocator = std.testing.allocator, .identity = "overflow", .storage = .{ .borrowed = &.{ 0x81, 0, 1, 0x80, 0 } } };
    var scan = Scan{ .input = &input, .limits = .{} };
    var packets: std.ArrayList(Packet) = .empty;
    defer packets.deinit(std.testing.allocator);
    try std.testing.expectError(error.TimestampOverflow, appendBlock(std.testing.allocator, &scan, &packets, .{ .number = 1, .codec = .vp9, .width = 16, .height = 16 }, .{ .id = 0xa3, .payload = 0, .end = 5, .unknown = false }, std.math.maxInt(u64), std.math.maxInt(i64), null, true));
    try std.testing.expectEqual(@as(usize, 0), input.retained_bytes);
}
