// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded static and fragmented MP4 packet indexes. No codec decoding.
const std = @import("std");
const iso = @import("isobmff.zig");
const source = @import("source.zig");
const timeline = @import("timeline.zig");
const tag = iso.fourcc;

pub const Limits = struct {
    max_metadata_bytes: usize = 16 * 1024 * 1024,
    max_index_bytes: usize = 32 * 1024 * 1024,
    max_samples: usize = 500_000,
    max_boxes: usize = 100_000,
    max_tracks: usize = 64,
    max_packet_bytes: u32 = 16 * 1024 * 1024,
    max_dimension: u32 = 16_384,
};
pub const Codec = enum { avc, mjpeg };
pub const Track = struct {
    codec: Codec = .avc,
    inband_parameter_sets: bool = false,
    id: u32,
    timescale: u32,
    width: u16,
    height: u16,
    /// AVC-only, borrowed from retained metadata; empty for complete MJPEG samples.
    avcc: []const u8,
    nal_length_bytes: u3,
    pixel_aspect: struct { horizontal: u32 = 1, vertical: u32 = 1 } = .{},
    /// Raw colr payload; downstream preparation must qualify its color policy.
    color_info: []const u8 = &.{},
    edit: timeline.Edit = .{},
    /// Decode scheduling preroll for signed negative ctts; media_dts keeps the
    /// unshifted stts clock while source dts starts early enough for reordering.
    decode_preroll_ticks: u32 = 0,
    /// Exact tkhd display matrix; interpretation belongs to display preparation.
    display_matrix: [9]i32,
};
pub const Packet = struct {
    offset: u64,
    size: u32,
    dts: i64,
    media_dts: i64 = 0,
    media_pts: i64,
    /// Source presentation ticks in Track.timescale, including the edit mapping.
    pts: i64,
    duration: u32,
    /// Container sync hint, not proof that an open GOP can start here.
    sync: bool,
};
const Span = struct { start: u64, end: u64 };
const Fragment = struct { offset: u64, size: usize };
const Defaults = struct { description: u32 = 1, duration: u32 = 0, size: u32 = 0, flags: u32 = 0 };
const Tables = struct {
    defaults: ?Defaults = null,
    track: ?Track = null,
    sizes: []const u8 = &.{},
    stsc: []const u8 = &.{},
    offsets: []const u8 = &.{},
    stts: []const u8 = &.{},
    ctts: []const u8 = &.{},
    stss: []const u8 = &.{},
    offset64: bool = false,
    compact: bool = false,
    elst: []const u8 = &.{},
    id: u32 = 0,
    scale: u32 = 0,
    handler: u32 = 0,
    matrix: [9]i32 = @splat(0),
    description_seen: bool = false,
    self_contained: bool = false,
};

/// Reader and source must remain at stable addresses while packet leases live.
/// Metadata/config stays valid until deinit; readPacket returns independent leases.
pub const Reader = struct {
    allocator: std.mem.Allocator,
    input: *source.Source,
    metadata: source.Lease,
    index_reservation: @import("admission.zig").Token = .{},
    track: Track,
    packets: []Packet,
    limits: Limits,

    pub fn init(allocator: std.mem.Allocator, input: *source.Source, limits: Limits) !Reader {
        var index_reservation = if (input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = limits.max_index_bytes }) else @import("admission.zig").Token{};
        errdefer index_reservation.deinit();
        var metadata: ?source.Lease = null;
        errdefer if (metadata) |*lease| lease.deinit();
        var spans: std.ArrayList(Span) = .empty;
        defer spans.deinit(allocator);
        var fragments: std.ArrayList(Fragment) = .empty;
        defer fragments.deinit(allocator);
        var fragment_bytes: u64 = 0;
        var cursor: u64 = 0;
        var boxes: usize = 0;
        while (cursor < input.length()) {
            try input.control.check();
            if (boxes >= limits.max_boxes) return error.ResourceLimitExceeded;
            boxes += 1;
            var header = try input.read(cursor, 8);
            var size: u64 = u32be(header.bytes[0..4]);
            const typ = u32be(header.bytes[4..8]);
            header.deinit();
            var header_size: u64 = 8;
            if (size == 1) {
                var extended = try input.read(cursor + 8, 8);
                size = u64be(extended.bytes);
                extended.deinit();
                header_size = 16;
            } else if (size == 0) size = input.length() - cursor;
            if (size < header_size or size > input.length() - cursor) return error.MalformedMedia;
            if (typ == tag("moof")) {
                fragment_bytes = try std.math.add(u64, fragment_bytes, size - header_size);
                if (fragment_bytes > limits.max_metadata_bytes or fragments.items.len >= 1024) return error.ResourceLimitExceeded;
                try fragments.append(allocator, .{ .offset = cursor, .size = @intCast(size) });
            }
            if (typ == tag("moov")) {
                if (metadata != null) return error.MalformedMedia;
                if (size - header_size > limits.max_metadata_bytes) return error.ResourceLimitExceeded;
                metadata = try input.read(cursor + header_size, @intCast(size - header_size));
            } else if (typ == tag("mdat")) {
                if (spans.items.len >= 1024) return error.ResourceLimitExceeded;
                try spans.append(allocator, .{ .start = cursor + header_size, .end = cursor + size });
            }
            cursor += size;
        }
        if (metadata == null) return error.MissingMovieMetadata;
        var parser = Parser{ .limits = limits, .control = input.control };
        const tables = try parser.movie(metadata.?.bytes);
        var track = tables.track orelse return error.UnsupportedVideoCodec;
        track.edit = try parseEdit(tables.elst, parser.movie_scale, track.timescale);
        track.decode_preroll_ticks = try decodePreroll(tables.ctts);
        const packets = if (tables.defaults) |defaults| blk: {
            if (tables.sizes.len < 12 or u32be(tables.sizes[8..12]) != 0) return error.UnsupportedHybridMp4;
            const result = try buildFragments(allocator, input, &track, defaults, fragments.items, spans.items, limits);
            break :blk result;
        } else blk: {
            if (fragments.items.len != 0) return error.MalformedMedia;
            break :blk try buildPackets(allocator, tables, track, spans.items, limits, input.control);
        };
        return .{ .index_reservation = index_reservation, .allocator = allocator, .input = input, .metadata = metadata.?, .track = track, .packets = packets, .limits = limits };
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
    /// Container candidate for seeking. Codec-specific open-GOP/preroll checks
    /// must refine this before a decoder accepts it.
    pub fn syncBefore(self: *const Reader, index: usize) !usize {
        if (index >= self.packets.len) return error.InvalidPacketIndex;
        var i = index;
        while (true) {
            if (self.packets[i].sync) return i;
            if (i == 0) return error.NoRandomAccessPoint;
            i -= 1;
        }
    }
};

const Parser = struct {
    limits: Limits,
    control: source.Control,
    boxes: usize = 0,
    tracks: usize = 0,
    movie_scale: u32 = 0,
    fn box(self: *Parser, bytes: []const u8, cursor: usize) !iso.Box {
        try self.control.check();
        if (self.boxes >= self.limits.max_boxes) return error.ResourceLimitExceeded;
        self.boxes += 1;
        return iso.readBox(bytes, cursor);
    }
    fn movie(self: *Parser, bytes: []const u8) !Tables {
        var cursor: usize = 0;
        // Movie timescale must not depend on mvhd/trak order.
        while (cursor < bytes.len) {
            const b = try self.box(bytes, cursor);
            if (b.typ == tag("mvhd")) self.movie_scale = try headerScale(b.payload);

            cursor = b.end;
        }
        if (self.movie_scale == 0) return error.MalformedMedia;
        var selected: ?Tables = null;
        cursor = 0;
        while (cursor < bytes.len) {
            const b = try self.box(bytes, cursor);
            if (b.typ == tag("trak")) {
                self.tracks += 1;
                if (self.tracks > self.limits.max_tracks) return error.ResourceLimitExceeded;
                var t = Tables{};
                try self.trackBoxes(b.payload, &t, 0);
                if (t.handler == tag("vide") and t.track != null and selected == null) {
                    if (t.id == 0 or t.scale == 0) return error.MalformedMedia;
                    if (!t.self_contained) return error.UnsupportedDataReference;
                    t.track.?.id = t.id;
                    t.track.?.timescale = t.scale;
                    t.track.?.display_matrix = t.matrix;
                    selected = t;
                }
            }
            cursor = b.end;
        }
        var result = selected orelse return error.UnsupportedVideoCodec;
        cursor = 0;
        while (cursor < bytes.len) {
            const b = try self.box(bytes, cursor);
            if (b.typ == tag("mvex")) {
                var nested: usize = 0;
                while (nested < b.payload.len) {
                    const child = try self.box(b.payload, nested);
                    if (child.typ == tag("trex")) {
                        const p = child.payload;
                        if (p.len != 24 or u32be(p[0..4]) != 0) return error.MalformedMedia;
                        if (u32be(p[4..8]) == result.id) {
                            if (result.defaults != null) return error.MalformedMedia;
                            result.defaults = .{ .description = u32be(p[8..12]), .duration = u32be(p[12..16]), .size = u32be(p[16..20]), .flags = u32be(p[20..24]) };
                        }
                    }
                    nested = child.end;
                }
                if (result.defaults == null) return error.MalformedMedia;
            }
            cursor = b.end;
        }
        return result;
    }
    fn trackBoxes(self: *Parser, bytes: []const u8, t: *Tables, depth: u8) !void {
        if (depth > 4) return error.MalformedMedia;
        var cursor: usize = 0;
        while (cursor < bytes.len) {
            const b = try self.box(bytes, cursor);
            const p = b.payload;
            switch (b.typ) {
                tag("tkhd") => {
                    if (p.len < 4) return error.MalformedMedia;
                    const id_offset: usize = if (p[0] == 0) 12 else if (p[0] == 1) 20 else return error.UnsupportedTimeline;
                    const matrix_offset: usize = if (p[0] == 0) 40 else 52;
                    if (p.len < matrix_offset + 44) return error.MalformedMedia;
                    t.id = u32be(p[id_offset..][0..4]);
                    for (&t.matrix, 0..) |*v, i| v.* = @bitCast(u32be(p[matrix_offset + i * 4 ..][0..4]));
                },
                tag("mdhd") => t.scale = try headerScale(p),
                tag("hdlr") => {
                    if (p.len < 12) return error.MalformedMedia;
                    // QuickTime minf may also contain a data hdlr; only the
                    // mdia handler identifies this track as video.
                    if (depth == 1) t.handler = u32be(p[8..12]);
                },
                tag("mdia"), tag("minf"), tag("stbl"), tag("edts"), tag("dinf") => try self.trackBoxes(p, t, depth + 1),
                tag("dref") => {
                    if (p.len < 8 or p[0] != 0) return error.MalformedMedia;
                    if (u32be(p[4..8]) == 1) {
                        const ref = try self.box(p, 8);
                        t.self_contained = ref.typ == tag("url ") and ref.end == p.len and
                            std.mem.eql(u8, ref.payload, &.{ 0, 0, 0, 1 });
                    }
                },
                tag("stsd") => try self.description(p, t),
                tag("stsz"), tag("stz2") => {
                    if (t.sizes.len != 0) return error.MalformedMedia;
                    t.sizes = p;
                    t.compact = b.typ == tag("stz2");
                },
                tag("stsc") => {
                    if (t.stsc.len != 0) return error.MalformedMedia;
                    t.stsc = p;
                },
                tag("stco"), tag("co64") => {
                    if (t.offsets.len != 0) return error.MalformedMedia;
                    t.offsets = p;
                    t.offset64 = b.typ == tag("co64");
                },
                tag("stts") => {
                    if (t.stts.len != 0) return error.MalformedMedia;
                    t.stts = p;
                },
                tag("ctts") => {
                    if (t.ctts.len != 0) return error.MalformedMedia;
                    t.ctts = p;
                },
                tag("stss") => {
                    if (t.stss.len != 0) return error.MalformedMedia;
                    t.stss = p;
                },
                tag("elst") => {
                    if (t.elst.len != 0) return error.MalformedMedia;
                    t.elst = p;
                },
                else => {},
            }
            cursor = b.end;
        }
    }
    fn description(self: *Parser, bytes: []const u8, t: *Tables) !void {
        if (t.description_seen) return error.MalformedMedia;
        t.description_seen = true;
        if (bytes.len < 8) return error.MalformedMedia;
        const count = u32be(bytes[4..8]);
        if (count > self.limits.max_tracks) return error.ResourceLimitExceeded;
        var cursor: usize = 8;
        for (0..count) |_| {
            const b = try self.box(bytes, cursor);
            if (b.typ == tag("avc1") or b.typ == tag("avc3") or b.typ == tag("jpeg")) {
                const codec: Codec = if (b.typ != tag("jpeg")) .avc else .mjpeg;
                if (count != 1) return error.UnsupportedSampleDescription;
                if (b.payload.len < 78) return error.MalformedMedia;
                const width = u16be(b.payload[24..26]);
                const height = u16be(b.payload[26..28]);
                if (width == 0 or height == 0) return error.MalformedMedia;
                if (width > self.limits.max_dimension or height > self.limits.max_dimension) return error.ResourceLimitExceeded;
                // External data references cannot safely resolve against this source.
                if (u16be(b.payload[6..8]) != 1) return error.UnsupportedDataReference;
                if (codec == .mjpeg and (u16be(b.payload[8..10]) != 0 or u16be(b.payload[40..42]) != 1)) return error.UnsupportedSampleDescription;
                var child: usize = 78;
                var avcc: ?[]const u8 = null;
                var color: []const u8 = &.{};
                var horizontal: u32 = 1;
                var vertical: u32 = 1;
                while (child < b.payload.len) {
                    const c = try self.box(b.payload, child);
                    if (c.typ == tag("avcC")) {
                        if (avcc != null) return error.MalformedMedia;
                        avcc = c.payload;
                    }
                    if (codec == .mjpeg and c.typ == tag("fiel") and (c.payload.len != 2 or c.payload[0] != 1)) return error.UnsupportedInterlacedVideo;
                    if (c.typ == tag("sinf")) return error.UnsupportedEncryptedMedia;
                    if (c.typ == tag("clap")) return error.UnsupportedDisplayGeometry;
                    if (c.typ == tag("colr")) color = c.payload;
                    if (c.typ == tag("pasp")) {
                        if (c.payload.len != 8) return error.MalformedMedia;
                        horizontal = u32be(c.payload[0..4]);
                        vertical = u32be(c.payload[4..8]);
                        if (horizontal == 0 or vertical == 0) return error.MalformedMedia;
                    }
                    child = c.end;
                }
                var config: []const u8 = &.{};
                var nal_len: u3 = 0;
                if (codec == .avc) {
                    config = avcc orelse return error.MalformedMedia;
                    try validateAvcc(config);
                    nal_len = @as(u3, @intCast(config[4] & 3)) + 1;
                    if (nal_len == 3) return error.MalformedMedia;
                } else if (avcc != null) return error.MalformedMedia;
                t.track = .{ .inband_parameter_sets = b.typ == tag("avc3"), .codec = codec, .id = 0, .timescale = 0, .width = width, .height = height, .avcc = config, .nal_length_bytes = nal_len, .pixel_aspect = .{ .horizontal = horizontal, .vertical = vertical }, .color_info = color, .display_matrix = @splat(0) };
            }
            cursor = b.end;
        }
        if (cursor != bytes.len) return error.MalformedMedia;
    }
};

fn validateAvcc(config: []const u8) !void {
    if (config.len < 7 or config[0] != 1) return error.MalformedMedia;
    var cursor: usize = 6;
    const sps_count = config[5] & 31;
    if (sps_count == 0) return error.MalformedMedia;
    for (0..sps_count) |_| try configNal(config, &cursor);
    if (cursor >= config.len) return error.MalformedMedia;
    const pps_count = config[cursor];
    cursor += 1;
    if (pps_count == 0) return error.MalformedMedia;
    for (0..pps_count) |_| try configNal(config, &cursor);
    // Optional high-profile extension remains borrowed for the codec backend.
}
fn configNal(config: []const u8, cursor: *usize) !void {
    if (config.len - cursor.* < 2) return error.MalformedMedia;
    const size = u16be(config[cursor.*..][0..2]);
    cursor.* += 2;
    if (size == 0 or size > config.len - cursor.*) return error.MalformedMedia;
    cursor.* += size;
}

fn headerScale(p: []const u8) !u32 {
    if (p.len < 4) return error.MalformedMedia;
    const offset: usize = if (p[0] == 0) 12 else if (p[0] == 1) 20 else return error.UnsupportedTimeline;
    if (p.len < offset + 4) return error.MalformedMedia;
    const value = u32be(p[offset..][0..4]);
    if (value == 0) return error.MalformedMedia;
    return value;
}
fn entries(p: []const u8, stride: usize) ![]const u8 {
    if (p.len < 8 or p[0] != 0) return error.MalformedMedia;
    const n = u32be(p[4..8]);
    if (n > (p.len - 8) / stride or p.len - 8 != @as(u64, n) * stride) return error.MalformedMedia;
    return p[8..];
}
fn decodePreroll(ctts: []const u8) !u32 {
    if (ctts.len == 0) return 0;
    if (ctts.len < 8 or ctts[0] > 1) return error.MalformedMedia;
    const count = u32be(ctts[4..8]);
    if (@as(u64, count) * 8 != ctts.len - 8) return error.MalformedMedia;
    if (ctts[0] == 0) return 0;
    var shift: u32 = 0;
    for (0..count) |i| {
        const offset: i64 = @as(i32, @bitCast(u32be(ctts[12 + i * 8 ..][0..4])));
        if (offset < 0) shift = @max(shift, @as(u32, @intCast(-offset)));
    }
    return shift;
}

fn parseEdit(p: []const u8, movie_scale: u32, track_scale: u32) !timeline.Edit {
    if (p.len == 0) return .{};
    if (p.len < 8 or p[0] > 1) return error.UnsupportedTimeline;
    const stride: usize = if (p[0] == 0) 12 else 20;
    const n = u32be(p[4..8]);
    if (n > (p.len - 8) / stride or p.len - 8 != @as(u64, n) * stride) return error.MalformedMedia;
    var out = timeline.Edit{};
    var saw_media = false;
    for (0..n) |i| {
        const e = p[8 + i * stride ..][0..stride];
        const duration: u64 = if (p[0] == 0) u32be(e[0..4]) else u64be(e[0..8]);
        const media_start: i64 = if (p[0] == 0) @as(i32, @bitCast(u32be(e[4..8]))) else @bitCast(u64be(e[8..16]));
        if (u16be(e[stride - 4 ..][0..2]) != 1 or u16be(e[stride - 2 ..][0..2]) != 0) return error.UnsupportedTimeline;
        const scaled = try timeline.rescale(std.math.cast(i64, duration) orelse return error.TimestampOverflow, movie_scale, track_scale);
        if (media_start == -1 and !saw_media) {
            out.source_start = std.math.add(i64, out.source_start, scaled) catch return error.TimestampOverflow;
        } else if (media_start >= 0 and !saw_media) {
            out.media_start = media_start;
            out.source_duration = scaled;
            saw_media = true;
        } else return error.UnsupportedTimeline;
    }
    if (!saw_media) return error.UnsupportedTimeline;
    return out;
}
const Run = struct {
    bytes: []const u8,
    cursor: usize = 0,
    remaining: u32 = 0,
    value: i64 = 0,
    signed: bool = false,
    fn next(self: *Run) !i64 {
        if (self.remaining == 0) {
            if (self.cursor >= self.bytes.len) return error.MalformedMedia;
            self.remaining = u32be(self.bytes[self.cursor..][0..4]);
            const v = u32be(self.bytes[self.cursor + 4 ..][0..4]);
            self.value = if (self.signed) @as(i32, @bitCast(v)) else v;
            self.cursor += 8;
            if (self.remaining == 0) return error.MalformedMedia;
        }
        self.remaining -= 1;
        return self.value;
    }
    fn finish(self: Run) !void {
        if (self.remaining != 0 or self.cursor != self.bytes.len) return error.MalformedMedia;
    }
};
fn buildPackets(allocator: std.mem.Allocator, t: Tables, track: Track, spans: []const Span, limits: Limits, control: source.Control) ![]Packet {
    if (t.sizes.len < 12) return error.MalformedMedia;
    if (t.sizes[0] != 0) return error.MalformedMedia;
    const n = u32be(t.sizes[8..12]);
    if (n == 0) return error.EmptyVideoTrack;
    if (n > limits.max_samples or @as(u64, n) * @sizeOf(Packet) > limits.max_index_bytes) return error.ResourceLimitExceeded;
    const fixed = if (t.compact) 0 else u32be(t.sizes[4..8]);
    const bits: u8 = if (t.compact) t.sizes[7] else 32;
    if (t.compact and bits != 4 and bits != 8 and bits != 16) return error.UnsupportedSampleSize;
    const expected: u64 = if (fixed != 0) 0 else (@as(u64, n) * bits + 7) / 8;
    if (t.sizes.len - 12 != expected) return error.MalformedMedia;
    const offsets = try entries(t.offsets, if (t.offset64) 8 else 4);
    const chunks = offsets.len / (if (t.offset64) @as(usize, 8) else 4);
    const mapping = try entries(t.stsc, 12);
    if (mapping.len == 0 or chunks == 0) return error.MalformedMedia;
    var previous: u32 = 0;
    for (0..mapping.len / 12) |i| {
        const e = mapping[i * 12 ..][0..12];
        const first = u32be(e[0..4]);
        if (first <= previous or first > chunks or (i == 0 and first != 1) or u32be(e[4..8]) == 0) return error.MalformedMedia;
        if (u32be(e[8..12]) != 1) return error.UnsupportedSampleDescription;
        previous = first;
    }
    var durations = Run{ .bytes = try entries(t.stts, 8) };
    var composition = Run{ .bytes = &.{} };
    if (t.ctts.len != 0) {
        if (t.ctts.len < 8 or t.ctts[0] > 1) return error.MalformedMedia;
        const count = u32be(t.ctts[4..8]);
        if (@as(u64, count) * 8 != t.ctts.len - 8) return error.MalformedMedia;
        composition = .{ .bytes = t.ctts[8..], .signed = t.ctts[0] == 1 };
    }
    const sync = if (t.stss.len != 0) try entries(t.stss, 4) else &.{};
    previous = 0;
    for (0..sync.len / 4) |i| {
        const v = u32be(sync[i * 4 ..][0..4]);
        if (v <= previous or v > n) return error.MalformedMedia;
        previous = v;
    }
    const packets = try allocator.alloc(Packet, n);
    errdefer allocator.free(packets);
    var sample: usize = 0;
    var map_index: usize = 0;
    var sync_index: usize = 0;
    var dts: i64 = 0;
    for (0..chunks) |chunk| {
        try control.check();
        while (map_index + 12 < mapping.len and u32be(mapping[map_index + 12 ..][0..4]) <= chunk + 1) map_index += 12;
        const count = u32be(mapping[map_index + 4 ..][0..4]);
        const stride: usize = if (t.offset64) 8 else 4;
        var offset = if (t.offset64) u64be(offsets[chunk * stride ..][0..8]) else @as(u64, u32be(offsets[chunk * stride ..][0..4]));
        for (0..count) |_| {
            if (sample >= n) return error.MalformedMedia;
            if (sample % 256 == 0) try control.check();
            const size: u32 = if (fixed != 0) fixed else switch (bits) {
                4 => if (sample % 2 == 0) t.sizes[12 + sample / 2] >> 4 else t.sizes[12 + sample / 2] & 15,
                8 => t.sizes[12 + sample],
                16 => u16be(t.sizes[12 + sample * 2 ..][0..2]),
                32 => u32be(t.sizes[12 + sample * 4 ..][0..4]),
                else => unreachable,
            };
            if (size == 0) return error.MalformedMedia;
            if (size > limits.max_packet_bytes) return error.ResourceLimitExceeded;
            const end = std.math.add(u64, offset, size) catch return error.MalformedMedia;
            var valid = false;
            for (spans) |span| if (offset >= span.start and end <= span.end) {
                valid = true;
                break;
            };
            if (!valid) return error.MalformedMedia;
            const duration = try durations.next();
            if (duration <= 0) return error.UnsupportedTimeline;
            const pts = std.math.add(i64, dts, if (t.ctts.len != 0) try composition.next() else 0) catch return error.TimestampOverflow;
            const is_sync = t.stss.len == 0 or (sync_index < sync.len and u32be(sync[sync_index..][0..4]) == sample + 1);
            if (is_sync and t.stss.len != 0) sync_index += 4;
            packets[sample] = .{ .offset = offset, .size = size, .dts = try track.edit.present(dts - track.decode_preroll_ticks), .media_dts = dts, .media_pts = pts, .pts = try track.edit.present(pts), .duration = @intCast(duration), .sync = is_sync };
            dts = std.math.add(i64, dts, duration) catch return error.TimestampOverflow;
            offset = end;
            sample += 1;
        }
    }
    if (sample != n) return error.MalformedMedia;
    try durations.finish();
    if (t.ctts.len != 0) try composition.finish();
    return packets;
}
fn u16be(bytes: []const u8) u16 {
    return std.mem.readInt(u16, bytes[0..2], .big);
}
fn u32be(bytes: []const u8) u32 {
    return std.mem.readInt(u32, bytes[0..4], .big);
}
fn u64be(bytes: []const u8) u64 {
    return std.mem.readInt(u64, bytes[0..8], .big);
}

fn fragmentField(bytes: []const u8, cursor: *usize) !u32 {
    if (cursor.* > bytes.len or bytes.len - cursor.* < 4) return error.MalformedMedia;
    const value = u32be(bytes[cursor.*..][0..4]);
    cursor.* += 4;
    return value;
}
fn buildFragments(allocator: std.mem.Allocator, input: *source.Source, track: *Track, defaults: Defaults, fragments: []const Fragment, spans: []const Span, limits: Limits) ![]Packet {
    if (defaults.description != 1) return error.UnsupportedSampleDescription;
    var packets: std.ArrayList(Packet) = .empty;
    errdefer packets.deinit(allocator);
    var box_count: usize = 0;
    for (fragments) |fragment| {
        try input.control.check();
        var lease = try input.read(fragment.offset, fragment.size);
        defer lease.deinit();
        const root = try iso.readBox(lease.bytes, 0);
        if (root.typ != tag("moof") or root.end != lease.bytes.len) return error.MalformedMedia;
        var cursor: usize = 0;
        while (cursor < root.payload.len) {
            const box = try iso.readBox(root.payload, cursor);
            cursor = box.end;
            box_count += 1;
            if (box_count > limits.max_boxes) return error.ResourceLimitExceeded;
            if (box.typ != tag("traf")) continue;
            var header: ?[]const u8 = null;
            var decode_time: ?[]const u8 = null;
            var nested: usize = 0;
            while (nested < box.payload.len) {
                const child = try iso.readBox(box.payload, nested);
                nested = child.end;
                box_count += 1;
                if (box_count > limits.max_boxes) return error.ResourceLimitExceeded;
                switch (child.typ) {
                    tag("tfhd") => {
                        if (header != null) return error.MalformedMedia;
                        header = child.payload;
                    },
                    tag("tfdt") => {
                        if (decode_time != null) return error.MalformedMedia;
                        decode_time = child.payload;
                    },
                    tag("senc"), tag("saiz"), tag("saio") => return error.UnsupportedEncryptedMedia,
                    else => {},
                }
            }
            const h = header orelse return error.MalformedMedia;
            if (h.len < 8 or h[0] != 0) return error.MalformedMedia;
            if (u32be(h[4..8]) != track.id) continue;
            const flags = u32be(h[0..4]) & 0xffffff;
            if (flags & ~@as(u32, 0x02003b) != 0) return error.UnsupportedFragmentedMp4;
            var hc: usize = 8;
            var base = fragment.offset;
            if (flags & 1 != 0) {
                if (h.len - hc < 8) return error.MalformedMedia;
                base = u64be(h[hc..][0..8]);
                hc += 8;
            } else if (flags & 0x020000 == 0) return error.UnsupportedFragmentedMp4;
            var current = defaults;
            if (flags & 2 != 0) current.description = try fragmentField(h, &hc);
            if (current.description != 1) return error.UnsupportedSampleDescription;
            if (flags & 8 != 0) current.duration = try fragmentField(h, &hc);
            if (flags & 16 != 0) current.size = try fragmentField(h, &hc);
            if (flags & 32 != 0) current.flags = try fragmentField(h, &hc);
            if (hc != h.len) return error.MalformedMedia;
            const dt = decode_time orelse return error.UnsupportedTimeline;
            if (dt.len < 4 or dt[0] > 1 or dt.len != (if (dt[0] == 0) @as(usize, 8) else 12) or u32be(dt[0..4]) & 0xffffff != 0) return error.MalformedMedia;
            var dts = std.math.cast(i64, if (dt[0] == 0) @as(u64, u32be(dt[4..8])) else u64be(dt[4..12])) orelse return error.TimestampOverflow;
            if (packets.items.len != 0) {
                const previous = packets.items[packets.items.len - 1];
                if (dts < try std.math.add(i64, previous.media_dts, previous.duration)) return error.UnsupportedTimeline;
            }
            var data_offset: ?u64 = null;
            nested = 0;
            while (nested < box.payload.len) {
                try input.control.check();
                const child = try iso.readBox(box.payload, nested);
                nested = child.end;
                if (child.typ != tag("trun")) continue;
                const p = child.payload;
                if (p.len < 8 or p[0] > 1) return error.MalformedMedia;
                const run_flags = u32be(p[0..4]) & 0xffffff;
                if (run_flags & ~@as(u32, 0xf05) != 0 or (run_flags & 4 != 0 and run_flags & 0x400 != 0)) return error.MalformedMedia;
                const count = u32be(p[4..8]);
                const total = try std.math.add(usize, packets.items.len, count);
                if (total > limits.max_samples or total > limits.max_index_bytes / @sizeOf(Packet)) return error.ResourceLimitExceeded;
                var pc: usize = 8;
                if (run_flags & 1 != 0) {
                    const delta: i32 = @bitCast(try fragmentField(p, &pc));
                    data_offset = std.math.cast(u64, @as(i128, base) + delta) orelse return error.MalformedMedia;
                }
                var first_flags = current.flags;
                if (run_flags & 4 != 0) first_flags = try fragmentField(p, &pc);
                var offset = data_offset orelse return error.UnsupportedFragmentedMp4;
                try packets.ensureTotalCapacityPrecise(allocator, total);
                for (0..count) |i| {
                    try input.control.check();
                    const duration = if (run_flags & 0x100 != 0) try fragmentField(p, &pc) else current.duration;
                    const size = if (run_flags & 0x200 != 0) try fragmentField(p, &pc) else current.size;
                    const sample_flags = if (run_flags & 0x400 != 0) try fragmentField(p, &pc) else if (i == 0) first_flags else current.flags;
                    const raw_composition = if (run_flags & 0x800 != 0) try fragmentField(p, &pc) else 0;
                    const composition: i64 = if (p[0] == 1) @as(i32, @bitCast(raw_composition)) else raw_composition;
                    if (size == 0 or size > limits.max_packet_bytes or duration == 0) return error.ResourceLimitExceeded;
                    const end = try std.math.add(u64, offset, size);
                    var valid = false;
                    for (spans) |span| if (offset >= span.start and end <= span.end) {
                        valid = true;
                        break;
                    };
                    if (!valid) return error.MalformedMedia;
                    const pts = try std.math.add(i64, dts, composition);
                    if (composition < 0) track.decode_preroll_ticks = @max(track.decode_preroll_ticks, std.math.cast(u32, -composition) orelse return error.TimestampOverflow);
                    packets.appendAssumeCapacity(.{ .offset = offset, .size = size, .dts = dts, .media_dts = dts, .media_pts = pts, .pts = try track.edit.present(pts), .duration = duration, .sync = sample_flags & 0x10000 == 0 });
                    dts = try std.math.add(i64, dts, duration);
                    offset = end;
                }
                if (pc != p.len) return error.MalformedMedia;
                data_offset = offset;
            }
        }
    }
    if (packets.items.len == 0) return error.EmptyVideoTrack;
    for (packets.items) |*packet| packet.dts = try track.edit.present(try std.math.sub(i64, packet.media_dts, track.decode_preroll_ticks));
    return packets.toOwnedSlice(allocator);
}
