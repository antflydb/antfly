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

const std = @import("std");
pub const Codec = enum {
    opus,
    vorbis,
    flac,
};

/// The audio track pulled out of a Matroska/WebM file: the codec's private
/// setup blob and the ordered list of per-track packets found in Cluster
/// blocks. `access_units` borrow directly from the caller's `audio_bytes` and
/// are only valid for as long as that buffer lives; only the slice of slices
/// itself is owned and must be freed via `deinit`.
pub const BlockPadding = struct { first: usize, end: usize, ns: i64 };

pub fn playbackGap(time_ns: u64, delay: usize, rate: u32, cursor: usize) usize {
    const wanted = sampleCount(time_ns, rate) -| delay;
    return if (wanted > cursor +| @max(1, rate / 1000)) wanted - cursor else 0;
}

pub fn sampleCount(ns: u64, rate: u32) usize {
    return std.math.cast(usize, @as(u128, ns) * rate / std.time.ns_per_s) orelse std.math.maxInt(usize);
}

pub const DemuxedAudio = struct {
    codec: Codec,
    channels: u16,
    codec_delay_ns: u64,
    seek_pre_roll_ns: u64,
    codec_private: []const u8,
    access_units: [][]const u8,
    /// Presentation time of each access unit, in nanoseconds from the start
    /// of the segment: the cluster's timestamp plus the block's relative
    /// timecode, scaled by TimestampScale. Same length as `access_units`;
    /// laced frames share their block's time.
    access_unit_times_ns: []u64,
    padding: []BlockPadding = &.{},
    allocator: std.mem.Allocator,

    pub fn deinit(self: *const DemuxedAudio) void {
        self.allocator.free(self.access_units);
        self.allocator.free(self.access_unit_times_ns);
        self.allocator.free(self.padding);
    }
};

// EBML IDs. Only the elements this demuxer needs to walk or recognize as
// "safe to skip" are listed; every other master or leaf element is skipped by
// its declared size without inspection.
pub const ebml_header_id: u32 = 0x1A45DFA3;
pub const segment_id: u32 = 0x18538067;
pub const tracks_id: u32 = 0x1654AE6B;
pub const track_entry_id: u32 = 0xAE;
pub const track_number_id: u32 = 0xD7;
pub const track_type_id: u32 = 0x83;
pub const codec_id_id: u32 = 0x86;
pub const codec_private_id: u32 = 0x63A2;
pub const codec_delay_id: u32 = 0x56AA;
pub const seek_pre_roll_id: u32 = 0x56BB;
pub const audio_settings_id: u32 = 0xE1;
pub const sampling_frequency_id: u32 = 0xB5;
pub const channels_id: u32 = 0x9F;
pub const info_id: u32 = 0x1549A966;
pub const timestamp_scale_id: u32 = 0x2AD7B1;
pub const cluster_id: u32 = 0x1F43B675;
pub const timecode_id: u32 = 0xE7;
pub const simple_block_id: u32 = 0xA3;
pub const block_group_id: u32 = 0xA0;
pub const block_id: u32 = 0xA1;
pub const discard_padding_id: u32 = 0x75A2;
pub const prev_size_id: u32 = 0xAB;
pub const position_id: u32 = 0xA7;
pub const silent_tracks_id: u32 = 0x5854;
pub const void_id: u32 = 0xEC;
pub const crc32_id: u32 = 0xBF;

pub const audio_track_type: u8 = 2;

/// Demuxes the first audio track out of a Matroska/WebM byte stream. Video
/// and subtitle tracks are recognized and skipped. Handles unknown-size
/// Segment and Cluster elements (used by live/streamed recordings such as
/// MediaRecorder output) and all three Matroska lacing modes.
pub fn demux(allocator: std.mem.Allocator, audio_bytes: []const u8) !DemuxedAudio {
    if (audio_bytes.len < 4) return error.UnsupportedAudioFormat;

    const ebml_elem = try readElementHeader(audio_bytes, 0);
    if (ebml_elem.id != ebml_header_id) return error.UnsupportedAudioFormat;
    var cursor = ebml_elem.data_end orelse return error.UnsupportedAudioFormat;

    var segment_elem: ?Element = null;
    while (cursor < audio_bytes.len) {
        const elem = try readElementHeader(audio_bytes, cursor);
        if (elem.id == segment_id) {
            segment_elem = elem;
            break;
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }
    const segment = segment_elem orelse return error.UnsupportedAudioFormat;

    const segment_payload = if (segment.size) |sz| blk: {
        const end_u64 = @as(u64, segment.data_start) + sz;
        const end = std.math.cast(usize, end_u64) orelse return error.UnsupportedAudioFormat;
        if (end > audio_bytes.len) return error.UnsupportedAudioFormat;
        break :blk audio_bytes[segment.data_start..end];
    } else audio_bytes[segment.data_start..];

    var state = DemuxState{};
    errdefer state.access_units.deinit(allocator);
    errdefer state.access_unit_times_ns.deinit(allocator);
    errdefer state.padding.deinit(allocator);

    try parseSegment(allocator, segment_payload, &state);

    const track = state.track orelse return error.UnsupportedAudioFormat;
    const codec = track.codec orelse return error.UnsupportedAudioFormat;
    if (state.access_units.items.len == 0) return error.UnsupportedAudioFormat;
    if (track.codec_private.len == 0) return error.UnsupportedAudioFormat;

    const access_units = try state.access_units.toOwnedSlice(allocator);
    errdefer allocator.free(access_units);
    const access_unit_times_ns = try state.access_unit_times_ns.toOwnedSlice(allocator);
    errdefer allocator.free(access_unit_times_ns);

    return .{
        .codec = codec,
        .channels = track.channels,
        .codec_delay_ns = track.codec_delay_ns,
        .seek_pre_roll_ns = track.seek_pre_roll_ns,
        .codec_private = track.codec_private,
        .access_units = access_units,
        .access_unit_times_ns = access_unit_times_ns,
        .padding = try state.padding.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

// --- EBML primitives ---------------------------------------------------

pub const VintResult = @import("ebml.zig").VintResult;
pub const SizeResult = @import("ebml.zig").SizeResult;
pub const Element = @import("ebml.zig").Element;
pub fn readElementId(bytes: []const u8, start: usize) !VintResult {
    return @import("ebml.zig").readElementId(bytes, start) catch return error.UnsupportedAudioFormat;
}
pub fn readVint(bytes: []const u8, start: usize) !SizeResult {
    return @import("ebml.zig").readVint(bytes, start) catch return error.UnsupportedAudioFormat;
}
pub fn readElementHeader(bytes: []const u8, start: usize) !Element {
    return @import("ebml.zig").readElementHeader(bytes, start) catch return error.UnsupportedAudioFormat;
}
pub fn elementPayload(bytes: []const u8, elem: Element) ![]const u8 {
    return @import("ebml.zig").elementPayload(bytes, elem) catch return error.UnsupportedAudioFormat;
}
pub fn readUint(bytes: []const u8) !u64 {
    return @import("ebml.zig").readUint(bytes) catch return error.UnsupportedAudioFormat;
}
pub fn readSignedInt(bytes: []const u8) !i64 {
    return @import("ebml.zig").readSignedInt(bytes) catch return error.UnsupportedAudioFormat;
}
pub fn readFloat(bytes: []const u8) !f64 {
    return @import("ebml.zig").readFloat(bytes) catch return error.UnsupportedAudioFormat;
}
pub fn vintLength(first_byte: u8) !usize {
    return @import("ebml.zig").vintLength(first_byte) catch return error.UnsupportedAudioFormat;
}

// --- Segment / Tracks / Clusters ---------------------------------------

pub const TrackInfo = struct {
    number: u64,
    codec: ?Codec,
    codec_private: []const u8 = &.{},
    channels: u16 = 1,
    codec_delay_ns: u64 = 0,
    seek_pre_roll_ns: u64 = 0,
};

/// Matroska's default TimestampScale: block timecodes count milliseconds.
pub const default_timestamp_scale_ns: u64 = 1_000_000;

pub const DemuxState = struct {
    track: ?TrackInfo = null,
    access_units: std.ArrayList([]const u8) = .empty,
    access_unit_times_ns: std.ArrayList(u64) = .empty,
    timestamp_scale_ns: u64 = default_timestamp_scale_ns,
    padding: std.ArrayList(BlockPadding) = .empty,
};

pub fn parseSegment(allocator: std.mem.Allocator, payload: []const u8, state: *DemuxState) !void {
    var cursor: usize = 0;
    while (cursor < payload.len) {
        const elem = try readElementHeader(payload, cursor);
        switch (elem.id) {
            info_id => try parseInfo(try elementPayload(payload, elem), state),
            tracks_id => try parseTracks(try elementPayload(payload, elem), state),
            cluster_id => {
                const track = state.track orelse return error.UnsupportedAudioFormat;
                const cluster_bytes = payload[elem.data_start..];
                const consumed = try parseCluster(
                    cluster_bytes,
                    elem.size,
                    track.number,
                    allocator,
                    &state.access_units,
                    &state.access_unit_times_ns,
                    state.timestamp_scale_ns,
                    &state.padding,
                );
                cursor = elem.data_start + consumed;
                continue;
            },
            else => {},
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }
}

/// Reads the segment's TimestampScale, which turns cluster and block
/// timecodes into nanoseconds. Everything else in Info is ignored.
pub fn parseInfo(payload: []const u8, state: *DemuxState) !void {
    var cursor: usize = 0;
    while (cursor < payload.len) {
        const elem = try readElementHeader(payload, cursor);
        if (elem.id == timestamp_scale_id) {
            const scale = try readUint(try elementPayload(payload, elem));
            if (scale != 0) state.timestamp_scale_ns = scale;
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }
}

pub fn parseTracks(payload: []const u8, state: *DemuxState) !void {
    var cursor: usize = 0;
    while (cursor < payload.len) {
        const elem = try readElementHeader(payload, cursor);
        if (elem.id == track_entry_id and state.track == null) {
            if (try parseTrackEntry(try elementPayload(payload, elem))) |info| {
                state.track = info;
            }
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }
}

/// Parses one TrackEntry. Returns `null` for non-audio tracks (video,
/// subtitle, etc.) so the caller keeps looking for the first audio track.
pub fn parseTrackEntry(payload: []const u8) !?TrackInfo {
    var number: ?u64 = null;
    var track_type: ?u8 = null;
    var codec_id: []const u8 = &.{};
    var codec_private: []const u8 = &.{};
    var channels: u16 = 1;
    var codec_delay_ns: u64 = 0;
    var seek_pre_roll_ns: u64 = 0;

    var cursor: usize = 0;
    while (cursor < payload.len) {
        const elem = try readElementHeader(payload, cursor);
        switch (elem.id) {
            track_number_id => number = try readUint(try elementPayload(payload, elem)),
            track_type_id => track_type = std.math.cast(u8, try readUint(try elementPayload(payload, elem))) orelse
                return error.UnsupportedAudioFormat,
            codec_id_id => codec_id = try elementPayload(payload, elem),
            codec_private_id => codec_private = try elementPayload(payload, elem),
            codec_delay_id => codec_delay_ns = try readUint(try elementPayload(payload, elem)),
            seek_pre_roll_id => seek_pre_roll_ns = try readUint(try elementPayload(payload, elem)),
            audio_settings_id => channels = try parseAudioSettingsChannels(try elementPayload(payload, elem)),
            else => {},
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }

    const ttype = track_type orelse return error.UnsupportedAudioFormat;
    if (ttype != audio_track_type) return null;
    const track_number = number orelse return error.UnsupportedAudioFormat;

    var codec: ?Codec = null;
    if (std.mem.eql(u8, codec_id, "A_OPUS")) {
        codec = .opus;
    } else if (std.mem.eql(u8, codec_id, "A_VORBIS")) {
        codec = .vorbis;
    } else if (std.mem.eql(u8, codec_id, "A_FLAC")) {
        codec = .flac;
    }

    return TrackInfo{
        .number = track_number,
        .codec = codec,
        .codec_private = codec_private,
        .channels = channels,
        .codec_delay_ns = codec_delay_ns,
        .seek_pre_roll_ns = seek_pre_roll_ns,
    };
}

pub fn parseAudioSettingsChannels(payload: []const u8) !u16 {
    var channels: u16 = 1;
    var cursor: usize = 0;
    while (cursor < payload.len) {
        const elem = try readElementHeader(payload, cursor);
        switch (elem.id) {
            channels_id => channels = std.math.cast(u16, try readUint(try elementPayload(payload, elem))) orelse
                return error.UnsupportedAudioFormat,
            sampling_frequency_id => _ = try readFloat(try elementPayload(payload, elem)),
            else => {},
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }
    return channels;
}

pub fn isClusterChildId(id: u32) bool {
    return switch (id) {
        timecode_id, simple_block_id, block_group_id, prev_size_id, position_id, silent_tracks_id, void_id, crc32_id => true,
        else => false,
    };
}

/// Parses one Cluster's children starting at `remaining[0]` (the Cluster's
/// own `data_start`) and returns how many bytes belong to it. A known size
/// bounds the scan directly; an unknown size (streamed recordings) is
/// resolved by scanning element-by-element and stopping at the first ID that
/// is not a recognized Cluster child, which is exactly how a Cluster's
/// end is inferred when it isn't declared.
pub fn parseCluster(
    remaining: []const u8,
    known_size: ?u64,
    target_track: u64,
    allocator: std.mem.Allocator,
    access_units: *std.ArrayList([]const u8),
    access_unit_times_ns: *std.ArrayList(u64),
    timestamp_scale_ns: u64,
    padding: *std.ArrayList(BlockPadding),
) !usize {
    const limit: usize = if (known_size) |sz| blk: {
        const cast_size = std.math.cast(usize, sz) orelse return error.UnsupportedAudioFormat;
        if (cast_size > remaining.len) return error.UnsupportedAudioFormat;
        break :blk cast_size;
    } else remaining.len;

    var cursor: usize = 0;
    // Matroska requires a cluster's Timestamp to precede its blocks.
    var cluster_ticks: u64 = 0;
    while (cursor < limit) {
        if (known_size == null) {
            const peek = readElementHeader(remaining, cursor) catch break;
            if (!isClusterChildId(peek.id)) break;
        }
        const elem = try readElementHeader(remaining, cursor);
        switch (elem.id) {
            timecode_id => cluster_ticks = try readUint(try elementPayload(remaining, elem)),
            simple_block_id => try parseBlockIntoAccessUnits(
                try elementPayload(remaining, elem),
                target_track,
                allocator,
                access_units,
                access_unit_times_ns,
                cluster_ticks,
                timestamp_scale_ns,
            ),
            block_group_id => try parseBlockGroup(
                try elementPayload(remaining, elem),
                target_track,
                allocator,
                access_units,
                access_unit_times_ns,
                cluster_ticks,
                timestamp_scale_ns,
                padding,
            ),
            else => {},
        }
        cursor = elem.data_end orelse {
            if (known_size == null) break;
            return error.UnsupportedAudioFormat;
        };
        if (cursor > limit) return error.UnsupportedAudioFormat;
    }
    return cursor;
}

pub fn parseBlockGroup(
    payload: []const u8,
    target_track: u64,
    allocator: std.mem.Allocator,
    access_units: *std.ArrayList([]const u8),
    access_unit_times_ns: *std.ArrayList(u64),
    cluster_ticks: u64,
    timestamp_scale_ns: u64,
    padding: *std.ArrayList(BlockPadding),
) !void {
    var cursor: usize = 0;
    const first = access_units.items.len;
    var pending_discard: ?[]const u8 = null;

    while (cursor < payload.len) {
        const elem = try readElementHeader(payload, cursor);
        switch (elem.id) {
            block_id => {
                try parseBlockIntoAccessUnits(
                    try elementPayload(payload, elem),
                    target_track,
                    allocator,
                    access_units,
                    access_unit_times_ns,
                    cluster_ticks,
                    timestamp_scale_ns,
                );
            },
            discard_padding_id => pending_discard = try elementPayload(payload, elem),
            else => {},
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }

    if (access_units.items.len > first) if (pending_discard) |bytes| {
        const ns = try readSignedInt(bytes);
        if (ns != 0) try padding.append(allocator, .{ .first = first, .end = access_units.items.len, .ns = ns });
    };
}

/// Parses a Block/SimpleBlock body: a vint track number, a 2-byte signed
/// relative timecode, a flags byte, and then zero or more laced frames.
/// Frames belonging to a track other than `target_track` are ignored
/// entirely. Every frame kept records the block's presentation time, which
/// is what puts the decoded audio back on the recording's timeline; laced
/// frames share that time, so they read as one contiguous run.
pub fn parseBlockIntoAccessUnits(
    block_bytes: []const u8,
    target_track: u64,
    allocator: std.mem.Allocator,
    access_units: *std.ArrayList([]const u8),
    access_unit_times_ns: *std.ArrayList(u64),
    cluster_ticks: u64,
    timestamp_scale_ns: u64,
) !void {
    var reader = BlockCursor{ .bytes = block_bytes };
    const block = try describeBlock(&reader, block_bytes.len, target_track, cluster_ticks, timestamp_scale_ns) orelse return;
    var offset = reader.position;
    for (block.sizes[0..block.count]) |size| {
        try access_units.append(allocator, block_bytes[offset..][0..size]);
        try access_unit_times_ns.append(allocator, block.time_ns);
        offset += size;
    }
}

const BlockCursor = struct {
    bytes: []const u8,
    position: usize = 0,

    fn byte(self: *BlockCursor) !u8 {
        if (self.position == self.bytes.len) return error.UnsupportedAudioFormat;
        const value = self.bytes[self.position];
        self.position += 1;
        return value;
    }

    fn vint(self: anytype) !SizeResult {
        var bytes: [8]u8 = undefined;
        bytes[0] = try self.byte();
        const len = try vintLength(bytes[0]);
        for (bytes[1..len]) |*value| value.* = try self.byte();
        return readVint(bytes[0..len], 0);
    }
};

const BlockLayout = struct {
    sizes: [256]usize = undefined,
    count: usize,
    time_ns: u64,
};

fn describeBlock(reader: anytype, length: usize, target: u64, ticks: u64, scale: u64) !?BlockLayout {
    const track = try BlockCursor.vint(reader);
    if (track.is_unknown) return error.UnsupportedAudioFormat;
    if (track.value != target) return null;
    const high = try reader.byte();
    const low = try reader.byte();
    const relative = std.mem.readInt(i16, &.{ high, low }, .big);
    const flags = try reader.byte();
    const absolute = if (relative < 0) ticks -| @as(u64, @intCast(-@as(i32, relative))) else try std.math.add(u64, ticks, @intCast(relative));
    var result = BlockLayout{ .count = 1, .time_ns = try std.math.mul(u64, absolute, scale) };
    const lacing: u2 = @truncate(flags >> 1);
    if (lacing != 0) result.count = @as(usize, try reader.byte()) + 1;
    var total: usize = 0;
    switch (lacing) {
        1 => for (result.sizes[0 .. result.count - 1]) |*size| {
            size.* = 0;
            while (true) {
                const value = try reader.byte();
                size.* = try std.math.add(usize, size.*, value);
                if (value != 255) break;
            }
            total = try std.math.add(usize, total, size.*);
        },
        2 => {
            const remaining = length - reader.position;
            if (remaining % result.count != 0) return error.UnsupportedAudioFormat;
            @memset(result.sizes[0..result.count], remaining / result.count);
            total = remaining - result.sizes[0];
        },
        3 => if (result.count > 1) {
            const first = try BlockCursor.vint(reader);
            if (first.is_unknown) return error.UnsupportedAudioFormat;
            var previous: i64 = @intCast(first.value);
            result.sizes[0] = std.math.cast(usize, previous) orelse return error.UnsupportedAudioFormat;
            total = result.sizes[0];
            for (result.sizes[1 .. result.count - 1]) |*size| {
                const encoded = try BlockCursor.vint(reader);
                if (encoded.is_unknown) return error.UnsupportedAudioFormat;
                const bias = (@as(i64, 1) << @intCast(7 * encoded.len - 1)) - 1;
                previous = try std.math.add(i64, previous, @as(i64, @intCast(encoded.value)) - bias);
                size.* = std.math.cast(usize, previous) orelse return error.UnsupportedAudioFormat;
                total = try std.math.add(usize, total, size.*);
            }
        },
        0 => {},
    }
    const remaining = length - reader.position;
    if (total > remaining) return error.UnsupportedAudioFormat;
    result.sizes[result.count - 1] = remaining - total;
    for (result.sizes[0..result.count]) |size| if (size == 0) return error.UnsupportedAudioFormat;
    return result;
}

const Source = @import("source.zig").Source;
const Webm = @import("webm.zig");
pub const Packet = struct {
    offset: u64,
    time_ns: u64,
    size: usize,
    prefix: [16]u8 = @splat(0),
};

pub const PacketSource = struct {
    source: *Source,
    packets: []const Packet,
    seek_pre_roll_ns: u64 = 0,

    pub fn read(self: PacketSource, index: usize) !@import("source.zig").Lease {
        if (index >= self.packets.len) return error.UnsupportedAudioFormat;
        const packet = self.packets[index];
        return self.source.readSparse(packet.offset, packet.size);
    }
};

/// Immutable packet geometry. The directory holds no source or payload lease.
pub const Directory = struct {
    track: TrackInfo,
    packets: []Packet,
    padding: []BlockPadding,
    allocator: std.mem.Allocator,

    pub fn init(a: std.mem.Allocator, source: *Source, limits: Webm.Limits) !Directory {
        return initWithFlacPacketLimit(a, source, limits, limits.max_packet_bytes);
    }

    /// FLAC packets may hold multiple frames and need not fit in one read.
    pub fn initWithFlacPacketLimit(a: std.mem.Allocator, source: *Source, limits: Webm.Limits, max_flac_packet_bytes: usize) !Directory {
        var scan = Webm.Scan{ .input = source, .limits = limits };
        const header = try scan.rawHeader(0, source.length());
        if (header.id != ebml_header_id or header.unknown) return error.UnsupportedAudioFormat;
        var offset = header.end;
        while (offset < source.length()) {
            const element = try scan.rawHeader(offset, source.length());
            if (element.id == segment_id) return scanSegment(a, &scan, element, max_flac_packet_bytes);
            if (element.unknown) return error.UnsupportedAudioFormat;
            offset = element.end;
        }
        return error.UnsupportedAudioFormat;
    }

    pub fn deinit(self: Directory) void {
        self.allocator.free(self.track.codec_private);
        self.allocator.free(self.packets);
        self.allocator.free(self.padding);
    }
};

const RangeBlock = struct {
    source: *Source,
    start: u64,
    length: usize,
    position: usize = 0,
    lease: ?@import("source.zig").Lease = null,
    lease_start: usize = 0,

    fn byte(self: *RangeBlock) !u8 {
        if (self.position == self.length) return error.UnsupportedAudioFormat;
        if (self.lease == null or self.position - self.lease_start >= self.lease.?.bytes.len) {
            self.close();
            self.lease_start = self.position;
            self.lease = try self.source.readSparse(self.start + self.position, @min(if (self.position == 0) @as(usize, 12) else 64, self.length - self.position));
        }
        const value = self.lease.?.bytes[self.position - self.lease_start];
        self.position += 1;
        return value;
    }

    fn close(self: *RangeBlock) void {
        if (self.lease) |*lease| lease.deinit();
        self.lease = null;
    }
};

fn scanSegment(a: std.mem.Allocator, scan: *Webm.Scan, segment: Webm.Element, max_flac_packet_bytes: usize) !Directory {
    var state = DemuxState{};
    defer if (state.track) |track| a.free(track.codec_private);
    var packets: std.ArrayList(Packet) = .empty;
    defer packets.deinit(a);
    defer state.padding.deinit(a);
    var pos = segment.payload;
    while (pos < segment.end) {
        const elem = try scan.rawHeader(pos, segment.end);
        if (elem.id == cluster_id) {
            const track = state.track orelse return error.UnsupportedAudioFormat;
            const packet_limit = if (track.codec == .flac) max_flac_packet_bytes else scan.limits.max_packet_bytes;
            pos = try scanCluster(a, scan, elem, track, state.timestamp_scale_ns, &packets, &state.padding, packet_limit);
            continue;
        }
        if (elem.unknown) return error.UnsupportedAudioFormat;
        const size = elem.end - elem.payload;
        if (elem.id == info_id or (elem.id == tracks_id and state.track == null)) {
            if (size > scan.limits.max_metadata_bytes) return error.ResourceLimitExceeded;
            var lease = try scan.input.readSparse(elem.payload, @intCast(size));
            defer lease.deinit();
            if (elem.id == info_id) {
                try parseInfo(lease.bytes, &state);
            } else {
                var parsed = DemuxState{};
                try parseTracks(lease.bytes, &parsed);
                if (parsed.track) |track| {
                    if (track.codec == null or track.codec_private.len == 0) return error.UnsupportedAudioFormat;
                    var owned = track;
                    owned.codec_private = try a.dupe(u8, track.codec_private);
                    state.track = owned;
                }
            }
        }
        pos = elem.end;
    }
    const track = state.track orelse return error.UnsupportedAudioFormat;
    if (packets.items.len == 0) return error.UnsupportedAudioFormat;
    const owned = try packets.toOwnedSlice(a);
    errdefer a.free(owned);
    const padding = try state.padding.toOwnedSlice(a);
    state.track = null;
    return .{ .track = track, .packets = owned, .padding = padding, .allocator = a };
}

fn scanCluster(a: std.mem.Allocator, scan: *Webm.Scan, cluster: Webm.Element, track: TrackInfo, scale: u64, packets: *std.ArrayList(Packet), padding: *std.ArrayList(BlockPadding), packet_limit: usize) !u64 {
    var ticks: u64 = 0;
    var pos = cluster.payload;
    while (pos < cluster.end) {
        const child = try scan.rawHeader(pos, cluster.end);
        if (cluster.unknown and !isClusterChildId(child.id)) break;
        if (child.unknown) return error.UnsupportedAudioFormat;
        const before = packets.items.len;
        switch (child.id) {
            timecode_id => {
                if (child.end - child.payload > 8) return error.UnsupportedAudioFormat;
                var lease = try scan.input.readSparse(child.payload, @intCast(child.end - child.payload));
                defer lease.deinit();
                ticks = try readUint(lease.bytes);
            },
            simple_block_id => try indexBlock(a, scan, child, track, ticks, scale, packets, packet_limit),
            block_group_id => {
                var discard: ?Webm.Element = null;
                var nested = child.payload;
                while (nested < child.end) {
                    const field = try scan.rawHeader(nested, child.end);
                    if (field.unknown) return error.UnsupportedAudioFormat;
                    switch (field.id) {
                        block_id => try indexBlock(a, scan, field, track, ticks, scale, packets, packet_limit),
                        discard_padding_id => discard = field,
                        else => {},
                    }
                    nested = field.end;
                }
                if (packets.items.len != before) if (discard) |field| {
                    if (field.end - field.payload > 8) return error.UnsupportedAudioFormat;
                    var lease = try scan.input.readSparse(field.payload, @intCast(field.end - field.payload));
                    defer lease.deinit();
                    const ns = try readSignedInt(lease.bytes);
                    if (ns != 0) try padding.append(a, .{ .first = before, .end = packets.items.len, .ns = ns });
                };
            },
            else => {},
        }
        pos = child.end;
    }
    return pos;
}

fn indexBlock(a: std.mem.Allocator, scan: *Webm.Scan, elem: Webm.Element, track: TrackInfo, ticks: u64, scale: u64, packets: *std.ArrayList(Packet), packet_limit: usize) !void {
    const length = std.math.cast(usize, elem.end - elem.payload) orelse return error.ResourceLimitExceeded;
    var reader = RangeBlock{ .source = scan.input, .start = elem.payload, .length = length };
    defer reader.close();
    const block = try describeBlock(&reader, length, track.number, ticks, scale) orelse return;
    const limit = @min(scan.limits.max_packets, scan.limits.max_index_bytes / @sizeOf(Packet));
    const count = packets.items.len + block.count;
    if (count > limit) return error.ResourceLimitExceeded;
    if (count > packets.capacity) try packets.ensureTotalCapacityPrecise(a, @min(limit, @max(count, packets.capacity *| 2)));
    var offset = reader.position;
    for (block.sizes[0..block.count]) |size| {
        if (size > packet_limit) return error.ResourceLimitExceeded;
        var packet = Packet{ .offset = elem.payload + offset, .time_ns = block.time_ns, .size = size };
        reader.position = offset;
        for (packet.prefix[0..@min(size, if (track.codec == .flac) packet.prefix.len else 2)]) |*value| value.* = try reader.byte();
        packets.appendAssumeCapacity(packet);
        offset += size;
    }
}

test "WebM borrowed and range indexes preserve signed block padding" {
    const a = std.testing.allocator;
    const track = TrackInfo{ .number = 1, .codec = .opus };
    const padded_block = [_]u8{ 0xa0, 0x8b, 0xa1, 0x85, 0x81, 0, 0, 0, 0, 0x75, 0xa2, 0x81, 1 };
    const plain_block = [_]u8{ 0xa3, 0x85, 0x81, 0, 0, 0, 0 };
    for ([_][]const u8{ &(padded_block ++ plain_block), &padded_block }) |contents| {
        var bytes: [padded_block.len + plain_block.len]u8 = undefined;
        @memcpy(bytes[0..contents.len], contents);
        const interior = contents.len != padded_block.len;
        if (!interior) bytes[padded_block.len - 1] = 0xff;

        var units: std.ArrayList([]const u8) = .empty;
        defer units.deinit(a);
        var times: std.ArrayList(u64) = .empty;
        defer times.deinit(a);
        var borrowed_padding: std.ArrayList(BlockPadding) = .empty;
        defer borrowed_padding.deinit(a);
        _ = try parseCluster(bytes[0..contents.len], contents.len, track.number, a, &units, &times, default_timestamp_scale_ns, &borrowed_padding);

        var source = Source{ .allocator = a, .identity = "block-padding", .storage = .{ .borrowed = bytes[0..contents.len] } };
        var scan = Webm.Scan{ .input = &source, .limits = .{} };
        var packets: std.ArrayList(Packet) = .empty;
        defer packets.deinit(a);
        var range_padding: std.ArrayList(BlockPadding) = .empty;
        defer range_padding.deinit(a);
        _ = try scanCluster(a, &scan, .{ .id = cluster_id, .payload = 0, .end = contents.len, .unknown = false }, track, default_timestamp_scale_ns, &packets, &range_padding, 1024);
        try std.testing.expectEqualSlices(BlockPadding, &.{.{ .first = 0, .end = 1, .ns = if (interior) 1 else -1 }}, borrowed_padding.items);
        try std.testing.expectEqualSlices(BlockPadding, borrowed_padding.items, range_padding.items);
    }
}
