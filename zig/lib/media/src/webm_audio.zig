// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
    discard_padding_ns: i64,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *const DemuxedAudio) void {
        self.allocator.free(self.access_units);
        self.allocator.free(self.access_unit_times_ns);
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

    try parseSegment(allocator, segment_payload, &state);

    const track = state.track orelse return error.UnsupportedAudioFormat;
    const codec = track.codec orelse return error.UnsupportedAudioFormat;
    if (state.access_units.items.len == 0) return error.UnsupportedAudioFormat;
    if (track.codec_private.len == 0) return error.UnsupportedAudioFormat;

    const access_units = try state.access_units.toOwnedSlice(allocator);
    errdefer allocator.free(access_units);
    const access_unit_times_ns = try state.access_unit_times_ns.toOwnedSlice(allocator);

    return .{
        .codec = codec,
        .channels = track.channels,
        .codec_delay_ns = track.codec_delay_ns,
        .seek_pre_roll_ns = track.seek_pre_roll_ns,
        .codec_private = track.codec_private,
        .access_units = access_units,
        .access_unit_times_ns = access_unit_times_ns,
        .discard_padding_ns = state.discard_padding_ns,
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
    discard_padding_ns: i64 = 0,
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
                    &state.discard_padding_ns,
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
    discard_padding_ns: *i64,
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
                discard_padding_ns,
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
    discard_padding_ns: *i64,
) !void {
    var cursor: usize = 0;
    var matched = false;
    var pending_discard: ?[]const u8 = null;

    while (cursor < payload.len) {
        const elem = try readElementHeader(payload, cursor);
        switch (elem.id) {
            block_id => {
                const before = access_units.items.len;
                try parseBlockIntoAccessUnits(
                    try elementPayload(payload, elem),
                    target_track,
                    allocator,
                    access_units,
                    access_unit_times_ns,
                    cluster_ticks,
                    timestamp_scale_ns,
                );
                matched = access_units.items.len > before;
            },
            discard_padding_id => pending_discard = try elementPayload(payload, elem),
            else => {},
        }
        cursor = elem.data_end orelse return error.UnsupportedAudioFormat;
    }

    if (matched) {
        if (pending_discard) |bytes| discard_padding_ns.* = try readSignedInt(bytes);
    }
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
    var cursor: usize = 0;
    const track_vint = try readVint(block_bytes, 0);
    cursor += track_vint.len;
    if (cursor + 3 > block_bytes.len) return error.UnsupportedAudioFormat;

    const relative_ticks = @as(i16, @bitCast(std.mem.readInt(u16, block_bytes[cursor..][0..2], .big)));
    const flags = block_bytes[cursor + 2];
    cursor += 3;
    if (track_vint.value != target_track) return;

    // A block before its cluster's timestamp is clamped to the start; the
    // alternative is a negative position on the timeline.
    const absolute_ticks: u64 = if (relative_ticks < 0)
        cluster_ticks -| @as(u64, @intCast(-@as(i32, relative_ticks)))
    else
        cluster_ticks + @as(u64, @intCast(relative_ticks));
    const time_ns = std.math.mul(u64, absolute_ticks, timestamp_scale_ns) catch
        return error.UnsupportedAudioFormat;

    const lacing: u2 = @intCast((flags & 0x06) >> 1);
    if (lacing == 0) {
        const frame = block_bytes[cursor..];
        if (frame.len == 0) return error.UnsupportedAudioFormat;
        try access_units.append(allocator, frame);
        try access_unit_times_ns.append(allocator, time_ns);
        return;
    }

    if (cursor >= block_bytes.len) return error.UnsupportedAudioFormat;
    const frame_count = @as(usize, block_bytes[cursor]) + 1;
    cursor += 1;
    if (frame_count == 0) return error.UnsupportedAudioFormat;

    const sizes = try allocator.alloc(usize, frame_count);
    defer allocator.free(sizes);

    switch (lacing) {
        1 => { // Xiph lacing.
            var total: usize = 0;
            for (0..frame_count - 1) |i| {
                var size: usize = 0;
                while (true) {
                    if (cursor >= block_bytes.len) return error.UnsupportedAudioFormat;
                    const b = block_bytes[cursor];
                    cursor += 1;
                    size += b;
                    if (b != 0xff) break;
                }
                sizes[i] = size;
                total += size;
            }
            const remaining = block_bytes.len - cursor;
            if (total > remaining) return error.UnsupportedAudioFormat;
            sizes[frame_count - 1] = remaining - total;
        },
        2 => { // Fixed-size lacing.
            const remaining = block_bytes.len - cursor;
            if (remaining % frame_count != 0) return error.UnsupportedAudioFormat;
            const each = remaining / frame_count;
            for (0..frame_count) |i| sizes[i] = each;
        },
        3 => { // EBML lacing.
            if (frame_count >= 2) {
                const first = try readVint(block_bytes, cursor);
                cursor += first.len;
                sizes[0] = std.math.cast(usize, first.value) orelse return error.UnsupportedAudioFormat;
                var prev_signed: i64 = @intCast(first.value);
                var total: usize = sizes[0];
                for (1..frame_count - 1) |i| {
                    const delta_vint = try readVint(block_bytes, cursor);
                    cursor += delta_vint.len;
                    const bias: i64 = (@as(i64, 1) << @intCast(7 * delta_vint.len - 1)) - 1;
                    const delta: i64 = @as(i64, @intCast(delta_vint.value)) - bias;
                    const size_signed = prev_signed + delta;
                    if (size_signed < 0) return error.UnsupportedAudioFormat;
                    sizes[i] = @intCast(size_signed);
                    total += sizes[i];
                    prev_signed = size_signed;
                }
                const remaining = block_bytes.len - cursor;
                if (total > remaining) return error.UnsupportedAudioFormat;
                sizes[frame_count - 1] = remaining - total;
            } else {
                sizes[0] = block_bytes.len - cursor;
            }
        },
        else => unreachable,
    }

    for (sizes) |size| {
        if (size > block_bytes.len - cursor) return error.UnsupportedAudioFormat;
        try access_units.append(allocator, block_bytes[cursor .. cursor + size]);
        try access_unit_times_ns.append(allocator, time_ns);
        cursor += size;
    }
}
