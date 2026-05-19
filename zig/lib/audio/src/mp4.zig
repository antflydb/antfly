// Copyright 2026 Antfly, Inc.
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

const tone_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.m4a");
const tone_mp4_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.mp4");

pub const Codec = enum {
    aac,
    alac,
};

pub const DemuxedAudio = struct {
    codec: Codec,
    sample_rate: u32,
    channels: u16,
    decoder_config: []const u8,
    access_units: [][]const u8,
    trim_start_frames: u64 = 0,
    playable_frames: ?u64 = null,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *const DemuxedAudio) void {
        self.allocator.free(self.access_units);
    }
};

const StscEntry = struct {
    first_chunk: u32,
    samples_per_chunk: u32,
    sample_description_index: u32,
};

const TrackTables = struct {
    codec: ?Codec = null,
    sample_rate: u32 = 0,
    channels: u16 = 0,
    decoder_config: []const u8 = &.{},
    movie_timescale: u32 = 0,
    media_timescale: u32 = 0,
    edit_entries: std.ArrayListUnmanaged(EditListEntry) = .empty,
    total_sample_duration: u64 = 0,
    sample_description_index: ?u32 = null,
    chunk_offsets: std.ArrayListUnmanaged(u64) = .empty,
    sample_sizes: std.ArrayListUnmanaged(u32) = .empty,
    stsc_entries: std.ArrayListUnmanaged(StscEntry) = .empty,

    fn deinit(self: *TrackTables, allocator: std.mem.Allocator) void {
        self.edit_entries.deinit(allocator);
        self.chunk_offsets.deinit(allocator);
        self.sample_sizes.deinit(allocator);
        self.stsc_entries.deinit(allocator);
    }
};

pub fn demux(allocator: std.mem.Allocator, audio_bytes: []const u8) !DemuxedAudio {
    var tables = TrackTables{};
    defer tables.deinit(allocator);

    try parseTopLevel(allocator, audio_bytes, &tables);

    const codec = tables.codec orelse return error.UnsupportedAudioFormat;
    if (tables.sample_rate == 0 or tables.channels == 0) return error.UnsupportedAudioFormat;
    if (tables.decoder_config.len == 0) return error.UnsupportedAudioFormat;
    if (tables.chunk_offsets.items.len == 0 or tables.sample_sizes.items.len == 0 or tables.stsc_entries.items.len == 0) {
        return error.UnsupportedAudioFormat;
    }

    const access_units = try buildAccessUnits(
        allocator,
        audio_bytes,
        tables.chunk_offsets.items,
        tables.sample_sizes.items,
        tables.stsc_entries.items,
    );
    const edit_trim = try resolveEditListTrim(tables);

    return .{
        .codec = codec,
        .sample_rate = tables.sample_rate,
        .channels = tables.channels,
        .decoder_config = tables.decoder_config,
        .access_units = access_units,
        .trim_start_frames = edit_trim.trim_start_frames,
        .playable_frames = edit_trim.playable_frames,
        .allocator = allocator,
    };
}

fn parseTopLevel(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    tables: *TrackTables,
) !void {
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        const box = try readBox(bytes, cursor);
        switch (box.typ) {
            fourcc("moov") => try parseMoov(allocator, box.payload, tables),
            else => {},
        }
        cursor = box.end;
    }
}

fn parseMoov(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    var cursor: usize = 0;
    while (cursor < payload.len) {
        const box = try readBox(payload, cursor);
        switch (box.typ) {
            fourcc("mvhd") => tables.movie_timescale = try parseMvhdTimescale(box.payload),
            fourcc("trak") => if (tables.codec == null) {
                var candidate = TrackTables{ .movie_timescale = tables.movie_timescale };
                parseTrak(allocator, box.payload, &candidate) catch |err| switch (err) {
                    error.UnsupportedAudioFormat => {
                        candidate.deinit(allocator);
                        cursor = box.end;
                        continue;
                    },
                    else => {
                        candidate.deinit(allocator);
                        return err;
                    },
                };
                if (candidate.codec != null) replaceSelectedTrack(allocator, tables, &candidate);
                candidate.deinit(allocator);
            },
            else => {},
        }
        cursor = box.end;
    }
}

fn replaceSelectedTrack(
    allocator: std.mem.Allocator,
    tables: *TrackTables,
    candidate: *TrackTables,
) void {
    tables.edit_entries.deinit(allocator);
    tables.edit_entries = .empty;
    tables.chunk_offsets.deinit(allocator);
    tables.chunk_offsets = .empty;
    tables.sample_sizes.deinit(allocator);
    tables.sample_sizes = .empty;
    tables.stsc_entries.deinit(allocator);
    tables.stsc_entries = .empty;

    tables.codec = candidate.codec;
    tables.sample_rate = candidate.sample_rate;
    tables.channels = candidate.channels;
    tables.decoder_config = candidate.decoder_config;
    tables.media_timescale = candidate.media_timescale;
    tables.edit_entries = candidate.edit_entries;
    tables.total_sample_duration = candidate.total_sample_duration;
    tables.sample_description_index = candidate.sample_description_index;
    tables.chunk_offsets = candidate.chunk_offsets;
    tables.sample_sizes = candidate.sample_sizes;
    tables.stsc_entries = candidate.stsc_entries;

    candidate.codec = null;
    candidate.sample_rate = 0;
    candidate.channels = 0;
    candidate.decoder_config = &.{};
    candidate.media_timescale = 0;
    candidate.edit_entries = .empty;
    candidate.total_sample_duration = 0;
    candidate.sample_description_index = null;
    candidate.chunk_offsets = .empty;
    candidate.sample_sizes = .empty;
    candidate.stsc_entries = .empty;
}

fn parseTrak(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    const prior_codec = tables.codec;
    tables.media_timescale = 0;
    tables.edit_entries.clearRetainingCapacity();
    tables.total_sample_duration = 0;

    var cursor: usize = 0;
    while (cursor < payload.len) {
        const box = try readBox(payload, cursor);
        switch (box.typ) {
            fourcc("edts") => try parseEdts(allocator, box.payload, tables),
            fourcc("mdia") => try parseMdia(allocator, box.payload, tables),
            else => {},
        }
        cursor = box.end;
    }

    if (tables.codec == prior_codec) {
        tables.media_timescale = 0;
        tables.edit_entries.clearRetainingCapacity();
        tables.total_sample_duration = 0;
    }
}

fn parseMdia(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    var cursor: usize = 0;
    var is_audio = false;
    var media_timescale: u32 = 0;
    var minf_payload: ?[]const u8 = null;
    while (cursor < payload.len) {
        const box = try readBox(payload, cursor);
        switch (box.typ) {
            fourcc("mdhd") => media_timescale = try parseMdhdTimescale(box.payload),
            fourcc("hdlr") => is_audio = isAudioHandler(box.payload),
            fourcc("minf") => minf_payload = box.payload,
            else => {},
        }
        cursor = box.end;
    }

    if (is_audio) {
        const minf = minf_payload orelse return;
        tables.media_timescale = media_timescale;
        try parseMinf(allocator, minf, tables);
    }
}

fn parseMinf(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    var cursor: usize = 0;
    while (cursor < payload.len) {
        const box = try readBox(payload, cursor);
        if (box.typ == fourcc("stbl")) {
            try parseStbl(allocator, box.payload, tables);
            return;
        }
        cursor = box.end;
    }
}

fn parseStbl(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    var cursor: usize = 0;
    var stsd_payload: ?[]const u8 = null;
    while (cursor < payload.len) {
        const box = try readBox(payload, cursor);
        switch (box.typ) {
            fourcc("stsd") => stsd_payload = box.payload,
            fourcc("stsc") => try parseStsc(allocator, box.payload, tables),
            fourcc("stsz") => try parseStsz(allocator, box.payload, tables),
            fourcc("stz2") => try parseStz2(allocator, box.payload, tables),
            fourcc("stts") => try parseStts(box.payload, tables),
            fourcc("stco") => try parseStco(allocator, box.payload, tables),
            fourcc("co64") => try parseCo64(allocator, box.payload, tables),
            else => {},
        }
        cursor = box.end;
    }

    if (stsd_payload) |stsd| try parseStsd(stsd, tables);
}

fn parseMvhdTimescale(payload: []const u8) !u32 {
    if (payload.len < 16) return error.UnsupportedAudioFormat;
    return switch (payload[0]) {
        0 => readU32(payload[12..16]),
        1 => blk: {
            if (payload.len < 28) return error.UnsupportedAudioFormat;
            break :blk readU32(payload[20..24]);
        },
        else => error.UnsupportedAudioFormat,
    };
}

fn parseMdhdTimescale(payload: []const u8) !u32 {
    if (payload.len < 16) return error.UnsupportedAudioFormat;
    return switch (payload[0]) {
        0 => readU32(payload[12..16]),
        1 => blk: {
            if (payload.len < 28) return error.UnsupportedAudioFormat;
            break :blk readU32(payload[20..24]);
        },
        else => error.UnsupportedAudioFormat,
    };
}

fn parseEdts(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    var cursor: usize = 0;
    while (cursor < payload.len) {
        const box = try readBox(payload, cursor);
        if (box.typ == fourcc("elst")) {
            try parseElst(allocator, box.payload, tables);
            return;
        }
        cursor = box.end;
    }
}

fn parseElst(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 8) return error.UnsupportedAudioFormat;
    const version = payload[0];
    const entry_count = readU32(payload[4..8]);
    if (entry_count == 0) return error.UnsupportedAudioFormat;

    const entry_size: usize = switch (version) {
        0 => 12,
        1 => 20,
        else => return error.UnsupportedAudioFormat,
    };
    const entries_len = try std.math.mul(usize, entry_count, entry_size);
    if (payload.len < 8 + entries_len) return error.UnsupportedAudioFormat;

    tables.edit_entries.clearRetainingCapacity();
    try tables.edit_entries.ensureTotalCapacity(allocator, entry_count);
    var cursor: usize = 8;
    for (0..entry_count) |_| {
        const entry = try parseElstEntry(version, payload[cursor .. cursor + entry_size]);
        cursor += entry_size;

        if (entry.media_time < -1) return error.UnsupportedAudioFormat;
        tables.edit_entries.appendAssumeCapacity(entry);
    }
}

const EditListEntry = struct {
    segment_duration: u64,
    media_time: i64,
};

fn parseElstEntry(version: u8, payload: []const u8) !EditListEntry {
    return switch (version) {
        0 => blk: {
            if (payload.len < 12) return error.UnsupportedAudioFormat;
            const segment_duration = readU32(payload[0..4]);
            const media_time_raw = readU32(payload[4..8]);
            const media_rate_integer = readU16(payload[8..10]);
            const media_rate_fraction = readU16(payload[10..12]);
            if (media_rate_integer != 1 or media_rate_fraction != 0) return error.UnsupportedAudioFormat;
            break :blk .{
                .segment_duration = segment_duration,
                .media_time = @as(i32, @bitCast(media_time_raw)),
            };
        },
        1 => blk: {
            if (payload.len < 20) return error.UnsupportedAudioFormat;
            const segment_duration = readU64(payload[0..8]);
            const media_time_raw = readU64(payload[8..16]);
            const media_rate_integer = readU16(payload[16..18]);
            const media_rate_fraction = readU16(payload[18..20]);
            if (media_rate_integer != 1 or media_rate_fraction != 0) return error.UnsupportedAudioFormat;
            break :blk .{
                .segment_duration = segment_duration,
                .media_time = @as(i64, @bitCast(media_time_raw)),
            };
        },
        else => error.UnsupportedAudioFormat,
    };
}

fn parseStsd(payload: []const u8, tables: *TrackTables) !void {
    if (payload.len < 8) return error.UnsupportedAudioFormat;
    const entry_count = readU32(payload[4..8]);
    if (entry_count == 0) return error.UnsupportedAudioFormat;
    const selected_description_index = tables.sample_description_index orelse 1;
    var cursor: usize = 8;
    for (0..entry_count) |entry_index_zero_based| {
        if (cursor >= payload.len) return error.UnsupportedAudioFormat;
        const entry = try readBox(payload, cursor);
        const entry_index: u32 = @intCast(entry_index_zero_based + 1);
        if (entry_index == selected_description_index) {
            try parseSampleEntry(entry.typ, entry.data, entry.payload, tables);
        }
        cursor = entry.end;
    }
    if (cursor != payload.len) return error.UnsupportedAudioFormat;
    if (tables.codec == null) return error.UnsupportedAudioFormat;
}

fn parseSampleEntry(
    sample_entry_type: u32,
    entry_bytes: []const u8,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 28) return error.UnsupportedAudioFormat;
    const layout = try parseAudioSampleEntryLayout(payload);
    tables.channels = layout.channels;
    tables.sample_rate = layout.sample_rate;
    switch (sample_entry_type) {
        fourcc("mp4a") => {
            tables.codec = .aac;
            tables.decoder_config = try parseEsdsDecoderConfig(payload[layout.child_offset..]);
        },
        fourcc("alac") => {
            tables.codec = .alac;
            tables.decoder_config = try parseAlacDecoderConfig(entry_bytes, payload[layout.child_offset..]);
        },
        else => return error.UnsupportedAudioFormat,
    }
}

const AudioSampleEntryLayout = struct {
    child_offset: usize,
    channels: u16,
    sample_rate: u32,
};

fn parseAudioSampleEntryLayout(payload: []const u8) !AudioSampleEntryLayout {
    if (payload.len < 28) return error.UnsupportedAudioFormat;
    const version = readU16(payload[8..10]);
    const child_offset: usize = switch (version) {
        0 => 28,
        1 => 44,
        2 => 64,
        else => return error.UnsupportedAudioFormat,
    };
    if (payload.len < child_offset) return error.UnsupportedAudioFormat;

    if (version == 2) {
        const sample_rate_float = @as(f64, @bitCast(readU64(payload[32..40])));
        if (!std.math.isFinite(sample_rate_float) or sample_rate_float <= 0) return error.UnsupportedAudioFormat;
        if (sample_rate_float > @as(f64, @floatFromInt(std.math.maxInt(u32)))) return error.UnsupportedAudioFormat;
        return .{
            .child_offset = child_offset,
            .channels = std.math.cast(u16, readU32(payload[40..44])) orelse return error.UnsupportedAudioFormat,
            .sample_rate = @intFromFloat(@round(sample_rate_float)),
        };
    }

    return .{
        .child_offset = child_offset,
        .channels = readU16(payload[16..18]),
        .sample_rate = readU32(payload[24..28]) >> 16,
    };
}

fn parseStsc(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 8) return error.UnsupportedAudioFormat;
    const entry_count = readU32(payload[4..8]);
    if (entry_count == 0) return error.UnsupportedAudioFormat;
    var cursor: usize = 8;
    try tables.stsc_entries.ensureTotalCapacity(allocator, entry_count);
    var previous_first_chunk: u32 = 0;
    for (0..entry_count) |_| {
        if (cursor + 12 > payload.len) return error.UnsupportedAudioFormat;
        const first_chunk = readU32(payload[cursor..][0..4]);
        const samples_per_chunk = readU32(payload[cursor + 4 ..][0..4]);
        const sample_description_index = readU32(payload[cursor + 8 ..][0..4]);
        if (first_chunk == 0 or samples_per_chunk == 0) return error.UnsupportedAudioFormat;
        if (previous_first_chunk != 0 and first_chunk <= previous_first_chunk) return error.UnsupportedAudioFormat;
        if (previous_first_chunk == 0 and first_chunk != 1) return error.UnsupportedAudioFormat;
        if (sample_description_index == 0) return error.UnsupportedAudioFormat;
        if (tables.sample_description_index) |selected| {
            if (sample_description_index != selected) return error.UnsupportedAudioFormat;
        } else {
            tables.sample_description_index = sample_description_index;
        }
        tables.stsc_entries.appendAssumeCapacity(.{
            .first_chunk = first_chunk,
            .samples_per_chunk = samples_per_chunk,
            .sample_description_index = sample_description_index,
        });
        previous_first_chunk = first_chunk;
        cursor += 12;
    }
}

fn parseStsz(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 12) return error.UnsupportedAudioFormat;
    if (tables.sample_sizes.items.len != 0) return error.UnsupportedAudioFormat;
    const uniform_size = readU32(payload[4..8]);
    const sample_count = readU32(payload[8..12]);
    try tables.sample_sizes.ensureTotalCapacity(allocator, sample_count);
    if (uniform_size != 0) {
        for (0..sample_count) |_| {
            tables.sample_sizes.appendAssumeCapacity(uniform_size);
        }
        return;
    }

    var cursor: usize = 12;
    for (0..sample_count) |_| {
        if (cursor + 4 > payload.len) return error.UnsupportedAudioFormat;
        tables.sample_sizes.appendAssumeCapacity(readU32(payload[cursor..][0..4]));
        cursor += 4;
    }
}

fn parseStz2(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 12) return error.UnsupportedAudioFormat;
    if (tables.sample_sizes.items.len != 0) return error.UnsupportedAudioFormat;

    const field_size = payload[7];
    const sample_count_u32 = readU32(payload[8..12]);
    const sample_count = std.math.cast(usize, sample_count_u32) orelse return error.UnsupportedAudioFormat;
    try tables.sample_sizes.ensureTotalCapacity(allocator, sample_count);

    switch (field_size) {
        4 => {
            const packed_len = (sample_count + 1) / 2;
            if (payload.len < 12 + packed_len) return error.UnsupportedAudioFormat;
            for (0..sample_count) |i| {
                const packed_byte = payload[12 + i / 2];
                const size = if ((i & 1) == 0) packed_byte >> 4 else packed_byte & 0x0f;
                tables.sample_sizes.appendAssumeCapacity(size);
            }
        },
        8 => {
            if (payload.len < 12 + sample_count) return error.UnsupportedAudioFormat;
            for (payload[12 .. 12 + sample_count]) |size| {
                tables.sample_sizes.appendAssumeCapacity(size);
            }
        },
        16 => {
            const table_len = try std.math.mul(usize, sample_count, 2);
            if (payload.len < 12 + table_len) return error.UnsupportedAudioFormat;
            var cursor: usize = 12;
            for (0..sample_count) |_| {
                tables.sample_sizes.appendAssumeCapacity(readU16(payload[cursor..][0..2]));
                cursor += 2;
            }
        },
        else => return error.UnsupportedAudioFormat,
    }
}

fn parseStts(
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 8) return error.UnsupportedAudioFormat;
    const entry_count = readU32(payload[4..8]);
    var cursor: usize = 8;
    var total: u64 = 0;
    for (0..entry_count) |_| {
        if (cursor + 8 > payload.len) return error.UnsupportedAudioFormat;
        const sample_count = readU32(payload[cursor..][0..4]);
        const sample_delta = readU32(payload[cursor + 4 ..][0..4]);
        total = try std.math.add(u64, total, try std.math.mul(u64, sample_count, sample_delta));
        cursor += 8;
    }
    tables.total_sample_duration = total;
}

const EditListTrim = struct {
    trim_start_frames: u64 = 0,
    playable_frames: ?u64 = null,
};

fn resolveEditListTrim(tables: TrackTables) !EditListTrim {
    if (tables.edit_entries.items.len == 0) return .{};
    if (tables.movie_timescale == 0 or tables.media_timescale == 0) return error.UnsupportedAudioFormat;

    var trim_start_frames: ?u64 = null;
    var playable_frames: u64 = 0;
    var expected_next_media_time: ?u64 = null;
    var saw_gap_after_media = false;

    for (tables.edit_entries.items) |entry| {
        if (entry.media_time == -1) {
            if (trim_start_frames != null) saw_gap_after_media = true;
            continue;
        }
        if (entry.media_time < 0) return error.UnsupportedAudioFormat;
        if (saw_gap_after_media) return error.UnsupportedAudioFormat;

        const media_time = std.math.cast(u64, entry.media_time) orelse return error.UnsupportedAudioFormat;
        if (trim_start_frames != null) {
            const expected = expected_next_media_time orelse return error.UnsupportedAudioFormat;
            if (media_time != expected) return error.UnsupportedAudioFormat;
        } else {
            trim_start_frames = media_time;
        }

        const entry_playable_frames = try scaleDurationToMediaFrames(
            entry.segment_duration,
            tables.movie_timescale,
            tables.media_timescale,
        );
        playable_frames = try std.math.add(u64, playable_frames, entry_playable_frames);
        expected_next_media_time = try std.math.add(u64, media_time, entry_playable_frames);
    }

    const trim_start = trim_start_frames orelse return error.UnsupportedAudioFormat;
    return .{
        .trim_start_frames = trim_start,
        .playable_frames = playable_frames,
    };
}

fn scaleDurationToMediaFrames(duration: u64, src_timescale: u32, dst_timescale: u32) !u64 {
    if (src_timescale == 0 or dst_timescale == 0) return error.UnsupportedAudioFormat;
    const numerator = try std.math.mul(u128, duration, dst_timescale);
    const rounded = numerator + src_timescale / 2;
    return std.math.cast(u64, rounded / src_timescale) orelse error.UnsupportedAudioFormat;
}

fn parseStco(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 8) return error.UnsupportedAudioFormat;
    const entry_count = readU32(payload[4..8]);
    var cursor: usize = 8;
    try tables.chunk_offsets.ensureTotalCapacity(allocator, entry_count);
    for (0..entry_count) |_| {
        if (cursor + 4 > payload.len) return error.UnsupportedAudioFormat;
        tables.chunk_offsets.appendAssumeCapacity(readU32(payload[cursor..][0..4]));
        cursor += 4;
    }
}

fn parseCo64(
    allocator: std.mem.Allocator,
    payload: []const u8,
    tables: *TrackTables,
) !void {
    if (payload.len < 8) return error.UnsupportedAudioFormat;
    const entry_count = readU32(payload[4..8]);
    var cursor: usize = 8;
    try tables.chunk_offsets.ensureTotalCapacity(allocator, entry_count);
    for (0..entry_count) |_| {
        if (cursor + 8 > payload.len) return error.UnsupportedAudioFormat;
        tables.chunk_offsets.appendAssumeCapacity(readU64(payload[cursor..][0..8]));
        cursor += 8;
    }
}

fn buildAccessUnits(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    chunk_offsets: []const u64,
    sample_sizes: []const u32,
    stsc_entries: []const StscEntry,
) ![][]const u8 {
    const access_units = try allocator.alloc([]const u8, sample_sizes.len);
    errdefer allocator.free(access_units);

    var sample_index: usize = 0;
    for (chunk_offsets, 0..) |chunk_offset_u64, chunk_index| {
        const samples_per_chunk = samplesPerChunkForIndex(stsc_entries, @intCast(chunk_index + 1));
        var chunk_cursor: usize = std.math.cast(usize, chunk_offset_u64) orelse return error.UnsupportedAudioFormat;
        for (0..samples_per_chunk) |_| {
            if (sample_index >= sample_sizes.len) return error.UnsupportedAudioFormat;
            const sample_size = sample_sizes[sample_index];
            const sample_size_usize = std.math.cast(usize, sample_size) orelse return error.UnsupportedAudioFormat;
            if (chunk_cursor + sample_size_usize > bytes.len) return error.UnsupportedAudioFormat;
            access_units[sample_index] = bytes[chunk_cursor .. chunk_cursor + sample_size_usize];
            chunk_cursor += sample_size_usize;
            sample_index += 1;
        }
    }

    if (sample_index != sample_sizes.len) return error.UnsupportedAudioFormat;
    return access_units;
}

fn samplesPerChunkForIndex(entries: []const StscEntry, chunk_index_1_based: u32) u32 {
    var active = entries[0];
    for (entries[1..]) |entry| {
        if (chunk_index_1_based < entry.first_chunk) break;
        active = entry;
    }
    return active.samples_per_chunk;
}

fn parseEsdsDecoderConfig(children: []const u8) ![]const u8 {
    const esds = try findChildBoxRecursive(children, fourcc("esds"));
    if (esds.payload.len < 4) return error.UnsupportedAudioFormat;
    return findDescriptorRecursive(esds.payload[4..], 0x05) orelse error.UnsupportedAudioFormat;
}

fn parseAlacDecoderConfig(entry_bytes: []const u8, children: []const u8) ![]const u8 {
    _ = entry_bytes;
    const box = try findChildBoxRecursive(children, fourcc("alac"));
    return box.data;
}

fn findChildBoxRecursive(children: []const u8, target_type: u32) error{UnsupportedAudioFormat}!Box {
    var cursor: usize = 0;
    while (cursor < children.len) {
        const box = try readBox(children, cursor);
        if (box.typ == target_type) return box;
        if (box.typ == fourcc("wave")) {
            const nested = findChildBoxRecursive(box.payload, target_type) catch {
                cursor = box.end;
                continue;
            };
            return nested;
        }
        cursor = box.end;
    }
    return error.UnsupportedAudioFormat;
}

fn findDescriptorRecursive(payload: []const u8, target_tag: u8) ?[]const u8 {
    var cursor: usize = 0;
    while (cursor < payload.len) {
        if (cursor + 2 > payload.len) return null;
        const tag = payload[cursor];
        cursor += 1;
        const descriptor_size = readDescriptorSize(payload, &cursor) catch return null;
        if (cursor + descriptor_size > payload.len) return null;
        const descriptor_payload = payload[cursor .. cursor + descriptor_size];
        if (tag == target_tag) return descriptor_payload;
        if (descriptorChildren(tag, descriptor_payload)) |children| {
            if (findDescriptorRecursive(children, target_tag)) |found| return found;
        }
        cursor += descriptor_size;
    }
    return null;
}

fn descriptorChildren(tag: u8, payload: []const u8) ?[]const u8 {
    return switch (tag) {
        0x03 => blk: {
            if (payload.len < 3) break :blk null;
            var cursor: usize = 3; // ES_ID + flags
            const flags = payload[2];
            if ((flags & 0x80) != 0) {
                if (cursor + 2 > payload.len) break :blk null;
                cursor += 2;
            }
            if ((flags & 0x40) != 0) {
                if (cursor + 1 > payload.len) break :blk null;
                const url_len = payload[cursor];
                cursor += 1;
                if (cursor + url_len > payload.len) break :blk null;
                cursor += url_len;
            }
            if ((flags & 0x20) != 0) {
                if (cursor + 2 > payload.len) break :blk null;
                cursor += 2;
            }
            break :blk payload[cursor..];
        },
        0x04 => if (payload.len >= 13) payload[13..] else null,
        else => payload,
    };
}

fn readDescriptorSize(payload: []const u8, cursor: *usize) !usize {
    var size: usize = 0;
    var read_count: usize = 0;
    while (cursor.* < payload.len and read_count < 4) : (read_count += 1) {
        const b = payload[cursor.*];
        cursor.* += 1;
        size = (size << 7) | (b & 0x7f);
        if ((b & 0x80) == 0) return size;
    }
    return error.UnsupportedAudioFormat;
}

fn isAudioHandler(payload: []const u8) bool {
    return payload.len >= 12 and readU32(payload[8..12]) == fourcc("soun");
}

const Box = struct {
    typ: u32,
    data: []const u8,
    payload: []const u8,
    end: usize,
};

fn readBox(bytes: []const u8, start: usize) !Box {
    if (start + 8 > bytes.len) return error.UnsupportedAudioFormat;
    const size32 = readU32(bytes[start..][0..4]);
    const typ = readU32(bytes[start + 4 ..][0..4]);

    var header_len: usize = 8;
    var box_size: u64 = size32;
    if (size32 == 1) {
        if (start + 16 > bytes.len) return error.UnsupportedAudioFormat;
        box_size = readU64(bytes[start + 8 ..][0..8]);
        header_len = 16;
    } else if (size32 == 0) {
        box_size = bytes.len - start;
    }

    const end_u64 = @as(u64, start) + box_size;
    const end = std.math.cast(usize, end_u64) orelse return error.UnsupportedAudioFormat;
    if (box_size < header_len or end > bytes.len) return error.UnsupportedAudioFormat;

    return .{
        .typ = typ,
        .data = bytes[start..end],
        .payload = bytes[start + header_len .. end],
        .end = end,
    };
}

fn readU16(bytes: []const u8) u16 {
    return std.mem.readInt(u16, bytes[0..2], .big);
}

fn readU32(bytes: []const u8) u32 {
    return std.mem.readInt(u32, bytes[0..4], .big);
}

fn readU64(bytes: []const u8) u64 {
    return std.mem.readInt(u64, bytes[0..8], .big);
}

fn fourcc(tag: *const [4:0]u8) u32 {
    return std.mem.readInt(u32, tag, .big);
}

test "parse edit list accepts leading empty edit before media edit" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x80, // empty edit segment_duration
        0xff, 0xff, 0xff, 0xff, // media_time = -1
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
        0x00, 0x00, 0x03, 0xe8, // media edit segment_duration
        0x00, 0x00, 0x04, 0x00, // media_time = 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
    };

    var tables = TrackTables{
        .movie_timescale = 1000,
        .media_timescale = 44100,
    };
    defer tables.deinit(std.testing.allocator);
    try parseElst(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(@as(usize, 2), tables.edit_entries.items.len);
    try std.testing.expectEqual(@as(i64, -1), tables.edit_entries.items[0].media_time);
    try std.testing.expectEqual(@as(i64, 1024), tables.edit_entries.items[1].media_time);
    try std.testing.expectEqual(@as(u64, 1000), tables.edit_entries.items[1].segment_duration);

    const trim = try resolveEditListTrim(tables);
    try std.testing.expectEqual(@as(u64, 1024), trim.trim_start_frames);
    try std.testing.expectEqual(@as(?u64, 44100), trim.playable_frames);
}

test "parse edit list accepts contiguous media edits" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x01, 0xf4, // first segment_duration
        0x00, 0x00, 0x04, 0x00, // first media_time
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
        0x00, 0x00, 0x00, 0xfa, // second segment_duration
        0x00, 0x00, 0x23, 0x40, // second media_time = 1024 + 8000
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
    };

    var tables = TrackTables{
        .movie_timescale = 1000,
        .media_timescale = 16000,
    };
    defer tables.deinit(std.testing.allocator);
    try parseElst(std.testing.allocator, &payload, &tables);

    const trim = try resolveEditListTrim(tables);
    try std.testing.expectEqual(@as(u64, 1024), trim.trim_start_frames);
    try std.testing.expectEqual(@as(?u64, 12000), trim.playable_frames);
}

test "parse edit list rejects discontiguous media edits" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x01, 0xf4, // first segment_duration
        0x00, 0x00, 0x04, 0x00, // first media_time
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
        0x00, 0x00, 0x00, 0xfa, // second segment_duration
        0x00, 0x00, 0x23, 0x41, // second media_time has a one-frame gap
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0
    };

    var tables = TrackTables{
        .movie_timescale = 1000,
        .media_timescale = 16000,
    };
    defer tables.deinit(std.testing.allocator);
    try parseElst(std.testing.allocator, &payload, &tables);
    try std.testing.expectError(error.UnsupportedAudioFormat, resolveEditListTrim(tables));
}

test "parse audio sample entry accepts version 1 child boxes" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x01, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0xac, 0x44, 0x00, 0x00, // sample rate 44100, 16.16 fixed point
        0x00, 0x00, 0x00, 0x00, // samples_per_packet
        0x00, 0x00, 0x00, 0x00, // bytes_per_packet
        0x00, 0x00, 0x00, 0x00, // bytes_per_frame
        0x00, 0x00, 0x00, 0x00, // bytes_per_sample
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x12, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    try parseSampleEntry(fourcc("mp4a"), &payload, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 44100), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x12, 0x10 }, tables.decoder_config);
}

test "parse audio sample entry accepts version 2 quicktime extension" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x02, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x00, // legacy channel count
        0x00, 0x10, // legacy sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x00, 0x00, 0x00, 0x00, // legacy sample rate
        0x00, 0x00, 0x00, 0x48, // size of version 2 structure
        0x40, 0xe5, 0x88, 0x80, 0x00, 0x00, 0x00, 0x00, // sample rate 44100.0
        0x00, 0x00, 0x00, 0x02, // channel count
        0x7f, 0x00, 0x00, 0x00, // always 0x7f000000
        0x00, 0x00, 0x00, 0x10, // bits per channel
        0x00, 0x00, 0x00, 0x00, // format specific flags
        0x00, 0x00, 0x00, 0x00, // bytes per audio packet
        0x00, 0x00, 0x00, 0x00, // LPCM frames per audio packet
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x12, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    try parseSampleEntry(fourcc("mp4a"), &payload, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 44100), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x12, 0x10 }, tables.decoder_config);
}

test "parse audio sample entry finds esds in wave wrapper" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x18, // wave size
        'w',  'a',  'v',  'e',
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    try parseSampleEntry(fourcc("mp4a"), &payload, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 16000), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10 }, tables.decoder_config);
}

test "parse media accepts minf before audio handler" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x3e, 0x80, // timescale 16000

        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo

        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
    };

    var tables = TrackTables{};
    try parseMdia(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(@as(u32, 16000), tables.media_timescale);
    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 16000), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10 }, tables.decoder_config);
}

test "parse movie accepts movie header after audio track" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0xb4, // trak size
        't',  'r',  'a',  'k',
        0x00, 0x00, 0x00, 0x24, // edts size
        'e',  'd',  't',  's',
        0x00, 0x00, 0x00, 0x1c, // elst size
        'e',  'l',  's',  't',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x01, 0xf4, // segment_duration 500
        0x00, 0x00, 0x04, 0x00, // media_time 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0

        0x00, 0x00, 0x00, 0x88, // mdia size
        'm',  'd',  'i',  'a',
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x3e, 0x80, // timescale 16000

        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo

        0x00, 0x00, 0x00, 0x18, // mvhd size
        'm',  'v',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x03, 0xe8, // movie timescale 1000
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseMoov(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u32, 1000), tables.movie_timescale);
    try std.testing.expectEqual(@as(u32, 16000), tables.media_timescale);
    try std.testing.expectEqual(@as(usize, 1), tables.edit_entries.items.len);
    try std.testing.expectEqual(@as(i64, 1024), tables.edit_entries.items[0].media_time);
    try std.testing.expectEqual(@as(u64, 500), tables.edit_entries.items[0].segment_duration);

    const trim = try resolveEditListTrim(tables);
    try std.testing.expectEqual(@as(u64, 1024), trim.trim_start_frames);
    try std.testing.expectEqual(@as(?u64, 8000), trim.playable_frames);
}

test "parse movie skips unsupported audio track before supported track" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0xb4, // unsupported trak size
        't',  'r',  'a',  'k',
        0x00, 0x00, 0x00, 0x24, // edts size
        'e',  'd',  't',  's',
        0x00, 0x00, 0x00, 0x1c, // elst size
        'e',  'l',  's',  't',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x01, 0xf4, // segment_duration 500
        0x00, 0x00, 0x04, 0x00, // media_time 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0

        0x00, 0x00, 0x00, 0x88, // mdia size
        'm',  'd',  'i',  'a',
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x3e, 0x80, // timescale 16000
        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'x',  'x',  'x',  'x',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo

        0x00, 0x00, 0x00, 0xb4, // supported trak size
        't',  'r',  'a',  'k',
        0x00, 0x00, 0x00, 0x24, // edts size
        'e',  'd',  't',  's',
        0x00, 0x00, 0x00, 0x1c, // elst size
        'e',  'l',  's',  't',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x01, 0xf4, // segment_duration 500
        0x00, 0x00, 0x04, 0x00, // media_time 1024
        0x00, 0x01, 0x00, 0x00, // media_rate = 1.0

        0x00, 0x00, 0x00, 0x88, // mdia size
        'm',  'd',  'i',  'a',
        0x00, 0x00, 0x00, 0x18, // mdhd size
        'm',  'd',  'h',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // creation time
        0x00, 0x00, 0x00, 0x00, // modification time
        0x00, 0x00, 0x56, 0x22, // timescale 22050
        0x00, 0x00, 0x00, 0x14, // hdlr size
        'h',  'd',  'l',  'r',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x00, // pre_defined
        's',  'o',  'u',  'n',
        0x00, 0x00, 0x00, 0x54, // minf size
        'm',  'i',  'n',  'f',
        0x00, 0x00, 0x00, 0x4c, // stbl size
        's',  't',  'b',  'l',
        0x00, 0x00, 0x00, 0x44, // stsd size
        's',  't',  's',  'd',
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x01, // entry_count
        0x00, 0x00, 0x00, 0x34, // sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x56, 0x22, 0x00, 0x00, // sample rate 22050, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x13, 0x90, // DecoderSpecificInfo
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseMoov(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u32, 22050), tables.media_timescale);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 22050), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x13, 0x90 }, tables.decoder_config);
}

test "parse sample-to-chunk table accepts monotonic first-description entries" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x01, // first_chunk
        0x00, 0x00, 0x00, 0x02, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
        0x00, 0x00, 0x00, 0x04, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseStsc(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqual(@as(usize, 2), tables.stsc_entries.items.len);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[0].first_chunk);
    try std.testing.expectEqual(@as(u32, 2), tables.stsc_entries.items[0].samples_per_chunk);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[0].sample_description_index);
    try std.testing.expectEqual(@as(u32, 4), tables.stsc_entries.items[1].first_chunk);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[1].samples_per_chunk);
    try std.testing.expectEqual(@as(u32, 1), tables.stsc_entries.items[1].sample_description_index);
}

test "parse sample description table uses selected entry" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x24, // unsupported sample entry size
        'x',  'x',  'x',  'x',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x34, // supported sample entry size
        'm',  'p',  '4',  'a',
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x01, // data_reference_index
        0x00, 0x00, // version
        0x00, 0x00, // revision
        0x00, 0x00, 0x00, 0x00, // vendor
        0x00, 0x02, // channels
        0x00, 0x10, // sample size
        0x00, 0x00, // compression id
        0x00, 0x00, // packet size
        0x3e, 0x80, 0x00, 0x00, // sample rate 16000, 16.16 fixed point
        0x00, 0x00, 0x00, 0x10, // esds size
        'e',  's',  'd',  's',
        0x00, 0x00, 0x00, 0x00, // esds version + flags
        0x05, 0x02, 0x14, 0x10, // DecoderSpecificInfo
    };

    var tables = TrackTables{ .sample_description_index = 2 };
    try parseStsd(&payload, &tables);

    try std.testing.expectEqual(Codec.aac, tables.codec.?);
    try std.testing.expectEqual(@as(u16, 2), tables.channels);
    try std.testing.expectEqual(@as(u32, 16000), tables.sample_rate);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10 }, tables.decoder_config);
}

test "parse sample-to-chunk table rejects sample-description switch" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x01, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
        0x00, 0x00, 0x00, 0x02, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x02, // sample_description_index
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnsupportedAudioFormat, parseStsc(std.testing.allocator, &payload, &tables));
}

test "parse sample-to-chunk table rejects nonmonotonic chunks" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x02, // entry_count
        0x00, 0x00, 0x00, 0x01, // first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
        0x00, 0x00, 0x00, 0x01, // repeated first_chunk
        0x00, 0x00, 0x00, 0x01, // samples_per_chunk
        0x00, 0x00, 0x00, 0x01, // sample_description_index
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnsupportedAudioFormat, parseStsc(std.testing.allocator, &payload, &tables));
}

test "parse compact sample size table handles 4-bit entries" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x04, // reserved + field_size
        0x00, 0x00, 0x00, 0x03, // sample_count
        0x12, 0x30, // three packed 4-bit sample sizes
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseStz2(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, tables.sample_sizes.items);
}

test "parse compact sample size table handles 16-bit entries" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x10, // reserved + field_size
        0x00, 0x00, 0x00, 0x02, // sample_count
        0x01, 0x23, 0x45, 0x67,
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try parseStz2(std.testing.allocator, &payload, &tables);

    try std.testing.expectEqualSlices(u32, &.{ 0x0123, 0x4567 }, tables.sample_sizes.items);
}

test "parse compact sample size table rejects unsupported field size" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00, // version + flags
        0x00, 0x00, 0x00, 0x0c, // reserved + unsupported field_size
        0x00, 0x00, 0x00, 0x01, // sample_count
        0x00, 0x01,
    };

    var tables = TrackTables{};
    defer tables.deinit(std.testing.allocator);
    try std.testing.expectError(error.UnsupportedAudioFormat, parseStz2(std.testing.allocator, &payload, &tables));
}

test "demux extracts checked-in m4a fixture" {
    var demuxed = try demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(Codec.aac, demuxed.codec);
    try std.testing.expectEqual(@as(u32, 16000), demuxed.sample_rate);
    try std.testing.expectEqual(@as(u16, 2), demuxed.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10, 0x56, 0xe5, 0x00 }, demuxed.decoder_config);
    try std.testing.expect(demuxed.access_units.len > 0);
    try std.testing.expect(demuxed.access_units[0].len > 0);
}

test "demux extracts checked-in generic mp4 fixture" {
    var demuxed = try demux(std.testing.allocator, tone_mp4_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(Codec.aac, demuxed.codec);
    try std.testing.expectEqual(@as(u32, 16000), demuxed.sample_rate);
    try std.testing.expectEqual(@as(u16, 2), demuxed.channels);
    try std.testing.expectEqualSlices(u8, &.{ 0x14, 0x10, 0x56, 0xe5, 0x00 }, demuxed.decoder_config);
    try std.testing.expect(demuxed.access_units.len > 0);
}
