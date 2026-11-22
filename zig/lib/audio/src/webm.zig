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

// WebM / Matroska container support.
//
// This is an EBML reader plus a Matroska/WebM demuxer narrow enough to pull a
// single audio track's packets out of the media produced by browser
// MediaRecorder, Zoom cloud recordings, OBS, and yt-dlp downloads. Video and
// subtitle tracks are walked (so the byte layout stays correct) but their
// payloads are never copied out. Only the codecs Matroska commonly carries
// for speech/voice capture are decoded: Opus, Vorbis, and FLAC.

const std = @import("std");
const ogg = @import("ogg.zig");
const opus = @import("opus.zig");
const vorbis = @import("vorbis.zig");
const flac = @import("flac.zig");

const tone_opus_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.opus");
const tone_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.ogg");
const tone_flac_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.flac");

const container = @import("antfly_media").webm_audio;
pub const Codec = container.Codec;
pub const DemuxedAudio = container.DemuxedAudio;
const ebml_header_id = container.ebml_header_id;
const segment_id = container.segment_id;
const tracks_id = container.tracks_id;
const track_entry_id = container.track_entry_id;
const track_number_id = container.track_number_id;
const track_type_id = container.track_type_id;
const codec_id_id = container.codec_id_id;
const codec_private_id = container.codec_private_id;
const codec_delay_id = container.codec_delay_id;
const seek_pre_roll_id = container.seek_pre_roll_id;
const audio_settings_id = container.audio_settings_id;
const sampling_frequency_id = container.sampling_frequency_id;
const channels_id = container.channels_id;
const info_id = container.info_id;
const timestamp_scale_id = container.timestamp_scale_id;
const cluster_id = container.cluster_id;
const timecode_id = container.timecode_id;
const simple_block_id = container.simple_block_id;
const block_group_id = container.block_group_id;
const block_id = container.block_id;
const discard_padding_id = container.discard_padding_id;
const prev_size_id = container.prev_size_id;
const position_id = container.position_id;
const silent_tracks_id = container.silent_tracks_id;
const void_id = container.void_id;
const crc32_id = container.crc32_id;
const audio_track_type = container.audio_track_type;
pub const demux = container.demux;
const VintResult = container.VintResult;
const SizeResult = container.SizeResult;
const Element = container.Element;
const readElementId = container.readElementId;
const readVint = container.readVint;
const readElementHeader = container.readElementHeader;
const elementPayload = container.elementPayload;
const readUint = container.readUint;
const readSignedInt = container.readSignedInt;
const readFloat = container.readFloat;
const vintLength = container.vintLength;
const TrackInfo = container.TrackInfo;
const default_timestamp_scale_ns = container.default_timestamp_scale_ns;
const DemuxState = container.DemuxState;
const parseSegment = container.parseSegment;
const parseInfo = container.parseInfo;
const parseTracks = container.parseTracks;
const parseTrackEntry = container.parseTrackEntry;
const parseAudioSettingsChannels = container.parseAudioSettingsChannels;
const isClusterChildId = container.isClusterChildId;
const parseCluster = container.parseCluster;
const parseBlockGroup = container.parseBlockGroup;
const parseBlockIntoAccessUnits = container.parseBlockIntoAccessUnits;

pub const DecodedInterleaved = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DecodedInterleaved) void {
        self.allocator.free(self.samples);
    }
};

/// Demuxes and decodes a Matroska/WebM file's first audio track to
/// interleaved PCM in one call.
pub fn decodeInterleaved(allocator: std.mem.Allocator, audio_bytes: []const u8) !DecodedInterleaved {
    var demuxed = try demux(allocator, audio_bytes);
    defer demuxed.deinit();

    const track = switch (demuxed.codec) {
        .opus => try decodeOpusTrack(allocator, demuxed),
        .vorbis => try decodeVorbisTrack(allocator, demuxed),
        .flac => try decodeFlacTrack(allocator, demuxed),
    };
    defer if (track.access_unit_frames) |frames| allocator.free(frames);
    var decoded = track.pcm;
    errdefer decoded.deinit();

    if (demuxed.padding.len != 0) {
        const frames = track.access_unit_frames orelse return error.UnsupportedAudioFormat;
        const Copy = struct {
            out: []f32 = &.{},
            offset: usize = 0,
            fn write(raw: *anyopaque, samples: []const f32, _: u32, _: u8) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (samples.len > self.out.len - self.offset) return error.UnsupportedAudioFormat;
                @memcpy(self.out[self.offset..][0..samples.len], samples);
                self.offset += samples.len;
            }
        };
        var copy = Copy{};
        var timeline = try TimelineSink.init(.{ .context = &copy, .write = Copy.write }, demuxed.access_unit_times_ns, frames, decoded.sample_rate, decoded.channels, track.timeline_delay_frames, decoded.samples.len / decoded.channels, demuxed.padding);
        const output = try allocator.alloc(f32, try std.math.mul(usize, timeline.keep, decoded.channels));
        errdefer allocator.free(output);
        copy.out = output;
        try timeline.consumer().push(decoded.samples, decoded.sample_rate, decoded.channels);
        try timeline.finish();
        allocator.free(decoded.samples);
        decoded.samples = output;
        return decoded;
    }

    decoded.samples = try placeOnTimelineAlloc(
        allocator,
        decoded.samples,
        decoded.channels,
        decoded.sample_rate,
        demuxed.access_unit_times_ns,
        track.access_unit_frames,
        track.timeline_delay_frames,
    );

    return decoded;
}

/// A decoded track plus, where the codec makes it exact, how many frames
/// each access unit contributed. That mapping is what lets a gap in the
/// container's timeline be reopened in the decoded audio.
const DecodedTrack = struct {
    pcm: DecodedInterleaved,
    access_unit_frames: ?[]usize = null,
    /// Priming frames the decoder already dropped from the front of `pcm`.
    /// Matroska subtracts the same delay from a block's timestamp to get its
    /// presentation time, so the two have to be applied together.
    timeline_delay_frames: usize = 0,
};

/// Silence a decoded track may gain to honour the container's timeline.
/// A recorder paused for minutes is ordinary and an hour is already
/// generous; past that the timestamps are broken input, and materialising
/// the silence would cost far more than any transcript of it is worth.
///
/// Exceeding it fails the decode. Trimming the silence instead would move
/// every word after the gap earlier while still reporting success, and a
/// transcript whose offsets do not match the recording is worse than one
/// the caller knows it did not get.
///
/// The silence is materialised, so an hour of it at 48 kHz stereo is about
/// 1.4 GB. Callers that decode untrusted input go through `decodeBounded`,
/// whose working-memory ceiling turns that into `error.AudioTooLarge` long
/// before it is reached.
const max_timeline_silence_seconds: u64 = 3600;

/// Puts decoded audio where the recording says it belongs.
///
/// Matroska gives every block a presentation time, and a recorder that was
/// paused, or that starts its audio track after its video, leaves real gaps
/// between them. Concatenating the packets would pull everything after a gap
/// earlier, so a transcript offset would no longer point at the moment the
/// words were said. Silence is inserted instead, bounded by
/// `max_timeline_silence_seconds` in total.
///
/// `access_unit_frames` is how many frames each access unit contributed to
/// `samples`. Without it only the track's start offset can be honoured,
/// because there is no way to tell where one packet's audio ends and the
/// next begins.
///
/// `delay_frames` is the priming the decoder already dropped. Matroska
/// requires CodecDelay to be subtracted from a block's timestamp to get its
/// presentation time: a block nominally starts one delay *before* the audio
/// it carries, because that much of it is priming. Placing blocks without
/// that subtraction leaves the timeline running one delay ahead of the
/// decoded audio, which shows up as false silence in ordinary,
/// uninterrupted recordings.
fn placeOnTimelineAlloc(
    allocator: std.mem.Allocator,
    samples: []f32,
    channels: u8,
    sample_rate: u32,
    times_ns: []const u64,
    access_unit_frames: ?[]const usize,
    delay_frames: usize,
) ![]f32 {
    if (channels == 0 or sample_rate == 0 or times_ns.len == 0) return samples;
    const total_frames = samples.len / channels;
    if (total_frames == 0) return samples;

    // Rounding a block time to frames can land a frame either side of the
    // truth; only a gap wider than a millisecond is a real one.
    const tolerance_frames: usize = @max(1, sample_rate / 1000);
    const budget_frames: usize = std.math.cast(usize, max_timeline_silence_seconds * sample_rate) orelse
        std.math.maxInt(usize);

    const frames = access_unit_frames orelse {
        // Start offset only: everything decoded stays contiguous after it.
        const offset = nsToFrames(times_ns[0], sample_rate) -| delay_frames;
        if (offset <= tolerance_frames) return samples;
        if (offset > budget_frames) return error.UnsupportedAudioFormat;
        const out = try allocator.alloc(f32, (total_frames + offset) * channels);
        @memset(out[0 .. offset * channels], 0);
        @memcpy(out[offset * channels ..], samples);
        allocator.free(samples);
        return out;
    };
    if (frames.len != times_ns.len) return samples;

    // First pass: where each access unit starts in the source and on the
    // timeline, and how much silence that needs in total.
    var inserted_total: usize = 0;
    var source_cursor: usize = 0;
    var timeline_cursor: usize = 0;
    for (frames, times_ns) |unit_frames, time_ns| {
        const want = nsToFrames(time_ns, sample_rate) -| delay_frames;
        if (want > timeline_cursor + tolerance_frames) {
            const gap = want - timeline_cursor;
            inserted_total = std.math.add(usize, inserted_total, gap) catch
                return error.UnsupportedAudioFormat;
            if (inserted_total > budget_frames) return error.UnsupportedAudioFormat;
            timeline_cursor += gap;
        }
        const available = total_frames -| source_cursor;
        const copied = @min(unit_frames, available);
        source_cursor += copied;
        timeline_cursor += copied;
    }
    if (inserted_total == 0) return samples;

    // Second pass: rebuild with the gaps opened up. Anything the access
    // units did not account for (a decoder's trailing frames) is kept.
    const out = try allocator.alloc(f32, (total_frames + inserted_total) * channels);
    errdefer allocator.free(out);
    @memset(out, 0);
    source_cursor = 0;
    timeline_cursor = 0;
    for (frames, times_ns) |unit_frames, time_ns| {
        const want = nsToFrames(time_ns, sample_rate) -| delay_frames;
        if (want > timeline_cursor + tolerance_frames) timeline_cursor = want;
        const available = total_frames -| source_cursor;
        const copied = @min(unit_frames, available);
        if (copied != 0) {
            @memcpy(
                out[timeline_cursor * channels ..][0 .. copied * channels],
                samples[source_cursor * channels ..][0 .. copied * channels],
            );
        }
        source_cursor += copied;
        timeline_cursor += copied;
    }
    if (source_cursor < total_frames) {
        const rest = total_frames - source_cursor;
        @memcpy(
            out[timeline_cursor * channels ..][0 .. rest * channels],
            samples[source_cursor * channels ..][0 .. rest * channels],
        );
    }
    allocator.free(samples);
    return out;
}

fn decodeOpusTrack(allocator: std.mem.Allocator, demuxed: DemuxedAudio) !DecodedTrack {
    const head = try opus.parseHead(demuxed.codec_private);

    const packet_tocs = try allocator.alloc(opus.Toc, demuxed.access_units.len);
    defer allocator.free(packet_tocs);
    for (demuxed.access_units, 0..) |packet, i| {
        if (packet.len == 0) return error.UnsupportedAudioFormat;
        packet_tocs[i] = try opus.parseToc(packet[0]);
    }

    const pre_skip = effectiveOpusPreSkip(head.pre_skip, demuxed.codec_delay_ns);
    const decoded = try opus.decodeInterleavedPacketsAlloc(
        allocator,
        demuxed.access_units,
        packet_tocs,
        head.channels,
        head.output_gain_q8,
        pre_skip,
        null,
    );
    errdefer allocator.free(decoded.samples);

    // An Opus packet's duration is in its TOC, so each packet's contribution
    // to the decoded PCM is exact. The pre-skip comes off the front.
    const frames = try allocator.alloc(usize, demuxed.access_units.len);
    errdefer allocator.free(frames);
    var skip: usize = pre_skip;
    for (demuxed.access_units, frames) |packet, *unit_frames| {
        const samples = opus.packetSamples(packet, decoded.sample_rate) catch {
            allocator.free(frames);
            return .{ .pcm = .{
                .samples = decoded.samples,
                .sample_rate = decoded.sample_rate,
                .channels = decoded.channels,
                .allocator = allocator,
            } };
        };
        const dropped = @min(skip, @as(usize, samples));
        skip -= dropped;
        unit_frames.* = @as(usize, samples) - dropped;
    }

    return .{
        .pcm = .{
            .samples = decoded.samples,
            .sample_rate = decoded.sample_rate,
            .channels = decoded.channels,
            .allocator = allocator,
        },
        .access_unit_frames = frames,
        .timeline_delay_frames = pre_skip,
    };
}

/// Matroska's CodecDelay should always agree with the OpusHead pre-skip
/// field, but not every muxer sets both consistently. Prefer the header's
/// exact sample count and fall back to converting CodecDelay's nanoseconds
/// only when the header itself reports no pre-skip.
fn effectiveOpusPreSkip(head_pre_skip: u16, codec_delay_ns: u64) u16 {
    if (head_pre_skip != 0) return head_pre_skip;
    if (codec_delay_ns == 0) return 0;
    const samples = (codec_delay_ns * 48_000 + 500_000_000) / 1_000_000_000;
    return std.math.cast(u16, samples) orelse std.math.maxInt(u16);
}

fn decodeVorbisTrack(allocator: std.mem.Allocator, demuxed: DemuxedAudio) !DecodedTrack {
    const headers = try splitXiphLacedTriple(demuxed.codec_private);

    const packets = try buildOggPackets(allocator, headers, demuxed.access_units);
    defer {
        for (packets) |p| allocator.free(p.bytes);
        allocator.free(packets);
    }

    var vorbis_demuxed = try vorbis.demuxPacketsAlloc(allocator, packets);
    defer vorbis_demuxed.deinit();

    const decoded = try vorbis.decodeDemuxedInterleavedAlloc(allocator, vorbis_demuxed);
    errdefer allocator.free(decoded.samples);

    // The Vorbis demuxer already resolved how much audio each packet adds
    // (half of the previous and current block sizes). The first packet only
    // primes the overlap, so it contributes nothing. A packet the demuxer
    // dropped as unparsable would desync the mapping, so it is only used
    // when the counts still line up.
    var frames: ?[]usize = null;
    errdefer if (frames) |owned| allocator.free(owned);
    if (vorbis_demuxed.audio_packets.len == demuxed.access_units.len) {
        const owned = try allocator.alloc(usize, demuxed.access_units.len);
        for (vorbis_demuxed.audio_packets, owned, 0..) |packet, *unit_frames, i| {
            unit_frames.* = if (i == 0) 0 else packet.decoded_sample_count;
        }
        frames = owned;
    }

    return .{
        .pcm = .{
            .samples = decoded.samples,
            .sample_rate = decoded.sample_rate,
            .channels = decoded.channels,
            .allocator = allocator,
        },
        .access_unit_frames = frames,
        // A Vorbis stream's first packet only primes the overlap: decoding
        // emits nothing until the second, and the audio that then comes out
        // starts half a block late. That half block is priming exactly as
        // Opus's pre-skip is, and muxers stamp blocks with the audio they
        // complete, so it has to come off the timeline the same way or a
        // remuxed file gains half a block of silence at its head.
        .timeline_delay_frames = @max(
            nsToFrames(demuxed.codec_delay_ns, decoded.sample_rate),
            if (vorbis_demuxed.audio_packets.len > 0) vorbis_demuxed.audio_packets[0].decoded_sample_count else 0,
        ),
    };
}

/// Matroska stores the three Vorbis setup packets (identification, comment,
/// setup) concatenated in CodecPrivate, with the length of the first two
/// Xiph-laced ahead of the data (the same lacing byte scheme Ogg uses).
fn splitXiphLacedTriple(codec_private: []const u8) ![3][]const u8 {
    if (codec_private.len < 1) return error.UnsupportedAudioFormat;
    const packet_count = @as(usize, codec_private[0]) + 1;
    if (packet_count != 3) return error.UnsupportedAudioFormat;

    var cursor: usize = 1;
    var sizes: [2]usize = undefined;
    for (0..2) |i| {
        var size: usize = 0;
        while (true) {
            if (cursor >= codec_private.len) return error.UnsupportedAudioFormat;
            const b = codec_private[cursor];
            cursor += 1;
            size += b;
            if (b != 0xff) break;
        }
        sizes[i] = size;
    }

    if (sizes[0] > codec_private.len - cursor) return error.UnsupportedAudioFormat;
    const first = codec_private[cursor .. cursor + sizes[0]];
    cursor += sizes[0];
    if (sizes[1] > codec_private.len - cursor) return error.UnsupportedAudioFormat;
    const second = codec_private[cursor .. cursor + sizes[1]];
    cursor += sizes[1];
    const third = codec_private[cursor..];
    if (third.len == 0) return error.UnsupportedAudioFormat;

    return .{ first, second, third };
}

fn buildOggPackets(allocator: std.mem.Allocator, headers: [3][]const u8, access_units: []const []const u8) ![]ogg.Packet {
    const packets = try allocator.alloc(ogg.Packet, 3 + access_units.len);
    var built: usize = 0;
    errdefer {
        for (packets[0..built]) |p| allocator.free(p.bytes);
        allocator.free(packets);
    }

    for (headers, 0..) |header, i| {
        packets[i] = .{
            .bytes = try allocator.dupe(u8, header),
            .page_granule_position = 0,
            .granule_applies = false,
            .sequence = @intCast(i),
            .is_bos = i == 0,
            .is_eos = false,
        };
        built += 1;
    }
    for (access_units, 0..) |unit, i| {
        packets[3 + i] = .{
            .bytes = try allocator.dupe(u8, unit),
            .page_granule_position = 0,
            .granule_applies = false,
            .sequence = @intCast(3 + i),
            .is_bos = false,
            .is_eos = i == access_units.len - 1,
        };
        built += 1;
    }
    return packets;
}

/// A_FLAC's CodecPrivate is the native FLAC "fLaC" marker plus metadata
/// blocks (at minimum STREAMINFO), exactly what a native .flac file starts
/// with. Each Block then carries one or more complete, self-delimited raw
/// FLAC frames. Concatenating the two reconstructs a native FLAC stream the
/// existing decoder can read as-is, mirroring how the Ogg-FLAC mapping is
/// reconstructed in ogg.zig.
/// FLAC frame lengths live in each frame's own header, which this module
/// does not parse, so a FLAC track can only be placed by its start offset:
/// an internal gap in a WebM/FLAC recording still closes up. No recorder in
/// the formats this decoder targets writes FLAC that way.
fn needsFlacTimeline(demuxed: DemuxedAudio, info: flac.StreamInfo) !bool {
    if (demuxed.padding.len != 0) return true;
    const first_time = demuxed.access_unit_times_ns[0];
    const delay = nsToFrames(demuxed.codec_delay_ns, info.sample_rate);
    const leading = container.playbackGap(first_time, delay, info.sample_rate, 0);
    var origin: ?u64 = null;
    for (demuxed.access_unit_times_ns[1..], demuxed.access_units[1..]) |time, unit| {
        if (time <= first_time) continue;
        if (origin == null) origin = try flac.frameStart(info, demuxed.access_units[0]);
        const raw = try flac.frameStart(info, unit);
        const offset = std.math.cast(usize, raw -| origin.?) orelse return error.UnsupportedAudioFormat;
        if (container.playbackGap(time, delay, info.sample_rate, leading +| offset) != 0) return true;
    }
    return false;
}

fn flacPlaybackCounts(info: flac.StreamInfo, units: []const []const u8, total: ?usize, counts: []usize) !usize {
    if (units.len == 0 or counts.len != units.len) return error.UnsupportedAudioFormat;
    const origin = try flac.frameStart(info, units[0]);
    var previous: usize = 0;
    for (units[1..], 0..) |unit, i| {
        const sample = try flac.frameStart(info, unit);
        if (sample <= origin) return error.UnsupportedAudioFormat;
        const next = std.math.cast(usize, sample - origin) orelse return error.UnsupportedAudioFormat;
        if (next <= previous) return error.UnsupportedAudioFormat;
        counts[i] = next - previous;
        previous = next;
    }
    if (total) |known| {
        if (known <= previous) return error.UnsupportedAudioFormat;
        counts[counts.len - 1] = known - previous;
    } else counts[counts.len - 1] = std.math.maxInt(usize) - previous;
    return previous;
}

fn decodeFlacTrack(allocator: std.mem.Allocator, demuxed: DemuxedAudio) !DecodedTrack {
    if (demuxed.codec_private.len < 4 or !std.mem.eql(u8, demuxed.codec_private[0..4], "fLaC")) {
        return error.UnsupportedAudioFormat;
    }

    var total_frame_bytes: usize = 0;
    for (demuxed.access_units) |unit| total_frame_bytes += unit.len;

    const native = try allocator.alloc(u8, demuxed.codec_private.len + total_frame_bytes);
    defer allocator.free(native);
    @memcpy(native[0..demuxed.codec_private.len], demuxed.codec_private);
    var offset = demuxed.codec_private.len;
    for (demuxed.access_units) |unit| {
        @memcpy(native[offset .. offset + unit.len], unit);
        offset += unit.len;
    }

    const decoded = try flac.decodeInterleaved(allocator, native);
    errdefer allocator.free(decoded.samples);
    const info = try flac.parseStreamInfo(native);
    const counts = if (try needsFlacTimeline(demuxed, info)) counts: {
        const frames = try allocator.alloc(usize, demuxed.access_units.len);
        errdefer allocator.free(frames);
        _ = try flacPlaybackCounts(info, demuxed.access_units, decoded.samples.len / decoded.channels, frames);
        break :counts frames;
    } else null;
    return .{
        .access_unit_frames = counts,
        .pcm = .{
            .samples = decoded.samples,
            .sample_rate = decoded.sample_rate,
            .channels = decoded.channels,
            .allocator = allocator,
        },
        .timeline_delay_frames = nsToFrames(demuxed.codec_delay_ns, decoded.sample_rate),
    };
}

fn nsToFrames(ns: u64, sample_rate: u32) usize {
    return container.sampleCount(ns, sample_rate);
}

const PlaybackGroup = struct {
    next: usize,
    available: usize,
    prefix: usize,
    kept: usize,
};

fn playbackGroup(counts: []const usize, packet: usize, remaining: usize, rate: u32, padding: []const container.BlockPadding, index: *usize) !PlaybackGroup {
    const block = if (index.* < padding.len and padding[index.*].first == packet) padding[index.*] else null;
    const next = if (block) |value| value.end else packet + 1;
    if (next <= packet or next > counts.len) return error.UnsupportedAudioFormat;
    var available: usize = 0;
    for (counts[packet..next]) |count| available += @min(count, remaining - available);
    const drop = if (block) |value| @min(available, nsToFrames(@abs(value.ns), rate)) else 0;
    if (block != null) index.* += 1;
    return .{ .next = next, .available = available, .prefix = if (block != null and block.?.ns < 0) drop else 0, .kept = available - drop };
}

const PendingFlacTail = struct {
    allocator: std.mem.Allocator,
    limit: usize,
    samples: std.ArrayList(f32) = .empty,
    head: usize = 0,

    fn push(self: *PendingFlacTail, sink: @import("pcm_sink.zig").Sink, values: []const f32, rate: u32, channels: u8) !void {
        if (values.len >= self.limit) {
            try self.samples.ensureTotalCapacityPrecise(self.allocator, self.limit);
            if (self.head < self.samples.items.len) try sink.push(self.samples.items[self.head..], rate, channels);
            if (self.head != 0) try sink.push(self.samples.items[0..self.head], rate, channels);
            const keep = values.len - self.limit;
            if (keep != 0) try sink.push(values[0..keep], rate, channels);
            self.samples.items.len = self.limit;
            @memcpy(self.samples.items, values[keep..]);
            self.head = 0;
            return;
        }
        const fill = @min(values.len, self.limit - self.samples.items.len);
        try self.samples.appendSlice(self.allocator, values[0..fill]);
        var at = fill;
        while (at < values.len) {
            const count = @min(values.len - at, self.samples.items.len - self.head);
            const pending = self.samples.items[self.head..][0..count];
            try sink.push(pending, rate, channels);
            @memcpy(pending, values[at..][0..count]);
            at += count;
            self.head = (self.head + count) % self.samples.items.len;
        }
    }
};

const TimelineSink = struct {
    sink: @import("pcm_sink.zig").Sink,
    times: []const u64,
    counts: []const usize,
    rate: u32,
    channels: u8,
    delay: usize,
    total: usize,
    padding: []const container.BlockPadding,
    padding_index: usize = 0,
    group: PlaybackGroup = undefined,
    keep: usize = 0,
    unit: usize = 0,
    unit_offset: usize = 0,
    source_frames: usize = 0,
    timeline_frames: usize = 0,
    inserted: usize = 0,
    unknown_prefix: ?usize = null,
    pending: ?PendingFlacTail = null,
    fn init(sink: @import("pcm_sink.zig").Sink, times: []const u64, counts: []const usize, rate: u32, channels: u8, delay: usize, total: usize, padding: []const container.BlockPadding) !TimelineSink {
        if (counts.len != times.len or rate == 0 or channels == 0) return error.UnsupportedAudioFormat;
        var cursor: usize = 0;
        var source_cursor: usize = 0;
        var inserted: usize = 0;
        var unit: usize = 0;
        var padding_index: usize = 0;
        while (unit < counts.len) {
            const gap = container.playbackGap(times[unit], delay, rate, cursor);
            inserted = try std.math.add(usize, inserted, gap);
            if (inserted > max_timeline_silence_seconds * rate) return error.UnsupportedAudioFormat;
            cursor += gap;
            const group = try playbackGroup(counts, unit, total - source_cursor, rate, padding, &padding_index);
            source_cursor += group.available;
            cursor = try std.math.add(usize, cursor, group.kept);
            unit = group.next;
        }
        if (padding_index != padding.len) return error.UnsupportedAudioFormat;
        cursor = try std.math.add(usize, cursor, total - source_cursor);
        return .{ .sink = sink, .times = times, .counts = counts, .rate = rate, .channels = channels, .delay = delay, .total = total, .padding = padding, .keep = cursor };
    }
    fn output(self: *@This(), samples: []const f32) !void {
        const frames = samples.len / self.channels;
        const keep = @min(frames, self.keep -| self.timeline_frames);
        if (keep != 0) try self.sink.push(samples[0 .. keep * self.channels], self.rate, self.channels);
        self.timeline_frames += frames;
    }
    fn write(raw: *anyopaque, samples: []const f32, rate: u32, channels: u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (rate != self.rate or channels != self.channels) return error.UnsupportedAudioFormat;
        var offset: usize = 0;
        while (offset < samples.len) {
            if (self.unit < self.counts.len and self.unit_offset == 0) {
                var gap = container.playbackGap(self.times[self.unit], self.delay, rate, self.timeline_frames);
                if (gap != 0) {
                    self.inserted += gap;
                    if (self.inserted > max_timeline_silence_seconds * rate) return error.UnsupportedAudioFormat;
                    const zeros: [2048]f32 = @splat(0);
                    while (gap != 0) {
                        const count = @min(gap, zeros.len / channels);
                        try self.output(zeros[0 .. count * channels]);
                        gap -= count;
                    }
                }
                self.group = try playbackGroup(self.counts, self.unit, self.total -| self.source_frames, rate, self.padding, &self.padding_index);
                if (self.pending != null and self.group.next == self.counts.len) self.group.kept = self.group.available;
                if (self.group.available == 0) {
                    self.unit = self.group.next;
                    continue;
                }
            }
            const available = (samples.len - offset) / channels;
            const count = if (self.unit < self.counts.len) @min(available, self.group.available - self.unit_offset) else available;
            if (self.unit < self.counts.len) {
                const first = @max(self.unit_offset, self.group.prefix);
                const end = @min(self.unit_offset + count, self.group.prefix + self.group.kept);
                if (end > first) {
                    const retained = samples[offset + (first - self.unit_offset) * channels ..][0 .. (end - first) * channels];
                    if (self.pending != null and self.group.next == self.counts.len) {
                        try self.pending.?.push(self.sink, retained, rate, channels);
                    } else try self.output(retained);
                }
            } else try self.output(samples[offset..][0 .. count * channels]);
            self.source_frames += count;
            offset += count * channels;
            if (self.unit < self.counts.len) {
                self.unit_offset += count;
                if (self.unit_offset == self.group.available) {
                    self.unit = self.group.next;
                    self.unit_offset = 0;
                }
            }
        }
    }
    fn finish(self: *@This()) !void {
        if (self.unknown_prefix) |prefix| {
            if (self.source_frames <= prefix or self.padding_index != self.padding.len) return error.UnsupportedAudioFormat;
            return;
        }
        if (self.source_frames != self.total) return error.UnsupportedAudioFormat;
        const zeros: [2048]f32 = @splat(0);
        while (self.timeline_frames < self.keep) {
            const count = @min(self.keep - self.timeline_frames, zeros.len / self.channels);
            try self.output(zeros[0 .. count * self.channels]);
        }
    }
    fn consumer(self: *@This()) @import("pcm_sink.zig").Sink {
        return .{ .context = self, .write = write };
    }
};

/// Immutable timeline spans over exact codec windows, including pause gaps,
/// codec delay and container tail padding. No decoded PCM is retained.
pub const WindowIndex = WindowIndexFor(opus.WindowIndex);
fn opusSamplesFromHeader(toc: opus.Toc, bytes: []const u8) !u16 {
    const count: u32 = switch (toc.code) {
        0 => 1,
        1, 2 => 2,
        3 => if (bytes.len >= 2) bytes[1] & 63 else return error.UnsupportedAudioFormat,
    };
    const samples = toc.frame_duration_us * 48 * count / 1000;
    if (samples == 0 or samples > 5760) return error.UnsupportedAudioFormat;
    return @intCast(samples);
}

pub fn WindowIndexFor(comptime OpusIndex: type) type {
    return struct {
        const Self = @This();
        const Span = struct { first: usize, source: usize, count: usize };
        const CodecIndex = union(enum) { opus: OpusIndex, vorbis: vorbis.WindowIndex };
        allocator: std.mem.Allocator,
        codec: CodecIndex,
        spans: []Span,
        samples: usize,
        sample_rate: u32,
        channels: u8,
        retained_owner: ?OpusIndex = null,
        packets: ?[]container.Packet = null,
        seek_pre_roll_ns: u64 = 0,
        pub fn init(a: std.mem.Allocator, bytes: []const u8, max_packets: usize) !Self {
            const Builder = struct {
                fn build(_: @This(), alloc: std.mem.Allocator, demuxed: opus.Demuxed, max: usize) !OpusIndex {
                    return OpusIndex.build(alloc, demuxed, max);
                }
            };
            return initWithBuilder(a, bytes, max_packets, Builder{});
        }
        pub fn initWithBuilder(a: std.mem.Allocator, bytes: []const u8, max_packets: usize, builder: anytype) !Self {
            var demuxed = try demux(a, bytes);
            defer demuxed.deinit();
            return initDemuxed(a, a, demuxed, max_packets, builder, false);
        }

        pub fn directoryScratchBytes(directory: container.Directory) usize {
            const per_packet = @sizeOf([]const u8) + @sizeOf(u64) + @sizeOf(usize);
            const timing = directory.packets.len *| per_packet;
            if (directory.track.codec != .vorbis) return timing;
            return timing +| (directory.packets.len +| 3) *| @sizeOf(ogg.Packet) +| directory.packets.len *| 2 +| directory.track.codec_private.len;
        }

        pub fn initDirectory(a: std.mem.Allocator, scratch: std.mem.Allocator, directory: container.Directory, max_packets: usize, builder: anytype) !Self {
            const packets = try a.dupe(container.Packet, directory.packets);
            errdefer a.free(packets);
            const units = try scratch.alloc([]const u8, packets.len);
            defer scratch.free(units);
            const times = try scratch.alloc(u64, packets.len);
            defer scratch.free(times);
            for (packets, units, times) |*packet, *unit, *time| {
                unit.* = packet.prefix[0..@min(2, packet.size)];
                time.* = packet.time_ns;
            }
            var index = try initDemuxed(a, scratch, .{
                .codec = directory.track.codec.?,
                .channels = directory.track.channels,
                .codec_delay_ns = directory.track.codec_delay_ns,
                .seek_pre_roll_ns = directory.track.seek_pre_roll_ns,
                .codec_private = directory.track.codec_private,
                .access_units = units,
                .access_unit_times_ns = times,
                .padding = directory.padding,
                .allocator = a,
            }, max_packets, builder, true);
            index.packets = packets;
            index.seek_pre_roll_ns = directory.track.seek_pre_roll_ns;
            return index;
        }

        fn initDemuxed(a: std.mem.Allocator, scratch: std.mem.Allocator, demuxed: DemuxedAudio, max_packets: usize, builder: anytype, header_only: bool) !Self {
            if (demuxed.access_units.len == 0 or demuxed.access_units.len > max_packets) return error.UnsupportedStreamingAudioProfile;
            const counts = try scratch.alloc(usize, demuxed.access_units.len);
            defer scratch.free(counts);
            var codec: CodecIndex = undefined;
            var delay: usize = 0;
            var rate: u32 = 48000;
            var channels: u8 = 0;
            var total: usize = 0;
            switch (demuxed.codec) {
                .opus => {
                    var head = try opus.parseHead(demuxed.codec_private);
                    head.pre_skip = effectiveOpusPreSkip(head.pre_skip, demuxed.codec_delay_ns);
                    delay = head.pre_skip;
                    channels = head.channels;
                    const packets = try a.alloc([]u8, demuxed.access_units.len);
                    errdefer a.free(packets);
                    var initialized: usize = 0;
                    errdefer for (packets[0..initialized]) |packet| a.free(packet);
                    const tocs = try a.alloc(opus.Toc, packets.len);
                    errdefer a.free(tocs);
                    const raw_counts = try a.alloc(u16, packets.len);
                    errdefer a.free(raw_counts);
                    var raw_total: u64 = 0;
                    var skip: usize = head.pre_skip;
                    for (packets, tocs, raw_counts, counts, demuxed.access_units) |*packet, *toc, *raw_count, *count, unit| {
                        if (unit.len == 0) return error.UnsupportedAudioFormat;
                        packet.* = try a.dupe(u8, if (header_only) &.{} else unit);
                        initialized += 1;
                        toc.* = try opus.parseToc(unit[0]);
                        raw_count.* = if (header_only) try opusSamplesFromHeader(toc.*, unit) else try opus.packetSamples(unit, 48000);
                        const drop = @min(skip, raw_count.*);
                        skip -= drop;
                        count.* = raw_count.* - drop;
                        raw_total += raw_count.*;
                        total = try std.math.add(usize, total, count.*);
                    }
                    const owned = opus.Demuxed{ .header = head, .packets = packets, .packet_tocs = tocs, .packet_sample_counts = raw_counts, .total_decoded_frames = raw_total, .playable_frames = total, .discard_padding_frames = 0, .allocator = a };
                    codec = .{ .opus = try builder.build(a, owned, max_packets) };
                },
                .vorbis => {
                    const packets = try buildOggPackets(scratch, try splitXiphLacedTriple(demuxed.codec_private), demuxed.access_units);
                    defer {
                        for (packets) |packet| scratch.free(packet.bytes);
                        scratch.free(packets);
                    }
                    const index = try vorbis.WindowIndex.initPackets(a, packets, max_packets);
                    if (index.demuxed.audio_packets.len != counts.len) {
                        var rejected = index;
                        rejected.deinit();
                        return error.UnsupportedAudioFormat;
                    }
                    codec = .{ .vorbis = index };
                    rate = index.demuxed.headers.identification.sample_rate;
                    channels = index.demuxed.headers.identification.channels;
                    total = index.samples;
                    delay = @max(nsToFrames(demuxed.codec_delay_ns, rate), index.demuxed.audio_packets[0].decoded_sample_count);
                    for (index.demuxed.audio_packets, counts, 0..) |packet, *count, i| count.* = if (i == 0) 0 else packet.decoded_sample_count;
                },
                else => return error.UnsupportedStreamingAudioProfile,
            }
            errdefer switch (codec) {
                inline else => |*index| index.deinit(),
            };
            var spans = std.ArrayList(Span).empty;
            errdefer spans.deinit(a);
            var cursor: usize = 0;
            var source: usize = 0;
            var inserted: usize = 0;
            var packet: usize = 0;
            var padding_index: usize = 0;
            while (packet < counts.len) {
                const group = try playbackGroup(counts, packet, total - source, rate, demuxed.padding, &padding_index);
                const gap = container.playbackGap(demuxed.access_unit_times_ns[packet], delay, rate, cursor);
                inserted = try std.math.add(usize, inserted, gap);
                if (inserted > max_timeline_silence_seconds * rate) return error.UnsupportedAudioFormat;
                cursor += gap;
                const first = source + group.prefix;
                if (group.kept != 0) {
                    if (spans.items.len != 0 and spans.items[spans.items.len - 1].first + spans.items[spans.items.len - 1].count == cursor and spans.items[spans.items.len - 1].source + spans.items[spans.items.len - 1].count == first) {
                        spans.items[spans.items.len - 1].count += group.kept;
                    } else try spans.append(a, .{ .first = cursor, .source = first, .count = group.kept });
                }
                source += group.available;
                cursor = try std.math.add(usize, cursor, group.kept);
                packet = group.next;
            }
            if (padding_index != demuxed.padding.len) return error.UnsupportedAudioFormat;
            if (source < total) {
                try spans.append(a, .{ .first = cursor, .source = source, .count = total - source });
                cursor = try std.math.add(usize, cursor, total - source);
            }
            return .{ .allocator = a, .codec = codec, .spans = try spans.toOwnedSlice(a), .samples = cursor, .sample_rate = rate, .channels = channels };
        }
        pub fn deinit(self: *Self) void {
            const a = self.allocator;
            const spans = self.spans;
            var retained = self.retained_owner;
            a.free(spans);
            if (self.packets) |packets| a.free(packets);
            switch (self.codec) {
                inline else => |*index| index.deinit(),
            }
            if (retained) |*owner| owner.deinit();
        }
        pub fn copy(self: Self, a: std.mem.Allocator) !Self {
            const spans = try a.dupe(Span, self.spans);
            errdefer a.free(spans);
            const packets = if (self.packets) |packets| try a.dupe(container.Packet, packets) else null;
            errdefer if (packets) |owned| a.free(owned);
            return .{ .allocator = a, .packets = packets, .seek_pre_roll_ns = self.seek_pre_roll_ns, .codec = switch (self.codec) {
                .opus => |index| .{ .opus = try index.copy(a) },
                .vorbis => |index| .{ .vorbis = try index.copy(a) },
            }, .spans = spans, .samples = self.samples, .sample_rate = self.sample_rate, .channels = self.channels };
        }
        pub fn estimatedBytes(self: Self) usize {
            return self.spans.len *| @sizeOf(Span) +| (if (self.packets) |packets| std.mem.sliceAsBytes(packets).len else 0) +| switch (self.codec) {
                inline else => |index| index.estimatedBytes(),
            };
        }
        pub fn visitWindow(self: *const Self, a: std.mem.Allocator, first: usize, last: usize, sink: @import("pcm_sink.zig").Sink) !void {
            return self.visitWindowSource(a, first, last, sink, null);
        }

        pub fn visitWindowSource(self: *const Self, a: std.mem.Allocator, first: usize, last: usize, sink: @import("pcm_sink.zig").Sink, source: ?*@import("antfly_media").source.Source) !void {
            const packets: ?container.PacketSource = if (self.packets) |directory| .{ .source = source orelse return error.UnsupportedAudioFormat, .packets = directory, .seek_pre_roll_ns = self.seek_pre_roll_ns } else null;
            if (last < first or last > self.samples) return error.UnsupportedAudioFormat;
            var lo: usize = 0;
            var hi = self.spans.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (self.spans[mid].first + self.spans[mid].count <= first) lo = mid + 1 else hi = mid;
            }
            var cursor = first;
            const zeros: [2048]f32 = @splat(0);
            for (self.spans[lo..]) |span| {
                if (span.first >= last) break;
                try @import("work_control.zig").check();
                while (cursor < @min(last, span.first)) {
                    const count = @min(@min(last, span.first) - cursor, zeros.len / self.channels);
                    try sink.push(zeros[0 .. count * self.channels], self.sample_rate, self.channels);
                    cursor += count;
                }
                const begin = @max(cursor, span.first);
                const end = @min(last, span.first + span.count);
                if (end > begin) {
                    switch (self.codec) {
                        inline else => |*index| {
                            const from = span.source + begin - span.first;
                            const to = span.source + end - span.first;
                            if (@hasDecl(@TypeOf(index.*), "visitWindowPackets")) {
                                try index.visitWindowPackets(a, from, to, sink, packets);
                            } else {
                                try index.visitWindow(a, from, to, sink);
                            }
                        },
                    }
                    cursor = end;
                }
            }
            while (cursor < last) {
                try @import("work_control.zig").check();
                const count = @min(last - cursor, zeros.len / self.channels);
                try sink.push(zeros[0 .. count * self.channels], self.sample_rate, self.channels);
                cursor += count;
            }
        }
    };
}

pub fn visitInterleaved(a: std.mem.Allocator, bytes: []const u8, sink: @import("pcm_sink.zig").Sink) !void {
    var demuxed = try demux(a, bytes);
    defer demuxed.deinit();
    const frames = try a.alloc(usize, demuxed.access_units.len);
    defer a.free(frames);
    switch (demuxed.codec) {
        .opus => {
            const head = try opus.parseHead(demuxed.codec_private);
            const skip = effectiveOpusPreSkip(head.pre_skip, demuxed.codec_delay_ns);
            var remaining_skip: usize = skip;
            const tocs = try a.alloc(opus.Toc, frames.len);
            defer a.free(tocs);
            var total: usize = 0;
            for (demuxed.access_units, frames, tocs) |unit, *count, *toc| {
                if (unit.len == 0) return error.UnsupportedAudioFormat;
                toc.* = try opus.parseToc(unit[0]);
                const decoded = try opus.packetSamples(unit, 48_000);
                const drop = @min(remaining_skip, decoded);
                remaining_skip -= drop;
                count.* = decoded - drop;
                total = try std.math.add(usize, total, count.*);
            }
            var timeline = try TimelineSink.init(sink, demuxed.access_unit_times_ns, frames, 48_000, head.channels, skip, total, demuxed.padding);
            try opus.visitPackets(a, demuxed.access_units, tocs, head.channels, head.output_gain_q8, skip, null, timeline.consumer());
            try timeline.finish();
        },
        .vorbis => {
            const packets = try buildOggPackets(a, try splitXiphLacedTriple(demuxed.codec_private), demuxed.access_units);
            defer {
                for (packets) |packet| a.free(packet.bytes);
                a.free(packets);
            }
            var decoded = try vorbis.demuxPacketsAlloc(a, packets);
            defer decoded.deinit();
            if (decoded.audio_packets.len != frames.len) return error.UnsupportedStreamingAudioProfile;
            for (decoded.audio_packets, frames, 0..) |packet, *count, i| count.* = if (i == 0) 0 else packet.decoded_sample_count;
            const rate = decoded.headers.identification.sample_rate;
            const delay = @max(nsToFrames(demuxed.codec_delay_ns, rate), decoded.audio_packets[0].decoded_sample_count);
            var timeline = try TimelineSink.init(sink, demuxed.access_unit_times_ns, frames, rate, decoded.headers.identification.channels, delay, @intCast(decoded.playable_samples), demuxed.padding);
            try vorbis.visitDemuxed(a, decoded, timeline.consumer());
            try timeline.finish();
        },
        .flac => {
            const info = try flac.parseStreamInfo(demuxed.codec_private);
            if (try needsFlacTimeline(demuxed, info)) {
                const declared = std.math.cast(usize, info.total_samples) orelse return error.UnsupportedAudioFormat;
                const delay = nsToFrames(demuxed.codec_delay_ns, info.sample_rate);
                const prefix = try flacPlaybackCounts(info, demuxed.access_units, if (declared == 0) null else declared, frames);
                const padding = demuxed.padding;
                const last = if (padding.len != 0) padding[padding.len - 1] else null;
                const trim = if (last) |block| (if (declared == 0 and block.end == frames.len and block.ns > 0) nsToFrames(@intCast(block.ns), info.sample_rate) else 0) else 0;
                const pending: ?PendingFlacTail = if (trim != 0) .{ .allocator = a, .limit = try std.math.mul(usize, trim, info.channels) } else null;
                var timeline: TimelineSink = if (declared != 0)
                    try TimelineSink.init(sink, demuxed.access_unit_times_ns, frames, info.sample_rate, info.channels, delay, declared, padding)
                else
                    .{ .sink = sink, .times = demuxed.access_unit_times_ns, .counts = frames, .rate = info.sample_rate, .channels = info.channels, .delay = delay, .total = std.math.maxInt(usize), .padding = padding, .keep = std.math.maxInt(usize), .unknown_prefix = prefix, .pending = pending };
                defer if (timeline.pending) |*tail| tail.samples.deinit(a);
                try flac.visitPackets(a, demuxed.codec_private, demuxed.access_units, timeline.consumer());
                try timeline.finish();
                return;
            }
            if (info.total_samples == 0) {
                const delay = nsToFrames(demuxed.codec_delay_ns, info.sample_rate);
                var gap = nsToFrames(demuxed.access_unit_times_ns[0], info.sample_rate) -| delay;
                if (gap > max_timeline_silence_seconds * info.sample_rate) return error.UnsupportedAudioFormat;
                if (gap > @max(1, info.sample_rate / 1000)) {
                    const zeros: [2048]f32 = @splat(0);
                    while (gap != 0) {
                        const count = @min(gap, zeros.len / info.channels);
                        try sink.push(zeros[0 .. count * info.channels], info.sample_rate, info.channels);
                        gap -= count;
                    }
                }
                try flac.visitPackets(a, demuxed.codec_private, demuxed.access_units, sink);
                return;
            }
            const count = [_]usize{@intCast(info.total_samples)};
            var timeline = try TimelineSink.init(sink, demuxed.access_unit_times_ns[0..1], &count, info.sample_rate, info.channels, nsToFrames(demuxed.codec_delay_ns, info.sample_rate), count[0], &.{});
            try flac.visitPackets(a, demuxed.codec_private, demuxed.access_units, timeline.consumer());
            try timeline.finish();
        },
    }
}

fn expectStreaming(a: std.mem.Allocator, bytes: []const u8, expected: DecodedInterleaved) !void {
    const Collector = struct {
        a: std.mem.Allocator,
        values: std.ArrayList(f32) = .empty,
        fn write(raw: *anyopaque, values: []const f32, _: u32, _: u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.values.appendSlice(self.a, values);
        }
    };
    var collector = Collector{ .a = a };
    defer collector.values.deinit(a);
    try visitInterleaved(a, bytes, .{ .context = &collector, .write = Collector.write });
    try std.testing.expectEqualSlices(f32, expected.samples, collector.values.items);
    var index = WindowIndex.init(a, bytes, 1000000) catch |err| switch (err) {
        error.UnsupportedStreamingAudioProfile => return,
        else => return err,
    };
    defer index.deinit();
    try std.testing.expectEqual(expected.samples.len / expected.channels, index.samples);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var cached = try index.copy(arena.allocator());
    var first: usize = 0;
    while (first < index.samples) : (first += 997) {
        const last = @min(index.samples, first + 2049);
        collector.values.clearRetainingCapacity();
        try cached.visitWindow(a, first, last, .{ .context = &collector, .write = Collector.write });
        try std.testing.expectEqualSlices(f32, expected.samples[first * expected.channels .. last * expected.channels], collector.values.items);
    }
}

// --- Test-fixture support -------------------------------------------------
//
// The rest of this file, from here down, builds and exercises synthetic
// WebM byte streams; none of it participates in decoding real input.

/// Builds a minimal single-Cluster, unlaced WebM/Opus file wrapping the given
/// OpusHead (CodecPrivate) and packets. Exposed so `mod.zig`'s dispatch tests
/// can exercise the public `decode`/`decodeInterleaved` entry points against
/// a real WebM byte stream without duplicating the EBML muxer here.
pub fn buildOpusTestFileAlloc(allocator: std.mem.Allocator, opus_head: []const u8, packets: []const []const u8) ![]u8 {
    return buildOpusTestFileWithPrerollAlloc(allocator, opus_head, packets, null);
}

pub fn buildOpusTestFileWithPrerollAlloc(allocator: std.mem.Allocator, opus_head: []const u8, packets: []const []const u8, preroll: ?u64) ![]u8 {
    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |b| allocator.free(b);
        blocks.deinit(allocator);
    }
    for (packets) |packet| {
        try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 1, packet));
    }

    const entry = try buildAudioTrackEntryWithPreroll(allocator, 1, "A_OPUS", opus_head, 2, null, preroll);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);
    const cluster = try buildCluster(allocator, 0, blocks.items);
    defer allocator.free(cluster);
    return buildWebmFile(allocator, tracks, &.{cluster});
}

fn appendVintSize(list: *std.ArrayList(u8), allocator: std.mem.Allocator, size: u64) !void {
    if (size < 0x7F) {
        try list.append(allocator, 0x80 | @as(u8, @intCast(size)));
    } else if (size < 0x3FFF) {
        try list.append(allocator, 0x40 | @as(u8, @intCast(size >> 8)));
        try list.append(allocator, @intCast(size & 0xFF));
    } else if (size < 0x1FFFFF) {
        try list.append(allocator, 0x20 | @as(u8, @intCast(size >> 16)));
        try list.append(allocator, @intCast((size >> 8) & 0xFF));
        try list.append(allocator, @intCast(size & 0xFF));
    } else {
        try list.append(allocator, 0x10 | @as(u8, @intCast(size >> 24)));
        try list.append(allocator, @intCast((size >> 16) & 0xFF));
        try list.append(allocator, @intCast((size >> 8) & 0xFF));
        try list.append(allocator, @intCast(size & 0xFF));
    }
}

fn appendLeafElement(list: *std.ArrayList(u8), allocator: std.mem.Allocator, id: []const u8, payload: []const u8) !void {
    try list.appendSlice(allocator, id);
    try appendVintSize(list, allocator, payload.len);
    try list.appendSlice(allocator, payload);
}

fn appendEbmlHeader(list: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);
    try appendLeafElement(&body, allocator, &.{ 0x42, 0x82 }, "webm"); // DocType
    try appendLeafElement(list, allocator, &.{ 0x1A, 0x45, 0xDF, 0xA3 }, body.items);
}

fn buildAudioTrackEntry(
    allocator: std.mem.Allocator,
    track_number: u8,
    codec_id: []const u8,
    codec_private: []const u8,
    channels: u8,
    codec_delay_ns: ?u32,
) ![]u8 {
    return buildAudioTrackEntryWithPreroll(allocator, track_number, codec_id, codec_private, channels, codec_delay_ns, null);
}

fn buildAudioTrackEntryWithPreroll(allocator: std.mem.Allocator, track_number: u8, codec_id: []const u8, codec_private: []const u8, channels: u8, codec_delay_ns: ?u32, preroll: ?u64) ![]u8 {
    var audio = std.ArrayList(u8).empty;
    defer audio.deinit(allocator);
    var freq_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &freq_bytes, @bitCast(@as(f64, 48_000.0)), .big);
    try appendLeafElement(&audio, allocator, &.{0xB5}, &freq_bytes); // SamplingFrequency
    try appendLeafElement(&audio, allocator, &.{0x9F}, &.{channels}); // Channels

    var entry = std.ArrayList(u8).empty;
    defer entry.deinit(allocator);
    try appendLeafElement(&entry, allocator, &.{0xD7}, &.{track_number}); // TrackNumber
    try appendLeafElement(&entry, allocator, &.{0x83}, &.{audio_track_type}); // TrackType
    try appendLeafElement(&entry, allocator, &.{0x86}, codec_id); // CodecID
    try appendLeafElement(&entry, allocator, &.{ 0x63, 0xA2 }, codec_private); // CodecPrivate
    if (codec_delay_ns) |delay| {
        var delay_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &delay_bytes, delay, .big);
        try appendLeafElement(&entry, allocator, &.{ 0x56, 0xAA }, &delay_bytes); // CodecDelay
    }
    if (preroll) |ns| {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, ns, .big);
        try appendLeafElement(&entry, allocator, &.{ 0x56, 0xBB }, &bytes);
    }
    try appendLeafElement(&entry, allocator, &.{0xE1}, audio.items); // Audio

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{0xAE}, entry.items); // TrackEntry
    return out.toOwnedSlice(allocator);
}

fn buildVideoTrackEntry(allocator: std.mem.Allocator, track_number: u8) ![]u8 {
    var entry = std.ArrayList(u8).empty;
    defer entry.deinit(allocator);
    try appendLeafElement(&entry, allocator, &.{0xD7}, &.{track_number}); // TrackNumber
    try appendLeafElement(&entry, allocator, &.{0x83}, &.{1}); // TrackType = video
    try appendLeafElement(&entry, allocator, &.{0x86}, "V_VP8"); // CodecID

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{0xAE}, entry.items);
    return out.toOwnedSlice(allocator);
}

fn buildTracks(allocator: std.mem.Allocator, entries: []const []const u8) ![]u8 {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);
    for (entries) |e| try body.appendSlice(allocator, e);

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{ 0x16, 0x54, 0xAE, 0x6B }, body.items);
    return out.toOwnedSlice(allocator);
}

fn buildSimpleBlockNoLacing(allocator: std.mem.Allocator, track_number: u8, frame: []const u8) ![]u8 {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);
    try body.append(allocator, 0x80 | track_number);
    try body.appendSlice(allocator, &[_]u8{ 0x00, 0x00 });
    try body.append(allocator, 0x80); // keyframe, no lacing
    try body.appendSlice(allocator, frame);

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{0xA3}, body.items);
    return out.toOwnedSlice(allocator);
}

/// A SimpleBlock carrying an explicit relative timecode, so a fixture can
/// place its packets along the cluster's timeline.
fn buildSimpleBlockAt(allocator: std.mem.Allocator, track_number: u8, frame: []const u8, relative_ticks: i16) ![]u8 {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);
    try body.append(allocator, 0x80 | track_number);
    var ticks: [2]u8 = undefined;
    std.mem.writeInt(u16, &ticks, @bitCast(relative_ticks), .big);
    try body.appendSlice(allocator, &ticks);
    try body.append(allocator, 0x80); // keyframe, no lacing
    try body.appendSlice(allocator, frame);

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{0xA3}, body.items);
    return out.toOwnedSlice(allocator);
}

fn appendEbmlSignedDelta(list: *std.ArrayList(u8), allocator: std.mem.Allocator, delta: i64) !void {
    // Always encoded as a 2-byte signed vint (bias 2^13 - 1), comfortably
    // wide enough for the small fixtures these tests build.
    const bias: i64 = (1 << 13) - 1;
    const raw = delta + bias;
    std.debug.assert(raw >= 0 and raw <= (1 << 14) - 2);
    const raw_u16: u16 = @intCast(raw);
    try list.append(allocator, 0x40 | @as(u8, @intCast(raw_u16 >> 8)));
    try list.append(allocator, @intCast(raw_u16 & 0xFF));
}

const Lacing = enum { xiph, fixed, ebml };

fn buildSimpleBlockLaced(
    allocator: std.mem.Allocator,
    track_number: u8,
    frames: []const []const u8,
    lacing: Lacing,
) ![]u8 {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);
    try body.append(allocator, 0x80 | track_number);
    try body.appendSlice(allocator, &[_]u8{ 0x00, 0x00 });
    const lacing_bits: u8 = switch (lacing) {
        .xiph => 0x02,
        .fixed => 0x04,
        .ebml => 0x06,
    };
    try body.append(allocator, 0x80 | lacing_bits);
    try body.append(allocator, @intCast(frames.len - 1));

    switch (lacing) {
        .xiph => {
            for (frames[0 .. frames.len - 1]) |f| {
                var remaining = f.len;
                while (remaining >= 255) : (remaining -= 255) try body.append(allocator, 0xFF);
                try body.append(allocator, @intCast(remaining));
            }
        },
        .fixed => {},
        .ebml => {
            if (frames.len >= 2) {
                try appendVintSize(&body, allocator, frames[0].len);
                var prev: i64 = @intCast(frames[0].len);
                for (frames[1 .. frames.len - 1]) |f| {
                    const delta = @as(i64, @intCast(f.len)) - prev;
                    try appendEbmlSignedDelta(&body, allocator, delta);
                    prev = @intCast(f.len);
                }
            }
        },
    }
    for (frames) |f| try body.appendSlice(allocator, f);

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{0xA3}, body.items);
    return out.toOwnedSlice(allocator);
}

fn buildCluster(allocator: std.mem.Allocator, timecode: u8, blocks: []const []const u8) ![]u8 {
    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);
    try appendLeafElement(&body, allocator, &.{0xE7}, &.{timecode}); // Timecode
    for (blocks) |b| try body.appendSlice(allocator, b);

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{ 0x1F, 0x43, 0xB6, 0x75 }, body.items);
    return out.toOwnedSlice(allocator);
}

/// A cluster stamped at an arbitrary millisecond offset, for fixtures that
/// need a timestamp wider than one byte.
fn buildClusterAtMs(allocator: std.mem.Allocator, timecode_ms: u64, blocks: []const []const u8) ![]u8 {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, timecode_ms, .big);
    var first: usize = 0;
    while (first < encoded.len - 1 and encoded[first] == 0) first += 1;

    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);
    try appendLeafElement(&body, allocator, &.{0xE7}, encoded[first..]);
    for (blocks) |b| try body.appendSlice(allocator, b);

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try appendLeafElement(&out, allocator, &.{ 0x1F, 0x43, 0xB6, 0x75 }, body.items);
    return out.toOwnedSlice(allocator);
}

fn buildClusterUnknownSize(allocator: std.mem.Allocator, timecode: u8, blocks: []const []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, &[_]u8{ 0x1F, 0x43, 0xB6, 0x75 });
    try out.append(allocator, 0xFF); // Unknown size (1-byte vint, reserved all-ones).
    try appendLeafElement(&out, allocator, &.{0xE7}, &.{timecode});
    for (blocks) |b| try out.appendSlice(allocator, b);
    return out.toOwnedSlice(allocator);
}

fn buildWebmFile(allocator: std.mem.Allocator, tracks_bytes: []const u8, clusters: []const []const u8) ![]u8 {
    var file = std.ArrayList(u8).empty;
    errdefer file.deinit(allocator);
    try appendEbmlHeader(&file, allocator);

    var segment_body = std.ArrayList(u8).empty;
    defer segment_body.deinit(allocator);
    try segment_body.appendSlice(allocator, tracks_bytes);
    for (clusters) |c| try segment_body.appendSlice(allocator, c);

    try appendLeafElement(&file, allocator, &.{ 0x18, 0x53, 0x80, 0x67 }, segment_body.items);
    return file.toOwnedSlice(allocator);
}

fn buildWebmFileUnknownSegment(allocator: std.mem.Allocator, tracks_bytes: []const u8, clusters: []const []const u8) ![]u8 {
    var file = std.ArrayList(u8).empty;
    errdefer file.deinit(allocator);
    try appendEbmlHeader(&file, allocator);
    try file.appendSlice(allocator, &[_]u8{ 0x18, 0x53, 0x80, 0x67 });
    try file.append(allocator, 0xFF); // Unknown Segment size.
    try file.appendSlice(allocator, tracks_bytes);
    for (clusters) |c| try file.appendSlice(allocator, c);
    return file.toOwnedSlice(allocator);
}

/// Finds the end of a native FLAC file's metadata blocks (the byte offset
/// where frame data begins), so a test can split a checked-in .flac fixture
/// into a CodecPrivate header and raw frame bytes the way a real muxer would.
fn flacMetadataEnd(bytes: []const u8) !usize {
    if (bytes.len < 4 or !std.mem.eql(u8, bytes[0..4], "fLaC")) return error.UnsupportedAudioFormat;
    var cursor: usize = 4;
    while (true) {
        if (cursor + 4 > bytes.len) return error.UnsupportedAudioFormat;
        const is_last = (bytes[cursor] & 0x80) != 0;
        const block_len = (@as(usize, bytes[cursor + 1]) << 16) |
            (@as(usize, bytes[cursor + 2]) << 8) |
            @as(usize, bytes[cursor + 3]);
        cursor += 4 + block_len;
        if (cursor > bytes.len) return error.UnsupportedAudioFormat;
        if (is_last) return cursor;
    }
}

/// Compares decoded PCM against an Ogg-decoded reference. Lengths are
/// allowed to differ by a small bound: unlike Ogg, Matroska has no per-track
/// granule position to trim a codec's final frame down to an exact sample
/// count, so real WebM Opus/Vorbis streams commonly carry a little trailing
/// encoder padding that Ogg's container-level trim removes. Only the
/// overlapping prefix is compared sample-for-sample.
fn expectPcmClose(expected: []const f32, actual: []const f32) !void {
    const max_trailing_padding_frames = 6_000;
    const len_diff = if (actual.len >= expected.len) actual.len - expected.len else expected.len - actual.len;
    try std.testing.expect(len_diff <= max_trailing_padding_frames);

    const common_len = @min(expected.len, actual.len);
    for (expected[0..common_len], actual[0..common_len]) |e, a| {
        try std.testing.expectApproxEqAbs(e, a, 1e-4);
    }
}

test "webm demux extracts opus packets from a synthetic single-cluster file" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    try std.testing.expect(ogg_packets.packets.len > 4);

    const opus_head = ogg_packets.packets[0].bytes;
    var packets = std.ArrayList([]const u8).empty;
    defer packets.deinit(allocator);
    for (ogg_packets.packets[2..]) |packet| try packets.append(allocator, packet.bytes);

    const file = try buildOpusTestFileAlloc(allocator, opus_head, packets.items);
    defer allocator.free(file);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x1A, 0x45, 0xDF, 0xA3 }, file[0..4]);

    var decoded = try decodeInterleaved(allocator, file);
    defer decoded.deinit();
    var reference = try opus.decodeInterleavedOggAlloc(allocator, tone_opus_bytes);
    defer reference.deinit();

    try std.testing.expectEqual(reference.sample_rate, decoded.sample_rate);
    try std.testing.expectEqual(reference.channels, decoded.channels);
    try expectPcmClose(reference.samples, decoded.samples);
}

test "webm demux extracts vorbis packets from a synthetic single-cluster file" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_ogg_bytes);
    defer ogg_packets.deinit();
    try std.testing.expect(ogg_packets.packets.len > 4);

    var codec_private = std.ArrayList(u8).empty;
    defer codec_private.deinit(allocator);
    try codec_private.append(allocator, 2); // 3 packets - 1
    for (ogg_packets.packets[0..2]) |header_packet| {
        var remaining = header_packet.bytes.len;
        while (remaining >= 255) : (remaining -= 255) try codec_private.append(allocator, 0xFF);
        try codec_private.append(allocator, @intCast(remaining));
    }
    for (ogg_packets.packets[0..3]) |header_packet| try codec_private.appendSlice(allocator, header_packet.bytes);

    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |b| allocator.free(b);
        blocks.deinit(allocator);
    }
    for (ogg_packets.packets[3..]) |packet| {
        try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 1, packet.bytes));
    }

    const entry = try buildAudioTrackEntry(allocator, 1, "A_VORBIS", codec_private.items, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);
    const cluster = try buildCluster(allocator, 0, blocks.items);
    defer allocator.free(cluster);
    const file = try buildWebmFile(allocator, tracks, &.{cluster});
    defer allocator.free(file);

    var demuxed = try demux(allocator, file);
    defer demuxed.deinit();
    try std.testing.expectEqual(Codec.vorbis, demuxed.codec);
    try std.testing.expectEqualSlices(u8, codec_private.items, demuxed.codec_private);
    try std.testing.expectEqual(ogg_packets.packets.len - 3, demuxed.access_units.len);
    for (ogg_packets.packets[3..], demuxed.access_units) |expected, actual| {
        try std.testing.expectEqualSlices(u8, expected.bytes, actual);
    }

    const headers = try splitXiphLacedTriple(demuxed.codec_private);
    try std.testing.expectEqualSlices(u8, ogg_packets.packets[0].bytes, headers[0]);
    try std.testing.expectEqualSlices(u8, ogg_packets.packets[1].bytes, headers[1]);
    try std.testing.expectEqualSlices(u8, ogg_packets.packets[2].bytes, headers[2]);

    var decoded = try decodeInterleaved(allocator, file);
    defer decoded.deinit();
    var reference = try vorbis.decodeInterleavedOggAlloc(allocator, tone_ogg_bytes);
    defer reference.deinit();

    try std.testing.expectEqual(reference.sample_rate, decoded.sample_rate);
    try std.testing.expectEqual(reference.channels, decoded.channels);
    try expectPcmClose(reference.samples, decoded.samples);
}

test "webm demux extracts flac frames from a synthetic single-cluster file" {
    const allocator = std.testing.allocator;

    const metadata_end = try flacMetadataEnd(tone_flac_bytes);
    const codec_private = tone_flac_bytes[0..metadata_end];
    const frame_bytes = tone_flac_bytes[metadata_end..];

    const block = try buildSimpleBlockNoLacing(allocator, 1, frame_bytes);
    defer allocator.free(block);

    const entry = try buildAudioTrackEntry(allocator, 1, "A_FLAC", codec_private, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);
    const cluster = try buildCluster(allocator, 0, &.{block});
    defer allocator.free(cluster);
    const file = try buildWebmFile(allocator, tracks, &.{cluster});
    defer allocator.free(file);

    var decoded = try decodeInterleaved(allocator, file);
    defer decoded.deinit();
    var reference = try flac.decodeInterleaved(allocator, tone_flac_bytes);
    defer reference.deinit();

    try std.testing.expectEqual(reference.sample_rate, decoded.sample_rate);
    try std.testing.expectEqual(reference.channels, decoded.channels);
    try std.testing.expectEqualSlices(f32, reference.samples, decoded.samples);
}

test "webm demux skips a video track that precedes the audio track" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();

    const opus_head = ogg_packets.packets[0].bytes;
    const video_entry = try buildVideoTrackEntry(allocator, 1);
    defer allocator.free(video_entry);
    const audio_entry = try buildAudioTrackEntry(allocator, 2, "A_OPUS", opus_head, 2, null);
    defer allocator.free(audio_entry);
    const tracks = try buildTracks(allocator, &.{ video_entry, audio_entry });
    defer allocator.free(tracks);

    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |b| allocator.free(b);
        blocks.deinit(allocator);
    }
    try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 1, &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF }));
    for (ogg_packets.packets[2..]) |packet| {
        try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 2, packet.bytes));
    }

    const cluster = try buildCluster(allocator, 0, blocks.items);
    defer allocator.free(cluster);
    const file = try buildWebmFile(allocator, tracks, &.{cluster});
    defer allocator.free(file);

    var decoded = try decodeInterleaved(allocator, file);
    defer decoded.deinit();
    var reference = try opus.decodeInterleavedOggAlloc(allocator, tone_opus_bytes);
    defer reference.deinit();

    try expectPcmClose(reference.samples, decoded.samples);
}

test "webm demux decodes real opus packets laced with Xiph and EBML lacing" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const opus_head = ogg_packets.packets[0].bytes;
    const audio_packets = ogg_packets.packets[2..];
    try std.testing.expect(audio_packets.len >= 6);

    const lacings = [_]Lacing{ .xiph, .ebml };
    for (lacings) |lacing| {
        var frames = std.ArrayList([]const u8).empty;
        defer frames.deinit(allocator);
        for (audio_packets) |p| try frames.append(allocator, p.bytes);

        const block = try buildSimpleBlockLaced(allocator, 1, frames.items, lacing);
        defer allocator.free(block);

        const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
        defer allocator.free(entry);
        const tracks = try buildTracks(allocator, &.{entry});
        defer allocator.free(tracks);
        const cluster = try buildCluster(allocator, 0, &.{block});
        defer allocator.free(cluster);
        const file = try buildWebmFile(allocator, tracks, &.{cluster});
        defer allocator.free(file);

        var decoded = try decodeInterleaved(allocator, file);
        defer decoded.deinit();
        var reference = try opus.decodeInterleavedOggAlloc(allocator, tone_opus_bytes);
        defer reference.deinit();

        try expectPcmClose(reference.samples, decoded.samples);
    }
}

test "webm decoding refuses a gap it cannot place" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const opus_head = ogg_packets.packets[0].bytes;
    const audio_packets = ogg_packets.packets[2..];

    const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);

    var blocks: [2][]u8 = undefined;
    var built: usize = 0;
    defer for (blocks[0..built]) |block| allocator.free(block);
    for (audio_packets[0..2]) |packet| {
        blocks[built] = try buildSimpleBlockNoLacing(allocator, 1, packet.bytes);
        built += 1;
    }

    // An hour of silence is still placed: a recorder left paused that long
    // is unusual but real, and the offsets have to stay true.
    {
        const first = try buildClusterAtMs(allocator, 0, blocks[0..1]);
        defer allocator.free(first);
        const second = try buildClusterAtMs(allocator, 59 * 60 * 1000, blocks[1..2]);
        defer allocator.free(second);
        const file = try buildWebmFile(allocator, tracks, &.{ first, second });
        defer allocator.free(file);
        var decoded = try decodeInterleaved(allocator, file);
        defer decoded.deinit();
        const frames = decoded.samples.len / decoded.channels;
        try std.testing.expect(frames > 59 * 60 * decoded.sample_rate);
    }

    // Past that the timestamps are not credible. Placing what fits and
    // returning success would move the second half of the recording hours
    // earlier while reporting nothing wrong, so the decode fails instead.
    {
        const first = try buildClusterAtMs(allocator, 0, blocks[0..1]);
        defer allocator.free(first);
        const second = try buildClusterAtMs(allocator, 5 * 60 * 60 * 1000, blocks[1..2]);
        defer allocator.free(second);
        const file = try buildWebmFile(allocator, tracks, &.{ first, second });
        defer allocator.free(file);
        try std.testing.expectError(error.UnsupportedAudioFormat, decodeInterleaved(allocator, file));
    }
}

test "streaming audio webm leaves uninterrupted opus alone" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const opus_head = ogg_packets.packets[0].bytes;
    const audio_packets = ogg_packets.packets[2..];

    const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);

    // The same packets twice: once as a single block, which carries one
    // timestamp and so cannot be placed at all, and once as a block per
    // packet timestamped where a muxer would put it. A recording with no
    // gaps must decode to exactly the same audio either way. It does not
    // unless CodecDelay is subtracted from those timestamps: the decoder
    // drops the stream's pre-skip, so the timeline would otherwise run one
    // pre-skip ahead of the audio and open a gap that was never there.
    var frames_list = std.ArrayList([]const u8).empty;
    defer frames_list.deinit(allocator);
    for (audio_packets) |packet| try frames_list.append(allocator, packet.bytes);
    const laced = try buildSimpleBlockLaced(allocator, 1, frames_list.items, .xiph);
    defer allocator.free(laced);
    const laced_cluster = try buildCluster(allocator, 0, &.{laced});
    defer allocator.free(laced_cluster);
    const laced_file = try buildWebmFile(allocator, tracks, &.{laced_cluster});
    defer allocator.free(laced_file);

    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |block| allocator.free(block);
        blocks.deinit(allocator);
    }
    var elapsed_samples: u32 = 0;
    for (audio_packets) |packet| {
        const relative_ms: i16 = @intCast(elapsed_samples / 48);
        try blocks.append(allocator, try buildSimpleBlockAt(allocator, 1, packet.bytes, relative_ms));
        elapsed_samples += try opus.packetSamples(packet.bytes, 48_000);
    }
    const timed_cluster = try buildCluster(allocator, 0, @ptrCast(blocks.items));
    defer allocator.free(timed_cluster);
    const timed_file = try buildWebmFile(allocator, tracks, &.{timed_cluster});
    defer allocator.free(timed_file);

    var laced_pcm = try decodeInterleaved(allocator, laced_file);
    defer laced_pcm.deinit();
    var timed_pcm = try decodeInterleaved(allocator, timed_file);
    defer timed_pcm.deinit();

    try expectStreaming(allocator, laced_file, laced_pcm);
    try expectStreaming(allocator, timed_file, timed_pcm);
    try std.testing.expectEqual(laced_pcm.samples.len, timed_pcm.samples.len);
    try std.testing.expectEqualSlices(f32, laced_pcm.samples, timed_pcm.samples);
}

test "streaming audio webm leaves a remuxed vorbis recording alone" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_ogg_bytes);
    defer ogg_packets.deinit();
    const codec_private = try vorbisCodecPrivateAlloc(allocator, ogg_packets.packets[0..3]);
    defer allocator.free(codec_private);
    const audio_packets = ogg_packets.packets[3..];

    const entry = try buildAudioTrackEntry(allocator, 1, "A_VORBIS", codec_private, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);

    // The same packets as one block, which carries a single timestamp and
    // so cannot be placed, and as a block per packet stamped the way a
    // remux stamps them: each packet advances the clock by its own
    // duration, the first included. Decoding emits nothing for that first
    // packet -- a Vorbis packet only yields audio once its successor
    // overlaps it -- so its duration is priming, and counting it as a gap
    // would prepend half a block of silence and shift every timestamp.
    var frames_list = std.ArrayList([]const u8).empty;
    defer frames_list.deinit(allocator);
    for (audio_packets) |packet| try frames_list.append(allocator, packet.bytes);
    const laced = try buildSimpleBlockLaced(allocator, 1, frames_list.items, .xiph);
    defer allocator.free(laced);
    const laced_cluster = try buildCluster(allocator, 0, &.{laced});
    defer allocator.free(laced_cluster);
    const laced_file = try buildWebmFile(allocator, tracks, &.{laced_cluster});
    defer allocator.free(laced_file);

    const test_media = @import("antfly_media");
    var source = test_media.source.Source{ .allocator = allocator, .storage = .{ .borrowed = laced_file }, .identity = "laced-vorbis" };
    const directory = try container.Directory.init(allocator, &source, .{});
    defer directory.deinit();
    const Builder = struct {
        fn build(_: @This(), _: std.mem.Allocator, _: opus.Demuxed, _: usize) !opus.WindowIndex {
            return error.UnsupportedAudioFormat;
        }
    };
    const Window = WindowIndexFor(opus.WindowIndex);
    var ranged = try Window.initDirectory(allocator, allocator, directory, 100_000, Builder{});
    defer ranged.deinit();
    var buffered = try Window.init(allocator, laced_file, 100_000);
    defer buffered.deinit();
    try std.testing.expectEqual(buffered.samples, ranged.samples);
    var cloned = try ranged.copy(allocator);
    defer cloned.deinit();
    const Capture = struct {
        allocator: std.mem.Allocator,
        values: std.ArrayList(f32) = .empty,
        fn write(raw: *anyopaque, values: []const f32, _: u32, _: u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.values.appendSlice(self.allocator, values);
        }
    };
    var expected = Capture{ .allocator = allocator };
    defer expected.values.deinit(allocator);
    var actual = Capture{ .allocator = allocator };
    defer actual.values.deinit(allocator);
    try buffered.visitWindow(allocator, 1024, 4096, .{ .context = &expected, .write = Capture.write });
    try cloned.visitWindowSource(allocator, 1024, 4096, .{ .context = &actual, .write = Capture.write }, &source);
    try std.testing.expectEqualSlices(f32, expected.values.items, actual.values.items);

    // A signed trim belongs to the entire laced group.
    var grouped = std.ArrayList(u8).empty;
    defer grouped.deinit(allocator);
    try grouped.append(allocator, 0xa1);
    try grouped.appendSlice(allocator, laced[1..]);
    var padding_bytes: [8]u8 = undefined;
    const padding_ns: i64 = -10 * std.time.ns_per_ms;
    std.mem.writeInt(i64, &padding_bytes, padding_ns, .big);
    try appendLeafElement(&grouped, allocator, &.{ 0x75, 0xa2 }, &padding_bytes);
    var padded_block = std.ArrayList(u8).empty;
    defer padded_block.deinit(allocator);
    try appendLeafElement(&padded_block, allocator, &.{0xa0}, grouped.items);
    const padded_cluster = try buildCluster(allocator, 0, &.{padded_block.items});
    defer allocator.free(padded_cluster);
    const padded_file = try buildWebmFile(allocator, tracks, &.{padded_cluster});
    defer allocator.free(padded_file);
    var padded_source = test_media.source.Source{ .allocator = allocator, .storage = .{ .borrowed = padded_file }, .identity = "padded-vorbis" };
    const padded_directory = try container.Directory.init(allocator, &padded_source, .{});
    defer padded_directory.deinit();
    var padded = try Window.initDirectory(allocator, allocator, padded_directory, 100_000, Builder{});
    defer padded.deinit();
    const trimmed = nsToFrames(@abs(padding_ns), padded.sample_rate);
    try std.testing.expectEqual(buffered.samples - trimmed, padded.samples);
    actual.values.clearRetainingCapacity();
    try padded.visitWindowSource(allocator, 1024 - trimmed, 4096 - trimmed, .{ .context = &actual, .write = Capture.write }, &padded_source);
    try std.testing.expectEqualSlices(f32, expected.values.items, actual.values.items);
    var padded_pcm = try decodeInterleaved(allocator, padded_file);
    defer padded_pcm.deinit();
    try expectStreaming(allocator, padded_file, padded_pcm);
    try std.testing.expectEqualSlices(f32, expected.values.items, padded_pcm.samples[(1024 - trimmed) * padded.channels .. (4096 - trimmed) * padded.channels]);

    var demuxed_vorbis = try vorbis.demuxOggAlloc(allocator, tone_ogg_bytes);
    defer demuxed_vorbis.deinit();
    try std.testing.expectEqual(audio_packets.len, demuxed_vorbis.audio_packets.len);
    const rate = demuxed_vorbis.headers.identification.sample_rate;

    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |block| allocator.free(block);
        blocks.deinit(allocator);
    }
    var elapsed_samples: u64 = 0;
    for (audio_packets, demuxed_vorbis.audio_packets) |packet, timing| {
        const relative_ms: i16 = @intCast(elapsed_samples * 1000 / rate);
        try blocks.append(allocator, try buildSimpleBlockAt(allocator, 1, packet.bytes, relative_ms));
        elapsed_samples += timing.decoded_sample_count;
    }
    const timed_cluster = try buildCluster(allocator, 0, @ptrCast(blocks.items));
    defer allocator.free(timed_cluster);
    const timed_file = try buildWebmFile(allocator, tracks, &.{timed_cluster});
    defer allocator.free(timed_file);

    var laced_pcm = try decodeInterleaved(allocator, laced_file);
    defer laced_pcm.deinit();
    var timed_pcm = try decodeInterleaved(allocator, timed_file);
    defer timed_pcm.deinit();

    try expectStreaming(allocator, laced_file, laced_pcm);
    try expectStreaming(allocator, timed_file, timed_pcm);
    try std.testing.expectEqual(laced_pcm.samples.len, timed_pcm.samples.len);
    try std.testing.expectEqualSlices(f32, laced_pcm.samples, timed_pcm.samples);

    // And the audio itself is still the recording, not a shifted copy.
    var reference = try vorbis.decodeInterleavedOggAlloc(allocator, tone_ogg_bytes);
    defer reference.deinit();
    try std.testing.expect(timed_pcm.samples.len >= reference.samples.len);
    try expectPcmClose(reference.samples, timed_pcm.samples[0..reference.samples.len]);
}

/// The three Vorbis setup packets Xiph-laced into a CodecPrivate blob.
fn vorbisCodecPrivateAlloc(allocator: std.mem.Allocator, headers: []const ogg.Packet) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, 2); // three packets, minus one
    for (headers[0..2]) |header_packet| {
        var remaining = header_packet.bytes.len;
        while (remaining >= 255) : (remaining -= 255) try out.append(allocator, 0xFF);
        try out.append(allocator, @intCast(remaining));
    }
    for (headers[0..3]) |header_packet| try out.appendSlice(allocator, header_packet.bytes);
    return out.toOwnedSlice(allocator);
}

test "streaming audio webm keeps a paused recording's gap on the timeline" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const opus_head = ogg_packets.packets[0].bytes;
    const audio_packets = ogg_packets.packets[2..];
    try std.testing.expect(audio_packets.len >= 4);

    const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);

    // Two clusters of two packets each. The recorder was paused between
    // them: the second cluster is stamped 200 ms, not the 40 ms that
    // uninterrupted audio would carry.
    var blocks: [4][]u8 = undefined;
    var built: usize = 0;
    defer for (blocks[0..built]) |block| allocator.free(block);
    for (audio_packets[0..4], 0..) |packet, i| {
        const relative_ms: i16 = if (i % 2 == 0) 0 else 20;
        blocks[built] = try buildSimpleBlockAt(allocator, 1, packet.bytes, relative_ms);
        built += 1;
    }

    var contiguous = try decodeTwoClusterWebm(allocator, tracks, blocks[0..2], blocks[2..4], 40);
    defer contiguous.deinit();
    var paused = try decodeTwoClusterWebm(allocator, tracks, blocks[0..2], blocks[2..4], 200);
    defer paused.deinit();
    // Decoding the first cluster alone says how much audio precedes the gap.
    var head_only = try decodeTwoClusterWebm(allocator, tracks, blocks[0..2], &.{}, 40);
    defer head_only.deinit();

    const channels = contiguous.channels;
    const rate = contiguous.sample_rate;
    const head_frames = head_only.samples.len / channels;
    const expected_gap_frames = 160 * rate / 1000;

    // The 160 ms the recorder spent paused become silence, so every later
    // word keeps the offset a player would seek to.
    try std.testing.expectEqual(
        contiguous.samples.len + expected_gap_frames * channels,
        paused.samples.len,
    );

    // The audio either side of the gap is untouched.
    try std.testing.expectEqualSlices(
        f32,
        contiguous.samples[0 .. head_frames * channels],
        paused.samples[0 .. head_frames * channels],
    );
    const gap_start = head_frames * channels;
    for (paused.samples[gap_start .. gap_start + expected_gap_frames * channels]) |sample| {
        try std.testing.expectEqual(@as(f32, 0), sample);
    }
    try std.testing.expectEqualSlices(
        f32,
        contiguous.samples[head_frames * channels ..],
        paused.samples[(head_frames + expected_gap_frames) * channels ..],
    );
}

/// Decodes a two-cluster WebM/Opus file whose second cluster carries
/// `second_cluster_timecode` milliseconds; an empty `second_blocks` builds
/// the first cluster alone.
fn decodeTwoClusterWebm(
    allocator: std.mem.Allocator,
    tracks: []const u8,
    first_blocks: []const []u8,
    second_blocks: []const []u8,
    second_cluster_timecode: u8,
) !DecodedInterleaved {
    const first = try buildCluster(allocator, 0, @ptrCast(first_blocks));
    defer allocator.free(first);
    if (second_blocks.len == 0) {
        const file = try buildWebmFile(allocator, tracks, &.{first});
        defer allocator.free(file);
        var decoded = try decodeInterleaved(allocator, file);
        errdefer decoded.deinit();
        try expectStreaming(allocator, file, decoded);
        return decoded;
    }
    const second = try buildCluster(allocator, second_cluster_timecode, @ptrCast(second_blocks));
    defer allocator.free(second);
    const file = try buildWebmFile(allocator, tracks, &.{ first, second });
    defer allocator.free(file);
    var decoded = try decodeInterleaved(allocator, file);
    errdefer decoded.deinit();
    try expectStreaming(allocator, file, decoded);
    return decoded;
}

test "webm demux reconstructs frames for all three lacing modes" {
    const allocator = std.testing.allocator;

    // Byte-level fidelity check, independent of any codec's ability to
    // decode the payloads: this exercises the lace/delace arithmetic itself
    // (varied frame lengths for Xiph/EBML, equal lengths for fixed-size).
    const long_frame = @as([300]u8, @splat('x')); // Forces multi-byte (0xFF-continued) Xiph lace sizes.
    const varied_frames = [_][]const u8{ "a", "bb", &long_frame, "dddd" };
    const equal_frames = [_][]const u8{ "wxyz", "1234", "!@#$", "z9y8" };

    const cases = [_]struct { lacing: Lacing, frames: []const []const u8 }{
        .{ .lacing = .xiph, .frames = &varied_frames },
        .{ .lacing = .ebml, .frames = &varied_frames },
        .{ .lacing = .fixed, .frames = &equal_frames },
    };

    // A minimal, well-formed OpusHead so `demux` accepts the track; the
    // synthetic frame payloads above are never run through the Opus decoder.
    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const opus_head = ogg_packets.packets[0].bytes;

    for (cases) |case| {
        const block = try buildSimpleBlockLaced(allocator, 1, case.frames, case.lacing);
        defer allocator.free(block);

        const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
        defer allocator.free(entry);
        const tracks = try buildTracks(allocator, &.{entry});
        defer allocator.free(tracks);
        const cluster = try buildCluster(allocator, 0, &.{block});
        defer allocator.free(cluster);
        const file = try buildWebmFile(allocator, tracks, &.{cluster});
        defer allocator.free(file);

        var demuxed = try demux(allocator, file);
        defer demuxed.deinit();

        try std.testing.expectEqual(case.frames.len, demuxed.access_units.len);
        for (case.frames, demuxed.access_units) |expected, actual| {
            try std.testing.expectEqualSlices(u8, expected, actual);
        }

        const Source = @import("antfly_media").source.Source;
        var source = Source{ .allocator = allocator, .storage = .{ .borrowed = file }, .identity = "laced" };
        const directory = try container.Directory.init(allocator, &source, .{});
        defer directory.deinit();
        try std.testing.expectEqual(case.frames.len, directory.packets.len);
        const packets = container.PacketSource{ .source = &source, .packets = directory.packets };
        for (case.frames, directory.packets, 0..) |expected, packet, index| {
            var range = try packets.read(index);
            defer range.deinit();
            try std.testing.expectEqualSlices(u8, expected, range.bytes);
            try std.testing.expectEqualSlices(u8, expected[0..@min(expected.len, 2)], packet.prefix[0..@min(expected.len, 2)]);
        }
    }
}

test "webm demux resolves unknown-size Segment and Cluster elements" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const opus_head = ogg_packets.packets[0].bytes;

    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |b| allocator.free(b);
        blocks.deinit(allocator);
    }
    for (ogg_packets.packets[2..]) |packet| {
        try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 1, packet.bytes));
    }

    const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);
    const cluster = try buildClusterUnknownSize(allocator, 0, blocks.items);
    defer allocator.free(cluster);
    const file = try buildWebmFileUnknownSegment(allocator, tracks, &.{cluster});
    defer allocator.free(file);

    var decoded = try decodeInterleaved(allocator, file);
    defer decoded.deinit();
    var reference = try opus.decodeInterleavedOggAlloc(allocator, tone_opus_bytes);
    defer reference.deinit();

    try expectPcmClose(reference.samples, decoded.samples);
}

test "webm demux honors CodecDelay when OpusHead reports no pre-skip" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const head = try opus.parseHead(ogg_packets.packets[0].bytes);
    try std.testing.expect(head.pre_skip != 0);
    const codec_delay_ns: u32 = @intCast((@as(u64, head.pre_skip) * 1_000_000_000) / 48_000);

    // Zero out the OpusHead's own pre-skip field (bytes 10-11, little-endian)
    // so the decoder must fall back to deriving it from CodecDelay instead.
    const zeroed_head = try allocator.dupe(u8, ogg_packets.packets[0].bytes);
    defer allocator.free(zeroed_head);
    zeroed_head[10] = 0;
    zeroed_head[11] = 0;

    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |b| allocator.free(b);
        blocks.deinit(allocator);
    }
    for (ogg_packets.packets[2..]) |packet| {
        try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 1, packet.bytes));
    }

    const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", zeroed_head, 2, codec_delay_ns);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);
    const cluster = try buildCluster(allocator, 0, blocks.items);
    defer allocator.free(cluster);
    const file = try buildWebmFile(allocator, tracks, &.{cluster});
    defer allocator.free(file);

    var decoded = try decodeInterleaved(allocator, file);
    defer decoded.deinit();
    var reference = try opus.decodeInterleavedOggAlloc(allocator, tone_opus_bytes);
    defer reference.deinit();

    try expectPcmClose(reference.samples, decoded.samples);
}

test "webm demux rejects a truncated file instead of crashing" {
    const allocator = std.testing.allocator;

    var ogg_packets = try ogg.parsePacketsAlloc(allocator, tone_opus_bytes);
    defer ogg_packets.deinit();
    const opus_head = ogg_packets.packets[0].bytes;

    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |b| allocator.free(b);
        blocks.deinit(allocator);
    }
    for (ogg_packets.packets[2..]) |packet| {
        try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 1, packet.bytes));
    }

    const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
    defer allocator.free(entry);
    const tracks = try buildTracks(allocator, &.{entry});
    defer allocator.free(tracks);
    const cluster = try buildCluster(allocator, 0, blocks.items);
    defer allocator.free(cluster);
    const file = try buildWebmFile(allocator, tracks, &.{cluster});
    defer allocator.free(file);

    try std.testing.expect(file.len > 64);
    const truncated = file[0 .. file.len / 2];
    try std.testing.expectError(error.UnsupportedAudioFormat, demux(allocator, truncated));

    const truncated_more = file[0..8];
    try std.testing.expectError(error.UnsupportedAudioFormat, demux(allocator, truncated_more));
}

test "webm demux rejects non-EBML input" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedAudioFormat, demux(allocator, "not a webm file"));
}

pub fn buildFlacTestFileAlloc(a: std.mem.Allocator, bytes: []const u8, unknown_total: bool) ![]u8 {
    return buildFlacTestBlocksAlloc(a, bytes, unknown_total, 1);
}

pub fn buildFlacTestBlocksAlloc(a: std.mem.Allocator, bytes: []const u8, unknown_total: bool, frames_per_block: usize) ![]u8 {
    return buildFlacBlocks(a, bytes, unknown_total, frames_per_block, &.{}, null);
}

pub const FlacTestGap = struct { packet: usize, ms: i16 };

pub fn buildPaddedFlacTestBlocksAlloc(a: std.mem.Allocator, bytes: []const u8, unknown_total: bool, padding: []const i64, gap: ?FlacTestGap) ![]u8 {
    return buildFlacBlocks(a, bytes, unknown_total, 1, padding, gap);
}

fn buildFlacBlocks(a: std.mem.Allocator, bytes: []const u8, unknown_total: bool, frames_per_block: usize, padding: []const i64, gap: ?FlacTestGap) ![]u8 {
    if (frames_per_block == 0) return error.InvalidArgument;
    const end = try flacMetadataEnd(bytes);
    const private = try a.dupe(u8, bytes[0..end]);
    defer a.free(private);
    const info = std.mem.readInt(u64, private[18..26], .big);
    if (unknown_total) std.mem.writeInt(u64, private[18..26], info & ~@as(u64, 0x0f_ffff_ffff), .big);
    const info_block = try flac.parseStreamInfo(bytes);
    var blocks: std.ArrayList([]const u8) = .empty;
    defer {
        for (blocks.items) |block| a.free(block);
        blocks.deinit(a);
    }
    var offset = end;
    while (offset < bytes.len) {
        const first = offset;
        for (0..frames_per_block) |_| {
            if (offset == bytes.len) break;
            var frame = try flac.decodeFrameAlloc(a, info_block, bytes[offset..]);
            defer frame.deinit();
            offset += frame.consumed;
        }
        const time = if (gap) |value| if (blocks.items.len >= value.packet) value.ms else 0 else 0;
        const simple = try buildSimpleBlockAt(a, 1, bytes[first..offset], time);
        const block = if (blocks.items.len < padding.len and padding[blocks.items.len] != 0) group: {
            defer a.free(simple);
            var body: std.ArrayList(u8) = .empty;
            defer body.deinit(a);
            try body.append(a, 0xa1);
            try body.appendSlice(a, simple[1..]);
            var value: [8]u8 = undefined;
            std.mem.writeInt(i64, &value, padding[blocks.items.len], .big);
            try appendLeafElement(&body, a, &.{ 0x75, 0xa2 }, &value);
            var encoded: std.ArrayList(u8) = .empty;
            defer encoded.deinit(a);
            try appendLeafElement(&encoded, a, &.{0xa0}, body.items);
            break :group try encoded.toOwnedSlice(a);
        } else simple;
        errdefer a.free(block);
        try blocks.append(a, block);
    }
    const entry = try buildAudioTrackEntry(a, 1, "A_FLAC", private, 2, null);
    defer a.free(entry);
    const tracks = try buildTracks(a, &.{entry});
    defer a.free(tracks);
    const cluster = try buildCluster(a, 0, blocks.items);
    defer a.free(cluster);
    const file = try buildWebmFile(a, tracks, &.{cluster});
    return file;
}

test "streaming audio webm FLAC with unknown sample total preserves full PCM" {
    const a = std.testing.allocator;
    const file = try buildFlacTestFileAlloc(a, tone_flac_bytes, true);
    defer a.free(file);
    var expected = try flac.decodeInterleaved(a, tone_flac_bytes);
    defer expected.deinit();
    const Capture = struct {
        expected: []const f32,
        offset: usize = 0,
        fn write(raw: *anyopaque, pcm: []const f32, _: u32, _: u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(pcm.len <= self.expected.len - self.offset);
            try std.testing.expectEqualSlices(f32, self.expected[self.offset..][0..pcm.len], pcm);
            self.offset += pcm.len;
        }
    };
    var capture = Capture{ .expected = expected.samples };
    try visitInterleaved(a, file, .{ .context = &capture, .write = Capture.write });
    try std.testing.expectEqual(expected.samples.len, capture.offset);
}

test "streaming audio webm FLAC padding emits before opening the final packet" {
    const a = std.testing.allocator;
    const info = try flac.parseStreamInfo(tone_flac_bytes);
    const packet_count = try std.math.divCeil(usize, @intCast(info.total_samples), info.max_block_size);
    const padding = try a.alloc(i64, packet_count);
    defer a.free(padding);
    @memset(padding, 0);
    padding[packet_count - 1] = 5 * std.time.ns_per_ms;
    const bytes = try buildPaddedFlacTestBlocksAlloc(a, tone_flac_bytes, true, padding, null);
    defer a.free(bytes);
    var expected = try decodeInterleaved(a, bytes);
    defer expected.deinit();
    try expectStreaming(a, bytes, expected);

    const last_byte = last_byte: {
        const parsed = try demux(a, bytes);
        defer parsed.deinit();
        const tail = parsed.access_units[parsed.access_units.len - 1];
        break :last_byte @intFromPtr(tail.ptr) - @intFromPtr(bytes.ptr) + tail.len - 1;
    };
    bytes[last_byte] ^= 1;
    const Count = struct {
        fn write(raw: *anyopaque, values: []const f32, _: u32, _: u8) !void {
            const count: *usize = @ptrCast(@alignCast(raw));
            count.* += values.len;
        }
    };
    var emitted: usize = 0;
    try std.testing.expectError(error.UnsupportedAudioFormat, visitInterleaved(a, bytes, .{ .context = &emitted, .write = Count.write }));
    try std.testing.expect(emitted != 0);
}

test "streaming audio webm FLAC keeps a pause without padding" {
    const a = std.testing.allocator;
    const bytes = try buildPaddedFlacTestBlocksAlloc(a, tone_flac_bytes, true, &.{}, .{ .packet = 1, .ms = 200 });
    defer a.free(bytes);
    var expected = try decodeInterleaved(a, bytes);
    defer expected.deinit();
    const info = try flac.parseStreamInfo(tone_flac_bytes);
    const pause_end = info.sample_rate / 5;
    try std.testing.expectEqual((@as(usize, @intCast(info.total_samples)) + pause_end - info.max_block_size) * info.channels, expected.samples.len);
    try expectStreaming(a, bytes, expected);
}
