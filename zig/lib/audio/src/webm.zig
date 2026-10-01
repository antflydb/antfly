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

pub const DecodedInterleaved = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DecodedInterleaved) void {
        self.allocator.free(self.samples);
    }
};

// EBML IDs. Only the elements this demuxer needs to walk or recognize as
// "safe to skip" are listed; every other master or leaf element is skipped by
// its declared size without inspection.
const ebml_header_id: u32 = 0x1A45DFA3;
const segment_id: u32 = 0x18538067;
const tracks_id: u32 = 0x1654AE6B;
const track_entry_id: u32 = 0xAE;
const track_number_id: u32 = 0xD7;
const track_type_id: u32 = 0x83;
const codec_id_id: u32 = 0x86;
const codec_private_id: u32 = 0x63A2;
const codec_delay_id: u32 = 0x56AA;
const seek_pre_roll_id: u32 = 0x56BB;
const audio_settings_id: u32 = 0xE1;
const sampling_frequency_id: u32 = 0xB5;
const channels_id: u32 = 0x9F;
const info_id: u32 = 0x1549A966;
const timestamp_scale_id: u32 = 0x2AD7B1;
const cluster_id: u32 = 0x1F43B675;
const timecode_id: u32 = 0xE7;
const simple_block_id: u32 = 0xA3;
const block_group_id: u32 = 0xA0;
const block_id: u32 = 0xA1;
const discard_padding_id: u32 = 0x75A2;
const prev_size_id: u32 = 0xAB;
const position_id: u32 = 0xA7;
const silent_tracks_id: u32 = 0x5854;
const void_id: u32 = 0xEC;
const crc32_id: u32 = 0xBF;

const audio_track_type: u8 = 2;

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

    decoded.samples = try placeOnTimelineAlloc(
        allocator,
        decoded.samples,
        decoded.channels,
        decoded.sample_rate,
        demuxed.access_unit_times_ns,
        track.access_unit_frames,
        track.timeline_delay_frames,
    );

    if (demuxed.discard_padding_ns > 0 and decoded.sample_rate != 0) {
        const trim_frames = nsToFrames(@intCast(demuxed.discard_padding_ns), decoded.sample_rate);
        decoded.samples = try trimTrailingFramesAlloc(allocator, decoded.samples, decoded.channels, trim_frames);
    }

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
    return .{
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
    const frames = (@as(u128, ns) * sample_rate) / 1_000_000_000;
    return std.math.cast(usize, frames) orelse std.math.maxInt(usize);
}

fn trimTrailingFramesAlloc(allocator: std.mem.Allocator, samples: []f32, channels: u8, trim_frames: usize) ![]f32 {
    if (trim_frames == 0 or channels == 0) return samples;
    const total_frames = samples.len / channels;
    if (trim_frames >= total_frames) return samples;

    const keep_frames = total_frames - trim_frames;
    const out = try allocator.alloc(f32, keep_frames * channels);
    @memcpy(out, samples[0 .. keep_frames * channels]);
    allocator.free(samples);
    return out;
}

// --- EBML primitives ---------------------------------------------------

const VintResult = struct { value: u64, len: usize };
const SizeResult = struct { value: u64, len: usize, is_unknown: bool };

const Element = struct {
    id: u32,
    size: ?u64,
    data_start: usize,
    data_end: ?usize,
};

fn vintLength(first_byte: u8) !usize {
    if (first_byte == 0) return error.UnsupportedAudioFormat;
    var mask: u8 = 0x80;
    var len: usize = 1;
    while ((first_byte & mask) == 0) : (len += 1) {
        mask >>= 1;
    }
    if (len > 8) return error.UnsupportedAudioFormat;
    return len;
}

/// Reads an EBML element ID. The marker bit(s) stay part of the returned
/// value, matching how element IDs are canonically compared.
fn readElementId(bytes: []const u8, start: usize) !VintResult {
    if (start >= bytes.len) return error.UnsupportedAudioFormat;
    const len = try vintLength(bytes[start]);
    if (len > 4) return error.UnsupportedAudioFormat;
    if (start + len > bytes.len) return error.UnsupportedAudioFormat;
    var value: u64 = 0;
    for (bytes[start .. start + len]) |b| value = (value << 8) | b;
    return .{ .value = value, .len = len };
}

/// Reads an EBML variable-size integer (used for element sizes, block track
/// numbers, and lace sizes). The marker bit is stripped from the returned
/// value. `is_unknown` reports the reserved all-ones encoding used for
/// streamed Segment/Cluster elements.
fn readVint(bytes: []const u8, start: usize) !SizeResult {
    if (start >= bytes.len) return error.UnsupportedAudioFormat;
    const len = try vintLength(bytes[start]);
    if (start + len > bytes.len) return error.UnsupportedAudioFormat;

    const shift: u4 = @intCast(len);
    const mask: u8 = @intCast(@as(u16, 0xFF) >> shift);
    var value: u64 = bytes[start] & mask;
    for (bytes[start + 1 .. start + len]) |b| value = (value << 8) | b;

    const max_value = (@as(u64, 1) << @intCast(7 * len)) - 1;
    return .{ .value = value, .len = len, .is_unknown = value == max_value };
}

fn readElementHeader(bytes: []const u8, start: usize) !Element {
    const id_result = try readElementId(bytes, start);
    const id = std.math.cast(u32, id_result.value) orelse return error.UnsupportedAudioFormat;

    const size_start = start + id_result.len;
    const size_result = try readVint(bytes, size_start);
    const data_start = size_start + size_result.len;
    if (data_start > bytes.len) return error.UnsupportedAudioFormat;

    if (size_result.is_unknown) {
        return .{ .id = id, .size = null, .data_start = data_start, .data_end = null };
    }
    const data_end_u64 = @as(u64, data_start) + size_result.value;
    const data_end = std.math.cast(usize, data_end_u64) orelse return error.UnsupportedAudioFormat;
    if (data_end > bytes.len) return error.UnsupportedAudioFormat;

    return .{ .id = id, .size = size_result.value, .data_start = data_start, .data_end = data_end };
}

fn elementPayload(bytes: []const u8, elem: Element) ![]const u8 {
    const end = elem.data_end orelse return error.UnsupportedAudioFormat;
    return bytes[elem.data_start..end];
}

fn readUint(bytes: []const u8) !u64 {
    if (bytes.len == 0 or bytes.len > 8) return error.UnsupportedAudioFormat;
    var value: u64 = 0;
    for (bytes) |b| value = (value << 8) | b;
    return value;
}

fn readSignedInt(bytes: []const u8) !i64 {
    if (bytes.len == 0 or bytes.len > 8) return error.UnsupportedAudioFormat;
    var value: u64 = 0;
    for (bytes) |b| value = (value << 8) | b;
    if (bytes.len == 8) return @bitCast(value);

    const bit_width: u8 = @intCast(bytes.len * 8);
    const shift: u6 = @intCast(64 - bit_width);
    const widened: i64 = @bitCast(value << shift);
    return widened >> shift;
}

fn readFloat(bytes: []const u8) !f64 {
    if (bytes.len == 4) {
        const bits = std.mem.readInt(u32, bytes[0..4], .big);
        return @as(f32, @bitCast(bits));
    }
    if (bytes.len == 8) {
        const bits = std.mem.readInt(u64, bytes[0..8], .big);
        return @bitCast(bits);
    }
    return error.UnsupportedAudioFormat;
}

// --- Segment / Tracks / Clusters ---------------------------------------

const TrackInfo = struct {
    number: u64,
    codec: ?Codec,
    codec_private: []const u8 = &.{},
    channels: u16 = 1,
    codec_delay_ns: u64 = 0,
    seek_pre_roll_ns: u64 = 0,
};

/// Matroska's default TimestampScale: block timecodes count milliseconds.
const default_timestamp_scale_ns: u64 = 1_000_000;

const DemuxState = struct {
    track: ?TrackInfo = null,
    access_units: std.ArrayList([]const u8) = .empty,
    access_unit_times_ns: std.ArrayList(u64) = .empty,
    timestamp_scale_ns: u64 = default_timestamp_scale_ns,
    discard_padding_ns: i64 = 0,
};

fn parseSegment(allocator: std.mem.Allocator, payload: []const u8, state: *DemuxState) !void {
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
fn parseInfo(payload: []const u8, state: *DemuxState) !void {
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

fn parseTracks(payload: []const u8, state: *DemuxState) !void {
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
fn parseTrackEntry(payload: []const u8) !?TrackInfo {
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

fn parseAudioSettingsChannels(payload: []const u8) !u16 {
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

fn isClusterChildId(id: u32) bool {
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
fn parseCluster(
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

fn parseBlockGroup(
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
fn parseBlockIntoAccessUnits(
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

// --- Test-fixture support -------------------------------------------------
//
// The rest of this file, from here down, builds and exercises synthetic
// WebM byte streams; none of it participates in decoding real input.

/// Builds a minimal single-Cluster, unlaced WebM/Opus file wrapping the given
/// OpusHead (CodecPrivate) and packets. Exposed so `mod.zig`'s dispatch tests
/// can exercise the public `decode`/`decodeInterleaved` entry points against
/// a real WebM byte stream without duplicating the EBML muxer here.
pub fn buildOpusTestFileAlloc(allocator: std.mem.Allocator, opus_head: []const u8, packets: []const []const u8) ![]u8 {
    var blocks = std.ArrayList([]u8).empty;
    defer {
        for (blocks.items) |b| allocator.free(b);
        blocks.deinit(allocator);
    }
    for (packets) |packet| {
        try blocks.append(allocator, try buildSimpleBlockNoLacing(allocator, 1, packet));
    }

    const entry = try buildAudioTrackEntry(allocator, 1, "A_OPUS", opus_head, 2, null);
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

test "webm decoding leaves uninterrupted opus alone" {
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

    try std.testing.expectEqual(laced_pcm.samples.len, timed_pcm.samples.len);
    try std.testing.expectEqualSlices(f32, laced_pcm.samples, timed_pcm.samples);
}

test "webm decoding leaves a remuxed vorbis recording alone" {
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

test "webm decoding keeps a paused recording's gap on the timeline" {
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
        return decodeInterleaved(allocator, file);
    }
    const second = try buildCluster(allocator, second_cluster_timecode, @ptrCast(second_blocks));
    defer allocator.free(second);
    const file = try buildWebmFile(allocator, tracks, &.{ first, second });
    defer allocator.free(file);
    return decodeInterleaved(allocator, file);
}

test "webm demux reconstructs frames for all three lacing modes" {
    const allocator = std.testing.allocator;

    // Byte-level fidelity check, independent of any codec's ability to
    // decode the payloads: this exercises the lace/delace arithmetic itself
    // (varied frame lengths for Xiph/EBML, equal lengths for fixed-size).
    const long_frame = [_]u8{'x'} ** 300; // Forces multi-byte (0xFF-continued) Xiph lace sizes.
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
