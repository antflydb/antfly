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
const ogg = @import("ogg.zig");
const opus_celt = @import("opus_celt.zig");
const opus_silk = @import("opus_silk.zig");
const builtin = @import("builtin");
const VEC_LEN = if (builtin.cpu.arch == .wasm32) 4 else 8;
const F32xN = @Vector(VEC_LEN, f32);

const tone_opus_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.opus");
const tone_opus_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-opus.ogg");
const tone_opus_mono_bytes = @embedFile("../testdata/codec-corpus/tone-mono-48k.opus");
const probe_celt_mono_5ms_opus_bytes = @embedFile("../testdata/codec-corpus/probe-celt-mono-48k-5ms.opus");
const probe_celt_mono_120ms_opus_bytes = @embedFile("../testdata/codec-corpus/probe-celt-mono-48k-120ms.opus");
const probe_celt_stereo_2p5ms_opus_bytes = @embedFile("../testdata/codec-corpus/probe-celt-stereo-48k-2p5ms.opus");
const probe_celt_stereo_40ms_opus_bytes = @embedFile("../testdata/codec-corpus/probe-celt-stereo-48k-40ms.opus");
const probe_celt_stereo_60ms_opus_bytes = @embedFile("../testdata/codec-corpus/probe-celt-stereo-48k-60ms.opus");
const probe_silk_mono_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec.ogg");
const probe_silk_mono_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec-10ms.ogg");
const probe_silk_mono_fec_40ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec-40ms.ogg");
const probe_silk_mono_fec_60ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec-60ms.ogg");
const probe_silk_mono_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-10ms.ogg");
const probe_silk_stereo_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec.ogg");
const probe_silk_stereo_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec-10ms.ogg");
const probe_silk_stereo_fec_40ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec-40ms.ogg");
const probe_silk_stereo_fec_60ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec-60ms.ogg");
const probe_silk_stereo_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-10ms.ogg");
const probe_hybrid_mono_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-mono-48k-10ms.ogg");
const probe_hybrid_mono_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-mono-48k-fec-10ms.ogg");
const probe_hybrid_mono_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-mono-48k-fec.ogg");
const probe_hybrid_stereo_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-stereo-48k-10ms.ogg");
const probe_hybrid_stereo_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-stereo-48k-fec-10ms.ogg");
const probe_hybrid_stereo_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-stereo-48k-fec.ogg");
const probe_silk_stereo_40ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-40ms.ogg");
const probe_silk_mono_60ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-60ms.ogg");

pub const Mode = enum {
    silk,
    hybrid,
    celt,
};

pub const Bandwidth = enum {
    nb,
    mb,
    wb,
    swb,
    fb,
};

pub const Toc = struct {
    raw: u8,
    config: u8,
    stereo: bool,
    code: u2,
    mode: Mode,
    bandwidth: Bandwidth,
    frame_duration_us: u32,
};

pub const FramePacket = struct {
    toc: Toc,
    frames: [][]const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *FramePacket) void {
        self.allocator.free(self.frames);
    }
};

pub const FrameShape = struct {
    mode: Mode,
    bandwidth: Bandwidth,
    stereo: bool,
    frame_count: usize,
    frame_duration_us: u32,
    packet_duration_us: u32,
};

pub const Head = struct {
    version: u8,
    channels: u8,
    pre_skip: u16,
    input_sample_rate: u32,
    output_gain_q8: i16,
    mapping_family: u8,
    stream_count: u8,
    coupled_count: u8,
    channel_mapping_len: u8,
    channel_mapping: [8]u8,
};

pub const Demuxed = struct {
    header: Head,
    packets: [][]u8,
    packet_tocs: []Toc,
    packet_sample_counts: []u16,
    total_decoded_frames: u64,
    playable_frames: u64,
    discard_padding_frames: u16,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Demuxed) void {
        for (self.packets) |packet| self.allocator.free(packet);
        self.allocator.free(self.packets);
        self.allocator.free(self.packet_tocs);
        self.allocator.free(self.packet_sample_counts);
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

pub const RangeDecoder = struct {
    bytes: []const u8,
    cursor: usize,
    end_offset: usize,
    end_window: u32,
    nend_bits: u8,
    nbits_total: usize,
    rem: u1,
    rng: u32,
    val: u32,

    pub fn init(bytes: []const u8) RangeDecoder {
        const b0: u8 = if (bytes.len > 0) bytes[0] else 0;
        var decoder = RangeDecoder{
            .bytes = bytes,
            .cursor = if (bytes.len > 0) 1 else 0,
            .end_offset = 0,
            .end_window = 0,
            .nend_bits = 0,
            // 33 - 3*8: normalize() below loads three symbol bytes and
            // brings this to 33, so tell() starts at 1 (RFC 6716 4.1.1).
            .nbits_total = 9,
            .rem = @truncate(b0 & 0x01),
            .rng = 128,
            .val = 127 - (b0 >> 1),
        };
        decoder.normalize();
        return decoder;
    }

    pub fn getFrequency(self: *const RangeDecoder, total: u16) !u16 {
        if (total == 0) return error.UnsupportedAudioFormat;
        const scale = self.rng / total;
        if (scale == 0) return error.UnsupportedAudioFormat;
        const value = total - @min(self.val / scale + 1, total);
        return @intCast(value);
    }

    pub fn getFrequency32(self: *const RangeDecoder, total: u32) !u32 {
        if (total == 0) return error.UnsupportedAudioFormat;
        const scale = self.rng / total;
        if (scale == 0) return error.UnsupportedAudioFormat;
        return total - @min(self.val / scale + 1, total);
    }

    pub fn update(self: *RangeDecoder, low: u16, high: u16, total: u16) !void {
        if (!(low < high and high <= total and total != 0)) return error.UnsupportedAudioFormat;
        const scale = self.rng / total;
        if (scale == 0) return error.UnsupportedAudioFormat;

        self.val -= scale * (total - high);
        if (low > 0) {
            self.rng = scale * (high - low);
        } else {
            self.rng -= scale * (total - high);
        }
        if (self.rng == 0) return error.UnsupportedAudioFormat;
        self.normalize();
    }

    pub fn update32(self: *RangeDecoder, low: u32, high: u32, total: u32) !void {
        if (!(low < high and high <= total and total != 0)) return error.UnsupportedAudioFormat;
        const scale = self.rng / total;
        if (scale == 0) return error.UnsupportedAudioFormat;

        self.val -= scale * (total - high);
        if (low > 0) {
            self.rng = scale * (high - low);
        } else {
            self.rng -= scale * (total - high);
        }
        if (self.rng == 0) return error.UnsupportedAudioFormat;
        self.normalize();
    }

    pub fn decodeSymbol(self: *RangeDecoder, cumulative: []const u16) !u16 {
        if (cumulative.len < 2 or cumulative[0] != 0) return error.UnsupportedAudioFormat;
        const total = cumulative[cumulative.len - 1];
        const fs = try self.getFrequency(total);
        for (0..cumulative.len - 1) |i| {
            const low = cumulative[i];
            const high = cumulative[i + 1];
            if (low > high) return error.UnsupportedAudioFormat;
            if (fs >= low and fs < high) {
                try self.update(low, high, total);
                return @intCast(i);
            }
        }
        return error.UnsupportedAudioFormat;
    }

    /// One binary symbol whose "1" has probability 2^-logp: the low
    /// `rng >> logp` slice of the range is the 1 (RFC 6716 4.1.3.1).
    pub fn decodeBitLogp(self: *RangeDecoder, logp: u5) !u1 {
        const s = self.rng >> logp;
        if (self.val < s) {
            self.rng = s;
            if (self.rng == 0) return error.UnsupportedAudioFormat;
            self.normalize();
            return 1;
        }
        self.val -= s;
        self.rng -= s;
        if (self.rng == 0) return error.UnsupportedAudioFormat;
        self.normalize();
        return 0;
    }

    pub fn readRawBits(self: *RangeDecoder, count: usize) !u32 {
        if (count > 25) return error.UnsupportedAudioFormat;

        var value: u32 = 0;
        var remaining = count;
        var shift: u5 = 0;
        while (remaining > 0) {
            if (self.nend_bits == 0) {
                const next: u8 = if (self.end_offset < self.bytes.len)
                    self.bytes[self.bytes.len - 1 - self.end_offset]
                else
                    0;
                if (self.end_offset < self.bytes.len) self.end_offset += 1;
                self.end_window = next;
                self.nend_bits = 8;
            }

            const take = @min(remaining, self.nend_bits);
            const take_u5: u5 = @intCast(take);
            const mask = (@as(u32, 1) << take_u5) - 1;
            value |= (self.end_window & mask) << shift;
            self.end_window >>= take_u5;
            self.nend_bits -= @intCast(take);
            shift += take_u5;
            remaining -= take;
        }
        self.nbits_total += count;
        return value;
    }

    pub fn readRawBitsWide(self: *RangeDecoder, count: u6) !u32 {
        var remaining: usize = count;
        var shift: u5 = 0;
        var value: u32 = 0;
        while (remaining > 0) {
            const take = @min(remaining, 25);
            value |= (try self.readRawBits(take)) << shift;
            shift += @intCast(take);
            remaining -= take;
        }
        return value;
    }

    pub fn decodeUint(self: *RangeDecoder, ft: u32) !u32 {
        if (ft == 0) return error.UnsupportedAudioFormat;
        const ftb = ilog(ft - 1);
        if (ftb <= 8) {
            const value = try self.getFrequency32(ft);
            try self.update32(value, value + 1, ft);
            return value;
        }

        const high_bits = ftb - 8;
        const ft_hi = ((ft - 1) >> @intCast(high_bits)) + 1;
        const high = try self.getFrequency32(ft_hi);
        try self.update32(high, high + 1, ft_hi);
        const low = try self.readRawBitsWide(high_bits);
        const value = (high << @intCast(high_bits)) | low;
        if (value >= ft) return error.UnsupportedAudioFormat;
        return value;
    }

    pub fn tell(self: *const RangeDecoder) usize {
        return self.nbits_total - ilog(self.rng);
    }

    pub fn tellFrac(self: *const RangeDecoder) usize {
        const correction = [_]u32{ 35733, 38967, 42495, 46340, 50535, 55109, 60097, 65535 };
        const nbits = self.nbits_total << 3;
        const l: usize = ilog(self.rng);
        // rng is at least 2^23 after normalisation, so l >= 24 > 16.
        const r: u32 = self.rng >> @intCast(l - 16);
        var b: usize = (r >> 12) - 8;
        b += @intFromBool(r > correction[b]);
        return nbits - ((l << 3) + b);
    }

    /// Inverse-CDF symbol with 8-bit probabilities (every SILK table).
    pub fn decodeIcdf(self: *RangeDecoder, icdf: []const u8) !u8 {
        return self.decodeIcdfBits(icdf, 8);
    }

    /// Inverse-CDF symbol whose table sums to `1 << ftb`; CELT's spread,
    /// trim and tapset tables use 5, 7 and 2 bits.
    pub fn decodeIcdfBits(self: *RangeDecoder, icdf: []const u8, ftb: u4) !u8 {
        if (icdf.len == 0 or ftb == 0) return error.UnsupportedAudioFormat;
        const ft: u16 = @as(u16, 1) << ftb;
        const fm = try self.getFrequency(ft);
        var low: u16 = 0;
        for (icdf, 0..) |entry, i| {
            const high = ft - entry;
            if (fm >= low and fm < high) {
                try self.update(low, high, ft);
                return @intCast(i);
            }
            low = high;
        }
        return error.UnsupportedAudioFormat;
    }

    fn normalize(self: *RangeDecoder) void {
        while (self.rng <= (1 << 23)) {
            self.rng <<= 8;
            const sym = self.readSym();
            self.val = ((self.val << 8) + (255 - sym)) & 0x7fff_ffff;
            self.nbits_total += 8;
        }
    }

    fn readSym(self: *RangeDecoder) u8 {
        const next: u8 = if (self.cursor < self.bytes.len) self.bytes[self.cursor] else 0;
        if (self.cursor < self.bytes.len) self.cursor += 1;
        const sym = (@as(u8, self.rem) << 7) | (next >> 1);
        self.rem = @truncate(next & 0x01);
        return sym;
    }
};

pub fn outputGainScale(gain_q8: i16) f32 {
    if (gain_q8 == 0) return 1.0;
    const gain_db = @as(f32, @floatFromInt(gain_q8)) / 256.0;
    return @exp((std.math.ln10 / 20.0) * gain_db);
}

pub fn applyOutputGainInPlace(samples: []f32, gain_q8: i16) void {
    const scale = outputGainScale(gain_q8);
    if (scale == 1.0) return;
    for (samples) |*sample| sample.* *= scale;
}

pub fn decodeInterleavedOggAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !DecodedInterleaved {
    var demuxed = try demuxOggAlloc(allocator, ogg_bytes);
    defer demuxed.deinit();

    if ((demuxed.header.channels != 1 and demuxed.header.channels != 2) or demuxed.header.mapping_family != 0) return error.UnsupportedAudioFormat;

    return decodeInterleavedPacketsAlloc(
        allocator,
        demuxed.packets,
        demuxed.packet_tocs,
        demuxed.header.channels,
        demuxed.header.output_gain_q8,
        demuxed.header.pre_skip,
        demuxed.playable_frames,
    );
}

pub fn decodeInterleavedPacketStreamAlloc(
    allocator: std.mem.Allocator,
    packet_stream_bytes: []const u8,
    output_channels: u8,
) !DecodedInterleaved {
    if (output_channels != 1 and output_channels != 2) return error.UnsupportedAudioFormat;

    var packets = std.ArrayList([]u8).empty;
    defer {
        for (packets.items) |packet| allocator.free(packet);
        packets.deinit(allocator);
    }
    var tocs = std.ArrayList(Toc).empty;
    defer tocs.deinit(allocator);

    var cursor: usize = 0;
    while (cursor < packet_stream_bytes.len) {
        if (packet_stream_bytes.len - cursor < 8) return error.UnsupportedAudioFormat;
        const packet_len = readBeU32(packet_stream_bytes[cursor .. cursor + 4]);
        cursor += 4;
        _ = readBeU32(packet_stream_bytes[cursor .. cursor + 4]);
        cursor += 4;

        const packet_len_usize = std.math.cast(usize, packet_len) orelse return error.UnsupportedAudioFormat;
        if (packet_len_usize > packet_stream_bytes.len - cursor) return error.UnsupportedAudioFormat;
        const packet_bytes = packet_stream_bytes[cursor .. cursor + packet_len_usize];
        cursor += packet_len_usize;

        if (packet_bytes.len == 0) return error.UnsupportedAudioFormat;
        const toc = try parseToc(packet_bytes[0]);
        const owned = try allocator.dupe(u8, packet_bytes);
        errdefer allocator.free(owned);
        try packets.append(allocator, owned);
        try tocs.append(allocator, toc);
    }
    if (packets.items.len == 0) return error.UnsupportedAudioFormat;

    return decodeInterleavedPacketsAlloc(
        allocator,
        packets.items,
        tocs.items,
        output_channels,
        0,
        0,
        null,
    );
}

/// Decodes already-demuxed Opus packets to interleaved PCM. This is the
/// packet-level entry point shared by the Ogg and Matroska/WebM demuxers:
/// callers that already have raw Opus packets (with their parsed TOC bytes)
/// and the OpusHead fields can decode directly without an Ogg container.
pub fn decodeInterleavedPacketsAlloc(
    allocator: std.mem.Allocator,
    packets: []const []const u8,
    packet_tocs: []const Toc,
    channels: u8,
    output_gain_q8: i16,
    pre_skip: u16,
    playable_frames_opt: ?u64,
) !DecodedInterleaved {
    if (packets.len != packet_tocs.len) return error.UnsupportedAudioFormat;

    var frame_state = OpusFrameState{ .celt = try opus_celt.Decoder.init(channels) };

    var samples = std.ArrayList(f32).empty;
    defer samples.deinit(allocator);

    for (packets, packet_tocs) |packet, toc| {
        var split = try splitFramesAlloc(allocator, packet);
        defer split.deinit();

        for (split.frames) |frame_bytes| {
            const frame_samples = frameSamples48k(toc);
            const stream_channels: u8 = if (toc.stereo) 2 else 1;
            switch (toc.mode) {
                .celt => {
                    const out = try allocator.alloc(f32, frame_samples * channels);
                    defer allocator.free(out);
                    try decodeCeltOnlyFrameInto(&frame_state, frame_bytes, toc, stream_channels, out);
                    try samples.appendSlice(allocator, out);
                },
                .silk, .hybrid => {
                    const out = try decodeSilkFamilyFrameAlloc(allocator, &frame_state, frame_bytes, toc, stream_channels, channels);
                    defer allocator.free(out);
                    try samples.appendSlice(allocator, out);
                },
            }
        }
    }

    var owned = try samples.toOwnedSlice(allocator);
    errdefer allocator.free(owned);
    applyOutputGainInPlace(owned, output_gain_q8);
    if (pre_skip != 0 or playable_frames_opt != null) {
        // With no explicit playable-frame count (no Ogg granule position to
        // anchor it), the only sensible default is "everything decoded minus
        // the leading pre-skip": defaulting to the full decoded frame count
        // instead would make trimInterleavedOwned's own invariant
        // (playable_frames <= decoded_frames - trim_start_frames) impossible
        // to satisfy whenever pre_skip is nonzero.
        const decoded_frames: u64 = @intCast(@divFloor(owned.len, channels));
        const default_playable_frames = decoded_frames -| @as(u64, pre_skip);
        owned = try trimInterleavedOwned(
            allocator,
            owned,
            channels,
            pre_skip,
            playable_frames_opt orelse default_playable_frames,
        );
    }

    return .{
        .samples = owned,
        .sample_rate = 48_000,
        .channels = channels,
        .allocator = allocator,
    };
}

const OpusFrameState = struct {
    celt: opus_celt.Decoder,
    silk: opus_silk.Decoder = opus_silk.Decoder.init(),
    prev_mode: ?Mode = null,
    prev_redundancy: bool = false,
};

/// Maps interleaved PCM between channel layouts the way opus_decode_frame
/// does for SILK output: mono is duplicated, stereo is averaged.
fn remixInterleavedAlloc(allocator: std.mem.Allocator, samples: []const f32, from: u8, to: u8) ![]f32 {
    if (from == to) return allocator.dupe(f32, samples);
    const frames = samples.len / from;
    const out = try allocator.alloc(f32, frames * to);
    if (from == 1 and to == 2) {
        for (0..frames) |i| {
            out[2 * i] = samples[i];
            out[2 * i + 1] = samples[i];
        }
    } else if (from == 2 and to == 1) {
        for (0..frames) |i| out[i] = 0.5 * (samples[2 * i] + samples[2 * i + 1]);
    } else {
        allocator.free(out);
        return error.UnsupportedAudioFormat;
    }
    return out;
}

fn frameSamples48k(toc: Toc) usize {
    return (@as(usize, toc.frame_duration_us) * 48) / 1000;
}

/// CELT end band per bandwidth, as opus_decoder.c sets it (MB and WB share 17).
fn celtEndBandForBandwidth(bandwidth: Bandwidth) usize {
    return switch (bandwidth) {
        .nb => 13,
        .mb => 17,
        .wb => 17,
        .swb => 19,
        .fb => 21,
    };
}

fn decodeCeltOnlyFrameInto(state: *OpusFrameState, frame_bytes: []const u8, toc: Toc, stream_channels: u8, out: []f32) !void {
    const n = frameSamples48k(toc);
    if (state.prev_mode) |prev| {
        if (prev != .celt and !state.prev_redundancy) state.celt.reset();
    }
    state.celt.start = 0;
    state.celt.end = celtEndBandForBandwidth(toc.bandwidth);
    var dec = RangeDecoder.init(frame_bytes);
    try state.celt.decodeFrame(frame_bytes.len, &dec, n, out, stream_channels);
    state.prev_mode = .celt;
    state.prev_redundancy = false;
}

/// One SILK or hybrid frame, following opus_decode_frame(): the SILK layer
/// is decoded first from the shared range decoder, then the redundancy
/// signalling, then the CELT layer (bands 17+ for hybrid) and finally the
/// 5 ms redundant CELT frame that smooths a mode transition.
fn decodeSilkFamilyFrameAlloc(
    allocator: std.mem.Allocator,
    state: *OpusFrameState,
    frame_bytes: []const u8,
    toc: Toc,
    stream_channels: u8,
    channels: u8,
) ![]f32 {
    const c_count: usize = channels;
    const n = frameSamples48k(toc);
    const hybrid = toc.mode == .hybrid;
    var dec = RangeDecoder.init(frame_bytes);

    // SILK layer (opus_decode_frame): reset after a CELT-only frame, then
    // decode one 10/20 ms internal frame at a time until the packet's frame
    // duration is covered.
    if (state.prev_mode == .celt) state.silk.reset();
    const ctl = opus_silk.Control{
        .n_channels_api = channels,
        .n_channels_internal = stream_channels,
        .api_sample_rate = 48_000,
        .internal_sample_rate = if (!hybrid) switch (toc.bandwidth) {
            .nb => @as(i32, 8_000),
            .mb => @as(i32, 12_000),
            else => @as(i32, 16_000),
        } else 16_000,
        .payload_size_ms = @max(10, @as(i32, @intCast(toc.frame_duration_us / 1000))),
    };
    const pcm_silk_48k = try allocator.alloc(f32, n * c_count);
    defer allocator.free(pcm_silk_48k);
    @memset(pcm_silk_48k, 0);
    var decoded: usize = 0;
    var first_frame = true;
    while (decoded < n) {
        const got = try state.silk.decode(ctl, first_frame, &dec, pcm_silk_48k[decoded * c_count ..]);
        if (got == 0) return error.UnsupportedAudioFormat;
        decoded += got;
        first_frame = false;
    }
    if (decoded != n) return error.UnsupportedAudioFormat;

    var len = frame_bytes.len;
    var redundancy = false;
    var celt_to_silk = false;
    var redundancy_bytes: usize = 0;
    if (dec.tell() + 17 + @as(usize, if (hybrid) 20 else 0) <= 8 * len) {
        redundancy = if (hybrid) (try dec.decodeBitLogp(12)) != 0 else true;
        if (redundancy) {
            celt_to_silk = (try dec.decodeBitLogp(1)) != 0;
            redundancy_bytes = if (hybrid) @as(usize, try dec.decodeUint(256)) + 2 else len -| ((dec.tell() + 7) >> 3);
            if (redundancy_bytes > len or (len - redundancy_bytes) * 8 < dec.tell()) {
                len = 0;
                redundancy_bytes = 0;
                redundancy = false;
            } else {
                len -= redundancy_bytes;
            }
            dec.bytes = dec.bytes[0..len];
        }
    }

    const out = try allocator.alloc(f32, n * c_count);
    errdefer allocator.free(out);
    @memset(out, 0);
    const end_band = celtEndBandForBandwidth(toc.bandwidth);
    state.celt.end = end_band;
    var redundant_audio: [240 * 2]f32 = undefined;
    const redundant = redundant_audio[0 .. 240 * c_count];

    if (redundancy and celt_to_silk) {
        state.celt.start = 0;
        var rdec = RangeDecoder.init(frame_bytes[len .. len + redundancy_bytes]);
        try state.celt.decodeFrame(redundancy_bytes, &rdec, 240, redundant, stream_channels);
    }

    if (hybrid) {
        if (state.prev_mode) |prev| {
            if (prev != .hybrid and !state.prev_redundancy) state.celt.reset();
        }
        state.celt.start = 17;
        try state.celt.decodeFrame(len, &dec, n, out, stream_channels);
    } else if (state.prev_mode == .hybrid and !(redundancy and celt_to_silk and state.prev_redundancy)) {
        state.celt.start = 0;
        const silence = [_]u8{ 0xFF, 0xFF };
        var sdec = RangeDecoder.init(&silence);
        try state.celt.decodeFrame(2, &sdec, 120, out[0 .. 120 * c_count], stream_channels);
    }

    for (out, pcm_silk_48k) |*dst, src| dst.* += src;

    if (redundancy and !celt_to_silk) {
        state.celt.reset();
        state.celt.start = 0;
        var rdec = RangeDecoder.init(frame_bytes[len .. len + redundancy_bytes]);
        try state.celt.decodeFrame(redundancy_bytes, &rdec, 240, redundant, stream_channels);
        const tail = out[(n - 120) * c_count ..];
        opus_celt.smoothFade(tail, redundant[120 * c_count ..], tail, 120, c_count);
    }
    if (redundancy and celt_to_silk) {
        @memcpy(out[0 .. 120 * c_count], redundant[0 .. 120 * c_count]);
        const head = out[120 * c_count .. 240 * c_count];
        opus_celt.smoothFade(redundant[120 * c_count ..], head, head, 120, c_count);
    }
    state.prev_mode = toc.mode;
    state.prev_redundancy = redundancy and !celt_to_silk;
    return out;
}

pub fn parseHead(packet: []const u8) !Head {
    if (packet.len < 19 or !std.mem.eql(u8, packet[0..8], "OpusHead")) return error.UnsupportedAudioFormat;
    const version = packet[8];
    if (version == 0 or version > 15) return error.UnsupportedAudioFormat;
    const channels = packet[9];
    if (channels == 0 or channels > 8) return error.UnsupportedAudioFormat;

    var header = Head{
        .version = version,
        .channels = channels,
        .pre_skip = readLeU16(packet[10..12]),
        .input_sample_rate = readLeU32(packet[12..16]),
        .output_gain_q8 = @bitCast(readLeU16(packet[16..18])),
        .mapping_family = packet[18],
        .stream_count = 0,
        .coupled_count = 0,
        .channel_mapping_len = channels,
        .channel_mapping = [_]u8{0} ** 8,
    };

    if (header.mapping_family == 0) {
        if (packet.len != 19 or channels > 2) return error.UnsupportedAudioFormat;
        header.stream_count = 1;
        header.coupled_count = if (channels == 2) 1 else 0;
        for (0..channels) |i| header.channel_mapping[i] = @intCast(i);
        return header;
    }

    if (packet.len < 21 + channels) return error.UnsupportedAudioFormat;
    header.stream_count = packet[19];
    header.coupled_count = packet[20];
    if (header.stream_count == 0 or header.coupled_count > header.stream_count) return error.UnsupportedAudioFormat;
    @memcpy(header.channel_mapping[0..channels], packet[21 .. 21 + channels]);
    return header;
}

pub fn parseToc(byte: u8) !Toc {
    const config = byte >> 3;
    const stereo = ((byte >> 2) & 0x01) != 0;
    const code: u2 = @truncate(byte);

    const mode: Mode, const bandwidth: Bandwidth, const frame_duration_us: u32 = switch (config) {
        0...3 => .{ .silk, .nb, silkFrameDurationUs(config - 0) },
        4...7 => .{ .silk, .mb, silkFrameDurationUs(config - 4) },
        8...11 => .{ .silk, .wb, silkFrameDurationUs(config - 8) },
        12...13 => .{ .hybrid, .swb, hybridFrameDurationUs(config - 12) },
        14...15 => .{ .hybrid, .fb, hybridFrameDurationUs(config - 14) },
        16...19 => .{ .celt, .nb, celtFrameDurationUs(config - 16) },
        20...23 => .{ .celt, .wb, celtFrameDurationUs(config - 20) },
        24...27 => .{ .celt, .swb, celtFrameDurationUs(config - 24) },
        28...31 => .{ .celt, .fb, celtFrameDurationUs(config - 28) },
        else => unreachable,
    };

    return .{
        .raw = byte,
        .config = config,
        .stereo = stereo,
        .code = code,
        .mode = mode,
        .bandwidth = bandwidth,
        .frame_duration_us = frame_duration_us,
    };
}

pub fn splitFramesAlloc(allocator: std.mem.Allocator, packet: []const u8) !FramePacket {
    if (packet.len < 1) return error.UnsupportedAudioFormat;
    const toc = try parseToc(packet[0]);

    switch (toc.code) {
        0 => {
            const frames = try allocator.alloc([]const u8, 1);
            errdefer allocator.free(frames);
            frames[0] = packet[1..];
            return .{ .toc = toc, .frames = frames, .allocator = allocator };
        },
        1 => {
            const payload = packet[1..];
            if (payload.len == 0 or payload.len % 2 != 0) return error.UnsupportedAudioFormat;
            const half = payload.len / 2;
            const frames = try allocator.alloc([]const u8, 2);
            errdefer allocator.free(frames);
            frames[0] = payload[0..half];
            frames[1] = payload[half..];
            return .{ .toc = toc, .frames = frames, .allocator = allocator };
        },
        2 => {
            const frame0_len, const header_len = try parseCode2FrameLength(packet[1..]);
            const payload = packet[1 + header_len ..];
            if (frame0_len > payload.len) return error.UnsupportedAudioFormat;
            const frame1_len = payload.len - frame0_len;
            const frames = try allocator.alloc([]const u8, 2);
            errdefer allocator.free(frames);
            frames[0] = payload[0..frame0_len];
            frames[1] = payload[frame0_len .. frame0_len + frame1_len];
            return .{ .toc = toc, .frames = frames, .allocator = allocator };
        },
        3 => return parseCode3FramesAlloc(allocator, toc, packet[1..]),
    }
}

pub fn packetSamples(packet: []const u8, sample_rate: u32) !u16 {
    if (sample_rate != 48_000 and sample_rate != 24_000 and sample_rate != 16_000 and sample_rate != 12_000 and sample_rate != 8_000) {
        return error.UnsupportedAudioFormat;
    }

    var split = try splitFramesAlloc(std.heap.page_allocator, packet);
    defer split.deinit();

    const frame_samples = @as(u32, split.toc.frame_duration_us) * sample_rate / 1_000_000;
    const total = frame_samples * @as(u32, @intCast(split.frames.len));
    if (total == 0 or total > 5760) return error.UnsupportedAudioFormat;
    return @intCast(total);
}

pub fn demuxOggAlloc(allocator: std.mem.Allocator, ogg_bytes: []const u8) !Demuxed {
    var packet_sequence = try ogg.parsePacketsAlloc(allocator, ogg_bytes);
    defer packet_sequence.deinit();

    if (packet_sequence.packets.len < 3) return error.UnsupportedAudioFormat;
    const header = try parseHead(packet_sequence.packets[0].bytes);
    if (packet_sequence.packets[1].bytes.len < 8 or !std.mem.eql(u8, packet_sequence.packets[1].bytes[0..8], "OpusTags")) {
        return error.UnsupportedAudioFormat;
    }

    const audio_packets = packet_sequence.packets[2..];
    if (audio_packets.len == 0) return error.UnsupportedAudioFormat;

    const packets = try allocator.alloc([]u8, audio_packets.len);
    errdefer {
        for (packets[0..audio_packets.len]) |packet| allocator.free(packet);
        allocator.free(packets);
    }
    const packet_tocs = try allocator.alloc(Toc, audio_packets.len);
    errdefer allocator.free(packet_tocs);
    const packet_sample_counts = try allocator.alloc(u16, audio_packets.len);
    errdefer allocator.free(packet_sample_counts);

    var total_decoded_frames: u64 = 0;
    var last_granule: ?u64 = null;
    for (audio_packets, 0..) |packet, i| {
        packets[i] = try allocator.dupe(u8, packet.bytes);
        packet_tocs[i] = try parseToc(packet.bytes[0]);
        packet_sample_counts[i] = try packetSamples(packet.bytes, 48_000);
        total_decoded_frames += packet_sample_counts[i];
        if (packet.granule_applies) {
            if (packet.page_granule_position < header.pre_skip or packet.page_granule_position > total_decoded_frames) {
                return error.UnsupportedAudioFormat;
            }
            last_granule = packet.page_granule_position;
        }
    }

    const final_granule = last_granule orelse return error.UnsupportedAudioFormat;
    const discard_padding_frames_u64 = total_decoded_frames - final_granule;
    if (discard_padding_frames_u64 > std.math.maxInt(u16)) return error.UnsupportedAudioFormat;

    return .{
        .header = header,
        .packets = packets,
        .packet_tocs = packet_tocs,
        .packet_sample_counts = packet_sample_counts,
        .total_decoded_frames = total_decoded_frames,
        .playable_frames = final_granule - header.pre_skip,
        .discard_padding_frames = @intCast(discard_padding_frames_u64),
        .allocator = allocator,
    };
}

pub fn classifyFrameShapeAlloc(allocator: std.mem.Allocator, packet: []const u8) !FrameShape {
    var split = try splitFramesAlloc(allocator, packet);
    defer split.deinit();
    return .{
        .mode = split.toc.mode,
        .bandwidth = split.toc.bandwidth,
        .stereo = split.toc.stereo,
        .frame_count = split.frames.len,
        .frame_duration_us = split.toc.frame_duration_us,
        .packet_duration_us = split.toc.frame_duration_us * @as(u32, @intCast(split.frames.len)),
    };
}

fn parseCode2FrameLength(payload: []const u8) !struct { usize, usize } {
    if (payload.len < 1) return error.UnsupportedAudioFormat;
    const b0 = payload[0];
    if (b0 < 252) return .{ b0, 1 };
    if (payload.len < 2) return error.UnsupportedAudioFormat;
    return .{ @as(usize, b0) + 4 * @as(usize, payload[1]), 2 };
}

fn parseCode3FramesAlloc(allocator: std.mem.Allocator, toc: Toc, payload: []const u8) !FramePacket {
    if (payload.len < 1) return error.UnsupportedAudioFormat;
    const frame_count = payload[0] & 0x3f;
    const has_padding = (payload[0] & 0x40) != 0;
    const is_vbr = (payload[0] & 0x80) != 0;
    if (frame_count == 0 or frame_count > 48) return error.UnsupportedAudioFormat;

    var cursor: usize = 1;
    var padding: usize = 0;
    if (has_padding) {
        while (true) {
            if (cursor >= payload.len) return error.UnsupportedAudioFormat;
            const padding_byte = payload[cursor];
            cursor += 1;
            // RFC 6716 3.2.5: a 255 byte adds 254 padding bytes and the
            // count continues in the next byte.
            padding += if (padding_byte == 255) @as(usize, 254) else padding_byte;
            if (padding_byte != 255) break;
        }
    }
    if (cursor + padding > payload.len) return error.UnsupportedAudioFormat;
    const packet_end = payload.len - padding;
    const frames = try allocator.alloc([]const u8, frame_count);
    errdefer allocator.free(frames);

    if (!is_vbr) {
        const frame_bytes = packet_end - cursor;
        if (frame_bytes % frame_count != 0) return error.UnsupportedAudioFormat;
        const per_frame = frame_bytes / frame_count;
        for (0..frame_count) |i| {
            frames[i] = payload[cursor + i * per_frame .. cursor + (i + 1) * per_frame];
        }
        return .{ .toc = toc, .frames = frames, .allocator = allocator };
    }

    var frame_lengths = [_]usize{0} ** 48;
    var bytes_remaining = packet_end - cursor;
    for (0..frame_count - 1) |i| {
        const frame_len, const header_len = try parseCode2FrameLength(payload[cursor..packet_end]);
        cursor += header_len;
        bytes_remaining -= header_len;
        if (frame_len > bytes_remaining) return error.UnsupportedAudioFormat;
        frame_lengths[i] = frame_len;
        bytes_remaining -= frame_len;
    }
    frame_lengths[frame_count - 1] = bytes_remaining;
    for (0..frame_count) |i| {
        const frame_len = frame_lengths[i];
        if (cursor + frame_len > packet_end) return error.UnsupportedAudioFormat;
        frames[i] = payload[cursor .. cursor + frame_len];
        cursor += frame_len;
    }
    if (cursor != packet_end) return error.UnsupportedAudioFormat;
    return .{ .toc = toc, .frames = frames, .allocator = allocator };
}

fn readBeU32(bytes: []const u8) u32 {
    return (@as(u32, bytes[0]) << 24) |
        (@as(u32, bytes[1]) << 16) |
        (@as(u32, bytes[2]) << 8) |
        @as(u32, bytes[3]);
}

fn silkFrameDurationUs(index: u8) u32 {
    return switch (index) {
        0 => 10_000,
        1 => 20_000,
        2 => 40_000,
        3 => 60_000,
        else => unreachable,
    };
}

fn hybridFrameDurationUs(index: u8) u32 {
    return switch (index) {
        0 => 10_000,
        1 => 20_000,
        else => unreachable,
    };
}

fn celtFrameDurationUs(index: u8) u32 {
    return switch (index) {
        0 => 2_500,
        1 => 5_000,
        2 => 10_000,
        3 => 20_000,
        else => unreachable,
    };
}

fn trimInterleavedOwned(
    allocator: std.mem.Allocator,
    samples: []f32,
    channels: usize,
    trim_start_frames_u16: u16,
    playable_frames_u64: u64,
) ![]f32 {
    if (channels == 0 or samples.len % channels != 0) return error.UnsupportedAudioFormat;
    const decoded_frames = samples.len / channels;
    const trim_start_frames = std.math.cast(usize, trim_start_frames_u16) orelse return error.UnsupportedAudioFormat;
    const playable_frames = std.math.cast(usize, playable_frames_u64) orelse return error.UnsupportedAudioFormat;
    if (trim_start_frames > decoded_frames) return error.UnsupportedAudioFormat;
    if (playable_frames > decoded_frames - trim_start_frames) return error.UnsupportedAudioFormat;

    const start = trim_start_frames * channels;
    const len = playable_frames * channels;
    const trimmed = try allocator.alloc(f32, len);
    @memcpy(trimmed, samples[start .. start + len]);
    allocator.free(samples);
    return trimmed;
}

fn ilog(v: anytype) u6 {
    var value: u64 = @intCast(v);
    var bits: u6 = 0;
    while (value != 0) : (value >>= 1) bits += 1;
    return bits;
}

fn readLeU16(bytes: []const u8) u16 {
    return @as(u16, bytes[0]) | (@as(u16, bytes[1]) << 8);
}

fn readLeU32(bytes: []const u8) u32 {
    return @as(u32, bytes[0]) |
        (@as(u32, bytes[1]) << 8) |
        (@as(u32, bytes[2]) << 16) |
        (@as(u32, bytes[3]) << 24);
}

fn appendLeU16(list: *std.ArrayList(u8), value: u16, allocator: std.mem.Allocator) !void {
    try list.appendSlice(allocator, &.{
        @truncate(value),
        @truncate(value >> 8),
    });
}

fn appendLeU32(list: *std.ArrayList(u8), value: u32, allocator: std.mem.Allocator) !void {
    try list.appendSlice(allocator, &.{
        @truncate(value),
        @truncate(value >> 8),
        @truncate(value >> 16),
        @truncate(value >> 24),
    });
}

fn appendLeU64(list: *std.ArrayList(u8), value: u64, allocator: std.mem.Allocator) !void {
    try list.appendSlice(allocator, &.{
        @truncate(value),
        @truncate(value >> 8),
        @truncate(value >> 16),
        @truncate(value >> 24),
        @truncate(value >> 32),
        @truncate(value >> 40),
        @truncate(value >> 48),
        @truncate(value >> 56),
    });
}

fn buildSyntheticOpusHeadAlloc(allocator: std.mem.Allocator, channels: u8, pre_skip: u16) ![]u8 {
    var head = std.ArrayList(u8).empty;
    errdefer head.deinit(allocator);

    try head.appendSlice(allocator, "OpusHead");
    try head.append(allocator, 1);
    try head.append(allocator, channels);
    try appendLeU16(&head, pre_skip, allocator);
    try appendLeU32(&head, 48_000, allocator);
    try appendLeU16(&head, 0, allocator);
    try head.append(allocator, 0);
    return head.toOwnedSlice(allocator);
}

fn buildSyntheticOpusTagsAlloc(allocator: std.mem.Allocator) ![]u8 {
    var tags = std.ArrayList(u8).empty;
    errdefer tags.deinit(allocator);

    try tags.appendSlice(allocator, "OpusTags");
    try appendLeU32(&tags, 0, allocator);
    try appendLeU32(&tags, 0, allocator);
    return tags.toOwnedSlice(allocator);
}

fn buildSyntheticPacketStreamAlloc(allocator: std.mem.Allocator, packets: []const []const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    for (packets) |packet| {
        const packet_len = std.math.cast(u32, packet.len) orelse return error.UnsupportedAudioFormat;
        try out.appendSlice(allocator, &.{
            @intCast(packet_len >> 24),
            @intCast((packet_len >> 16) & 0xff),
            @intCast((packet_len >> 8) & 0xff),
            @intCast(packet_len & 0xff),
            0,
            0,
            0,
            0,
        });
        try out.appendSlice(allocator, packet);
    }
    return out.toOwnedSlice(allocator);
}

fn appendSyntheticOggPage(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    serial: u32,
    sequence: u32,
    header_type: u8,
    granule_position: u64,
    packet: []const u8,
) !void {
    const segment_count = @divTrunc(packet.len + 254, 255);
    try out.appendSlice(allocator, "OggS");
    try out.append(allocator, 0);
    try out.append(allocator, header_type);
    try appendLeU64(out, granule_position, allocator);
    try appendLeU32(out, serial, allocator);
    try appendLeU32(out, sequence, allocator);
    try appendLeU32(out, 0, allocator);
    try out.append(allocator, @intCast(segment_count));

    var remaining = packet.len;
    while (remaining >= 255) : (remaining -= 255) try out.append(allocator, 255);
    try out.append(allocator, @intCast(remaining));
    try out.appendSlice(allocator, packet);
}

fn buildSyntheticOggOpusStreamAlloc(
    allocator: std.mem.Allocator,
    channels: u8,
    pre_skip: u16,
    audio_packets: []const []const u8,
    packet_sample_counts: []const u16,
) ![]u8 {
    if (audio_packets.len == 0 or audio_packets.len != packet_sample_counts.len) return error.UnsupportedAudioFormat;

    const serial: u32 = 0x1234_5678;
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);

    const head = try buildSyntheticOpusHeadAlloc(allocator, channels, pre_skip);
    defer allocator.free(head);
    const tags = try buildSyntheticOpusTagsAlloc(allocator);
    defer allocator.free(tags);

    try appendSyntheticOggPage(&out, allocator, serial, 0, 0x02, 0, head);
    try appendSyntheticOggPage(&out, allocator, serial, 1, 0x00, 0, tags);

    var granule: u64 = 0;
    for (audio_packets, packet_sample_counts, 0..) |packet, sample_count, idx| {
        granule += sample_count;
        try appendSyntheticOggPage(
            &out,
            allocator,
            serial,
            @intCast(idx + 2),
            if (idx + 1 == audio_packets.len) 0x04 else 0x00,
            granule,
            packet,
        );
    }
    return out.toOwnedSlice(allocator);
}

test "parse checked-in opus head" {
    var packets = try ogg.parsePacketsAlloc(std.testing.allocator, tone_opus_bytes);
    defer packets.deinit();

    const head = try parseHead(packets.packets[0].bytes);
    try std.testing.expectEqual(@as(u8, 2), head.channels);
    try std.testing.expectEqual(@as(u16, 312), head.pre_skip);
    try std.testing.expectEqual(@as(u32, 16000), head.input_sample_rate);
    try std.testing.expectEqual(@as(u8, 0), head.mapping_family);
}

test "parse checked-in opus toc and frame packing" {
    inline for ([_]struct {
        bytes: []const u8,
        expected_channels: u8,
        expected_bandwidth: Bandwidth,
    }{
        .{ .bytes = tone_opus_bytes, .expected_channels = 2, .expected_bandwidth = .wb },
        .{ .bytes = tone_opus_ogg_bytes, .expected_channels = 2, .expected_bandwidth = .wb },
        .{ .bytes = tone_opus_mono_bytes, .expected_channels = 1, .expected_bandwidth = .fb },
    }) |case| {
        var demuxed = try demuxOggAlloc(std.testing.allocator, case.bytes);
        defer demuxed.deinit();

        try std.testing.expectEqual(case.expected_channels, demuxed.header.channels);
        try std.testing.expectEqual(@as(usize, 51), demuxed.packets.len);
        try std.testing.expectEqual(@as(Mode, .celt), demuxed.packet_tocs[0].mode);
        try std.testing.expectEqual(case.expected_bandwidth, demuxed.packet_tocs[0].bandwidth);
        try std.testing.expectEqual(@as(u32, 20_000), demuxed.packet_tocs[0].frame_duration_us);
        try std.testing.expectEqual(@as(bool, case.expected_channels == 2), demuxed.packet_tocs[0].stereo);

        var split = try splitFramesAlloc(std.testing.allocator, demuxed.packets[0]);
        defer split.deinit();
        try std.testing.expectEqual(@as(usize, 1), split.frames.len);
        try std.testing.expect(split.frames[0].len > 100);
    }
}

test "demux checked-in opus fixtures exposes trim and packet counts" {
    inline for ([_]struct {
        bytes: []const u8,
        expected_channels: u8,
    }{
        .{ .bytes = tone_opus_bytes, .expected_channels = 2 },
        .{ .bytes = tone_opus_ogg_bytes, .expected_channels = 2 },
        .{ .bytes = tone_opus_mono_bytes, .expected_channels = 1 },
    }) |case| {
        var demuxed = try demuxOggAlloc(std.testing.allocator, case.bytes);
        defer demuxed.deinit();

        try std.testing.expectEqual(case.expected_channels, demuxed.header.channels);
        try std.testing.expectEqual(@as(u16, 312), demuxed.header.pre_skip);
        try std.testing.expectEqual(@as(usize, 51), demuxed.packets.len);
        try std.testing.expectEqual(@as(u64, 48_960), demuxed.total_decoded_frames);
        try std.testing.expectEqual(@as(u64, 48_000), demuxed.playable_frames);
        try std.testing.expectEqual(@as(u16, 648), demuxed.discard_padding_frames);
        try std.testing.expectEqual(@as(u16, 960), demuxed.packet_sample_counts[0]);
        try std.testing.expectEqual(@as(u16, 960), demuxed.packet_sample_counts[demuxed.packet_sample_counts.len - 1]);
    }
}

test "classify checked-in opus frame shapes" {
    inline for ([_]struct {
        bytes: []const u8,
        expected_bandwidth: Bandwidth,
        expected_stereo: bool,
    }{
        .{ .bytes = tone_opus_bytes, .expected_bandwidth = .wb, .expected_stereo = true },
        .{ .bytes = tone_opus_ogg_bytes, .expected_bandwidth = .wb, .expected_stereo = true },
        .{ .bytes = tone_opus_mono_bytes, .expected_bandwidth = .fb, .expected_stereo = false },
    }) |case| {
        var demuxed = try demuxOggAlloc(std.testing.allocator, case.bytes);
        defer demuxed.deinit();

        const shape = try classifyFrameShapeAlloc(std.testing.allocator, demuxed.packets[0]);
        try std.testing.expectEqual(@as(Mode, .celt), shape.mode);
        try std.testing.expectEqual(case.expected_bandwidth, shape.bandwidth);
        try std.testing.expectEqual(case.expected_stereo, shape.stereo);
        try std.testing.expectEqual(@as(usize, 1), shape.frame_count);
        try std.testing.expectEqual(@as(u32, 20_000), shape.frame_duration_us);
        try std.testing.expectEqual(@as(u32, 20_000), shape.packet_duration_us);
    }
}

test "classify real mono celt 5ms packet shape" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_celt_mono_5ms_opus_bytes);
    defer demuxed.deinit();

    const shape = try classifyFrameShapeAlloc(std.testing.allocator, demuxed.packets[0]);
    try std.testing.expectEqual(@as(Mode, .celt), shape.mode);
    try std.testing.expectEqual(@as(Bandwidth, .fb), shape.bandwidth);
    try std.testing.expectEqual(@as(bool, false), shape.stereo);
    try std.testing.expectEqual(@as(usize, 1), shape.frame_count);
    try std.testing.expectEqual(@as(u32, 5_000), shape.frame_duration_us);
    try std.testing.expectEqual(@as(u32, 5_000), shape.packet_duration_us);
}

test "classify real mono celt 120ms packet shape" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_celt_mono_120ms_opus_bytes);
    defer demuxed.deinit();

    const shape = try classifyFrameShapeAlloc(std.testing.allocator, demuxed.packets[0]);
    try std.testing.expectEqual(@as(Mode, .celt), shape.mode);
    try std.testing.expectEqual(@as(Bandwidth, .fb), shape.bandwidth);
    try std.testing.expectEqual(@as(bool, false), shape.stereo);
    try std.testing.expectEqual(@as(usize, 6), shape.frame_count);
    try std.testing.expectEqual(@as(u32, 20_000), shape.frame_duration_us);
    try std.testing.expectEqual(@as(u32, 120_000), shape.packet_duration_us);
}

test "classify real stereo celt 2.5ms packet shape" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_celt_stereo_2p5ms_opus_bytes);
    defer demuxed.deinit();

    const shape = try classifyFrameShapeAlloc(std.testing.allocator, demuxed.packets[0]);
    try std.testing.expectEqual(@as(Mode, .celt), shape.mode);
    try std.testing.expectEqual(@as(Bandwidth, .fb), shape.bandwidth);
    try std.testing.expectEqual(@as(bool, true), shape.stereo);
    try std.testing.expectEqual(@as(usize, 1), shape.frame_count);
    try std.testing.expectEqual(@as(u32, 2_500), shape.frame_duration_us);
    try std.testing.expectEqual(@as(u32, 2_500), shape.packet_duration_us);
}

test "classify real stereo celt 60ms packet shape" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_celt_stereo_60ms_opus_bytes);
    defer demuxed.deinit();

    const shape = try classifyFrameShapeAlloc(std.testing.allocator, demuxed.packets[0]);
    try std.testing.expectEqual(@as(Mode, .celt), shape.mode);
    try std.testing.expectEqual(@as(Bandwidth, .fb), shape.bandwidth);
    try std.testing.expectEqual(@as(bool, true), shape.stereo);
    try std.testing.expectEqual(@as(usize, 3), shape.frame_count);
    try std.testing.expectEqual(@as(u32, 20_000), shape.frame_duration_us);
    try std.testing.expectEqual(@as(u32, 60_000), shape.packet_duration_us);
}

test "classify real stereo celt 40ms packet shape" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_celt_stereo_40ms_opus_bytes);
    defer demuxed.deinit();

    const shape = try classifyFrameShapeAlloc(std.testing.allocator, demuxed.packets[0]);
    try std.testing.expectEqual(@as(Mode, .celt), shape.mode);
    try std.testing.expectEqual(@as(Bandwidth, .fb), shape.bandwidth);
    try std.testing.expectEqual(@as(bool, true), shape.stereo);
    try std.testing.expectEqual(@as(usize, 2), shape.frame_count);
    try std.testing.expectEqual(@as(u32, 20_000), shape.frame_duration_us);
    try std.testing.expectEqual(@as(u32, 40_000), shape.packet_duration_us);
}

test "opus output gain scale follows q8 db units" {
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), outputGainScale(0), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.1220185), outputGainScale(256), 1e-5);

    var samples = [_]f32{ 0.25, -0.25, 0.5, -0.5 };
    applyOutputGainInPlace(samples[0..], 256);
    try std.testing.expectApproxEqAbs(@as(f32, 0.2805046), samples[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, -0.2805046), samples[1], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5610092), samples[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, -0.5610092), samples[3], 1e-5);
}

test "opus range decoder initialization follows RFC 6716" {
    {
        const decoder = RangeDecoder.init(&.{});
        try std.testing.expectEqual(@as(u32, 0x8000_0000), decoder.rng);
        try std.testing.expectEqual(@as(u32, 0x7fff_ffff), decoder.val);
        try std.testing.expectEqual(@as(u1, 0), decoder.rem);
    }
    {
        const decoder = RangeDecoder.init(&.{0xff});
        try std.testing.expectEqual(@as(u32, 0x8000_0000), decoder.rng);
        try std.testing.expectEqual(@as(u32, 0x007f_ffff), decoder.val);
        try std.testing.expectEqual(@as(u1, 0), decoder.rem);
    }
}

test "opus range decoder decodes simple binary symbols from zero stream" {
    var decoder = RangeDecoder.init(&.{});
    const cumulative = [_]u16{ 0, 1, 2 };

    try std.testing.expectEqual(@as(u16, 0), try decoder.decodeSymbol(&cumulative));
    try std.testing.expectEqual(@as(u32, 0x4000_0000), decoder.rng);
    try std.testing.expectEqual(@as(u32, 0x3fff_ffff), decoder.val);

    try std.testing.expectEqual(@as(u16, 0), try decoder.decodeSymbol(&cumulative));
    try std.testing.expect(decoder.rng > (1 << 23));
}

test "opus range decoder reads raw tail bits from end of frame" {
    var decoder = RangeDecoder.init(&.{ 0xaa, 0xf0 });
    try std.testing.expectEqual(@as(u32, 0x0), try decoder.readRawBits(4));
    try std.testing.expectEqual(@as(u32, 0xf), try decoder.readRawBits(4));
    try std.testing.expectEqual(@as(u32, 0xaa), try decoder.readRawBits(8));
}

test "opus range decoder decodes binary symbol with logp shortcut" {
    var decoder = RangeDecoder.init(&.{});
    try std.testing.expectEqual(@as(u1, 0), try decoder.decodeBitLogp(1));
    try std.testing.expect(decoder.rng > (1 << 23));
}

test "decode checked-in mono opus to interleaved pcm on narrow pure-zig lane" {
    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, tone_opus_mono_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 40_000);
    for (decoded.samples[0..@min(decoded.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "decode raw opus packet stream matches ogg alias output" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, tone_opus_ogg_bytes);
    defer demuxed.deinit();

    const packet_stream = try buildSyntheticPacketStreamAlloc(std.testing.allocator, demuxed.packets);
    defer std.testing.allocator.free(packet_stream);

    var from_stream = try decodeInterleavedPacketStreamAlloc(std.testing.allocator, packet_stream, demuxed.header.channels);
    defer from_stream.deinit();
    var from_ogg = try decodeInterleavedOggAlloc(std.testing.allocator, tone_opus_ogg_bytes);
    defer from_ogg.deinit();

    try std.testing.expectEqual(from_ogg.sample_rate, from_stream.sample_rate);
    try std.testing.expectEqual(from_ogg.channels, from_stream.channels);
    // The Ogg path trims the pre-skip and the end padding; the raw packet
    // stream keeps every decoded sample.
    const channels: usize = from_ogg.channels;
    const skip = @as(usize, demuxed.header.pre_skip) * channels;
    const padding = @as(usize, demuxed.discard_padding_frames) * channels;
    try std.testing.expectEqual(from_ogg.samples.len + skip + padding, from_stream.samples.len);
    for (from_ogg.samples, from_stream.samples[skip .. skip + from_ogg.samples.len]) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "decode real silk and hybrid opus fixtures to interleaved pcm" {
    inline for ([_]struct { bytes: []const u8, channels: u8 }{
        .{ .bytes = probe_silk_mono_fec_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_silk_mono_fec_10ms_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_silk_mono_fec_40ms_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_silk_mono_fec_60ms_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_silk_mono_10ms_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_silk_mono_60ms_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_silk_stereo_fec_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_silk_stereo_fec_10ms_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_silk_stereo_fec_40ms_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_silk_stereo_fec_60ms_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_silk_stereo_10ms_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_silk_stereo_40ms_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_hybrid_mono_10ms_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_hybrid_mono_fec_10ms_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_hybrid_mono_fec_ogg_bytes, .channels = 1 },
        .{ .bytes = probe_hybrid_stereo_10ms_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_hybrid_stereo_fec_10ms_ogg_bytes, .channels = 2 },
        .{ .bytes = probe_hybrid_stereo_fec_ogg_bytes, .channels = 2 },
    }) |case| {
        var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, case.bytes);
        defer decoded.deinit();

        try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
        try std.testing.expectEqual(case.channels, decoded.channels);
        try std.testing.expect(decoded.samples.len >= 960 * @as(usize, case.channels));
        var peak: f32 = 0;
        for (decoded.samples) |sample| {
            try std.testing.expect(std.math.isFinite(sample));
            peak = @max(peak, @abs(sample));
        }
        try std.testing.expect(peak > 1e-4);
        try std.testing.expect(peak <= 1.0);
    }
}

test "decode real mono celt 5ms opus fixture to interleaved pcm" {
    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, probe_celt_mono_5ms_opus_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 40_000);
    for (decoded.samples[0..@min(decoded.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "decode real mono celt 120ms opus fixture to interleaved pcm" {
    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, probe_celt_mono_120ms_opus_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 40_000);
    for (decoded.samples[0..@min(decoded.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "decode real stereo celt 2.5ms opus fixture to interleaved pcm" {
    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, probe_celt_stereo_2p5ms_opus_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len > 80_000);
    for (decoded.samples[0..@min(decoded.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "decode real stereo celt 60ms opus fixture to interleaved pcm" {
    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, probe_celt_stereo_60ms_opus_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len > 80_000);
    for (decoded.samples[0..@min(decoded.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "decode real stereo celt 40ms opus fixture to interleaved pcm" {
    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, probe_celt_stereo_40ms_opus_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len > 80_000);
    for (decoded.samples[0..@min(decoded.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "decode checked-in stereo opus aliases to interleaved pcm on widened pure-zig lane" {
    var first = try decodeInterleavedOggAlloc(std.testing.allocator, tone_opus_bytes);
    defer first.deinit();
    var second = try decodeInterleavedOggAlloc(std.testing.allocator, tone_opus_ogg_bytes);
    defer second.deinit();

    inline for ([_]*DecodedInterleaved{ &first, &second }) |decoded| {
        try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
        try std.testing.expectEqual(@as(u8, 2), decoded.channels);
        try std.testing.expect(decoded.samples.len > 80_000);
        for (decoded.samples[0..@min(decoded.samples.len, 512)]) |sample| {
            try std.testing.expect(std.math.isFinite(sample));
        }
    }

    try std.testing.expectEqual(first.samples.len, second.samples.len);
}
