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

// Shared generic audio primitives.
//
// This module intentionally stays model-agnostic. Model-specific presets and
// feature shaping for Whisper, CLAP, etc. live under src/pipelines/audio.zig.

const std = @import("std");
const builtin = @import("builtin");
const wav_impl = @import("wav.zig");
const mp3_impl = @import("mp3.zig");
const mp4_impl = @import("mp4.zig");
const caf_impl = @import("caf.zig");
const aac_impl = @import("aac.zig");
const alac_impl = @import("alac.zig");
const aiff_impl = @import("aiff.zig");
const au_impl = @import("au.zig");
const flac_impl = @import("flac.zig");
const ogg_impl = @import("ogg.zig");
const opus_impl = @import("opus.zig");
const vorbis_impl = @import("vorbis.zig");
const conformance_impl = @import("conformance.zig");

const tone_wav_bytes = @embedFile("../testdata/tone.wav");
const tone_mp3_bytes = @embedFile("../testdata/tone.mp3");
const tone_aac_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.aac");
const tone_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.m4a");
const tone_sbr_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-12k-sbr.m4a");
const tone_mp4_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.mp4");
const tone_alac_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac.m4a");
const tone_alac_mp4_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac.mp4");
const tone_alac_24bit_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac-24bit.m4a");
const tone_alac_24bit_mp4_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac-24bit.mp4");
const tone_aiff_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.aiff");
const tone_caf_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.caf");
const tone_caf_24bit_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-alac-24bit.caf");
const tone_oga_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.oga");
const tone_opus_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-opus.ogg");
const tone_flac_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-flac.ogg");
const tone_aac_44k_mono_bytes = @embedFile("../testdata/codec-corpus/tone-mono-44k.aac");
const tone_m4a_44k_mono_bytes = @embedFile("../testdata/codec-corpus/tone-mono-44k.m4a");
const tone_mp4_44k_mono_bytes = @embedFile("../testdata/codec-corpus/tone-mono-44k.mp4");
const transient_aac_44k_pns_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-pns.aac");
const noise_aac_44k_tns_gain_bytes = @embedFile("../testdata/codec-corpus/noise-mono-44k-tns-gain.aac");
const noise_stereo_aac_44k_tns_bytes = @embedFile("../testdata/codec-corpus/noise-stereo-44k-tns.aac");
const transient_aac_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-short.aac");
const transient_stereo_aac_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-stereo-44k-short.aac");
const transient_m4a_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-short.m4a");
const transient_mp4_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-short.mp4");
const transient_stereo_m4a_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-stereo-44k-short.m4a");
const transient_stereo_mp4_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-stereo-44k-short.mp4");
const tone_opus_48k_mono_bytes = @embedFile("../testdata/codec-corpus/tone-mono-48k.opus");
const probe_opus_celt_48k_mono_5ms_bytes = @embedFile("../testdata/codec-corpus/probe-celt-mono-48k-5ms.opus");
const probe_opus_celt_48k_mono_120ms_bytes = @embedFile("../testdata/codec-corpus/probe-celt-mono-48k-120ms.opus");
const probe_opus_celt_48k_stereo_2p5ms_bytes = @embedFile("../testdata/codec-corpus/probe-celt-stereo-48k-2p5ms.opus");
const probe_opus_celt_48k_stereo_40ms_bytes = @embedFile("../testdata/codec-corpus/probe-celt-stereo-48k-40ms.opus");
const probe_opus_celt_48k_stereo_60ms_bytes = @embedFile("../testdata/codec-corpus/probe-celt-stereo-48k-60ms.opus");
const probe_opus_silk_16k_mono_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec.ogg");
const probe_opus_silk_16k_mono_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec-10ms.ogg");
const probe_opus_silk_16k_mono_fec_40ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec-40ms.ogg");
const probe_opus_silk_16k_mono_fec_60ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-fec-60ms.ogg");
const probe_opus_silk_16k_mono_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-10ms.ogg");
const probe_opus_silk_16k_mono_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k.ogg");
const probe_opus_silk_16k_mono_60ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-mono-16k-60ms.ogg");
const probe_opus_silk_16k_stereo_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec.ogg");
const probe_opus_silk_16k_stereo_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec-10ms.ogg");
const probe_opus_silk_16k_stereo_fec_40ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec-40ms.ogg");
const probe_opus_silk_16k_stereo_fec_60ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-fec-60ms.ogg");
const probe_opus_silk_16k_stereo_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-10ms.ogg");
const probe_opus_silk_16k_stereo_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k.ogg");
const probe_opus_silk_16k_stereo_40ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-silk-stereo-16k-40ms.ogg");
const probe_opus_hybrid_48k_mono_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-mono-48k-10ms.ogg");
const probe_opus_hybrid_48k_mono_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-mono-48k-fec-10ms.ogg");
const probe_opus_hybrid_48k_mono_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-mono-48k-fec.ogg");
const probe_opus_hybrid_48k_mono_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-mono-48k.ogg");
const probe_opus_hybrid_48k_stereo_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-stereo-48k-10ms.ogg");
const probe_opus_hybrid_48k_stereo_fec_10ms_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-stereo-48k-fec-10ms.ogg");
const probe_opus_hybrid_48k_stereo_fec_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-stereo-48k-fec.ogg");
const probe_opus_hybrid_48k_stereo_ogg_bytes = @embedFile("../testdata/codec-corpus/probe-hybrid-stereo-48k.ogg");
const tone_flac_24bit_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-24bit.flac");
const unknown_audio_bytes = [_]u8{ 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 };
const synthetic_au_pcm16_bytes = [_]u8{
    '.',  's',  'n',  'd',  0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x04,
    0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x3e, 0x80, 0x00, 0x00, 0x00, 0x02,
    0x80, 0x00, 0x40, 0x00,
};
const synthetic_caf_lpcm_pcm16_bytes = [_]u8{
    'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
    'd',  'e',  's',  'c',  0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x20, 0x40, 0xcf, 0x40, 0x00,
    0x00, 0x00, 0x00, 0x00, 'l',  'p',  'c',  'm',
    0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x04,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
    0x00, 0x00, 0x00, 0x10, 'd',  'a',  't',  'a',
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x40,
};
const tone_ogg_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.ogg");
const tone_opus_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.opus");
const tone_flac_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.flac");

pub const wav = wav_impl;
pub const mp3 = mp3_impl;
pub const mp4 = mp4_impl;
pub const caf = caf_impl;
pub const aac = aac_impl;
pub const alac = alac_impl;
pub const aiff = aiff_impl;
pub const au = au_impl;
pub const flac = flac_impl;
pub const ogg = ogg_impl;
pub const opus = opus_impl;
pub const vorbis = vorbis_impl;
pub const conformance = conformance_impl;

const VEC_LEN = if (builtin.cpu.arch == .wasm32) 4 else 8;
const F32xN = @Vector(VEC_LEN, f32);
const Complex = struct { re: f64, im: f64 };

pub const NormalizationType = enum { whisper, simple };

pub const AudioConfig = struct {
    sample_rate: u32,
    n_fft: u32,
    hop_length: u32,
    n_mels: u32,
    chunk_length_s: u32,
    normalization: NormalizationType,
};

pub const Audio = struct {
    samples: []f32,
    sample_rate: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Audio) void {
        self.allocator.free(self.samples);
    }
};

pub const AudioInterleaved = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *AudioInterleaved) void {
        self.allocator.free(self.samples);
    }
};

pub const PcmAudio = struct {
    samples: []const f32,
    sample_rate: u32,
};

pub const PcmAudioInterleaved = struct {
    samples: []const f32,
    sample_rate: u32,
    channels: u8,
};

pub const EncodedFormat = enum {
    wav,
    mp3,
    aac,
    mp4,
    ogg,
    opus,
    flac,
    aiff,
    caf,
    au,
};

pub const DecodeOptions = struct {
    format_hint: ?EncodedFormat = null,
    mime_hint: ?[]const u8 = null,
    file_name_hint: ?[]const u8 = null,
};

fn detectFormatWithOptions(audio_bytes: []const u8, options: DecodeOptions) ?EncodedFormat {
    if (options.format_hint) |format| return format;
    if (options.mime_hint) |mime| {
        if (detectFormatFromMime(mime)) |format| return format;
    }
    if (detectFormat(audio_bytes)) |sniffed| {
        if (sniffed == .mp3 or sniffed == .aac) {
            if (options.file_name_hint) |file_name| {
                if (detectFormatFromFilename(file_name)) |hinted| {
                    if (hinted != sniffed) return hinted;
                }
            }
        }
        return sniffed;
    }
    if (options.file_name_hint) |file_name| return detectFormatFromFilename(file_name);
    return null;
}

pub fn decodeWav(allocator: std.mem.Allocator, wav_bytes: []const u8) !Audio {
    var decoded = try decodeInterleavedWav(allocator, wav_bytes);
    defer decoded.deinit();

    const mono = try downmixToMono(allocator, decoded.samples, decoded.channels);
    return .{
        .samples = mono,
        .sample_rate = decoded.sample_rate,
        .allocator = allocator,
    };
}

pub fn decodeInterleavedWav(allocator: std.mem.Allocator, wav_bytes: []const u8) !AudioInterleaved {
    const decoded = try wav.decodeInterleaved(allocator, wav_bytes);
    return .{
        .samples = decoded.samples,
        .sample_rate = decoded.format.sample_rate,
        .channels = @intCast(decoded.channels),
        .allocator = allocator,
    };
}

pub fn detectFormat(audio_bytes: []const u8) ?EncodedFormat {
    if (audio_bytes.len >= 12 and
        (std.mem.eql(u8, audio_bytes[0..4], "RIFF") or
            std.mem.eql(u8, audio_bytes[0..4], "RIFX") or
            std.mem.eql(u8, audio_bytes[0..4], "RF64") or
            std.mem.eql(u8, audio_bytes[0..4], "BW64")) and
        std.mem.eql(u8, audio_bytes[8..12], "WAVE")) return .wav;
    if (audio_bytes.len >= 12 and
        std.mem.eql(u8, audio_bytes[0..4], "FORM") and
        (std.mem.eql(u8, audio_bytes[8..12], "AIFF") or std.mem.eql(u8, audio_bytes[8..12], "AIFC"))) return .aiff;
    if (audio_bytes.len >= 4 and std.mem.eql(u8, audio_bytes[0..4], "caff")) return .caf;
    if (audio_bytes.len >= 4 and std.mem.eql(u8, audio_bytes[0..4], ".snd")) return .au;
    if (audio_bytes.len >= 4 and std.mem.eql(u8, audio_bytes[0..4], "fLaC")) return .flac;
    if (detectIsoBmffAudioFormat(audio_bytes)) |format| return format;
    if (audio_bytes.len >= 4 and std.mem.eql(u8, audio_bytes[0..4], "OggS")) {
        const probe_end = @min(audio_bytes.len, 96);
        if (std.mem.indexOf(u8, audio_bytes[0..probe_end], "OpusHead") != null) return .opus;
        return .ogg;
    }
    if (audio_bytes.len >= 3 and std.mem.eql(u8, audio_bytes[0..3], "ID3")) return .mp3;
    if (audio_bytes.len >= 2 and looksLikeMp3Header(audio_bytes[0], audio_bytes[1])) return .mp3;
    if (audio_bytes.len >= 2 and looksLikeAdtsHeader(audio_bytes[0], audio_bytes[1])) return .aac;
    return null;
}

pub fn detectFormatFromMime(mime: []const u8) ?EncodedFormat {
    const normalized = normalizeMime(mime);
    if (normalized.len == 0) return null;

    if (std.mem.eql(u8, normalized, "audio/wav") or
        std.mem.eql(u8, normalized, "audio/x-wav") or
        std.mem.eql(u8, normalized, "audio/wave"))
        return .wav;

    if (std.mem.eql(u8, normalized, "audio/mpeg") or
        std.mem.eql(u8, normalized, "audio/mp3"))
        return .mp3;

    if (std.mem.eql(u8, normalized, "audio/aac") or
        std.mem.eql(u8, normalized, "audio/x-aac") or
        std.mem.eql(u8, normalized, "audio/mp4a-latm"))
        return .aac;

    if (std.mem.eql(u8, normalized, "audio/mp4") or
        std.mem.eql(u8, normalized, "audio/m4a") or
        std.mem.eql(u8, normalized, "audio/x-m4a") or
        std.mem.eql(u8, normalized, "audio/x-m4b") or
        std.mem.eql(u8, normalized, "audio/x-m4p") or
        std.mem.eql(u8, normalized, "audio/quicktime") or
        std.mem.eql(u8, normalized, "video/quicktime") or
        std.mem.eql(u8, normalized, "application/mp4") or
        std.mem.eql(u8, normalized, "video/mp4"))
        return .mp4;

    if (std.mem.eql(u8, normalized, "audio/flac") or
        std.mem.eql(u8, normalized, "audio/x-flac"))
        return .flac;

    if (std.mem.eql(u8, normalized, "audio/aiff") or
        std.mem.eql(u8, normalized, "audio/x-aiff") or
        std.mem.eql(u8, normalized, "audio/aif") or
        std.mem.eql(u8, normalized, "audio/x-aif") or
        std.mem.eql(u8, normalized, "audio/aifc") or
        std.mem.eql(u8, normalized, "audio/x-aifc"))
        return .aiff;

    if (std.mem.eql(u8, normalized, "audio/caf") or
        std.mem.eql(u8, normalized, "audio/x-caf"))
        return .caf;

    if (std.mem.eql(u8, normalized, "audio/basic") or
        std.mem.eql(u8, normalized, "audio/au") or
        std.mem.eql(u8, normalized, "audio/x-au") or
        std.mem.eql(u8, normalized, "audio/snd") or
        std.mem.eql(u8, normalized, "audio/x-snd"))
        return .au;

    if (std.mem.eql(u8, normalized, "audio/opus"))
        return .opus;

    if (std.mem.eql(u8, normalized, "audio/ogg") or
        std.mem.eql(u8, normalized, "application/ogg"))
    {
        if (std.mem.indexOf(u8, mime, "opus") != null) return .opus;
        return .ogg;
    }

    if (std.mem.eql(u8, normalized, "audio/vorbis")) return .ogg;

    return null;
}

pub fn detectFormatFromFilename(file_name: []const u8) ?EncodedFormat {
    const base = basename(file_name);
    const dot_index = std.mem.lastIndexOfScalar(u8, base, '.') orelse return null;
    const ext = base[dot_index + 1 ..];

    if (asciiEqlIgnoreCase(ext, "wav") or asciiEqlIgnoreCase(ext, "wave")) return .wav;
    if (asciiEqlIgnoreCase(ext, "mp3")) return .mp3;
    if (asciiEqlIgnoreCase(ext, "aac") or asciiEqlIgnoreCase(ext, "adts")) return .aac;
    if (asciiEqlIgnoreCase(ext, "m4a") or
        asciiEqlIgnoreCase(ext, "m4b") or
        asciiEqlIgnoreCase(ext, "m4p") or
        asciiEqlIgnoreCase(ext, "mp4") or
        asciiEqlIgnoreCase(ext, "mov") or
        asciiEqlIgnoreCase(ext, "qt")) return .mp4;
    if (asciiEqlIgnoreCase(ext, "flac")) return .flac;
    if (asciiEqlIgnoreCase(ext, "aif") or asciiEqlIgnoreCase(ext, "aiff") or asciiEqlIgnoreCase(ext, "aifc")) return .aiff;
    if (asciiEqlIgnoreCase(ext, "caf")) return .caf;
    if (asciiEqlIgnoreCase(ext, "au") or asciiEqlIgnoreCase(ext, "snd")) return .au;
    if (asciiEqlIgnoreCase(ext, "ogg") or asciiEqlIgnoreCase(ext, "oga")) return .ogg;
    if (asciiEqlIgnoreCase(ext, "opus")) return .opus;

    return null;
}

pub fn canDecodeMime(mime: []const u8) bool {
    const format = detectFormatFromMime(mime) orelse return false;
    return canDecodeFormat(format);
}

pub fn canDecodeFilename(file_name: []const u8) bool {
    const format = detectFormatFromFilename(file_name) orelse return false;
    return canDecodeFormat(format);
}

pub fn canDecodeWithOptions(audio_bytes: []const u8, options: DecodeOptions) bool {
    const format = options.format_hint orelse
        detectFormat(audio_bytes) orelse
        (if (options.mime_hint) |mime| detectFormatFromMime(mime) else null) orelse
        (if (options.file_name_hint) |file_name| detectFormatFromFilename(file_name) else null) orelse
        return false;
    switch (format) {
        .aac => {
            var decoded_stereo = aac.decodeInterleavedStereoAdtsAlloc(std.heap.page_allocator, audio_bytes) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return canDecodeFormat(format),
            };
            if (decoded_stereo) |*owned| {
                owned.deinit();
                return true;
            }
            var decoded_mono = aac.decodeInterleavedMonoAdtsAlloc(std.heap.page_allocator, audio_bytes) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return canDecodeFormat(format),
            };
            if (decoded_mono) |*owned| {
                owned.deinit();
                return true;
            }
            return canDecodeFormat(format);
        },
        .mp4 => {
            const demuxed = mp4.demux(std.heap.page_allocator, audio_bytes) catch return canDecodeFormat(format);
            var owned = demuxed;
            defer owned.deinit();
            if (owned.codec == .aac) {
                var decoded_stereo = aac.decodeInterleavedStereoAccessUnitsAlloc(
                    std.heap.page_allocator,
                    owned.sample_rate,
                    owned.channels,
                    owned.decoder_config,
                    owned.access_units,
                ) catch |err| switch (err) {
                    error.UnsupportedAudioFormat => null,
                    else => return canDecodeFormat(format),
                };
                if (decoded_stereo) |*owned_decoded| {
                    owned_decoded.deinit();
                    return true;
                }
                var decoded_mono = aac.decodeInterleavedMonoAccessUnitsAlloc(
                    std.heap.page_allocator,
                    owned.sample_rate,
                    owned.channels,
                    owned.decoder_config,
                    owned.access_units,
                ) catch |err| switch (err) {
                    error.UnsupportedAudioFormat => null,
                    else => return canDecodeFormat(format),
                };
                if (decoded_mono) |*owned_decoded| {
                    owned_decoded.deinit();
                    return true;
                }
            } else if (owned.codec == .alac) {
                var decoded_alac = alac.decodeInterleavedPacketizedAlloc(
                    std.heap.page_allocator,
                    owned.sample_rate,
                    owned.channels,
                    owned.decoder_config,
                    owned.access_units,
                ) catch |err| switch (err) {
                    error.UnsupportedAudioFormat => null,
                    else => return canDecodeFormat(format),
                };
                if (decoded_alac) |*owned_decoded| {
                    owned_decoded.deinit();
                    return true;
                }
            }
        },
        .ogg => {
            const codec = ogg.sniffCodec(std.heap.page_allocator, audio_bytes) catch return canDecodeFormat(format);
            if (codec == .flac) {
                var decoded_flac = ogg.decodeInterleavedFlacAlloc(std.heap.page_allocator, audio_bytes) catch |err| switch (err) {
                    error.UnsupportedAudioFormat => null,
                    else => return canDecodeFormat(format),
                };
                if (decoded_flac) |*owned| {
                    owned.deinit();
                    return true;
                }
            }
        },
        .caf => {
            var decoded_lpcm = caf.decodeInterleaved(std.heap.page_allocator, audio_bytes) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return canDecodeFormat(format),
            };
            if (decoded_lpcm) |*owned| {
                owned.deinit();
                return true;
            }

            const demuxed = caf.demux(std.heap.page_allocator, audio_bytes) catch return canDecodeFormat(format);
            var owned = demuxed;
            defer owned.deinit();
            var decoded_alac = alac.decodeInterleavedPacketizedAlloc(
                std.heap.page_allocator,
                owned.sample_rate,
                owned.channels,
                owned.decoder_config,
                owned.access_units,
            ) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return canDecodeFormat(format),
            };
            if (decoded_alac) |*owned_decoded| {
                owned_decoded.deinit();
                return true;
            }
        },
        .aiff => {
            var decoded = aiff.decodeInterleaved(std.heap.page_allocator, audio_bytes) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return canDecodeFormat(format),
            };
            if (decoded) |*owned| {
                owned.deinit();
                return true;
            }
        },
        .au => {
            var decoded = au.decodeInterleaved(std.heap.page_allocator, audio_bytes) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return canDecodeFormat(format),
            };
            if (decoded) |*owned| {
                owned.deinit();
                return true;
            }
        },
        else => {},
    }
    return canDecodeFormat(format);
}

pub fn decode(allocator: std.mem.Allocator, audio_bytes: []const u8, options: DecodeOptions) !Audio {
    var decoded = try decodeInterleaved(allocator, audio_bytes, options);
    defer decoded.deinit();

    const mono = try downmixToMono(allocator, decoded.samples, decoded.channels);
    return .{
        .samples = mono,
        .sample_rate = decoded.sample_rate,
        .allocator = allocator,
    };
}

fn decodeWithoutFallback(allocator: std.mem.Allocator, audio_bytes: []const u8, options: DecodeOptions) !Audio {
    var decoded = try decodeInterleavedWithoutFallback(allocator, audio_bytes, options);
    defer decoded.deinit();

    const mono = try downmixToMono(allocator, decoded.samples, decoded.channels);
    return .{
        .samples = mono,
        .sample_rate = decoded.sample_rate,
        .allocator = allocator,
    };
}

pub fn decodeInterleaved(
    allocator: std.mem.Allocator,
    audio_bytes: []const u8,
    options: DecodeOptions,
) !AudioInterleaved {
    const format = detectFormatWithOptions(audio_bytes, options) orelse
        return error.UnsupportedAudioFormat;
    var decoded = try switch (format) {
        .wav => decodeInterleavedWav(allocator, audio_bytes),
        .mp3 => decodeInterleavedMp3(allocator, audio_bytes),
        .aac => decodeInterleavedAac(allocator, audio_bytes),
        .mp4 => decodeInterleavedMp4(allocator, audio_bytes),
        .ogg => decodeInterleavedOgg(allocator, audio_bytes),
        .opus => decodeInterleavedOpus(allocator, audio_bytes),
        .flac => decodeInterleavedFlac(allocator, audio_bytes),
        .aiff => decodeInterleavedAiff(allocator, audio_bytes),
        .caf => decodeInterleavedCaf(allocator, audio_bytes),
        .au => decodeInterleavedAu(allocator, audio_bytes),
    };
    errdefer decoded.deinit();
    normalizePcmInPlace(decoded.samples);
    return decoded;
}

fn decodeInterleavedWithoutFallback(
    allocator: std.mem.Allocator,
    audio_bytes: []const u8,
    options: DecodeOptions,
) !AudioInterleaved {
    const format = detectFormatWithOptions(audio_bytes, options) orelse
        return error.UnsupportedAudioFormat;
    var decoded = try switch (format) {
        .wav => decodeInterleavedWav(allocator, audio_bytes),
        .aac => decodeInterleavedAacPureZig(allocator, audio_bytes),
        .mp4 => decodeInterleavedMp4PureZig(allocator, audio_bytes),
        .ogg => decodeInterleavedOggPureZig(allocator, audio_bytes),
        .opus => decodeInterleavedOpusPureZig(allocator, audio_bytes),
        .flac => decodeInterleavedFlac(allocator, audio_bytes),
        .aiff => decodeInterleavedAiff(allocator, audio_bytes),
        .caf => decodeInterleavedCafPureZig(allocator, audio_bytes),
        .au => decodeInterleavedAu(allocator, audio_bytes),
        else => return error.UnsupportedAudioFormat,
    };
    errdefer decoded.deinit();
    normalizePcmInPlace(decoded.samples);
    return decoded;
}

pub fn canDecodeFormat(format: EncodedFormat) bool {
    return switch (format) {
        .wav, .aac, .mp4, .ogg, .opus, .flac, .aiff, .caf, .au => true,
        .mp3 => mp3.enabled(),
    };
}

fn decodeMp3(allocator: std.mem.Allocator, mp3_bytes: []const u8) !Audio {
    var decoded = try decodeInterleavedMp3(allocator, mp3_bytes);
    defer decoded.deinit();

    const mono = try downmixToMono(allocator, decoded.samples, decoded.channels);
    return .{
        .samples = mono,
        .sample_rate = decoded.sample_rate,
        .allocator = allocator,
    };
}

fn decodeInterleavedMp3(allocator: std.mem.Allocator, mp3_bytes: []const u8) !AudioInterleaved {
    const decoded = try mp3.decodeInterleaved(allocator, mp3_bytes);
    return .{
        .samples = decoded.samples,
        .sample_rate = decoded.sample_rate,
        .channels = decoded.channels,
        .allocator = allocator,
    };
}

fn decodeInterleavedAiff(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const decoded = try aiff.decodeInterleaved(allocator, audio_bytes);
    return .{
        .samples = decoded.samples,
        .sample_rate = decoded.sample_rate,
        .channels = decoded.channels,
        .allocator = allocator,
    };
}

fn decodeInterleavedAu(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const decoded = try au.decodeInterleaved(allocator, audio_bytes);
    return .{
        .samples = decoded.samples,
        .sample_rate = decoded.sample_rate,
        .channels = decoded.channels,
        .allocator = allocator,
    };
}

fn decodeInterleavedFlac(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const decoded = try flac.decodeInterleaved(allocator, audio_bytes);
    return .{
        .samples = decoded.samples,
        .sample_rate = decoded.sample_rate,
        .channels = decoded.channels,
        .allocator = allocator,
    };
}

fn decodeInterleavedOgg(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    return decodeInterleavedOggPureZig(allocator, audio_bytes);
}

fn decodeInterleavedOpus(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    return decodeInterleavedOpusPureZig(allocator, audio_bytes);
}

fn decodeInterleavedOpusPureZig(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const decoded = try opus.decodeInterleavedOggAlloc(allocator, audio_bytes);
    return .{
        .samples = decoded.samples,
        .sample_rate = decoded.sample_rate,
        .channels = decoded.channels,
        .allocator = allocator,
    };
}

fn decodeInterleavedOggPureZig(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const codec = try ogg.sniffCodec(allocator, audio_bytes);
    switch (codec) {
        .flac => {
            const decoded = try ogg.decodeInterleavedFlacAlloc(allocator, audio_bytes);
            return .{
                .samples = decoded.samples,
                .sample_rate = decoded.sample_rate,
                .channels = decoded.channels,
                .allocator = allocator,
            };
        },
        .vorbis => {
            const decoded = try vorbis.decodeInterleavedOggAlloc(allocator, audio_bytes);
            return .{
                .samples = decoded.samples,
                .sample_rate = decoded.sample_rate,
                .channels = decoded.channels,
                .allocator = allocator,
            };
        },
        .opus => return decodeInterleavedOpusPureZig(allocator, audio_bytes),
    }
}

fn decodeInterleavedAac(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    return decodeInterleavedAacPureZig(allocator, audio_bytes);
}

fn decodeInterleavedAacPureZig(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const pure_zig = aac.decodeInterleavedStereoAdtsAlloc(allocator, audio_bytes) catch |err| switch (err) {
        error.UnsupportedAudioFormat => null,
        else => return err,
    };
    if (pure_zig) |owned_value| {
        const owned = owned_value;
        return .{
            .samples = owned.samples,
            .sample_rate = owned.sample_rate,
            .channels = 2,
            .allocator = allocator,
        };
    }
    const pure_zig_mono = aac.decodeInterleavedMonoAdtsAlloc(allocator, audio_bytes) catch |err| switch (err) {
        error.UnsupportedAudioFormat => null,
        else => return err,
    };
    if (pure_zig_mono) |owned_value| {
        const owned = owned_value;
        return .{
            .samples = owned.samples,
            .sample_rate = owned.sample_rate,
            .channels = 1,
            .allocator = allocator,
        };
    }
    return error.UnsupportedAudioFormat;
}

fn decodeInterleavedMp4(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    return decodeInterleavedMp4PureZig(allocator, audio_bytes);
}

fn decodeInterleavedMp4PureZig(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const demuxed = mp4.demux(allocator, audio_bytes) catch |err| switch (err) {
        error.UnsupportedAudioFormat => null,
        else => return err,
    };
    if (demuxed) |owned_value| {
        var owned = owned_value;
        defer owned.deinit();

        if (owned.codec == .aac) {
            const aac_channels: u16 = blk: {
                const config = aac.parseAudioSpecificConfig(owned.decoder_config) catch break :blk owned.channels;
                const config_channels = config.explicit_channel_count orelse config.channel_config;
                if (config_channels == 1 or config_channels == 2) break :blk config_channels;
                break :blk owned.channels;
            };

            const pure_zig = aac.decodeInterleavedStereoAccessUnitsAlloc(
                allocator,
                owned.sample_rate,
                aac_channels,
                owned.decoder_config,
                owned.access_units,
            ) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return err,
            };
            if (pure_zig) |seq| {
                var decoded = seq;
                errdefer decoded.deinit();
                decoded.samples = try trimOwnedInterleavedForMp4EditList(
                    allocator,
                    decoded.samples,
                    2,
                    owned.trim_start_frames,
                    owned.playable_frames,
                );
                return .{
                    .samples = decoded.samples,
                    .sample_rate = decoded.sample_rate,
                    .channels = 2,
                    .allocator = allocator,
                };
            }
            const pure_zig_mono = aac.decodeInterleavedMonoAccessUnitsAlloc(
                allocator,
                owned.sample_rate,
                aac_channels,
                owned.decoder_config,
                owned.access_units,
            ) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return err,
            };
            if (pure_zig_mono) |seq| {
                var decoded = seq;
                errdefer decoded.deinit();
                decoded.samples = try trimOwnedInterleavedForMp4EditList(
                    allocator,
                    decoded.samples,
                    1,
                    owned.trim_start_frames,
                    owned.playable_frames,
                );
                return .{
                    .samples = decoded.samples,
                    .sample_rate = decoded.sample_rate,
                    .channels = 1,
                    .allocator = allocator,
                };
            }
        } else if (owned.codec == .alac) {
            const pure_zig_alac = alac.decodeInterleavedPacketizedAlloc(
                allocator,
                owned.sample_rate,
                owned.channels,
                owned.decoder_config,
                owned.access_units,
            ) catch |err| switch (err) {
                error.UnsupportedAudioFormat => null,
                else => return err,
            };
            if (pure_zig_alac) |owned_decoded| {
                var decoded = owned_decoded;
                errdefer decoded.deinit();
                decoded.samples = try trimOwnedInterleavedForMp4EditList(
                    allocator,
                    decoded.samples,
                    decoded.channels,
                    owned.trim_start_frames,
                    owned.playable_frames,
                );
                return .{
                    .samples = decoded.samples,
                    .sample_rate = decoded.sample_rate,
                    .channels = decoded.channels,
                    .allocator = allocator,
                };
            }
        }
    }

    return error.UnsupportedAudioFormat;
}

fn decodeInterleavedCaf(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    return decodeInterleavedCafPureZig(allocator, audio_bytes);
}

fn decodeInterleavedCafPureZig(allocator: std.mem.Allocator, audio_bytes: []const u8) !AudioInterleaved {
    const pure_zig_lpcm = caf.decodeInterleaved(allocator, audio_bytes) catch |err| switch (err) {
        error.UnsupportedAudioFormat => null,
        else => return err,
    };
    if (pure_zig_lpcm) |owned_value| {
        const owned = owned_value;
        return .{
            .samples = owned.samples,
            .sample_rate = owned.sample_rate,
            .channels = owned.channels,
            .allocator = allocator,
        };
    }

    const demuxed = caf.demux(allocator, audio_bytes) catch |err| switch (err) {
        error.UnsupportedAudioFormat => null,
        else => return err,
    };
    if (demuxed) |owned_value| {
        var owned = owned_value;
        defer owned.deinit();

        const pure_zig_alac = alac.decodeInterleavedPacketizedAlloc(
            allocator,
            owned.sample_rate,
            owned.channels,
            owned.decoder_config,
            owned.access_units,
        ) catch |err| switch (err) {
            error.UnsupportedAudioFormat => null,
            else => return err,
        };
        if (pure_zig_alac) |owned_decoded| {
            var decoded = owned_decoded;
            errdefer decoded.deinit();
            decoded.samples = try trimOwnedInterleavedForCafPacketTable(
                allocator,
                decoded.samples,
                decoded.channels,
                owned.valid_frames,
                owned.priming_frames,
                owned.remainder_frames,
            );
            return .{
                .samples = decoded.samples,
                .sample_rate = decoded.sample_rate,
                .channels = decoded.channels,
                .allocator = allocator,
            };
        }
    }

    return error.UnsupportedAudioFormat;
}

fn trimOwnedInterleavedForCafPacketTable(
    allocator: std.mem.Allocator,
    samples: []f32,
    channels: u8,
    valid_frames_u64: u64,
    priming_frames_u32: u32,
    remainder_frames_u32: u32,
) ![]f32 {
    if (channels == 0) return error.UnsupportedAudioFormat;

    const valid_frames = std.math.cast(usize, valid_frames_u64) orelse return error.UnsupportedAudioFormat;
    const priming_frames = std.math.cast(usize, priming_frames_u32) orelse return error.UnsupportedAudioFormat;
    const remainder_frames = std.math.cast(usize, remainder_frames_u32) orelse return error.UnsupportedAudioFormat;
    if (valid_frames < priming_frames or valid_frames - priming_frames < remainder_frames) {
        return error.UnsupportedAudioFormat;
    }

    const playable_frames = valid_frames - priming_frames - remainder_frames;
    const start = try std.math.mul(usize, priming_frames, channels);
    const target_len = try std.math.mul(usize, playable_frames, channels);
    if (start + target_len > samples.len) return error.UnsupportedAudioFormat;
    if (start == 0 and target_len == samples.len) return samples;

    const trimmed = try allocator.alloc(f32, target_len);
    @memcpy(trimmed, samples[start .. start + target_len]);
    allocator.free(samples);
    return trimmed;
}

fn trimOwnedInterleavedForMp4EditList(
    allocator: std.mem.Allocator,
    samples: []f32,
    channels: u8,
    trim_start_frames: u64,
    playable_frames_opt: ?u64,
) ![]f32 {
    if (channels == 0) return error.UnsupportedAudioFormat;
    if (samples.len % channels != 0) return error.UnsupportedAudioFormat;
    if (trim_start_frames == 0 and playable_frames_opt == null) return samples;

    const decoded_frames: u64 = @intCast(samples.len / channels);
    if (trim_start_frames > decoded_frames) return error.UnsupportedAudioFormat;
    const playable_frames = playable_frames_opt orelse decoded_frames - trim_start_frames;
    if (playable_frames > decoded_frames - trim_start_frames) return error.UnsupportedAudioFormat;

    const trim_start = std.math.cast(usize, trim_start_frames) orelse return error.UnsupportedAudioFormat;
    const playable = std.math.cast(usize, playable_frames) orelse return error.UnsupportedAudioFormat;
    const start = try std.math.mul(usize, trim_start, channels);
    const target_len = try std.math.mul(usize, playable, channels);
    if (start == 0 and target_len == samples.len) return samples;

    const trimmed = try allocator.alloc(f32, target_len);
    @memcpy(trimmed, samples[start .. start + target_len]);
    allocator.free(samples);
    return trimmed;
}

pub fn normalizePcmInPlace(samples: []f32) void {
    for (samples) |*sample| {
        if (!std.math.isFinite(sample.*)) {
            sample.* = 0.0;
        } else {
            sample.* = std.math.clamp(sample.*, -1.0, 1.0);
        }
    }
}

pub fn downmixToMono(allocator: std.mem.Allocator, samples: []const f32, channels: u8) ![]f32 {
    if (channels == 0) return error.UnsupportedAudioFormat;
    if (channels == 1) {
        const out = try allocator.alloc(f32, samples.len);
        @memcpy(out, samples);
        return out;
    }
    if (channels == 2) {
        return downmixStereoToMono(allocator, samples);
    }
    if (samples.len % channels != 0) return error.UnsupportedAudioFormat;

    const frame_count = samples.len / channels;
    const mono = try allocator.alloc(f32, frame_count);
    errdefer allocator.free(mono);

    for (0..frame_count) |i| {
        var sum: f32 = 0;
        for (0..channels) |ch| {
            sum += samples[i * channels + ch];
        }
        mono[i] = sum / @as(f32, @floatFromInt(channels));
    }

    return mono;
}

pub fn resample(allocator: std.mem.Allocator, samples: []const f32, src_rate: u32, dst_rate: u32) ![]f32 {
    if (src_rate == dst_rate) {
        const out = try allocator.alloc(f32, samples.len);
        @memcpy(out, samples);
        return out;
    }

    const ratio = @as(f64, @floatFromInt(src_rate)) / @as(f64, @floatFromInt(dst_rate));
    const out_len: usize = @intFromFloat(@as(f64, @floatFromInt(samples.len)) / ratio);
    const out = try allocator.alloc(f32, out_len);
    errdefer allocator.free(out);

    for (0..out_len) |i| {
        const src_pos = @as(f64, @floatFromInt(i)) * ratio;
        const idx0: usize = @intFromFloat(@floor(src_pos));
        const idx1 = @min(idx0 + 1, samples.len - 1);
        const frac: f32 = @floatCast(src_pos - @as(f64, @floatFromInt(idx0)));
        out[i] = samples[idx0] * (1.0 - frac) + samples[idx1] * frac;
    }

    return out;
}

pub fn copyOrResample(allocator: std.mem.Allocator, samples: []const f32, src_rate: u32, dst_rate: u32) ![]f32 {
    if (src_rate == dst_rate) {
        const out = try allocator.alloc(f32, samples.len);
        @memcpy(out, samples);
        return out;
    }

    return resample(allocator, samples, src_rate, dst_rate);
}

pub fn logMelSpectrogramWithConfig(
    allocator: std.mem.Allocator,
    samples: []const f32,
    config: AudioConfig,
) ![]f32 {
    const n_fft = config.n_fft;
    const hop = config.hop_length;
    const n_mels = config.n_mels;
    const n_freq = n_fft / 2 + 1;
    const max_frames = config.chunk_length_s * config.sample_rate / hop;

    const min_samples = config.chunk_length_s * config.sample_rate;
    const padded_len = @max(samples.len, min_samples);
    const padded = try allocator.alloc(f32, padded_len);
    defer allocator.free(padded);
    @memcpy(padded[0..samples.len], samples);
    @memset(padded[samples.len..], 0);

    const filters = try melFilterbank(allocator, n_mels, n_fft, config.sample_rate);
    defer allocator.free(filters);

    const window = try allocator.alloc(f32, n_fft);
    defer allocator.free(window);
    for (0..n_fft) |i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(n_fft));
        window[i] = 0.5 * (1.0 - @cos(2.0 * std.math.pi * t));
    }

    var fft_plan = try FftPlan.init(allocator, @intCast(n_fft));
    defer fft_plan.deinit(allocator);

    const output = try allocator.alloc(f32, n_mels * max_frames);
    @memset(output, 0);

    const actual_frames = @min((padded_len - n_fft) / hop + 1, max_frames);
    const magnitudes = try allocator.alloc(f32, n_freq);
    defer allocator.free(magnitudes);

    for (0..actual_frames) |frame| {
        const start = frame * hop;
        try fft_plan.powerSpectrumWindowed(padded[start .. start + n_fft], window, magnitudes);

        for (0..n_mels) |m| {
            const filter_row = filters[m * n_freq ..][0..n_freq];
            const sum = dotProductSimd(filter_row, magnitudes);
            output[m * max_frames + frame] = @max(sum, 1e-10);
        }
    }

    switch (config.normalization) {
        .whisper => {
            for (output[0 .. n_mels * max_frames]) |*v| {
                v.* = @log(v.*) / @log(10.0);
            }
            const max_val = maxValueSimd(output[0 .. n_mels * max_frames]);
            clampScaleWhisper(output[0 .. n_mels * max_frames], max_val);
        },
        .simple => {
            for (output[0 .. n_mels * max_frames]) |*v| {
                v.* = @log(v.*);
            }
        },
    }

    return output;
}

pub fn melFilterbankWithRange(
    allocator: std.mem.Allocator,
    n_mels: u32,
    n_fft: u32,
    sample_rate: u32,
    min_hz: f32,
    max_hz: f32,
) ![]f32 {
    const n_freq = n_fft / 2 + 1;
    const filters = try allocator.alloc(f32, n_mels * n_freq);
    @memset(filters, 0);

    const sr_f: f32 = @floatFromInt(sample_rate);
    const fft_f: f32 = @floatFromInt(n_fft);

    const mel_low = hzToMel(min_hz);
    const mel_high = hzToMel(if (max_hz > 0) @min(max_hz, sr_f / 2.0) else sr_f / 2.0);

    const n_points = n_mels + 2;
    const mel_points = try allocator.alloc(f32, n_points);
    defer allocator.free(mel_points);
    for (0..n_points) |i| {
        const mel = mel_low + @as(f32, @floatFromInt(i)) * (mel_high - mel_low) / @as(f32, @floatFromInt(n_points - 1));
        mel_points[i] = melToHz(mel);
    }

    const bin_points = try allocator.alloc(f32, n_points);
    defer allocator.free(bin_points);
    for (0..n_points) |i| {
        bin_points[i] = mel_points[i] * fft_f / sr_f;
    }

    for (0..n_mels) |m| {
        const left = bin_points[m];
        const center = bin_points[m + 1];
        const right = bin_points[m + 2];

        for (0..n_freq) |k| {
            const k_f = @as(f32, @floatFromInt(k));
            if (k_f > left and k_f <= center) {
                filters[m * n_freq + k] = (k_f - left) / (center - left);
            } else if (k_f > center and k_f < right) {
                filters[m * n_freq + k] = (right - k_f) / (right - center);
            }
        }

        const enorm = 2.0 / (mel_points[m + 2] - mel_points[m]);
        for (0..n_freq) |k| {
            filters[m * n_freq + k] *= enorm;
        }
    }

    return filters;
}

fn melFilterbank(allocator: std.mem.Allocator, n_mels: u32, n_fft: u32, sample_rate: u32) ![]f32 {
    return melFilterbankWithRange(allocator, n_mels, n_fft, sample_rate, 0, 0);
}

fn hzToMel(hz: f32) f32 {
    return 2595.0 * std.math.log10(1.0 + hz / 700.0);
}

fn melToHz(mel: f32) f32 {
    return 700.0 * (std.math.pow(f32, 10.0, mel / 2595.0) - 1.0);
}

fn looksLikeMp3Header(first: u8, second: u8) bool {
    if (first != 0xFF) return false;
    if ((second & 0xE0) != 0xE0) return false;
    if (((second >> 1) & 0x3) == 0x0) return false;
    return true;
}

fn looksLikeAdtsHeader(first: u8, second: u8) bool {
    return first == 0xFF and (second & 0xF6) == 0xF0;
}

fn detectIsoBmffAudioFormat(audio_bytes: []const u8) ?EncodedFormat {
    if (audio_bytes.len < 12) return null;
    if (!std.mem.eql(u8, audio_bytes[4..8], "ftyp")) return null;
    const ftyp_size = std.mem.readInt(u32, audio_bytes[0..4], .big);
    if (ftyp_size < 16) return null;
    const ftyp_end = @min(audio_bytes.len, @as(usize, ftyp_size));

    if (isKnownAudioMp4Brand(audio_bytes[8..12])) return .mp4;

    var offset: usize = 16;
    while (offset + 4 <= ftyp_end) : (offset += 4) {
        if (isKnownAudioMp4Brand(audio_bytes[offset .. offset + 4])) return .mp4;
    }

    return null;
}

fn isKnownAudioMp4Brand(brand: []const u8) bool {
    return std.mem.eql(u8, brand, "M4A ") or
        std.mem.eql(u8, brand, "isom") or
        std.mem.eql(u8, brand, "iso2") or
        std.mem.eql(u8, brand, "mp41") or
        std.mem.eql(u8, brand, "mp42") or
        std.mem.eql(u8, brand, "M4B ") or
        std.mem.eql(u8, brand, "M4P ") or
        std.mem.eql(u8, brand, "qt  ");
}

fn normalizeMime(mime: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, mime, ';') orelse mime.len;
    return std.mem.trim(u8, mime[0..end], " \t\r\n");
}

fn basename(path: []const u8) []const u8 {
    const slash = std.mem.lastIndexOfAny(u8, path, "/\\") orelse return path;
    return path[slash + 1 ..];
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |lhs, rhs| {
        if (std.ascii.toLower(lhs) != std.ascii.toLower(rhs)) return false;
    }
    return true;
}

const CodecCase = conformance.CodecCase(EncodedFormat);
const DecodeButNotClaimedCase = conformance.DecodeButNotClaimedCase(EncodedFormat, DecodeOptions);
const UnsupportedCodecCase = conformance.UnsupportedCodecCase(EncodedFormat, DecodeOptions);
const Mp4DemuxCase = conformance.DemuxCase(mp4.Codec);
const CafDemuxCase = conformance.DemuxCase(caf.Codec);
const AacToolCorpusCase = struct {
    name: []const u8,
    bytes: []const u8,
    expected_sample_rate: u32,
    expected_channels: u8,
};

const checked_in_additional_codec_cases: [58]CodecCase = conformance.buildCheckedInCodecCases(EncodedFormat, .{
    .tone_aac_bytes = tone_aac_bytes,
    .tone_aac_44k_mono_bytes = tone_aac_44k_mono_bytes,
    .transient_aac_44k_pns_bytes = transient_aac_44k_pns_bytes,
    .noise_aac_44k_tns_gain_bytes = noise_aac_44k_tns_gain_bytes,
    .noise_stereo_aac_44k_tns_bytes = noise_stereo_aac_44k_tns_bytes,
    .transient_aac_44k_short_bytes = transient_aac_44k_short_bytes,
    .transient_stereo_aac_44k_short_bytes = transient_stereo_aac_44k_short_bytes,
    .tone_m4a_bytes = tone_m4a_bytes,
    .tone_sbr_m4a_bytes = tone_sbr_m4a_bytes,
    .tone_m4a_44k_mono_bytes = tone_m4a_44k_mono_bytes,
    .transient_m4a_44k_short_bytes = transient_m4a_44k_short_bytes,
    .transient_stereo_m4a_44k_short_bytes = transient_stereo_m4a_44k_short_bytes,
    .tone_mp4_bytes = tone_mp4_bytes,
    .tone_alac_m4a_bytes = tone_alac_m4a_bytes,
    .tone_alac_mp4_bytes = tone_alac_mp4_bytes,
    .tone_alac_24bit_m4a_bytes = tone_alac_24bit_m4a_bytes,
    .tone_alac_24bit_mp4_bytes = tone_alac_24bit_mp4_bytes,
    .tone_mp4_44k_mono_bytes = tone_mp4_44k_mono_bytes,
    .transient_mp4_44k_short_bytes = transient_mp4_44k_short_bytes,
    .transient_stereo_mp4_44k_short_bytes = transient_stereo_mp4_44k_short_bytes,
    .tone_aiff_bytes = tone_aiff_bytes,
    .tone_caf_bytes = tone_caf_bytes,
    .tone_caf_24bit_bytes = tone_caf_24bit_bytes,
    .tone_ogg_bytes = tone_ogg_bytes,
    .tone_oga_bytes = tone_oga_bytes,
    .tone_opus_bytes = tone_opus_bytes,
    .tone_opus_48k_mono_bytes = tone_opus_48k_mono_bytes,
    .tone_opus_ogg_bytes = tone_opus_ogg_bytes,
    .probe_opus_celt_48k_mono_5ms_bytes = probe_opus_celt_48k_mono_5ms_bytes,
    .probe_opus_celt_48k_mono_120ms_bytes = probe_opus_celt_48k_mono_120ms_bytes,
    .probe_opus_celt_48k_stereo_2p5ms_bytes = probe_opus_celt_48k_stereo_2p5ms_bytes,
    .probe_opus_celt_48k_stereo_40ms_bytes = probe_opus_celt_48k_stereo_40ms_bytes,
    .probe_opus_celt_48k_stereo_60ms_bytes = probe_opus_celt_48k_stereo_60ms_bytes,
    .probe_opus_silk_16k_mono_fec_ogg_bytes = probe_opus_silk_16k_mono_fec_ogg_bytes,
    .probe_opus_silk_16k_mono_fec_10ms_ogg_bytes = probe_opus_silk_16k_mono_fec_10ms_ogg_bytes,
    .probe_opus_silk_16k_mono_fec_40ms_ogg_bytes = probe_opus_silk_16k_mono_fec_40ms_ogg_bytes,
    .probe_opus_silk_16k_mono_fec_60ms_ogg_bytes = probe_opus_silk_16k_mono_fec_60ms_ogg_bytes,
    .probe_opus_silk_16k_mono_10ms_ogg_bytes = probe_opus_silk_16k_mono_10ms_ogg_bytes,
    .probe_opus_silk_16k_mono_60ms_ogg_bytes = probe_opus_silk_16k_mono_60ms_ogg_bytes,
    .probe_opus_silk_16k_mono_ogg_bytes = probe_opus_silk_16k_mono_ogg_bytes,
    .probe_opus_silk_16k_stereo_fec_ogg_bytes = probe_opus_silk_16k_stereo_fec_ogg_bytes,
    .probe_opus_silk_16k_stereo_fec_10ms_ogg_bytes = probe_opus_silk_16k_stereo_fec_10ms_ogg_bytes,
    .probe_opus_silk_16k_stereo_fec_40ms_ogg_bytes = probe_opus_silk_16k_stereo_fec_40ms_ogg_bytes,
    .probe_opus_silk_16k_stereo_fec_60ms_ogg_bytes = probe_opus_silk_16k_stereo_fec_60ms_ogg_bytes,
    .probe_opus_silk_16k_stereo_10ms_ogg_bytes = probe_opus_silk_16k_stereo_10ms_ogg_bytes,
    .probe_opus_silk_16k_stereo_ogg_bytes = probe_opus_silk_16k_stereo_ogg_bytes,
    .probe_opus_silk_16k_stereo_40ms_ogg_bytes = probe_opus_silk_16k_stereo_40ms_ogg_bytes,
    .probe_opus_hybrid_48k_mono_10ms_ogg_bytes = probe_opus_hybrid_48k_mono_10ms_ogg_bytes,
    .probe_opus_hybrid_48k_mono_fec_10ms_ogg_bytes = probe_opus_hybrid_48k_mono_fec_10ms_ogg_bytes,
    .probe_opus_hybrid_48k_mono_fec_ogg_bytes = probe_opus_hybrid_48k_mono_fec_ogg_bytes,
    .probe_opus_hybrid_48k_mono_ogg_bytes = probe_opus_hybrid_48k_mono_ogg_bytes,
    .probe_opus_hybrid_48k_stereo_10ms_ogg_bytes = probe_opus_hybrid_48k_stereo_10ms_ogg_bytes,
    .probe_opus_hybrid_48k_stereo_fec_10ms_ogg_bytes = probe_opus_hybrid_48k_stereo_fec_10ms_ogg_bytes,
    .probe_opus_hybrid_48k_stereo_fec_ogg_bytes = probe_opus_hybrid_48k_stereo_fec_ogg_bytes,
    .probe_opus_hybrid_48k_stereo_ogg_bytes = probe_opus_hybrid_48k_stereo_ogg_bytes,
    .tone_flac_bytes = tone_flac_bytes,
    .tone_flac_24bit_bytes = tone_flac_24bit_bytes,
    .tone_flac_ogg_bytes = tone_flac_ogg_bytes,
});

const checked_in_decode_but_not_claimed_cases: [0]DecodeButNotClaimedCase = .{};

const checked_in_unsupported_codec_cases: [1]UnsupportedCodecCase = conformance.buildCheckedInUnsupportedCodecCases(EncodedFormat, DecodeOptions, .{
    .unknown_audio_bytes = unknown_audio_bytes[0..],
});

const checked_in_mp4_demux_cases: [12]Mp4DemuxCase = conformance.buildCheckedInMp4DemuxCases(mp4.Codec, .{
    .tone_m4a_bytes = tone_m4a_bytes,
    .tone_m4a_44k_mono_bytes = tone_m4a_44k_mono_bytes,
    .transient_m4a_44k_short_bytes = transient_m4a_44k_short_bytes,
    .transient_stereo_m4a_44k_short_bytes = transient_stereo_m4a_44k_short_bytes,
    .tone_mp4_bytes = tone_mp4_bytes,
    .tone_mp4_44k_mono_bytes = tone_mp4_44k_mono_bytes,
    .transient_mp4_44k_short_bytes = transient_mp4_44k_short_bytes,
    .transient_stereo_mp4_44k_short_bytes = transient_stereo_mp4_44k_short_bytes,
    .tone_alac_m4a_bytes = tone_alac_m4a_bytes,
    .tone_alac_24bit_m4a_bytes = tone_alac_24bit_m4a_bytes,
    .tone_alac_mp4_bytes = tone_alac_mp4_bytes,
    .tone_alac_24bit_mp4_bytes = tone_alac_24bit_mp4_bytes,
});

const checked_in_caf_demux_cases: [2]CafDemuxCase = conformance.buildCheckedInCafDemuxCases(caf.Codec, .{
    .tone_caf_bytes = tone_caf_bytes,
    .tone_caf_24bit_bytes = tone_caf_24bit_bytes,
});

const checked_in_real_aac_tool_corpus_cases = [_]AacToolCorpusCase{
    .{
        .name = "transient-mono-44k-pns.aac",
        .bytes = transient_aac_44k_pns_bytes,
        .expected_sample_rate = 44100,
        .expected_channels = 1,
    },
    .{
        .name = "noise-mono-44k-tns-gain.aac",
        .bytes = noise_aac_44k_tns_gain_bytes,
        .expected_sample_rate = 44100,
        .expected_channels = 1,
    },
    .{
        .name = "noise-stereo-44k-tns.aac",
        .bytes = noise_stereo_aac_44k_tns_bytes,
        .expected_sample_rate = 44100,
        .expected_channels = 2,
    },
    .{
        .name = "transient-mono-44k-short.aac",
        .bytes = transient_aac_44k_short_bytes,
        .expected_sample_rate = 44100,
        .expected_channels = 1,
    },
    .{
        .name = "transient-stereo-44k-short.aac",
        .bytes = transient_stereo_aac_44k_short_bytes,
        .expected_sample_rate = 44100,
        .expected_channels = 2,
    },
};

const non_mp3_lane_summaries = conformance.buildNonMp3LaneSummaries(
    checked_in_additional_codec_cases.len,
    checked_in_decode_but_not_claimed_cases.len,
    checked_in_unsupported_codec_cases.len,
);

pub const FftPlan = struct {
    n: usize,
    n_freq: usize,
    m: usize,
    use_bluestein: bool,
    chirp: []Complex,
    kernel_fft: []Complex,
    work: []Complex,

    pub fn init(allocator: std.mem.Allocator, n: usize) !FftPlan {
        if (std.math.isPowerOfTwo(n)) {
            const work = try allocator.alloc(Complex, n);
            errdefer allocator.free(work);
            return .{
                .n = n,
                .n_freq = n / 2 + 1,
                .m = n,
                .use_bluestein = false,
                .chirp = &.{},
                .kernel_fft = &.{},
                .work = work,
            };
        }

        const m = nextPowerOfTwo(2 * n - 1);
        const chirp = try allocator.alloc(Complex, n);
        errdefer allocator.free(chirp);
        const kernel_fft = try allocator.alloc(Complex, m);
        errdefer allocator.free(kernel_fft);
        const work = try allocator.alloc(Complex, m);
        errdefer allocator.free(work);

        @memset(kernel_fft, .{ .re = 0, .im = 0 });
        for (0..n) |i| {
            const angle = std.math.pi * @as(f64, @floatFromInt((i * i) % (2 * n))) / @as(f64, @floatFromInt(n));
            const cosine = @cos(angle);
            const sine = @sin(angle);
            chirp[i] = .{ .re = cosine, .im = -sine };

            kernel_fft[i] = .{ .re = cosine, .im = sine };
            if (i != 0) {
                kernel_fft[m - i] = .{ .re = cosine, .im = sine };
            }
        }

        try fftComplex(kernel_fft, false);
        return .{
            .n = n,
            .n_freq = n / 2 + 1,
            .m = m,
            .use_bluestein = true,
            .chirp = chirp,
            .kernel_fft = kernel_fft,
            .work = work,
        };
    }

    pub fn deinit(self: *FftPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.chirp);
        allocator.free(self.kernel_fft);
        allocator.free(self.work);
    }

    pub fn powerSpectrumWindowed(
        self: *FftPlan,
        frame: []const f32,
        window: []const f32,
        out: []f32,
    ) !void {
        std.debug.assert(frame.len == self.n);
        std.debug.assert(window.len == self.n);
        std.debug.assert(out.len >= self.n_freq);

        @memset(self.work, .{ .re = 0, .im = 0 });
        if (!self.use_bluestein) {
            for (0..self.n) |i| {
                self.work[i] = .{ .re = @as(f64, @floatCast(frame[i] * window[i])), .im = 0 };
            }
            try fftComplex(self.work, false);
            for (0..self.n_freq) |k| {
                const spectrum = self.work[k];
                out[k] = @floatCast(spectrum.re * spectrum.re + spectrum.im * spectrum.im);
            }
            return;
        }

        for (0..self.n) |i| {
            const value: f64 = @floatCast(frame[i] * window[i]);
            self.work[i] = .{
                .re = value * self.chirp[i].re,
                .im = value * self.chirp[i].im,
            };
        }

        try fftComplex(self.work, false);
        for (0..self.m) |i| {
            self.work[i] = complexMul(self.work[i], self.kernel_fft[i]);
        }
        try fftComplex(self.work, true);

        for (0..self.n_freq) |k| {
            const spectrum = complexMul(self.work[k], self.chirp[k]);
            out[k] = @floatCast(spectrum.re * spectrum.re + spectrum.im * spectrum.im);
        }
    }
};

fn fftComplex(values: []Complex, inverse: bool) !void {
    if (values.len == 0) return;
    if (!std.math.isPowerOfTwo(values.len)) return error.InvalidDimensions;

    var j: usize = 0;
    for (1..values.len) |i| {
        var bit = values.len >> 1;
        while (j & bit != 0) : (bit >>= 1) {
            j ^= bit;
        }
        j ^= bit;
        if (i < j) std.mem.swap(Complex, &values[i], &values[j]);
    }

    var len: usize = 2;
    while (len <= values.len) : (len <<= 1) {
        const half = len >> 1;
        const sign: f64 = if (inverse) 1.0 else -1.0;
        const angle_step = sign * (2.0 * std.math.pi / @as(f64, @floatFromInt(len)));
        const wlen = Complex{ .re = @cos(angle_step), .im = @sin(angle_step) };

        var start: usize = 0;
        while (start < values.len) : (start += len) {
            var w = Complex{ .re = 1.0, .im = 0.0 };
            for (0..half) |i| {
                const u = values[start + i];
                const v = complexMul(values[start + i + half], w);
                values[start + i] = complexAdd(u, v);
                values[start + i + half] = complexSub(u, v);
                w = complexMul(w, wlen);
            }
        }
    }

    if (inverse) {
        const scale = @as(f64, @floatFromInt(values.len));
        for (values) |*value| {
            value.re /= scale;
            value.im /= scale;
        }
    }
}

fn complexAdd(a: Complex, b: Complex) Complex {
    return .{ .re = a.re + b.re, .im = a.im + b.im };
}

fn complexSub(a: Complex, b: Complex) Complex {
    return .{ .re = a.re - b.re, .im = a.im - b.im };
}

fn complexMul(a: Complex, b: Complex) Complex {
    return .{
        .re = a.re * b.re - a.im * b.im,
        .im = a.re * b.im + a.im * b.re,
    };
}

fn nextPowerOfTwo(value: usize) usize {
    var out: usize = 1;
    while (out < value) : (out <<= 1) {}
    return out;
}

fn naivePowerSpectrumWindowed(
    frame: []const f32,
    window: []const f32,
    out: []f32,
) void {
    const n_fft = frame.len;
    const n_freq = n_fft / 2 + 1;
    std.debug.assert(window.len == n_fft);
    std.debug.assert(out.len >= n_freq);

    for (0..n_freq) |k| {
        var re: f32 = 0;
        var im: f32 = 0;
        const k_f = @as(f32, @floatFromInt(k));
        for (0..n_fft) |n| {
            const val = frame[n] * window[n];
            const angle = -2.0 * std.math.pi * k_f * @as(f32, @floatFromInt(n)) / @as(f32, @floatFromInt(n_fft));
            re += val * @cos(angle);
            im += val * @sin(angle);
        }
        out[k] = re * re + im * im;
    }
}

fn dotProductSimd(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);

    var acc: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= a.len) : (i += VEC_LEN) {
        const av: F32xN = a[i..][0..VEC_LEN].*;
        const bv: F32xN = b[i..][0..VEC_LEN].*;
        acc += av * bv;
    }

    var sum = @reduce(.Add, acc);
    while (i < a.len) : (i += 1) {
        sum += a[i] * b[i];
    }
    return sum;
}

fn maxValueSimd(values: []const f32) f32 {
    std.debug.assert(values.len != 0);

    if (values.len < VEC_LEN) {
        var scalar_max = values[0];
        for (values[1..]) |value| {
            if (value > scalar_max) scalar_max = value;
        }
        return scalar_max;
    }

    var i: usize = 0;
    var acc: F32xN = values[0..VEC_LEN].*;
    i = VEC_LEN;
    while (i + VEC_LEN <= values.len) : (i += VEC_LEN) {
        const vv: F32xN = values[i..][0..VEC_LEN].*;
        acc = @max(acc, vv);
    }

    var max_val = @reduce(.Max, acc);
    while (i < values.len) : (i += 1) {
        if (values[i] > max_val) max_val = values[i];
    }
    return max_val;
}

fn clampScaleWhisper(values: []f32, max_val: f32) void {
    const floor_val = max_val - 8.0;
    const offset_val = max_val - 4.0;
    const floor_vec: F32xN = @splat(floor_val);
    const offset_vec: F32xN = @splat(offset_val);
    const scale_vec: F32xN = @splat(0.25);

    var i: usize = 0;
    while (i + VEC_LEN <= values.len) : (i += VEC_LEN) {
        var vv: F32xN = values[i..][0..VEC_LEN].*;
        vv = @max(vv, floor_vec);
        vv = (vv - offset_vec) * scale_vec;
        values[i..][0..VEC_LEN].* = vv;
    }

    while (i < values.len) : (i += 1) {
        values[i] = @max(values[i], floor_val);
        values[i] = (values[i] - offset_val) * 0.25;
    }
}

fn downmixStereoToMono(allocator: std.mem.Allocator, samples: []const f32) ![]f32 {
    if ((samples.len & 1) != 0) return error.UnsupportedAudioFormat;

    const frame_count = samples.len / 2;
    const mono = try allocator.alloc(f32, frame_count);
    errdefer allocator.free(mono);

    const half_vec: F32xN = @splat(0.5);
    const even_mask = comptime buildStereoShuffleMask(0);
    const odd_mask = comptime buildStereoShuffleMask(1);

    var src: usize = 0;
    var dst: usize = 0;
    while (src + VEC_LEN * 2 <= samples.len) : ({
        src += VEC_LEN * 2;
        dst += VEC_LEN;
    }) {
        const lo: F32xN = samples[src..][0..VEC_LEN].*;
        const hi: F32xN = samples[src + VEC_LEN ..][0..VEC_LEN].*;
        const left: F32xN = @shuffle(f32, lo, hi, even_mask);
        const right: F32xN = @shuffle(f32, lo, hi, odd_mask);
        mono[dst..][0..VEC_LEN].* = (left + right) * half_vec;
    }

    while (src < samples.len) : ({
        src += 2;
        dst += 1;
    }) {
        mono[dst] = (samples[src] + samples[src + 1]) * 0.5;
    }

    return mono;
}

fn buildStereoShuffleMask(comptime offset: comptime_int) @Vector(VEC_LEN, i32) {
    var mask: [VEC_LEN]i32 = undefined;
    inline for (0..VEC_LEN) |i| {
        const raw_index = i * 2 + offset;
        mask[i] = if (raw_index < VEC_LEN)
            @intCast(raw_index)
        else
            -@as(i32, @intCast(raw_index - VEC_LEN + 1));
    }
    return mask;
}

test "resample identity" {
    const samples = [_]f32{ 0.0, 0.5, 1.0, 0.5 };
    const out = try resample(std.testing.allocator, &samples, 16000, 16000);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 4), out.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), out[1], 1e-6);
}

test "copy or resample copies on matching rate" {
    const samples = [_]f32{ 0.25, -0.25 };
    const out = try copyOrResample(std.testing.allocator, &samples, 16000, 16000);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualSlices(f32, &samples, out);
}

test "detect format recognizes wav and mp3 signatures" {
    try std.testing.expectEqual(EncodedFormat.wav, detectFormat("RIFFxxxxWAVE").?);
    try std.testing.expectEqual(EncodedFormat.wav, detectFormat("RIFXxxxxWAVE").?);
    try std.testing.expectEqual(EncodedFormat.wav, detectFormat("RF64xxxxWAVE").?);
    try std.testing.expectEqual(EncodedFormat.wav, detectFormat("BW64xxxxWAVE").?);
    try std.testing.expectEqual(EncodedFormat.au, detectFormat(synthetic_au_pcm16_bytes[0..]).?);
    try std.testing.expectEqual(EncodedFormat.mp3, detectFormat("ID3abc").?);
    try std.testing.expectEqual(EncodedFormat.mp3, detectFormat(&.{ 0xFF, 0xFB, 0x90, 0x64 }).?);
}

test "detect format recognizes aac ogg opus and flac signatures" {
    const compatible_qt_ftyp = [_]u8{
        0x00, 0x00, 0x00, 0x28, // ftyp size
        'f',  't',  'y',  'p',
        'i',  's',  'o',  'm',
        0x00, 0x00, 0x00, 0x00, // minor version
        'x',  'x',  'x',  'x',
        'x',  'x',  'x',  'x',
        'x',  'x',  'x',  'x',
        'x',  'x',  'x',  'x',
        'q',  't',  ' ',  ' ',
    };

    try std.testing.expectEqual(EncodedFormat.aac, detectFormat(tone_aac_bytes).?);
    try std.testing.expectEqual(EncodedFormat.aac, detectFormat(tone_aac_44k_mono_bytes).?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormat(tone_m4a_bytes).?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormat(tone_mp4_bytes).?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormat(&compatible_qt_ftyp).?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormat(tone_alac_m4a_bytes).?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormat(tone_alac_mp4_bytes).?);
    try std.testing.expectEqual(EncodedFormat.aiff, detectFormat(tone_aiff_bytes).?);
    try std.testing.expectEqual(EncodedFormat.caf, detectFormat(tone_caf_bytes).?);
    try std.testing.expectEqual(EncodedFormat.ogg, detectFormat(tone_ogg_bytes).?);
    try std.testing.expectEqual(EncodedFormat.ogg, detectFormat(tone_oga_bytes).?);
    try std.testing.expectEqual(EncodedFormat.opus, detectFormat(tone_opus_bytes).?);
    try std.testing.expectEqual(EncodedFormat.opus, detectFormat(tone_opus_48k_mono_bytes).?);
    try std.testing.expectEqual(EncodedFormat.opus, detectFormat(tone_opus_ogg_bytes).?);
    try std.testing.expectEqual(EncodedFormat.flac, detectFormat(tone_flac_bytes).?);
    try std.testing.expectEqual(EncodedFormat.flac, detectFormat(tone_flac_24bit_bytes).?);
    try std.testing.expectEqual(EncodedFormat.ogg, detectFormat(tone_flac_ogg_bytes).?);
}

test "detect format from mime covers common audio media types" {
    try std.testing.expectEqual(EncodedFormat.wav, detectFormatFromMime("audio/wav").?);
    try std.testing.expectEqual(EncodedFormat.mp3, detectFormatFromMime("audio/mpeg").?);
    try std.testing.expectEqual(EncodedFormat.aac, detectFormatFromMime("audio/aac").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromMime("audio/mp4; codecs=mp4a.40.2").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromMime("application/mp4").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromMime("audio/quicktime").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromMime("video/quicktime").?);
    try std.testing.expectEqual(EncodedFormat.aiff, detectFormatFromMime("audio/aiff").?);
    try std.testing.expectEqual(EncodedFormat.caf, detectFormatFromMime("audio/x-caf").?);
    try std.testing.expectEqual(EncodedFormat.au, detectFormatFromMime("audio/basic").?);
    try std.testing.expectEqual(EncodedFormat.au, detectFormatFromMime("audio/x-snd").?);
    try std.testing.expectEqual(EncodedFormat.ogg, detectFormatFromMime("audio/ogg").?);
    try std.testing.expectEqual(EncodedFormat.opus, detectFormatFromMime("audio/ogg; codecs=opus").?);
    try std.testing.expectEqual(EncodedFormat.flac, detectFormatFromMime("audio/flac").?);
}

test "compatibility helpers follow supported formats for mime and filename" {
    try std.testing.expect(canDecodeMime("audio/wav"));
    try std.testing.expect(canDecodeMime("audio/basic"));
    try std.testing.expect(canDecodeMime("audio/mpeg") == mp3.enabled());
    try std.testing.expect(canDecodeMime("audio/aac"));
    try std.testing.expect(canDecodeMime("audio/mp4"));
    try std.testing.expect(canDecodeMime("audio/aiff"));
    try std.testing.expect(canDecodeMime("audio/caf"));
    try std.testing.expect(canDecodeMime("audio/flac"));
    try std.testing.expect(!canDecodeMime("audio/unknown"));

    try std.testing.expect(canDecodeFilename("clip.wav"));
    try std.testing.expect(canDecodeFilename("clip.au"));
    try std.testing.expect(canDecodeFilename("clip.snd"));
    try std.testing.expect(canDecodeFilename("clip.mp3") == mp3.enabled());
    try std.testing.expect(canDecodeFilename("clip.aac"));
    try std.testing.expect(canDecodeFilename("clip.m4a"));
    try std.testing.expect(canDecodeFilename("clip.m4b"));
    try std.testing.expect(canDecodeFilename("clip.mov"));
    try std.testing.expect(canDecodeFilename("clip.aiff"));
    try std.testing.expect(canDecodeFilename("clip.caf"));
    try std.testing.expect(canDecodeFilename("clip.flac"));
    try std.testing.expect(!canDecodeFilename("clip.bin"));
}

test "canDecodeWithOptions tracks direct and hinted compatibility" {
    try std.testing.expect(canDecodeWithOptions(tone_wav_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_aac_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(transient_aac_44k_short_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(transient_stereo_aac_44k_short_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_m4a_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_m4a_44k_mono_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(transient_mp4_44k_short_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(transient_stereo_m4a_44k_short_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(transient_stereo_mp4_44k_short_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_mp4_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_sbr_m4a_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_alac_m4a_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_alac_mp4_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_mp4_44k_mono_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_aiff_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_caf_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_flac_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_flac_24bit_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(tone_flac_ogg_bytes, .{}));
    try std.testing.expect(canDecodeWithOptions(synthetic_au_pcm16_bytes[0..], .{}));
    try std.testing.expect(canDecodeWithOptions(synthetic_caf_lpcm_pcm16_bytes[0..], .{}));
    try std.testing.expect(canDecodeWithOptions(unknown_audio_bytes[0..], .{ .format_hint = .flac }));
}

test "detect format from filename covers common audio extensions" {
    try std.testing.expectEqual(EncodedFormat.wav, detectFormatFromFilename("clip.wav").?);
    try std.testing.expectEqual(EncodedFormat.mp3, detectFormatFromFilename("/tmp/clip.MP3").?);
    try std.testing.expectEqual(EncodedFormat.aac, detectFormatFromFilename("clip.aac").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromFilename("nested/clip.m4a").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromFilename("nested/clip.m4b").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromFilename("nested/clip.mp4").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromFilename("nested/clip.mov").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromFilename("nested/clip.qt").?);
    try std.testing.expectEqual(EncodedFormat.aiff, detectFormatFromFilename("clip.aifc").?);
    try std.testing.expectEqual(EncodedFormat.caf, detectFormatFromFilename("clip.caf").?);
    try std.testing.expectEqual(EncodedFormat.au, detectFormatFromFilename("clip.au").?);
    try std.testing.expectEqual(EncodedFormat.au, detectFormatFromFilename("clip.snd").?);
    try std.testing.expectEqual(EncodedFormat.ogg, detectFormatFromFilename("clip.oga").?);
    try std.testing.expectEqual(EncodedFormat.opus, detectFormatFromFilename("clip.opus").?);
    try std.testing.expectEqual(EncodedFormat.flac, detectFormatFromFilename("clip.flac").?);
}

test "file name hint overrides ambiguous sync-word sniffing for flac" {
    const ambiguous_sync = [_]u8{ 0xff, 0xf8, 0x00, 0x00 };
    try std.testing.expectEqual(EncodedFormat.mp3, detectFormat(ambiguous_sync[0..]).?);
    try std.testing.expectEqual(EncodedFormat.flac, detectFormatWithOptions(ambiguous_sync[0..], .{
        .file_name_hint = "raw-frame.flac",
    }).?);
}

test "decode dispatch handles wav fixture" {
    try std.testing.expect(canDecodeFormat(.wav));
    try std.testing.expect(canDecodeFormat(.aac));
    try std.testing.expect(canDecodeFormat(.mp4));
    try std.testing.expect(canDecodeFormat(.aiff));
    try std.testing.expect(canDecodeFormat(.caf));
    try std.testing.expect(canDecodeFormat(.au));
    try std.testing.expectEqual(EncodedFormat.wav, detectFormat(tone_wav_bytes).?);

    var decoded = try decode(std.testing.allocator, tone_wav_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 16000), decoded.samples.len);
}

test "decodeInterleaved handles wav stereo round-trip" {
    const wav_bytes = try wav.encodeInterleaved(std.testing.allocator, &.{ 1.0, -1.0, 0.5, -0.5 }, 2, .{
        .audio_format = 1,
        .sample_rate = 8000,
        .bits_per_sample = 16,
    });
    defer std.testing.allocator.free(wav_bytes);

    var decoded = try decodeInterleaved(std.testing.allocator, wav_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 8000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 4), decoded.samples.len);
}

test "decodeInterleaved handles wav common pcm formats" {
    const formats = [_]wav.Format{
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 8 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 16 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 24 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 32 },
        .{ .audio_format = 1, .sample_rate = 8000, .bits_per_sample = 64 },
        .{ .audio_format = 3, .sample_rate = 8000, .bits_per_sample = 32 },
        .{ .audio_format = 3, .sample_rate = 8000, .bits_per_sample = 64 },
    };

    for (formats) |format| {
        const wav_bytes = try wav.encodeInterleaved(std.testing.allocator, &.{ 1.0, -1.0, 0.5, -0.5 }, 2, format);
        defer std.testing.allocator.free(wav_bytes);

        var decoded = try decodeInterleaved(std.testing.allocator, wav_bytes, .{});
        defer decoded.deinit();

        try std.testing.expectEqual(@as(u8, 2), decoded.channels);
        try std.testing.expectEqual(@as(usize, 4), decoded.samples.len);
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded.samples[0], 0.01);
        try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[1], 0.01);
        try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[2], 0.01);
        try std.testing.expectApproxEqAbs(@as(f32, -0.5), decoded.samples[3], 0.01);
    }
}

test "decodeInterleaved handles checked-in aiff pcm16 fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_aiff_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 16000 * 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.015197754), decoded.samples[2], 1e-5);
}

test "decodeInterleaved handles checked-in flac fixtures" {
    inline for ([_]struct { bytes: []const u8, bits_per_sample: u8 }{
        .{ .bytes = tone_flac_bytes, .bits_per_sample = 16 },
        .{ .bytes = tone_flac_24bit_bytes, .bits_per_sample = 24 },
    }) |case| {
        var decoded = try decodeInterleaved(std.testing.allocator, case.bytes, .{});
        defer decoded.deinit();

        try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
        try std.testing.expectEqual(@as(u8, 2), decoded.channels);
        try std.testing.expectEqual(@as(usize, 16000 * 2), decoded.samples.len);
        try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded.samples[0], 2e-4);
        try std.testing.expectApproxEqAbs(@as(f32, 0.015197754), decoded.samples[2], if (case.bits_per_sample == 24) 2e-4 else 1e-5);
    }
}

test "decodeInterleaved handles checked-in ogg flac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_flac_ogg_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 16000 * 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), decoded.samples[0], 2e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.015197754), decoded.samples[2], 1e-5);
}

test "decodeInterleaved handles synthetic au pcm16 fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, synthetic_au_pcm16_bytes[0..], .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[1], 1e-6);
}

test "decodeInterleaved handles synthetic caf lpcm pcm16 fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, synthetic_caf_lpcm_pcm16_bytes[0..], .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 2), decoded.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, -1.0), decoded.samples[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), decoded.samples[1], 1e-6);
}

test "decodeInterleaved normalizes finite PCM range at public boundary" {
    const wav_bytes = try wav.encodeInterleaved(std.testing.allocator, &.{ std.math.inf(f32), -2.0 }, 1, .{
        .audio_format = 3,
        .sample_rate = 8000,
        .bits_per_sample = 32,
    });
    defer std.testing.allocator.free(wav_bytes);

    var decoded = try decodeInterleaved(std.testing.allocator, wav_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(f32, 0.0), decoded.samples[0]);
    try std.testing.expectEqual(@as(f32, -1.0), decoded.samples[1]);
}

test "mp4 edit-list trim helper removes priming and remainder frames" {
    const samples = try std.testing.allocator.alloc(f32, 10);
    for (samples, 0..) |*sample, i| sample.* = @floatFromInt(i);

    const trimmed = try trimOwnedInterleavedForMp4EditList(std.testing.allocator, samples, 2, 1, 3);
    defer std.testing.allocator.free(trimmed);

    try std.testing.expectEqual(@as(usize, 6), trimmed.len);
    try std.testing.expectEqual(@as(f32, 2), trimmed[0]);
    try std.testing.expectEqual(@as(f32, 7), trimmed[5]);
}

test "decode dispatch handles mp3 fixture" {
    if (!mp3.enabled()) return error.SkipZigTest;

    try std.testing.expect(canDecodeFormat(.mp3));
    try std.testing.expectEqual(EncodedFormat.mp3, detectFormat(tone_mp3_bytes).?);

    var decoded = try decode(std.testing.allocator, tone_mp3_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expect(decoded.samples.len >= 15200);
}

test "decodeInterleaved handles mp3 stereo fixture" {
    if (!mp3.enabled()) return error.SkipZigTest;

    var decoded = try decodeInterleaved(std.testing.allocator, @embedFile("../testdata/mp3-corpus/l3-he_mode.bit"), .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len >= 147456 * 2);
}

test "checked-in codec corpus decodes and stays close to wav reference" {
    var reference = try decode(std.testing.allocator, tone_wav_bytes, .{});
    defer reference.deinit();

    for (checked_in_additional_codec_cases) |case| {
        try std.testing.expect(canDecodeFormat(case.format));
        try std.testing.expectEqual(case.format, detectFormat(case.bytes).?);

        var interleaved = try decodeInterleaved(std.testing.allocator, case.bytes, .{});
        defer interleaved.deinit();

        try conformance.assertInterleavedShape(
            case.expected_sample_rate,
            case.expected_channels,
            interleaved.samples,
            interleaved.sample_rate,
            interleaved.channels,
        );
        try conformance.assertStereoCoherenceIfExpected(case.expected_channels, interleaved.samples);

        var mono = try decode(std.testing.allocator, case.bytes, .{});
        defer mono.deinit();

        try std.testing.expectEqual(case.expected_sample_rate, mono.sample_rate);
        try std.testing.expect(mono.samples.len >= case.expected_sample_rate);

        try conformance.assertReferenceCloseness(
            std.testing.allocator,
            reference.samples,
            reference.sample_rate,
            mono.samples,
            mono.sample_rate,
            case.min_compared,
            case.min_correlation,
            case.max_mean_abs_error,
            copyOrResample,
            resample,
        );
    }
}

test "checked-in mono opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, tone_opus_48k_mono_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len > 40_000);
}

test "checked-in aac fixtures decode on the pure-zig no-fallback lane" {
    const cases = [_]struct {
        bytes: []const u8,
        sample_rate: u32,
        channels: u8,
    }{
        .{ .bytes = tone_aac_bytes, .sample_rate = 16_000, .channels = 2 },
        .{ .bytes = tone_aac_44k_mono_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = transient_aac_44k_pns_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = noise_aac_44k_tns_gain_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = noise_stereo_aac_44k_tns_bytes, .sample_rate = 44_100, .channels = 2 },
        .{ .bytes = transient_aac_44k_short_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = transient_stereo_aac_44k_short_bytes, .sample_rate = 44_100, .channels = 2 },
    };
    for (cases) |case| {
        var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, case.bytes, .{});
        defer zig.deinit();

        try std.testing.expectEqual(case.sample_rate, zig.sample_rate);
        try std.testing.expectEqual(case.channels, zig.channels);
        try std.testing.expect(zig.samples.len > 0);
    }
}

test "checked-in mp4 aac fixtures decode on the pure-zig no-fallback lane" {
    const cases = [_]struct {
        bytes: []const u8,
        sample_rate: u32,
        channels: u8,
    }{
        .{ .bytes = tone_m4a_bytes, .sample_rate = 16_000, .channels = 2 },
        .{ .bytes = tone_sbr_m4a_bytes, .sample_rate = 16_000, .channels = 2 },
        .{ .bytes = tone_m4a_44k_mono_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = tone_mp4_44k_mono_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = transient_m4a_44k_short_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = transient_mp4_44k_short_bytes, .sample_rate = 44_100, .channels = 1 },
        .{ .bytes = transient_stereo_m4a_44k_short_bytes, .sample_rate = 44_100, .channels = 2 },
        .{ .bytes = transient_stereo_mp4_44k_short_bytes, .sample_rate = 44_100, .channels = 2 },
    };
    for (cases) |case| {
        var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, case.bytes, .{});
        defer zig.deinit();

        try std.testing.expectEqual(case.sample_rate, zig.sample_rate);
        try std.testing.expectEqual(case.channels, zig.channels);
        try std.testing.expect(zig.samples.len > 0);
    }
}

test "checked-in aiff fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, tone_aiff_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 16_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len > 0);
}

test "checked-in caf fixtures decode on the pure-zig no-fallback lane" {
    inline for ([_][]const u8{ tone_caf_bytes, tone_caf_24bit_bytes, synthetic_caf_lpcm_pcm16_bytes[0..] }) |fixture| {
        var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, fixture, .{});
        defer zig.deinit();

        try std.testing.expectEqual(@as(u32, 16_000), zig.sample_rate);
        try std.testing.expectEqual(@as(u8, 2), zig.channels);
        try std.testing.expect(zig.samples.len > 0);
    }
}

test "checked-in ogg flac fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, tone_flac_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 16_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expectEqual(@as(usize, 16_000 * 2), zig.samples.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), zig.samples[0], 2e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0.015197754), zig.samples[2], 1e-5);
}

test "checked-in vorbis fixtures decode on the pure-zig no-fallback lane" {
    inline for ([_][]const u8{ tone_ogg_bytes, tone_oga_bytes }) |fixture| {
        var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, fixture, .{});
        defer zig.deinit();

        try std.testing.expectEqual(@as(u32, 16_000), zig.sample_rate);
        try std.testing.expectEqual(@as(u8, 2), zig.channels);
        try std.testing.expectEqual(@as(usize, 16_000 * 2), zig.samples.len);
        try std.testing.expectApproxEqAbs(@as(f32, 0.0), zig.samples[0], 2e-4);
        try std.testing.expectApproxEqAbs(@as(f32, 0.015197754), zig.samples[2], 5e-4);
    }
}

test "checked-in stereo opus fixtures decode on the pure-zig no-fallback lane" {
    inline for ([_][]const u8{ tone_opus_bytes, tone_opus_ogg_bytes }) |fixture| {
        var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, fixture, .{});
        defer zig.deinit();

        try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
        try std.testing.expectEqual(@as(u8, 2), zig.channels);
        try std.testing.expect(zig.samples.len > 80_000);
    }
}

test "real mono celt 5ms opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_celt_48k_mono_5ms_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len > 40_000);
}

test "real mono celt 120ms opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_celt_48k_mono_120ms_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len > 40_000);
}

test "real stereo celt 2.5ms opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_celt_48k_stereo_2p5ms_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len > 80_000);
}

test "real stereo celt 60ms opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_celt_48k_stereo_60ms_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len > 80_000);
}

test "real stereo celt 40ms opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_celt_48k_stereo_40ms_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len > 80_000);
}

test "real mono silk ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_mono_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 12_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono silk fec ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_mono_fec_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono silk fec 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_mono_fec_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono silk fec 40ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_mono_fec_40ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono silk fec 60ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_mono_fec_60ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono silk 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_mono_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 12_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono silk 60ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_mono_60ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 12_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo silk fec ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_stereo_fec_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo silk fec 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_stereo_fec_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo silk fec 40ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_stereo_fec_40ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo silk fec 60ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_stereo_fec_60ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo silk 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_stereo_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo silk ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_stereo_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo silk 40ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_silk_16k_stereo_40ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono hybrid ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_mono_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono hybrid 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_mono_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono hybrid fec ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_mono_fec_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real mono hybrid fec 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_mono_fec_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo hybrid ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_stereo_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo hybrid fec ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_stereo_fec_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo hybrid fec 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_stereo_fec_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "real stereo hybrid 10ms ogg opus fixture decodes on the pure-zig no-fallback lane" {
    var zig = try decodeInterleavedWithoutFallback(std.testing.allocator, probe_opus_hybrid_48k_stereo_10ms_ogg_bytes, .{});
    defer zig.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), zig.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), zig.channels);
    try std.testing.expect(zig.samples.len >= 24_000);
    for (zig.samples[0..@min(zig.samples.len, 512)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "checked-in mp4 demux corpus exposes expected codec metadata" {
    for (checked_in_mp4_demux_cases) |case| {
        try conformance.assertDemuxCase(case, mp4.demux);
    }
}

test "checked-in caf demux corpus exposes expected codec metadata" {
    for (checked_in_caf_demux_cases) |case| {
        try conformance.assertDemuxCase(case, caf.demux);
    }
}

test "checked-in mp4 alac demux path stays aligned with pure-zig direct decode" {
    for (checked_in_mp4_demux_cases) |case| {
        if (case.expected_codec != .alac) continue;

        var direct = try decodeInterleaved(std.testing.allocator, case.bytes, .{});
        defer direct.deinit();

        var demuxed = try mp4.demux(std.testing.allocator, case.bytes);
        defer demuxed.deinit();

        var packetized = try alac.decodeInterleavedPacketizedAlloc(
            std.testing.allocator,
            demuxed.sample_rate,
            @intCast(demuxed.channels),
            demuxed.access_units,
            demuxed.decoder_config,
        );
        defer packetized.deinit();

        try std.testing.expectEqual(direct.sample_rate, packetized.sample_rate);
        try std.testing.expectEqual(direct.channels, packetized.channels);
        try std.testing.expectEqual(direct.samples.len, packetized.samples.len);
        for (direct.samples, packetized.samples) |expected, actual| {
            try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
        }
    }
}

test "checked-in caf alac demux path stays aligned with pure-zig direct decode" {
    for (checked_in_caf_demux_cases) |case| {
        var direct = try decodeInterleaved(std.testing.allocator, case.bytes, .{});
        defer direct.deinit();

        var demuxed = try caf.demux(std.testing.allocator, case.bytes);
        defer demuxed.deinit();

        var packetized = try alac.decodeInterleavedPacketizedAlloc(
            std.testing.allocator,
            demuxed.sample_rate,
            @intCast(demuxed.channels),
            demuxed.access_units,
            demuxed.decoder_config,
        );
        defer packetized.deinit();

        try std.testing.expectEqual(direct.sample_rate, packetized.sample_rate);
        try std.testing.expectEqual(direct.channels, packetized.channels);
        try std.testing.expectEqual(direct.samples.len, packetized.samples.len);
        for (direct.samples, packetized.samples) |expected, actual| {
            try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
        }
    }
}

test "checked-in aac fixtures expose the first real channel element after fill" {
    const stereo_frame = try aac.parseAdtsFrame(tone_aac_bytes);
    const stereo_summary = try aac.summarizeAccessUnit(stereo_frame.payload);
    try std.testing.expectEqual(aac.ElementKind.cpe, stereo_summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), stereo_summary.element_instance_tag);
    try std.testing.expectEqual(@as(bool, true), stereo_summary.common_window.?);

    const stereo_prefix = try aac.parseFirstElementPrefix(stereo_frame.payload);
    switch (stereo_prefix) {
        .cpe => |cpe| {
            try std.testing.expectEqual(@as(u8, 0), cpe.element_instance_tag);
            try std.testing.expectEqual(@as(bool, true), cpe.common_window);
            try std.testing.expectEqual(@as(u8, 140), cpe.left_global_gain);
            try std.testing.expectEqual(@as(u2, 1), cpe.ms_present.?);
            try std.testing.expect(cpe.shared_ics_info != null);
            try std.testing.expectEqual(aac.WindowSequence.long_start, cpe.shared_ics_info.?.window_sequence);
            try std.testing.expectEqual(@as(u8, 43), cpe.shared_ics_info.?.max_sfb);
        },
        else => return error.TestUnexpectedResult,
    }

    var m4a_demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer m4a_demuxed.deinit();
    const m4a_summary = try aac.summarizeAccessUnit(m4a_demuxed.access_units[0]);
    try std.testing.expectEqual(aac.ElementKind.cpe, m4a_summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), m4a_summary.element_instance_tag);

    const m4a_prefix = try aac.parseFirstElementPrefix(m4a_demuxed.access_units[0]);
    switch (m4a_prefix) {
        .cpe => |cpe| {
            try std.testing.expectEqual(@as(u8, 0), cpe.element_instance_tag);
            try std.testing.expectEqual(@as(bool, true), cpe.common_window);
            try std.testing.expectEqual(@as(u8, 140), cpe.left_global_gain);
        },
        else => return error.TestUnexpectedResult,
    }

    const mono_frame = try aac.parseAdtsFrame(tone_aac_44k_mono_bytes);
    const mono_summary = try aac.summarizeAccessUnit(mono_frame.payload);
    try std.testing.expectEqual(aac.ElementKind.sce, mono_summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), mono_summary.element_instance_tag);
    try std.testing.expectEqual(@as(u8, 152), mono_summary.first_channel_global_gain.?);

    const mono_prefix = try aac.parseFirstElementPrefix(mono_frame.payload);
    switch (mono_prefix) {
        .sce => |sce| {
            try std.testing.expectEqual(@as(u8, 0), sce.element_instance_tag);
            try std.testing.expectEqual(@as(u8, 152), sce.global_gain);
            try std.testing.expectEqual(aac.WindowSequence.long_start, sce.ics_info.window_sequence);
            try std.testing.expectEqual(@as(u8, 47), sce.ics_info.max_sfb);
            try std.testing.expectEqual(@as(u8, 1), sce.ics_info.num_window_groups);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "checked-in m4a first-channel scalefactors stay aligned with adts fixture" {
    const stereo_frame = try aac.parseAdtsFrame(tone_aac_bytes);
    var adts_scalefactors = try aac.parseFirstChannelScalefactorsAlloc(std.testing.allocator, stereo_frame.payload);
    defer adts_scalefactors.deinit();

    var m4a_demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer m4a_demuxed.deinit();
    var m4a_scalefactors = try aac.parseFirstChannelScalefactorsAlloc(
        std.testing.allocator,
        m4a_demuxed.access_units[0],
    );
    defer m4a_scalefactors.deinit();

    try std.testing.expectEqual(adts_scalefactors.ics_info.max_sfb, m4a_scalefactors.ics_info.max_sfb);
    try std.testing.expectEqual(adts_scalefactors.sections.len, m4a_scalefactors.sections.len);
    try std.testing.expectEqual(adts_scalefactors.bands.len, m4a_scalefactors.bands.len);

    for (adts_scalefactors.sections, m4a_scalefactors.sections) |expected, actual| {
        try std.testing.expectEqual(expected.band_type, actual.band_type);
        try std.testing.expectEqual(expected.start_sfb, actual.start_sfb);
        try std.testing.expectEqual(expected.end_sfb, actual.end_sfb);
    }
    for (adts_scalefactors.bands, m4a_scalefactors.bands) |expected, actual| {
        try std.testing.expectEqual(expected.band_type, actual.band_type);
        try std.testing.expectEqual(expected.kind, actual.kind);
        try std.testing.expectEqual(expected.value, actual.value);
    }
}

test "decode dispatch handles checked-in stereo aac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_aac_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 17 * 1024 * 2), decoded.samples.len);
}

test "decode dispatch handles checked-in mono aac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_aac_44k_mono_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in mono pns aac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, transient_aac_44k_pns_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in mono tns gain aac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, noise_aac_44k_tns_gain_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in stereo tns aac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, noise_stereo_aac_44k_tns_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in short-window mono aac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, transient_aac_44k_short_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in short-window stereo aac fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, transient_stereo_aac_44k_short_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in real aac tool corpus" {
    for (checked_in_real_aac_tool_corpus_cases) |case| {
        var decoded = try decodeInterleaved(std.testing.allocator, case.bytes, .{});
        defer decoded.deinit();

        try std.testing.expectEqual(case.expected_sample_rate, decoded.sample_rate);
        try std.testing.expectEqual(case.expected_channels, decoded.channels);
        try std.testing.expect(decoded.samples.len > 0);
    }
}

test "decode dispatch handles checked-in mono m4a fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_m4a_44k_mono_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in mono mp4 fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_mp4_44k_mono_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in short-window mono m4a fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, transient_m4a_44k_short_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in short-window mono mp4 fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, transient_mp4_44k_short_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in short-window stereo m4a fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, transient_stereo_m4a_44k_short_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in short-window stereo mp4 fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, transient_stereo_mp4_44k_short_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len > 0);
}

test "decode dispatch handles checked-in stereo m4a fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_m4a_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len >= 17 * 1024 * 2);
}

test "decode dispatch handles checked-in sbr-signaled m4a fixture" {
    var decoded = try decodeInterleaved(std.testing.allocator, tone_sbr_m4a_bytes, .{});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expect(decoded.samples.len >= 17 * 1024 * 2);
}

test "unsupported codec corpus remains explicitly unsupported" {
    for (checked_in_unsupported_codec_cases) |case| {
        try conformance.assertUnsupportedCase(case, detectFormat, decode);
    }
}

test "non-mp3 corpus lane summary remains coherent" {
    try std.testing.expectEqual(@as(usize, 3), non_mp3_lane_summaries.len);
    try std.testing.expectEqual(conformance.CorpusLane.passing, non_mp3_lane_summaries[0].lane);
    try std.testing.expectEqual(checked_in_additional_codec_cases.len, non_mp3_lane_summaries[0].case_count);
    try std.testing.expectEqual(conformance.CorpusLane.decode_but_not_claimed, non_mp3_lane_summaries[1].lane);
    try std.testing.expectEqual(checked_in_decode_but_not_claimed_cases.len, non_mp3_lane_summaries[1].case_count);
    try std.testing.expectEqual(conformance.CorpusLane.unsupported, non_mp3_lane_summaries[2].lane);
    try std.testing.expectEqual(checked_in_unsupported_codec_cases.len, non_mp3_lane_summaries[2].case_count);
}

test "checked-in codec corpus directory stays represented by the shared passing table" {
    var fixture_names = try collectCheckedInCodecCorpusFileNames(std.testing.allocator);
    defer freeStringSet(std.testing.allocator, &fixture_names);

    try std.testing.expectEqual(@as(usize, checked_in_additional_codec_cases.len), fixture_names.count());

    for (checked_in_additional_codec_cases) |case| {
        const removed = fixture_names.remove(case.name);
        try std.testing.expect(removed);
    }

    try std.testing.expectEqual(@as(usize, 0), fixture_names.count());
}

test "checked-in codec corpus fixtures stay documented in the corpus README" {
    var documented_names = try collectCheckedInCodecCorpusReadmeNames(std.testing.allocator);
    defer freeStringSet(std.testing.allocator, &documented_names);

    for (checked_in_additional_codec_cases) |case| {
        try std.testing.expect(documented_names.contains(case.name));
    }
}

test "checked-in codec corpus README entries stay aligned with files on disk" {
    var fixture_names = try collectCheckedInCodecCorpusFileNames(std.testing.allocator);
    defer freeStringSet(std.testing.allocator, &fixture_names);

    var documented_names = try collectCheckedInCodecCorpusReadmeNames(std.testing.allocator);
    defer freeStringSet(std.testing.allocator, &documented_names);

    try std.testing.expectEqual(fixture_names.count(), documented_names.count());

    var iter = documented_names.iterator();
    while (iter.next()) |entry| {
        try std.testing.expect(fixture_names.contains(entry.key_ptr.*));
    }
}

fn collectCheckedInCodecCorpusFileNames(
    allocator: std.mem.Allocator,
) !std.StringHashMapUnmanaged(void) {
    var fixture_names = std.StringHashMapUnmanaged(void){};
    errdefer fixture_names.deinit(allocator);

    const io = std.testing.io;
    var dir = try std.Io.Dir.cwd().openDir(io, "lib/audio/testdata/codec-corpus", .{ .iterate = true });
    defer dir.close(io);

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (std.mem.eql(u8, entry.name, "README.md")) continue;
        const name = try allocator.dupe(u8, entry.name);
        errdefer allocator.free(name);
        try fixture_names.put(allocator, name, {});
    }

    return fixture_names;
}

fn collectCheckedInCodecCorpusReadmeNames(
    allocator: std.mem.Allocator,
) !std.StringHashMapUnmanaged(void) {
    var documented_names = std.StringHashMapUnmanaged(void){};
    errdefer {
        var err_iter = documented_names.iterator();
        while (err_iter.next()) |entry| allocator.free(entry.key_ptr.*);
        documented_names.deinit(allocator);
    }

    const readme = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "lib/audio/testdata/codec-corpus/README.md",
        allocator,
        .limited(256 * 1024),
    );
    defer allocator.free(readme);

    var lines = std.mem.splitScalar(u8, readme, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, "- `")) continue;
        const rest = line[3..];
        const end = std.mem.indexOfScalar(u8, rest, '`') orelse continue;
        const name = try allocator.dupe(u8, rest[0..end]);
        errdefer allocator.free(name);
        try documented_names.put(allocator, name, {});
    }

    return documented_names;
}

fn freeStringSet(
    allocator: std.mem.Allocator,
    set: *std.StringHashMapUnmanaged(void),
) void {
    var iter = set.iterator();
    while (iter.next()) |entry| allocator.free(entry.key_ptr.*);
    set.deinit(allocator);
}

test "non-mp3 corpus lane summary json export stays coherent" {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(std.testing.allocator);

    try conformance.appendLaneSummariesJson(&buf, std.testing.allocator, &non_mp3_lane_summaries);
    try std.testing.expectEqualStrings(
        "[{\"lane\":\"passing\",\"case_count\":58},{\"lane\":\"decode_but_not_claimed\",\"case_count\":0},{\"lane\":\"unsupported\",\"case_count\":1}]",
        buf.items,
    );
}

test "mp4-family fixtures decode via direct container sniffing and aliases remain coherent" {
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormat(tone_m4a_bytes).?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormat(tone_mp4_bytes).?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromFilename("tone-stereo.m4a").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromFilename("tone-stereo.m4b").?);
    try std.testing.expectEqual(EncodedFormat.mp4, detectFormatFromMime("audio/x-m4p").?);

    var decoded = try decode(std.testing.allocator, tone_m4a_bytes, .{
        .mime_hint = "audio/mp4",
    });
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expect(decoded.samples.len >= 15000);

    var decoded_by_name = try decode(std.testing.allocator, tone_m4a_bytes, .{
        .file_name_hint = "tone-stereo.m4a",
    });
    defer decoded_by_name.deinit();

    try std.testing.expectEqual(decoded.sample_rate, decoded_by_name.sample_rate);
    try std.testing.expectEqual(decoded.samples.len, decoded_by_name.samples.len);

    var generic_mp4 = try decode(std.testing.allocator, tone_mp4_bytes, .{});
    defer generic_mp4.deinit();

    try std.testing.expectEqual(@as(u32, 16000), generic_mp4.sample_rate);
    try std.testing.expect(generic_mp4.samples.len >= 15000);
}

test "downmixToMono averages interleaved stereo frames" {
    const mono = try downmixToMono(std.testing.allocator, &.{ 1.0, -1.0, 0.5, 0.25 }, 2);
    defer std.testing.allocator.free(mono);

    try std.testing.expectEqualSlices(f32, &.{ 0.0, 0.375 }, mono);
}

test "bluestein power spectrum matches naive DFT for non-power-of-two window" {
    const frame = [_]f32{ 0.25, -0.5, 1.0, 0.0, 0.75 };
    const window = [_]f32{ 1.0, 0.5, 0.25, 0.75, 1.0 };
    var fft_plan = try FftPlan.init(std.testing.allocator, frame.len);
    defer fft_plan.deinit(std.testing.allocator);

    var fft_out: [frame.len / 2 + 1]f32 = undefined;
    var naive_out: [frame.len / 2 + 1]f32 = undefined;
    try fft_plan.powerSpectrumWindowed(&frame, &window, &fft_out);
    naivePowerSpectrumWindowed(&frame, &window, &naive_out);

    for (fft_out, naive_out) |actual, expected| {
        try std.testing.expectApproxEqAbs(expected, actual, 0.001);
    }
}

test "mp3 fixture remains close to wav fixture after alignment" {
    if (!mp3.enabled()) return error.SkipZigTest;

    var reference = try decode(std.testing.allocator, tone_wav_bytes, .{});
    defer reference.deinit();

    var candidate = try decode(std.testing.allocator, tone_mp3_bytes, .{});
    defer candidate.deinit();

    const metrics = mp3.conformance.bestAlignmentMetrics(reference.samples, candidate.samples, 2048);
    try std.testing.expect(metrics.compared >= 12000);
    try std.testing.expect(metrics.correlation > 0.999);
    try std.testing.expect(metrics.mean_abs_error < 0.01);
}
