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
const mp3_conformance = @import("mp3/conformance.zig");

pub const AlignmentMetrics = mp3_conformance.AlignmentMetrics;
pub const bestAlignmentMetrics = mp3_conformance.bestAlignmentMetrics;

pub const CorpusLane = enum {
    passing,
    decode_but_not_claimed,
    unsupported,
};

pub const LaneSummary = struct {
    lane: CorpusLane,
    case_count: usize,
};

pub fn laneName(lane: CorpusLane) []const u8 {
    return switch (lane) {
        .passing => "passing",
        .decode_but_not_claimed => "decode_but_not_claimed",
        .unsupported => "unsupported",
    };
}

pub fn CodecCase(comptime EncodedFormat: type) type {
    return struct {
        name: []const u8,
        format: EncodedFormat,
        bytes: []const u8,
        expected_sample_rate: u32,
        expected_channels: u8,
        min_compared: usize,
        min_correlation: f32,
        max_mean_abs_error: f32,
    };
}

pub fn DecodeButNotClaimedCase(comptime EncodedFormat: type, comptime DecodeOptions: type) type {
    return struct {
        name: []const u8,
        bytes: []const u8,
        expected_detect_format: ?EncodedFormat,
        expected_direct_error: anyerror,
        decode_options: DecodeOptions,
        expected_sample_rate: u32,
        expected_channels: u8,
        min_compared: usize,
        min_correlation: f32,
        max_mean_abs_error: f32,
    };
}

pub fn UnsupportedCodecCase(comptime EncodedFormat: type, comptime DecodeOptions: type) type {
    return struct {
        name: []const u8,
        bytes: []const u8,
        decode_options: DecodeOptions,
        expected_detect_format: ?EncodedFormat,
        expected_error: anyerror,
    };
}

pub fn DemuxCase(comptime Codec: type) type {
    return struct {
        name: []const u8,
        bytes: []const u8,
        expected_codec: Codec,
        expected_sample_rate: u32,
        expected_channels: u16,
        expected_decoder_config: []const u8,
        expected_access_unit_count: usize,
        expected_first_access_unit_size: usize,
        expected_trim_start_frames: u64 = 0,
        expected_playable_frames: ?u64 = null,
    };
}

pub fn buildCheckedInCodecCases(comptime EncodedFormat: type, fixtures: anytype) [58]CodecCase(EncodedFormat) {
    return .{
        .{
            .name = "tone-stereo.aac",
            .format = @field(EncodedFormat, "aac"),
            .bytes = fixtures.tone_aac_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.995,
            .max_mean_abs_error = 0.04,
        },
        .{
            .name = "tone-mono-44k.aac",
            .format = @field(EncodedFormat, "aac"),
            .bytes = fixtures.tone_aac_44k_mono_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "transient-mono-44k-pns.aac",
            .format = @field(EncodedFormat, "aac"),
            .bytes = fixtures.transient_aac_44k_pns_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "noise-mono-44k-tns-gain.aac",
            .format = @field(EncodedFormat, "aac"),
            .bytes = fixtures.noise_aac_44k_tns_gain_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "noise-stereo-44k-tns.aac",
            .format = @field(EncodedFormat, "aac"),
            .bytes = fixtures.noise_stereo_aac_44k_tns_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 2,
            .min_compared = 60000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "transient-mono-44k-short.aac",
            .format = @field(EncodedFormat, "aac"),
            .bytes = fixtures.transient_aac_44k_short_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "transient-stereo-44k-short.aac",
            .format = @field(EncodedFormat, "aac"),
            .bytes = fixtures.transient_stereo_aac_44k_short_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 2,
            .min_compared = 60000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "tone-stereo.m4a",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_m4a_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.995,
            .max_mean_abs_error = 0.04,
        },
        .{
            .name = "tone-stereo-12k-sbr.m4a",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_sbr_m4a_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.98,
            .max_mean_abs_error = 0.08,
        },
        .{
            .name = "tone-mono-44k.m4a",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_m4a_44k_mono_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "transient-mono-44k-short.m4a",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.transient_m4a_44k_short_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "transient-stereo-44k-short.m4a",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.transient_stereo_m4a_44k_short_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 2,
            .min_compared = 60000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "tone-stereo.mp4",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_mp4_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.995,
            .max_mean_abs_error = 0.04,
        },
        .{
            .name = "tone-stereo-alac.m4a",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_alac_m4a_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.999,
            .max_mean_abs_error = 0.003,
        },
        .{
            .name = "tone-stereo-alac-24bit.m4a",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_alac_24bit_m4a_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.999,
            .max_mean_abs_error = 0.003,
        },
        .{
            .name = "tone-stereo-alac.mp4",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_alac_mp4_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.999,
            .max_mean_abs_error = 0.003,
        },
        .{
            .name = "tone-stereo-alac-24bit.mp4",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_alac_24bit_mp4_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.999,
            .max_mean_abs_error = 0.003,
        },
        .{
            .name = "tone-mono-44k.mp4",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.tone_mp4_44k_mono_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "transient-mono-44k-short.mp4",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.transient_mp4_44k_short_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .min_compared = 30000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "transient-stereo-44k-short.mp4",
            .format = @field(EncodedFormat, "mp4"),
            .bytes = fixtures.transient_stereo_mp4_44k_short_bytes,
            .expected_sample_rate = 44100,
            .expected_channels = 2,
            .min_compared = 60000,
            .min_correlation = 0.99,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "tone-stereo.ogg",
            .format = @field(EncodedFormat, "ogg"),
            .bytes = fixtures.tone_ogg_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.995,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "tone-stereo.oga",
            .format = @field(EncodedFormat, "ogg"),
            .bytes = fixtures.tone_oga_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.995,
            .max_mean_abs_error = 0.05,
        },
        .{
            .name = "tone-stereo.opus",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.tone_opus_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 36000,
            .min_correlation = 0.98,
            .max_mean_abs_error = 0.08,
        },
        .{
            .name = "tone-mono-48k.opus",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.tone_opus_48k_mono_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 36000,
            .min_correlation = 0.98,
            .max_mean_abs_error = 0.08,
        },
        .{
            .name = "tone-stereo-opus.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.tone_opus_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 36000,
            .min_correlation = 0.98,
            .max_mean_abs_error = 0.08,
        },
        .{
            .name = "probe-celt-mono-48k-5ms.opus",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_celt_48k_mono_5ms_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 36_000,
            .min_correlation = 0.97,
            .max_mean_abs_error = 0.1,
        },
        .{
            .name = "probe-celt-mono-48k-120ms.opus",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_celt_48k_mono_120ms_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 36_000,
            .min_correlation = 0.97,
            .max_mean_abs_error = 0.1,
        },
        .{
            .name = "probe-celt-stereo-48k-2p5ms.opus",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_celt_48k_stereo_2p5ms_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 36_000,
            .min_correlation = 0.97,
            .max_mean_abs_error = 0.1,
        },
        .{
            .name = "probe-celt-stereo-48k-60ms.opus",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_celt_48k_stereo_60ms_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 36_000,
            .min_correlation = 0.97,
            .max_mean_abs_error = 0.1,
        },
        .{
            .name = "probe-celt-stereo-48k-40ms.opus",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_celt_48k_stereo_40ms_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 36_000,
            .min_correlation = 0.97,
            .max_mean_abs_error = 0.1,
        },
        .{
            .name = "probe-silk-mono-16k-fec.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_mono_fec_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-silk-mono-16k-fec-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_mono_fec_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-silk-mono-16k-fec-40ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_mono_fec_40ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-silk-mono-16k-fec-60ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_mono_fec_60ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-silk-mono-16k-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_mono_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-silk-mono-16k-60ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_mono_60ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-silk-mono-16k.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_mono_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-silk-stereo-16k-fec.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_stereo_fec_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-silk-stereo-16k-fec-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_stereo_fec_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-silk-stereo-16k-fec-40ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_stereo_fec_40ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-silk-stereo-16k-fec-60ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_stereo_fec_60ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-silk-stereo-16k-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_stereo_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-silk-stereo-16k.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_stereo_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-silk-stereo-16k-40ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_silk_16k_stereo_40ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-hybrid-mono-48k-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_mono_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-hybrid-mono-48k-fec-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_mono_fec_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-hybrid-mono-48k-fec.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_mono_fec_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-hybrid-mono-48k.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_mono_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 1,
            .min_compared = 8_000,
            .min_correlation = 0.2,
            .max_mean_abs_error = 0.8,
        },
        .{
            .name = "probe-hybrid-stereo-48k-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_stereo_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-hybrid-stereo-48k-fec-10ms.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_stereo_fec_10ms_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-hybrid-stereo-48k-fec.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_stereo_fec_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "probe-hybrid-stereo-48k.ogg",
            .format = @field(EncodedFormat, "opus"),
            .bytes = fixtures.probe_opus_hybrid_48k_stereo_ogg_bytes,
            .expected_sample_rate = 48000,
            .expected_channels = 2,
            .min_compared = 8_000,
            .min_correlation = 0.15,
            .max_mean_abs_error = 1.0,
        },
        .{
            .name = "tone-stereo.flac",
            .format = @field(EncodedFormat, "flac"),
            .bytes = fixtures.tone_flac_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.9999,
            .max_mean_abs_error = 0.0005,
        },
        .{
            .name = "tone-stereo-24bit.flac",
            .format = @field(EncodedFormat, "flac"),
            .bytes = fixtures.tone_flac_24bit_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.9999,
            .max_mean_abs_error = 0.0005,
        },
        .{
            .name = "tone-stereo-flac.ogg",
            .format = @field(EncodedFormat, "ogg"),
            .bytes = fixtures.tone_flac_ogg_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.9999,
            .max_mean_abs_error = 0.0005,
        },
        .{
            .name = "tone-stereo.aiff",
            .format = @field(EncodedFormat, "aiff"),
            .bytes = fixtures.tone_aiff_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.9999,
            .max_mean_abs_error = 0.0005,
        },
        .{
            .name = "tone-stereo.caf",
            .format = @field(EncodedFormat, "caf"),
            .bytes = fixtures.tone_caf_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.999,
            .max_mean_abs_error = 0.003,
        },
        .{
            .name = "tone-stereo-alac-24bit.caf",
            .format = @field(EncodedFormat, "caf"),
            .bytes = fixtures.tone_caf_24bit_bytes,
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .min_compared = 12000,
            .min_correlation = 0.999,
            .max_mean_abs_error = 0.003,
        },
    };
}

pub fn buildCheckedInDecodeButNotClaimedCases(
    comptime EncodedFormat: type,
    comptime DecodeOptions: type,
    fixtures: anytype,
) [0]DecodeButNotClaimedCase(EncodedFormat, DecodeOptions) {
    _ = fixtures;
    return .{};
}

pub fn buildCheckedInMp4DemuxCases(comptime Codec: type, fixtures: anytype) [12]DemuxCase(Codec) {
    return .{
        .{
            .name = "tone-stereo.m4a",
            .bytes = fixtures.tone_m4a_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{ 0x14, 0x10, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 17,
            .expected_first_access_unit_size = 665,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 16000,
        },
        .{
            .name = "tone-mono-44k.m4a",
            .bytes = fixtures.tone_m4a_44k_mono_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .expected_decoder_config = &.{ 0x12, 0x08, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 45,
            .expected_first_access_unit_size = 312,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 44100,
        },
        .{
            .name = "transient-mono-44k-short.m4a",
            .bytes = fixtures.transient_m4a_44k_short_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .expected_decoder_config = &.{ 0x12, 0x08, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 45,
            .expected_first_access_unit_size = 401,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 44100,
        },
        .{
            .name = "transient-stereo-44k-short.m4a",
            .bytes = fixtures.transient_stereo_m4a_44k_short_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 44100,
            .expected_channels = 2,
            .expected_decoder_config = &.{ 0x12, 0x10, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 45,
            .expected_first_access_unit_size = 315,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 44100,
        },
        .{
            .name = "tone-stereo.mp4",
            .bytes = fixtures.tone_mp4_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{ 0x14, 0x10, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 17,
            .expected_first_access_unit_size = 665,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 16000,
        },
        .{
            .name = "tone-mono-44k.mp4",
            .bytes = fixtures.tone_mp4_44k_mono_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .expected_decoder_config = &.{ 0x12, 0x08, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 45,
            .expected_first_access_unit_size = 312,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 44100,
        },
        .{
            .name = "transient-mono-44k-short.mp4",
            .bytes = fixtures.transient_mp4_44k_short_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 44100,
            .expected_channels = 1,
            .expected_decoder_config = &.{ 0x12, 0x08, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 45,
            .expected_first_access_unit_size = 401,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 44100,
        },
        .{
            .name = "transient-stereo-44k-short.mp4",
            .bytes = fixtures.transient_stereo_mp4_44k_short_bytes,
            .expected_codec = @field(Codec, "aac"),
            .expected_sample_rate = 44100,
            .expected_channels = 2,
            .expected_decoder_config = &.{ 0x12, 0x10, 0x56, 0xe5, 0x00 },
            .expected_access_unit_count = 45,
            .expected_first_access_unit_size = 315,
            .expected_trim_start_frames = 1024,
            .expected_playable_frames = 44100,
        },
        .{
            .name = "tone-stereo-alac.m4a",
            .bytes = fixtures.tone_alac_m4a_bytes,
            .expected_codec = @field(Codec, "alac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{
                0x00, 0x00, 0x00, 0x48, 0x61, 0x6c, 0x61, 0x63,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x02, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00,
                0x3e, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24,
                0x61, 0x6c, 0x61, 0x63, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x10, 0x00, 0x00, 0x10, 0x28, 0x0a,
                0x0e, 0x02, 0x00, 0x00, 0x00, 0x00, 0x40, 0x04,
                0x00, 0x07, 0xd0, 0x00, 0x00, 0x00, 0x3e, 0x80,
            },
            .expected_access_unit_count = 4,
            .expected_first_access_unit_size = 1904,
            .expected_playable_frames = 16000,
        },
        .{
            .name = "tone-stereo-alac-24bit.m4a",
            .bytes = fixtures.tone_alac_24bit_m4a_bytes,
            .expected_codec = @field(Codec, "alac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{
                0x00, 0x00, 0x00, 0x24, 0x61, 0x6c, 0x61, 0x63,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
                0x00, 0x18, 0x28, 0x0a, 0x0e, 0x02, 0x00, 0x00,
                0x00, 0x00, 0x60, 0x04, 0x00, 0x0b, 0xb8, 0x00,
                0x00, 0x00, 0x3e, 0x80,
            },
            .expected_access_unit_count = 4,
            .expected_first_access_unit_size = 10168,
            .expected_playable_frames = 16000,
        },
        .{
            .name = "tone-stereo-alac.mp4",
            .bytes = fixtures.tone_alac_mp4_bytes,
            .expected_codec = @field(Codec, "alac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{
                0x00, 0x00, 0x00, 0x48, 0x61, 0x6c, 0x61, 0x63,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x02, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00,
                0x3e, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24,
                0x61, 0x6c, 0x61, 0x63, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x10, 0x00, 0x00, 0x10, 0x28, 0x0a,
                0x0e, 0x02, 0x00, 0x00, 0x00, 0x00, 0x40, 0x04,
                0x00, 0x07, 0xd0, 0x00, 0x00, 0x00, 0x3e, 0x80,
            },
            .expected_access_unit_count = 4,
            .expected_first_access_unit_size = 1904,
            .expected_playable_frames = 16000,
        },
        .{
            .name = "tone-stereo-alac-24bit.mp4",
            .bytes = fixtures.tone_alac_24bit_mp4_bytes,
            .expected_codec = @field(Codec, "alac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{
                0x00, 0x00, 0x00, 0x24, 0x61, 0x6c, 0x61, 0x63,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
                0x00, 0x18, 0x28, 0x0a, 0x0e, 0x02, 0x00, 0x00,
                0x00, 0x00, 0x60, 0x04, 0x00, 0x0b, 0xb8, 0x00,
                0x00, 0x00, 0x3e, 0x80,
            },
            .expected_access_unit_count = 4,
            .expected_first_access_unit_size = 10168,
            .expected_playable_frames = 16000,
        },
    };
}

pub fn buildCheckedInCafDemuxCases(comptime Codec: type, fixtures: anytype) [2]DemuxCase(Codec) {
    return .{
        .{
            .name = "tone-stereo.caf",
            .bytes = fixtures.tone_caf_bytes,
            .expected_codec = @field(Codec, "alac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{
                0x00, 0x00, 0x00, 0x24, 0x61, 0x6c, 0x61, 0x63,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
                0x00, 0x10, 0x28, 0x0a, 0x0e, 0x02, 0x00, 0x00,
                0x00, 0x00, 0x40, 0x04, 0x00, 0x07, 0xd0, 0x00,
                0x00, 0x00, 0x3e, 0x80,
            },
            .expected_access_unit_count = 4,
            .expected_first_access_unit_size = 1904,
        },
        .{
            .name = "tone-stereo-alac-24bit.caf",
            .bytes = fixtures.tone_caf_24bit_bytes,
            .expected_codec = @field(Codec, "alac"),
            .expected_sample_rate = 16000,
            .expected_channels = 2,
            .expected_decoder_config = &.{
                0x00, 0x00, 0x00, 0x24, 0x61, 0x6c, 0x61, 0x63,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,
                0x00, 0x18, 0x28, 0x0a, 0x0e, 0x02, 0x00, 0x00,
                0x00, 0x00, 0x60, 0x04, 0x00, 0x0b, 0xb8, 0x00,
                0x00, 0x00, 0x3e, 0x80,
            },
            .expected_access_unit_count = 4,
            .expected_first_access_unit_size = 10168,
        },
    };
}

pub fn buildCheckedInUnsupportedCodecCases(
    comptime EncodedFormat: type,
    comptime DecodeOptions: type,
    fixtures: anytype,
) [1]UnsupportedCodecCase(EncodedFormat, DecodeOptions) {
    return .{.{
        .name = "unknown bytes",
        .bytes = fixtures.unknown_audio_bytes,
        .decode_options = .{},
        .expected_detect_format = null,
        .expected_error = error.UnsupportedAudioFormat,
    }};
}

pub fn buildNonMp3LaneSummaries(
    passing_count: usize,
    decode_but_not_claimed_count: usize,
    unsupported_count: usize,
) [3]LaneSummary {
    return .{
        .{ .lane = .passing, .case_count = passing_count },
        .{ .lane = .decode_but_not_claimed, .case_count = decode_but_not_claimed_count },
        .{ .lane = .unsupported, .case_count = unsupported_count },
    };
}

pub fn appendLaneSummariesJson(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    summaries: []const LaneSummary,
) !void {
    try buf.append(allocator, '[');
    for (summaries, 0..) |summary, i| {
        if (i > 0) try buf.append(allocator, ',');
        try buf.appendSlice(allocator, "{\"lane\":");
        try appendJsonString(buf, allocator, laneName(summary.lane));
        const tail = try std.fmt.allocPrint(allocator, ",\"case_count\":{d}}}", .{summary.case_count});
        defer allocator.free(tail);
        try buf.appendSlice(allocator, tail);
    }
    try buf.append(allocator, ']');
}

fn appendJsonString(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    text: []const u8,
) !void {
    try buf.append(allocator, '"');
    for (text) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, ch),
        }
    }
    try buf.append(allocator, '"');
}

pub fn assertInterleavedShape(
    expected_sample_rate: u32,
    expected_channels: u8,
    samples: []const f32,
    sample_rate: u32,
    channels: u8,
) !void {
    try std.testing.expectEqual(expected_sample_rate, sample_rate);
    try std.testing.expectEqual(expected_channels, channels);
    try std.testing.expect(samples.len >= expected_sample_rate * expected_channels);
}

pub fn assertStereoCoherenceIfExpected(expected_channels: u8, samples: []const f32) !void {
    if (expected_channels == 2) {
        const left = samples[0];
        const right = samples[1];
        try std.testing.expectApproxEqAbs(left, right, 0.05);
    }
}

pub fn assertReferenceCloseness(
    allocator: std.mem.Allocator,
    reference_samples: []const f32,
    reference_sample_rate: u32,
    mono_samples: []const f32,
    mono_sample_rate: u32,
    min_compared: usize,
    min_correlation: f32,
    max_mean_abs_error: f32,
    copy_or_resample_fn: anytype,
    resample_fn: anytype,
) !void {
    const aligned_reference = if (mono_sample_rate == reference_sample_rate)
        try copy_or_resample_fn(allocator, reference_samples, reference_sample_rate, reference_sample_rate)
    else
        try resample_fn(allocator, reference_samples, reference_sample_rate, mono_sample_rate);
    defer allocator.free(aligned_reference);

    const metrics = mp3_conformance.bestAlignmentMetrics(aligned_reference, mono_samples, 8192);
    try std.testing.expect(metrics.compared >= min_compared);
    try std.testing.expect(metrics.correlation >= min_correlation);
    try std.testing.expect(metrics.mean_abs_error <= max_mean_abs_error);
}

pub fn assertDecodeButNotClaimedCase(
    allocator: std.mem.Allocator,
    case: anytype,
    reference_samples: []const f32,
    reference_sample_rate: u32,
    detect_format_fn: anytype,
    decode_direct_fn: anytype,
    decode_fn: anytype,
    decode_interleaved_fn: anytype,
    copy_or_resample_fn: anytype,
    resample_fn: anytype,
) !void {
    try std.testing.expectEqual(case.expected_detect_format, detect_format_fn(case.bytes));
    try std.testing.expectError(case.expected_direct_error, decode_direct_fn(allocator, case.bytes, case.decode_options));

    var interleaved = try decode_interleaved_fn(allocator, case.bytes, case.decode_options);
    defer interleaved.deinit();

    try assertInterleavedShape(
        case.expected_sample_rate,
        case.expected_channels,
        interleaved.samples,
        interleaved.sample_rate,
        interleaved.channels,
    );

    var mono = try decode_fn(allocator, case.bytes, case.decode_options);
    defer mono.deinit();

    try assertReferenceCloseness(
        allocator,
        reference_samples,
        reference_sample_rate,
        mono.samples,
        mono.sample_rate,
        case.min_compared,
        case.min_correlation,
        case.max_mean_abs_error,
        copy_or_resample_fn,
        resample_fn,
    );
}

pub fn assertUnsupportedCase(case: anytype, detect_format_fn: anytype, decode_fn: anytype) !void {
    try std.testing.expectEqual(case.expected_detect_format, detect_format_fn(case.bytes));
    try std.testing.expectError(case.expected_error, decode_fn(std.testing.allocator, case.bytes, case.decode_options));
}

pub fn assertDemuxCase(case: anytype, demux_fn: anytype) !void {
    var demuxed = try demux_fn(std.testing.allocator, case.bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(case.expected_codec, demuxed.codec);
    try std.testing.expectEqual(case.expected_sample_rate, demuxed.sample_rate);
    try std.testing.expectEqual(case.expected_channels, demuxed.channels);
    try std.testing.expectEqualSlices(u8, case.expected_decoder_config, demuxed.decoder_config);
    try std.testing.expectEqual(case.expected_access_unit_count, demuxed.access_units.len);
    try std.testing.expectEqual(case.expected_first_access_unit_size, demuxed.access_units[0].len);
    if (@hasField(@TypeOf(demuxed), "trim_start_frames")) {
        try std.testing.expectEqual(case.expected_trim_start_frames, demuxed.trim_start_frames);
        try std.testing.expectEqual(case.expected_playable_frames, demuxed.playable_frames);
    }
}

test "lane summary json export is stable" {
    const summaries = buildNonMp3LaneSummaries(11, 3, 1);
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(std.testing.allocator);

    try appendLaneSummariesJson(&buf, std.testing.allocator, &summaries);
    try std.testing.expectEqualStrings(
        "[{\"lane\":\"passing\",\"case_count\":11},{\"lane\":\"decode_but_not_claimed\",\"case_count\":3},{\"lane\":\"unsupported\",\"case_count\":1}]",
        buf.items,
    );
}
