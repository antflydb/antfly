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

const inference_audio = @import("src/mod.zig");

pub const OpenCorpusCodecCase = inference_audio.conformance.CodecCase(inference_audio.EncodedFormat);

const tone_aac_bytes = @embedFile("testdata/codec-corpus/tone-stereo.aac");
const tone_m4a_bytes = @embedFile("testdata/codec-corpus/tone-stereo.m4a");
const tone_sbr_m4a_bytes = @embedFile("testdata/codec-corpus/tone-stereo-12k-sbr.m4a");
const tone_mp4_bytes = @embedFile("testdata/codec-corpus/tone-stereo.mp4");
const tone_alac_m4a_bytes = @embedFile("testdata/codec-corpus/tone-stereo-alac.m4a");
const tone_alac_mp4_bytes = @embedFile("testdata/codec-corpus/tone-stereo-alac.mp4");
const tone_alac_24bit_m4a_bytes = @embedFile("testdata/codec-corpus/tone-stereo-alac-24bit.m4a");
const tone_alac_24bit_mp4_bytes = @embedFile("testdata/codec-corpus/tone-stereo-alac-24bit.mp4");
const tone_aiff_bytes = @embedFile("testdata/codec-corpus/tone-stereo.aiff");
const tone_caf_bytes = @embedFile("testdata/codec-corpus/tone-stereo.caf");
const tone_caf_24bit_bytes = @embedFile("testdata/codec-corpus/tone-stereo-alac-24bit.caf");
const tone_oga_bytes = @embedFile("testdata/codec-corpus/tone-stereo.oga");
const tone_opus_ogg_bytes = @embedFile("testdata/codec-corpus/tone-stereo-opus.ogg");
const tone_flac_ogg_bytes = @embedFile("testdata/codec-corpus/tone-stereo-flac.ogg");
const tone_aac_44k_mono_bytes = @embedFile("testdata/codec-corpus/tone-mono-44k.aac");
const tone_m4a_44k_mono_bytes = @embedFile("testdata/codec-corpus/tone-mono-44k.m4a");
const tone_mp4_44k_mono_bytes = @embedFile("testdata/codec-corpus/tone-mono-44k.mp4");
const transient_aac_44k_pns_bytes = @embedFile("testdata/codec-corpus/transient-mono-44k-pns.aac");
const noise_aac_44k_tns_gain_bytes = @embedFile("testdata/codec-corpus/noise-mono-44k-tns-gain.aac");
const noise_stereo_aac_44k_tns_bytes = @embedFile("testdata/codec-corpus/noise-stereo-44k-tns.aac");
const transient_aac_44k_short_bytes = @embedFile("testdata/codec-corpus/transient-mono-44k-short.aac");
const transient_stereo_aac_44k_short_bytes = @embedFile("testdata/codec-corpus/transient-stereo-44k-short.aac");
const transient_m4a_44k_short_bytes = @embedFile("testdata/codec-corpus/transient-mono-44k-short.m4a");
const transient_mp4_44k_short_bytes = @embedFile("testdata/codec-corpus/transient-mono-44k-short.mp4");
const transient_stereo_m4a_44k_short_bytes = @embedFile("testdata/codec-corpus/transient-stereo-44k-short.m4a");
const transient_stereo_mp4_44k_short_bytes = @embedFile("testdata/codec-corpus/transient-stereo-44k-short.mp4");
const tone_opus_48k_mono_bytes = @embedFile("testdata/codec-corpus/tone-mono-48k.opus");
const probe_opus_celt_48k_mono_5ms_bytes = @embedFile("testdata/codec-corpus/probe-celt-mono-48k-5ms.opus");
const probe_opus_celt_48k_mono_120ms_bytes = @embedFile("testdata/codec-corpus/probe-celt-mono-48k-120ms.opus");
const probe_opus_celt_48k_stereo_2p5ms_bytes = @embedFile("testdata/codec-corpus/probe-celt-stereo-48k-2p5ms.opus");
const probe_opus_celt_48k_stereo_40ms_bytes = @embedFile("testdata/codec-corpus/probe-celt-stereo-48k-40ms.opus");
const probe_opus_celt_48k_stereo_60ms_bytes = @embedFile("testdata/codec-corpus/probe-celt-stereo-48k-60ms.opus");
const probe_opus_silk_16k_mono_fec_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-mono-16k-fec.ogg");
const probe_opus_silk_16k_mono_fec_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-mono-16k-fec-10ms.ogg");
const probe_opus_silk_16k_mono_fec_40ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-mono-16k-fec-40ms.ogg");
const probe_opus_silk_16k_mono_fec_60ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-mono-16k-fec-60ms.ogg");
const probe_opus_silk_16k_mono_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-mono-16k-10ms.ogg");
const probe_opus_silk_16k_mono_60ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-mono-16k-60ms.ogg");
const probe_opus_silk_16k_mono_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-mono-16k.ogg");
const probe_opus_silk_16k_stereo_fec_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-stereo-16k-fec.ogg");
const probe_opus_silk_16k_stereo_fec_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-stereo-16k-fec-10ms.ogg");
const probe_opus_silk_16k_stereo_fec_40ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-stereo-16k-fec-40ms.ogg");
const probe_opus_silk_16k_stereo_fec_60ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-stereo-16k-fec-60ms.ogg");
const probe_opus_silk_16k_stereo_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-stereo-16k-10ms.ogg");
const probe_opus_silk_16k_stereo_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-stereo-16k.ogg");
const probe_opus_silk_16k_stereo_40ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-silk-stereo-16k-40ms.ogg");
const probe_opus_hybrid_48k_mono_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-mono-48k-10ms.ogg");
const probe_opus_hybrid_48k_mono_fec_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-mono-48k-fec-10ms.ogg");
const probe_opus_hybrid_48k_mono_fec_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-mono-48k-fec.ogg");
const probe_opus_hybrid_48k_mono_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-mono-48k.ogg");
const probe_opus_hybrid_48k_stereo_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-stereo-48k-10ms.ogg");
const probe_opus_hybrid_48k_stereo_fec_10ms_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-stereo-48k-fec-10ms.ogg");
const probe_opus_hybrid_48k_stereo_fec_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-stereo-48k-fec.ogg");
const probe_opus_hybrid_48k_stereo_ogg_bytes = @embedFile("testdata/codec-corpus/probe-hybrid-stereo-48k.ogg");
const tone_flac_24bit_bytes = @embedFile("testdata/codec-corpus/tone-stereo-24bit.flac");
const tone_ogg_bytes = @embedFile("testdata/codec-corpus/tone-stereo.ogg");
const tone_opus_bytes = @embedFile("testdata/codec-corpus/tone-stereo.opus");
const tone_flac_bytes = @embedFile("testdata/codec-corpus/tone-stereo.flac");

pub const checked_in_cases: [58]OpenCorpusCodecCase = inference_audio.conformance.buildCheckedInCodecCases(inference_audio.EncodedFormat, .{
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
