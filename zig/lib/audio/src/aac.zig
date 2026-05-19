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
const builtin = @import("builtin");
const fast_imdct = @import("imdct.zig");

pub const PerfCounters = struct {
    config_parse_ns: u64 = 0,
    access_unit_collect_ns: u64 = 0,
    enhancement_info_ns: u64 = 0,
    spectral_state_init_ns: u64 = 0,
    coeff_offsets_ns: u64 = 0,
    coeff_copy_ns: u64 = 0,
    spectral_parse_ns: u64 = 0,
    spectral_decode_ns: u64 = 0,
    tns_tools_ns: u64 = 0,
    trailing_validate_ns: u64 = 0,
    pcm_block_ns: u64 = 0,
    sequence_unit_decode_ns: u64 = 0,
    sequence_output_ns: u64 = 0,
    filterbank_sequence_ns: u64 = 0,
    filterbank_ns: u64 = 0,
    filterbank_imdct_ns: u64 = 0,
    filterbank_window_ns: u64 = 0,
    filterbank_overlap_ns: u64 = 0,
    postprocess_ns: u64 = 0,
    access_unit_count: usize = 0,
    channel_decode_count: usize = 0,
};

var perf_enabled = false;
var perf_counters = PerfCounters{};

pub fn resetPerfCounters() void {
    perf_counters = .{};
    perf_enabled = true;
}

pub fn disablePerfCounters() void {
    perf_enabled = false;
}

pub fn snapshotPerfCounters() PerfCounters {
    return perf_counters;
}

const PerfField = enum {
    config_parse_ns,
    access_unit_collect_ns,
    enhancement_info_ns,
    spectral_state_init_ns,
    coeff_offsets_ns,
    coeff_copy_ns,
    spectral_parse_ns,
    spectral_decode_ns,
    tns_tools_ns,
    trailing_validate_ns,
    pcm_block_ns,
    sequence_unit_decode_ns,
    sequence_output_ns,
    filterbank_sequence_ns,
    filterbank_ns,
    filterbank_imdct_ns,
    filterbank_window_ns,
    filterbank_overlap_ns,
    postprocess_ns,
};

fn perfNowNs() u64 {
    if (!perf_enabled) return 0;
    if (builtin.os.tag == .freestanding) return 0;
    if (!@hasDecl(std.posix.system, "clock_gettime")) return 0;
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}

fn perfAccumulate(field: PerfField, started_ns: u64) void {
    if (!perf_enabled or started_ns == 0) return;
    const elapsed = perfNowNs() - started_ns;
    switch (field) {
        .config_parse_ns => perf_counters.config_parse_ns += elapsed,
        .access_unit_collect_ns => perf_counters.access_unit_collect_ns += elapsed,
        .enhancement_info_ns => perf_counters.enhancement_info_ns += elapsed,
        .spectral_state_init_ns => perf_counters.spectral_state_init_ns += elapsed,
        .coeff_offsets_ns => perf_counters.coeff_offsets_ns += elapsed,
        .coeff_copy_ns => perf_counters.coeff_copy_ns += elapsed,
        .spectral_parse_ns => perf_counters.spectral_parse_ns += elapsed,
        .spectral_decode_ns => perf_counters.spectral_decode_ns += elapsed,
        .tns_tools_ns => perf_counters.tns_tools_ns += elapsed,
        .trailing_validate_ns => perf_counters.trailing_validate_ns += elapsed,
        .pcm_block_ns => perf_counters.pcm_block_ns += elapsed,
        .sequence_unit_decode_ns => perf_counters.sequence_unit_decode_ns += elapsed,
        .sequence_output_ns => perf_counters.sequence_output_ns += elapsed,
        .filterbank_sequence_ns => perf_counters.filterbank_sequence_ns += elapsed,
        .filterbank_ns => perf_counters.filterbank_ns += elapsed,
        .filterbank_imdct_ns => perf_counters.filterbank_imdct_ns += elapsed,
        .filterbank_window_ns => perf_counters.filterbank_window_ns += elapsed,
        .filterbank_overlap_ns => perf_counters.filterbank_overlap_ns += elapsed,
        .postprocess_ns => perf_counters.postprocess_ns += elapsed,
    }
}

const tone_aac_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.aac");
const tone_aac_44k_mono_bytes = @embedFile("../testdata/codec-corpus/tone-mono-44k.aac");
const transient_aac_44k_pns_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-pns.aac");
const transient_aac_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-short.aac");
const transient_stereo_aac_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-stereo-44k-short.aac");
const tone_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo.m4a");
const tone_sbr_m4a_bytes = @embedFile("../testdata/codec-corpus/tone-stereo-12k-sbr.m4a");
const transient_m4a_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-short.m4a");
const transient_mp4_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-mono-44k-short.mp4");
const transient_stereo_m4a_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-stereo-44k-short.m4a");
const transient_stereo_mp4_44k_short_bytes = @embedFile("../testdata/codec-corpus/transient-stereo-44k-short.mp4");
const mp4 = @import("mp4.zig");

pub const AudioSpecificConfig = struct {
    object_type: u8,
    sample_rate: u32,
    channel_config: u8,
    frame_length_960: bool = false,
    depends_on_core_coder: bool = false,
    extension_flag: bool = false,
    extension_object_type: ?u8 = null,
    extension_sample_rate: ?u32 = null,
    sbr_present: bool = false,
    ps_present: bool = false,
    explicit_channel_count: ?u8 = null,
    explicit_layout: ?ProgramConfigLayout = null,
};

pub const AdtsHeader = struct {
    object_type: u8,
    sample_rate: u32,
    channel_config: u8,
    frame_length: u16,
    protection_absent: bool,
    data_blocks_in_frame: u8,

    pub fn samplesPerChannel(self: AdtsHeader) usize {
        return 1024 * (@as(usize, self.data_blocks_in_frame) + 1);
    }
};

pub const AdtsFrame = struct {
    header: AdtsHeader,
    header_len: usize,
    payload: []const u8,
};

pub const ElementKind = enum(u3) {
    sce = 0,
    cpe = 1,
    cce = 2,
    lfe = 3,
    dse = 4,
    pce = 5,
    fil = 6,
    end = 7,
};

pub const AccessUnitSummary = struct {
    first_element: ElementKind,
    element_instance_tag: u8,
    common_window: ?bool = null,
    first_channel_global_gain: ?u8 = null,
};

pub const WindowSequence = enum(u2) {
    only_long = 0,
    long_start = 1,
    eight_short = 2,
    long_stop = 3,
};

const max_prediction_bands = 41;
const max_predictors = 672;

pub const PredictorState = struct {
    r0: f32 = 0,
    r1: f32 = 0,
    cor0: f32 = 0,
    cor1: f32 = 0,
    var0: f32 = 1,
    var1: f32 = 1,
};

pub const IcsInfo = struct {
    window_sequence: WindowSequence,
    window_shape: u1,
    max_sfb: u8,
    num_window_groups: u8,
    window_group_length: [8]u8 = [_]u8{0} ** 8,
    predictor_data_present: ?bool = null,
    predictor_reset_group: u8 = 0,
    prediction_used: [max_prediction_bands]bool = [_]bool{false} ** max_prediction_bands,
};

pub const ElementPrefix = union(ElementKind) {
    sce: SingleChannelElementPrefix,
    cpe: ChannelPairElementPrefix,
    cce: GenericElementPrefix,
    lfe: SingleChannelElementPrefix,
    dse: GenericElementPrefix,
    pce: GenericElementPrefix,
    fil: GenericElementPrefix,
    end: GenericElementPrefix,
};

pub const GenericElementPrefix = struct {
    element_instance_tag: u8,
};

const FillElementInfo = struct {
    saw_sbr_payload: bool = false,
    saw_plain_sbr_payload: bool = false,
    saw_ps_payload: bool = false,
    payload_len: u16 = 0,
    payload_hash: u32 = 2166136261,
    ps_payload_hash: u32 = 2166136261,
    envelope_hint: u8 = 0,
    noise_hint: u8 = 0,
    stereo_hint: u8 = 0,
    harmonic_hint: u8 = 0,
    detail_hint: u8 = 0,
    phase_hint: u8 = 0,
    ps_noise_hint: u8 = 0,
    ps_stereo_hint: u8 = 0,
    ps_harmonic_hint: u8 = 0,
    ps_detail_hint: u8 = 0,
    ps_phase_hint: u8 = 0,
};

const TrailingElementInfo = struct {
    saw_sbr_payload: bool = false,
    saw_plain_sbr_payload: bool = false,
    sbr_carry_generations: u8 = 0,
    saw_ps_payload: bool = false,
    ps_carry_generations: u8 = 0,
    explicit_payload_active: bool = true,
    explicit_ps_payload_active: bool = true,
    max_payload_len: u16 = 0,
    payload_hash: u32 = 2166136261,
    ps_max_payload_len: u16 = 0,
    ps_payload_hash: u32 = 2166136261,
    envelope_hint: u8 = 0,
    noise_hint: u8 = 0,
    stereo_hint: u8 = 0,
    harmonic_hint: u8 = 0,
    detail_hint: u8 = 0,
    phase_hint: u8 = 0,
    ps_noise_hint: u8 = 0,
    ps_stereo_hint: u8 = 0,
    ps_harmonic_hint: u8 = 0,
    ps_detail_hint: u8 = 0,
    ps_phase_hint: u8 = 0,

    fn merge(self: *TrailingElementInfo, other: FillElementInfo) void {
        if (!other.saw_sbr_payload) return;
        const had_plain_sbr = self.saw_plain_sbr_payload;
        self.saw_sbr_payload = true;
        self.saw_plain_sbr_payload = self.saw_plain_sbr_payload or other.saw_plain_sbr_payload;
        self.sbr_carry_generations = 0;
        self.saw_ps_payload = self.saw_ps_payload or other.saw_ps_payload;
        if (other.saw_ps_payload) self.ps_carry_generations = 0;
        if (other.saw_plain_sbr_payload or !had_plain_sbr) {
            self.max_payload_len = other.payload_len;
            self.payload_hash = other.payload_hash;
            if (enhancementPayloadHasEnvelope(other.payload_len)) self.envelope_hint = other.envelope_hint;
            if (enhancementPayloadHasNoise(other.payload_len)) self.noise_hint = other.noise_hint;
            if (enhancementPayloadHasStereo(other.payload_len)) self.stereo_hint = other.stereo_hint;
            if (enhancementPayloadHasTail(other.payload_len)) {
                self.harmonic_hint = other.harmonic_hint;
                self.detail_hint = other.detail_hint;
            }
            if (enhancementPayloadHasPhase(other.payload_len)) self.phase_hint = other.phase_hint;
        }
        if (other.saw_ps_payload) {
            self.ps_max_payload_len = other.payload_len;
            self.ps_payload_hash = other.ps_payload_hash;
            if (enhancementPayloadHasNoise(other.payload_len)) self.ps_noise_hint = other.ps_noise_hint;
            if (enhancementPayloadHasStereo(other.payload_len)) self.ps_stereo_hint = other.ps_stereo_hint;
            if (enhancementPayloadHasTail(other.payload_len)) {
                self.ps_harmonic_hint = other.ps_harmonic_hint;
                self.ps_detail_hint = other.ps_detail_hint;
            }
            if (enhancementPayloadHasPhase(other.payload_len)) self.ps_phase_hint = other.ps_phase_hint;
        }
    }

    fn mergeTrailing(self: *TrailingElementInfo, other: TrailingElementInfo) void {
        if (!other.saw_sbr_payload) return;
        const had_plain_sbr = self.saw_plain_sbr_payload;
        self.saw_sbr_payload = true;
        self.saw_plain_sbr_payload = self.saw_plain_sbr_payload or other.saw_plain_sbr_payload;
        self.sbr_carry_generations = other.sbr_carry_generations;
        self.saw_ps_payload = self.saw_ps_payload or other.saw_ps_payload;
        if (other.saw_ps_payload) self.ps_carry_generations = @min(other.ps_carry_generations, std.math.maxInt(u8));
        if (other.saw_plain_sbr_payload or !had_plain_sbr) {
            self.max_payload_len = other.max_payload_len;
            self.payload_hash = other.payload_hash;
            if (enhancementPayloadHasEnvelope(other.max_payload_len)) self.envelope_hint = other.envelope_hint;
            if (enhancementPayloadHasNoise(other.max_payload_len)) self.noise_hint = other.noise_hint;
            if (enhancementPayloadHasStereo(other.max_payload_len)) self.stereo_hint = other.stereo_hint;
            if (enhancementPayloadHasTail(other.max_payload_len)) {
                self.harmonic_hint = other.harmonic_hint;
                self.detail_hint = other.detail_hint;
            }
            if (enhancementPayloadHasPhase(other.max_payload_len)) self.phase_hint = other.phase_hint;
        }
        if (other.saw_ps_payload) {
            self.ps_max_payload_len = other.ps_max_payload_len;
            self.ps_payload_hash = other.ps_payload_hash;
            if (enhancementPayloadHasNoise(other.ps_max_payload_len)) self.ps_noise_hint = other.ps_noise_hint;
            if (enhancementPayloadHasStereo(other.ps_max_payload_len)) self.ps_stereo_hint = other.ps_stereo_hint;
            if (enhancementPayloadHasTail(other.ps_max_payload_len)) {
                self.ps_harmonic_hint = other.ps_harmonic_hint;
                self.ps_detail_hint = other.ps_detail_hint;
            }
            if (enhancementPayloadHasPhase(other.ps_max_payload_len)) self.ps_phase_hint = other.ps_phase_hint;
        }
    }
};

fn enhancementPayloadHasEnvelope(payload_len: u16) bool {
    return payload_len >= 2;
}

fn enhancementPayloadHasNoise(payload_len: u16) bool {
    return payload_len >= 3;
}

fn enhancementPayloadHasStereo(payload_len: u16) bool {
    return payload_len >= 4;
}

fn enhancementPayloadHasTail(payload_len: u16) bool {
    return payload_len >= 5;
}

fn enhancementPayloadHasPhase(payload_len: u16) bool {
    return payload_len >= 6;
}

fn carryMissingSbrSubfields(info: *TrailingElementInfo, previous: TrailingElementInfo) void {
    if (!enhancementPayloadHasEnvelope(info.max_payload_len)) info.envelope_hint = previous.envelope_hint;
    if (!enhancementPayloadHasNoise(info.max_payload_len)) info.noise_hint = previous.noise_hint;
    if (!enhancementPayloadHasStereo(info.max_payload_len)) info.stereo_hint = previous.stereo_hint;
    if (!enhancementPayloadHasTail(info.max_payload_len)) {
        info.harmonic_hint = previous.harmonic_hint;
        info.detail_hint = previous.detail_hint;
    }
    if (!enhancementPayloadHasPhase(info.max_payload_len)) info.phase_hint = previous.phase_hint;
}

fn carryMissingPsSubfields(info: *TrailingElementInfo, previous: TrailingElementInfo) void {
    if (!enhancementPayloadHasNoise(info.ps_max_payload_len)) info.ps_noise_hint = previous.ps_noise_hint;
    if (!enhancementPayloadHasStereo(info.ps_max_payload_len)) info.ps_stereo_hint = previous.ps_stereo_hint;
    if (!enhancementPayloadHasTail(info.ps_max_payload_len)) {
        info.ps_harmonic_hint = previous.ps_harmonic_hint;
        info.ps_detail_hint = previous.ps_detail_hint;
    }
    if (!enhancementPayloadHasPhase(info.ps_max_payload_len)) info.ps_phase_hint = previous.ps_phase_hint;
}

pub const SingleChannelElementPrefix = struct {
    element_instance_tag: u8,
    global_gain: u8,
    ics_info: IcsInfo,
};

pub const ChannelPairElementPrefix = struct {
    element_instance_tag: u8,
    common_window: bool,
    shared_ics_info: ?IcsInfo,
    ms_present: ?u2,
    left_global_gain: u8,
    left_ics_info: ?IcsInfo,
};

pub const Section = struct {
    band_type: u4,
    start_sfb: u8,
    end_sfb: u8,
};

pub const SectionData = struct {
    ics_info: IcsInfo,
    sections: []Section,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SectionData) void {
        self.allocator.free(self.sections);
    }
};

pub const ScalefactorKind = enum {
    zero,
    spectral,
    noise,
    intensity,
};

pub const ScalefactorBand = struct {
    band_type: u4,
    kind: ScalefactorKind,
    value: i16,
};

pub const ScalefactorData = struct {
    ics_info: IcsInfo,
    sections: []Section,
    bands: []ScalefactorBand,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ScalefactorData) void {
        self.allocator.free(self.sections);
        self.allocator.free(self.bands);
    }
};

pub const PostScalefactorTools = struct {
    ics_info: IcsInfo,
    sections: []Section,
    bands: []ScalefactorBand,
    pulse_present: bool,
    tns_present: bool,
    gain_control_present: bool,
    pulse_data: ?PulseData = null,
    tns_data: ?TnsData = null,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *PostScalefactorTools) void {
        self.allocator.free(self.sections);
        self.allocator.free(self.bands);
    }
};

pub const SpectralCodebookClass = enum {
    zero,
    quad,
    pair,
    escape,
    noise,
    intensity,
};

pub const SpectralBandPlan = struct {
    band_type: u4,
    class: SpectralCodebookClass,
    dimensions: u8,
    unsigned_values: bool,
    uses_escape: bool,
    scalefactor_kind: ScalefactorKind,
    scalefactor_value: i16,
};

pub const SpectralPlan = struct {
    ics_info: IcsInfo,
    sections: []Section,
    bands: []ScalefactorBand,
    plans: []SpectralBandPlan,
    pulse_present: bool,
    tns_present: bool,
    gain_control_present: bool,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SpectralPlan) void {
        self.allocator.free(self.sections);
        self.allocator.free(self.bands);
        self.allocator.free(self.plans);
    }
};

pub const SpectralBandLayout = struct {
    band_type: u4,
    class: SpectralCodebookClass,
    dimensions: u8,
    unsigned_values: bool,
    uses_escape: bool,
    scalefactor_kind: ScalefactorKind,
    scalefactor_value: i16,
    coeff_start: u16,
    coeff_end: u16,
    symbol_count: u16,
};

pub const SpectralLayoutPlan = struct {
    ics_info: IcsInfo,
    sections: []Section,
    bands: []ScalefactorBand,
    plans: []SpectralBandLayout,
    pulse_present: bool,
    tns_present: bool,
    gain_control_present: bool,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SpectralLayoutPlan) void {
        self.allocator.free(self.sections);
        self.allocator.free(self.bands);
        self.allocator.free(self.plans);
    }
};

pub const SpectralCoefficients = struct {
    ics_info: IcsInfo,
    coefficients: []i16,
    contains_noise: bool,
    contains_intensity: bool,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SpectralCoefficients) void {
        self.allocator.free(self.coefficients);
    }
};

pub const DequantizedSpectralCoefficients = struct {
    ics_info: IcsInfo,
    coefficients: []f32,
    contains_noise: bool,
    contains_intensity: bool,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DequantizedSpectralCoefficients) void {
        self.allocator.free(self.coefficients);
    }
};

const FrameShape = struct {
    long_coefficients: usize,
    short_coefficients: usize,
    long_window_samples: usize,
    short_window_samples: usize,
    pcm_samples: usize,
    transition_flat_samples: usize,

    fn default() FrameShape {
        return forFrameLength960(false);
    }

    fn forFrameLength960(frame_length_960: bool) FrameShape {
        if (frame_length_960) {
            return .{
                .long_coefficients = 960,
                .short_coefficients = 120,
                .long_window_samples = 1920,
                .short_window_samples = 240,
                .pcm_samples = 960,
                .transition_flat_samples = 420,
            };
        }
        return .{
            .long_coefficients = 1024,
            .short_coefficients = 128,
            .long_window_samples = 2048,
            .short_window_samples = 256,
            .pcm_samples = 1024,
            .transition_flat_samples = 448,
        };
    }
};

const AacDecodeScratch = struct {
    allocator: std.mem.Allocator,
    windowed_samples: []f32 = &.{},
    short_block: []f32 = &.{},
    imdct_work: []fast_imdct.Complex = &.{},

    fn deinit(self: *AacDecodeScratch) void {
        self.allocator.free(self.windowed_samples);
        self.allocator.free(self.short_block);
        self.allocator.free(self.imdct_work);
        self.* = undefined;
    }

    fn ensureWindowedSamples(self: *AacDecodeScratch, len: usize) ![]f32 {
        if (self.windowed_samples.len < len) self.windowed_samples = try self.allocator.realloc(self.windowed_samples, len);
        return self.windowed_samples[0..len];
    }

    fn ensureShortBlock(self: *AacDecodeScratch, len: usize) ![]f32 {
        if (self.short_block.len < len) self.short_block = try self.allocator.realloc(self.short_block, len);
        return self.short_block[0..len];
    }

    fn ensureImdctWork(self: *AacDecodeScratch, len: usize) ![]fast_imdct.Complex {
        if (self.imdct_work.len < len) self.imdct_work = try self.allocator.realloc(self.imdct_work, len);
        return self.imdct_work[0..len];
    }
};

const ImdctPlan = fast_imdct.Plan;

const AacWindowTables = struct {
    short: []f32,
    long_only: [2][]f32,
    long_start: [2][]f32,
    long_stop: [2][]f32,
    allocator: std.mem.Allocator,

    fn buildAlloc(allocator: std.mem.Allocator, shape: FrameShape) !AacWindowTables {
        var tables = AacWindowTables{
            .short = try allocator.alloc(f32, shape.short_window_samples),
            .long_only = undefined,
            .long_start = undefined,
            .long_stop = undefined,
            .allocator = allocator,
        };
        errdefer allocator.free(tables.short);

        for (0..2) |window_shape| {
            tables.long_only[window_shape] = try allocator.alloc(f32, shape.long_window_samples);
            errdefer allocator.free(tables.long_only[window_shape]);
            tables.long_start[window_shape] = try allocator.alloc(f32, shape.long_window_samples);
            errdefer allocator.free(tables.long_start[window_shape]);
            tables.long_stop[window_shape] = try allocator.alloc(f32, shape.long_window_samples);
            errdefer allocator.free(tables.long_stop[window_shape]);
        }

        for (tables.short, 0..) |*gain, index| {
            gain.* = shortWindowGainWithShape(index, shape);
        }
        for (0..2) |window_shape| {
            for (tables.long_only[window_shape], 0..) |*gain, index| {
                gain.* = windowGainForIndexWithShape(.only_long, @intCast(window_shape), index, shape);
            }
            for (tables.long_start[window_shape], 0..) |*gain, index| {
                gain.* = windowGainForIndexWithShape(.long_start, @intCast(window_shape), index, shape);
            }
            for (tables.long_stop[window_shape], 0..) |*gain, index| {
                gain.* = windowGainForIndexWithShape(.long_stop, @intCast(window_shape), index, shape);
            }
        }
        return tables;
    }

    fn deinit(self: *AacWindowTables) void {
        self.allocator.free(self.short);
        for (self.long_only) |window| self.allocator.free(window);
        for (self.long_start) |window| self.allocator.free(window);
        for (self.long_stop) |window| self.allocator.free(window);
        self.* = undefined;
    }

    fn longFor(self: *const AacWindowTables, sequence: WindowSequence, window_shape: u1) ?[]const f32 {
        return switch (sequence) {
            .only_long => self.long_only[window_shape],
            .long_start => self.long_start[window_shape],
            .long_stop => self.long_stop[window_shape],
            .eight_short => null,
        };
    }
};

const AacImdctPlans = struct {
    long: ImdctPlan,
    short: ImdctPlan,
    windows: AacWindowTables,

    fn buildAlloc(allocator: std.mem.Allocator, shape: FrameShape) !AacImdctPlans {
        return .{
            .long = try buildImdctPlanAlloc(allocator, shape.long_window_samples),
            .short = try buildImdctPlanAlloc(allocator, shape.short_window_samples),
            .windows = try AacWindowTables.buildAlloc(allocator, shape),
        };
    }

    fn deinit(self: *AacImdctPlans) void {
        self.windows.deinit();
        self.long.deinit();
        self.short.deinit();
        self.* = undefined;
    }
};

var shared_aac_plan_lock: std.atomic.Mutex = .unlocked;
var shared_aac_default_plans: ?AacImdctPlans = null;
var shared_aac_960_plans: ?AacImdctPlans = null;

fn sharedAacImdctPlans(shape: FrameShape) !*const AacImdctPlans {
    while (!shared_aac_plan_lock.tryLock()) std.atomic.spinLoopHint();
    defer shared_aac_plan_lock.unlock();

    const slot = if (shape.long_window_samples == FrameShape.default().long_window_samples)
        &shared_aac_default_plans
    else
        &shared_aac_960_plans;

    if (slot.* == null) {
        slot.* = try AacImdctPlans.buildAlloc(std.heap.page_allocator, shape);
    }
    return &(slot.*.?);
}

fn buildImdctPlanAlloc(allocator: std.mem.Allocator, n: usize) !ImdctPlan {
    return ImdctPlan.init(allocator, n);
}

fn imdctIntoWithPlan(out: []f32, coefficients: []const f32, plan: ImdctPlan) !void {
    const work = try std.heap.page_allocator.alloc(fast_imdct.Complex, plan.fft_len);
    defer std.heap.page_allocator.free(work);
    try imdctIntoWithPlanAndScratch(out, coefficients, plan, work);
}

fn imdctIntoWithPlanAndScratch(out: []f32, coefficients: []const f32, plan: ImdctPlan, work: []fast_imdct.Complex) !void {
    try fast_imdct.imdctInto(out, coefficients, &plan, work);
}

fn imdctIntoNaive(out: []f32, coefficients: []const f32) !void {
    if (out.len != coefficients.len * 2) return error.UnsupportedAudioFormat;

    const n = out.len;
    const scale = 2.0 / @as(f32, @floatFromInt(n));
    for (out, 0..) |*sample, n_idx| {
        const n_term = @as(f32, @floatFromInt(n_idx)) + 0.5 + @as(f32, @floatFromInt(n)) / 4.0;
        var accum: f32 = 0;
        for (coefficients, 0..) |coef, k_idx| {
            const k_term = @as(f32, @floatFromInt(k_idx)) + 0.5;
            accum += coef * @cos((std.math.pi / @as(f32, @floatFromInt(n))) * n_term * k_term);
        }
        sample.* = accum * scale;
    }
}

fn isExplicitHeObjectType(object_type: u8) bool {
    return object_type == 5 or object_type == 29;
}

fn configBaseObjectType(config: AudioSpecificConfig) u8 {
    if (isExplicitHeObjectType(config.object_type)) return config.extension_object_type orelse 0;
    return config.object_type;
}

fn configDeclaredOutputSampleRate(config: AudioSpecificConfig) u32 {
    if (isExplicitHeObjectType(config.object_type)) return config.extension_sample_rate orelse config.sample_rate;
    return config.sample_rate;
}

fn spectralCoefficientCount(ics_info: IcsInfo, shape: FrameShape) usize {
    return if (ics_info.window_sequence == .eight_short)
        shape.short_coefficients * 8
    else
        shape.long_coefficients;
}

pub const WindowedLongBlock = struct {
    ics_info: IcsInfo,
    samples: []f32,
    contains_noise: bool,
    contains_intensity: bool,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *WindowedLongBlock) void {
        self.allocator.free(self.samples);
    }
};

pub const OverlapAddedLongBlock = struct {
    pcm: []f32,
    tail: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *OverlapAddedLongBlock) void {
        self.allocator.free(self.pcm);
        self.allocator.free(self.tail);
    }
};

pub const FirstChannelPcmBlock = struct {
    ics_info: IcsInfo,
    pcm: []f32,
    tail: []f32,
    contains_noise: bool,
    contains_intensity: bool,
    trailing_info: TrailingElementInfo,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *FirstChannelPcmBlock) void {
        self.allocator.free(self.pcm);
        self.allocator.free(self.tail);
    }
};

pub const ChannelPairDequantizedCoefficients = struct {
    common_window: bool,
    left_ics_info: IcsInfo,
    right_ics_info: IcsInfo,
    ms_mask: []bool,
    left: []f32,
    right: []f32,
    trailing_info: TrailingElementInfo,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ChannelPairDequantizedCoefficients) void {
        self.allocator.free(self.ms_mask);
        self.allocator.free(self.left);
        self.allocator.free(self.right);
    }
};

pub const ChannelPairPcmBlock = struct {
    common_window: bool,
    left_ics_info: IcsInfo,
    right_ics_info: IcsInfo,
    ms_mask: []bool,
    left_pcm: []f32,
    right_pcm: []f32,
    left_tail: []f32,
    right_tail: []f32,
    trailing_info: TrailingElementInfo,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ChannelPairPcmBlock) void {
        self.allocator.free(self.ms_mask);
        self.allocator.free(self.left_pcm);
        self.allocator.free(self.right_pcm);
        self.allocator.free(self.left_tail);
        self.allocator.free(self.right_tail);
    }
};

pub const ChannelPairPcmSequence = struct {
    samples: []f32,
    sample_rate: u32,
    frame_count: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ChannelPairPcmSequence) void {
        self.allocator.free(self.samples);
    }
};

pub const FirstChannelPcmSequence = struct {
    samples: []f32,
    sample_rate: u32,
    frame_count: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *FirstChannelPcmSequence) void {
        self.allocator.free(self.samples);
    }
};

pub const WindowedShortSequence = struct {
    samples: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *WindowedShortSequence) void {
        self.allocator.free(self.samples);
    }
};

pub const ShortWindowPcmBlock = struct {
    pcm: []f32,
    tail: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ShortWindowPcmBlock) void {
        self.allocator.free(self.pcm);
        self.allocator.free(self.tail);
    }
};

pub const SpectralSymbol = struct {
    values: [4]i16,
    dimensions: u8,
};

pub const PulseData = struct {
    num_pulse: u8,
    pulse_swb: u8,
    offsets: [4]u8,
    amplitudes: [4]u8,
};

pub const TnsFilter = struct {
    length: u8 = 0,
    order: u8 = 0,
    direction: bool = false,
    coef_compress: bool = false,
    coef_len: u8 = 0,
    coefficients: [20]u8 = [_]u8{0} ** 20,
};

pub const TnsWindow = struct {
    n_filt: u8 = 0,
    coef_res: u1 = 0,
    filters: [4]TnsFilter = [_]TnsFilter{.{}} ** 4,
};

pub const TnsData = struct {
    num_windows: u8,
    windows: [8]TnsWindow = [_]TnsWindow{.{}} ** 8,
};

pub const GainControlAdjustment = struct {
    level: u4 = 0,
    location: u8 = 0,
};

pub const GainControlWindow = struct {
    adjust_num: u8 = 0,
    adjustments: [7]GainControlAdjustment = [_]GainControlAdjustment{.{}} ** 7,
};

pub const GainControlBand = struct {
    windows: [8]GainControlWindow = [_]GainControlWindow{.{}} ** 8,
};

pub const GainControlData = struct {
    max_band: u8 = 0,
    bands: [8]GainControlBand = [_]GainControlBand{.{}} ** 8,
};

const ElementHeader = struct {
    kind: ElementKind,
    element_instance_tag: u8,
};

const sample_rate_table = [_]u32{
    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050,
    16000, 12000, 11025, 8000,  7350,
};

const predictor_sfb_max = [_]u8{ 33, 33, 38, 40, 40, 40, 41, 41, 37, 37, 37, 34, 34 };
const aac_main_predict_scale: f32 = -1.0 / 1024.0;

const ZERO_BT: u4 = 0;
const NOISE_BT: u4 = 13;
const INTENSITY_BT: u4 = 14;
const INTENSITY_BT2: u4 = 15;
const INITIAL_PNS_STATE: u32 = 0x1f2e3d4c;

const SpectralHuffmanEntry = struct {
    code: u16,
    bits: u8,
    values: [4]u8,
};

const codebook_vector0_signed_vals = [_]i16{ -1, 0, 1 };
const codebook_vector0_unsigned_vals = [_]i16{ 0, 1, 2 };

const codebook_vector02_idx = [_]u16{
    0x0000, 0x8140, 0x8180, 0x4110, 0xc250, 0xc290, 0x4120, 0xc260, 0xc2a0,
    0x2104, 0xa244, 0xa284, 0x6214, 0xe354, 0xe394, 0x6224, 0xe364, 0xe3a4,
    0x2108, 0xa248, 0xa288, 0x6218, 0xe358, 0xe398, 0x6228, 0xe368, 0xe3a8,
    0x1101, 0x9241, 0x9281, 0x5211, 0xd351, 0xd391, 0x5221, 0xd361, 0xd3a1,
    0x3205, 0xb345, 0xb385, 0x7315, 0xf455, 0xf495, 0x7325, 0xf465, 0xf4a5,
    0x3209, 0xb349, 0xb389, 0x7319, 0xf459, 0xf499, 0x7329, 0xf469, 0xf4a9,
    0x1102, 0x9242, 0x9282, 0x5212, 0xd352, 0xd392, 0x5222, 0xd362, 0xd3a2,
    0x3206, 0xb346, 0xb386, 0x7316, 0xf456, 0xf496, 0x7326, 0xf466, 0xf4a6,
    0x320a, 0xb34a, 0xb38a, 0x731a, 0xf45a, 0xf49a, 0x732a, 0xf46a, 0xf4aa,
};

const SpectralCodebookDescriptor = struct {
    dimensions: u8,
    unsigned_values: bool,
    uses_escape: bool = false,
    entries: []const SpectralHuffmanEntry,
};

const codebook1_entries = [_]SpectralHuffmanEntry{
    .{ .code = 0b0, .bits = 1, .values = .{ 0, 0, 0, 0 } },
    .{ .code = 0b100, .bits = 3, .values = .{ 1, 0, 0, 0 } },
    .{ .code = 0b101, .bits = 3, .values = .{ 0, 1, 0, 0 } },
    .{ .code = 0b110, .bits = 3, .values = .{ 0, 0, 1, 0 } },
    .{ .code = 0b111, .bits = 3, .values = .{ 0, 0, 0, 1 } },
};

const codebook5_entries = [_]SpectralHuffmanEntry{
    .{ .code = 0b00, .bits = 2, .values = .{ 0, 0, 0, 0 } },
    .{ .code = 0b01, .bits = 2, .values = .{ 1, 0, 0, 0 } },
    .{ .code = 0b10, .bits = 2, .values = .{ 0, 1, 0, 0 } },
    .{ .code = 0b110, .bits = 3, .values = .{ 1, 1, 0, 0 } },
    .{ .code = 0b111, .bits = 3, .values = .{ 2, 1, 0, 0 } },
};

const spectral_codebook1 = SpectralCodebookDescriptor{
    .dimensions = 4,
    .unsigned_values = true,
    .uses_escape = false,
    .entries = &codebook1_entries,
};

const spectral_codebook5 = SpectralCodebookDescriptor{
    .dimensions = 2,
    .unsigned_values = true,
    .uses_escape = false,
    .entries = &codebook5_entries,
};

const aac_codes2 = [_]u16{
    0x1f3, 0x06f, 0x1fd, 0x0eb, 0x023, 0x0ea, 0x1f7, 0x0e8, 0x1fa,
    0x0f2, 0x02d, 0x070, 0x020, 0x006, 0x02b, 0x06e, 0x028, 0x0e9,
    0x1f9, 0x066, 0x0f8, 0x0e7, 0x01b, 0x0f1, 0x1f4, 0x06b, 0x1f5,
    0x0ec, 0x02a, 0x06c, 0x02c, 0x00a, 0x027, 0x067, 0x01a, 0x0f5,
    0x024, 0x008, 0x01f, 0x009, 0x000, 0x007, 0x01d, 0x00b, 0x030,
    0x0ef, 0x01c, 0x064, 0x01e, 0x00c, 0x029, 0x0f3, 0x02f, 0x0f0,
    0x1fc, 0x071, 0x1f2, 0x0f4, 0x021, 0x0e6, 0x0f7, 0x068, 0x1f8,
    0x0ee, 0x022, 0x065, 0x031, 0x002, 0x026, 0x0ed, 0x025, 0x06a,
    0x1fb, 0x072, 0x1fe, 0x069, 0x02e, 0x0f6, 0x1ff, 0x06d, 0x1f6,
};

const aac_bits2 = [_]u8{
    9, 7, 9, 8, 6, 8, 9, 8, 9, 8, 6, 7, 6, 5, 6, 7, 6, 8, 9, 7, 8,
    8, 6, 8, 9, 7, 9, 8, 6, 7, 6, 5, 6, 7, 6, 8, 6, 5, 6, 5, 3, 5,
    6, 5, 6, 8, 6, 7, 6, 5, 6, 8, 6, 8, 9, 7, 9, 8, 6, 8, 8, 7, 9,
    8, 6, 7, 6, 4, 6, 8, 6, 7, 9, 7, 9, 7, 6, 8, 9, 7, 9,
};

const aac_codes1 = [_]u16{
    0x7f8, 0x1f1, 0x7fd, 0x3f5, 0x068, 0x3f0, 0x7f7, 0x1ec,
    0x7f5, 0x3f1, 0x072, 0x3f4, 0x074, 0x011, 0x076, 0x1eb,
    0x06c, 0x3f6, 0x7fc, 0x1e1, 0x7f1, 0x1f0, 0x061, 0x1f6,
    0x7f2, 0x1ea, 0x7fb, 0x1f2, 0x069, 0x1ed, 0x077, 0x017,
    0x06f, 0x1e6, 0x064, 0x1e5, 0x067, 0x015, 0x062, 0x012,
    0x000, 0x014, 0x065, 0x016, 0x06d, 0x1e9, 0x063, 0x1e4,
    0x06b, 0x013, 0x071, 0x1e3, 0x070, 0x1f3, 0x7fe, 0x1e7,
    0x7f3, 0x1ef, 0x060, 0x1ee, 0x7f0, 0x1e2, 0x7fa, 0x3f3,
    0x06a, 0x1e8, 0x075, 0x010, 0x073, 0x1f4, 0x06e, 0x3f7,
    0x7f6, 0x1e0, 0x7f9, 0x3f2, 0x066, 0x1f5, 0x7ff, 0x1f7,
    0x7f4,
};

const aac_bits1 = [_]u8{
    11, 9,  11, 10, 7,  10, 11, 9,  11, 10, 7,  10, 7,  5, 7,  9,
    7,  10, 11, 9,  11, 9,  7,  9,  11, 9,  11, 9,  7,  9, 7,  5,
    7,  9,  7,  9,  7,  5,  7,  5,  1,  5,  7,  5,  7,  9, 7,  9,
    7,  5,  7,  9,  7,  9,  11, 9,  11, 9,  7,  9,  11, 9, 11, 10,
    7,  9,  7,  5,  7,  9,  7,  10, 11, 9,  11, 10, 7,  9, 11, 9,
    11,
};

const aac_codes3 = [_]u16{
    0x0000, 0x0009, 0x00ef, 0x000b, 0x0019, 0x00f0, 0x01eb, 0x01e6,
    0x03f2, 0x000a, 0x0035, 0x01ef, 0x0034, 0x0037, 0x01e9, 0x01ed,
    0x01e7, 0x03f3, 0x01ee, 0x03ed, 0x1ffa, 0x01ec, 0x01f2, 0x07f9,
    0x07f8, 0x03f8, 0x0ff8, 0x0008, 0x0038, 0x03f6, 0x0036, 0x0075,
    0x03f1, 0x03eb, 0x03ec, 0x0ff4, 0x0018, 0x0076, 0x07f4, 0x0039,
    0x0074, 0x03ef, 0x01f3, 0x01f4, 0x07f6, 0x01e8, 0x03ea, 0x1ffc,
    0x00f2, 0x01f1, 0x0ffb, 0x03f5, 0x07f3, 0x0ffc, 0x00ee, 0x03f7,
    0x7ffe, 0x01f0, 0x07f5, 0x7ffd, 0x1ffb, 0x3ffa, 0xffff, 0x00f1,
    0x03f0, 0x3ffc, 0x01ea, 0x03ee, 0x3ffb, 0x0ff6, 0x0ffa, 0x7ffc,
    0x07f2, 0x0ff5, 0xfffe, 0x03f4, 0x07f7, 0x7ffb, 0x0ff7, 0x0ff9,
    0x7ffa,
};

const aac_bits3 = [_]u8{
    1,  4,  8,  4,  5,  8,  9,  9,  10, 4,  6,  9,  6,  6,  9,  9,
    9,  10, 9,  10, 13, 9,  9,  11, 11, 10, 12, 4,  6,  10, 6,  7,
    10, 10, 10, 12, 5,  7,  11, 6,  7,  10, 9,  9,  11, 9,  10, 13,
    8,  9,  12, 10, 11, 12, 8,  10, 15, 9,  11, 15, 13, 14, 16, 8,
    10, 14, 9,  10, 14, 12, 12, 15, 11, 12, 16, 10, 11, 15, 12, 12,
    15,
};

const aac_codes5 = [_]u16{
    0x1fff, 0x0ff7, 0x07f4, 0x07e8, 0x03f1, 0x07ee, 0x07f9, 0x0ff8,
    0x1ffd, 0x0ffd, 0x07f1, 0x03e8, 0x01e8, 0x00f0, 0x01ec, 0x03ee,
    0x07f2, 0x0ffa, 0x0ff4, 0x03ef, 0x01f2, 0x00e8, 0x0070, 0x00ec,
    0x01f0, 0x03ea, 0x07f3, 0x07eb, 0x01eb, 0x00ea, 0x001a, 0x0008,
    0x0019, 0x00ee, 0x01ef, 0x07ed, 0x03f0, 0x00f2, 0x0073, 0x000b,
    0x0000, 0x000a, 0x0071, 0x00f3, 0x07e9, 0x07ef, 0x01ee, 0x00ef,
    0x0018, 0x0009, 0x001b, 0x00eb, 0x01e9, 0x07ec, 0x07f6, 0x03eb,
    0x01f3, 0x00ed, 0x0072, 0x00e9, 0x01f1, 0x03ed, 0x07f7, 0x0ff6,
    0x07f0, 0x03e9, 0x01ed, 0x00f1, 0x01ea, 0x03ec, 0x07f8, 0x0ff9,
    0x1ffc, 0x0ffc, 0x0ff5, 0x07ea, 0x03f3, 0x03f2, 0x07f5, 0x0ffb,
    0x1ffe,
};

const aac_bits5 = [_]u8{
    13, 12, 11, 11, 10, 11, 11, 12, 13, 12, 11, 10, 9,  8,  9,  10,
    11, 12, 12, 10, 9,  8,  7,  8,  9,  10, 11, 11, 9,  8,  5,  4,
    5,  8,  9,  11, 10, 8,  7,  4,  1,  4,  7,  8,  11, 11, 9,  8,
    5,  4,  5,  8,  9,  11, 11, 10, 9,  8,  7,  8,  9,  10, 11, 12,
    11, 10, 9,  8,  9,  10, 11, 12, 13, 12, 12, 11, 10, 10, 11, 12,
    13,
};

const aac_codes4 = [_]u16{
    0x007, 0x016, 0x0f6, 0x018, 0x008, 0x0ef, 0x1ef, 0x0f3, 0x7f8,
    0x019, 0x017, 0x0ed, 0x015, 0x001, 0x0e2, 0x0f0, 0x070, 0x3f0,
    0x1ee, 0x0f1, 0x7fa, 0x0ee, 0x0e4, 0x3f2, 0x7f6, 0x3ef, 0x7fd,
    0x005, 0x014, 0x0f2, 0x009, 0x004, 0x0e5, 0x0f4, 0x0e8, 0x3f4,
    0x006, 0x002, 0x0e7, 0x003, 0x000, 0x06b, 0x0e3, 0x069, 0x1f3,
    0x0eb, 0x0e6, 0x3f6, 0x06e, 0x06a, 0x1f4, 0x3ec, 0x1f0, 0x3f9,
    0x0f5, 0x0ec, 0x7fb, 0x0ea, 0x06f, 0x3f7, 0x7f9, 0x3f3, 0xfff,
    0x0e9, 0x06d, 0x3f8, 0x06c, 0x068, 0x1f5, 0x3ee, 0x1f2, 0x7f4,
    0x7f7, 0x3f1, 0xffe, 0x3ed, 0x1f1, 0x7f5, 0x7fe, 0x3f5, 0x7fc,
};

const aac_bits4 = [_]u8{
    4, 5, 8,  5,  4,  8,  9,  8, 11, 5,  5,  8,  5,  4, 8,  8,  7,  10, 9,  8,  11,
    8, 8, 10, 11, 10, 11, 4,  5, 8,  4,  4,  8,  8,  8, 10, 4,  4,  8,  4,  4,  7,
    8, 7, 9,  8,  8,  10, 7,  7, 9,  10, 9,  10, 8,  8, 11, 8,  7,  10, 11, 10, 12,
    8, 7, 10, 7,  7,  9,  10, 9, 11, 11, 10, 12, 10, 9, 11, 11, 10, 11,
};

const aac_codes6 = [_]u16{
    0x7fe, 0x3fd, 0x1f1, 0x1eb, 0x1f4, 0x1ea, 0x1f0, 0x3fc, 0x7fd,
    0x3f6, 0x1e5, 0x0ea, 0x06c, 0x071, 0x068, 0x0f0, 0x1e6, 0x3f7,
    0x1f3, 0x0ef, 0x032, 0x027, 0x028, 0x026, 0x031, 0x0eb, 0x1f7,
    0x1e8, 0x06f, 0x02e, 0x008, 0x004, 0x006, 0x029, 0x06b, 0x1ee,
    0x1ef, 0x072, 0x02d, 0x002, 0x000, 0x003, 0x02f, 0x073, 0x1fa,
    0x1e7, 0x06e, 0x02b, 0x007, 0x001, 0x005, 0x02c, 0x06d, 0x1ec,
    0x1f9, 0x0ee, 0x030, 0x024, 0x02a, 0x025, 0x033, 0x0ec, 0x1f2,
    0x3f8, 0x1e4, 0x0ed, 0x06a, 0x070, 0x069, 0x074, 0x0f1, 0x3fa,
    0x7ff, 0x3f9, 0x1f6, 0x1ed, 0x1f8, 0x1e9, 0x1f5, 0x3fb, 0x7fc,
};

const aac_bits6 = [_]u8{
    11, 10, 9, 9, 9, 9, 9, 10, 11, 10, 9,  8, 7, 7, 7, 8, 9,  10, 9, 8, 6,
    6,  6,  6, 6, 8, 9, 9, 7,  6,  4,  4,  4, 6, 7, 9, 9, 7,  6,  4, 4, 4,
    6,  7,  9, 9, 7, 6, 4, 4,  4,  6,  7,  9, 9, 8, 6, 6, 6,  6,  6, 8, 9,
    10, 9,  8, 7, 7, 7, 7, 8,  10, 11, 10, 9, 9, 9, 9, 9, 10, 11,
};

const aac_codes7 = [_]u16{
    0x000, 0x005, 0x037, 0x074, 0x0f2, 0x1eb, 0x3ed, 0x7f7,
    0x004, 0x00c, 0x035, 0x071, 0x0ec, 0x0ee, 0x1ee, 0x1f5,
    0x036, 0x034, 0x072, 0x0ea, 0x0f1, 0x1e9, 0x1f3, 0x3f5,
    0x073, 0x070, 0x0eb, 0x0f0, 0x1f1, 0x1f0, 0x3ec, 0x3fa,
    0x0f3, 0x0ed, 0x1e8, 0x1ef, 0x3ef, 0x3f1, 0x3f9, 0x7fb,
    0x1ed, 0x0ef, 0x1ea, 0x1f2, 0x3f3, 0x3f8, 0x7f9, 0x7fc,
    0x3ee, 0x1ec, 0x1f4, 0x3f4, 0x3f7, 0x7f8, 0xffd, 0xffe,
    0x7f6, 0x3f0, 0x3f2, 0x3f6, 0x7fa, 0x7fd, 0xffc, 0xfff,
};

const aac_bits7 = [_]u8{
    1,  3, 6, 7,  8,  9,  10, 11, 3,  4,  6,  7,  8,  8,  9,  9,
    6,  6, 7, 8,  8,  9,  9,  10, 7,  7,  8,  8,  9,  9,  10, 10,
    8,  8, 9, 9,  10, 10, 10, 11, 9,  8,  9,  9,  10, 10, 11, 11,
    10, 9, 9, 10, 10, 11, 12, 12, 11, 10, 10, 10, 11, 11, 12, 12,
};

const aac_codes8 = [_]u16{
    0x00e, 0x005, 0x010, 0x030, 0x06f, 0x0f1, 0x1fa, 0x3fe, 0x003,
    0x000, 0x004, 0x012, 0x02c, 0x06a, 0x075, 0x0f8, 0x00f, 0x002,
    0x006, 0x014, 0x02e, 0x069, 0x072, 0x0f5, 0x02f, 0x011, 0x013,
    0x02a, 0x032, 0x06c, 0x0ec, 0x0fa, 0x071, 0x02b, 0x02d, 0x031,
    0x06d, 0x070, 0x0f2, 0x1f9, 0x0ef, 0x068, 0x033, 0x06b, 0x06e,
    0x0ee, 0x0f9, 0x3fc, 0x1f8, 0x074, 0x073, 0x0ed, 0x0f0, 0x0f6,
    0x1f6, 0x1fd, 0x3fd, 0x0f3, 0x0f4, 0x0f7, 0x1f7, 0x1fb, 0x1fc,
    0x3ff,
};

const aac_bits8 = [_]u8{
    5,  4, 5, 6, 7, 8,  9, 10, 4, 3, 4, 5, 6, 7, 7,  8, 5, 4, 4, 5, 6,
    7,  7, 8, 6, 5, 5,  6, 6,  7, 8, 8, 7, 6, 6, 6,  7, 7, 8, 9, 8, 7,
    6,  7, 7, 8, 8, 10, 9, 7,  7, 8, 8, 8, 9, 9, 10, 8, 8, 8, 9, 9, 9,
    10,
};

const aac_codes9 = [_]u16{
    0x0000, 0x0005, 0x0037, 0x00e7, 0x01de, 0x03ce, 0x03d9, 0x07c8,
    0x07cd, 0x0fc8, 0x0fdd, 0x1fe4, 0x1fec, 0x0004, 0x000c, 0x0035,
    0x0072, 0x00ea, 0x00ed, 0x01e2, 0x03d1, 0x03d3, 0x03e0, 0x07d8,
    0x0fcf, 0x0fd5, 0x0036, 0x0034, 0x0071, 0x00e8, 0x00ec, 0x01e1,
    0x03cf, 0x03dd, 0x03db, 0x07d0, 0x0fc7, 0x0fd4, 0x0fe4, 0x00e6,
    0x0070, 0x00e9, 0x01dd, 0x01e3, 0x03d2, 0x03dc, 0x07cc, 0x07ca,
    0x07de, 0x0fd8, 0x0fea, 0x1fdb, 0x01df, 0x00eb, 0x01dc, 0x01e6,
    0x03d5, 0x03de, 0x07cb, 0x07dd, 0x07dc, 0x0fcd, 0x0fe2, 0x0fe7,
    0x1fe1, 0x03d0, 0x01e0, 0x01e4, 0x03d6, 0x07c5, 0x07d1, 0x07db,
    0x0fd2, 0x07e0, 0x0fd9, 0x0feb, 0x1fe3, 0x1fe9, 0x07c4, 0x01e5,
    0x03d7, 0x07c6, 0x07cf, 0x07da, 0x0fcb, 0x0fda, 0x0fe3, 0x0fe9,
    0x1fe6, 0x1ff3, 0x1ff7, 0x07d3, 0x03d8, 0x03e1, 0x07d4, 0x07d9,
    0x0fd3, 0x0fde, 0x1fdd, 0x1fd9, 0x1fe2, 0x1fea, 0x1ff1, 0x1ff6,
    0x07d2, 0x03d4, 0x03da, 0x07c7, 0x07d7, 0x07e2, 0x0fce, 0x0fdb,
    0x1fd8, 0x1fee, 0x3ff0, 0x1ff4, 0x3ff2, 0x07e1, 0x03df, 0x07c9,
    0x07d6, 0x0fca, 0x0fd0, 0x0fe5, 0x0fe6, 0x1feb, 0x1fef, 0x3ff3,
    0x3ff4, 0x3ff5, 0x0fe0, 0x07ce, 0x07d5, 0x0fc6, 0x0fd1, 0x0fe1,
    0x1fe0, 0x1fe8, 0x1ff0, 0x3ff1, 0x3ff8, 0x3ff6, 0x7ffc, 0x0fe8,
    0x07df, 0x0fc9, 0x0fd7, 0x0fdc, 0x1fdc, 0x1fdf, 0x1fed, 0x1ff5,
    0x3ff9, 0x3ffb, 0x7ffd, 0x7ffe, 0x1fe7, 0x0fcc, 0x0fd6, 0x0fdf,
    0x1fde, 0x1fda, 0x1fe5, 0x1ff2, 0x3ffa, 0x3ff7, 0x3ffc, 0x3ffd,
    0x7fff,
};

const aac_bits9 = [_]u8{
    1,  3,  6,  8,  9,  10, 10, 11, 11, 12, 12, 13, 13, 3,  4,  6,  7,
    8,  8,  9,  10, 10, 10, 11, 12, 12, 6,  6,  7,  8,  8,  9,  10, 10,
    10, 11, 12, 12, 12, 8,  7,  8,  9,  9,  10, 10, 11, 11, 11, 12, 12,
    13, 9,  8,  9,  9,  10, 10, 11, 11, 11, 12, 12, 12, 13, 10, 9,  9,
    10, 11, 11, 11, 12, 11, 12, 12, 13, 13, 11, 9,  10, 11, 11, 11, 12,
    12, 12, 12, 13, 13, 13, 11, 10, 10, 11, 11, 12, 12, 13, 13, 13, 13,
    13, 13, 11, 10, 10, 11, 11, 11, 12, 12, 13, 13, 14, 13, 14, 11, 10,
    11, 11, 12, 12, 12, 12, 13, 13, 14, 14, 14, 12, 11, 11, 12, 12, 12,
    13, 13, 13, 14, 14, 14, 15, 12, 11, 12, 12, 12, 13, 13, 13, 13, 14,
    14, 15, 15, 13, 12, 12, 12, 13, 13, 13, 13, 14, 14, 14, 14, 15,
};

const aac_codes10 = [_]u16{
    0x022, 0x008, 0x01d, 0x026, 0x05f, 0x0d3, 0x1cf, 0x3d0, 0x3d7,
    0x3ed, 0x7f0, 0x7f6, 0xffd, 0x007, 0x000, 0x001, 0x009, 0x020,
    0x054, 0x060, 0x0d5, 0x0dc, 0x1d4, 0x3cd, 0x3de, 0x7e7, 0x01c,
    0x002, 0x006, 0x00c, 0x01e, 0x028, 0x05b, 0x0cd, 0x0d9, 0x1ce,
    0x1dc, 0x3d9, 0x3f1, 0x025, 0x00b, 0x00a, 0x00d, 0x024, 0x057,
    0x061, 0x0cc, 0x0dd, 0x1cc, 0x1de, 0x3d3, 0x3e7, 0x05d, 0x021,
    0x01f, 0x023, 0x027, 0x059, 0x064, 0x0d8, 0x0df, 0x1d2, 0x1e2,
    0x3dd, 0x3ee, 0x0d1, 0x055, 0x029, 0x056, 0x058, 0x062, 0x0ce,
    0x0e0, 0x0e2, 0x1da, 0x3d4, 0x3e3, 0x7eb, 0x1c9, 0x05e, 0x05a,
    0x05c, 0x063, 0x0ca, 0x0da, 0x1c7, 0x1ca, 0x1e0, 0x3db, 0x3e8,
    0x7ec, 0x1e3, 0x0d2, 0x0cb, 0x0d0, 0x0d7, 0x0db, 0x1c6, 0x1d5,
    0x1d8, 0x3ca, 0x3da, 0x7ea, 0x7f1, 0x1e1, 0x0d4, 0x0cf, 0x0d6,
    0x0de, 0x0e1, 0x1d0, 0x1d6, 0x3d1, 0x3d5, 0x3f2, 0x7ee, 0x7fb,
    0x3e9, 0x1cd, 0x1c8, 0x1cb, 0x1d1, 0x1d7, 0x1df, 0x3cf, 0x3e0,
    0x3ef, 0x7e6, 0x7f8, 0xffa, 0x3eb, 0x1dd, 0x1d3, 0x1d9, 0x1db,
    0x3d2, 0x3cc, 0x3dc, 0x3ea, 0x7ed, 0x7f3, 0x7f9, 0xff9, 0x7f2,
    0x3ce, 0x1e4, 0x3cb, 0x3d8, 0x3d6, 0x3e2, 0x3e5, 0x7e8, 0x7f4,
    0x7f5, 0x7f7, 0xffb, 0x7fa, 0x3ec, 0x3df, 0x3e1, 0x3e4, 0x3e6,
    0x3f0, 0x7e9, 0x7ef, 0xff8, 0xffe, 0xffc, 0xfff,
};

const aac_bits10 = [_]u8{
    6,  5,  6,  6,  7,  8,  9,  10, 10, 10, 11, 11, 12, 5,  4,  4,  5,  6,  7,  7,  8,
    8,  9,  10, 10, 11, 6,  4,  5,  5,  6,  6,  7,  8,  8,  9,  9,  10, 10, 6,  5,  5,
    5,  6,  7,  7,  8,  8,  9,  9,  10, 10, 7,  6,  6,  6,  6,  7,  7,  8,  8,  9,  9,
    10, 10, 8,  7,  6,  7,  7,  7,  8,  8,  8,  9,  10, 10, 11, 9,  7,  7,  7,  7,  8,
    8,  9,  9,  9,  10, 10, 11, 9,  8,  8,  8,  8,  8,  9,  9,  9,  10, 10, 11, 11, 9,
    8,  8,  8,  8,  8,  9,  9,  10, 10, 10, 11, 11, 10, 9,  9,  9,  9,  9,  9,  10, 10,
    10, 11, 11, 12, 10, 9,  9,  9,  9,  10, 10, 10, 10, 11, 11, 11, 12, 11, 10, 9,  10,
    10, 10, 10, 10, 11, 11, 11, 11, 12, 11, 10, 10, 10, 10, 10, 10, 11, 11, 12, 12, 12,
    12,
};

const aac_codes11 = [_]u16{
    0x000, 0x006, 0x019, 0x03d, 0x09c, 0x0c6, 0x1a7, 0x390, 0x3c2, 0x3df,
    0x7e6, 0x7f3, 0xffb, 0x7ec, 0xffa, 0xffe, 0x38e, 0x005, 0x001, 0x008,
    0x014, 0x037, 0x042, 0x092, 0x0af, 0x191, 0x1a5, 0x1b5, 0x39e, 0x3c0,
    0x3a2, 0x3cd, 0x7d6, 0x0ae, 0x017, 0x007, 0x009, 0x018, 0x039, 0x040,
    0x08e, 0x0a3, 0x0b8, 0x199, 0x1ac, 0x1c1, 0x3b1, 0x396, 0x3be, 0x3ca,
    0x09d, 0x03c, 0x015, 0x016, 0x01a, 0x03b, 0x044, 0x091, 0x0a5, 0x0be,
    0x196, 0x1ae, 0x1b9, 0x3a1, 0x391, 0x3a5, 0x3d5, 0x094, 0x09a, 0x036,
    0x038, 0x03a, 0x041, 0x08c, 0x09b, 0x0b0, 0x0c3, 0x19e, 0x1ab, 0x1bc,
    0x39f, 0x38f, 0x3a9, 0x3cf, 0x093, 0x0bf, 0x03e, 0x03f, 0x043, 0x045,
    0x09e, 0x0a7, 0x0b9, 0x194, 0x1a2, 0x1ba, 0x1c3, 0x3a6, 0x3a7, 0x3bb,
    0x3d4, 0x09f, 0x1a0, 0x08f, 0x08d, 0x090, 0x098, 0x0a6, 0x0b6, 0x0c4,
    0x19f, 0x1af, 0x1bf, 0x399, 0x3bf, 0x3b4, 0x3c9, 0x3e7, 0x0a8, 0x1b6,
    0x0ab, 0x0a4, 0x0aa, 0x0b2, 0x0c2, 0x0c5, 0x198, 0x1a4, 0x1b8, 0x38c,
    0x3a4, 0x3c4, 0x3c6, 0x3dd, 0x3e8, 0x0ad, 0x3af, 0x192, 0x0bd, 0x0bc,
    0x18e, 0x197, 0x19a, 0x1a3, 0x1b1, 0x38d, 0x398, 0x3b7, 0x3d3, 0x3d1,
    0x3db, 0x7dd, 0x0b4, 0x3de, 0x1a9, 0x19b, 0x19c, 0x1a1, 0x1aa, 0x1ad,
    0x1b3, 0x38b, 0x3b2, 0x3b8, 0x3ce, 0x3e1, 0x3e0, 0x7d2, 0x7e5, 0x0b7,
    0x7e3, 0x1bb, 0x1a8, 0x1a6, 0x1b0, 0x1b2, 0x1b7, 0x39b, 0x39a, 0x3ba,
    0x3b5, 0x3d6, 0x7d7, 0x3e4, 0x7d8, 0x7ea, 0x0ba, 0x7e8, 0x3a0, 0x1bd,
    0x1b4, 0x38a, 0x1c4, 0x392, 0x3aa, 0x3b0, 0x3bc, 0x3d7, 0x7d4, 0x7dc,
    0x7db, 0x7d5, 0x7f0, 0x0c1, 0x7fb, 0x3c8, 0x3a3, 0x395, 0x39d, 0x3ac,
    0x3ae, 0x3c5, 0x3d8, 0x3e2, 0x3e6, 0x7e4, 0x7e7, 0x7e0, 0x7e9, 0x7f7,
    0x190, 0x7f2, 0x393, 0x1be, 0x1c0, 0x394, 0x397, 0x3ad, 0x3c3, 0x3c1,
    0x3d2, 0x7da, 0x7d9, 0x7df, 0x7eb, 0x7f4, 0x7fa, 0x195, 0x7f8, 0x3bd,
    0x39c, 0x3ab, 0x3a8, 0x3b3, 0x3b9, 0x3d0, 0x3e3, 0x3e5, 0x7e2, 0x7de,
    0x7ed, 0x7f1, 0x7f9, 0x7fc, 0x193, 0xffd, 0x3dc, 0x3b6, 0x3c7, 0x3cc,
    0x3cb, 0x3d9, 0x3da, 0x7d3, 0x7e1, 0x7ee, 0x7ef, 0x7f5, 0x7f6, 0xffc,
    0xfff, 0x19d, 0x1c2, 0x0b5, 0x0a1, 0x096, 0x097, 0x095, 0x099, 0x0a0,
    0x0a2, 0x0ac, 0x0a9, 0x0b1, 0x0b3, 0x0bb, 0x0c0, 0x18f, 0x004,
};

const aac_bits11 = [_]u8{
    4,  5,  6,  7,  8,  8,  9,  10, 10, 10, 11, 11, 12, 11, 12, 12, 10, 5,  4,  5,
    6,  7,  7,  8,  8,  9,  9,  9,  10, 10, 10, 10, 11, 8,  6,  5,  5,  6,  7,  7,
    8,  8,  8,  9,  9,  9,  10, 10, 10, 10, 8,  7,  6,  6,  6,  7,  7,  8,  8,  8,
    9,  9,  9,  10, 10, 10, 10, 8,  8,  7,  7,  7,  7,  8,  8,  8,  8,  9,  9,  9,
    10, 10, 10, 10, 8,  8,  7,  7,  7,  7,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10,
    10, 8,  9,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  10, 10, 10, 10, 10, 8,  9,
    8,  8,  8,  8,  8,  8,  9,  9,  9,  10, 10, 10, 10, 10, 10, 8,  10, 9,  8,  8,
    9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 11, 8,  10, 9,  9,  9,  9,  9,  9,
    9,  10, 10, 10, 10, 10, 10, 11, 11, 8,  11, 9,  9,  9,  9,  9,  9,  10, 10, 10,
    10, 10, 11, 10, 11, 11, 8,  11, 10, 9,  9,  10, 9,  10, 10, 10, 10, 10, 11, 11,
    11, 11, 11, 8,  11, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11,
    9,  11, 10, 9,  9,  10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 9,  11, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 9,  12, 10, 10, 10, 10,
    10, 10, 10, 11, 11, 11, 11, 11, 11, 12, 12, 9,  9,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  9,  5,
};

const AAC_SPECTRAL_LOOKUP_BITS = 12;

const AacSpectralLookup = struct {
    index: u16 = std.math.maxInt(u16),
    bits: u8 = 0,
};

fn buildAacSpectralLookup(comptime codes: anytype, comptime bits: anytype) [1 << AAC_SPECTRAL_LOOKUP_BITS]AacSpectralLookup {
    @setEvalBranchQuota(100_000);
    var table = [_]AacSpectralLookup{.{}} ** (1 << AAC_SPECTRAL_LOOKUP_BITS);
    for (codes, bits, 0..) |code, bit_len, i| {
        if (bit_len == 0 or bit_len > AAC_SPECTRAL_LOOKUP_BITS) continue;
        const fill_bits = AAC_SPECTRAL_LOOKUP_BITS - bit_len;
        const shift: std.math.Log2Int(usize) = @intCast(fill_bits);
        const start = @as(usize, code) << shift;
        const count = @as(usize, 1) << shift;
        for (0..count) |suffix| {
            table[start + suffix] = .{
                .index = @intCast(i),
                .bits = bit_len,
            };
        }
    }
    return table;
}

const aac_lookup1 = buildAacSpectralLookup(aac_codes1, aac_bits1);
const aac_lookup2 = buildAacSpectralLookup(aac_codes2, aac_bits2);
const aac_lookup3 = buildAacSpectralLookup(aac_codes3, aac_bits3);
const aac_lookup4 = buildAacSpectralLookup(aac_codes4, aac_bits4);
const aac_lookup5 = buildAacSpectralLookup(aac_codes5, aac_bits5);
const aac_lookup6 = buildAacSpectralLookup(aac_codes6, aac_bits6);
const aac_lookup7 = buildAacSpectralLookup(aac_codes7, aac_bits7);
const aac_lookup8 = buildAacSpectralLookup(aac_codes8, aac_bits8);
const aac_lookup9 = buildAacSpectralLookup(aac_codes9, aac_bits9);
const aac_lookup10 = buildAacSpectralLookup(aac_codes10, aac_bits10);
const aac_lookup11 = buildAacSpectralLookup(aac_codes11, aac_bits11);

const AacSpectralCodebook = struct {
    dimensions: u8,
    unsigned_values: bool,
    uses_escape: bool,
    radix: u8,
    codes: []const u16,
    bits: []const u8,
    lookup: []const AacSpectralLookup,
};

// FFmpeg libavcodec/aactab.c ff_aac_scalefactor_code / ff_aac_scalefactor_bits.
const scalefactor_codes = [_]u32{
    0x3ffe8, 0x3ffe6, 0x3ffe7, 0x3ffe5, 0x7fff5, 0x7fff1, 0x7ffed, 0x7fff6,
    0x7ffee, 0x7ffef, 0x7fff0, 0x7fffc, 0x7fffd, 0x7ffff, 0x7fffe, 0x7fff7,
    0x7fff8, 0x7fffb, 0x7fff9, 0x3ffe4, 0x7fffa, 0x3ffe3, 0x1ffef, 0x1fff0,
    0x0fff5, 0x1ffee, 0x0fff2, 0x0fff3, 0x0fff4, 0x0fff1, 0x07ff6, 0x07ff7,
    0x03ff9, 0x03ff5, 0x03ff7, 0x03ff3, 0x03ff6, 0x03ff2, 0x01ff7, 0x01ff5,
    0x00ff9, 0x00ff7, 0x00ff6, 0x007f9, 0x00ff4, 0x007f8, 0x003f9, 0x003f7,
    0x003f5, 0x001f8, 0x001f7, 0x000fa, 0x000f8, 0x000f6, 0x00079, 0x0003a,
    0x00038, 0x0001a, 0x0000b, 0x00004, 0x00000, 0x0000a, 0x0000c, 0x0001b,
    0x00039, 0x0003b, 0x00078, 0x0007a, 0x000f7, 0x000f9, 0x001f6, 0x001f9,
    0x003f4, 0x003f6, 0x003f8, 0x007f5, 0x007f4, 0x007f6, 0x007f7, 0x00ff5,
    0x00ff8, 0x01ff4, 0x01ff6, 0x01ff8, 0x03ff8, 0x03ff4, 0x0fff0, 0x07ff4,
    0x0fff6, 0x07ff5, 0x3ffe2, 0x7ffd9, 0x7ffda, 0x7ffdb, 0x7ffdc, 0x7ffdd,
    0x7ffde, 0x7ffd8, 0x7ffd2, 0x7ffd3, 0x7ffd4, 0x7ffd5, 0x7ffd6, 0x7fff2,
    0x7ffdf, 0x7ffe7, 0x7ffe8, 0x7ffe9, 0x7ffea, 0x7ffeb, 0x7ffe6, 0x7ffe0,
    0x7ffe1, 0x7ffe2, 0x7ffe3, 0x7ffe4, 0x7ffe5, 0x7ffd7, 0x7ffec, 0x7fff4,
    0x7fff3,
};

const scalefactor_bits = [_]u8{
    18, 18, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19,
    19, 19, 19, 18, 19, 18, 17, 17, 16, 17, 16, 16, 16, 16, 15, 15,
    14, 14, 14, 14, 14, 14, 13, 13, 12, 12, 12, 11, 12, 11, 10, 10,
    10, 9,  9,  8,  8,  8,  7,  6,  6,  5,  4,  3,  1,  4,  4,  5,
    6,  6,  7,  7,  8,  8,  9,  9,  10, 10, 10, 11, 11, 11, 11, 12,
    12, 13, 13, 13, 14, 14, 16, 15, 16, 15, 18, 19, 19, 19, 19, 19,
    19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19,
    19, 19, 19, 19, 19, 19, 19, 19, 19,
};

const AAC_SCALEFACTOR_LOOKUP_BITS = 12;

const AacScalefactorLookup = struct {
    symbol: u8 = 0,
    bits: u8 = 0,
};

fn buildAacScalefactorLookup() [1 << AAC_SCALEFACTOR_LOOKUP_BITS]AacScalefactorLookup {
    @setEvalBranchQuota(100_000);
    var table = [_]AacScalefactorLookup{.{}} ** (1 << AAC_SCALEFACTOR_LOOKUP_BITS);
    for (scalefactor_codes, scalefactor_bits, 0..) |code, bit_len, symbol| {
        if (bit_len == 0 or bit_len > AAC_SCALEFACTOR_LOOKUP_BITS) continue;
        const fill_bits = AAC_SCALEFACTOR_LOOKUP_BITS - bit_len;
        const shift: std.math.Log2Int(usize) = @intCast(fill_bits);
        const start = @as(usize, code) << shift;
        const count = @as(usize, 1) << shift;
        for (0..count) |suffix| {
            table[start + suffix] = .{
                .symbol = @intCast(symbol),
                .bits = bit_len,
            };
        }
    }
    return table;
}

const aac_scalefactor_lookup = buildAacScalefactorLookup();
const aac_pow43_table_len = 1024;
const aac_scalefactor_scale_table_len = 256;
const aac_pow43_table = buildAacPow43Table();
const aac_scalefactor_scale_table = buildAacScalefactorScaleTable();

fn buildAacPow43Table() [aac_pow43_table_len]f32 {
    @setEvalBranchQuota(500_000);
    var table: [aac_pow43_table_len]f32 = undefined;
    for (&table, 0..) |*slot, i| {
        slot.* = std.math.pow(f32, @floatFromInt(i), 4.0 / 3.0);
    }
    return table;
}

fn buildAacScalefactorScaleTable() [aac_scalefactor_scale_table_len]f32 {
    @setEvalBranchQuota(500_000);
    var table: [aac_scalefactor_scale_table_len]f32 = undefined;
    for (&table, 0..) |*slot, i| {
        slot.* = std.math.pow(f32, 2.0, (@as(f32, @floatFromInt(i)) - 100.0) / 4.0);
    }
    return table;
}

const swb_offset_1024_96 = [_]u16{
    0,   4,    8,   12,  16,  20,  24,  28,
    32,  36,   40,  44,  48,  52,  56,  64,
    72,  80,   88,  96,  108, 120, 132, 144,
    156, 172,  188, 212, 240, 276, 320, 384,
    448, 512,  576, 640, 704, 768, 832, 896,
    960, 1024,
};

const swb_offset_1024_64 = [_]u16{
    0,   4,   8,   12,  16,  20,  24,  28,
    32,  36,  40,  44,  48,  52,  56,  64,
    72,  80,  88,  100, 112, 124, 140, 156,
    172, 192, 216, 240, 268, 304, 344, 384,
    424, 464, 504, 544, 584, 624, 664, 704,
    744, 784, 824, 864, 904, 944, 984, 1024,
};

const swb_offset_1024_48 = [_]u16{
    0,   4,    8,   12,  16,  20,  24,  28,
    32,  36,   40,  48,  56,  64,  72,  80,
    88,  96,   108, 120, 132, 144, 160, 176,
    196, 216,  240, 264, 292, 320, 352, 384,
    416, 448,  480, 512, 544, 576, 608, 640,
    672, 704,  736, 768, 800, 832, 864, 896,
    928, 1024,
};

const swb_offset_1024_32 = [_]u16{
    0,   4,   8,   12,   16,  20,  24,  28,
    32,  36,  40,  48,   56,  64,  72,  80,
    88,  96,  108, 120,  132, 144, 160, 176,
    196, 216, 240, 264,  292, 320, 352, 384,
    416, 448, 480, 512,  544, 576, 608, 640,
    672, 704, 736, 768,  800, 832, 864, 896,
    928, 960, 992, 1024,
};

const swb_offset_1024_24 = [_]u16{
    0,   4,   8,   12,  16,  20,  24,  28,
    32,  36,  40,  44,  52,  60,  68,  76,
    84,  92,  100, 108, 116, 124, 136, 148,
    160, 172, 188, 204, 220, 240, 260, 284,
    308, 336, 364, 396, 432, 468, 508, 552,
    600, 652, 704, 768, 832, 896, 960, 1024,
};

const swb_offset_1024_16 = [_]u16{
    0,   8,   16,  24,   32,  40,  48,  56,
    64,  72,  80,  88,   100, 112, 124, 136,
    148, 160, 172, 184,  196, 212, 228, 244,
    260, 280, 300, 320,  344, 368, 396, 424,
    456, 492, 532, 572,  616, 664, 716, 772,
    832, 896, 960, 1024,
};

const swb_offset_1024_8 = [_]u16{
    0,    12,  24,  36,  48,  60,  72,  84,
    96,   108, 120, 132, 144, 156, 172, 188,
    204,  220, 236, 252, 268, 288, 308, 328,
    348,  372, 396, 420, 448, 476, 508, 544,
    580,  620, 664, 712, 764, 820, 880, 944,
    1024,
};

const swb_offset_960_96 = [_]u16{
    0,   4,   8,   12,  16,  20,  24,  28,  32,  36,
    40,  44,  48,  52,  56,  64,  72,  80,  88,  96,
    108, 120, 132, 144, 156, 172, 188, 212, 240, 276,
    320, 384, 448, 512, 576, 640, 704, 768, 832, 896,
    960,
};

const swb_offset_960_64 = [_]u16{
    0,   4,   8,   12,  16,  20,  24,  28,  32,  36,
    40,  44,  48,  52,  56,  64,  72,  80,  88,  100,
    112, 124, 140, 156, 172, 192, 216, 240, 268, 304,
    344, 384, 424, 464, 504, 544, 584, 624, 664, 704,
    744, 784, 824, 864, 904, 944, 960,
};

const swb_offset_960_48 = [_]u16{
    0,   4,   8,   12,  16,  20,  24,  28,  32,  36,
    40,  48,  56,  64,  72,  80,  88,  96,  108, 120,
    132, 144, 160, 176, 196, 216, 240, 264, 292, 320,
    352, 384, 416, 448, 480, 512, 544, 576, 608, 640,
    672, 704, 736, 768, 800, 832, 864, 896, 928, 960,
};

const swb_offset_960_32 = [_]u16{
    0,   4,   8,   12,  16,  20,  24,  28,  32,  36,
    40,  48,  56,  64,  72,  80,  88,  96,  108, 120,
    132, 144, 160, 176, 196, 216, 240, 264, 292, 320,
    352, 384, 416, 448, 480, 512, 544, 576, 608, 640,
    672, 704, 736, 768, 800, 832, 864, 896, 928, 960,
};

const swb_offset_960_24 = [_]u16{
    0,   4,   8,   12,  16,  20,  24,  28,  32,  36,
    40,  44,  52,  60,  68,  76,  84,  92,  100, 108,
    116, 124, 136, 148, 160, 172, 188, 204, 220, 240,
    260, 284, 308, 336, 364, 396, 432, 468, 508, 552,
    600, 652, 704, 768, 832, 896, 960,
};

const swb_offset_960_16 = [_]u16{
    0,   8,   16,  24,  32,  40,  48,  56,  64,  72,
    80,  88,  100, 112, 124, 136, 148, 160, 172, 184,
    196, 212, 228, 244, 260, 280, 300, 320, 344, 368,
    396, 424, 456, 492, 532, 572, 616, 664, 716, 772,
    832, 896, 960,
};

const swb_offset_960_8 = [_]u16{
    0,   12,  24,  36,  48,  60,  72,  84,  96,  108,
    120, 132, 144, 156, 172, 188, 204, 220, 236, 252,
    268, 288, 308, 328, 348, 372, 396, 420, 448, 476,
    508, 544, 580, 620, 664, 712, 764, 820, 880, 944,
    960,
};

const swb_offset_128_96 = [_]u16{
    0,  4,  8,  12, 16,  20, 24, 32,
    40, 48, 64, 92, 128,
};

const swb_offset_120_96 = [_]u16{
    0, 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 92, 120,
};

const swb_offset_120_64 = [_]u16{
    0, 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 92, 120,
};

const swb_offset_128_48 = [_]u16{
    0,  4,  8,  12, 16, 20,  28,  36,
    44, 56, 68, 80, 96, 112, 128,
};

const swb_offset_120_48 = [_]u16{
    0, 4, 8, 12, 16, 20, 28, 36, 44, 56, 68, 80, 96, 112, 120,
};

const swb_offset_128_24 = [_]u16{
    0,  4,  8,  12, 16, 20, 24,  28,
    36, 44, 52, 64, 76, 92, 108, 128,
};

const swb_offset_120_24 = [_]u16{
    0, 4, 8, 12, 16, 20, 24, 28, 36, 44, 52, 64, 76, 92, 108, 120,
};

const swb_offset_128_16 = [_]u16{
    0,  4,  8,  12, 16, 20, 24,  28,
    32, 40, 48, 60, 72, 88, 108, 128,
};

const swb_offset_120_16 = [_]u16{
    0, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 60, 72, 88, 108, 120,
};

const swb_offset_128_8 = [_]u16{
    0,  4,  8,  12, 16, 20, 24,  28,
    36, 44, 52, 60, 72, 88, 108, 128,
};

const swb_offset_120_8 = [_]u16{
    0, 4, 8, 12, 16, 20, 24, 28, 36, 44, 52, 60, 72, 88, 108, 120,
};

pub fn parseAudioSpecificConfig(bytes: []const u8) !AudioSpecificConfig {
    var reader = BitReader.init(bytes);
    var config = AudioSpecificConfig{
        .object_type = try readAudioObjectType(&reader),
        .sample_rate = try readSamplingFrequency(&reader),
        .channel_config = try reader.readBits(u8, 4),
    };
    if (config.object_type == 5 or config.object_type == 29) {
        config.sbr_present = true;
        config.ps_present = config.object_type == 29;
        config.extension_sample_rate = try readSamplingFrequency(&reader);
        config.extension_object_type = try readAudioObjectType(&reader);
        if (isGaSpecificObjectType(config.extension_object_type.?)) {
            try parseGaSpecificConfig(&reader, config.extension_object_type.?, &config);
        }
    } else {
        try parseGaSpecificConfig(&reader, config.object_type, &config);
        if (findGaSpecificSyncExtension(bytes, reader.bit_offset)) |extension| {
            config.extension_object_type = extension.object_type;
            config.extension_sample_rate = extension.sample_rate;
            config.sbr_present = extension.object_type == 5 or extension.object_type == 29;
            config.ps_present = extension.object_type == 29;
        }
    }

    return config;
}

fn isGaSpecificObjectType(object_type: u8) bool {
    return switch (object_type) {
        1, 2, 3, 4, 6, 7, 17, 19, 20, 21, 22, 23 => true,
        else => false,
    };
}

fn parseGaSpecificConfig(reader: *BitReader, object_type: u8, config: *AudioSpecificConfig) !void {
    if (!isGaSpecificObjectType(object_type)) return;
    const remaining_bits = reader.bytes.len * 8 - reader.bit_offset;
    if (remaining_bits == 0) return;
    if (remaining_bits < 3) return error.UnsupportedAudioFormat;

    config.frame_length_960 = (try reader.readBits(u1, 1)) != 0;
    config.depends_on_core_coder = (try reader.readBits(u1, 1)) != 0;
    if (config.depends_on_core_coder) try reader.skipBits(14);
    config.extension_flag = (try reader.readBits(u1, 1)) != 0;

    if (config.channel_config == 0) {
        const layout = try parseProgramConfigElementWithTag(reader);
        config.explicit_channel_count = layout.channel_count;
        config.explicit_layout = layout;
    }
}

pub fn parseAdtsHeader(bytes: []const u8) !AdtsHeader {
    if (bytes.len < 7) return error.UnsupportedAudioFormat;
    if (bytes[0] != 0xFF or (bytes[1] & 0xF0) != 0xF0) return error.UnsupportedAudioFormat;
    if ((bytes[1] & 0x06) != 0) return error.UnsupportedAudioFormat;

    const protection_absent = (bytes[1] & 0x01) != 0;
    const profile = (bytes[2] >> 6) & 0x03;
    const sample_rate_index = (bytes[2] >> 2) & 0x0F;
    const channel_config = ((bytes[2] & 0x01) << 2) | ((bytes[3] >> 6) & 0x03);
    const frame_length = (@as(u16, bytes[3] & 0x03) << 11) |
        (@as(u16, bytes[4]) << 3) |
        (@as(u16, bytes[5] >> 5));
    const data_blocks_in_frame = bytes[6] & 0x03;

    return .{
        .object_type = profile + 1,
        .sample_rate = try sampleRateFromIndex(sample_rate_index),
        .channel_config = channel_config,
        .frame_length = frame_length,
        .protection_absent = protection_absent,
        .data_blocks_in_frame = data_blocks_in_frame,
    };
}

pub fn parseAdtsFrame(bytes: []const u8) !AdtsFrame {
    const header = try parseAdtsHeader(bytes);
    const header_len = adtsPayloadOffset(header);
    if (header.frame_length < header_len or header.frame_length > bytes.len) return error.UnsupportedAudioFormat;
    return .{
        .header = header,
        .header_len = header_len,
        .payload = bytes[header_len..header.frame_length],
    };
}

fn adtsPayloadOffset(header: AdtsHeader) usize {
    if (header.protection_absent) return 7;
    if (header.data_blocks_in_frame == 0) return 9;
    return 9 + @as(usize, header.data_blocks_in_frame) * 2;
}

pub fn scanAdtsFramesAlloc(allocator: std.mem.Allocator, bytes: []const u8) ![]AdtsFrame {
    var frames = std.ArrayList(AdtsFrame).empty;
    defer frames.deinit(allocator);

    const end = trimTrailingId3v1Tag(bytes);
    var cursor: usize = try skipLeadingId3v2Tags(bytes, end);
    if (cursor > end) return error.UnsupportedAudioFormat;
    while (cursor < end) {
        if (try skipId3v2TagAt(bytes, &cursor, end)) continue;
        const frame = try parseAdtsFrame(bytes[cursor..end]);
        try frames.append(allocator, frame);
        cursor += frame.header.frame_length;
    }
    if (cursor != end) return error.UnsupportedAudioFormat;
    return try frames.toOwnedSlice(allocator);
}

fn skipLeadingId3v2Tags(bytes: []const u8, end: usize) !usize {
    var cursor: usize = 0;
    while (try skipId3v2TagAt(bytes, &cursor, end)) {}
    return cursor;
}

fn skipId3v2TagAt(bytes: []const u8, cursor: *usize, end: usize) !bool {
    if (cursor.* > end or end - cursor.* < 10 or !std.mem.eql(u8, bytes[cursor.* .. cursor.* + 3], "ID3")) return false;

    const size_bytes = bytes[cursor.* + 6 ..][0..4];
    for (size_bytes) |byte| {
        if ((byte & 0x80) != 0) return error.UnsupportedAudioFormat;
    }

    const tag_size =
        (@as(usize, size_bytes[0]) << 21) |
        (@as(usize, size_bytes[1]) << 14) |
        (@as(usize, size_bytes[2]) << 7) |
        @as(usize, size_bytes[3]);
    const footer_size: usize = if ((bytes[cursor.* + 5] & 0x10) != 0) 10 else 0;
    const tag_total = std.math.add(usize, 10, tag_size) catch return error.UnsupportedAudioFormat;
    const total = std.math.add(usize, tag_total, footer_size) catch return error.UnsupportedAudioFormat;
    if (cursor.* + total > end) return error.UnsupportedAudioFormat;
    cursor.* += total;
    return true;
}

fn trimTrailingId3v1Tag(bytes: []const u8) usize {
    if (bytes.len >= 128 and std.mem.eql(u8, bytes[bytes.len - 128 ..][0..3], "TAG")) {
        return bytes.len - 128;
    }
    return bytes.len;
}

pub fn summarizeAccessUnit(bytes: []const u8) !AccessUnitSummary {
    if (bytes.len == 0) return error.UnsupportedAudioFormat;

    var reader = BitReader.init(bytes);
    const first_element = try seekFirstChannelElement(&reader);

    var summary = AccessUnitSummary{
        .first_element = first_element.kind,
        .element_instance_tag = first_element.element_instance_tag,
    };

    switch (first_element.kind) {
        .sce, .lfe => {
            summary.first_channel_global_gain = try reader.readBits(u8, 8);
        },
        .cpe => {
            const common_window = (try reader.readBits(u1, 1)) != 0;
            summary.common_window = common_window;
            if (!common_window) {
                summary.first_channel_global_gain = try reader.readBits(u8, 8);
            }
        },
        else => {},
    }

    return summary;
}

pub fn parseFirstElementPrefix(bytes: []const u8) !ElementPrefix {
    if (bytes.len == 0) return error.UnsupportedAudioFormat;

    var reader = BitReader.init(bytes);
    const first_element = try seekFirstChannelElement(&reader);

    return switch (first_element.kind) {
        .sce => blk: {
            const global_gain = try reader.readBits(u8, 8);
            const ics_info = try parseIcsInfo(&reader, null);
            break :blk .{ .sce = .{
                .element_instance_tag = first_element.element_instance_tag,
                .global_gain = global_gain,
                .ics_info = ics_info,
            } };
        },
        .lfe => blk: {
            const global_gain = try reader.readBits(u8, 8);
            const ics_info = try parseIcsInfo(&reader, null);
            break :blk .{ .lfe = .{
                .element_instance_tag = first_element.element_instance_tag,
                .global_gain = global_gain,
                .ics_info = ics_info,
            } };
        },
        .cpe => blk: {
            const common_window = (try reader.readBits(u1, 1)) != 0;
            var shared_ics_info: ?IcsInfo = null;
            var ms_present: ?u2 = null;
            if (common_window) {
                const info = try parseIcsInfo(&reader, null);
                shared_ics_info = info;
                ms_present = try reader.readBits(u2, 2);
                if (ms_present.? == 1) {
                    try reader.skipBits(@as(usize, info.num_window_groups) * info.max_sfb);
                }
            }
            const left_global_gain = try reader.readBits(u8, 8);
            const left_ics_info = if (common_window) null else try parseIcsInfo(&reader, null);
            break :blk .{ .cpe = .{
                .element_instance_tag = first_element.element_instance_tag,
                .common_window = common_window,
                .shared_ics_info = shared_ics_info,
                .ms_present = ms_present,
                .left_global_gain = left_global_gain,
                .left_ics_info = left_ics_info,
            } };
        },
        .cce => .{ .cce = .{ .element_instance_tag = first_element.element_instance_tag } },
        .dse => .{ .dse = .{ .element_instance_tag = first_element.element_instance_tag } },
        .pce => .{ .pce = .{ .element_instance_tag = first_element.element_instance_tag } },
        .fil => .{ .fil = .{ .element_instance_tag = first_element.element_instance_tag } },
        .end => .{ .end = .{ .element_instance_tag = first_element.element_instance_tag } },
    };
}

pub fn parseFirstChannelSectionDataAlloc(allocator: std.mem.Allocator, bytes: []const u8) !SectionData {
    if (bytes.len == 0) return error.UnsupportedAudioFormat;

    var first_channel = try initFirstChannelReader(bytes, null);
    const sections = try parseSectionDataAlloc(allocator, &first_channel.reader, first_channel.ics_info);

    return .{
        .ics_info = first_channel.ics_info,
        .sections = sections,
        .allocator = allocator,
    };
}

pub fn parseFirstChannelScalefactorsAlloc(allocator: std.mem.Allocator, bytes: []const u8) !ScalefactorData {
    if (bytes.len == 0) return error.UnsupportedAudioFormat;

    var first_channel = try initFirstChannelReader(bytes, null);
    const sections = try parseSectionDataAlloc(allocator, &first_channel.reader, first_channel.ics_info);
    errdefer allocator.free(sections);

    const bands = try parseScalefactorBandsAlloc(
        allocator,
        &first_channel.reader,
        first_channel.global_gain,
        first_channel.ics_info,
        sections,
    );
    errdefer allocator.free(bands);

    return .{
        .ics_info = first_channel.ics_info,
        .sections = sections,
        .bands = bands,
        .allocator = allocator,
    };
}

pub fn parseFirstChannelPostScalefactorToolsAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) !PostScalefactorTools {
    if (bytes.len == 0) return error.UnsupportedAudioFormat;

    var first_channel = try initFirstChannelReader(bytes, null);
    const sections = try parseSectionDataAlloc(allocator, &first_channel.reader, first_channel.ics_info);
    errdefer allocator.free(sections);

    const bands = try parseScalefactorBandsAlloc(
        allocator,
        &first_channel.reader,
        first_channel.global_gain,
        first_channel.ics_info,
        sections,
    );
    errdefer allocator.free(bands);

    const pulse_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    const pulse_data = if (pulse_present)
        try parsePulseData(&first_channel.reader, first_channel.ics_info)
    else
        null;
    const tns_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    const tns_data = if (tns_present)
        try parseTnsData(&first_channel.reader, first_channel.ics_info, false)
    else
        null;
    const gain_control_present = (try first_channel.reader.readBits(u1, 1)) != 0;

    return .{
        .ics_info = first_channel.ics_info,
        .sections = sections,
        .bands = bands,
        .pulse_present = pulse_present,
        .tns_present = tns_present,
        .gain_control_present = gain_control_present,
        .pulse_data = pulse_data,
        .tns_data = tns_data,
        .allocator = allocator,
    };
}

pub fn parseFirstChannelSpectralPlanAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) !SpectralPlan {
    var tools = try parseFirstChannelPostScalefactorToolsAlloc(allocator, bytes);
    errdefer tools.deinit();

    const plans = try allocator.alloc(SpectralBandPlan, tools.bands.len);
    for (tools.bands, plans) |band, *plan| {
        const info = spectralCodebookInfo(band.band_type);
        plan.* = .{
            .band_type = band.band_type,
            .class = spectralCodebookClass(band.band_type),
            .dimensions = info.dimensions,
            .unsigned_values = info.unsigned_values,
            .uses_escape = info.uses_escape,
            .scalefactor_kind = band.kind,
            .scalefactor_value = band.value,
        };
    }

    const sections = tools.sections;
    const bands = tools.bands;
    const pulse_present = tools.pulse_present;
    const tns_present = tools.tns_present;
    const gain_control_present = tools.gain_control_present;
    tools.sections = &.{};
    tools.bands = &.{};
    tools.deinit();

    return .{
        .ics_info = tools.ics_info,
        .sections = sections,
        .bands = bands,
        .plans = plans,
        .pulse_present = pulse_present,
        .tns_present = tns_present,
        .gain_control_present = gain_control_present,
        .allocator = allocator,
    };
}

pub fn parseFirstChannelSpectralLayoutPlanAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
) !SpectralLayoutPlan {
    var state = try initFirstChannelSpectralStateAlloc(allocator, sample_rate, bytes);
    defer {
        state.sections = &.{};
        state.bands = &.{};
        state.plans = &.{};
        state.deinit();
    }

    return .{
        .ics_info = state.ics_info,
        .sections = state.sections,
        .bands = state.bands,
        .plans = state.plans,
        .pulse_present = state.pulse_present,
        .tns_present = state.tns_present,
        .gain_control_present = state.gain_control_present,
        .allocator = allocator,
    };
}

pub fn decodeFirstChannelSpectralCoefficientsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
) !SpectralCoefficients {
    var state = try initFirstChannelSpectralStateAlloc(allocator, sample_rate, bytes);
    defer state.deinit();

    const coeff_len = spectralCoefficientCount(state.ics_info);
    const coefficients = try allocator.alloc(i16, coeff_len);
    @memset(coefficients, 0);
    errdefer allocator.free(coefficients);

    var contains_noise = false;
    var contains_intensity = false;

    for (state.plans) |plan| {
        switch (plan.class) {
            .zero => continue,
            .noise => {
                contains_noise = true;
                continue;
            },
            .intensity => {
                contains_intensity = true;
                continue;
            },
            .quad, .pair, .escape => {},
        }

        const codebook = try aacSpectralCodebook(plan.band_type);
        var coeff_index: usize = plan.coeff_start;
        for (0..plan.symbol_count) |_| {
            const symbol = try decodeAacSpectralSymbol(&state.reader, codebook);
            for (0..symbol.dimensions) |i| {
                coefficients[coeff_index] = symbol.values[i];
                coeff_index += 1;
            }
        }
        if (coeff_index != plan.coeff_end) return error.UnsupportedAudioFormat;
    }

    return .{
        .ics_info = state.ics_info,
        .coefficients = coefficients,
        .contains_noise = contains_noise,
        .contains_intensity = contains_intensity,
        .allocator = allocator,
    };
}

pub fn dequantizeFirstChannelSpectralCoefficientsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
) !DequantizedSpectralCoefficients {
    return dequantizeFirstChannelSpectralCoefficientsAllocWithShape(
        allocator,
        sample_rate,
        bytes,
        FrameShape.default(),
    );
}

fn dequantizeFirstChannelSpectralCoefficientsAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
    shape: FrameShape,
) !DequantizedSpectralCoefficients {
    var state = try initFirstChannelSpectralStateAllocWithShape(allocator, sample_rate, bytes, shape);
    defer state.deinit();
    var predictor_states = [_]PredictorState{.{}} ** max_predictors;
    resetAllPredictors(&predictor_states);
    return try dequantizeFirstChannelSpectralStateAllocWithShape(allocator, sample_rate, &state, &predictor_states, shape);
}

fn dequantizeFirstChannelSpectralStateAlloc(
    allocator: std.mem.Allocator,
    sample_rate: ?u32,
    state: *FirstChannelSpectralState,
    predictor_states: ?*[max_predictors]PredictorState,
) !DequantizedSpectralCoefficients {
    return dequantizeFirstChannelSpectralStateAllocWithShape(
        allocator,
        sample_rate,
        state,
        predictor_states,
        FrameShape.default(),
    );
}

fn dequantizeFirstChannelSpectralStateAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: ?u32,
    state: *FirstChannelSpectralState,
    predictor_states: ?*[max_predictors]PredictorState,
    shape: FrameShape,
) !DequantizedSpectralCoefficients {
    const coefficients = try decodeChannelDequantizedIntoAllocWithShape(
        allocator,
        &state.reader,
        state.coeff_offsets,
        state.bands,
        spectralCoefficientCount(state.ics_info, shape),
        shape,
    );
    errdefer allocator.free(coefficients);

    const contains_noise = containsBandKind(state.bands, .noise);
    const contains_intensity = containsBandKind(state.bands, .intensity);
    var pns_state = INITIAL_PNS_STATE;
    try applyPerceptualNoiseSubstitutionForBands(
        coefficients,
        state.bands,
        state.coeff_offsets,
        &pns_state,
    );
    if (state.pulse_data) |pulse| {
        try applyPulseToolForBands(
            coefficients,
            state.bands,
            state.coeff_offsets,
            state.raw_swb_offsets,
            pulse,
        );
    }
    if (sample_rate) |known_sample_rate| {
        if (predictor_states) |states| {
            try applyMainPrediction(coefficients, state.ics_info, state.raw_swb_offsets, known_sample_rate, states);
        }
    }
    if (state.tns_data) |tns| {
        try applyTnsToolForBands(
            coefficients,
            state.ics_info,
            state.bands,
            state.coeff_offsets,
            state.raw_swb_offsets,
            tns,
        );
    }

    return .{
        .ics_info = state.ics_info,
        .coefficients = coefficients,
        .contains_noise = contains_noise,
        .contains_intensity = contains_intensity,
        .allocator = allocator,
    };
}

fn dequantizeFirstChannelSpectralCoefficientsWithCoeffOffsetsAlloc(
    allocator: std.mem.Allocator,
    coeff_offsets: []const u16,
    bytes: []const u8,
) !DequantizedSpectralCoefficients {
    var state = try initFirstChannelSpectralStateWithCoeffOffsetsAlloc(allocator, coeff_offsets, bytes);
    defer state.deinit();

    return try dequantizeFirstChannelSpectralStateAlloc(allocator, null, &state, null);
}

pub fn windowFirstChannelLongBlockAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
) !WindowedLongBlock {
    var dequantized = try dequantizeFirstChannelSpectralCoefficientsAlloc(
        allocator,
        sample_rate,
        bytes,
    );
    defer dequantized.deinit();

    if (dequantized.ics_info.window_sequence == .eight_short) return error.UnsupportedAudioFormat;

    const samples = try allocator.alloc(f32, 2048);
    errdefer allocator.free(samples);
    try imdctLongInto(samples, dequantized.coefficients);
    try applyAacLongWindow(dequantized.ics_info, samples);

    return .{
        .ics_info = dequantized.ics_info,
        .samples = samples,
        .contains_noise = dequantized.contains_noise,
        .contains_intensity = dequantized.contains_intensity,
        .allocator = allocator,
    };
}

fn windowFirstChannelShortSequenceWithCoeffOffsetsAlloc(
    allocator: std.mem.Allocator,
    coeff_offsets: []const u16,
    bytes: []const u8,
) !WindowedShortSequence {
    var dequantized = try dequantizeFirstChannelSpectralCoefficientsWithCoeffOffsetsAlloc(
        allocator,
        coeff_offsets,
        bytes,
    );
    defer dequantized.deinit();

    if (dequantized.ics_info.window_sequence != .eight_short) return error.UnsupportedAudioFormat;
    return try composeEightShortWindowSequenceAlloc(allocator, dequantized.coefficients);
}

fn decodeFirstChannelShortPcmBlockWithCoeffOffsetsAlloc(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    coeff_offsets: []const u16,
    bytes: []const u8,
) !ShortWindowPcmBlock {
    var windowed = try windowFirstChannelShortSequenceWithCoeffOffsetsAlloc(
        allocator,
        coeff_offsets,
        bytes,
    );
    defer windowed.deinit();

    return try overlapAddShortWindowSequenceAlloc(allocator, previous_tail, windowed.samples);
}

pub fn overlapAddLongBlockAlloc(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    current_windowed: []const f32,
) !OverlapAddedLongBlock {
    return overlapAddLongBlockAllocWithShape(allocator, previous_tail, current_windowed, FrameShape.default());
}

fn overlapAddLongBlockAllocWithShape(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    current_windowed: []const f32,
    shape: FrameShape,
) !OverlapAddedLongBlock {
    if (current_windowed.len != shape.long_window_samples) return error.UnsupportedAudioFormat;
    if (previous_tail) |tail| {
        if (tail.len != shape.pcm_samples) return error.UnsupportedAudioFormat;
    }

    const pcm = try allocator.alloc(f32, shape.pcm_samples);
    errdefer allocator.free(pcm);
    const tail = try allocator.alloc(f32, shape.pcm_samples);
    errdefer allocator.free(tail);

    if (previous_tail) |prev| {
        for (pcm, prev, current_windowed[0..shape.pcm_samples]) |*out, lhs, rhs| {
            out.* = lhs + rhs;
        }
    } else {
        @memcpy(pcm, current_windowed[0..shape.pcm_samples]);
    }
    @memcpy(tail, current_windowed[shape.pcm_samples..shape.long_window_samples]);

    return .{
        .pcm = pcm,
        .tail = tail,
        .allocator = allocator,
    };
}

fn overlapAddLongBlockFromImdctAllocWithShape(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    current_imdct: []const f32,
    ics_info: IcsInfo,
    windows: ?*const AacWindowTables,
    shape: FrameShape,
) !OverlapAddedLongBlock {
    if (current_imdct.len != shape.long_window_samples) return error.UnsupportedAudioFormat;
    if (previous_tail) |tail| {
        if (tail.len != shape.pcm_samples) return error.UnsupportedAudioFormat;
    }

    const pcm = try allocator.alloc(f32, shape.pcm_samples);
    errdefer allocator.free(pcm);
    const tail = try allocator.alloc(f32, shape.pcm_samples);
    errdefer allocator.free(tail);

    if (windows) |owned| {
        if (owned.longFor(ics_info.window_sequence, ics_info.window_shape)) |gains| {
            if (gains.len != shape.long_window_samples) return error.UnsupportedAudioFormat;
            overlapAddLongBlockFromImdctWithGains(pcm, tail, previous_tail, current_imdct, gains, shape);
            return .{
                .pcm = pcm,
                .tail = tail,
                .allocator = allocator,
            };
        }
    }

    overlapAddLongBlockFromImdctWithComputedWindow(pcm, tail, previous_tail, current_imdct, ics_info, shape);
    return .{
        .pcm = pcm,
        .tail = tail,
        .allocator = allocator,
    };
}

fn overlapAddLongBlockFromImdctWithGains(
    pcm: []f32,
    tail: []f32,
    previous_tail: ?[]const f32,
    current_imdct: []const f32,
    gains: []const f32,
    shape: FrameShape,
) void {
    if (previous_tail) |prev| {
        for (pcm, prev, current_imdct[0..shape.pcm_samples], gains[0..shape.pcm_samples]) |*out, lhs, rhs, gain| {
            out.* = lhs + rhs * gain;
        }
    } else {
        for (pcm, current_imdct[0..shape.pcm_samples], gains[0..shape.pcm_samples]) |*out, sample, gain| {
            out.* = sample * gain;
        }
    }
    for (
        tail,
        current_imdct[shape.pcm_samples..shape.long_window_samples],
        gains[shape.pcm_samples..shape.long_window_samples],
    ) |*out, sample, gain| {
        out.* = sample * gain;
    }
}

fn overlapAddLongBlockFromImdctWithComputedWindow(
    pcm: []f32,
    tail: []f32,
    previous_tail: ?[]const f32,
    current_imdct: []const f32,
    ics_info: IcsInfo,
    shape: FrameShape,
) void {
    if (previous_tail) |prev| {
        for (pcm, prev, current_imdct[0..shape.pcm_samples], 0..) |*out, lhs, rhs, index| {
            out.* = lhs + rhs * windowGainForIndexWithShape(ics_info.window_sequence, ics_info.window_shape, index, shape);
        }
    } else {
        for (pcm, current_imdct[0..shape.pcm_samples], 0..) |*out, sample, index| {
            out.* = sample * windowGainForIndexWithShape(ics_info.window_sequence, ics_info.window_shape, index, shape);
        }
    }
    for (tail, current_imdct[shape.pcm_samples..shape.long_window_samples], 0..) |*out, sample, offset| {
        const index = shape.pcm_samples + offset;
        out.* = sample * windowGainForIndexWithShape(ics_info.window_sequence, ics_info.window_shape, index, shape);
    }
}

pub fn decodeFirstChannelPcmBlockAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_tail: ?[]const f32,
    bytes: []const u8,
) !FirstChannelPcmBlock {
    var dequantized = try dequantizeFirstChannelSpectralCoefficientsAlloc(allocator, sample_rate, bytes);
    defer dequantized.deinit();

    return decodeFirstChannelPcmBlockFromDequantizedAlloc(allocator, previous_tail, dequantized);
}

fn decodeSingleChannelPcmBlockAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_tail: ?[]const f32,
    bytes: []const u8,
) !FirstChannelPcmBlock {
    return decodeSingleChannelPcmBlockWithExpectedLayoutAlloc(allocator, sample_rate, previous_tail, bytes, null);
}

fn decodeSingleChannelPcmBlockWithExpectedLayoutAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_tail: ?[]const f32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
) !FirstChannelPcmBlock {
    var predictor_states = [_]PredictorState{.{}} ** max_predictors;
    resetAllPredictors(&predictor_states);
    return decodeSingleChannelPcmBlockWithExpectedLayoutAndPredictorsAlloc(
        allocator,
        sample_rate,
        previous_tail,
        bytes,
        expected_layout,
        &predictor_states,
    );
}

fn decodeSingleChannelPcmBlockWithExpectedLayoutAndPredictorsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_tail: ?[]const f32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
    predictor_states: *[max_predictors]PredictorState,
) !FirstChannelPcmBlock {
    return decodeSingleChannelPcmBlockWithExpectedLayoutAndPredictorsAllocWithShape(
        allocator,
        sample_rate,
        previous_tail,
        bytes,
        expected_layout,
        predictor_states,
        null,
        null,
        FrameShape.default(),
    );
}

fn decodeSingleChannelPcmBlockWithExpectedLayoutAndPredictorsAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_tail: ?[]const f32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
    predictor_states: *[max_predictors]PredictorState,
    plans: ?*const AacImdctPlans,
    scratch: ?*AacDecodeScratch,
    shape: FrameShape,
) !FirstChannelPcmBlock {
    const state_started = perfNowNs();
    var state = try initFirstChannelSpectralStateAllocWithShape(allocator, sample_rate, bytes, shape);
    perfAccumulate(.spectral_state_init_ns, state_started);
    defer state.deinit();
    if (state.element_kind != .sce and state.element_kind != .lfe) return error.UnsupportedAudioFormat;
    if (expected_layout) |layout| {
        if (!layout.matchesFirstElement(1, .{
            .kind = state.element_kind,
            .element_instance_tag = state.element_instance_tag,
        })) return error.UnsupportedAudioFormat;
    }

    var dequantized = try dequantizeFirstChannelSpectralStateAllocWithShape(allocator, sample_rate, &state, predictor_states, shape);
    defer dequantized.deinit();
    const trailing_started = perfNowNs();
    var trailing_info = state.trailing_info;
    trailing_info.mergeTrailing(try scanSupportedTrailingElements(&state.reader));
    perfAccumulate(.trailing_validate_ns, trailing_started);
    const block_started = perfNowNs();
    var block = try decodeFirstChannelPcmBlockFromDequantizedWithScratchAlloc(allocator, scratch, previous_tail, dequantized, plans, shape);
    block.trailing_info = trailing_info;
    perfAccumulate(.pcm_block_ns, block_started);
    return block;
}

fn decodeFirstChannelPcmBlockFromDequantizedAlloc(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    dequantized: DequantizedSpectralCoefficients,
) !FirstChannelPcmBlock {
    return decodeFirstChannelPcmBlockFromDequantizedWithShapeAlloc(
        allocator,
        previous_tail,
        dequantized,
        null,
        FrameShape.default(),
    );
}

fn decodeFirstChannelPcmBlockFromDequantizedWithShapeAlloc(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    dequantized: DequantizedSpectralCoefficients,
    plans: ?*const AacImdctPlans,
    shape: FrameShape,
) !FirstChannelPcmBlock {
    return decodeFirstChannelPcmBlockFromDequantizedWithScratchAlloc(allocator, null, previous_tail, dequantized, plans, shape);
}

fn decodeFirstChannelPcmBlockFromDequantizedWithScratchAlloc(
    allocator: std.mem.Allocator,
    scratch: ?*AacDecodeScratch,
    previous_tail: ?[]const f32,
    dequantized: DequantizedSpectralCoefficients,
    plans: ?*const AacImdctPlans,
    shape: FrameShape,
) !FirstChannelPcmBlock {
    return switch (dequantized.ics_info.window_sequence) {
        .eight_short => blk: {
            if (scratch) |owned| {
                const out = try owned.ensureWindowedSamples(shape.long_window_samples);
                const block = try owned.ensureShortBlock(shape.short_window_samples);
                try composeEightShortWindowSequenceIntoWithShape(out, block, dequantized.coefficients, plans, owned, shape);
                var overlapped = try overlapAddShortWindowSequenceAllocWithShape(allocator, previous_tail, out, shape);
                errdefer overlapped.deinit();

                break :blk .{
                    .ics_info = dequantized.ics_info,
                    .pcm = overlapped.pcm,
                    .tail = overlapped.tail,
                    .contains_noise = dequantized.contains_noise,
                    .contains_intensity = dequantized.contains_intensity,
                    .trailing_info = .{},
                    .allocator = allocator,
                };
            } else {
                var windowed = try composeEightShortWindowSequenceAllocWithShape(allocator, dequantized.coefficients, plans, shape);
                defer windowed.deinit();
                var overlapped = try overlapAddShortWindowSequenceAllocWithShape(allocator, previous_tail, windowed.samples, shape);
                errdefer overlapped.deinit();

                break :blk .{
                    .ics_info = dequantized.ics_info,
                    .pcm = overlapped.pcm,
                    .tail = overlapped.tail,
                    .contains_noise = dequantized.contains_noise,
                    .contains_intensity = dequantized.contains_intensity,
                    .trailing_info = .{},
                    .allocator = allocator,
                };
            }
        },
        else => blk: {
            const samples = if (scratch) |owned|
                try owned.ensureWindowedSamples(shape.long_window_samples)
            else
                try allocator.alloc(f32, shape.long_window_samples);
            errdefer if (scratch == null) allocator.free(samples);
            try imdctLongIntoWithShapeAndScratch(samples, dequantized.coefficients, if (plans) |owned| &owned.long else null, scratch, shape);
            const overlapped = try overlapAddLongBlockFromImdctAllocWithShape(
                allocator,
                previous_tail,
                samples,
                dequantized.ics_info,
                if (plans) |owned| &owned.windows else null,
                shape,
            );
            if (scratch == null) allocator.free(samples);
            errdefer overlapped.deinit();

            break :blk .{
                .ics_info = dequantized.ics_info,
                .pcm = overlapped.pcm,
                .tail = overlapped.tail,
                .contains_noise = dequantized.contains_noise,
                .contains_intensity = dequantized.contains_intensity,
                .trailing_info = .{},
                .allocator = allocator,
            };
        },
    };
}

fn coeffOffsetsForIcsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    ics_info: IcsInfo,
) !struct {
    offsets: []const u16,
    raw_swb_offsets: []const u16,
    owned: ?[]u16,
} {
    const result = try coeffOffsetsForIcsAllocWithShape(allocator, sample_rate, ics_info, FrameShape.default());
    return .{
        .offsets = result.offsets,
        .raw_swb_offsets = result.raw_swb_offsets,
        .owned = result.owned,
    };
}

fn coeffOffsetsForIcsAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    ics_info: IcsInfo,
    shape: FrameShape,
) !struct {
    offsets: []const u16,
    raw_swb_offsets: []const u16,
    owned: ?[]u16,
} {
    if (ics_info.window_sequence == .eight_short) {
        const short_offsets = try swbOffsetsShort(sample_rate, shape);
        const grouped = try buildGroupedShortBandOffsetsAlloc(allocator, short_offsets, ics_info);
        return .{ .offsets = grouped, .raw_swb_offsets = short_offsets, .owned = grouped };
    }
    const long_offsets = try swbOffsetsLong(sample_rate, shape);
    if (long_offsets.len < @as(usize, ics_info.max_sfb) + 1) return error.UnsupportedAudioFormat;
    return .{
        .offsets = long_offsets[0 .. @as(usize, ics_info.max_sfb) + 1],
        .raw_swb_offsets = long_offsets,
        .owned = null,
    };
}

fn decodeWindowedPcmBlockAlloc(
    allocator: std.mem.Allocator,
    ics_info: IcsInfo,
    previous_tail: ?[]const f32,
    coefficients: []const f32,
) !ShortWindowPcmBlock {
    return decodeWindowedPcmBlockWithShapeAlloc(allocator, ics_info, previous_tail, coefficients, null, FrameShape.default());
}

fn decodeWindowedPcmBlockWithShapeAlloc(
    allocator: std.mem.Allocator,
    ics_info: IcsInfo,
    previous_tail: ?[]const f32,
    coefficients: []const f32,
    plans: ?*const AacImdctPlans,
    shape: FrameShape,
) !ShortWindowPcmBlock {
    return decodeWindowedPcmBlockWithScratchAlloc(allocator, null, ics_info, previous_tail, coefficients, plans, shape);
}

fn decodeWindowedPcmBlockWithScratchAlloc(
    allocator: std.mem.Allocator,
    scratch: ?*AacDecodeScratch,
    ics_info: IcsInfo,
    previous_tail: ?[]const f32,
    coefficients: []const f32,
    plans: ?*const AacImdctPlans,
    shape: FrameShape,
) !ShortWindowPcmBlock {
    perf_counters.channel_decode_count += 1;
    const started = perfNowNs();
    defer perfAccumulate(.filterbank_ns, started);
    return switch (ics_info.window_sequence) {
        .eight_short => blk: {
            if (scratch) |owned| {
                const out = try owned.ensureWindowedSamples(shape.long_window_samples);
                const block = try owned.ensureShortBlock(shape.short_window_samples);
                try composeEightShortWindowSequenceIntoWithShape(out, block, coefficients, plans, owned, shape);
                const overlap_started = perfNowNs();
                const overlapped = try overlapAddShortWindowSequenceAllocWithShape(allocator, previous_tail, out, shape);
                perfAccumulate(.filterbank_overlap_ns, overlap_started);
                break :blk overlapped;
            } else {
                var windowed = try composeEightShortWindowSequenceAllocWithShape(allocator, coefficients, plans, shape);
                defer windowed.deinit();
                const overlap_started = perfNowNs();
                const overlapped = try overlapAddShortWindowSequenceAllocWithShape(allocator, previous_tail, windowed.samples, shape);
                perfAccumulate(.filterbank_overlap_ns, overlap_started);
                break :blk overlapped;
            }
        },
        else => blk: {
            const samples = if (scratch) |owned|
                try owned.ensureWindowedSamples(shape.long_window_samples)
            else
                try allocator.alloc(f32, shape.long_window_samples);
            errdefer if (scratch == null) allocator.free(samples);
            const imdct_started = perfNowNs();
            try imdctLongIntoWithShapeAndScratch(samples, coefficients, if (plans) |owned| &owned.long else null, scratch, shape);
            perfAccumulate(.filterbank_imdct_ns, imdct_started);
            const overlap_started = perfNowNs();
            const overlapped = try overlapAddLongBlockFromImdctAllocWithShape(
                allocator,
                previous_tail,
                samples,
                ics_info,
                if (plans) |owned| &owned.windows else null,
                shape,
            );
            perfAccumulate(.filterbank_overlap_ns, overlap_started);
            if (scratch == null) allocator.free(samples);
            break :blk .{
                .pcm = overlapped.pcm,
                .tail = overlapped.tail,
                .allocator = allocator,
            };
        },
    };
}

pub fn decodeChannelPairDequantizedCoefficientsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
) !ChannelPairDequantizedCoefficients {
    return decodeChannelPairDequantizedCoefficientsWithExpectedLayoutAlloc(allocator, sample_rate, bytes, null);
}

fn decodeChannelPairDequantizedCoefficientsWithExpectedLayoutAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
) !ChannelPairDequantizedCoefficients {
    var left_predictor_states = [_]PredictorState{.{}} ** max_predictors;
    var right_predictor_states = [_]PredictorState{.{}} ** max_predictors;
    resetAllPredictors(&left_predictor_states);
    resetAllPredictors(&right_predictor_states);
    return decodeChannelPairDequantizedCoefficientsWithExpectedLayoutAndPredictorsAlloc(
        allocator,
        sample_rate,
        bytes,
        expected_layout,
        &left_predictor_states,
        &right_predictor_states,
    );
}

fn decodeChannelPairDequantizedCoefficientsWithExpectedLayoutAndPredictorsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
    left_predictor_states: *[max_predictors]PredictorState,
    right_predictor_states: *[max_predictors]PredictorState,
) !ChannelPairDequantizedCoefficients {
    return decodeChannelPairDequantizedCoefficientsWithExpectedLayoutAndPredictorsAllocWithShape(
        allocator,
        sample_rate,
        bytes,
        expected_layout,
        left_predictor_states,
        right_predictor_states,
        FrameShape.default(),
    );
}

fn decodeChannelPairDequantizedCoefficientsWithExpectedLayoutAndPredictorsAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
    left_predictor_states: *[max_predictors]PredictorState,
    right_predictor_states: *[max_predictors]PredictorState,
    shape: FrameShape,
) !ChannelPairDequantizedCoefficients {
    var reader = BitReader.init(bytes);
    var leading_info = TrailingElementInfo{};
    const first_element = try seekFirstChannelElementWithTrailingInfo(&reader, &leading_info);
    if (expected_layout) |layout| {
        if (!layout.matchesFirstElement(2, first_element)) return error.UnsupportedAudioFormat;
    }
    if (first_element.kind == .sce) {
        return decodeIndependentScePairDequantizedCoefficientsAllocWithShape(
            allocator,
            sample_rate,
            &reader,
            expected_layout,
            leading_info,
            left_predictor_states,
            right_predictor_states,
            shape,
        );
    }
    if (first_element.kind != .cpe) return error.UnsupportedAudioFormat;

    const common_window = (try reader.readBits(u1, 1)) != 0;

    var left_ics_info: IcsInfo = undefined;
    var right_ics_info: IcsInfo = undefined;
    const ms_mask = blk: {
        if (common_window) {
            left_ics_info = try parseIcsInfo(&reader, sample_rate);
            right_ics_info = left_ics_info;

            const ms_present = try reader.readBits(u2, 2);
            const band_count = @as(usize, left_ics_info.max_sfb) * left_ics_info.num_window_groups;
            const mask = try allocator.alloc(bool, band_count);
            errdefer allocator.free(mask);
            switch (ms_present) {
                0 => @memset(mask, false),
                1 => {
                    for (mask) |*bit| bit.* = (try reader.readBits(u1, 1)) != 0;
                },
                2 => @memset(mask, true),
                else => return error.UnsupportedAudioFormat,
            }
            break :blk mask;
        }

        const mask = try allocator.alloc(bool, 0);
        errdefer allocator.free(mask);
        break :blk mask;
    };
    errdefer allocator.free(ms_mask);

    var pns_state = INITIAL_PNS_STATE;

    const left_channel = try parseChannelReader(&reader);
    if (!common_window) left_ics_info = try parseIcsInfo(&reader, sample_rate);
    const left_offsets_started = perfNowNs();
    const left_offsets = try coeffOffsetsForIcsAllocWithShape(allocator, sample_rate, left_ics_info, shape);
    perfAccumulate(.coeff_offsets_ns, left_offsets_started);
    defer if (left_offsets.owned) |owned| allocator.free(owned);
    var left = try decodeChannelDequantizedAllocWithShape(
        allocator,
        sample_rate,
        &reader,
        left_ics_info,
        left_channel.global_gain,
        left_offsets.offsets,
        left_offsets.raw_swb_offsets,
        &pns_state,
        .{ .predictor_states = left_predictor_states },
        shape,
    );
    defer left.deinit();

    const right_channel = try parseChannelReader(&reader);
    if (!common_window) right_ics_info = try parseIcsInfo(&reader, sample_rate);
    const right_offsets_started = perfNowNs();
    const right_offsets = try coeffOffsetsForIcsAllocWithShape(allocator, sample_rate, right_ics_info, shape);
    perfAccumulate(.coeff_offsets_ns, right_offsets_started);
    defer if (right_offsets.owned) |owned| allocator.free(owned);
    var right = try decodeChannelDequantizedAllocWithShape(
        allocator,
        sample_rate,
        &reader,
        right_ics_info,
        right_channel.global_gain,
        right_offsets.offsets,
        right_offsets.raw_swb_offsets,
        &pns_state,
        .{ .predictor_states = right_predictor_states },
        shape,
    );
    defer right.deinit();

    const trailing_started = perfNowNs();
    var trailing_info = leading_info;
    trailing_info.mergeTrailing(try scanSupportedTrailingElements(&reader));
    perfAccumulate(.trailing_validate_ns, trailing_started);

    if (common_window) {
        applyMsStereo(left.coefficients, right.coefficients, left.bands, right.bands, ms_mask, left_offsets.offsets);
    }
    applyIntensityStereo(left.coefficients, right.coefficients, left.bands, right.bands, ms_mask, left_offsets.offsets);

    const coeff_copy_started = perfNowNs();
    const left_out = try allocator.dupe(f32, left.coefficients);
    errdefer allocator.free(left_out);
    const right_out = try allocator.dupe(f32, right.coefficients);
    errdefer allocator.free(right_out);
    perfAccumulate(.coeff_copy_ns, coeff_copy_started);

    return .{
        .common_window = common_window,
        .left_ics_info = left_ics_info,
        .right_ics_info = right_ics_info,
        .ms_mask = ms_mask,
        .left = left_out,
        .right = right_out,
        .trailing_info = trailing_info,
        .allocator = allocator,
    };
}

fn decodeIndependentScePairDequantizedCoefficientsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
    expected_layout: ?ProgramConfigLayout,
    leading_info: TrailingElementInfo,
    left_predictor_states: *[max_predictors]PredictorState,
    right_predictor_states: *[max_predictors]PredictorState,
) !ChannelPairDequantizedCoefficients {
    return decodeIndependentScePairDequantizedCoefficientsAllocWithShape(
        allocator,
        sample_rate,
        reader,
        expected_layout,
        leading_info,
        left_predictor_states,
        right_predictor_states,
        FrameShape.default(),
    );
}

fn decodeIndependentScePairDequantizedCoefficientsAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
    expected_layout: ?ProgramConfigLayout,
    leading_info: TrailingElementInfo,
    left_predictor_states: *[max_predictors]PredictorState,
    right_predictor_states: *[max_predictors]PredictorState,
    shape: FrameShape,
) !ChannelPairDequantizedCoefficients {
    var pns_state = INITIAL_PNS_STATE;

    const left_global_gain = try reader.readBits(u8, 8);
    const left_ics_info = try parseIcsInfo(reader, sample_rate);
    const left_offsets_started = perfNowNs();
    const left_offsets = try coeffOffsetsForIcsAllocWithShape(allocator, sample_rate, left_ics_info, shape);
    perfAccumulate(.coeff_offsets_ns, left_offsets_started);
    defer if (left_offsets.owned) |owned| allocator.free(owned);
    var left = try decodeChannelDequantizedAllocWithShape(
        allocator,
        sample_rate,
        reader,
        left_ics_info,
        left_global_gain,
        left_offsets.offsets,
        left_offsets.raw_swb_offsets,
        &pns_state,
        .{ .predictor_states = left_predictor_states },
        shape,
    );
    defer left.deinit();

    var trailing_info = leading_info;
    const second_element = try seekFirstChannelElementWithTrailingInfo(reader, &trailing_info);
    if (second_element.kind != .sce) return error.UnsupportedAudioFormat;
    if (expected_layout) |layout| {
        if (!layout.matchesSecondStereoSce(second_element)) return error.UnsupportedAudioFormat;
    }

    const right_global_gain = try reader.readBits(u8, 8);
    const right_ics_info = try parseIcsInfo(reader, sample_rate);
    const right_offsets_started = perfNowNs();
    const right_offsets = try coeffOffsetsForIcsAllocWithShape(allocator, sample_rate, right_ics_info, shape);
    perfAccumulate(.coeff_offsets_ns, right_offsets_started);
    defer if (right_offsets.owned) |owned| allocator.free(owned);
    var right = try decodeChannelDequantizedAllocWithShape(
        allocator,
        sample_rate,
        reader,
        right_ics_info,
        right_global_gain,
        right_offsets.offsets,
        right_offsets.raw_swb_offsets,
        &pns_state,
        .{ .predictor_states = right_predictor_states },
        shape,
    );
    defer right.deinit();

    const trailing_started = perfNowNs();
    trailing_info.mergeTrailing(try scanSupportedTrailingElements(reader));
    perfAccumulate(.trailing_validate_ns, trailing_started);

    const ms_mask = try allocator.alloc(bool, 0);
    errdefer allocator.free(ms_mask);
    const coeff_copy_started = perfNowNs();
    const left_out = try allocator.dupe(f32, left.coefficients);
    errdefer allocator.free(left_out);
    const right_out = try allocator.dupe(f32, right.coefficients);
    errdefer allocator.free(right_out);
    perfAccumulate(.coeff_copy_ns, coeff_copy_started);

    return .{
        .common_window = false,
        .left_ics_info = left_ics_info,
        .right_ics_info = right_ics_info,
        .ms_mask = ms_mask,
        .left = left_out,
        .right = right_out,
        .trailing_info = trailing_info,
        .allocator = allocator,
    };
}

pub fn decodeChannelPairPcmBlockAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_left_tail: ?[]const f32,
    previous_right_tail: ?[]const f32,
    bytes: []const u8,
) !ChannelPairPcmBlock {
    return decodeChannelPairPcmBlockWithExpectedLayoutAlloc(
        allocator,
        sample_rate,
        previous_left_tail,
        previous_right_tail,
        bytes,
        null,
    );
}

fn decodeChannelPairPcmBlockWithExpectedLayoutAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_left_tail: ?[]const f32,
    previous_right_tail: ?[]const f32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
) !ChannelPairPcmBlock {
    var left_predictor_states = [_]PredictorState{.{}} ** max_predictors;
    var right_predictor_states = [_]PredictorState{.{}} ** max_predictors;
    resetAllPredictors(&left_predictor_states);
    resetAllPredictors(&right_predictor_states);
    return decodeChannelPairPcmBlockWithExpectedLayoutAndPredictorsAlloc(
        allocator,
        sample_rate,
        previous_left_tail,
        previous_right_tail,
        bytes,
        expected_layout,
        &left_predictor_states,
        &right_predictor_states,
    );
}

fn decodeChannelPairPcmBlockWithExpectedLayoutAndPredictorsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_left_tail: ?[]const f32,
    previous_right_tail: ?[]const f32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
    left_predictor_states: *[max_predictors]PredictorState,
    right_predictor_states: *[max_predictors]PredictorState,
) !ChannelPairPcmBlock {
    return decodeChannelPairPcmBlockWithExpectedLayoutAndPredictorsAllocWithShape(
        allocator,
        sample_rate,
        previous_left_tail,
        previous_right_tail,
        bytes,
        expected_layout,
        left_predictor_states,
        right_predictor_states,
        null,
        null,
        FrameShape.default(),
    );
}

fn decodeChannelPairPcmBlockWithExpectedLayoutAndPredictorsAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    previous_left_tail: ?[]const f32,
    previous_right_tail: ?[]const f32,
    bytes: []const u8,
    expected_layout: ?ProgramConfigLayout,
    left_predictor_states: *[max_predictors]PredictorState,
    right_predictor_states: *[max_predictors]PredictorState,
    plans: ?*const AacImdctPlans,
    scratch: ?*AacDecodeScratch,
    shape: FrameShape,
) !ChannelPairPcmBlock {
    var pair = try decodeChannelPairDequantizedCoefficientsWithExpectedLayoutAndPredictorsAllocWithShape(
        allocator,
        sample_rate,
        bytes,
        expected_layout,
        left_predictor_states,
        right_predictor_states,
        shape,
    );
    defer pair.deinit();

    const left = try decodeWindowedPcmBlockWithScratchAlloc(
        allocator,
        scratch,
        pair.left_ics_info,
        previous_left_tail,
        pair.left,
        plans,
        shape,
    );
    errdefer {
        allocator.free(left.pcm);
        allocator.free(left.tail);
    }
    const right = try decodeWindowedPcmBlockWithScratchAlloc(
        allocator,
        scratch,
        pair.right_ics_info,
        previous_right_tail,
        pair.right,
        plans,
        shape,
    );
    errdefer {
        allocator.free(right.pcm);
        allocator.free(right.tail);
    }
    const ms_mask = try allocator.dupe(bool, pair.ms_mask);
    errdefer allocator.free(ms_mask);

    return .{
        .common_window = pair.common_window,
        .left_ics_info = pair.left_ics_info,
        .right_ics_info = pair.right_ics_info,
        .ms_mask = ms_mask,
        .left_pcm = left.pcm,
        .right_pcm = right.pcm,
        .left_tail = left.tail,
        .right_tail = right.tail,
        .trailing_info = pair.trailing_info,
        .allocator = allocator,
    };
}

pub fn decodeChannelPairPcmSequenceAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
) !ChannelPairPcmSequence {
    return decodeChannelPairPcmSequenceWithExpectedLayoutAlloc(allocator, sample_rate, access_units, null);
}

fn decodeChannelPairPcmSequenceWithExpectedLayoutAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
    expected_layout: ?ProgramConfigLayout,
) !ChannelPairPcmSequence {
    return decodeChannelPairPcmSequenceWithExpectedLayoutAllocAndShape(
        allocator,
        sample_rate,
        access_units,
        expected_layout,
        FrameShape.default(),
    );
}

const ChannelPairPcmSequenceDecode = struct {
    sequence: ChannelPairPcmSequence,
    trailing_infos: []TrailingElementInfo,
};

fn decodeChannelPairPcmSequenceWithExpectedLayoutAllocAndShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
    expected_layout: ?ProgramConfigLayout,
    shape: FrameShape,
) !ChannelPairPcmSequence {
    const decoded = try decodeChannelPairPcmSequenceWithExpectedLayoutAllocAndShapeMaybeTrailingInfos(
        allocator,
        sample_rate,
        access_units,
        expected_layout,
        shape,
        false,
    );
    return decoded.sequence;
}

fn decodeChannelPairPcmSequenceWithExpectedLayoutAllocAndShapeMaybeTrailingInfos(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
    expected_layout: ?ProgramConfigLayout,
    shape: FrameShape,
    collect_trailing_infos: bool,
) !ChannelPairPcmSequenceDecode {
    const plans = try sharedAacImdctPlans(shape);

    var interleaved = std.ArrayList(f32).empty;
    defer interleaved.deinit(allocator);
    try interleaved.ensureTotalCapacityPrecise(allocator, access_units.len * shape.pcm_samples * 2);
    const trailing_infos: []TrailingElementInfo = if (collect_trailing_infos)
        try allocator.alloc(TrailingElementInfo, access_units.len)
    else
        &.{};
    errdefer if (collect_trailing_infos) allocator.free(trailing_infos);

    var left_tail: ?[]f32 = null;
    defer if (left_tail) |tail| allocator.free(tail);
    var right_tail: ?[]f32 = null;
    defer if (right_tail) |tail| allocator.free(tail);
    var left_predictor_states = [_]PredictorState{.{}} ** max_predictors;
    var right_predictor_states = [_]PredictorState{.{}} ** max_predictors;
    resetAllPredictors(&left_predictor_states);
    resetAllPredictors(&right_predictor_states);
    var scratch = AacDecodeScratch{ .allocator = allocator };
    defer scratch.deinit();

    for (access_units, 0..) |unit, unit_index| {
        const unit_decode_started = perfNowNs();
        const block = try decodeChannelPairPcmBlockWithExpectedLayoutAndPredictorsAllocWithShape(
            allocator,
            sample_rate,
            left_tail,
            right_tail,
            unit,
            expected_layout,
            &left_predictor_states,
            &right_predictor_states,
            plans,
            &scratch,
            shape,
        );
        perfAccumulate(.sequence_unit_decode_ns, unit_decode_started);
        if (collect_trailing_infos) trailing_infos[unit_index] = block.trailing_info;

        const output_started = perfNowNs();
        if (left_tail) |tail| {
            allocator.free(tail);
            left_tail = null;
        }
        if (right_tail) |tail| {
            allocator.free(tail);
            right_tail = null;
        }
        left_tail = block.left_tail;
        right_tail = block.right_tail;

        for (block.left_pcm, block.right_pcm) |left, right| {
            interleaved.appendAssumeCapacity(left);
            interleaved.appendAssumeCapacity(right);
        }
        allocator.free(block.ms_mask);
        allocator.free(block.left_pcm);
        allocator.free(block.right_pcm);
        perfAccumulate(.sequence_output_ns, output_started);
    }

    return .{
        .sequence = .{
            .samples = try interleaved.toOwnedSlice(allocator),
            .sample_rate = sample_rate,
            .frame_count = access_units.len,
            .allocator = allocator,
        },
        .trailing_infos = trailing_infos,
    };
}

pub fn decodeFirstChannelPcmSequenceAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
) !FirstChannelPcmSequence {
    return decodeFirstChannelPcmSequenceWithExpectedLayoutAlloc(allocator, sample_rate, access_units, null);
}

fn decodeFirstChannelPcmSequenceWithExpectedLayoutAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
    expected_layout: ?ProgramConfigLayout,
) !FirstChannelPcmSequence {
    return decodeFirstChannelPcmSequenceWithExpectedLayoutAllocAndShape(
        allocator,
        sample_rate,
        access_units,
        expected_layout,
        FrameShape.default(),
    );
}

const FirstChannelPcmSequenceDecode = struct {
    sequence: FirstChannelPcmSequence,
    trailing_infos: []TrailingElementInfo,
};

fn decodeFirstChannelPcmSequenceWithExpectedLayoutAllocAndShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
    expected_layout: ?ProgramConfigLayout,
    shape: FrameShape,
) !FirstChannelPcmSequence {
    const decoded = try decodeFirstChannelPcmSequenceWithExpectedLayoutAllocAndShapeMaybeTrailingInfos(
        allocator,
        sample_rate,
        access_units,
        expected_layout,
        shape,
        false,
    );
    return decoded.sequence;
}

fn decodeFirstChannelPcmSequenceWithExpectedLayoutAllocAndShapeMaybeTrailingInfos(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
    expected_layout: ?ProgramConfigLayout,
    shape: FrameShape,
    collect_trailing_infos: bool,
) !FirstChannelPcmSequenceDecode {
    const plans = try sharedAacImdctPlans(shape);

    var pcm = std.ArrayList(f32).empty;
    defer pcm.deinit(allocator);
    try pcm.ensureTotalCapacityPrecise(allocator, access_units.len * shape.pcm_samples);
    const trailing_infos: []TrailingElementInfo = if (collect_trailing_infos)
        try allocator.alloc(TrailingElementInfo, access_units.len)
    else
        &.{};
    errdefer if (collect_trailing_infos) allocator.free(trailing_infos);

    var tail: ?[]f32 = null;
    defer if (tail) |owned| allocator.free(owned);
    var predictor_states = [_]PredictorState{.{}} ** max_predictors;
    resetAllPredictors(&predictor_states);
    var scratch = AacDecodeScratch{ .allocator = allocator };
    defer scratch.deinit();

    for (access_units, 0..) |unit, unit_index| {
        const unit_decode_started = perfNowNs();
        const block = try decodeSingleChannelPcmBlockWithExpectedLayoutAndPredictorsAllocWithShape(
            allocator,
            sample_rate,
            tail,
            unit,
            expected_layout,
            &predictor_states,
            plans,
            &scratch,
            shape,
        );
        perfAccumulate(.sequence_unit_decode_ns, unit_decode_started);
        if (collect_trailing_infos) trailing_infos[unit_index] = block.trailing_info;

        const output_started = perfNowNs();
        if (block.contains_intensity) {
            allocator.free(block.pcm);
            allocator.free(block.tail);
            return error.UnsupportedAudioFormat;
        }

        if (tail) |owned| {
            allocator.free(owned);
            tail = null;
        }
        tail = block.tail;

        pcm.appendSliceAssumeCapacity(block.pcm);
        allocator.free(block.pcm);
        perfAccumulate(.sequence_output_ns, output_started);
    }

    return .{
        .sequence = .{
            .samples = try pcm.toOwnedSlice(allocator),
            .sample_rate = sample_rate,
            .frame_count = access_units.len,
            .allocator = allocator,
        },
        .trailing_infos = trailing_infos,
    };
}

fn upsampleLinearInterleavedAlloc(
    allocator: std.mem.Allocator,
    samples: []const f32,
    channels: usize,
) ![]f32 {
    if (channels == 0 or samples.len % channels != 0) return error.UnsupportedAudioFormat;
    const frame_count = samples.len / channels;
    const out = try allocator.alloc(f32, samples.len * 2);
    errdefer allocator.free(out);

    for (0..frame_count) |frame_index| {
        for (0..channels) |channel_index| {
            const current = samples[frame_index * channels + channel_index];
            const next = if (frame_index + 1 < frame_count)
                samples[(frame_index + 1) * channels + channel_index]
            else
                current;
            out[(frame_index * 2) * channels + channel_index] = current;
            out[(frame_index * 2 + 1) * channels + channel_index] = (current + next) * 0.5;
        }
    }
    return out;
}

fn upsampleAndEnhanceSbrInterleavedAlloc(
    allocator: std.mem.Allocator,
    samples: []const f32,
    channels: usize,
    trailing_info: TrailingElementInfo,
) ![]f32 {
    const upsampled = try upsampleLinearInterleavedAlloc(allocator, samples, channels);
    errdefer allocator.free(upsampled);

    const reference = try allocator.dupe(f32, upsampled);
    defer allocator.free(reference);

    const frame_count = upsampled.len / channels;
    const carry_attenuation: f32 = switch (trailing_info.sbr_carry_generations) {
        0 => 1.0,
        1 => 0.78,
        2 => 0.58,
        else => 0.42,
    };
    const payload_strength = @min(
        @as(f32, 0.55),
        (0.16 +
            @as(f32, @floatFromInt(trailing_info.max_payload_len)) * 0.012 +
            @as(f32, @floatFromInt(trailing_info.envelope_hint)) / 255.0 * 0.2) * carry_attenuation,
    );
    const noise_strength = (0.04 + @as(f32, @floatFromInt(trailing_info.noise_hint)) / 255.0 * 0.14) * carry_attenuation;
    const harmonic_mix = (0.05 + @as(f32, @floatFromInt(trailing_info.harmonic_hint)) / 255.0 * 0.18) * carry_attenuation;
    const detail_mix = (0.02 + @as(f32, @floatFromInt(trailing_info.detail_hint)) / 255.0 * 0.12) * carry_attenuation;
    const tilt = 0.7 + @as(f32, @floatFromInt(trailing_info.envelope_hint & 0x0f)) / 15.0 * 0.8;
    const stereo_skew = @as(f32, @floatFromInt(trailing_info.stereo_hint)) / 255.0 * 0.16 * carry_attenuation;
    const phase_offset = trailing_info.phase_hint & 0x03;
    const payload_phase = if ((trailing_info.payload_hash & 1) == 0) @as(f32, 1.0) else @as(f32, -1.0);
    for (0..channels) |channel_index| {
        for (0..frame_count) |frame_index| {
            const idx = frame_index * channels + channel_index;
            const current = reference[idx];
            const previous = if (frame_index > 0)
                reference[(frame_index - 1) * channels + channel_index]
            else
                current;
            const next = if (frame_index + 1 < frame_count)
                reference[(frame_index + 1) * channels + channel_index]
            else
                current;
            const curvature = next - (2.0 * current) + previous;
            const derivative = next - previous;
            const normalized_position = if (frame_count > 1)
                @as(f32, @floatFromInt(frame_index)) / @as(f32, @floatFromInt(frame_count - 1))
            else
                0;
            const phase: f32 = if ((frame_index & 1) == 0) payload_phase else -payload_phase;
            const ripple_phase: f32 = switch ((frame_index + phase_offset) & 0x03) {
                0 => 1.0,
                1 => 0.35,
                2 => -0.8,
                else => -0.2,
            };
            const frame_tilt = 1.0 + (normalized_position - 0.5) * tilt;
            const channel_tilt = if (channels == 2 and channel_index == 1) 1.0 + stereo_skew else 1.0 - stereo_skew;
            upsampled[idx] = current +
                curvature * payload_strength * phase * frame_tilt * channel_tilt +
                curvature * detail_mix * ripple_phase * channel_tilt +
                derivative * noise_strength * -phase +
                current * harmonic_mix * 0.08 * frame_tilt;
        }
    }
    return upsampled;
}

fn psWideningFactor(trailing_info: TrailingElementInfo) f32 {
    return 1.02 + @as(f32, @floatFromInt(psStereoHint(trailing_info))) / 255.0 * 0.45;
}

fn shouldApplyExplicitHeSbr(config: AudioSpecificConfig, trailing_info: TrailingElementInfo) bool {
    return isExplicitHeObjectType(config.object_type) and trailing_info.explicit_payload_active;
}

fn shouldApplyExplicitHePs(config: AudioSpecificConfig, trailing_info: TrailingElementInfo) bool {
    return shouldApplyExplicitHeSbr(config, trailing_info) and
        config.ps_present and
        trailing_info.explicit_ps_payload_active;
}

fn shouldApplySyncExtensionSbr(config: AudioSpecificConfig, trailing_info: TrailingElementInfo) bool {
    return (config.sbr_present or config.ps_present) and trailing_info.saw_sbr_payload;
}

fn shouldApplySyncExtensionPs(config: AudioSpecificConfig, trailing_info: TrailingElementInfo) bool {
    return config.ps_present and trailing_info.saw_ps_payload;
}

fn psPayloadMaxLen(trailing_info: TrailingElementInfo) u16 {
    return if (trailing_info.saw_ps_payload) trailing_info.ps_max_payload_len else trailing_info.max_payload_len;
}

fn psPayloadHash(trailing_info: TrailingElementInfo) u32 {
    return if (trailing_info.saw_ps_payload) trailing_info.ps_payload_hash else trailing_info.payload_hash;
}

fn psNoiseHint(trailing_info: TrailingElementInfo) u8 {
    return if (trailing_info.saw_ps_payload) trailing_info.ps_noise_hint else trailing_info.noise_hint;
}

fn psStereoHint(trailing_info: TrailingElementInfo) u8 {
    return if (trailing_info.saw_ps_payload) trailing_info.ps_stereo_hint else trailing_info.stereo_hint;
}

fn psHarmonicHint(trailing_info: TrailingElementInfo) u8 {
    return if (trailing_info.saw_ps_payload) trailing_info.ps_harmonic_hint else trailing_info.harmonic_hint;
}

fn psDetailHint(trailing_info: TrailingElementInfo) u8 {
    return if (trailing_info.saw_ps_payload) trailing_info.ps_detail_hint else trailing_info.detail_hint;
}

fn psPhaseHint(trailing_info: TrailingElementInfo) u8 {
    return if (trailing_info.saw_ps_payload) trailing_info.ps_phase_hint else trailing_info.phase_hint;
}

fn psCarryAttenuation(trailing_info: TrailingElementInfo) f32 {
    return switch (trailing_info.ps_carry_generations) {
        0 => 1.0,
        1 => 0.72,
        2 => 0.5,
        else => 0.35,
    };
}

fn applyParametricStereoWideningInterleavedInPlace(samples: []f32, trailing_info: TrailingElementInfo) !void {
    if (samples.len % 2 != 0) return error.UnsupportedAudioFormat;
    const frame_count = samples.len / 2;
    var previous_mid: f32 = if (frame_count > 0) 0.5 * (samples[0] + samples[1]) else 0;
    const carry_attenuation = psCarryAttenuation(trailing_info);
    const decorrelation_scale = 0.16 +
        @as(f32, @floatFromInt(psPayloadMaxLen(trailing_info))) * 0.008 +
        @as(f32, @floatFromInt(psNoiseHint(trailing_info))) / 255.0 * 0.14 +
        @as(f32, @floatFromInt(psDetailHint(trailing_info))) / 255.0 * 0.05;
    const width = psWideningFactor(trailing_info);
    const stereo_bias = ((@as(f32, @floatFromInt(psStereoHint(trailing_info) & 0x0f)) / 15.0 - 0.5) * 0.2) +
        ((@as(f32, @floatFromInt(psPhaseHint(trailing_info) & 0x0f)) / 15.0 - 0.5) * 0.08);
    const harmonic_mix = 0.02 +
        @as(f32, @floatFromInt(psHarmonicHint(trailing_info))) / 255.0 * 0.06 +
        @as(f32, @floatFromInt(psDetailHint(trailing_info))) / 255.0 * 0.03;
    const payload_phase = if ((psPayloadHash(trailing_info) & 1) == 0) @as(f32, 1.0) else @as(f32, -1.0);

    for (0..frame_count) |frame_index| {
        const left = samples[frame_index * 2];
        const right = samples[frame_index * 2 + 1];
        const mid = 0.5 * (left + right);
        const side = 0.5 * (left - right);
        const phase: f32 = if ((frame_index & 1) == 0) payload_phase * decorrelation_scale else -payload_phase * decorrelation_scale;
        const decorrelated = (mid - previous_mid) * phase * carry_attenuation;
        const widened_side = (side * width + decorrelated + mid * harmonic_mix * carry_attenuation) * carry_attenuation;
        samples[frame_index * 2] = mid + widened_side * (1.0 + stereo_bias * carry_attenuation);
        samples[frame_index * 2 + 1] = mid - widened_side * (1.0 - stereo_bias * carry_attenuation);
        previous_mid = mid;
    }
}

fn synthesizeParametricStereoFromMonoAlloc(
    allocator: std.mem.Allocator,
    mono_samples: []const f32,
    trailing_info: TrailingElementInfo,
) ![]f32 {
    const stereo = try allocator.alloc(f32, mono_samples.len * 2);
    errdefer allocator.free(stereo);
    const carry_attenuation = psCarryAttenuation(trailing_info);
    const decorrelation_scale = 0.16 +
        @as(f32, @floatFromInt(psPayloadMaxLen(trailing_info))) * 0.008 +
        @as(f32, @floatFromInt(psNoiseHint(trailing_info))) / 255.0 * 0.14 +
        @as(f32, @floatFromInt(psDetailHint(trailing_info))) / 255.0 * 0.05;
    const width = psWideningFactor(trailing_info);
    const stereo_bias = ((@as(f32, @floatFromInt(psStereoHint(trailing_info) & 0x0f)) / 15.0 - 0.5) * 0.2) +
        ((@as(f32, @floatFromInt(psPhaseHint(trailing_info) & 0x0f)) / 15.0 - 0.5) * 0.08);
    const harmonic_mix = 0.02 +
        @as(f32, @floatFromInt(psHarmonicHint(trailing_info))) / 255.0 * 0.06 +
        @as(f32, @floatFromInt(psDetailHint(trailing_info))) / 255.0 * 0.03;
    const payload_phase = if ((psPayloadHash(trailing_info) & 1) == 0) @as(f32, 1.0) else @as(f32, -1.0);

    for (mono_samples, 0..) |mid, frame_index| {
        const previous = if (frame_index > 0) mono_samples[frame_index - 1] else mid;
        const next = if (frame_index + 1 < mono_samples.len) mono_samples[frame_index + 1] else mid;
        const phase: f32 = if ((frame_index & 1) == 0) payload_phase * decorrelation_scale else -payload_phase * decorrelation_scale;
        const decorrelated = ((next - previous) * phase + mid * harmonic_mix) * carry_attenuation;
        stereo[frame_index * 2] = mid + decorrelated * width * (1.0 + stereo_bias * carry_attenuation);
        stereo[frame_index * 2 + 1] = mid - decorrelated * width * (1.0 - stereo_bias * carry_attenuation);
    }

    return stereo;
}

fn applyExplicitHeStereoPostprocessAlloc(
    allocator: std.mem.Allocator,
    config: AudioSpecificConfig,
    trailing_infos: []const TrailingElementInfo,
    sequence: ChannelPairPcmSequence,
) !ChannelPairPcmSequence {
    if (sequence.frame_count == 0) return error.UnsupportedAudioFormat;
    if (trailing_infos.len != sequence.frame_count) return error.UnsupportedAudioFormat;
    const frame_stride = sequence.samples.len / sequence.frame_count;
    if (frame_stride * sequence.frame_count != sequence.samples.len) return error.UnsupportedAudioFormat;
    if (frame_stride % 2 != 0) return error.UnsupportedAudioFormat;

    var needs_processing = false;
    for (trailing_infos) |trailing_info| {
        const should_enhance = shouldApplyExplicitHeSbr(config, trailing_info) or
            shouldApplySyncExtensionSbr(config, trailing_info);
        if (should_enhance) {
            needs_processing = true;
            break;
        }
    }
    if (!needs_processing) return sequence;

    const target_rate = config.extension_sample_rate orelse return error.UnsupportedAudioFormat;

    if (target_rate == sequence.sample_rate) {
        var frame_index: usize = 0;
        while (frame_index < sequence.frame_count) : (frame_index += 1) {
            const trailing_info = trailing_infos[frame_index];
            const should_enhance = shouldApplyExplicitHeSbr(config, trailing_info) or
                shouldApplySyncExtensionSbr(config, trailing_info);
            if (!should_enhance) continue;
            if (shouldApplyExplicitHePs(config, trailing_info) or shouldApplySyncExtensionPs(config, trailing_info)) {
                const block = sequence.samples[frame_index * frame_stride ..][0..frame_stride];
                try applyParametricStereoWideningInterleavedInPlace(block, trailing_info);
            }
        }
        return sequence;
    }
    if (target_rate != sequence.sample_rate * 2) return error.UnsupportedAudioFormat;

    const upsampled = try allocator.alloc(f32, sequence.samples.len * 2);
    errdefer allocator.free(upsampled);
    const output_frame_stride = frame_stride * 2;
    var frame_index: usize = 0;
    while (frame_index < sequence.frame_count) : (frame_index += 1) {
        const trailing_info = trailing_infos[frame_index];
        const should_enhance = shouldApplyExplicitHeSbr(config, trailing_info) or
            shouldApplySyncExtensionSbr(config, trailing_info);
        const in_block = sequence.samples[frame_index * frame_stride ..][0..frame_stride];
        const out_block = upsampled[frame_index * output_frame_stride ..][0..output_frame_stride];

        if (!should_enhance) {
            const linear = try upsampleLinearInterleavedAlloc(allocator, in_block, 2);
            defer allocator.free(linear);
            @memcpy(out_block, linear);
            continue;
        }

        const enhanced = try upsampleAndEnhanceSbrInterleavedAlloc(allocator, in_block, 2, trailing_info);
        defer allocator.free(enhanced);
        @memcpy(out_block, enhanced);
        if (shouldApplyExplicitHePs(config, trailing_info) or shouldApplySyncExtensionPs(config, trailing_info)) {
            try applyParametricStereoWideningInterleavedInPlace(out_block, trailing_info);
        }
    }

    sequence.allocator.free(sequence.samples);
    return .{
        .samples = upsampled,
        .sample_rate = target_rate,
        .frame_count = sequence.frame_count,
        .allocator = allocator,
    };
}

fn applyExplicitHeMonoPostprocessAlloc(
    allocator: std.mem.Allocator,
    config: AudioSpecificConfig,
    trailing_infos: []const TrailingElementInfo,
    sequence: FirstChannelPcmSequence,
) !FirstChannelPcmSequence {
    if (sequence.frame_count == 0) return error.UnsupportedAudioFormat;
    if (trailing_infos.len != sequence.frame_count) return error.UnsupportedAudioFormat;
    const frame_stride = sequence.samples.len / sequence.frame_count;
    if (frame_stride * sequence.frame_count != sequence.samples.len) return error.UnsupportedAudioFormat;

    var needs_processing = false;
    for (trailing_infos) |trailing_info| {
        const should_enhance = shouldApplyExplicitHeSbr(config, trailing_info) or
            shouldApplySyncExtensionSbr(config, trailing_info);
        if (should_enhance) {
            needs_processing = true;
            break;
        }
    }
    if (!needs_processing) return sequence;

    const target_rate = config.extension_sample_rate orelse return error.UnsupportedAudioFormat;

    if (target_rate == sequence.sample_rate) return sequence;
    if (target_rate != sequence.sample_rate * 2) return error.UnsupportedAudioFormat;

    const upsampled = try allocator.alloc(f32, sequence.samples.len * 2);
    errdefer allocator.free(upsampled);
    const output_frame_stride = frame_stride * 2;
    var frame_index: usize = 0;
    while (frame_index < sequence.frame_count) : (frame_index += 1) {
        const trailing_info = trailing_infos[frame_index];
        const should_enhance = shouldApplyExplicitHeSbr(config, trailing_info) or
            shouldApplySyncExtensionSbr(config, trailing_info);
        const in_block = sequence.samples[frame_index * frame_stride ..][0..frame_stride];
        const out_block = upsampled[frame_index * output_frame_stride ..][0..output_frame_stride];

        if (!should_enhance) {
            const linear = try upsampleLinearInterleavedAlloc(allocator, in_block, 1);
            defer allocator.free(linear);
            @memcpy(out_block, linear);
            continue;
        }

        const enhanced = try upsampleAndEnhanceSbrInterleavedAlloc(allocator, in_block, 1, trailing_info);
        defer allocator.free(enhanced);
        @memcpy(out_block, enhanced);
    }

    sequence.allocator.free(sequence.samples);
    return .{
        .samples = upsampled,
        .sample_rate = target_rate,
        .frame_count = sequence.frame_count,
        .allocator = allocator,
    };
}

fn applyExplicitHePsMonoCoreStereoPostprocessAlloc(
    allocator: std.mem.Allocator,
    config: AudioSpecificConfig,
    trailing_infos: []const TrailingElementInfo,
    sequence: FirstChannelPcmSequence,
) !ChannelPairPcmSequence {
    if (sequence.frame_count == 0) return error.UnsupportedAudioFormat;
    if (trailing_infos.len != sequence.frame_count) return error.UnsupportedAudioFormat;
    if (!config.ps_present) return error.UnsupportedAudioFormat;

    const target_rate = config.extension_sample_rate orelse return error.UnsupportedAudioFormat;
    const frame_stride = sequence.samples.len / sequence.frame_count;
    if (frame_stride * sequence.frame_count != sequence.samples.len) return error.UnsupportedAudioFormat;

    var needs_processing = false;
    for (trailing_infos) |trailing_info| {
        const should_enhance = shouldApplyExplicitHePs(config, trailing_info) or
            shouldApplySyncExtensionPs(config, trailing_info);
        if (should_enhance) {
            needs_processing = true;
            break;
        }
    }
    if (!needs_processing) return error.UnsupportedAudioFormat;

    const output_rate_same = target_rate == sequence.sample_rate;
    if (!output_rate_same and target_rate != sequence.sample_rate * 2) return error.UnsupportedAudioFormat;

    const stereo_frame_stride = frame_stride * 2 * (if (output_rate_same) @as(usize, 1) else 2);
    const stereo = try allocator.alloc(f32, stereo_frame_stride * sequence.frame_count);
    errdefer allocator.free(stereo);

    var frame_index: usize = 0;
    while (frame_index < sequence.frame_count) : (frame_index += 1) {
        const trailing_info = trailing_infos[frame_index];
        const should_ps = shouldApplyExplicitHePs(config, trailing_info) or
            shouldApplySyncExtensionPs(config, trailing_info);
        const should_sbr = shouldApplyExplicitHeSbr(config, trailing_info) or
            shouldApplySyncExtensionSbr(config, trailing_info);

        const in_block = sequence.samples[frame_index * frame_stride ..][0..frame_stride];
        const mono_block = if (output_rate_same)
            try allocator.dupe(f32, in_block)
        else if (should_sbr)
            try upsampleAndEnhanceSbrInterleavedAlloc(allocator, in_block, 1, trailing_info)
        else
            try upsampleLinearInterleavedAlloc(allocator, in_block, 1);
        defer allocator.free(mono_block);

        const out_block = stereo[frame_index * stereo_frame_stride ..][0..stereo_frame_stride];
        if (!should_ps) {
            for (mono_block, 0..) |sample, sample_index| {
                out_block[sample_index * 2] = sample;
                out_block[sample_index * 2 + 1] = sample;
            }
            continue;
        }

        const stereo_block = try synthesizeParametricStereoFromMonoAlloc(allocator, mono_block, trailing_info);
        defer allocator.free(stereo_block);
        @memcpy(out_block, stereo_block);
    }

    sequence.allocator.free(sequence.samples);
    return .{
        .samples = stereo,
        .sample_rate = target_rate,
        .frame_count = sequence.frame_count,
        .allocator = allocator,
    };
}

pub fn decodeInterleavedStereoAdtsAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) !ChannelPairPcmSequence {
    const frames = try scanAdtsFramesAlloc(allocator, bytes);
    defer allocator.free(frames);

    const decode_config = try adtsDecodeConfig(allocator, frames, 2);
    var access_units = try collectAdtsAccessUnitsAlloc(allocator, frames, decode_config.sample_rate);
    defer access_units.deinit();
    if (access_units.units.items.len == 0) return error.UnsupportedAudioFormat;
    return decodeChannelPairPcmSequenceWithExpectedLayoutAlloc(allocator, decode_config.sample_rate, access_units.units.items, decode_config.expected_layout);
}

pub fn decodeInterleavedMonoAdtsAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
) !FirstChannelPcmSequence {
    const frames = try scanAdtsFramesAlloc(allocator, bytes);
    defer allocator.free(frames);

    const decode_config = try adtsDecodeConfig(allocator, frames, 1);
    var access_units = try collectAdtsAccessUnitsAlloc(allocator, frames, decode_config.sample_rate);
    defer access_units.deinit();
    if (access_units.units.items.len == 0) return error.UnsupportedAudioFormat;
    return decodeFirstChannelPcmSequenceWithExpectedLayoutAlloc(allocator, decode_config.sample_rate, access_units.units.items, decode_config.expected_layout);
}

fn configNeedsEnhancementTrailingInfos(config: AudioSpecificConfig) bool {
    return isExplicitHeObjectType(config.object_type) or config.sbr_present or config.ps_present;
}

pub fn decodeInterleavedStereoAccessUnitsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    channels: u16,
    decoder_config: []const u8,
    access_units: []const []const u8,
) !ChannelPairPcmSequence {
    const config_started = perfNowNs();
    const config = try parseAudioSpecificConfig(decoder_config);
    perfAccumulate(.config_parse_ns, config_started);
    const core_sample_rate = config.sample_rate;
    const shape = FrameShape.forFrameLength960(config.frame_length_960);
    const collect_started = perfNowNs();
    var filtered_access_units = try collectChannelAccessUnitsAlloc(allocator, core_sample_rate, config.explicit_layout, access_units);
    perfAccumulate(.access_unit_collect_ns, collect_started);
    defer filtered_access_units.deinit();
    if (filtered_access_units.units.items.len == 0) return error.UnsupportedAudioFormat;
    perf_counters.access_unit_count += filtered_access_units.units.items.len;
    if (isSupportedPsMonoCoreStereoConfig(config, sample_rate, channels)) {
        const sequence_started = perfNowNs();
        var sequence_decode = try decodeFirstChannelPcmSequenceWithExpectedLayoutAllocAndShapeMaybeTrailingInfos(
            allocator,
            core_sample_rate,
            filtered_access_units.units.items,
            config.explicit_layout,
            shape,
            true,
        );
        perfAccumulate(.filterbank_sequence_ns, sequence_started);
        var sequence_owned = true;
        errdefer if (sequence_owned) sequence_decode.sequence.deinit();
        defer allocator.free(sequence_decode.trailing_infos);

        const enhancement_started = perfNowNs();
        const trailing_infos = try resolveEnhancementTrailingInfosAlloc(allocator, config, sequence_decode.trailing_infos);
        perfAccumulate(.enhancement_info_ns, enhancement_started);
        defer allocator.free(trailing_infos);
        const postprocess_started = perfNowNs();
        const out = try applyExplicitHePsMonoCoreStereoPostprocessAlloc(allocator, config, trailing_infos, sequence_decode.sequence);
        sequence_owned = false;
        perfAccumulate(.postprocess_ns, postprocess_started);
        return out;
    }
    if (!isSupportedMainOrLcConfig(config, sample_rate, channels, 2)) return error.UnsupportedAudioFormat;
    const sequence_started = perfNowNs();
    var sequence_decode = try decodeChannelPairPcmSequenceWithExpectedLayoutAllocAndShapeMaybeTrailingInfos(
        allocator,
        core_sample_rate,
        filtered_access_units.units.items,
        config.explicit_layout,
        shape,
        configNeedsEnhancementTrailingInfos(config),
    );
    perfAccumulate(.filterbank_sequence_ns, sequence_started);
    if (!configNeedsEnhancementTrailingInfos(config)) return sequence_decode.sequence;
    var sequence_owned = true;
    errdefer if (sequence_owned) sequence_decode.sequence.deinit();
    defer allocator.free(sequence_decode.trailing_infos);

    const enhancement_started = perfNowNs();
    const trailing_infos = try resolveEnhancementTrailingInfosAlloc(allocator, config, sequence_decode.trailing_infos);
    perfAccumulate(.enhancement_info_ns, enhancement_started);
    defer allocator.free(trailing_infos);
    const postprocess_started = perfNowNs();
    const out = try applyExplicitHeStereoPostprocessAlloc(allocator, config, trailing_infos, sequence_decode.sequence);
    sequence_owned = false;
    perfAccumulate(.postprocess_ns, postprocess_started);
    return out;
}

pub fn decodeInterleavedMonoAccessUnitsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    channels: u16,
    decoder_config: []const u8,
    access_units: []const []const u8,
) !FirstChannelPcmSequence {
    const config_started = perfNowNs();
    const config = try parseAudioSpecificConfig(decoder_config);
    perfAccumulate(.config_parse_ns, config_started);
    const core_sample_rate = config.sample_rate;
    if (!isSupportedMainOrLcConfig(config, sample_rate, channels, 1)) return error.UnsupportedAudioFormat;
    const shape = FrameShape.forFrameLength960(config.frame_length_960);
    const collect_started = perfNowNs();
    var filtered_access_units = try collectChannelAccessUnitsAlloc(allocator, core_sample_rate, config.explicit_layout, access_units);
    perfAccumulate(.access_unit_collect_ns, collect_started);
    defer filtered_access_units.deinit();
    if (filtered_access_units.units.items.len == 0) return error.UnsupportedAudioFormat;
    perf_counters.access_unit_count += filtered_access_units.units.items.len;
    const sequence_started = perfNowNs();
    var sequence_decode = try decodeFirstChannelPcmSequenceWithExpectedLayoutAllocAndShapeMaybeTrailingInfos(
        allocator,
        core_sample_rate,
        filtered_access_units.units.items,
        config.explicit_layout,
        shape,
        configNeedsEnhancementTrailingInfos(config),
    );
    perfAccumulate(.filterbank_sequence_ns, sequence_started);
    if (!configNeedsEnhancementTrailingInfos(config)) return sequence_decode.sequence;
    var sequence_owned = true;
    errdefer if (sequence_owned) sequence_decode.sequence.deinit();
    defer allocator.free(sequence_decode.trailing_infos);

    const enhancement_started = perfNowNs();
    const trailing_infos = try resolveEnhancementTrailingInfosAlloc(allocator, config, sequence_decode.trailing_infos);
    perfAccumulate(.enhancement_info_ns, enhancement_started);
    defer allocator.free(trailing_infos);
    const postprocess_started = perfNowNs();
    const out = try applyExplicitHeMonoPostprocessAlloc(allocator, config, trailing_infos, sequence_decode.sequence);
    sequence_owned = false;
    perfAccumulate(.postprocess_ns, postprocess_started);
    return out;
}

fn isSupportedPsMonoCoreStereoConfig(config: AudioSpecificConfig, sample_rate: u32, channels: u16) bool {
    const config_channels = config.explicit_channel_count orelse config.channel_config;
    const base_object_type = configBaseObjectType(config);
    const declared_output_rate = config.extension_sample_rate orelse configDeclaredOutputSampleRate(config);
    return (base_object_type == 1 or base_object_type == 2) and
        config.ps_present and
        config.extension_sample_rate != null and
        config_channels == 1 and
        (if (config.explicit_layout) |layout| layout.matchesSupportedOutput(1) else true) and
        !config.depends_on_core_coder and
        channels == 2 and
        declared_output_rate == sample_rate;
}

fn isSupportedMainOrLcConfig(config: AudioSpecificConfig, sample_rate: u32, channels: u16, expected_channels: u8) bool {
    const config_channels = config.explicit_channel_count orelse config.channel_config;
    return supportsDecodableMainLcCoreConfig(config) and
        config_channels == expected_channels and
        (if (config.explicit_layout) |layout| layout.matchesSupportedOutput(expected_channels) else true) and
        !config.depends_on_core_coder and
        channels == expected_channels and
        configDeclaredOutputSampleRate(config) == sample_rate;
}

fn supportsDecodableMainLcCoreConfig(config: AudioSpecificConfig) bool {
    const base_object_type = configBaseObjectType(config);
    if (base_object_type != 1 and base_object_type != 2) return false;
    if (isExplicitHeObjectType(config.object_type)) {
        return config.extension_sample_rate != null and config.sbr_present;
    }
    if (config.ps_present) return false;
    if (!config.sbr_present) return true;

    // Some real low-bitrate MP4/M4A files carry an AAC-LC core with an SBR sync
    // extension while the packetized core remains directly decodable at the
    // container sample rate. Accept only that narrow shape here; actual HE-AAC
    // reconstruction is still outside this decoder.
    return config.object_type == 2 and config.extension_object_type == 5;
}

const AdtsDecodeConfig = struct {
    sample_rate: u32,
    expected_layout: ?ProgramConfigLayout,
};

fn adtsDecodeConfig(
    allocator: std.mem.Allocator,
    frames: []const AdtsFrame,
    expected_channels: u8,
) !AdtsDecodeConfig {
    if (frames.len == 0) return error.UnsupportedAudioFormat;

    const sample_rate = frames[0].header.sample_rate;
    for (frames) |frame| {
        if (frame.header.object_type != 1 and frame.header.object_type != 2) return error.UnsupportedAudioFormat;
        if (frame.header.sample_rate != sample_rate) return error.UnsupportedAudioFormat;
    }

    const expected_layout = try adtsExpectedLayout(allocator, frames, sample_rate, expected_channels);
    for (frames) |frame| {
        if (!adtsFrameMatchesChannelCount(frame, expected_channels, expected_layout)) return error.UnsupportedAudioFormat;
    }

    return .{
        .sample_rate = sample_rate,
        .expected_layout = expected_layout,
    };
}

fn adtsFrameMatchesChannelCount(frame: AdtsFrame, expected_channels: u8, expected_layout: ?ProgramConfigLayout) bool {
    if (frame.header.channel_config == expected_channels) return true;
    if (frame.header.channel_config != 0) return false;
    return expected_layout != null;
}

fn adtsExpectedLayout(
    allocator: std.mem.Allocator,
    frames: []const AdtsFrame,
    sample_rate: u32,
    expected_channels: u8,
) !?ProgramConfigLayout {
    var expected_layout: ?ProgramConfigLayout = null;
    for (frames) |frame| {
        if (frame.header.channel_config != 0) continue;
        const layout = try inferAdtsFrameProgramConfigLayoutAlloc(
            allocator,
            sample_rate,
            frame,
            expected_layout == null,
        ) orelse continue;
        if (layout.channel_count != expected_channels or !layout.matchesSupportedOutput(expected_channels)) return error.UnsupportedAudioFormat;
        if (expected_layout) |existing| {
            if (!std.meta.eql(existing, layout)) return error.UnsupportedAudioFormat;
        } else {
            expected_layout = layout;
        }
    }
    return expected_layout;
}

fn inferAdtsFrameProgramConfigLayoutAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    frame: AdtsFrame,
    require_layout_before_channel: bool,
) !?ProgramConfigLayout {
    if (frame.header.data_blocks_in_frame == 0) {
        return inferRawDataBlockProgramConfigLayoutAlloc(
            allocator,
            sample_rate,
            frame.payload,
            require_layout_before_channel,
        );
    }

    var frame_layout: ?ProgramConfigLayout = null;
    const block_count = @as(usize, frame.header.data_blocks_in_frame) + 1;
    var start_bit: usize = 0;
    for (0..block_count) |_| {
        const end_bit = try supportedRawDataBlockEndBit(allocator, sample_rate, frame.payload, start_bit);
        const block = try copyBitRangeAlloc(allocator, frame.payload, start_bit, end_bit);
        defer allocator.free(block);

        const layout = try inferRawDataBlockProgramConfigLayoutAlloc(
            allocator,
            sample_rate,
            block,
            require_layout_before_channel and frame_layout == null,
        );
        if (layout) |found| {
            if (frame_layout) |existing| {
                if (!std.meta.eql(existing, found)) return error.UnsupportedAudioFormat;
            } else {
                frame_layout = found;
            }
        }

        start_bit = end_bit;
        if (!frame.header.protection_absent) start_bit = checkedBitOffsetAfterRawDataBlockCrc(frame.payload, start_bit) catch return error.UnsupportedAudioFormat;
    }
    if (!trailingBitsAreZero(frame.payload, start_bit)) return error.UnsupportedAudioFormat;
    return frame_layout;
}

fn inferRawDataBlockProgramConfigLayoutAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
    require_layout_before_channel: bool,
) !?ProgramConfigLayout {
    var reader = BitReader.init(bytes);
    var layout: ?ProgramConfigLayout = null;

    while (reader.bit_offset < reader.bytes.len * 8) {
        if (trailingBitsAreZero(reader.bytes, reader.bit_offset)) return layout;
        if (reader.bytes.len * 8 - reader.bit_offset < 3) return error.UnsupportedAudioFormat;

        const kind = @as(ElementKind, @enumFromInt(try reader.readBits(u3, 3)));
        switch (kind) {
            .sce, .lfe => {
                if (require_layout_before_channel and layout == null) return error.UnsupportedAudioFormat;
                _ = try reader.readBits(u8, 4);
                try skipSingleChannelElementPayload(allocator, sample_rate, &reader);
            },
            .cpe => {
                if (require_layout_before_channel and layout == null) return error.UnsupportedAudioFormat;
                _ = try reader.readBits(u8, 4);
                try skipChannelPairElementPayload(allocator, sample_rate, &reader);
            },
            .dse => {
                _ = try reader.readBits(u8, 4);
                try skipDataStreamElement(&reader);
            },
            .pce => {
                const found = try parseProgramConfigElementWithTag(&reader);
                if (layout) |existing| {
                    if (!std.meta.eql(existing, found)) return error.UnsupportedAudioFormat;
                } else {
                    layout = found;
                }
            },
            .fil => try skipFillElement(&reader),
            .end => {
                if (!trailingBitsAreZero(reader.bytes, reader.bit_offset)) return error.UnsupportedAudioFormat;
                return layout;
            },
            .cce => {
                _ = try reader.readBits(u8, 4);
                return error.UnsupportedAudioFormat;
            },
        }
    }
    return layout;
}

const ChannelAccessUnits = struct {
    units: std.ArrayList([]const u8),
    owned: std.ArrayList([]u8),
    allocator: std.mem.Allocator,

    fn deinit(self: *ChannelAccessUnits) void {
        for (self.owned.items) |bytes| self.allocator.free(bytes);
        self.owned.deinit(self.allocator);
        self.units.deinit(self.allocator);
    }
};

fn collectChannelAccessUnitsAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    expected_layout: ?ProgramConfigLayout,
    access_units: []const []const u8,
) !ChannelAccessUnits {
    var channel_access_units = ChannelAccessUnits{
        .units = .empty,
        .owned = .empty,
        .allocator = allocator,
    };
    errdefer channel_access_units.deinit();

    for (access_units) |unit| {
        if (expected_layout) |expected| {
            const layout = try inferRawDataBlockProgramConfigLayoutAlloc(allocator, sample_rate, unit, false);
            if (layout) |found| {
                if (!std.meta.eql(expected, found)) return error.UnsupportedAudioFormat;
            }
        }
        if (try rawDataBlockHasChannelElement(unit)) {
            try channel_access_units.units.append(allocator, unit);
        }
    }

    return channel_access_units;
}

fn collectAdtsAccessUnitsAlloc(
    allocator: std.mem.Allocator,
    frames: []const AdtsFrame,
    sample_rate: u32,
) !ChannelAccessUnits {
    var access_units = ChannelAccessUnits{
        .units = .empty,
        .owned = .empty,
        .allocator = allocator,
    };
    errdefer access_units.deinit();

    for (frames) |frame| {
        if (frame.header.data_blocks_in_frame == 0) {
            if (try rawDataBlockHasChannelElement(frame.payload)) {
                try access_units.units.append(allocator, frame.payload);
            }
            continue;
        }

        const block_count = @as(usize, frame.header.data_blocks_in_frame) + 1;
        var start_bit: usize = 0;
        for (0..block_count) |_| {
            const end_bit = try supportedRawDataBlockEndBit(allocator, sample_rate, frame.payload, start_bit);
            const copied = try copyBitRangeAlloc(allocator, frame.payload, start_bit, end_bit);
            const has_channel = rawDataBlockHasChannelElement(copied) catch |err| {
                allocator.free(copied);
                return err;
            };
            if (has_channel) {
                access_units.units.ensureUnusedCapacity(allocator, 1) catch |err| {
                    allocator.free(copied);
                    return err;
                };
                access_units.owned.ensureUnusedCapacity(allocator, 1) catch |err| {
                    allocator.free(copied);
                    return err;
                };
                access_units.owned.appendAssumeCapacity(copied);
                access_units.units.appendAssumeCapacity(copied);
            } else {
                allocator.free(copied);
            }
            start_bit = end_bit;
            if (!frame.header.protection_absent) start_bit = checkedBitOffsetAfterRawDataBlockCrc(frame.payload, start_bit) catch return error.UnsupportedAudioFormat;
        }
        if (!trailingBitsAreZero(frame.payload, start_bit)) return error.UnsupportedAudioFormat;
    }

    return access_units;
}

fn rawDataBlockHasChannelElement(bytes: []const u8) !bool {
    var reader = BitReader.init(bytes);
    while (reader.bit_offset < reader.bytes.len * 8) {
        if (trailingBitsAreZero(reader.bytes, reader.bit_offset)) return false;
        if (reader.bytes.len * 8 - reader.bit_offset < 3) return error.UnsupportedAudioFormat;

        const kind = @as(ElementKind, @enumFromInt(try reader.readBits(u3, 3)));
        switch (kind) {
            .sce, .cpe, .lfe => return true,
            .dse => {
                _ = try reader.readBits(u8, 4);
                try skipDataStreamElement(&reader);
            },
            .pce => {
                _ = try reader.readBits(u8, 4);
                try skipProgramConfigElement(&reader);
            },
            .fil => try skipFillElement(&reader),
            .end => {
                if (!trailingBitsAreZero(reader.bytes, reader.bit_offset)) return error.UnsupportedAudioFormat;
                return false;
            },
            .cce => {
                _ = try reader.readBits(u8, 4);
                return error.UnsupportedAudioFormat;
            },
        }
    }
    return false;
}

fn checkedBitOffsetAfterRawDataBlockCrc(bytes: []const u8, bit_offset: usize) !usize {
    const next = bit_offset + 16;
    if (next > bytes.len * 8) return error.UnsupportedAudioFormat;
    return next;
}

fn supportedRawDataBlockEndBit(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
    start_bit: usize,
) !usize {
    var reader = BitReader.initFromBitOffset(bytes, start_bit);
    while (true) {
        const kind = @as(ElementKind, @enumFromInt(try reader.readBits(u3, 3)));
        switch (kind) {
            .sce, .lfe => {
                _ = try reader.readBits(u8, 4);
                try skipSingleChannelElementPayload(allocator, sample_rate, &reader);
            },
            .cpe => {
                _ = try reader.readBits(u8, 4);
                try skipChannelPairElementPayload(allocator, sample_rate, &reader);
            },
            .dse => {
                _ = try reader.readBits(u8, 4);
                try skipDataStreamElement(&reader);
            },
            .pce => {
                _ = try reader.readBits(u8, 4);
                try skipProgramConfigElement(&reader);
            },
            .fil => try skipFillElement(&reader),
            .cce => {
                _ = try reader.readBits(u8, 4);
                return error.UnsupportedAudioFormat;
            },
            .end => return reader.bit_offset,
        }
    }
}

fn skipSingleChannelElementPayload(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
) !void {
    const global_gain = try reader.readBits(u8, 8);
    const ics_info = try parseIcsInfo(reader, sample_rate);
    const offsets = try coeffOffsetsForIcsAlloc(allocator, sample_rate, ics_info);
    defer if (offsets.owned) |owned| allocator.free(owned);
    try skipChannelSpectralPayloadAlloc(
        allocator,
        sample_rate,
        reader,
        ics_info,
        global_gain,
        offsets.offsets,
        offsets.raw_swb_offsets,
    );
}

fn skipChannelPairElementPayload(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
) !void {
    const common_window = (try reader.readBits(u1, 1)) != 0;

    var left_ics_info: IcsInfo = undefined;
    var right_ics_info: IcsInfo = undefined;
    if (common_window) {
        left_ics_info = try parseIcsInfo(reader, sample_rate);
        right_ics_info = left_ics_info;

        const ms_present = try reader.readBits(u2, 2);
        const band_count = @as(usize, left_ics_info.max_sfb) * left_ics_info.num_window_groups;
        switch (ms_present) {
            0, 2 => {},
            1 => try reader.skipBits(band_count),
            else => return error.UnsupportedAudioFormat,
        }
    }

    const left_channel = try parseChannelReader(reader);
    if (!common_window) left_ics_info = try parseIcsInfo(reader, sample_rate);
    const left_offsets = try coeffOffsetsForIcsAlloc(allocator, sample_rate, left_ics_info);
    defer if (left_offsets.owned) |owned| allocator.free(owned);
    try skipChannelSpectralPayloadAlloc(
        allocator,
        sample_rate,
        reader,
        left_ics_info,
        left_channel.global_gain,
        left_offsets.offsets,
        left_offsets.raw_swb_offsets,
    );

    const right_channel = try parseChannelReader(reader);
    if (!common_window) right_ics_info = try parseIcsInfo(reader, sample_rate);
    const right_offsets = try coeffOffsetsForIcsAlloc(allocator, sample_rate, right_ics_info);
    defer if (right_offsets.owned) |owned| allocator.free(owned);
    try skipChannelSpectralPayloadAlloc(
        allocator,
        sample_rate,
        reader,
        right_ics_info,
        right_channel.global_gain,
        right_offsets.offsets,
        right_offsets.raw_swb_offsets,
    );
}

fn skipChannelSpectralPayloadAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
    ics_info: IcsInfo,
    global_gain: u8,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
) !void {
    return skipChannelSpectralPayloadAllocWithShape(
        allocator,
        sample_rate,
        reader,
        ics_info,
        global_gain,
        coeff_offsets,
        raw_swb_offsets,
        FrameShape.default(),
    );
}

fn skipChannelSpectralPayloadAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
    ics_info: IcsInfo,
    global_gain: u8,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
    shape: FrameShape,
) !void {
    _ = sample_rate;
    _ = raw_swb_offsets;
    const sections = try parseSectionDataAlloc(allocator, reader, ics_info);
    defer allocator.free(sections);
    const bands = try parseScalefactorBandsAlloc(allocator, reader, global_gain, ics_info, sections);
    defer allocator.free(bands);

    const pulse_present = (try reader.readBits(u1, 1)) != 0;
    if (pulse_present) _ = try parsePulseData(reader, ics_info);
    const tns_present = (try reader.readBits(u1, 1)) != 0;
    if (tns_present) _ = try parseTnsData(reader, ics_info, false);
    const gain_control_present = (try reader.readBits(u1, 1)) != 0;
    if (gain_control_present) return error.UnsupportedAudioFormat;

    try skipChannelSpectralData(reader, coeff_offsets, bands, spectralCoefficientCount(ics_info, shape), shape);
}

fn scanAccessUnitTrailingInfoAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
) !TrailingElementInfo {
    var reader = BitReader.init(bytes);
    var info = TrailingElementInfo{};

    while (reader.bit_offset < reader.bytes.len * 8) {
        if (trailingBitsAreZero(reader.bytes, reader.bit_offset)) return info;
        const kind = @as(ElementKind, @enumFromInt(try reader.readBits(u3, 3)));
        switch (kind) {
            .sce, .lfe => {
                _ = try reader.readBits(u8, 4);
                try skipSingleChannelElementPayload(allocator, sample_rate, &reader);
            },
            .cpe => {
                _ = try reader.readBits(u8, 4);
                try skipChannelPairElementPayload(allocator, sample_rate, &reader);
            },
            .dse => {
                _ = try reader.readBits(u8, 4);
                try skipDataStreamElement(&reader);
            },
            .pce => {
                _ = try reader.readBits(u8, 4);
                try skipProgramConfigElement(&reader);
            },
            .fil => info.merge(try parseFillElement(&reader)),
            .cce => {
                _ = try reader.readBits(u8, 4);
                return error.UnsupportedAudioFormat;
            },
            .end => return info,
        }
    }
    return info;
}

fn accessUnitsTrailingInfoAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
) !TrailingElementInfo {
    const infos = try accessUnitsTrailingInfosAlloc(allocator, sample_rate, access_units);
    defer allocator.free(infos);
    var info = TrailingElementInfo{};
    for (infos) |unit_info| {
        info.mergeTrailing(unit_info);
    }
    return info;
}

fn accessUnitsTrailingInfosAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    access_units: []const []const u8,
) ![]TrailingElementInfo {
    const infos = try allocator.alloc(TrailingElementInfo, access_units.len);
    errdefer allocator.free(infos);
    for (access_units, 0..) |unit, index| {
        infos[index] = try scanAccessUnitTrailingInfoAlloc(allocator, sample_rate, unit);
    }
    return infos;
}

fn resolveEnhancementTrailingInfosAlloc(
    allocator: std.mem.Allocator,
    config: AudioSpecificConfig,
    trailing_infos: []const TrailingElementInfo,
) ![]TrailingElementInfo {
    const resolved = try allocator.dupe(TrailingElementInfo, trailing_infos);
    errdefer allocator.free(resolved);

    if (!(config.sbr_present or config.ps_present)) return resolved;

    const explicit_he = isExplicitHeObjectType(config.object_type);
    var explicit_sbr_payload_seen = !explicit_he;
    var explicit_ps_payload_seen = !explicit_he or !config.ps_present;
    if (explicit_he) {
        for (trailing_infos) |info| {
            if (info.saw_sbr_payload) {
                explicit_sbr_payload_seen = true;
                break;
            }
        }
        explicit_sbr_payload_seen = !explicit_sbr_payload_seen;
        if (config.ps_present) {
            for (trailing_infos) |info| {
                if (info.saw_ps_payload) {
                    explicit_ps_payload_seen = true;
                    break;
                }
            }
            explicit_ps_payload_seen = !explicit_ps_payload_seen;
        }
    }

    var carried_sbr: ?TrailingElementInfo = null;
    var carried_ps: ?TrailingElementInfo = null;
    for (resolved) |*info| {
        if (!info.saw_sbr_payload) {
            if (carried_sbr) |previous| {
                info.* = previous;
                info.sbr_carry_generations = previous.sbr_carry_generations +| 1;
                if (info.saw_ps_payload) info.ps_carry_generations = previous.ps_carry_generations +| 1;
            }
        } else if (!info.saw_plain_sbr_payload) {
            if (carried_sbr) |previous| {
                info.max_payload_len = previous.max_payload_len;
                info.payload_hash = previous.payload_hash;
                info.envelope_hint = previous.envelope_hint;
                info.noise_hint = previous.noise_hint;
                info.stereo_hint = previous.stereo_hint;
                info.harmonic_hint = previous.harmonic_hint;
                info.detail_hint = previous.detail_hint;
                info.phase_hint = previous.phase_hint;
                info.sbr_carry_generations = previous.sbr_carry_generations +| 1;
            } else {
                info.sbr_carry_generations = 0;
            }
        } else {
            if (carried_sbr) |previous| carryMissingSbrSubfields(info, previous);
            info.sbr_carry_generations = 0;
        }
        if (config.ps_present and !info.saw_ps_payload and info.saw_sbr_payload) {
            if (carried_ps) |previous_ps| {
                info.saw_ps_payload = true;
                info.ps_carry_generations = previous_ps.ps_carry_generations +| 1;
                info.ps_max_payload_len = previous_ps.ps_max_payload_len;
                info.ps_payload_hash = previous_ps.ps_payload_hash;
                info.ps_noise_hint = previous_ps.ps_noise_hint;
                info.ps_stereo_hint = previous_ps.ps_stereo_hint;
                info.ps_harmonic_hint = previous_ps.ps_harmonic_hint;
                info.ps_detail_hint = previous_ps.ps_detail_hint;
                info.ps_phase_hint = previous_ps.ps_phase_hint;
            }
        } else if (info.saw_ps_payload) {
            if (carried_ps) |previous_ps| carryMissingPsSubfields(info, previous_ps);
            info.ps_carry_generations = 0;
        }
        if (explicit_he) {
            if (info.saw_sbr_payload) explicit_sbr_payload_seen = true;
            info.explicit_payload_active = explicit_sbr_payload_seen;
            if (config.ps_present) {
                if (info.saw_ps_payload) explicit_ps_payload_seen = true;
                info.explicit_ps_payload_active = explicit_ps_payload_seen;
            }
        }
        if (info.saw_ps_payload) {
            carried_ps = info.*;
        }
        if (info.saw_sbr_payload) {
            carried_sbr = info.*;
        }
    }
    return resolved;
}

fn copyBitRangeAlloc(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    start_bit: usize,
    end_bit: usize,
) ![]u8 {
    if (end_bit < start_bit or end_bit > bytes.len * 8) return error.UnsupportedAudioFormat;
    const bit_count = end_bit - start_bit;
    const out = try allocator.alloc(u8, (bit_count + 7) / 8);
    @memset(out, 0);
    errdefer allocator.free(out);

    for (0..bit_count) |i| {
        const src_bit = start_bit + i;
        const bit = (bytes[src_bit / 8] >> @intCast(7 - (src_bit % 8))) & 1;
        out[i / 8] |= bit << @intCast(7 - (i % 8));
    }
    return out;
}

fn trailingBitsAreZero(bytes: []const u8, start_bit: usize) bool {
    if (start_bit > bytes.len * 8) return false;
    var bit = start_bit;
    while (bit < bytes.len * 8) : (bit += 1) {
        if (((bytes[bit / 8] >> @intCast(7 - (bit % 8))) & 1) != 0) return false;
    }
    return true;
}

fn parseScalefactorBandsAlloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    global_gain: u8,
    ics_info: IcsInfo,
    sections: []const Section,
) ![]ScalefactorBand {
    const total_bands = @as(usize, ics_info.max_sfb) * ics_info.num_window_groups;
    var bands = try allocator.alloc(ScalefactorBand, total_bands);

    var spectral_gain: i16 = global_gain;
    var noise_gain: i16 = @as(i16, global_gain) - 90;
    var intensity_position: i16 = 0;
    var noise_flag = true;
    var band_index: usize = 0;

    for (sections) |section| {
        const run_len = @as(usize, section.end_sfb - section.start_sfb);
        switch (section.band_type) {
            ZERO_BT => {
                for (0..run_len) |_| {
                    bands[band_index] = .{
                        .band_type = section.band_type,
                        .kind = .zero,
                        .value = 0,
                    };
                    band_index += 1;
                }
            },
            INTENSITY_BT, INTENSITY_BT2 => {
                for (0..run_len) |_| {
                    intensity_position += @as(i16, try decodeScalefactorSymbol(reader)) - 60;
                    bands[band_index] = .{
                        .band_type = section.band_type,
                        .kind = .intensity,
                        .value = intensity_position,
                    };
                    band_index += 1;
                }
            },
            NOISE_BT => {
                for (0..run_len) |_| {
                    if (noise_flag) {
                        noise_gain += @as(i16, @intCast(try reader.readBits(u16, 9))) - 256;
                        noise_flag = false;
                    } else {
                        noise_gain += @as(i16, try decodeScalefactorSymbol(reader)) - 60;
                    }
                    bands[band_index] = .{
                        .band_type = section.band_type,
                        .kind = .noise,
                        .value = noise_gain,
                    };
                    band_index += 1;
                }
            },
            else => {
                for (0..run_len) |_| {
                    spectral_gain += @as(i16, try decodeScalefactorSymbol(reader)) - 60;
                    bands[band_index] = .{
                        .band_type = section.band_type,
                        .kind = .spectral,
                        .value = spectral_gain,
                    };
                    band_index += 1;
                }
            },
        }
    }

    return bands;
}

fn spectralCodebookClass(band_type: u4) SpectralCodebookClass {
    return switch (band_type) {
        0 => .zero,
        1, 2, 3, 4 => .quad,
        5, 6, 7, 8, 9, 10 => .pair,
        11 => .escape,
        13 => .noise,
        14, 15 => .intensity,
        else => .zero,
    };
}

const SpectralCodebookInfo = struct {
    dimensions: u8,
    unsigned_values: bool,
    uses_escape: bool,
};

fn spectralCodebookInfo(band_type: u4) SpectralCodebookInfo {
    return switch (band_type) {
        0 => .{ .dimensions = 0, .unsigned_values = false, .uses_escape = false },
        1, 2 => .{ .dimensions = 4, .unsigned_values = false, .uses_escape = false },
        3, 4 => .{ .dimensions = 4, .unsigned_values = true, .uses_escape = false },
        5, 6 => .{ .dimensions = 2, .unsigned_values = false, .uses_escape = false },
        7, 8, 9, 10 => .{ .dimensions = 2, .unsigned_values = true, .uses_escape = false },
        11 => .{ .dimensions = 2, .unsigned_values = true, .uses_escape = true },
        13, 14, 15 => .{ .dimensions = 0, .unsigned_values = false, .uses_escape = false },
        else => .{ .dimensions = 0, .unsigned_values = false, .uses_escape = false },
    };
}

fn aacSpectralCodebook(band_type: u4) !AacSpectralCodebook {
    return switch (band_type) {
        1 => .{
            .dimensions = 4,
            .unsigned_values = false,
            .uses_escape = false,
            .radix = 0,
            .codes = &aac_codes1,
            .bits = &aac_bits1,
            .lookup = &aac_lookup1,
        },
        2 => .{
            .dimensions = 4,
            .unsigned_values = false,
            .uses_escape = false,
            .radix = 3,
            .codes = &aac_codes2,
            .bits = &aac_bits2,
            .lookup = &aac_lookup2,
        },
        3 => .{
            .dimensions = 4,
            .unsigned_values = true,
            .uses_escape = false,
            .radix = 3,
            .codes = &aac_codes3,
            .bits = &aac_bits3,
            .lookup = &aac_lookup3,
        },
        4 => .{
            .dimensions = 4,
            .unsigned_values = true,
            .uses_escape = false,
            .radix = 3,
            .codes = &aac_codes4,
            .bits = &aac_bits4,
            .lookup = &aac_lookup4,
        },
        5 => .{
            .dimensions = 2,
            .unsigned_values = false,
            .uses_escape = false,
            .radix = 9,
            .codes = &aac_codes5,
            .bits = &aac_bits5,
            .lookup = &aac_lookup5,
        },
        6 => .{
            .dimensions = 2,
            .unsigned_values = false,
            .uses_escape = false,
            .radix = 9,
            .codes = &aac_codes6,
            .bits = &aac_bits6,
            .lookup = &aac_lookup6,
        },
        7 => .{
            .dimensions = 2,
            .unsigned_values = true,
            .uses_escape = false,
            .radix = 8,
            .codes = &aac_codes7,
            .bits = &aac_bits7,
            .lookup = &aac_lookup7,
        },
        8 => .{
            .dimensions = 2,
            .unsigned_values = true,
            .uses_escape = false,
            .radix = 8,
            .codes = &aac_codes8,
            .bits = &aac_bits8,
            .lookup = &aac_lookup8,
        },
        9 => .{
            .dimensions = 2,
            .unsigned_values = true,
            .uses_escape = false,
            .radix = 13,
            .codes = &aac_codes9,
            .bits = &aac_bits9,
            .lookup = &aac_lookup9,
        },
        10 => .{
            .dimensions = 2,
            .unsigned_values = true,
            .uses_escape = false,
            .radix = 13,
            .codes = &aac_codes10,
            .bits = &aac_bits10,
            .lookup = &aac_lookup10,
        },
        11 => .{
            .dimensions = 2,
            .unsigned_values = true,
            .uses_escape = true,
            .radix = 17,
            .codes = &aac_codes11,
            .bits = &aac_bits11,
            .lookup = &aac_lookup11,
        },
        else => error.UnsupportedAudioFormat,
    };
}

fn swbOffsets1024(sample_rate: u32) ![]const u16 {
    return switch (sample_rate) {
        96000, 88200 => &swb_offset_1024_96,
        64000 => &swb_offset_1024_64,
        48000, 44100 => &swb_offset_1024_48,
        32000 => &swb_offset_1024_32,
        24000, 22050 => &swb_offset_1024_24,
        16000, 12000, 11025 => &swb_offset_1024_16,
        8000, 7350 => &swb_offset_1024_8,
        else => error.UnsupportedAudioFormat,
    };
}

fn swbOffsets128(sample_rate: u32) ![]const u16 {
    return switch (sample_rate) {
        96000, 88200, 64000 => &swb_offset_128_96,
        48000, 44100, 32000 => &swb_offset_128_48,
        24000, 22050 => &swb_offset_128_24,
        16000, 12000, 11025 => &swb_offset_128_16,
        8000, 7350 => &swb_offset_128_8,
        else => error.UnsupportedAudioFormat,
    };
}

fn swbOffsetsLong(sample_rate: u32, shape: FrameShape) ![]const u16 {
    if (shape.long_coefficients == 960) {
        return switch (sample_rate) {
            96000, 88200 => &swb_offset_960_96,
            64000 => &swb_offset_960_64,
            48000, 44100 => &swb_offset_960_48,
            32000 => &swb_offset_960_32,
            24000, 22050 => &swb_offset_960_24,
            16000, 12000, 11025 => &swb_offset_960_16,
            8000, 7350 => &swb_offset_960_8,
            else => error.UnsupportedAudioFormat,
        };
    }
    return swbOffsets1024(sample_rate);
}

fn swbOffsetsShort(sample_rate: u32, shape: FrameShape) ![]const u16 {
    if (shape.short_coefficients == 120) {
        return switch (sample_rate) {
            96000, 88200, 64000 => &swb_offset_120_96,
            48000, 44100, 32000 => &swb_offset_120_48,
            24000, 22050 => &swb_offset_120_24,
            16000, 12000, 11025 => &swb_offset_120_16,
            8000, 7350 => &swb_offset_120_8,
            else => error.UnsupportedAudioFormat,
        };
    }
    return swbOffsets128(sample_rate);
}

fn buildGroupedShortBandOffsetsAlloc(
    allocator: std.mem.Allocator,
    swb_offsets: []const u16,
    ics_info: IcsInfo,
) ![]u16 {
    if (ics_info.window_sequence != .eight_short) return error.UnsupportedAudioFormat;
    if (swb_offsets.len < @as(usize, ics_info.max_sfb) + 1) return error.UnsupportedAudioFormat;

    var total_windows: u8 = 0;
    for (ics_info.window_group_length[0..ics_info.num_window_groups]) |group_len| {
        if (group_len == 0) return error.UnsupportedAudioFormat;
        total_windows += group_len;
    }
    if (total_windows != 8) return error.UnsupportedAudioFormat;

    const total_bands = @as(usize, ics_info.max_sfb) * ics_info.num_window_groups;
    const offsets = try allocator.alloc(u16, total_bands + 1);
    errdefer allocator.free(offsets);

    offsets[0] = 0;
    var coeff_offset: u16 = 0;
    var band_index: usize = 0;
    for (ics_info.window_group_length[0..ics_info.num_window_groups]) |group_len| {
        for (0..ics_info.max_sfb) |sfb| {
            const swb_start = swb_offsets[sfb];
            const swb_end = swb_offsets[sfb + 1];
            const width = (swb_end - swb_start) * group_len;
            coeff_offset += width;
            band_index += 1;
            offsets[band_index] = coeff_offset;
        }
    }
    return offsets;
}

fn buildSpectralLayoutsAlloc(
    allocator: std.mem.Allocator,
    bands: []const ScalefactorBand,
    coeff_offsets: []const u16,
) ![]SpectralBandLayout {
    if (coeff_offsets.len != bands.len + 1) return error.UnsupportedAudioFormat;

    const plans = try allocator.alloc(SpectralBandLayout, bands.len);
    errdefer allocator.free(plans);

    for (bands, plans, 0..) |band, *layout, i| {
        const info = spectralCodebookInfo(band.band_type);
        const coeff_start = coeff_offsets[i];
        const coeff_end = coeff_offsets[i + 1];
        const coeff_width = coeff_end - coeff_start;
        const band_class = spectralCodebookClass(band.band_type);
        const symbol_count: u16 = switch (band_class) {
            .zero, .noise, .intensity => 0,
            .quad => blk: {
                if (coeff_width % 4 != 0) return error.UnsupportedAudioFormat;
                break :blk coeff_width / 4;
            },
            .pair, .escape => blk: {
                if (coeff_width % 2 != 0) return error.UnsupportedAudioFormat;
                break :blk coeff_width / 2;
            },
        };
        layout.* = .{
            .band_type = band.band_type,
            .class = band_class,
            .dimensions = info.dimensions,
            .unsigned_values = info.unsigned_values,
            .uses_escape = info.uses_escape,
            .scalefactor_kind = band.kind,
            .scalefactor_value = band.value,
            .coeff_start = coeff_start,
            .coeff_end = coeff_end,
            .symbol_count = symbol_count,
        };
    }

    return plans;
}

fn decodeSpectralSymbol(
    reader: *BitReader,
    descriptor: SpectralCodebookDescriptor,
) !SpectralSymbol {
    const entry = try decodeSpectralHuffmanEntry(reader, descriptor.entries);
    var symbol = SpectralSymbol{
        .values = .{ 0, 0, 0, 0 },
        .dimensions = descriptor.dimensions,
    };
    for (0..descriptor.dimensions) |i| {
        var value: i16 = if (descriptor.uses_escape)
            try decodeEscapedSpectralValue(reader, entry.values[i])
        else
            entry.values[i];
        if (descriptor.unsigned_values and value != 0) {
            const negative = (try reader.readBits(u1, 1)) != 0;
            if (negative) value = -value;
        }
        symbol.values[i] = value;
    }
    return symbol;
}

fn decodeAacSpectralSymbol(reader: *BitReader, codebook: AacSpectralCodebook) !SpectralSymbol {
    const index = try decodeAacSpectralIndex(reader, codebook);
    const raw = if (codebook.dimensions == 4 and !codebook.uses_escape)
        unpackAacCodebook02Index(index, !codebook.unsigned_values)
    else
        unpackAacSpectralIndex(index, codebook.radix, codebook.dimensions);
    var symbol = SpectralSymbol{
        .values = .{ 0, 0, 0, 0 },
        .dimensions = codebook.dimensions,
    };
    var negative = [_]bool{false} ** 4;

    if (codebook.unsigned_values) {
        for (0..codebook.dimensions) |i| {
            if (raw[i] != 0) {
                negative[i] = (try reader.readBits(u1, 1)) != 0;
            }
        }
    }

    for (0..codebook.dimensions) |i| {
        var value: i16 = raw[i];
        if (codebook.uses_escape) {
            value = try decodeEscapedSpectralValue(reader, @intCast(value));
        }
        if (codebook.unsigned_values and value != 0 and negative[i]) {
            value = -value;
        }
        symbol.values[i] = value;
    }
    return symbol;
}

fn unpackAacCodebook02Index(index: usize, signed: bool) [4]i16 {
    const entry = codebook_vector02_idx[index];
    const vals = if (signed) &codebook_vector0_signed_vals else &codebook_vector0_unsigned_vals;
    return .{
        vals[@intCast((entry >> 0) & 0x3)],
        vals[@intCast((entry >> 2) & 0x3)],
        vals[@intCast((entry >> 4) & 0x3)],
        vals[@intCast((entry >> 6) & 0x3)],
    };
}

fn decodeEscapedSpectralValue(reader: *BitReader, magnitude: u8) !i16 {
    if (magnitude != 16) return magnitude;

    var bits: u8 = 4;
    while ((try reader.readBits(u1, 1)) != 0) {
        bits += 1;
        if (bits > 15) return error.UnsupportedAudioFormat;
    }
    const extra = try reader.readBits(u16, bits);
    return @as(i16, @intCast((@as(u32, 1) << @as(u5, @intCast(bits))) + extra));
}

fn decodeSpectralHuffmanEntry(
    reader: *BitReader,
    entries: []const SpectralHuffmanEntry,
) !SpectralHuffmanEntry {
    for (1..17) |bit_count| {
        const code = try reader.peekBits(u16, bit_count);
        for (entries) |entry| {
            if (entry.bits == bit_count and entry.code == code) {
                try reader.skipBits(bit_count);
                return entry;
            }
        }
    }
    return error.UnsupportedAudioFormat;
}

fn decodeAacSpectralIndex(reader: *BitReader, codebook: AacSpectralCodebook) !usize {
    if (reader.bit_offset + AAC_SPECTRAL_LOOKUP_BITS <= reader.bytes.len * 8) {
        const prefix = try reader.peekBits(u16, AAC_SPECTRAL_LOOKUP_BITS);
        const entry = codebook.lookup[prefix];
        if (entry.bits != 0) {
            try reader.skipBits(entry.bits);
            return @intCast(entry.index);
        }
    }

    const entry_len = @min(codebook.codes.len, codebook.bits.len);
    for (1..17) |bit_count| {
        const code = try reader.peekBits(u16, bit_count);
        for (0..entry_len) |i| {
            const entry_code = codebook.codes[i];
            const entry_bits = codebook.bits[i];
            if (entry_bits == bit_count and entry_code == code) {
                try reader.skipBits(bit_count);
                return i;
            }
        }
    }
    return error.UnsupportedAudioFormat;
}

fn unpackAacSpectralIndex(index: usize, radix: u8, dimensions: u8) [4]i16 {
    var out = [_]i16{0} ** 4;
    var remaining = index;
    var digits = [_]u8{0} ** 4;
    var i = dimensions;
    while (i > 0) {
        i -= 1;
        digits[i] = @intCast(remaining % radix);
        remaining /= radix;
    }

    const signed_offset: i16 = switch (radix) {
        3 => 1,
        9 => 4,
        else => 0,
    };
    const signed = signed_offset != 0;

    for (0..dimensions) |dim| {
        out[dim] = if (signed) @as(i16, digits[dim]) - signed_offset else digits[dim];
    }
    return out;
}

fn dequantizeAacCoefficient(raw: i16, scalefactor_value: i16) f32 {
    if (raw == 0) return 0;

    const sign: f32 = if (raw < 0) -1 else 1;
    const raw_i32 = @as(i32, raw);
    const magnitude_i32 = if (raw_i32 < 0) -raw_i32 else raw_i32;
    const magnitude_index: usize = @intCast(magnitude_i32);
    const pow43 = if (magnitude_index < aac_pow43_table.len)
        aac_pow43_table[magnitude_index]
    else
        std.math.pow(f32, @floatFromInt(magnitude_i32), 4.0 / 3.0);
    const scale = scalefactorScale(scalefactor_value);
    return sign * pow43 * scale;
}

fn scalefactorScale(scalefactor_value: i16) f32 {
    if (scalefactor_value >= 0 and scalefactor_value < aac_scalefactor_scale_table.len) {
        return aac_scalefactor_scale_table[@intCast(scalefactor_value)];
    }
    return std.math.pow(f32, 2.0, @as(f32, @floatFromInt(scalefactor_value - 100)) / 4.0);
}

fn lcgRandom(state: *u32) i32 {
    state.* = state.* *% 1664525 +% 1013904223;
    return @bitCast(state.*);
}

fn injectNoise(coefficients: []f32, start: usize, end: usize, scalefactor_value: i16, state: *u32) void {
    if (start >= end) return;

    var energy: f64 = 0;
    for (start..end) |i| {
        const sample = @as(f32, @floatFromInt(lcgRandom(state)));
        coefficients[i] = sample;
        energy += @as(f64, sample) * @as(f64, sample);
    }

    if (energy == 0) return;

    const scale = -scalefactorScale(scalefactor_value) / @as(f32, @floatCast(std.math.sqrt(energy)));
    for (start..end) |i| {
        coefficients[i] *= scale;
    }
}

fn applyPerceptualNoiseSubstitution(
    coefficients: []f32,
    plans: []const SpectralBandLayout,
    state: *u32,
) !void {
    for (plans) |plan| {
        if (plan.class != .noise) continue;
        injectNoise(coefficients, plan.coeff_start, plan.coeff_end, plan.scalefactor_value, state);
    }
}

fn applyPerceptualNoiseSubstitutionForBands(
    coefficients: []f32,
    bands: []const ScalefactorBand,
    coeff_offsets: []const u16,
    state: *u32,
) !void {
    for (bands, 0..) |band, i| {
        if (band.kind != .noise) continue;
        injectNoise(coefficients, coeff_offsets[i], coeff_offsets[i + 1], band.value, state);
    }
}

fn applyPulseAtPosition(coefficients: []f32, position: usize, scalefactor_value: i16, amplitude: u8) void {
    if (position >= coefficients.len) return;

    const sf = scalefactorScale(scalefactor_value);
    var shaped: f32 = -@as(f32, @floatFromInt(amplitude));
    const coefficient = coefficients[position];
    if (coefficient != 0 and sf != 0) {
        const normalized = coefficient / sf;
        const abs_normalized = @abs(normalized);
        shaped = if (normalized > 0)
            std.math.pow(f32, abs_normalized, 0.75) - @as(f32, @floatFromInt(amplitude))
        else
            -(std.math.pow(f32, abs_normalized, 0.75) + @as(f32, @floatFromInt(amplitude)));
    }
    coefficients[position] = std.math.cbrt(@abs(shaped)) * shaped * sf;
}

fn applyPulseTool(
    coefficients: []f32,
    plans: []const SpectralBandLayout,
    raw_swb_offsets: []const u16,
    pulse: PulseData,
) !void {
    if (raw_swb_offsets.len == 0 or pulse.pulse_swb >= raw_swb_offsets.len - 1) return error.UnsupportedAudioFormat;

    var position: usize = raw_swb_offsets[pulse.pulse_swb] + pulse.offsets[0];
    var pulse_index: usize = 0;
    while (pulse_index < pulse.num_pulse) : (pulse_index += 1) {
        var found = false;
        for (plans) |plan| {
            if (plan.class == .noise or plan.class == .intensity or plan.class == .zero) continue;
            if (position >= plan.coeff_start and position < plan.coeff_end) {
                applyPulseAtPosition(coefficients, position, plan.scalefactor_value, pulse.amplitudes[pulse_index]);
                found = true;
                break;
            }
        }
        if (!found) return error.UnsupportedAudioFormat;
        if (pulse_index + 1 < pulse.num_pulse) position += pulse.offsets[pulse_index + 1];
    }
}

fn applyPulseToolForBands(
    coefficients: []f32,
    bands: []const ScalefactorBand,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
    pulse: PulseData,
) !void {
    if (raw_swb_offsets.len == 0 or pulse.pulse_swb >= raw_swb_offsets.len - 1) return error.UnsupportedAudioFormat;

    var position: usize = raw_swb_offsets[pulse.pulse_swb] + pulse.offsets[0];
    var pulse_index: usize = 0;
    while (pulse_index < pulse.num_pulse) : (pulse_index += 1) {
        var found = false;
        for (bands, 0..) |band, i| {
            if (band.kind != .spectral) continue;
            if (position >= coeff_offsets[i] and position < coeff_offsets[i + 1]) {
                applyPulseAtPosition(coefficients, position, band.value, pulse.amplitudes[pulse_index]);
                found = true;
                break;
            }
        }
        if (!found) return error.UnsupportedAudioFormat;
        if (pulse_index + 1 < pulse.num_pulse) position += pulse.offsets[pulse_index + 1];
    }
}

fn parseGainControlData(reader: *BitReader, ics_info: IcsInfo) !GainControlData {
    const gain_mode = [4][3]u8{
        .{ 1, 0, 5 },
        .{ 2, 1, 2 },
        .{ 8, 0, 2 },
        .{ 2, 1, 5 },
    };

    const mode = @intFromEnum(ics_info.window_sequence);
    const max_band_bits: usize = if (ics_info.window_sequence == .eight_short) 3 else 2;
    const max_band = try reader.readBits(u8, max_band_bits);
    if (max_band > 8) return error.UnsupportedAudioFormat;

    var data = GainControlData{ .max_band = max_band };
    for (data.bands[0..max_band]) |*band| {
        for (band.windows[0..gain_mode[mode][0]], 0..) |*window, wd| {
            const adjust_num = try reader.readBits(u8, 3);
            if (adjust_num > window.adjustments.len) return error.UnsupportedAudioFormat;
            window.adjust_num = adjust_num;
            for (window.adjustments[0..adjust_num]) |*adjustment| {
                const location_bits: usize = if (wd == 0 and gain_mode[mode][1] != 0) 4 else gain_mode[mode][2];
                adjustment.* = .{
                    .level = try reader.readBits(u4, 4),
                    .location = try reader.readBits(u8, location_bits),
                };
            }
        }
    }
    return data;
}

fn skipGainControlData(reader: *BitReader, ics_info: IcsInfo) !void {
    _ = try parseGainControlData(reader, ics_info);
}

fn resetPredictState(state: *PredictorState) void {
    state.* = .{};
}

fn resetAllPredictors(states: *[max_predictors]PredictorState) void {
    for (states) |*state| resetPredictState(state);
}

fn resetPredictorGroup(states: *[max_predictors]PredictorState, group: u8) void {
    var index: usize = group - 1;
    while (index < max_predictors) : (index += 30) {
        resetPredictState(&states[index]);
    }
}

fn scaleByPowerOfTwo(value: f32, exponent: i32) f32 {
    var result = value;
    var e = exponent;
    while (e > 0) : (e -= 1) result *= 2.0;
    while (e < 0) : (e += 1) result *= 0.5;
    return result;
}

fn flt16Round(value: f32) f32 {
    const split = std.math.frexp(value);
    return scaleByPowerOfTwo(@round(scaleByPowerOfTwo(split.significand, 8)), split.exponent - 8);
}

fn flt16Even(value: f32) f32 {
    const split = std.math.frexp(value);
    return scaleByPowerOfTwo(@round(split.significand * 256.0), split.exponent - 8);
}

fn flt16Trunc(value: f32) f32 {
    const split = std.math.frexp(value);
    return scaleByPowerOfTwo(@trunc(split.significand * 256.0), split.exponent - 8);
}

fn predictCoefficient(state: *PredictorState, coefficient: *f32, output_enable: bool) void {
    const a: f32 = 61.0 / 64.0;
    const alpha: f32 = 29.0 / 32.0;

    const k1 = if (state.var0 > 1.0) state.cor0 * flt16Even(a / state.var0) else 0.0;
    const k2 = if (state.var1 > 1.0) state.cor1 * flt16Even(a / state.var1) else 0.0;

    const predicted = flt16Round(k1 * state.r0 + k2 * state.r1);
    if (output_enable) {
        coefficient.* += predicted * aac_main_predict_scale;
    }

    const e0 = coefficient.* / aac_main_predict_scale;
    const e1 = e0 - k1 * state.r0;

    state.cor1 = flt16Trunc(alpha * state.cor1 + state.r1 * e1);
    state.var1 = flt16Trunc(alpha * state.var1 + 0.5 * (state.r1 * state.r1 + e1 * e1));
    state.cor0 = flt16Trunc(alpha * state.cor0 + state.r0 * e0);
    state.var0 = flt16Trunc(alpha * state.var0 + 0.5 * (state.r0 * state.r0 + e0 * e0));
    state.r1 = flt16Trunc(a * (state.r0 - k1 * e0));
    state.r0 = flt16Trunc(a * e0);
}

fn applyMainPrediction(
    coefficients: []f32,
    ics_info: IcsInfo,
    raw_swb_offsets: []const u16,
    sample_rate: u32,
    predictor_states: *[max_predictors]PredictorState,
) !void {
    if (ics_info.window_sequence == .eight_short) {
        resetAllPredictors(predictor_states);
        return;
    }

    const sample_rate_index = try sampleRateIndexForRate(sample_rate);
    const band_limit = @min(ics_info.max_sfb, predictor_sfb_max[sample_rate_index]);
    if (raw_swb_offsets.len < @as(usize, band_limit) + 1) return error.UnsupportedAudioFormat;
    if (raw_swb_offsets[@as(usize, band_limit)] > max_predictors) return error.UnsupportedAudioFormat;

    for (0..band_limit) |sfb| {
        const output_enable = (ics_info.predictor_data_present orelse false) and ics_info.prediction_used[sfb];
        for (raw_swb_offsets[sfb]..raw_swb_offsets[sfb + 1]) |coeff_index| {
            predictCoefficient(&predictor_states[coeff_index], &coefficients[coeff_index], output_enable);
        }
    }
    if (ics_info.predictor_reset_group != 0) {
        resetPredictorGroup(predictor_states, ics_info.predictor_reset_group);
    }
}

const tns_tmp2_map_0_3 = [_]f32{ 0.0, -0.43388373, -0.7818315, -0.9749279, 0.98480773, 0.86602539, 0.64278758, 0.34202015 };
const tns_tmp2_map_1_3 = [_]f32{ 0.0, -0.43388373, 0.64278758, 0.34202015 };
const tns_tmp2_map_0_4 = [_]f32{ 0.0, -0.2079117, -0.40673664, -0.58778524, -0.74314481, -0.86602539, -0.95105654, -0.99452192, 0.99573416, 0.96182561, 0.8951633, 0.7980172, 0.67369562, 0.52643216, 0.36124167, 0.18374951 };
const tns_tmp2_map_1_4 = [_]f32{ 0.0, -0.2079117, -0.40673664, -0.58778524, 0.67369562, 0.52643216, 0.36124167, 0.18374951 };

fn tnsCoefficientMap(filter: TnsFilter) []const f32 {
    const resolution = filter.coef_len + @intFromBool(filter.coef_compress);
    return switch (resolution) {
        3 => if (filter.coef_compress) &tns_tmp2_map_1_3 else &tns_tmp2_map_0_3,
        4 => if (filter.coef_compress) &tns_tmp2_map_1_4 else &tns_tmp2_map_0_4,
        else => &.{},
    };
}

fn computeTnsLpc(filter: TnsFilter, out: *[20]f32) !usize {
    if (filter.order == 0) return 0;
    const map = tnsCoefficientMap(filter);
    if (map.len == 0) return error.UnsupportedAudioFormat;

    var lpc = [_]f32{0} ** 20;
    for (0..filter.order) |m| {
        const k = map[filter.coefficients[m]];
        var next = lpc;
        next[m] = k;
        for (0..m) |i| {
            next[i] = lpc[i] + k * lpc[m - 1 - i];
        }
        lpc = next;
    }
    out.* = lpc;
    return filter.order;
}

fn shortWindowGroupIndex(ics_info: IcsInfo, window_index: usize) !struct { group: usize, within_group: usize } {
    var remaining = window_index;
    for (ics_info.window_group_length[0..ics_info.num_window_groups], 0..) |group_len, group_index| {
        if (remaining < group_len) return .{ .group = group_index, .within_group = remaining };
        remaining -= group_len;
    }
    return error.UnsupportedAudioFormat;
}

fn buildTnsLineIndices(
    out: *[1024]usize,
    ics_info: IcsInfo,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
    window_index: usize,
    bottom: usize,
    top: usize,
) !usize {
    if (top <= bottom) return 0;

    var count: usize = 0;
    if (ics_info.window_sequence == .eight_short) {
        const group_info = try shortWindowGroupIndex(ics_info, window_index);
        const group_base = group_info.group * @as(usize, ics_info.max_sfb);
        for (bottom..top) |sfb| {
            const band_width = raw_swb_offsets[sfb + 1] - raw_swb_offsets[sfb];
            const band_start = coeff_offsets[group_base + sfb];
            const window_start = band_start + group_info.within_group * band_width;
            for (0..band_width) |line| {
                out[count] = window_start + line;
                count += 1;
            }
        }
    } else {
        const start = coeff_offsets[bottom];
        const end = coeff_offsets[top];
        for (start..end) |coeff_index| {
            out[count] = coeff_index;
            count += 1;
        }
    }
    return count;
}

fn reverseIndices(indices: []usize) void {
    var i: usize = 0;
    var j: usize = indices.len;
    while (i < j) {
        j -= 1;
        if (i >= j) break;
        const tmp = indices[i];
        indices[i] = indices[j];
        indices[j] = tmp;
        i += 1;
    }
}

fn applyTnsFilter(coefficients: []f32, indices: []usize, lpc: []const f32) void {
    for (indices, 0..) |coeff_index, m| {
        for (1..@min(m, lpc.len) + 1) |i| {
            coefficients[coeff_index] -= coefficients[indices[m - i]] * lpc[i - 1];
        }
    }
}

fn applyTnsTool(
    coefficients: []f32,
    ics_info: IcsInfo,
    plans: []const SpectralBandLayout,
    raw_swb_offsets: []const u16,
    tns: TnsData,
) !void {
    if (plans.len == 0) return;
    if (plans.len + 1 > 513) return error.UnsupportedAudioFormat;

    var coeff_offsets_buf: [513]u16 = undefined;
    coeff_offsets_buf[0] = plans[0].coeff_start;
    for (plans, 0..) |plan, i| {
        coeff_offsets_buf[i + 1] = plan.coeff_end;
    }
    try applyTnsToolWithOffsets(coefficients, ics_info, coeff_offsets_buf[0 .. plans.len + 1], raw_swb_offsets, tns);
}

fn applyTnsToolForBands(
    coefficients: []f32,
    ics_info: IcsInfo,
    bands: []const ScalefactorBand,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
    tns: TnsData,
) !void {
    _ = bands;
    try applyTnsToolWithOffsets(coefficients, ics_info, coeff_offsets, raw_swb_offsets, tns);
}

fn applyTnsToolWithOffsets(
    coefficients: []f32,
    ics_info: IcsInfo,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
    tns: TnsData,
) !void {
    if (raw_swb_offsets.len == 0) return error.UnsupportedAudioFormat;

    const max_band = @min(@as(usize, ics_info.max_sfb), raw_swb_offsets.len - 1);
    var indices: [1024]usize = undefined;
    var lpc: [20]f32 = undefined;

    for (0..tns.num_windows) |window_index| {
        var bottom = max_band;
        const window = tns.windows[window_index];
        for (0..window.n_filt) |filter_index| {
            const filter = window.filters[filter_index];
            const top = bottom;
            bottom = if (filter.length > top) 0 else top - filter.length;
            if (filter.order == 0) continue;

            const order = try computeTnsLpc(filter, &lpc);
            const count = try buildTnsLineIndices(
                &indices,
                ics_info,
                coeff_offsets,
                raw_swb_offsets,
                window_index,
                bottom,
                top,
            );
            if (count == 0) continue;
            if (filter.direction) reverseIndices(indices[0..count]);
            applyTnsFilter(coefficients, indices[0..count], lpc[0..order]);
        }
    }
}

fn imdctLongInto(out: []f32, coefficients: []const f32) !void {
    return imdctLongIntoWithShape(out, coefficients, null, FrameShape.default());
}

fn imdctLongIntoWithShape(out: []f32, coefficients: []const f32, plan: ?*const ImdctPlan, shape: FrameShape) !void {
    return imdctLongIntoWithShapeAndScratch(out, coefficients, plan, null, shape);
}

fn imdctLongIntoWithShapeAndScratch(
    out: []f32,
    coefficients: []const f32,
    plan: ?*const ImdctPlan,
    scratch: ?*AacDecodeScratch,
    shape: FrameShape,
) !void {
    if (coefficients.len != shape.long_coefficients or out.len != shape.long_window_samples) return error.UnsupportedAudioFormat;
    if (plan) |owned| {
        if (scratch) |scratch_ptr| {
            const work = try scratch_ptr.ensureImdctWork(owned.fft_len);
            return imdctIntoWithPlanAndScratch(out, coefficients, owned.*, work);
        }
        return imdctIntoWithPlan(out, coefficients, owned.*);
    }

    const n = out.len;
    const scale = 2.0 / @as(f32, @floatFromInt(n));
    const pi = std.math.pi;

    for (out, 0..) |*sample, n_idx| {
        const n_term = @as(f32, @floatFromInt(n_idx)) + 0.5 + @as(f32, @floatFromInt(n)) / 4.0;
        var accum: f32 = 0;
        for (coefficients, 0..) |coef, k_idx| {
            const k_term = @as(f32, @floatFromInt(k_idx)) + 0.5;
            accum += coef * @cos((pi / @as(f32, @floatFromInt(n))) * n_term * k_term);
        }
        sample.* = accum * scale;
    }
}

fn imdctShortInto(out: []f32, coefficients: []const f32) !void {
    return imdctShortIntoWithShape(out, coefficients, null, FrameShape.default());
}

fn imdctShortIntoWithShape(out: []f32, coefficients: []const f32, plan: ?*const ImdctPlan, shape: FrameShape) !void {
    return imdctShortIntoWithShapeAndScratch(out, coefficients, plan, null, shape);
}

fn imdctShortIntoWithShapeAndScratch(
    out: []f32,
    coefficients: []const f32,
    plan: ?*const ImdctPlan,
    scratch: ?*AacDecodeScratch,
    shape: FrameShape,
) !void {
    if (coefficients.len != shape.short_coefficients or out.len != shape.short_window_samples) return error.UnsupportedAudioFormat;
    if (plan) |owned| {
        if (scratch) |scratch_ptr| {
            const work = try scratch_ptr.ensureImdctWork(owned.fft_len);
            return imdctIntoWithPlanAndScratch(out, coefficients, owned.*, work);
        }
        return imdctIntoWithPlan(out, coefficients, owned.*);
    }

    const n = out.len;
    const scale = 2.0 / @as(f32, @floatFromInt(n));
    const pi = std.math.pi;

    for (out, 0..) |*sample, n_idx| {
        const n_term = @as(f32, @floatFromInt(n_idx)) + 0.5 + @as(f32, @floatFromInt(n)) / 4.0;
        var accum: f32 = 0;
        for (coefficients, 0..) |coef, k_idx| {
            const k_term = @as(f32, @floatFromInt(k_idx)) + 0.5;
            accum += coef * @cos((pi / @as(f32, @floatFromInt(n))) * n_term * k_term);
        }
        sample.* = accum * scale;
    }
}

fn applyAacLongWindow(ics_info: IcsInfo, samples: []f32) !void {
    return applyAacLongWindowWithShape(ics_info, samples, null, FrameShape.default());
}

fn applyAacLongWindowWithShape(
    ics_info: IcsInfo,
    samples: []f32,
    windows: ?*const AacWindowTables,
    shape: FrameShape,
) !void {
    if (samples.len != shape.long_window_samples) return error.UnsupportedAudioFormat;

    if (windows) |owned| {
        if (owned.longFor(ics_info.window_sequence, ics_info.window_shape)) |gains| {
            for (samples, gains) |*sample, gain| {
                sample.* *= gain;
            }
            return;
        }
    }

    for (samples, 0..) |*sample, index| {
        sample.* *= windowGainForIndexWithShape(ics_info.window_sequence, ics_info.window_shape, index, shape);
    }
}

fn applyAacShortWindow(samples: []f32) !void {
    return applyAacShortWindowWithShape(samples, null, FrameShape.default());
}

fn applyAacShortWindowWithShape(samples: []f32, windows: ?*const AacWindowTables, shape: FrameShape) !void {
    if (samples.len != shape.short_window_samples) return error.UnsupportedAudioFormat;
    if (windows) |owned| {
        for (samples, owned.short) |*sample, gain| {
            sample.* *= gain;
        }
        return;
    }
    for (samples, 0..) |*sample, i| {
        sample.* *= shortWindowGainWithShape(i, shape);
    }
}

pub fn composeEightShortWindowSequenceAlloc(
    allocator: std.mem.Allocator,
    coefficients: []const f32,
) !WindowedShortSequence {
    return composeEightShortWindowSequenceAllocWithShape(allocator, coefficients, null, FrameShape.default());
}

fn composeEightShortWindowSequenceAllocWithShape(
    allocator: std.mem.Allocator,
    coefficients: []const f32,
    plans: ?*const AacImdctPlans,
    shape: FrameShape,
) !WindowedShortSequence {
    if (coefficients.len != shape.short_coefficients * 8) return error.UnsupportedAudioFormat;

    const out = try allocator.alloc(f32, shape.long_window_samples);
    errdefer allocator.free(out);

    const block = try allocator.alloc(f32, shape.short_window_samples);
    defer allocator.free(block);
    try composeEightShortWindowSequenceIntoWithShape(out, block, coefficients, plans, null, shape);

    return .{
        .samples = out,
        .allocator = allocator,
    };
}

fn composeEightShortWindowSequenceIntoWithShape(
    out: []f32,
    block: []f32,
    coefficients: []const f32,
    plans: ?*const AacImdctPlans,
    scratch: ?*AacDecodeScratch,
    shape: FrameShape,
) !void {
    if (coefficients.len != shape.short_coefficients * 8) return error.UnsupportedAudioFormat;
    if (out.len != shape.long_window_samples or block.len != shape.short_window_samples) return error.UnsupportedAudioFormat;
    @memset(out, 0);
    for (0..8) |window_index| {
        const coeff_start = window_index * shape.short_coefficients;
        const imdct_started = perfNowNs();
        try imdctShortIntoWithShapeAndScratch(block, coefficients[coeff_start .. coeff_start + shape.short_coefficients], if (plans) |owned| &owned.short else null, scratch, shape);
        perfAccumulate(.filterbank_imdct_ns, imdct_started);
        const window_started = perfNowNs();
        try applyAacShortWindowWithShape(block, if (plans) |owned| &owned.windows else null, shape);
        perfAccumulate(.filterbank_window_ns, window_started);

        const out_start = shape.transition_flat_samples + window_index * shape.short_coefficients;
        for (block, 0..) |sample, i| {
            out[out_start + i] += sample;
        }
    }
}

pub fn overlapAddShortWindowSequenceAlloc(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    current_windowed: []const f32,
) !ShortWindowPcmBlock {
    return overlapAddShortWindowSequenceAllocWithShape(allocator, previous_tail, current_windowed, FrameShape.default());
}

fn overlapAddShortWindowSequenceAllocWithShape(
    allocator: std.mem.Allocator,
    previous_tail: ?[]const f32,
    current_windowed: []const f32,
    shape: FrameShape,
) !ShortWindowPcmBlock {
    if (current_windowed.len != shape.long_window_samples) return error.UnsupportedAudioFormat;
    if (previous_tail) |tail| {
        if (tail.len != shape.pcm_samples) return error.UnsupportedAudioFormat;
    }

    const pcm = try allocator.alloc(f32, shape.pcm_samples);
    errdefer allocator.free(pcm);
    const tail = try allocator.alloc(f32, shape.pcm_samples);
    errdefer allocator.free(tail);

    if (previous_tail) |prev| {
        for (pcm, prev, current_windowed[0..shape.pcm_samples]) |*out, lhs, rhs| {
            out.* = lhs + rhs;
        }
    } else {
        @memcpy(pcm, current_windowed[0..shape.pcm_samples]);
    }
    @memcpy(tail, current_windowed[shape.pcm_samples..shape.long_window_samples]);

    return .{
        .pcm = pcm,
        .tail = tail,
        .allocator = allocator,
    };
}

fn windowGainForIndex(sequence: WindowSequence, window_shape: u1, index: usize) f32 {
    return windowGainForIndexWithShape(sequence, window_shape, index, FrameShape.default());
}

fn windowGainForIndexWithShape(sequence: WindowSequence, window_shape: u1, index: usize, shape: FrameShape) f32 {
    return switch (sequence) {
        .only_long => onlyLongWindowGain(window_shape, index, shape),
        .long_start => longStartWindowGain(window_shape, index, shape),
        .long_stop => longStopWindowGain(window_shape, index, shape),
        .eight_short => 0,
    };
}

fn shortWindowGain(index: usize) f32 {
    return shortWindowGainWithShape(index, FrameShape.default());
}

fn shortWindowGainWithShape(index: usize, shape: FrameShape) f32 {
    if (index < shape.short_coefficients) return sineWindowValue(shape.short_coefficients, index);
    return sineWindowValue(shape.short_coefficients, shape.short_window_samples - 1 - index);
}

fn onlyLongWindowGain(window_shape: u1, index: usize, shape: FrameShape) f32 {
    if (window_shape == 0) {
        if (index < shape.long_coefficients) return sineWindowValue(shape.long_coefficients, index);
        return sineWindowValue(shape.long_coefficients, shape.long_window_samples - 1 - index);
    }
    if (index < shape.long_coefficients) return kbdWindowValue(shape.long_coefficients, index, 4.0);
    return kbdWindowValue(shape.long_coefficients, shape.long_window_samples - 1 - index, 4.0);
}

fn longStartWindowGain(window_shape: u1, index: usize, shape: FrameShape) f32 {
    if (index < shape.long_coefficients) {
        return if (window_shape == 0)
            sineWindowValue(shape.long_coefficients, index)
        else
            kbdWindowValue(shape.long_coefficients, index, 4.0);
    }
    if (index < shape.long_coefficients + shape.transition_flat_samples) return 1.0;
    if (index < shape.long_coefficients + shape.transition_flat_samples + shape.short_coefficients) {
        return sineWindowValue(shape.short_coefficients, shape.long_coefficients + shape.transition_flat_samples + shape.short_coefficients - 1 - index);
    }
    return 0.0;
}

fn longStopWindowGain(window_shape: u1, index: usize, shape: FrameShape) f32 {
    if (index < shape.transition_flat_samples) return 0.0;
    if (index < shape.transition_flat_samples + shape.short_coefficients) return sineWindowValue(shape.short_coefficients, index - shape.transition_flat_samples);
    if (index < shape.long_coefficients) return 1.0;
    return if (window_shape == 0)
        sineWindowValue(shape.long_coefficients, shape.long_window_samples - 1 - index)
    else
        kbdWindowValue(shape.long_coefficients, shape.long_window_samples - 1 - index, 4.0);
}

fn sineWindowValue(length: usize, index: usize) f32 {
    const pi = std.math.pi;
    const phase = (pi / (2.0 * @as(f32, @floatFromInt(length)))) *
        (2.0 * @as(f32, @floatFromInt(index)) + 1.0);
    return @sin(phase);
}

fn besselI0(x: f64) f64 {
    const half_x = x * 0.5;
    const half_x_sq = half_x * half_x;
    var term: f64 = 1.0;
    var sum: f64 = 1.0;
    var k: usize = 1;
    while (k < 64) : (k += 1) {
        term *= half_x_sq / (@as(f64, @floatFromInt(k)) * @as(f64, @floatFromInt(k)));
        sum += term;
        if (term < 1e-14) break;
    }
    return sum;
}

fn kaiserBesselValue(length: usize, index: usize, alpha: f64) f64 {
    const ratio = (2.0 * @as(f64, @floatFromInt(index))) / @as(f64, @floatFromInt(length - 1)) - 1.0;
    const inner = @max(0.0, 1.0 - ratio * ratio);
    return besselI0(std.math.pi * alpha * @sqrt(inner));
}

fn kbdWindowValue(length: usize, index: usize, alpha: f64) f32 {
    var numerator: f64 = 0.0;
    for (0..index + 1) |i| {
        numerator += kaiserBesselValue(length, i, alpha);
    }

    var denominator: f64 = 0.0;
    for (0..length) |i| {
        denominator += kaiserBesselValue(length, i, alpha);
    }

    return @floatCast(@sqrt(numerator / denominator));
}

fn parsePulseData(reader: *BitReader, ics_info: IcsInfo) !PulseData {
    if (ics_info.window_sequence == .eight_short) return error.UnsupportedAudioFormat;

    var pulse = PulseData{
        .num_pulse = try reader.readBits(u8, 2) + 1,
        .pulse_swb = try reader.readBits(u8, 6),
        .offsets = [_]u8{0} ** 4,
        .amplitudes = [_]u8{0} ** 4,
    };
    pulse.offsets[0] = try reader.readBits(u8, 5);
    pulse.amplitudes[0] = try reader.readBits(u8, 4);
    for (1..pulse.num_pulse) |i| {
        pulse.offsets[i] = try reader.readBits(u8, 5);
        pulse.amplitudes[i] = try reader.readBits(u8, 4);
    }
    return pulse;
}

fn parseTnsData(reader: *BitReader, ics_info: IcsInfo, is_aac_main: bool) !TnsData {
    const is8 = ics_info.window_sequence == .eight_short;
    const num_windows: u8 = if (is8) 8 else 1;
    _ = is_aac_main;
    const tns_max_order: u8 = if (is8) 7 else 20;

    var tns = TnsData{ .num_windows = num_windows };
    for (0..num_windows) |w| {
        const n_filt = try reader.readBits(u8, if (is8) 1 else 2);
        tns.windows[w].n_filt = n_filt;
        if (n_filt == 0) continue;
        const coef_res = try reader.readBits(u1, 1);
        tns.windows[w].coef_res = coef_res;

        for (0..n_filt) |f| {
            var filter = TnsFilter{
                .length = try reader.readBits(u8, if (is8) 4 else 6),
                .order = try reader.readBits(u8, if (is8) 3 else 5),
            };
            if (filter.order > tns_max_order) return error.UnsupportedAudioFormat;
            if (filter.order != 0) {
                filter.direction = (try reader.readBits(u1, 1)) != 0;
                filter.coef_compress = (try reader.readBits(u1, 1)) != 0;
                filter.coef_len = @as(u8, coef_res) + 3 - @intFromBool(filter.coef_compress);
                for (0..filter.order) |i| {
                    filter.coefficients[i] = try reader.readBits(u8, filter.coef_len);
                }
            }
            tns.windows[w].filters[f] = filter;
        }
    }
    return tns;
}

fn seekFirstChannelElement(reader: *BitReader) !ElementHeader {
    var trailing_info = TrailingElementInfo{};
    return seekFirstChannelElementWithTrailingInfo(reader, &trailing_info);
}

fn seekFirstChannelElementWithTrailingInfo(reader: *BitReader, trailing_info: *TrailingElementInfo) !ElementHeader {
    while (true) {
        const kind = @as(ElementKind, @enumFromInt(try reader.readBits(u3, 3)));
        switch (kind) {
            .sce, .cpe, .lfe => {
                const element_instance_tag = try reader.readBits(u8, 4);
                return .{
                    .kind = kind,
                    .element_instance_tag = element_instance_tag,
                };
            },
            .dse => {
                _ = try reader.readBits(u8, 4);
                try skipDataStreamElement(reader);
            },
            .cce => {
                _ = try reader.readBits(u8, 4);
                return error.UnsupportedAudioFormat;
            },
            .pce => {
                _ = try reader.readBits(u8, 4);
                try skipProgramConfigElement(reader);
            },
            .fil => trailing_info.merge(try parseFillElement(reader)),
            .end => return error.UnsupportedAudioFormat,
        }
    }
}

fn scanSupportedTrailingElements(reader: *BitReader) !TrailingElementInfo {
    var info = TrailingElementInfo{};
    while (reader.bit_offset < reader.bytes.len * 8) {
        if (trailingBitsAreZero(reader.bytes, reader.bit_offset)) return info;
        if (reader.bytes.len * 8 - reader.bit_offset < 3) return error.UnsupportedAudioFormat;

        const kind = @as(ElementKind, @enumFromInt(try reader.readBits(u3, 3)));
        switch (kind) {
            .dse => {
                _ = try reader.readBits(u8, 4);
                try skipDataStreamElement(reader);
            },
            .pce => {
                _ = try reader.readBits(u8, 4);
                try skipProgramConfigElement(reader);
            },
            .fil => info.merge(try parseFillElement(reader)),
            .end => {
                if (!trailingBitsAreZero(reader.bytes, reader.bit_offset)) return error.UnsupportedAudioFormat;
                return info;
            },
            .sce, .cpe, .lfe, .cce => return error.UnsupportedAudioFormat,
        }
    }
    return info;
}

fn validateSupportedTrailingElements(reader: *BitReader) !void {
    _ = try scanSupportedTrailingElements(reader);
}

const FirstChannelReader = struct {
    reader: BitReader,
    element_kind: ElementKind,
    element_instance_tag: u8,
    trailing_info: TrailingElementInfo,
    ics_info: IcsInfo,
    global_gain: u8,
};

const FirstChannelSpectralState = struct {
    reader: BitReader,
    element_kind: ElementKind,
    element_instance_tag: u8,
    trailing_info: TrailingElementInfo,
    ics_info: IcsInfo,
    sections: []Section,
    bands: []ScalefactorBand,
    plans: []SpectralBandLayout,
    coeff_offsets: []const u16,
    owned_coeff_offsets: ?[]u16,
    raw_swb_offsets: []const u16,
    pulse_present: bool,
    tns_present: bool,
    gain_control_present: bool,
    pulse_data: ?PulseData,
    tns_data: ?TnsData,
    allocator: std.mem.Allocator,

    fn deinit(self: *FirstChannelSpectralState) void {
        self.allocator.free(self.sections);
        self.allocator.free(self.bands);
        self.allocator.free(self.plans);
        if (self.owned_coeff_offsets) |owned| self.allocator.free(owned);
    }
};

const ChannelReader = struct {
    global_gain: u8,
};

const ChannelDequantized = struct {
    bands: []ScalefactorBand,
    coefficients: []f32,
    contains_noise: bool,
    contains_intensity: bool,
    allocator: std.mem.Allocator,

    fn deinit(self: *ChannelDequantized) void {
        self.allocator.free(self.bands);
        self.allocator.free(self.coefficients);
    }
};

const ChannelDequantizeOptions = struct {
    allow_gain_control: bool = true,
    predictor_states: ?*[max_predictors]PredictorState = null,
};

noinline fn initFirstChannelReader(bytes: []const u8, sample_rate: ?u32) !FirstChannelReader {
    var reader = BitReader.init(bytes);
    var trailing_info = TrailingElementInfo{};
    const first_element = try seekFirstChannelElementWithTrailingInfo(&reader, &trailing_info);

    switch (first_element.kind) {
        .sce, .lfe => {
            const global_gain = try reader.readBits(u8, 8);
            const ics_info = try parseIcsInfo(&reader, sample_rate);
            return .{
                .reader = reader,
                .element_kind = first_element.kind,
                .element_instance_tag = first_element.element_instance_tag,
                .trailing_info = trailing_info,
                .ics_info = ics_info,
                .global_gain = global_gain,
            };
        },
        .cpe => {
            const common_window = (try reader.readBits(u1, 1)) != 0;
            if (common_window) {
                const info = try parseIcsInfo(&reader, sample_rate);
                const ms_present = try reader.readBits(u2, 2);
                if (ms_present == 1) {
                    try reader.skipBits(@as(usize, info.num_window_groups) * info.max_sfb);
                }
                const global_gain = try reader.readBits(u8, 8);
                return .{
                    .reader = reader,
                    .element_kind = first_element.kind,
                    .element_instance_tag = first_element.element_instance_tag,
                    .trailing_info = trailing_info,
                    .ics_info = info,
                    .global_gain = global_gain,
                };
            }

            const global_gain = try reader.readBits(u8, 8);
            const ics_info = try parseIcsInfo(&reader, sample_rate);
            return .{
                .reader = reader,
                .element_kind = first_element.kind,
                .element_instance_tag = first_element.element_instance_tag,
                .trailing_info = trailing_info,
                .ics_info = ics_info,
                .global_gain = global_gain,
            };
        },
        else => return error.UnsupportedAudioFormat,
    }
}

fn parseChannelReader(reader: *BitReader) !ChannelReader {
    return .{
        .global_gain = try reader.readBits(u8, 8),
    };
}

fn initFirstChannelSpectralStateAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
) !FirstChannelSpectralState {
    return initFirstChannelSpectralStateAllocWithShape(allocator, sample_rate, bytes, FrameShape.default());
}

fn initFirstChannelSpectralStateAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    bytes: []const u8,
    shape: FrameShape,
) !FirstChannelSpectralState {
    var first_channel = try initFirstChannelReader(bytes, sample_rate);
    const sections = try parseSectionDataAlloc(allocator, &first_channel.reader, first_channel.ics_info);
    errdefer allocator.free(sections);

    const bands = try parseScalefactorBandsAlloc(
        allocator,
        &first_channel.reader,
        first_channel.global_gain,
        first_channel.ics_info,
        sections,
    );
    errdefer allocator.free(bands);

    const pulse_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    const pulse_data = if (pulse_present)
        try parsePulseData(&first_channel.reader, first_channel.ics_info)
    else
        null;
    const tns_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    const tns_data = if (tns_present)
        try parseTnsData(&first_channel.reader, first_channel.ics_info, false)
    else
        null;
    const gain_control_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    if (gain_control_present) try skipGainControlData(&first_channel.reader, first_channel.ics_info);

    const full_swb_offsets: []const u16 = if (first_channel.ics_info.window_sequence == .eight_short)
        try swbOffsetsShort(sample_rate, shape)
    else
        try swbOffsetsLong(sample_rate, shape);
    if (full_swb_offsets.len < @as(usize, first_channel.ics_info.max_sfb) + 1) return error.UnsupportedAudioFormat;
    const base_swb_offsets = full_swb_offsets[0 .. @as(usize, first_channel.ics_info.max_sfb) + 1];
    var owned_coeff_offsets: ?[]u16 = null;
    const coeff_offsets: []const u16 = if (first_channel.ics_info.window_sequence == .eight_short) blk: {
        owned_coeff_offsets = try buildGroupedShortBandOffsetsAlloc(allocator, base_swb_offsets, first_channel.ics_info);
        break :blk owned_coeff_offsets.?;
    } else base_swb_offsets;

    const plans = try buildSpectralLayoutsAlloc(allocator, bands, coeff_offsets);

    return .{
        .reader = first_channel.reader,
        .element_kind = first_channel.element_kind,
        .element_instance_tag = first_channel.element_instance_tag,
        .trailing_info = first_channel.trailing_info,
        .ics_info = first_channel.ics_info,
        .sections = sections,
        .bands = bands,
        .plans = plans,
        .coeff_offsets = coeff_offsets,
        .owned_coeff_offsets = owned_coeff_offsets,
        .raw_swb_offsets = full_swb_offsets,
        .pulse_present = pulse_present,
        .tns_present = tns_present,
        .gain_control_present = gain_control_present,
        .pulse_data = pulse_data,
        .tns_data = tns_data,
        .allocator = allocator,
    };
}

fn initFirstChannelSpectralStateWithCoeffOffsetsAlloc(
    allocator: std.mem.Allocator,
    coeff_offsets: []const u16,
    bytes: []const u8,
) !FirstChannelSpectralState {
    var first_channel = try initFirstChannelReader(bytes, null);
    const sections = try parseSectionDataAlloc(allocator, &first_channel.reader, first_channel.ics_info);
    errdefer allocator.free(sections);

    const bands = try parseScalefactorBandsAlloc(
        allocator,
        &first_channel.reader,
        first_channel.global_gain,
        first_channel.ics_info,
        sections,
    );
    errdefer allocator.free(bands);

    const pulse_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    const pulse_data = if (pulse_present)
        try parsePulseData(&first_channel.reader, first_channel.ics_info)
    else
        null;
    const tns_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    const tns_data = if (tns_present)
        try parseTnsData(&first_channel.reader, first_channel.ics_info, false)
    else
        null;
    const gain_control_present = (try first_channel.reader.readBits(u1, 1)) != 0;
    if (gain_control_present) try skipGainControlData(&first_channel.reader, first_channel.ics_info);

    const plans = try buildSpectralLayoutsAlloc(allocator, bands, coeff_offsets);

    return .{
        .reader = first_channel.reader,
        .element_kind = first_channel.element_kind,
        .element_instance_tag = first_channel.element_instance_tag,
        .ics_info = first_channel.ics_info,
        .sections = sections,
        .bands = bands,
        .plans = plans,
        .coeff_offsets = coeff_offsets,
        .owned_coeff_offsets = null,
        .raw_swb_offsets = coeff_offsets,
        .pulse_present = pulse_present,
        .tns_present = tns_present,
        .gain_control_present = gain_control_present,
        .pulse_data = pulse_data,
        .tns_data = tns_data,
        .allocator = allocator,
    };
}

fn decodeChannelDequantizedIntoAlloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    swb_offsets: []const u16,
    bands: []const ScalefactorBand,
) ![]f32 {
    return decodeChannelDequantizedIntoAllocWithShape(
        allocator,
        reader,
        swb_offsets,
        bands,
        1024,
        FrameShape.default(),
    );
}

fn decodeChannelDequantizedIntoAllocWithShape(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    swb_offsets: []const u16,
    bands: []const ScalefactorBand,
    coeff_len: usize,
    shape: FrameShape,
) ![]f32 {
    if (coeff_len != shape.long_coefficients and coeff_len != shape.short_coefficients * 8) {
        return error.UnsupportedAudioFormat;
    }
    if (swb_offsets.len > 0 and swb_offsets[swb_offsets.len - 1] > coeff_len) return error.UnsupportedAudioFormat;
    const coefficients = try allocator.alloc(f32, coeff_len);
    @memset(coefficients, 0);
    errdefer allocator.free(coefficients);

    for (bands, 0..) |band, i| {
        const info = spectralCodebookInfo(band.band_type);
        const band_class = spectralCodebookClass(band.band_type);
        const coeff_start = swb_offsets[i];
        const coeff_end = swb_offsets[i + 1];
        const coeff_width = coeff_end - coeff_start;
        const symbol_count: u16 = switch (band_class) {
            .zero, .noise, .intensity => 0,
            .quad => blk: {
                if (coeff_width % 4 != 0) return error.UnsupportedAudioFormat;
                break :blk coeff_width / 4;
            },
            .pair, .escape => blk: {
                if (coeff_width % 2 != 0) return error.UnsupportedAudioFormat;
                break :blk coeff_width / 2;
            },
        };

        switch (band_class) {
            .zero, .noise, .intensity => continue,
            .quad, .pair, .escape => {},
        }

        const codebook = try aacSpectralCodebook(band.band_type);
        var coeff_index: usize = coeff_start;
        for (0..symbol_count) |_| {
            const symbol = try decodeAacSpectralSymbol(reader, codebook);
            for (0..info.dimensions) |dim| {
                coefficients[coeff_index] = dequantizeAacCoefficient(symbol.values[dim], band.value);
                coeff_index += 1;
            }
        }
        if (coeff_index != coeff_end) return error.UnsupportedAudioFormat;
    }
    return coefficients;
}

fn skipChannelSpectralData(
    reader: *BitReader,
    swb_offsets: []const u16,
    bands: []const ScalefactorBand,
    coeff_len: usize,
    shape: FrameShape,
) !void {
    if (coeff_len != shape.long_coefficients and coeff_len != shape.short_coefficients * 8) {
        return error.UnsupportedAudioFormat;
    }
    if (swb_offsets.len > 0 and swb_offsets[swb_offsets.len - 1] > coeff_len) return error.UnsupportedAudioFormat;

    for (bands, 0..) |band, i| {
        const band_class = spectralCodebookClass(band.band_type);
        const coeff_start = swb_offsets[i];
        const coeff_end = swb_offsets[i + 1];
        const coeff_width = coeff_end - coeff_start;
        const symbol_count: u16 = switch (band_class) {
            .zero, .noise, .intensity => 0,
            .quad => blk: {
                if (coeff_width % 4 != 0) return error.UnsupportedAudioFormat;
                break :blk coeff_width / 4;
            },
            .pair, .escape => blk: {
                if (coeff_width % 2 != 0) return error.UnsupportedAudioFormat;
                break :blk coeff_width / 2;
            },
        };

        switch (band_class) {
            .zero, .noise, .intensity => continue,
            .quad, .pair, .escape => {},
        }

        const codebook = try aacSpectralCodebook(band.band_type);
        for (0..symbol_count) |_| {
            _ = try decodeAacSpectralSymbol(reader, codebook);
        }
    }
}

fn decodeChannelDequantizedAlloc(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
    ics_info: IcsInfo,
    global_gain: u8,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
    pns_state: *u32,
    options: ChannelDequantizeOptions,
) !ChannelDequantized {
    return decodeChannelDequantizedAllocWithShape(
        allocator,
        sample_rate,
        reader,
        ics_info,
        global_gain,
        coeff_offsets,
        raw_swb_offsets,
        pns_state,
        options,
        FrameShape.default(),
    );
}

fn decodeChannelDequantizedAllocWithShape(
    allocator: std.mem.Allocator,
    sample_rate: u32,
    reader: *BitReader,
    ics_info: IcsInfo,
    global_gain: u8,
    coeff_offsets: []const u16,
    raw_swb_offsets: []const u16,
    pns_state: *u32,
    options: ChannelDequantizeOptions,
    shape: FrameShape,
) !ChannelDequantized {
    const parse_started = perfNowNs();
    const sections = try parseSectionDataAlloc(allocator, reader, ics_info);
    defer allocator.free(sections);
    const bands = try parseScalefactorBandsAlloc(allocator, reader, global_gain, ics_info, sections);
    perfAccumulate(.spectral_parse_ns, parse_started);
    errdefer allocator.free(bands);

    const pulse_present = (try reader.readBits(u1, 1)) != 0;
    const pulse_data = if (pulse_present) try parsePulseData(reader, ics_info) else null;
    const tns_present = (try reader.readBits(u1, 1)) != 0;
    const tns_data = if (tns_present) try parseTnsData(reader, ics_info, false) else null;
    const gain_control_present = (try reader.readBits(u1, 1)) != 0;
    if (gain_control_present) {
        if (!options.allow_gain_control) return error.UnsupportedAudioFormat;
        try skipGainControlData(reader, ics_info);
    }

    const decode_started = perfNowNs();
    const coefficients = try decodeChannelDequantizedIntoAllocWithShape(
        allocator,
        reader,
        coeff_offsets,
        bands,
        spectralCoefficientCount(ics_info, shape),
        shape,
    );
    perfAccumulate(.spectral_decode_ns, decode_started);
    errdefer allocator.free(coefficients);

    const tools_started = perfNowNs();
    try applyPerceptualNoiseSubstitutionForBands(coefficients, bands, coeff_offsets, pns_state);
    if (pulse_data) |pulse| {
        try applyPulseToolForBands(coefficients, bands, coeff_offsets, raw_swb_offsets, pulse);
    }
    if (options.predictor_states) |predictor_states| {
        try applyMainPrediction(coefficients, ics_info, raw_swb_offsets, sample_rate, predictor_states);
    }
    if (tns_data) |tns| {
        try applyTnsToolForBands(
            coefficients,
            ics_info,
            bands,
            coeff_offsets,
            raw_swb_offsets,
            tns,
        );
    }
    perfAccumulate(.tns_tools_ns, tools_started);

    return .{
        .bands = bands,
        .coefficients = coefficients,
        .contains_noise = containsBandKind(bands, .noise),
        .contains_intensity = containsBandKind(bands, .intensity),
        .allocator = allocator,
    };
}

fn applyMsStereo(
    left: []f32,
    right: []f32,
    left_bands: []const ScalefactorBand,
    right_bands: []const ScalefactorBand,
    ms_mask: []const bool,
    swb_offsets: []const u16,
) void {
    for (ms_mask, 0..) |use_ms, i| {
        if (!use_ms) continue;
        if (left_bands[i].kind != .spectral or right_bands[i].kind != .spectral) continue;
        if (left_bands[i].band_type == INTENSITY_BT or left_bands[i].band_type == INTENSITY_BT2) continue;
        if (right_bands[i].band_type == INTENSITY_BT or right_bands[i].band_type == INTENSITY_BT2) continue;

        const start = swb_offsets[i];
        const end = swb_offsets[i + 1];
        for (start..end) |coeff_index| {
            const mid = left[coeff_index];
            const side = right[coeff_index];
            left[coeff_index] = mid + side;
            right[coeff_index] = mid - side;
        }
    }
}

fn applyIntensityStereo(
    left: []const f32,
    right: []f32,
    left_bands: []const ScalefactorBand,
    right_bands: []const ScalefactorBand,
    ms_mask: []const bool,
    swb_offsets: []const u16,
) void {
    _ = left_bands;
    for (right_bands, 0..) |band, i| {
        if (band.band_type != INTENSITY_BT and band.band_type != INTENSITY_BT2) continue;

        var sign: f32 = if (band.band_type == INTENSITY_BT) -1 else 1;
        if (ms_mask.len != 0 and ms_mask[i]) sign = -sign;
        const scale = sign * std.math.pow(f32, 2.0, -@as(f32, @floatFromInt(band.value)) / 4.0);

        const start = swb_offsets[i];
        const end = swb_offsets[i + 1];
        for (start..end) |coeff_index| {
            right[coeff_index] = left[coeff_index] * scale;
        }
    }
}

fn containsBandKind(bands: []const ScalefactorBand, kind: ScalefactorKind) bool {
    for (bands) |band| {
        if (band.kind == kind) return true;
    }
    return false;
}

fn readBitsCompat(reader: *BitReader, bit_count: usize) !usize {
    if (bit_count == 0) return 0;
    if (reader.bit_offset + bit_count > reader.bytes.len * 8) return error.UnsupportedAudioFormat;

    var value: usize = 0;
    for (0..bit_count) |_| {
        const byte = reader.bytes[reader.bit_offset / 8];
        const shift: u3 = @intCast(7 - (reader.bit_offset % 8));
        value = (value << 1) | @as(usize, (byte >> shift) & 1);
        reader.bit_offset += 1;
    }
    return value;
}

noinline fn parseSectionDataAlloc(
    allocator: std.mem.Allocator,
    reader: *BitReader,
    ics_info: IcsInfo,
) ![]Section {
    var sections = std.ArrayList(Section).empty;
    defer sections.deinit(allocator);

    const sect_bits: usize = if (ics_info.window_sequence == .eight_short) 3 else 5;
    const sect_esc_value: usize = if (ics_info.window_sequence == .eight_short) 7 else 31;
    const max_sfb: usize = ics_info.max_sfb;

    for (0..ics_info.num_window_groups) |_| {
        var group_sfb: usize = 0;
        while (group_sfb < max_sfb) {
            const band_type: u4 = @intCast(try readBitsCompat(reader, 4));
            if (band_type == 12) return error.UnsupportedAudioFormat;
            var section_len: usize = group_sfb;
            while (true) {
                const incr = try readBitsCompat(reader, sect_bits);
                section_len += incr;
                if (incr != sect_esc_value) break;
            }
            if (section_len <= group_sfb or section_len > max_sfb) return error.UnsupportedAudioFormat;
            try sections.append(allocator, .{
                .band_type = band_type,
                .start_sfb = @intCast(group_sfb),
                .end_sfb = @intCast(section_len),
            });
            group_sfb = section_len;
        }
    }

    return try sections.toOwnedSlice(allocator);
}

fn decodeScalefactorSymbol(reader: *BitReader) !u8 {
    if (reader.remainingBits() >= AAC_SCALEFACTOR_LOOKUP_BITS) {
        const prefix = try reader.peekBits(usize, AAC_SCALEFACTOR_LOOKUP_BITS);
        const entry = aac_scalefactor_lookup[prefix];
        if (entry.bits != 0) {
            try reader.skipBits(entry.bits);
            return entry.symbol;
        }
    }

    for (1..20) |bit_count| {
        const code = try reader.peekBits(u32, bit_count);
        for (scalefactor_codes, scalefactor_bits, 0..) |entry_code, entry_bits, i| {
            if (entry_bits == bit_count and entry_code == code) {
                try reader.skipBits(bit_count);
                return @intCast(i);
            }
        }
    }
    return error.UnsupportedAudioFormat;
}

fn parseFillElement(reader: *BitReader) !FillElementInfo {
    var count = try reader.readBits(u16, 4);
    if (count == 15) {
        const esc_count = try reader.readBits(u16, 8);
        count = count + esc_count - 1;
    }

    var info = FillElementInfo{ .payload_len = count };
    for (0..count) |index| {
        const byte = try reader.readBits(u8, 8);
        if (index == 0) {
            const extension_type = byte >> 4;
            if (extension_type == 13 or extension_type == 14) {
                info.saw_sbr_payload = true;
                info.saw_plain_sbr_payload = extension_type == 13;
                info.saw_ps_payload = extension_type == 14;
            }
        } else if (index == 1) {
            info.envelope_hint = byte;
        } else if (index == 2) {
            info.noise_hint = byte;
            if (info.saw_ps_payload) info.ps_noise_hint = byte;
        } else if (index == 3) {
            info.stereo_hint = byte;
            if (info.saw_ps_payload) info.ps_stereo_hint = byte;
        } else {
            info.harmonic_hint ^= byte;
            if ((index & 1) == 0) {
                info.detail_hint +%= byte;
                if (info.saw_ps_payload) info.ps_detail_hint +%= byte;
            } else {
                const phase_byte = byte ^ @as(u8, @truncate(index * 29));
                info.phase_hint ^= phase_byte;
                if (info.saw_ps_payload) info.ps_phase_hint ^= phase_byte;
            }
            if (info.saw_ps_payload) info.ps_harmonic_hint ^= byte;
        }
        info.payload_hash = (info.payload_hash ^ byte) *% 16777619;
        if (info.saw_ps_payload) {
            info.ps_payload_hash = (info.ps_payload_hash ^ byte) *% 16777619;
        }
    }
    return info;
}

fn skipFillElement(reader: *BitReader) !void {
    _ = try parseFillElement(reader);
}

pub const ProgramConfigLayout = struct {
    channel_count: u8,
    front_single_count: u8 = 0,
    front_pair_count: u8 = 0,
    side_single_count: u8 = 0,
    side_pair_count: u8 = 0,
    back_single_count: u8 = 0,
    back_pair_count: u8 = 0,
    lfe_count: u8 = 0,
    front_single_tags: [4]u8 = [_]u8{0} ** 4,
    front_pair_tags: [4]u8 = [_]u8{0} ** 4,
    side_single_tags: [4]u8 = [_]u8{0} ** 4,
    side_pair_tags: [4]u8 = [_]u8{0} ** 4,
    back_single_tags: [4]u8 = [_]u8{0} ** 4,
    back_pair_tags: [4]u8 = [_]u8{0} ** 4,
    lfe_tags: [4]u8 = [_]u8{0} ** 4,

    fn regularSingleCount(self: ProgramConfigLayout) u8 {
        return self.front_single_count + self.side_single_count + self.back_single_count;
    }

    fn pairCount(self: ProgramConfigLayout) u8 {
        return self.front_pair_count + self.side_pair_count + self.back_pair_count;
    }

    fn matchesSupportedOutput(self: ProgramConfigLayout, expected_channels: u8) bool {
        return switch (expected_channels) {
            1 => self.channel_count == 1 and self.pairCount() == 0 and
                ((self.regularSingleCount() == 1 and self.lfe_count == 0) or
                    (self.regularSingleCount() == 0 and self.lfe_count == 1)),
            2 => self.channel_count == 2 and self.lfe_count == 0 and
                ((self.pairCount() == 1 and self.regularSingleCount() == 0) or
                    (self.pairCount() == 0 and self.regularSingleCount() == 2)),
            else => false,
        };
    }

    fn matchesFirstElement(self: ProgramConfigLayout, expected_channels: u8, element: ElementHeader) bool {
        if (!self.matchesSupportedOutput(expected_channels)) return false;
        return switch (expected_channels) {
            1 => if (self.monoSceTag()) |tag|
                element.kind == .sce and element.element_instance_tag == tag
            else
                self.lfe_count == 1 and element.kind == .lfe and element.element_instance_tag == self.lfe_tags[0],
            2 => if (self.stereoPairTag()) |tag|
                element.kind == .cpe and element.element_instance_tag == tag
            else if (self.stereoSceTags()) |tags|
                element.kind == .sce and element.element_instance_tag == tags[0]
            else
                false,
            else => false,
        };
    }

    fn matchesSecondStereoSce(self: ProgramConfigLayout, element: ElementHeader) bool {
        const tags = self.stereoSceTags() orelse return false;
        return self.matchesSupportedOutput(2) and
            element.kind == .sce and
            element.element_instance_tag == tags[1];
    }

    fn monoSceTag(self: ProgramConfigLayout) ?u8 {
        if (self.regularSingleCount() != 1) return null;
        if (self.front_single_count == 1) return self.front_single_tags[0];
        if (self.side_single_count == 1) return self.side_single_tags[0];
        if (self.back_single_count == 1) return self.back_single_tags[0];
        return null;
    }

    fn stereoPairTag(self: ProgramConfigLayout) ?u8 {
        if (self.pairCount() != 1 or self.regularSingleCount() != 0 or self.lfe_count != 0) return null;
        if (self.front_pair_count == 1) return self.front_pair_tags[0];
        if (self.side_pair_count == 1) return self.side_pair_tags[0];
        if (self.back_pair_count == 1) return self.back_pair_tags[0];
        return null;
    }

    fn stereoSceTags(self: ProgramConfigLayout) ?[2]u8 {
        if (self.regularSingleCount() != 2 or self.pairCount() != 0 or self.lfe_count != 0) return null;
        var tags: [2]u8 = undefined;
        var count: usize = 0;
        for (self.front_single_tags[0..self.front_single_count]) |tag| {
            tags[count] = tag;
            count += 1;
        }
        for (self.side_single_tags[0..self.side_single_count]) |tag| {
            tags[count] = tag;
            count += 1;
        }
        for (self.back_single_tags[0..self.back_single_count]) |tag| {
            tags[count] = tag;
            count += 1;
        }
        return if (count == 2) tags else null;
    }
};

fn skipProgramConfigElement(reader: *BitReader) !void {
    _ = try parseProgramConfigElementPayload(reader);
}

fn parseProgramConfigElementWithTag(reader: *BitReader) !ProgramConfigLayout {
    _ = try reader.readBits(u8, 4); // element_instance_tag
    return parseProgramConfigElementPayload(reader);
}

fn parseProgramConfigElementPayload(reader: *BitReader) !ProgramConfigLayout {
    _ = try reader.readBits(u2, 2); // profile
    _ = try reader.readBits(u4, 4); // sampling_frequency_index
    const front_count = try reader.readBits(u8, 4);
    const side_count = try reader.readBits(u8, 4);
    const back_count = try reader.readBits(u8, 4);
    const lfe_count = try reader.readBits(u8, 2);
    const assoc_data_count = try reader.readBits(u8, 3);
    const valid_cc_count = try reader.readBits(u8, 4);

    if ((try reader.readBits(u1, 1)) != 0) try reader.skipBits(4);
    if ((try reader.readBits(u1, 1)) != 0) try reader.skipBits(4);
    if ((try reader.readBits(u1, 1)) != 0) try reader.skipBits(3);

    var layout = ProgramConfigLayout{ .channel_count = 0 };
    for (0..front_count) |_| {
        const element = try parseProgramConfigChannelElement(reader);
        addProgramConfigChannelElement(
            &layout.channel_count,
            &layout.front_single_count,
            &layout.front_pair_count,
            &layout.front_single_tags,
            &layout.front_pair_tags,
            element,
        );
    }
    for (0..side_count) |_| {
        const element = try parseProgramConfigChannelElement(reader);
        addProgramConfigChannelElement(
            &layout.channel_count,
            &layout.side_single_count,
            &layout.side_pair_count,
            &layout.side_single_tags,
            &layout.side_pair_tags,
            element,
        );
    }
    for (0..back_count) |_| {
        const element = try parseProgramConfigChannelElement(reader);
        addProgramConfigChannelElement(
            &layout.channel_count,
            &layout.back_single_count,
            &layout.back_pair_count,
            &layout.back_single_tags,
            &layout.back_pair_tags,
            element,
        );
    }
    for (0..lfe_count) |i| {
        const tag = try reader.readBits(u8, 4);
        if (i < layout.lfe_tags.len) layout.lfe_tags[i] = tag;
    }
    layout.channel_count += lfe_count;
    layout.lfe_count = lfe_count;
    for (0..assoc_data_count) |_| try reader.skipBits(4);
    for (0..valid_cc_count) |_| try reader.skipBits(5);

    try reader.alignToByte();
    const comment_bytes = try reader.readBits(u8, 8);
    try reader.skipBits(@as(usize, comment_bytes) * 8);

    return layout;
}

const ProgramConfigChannelElement = struct {
    is_pair: bool,
    tag: u8,
};

fn parseProgramConfigChannelElement(reader: *BitReader) !ProgramConfigChannelElement {
    const is_pair = (try reader.readBits(u1, 1)) != 0;
    const tag = try reader.readBits(u8, 4);
    return .{ .is_pair = is_pair, .tag = tag };
}

fn addProgramConfigChannelElement(
    channel_count: *u8,
    single_count: *u8,
    pair_count: *u8,
    single_tags: ?*[4]u8,
    pair_tags: ?*[4]u8,
    element: ProgramConfigChannelElement,
) void {
    if (element.is_pair) {
        if (pair_tags) |tags| {
            if (pair_count.* < tags.len) tags[pair_count.*] = element.tag;
        }
        pair_count.* += 1;
        channel_count.* += 2;
    } else {
        if (single_tags) |tags| {
            if (single_count.* < tags.len) tags[single_count.*] = element.tag;
        }
        single_count.* += 1;
        channel_count.* += 1;
    }
}

fn inferLeadingProgramConfigLayout(bytes: []const u8) !?ProgramConfigLayout {
    var reader = BitReader.init(bytes);
    while (true) {
        const kind = @as(ElementKind, @enumFromInt(try reader.readBits(u3, 3)));
        switch (kind) {
            .pce => return try parseProgramConfigElementWithTag(&reader),
            .fil => try skipFillElement(&reader),
            .dse => {
                _ = try reader.readBits(u8, 4);
                try skipDataStreamElement(&reader);
            },
            .end => {
                if (trailingBitsAreZero(reader.bytes, reader.bit_offset)) return null;
            },
            .sce, .cpe, .lfe, .cce => return null,
        }
    }
}

fn skipDataStreamElement(reader: *BitReader) !void {
    const byte_align = (try reader.readBits(u1, 1)) != 0;
    var count = try reader.readBits(u16, 8);
    if (count == 255) {
        count += try reader.readBits(u16, 8);
    }
    if (byte_align) {
        try reader.alignToByte();
    }
    try reader.skipBits(@as(usize, count) * 8);
}

fn decodePredictionData(reader: *BitReader, max_sfb: u8, sample_rate: u32) !struct {
    predictor_reset_group: u8,
    prediction_used: [max_prediction_bands]bool,
} {
    const sample_rate_index = try sampleRateIndexForRate(sample_rate);
    const band_limit = @min(max_sfb, predictor_sfb_max[sample_rate_index]);
    var predictor_reset_group: u8 = 0;
    var prediction_used = [_]bool{false} ** max_prediction_bands;

    if ((try reader.readBits(u1, 1)) != 0) {
        predictor_reset_group = try reader.readBits(u8, 5);
        if (predictor_reset_group == 0 or predictor_reset_group > 30) return error.UnsupportedAudioFormat;
    }
    for (0..band_limit) |sfb| {
        prediction_used[sfb] = (try reader.readBits(u1, 1)) != 0;
    }

    return .{
        .predictor_reset_group = predictor_reset_group,
        .prediction_used = prediction_used,
    };
}

fn parseIcsInfo(reader: *BitReader, sample_rate: ?u32) !IcsInfo {
    _ = try reader.readBits(u1, 1);
    const window_sequence = try reader.readBits(u2, 2);
    const window_shape = try reader.readBits(u1, 1);
    if (window_sequence == @intFromEnum(WindowSequence.eight_short)) {
        const max_sfb = try reader.readBits(u8, 4);
        const grouping = try reader.readBits(u8, 7);
        const group_lengths = shortWindowGroupLengths(grouping);
        return .{
            .window_sequence = @enumFromInt(window_sequence),
            .window_shape = window_shape,
            .max_sfb = max_sfb,
            .num_window_groups = group_lengths.num_groups,
            .window_group_length = group_lengths.lengths,
            .predictor_data_present = null,
            .predictor_reset_group = 0,
            .prediction_used = [_]bool{false} ** max_prediction_bands,
        };
    }

    const max_sfb = try reader.readBits(u8, 6);
    const predictor_data_present = (try reader.readBits(u1, 1)) != 0;
    var predictor_reset_group: u8 = 0;
    var prediction_used = [_]bool{false} ** max_prediction_bands;
    if (predictor_data_present) {
        const known_sample_rate = sample_rate orelse return error.UnsupportedAudioFormat;
        const prediction = try decodePredictionData(reader, max_sfb, known_sample_rate);
        predictor_reset_group = prediction.predictor_reset_group;
        prediction_used = prediction.prediction_used;
    }
    return .{
        .window_sequence = @enumFromInt(window_sequence),
        .window_shape = window_shape,
        .max_sfb = max_sfb,
        .num_window_groups = 1,
        .window_group_length = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = predictor_data_present,
        .predictor_reset_group = predictor_reset_group,
        .prediction_used = prediction_used,
    };
}

const ShortWindowGroups = struct {
    num_groups: u8,
    lengths: [8]u8,
};

fn shortWindowGroupLengths(grouping: u8) ShortWindowGroups {
    var lengths = [_]u8{0} ** 8;
    lengths[0] = 1;
    var groups: u8 = 1;
    for (0..7) |i| {
        const shift: u3 = @intCast(6 - i);
        if (((grouping >> shift) & 1) != 0) {
            lengths[groups - 1] += 1;
        } else {
            lengths[groups] = 1;
            groups += 1;
        }
    }
    return .{
        .num_groups = groups,
        .lengths = lengths,
    };
}

fn sampleRateFromIndex(index: u8) !u32 {
    if (index == 0x0f) return error.UnsupportedAudioFormat;
    if (index >= sample_rate_table.len) return error.UnsupportedAudioFormat;
    return sample_rate_table[index];
}

fn sampleRateIndexForRate(sample_rate: u32) !usize {
    for (sample_rate_table, 0..) |candidate, index| {
        if (candidate == sample_rate) return index;
    }
    return error.UnsupportedAudioFormat;
}

fn readSamplingFrequency(reader: *BitReader) !u32 {
    const index = try reader.readBits(u8, 4);
    if (index == 0x0f) return try reader.readBits(u32, 24);
    return sampleRateFromIndex(index);
}

fn readAudioObjectType(reader: *BitReader) !u8 {
    const object_type = try reader.readBits(u8, 5);
    if (object_type == 31) return 32 + try reader.readBits(u8, 6);
    return object_type;
}

const SyncExtension = struct {
    object_type: u8,
    sample_rate: u32,
};

fn findGaSpecificSyncExtension(bytes: []const u8, start_bit_offset: usize) ?SyncExtension {
    var bit_offset: usize = start_bit_offset;
    while (bit_offset + 11 <= bytes.len * 8) : (bit_offset += 1) {
        if (peekBits(bytes, bit_offset, 11) != 0x2b7) continue;
        var reader = BitReader.initFromBitOffset(bytes, bit_offset + 11);
        const extension_object_type = readAudioObjectType(&reader) catch continue;
        if (extension_object_type != 5 and extension_object_type != 29) continue;
        const extension_sample_rate = readSamplingFrequency(&reader) catch continue;
        return .{
            .object_type = extension_object_type,
            .sample_rate = extension_sample_rate,
        };
    }
    return null;
}

fn peekBitsValue(bytes: []const u8, bit_offset: usize, bit_count: usize) u64 {
    if (bit_count == 0) return 0;
    if (bit_count <= 56) {
        const byte_offset = bit_offset / 8;
        const bit_in_byte = bit_offset % 8;
        const total_bits = bit_in_byte + bit_count;
        const byte_count = (total_bits + 7) / 8;
        var value: u64 = 0;
        for (0..byte_count) |i| {
            value = (value << 8) | bytes[byte_offset + i];
        }
        const unused_low_bits: u6 = @intCast(byte_count * 8 - total_bits);
        const mask = (@as(u64, 1) << @as(u6, @intCast(bit_count))) - 1;
        return (value >> unused_low_bits) & mask;
    }

    var value: u64 = 0;
    var cursor = bit_offset;
    for (0..bit_count) |_| {
        value <<= 1;
        const byte = bytes[cursor / 8];
        const shift: u3 = @intCast(7 - (cursor % 8));
        value |= @as(u64, (byte >> shift) & 1);
        cursor += 1;
    }
    return value;
}

fn peekBits(bytes: []const u8, bit_offset: usize, bit_count: usize) u16 {
    return @intCast(peekBitsValue(bytes, bit_offset, bit_count));
}

const BitReader = struct {
    bytes: []const u8,
    bit_offset: usize = 0,

    fn init(bytes: []const u8) BitReader {
        return .{ .bytes = bytes };
    }

    fn initFromBitOffset(bytes: []const u8, bit_offset: usize) BitReader {
        return .{ .bytes = bytes, .bit_offset = bit_offset };
    }

    fn remainingBits(self: BitReader) usize {
        return self.bytes.len * 8 -| self.bit_offset;
    }

    fn readBits(self: *BitReader, comptime T: type, bit_count: usize) !T {
        if (bit_count == 0) return 0;
        if (self.bit_offset + bit_count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        const value = peekBitsValue(self.bytes, self.bit_offset, bit_count);
        self.bit_offset += bit_count;
        return @intCast(value);
    }

    fn skipBits(self: *BitReader, bit_count: usize) !void {
        if (self.bit_offset + bit_count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        self.bit_offset += bit_count;
    }

    fn peekBits(self: *BitReader, comptime T: type, bit_count: usize) !T {
        if (bit_count == 0) return 0;
        if (self.bit_offset + bit_count > self.bytes.len * 8) return error.UnsupportedAudioFormat;
        const value = peekBitsValue(self.bytes, self.bit_offset, bit_count);
        return @intCast(value);
    }

    fn alignToByte(self: *BitReader) !void {
        const remainder = self.bit_offset % 8;
        if (remainder == 0) return;
        try self.skipBits(8 - remainder);
    }
};

test "parse adts header for checked-in stereo aac fixture" {
    const header = try parseAdtsHeader(tone_aac_bytes);
    try std.testing.expectEqual(@as(u8, 2), header.object_type);
    try std.testing.expectEqual(@as(u32, 16000), header.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), header.channel_config);
    try std.testing.expectEqual(@as(usize, 1024), header.samplesPerChannel());
}

test "parse adts header for checked-in mono aac fixture" {
    const header = try parseAdtsHeader(tone_aac_44k_mono_bytes);
    try std.testing.expectEqual(@as(u8, 2), header.object_type);
    try std.testing.expectEqual(@as(u32, 44100), header.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), header.channel_config);
}

test "parse adts header rejects nonzero layer bits" {
    const frame = [_]u8{
        0xff,
        0xf3, // layer must be zero; this would otherwise look like a sync header.
        0x50,
        0x80,
        0x01,
        0x1f,
        0xfc,
    };
    try std.testing.expectError(error.UnsupportedAudioFormat, parseAdtsHeader(&frame));
    try std.testing.expectError(error.UnsupportedAudioFormat, parseAdtsFrame(&frame));
}

test "scan checked-in adts fixtures into exact frame counts" {
    const stereo = try scanAdtsFramesAlloc(std.testing.allocator, tone_aac_bytes);
    defer std.testing.allocator.free(stereo);
    try std.testing.expectEqual(@as(usize, 17), stereo.len);
    try std.testing.expectEqual(@as(u16, 672), stereo[0].header.frame_length);
    try std.testing.expectEqual(@as(u16, 923), stereo[1].header.frame_length);

    const mono = try scanAdtsFramesAlloc(std.testing.allocator, tone_aac_44k_mono_bytes);
    defer std.testing.allocator.free(mono);
    try std.testing.expectEqual(@as(usize, 45), mono.len);
    try std.testing.expectEqual(@as(u16, 319), mono[0].header.frame_length);
    try std.testing.expectEqual(@as(u16, 387), mono[1].header.frame_length);

    const short = try scanAdtsFramesAlloc(std.testing.allocator, transient_aac_44k_short_bytes);
    defer std.testing.allocator.free(short);
    try std.testing.expect(short.len > 8);

    const stereo_short = try scanAdtsFramesAlloc(std.testing.allocator, transient_stereo_aac_44k_short_bytes);
    defer std.testing.allocator.free(stereo_short);
    try std.testing.expect(stereo_short.len > 8);
}

test "scan adts frames skips leading id3v2 tag" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast(0x80 | ((frame_len >> 11) & 0x03)),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var plain_bytes = std.ArrayList(u8).empty;
    defer plain_bytes.deinit(std.testing.allocator);
    try plain_bytes.appendSlice(std.testing.allocator, &frame);
    try plain_bytes.appendSlice(std.testing.allocator, payload);

    var tagged = std.ArrayList(u8).empty;
    defer tagged.deinit(std.testing.allocator);
    try tagged.appendSlice(std.testing.allocator, &.{ 'I', 'D', '3', 4, 0, 0, 0, 0, 0, 0 });
    try tagged.appendSlice(std.testing.allocator, plain_bytes.items);

    const plain = try scanAdtsFramesAlloc(std.testing.allocator, plain_bytes.items);
    defer std.testing.allocator.free(plain);
    const with_tag = try scanAdtsFramesAlloc(std.testing.allocator, tagged.items);
    defer std.testing.allocator.free(with_tag);

    try std.testing.expectEqual(plain.len, with_tag.len);
    try std.testing.expectEqual(plain[0].header.frame_length, with_tag[0].header.frame_length);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, tagged.items);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(plain.len, decoded.frame_count);
}

test "scan adts frames skips sized leading id3v2 tag with footer" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast(0x80 | ((frame_len >> 11) & 0x03)),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var plain_bytes = std.ArrayList(u8).empty;
    defer plain_bytes.deinit(std.testing.allocator);
    try plain_bytes.appendSlice(std.testing.allocator, &frame);
    try plain_bytes.appendSlice(std.testing.allocator, payload);

    var tagged = std.ArrayList(u8).empty;
    defer tagged.deinit(std.testing.allocator);
    try tagged.appendSlice(std.testing.allocator, &.{ 'I', 'D', '3', 4, 0, 0x10, 0, 0, 0, 4 });
    try tagged.appendSlice(std.testing.allocator, &.{ 't', 'e', 's', 't' });
    try tagged.appendSlice(std.testing.allocator, &.{ '3', 'D', 'I', 4, 0, 0x10, 0, 0, 0, 4 });
    try tagged.appendSlice(std.testing.allocator, plain_bytes.items);

    const plain = try scanAdtsFramesAlloc(std.testing.allocator, plain_bytes.items);
    defer std.testing.allocator.free(plain);
    const with_tag = try scanAdtsFramesAlloc(std.testing.allocator, tagged.items);
    defer std.testing.allocator.free(with_tag);

    try std.testing.expectEqual(plain.len, with_tag.len);
    try std.testing.expectEqual(plain[0].header.frame_length, with_tag[0].header.frame_length);
}

test "scan adts frames skips interstitial id3v2 tag" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast(0x80 | ((frame_len >> 11) & 0x03)),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var tagged = std.ArrayList(u8).empty;
    defer tagged.deinit(std.testing.allocator);
    try tagged.appendSlice(std.testing.allocator, &frame);
    try tagged.appendSlice(std.testing.allocator, payload);
    try tagged.appendSlice(std.testing.allocator, &.{ 'I', 'D', '3', 4, 0, 0, 0, 0, 0, 0 });
    try tagged.appendSlice(std.testing.allocator, &frame);
    try tagged.appendSlice(std.testing.allocator, payload);

    const with_tag = try scanAdtsFramesAlloc(std.testing.allocator, tagged.items);
    defer std.testing.allocator.free(with_tag);

    try std.testing.expectEqual(@as(usize, 2), with_tag.len);
    try std.testing.expectEqual(frame_len, with_tag[0].header.frame_length);
    try std.testing.expectEqual(frame_len, with_tag[1].header.frame_length);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, tagged.items);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
}

test "scan adts frames skips trailing id3v1 tag" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast(0x80 | ((frame_len >> 11) & 0x03)),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var plain_bytes = std.ArrayList(u8).empty;
    defer plain_bytes.deinit(std.testing.allocator);
    try plain_bytes.appendSlice(std.testing.allocator, &frame);
    try plain_bytes.appendSlice(std.testing.allocator, payload);

    var id3v1 = [_]u8{0} ** 128;
    id3v1[0] = 'T';
    id3v1[1] = 'A';
    id3v1[2] = 'G';

    var tagged = std.ArrayList(u8).empty;
    defer tagged.deinit(std.testing.allocator);
    try tagged.appendSlice(std.testing.allocator, plain_bytes.items);
    try tagged.appendSlice(std.testing.allocator, &id3v1);

    const plain = try scanAdtsFramesAlloc(std.testing.allocator, plain_bytes.items);
    defer std.testing.allocator.free(plain);
    const with_tag = try scanAdtsFramesAlloc(std.testing.allocator, tagged.items);
    defer std.testing.allocator.free(with_tag);

    try std.testing.expectEqual(plain.len, with_tag.len);
    try std.testing.expectEqual(plain[0].header.frame_length, with_tag[0].header.frame_length);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, tagged.items);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(plain.len, decoded.frame_count);
}

test "decode adts crc-protected frame skips crc bytes" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(9 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf0,
        0x50,
        @intCast(0x80 | ((frame_len >> 11) & 0x03)),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
        0x12,
        0x34,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    const parsed = try parseAdtsFrame(adts.items);
    try std.testing.expectEqual(@as(bool, false), parsed.header.protection_absent);
    try std.testing.expectEqual(@as(usize, 9), parsed.header_len);
    try std.testing.expectEqual(payload.len, parsed.payload.len);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "decode adts frame with two crc-absent raw data blocks" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast(0x80 | ((frame_len >> 11) & 0x03)),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfd, // one extra raw_data_block, so two access units total
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    const parsed = try parseAdtsFrame(adts.items);
    try std.testing.expectEqual(@as(u8, 1), parsed.header.data_blocks_in_frame);
    try std.testing.expectEqual(@as(usize, 2048), parsed.header.samplesPerChannel());

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "decode adts crc-protected frame with two raw data blocks skips block crcs" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x1234, 16); // first raw_data_block CRC
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x5678, 16); // second raw_data_block CRC

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(11 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf0,
        0x50,
        @intCast(0x80 | ((frame_len >> 11) & 0x03)),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfd, // one extra raw_data_block, so one position word follows
        0x00,
        0x00, // raw_data_block_position, currently skipped rather than trusted
        0xab,
        0xcd, // header CRC
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    const parsed = try parseAdtsFrame(adts.items);
    try std.testing.expectEqual(@as(bool, false), parsed.header.protection_absent);
    try std.testing.expectEqual(@as(u8, 1), parsed.header.data_blocks_in_frame);
    try std.testing.expectEqual(@as(usize, 11), parsed.header_len);
    try std.testing.expectEqual(payload.len, parsed.payload.len);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts fixed channel config skips metadata-only leading mono frame" {
    var metadata_payload_builder = TestBitBuilder.init();
    defer metadata_payload_builder.deinit(std.testing.allocator);
    try metadata_payload_builder.appendMetadataOnlyDseFilEnd(std.testing.allocator);

    var audio_payload_builder = TestBitBuilder.init();
    defer audio_payload_builder.deinit(std.testing.allocator);
    try audio_payload_builder.appendSilentMonoSce(std.testing.allocator);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, metadata_payload_builder.bytes.items, .{
        .channel_config = 1,
    });
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, audio_payload_builder.bytes.items, .{
        .channel_config = 1,
    });

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts fixed channel config rejects metadata-only mono stream" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendMetadataOnlyDseFilEnd(std.testing.allocator);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .channel_config = 1,
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts fixed channel config skips metadata-only first stereo raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendMetadataOnlyDseFilEnd(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .channel_config = 2,
        .data_blocks_in_frame = 1,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts fixed channel config skips metadata-only crc-protected first stereo raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendMetadataOnlyDseFilEnd(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, 0x1234, 16); // first raw_data_block CRC
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x5678, 16); // second raw_data_block CRC

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    const raw_data_block_positions = [_]u16{0};
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .channel_config = 2,
        .protection_absent = false,
        .data_blocks_in_frame = 1,
        .raw_data_block_positions = &raw_data_block_positions,
        .header_crc = 0xabcd,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "summarize first raw access unit element for checked-in aac fixtures" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    const stereo_summary = try summarizeAccessUnit(stereo_frame.payload);
    try std.testing.expectEqual(ElementKind.cpe, stereo_summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), stereo_summary.element_instance_tag);

    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    const mono_summary = try summarizeAccessUnit(mono_frame.payload);
    try std.testing.expectEqual(ElementKind.sce, mono_summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), mono_summary.element_instance_tag);
    try std.testing.expect(mono_summary.first_channel_global_gain != null);

    const short_frame = try parseAdtsFrame(transient_aac_44k_short_bytes);
    const short_summary = try summarizeAccessUnit(short_frame.payload);
    try std.testing.expectEqual(ElementKind.sce, short_summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), short_summary.element_instance_tag);
    try std.testing.expect(short_summary.first_channel_global_gain != null);

    const stereo_short_frame = try parseAdtsFrame(transient_stereo_aac_44k_short_bytes);
    const stereo_short_summary = try summarizeAccessUnit(stereo_short_frame.payload);
    try std.testing.expectEqual(ElementKind.cpe, stereo_short_summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), stereo_short_summary.element_instance_tag);
}

test "parse first element prefix for checked-in aac fixtures" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    const stereo_prefix = try parseFirstElementPrefix(stereo_frame.payload);
    switch (stereo_prefix) {
        .cpe => |cpe| {
            try std.testing.expectEqual(@as(u8, 0), cpe.element_instance_tag);
            try std.testing.expectEqual(@as(u8, 140), cpe.left_global_gain);
            if (cpe.shared_ics_info) |info| {
                try std.testing.expect(info.max_sfb > 0);
            } else if (cpe.left_ics_info) |info| {
                try std.testing.expect(info.max_sfb > 0);
            } else {
                return error.TestExpectedEqual;
            }
        },
        else => return error.TestUnexpectedResult,
    }

    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    const mono_prefix = try parseFirstElementPrefix(mono_frame.payload);
    switch (mono_prefix) {
        .sce => |sce| {
            try std.testing.expectEqual(@as(u8, 0), sce.element_instance_tag);
            try std.testing.expect(sce.ics_info.max_sfb > 0);
            try std.testing.expect(sce.ics_info.num_window_groups >= 1);
        },
        else => return error.TestUnexpectedResult,
    }

    const short_frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_aac_44k_short_bytes);
    defer std.testing.allocator.free(short_frames);
    var short_count: usize = 0;
    var first_short: ?usize = null;
    for (short_frames, 0..) |frame, i| {
        const short_prefix = try parseFirstElementPrefix(frame.payload);
        switch (short_prefix) {
            .sce => |sce| {
                if (sce.ics_info.window_sequence == .eight_short) {
                    short_count += 1;
                    if (first_short == null) first_short = i;
                }
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expect(short_count > 0);
    try std.testing.expectEqual(@as(?usize, 4), first_short);

    const stereo_short_frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_stereo_aac_44k_short_bytes);
    defer std.testing.allocator.free(stereo_short_frames);
    var stereo_short_count: usize = 0;
    for (stereo_short_frames) |frame| {
        const prefix = try parseFirstElementPrefix(frame.payload);
        switch (prefix) {
            .cpe => |cpe| {
                const info = cpe.shared_ics_info orelse cpe.left_ics_info orelse return error.TestUnexpectedResult;
                if (info.window_sequence == .eight_short) {
                    stereo_short_count += 1;
                }
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expect(stereo_short_count > 0);
}

test "parse synthetic eight-short ics info keeps window grouping lengths" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 9, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1011000, 7); // 2, 3, 1, 1, 1

    var reader = BitReader.init(builder.bytes.items);
    const info = try parseIcsInfo(&reader, null);
    try std.testing.expectEqual(WindowSequence.eight_short, info.window_sequence);
    try std.testing.expectEqual(@as(u8, 9), info.max_sfb);
    try std.testing.expectEqual(@as(u8, 5), info.num_window_groups);
    try std.testing.expectEqualSlices(u8, &.{ 2, 3, 1, 1, 1 }, info.window_group_length[0..5]);
}

test "parse synthetic long-window ics info uses single group length" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 42, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present

    var reader = BitReader.init(builder.bytes.items);
    const info = try parseIcsInfo(&reader, null);
    try std.testing.expectEqual(WindowSequence.only_long, info.window_sequence);
    try std.testing.expectEqual(@as(u8, 1), info.num_window_groups);
    try std.testing.expectEqual(@as(u8, 1), info.window_group_length[0]);
    for (info.window_group_length[1..]) |len| {
        try std.testing.expectEqual(@as(u8, 0), len);
    }
}

test "parse synthetic long-window ics info rejects predictor data" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 42, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 1, 1); // predictor_data_present

    var reader = BitReader.init(builder.bytes.items);
    try std.testing.expectError(error.UnsupportedAudioFormat, parseIcsInfo(&reader, null));
}

test "parse synthetic long-window ics info decodes predictor data when sample rate is known" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 1, 1); // predictor_data_present
    try builder.appendBits(std.testing.allocator, 1, 1); // predictor_reset
    try builder.appendBits(std.testing.allocator, 7, 5); // predictor_reset_group
    try builder.appendBits(std.testing.allocator, 1, 1); // prediction_used[0]
    try builder.appendBits(std.testing.allocator, 0, 1); // prediction_used[1]

    var reader = BitReader.init(builder.bytes.items);
    const info = try parseIcsInfo(&reader, 44100);
    try std.testing.expectEqual(WindowSequence.only_long, info.window_sequence);
    try std.testing.expectEqual(@as(bool, true), info.predictor_data_present.?);
    try std.testing.expectEqual(@as(u8, 7), info.predictor_reset_group);
    try std.testing.expectEqual(@as(bool, true), info.prediction_used[0]);
    try std.testing.expectEqual(@as(bool, false), info.prediction_used[1]);
}

test "build grouped short band offsets multiplies widths by group length" {
    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 5,
        .window_group_length = .{ 2, 3, 1, 1, 1, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };

    const offsets = try buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info);
    defer std.testing.allocator.free(offsets);

    try std.testing.expectEqualSlices(u16, &.{ 0, 8, 16, 28, 40, 44, 48, 52, 56, 60, 64 }, offsets);
}

test "build grouped short band offsets rejects invalid total window count" {
    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 2,
        .window_group_length = .{ 2, 2, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info),
    );
}

test "build spectral layouts supports grouped short band offsets" {
    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 5,
        .window_group_length = .{ 2, 3, 1, 1, 1, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };
    const coeff_offsets = try buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info);
    defer std.testing.allocator.free(coeff_offsets);

    const bands = [_]ScalefactorBand{
        .{ .band_type = ZERO_BT, .kind = .zero, .value = 0 },
        .{ .band_type = 4, .kind = .spectral, .value = 120 },
        .{ .band_type = 11, .kind = .spectral, .value = 118 },
        .{ .band_type = NOISE_BT, .kind = .noise, .value = 0 },
        .{ .band_type = 2, .kind = .spectral, .value = 117 },
        .{ .band_type = INTENSITY_BT, .kind = .intensity, .value = 0 },
        .{ .band_type = 8, .kind = .spectral, .value = 115 },
        .{ .band_type = ZERO_BT, .kind = .zero, .value = 0 },
        .{ .band_type = 6, .kind = .spectral, .value = 112 },
        .{ .band_type = 10, .kind = .spectral, .value = 111 },
    };

    const plans = try buildSpectralLayoutsAlloc(std.testing.allocator, &bands, coeff_offsets);
    defer std.testing.allocator.free(plans);

    try std.testing.expectEqual(@as(usize, bands.len), plans.len);
    try std.testing.expectEqual(@as(u16, 0), plans[0].coeff_start);
    try std.testing.expectEqual(@as(u16, 8), plans[0].coeff_end);
    try std.testing.expectEqual(@as(u16, 0), plans[0].symbol_count);
    try std.testing.expectEqual(SpectralCodebookClass.quad, plans[1].class);
    try std.testing.expectEqual(@as(u16, 2), plans[1].symbol_count);
    try std.testing.expectEqual(SpectralCodebookClass.escape, plans[2].class);
    try std.testing.expectEqual(@as(u16, 6), plans[2].symbol_count);
    try std.testing.expectEqual(SpectralCodebookClass.noise, plans[3].class);
    try std.testing.expectEqual(@as(u16, 0), plans[3].symbol_count);
    try std.testing.expectEqual(SpectralCodebookClass.intensity, plans[5].class);
    try std.testing.expectEqual(@as(u16, 0), plans[5].symbol_count);
    try std.testing.expectEqual(@as(u16, 56), plans[8].coeff_start);
    try std.testing.expectEqual(@as(u16, 60), plans[8].coeff_end);
    try std.testing.expectEqual(@as(u16, 2), plans[8].symbol_count);
    try std.testing.expectEqual(@as(u16, 60), plans[9].coeff_start);
    try std.testing.expectEqual(@as(u16, 64), plans[9].coeff_end);
    try std.testing.expectEqual(@as(u16, 2), plans[9].symbol_count);
}

test "init synthetic eight-short spectral state with supplied offsets" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4); // section band_type
    try builder.appendBits(std.testing.allocator, 2, 3); // section length to max_sfb
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 1,
        .window_group_length = .{ 8, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };
    const coeff_offsets = try buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info);
    defer std.testing.allocator.free(coeff_offsets);

    var state = try initFirstChannelSpectralStateWithCoeffOffsetsAlloc(
        std.testing.allocator,
        coeff_offsets,
        builder.bytes.items,
    );
    defer state.deinit();

    try std.testing.expectEqual(WindowSequence.eight_short, state.ics_info.window_sequence);
    try std.testing.expectEqual(@as(u8, 1), state.ics_info.num_window_groups);
    try std.testing.expectEqual(@as(u8, 8), state.ics_info.window_group_length[0]);
    try std.testing.expectEqual(@as(usize, 1), state.sections.len);
    try std.testing.expectEqual(@as(usize, 2), state.bands.len);
    try std.testing.expectEqual(@as(usize, 2), state.plans.len);
    try std.testing.expectEqual(SpectralCodebookClass.zero, state.plans[0].class);
    try std.testing.expectEqual(@as(u16, 0), state.plans[0].coeff_start);
    try std.testing.expectEqual(@as(u16, 32), state.plans[0].coeff_end);
    try std.testing.expectEqual(@as(u16, 0), state.plans[0].symbol_count);
    try std.testing.expectEqual(@as(u16, 32), state.plans[1].coeff_start);
    try std.testing.expectEqual(@as(u16, 64), state.plans[1].coeff_end);
    try std.testing.expectEqual(@as(u16, 0), state.plans[1].symbol_count);
}

test "parse synthetic eight-short spectral layout plan with actual swb_offset_128 tables" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3);
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var plan = try parseFirstChannelSpectralLayoutPlanAlloc(
        std.testing.allocator,
        44100,
        builder.bytes.items,
    );
    defer plan.deinit();

    try std.testing.expectEqual(WindowSequence.eight_short, plan.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 2), plan.plans.len);
    try std.testing.expectEqual(@as(u16, 0), plan.plans[0].coeff_start);
    try std.testing.expectEqual(@as(u16, 32), plan.plans[0].coeff_end);
    try std.testing.expectEqual(@as(u16, 0), plan.plans[0].symbol_count);
    try std.testing.expectEqual(@as(u16, 32), plan.plans[1].coeff_start);
    try std.testing.expectEqual(@as(u16, 64), plan.plans[1].coeff_end);
    try std.testing.expectEqual(@as(u16, 0), plan.plans[1].symbol_count);
}

test "aac swb offset tables cover long and short profile sample rates" {
    const long_cases = [_]struct {
        sample_rate: u32,
        last_before_end: u16,
        len: usize,
    }{
        .{ .sample_rate = 96000, .last_before_end = 960, .len = swb_offset_1024_96.len },
        .{ .sample_rate = 88200, .last_before_end = 960, .len = swb_offset_1024_96.len },
        .{ .sample_rate = 64000, .last_before_end = 984, .len = swb_offset_1024_64.len },
        .{ .sample_rate = 32000, .last_before_end = 992, .len = swb_offset_1024_32.len },
        .{ .sample_rate = 24000, .last_before_end = 960, .len = swb_offset_1024_24.len },
        .{ .sample_rate = 22050, .last_before_end = 960, .len = swb_offset_1024_24.len },
        .{ .sample_rate = 12000, .last_before_end = 960, .len = swb_offset_1024_16.len },
        .{ .sample_rate = 11025, .last_before_end = 960, .len = swb_offset_1024_16.len },
        .{ .sample_rate = 8000, .last_before_end = 944, .len = swb_offset_1024_8.len },
        .{ .sample_rate = 7350, .last_before_end = 944, .len = swb_offset_1024_8.len },
    };
    for (long_cases) |case| {
        const offsets = try swbOffsets1024(case.sample_rate);
        try std.testing.expectEqual(@as(u16, 0), offsets[0]);
        try std.testing.expectEqual(case.last_before_end, offsets[offsets.len - 2]);
        try std.testing.expectEqual(@as(u16, 1024), offsets[offsets.len - 1]);
        try std.testing.expectEqual(case.len, offsets.len);
    }

    const short_96 = try swbOffsets128(96000);
    try std.testing.expectEqual(@as(u16, 0), short_96[0]);
    try std.testing.expectEqual(@as(u16, 92), short_96[short_96.len - 2]);
    try std.testing.expectEqual(@as(u16, 128), short_96[short_96.len - 1]);
    try std.testing.expectEqual(swb_offset_128_96.len, short_96.len);
}

test "dequantize synthetic eight-short spectral state with supplied offsets stays zero" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3);
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 1,
        .window_group_length = .{ 8, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };
    const coeff_offsets = try buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info);
    defer std.testing.allocator.free(coeff_offsets);

    var dequantized = try dequantizeFirstChannelSpectralCoefficientsWithCoeffOffsetsAlloc(
        std.testing.allocator,
        coeff_offsets,
        builder.bytes.items,
    );
    defer dequantized.deinit();

    try std.testing.expectEqual(WindowSequence.eight_short, dequantized.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 1024), dequantized.coefficients.len);
    try std.testing.expectEqual(@as(bool, false), dequantized.contains_noise);
    try std.testing.expectEqual(@as(bool, false), dequantized.contains_intensity);
    for (dequantized.coefficients) |coeff| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
    }
}

test "dequantize synthetic eight-short spectral state with actual swb_offset_128 tables stays zero" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3);
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var dequantized = try dequantizeFirstChannelSpectralCoefficientsAlloc(
        std.testing.allocator,
        44100,
        builder.bytes.items,
    );
    defer dequantized.deinit();

    try std.testing.expectEqual(WindowSequence.eight_short, dequantized.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 1024), dequantized.coefficients.len);
    for (dequantized.coefficients) |coeff| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
    }
}

test "window synthetic eight-short spectral state with supplied offsets stays zero" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3);
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 1,
        .window_group_length = .{ 8, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };
    const coeff_offsets = try buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info);
    defer std.testing.allocator.free(coeff_offsets);

    var seq = try windowFirstChannelShortSequenceWithCoeffOffsetsAlloc(
        std.testing.allocator,
        coeff_offsets,
        builder.bytes.items,
    );
    defer seq.deinit();

    try std.testing.expectEqual(@as(usize, 2048), seq.samples.len);
    for (seq.samples) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "decode synthetic eight-short pcm block with supplied offsets stays zero" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3);
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 1,
        .window_group_length = .{ 8, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };
    const coeff_offsets = try buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info);
    defer std.testing.allocator.free(coeff_offsets);

    var block = try decodeFirstChannelShortPcmBlockWithCoeffOffsetsAlloc(
        std.testing.allocator,
        null,
        coeff_offsets,
        builder.bytes.items,
    );
    defer block.deinit();

    try std.testing.expectEqual(@as(usize, 1024), block.pcm.len);
    try std.testing.expectEqual(@as(usize, 1024), block.tail.len);
    for (block.pcm) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
    for (block.tail) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "decode synthetic eight-short pcm block adds previous tail" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3);
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    const ics_info: IcsInfo = .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 2,
        .num_window_groups = 1,
        .window_group_length = .{ 8, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    };
    const swb_offsets = [_]u16{ 0, 4, 8 };
    const coeff_offsets = try buildGroupedShortBandOffsetsAlloc(std.testing.allocator, &swb_offsets, ics_info);
    defer std.testing.allocator.free(coeff_offsets);

    var prev: [1024]f32 = undefined;
    for (&prev, 0..) |*sample, i| sample.* = @as(f32, 0.25) * @as(f32, @floatFromInt(i));

    var block = try decodeFirstChannelShortPcmBlockWithCoeffOffsetsAlloc(
        std.testing.allocator,
        &prev,
        coeff_offsets,
        builder.bytes.items,
    );
    defer block.deinit();

    for (block.pcm, 0..) |sample, i| {
        try std.testing.expectApproxEqAbs(prev[i], sample, 1e-6);
    }
    for (block.tail) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "decode first-channel pcm block accepts synthetic eight-short data with actual swb_offset_128 tables" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3);
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var block = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        44100,
        null,
        builder.bytes.items,
    );
    defer block.deinit();

    try std.testing.expectEqual(WindowSequence.eight_short, block.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 1024), block.pcm.len);
    try std.testing.expectEqual(@as(usize, 1024), block.tail.len);
    try std.testing.expectEqual(@as(bool, false), block.contains_noise);
    try std.testing.expectEqual(@as(bool, false), block.contains_intensity);
    for (block.pcm) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
    for (block.tail) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "parse first-channel section data for checked-in aac fixtures" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var stereo = try parseFirstChannelSectionDataAlloc(std.testing.allocator, stereo_frame.payload);
    defer stereo.deinit();
    try std.testing.expect(stereo.sections.len > 0);
    var stereo_covered: u8 = 0;
    for (stereo.sections) |section| {
        try std.testing.expect(section.end_sfb > section.start_sfb);
        stereo_covered += section.end_sfb - section.start_sfb;
    }
    try std.testing.expectEqual(stereo.ics_info.max_sfb * stereo.ics_info.num_window_groups, stereo_covered);

    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try parseFirstChannelSectionDataAlloc(std.testing.allocator, mono_frame.payload);
    defer mono.deinit();
    try std.testing.expect(mono.sections.len > 0);
    var mono_covered: u8 = 0;
    for (mono.sections) |section| {
        try std.testing.expect(section.end_sfb > section.start_sfb);
        mono_covered += section.end_sfb - section.start_sfb;
    }
    try std.testing.expectEqual(mono.ics_info.max_sfb * mono.ics_info.num_window_groups, mono_covered);
}

test "parse first-channel scalefactors for checked-in aac fixtures" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var stereo = try parseFirstChannelScalefactorsAlloc(std.testing.allocator, stereo_frame.payload);
    defer stereo.deinit();
    try std.testing.expectEqual(@as(usize, 43), stereo.bands.len);
    for (stereo.bands) |band| {
        try std.testing.expectEqual(ScalefactorKind.spectral, band.kind);
    }
    try std.testing.expectEqual(@as(i16, 140), stereo.bands[0].value);
    try std.testing.expectEqual(@as(i16, 139), stereo.bands[1].value);
    try std.testing.expectEqual(@as(i16, 150), stereo.bands[7].value);
    try std.testing.expectEqual(@as(i16, 129), stereo.bands[20].value);
    try std.testing.expectEqual(@as(i16, 124), stereo.bands[40].value);
    try std.testing.expectEqual(@as(i16, 101), stereo.bands[42].value);

    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try parseFirstChannelScalefactorsAlloc(std.testing.allocator, mono_frame.payload);
    defer mono.deinit();
    try std.testing.expectEqual(@as(usize, 47), mono.bands.len);
    for (mono.bands[0..32]) |band| {
        try std.testing.expectEqual(ScalefactorKind.spectral, band.kind);
        try std.testing.expect(band.band_type == 11 or band.band_type == 10);
    }
    for (mono.bands[32..]) |band| {
        try std.testing.expectEqual(ScalefactorKind.noise, band.kind);
        try std.testing.expectEqual(@as(u4, 13), band.band_type);
    }
}

test "parse first-channel post-scalefactor tool flags for checked-in aac fixtures" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var stereo = try parseFirstChannelPostScalefactorToolsAlloc(std.testing.allocator, stereo_frame.payload);
    defer stereo.deinit();
    try std.testing.expectEqual(@as(bool, false), stereo.pulse_present);
    try std.testing.expectEqual(@as(bool, false), stereo.tns_present);
    try std.testing.expectEqual(@as(bool, false), stereo.gain_control_present);
    try std.testing.expectEqual(@as(?PulseData, null), stereo.pulse_data);
    try std.testing.expectEqual(@as(?TnsData, null), stereo.tns_data);
    try std.testing.expectEqual(@as(usize, 43), stereo.bands.len);

    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try parseFirstChannelPostScalefactorToolsAlloc(std.testing.allocator, mono_frame.payload);
    defer mono.deinit();
    try std.testing.expectEqual(@as(bool, false), mono.pulse_present);
    try std.testing.expectEqual(@as(bool, false), mono.tns_present);
    try std.testing.expectEqual(@as(bool, false), mono.gain_control_present);
    try std.testing.expectEqual(@as(?PulseData, null), mono.pulse_data);
    try std.testing.expectEqual(@as(?TnsData, null), mono.tns_data);
    try std.testing.expectEqual(@as(usize, 47), mono.bands.len);
}

test "parse first-channel spectral plan for checked-in aac fixtures" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var stereo = try parseFirstChannelSpectralPlanAlloc(std.testing.allocator, stereo_frame.payload);
    defer stereo.deinit();
    try std.testing.expectEqual(@as(usize, 43), stereo.plans.len);
    try std.testing.expectEqual(SpectralCodebookClass.escape, stereo.plans[0].class);
    try std.testing.expectEqual(@as(u8, 2), stereo.plans[0].dimensions);
    try std.testing.expectEqual(@as(bool, true), stereo.plans[0].unsigned_values);
    try std.testing.expectEqual(@as(bool, true), stereo.plans[0].uses_escape);
    try std.testing.expectEqual(SpectralCodebookClass.escape, stereo.plans[19].class);
    try std.testing.expectEqual(SpectralCodebookClass.pair, stereo.plans[20].class);
    try std.testing.expectEqual(@as(u8, 2), stereo.plans[20].dimensions);
    try std.testing.expectEqual(@as(bool, true), stereo.plans[20].unsigned_values);
    try std.testing.expectEqual(@as(bool, false), stereo.plans[20].uses_escape);
    try std.testing.expectEqual(SpectralCodebookClass.pair, stereo.plans[32].class);
    try std.testing.expectEqual(SpectralCodebookClass.quad, stereo.plans[33].class);
    try std.testing.expectEqual(@as(u8, 4), stereo.plans[33].dimensions);
    try std.testing.expectEqual(SpectralCodebookClass.pair, stereo.plans[34].class);
    try std.testing.expectEqual(SpectralCodebookClass.quad, stereo.plans[41].class);
    try std.testing.expectEqual(SpectralCodebookClass.escape, stereo.plans[42].class);
    try std.testing.expectEqual(@as(i16, 140), stereo.plans[0].scalefactor_value);
    try std.testing.expectEqual(@as(i16, 101), stereo.plans[42].scalefactor_value);
    try std.testing.expectEqual(@as(bool, false), stereo.pulse_present);
    try std.testing.expectEqual(@as(bool, false), stereo.tns_present);
    try std.testing.expectEqual(@as(bool, false), stereo.gain_control_present);

    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try parseFirstChannelSpectralPlanAlloc(std.testing.allocator, mono_frame.payload);
    defer mono.deinit();
    try std.testing.expectEqual(@as(usize, 47), mono.plans.len);
    for (mono.plans[0..14]) |plan| try std.testing.expectEqual(SpectralCodebookClass.escape, plan.class);
    for (mono.plans[14..32]) |plan| try std.testing.expectEqual(SpectralCodebookClass.pair, plan.class);
    for (mono.plans[32..]) |plan| try std.testing.expectEqual(SpectralCodebookClass.noise, plan.class);
}

test "parse first-channel spectral layout plan for checked-in aac fixtures" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var stereo = try parseFirstChannelSpectralLayoutPlanAlloc(
        std.testing.allocator,
        16000,
        stereo_frame.payload,
    );
    defer stereo.deinit();
    try std.testing.expectEqual(@as(usize, 43), stereo.plans.len);
    try std.testing.expectEqual(@as(u16, 0), stereo.plans[0].coeff_start);
    try std.testing.expectEqual(@as(u16, 8), stereo.plans[0].coeff_end);
    try std.testing.expectEqual(@as(u16, 4), stereo.plans[0].symbol_count);
    try std.testing.expectEqual(@as(u16, 88), stereo.plans[11].coeff_start);
    try std.testing.expectEqual(@as(u16, 100), stereo.plans[11].coeff_end);
    try std.testing.expectEqual(@as(u16, 6), stereo.plans[11].symbol_count);
    try std.testing.expectEqual(@as(u16, 196), stereo.plans[20].coeff_start);
    try std.testing.expectEqual(@as(u16, 212), stereo.plans[20].coeff_end);
    try std.testing.expectEqual(@as(u16, 8), stereo.plans[20].symbol_count);
    try std.testing.expectEqual(@as(u16, 616), stereo.plans[36].coeff_start);
    try std.testing.expectEqual(@as(u16, 664), stereo.plans[36].coeff_end);
    try std.testing.expectEqual(@as(u16, 24), stereo.plans[36].symbol_count);

    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try parseFirstChannelSpectralLayoutPlanAlloc(
        std.testing.allocator,
        44100,
        mono_frame.payload,
    );
    defer mono.deinit();
    try std.testing.expectEqual(@as(usize, 47), mono.plans.len);
    try std.testing.expectEqual(@as(u16, 0), mono.plans[0].coeff_start);
    try std.testing.expectEqual(@as(u16, 4), mono.plans[0].coeff_end);
    try std.testing.expectEqual(@as(u16, 2), mono.plans[0].symbol_count);
    try std.testing.expectEqual(@as(u16, 196), mono.plans[24].coeff_start);
    try std.testing.expectEqual(@as(u16, 216), mono.plans[24].coeff_end);
    try std.testing.expectEqual(@as(u16, 10), mono.plans[24].symbol_count);
    try std.testing.expectEqual(@as(u16, 832), mono.plans[45].coeff_start);
    try std.testing.expectEqual(@as(u16, 864), mono.plans[45].coeff_end);
    try std.testing.expectEqual(@as(u16, 0), mono.plans[45].symbol_count);
}

test "decode first-channel spectral coefficients for checked-in stereo fixtures stays aligned between adts and m4a" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var adts = try decodeFirstChannelSpectralCoefficientsAlloc(
        std.testing.allocator,
        16000,
        stereo_frame.payload,
    );
    defer adts.deinit();
    try std.testing.expectEqual(@as(usize, 1024), adts.coefficients.len);
    try std.testing.expectEqual(@as(bool, false), adts.contains_noise);
    try std.testing.expectEqual(@as(bool, false), adts.contains_intensity);

    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    var m4a = try decodeFirstChannelSpectralCoefficientsAlloc(
        std.testing.allocator,
        config.sample_rate,
        demuxed.access_units[0],
    );
    defer m4a.deinit();
    try std.testing.expectEqualSlices(i16, adts.coefficients, m4a.coefficients);

    var non_zero: usize = 0;
    for (adts.coefficients[0..212]) |coeff| {
        if (coeff != 0) non_zero += 1;
    }
    try std.testing.expect(non_zero > 0);
}

test "decode first-channel spectral coefficients for checked-in mono fixture marks pns tail" {
    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try decodeFirstChannelSpectralCoefficientsAlloc(
        std.testing.allocator,
        44100,
        mono_frame.payload,
    );
    defer mono.deinit();
    try std.testing.expectEqual(@as(usize, 1024), mono.coefficients.len);
    try std.testing.expectEqual(@as(bool, true), mono.contains_noise);
    try std.testing.expectEqual(@as(bool, false), mono.contains_intensity);

    var non_zero: usize = 0;
    for (mono.coefficients[0..240]) |coeff| {
        if (coeff != 0) non_zero += 1;
    }
    try std.testing.expect(non_zero > 0);
    for (mono.coefficients[832..864]) |coeff| {
        try std.testing.expectEqual(@as(i16, 0), coeff);
    }
}

test "dequantize first-channel spectral coefficients for checked-in stereo fixtures stays aligned between adts and m4a" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var adts = try dequantizeFirstChannelSpectralCoefficientsAlloc(
        std.testing.allocator,
        16000,
        stereo_frame.payload,
    );
    defer adts.deinit();
    try std.testing.expectEqual(@as(usize, 1024), adts.coefficients.len);
    try std.testing.expectEqual(@as(bool, false), adts.contains_noise);
    try std.testing.expectEqual(@as(bool, false), adts.contains_intensity);

    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    var m4a = try dequantizeFirstChannelSpectralCoefficientsAlloc(
        std.testing.allocator,
        config.sample_rate,
        demuxed.access_units[0],
    );
    defer m4a.deinit();

    try std.testing.expectEqual(adts.coefficients.len, m4a.coefficients.len);
    for (adts.coefficients, m4a.coefficients) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }

    var non_zero: usize = 0;
    for (adts.coefficients[0..212]) |coeff| {
        if (coeff != 0) non_zero += 1;
    }
    try std.testing.expect(non_zero > 0);
}

test "decode channel-pair dequantized coefficients for checked-in stereo fixtures stays aligned between adts and m4a" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, tone_aac_bytes);
    defer std.testing.allocator.free(adts_frames);

    var adts = try decodeChannelPairDequantizedCoefficientsAlloc(
        std.testing.allocator,
        16000,
        adts_frames[0].payload,
    );
    defer adts.deinit();
    try std.testing.expectEqual(@as(bool, true), adts.common_window);
    try std.testing.expectEqual(@as(usize, 43), adts.ms_mask.len);
    try std.testing.expectEqual(WindowSequence.long_start, adts.left_ics_info.window_sequence);
    try std.testing.expectEqual(adts.left_ics_info.window_sequence, adts.right_ics_info.window_sequence);

    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    var m4a = try decodeChannelPairDequantizedCoefficientsAlloc(
        std.testing.allocator,
        config.sample_rate,
        demuxed.access_units[0],
    );
    defer m4a.deinit();

    try std.testing.expectEqualSlices(bool, adts.ms_mask, m4a.ms_mask);
    for (adts.left, m4a.left) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    for (adts.right, m4a.right) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);

    try std.testing.expect(adts.left.len > 0);
    try std.testing.expect(adts.right.len > 0);
}

test "decode channel-pair dequantized coefficients for checked-in short-window stereo fixtures stays aligned between adts and m4a" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_stereo_aac_44k_short_bytes);
    defer std.testing.allocator.free(adts_frames);

    var short_index: ?usize = null;
    for (adts_frames, 0..) |frame, i| {
        const prefix = try parseFirstElementPrefix(frame.payload);
        switch (prefix) {
            .cpe => |cpe| {
                const info = cpe.shared_ics_info orelse cpe.left_ics_info orelse return error.TestUnexpectedResult;
                if (info.window_sequence == .eight_short) {
                    short_index = i;
                    break;
                }
            },
            else => return error.TestUnexpectedResult,
        }
    }
    const idx = short_index orelse return error.TestExpectedEqual;

    var adts = try decodeChannelPairDequantizedCoefficientsAlloc(
        std.testing.allocator,
        44100,
        adts_frames[idx].payload,
    );
    defer adts.deinit();
    try std.testing.expectEqual(WindowSequence.eight_short, adts.left_ics_info.window_sequence);
    try std.testing.expectEqual(adts.left_ics_info.window_sequence, adts.right_ics_info.window_sequence);

    var demuxed = try mp4.demux(std.testing.allocator, transient_stereo_m4a_44k_short_bytes);
    defer demuxed.deinit();
    var m4a = try decodeChannelPairDequantizedCoefficientsAlloc(
        std.testing.allocator,
        demuxed.sample_rate,
        demuxed.access_units[idx],
    );
    defer m4a.deinit();

    try std.testing.expectEqualSlices(bool, adts.ms_mask, m4a.ms_mask);
    for (adts.left, m4a.left) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    for (adts.right, m4a.right) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
}

test "dequantize first-channel spectral coefficients for checked-in mono fixture applies pns tail" {
    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try dequantizeFirstChannelSpectralCoefficientsAlloc(
        std.testing.allocator,
        44100,
        mono_frame.payload,
    );
    defer mono.deinit();
    try std.testing.expectEqual(@as(usize, 1024), mono.coefficients.len);
    try std.testing.expectEqual(@as(bool, true), mono.contains_noise);
    try std.testing.expectEqual(@as(bool, false), mono.contains_intensity);

    var max_abs: f32 = 0;
    for (mono.coefficients[0..240]) |coeff| {
        max_abs = @max(max_abs, @abs(coeff));
    }
    try std.testing.expect(max_abs > 0);
    var noise_max_abs: f32 = 0;
    for (mono.coefficients[832..864]) |coeff| noise_max_abs = @max(noise_max_abs, @abs(coeff));
    try std.testing.expect(noise_max_abs > 0);
}

test "window first-channel long block for checked-in stereo fixtures stays aligned between adts and m4a" {
    const stereo_frame = try parseAdtsFrame(tone_aac_bytes);
    var adts = try windowFirstChannelLongBlockAlloc(
        std.testing.allocator,
        16000,
        stereo_frame.payload,
    );
    defer adts.deinit();
    try std.testing.expectEqual(@as(usize, 2048), adts.samples.len);
    try std.testing.expectEqual(WindowSequence.long_start, adts.ics_info.window_sequence);
    try std.testing.expectEqual(@as(bool, false), adts.contains_noise);
    try std.testing.expectEqual(@as(bool, false), adts.contains_intensity);

    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    var m4a = try windowFirstChannelLongBlockAlloc(
        std.testing.allocator,
        config.sample_rate,
        demuxed.access_units[0],
    );
    defer m4a.deinit();
    for (adts.samples, m4a.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
    for (adts.samples[1600..]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "window first-channel long block for checked-in mono fixture keeps pns tail explicit" {
    const mono_frame = try parseAdtsFrame(tone_aac_44k_mono_bytes);
    var mono = try windowFirstChannelLongBlockAlloc(
        std.testing.allocator,
        44100,
        mono_frame.payload,
    );
    defer mono.deinit();
    try std.testing.expectEqual(@as(usize, 2048), mono.samples.len);
    try std.testing.expectEqual(WindowSequence.long_start, mono.ics_info.window_sequence);
    try std.testing.expectEqual(@as(bool, true), mono.contains_noise);
    try std.testing.expectEqual(@as(bool, false), mono.contains_intensity);

    var max_abs: f32 = 0;
    for (mono.samples[0..1600]) |sample| {
        max_abs = @max(max_abs, @abs(sample));
    }
    try std.testing.expect(max_abs > 0);
    for (mono.samples[1600..]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "overlap add long block handles null previous tail" {
    const windowed = [_]f32{1} ** 2048;
    var overlapped = try overlapAddLongBlockAlloc(std.testing.allocator, null, &windowed);
    defer overlapped.deinit();
    try std.testing.expectEqual(@as(usize, 1024), overlapped.pcm.len);
    try std.testing.expectEqual(@as(usize, 1024), overlapped.tail.len);
    for (overlapped.pcm) |sample| try std.testing.expectEqual(@as(f32, 1), sample);
    for (overlapped.tail) |sample| try std.testing.expectEqual(@as(f32, 1), sample);
}

test "overlap add long block sums previous tail" {
    const prev = [_]f32{0.25} ** 1024;
    const curr = [_]f32{0.75} ** 2048;
    var overlapped = try overlapAddLongBlockAlloc(std.testing.allocator, &prev, &curr);
    defer overlapped.deinit();
    for (overlapped.pcm) |sample| try std.testing.expectEqual(@as(f32, 1.0), sample);
    for (overlapped.tail) |sample| try std.testing.expectEqual(@as(f32, 0.75), sample);
}

test "fused long IMDCT window overlap matches separate passes" {
    var imdct_samples: [2048]f32 = undefined;
    for (&imdct_samples, 0..) |*sample, index| {
        const centered = @as(f32, @floatFromInt(@as(i32, @intCast(index % 73)) - 36));
        sample.* = centered / 36.0;
    }
    var prev: [1024]f32 = undefined;
    for (&prev, 0..) |*sample, index| {
        sample.* = @as(f32, @floatFromInt(@as(i32, @intCast(index % 41)) - 20)) / 100.0;
    }

    var tables = try AacWindowTables.buildAlloc(std.testing.allocator, FrameShape.default());
    defer tables.deinit();

    const sequences = [_]WindowSequence{ .only_long, .long_start, .long_stop };
    for (sequences) |sequence| {
        for (0..2) |shape_index| {
            const info = IcsInfo{
                .window_sequence = sequence,
                .window_shape = @intCast(shape_index),
                .max_sfb = 0,
                .num_window_groups = 1,
            };
            var separate_samples = imdct_samples;
            try applyAacLongWindow(info, &separate_samples);
            var separate = try overlapAddLongBlockAlloc(std.testing.allocator, &prev, &separate_samples);
            defer separate.deinit();

            var fused = try overlapAddLongBlockFromImdctAllocWithShape(
                std.testing.allocator,
                &prev,
                &imdct_samples,
                info,
                &tables,
                FrameShape.default(),
            );
            defer fused.deinit();

            for (separate.pcm, fused.pcm) |expected, actual| {
                try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
            }
            for (separate.tail, fused.tail) |expected, actual| {
                try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
            }
        }
    }
}

test "decode first-channel pcm block for checked-in stereo fixtures stays aligned between adts and m4a" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, tone_aac_bytes);
    defer std.testing.allocator.free(adts_frames);

    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);

    var adts0 = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        16000,
        null,
        adts_frames[0].payload,
    );
    defer adts0.deinit();
    var m4a0 = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        config.sample_rate,
        null,
        demuxed.access_units[0],
    );
    defer m4a0.deinit();
    try std.testing.expectEqual(WindowSequence.long_start, adts0.ics_info.window_sequence);
    for (adts0.pcm, m4a0.pcm) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
    for (adts0.tail, m4a0.tail) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }

    var adts1 = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        16000,
        adts0.tail,
        adts_frames[1].payload,
    );
    defer adts1.deinit();
    var m4a1 = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        config.sample_rate,
        m4a0.tail,
        demuxed.access_units[1],
    );
    defer m4a1.deinit();
    for (adts1.pcm, m4a1.pcm) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
    for (adts1.tail, m4a1.tail) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "decode channel-pair pcm block for checked-in stereo fixtures stays aligned between adts and m4a" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, tone_aac_bytes);
    defer std.testing.allocator.free(adts_frames);

    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);

    var adts0 = try decodeChannelPairPcmBlockAlloc(
        std.testing.allocator,
        16000,
        null,
        null,
        adts_frames[0].payload,
    );
    defer adts0.deinit();
    var m4a0 = try decodeChannelPairPcmBlockAlloc(
        std.testing.allocator,
        config.sample_rate,
        null,
        null,
        demuxed.access_units[0],
    );
    defer m4a0.deinit();
    try std.testing.expectEqual(WindowSequence.long_start, adts0.left_ics_info.window_sequence);
    try std.testing.expectEqual(adts0.left_ics_info.window_sequence, adts0.right_ics_info.window_sequence);
    try std.testing.expectEqualSlices(bool, adts0.ms_mask, m4a0.ms_mask);
    for (adts0.left_pcm, m4a0.left_pcm) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    for (adts0.right_pcm, m4a0.right_pcm) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    for (adts0.left_tail, m4a0.left_tail) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    for (adts0.right_tail, m4a0.right_tail) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);

    var adts1 = try decodeChannelPairPcmBlockAlloc(
        std.testing.allocator,
        16000,
        adts0.left_tail,
        adts0.right_tail,
        adts_frames[1].payload,
    );
    defer adts1.deinit();
    var m4a1 = try decodeChannelPairPcmBlockAlloc(
        std.testing.allocator,
        config.sample_rate,
        m4a0.left_tail,
        m4a0.right_tail,
        demuxed.access_units[1],
    );
    defer m4a1.deinit();
    for (adts1.left_pcm, m4a1.left_pcm) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    for (adts1.right_pcm, m4a1.right_pcm) |lhs, rhs| try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
}

test "decode channel-pair pcm sequence for checked-in short-window stereo fixtures stays aligned between adts and m4a" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_stereo_aac_44k_short_bytes);
    defer std.testing.allocator.free(adts_frames);

    const adts_units = try std.testing.allocator.alloc([]const u8, adts_frames.len);
    defer std.testing.allocator.free(adts_units);
    for (adts_frames, 0..) |frame, i| adts_units[i] = frame.payload;

    var adts = try decodeChannelPairPcmSequenceAlloc(
        std.testing.allocator,
        44100,
        adts_units,
    );
    defer adts.deinit();

    var demuxed = try mp4.demux(std.testing.allocator, transient_stereo_m4a_44k_short_bytes);
    defer demuxed.deinit();
    var m4a = try decodeChannelPairPcmSequenceAlloc(
        std.testing.allocator,
        demuxed.sample_rate,
        demuxed.access_units,
    );
    defer m4a.deinit();

    try std.testing.expectEqual(adts.sample_rate, m4a.sample_rate);
    try std.testing.expectEqual(adts.frame_count, m4a.frame_count);
    try std.testing.expectEqual(adts.samples.len, m4a.samples.len);
    for (adts.samples, m4a.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "decode channel-pair pcm sequence for checked-in short-window stereo fixtures stays aligned between adts and mp4" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_stereo_aac_44k_short_bytes);
    defer std.testing.allocator.free(adts_frames);

    const adts_units = try std.testing.allocator.alloc([]const u8, adts_frames.len);
    defer std.testing.allocator.free(adts_units);
    for (adts_frames, 0..) |frame, i| adts_units[i] = frame.payload;

    var adts = try decodeChannelPairPcmSequenceAlloc(
        std.testing.allocator,
        44100,
        adts_units,
    );
    defer adts.deinit();

    var demuxed = try mp4.demux(std.testing.allocator, transient_stereo_mp4_44k_short_bytes);
    defer demuxed.deinit();
    var mp4_seq = try decodeChannelPairPcmSequenceAlloc(
        std.testing.allocator,
        demuxed.sample_rate,
        demuxed.access_units,
    );
    defer mp4_seq.deinit();

    try std.testing.expectEqual(adts.sample_rate, mp4_seq.sample_rate);
    try std.testing.expectEqual(adts.frame_count, mp4_seq.frame_count);
    try std.testing.expectEqual(adts.samples.len, mp4_seq.samples.len);
    for (adts.samples, mp4_seq.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "decode synthetic channel-pair dequantized coefficients supports common_window false long windows" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.cpe), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 0, 1); // common_window = false

    try builder.appendBits(std.testing.allocator, 100, 8); // left global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 1, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 1, 5); // section length
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    try builder.appendBits(std.testing.allocator, 100, 8); // right global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 1, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 1, 5); // section length
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var pair = try decodeChannelPairDequantizedCoefficientsAlloc(
        std.testing.allocator,
        16000,
        builder.bytes.items,
    );
    defer pair.deinit();

    try std.testing.expectEqual(@as(bool, false), pair.common_window);
    try std.testing.expectEqual(@as(usize, 0), pair.ms_mask.len);
    try std.testing.expectEqual(WindowSequence.only_long, pair.left_ics_info.window_sequence);
    try std.testing.expectEqual(WindowSequence.only_long, pair.right_ics_info.window_sequence);
    for (pair.left) |coeff| try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
    for (pair.right) |coeff| try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
}

test "decode synthetic mono block skips leading pce element" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // element_instance_tag
    try builder.appendBits(std.testing.allocator, 1, 2); // profile
    try builder.appendBits(std.testing.allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
    try builder.appendBits(std.testing.allocator, 1, 4); // num_front_channel_elements
    try builder.appendBits(std.testing.allocator, 0, 4); // num_side_channel_elements
    try builder.appendBits(std.testing.allocator, 0, 4); // num_back_channel_elements
    try builder.appendBits(std.testing.allocator, 0, 2); // num_lfe_channel_elements
    try builder.appendBits(std.testing.allocator, 0, 3); // num_assoc_data_elements
    try builder.appendBits(std.testing.allocator, 0, 4); // num_valid_cc_elements
    try builder.appendBits(std.testing.allocator, 0, 1); // mono_mixdown_present
    try builder.appendBits(std.testing.allocator, 0, 1); // stereo_mixdown_present
    try builder.appendBits(std.testing.allocator, 0, 1); // matrix_mixdown_idx_present
    try builder.appendBits(std.testing.allocator, 0, 1); // front element is SCE
    try builder.appendBits(std.testing.allocator, 0, 4); // front element tag
    try builder.alignToByte(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, 0, 8); // comment_field_bytes

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 0, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var block = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        44100,
        null,
        builder.bytes.items,
    );
    defer block.deinit();

    try std.testing.expectEqual(WindowSequence.only_long, block.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 1024), block.pcm.len);
    for (block.pcm) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "decode synthetic mono block skips leading dse element" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.dse), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // element_instance_tag
    try builder.appendBits(std.testing.allocator, 1, 1); // byte_align
    try builder.appendBits(std.testing.allocator, 2, 8); // count
    try builder.alignToByte(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, 0xaa, 8);
    try builder.appendBits(std.testing.allocator, 0xbb, 8);
    try builder.appendSilentMonoSce(std.testing.allocator);

    var block = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        44100,
        null,
        builder.bytes.items,
    );
    defer block.deinit();

    try std.testing.expectEqual(WindowSequence.only_long, block.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 1024), block.pcm.len);
    for (block.pcm) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "decode synthetic mono block skips leading fil element" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.fil), 3);
    try builder.appendBits(std.testing.allocator, 2, 4); // count
    try builder.appendBits(std.testing.allocator, 0xaa, 8);
    try builder.appendBits(std.testing.allocator, 0xbb, 8);
    try builder.appendSilentMonoSce(std.testing.allocator);

    var block = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        44100,
        null,
        builder.bytes.items,
    );
    defer block.deinit();

    try std.testing.expectEqual(WindowSequence.only_long, block.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 1024), block.pcm.len);
    for (block.pcm) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "decode synthetic mono block skips leading fil element with zero escape count" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.fil), 3);
    try builder.appendBits(std.testing.allocator, 15, 4); // escaped count
    try builder.appendBits(std.testing.allocator, 0, 8); // count becomes 14
    for (0..14) |_| try builder.appendBits(std.testing.allocator, 0xaa, 8);
    try builder.appendSilentMonoSce(std.testing.allocator);

    var block = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        44100,
        null,
        builder.bytes.items,
    );
    defer block.deinit();

    try std.testing.expectEqual(WindowSequence.only_long, block.ics_info.window_sequence);
    try std.testing.expectEqual(@as(usize, 1024), block.pcm.len);
    for (block.pcm) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "decode mono pcm sequence skips supported trailing metadata elements" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendSilentMonoSce(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.dse), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // element_instance_tag
    try builder.appendBits(std.testing.allocator, 1, 1); // byte_align
    try builder.appendBits(std.testing.allocator, 1, 8); // count
    try builder.alignToByte(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, 0xaa, 8);
    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.fil), 3);
    try builder.appendBits(std.testing.allocator, 1, 4); // count
    try builder.appendBits(std.testing.allocator, 0xbb, 8);
    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var decoded = try decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, 44100, &.{builder.bytes.items});
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "decode mono pcm sequence rejects trailing unexpected channel element" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendSilentMonoSceWithTag(std.testing.allocator, 0);
    try builder.appendSilentMonoSceWithTag(std.testing.allocator, 1);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, 44100, &.{builder.bytes.items}),
    );
}

test "decode mono pcm sequence rejects leading channel-pair element" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendSilentStereoCpe(std.testing.allocator);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, 44100, &.{builder.bytes.items}),
    );
}

test "decode synthetic channel-pair rejects truncated gain-control payload" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.cpe), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 1, 1); // common_window
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 21, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present
    try builder.appendBits(std.testing.allocator, 0, 2); // ms_present
    try builder.appendBits(std.testing.allocator, 100, 8); // left global_gain
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 21, 5); // section length
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 1, 1); // gain_control_present

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeChannelPairDequantizedCoefficientsAlloc(
            std.testing.allocator,
            44100,
            builder.bytes.items,
        ),
    );
}

test "decode synthetic channel-pair parses tns data before gain-control flag" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.cpe), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 1, 1); // common_window
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 0, 6); // max_sfb
    try builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present
    try builder.appendBits(std.testing.allocator, 0, 2); // ms_present

    try builder.appendBits(std.testing.allocator, 100, 8); // left global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 1, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 2); // TNS n_filt = 0
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    try builder.appendBits(std.testing.allocator, 100, 8); // right global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var pair = try decodeChannelPairDequantizedCoefficientsAlloc(
        std.testing.allocator,
        44100,
        builder.bytes.items,
    );
    defer pair.deinit();

    try std.testing.expectEqual(@as(bool, true), pair.common_window);
    for (pair.left) |coeff| try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
    for (pair.right) |coeff| try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
}

test "decode synthetic channel-pair skips supported trailing metadata elements" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendSilentStereoCpe(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.dse), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // element_instance_tag
    try builder.appendBits(std.testing.allocator, 1, 1); // byte_align
    try builder.appendBits(std.testing.allocator, 1, 8); // count
    try builder.alignToByte(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, 0xaa, 8);
    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.fil), 3);
    try builder.appendBits(std.testing.allocator, 1, 4); // count
    try builder.appendBits(std.testing.allocator, 0xbb, 8);
    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var pair = try decodeChannelPairDequantizedCoefficientsAlloc(
        std.testing.allocator,
        44100,
        builder.bytes.items,
    );
    defer pair.deinit();

    try std.testing.expectEqual(@as(bool, true), pair.common_window);
    for (pair.left) |coeff| try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
    for (pair.right) |coeff| try std.testing.expectApproxEqAbs(@as(f32, 0), coeff, 1e-6);
}

test "decode synthetic channel-pair rejects trailing unexpected channel element" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendSilentStereoCpe(std.testing.allocator);
    try builder.appendSilentMonoSce(std.testing.allocator);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeChannelPairDequantizedCoefficientsAlloc(
            std.testing.allocator,
            44100,
            builder.bytes.items,
        ),
    );
}

test "decode synthetic channel-pair pcm block supports common_window false eight-short windows" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.cpe), 3);
    try builder.appendBits(std.testing.allocator, 0, 4); // tag
    try builder.appendBits(std.testing.allocator, 0, 1); // common_window = false

    try builder.appendBits(std.testing.allocator, 100, 8); // left global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3); // section length
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    try builder.appendBits(std.testing.allocator, 100, 8); // right global_gain
    try builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.eight_short), 2);
    try builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try builder.appendBits(std.testing.allocator, 2, 4); // max_sfb
    try builder.appendBits(std.testing.allocator, 0b1111111, 7); // one group of 8 windows
    try builder.appendBits(std.testing.allocator, ZERO_BT, 4);
    try builder.appendBits(std.testing.allocator, 2, 3); // section length
    try builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var pair = try decodeChannelPairPcmBlockAlloc(
        std.testing.allocator,
        44100,
        null,
        null,
        builder.bytes.items,
    );
    defer pair.deinit();

    try std.testing.expectEqual(@as(bool, false), pair.common_window);
    try std.testing.expectEqual(@as(usize, 0), pair.ms_mask.len);
    try std.testing.expectEqual(WindowSequence.eight_short, pair.left_ics_info.window_sequence);
    try std.testing.expectEqual(WindowSequence.eight_short, pair.right_ics_info.window_sequence);
    for (pair.left_pcm) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    for (pair.right_pcm) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    for (pair.left_tail) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    for (pair.right_tail) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac pcm sequence tail replacement handles allocation failure" {
    var mono_builder = TestBitBuilder.init();
    defer mono_builder.deinit(std.testing.allocator);

    try mono_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try mono_builder.appendBits(std.testing.allocator, 0, 4); // tag
    try mono_builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try mono_builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try mono_builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try mono_builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try mono_builder.appendBits(std.testing.allocator, 0, 6); // max_sfb
    try mono_builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present
    try mono_builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try mono_builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try mono_builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        struct {
            fn run(allocator: std.mem.Allocator, unit: []const u8) !void {
                var seq = try decodeFirstChannelPcmSequenceAlloc(allocator, 44100, &.{ unit, unit });
                defer seq.deinit();
                try std.testing.expectEqual(@as(usize, 2 * 1024), seq.samples.len);
            }
        }.run,
        .{mono_builder.bytes.items},
    );

    var stereo_builder = TestBitBuilder.init();
    defer stereo_builder.deinit(std.testing.allocator);

    try stereo_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.cpe), 3);
    try stereo_builder.appendBits(std.testing.allocator, 0, 4); // tag
    try stereo_builder.appendBits(std.testing.allocator, 1, 1); // common_window
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try stereo_builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try stereo_builder.appendBits(std.testing.allocator, 0, 6); // max_sfb
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // predictor_data_present
    try stereo_builder.appendBits(std.testing.allocator, 0, 2); // ms_present
    try stereo_builder.appendBits(std.testing.allocator, 100, 8); // left global_gain
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present
    try stereo_builder.appendBits(std.testing.allocator, 100, 8); // right global_gain
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try stereo_builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        struct {
            fn run(allocator: std.mem.Allocator, unit: []const u8) !void {
                var seq = try decodeChannelPairPcmSequenceAlloc(allocator, 44100, &.{ unit, unit });
                defer seq.deinit();
                try std.testing.expectEqual(@as(usize, 2 * 1024 * 2), seq.samples.len);
            }
        }.run,
        .{stereo_builder.bytes.items},
    );
}

test "decode channel-pair pcm sequence for checked-in stereo fixtures stays aligned between adts and m4a" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, tone_aac_bytes);
    defer std.testing.allocator.free(adts_frames);

    const adts_units = try std.testing.allocator.alloc([]const u8, adts_frames.len);
    defer std.testing.allocator.free(adts_units);
    for (adts_frames, 0..) |frame, i| adts_units[i] = frame.payload;

    var adts = try decodeChannelPairPcmSequenceAlloc(
        std.testing.allocator,
        16000,
        adts_units,
    );
    defer adts.deinit();
    try std.testing.expectEqual(@as(u32, 16000), adts.sample_rate);
    try std.testing.expectEqual(adts_frames.len, adts.frame_count);
    try std.testing.expectEqual(@as(usize, adts_frames.len * 1024 * 2), adts.samples.len);

    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    var m4a = try decodeChannelPairPcmSequenceAlloc(
        std.testing.allocator,
        config.sample_rate,
        demuxed.access_units,
    );
    defer m4a.deinit();

    try std.testing.expectEqual(adts.frame_count, m4a.frame_count);
    try std.testing.expectEqual(adts.samples.len, m4a.samples.len);
    for (adts.samples, m4a.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "decode first-channel pcm block for checked-in mono fixture keeps pns explicit" {
    const mono_frames = try scanAdtsFramesAlloc(std.testing.allocator, tone_aac_44k_mono_bytes);
    defer std.testing.allocator.free(mono_frames);

    var first = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        44100,
        null,
        mono_frames[0].payload,
    );
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 1024), first.pcm.len);
    try std.testing.expectEqual(@as(usize, 1024), first.tail.len);
    try std.testing.expectEqual(WindowSequence.long_start, first.ics_info.window_sequence);
    try std.testing.expectEqual(@as(bool, true), first.contains_noise);
    try std.testing.expectEqual(@as(bool, false), first.contains_intensity);

    var max_abs: f32 = 0;
    for (first.pcm) |sample| {
        max_abs = @max(max_abs, @abs(sample));
    }
    try std.testing.expect(max_abs > 0);

    var second = try decodeFirstChannelPcmBlockAlloc(
        std.testing.allocator,
        44100,
        first.tail,
        mono_frames[1].payload,
    );
    defer second.deinit();
    var diff_found = false;
    for (first.pcm, second.pcm) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            diff_found = true;
            break;
        }
    }
    try std.testing.expect(diff_found);
}

test "decode first-channel pcm sequence for checked-in short-window mono fixture reconstructs across short frames" {
    const frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_aac_44k_short_bytes);
    defer std.testing.allocator.free(frames);

    const access_units = try std.testing.allocator.alloc([]const u8, frames.len);
    defer std.testing.allocator.free(access_units);
    for (frames, 0..) |frame, i| access_units[i] = frame.payload;

    var seq = try decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, 44100, access_units);
    defer seq.deinit();

    try std.testing.expectEqual(@as(usize, frames.len), seq.frame_count);
    try std.testing.expectEqual(@as(usize, frames.len * 1024), seq.samples.len);

    var max_abs: f32 = 0;
    for (seq.samples) |sample| {
        max_abs = @max(max_abs, @abs(sample));
    }
    try std.testing.expect(max_abs > 0);
}

test "decode first-channel pcm sequence for checked-in short-window mono fixtures stays aligned between adts and m4a" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_aac_44k_short_bytes);
    defer std.testing.allocator.free(adts_frames);
    const adts_access_units = try std.testing.allocator.alloc([]const u8, adts_frames.len);
    defer std.testing.allocator.free(adts_access_units);
    for (adts_frames, 0..) |frame, i| adts_access_units[i] = frame.payload;

    var adts = try decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, 44100, adts_access_units);
    defer adts.deinit();

    var demuxed = try mp4.demux(std.testing.allocator, transient_m4a_44k_short_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    var m4a = try decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, config.sample_rate, demuxed.access_units);
    defer m4a.deinit();

    try std.testing.expectEqual(adts.sample_rate, m4a.sample_rate);
    try std.testing.expectEqual(adts.frame_count, m4a.frame_count);
    try std.testing.expectEqual(adts.samples.len, m4a.samples.len);
    for (adts.samples, m4a.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "decode first-channel pcm sequence for checked-in short-window mono fixtures stays aligned between adts and mp4" {
    const adts_frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_aac_44k_short_bytes);
    defer std.testing.allocator.free(adts_frames);
    const adts_access_units = try std.testing.allocator.alloc([]const u8, adts_frames.len);
    defer std.testing.allocator.free(adts_access_units);
    for (adts_frames, 0..) |frame, i| adts_access_units[i] = frame.payload;

    var adts = try decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, 44100, adts_access_units);
    defer adts.deinit();

    var demuxed = try mp4.demux(std.testing.allocator, transient_mp4_44k_short_bytes);
    defer demuxed.deinit();
    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    var mp4_seq = try decodeFirstChannelPcmSequenceAlloc(std.testing.allocator, config.sample_rate, demuxed.access_units);
    defer mp4_seq.deinit();

    try std.testing.expectEqual(adts.sample_rate, mp4_seq.sample_rate);
    try std.testing.expectEqual(adts.frame_count, mp4_seq.frame_count);
    try std.testing.expectEqual(adts.samples.len, mp4_seq.samples.len);
    for (adts.samples, mp4_seq.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "decode synthetic spectral quad symbol with sign bits" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0b100, 3);
    try builder.appendBits(std.testing.allocator, 1, 1);

    var reader = BitReader.init(builder.bytes.items);
    const symbol = try decodeSpectralSymbol(&reader, spectral_codebook1);
    try std.testing.expectEqual(@as(u8, 4), symbol.dimensions);
    try std.testing.expectEqual(@as(i16, -1), symbol.values[0]);
    try std.testing.expectEqual(@as(i16, 0), symbol.values[1]);
    try std.testing.expectEqual(@as(i16, 0), symbol.values[2]);
    try std.testing.expectEqual(@as(i16, 0), symbol.values[3]);
}

test "decode real aac signed pair codebook symbol" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0x008, 4);

    var reader = BitReader.init(builder.bytes.items);
    const symbol = try decodeAacSpectralSymbol(&reader, try aacSpectralCodebook(6));
    try std.testing.expectEqual(@as(u8, 2), symbol.dimensions);
    try std.testing.expectEqual(@as(i16, -1), symbol.values[0]);
    try std.testing.expectEqual(@as(i16, -1), symbol.values[1]);
}

test "decode real aac escape pair codebook symbol" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0x38e, 10);
    try builder.appendBits(std.testing.allocator, 0, 1);
    try builder.appendBits(std.testing.allocator, 0b10, 2);
    try builder.appendBits(std.testing.allocator, 0b00001, 5);

    var reader = BitReader.init(builder.bytes.items);
    const symbol = try decodeAacSpectralSymbol(&reader, try aacSpectralCodebook(11));
    try std.testing.expectEqual(@as(u8, 2), symbol.dimensions);
    try std.testing.expectEqual(@as(i16, 0), symbol.values[0]);
    try std.testing.expectEqual(@as(i16, 33), symbol.values[1]);
}

test "dequantize aac coefficient preserves sign and zero" {
    try std.testing.expectEqual(@as(f32, 0), dequantizeAacCoefficient(0, 120));

    const positive = dequantizeAacCoefficient(2, 120);
    const negative = dequantizeAacCoefficient(-2, 120);
    try std.testing.expect(positive > 0);
    try std.testing.expectApproxEqAbs(positive, -negative, 1e-6);

    const louder = dequantizeAacCoefficient(2, 124);
    try std.testing.expect(louder > positive);
}

test "imdct long of zero coefficients stays zero" {
    const coeffs = [_]f32{0} ** 1024;
    var out = [_]f32{1} ** 2048;
    try imdctLongInto(&out, &coeffs);
    for (out) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "imdct short of zero coefficients stays zero" {
    const coeffs = [_]f32{0} ** 128;
    var out = [_]f32{1} ** 256;
    try imdctShortInto(&out, &coeffs);
    for (out) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "optimized aac imdct stays close to naive transform" {
    const coefficients = [_]f32{ 0.25, -0.5, 0.75, -1.0 };
    var expected: [8]f32 = undefined;
    var actual: [8]f32 = undefined;

    var plan = try buildImdctPlanAlloc(std.testing.allocator, 8);
    defer plan.deinit();

    try imdctIntoNaive(&expected, &coefficients);
    try imdctIntoWithPlan(&actual, &coefficients, plan);

    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-4);
    }
}

test "compose eight short window sequence places first short block at aac offset" {
    var coeffs = [_]f32{0} ** 1024;
    coeffs[0] = 1.0;

    var seq = try composeEightShortWindowSequenceAlloc(std.testing.allocator, &coeffs);
    defer seq.deinit();
    try std.testing.expectEqual(@as(usize, 2048), seq.samples.len);

    for (seq.samples[0..448]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }

    var active_count: usize = 0;
    for (seq.samples[448..704]) |sample| {
        if (@abs(sample) > 1e-6) active_count += 1;
    }
    try std.testing.expect(active_count > 0);

    for (seq.samples[704..]) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
    }
}

test "compose eight short window sequence overlaps adjacent windows by 128 samples" {
    var coeffs = [_]f32{0} ** 1024;
    coeffs[0] = 1.0;
    coeffs[128] = 1.0;

    var seq = try composeEightShortWindowSequenceAlloc(std.testing.allocator, &coeffs);
    defer seq.deinit();

    var first_only_count: usize = 0;
    for (seq.samples[448..576]) |sample| {
        if (@abs(sample) > 1e-6) first_only_count += 1;
    }
    try std.testing.expect(first_only_count > 0);

    var overlap_count: usize = 0;
    for (seq.samples[576..704]) |sample| {
        if (@abs(sample) > 1e-6) overlap_count += 1;
    }
    try std.testing.expect(overlap_count > 0);

    var second_only_count: usize = 0;
    for (seq.samples[704..832]) |sample| {
        if (@abs(sample) > 1e-6) second_only_count += 1;
    }
    try std.testing.expect(second_only_count > 0);
}

test "overlap add short window sequence without previous tail copies first half" {
    var curr: [2048]f32 = undefined;
    for (&curr, 0..) |*sample, i| sample.* = @floatFromInt(i);

    var overlapped = try overlapAddShortWindowSequenceAlloc(std.testing.allocator, null, &curr);
    defer overlapped.deinit();

    try std.testing.expectEqual(@as(usize, 1024), overlapped.pcm.len);
    try std.testing.expectEqual(@as(usize, 1024), overlapped.tail.len);

    for (0..1024) |i| {
        try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(i)), overlapped.pcm[i], 1e-6);
        try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(i + 1024)), overlapped.tail[i], 1e-6);
    }
}

test "overlap add short window sequence adds previous tail into first half" {
    var prev: [1024]f32 = undefined;
    for (&prev, 0..) |*sample, i| sample.* = @as(f32, 0.5) * @as(f32, @floatFromInt(i));

    var curr: [2048]f32 = undefined;
    for (&curr, 0..) |*sample, i| sample.* = @floatFromInt(i);

    var overlapped = try overlapAddShortWindowSequenceAlloc(std.testing.allocator, &prev, &curr);
    defer overlapped.deinit();

    for (0..1024) |i| {
        const expected_pcm = @as(f32, @floatFromInt(i)) + prev[i];
        try std.testing.expectApproxEqAbs(expected_pcm, overlapped.pcm[i], 1e-6);
        try std.testing.expectApproxEqAbs(@as(f32, @floatFromInt(i + 1024)), overlapped.tail[i], 1e-6);
    }
}

test "short window overlap add splits composed sequence into pcm and tail" {
    var coeffs = [_]f32{0} ** 1024;
    coeffs[0] = 1.0;
    coeffs[7 * 128] = 1.0;

    var seq = try composeEightShortWindowSequenceAlloc(std.testing.allocator, &coeffs);
    defer seq.deinit();

    var block = try overlapAddShortWindowSequenceAlloc(std.testing.allocator, null, seq.samples);
    defer block.deinit();

    try std.testing.expectEqual(@as(usize, 1024), block.pcm.len);
    try std.testing.expectEqual(@as(usize, 1024), block.tail.len);

    var active_pcm: usize = 0;
    for (block.pcm) |sample| {
        if (@abs(sample) > 1e-6) active_pcm += 1;
    }
    try std.testing.expect(active_pcm > 0);

    var active_tail: usize = 0;
    for (block.tail) |sample| {
        if (@abs(sample) > 1e-6) active_tail += 1;
    }
    try std.testing.expect(active_tail > 0);
}

test "decode synthetic spectral pair symbol with sign bits" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0b111, 3);
    try builder.appendBits(std.testing.allocator, 0, 1);
    try builder.appendBits(std.testing.allocator, 1, 1);

    var reader = BitReader.init(builder.bytes.items);
    const symbol = try decodeSpectralSymbol(&reader, spectral_codebook5);
    try std.testing.expectEqual(@as(u8, 2), symbol.dimensions);
    try std.testing.expectEqual(@as(i16, 2), symbol.values[0]);
    try std.testing.expectEqual(@as(i16, -1), symbol.values[1]);
    try std.testing.expectEqual(@as(i16, 0), symbol.values[2]);
    try std.testing.expectEqual(@as(i16, 0), symbol.values[3]);
}

test "decode escaped spectral value" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0b10, 2); // unary 1,0 => bits = 5
    try builder.appendBits(std.testing.allocator, 0b00001, 5); // extra = 1 => 33

    var reader = BitReader.init(builder.bytes.items);
    const value = try decodeEscapedSpectralValue(&reader, 16);
    try std.testing.expectEqual(@as(i16, 33), value);
}

test "parse synthetic pulse data" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0b10, 2); // num_pulse = 3
    try builder.appendBits(std.testing.allocator, 0b000101, 6); // pulse_swb = 5
    try builder.appendBits(std.testing.allocator, 0b00111, 5);
    try builder.appendBits(std.testing.allocator, 0b1001, 4);
    try builder.appendBits(std.testing.allocator, 0b00011, 5);
    try builder.appendBits(std.testing.allocator, 0b0001, 4);
    try builder.appendBits(std.testing.allocator, 0b00100, 5);
    try builder.appendBits(std.testing.allocator, 0b0010, 4);

    var reader = BitReader.init(builder.bytes.items);
    const pulse = try parsePulseData(&reader, .{
        .window_sequence = .only_long,
        .window_shape = 0,
        .max_sfb = 8,
        .num_window_groups = 1,
        .window_group_length = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = false,
    });
    try std.testing.expectEqual(@as(u8, 3), pulse.num_pulse);
    try std.testing.expectEqual(@as(u8, 5), pulse.pulse_swb);
    try std.testing.expectEqual(@as(u8, 7), pulse.offsets[0]);
    try std.testing.expectEqual(@as(u8, 9), pulse.amplitudes[0]);
    try std.testing.expectEqual(@as(u8, 3), pulse.offsets[1]);
    try std.testing.expectEqual(@as(u8, 1), pulse.amplitudes[1]);
    try std.testing.expectEqual(@as(u8, 4), pulse.offsets[2]);
    try std.testing.expectEqual(@as(u8, 2), pulse.amplitudes[2]);
}

test "parse synthetic tns data" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0b01, 2); // n_filt = 1
    try builder.appendBits(std.testing.allocator, 0b1, 1); // coef_res = 1
    try builder.appendBits(std.testing.allocator, 17, 6); // length
    try builder.appendBits(std.testing.allocator, 3, 5); // order
    try builder.appendBits(std.testing.allocator, 1, 1); // direction
    try builder.appendBits(std.testing.allocator, 0, 1); // coef_compress
    try builder.appendBits(std.testing.allocator, 1, 4);
    try builder.appendBits(std.testing.allocator, 7, 4);
    try builder.appendBits(std.testing.allocator, 15, 4);

    var reader = BitReader.init(builder.bytes.items);
    const tns = try parseTnsData(&reader, .{
        .window_sequence = .only_long,
        .window_shape = 0,
        .max_sfb = 8,
        .num_window_groups = 1,
        .window_group_length = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = false,
    }, false);
    try std.testing.expectEqual(@as(u8, 1), tns.num_windows);
    try std.testing.expectEqual(@as(u8, 1), tns.windows[0].n_filt);
    try std.testing.expectEqual(@as(u1, 1), tns.windows[0].coef_res);
    try std.testing.expectEqual(@as(u8, 17), tns.windows[0].filters[0].length);
    try std.testing.expectEqual(@as(u8, 3), tns.windows[0].filters[0].order);
    try std.testing.expectEqual(@as(bool, true), tns.windows[0].filters[0].direction);
    try std.testing.expectEqual(@as(bool, false), tns.windows[0].filters[0].coef_compress);
    try std.testing.expectEqual(@as(u8, 4), tns.windows[0].filters[0].coef_len);
    try std.testing.expectEqual(@as(u8, 1), tns.windows[0].filters[0].coefficients[0]);
    try std.testing.expectEqual(@as(u8, 7), tns.windows[0].filters[0].coefficients[1]);
    try std.testing.expectEqual(@as(u8, 15), tns.windows[0].filters[0].coefficients[2]);
}

const TestBitBuilder = struct {
    bytes: std.ArrayList(u8),
    bit_len: usize,

    const PceElementPosition = enum {
        front,
        side,
        back,
    };

    fn init() TestBitBuilder {
        return .{
            .bytes = .empty,
            .bit_len = 0,
        };
    }

    fn deinit(self: *TestBitBuilder, allocator: std.mem.Allocator) void {
        self.bytes.deinit(allocator);
    }

    fn appendBits(self: *TestBitBuilder, allocator: std.mem.Allocator, value: u64, bit_count: usize) !void {
        for (0..bit_count) |i| {
            const bit_index = bit_count - 1 - i;
            const bit = (value >> @intCast(bit_index)) & 1;
            const byte_index = self.bit_len / 8;
            const shift: u3 = @intCast(7 - (self.bit_len % 8));
            if (byte_index == self.bytes.items.len) {
                try self.bytes.append(allocator, 0);
            }
            self.bytes.items[byte_index] |= @as(u8, @intCast(bit)) << shift;
            self.bit_len += 1;
        }
    }

    fn appendScalefactorSymbol(self: *TestBitBuilder, allocator: std.mem.Allocator, symbol: usize) !void {
        if (symbol >= scalefactor_codes.len or symbol >= scalefactor_bits.len) return error.UnsupportedAudioFormat;
        try self.appendBits(allocator, scalefactor_codes[symbol], scalefactor_bits[symbol]);
    }

    fn appendAacPairCodebook6NegOneNegOne(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendBits(allocator, 0x008, 4);
    }

    fn alignToByte(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        const remainder = self.bit_len % 8;
        if (remainder != 0) try self.appendBits(allocator, 0, 8 - remainder);
    }

    fn appendMetadataOnlyDseFilEnd(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendBits(allocator, @intFromEnum(ElementKind.dse), 3);
        try self.appendBits(allocator, 0, 4); // element_instance_tag
        try self.appendBits(allocator, 1, 1); // byte_align
        try self.appendBits(allocator, 1, 8); // count
        try self.alignToByte(allocator);
        try self.appendBits(allocator, 0xaa, 8);
        try self.appendBits(allocator, @intFromEnum(ElementKind.fil), 3);
        try self.appendBits(allocator, 1, 4); // count
        try self.appendBits(allocator, 0xbb, 8);
        try self.appendBits(allocator, @intFromEnum(ElementKind.end), 3);
    }

    fn appendByteAlignedMetadataOnlyFilEnd(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        for (0..3) |_| {
            try self.appendBits(allocator, @intFromEnum(ElementKind.fil), 3);
            try self.appendBits(allocator, 0, 4); // count
        }
        try self.appendBits(allocator, @intFromEnum(ElementKind.end), 3);
    }

    fn appendSilentMonoSce(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendSilentMonoSceWithTag(allocator, 0);
    }

    fn appendNonZeroMonoSce(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendBits(allocator, @intFromEnum(ElementKind.sce), 3);
        try self.appendBits(allocator, 0, 4); // tag
        try self.appendBits(allocator, 100, 8); // global_gain
        try self.appendBits(allocator, 0, 1); // reserved
        try self.appendBits(allocator, @intFromEnum(WindowSequence.only_long), 2);
        try self.appendBits(allocator, 0, 1); // window_shape
        try self.appendBits(allocator, 1, 6); // max_sfb
        try self.appendBits(allocator, 0, 1); // predictor_data_present
        try self.appendBits(allocator, 6, 4); // section band_type
        try self.appendBits(allocator, 1, 5); // section length
        try self.appendScalefactorSymbol(allocator, 60); // zero scalefactor delta
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 0, 1); // tns_present
        try self.appendBits(allocator, 0, 1); // gain_control_present
        try self.appendAacPairCodebook6NegOneNegOne(allocator);
        try self.appendAacPairCodebook6NegOneNegOne(allocator);
    }

    fn appendSilentMonoSceWithTag(self: *TestBitBuilder, allocator: std.mem.Allocator, tag: u4) !void {
        try self.appendSilentSingleChannelElementWithTag(allocator, .sce, tag);
    }

    fn appendSilentLfeWithTag(self: *TestBitBuilder, allocator: std.mem.Allocator, tag: u4) !void {
        try self.appendSilentSingleChannelElementWithTag(allocator, .lfe, tag);
    }

    fn appendSilentSingleChannelElementWithTag(
        self: *TestBitBuilder,
        allocator: std.mem.Allocator,
        kind: ElementKind,
        tag: u4,
    ) !void {
        try self.appendBits(allocator, @intFromEnum(kind), 3);
        try self.appendBits(allocator, tag, 4);
        try self.appendBits(allocator, 100, 8); // global_gain
        try self.appendBits(allocator, 0, 1); // reserved
        try self.appendBits(allocator, @intFromEnum(WindowSequence.only_long), 2);
        try self.appendBits(allocator, 0, 1); // window_shape
        try self.appendBits(allocator, 0, 6); // max_sfb
        try self.appendBits(allocator, 0, 1); // predictor_data_present
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 0, 1); // tns_present
        try self.appendBits(allocator, 0, 1); // gain_control_present
    }

    fn appendMonoPce(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendMonoRegularScePceWithTag(allocator, .front, 0);
    }

    fn appendMonoRegularScePceWithTag(
        self: *TestBitBuilder,
        allocator: std.mem.Allocator,
        position: PceElementPosition,
        tag: u4,
    ) !void {
        try self.appendBits(allocator, 0, 4); // element_instance_tag
        try self.appendBits(allocator, 1, 2); // profile
        try self.appendBits(allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
        try self.appendBits(allocator, if (position == .front) 1 else 0, 4); // num_front_channel_elements
        try self.appendBits(allocator, if (position == .side) 1 else 0, 4); // num_side_channel_elements
        try self.appendBits(allocator, if (position == .back) 1 else 0, 4); // num_back_channel_elements
        try self.appendBits(allocator, 0, 2); // num_lfe_channel_elements
        try self.appendBits(allocator, 0, 3); // num_assoc_data_elements
        try self.appendBits(allocator, 0, 4); // num_valid_cc_elements
        try self.appendBits(allocator, 0, 1); // mono_mixdown_present
        try self.appendBits(allocator, 0, 1); // stereo_mixdown_present
        try self.appendBits(allocator, 0, 1); // matrix_mixdown_idx_present
        try self.appendBits(allocator, 0, 1); // selected element is SCE
        try self.appendBits(allocator, tag, 4); // selected element tag
        try self.alignToByte(allocator);
        try self.appendBits(allocator, 0, 8); // comment_field_bytes
    }

    fn appendMonoLfePce(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendBits(allocator, 0, 4); // element_instance_tag
        try self.appendBits(allocator, 1, 2); // profile
        try self.appendBits(allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
        try self.appendBits(allocator, 0, 4); // num_front_channel_elements
        try self.appendBits(allocator, 0, 4); // num_side_channel_elements
        try self.appendBits(allocator, 0, 4); // num_back_channel_elements
        try self.appendBits(allocator, 1, 2); // num_lfe_channel_elements
        try self.appendBits(allocator, 0, 3); // num_assoc_data_elements
        try self.appendBits(allocator, 0, 4); // num_valid_cc_elements
        try self.appendBits(allocator, 0, 1); // mono_mixdown_present
        try self.appendBits(allocator, 0, 1); // stereo_mixdown_present
        try self.appendBits(allocator, 0, 1); // matrix_mixdown_idx_present
        try self.appendBits(allocator, 0, 4); // lfe element tag
        try self.alignToByte(allocator);
        try self.appendBits(allocator, 0, 8); // comment_field_bytes
    }

    fn appendStereoScePairPce(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendStereoScePairPceWithTags(allocator, 0, 1);
    }

    fn appendStereoScePairPceWithTags(
        self: *TestBitBuilder,
        allocator: std.mem.Allocator,
        left_tag: u4,
        right_tag: u4,
    ) !void {
        try self.appendBits(allocator, 0, 4); // element_instance_tag
        try self.appendBits(allocator, 1, 2); // profile
        try self.appendBits(allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
        try self.appendBits(allocator, 2, 4); // two front SCE elements
        try self.appendBits(allocator, 0, 4); // num_side_channel_elements
        try self.appendBits(allocator, 0, 4); // num_back_channel_elements
        try self.appendBits(allocator, 0, 2); // num_lfe_channel_elements
        try self.appendBits(allocator, 0, 3); // num_assoc_data_elements
        try self.appendBits(allocator, 0, 4); // num_valid_cc_elements
        try self.appendBits(allocator, 0, 1); // mono_mixdown_present
        try self.appendBits(allocator, 0, 1); // stereo_mixdown_present
        try self.appendBits(allocator, 0, 1); // matrix_mixdown_idx_present
        try self.appendBits(allocator, 0, 1); // first front element is SCE
        try self.appendBits(allocator, left_tag, 4); // first front element tag
        try self.appendBits(allocator, 0, 1); // second front element is SCE
        try self.appendBits(allocator, right_tag, 4); // second front element tag
        try self.alignToByte(allocator);
        try self.appendBits(allocator, 0, 8); // comment_field_bytes
    }

    fn appendStereoScePairPceWithFrontBackTags(
        self: *TestBitBuilder,
        allocator: std.mem.Allocator,
        front_tag: u4,
        back_tag: u4,
    ) !void {
        try self.appendBits(allocator, 0, 4); // element_instance_tag
        try self.appendBits(allocator, 1, 2); // profile
        try self.appendBits(allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
        try self.appendBits(allocator, 1, 4); // one front SCE element
        try self.appendBits(allocator, 0, 4); // num_side_channel_elements
        try self.appendBits(allocator, 1, 4); // one back SCE element
        try self.appendBits(allocator, 0, 2); // num_lfe_channel_elements
        try self.appendBits(allocator, 0, 3); // num_assoc_data_elements
        try self.appendBits(allocator, 0, 4); // num_valid_cc_elements
        try self.appendBits(allocator, 0, 1); // mono_mixdown_present
        try self.appendBits(allocator, 0, 1); // stereo_mixdown_present
        try self.appendBits(allocator, 0, 1); // matrix_mixdown_idx_present
        try self.appendBits(allocator, 0, 1); // front element is SCE
        try self.appendBits(allocator, front_tag, 4); // front element tag
        try self.appendBits(allocator, 0, 1); // back element is SCE
        try self.appendBits(allocator, back_tag, 4); // back element tag
        try self.alignToByte(allocator);
        try self.appendBits(allocator, 0, 8); // comment_field_bytes
    }

    fn appendStereoSceLfePce(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendBits(allocator, 0, 4); // element_instance_tag
        try self.appendBits(allocator, 1, 2); // profile
        try self.appendBits(allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
        try self.appendBits(allocator, 1, 4); // one front SCE
        try self.appendBits(allocator, 0, 4); // num_side_channel_elements
        try self.appendBits(allocator, 0, 4); // num_back_channel_elements
        try self.appendBits(allocator, 1, 2); // one LFE channel
        try self.appendBits(allocator, 0, 3); // num_assoc_data_elements
        try self.appendBits(allocator, 0, 4); // num_valid_cc_elements
        try self.appendBits(allocator, 0, 1); // mono_mixdown_present
        try self.appendBits(allocator, 0, 1); // stereo_mixdown_present
        try self.appendBits(allocator, 0, 1); // matrix_mixdown_idx_present
        try self.appendBits(allocator, 0, 1); // front element is SCE
        try self.appendBits(allocator, 0, 4); // front element tag
        try self.appendBits(allocator, 1, 4); // lfe element tag
        try self.alignToByte(allocator);
        try self.appendBits(allocator, 0, 8); // comment_field_bytes
    }

    fn appendStereoPce(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendStereoPceWithTag(allocator, 0);
    }

    fn appendStereoPceWithTag(self: *TestBitBuilder, allocator: std.mem.Allocator, tag: u4) !void {
        try self.appendStereoPceWithPositionTag(allocator, .front, tag);
    }

    fn appendStereoPceWithPositionTag(
        self: *TestBitBuilder,
        allocator: std.mem.Allocator,
        position: PceElementPosition,
        tag: u4,
    ) !void {
        try self.appendBits(allocator, 0, 4); // element_instance_tag
        try self.appendBits(allocator, 1, 2); // profile
        try self.appendBits(allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
        try self.appendBits(allocator, if (position == .front) 1 else 0, 4); // num_front_channel_elements
        try self.appendBits(allocator, if (position == .side) 1 else 0, 4); // num_side_channel_elements
        try self.appendBits(allocator, if (position == .back) 1 else 0, 4); // num_back_channel_elements
        try self.appendBits(allocator, 0, 2); // num_lfe_channel_elements
        try self.appendBits(allocator, 0, 3); // num_assoc_data_elements
        try self.appendBits(allocator, 0, 4); // num_valid_cc_elements
        try self.appendBits(allocator, 0, 1); // mono_mixdown_present
        try self.appendBits(allocator, 0, 1); // stereo_mixdown_present
        try self.appendBits(allocator, 0, 1); // matrix_mixdown_idx_present
        try self.appendBits(allocator, 1, 1); // selected element is CPE
        try self.appendBits(allocator, tag, 4); // selected element tag
        try self.alignToByte(allocator);
        try self.appendBits(allocator, 0, 8); // comment_field_bytes
    }

    fn appendSilentStereoCpe(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendSilentStereoCpeWithTag(allocator, 0);
    }

    fn appendSilentStereoCpeWithTag(self: *TestBitBuilder, allocator: std.mem.Allocator, tag: u4) !void {
        try self.appendBits(allocator, @intFromEnum(ElementKind.cpe), 3);
        try self.appendBits(allocator, tag, 4);
        try self.appendBits(allocator, 1, 1); // common_window
        try self.appendBits(allocator, 0, 1); // reserved
        try self.appendBits(allocator, @intFromEnum(WindowSequence.only_long), 2);
        try self.appendBits(allocator, 0, 1); // window_shape
        try self.appendBits(allocator, 0, 6); // max_sfb
        try self.appendBits(allocator, 0, 1); // predictor_data_present
        try self.appendBits(allocator, 0, 2); // ms_present
        try self.appendBits(allocator, 100, 8); // left global_gain
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 0, 1); // tns_present
        try self.appendBits(allocator, 0, 1); // gain_control_present
        try self.appendBits(allocator, 100, 8); // right global_gain
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 0, 1); // tns_present
        try self.appendBits(allocator, 0, 1); // gain_control_present
    }

    fn appendSilentStereoCpeWithGainControlAndSbrFill(self: *TestBitBuilder, allocator: std.mem.Allocator, tag: u4) !void {
        try self.appendBits(allocator, @intFromEnum(ElementKind.cpe), 3);
        try self.appendBits(allocator, tag, 4);
        try self.appendBits(allocator, 1, 1); // common_window
        try self.appendBits(allocator, 0, 1); // reserved
        try self.appendBits(allocator, @intFromEnum(WindowSequence.only_long), 2);
        try self.appendBits(allocator, 0, 1); // window_shape
        try self.appendBits(allocator, 0, 6); // max_sfb
        try self.appendBits(allocator, 0, 1); // predictor_data_present
        try self.appendBits(allocator, 0, 2); // ms_present
        try self.appendBits(allocator, 100, 8); // left global_gain
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 0, 1); // tns_present
        try self.appendBits(allocator, 1, 1); // gain_control_present
        try self.appendBits(allocator, 0, 2); // max_band = 0
        try self.appendBits(allocator, 100, 8); // right global_gain
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 0, 1); // tns_present
        try self.appendBits(allocator, 1, 1); // gain_control_present
        try self.appendBits(allocator, 0, 2); // max_band = 0
        try self.appendSyntheticSbrFillElement(allocator);
    }

    fn appendStereoIntensityCpeWithTnsGainAndSbrFill(self: *TestBitBuilder, allocator: std.mem.Allocator, tag: u4) !void {
        try self.appendBits(allocator, @intFromEnum(ElementKind.cpe), 3);
        try self.appendBits(allocator, tag, 4);
        try self.appendBits(allocator, 1, 1); // common_window
        try self.appendBits(allocator, 0, 1); // reserved
        try self.appendBits(allocator, @intFromEnum(WindowSequence.only_long), 2);
        try self.appendBits(allocator, 0, 1); // window_shape
        try self.appendBits(allocator, 1, 6); // max_sfb
        try self.appendBits(allocator, 0, 1); // predictor_data_present
        try self.appendBits(allocator, 0, 2); // ms_present

        try self.appendBits(allocator, 100, 8); // left global_gain
        try self.appendBits(allocator, 6, 4); // left section band_type
        try self.appendBits(allocator, 1, 5); // left section length
        try self.appendScalefactorSymbol(allocator, 60); // zero scalefactor delta
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 1, 1); // tns_present
        try self.appendBits(allocator, 0, 2); // n_filt = 0
        try self.appendBits(allocator, 1, 1); // gain_control_present
        try self.appendBits(allocator, 0, 2); // max_band = 0
        try self.appendAacPairCodebook6NegOneNegOne(allocator);
        try self.appendAacPairCodebook6NegOneNegOne(allocator);

        try self.appendBits(allocator, 100, 8); // right global_gain
        try self.appendBits(allocator, INTENSITY_BT2, 4); // right section band_type
        try self.appendBits(allocator, 1, 5); // right section length
        try self.appendScalefactorSymbol(allocator, 64); // intensity position +4
        try self.appendBits(allocator, 0, 1); // pulse_present
        try self.appendBits(allocator, 0, 1); // tns_present
        try self.appendBits(allocator, 1, 1); // gain_control_present
        try self.appendBits(allocator, 0, 2); // max_band = 0
        try self.appendSyntheticSbrFillElement(allocator);
    }

    fn appendSyntheticSbrFillElement(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendSyntheticEnhancementFillElement(allocator, &.{0xd0});
    }

    fn appendSyntheticPsFillElement(self: *TestBitBuilder, allocator: std.mem.Allocator) !void {
        try self.appendSyntheticEnhancementFillElement(allocator, &.{0xe0});
    }

    fn appendSyntheticEnhancementFillElement(self: *TestBitBuilder, allocator: std.mem.Allocator, payload: []const u8) !void {
        try self.appendBits(allocator, @intFromEnum(ElementKind.fil), 3);
        if (payload.len > 15) return error.UnsupportedAudioFormat;
        try self.appendBits(allocator, payload.len, 4);
        for (payload) |byte| try self.appendBits(allocator, byte, 8);
    }
};

fn appendSyntheticAdtsFrame(
    allocator: std.mem.Allocator,
    adts: *std.ArrayList(u8),
    payload: []const u8,
) !void {
    try appendSyntheticAdtsFrameWithOptions(allocator, adts, payload, .{});
}

const SyntheticAdtsFrameOptions = struct {
    profile: u2 = 1,
    sample_rate_index: u4 = 4,
    channel_config: u3 = 0,
    protection_absent: bool = true,
    data_blocks_in_frame: u2 = 0,
    raw_data_block_positions: []const u16 = &.{},
    header_crc: u16 = 0,
};

fn appendSyntheticAdtsFrameWithOptions(
    allocator: std.mem.Allocator,
    adts: *std.ArrayList(u8),
    payload: []const u8,
    options: SyntheticAdtsFrameOptions,
) !void {
    if (!options.protection_absent and options.raw_data_block_positions.len != options.data_blocks_in_frame) {
        return error.UnsupportedAudioFormat;
    }
    const header_len: usize = if (options.protection_absent) 7 else 9 + @as(usize, options.data_blocks_in_frame) * 2;
    const frame_len_usize = header_len + payload.len;
    if (frame_len_usize > 0x1fff) return error.UnsupportedAudioFormat;
    const frame_len: u16 = @intCast(frame_len_usize);
    const frame = [_]u8{
        0xff,
        if (options.protection_absent) 0xf1 else 0xf0,
        @intCast((@as(u8, options.profile) << 6) | (@as(u8, options.sample_rate_index) << 2) | @as(u8, @intCast((options.channel_config >> 2) & 0x01))),
        @intCast((@as(u8, @intCast(options.channel_config & 0x03)) << 6) | @as(u8, @intCast((frame_len >> 11) & 0x03))),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        @intCast(0xfc | @as(u8, options.data_blocks_in_frame)),
    };
    try adts.appendSlice(allocator, &frame);
    if (!options.protection_absent) {
        for (options.raw_data_block_positions) |position| {
            try adts.append(allocator, @intCast((position >> 8) & 0xff));
            try adts.append(allocator, @intCast(position & 0xff));
        }
        try adts.append(allocator, @intCast((options.header_crc >> 8) & 0xff));
        try adts.append(allocator, @intCast(options.header_crc & 0xff));
    }
    try adts.appendSlice(allocator, payload);
}

test "parse audio specific config from checked-in m4a fixture" {
    var demuxed = try mp4.demux(std.testing.allocator, tone_m4a_bytes);
    defer demuxed.deinit();

    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    try std.testing.expectEqual(@as(u8, 2), config.object_type);
    try std.testing.expectEqual(@as(u32, 16000), config.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), config.channel_config);
    try std.testing.expectEqual(@as(bool, false), config.frame_length_960);
    try std.testing.expectEqual(@as(bool, false), config.depends_on_core_coder);
    try std.testing.expectEqual(@as(bool, false), config.extension_flag);

    const summary = try summarizeAccessUnit(demuxed.access_units[0]);
    try std.testing.expectEqual(ElementKind.cpe, summary.first_element);
    try std.testing.expectEqual(@as(u8, 0), summary.element_instance_tag);

    const prefix = try parseFirstElementPrefix(demuxed.access_units[0]);
    switch (prefix) {
        .cpe => |cpe| {
            try std.testing.expectEqual(@as(u8, 0), cpe.element_instance_tag);
            try std.testing.expectEqual(@as(u8, 140), cpe.left_global_gain);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse audio specific config tracks unsupported ga flags" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz
    try builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try builder.appendBits(std.testing.allocator, 1, 1); // frameLengthFlag = 960
    try builder.appendBits(std.testing.allocator, 1, 1); // dependsOnCoreCoder
    try builder.appendBits(std.testing.allocator, 0x1234, 14); // coreCoderDelay
    try builder.appendBits(std.testing.allocator, 1, 1); // extensionFlag

    const config = try parseAudioSpecificConfig(builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 2), config.object_type);
    try std.testing.expectEqual(@as(u32, 16000), config.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), config.channel_config);
    try std.testing.expectEqual(@as(bool, true), config.frame_length_960);
    try std.testing.expectEqual(@as(bool, true), config.depends_on_core_coder);
    try std.testing.expectEqual(@as(bool, true), config.extension_flag);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            16000,
            2,
            builder.bytes.items,
            &.{},
        ),
    );
}

test "aac main mp4 access unit config decodes mono access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 1, 5); // AAC Main
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSce(std.testing.allocator);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac main mp4 access unit config decodes stereo access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 1, 5); // AAC Main
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentStereoCpe(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config decodes mono 960-sample access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 1, 1); // frameLengthFlag = 960
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSce(std.testing.allocator);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 960), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config decodes stereo 960-sample access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 1, 1); // frameLengthFlag = 960
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentStereoCpe(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 960 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac main adts fixed channel config decodes mono access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentMonoSce(std.testing.allocator);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .profile = 0, // AAC Main
        .channel_config = 1,
    });

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac main adts fixed channel config decodes stereo access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .profile = 0, // AAC Main
        .channel_config = 2,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac main mp4 access unit config decodes mono predictor data" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 1, 5); // AAC Main
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.sce), 3);
    try unit_builder.appendBits(std.testing.allocator, 0, 4); // tag
    try unit_builder.appendBits(std.testing.allocator, 100, 8); // global_gain
    try unit_builder.appendBits(std.testing.allocator, 0, 1); // reserved
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(WindowSequence.only_long), 2);
    try unit_builder.appendBits(std.testing.allocator, 0, 1); // window_shape
    try unit_builder.appendBits(std.testing.allocator, 1, 6); // max_sfb
    try unit_builder.appendBits(std.testing.allocator, 1, 1); // predictor_data_present
    try unit_builder.appendBits(std.testing.allocator, 0, 1); // predictor_reset
    try unit_builder.appendBits(std.testing.allocator, 1, 1); // prediction_used[0]
    try unit_builder.appendBits(std.testing.allocator, ZERO_BT, 4); // section band_type
    try unit_builder.appendBits(std.testing.allocator, 1, 5); // section length
    try unit_builder.appendBits(std.testing.allocator, 0, 1); // pulse_present
    try unit_builder.appendBits(std.testing.allocator, 0, 1); // tns_present
    try unit_builder.appendBits(std.testing.allocator, 0, 1); // gain_control_present

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config accepts lc extension flag" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 1, 1); // extensionFlag

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 2), config.object_type);
    try std.testing.expectEqual(@as(u32, 44100), config.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), config.channel_config);
    try std.testing.expectEqual(@as(bool, true), config.extension_flag);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSce(std.testing.allocator);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config rejects sbr sync extension" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 4, 4); // extension sampling_frequency_index

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 2), config.object_type);
    try std.testing.expectEqual(@as(?u8, 5), config.extension_object_type);
    try std.testing.expectEqual(@as(bool, true), config.sbr_present);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSce(std.testing.allocator);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            1,
            config_builder.bytes.items,
            &.{unit_builder.bytes.items},
        ),
    );
}

test "aac real low-bitrate m4a fixture exposes sbr sync extension" {
    var demuxed = try mp4.demux(std.testing.allocator, tone_sbr_m4a_bytes);
    defer demuxed.deinit();

    const config = try parseAudioSpecificConfig(demuxed.decoder_config);
    try std.testing.expectEqual(@as(u8, 2), config.object_type);
    try std.testing.expectEqual(@as(u32, 16000), config.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), config.channel_config);
    try std.testing.expectEqual(@as(bool, true), config.sbr_present);
    try std.testing.expectEqual(@as(bool, false), config.ps_present);
    try std.testing.expectEqual(@as(?u8, 5), config.extension_object_type);
    try std.testing.expectEqual(@as(?u32, 32000), config.extension_sample_rate);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        demuxed.sample_rate,
        demuxed.channels,
        demuxed.decoder_config,
        demuxed.access_units,
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 17), decoded.frame_count);
    try std.testing.expect(decoded.samples.len > 0);
}

test "aac real low-bitrate m4a fixture lc core decodes at the packet layer" {
    var demuxed = try mp4.demux(std.testing.allocator, tone_sbr_m4a_bytes);
    defer demuxed.deinit();

    var decoded = try decodeChannelPairPcmSequenceAlloc(
        std.testing.allocator,
        demuxed.sample_rate,
        demuxed.access_units,
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 16000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 17), decoded.frame_count);
    try std.testing.expect(decoded.samples.len > 0);
}

test "aac he-aac mp4 access unit config decodes explicit sbr object type" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR
    try config_builder.appendBits(std.testing.allocator, 8, 4); // core sample rate = 16 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sample rate = 32 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC base object type
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 5), config.object_type);
    try std.testing.expectEqual(@as(bool, true), config.sbr_present);
    try std.testing.expectEqual(@as(bool, false), config.ps_present);
    try std.testing.expectEqual(@as(?u8, 2), config.extension_object_type);
    try std.testing.expectEqual(@as(?u32, 32000), config.extension_sample_rate);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentStereoCpe(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac ps mp4 access unit config decodes explicit ps object type" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sample rate = 32 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC base object type
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 29), config.object_type);
    try std.testing.expectEqual(@as(bool, true), config.sbr_present);
    try std.testing.expectEqual(@as(bool, true), config.ps_present);
    try std.testing.expectEqual(@as(?u8, 2), config.extension_object_type);
    try std.testing.expectEqual(@as(?u32, 32000), config.extension_sample_rate);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentStereoCpe(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac ps mp4 access unit config decodes explicit ps mono-core stereo output" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono core
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sample rate = 32 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC base object type
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendNonZeroMonoSce(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
    var side_energy: f32 = 0;
    for (0..decoded.samples.len / 2) |frame_index| {
        side_energy += @abs(decoded.samples[frame_index * 2] - decoded.samples[frame_index * 2 + 1]);
    }
    try std.testing.expect(side_energy > 0);
}

test "aac explicit ps mono-core stereo carries payload profile across later no-fill access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono core
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sample rate = 32 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC base object type
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x20, 0xf0, 0x40 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[1].saw_ps_payload);
    try std.testing.expectEqual(@as(u8, 1), resolved_infos[1].ps_carry_generations);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 4096 * 2), decoded.samples.len);

    const first_block = decoded.samples[0..4096];
    const second_block = decoded.samples[4096..8192];
    var first_side_energy: f32 = 0;
    var second_side_energy: f32 = 0;
    for (0..first_block.len / 2) |frame_index| {
        const first_side = first_block[frame_index * 2] - first_block[frame_index * 2 + 1];
        const second_side = second_block[frame_index * 2] - second_block[frame_index * 2 + 1];
        first_side_energy += first_side * first_side;
        second_side_energy += second_side * second_side;
    }
    try std.testing.expect(first_side_energy > second_side_energy);
    try std.testing.expect(second_side_energy > 0);
}

test "aac explicit ps mono-core stereo delays activation until first payload access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono core
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sample rate = 32 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC base object type
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticPsFillElement(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(bool, false), resolved_infos[0].explicit_payload_active);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[1].explicit_payload_active);
    try std.testing.expectEqual(@as(bool, false), resolved_infos[0].explicit_ps_payload_active);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[1].explicit_ps_payload_active);
    try std.testing.expectEqual(@as(bool, false), resolved_infos[0].saw_ps_payload);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[1].saw_ps_payload);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded.deinit();

    const first_block = decoded.samples[0..4096];
    const second_block = decoded.samples[4096..8192];
    var first_side_energy: f32 = 0;
    var second_side_energy: f32 = 0;
    for (0..first_block.len / 2) |frame_index| {
        const first_side = first_block[frame_index * 2] - first_block[frame_index * 2 + 1];
        const second_side = second_block[frame_index * 2] - second_block[frame_index * 2 + 1];
        first_side_energy += first_side * first_side;
        second_side_energy += second_side * second_side;
    }
    try std.testing.expectApproxEqAbs(@as(f32, 0), first_side_energy, 1e-6);
    try std.testing.expect(second_side_energy > 0);
}

test "aac explicit ps mono-core stereo ignores sbr-only payload until first ps payload" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono core
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sample rate = 32 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC base object type
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticSbrFillElement(std.testing.allocator);

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticPsFillElement(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[0].explicit_payload_active);
    try std.testing.expectEqual(@as(bool, false), resolved_infos[0].explicit_ps_payload_active);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[1].explicit_ps_payload_active);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded.deinit();

    const first_block = decoded.samples[0..4096];
    const second_block = decoded.samples[4096..8192];
    var first_side_energy: f32 = 0;
    var second_side_energy: f32 = 0;
    for (0..first_block.len / 2) |frame_index| {
        const first_side = first_block[frame_index * 2] - first_block[frame_index * 2 + 1];
        const second_side = second_block[frame_index * 2] - second_block[frame_index * 2 + 1];
        first_side_energy += first_side * first_side;
        second_side_energy += second_side * second_side;
    }
    try std.testing.expectApproxEqAbs(@as(f32, 0), first_side_energy, 1e-6);
    try std.testing.expect(second_side_energy > 0);
}

test "aac lc mp4 sync-extension ps fill payload decodes mono-core stereo output" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono core
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sampling_frequency_index = 32 kHz

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 2), config.object_type);
    try std.testing.expectEqual(@as(bool, true), config.sbr_present);
    try std.testing.expectEqual(@as(bool, true), config.ps_present);
    try std.testing.expectEqual(@as(?u8, 29), config.extension_object_type);
    try std.testing.expectEqual(@as(?u32, 32000), config.extension_sample_rate);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendNonZeroMonoSce(std.testing.allocator);
    try unit_builder.appendSyntheticPsFillElement(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
    var side_energy: f32 = 0;
    for (0..decoded.samples.len / 2) |frame_index| {
        side_energy += @abs(decoded.samples[frame_index * 2] - decoded.samples[frame_index * 2 + 1]);
    }
    try std.testing.expect(side_energy > 0);
}

test "aac lc mp4 sync-extension sbr fill payload upsamples stereo access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // core sample rate = 16 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sampling_frequency_index = 32 kHz

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentStereoCpe(std.testing.allocator);
    try unit_builder.appendSyntheticSbrFillElement(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac stereo cpe with gain control and sbr fill payload decodes" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // core sample rate = 16 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sampling_frequency_index = 32 kHz

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentStereoCpeWithGainControlAndSbrFill(std.testing.allocator, 0);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac stereo intensity cpe with tns gain and sbr fill decodes" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // core sample rate = 16 kHz
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // extension sampling_frequency_index = 32 kHz

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendStereoIntensityCpeWithTnsGainAndSbrFill(std.testing.allocator, 0);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
    try std.testing.expect(@abs(decoded.samples[0]) > 0);
    try std.testing.expect(@abs(decoded.samples[1]) > 0);
    try std.testing.expect(@abs(decoded.samples[0] - decoded.samples[1]) > 1e-6);
}

test "aac lc mp4 access unit config ignores sbr sync pattern inside pce comment" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0, 4); // element_instance_tag
    try config_builder.appendBits(std.testing.allocator, 1, 2); // profile
    try config_builder.appendBits(std.testing.allocator, 4, 4); // sampling_frequency_index = 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // num_front_channel_elements
    try config_builder.appendBits(std.testing.allocator, 0, 4); // num_side_channel_elements
    try config_builder.appendBits(std.testing.allocator, 0, 4); // num_back_channel_elements
    try config_builder.appendBits(std.testing.allocator, 0, 2); // num_lfe_channel_elements
    try config_builder.appendBits(std.testing.allocator, 0, 3); // num_assoc_data_elements
    try config_builder.appendBits(std.testing.allocator, 0, 4); // num_valid_cc_elements
    try config_builder.appendBits(std.testing.allocator, 0, 1); // mono_mixdown_present
    try config_builder.appendBits(std.testing.allocator, 0, 1); // stereo_mixdown_present
    try config_builder.appendBits(std.testing.allocator, 0, 1); // matrix_mixdown_idx_present
    try config_builder.appendBits(std.testing.allocator, 0, 1); // front element is SCE
    try config_builder.appendBits(std.testing.allocator, 0, 4); // front element tag
    try config_builder.alignToByte(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 3, 8); // comment_field_bytes
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType inside comment only
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type inside comment only
    try config_builder.appendBits(std.testing.allocator, 4, 4); // extension sampling_frequency_index inside comment only
    try config_builder.appendBits(std.testing.allocator, 0, 4); // comment padding

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 2), config.object_type);
    try std.testing.expectEqual(@as(u32, 44100), config.sample_rate);
    try std.testing.expectEqual(@as(u8, 0), config.channel_config);
    try std.testing.expectEqual(@as(?u8, 1), config.explicit_channel_count);
    try std.testing.expectEqual(@as(?u8, null), config.extension_object_type);
    try std.testing.expectEqual(@as(bool, false), config.sbr_present);
    try std.testing.expectEqual(@as(bool, false), config.ps_present);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSce(std.testing.allocator);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config infers mono from explicit pce" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendMonoPce(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 0), config.channel_config);
    try std.testing.expectEqual(@as(?u8, 1), config.explicit_channel_count);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSce(std.testing.allocator);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config skips metadata-only mono access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var metadata_unit = TestBitBuilder.init();
    defer metadata_unit.deinit(std.testing.allocator);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.dse), 3);
    try metadata_unit.appendBits(std.testing.allocator, 0, 4); // element_instance_tag
    try metadata_unit.appendBits(std.testing.allocator, 1, 1); // byte_align
    try metadata_unit.appendBits(std.testing.allocator, 1, 8); // count
    try metadata_unit.alignToByte(std.testing.allocator);
    try metadata_unit.appendBits(std.testing.allocator, 0xaa, 8);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.fil), 3);
    try metadata_unit.appendBits(std.testing.allocator, 1, 4); // count
    try metadata_unit.appendBits(std.testing.allocator, 0xbb, 8);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var audio_unit = TestBitBuilder.init();
    defer audio_unit.deinit(std.testing.allocator);
    try audio_unit.appendSilentMonoSce(std.testing.allocator);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{ metadata_unit.bytes.items, audio_unit.bytes.items },
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config rejects metadata-only mono access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag

    var metadata_unit = TestBitBuilder.init();
    defer metadata_unit.deinit(std.testing.allocator);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.fil), 3);
    try metadata_unit.appendBits(std.testing.allocator, 1, 4); // count
    try metadata_unit.appendBits(std.testing.allocator, 0xbb, 8);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            1,
            config_builder.bytes.items,
            &.{metadata_unit.bytes.items},
        ),
    );
}

test "aac lc adts explicit pce channel config decodes mono access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoPce(std.testing.allocator);
    try payload_builder.appendSilentMonoSce(std.testing.allocator);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config reuses first-frame mono layout" {
    var first_payload_builder = TestBitBuilder.init();
    defer first_payload_builder.deinit(std.testing.allocator);
    try first_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try first_payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);
    try first_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    var second_payload_builder = TestBitBuilder.init();
    defer second_payload_builder.deinit(std.testing.allocator);
    try second_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, first_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, second_payload_builder.bytes.items);

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config skips metadata-only leading mono frame" {
    var metadata_payload_builder = TestBitBuilder.init();
    defer metadata_payload_builder.deinit(std.testing.allocator);
    try metadata_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try metadata_payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);
    try metadata_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var audio_payload_builder = TestBitBuilder.init();
    defer audio_payload_builder.deinit(std.testing.allocator);
    try audio_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, metadata_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, audio_payload_builder.bytes.items);

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config skips metadata-only pre-layout mono frame" {
    var metadata_payload_builder = TestBitBuilder.init();
    defer metadata_payload_builder.deinit(std.testing.allocator);
    try metadata_payload_builder.appendMetadataOnlyDseFilEnd(std.testing.allocator);

    var audio_payload_builder = TestBitBuilder.init();
    defer audio_payload_builder.deinit(std.testing.allocator);
    try audio_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try audio_payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);
    try audio_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, metadata_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, audio_payload_builder.bytes.items);

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config rejects metadata-only mono stream" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, payload_builder.bytes.items);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects missing initial layout" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentMonoSce(std.testing.allocator);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, payload_builder.bytes.items);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects later mono tag mismatch" {
    var first_payload_builder = TestBitBuilder.init();
    defer first_payload_builder.deinit(std.testing.allocator);
    try first_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try first_payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try first_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 3);

    var second_payload_builder = TestBitBuilder.init();
    defer second_payload_builder.deinit(std.testing.allocator);
    try second_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, first_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, second_payload_builder.bytes.items);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects mixed sample rate frames" {
    var first_payload_builder = TestBitBuilder.init();
    defer first_payload_builder.deinit(std.testing.allocator);
    try first_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try first_payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .front, 0);
    try first_payload_builder.appendSilentMonoSce(std.testing.allocator);

    var second_payload_builder = TestBitBuilder.init();
    defer second_payload_builder.deinit(std.testing.allocator);
    try second_payload_builder.appendSilentMonoSce(std.testing.allocator);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, first_payload_builder.bytes.items);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, second_payload_builder.bytes.items, .{
        .sample_rate_index = 3, // 48 kHz, mismatching the leading 44.1 kHz frame
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc mp4 explicit pce mono lfe decodes access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendMonoLfePce(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(?u8, 1), config.explicit_channel_count);
    try std.testing.expect(config.explicit_layout.?.matchesSupportedOutput(1));

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentLfeWithTag(std.testing.allocator, 0);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config decodes mono lfe access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoLfePce(std.testing.allocator);
    try payload_builder.appendSilentLfeWithTag(std.testing.allocator, 0);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 explicit pce mono side sce tag must match access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(?u8, 1), config.explicit_channel_count);
    try std.testing.expect(config.explicit_layout.?.matchesSupportedOutput(1));

    var matching_unit = TestBitBuilder.init();
    defer matching_unit.deinit(std.testing.allocator);
    try matching_unit.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        1,
        config_builder.bytes.items,
        &.{matching_unit.bytes.items},
    );
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);

    var mismatched_unit = TestBitBuilder.init();
    defer mismatched_unit.deinit(std.testing.allocator);
    try mismatched_unit.appendSilentMonoSceWithTag(std.testing.allocator, 0);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            1,
            config_builder.bytes.items,
            &.{mismatched_unit.bytes.items},
        ),
    );
}

test "aac lc mp4 explicit pce rejects mismatched metadata-only mono access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);

    var metadata_unit = TestBitBuilder.init();
    defer metadata_unit.deinit(std.testing.allocator);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try metadata_unit.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var audio_unit = TestBitBuilder.init();
    defer audio_unit.deinit(std.testing.allocator);
    try audio_unit.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            1,
            config_builder.bytes.items,
            &.{ metadata_unit.bytes.items, audio_unit.bytes.items },
        ),
    );
}

test "aac lc mp4 explicit pce rejects mismatched in-band mono pce before matching audio" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try unit_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try unit_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            1,
            config_builder.bytes.items,
            &.{unit_builder.bytes.items},
        ),
    );
}

test "aac lc mp4 explicit pce rejects conflicting repeated in-band mono pce" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try unit_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);
    try unit_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try unit_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            1,
            config_builder.bytes.items,
            &.{unit_builder.bytes.items},
        ),
    );
}

test "aac lc adts explicit pce channel config decodes mono back sce access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 3);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 access unit config infers stereo from explicit pce" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPce(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 0), config.channel_config);
    try std.testing.expectEqual(@as(?u8, 2), config.explicit_channel_count);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentStereoCpe(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 explicit pce skips metadata-only stereo access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);

    var metadata_unit = TestBitBuilder.init();
    defer metadata_unit.deinit(std.testing.allocator);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try metadata_unit.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var audio_unit = TestBitBuilder.init();
    defer audio_unit.deinit(std.testing.allocator);
    try audio_unit.appendSilentStereoCpeWithTag(std.testing.allocator, 2);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{ metadata_unit.bytes.items, audio_unit.bytes.items },
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 explicit pce rejects mismatched metadata-only stereo access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);

    var metadata_unit = TestBitBuilder.init();
    defer metadata_unit.deinit(std.testing.allocator);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try metadata_unit.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var audio_unit = TestBitBuilder.init();
    defer audio_unit.deinit(std.testing.allocator);
    try audio_unit.appendSilentStereoCpeWithTag(std.testing.allocator, 2);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{ metadata_unit.bytes.items, audio_unit.bytes.items },
        ),
    );
}

test "aac lc mp4 explicit pce rejects metadata-only stereo access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);

    var metadata_unit = TestBitBuilder.init();
    defer metadata_unit.deinit(std.testing.allocator);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try metadata_unit.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try metadata_unit.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{metadata_unit.bytes.items},
        ),
    );
}

test "aac lc mp4 explicit pce rejects mismatched in-band stereo pce before matching audio" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try unit_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try unit_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{unit_builder.bytes.items},
        ),
    );
}

test "aac lc mp4 explicit pce rejects conflicting repeated in-band stereo pce" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try unit_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try unit_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try unit_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try unit_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{unit_builder.bytes.items},
        ),
    );
}

test "aac lc adts explicit pce channel config decodes stereo access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPce(std.testing.allocator);
    try payload_builder.appendSilentStereoCpe(std.testing.allocator);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config reuses first-frame stereo cpe layout" {
    var first_payload_builder = TestBitBuilder.init();
    defer first_payload_builder.deinit(std.testing.allocator);
    try first_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try first_payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try first_payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    var second_payload_builder = TestBitBuilder.init();
    defer second_payload_builder.deinit(std.testing.allocator);
    try second_payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, first_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, second_payload_builder.bytes.items);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config reuses first-frame stereo sce layout" {
    var first_payload_builder = TestBitBuilder.init();
    defer first_payload_builder.deinit(std.testing.allocator);
    try first_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try first_payload_builder.appendStereoScePairPceWithFrontBackTags(std.testing.allocator, 2, 3);
    try first_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try first_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 3);

    var second_payload_builder = TestBitBuilder.init();
    defer second_payload_builder.deinit(std.testing.allocator);
    try second_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try second_payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, first_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, second_payload_builder.bytes.items);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config reuses first raw-data-block stereo layout" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .data_blocks_in_frame = 1,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config skips metadata-only first raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .data_blocks_in_frame = 1,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config skips metadata-only pre-layout raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendByteAlignedMetadataOnlyFilEnd(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .data_blocks_in_frame = 1,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config rejects later raw-data-block stereo tag mismatch" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .data_blocks_in_frame = 1,
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects conflicting repeated layout in same raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, payload_builder.bytes.items);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects conflicting repeated mono layout in same raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, payload_builder.bytes.items);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects conflicting repeated layout in later raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .data_blocks_in_frame = 1,
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects conflicting repeated mono layout in later raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .data_blocks_in_frame = 1,
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config reuses crc-protected raw-data-block stereo layout" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoScePairPceWithFrontBackTags(std.testing.allocator, 2, 3);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x1234, 16); // first raw_data_block CRC
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 3);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x5678, 16); // second raw_data_block CRC

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    const raw_data_block_positions = [_]u16{0};
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .protection_absent = false,
        .data_blocks_in_frame = 1,
        .raw_data_block_positions = &raw_data_block_positions,
        .header_crc = 0xabcd,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config skips crc-protected metadata-only pre-layout raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendByteAlignedMetadataOnlyFilEnd(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, 0x1234, 16); // first raw_data_block CRC
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x5678, 16); // second raw_data_block CRC

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    const raw_data_block_positions = [_]u16{0};
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .protection_absent = false,
        .data_blocks_in_frame = 1,
        .raw_data_block_positions = &raw_data_block_positions,
        .header_crc = 0xabcd,
    });

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config rejects crc-protected audio before layout raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x1234, 16); // first raw_data_block CRC
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x5678, 16); // second raw_data_block CRC

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    const raw_data_block_positions = [_]u16{0};
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .protection_absent = false,
        .data_blocks_in_frame = 1,
        .raw_data_block_positions = &raw_data_block_positions,
        .header_crc = 0xabcd,
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects conflicting repeated layout in crc-protected raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x1234, 16); // first raw_data_block CRC
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x5678, 16); // second raw_data_block CRC

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    const raw_data_block_positions = [_]u16{0};
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .protection_absent = false,
        .data_blocks_in_frame = 1,
        .raw_data_block_positions = &raw_data_block_positions,
        .header_crc = 0xabcd,
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects conflicting repeated mono layout in crc-protected raw-data-block" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .side, 2);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x1234, 16); // first raw_data_block CRC
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendMonoRegularScePceWithTag(std.testing.allocator, .back, 3);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.end), 3);
    try payload_builder.appendBits(std.testing.allocator, 0x5678, 16); // second raw_data_block CRC

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    const raw_data_block_positions = [_]u16{0};
    try appendSyntheticAdtsFrameWithOptions(std.testing.allocator, &adts, payload_builder.bytes.items, .{
        .protection_absent = false,
        .data_blocks_in_frame = 1,
        .raw_data_block_positions = &raw_data_block_positions,
        .header_crc = 0xabcd,
    });

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedMonoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects later stereo tag mismatch" {
    var first_payload_builder = TestBitBuilder.init();
    defer first_payload_builder.deinit(std.testing.allocator);
    try first_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try first_payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try first_payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);

    var second_payload_builder = TestBitBuilder.init();
    defer second_payload_builder.deinit(std.testing.allocator);
    try second_payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, first_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, second_payload_builder.bytes.items);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc adts explicit pce channel config rejects conflicting repeated stereo layout" {
    var first_payload_builder = TestBitBuilder.init();
    defer first_payload_builder.deinit(std.testing.allocator);
    try first_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try first_payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);
    try first_payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 2);

    var second_payload_builder = TestBitBuilder.init();
    defer second_payload_builder.deinit(std.testing.allocator);
    try second_payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try second_payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try second_payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, first_payload_builder.bytes.items);
    try appendSyntheticAdtsFrame(std.testing.allocator, &adts, second_payload_builder.bytes.items);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc mp4 explicit pce stereo cpe tag must match access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPceWithTag(std.testing.allocator, 2);

    var matching_unit = TestBitBuilder.init();
    defer matching_unit.deinit(std.testing.allocator);
    try matching_unit.appendSilentStereoCpeWithTag(std.testing.allocator, 2);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{matching_unit.bytes.items},
    );
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);

    var mismatched_unit = TestBitBuilder.init();
    defer mismatched_unit.deinit(std.testing.allocator);
    try mismatched_unit.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{mismatched_unit.bytes.items},
        ),
    );
}

test "aac lc adts explicit pce stereo cpe tag must match access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithTag(std.testing.allocator, 2);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac lc mp4 explicit pce stereo side cpe tag must match access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoPceWithPositionTag(std.testing.allocator, .side, 2);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 0), config.channel_config);
    try std.testing.expectEqual(@as(?u8, 2), config.explicit_channel_count);

    var matching_unit = TestBitBuilder.init();
    defer matching_unit.deinit(std.testing.allocator);
    try matching_unit.appendSilentStereoCpeWithTag(std.testing.allocator, 2);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{matching_unit.bytes.items},
    );
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);

    var mismatched_unit = TestBitBuilder.init();
    defer mismatched_unit.deinit(std.testing.allocator);
    try mismatched_unit.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{mismatched_unit.bytes.items},
        ),
    );
}

test "aac lc adts explicit pce channel config decodes stereo back cpe access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoPceWithPositionTag(std.testing.allocator, .back, 3);
    try payload_builder.appendSilentStereoCpeWithTag(std.testing.allocator, 3);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 explicit pce stereo sce pair decodes access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoScePairPce(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 0), config.channel_config);
    try std.testing.expectEqual(@as(?u8, 2), config.explicit_channel_count);

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSceWithTag(std.testing.allocator, 0);
    try unit_builder.appendSilentMonoSceWithTag(std.testing.allocator, 1);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc adts explicit pce channel config decodes stereo sce pair access unit" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoScePairPce(std.testing.allocator);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 0);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 1);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    var decoded = try decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);
    for (decoded.samples) |sample| try std.testing.expectApproxEqAbs(@as(f32, 0), sample, 1e-6);
}

test "aac lc mp4 explicit pce stereo sce pair tags must match access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoScePairPceWithTags(std.testing.allocator, 2, 3);

    var matching_unit = TestBitBuilder.init();
    defer matching_unit.deinit(std.testing.allocator);
    try matching_unit.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try matching_unit.appendSilentMonoSceWithTag(std.testing.allocator, 3);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{matching_unit.bytes.items},
    );
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);

    var mismatched_unit = TestBitBuilder.init();
    defer mismatched_unit.deinit(std.testing.allocator);
    try mismatched_unit.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try mismatched_unit.appendSilentMonoSceWithTag(std.testing.allocator, 1);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{mismatched_unit.bytes.items},
        ),
    );
}

test "aac lc mp4 explicit pce stereo front back sce tags must match access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoScePairPceWithFrontBackTags(std.testing.allocator, 2, 3);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(u8, 0), config.channel_config);
    try std.testing.expectEqual(@as(?u8, 2), config.explicit_channel_count);

    var matching_unit = TestBitBuilder.init();
    defer matching_unit.deinit(std.testing.allocator);
    try matching_unit.appendSilentMonoSceWithTag(std.testing.allocator, 2);
    try matching_unit.appendSilentMonoSceWithTag(std.testing.allocator, 3);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{matching_unit.bytes.items},
    );
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1024 * 2), decoded.samples.len);

    var mismatched_unit = TestBitBuilder.init();
    defer mismatched_unit.deinit(std.testing.allocator);
    try mismatched_unit.appendSilentMonoSceWithTag(std.testing.allocator, 3);
    try mismatched_unit.appendSilentMonoSceWithTag(std.testing.allocator, 2);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{mismatched_unit.bytes.items},
        ),
    );
}

test "aac lc mp4 explicit pce rejects sce plus lfe as stereo" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz
    try config_builder.appendBits(std.testing.allocator, 0, 4); // explicit PCE channel config
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendStereoSceLfePce(std.testing.allocator);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    try std.testing.expectEqual(@as(?u8, 2), config.explicit_channel_count);
    try std.testing.expect(!config.explicit_layout.?.matchesSupportedOutput(2));

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSceWithTag(std.testing.allocator, 0);
    try unit_builder.appendSilentLfeWithTag(std.testing.allocator, 1);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            44100,
            2,
            config_builder.bytes.items,
            &.{unit_builder.bytes.items},
        ),
    );
}

test "aac lc adts explicit pce rejects sce plus lfe as stereo" {
    var payload_builder = TestBitBuilder.init();
    defer payload_builder.deinit(std.testing.allocator);
    try payload_builder.appendBits(std.testing.allocator, @intFromEnum(ElementKind.pce), 3);
    try payload_builder.appendStereoSceLfePce(std.testing.allocator);
    try payload_builder.appendSilentMonoSceWithTag(std.testing.allocator, 0);
    try payload_builder.appendSilentLfeWithTag(std.testing.allocator, 1);

    const payload = payload_builder.bytes.items;
    const frame_len: u16 = @intCast(7 + payload.len);
    const frame = [_]u8{
        0xff,
        0xf1,
        0x50,
        @intCast((frame_len >> 11) & 0x03),
        @intCast((frame_len >> 3) & 0xff),
        @intCast(((frame_len & 0x07) << 5) | 0x1f),
        0xfc,
    };

    var adts = std.ArrayList(u8).empty;
    defer adts.deinit(std.testing.allocator);
    try adts.appendSlice(std.testing.allocator, &frame);
    try adts.appendSlice(std.testing.allocator, payload);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAdtsAlloc(std.testing.allocator, adts.items),
    );
}

test "aac gain control parser consumes ffmpeg-compatible payload shape" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 1, 2); // max_band
    try builder.appendBits(std.testing.allocator, 2, 3); // adjust_num
    try builder.appendBits(std.testing.allocator, 0xa, 4); // alevcode
    try builder.appendBits(std.testing.allocator, 0x11, 5); // aloccode
    try builder.appendBits(std.testing.allocator, 0x3, 4); // alevcode
    try builder.appendBits(std.testing.allocator, 0x05, 5); // aloccode

    var reader = BitReader.init(builder.bytes.items);
    const gain = try parseGainControlData(&reader, .{
        .window_sequence = .only_long,
        .window_shape = 0,
        .max_sfb = 1,
        .num_window_groups = 1,
        .window_group_length = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    });
    try std.testing.expectEqual(@as(u8, 1), gain.max_band);
    try std.testing.expectEqual(@as(u8, 2), gain.bands[0].windows[0].adjust_num);
    try std.testing.expectEqual(@as(u4, 0xa), gain.bands[0].windows[0].adjustments[0].level);
    try std.testing.expectEqual(@as(u8, 0x11), gain.bands[0].windows[0].adjustments[0].location);
    try std.testing.expectEqual(@as(u4, 0x3), gain.bands[0].windows[0].adjustments[1].level);
    try std.testing.expectEqual(@as(u8, 0x05), gain.bands[0].windows[0].adjustments[1].location);
    try std.testing.expectEqual(builder.bit_len, reader.bit_offset);
}

test "aac gain control parser handles long-start window location widths" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 1, 2); // max_band
    try builder.appendBits(std.testing.allocator, 1, 3); // window 0 adjust_num
    try builder.appendBits(std.testing.allocator, 0x7, 4); // alevcode
    try builder.appendBits(std.testing.allocator, 0xe, 4); // long-start window 0 aloccode
    try builder.appendBits(std.testing.allocator, 1, 3); // window 1 adjust_num
    try builder.appendBits(std.testing.allocator, 0x6, 4); // alevcode
    try builder.appendBits(std.testing.allocator, 0x2, 2); // long-start window 1 aloccode

    var reader = BitReader.init(builder.bytes.items);
    const gain = try parseGainControlData(&reader, .{
        .window_sequence = .long_start,
        .window_shape = 0,
        .max_sfb = 1,
        .num_window_groups = 1,
        .window_group_length = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    });

    try std.testing.expectEqual(@as(u8, 1), gain.max_band);
    try std.testing.expectEqual(@as(u8, 1), gain.bands[0].windows[0].adjust_num);
    try std.testing.expectEqual(@as(u4, 0x7), gain.bands[0].windows[0].adjustments[0].level);
    try std.testing.expectEqual(@as(u8, 0xe), gain.bands[0].windows[0].adjustments[0].location);
    try std.testing.expectEqual(@as(u8, 1), gain.bands[0].windows[1].adjust_num);
    try std.testing.expectEqual(@as(u4, 0x6), gain.bands[0].windows[1].adjustments[0].level);
    try std.testing.expectEqual(@as(u8, 0x2), gain.bands[0].windows[1].adjustments[0].location);
    try std.testing.expectEqual(builder.bit_len, reader.bit_offset);
}

test "aac eight-short gain control parser consumes max-band shape" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);

    try builder.appendBits(std.testing.allocator, 0, 3); // max_band

    var reader = BitReader.init(builder.bytes.items);
    const gain = try parseGainControlData(&reader, .{
        .window_sequence = .eight_short,
        .window_shape = 0,
        .max_sfb = 1,
        .num_window_groups = 1,
        .window_group_length = .{ 8, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = null,
    });
    try std.testing.expectEqual(@as(u8, 0), gain.max_band);
    try std.testing.expectEqual(builder.bit_len, reader.bit_offset);
}

test "aac perceptual noise substitution is deterministic" {
    var coefficients_a = [_]f32{0} ** 8;
    var coefficients_b = [_]f32{0} ** 8;
    const plans = [_]SpectralBandLayout{.{
        .band_type = NOISE_BT,
        .class = .noise,
        .dimensions = 0,
        .unsigned_values = false,
        .uses_escape = false,
        .scalefactor_kind = .noise,
        .scalefactor_value = 100,
        .coeff_start = 0,
        .coeff_end = 8,
        .symbol_count = 0,
    }};
    var state_a = INITIAL_PNS_STATE;
    var state_b = INITIAL_PNS_STATE;
    try applyPerceptualNoiseSubstitution(&coefficients_a, &plans, &state_a);
    try applyPerceptualNoiseSubstitution(&coefficients_b, &plans, &state_b);
    try std.testing.expectEqualSlices(f32, &coefficients_a, &coefficients_b);
    try std.testing.expect(@abs(coefficients_a[0]) > 0);
}

test "aac pulse tool adjusts dequantized coefficient" {
    var coefficients = [_]f32{ 0, 2.0, 0, 0 };
    const plans = [_]SpectralBandLayout{.{
        .band_type = 5,
        .class = .pair,
        .dimensions = 2,
        .unsigned_values = true,
        .uses_escape = false,
        .scalefactor_kind = .spectral,
        .scalefactor_value = 100,
        .coeff_start = 0,
        .coeff_end = 4,
        .symbol_count = 2,
    }};
    const pulse = PulseData{
        .num_pulse = 1,
        .pulse_swb = 0,
        .offsets = .{ 1, 0, 0, 0 },
        .amplitudes = .{ 2, 0, 0, 0 },
    };
    try applyPulseTool(&coefficients, &plans, &.{ 0, 4 }, pulse);
    try std.testing.expect(coefficients[1] != 2.0);
}

test "aac intensity stereo reconstructs right channel band" {
    const left = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var right = [_]f32{ 0, 0, 0, 0 };
    const left_bands = [_]ScalefactorBand{.{ .band_type = 5, .kind = .spectral, .value = 100 }};
    const right_bands = [_]ScalefactorBand{.{ .band_type = INTENSITY_BT2, .kind = .intensity, .value = 4 }};
    applyIntensityStereo(&left, &right, &left_bands, &right_bands, &.{false}, &.{ 0, 4 });
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), right[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), right[3], 1e-6);
}

test "aac sbr enhancement synthesis adds detail beyond linear upsample" {
    const input = [_]f32{ 0.0, 1.0, 0.0, -1.0 };
    const linear = try upsampleLinearInterleavedAlloc(std.testing.allocator, &input, 1);
    defer std.testing.allocator.free(linear);
    const enhanced = try upsampleAndEnhanceSbrInterleavedAlloc(std.testing.allocator, &input, 1, .{
        .saw_sbr_payload = true,
        .max_payload_len = 1,
        .payload_hash = 0x12345678,
        .envelope_hint = 0x30,
        .noise_hint = 0x10,
        .stereo_hint = 0x20,
        .harmonic_hint = 0x08,
    });
    defer std.testing.allocator.free(enhanced);

    try std.testing.expectEqual(linear.len, enhanced.len);
    var saw_difference = false;
    for (linear, enhanced) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac sbr enhancement synthesis responds to tail detail hints" {
    const input = [_]f32{ 0.0, 1.0, 0.0, -1.0 };
    const narrow = try upsampleAndEnhanceSbrInterleavedAlloc(std.testing.allocator, &input, 1, .{
        .saw_sbr_payload = true,
        .max_payload_len = 6,
        .payload_hash = 0x12345678,
        .envelope_hint = 0x30,
        .noise_hint = 0x10,
        .stereo_hint = 0x20,
        .harmonic_hint = 0x08,
        .detail_hint = 0x08,
        .phase_hint = 0x01,
    });
    defer std.testing.allocator.free(narrow);
    const wide = try upsampleAndEnhanceSbrInterleavedAlloc(std.testing.allocator, &input, 1, .{
        .saw_sbr_payload = true,
        .max_payload_len = 6,
        .payload_hash = 0x12345678,
        .envelope_hint = 0x30,
        .noise_hint = 0x10,
        .stereo_hint = 0x20,
        .harmonic_hint = 0x08,
        .detail_hint = 0xf0,
        .phase_hint = 0x03,
    });
    defer std.testing.allocator.free(wide);

    var saw_difference = false;
    for (narrow, wide) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac fill element parser captures payload stats" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, 5, 4);
    try builder.appendBits(std.testing.allocator, 0xd0, 8);
    try builder.appendBits(std.testing.allocator, 0x11, 8);
    try builder.appendBits(std.testing.allocator, 0x22, 8);
    try builder.appendBits(std.testing.allocator, 0x33, 8);
    try builder.appendBits(std.testing.allocator, 0x44, 8);

    var reader = BitReader.init(builder.bytes.items);
    const info = try parseFillElement(&reader);
    try std.testing.expectEqual(@as(bool, true), info.saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, false), info.saw_ps_payload);
    try std.testing.expectEqual(@as(u16, 5), info.payload_len);
    try std.testing.expect(info.payload_hash != 2166136261);
    try std.testing.expectEqual(@as(u32, 2166136261), info.ps_payload_hash);
    try std.testing.expectEqual(@as(u8, 0x11), info.envelope_hint);
    try std.testing.expectEqual(@as(u8, 0x22), info.noise_hint);
    try std.testing.expectEqual(@as(u8, 0x33), info.stereo_hint);
    try std.testing.expectEqual(@as(u8, 0x44), info.harmonic_hint);
    try std.testing.expectEqual(builder.bit_len, reader.bit_offset);
}

test "aac fill element parser distinguishes ps payload marker" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, 5, 4);
    try builder.appendBits(std.testing.allocator, 0xe0, 8);
    try builder.appendBits(std.testing.allocator, 0x11, 8);
    try builder.appendBits(std.testing.allocator, 0x22, 8);
    try builder.appendBits(std.testing.allocator, 0x33, 8);
    try builder.appendBits(std.testing.allocator, 0x44, 8);

    var reader = BitReader.init(builder.bytes.items);
    const info = try parseFillElement(&reader);
    try std.testing.expectEqual(@as(bool, true), info.saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, true), info.saw_ps_payload);
    try std.testing.expect(info.ps_payload_hash != 2166136261);
    try std.testing.expectEqual(@as(u8, 0x22), info.ps_noise_hint);
    try std.testing.expectEqual(@as(u8, 0x33), info.ps_stereo_hint);
    try std.testing.expectEqual(@as(u8, 0x44), info.ps_harmonic_hint);
    try std.testing.expectEqual(builder.bit_len, reader.bit_offset);
}

test "aac fill element parser captures tail detail hints" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendBits(std.testing.allocator, 7, 4);
    try builder.appendBits(std.testing.allocator, 0xe0, 8);
    try builder.appendBits(std.testing.allocator, 0x11, 8);
    try builder.appendBits(std.testing.allocator, 0x22, 8);
    try builder.appendBits(std.testing.allocator, 0x33, 8);
    try builder.appendBits(std.testing.allocator, 0x44, 8);
    try builder.appendBits(std.testing.allocator, 0x55, 8);
    try builder.appendBits(std.testing.allocator, 0x66, 8);

    var reader = BitReader.init(builder.bytes.items);
    const info = try parseFillElement(&reader);
    try std.testing.expectEqual(@as(u8, 0x44), info.detail_hint);
    try std.testing.expect(info.phase_hint != 0);
    try std.testing.expectEqual(@as(u8, 0x44), info.ps_detail_hint);
    try std.testing.expect(info.ps_phase_hint != 0);
}

test "aac access unit trailing info aggregates payload structure" {
    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x12, 0x20, 0x11, 0x08 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x90, 0x40, 0x80, 0x44 });

    const info = try accessUnitsTrailingInfoAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    try std.testing.expectEqual(@as(bool, true), info.saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, false), info.saw_ps_payload);
    try std.testing.expectEqual(@as(u16, 5), info.max_payload_len);
    try std.testing.expectEqual(@as(u8, 0x90), info.envelope_hint);
    try std.testing.expectEqual(@as(u8, 0x40), info.noise_hint);
    try std.testing.expectEqual(@as(u8, 0x80), info.stereo_hint);
    try std.testing.expectEqual(@as(u8, 0x44), info.harmonic_hint);
}

test "aac access unit trailing info prefers latest ps payload structure" {
    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x12, 0x20, 0x11, 0x08, 0x01 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x90, 0x40, 0x80, 0x44, 0x22 });

    const info = try accessUnitsTrailingInfoAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    try std.testing.expectEqual(@as(bool, true), info.saw_ps_payload);
    try std.testing.expectEqual(@as(u16, 6), info.ps_max_payload_len);
    try std.testing.expectEqual(@as(u8, 0x40), info.ps_noise_hint);
    try std.testing.expectEqual(@as(u8, 0x80), info.ps_stereo_hint);
    try std.testing.expectEqual(@as(u8, 0x44), info.ps_harmonic_hint);
    try std.testing.expectEqual(@as(u8, 0x44), info.ps_detail_hint);
}

test "aac access unit trailing info keeps latest plain sbr across later ps-only access unit" {
    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x90, 0x40, 0x80, 0x44, 0x22 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x12, 0x20, 0x11, 0x08, 0x01 });

    const info = try accessUnitsTrailingInfoAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    try std.testing.expectEqual(@as(bool, true), info.saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, true), info.saw_plain_sbr_payload);
    try std.testing.expectEqual(@as(u16, 6), info.max_payload_len);
    try std.testing.expectEqual(@as(u8, 0x90), info.envelope_hint);
    try std.testing.expectEqual(@as(u8, 0x40), info.noise_hint);
    try std.testing.expectEqual(@as(u8, 0x80), info.stereo_hint);
    try std.testing.expectEqual(@as(u8, 0x44), info.harmonic_hint);
    try std.testing.expectEqual(@as(u8, 0x44), info.detail_hint);
    try std.testing.expectEqual(@as(bool, true), info.saw_ps_payload);
    try std.testing.expectEqual(@as(u8, 0x20), info.ps_noise_hint);
    try std.testing.expectEqual(@as(u8, 0x11), info.ps_stereo_hint);
}

test "aac sync-extension sbr carries forward last enhancement payload across access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var enhanced_builder = TestBitBuilder.init();
    defer enhanced_builder.deinit(std.testing.allocator);
    try enhanced_builder.appendNonZeroMonoSce(std.testing.allocator);
    try enhanced_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x80, 0x40, 0x20, 0x10 });

    var plain_builder = TestBitBuilder.init();
    defer plain_builder.deinit(std.testing.allocator);
    try plain_builder.appendNonZeroMonoSce(std.testing.allocator);

    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ enhanced_builder.bytes.items, plain_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    try std.testing.expectEqual(@as(bool, true), raw_infos[0].saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, false), raw_infos[0].saw_ps_payload);
    try std.testing.expectEqual(@as(bool, false), raw_infos[1].saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, false), raw_infos[1].saw_ps_payload);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[1].saw_sbr_payload);
    try std.testing.expectEqual(@as(u8, 1), resolved_infos[1].sbr_carry_generations);
    try std.testing.expectEqual(@as(bool, false), resolved_infos[1].saw_ps_payload);
    try std.testing.expectEqual(resolved_infos[0].payload_hash, resolved_infos[1].payload_hash);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{ enhanced_builder.bytes.items, plain_builder.bytes.items },
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 4096), decoded.samples.len);
}

test "aac sync-extension sbr carried enhancement decays across repeated no-fill access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xf0, 0xc0, 0x80, 0x60 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);

    var third_builder = TestBitBuilder.init();
    defer third_builder.deinit(std.testing.allocator);
    try third_builder.appendNonZeroMonoSce(std.testing.allocator);

    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items, third_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(u8, 0), resolved_infos[0].sbr_carry_generations);
    try std.testing.expectEqual(@as(u8, 1), resolved_infos[1].sbr_carry_generations);
    try std.testing.expectEqual(@as(u8, 2), resolved_infos[2].sbr_carry_generations);

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{ first_builder.bytes.items, second_builder.bytes.items, third_builder.bytes.items },
    );
    defer decoded.deinit();

    const first_block = decoded.samples[0..2048];
    const second_block = decoded.samples[2048..4096];
    const third_block = decoded.samples[4096..6144];

    var first_delta: f32 = 0;
    var second_delta: f32 = 0;
    var third_delta: f32 = 0;
    for (0..1024) |i| {
        const base = i * 2;
        first_delta += @abs(first_block[base + 1] - first_block[base]);
        second_delta += @abs(second_block[base + 1] - second_block[base]);
        third_delta += @abs(third_block[base + 1] - third_block[base]);
    }
    try std.testing.expect(first_delta > second_delta);
    try std.testing.expect(second_delta > third_delta);
}

test "aac sync-extension ps carries forward ps payload across later sbr-only access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x20, 0xf0, 0x40 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x90, 0x80, 0x10, 0x08 });

    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    try std.testing.expectEqual(@as(bool, true), raw_infos[0].saw_ps_payload);
    try std.testing.expectEqual(@as(bool, false), raw_infos[1].saw_ps_payload);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(bool, true), resolved_infos[1].saw_ps_payload);
    try std.testing.expectEqual(resolved_infos[0].ps_payload_hash, resolved_infos[1].ps_payload_hash);
    try std.testing.expect(resolved_infos[0].payload_hash != resolved_infos[1].payload_hash);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 4096 * 2), decoded.samples.len);

    const second_block = decoded.samples[4096..8192];
    var second_side_energy: f32 = 0;
    for (0..second_block.len / 2) |frame_index| {
        const side = second_block[frame_index * 2] - second_block[frame_index * 2 + 1];
        second_side_energy += side * side;
    }
    try std.testing.expect(second_side_energy > 0);
}

test "aac sync-extension sbr carries forward last plain sbr payload across later ps-only access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x80, 0x40, 0x20, 0x10 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x20, 0xf0, 0x40 });

    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    try std.testing.expectEqual(@as(bool, true), raw_infos[0].saw_plain_sbr_payload);
    try std.testing.expectEqual(@as(bool, false), raw_infos[1].saw_plain_sbr_payload);
    try std.testing.expectEqual(@as(bool, true), raw_infos[1].saw_ps_payload);

    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(u8, 1), resolved_infos[1].sbr_carry_generations);
    try std.testing.expectEqual(@as(u8, 0), resolved_infos[1].ps_carry_generations);
    try std.testing.expectEqual(resolved_infos[0].payload_hash, resolved_infos[1].payload_hash);
    try std.testing.expectEqual(raw_infos[1].ps_payload_hash, resolved_infos[1].ps_payload_hash);
}

test "aac sync-extension ps-only refresh keeps carried sbr shaping profile" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_a_builder = TestBitBuilder.init();
    defer first_a_builder.deinit(std.testing.allocator);
    try first_a_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_a_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x10, 0x08 });

    var first_b_builder = TestBitBuilder.init();
    defer first_b_builder.deinit(std.testing.allocator);
    try first_b_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_b_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xe0, 0xc0, 0xf0, 0x80 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x20, 0xf0, 0x40 });

    var decoded_a = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{ first_a_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded_a.deinit();
    var decoded_b = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{ first_b_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded_b.deinit();

    const second_block_a = decoded_a.samples[4096..8192];
    const second_block_b = decoded_b.samples[4096..8192];
    var saw_difference = false;
    for (second_block_a, second_block_b) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac sync-extension sbr refresh keeps prior unrefreshed subfields across access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x30, 0x50, 0x70, 0x44, 0x22 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xf0 });

    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(u8, 0), resolved_infos[1].sbr_carry_generations);
    try std.testing.expectEqual(raw_infos[1].payload_hash, resolved_infos[1].payload_hash);
    try std.testing.expectEqual(raw_infos[1].envelope_hint, resolved_infos[1].envelope_hint);
    try std.testing.expectEqual(raw_infos[0].noise_hint, resolved_infos[1].noise_hint);
    try std.testing.expectEqual(raw_infos[0].stereo_hint, resolved_infos[1].stereo_hint);
    try std.testing.expectEqual(raw_infos[0].detail_hint, resolved_infos[1].detail_hint);
    try std.testing.expectEqual(raw_infos[0].phase_hint, resolved_infos[1].phase_hint);
}

test "aac sync-extension ps refresh keeps prior unrefreshed ps subfields across access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x50, 0x70, 0x44, 0x22 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0xf0, 0x66 });

    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(u8, 0), resolved_infos[1].ps_carry_generations);
    try std.testing.expectEqual(raw_infos[1].ps_payload_hash, resolved_infos[1].ps_payload_hash);
    try std.testing.expectEqual(raw_infos[1].ps_noise_hint, resolved_infos[1].ps_noise_hint);
    try std.testing.expectEqual(raw_infos[0].ps_stereo_hint, resolved_infos[1].ps_stereo_hint);
    try std.testing.expectEqual(raw_infos[0].ps_detail_hint, resolved_infos[1].ps_detail_hint);
    try std.testing.expectEqual(raw_infos[0].ps_phase_hint, resolved_infos[1].ps_phase_hint);
}

test "aac sync-extension ps carried stereoization decays across repeated sbr-only access units" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x20, 0xf0, 0x40 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x90, 0x80, 0x10, 0x08 });

    var third_builder = TestBitBuilder.init();
    defer third_builder.deinit(std.testing.allocator);
    try third_builder.appendNonZeroMonoSce(std.testing.allocator);
    try third_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x50, 0x40, 0x08, 0x04 });

    const raw_infos = try accessUnitsTrailingInfosAlloc(
        std.testing.allocator,
        16000,
        &.{ first_builder.bytes.items, second_builder.bytes.items, third_builder.bytes.items },
    );
    defer std.testing.allocator.free(raw_infos);
    const config = try parseAudioSpecificConfig(config_builder.bytes.items);
    const resolved_infos = try resolveEnhancementTrailingInfosAlloc(std.testing.allocator, config, raw_infos);
    defer std.testing.allocator.free(resolved_infos);
    try std.testing.expectEqual(@as(u8, 0), resolved_infos[0].ps_carry_generations);
    try std.testing.expectEqual(@as(u8, 1), resolved_infos[1].ps_carry_generations);
    try std.testing.expectEqual(@as(u8, 2), resolved_infos[2].ps_carry_generations);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{ first_builder.bytes.items, second_builder.bytes.items, third_builder.bytes.items },
    );
    defer decoded.deinit();

    const first_block = decoded.samples[0..4096];
    const second_block = decoded.samples[4096..8192];
    const third_block = decoded.samples[8192..12288];
    var first_side_energy: f32 = 0;
    var second_side_energy: f32 = 0;
    var third_side_energy: f32 = 0;
    for (0..first_block.len / 2) |frame_index| {
        const first_side = first_block[frame_index * 2] - first_block[frame_index * 2 + 1];
        const second_side = second_block[frame_index * 2] - second_block[frame_index * 2 + 1];
        const third_side = third_block[frame_index * 2] - third_block[frame_index * 2 + 1];
        first_side_energy += first_side * first_side;
        second_side_energy += second_side * second_side;
        third_side_energy += third_side * third_side;
    }
    try std.testing.expect(first_side_energy > second_side_energy);
    try std.testing.expect(second_side_energy > third_side_energy);
}

test "aac trailing info scans past leading non-sbr fill to later sbr fill" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0x10, 0x01, 0x02, 0x03, 0x04 });
    try builder.appendNonZeroMonoSce(std.testing.allocator);
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x80, 0x40, 0x20, 0x10 });

    var pure_builder = TestBitBuilder.init();
    defer pure_builder.deinit(std.testing.allocator);
    try pure_builder.appendNonZeroMonoSce(std.testing.allocator);
    try pure_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x80, 0x40, 0x20, 0x10 });

    const pure_sbr = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, pure_builder.bytes.items);
    const info = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, builder.bytes.items);
    try std.testing.expectEqual(@as(bool, true), info.saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, false), info.saw_ps_payload);
    try std.testing.expectEqual(@as(u16, 5), info.max_payload_len);
    try std.testing.expectEqual(@as(u8, 0x80), info.envelope_hint);
    try std.testing.expectEqual(@as(u8, 0x40), info.noise_hint);
    try std.testing.expectEqual(@as(u8, 0x20), info.stereo_hint);
    try std.testing.expectEqual(@as(u8, 0x10), info.harmonic_hint);
    try std.testing.expectEqual(pure_sbr.payload_hash, info.payload_hash);
}

test "aac sync-extension sbr decode honors trailing fill after leading non-sbr fill" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0x10, 0x01, 0x02, 0x03, 0x04 });
    try unit_builder.appendNonZeroMonoSce(std.testing.allocator);
    try unit_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x80, 0x40, 0x20, 0x10 });

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 2048), decoded.samples.len);
}

test "aac trailing info scans stereo sce pair with trailing sbr fill" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendSilentMonoSceWithTag(std.testing.allocator, 0);
    try builder.appendSilentMonoSceWithTag(std.testing.allocator, 1);
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x70, 0x30, 0x20, 0x10 });

    const info = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 44100, builder.bytes.items);
    try std.testing.expectEqual(@as(bool, true), info.saw_sbr_payload);
    try std.testing.expectEqual(@as(bool, false), info.saw_ps_payload);
    try std.testing.expectEqual(@as(u8, 0x70), info.envelope_hint);
    try std.testing.expectEqual(@as(u8, 0x30), info.noise_hint);
    try std.testing.expectEqual(@as(u8, 0x20), info.stereo_hint);
    try std.testing.expectEqual(@as(u8, 0x10), info.harmonic_hint);
}

test "aac trailing info prefers latest sbr fill in same access unit" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendNonZeroMonoSce(std.testing.allocator);
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xf0, 0xc0, 0xe0, 0xaa, 0xbb });
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x08, 0x11, 0x22 });

    var latest_builder = TestBitBuilder.init();
    defer latest_builder.deinit(std.testing.allocator);
    try latest_builder.appendNonZeroMonoSce(std.testing.allocator);
    try latest_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x08, 0x11, 0x22 });

    const info = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, builder.bytes.items);
    const latest = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, latest_builder.bytes.items);
    try std.testing.expectEqual(latest.max_payload_len, info.max_payload_len);
    try std.testing.expectEqual(latest.payload_hash, info.payload_hash);
    try std.testing.expectEqual(latest.envelope_hint, info.envelope_hint);
    try std.testing.expectEqual(latest.noise_hint, info.noise_hint);
    try std.testing.expectEqual(latest.stereo_hint, info.stereo_hint);
    try std.testing.expectEqual(latest.harmonic_hint, info.harmonic_hint);
    try std.testing.expectEqual(latest.detail_hint, info.detail_hint);
    try std.testing.expectEqual(latest.phase_hint, info.phase_hint);
}

test "aac trailing info prefers latest ps fill in same access unit" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendNonZeroMonoSce(std.testing.allocator);
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x20, 0xf0, 0x40, 0x10 });
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x70, 0x60, 0x10, 0x22, 0x33 });

    var latest_builder = TestBitBuilder.init();
    defer latest_builder.deinit(std.testing.allocator);
    try latest_builder.appendNonZeroMonoSce(std.testing.allocator);
    try latest_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x70, 0x60, 0x10, 0x22, 0x33 });

    const info = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, builder.bytes.items);
    const latest = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, latest_builder.bytes.items);
    try std.testing.expectEqual(latest.payload_hash, info.payload_hash);
    try std.testing.expectEqual(latest.ps_payload_hash, info.ps_payload_hash);
    try std.testing.expectEqual(latest.ps_noise_hint, info.ps_noise_hint);
    try std.testing.expectEqual(latest.ps_stereo_hint, info.ps_stereo_hint);
    try std.testing.expectEqual(latest.ps_harmonic_hint, info.ps_harmonic_hint);
    try std.testing.expectEqual(latest.ps_detail_hint, info.ps_detail_hint);
    try std.testing.expectEqual(latest.ps_phase_hint, info.ps_phase_hint);
}

test "aac trailing info preserves prior sbr subfields on shorter latest same access unit fill" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendNonZeroMonoSce(std.testing.allocator);
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x30, 0x50, 0x70, 0x44, 0x22 });
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xf0 });

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x30, 0x50, 0x70, 0x44, 0x22 });

    var latest_builder = TestBitBuilder.init();
    defer latest_builder.deinit(std.testing.allocator);
    try latest_builder.appendNonZeroMonoSce(std.testing.allocator);
    try latest_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xf0 });

    const info = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, builder.bytes.items);
    const first = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, first_builder.bytes.items);
    const latest = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, latest_builder.bytes.items);
    try std.testing.expectEqual(latest.max_payload_len, info.max_payload_len);
    try std.testing.expectEqual(latest.payload_hash, info.payload_hash);
    try std.testing.expectEqual(latest.envelope_hint, info.envelope_hint);
    try std.testing.expectEqual(first.noise_hint, info.noise_hint);
    try std.testing.expectEqual(first.stereo_hint, info.stereo_hint);
    try std.testing.expectEqual(first.harmonic_hint, info.harmonic_hint);
    try std.testing.expectEqual(first.detail_hint, info.detail_hint);
    try std.testing.expectEqual(first.phase_hint, info.phase_hint);
}

test "aac trailing info preserves prior ps subfields on shorter latest same access unit fill" {
    var builder = TestBitBuilder.init();
    defer builder.deinit(std.testing.allocator);
    try builder.appendNonZeroMonoSce(std.testing.allocator);
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x50, 0x70, 0x44, 0x22 });
    try builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0xf0, 0x66 });

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x50, 0x70, 0x44, 0x22 });

    var latest_builder = TestBitBuilder.init();
    defer latest_builder.deinit(std.testing.allocator);
    try latest_builder.appendNonZeroMonoSce(std.testing.allocator);
    try latest_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0xf0, 0x66 });

    const info = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, builder.bytes.items);
    const first = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, first_builder.bytes.items);
    const latest = try scanAccessUnitTrailingInfoAlloc(std.testing.allocator, 16000, latest_builder.bytes.items);
    try std.testing.expectEqual(latest.ps_payload_hash, info.ps_payload_hash);
    try std.testing.expectEqual(latest.ps_noise_hint, info.ps_noise_hint);
    try std.testing.expectEqual(first.ps_stereo_hint, info.ps_stereo_hint);
    try std.testing.expectEqual(first.ps_harmonic_hint, info.ps_harmonic_hint);
    try std.testing.expectEqual(first.ps_detail_hint, info.ps_detail_hint);
    try std.testing.expectEqual(first.ps_phase_hint, info.ps_phase_hint);
}

test "aac sync-extension sbr decode prefers latest fill in same access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var mixed_builder = TestBitBuilder.init();
    defer mixed_builder.deinit(std.testing.allocator);
    try mixed_builder.appendNonZeroMonoSce(std.testing.allocator);
    try mixed_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xf0, 0xc0, 0xe0, 0xaa, 0xbb });
    try mixed_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x08, 0x11, 0x22 });

    var latest_builder = TestBitBuilder.init();
    defer latest_builder.deinit(std.testing.allocator);
    try latest_builder.appendNonZeroMonoSce(std.testing.allocator);
    try latest_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x08, 0x11, 0x22 });

    var decoded_mixed = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{mixed_builder.bytes.items},
    );
    defer decoded_mixed.deinit();
    var decoded_latest = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{latest_builder.bytes.items},
    );
    defer decoded_latest.deinit();

    try std.testing.expectEqual(decoded_latest.sample_rate, decoded_mixed.sample_rate);
    try std.testing.expectEqual(decoded_latest.samples.len, decoded_mixed.samples.len);
    for (decoded_latest.samples, decoded_mixed.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
    }
}

test "aac sync-extension sbr stereo sce pair decodes with trailing fill" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 4, 4); // 44.1 kHz core
    try config_builder.appendBits(std.testing.allocator, 2, 4); // stereo
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 3, 4); // 48 kHz extension sample rate

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendSilentMonoSceWithTag(std.testing.allocator, 0);
    try unit_builder.appendSilentMonoSceWithTag(std.testing.allocator, 1);
    try unit_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x70, 0x30, 0x20, 0x10 });

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        44100,
        2,
        config_builder.bytes.items,
        &.{unit_builder.bytes.items},
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 2048 * 2), decoded.samples.len);
}

test "aac sync-extension sbr enhancement varies with payload structure" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var unit_a_builder = TestBitBuilder.init();
    defer unit_a_builder.deinit(std.testing.allocator);
    try unit_a_builder.appendNonZeroMonoSce(std.testing.allocator);
    try unit_a_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x10, 0x08 });

    var unit_b_builder = TestBitBuilder.init();
    defer unit_b_builder.deinit(std.testing.allocator);
    try unit_b_builder.appendNonZeroMonoSce(std.testing.allocator);
    try unit_b_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xe0, 0xc0, 0xf0, 0x80 });

    var decoded_a = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{unit_a_builder.bytes.items},
    );
    defer decoded_a.deinit();
    var decoded_b = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{unit_b_builder.bytes.items},
    );
    defer decoded_b.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded_a.sample_rate);
    try std.testing.expectEqual(decoded_a.sample_rate, decoded_b.sample_rate);
    var saw_difference = false;
    for (decoded_a.samples, decoded_b.samples) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac sync-extension sbr decode preserves prior subfields on shorter latest fill" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_a_builder = TestBitBuilder.init();
    defer first_a_builder.deinit(std.testing.allocator);
    try first_a_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_a_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x08, 0x11, 0x22 });
    try first_a_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xe0 });

    var first_b_builder = TestBitBuilder.init();
    defer first_b_builder.deinit(std.testing.allocator);
    try first_b_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_b_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x90, 0xf0, 0xaa, 0xbb });
    try first_b_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xe0 });

    var decoded_a = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{first_a_builder.bytes.items},
    );
    defer decoded_a.deinit();
    var decoded_b = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{first_b_builder.bytes.items},
    );
    defer decoded_b.deinit();

    var saw_difference = false;
    for (decoded_a.samples, decoded_b.samples) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac sync-extension sbr decode preserves prior subfields on shorter later access unit fill" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_a_builder = TestBitBuilder.init();
    defer first_a_builder.deinit(std.testing.allocator);
    try first_a_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_a_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x10, 0x08, 0x11, 0x22 });

    var first_b_builder = TestBitBuilder.init();
    defer first_b_builder.deinit(std.testing.allocator);
    try first_b_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_b_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x20, 0x90, 0xf0, 0xaa, 0xbb });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xe0 });

    var decoded_a = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{ first_a_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded_a.deinit();
    var decoded_b = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{ first_b_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded_b.deinit();

    const second_block_a = decoded_a.samples[2048..4096];
    const second_block_b = decoded_b.samples[2048..4096];
    var saw_difference = false;
    for (second_block_a, second_block_b) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac sync-extension ps decode preserves prior subfields on shorter later access unit fill" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_a_builder = TestBitBuilder.init();
    defer first_a_builder.deinit(std.testing.allocator);
    try first_a_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_a_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x10, 0x10, 0x04, 0x12 });

    var first_b_builder = TestBitBuilder.init();
    defer first_b_builder.deinit(std.testing.allocator);
    try first_b_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_b_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x10, 0xf0, 0x40, 0x34 });

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0xf0, 0x66 });

    var decoded_a = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{ first_a_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded_a.deinit();
    var decoded_b = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{ first_b_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded_b.deinit();

    const second_block_a = decoded_a.samples[4096..8192];
    const second_block_b = decoded_b.samples[4096..8192];
    var saw_difference = false;
    for (second_block_a, second_block_b) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac sync-extension sbr enhancement is applied per access unit" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 5, 5); // SBR extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_unit_builder = TestBitBuilder.init();
    defer first_unit_builder.deinit(std.testing.allocator);
    try first_unit_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_unit_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0x10, 0x10, 0x10, 0x04 });

    var second_unit_builder = TestBitBuilder.init();
    defer second_unit_builder.deinit(std.testing.allocator);
    try second_unit_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_unit_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xd0, 0xf0, 0xc0, 0x80, 0x60 });

    var decoded = try decodeInterleavedMonoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        1,
        config_builder.bytes.items,
        &.{ first_unit_builder.bytes.items, second_unit_builder.bytes.items },
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 4096), decoded.samples.len);

    const first_block = decoded.samples[0..2048];
    const second_block = decoded.samples[2048..4096];
    var saw_difference = false;
    for (first_block, second_block) |lhs, rhs| {
        if (@abs(lhs - rhs) > 1e-6) {
            saw_difference = true;
            break;
        }
    }
    try std.testing.expect(saw_difference);
}

test "aac ps stereoization varies with payload structure" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var narrow_builder = TestBitBuilder.init();
    defer narrow_builder.deinit(std.testing.allocator);
    try narrow_builder.appendNonZeroMonoSce(std.testing.allocator);
    try narrow_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x10, 0x10, 0x04 });

    var wide_builder = TestBitBuilder.init();
    defer wide_builder.deinit(std.testing.allocator);
    try wide_builder.appendNonZeroMonoSce(std.testing.allocator);
    try wide_builder.appendSyntheticEnhancementFillElement(std.testing.allocator, &.{ 0xe0, 0x30, 0x10, 0xf0, 0x40 });

    var decoded_narrow = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{narrow_builder.bytes.items},
    );
    defer decoded_narrow.deinit();
    var decoded_wide = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        16000,
        2,
        config_builder.bytes.items,
        &.{wide_builder.bytes.items},
    );
    defer decoded_wide.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded_narrow.sample_rate);
    try std.testing.expectEqual(decoded_narrow.sample_rate, decoded_wide.sample_rate);

    var narrow_side_energy: f32 = 0;
    var wide_side_energy: f32 = 0;
    for (0..decoded_narrow.samples.len / 2) |frame_index| {
        const narrow_side = decoded_narrow.samples[frame_index * 2] - decoded_narrow.samples[frame_index * 2 + 1];
        const wide_side = decoded_wide.samples[frame_index * 2] - decoded_wide.samples[frame_index * 2 + 1];
        narrow_side_energy += narrow_side * narrow_side;
        wide_side_energy += wide_side * wide_side;
    }
    try std.testing.expect(@abs(wide_side_energy - narrow_side_energy) > 1e-4);
}

test "aac sync-extension ps mono-core stereo output rejects sbr-only fill payload" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono core
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var unit_builder = TestBitBuilder.init();
    defer unit_builder.deinit(std.testing.allocator);
    try unit_builder.appendNonZeroMonoSce(std.testing.allocator);
    try unit_builder.appendSyntheticSbrFillElement(std.testing.allocator);

    try std.testing.expectError(
        error.UnsupportedAudioFormat,
        decodeInterleavedStereoAccessUnitsAlloc(
            std.testing.allocator,
            32000,
            2,
            config_builder.bytes.items,
            &.{unit_builder.bytes.items},
        ),
    );
}

test "aac sync-extension ps mono-core stereo output tolerates delayed first ps payload" {
    var config_builder = TestBitBuilder.init();
    defer config_builder.deinit(std.testing.allocator);
    try config_builder.appendBits(std.testing.allocator, 2, 5); // AAC-LC
    try config_builder.appendBits(std.testing.allocator, 8, 4); // 16 kHz core
    try config_builder.appendBits(std.testing.allocator, 1, 4); // mono core
    try config_builder.appendBits(std.testing.allocator, 0, 1); // frameLengthFlag
    try config_builder.appendBits(std.testing.allocator, 0, 1); // dependsOnCoreCoder
    try config_builder.appendBits(std.testing.allocator, 0, 1); // extensionFlag
    try config_builder.appendBits(std.testing.allocator, 0x2b7, 11); // syncExtensionType
    try config_builder.appendBits(std.testing.allocator, 29, 5); // PS extension object type
    try config_builder.appendBits(std.testing.allocator, 5, 4); // 32 kHz extension sample rate

    var first_builder = TestBitBuilder.init();
    defer first_builder.deinit(std.testing.allocator);
    try first_builder.appendNonZeroMonoSce(std.testing.allocator);
    try first_builder.appendSyntheticSbrFillElement(std.testing.allocator);

    var second_builder = TestBitBuilder.init();
    defer second_builder.deinit(std.testing.allocator);
    try second_builder.appendNonZeroMonoSce(std.testing.allocator);
    try second_builder.appendSyntheticPsFillElement(std.testing.allocator);

    var decoded = try decodeInterleavedStereoAccessUnitsAlloc(
        std.testing.allocator,
        32000,
        2,
        config_builder.bytes.items,
        &.{ first_builder.bytes.items, second_builder.bytes.items },
    );
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 32000), decoded.sample_rate);
    try std.testing.expectEqual(@as(usize, 2), decoded.frame_count);
    try std.testing.expectEqual(@as(usize, 4096 * 2), decoded.samples.len);

    const first_block = decoded.samples[0..4096];
    const second_block = decoded.samples[4096..8192];
    var first_side_energy: f32 = 0;
    var second_side_energy: f32 = 0;
    for (0..first_block.len / 2) |frame_index| {
        const first_side = first_block[frame_index * 2] - first_block[frame_index * 2 + 1];
        const second_side = second_block[frame_index * 2] - second_block[frame_index * 2 + 1];
        first_side_energy += first_side * first_side;
        second_side_energy += second_side * second_side;
    }
    try std.testing.expect(first_side_energy < 1e-6);
    try std.testing.expect(second_side_energy > 0);
}

test "aac main prediction carries state and honors reset groups" {
    var prediction_used = [_]bool{false} ** max_prediction_bands;
    prediction_used[0] = true;

    const base_ics_info: IcsInfo = .{
        .window_sequence = .only_long,
        .window_shape = 0,
        .max_sfb = 1,
        .num_window_groups = 1,
        .window_group_length = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
        .predictor_data_present = true,
        .prediction_used = prediction_used,
    };

    var predictor_states = [_]PredictorState{.{}} ** max_predictors;
    resetAllPredictors(&predictor_states);

    var first = [_]f32{0} ** 1024;
    first[0] = 0.25;
    try applyMainPrediction(&first, base_ics_info, &.{ 0, 4 }, 44100, &predictor_states);

    var second = [_]f32{0} ** 1024;
    try applyMainPrediction(&second, base_ics_info, &.{ 0, 4 }, 44100, &predictor_states);
    try std.testing.expect(@abs(second[0]) > 1e-6);

    var reset_ics_info = base_ics_info;
    reset_ics_info.predictor_reset_group = 1;
    var third = [_]f32{0} ** 1024;
    try applyMainPrediction(&third, reset_ics_info, &.{ 0, 4 }, 44100, &predictor_states);
    try std.testing.expect(@abs(third[0]) > 1e-6);

    var fourth = [_]f32{0} ** 1024;
    try applyMainPrediction(&fourth, base_ics_info, &.{ 0, 4 }, 44100, &predictor_states);
    try std.testing.expectApproxEqAbs(@as(f32, 0), fourth[0], 1e-6);
}

test "aac tns tool accepts zeroed long-window coefficients" {
    var coefficients = [_]f32{0} ** 4;
    const plans = [_]SpectralBandLayout{.{
        .band_type = 5,
        .class = .pair,
        .dimensions = 2,
        .unsigned_values = true,
        .uses_escape = false,
        .scalefactor_kind = .spectral,
        .scalefactor_value = 100,
        .coeff_start = 0,
        .coeff_end = 4,
        .symbol_count = 2,
    }};
    var tns = TnsData{ .num_windows = 1 };
    tns.windows[0].n_filt = 1;
    tns.windows[0].coef_res = 0;
    tns.windows[0].filters[0] = .{
        .length = 1,
        .order = 1,
        .direction = false,
        .coef_compress = false,
        .coef_len = 3,
        .coefficients = .{ 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    };

    try applyTnsTool(
        &coefficients,
        .{
            .window_sequence = .only_long,
            .window_shape = 0,
            .max_sfb = 1,
            .num_window_groups = 1,
            .window_group_length = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .predictor_data_present = null,
        },
        &plans,
        &.{ 0, 4 },
        tns,
    );
    for (coefficients) |coefficient| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), coefficient, 1e-6);
    }
}

test "aac tns tool accepts zeroed grouped short-window coefficients" {
    var coefficients = [_]f32{0} ** 32;
    const coeff_offsets = [_]u16{ 0, 16, 32 };
    var tns = TnsData{ .num_windows = 8 };
    tns.windows[0].n_filt = 1;
    tns.windows[0].coef_res = 0;
    tns.windows[0].filters[0] = .{
        .length = 2,
        .order = 1,
        .direction = false,
        .coef_compress = false,
        .coef_len = 3,
        .coefficients = .{ 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    };

    try applyTnsToolWithOffsets(
        &coefficients,
        .{
            .window_sequence = .eight_short,
            .window_shape = 0,
            .max_sfb = 2,
            .num_window_groups = 1,
            .window_group_length = .{ 8, 0, 0, 0, 0, 0, 0, 0 },
            .predictor_data_present = null,
        },
        &coeff_offsets,
        &.{ 0, 2, 4 },
        tns,
    );
    for (coefficients) |coefficient| {
        try std.testing.expectApproxEqAbs(@as(f32, 0), coefficient, 1e-6);
    }
}

test "checked-in mono pns aac fixture decodes through pure zig path" {
    const frames = try scanAdtsFramesAlloc(std.testing.allocator, transient_aac_44k_pns_bytes);
    defer std.testing.allocator.free(frames);
    try std.testing.expect(frames.len > 0);

    var saw_noise = false;
    for (frames) |frame| {
        var state = try initFirstChannelSpectralStateAlloc(std.testing.allocator, frame.header.sample_rate, frame.payload);
        defer state.deinit();
        saw_noise = saw_noise or containsBandKind(state.bands, .noise);
    }
    try std.testing.expect(saw_noise);

    var decoded = try decodeInterleavedMonoAdtsAlloc(std.testing.allocator, transient_aac_44k_pns_bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 44100), decoded.sample_rate);
    try std.testing.expect(decoded.samples.len > 0);
}
