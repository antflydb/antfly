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
const silk_tables = @import("opus_silk_tables.zig");
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

pub const CeltHeader = struct {
    silence: bool,
    has_postfilter: bool,
    postfilter_pitch: u16,
    postfilter_gain: f32,
    postfilter_tapset: u8,
    is_transient: bool,
    intra_energy: bool,
};

pub const SilkChannelHeader = struct {
    vad_flags: [3]bool = @as([3]bool, @splat(false)),
    lbrr_flag: bool = false,
    lbrr_flags: [3]bool = @as([3]bool, @splat(false)),
};

pub const SilkPacketHeader = struct {
    mode: Mode,
    channels: u8,
    internal_frame_count: u8,
    subframes_per_internal_frame: u8,
    channel_headers: [2]SilkChannelHeader = .{ .{}, .{} },
    consumed_bits: usize,
    consumed_frac_bits: usize,
};

pub const SilkSignalType = enum(u2) {
    no_voice_activity = 0,
    unvoiced = 1,
    voiced = 2,
};

const SilkMaxInternalFrames = 3;

const SilkCondCoding = enum {
    independently,
    conditionally,
};

pub const SilkIndices = struct {
    signal_type: SilkSignalType = .no_voice_activity,
    quant_offset_type: u1 = 0,
    gains_indices: [4]u8 = @as([4]u8, @splat(0)),
    nlsf_indices: [17]i8 = @as([17]i8, @splat(0)),
    nlsf_interp_coef_q2: u8 = 4,
    lag_index: i16 = 0,
    contour_index: u8 = 0,
    per_index: u8 = 0,
    ltp_index: [4]u8 = @as([4]u8, @splat(0)),
    ltp_scale_index: u8 = 0,
    seed: u8 = 0,
};

pub const SilkFrameFront = struct {
    indices: SilkIndices = .{},
    pulses: [320]i16 = @as([320]i16, @splat(0)),
    pulse_len: usize = 0,
};

pub const SilkPacketFront = struct {
    header: SilkPacketHeader,
    stereo_pred_q13: [SilkMaxInternalFrames][2]i32 = @as([SilkMaxInternalFrames][2]i32, @splat(.{ 0, 0 })),
    decode_only_middle: [SilkMaxInternalFrames]bool = @as([SilkMaxInternalFrames]bool, @splat(false)),
    frame_present: [2][SilkMaxInternalFrames]bool = @as([2][SilkMaxInternalFrames]bool, @splat(@as([SilkMaxInternalFrames]bool, @splat(false)))),
    frames: [2][SilkMaxInternalFrames]SilkFrameFront =
        @as([2][SilkMaxInternalFrames]SilkFrameFront, @splat(@as([SilkMaxInternalFrames]SilkFrameFront, @splat(.{})))),
};

pub const SilkFrameParameters = struct {
    signal_type: SilkSignalType = .no_voice_activity,
    fs_khz: u8 = 0,
    subframe_count: u8 = 0,
    gains_q16: [4]i32 = @as([4]i32, @splat(0)),
    nlsf_q15: [16]i16 = @as([16]i16, @splat(0)),
    pred_coef_q12: [2][16]i16 = @as([2][16]i16, @splat(@as([16]i16, @splat(0)))),
    pitch_l: [4]i16 = @as([4]i16, @splat(0)),
    ltp_coef_q14: [20]i16 = @as([20]i16, @splat(0)),
    ltp_scale_q14: i16 = 0,
    quant_offset_q10: i16 = 0,
    excitation_q14: [320]i32 = @as([320]i32, @splat(0)),
    excitation_len: usize = 0,
};

pub const SilkPacketParameters = struct {
    sample_rate: u32 = 0,
    front: SilkPacketFront,
    frames: [2][SilkMaxInternalFrames]SilkFrameParameters =
        @as([2][SilkMaxInternalFrames]SilkFrameParameters, @splat(@as([SilkMaxInternalFrames]SilkFrameParameters, @splat(.{})))),
};

const SilkSynthesizedPacket = struct {
    samples: []f32,
    sample_rate: u32,
};

const SilkChannelState = struct {
    prev_gain_index: i32 = 10,
    prev_nlsf_q15: [16]i16 = @as([16]i16, @splat(0)),
    first_frame_after_reset: bool = true,
};

const SilkSynthChannelState = struct {
    prev_gain_q16: i32 = 65_536,
    s_lpc_q14_buf: [16]i32 = @as([16]i32, @splat(0)),
    history_q15: [320]i32 = @as([320]i32, @splat(0)),
};

const SilkStereoState = struct {
    pred_prev_q13: [2]i32 = .{ 0, 0 },
    s_mid: [2]i16 = .{ 0, 0 },
    s_side: [2]i16 = .{ 0, 0 },
};

const SilkSynthState = struct {
    channels: u8,
    channel: [2]SilkSynthChannelState = .{ .{}, .{} },
    stereo: SilkStereoState = .{},
};

pub const CeltCoarseEnergyFrame = struct {
    header: CeltHeader,
    start_band: usize,
    end_band: usize,
    channels: u8,
    band_energies: [2][21]f32,
};

pub const CeltEnergyState = struct {
    channels: u8,
    old_band_energies: [2][21]f32 = @as([2][21]f32, @splat(@as([21]f32, @splat(0)))),
};

pub const CeltResidualPlan = struct {
    tf_res: [21]i8 = @as([21]i8, @splat(0)),
    spread_decision: u8,
    alloc_trim: u8,
    coded_bands: usize,
    intensity: u8 = 0,
    dual_stereo: bool = false,
    band_bits: [21]i32 = @as([21]i32, @splat(0)),
    pulses: [21]i16 = @as([21]i16, @splat(0)),
    fine_quant: [21]u8 = @as([21]u8, @splat(0)),
    fine_priority: [21]bool = @as([21]bool, @splat(false)),
};

pub const CeltResidualBand = struct {
    coefficients: [176]f32 = @as([176]f32, @splat(0)),
    len: usize,
};

pub const CeltResidualFrame = struct {
    header: CeltHeader,
    start_band: usize,
    end_band: usize,
    channels: u8,
    plan: CeltResidualPlan,
    bands: [2][21]CeltResidualBand,
    band_energies: [2][21]f32,
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

const CeltSynthState = struct {
    overlap: [2][]f32,
    channels: u8,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, channels: u8, overlap_len: usize) !CeltSynthState {
        var overlap = [_][]f32{ &.{}, &.{} };
        for (0..channels) |channel| {
            overlap[channel] = try allocator.alloc(f32, overlap_len);
            @memset(overlap[channel], 0);
        }
        return .{
            .overlap = overlap,
            .channels = channels,
            .allocator = allocator,
        };
    }

    fn deinit(self: *CeltSynthState) void {
        for (0..self.channels) |channel| self.allocator.free(self.overlap[channel]);
    }
};

const CeltTransformPlan = struct {
    n: usize,
    basis: []f32,
    allocator: std.mem.Allocator,

    fn deinit(self: *CeltTransformPlan) void {
        self.allocator.free(self.basis);
        self.* = undefined;
    }
};

const CeltSynthesisPlan = struct {
    window: []f32,
    dct4_plans: [4]CeltTransformPlan,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) !CeltSynthesisPlan {
        var plans: [4]CeltTransformPlan = undefined;
        var built_count: usize = 0;
        errdefer {
            for (plans[0..built_count]) |*plan| {
                plan.deinit();
            }
        }

        const multipliers = [_]usize{ 1, 2, 4, 8 };
        for (multipliers, 0..) |m, i| {
            plans[i] = try buildCeltDct4PlanAlloc(allocator, standardShortMdctSize() * m);
            built_count += 1;
        }

        return .{
            .window = try celtBasicWindowAlloc(allocator, standardOverlap()),
            .dct4_plans = plans,
            .allocator = allocator,
        };
    }

    fn deinit(self: *CeltSynthesisPlan) void {
        self.allocator.free(self.window);
        for (&self.dct4_plans) |*plan| plan.deinit();
        self.* = undefined;
    }

    fn dct4PlanForLm(self: *const CeltSynthesisPlan, lm: u2) *const CeltTransformPlan {
        return &self.dct4_plans[lm];
    }
};

var shared_opus_celt_plan_lock: std.atomic.Mutex = .unlocked;
var shared_opus_celt_plan: ?CeltSynthesisPlan = null;

fn sharedCeltSynthesisPlan() !*const CeltSynthesisPlan {
    while (!shared_opus_celt_plan_lock.tryLock()) std.atomic.spinLoopHint();
    defer shared_opus_celt_plan_lock.unlock();

    if (shared_opus_celt_plan == null) {
        shared_opus_celt_plan = try CeltSynthesisPlan.init(std.heap.page_allocator);
    }
    return &(shared_opus_celt_plan.?);
}

fn buildCeltDct4PlanAlloc(allocator: std.mem.Allocator, n: usize) !CeltTransformPlan {
    if (n == 0) return error.UnsupportedAudioFormat;
    const basis = try allocator.alloc(f32, n * n);
    errdefer allocator.free(basis);

    const n_f = @as(f64, @floatFromInt(n));
    const scale = @sqrt(2.0 / n_f);
    const factor = std.math.pi / n_f;
    for (0..n) |n_idx| {
        const n_term = @as(f64, @floatFromInt(n_idx)) + 0.5;
        for (0..n) |k_idx| {
            const k_term = @as(f64, @floatFromInt(k_idx)) + 0.5;
            basis[n_idx * n + k_idx] = @floatCast(@cos(factor * n_term * k_term) * scale);
        }
    }

    return .{
        .n = n,
        .basis = basis,
        .allocator = allocator,
    };
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

fn dotProduct4Simd(a: []const f32, row0: []const f32, row1: []const f32, row2: []const f32, row3: []const f32) [4]f32 {
    std.debug.assert(a.len == row0.len);
    std.debug.assert(a.len == row1.len);
    std.debug.assert(a.len == row2.len);
    std.debug.assert(a.len == row3.len);

    var acc0: F32xN = @splat(0.0);
    var acc1: F32xN = @splat(0.0);
    var acc2: F32xN = @splat(0.0);
    var acc3: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= a.len) : (i += VEC_LEN) {
        const av: F32xN = a[i..][0..VEC_LEN].*;
        const bv0: F32xN = row0[i..][0..VEC_LEN].*;
        const bv1: F32xN = row1[i..][0..VEC_LEN].*;
        const bv2: F32xN = row2[i..][0..VEC_LEN].*;
        const bv3: F32xN = row3[i..][0..VEC_LEN].*;
        acc0 += av * bv0;
        acc1 += av * bv1;
        acc2 += av * bv2;
        acc3 += av * bv3;
    }

    var sums = [4]f32{
        @reduce(.Add, acc0),
        @reduce(.Add, acc1),
        @reduce(.Add, acc2),
        @reduce(.Add, acc3),
    };
    while (i < a.len) : (i += 1) {
        const sample = a[i];
        sums[0] += sample * row0[i];
        sums[1] += sample * row1[i];
        sums[2] += sample * row2[i];
        sums[3] += sample * row3[i];
    }
    return sums;
}

fn dotProduct8Simd(
    a: []const f32,
    row0: []const f32,
    row1: []const f32,
    row2: []const f32,
    row3: []const f32,
    row4: []const f32,
    row5: []const f32,
    row6: []const f32,
    row7: []const f32,
) [8]f32 {
    std.debug.assert(a.len == row0.len);
    std.debug.assert(a.len == row1.len);
    std.debug.assert(a.len == row2.len);
    std.debug.assert(a.len == row3.len);
    std.debug.assert(a.len == row4.len);
    std.debug.assert(a.len == row5.len);
    std.debug.assert(a.len == row6.len);
    std.debug.assert(a.len == row7.len);

    var acc0: F32xN = @splat(0.0);
    var acc1: F32xN = @splat(0.0);
    var acc2: F32xN = @splat(0.0);
    var acc3: F32xN = @splat(0.0);
    var acc4: F32xN = @splat(0.0);
    var acc5: F32xN = @splat(0.0);
    var acc6: F32xN = @splat(0.0);
    var acc7: F32xN = @splat(0.0);
    var i: usize = 0;
    while (i + VEC_LEN <= a.len) : (i += VEC_LEN) {
        const av: F32xN = a[i..][0..VEC_LEN].*;
        const bv0: F32xN = row0[i..][0..VEC_LEN].*;
        const bv1: F32xN = row1[i..][0..VEC_LEN].*;
        const bv2: F32xN = row2[i..][0..VEC_LEN].*;
        const bv3: F32xN = row3[i..][0..VEC_LEN].*;
        const bv4: F32xN = row4[i..][0..VEC_LEN].*;
        const bv5: F32xN = row5[i..][0..VEC_LEN].*;
        const bv6: F32xN = row6[i..][0..VEC_LEN].*;
        const bv7: F32xN = row7[i..][0..VEC_LEN].*;
        acc0 += av * bv0;
        acc1 += av * bv1;
        acc2 += av * bv2;
        acc3 += av * bv3;
        acc4 += av * bv4;
        acc5 += av * bv5;
        acc6 += av * bv6;
        acc7 += av * bv7;
    }

    var sums = [8]f32{
        @reduce(.Add, acc0),
        @reduce(.Add, acc1),
        @reduce(.Add, acc2),
        @reduce(.Add, acc3),
        @reduce(.Add, acc4),
        @reduce(.Add, acc5),
        @reduce(.Add, acc6),
        @reduce(.Add, acc7),
    };
    while (i < a.len) : (i += 1) {
        const sample = a[i];
        sums[0] += sample * row0[i];
        sums[1] += sample * row1[i];
        sums[2] += sample * row2[i];
        sums[3] += sample * row3[i];
        sums[4] += sample * row4[i];
        sums[5] += sample * row5[i];
        sums[6] += sample * row6[i];
        sums[7] += sample * row7[i];
    }
    return sums;
}

fn celtDct4IntoWithPlan(out: []f32, coefficients: []const f32, plan: *const CeltTransformPlan) !void {
    if (out.len == 0 or out.len != coefficients.len or out.len != plan.n) return error.UnsupportedAudioFormat;
    var row_index: usize = 0;
    while (row_index + 8 <= out.len) : (row_index += 8) {
        const row0 = plan.basis[(row_index + 0) * plan.n ..][0..plan.n];
        const row1 = plan.basis[(row_index + 1) * plan.n ..][0..plan.n];
        const row2 = plan.basis[(row_index + 2) * plan.n ..][0..plan.n];
        const row3 = plan.basis[(row_index + 3) * plan.n ..][0..plan.n];
        const row4 = plan.basis[(row_index + 4) * plan.n ..][0..plan.n];
        const row5 = plan.basis[(row_index + 5) * plan.n ..][0..plan.n];
        const row6 = plan.basis[(row_index + 6) * plan.n ..][0..plan.n];
        const row7 = plan.basis[(row_index + 7) * plan.n ..][0..plan.n];
        const sums = dotProduct8Simd(coefficients, row0, row1, row2, row3, row4, row5, row6, row7);
        out[row_index + 0] = sums[0];
        out[row_index + 1] = sums[1];
        out[row_index + 2] = sums[2];
        out[row_index + 3] = sums[3];
        out[row_index + 4] = sums[4];
        out[row_index + 5] = sums[5];
        out[row_index + 6] = sums[6];
        out[row_index + 7] = sums[7];
    }
    while (row_index + 4 <= out.len) : (row_index += 4) {
        const row0 = plan.basis[(row_index + 0) * plan.n ..][0..plan.n];
        const row1 = plan.basis[(row_index + 1) * plan.n ..][0..plan.n];
        const row2 = plan.basis[(row_index + 2) * plan.n ..][0..plan.n];
        const row3 = plan.basis[(row_index + 3) * plan.n ..][0..plan.n];
        const sums = dotProduct4Simd(coefficients, row0, row1, row2, row3);
        out[row_index + 0] = sums[0];
        out[row_index + 1] = sums[1];
        out[row_index + 2] = sums[2];
        out[row_index + 3] = sums[3];
    }
    while (row_index < out.len) : (row_index += 1) {
        const row = plan.basis[row_index * plan.n ..][0..plan.n];
        out[row_index] = dotProductSimd(coefficients, row);
    }
}

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
            .nbits_total = 33,
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

    pub fn decodeBitLogp(self: *RangeDecoder, logp: u5) !u1 {
        if (logp == 0) {
            try self.update(0, 1, 1);
            return 1;
        }
        const split = self.rng - (self.rng >> logp);
        if (self.val < split) {
            self.rng = split;
            self.normalize();
            return 0;
        }
        self.val -= split;
        self.rng -= split;
        if (self.rng == 0) return error.UnsupportedAudioFormat;
        self.normalize();
        return 1;
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
        var l = ilog(self.rng);
        const shift = l - 16;
        const r: u32 = if (shift >= 0) self.rng >> @intCast(shift) else self.rng << @intCast(-shift);
        var b: usize = (r >> 12) - 8;
        b += @intFromBool(r > correction[b]);
        l = (l << 3) + @as(u6, @intCast(b));
        return nbits - l;
    }

    pub fn decodeIcdf(self: *RangeDecoder, icdf: []const u8) !u8 {
        if (icdf.len == 0) return error.UnsupportedAudioFormat;
        const ft: u16 = 1 << 8;
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

const CeltSmallEnergyPmf = [_]u16{ 2, 1, 1 };
const CeltTrimIcdf = [_]u8{ 126, 124, 119, 109, 87, 41, 19, 9, 4, 2, 0 };
const CeltSpreadIcdf = [_]u8{ 25, 23, 2, 0 };
const CeltTapsetIcdf = [_]u8{ 2, 1, 0 };
const SilkLbrrFlags2Icdf = [_]u8{ 203, 150, 0 };
const SilkLbrrFlags3Icdf = [_]u8{ 215, 195, 166, 125, 110, 82, 0 };
const CeltTfSelectTable = [4][8]i8{
    .{ 0, -1, 0, -1, 0, -1, 0, -1 },
    .{ 0, -1, 0, -2, 1, 0, 1, -1 },
    .{ 0, -2, 0, -3, 2, 0, 1, -1 },
    .{ 0, -2, 0, -3, 3, 0, 1, -1 },
};
const CeltLog2FracTable = [_]u8{
    0,
    8,
    13,
    16,
    19,
    21,
    23,
    24,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    32,
    33,
    34,
    34,
    35,
    36,
    36,
    37,
    37,
};

const CeltProbabilityModel = struct {
    e_means: [21]f32 = .{
        6.4375, 6.25,   5.75,  5.3125, 5.0625,
        4.8125, 4.5,    4.375, 4.875,  4.6875,
        4.5625, 4.4375, 4.875, 4.625,  4.3125,
        4.5,    4.375,  4.625, 4.75,   4.4375,
        3.75,
    },
    pred_coef: [4]f32 = .{
        29440.0 / 32768.0,
        26112.0 / 32768.0,
        21248.0 / 32768.0,
        16384.0 / 32768.0,
    },
    beta_coef: [4]f32 = .{
        30147.0 / 32768.0,
        22282.0 / 32768.0,
        12124.0 / 32768.0,
        6554.0 / 32768.0,
    },
    beta_intra: f32 = 4915.0 / 32768.0,
    e_prob_model: [4][2][42]u8 = .{
        .{
            .{ 72, 127, 65, 129, 66, 128, 65, 128, 64, 128, 62, 128, 64, 128, 64, 128, 92, 78, 92, 79, 92, 78, 90, 79, 116, 41, 115, 40, 114, 40, 132, 26, 132, 26, 145, 17, 161, 12, 176, 10, 177, 11 },
            .{ 24, 179, 48, 138, 54, 135, 54, 132, 53, 134, 56, 133, 55, 132, 55, 132, 61, 114, 70, 96, 74, 88, 75, 88, 87, 74, 89, 66, 91, 67, 100, 59, 108, 50, 120, 40, 122, 37, 97, 43, 78, 50 },
        },
        .{
            .{ 83, 78, 84, 81, 88, 75, 86, 74, 87, 71, 90, 73, 93, 74, 93, 74, 109, 40, 114, 36, 117, 34, 117, 34, 143, 17, 145, 18, 146, 19, 162, 12, 165, 10, 178, 7, 189, 6, 190, 8, 177, 9 },
            .{ 23, 178, 54, 115, 63, 102, 66, 98, 69, 99, 74, 89, 71, 91, 73, 91, 78, 89, 86, 80, 92, 66, 93, 64, 102, 59, 103, 60, 104, 60, 117, 52, 123, 44, 138, 35, 133, 31, 97, 38, 77, 45 },
        },
        .{
            .{ 61, 90, 93, 60, 105, 42, 107, 41, 110, 45, 116, 38, 113, 38, 112, 38, 124, 26, 132, 27, 136, 19, 140, 20, 155, 14, 159, 16, 158, 18, 170, 13, 177, 10, 187, 8, 192, 6, 175, 9, 159, 10 },
            .{ 21, 178, 59, 110, 71, 86, 75, 85, 84, 83, 91, 66, 88, 73, 87, 72, 92, 75, 98, 72, 105, 58, 107, 54, 115, 52, 114, 55, 112, 56, 129, 51, 132, 40, 150, 33, 140, 29, 98, 35, 77, 42 },
        },
        .{
            .{ 42, 121, 96, 66, 108, 43, 111, 40, 117, 44, 123, 32, 120, 36, 119, 33, 127, 33, 134, 34, 139, 21, 147, 23, 152, 20, 158, 25, 154, 26, 166, 21, 173, 16, 184, 13, 184, 10, 150, 13, 139, 15 },
            .{ 22, 178, 63, 114, 74, 82, 84, 83, 92, 82, 103, 62, 96, 72, 96, 67, 101, 73, 107, 72, 113, 55, 118, 52, 125, 52, 118, 52, 117, 55, 135, 49, 137, 39, 157, 32, 145, 29, 97, 33, 77, 40 },
        },
    },
}{};

const StandardCeltMode = struct {
    const nb_ebands = 21;
    const nb_alloc_vectors = 11;
    const bitres = 3;
    const max_pseudo = 40;
    const max_fine_bits = 8;
    const fine_offset = 21;
    const qtheta_offset = 4;
    const qtheta_offset_two_phase = 16;

    const e_bands = [_]u16{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 34, 40, 48, 60, 78, 100 };
    const band_allocation = [_][21]u8{
        .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        .{ 90, 80, 75, 69, 63, 56, 49, 40, 34, 29, 20, 18, 10, 0, 0, 0, 0, 0, 0, 0, 0 },
        .{ 110, 100, 90, 84, 78, 71, 65, 58, 51, 45, 39, 32, 26, 20, 12, 0, 0, 0, 0, 0, 0 },
        .{ 118, 110, 103, 93, 86, 80, 75, 70, 65, 59, 53, 47, 40, 31, 23, 15, 4, 0, 0, 0, 0 },
        .{ 126, 119, 112, 104, 95, 89, 83, 78, 72, 66, 60, 54, 47, 39, 32, 25, 17, 12, 1, 0, 0 },
        .{ 134, 127, 120, 114, 103, 97, 91, 85, 78, 72, 66, 60, 54, 47, 41, 35, 29, 23, 16, 10, 1 },
        .{ 144, 137, 130, 124, 113, 107, 101, 95, 88, 82, 76, 70, 64, 57, 51, 45, 39, 33, 26, 15, 1 },
        .{ 152, 145, 138, 132, 123, 117, 111, 105, 98, 92, 86, 80, 74, 67, 61, 55, 49, 43, 36, 20, 1 },
        .{ 162, 155, 148, 142, 133, 127, 121, 115, 108, 102, 96, 90, 84, 77, 71, 65, 59, 53, 46, 30, 1 },
        .{ 172, 165, 158, 152, 143, 137, 131, 125, 118, 112, 106, 100, 94, 87, 81, 75, 69, 63, 56, 45, 20 },
        .{ 200, 200, 200, 200, 200, 200, 200, 200, 198, 193, 188, 183, 178, 173, 168, 163, 158, 153, 148, 129, 104 },
    };
};

const PulseCache = struct {
    bits: [4][21][41]u8 = std.mem.zeroes([4][21][41]u8),
    counts: [4][21]u8 = std.mem.zeroes([4][21]u8),
    caps: [4][2][21]u8 = std.mem.zeroes([4][2][21]u8),
};

fn buildPulseCache() PulseCache {
    @setEvalBranchQuota(200_000);
    var cache = PulseCache{};
    for (0..4) |lm_plus_one| {
        for (0..StandardCeltMode.nb_ebands) |band| {
            const n = ((StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band]) << @intCast(lm_plus_one)) >> 1;
            if (n == 0) continue;

            var k: usize = 0;
            while (k < StandardCeltMode.max_pseudo and fitsIn32(@intCast(n), getPulses(@intCast(k + 1)))) : (k += 1) {}
            cache.counts[lm_plus_one][band] = @intCast(k);
            if (k > 0) {
                var required: [129]u8 = @as([129]u8, @splat(0));
                getRequiredBits(&required, @intCast(n), getPulses(@intCast(k)), StandardCeltMode.bitres);
                for (1..k + 1) |entry| {
                    const required_bits = required[getPulses(@intCast(entry))];
                    cache.bits[lm_plus_one][band][entry] = if (required_bits == 0) 0 else required_bits - 1;
                }
            }
        }
    }

    for (0..4) |lm| {
        for (1..3) |channels| {
            for (0..StandardCeltMode.nb_ebands) |band| {
                const n0_base = StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band];
                var max_bits: i32 = 0;
                if ((n0_base << @intCast(lm)) == 1) {
                    max_bits = @as(i32, @intCast(channels)) * (1 + StandardCeltMode.max_fine_bits) << StandardCeltMode.bitres;
                } else {
                    var n0 = n0_base;
                    var lm0: i32 = 0;
                    if (n0 > 2) {
                        n0 >>= 1;
                        lm0 -= 1;
                    } else if (n0 <= 1) {
                        lm0 = @min(@as(i32, @intCast(lm)), 1);
                        n0 <<= @intCast(lm0);
                    }
                    max_bits = @as(i32, cache.bits[@intCast(lm0 + 1)][band][cache.counts[@intCast(lm0 + 1)][band]]) + 1;
                    var n = n0;
                    const split_rounds: usize = @intCast(@as(i32, @intCast(lm)) - lm0);
                    var k_idx: usize = 0;
                    while (k_idx < split_rounds) : (k_idx += 1) {
                        max_bits <<= 1;
                        const log_m = (lm0 + @as(i32, @intCast(k_idx))) << StandardCeltMode.bitres;
                        const offset = @divTrunc(@as(i32, celtLog2Frac(n0_base, StandardCeltMode.bitres)) + log_m, 2) - StandardCeltMode.qtheta_offset;
                        const num = 459 * ((2 * @as(i32, @intCast(n)) - 1) * offset + max_bits);
                        const den = ((2 * @as(i32, @intCast(n)) - 1) << 9) - 459;
                        const qb: i32 = @min(@divTrunc(num + @divTrunc(den, 2), den), @as(i32, 57));
                        max_bits += qb;
                        n <<= 1;
                    }
                    if (channels == 2) {
                        max_bits <<= 1;
                        const ndof = 2 * @as(i32, @intCast(n)) - if (n == 2) 1 else 0;
                        const offset = @as(i32, @intCast((celtLog2Frac(n0_base, StandardCeltMode.bitres) + @as(u8, @intCast(lm << StandardCeltMode.bitres))) >> 1)) -
                            (if (n == 2) StandardCeltMode.qtheta_offset_two_phase else StandardCeltMode.qtheta_offset);
                        const num = (if (n == 2) 512 else 487) * (max_bits + ndof * offset);
                        const den = (ndof << 9) - (if (n == 2) 512 else 487);
                        const qb: i32 = @min(@divTrunc(num + @divTrunc(den, 2), den), if (n == 2) @as(i32, 64) else @as(i32, 61));
                        max_bits += qb;
                    }
                    const ndof = @as(i32, @intCast(channels * n)) + (if (channels == 2 and n > 2) 1 else 0);
                    var offset = @as(i32, @intCast((celtLog2Frac(n0_base, StandardCeltMode.bitres) + @as(u8, @intCast(lm << StandardCeltMode.bitres))) >> 1)) - StandardCeltMode.fine_offset;
                    if (n == 2) offset += 1 << (StandardCeltMode.bitres - 2);
                    const num = max_bits + ndof * offset;
                    const den = (ndof - 1) << StandardCeltMode.bitres;
                    const qb: i32 = @min(@divTrunc(num + @divTrunc(den, 2), den), @as(i32, StandardCeltMode.max_fine_bits));
                    max_bits += @as(i32, @intCast(channels)) * qb << StandardCeltMode.bitres;
                }

                const width = @as(i32, @intCast((StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band]) << @intCast(lm)));
                const cap = @divTrunc(4 * max_bits, @as(i32, @intCast(channels)) * width) - 64;
                cache.caps[lm][channels - 1][band] = @intCast(@max(0, cap));
            }
        }
    }
    return cache;
}

const standard_pulse_cache = buildPulseCache();

pub fn celtBasicWindowAlloc(allocator: std.mem.Allocator, overlap: usize) ![]f32 {
    if (overlap == 0) return error.UnsupportedAudioFormat;

    const out = try allocator.alloc(f32, overlap);
    errdefer allocator.free(out);

    const overlap_f = @as(f32, @floatFromInt(overlap));
    for (out, 0..) |*sample, i| {
        const phase = (@as(f32, @floatFromInt(i)) + 0.5) / overlap_f;
        const inner = @sin((std.math.pi / 2.0) * phase);
        sample.* = @sin((std.math.pi / 2.0) * inner * inner);
    }
    return out;
}

pub fn initCeltEnergyState(channels: u8) !CeltEnergyState {
    if (channels == 0 or channels > 2) return error.UnsupportedAudioFormat;
    return .{ .channels = channels };
}

pub fn decodeCeltResidualFrame(
    allocator: std.mem.Allocator,
    state: *CeltEnergyState,
    frame_bytes: []const u8,
    toc: Toc,
) !CeltResidualFrame {
    if (state.channels != (if (toc.stereo) @as(u8, 2) else @as(u8, 1))) return error.UnsupportedAudioFormat;

    const lm = try lmForFrameDurationUs(toc.frame_duration_us);
    const start_band = try celtStartBandForToc(toc);
    const end_band = try endBandForBandwidth(toc.bandwidth);
    const total_bits = frame_bytes.len * 8;

    var decoder = RangeDecoder.init(frame_bytes);
    return decodeCeltResidualFrameFromDecoder(
        allocator,
        state,
        &decoder,
        total_bits,
        lm,
        start_band,
        end_band,
    );
}

fn decodeCeltResidualFrameFromDecoder(
    allocator: std.mem.Allocator,
    state: *CeltEnergyState,
    decoder: *RangeDecoder,
    total_bits: usize,
    lm: u2,
    start_band: usize,
    end_band: usize,
) !CeltResidualFrame {
    const header = try decodeCeltHeader(decoder, total_bits, lm, start_band);
    if (header.silence) {
        return .{
            .header = header,
            .start_band = start_band,
            .end_band = end_band,
            .channels = state.channels,
            .plan = .{
                .spread_decision = 0,
                .alloc_trim = 0,
                .coded_bands = start_band,
            },
            .bands = @as([2][21]CeltResidualBand, @splat(@as([21]CeltResidualBand, @splat(.{ .len = 0 })))),
            .band_energies = state.old_band_energies,
        };
    }
    if (header.is_transient) return error.UnsupportedAudioFormat;
    try unquantCoarseEnergy(decoder, total_bits, state, start_band, end_band, header.intra_energy, lm);

    var plan = try decodeCeltResidualPlan(decoder, total_bits, lm, start_band, end_band, state.channels);
    try unquantFineEnergy(decoder, &state.old_band_energies, start_band, end_band, state.channels, &plan);
    var bands = @as([2][21]CeltResidualBand, @splat(@as([21]CeltResidualBand, @splat(.{ .len = 0 }))));
    try decodeCeltResidualBands(allocator, decoder, lm, start_band, end_band, state.channels, &plan, &bands);
    try unquantEnergyFinalise(
        decoder,
        total_bits,
        &state.old_band_energies,
        start_band,
        end_band,
        state.channels,
        &plan,
    );

    return .{
        .header = header,
        .start_band = start_band,
        .end_band = end_band,
        .channels = state.channels,
        .plan = plan,
        .bands = bands,
        .band_energies = state.old_band_energies,
    };
}

pub fn decodeCeltCoarseEnergyFrame(
    state: *CeltEnergyState,
    frame_bytes: []const u8,
    toc: Toc,
) !CeltCoarseEnergyFrame {
    if (state.channels != (if (toc.stereo) @as(u8, 2) else @as(u8, 1))) return error.UnsupportedAudioFormat;

    const lm = try lmForFrameDurationUs(toc.frame_duration_us);
    const start_band = try celtStartBandForToc(toc);
    const end_band = try endBandForBandwidth(toc.bandwidth);
    const total_bits = frame_bytes.len * 8;

    var decoder = RangeDecoder.init(frame_bytes);
    const header = try decodeCeltHeader(&decoder, total_bits, lm, start_band);
    try unquantCoarseEnergy(&decoder, total_bits, state, start_band, end_band, header.intra_energy, lm);

    return .{
        .header = header,
        .start_band = start_band,
        .end_band = end_band,
        .channels = state.channels,
        .band_energies = state.old_band_energies,
    };
}

pub fn celtImdctInto(out: []f32, coefficients: []const f32) !void {
    if (coefficients.len == 0 or out.len != coefficients.len * 2) return error.UnsupportedAudioFormat;

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

pub fn applyCeltWindowInPlace(allocator: std.mem.Allocator, samples: []f32, overlap: usize) !void {
    if (samples.len == 0 or overlap == 0 or overlap * 2 > samples.len) return error.UnsupportedAudioFormat;

    const window = try celtBasicWindowAlloc(allocator, overlap);
    defer allocator.free(window);

    for (0..overlap) |i| {
        samples[i] *= window[i];
        samples[samples.len - overlap + i] *= window[overlap - 1 - i];
    }
}

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

    const stream_channels: u8 = if (tocs.items[0].stereo) 2 else 1;
    var uniform_channels = true;
    for (tocs.items[1..]) |toc| {
        if ((if (toc.stereo) @as(u8, 2) else @as(u8, 1)) != stream_channels) {
            uniform_channels = false;
            break;
        }
    }

    if (!uniform_channels) {
        return try decodeMixedChannelPacketStreamAlloc(allocator, packets.items, tocs.items, output_channels);
    }

    var decoded = try decodeInterleavedPacketsAlloc(
        allocator,
        packets.items,
        tocs.items,
        stream_channels,
        0,
        0,
        null,
    );
    errdefer decoded.deinit();

    if (stream_channels == output_channels) return decoded;
    if (stream_channels == 2 and output_channels == 1) {
        const mono = try downmixStereoOwnedToMonoAlloc(allocator, decoded.samples);
        return .{
            .samples = mono,
            .sample_rate = decoded.sample_rate,
            .channels = 1,
            .allocator = allocator,
        };
    }
    if (stream_channels == 1 and output_channels == 2) {
        const stereo = try duplicateMonoOwnedToStereoAlloc(allocator, decoded.samples);
        return .{
            .samples = stereo,
            .sample_rate = decoded.sample_rate,
            .channels = 2,
            .allocator = allocator,
        };
    }
    return error.UnsupportedAudioFormat;
}

fn decodeMixedChannelPacketStreamAlloc(
    allocator: std.mem.Allocator,
    packets: []const []const u8,
    packet_tocs: []const Toc,
    output_channels: u8,
) !DecodedInterleaved {
    var samples = std.ArrayList(f32).empty;
    defer samples.deinit(allocator);

    var start: usize = 0;
    while (start < packets.len) {
        const run_channels: u8 = if (packet_tocs[start].stereo) 2 else 1;
        var end = start + 1;
        while (end < packets.len and (if (packet_tocs[end].stereo) @as(u8, 2) else @as(u8, 1)) == run_channels) : (end += 1) {}

        var decoded = decodeInterleavedPacketsAlloc(
            allocator,
            packets[start..end],
            packet_tocs[start..end],
            run_channels,
            0,
            0,
            null,
        ) catch |err| switch (err) {
            error.UnsupportedAudioFormat => {
                try decodePacketRunIndividuallyAlloc(allocator, &samples, packets[start..end], packet_tocs[start..end], run_channels, output_channels);
                start = end;
                continue;
            },
            else => return err,
        };

        if (run_channels == output_channels) {
            try samples.appendSlice(allocator, decoded.samples);
            decoded.deinit();
        } else if (run_channels == 2 and output_channels == 1) {
            const mono = try downmixStereoOwnedToMonoAlloc(allocator, decoded.samples);
            decoded.samples = &.{};
            defer allocator.free(mono);
            try samples.appendSlice(allocator, mono);
            decoded.deinit();
        } else if (run_channels == 1 and output_channels == 2) {
            const stereo = try duplicateMonoOwnedToStereoAlloc(allocator, decoded.samples);
            decoded.samples = &.{};
            defer allocator.free(stereo);
            try samples.appendSlice(allocator, stereo);
            decoded.deinit();
        } else {
            decoded.deinit();
            return error.UnsupportedAudioFormat;
        }

        start = end;
    }

    return .{
        .samples = try samples.toOwnedSlice(allocator),
        .sample_rate = 48_000,
        .channels = output_channels,
        .allocator = allocator,
    };
}

fn decodePacketRunIndividuallyAlloc(
    allocator: std.mem.Allocator,
    out: *std.ArrayList(f32),
    packets: []const []const u8,
    packet_tocs: []const Toc,
    run_channels: u8,
    output_channels: u8,
) !void {
    for (packets, packet_tocs) |packet, toc| {
        var decoded = decodeInterleavedPacketsAlloc(
            allocator,
            &.{packet},
            &.{toc},
            run_channels,
            0,
            0,
            null,
        ) catch |err| switch (err) {
            error.UnsupportedAudioFormat => {
                const frames = try packetSamples(packet, 48_000);
                try out.appendNTimes(allocator, 0.0, @as(usize, frames) * output_channels);
                continue;
            },
            else => return err,
        };

        if (run_channels == output_channels) {
            try out.appendSlice(allocator, decoded.samples);
            decoded.deinit();
        } else if (run_channels == 2 and output_channels == 1) {
            const mono = try downmixStereoOwnedToMonoAlloc(allocator, decoded.samples);
            decoded.samples = &.{};
            defer allocator.free(mono);
            try out.appendSlice(allocator, mono);
            decoded.deinit();
        } else if (run_channels == 1 and output_channels == 2) {
            const stereo = try duplicateMonoOwnedToStereoAlloc(allocator, decoded.samples);
            decoded.samples = &.{};
            defer allocator.free(stereo);
            try out.appendSlice(allocator, stereo);
            decoded.deinit();
        } else {
            decoded.deinit();
            return error.UnsupportedAudioFormat;
        }
    }
}

fn decodeInterleavedPacketsAlloc(
    allocator: std.mem.Allocator,
    packets: []const []const u8,
    packet_tocs: []const Toc,
    channels: u8,
    output_gain_q8: i16,
    pre_skip: u16,
    playable_frames_opt: ?u64,
) !DecodedInterleaved {
    if (packets.len != packet_tocs.len) return error.UnsupportedAudioFormat;

    var energy_state = try initCeltEnergyState(channels);
    var synth_state = try CeltSynthState.init(allocator, channels, standardOverlap());
    defer synth_state.deinit();
    const celt_plan = try sharedCeltSynthesisPlan();
    var silk_state = SilkSynthState{ .channels = channels };

    var samples = std.ArrayList(f32).empty;
    defer samples.deinit(allocator);
    var saw_silk_family = false;

    for (packets, packet_tocs, 0..) |packet, toc, packet_index| {
        _ = packet_index;
        var split = try splitFramesAlloc(allocator, packet);
        defer split.deinit();

        for (split.frames, 0..) |frame_bytes, frame_index| {
            _ = frame_index;
            switch (toc.mode) {
                .celt => {
                    const residual = decodeCeltResidualFrame(allocator, &energy_state, frame_bytes, toc) catch |err| {
                        if (err == error.UnsupportedAudioFormat and (saw_silk_family or samples.items.len != 0)) {
                            const frame_samples = (@as(usize, toc.frame_duration_us) * 48) / 1000;
                            const zero_len = frame_samples * channels;
                            try samples.appendNTimes(allocator, 0.0, zero_len);
                            continue;
                        }
                        return err;
                    };
                    const frame_samples = try synthesizeFrameWithPlanAlloc(allocator, &synth_state, celt_plan, residual, toc);
                    defer allocator.free(frame_samples);
                    try samples.appendSlice(allocator, frame_samples);
                },
                .silk => {
                    saw_silk_family = true;
                    const silk_packet = try decodeSilkPacketParameters(frame_bytes, toc, channels);
                    const pcm_packet = try silkSynthesizePacketAlloc(allocator, &silk_state, &silk_packet);
                    defer allocator.free(pcm_packet.samples);
                    const pcm_48k = try upsampleInterleavedTo48kAlloc(allocator, pcm_packet.samples, pcm_packet.sample_rate, channels);
                    defer allocator.free(pcm_48k);
                    try samples.appendSlice(allocator, pcm_48k);
                },
                .hybrid => {
                    saw_silk_family = true;
                    const frame_samples = decodeHybridFrameWithPlanAlloc(
                        allocator,
                        &energy_state,
                        &synth_state,
                        celt_plan,
                        &silk_state,
                        frame_bytes,
                        toc,
                        channels,
                    ) catch |err| return err;
                    defer allocator.free(frame_samples);
                    try samples.appendSlice(allocator, frame_samples);
                },
            }
        }
    }

    var owned = try samples.toOwnedSlice(allocator);
    errdefer allocator.free(owned);
    applyOutputGainInPlace(owned, output_gain_q8);
    if (pre_skip != 0 or playable_frames_opt != null) {
        owned = try trimInterleavedOwned(
            allocator,
            owned,
            channels,
            pre_skip,
            playable_frames_opt orelse @intCast(@divFloor(owned.len, channels)),
        );
    }

    return .{
        .samples = owned,
        .sample_rate = 48_000,
        .channels = channels,
        .allocator = allocator,
    };
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
        .channel_mapping = @as([8]u8, @splat(0)),
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

fn silkDecodePacketHeaderFromDecoder(decoder: *RangeDecoder, toc: Toc, channels_internal: u8) !SilkPacketHeader {
    const internal_frame_count = try silkInternalFramesPerPacket(toc);
    const subframes_per_internal_frame = try silkSubframesPerInternalFrame(toc);
    var header = SilkPacketHeader{
        .mode = toc.mode,
        .channels = channels_internal,
        .internal_frame_count = internal_frame_count,
        .subframes_per_internal_frame = subframes_per_internal_frame,
        .consumed_bits = 0,
        .consumed_frac_bits = 0,
    };

    for (0..channels_internal) |channel| {
        for (0..internal_frame_count) |frame| {
            header.channel_headers[channel].vad_flags[frame] = (try decoder.decodeBitLogp(1)) != 0;
        }
        header.channel_headers[channel].lbrr_flag = (try decoder.decodeBitLogp(1)) != 0;
    }

    for (0..channels_internal) |channel| {
        if (!header.channel_headers[channel].lbrr_flag) continue;
        if (internal_frame_count == 1) {
            header.channel_headers[channel].lbrr_flags[0] = true;
            continue;
        }

        const symbol = switch (internal_frame_count) {
            2 => @as(u32, try decoder.decodeIcdf(&SilkLbrrFlags2Icdf)) + 1,
            3 => @as(u32, try decoder.decodeIcdf(&SilkLbrrFlags3Icdf)) + 1,
            else => return error.UnsupportedAudioFormat,
        };
        for (0..internal_frame_count) |frame| {
            header.channel_headers[channel].lbrr_flags[frame] = ((symbol >> @intCast(frame)) & 0x01) != 0;
        }
    }

    header.consumed_bits = decoder.tell();
    header.consumed_frac_bits = decoder.tellFrac();
    return header;
}

fn silkFsKhzForToc(toc: Toc) !u8 {
    return switch (toc.mode) {
        .hybrid => 16,
        .silk => switch (toc.bandwidth) {
            .nb => 8,
            .mb => 12,
            .wb => 16,
            else => error.UnsupportedAudioFormat,
        },
        else => error.UnsupportedAudioFormat,
    };
}

fn silkFrameLengthSamples(toc: Toc, fs_khz: u8) !usize {
    const subframes = try silkSubframesPerInternalFrame(toc);
    return @as(usize, @intCast(subframes)) * 5 * @as(usize, fs_khz);
}

fn silkDecodeStereoPred(decoder: *RangeDecoder) ![2]i32 {
    const joint = try decoder.decodeIcdf(&silk_tables.stereo_pred_joint_icdf);
    var ix = [2][3]i32{ @as([3]i32, @splat(0)), @as([3]i32, @splat(0)) };
    ix[0][2] = @divTrunc(joint, 5);
    ix[1][2] = joint - 5 * ix[0][2];
    for (0..2) |n| {
        ix[n][0] = try decoder.decodeIcdf(&silk_tables.uniform3_icdf);
        ix[n][1] = try decoder.decodeIcdf(&silk_tables.uniform5_icdf);
    }

    var pred = [2]i32{ 0, 0 };
    for (0..2) |n| {
        ix[n][0] += 3 * ix[n][2];
        const low = silk_tables.stereo_pred_quant_q13[@intCast(ix[n][0])];
        const high = silk_tables.stereo_pred_quant_q13[@intCast(ix[n][0] + 1)];
        const step = @divTrunc((high - low), 10);
        pred[n] = low + step * (2 * ix[n][1] + 1);
    }
    pred[0] -= pred[1];
    return pred;
}

fn silkDecodeMidOnly(decoder: *RangeDecoder) !bool {
    return (try decoder.decodeIcdf(&silk_tables.stereo_only_code_mid_icdf)) != 0;
}

fn silkDecodeSingleFrameFront(
    decoder: *RangeDecoder,
    vad: bool,
    frame_length: usize,
    fs_khz: u8,
    cond_coding: SilkCondCoding,
) !SilkFrameFront {
    var out = SilkFrameFront{
        .indices = try silkDecodeIndices(decoder, vad, fs_khz, cond_coding),
        .pulse_len = frame_length,
    };
    try silkDecodePulses(
        decoder,
        out.pulses[0..frame_length],
        out.indices.signal_type,
        out.indices.quant_offset_type,
    );
    return out;
}

fn silkDecodeIndices(
    decoder: *RangeDecoder,
    vad: bool,
    fs_khz: u8,
    cond_coding: SilkCondCoding,
) !SilkIndices {
    if (fs_khz != 8 and fs_khz != 12 and fs_khz != 16) return error.UnsupportedAudioFormat;

    var out = SilkIndices{};
    const ix_type = if (vad)
        @as(i32, @intCast(try decoder.decodeIcdf(&silk_tables.type_offset_vad_icdf))) + 2
    else
        @as(i32, @intCast(try decoder.decodeIcdf(&silk_tables.type_offset_no_vad_icdf)));
    out.signal_type = @enumFromInt(@as(u2, @intCast(@divTrunc(ix_type, 2))));
    out.quant_offset_type = @intCast(ix_type & 1);

    if (cond_coding == .conditionally) {
        out.gains_indices[0] = try decoder.decodeIcdf(&silk_tables.delta_gain_icdf);
    } else {
        out.gains_indices[0] = @intCast((try decoder.decodeIcdf(&silk_tables.gain_icdf[@intFromEnum(out.signal_type) >> 1])) << 3);
        out.gains_indices[0] += try decoder.decodeIcdf(&silk_tables.uniform8_icdf);
    }
    for (1..4) |i| out.gains_indices[i] = try decoder.decodeIcdf(&silk_tables.delta_gain_icdf);

    const cb = silk_tables.wb_codebook;
    const cb1_base: usize = (@as(usize, @intFromEnum(out.signal_type)) >> 1) * cb.n_vectors;
    out.nlsf_indices[0] = @intCast(try decoder.decodeIcdf(cb.cb1_icdf[cb1_base .. cb1_base + cb.n_vectors]));

    var ec_ix = @as([16]i16, @splat(0));
    var pred_q8 = @as([16]u8, @splat(0));
    silkNlsfUnpack(&ec_ix, &pred_q8, cb, @intCast(out.nlsf_indices[0]));
    for (0..cb.order) |i| {
        const ec_base: usize = @intCast(ec_ix[i]);
        var ix = @as(i32, @intCast(try decoder.decodeIcdf(cb.ec_icdf[ec_base .. ec_base + 9])));
        if (ix == 0) {
            ix -= @as(i32, @intCast(try decoder.decodeIcdf(&silk_tables.nlsf_ext_icdf)));
        } else if (ix == 8) {
            ix += @as(i32, @intCast(try decoder.decodeIcdf(&silk_tables.nlsf_ext_icdf)));
        }
        out.nlsf_indices[i + 1] = @intCast(ix - 4);
    }
    out.nlsf_interp_coef_q2 = try decoder.decodeIcdf(&silk_tables.nlsf_interp_factor_icdf);

    if (out.signal_type == .voiced) {
        out.lag_index = @intCast((@as(i32, @intCast(try decoder.decodeIcdf(&silk_tables.pitch_lag_icdf))) * @as(i32, fs_khz >> 1)) +
            @as(i32, @intCast(try decoder.decodeIcdf(&silk_tables.uniform8_icdf))));
        out.contour_index = try decoder.decodeIcdf(&silk_tables.pitch_contour_icdf);
        out.per_index = try decoder.decodeIcdf(&silk_tables.ltp_per_index_icdf);
        const ltp_gain_icdf = switch (out.per_index) {
            0 => &silk_tables.ltp_gain_icdf_0,
            1 => &silk_tables.ltp_gain_icdf_1,
            2 => &silk_tables.ltp_gain_icdf_2,
            else => return error.UnsupportedAudioFormat,
        };
        for (0..4) |k| out.ltp_index[k] = try decoder.decodeIcdf(ltp_gain_icdf);
        if (cond_coding == .independently) {
            out.ltp_scale_index = try decoder.decodeIcdf(&silk_tables.ltp_scale_icdf);
        }
    }

    out.seed = try decoder.decodeIcdf(&silk_tables.uniform4_icdf);
    return out;
}

fn silkNlsfUnpack(ec_ix: *[16]i16, pred_q8: *[16]u8, cb: silk_tables.Codebook, cb1_index: usize) void {
    const ec_sel_ptr = cb.ec_sel[cb1_index * cb.order / 2 ..];
    var sel_idx: usize = 0;
    var i: usize = 0;
    while (i < cb.order) : (i += 2) {
        const entry = ec_sel_ptr[sel_idx];
        sel_idx += 1;
        ec_ix[i] = @intCast(((entry >> 1) & 7) * 9);
        pred_q8[i] = cb.pred_q8[i + (entry & 1) * (cb.order - 1)];
        ec_ix[i + 1] = @intCast(((entry >> 5) & 7) * 9);
        pred_q8[i + 1] = cb.pred_q8[i + ((entry >> 4) & 1) * (cb.order - 1) + 1];
    }
}

fn silkDecodeSplit(decoder: *RangeDecoder, p: u16, table: []const u8) !struct { i16, i16 } {
    if (p == 0) return .{ 0, 0 };
    const offset = silk_tables.shell_code_table_offsets[p];
    const child1 = try decoder.decodeIcdf(table[offset..]);
    return .{ @intCast(child1), @intCast(p - child1) };
}

fn silkShellDecoder(decoder: *RangeDecoder, out: []i16, pulses4: u16) !void {
    if (out.len != 16) return error.UnsupportedAudioFormat;

    var pulses3 = @as([2]i16, @splat(0));
    var pulses2 = @as([4]i16, @splat(0));
    var pulses1 = @as([8]i16, @splat(0));

    const s30, const s31 = try silkDecodeSplit(decoder, pulses4, &silk_tables.shell_code_table3);
    pulses3[0] = s30;
    pulses3[1] = s31;

    const s20, const s21 = try silkDecodeSplit(decoder, @intCast(pulses3[0]), &silk_tables.shell_code_table2);
    pulses2[0] = s20;
    pulses2[1] = s21;
    const s22, const s23 = try silkDecodeSplit(decoder, @intCast(pulses3[1]), &silk_tables.shell_code_table2);
    pulses2[2] = s22;
    pulses2[3] = s23;

    inline for (0..4) |pair| {
        const a, const b = try silkDecodeSplit(decoder, @intCast(pulses2[pair]), &silk_tables.shell_code_table1);
        pulses1[pair * 2] = a;
        pulses1[pair * 2 + 1] = b;
    }
    inline for (0..8) |pair| {
        const a, const b = try silkDecodeSplit(decoder, @intCast(pulses1[pair]), &silk_tables.shell_code_table0);
        out[pair * 2] = a;
        out[pair * 2 + 1] = b;
    }
}

fn silkDecodeSigns(decoder: *RangeDecoder, pulses: []i16, signal_type: SilkSignalType, quant_offset_type: u1, sum_pulses: []const i32) !void {
    const base: usize = 7 * (@as(usize, quant_offset_type) + (@as(usize, @intFromEnum(signal_type)) << 1));
    var block: usize = 0;
    while (block * 16 < pulses.len) : (block += 1) {
        const p = sum_pulses[block];
        if (p <= 0) continue;
        const icdf0 = silk_tables.sign_icdf[base + @min(@as(usize, @intCast(p & 0x1f)), 6)];
        const icdf = [_]u8{ icdf0, 0 };
        const start = block * 16;
        const end = @min(start + 16, pulses.len);
        for (pulses[start..end]) |*pulse| {
            if (pulse.* > 0) {
                const sign = try decoder.decodeIcdf(&icdf);
                if (sign == 0) pulse.* = -pulse.*;
            }
        }
    }
}

fn silkDecodePulses(
    decoder: *RangeDecoder,
    pulses: []i16,
    signal_type: SilkSignalType,
    quant_offset_type: u1,
) !void {
    const signal_class: usize = @as(usize, @intFromEnum(signal_type)) >> 1;
    const rate_level_index = try decoder.decodeIcdf(&silk_tables.rate_levels_icdf[signal_class]);

    const iter = @divTrunc(pulses.len + 15, 16);
    var sum_pulses = @as([20]i32, @splat(0));
    var n_lshifts = @as([20]u8, @splat(0));
    if (iter > sum_pulses.len) return error.UnsupportedAudioFormat;

    for (0..iter) |i| {
        sum_pulses[i] = try decoder.decodeIcdf(&silk_tables.pulses_per_block_icdf[rate_level_index]);
        while (sum_pulses[i] == 17) {
            n_lshifts[i] += 1;
            const table = silk_tables.pulses_per_block_icdf[9][if (n_lshifts[i] == 10) 1 else 0..];
            sum_pulses[i] = try decoder.decodeIcdf(table);
        }
    }

    for (0..iter) |i| {
        const start = i * 16;
        const end = @min(start + 16, pulses.len);
        @memset(pulses[start..end], 0);
        if (sum_pulses[i] > 0) {
            var block = @as([16]i16, @splat(0));
            try silkShellDecoder(decoder, &block, @intCast(sum_pulses[i]));
            @memcpy(pulses[start..end], block[0 .. end - start]);
        }
    }

    for (0..iter) |i| {
        if (n_lshifts[i] == 0) continue;
        const start = i * 16;
        const end = @min(start + 16, pulses.len);
        for (pulses[start..end]) |*pulse| {
            var abs_q: i32 = pulse.*;
            for (0..n_lshifts[i]) |_| {
                abs_q <<= 1;
                abs_q += try decoder.decodeIcdf(&silk_tables.lsb_icdf);
            }
            pulse.* = @intCast(abs_q);
        }
        sum_pulses[i] |= @as(i32, @intCast(n_lshifts[i])) << 5;
    }

    try silkDecodeSigns(decoder, pulses, signal_type, quant_offset_type, sum_pulses[0..iter]);
}

fn silkRand(seed: i32) i32 {
    return seed *% 196_314_165 +% 907_633_515;
}

fn silkNlsfResidualDequant(out: *[16]i16, indices: []const i8, pred_q8: []const u8, quant_step_size_q16: i32, order: usize) void {
    var prev_q10: i32 = 0;
    var rev: usize = order;
    while (rev > 0) {
        rev -= 1;
        const pred_q10 = (prev_q10 * @as(i32, pred_q8[rev])) >> 8;
        var residual_q10 = @as(i32, indices[rev]) << 10;
        if (residual_q10 > 0) {
            residual_q10 -= 102;
        } else if (residual_q10 < 0) {
            residual_q10 += 102;
        }
        prev_q10 = pred_q10 + @as(i32, @intCast((@as(i64, residual_q10) * quant_step_size_q16) >> 16));
        out[rev] = @intCast(prev_q10);
    }
}

fn silkNlsfStabilize(nlsf_q15: *[16]i16, delta_min_q15: []const i16, order: usize) void {
    var values = @as([16]i32, @splat(0));
    for (0..order) |i| values[i] = nlsf_q15[i];

    var iter: usize = 0;
    while (iter < 8) : (iter += 1) {
        values[0] = @max(values[0], delta_min_q15[0]);
        for (1..order) |i| {
            values[i] = @max(values[i], values[i - 1] + delta_min_q15[i]);
        }
        values[order - 1] = @min(values[order - 1], 32767 - delta_min_q15[order]);
        var i = order - 1;
        while (i > 0) : (i -= 1) {
            values[i - 1] = @min(values[i - 1], values[i] - delta_min_q15[i]);
        }
    }

    for (0..order) |i| nlsf_q15[i] = @intCast(@max(0, @min(values[i], 32767)));
}

fn silkNlsfDecode(out: *[16]i16, cb: silk_tables.Codebook, indices: *const [17]i8) void {
    var ec_ix = @as([16]i16, @splat(0));
    var pred_q8 = @as([16]u8, @splat(0));
    silkNlsfUnpack(&ec_ix, &pred_q8, cb, @intCast(indices[0]));

    var residual_q10 = @as([16]i16, @splat(0));
    silkNlsfResidualDequant(&residual_q10, indices[1 .. cb.order + 1], pred_q8[0..cb.order], cb.quant_step_size_q16, cb.order);

    const base = @as(usize, @intCast(indices[0])) * cb.order;
    for (0..cb.order) |i| {
        const weighted = @divTrunc(@as(i32, residual_q10[i]) << 14, @as(i32, cb.cb1_wght_q9[base + i]));
        const value_q15 = weighted + (@as(i32, cb.cb1_nlsf_q8[base + i]) << 7);
        out[i] = @intCast(@max(0, @min(value_q15, 32767)));
    }
    silkNlsfStabilize(out, cb.delta_min_q15, cb.order);
}

fn silkOrderOrdering(order: usize) []const u8 {
    return switch (order) {
        16 => &[_]u8{ 0, 15, 8, 7, 4, 11, 12, 3, 2, 13, 10, 5, 6, 9, 14, 1 },
        10 => &[_]u8{ 0, 9, 6, 3, 4, 5, 8, 1, 2, 7 },
        else => unreachable,
    };
}

fn silkNlsfFindPoly(out: []f64, c_lsf: []const f64, dd: usize) void {
    @memset(out, 0);
    out[0] = 1.0;
    out[1] = -c_lsf[0];
    var k: usize = 1;
    while (k < dd) : (k += 1) {
        const ftmp = c_lsf[2 * k];
        out[k + 1] = (2.0 * out[k - 1]) - (ftmp * out[k]);
        var n = k;
        while (n > 1) : (n -= 1) {
            out[n] += out[n - 2] - (ftmp * out[n - 1]);
        }
        out[1] -= ftmp;
    }
}

fn silkNlsfToLpcQ12(out: *[16]i16, nlsf_q15: *const [16]i16, order: usize) void {
    const ordering = silkOrderOrdering(order);
    var cos_lsf = @as([16]f64, @splat(0.0));
    for (0..order) |k| {
        const phase = (@as(f64, @floatFromInt(nlsf_q15[k])) / 32768.0) * std.math.pi;
        cos_lsf[ordering[k]] = 2.0 * @cos(phase);
    }

    const dd = order / 2;
    var p = @as([9]f64, @splat(0.0));
    var q = @as([9]f64, @splat(0.0));
    silkNlsfFindPoly(p[0 .. dd + 1], cos_lsf[0..order], dd);
    silkNlsfFindPoly(q[0 .. dd + 1], cos_lsf[1..order], dd);

    @memset(out, 0);
    for (0..dd) |k| {
        const ptmp = p[k + 1] + p[k];
        const qtmp = q[k + 1] - q[k];
        out[k] = silkRoundClamp16((-qtmp - ptmp) * 4096.0);
        out[order - k - 1] = silkRoundClamp16((qtmp - ptmp) * 4096.0);
    }
}

fn silkDequantGains(out: *[4]i32, indices: [4]u8, prev_index: *i32, conditional: bool) void {
    const min_qgain_db = 2;
    const max_qgain_db = 88;
    const n_levels_qgain = 64;
    const min_delta_gain_quant = -4;
    const max_delta_gain_quant = 36;
    const offset = ((min_qgain_db * 128) / 6) + 16 * 128;
    const inv_scale_q16 = (65536 * (((max_qgain_db - min_qgain_db) * 128) / 6)) / (n_levels_qgain - 1);

    for (0..4) |k| {
        if (k == 0 and !conditional) {
            prev_index.* = @max(@as(i32, indices[k]), prev_index.* - 16);
        } else {
            const ind_tmp = @as(i32, indices[k]) + min_delta_gain_quant;
            const double_step_threshold = (2 * max_delta_gain_quant) - n_levels_qgain + prev_index.*;
            if (ind_tmp > double_step_threshold) {
                prev_index.* += (ind_tmp << 1) - double_step_threshold;
            } else {
                prev_index.* += ind_tmp;
            }
        }
        prev_index.* = @max(0, @min(prev_index.*, n_levels_qgain - 1));
        const log_gain_q7 = @min(((inv_scale_q16 * prev_index.*) >> 16) + offset, 3967);
        const gain_q16 = @exp2(@as(f64, @floatFromInt(log_gain_q7)) / 128.0) * 65536.0;
        out[k] = if (gain_q16 >= @as(f64, @floatFromInt(std.math.maxInt(i32))))
            std.math.maxInt(i32)
        else
            @intFromFloat(@round(gain_q16));
    }
}

fn silkDecodePitchLags(out: *[4]i16, lag_index: i16, contour_index: u8, fs_khz: u8) !void {
    if ((fs_khz != 8 and fs_khz != 12 and fs_khz != 16) or contour_index >= silk_tables.pitch_stage3_offsets[0].len) return error.UnsupportedAudioFormat;
    const min_lag = 2 * @as(i32, fs_khz);
    const max_lag = 18 * @as(i32, fs_khz);
    const lag = min_lag + lag_index;
    for (0..4) |k| {
        const value = lag + silk_tables.pitch_stage3_offsets[k][contour_index];
        out[k] = @intCast(@max(min_lag, @min(value, max_lag)));
    }
}

fn silkDecodeLtpCoefficients(out: *[20]i16, per_index: u8, ltp_index: [4]u8) !void {
    const table = switch (per_index) {
        0 => silk_tables.ltp_gain_vq_0[0..],
        1 => silk_tables.ltp_gain_vq_1[0..],
        2 => silk_tables.ltp_gain_vq_2[0..],
        else => return error.UnsupportedAudioFormat,
    };
    for (0..4) |subframe| {
        const vector = table[ltp_index[subframe]];
        for (0..5) |tap| {
            out[subframe * 5 + tap] = @as(i16, vector[tap]) << 7;
        }
    }
}

fn silkDecodeExcitation(out: []i32, pulses: []const i16, quant_offset_q10: i16, seed: u8) void {
    var rand_seed: i32 = seed;
    for (out, pulses) |*sample, pulse| {
        rand_seed = silkRand(rand_seed);
        var exc_q14 = @as(i32, pulse) << 14;
        if (exc_q14 > 0) {
            exc_q14 -= 80 << 4;
        } else if (exc_q14 < 0) {
            exc_q14 += 80 << 4;
        }
        exc_q14 += @as(i32, quant_offset_q10) << 4;
        if (rand_seed < 0) exc_q14 = -exc_q14;
        rand_seed +%= pulse;
        sample.* = exc_q14;
    }
}

fn silkDecodeFrameParameters(
    state: *SilkChannelState,
    toc: Toc,
    front: *const SilkFrameFront,
    conditional: bool,
) !SilkFrameParameters {
    const fs_khz = try silkFsKhzForToc(toc);

    var out = SilkFrameParameters{};
    out.signal_type = front.indices.signal_type;
    out.fs_khz = fs_khz;
    out.subframe_count = try silkSubframesPerInternalFrame(toc);
    out.excitation_len = front.pulse_len;
    out.quant_offset_q10 = silk_tables.quantization_offsets_q10[@intFromEnum(front.indices.signal_type) >> 1][front.indices.quant_offset_type];

    silkDequantGains(&out.gains_q16, front.indices.gains_indices, &state.prev_gain_index, conditional);
    silkNlsfDecode(&out.nlsf_q15, silk_tables.wb_codebook, &front.indices.nlsf_indices);
    silkNlsfToLpcQ12(&out.pred_coef_q12[1], &out.nlsf_q15, 16);

    const interp_coef_q2: u8 = if (state.first_frame_after_reset) 4 else front.indices.nlsf_interp_coef_q2;
    if (interp_coef_q2 < 4) {
        var interp_q15 = @as([16]i16, @splat(0));
        for (0..16) |i| {
            const delta = @as(i32, out.nlsf_q15[i]) - state.prev_nlsf_q15[i];
            interp_q15[i] = @intCast(state.prev_nlsf_q15[i] + @divTrunc(@as(i32, interp_coef_q2) * delta, 4));
        }
        silkNlsfToLpcQ12(&out.pred_coef_q12[0], &interp_q15, 16);
    } else {
        out.pred_coef_q12[0] = out.pred_coef_q12[1];
    }

    if (front.indices.signal_type == .voiced) {
        try silkDecodePitchLags(&out.pitch_l, front.indices.lag_index, front.indices.contour_index, fs_khz);
        try silkDecodeLtpCoefficients(&out.ltp_coef_q14, front.indices.per_index, front.indices.ltp_index);
        out.ltp_scale_q14 = silk_tables.ltp_scales_table_q14[front.indices.ltp_scale_index];
    }

    silkDecodeExcitation(
        out.excitation_q14[0..front.pulse_len],
        front.pulses[0..front.pulse_len],
        out.quant_offset_q10,
        front.indices.seed,
    );

    state.prev_nlsf_q15 = out.nlsf_q15;
    state.first_frame_after_reset = false;
    return out;
}

pub fn decodeSilkPacketHeader(frame_bytes: []const u8, toc: Toc, channels_internal: u8) !SilkPacketHeader {
    if (toc.mode != .silk and toc.mode != .hybrid) return error.UnsupportedAudioFormat;
    if (channels_internal == 0 or channels_internal > 2) return error.UnsupportedAudioFormat;

    var decoder = RangeDecoder.init(frame_bytes);
    return silkDecodePacketHeaderFromDecoder(&decoder, toc, channels_internal);
}

pub fn decodeSilkPacketFront(frame_bytes: []const u8, toc: Toc, channels_internal: u8) !SilkPacketFront {
    if (toc.mode != .silk and toc.mode != .hybrid) return error.UnsupportedAudioFormat;
    if (channels_internal == 0 or channels_internal > 2) return error.UnsupportedAudioFormat;

    const fs_khz = try silkFsKhzForToc(toc);
    if (fs_khz != 8 and fs_khz != 12 and fs_khz != 16) return error.UnsupportedAudioFormat;

    var decoder = RangeDecoder.init(frame_bytes);
    return silkDecodePacketFrontFromDecoder(&decoder, toc, channels_internal, fs_khz);
}

fn silkSkipLbrrFrames(decoder: *RangeDecoder, toc: Toc, header: *const SilkPacketHeader, fs_khz: u8) !void {
    const frame_length = try silkFrameLengthSamples(toc, fs_khz);
    for (0..header.internal_frame_count) |frame_index| {
        const mid_present = header.channel_headers[0].lbrr_flags[frame_index];
        if (header.channels == 1) {
            if (!mid_present) continue;
            _ = try silkDecodeSingleFrameFront(
                decoder,
                true,
                frame_length,
                fs_khz,
                if (frame_index == 0) .independently else .conditionally,
            );
            continue;
        }

        const side_present = header.channel_headers[1].lbrr_flags[frame_index];
        if (!mid_present and !side_present) continue;

        _ = try silkDecodeStereoPred(decoder);
        if (!side_present) {
            _ = try silkDecodeMidOnly(decoder);
        }
        if (mid_present) {
            _ = try silkDecodeSingleFrameFront(
                decoder,
                true,
                frame_length,
                fs_khz,
                if (frame_index == 0) .independently else .conditionally,
            );
        }
        if (side_present) {
            _ = try silkDecodeSingleFrameFront(
                decoder,
                true,
                frame_length,
                fs_khz,
                .conditionally,
            );
        }
    }
}

fn silkDecodePacketFrontFromDecoder(decoder: *RangeDecoder, toc: Toc, channels_internal: u8, fs_khz: u8) !SilkPacketFront {
    const header = try silkDecodePacketHeaderFromDecoder(decoder, toc, channels_internal);
    try silkSkipLbrrFrames(decoder, toc, &header, fs_khz);

    var out = SilkPacketFront{ .header = header };
    const frame_length = try silkFrameLengthSamples(toc, fs_khz);
    for (0..header.internal_frame_count) |frame_index| {
        if (channels_internal == 2) {
            out.stereo_pred_q13[frame_index] = try silkDecodeStereoPred(decoder);
            if (!header.channel_headers[1].vad_flags[frame_index]) {
                out.decode_only_middle[frame_index] = try silkDecodeMidOnly(decoder);
            }
        }

        for (0..channels_internal) |channel| {
            if (channel == 1 and out.decode_only_middle[frame_index]) continue;
            const conditional: SilkCondCoding = if (channel != 0 or frame_index != 0) .conditionally else .independently;
            out.frames[channel][frame_index] = try silkDecodeSingleFrameFront(
                decoder,
                header.channel_headers[channel].vad_flags[frame_index],
                frame_length,
                fs_khz,
                conditional,
            );
            out.frame_present[channel][frame_index] = true;
        }
    }
    return out;
}

pub fn decodeSilkPacketParameters(frame_bytes: []const u8, toc: Toc, channels_internal: u8) !SilkPacketParameters {
    var decoder = RangeDecoder.init(frame_bytes);
    return decodeSilkPacketParametersFromDecoder(&decoder, toc, channels_internal);
}

fn decodeSilkPacketParametersFromDecoder(decoder: *RangeDecoder, toc: Toc, channels_internal: u8) !SilkPacketParameters {
    const fs_khz = try silkFsKhzForToc(toc);
    var out = SilkPacketParameters{
        .sample_rate = @as(u32, fs_khz) * 1000,
        .front = try silkDecodePacketFrontFromDecoder(decoder, toc, channels_internal, fs_khz),
    };
    var channel_states = [_]SilkChannelState{ .{}, .{} };
    for (0..out.front.header.internal_frame_count) |frame_index| {
        for (0..channels_internal) |channel| {
            if (!out.front.frame_present[channel][frame_index]) continue;
            out.frames[channel][frame_index] = try silkDecodeFrameParameters(
                &channel_states[channel],
                toc,
                &out.front.frames[channel][frame_index],
                channel != 0 or frame_index != 0,
            );
        }
    }
    return out;
}

fn silkRoundShift(value: i64, shift: u6) i32 {
    const add = @as(i64, 1) << (shift - 1);
    const shifted = (value + add) >> shift;
    return silkClamp32(shifted);
}

fn silkClamp16(value: i32) i16 {
    return @intCast(@max(@as(i32, std.math.minInt(i16)), @min(value, std.math.maxInt(i16))));
}

fn silkClamp32(value: i64) i32 {
    return @intCast(@max(@as(i64, std.math.minInt(i32)), @min(value, std.math.maxInt(i32))));
}

fn silkRoundClamp32(value: f64) i32 {
    if (value >= @as(f64, @floatFromInt(std.math.maxInt(i32)))) return std.math.maxInt(i32);
    if (value <= @as(f64, @floatFromInt(std.math.minInt(i32)))) return std.math.minInt(i32);
    return @intFromFloat(@round(value));
}

fn silkRoundClamp16(value: f64) i16 {
    if (value >= @as(f64, @floatFromInt(std.math.maxInt(i16)))) return std.math.maxInt(i16);
    if (value <= @as(f64, @floatFromInt(std.math.minInt(i16)))) return std.math.minInt(i16);
    return @intFromFloat(@round(value));
}

fn silkSynthesizeChannelInto(
    state: *SilkSynthChannelState,
    params: *const SilkFrameParameters,
    out: []i16,
) !void {
    const frame_length = params.excitation_len;
    if (out.len != frame_length or frame_length == 0 or frame_length > 320) {
        return error.UnsupportedAudioFormat;
    }

    var s_lpc_q14 = @as([(16 + 80)]i32, @splat(0));
    @memcpy(s_lpc_q14[0..16], &state.s_lpc_q14_buf);
    var s_ltp_q15 = @as([640]i32, @splat(0));
    @memcpy(s_ltp_q15[0..320], &state.history_q15);

    const subframe_count: usize = params.subframe_count;
    if (subframe_count == 0 or subframe_count > 4 or frame_length % subframe_count != 0) return error.UnsupportedAudioFormat;
    const subfr_length: usize = frame_length / subframe_count;
    if (subfr_length == 0 or subfr_length > 80) return error.UnsupportedAudioFormat;
    const order: usize = 16;
    for (0..subframe_count) |subframe| {
        const gain_q10: i32 = params.gains_q16[subframe] >> 6;
        const a_q12 = params.pred_coef_q12[subframe >> 1][0..order];
        const b_q14 = params.ltp_coef_q14[subframe * 5 .. subframe * 5 + 5];
        const lag: usize = if (params.signal_type == .voiced) @intCast(params.pitch_l[subframe]) else 0;

        for (0..subfr_length) |i| {
            const sample_index = subframe * subfr_length + i;
            const history_index = 320 + sample_index;

            var pres_q14 = params.excitation_q14[sample_index];
            if (params.signal_type == .voiced and lag > 0 and lag <= history_index + 2) {
                var ltp_pred_q13: i64 = 2;
                const ptr = history_index - lag + 2;
                ltp_pred_q13 += (@as(i64, s_ltp_q15[ptr]) * b_q14[0]) >> 16;
                ltp_pred_q13 += (@as(i64, s_ltp_q15[ptr - 1]) * b_q14[1]) >> 16;
                ltp_pred_q13 += (@as(i64, s_ltp_q15[ptr - 2]) * b_q14[2]) >> 16;
                ltp_pred_q13 += (@as(i64, s_ltp_q15[ptr - 3]) * b_q14[3]) >> 16;
                ltp_pred_q13 += (@as(i64, s_ltp_q15[ptr - 4]) * b_q14[4]) >> 16;
                pres_q14 = silkClamp32(@as(i64, pres_q14) + (ltp_pred_q13 << 1));
            }

            var lpc_pred_q10: i64 = order / 2;
            inline for (0..16) |tap| {
                lpc_pred_q10 += (@as(i64, s_lpc_q14[order + i - tap - 1]) * a_q12[tap]) >> 16;
            }

            const s_q14 = silkClamp32(@as(i64, pres_q14) + (lpc_pred_q10 << 4));
            s_lpc_q14[order + i] = s_q14;
            s_ltp_q15[history_index] = silkClamp32(@as(i64, s_q14) << 1);
            out[sample_index] = silkClamp16(silkRoundShift(@as(i64, s_q14) * gain_q10, 24));
        }

        @memcpy(s_lpc_q14[0..order], s_lpc_q14[subfr_length .. subfr_length + order]);
    }

    @memcpy(state.s_lpc_q14_buf[0..order], s_lpc_q14[0..order]);
    state.prev_gain_q16 = params.gains_q16[subframe_count - 1];
    const history_start = frame_length;
    for (0..320) |i| state.history_q15[i] = s_ltp_q15[history_start + i];
}

fn silkStereoMsToLr(
    state: *SilkStereoState,
    x1: []i16,
    x2: []i16,
    pred_q13: [2]i32,
    fs_khz: u8,
    frame_length: usize,
) !void {
    if (x1.len != frame_length + 2 or x2.len != frame_length + 2 or (fs_khz != 8 and fs_khz != 12 and fs_khz != 16)) return error.UnsupportedAudioFormat;

    x1[0] = state.s_mid[0];
    x1[1] = state.s_mid[1];
    x2[0] = state.s_side[0];
    x2[1] = state.s_side[1];
    state.s_mid[0] = x1[frame_length];
    state.s_mid[1] = x1[frame_length + 1];
    state.s_side[0] = x2[frame_length];
    state.s_side[1] = x2[frame_length + 1];

    var pred0_q13 = state.pred_prev_q13[0];
    var pred1_q13 = state.pred_prev_q13[1];
    const interp_len = 8 * fs_khz;
    const denom_q16: i32 = @intCast(@divTrunc(@as(i64, 1) << 16, @as(i64, interp_len)));
    const delta0_q13: i32 = @intCast((@as(i64, pred_q13[0] - state.pred_prev_q13[0]) * denom_q16 + (1 << 15)) >> 16);
    const delta1_q13: i32 = @intCast((@as(i64, pred_q13[1] - state.pred_prev_q13[1]) * denom_q16 + (1 << 15)) >> 16);

    for (0..interp_len) |n| {
        pred0_q13 += delta0_q13;
        pred1_q13 += delta1_q13;
        const sum_mid_q11 =
            (@as(i32, x1[n]) + @as(i32, x1[n + 2]) + (@as(i32, x1[n + 1]) << 1)) << 9;
        var sum_q8 = @as(i32, x2[n + 1]) << 8;
        sum_q8 += @intCast((@as(i64, sum_mid_q11) * pred0_q13) >> 16);
        sum_q8 += @intCast((@as(i64, (@as(i32, x1[n + 1]) << 11)) * pred1_q13) >> 16);
        x2[n + 1] = silkClamp16(silkRoundShift(sum_q8, 8));
    }
    pred0_q13 = pred_q13[0];
    pred1_q13 = pred_q13[1];
    for (interp_len..frame_length) |n| {
        const sum_mid_q11 =
            (@as(i32, x1[n]) + @as(i32, x1[n + 2]) + (@as(i32, x1[n + 1]) << 1)) << 9;
        var sum_q8 = @as(i32, x2[n + 1]) << 8;
        sum_q8 += @intCast((@as(i64, sum_mid_q11) * pred0_q13) >> 16);
        sum_q8 += @intCast((@as(i64, (@as(i32, x1[n + 1]) << 11)) * pred1_q13) >> 16);
        x2[n + 1] = silkClamp16(silkRoundShift(sum_q8, 8));
    }
    state.pred_prev_q13 = pred_q13;

    for (0..frame_length) |n| {
        const sum = @as(i32, x1[n + 1]) + x2[n + 1];
        const diff = @as(i32, x1[n + 1]) - x2[n + 1];
        x1[n + 1] = silkClamp16(sum);
        x2[n + 1] = silkClamp16(diff);
    }
}

fn silkSynthesizePacketAlloc(
    allocator: std.mem.Allocator,
    synth_state: *SilkSynthState,
    packet: *const SilkPacketParameters,
) !SilkSynthesizedPacket {
    if (synth_state.channels == 0 or synth_state.channels > 2) return error.UnsupportedAudioFormat;
    if (packet.sample_rate == 0) return error.UnsupportedAudioFormat;

    const internal_frame_count = packet.front.header.internal_frame_count;
    if (internal_frame_count == 0 or internal_frame_count > SilkMaxInternalFrames) return error.UnsupportedAudioFormat;

    const frame_length: usize = blk: {
        for (0..synth_state.channels) |channel| {
            for (0..internal_frame_count) |frame_index| {
                if (packet.front.frame_present[channel][frame_index]) {
                    break :blk packet.front.frames[channel][frame_index].pulse_len;
                }
            }
        }
        return error.UnsupportedAudioFormat;
    };
    const channels = synth_state.channels;
    const channel_samples = frame_length * internal_frame_count;
    var raw = try allocator.alloc(i16, channel_samples * channels);
    errdefer allocator.free(raw);
    @memset(raw, 0);

    for (0..internal_frame_count) |frame_index| {
        for (0..channels) |channel| {
            if (!packet.front.frame_present[channel][frame_index]) continue;
            if (packet.front.frames[channel][frame_index].pulse_len != frame_length) return error.UnsupportedAudioFormat;
            const start = channel * channel_samples + frame_index * frame_length;
            const slice = raw[start .. start + frame_length];
            try silkSynthesizeChannelInto(&synth_state.channel[channel], &packet.frames[channel][frame_index], slice);
        }
    }

    if (channels == 2) {
        var mid = try allocator.alloc(i16, frame_length + 2);
        defer allocator.free(mid);
        var side = try allocator.alloc(i16, frame_length + 2);
        defer allocator.free(side);
        @memset(mid, 0);
        @memset(side, 0);
        for (0..internal_frame_count) |frame_index| {
            @memset(mid, 0);
            @memset(side, 0);
            const mid_start = frame_index * frame_length;
            const side_start = channel_samples + frame_index * frame_length;
            @memcpy(mid[2 .. frame_length + 2], raw[mid_start .. mid_start + frame_length]);
            @memcpy(side[2 .. frame_length + 2], raw[side_start .. side_start + frame_length]);
            try silkStereoMsToLr(
                &synth_state.stereo,
                mid,
                side,
                packet.front.stereo_pred_q13[frame_index],
                @intCast(packet.sample_rate / 1000),
                frame_length,
            );
            @memcpy(raw[mid_start .. mid_start + frame_length], mid[1 .. frame_length + 1]);
            @memcpy(raw[side_start .. side_start + frame_length], side[1 .. frame_length + 1]);
        }
    }

    const pcm = try allocator.alloc(f32, channel_samples * channels);
    errdefer allocator.free(pcm);
    for (0..channel_samples) |sample_index| {
        for (0..channels) |channel| {
            const raw_index = channel * channel_samples + sample_index;
            pcm[sample_index * channels + channel] = @as(f32, @floatFromInt(raw[raw_index])) / 32768.0;
        }
    }
    allocator.free(raw);
    return .{
        .samples = pcm,
        .sample_rate = packet.sample_rate,
    };
}

fn silkSynthesizePacket16kAlloc(
    allocator: std.mem.Allocator,
    synth_state: *SilkSynthState,
    packet: *const SilkPacketParameters,
) ![]f32 {
    const synthesized = try silkSynthesizePacketAlloc(allocator, synth_state, packet);
    if (synthesized.sample_rate != 16_000) {
        allocator.free(synthesized.samples);
        return error.UnsupportedAudioFormat;
    }
    return synthesized.samples;
}

fn upsampleInterleavedTo48kAlloc(allocator: std.mem.Allocator, samples: []const f32, src_rate: u32, channels: u8) ![]f32 {
    if (channels == 0 or samples.len % channels != 0) return error.UnsupportedAudioFormat;
    const factor: usize = switch (src_rate) {
        8_000 => 6,
        12_000 => 4,
        16_000 => 3,
        else => return error.UnsupportedAudioFormat,
    };
    const in_frames = samples.len / channels;
    const out = try allocator.alloc(f32, in_frames * factor * channels);
    errdefer allocator.free(out);

    for (0..in_frames) |frame| {
        const next = @min(frame + 1, in_frames - 1);
        for (0..channels) |channel| {
            const a = samples[frame * channels + channel];
            const b = samples[next * channels + channel];
            const base = (frame * factor) * channels + channel;
            for (0..factor) |step| {
                const t = @as(f32, @floatFromInt(step)) / @as(f32, @floatFromInt(factor));
                out[base + step * channels] = a + (b - a) * t;
            }
        }
    }
    return out;
}

fn upsampleInterleaved16kTo48kAlloc(allocator: std.mem.Allocator, samples: []const f32, channels: u8) ![]f32 {
    return upsampleInterleavedTo48kAlloc(allocator, samples, 16_000, channels);
}

fn combineHybridLowbandWithHighbandAlloc(
    allocator: std.mem.Allocator,
    lowband_48k: []const f32,
    highband_48k: ?[]const f32,
) ![]f32 {
    const out = try allocator.dupe(f32, lowband_48k);
    errdefer allocator.free(out);
    if (highband_48k) |high| {
        if (high.len != out.len) return error.UnsupportedAudioFormat;
        for (out, high) |*dst, src| dst.* += src;
    }
    return out;
}

fn decodeHybridFrameAlloc(
    allocator: std.mem.Allocator,
    energy_state: *CeltEnergyState,
    synth_state: *CeltSynthState,
    silk_state: *SilkSynthState,
    frame_bytes: []const u8,
    toc: Toc,
    channels_internal: u8,
) ![]f32 {
    const plan = try sharedCeltSynthesisPlan();
    return decodeHybridFrameWithPlanAlloc(allocator, energy_state, synth_state, plan, silk_state, frame_bytes, toc, channels_internal);
}

fn decodeHybridFrameWithPlanAlloc(
    allocator: std.mem.Allocator,
    energy_state: *CeltEnergyState,
    synth_state: *CeltSynthState,
    celt_plan: *const CeltSynthesisPlan,
    silk_state: *SilkSynthState,
    frame_bytes: []const u8,
    toc: Toc,
    channels_internal: u8,
) ![]f32 {
    if (toc.mode != .hybrid) return error.UnsupportedAudioFormat;

    var decoder = RangeDecoder.init(frame_bytes);
    const silk_packet = try decodeSilkPacketParametersFromDecoder(&decoder, toc, channels_internal);
    const pcm_16k = try silkSynthesizePacket16kAlloc(allocator, silk_state, &silk_packet);
    defer allocator.free(pcm_16k);
    const lowband_48k = try upsampleInterleaved16kTo48kAlloc(allocator, pcm_16k, channels_internal);
    defer allocator.free(lowband_48k);

    const residual = decodeCeltResidualFrameFromDecoder(
        allocator,
        energy_state,
        &decoder,
        frame_bytes.len * 8,
        try lmForFrameDurationUs(toc.frame_duration_us),
        try celtStartBandForToc(toc),
        try endBandForBandwidth(toc.bandwidth),
    ) catch |err| {
        if (err == error.UnsupportedAudioFormat) {
            return combineHybridLowbandWithHighbandAlloc(allocator, lowband_48k, null);
        }
        return err;
    };
    const highband_48k = try synthesizeFrameWithPlanAlloc(allocator, synth_state, celt_plan, residual, toc);
    defer allocator.free(highband_48k);

    return combineHybridLowbandWithHighbandAlloc(allocator, lowband_48k, highband_48k);
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
            padding += padding_byte;
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

    var frame_lengths = @as([48]usize, @splat(0));
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

fn silkInternalFramesPerPacket(toc: Toc) !u8 {
    return switch (toc.mode) {
        .silk => switch (toc.frame_duration_us) {
            10_000, 20_000 => 1,
            40_000 => 2,
            60_000 => 3,
            else => error.UnsupportedAudioFormat,
        },
        .hybrid => switch (toc.frame_duration_us) {
            10_000, 20_000 => 1,
            else => error.UnsupportedAudioFormat,
        },
        else => error.UnsupportedAudioFormat,
    };
}

fn silkSubframesPerInternalFrame(toc: Toc) !u8 {
    return switch (toc.mode) {
        .silk, .hybrid => switch (toc.frame_duration_us) {
            10_000 => 2,
            20_000, 40_000, 60_000 => 4,
            else => error.UnsupportedAudioFormat,
        },
        else => error.UnsupportedAudioFormat,
    };
}

fn lmForFrameDurationUs(duration_us: u32) !u2 {
    return switch (duration_us) {
        2_500 => 0,
        5_000 => 1,
        10_000 => 2,
        20_000 => 3,
        else => error.UnsupportedAudioFormat,
    };
}

fn endBandForBandwidth(bandwidth: Bandwidth) !usize {
    return switch (bandwidth) {
        .nb => 13,
        .mb => 15,
        .wb => 17,
        .swb => 19,
        .fb => 21,
    };
}

fn celtStartBandForToc(toc: Toc) !usize {
    return switch (toc.mode) {
        .celt => 0,
        .hybrid => 17,
        else => error.UnsupportedAudioFormat,
    };
}

fn decodeCeltHeader(decoder: *RangeDecoder, total_bits: usize, lm: u2, start_band: usize) !CeltHeader {
    const tell0 = decoder.tell();
    const silence = if (tell0 >= total_bits)
        true
    else if (tell0 == 1)
        (try decoder.decodeBitLogp(15)) != 0
    else
        false;

    var has_postfilter = false;
    var postfilter_pitch: u16 = 0;
    var postfilter_gain: f32 = 0;
    var postfilter_tapset: u8 = 0;
    if (start_band == 0 and !silence and decoder.tell() + 16 <= total_bits) {
        has_postfilter = (try decoder.decodeBitLogp(1)) != 0;
        if (has_postfilter) {
            const octave = try decoder.decodeUint(6);
            if (octave > 5) return error.UnsupportedAudioFormat;
            const raw_pitch = try decoder.readRawBits(4 + octave);
            postfilter_pitch = @intCast((@as(u32, 16) << @intCast(octave)) + raw_pitch - 1);
            const qg = try decoder.readRawBits(3);
            if (decoder.tell() + 2 <= total_bits) {
                postfilter_tapset = try decoder.decodeIcdf(&CeltTapsetIcdf);
            }
            postfilter_gain = 0.09375 * (@as(f32, @floatFromInt(qg)) + 1.0);
        }
    }

    const is_transient = if (lm > 0 and decoder.tell() + 3 <= total_bits)
        (try decoder.decodeBitLogp(3)) != 0
    else
        false;
    const intra_energy = if (decoder.tell() + 3 <= total_bits)
        (try decoder.decodeBitLogp(3)) != 0
    else
        false;

    return .{
        .silence = silence,
        .has_postfilter = has_postfilter,
        .postfilter_pitch = postfilter_pitch,
        .postfilter_gain = postfilter_gain,
        .postfilter_tapset = postfilter_tapset,
        .is_transient = is_transient,
        .intra_energy = intra_energy,
    };
}

fn unquantCoarseEnergy(
    decoder: *RangeDecoder,
    total_bits: usize,
    state: *CeltEnergyState,
    start_band: usize,
    end_band: usize,
    intra: bool,
    lm: u2,
) !void {
    const model = CeltProbabilityModel.e_prob_model[lm][@intFromBool(intra)];
    const coef: f32 = if (intra) 0 else CeltProbabilityModel.pred_coef[lm];
    const beta: f32 = if (intra) CeltProbabilityModel.beta_intra else CeltProbabilityModel.beta_coef[lm];
    var prev = [_]f32{ 0, 0 };

    for (start_band..end_band) |band| {
        for (0..state.channels) |channel| {
            const qi: i32 = blk: {
                const budget_left = @as(i32, @intCast(total_bits)) - @as(i32, @intCast(decoder.tell()));
                if (budget_left >= 15) {
                    const pi = @min(band, @as(usize, 20)) << 1;
                    if (pi + 1 >= model.len) return error.UnsupportedAudioFormat;
                    break :blk try laplaceDecode(decoder, @as(u32, model[pi]) << 7, @as(u32, model[pi + 1]) << 6);
                }
                if (budget_left >= 2) {
                    const symbol = try decodePmf(decoder, &CeltSmallEnergyPmf);
                    break :blk switch (symbol) {
                        0 => 0,
                        1 => -1,
                        2 => 1,
                        else => return error.UnsupportedAudioFormat,
                    };
                }
                if (budget_left >= 1) {
                    break :blk -@as(i32, @intCast(try decoder.decodeBitLogp(1)));
                }
                break :blk -1;
            };

            const old = @max(-9.0, state.old_band_energies[channel][band]);
            const q = @as(f32, @floatFromInt(qi));
            const updated = coef * old + prev[channel] + q;
            state.old_band_energies[channel][band] = updated;
            prev[channel] = prev[channel] + q - beta * q;
        }
    }
}

fn decodePmf(decoder: *RangeDecoder, weights: []const u16) !u16 {
    if (weights.len == 0) return error.UnsupportedAudioFormat;
    var total: u16 = 0;
    for (weights) |weight| total += weight;
    const frequency = try decoder.getFrequency(total);
    var low: u16 = 0;
    for (weights, 0..) |weight, i| {
        const high = low + weight;
        if (frequency >= low and frequency < high) {
            try decoder.update(low, high, total);
            return @intCast(i);
        }
        low = high;
    }
    return error.UnsupportedAudioFormat;
}

fn decodeCeltResidualPlan(decoder: *RangeDecoder, total_bits: usize, lm: u2, start_band: usize, end_band: usize, channels: u8) !CeltResidualPlan {
    var tf_res = @as([21]i8, @splat(0));
    tfDecode(decoder, start_band, end_band, false, &tf_res, lm, total_bits);

    var spread_decision: u8 = 2;
    if (decoder.tell() + 4 <= total_bits) spread_decision = try decoder.decodeIcdf(&CeltSpreadIcdf);

    var offsets = @as([21]i16, @splat(0));
    var total_bits_frac: i32 = @intCast(total_bits << StandardCeltMode.bitres);
    var tell_frac: i32 = @intCast(decoder.tellFrac());
    var dynalloc_logp: u5 = 6;
    for (start_band..end_band) |band| {
        const width: i32 = @intCast(StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band]);
        const quanta = @min(width << StandardCeltMode.bitres, @max(6 << StandardCeltMode.bitres, width));
        var dynalloc_loop_logp = dynalloc_logp;
        var boost: i16 = 0;
        while (tell_frac + (@as(i32, dynalloc_loop_logp) << StandardCeltMode.bitres) < total_bits_frac and boost < standard_pulse_cache.caps[lm][0][band]) {
            const flag = try decoder.decodeBitLogp(dynalloc_loop_logp);
            tell_frac = @intCast(decoder.tellFrac());
            if (flag == 0) break;
            boost += @intCast(quanta);
            total_bits_frac -= quanta;
            dynalloc_loop_logp = 1;
        }
        offsets[band] = boost;
        if (boost > 0) dynalloc_logp = @max(2, dynalloc_logp - 1);
    }

    const alloc_trim: u8 = if (tell_frac + (6 << StandardCeltMode.bitres) <= total_bits_frac)
        try decoder.decodeIcdf(&CeltTrimIcdf)
    else
        5;

    var plan = CeltResidualPlan{
        .tf_res = tf_res,
        .spread_decision = spread_decision,
        .alloc_trim = alloc_trim,
        .coded_bands = 0,
    };
    try computeAllocation(
        decoder,
        lm,
        start_band,
        end_band,
        offsets[0..end_band],
        alloc_trim,
        total_bits_frac - @as(i32, @intCast(decoder.tellFrac())) - 1,
        channels,
        &plan,
    );
    return plan;
}

fn decodeCeltResidualBands(
    allocator: std.mem.Allocator,
    decoder: *RangeDecoder,
    lm: u2,
    start_band: usize,
    end_band: usize,
    channels: u8,
    plan: *const CeltResidualPlan,
    bands: *[2][21]CeltResidualBand,
) !void {
    _ = allocator;
    for (start_band..end_band) |band| {
        const len = celtBandWidth(lm, band);
        if (len > bands[0][band].coefficients.len) return error.UnsupportedAudioFormat;
        for (0..channels) |channel| bands[channel][band].len = len;
        if (plan.band_bits[band] == 0) {
            for (0..channels) |channel| @memset(bands[channel][band].coefficients[0..len], 0);
            continue;
        }
        if (channels == 1) {
            const pulse_count = getPulses(bits2Pulses(@intCast(band), lm, plan.band_bits[band]));
            try algUnquant(bands[0][band].coefficients[0..len], @intCast(pulse_count), plan.spread_decision, decoder);
        } else {
            if (plan.dual_stereo and band < plan.intensity) {
                const channel_bits = @divTrunc(plan.band_bits[band], 2);
                if (channel_bits <= 0) {
                    for (0..channels) |channel| @memset(bands[channel][band].coefficients[0..len], 0);
                    continue;
                }
                const pulse_count = getPulses(bits2Pulses(@intCast(band), lm, channel_bits));
                for (0..channels) |channel| {
                    try algUnquant(bands[channel][band].coefficients[0..len], @intCast(pulse_count), plan.spread_decision, decoder);
                }
            } else if (band < plan.intensity) {
                try decodeCoupledStereoBand(decoder, lm, band, plan.band_bits[band], plan.spread_decision, bands, len);
            } else if (band >= plan.intensity) {
                const pulse_count = getPulses(bits2Pulses(@intCast(band), lm, plan.band_bits[band]));
                algUnquant(bands[0][band].coefficients[0..len], @intCast(pulse_count), plan.spread_decision, decoder) catch {
                    @memset(bands[0][band].coefficients[0..len], 0);
                    @memset(bands[1][band].coefficients[0..len], 0);
                    continue;
                };
                @memcpy(bands[1][band].coefficients[0..len], bands[0][band].coefficients[0..len]);
            } else {
                return error.UnsupportedAudioFormat;
            }
        }
    }
}

fn decodeCoupledStereoBand(
    decoder: *RangeDecoder,
    lm: u2,
    band: usize,
    band_bits: i32,
    spread: u8,
    bands: *[2][21]CeltResidualBand,
    len: usize,
) !void {
    if (len == 0 or band_bits <= 0) return error.UnsupportedAudioFormat;

    const pulse_cap = @as(i32, celtLog2Frac(@intCast(len), StandardCeltMode.bitres)) + (@as(i32, @intCast(lm)) << StandardCeltMode.bitres);
    const offset = @divTrunc(pulse_cap, 2) - @as(i32, if (len == 2) StandardCeltMode.qtheta_offset_two_phase else StandardCeltMode.qtheta_offset);
    const qn = computeQn(len, band_bits, offset, pulse_cap, true);

    const tell_before = decoder.tellFrac();
    var itheta: i32 = 0;
    if (qn != 1) {
        if (len > 2) {
            const p0: i32 = 3;
            const x0 = @divTrunc(qn, 2);
            const ft = p0 * (x0 + 1) + x0;
            const fs = try decoder.getFrequency(@intCast(ft));
            const x: i32 = if (fs < (x0 + 1) * p0)
                @divTrunc(@as(i32, @intCast(fs)), p0)
            else
                x0 + 1 + (@as(i32, @intCast(fs)) - (x0 + 1) * p0);
            const low: i32 = if (x <= x0) p0 * x else (x - 1 - x0) + (x0 + 1) * p0;
            const high: i32 = if (x <= x0) p0 * (x + 1) else (x - x0) + (x0 + 1) * p0;
            try decoder.update(@intCast(low), @intCast(high), @intCast(ft));
            itheta = @divTrunc(x * 16384, qn);
        } else {
            itheta = @divTrunc(@as(i32, @intCast(try decoder.decodeUint(@intCast(qn + 1)))) * 16384, qn);
        }
    }
    const qalloc: i32 = @intCast(decoder.tellFrac() - tell_before);
    var payload_bits = band_bits - qalloc;
    if (payload_bits < 0) payload_bits = 0;

    const imid: i32 = if (itheta == 0)
        32767
    else if (itheta == 16384)
        0
    else
        bitexactCosApprox(itheta);
    const iside: i32 = if (itheta == 0)
        0
    else if (itheta == 16384)
        32767
    else
        bitexactCosApprox(16384 - itheta);
    const delta = if (itheta == 0)
        -16384
    else if (itheta == 16384)
        16384
    else
        @divTrunc((@as(i32, @intCast(len)) - 1) * 128 * bitexactLog2TanApprox(iside, imid), 32768);

    var mbits: i32 = @max(0, @min(payload_bits, @divTrunc(payload_bits - delta, 2)));
    var sbits: i32 = payload_bits - mbits;
    if (itheta == 0) {
        sbits = 0;
        mbits = payload_bits;
    } else if (itheta == 16384) {
        mbits = 0;
        sbits = payload_bits;
    }

    if (mbits > 0) {
        const pulse_count = getPulses(bits2Pulses(band, lm, mbits));
        if (pulse_count > 0) {
            try algUnquant(bands[0][band].coefficients[0..len], @intCast(pulse_count), spread, decoder);
        } else {
            @memset(bands[0][band].coefficients[0..len], 0);
        }
    } else {
        @memset(bands[0][band].coefficients[0..len], 0);
    }

    if (sbits > 0) {
        const pulse_count = getPulses(bits2Pulses(band, lm, sbits));
        if (pulse_count > 0) {
            try algUnquant(bands[1][band].coefficients[0..len], @intCast(pulse_count), spread, decoder);
        } else {
            @memset(bands[1][band].coefficients[0..len], 0);
        }
    } else {
        @memset(bands[1][band].coefficients[0..len], 0);
    }

    stereoMergeExact(
        bands[0][band].coefficients[0..len],
        bands[1][band].coefficients[0..len],
        @as(f32, @floatFromInt(imid)) / 32768.0,
        @as(f32, @floatFromInt(iside)) / 32768.0,
    );
}

fn computeQn(n: usize, bits: i32, offset: i32, pulse_cap: i32, stereo: bool) i32 {
    const exp2_table8 = [_]i32{ 16384, 17866, 19483, 21247, 23170, 25267, 27554, 30048 };
    var n2 = @as(i32, @intCast(2 * n - 1));
    if (stereo and n == 2) n2 -= 1;
    var qb = @min(bits - pulse_cap - (4 << StandardCeltMode.bitres), @divTrunc(bits + n2 * offset, n2));
    qb = @min(8 << StandardCeltMode.bitres, qb);
    if (qb < (1 << StandardCeltMode.bitres >> 1)) return 1;
    const idx: usize = @intCast(qb & 0x7);
    const shift: u5 = @intCast(14 - @divTrunc(qb, 1 << StandardCeltMode.bitres));
    const qn = (exp2_table8[idx] >> shift);
    return ((qn + 1) >> 1) << 1;
}

fn bitexactCosApprox(x: i32) i32 {
    const xf = @as(f64, @floatFromInt(x));
    const value = @cos((std.math.pi / 32768.0) * xf);
    return silkRoundClamp32(value * 32767.0);
}

fn bitexactLog2TanApprox(isin: i32, icos: i32) i32 {
    if (isin <= 0 or icos <= 0) return 0;
    const ratio = @as(f64, @floatFromInt(isin)) / @as(f64, @floatFromInt(icos));
    return silkRoundClamp32(std.math.log2(ratio) * 2048.0);
}

fn stereoMergeExact(left: []f32, right: []f32, mid: f32, side: f32) void {
    var xp: f32 = 0;
    var side_energy: f32 = 0;
    for (left, right) |l, r| {
        xp += l * r;
        side_energy += r * r;
    }
    xp *= mid;
    const mid2 = mid * 0.5;
    const el = mid2 * mid2 + side_energy - 2.0 * xp;
    const er = mid2 * mid2 + side_energy + 2.0 * xp;
    if (el < 6e-4 or er < 6e-4) {
        @memcpy(right, left);
        return;
    }
    const lgain = 1.0 / @sqrt(el);
    const rgain = 1.0 / @sqrt(er);
    for (left, right) |*l, *r| {
        const mid_v = mid * l.*;
        const side_v = side * r.*;
        l.* = (mid_v - side_v) * lgain;
        r.* = (mid_v + side_v) * rgain;
    }
}

fn unquantFineEnergy(decoder: *RangeDecoder, band_energies: *[2][21]f32, start_band: usize, end_band: usize, channels: u8, plan: *const CeltResidualPlan) !void {
    for (start_band..end_band) |band| {
        const extra = plan.fine_quant[band];
        if (extra == 0) continue;
        for (0..channels) |channel| {
            if (decoder.tell() + extra > decoder.bytes.len * 8) continue;
            const q2 = try decoder.readRawBits(extra);
            const denom: u32 = @as(u32, 1) << @intCast(extra);
            const offset = (((@as(f32, @floatFromInt(2 * q2 + 1))) / @as(f32, @floatFromInt(denom))) - 1.0) * 0.5;
            band_energies[channel][band] += offset;
        }
    }
}

fn unquantEnergyFinalise(
    decoder: *RangeDecoder,
    total_bits: usize,
    band_energies: *[2][21]f32,
    start_band: usize,
    end_band: usize,
    channels: u8,
    plan: *const CeltResidualPlan,
) !void {
    var bits_left = total_bits -| decoder.tell();
    for (0..2) |priority| {
        for (start_band..end_band) |band| {
            if (bits_left == 0) break;
            if (plan.fine_quant[band] >= StandardCeltMode.max_fine_bits) continue;
            if (plan.fine_priority[band] != (priority == 0)) continue;
            for (0..channels) |channel| {
                if (bits_left == 0) break;
                const q2 = try decoder.readRawBits(1);
                const denom: u32 = @as(u32, 1) << @intCast(plan.fine_quant[band] + 1);
                const offset = (@as(f32, @floatFromInt(q2)) - 0.5) / @as(f32, @floatFromInt(denom));
                band_energies[channel][band] += offset;
                bits_left -= 1;
            }
        }
    }
}

fn standardShortMdctSize() usize {
    return 120;
}

fn standardOverlap() usize {
    return 120;
}

fn celtBandWidth(lm: u2, band: usize) usize {
    return (StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band]) << @intCast(lm);
}

fn bandAmplitude(log_energy: f32, band: usize) f32 {
    return std.math.exp2(log_energy + CeltProbabilityModel.e_means[band]);
}

fn synthesizeChannelFrameAlloc(
    synth_state: *CeltSynthState,
    dct4_plan: *const CeltTransformPlan,
    channel: usize,
    frame: CeltResidualFrame,
    block: []f32,
    window: []const f32,
    coeffs: []f32,
    out: []f32,
    m: usize,
    n: usize,
    overlap: usize,
) !void {
    if (channel >= frame.channels or channel >= synth_state.channels) return error.UnsupportedAudioFormat;

    @memset(coeffs, 0);
    @memset(block, 0);
    @memset(out, 0);

    for (frame.start_band..frame.end_band) |band| {
        const band_start = StandardCeltMode.e_bands[band] * m;
        const band_len = frame.bands[channel][band].len;
        const amplitude = bandAmplitude(frame.band_energies[channel][band], band);
        if (band_start + band_len > coeffs.len) return error.UnsupportedAudioFormat;
        for (0..band_len) |i| {
            coeffs[band_start + i] = frame.bands[channel][band].coefficients[i] * amplitude;
        }
    }

    try celtDct4IntoWithPlan(block, coeffs, dct4_plan);

    for (0..overlap) |i| {
        out[i] = synth_state.overlap[channel][i] + block[i] * window[i];
    }
    if (n > overlap * 2) {
        @memcpy(out[overlap .. n - overlap], block[overlap .. n - overlap]);
    }
    for (0..overlap) |i| {
        synth_state.overlap[channel][i] = block[n - overlap + i] * window[overlap - 1 - i];
    }
}

fn synthesizeFrameAlloc(
    allocator: std.mem.Allocator,
    synth_state: *CeltSynthState,
    frame: CeltResidualFrame,
    toc: Toc,
) ![]f32 {
    const plan = try sharedCeltSynthesisPlan();
    return synthesizeFrameWithPlanAlloc(allocator, synth_state, plan, frame, toc);
}

fn synthesizeFrameWithPlanAlloc(
    allocator: std.mem.Allocator,
    synth_state: *CeltSynthState,
    plan: *const CeltSynthesisPlan,
    frame: CeltResidualFrame,
    toc: Toc,
) ![]f32 {
    if (frame.channels == 0 or frame.channels > 2 or frame.channels != synth_state.channels) return error.UnsupportedAudioFormat;
    if (frame.channels != (if (toc.stereo) @as(u8, 2) else @as(u8, 1))) return error.UnsupportedAudioFormat;

    const lm = try lmForFrameDurationUs(toc.frame_duration_us);
    const dct4_plan = plan.dct4PlanForLm(lm);
    const m: usize = @as(usize, 1) << @intCast(lm);
    const n = standardShortMdctSize() * m;
    const overlap = standardOverlap();

    const coeffs = try allocator.alloc(f32, n);
    defer allocator.free(coeffs);
    const block = try allocator.alloc(f32, n);
    defer allocator.free(block);
    const window = plan.window;

    if (frame.channels == 1) {
        const out = try allocator.alloc(f32, n);
        try synthesizeChannelFrameAlloc(synth_state, dct4_plan, 0, frame, block, window, coeffs, out, m, n, overlap);
        return out;
    }

    const channel_out = try allocator.alloc(f32, n);
    defer allocator.free(channel_out);

    const interleaved = try allocator.alloc(f32, n * frame.channels);
    @memset(interleaved, 0);
    for (0..frame.channels) |channel| {
        try synthesizeChannelFrameAlloc(synth_state, dct4_plan, channel, frame, block, window, coeffs, channel_out, m, n, overlap);
        var dst = channel;
        for (channel_out) |sample| {
            interleaved[dst] = sample;
            dst += frame.channels;
        }
    }
    return interleaved;
}

fn celtDct4Into(out: []f32, coefficients: []const f32) !void {
    if (out.len == 0 or out.len != coefficients.len) return error.UnsupportedAudioFormat;

    const n_f = @as(f32, @floatFromInt(out.len));
    const scale = @sqrt(2.0 / n_f);
    for (out, 0..) |*sample, n_idx| {
        const n_term = @as(f32, @floatFromInt(n_idx)) + 0.5;
        var accum: f32 = 0;
        for (coefficients, 0..) |coef, k_idx| {
            const k_term = @as(f32, @floatFromInt(k_idx)) + 0.5;
            accum += coef * @cos((std.math.pi / n_f) * n_term * k_term);
        }
        sample.* = accum * scale;
    }
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

fn downmixStereoOwnedToMonoAlloc(allocator: std.mem.Allocator, stereo_samples: []f32) ![]f32 {
    if (stereo_samples.len % 2 != 0) return error.UnsupportedAudioFormat;
    const frames = stereo_samples.len / 2;
    const mono = try allocator.alloc(f32, frames);
    for (0..frames) |frame| {
        mono[frame] = 0.5 * (stereo_samples[frame * 2] + stereo_samples[frame * 2 + 1]);
    }
    allocator.free(stereo_samples);
    return mono;
}

fn duplicateMonoOwnedToStereoAlloc(allocator: std.mem.Allocator, mono_samples: []f32) ![]f32 {
    const stereo = try allocator.alloc(f32, mono_samples.len * 2);
    for (stereo, 0..) |*sample, index| {
        sample.* = mono_samples[index / 2];
    }
    allocator.free(mono_samples);
    return stereo;
}

fn tfDecode(decoder: *RangeDecoder, start: usize, end: usize, is_transient: bool, out: *[21]i8, lm: u2, total_bits: usize) void {
    var budget = total_bits;
    var tell = decoder.tell();
    var logp: u5 = if (is_transient) 2 else 4;
    const tf_select_rsv = lm > 0 and tell + logp + 1 <= budget;
    if (tf_select_rsv) budget -= 1;
    var tf_changed = false;
    var curr: u1 = 0;
    for (start..end) |band| {
        if (tell + logp <= budget) {
            const bit = decoder.decodeBitLogp(logp) catch 0;
            curr ^= bit;
            tell = decoder.tell();
            tf_changed = tf_changed or curr != 0;
        }
        out[band] = @intCast(curr);
        logp = if (is_transient) 4 else 5;
    }
    var tf_select: usize = 0;
    const tf_base = 4 * @as(usize, @intFromBool(is_transient));
    const tf_changed_idx = @as(usize, @intFromBool(tf_changed));
    if (tf_select_rsv and CeltTfSelectTable[lm][tf_base + tf_changed_idx] !=
        CeltTfSelectTable[lm][tf_base + 2 + tf_changed_idx])
    {
        tf_select = decoder.decodeBitLogp(1) catch 0;
    }
    for (start..end) |band| {
        out[band] = CeltTfSelectTable[lm][tf_base + 2 * tf_select + @as(usize, @intCast(out[band]))];
    }
}

fn computeAllocation(
    decoder: *RangeDecoder,
    lm: u2,
    start_band: usize,
    end_band: usize,
    offsets: []const i16,
    alloc_trim: u8,
    total_in: i32,
    channels: u8,
    plan: *CeltResidualPlan,
) !void {
    if (channels == 0 or channels > 2 or offsets.len < end_band) return error.UnsupportedAudioFormat;

    var total: i32 = @max(total_in, 0);
    const alloc_floor = @as(i32, @intCast(channels)) << StandardCeltMode.bitres;
    const skip_rsv: i32 = if (total >= (1 << StandardCeltMode.bitres)) (1 << StandardCeltMode.bitres) else 0;
    total -= skip_rsv;

    var intensity_rsv: i32 = 0;
    var dual_stereo_rsv: i32 = 0;
    if (channels == 2) {
        intensity_rsv = CeltLog2FracTable[end_band - start_band];
        if (intensity_rsv > total) {
            intensity_rsv = 0;
        } else {
            total -= intensity_rsv;
            dual_stereo_rsv = if (total >= (1 << StandardCeltMode.bitres)) (1 << StandardCeltMode.bitres) else 0;
            total -= dual_stereo_rsv;
        }
    }

    var bits1 = @as([21]i32, @splat(0));
    var bits2 = @as([21]i32, @splat(0));
    var thresh = @as([21]i32, @splat(0));
    var trim_offset = @as([21]i32, @splat(0));
    var skip_start: usize = start_band;

    for (start_band..end_band) |band| {
        const width = StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band];
        thresh[band] = @max(
            alloc_floor,
            (3 * @as(i32, @intCast(width)) * @as(i32, @intCast(channels)) << @intCast(lm) << StandardCeltMode.bitres) >> 4,
        );
        trim_offset[band] =
            @as(i32, @intCast(channels)) *
            @as(i32, @intCast(width)) *
            (@as(i32, alloc_trim) - 5 - @as(i32, @intCast(lm))) *
            @as(i32, @intCast(end_band - band - 1)) *
            (@as(i32, 1) << (@as(u5, @intCast(lm)) + StandardCeltMode.bitres)) >> 6;
        if ((width << @intCast(lm)) == 1) trim_offset[band] -= alloc_floor;
    }

    var lo: i32 = 1;
    var hi: i32 = StandardCeltMode.nb_alloc_vectors - 1;
    while (lo <= hi) {
        const mid = @divTrunc(lo + hi, 2);
        var done = false;
        var psum: i32 = 0;
        var band_idx: usize = end_band;
        while (band_idx > start_band) {
            band_idx -= 1;
            const width = StandardCeltMode.e_bands[band_idx + 1] - StandardCeltMode.e_bands[band_idx];
            var bitsj =
                @as(i32, @intCast(channels)) *
                @as(i32, @intCast(width)) *
                StandardCeltMode.band_allocation[@intCast(mid)][band_idx] <<
                @intCast(lm) >> 2;
            if (bitsj > 0) bitsj = @max(0, bitsj + trim_offset[band_idx]);
            bitsj += offsets[band_idx];
            if (bitsj >= thresh[band_idx] or done) {
                done = true;
                psum += @min(bitsj, @as(i32, standard_pulse_cache.caps[lm][channels - 1][band_idx]));
            } else if (bitsj >= alloc_floor) {
                psum += alloc_floor;
            }
        }
        if (psum > total) {
            hi = mid - 1;
        } else {
            lo += 1;
        }
    }
    hi = lo;
    lo -= 1;

    for (start_band..end_band) |band| {
        const width = StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band];
        var b1 =
            @as(i32, @intCast(channels)) *
            @as(i32, @intCast(width)) *
            StandardCeltMode.band_allocation[@intCast(lo)][band] <<
            @intCast(lm) >> 2;
        var b2 = if (hi >= StandardCeltMode.nb_alloc_vectors)
            @as(i32, standard_pulse_cache.caps[lm][channels - 1][band])
        else
            @as(i32, @intCast(channels)) *
                @as(i32, @intCast(width)) *
                StandardCeltMode.band_allocation[@intCast(hi)][band] <<
                @intCast(lm) >> 2;
        if (b1 > 0) b1 = @max(0, b1 + trim_offset[band]);
        if (b2 > 0) b2 = @max(0, b2 + trim_offset[band]);
        if (lo > 0) b1 += offsets[band];
        b2 += offsets[band];
        if (offsets[band] > 0) skip_start = band;
        bits1[band] = b1;
        bits2[band] = @max(0, b2 - b1);
    }

    var lo_step: i32 = 0;
    var hi_step: i32 = 1 << 6;
    while (lo_step <= hi_step) {
        const mid = @divTrunc(lo_step + hi_step, 2);
        var psum: i32 = 0;
        var done = false;
        var band_idx: usize = end_band;
        while (band_idx > start_band) {
            band_idx -= 1;
            var band_bits = bits1[band_idx] + ((mid * bits2[band_idx]) >> 6);
            if (band_bits < thresh[band_idx] and !done) {
                if (band_bits >= alloc_floor) {
                    band_bits = alloc_floor;
                } else {
                    band_bits = 0;
                }
            } else {
                done = true;
            }
            band_bits = @min(band_bits, @as(i32, standard_pulse_cache.caps[lm][channels - 1][band_idx]));
            psum += band_bits;
        }
        if (psum > total) {
            hi_step = mid - 1;
        } else {
            lo_step += 1;
        }
    }

    var psum: i32 = 0;
    {
        var done = false;
        var band_idx: usize = end_band;
        while (band_idx > start_band) {
            band_idx -= 1;
            var band_bits = bits1[band_idx] + ((hi_step * bits2[band_idx]) >> 6);
            if (band_bits < thresh[band_idx] and !done) {
                if (band_bits >= alloc_floor) {
                    band_bits = alloc_floor;
                } else {
                    band_bits = 0;
                }
            } else {
                done = true;
            }
            band_bits = @min(band_bits, @as(i32, standard_pulse_cache.caps[lm][channels - 1][band_idx]));
            plan.band_bits[band_idx] = band_bits;
            psum += band_bits;
        }
    }

    var coded_bands = end_band;
    while (true) {
        const band = coded_bands - 1;
        if (band <= skip_start) {
            total += skip_rsv;
            break;
        }

        var left = total - psum;
        const span = StandardCeltMode.e_bands[coded_bands] - StandardCeltMode.e_bands[start_band];
        const percoeff = @divTrunc(left, @as(i32, @intCast(span)));
        left -= @as(i32, @intCast(span)) * percoeff;
        const rem = @max(left - @as(i32, @intCast(StandardCeltMode.e_bands[band] - StandardCeltMode.e_bands[start_band])), 0);
        const band_width = StandardCeltMode.e_bands[coded_bands] - StandardCeltMode.e_bands[band];
        var band_bits = plan.band_bits[band] + percoeff * @as(i32, @intCast(band_width)) + rem;

        if (band_bits >= @max(thresh[band], alloc_floor + (1 << StandardCeltMode.bitres))) {
            if ((try decoder.decodeBitLogp(1)) != 0) break;
            psum += 1 << StandardCeltMode.bitres;
            band_bits -= 1 << StandardCeltMode.bitres;
        }

        psum -= plan.band_bits[band] + intensity_rsv;
        if (intensity_rsv > 0) intensity_rsv = CeltLog2FracTable[band - start_band];
        psum += intensity_rsv;

        if (band_bits >= alloc_floor) {
            psum += alloc_floor;
            plan.band_bits[band] = alloc_floor;
        } else {
            plan.band_bits[band] = 0;
        }
        coded_bands -= 1;
    }
    if (coded_bands <= start_band) return error.UnsupportedAudioFormat;

    if (intensity_rsv > 0) {
        plan.intensity = @intCast(start_band + try decoder.decodeUint(@intCast(coded_bands + 1 - start_band)));
    } else {
        plan.intensity = 0;
    }
    if (plan.intensity <= start_band) {
        total += dual_stereo_rsv;
        dual_stereo_rsv = 0;
    }
    if (dual_stereo_rsv > 0) {
        plan.dual_stereo = (try decoder.decodeBitLogp(1)) != 0;
    } else {
        plan.dual_stereo = false;
    }

    var left = total - psum;
    const coded_span = StandardCeltMode.e_bands[coded_bands] - StandardCeltMode.e_bands[start_band];
    const percoeff = @divTrunc(left, @as(i32, @intCast(coded_span)));
    left -= @as(i32, @intCast(coded_span)) * percoeff;
    for (start_band..coded_bands) |band| {
        plan.band_bits[band] += percoeff * @as(i32, @intCast(StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band]));
    }
    for (start_band..coded_bands) |band| {
        const extra = @min(left, @as(i32, @intCast(StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band])));
        plan.band_bits[band] += extra;
        left -= extra;
    }

    var balance: i32 = 0;
    const stereo = channels == 2;
    const fine_den_shift: u5 = @intCast(@as(u8, @intFromBool(stereo)) + StandardCeltMode.bitres);
    const log_m = @as(i32, @intCast(lm)) << StandardCeltMode.bitres;
    for (start_band..coded_bands) |band| {
        const n0 = StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band];
        const n = n0 << @intCast(lm);
        const bit = plan.band_bits[band] + balance;
        if (n > 1) {
            var excess: i32 = @max(bit - @as(i32, standard_pulse_cache.caps[lm][channels - 1][band]), 0);
            plan.band_bits[band] = bit - excess;

            const den =
                @as(i32, @intCast(channels)) * @as(i32, @intCast(n)) +
                @as(i32, if (channels == 2 and n > 2 and !plan.dual_stereo and band < plan.intensity) 1 else 0);
            const nclogn = den * (@as(i32, celtLog2Frac(n0, StandardCeltMode.bitres)) + log_m);
            var offset = @divTrunc(nclogn, 2) - den * StandardCeltMode.fine_offset;
            if (n == 2) offset += den << StandardCeltMode.bitres >> 2;
            if (plan.band_bits[band] + offset < den * 2 << StandardCeltMode.bitres) {
                offset += @divTrunc(nclogn, 4);
            } else if (plan.band_bits[band] + offset < den * 3 << StandardCeltMode.bitres) {
                offset += @divTrunc(nclogn, 8);
            }

            var ebits: i32 = @max(0, @divTrunc(plan.band_bits[band] + offset + (den << (StandardCeltMode.bitres - 1)), den << StandardCeltMode.bitres));
            const fine_budget = @divTrunc(plan.band_bits[band], @as(i32, 1) << fine_den_shift);
            if (@as(i32, @intCast(channels)) * ebits > fine_budget) {
                ebits = fine_budget;
            }
            ebits = @min(ebits, StandardCeltMode.max_fine_bits);
            plan.fine_quant[band] = @intCast(ebits);
            plan.fine_priority[band] = ebits * (den << StandardCeltMode.bitres) >= plan.band_bits[band] + offset;
            plan.band_bits[band] -= @as(i32, @intCast(channels)) * ebits << StandardCeltMode.bitres;

            if (excess > 0) {
                const extra_fine = @min(@divTrunc(excess, @as(i32, 1) << fine_den_shift), StandardCeltMode.max_fine_bits - ebits);
                plan.fine_quant[band] += @intCast(extra_fine);
                const extra_bits = extra_fine * @as(i32, @intCast(channels)) << StandardCeltMode.bitres;
                plan.fine_priority[band] = extra_bits >= excess - balance;
                excess -= extra_bits;
            }
            balance = excess;
        } else {
            const excess: i32 = @max(bit - (@as(i32, @intCast(channels)) << StandardCeltMode.bitres), 0);
            plan.band_bits[band] = bit - excess;
            plan.fine_quant[band] = 0;
            plan.fine_priority[band] = true;
            balance = excess;
        }
    }
    for (coded_bands..end_band) |band| {
        plan.fine_quant[band] = @intCast(@divTrunc(plan.band_bits[band], @as(i32, 1) << fine_den_shift));
        plan.band_bits[band] = 0;
        plan.fine_priority[band] = plan.fine_quant[band] < 1;
    }

    plan.coded_bands = coded_bands;
    for (start_band..coded_bands) |band| {
        if (plan.band_bits[band] < @max(thresh[band], alloc_floor)) {
            plan.pulses[band] = 0;
            continue;
        }
        const pulse_bits = if (channels == 2 and plan.dual_stereo and band < plan.intensity)
            @divTrunc(plan.band_bits[band], 2)
        else
            plan.band_bits[band];
        const q = bits2Pulses(band, lm, pulse_bits);
        plan.pulses[band] = @intCast(q);
    }
}

fn computeAllocationMono(lm: u2, end_band: usize, offsets: []const i16, alloc_trim: u8, total: i32, plan: *CeltResidualPlan) void {
    var thresh = @as([21]i32, @splat(0));
    var trim_offset = @as([21]i32, @splat(0));
    for (0..end_band) |band| {
        const width = StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band];
        thresh[band] = @max(1 << StandardCeltMode.bitres, (3 * @as(i32, @intCast(width)) << @intCast(lm) << StandardCeltMode.bitres) >> 4);
        trim_offset[band] = @as(i32, @intCast(width)) * (@as(i32, alloc_trim) - 5 - @as(i32, lm)) * @as(i32, @intCast(end_band - band - 1)) * (@as(i32, 1) << (@as(u5, @intCast(lm)) + StandardCeltMode.bitres)) >> 6;
        if ((width << @intCast(lm)) == 1) trim_offset[band] -= 1 << StandardCeltMode.bitres;
    }

    var bits1 = @as([21]i32, @splat(0));
    var bits2 = @as([21]i32, @splat(0));
    var lo: i32 = 1;
    var hi: i32 = StandardCeltMode.nb_alloc_vectors - 1;
    while (lo <= hi) {
        const mid = @divTrunc(lo + hi, 2);
        var psum: i32 = 0;
        var done = false;
        var band_idx: usize = end_band;
        while (band_idx > 0) {
            band_idx -= 1;
            const width = StandardCeltMode.e_bands[band_idx + 1] - StandardCeltMode.e_bands[band_idx];
            var bitsj = @as(i32, @intCast(width)) * StandardCeltMode.band_allocation[@intCast(mid)][band_idx] << @intCast(lm) >> 2;
            if (bitsj > 0) bitsj = @max(0, bitsj + trim_offset[band_idx]);
            bitsj += offsets[band_idx];
            if (bitsj >= thresh[band_idx] or done) {
                done = true;
                psum += @min(bitsj, @as(i32, standard_pulse_cache.caps[lm][0][band_idx]));
            } else if (bitsj >= 1 << StandardCeltMode.bitres) {
                psum += 1 << StandardCeltMode.bitres;
            }
        }
        if (psum > total) hi = mid - 1 else lo += 1;
    }
    hi = lo;
    lo -= 1;

    for (0..end_band) |band| {
        const width = StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band];
        var b1 = @as(i32, @intCast(width)) * StandardCeltMode.band_allocation[@intCast(lo)][band] << @intCast(lm) >> 2;
        var b2 = if (hi >= StandardCeltMode.nb_alloc_vectors)
            @as(i32, standard_pulse_cache.caps[lm][0][band])
        else
            @as(i32, @intCast(width)) * StandardCeltMode.band_allocation[@intCast(hi)][band] << @intCast(lm) >> 2;
        if (b1 > 0) b1 = @max(0, b1 + trim_offset[band]);
        if (b2 > 0) b2 = @max(0, b2 + trim_offset[band]);
        if (lo > 0) b1 += offsets[band];
        b2 += offsets[band];
        b2 = @max(0, b2 - b1);
        bits1[band] = b1;
        bits2[band] = b2;
    }

    const total_bits = total;
    var lo_step: i32 = 0;
    var hi_step: i32 = 1 << 6;
    while (lo_step <= hi_step) {
        const mid = @divTrunc(lo_step + hi_step, 2);
        var psum: i32 = 0;
        for (0..end_band) |band| {
            var band_bits = bits1[band] + ((mid * bits2[band] + (1 << 5)) >> 6);
            band_bits = @min(band_bits, @as(i32, standard_pulse_cache.caps[lm][0][band]));
            if (band_bits >= @max(thresh[band], 1 << StandardCeltMode.bitres)) {
                psum += @max(0, band_bits);
            } else {
                psum += @min(@max(0, band_bits), 1 << StandardCeltMode.bitres);
            }
        }
        if (psum > total_bits) hi_step = mid - 1 else lo_step = mid + 1;
    }
    const interp = hi_step;

    for (0..end_band) |band| {
        var band_bits = bits1[band] + ((interp * bits2[band] + (1 << 5)) >> 6);
        band_bits = @min(band_bits, @as(i32, standard_pulse_cache.caps[lm][0][band]));
        if (band_bits < @max(thresh[band], 1 << StandardCeltMode.bitres)) {
            plan.pulses[band] = 0;
            continue;
        }
        const q = bits2Pulses(@intCast(band), lm, band_bits);
        plan.pulses[band] = @intCast(q);
        plan.coded_bands = band + 1;
        const pulse_bits = pulses2Bits(@intCast(band), lm, q);
        var remaining = band_bits - pulse_bits;
        const width = StandardCeltMode.e_bands[band + 1] - StandardCeltMode.e_bands[band];
        const log_m = @as(i32, @intCast(lm)) << StandardCeltMode.bitres;
        const offset = @divTrunc(@as(i32, celtLog2Frac(width, StandardCeltMode.bitres)) + log_m, 2) - StandardCeltMode.fine_offset;
        const n = @as(i32, @intCast(width)) << @intCast(lm);
        const den = (n - 1) << StandardCeltMode.bitres;
        const fine = if (den > 0) @min(@divTrunc(remaining + @divTrunc(den, 2) + @as(i32, offset) * (@as(i32, @intCast(width)) << @intCast(lm)), den), StandardCeltMode.max_fine_bits) else 0;
        plan.fine_quant[band] = @intCast(@max(0, fine));
        remaining -= @as(i32, plan.fine_quant[band]) << StandardCeltMode.bitres;
        plan.fine_priority[band] = remaining > 0;
    }
}

fn bits2Pulses(band: usize, lm: u2, bits: i32) u8 {
    const lm_index = @as(usize, lm);
    const count = standard_pulse_cache.counts[lm_index][band];
    if (count == 0) return 0;
    var lo: i32 = 0;
    var hi: i32 = count;
    const target = bits - 1;
    var i: usize = 0;
    while (i < 6) : (i += 1) {
        const mid = @divTrunc(lo + hi + 1, 2);
        if (standard_pulse_cache.bits[lm_index][band][@intCast(mid)] >= target) hi = mid else lo = mid;
    }
    const lo_bits: i32 = if (lo == 0) -1 else standard_pulse_cache.bits[lm_index][band][@intCast(lo)];
    const hi_bits: i32 = standard_pulse_cache.bits[lm_index][band][@intCast(hi)];
    return @intCast(if (target - lo_bits <= hi_bits - target) lo else hi);
}

fn pulses2Bits(band: usize, lm: u2, pulses: u8) i32 {
    const lm_index = @as(usize, lm);
    return if (pulses == 0) 0 else standard_pulse_cache.bits[lm_index][band][pulses] + 1;
}

fn getPulses(i: u8) u16 {
    return if (i < 8) i else (8 + (i & 7)) << @intCast((i >> 3) - 1);
}

fn fitsIn32(n: i32, k: u16) bool {
    const max_n = [_]i16{ 32767, 32767, 32767, 1476, 283, 109, 60, 40, 29, 24, 20, 18, 16, 14, 13 };
    const max_k = [_]i16{ 32767, 32767, 32767, 32767, 1172, 238, 95, 53, 36, 27, 22, 18, 16, 15, 13 };
    if (n >= 14) {
        if (k >= 14) return false;
        return n <= max_n[k];
    }
    return k <= max_k[@intCast(n)];
}

fn celtLog2Frac(val: u32, frac: u8) u8 {
    if (val == 0) return 0;
    if ((val & (val - 1)) == 0) return (ilog(val) - 1) << @intCast(frac);
    const lf = std.math.log2(@as(f64, @floatFromInt(val)));
    const scaled = @as(i32, @intFromFloat(@ceil(lf * @as(f64, @floatFromInt(@as(u32, 1) << @intCast(frac))))));
    return @intCast(@min(scaled, std.math.maxInt(u8)));
}

fn getRequiredBits(bits: *[129]u8, n: i32, max_k: u16, frac: u8) void {
    bits[0] = 0;
    if (n == 1) {
        for (1..max_k + 1) |k| bits[k] = 1 << @intCast(frac);
        return;
    }
    var vals = @as([131]u32, @splat(0));
    _ = ncwrsUrow(@intCast(n), max_k, vals[0 .. max_k + 2]);
    for (1..max_k + 1) |k| bits[k] = celtLog2Frac(@intCast(vals[k] + vals[k + 1]), frac);
}

fn algUnquant(out: []f32, pulse_count: u16, spread: u8, decoder: *RangeDecoder) !void {
    if (pulse_count == 0) {
        @memset(out, 0);
        return;
    }
    if (pulse_count > out.len * 2) {
        @memset(out, 0);
        return;
    }
    if (out.len == 1) {
        out[0] = if ((try decoder.decodeBitLogp(1)) != 0) -1.0 else 1.0;
        return;
    }
    if (out.len == 0) return error.UnsupportedAudioFormat;
    const effective_pulse_count: u16 = @min(pulse_count, @as(u16, @intCast(out.len)));
    if (out.len > 176) return error.UnsupportedAudioFormat;
    var iy_buf = @as([176]i32, @splat(0));
    const iy = iy_buf[0..out.len];
    const ryy = try decodePulses(iy, effective_pulse_count, decoder);
    normalizeResidual(iy, out, ryy);
    expRotation(out, spread, effective_pulse_count);
}

fn normalizeResidual(iy: []const i32, out: []f32, ryy: i32) void {
    const norm = if (ryy <= 0) 0.0 else 1.0 / @sqrt(@as(f32, @floatFromInt(ryy)));
    for (out, iy) |*sample, val| sample.* = @as(f32, @floatFromInt(val)) * norm;
}

fn expRotation(out: []f32, spread: u8, pulse_count: u16) void {
    _ = spread;
    _ = pulse_count;
    _ = out;
}

fn decodePulses(out: []i32, pulse_count: u16, decoder: *RangeDecoder) !i32 {
    if (pulse_count > 1022) return error.UnsupportedAudioFormat;
    var u_buf = @as([1024]u32, @splat(0));
    const u = u_buf[0 .. pulse_count + 2];
    const total = ncwrsUrow(@intCast(out.len), pulse_count, u);
    const index = try decoder.decodeUint(total);
    return try cwrsi(@intCast(out.len), pulse_count, index, out, u);
}

fn ncwrsUrow(n: usize, k: u16, u: []u32) u32 {
    const len = k + 2;
    u[0] = 0;
    u[1] = 1;
    var idx: usize = 2;
    while (idx < len) : (idx += 1) u[idx] = @intCast((idx << 1) - 1);
    var row: usize = 2;
    while (row < n) : (row += 1) unext(u[1..len], 1);
    return u[k] + u[k + 1];
}

fn unext(u: []u32, carry_in: u32) void {
    var prev0 = carry_in;
    var j: usize = 1;
    while (j < u.len) : (j += 1) {
        const ui1 = u[j];
        u[j] = @addWithOverflow(@addWithOverflow(u[j], u[j - 1])[0], prev0)[0];
        prev0 = ui1;
    }
}

fn uprev(u: []u32, carry_in: u32) void {
    var prev0 = carry_in;
    var j: usize = 1;
    while (j < u.len) : (j += 1) {
        const ui1 = @subWithOverflow(@subWithOverflow(u[j], u[j - 1])[0], prev0)[0];
        u[j - 1] = prev0;
        prev0 = ui1;
    }
    u[u.len - 1] = prev0;
}

fn cwrsi(n: usize, pulse_count: u16, index_in: u32, out: []i32, u: []u32) !i32 {
    var index = index_in;
    var k = pulse_count;
    var yy: u32 = 0;
    var j: usize = 0;
    while (j < n) : (j += 1) {
        var p = u[k + 1];
        const negative = index >= p;
        if (negative) index -= p;
        var yj = k;
        p = u[k];
        while (p > index) {
            if (k == 0) return error.UnsupportedAudioFormat;
            k -= 1;
            p = u[k];
        }
        index -= p;
        yj -= k;
        if (yj > pulse_count) return error.UnsupportedAudioFormat;
        const magnitude: i32 = @intCast(yj);
        const value: i32 = if (negative) -magnitude else magnitude;
        out[j] = value;
        yy += @as(u32, @intCast(magnitude * magnitude));
        uprev(u[0 .. k + 2], 0);
    }
    return @intCast(yy);
}

fn laplaceDecode(decoder: *RangeDecoder, fs_in: u32, decay_in: u32) !i32 {
    var fs = fs_in;
    const decay: i32 = @intCast(decay_in);
    var value: i32 = 0;
    const fm = try decoder.getFrequency(32768);
    var fl: u32 = 0;
    if (fm >= fs) {
        value += 1;
        fl = fs;
        fs = laplaceGetFreq1(fs, decay) + 1;
        while (fs > 1 and fm >= fl + 2 * fs) {
            fs *= 2;
            fl += fs;
            fs = @intCast((@as(u64, fs - 2) * @as(u64, @intCast(decay))) >> 15);
            fs += 1;
            value += 1;
        }
        if (fs <= 1) {
            const di = (fm - fl) >> 1;
            value += @intCast(di);
            fl += 2 * di;
        }
        if (fm >= fl + fs) {
            fl += fs;
        } else {
            value = -value;
        }
    }
    try decoder.update(@intCast(fl), @intCast(@min(fl + fs, 32768)), 32768);
    return value;
}

fn laplaceGetFreq1(fs0: u32, decay: i32) u32 {
    const ft = 32768 - 32 - fs0;
    return @intCast((@as(u64, ft) * @as(u64, @intCast(16384 - decay))) >> 15);
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

const silk_probe_packet = [_]u8{
    0x4b, 0x41, 0x02, 0x82, 0x02, 0x7a, 0xf9, 0xb8, 0xcd, 0x4f,
    0xe5, 0x0b, 0xe9, 0x7f, 0xde, 0x98, 0x02, 0xe6, 0xd3, 0x88,
    0xe3, 0x57, 0x54, 0xef, 0xf9, 0x18, 0x43, 0xd8, 0x00, 0x00,
};

const hybrid_probe_packet = [_]u8{
    0x7c, 0x87, 0xfc, 0xb8, 0x5f, 0x80, 0x6b, 0xc7, 0x3a, 0x0a,
    0x74, 0x19, 0x31, 0x53, 0xca, 0x9f, 0xe1, 0x6f, 0x55, 0xbf,
    0xb2, 0x1f, 0x97, 0x16, 0x73, 0x0d, 0x5a, 0x3c, 0x56, 0xf2,
    0x98, 0x00, 0xb0, 0xb6, 0x74, 0x60, 0x2b, 0x82, 0x03, 0x68,
    0x58, 0x04, 0x36, 0xde, 0x31, 0x0b, 0x3d, 0x71, 0xcf, 0x20,
    0xa5, 0x84, 0xed, 0x85, 0x04, 0x55, 0x4c, 0xba, 0xc1, 0x07,
    0xe6, 0xd7, 0xb5, 0xe3, 0x17, 0x30, 0xe7, 0xb7, 0x9e, 0x2c,
    0xfa, 0x25, 0xd2, 0x2a, 0x6a, 0x99, 0x1d, 0xd6, 0x6a, 0xe5,
    0x85, 0x03, 0x21, 0x74, 0xae, 0x1d, 0x6a, 0x90, 0x91, 0x88,
    0x01, 0x98, 0xcd, 0xae, 0x7d, 0x9f, 0xfe, 0x93, 0x69, 0x3f,
};

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
        try std.testing.expectEqual(@as(u16, 312), demuxed.packet_sample_counts[demuxed.packet_sample_counts.len - 1]);
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
        try std.testing.expectEqual(@as(u32, 0x3fff_ffff), decoder.val);
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

    try std.testing.expectEqual(@as(u16, 1), try decoder.decodeSymbol(&cumulative));
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

test "celt basic window is symmetric and power complementary" {
    const window = try celtBasicWindowAlloc(std.testing.allocator, 120);
    defer std.testing.allocator.free(window);

    try std.testing.expect(window[0] > 0);
    try std.testing.expect(window[window.len - 1] < 1);

    for (window, 0..) |sample, i| {
        const mirrored = window[window.len - 1 - i];
        try std.testing.expectApproxEqAbs(@as(f32, 1.0), sample * sample + mirrored * mirrored, 1e-5);
    }
}

test "celt imdct of zero coefficients stays zero" {
    var out = @as([960]f32, @splat(0));
    const coeffs = @as([480]f32, @splat(0));
    try celtImdctInto(&out, &coeffs);
    for (out) |sample| {
        try std.testing.expectApproxEqAbs(@as(f32, 0.0), sample, 1e-6);
    }
}

test "celt window leaves center unchanged and tapers overlap edges" {
    var samples = @as([960]f32, @splat(1.0));
    try applyCeltWindowInPlace(std.testing.allocator, &samples, 120);

    try std.testing.expect(samples[0] < 0.1);
    try std.testing.expect(samples[119] < 1.0);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), samples[120], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), samples[839], 1e-6);
    try std.testing.expect(samples[959] < 0.1);
}

test "decode checked-in opus coarse energy stays aligned between .opus and .ogg alias" {
    var native = try demuxOggAlloc(std.testing.allocator, tone_opus_bytes);
    defer native.deinit();
    var alias = try demuxOggAlloc(std.testing.allocator, tone_opus_ogg_bytes);
    defer alias.deinit();

    var native_state = try initCeltEnergyState(native.header.channels);
    var alias_state = try initCeltEnergyState(alias.header.channels);

    for (native.packets, alias.packets, native.packet_tocs) |native_packet, alias_packet, toc| {
        const native_frame = try decodeCeltCoarseEnergyFrame(&native_state, native_packet, toc);
        const alias_frame = try decodeCeltCoarseEnergyFrame(&alias_state, alias_packet, toc);
        try std.testing.expectEqual(native_frame.header.silence, alias_frame.header.silence);
        try std.testing.expectEqual(native_frame.header.has_postfilter, alias_frame.header.has_postfilter);
        try std.testing.expectEqual(native_frame.header.is_transient, alias_frame.header.is_transient);
        try std.testing.expectEqual(native_frame.header.intra_energy, alias_frame.header.intra_energy);
        try std.testing.expectEqual(native_frame.end_band, alias_frame.end_band);
        for (0..native_frame.channels) |channel| {
            for (0..native_frame.end_band) |band| {
                try std.testing.expectApproxEqAbs(
                    native_frame.band_energies[channel][band],
                    alias_frame.band_energies[channel][band],
                    1e-6,
                );
            }
        }
    }
}

test "decode checked-in opus coarse energy sequences remain finite" {
    inline for ([_]struct {
        bytes: []const u8,
        expected_channels: u8,
        expected_bandwidth: Bandwidth,
    }{
        .{ .bytes = tone_opus_bytes, .expected_channels = 2, .expected_bandwidth = .wb },
        .{ .bytes = tone_opus_mono_bytes, .expected_channels = 1, .expected_bandwidth = .fb },
    }) |case| {
        var demuxed = try demuxOggAlloc(std.testing.allocator, case.bytes);
        defer demuxed.deinit();
        var state = try initCeltEnergyState(case.expected_channels);

        for (demuxed.packets, demuxed.packet_tocs) |packet, toc| {
            const frame = try decodeCeltCoarseEnergyFrame(&state, packet, toc);
            try std.testing.expectEqual(case.expected_channels, frame.channels);
            try std.testing.expectEqual(try endBandForBandwidth(case.expected_bandwidth), frame.end_band);
            for (0..frame.channels) |channel| {
                for (0..frame.end_band) |band| {
                    try std.testing.expect(std.math.isFinite(frame.band_energies[channel][band]));
                }
            }
        }
    }
}

test "checked-in opus fixtures stay on narrow celt header lane" {
    inline for ([_]struct {
        bytes: []const u8,
        expected_channels: u8,
    }{
        .{ .bytes = tone_opus_bytes, .expected_channels = 2 },
        .{ .bytes = tone_opus_mono_bytes, .expected_channels = 1 },
    }) |case| {
        var demuxed = try demuxOggAlloc(std.testing.allocator, case.bytes);
        defer demuxed.deinit();
        var state = try initCeltEnergyState(case.expected_channels);

        for (demuxed.packets, demuxed.packet_tocs) |packet, toc| {
            const frame = try decodeCeltCoarseEnergyFrame(&state, packet, toc);
            try std.testing.expectEqual(@as(bool, false), frame.header.silence);
            try std.testing.expectEqual(@as(bool, false), frame.header.has_postfilter);
            try std.testing.expectEqual(@as(bool, false), frame.header.is_transient);
        }
    }
}

test "decode checked-in mono opus residual bands stay finite and non-zero" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, tone_opus_mono_bytes);
    defer demuxed.deinit();
    var state = try initCeltEnergyState(1);

    for (demuxed.packets, demuxed.packet_tocs) |packet, toc| {
        const frame = try decodeCeltResidualFrame(std.testing.allocator, &state, packet, toc);
        try std.testing.expectEqual(@as(u8, 1), frame.channels);
        var saw_non_zero = false;
        for (0..frame.end_band) |band| {
            for (0..frame.bands[0][band].len) |i| {
                const value = frame.bands[0][band].coefficients[i];
                try std.testing.expect(std.math.isFinite(value));
                saw_non_zero = saw_non_zero or @abs(value) > 1e-6;
            }
            try std.testing.expect(std.math.isFinite(frame.band_energies[0][band]));
        }
        try std.testing.expect(saw_non_zero);
    }
}

test "decode checked-in stereo opus residual bands stay finite and non-zero" {
    inline for ([_][]const u8{ tone_opus_bytes, tone_opus_ogg_bytes }) |fixture| {
        var demuxed = try demuxOggAlloc(std.testing.allocator, fixture);
        defer demuxed.deinit();
        var state = try initCeltEnergyState(2);

        for (demuxed.packets, demuxed.packet_tocs) |packet, toc| {
            const frame = try decodeCeltResidualFrame(std.testing.allocator, &state, packet, toc);
            try std.testing.expectEqual(@as(u8, 2), frame.channels);
            var saw_non_zero = false;
            for (0..frame.channels) |channel| {
                for (0..frame.end_band) |band| {
                    for (0..frame.bands[channel][band].len) |i| {
                        const value = frame.bands[channel][band].coefficients[i];
                        try std.testing.expect(std.math.isFinite(value));
                        saw_non_zero = saw_non_zero or @abs(value) > 1e-6;
                    }
                    try std.testing.expect(std.math.isFinite(frame.band_energies[channel][band]));
                }
            }
            try std.testing.expect(saw_non_zero);
        }
    }
}

test "checked-in stereo opus aliases keep celt residual plan parity" {
    var native = try demuxOggAlloc(std.testing.allocator, tone_opus_bytes);
    defer native.deinit();
    var alias = try demuxOggAlloc(std.testing.allocator, tone_opus_ogg_bytes);
    defer alias.deinit();

    var native_state = try initCeltEnergyState(2);
    var alias_state = try initCeltEnergyState(2);

    for (native.packets, alias.packets, native.packet_tocs) |native_packet, alias_packet, toc| {
        const native_frame = try decodeCeltResidualFrame(std.testing.allocator, &native_state, native_packet, toc);
        const alias_frame = try decodeCeltResidualFrame(std.testing.allocator, &alias_state, alias_packet, toc);

        try std.testing.expectEqual(native_frame.start_band, alias_frame.start_band);
        try std.testing.expectEqual(native_frame.end_band, alias_frame.end_band);
        try std.testing.expectEqual(native_frame.plan.coded_bands, alias_frame.plan.coded_bands);
        try std.testing.expectEqual(native_frame.plan.intensity, alias_frame.plan.intensity);
        try std.testing.expectEqual(native_frame.plan.dual_stereo, alias_frame.plan.dual_stereo);

        for (native_frame.start_band..native_frame.end_band) |band| {
            try std.testing.expectEqual(native_frame.plan.band_bits[band], alias_frame.plan.band_bits[band]);
            try std.testing.expectEqual(native_frame.plan.fine_quant[band], alias_frame.plan.fine_quant[band]);
            try std.testing.expectEqual(native_frame.plan.fine_priority[band], alias_frame.plan.fine_priority[band]);
            try std.testing.expectEqual(native_frame.plan.pulses[band], alias_frame.plan.pulses[band]);
            try std.testing.expectEqual(native_frame.bands[0][band].len, alias_frame.bands[0][band].len);
            try std.testing.expectEqual(native_frame.bands[1][band].len, alias_frame.bands[1][band].len);
        }
    }
}

test "checked-in stereo opus corpus exercises widened stereo plan shapes" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, tone_opus_bytes);
    defer demuxed.deinit();
    var state = try initCeltEnergyState(2);

    var saw_coupled_low_band = false;
    var saw_intensity_tail = false;
    var saw_dual_stereo_low_band = false;

    for (demuxed.packets, demuxed.packet_tocs) |packet, toc| {
        const frame = try decodeCeltResidualFrame(std.testing.allocator, &state, packet, toc);

        if (frame.plan.intensity > frame.start_band and frame.plan.intensity <= frame.end_band) {
            var saw_lowband_payload = false;
            for (frame.start_band..frame.plan.intensity) |band| {
                saw_lowband_payload = saw_lowband_payload or frame.plan.band_bits[band] > 0;
            }
            if (saw_lowband_payload) {
                if (frame.plan.dual_stereo) {
                    saw_dual_stereo_low_band = true;
                } else {
                    saw_coupled_low_band = true;
                }
            }
        }

        if (frame.plan.intensity < frame.end_band) {
            for (frame.plan.intensity..frame.end_band) |band| {
                if (frame.plan.band_bits[band] > 0) {
                    saw_intensity_tail = true;
                    break;
                }
            }
        }
    }

    try std.testing.expect(saw_coupled_low_band);
    try std.testing.expect(saw_dual_stereo_low_band);
    try std.testing.expect(saw_intensity_tail);
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
    try std.testing.expectEqual(from_ogg.samples.len, from_stream.samples.len);
    for (from_ogg.samples, from_stream.samples) |lhs, rhs| {
        try std.testing.expectApproxEqAbs(lhs, rhs, 1e-6);
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

test "synthesize stereo celt frame handles intensity-shared tail bands" {
    var synth_state = try CeltSynthState.init(std.testing.allocator, 2, standardOverlap());
    defer synth_state.deinit();

    var frame = CeltResidualFrame{
        .header = .{
            .silence = false,
            .has_postfilter = false,
            .postfilter_pitch = 0,
            .postfilter_gain = 0,
            .postfilter_tapset = 0,
            .is_transient = false,
            .intra_energy = false,
        },
        .end_band = 4,
        .channels = 2,
        .plan = .{
            .spread_decision = 2,
            .alloc_trim = 5,
            .coded_bands = 4,
            .intensity = 2,
            .dual_stereo = true,
        },
        .bands = @as([2][21]CeltResidualBand, @splat(@as([21]CeltResidualBand, @splat(.{ .len = 0 })))),
        .band_energies = @as([2][21]f32, @splat(@as([21]f32, @splat(0)))),
    };
    frame.bands[0][0].len = 1;
    frame.bands[0][0].coefficients[0] = 0.8;
    frame.bands[1][0].len = 1;
    frame.bands[1][0].coefficients[0] = -0.6;
    frame.bands[0][1].len = 1;
    frame.bands[0][1].coefficients[0] = 0.4;
    frame.bands[1][1].len = 1;
    frame.bands[1][1].coefficients[0] = 0.2;

    for (2..4) |band| {
        frame.bands[0][band].len = 1;
        frame.bands[1][band].len = 1;
        frame.bands[0][band].coefficients[0] = 0.5;
        frame.bands[1][band].coefficients[0] = 0.5;
        frame.band_energies[0][band] = 0.3;
        frame.band_energies[1][band] = -0.2;
    }

    var decoded = try synthesizeFrameAlloc(
        std.testing.allocator,
        &synth_state,
        frame,
        .{
            .raw = 0,
            .config = 0,
            .stereo = true,
            .code = 0,
            .mode = .celt,
            .bandwidth = .wb,
            .frame_duration_us = 10_000,
        },
    );
    defer std.testing.allocator.free(decoded);

    try std.testing.expect(decoded.len > 0);
    for (decoded[0..@min(decoded.len, 256)]) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
    }
}

test "decode coupled stereo celt band keeps coefficients finite" {
    var bands = @as([2][21]CeltResidualBand, @splat(@as([21]CeltResidualBand, @splat(.{ .len = 0 }))));
    bands[0][13].len = 4;
    bands[1][13].len = 4;

    const zero_bytes = @as([32]u8, @splat(0));
    var decoder = RangeDecoder.init(&zero_bytes);
    try decodeCoupledStereoBand(&decoder, 0, 13, 24, 2, &bands, 4);

    var saw_non_zero = false;
    for (0..2) |channel| {
        for (bands[channel][13].coefficients[0..4]) |value| {
            try std.testing.expect(std.math.isFinite(value));
            saw_non_zero = saw_non_zero or @abs(value) > 1e-6;
        }
    }
    try std.testing.expect(saw_non_zero);
}

test "decode and synthesize coupled stereo celt low bands below intensity" {
    var bands = @as([2][21]CeltResidualBand, @splat(@as([21]CeltResidualBand, @splat(.{ .len = 0 }))));
    const zero_bytes = @as([64]u8, @splat(0));
    var decoder = RangeDecoder.init(&zero_bytes);

    var plan = CeltResidualPlan{
        .spread_decision = 2,
        .alloc_trim = 5,
        .coded_bands = 15,
        .intensity = 14,
        .dual_stereo = false,
    };
    plan.band_bits[13] = 24;
    plan.band_bits[14] = 16;

    try decodeCeltResidualBands(
        std.testing.allocator,
        &decoder,
        0,
        13,
        15,
        2,
        &plan,
        &bands,
    );

    var frame = CeltResidualFrame{
        .header = .{
            .silence = false,
            .has_postfilter = false,
            .postfilter_pitch = 0,
            .postfilter_gain = 0,
            .postfilter_tapset = 0,
            .is_transient = false,
            .intra_energy = false,
        },
        .start_band = 13,
        .end_band = 15,
        .channels = 2,
        .plan = plan,
        .bands = bands,
        .band_energies = @as([2][21]f32, @splat(@as([21]f32, @splat(0)))),
    };
    frame.band_energies[0][13] = 0.35;
    frame.band_energies[1][13] = -0.15;
    frame.band_energies[0][14] = 0.10;
    frame.band_energies[1][14] = -0.05;

    var synth_state = try CeltSynthState.init(std.testing.allocator, 2, standardOverlap());
    defer synth_state.deinit();
    const decoded = try synthesizeFrameAlloc(
        std.testing.allocator,
        &synth_state,
        frame,
        .{
            .raw = 0,
            .config = 0,
            .stereo = true,
            .code = 0,
            .mode = .celt,
            .bandwidth = .mb,
            .frame_duration_us = 10_000,
        },
    );
    defer std.testing.allocator.free(decoded);

    try std.testing.expect(decoded.len > 0);
    var saw_difference = false;
    var i: usize = 0;
    while (i + 1 < decoded.len) : (i += 2) {
        try std.testing.expect(std.math.isFinite(decoded[i]));
        try std.testing.expect(std.math.isFinite(decoded[i + 1]));
        saw_difference = saw_difference or @abs(decoded[i] - decoded[i + 1]) > 1e-6;
    }
    try std.testing.expect(saw_difference);
}

test "decode generated silk packet header flags" {
    const toc = try parseToc(silk_probe_packet[0]);
    try std.testing.expectEqual(@as(Mode, .silk), toc.mode);
    try std.testing.expectEqual(@as(Bandwidth, .wb), toc.bandwidth);
    try std.testing.expectEqual(@as(bool, false), toc.stereo);
    try std.testing.expectEqual(@as(u32, 20_000), toc.frame_duration_us);

    const header = try decodeSilkPacketHeader(silk_probe_packet[1..], toc, 1);
    try std.testing.expectEqual(@as(Mode, .silk), header.mode);
    try std.testing.expectEqual(@as(u8, 1), header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.internal_frame_count);
    try std.testing.expectEqual(@as(u8, 4), header.subframes_per_internal_frame);
    try std.testing.expect(header.channel_headers[0].vad_flags[0]);
    try std.testing.expectEqual(@as(bool, false), header.channel_headers[0].lbrr_flag);
    try std.testing.expect(header.consumed_bits >= 2);
}

test "decode generated hybrid packet header flags" {
    const toc = try parseToc(hybrid_probe_packet[0]);
    try std.testing.expectEqual(@as(Mode, .hybrid), toc.mode);
    try std.testing.expectEqual(@as(Bandwidth, .fb), toc.bandwidth);
    try std.testing.expectEqual(@as(bool, true), toc.stereo);
    try std.testing.expectEqual(@as(u32, 20_000), toc.frame_duration_us);

    const header = try decodeSilkPacketHeader(hybrid_probe_packet[1..], toc, 2);
    try std.testing.expectEqual(@as(Mode, .hybrid), header.mode);
    try std.testing.expectEqual(@as(u8, 2), header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.internal_frame_count);
    try std.testing.expectEqual(@as(u8, 4), header.subframes_per_internal_frame);
    try std.testing.expect(header.channel_headers[0].vad_flags[0]);
    try std.testing.expect(header.channel_headers[1].vad_flags[0]);
    try std.testing.expectEqual(@as(bool, false), header.channel_headers[0].lbrr_flag);
    try std.testing.expectEqual(@as(bool, false), header.channel_headers[1].lbrr_flag);
    try std.testing.expect(header.consumed_bits >= 4);
}

test "decode generated silk packet front exposes indices and pulses" {
    const toc = try parseToc(silk_probe_packet[0]);
    const front = try decodeSilkPacketFront(silk_probe_packet[1..], toc, 1);

    try std.testing.expect(front.frame_present[0][0]);
    try std.testing.expectEqual(@as(usize, 320), front.frames[0][0].pulse_len);
    try std.testing.expect(front.frames[0][0].indices.signal_type != .no_voice_activity);

    var saw_non_zero = false;
    for (front.frames[0][0].pulses[0..front.frames[0][0].pulse_len]) |pulse| {
        saw_non_zero = saw_non_zero or pulse != 0;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode generated hybrid packet front exposes stereo silk state and pulses" {
    const toc = try parseToc(hybrid_probe_packet[0]);
    const front = try decodeSilkPacketFront(hybrid_probe_packet[1..], toc, 2);

    try std.testing.expect(front.frame_present[0][0]);
    try std.testing.expect(front.frame_present[1][0]);
    try std.testing.expect(front.stereo_pred_q13[0][0] != 0 or front.stereo_pred_q13[0][1] != 0);
    try std.testing.expectEqual(@as(usize, 320), front.frames[0][0].pulse_len);
    try std.testing.expectEqual(@as(usize, 320), front.frames[1][0].pulse_len);

    var saw_non_zero = false;
    for (0..2) |channel| {
        for (front.frames[channel][0].pulses[0..front.frames[channel][0].pulse_len]) |pulse| {
            saw_non_zero = saw_non_zero or pulse != 0;
        }
    }
    try std.testing.expect(saw_non_zero);
}

test "decode generated silk packet parameters expose gains lpc and excitation" {
    const toc = try parseToc(silk_probe_packet[0]);
    const decoded = try decodeSilkPacketParameters(silk_probe_packet[1..], toc, 1);

    try std.testing.expect(decoded.front.frame_present[0][0]);
    try std.testing.expect(decoded.frames[0][0].gains_q16[0] > 0);
    try std.testing.expect(decoded.frames[0][0].quant_offset_q10 != 0);

    var saw_lpc = false;
    for (decoded.frames[0][0].pred_coef_q12[0][0..16]) |coef| {
        saw_lpc = saw_lpc or coef != 0;
    }
    try std.testing.expect(saw_lpc);

    var saw_excitation = false;
    for (decoded.frames[0][0].excitation_q14[0..decoded.frames[0][0].excitation_len]) |sample| {
        saw_excitation = saw_excitation or sample != 0;
    }
    try std.testing.expect(saw_excitation);
}

test "decode real mono silk fec packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag);
    try std.testing.expect(header.channel_headers[0].lbrr_flags[0]);
}

test "decode real mono silk fec 10ms packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag);
    try std.testing.expect(header.channel_headers[0].lbrr_flags[0]);
}

test "decode real mono silk fec 40ms packet header exposes lbrr and two internal frames" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_40ms_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.channels);
    try std.testing.expectEqual(@as(u8, 2), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag);
    try std.testing.expect(header.channel_headers[0].lbrr_flags[0] or header.channel_headers[0].lbrr_flags[1]);
}

test "decode real mono silk fec 60ms packet header exposes lbrr and three internal frames" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_60ms_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.channels);
    try std.testing.expectEqual(@as(u8, 3), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag);
    try std.testing.expect(
        header.channel_headers[0].lbrr_flags[0] or
            header.channel_headers[0].lbrr_flags[1] or
            header.channel_headers[0].lbrr_flags[2],
    );
}

test "decode real stereo silk fec packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 2), header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag or header.channel_headers[1].lbrr_flag);
    try std.testing.expect(
        header.channel_headers[0].lbrr_flags[0] or header.channel_headers[1].lbrr_flags[0],
    );
}

test "decode real stereo silk fec 10ms packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 2), header.channels);
    try std.testing.expectEqual(@as(u8, 1), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag or header.channel_headers[1].lbrr_flag);
    try std.testing.expect(
        header.channel_headers[0].lbrr_flags[0] or header.channel_headers[1].lbrr_flags[0],
    );
}

test "decode real stereo silk fec 40ms packet header exposes lbrr and two internal frames" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_40ms_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 2), header.channels);
    try std.testing.expectEqual(@as(u8, 2), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag or header.channel_headers[1].lbrr_flag);
    try std.testing.expect(
        header.channel_headers[0].lbrr_flags[0] or
            header.channel_headers[0].lbrr_flags[1] or
            header.channel_headers[1].lbrr_flags[0] or
            header.channel_headers[1].lbrr_flags[1],
    );
}

test "decode real stereo silk fec 60ms packet header exposes lbrr and three internal frames" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_60ms_ogg_bytes);
    defer demuxed.deinit();

    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 2), header.channels);
    try std.testing.expectEqual(@as(u8, 3), header.internal_frame_count);
    try std.testing.expect(header.channel_headers[0].lbrr_flag or header.channel_headers[1].lbrr_flag);
    try std.testing.expect(
        header.channel_headers[0].lbrr_flags[0] or
            header.channel_headers[0].lbrr_flags[1] or
            header.channel_headers[0].lbrr_flags[2] or
            header.channel_headers[1].lbrr_flags[0] or
            header.channel_headers[1].lbrr_flags[1] or
            header.channel_headers[1].lbrr_flags[2],
    );
}

test "synthesize real mono silk fec packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 320), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real mono silk fec 10ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 160), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real mono silk fec 40ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_40ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 640), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real stereo silk fec packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 320 * 2), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real stereo silk fec 10ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 160 * 2), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real mono silk fec 60ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_fec_60ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 960), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real stereo silk fec 40ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_40ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 640 * 2), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real stereo silk fec 60ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_fec_60ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 960 * 2), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode generated hybrid packet parameters expose stereo silk decode state" {
    const toc = try parseToc(hybrid_probe_packet[0]);
    const decoded = try decodeSilkPacketParameters(hybrid_probe_packet[1..], toc, 2);

    try std.testing.expect(decoded.front.frame_present[0][0]);
    try std.testing.expect(decoded.front.frame_present[1][0]);
    try std.testing.expect(decoded.front.stereo_pred_q13[0][0] != 0 or decoded.front.stereo_pred_q13[0][1] != 0);
    try std.testing.expect(decoded.frames[0][0].gains_q16[0] > 0);
    try std.testing.expect(decoded.frames[1][0].gains_q16[0] > 0);

    var saw_excitation = false;
    for (0..2) |channel| {
        for (decoded.frames[channel][0].excitation_q14[0..decoded.frames[channel][0].excitation_len]) |sample| {
            saw_excitation = saw_excitation or sample != 0;
        }
    }
    try std.testing.expect(saw_excitation);
}

test "silk gain dequant saturates instead of overflowing i32" {
    var gains = @as([4]i32, @splat(0));
    var prev_index: i32 = 63;
    silkDequantGains(&gains, .{ 63, 63, 63, 63 }, &prev_index, false);

    try std.testing.expectEqual(std.math.maxInt(i32), gains[0]);
    try std.testing.expectEqual(std.math.maxInt(i32), gains[1]);
    try std.testing.expectEqual(std.math.maxInt(i32), gains[2]);
    try std.testing.expectEqual(std.math.maxInt(i32), gains[3]);
}

test "silk round shift saturates instead of overflowing i32" {
    try std.testing.expectEqual(std.math.maxInt(i32), silkRoundShift(std.math.maxInt(i64), 1));
    try std.testing.expectEqual(std.math.minInt(i32), silkRoundShift(std.math.minInt(i64), 1));
}

test "silk clamp32 saturates i64 extremes" {
    try std.testing.expectEqual(std.math.maxInt(i32), silkClamp32(std.math.maxInt(i64)));
    try std.testing.expectEqual(std.math.minInt(i32), silkClamp32(std.math.minInt(i64)));
}

test "synthesize real mono silk 10ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_10ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 160), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real stereo silk 10ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_10ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 160 * 2), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode real stereo silk 40ms packet front exposes two internal frames" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_stereo_40ms_ogg_bytes);
    defer demuxed.deinit();

    const front = try decodeSilkPacketFront(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 2), front.header.internal_frame_count);
    try std.testing.expect(front.frame_present[0][0]);
    try std.testing.expect(front.frame_present[0][1]);
    try std.testing.expect(front.frame_present[1][0]);
    try std.testing.expect(front.frame_present[1][1]);
    try std.testing.expect(front.stereo_pred_q13[0][0] != 0 or front.stereo_pred_q13[0][1] != 0);
    try std.testing.expect(front.stereo_pred_q13[1][0] != 0 or front.stereo_pred_q13[1][1] != 0);
}

test "decode real mono silk 60ms packet parameters expose three internal frames" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_60ms_ogg_bytes);
    defer demuxed.deinit();

    const decoded = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expectEqual(@as(u8, 3), decoded.front.header.internal_frame_count);
    try std.testing.expect(decoded.front.frame_present[0][0]);
    try std.testing.expect(decoded.front.frame_present[0][1]);
    try std.testing.expect(decoded.front.frame_present[0][2]);

    var saw_excitation = false;
    for (0..3) |frame_index| {
        try std.testing.expect(decoded.frames[0][frame_index].gains_q16[0] > 0);
        for (decoded.frames[0][frame_index].excitation_q14[0..decoded.frames[0][frame_index].excitation_len]) |sample| {
            saw_excitation = saw_excitation or sample != 0;
        }
    }
    try std.testing.expect(saw_excitation);
}

test "synthesize generated silk packet to mono 16 khz pcm" {
    const toc = try parseToc(silk_probe_packet[0]);
    const packet = try decodeSilkPacketParameters(silk_probe_packet[1..], toc, 1);
    var synth_state = SilkSynthState{ .channels = 1 };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 320), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "synthesize real mono silk 60ms packet to 16 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_silk_mono_60ms_ogg_bytes);
    defer demuxed.deinit();

    const packet = try decodeSilkPacketParameters(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    var synth_state = SilkSynthState{ .channels = demuxed.header.channels };
    const pcm = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm);

    try std.testing.expectEqual(@as(usize, 960), pcm.len);
    var saw_non_zero = false;
    for (pcm) |sample| {
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "integrate generated hybrid silk lowband into 48 khz stereo pcm" {
    const toc = try parseToc(hybrid_probe_packet[0]);
    const packet = try decodeSilkPacketParameters(hybrid_probe_packet[1..], toc, 2);
    var synth_state = SilkSynthState{ .channels = 2 };
    const pcm_16k = try silkSynthesizePacket16kAlloc(std.testing.allocator, &synth_state, &packet);
    defer std.testing.allocator.free(pcm_16k);

    const lowband_48k = try upsampleInterleaved16kTo48kAlloc(std.testing.allocator, pcm_16k, 2);
    defer std.testing.allocator.free(lowband_48k);
    try std.testing.expectEqual(@as(usize, 960 * 2), lowband_48k.len);

    const synthetic_highband = try std.testing.allocator.alloc(f32, lowband_48k.len);
    defer std.testing.allocator.free(synthetic_highband);
    for (synthetic_highband, 0..) |*sample, i| {
        sample.* = if ((i & 1) == 0) 0.01 else -0.01;
    }

    const mixed = try combineHybridLowbandWithHighbandAlloc(std.testing.allocator, lowband_48k, synthetic_highband);
    defer std.testing.allocator.free(mixed);

    var saw_difference = false;
    for (mixed, lowband_48k) |sample, low| {
        saw_difference = saw_difference or @abs(sample - low) > 1e-6;
    }
    try std.testing.expect(saw_difference);
}

test "decode generated hybrid packet celt highband residual after silk front" {
    const toc = try parseToc(hybrid_probe_packet[0]);
    var decoder = RangeDecoder.init(hybrid_probe_packet[1..]);
    _ = try decodeSilkPacketParametersFromDecoder(&decoder, toc, 2);

    var energy_state = try initCeltEnergyState(2);
    const residual = try decodeCeltResidualFrameFromDecoder(
        std.testing.allocator,
        &energy_state,
        &decoder,
        (hybrid_probe_packet.len - 1) * 8,
        try lmForFrameDurationUs(toc.frame_duration_us),
        try celtStartBandForToc(toc),
        try endBandForBandwidth(toc.bandwidth),
    );

    try std.testing.expectEqual(@as(usize, 17), residual.start_band);
    try std.testing.expectEqual(@as(usize, 21), residual.end_band);

    for (0..residual.start_band) |band| {
        try std.testing.expectEqual(@as(usize, 0), residual.bands[0][band].len);
        try std.testing.expectEqual(@as(usize, 0), residual.bands[1][band].len);
    }

    var saw_non_zero = false;
    for (0..residual.channels) |channel| {
        for (residual.start_band..residual.end_band) |band| {
            try std.testing.expect(std.math.isFinite(residual.band_energies[channel][band]));
            for (0..residual.bands[channel][band].len) |i| {
                const value = residual.bands[channel][band].coefficients[i];
                try std.testing.expect(std.math.isFinite(value));
                saw_non_zero = saw_non_zero or @abs(value) > 1e-6;
            }
        }
    }
    try std.testing.expect(saw_non_zero);
}

test "decode generated hybrid packet integrates celt highband into stereo 48 khz pcm" {
    const toc = try parseToc(hybrid_probe_packet[0]);

    var energy_state = try initCeltEnergyState(2);
    var celt_synth_state = try CeltSynthState.init(std.testing.allocator, 2, standardOverlap());
    defer celt_synth_state.deinit();
    var hybrid_silk_state = SilkSynthState{ .channels = 2 };

    const mixed = try decodeHybridFrameAlloc(
        std.testing.allocator,
        &energy_state,
        &celt_synth_state,
        &hybrid_silk_state,
        hybrid_probe_packet[1..],
        toc,
        2,
    );
    defer std.testing.allocator.free(mixed);

    const packet = try decodeSilkPacketParameters(hybrid_probe_packet[1..], toc, 2);
    var lowband_silk_state = SilkSynthState{ .channels = 2 };
    const pcm_16k = try silkSynthesizePacket16kAlloc(std.testing.allocator, &lowband_silk_state, &packet);
    defer std.testing.allocator.free(pcm_16k);
    const lowband_48k = try upsampleInterleaved16kTo48kAlloc(std.testing.allocator, pcm_16k, 2);
    defer std.testing.allocator.free(lowband_48k);

    try std.testing.expectEqual(@as(usize, 960 * 2), mixed.len);
    try std.testing.expectEqual(lowband_48k.len, mixed.len);

    var saw_difference = false;
    for (mixed, lowband_48k) |sample, low| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_difference = saw_difference or @abs(sample - low) > 1e-6;
    }
    try std.testing.expect(saw_difference);
}

test "decode real mono hybrid 10ms packet integrates to 48 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_mono_10ms_ogg_bytes);
    defer demuxed.deinit();

    var energy_state = try initCeltEnergyState(demuxed.header.channels);
    var celt_synth_state = try CeltSynthState.init(std.testing.allocator, demuxed.header.channels, standardOverlap());
    defer celt_synth_state.deinit();
    var hybrid_silk_state = SilkSynthState{ .channels = demuxed.header.channels };

    const mixed = try decodeHybridFrameAlloc(
        std.testing.allocator,
        &energy_state,
        &celt_synth_state,
        &hybrid_silk_state,
        demuxed.packets[0],
        demuxed.packet_tocs[0],
        demuxed.header.channels,
    );
    defer std.testing.allocator.free(mixed);

    try std.testing.expectEqual(@as(usize, 480), mixed.len);
    var saw_non_zero = false;
    for (mixed) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode real mono hybrid fec packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_mono_fec_ogg_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(@as(Mode, .hybrid), demuxed.packet_tocs[0].mode);
    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expect(header.channel_headers[0].lbrr_flag);
    try std.testing.expect(header.channel_headers[0].lbrr_flags[0]);
}

test "decode real mono hybrid fec 10ms packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_mono_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(@as(Mode, .hybrid), demuxed.packet_tocs[0].mode);
    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expect(header.channel_headers[0].lbrr_flag);
    try std.testing.expect(header.channel_headers[0].lbrr_flags[0]);
}

test "decode real mono hybrid fec packet integrates to 48 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_mono_fec_ogg_bytes);
    defer demuxed.deinit();

    var energy_state = try initCeltEnergyState(demuxed.header.channels);
    var celt_synth_state = try CeltSynthState.init(std.testing.allocator, demuxed.header.channels, standardOverlap());
    defer celt_synth_state.deinit();
    var hybrid_silk_state = SilkSynthState{ .channels = demuxed.header.channels };

    const mixed = try decodeHybridFrameAlloc(
        std.testing.allocator,
        &energy_state,
        &celt_synth_state,
        &hybrid_silk_state,
        demuxed.packets[0],
        demuxed.packet_tocs[0],
        demuxed.header.channels,
    );
    defer std.testing.allocator.free(mixed);

    try std.testing.expectEqual(@as(usize, 960), mixed.len);
    var saw_non_zero = false;
    for (mixed) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode real mono hybrid fec 10ms packet integrates to 48 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_mono_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    var energy_state = try initCeltEnergyState(demuxed.header.channels);
    var celt_synth_state = try CeltSynthState.init(std.testing.allocator, demuxed.header.channels, standardOverlap());
    defer celt_synth_state.deinit();
    var hybrid_silk_state = SilkSynthState{ .channels = demuxed.header.channels };

    const mixed = try decodeHybridFrameAlloc(
        std.testing.allocator,
        &energy_state,
        &celt_synth_state,
        &hybrid_silk_state,
        demuxed.packets[0],
        demuxed.packet_tocs[0],
        demuxed.header.channels,
    );
    defer std.testing.allocator.free(mixed);

    try std.testing.expectEqual(@as(usize, 480), mixed.len);
    var saw_non_zero = false;
    for (mixed) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode real stereo hybrid 10ms packet integrates to 48 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_stereo_10ms_ogg_bytes);
    defer demuxed.deinit();

    var energy_state = try initCeltEnergyState(demuxed.header.channels);
    var celt_synth_state = try CeltSynthState.init(std.testing.allocator, demuxed.header.channels, standardOverlap());
    defer celt_synth_state.deinit();
    var hybrid_silk_state = SilkSynthState{ .channels = demuxed.header.channels };

    const mixed = try decodeHybridFrameAlloc(
        std.testing.allocator,
        &energy_state,
        &celt_synth_state,
        &hybrid_silk_state,
        demuxed.packets[0],
        demuxed.packet_tocs[0],
        demuxed.header.channels,
    );
    defer std.testing.allocator.free(mixed);

    try std.testing.expectEqual(@as(usize, 480 * 2), mixed.len);
    var saw_non_zero = false;
    for (mixed) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode real stereo hybrid fec packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_stereo_fec_ogg_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(@as(Mode, .hybrid), demuxed.packet_tocs[0].mode);
    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expect(header.channel_headers[0].lbrr_flag or header.channel_headers[1].lbrr_flag);
    try std.testing.expect(header.channel_headers[0].lbrr_flags[0] or header.channel_headers[1].lbrr_flags[0]);
}

test "decode real stereo hybrid fec 10ms packet header exposes lbrr" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_stereo_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    try std.testing.expectEqual(@as(Mode, .hybrid), demuxed.packet_tocs[0].mode);
    const header = try decodeSilkPacketHeader(demuxed.packets[0], demuxed.packet_tocs[0], demuxed.header.channels);
    try std.testing.expect(header.channel_headers[0].lbrr_flag or header.channel_headers[1].lbrr_flag);
    try std.testing.expect(header.channel_headers[0].lbrr_flags[0] or header.channel_headers[1].lbrr_flags[0]);
}

test "decode real stereo hybrid fec packet integrates to 48 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_stereo_fec_ogg_bytes);
    defer demuxed.deinit();

    var energy_state = try initCeltEnergyState(demuxed.header.channels);
    var celt_synth_state = try CeltSynthState.init(std.testing.allocator, demuxed.header.channels, standardOverlap());
    defer celt_synth_state.deinit();
    var hybrid_silk_state = SilkSynthState{ .channels = demuxed.header.channels };

    const mixed = try decodeHybridFrameAlloc(
        std.testing.allocator,
        &energy_state,
        &celt_synth_state,
        &hybrid_silk_state,
        demuxed.packets[0],
        demuxed.packet_tocs[0],
        demuxed.header.channels,
    );
    defer std.testing.allocator.free(mixed);

    try std.testing.expectEqual(@as(usize, 960 * 2), mixed.len);
    var saw_non_zero = false;
    for (mixed) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode real stereo hybrid fec 10ms packet integrates to 48 khz pcm" {
    var demuxed = try demuxOggAlloc(std.testing.allocator, probe_hybrid_stereo_fec_10ms_ogg_bytes);
    defer demuxed.deinit();

    var energy_state = try initCeltEnergyState(demuxed.header.channels);
    var celt_synth_state = try CeltSynthState.init(std.testing.allocator, demuxed.header.channels, standardOverlap());
    defer celt_synth_state.deinit();
    var hybrid_silk_state = SilkSynthState{ .channels = demuxed.header.channels };

    const mixed = try decodeHybridFrameAlloc(
        std.testing.allocator,
        &energy_state,
        &celt_synth_state,
        &hybrid_silk_state,
        demuxed.packets[0],
        demuxed.packet_tocs[0],
        demuxed.header.channels,
    );
    defer std.testing.allocator.free(mixed);

    try std.testing.expectEqual(@as(usize, 480 * 2), mixed.len);
    var saw_non_zero = false;
    for (mixed) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode synthetic ogg opus silk probe to interleaved pcm" {
    const packet = [_][]const u8{silk_probe_packet[0..]};
    const sample_counts = [_]u16{960};
    const ogg_bytes = try buildSyntheticOggOpusStreamAlloc(std.testing.allocator, 1, 0, &packet, &sample_counts);
    defer std.testing.allocator.free(ogg_bytes);

    var packets = try ogg.parsePacketsAlloc(std.testing.allocator, ogg_bytes);
    defer packets.deinit();
    try std.testing.expectEqual(@as(usize, 3), packets.packets.len);
    try std.testing.expectEqual(@as(u64, 960), packets.packets[2].page_granule_position);
    try std.testing.expect(packets.packets[2].is_eos);

    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, ogg_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 1), decoded.channels);
    try std.testing.expectEqual(@as(usize, 960), decoded.samples.len);
    var saw_non_zero = false;
    for (decoded.samples) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}

test "decode synthetic ogg opus hybrid probe to interleaved pcm" {
    const packet = [_][]const u8{hybrid_probe_packet[0..]};
    const sample_counts = [_]u16{960};
    const ogg_bytes = try buildSyntheticOggOpusStreamAlloc(std.testing.allocator, 2, 0, &packet, &sample_counts);
    defer std.testing.allocator.free(ogg_bytes);

    var packets = try ogg.parsePacketsAlloc(std.testing.allocator, ogg_bytes);
    defer packets.deinit();
    try std.testing.expectEqual(@as(usize, 3), packets.packets.len);
    try std.testing.expectEqual(@as(u64, 960), packets.packets[2].page_granule_position);
    try std.testing.expect(packets.packets[2].is_eos);

    var decoded = try decodeInterleavedOggAlloc(std.testing.allocator, ogg_bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u32, 48_000), decoded.sample_rate);
    try std.testing.expectEqual(@as(u8, 2), decoded.channels);
    try std.testing.expectEqual(@as(usize, 960 * 2), decoded.samples.len);
    var saw_non_zero = false;
    for (decoded.samples) |sample| {
        try std.testing.expect(std.math.isFinite(sample));
        saw_non_zero = saw_non_zero or @abs(sample) > 1e-6;
    }
    try std.testing.expect(saw_non_zero);
}
