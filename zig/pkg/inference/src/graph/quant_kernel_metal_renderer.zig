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

//! Descriptor-driven renderer for generated Metal small-batch quant matmul
//! kernels. A kernel is `render(kernel_id, decoder, schedule, epilogue)`:
//! one canonical skeleton parametrized by the launch schedule (threads/cols/
//! reduction), with the per-format dequant math supplied as a leaf MSL fragment
//! (`FormatDecoder.lane_decode_msl`).
//!
//! This produces "v2" kernels. They are NOT byte-identical to the frozen v1
//! bodies — the skeleton is normalized (redundant `if (tid < N)` guards dropped
//! in favor of the strided lane loop's natural bounds; a single shared gelu /
//! half-load vocabulary; consistent `partial[]` placement). Every rendered
//! kernel earns production through the existing evidence gates against its v1
//! baseline, so semantic parity is proven by benchmark + CPU conformance, not
//! by construction.

const std = @import("std");
const compiler = @import("quant_kernel_compiler.zig");
const quant_kernel_op = @import("quant_kernel_op.zig");
const quant_matmul = @import("quant_matmul.zig");

const Epilogue = compiler.Epilogue;
const KernelSchedule = compiler.KernelSchedule;
const ReductionKind = compiler.ReductionKind;
const OpKind = quant_kernel_op.OpKind;

pub const MicrokernelKind = quant_kernel_op.MicrokernelKind;
pub const AttentionKind = quant_kernel_op.AttentionKind;

/// A shared MSL vocabulary helper (dedup of the per-format v1 copies).
pub const HelperFragment = struct {
    name: []const u8,
    msl: []const u8,
};

/// Per-format dequant descriptor. `lane_decode_msl` is a leaf fragment with the
/// fixed contract `inline float <lane_decode_fn>(device const uchar *block,
/// int lane)` — a pure function of the block bytes and the lane index, validated
/// against the CPU reference by conformance tests. `helpers` are the shared
/// vocabulary fragments the fragment references, in dependency order.
pub const FormatDecoder = struct {
    format: quant_matmul.Format,
    weight_param: []const u8, // e.g. "weight_q4_0"
    lane_decode_fn: []const u8, // e.g. "antfly_q4_0_dequant_lane_v2"
    lane_decode_msl: []const u8,
    helpers: []const HelperFragment,
};

// ---- Shared vocabulary helpers -------------------------------------------

pub const helper_qk_half_le_to_float = HelperFragment{
    .name = "antfly_qk_half_le_to_float",
    .msl = "inline float antfly_qk_half_le_to_float(device const uchar *p) { ushort bits = (ushort(p[0]) | (ushort(p[1]) << 8)); return float(as_type<half>(bits)); }",
};

pub const helper_qk_f32_le_to_float = HelperFragment{
    .name = "antfly_qk_f32_le_to_float",
    .msl = "inline float antfly_qk_f32_le_to_float(device const uchar *p) { uint bits = uint(p[0]) | (uint(p[1]) << 8) | (uint(p[2]) << 16) | (uint(p[3]) << 24); return as_type<float>(bits); }",
};

pub const helper_qk_u32_le = HelperFragment{
    .name = "antfly_qk_u32_le",
    .msl = "inline uint antfly_qk_u32_le(device const uchar *p) { return uint(p[0]) | (uint(p[1]) << 8) | (uint(p[2]) << 16) | (uint(p[3]) << 24); }",
};

pub const helper_qk_gelu = HelperFragment{
    .name = "antfly_qk_gelu",
    .msl = "inline float antfly_qk_gelu(float x) { if (!isfinite(x)) return 0.0f; float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x); if (inner > 10.0f) return x; if (inner < -10.0f) return 0.0f; float y = 0.5f * x * (1.0f + tanh(inner)); return isfinite(y) ? y : 0.0f; }",
};

pub const helper_qk_unpack_scale_min_6bit = HelperFragment{
    .name = "antfly_qk_unpack_scale_min_6bit",
    .msl =
    \\inline void antfly_qk_unpack_scale_min_6bit(device const uchar *scales, int sub, thread float &scale, thread float &min_v) {
    \\    if (sub < 4) { scale = float(scales[sub] & 63u); min_v = float(scales[sub + 4] & 63u); return; }
    \\    scale = float((scales[sub + 4] & 0x0fu) | ((scales[sub - 4] >> 6) << 4)); min_v = float((scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4));
    \\}
    ,
};

pub const helper_q3_k_raw_scale = HelperFragment{
    .name = "antfly_q3_k_raw_scale",
    .msl =
    \\inline int antfly_q3_k_raw_scale(device const uchar *scale_data, uint sub) {
    \\    uint i = sub & 3u; uint low = 0u; uint high = 0u;
    \\    if (sub < 4u) { low = uint(scale_data[i] & 0x0fu); high = uint(scale_data[8u + i] & 0x03u); }
    \\    else if (sub < 8u) { low = uint(scale_data[4u + i] & 0x0fu); high = uint((scale_data[8u + i] >> 2) & 0x03u); }
    \\    else if (sub < 12u) { low = uint((scale_data[i] >> 4) & 0x0fu); high = uint((scale_data[8u + i] >> 4) & 0x03u); }
    \\    else { low = uint((scale_data[4u + i] >> 4) & 0x0fu); high = uint((scale_data[8u + i] >> 6) & 0x03u); }
    \\    return int(low | (high << 4)) - 32;
    \\}
    ,
};

// ---- Attention helper fragments ------------------------------------------
// The generated decode-attention kernel is self-contained: it emits its own
// params struct + paging helper rather than referencing the hand-written
// `termite_metal_paged_attention_params` / `termite_attention_page_token` in
// metal_kernels.m. This is mandatory, not stylistic — the runtime region
// (`quant-kernel-codegen:begin..end`) and the hand-written attention kernels
// compile into the SAME Metal library, and the hand-written `page_token` is
// defined *after* the region, so a runtime-region body referencing it would be
// a use-before-declaration error. Renaming (`antfly_paged_attention_1x_*`) also
// avoids redefining the struct that is declared before the region. Both copies
// (checked-in `.metal` and runtime region) emit these fragments identically, so
// the generated kernel stays byte-identical across the two, exactly like the
// RMSNorm microkernel.
//
// `paged_attention_params_field_body` is the single source of truth for the
// struct layout. The compiler pins the hand-written MSL copy in metal_kernels.m
// to this same body via a `metal_runtime_external_helpers` drift guard, so the
// generated struct (which the dispatch's `termite_metal_paged_attention_params`
// bytes are reinterpret-cast into at buffer(6)) can never silently diverge in
// layout from what the runtime binds.
pub const paged_attention_params_field_body = "uint q_len; uint kv_tokens; uint num_heads; uint num_kv_heads; uint head_dim; uint key_row_bytes; uint base_key_row_bytes; uint query_position_offset; uint kv_position_offset; uint sliding_window; uint v_row_stride; uint page_size; uint block_count; uint contiguous_base_token; uint contiguous_blocks; uint format; uint v_element_bytes; uint has_sinks; float softcap; uint swa_scan_clamp;";

pub const helper_paged_attention_1x_params = HelperFragment{
    .name = "antfly_paged_attention_1x_params",
    .msl = "struct antfly_paged_attention_1x_params { " ++ paged_attention_params_field_body ++ " };",
};

pub const helper_paged_attention_1x_page_token = HelperFragment{
    .name = "antfly_paged_attention_1x_page_token",
    .msl = "inline uint antfly_paged_attention_1x_page_token(device const uint *block_table, constant antfly_paged_attention_1x_params &p, uint logical_token) { if (p.contiguous_blocks != 0u) return p.contiguous_base_token + logical_token; uint logical_block = logical_token / p.page_size; uint token_in_block = logical_token - logical_block * p.page_size; if (logical_block >= p.block_count) return 0xffffffffu; return block_table[logical_block] + token_in_block; }",
};

// ---- Per-format decoders (v2 fragments) ----------------------------------
// These are the v1 dequant helpers with the lane fn renamed `_v2`, per-format
// `half_le_to_float`/`u32_le` calls repointed at the shared `antfly_qk_*`
// vocabulary, and q8_0 inlined so it no longer needs the external
// `termite_q8_0_block_scale` helper.

pub const decoder_q4_0 = FormatDecoder{
    .format = .q4_0,
    .weight_param = "weight_q4_0",
    .lane_decode_fn = "antfly_q4_0_dequant_lane_v2",
    .lane_decode_msl = "inline float antfly_q4_0_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); int packed_index = lane & 15; uchar packed = block[2 + packed_index]; int q = lane < 16 ? int(packed & 0x0fu) - 8 : int(packed >> 4) - 8; return d * float(q); }",
    .helpers = &.{helper_qk_half_le_to_float},
};

pub const decoder_q4_1 = FormatDecoder{
    .format = .q4_1,
    .weight_param = "weight_q4_1",
    .lane_decode_fn = "antfly_q4_1_dequant_lane_v2",
    .lane_decode_msl = "inline float antfly_q4_1_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); float m = antfly_qk_half_le_to_float(block + 2); int packed_index = lane & 15; uchar packed = block[4 + packed_index]; int q = lane < 16 ? int(packed & 0x0fu) : int(packed >> 4); return d * float(q) + m; }",
    .helpers = &.{helper_qk_half_le_to_float},
};

pub const decoder_q5_0 = FormatDecoder{
    .format = .q5_0,
    .weight_param = "weight_q5_0",
    .lane_decode_fn = "antfly_q5_0_dequant_lane_v2",
    .lane_decode_msl = "inline float antfly_q5_0_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); uint qh = antfly_qk_u32_le(block + 2); int packed_index = lane & 15; uchar packed = block[6 + packed_index]; int low4 = lane < 16 ? int(packed & 0x0fu) : int(packed >> 4); int high = int((qh >> uint(lane)) & 1u); return d * float((low4 | (high << 4)) - 16); }",
    .helpers = &.{ helper_qk_half_le_to_float, helper_qk_u32_le },
};

pub const decoder_q5_1 = FormatDecoder{
    .format = .q5_1,
    .weight_param = "weight_q5_1",
    .lane_decode_fn = "antfly_q5_1_dequant_lane_v2",
    .lane_decode_msl = "inline float antfly_q5_1_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); float m = antfly_qk_half_le_to_float(block + 2); uint qh = antfly_qk_u32_le(block + 4); int packed_index = lane & 15; uchar packed = block[8 + packed_index]; int low4 = lane < 16 ? int(packed & 0x0fu) : int(packed >> 4); int high = int((qh >> uint(lane)) & 1u); return d * float(low4 | (high << 4)) + m; }",
    .helpers = &.{ helper_qk_half_le_to_float, helper_qk_u32_le },
};

pub const decoder_q8_0 = FormatDecoder{
    .format = .q8_0,
    .weight_param = "weight_q8_0",
    .lane_decode_fn = "antfly_q8_0_dequant_lane_v2",
    .lane_decode_msl = "inline float antfly_q8_0_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); int q = int(as_type<char>(block[2 + lane])); return d * float(q); }",
    .helpers = &.{helper_qk_half_le_to_float},
};

pub const decoder_q8_1 = FormatDecoder{
    .format = .q8_1,
    .weight_param = "weight_q8_1",
    .lane_decode_fn = "antfly_q8_1_dequant_lane_v2",
    .lane_decode_msl = "inline float antfly_q8_1_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_half_le_to_float(block); int q = int(as_type<char>(block[4 + lane])); return d * float(q); }",
    .helpers = &.{helper_qk_half_le_to_float},
};

pub const decoder_q8_k = FormatDecoder{
    .format = .q8_k,
    .weight_param = "weight_q8_k",
    .lane_decode_fn = "antfly_q8_k_dequant_lane_v2",
    .lane_decode_msl = "inline float antfly_q8_k_dequant_lane_v2(device const uchar *block, int lane) { float d = antfly_qk_f32_le_to_float(block); int q = int(as_type<char>(block[4 + lane])); return d * float(q); }",
    .helpers = &.{helper_qk_f32_le_to_float},
};

pub const decoder_q2_k = FormatDecoder{
    .format = .q2_k,
    .weight_param = "weight_q2_k",
    .lane_decode_fn = "antfly_q2_k_dequant_lane_v2",
    .lane_decode_msl =
    \\inline float antfly_q2_k_dequant_lane_v2(device const uchar *block, int lane) {
    \\    uint sub = uint(lane) >> 4; uint i = uint(lane) & 15u; uchar scale_byte = block[sub]; float dsc = antfly_qk_half_le_to_float(block + 16) * float(scale_byte & 0x0fu); float dmn = antfly_qk_half_le_to_float(block + 18) * float(scale_byte >> 4);
    \\    uint chunk = sub >> 3; uint group = (sub & 7u) >> 1; uint l_base = (sub & 1u) << 4; uint q_base = chunk << 5; uint shift = group << 1; uint q = (uint(block[20u + q_base + l_base + i]) >> shift) & 0x03u;
    \\    return dsc * float(q) - dmn;
    \\}
    ,
    .helpers = &.{helper_qk_half_le_to_float},
};

pub const decoder_q3_k = FormatDecoder{
    .format = .q3_k,
    .weight_param = "weight_q3_k",
    .lane_decode_fn = "antfly_q3_k_dequant_lane_v2",
    .lane_decode_msl =
    \\inline float antfly_q3_k_dequant_lane_v2(device const uchar *block, int lane) {
    \\    uint sub = uint(lane) >> 4; uint i = uint(lane) & 15u; uint chunk = sub >> 3; uint group = (sub & 7u) >> 1; uint l = ((sub & 1u) << 4) + i; uint q_base = chunk << 5; uint shift = group << 1; uint hm_bit = (chunk << 2) + group;
    \\    int low2 = int((uint(block[32u + q_base + l]) >> shift) & 0x03u); int high1 = int((uint(block[l]) >> hm_bit) & 0x01u); int q = low2 + high1 * 4 - 4;
    \\    return antfly_qk_half_le_to_float(block + 108) * float(antfly_q3_k_raw_scale(block + 96, sub)) * float(q);
    \\}
    ,
    .helpers = &.{ helper_qk_half_le_to_float, helper_q3_k_raw_scale },
};

pub const decoder_q4_k = FormatDecoder{
    .format = .q4_k,
    .weight_param = "weight_q4_k",
    .lane_decode_fn = "antfly_q4_k_dequant_lane_v2",
    .lane_decode_msl =
    \\inline float antfly_q4_k_dequant_lane_v2(device const uchar *block, int lane) {
    \\    device const uchar *scales = block + 4; device const uchar *qs = block + 16; int sub = lane >> 5; int q_index = (sub >> 1) * 32 + (lane & 31);
    \\    uchar packed = qs[q_index]; int q = (sub & 1) == 0 ? int(packed & 0x0fu) : int(packed >> 4);
    \\    float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(scales, sub, raw_scale, raw_min);
    \\    return antfly_qk_half_le_to_float(block) * raw_scale * float(q) - antfly_qk_half_le_to_float(block + 2) * raw_min;
    \\}
    ,
    .helpers = &.{ helper_qk_half_le_to_float, helper_qk_unpack_scale_min_6bit },
};

pub const decoder_q5_k = FormatDecoder{
    .format = .q5_k,
    .weight_param = "weight_q5_k",
    .lane_decode_fn = "antfly_q5_k_dequant_lane_v2",
    .lane_decode_msl =
    \\inline float antfly_q5_k_dequant_lane_v2(device const uchar *block, int lane) {
    \\    device const uchar *scales = block + 4; device const uchar *qh = block + 16; device const uchar *ql = block + 48; int sub = lane >> 5; int i = lane & 31; int q_index = (sub >> 1) * 32 + i;
    \\    uchar packed = ql[q_index]; int low = (sub & 1) == 0 ? int(packed & 0x0fu) : int(packed >> 4); int high = int((qh[i] >> sub) & 1u); int q = low + high * 16;
    \\    float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(scales, sub, raw_scale, raw_min);
    \\    return antfly_qk_half_le_to_float(block) * raw_scale * float(q) - antfly_qk_half_le_to_float(block + 2) * raw_min;
    \\}
    ,
    .helpers = &.{ helper_qk_half_le_to_float, helper_qk_unpack_scale_min_6bit },
};

pub const decoder_q6_k = FormatDecoder{
    .format = .q6_k,
    .weight_param = "weight_q6_k",
    .lane_decode_fn = "antfly_q6_k_dequant_lane_v2",
    .lane_decode_msl =
    \\inline float antfly_q6_k_dequant_lane_v2(device const uchar *block, int lane) {
    \\    device const uchar *ql = block; device const uchar *qh = block + 128; device const uchar *scales = block + 192; int sub = lane >> 4; int i = lane & 15; int half_idx = sub >> 3; int group = (sub & 7) >> 1; int l = ((sub & 1) << 4) + i;
    \\    int ql_off = half_idx * 64 + (group & 1) * 32; int qh_off = half_idx * 32; int qh_shift = group * 2; int nibble_shift = (group >> 1) * 4; int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0fu); int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u);
    \\    int scale_u = int(scales[sub]); int scale = scale_u >= 128 ? scale_u - 256 : scale_u; return antfly_qk_half_le_to_float(block + 208) * float(scale) * float((low4 | (high2 << 4)) - 32);
    \\}
    ,
    .helpers = &.{helper_qk_half_le_to_float},
};

pub const all_decoders = [_]FormatDecoder{
    decoder_q4_0, decoder_q4_1, decoder_q5_0, decoder_q5_1,
    decoder_q8_0, decoder_q8_1, decoder_q8_k, decoder_q2_k,
    decoder_q3_k, decoder_q4_k, decoder_q5_k, decoder_q6_k,
};

pub fn decoderFor(format: quant_matmul.Format) ?FormatDecoder {
    for (all_decoders) |d| {
        if (d.format == format) return d;
    }
    return null;
}

// ---- Renderer ------------------------------------------------------------

fn appendFmt(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), comptime fmt: []const u8, args: anytype) !void {
    const chunk = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

fn log2Usize(value: usize) u6 {
    return @intCast(std.math.log2_int(usize, value));
}

/// Renders the full standalone MSL kernel: vocabulary helpers + dequant
/// fragment + (for gelu) the shared gelu helper + the kernel body.
pub fn renderKernel(
    allocator: std.mem.Allocator,
    kernel_id: []const u8,
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    epilogue: Epilogue,
) ![]u8 {
    const block_values = decoder.format.valuesPerBlock() orelse return error.MissingBlockValues;
    const block_bytes = decoder.format.bytesPerBlock() orelse return error.MissingBlockBytes;
    try schedule.validate(block_values);
    try validateEpilogueSchedule(schedule, epilogue);
    try validateTiledSchedule(decoder.format, schedule, epilogue);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    // Helper vocabulary (deduped by name across decoder + gelu).
    var emitted_names = EmittedNames{};
    for (decoder.helpers) |helper| try emitHelper(allocator, &out, &emitted_names, helper);
    if (epilogue == .bias_gelu) try emitHelper(allocator, &out, &emitted_names, helper_qk_gelu);
    try out.appendSlice(allocator, decoder.lane_decode_msl);
    try out.append(allocator, '\n');

    try renderBody(allocator, &out, kernel_id, decoder, schedule, epilogue, block_values, block_bytes);
    return out.toOwnedSlice(allocator);
}

/// Renders a single-dispatch Q/K/V projection for one homogeneous quant
/// format and output shape. The z grid selects Q, K, or V, so the exact tuned
/// schedule is reused without a second kernel family or packed weight layout.
pub fn renderQkvKernel(
    allocator: std.mem.Allocator,
    kernel_id: []const u8,
    decoder: FormatDecoder,
    schedule: KernelSchedule,
) ![]u8 {
    const block_values = decoder.format.valuesPerBlock() orelse return error.MissingBlockValues;
    const block_bytes = decoder.format.bytesPerBlock() orelse return error.MissingBlockBytes;
    if (decoder.format != .q4_k and decoder.format != .q6_k) return error.QkvRequiresSupportedFormat;
    try schedule.validate(block_values);
    try validateEpilogueSchedule(schedule, .none);
    try validateTiledSchedule(decoder.format, schedule, .none);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var emitted_names = EmittedNames{};
    for (decoder.helpers) |helper| try emitHelper(allocator, &out, &emitted_names, helper);
    try out.appendSlice(allocator, decoder.lane_decode_msl);
    try out.append(allocator, '\n');
    try renderQkvBody(allocator, &out, kernel_id, decoder, schedule, block_values, block_bytes);
    return out.toOwnedSlice(allocator);
}

// ---- Microkernels (non-matmul fused ops) ---------------------------------

/// Renders the full standalone MSL for a fused microkernel. Microkernels are
/// pure-f32 (no dequant decoder, no epilogue, no shared vocabulary today), so
/// this is just the kernel body — the same bytes emitted into the runtime
/// region, so the checked-in `.metal` and the runtime-embedded copy stay
/// single-sourced.
pub fn renderMicrokernel(
    allocator: std.mem.Allocator,
    kernel_id: []const u8,
    kind: MicrokernelKind,
    schedule: KernelSchedule,
) ![]u8 {
    try validateMicrokernelSchedule(kind, schedule);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try renderMicrokernelBody(allocator, &out, kernel_id, kind, schedule);
    return out.toOwnedSlice(allocator);
}

/// Convenience wrapper for the RMSNorm microkernel.
pub fn renderRmsNormKernel(
    allocator: std.mem.Allocator,
    kernel_id: []const u8,
    schedule: KernelSchedule,
) ![]u8 {
    return renderMicrokernel(allocator, kernel_id, .rms_norm, schedule);
}

fn validateMicrokernelSchedule(kind: MicrokernelKind, schedule: KernelSchedule) !void {
    switch (kind) {
        // RMSNorm parallelises the per-row `d` reduction with a threadgroup
        // tree, so it needs a power-of-two thread count of at least 2 (the
        // reduction halves each step). One threadgroup handles one row.
        .rms_norm => {
            const t = schedule.threads_per_threadgroup;
            if (t == 0 or (t & (t - 1)) != 0) return error.MicrokernelThreadsNotPowerOfTwo;
            if (t < 2) return error.MicrokernelThreadsTooFew;
            if (schedule.cols_per_threadgroup != 1) return error.MicrokernelColsMustBeOne;
        },
    }
}

fn renderMicrokernelBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    kind: MicrokernelKind,
    schedule: KernelSchedule,
) !void {
    try validateMicrokernelSchedule(kind, schedule);
    switch (kind) {
        .rms_norm => try renderRmsNormBody(allocator, out, kernel_id, schedule),
    }
}

/// RMSNorm: `out[r,i] = in[r,i] * rsqrt(mean_r(in^2) + eps) * weight[i]`.
/// One threadgroup per row; `threads` threads cooperatively sum the squares
/// with a strided lane loop, reduce with a threadgroup tree, then rescale.
/// Self-contained scalar params (input=0, weight=1, output=2, rows=3,
/// hidden_size=4, eps=5) so the source compiles standalone and inside the
/// precise runtime library alike. Matches `activations.rmsNorm` (the CPU
/// oracle) up to f32 summation order.
fn renderRmsNormBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    schedule: KernelSchedule,
) !void {
    const threads = schedule.threads_per_threadgroup;
    try appendFmt(allocator, out, "kernel void {s}(device const float *input [[buffer(0)]], device const float *weight [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &hidden_size [[buffer(4)]], constant float &eps [[buffer(5)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {{\n", .{kernel_id});
    try appendFmt(allocator, out, "    uint tid = thread_pos.x; int row = int(group_pos.x); if (row >= rows || hidden_size < 1) return; device const float *row_input = input + row * hidden_size; float sq = 0.0f;\n", .{});
    try appendFmt(allocator, out, "    for (int i = int(tid); i < hidden_size; i += {d}) {{ float x = row_input[i]; sq += x * x; }}\n", .{threads});
    try appendFmt(allocator, out, "    threadgroup float partial[{d}]; partial[tid] = sq; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = {d}u; stride > 0u; stride >>= 1) {{ if (tid < stride) partial[tid] += partial[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }}\n", .{ threads, threads / 2 });
    try appendFmt(allocator, out, "    float inv_rms = rsqrt(partial[0] / float(hidden_size) + eps);\n", .{});
    try appendFmt(allocator, out, "    for (int i = int(tid); i < hidden_size; i += {d}) output[row * hidden_size + i] = row_input[i] * inv_rms * weight[i];\n", .{threads});
    try out.appendSlice(allocator, "}\n");
}

// ---- Attention (paged decode) --------------------------------------------

/// Renders the full standalone MSL for a paged-attention kernel: the
/// self-contained params struct + paging helper (deduped) followed by the
/// kernel body. The same bytes are emitted into the runtime region, so the
/// checked-in `.metal` and the runtime-embedded copy stay single-sourced (and
/// byte-identical) exactly like the matmul and RMSNorm routes.
pub fn renderAttention(
    allocator: std.mem.Allocator,
    kernel_id: []const u8,
    kind: AttentionKind,
    schedule: KernelSchedule,
) ![]u8 {
    try validateAttentionSchedule(kind, schedule);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    var emitted_names = EmittedNames{};
    for (attentionHelpers(kind)) |helper| try emitHelper(allocator, &out, &emitted_names, helper);
    try renderAttentionBody(allocator, &out, kernel_id, kind, schedule);
    return out.toOwnedSlice(allocator);
}

/// Convenience wrapper for the scalar decode-1x paged-attention kernel.
pub fn renderPagedAttention1xKernel(
    allocator: std.mem.Allocator,
    kernel_id: []const u8,
    schedule: KernelSchedule,
) ![]u8 {
    return renderAttention(allocator, kernel_id, .decode_1x, schedule);
}

/// Convenience wrapper for the flash-prefill paged-attention kernel.
pub fn renderPrefillFlashKernel(
    allocator: std.mem.Allocator,
    kernel_id: []const u8,
    schedule: KernelSchedule,
) ![]u8 {
    return renderAttention(allocator, kernel_id, .prefill_flash, schedule);
}

/// The self-contained helper fragments an attention kernel references, in
/// dependency order (the paging helper takes the params struct by reference, so
/// the struct must be emitted first). The flash-prefill kernel binds and
/// reinterprets the same `termite_metal_paged_attention_params` bytes at
/// buffer(6) as decode_1x, so it emits the same two self-contained fragments.
fn attentionHelpers(kind: AttentionKind) []const HelperFragment {
    return switch (kind) {
        .decode_1x, .prefill_flash => &.{ helper_paged_attention_1x_params, helper_paged_attention_1x_page_token },
    };
}

fn validateAttentionSchedule(kind: AttentionKind, schedule: KernelSchedule) !void {
    switch (kind) {
        // decode-1x runs NT = NSG*32 threads: NSG simdgroups each own a strided
        // subset of KV tokens (32 lanes reduce the head_dim dot with simd_sum),
        // then the whole threadgroup runs a power-of-two tree over `partials`
        // for the softmax max/sum. So NT must be a positive multiple of the
        // 32-lane simdgroup width AND a power of two, and one threadgroup owns
        // one (query, head) — never more than one column.
        .decode_1x => {
            const t = schedule.threads_per_threadgroup;
            if (t == 0 or t % 32 != 0) return error.AttentionThreadsNotSimdgroupMultiple;
            if ((t & (t - 1)) != 0) return error.AttentionThreadsNotPowerOfTwo;
            if (schedule.cols_per_threadgroup != 1) return error.AttentionColsMustBeOne;
        },
        // flash-prefill is structurally a 128-thread / 4-simdgroup / 8-query-tile
        // kernel: the query tile is 8, and each simdgroup owns 2 of the 8 query
        // rows. Those are fixed, so only the two
        // sweep knobs vary. `key_chunk` is the KV tokens per flash chunk and must
        // be a positive multiple of the 32-lane simdgroup width (each lane owns
        // key_chunk/32 keys in the score pass; the 4 simdgroups tile key_chunk/8
        // MMA tiles, key_chunk/32 per simdgroup — always integral for a multiple
        // of 32). It must also be <= 128: the per-chunk `sphys` populate is a
        // single-pass `if (tid < key_chunk)` write over the 128 threads, so a
        // chunk wider than the
        // thread count would leave sphys[128..] unwritten. 32 and 64 are the swept
        // values. head_dim (a runtime value, must be a multiple of 32) is guarded
        // inside the kernel body.
        .prefill_flash => {
            if (schedule.cols_per_threadgroup != 1) return error.AttentionColsMustBeOne;
            if (schedule.threads_per_threadgroup == 256) {
                if (schedule.key_chunk != 64) return error.AttentionFlashHd512KeyChunkMustBe64;
                if (schedule.skip_rescale) return error.AttentionFlashHd512SkipRescaleUnsupported;
                return;
            }
            if (schedule.threads_per_threadgroup != 128) return error.AttentionFlashThreadsMustBe128Or256;
            const kc = schedule.key_chunk;
            if (kc == 0 or kc % 32 != 0) return error.AttentionFlashKeyChunkNotSimdgroupMultiple;
            if (kc > 128) return error.AttentionFlashKeyChunkTooWide;
        },
    }
}

fn renderAttentionBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    kind: AttentionKind,
    schedule: KernelSchedule,
) !void {
    try validateAttentionSchedule(kind, schedule);
    switch (kind) {
        .decode_1x => try renderPagedAttention1xBody(allocator, out, kernel_id, schedule),
        .prefill_flash => if (schedule.threads_per_threadgroup == 256)
            try renderPrefillFlashHd512Body(allocator, out, kernel_id)
        else
            try renderPrefillFlashBody(allocator, out, kernel_id, schedule),
    }
}

/// Paged decode attention (`termite_paged_attention_kv_1x`): one threadgroup per
/// (query, head). Grid `(q_len, num_heads)`, NT threads. NSG = NT/32 simdgroups
/// each score a strided subset of KV tokens (32 lanes reduce the head_dim dot
/// with `simd_sum`); a threadgroup tree finds the softmax max, a second tree the
/// normaliser, then the head_dim outputs are accumulated strided over `P·V`.
/// f16 paged KV (`format==3`), GQA via `heads_per_group`, causal + sliding
/// masking; no sinks, no softcap (the dispatch gates those to the scalar path).
///
/// This is a verbatim transcription of the hand-written kernel body with NT/NSG
/// parametrized and the params struct / paging helper renamed to the generated
/// self-contained copies — so the generated route is behaviourally byte-for-byte
/// the hand-written one, which is what the model-token acceptance gate checks.
fn renderPagedAttention1xBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    schedule: KernelSchedule,
) !void {
    const nt = schedule.threads_per_threadgroup;
    const nsg = nt / 32;
    try appendFmt(allocator, out, "kernel void {s}(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup float *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {{\n", .{kernel_id});
    try appendFmt(allocator, out, "    uint qi = tg.x; uint h = tg.y; if (qi >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || p.num_kv_heads == 0u) return;\n", .{});
    try appendFmt(allocator, out, "    const uint NT = {d}u; const uint NW = 32u; const uint NSG = {d}u; const float scale = rsqrt(float(p.head_dim)); uint heads_per_group = p.num_heads / p.num_kv_heads; uint kv_h = h / heads_per_group; uint q_stride = p.num_heads * p.head_dim; uint q_base = qi * q_stride + h * p.head_dim; uint out_base = qi * q_stride + h * p.head_dim; uint kv_head_base = kv_h * p.head_dim; uint query_pos = p.query_position_offset + qi; uint kv_end_pos = p.kv_position_offset + p.kv_tokens; bool all_tokens_allowed = query_pos + 1u >= kv_end_pos && (p.sliding_window == 0u || p.sliding_window >= p.kv_tokens); threadgroup float *scores = shmem; threadgroup float *partials = shmem + p.kv_tokens; threadgroup float *phys_tokens = shmem + p.kv_tokens + {d}u; const device half *v_half = reinterpret_cast<const device half *>(v_bytes);\n", .{ nt, nsg, nt });
    try appendFmt(allocator, out, "    for (uint base = 0u; base < p.kv_tokens; base += NSG) {{ uint ki = base + uint(sgitg); bool allowed = ki < p.kv_tokens && all_tokens_allowed; if (ki < p.kv_tokens && !allowed) {{ uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; }} uint physical_token = allowed ? antfly_paged_attention_1x_page_token(block_table, p, ki) : 0xffffffffu; if (physical_token == 0xffffffffu) allowed = false; float acc = 0.0f; if (allowed) {{ const device half *k_half = reinterpret_cast<const device half *>(encoded_key + physical_token * p.key_row_bytes); for (uint d = uint(lane); d < p.head_dim; d += NW) acc += q[q_base + d] * float(k_half[kv_head_base + d]); }} float score = allowed ? simd_sum(acc) * scale : -3.402823466e+38f; if (!isfinite(score)) score = -3.402823466e+38f; if (lane == 0u && ki < p.kv_tokens) {{ scores[ki] = score; phys_tokens[ki] = as_type<float>(allowed ? physical_token : 0u); }} }}\n", .{});
    try appendFmt(allocator, out, "    threadgroup_barrier(mem_flags::mem_threadgroup); float local_best = -3.402823466e+38f; for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) {{ float score = scores[ki]; local_best = score > local_best ? score : local_best; }} partials[tid] = local_best; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = NT >> 1u; stride > 0u; stride >>= 1u) {{ if (uint(tid) < stride) {{ float rhs = partials[tid + stride]; partials[tid] = rhs > partials[tid] ? rhs : partials[tid]; }} threadgroup_barrier(mem_flags::mem_threadgroup); }} float best = partials[0];\n", .{});
    try appendFmt(allocator, out, "    float local_sum = 0.0f; for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) {{ float score = scores[ki]; float e = (best > -3.0e+38f && score > -3.0e+38f) ? exp(score - best) : 0.0f; e = isfinite(e) ? e : 0.0f; scores[ki] = e; local_sum += e; }} partials[tid] = local_sum; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = NT >> 1u; stride > 0u; stride >>= 1u) {{ if (uint(tid) < stride) partials[tid] += partials[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }} const float inv_sum = partials[0] > 0.0f ? 1.0f / partials[0] : 0.0f;\n", .{});
    try appendFmt(allocator, out, "    for (uint ki = uint(tid); ki < p.kv_tokens; ki += NT) scores[ki] *= inv_sum; threadgroup_barrier(mem_flags::mem_threadgroup);\n", .{});
    try appendFmt(allocator, out, "    for (uint d = uint(tid); d < p.head_dim; d += NT) {{ const device half *v_col = v_half + kv_head_base + d; float value = 0.0f; for (uint ki = 0u; ki < p.kv_tokens; ++ki) {{ float weight = scores[ki]; value += weight * select(float(v_col[as_type<uint>(phys_tokens[ki]) * p.v_row_stride]), 0.0f, weight == 0.0f); }} output[out_base + d] = value; }}\n", .{});
    try out.appendSlice(allocator, "}\n");
}

/// Flash-style paged prefill attention (`termite_paged_attention_kv_prefill_sg`):
/// an 8-query tile per threadgroup, 4 simdgroups / 128 threads, simdgroup-MMA for
/// Q·Kᵀ and P·V with an online (running max/sum) softmax. f16 paged KV
/// (`format==3`), GQA, causal + sliding masking; no sinks, no softcap. For
/// K/V are read as page-local 8-token tiles, so the only shared memory is Q,
/// softmax state, and one reusable partial-tail output tile. `page_size` must be
/// a multiple of 8; the host keeps all other layouts on the paged fallback.
///
/// Two schedule knobs tune it:
///  - `key_chunk` (KC): the KV tokens staged + scored per flash chunk. The shmem
///    layout, the number of Q·Kᵀ MMA tiles per simdgroup (KC/32), the keys each
///    lane scores (KC/32), and the P·V inner tile count (KC/8) all derive from it.
///    KC=32 preserves the hand-written kernel's MMA and online-softmax order and
///    is the model-token baseline.
///    KC=64 restructures the Q·Kᵀ and score passes (2 tiles/simdgroup, 2
///    keys/lane). Its shmem footprint is `24*hd + 52*KC + 352` bytes, so both
///    KC=32 and KC=64 fit the 32 KB threadgroup limit at head_dim=256.
///  - `skip_rescale`: guard the online-softmax O-accumulator rescale (an identity
///    `simdgroup_multiply` when no row's running max moved this chunk) behind a
///    threadgroup flag. ALU-only; the flag lives in the layout's reserved word so
///    the footprint is unchanged.
///
/// Shmem layout (bytes, from `shmem`): `sq` 8*hd half, then the float block `fb`
/// at `8*hd*2`: `ss` 8×KC f32 (0), `sp` 8×KC half (32*KC),
/// `sphys` KC u32 (48*KC), `sM` 8 f32 (52*KC), `sS` 8 f32 (52*KC+32), a reserved
/// word (52*KC+64, the `skip_rescale` flag), `sdiag` 8×8 f32 (52*KC+96), then
/// `so` 8×(hd/4) f32 for a partial query tile. At KC=32 the total is
/// `24*hd + 2016` bytes (8,160 bytes at head_dim=256).
fn renderPrefillFlashBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    schedule: KernelSchedule,
) !void {
    const kc: usize = schedule.key_chunk;
    const skip = schedule.skip_rescale;
    const sp_off = 32 * kc;
    const sphys_off = 48 * kc;
    const sm_off = 52 * kc;
    const ss_off = 52 * kc + 32;
    const changed_off = 52 * kc + 64;
    const diag_off = 52 * kc + 96;
    const pv_tiles = kc / 8;
    const tiles_per_sg = kc / 32;
    const keys_per_lane = kc / 32;

    const so_off = diag_off + 256;

    // ---- signature + prologue ----
    try appendFmt(allocator, out, "kernel void {s}(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {{\n", .{kernel_id});
    try out.appendSlice(allocator, "    const uint q0 = tg.x * 8u; const uint h = tg.y;\n");
    try out.appendSlice(allocator, "    if (q0 >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || (p.page_size & 7u) != 0u || p.num_kv_heads == 0u || p.num_heads % p.num_kv_heads != 0u || p.head_dim != 256u) return;\n");
    try out.appendSlice(allocator, "    const float scale = rsqrt(256.0f); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * 256u;\n");
    try out.appendSlice(allocator, "    const uint q_stride = p.num_heads * 256u; const uint k_row_halfs = p.key_row_bytes / 2u;\n");
    try out.appendSlice(allocator, "    threadgroup half *sq = (threadgroup half *)shmem;\n");
    try out.appendSlice(allocator, "    threadgroup char *fb = shmem + 8u * 256u * 2u;\n");
    try out.appendSlice(allocator, "    threadgroup float *ss = (threadgroup float *)fb;\n");
    try appendFmt(allocator, out, "    threadgroup half *sp = (threadgroup half *)(fb + {d}u);\n", .{sp_off});
    try appendFmt(allocator, out, "    threadgroup uint *sphys = (threadgroup uint *)(fb + {d}u);\n", .{sphys_off});
    try appendFmt(allocator, out, "    threadgroup float *sM = (threadgroup float *)(fb + {d}u);\n", .{sm_off});
    try appendFmt(allocator, out, "    threadgroup float *sS = (threadgroup float *)(fb + {d}u);\n", .{ss_off});
    if (skip) try appendFmt(allocator, out, "    threadgroup atomic_uint *schanged = (threadgroup atomic_uint *)(fb + {d}u);\n", .{changed_off});
    try appendFmt(allocator, out, "    threadgroup float *sdiag = (threadgroup float *)(fb + {d}u);\n", .{diag_off});
    try appendFmt(allocator, out, "    threadgroup float *so = (threadgroup float *)(fb + {d}u);\n", .{so_off});
    try out.appendSlice(allocator, "    const device half *v_half = reinterpret_cast<const device half *>(v_bytes);\n");
    try out.appendSlice(allocator, "    const device half *k_half = reinterpret_cast<const device half *>(encoded_key);\n");
    try out.appendSlice(allocator, "    for (uint i = uint(tid); i < 8u * 256u; i += 128u) { uint j = i / 256u; uint d = i - j * 256u; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + h * 256u + d] * scale) : half(0.0f); }\n");
    try out.appendSlice(allocator, "    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; }\n");
    try out.appendSlice(allocator, "    if (tid < 64u) sdiag[tid] = 0.0f;\n");
    try out.appendSlice(allocator, "    const uint dslice = uint(sgitg) * 64u;\n");
    try out.appendSlice(allocator, "    simdgroup_float8x8 mo[8];\n");
    try out.appendSlice(allocator, "    for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n");

    // ---- per-chunk online-softmax loop ----
    // For SWA, start at the chunk containing the first key that can be live for
    // the earliest query in this query tile. Align in logical-KV coordinates:
    // the page helper consumes logical token indices, even when physical pages
    // are biased or permuted. The boundary chunk remains in the loop so the
    // existing per-key mask removes its expired prefix exactly as before.
    try out.appendSlice(allocator, "    const uint q_last = min(q0 + 7u, p.q_len - 1u); const uint tile_first_q = p.query_position_offset + q0; const uint tile_last_q = p.query_position_offset + q_last;\n");
    try appendFmt(allocator, out, "    uint kc_start = 0u; if (p.swa_scan_clamp != 0u && p.sliding_window != 0u) {{ const uint window_minus_one = p.sliding_window - 1u; const uint earliest_live_key = tile_first_q >= window_minus_one ? tile_first_q - window_minus_one : 0u; if (earliest_live_key > p.kv_position_offset) {{ const uint earliest_logical_key = earliest_live_key - p.kv_position_offset; kc_start = (earliest_logical_key / {d}u) * {d}u; }} }}\n", .{ kc, kc });
    try appendFmt(allocator, out, "    for (uint kc = kc_start; kc < p.kv_tokens; kc += {d}u) {{\n", .{kc});
    try out.appendSlice(allocator, "        threadgroup_barrier(mem_flags::mem_threadgroup);\n");
    try appendFmt(allocator, out, "        uint key_first = p.kv_position_offset + kc; uint key_last = p.kv_position_offset + min(kc + {d}u, p.kv_tokens - 1u); bool wholly_future = key_first > tile_last_q; bool wholly_expired = p.sliding_window != 0u && tile_first_q >= key_last && tile_first_q - key_last >= p.sliding_window; if (wholly_future) break; if (wholly_expired) continue;\n", .{kc - 1});
    try appendFmt(allocator, out, "        if (tid < {d}u) {{ uint ki = kc + uint(tid); sphys[tid] = ki < p.kv_tokens ? antfly_paged_attention_1x_page_token(block_table, p, ki) : 0xffffffffu; }}\n", .{kc});
    if (skip) try out.appendSlice(allocator, "        if (tid == 0u) atomic_store_explicit(schanged, 0u, memory_order_relaxed);\n");
    try out.appendSlice(allocator, "        threadgroup_barrier(mem_flags::mem_threadgroup);\n");

    // Q·Kᵀ: page_size is a multiple of 8, so each 8-token matrix load is
    // physically contiguous even when the page table itself is not.
    if (kc == 32) {
        try out.appendSlice(allocator, "        simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f);\n");
        try out.appendSlice(allocator, "        uint phys = sphys[uint(sgitg) * 8u]; const device half *kbase = k_half + (phys != 0xffffffffu ? phys : 0u) * k_row_halfs + kv_head_base; for (uint d = 0u; d < 256u; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, 256u); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); }\n");
        try out.appendSlice(allocator, "        simdgroup_store(ms, ss + uint(sgitg) * 8u, 32u, 0, false);\n");
    } else {
        try appendFmt(allocator, out, "        for (uint kt = 0u; kt < {d}u; ++kt) {{\n", .{tiles_per_sg});
        try out.appendSlice(allocator, "            uint ktile = uint(sgitg) + kt * 4u;\n");
        try out.appendSlice(allocator, "            simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f);\n");
        try out.appendSlice(allocator, "            uint phys = sphys[ktile * 8u]; const device half *kbase = k_half + (phys != 0xffffffffu ? phys : 0u) * k_row_halfs + kv_head_base; for (uint d = 0u; d < 256u; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, 256u); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); }\n");
        try appendFmt(allocator, out, "            simdgroup_store(ms, ss + ktile * 8u, {d}u, 0, false);\n", .{kc});
        try out.appendSlice(allocator, "        }\n");
    }

    try out.appendSlice(allocator, "        threadgroup_barrier(mem_flags::mem_threadgroup);\n");

    // Score + online-softmax update. KC=32: 1 key per lane. KC>32: KC/32 keys
    // per lane, pre-combined before the simd reduce.
    if (kc == 32) {
        try out.appendSlice(allocator, "        for (uint jj = 0u; jj < 2u; ++jj) {\n");
        try out.appendSlice(allocator, "            uint j = uint(sgitg) + jj * 4u; uint qi = q0 + j; uint query_pos = p.query_position_offset + qi; uint kk = uint(lane); uint ki = kc + kk;\n");
        try out.appendSlice(allocator, "            bool allowed = ki < p.kv_tokens && qi < p.q_len && sphys[kk] != 0xffffffffu;\n");
        try out.appendSlice(allocator, "            if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; }\n");
        try out.appendSlice(allocator, "            float sc = allowed ? ss[j * 32u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f;\n");
        try out.appendSlice(allocator, "            float row_max = simd_max(sc); float m_old = sM[j]; float m_new = max(m_old, row_max);\n");
        try out.appendSlice(allocator, "            float e = 0.0f; float corr = 1.0f;\n");
        try out.appendSlice(allocator, "            if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; e = sc > -3.0e+38f ? exp(sc - m_new) : 0.0f; }\n");
        try out.appendSlice(allocator, "            sp[j * 32u + kk] = half(e);\n");
        try out.appendSlice(allocator, "            float row_sum = simd_sum(e);\n");
        if (skip) {
            try out.appendSlice(allocator, "            if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; if (m_new > m_old) atomic_fetch_or_explicit(schanged, 1u, memory_order_relaxed); }\n");
        } else {
            try out.appendSlice(allocator, "            if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; }\n");
        }
        try out.appendSlice(allocator, "        }\n");
    } else {
        try out.appendSlice(allocator, "        for (uint jj = 0u; jj < 2u; ++jj) {\n");
        try out.appendSlice(allocator, "            uint j = uint(sgitg) + jj * 4u; uint qi = q0 + j; uint query_pos = p.query_position_offset + qi;\n");
        try appendFmt(allocator, out, "            float m_old = sM[j]; float row_max = -3.402823466e+38f; float scs[{d}];\n", .{keys_per_lane});
        try appendFmt(allocator, out, "            for (uint kl = 0u; kl < {d}u; ++kl) {{\n", .{keys_per_lane});
        try out.appendSlice(allocator, "                uint kk = uint(lane) + kl * 32u; uint ki = kc + kk;\n");
        try out.appendSlice(allocator, "                bool allowed = ki < p.kv_tokens && qi < p.q_len && sphys[kk] != 0xffffffffu;\n");
        try out.appendSlice(allocator, "                if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; }\n");
        try appendFmt(allocator, out, "                float sc = allowed ? ss[j * {d}u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f;\n", .{kc});
        try out.appendSlice(allocator, "                scs[kl] = sc; row_max = max(row_max, sc);\n");
        try out.appendSlice(allocator, "            }\n");
        try out.appendSlice(allocator, "            row_max = simd_max(row_max); float m_new = max(m_old, row_max); float corr = 1.0f;\n");
        try out.appendSlice(allocator, "            if (m_new > -3.0e+38f) corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f;\n");
        try out.appendSlice(allocator, "            float row_sum_local = 0.0f;\n");
        try appendFmt(allocator, out, "            for (uint kl = 0u; kl < {d}u; ++kl) {{\n", .{keys_per_lane});
        try out.appendSlice(allocator, "                uint kk = uint(lane) + kl * 32u; float sc = scs[kl]; float e = (m_new > -3.0e+38f && sc > -3.0e+38f) ? exp(sc - m_new) : 0.0f;\n");
        try appendFmt(allocator, out, "                sp[j * {d}u + kk] = half(e); row_sum_local += e;\n", .{kc});
        try out.appendSlice(allocator, "            }\n");
        try out.appendSlice(allocator, "            float row_sum = simd_sum(row_sum_local);\n");
        if (skip) {
            try out.appendSlice(allocator, "            if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; if (m_new > m_old) atomic_fetch_or_explicit(schanged, 1u, memory_order_relaxed); }\n");
        } else {
            try out.appendSlice(allocator, "            if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; }\n");
        }
        try out.appendSlice(allocator, "        }\n");
    }

    try out.appendSlice(allocator, "        threadgroup_barrier(mem_flags::mem_threadgroup);\n");

    // O-accumulator diagonal rescale. `skip_rescale` guards it behind the
    // per-chunk "any row's running max moved" flag (an identity multiply when it
    // did not). The load + multiply are simdgroup-uniform, and `schanged[0]` is
    // threadgroup-uniform after the barrier above, so the branch is coherent.
    if (skip) {
        try out.appendSlice(allocator, "        if (atomic_load_explicit(schanged, memory_order_relaxed) != 0u) { simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u); for (uint i = 0u; i < 8u; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; } }\n");
    } else {
        try out.appendSlice(allocator, "        simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u);\n");
        try out.appendSlice(allocator, "        for (uint i = 0u; i < 8u; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; }\n");
    }
    // P·V accumulation. Load each shared score tile once and reuse it across
    // all output slices; every output accumulator still visits keys in the same
    // order, so this only removes redundant threadgroup reads.
    try appendFmt(allocator, out, "        for (uint kk8 = 0u; kk8 < {d}u; ++kk8) {{ uint phys = sphys[kk8 * 8u]; simdgroup_half8x8 mp; simdgroup_load(mp, sp + kk8 * 8u, {d}u); for (uint dt = 0u; dt < 8u; ++dt) {{ uint d8 = dslice + dt * 8u; simdgroup_half8x8 mv; if (kc + kk8 * 8u + 8u <= p.kv_tokens) {{ const device half *vbase = v_half + phys * p.v_row_stride + kv_head_base + d8; simdgroup_load(mv, vbase, p.v_row_stride); }} else {{ threadgroup half *sv = (threadgroup half *)ss + uint(sgitg) * 64u; for (uint vi = uint(lane); vi < 64u; vi += 32u) {{ uint vr = vi / 8u; uint vc = vi - vr * 8u; uint vphys = sphys[kk8 * 8u + vr]; sv[vi] = vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride + kv_head_base + d8 + vc] : half(0.0f); }} simdgroup_barrier(mem_flags::mem_threadgroup); simdgroup_load(mv, sv, 8u); }} simdgroup_multiply_accumulate(mo[dt], mp, mv, mo[dt]); }} }}\n", .{ pv_tiles, kc });
    try out.appendSlice(allocator, "    }\n");

    // ---- epilogue: final inverse-sum normalise + direct strided store. The
    // partial query tile reuses one hd/4-wide threadgroup output tile. ----
    try out.appendSlice(allocator, "    threadgroup_barrier(mem_flags::mem_threadgroup);\n");
    try out.appendSlice(allocator, "    if (tid < 8u) { float denom = sS[tid]; sdiag[uint(tid) * 8u + uint(tid)] = denom > 0.0f ? 1.0f / denom : 0.0f; }\n");
    try out.appendSlice(allocator, "    threadgroup_barrier(mem_flags::mem_threadgroup);\n");
    try out.appendSlice(allocator, "    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u);\n");
    try out.appendSlice(allocator, "    for (uint dt = 0u; dt < 8u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); mo[dt] = scaled; }\n");
    try out.appendSlice(allocator, "    if (q0 + 8u <= p.q_len) { for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], output + q0 * q_stride + h * 256u + dslice + dt * 8u, q_stride); return; }\n");
    try out.appendSlice(allocator, "    const uint valid_rows = p.q_len - q0; for (uint owner = 0u; owner < 4u; ++owner) { threadgroup_barrier(mem_flags::mem_threadgroup); if (uint(sgitg) == owner) for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], so + dt * 8u, 64u); threadgroup_barrier(mem_flags::mem_threadgroup); for (uint i = uint(tid); i < valid_rows * 64u; i += 128u) { uint j = i / 64u; uint d = i - j * 64u; output[(q0 + j) * q_stride + h * 256u + owner * 64u + d] = so[i]; } }\n");
    try out.appendSlice(allocator, "}\n");
}

/// Gemma4 global-attention specialization: 8 queries, head_dim=512, KC=64,
/// 256 threads. All eight simdgroups produce Q*K^T and own 64 output columns.
/// K/V are read as page-local 8-token tiles, so shared memory is only Q (8 KiB),
/// softmax state, and one reusable 8x64 partial-tail store (13,888 bytes total).
fn renderPrefillFlashHd512Body(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
) !void {
    try appendFmt(allocator, out, "kernel void {s}(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {{\n", .{kernel_id});
    try out.appendSlice(allocator, "    const uint hd = p.head_dim; const uint q0 = tg.x * 8u; const uint h = tg.y;\n" ++
        "    if (q0 >= p.q_len || h >= p.num_heads || p.format != 3u || p.page_size == 0u || (p.page_size & 7u) != 0u || p.num_kv_heads == 0u || p.num_heads % p.num_kv_heads != 0u || hd != 512u) return;\n" ++
        "    const float scale = rsqrt(512.0f); const uint heads_per_group = p.num_heads / p.num_kv_heads; const uint kv_head_base = (h / heads_per_group) * hd;\n" ++
        "    const uint q_stride = p.num_heads * hd; const uint k_row_halfs = p.key_row_bytes / 2u;\n" ++
        "    threadgroup half *sq = (threadgroup half *)shmem;\n" ++
        "    threadgroup char *fb = shmem + 8u * 512u * 2u;\n" ++
        "    threadgroup float *ss = (threadgroup float *)fb;\n" ++
        "    threadgroup half *sp = (threadgroup half *)(fb + 2048u);\n" ++
        "    threadgroup uint *sphys = (threadgroup uint *)(fb + 3072u);\n" ++
        "    threadgroup float *sM = (threadgroup float *)(fb + 3328u);\n" ++
        "    threadgroup float *sS = (threadgroup float *)(fb + 3360u);\n" ++
        "    threadgroup float *sdiag = (threadgroup float *)(fb + 3392u);\n" ++
        "    threadgroup float *so = (threadgroup float *)(fb + 3648u);\n" ++
        "    const device half *k_half = reinterpret_cast<const device half *>(encoded_key); const device half *v_half = reinterpret_cast<const device half *>(v_bytes);\n" ++
        "    for (uint i = uint(tid); i < 8u * 512u; i += 256u) { uint j = i / 512u; uint d = i - j * 512u; uint qi = q0 + j; sq[i] = qi < p.q_len ? half(q[qi * q_stride + h * 512u + d] * scale) : half(0.0f); }\n" ++
        "    if (tid < 8u) { sM[tid] = -3.402823466e+38f; sS[tid] = 0.0f; } if (tid < 64u) sdiag[tid] = 0.0f;\n" ++
        "    const uint dslice = uint(sgitg) * 64u; simdgroup_float8x8 mo[8]; for (uint i = 0u; i < 8u; ++i) mo[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n");

    try out.appendSlice(allocator, "    for (uint kc = 0u; kc < p.kv_tokens; kc += 64u) {\n" ++
        "        threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
        "        uint q_last = min(q0 + 7u, p.q_len - 1u); uint tile_first_q = p.query_position_offset + q0; uint tile_last_q = p.query_position_offset + q_last; uint key_first = p.kv_position_offset + kc; uint key_last = p.kv_position_offset + min(kc + 63u, p.kv_tokens - 1u); bool wholly_future = key_first > tile_last_q; bool wholly_expired = p.sliding_window != 0u && tile_first_q >= key_last && tile_first_q - key_last >= p.sliding_window; if (wholly_future) break; if (wholly_expired) continue;\n" ++
        "        if (tid < 64u) { uint ki = kc + uint(tid); sphys[tid] = ki < p.kv_tokens ? antfly_paged_attention_1x_page_token(block_table, p, ki) : 0xffffffffu; }\n" ++
        "        threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
        "        uint ktile = uint(sgitg); uint phys = sphys[ktile * 8u]; const device half *kbase = k_half + (phys != 0xffffffffu ? phys : 0u) * k_row_halfs + kv_head_base; simdgroup_float8x8 ms = make_filled_simdgroup_matrix<float, 8>(0.0f); for (uint d = 0u; d < 512u; d += 8u) { simdgroup_half8x8 mq; simdgroup_half8x8 mk; simdgroup_load(mq, sq + d, 512u); simdgroup_load(mk, kbase + d, k_row_halfs, 0, true); simdgroup_multiply_accumulate(ms, mq, mk, ms); } simdgroup_store(ms, ss + ktile * 8u, 64u, 0, false);\n" ++
        "        threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
        "        uint j = uint(sgitg); uint qi = q0 + j; uint query_pos = p.query_position_offset + qi; float scores[2]; float row_max = -3.402823466e+38f; for (uint kl = 0u; kl < 2u; ++kl) { uint kk = uint(lane) + kl * 32u; uint ki = kc + kk; bool allowed = ki < p.kv_tokens && qi < p.q_len && sphys[kk] != 0xffffffffu; if (allowed) { uint key_pos = p.kv_position_offset + ki; allowed = key_pos <= query_pos; if (p.sliding_window != 0u && allowed) allowed = (query_pos - key_pos) < p.sliding_window; } float sc = allowed ? ss[j * 64u + kk] : -3.402823466e+38f; if (!isfinite(sc)) sc = -3.402823466e+38f; scores[kl] = sc; row_max = max(row_max, sc); } row_max = simd_max(row_max); float m_old = sM[j]; float m_new = max(m_old, row_max); float corr = 1.0f; if (m_new > -3.0e+38f) corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; float row_sum_local = 0.0f; for (uint kl = 0u; kl < 2u; ++kl) { uint kk = uint(lane) + kl * 32u; float sc = scores[kl]; float e = (m_new > -3.0e+38f && sc > -3.0e+38f) ? exp(sc - m_new) : 0.0f; sp[j * 64u + kk] = half(e); row_sum_local += e; } float row_sum = simd_sum(row_sum_local); if (lane == 0u) { sS[j] = sS[j] * corr + row_sum; sM[j] = m_new; sdiag[j * 8u + j] = corr; }\n" ++
        "        threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
        "        simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u); for (uint i = 0u; i < 8u; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; }\n" ++
        "        for (uint kk8 = 0u; kk8 < 8u; ++kk8) { uint phys_v = sphys[kk8 * 8u]; simdgroup_half8x8 mp; simdgroup_load(mp, sp + kk8 * 8u, 64u); for (uint dt = 0u; dt < 8u; ++dt) { uint d8 = dslice + dt * 8u; simdgroup_half8x8 mv; if (kc + kk8 * 8u + 8u <= p.kv_tokens) { const device half *vbase = v_half + phys_v * p.v_row_stride + kv_head_base + d8; simdgroup_load(mv, vbase, p.v_row_stride); } else { threadgroup half *sv = (threadgroup half *)ss + uint(sgitg) * 64u; for (uint vi = uint(lane); vi < 64u; vi += 32u) { uint vr = vi / 8u; uint vc = vi - vr * 8u; uint vphys = sphys[kk8 * 8u + vr]; sv[vi] = vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride + kv_head_base + d8 + vc] : half(0.0f); } simdgroup_barrier(mem_flags::mem_threadgroup); simdgroup_load(mv, sv, 8u); } simdgroup_multiply_accumulate(mo[dt], mp, mv, mo[dt]); } }\n" ++
        "    }\n");

    try out.appendSlice(allocator, "    threadgroup_barrier(mem_flags::mem_threadgroup); if (tid < 8u) { float denom = sS[tid]; sdiag[uint(tid) * 8u + uint(tid)] = denom > 0.0f ? 1.0f / denom : 0.0f; } threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
        "    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u); for (uint dt = 0u; dt < 8u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); mo[dt] = scaled; }\n" ++
        "    if (q0 + 8u <= p.q_len) { for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], output + q0 * q_stride + h * 512u + dslice + dt * 8u, q_stride); return; }\n" ++
        "    const uint valid_rows = p.q_len - q0; for (uint owner = 0u; owner < 8u; ++owner) { threadgroup_barrier(mem_flags::mem_threadgroup); if (uint(sgitg) == owner) for (uint dt = 0u; dt < 8u; ++dt) simdgroup_store(mo[dt], so + dt * 8u, 64u); threadgroup_barrier(mem_flags::mem_threadgroup); for (uint i = uint(tid); i < valid_rows * 64u; i += 256u) { uint j = i / 64u; uint d = i - j * 64u; output[(q0 + j) * q_stride + h * 512u + owner * 64u + d] = so[i]; } }\n" ++
        "}\n");
}

/// One generated Metal kernel to emit into the shared runtime region: its
/// stable kernel name plus the descriptor/schedule/epilogue it renders from.
///
/// `op_kind` selects which body renderer runs. The matmul fields
/// (`decoder`/`epilogue`) are only read for `.small_batch_matmul`; the
/// `microkernel` field only for `.microkernel`; the `attention` field only for
/// `.attention`. All carry inert defaults so a caller only fills the ones its
/// op-kind needs — existing matmul construction sites (which set
/// `decoder`/`schedule`/`epilogue`) stay byte-identical.
pub const RegionKernel = struct {
    kernel_id: []const u8,
    op_kind: OpKind = .small_batch_matmul,
    decoder: FormatDecoder = decoder_q4_0,
    schedule: KernelSchedule,
    epilogue: Epilogue = .none,
    microkernel: MicrokernelKind = .rms_norm,
    attention: AttentionKind = .decode_1x,
};

/// Renders the shared runtime region (all routes in one Metal compilation unit):
/// the union of vocabulary helpers + per-format dequant fragments (each emitted
/// exactly once, in first-use order), followed by every kernel body. This is the
/// single source of truth for both the `.metal` files and the metal_kernels.m
/// runtime region — the renderer, not any frozen constant.
pub fn renderRuntimeRegion(
    allocator: std.mem.Allocator,
    kernels: []const RegionKernel,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    // Pass 1: emit the deduped helper vocabulary + dequant fragments once each,
    // in the order they are first referenced across the routes. Microkernels
    // carry no dequant decoder; they share the same dedup set so any future
    // microkernel helper joins the matmul vocabulary without duplication.
    var emitted_names = EmittedNames{};
    for (kernels) |k| {
        switch (k.op_kind) {
            .small_batch_matmul => {
                try k.schedule.validate(k.decoder.format.valuesPerBlock() orelse return error.MissingBlockValues);
                try validateEpilogueSchedule(k.schedule, k.epilogue);
                try validateTiledSchedule(k.decoder.format, k.schedule, k.epilogue);
                for (k.decoder.helpers) |helper| try emitHelper(allocator, &out, &emitted_names, helper);
                if (k.epilogue == .bias_gelu) try emitHelper(allocator, &out, &emitted_names, helper_qk_gelu);
                try emitHelper(allocator, &out, &emitted_names, .{ .name = k.decoder.lane_decode_fn, .msl = k.decoder.lane_decode_msl });
            },
            .microkernel => try validateMicrokernelSchedule(k.microkernel, k.schedule),
            // Attention kernels are self-contained: their params struct + paging
            // helper join the shared dedup set (emitted once each), so the
            // runtime region and the standalone `.metal` carry identical bytes.
            .attention => {
                try validateAttentionSchedule(k.attention, k.schedule);
                for (attentionHelpers(k.attention)) |helper| try emitHelper(allocator, &out, &emitted_names, helper);
            },
        }
    }

    // Pass 2: emit every kernel body.
    for (kernels) |k| {
        switch (k.op_kind) {
            .small_batch_matmul => {
                const block_values = k.decoder.format.valuesPerBlock() orelse return error.MissingBlockValues;
                const block_bytes = k.decoder.format.bytesPerBlock() orelse return error.MissingBlockBytes;
                try renderBody(allocator, &out, k.kernel_id, k.decoder, k.schedule, k.epilogue, block_values, block_bytes);
            },
            .microkernel => try renderMicrokernelBody(allocator, &out, k.kernel_id, k.microkernel, k.schedule),
            .attention => try renderAttentionBody(allocator, &out, k.kernel_id, k.attention, k.schedule),
        }
    }
    return out.toOwnedSlice(allocator);
}

/// Tracks the helper names already emitted so the shared vocabulary is written
/// exactly once. Backed by a fixed buffer (no allocation) so `renderKernel` and
/// `renderRuntimeRegion` stay usable at comptime — the checked-in `.metal`
/// sources render this way (a heap `ArrayList([]const u8)` would need pointer
/// alignment via `@intFromPtr`, which comptime cannot evaluate). The region has
/// ~18 distinct helper names across all routes; 64 leaves ample headroom.
const EmittedNames = struct {
    names: [64][]const u8 = undefined,
    len: usize = 0,

    fn contains(self: *const EmittedNames, name: []const u8) bool {
        for (self.names[0..self.len]) |n| {
            if (std.mem.eql(u8, n, name)) return true;
        }
        return false;
    }

    fn add(self: *EmittedNames, name: []const u8) void {
        std.debug.assert(self.len < self.names.len);
        self.names[self.len] = name;
        self.len += 1;
    }
};

fn emitHelper(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    emitted: *EmittedNames,
    helper: HelperFragment,
) !void {
    if (emitted.contains(helper.name)) return;
    emitted.add(helper.name);
    try out.appendSlice(allocator, helper.msl);
    try out.append(allocator, '\n');
}

fn validateEpilogueSchedule(schedule: KernelSchedule, epilogue: Epilogue) !void {
    switch (epilogue) {
        .none, .bias, .bias_gelu, .relu => {},
        else => return error.UnsupportedEpilogue,
    }
    // Two-column kernels only exist on the single-simdgroup reduction.
    if (schedule.cols_per_threadgroup == 2 and schedule.reduction != .simd_sum and schedule.reduction != .simdgroup_tiled) {
        return error.TwoColRequiresSimdSum;
    }
    // bias_gelu two-column is supported (q8_0); relu is single-column only in
    // the current route set but the skeleton handles it either way.
}

fn validateTiledSchedule(
    format: quant_matmul.Format,
    schedule: KernelSchedule,
    epilogue: Epilogue,
) !void {
    if (schedule.reduction == .simdgroup_matrix) {
        if (format != .q4_k and format != .q6_k) return error.MatrixRequiresSupportedFormat;
        if (epilogue != .none) return error.MatrixRequiresNoEpilogue;
        return;
    }
    if (schedule.reduction != .simdgroup_tiled) return;
    if (format != .q4_0 and format != .q4_k and format != .q6_k) return error.TiledRequiresSupportedFormat;
    if (format == .q4_0 and epilogue != .none) return error.PackedTiledRequiresNoEpilogue;
    if (format != .q4_0 and epilogue != .none and epilogue != .bias and epilogue != .bias_gelu) {
        return error.UnsupportedEpilogue;
    }
}

fn renderBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    epilogue: Epilogue,
    block_values: usize,
    block_bytes: usize,
) !void {
    const has_bias = epilogue == .bias or epilogue == .bias_gelu;

    // Signature. Buffer layout: input=0, weight=1, [bias=2], output, rows,
    // in_dim, out_dim; bias shifts output and the scalars up by one slot.
    const bias_param = if (has_bias) "device const float *bias [[buffer(2)]], " else "";
    const out_idx: u8 = if (has_bias) 3 else 2;
    const simd_params = if (schedule.reduction == .hybrid_simd or schedule.reduction == .simdgroup_tiled or schedule.reduction == .simdgroup_matrix)
        ", ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]"
    else
        "";
    try appendFmt(allocator, out, "kernel void {s}(device const float *input [[buffer(0)]], device const uchar *{s} [[buffer(1)]], {s}device float *output [[buffer({d})]], constant int &rows [[buffer({d})]], constant int &in_dim [[buffer({d})]], constant int &out_dim [[buffer({d})]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]{s}) {{\n", .{ kernel_id, decoder.weight_param, bias_param, out_idx, out_idx + 1, out_idx + 2, out_idx + 3, simd_params });

    try renderMatmulComputation(allocator, out, decoder, schedule, epilogue, block_values, block_bytes);
    try out.appendSlice(allocator, "}\n");
}

fn renderQkvBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    block_values: usize,
    block_bytes: usize,
) !void {
    const simd_params = if (schedule.reduction == .hybrid_simd or schedule.reduction == .simdgroup_tiled or schedule.reduction == .simdgroup_matrix)
        ", ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]]"
    else
        "";
    try appendFmt(allocator, out, "kernel void {s}(device const float *input [[buffer(0)]], device const uchar *weight_q [[buffer(1)]], device const uchar *weight_k [[buffer(2)]], device const uchar *weight_v [[buffer(3)]], device float *output_q [[buffer(4)]], device float *output_k [[buffer(5)]], device float *output_v [[buffer(6)]], constant int &rows [[buffer(7)]], constant int &in_dim [[buffer(8)]], constant int &out_dim [[buffer(9)]], uint3 thread_pos [[thread_position_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]{s}) {{\n", .{ kernel_id, simd_params });
    try appendFmt(allocator, out, "    if (group_pos.z >= 3u) return; device const uchar *{s} = group_pos.z == 0u ? weight_q : (group_pos.z == 1u ? weight_k : weight_v); device float *output = group_pos.z == 0u ? output_q : (group_pos.z == 1u ? output_k : output_v);\n", .{decoder.weight_param});
    try renderMatmulComputation(allocator, out, decoder, schedule, .none, block_values, block_bytes);
    try out.appendSlice(allocator, "}\n");
}

fn renderMatmulComputation(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    epilogue: Epilogue,
    block_values: usize,
    block_bytes: usize,
) !void {
    const threads = schedule.threads_per_threadgroup;
    const mask = block_values - 1;
    const shift = log2Usize(block_values);
    const two_col = schedule.cols_per_threadgroup == 2;

    if (schedule.reduction == .simdgroup_matrix) {
        try renderSimdgroupMatrixBody(allocator, out, decoder, schedule);
    } else if (schedule.reduction == .simdgroup_tiled) {
        switch (decoder.format) {
            .q4_0 => try renderPackedQ4_0TiledBody(allocator, out, decoder, schedule),
            .q4_k => try renderQ4KTiledBody(allocator, out, decoder, schedule, epilogue),
            .q6_k => try renderQ6KTiledBody(allocator, out, decoder, schedule, epilogue),
            else => return error.TiledRequiresSupportedFormat,
        }
    } else if (two_col) {
        try renderTwoColBody(allocator, out, decoder, schedule, epilogue, mask, shift, block_values, block_bytes);
    } else {
        try renderSingleColBody(allocator, out, decoder, schedule, epilogue, threads, mask, shift, block_values, block_bytes);
    }
}

fn renderSimdgroupMatrixBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decoder: FormatDecoder,
    schedule: KernelSchedule,
) !void {
    const tile_rows: usize = schedule.rows_per_threadgroup;
    const threads: usize = schedule.threads_per_threadgroup;
    const input_elements = tile_rows * 32;
    const result_elements = tile_rows * 64;
    const shared_bytes = @max((2048 + input_elements) * @sizeOf(f16), result_elements * @sizeOf(f32));
    try appendFmt(
        allocator,
        out,
        "    const uint tid = thread_pos.x; const uint first_o = group_pos.x * 64u; const uint first_r = group_pos.y * {d}u;\n" ++
            "    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return;\n" ++
            "    threadgroup uchar shared[{d}]; threadgroup half *weight_tile = (threadgroup half *)shared; threadgroup half *input_tile = weight_tile + 2048;\n" ++
            "    simdgroup_half8x8 mw[8]; simdgroup_half8x8 mx; simdgroup_float8x8 acc[8];\n" ++
            "    for (uint i = 0u; i < 8u; ++i) acc[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);\n" ++
            "    const uint block_count = uint(in_dim) >> 8; const uint sg_row = uint(simdgroup_id) * 8u;\n" ++
            "    for (uint k0 = 0u; k0 < uint(in_dim); k0 += 32u) {{\n" ++
            "        for (uint index = tid; index < 2048u; index += {d}u) {{ uint k = index >> 6; uint col = index & 63u; uint global_col = first_o + col; uint global_k = k0 + k; half value = half(0.0f); if (global_col < uint(out_dim)) {{ device const uchar *block = ",
        .{ tile_rows, shared_bytes, threads },
    );
    try out.appendSlice(allocator, decoder.weight_param);
    try out.appendSlice(allocator, " + (global_col * block_count + (global_k >> 8)) * ");
    try appendFmt(allocator, out, "{d}u", .{decoder.format.bytesPerBlock().?});
    try out.appendSlice(allocator, "; value = half(");
    try out.appendSlice(allocator, decoder.lane_decode_fn);
    try appendFmt(
        allocator,
        out,
        "(block, int(global_k & 255u))); }} weight_tile[k * 64u + col] = value; }}\n" ++
            "        for (uint index = tid; index < {d}u; index += {d}u) {{ uint row = index >> 5; uint k = index & 31u; uint global_row = first_r + row; input_tile[row * 32u + k] = global_row < uint(rows) ? half(input[global_row * uint(in_dim) + k0 + k]) : half(0.0f); }}\n" ++
            "        threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
            "        for (uint k = 0u; k < 32u; k += 8u) {{ for (uint i = 0u; i < 8u; ++i) simdgroup_load(mw[i], weight_tile + k * 64u + i * 8u, 64u); simdgroup_load(mx, input_tile + sg_row * 32u + k, 32u); for (uint i = 0u; i < 8u; ++i) simdgroup_multiply_accumulate(acc[i], mx, mw[i], acc[i]); }}\n" ++
            "        threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
            "    }}\n" ++
            "    threadgroup float *result_tile = (threadgroup float *)shared; for (uint i = 0u; i < 8u; ++i) simdgroup_store(acc[i], result_tile + sg_row * 64u + i * 8u, 64u);\n" ++
            "    threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
            "    for (uint index = tid; index < {d}u; index += {d}u) {{ uint row = index >> 6; uint col = index & 63u; uint global_row = first_r + row; if (global_row < uint(rows) && first_o + col < uint(out_dim)) output[global_row * uint(out_dim) + first_o + col] = result_tile[index]; }}\n",
        .{ input_elements, threads, result_elements, threads },
    );
}

fn writeExpr(allocator: std.mem.Allocator, acc: []const u8, col: []const u8, epilogue: Epilogue) ![]u8 {
    return switch (epilogue) {
        .none => allocator.dupe(u8, acc),
        .relu => std.fmt.allocPrint(allocator, "max({s}, 0.0f)", .{acc}),
        .bias => std.fmt.allocPrint(allocator, "{s} + bias[{s}]", .{ acc, col }),
        .bias_gelu => std.fmt.allocPrint(allocator, "antfly_qk_gelu({s} + bias[{s}])", .{ acc, col }),
        else => error.UnsupportedEpilogue,
    };
}

fn renderSingleColBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    epilogue: Epilogue,
    threads: u16,
    mask: usize,
    shift: u6,
    block_values: usize,
    block_bytes: usize,
) !void {
    try appendFmt(allocator, out, "    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & {d}) != 0) return; float acc = 0.0f; int block_count = in_dim >> {d};\n", .{ mask, shift });
    // Accumulation: strided lane loop covers block_values with `threads` threads.
    try appendFmt(allocator, out, "    for (int block_idx = 0; block_idx < block_count; ++block_idx) {{ device const uchar *block = {s} + ((col * block_count + block_idx) * {d}); int base = block_idx << {d}; for (int lane = int(tid); lane < {d}; lane += {d}) acc += input[row * in_dim + base + lane] * {s}(block, lane); }}\n", .{ decoder.weight_param, block_bytes, shift, block_values, threads, decoder.lane_decode_fn });
    try renderReductionAndWrite(allocator, out, schedule, epilogue, threads);
}

fn renderReductionAndWrite(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schedule: KernelSchedule,
    epilogue: Epilogue,
    threads: u16,
) !void {
    const write = try writeExpr(allocator, reductionResultName(schedule.reduction), "col", epilogue);
    defer allocator.free(write);
    switch (schedule.reduction) {
        .simd_sum => try appendFmt(allocator, out, "    acc = simd_sum(acc); if (tid == 0) output[row * out_dim + col] = {s};\n", .{write}),
        .threadgroup_tree => try appendFmt(allocator, out, "    threadgroup float partial[{d}]; partial[tid] = acc; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = {d}u; stride > 0u; stride >>= 1) {{ if (tid < stride) partial[tid] += partial[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }} if (tid == 0) output[row * out_dim + col] = {s};\n", .{ threads, threads / 2, write }),
        .hybrid_simd => try appendFmt(allocator, out, "    threadgroup float partial[32]; acc = simd_sum(acc); if (lane_id == 0u) partial[simdgroup_id] = acc; if (simdgroup_id == 0u && lane_id >= {d}u) partial[lane_id] = 0.0f; threadgroup_barrier(mem_flags::mem_threadgroup); float total = simd_sum(partial[lane_id]); if (lane_id == 0u && simdgroup_id == 0u) output[row * out_dim + col] = {s};\n", .{ threads / 32, write }),
        .simdgroup_tiled => return error.TiledReductionRequiresPackedBody,
        .simdgroup_matrix => return error.MatrixReductionRequiresPackedBody,
    }
}

fn reductionResultName(reduction: ReductionKind) []const u8 {
    return switch (reduction) {
        .simd_sum => "acc",
        .threadgroup_tree => "partial[0]",
        .hybrid_simd => "total",
        .simdgroup_tiled => "acc",
        .simdgroup_matrix => "acc",
    };
}

/// Packed Q4_0 register tile for an exact two-row projection. Each simdgroup
/// owns 2, 4, or 8 output columns and reuses their packed ushort weights across
/// both input rows.
fn renderPackedQ4_0TiledBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decoder: FormatDecoder,
    schedule: KernelSchedule,
) !void {
    const simdgroups = schedule.threads_per_threadgroup / 32;
    const cols_per_simdgroup = schedule.cols_per_threadgroup / @as(u8, @intCast(simdgroups));

    try appendFmt(allocator, out, "    const uint NSG = {d}u; const uint NR0 = {d}u; const uint NQ16 = 16u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NR0; const uint first_r = group_pos.y * 2u;\n", .{ simdgroups, cols_per_simdgroup });
    try appendFmt(allocator, out, "    if (rows != 2 || in_dim <= 0 || (uint(in_dim) & 31u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 5; const uint ix = uint(lane_id) >> 1; const uint il = (uint(lane_id) & 1u) * 8u; float acc[{d}][2]; for (uint c = 0u; c < NR0; ++c) for (uint rr = 0u; rr < 2u; ++rr) acc[c][rr] = 0.0f;\n", .{cols_per_simdgroup});
    try appendFmt(allocator, out, "    for (uint b = ix; b < block_count; b += NQ16) {{ ushort qs_cache[{d}][4]; float dscale[{d}]; for (uint c = 0u; c < NR0; ++c) {{ uint col = first_o + c; if (col >= uint(out_dim)) {{ dscale[c] = 0.0f; for (uint q = 0u; q < 4u; ++q) qs_cache[c][q] = 0u; continue; }} uint off = (col * block_count + b) * 18u; ushort bits = (ushort({s}[off + 1u]) << 8) | ushort({s}[off]); dscale[c] = float(as_type<half>(bits)); const device ushort *qs = reinterpret_cast<const device ushort *>({s} + off + 2u) + (il >> 1); for (uint q = 0u; q < 4u; ++q) qs_cache[c][q] = qs[q]; }}\n", .{ cols_per_simdgroup, cols_per_simdgroup, decoder.weight_param, decoder.weight_param, decoder.weight_param });
    try out.appendSlice(allocator, "        for (uint rr = 0u; rr < 2u; ++rr) { uint in_off = (first_r + rr) * uint(in_dim) + b * 32u + il; float yl[16]; float sumy = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { float x0 = input[in_off + i]; float x1 = input[in_off + i + 1u]; float x16 = input[in_off + 16u + i]; float x17 = input[in_off + 16u + i + 1u]; sumy += x0 + x1 + x16 + x17; yl[i] = x0; yl[i + 1u] = x1 / 256.0f; yl[i + 8u] = x16 / 16.0f; yl[i + 9u] = x17 / 4096.0f; }\n");
    try out.appendSlice(allocator, "            for (uint c = 0u; c < NR0; ++c) { float qacc = 0.0f; for (uint i = 0u; i < 8u; i += 2u) { uint packed = uint(qs_cache[c][i >> 1]); qacc += yl[i] * float(packed & 0x000Fu); qacc += yl[i + 1u] * float(packed & 0x0F00u); qacc += yl[i + 8u] * float(packed & 0x00F0u); qacc += yl[i + 9u] * float(packed & 0xF000u); } acc[c][rr] += dscale[c] * (qacc - 8.0f * sumy); } } }\n");
    try out.appendSlice(allocator, "    for (uint c = 0u; c < NR0; ++c) for (uint rr = 0u; rr < 2u; ++rr) { float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = total; }\n");
}

/// Q4_K register tile. Each simdgroup owns several output columns, reusing one
/// activation slice across columns and one dequantized weight slice across the
/// scheduled row tile.
fn renderQ4KTiledBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    epilogue: Epilogue,
) !void {
    const simdgroups = schedule.threads_per_threadgroup / 32;
    const cols_per_simdgroup = schedule.cols_per_threadgroup / @as(u8, @intCast(simdgroups));

    try appendFmt(allocator, out, "    const uint NSG = {d}u; const uint NC = {d}u; const uint NR = {d}u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;\n", .{ simdgroups, cols_per_simdgroup, schedule.rows_per_threadgroup });
    try appendFmt(allocator, out, "    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 2; const uint j0 = (uint(lane_id) & 3u) << 3; const uint chunk = sub >> 1; const bool high = (sub & 1u) != 0u; float acc[{d}][{d}]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;\n", .{ cols_per_simdgroup, schedule.rows_per_threadgroup });
    try appendFmt(allocator, out, "    for (uint b = 0u; b < block_count; ++b) {{ uint input_off = first_r * uint(in_dim) + b * 256u + sub * 32u + j0; float x[{d}][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;\n", .{schedule.rows_per_threadgroup});
    try appendFmt(allocator, out, "        for (uint c = 0u; c < NC; ++c) {{ uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = {s} + (col * block_count + b) * 144u; float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(block + 4u, int(sub), raw_scale, raw_min); float dsc = antfly_qk_half_le_to_float(block) * raw_scale; float dmn = antfly_qk_half_le_to_float(block + 2u) * raw_min; device const uchar *qs = block + 16u + chunk * 32u + j0; for (uint i = 0u; i < 8u; ++i) {{ uchar packed = qs[i]; float w = dsc * float(high ? packed >> 4 : packed & 0x0fu) - dmn; for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; }} }} }}\n", .{decoder.weight_param});
    const write = try writeExpr(allocator, "total", "col", epilogue);
    defer allocator.free(write);
    try appendFmt(allocator, out, "    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) {{ float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = {s}; }}\n", .{write});
}

/// Q6_K register tile. Two lanes cover each 16-value scale subblock; every lane
/// decodes eight weights and reuses them across the scheduled row tile.
fn renderQ6KTiledBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    epilogue: Epilogue,
) !void {
    const simdgroups = schedule.threads_per_threadgroup / 32;
    const cols_per_simdgroup = schedule.cols_per_threadgroup / @as(u8, @intCast(simdgroups));

    try appendFmt(allocator, out, "    const uint NSG = {d}u; const uint NC = {d}u; const uint NR = {d}u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC; const uint first_r = group_pos.y * NR;\n", .{ simdgroups, cols_per_simdgroup, schedule.rows_per_threadgroup });
    try appendFmt(allocator, out, "    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return; const uint block_count = uint(in_dim) >> 8; const uint sub = uint(lane_id) >> 1; const uint j0 = (uint(lane_id) & 1u) << 3; const uint half_idx = sub >> 3; const uint group = (sub & 7u) >> 1; const uint l0 = ((sub & 1u) << 4) + j0; const uint ql_off = half_idx * 64u + (group & 1u) * 32u; const uint qh_off = half_idx * 32u; const uint qh_shift = group * 2u; const uint nibble_shift = (group >> 1) * 4u; float acc[{d}][{d}]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;\n", .{ cols_per_simdgroup, schedule.rows_per_threadgroup });
    try appendFmt(allocator, out, "    for (uint b = 0u; b < block_count; ++b) {{ uint input_off = first_r * uint(in_dim) + b * 256u + sub * 16u + j0; float x[{d}][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i) x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;\n", .{schedule.rows_per_threadgroup});
    try appendFmt(allocator, out, "        for (uint c = 0u; c < NC; ++c) {{ uint col = first_o + c; if (col >= uint(out_dim)) continue; device const uchar *block = {s} + (col * block_count + b) * 210u; device const uchar *ql = block; device const uchar *qh = block + 128u; int scale_u = int(block[192u + sub]); int scale = scale_u >= 128 ? scale_u - 256 : scale_u; float dscale = antfly_qk_half_le_to_float(block + 208u) * float(scale); for (uint i = 0u; i < 8u; ++i) {{ uint l = l0 + i; int low4 = int((ql[ql_off + l] >> nibble_shift) & 0x0fu); int high2 = int((qh[qh_off + l] >> qh_shift) & 0x03u); float w = dscale * float((low4 | (high2 << 4)) - 32); for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w; }} }} }}\n", .{decoder.weight_param});
    const write = try writeExpr(allocator, "total", "col", epilogue);
    defer allocator.free(write);
    try appendFmt(allocator, out, "    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) {{ float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr; if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = {s}; }}\n", .{write});
}

fn renderTwoColBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    decoder: FormatDecoder,
    schedule: KernelSchedule,
    epilogue: Epilogue,
    mask: usize,
    shift: u6,
    block_values: usize,
    block_bytes: usize,
) !void {
    _ = schedule; // two-col is always simd_sum (validated)
    try appendFmt(allocator, out, "    uint tid = thread_pos.x; int col0 = int(group_pos.x << 1); int col1 = col0 + 1; int row = int(group_pos.y); if (row >= rows || rows < 2 || rows > 8 || col0 >= out_dim || (in_dim & {d}) != 0) return; float acc0 = 0.0f; float acc1 = 0.0f; int block_count = in_dim >> {d};\n", .{ mask, shift });
    try appendFmt(allocator, out, "    device const float *row_input = input + row * in_dim; device const uchar *col0_weight = {s} + col0 * block_count * {d}; bool has_col1 = col1 < out_dim; device const uchar *col1_weight = has_col1 ? {s} + col1 * block_count * {d} : col0_weight;\n", .{ decoder.weight_param, block_bytes, decoder.weight_param, block_bytes });
    try appendFmt(allocator, out, "    for (int block_idx = 0; block_idx < block_count; ++block_idx) {{ device const uchar *block0 = col0_weight + block_idx * {d}; device const uchar *block1 = col1_weight + block_idx * {d}; int base = block_idx << {d}; for (int lane = int(tid); lane < {d}; lane += {d}) {{ float x = row_input[base + lane]; acc0 += x * {s}(block0, lane); if (has_col1) acc1 += x * {s}(block1, lane); }} }}\n", .{ block_bytes, block_bytes, shift, block_values, @as(u16, 32), decoder.lane_decode_fn, decoder.lane_decode_fn });
    const w0 = try writeExpr(allocator, "acc0", "col0", epilogue);
    defer allocator.free(w0);
    const w1 = try writeExpr(allocator, "acc1", "col1", epilogue);
    defer allocator.free(w1);
    try appendFmt(allocator, out, "    acc0 = simd_sum(acc0); acc1 = simd_sum(acc1); if (tid == 0) {{ output[row * out_dim + col0] = {s}; if (has_col1) output[row * out_dim + col1] = {s}; }}\n", .{ w0, w1 });
}

// ---- Tests ---------------------------------------------------------------

test "metal renderer renders every production route and passes structure checks" {
    const allocator = std.testing.allocator;
    for (compiler.metal_production_schedules) |entry| {
        const decoder = decoderFor(entry.format) orelse return error.MissingDecoder;
        const kernel_id = try std.fmt.allocPrint(allocator, "antfly_{s}_small_batch{s}_msl_v2", .{
            @tagName(entry.format),
            epilogueSuffixForTest(entry.epilogue),
        });
        defer allocator.free(kernel_id);
        const source = try renderKernel(allocator, kernel_id, decoder, entry.schedule, entry.epilogue);
        defer allocator.free(source);

        // Kernel entrypoint present.
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, kernel_id));
        // Balanced braces.
        var depth: i32 = 0;
        for (source) |c| {
            if (c == '{') depth += 1 else if (c == '}') depth -= 1;
        }
        try std.testing.expectEqual(@as(i32, 0), depth);
        // Dequant fragment and its call present.
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, decoder.lane_decode_fn));
        // Reduction primitive present.
        switch (entry.schedule.reduction) {
            .simd_sum => try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simd_sum(")),
            .threadgroup_tree => try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "partial[tid] += partial[tid + stride]")),
            .hybrid_simd => {
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simd_sum(partial[lane_id])"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_id"));
            },
            .simdgroup_tiled => {
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint NSG"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simd_sum"));
            },
            .simdgroup_matrix => {
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_multiply_accumulate"));
            },
        }
        // Multi-simdgroup bodies need explicit SIMD lane/group coordinates.
        const has_simd_params = std.mem.containsAtLeast(u8, source, 1, "lane_id [[thread_index_in_simdgroup]]");
        try std.testing.expectEqual(entry.schedule.reduction == .hybrid_simd or entry.schedule.reduction == .simdgroup_tiled or entry.schedule.reduction == .simdgroup_matrix, has_simd_params);
        // Two-column marker matches schedule.
        const has_two_col = std.mem.containsAtLeast(u8, source, 1, "group_pos.x << 1");
        try std.testing.expectEqual(entry.schedule.cols_per_threadgroup == 2, has_two_col);
        // Epilogue write.
        switch (entry.epilogue) {
            .none => {},
            .bias => try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "+ bias[")),
            .bias_gelu => try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_qk_gelu(")),
            .relu => try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "max(")),
            else => return error.UnsupportedEpilogueForTest,
        }
        // No leftover per-format gelu/half duplication: v2 uses shared vocabulary.
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "termite_q8_0_block_scale"));
    }
}

test "metal renderer emits the shape-general two-row packed Q4_0 register-tile grid including NR2" {
    const allocator = std.testing.allocator;
    var schedules: [compiler.metal_schedule_candidate_capacity]KernelSchedule = undefined;
    const covered_shapes = [_]struct { in_dim: u64, out_dim: u64 }{
        .{ .in_dim = 10240, .out_dim = 2560 },
        .{ .in_dim = 2048, .out_dim = 2560 },
        .{ .in_dim = 2560, .out_dim = 2048 },
        .{ .in_dim = 2560, .out_dim = 512 },
        .{ .in_dim = 544, .out_dim = 255 },
    };
    for (covered_shapes) |shape| {
        const count = compiler.metalScheduleCandidatesForExactShape(.q4_0, .none, 2, shape.in_dim, shape.out_dim, &schedules);
        try std.testing.expectEqual(@as(usize, 8), count);
        if (shape.out_dim <= 512) {
            try std.testing.expectEqual(@as(u16, 32), schedules[1].threads_per_threadgroup);
            try std.testing.expectEqual(@as(u8, 2), schedules[1].cols_per_threadgroup);
        }
        for (schedules[0..count]) |schedule| {
            const source = try renderKernel(allocator, "antfly_q4_0_rows2_exact", decoder_q4_0, schedule, .none);
            defer allocator.free(source);
            const simdgroups = schedule.threads_per_threadgroup / 32;
            const cols_per_simdgroup = schedule.cols_per_threadgroup / @as(u8, @intCast(simdgroups));
            const grid_marker = try std.fmt.allocPrint(allocator, "const uint NSG = {d}u; const uint NR0 = {d}u", .{ simdgroups, cols_per_simdgroup });
            defer allocator.free(grid_marker);
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, grid_marker));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint NQ16 = 16u"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "reinterpret_cast<const device ushort *>"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float(packed & 0xF000u)"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rows != 2 || in_dim <= 0 || (uint(in_dim) & 31u) != 0u || out_dim <= 0"));
            try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "10240"));
            try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "2560"));
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "antfly_q4_0_dequant_lane_v2("));
            var brace_depth: i32 = 0;
            for (source) |c| {
                if (c == '{') brace_depth += 1 else if (c == '}') brace_depth -= 1;
                try std.testing.expect(brace_depth >= 0);
            }
            try std.testing.expectEqual(@as(i32, 0), brace_depth);
        }
    }
}

test "metal renderer emits a shape-general two-row Q4_K register tile" {
    const allocator = std.testing.allocator;
    const schedule = KernelSchedule{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled };
    const source = try renderKernel(allocator, "antfly_q4_k_tiled", decoder_q4_k, schedule, .none);
    defer allocator.free(source);

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rows < 2 || in_dim <= 0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float x[2][8]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_qk_unpack_scale_min_6bit"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "acc[c][rr] += x[rr][i] * w"));

    const nr4 = try renderKernel(allocator, "antfly_q4_k_tiled_nr4", decoder_q4_k, .{
        .threads_per_threadgroup = 64,
        .cols_per_threadgroup = 8,
        .rows_per_threadgroup = 4,
        .reduction = .simdgroup_tiled,
    }, .none);
    defer allocator.free(nr4);
    try std.testing.expect(std.mem.containsAtLeast(u8, nr4, 1, "const uint NR = 4u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr4, 1, "float acc[4][4]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr4, 1, "float x[4][8]"));

    const fused = try renderKernel(allocator, "antfly_q4_k_tiled_bias_gelu", decoder_q4_k, schedule, .bias_gelu);
    defer allocator.free(fused);
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "device const float *bias [[buffer(2)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "antfly_qk_gelu(total + bias[col])"));
}

test "metal renderer emits a shape-general two-row Q6_K register tile" {
    const allocator = std.testing.allocator;
    const schedule = KernelSchedule{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled };
    const source = try renderKernel(allocator, "antfly_q6_k_tiled", decoder_q6_k, schedule, .none);
    defer allocator.free(source);

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rows < 2 || in_dim <= 0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint sub = uint(lane_id) >> 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "block[192u + sub]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "high2 << 4"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "acc[c][rr] += x[rr][i] * w"));

    const nr8 = try renderKernel(allocator, "antfly_q6_k_tiled_nr8", decoder_q6_k, .{
        .threads_per_threadgroup = 128,
        .cols_per_threadgroup = 16,
        .rows_per_threadgroup = 8,
        .reduction = .simdgroup_tiled,
    }, .none);
    defer allocator.free(nr8);
    try std.testing.expect(std.mem.containsAtLeast(u8, nr8, 1, "const uint NR = 8u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr8, 1, "float acc[4][8]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr8, 1, "float x[8][8]"));

    const fused = try renderKernel(allocator, "antfly_q6_k_tiled_bias", decoder_q6_k, schedule, .bias);
    defer allocator.free(fused);
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "device const float *bias [[buffer(2)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "total + bias[col]"));
}

test "metal renderer emits a Q4_K and Q6_K simdgroup matrix tile" {
    const allocator = std.testing.allocator;
    inline for (.{ decoder_q4_k, decoder_q6_k }) |decoder| {
        inline for (.{
            .{ KernelSchedule{ .threads_per_threadgroup = 160, .cols_per_threadgroup = 64, .rows_per_threadgroup = 40, .reduction = .simdgroup_matrix }, "threadgroup uchar shared[10240]", "group_pos.y * 40u" },
            .{ KernelSchedule{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 64, .rows_per_threadgroup = 64, .reduction = .simdgroup_matrix }, "threadgroup uchar shared[16384]", "group_pos.y * 64u" },
        }) |case| {
            const source = try renderKernel(allocator, "antfly_quant_matrix", decoder, case[0], .none);
            defer allocator.free(source);
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, case[1]));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_multiply_accumulate"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "group_pos.x * 64u"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, case[2]));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "global_row < uint(rows)"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, decoder.lane_decode_fn));
        }
    }
}

test "metal renderer emits one exact QKV matrix dispatch" {
    const source = try renderQkvKernel(std.testing.allocator, "antfly_q4_k_qkv", decoder_q4_k, .{
        .threads_per_threadgroup = 256,
        .cols_per_threadgroup = 64,
        .rows_per_threadgroup = 64,
        .reduction = .simdgroup_matrix,
    });
    defer std.testing.allocator.free(source);

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "weight_q [[buffer(1)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "weight_v [[buffer(3)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "output_q [[buffer(4)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "output_v [[buffer(6)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "group_pos.z == 0u ? weight_q"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "threadgroup uchar shared[16384]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_multiply_accumulate"));
}

test "metal renderer validate rejects invalid schedules" {
    // simd_sum needs exactly 32 threads.
    try std.testing.expectError(error.SimdSumNeeds32Threads, (KernelSchedule{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 1, .reduction = .simd_sum }).validate(32));
    // tree/hybrid need >= 64 threads.
    try std.testing.expectError(error.MultiSimdgroupNeeds64Threads, (KernelSchedule{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree }).validate(256));
    // two-col requires simd_sum.
    const allocator = std.testing.allocator;
    const bad = renderKernel(allocator, "antfly_q4_k_x", decoder_q4_k, .{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 2, .reduction = .threadgroup_tree }, .none);
    try std.testing.expectError(error.TwoColRequiresSimdSum, bad);
    const tiled = KernelSchedule{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled };
    try std.testing.expectError(error.TiledRequiresSupportedFormat, renderKernel(allocator, "antfly_q4_1_x", decoder_q4_1, tiled, .none));
    try std.testing.expectError(error.PackedTiledRequiresNoEpilogue, renderKernel(allocator, "antfly_q4_0_bias_x", decoder_q4_0, tiled, .bias));
    const invalid_nr1 = KernelSchedule{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled };
    try std.testing.expectError(error.InvalidColCount, renderKernel(allocator, "antfly_q4_0_nr1_x", decoder_q4_0, invalid_nr1, .none));
    const matrix = KernelSchedule{ .threads_per_threadgroup = 160, .cols_per_threadgroup = 64, .rows_per_threadgroup = 40, .reduction = .simdgroup_matrix };
    try std.testing.expectError(error.MatrixRequiresSupportedFormat, renderKernel(allocator, "antfly_q5_k_matrix", decoder_q5_k, matrix, .none));
    try std.testing.expectError(error.MatrixRequiresNoEpilogue, renderKernel(allocator, "antfly_q4_k_matrix_bias", decoder_q4_k, matrix, .bias));
}

fn epilogueSuffixForTest(epilogue: Epilogue) []const u8 {
    return switch (epilogue) {
        .none => "",
        .bias => "_bias",
        .bias_gelu => "_bias_gelu",
        .relu => "_relu",
        else => "_x",
    };
}

test "metal renderer renders the RMSNorm microkernel with a balanced parallel reduction" {
    const allocator = std.testing.allocator;
    const schedule = KernelSchedule{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree };
    const source = try renderRmsNormKernel(allocator, "antfly_rms_norm_generated_msl_v1", schedule);
    defer allocator.free(source);

    // Entrypoint + self-contained scalar params (no dequant decoder, no epilogue).
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "kernel void antfly_rms_norm_generated_msl_v1("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "constant float &eps [[buffer(5)]]"));
    // rsqrt(mean(x^2)+eps) rescale, matching activations.rmsNorm.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rsqrt(partial[0] / float(hidden_size) + eps)"));
    // Reduction sized/strided to the schedule's thread count.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "threadgroup float partial[256]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (uint stride = 128u"));
    // No matmul artefacts leak into the microkernel.
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "weight_q4_0"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "out_dim"));
    // Balanced braces.
    var depth: i32 = 0;
    for (source) |c| {
        if (c == '{') depth += 1 else if (c == '}') depth -= 1;
    }
    try std.testing.expectEqual(@as(i32, 0), depth);
}

test "metal renderer rejects invalid microkernel schedules" {
    try std.testing.expectError(error.MicrokernelThreadsNotPowerOfTwo, validateMicrokernelSchedule(.rms_norm, .{ .threads_per_threadgroup = 96, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree }));
    try std.testing.expectError(error.MicrokernelColsMustBeOne, validateMicrokernelSchedule(.rms_norm, .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 2, .reduction = .threadgroup_tree }));
}

test "metal renderer renders the decode-1x paged attention kernel self-contained" {
    const allocator = std.testing.allocator;
    const schedule = KernelSchedule{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree };
    const source = try renderPagedAttention1xKernel(allocator, "antfly_paged_attention_1x_generated_msl_v1", schedule);
    defer allocator.free(source);

    // Entrypoint + the shared attention buffer layout (q/key/v/block_table/sinks/output/params).
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "kernel void antfly_paged_attention_1x_generated_msl_v1("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "device const uint *block_table [[buffer(3)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "constant antfly_paged_attention_1x_params &p [[buffer(6)]]"));
    // Self-contained: emits its own params struct + paging helper (no external
    // dependency on the hand-written termite_* copies), each exactly once.
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "struct antfly_paged_attention_1x_params {"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "inline uint antfly_paged_attention_1x_page_token("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float softcap; uint swa_scan_clamp;"));
    // Never references the hand-written names (would collide in the shared library
    // and would use-before-declaration in the runtime region).
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "termite_metal_paged_attention_params"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "termite_attention_page_token"));
    // f16 paged KV + causal/sliding mask + softmax structure markers.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (qi >= p.q_len || h >= p.num_heads || p.format != 3u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simd_sum(acc) * scale"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float e = (best > -3.0e+38f && score > -3.0e+38f) ? exp(score - best) : 0.0f"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "value += weight * select(float(v_col[as_type<uint>(phys_tokens[ki]) * p.v_row_stride]), 0.0f, weight == 0.0f)"));
    // NT/NSG parametrized to the schedule (256 threads -> 8 simdgroups).
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint NT = 256u; const uint NW = 32u; const uint NSG = 8u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "phys_tokens = shmem + p.kv_tokens + 256u"));
    // No matmul artefacts leak into the attention kernel.
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "weight_q4_0"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "out_dim"));
    // Balanced braces.
    var depth: i32 = 0;
    for (source) |c| {
        if (c == '{') depth += 1 else if (c == '}') depth -= 1;
    }
    try std.testing.expectEqual(@as(i32, 0), depth);
}

test "metal renderer parametrizes the decode-1x attention NSG from the thread count" {
    const allocator = std.testing.allocator;
    const schedule = KernelSchedule{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree };
    const source = try renderPagedAttention1xKernel(allocator, "antfly_attn_test", schedule);
    defer allocator.free(source);
    // 128 threads -> 4 simdgroups, partials array sized to NT.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint NT = 128u; const uint NW = 32u; const uint NSG = 4u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "phys_tokens = shmem + p.kv_tokens + 128u"));
}

test "metal renderer rejects invalid attention schedules" {
    // Not a multiple of the 32-lane simdgroup width.
    try std.testing.expectError(error.AttentionThreadsNotSimdgroupMultiple, validateAttentionSchedule(.decode_1x, .{ .threads_per_threadgroup = 48, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree }));
    // Multiple of 32 but not a power of two (the softmax tree halves each step).
    try std.testing.expectError(error.AttentionThreadsNotPowerOfTwo, validateAttentionSchedule(.decode_1x, .{ .threads_per_threadgroup = 96, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree }));
    // One threadgroup owns exactly one (query, head).
    try std.testing.expectError(error.AttentionColsMustBeOne, validateAttentionSchedule(.decode_1x, .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 2, .reduction = .threadgroup_tree }));
}

const flash_baseline_schedule = KernelSchedule{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = 32, .skip_rescale = false };

test "metal renderer renders the low-memory flash-prefill baseline self-contained" {
    const allocator = std.testing.allocator;
    const source = try renderPrefillFlashKernel(allocator, "antfly_paged_attention_prefill_flash_generated_msl_v1", flash_baseline_schedule);
    defer allocator.free(source);

    // Entrypoint + shared self-contained params/paging helper (reused from decode_1x).
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "kernel void antfly_paged_attention_prefill_flash_generated_msl_v1("));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "constant antfly_paged_attention_1x_params &p [[buffer(6)]]"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "struct antfly_paged_attention_1x_params {"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "inline uint antfly_paged_attention_1x_page_token("));
    // Never references the hand-written names.
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "termite_metal_paged_attention_params"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "termite_attention_page_token"));
    // This production candidate is specialized for Gemma4's 256-wide local
    // heads; K/V load directly from page-local tiles with no KC*hd gather.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "p.head_dim != 256u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "threadgroup char *fb = shmem + 8u * 256u * 2u;"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "threadgroup half *skv"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sp = (threadgroup half *)(fb + 1024u);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sphys = (threadgroup uint *)(fb + 1536u);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sM = (threadgroup float *)(fb + 1664u);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sS = (threadgroup float *)(fb + 1696u);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sdiag = (threadgroup float *)(fb + 1760u);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "so = (threadgroup float *)(fb + 2016u);"));
    // Flash structure: 32-key chunk, simdgroup MMA for Q·Kᵀ and P·V, online softmax.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint q_last = min(q0 + 7u, p.q_len - 1u); const uint tile_first_q = p.query_position_offset + q0;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "uint kc_start = 0u; if (p.swa_scan_clamp != 0u && p.sliding_window != 0u) {"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "uint kc_start = 0u; if (p.sliding_window != 0u) {"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint window_minus_one = p.sliding_window - 1u; const uint earliest_live_key = tile_first_q >= window_minus_one ? tile_first_q - window_minus_one : 0u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint earliest_logical_key = earliest_live_key - p.kv_position_offset; kc_start = (earliest_logical_key / 32u) * 32u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (uint kc = kc_start; kc < p.kv_tokens; kc += 32u) {"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "for (uint kc = 0u; kc < p.kv_tokens; kc += 32u) {"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (wholly_future) break; if (wholly_expired) continue;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_multiply_accumulate(ms, mq, mk, ms)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (uint kk8 = 0u; kk8 < 4u; ++kk8)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_load(mp, sp + kk8 * 8u, 32u); for (uint dt = 0u; dt < 8u; ++dt)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_store(mo[dt], output + q0 * q_stride"));
    // Baseline: rescale is unconditional (no skip flag).
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u);"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "schanged"));
    // Balanced braces.
    var depth: i32 = 0;
    for (source) |c| {
        if (c == '{') depth += 1 else if (c == '}') depth -= 1;
    }
    try std.testing.expectEqual(@as(i32, 0), depth);
}

test "metal renderer skip_rescale guards the flash O-accumulator rescale behind a per-chunk flag" {
    const allocator = std.testing.allocator;
    var schedule = flash_baseline_schedule;
    schedule.skip_rescale = true;
    const source = try renderPrefillFlashKernel(allocator, "antfly_flash_skip", schedule);
    defer allocator.free(source);
    // The reserved layout word becomes the "did any row's max move" flag.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "threadgroup atomic_uint *schanged = (threadgroup atomic_uint *)(fb + 1728u);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (tid == 0u) atomic_store_explicit(schanged, 0u, memory_order_relaxed);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (m_new > m_old) atomic_fetch_or_explicit(schanged, 1u, memory_order_relaxed);"));
    // Rescale is now guarded (identity multiply skipped when unset).
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (atomic_load_explicit(schanged, memory_order_relaxed) != 0u) { simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u);"));
    // Layout total is unchanged (flag reuses the reserved word before sdiag).
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sdiag = (threadgroup float *)(fb + 1760u);"));
    var depth: i32 = 0;
    for (source) |c| {
        if (c == '{') depth += 1 else if (c == '}') depth -= 1;
    }
    try std.testing.expectEqual(@as(i32, 0), depth);
}

test "metal renderer restructures the flash Q·Kᵀ and score passes for key_chunk=64" {
    const allocator = std.testing.allocator;
    var schedule = flash_baseline_schedule;
    schedule.key_chunk = 64;
    const source = try renderPrefillFlashKernel(allocator, "antfly_flash_kc64", schedule);
    defer allocator.free(source);
    // Metadata offsets grow with KC, while the K/V gather remains absent.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "threadgroup char *fb = shmem + 8u * 256u * 2u;"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "threadgroup half *skv"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sp = (threadgroup half *)(fb + 2048u);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sdiag = (threadgroup float *)(fb + 3424u);"));
    // 64-token chunk; 4 simdgroups tile 2 MMA k-tiles each; 2 keys per lane; P·V 8 inner tiles.
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "kc_start = (earliest_logical_key / 64u) * 64u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (uint kc = kc_start; kc < p.kv_tokens; kc += 64u) {"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (uint kt = 0u; kt < 2u; ++kt) {"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "uint ktile = uint(sgitg) + kt * 4u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float scs[2];"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (uint kk8 = 0u; kk8 < 8u; ++kk8)"));
    var depth: i32 = 0;
    for (source) |c| {
        if (c == '{') depth += 1 else if (c == '}') depth -= 1;
    }
    try std.testing.expectEqual(@as(i32, 0), depth);
}

test "metal renderer rejects invalid flash-prefill schedules" {
    // Flash uses either the 128-thread general route or the exact 256-thread
    // head_dim=512 specialization.
    try std.testing.expectError(error.AttentionFlashThreadsMustBe128Or256, validateAttentionSchedule(.prefill_flash, .{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = 32 }));
    // key_chunk must be a positive multiple of the 32-lane simdgroup width.
    try std.testing.expectError(error.AttentionFlashKeyChunkNotSimdgroupMultiple, validateAttentionSchedule(.prefill_flash, .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = 48 }));
    // key_chunk must fit the single-pass sphys write (<= 128 threads).
    try std.testing.expectError(error.AttentionFlashKeyChunkTooWide, validateAttentionSchedule(.prefill_flash, .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = 160 }));
    // One threadgroup owns one 8-query tile.
    try std.testing.expectError(error.AttentionColsMustBeOne, validateAttentionSchedule(.prefill_flash, .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 2, .reduction = .threadgroup_tree, .key_chunk = 32 }));
}

test "metal renderer emits the low-memory head-dim-512 flash prefill specialization" {
    const source = try renderPrefillFlashKernel(std.testing.allocator, "antfly_flash_hd512", .{
        .threads_per_threadgroup = 256,
        .cols_per_threadgroup = 1,
        .reduction = .threadgroup_tree,
        .key_chunk = 64,
    });
    defer std.testing.allocator.free(source);

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "hd != 512u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "(p.page_size & 7u) != 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "for (uint kc = 0u; kc < p.kv_tokens; kc += 64u)"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "if (sgitg < 4u)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "if (wholly_future) break; if (wholly_expired) continue;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint dslice = uint(sgitg) * 64u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "threadgroup float *so = (threadgroup float *)(fb + 3648u)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const device half *vbase = v_half + phys_v * p.v_row_stride"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_store(mo[dt], output + q0 * q_stride"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "threadgroup half *skv"));
    try std.testing.expectError(error.AttentionFlashHd512KeyChunkMustBe64, renderPrefillFlashKernel(std.testing.allocator, "bad", .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = 32 }));
}
