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

//! Descriptor-driven renderer for generated Metal small-batch quant matmul
//! kernels. A kernel is `render(kernel_id, decoder, schedule, epilogue)`:
//! one canonical skeleton parametrized by the launch schedule (threads/cols/
//! reduction), with the per-format dequant math supplied as a leaf MSL fragment
//! (`FormatDecoder.recipe`).
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
const attention_phases = @import("quant_kernel_metal_attention_phases.zig");
const param_abi = @import("quant_kernel_metal_param_abi.zig");

const Epilogue = compiler.Epilogue;
const KernelSchedule = compiler.KernelSchedule;
const ReductionKind = compiler.ReductionKind;
const OpKind = quant_kernel_op.OpKind;
const kernel_math = @import("antfly_tile").native_math;
const quant_recipe = @import("quant_recipe.zig");

pub const MicrokernelKind = quant_kernel_op.MicrokernelKind;
pub const AttentionKind = quant_kernel_op.AttentionKind;

/// A shared MSL vocabulary helper (dedup of the per-format v1 copies).
pub const HelperFragment = struct {
    name: []const u8,
    msl: []const u8,
};

/// A packed block recipe plus target vocabulary. Arithmetic is shared with
/// CPU evaluation and CUDA generation; this renderer owns only MSL schedules.
pub const FormatDecoder = struct {
    format: quant_matmul.Format,
    weight_param: []const u8,
    lane_decode_fn: []const u8,
    recipe: quant_recipe.Recipe,
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
    .msl = kernel_math.guardedGeluSource(kernel_math.metal, "inline", "antfly_qk_gelu", true),
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
// The typed parameter schema supplies C, runtime MSL and this standalone
// view. The external-helper guard also checks native declaration placement.
pub const paged_attention_params_field_body = param_abi.fields("paged_attention");

pub const helper_paged_attention_1x_params = HelperFragment{
    .name = "antfly_paged_attention_1x_params",
    .msl = "struct antfly_paged_attention_1x_params { " ++ paged_attention_params_field_body ++ " };",
};

pub const helper_paged_attention_1x_page_token = HelperFragment{
    .name = "antfly_paged_attention_1x_page_token",
    .msl = "template<typename Params> inline uint antfly_paged_attention_1x_page_token(device const uint *block_table, constant Params &p, uint logical_token) { if (p.contiguous_blocks != 0u) return p.contiguous_base_token + logical_token; uint logical_block = logical_token / p.page_size; uint token_in_block = logical_token - logical_block * p.page_size; if (logical_block >= p.block_count) return 0xffffffffu; return block_table[logical_block] + token_in_block; }",
};

const helper_paged_decode_native = HelperFragment{
    .name = "antfly_paged_decode_body",
    .msl = @embedFile("templates/metal_paged_decode_native.metal"),
};

fn formatDecoder(comptime format: quant_matmul.Format) FormatDecoder {
    return .{
        .format = format,
        .weight_param = "weight_" ++ @tagName(format),
        .lane_decode_fn = "antfly_" ++ @tagName(format) ++ "_dequant_lane_v2",
        .recipe = quant_recipe.forFormat(format),
        .helpers = switch (format) {
            .q8_k => &.{helper_qk_f32_le_to_float},
            .q5_0, .q5_1 => &.{ helper_qk_half_le_to_float, helper_qk_u32_le },
            else => &.{helper_qk_half_le_to_float},
        },
    };
}

pub const decoder_q4_0 = formatDecoder(.q4_0);
pub const decoder_q4_1 = formatDecoder(.q4_1);
pub const decoder_q5_0 = formatDecoder(.q5_0);
pub const decoder_q5_1 = formatDecoder(.q5_1);
pub const decoder_q8_0 = formatDecoder(.q8_0);
pub const decoder_q8_1 = formatDecoder(.q8_1);
pub const decoder_q8_k = formatDecoder(.q8_k);
pub const decoder_q2_k = formatDecoder(.q2_k);
pub const decoder_q3_k = formatDecoder(.q3_k);
pub const decoder_q4_k = formatDecoder(.q4_k);
pub const decoder_q5_k = formatDecoder(.q5_k);
pub const decoder_q6_k = formatDecoder(.q6_k);

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
    try emitHelper(allocator, &out, &emitted_names, helper_qk_gelu);
    for (decoder.helpers) |helper| try emitHelper(allocator, &out, &emitted_names, helper);
    try emitHelper(allocator, &out, &emitted_names, helper_qk_gelu);
    try emitDecoder(allocator, &out, &emitted_names, decoder);
    try emitProjectionPolicy(allocator, &out, decoder);
    try out.appendSlice(allocator, projection_native_source);

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
    try emitHelper(allocator, &out, &emitted_names, helper_qk_gelu);
    for (decoder.helpers) |helper| try emitHelper(allocator, &out, &emitted_names, helper);
    try emitDecoder(allocator, &out, &emitted_names, decoder);
    try emitProjectionPolicy(allocator, &out, decoder);
    try out.appendSlice(allocator, projection_native_source);
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
pub const helper_attention_online_row32 = HelperFragment{
    .name = "antfly_attention_online_row32",
    .msl =
    \\// Exact online row phase. Score admission, masks, diagonal publication and
    \\// matrix schedules remain with each caller.
    \\__attribute__((always_inline)) inline void antfly_attention_online_row32(float sc, threadgroup const float *maxima, uint state_row, threadgroup half *probability, uint probability_index, thread float &m_old, thread float &m_new, thread float &corr, thread float &row_sum) {
    \\    float row_max = simd_max(sc); m_old = maxima[state_row]; m_new = max(m_old, row_max);
    \\    float e = 0.0f; corr = 1.0f;
    \\    if (m_new > -3.0e+38f) { corr = m_old > -3.0e+38f ? exp(m_old - m_new) : 0.0f; e = sc > -3.0e+38f ? exp(sc - m_new) : 0.0f; }
    \\    probability[probability_index] = half(e);
    \\    row_sum = simd_sum(e);
    \\}
    ,
};

fn attentionHelpers(kind: AttentionKind) []const HelperFragment {
    return switch (kind) {
        .decode_1x => &.{ helper_paged_attention_1x_params, helper_paged_attention_1x_page_token, helper_attention_online_row32, helper_paged_decode_native },
        .prefill_flash => &.{ helper_paged_attention_1x_params, helper_paged_attention_1x_page_token, helper_attention_online_row32 },
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
        .decode_1x => try renderPagedAttention1xBody(allocator, out, kernel_id, schedule, .standalone),
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
/// One owner for standalone and compatibility-runtime decode. The legacy
/// runtime and generated schedules share guarded zero-mass semantics and
/// synchronize the max read before reusing the shared partial array.
fn renderPagedAttention1xBody(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    kernel_id: []const u8,
    schedule: KernelSchedule,
    abi: AttentionSourceAbi,
) !void {
    const params_name = if (abi == .runtime) "termite_metal_paged_attention_params" else "antfly_paged_attention_1x_params";
    try appendFmt(allocator, out, "kernel void {s}(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant {s} &p [[buffer(6)]], threadgroup float *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {{\n", .{ kernel_id, params_name });
    try appendFmt(allocator, out, "    antfly_paged_decode_body<{s}, {d}u>(q, encoded_key, v_bytes, block_table, sinks, output, p, shmem, tid, lane, sgitg, tg);\n", .{ params_name, schedule.threads_per_threadgroup });
    try out.appendSlice(allocator, "}\n");
}

const AttentionSourceAbi = enum { standalone, runtime };
const compatibility_prefill_source = @embedFile("templates/metal_paged_prefill_compat.metal");

fn renderCompatibilityPrefillBody(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), kernel_id: []const u8, abi: AttentionSourceAbi) !void {
    try appendFmt(allocator, out, "#define ANTFLY_COMPAT_PREFILL_KERNEL {s}\n#define ANTFLY_COMPAT_PREFILL_PARAMS {s}\n#define ANTFLY_COMPAT_PAGE_TOKEN {s}\n", .{
        kernel_id,
        if (abi == .runtime) "termite_metal_paged_attention_params" else "antfly_paged_attention_1x_params",
        if (abi == .runtime) "termite_attention_page_token" else "antfly_paged_attention_1x_page_token",
    });
    const body = try attention_phases.expand(allocator, compatibility_prefill_source);
    defer allocator.free(body);
    try out.appendSlice(allocator, body);
    try out.appendSlice(allocator, "#undef ANTFLY_COMPAT_PAGE_TOKEN\n#undef ANTFLY_COMPAT_PREFILL_PARAMS\n#undef ANTFLY_COMPAT_PREFILL_KERNEL\n");
}

/// Exact legacy schedules with standalone params/helpers for direct qualification.
/// This does not admit a new runtime route or alter generated plan identities.
pub fn renderCompatibilityAttention(allocator: std.mem.Allocator, kernel_id: []const u8, kind: AttentionKind) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var names = EmittedNames{};
    for (attentionHelpers(kind)) |helper| try emitHelper(allocator, &out, &names, helper);
    switch (kind) {
        .decode_1x => try renderPagedAttention1xBody(allocator, &out, kernel_id, .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree }, .standalone),
        .prefill_flash => try renderCompatibilityPrefillBody(allocator, &out, kernel_id, .standalone),
    }
    return out.toOwnedSlice(allocator);
}

pub fn renderCompatibilityRuntimeAttention(allocator: std.mem.Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try renderPagedAttention1xBody(allocator, &out, "termite_paged_attention_kv_1x", .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree }, .runtime);
    try renderCompatibilityPrefillBody(allocator, &out, "termite_paged_attention_kv_prefill_sg", .runtime);
    return out.toOwnedSlice(allocator);
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
// Both schedules expose the same wire signature; storage and bodies stay local.
const flash_prefill_signature = "kernel void {s}(device const float *q [[buffer(0)]], device const uchar *encoded_key [[buffer(1)]], device const uchar *v_bytes [[buffer(2)]], device const uint *block_table [[buffer(3)]], device const float *sinks [[buffer(4)]], device float *output [[buffer(5)]], constant antfly_paged_attention_1x_params &p [[buffer(6)]], threadgroup char *shmem [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint2 tg [[threadgroup_position_in_grid]]) {{\n";

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
    try appendFmt(allocator, out, flash_prefill_signature, .{kernel_id});
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
    try out.appendSlice(allocator, attention_phases.online_init);
    try out.appendSlice(allocator, "    const uint dslice = uint(sgitg) * 64u;\n");
    try out.appendSlice(allocator, attention_phases.accumulator_init);

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
        try out.appendSlice(allocator, attention_phases.row32_update);
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
    try appendFmt(allocator, out, attention_phases.flash_value_accumulation, .{ pv_tiles, "phys", kc, "phys" });
    try out.appendSlice(allocator, "    }\n");

    // ---- epilogue: final inverse-sum normalise + direct strided store. The
    // partial query tile reuses one hd/4-wide threadgroup output tile. ----
    try out.appendSlice(allocator, attention_phases.normalization);
    try out.appendSlice(allocator, "    for (uint dt = 0u; dt < 8u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); mo[dt] = scaled; }\n");
    try appendFmt(allocator, out, attention_phases.flash_full_output, .{256});
    try appendFmt(allocator, out, attention_phases.flash_tail_output, .{ 4, 128, 256 });
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
    try appendFmt(allocator, out, flash_prefill_signature, .{kernel_id});
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
        "        simdgroup_float8x8 mcorr; simdgroup_load(mcorr, sdiag, 8u); for (uint i = 0u; i < 8u; ++i) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, mcorr, mo[i]); mo[i] = scaled; }\n");
    try appendFmt(allocator, out, attention_phases.flash_value_accumulation, .{ 8, "phys_v", 64, "phys_v" });
    try out.appendSlice(allocator, "    }\n");

    try out.appendSlice(allocator, "    threadgroup_barrier(mem_flags::mem_threadgroup); if (tid < 8u) { float denom = sS[tid]; sdiag[uint(tid) * 8u + uint(tid)] = denom > 0.0f ? 1.0f / denom : 0.0f; } threadgroup_barrier(mem_flags::mem_threadgroup);\n" ++
        "    simdgroup_float8x8 minv; simdgroup_load(minv, sdiag, 8u); for (uint dt = 0u; dt < 8u; ++dt) { simdgroup_float8x8 scaled; simdgroup_multiply(scaled, minv, mo[dt]); mo[dt] = scaled; }\n");
    try appendFmt(allocator, out, attention_phases.flash_full_output, .{512});
    try appendFmt(allocator, out, attention_phases.flash_tail_output, .{ 8, 256, 512 });
    try out.appendSlice(allocator, "}\n");
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

/// Retain Florence's wire signature and four-row schedule while using the
/// same K-family traversal as generated small-batch projections.
pub fn renderFlorenceNr4Compatibility(allocator: std.mem.Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "kernel void termite_florence_q4_k_mm_nr4(device const float *input [[buffer(0)]], device const uchar *weight_q4_k [[buffer(1)]], device float *output [[buffer(2)]], constant int &rows [[buffer(3)]], constant int &in_dim [[buffer(4)]], constant int &out_dim [[buffer(5)]], ushort lane_id [[thread_index_in_simdgroup]], ushort simdgroup_id [[simdgroup_index_in_threadgroup]], uint3 group_pos [[threadgroup_position_in_grid]]) {\n");
    try renderMatmulComputation(allocator, &out, decoderFor(.q4_k).?, .{
        .threads_per_threadgroup = 128,
        .cols_per_threadgroup = 16,
        .rows_per_threadgroup = 4,
        .reduction = .simdgroup_tiled,
    }, .none, 256, 144);
    try out.appendSlice(allocator, "}\n");
    return out.toOwnedSlice(allocator);
}

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
                try emitHelper(allocator, &out, &emitted_names, helper_qk_gelu);
                try emitDecoder(allocator, &out, &emitted_names, k.decoder);
                if (!emitted_names.contains(k.decoder.weight_param)) {
                    try emitProjectionPolicy(allocator, &out, k.decoder);
                    // Names must have stable storage for the rest of this render.
                    emitted_names.add(k.decoder.weight_param);
                }
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

    for (kernels) |k| {
        if (k.op_kind == .small_batch_matmul) {
            try out.appendSlice(allocator, projection_native_source);
            break;
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
    _ = block_values;
    _ = block_bytes;
    const policy = try std.fmt.allocPrint(allocator, "antfly_projection_decode_{s}", .{@tagName(decoder.format)});
    defer allocator.free(policy);
    const ep = switch (epilogue) {
        .none => "antfly_projection_plain",
        .bias => "antfly_projection_bias",
        .bias_gelu => "antfly_projection_bias_gelu",
        .relu => "antfly_projection_relu",
        else => return error.UnsupportedEpilogue,
    };
    const bias = if (epilogue == .bias or epilogue == .bias_gelu) "bias" else "nullptr";
    const groups = schedule.threads_per_threadgroup / 32;
    const columns = schedule.cols_per_threadgroup / @as(u8, @intCast(groups));
    switch (schedule.reduction) {
        .simdgroup_matrix => {
            const scalar_bytes: usize = if (decoder.format == .q6_k) 4 else 2;
            const shared_bytes = @max((2048 + @as(usize, schedule.rows_per_threadgroup) * 32) * scalar_bytes, @as(usize, schedule.rows_per_threadgroup) * 64 * 4);
            try appendFmt(allocator, out, "    threadgroup uchar shared[{d}];\n    antfly_projection_matrix<{s}, {d}u, {d}u>(input, {s}, output, rows, in_dim, out_dim, thread_pos, group_pos, simdgroup_id, shared);\n", .{ shared_bytes, policy, schedule.rows_per_threadgroup, schedule.threads_per_threadgroup, decoder.weight_param });
        },
        .simdgroup_tiled => if (decoder.format == .q4_0) {
            try appendFmt(allocator, out, "    antfly_projection_q4_packed_tile<{d}u, {d}u>(input, {s}, output, rows, in_dim, out_dim, lane_id, simdgroup_id, group_pos);\n", .{ groups, columns, decoder.weight_param });
        } else {
            try appendFmt(allocator, out, "    antfly_projection_k_tile<{s}, {s}, {d}u, {d}u, {d}u>(input, {s}, {s}, output, rows, in_dim, out_dim, lane_id, simdgroup_id, group_pos);\n", .{ policy, ep, groups, columns, schedule.rows_per_threadgroup, decoder.weight_param, bias });
        },
        else => if (schedule.cols_per_threadgroup == 2) {
            try appendFmt(allocator, out, "    antfly_projection_two_columns<{s}, {s}>(input, {s}, {s}, output, rows, in_dim, out_dim, thread_pos, group_pos);\n", .{ policy, ep, decoder.weight_param, bias });
        } else {
            const reduction: u8 = switch (schedule.reduction) {
                .simd_sum => 0,
                .threadgroup_tree => 1,
                .hybrid_simd => 2,
                else => unreachable,
            };
            if (reduction != 0) try appendFmt(allocator, out, "    threadgroup float partial[{d}];\n", .{if (reduction == 1) schedule.threads_per_threadgroup else @as(u16, 32)});
            try appendFmt(allocator, out, "    antfly_projection_column<{s}, {s}, {d}u, {d}u>(input, {s}, {s}, output, rows, in_dim, out_dim, thread_pos, group_pos, {s}, {s}, {s});\n", .{ policy, ep, schedule.threads_per_threadgroup, reduction, decoder.weight_param, bias, if (reduction == 2) "lane_id" else "0", if (reduction == 2) "simdgroup_id" else "0", if (reduction != 0) "partial" else "nullptr" });
        },
    }
}

/// Packed Q4_0 register tile for an exact two-row projection. Each simdgroup
/// owns 2, 4, or 8 output columns and reuses their packed ushort weights across
/// both input rows.
/// One register-tile schedule for K-family projections. Format-specific
/// decoding keeps Q4_K's scale/min hoisting and Q6_K's eight-value scale reuse;
/// grid mapping, row tails, activation loads, accumulation and writes are shared.
const projection_native_source = @embedFile("templates/metal_projection_native.metal");

// Keep the decoder policy transparent to Metal's inliner: omitting the
// attribute regresses Q4_K 64-thread tree/hybrid reductions by about 9%.
fn emitProjectionPolicy(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), decoder: FormatDecoder) !void {
    try appendFmt(allocator, out, "struct antfly_projection_decode_{s} {{\n    enum : int {{ values = {d}, bytes = {d}, shift = {d} }};\n    __attribute__((always_inline)) static float decode(device const uchar* block, int lane) {{ return {s}(block, lane); }}\n", .{ @tagName(decoder.format), decoder.format.valuesPerBlock().?, decoder.format.bytesPerBlock().?, log2Usize(decoder.format.valuesPerBlock().?), decoder.lane_decode_fn });
    switch (decoder.format) {
        .q4_k => try out.appendSlice(allocator,
            \\    using Scalar = half; using Matrix = simdgroup_half8x8; using Scale = float2;
            \\    enum : uint { sub_shift = 2u, sub_mask = 3u, sub_values = 32u };
            \\    static Scale scale(device const uchar* block, uint sub) { float raw_scale = 0.0f; float raw_min = 0.0f; antfly_qk_unpack_scale_min_6bit(block + 4u, int(sub), raw_scale, raw_min); return float2(antfly_qk_half_le_to_float(block) * raw_scale, antfly_qk_half_le_to_float(block + 2u) * raw_min); }
            \\    static float value(device const uchar* block, Scale scale, uint sub, uint j0, uint i) { const uint chunk = sub >> 1; const bool high = (sub & 1u) != 0u; device const uchar* qs = block + 16u + chunk * 32u + j0; uchar packed = qs[i]; return scale.x * float(high ? packed >> 4 : packed & 0x0fu) - scale.y; }
            \\
        ),
        .q6_k => try out.appendSlice(allocator,
            \\    using Scalar = float; using Matrix = simdgroup_float8x8; using Scale = float;
            \\    enum : uint { sub_shift = 1u, sub_mask = 1u, sub_values = 16u };
            \\    static Scale scale(device const uchar* block, uint sub) { return antfly_qk_q6_k_scale(block, int(sub * 16u)); }
            \\    static float value(device const uchar* block, Scale scale, uint sub, uint j0, uint i) { return scale * antfly_qk_q6_k_value(block, int(sub * 16u + j0 + i)); }
            \\
        ),
        else => {},
    }
    try out.appendSlice(allocator, "};\n");
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
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "__attribute__((always_inline)) static float decode("));
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
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_projection_"));
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
        const has_two_col = std.mem.containsAtLeast(u8, source, 1, "    antfly_projection_two_columns<");
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
            const grid_marker = try std.fmt.allocPrint(allocator, "antfly_projection_q4_packed_tile<{d}u, {d}u>", .{ simdgroups, cols_per_simdgroup });
            defer allocator.free(grid_marker);
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, grid_marker));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const uint NQ16 = 16u"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "reinterpret_cast<const device ushort*>"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float(packed & 0xF000u)"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rows != 2 || in_dim <= 0 || (uint(in_dim) & 31u) != 0u || out_dim <= 0"));
            try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "10240"));
            try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "2560"));
            try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, source, "antfly_q4_0_dequant_lane_v2("));
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

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_projection_k_tile<antfly_projection_decode_q4_k, antfly_projection_plain, 4u, 4u, 2u>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rows < 2 || in_dim <= 0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float x[NR][8]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_qk_unpack_scale_min_6bit"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "acc[c][rr] += x[rr][i] * w"));

    const nr4 = try renderKernel(allocator, "antfly_q4_k_tiled_nr4", decoder_q4_k, .{
        .threads_per_threadgroup = 64,
        .cols_per_threadgroup = 8,
        .rows_per_threadgroup = 4,
        .reduction = .simdgroup_tiled,
    }, .none);
    defer allocator.free(nr4);
    try std.testing.expect(std.mem.containsAtLeast(u8, nr4, 1, "antfly_projection_k_tile<antfly_projection_decode_q4_k, antfly_projection_plain, 2u, 4u, 4u>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr4, 1, "float acc[NC][NR]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr4, 1, "float x[NR][8]"));

    const fused = try renderKernel(allocator, "antfly_q4_k_tiled_bias_gelu", decoder_q4_k, schedule, .bias_gelu);
    defer allocator.free(fused);
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "device const float *bias [[buffer(2)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "antfly_projection_bias_gelu"));
}

test "metal renderer emits a shape-general two-row Q6_K register tile" {
    const allocator = std.testing.allocator;
    const schedule = KernelSchedule{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled };
    const source = try renderKernel(allocator, "antfly_q6_k_tiled", decoder_q6_k, schedule, .none);
    defer allocator.free(source);

    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_projection_k_tile<antfly_projection_decode_q6_k, antfly_projection_plain, 4u, 4u, 2u>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "rows < 2 || in_dim <= 0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "sub_shift = 1u, sub_mask = 1u, sub_values = 16u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "return antfly_qk_q6_k_scale(block"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "return scale * antfly_qk_q6_k_value(block"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "acc[c][rr] += x[rr][i] * w"));

    const nr8 = try renderKernel(allocator, "antfly_q6_k_tiled_nr8", decoder_q6_k, .{
        .threads_per_threadgroup = 128,
        .cols_per_threadgroup = 16,
        .rows_per_threadgroup = 8,
        .reduction = .simdgroup_tiled,
    }, .none);
    defer allocator.free(nr8);
    try std.testing.expect(std.mem.containsAtLeast(u8, nr8, 1, "antfly_projection_k_tile<antfly_projection_decode_q6_k, antfly_projection_plain, 4u, 4u, 8u>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr8, 1, "float acc[NC][NR]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, nr8, 1, "float x[NR][8]"));

    const fused = try renderKernel(allocator, "antfly_q6_k_tiled_bias", decoder_q6_k, schedule, .bias);
    defer allocator.free(fused);
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "device const float *bias [[buffer(2)]]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, fused, 1, "antfly_projection_bias"));
}

test "metal renderer emits a Q4_K and Q6_K simdgroup matrix tile" {
    const allocator = std.testing.allocator;
    inline for (.{ decoder_q4_k, decoder_q6_k }) |decoder| {
        inline for (.{
            .{ KernelSchedule{ .threads_per_threadgroup = 160, .cols_per_threadgroup = 64, .rows_per_threadgroup = 40, .reduction = .simdgroup_matrix }, "threadgroup uchar shared[10240]", "40u, 160u>" },
            .{ KernelSchedule{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 64, .rows_per_threadgroup = 64, .reduction = .simdgroup_matrix }, "threadgroup uchar shared[16384]", "64u, 256u>" },
        }) |case| {
            const source = try renderKernel(allocator, "antfly_quant_matrix", decoder, case[0], .none);
            defer allocator.free(source);
            const shared_marker = if (decoder.format == .q6_k and case[0].rows_per_threadgroup == 40)
                "threadgroup uchar shared[13312]"
            else
                case[1];
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, shared_marker));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "simdgroup_multiply_accumulate"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "group_pos.x * 64u"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, case[2]));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "global_row < uint(rows)"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, decoder.lane_decode_fn));
            const operand_marker = if (decoder.format == .q6_k)
                "using Scalar = float; using Matrix = simdgroup_float8x8"
            else
                "using Scalar = half; using Matrix = simdgroup_half8x8";
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, operand_marker));
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
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_paged_decode_body<antfly_paged_attention_1x_params, 256u>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "phys_tokens = shmem + p.kv_tokens + NT"));
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
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_paged_decode_body<antfly_paged_attention_1x_params, 128u>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "phys_tokens = shmem + p.kv_tokens + NT"));
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

fn emitDecoder(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), names: *EmittedNames, decoder: FormatDecoder) !void {
    if (decoder.format == .q4_k) {
        inline for (.{ false, true }) |minimum| {
            const name = if (minimum) "antfly_qk_header_min" else "antfly_qk_header_scale";
            const helper_source = try quant_recipe.render(allocator, quant_recipe.scaleMinHeader(minimum), .metal, name);
            defer allocator.free(helper_source);
            try emitHelper(allocator, out, names, .{ .name = name, .msl = helper_source });
        }
        try emitHelper(allocator, out, names, .{
            .name = "antfly_qk_unpack_scale_min_6bit",
            .msl = "inline void antfly_qk_unpack_scale_min_6bit(device const uchar *scales, int sub, thread float &scale, thread float &min_v) { scale = antfly_qk_header_scale(scales, sub); min_v = antfly_qk_header_min(scales, sub); }",
        });
    }
    if (decoder.format == .q6_k) {
        for ([_]struct { name: []const u8, id: quant_recipe.Id }{
            .{ .name = "antfly_qk_q6_k_scale", .id = decoder.recipe.scale_component.? },
            .{ .name = "antfly_qk_q6_k_value", .id = decoder.recipe.value_component.? },
        }) |part| {
            const component = decoder.recipe.component(part.id);
            const source = try quant_recipe.render(allocator, component, .metal, part.name);
            defer allocator.free(source);
            try emitHelper(allocator, out, names, .{ .name = part.name, .msl = source });
        }
    }
    const source = try quant_recipe.render(allocator, decoder.recipe, .metal, decoder.lane_decode_fn);
    defer allocator.free(source);
    try emitHelper(allocator, out, names, .{ .name = decoder.lane_decode_fn, .msl = source });
}

test "metal renderer compatibility attention retains standalone ABI and legacy precision policies" {
    const a = std.testing.allocator;
    for ([_]AttentionKind{ .decode_1x, .prefill_flash }) |kind| {
        const source = try renderCompatibilityAttention(a, "compatibility_attention", kind);
        defer a.free(source);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "struct antfly_paged_attention_1x_params {"));
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, "inline uint antfly_paged_attention_1x_page_token("));
        try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "termite_metal_paged_attention_params"));
        switch (kind) {
            .decode_1x => {
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_paged_decode_body<antfly_paged_attention_1x_params, 256u>"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "float e = (best > -3.0e+38f && score > -3.0e+38f) ? exp(score - best) : 0.0f"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const float best = simd_max(uint(lane) < NSG ? partials[lane] : -3.402823466e+38f);\n    // Every SIMD group must load the maximum before partials[0] is reused.\n    threadgroup_barrier"));
            },
            .prefill_flash => {
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "#define ANTFLY_COMPAT_PREFILL_KERNEL compatibility_attention"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "#define ANTFLY_COMPAT_PREFILL_PARAMS antfly_paged_attention_1x_params"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "hd % 32u != 0u"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "half(e)"));
            },
        }
    }
}

/// Compact stored AOT text; JIT and fingerprints keep the complete native body.
pub fn compactNativeSourceFile(allocator: std.mem.Allocator, source: []const u8) !?[]u8 {
    return @import("kernel_source_owner.zig").compact(allocator, source, &native_source_owners);
}

const native_source_owners = [_]@import("kernel_source_owner.zig").NativeOwner{
    .{ .source = projection_native_source, .include_path = "../../../graph/templates/metal_projection_native.metal" },
    .{ .source = helper_paged_decode_native.msl, .include_path = "../../../graph/templates/metal_paged_decode_native.metal" },
};

pub fn expandNativeSourceFile(allocator: std.mem.Allocator, source: []const u8) !?[]u8 {
    return @import("kernel_source_owner.zig").expand(allocator, source, &native_source_owners);
}
