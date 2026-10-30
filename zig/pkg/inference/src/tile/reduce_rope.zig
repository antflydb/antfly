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

//! CUDA rotary position embeddings. One thread per element rotates pair
//! (idx0, idx1) of a head_dim chunk by `position * freq_scale * frequency`.
//! Kernels differ in where the position comes from (a launch offset, the
//! device decode scalars, per batch item), an output scale, a fused RMS norm
//! over the head, and for Qwen3-VL a per-axis position table.

const std = @import("std");
const fmt = std.fmt.comptimePrint;

/// `(\n    arg,\n    arg\n) {\n`
fn signature(comptime name: []const u8, comptime args: []const []const u8) []const u8 {
    var s: []const u8 = "extern \"C\" __global__ void " ++ name ++ "(\n";
    for (args, 0..) |a, i| s = s ++ "    " ++ a ++ (if (i + 1 == args.len) "\n" else ",\n");
    return s ++ ") {\n";
}

const rope_args = [_][]const u8{ "unsigned int total_chunks", "unsigned int head_dim", "unsigned int rope_dim" };
const position_args = [_][]const u8{ "unsigned int position_offset", "unsigned int seq_len", "unsigned int chunks_per_position", "unsigned int consecutive_pairs" };
const freq_args = [_][]const u8{ "float theta", "float freq_scale" };
const decode_arg = [_][]const u8{"const unsigned int* decode_scalars"};
const decode_line = "if (decode_scalars != 0) position_offset = decode_scalars[0];\n";
const call = "termite_rope_value(input, base, d, head_dim, rope_dim, position, theta, freq_scale, consecutive_pairs)";

fn offsetPosition(comptime indent: []const u8) []const u8 {
    return indent ++ "unsigned int token_pos = (chunk / chunks_per_position) % seq_len;\n" ++
        indent ++ "unsigned int position = position_offset + token_pos;\n";
}

const Position = enum {
    /// `position_offset` (or decode_scalars[0]) + token index within seq_len.
    offset,
    /// Per batch item offset; padding tokens past query_lengths use 0.
    per_item,
};

/// Elementwise RoPE through `termite_rope_value`.
fn rope(comptime name: []const u8, comptime position: Position, comptime scaled: bool, comptime decode: bool) []const u8 {
    const head = [_][]const u8{ "float* dst", "const float* input" };
    const args = switch (position) {
        .offset => head ++ rope_args ++ freq_args ++ position_args ++ (if (scaled) [_][]const u8{"float scale"} else .{}) ++ (if (decode) decode_arg else .{}),
        .per_item => head ++ .{ "const unsigned int* query_lengths", "const unsigned int* position_offsets", "unsigned int batch", "unsigned int max_seq_len", "unsigned int num_heads", "unsigned int head_dim", "unsigned int rope_dim" } ++ freq_args ++ .{"unsigned int consecutive_pairs"},
    };
    var s = signature(name, &args);
    if (decode) s = s ++ "    " ++ decode_line;
    s = s ++ "    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;\n";
    if (position == .per_item) s = s ++ "    unsigned int total_chunks = batch * max_seq_len * num_heads;\n";
    s = s ++
        \\    unsigned int total = total_chunks * head_dim;
        \\    if (idx >= total) return;
        \\    unsigned int chunk = idx / head_dim;
        \\    unsigned int d = idx - chunk * head_dim;
        \\
    ;
    s = s ++ switch (position) {
        .offset => offsetPosition("    "),
        .per_item =>
        \\    unsigned int token = chunk / num_heads;
        \\    unsigned int item = token / max_seq_len;
        \\    unsigned int pos = token - item * max_seq_len;
        \\    unsigned int position = 0u;
        \\    if (item < batch && pos < query_lengths[item]) {
        \\        position = position_offsets[item] + pos;
        \\    }
        \\
        ,
    };
    s = s ++ "    unsigned int base = chunk * head_dim;\n";
    s = s ++ if (scaled) "    float value = " ++ call ++ ";\n    dst[idx] = value * scale;\n" else "    dst[idx] = " ++ call ++ ";\n";
    return s ++ "}\n";
}

/// One block per head: RMS-normalize the head (block tree over 256 shared
/// slots), then rotate the normalized pairs and scale.
fn normRope(comptime name: []const u8, comptime decode: bool) []const u8 {
    const args = [_][]const u8{ "float* dst", "const float* input", "const float* weight" } ++ rope_args ++ .{"float eps"} ++ freq_args ++ position_args ++ .{"float output_scale"} ++ (if (decode) decode_arg else .{});
    return signature(name, &args) ++
        \\    unsigned int chunk = blockIdx.x;
        \\    unsigned int tid = threadIdx.x;
        \\    if (chunk >= total_chunks || head_dim == 0u) return;
        \\
    ++ (if (decode) "    " ++ decode_line else "") ++
        \\    unsigned int base = chunk * head_dim;
        \\
        \\    float sumsq = 0.0f;
        \\    for (unsigned int i = tid; i < head_dim; i += blockDim.x) {
        \\        float x = input[base + i];
        \\        sumsq += x * x;
        \\    }
        \\    __shared__ float partial[256];
        \\    partial[tid] = sumsq;
        \\    __syncthreads();
        \\    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
        \\        if (tid < stride) partial[tid] += partial[tid + stride];
        \\        __syncthreads();
        \\    }
        \\    float norm_scale = rsqrtf(partial[0] / (float)head_dim + eps);
        \\
        \\    for (unsigned int d = tid; d < head_dim; d += blockDim.x) {
        \\        float value = input[base + d] * norm_scale * weight[d];
        \\        unsigned int idx0;
        \\        unsigned int idx1;
        \\        unsigned int pair_index;
        \\        unsigned int second;
        \\        if (termite_rope_pair_info(d, head_dim, rope_dim, consecutive_pairs, &idx0, &idx1, &pair_index, &second)) {
        \\
    ++ offsetPosition("            ") ++
        \\            float angle = ((float)position) * freq_scale * termite_rope_frequency(pair_index, rope_dim, theta);
        \\            float s = sinf(angle);
        \\            float c = cosf(angle);
        \\            float x0 = input[base + idx0] * norm_scale * weight[idx0];
        \\            float x1 = input[base + idx1] * norm_scale * weight[idx1];
        \\            value = second ? (x0 * s + x1 * c) : (x0 * c - x1 * s);
        \\        }
        \\        dst[base + d] = value * output_scale;
        \\    }
        \\}
        \\
    ;
}

/// Qwen3-VL: half-split pairs whose position comes from an axis-major
/// `positions` table; `text` interleaves 3 axes by section, `vision` splits
/// the pairs into a height half and a width half.
fn qwen3vl(comptime comment: []const u8, comptime name: []const u8, comptime text: bool) []const u8 {
    const per = if (text) "chunks_per_token" else "heads_per_token";
    const args = [_][]const u8{ "float* dst", "const float* input", "const unsigned int* positions", "unsigned int total_chunks", "unsigned int token_count", "unsigned int " ++ per, "unsigned int head_dim", "float theta" } ++
        (if (text) [_][]const u8{ "float freq_scale", "unsigned int section_height", "unsigned int section_width" } else .{});
    return comment ++ signature(name, &args) ++
        \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
        \\    unsigned int total = total_chunks * head_dim;
        \\    if (idx >= total) return;
        \\
        \\    unsigned int chunk = idx / head_dim;
        \\    unsigned int d = idx - chunk * head_dim;
        \\    unsigned int rotary_pairs = head_dim / 2u;
        \\
    ++ (if (text) "" else "    unsigned int pairs_per_axis = rotary_pairs / 2u;\n") ++
        "    unsigned int pair = d < rotary_pairs ? d : d - rotary_pairs;\n" ++
        (if (text)
            \\    unsigned int axis = 0u;
            \\    if ((pair % 3u) == 1u && pair < section_height * 3u) {
            \\        axis = 1u;
            \\    } else if ((pair % 3u) == 2u && pair < section_width * 3u) {
            \\        axis = 2u;
            \\    }
            \\
        else
            \\    unsigned int axis = pair < pairs_per_axis ? 0u : 1u;
            \\    unsigned int local_pair = pair % pairs_per_axis;
            \\
        ) ++ fmt(
        \\    unsigned int token = chunk / {s};
        \\    unsigned int position = positions[axis * token_count + token];
        \\    float exponent = (2.0f * (float){s}) / (float){s};
        \\    float frequency = 1.0f / powf(theta, exponent);
        \\    float angle = (float)position * {s}frequency;
        \\    float s = sinf(angle);
        \\    float c = cosf(angle);
        \\    unsigned int base = chunk * head_dim;
        \\    float left = input[base + pair];
        \\    float right = input[base + rotary_pairs + pair];
        \\    dst[idx] = d < rotary_pairs ? left * c - right * s : left * s + right * c;
        \\}}
        \\
    , .{ per, if (text) "pair" else "local_pair", if (text) "head_dim" else "(head_dim / 2u)", if (text) "freq_scale * " else "" });
}

pub fn cudaRegion() []const u8 {
    comptime {
        @setEvalBranchQuota(1_000_000);
        const kernels = [_][]const u8{
            rope("termite_rope_f32", .offset, false, false),
            qwen3vl(
                \\// Qwen3-VL text M-RoPE. Positions are axis-major [3, token_count], while
                \\// input/output are token-major chunks of head_dim floats. The section values
                \\// count rotary pairs and are validated by the host before launch.
                \\
            , "termite_qwen3vl_mrope_f32", true),
            qwen3vl(
                \\// Qwen3-VL vision RoPE. Positions are axis-major [height,width] and each
                \\// axis restarts the frequency index at zero, matching the HF concatenation.
                \\
            , "termite_qwen3vl_vision_rope_f32", false),
            rope("termite_rope_decode_scalars_f32", .offset, false, true),
            rope("termite_rope_scaled_f32", .offset, true, false),
            rope("termite_rope_scaled_decode_scalars_f32", .offset, true, true),
            rope("termite_rope_per_item_f32", .per_item, false, false),
            normRope("termite_rms_norm_heads_rope_f32", false),
            normRope("termite_rms_norm_heads_rope_decode_scalars_f32", true),
        };
        var out: []const u8 = "";
        for (kernels, 0..) |k, i| out = out ++ (if (i == 0) "" else "\n") ++ k;
        return out;
    }
}

// ---------------------------------------------------------------- Metal

/// Metal elementwise RoPE over one or more same-shaped tensors sharing the
/// positions; `prefixes` names them (`""` → input/output, `"first_"` →
/// first_input/first_output, ...). Elements outside the rotated pairs copy
/// through.
fn metalRope(comptime name: []const u8, comptime prefixes: []const []const u8) []const u8 {
    var args: []const u8 = "";
    var pass: []const u8 = "";
    var loads: []const u8 = "";
    var first: []const u8 = "";
    var second: []const u8 = "";
    for (prefixes, 0..) |t, i| {
        args = args ++ fmt("device const float *{s}input [[buffer({d})]], ", .{ t, i });
        pass = pass ++ (if (i == 0) "" else " ") ++ fmt("{0s}output[gid] = {0s}input[gid];", .{t});
        loads = loads ++ fmt("    float {0s}x0 = {0s}input[base + idx0];\n    float {0s}x1 = {0s}input[base + idx1];\n", .{t});
        first = first ++ (if (i == 0) "" else " ") ++ fmt("{0s}output[gid] = {0s}x0 * cos_val - {0s}x1 * sin_val;", .{t});
        second = second ++ (if (i == 0) "" else " ") ++ fmt("{0s}output[gid] = {0s}x0 * sin_val + {0s}x1 * cos_val;", .{t});
    }
    const n = prefixes.len;
    args = args ++ fmt("device const uint *positions [[buffer({d})]], ", .{n});
    for (prefixes, 0..) |t, i| args = args ++ fmt("device float *{s}output [[buffer({d})]], ", .{ t, n + 1 + i });
    // One store is a bare statement, several a braced block.
    const brace = struct {
        fn f(comptime s: []const u8, comptime count: usize) []const u8 {
            return if (count == 1) s else "{ " ++ s ++ " }";
        }
    }.f;
    return fmt("kernel void {s}({s}constant termite_metal_rope_params &p [[buffer({d})]], uint gid [[thread_position_in_grid]]) {{\n", .{ name, args, 2 * n + 1 }) ++
        \\    uint total = p.total_chunks * p.head_dim;
        \\    if (gid >= total) return;
        \\    uint chunk = gid / p.head_dim;
        \\    uint d = gid - chunk * p.head_dim;
        \\    uint base = chunk * p.head_dim;
        \\
    ++ "    if (p.rope_dim < 2u) { " ++ pass ++ " return; }\n" ++
        \\    uint rope_half = p.rope_dim / 2u;
        \\    uint head_half = p.head_dim / 2u;
        \\    uint idx0 = 0u;
        \\    uint idx1 = 0u;
        \\    uint j = 0u;
        \\    if (p.consecutive_pairs != 0u) {
        \\
    ++ "        if (d >= p.rope_dim) { " ++ pass ++ " return; }\n" ++
        \\        j = d / 2u;
        \\        idx0 = 2u * j;
        \\        idx1 = idx0 + 1u;
        \\    } else {
        \\        if (d < rope_half) { j = d; idx0 = d; idx1 = head_half + d; }
        \\        else if (d >= head_half && d < head_half + rope_half) { j = d - head_half; idx0 = j; idx1 = d; }
        \\
    ++ "        else { " ++ pass ++ " return; }\n" ++
        \\    }
        \\    float pos = float(positions[chunk]);
        \\    float freq = 1.0f / pow(p.theta, float(2u * j) / float(p.rope_dim));
        \\    float angle = pos * p.freq_scale * freq;
        \\    float cos_val = cos(angle);
        \\    float sin_val = sin(angle);
        \\
    ++ loads ++
        "    if (d == idx0) " ++ brace(first, n) ++ "\n" ++
        "    else if (d == idx1) " ++ brace(second, n) ++ "\n" ++
        "    else " ++ brace(pass, n) ++ "\n}\n";
}

pub fn metalRegion() []const u8 {
    comptime {
        @setEvalBranchQuota(1_000_000);
        return metalRope("termite_apply_rope", &.{""}) ++ metalRope("termite_apply_rope_pair", &.{ "first_", "second_" });
    }
}
