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

//! CUDA attention kernels generated in pairs: each family keeps one copy of
//! the kernel text and marks where its two variants differ (see
//! attention_template.zig). The text equals the hand-written kernels it
//! replaced, so their tuned performance is unchanged.

const template = @import("attention_template.zig");

/// One block per (batch, query, head); every key's score is a shared-memory
/// tree over head_dim lanes. `scalars` reads the decode positions and lengths
/// from a device array (graph replay) when it is given.
pub const gqa_decode = struct {
    pub fn kernel(comptime name: []const u8, comptime scalars: bool) []const u8 {
        return template.render(text, .{ .name = name, .scalars = scalars });
    }
    const text =
        \\extern "C" __global__ void {{name}}(
        \\    float* dst,
        \\    const float* q,
        \\    const float* k,
        \\    const float* v,
        \\    const unsigned char* attn_or_mask,
        \\    const float* bias,
        \\    unsigned int batch,
        \\    unsigned int q_seq_len,
        \\    unsigned int kv_seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int num_kv_heads,
        \\    unsigned int head_dim,
        \\    unsigned int query_position_offset,
        \\    unsigned int kv_position_offset,
        \\    unsigned int sliding_window,
        \\    unsigned int total_sequence_len,
        \\    unsigned int mask_len,
        \\    unsigned int bias_mode{{#scalars}},
        \\    const unsigned int* decode_scalars{{/scalars}}
        \\) {
        \\    {{#scalars}}if (decode_scalars != 0) {
        \\        kv_position_offset = decode_scalars[4];
        \\        query_position_offset = decode_scalars[1];
        \\        kv_seq_len = decode_scalars[2];
        \\        total_sequence_len = decode_scalars[3];
        \\    }
        \\
        \\    {{/scalars}}__shared__ float reduce[512];
        \\    __shared__ float shared_max_score;
        \\    __shared__ float shared_denom;
        \\    unsigned int lane = threadIdx.x;
        \\    unsigned int block = blockIdx.x;
        \\    unsigned int total_blocks = batch * q_seq_len * num_heads;
        \\    if (block >= total_blocks || head_dim > 512u || blockDim.x < head_dim || num_kv_heads == 0u || (num_heads % num_kv_heads) != 0u) return;
        \\
        \\    unsigned int head = block % num_heads;
        \\    unsigned int tmp = block / num_heads;
        \\    unsigned int qi = tmp % q_seq_len;
        \\    unsigned int b = tmp / q_seq_len;
        \\    unsigned int heads_per_group = num_heads / num_kv_heads;
        \\    unsigned int kv_head = head / heads_per_group;
        \\    unsigned int q_hidden = num_heads * head_dim;
        \\    unsigned int kv_hidden = num_kv_heads * head_dim;
        \\    unsigned int query_pos = query_position_offset + qi;
        \\    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
        \\    float scale = rsqrtf((float)head_dim);
        \\    if (lane == 0u) shared_max_score = -3.402823466e+38f;
        \\    __syncthreads();
        \\
        \\    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        \\        unsigned int key_pos = kv_position_offset + ki;
        \\        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        \\        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        \\        bool future_blocked = key_pos > query_pos && !future_allowed;
        \\        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        \\        bool valid = !(future_blocked || past_blocked);
        \\        float partial = 0.0f;
        \\        if (valid && lane < head_dim) {
        \\            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
        \\            partial = q[q_base + lane] * k[k_base + lane];
        \\        }
        \\        reduce[lane] = partial;
        \\        __syncthreads();
        \\        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
        \\            if (lane < stride) reduce[lane] += reduce[lane + stride];
        \\            __syncthreads();
        \\        }
        \\        if (lane == 0u && valid) {
        \\            float score = reduce[0] * scale;
        \\            if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
        \\            if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
        \\            shared_max_score = fmaxf(shared_max_score, score);
        \\        }
        \\        __syncthreads();
        \\    }
        \\
        \\    if (lane == 0u) shared_denom = 0.0f;
        \\    __syncthreads();
        \\    float acc = 0.0f;
        \\    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        \\        unsigned int key_pos = kv_position_offset + ki;
        \\        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        \\        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        \\        bool future_blocked = key_pos > query_pos && !future_allowed;
        \\        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        \\        bool valid = !(future_blocked || past_blocked);
        \\        float partial = 0.0f;
        \\        if (valid && lane < head_dim) {
        \\            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
        \\            partial = q[q_base + lane] * k[k_base + lane];
        \\        }
        \\        reduce[lane] = partial;
        \\        __syncthreads();
        \\        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
        \\            if (lane < stride) reduce[lane] += reduce[lane + stride];
        \\            __syncthreads();
        \\        }
        \\        float e = 0.0f;
        \\        if (valid) {
        \\            float score = reduce[0] * scale;
        \\            if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
        \\            if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
        \\            e = expf(score - shared_max_score);
        \\        }
        \\        if (lane == 0u) shared_denom += e;
        \\        if (lane < head_dim) {
        \\            unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + lane;
        \\            acc += e * v[v_idx];
        \\        }
        \\        __syncthreads();
        \\    }
        \\
        \\    if (lane < head_dim) {
        \\        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        \\        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
        \\    }
        \\}
        \\
    ;
};

/// One block per (batch, query, head) running an online softmax over the
/// visible key window, with a warp-shuffle block reduction per score.
/// `decode` is the batch-1, single-query form that takes its positions from
/// `decode_scalars`.
pub const gqa_fast = struct {
    pub fn kernel(comptime name: []const u8, comptime decode: bool) []const u8 {
        return template.render(text, .{ .name = name, .decode = decode });
    }
    const text =
        \\extern "C" __global__ void {{name}}(
        \\    float* dst,
        \\    const float* q,
        \\    const float* k,
        \\    const float* v,
        \\    const unsigned char* attn_or_mask,
        \\    const float* bias,
        \\    unsigned int batch,
        \\    unsigned int q_seq_len,
        \\    unsigned int kv_seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int num_kv_heads,
        \\    unsigned int head_dim,
        \\    unsigned int query_position_offset,
        \\    unsigned int kv_position_offset,
        \\    unsigned int sliding_window,
        \\    unsigned int total_sequence_len,
        \\    unsigned int mask_len,
        \\    unsigned int bias_mode{{#decode}},
        \\    const unsigned int* decode_scalars{{/decode}}
        \\) {
        \\    {{#decode}}if (decode_scalars != 0) {
        \\        kv_position_offset = decode_scalars[4];
        \\        query_position_offset = decode_scalars[1];
        \\        kv_seq_len = decode_scalars[2];
        \\        total_sequence_len = decode_scalars[3]{{/decode}}{{^decode}}(void)attn_or_mask{{/decode}};
        \\    {{#decode}}}{{/decode}}{{^decode}}(void)bias;
        \\    (void)total_sequence_len;{{/decode}}
        \\
        \\    __shared__ float warp_sums[16];
        \\    __shared__ float shared_max_score;
        \\    __shared__ float shared_denom;
        \\    __shared__ float shared_alpha;
        \\    __shared__ float shared_beta;
        \\    unsigned int lane = threadIdx.x;
        \\    unsigned int {{#decode}}head{{/decode}}{{^decode}}block{{/decode}} = blockIdx.x;
        \\    {{^decode}}unsigned int total_blocks = batch * q_seq_len * num_heads;
        \\    {{/decode}}if (batch {{#decode}}!{{/decode}}{{^decode}}== 0u ||
        \\        q_seq_len <{{/decode}}= 1u ||
        \\        {{#decode}}q_seq_len != 1u{{/decode}}{{^decode}}block >= total_blocks{{/decode}} ||
        \\        mask_len != 0u ||
        \\        bias_mode != 0u{{#decode}} ||
        \\        head >= num_heads{{/decode}} ||
        \\        head_dim > 512u ||
        \\        (head_dim & 31u) != 0u ||
        \\        blockDim.x < head_dim ||
        \\        num_kv_heads == 0u ||
        \\        (num_heads % num_kv_heads) != 0u) return;
        \\
        \\    unsigned int {{^decode}}head = block % num_heads;
        \\    unsigned int tmp = block / num_heads;
        \\    unsigned int qi = tmp % q_seq_len;
        \\    unsigned int b = tmp / q_seq_len;
        \\    unsigned int query_pos = query_position_offset + qi;
        \\
        \\    unsigned int {{/decode}}key_start = 0u;
        \\    unsigned int key_end = 0u{{#decode}};
        \\    unsigned int query_pos = query_position_offset{{/decode}};
        \\    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        \\        unsigned int visible = query_pos - kv_position_offset + 1u;
        \\        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        \\        if (sliding_window != 0u) {
        \\            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
        \\            if (window_start_abs > kv_position_offset) {
        \\                key_start = window_start_abs - kv_position_offset;
        \\                if (key_start > key_end) key_start = key_end;
        \\            }
        \\        }
        \\    }
        \\
        \\    unsigned int heads_per_group = num_heads / num_kv_heads;
        \\    unsigned int kv_head = head / heads_per_group;
        \\    unsigned int {{^decode}}q_hidden = num_heads * head_dim;
        \\    unsigned int {{/decode}}kv_hidden = num_kv_heads * head_dim;
        \\    unsigned int q_base ={{^decode}} (b * q_seq_len + qi) * q_hidden +{{/decode}} head * head_dim;
        \\    float scale = rsqrtf((float)head_dim);
        \\    if (lane == 0u) {
        \\        shared_max_score = -3.402823466e+38f;
        \\        shared_denom = 0.0f;
        \\    }
        \\    __syncthreads();
        \\
        \\    float acc = 0.0f;
        \\    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        \\        float partial = 0.0f;
        \\        if (lane < head_dim) {
        \\            unsigned int k_base = {{#decode}}ki{{/decode}}{{^decode}}(b * kv_seq_len + ki){{/decode}} * kv_hidden + kv_head * head_dim;
        \\            partial = q[q_base + lane] * k[k_base + lane];
        \\        }
        \\        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        \\        if (lane == 0u) {
        \\            float score = dot * scale;
        \\            float next_max = fmaxf(shared_max_score, score);
        \\            shared_alpha = expf(shared_max_score - next_max);
        \\            shared_beta = expf(score - next_max);
        \\            shared_denom = shared_denom * shared_alpha + shared_beta;
        \\            shared_max_score = next_max;
        \\        }
        \\        __syncthreads();
        \\        if (lane < head_dim) {
        \\            unsigned int v_idx = {{#decode}}ki{{/decode}}{{^decode}}(b * kv_seq_len + ki){{/decode}} * kv_hidden + kv_head * head_dim + lane;
        \\            acc = acc * shared_alpha + shared_beta * v[v_idx];
        \\        }
        \\        __syncthreads();
        \\    }
        \\
        \\    if (lane < head_dim) {
        \\        unsigned int out_idx = {{^decode}}(b * q_seq_len + qi) * q_hidden + {{/decode}}head * head_dim + lane;
        \\        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
        \\    }
        \\}
        \\
    ;
};

/// Fused bias-free Q/K/V projection, one output column per thread; `bf16`
/// reads bf16 weights.
pub const qkv_tiled = struct {
    pub fn kernel(comptime name: []const u8, comptime bf16: bool) []const u8 {
        return template.render(text, .{ .name = name, .bf16 = bf16 });
    }
    const text =
        \\extern "C" __global__ void {{name}}(
        \\    float* dst_q,
        \\    float* dst_k,
        \\    float* dst_v,
        \\    const float* input,
        \\    const {{#bf16}}unsigned short{{/bf16}}{{^bf16}}float{{/bf16}}* weight_q,
        \\    const {{#bf16}}unsigned short{{/bf16}}{{^bf16}}float{{/bf16}}* weight_k,
        \\    const {{#bf16}}unsigned short{{/bf16}}{{^bf16}}float{{/bf16}}* weight_v,
        \\    unsigned int rows,
        \\    unsigned int in_dim,
        \\    unsigned int q_out_dim,
        \\    unsigned int kv_out_dim
        \\) {
        \\    unsigned int q_plane = rows * q_out_dim;
        \\    unsigned int kv_plane = rows * kv_out_dim;
        \\    unsigned int total = q_plane + kv_plane * 2u;
        \\    unsigned int global = blockIdx.x;
        \\    if (global >= total) return;
        \\
        \\    const {{#bf16}}unsigned short{{/bf16}}{{^bf16}}float{{/bf16}}* weight;
        \\    float* dst;
        \\    unsigned int out_dim;
        \\    unsigned int local;
        \\    if (global < q_plane) {
        \\        weight = weight_q;
        \\        dst = dst_q;
        \\        out_dim = q_out_dim;
        \\        local = global;
        \\    } else if (global < q_plane + kv_plane) {
        \\        weight = weight_k;
        \\        dst = dst_k;
        \\        out_dim = kv_out_dim;
        \\        local = global - q_plane;
        \\    } else {
        \\        weight = weight_v;
        \\        dst = dst_v;
        \\        out_dim = kv_out_dim;
        \\        local = global - q_plane - kv_plane;
        \\    }
        \\
        \\    unsigned int row = local / out_dim;
        \\    unsigned int col = local - row * out_dim;
        \\    unsigned int tid = threadIdx.x;
        \\    __shared__ float partial[256];
        \\    float acc = 0.0f;
        \\    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        \\        acc += input[row * in_dim + i] * {{#bf16}}termite_bf16_to_f32({{/bf16}}weight[col * in_dim + i]{{#bf16}}){{/bf16}};
        \\    }
        \\    partial[tid] = acc;
        \\    __syncthreads();
        \\    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        \\        if (tid < stride) partial[tid] += partial[tid + stride];
        \\        __syncthreads();
        \\    }
        \\    if (tid == 0u) dst[local] = partial[0];
        \\}
        \\
    ;
};

/// Four warps score independent keys of a windowed encoder attention row.
/// `laya` is the Laya model's form; otherwise GLiNER ModernBERT's.
pub const warp = struct {
    pub fn kernel(comptime name: []const u8, comptime laya: bool) []const u8 {
        return template.render(text, .{ .name = name, .laya = laya });
    }
    const text =
        \\// {{#laya}}Four warps score independent keys. No block barrier inside the key loop.{{/laya}}{{^laya}}GLiNER ModernBERT: dynamic shared scores cover only the attended key{{/laya}}
        \\// {{#laya}}All reductions and activations remain FP32; no tensor-core narrowing{{/laya}}{{^laya}}window. Global layers support 8192 keys; local layers retain bounded scratch{{/laya}}.
        \\extern "C" __global__ void {{name}}(
        \\    float* dst, const float* q, const float* k, const float* v,
        \\    const long long* mask, unsigned int batch, unsigned int seq,
        \\    unsigned int heads, unsigned int dim, unsigned int radius
        \\) {
        \\    const unsigned int row = blockIdx.x;
        \\    if (row >= batch * seq * heads || seq == 0u || seq > {{#laya}}512u{{/laya}}{{^laya}}8192u{{/laya}} ||
        \\        (dim != 64u && dim != 128u) || blockDim.x != 128u) return;
        \\    const unsigned int tid = threadIdx.x, lane = tid & 31u, warp = tid >> 5u;
        \\    const unsigned int head = row % heads, qi = (row / heads) % seq;
        \\    const unsigned int b = row / (heads * seq), hidden = heads * dim;
        \\    const unsigned int base = (b * seq + qi) * hidden + head * dim;
        \\    const unsigned int begin = qi > radius ? qi - radius : 0u;
        \\    const unsigned int end = radius >= seq - 1u - qi ? seq : qi + radius + 1u;
        \\    float query[4];
        \\    #pragma unroll
        \\    for (unsigned int j = 0; j < 4u; ++j)
        \\        query[j] = lane + j * 32u < dim ? q[base + lane + j * 32u] : 0.0f;
        \\    {{^laya}}extern {{/laya}}__shared__ float scores[{{#laya}}512{{/laya}}];
        \\    __shared__ float partial[4];
        \\    __shared__ float maximum, denominator;
        \\    for (unsigned int ki = begin + warp; ki < end; ki += 4u) {
        \\        const bool valid = !mask || mask[b * seq + ki] != 0ll;
        \\        float dot = 0.0f;
        \\        if (valid) {
        \\            const unsigned int kb = (b * seq + ki) * hidden + head * dim;
        \\            #pragma unroll
        \\            for (unsigned int j = 0; j < 4u; ++j)
        \\                if (lane + j * 32u < dim) dot += query[j] * k[kb + lane + j * 32u];
        \\        }
        \\        for (unsigned int shift = 16u; shift; shift >>= 1u)
        \\            dot += __shfl_down_sync(0xffffffffu, dot, shift);
        \\        if (lane == 0u) scores[ki{{^laya}} - begin{{/laya}}] = valid ? dot * rsqrtf((float)dim) : -INFINITY;
        \\    }
        \\    __syncthreads();
        \\    float m = -INFINITY;
        \\    for (unsigned int ki = begin + tid; ki < end; ki += 128u) m = fmaxf(m, scores[ki{{^laya}} - begin{{/laya}}]);
        \\    for (unsigned int shift = 16u; shift; shift >>= 1u)
        \\        m = fmaxf(m, __shfl_down_sync(0xffffffffu, m, shift));
        \\    if (lane == 0u) partial[warp] = m;
        \\    __syncthreads();
        \\    if (tid == 0u) maximum = fmaxf(fmaxf(partial[0], partial[1]), fmaxf(partial[2], partial[3]));
        \\    __syncthreads();
        \\    float sum = 0.0f;
        \\    for (unsigned int ki = begin + tid; ki < end; ki += 128u) {
        \\        float p = isfinite(maximum) ? expf(scores[ki{{^laya}} - begin{{/laya}}] - maximum) : 0.0f;
        \\        scores[ki{{^laya}} - begin{{/laya}}] = p;
        \\        sum += p;
        \\    }
        \\    for (unsigned int shift = 16u; shift; shift >>= 1u)
        \\        sum += __shfl_down_sync(0xffffffffu, sum, shift);
        \\    if (lane == 0u) partial[warp] = sum;
        \\    __syncthreads();
        \\    if (tid == 0u) denominator = (partial[0] + partial[1]) + (partial[2] + partial[3]);
        \\    __syncthreads();
        \\    if (tid < dim) {
        \\        float acc = 0.0f;
        \\        for (unsigned int ki = begin; ki < end; ++ki)
        \\            acc += scores[ki{{^laya}} - begin{{/laya}}] * v[(b * seq + ki) * hidden + head * dim + tid];
        \\        dst[base + tid] = denominator > 0.0f ? acc / denominator : 0.0f;
        \\    }
        \\}
        \\
    ;
};

/// One block per (batch, query, head): a 128-thread tree reduction per key
/// score, scores kept in shared memory (up to 512 keys), then the softmax and
/// the weighted sum. `laya` limits keys to Laya's local radius window.
pub const block = struct {
    pub fn kernel(comptime name: []const u8, comptime laya: bool) []const u8 {
        return template.render(text, .{ .name = name, .laya = laya });
    }
    const text =
        \\extern "C" __global__ void {{name}}(
        \\    float* dst,
        \\    const float* q,
        \\    const float* k,
        \\    const float* v,
        \\    const long long* mask,
        \\    {{^laya}}const float* bias,
        \\    {{/laya}}unsigned int batch,
        \\    unsigned int seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int head_dim,
        \\    unsigned int {{#laya}}radius
        \\) {{{/laya}}{{^laya}}causal,{{/laya}}
        \\    {{#laya}}const {{/laya}}{{^laya}}unsigned int has_mask,
        \\    unsigned int bias_mode,
        \\    {{/laya}}unsigned int head_major{{#laya}} = 0;{{/laya}}{{^laya}}
        \\) {{{/laya}}
        \\    unsigned int row_id = blockIdx.x;
        \\    unsigned int total_rows = batch * seq_len * num_heads;
        \\    if (row_id >= total_rows || seq_len > 512u || head_dim > 128u) return;
        \\    unsigned int head = row_id % num_heads;
        \\    unsigned int tmp = row_id / num_heads;
        \\    unsigned int qi = tmp % seq_len;
        \\    unsigned int b = tmp / seq_len;
        \\    unsigned int tid = threadIdx.x;
        \\    unsigned int hidden = num_heads * head_dim;
        \\    __shared__ float scratch[128];
        \\    __shared__ float scores[512];
        \\    float scale = rsqrtf((float)head_dim);
        \\    unsigned int q_base = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim : (b * seq_len + qi) * hidden + head * head_dim;
        \\
        \\    {{#laya}}const unsigned int begin = qi > radius ? qi - radius : 0u;
        \\    const unsigned int end = radius >= seq_len - 1u - qi ? seq_len : qi + radius + 1u;
        \\    {{/laya}}float max_score = -{{#laya}}INFINITY{{/laya}}{{^laya}}3.402823466e+38f{{/laya}};
        \\    for (unsigned int ki = {{#laya}}begin{{/laya}}{{^laya}}0{{/laya}}; ki < {{#laya}}end{{/laya}}{{^laya}}seq_len{{/laya}}; ++ki) {
        \\        bool valid = {{^laya}}!(causal && ki > qi) && !(has_mask && {{/laya}}mask[b * seq_len + ki] {{#laya}}!= 0ll{{/laya}}{{^laya}}== 0ll){{/laya}};
        \\        float part = 0.0f;
        \\        if (valid) {
        \\            unsigned int k_base = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim : (b * seq_len + ki) * hidden + head * head_dim;
        \\            for (unsigned int d = tid; d < head_dim; d += blockDim.x) part += q[q_base + d] * k[k_base + d];
        \\        }
        \\        scratch[tid] = part;
        \\        __syncthreads();
        \\        for (unsigned int stride = 64u; stride > 0u; stride >>= 1u) {
        \\            if (tid < stride) scratch[tid] += scratch[tid + stride];
        \\            __syncthreads();
        \\        }
        \\        if (tid == 0u) {
        \\            float score = valid ? scratch[0] * scale : -{{#laya}}INFINITY{{/laya}}{{^laya}}3.402823466e+38f;
        \\            if (valid && bias_mode == 1u) score += bias[(head * seq_len + qi) * seq_len + ki];
        \\            if (valid && bias_mode == 2u) score += bias[((b * num_heads + head) * seq_len + qi) * seq_len + ki]{{/laya}};
        \\            scores[ki] = score;
        \\            max_score = fmaxf(max_score, score);
        \\        }
        \\        __syncthreads();
        \\    }
        \\
        \\    __shared__ float shared_max;
        \\    __shared__ float shared_denom;
        \\    if (tid == 0u) shared_max = max_score;
        \\    __syncthreads();
        \\
        \\    float denom_part = 0.0f;
        \\    for (unsigned int ki = {{#laya}}begin + {{/laya}}tid; ki < {{#laya}}end{{/laya}}{{^laya}}seq_len{{/laya}}; ki += blockDim.x) {
        \\        float e = {{#laya}}isfinite(shared_max) ? {{/laya}}expf(scores[ki] - shared_max){{#laya}} : 0.0f{{/laya}};
        \\        scores[ki] = e;
        \\        denom_part += e;
        \\    }
        \\    scratch[tid] = denom_part;
        \\    __syncthreads();
        \\    for (unsigned int stride = 64u; stride > 0u; stride >>= 1u) {
        \\        if (tid < stride) scratch[tid] += scratch[tid + stride];
        \\        __syncthreads();
        \\    }
        \\    if (tid == 0u) shared_denom = scratch[0];
        \\    __syncthreads();
        \\
        \\    for (unsigned int d = tid; d < head_dim; d += blockDim.x) {
        \\        float acc = 0.0f;
        \\        for (unsigned int ki = {{#laya}}begin{{/laya}}{{^laya}}0{{/laya}}; ki < {{#laya}}end{{/laya}}{{^laya}}seq_len{{/laya}}; ++ki) {
        \\            unsigned int v_idx = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim + d : (b * seq_len + ki) * hidden + head * head_dim + d;
        \\            acc += scores[ki] * v[v_idx];
        \\        }
        \\        unsigned int out_idx = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim + d : (b * seq_len + qi) * hidden + head * head_dim + d;
        \\        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
        \\    }
        \\}
        \\
    ;
};

/// BERT prefill (256 keys, head_dim 64) with several query rows per block
/// sharing K/V tiles: 8 rows, or 16 with `q16`.
pub const bert_prefill = struct {
    pub fn kernel(comptime name: []const u8, comptime q16: bool) []const u8 {
        return template.render(text, .{ .name = name, .q16 = q16 });
    }
    const text =
        \\// {{#q16}}Sixteen-query version{{/q16}}{{^q16}}Eight-query tile{{/q16}} of the {{#q16}}exact q8 schedule above. It preserves every{{/q16}}{{^q16}}same S=256, D=64 BERT attention.  Each warp owns a{{/q16}}
        \\// {{#q16}}per-row arithmetic and reduction order, but doubles query rows per launch{{/q16}}{{^q16}}query row, which removes the cross-warp hand-offs in the single-query{{/q16}}
        \\// {{#q16}}block to reduce grid traffic and improve occupancy on encoder prefill{{/q16}}{{^q16}}schedule and gives BERT's medium-batch prefill enough independent work to
        \\// fill the GPU.  The reduction tree deliberately mirrors the single-query
        \\// generic kernel so encoder embeddings remain bit-stable{{/q16}}.
        \\extern "C" __global__ void {{name}}(
        \\    float* dst,
        \\    const float* q,
        \\    const float* k,
        \\    const float* v,
        \\    const long long* mask,
        \\    const float* bias,
        \\    unsigned int batch,
        \\    unsigned int seq_len,
        \\    unsigned int num_heads,
        \\    unsigned int head_dim,
        \\    unsigned int causal,
        \\    unsigned int has_mask,
        \\    unsigned int bias_mode,
        \\    unsigned int head_major
        \\) {
        \\    (void)mask;
        \\    (void)bias;
        \\    (void)causal;
        \\    (void)has_mask;
        \\    (void)bias_mode;
        \\    (void)head_major;
        \\
        \\    const unsigned {{#q16}}long long{{/q16}}{{^q16}}int{{/q16}} tile_id = blockIdx.x;
        \\    const unsigned int tiles_per_head = {{#q16}}16u{{/q16}}{{^q16}}32u{{/q16}};
        \\    const unsigned {{#q16}}long long{{/q16}}{{^q16}}int{{/q16}} total_tiles = {{#q16}}static_cast<unsigned long long>(batch){{/q16}}{{^q16}}batch{{/q16}} * num_heads * tiles_per_head;
        \\    if (tile_id >= total_tiles || seq_len != 256u || head_dim != 64u{{#q16}} || blockDim.x != 512u{{/q16}}) return;
        \\
        \\    const unsigned int tid = threadIdx.x;
        \\    const unsigned int lane = tid & 31u;
        \\    const unsigned int warp = tid >> 5u;
        \\    const unsigned int tile = {{#q16}}static_cast<unsigned int>({{/q16}}tile_id % tiles_per_head{{#q16}}){{/q16}};
        \\    const unsigned {{#q16}}long long{{/q16}}{{^q16}}int{{/q16}} tmp = tile_id / tiles_per_head;
        \\    const unsigned int head = {{#q16}}static_cast<unsigned int>({{/q16}}tmp % num_heads{{#q16}}){{/q16}};
        \\    const unsigned int b = {{#q16}}static_cast<unsigned int>({{/q16}}tmp / num_heads{{#q16}}){{/q16}};
        \\    const unsigned int qi = tile * {{#q16}}16u{{/q16}}{{^q16}}8u{{/q16}} + warp;
        \\    const {{#q16}}size_t{{/q16}}{{^q16}}unsigned int{{/q16}} base = ({{#q16}}static_cast<size_t>(b){{/q16}}{{^q16}}(b{{/q16}} * num_heads + head) * 256u{{^q16}}){{/q16}} * 64u;
        \\    const {{#q16}}size_t{{/q16}}{{^q16}}unsigned int{{/q16}} q_base = base + qi * 64u;
        \\    const float q0 = q[q_base + lane];
        \\    const float q1 = q[q_base + lane + 32u];
        \\
        \\    __shared__ float scores[{{#q16}}16{{/q16}}{{^q16}}8{{/q16}}][256];{{^q16}}
        \\    // One [32, 64] tile is reused for K and V.  At B8 the eight query warps
        \\    // otherwise repeatedly fetch the same 256-byte rows from global memory.{{/q16}}
        \\    __shared__ float kv_tile[32][64];
        \\    float max_score = -3.402823466e+38f;
        \\    for (unsigned int key_start = 0u; key_start < 256u; key_start += 32u) {
        \\        for (unsigned int element = tid; element < 32u * 64u; element += blockDim.x) {
        \\            kv_tile[element / 64u][element % 64u] = k[base + key_start * 64u + element];
        \\        }
        \\        __syncthreads();
        \\        #pragma unroll
        \\        for (unsigned int key = 0u; key < 32u; ++key) {
        \\            const float dot_lo = q0 * kv_tile[key][lane];
        \\            const float dot_hi = q1 * kv_tile[key][lane + 32u];
        \\            float dot = __fadd_rn(dot_lo, dot_hi);
        \\            #pragma unroll
        \\            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        \\                dot = __fadd_rn(dot, __shfl_down_sync(0xffffffffu, dot, offset));
        \\            }
        \\            if (lane == 0u) {
        \\                const float score = dot * 0.125f;
        \\                scores[warp][key_start + key] = score;
        \\                max_score = fmaxf(max_score, score);
        \\            }
        \\        }
        \\        __syncthreads();
        \\    }
        \\    max_score = __shfl_sync(0xffffffffu, max_score, 0);
        \\    __syncwarp();
        \\
        \\    {{^q16}}// Reconstruct the generic 128-lane denominator reduction exactly.  Each
        \\    // lane owns four of its partial sums, then the warp performs the remaining
        \\    // 32-way tail of that same reduction tree.
        \\    {{/q16}}const unsigned int score_base = lane;
        \\    const float e0 = expf(scores[warp][score_base] - max_score);
        \\    const float e1 = expf(scores[warp][score_base + 32u] - max_score);
        \\    const float e2 = expf(scores[warp][score_base + 64u] - max_score);
        \\    const float e3 = expf(scores[warp][score_base + 96u] - max_score);
        \\    const float e4 = expf(scores[warp][score_base + 128u] - max_score);
        \\    const float e5 = expf(scores[warp][score_base + 160u] - max_score);
        \\    const float e6 = expf(scores[warp][score_base + 192u] - max_score);
        \\    const float e7 = expf(scores[warp][score_base + 224u] - max_score);
        \\    scores[warp][score_base] = e0;
        \\    scores[warp][score_base + 32u] = e1;
        \\    scores[warp][score_base + 64u] = e2;
        \\    scores[warp][score_base + 96u] = e3;
        \\    scores[warp][score_base + 128u] = e4;
        \\    scores[warp][score_base + 160u] = e5;
        \\    scores[warp][score_base + 192u] = e6;
        \\    scores[warp][score_base + 224u] = e7;
        \\    float denom = __fadd_rn(
        \\        __fadd_rn(__fadd_rn(e0, e4), __fadd_rn(e2, e6)),
        \\        __fadd_rn(__fadd_rn(e1, e5), __fadd_rn(e3, e7))
        \\    );
        \\    #pragma unroll
        \\    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        \\        denom = __fadd_rn(denom, __shfl_down_sync(0xffffffffu, denom, offset));
        \\    }
        \\    denom = __shfl_sync(0xffffffffu, denom, 0);
        \\    __syncwarp();
        \\
        \\    float acc0 = 0.0f;
        \\    float acc1 = 0.0f;
        \\    for (unsigned int key_start = 0u; key_start < 256u; key_start += 32u) {
        \\        for (unsigned int element = tid; element < 32u * 64u; element += blockDim.x) {
        \\            kv_tile[element / 64u][element % 64u] = v[base + key_start * 64u + element];
        \\        }
        \\        __syncthreads();
        \\        #pragma unroll
        \\        for (unsigned int key = 0u; key < 32u; ++key) {
        \\            const float weight = scores[warp][key_start + key];
        \\            acc0 += weight * kv_tile[key][lane];
        \\            acc1 += weight * kv_tile[key][lane + 32u];
        \\        }
        \\        __syncthreads();
        \\    }
        \\    dst[q_base + lane] = acc0 / denom;
        \\    dst[q_base + lane + 32u] = acc1 / denom;
        \\}
        \\
    ;
};

/// GLiNER boundary attention on f16 tensor cores for long sequences with
/// 64-wide relative tiles; `compact` shares one product between adjacent equal
/// relative buckets.
pub const gliner_boundary_tc = struct {
    pub fn kernel(comptime name: []const u8, comptime compact: bool) []const u8 {
        return template.render(text, .{ .name = name, .compact = compact });
    }
    const text =
        \\// {{#compact}}Long-sequence direct attention. Adjacent equal relative buckets share one{{/compact}}{{^compact}}Wider direct boundary attention for sequences of at least 512 tokens.{{/compact}}
        \\// {{#compact}}product; the run list is derived from every actual bucket, so arbitrary{{/compact}}{{^compact}}Query/key fragments stay in registers; 16-column relative tiles scatter{{/compact}}
        \\// {{#compact}}non-monotonic maps remain correct. Products, score addition and expf retain{{/compact}}{{^compact}}only their diagonal gathers. This requires no global relative-product cache{{/compact}}
        \\// {{#compact}}the wider tile's arithmetic. No global cache or additional workspace{{/compact}}{{^compact}}and preserves the same admitted workspace contract as the short tile{{/compact}}.
        \\extern "C" __global__ __launch_bounds__(256) void {{name}}(
        \\    const float* q, const float* k, const float* v, const float* q_r,
        \\    const float* k_r, const float* bucket_values, const float* mask_values,
        \\    const float* unused7, const float* unused8, const float* unused9,
        \\    float* dst, Gliner25BoundaryParams p, unsigned int work_items
        \\) {
        \\    const unsigned int batch=p.dims[0], seq=p.dims[1], heads=p.dims[2], dim=p.dims[3], relative_rows=p.dims[4];
        \\    const int* mask=reinterpret_cast<const int*>(mask_values);
        \\    const int* buckets=reinterpret_cast<const int*>(bucket_values);
        \\    constexpr unsigned int M = 64u, N = 64u, D = 64u, P = 72u, S = 68u, C = 20u, T = 256u;
        \\    if (!seq || seq > 16384u || dim != D || blockDim.x != T) return;
        \\    const unsigned int tid = threadIdx.x, lane = tid & 31u, warp = tid >> 5u;
        \\    const unsigned int tiles = (seq + M - 1u) / M;
        \\    const unsigned int start = (blockIdx.x % tiles) * M;
        \\    const unsigned int matrix = blockIdx.x / tiles, head = matrix % heads, b = matrix / heads;
        \\    if (b >= batch) return;
        \\    const unsigned int hidden = heads * D, head_off = head * D;
        \\    __shared__ __align__(16) half kv[N * P], qr[16u * P], kr[16u * P];
        \\    __shared__ __align__(16) float shared_scores[M * P / 2u + 32u * S];
        \\    __shared__ __align__(16) float relative_scores[2u * M * C];
        \\    half* qt = reinterpret_cast<half*>(shared_scores);
        \\    half* probabilities = reinterpret_cast<half*>(shared_scores);
        \\    float* scratch = shared_scores + M * P / 2u;
        \\    __shared__ float maximum[M], denominator[M], alpha[M];
        \\    __shared__ unsigned int invalid[M{{#compact}}], warp_counts[8], run_start[128], run_count;
        \\    __shared__ int run_bucket[128{{/compact}}];
        \\    wmma::fragment<wmma::accumulator, 16, 16, 16, float> output[2];
        \\    wmma::fragment<wmma::matrix_a,16,16,16,half,wmma::row_major> rows[4];
        \\    #pragma unroll
        \\    for (unsigned int g = 0u; g < 2u; ++g) wmma::fill_fragment(output[g], 0.0f);
        \\    for (unsigned int i = tid; i < M * D; i += T) {
        \\        const unsigned int row = i / D, d = i % D;
        \\        qt[row * P + d] = __float2half_rn(start + row < seq ? q[(b * seq + start + row) * hidden + head_off + d] : 0.0f);
        \\    }
        \\    if (tid < M) { maximum[tid] = -INFINITY; denominator[tid] = 0.0f; invalid[tid] = 0u; }
        \\    __syncthreads();
        \\    if (warp < 4u) {
        \\        #pragma unroll
        \\        for (unsigned int d=0u;d<4u;++d) wmma::load_matrix_sync(rows[d], qt+warp*16u*P+d*16u, P);
        \\    }
        \\    __syncthreads();
        \\    for (unsigned int key_start = 0u; key_start < seq; key_start += N) {
        \\        for (unsigned int i = tid; i < N * D; i += T) {
        \\            const unsigned int row = i / D, d = i % D, key = key_start + row;
        \\            const bool valid = key < seq && (!mask || mask[b * seq + key] != 0ll);
        \\            const unsigned int offset = (b * seq + key) * hidden + head_off + d;
        \\            kv[row * P + d] = __float2half_rn(valid ? k[offset] : 0.0f);
        \\        }
        \\        __syncthreads();
        \\        if (warp < 4u) {
        \\            #pragma unroll
        \\            for (unsigned int kc=0u;kc<4u;++kc) {
        \\                wmma::fragment<wmma::accumulator,16,16,16,float> dot;
        \\                wmma::fill_fragment(dot,0.0f);
        \\                #pragma unroll
        \\                for (unsigned int d=0u;d<4u;++d) {
        \\                    wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::col_major> keys;
        \\                    wmma::load_matrix_sync(keys,kv+kc*16u*P+d*16u,P);
        \\                    wmma::mma_sync(dot,rows[d],keys,dot);
        \\                }
        \\                wmma::store_matrix_sync(shared_scores+warp*16u*S+kc*16u,dot,S,wmma::mem_row_major);
        \\            }
        \\        } else {
        \\            #pragma unroll
        \\            for (unsigned int d=0u;d<4u;++d) wmma::load_matrix_sync(rows[d],kv+(warp-4u)*16u*P+d*16u,P);
        \\        }
        \\        __syncthreads();
        \\        // Both relative products are computed in compact 16-column tiles.
        \\        // Scatter only their exact diagonal gather into QK before reusing the
        \\        // tile. No token-by-relative or quadratic global buffer is needed.
        \\        {{#compact}}const int first_offset=(int)start+(int)seq-1-(int)key_start-(int)(N-1u);
        \\        const int here=first_offset+(int)tid;
        \\        const int bucket_value=here>=0 && here<(int)(2u*seq-1u)?buckets[here]:-1;
        \\        const int previous=here>0 && here<=(int)(2u*seq-1u)?buckets[here-1]:-1;
        \\        const bool begins=tid<M+N-1u && (tid==0 || bucket_value!=previous);
        \\        unsigned int prefix=begins?1u:0u;
        \\        #pragma unroll
        \\        for (unsigned int d=1;d<32;d*=2) { unsigned int earlier=__shfl_up_sync(0xffffffffu,prefix,d); if(lane>=d)prefix+=earlier; }
        \\        if (lane==31u)warp_counts[warp]=prefix;
        \\        __syncthreads();
        \\        for (unsigned int w=0;w<warp;++w)prefix+=warp_counts[w];
        \\        if (begins) { run_start[prefix-1u]=tid;run_bucket[prefix-1u]=bucket_value; }
        \\        if (tid==M+N-2u)run_count=prefix;
        \\        __syncthreads();
        \\        const bool uniform=run_count==1u;
        \\        const unsigned int relative_count=run_count;
        \\        {{/compact}}for (unsigned int rs=0u;rs<{{#compact}}relative_count{{/compact}}{{^compact}}M+N-1u{{/compact}};rs+=16u) {
        \\            for (unsigned int i=tid;i<16u*D;i+=T) {
        \\                const unsigned int r=i/D,d=i%D;
        \\                const int {{#compact}}bucket=rs+r<run_count?run_bucket[rs+r{{/compact}}{{^compact}}offset=(int)start+(int)seq-1-(int)key_start-(int)(N-1u)+(int)rs+(int)r;
        \\                const int bucket=offset>=0 && offset<(int)(2u*seq-1u)?buckets[offset{{/compact}}]:-1;
        \\                const bool valid=bucket>=0 && (unsigned int)bucket<relative_rows;
        \\                qr[r*P+d]=__float2half_rn(valid?q_r[(unsigned int)bucket*hidden+head_off+d]:0.0f);
        \\                kr[r*P+d]=__float2half_rn(valid?k_r[(unsigned int)bucket*hidden+head_off+d]:0.0f);
        \\            }
        \\            __syncthreads();
        \\            wmma::fragment<wmma::accumulator,16,16,16,float> relative;
        \\            wmma::fill_fragment(relative,0.0f);
        \\            #pragma unroll
        \\            for (unsigned int d=0u;d<4u;++d) {
        \\                wmma::fragment<wmma::matrix_b,16,16,16,half,wmma::col_major> rf;
        \\                wmma::load_matrix_sync(rf,(warp<4u?kr:qr)+d*16u,P);
        \\                wmma::mma_sync(relative,rows[d],rf,relative);
        \\            }
        \\            wmma::store_matrix_sync(relative_scores+warp*16u*C,relative,C,wmma::mem_row_major);
        \\            __syncthreads();
        \\            {{#compact}}if (uniform) {
        \\                for (unsigned int i=tid;i<M*N;i+=T) {
        \\                    const unsigned int row=i/N,col=i%N;
        \\                    shared_scores[row*S+col]=(shared_scores[row*S+col]+relative_scores[row*C])+relative_scores[M*C+col*C];
        \\                }
        \\            } else {{/compact}}for (unsigned int i=tid;i<M*16u;i+=T) {
        \\                const unsigned int row=i/16u,r=rs+i%16u;
        \\                {{#compact}}if (r<run_count) {
        \\                    {{/compact}}const int {{#compact}}lo=max(0,{{/compact}}{{^compact}}col={{/compact}}(int)row+(int)N-{{#compact}}(int)(r+1u<run_count?run_start[r+1u]:M+N-1u));
        \\                    const int hi=min((int)N,(int)row+(int)N{{/compact}}{{^compact}}1{{/compact}}-(int){{#compact}}run_start[r]);
        \\                    for (int col=lo;col<hi;++col)
        \\                        {{/compact}}{{^compact}}r;
        \\                if (col>=0 && col<(int)N)
        \\                    {{/compact}}shared_scores[row*S+col]=(shared_scores[row*S+col]+relative_scores[row*C+i%16u])+relative_scores[M*C+(unsigned int)col*C+i%16u];{{#compact}}
        \\                }{{/compact}}
        \\            }
        \\            __syncthreads();
        \\        }
        \\        float staged_probabilities[8][2];
        \\        #pragma unroll
        \\        for (unsigned int g = 0u; g < 8u; ++g) {
        \\            const unsigned int row = warp * 8u + g;
        \\            float values[2], next_max = -INFINITY;
        \\            #pragma unroll
        \\            for (unsigned int j = 0u; j < 2u; ++j) {
        \\                const unsigned int n = lane + j * 32u, key = key_start + n;
        \\                const bool valid = start + row < seq && key < seq && (!mask || mask[b * seq + key] != 0ll);
        \\                float score=-INFINITY;
        \\                if (valid) {
        \\                    const int bucket=buckets[start+row+seq-1u-key];
        \\                    if (bucket<0 || (unsigned int)bucket>=relative_rows) atomicOr(&invalid[row],1u);
        \\                    else {
        \\                        score=shared_scores[row*S+n]*rsqrtf(192.0f);
        \\                        if (!isfinite(score)) atomicOr(&invalid[row],1u);
        \\                    }
        \\                }
        \\                values[j] = score;
        \\                next_max = fmaxf(next_max, values[j]);
        \\            }
        \\            for (unsigned int shift = 16u; shift; shift >>= 1u)
        \\                next_max = fmaxf(next_max, __shfl_down_sync(0xffffffffu, next_max, shift));
        \\            next_max = fmaxf(maximum[row], __shfl_sync(0xffffffffu, next_max, 0u));
        \\            const float a = isfinite(maximum[row]) ? expf(maximum[row] - next_max) : 0.0f;
        \\            float sum = 0.0f;
        \\            #pragma unroll
        \\            for (unsigned int j = 0u; j < 2u; ++j) {
        \\                const float p = isfinite(next_max) ? expf(values[j] - next_max) : 0.0f;
        \\                staged_probabilities[g][j] = p;
        \\                sum += p;
        \\            }
        \\            for (unsigned int shift = 16u; shift; shift >>= 1u)
        \\                sum += __shfl_down_sync(0xffffffffu, sum, shift);
        \\            if (lane == 0u) {
        \\                alpha[row] = a;
        \\                denominator[row] = denominator[row] * a + sum;
        \\                maximum[row] = next_max;
        \\            }
        \\        }
        \\        // Half probabilities alias the lower half of shared score storage.
        \\        // No writer may reuse it before every warp has read all of its rows.
        \\        __syncthreads();
        \\        #pragma unroll
        \\        for (unsigned int g = 0u; g < 8u; ++g) {
        \\            const unsigned int row = warp * 8u + g;
        \\            #pragma unroll
        \\            for (unsigned int j = 0u; j < 2u; ++j)
        \\                probabilities[row * P + lane + j * 32u] = __float2half_rn(staged_probabilities[g][j]);
        \\        }
        \\        __syncthreads();
        \\        for (unsigned int i=tid;i<N*D;i+=T) {
        \\            const unsigned int row=i/D,d=i%D,key=key_start+row;
        \\            const bool valid=key<seq && (!mask||mask[b*seq+key]!=0);
        \\            kv[row*P+d]=__float2half_rn(valid?v[(b*seq+key)*hidden+head_off+d]:0.0f);
        \\        }
        \\        __syncthreads();
        \\        #pragma unroll
        \\        for (unsigned int g = 0u; g < 2u; ++g) {
        \\            const unsigned int qr = warp / 4u, dc = warp % 4u;
        \\            // WMMA's fragment layout is opaque. Store, rescale by row, and
        \\            // reload through the public API instead of assuming lane mapping.
        \\            wmma::store_matrix_sync(scratch + qr * 16u * S + dc * 16u, output[g], S, wmma::mem_row_major);
        \\            __syncthreads();
        \\            for (unsigned int i = tid; i < 32u * D; i += T) scratch[(i / D) * S + i % D] *= alpha[g * 32u + i / D];
        \\            __syncthreads();
        \\            wmma::load_matrix_sync(output[g], scratch + qr * 16u * S + dc * 16u, S, wmma::mem_row_major);
        \\            #pragma unroll
        \\            for (unsigned int n = 0u; n < N; n += 16u) {
        \\                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> pf;
        \\                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> vf;
        \\                wmma::load_matrix_sync(pf, probabilities + (g * 32u + qr * 16u) * P + n, P);
        \\                wmma::load_matrix_sync(vf, kv + n * P + dc * 16u, P);
        \\                wmma::mma_sync(output[g], pf, vf, output[g]);
        \\            }
        \\            __syncthreads();
        \\        }
        \\    }
        \\    #pragma unroll
        \\    for (unsigned int g = 0u; g < 2u; ++g) {
        \\        wmma::store_matrix_sync(scratch + (warp / 4u) * 16u * S + (warp % 4u) * 16u, output[g], S, wmma::mem_row_major);
        \\        __syncthreads();
        \\        for (unsigned int i = tid; i < 32u * D; i += T) {
        \\            const unsigned int row = g * 32u + i / D, d = i % D;
        \\            if (start + row < seq)
        \\                dst[(b * seq + start + row) * hidden + head_off + d] = (mask && mask[b*seq+start+row]==0) ? 0.0f : (invalid[row]==0u && denominator[row] > 0.0f ? scratch[(i / D) * S + i % D] / denominator[row] : NAN);
        \\        }
        \\        __syncthreads();
        \\    }
        \\}
        \\
    ;
};
