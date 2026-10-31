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

// native-program:entry termite_attention_f32
extern "C" __global__ void termite_attention_f32(ANTFLY_CUDA_DENSE_ATTENTION_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int hidden = num_heads * head_dim;
    unsigned int total = batch * seq_len * hidden;
    if (idx >= total) return;
    unsigned int d = idx % head_dim;
    unsigned int head;
    unsigned int qi;
    unsigned int b;
    if (head_major) {
        unsigned int tmp = idx / head_dim;
        qi = tmp % seq_len;
        tmp /= seq_len;
        head = tmp % num_heads;
        b = tmp / num_heads;
    } else {
        unsigned int tmp = idx / head_dim;
        head = tmp % num_heads;
        tmp /= num_heads;
        qi = tmp % seq_len;
        b = tmp / seq_len;
    }
    float scale = rsqrtf((float)head_dim);
    float max_score = -3.402823466e+38f;
    for (unsigned int ki = 0; ki < seq_len; ++ki) {
{{self_scores}}
        max_score = fmaxf(max_score, score);
    }
    float denom = 0.0f;
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < seq_len; ++ki) {
{{self_scores}}
        float e = expf(score - max_score);
        denom += e;
        unsigned int v_idx = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim + d : (b * seq_len + ki) * hidden + head * head_dim + d;
        acc += e * v[v_idx];
    }
    dst[idx] = denom > 0.0f ? acc / denom : 0.0f;
}
// native-program:entry termite_qwen3vl_vision_attention_tc_bf16_m32n16
extern "C" __global__ __launch_bounds__(256) void termite_qwen3vl_vision_attention_tc_bf16_m32n16(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
#include "../kernels/vision_bf16_attention_m32n16.cuh"
}
// native-program:entry termite_attention_f32_bert_prefill_s256_hd64
extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64(ANTFLY_CUDA_DENSE_ATTENTION_PARAMS) {
    (void)mask;
    (void)bias;
    (void)causal;
    (void)has_mask;
    (void)bias_mode;
    (void)head_major;

    const unsigned int row_id = blockIdx.x;
    const unsigned int total_rows = batch * seq_len * num_heads;
    if (row_id >= total_rows || seq_len != 256u || head_dim != 64u) return;

    const unsigned int head = row_id % num_heads;
    const unsigned int row = row_id / num_heads;
    const unsigned int qi = row % seq_len;
    const unsigned int b = row / seq_len;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int base = ((b * num_heads + head) * seq_len) * 64u;
    const unsigned int q_base = base + qi * 64u;
    const float q0 = q[q_base + lane];
    const float q1 = q[q_base + lane + 32u];

    __shared__ float scores[256];
    __shared__ float reductions[8];
    __shared__ float scratch[128];
    __shared__ float shared_max;
    __shared__ float shared_denom;

    float local_max = -3.402823466e+38f;
    for (unsigned int ki = warp; ki < 256u; ki += 8u) {
        const unsigned int k_base = base + ki * 64u;
        // Match the generic block kernel's separate products followed by a
        // round-to-nearest tree add.  A fused multiply-add here is fast but
        // causes enough encoder-layer drift to be visible in final embeddings.
        const float dot_lo = q0 * k[k_base + lane];
        const float dot_hi = q1 * k[k_base + lane + 32u];
        float dot = __fadd_rn(dot_lo, dot_hi);
        #pragma unroll
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot = __fadd_rn(dot, __shfl_down_sync(0xffffffffu, dot, offset));
        }
        if (lane == 0u) {
            const float score = dot * 0.125f;
            scores[ki] = score;
            local_max = fmaxf(local_max, score);
        }
    }

    if (lane == 0u) reductions[warp] = local_max;
    __syncthreads();
    if (warp == 0u) {
        float value = lane < 8u ? reductions[lane] : -3.402823466e+38f;
        #pragma unroll
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, offset));
        }
        if (lane == 0u) shared_max = value;
    }
    __syncthreads();

    // Preserve the generic S=256 denominator order: 128 lanes each sum two
    // scores, then the same 128-way reduction tree.  This costs six phase
    // barriers per row, versus seven barriers for every one of 256 keys.
    if (tid < 128u) {
        float denom_part = 0.0f;
        #pragma unroll
        for (unsigned int ki = tid; ki < 256u; ki += 128u) {
            const float e = expf(scores[ki] - shared_max);
            scores[ki] = e;
            denom_part = __fadd_rn(denom_part, e);
        }
        scratch[tid] = denom_part;
    }
    __syncthreads();
    #pragma unroll
    for (unsigned int stride = 64u; stride > 0u; stride >>= 1u) {
        if (tid < stride) scratch[tid] = __fadd_rn(scratch[tid], scratch[tid + stride]);
        __syncthreads();
    }
    if (tid == 0u) shared_denom = scratch[0];
    __syncthreads();

    if (tid < 64u) {
        float acc = 0.0f;
        #pragma unroll 4
        for (unsigned int ki = 0u; ki < 256u; ++ki) {
            acc += scores[ki] * v[base + ki * 64u + tid];
        }
        dst[q_base + tid] = acc / shared_denom;
    }
}
// native-program:entry termite_attention_f32_bert_prefill_s256_hd64_mma
extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64_mma(ANTFLY_CUDA_DENSE_ATTENTION_PARAMS) {
    (void)mask;
    (void)bias;
    (void)causal;
    (void)has_mask;
    (void)bias_mode;
    (void)head_major;

    constexpr unsigned int tile_m = 16u;
    constexpr unsigned int tile_n = 64u;
    constexpr unsigned int dim = 64u;
    constexpr unsigned int kv_pitch = 72u;
    constexpr unsigned int score_pitch = 72u;
    constexpr float attention_scale = 0.125f;
    const unsigned long long tile_id = blockIdx.x;
    const unsigned int tiles_per_head = 16u;
    const unsigned long long total_tiles = static_cast<unsigned long long>(batch) * num_heads * tiles_per_head;
    if (tile_id >= total_tiles || seq_len != 256u || head_dim != dim || blockDim.x != 256u) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int tile = static_cast<unsigned int>(tile_id % tiles_per_head);
    const unsigned long long tmp = tile_id / tiles_per_head;
    const unsigned int head = static_cast<unsigned int>(tmp % num_heads);
    const unsigned int b = static_cast<unsigned int>(tmp / num_heads);
    const unsigned int query_start = tile * tile_m;
    const size_t base = (static_cast<size_t>(b) * num_heads + head) * 256u * dim;

    extern __shared__ __align__(32) unsigned char mma_smem[];
    half* kv_tile = reinterpret_cast<half*>(mma_smem) + tile_m * dim;
    float* scores = reinterpret_cast<float*>(kv_tile + tile_n * kv_pitch);
    float* probabilities = scores + tile_m * score_pitch;
    half* probabilities_hi = reinterpret_cast<half*>(probabilities + tile_m * score_pitch);
    float* value_tile = reinterpret_cast<float*>(probabilities_hi + tile_m * score_pitch);
    float* output = value_tile + tile_n * kv_pitch;
    __shared__ float alpha[tile_m];

    for (unsigned int index = tid; index < tile_m * dim; index += blockDim.x) {
        output[index] = 0.0f;
    }
    __syncthreads();

    float max0 = -3.402823466e+38f;
    float max1 = -3.402823466e+38f;
    float denom0 = 0.0f;
    float denom1 = 0.0f;

    for (unsigned int key_start = 0u; key_start < 256u; key_start += tile_n) {
        // One warp owns two query rows. Its lanes evaluate two score columns
        // apiece, matching the FP32 dot-product contract of the exact path.
        #pragma unroll
        for (unsigned int sub = 0u; sub < 2u; ++sub) {
            const unsigned int row = warp * 2u + sub;
            const unsigned int ja = lane;
            const unsigned int jb = lane + 32u;
            float score_a = 0.0f;
            float score_b = 0.0f;
            #pragma unroll
            for (unsigned int d = 0u; d < dim; ++d) {
                const float query = q[base + (query_start + row) * dim + d];
                score_a += query * k[base + (key_start + ja) * dim + d];
                score_b += query * k[base + (key_start + jb) * dim + d];
            }
            scores[row * score_pitch + ja] = score_a;
            scores[row * score_pitch + jb] = score_b;
        }
        __syncthreads();

        #pragma unroll
        for (unsigned int sub = 0u; sub < 2u; ++sub) {
            const unsigned int row = warp * 2u + sub;
            const unsigned int ja = lane;
            const unsigned int jb = lane + 32u;
            const float score_a = scores[row * score_pitch + ja] * attention_scale;
            const float score_b = scores[row * score_pitch + jb] * attention_scale;
            float tile_max = fmaxf(score_a, score_b);
            #pragma unroll
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                tile_max = fmaxf(tile_max, __shfl_down_sync(0xffffffffu, tile_max, offset));
            }
            tile_max = __shfl_sync(0xffffffffu, tile_max, 0u);

            float* running_max = sub == 0u ? &max0 : &max1;
            float* running_denom = sub == 0u ? &denom0 : &denom1;
            const float new_max = fmaxf(*running_max, tile_max);
            const float scale_old = *running_max > -3.402823466e+38f ? expf(*running_max - new_max) : 0.0f;
            const float p_a = expf(score_a - new_max);
            const float p_b = expf(score_b - new_max);
            float sum = p_a + p_b;
            #pragma unroll
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                sum += __shfl_down_sync(0xffffffffu, sum, offset);
            }
            sum = __shfl_sync(0xffffffffu, sum, 0u);
            *running_max = new_max;
            *running_denom = *running_denom * scale_old + sum;
            probabilities[row * score_pitch + ja] = p_a;
            probabilities[row * score_pitch + jb] = p_b;
            probabilities_hi[row * score_pitch + ja] = __float2half(p_a);
            probabilities_hi[row * score_pitch + jb] = __float2half(p_b);
            if (lane == 0u) alpha[row] = scale_old;
        }
        __syncthreads();

        for (unsigned int index = tid; index < tile_m * dim; index += blockDim.x) {
            const unsigned int row = index / dim;
            output[index] *= alpha[row];
        }
        for (unsigned int index = tid; index < tile_n * dim; index += blockDim.x) {
            const unsigned int row = index / dim;
            const unsigned int col = index % dim;
            const float value = v[base + (key_start + row) * dim + col];
            kv_tile[row * kv_pitch + col] = __float2half(value);
            value_tile[row * kv_pitch + col] = value;
        }
        __syncthreads();

        if (warp < 4u) {
            const unsigned int column_chunk = warp;
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> output_acc;
            wmma::load_matrix_sync(output_acc, output + column_chunk * 16u, dim, wmma::mem_row_major);
            #pragma unroll
            for (unsigned int kb = 0u; kb < 4u; ++kb) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
                wmma::load_matrix_sync(p_frag, probabilities_hi + kb * 16u, score_pitch);
                wmma::load_matrix_sync(v_frag, kv_tile + (kb * 16u) * kv_pitch + column_chunk * 16u, kv_pitch);
                wmma::mma_sync(output_acc, p_frag, v_frag, output_acc);
            }
            wmma::store_matrix_sync(output + column_chunk * 16u, output_acc, dim, wmma::mem_row_major);
        }
        __syncthreads();

        // Reconstruct P*V exactly with respect to the full-precision softmax
        // probability and value: p*v = ph*vh + (p-ph)*v + ph*(v-vh).
        // Tensor cores compute the first, dominant term; each thread owns two
        // output columns for the two rows assigned to its warp.
        const unsigned int row0 = warp * 2u;
        const unsigned int row1 = row0 + 1u;
        for (unsigned int col = lane; col < dim; col += 32u) {
            float correction0 = 0.0f;
            float correction1 = 0.0f;
            #pragma unroll
            for (unsigned int key = 0u; key < tile_n; ++key) {
                const float p0 = probabilities[row0 * score_pitch + key];
                const float p1 = probabilities[row1 * score_pitch + key];
                const float ph0 = __half2float(probabilities_hi[row0 * score_pitch + key]);
                const float ph1 = __half2float(probabilities_hi[row1 * score_pitch + key]);
                const float value = value_tile[key * kv_pitch + col];
                const float value_hi = __half2float(kv_tile[key * kv_pitch + col]);
                correction0 += (p0 - ph0) * value + ph0 * (value - value_hi);
                correction1 += (p1 - ph1) * value + ph1 * (value - value_hi);
            }
            output[row0 * dim + col] += correction0;
            output[row1 * dim + col] += correction1;
        }
        __syncthreads();
    }

    const unsigned int row0 = warp * 2u;
    const unsigned int row1 = row0 + 1u;
    const float inv_denom0 = denom0 > 0.0f ? 1.0f / denom0 : 0.0f;
    const float inv_denom1 = denom1 > 0.0f ? 1.0f / denom1 : 0.0f;
    for (unsigned int col = lane; col < dim; col += 32u) {
        dst[base + (query_start + row0) * dim + col] = output[row0 * dim + col] * inv_denom0;
        dst[base + (query_start + row1) * dim + col] = output[row1 * dim + col] * inv_denom1;
    }
}
// native-program:entry termite_cross_attention_f32
extern "C" __global__ void termite_cross_attention_f32(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const long long* mask,
    unsigned int batch,
    unsigned int dec_seq,
    unsigned int enc_seq,
    unsigned int num_heads,
    unsigned int head_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int hidden = num_heads * head_dim;
    unsigned int total = batch * dec_seq * hidden;
    if (idx >= total) return;

    unsigned int d = idx % head_dim;
    unsigned int tmp = idx / head_dim;
    unsigned int head = tmp % num_heads;
    tmp /= num_heads;
    unsigned int qi = tmp % dec_seq;
    unsigned int b = tmp / dec_seq;
    float scale = rsqrtf((float)head_dim);

    unsigned int q_base = (b * dec_seq + qi) * hidden + head * head_dim;
    float max_score = -3.402823466e+38f;
{{cross_attention_scores}}
        max_score = fmaxf(max_score, score);
    }

    float denom = 0.0f;
    float acc = 0.0f;
{{cross_attention_scores}}
        float e = expf(score - max_score);
        denom += e;
        unsigned int v_idx = (b * enc_seq + ki) * hidden + head * head_dim + d;
        acc += e * v[v_idx];
    }
    dst[idx] = denom > 0.0f ? acc / denom : 0.0f;
}
// native-program:entry termite_cross_attention_q1_f32
extern "C" __global__ void termite_cross_attention_q1_f32(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const long long* mask,
    unsigned int batch,
    unsigned int enc_seq,
    unsigned int num_heads,
    unsigned int head_dim
) {
    extern __shared__ float shared[];
    float* scores = shared;
    float* scratch = shared + enc_seq;

    unsigned int block = blockIdx.x;
    unsigned int head = block % num_heads;
    unsigned int b = block / num_heads;
    if (b >= batch) return;

    unsigned int tid = threadIdx.x;
    unsigned int hidden = num_heads * head_dim;
    unsigned int q_base = b * hidden + head * head_dim;
    float scale = rsqrtf((float)head_dim);

    float local_max = -3.402823466e+38f;
    for (unsigned int ki = tid; ki < enc_seq; ki += blockDim.x) {
        bool valid = mask == nullptr || mask[b * enc_seq + ki] != 0ll;
        float score = -3.402823466e+38f;
        if (valid) {
            unsigned int k_base = (b * enc_seq + ki) * hidden + head * head_dim;
            float dot = 0.0f;
            for (unsigned int j = 0; j < head_dim; ++j) {
                dot += q[q_base + j] * k[k_base + j];
            }
            score = dot * scale;
        }
        scores[ki] = score;
        local_max = fmaxf(local_max, score);
    }
    scratch[tid] = local_max;
    __syncthreads();

    for (unsigned int stride = blockDim.x >> 1; stride > 0; stride >>= 1) {
        if (tid < stride) scratch[tid] = fmaxf(scratch[tid], scratch[tid + stride]);
        __syncthreads();
    }
    float max_score = scratch[0];

    float local_denom = 0.0f;
    for (unsigned int ki = tid; ki < enc_seq; ki += blockDim.x) {
        float score = scores[ki];
        float e = score > -3.0e38f ? expf(score - max_score) : 0.0f;
        scores[ki] = e;
        local_denom += e;
    }
    scratch[tid] = local_denom;
    __syncthreads();

    for (unsigned int stride = blockDim.x >> 1; stride > 0; stride >>= 1) {
        if (tid < stride) scratch[tid] += scratch[tid + stride];
        __syncthreads();
    }
    float denom = scratch[0];

    for (unsigned int d = tid; d < head_dim; d += blockDim.x) {
        float acc = 0.0f;
        for (unsigned int ki = 0; ki < enc_seq; ++ki) {
            float e = scores[ki];
            unsigned int v_idx = (b * enc_seq + ki) * hidden + head * head_dim + d;
            acc += e * v[v_idx];
        }
        unsigned int dst_idx = b * hidden + head * head_dim + d;
        dst[dst_idx] = denom > 0.0f ? acc / denom : 0.0f;
    }
}
// native-program:entry termite_channel_scores_softmax_f32
extern "C" __global__ void termite_channel_scores_softmax_f32(
    float* scores,
    const float* qkv,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int dim,
    unsigned int groups
) {
    unsigned int channels_per_group = dim / groups;
    unsigned int row_id = blockIdx.x;
    unsigned int total_rows = batch * groups * channels_per_group;
    if (row_id >= total_rows || channels_per_group > 256u) return;
    unsigned int qc = row_id % channels_per_group;
    unsigned int tmp = row_id / channels_per_group;
    unsigned int g = tmp % groups;
    unsigned int b = tmp / groups;
    unsigned int tid = threadIdx.x;
    __shared__ float row_scores[256];
    __shared__ float scratch[256];
    float scale = rsqrtf((float)seq_len);
    unsigned int group_offset = g * channels_per_group;

    unsigned int lanes_per_key = 256u / channels_per_group;
    if (lanes_per_key == 0u) lanes_per_key = 1u;
    unsigned int active_threads = lanes_per_key * channels_per_group;
    unsigned int kc = tid % channels_per_group;
    unsigned int lane = tid / channels_per_group;
    float partial = 0.0f;
    if (tid < active_threads) {
        float acc = 0.0f;
        for (unsigned int n = lane; n < seq_len; n += lanes_per_key) {
            unsigned int base = ((b * seq_len + n) * dim * 3) + group_offset;
            acc += qkv[base + qc] * qkv[base + dim + kc];
        }
        partial = acc;
    }
    scratch[tid] = partial;
    __syncthreads();
    for (unsigned int stride = lanes_per_key >> 1u; stride > 0u; stride >>= 1u) {
        if (tid < active_threads && lane < stride) {
            scratch[tid] += scratch[tid + stride * channels_per_group];
        }
        __syncthreads();
    }

    float value = -3.402823466e+38f;
    if (tid < channels_per_group) {
        value = scratch[tid] * scale;
        row_scores[tid] = value;
    }
    scratch[tid] = value;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) scratch[tid] = fmaxf(scratch[tid], scratch[tid + stride]);
        __syncthreads();
    }
    float max_score = scratch[0];
    float exp_value = 0.0f;
    if (tid < channels_per_group) {
        exp_value = expf(row_scores[tid] - max_score);
        row_scores[tid] = exp_value;
    }
    scratch[tid] = exp_value;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) scratch[tid] += scratch[tid + stride];
        __syncthreads();
    }
    float denom = scratch[0];
    if (tid < channels_per_group) {
        unsigned int score_idx = ((b * groups + g) * channels_per_group + qc) * channels_per_group + tid;
        scores[score_idx] = denom > 0.0f ? row_scores[tid] / denom : 0.0f;
    }
}
// native-program:entry termite_channel_apply_f32
extern "C" __global__ void termite_channel_apply_f32(
    float* dst,
    const float* qkv,
    const float* scores,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int dim,
    unsigned int groups
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * seq_len * dim;
    if (idx >= total) return;
    unsigned int c = idx % dim;
    unsigned int tmp = idx / dim;
    unsigned int n = tmp % seq_len;
    unsigned int b = tmp / seq_len;
    unsigned int channels_per_group = dim / groups;
    unsigned int g = c / channels_per_group;
    unsigned int qc = c - g * channels_per_group;
    unsigned int group_offset = g * channels_per_group;
    float acc = 0.0f;
    for (unsigned int vc = 0; vc < channels_per_group; ++vc) {
        unsigned int score_idx = ((b * groups + g) * channels_per_group + qc) * channels_per_group + vc;
        unsigned int v_idx = ((b * seq_len + n) * dim * 3) + 2 * dim + group_offset + vc;
        acc += scores[score_idx] * qkv[v_idx];
    }
    dst[idx] = acc;
}
// native-program:entry termite_gqa_attention_f32
extern "C" __global__ void termite_gqa_attention_f32(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const unsigned char* attn_or_mask,
    const float* bias,
    unsigned int batch,
    unsigned int q_seq_len,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int query_position_offset,
    unsigned int kv_position_offset,
    unsigned int sliding_window,
    unsigned int total_sequence_len,
    unsigned int mask_len,
    unsigned int bias_mode
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int kv_hidden = num_kv_heads * head_dim;
    unsigned int total = batch * q_seq_len * q_hidden;
    if (idx >= total || num_kv_heads == 0u || (num_heads % num_kv_heads) != 0u) return;

    unsigned int d = idx % head_dim;
    unsigned int tmp = idx / head_dim;
    unsigned int head = tmp % num_heads;
    tmp /= num_heads;
    unsigned int qi = tmp % q_seq_len;
    unsigned int b = tmp / q_seq_len;
    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int query_pos = query_position_offset + qi;
    float scale = rsqrtf((float)head_dim);
    float max_score = -3.402823466e+38f;

    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
{{masked_cross_scores}}
        max_score = fmaxf(max_score, score);
    }

    float denom = 0.0f;
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
{{masked_cross_scores}}
        float e = expf(score - max_score);
        denom += e;
        unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + d;
        acc += e * v[v_idx];
    }

    dst[idx] = denom > 0.0f ? acc / denom : 0.0f;
}
// native-program:entry termite_gqa_attention_decode_turboquant_fast_f32
extern "C" __global__ void termite_gqa_attention_decode_turboquant_fast_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }

    unsigned int lane = threadIdx.x;
    unsigned int head = blockIdx.x;
    if (batch != 1u ||
        q_seq_len != 1u ||
        mask_len != 0u ||
        bias_mode != 0u ||
        (format != 0u && format != 2u) ||
        base_key_row_bytes != key_row_bytes ||
        head >= num_heads ||
        head_dim > 512u ||
        (head_dim & 31u) != 0u ||
        blockDim.x < head_dim ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        key_row_bytes == 0u ||
        value_row_bytes == 0u) return;

    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    unsigned int query_pos = query_position_offset;
{{visible_key_window}}
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_base = head * head_dim;
    termite_tq_fast_online_impl(dst, q, k, v, block_table, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes, block_count, page_size_tokens, format, value_format, physical_token_capacity, decode_scalars, lane, q_base, kv_head, key_start, key_end);
}
// native-program:entry termite_gqa_attention_prefill_turboquant_fast_f32
extern "C" __global__ void termite_gqa_attention_prefill_turboquant_fast_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
    (void)decode_scalars;

    unsigned int lane = threadIdx.x;
    unsigned int block = blockIdx.x;
    unsigned int total_blocks = batch * q_seq_len * num_heads;
    if (batch != 1u ||
        q_seq_len <= 1u ||
        block >= total_blocks ||
        mask_len != 0u ||
        bias_mode != 0u ||
        (format != 0u && format != 2u) ||
        base_key_row_bytes != key_row_bytes ||
        head_dim > 512u ||
        (head_dim & 31u) != 0u ||
        blockDim.x < head_dim ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        key_row_bytes == 0u ||
        value_row_bytes == 0u) return;

    unsigned int head = block % num_heads;
    unsigned int tmp = block / num_heads;
    unsigned int qi = tmp % q_seq_len;
    unsigned int b = tmp / q_seq_len;
    unsigned int query_pos = query_position_offset + qi;

    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
{{visible_key_window}}
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    termite_tq_fast_online_impl(dst, q, k, v, block_table, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes, block_count, page_size_tokens, format, value_format, physical_token_capacity, decode_scalars, lane, q_base, kv_head, key_start, key_end);
}
// native-program:entry termite_gqa_attention_prefill_tiled_f16_exact_f32
extern "C" __global__ void termite_gqa_attention_prefill_tiled_f16_exact_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;

    const unsigned int tile_m = 16u;
    const float neg_inf = -3.402823466e+38f;
    __shared__ float warp_sums[16];
    __shared__ unsigned int key_start[16];
    __shared__ unsigned int key_end[16];
    __shared__ unsigned int row_valid[16];
    __shared__ unsigned int block_key_start;
    __shared__ unsigned int block_key_end;
    __shared__ float max_score[16];
    __shared__ float denom[16];
    __shared__ float alpha[16];
    __shared__ float beta[16];

    const unsigned int expected_row_bytes = head_dim * 2u;
    if (batch != 1u ||
        q_seq_len <= 1u ||
        num_heads != 8u ||
        num_kv_heads != 1u ||
        (head_dim != 256u && head_dim != 512u) ||
        blockDim.x != head_dim ||
        mask_len != 0u ||
        bias_mode != 0u ||
        format != 2u ||
        value_format != 2u ||
        key_row_bytes != expected_row_bytes ||
        base_key_row_bytes != expected_row_bytes ||
        value_row_bytes != expected_row_bytes ||
        !termite_tq_f16_prefill_page_layout_valid(
            kv_seq_len,
            block_table,
            block_count,
            page_size_tokens,
            physical_token_capacity
        ) ||
        decode_scalars != 0) return;

    const unsigned int head = blockIdx.x;
    const unsigned int tile_row_start = blockIdx.y * tile_m;
    if (head >= num_heads || tile_row_start >= q_seq_len) return;

    const unsigned int tid = threadIdx.x;
    if (tid < tile_m) {
        const unsigned int qi = tile_row_start + tid;
        const bool valid_row = qi < q_seq_len;
        row_valid[tid] = valid_row ? 1u : 0u;
        unsigned int start = 0u;
        unsigned int end = 0u;
        if (valid_row) {
            const unsigned int query_pos = query_position_offset + qi;
            if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
                const unsigned int visible = query_pos - kv_position_offset + 1u;
                end = visible < kv_seq_len ? visible : kv_seq_len;
                if (sliding_window != 0u) {
                    const unsigned int window_start_abs = query_pos + 1u > sliding_window
                        ? query_pos + 1u - sliding_window
                        : 0u;
                    if (window_start_abs > kv_position_offset) {
                        start = window_start_abs - kv_position_offset;
                        if (start > end) start = end;
                    }
                }
            }
        }
        key_start[tid] = start;
        key_end[tid] = end;
        max_score[tid] = neg_inf;
        denom[tid] = 0.0f;
    }
    __syncthreads();

    if (tid == 0u) {
        unsigned int valid_rows = q_seq_len - tile_row_start;
        if (valid_rows > tile_m) valid_rows = tile_m;
        block_key_start = key_start[0];
        block_key_end = key_end[valid_rows - 1u];
    }
    __syncthreads();

    const unsigned int q_hidden = num_heads * head_dim;
    const float scale = rsqrtf((float)head_dim);
    float output_acc[16];
#pragma unroll
    for (unsigned int row = 0u; row < tile_m; ++row) output_acc[row] = 0.0f;

    for (unsigned int ki = block_key_start; ki < block_key_end; ++ki) {
        const unsigned int physical_token = termite_tq_physical_token(
            ki,
            block_table,
            block_count,
            page_size_tokens,
            physical_token_capacity
        );
        const bool physical_valid = physical_token != 0xffffffffu;
        const half* k_row = physical_valid
            ? reinterpret_cast<const half*>(k + (size_t)physical_token * key_row_bytes)
            : 0;
        const half* v_row = physical_valid
            ? reinterpret_cast<const half*>(v + (size_t)physical_token * value_row_bytes)
            : 0;
        const float key_value = physical_valid ? __half2float(k_row[tid]) : 0.0f;
        const float value = physical_valid ? __half2float(v_row[tid]) : 0.0f;

#pragma unroll
        for (unsigned int row = 0u; row < tile_m; ++row) {
            const bool visible = physical_valid && row_valid[row] != 0u &&
                ki >= key_start[row] && ki < key_end[row];
            const unsigned int qi = tile_row_start + row;
            const unsigned int q_base = qi * q_hidden + head * head_dim;
            const float partial = visible ? q[q_base + tid] * key_value : 0.0f;
            const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
            if (tid == 0u) {
                if (visible) {
                    const float score = dot * scale;
                    const float next_max = fmaxf(max_score[row], score);
                    alpha[row] = max_score[row] > neg_inf ? expf(max_score[row] - next_max) : 0.0f;
                    beta[row] = expf(score - next_max);
                    denom[row] = denom[row] * alpha[row] + beta[row];
                    max_score[row] = next_max;
                } else {
                    alpha[row] = 1.0f;
                    beta[row] = 0.0f;
                }
            }
            __syncthreads();
            // Keep the fast baseline's two-step F32 recurrence exactly and
            // skip union-range keys that this query cannot see. In
            // particular, do not introduce multiply-by-one/add-zero updates
            // for earlier rows in the tile.
            if (visible) {
                output_acc[row] *= alpha[row];
                output_acc[row] += beta[row] * value;
            }
            // Match the required-fast recurrence boundary. The block
            // reduction has no post-consume barrier of its own, so every warp
            // must finish this row before any warp-sum slot can be reused by
            // the next query reduction.
            __syncthreads();
        }
    }

#pragma unroll
    for (unsigned int row = 0u; row < tile_m; ++row) {
        if (row_valid[row] != 0u) {
            const unsigned int qi = tile_row_start + row;
            const unsigned int out_idx = qi * q_hidden + head * head_dim + tid;
            dst[out_idx] = denom[row] > 0.0f ? output_acc[row] / denom[row] : 0.0f;
        }
    }
}
// native-program:entry termite_gqa_attention_prefill_tiled_f16_warp_f32
extern "C" __global__ void termite_gqa_attention_prefill_tiled_f16_warp_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;

    const unsigned int tile_m = 16u;
    const float neg_inf = -3.402823466e+38f;
    const unsigned int expected_row_bytes = head_dim * 2u;
    if (batch != 1u ||
        q_seq_len <= 1u ||
        num_heads != 8u ||
        num_kv_heads != 1u ||
        (head_dim != 256u && head_dim != 512u) ||
        blockDim.x != 256u ||
        mask_len != 0u ||
        bias_mode != 0u ||
        format != 2u ||
        value_format != 2u ||
        key_row_bytes != expected_row_bytes ||
        base_key_row_bytes != expected_row_bytes ||
        value_row_bytes != expected_row_bytes ||
        !termite_tq_f16_prefill_page_layout_valid(
            kv_seq_len,
            block_table,
            block_count,
            page_size_tokens,
            physical_token_capacity
        ) ||
        decode_scalars != 0) return;

    const unsigned int head = blockIdx.x;
    const unsigned int tile_row_start = blockIdx.y * tile_m;
    if (head >= num_heads || tile_row_start >= q_seq_len) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int warp = tid >> 5;
    const unsigned int lane = tid & 31u;
    const unsigned int qi0 = tile_row_start + warp * 2u;
    const unsigned int qi1 = qi0 + 1u;
    const bool row_valid0 = qi0 < q_seq_len;
    const bool row_valid1 = qi1 < q_seq_len;
    if (!row_valid0) return;

    unsigned int key_start0 = 0u, key_end0 = 0u;
    unsigned int key_start1 = 0u, key_end1 = 0u;
    const unsigned int query_pos0 = query_position_offset + qi0;
    if (kv_seq_len != 0u && query_pos0 >= kv_position_offset) {
        const unsigned int visible = query_pos0 - kv_position_offset + 1u;
        key_end0 = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = query_pos0 + 1u > sliding_window
                ? query_pos0 + 1u - sliding_window
                : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start0 = window_start_abs - kv_position_offset;
                if (key_start0 > key_end0) key_start0 = key_end0;
            }
        }
    }
    if (row_valid1) {
        const unsigned int query_pos1 = query_position_offset + qi1;
        if (kv_seq_len != 0u && query_pos1 >= kv_position_offset) {
            const unsigned int visible = query_pos1 - kv_position_offset + 1u;
            key_end1 = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u) {
                const unsigned int window_start_abs = query_pos1 + 1u > sliding_window
                    ? query_pos1 + 1u - sliding_window
                    : 0u;
                if (window_start_abs > kv_position_offset) {
                    key_start1 = window_start_abs - kv_position_offset;
                    if (key_start1 > key_end1) key_start1 = key_end1;
                }
            }
        }
    }

    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int q_base0 = qi0 * q_hidden + head * head_dim;
    const unsigned int q_base1 = row_valid1 ? qi1 * q_hidden + head * head_dim : q_base0;
    const float scale = rsqrtf((float)head_dim);
    float max0 = neg_inf, max1 = neg_inf;
    float denom0 = 0.0f, denom1 = 0.0f;
    float output0[16];
    float output1[16];
#pragma unroll
    for (unsigned int e = 0u; e < 16u; ++e) {
        output0[e] = 0.0f;
        output1[e] = 0.0f;
    }

    const unsigned int block_key_start = key_start0;
    const unsigned int block_key_end = row_valid1 ? key_end1 : key_end0;
    for (unsigned int n0 = block_key_start; n0 < block_key_end; n0 += 32u) {
        const unsigned int ki = n0 + lane;
        const bool in_tile = ki < block_key_end;
        const unsigned int physical_token = in_tile
            ? termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity)
            : 0xffffffffu;
        const bool physical_valid = physical_token != 0xffffffffu;
        const half* k_row = physical_valid
            ? reinterpret_cast<const half*>(k + (size_t)physical_token * key_row_bytes)
            : 0;
        float dot0 = 0.0f;
        float dot1 = 0.0f;
        if (physical_valid) {
            for (unsigned int d = 0u; d < head_dim; ++d) {
                const float key_value = __half2float(k_row[d]);
                dot0 += q[q_base0 + d] * key_value;
                if (row_valid1) dot1 += q[q_base1 + d] * key_value;
            }
        }
        const bool score_valid0 = physical_valid && ki >= key_start0 && ki < key_end0;
        const bool score_valid1 = row_valid1 && physical_valid && ki >= key_start1 && ki < key_end1;
        const float lane_score0 = score_valid0 ? dot0 * scale : neg_inf;
        const float lane_score1 = score_valid1 ? dot1 * scale : neg_inf;
        const unsigned int lane_valid0 = score_valid0 ? 1u : 0u;
        const unsigned int lane_valid1 = score_valid1 ? 1u : 0u;

#pragma unroll
        for (unsigned int key_lane = 0u; key_lane < 32u; ++key_lane) {
            const unsigned int replay_physical = __shfl_sync(0xffffffffu, physical_token, key_lane);
            const bool replay_valid0 = __shfl_sync(0xffffffffu, lane_valid0, key_lane) != 0u;
            const bool replay_valid1 = __shfl_sync(0xffffffffu, lane_valid1, key_lane) != 0u;
            const float score0 = __shfl_sync(0xffffffffu, lane_score0, key_lane);
            const float score1 = __shfl_sync(0xffffffffu, lane_score1, key_lane);

            float alpha0 = 1.0f, beta0 = 0.0f;
            float alpha1 = 1.0f, beta1 = 0.0f;
            if (replay_valid0) {
                const float next_max = fmaxf(max0, score0);
                alpha0 = max0 > neg_inf ? expf(max0 - next_max) : 0.0f;
                beta0 = expf(score0 - next_max);
                denom0 = denom0 * alpha0 + beta0;
                max0 = next_max;
            }
            if (replay_valid1) {
                const float next_max = fmaxf(max1, score1);
                alpha1 = max1 > neg_inf ? expf(max1 - next_max) : 0.0f;
                beta1 = expf(score1 - next_max);
                denom1 = denom1 * alpha1 + beta1;
                max1 = next_max;
            }

            const half* v_row = replay_physical != 0xffffffffu
                ? reinterpret_cast<const half*>(v + (size_t)replay_physical * value_row_bytes)
                : 0;
#pragma unroll
            for (unsigned int e = 0u; e < 16u; ++e) {
                const unsigned int d = lane + e * 32u;
                if (d < head_dim) {
                    const float value = v_row != 0 ? __half2float(v_row[d]) : 0.0f;
                    output0[e] = output0[e] * alpha0 + beta0 * value;
                    output1[e] = output1[e] * alpha1 + beta1 * value;
                }
            }
        }
    }

#pragma unroll
    for (unsigned int e = 0u; e < 16u; ++e) {
        const unsigned int d = lane + e * 32u;
        if (d < head_dim) {
            dst[q_base0 + d] = denom0 > 0.0f ? output0[e] / denom0 : 0.0f;
            if (row_valid1) dst[q_base1 + d] = denom1 > 0.0f ? output1[e] / denom1 : 0.0f;
        }
    }
}
// native-program:entry termite_gqa_attention_prefill_turboquant_tiled_f32
extern "C" __global__ void termite_gqa_attention_prefill_turboquant_tiled_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
    (void)decode_scalars;

    const float neg_inf = -3.402823466e+38f;
    extern __shared__ unsigned short kv_tile[];
    __shared__ float p_tile[TERMITE_TQ_PREFILL_TILE_M][32];
    __shared__ unsigned int tile_phys[32];

    if (batch != 1u ||
        q_seq_len <= 1u ||
        mask_len != 0u ||
        bias_mode != 0u ||
        (format != 0u && format != 2u) ||
        base_key_row_bytes != key_row_bytes ||
        head_dim > 512u ||
        (head_dim & 31u) != 0u ||
        blockDim.x != 256u ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        key_row_bytes == 0u ||
        value_row_bytes == 0u) return;

    unsigned int tile_n = head_dim <= 256u ? 32u : 16u;
    unsigned int row_pitch = head_dim + 2u;

    unsigned int head = blockIdx.x;
    unsigned int tile_row_start = blockIdx.y * TERMITE_TQ_PREFILL_TILE_M;
    if (head >= num_heads || tile_row_start >= q_seq_len) return;

    unsigned int tid = threadIdx.x;
    unsigned int warp = tid >> 5;
    unsigned int lane = tid & 31u;
    unsigned int hd_chunks = head_dim >> 5;

    // Each warp owns two adjacent query rows.
    unsigned int qi0 = tile_row_start + warp * 2u;
    unsigned int qi1 = qi0 + 1u;
    bool row_valid0 = qi0 < q_seq_len;
    bool row_valid1 = qi1 < q_seq_len;
    unsigned int qi0_clamped = row_valid0 ? qi0 : (q_seq_len - 1u);
    unsigned int qi1_clamped = row_valid1 ? qi1 : (q_seq_len - 1u);

    unsigned int key_start0 = 0u, key_end0 = 0u;
    unsigned int key_start1 = 0u, key_end1 = 0u;
    {
        unsigned int query_pos = query_position_offset + qi0_clamped;
        if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
            unsigned int visible = query_pos - kv_position_offset + 1u;
            key_end0 = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u) {
                unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    key_start0 = window_start_abs - kv_position_offset;
                    if (key_start0 > key_end0) key_start0 = key_end0;
                }
            }
        }
        query_pos = query_position_offset + qi1_clamped;
        if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
            unsigned int visible = query_pos - kv_position_offset + 1u;
            key_end1 = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u) {
                unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    key_start1 = window_start_abs - kv_position_offset;
                    if (key_start1 > key_end1) key_start1 = key_end1;
                }
            }
        }
    }
    if (!row_valid0) { key_start0 = 0u; key_end0 = 0u; }
    if (!row_valid1) { key_start1 = 0u; key_end1 = 0u; }

    // Block-wide key range: causal end grows with qi and the sliding-window
    // start grows with qi, so the union is [start(first row), end(last row)].
    unsigned int qi_last = tile_row_start + TERMITE_TQ_PREFILL_TILE_M - 1u;
    if (qi_last >= q_seq_len) qi_last = q_seq_len - 1u;
    unsigned int block_key_start = 0u;
    unsigned int block_key_end = 0u;
    {
        unsigned int first_pos = query_position_offset + tile_row_start;
        unsigned int last_pos = query_position_offset + qi_last;
        if (kv_seq_len != 0u && last_pos >= kv_position_offset) {
            unsigned int visible = last_pos - kv_position_offset + 1u;
            block_key_end = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u && first_pos >= kv_position_offset) {
                unsigned int window_start_abs = (first_pos + 1u > sliding_window) ? (first_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    block_key_start = window_start_abs - kv_position_offset;
                    if (block_key_start > block_key_end) block_key_start = block_key_end;
                }
            }
        }
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    float scale = rsqrtf((float)head_dim);

    const float* q_row0 = q + qi0_clamped * q_hidden + head * head_dim;
    const float* q_row1 = q + qi1_clamped * q_hidden + head * head_dim;

    float acc0[16];
    float acc1[16];
    for (unsigned int e = 0u; e < hd_chunks; ++e) { acc0[e] = 0.0f; acc1[e] = 0.0f; }
    float m_run0 = neg_inf, d_run0 = 0.0f;
    float m_run1 = neg_inf, d_run1 = 0.0f;

    for (unsigned int n0 = block_key_start; n0 < block_key_end; n0 += tile_n) {
        unsigned int n_count = block_key_end - n0;
        if (n_count > tile_n) n_count = tile_n;

        if (tid < tile_n) {
            tile_phys[tid] = tid < n_count
                ? termite_tq_physical_token(n0 + tid, block_table, block_count, page_size_tokens, physical_token_capacity)
                : 0xffffffffu;
        }
        __syncthreads();

        for (unsigned int idx = tid; idx < n_count * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            unsigned int col = idx - row * head_dim;
            unsigned int phys = tile_phys[row];
            float key_value = 0.0f;
            if (phys != 0xffffffffu) {
                const unsigned char* k_row = k + (size_t)phys * key_row_bytes;
                unsigned int value_index = kv_head * head_dim + col;
                key_value = format == 0u
                    ? termite_tq_decode_polar4_at(k_row, value_index)
                    : termite_tq_f16_value(k_row, value_index);
            }
            kv_tile[row * row_pitch + col] = termite_f32_to_bf16(key_value);
        }
        __syncthreads();

        // Each lane owns one key of the tile and computes its dot against
        // both of the warp's query rows: every K element read from shared
        // memory feeds two FMAs. q_row reads hit the same address warp-wide
        // (L1 broadcast). When the tile holds only 16 keys (head_dim > 256)
        // lane pairs split the dims of one key and combine via shfl_xor so
        // no lanes idle. Scores are masked to the owning lane afterward.
        unsigned int key_lane = tile_n < 32u ? (lane & (tile_n - 1u)) : lane;
        unsigned int d_begin = (tile_n < 32u && lane >= tile_n) ? (head_dim >> 1) : 0u;
        unsigned int d_count = tile_n < 32u ? (head_dim >> 1) : head_dim;
        unsigned int ki = n0 + key_lane;
        bool in_tile = key_lane < n_count && tile_phys[key_lane] != 0xffffffffu;
        float dot0 = 0.0f;
        float dot1 = 0.0f;
        const unsigned short* k_vec = &kv_tile[key_lane * row_pitch] + d_begin;
        const float* q_ptr0 = q_row0 + d_begin;
        const float* q_ptr1 = q_row1 + d_begin;
        #pragma unroll 4
        for (unsigned int d = 0u; d < d_count; ++d) {
            float kd = termite_bf16_bits_to_f32(k_vec[d]);
            dot0 += q_ptr0[d] * kd;
            dot1 += q_ptr1[d] * kd;
        }
        if (tile_n < 32u) {
            dot0 += __shfl_xor_sync(0xffffffffu, dot0, 16u);
            dot1 += __shfl_xor_sync(0xffffffffu, dot1, 16u);
        }
        bool own_key = in_tile && lane < tile_n;
        bool key_valid0 = own_key && ki >= key_start0 && ki < key_end0;
        bool key_valid1 = own_key && ki >= key_start1 && ki < key_end1;
        float score0 = key_valid0 ? dot0 * scale : neg_inf;
        float score1 = key_valid1 ? dot1 * scale : neg_inf;

        float tile_max0 = score0;
        float tile_max1 = score1;
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
            tile_max0 = fmaxf(tile_max0, __shfl_down_sync(0xffffffffu, tile_max0, offset));
            tile_max1 = fmaxf(tile_max1, __shfl_down_sync(0xffffffffu, tile_max1, offset));
        }
        tile_max0 = __shfl_sync(0xffffffffu, tile_max0, 0u);
        tile_max1 = __shfl_sync(0xffffffffu, tile_max1, 0u);

        float alpha0 = 1.0f, p0 = 0.0f;
        float alpha1 = 1.0f, p1 = 0.0f;
        if (tile_max0 > neg_inf) {
            float new_max = fmaxf(m_run0, tile_max0);
            alpha0 = m_run0 > neg_inf ? expf(m_run0 - new_max) : 0.0f;
            p0 = key_valid0 ? expf(score0 - new_max) : 0.0f;
            m_run0 = new_max;
        }
        if (tile_max1 > neg_inf) {
            float new_max = fmaxf(m_run1, tile_max1);
            alpha1 = m_run1 > neg_inf ? expf(m_run1 - new_max) : 0.0f;
            p1 = key_valid1 ? expf(score1 - new_max) : 0.0f;
            m_run1 = new_max;
        }
        float sum_p0 = p0;
        float sum_p1 = p1;
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
            sum_p0 += __shfl_down_sync(0xffffffffu, sum_p0, offset);
            sum_p1 += __shfl_down_sync(0xffffffffu, sum_p1, offset);
        }
        sum_p0 = __shfl_sync(0xffffffffu, sum_p0, 0u);
        sum_p1 = __shfl_sync(0xffffffffu, sum_p1, 0u);
        if (tile_max0 > neg_inf) d_run0 = d_run0 * alpha0 + sum_p0;
        if (tile_max1 > neg_inf) d_run1 = d_run1 * alpha1 + sum_p1;
        p_tile[warp * 2u][lane] = p0;
        p_tile[warp * 2u + 1u][lane] = p1;
        __syncthreads();

        for (unsigned int idx = tid; idx < n_count * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            unsigned int col = idx - row * head_dim;
            unsigned int phys = tile_phys[row];
            float value = 0.0f;
            if (phys != 0xffffffffu) {
                const unsigned char* v_row = v + (size_t)phys * value_row_bytes;
                value = termite_tq_value_at(v_row, kv_head, col, head_dim, value_format);
            }
            kv_tile[row * row_pitch + col] = termite_f32_to_bf16(value);
        }
        __syncthreads();

        for (unsigned int e = 0u; e < hd_chunks; ++e) { acc0[e] *= alpha0; acc1[e] *= alpha1; }
        for (unsigned int j = 0u; j < n_count; ++j) {
            float pj0 = p_tile[warp * 2u][j];
            float pj1 = p_tile[warp * 2u + 1u][j];
            if (pj0 != 0.0f || pj1 != 0.0f) {
                const unsigned short* v_vec = &kv_tile[j * row_pitch];
                for (unsigned int e = 0u; e < hd_chunks; ++e) {
                    float ve = termite_bf16_bits_to_f32(v_vec[lane + (e << 5)]);
                    acc0[e] += pj0 * ve;
                    acc1[e] += pj1 * ve;
                }
            }
        }
        __syncthreads();
    }

    if (row_valid0) {
        unsigned int out_base = qi0 * q_hidden + head * head_dim;
        float inv_denom = d_run0 > 0.0f ? 1.0f / d_run0 : 0.0f;
        for (unsigned int e = 0u; e < hd_chunks; ++e) {
            dst[out_base + lane + (e << 5)] = acc0[e] * inv_denom;
        }
    }
    if (row_valid1) {
        unsigned int out_base = qi1 * q_hidden + head * head_dim;
        float inv_denom = d_run1 > 0.0f ? 1.0f / d_run1 : 0.0f;
        for (unsigned int e = 0u; e < hd_chunks; ++e) {
            dst[out_base + lane + (e << 5)] = acc1[e] * inv_denom;
        }
    }
}
// native-program:entry termite_gqa_attention_prefill_turboquant_mma_f32
extern "C" __global__ void termite_gqa_attention_prefill_turboquant_mma_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
#define TERMITE_TQ_MMA_PREFILL_ROWS 16u
#include "../kernels/tq_mma_prefill.cuh"
}
// native-program:entry termite_gqa_attention_prefill_turboquant_mma_m32_f32
extern "C" __global__ void termite_gqa_attention_prefill_turboquant_mma_m32_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
#define TERMITE_TQ_MMA_PREFILL_ROWS 32u
#include "../kernels/tq_mma_prefill.cuh"
}
// native-program:entry termite_gqa_attention_decode_turboquant_split_stage1_f32
extern "C" __global__ void termite_gqa_attention_decode_turboquant_split_stage1_f32(ANTFLY_CUDA_TQ_SPLIT_STAGE1_PARAMS) {
    termite_tq_split_stage1_impl<false >(partial_acc, partial_meta, q, k, v, block_table, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, key_row_bytes, value_row_bytes, block_count, page_size_tokens, format, value_format, physical_token_capacity, decode_scalars, chunk_size, chunk_count);
}
// native-program:entry termite_gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32
extern "C" __global__ void termite_gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32(ANTFLY_CUDA_TQ_SPLIT_STAGE1_PARAMS) {
    termite_tq_split_stage1_impl<true >(partial_acc, partial_meta, q, k, v, block_table, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, key_row_bytes, value_row_bytes, block_count, page_size_tokens, format, value_format, physical_token_capacity, decode_scalars, chunk_size, chunk_count);
}
// native-program:entry termite_gqa_attention_decode_turboquant_split_stage2_f32
extern "C" __global__ void termite_gqa_attention_decode_turboquant_split_stage2_f32(
    float* dst,
    const float* partial_acc,
    const float* partial_meta,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    const unsigned int* decode_scalars,
    unsigned int chunk_size,
    unsigned int chunk_count
) {
    if (decode_scalars != 0) {
        kv_seq_len = decode_scalars[2];
    }
    unsigned int active_chunk_count = chunk_count;
    if (chunk_size != 0u) {
        active_chunk_count = (kv_seq_len + chunk_size - 1u) / chunk_size;
        if (active_chunk_count > chunk_count) active_chunk_count = chunk_count;
    }

    const float neg_inf = -3.402823466e+38f;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    unsigned int lane = threadIdx.x;
    unsigned int head = blockIdx.x;
    if (head >= num_heads || head_dim > 512u || blockDim.x < head_dim || chunk_count == 0u || chunk_count > 128u) return;

    if (lane == 0u) {
        float max_score = neg_inf;
        for (unsigned int chunk = 0u; chunk < active_chunk_count; ++chunk) {
            float score = partial_meta[(head * chunk_count + chunk) * 2u];
            max_score = fmaxf(max_score, score);
        }
        float denom = 0.0f;
        for (unsigned int chunk = 0u; chunk < active_chunk_count; ++chunk) {
            unsigned int partial_index = head * chunk_count + chunk;
            float score = partial_meta[partial_index * 2u];
            float local_denom = partial_meta[partial_index * 2u + 1u];
            if (local_denom > 0.0f && score > neg_inf * 0.5f) denom += local_denom * expf(score - max_score);
        }
        shared_max_score = max_score;
        shared_denom = denom;
    }
    __syncthreads();

    if (lane < head_dim) {
        float acc = 0.0f;
        for (unsigned int chunk = 0u; chunk < active_chunk_count; ++chunk) {
            unsigned int partial_index = head * chunk_count + chunk;
            float score = partial_meta[partial_index * 2u];
            float local_denom = partial_meta[partial_index * 2u + 1u];
            if (local_denom > 0.0f && score > neg_inf * 0.5f) {
                float scale = expf(score - shared_max_score);
                acc += partial_acc[partial_index * head_dim + lane] * scale;
            }
        }
        dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}
// native-program:entry termite_gqa_attention_decode_turboquant_f32
extern "C" __global__ void termite_gqa_attention_decode_turboquant_f32(ANTFLY_CUDA_PAGED_ATTENTION_PARAMS) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }

    const unsigned int score_cache_limit = 4096u;
    const float neg_inf = -3.402823466e+38f;
    __shared__ float reduce[512];
    __shared__ float shared_scores[4096];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_score;
    __shared__ float shared_turbo3_projected_query[32];
    unsigned int lane = threadIdx.x;
    unsigned int block = blockIdx.x;
    unsigned int total_blocks = batch * q_seq_len * num_heads;
    if (block >= total_blocks || batch != 1u || head_dim > 512u || blockDim.x < head_dim || num_kv_heads == 0u || (num_heads % num_kv_heads) != 0u) return;
    if (key_row_bytes == 0u || base_key_row_bytes == 0u || value_row_bytes == 0u || base_key_row_bytes > key_row_bytes) return;

    unsigned int head = block % num_heads;
    unsigned int tmp = block / num_heads;
    unsigned int qi = tmp % q_seq_len;
    unsigned int b = tmp / q_seq_len;
    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int query_pos = query_position_offset + qi;
    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    bool cache_scores = kv_seq_len <= score_cache_limit;
    float scale = rsqrtf((float)head_dim);
    if (format == 1u && lane < 32u) {
        float projected_query = 0.0f;
        for (unsigned int d = 0u; d < head_dim; ++d) {
            projected_query += termite_tq_random_sign(kv_head, lane, d) * q[q_base + d];
        }
        shared_turbo3_projected_query[lane] = projected_query;
    }
    if (lane == 0u) shared_max_score = neg_inf;
    __syncthreads();

    for (unsigned int ki = 0u; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        if (physical_token == 0xffffffffu) valid = false;
        const unsigned char* k_row = valid ? k + physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int value_index = kv_head * head_dim + lane;
            float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : (format == 1u
                    ? termite_tq_decode_turbo3_at(k_row, value_index)
                    : termite_tq_f16_value(k_row, value_index));
            partial = q[q_base + lane] * key_value;
        }
        reduce[lane] = partial;
        __syncthreads();
        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
            if (lane < stride) reduce[lane] += reduce[lane + stride];
            __syncthreads();
        }
        if (lane == 0u) {
            float score = neg_inf;
            if (valid) {
                score = reduce[0];
                if (format == 1u) {
                    score += 0.125f * termite_tq_turbo3_projected_residual_score(shared_turbo3_projected_query, k_row + base_key_row_bytes, kv_head);
                }
                score *= scale;
                if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
                if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
                shared_max_score = fmaxf(shared_max_score, score);
            }
            if (cache_scores) shared_scores[ki] = score;
        }
        __syncthreads();
    }

    if (lane == 0u) shared_denom = 0.0f;
    __syncthreads();
    float acc = 0.0f;
    for (unsigned int ki = 0u; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        if (physical_token == 0xffffffffu) valid = false;
        const unsigned char* k_row = valid ? k + physical_token * key_row_bytes : k;
        if (cache_scores) {
            if (lane == 0u) shared_score = shared_scores[ki];
            __syncthreads();
        } else {
            float partial = 0.0f;
            if (valid && lane < head_dim) {
                unsigned int value_index = kv_head * head_dim + lane;
                float key_value = format == 0u
                    ? termite_tq_decode_polar4_at(k_row, value_index)
                    : (format == 1u
                        ? termite_tq_decode_turbo3_at(k_row, value_index)
                        : termite_tq_f16_value(k_row, value_index));
                partial = q[q_base + lane] * key_value;
            }
            reduce[lane] = partial;
            __syncthreads();
            for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
                if (lane < stride) reduce[lane] += reduce[lane + stride];
                __syncthreads();
            }
            if (lane == 0u) {
                if (valid) {
                    float score = reduce[0];
                    if (format == 1u) {
                        score += 0.125f * termite_tq_turbo3_projected_residual_score(shared_turbo3_projected_query, k_row + base_key_row_bytes, kv_head);
                    }
                    score *= scale;
                    if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
                    if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
                    shared_score = score;
                } else {
                    shared_score = neg_inf;
                }
            }
            __syncthreads();
        }
        float e = (valid && shared_score > neg_inf * 0.5f) ? expf(shared_score - shared_max_score) : 0.0f;
        if (lane == 0u) shared_denom += e;
        if (valid && lane < head_dim) {
            const unsigned char* v_row = v + physical_token * value_row_bytes;
            float value = termite_tq_value_at(v_row, kv_head, lane, head_dim, value_format);
            acc += e * value;
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}
// native-program:entry termite_deberta_attention_f32
extern "C" __global__ void termite_deberta_attention_f32(ANTFLY_CUDA_DEBERTA_ATTENTION_F32_PARAMS) {
    unsigned int row = blockIdx.x;
    unsigned int qi = row % seq_len;
    unsigned int tmp = row / seq_len;
    unsigned int head = tmp % num_heads;
    unsigned int b = tmp / num_heads;
    if (b >= batch) return;

    extern __shared__ float scratch[];
    float* scores = scratch;
    float* reduction = scores + seq_len;
    unsigned int tid = threadIdx.x;

    float local_max = -3.402823466e+38f;
    for (unsigned int ki = tid; ki < seq_len; ki += blockDim.x) {
        if (termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) {
            float score = termite_deberta_attention_score_f32(q, k, q_r, k_r, b, qi, ki, head, seq_len, num_heads, head_dim);
            scores[ki] = score;
            local_max = fmaxf(local_max, score);
        } else {
            scores[ki] = -3.402823466e+38f;
        }
    }
    reduction[tid] = local_max;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) reduction[tid] = fmaxf(reduction[tid], reduction[tid + stride]);
        __syncthreads();
    }
    float row_max = reduction[0];
    // Every warp must consume reduction[0] before thread 0 can reuse that
    // shared-memory location for the denominator reduction below. Without
    // this barrier, long attention rows race after the first reduction and
    // produce nondeterministic forward activations.
    __syncthreads();

    float local_sum = 0.0f;
    for (unsigned int ki = tid; ki < seq_len; ki += blockDim.x) {
        if (termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) {
            float e = expf(scores[ki] - row_max);
            scores[ki] = e;
            local_sum += e;
        } else {
            scores[ki] = 0.0f;
        }
    }
    reduction[tid] = local_sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) reduction[tid] += reduction[tid + stride];
        __syncthreads();
    }
    float denom = reduction[0];
    __syncthreads();

    unsigned int hidden = num_heads * head_dim;
    unsigned int head_off = head * head_dim;
    for (unsigned int d = tid; d < head_dim; d += blockDim.x) {
        float acc = 0.0f;
        for (unsigned int ki = 0; ki < seq_len; ++ki) {
            acc += scores[ki] * v[(b * seq_len + ki) * hidden + head_off + d];
        }
        dst[(b * seq_len + qi) * hidden + head_off + d] = denom > 0.0f ? acc / denom : 0.0f;
    }
}
// native-program:entry termite_deberta_attention_fused_f32
extern "C" __global__ void termite_deberta_attention_fused_f32(ANTFLY_CUDA_DEBERTA_ATTENTION_PARAMS) {
    constexpr unsigned int kThreads = 256u;
    constexpr unsigned int kWarps = kThreads / 32u;
    constexpr unsigned int kMaxSeq = 512u;
    if (seq_len == 0u || seq_len > kMaxSeq || blockDim.x != kThreads) return;

    const unsigned int block = blockIdx.x;
    const unsigned int qi = block % seq_len;
    const unsigned int bh = block / seq_len;
    const unsigned int head = bh % num_heads;
    const unsigned int b = bh / num_heads;
    if (b >= batch) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int hidden = num_heads * head_dim;
    const unsigned int head_off = head * head_dim;
    const unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
    const float scale = rsqrtf((float)head_dim * 3.0f);

    __shared__ float probabilities[kMaxSeq];
    __shared__ float reductions[kThreads];

    // Eight warps form eight key scores at a time.  A warp spans head_dim in
    // 32-wide stripes and uses shuffle reduction, avoiding redundant score
    // computation for every V channel.
    for (unsigned int key_base = 0u; key_base < seq_len; key_base += kWarps) {
        const unsigned int ki = key_base + warp;
        if (ki < seq_len) {
            float partial = 0.0f;
            if (mask[b * seq_len + ki] != 0ll) {
                const unsigned int rel_idx = qi + seq_len - 1u - ki;
                const unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
                const unsigned int rel_base = rel_idx * hidden + head_off;
                for (unsigned int j = lane; j < head_dim; j += 32u) {
                    const float q_value = q[q_base + j];
                    const float k_value = k[k_base + j];
                    partial += q_value * k_value;
                    partial += q_value * k_r[rel_base + j];
                    partial += q_r[rel_base + j] * k_value;
                }
            }
            partial += __shfl_down_sync(0xffffffffu, partial, 16);
            partial += __shfl_down_sync(0xffffffffu, partial, 8);
            partial += __shfl_down_sync(0xffffffffu, partial, 4);
            partial += __shfl_down_sync(0xffffffffu, partial, 2);
            partial += __shfl_down_sync(0xffffffffu, partial, 1);
            if (lane == 0u) {
                probabilities[ki] = mask[b * seq_len + ki] != 0ll ? partial * scale : -3.402823466e+38f;
            }
        }
    }
    __syncthreads();

    float local_max = -3.402823466e+38f;
    for (unsigned int ki = tid; ki < seq_len; ki += kThreads) local_max = fmaxf(local_max, probabilities[ki]);
    reductions[tid] = local_max;
    __syncthreads();
    for (unsigned int stride = kThreads / 2u; stride > 0u; stride >>= 1u) {
        if (tid < stride) reductions[tid] = fmaxf(reductions[tid], reductions[tid + stride]);
        __syncthreads();
    }
    const float max_score = reductions[0];

    float local_sum = 0.0f;
    for (unsigned int ki = tid; ki < seq_len; ki += kThreads) {
        const float probability = mask[b * seq_len + ki] != 0ll ? expf(probabilities[ki] - max_score) : 0.0f;
        probabilities[ki] = probability;
        local_sum += probability;
    }
    reductions[tid] = local_sum;
    __syncthreads();
    for (unsigned int stride = kThreads / 2u; stride > 0u; stride >>= 1u) {
        if (tid < stride) reductions[tid] += reductions[tid + stride];
        __syncthreads();
    }
    const float denom = reductions[0];

    if (tid < head_dim) {
        float acc = 0.0f;
        for (unsigned int ki = 0u; ki < seq_len; ++ki) {
            acc += probabilities[ki] * v[(b * seq_len + ki) * hidden + head_off + tid];
        }
        dst[(b * seq_len + qi) * hidden + head_off + tid] = denom > 0.0f ? acc / denom : 0.0f;
    }
}
// native-program:entry termite_deberta_attention_stream_f16
extern "C" __global__ void termite_deberta_attention_stream_f16(ANTFLY_CUDA_DEBERTA_ATTENTION_STREAM_F16_PARAMS) {
    constexpr unsigned int kWarpsPerBlock = 8u;
    constexpr unsigned int kHeadDim = 64u;
    if (seq_len == 0u || seq_len > 256u || head_dim != kHeadDim || blockDim.x != 256u) return;

    const unsigned int lane = threadIdx.x & 31u;
    const unsigned int warp = threadIdx.x >> 5u;
    const unsigned int block = blockIdx.x;
    const unsigned int query_group = block % ((seq_len + kWarpsPerBlock - 1u) / kWarpsPerBlock);
    const unsigned int bh = block / ((seq_len + kWarpsPerBlock - 1u) / kWarpsPerBlock);
    const unsigned int head = bh % num_heads;
    const unsigned int b = bh / num_heads;
    const unsigned int qi = query_group * kWarpsPerBlock + warp;
    if (b >= batch || qi >= seq_len) return;

    const unsigned int hidden = num_heads * head_dim;
    const unsigned int head_off = head * head_dim;
    const unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
    const float scale = rsqrtf((float)head_dim * 3.0f);
    const float q0 = __half2float(__ushort_as_half(q[q_base + lane]));
    const float q1 = __half2float(__ushort_as_half(q[q_base + lane + 32u]));

    float running_max = -3.402823466e+38f;
    float running_sum = 0.0f;
    float value0 = 0.0f;
    float value1 = 0.0f;
    for (unsigned int ki = 0u; ki < seq_len; ++ki) {
        if (mask[b * seq_len + ki] == 0ll) continue;
        const unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
        const unsigned int rel_idx = qi + seq_len - 1u - ki;
        const unsigned int rel_base = rel_idx * hidden + head_off;
        const float k0 = __half2float(__ushort_as_half(k[k_base + lane]));
        const float k1 = __half2float(__ushort_as_half(k[k_base + lane + 32u]));
        const float kr0 = __half2float(__ushort_as_half(k_r[rel_base + lane]));
        const float kr1 = __half2float(__ushort_as_half(k_r[rel_base + lane + 32u]));
        const float qr0 = __half2float(__ushort_as_half(q_r[rel_base + lane]));
        const float qr1 = __half2float(__ushort_as_half(q_r[rel_base + lane + 32u]));
        float score = q0 * k0 + q1 * k1 + q0 * kr0 + q1 * kr1 + qr0 * k0 + qr1 * k1;
        score += __shfl_down_sync(0xffffffffu, score, 16);
        score += __shfl_down_sync(0xffffffffu, score, 8);
        score += __shfl_down_sync(0xffffffffu, score, 4);
        score += __shfl_down_sync(0xffffffffu, score, 2);
        score += __shfl_down_sync(0xffffffffu, score, 1);
        score = __shfl_sync(0xffffffffu, score, 0) * scale;

        const float next_max = fmaxf(running_max, score);
        const float alpha = expf(running_max - next_max);
        const float beta = expf(score - next_max);
        value0 = value0 * alpha + beta * __half2float(__ushort_as_half(v[k_base + lane]));
        value1 = value1 * alpha + beta * __half2float(__ushort_as_half(v[k_base + lane + 32u]));
        running_sum = running_sum * alpha + beta;
        running_max = next_max;
    }
    const float inv_sum = running_sum > 0.0f ? 1.0f / running_sum : 0.0f;
    dst[q_base + lane] = value0 * inv_sum;
    dst[q_base + lane + 32u] = value1 * inv_sum;
}
// native-program:entry termite_deberta_attention_tc_f16_m16n32
extern "C" __global__ __launch_bounds__(256) void termite_deberta_attention_tc_f16_m16n32(ANTFLY_CUDA_DEBERTA_ATTENTION_PARAMS) {
    constexpr unsigned int QueryTile = 16u, KeyTile = 32u;
#include "../kernels/relative_attention_tensor_tiles.cuh"
}
// native-program:entry termite_deberta_attention_tc_f16_m32n16
extern "C" __global__ __launch_bounds__(256) void termite_deberta_attention_tc_f16_m32n16(ANTFLY_CUDA_DEBERTA_ATTENTION_PARAMS) {
    constexpr unsigned int QueryTile = 32u, KeyTile = 16u;
#include "../kernels/relative_attention_tensor_tiles.cuh"
}
// native-program:entry termite_gliner_encoder_attention_tc_f16
extern "C" __global__ __launch_bounds__(256) void termite_gliner_encoder_attention_tc_f16(
    float* dst, const float* q, const float* k, const float* v,
    const long long* mask, unsigned int batch, unsigned int seq,
    unsigned int heads, unsigned int dim, unsigned int radius
) {
#if defined(__CUDA_ARCH__) && __CUDA_ARCH__ >= 700
    constexpr unsigned int M = 32u, N = 64u, D = 64u, P = 72u, T = 256u; // Skew shared rows to avoid bank conflicts.
    if (!seq || seq > 8192u || dim != D || blockDim.x != T) return;
{{tensor_attention_tile}}
    __shared__ __align__(16) half qt[M * P], kt[N * P], vt[N * P], probabilities[M * N];
    __shared__ __align__(16) float scores[M * N], output[M * D];
    __shared__ float maximum[M], denominator[M], alpha[M];
    for (unsigned int i = tid; i < M * D; i += T) {
        const unsigned int row = i / D, d = i % D;
        qt[row * P + d] = __float2half_rn(start + row < seq ? q[(b * seq + start + row) * hidden + head_off + d] : 0.0f);
        output[i] = 0.0f;
    }
    if (tid < M) { maximum[tid] = -INFINITY; denominator[tid] = 0.0f; }
    __syncthreads();
    const unsigned int key_begin = start > radius ? ((start - radius) / N) * N : 0u;
    const unsigned int key_end = min(seq, start + M + radius);
    for (unsigned int key_start = key_begin; key_start < key_end; key_start += N) {
        for (unsigned int i = tid; i < D * N; i += T) {
            // Keep adjacent lanes on adjacent head dimensions. K is consumed
            // as a column-major D-by-N matrix, so neither global loads nor
            // shared stores need the strided sequence-axis transpose.
            const unsigned int n = i / D, d = i % D, key = key_start + n;
            const bool valid = key < seq && (!mask || mask[b * seq + key] != 0ll);
            const unsigned int offset = (b * seq + key) * hidden + head_off + d;
            kt[n * P + d] = __float2half_rn(valid ? k[offset] : 0.0f);
            vt[n * P + d] = __float2half_rn(valid ? v[offset] : 0.0f);
        }
        __syncthreads();
        const unsigned int qr = warp / 4u, kc = warp % 4u;
        wmma::fragment<wmma::accumulator, 16, 16, 16, float> dot;
        wmma::fill_fragment(dot, 0.0f);
        #pragma unroll
        for (unsigned int d = 0u; d < D; d += 16u) {
            wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> qf;
            wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major> kf;
            wmma::load_matrix_sync(qf, qt + qr * 16u * P + d, P);
            wmma::load_matrix_sync(kf, kt + kc * 16u * P + d, P);
            wmma::mma_sync(dot, qf, kf, dot);
        }
        wmma::store_matrix_sync(scores + qr * 16u * N + kc * 16u, dot, N, wmma::mem_row_major);
        __syncthreads();
        #pragma unroll
        for (unsigned int group = 0u; group < 4u; ++group) {
            const unsigned int row = warp * 4u + group;
            float values[2];
            float next_max = -INFINITY;
            #pragma unroll
            for (unsigned int j = 0u; j < 2u; ++j) {
                const unsigned int n = lane + j * 32u, key = key_start + n;
                const unsigned int query = start + row;
                const bool valid = query < seq && key < seq && key + radius >= query && key <= query + radius &&
                    (!mask || mask[b * seq + key] != 0ll);
                values[j] = valid ? scores[row * N + n] * 0.125f : -INFINITY;
                next_max = fmaxf(next_max, values[j]);
            }
            for (unsigned int shift = 16u; shift; shift >>= 1u)
                next_max = fmaxf(next_max, __shfl_down_sync(0xffffffffu, next_max, shift));
            next_max = fmaxf(maximum[row], __shfl_sync(0xffffffffu, next_max, 0u));
            const float a = isfinite(maximum[row]) ? __expf(maximum[row] - next_max) : 0.0f;
            float sum = 0.0f;
            #pragma unroll
            for (unsigned int j = 0u; j < 2u; ++j) {
                const float p = isfinite(next_max) ? __expf(values[j] - next_max) : 0.0f;
                probabilities[row * N + lane + j * 32u] = __float2half_rn(p);
                sum += p;
            }
            for (unsigned int shift = 16u; shift; shift >>= 1u)
                sum += __shfl_down_sync(0xffffffffu, sum, shift);
            if (lane == 0u) {
                alpha[row] = a;
                denominator[row] = denominator[row] * a + sum;
                maximum[row] = next_max;
            }
        }
        __syncthreads();
        for (unsigned int i = tid; i < M * D; i += T) output[i] *= alpha[i / D];
        __syncthreads();
        wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
        wmma::load_matrix_sync(acc, output + qr * 16u * D + kc * 16u, D, wmma::mem_row_major);
        #pragma unroll
        for (unsigned int n = 0u; n < N; n += 16u) {
            wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> pf;
            wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> vf;
            wmma::load_matrix_sync(pf, probabilities + qr * 16u * N + n, N);
            wmma::load_matrix_sync(vf, vt + n * P + kc * 16u, P);
            wmma::mma_sync(acc, pf, vf, acc);
        }
        wmma::store_matrix_sync(output + qr * 16u * D + kc * 16u, acc, D, wmma::mem_row_major);
        __syncthreads();
    }
    for (unsigned int i = tid; i < M * D; i += T) {
        const unsigned int row = i / D, d = i % D;
        if (start + row < seq)
            dst[(b * seq + start + row) * hidden + head_off + d] = denominator[row] > 0.0f ? output[i] / denominator[row] : 0.0f;
    }
#else
    (void)dst; (void)q; (void)k; (void)v; (void)mask; (void)batch; (void)seq; (void)heads; (void)dim; (void)radius;
#endif
}
// native-program:entry termite_gliner_boundary_attention_f32
extern "C" __global__ void termite_gliner_boundary_attention_f32(
    const float* q, const float* k, const float* v, const float* qr,
    const float* kr, const float* buckets_f32, const float* mask_f32,
    const float* unused7, const float* unused8, const float* unused9,
    float* output, Gliner25BoundaryParams p, unsigned int work_items
) {
    const uint tid = threadIdx.x, lane = tid & 31u, warp = tid >> 5u;
    const uint sequence = p.dims[1], heads = p.dims[2], dim = p.dims[3];
    const uint h = blockIdx.x % heads, qi = (blockIdx.x / heads) % sequence;
    const uint b = blockIdx.x / (heads * sequence), hidden = heads * dim;
    const uint qo = (b * sequence + qi) * hidden + h * dim;
    const int* mask = reinterpret_cast<const int*>(mask_f32);
    const int* buckets = reinterpret_cast<const int*>(buckets_f32);
    if (mask[b * sequence + qi] == 0) {
        if (tid < dim) output[qo + tid] = 0.0f;
        return;
    }
    __shared__ float scores[64], weights[64], partial[4], state[3];
    __shared__ unsigned int invalid;
    if (tid == 0u) { state[0] = -INFINITY; state[1] = 0.0f; invalid = 0u; }
    float value = 0.0f, query[4];
    for (uint j = 0u; j < 4u; ++j)
        query[j] = lane + j * 32u < dim ? q[qo + lane + j * 32u] : 0.0f;
    const float scale = rsqrtf(3.0f * float(dim));
    __syncthreads();
    for (uint start = 0u; start < sequence; start += 64u) {
        const uint count = min(64u, sequence - start);
        for (uint slot = warp; slot < count; slot += 4u) {
            const uint ki = start + slot;
            float score = -INFINITY;
            if (mask[b * sequence + ki] != 0) {
                const int bucket = buckets[qi + sequence - 1u - ki];
                const bool legal = bucket >= 0 && uint(bucket) < p.dims[4];
                if (!legal && lane == 0u) atomicOr(&invalid, 1u);
                const uint ko = (b * sequence + ki) * hidden + h * dim;
                const uint ro = (legal ? uint(bucket) : 0u) * hidden + h * dim;
                float cc = 0.0f, cp = 0.0f, pc = 0.0f;
                for (uint j = 0u; j < 4u; ++j) {
                    const uint d = lane + j * 32u;
                    if (d < dim) {
                        const float kv = k[ko + d];
                        cc += query[j] * kv;
                        cp += query[j] * kr[ro + d];
                        pc += kv * qr[ro + d];
                    }
                }
                score = (gliner25_warp_sum(cc) + gliner25_warp_sum(cp) + gliner25_warp_sum(pc)) * scale;
                if (!isfinite(score) && lane == 0u) atomicOr(&invalid, 1u);
            }
            if (lane == 0u) scores[slot] = score;
        }
        __syncthreads();
        float maximum = tid < count ? scores[tid] : -INFINITY;
        maximum = gliner25_warp_max(maximum);
        if (lane == 0u) partial[warp] = maximum;
        __syncthreads();
        if (tid == 0u) {
            maximum = state[0];
            for (uint i = 0u; i < 4u; ++i) maximum = fmaxf(maximum, partial[i]);
            state[2] = state[1] == 0.0f ? 0.0f : expf(state[0] - maximum);
            state[0] = maximum;
        }
        __syncthreads();
        float sum = tid < count && scores[tid] != -INFINITY ? expf(scores[tid] - state[0]) : 0.0f;
        if (tid < count) weights[tid] = sum;
        sum = gliner25_warp_sum(sum);
        if (lane == 0u) partial[warp] = sum;
        __syncthreads();
        if (tid == 0u) {
            sum = 0.0f;
            for (uint i = 0u; i < 4u; ++i) sum += partial[i];
            state[1] = state[1] * state[2] + sum;
        }
        if (tid < dim) {
            value *= state[2];
            for (uint i = 0u; i < count; ++i)
                if (weights[i] != 0.0f)
                    value += weights[i] * v[(b * sequence + start + i) * hidden + h * dim + tid];
        }
        __syncthreads();
    }
    if (tid < dim) output[qo + tid] = invalid == 0u && state[1] > 0.0f ? value / state[1] : NAN;
}
// native-program:entry termite_gliner_boundary_attention_tc_f16
extern "C" __global__ __launch_bounds__(256) void termite_gliner_boundary_attention_tc_f16(ANTFLY_CUDA_GLINER_BOUNDARY_PARAMS) {
    const unsigned int batch = p.dims[0], seq_len = p.dims[1], num_heads = p.dims[2], head_dim = p.dims[3];
    const int* mask = reinterpret_cast<const int*>(mask_values);
    const int* buckets = reinterpret_cast<const int*>(bucket_values);
    constexpr unsigned int kThreads = 256u;
    constexpr unsigned int kQueryTile = 16u;
    constexpr unsigned int kKeyTile = 32u;
    constexpr unsigned int kHeadDim = 64u;
    constexpr unsigned int kPitch = 72u;
    constexpr unsigned int kRelTile = kQueryTile + kKeyTile - 1u;
    constexpr unsigned int kRelPitch = 48u; // WMMA requires a 16-column tail.
    constexpr float kNegInf = -3.402823466e+38f;
    if (seq_len == 0u || seq_len > 16384u || head_dim != kHeadDim || blockDim.x != kThreads) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int query_tiles = (seq_len + kQueryTile - 1u) / kQueryTile;
    const unsigned int block = blockIdx.x;
    const unsigned int query_tile = block % query_tiles;
    const unsigned int matrix = block / query_tiles;
    const unsigned int head = matrix % num_heads;
    const unsigned int b = matrix / num_heads;
    if (b >= batch) return;

    const unsigned int hidden = num_heads * kHeadDim;
    const unsigned int head_off = head * kHeadDim;
    const unsigned int query_start = query_tile * kQueryTile;
    const float scale = rsqrtf((float)(kHeadDim * 3u));

    __shared__ __align__(16) half q_tile[kQueryTile * kPitch];
    // The same coalesced [N,D] storage serves row-major A and column-major B.
    __shared__ __align__(16) half k_rows[kKeyTile * kPitch];
    __shared__ __align__(16) half v_tile[kKeyTile * kPitch];
    // Relative tensors use [R,D] storage and column-major WMMA B views.
    __shared__ __align__(16) half qr_tile[kPitch * kRelPitch];
    __shared__ __align__(16) half kr_tile[kPitch * kRelPitch];
    __shared__ __align__(16) float scores[kQueryTile * kKeyTile];
    __shared__ __align__(16) float c2p_scores[kQueryTile * kRelPitch];
    __shared__ __align__(16) float p2c_scores[kKeyTile * kRelPitch];
    __shared__ __align__(16) half probabilities[kQueryTile * kKeyTile];
    __shared__ __align__(16) float output[kQueryTile * kHeadDim];
    __shared__ float running_max[kQueryTile];
    __shared__ float running_sum[kQueryTile];
    __shared__ float row_alpha[kQueryTile];
    __shared__ unsigned int invalid[kQueryTile];

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int row = index / kHeadDim;
        const unsigned int d = index - row * kHeadDim;
        const unsigned int qi = query_start + row;
        q_tile[row * kPitch + d] = qi < seq_len ? __float2half_rn(q[(b * seq_len + qi) * hidden + head_off + d]) : __float2half_rn(0.0f);
        output[index] = 0.0f;
    }
    if (tid < kQueryTile) {
        running_max[tid] = kNegInf;
        running_sum[tid] = 0.0f;
        invalid[tid] = 0u;
    }
    __syncthreads();

    for (unsigned int key_start = 0u; key_start < seq_len; key_start += kKeyTile) {
        // The relative window spans r=(query_start - key_start) +
        // [-(N-1), M-1], exactly the values needed by this score tile.
        const int rel_low = (int)query_start + (int)seq_len - 1 - ((int)key_start + (int)kKeyTile - 1);
        for (unsigned int index = tid; index < kHeadDim * kKeyTile; index += kThreads) {
            const unsigned int n = index / kHeadDim;
            const unsigned int d = index % kHeadDim;
            const unsigned int ki = key_start + n;
            const float kval = ki < seq_len && mask[b * seq_len + ki] != 0 ? k[(b * seq_len + ki) * hidden + head_off + d] : 0.0f;
            k_rows[n * kPitch + d] = __float2half_rn(kval);
            v_tile[n * kPitch + d] = __float2half_rn(ki < seq_len && mask[b * seq_len + ki] != 0 ? v[(b * seq_len + ki) * hidden + head_off + d] : 0.0f);
        }
        for (unsigned int index = tid; index < kHeadDim * kRelPitch; index += kThreads) {
            const unsigned int r = index / kHeadDim;
            const unsigned int d = index % kHeadDim;
            const int rel = rel_low + (int)r;
            const int bucket = r < kRelTile && rel >= 0 && rel < (int)(seq_len * 2u - 1u) ? buckets[rel] : -1;
            const bool valid = bucket >= 0 && (unsigned int)bucket < p.dims[4];
            const float qrv = valid ? q_r[(unsigned int)bucket * hidden + head_off + d] : 0.0f;
            const float krv = valid ? k_r[(unsigned int)bucket * hidden + head_off + d] : 0.0f;
            qr_tile[r * kPitch + d] = __float2half_rn(qrv);
            kr_tile[r * kPitch + d] = __float2half_rn(krv);
        }
        __syncthreads();

        // Q*K^T: two 16x16 fragments, one per warp.
        if (warp < 2u) {
            ANTFLY_CUDA_WMMA_DOT16_F32(
                half, wmma::col_major, kHeadDim,
                q_tile + d0, kPitch,
                k_rows + warp * 16u * kPitch + d0, kPitch,
                scores + warp * 16u, kKeyTile)
        }

        // Q*Kr^T: three compact relative columns. Warp 2 handles chunks 0/2
        // and warp 3 handles chunk 1, keeping all relative gathers local.
        if (warp == 2u || warp == 3u) {
            const unsigned int first_chunk = warp == 2u ? 0u : 1u;
            for (unsigned int chunk = first_chunk; chunk < 3u; chunk += 2u) {
                ANTFLY_CUDA_WMMA_DOT16_F32(
                    half, wmma::col_major, kHeadDim,
                    q_tile + d0, kPitch,
                    kr_tile + chunk * 16u * kPitch + d0, kPitch,
                    c2p_scores + chunk * 16u, kRelPitch)
            }
        }

        // K*Qr^T: six compact relative fragments.  The result is indexed by
        // key row so the p2c diagonal is a simple shared-memory gather.
        if (warp >= 4u) {
            for (unsigned int work = warp - 4u; work < 6u; work += 4u) {
                const unsigned int key_chunk = work / 3u;
                const unsigned int rel_chunk = work - key_chunk * 3u;
                ANTFLY_CUDA_WMMA_DOT16_F32(
                    half, wmma::col_major, kHeadDim,
                    k_rows + (key_chunk * 16u) * kPitch + d0, kPitch,
                    qr_tile + rel_chunk * 16u * kPitch + d0, kPitch,
                    p2c_scores + (key_chunk * 16u) * kRelPitch + rel_chunk * 16u, kRelPitch)
            }
        }
        __syncthreads();

        // Eight warps each normalize two query rows.  Scores are kept F32;
        // only the matrix-A probabilities are narrowed for the P*V MMA.
        #pragma unroll
        for (unsigned int sub = 0u; sub < 2u; ++sub) {
            const unsigned int m = warp * 2u + sub;
            const unsigned int qi = query_start + m;
            const unsigned int ki = key_start + lane;
            float score = kNegInf;
            if (qi < seq_len && ki < seq_len && mask[b * seq_len + ki] != 0) {
                const unsigned int rel = m + kKeyTile - 1u - lane;
                score = (scores[m * kKeyTile + lane] + c2p_scores[m * kRelPitch + rel] + p2c_scores[lane * kRelPitch + rel]) * scale;
                const int bucket = buckets[qi + seq_len - 1u - ki];
                if (bucket < 0 || (unsigned int)bucket >= p.dims[4] || !isfinite(score)) atomicOr(&invalid[m], 1u);
            }
            scores[m * kKeyTile + lane] = score;
            float tile_max = score;
            #pragma unroll
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) tile_max = fmaxf(tile_max, __shfl_down_sync(0xffffffffu, tile_max, offset));
            tile_max = __shfl_sync(0xffffffffu, tile_max, 0u);
            const float old_max = running_max[m];
            const float next_max = fmaxf(old_max, tile_max);
            const float alpha = old_max > kNegInf * 0.5f ? expf(old_max - next_max) : 0.0f;
            const float beta = score > kNegInf * 0.5f ? expf(score - next_max) : 0.0f;
            float tile_sum = beta;
            #pragma unroll
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) tile_sum += __shfl_down_sync(0xffffffffu, tile_sum, offset);
            tile_sum = __shfl_sync(0xffffffffu, tile_sum, 0u);
            probabilities[m * kKeyTile + lane] = __float2half_rn(beta);
            if (lane == 0u) {
                running_max[m] = next_max;
                running_sum[m] = running_sum[m] * alpha + tile_sum;
                row_alpha[m] = alpha;
            }
        }
        __syncthreads();

        for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
            output[index] *= row_alpha[index / kHeadDim];
        }
        __syncthreads();

        // P*V: four output-D fragments, accumulated into the online F32
        // output state.  This is the only value path; no probability tensor
        // ever reaches device memory.
        if (warp < 4u) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::load_matrix_sync(acc, output + warp * 16u, kHeadDim, wmma::mem_row_major);
            #pragma unroll
            for (unsigned int key_chunk = 0u; key_chunk < 2u; ++key_chunk) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
                wmma::load_matrix_sync(p_frag, probabilities + key_chunk * 16u, kKeyTile);
                wmma::load_matrix_sync(v_frag, v_tile + (key_chunk * 16u) * kPitch + warp * 16u, kPitch);
                wmma::mma_sync(acc, p_frag, v_frag, acc);
            }
            wmma::store_matrix_sync(output + warp * 16u, acc, kHeadDim, wmma::mem_row_major);
        }
        __syncthreads();
    }

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int m = index / kHeadDim;
        const unsigned int d = index - m * kHeadDim;
        const unsigned int qi = query_start + m;
        if (qi < seq_len) {
            const float denom = running_sum[m];
            dst[(b * seq_len + qi) * hidden + head_off + d] = mask[b * seq_len + qi] == 0 ? 0.0f :
                (invalid[m] == 0u && denom > 0.0f ? output[index] / denom : NAN);
        }
    }
}
// native-program:entry termite_gliner_encoder_attention_tc_f16_m64
extern "C" __global__ __launch_bounds__(256) void termite_gliner_encoder_attention_tc_f16_m64(
    float* dst, const float* q, const float* k, const float* v,
    const long long* mask, unsigned int batch, unsigned int seq,
    unsigned int heads, unsigned int dim, unsigned int radius
) {
    constexpr unsigned int M = 64u, N = 64u, D = 64u, P = 72u, S = 68u, T = 256u;
    if (!seq || seq > 8192u || dim != D || radius < seq || blockDim.x != T) return;
{{tensor_attention_tile}}
    __shared__ __align__(16) half qt[M * P], kt[N * P], vt[N * P];
    __shared__ __align__(16) float shared_scores[M * P / 2u + 32u * S];
    half* probabilities = reinterpret_cast<half*>(shared_scores);
    float* scratch = shared_scores + M * P / 2u;
    __shared__ float maximum[M], denominator[M], alpha[M];
    wmma::fragment<wmma::accumulator, 16, 16, 16, float> output[2];
    #pragma unroll
    for (unsigned int g = 0u; g < 2u; ++g) wmma::fill_fragment(output[g], 0.0f);
    for (unsigned int i = tid; i < M * D; i += T) {
        const unsigned int row = i / D, d = i % D;
        qt[row * P + d] = __float2half_rn(start + row < seq ? q[(b * seq + start + row) * hidden + head_off + d] : 0.0f);
    }
    if (tid < M) { maximum[tid] = -INFINITY; denominator[tid] = 0.0f; }
    __syncthreads();
    for (unsigned int key_start = 0u; key_start < seq; key_start += N) {
        for (unsigned int i = tid; i < N * D; i += T) {
            const unsigned int row = i / D, d = i % D, key = key_start + row;
            const bool valid = key < seq && (!mask || mask[b * seq + key] != 0ll);
            const unsigned int offset = (b * seq + key) * hidden + head_off + d;
            kt[row * P + d] = __float2half_rn(valid ? k[offset] : 0.0f);
            vt[row * P + d] = __float2half_rn(valid ? v[offset] : 0.0f);
        }
        __syncthreads();
        #pragma unroll
        for (unsigned int g = 0u; g < 2u; ++g) {
            const unsigned int qr = warp / 4u + g * 2u, kc = warp % 4u;
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> dot;
            wmma::fill_fragment(dot, 0.0f);
            #pragma unroll
            for (unsigned int d = 0u; d < D; d += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> qf;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major> kf;
                wmma::load_matrix_sync(qf, qt + qr * 16u * P + d, P);
                wmma::load_matrix_sync(kf, kt + kc * 16u * P + d, P);
                wmma::mma_sync(dot, qf, kf, dot);
            }
            wmma::store_matrix_sync(shared_scores + qr * 16u * S + kc * 16u, dot, S, wmma::mem_row_major);
        }
        __syncthreads();
        float staged_probabilities[8][2];
        #pragma unroll
        for (unsigned int g = 0u; g < 8u; ++g) {
            const unsigned int row = warp * 8u + g;
            float values[2], next_max = -INFINITY;
            #pragma unroll
            for (unsigned int j = 0u; j < 2u; ++j) {
                const unsigned int n = lane + j * 32u, key = key_start + n;
                const bool valid = start + row < seq && key < seq && (!mask || mask[b * seq + key] != 0ll);
                values[j] = valid ? shared_scores[row * S + n] * 0.125f : -INFINITY;
                next_max = fmaxf(next_max, values[j]);
            }
            for (unsigned int shift = 16u; shift; shift >>= 1u)
                next_max = fmaxf(next_max, __shfl_down_sync(0xffffffffu, next_max, shift));
            next_max = fmaxf(maximum[row], __shfl_sync(0xffffffffu, next_max, 0u));
            const float a = isfinite(maximum[row]) ? __expf(maximum[row] - next_max) : 0.0f;
            float sum = 0.0f;
            #pragma unroll
            for (unsigned int j = 0u; j < 2u; ++j) {
                const float p = isfinite(next_max) ? __expf(values[j] - next_max) : 0.0f;
                staged_probabilities[g][j] = p;
                sum += p;
            }
            for (unsigned int shift = 16u; shift; shift >>= 1u)
                sum += __shfl_down_sync(0xffffffffu, sum, shift);
            if (lane == 0u) {
                alpha[row] = a;
                denominator[row] = denominator[row] * a + sum;
                maximum[row] = next_max;
            }
        }
        // Half probabilities alias the lower half of shared score storage.
        // No writer may reuse it before every warp has read all of its rows.
        __syncthreads();
        #pragma unroll
        for (unsigned int g = 0u; g < 8u; ++g) {
            const unsigned int row = warp * 8u + g;
            #pragma unroll
            for (unsigned int j = 0u; j < 2u; ++j)
                probabilities[row * P + lane + j * 32u] = __float2half_rn(staged_probabilities[g][j]);
        }
        __syncthreads();
        #pragma unroll
        for (unsigned int g = 0u; g < 2u; ++g) {
            const unsigned int qr = warp / 4u, dc = warp % 4u;
            // WMMA's fragment layout is opaque. Store, rescale by row, and
            // reload through the public API instead of assuming lane mapping.
            wmma::store_matrix_sync(scratch + qr * 16u * S + dc * 16u, output[g], S, wmma::mem_row_major);
            __syncthreads();
            for (unsigned int i = tid; i < 32u * D; i += T) scratch[(i / D) * S + i % D] *= alpha[g * 32u + i / D];
            __syncthreads();
            wmma::load_matrix_sync(output[g], scratch + qr * 16u * S + dc * 16u, S, wmma::mem_row_major);
            #pragma unroll
            for (unsigned int n = 0u; n < N; n += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> pf;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> vf;
                wmma::load_matrix_sync(pf, probabilities + (g * 32u + qr * 16u) * P + n, P);
                wmma::load_matrix_sync(vf, vt + n * P + dc * 16u, P);
                wmma::mma_sync(output[g], pf, vf, output[g]);
            }
            __syncthreads();
        }
    }
    #pragma unroll
    for (unsigned int g = 0u; g < 2u; ++g) {
        wmma::store_matrix_sync(scratch + (warp / 4u) * 16u * S + (warp % 4u) * 16u, output[g], S, wmma::mem_row_major);
        __syncthreads();
        for (unsigned int i = tid; i < 32u * D; i += T) {
            const unsigned int row = g * 32u + i / D, d = i % D;
            if (start + row < seq)
                dst[(b * seq + start + row) * hidden + head_off + d] = denominator[row] > 0.0f ? scratch[(i / D) * S + d] / denominator[row] : 0.0f;
        }
        __syncthreads();
    }
}
