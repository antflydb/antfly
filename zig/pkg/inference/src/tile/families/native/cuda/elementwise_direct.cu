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

// native-program:entry termite_fill_f32
extern "C" __global__ void termite_fill_f32(float* dst, unsigned int n, float value) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = value;
}
// native-program:entry termite_scale_f32
extern "C" __global__ void termite_scale_f32(
    float* dst,
    const float* input,
    unsigned int n,
    float scale
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = input[i] * scale;
}
// native-program:entry termite_add_scalar_f32
extern "C" __global__ void termite_add_scalar_f32(
    float* dst,
    const float* input,
    unsigned int n,
    float value
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = input[i] + value;
}
// native-program:entry termite_binary_scalar_f32
extern "C" __global__ void termite_binary_scalar_f32(
    float* dst,
    const float* input,
    const float* scalar,
    unsigned int n,
    unsigned int op,
    unsigned int scalar_on_left
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    float value = scalar[0];
    float input_value = input[i];
    float x = scalar_on_left ? value : input_value;
    float y = scalar_on_left ? input_value : value;
    if (op == 0u) dst[i] = x + y;
    else if (op == 1u) dst[i] = x * y;
    else if (op == 8u) dst[i] = x - y;
    else if (op == 9u) dst[i] = x / y;
    else dst[i] = x < y ? 1.0f : 0.0f;
}
// native-program:entry termite_add_mul_scalar_f32
extern "C" __global__ void termite_add_mul_scalar_f32(
    float* dst,
    const float* a,
    const float* b,
    const float* scalar,
    unsigned int n
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = a[i] * scalar[0] + b[i];
}
// native-program:entry termite_add_weighted_scalars_f32
extern "C" __global__ void termite_add_weighted_scalars_f32(
    float* dst,
    const float* a,
    const float* b,
    float scale_a,
    float scale_b,
    unsigned int n
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = a[i] * scale_a + b[i] * scale_b;
}
// native-program:entry termite_round_f32
extern "C" __global__ void termite_round_f32(
    float* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    dst[idx] = nearbyintf(input[idx]);
}
// native-program:entry termite_add_bias_rows_f32
extern "C" __global__ void termite_add_bias_rows_f32(
    float* dst,
    const float* bias,
    unsigned int rows,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int col = idx % out_dim;
    dst[idx] += bias[col];
}
// native-program:entry termite_add_bias_relu_rows_f32
extern "C" __global__ void termite_add_bias_relu_rows_f32(
    float* dst,
    const float* bias,
    unsigned int rows,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int col = idx % out_dim;
    float value = dst[idx] + bias[col];
    dst[idx] = value > 0.0f ? value : 0.0f;
}
// native-program:entry termite_gemma4_a4b_parallel_ffn_post_residual_f32
extern "C" __global__ void termite_gemma4_a4b_parallel_ffn_post_residual_f32(
    float* dst,
    const float* shared,
    const float* shared_weight,
    const float* routed,
    const float* routed_weight,
    const float* combined_weight,
    const float* residual,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    const unsigned int row = blockIdx.x;
    if (row >= rows) return;
    const unsigned int tid = threadIdx.x;
    const unsigned int base = row * dim;
    __shared__ float shared_partial[256];
    __shared__ float routed_partial[256];
    float shared_sumsq = 0.0f;
    float routed_sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        const float shared_value = shared[base + i];
        const float routed_value = routed[base + i];
        shared_sumsq += shared_value * shared_value;
        routed_sumsq += routed_value * routed_value;
    }
    float values[] = {shared_sumsq, routed_sumsq};
    float* const planes[] = {shared_partial, routed_partial};
    antfly_row_tree_statistics(values, planes);
    const float shared_scale = rsqrtf(shared_partial[0] / (float)dim + eps);
    const float routed_scale = rsqrtf(routed_partial[0] / (float)dim + eps);
    // Every lane must consume the two completed reductions before any lane
    // reuses shared_partial for the combined-value reduction below. Without
    // this barrier thread 0 may overwrite shared_partial[0] while another
    // warp is still loading the shared RMS denominator.
    __syncthreads();

    float combined_sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        const unsigned int idx = base + i;
        const float combined =
            shared[idx] * shared_scale * shared_weight[i] +
            routed[idx] * routed_scale * routed_weight[i];
        dst[idx] = combined;
        combined_sumsq += combined * combined;
    }
    float combined_values[] = {combined_sumsq};
    float* const combined_planes[] = {shared_partial};
    antfly_row_tree_statistics(combined_values, combined_planes);
    const float combined_scale = rsqrtf(shared_partial[0] / (float)dim + eps);
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        const unsigned int idx = base + i;
        dst[idx] = dst[idx] * combined_scale * combined_weight[i] + residual[idx];
    }
}
// native-program:entry termite_elementwise_f32
extern "C" __global__ void termite_elementwise_f32(
    float* dst,
    const float* a,
    const float* b,
    unsigned int count,
    unsigned int op
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float x = a[i];
    float y = b ? b[i] : 0.0f;
    float out;
    if (op == 0u) {
        out = x + y;
    } else if (op == 1u) {
        out = x * y;
    } else if (op == 2u) {
        out = termite_decoder_activation_f32(x, 2u);
    } else if (op == 3u) {
        out = termite_decoder_activation_f32(x, 0u);
    } else if (op == 4u) {
        out = fmaxf(x, 0.0f);
    } else if (op == 5u) {
        out = termite_decoder_activation_f32(x, 4u);
    } else if (op == 6u) {
        out = 1.0f / (1.0f + expf(-x));
    } else if (op == 7u) {
        out = tanhf(x);
    } else if (op == 8u) {
        out = x - y;
    } else if (op == 9u) {
        out = x / y;
    } else if (op == 10u) {
        out = -x;
    } else if (op == 11u) {
        out = sqrtf(x);
    } else if (op == 12u) {
        out = rsqrtf(x);
    } else if (op == 13u) {
        out = expf(x);
    } else if (op == 14u) {
        out = logf(x);
    } else if (op == 15u) {
        out = sinf(x);
    } else if (op == 16u) {
        out = cosf(x);
    } else if (op == 17u) {
        out = erff(x);
    } else if (op == 18u) {
        out = fabsf(x);
    } else if (op == 19u) {
        out = x < y ? 1.0f : 0.0f;
    } else if (op == 20u) {
        out = isfinite(x) ? 0.5f * x * (1.0f + erff(x * 0.7071067811865476f)) : 0.0f;
    } else if (op == 21u) {
        if (!isfinite(x)) {
            out = 0.0f;
        } else {
            float cdf = 0.5f * (1.0f + erff(x * 0.7071067811865476f));
            float pdf = expf(-0.5f * x * x) * 0.3989422804014327f;
            float derivative = cdf + x * pdf;
            out = isfinite(derivative) ? y * derivative : 0.0f;
        }
    } else {
        float x2 = x * x;
        float inner = 0.7978845608028654f * (x + 0.044715f * x * x2);
        float derivative;
        if (inner > 10.0f) {
            derivative = 1.0f;
        } else if (inner < -10.0f) {
            derivative = 0.0f;
        } else {
            float t = tanhf(inner);
            derivative = {{gelu_derivative_x}};
        }
        out = isfinite(derivative) ? y * derivative : 0.0f;
    }
    dst[i] = out;
}
// native-program:entry termite_silu_multiply_f32
extern "C" __global__ void termite_silu_multiply_f32(
    float* dst,
    const float* gate,
    const float* up,
    unsigned int count
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float x = gate[i];
    dst[i] = (termite_decoder_activation_f32(x, 2u)) * up[i];
}
// native-program:entry termite_activation_multiply_f32
extern "C" __global__ void termite_activation_multiply_f32(
    float* dst,
    const float* gate,
    const float* up,
    unsigned int count,
    unsigned int activation
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float x = gate[i];
    float act = termite_decoder_activation_f32(x, activation);
    dst[i] = act * up[i];
}
// native-program:entry termite_florence_vision_tail_sources_f32
extern "C" __global__ void termite_florence_vision_tail_sources_f32(ANTFLY_CUDA_FLORENCE_VISION_TAIL_SOURCES_F32_PARAMS) {
    unsigned int token_count = height * width;
    unsigned int out_seq = token_count + 1u;
    unsigned int total = batch * out_seq * dim;
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= total || token_count == 0u) return;

    unsigned int feature = idx % dim;
    unsigned int out_token = (idx / dim) % out_seq;
    unsigned int b = idx / (dim * out_seq);
    unsigned int row_dim = dim / 2u;
    unsigned int col_dim = dim - row_dim;

    float temporal = (has_temporal != 0u && temporal_embed != nullptr) ? termite_load_tail_weight_f32(temporal_embed, temporal_dtype, feature) : 0.0f;
    if (out_token == 0u) {
        float acc = 0.0f;
        for (unsigned int token = 0u; token < token_count; ++token) {
            unsigned int y = token / width;
            unsigned int x = token - y * width;
            float pos = feature < col_dim
                ? termite_load_tail_weight_f32(col_embed, col_dtype, x * col_dim + feature)
                : termite_load_tail_weight_f32(row_embed, row_dtype, y * row_dim + (feature - col_dim));
            acc += tokens[(b * token_count + token) * dim + feature] + pos + temporal;
        }
        dst[idx] = acc / (float)token_count;
        return;
    }

    unsigned int token = out_token - 1u;
    unsigned int y = token / width;
    unsigned int x = token - y * width;
    float pos = feature < col_dim
        ? termite_load_tail_weight_f32(col_embed, col_dtype, x * col_dim + feature)
        : termite_load_tail_weight_f32(row_embed, row_dtype, y * row_dim + (feature - col_dim));
    dst[idx] = tokens[(b * token_count + token) * dim + feature] + pos + temporal;
}
// native-program:entry termite_dequant_q4_0_bf16
extern "C" __global__ void termite_dequant_q4_0_bf16(unsigned short* dst, const unsigned char* src, unsigned int block_count) {
    const size_t pairs = size_t(block_count) * 16u;
    const size_t stride = size_t(gridDim.x) * blockDim.x;
    for (size_t pair = size_t(blockIdx.x) * blockDim.x + threadIdx.x; pair < pairs; pair += stride) {
        const unsigned char* bp = src + (pair / 16u) * 18u;
        const unsigned short h = unsigned(bp[0]) | (unsigned(bp[1]) << 8u);
        const float d = termite_half_to_float(h);
        const unsigned char packed = bp[2u + pair % 16u];
        unsigned short* out = dst + (pair / 16u) * 32u + pair % 16u;
        out[0] = termite_f32_to_bf16(float(int(packed & 15u) - 8) * d);
        out[16] = termite_f32_to_bf16(float(int(packed >> 4u) - 8) * d);
    }
}
// native-program:entry termite_primitive_softmax_f32
extern "C" __global__ void termite_primitive_softmax_f32(ANTFLY_CUDA_PRIMITIVE_SOFTMAX_PARAMS) {
    unsigned int row = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int rows = count / last_dim;
    if (row >= rows) return;
    unsigned int row_base = row * last_dim;
    float row_max = -3.402823466e+38f;
    for (unsigned int d = 0; d < last_dim; ++d) row_max = fmaxf(row_max, input[row_base + d]);
    float sum = 0.0f;
    for (unsigned int d = 0; d < last_dim; ++d) sum += expf(input[row_base + d] - row_max);
    for (unsigned int d = 0; d < last_dim; ++d) {
        unsigned int idx = row_base + d;
        output[idx] = log_softmax ? input[idx] - row_max - logf(sum) : expf(input[idx] - row_max) / sum;
    }
}
// native-program:entry termite_selected_token_logprobs_f32
extern "C" __global__ void termite_selected_token_logprobs_f32(
    float* output,
    const float* logits,
    const unsigned int* row_indices,
    const unsigned int* token_ids,
    unsigned int selected_count,
    unsigned int rows,
    unsigned int vocab_size
) {
    unsigned int selected = blockIdx.x;
    if (selected >= selected_count) return;
    unsigned int row = row_indices[selected];
    unsigned int token = token_ids[selected];
    if (row >= rows || token >= vocab_size) return;
    const float* row_logits = logits + (size_t)row * vocab_size;

    __shared__ float reduction[256];
    float local_max = -3.402823466e+38f;
    for (unsigned int col = threadIdx.x; col < vocab_size; col += blockDim.x) {
        local_max = fmaxf(local_max, row_logits[col]);
    }
    reduction[threadIdx.x] = local_max;
    __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (threadIdx.x < stride) reduction[threadIdx.x] = fmaxf(reduction[threadIdx.x], reduction[threadIdx.x + stride]);
        __syncthreads();
    }
    float row_max = reduction[0];

    float local_sum = 0.0f;
    for (unsigned int col = threadIdx.x; col < vocab_size; col += blockDim.x) {
        local_sum += expf(row_logits[col] - row_max);
    }
    reduction[threadIdx.x] = local_sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (threadIdx.x < stride) reduction[threadIdx.x] += reduction[threadIdx.x + stride];
        __syncthreads();
    }
    if (threadIdx.x == 0) output[selected] = row_logits[token] - row_max - logf(reduction[0]);
}
// native-program:entry termite_deberta_scores_softmax_f32
extern "C" __global__ void termite_deberta_scores_softmax_f32(
    float* content_scores,
    const float* c2p_scores,
    const float* p2c_scores,
    const long long* mask,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    constexpr unsigned int kThreads = 256u;
    if (seq_len == 0u || blockDim.x != kThreads) return;
    const unsigned int block = blockIdx.x;
    const unsigned int qi = block % seq_len;
    const unsigned int matrix = block / seq_len;
    const unsigned int b = matrix / num_heads;
    if (b >= batch) return;
    const unsigned int tid = threadIdx.x;
    const unsigned int rel_len = seq_len * 2u - 1u;
    const unsigned int score_base = (matrix * seq_len + qi) * seq_len;
    const unsigned int c2p_base = (matrix * seq_len + qi) * rel_len;
    const unsigned int p2c_matrix_base = matrix * seq_len * rel_len;
    const unsigned int rel_base = qi + seq_len - 1u;
    const float scale = rsqrtf((float)head_dim * 3.0f);
    __shared__ float reductions[kThreads];

    float local_max = -3.402823466e+38f;
    for (unsigned int ki = tid; ki < seq_len; ki += kThreads) {
        float score = -3.402823466e+38f;
        if (mask[b * seq_len + ki] != 0ll) {
            const unsigned int rel_idx = rel_base - ki;
            score = (content_scores[score_base + ki] + c2p_scores[c2p_base + rel_idx] + p2c_scores[p2c_matrix_base + ki * rel_len + rel_idx]) * scale;
        }
        content_scores[score_base + ki] = score;
        local_max = fmaxf(local_max, score);
    }
    reductions[tid] = local_max;
    __syncthreads();
    for (unsigned int stride = kThreads / 2u; stride > 0u; stride >>= 1u) {
        if (tid < stride) reductions[tid] = fmaxf(reductions[tid], reductions[tid + stride]);
        __syncthreads();
    }
    const float max_score = reductions[0];
    float local_sum = 0.0f;
    for (unsigned int ki = tid; ki < seq_len; ki += kThreads) {
        const float value = content_scores[score_base + ki];
        const float probability = value > -3.4e38f ? expf(value - max_score) : 0.0f;
        content_scores[score_base + ki] = probability;
        local_sum += probability;
    }
    reductions[tid] = local_sum;
    __syncthreads();
    for (unsigned int stride = kThreads / 2u; stride > 0u; stride >>= 1u) {
        if (tid < stride) reductions[tid] += reductions[tid + stride];
        __syncthreads();
    }
    const float inv_sum = reductions[0] > 0.0f ? 1.0f / reductions[0] : 0.0f;
    for (unsigned int ki = tid; ki < seq_len; ki += kThreads) content_scores[score_base + ki] *= inv_sum;
}
