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

#include <cuda_bf16.h>
#include <cuda_fp16.h>
#include <mma.h>

using namespace nvcuda;

extern "C" __global__ void termite_fill_f32(float* dst, unsigned int n, float value) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = value;
}

extern "C" __global__ void termite_scale_f32(
    float* dst,
    const float* input,
    unsigned int n,
    float scale
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = input[i] * scale;
}

extern "C" __global__ void termite_add_scalar_f32(
    float* dst,
    const float* input,
    unsigned int n,
    float value
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = input[i] + value;
}

extern "C" __global__ void termite_binary_scalar_f32(
    float* dst,
    const float* input,
    const float* scalar,
    unsigned int n,
    unsigned int op
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    float value = scalar[0];
    float x = input[i];
    dst[i] = op == 0u ? x + value : x * value;
}

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

extern "C" __global__ void termite_linear_f32(
    float* dst,
    const float* input,
    const float* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    float acc = 0.0f;
    for (unsigned int k = 0; k < in_dim; ++k) {
        acc += input[row * in_dim + k] * weight[col * in_dim + k];
    }
    dst[idx] = acc;
}

__device__ __forceinline__ float termite_bf16_to_f32(unsigned short value) {
    return __uint_as_float(((unsigned int)value) << 16);
}

__device__ __forceinline__ unsigned short termite_f32_to_bf16(float value) {
    unsigned int bits = __float_as_uint(value);
    unsigned int lsb = (bits >> 16) & 1u;
    unsigned int rounding_bias = 0x7fffu + lsb;
    return (unsigned short)((bits + rounding_bias) >> 16);
}

extern "C" __global__ void termite_f32_to_bf16(
    unsigned short* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    dst[idx] = termite_f32_to_bf16(input[idx]);
}

extern "C" __global__ void termite_linear_bf16_weight_f32_tiled(
    float* dst,
    const float* input,
    const unsigned short* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int global = blockIdx.x;
    unsigned int total = rows * out_dim;
    if (global >= total) return;
    unsigned int row = global / out_dim;
    unsigned int col = global - row * out_dim;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        acc += input[row * in_dim + i] * termite_bf16_to_f32(weight[col * in_dim + i]);
    }
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[global] = partial[0];
}

extern "C" __global__ void termite_argmax_last_row_f32(
    unsigned int* dst,
    const float* input,
    unsigned int rows,
    unsigned int dim
) {
    __shared__ float best_values[256];
    __shared__ unsigned int best_indices[256];
    unsigned int tid = threadIdx.x;
    unsigned int row = rows == 0u ? 0u : rows - 1u;
    const float* row_ptr = input + row * dim;
    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0u;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float value = row_ptr[i];
        if (value > best_value || (value == best_value && i < best_index)) {
            best_value = value;
            best_index = i;
        }
    }
    best_values[tid] = best_value;
    best_indices[tid] = best_index;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float other_value = best_values[tid + stride];
            unsigned int other_index = best_indices[tid + stride];
            if (other_value > best_values[tid] || (other_value == best_values[tid] && other_index < best_indices[tid])) {
                best_values[tid] = other_value;
                best_indices[tid] = other_index;
            }
        }
        __syncthreads();
    }
    if (tid == 0u) dst[0] = best_indices[0];
}

extern "C" __global__ void termite_argmax_rows_f32(
    unsigned int* dst,
    const float* input,
    unsigned int row_start,
    unsigned int row_count,
    unsigned int dim
) {
    __shared__ float best_values[256];
    __shared__ unsigned int best_indices[256];
    unsigned int tid = threadIdx.x;
    unsigned int row_local = blockIdx.x;
    if (row_local >= row_count) return;
    const float* row_ptr = input + (row_start + row_local) * dim;
    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0u;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float value = row_ptr[i];
        if (value > best_value || (value == best_value && i < best_index)) {
            best_value = value;
            best_index = i;
        }
    }
    best_values[tid] = best_value;
    best_indices[tid] = best_index;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float other_value = best_values[tid + stride];
            unsigned int other_index = best_indices[tid + stride];
            if (other_value > best_values[tid] || (other_value == best_values[tid] && other_index < best_indices[tid])) {
                best_values[tid] = other_value;
                best_indices[tid] = other_index;
            }
        }
        __syncthreads();
    }
    if (tid == 0u) dst[row_local] = best_indices[0];
}

extern "C" __global__ void termite_argmax_rows_suppress_f32(
    unsigned int* dst,
    const float* input,
    const int* suppress_token_ids,
    unsigned int row_start,
    unsigned int row_count,
    unsigned int dim,
    unsigned int suppress_count
) {
    __shared__ float best_values[256];
    __shared__ unsigned int best_indices[256];
    unsigned int tid = threadIdx.x;
    unsigned int row_local = blockIdx.x;
    if (row_local >= row_count) return;
    const float* row_ptr = input + (row_start + row_local) * dim;
    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0u;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        bool suppressed = false;
        for (unsigned int j = 0; j < suppress_count; ++j) {
            int token_id = suppress_token_ids[j];
            if (token_id >= 0 && (unsigned int)token_id == i) {
                suppressed = true;
                break;
            }
        }
        if (suppressed) continue;
        float value = row_ptr[i];
        if (value > best_value || (value == best_value && i < best_index)) {
            best_value = value;
            best_index = i;
        }
    }
    best_values[tid] = best_value;
    best_indices[tid] = best_index;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float other_value = best_values[tid + stride];
            unsigned int other_index = best_indices[tid + stride];
            if (other_value > best_values[tid] || (other_value == best_values[tid] && other_index < best_indices[tid])) {
                best_values[tid] = other_value;
                best_indices[tid] = other_index;
            }
        }
        __syncthreads();
    }
    if (tid == 0u) dst[row_local] = best_indices[0];
}

extern "C" __global__ void termite_argmax_last_row_suppress_f32(
    unsigned int* dst,
    const float* input,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int dim,
    unsigned int suppress_count
) {
    __shared__ float best_values[256];
    __shared__ unsigned int best_indices[256];
    unsigned int tid = threadIdx.x;
    unsigned int row = rows == 0u ? 0u : rows - 1u;
    const float* row_ptr = input + row * dim;
    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0u;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        bool suppressed = false;
        for (unsigned int j = 0; j < suppress_count; ++j) {
            int token_id = suppress_token_ids[j];
            if (token_id >= 0 && (unsigned int)token_id == i) {
                suppressed = true;
                break;
            }
        }
        if (suppressed) continue;
        float value = row_ptr[i];
        if (value > best_value || (value == best_value && i < best_index)) {
            best_value = value;
            best_index = i;
        }
    }
    best_values[tid] = best_value;
    best_indices[tid] = best_index;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float other_value = best_values[tid + stride];
            unsigned int other_index = best_indices[tid + stride];
            if (other_value > best_values[tid] || (other_value == best_values[tid] && other_index < best_indices[tid])) {
                best_values[tid] = other_value;
                best_indices[tid] = other_index;
            }
        }
        __syncthreads();
    }
    if (tid == 0u) dst[0] = best_indices[0];
}

extern "C" __global__ void termite_gemma4_mtp_masked_argmax_f32(
    unsigned int* dst,
    const float* logits,
    const float* centroid_logits,
    const float* token_ordering,
    unsigned int vocab_size,
    unsigned int num_centroids,
    unsigned int top_k,
    unsigned int use_inverse_ordering
) {
    __shared__ float top_scores[128];
    __shared__ unsigned int top_centroids[128];
    __shared__ float block_values[256];
    __shared__ unsigned int block_indices[256];

    if (vocab_size == 0u || num_centroids == 0u || top_k == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }
    if (top_k > 128u) top_k = 128u;
    unsigned int cluster_size = vocab_size / num_centroids;
    if (cluster_size == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }

    if (threadIdx.x == 0u) {
        for (unsigned int i = 0u; i < top_k; ++i) {
            top_scores[i] = -3.402823466e+38f;
            top_centroids[i] = 0u;
        }
        for (unsigned int centroid = 0u; centroid < num_centroids; ++centroid) {
            float score = centroid_logits[centroid];
            unsigned int insert_at = top_k;
            for (unsigned int i = 0u; i < top_k; ++i) {
                if (score > top_scores[i]) {
                    insert_at = i;
                    break;
                }
            }
            if (insert_at == top_k) continue;
            for (unsigned int i = top_k - 1u; i > insert_at; --i) {
                top_scores[i] = top_scores[i - 1u];
                top_centroids[i] = top_centroids[i - 1u];
            }
            top_scores[insert_at] = score;
            top_centroids[insert_at] = centroid;
        }
    }
    __syncthreads();

    float best_score = -3.402823466e+38f;
    unsigned int best_token = 0u;
    if (use_inverse_ordering != 0u) {
        for (unsigned int token = threadIdx.x; token < vocab_size; token += blockDim.x) {
            float ordered_pos_f = token_ordering[token];
            if (ordered_pos_f < 0.0f) continue;
            unsigned int ordered_pos = (unsigned int)ordered_pos_f;
            unsigned int centroid = ordered_pos / cluster_size;
            bool selected = false;
            for (unsigned int i = 0u; i < top_k; ++i) {
                if (top_centroids[i] == centroid) {
                    selected = true;
                    break;
                }
            }
            if (!selected) continue;
            float score = logits[token];
            if (score > best_score || (score == best_score && token < best_token)) {
                best_score = score;
                best_token = token;
            }
        }
    } else {
        unsigned int candidate_count = top_k * cluster_size;
        for (unsigned int candidate = threadIdx.x; candidate < candidate_count; candidate += blockDim.x) {
            unsigned int top_slot = candidate / cluster_size;
            unsigned int offset = candidate - top_slot * cluster_size;
            unsigned int ordered_pos = top_centroids[top_slot] * cluster_size + offset;
            if (ordered_pos >= vocab_size) continue;
            float token_f = token_ordering[ordered_pos];
            if (token_f < 0.0f) continue;
            unsigned int token = (unsigned int)token_f;
            if (token >= vocab_size) continue;
            float score = logits[token];
            if (score > best_score || (score == best_score && token < best_token)) {
                best_score = score;
                best_token = token;
            }
        }
    }

    block_values[threadIdx.x] = best_score;
    block_indices[threadIdx.x] = best_token;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (threadIdx.x < stride) {
            float rhs_score = block_values[threadIdx.x + stride];
            unsigned int rhs_token = block_indices[threadIdx.x + stride];
            float lhs_score = block_values[threadIdx.x];
            unsigned int lhs_token = block_indices[threadIdx.x];
            if (rhs_score > lhs_score || (rhs_score == lhs_score && rhs_token < lhs_token)) {
                block_values[threadIdx.x] = rhs_score;
                block_indices[threadIdx.x] = rhs_token;
            }
        }
        __syncthreads();
    }
    if (threadIdx.x == 0u) dst[0] = block_indices[0];
}

__device__ bool termite_gemma4_mtp_token_is_eos_u32(unsigned int token, const int* eos_token_ids, unsigned int eos_count) {
    for (unsigned int i = 0u; i < eos_count; ++i) {
        int eos = eos_token_ids[i];
        if (eos >= 0 && token == (unsigned int)eos) return true;
    }
    return false;
}

extern "C" __global__ void termite_gemma4_mtp_verify_commit_u32(
    unsigned int* result,
    const unsigned int* target_choices,
    const long long* draft_tokens,
    const int* eos_token_ids,
    unsigned int draft_count,
    unsigned int eos_count,
    unsigned int accept_bonus
) {
    if (threadIdx.x != 0u || blockIdx.x != 0u) return;

    unsigned int matched_drafts = 0u;
    unsigned int accepted = 0u;
    unsigned int correction_added = 0u;
    unsigned int had_bonus = 0u;
    unsigned int bonus_skipped = 0u;
    unsigned int hit_eos = 0u;
    unsigned int correction_token = 0u;
    unsigned int bonus_token = 0u;
    unsigned int invalid_model_output = 0u;

    for (unsigned int i = 0u; i < draft_count; ++i) {
        long long draft_raw = draft_tokens[i];
        if (draft_raw < 0ll) {
            invalid_model_output = 1u;
            break;
        }
        unsigned int draft = (unsigned int)draft_raw;
        unsigned int target_choice = target_choices[i];
        if (target_choice == draft) {
            matched_drafts += 1u;
            accepted += 1u;
            if (termite_gemma4_mtp_token_is_eos_u32(target_choice, eos_token_ids, eos_count)) {
                hit_eos = 1u;
                break;
            }
        } else {
            correction_added = 1u;
            correction_token = target_choice;
            accepted = matched_drafts + 1u;
            if (termite_gemma4_mtp_token_is_eos_u32(target_choice, eos_token_ids, eos_count)) {
                hit_eos = 1u;
            }
            break;
        }
    }

    unsigned int can_bonus = (invalid_model_output == 0u && matched_drafts == draft_count && hit_eos == 0u) ? 1u : 0u;
    if (can_bonus != 0u && accept_bonus != 0u) {
        had_bonus = 1u;
        bonus_token = target_choices[draft_count];
        accepted += 1u;
        if (termite_gemma4_mtp_token_is_eos_u32(bonus_token, eos_token_ids, eos_count)) {
            hit_eos = 1u;
        }
    } else if (can_bonus != 0u) {
        bonus_skipped = 1u;
    }

    unsigned int commit_forward_required = (correction_added != 0u || had_bonus != 0u) ? 1u : 0u;
    unsigned int accepted_hidden_valid = (accepted != 0u && commit_forward_required == 0u) ? 1u : 0u;
    unsigned int accepted_hidden_row = accepted_hidden_valid != 0u ? accepted - 1u : 0xffffffffu;

    result[0] = matched_drafts;
    result[1] = accepted;
    result[2] = correction_added;
    result[3] = had_bonus;
    result[4] = bonus_skipped;
    result[5] = hit_eos;
    result[6] = commit_forward_required;
    result[7] = accepted_hidden_valid;
    result[8] = accepted_hidden_row;
    result[9] = correction_token;
    result[10] = bonus_token;
    result[11] = invalid_model_output;
}

extern "C" __global__ void termite_linear_bias_f32(
    float* dst,
    const float* input,
    const float* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    float acc = bias[col];
    for (unsigned int k = 0; k < in_dim; ++k) {
        acc += input[row * in_dim + k] * weight[col * in_dim + k];
    }
    dst[idx] = acc;
}

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

template <unsigned int ROWS_PER_BLOCK, unsigned int COLS, unsigned int MODE>
__device__ void termite_linear_bias_f32_tile_rows_cols(
    float* dst,
    const float* input,
    const float* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row_base = blockIdx.y * ROWS_PER_BLOCK;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[ROWS_PER_BLOCK][COLS][256];
    float acc[ROWS_PER_BLOCK][COLS];
    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) acc[r][c] = 0.0f;
    }

    for (unsigned int k = tid; k < in_dim; k += 256u) {
        float x[ROWS_PER_BLOCK];
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            unsigned int row = row_base + r;
            x[r] = row < rows ? input[row * in_dim + k] : 0.0f;
        }
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float w = weight[col * in_dim + k];
                #pragma unroll
                for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                    acc[r][c] += x[r] * w;
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] = acc[r][c];
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                #pragma unroll
                for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] += partial[r][c][tid + stride];
            }
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                float y = partial[r][c][0] + bias[col];
                if (MODE == 1u && y < 0.0f) y = 0.0f;
                if (MODE == 2u) y = 0.5f * y * (1.0f + tanhf(0.7978845608028654f * (y + 0.044715f * y * y * y)));
                unsigned int idx = row * out_dim + col;
                if (MODE == 3u) y += residual[idx];
                dst[idx] = y;
            }
        }
    }
}

extern "C" __global__ void termite_linear_bias_f32_tile4_r2(
    float* dst,
    const float* input,
    const float* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_linear_bias_f32_tile_rows_cols<2u, 4u, 0u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_bias_relu_f32_tile4_r2(
    float* dst,
    const float* input,
    const float* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_linear_bias_f32_tile_rows_cols<2u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_bias_gelu_f32_tile4_r2(
    float* dst,
    const float* input,
    const float* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_linear_bias_f32_tile_rows_cols<2u, 4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_bias_add_f32_tile4_r2(
    float* dst,
    const float* input,
    const float* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_linear_bias_f32_tile_rows_cols<2u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_pair_bias_f32_tile4_r2(
    float* dst_a,
    float* dst_b,
    const float* input,
    const float* weight_a,
    const float* bias_a,
    const float* weight_b,
    const float* bias_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row_base = blockIdx.y * 2u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial_a[2][4][256];
    __shared__ float partial_b[2][4][256];
    float acc_a[2][4];
    float acc_b[2][4];
    #pragma unroll
    for (unsigned int r = 0; r < 2u; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            acc_a[r][c] = 0.0f;
            acc_b[r][c] = 0.0f;
        }
    }

    for (unsigned int k = tid; k < in_dim; k += 256u) {
        float x[2];
        #pragma unroll
        for (unsigned int r = 0; r < 2u; ++r) {
            unsigned int row = row_base + r;
            x[r] = row < rows ? input[row * in_dim + k] : 0.0f;
        }
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float wa = weight_a[col * in_dim + k];
                float wb = weight_b[col * in_dim + k];
                #pragma unroll
                for (unsigned int r = 0; r < 2u; ++r) {
                    acc_a[r][c] += x[r] * wa;
                    acc_b[r][c] += x[r] * wb;
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int r = 0; r < 2u; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            partial_a[r][c][tid] = acc_a[r][c];
            partial_b[r][c][tid] = acc_b[r][c];
        }
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < 2u; ++r) {
                #pragma unroll
                for (unsigned int c = 0; c < 4u; ++c) {
                    partial_a[r][c][tid] += partial_a[r][c][tid + stride];
                    partial_b[r][c][tid] += partial_b[r][c][tid + stride];
                }
            }
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < 2u; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                unsigned int out_idx = row * out_dim + col;
                dst_a[out_idx] = partial_a[r][c][0] + bias_a[col];
                dst_b[out_idx] = partial_b[r][c][0] + bias_b[col];
            }
        }
    }
}

extern "C" __global__ void termite_linear_triple_bias_f32_tile4_r2(
    float* dst_a,
    float* dst_b,
    float* dst_c,
    const float* input,
    const float* weight_a,
    const float* bias_a,
    const float* weight_b,
    const float* bias_b,
    const float* weight_c,
    const float* bias_c,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row_base = blockIdx.y * 2u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial_a[2][4][256];
    __shared__ float partial_b[2][4][256];
    __shared__ float partial_c[2][4][256];
    float acc_a[2][4];
    float acc_b[2][4];
    float acc_c[2][4];
    #pragma unroll
    for (unsigned int r = 0; r < 2u; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            acc_a[r][c] = 0.0f;
            acc_b[r][c] = 0.0f;
            acc_c[r][c] = 0.0f;
        }
    }

    for (unsigned int k = tid; k < in_dim; k += 256u) {
        float x[2];
        #pragma unroll
        for (unsigned int r = 0; r < 2u; ++r) {
            unsigned int row = row_base + r;
            x[r] = row < rows ? input[row * in_dim + k] : 0.0f;
        }
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float wa = weight_a[col * in_dim + k];
                float wb = weight_b[col * in_dim + k];
                float wc = weight_c[col * in_dim + k];
                #pragma unroll
                for (unsigned int r = 0; r < 2u; ++r) {
                    acc_a[r][c] += x[r] * wa;
                    acc_b[r][c] += x[r] * wb;
                    acc_c[r][c] += x[r] * wc;
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int r = 0; r < 2u; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            partial_a[r][c][tid] = acc_a[r][c];
            partial_b[r][c][tid] = acc_b[r][c];
            partial_c[r][c][tid] = acc_c[r][c];
        }
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < 2u; ++r) {
                #pragma unroll
                for (unsigned int c = 0; c < 4u; ++c) {
                    partial_a[r][c][tid] += partial_a[r][c][tid + stride];
                    partial_b[r][c][tid] += partial_b[r][c][tid + stride];
                    partial_c[r][c][tid] += partial_c[r][c][tid + stride];
                }
            }
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < 2u; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                unsigned int out_idx = row * out_dim + col;
                dst_a[out_idx] = partial_a[r][c][0] + bias_a[col];
                dst_b[out_idx] = partial_b[r][c][0] + bias_b[col];
                dst_c[out_idx] = partial_c[r][c][0] + bias_c[col];
            }
        }
    }
}

extern "C" __global__ void termite_rms_norm_f32(
    float* dst,
    const float* input,
    const float* weight,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    const unsigned int base = row * dim;
    __shared__ float partial[256];
    float sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float scale = rsqrtf(partial[0] / (float)dim + eps);
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        dst[base + i] = input[base + i] * scale * weight[i];
    }
}

extern "C" __global__ void termite_rms_norm_add_f32(
    float* dst,
    const float* input,
    const float* weight,
    const float* residual,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    const unsigned int base = row * dim;
    __shared__ float partial[256];
    float sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float norm_scale = rsqrtf(partial[0] / (float)dim + eps);
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        unsigned int idx = base + i;
        dst[idx] = input[idx] * norm_scale * weight[i] + residual[idx];
    }
}

extern "C" __global__ void termite_rms_norm_add_mul_scalar_f32(
    float* dst,
    const float* input,
    const float* weight,
    const float* residual,
    const float* scalar,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    const unsigned int base = row * dim;
    __shared__ float partial[256];
    float sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float norm_scale = rsqrtf(partial[0] / (float)dim + eps);
    float output_scale = scalar[0];
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        unsigned int idx = base + i;
        dst[idx] = input[idx] * norm_scale * weight[i] * output_scale + residual[idx];
    }
}

extern "C" __global__ void termite_rms_norm_add_output_scale_f32(
    float* dst,
    const float* input,
    const float* weight,
    const float* residual,
    const float* scalar,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    const unsigned int base = row * dim;
    __shared__ float partial[256];
    float sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float norm_scale = rsqrtf(partial[0] / (float)dim + eps);
    float output_scale = scalar[0];
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        unsigned int idx = base + i;
        dst[idx] = (input[idx] * norm_scale * weight[i] + residual[idx]) * output_scale;
    }
}

extern "C" __global__ void termite_rms_norm_bare_f32(
    float* dst,
    const float* input,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    const unsigned int base = row * dim;
    __shared__ float partial[256];
    float sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float scale = rsqrtf(partial[0] / (float)dim + eps);
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        dst[base + i] = input[base + i] * scale;
    }
}

extern "C" __global__ void termite_layer_norm_f32(
    float* dst,
    const float* input,
    const float* gamma,
    const float* beta,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    const unsigned int base = row * dim;
    unsigned int tid = threadIdx.x;
    __shared__ float sums[256];
    __shared__ float sumsq[256];

    float local_sum = 0.0f;
    float local_sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = input[base + i];
        local_sum += x;
        local_sumsq += x * x;
    }
    sums[tid] = local_sum;
    sumsq[tid] = local_sumsq;
    __syncthreads();

    for (unsigned int stride = blockDim.x >> 1u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            sums[tid] += sums[tid + stride];
            sumsq[tid] += sumsq[tid + stride];
        }
        __syncthreads();
    }

    float mean = sums[0] / (float)dim;
    float var = fmaxf(sumsq[0] / (float)dim - mean * mean, 0.0f);
    float inv = rsqrtf(var + eps);
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = input[base + i];
        dst[base + i] = (x - mean) * inv * gamma[i] + beta[i];
    }
}

extern "C" __global__ void termite_add_layer_norm_f32(
    float* dst,
    const float* a,
    const float* b,
    const float* gamma,
    const float* beta,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= rows) return;
    const unsigned int base = row * dim;
    unsigned int tid = threadIdx.x;
    __shared__ float sums[256];
    __shared__ float sumsq[256];

    float local_sum = 0.0f;
    float local_sumsq = 0.0f;
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = a[base + i] + b[base + i];
        local_sum += x;
        local_sumsq += x * x;
    }
    sums[tid] = local_sum;
    sumsq[tid] = local_sumsq;
    __syncthreads();

    for (unsigned int stride = blockDim.x >> 1u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            sums[tid] += sums[tid + stride];
            sumsq[tid] += sumsq[tid + stride];
        }
        __syncthreads();
    }

    float mean = sums[0] / (float)dim;
    float var = fmaxf(sumsq[0] / (float)dim - mean * mean, 0.0f);
    float inv = rsqrtf(var + eps);
    for (unsigned int i = tid; i < dim; i += blockDim.x) {
        float x = a[base + i] + b[base + i];
        dst[base + i] = (x - mean) * inv * gamma[i] + beta[i];
    }
}

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
        out = x / (1.0f + expf(-x));
    } else if (op == 3u) {
        out = 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    } else if (op == 4u) {
        out = fmaxf(x, 0.0f);
    } else if (op == 5u) {
        out = x / (1.0f + expf(-1.702f * x));
    } else if (op == 6u) {
        out = 1.0f / (1.0f + expf(-x));
    } else {
        out = tanhf(x);
    }
    dst[i] = out;
}

extern "C" __global__ void termite_silu_multiply_f32(
    float* dst,
    const float* gate,
    const float* up,
    unsigned int count
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float x = gate[i];
    dst[i] = (x / (1.0f + expf(-x))) * up[i];
}

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
    float act;
    if (activation == 0u) {
        act = 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    } else if (activation == 1u) {
        act = 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    } else if (activation == 2u) {
        act = x / (1.0f + expf(-x));
    } else if (activation == 3u) {
        act = fmaxf(x, 0.0f);
    } else if (activation == 4u) {
        act = x / (1.0f + expf(-1.702f * x));
    } else {
        float r = fmaxf(x, 0.0f);
        act = r * r;
    }
    dst[i] = act * up[i];
}

__device__ __forceinline__ float termite_decoder_activation_f32(float x, unsigned int activation) {
    if (activation == 0u) {
        return 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    } else if (activation == 1u) {
        return 0.5f * x * (1.0f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    } else if (activation == 2u) {
        return x / (1.0f + expf(-x));
    } else if (activation == 3u) {
        return fmaxf(x, 0.0f);
    } else if (activation == 4u) {
        return x / (1.0f + expf(-1.702f * x));
    }
    float r = fmaxf(x, 0.0f);
    return r * r;
}

extern "C" __global__ void termite_embedding_lookup_f32(
    float* dst,
    const float* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    long long id = ids[row];
    dst[idx] = weight[(unsigned long long)id * dim + col];
}

extern "C" __global__ void termite_embedding_lookup_bf16_weight_f32(
    float* dst,
    const unsigned short* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    long long id = ids[row];
    dst[idx] = termite_bf16_to_f32(weight[(unsigned long long)id * dim + col]);
}

extern "C" __global__ void termite_embedding_lookup_i32_f32(
    float* dst,
    const float* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    int id = ids[row];
    dst[idx] = weight[(unsigned long long)((unsigned int)id) * dim + col];
}

extern "C" __global__ void termite_take_rows_f32(
    float* dst,
    const float* input,
    const unsigned int* row_ids,
    unsigned int source_rows,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    unsigned int src_row = row_ids[row];
    dst[idx] = src_row < source_rows ? input[src_row * dim + col] : 0.0f;
}

extern "C" __global__ void termite_gliner_gather_concat_relu_f32(
    float* dst,
    const float* start,
    const float* end,
    const unsigned int* start_rows,
    const unsigned int* end_rows,
    unsigned int source_rows,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int out_dim = dim * 2u;
    unsigned int count = rows * out_dim;
    if (idx >= count) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    bool is_end = col >= dim;
    unsigned int inner_col = is_end ? col - dim : col;
    unsigned int src_row = is_end ? end_rows[row] : start_rows[row];
    const float* src = is_end ? end : start;
    float y = src_row < source_rows ? src[src_row * dim + inner_col] : 0.0f;
    dst[idx] = y < 0.0f ? 0.0f : y;
}

extern "C" __global__ void termite_slice_last_dim_f32(
    float* dst,
    const float* input,
    unsigned int rows,
    unsigned int cols,
    unsigned int start,
    unsigned int out_cols
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * out_cols;
    if (idx >= count) return;
    unsigned int row = idx / out_cols;
    unsigned int out_col = idx - row * out_cols;
    dst[idx] = input[row * cols + start + out_col];
}

extern "C" __global__ void termite_gliner_word_embeddings_f32(
    float* dst,
    const float* hidden,
    const long long* words_mask,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int hidden_size,
    unsigned int num_words
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int out_count = batch * num_words * hidden_size;
    if (idx >= out_count) return;

    unsigned int d = idx % hidden_size;
    unsigned int tmp = idx / hidden_size;
    unsigned int word = tmp % num_words;
    unsigned int b = tmp / num_words;
    long long wanted = (long long)word + 1ll;

    unsigned int token_base = b * seq_len;
    unsigned int hidden_base = token_base * hidden_size + d;
    for (unsigned int t = 0; t < seq_len; ++t) {
        if (words_mask[token_base + t] == wanted) {
            dst[idx] = hidden[hidden_base + t * hidden_size];
            return;
        }
    }
    dst[idx] = 0.0f;
}

extern "C" __global__ void termite_repeat_first_row_f32(
    float* dst,
    const float* src,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * dim;
    if (idx >= count) return;
    unsigned int col = idx % dim;
    dst[idx] = src[col];
}

extern "C" __global__ void termite_gliner_gru_combine_f32(
    float* dst,
    const float* label_embeddings,
    const float* gi,
    const float* gh,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    unsigned int gate_base = row * dim * 3u + col;
    float r = 1.0f / (1.0f + expf(-(gi[gate_base] + gh[gate_base])));
    float z = 1.0f / (1.0f + expf(-(gi[gate_base + dim] + gh[gate_base + dim])));
    float n = tanhf(gi[gate_base + dim * 2u] + r * gh[gate_base + dim * 2u]);
    float h0 = label_embeddings[idx];
    float h1 = (1.0f - z) * n + z * h0;
    dst[idx] = h1 + h0;
}

extern "C" __global__ void termite_concat_lastdim_f32(
    float* dst,
    const float* a,
    const float* b,
    unsigned int total,
    unsigned int dim_a,
    unsigned int dim_b
) {
    unsigned int out_dim = dim_a + dim_b;
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * out_dim;
    if (idx >= count) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    if (col < dim_a) {
        dst[idx] = a[row * dim_a + col];
    } else {
        dst[idx] = b[row * dim_b + (col - dim_a)];
    }
}

extern "C" __global__ void termite_conv2d_f32(
    float* dst,
    const float* input,
    const float* weight,
    const float* bias,
    unsigned int batch,
    unsigned int in_channels,
    unsigned int out_channels,
    unsigned int height,
    unsigned int width,
    unsigned int kernel_h,
    unsigned int kernel_w,
    unsigned int stride_h,
    unsigned int stride_w,
    unsigned int padding_h,
    unsigned int padding_w,
    unsigned int groups,
    unsigned int out_h,
    unsigned int out_w
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * out_channels * out_h * out_w;
    if (idx >= total) return;
    unsigned int ox = idx % out_w;
    unsigned int tmp = idx / out_w;
    unsigned int oy = tmp % out_h;
    tmp /= out_h;
    unsigned int oc = tmp % out_channels;
    unsigned int b = tmp / out_channels;
    unsigned int in_per_group = in_channels / groups;
    unsigned int out_per_group = out_channels / groups;
    unsigned int group = oc / out_per_group;
    float acc = bias[oc];
    for (unsigned int ig = 0; ig < in_per_group; ++ig) {
        unsigned int ic = group * in_per_group + ig;
        for (unsigned int ky = 0; ky < kernel_h; ++ky) {
            int iy = (int)(oy * stride_h + ky) - (int)padding_h;
            if (iy < 0 || iy >= (int)height) continue;
            for (unsigned int kx = 0; kx < kernel_w; ++kx) {
                int ix = (int)(ox * stride_w + kx) - (int)padding_w;
                if (ix < 0 || ix >= (int)width) continue;
                unsigned int x_idx = ((b * in_channels + ic) * height + (unsigned int)iy) * width + (unsigned int)ix;
                unsigned int w_idx = (((oc * in_per_group + ig) * kernel_h + ky) * kernel_w) + kx;
                acc += input[x_idx] * weight[w_idx];
            }
        }
    }
    dst[idx] = acc;
}

extern "C" __global__ void termite_attention_f32(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const long long* mask,
    const float* bias,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int causal,
    unsigned int has_mask,
    unsigned int bias_mode,
    unsigned int head_major
) {
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
        if (causal && ki > qi) continue;
        if (has_mask && mask[b * seq_len + ki] == 0ll) continue;
        float score = 0.0f;
        unsigned int q_base = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim : (b * seq_len + qi) * hidden + head * head_dim;
        unsigned int k_base = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim : (b * seq_len + ki) * hidden + head * head_dim;
        for (unsigned int j = 0; j < head_dim; ++j) score += q[q_base + j] * k[k_base + j];
        score *= scale;
        if (bias_mode == 1u) score += bias[(head * seq_len + qi) * seq_len + ki];
        if (bias_mode == 2u) score += bias[((b * num_heads + head) * seq_len + qi) * seq_len + ki];
        max_score = fmaxf(max_score, score);
    }
    float denom = 0.0f;
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < seq_len; ++ki) {
        if (causal && ki > qi) continue;
        if (has_mask && mask[b * seq_len + ki] == 0ll) continue;
        float score = 0.0f;
        unsigned int q_base = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim : (b * seq_len + qi) * hidden + head * head_dim;
        unsigned int k_base = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim : (b * seq_len + ki) * hidden + head * head_dim;
        for (unsigned int j = 0; j < head_dim; ++j) score += q[q_base + j] * k[k_base + j];
        score *= scale;
        if (bias_mode == 1u) score += bias[(head * seq_len + qi) * seq_len + ki];
        if (bias_mode == 2u) score += bias[((b * num_heads + head) * seq_len + qi) * seq_len + ki];
        float e = expf(score - max_score);
        denom += e;
        unsigned int v_idx = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim + d : (b * seq_len + ki) * hidden + head * head_dim + d;
        acc += e * v[v_idx];
    }
    dst[idx] = denom > 0.0f ? acc / denom : 0.0f;
}

extern "C" __global__ void termite_attention_f32_block(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const long long* mask,
    const float* bias,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int causal,
    unsigned int has_mask,
    unsigned int bias_mode,
    unsigned int head_major
) {
    unsigned int row_id = blockIdx.x;
    unsigned int total_rows = batch * seq_len * num_heads;
    if (row_id >= total_rows || seq_len > 512u || head_dim > 128u) return;
    unsigned int head = row_id % num_heads;
    unsigned int tmp = row_id / num_heads;
    unsigned int qi = tmp % seq_len;
    unsigned int b = tmp / seq_len;
    unsigned int tid = threadIdx.x;
    unsigned int hidden = num_heads * head_dim;
    __shared__ float scratch[128];
    __shared__ float scores[512];
    float scale = rsqrtf((float)head_dim);
    unsigned int q_base = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim : (b * seq_len + qi) * hidden + head * head_dim;

    float max_score = -3.402823466e+38f;
    for (unsigned int ki = 0; ki < seq_len; ++ki) {
        bool valid = !(causal && ki > qi) && !(has_mask && mask[b * seq_len + ki] == 0ll);
        float part = 0.0f;
        if (valid) {
            unsigned int k_base = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim : (b * seq_len + ki) * hidden + head * head_dim;
            for (unsigned int d = tid; d < head_dim; d += blockDim.x) part += q[q_base + d] * k[k_base + d];
        }
        scratch[tid] = part;
        __syncthreads();
        for (unsigned int stride = 64u; stride > 0u; stride >>= 1u) {
            if (tid < stride) scratch[tid] += scratch[tid + stride];
            __syncthreads();
        }
        if (tid == 0u) {
            float score = valid ? scratch[0] * scale : -3.402823466e+38f;
            if (valid && bias_mode == 1u) score += bias[(head * seq_len + qi) * seq_len + ki];
            if (valid && bias_mode == 2u) score += bias[((b * num_heads + head) * seq_len + qi) * seq_len + ki];
            scores[ki] = score;
            max_score = fmaxf(max_score, score);
        }
        __syncthreads();
    }

    __shared__ float shared_max;
    __shared__ float shared_denom;
    if (tid == 0u) shared_max = max_score;
    __syncthreads();

    float denom_part = 0.0f;
    for (unsigned int ki = tid; ki < seq_len; ki += blockDim.x) {
        float e = expf(scores[ki] - shared_max);
        scores[ki] = e;
        denom_part += e;
    }
    scratch[tid] = denom_part;
    __syncthreads();
    for (unsigned int stride = 64u; stride > 0u; stride >>= 1u) {
        if (tid < stride) scratch[tid] += scratch[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) shared_denom = scratch[0];
    __syncthreads();

    for (unsigned int d = tid; d < head_dim; d += blockDim.x) {
        float acc = 0.0f;
        for (unsigned int ki = 0; ki < seq_len; ++ki) {
            unsigned int v_idx = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim + d : (b * seq_len + ki) * hidden + head * head_dim + d;
            acc += scores[ki] * v[v_idx];
        }
        unsigned int out_idx = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim + d : (b * seq_len + qi) * hidden + head * head_dim + d;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}

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
    for (unsigned int ki = 0; ki < enc_seq; ++ki) {
        if (mask != nullptr && mask[b * enc_seq + ki] == 0ll) continue;
        unsigned int k_base = (b * enc_seq + ki) * hidden + head * head_dim;
        float score = 0.0f;
        for (unsigned int j = 0; j < head_dim; ++j) {
            score += q[q_base + j] * k[k_base + j];
        }
        score *= scale;
        max_score = fmaxf(max_score, score);
    }

    float denom = 0.0f;
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < enc_seq; ++ki) {
        if (mask != nullptr && mask[b * enc_seq + ki] == 0ll) continue;
        unsigned int k_base = (b * enc_seq + ki) * hidden + head * head_dim;
        float score = 0.0f;
        for (unsigned int j = 0; j < head_dim; ++j) {
            score += q[q_base + j] * k[k_base + j];
        }
        score *= scale;
        float e = expf(score - max_score);
        denom += e;
        unsigned int v_idx = (b * enc_seq + ki) * hidden + head * head_dim + d;
        acc += e * v[v_idx];
    }
    dst[idx] = denom > 0.0f ? acc / denom : 0.0f;
}

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

extern "C" __global__ void termite_token_to_nchw_f32(
    float* dst,
    const float* src,
    unsigned int batch,
    unsigned int channels,
    unsigned int height,
    unsigned int width
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * height * width * channels;
    if (idx >= total) return;
    unsigned int c = idx % channels;
    unsigned int tmp = idx / channels;
    unsigned int x = tmp % width;
    tmp /= width;
    unsigned int y = tmp % height;
    unsigned int b = tmp / height;
    unsigned int dst_idx = ((b * channels + c) * height + y) * width + x;
    dst[dst_idx] = src[idx];
}

extern "C" __global__ void termite_nchw_to_token_f32(
    float* dst,
    const float* src,
    unsigned int batch,
    unsigned int channels,
    unsigned int height,
    unsigned int width
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * height * width * channels;
    if (idx >= total) return;
    unsigned int c = idx % channels;
    unsigned int tmp = idx / channels;
    unsigned int x = tmp % width;
    tmp /= width;
    unsigned int y = tmp % height;
    unsigned int b = tmp / height;
    unsigned int src_idx = ((b * channels + c) * height + y) * width + x;
    dst[idx] = src[src_idx];
}

extern "C" __global__ void termite_pack_windows_f32(
    float* dst,
    const float* src,
    unsigned int batch,
    unsigned int height,
    unsigned int width,
    unsigned int dim,
    unsigned int window_size,
    unsigned int padded_h,
    unsigned int padded_w
) {
    unsigned int windows_h = padded_h / window_size;
    unsigned int windows_w = padded_w / window_size;
    unsigned int window_area = window_size * window_size;
    unsigned int window_count = batch * windows_h * windows_w;
    unsigned int total = window_count * window_area * dim;
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= total) return;

    unsigned int feature = idx % dim;
    unsigned int token = (idx / dim) % window_area;
    unsigned int window_idx = idx / (dim * window_area);
    unsigned int dx = token % window_size;
    unsigned int dy = token / window_size;
    unsigned int ww = window_idx % windows_w;
    unsigned int tmp = window_idx / windows_w;
    unsigned int wh = tmp % windows_h;
    unsigned int b = tmp / windows_h;
    unsigned int src_y = wh * window_size + dy;
    unsigned int src_x = ww * window_size + dx;
    if (src_y >= height || src_x >= width) {
        dst[idx] = 0.0f;
        return;
    }
    unsigned int src_idx = (b * height * width + src_y * width + src_x) * dim + feature;
    dst[idx] = src[src_idx];
}

extern "C" __global__ void termite_unpad_windows_f32(
    float* dst,
    const float* src,
    unsigned int batch,
    unsigned int height,
    unsigned int width,
    unsigned int dim,
    unsigned int window_size,
    unsigned int padded_h,
    unsigned int padded_w
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batch * height * width * dim;
    if (idx >= total) return;

    unsigned int feature = idx % dim;
    unsigned int tmp = idx / dim;
    unsigned int x = tmp % width;
    tmp /= width;
    unsigned int y = tmp % height;
    unsigned int b = tmp / height;
    unsigned int windows_w = padded_w / window_size;
    unsigned int wh = y / window_size;
    unsigned int ww = x / window_size;
    unsigned int dy = y % window_size;
    unsigned int dx = x % window_size;
    unsigned int window_idx = (b * (padded_h / window_size) + wh) * windows_w + ww;
    unsigned int token = dy * window_size + dx;
    unsigned int src_idx = (window_idx * window_size * window_size + token) * dim + feature;
    dst[idx] = src[src_idx];
}

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

__device__ float termite_load_tail_weight_f32(const void* ptr, unsigned int dtype, unsigned int offset) {
    if (dtype == 1u) return __half2float(reinterpret_cast<const __half*>(ptr)[offset]);
    if (dtype == 2u) return __bfloat162float(reinterpret_cast<const __nv_bfloat16*>(ptr)[offset]);
    return reinterpret_cast<const float*>(ptr)[offset];
}

extern "C" __global__ void termite_florence_vision_tail_sources_f32(
    float* dst,
    const float* tokens,
    const void* row_embed,
    const void* col_embed,
    const void* temporal_embed,
    unsigned int batch,
    unsigned int height,
    unsigned int width,
    unsigned int dim,
    unsigned int has_temporal,
    unsigned int row_dtype,
    unsigned int col_dtype,
    unsigned int temporal_dtype
) {
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

__device__ float termite_rope_frequency(unsigned int pair_index, unsigned int rope_dim, float theta) {
    return 1.0f / powf(theta, (2.0f * (float)pair_index) / (float)rope_dim);
}

__device__ unsigned int termite_rope_pair_info(
    unsigned int d,
    unsigned int head_dim,
    unsigned int rope_dim,
    unsigned int consecutive_pairs,
    unsigned int* idx0,
    unsigned int* idx1,
    unsigned int* pair_index,
    unsigned int* second
) {
    if (rope_dim < 2u || rope_dim > head_dim) return 0u;

    if (consecutive_pairs) {
        if (d >= rope_dim) return 0u;
        *pair_index = d >> 1u;
        *idx0 = *pair_index << 1u;
        *idx1 = *idx0 + 1u;
        *second = d & 1u;
        return *idx1 < head_dim;
    }

    unsigned int active_pairs = rope_dim >> 1u;
    unsigned int head_half = head_dim >> 1u;
    if (head_half == 0u) return 0u;
    if (d < active_pairs) {
        *pair_index = d;
        *idx0 = d;
        *idx1 = d + head_half;
        *second = 0u;
        return *idx1 < head_dim;
    }
    if (d >= head_half && d < head_half + active_pairs) {
        *pair_index = d - head_half;
        *idx0 = *pair_index;
        *idx1 = d;
        *second = 1u;
        return *idx1 < head_dim;
    }
    return 0u;
}

__device__ float termite_rope_value(
    const float* input,
    unsigned int base,
    unsigned int d,
    unsigned int head_dim,
    unsigned int rope_dim,
    unsigned int position,
    float theta,
    float freq_scale,
    unsigned int consecutive_pairs
) {
    unsigned int idx0;
    unsigned int idx1;
    unsigned int pair_index;
    unsigned int second;
    if (!termite_rope_pair_info(d, head_dim, rope_dim, consecutive_pairs, &idx0, &idx1, &pair_index, &second)) return input[base + d];
    float angle = ((float)position) * freq_scale * termite_rope_frequency(pair_index, rope_dim, theta);
    float s = sinf(angle);
    float c = cosf(angle);
    float x0 = input[base + idx0];
    float x1 = input[base + idx1];
    return second ? (x0 * s + x1 * c) : (x0 * c - x1 * s);
}

extern "C" __global__ void termite_rope_f32(
    float* dst,
    const float* input,
    unsigned int total_chunks,
    unsigned int head_dim,
    unsigned int rope_dim,
    float theta,
    float freq_scale,
    unsigned int position_offset,
    unsigned int seq_len,
    unsigned int chunks_per_position,
    unsigned int consecutive_pairs
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = total_chunks * head_dim;
    if (idx >= total) return;
    unsigned int chunk = idx / head_dim;
    unsigned int d = idx - chunk * head_dim;
    unsigned int token_pos = (chunk / chunks_per_position) % seq_len;
    unsigned int position = position_offset + token_pos;
    unsigned int base = chunk * head_dim;
    dst[idx] = termite_rope_value(input, base, d, head_dim, rope_dim, position, theta, freq_scale, consecutive_pairs);
}

extern "C" __global__ void termite_rope_scaled_f32(
    float* dst,
    const float* input,
    unsigned int total_chunks,
    unsigned int head_dim,
    unsigned int rope_dim,
    float theta,
    float freq_scale,
    unsigned int position_offset,
    unsigned int seq_len,
    unsigned int chunks_per_position,
    unsigned int consecutive_pairs,
    float scale
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = total_chunks * head_dim;
    if (idx >= total) return;
    unsigned int chunk = idx / head_dim;
    unsigned int d = idx - chunk * head_dim;
    unsigned int token_pos = (chunk / chunks_per_position) % seq_len;
    unsigned int position = position_offset + token_pos;
    unsigned int base = chunk * head_dim;
    float value = termite_rope_value(input, base, d, head_dim, rope_dim, position, theta, freq_scale, consecutive_pairs);
    dst[idx] = value * scale;
}

extern "C" __global__ void termite_rope_per_item_f32(
    float* dst,
    const float* input,
    const unsigned int* query_lengths,
    const unsigned int* position_offsets,
    unsigned int batch,
    unsigned int max_seq_len,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int rope_dim,
    float theta,
    float freq_scale,
    unsigned int consecutive_pairs
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total_chunks = batch * max_seq_len * num_heads;
    unsigned int total = total_chunks * head_dim;
    if (idx >= total) return;
    unsigned int chunk = idx / head_dim;
    unsigned int d = idx - chunk * head_dim;
    unsigned int token = chunk / num_heads;
    unsigned int item = token / max_seq_len;
    unsigned int pos = token - item * max_seq_len;
    unsigned int position = 0u;
    if (item < batch && pos < query_lengths[item]) {
        position = position_offsets[item] + pos;
    }
    unsigned int base = chunk * head_dim;
    dst[idx] = termite_rope_value(input, base, d, head_dim, rope_dim, position, theta, freq_scale, consecutive_pairs);
}

extern "C" __global__ void termite_rms_norm_heads_rope_f32(
    float* dst,
    const float* input,
    const float* weight,
    unsigned int total_chunks,
    unsigned int head_dim,
    unsigned int rope_dim,
    float eps,
    float theta,
    float freq_scale,
    unsigned int position_offset,
    unsigned int seq_len,
    unsigned int chunks_per_position,
    unsigned int consecutive_pairs,
    float output_scale
) {
    unsigned int chunk = blockIdx.x;
    unsigned int tid = threadIdx.x;
    if (chunk >= total_chunks || head_dim == 0u) return;
    unsigned int base = chunk * head_dim;

    float sumsq = 0.0f;
    for (unsigned int i = tid; i < head_dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    __shared__ float partial[256];
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float norm_scale = rsqrtf(partial[0] / (float)head_dim + eps);

    for (unsigned int d = tid; d < head_dim; d += blockDim.x) {
        float value = input[base + d] * norm_scale * weight[d];
        unsigned int idx0;
        unsigned int idx1;
        unsigned int pair_index;
        unsigned int second;
        if (termite_rope_pair_info(d, head_dim, rope_dim, consecutive_pairs, &idx0, &idx1, &pair_index, &second)) {
            unsigned int token_pos = (chunk / chunks_per_position) % seq_len;
            unsigned int position = position_offset + token_pos;
            float angle = ((float)position) * freq_scale * termite_rope_frequency(pair_index, rope_dim, theta);
            float s = sinf(angle);
            float c = cosf(angle);
            float x0 = input[base + idx0] * norm_scale * weight[idx0];
            float x1 = input[base + idx1] * norm_scale * weight[idx1];
            value = second ? (x0 * s + x1 * c) : (x0 * c - x1 * s);
        }
        dst[base + d] = value * output_scale;
    }
}

extern "C" __global__ void termite_rms_norm_heads_rope_decode_scalars_f32(
    float* dst,
    const float* input,
    const float* weight,
    unsigned int total_chunks,
    unsigned int head_dim,
    unsigned int rope_dim,
    float eps,
    float theta,
    float freq_scale,
    unsigned int position_offset,
    unsigned int seq_len,
    unsigned int chunks_per_position,
    unsigned int consecutive_pairs,
    float output_scale,
    const unsigned int* decode_scalars
) {
    unsigned int chunk = blockIdx.x;
    unsigned int tid = threadIdx.x;
    if (chunk >= total_chunks || head_dim == 0u) return;
    if (decode_scalars != 0) position_offset = decode_scalars[0];
    unsigned int base = chunk * head_dim;

    float sumsq = 0.0f;
    for (unsigned int i = tid; i < head_dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    __shared__ float partial[256];
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float norm_scale = rsqrtf(partial[0] / (float)head_dim + eps);

    for (unsigned int d = tid; d < head_dim; d += blockDim.x) {
        float value = input[base + d] * norm_scale * weight[d];
        unsigned int idx0;
        unsigned int idx1;
        unsigned int pair_index;
        unsigned int second;
        if (termite_rope_pair_info(d, head_dim, rope_dim, consecutive_pairs, &idx0, &idx1, &pair_index, &second)) {
            unsigned int token_pos = (chunk / chunks_per_position) % seq_len;
            unsigned int position = position_offset + token_pos;
            float angle = ((float)position) * freq_scale * termite_rope_frequency(pair_index, rope_dim, theta);
            float s = sinf(angle);
            float c = cosf(angle);
            float x0 = input[base + idx0] * norm_scale * weight[idx0];
            float x1 = input[base + idx1] * norm_scale * weight[idx1];
            value = second ? (x0 * s + x1 * c) : (x0 * c - x1 * s);
        }
        dst[base + d] = value * output_scale;
    }
}

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
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        if (!valid) continue;

        unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
        unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
        float score = 0.0f;
        for (unsigned int j = 0; j < head_dim; ++j) score += q[q_base + j] * k[k_base + j];
        score *= scale;
        if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
        if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
        max_score = fmaxf(max_score, score);
    }

    float denom = 0.0f;
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        if (!valid) continue;

        unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
        unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
        float score = 0.0f;
        for (unsigned int j = 0; j < head_dim; ++j) score += q[q_base + j] * k[k_base + j];
        score *= scale;
        if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
        if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
        float e = expf(score - max_score);
        denom += e;
        unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + d;
        acc += e * v[v_idx];
    }

    dst[idx] = denom > 0.0f ? acc / denom : 0.0f;
}

extern "C" __global__ void termite_gqa_attention_decode_f32(
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
    __shared__ float reduce[512];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    unsigned int lane = threadIdx.x;
    unsigned int block = blockIdx.x;
    unsigned int total_blocks = batch * q_seq_len * num_heads;
    if (block >= total_blocks || head_dim > 512u || blockDim.x < head_dim || num_kv_heads == 0u || (num_heads % num_kv_heads) != 0u) return;

    unsigned int head = block % num_heads;
    unsigned int tmp = block / num_heads;
    unsigned int qi = tmp % q_seq_len;
    unsigned int b = tmp / q_seq_len;
    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int kv_hidden = num_kv_heads * head_dim;
    unsigned int query_pos = query_position_offset + qi;
    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) shared_max_score = -3.402823466e+38f;
    __syncthreads();

    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        reduce[lane] = partial;
        __syncthreads();
        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
            if (lane < stride) reduce[lane] += reduce[lane + stride];
            __syncthreads();
        }
        if (lane == 0u && valid) {
            float score = reduce[0] * scale;
            if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
            if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
            shared_max_score = fmaxf(shared_max_score, score);
        }
        __syncthreads();
    }

    if (lane == 0u) shared_denom = 0.0f;
    __syncthreads();
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        reduce[lane] = partial;
        __syncthreads();
        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
            if (lane < stride) reduce[lane] += reduce[lane + stride];
            __syncthreads();
        }
        float e = 0.0f;
        if (valid) {
            float score = reduce[0] * scale;
            if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
            if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
            e = expf(score - shared_max_score);
        }
        if (lane == 0u) shared_denom += e;
        if (lane < head_dim) {
            unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + lane;
            acc += e * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}

extern "C" __global__ void termite_gqa_attention_decode_scalars_f32(
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
    unsigned int bias_mode,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }

    __shared__ float reduce[512];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    unsigned int lane = threadIdx.x;
    unsigned int block = blockIdx.x;
    unsigned int total_blocks = batch * q_seq_len * num_heads;
    if (block >= total_blocks || head_dim > 512u || blockDim.x < head_dim || num_kv_heads == 0u || (num_heads % num_kv_heads) != 0u) return;

    unsigned int head = block % num_heads;
    unsigned int tmp = block / num_heads;
    unsigned int qi = tmp % q_seq_len;
    unsigned int b = tmp / q_seq_len;
    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int kv_hidden = num_kv_heads * head_dim;
    unsigned int query_pos = query_position_offset + qi;
    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) shared_max_score = -3.402823466e+38f;
    __syncthreads();

    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        reduce[lane] = partial;
        __syncthreads();
        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
            if (lane < stride) reduce[lane] += reduce[lane + stride];
            __syncthreads();
        }
        if (lane == 0u && valid) {
            float score = reduce[0] * scale;
            if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
            if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
            shared_max_score = fmaxf(shared_max_score, score);
        }
        __syncthreads();
    }

    if (lane == 0u) shared_denom = 0.0f;
    __syncthreads();
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        bool future_blocked = key_pos > query_pos && !future_allowed;
        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        bool valid = !(future_blocked || past_blocked);
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        reduce[lane] = partial;
        __syncthreads();
        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
            if (lane < stride) reduce[lane] += reduce[lane + stride];
            __syncthreads();
        }
        float e = 0.0f;
        if (valid) {
            float score = reduce[0] * scale;
            if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
            if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
            e = expf(score - shared_max_score);
        }
        if (lane == 0u) shared_denom += e;
        if (lane < head_dim) {
            unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + lane;
            acc += e * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}

extern "C" __global__ void termite_kv_write_suffix_decode_scalars_f32(
    float* k_dst,
    float* v_dst,
    const float* k_src,
    const float* v_src,
    const unsigned int* decode_scalars,
    unsigned int suffix_token_count,
    unsigned int row_width,
    unsigned int fallback_total_token_count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = suffix_token_count * row_width;
    if (idx >= total || suffix_token_count == 0u || row_width == 0u) return;
    unsigned int total_token_count = fallback_total_token_count;
    if (decode_scalars != 0) total_token_count = decode_scalars[2];
    if (total_token_count < suffix_token_count) return;
    unsigned int token_start = total_token_count - suffix_token_count;
    unsigned int dst_idx = token_start * row_width + idx;
    k_dst[dst_idx] = k_src[idx];
    v_dst[dst_idx] = v_src[idx];
}

__device__ __forceinline__ unsigned char termite_tq_encode_polar4_scalar(float value) {
    float clipped = fminf(fmaxf(value, -1.0f), 1.0f);
    float scaled = roundf((clipped + 1.0f) * 7.5f);
    return (unsigned char)fminf(fmaxf(scaled, 0.0f), 15.0f);
}

__device__ __forceinline__ float termite_tq_decode_polar4_scalar(unsigned char code) {
    return ((float)(code & 0x0fu) / 7.5f) - 1.0f;
}

__device__ __forceinline__ float termite_tq_decode_polar4_at(const unsigned char* encoded, unsigned int value_index) {
    unsigned char packed = encoded[value_index >> 1];
    unsigned char code = (value_index & 1u) == 0u ? (packed & 0x0fu) : ((packed >> 4) & 0x0fu);
    return termite_tq_decode_polar4_scalar(code);
}

__device__ __forceinline__ unsigned char termite_tq_encode_turbo3_scalar(float value) {
    float clipped = fminf(fmaxf(value, -1.0f), 1.0f);
    float scaled = roundf((clipped + 1.0f) * 3.5f);
    return (unsigned char)fminf(fmaxf(scaled, 0.0f), 7.0f);
}

__device__ __forceinline__ float termite_tq_decode_turbo3_scalar(unsigned char code) {
    return ((float)(code & 0x07u) / 3.5f) - 1.0f;
}

__device__ __forceinline__ unsigned char termite_tq_get_packed3(const unsigned char* encoded, unsigned int value_index) {
    unsigned int bit_offset = value_index * 3u;
    unsigned int byte_index = bit_offset >> 3;
    unsigned int shift = bit_offset & 7u;
    unsigned int bits = ((unsigned int)encoded[byte_index]) >> shift;
    if (shift > 5u) bits |= ((unsigned int)encoded[byte_index + 1u]) << (8u - shift);
    return (unsigned char)(bits & 0x07u);
}

__device__ __forceinline__ float termite_tq_decode_turbo3_at(const unsigned char* encoded, unsigned int value_index) {
    return termite_tq_decode_turbo3_scalar(termite_tq_get_packed3(encoded, value_index));
}

__device__ __forceinline__ unsigned char termite_tq_packed3_byte(const float* src, unsigned int value_count, unsigned int byte_index) {
    unsigned char out = 0u;
    unsigned int first_bit = byte_index * 8u;
    for (unsigned int bit = 0u; bit < 8u; ++bit) {
        unsigned int global_bit = first_bit + bit;
        unsigned int value_index = global_bit / 3u;
        if (value_index >= value_count) continue;
        unsigned char code = termite_tq_encode_turbo3_scalar(src[value_index]);
        unsigned int code_bit = global_bit - value_index * 3u;
        if (((code >> code_bit) & 1u) != 0u) out |= (unsigned char)(1u << bit);
    }
    return out;
}

__device__ __forceinline__ float termite_tq_random_sign(unsigned int head, unsigned int projection, unsigned int dim) {
    unsigned long long x = ((unsigned long long)(head + 1u)) * 0x9e3779b97f4a7c15ull;
    x ^= ((unsigned long long)(projection + 1u)) * 0xbf58476d1ce4e5b9ull;
    x ^= ((unsigned long long)(dim + 1u)) * 0x94d049bb133111ebull;
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return (x & 1ull) == 0ull ? 1.0f : -1.0f;
}

__device__ unsigned char termite_tq_turbo3_residual_byte(
    const float* src,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int byte_index
) {
    unsigned char out = 0u;
    unsigned int first_bit = byte_index * 8u;
    unsigned int total_bits = num_kv_heads * 32u;
    for (unsigned int bit = 0u; bit < 8u; ++bit) {
        unsigned int bit_index = first_bit + bit;
        if (bit_index >= total_bits) continue;
        unsigned int kv_head = bit_index / 32u;
        unsigned int projection = bit_index - kv_head * 32u;
        unsigned int value_start = kv_head * head_dim;
        float projected_residual = 0.0f;
        for (unsigned int d = 0u; d < head_dim; ++d) {
            float decoded = termite_tq_decode_turbo3_scalar(termite_tq_encode_turbo3_scalar(src[value_start + d]));
            float residual = src[value_start + d] - decoded;
            projected_residual += termite_tq_random_sign(kv_head, projection, d) * residual;
        }
        if (projected_residual >= 0.0f) out |= (unsigned char)(1u << bit);
    }
    return out;
}

__device__ float termite_tq_turbo3_projected_residual_score(
    const float* projected_query,
    const unsigned char* residual_sketch,
    unsigned int kv_head
) {
    float acc = 0.0f;
    for (unsigned int projection = 0u; projection < 32u; ++projection) {
        unsigned int bit_index = kv_head * 32u + projection;
        unsigned char packed = residual_sketch[bit_index >> 3];
        float residual_sign = ((packed >> (bit_index & 7u)) & 1u) != 0u ? 1.0f : -1.0f;
        acc += residual_sign * projected_query[projection];
    }
    return acc / 32.0f;
}

__device__ __forceinline__ unsigned int termite_tq_physical_token(
    unsigned int logical_token,
    const unsigned int* block_table,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int physical_token_capacity
) {
    unsigned int physical = logical_token;
    if (block_table != 0 && block_count != 0u && page_size_tokens != 0u) {
        unsigned int block_index = logical_token / page_size_tokens;
        if (block_index >= block_count) return 0xffffffffu;
        unsigned int token_offset = logical_token - block_index * page_size_tokens;
        physical = block_table[block_index] * page_size_tokens + token_offset;
    }
    return physical < physical_token_capacity ? physical : 0xffffffffu;
}

__device__ __forceinline__ void termite_tq_store_f32_byte(unsigned char* dst, float value, unsigned int byte) {
    const unsigned char* src = reinterpret_cast<const unsigned char*>(&value);
    dst[byte] = src[byte];
}

__device__ __forceinline__ float termite_tq_load_f32_bytes(const unsigned char* src) {
    unsigned int bits =
        ((unsigned int)src[0]) |
        ((unsigned int)src[1] << 8) |
        ((unsigned int)src[2] << 16) |
        ((unsigned int)src[3] << 24);
    return __uint_as_float(bits);
}

__device__ float termite_tq_int8_head_scale(const float* src_head, unsigned int head_dim) {
    float max_abs = 0.0f;
    for (unsigned int d = 0u; d < head_dim; ++d) {
        max_abs = fmaxf(max_abs, fabsf(src_head[d]));
    }
    return max_abs == 0.0f ? 1.0f : max_abs / 127.0f;
}

__device__ __forceinline__ float termite_tq_value_int8_per_head(
    const unsigned char* row,
    unsigned int kv_head,
    unsigned int lane,
    unsigned int head_dim
) {
    unsigned int head_stride = head_dim + 4u;
    const unsigned char* head = row + kv_head * head_stride;
    float scale = termite_tq_load_f32_bytes(head);
    signed char q = (signed char)head[4u + lane];
    return (float)q * scale;
}

extern "C" __global__ void termite_kv_write_suffix_turboquant_f32(
    unsigned char* k_dst,
    unsigned char* v_dst,
    const unsigned int* block_table,
    const float* k_src,
    const float* v_src,
    const unsigned int* decode_scalars,
    unsigned int suffix_token_count,
    unsigned int row_width,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int key_row_bytes,
    unsigned int base_key_row_bytes,
    unsigned int value_row_bytes,
    unsigned int fallback_total_token_count,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int format,
    unsigned int value_format,
    unsigned int physical_token_capacity
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (suffix_token_count == 0u || row_width == 0u || key_row_bytes == 0u || base_key_row_bytes == 0u || value_row_bytes == 0u) return;
    unsigned int total_token_count = fallback_total_token_count;
    if (decode_scalars != 0) total_token_count = decode_scalars[2];
    if (total_token_count < suffix_token_count) return;
    unsigned int token_start = total_token_count - suffix_token_count;

    unsigned int key_total = suffix_token_count * key_row_bytes;
    if (idx < key_total) {
        unsigned int token = idx / key_row_bytes;
        unsigned int byte = idx - token * key_row_bytes;
        const float* src_row = k_src + token * row_width;
        unsigned int physical_token = termite_tq_physical_token(token_start + token, block_table, block_count, page_size_tokens, physical_token_capacity);
        if (physical_token == 0xffffffffu) return;
        unsigned char* dst_row = k_dst + physical_token * key_row_bytes;
        if (format == 0u) {
            unsigned int value_index = byte * 2u;
            unsigned char lo = value_index < row_width ? termite_tq_encode_polar4_scalar(src_row[value_index]) : 0u;
            unsigned char hi = (value_index + 1u) < row_width ? termite_tq_encode_polar4_scalar(src_row[value_index + 1u]) : 0u;
            dst_row[byte] = lo | (unsigned char)(hi << 4);
        } else if (format == 1u) {
            if (byte < base_key_row_bytes) {
                dst_row[byte] = termite_tq_packed3_byte(src_row, row_width, byte);
            } else {
                dst_row[byte] = termite_tq_turbo3_residual_byte(src_row, num_kv_heads, head_dim, byte - base_key_row_bytes);
            }
        }
    }

    unsigned int value_total = suffix_token_count * (value_format == 0u ? row_width : value_row_bytes);
    if (idx < value_total) {
        unsigned int token;
        unsigned int lane_or_byte;
        if (value_format == 0u) {
            token = idx / row_width;
            lane_or_byte = idx - token * row_width;
        } else {
            token = idx / value_row_bytes;
            lane_or_byte = idx - token * value_row_bytes;
        }
        unsigned int physical_token = termite_tq_physical_token(token_start + token, block_table, block_count, page_size_tokens, physical_token_capacity);
        if (physical_token == 0xffffffffu) return;
        const float* src_row = v_src + token * row_width;
        unsigned char* dst_row = v_dst + physical_token * value_row_bytes;
        if (value_format == 0u) {
            reinterpret_cast<float*>(dst_row)[lane_or_byte] = src_row[lane_or_byte];
        } else if (value_format == 1u) {
            unsigned int head_stride = head_dim + 4u;
            unsigned int kv_head = lane_or_byte / head_stride;
            unsigned int head_byte = lane_or_byte - kv_head * head_stride;
            if (kv_head >= num_kv_heads) return;
            const float* src_head = src_row + kv_head * head_dim;
            float scale = termite_tq_int8_head_scale(src_head, head_dim);
            if (head_byte < 4u) {
                termite_tq_store_f32_byte(dst_row + kv_head * head_stride, scale, head_byte);
            } else {
                unsigned int lane = head_byte - 4u;
                if (lane >= head_dim) return;
                float scaled = roundf(src_head[lane] / scale);
                scaled = fminf(fmaxf(scaled, -127.0f), 127.0f);
                signed char q = (signed char)scaled;
                dst_row[kv_head * head_stride + 4u + lane] = (unsigned char)q;
            }
        }
    }
}

extern "C" __global__ void termite_gqa_attention_decode_turboquant_f32(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned int* block_table,
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
    unsigned int bias_mode,
    unsigned int key_row_bytes,
    unsigned int base_key_row_bytes,
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int format,
    unsigned int value_format,
    unsigned int physical_token_capacity
) {
    __shared__ float reduce[512];
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
    float scale = rsqrtf((float)head_dim);
    if (format == 1u && lane < 32u) {
        float projected_query = 0.0f;
        for (unsigned int d = 0u; d < head_dim; ++d) {
            projected_query += termite_tq_random_sign(kv_head, lane, d) * q[q_base + d];
        }
        shared_turbo3_projected_query[lane] = projected_query;
    }
    if (lane == 0u) shared_max_score = -3.402823466e+38f;
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
                : termite_tq_decode_turbo3_at(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        reduce[lane] = partial;
        __syncthreads();
        for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1) {
            if (lane < stride) reduce[lane] += reduce[lane + stride];
            __syncthreads();
        }
        if (lane == 0u && valid) {
            float score = reduce[0];
            if (format == 1u) {
                score += 0.125f * termite_tq_turbo3_projected_residual_score(shared_turbo3_projected_query, k_row + base_key_row_bytes, kv_head);
            }
            score *= scale;
            if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
            if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
            shared_max_score = fmaxf(shared_max_score, score);
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
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int value_index = kv_head * head_dim + lane;
            float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_decode_turbo3_at(k_row, value_index);
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
                shared_score = -3.402823466e+38f;
            }
        }
        __syncthreads();
        float e = valid ? expf(shared_score - shared_max_score) : 0.0f;
        if (lane == 0u) shared_denom += e;
        if (valid && lane < head_dim) {
            const unsigned char* v_row = v + physical_token * value_row_bytes;
            float value = value_format == 0u
                ? reinterpret_cast<const float*>(v_row)[kv_head * head_dim + lane]
                : termite_tq_value_int8_per_head(v_row, kv_head, lane, head_dim);
            acc += e * value;
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}

__device__ __forceinline__ float termite_half_to_float(unsigned short h) {
    return __half2float(__ushort_as_half(h));
}

__device__ float termite_q8_0_value(const unsigned char* bp, unsigned int lane) {
    // GGUF Q8_0 is scaled int8, not CUDA FP8; keep scale decode on CUDA's
    // native half path and reserve FP8 intrinsics for real FP8 storage.
    unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    float d = termite_half_to_float(h);
    signed char q = (signed char)bp[2u + lane];
    return (float)q * d;
}

__device__ __forceinline__ float termite_warp_reduce_sum(float v) {
    v += __shfl_down_sync(0xffffffffu, v, 16);
    v += __shfl_down_sync(0xffffffffu, v, 8);
    v += __shfl_down_sync(0xffffffffu, v, 4);
    v += __shfl_down_sync(0xffffffffu, v, 2);
    v += __shfl_down_sync(0xffffffffu, v, 1);
    return v;
}

__device__ float termite_q8_0_value_broadcast_scale(const unsigned char* bp, unsigned int lane) {
    float d = 0.0f;
    if (lane == 0u) {
        unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
        d = termite_half_to_float(h);
    }
    d = __shfl_sync(0xffffffffu, d, 0);
    signed char q = (signed char)bp[2u + lane];
    return (float)q * d;
}

template <unsigned int ROWS_PER_BLOCK, unsigned int COLS, unsigned int MODE>
__device__ void termite_q8_0_tile_rows_cols(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row_base = blockIdx.y * ROWS_PER_BLOCK;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    unsigned int block_tiles = in_dim / 256u;
    __shared__ float partial[ROWS_PER_BLOCK][COLS][256];
    float acc[ROWS_PER_BLOCK][COLS];
    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) acc[r][c] = 0.0f;
    }
    if (tid < 256u) {
        unsigned int sub_block = tid >> 5;
        unsigned int lane = tid & 31u;
        for (unsigned int tile = 0; tile < block_tiles; ++tile) {
            unsigned int block = tile * 8u + sub_block;
            float x[ROWS_PER_BLOCK];
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                unsigned int row = row_base + r;
                x[r] = row < rows ? input[row * in_dim + block * 32u + lane] : 0.0f;
            }
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                    float q = termite_q8_0_value(bp, lane);
                    #pragma unroll
                    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                        acc[r][c] += x[r] * q;
                    }
                }
            }
        }
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] = acc[r][c];
        }
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                #pragma unroll
                for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] += partial[r][c][tid + stride];
            }
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                unsigned int idx = row * out_dim + col;
                float y = partial[r][c][0];
                if (MODE == 1u || MODE == 2u || MODE == 3u || MODE == 4u) y += bias[col];
                if (MODE == 2u) y = 0.5f * y * (1.0f + tanhf(0.7978845608028654f * (y + 0.044715f * y * y * y)));
                if (MODE == 3u) y += residual[idx];
                if (MODE == 4u && y < 0.0f) y = 0.0f;
                dst[idx] = y;
            }
        }
    }
}

template <unsigned int ROWS_PER_BLOCK, unsigned int COLS, unsigned int MODE>
__device__ void termite_q8_0_tile_rows_cols_fast(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row_base = blockIdx.y * ROWS_PER_BLOCK;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    unsigned int block_tiles = in_dim / 256u;
    __shared__ float warp_partial[ROWS_PER_BLOCK][COLS][8];
    float acc[ROWS_PER_BLOCK][COLS];
    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) acc[r][c] = 0.0f;
    }
    if (tid < 256u) {
        unsigned int sub_block = warp;
        for (unsigned int tile = 0; tile < block_tiles; ++tile) {
            unsigned int block = tile * 8u + sub_block;
            float x[ROWS_PER_BLOCK];
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                unsigned int row = row_base + r;
                x[r] = row < rows ? input[row * in_dim + block * 32u + lane] : 0.0f;
            }
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                    float q = termite_q8_0_value_broadcast_scale(bp, lane);
                    #pragma unroll
                    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                        acc[r][c] += x[r] * q;
                    }
                }
            }
        }
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                float sum = termite_warp_reduce_sum(acc[r][c]);
                if (lane == 0u) warp_partial[r][c][warp] = sum;
            }
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 8u; ++w) y += warp_partial[r][c][w];
                unsigned int idx = row * out_dim + col;
                if (MODE == 1u || MODE == 2u || MODE == 3u || MODE == 4u) y += bias[col];
                if (MODE == 2u) y = 0.5f * y * (1.0f + tanhf(0.7978845608028654f * (y + 0.044715f * y * y * y)));
                if (MODE == 3u) y += residual[idx];
                if (MODE == 4u && y < 0.0f) y = 0.0f;
                dst[idx] = y;
            }
        }
    }
}

extern "C" __global__ void termite_linear_q8_0_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols<2u, 4u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols<2u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_gelu_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols<2u, 4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_add_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols<2u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_f32_fast_r2c8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<2u, 8u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_f32_fast_r2c8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<2u, 8u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_gelu_f32_fast_r2c8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<2u, 8u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_add_f32_fast_r2c8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<2u, 8u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_f32_fast_r4c4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<4u, 4u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_f32_fast_r4c4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<4u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_gelu_f32_fast_r4c4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<4u, 4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_add_f32_fast_r4c4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_rows_cols_fast<4u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

__device__ float termite_q4_0_value(const unsigned char* bp, unsigned int lane) {
    unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    float d = termite_half_to_float(h);
    unsigned char packed;
    int q;
    if (lane < 16u) {
        packed = bp[2u + lane];
        q = (int)(packed & 0x0fu);
    } else {
        packed = bp[2u + lane - 16u];
        q = (int)(packed >> 4);
    }
    return (float)(q - 8) * d;
}

extern "C" __global__ void termite_linear_q8_0_f32(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    unsigned int row_blocks = in_dim / 32u;
    float acc = 0.0f;
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
        unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
        float d = termite_half_to_float(h);
        for (unsigned int i = 0; i < 32u; ++i) {
            signed char q = (signed char)bp[2u + i];
            acc += input[row * in_dim + block * 32u + i] * ((float)q * d);
        }
    }
    dst[idx] = acc;
}

template <unsigned int COLS>
__device__ void termite_q8_0_tile_cols(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[COLS][256];
    float acc[COLS];
    #pragma unroll
    for (unsigned int c = 0; c < COLS; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                acc[c] += x * termite_q8_0_value(bp, lane);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < COLS; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
}

extern "C" __global__ void termite_linear_q8_0_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q8_0_tile_cols<4u>(dst, input, weight, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_gated_down_f32_tile4(
    float* dst,
    const float* gate,
    const float* up,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        unsigned int input_idx = row * in_dim + i;
        float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                acc[c] += x * termite_q8_0_value(bp, lane);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_f32(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    unsigned int row_blocks = in_dim / 32u;
    float acc = 0.0f;
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
        for (unsigned int i = 0; i < 32u; ++i) {
            acc += input[row * in_dim + block * 32u + i] * termite_q4_0_value(bp, i);
        }
    }
    dst[idx] = acc;
}

__device__ unsigned int termite_q4k_scale(const unsigned char* scales, unsigned int sub) {
    if (sub < 4u) return (unsigned int)(scales[sub] & 63u);
    return (unsigned int)((scales[sub + 4u] & 0x0fu) | ((scales[sub - 4u] >> 6u) << 4u));
}

__device__ unsigned int termite_q4k_min(const unsigned char* scales, unsigned int sub) {
    if (sub < 4u) return (unsigned int)(scales[sub + 4u] & 63u);
    return (unsigned int)((scales[sub + 4u] >> 4u) | ((scales[sub] >> 6u) << 4u));
}

__device__ float termite_q4k_value(const unsigned char* bp, unsigned int value_index) {
    unsigned short dh = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    unsigned short dminh = (unsigned short)bp[2] | ((unsigned short)bp[3] << 8);
    float d = termite_half_to_float(dh);
    float dmin = termite_half_to_float(dminh);
    const unsigned char* scales = bp + 4u;
    const unsigned char* qs = bp + 16u;
    unsigned int sub = value_index / 32u;
    unsigned int chunk = value_index / 64u;
    unsigned int lane = value_index & 31u;
    unsigned char packed = qs[chunk * 32u + lane];
    unsigned int q = (sub & 1u) == 0u ? (unsigned int)(packed & 0x0fu) : (unsigned int)(packed >> 4u);
    float scale = d * (float)termite_q4k_scale(scales, sub);
    float minv = dmin * (float)termite_q4k_min(scales, sub);
    return scale * (float)q - minv;
}

__device__ float termite_q4k_value_broadcast_scale(const unsigned char* bp, unsigned int value_index, unsigned int lane) {
    unsigned int sub = value_index / 32u;
    unsigned int chunk = value_index / 64u;
    float d = 0.0f;
    float dmin = 0.0f;
    int scale_i = 0;
    int min_i = 0;
    if (lane == 0u) {
        unsigned short dh = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
        unsigned short dminh = (unsigned short)bp[2] | ((unsigned short)bp[3] << 8);
        d = termite_half_to_float(dh);
        dmin = termite_half_to_float(dminh);
        const unsigned char* scales = bp + 4u;
        scale_i = termite_q4k_scale(scales, sub);
        min_i = termite_q4k_min(scales, sub);
    }
    d = __shfl_sync(0xffffffffu, d, 0);
    dmin = __shfl_sync(0xffffffffu, dmin, 0);
    scale_i = __shfl_sync(0xffffffffu, scale_i, 0);
    min_i = __shfl_sync(0xffffffffu, min_i, 0);
    const unsigned char* qs = bp + 16u;
    unsigned char packed = qs[chunk * 32u + lane];
    unsigned int q = (sub & 1u) == 0u ? (unsigned int)(packed & 0x0fu) : (unsigned int)(packed >> 4u);
    return (d * (float)scale_i) * (float)q - (dmin * (float)min_i);
}

static constexpr unsigned int TERMITE_QTC_M = 64u;
static constexpr unsigned int TERMITE_QTC_N = 32u;
static constexpr unsigned int TERMITE_QTC_K = 16u;
static constexpr unsigned int TERMITE_QTC_THREADS = 256u;

__device__ __forceinline__ half termite_half_from_le(const unsigned char* src) {
    unsigned short bits = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    return __ushort_as_half(bits);
}

__device__ __forceinline__ half termite_q8_0_tc_value_at(
    const unsigned char* packed,
    unsigned int out_dim,
    unsigned int col,
    unsigned int row_blocks,
    unsigned int k_abs
) {
    unsigned int block = k_abs / 32u;
    unsigned int lane = k_abs & 31u;
    unsigned int block_index = col * row_blocks + block;
    unsigned int block_count = out_dim * row_blocks;
    half d = termite_half_from_le(packed + block_index * 2u);
    signed char q = (signed char)packed[block_count * 2u + block_index * 32u + lane];
    return __float2half_rn((float)q * __half2float(d));
}

__device__ __forceinline__ half termite_q4_k_tc_value_at(
    const unsigned char* packed,
    unsigned int out_dim,
    unsigned int col,
    unsigned int row_blocks,
    unsigned int k_abs
) {
    unsigned int block = k_abs / 256u;
    unsigned int in_block = k_abs & 255u;
    unsigned int sub = in_block / 32u;
    unsigned int chunk = in_block / 64u;
    unsigned int lane = in_block & 31u;
    unsigned int block_index = col * row_blocks + block;
    unsigned int block_count = out_dim * row_blocks;
    const unsigned char* meta = packed + block_index * 20u;
    const unsigned char* qs = packed + block_count * 20u + block_index * 128u;
    half dh = termite_half_from_le(meta);
    half dminh = termite_half_from_le(meta + 2u);
    float d = __half2float(dh);
    float dmin = __half2float(dminh);
    unsigned char packed_q = qs[chunk * 32u + lane];
    unsigned int q = (sub & 1u) == 0u ? (unsigned int)(packed_q & 0x0fu) : (unsigned int)(packed_q >> 4u);
    float scale = d * (float)meta[4u + sub];
    float minv = dmin * (float)meta[12u + sub];
    return __float2half_rn(scale * (float)q - minv);
}

template <unsigned int MODE, bool Q4K>
__device__ void termite_qtc_hmma_tile(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int tid = threadIdx.x;
    unsigned int warp = tid >> 5;
    if (warp >= 8u) return;
    unsigned int warp_m = warp & 3u;
    unsigned int warp_n = warp >> 2;
    unsigned int row_base = blockIdx.y * TERMITE_QTC_M;
    unsigned int col_base = blockIdx.x * TERMITE_QTC_N;
    unsigned int row_blocks = Q4K ? (in_dim / 256u) : (in_dim / 32u);

    __shared__ half a_tile[TERMITE_QTC_M * TERMITE_QTC_K];
    __shared__ half b_tile[TERMITE_QTC_K * TERMITE_QTC_N];
    __shared__ float c_tile[TERMITE_QTC_M * TERMITE_QTC_N];

    wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
    wmma::fill_fragment(acc, 0.0f);

    for (unsigned int k_base = 0; k_base < in_dim; k_base += TERMITE_QTC_K) {
        for (unsigned int i = tid; i < TERMITE_QTC_M * TERMITE_QTC_K; i += TERMITE_QTC_THREADS) {
            unsigned int local_row = i / TERMITE_QTC_K;
            unsigned int local_k = i - local_row * TERMITE_QTC_K;
            unsigned int row = row_base + local_row;
            unsigned int k_abs = k_base + local_k;
            float x = (row < rows && k_abs < in_dim) ? input[row * in_dim + k_abs] : 0.0f;
            a_tile[i] = __float2half_rn(x);
        }
        for (unsigned int i = tid; i < TERMITE_QTC_K * TERMITE_QTC_N; i += TERMITE_QTC_THREADS) {
            unsigned int local_k = i / TERMITE_QTC_N;
            unsigned int local_col = i - local_k * TERMITE_QTC_N;
            unsigned int col = col_base + local_col;
            unsigned int k_abs = k_base + local_k;
            half w = __float2half_rn(0.0f);
            if (col < out_dim && k_abs < in_dim) {
                w = Q4K
                    ? termite_q4_k_tc_value_at(packed_weight, out_dim, col, row_blocks, k_abs)
                    : termite_q8_0_tc_value_at(packed_weight, out_dim, col, row_blocks, k_abs);
            }
            b_tile[i] = w;
        }
        __syncthreads();

        wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
        wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
        wmma::load_matrix_sync(a_frag, a_tile + warp_m * 16u * TERMITE_QTC_K, TERMITE_QTC_K);
        wmma::load_matrix_sync(b_frag, b_tile + warp_n * 16u, TERMITE_QTC_N);
        wmma::mma_sync(acc, a_frag, b_frag, acc);
        __syncthreads();
    }

    wmma::store_matrix_sync(c_tile + warp_m * 16u * TERMITE_QTC_N + warp_n * 16u, acc, TERMITE_QTC_N, wmma::mem_row_major);
    __syncthreads();

    for (unsigned int i = tid; i < TERMITE_QTC_M * TERMITE_QTC_N; i += TERMITE_QTC_THREADS) {
        unsigned int local_row = i / TERMITE_QTC_N;
        unsigned int local_col = i - local_row * TERMITE_QTC_N;
        unsigned int row = row_base + local_row;
        unsigned int col = col_base + local_col;
        if (row >= rows || col >= out_dim) continue;
        unsigned int idx = row * out_dim + col;
        float y = c_tile[i];
        if (MODE == 1u || MODE == 2u || MODE == 3u || MODE == 4u || MODE == 5u) y += bias[col];
        if (MODE == 2u) y = 0.5f * y * (1.0f + tanhf(0.7978845608028654f * (y + 0.044715f * y * y * y)));
        if (MODE == 3u) y += residual[idx];
        if (MODE == 4u) y = y / (1.0f + expf(-1.702f * y));
        if (MODE == 5u) y = fmaxf(y, 0.0f);
        dst[idx] = y;
    }
}

extern "C" __global__ void termite_linear_q8_0_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<0u, false>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<1u, false>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_gelu_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<2u, false>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q8_0_bias_add_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<3u, false>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<0u, true>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<1u, true>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_gelu_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<2u, true>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_add_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<3u, true>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_quick_gelu_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<4u, true>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_relu_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<5u, true>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_triple_bias_f32_tc_hmma(
    float* dst_a,
    float* dst_b,
    float* dst_c,
    const float* input,
    const unsigned char* packed_weight_a,
    const float* bias_a,
    const unsigned char* packed_weight_b,
    const float* bias_b,
    const unsigned char* packed_weight_c,
    const float* bias_c,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int projection = blockIdx.z;
    float* dst = projection == 0u ? dst_a : (projection == 1u ? dst_b : dst_c);
    const unsigned char* packed_weight = projection == 0u ? packed_weight_a : (projection == 1u ? packed_weight_b : packed_weight_c);
    const float* bias = projection == 0u ? bias_a : (projection == 1u ? bias_b : bias_c);
    termite_qtc_hmma_tile<1u, true>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_pair_bias_f32_tc_hmma(
    float* dst_a,
    float* dst_b,
    const float* input,
    const unsigned char* packed_weight_a,
    const float* bias_a,
    const unsigned char* packed_weight_b,
    const float* bias_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int projection = blockIdx.z;
    float* dst = projection == 0u ? dst_a : dst_b;
    const unsigned char* packed_weight = projection == 0u ? packed_weight_a : packed_weight_b;
    const float* bias = projection == 0u ? bias_a : bias_b;
    termite_qtc_hmma_tile<1u, true>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

template <unsigned int COLS, unsigned int MODE>
__device__ void termite_q4k_tile_cols(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[COLS][256];
    float acc[COLS];
    #pragma unroll
    for (unsigned int c = 0; c < COLS; ++c) acc[c] = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x = input[row * in_dim + block * 256u + tid];
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
                    acc[c] += x * termite_q4k_value(bp, tid);
                }
            }
        }
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) partial[c][tid] = acc[c];
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            unsigned int col = col_tile + c;
            if (col >= out_dim) continue;
            unsigned int idx = row * out_dim + col;
            float y = partial[c][0];
            if (MODE == 1u || MODE == 2u || MODE == 3u) y += bias[col];
            if (MODE == 2u) y = y / (1.0f + expf(-1.702f * y));
            if (MODE == 3u) y += residual[idx];
            if (MODE == 4u) {
                y += bias[col];
                if (y < 0.0f) y = 0.0f;
            }
            dst[idx] = y;
        }
    }
}

template <unsigned int ROWS_PER_BLOCK, unsigned int COLS, unsigned int MODE>
__device__ void termite_q4k_tile_rows_cols(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row_base = blockIdx.y * ROWS_PER_BLOCK;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[ROWS_PER_BLOCK][COLS][256];
    float acc[ROWS_PER_BLOCK][COLS];
    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) acc[r][c] = 0.0f;
    }
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x[ROWS_PER_BLOCK];
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                unsigned int row = row_base + r;
                x[r] = row < rows ? input[row * in_dim + block * 256u + tid] : 0.0f;
            }
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
                    float q = termite_q4k_value(bp, tid);
                    #pragma unroll
                    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                        acc[r][c] += x[r] * q;
                    }
                }
            }
        }
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] = acc[r][c];
        }
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                #pragma unroll
                for (unsigned int c = 0; c < COLS; ++c) partial[r][c][tid] += partial[r][c][tid + stride];
            }
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                unsigned int idx = row * out_dim + col;
                float y = partial[r][c][0] + bias[col];
                if (MODE == 2u) y = 0.5f * y * (1.0f + tanhf(0.7978845608028654f * (y + 0.044715f * y * y * y)));
                if (MODE == 3u) y += residual[idx];
                if (MODE == 4u && y < 0.0f) y = 0.0f;
                dst[idx] = y;
            }
        }
    }
}

template <unsigned int ROWS_PER_BLOCK, unsigned int COLS, unsigned int MODE>
__device__ void termite_q4k_tile_rows_cols_fast(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row_base = blockIdx.y * ROWS_PER_BLOCK;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float warp_partial[ROWS_PER_BLOCK][COLS][8];
    float acc[ROWS_PER_BLOCK][COLS];
    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) acc[r][c] = 0.0f;
    }
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x[ROWS_PER_BLOCK];
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                unsigned int row = row_base + r;
                x[r] = row < rows ? input[row * in_dim + block * 256u + tid] : 0.0f;
            }
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
                    float q = termite_q4k_value_broadcast_scale(bp, tid, lane);
                    #pragma unroll
                    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                        acc[r][c] += x[r] * q;
                    }
                }
            }
        }
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                float sum = termite_warp_reduce_sum(acc[r][c]);
                if (lane == 0u) warp_partial[r][c][warp] = sum;
            }
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 8u; ++w) y += warp_partial[r][c][w];
                unsigned int idx = row * out_dim + col;
                if (MODE == 1u || MODE == 2u || MODE == 3u || MODE == 4u) y += bias[col];
                if (MODE == 2u) y = 0.5f * y * (1.0f + tanhf(0.7978845608028654f * (y + 0.044715f * y * y * y)));
                if (MODE == 3u) y += residual[idx];
                if (MODE == 4u && y < 0.0f) y = 0.0f;
                dst[idx] = y;
            }
        }
    }
}

template <unsigned int ROWS_PER_BLOCK, unsigned int COLS, bool RELU, bool SEPARATE_INPUTS>
__device__ void termite_q4k_pair_tile_rows_cols(
    float* dst_a,
    float* dst_b,
    const float* input,
    const float* input_b,
    const unsigned char* weight_a,
    const float* bias_a,
    const unsigned char* weight_b,
    const float* bias_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * COLS;
    unsigned int row_base = blockIdx.y * ROWS_PER_BLOCK;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial_a[ROWS_PER_BLOCK][COLS][256];
    __shared__ float partial_b[ROWS_PER_BLOCK][COLS][256];
    float acc_a[ROWS_PER_BLOCK][COLS];
    float acc_b[ROWS_PER_BLOCK][COLS];
    #pragma unroll
    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            acc_a[r][c] = 0.0f;
            acc_b[r][c] = 0.0f;
        }
    }
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x_a[ROWS_PER_BLOCK];
            float x_b[ROWS_PER_BLOCK];
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                unsigned int row = row_base + r;
                x_a[r] = row < rows ? input[row * in_dim + block * 256u + tid] : 0.0f;
                x_b[r] = SEPARATE_INPUTS && row < rows ? input_b[row * in_dim + block * 256u + tid] : x_a[r];
            }
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp_a = weight_a + (col * row_blocks + block) * 144u;
                    const unsigned char* bp_b = weight_b + (col * row_blocks + block) * 144u;
                    float qa = termite_q4k_value(bp_a, tid);
                    float qb = termite_q4k_value(bp_b, tid);
                    #pragma unroll
                    for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                        acc_a[r][c] += x_a[r] * qa;
                        acc_b[r][c] += x_b[r] * qb;
                    }
                }
            }
        }
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                partial_a[r][c][tid] = acc_a[r][c];
                partial_b[r][c][tid] = acc_b[r][c];
            }
        }
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
                #pragma unroll
                for (unsigned int c = 0; c < COLS; ++c) {
                    partial_a[r][c][tid] += partial_a[r][c][tid + stride];
                    partial_b[r][c][tid] += partial_b[r][c][tid + stride];
                }
            }
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0; r < ROWS_PER_BLOCK; ++r) {
            unsigned int row = row_base + r;
            if (row >= rows) continue;
            #pragma unroll
            for (unsigned int c = 0; c < COLS; ++c) {
                unsigned int col = col_tile + c;
                if (col >= out_dim) continue;
                unsigned int idx = row * out_dim + col;
                float y_a = partial_a[r][c][0] + bias_a[col];
                float y_b = partial_b[r][c][0] + bias_b[col];
                if (RELU) {
                    if (y_a < 0.0f) y_a = 0.0f;
                    if (y_b < 0.0f) y_b = 0.0f;
                }
                dst_a[idx] = y_a;
                dst_b[idx] = y_b;
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_k_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_cols<4u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_gated_down_f32_tile4(
    float* dst,
    const float* gate,
    const float* up,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            unsigned int input_idx = row * in_dim + block * 256u + tid;
            float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
                    acc[c] += x * termite_q4k_value(bp, tid);
                }
            }
        }
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
}

extern "C" __global__ void termite_linear_q4_k_f32_tile8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_cols<8u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_cols<4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<2u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_gelu_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<2u, 4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_add_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<2u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_f32_fast_r2c8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols_fast<2u, 8u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_gelu_f32_fast_r2c8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols_fast<2u, 8u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_add_f32_fast_r2c8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols_fast<2u, 8u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_f32_fast_r4c4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols_fast<4u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_gelu_f32_fast_r4c4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols_fast<4u, 4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_add_f32_fast_r4c4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols_fast<4u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_span_bias_f32_tile8_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<2u, 8u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_span_bias_relu_f32_tile8_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<2u, 8u, 4u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_span_bias_f32_tile4_r8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<8u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_span_bias_relu_f32_tile4_r8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<8u, 4u, 4u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_span_pair_bias_f32_tile8_r2(
    float* dst_a,
    float* dst_b,
    const float* input,
    const unsigned char* weight_a,
    const float* bias_a,
    const unsigned char* weight_b,
    const float* bias_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_pair_tile_rows_cols<2u, 8u, false, false>(dst_a, dst_b, input, nullptr, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_span_pair_bias_relu_f32_tile8_r2(
    float* dst_a,
    float* dst_b,
    const float* input,
    const unsigned char* weight_a,
    const float* bias_a,
    const unsigned char* weight_b,
    const float* bias_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_pair_tile_rows_cols<2u, 8u, true, false>(dst_a, dst_b, input, nullptr, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_span_pair2_bias_f32_tile8_r2(
    float* dst_a,
    float* dst_b,
    const float* input_a,
    const float* input_b,
    const unsigned char* weight_a,
    const float* bias_a,
    const unsigned char* weight_b,
    const float* bias_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_pair_tile_rows_cols<2u, 8u, false, true>(dst_a, dst_b, input_a, input_b, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_quick_gelu_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_cols<4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_relu_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_cols<4u, 4u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_relu_f32_tile4_r2(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_rows_cols<2u, 4u, 4u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_bias_add_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4k_tile_cols<4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_f32(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    float acc = 0.0f;
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
        for (unsigned int i = 0; i < 256u; ++i) {
            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
        }
    }
    dst[idx] = acc;
}

extern "C" __global__ void termite_linear_q4_k_bias_f32(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    float acc = bias[col];
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
        for (unsigned int i = 0; i < 256u; ++i) {
            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
        }
    }
    dst[idx] = acc;
}

extern "C" __global__ void termite_linear_q4_k_f32_tiled(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int out_idx = blockIdx.x;
    unsigned int total = rows * out_dim;
    if (out_idx >= total) return;
    unsigned int row = out_idx / out_dim;
    unsigned int col = out_idx - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[out_idx] = partial[0];
}

extern "C" __global__ void termite_linear_q4_k_bias_f32_tiled(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int out_idx = blockIdx.x;
    unsigned int total = rows * out_dim;
    if (out_idx >= total) return;
    unsigned int row = out_idx / out_dim;
    unsigned int col = out_idx - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[out_idx] = partial[0] + bias[col];
}

extern "C" __global__ void termite_linear_q4_k_bias_quick_gelu_f32_tiled(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int out_idx = blockIdx.x;
    unsigned int total = rows * out_dim;
    if (out_idx >= total) return;
    unsigned int row = out_idx / out_dim;
    unsigned int col = out_idx - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) {
        float x = partial[0] + bias[col];
        dst[out_idx] = x / (1.0f + expf(-1.702f * x));
    }
}

extern "C" __global__ void termite_linear_q4_k_triple_bias_f32(
    float* dst_a,
    float* dst_b,
    float* dst_c,
    const float* input,
    const unsigned char* weight_a,
    const float* bias_a,
    const unsigned char* weight_b,
    const float* bias_b,
    const unsigned char* weight_c,
    const float* bias_c,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int plane = rows * out_dim;
    unsigned int total = plane * 3u;
    if (idx >= total) return;
    unsigned int projection = idx / plane;
    unsigned int local = idx - projection * plane;
    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    const unsigned char* weight = projection == 0u ? weight_a : (projection == 1u ? weight_b : weight_c);
    const float* bias = projection == 0u ? bias_a : (projection == 1u ? bias_b : bias_c);
    float* dst = projection == 0u ? dst_a : (projection == 1u ? dst_b : dst_c);
    unsigned int row_blocks = in_dim / 256u;
    float acc = bias[col];
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
        for (unsigned int i = 0; i < 256u; ++i) {
            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
        }
    }
    dst[local] = acc;
}

extern "C" __global__ void termite_linear_q4_k_triple_bias_f32_tiled(
    float* dst_a,
    float* dst_b,
    float* dst_c,
    const float* input,
    const unsigned char* weight_a,
    const float* bias_a,
    const unsigned char* weight_b,
    const float* bias_b,
    const unsigned char* weight_c,
    const float* bias_c,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int plane = rows * out_dim;
    unsigned int global = blockIdx.x;
    if (global >= plane * 3u) return;
    unsigned int projection = global / plane;
    unsigned int local = global - projection * plane;
    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    const unsigned char* weight = projection == 0u ? weight_a : (projection == 1u ? weight_b : weight_c);
    const float* bias = projection == 0u ? bias_a : (projection == 1u ? bias_b : bias_c);
    float* dst = projection == 0u ? dst_a : (projection == 1u ? dst_b : dst_c);
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0] + bias[col];
}

extern "C" __global__ void termite_linear_q4_k_pair_bias_f32_tiled(
    float* dst_a,
    float* dst_b,
    const float* input,
    const unsigned char* weight_a,
    const float* bias_a,
    const unsigned char* weight_b,
    const float* bias_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int plane = rows * out_dim;
    unsigned int global = blockIdx.x;
    if (global >= plane * 2u) return;
    unsigned int projection = global / plane;
    unsigned int local = global - projection * plane;
    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    const float* bias = projection == 0u ? bias_a : bias_b;
    float* dst = projection == 0u ? dst_a : dst_b;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0] + bias[col];
}

extern "C" __global__ void termite_linear_q8_0_pair_nobias_f32_tile4(
    float* dst_a,
    float* dst_b,
    const float* input,
    const unsigned char* weight_a,
    const unsigned char* weight_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int tiles_per_row = tiles * 2u;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles_per_row;
    if (row >= rows) return;
    unsigned int tile_local = global - row * tiles_per_row;
    unsigned int projection = tile_local / tiles;
    unsigned int col_tile = (tile_local - projection * tiles) * cols;
    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    float* dst = projection == 0u ? dst_a : dst_b;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                acc[c] += x * termite_q8_0_value(bp, lane);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
}

extern "C" __global__ void termite_linear_q4_k_pair_nobias_f32_tile4(
    float* dst_a,
    float* dst_b,
    const float* input,
    const unsigned char* weight_a,
    const unsigned char* weight_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int tiles_per_row = tiles * 2u;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles_per_row;
    if (row >= rows) return;
    unsigned int tile_local = global - row * tiles_per_row;
    unsigned int projection = tile_local / tiles;
    unsigned int col_tile = (tile_local - projection * tiles) * cols;
    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    float* dst = projection == 0u ? dst_a : dst_b;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x = input[row * in_dim + block * 256u + tid];
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
                    acc[c] += x * termite_q4k_value(bp, tid);
                }
            }
        }
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
}

extern "C" __global__ void termite_linear_q8_0_argmax_stage1_tile4(
    float* partial_values,
    unsigned int* partial_indices,
    const float* input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 4u;
    unsigned int tile = blockIdx.x;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row = rows == 0u ? 0u : rows - 1u;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                acc[c] += x * termite_q8_0_value(bp, lane);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col >= out_dim) continue;
            bool suppressed = false;
            for (unsigned int j = 0; j < suppress_count; ++j) {
                int token_id = suppress_token_ids[j];
                if (token_id >= 0 && (unsigned int)token_id == col) {
                    suppressed = true;
                    break;
                }
            }
            if (suppressed) continue;
            float value = partial[c][0];
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[tile] = best_value;
        partial_indices[tile] = best_index;
    }
}

extern "C" __global__ void termite_linear_q4_k_argmax_stage1_tile4(
    float* partial_values,
    unsigned int* partial_indices,
    const float* input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 4u;
    unsigned int tile = blockIdx.x;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row = rows == 0u ? 0u : rows - 1u;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int block = 0; block < row_blocks; ++block) {
        float x = input[row * in_dim + block * 256u + tid];
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
                acc[c] += x * termite_q4k_value(bp, tid);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col >= out_dim) continue;
            bool suppressed = false;
            for (unsigned int j = 0; j < suppress_count; ++j) {
                int token_id = suppress_token_ids[j];
                if (token_id >= 0 && (unsigned int)token_id == col) {
                    suppressed = true;
                    break;
                }
            }
            if (suppressed) continue;
            float value = partial[c][0];
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[tile] = best_value;
        partial_indices[tile] = best_index;
    }
}

extern "C" __global__ void termite_linear_q8_0_argmax_rows_stage1_tile4(
    float* partial_values,
    unsigned int* partial_indices,
    const float* input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 4u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    if (row < rows) {
        for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
            float x = input[row * in_dim + i];
            unsigned int block = i / 32u;
            unsigned int lane = i - block * 32u;
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                    acc[c] += x * termite_q8_0_value(bp, lane);
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (row >= rows || col >= out_dim) continue;
            bool suppressed = false;
            for (unsigned int j = 0; j < suppress_count; ++j) {
                int token_id = suppress_token_ids[j];
                if (token_id >= 0 && (unsigned int)token_id == col) {
                    suppressed = true;
                    break;
                }
            }
            if (suppressed) continue;
            float value = partial[c][0];
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}

extern "C" __global__ void termite_linear_q4_0_argmax_rows_stage1_tile4(
    float* partial_values,
    unsigned int* partial_indices,
    const float* input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 4u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    if (row < rows) {
        for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
            float x = input[row * in_dim + i];
            unsigned int block = i / 32u;
            unsigned int lane = i - block * 32u;
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += x * termite_q4_0_value(bp, lane);
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (row >= rows || col >= out_dim) continue;
            bool suppressed = false;
            for (unsigned int j = 0; j < suppress_count; ++j) {
                int token_id = suppress_token_ids[j];
                if (token_id >= 0 && (unsigned int)token_id == col) {
                    suppressed = true;
                    break;
                }
            }
            if (suppressed) continue;
            float value = partial[c][0];
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}

extern "C" __global__ void termite_linear_q4_k_argmax_rows_stage1_tile4(
    float* partial_values,
    unsigned int* partial_indices,
    const float* input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 4u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    if (row < rows) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x = input[row * in_dim + block * 256u + tid];
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
                    acc[c] += x * termite_q4k_value(bp, tid);
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (row >= rows || col >= out_dim) continue;
            bool suppressed = false;
            for (unsigned int j = 0; j < suppress_count; ++j) {
                int token_id = suppress_token_ids[j];
                if (token_id >= 0 && (unsigned int)token_id == col) {
                    suppressed = true;
                    break;
                }
            }
            if (suppressed) continue;
            float value = partial[c][0];
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}

extern "C" __global__ void termite_argmax_reduce_rows_pairs_f32(
    unsigned int* dst,
    const float* partial_values,
    const unsigned int* partial_indices,
    unsigned int rows,
    unsigned int col_tiles
) {
    __shared__ float best_values[256];
    __shared__ unsigned int best_indices[256];
    unsigned int row = blockIdx.x;
    unsigned int tid = threadIdx.x;
    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
    if (row < rows) {
        unsigned int base = row * col_tiles;
        for (unsigned int i = tid; i < col_tiles; i += blockDim.x) {
            unsigned int index = partial_indices[base + i];
            if (index == 0xffffffffu) continue;
            float value = partial_values[base + i];
            if (value > best_value || (value == best_value && index < best_index)) {
                best_value = value;
                best_index = index;
            }
        }
    }
    best_values[tid] = best_value;
    best_indices[tid] = best_index;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float other_value = best_values[tid + stride];
            unsigned int other_index = best_indices[tid + stride];
            if (other_index != 0xffffffffu &&
                (other_value > best_values[tid] || (other_value == best_values[tid] && other_index < best_indices[tid]))) {
                best_values[tid] = other_value;
                best_indices[tid] = other_index;
            }
        }
        __syncthreads();
    }
    if (tid == 0u && row < rows) dst[row] = best_indices[0] == 0xffffffffu ? 0u : best_indices[0];
}

extern "C" __global__ void termite_argmax_reduce_pairs_f32(
    unsigned int* dst,
    const float* partial_values,
    const unsigned int* partial_indices,
    unsigned int count
) {
    __shared__ float best_values[256];
    __shared__ unsigned int best_indices[256];
    unsigned int tid = threadIdx.x;
    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
    for (unsigned int i = tid; i < count; i += blockDim.x) {
        unsigned int index = partial_indices[i];
        if (index == 0xffffffffu) continue;
        float value = partial_values[i];
        if (value > best_value || (value == best_value && index < best_index)) {
            best_value = value;
            best_index = index;
        }
    }
    best_values[tid] = best_value;
    best_indices[tid] = best_index;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float other_value = best_values[tid + stride];
            unsigned int other_index = best_indices[tid + stride];
            if (other_index != 0xffffffffu &&
                (other_value > best_values[tid] || (other_value == best_values[tid] && other_index < best_indices[tid]))) {
                best_values[tid] = other_value;
                best_indices[tid] = other_index;
            }
        }
        __syncthreads();
    }
    if (tid == 0u) dst[0] = best_indices[0] == 0xffffffffu ? 0u : best_indices[0];
}

extern "C" __global__ void termite_linear_f32_qkv_nobias_tiled(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const float* input,
    const float* weight_q,
    const float* weight_k,
    const float* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    unsigned int q_plane = rows * q_out_dim;
    unsigned int kv_plane = rows * kv_out_dim;
    unsigned int total = q_plane + kv_plane * 2u;
    unsigned int global = blockIdx.x;
    if (global >= total) return;

    const float* weight;
    float* dst;
    unsigned int out_dim;
    unsigned int local;
    if (global < q_plane) {
        weight = weight_q;
        dst = dst_q;
        out_dim = q_out_dim;
        local = global;
    } else if (global < q_plane + kv_plane) {
        weight = weight_k;
        dst = dst_k;
        out_dim = kv_out_dim;
        local = global - q_plane;
    } else {
        weight = weight_v;
        dst = dst_v;
        out_dim = kv_out_dim;
        local = global - q_plane - kv_plane;
    }

    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        acc += input[row * in_dim + i] * weight[col * in_dim + i];
    }
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0];
}

extern "C" __global__ void termite_linear_bf16_weight_f32_qkv_nobias_tiled(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const float* input,
    const unsigned short* weight_q,
    const unsigned short* weight_k,
    const unsigned short* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    unsigned int q_plane = rows * q_out_dim;
    unsigned int kv_plane = rows * kv_out_dim;
    unsigned int total = q_plane + kv_plane * 2u;
    unsigned int global = blockIdx.x;
    if (global >= total) return;

    const unsigned short* weight;
    float* dst;
    unsigned int out_dim;
    unsigned int local;
    if (global < q_plane) {
        weight = weight_q;
        dst = dst_q;
        out_dim = q_out_dim;
        local = global;
    } else if (global < q_plane + kv_plane) {
        weight = weight_k;
        dst = dst_k;
        out_dim = kv_out_dim;
        local = global - q_plane;
    } else {
        weight = weight_v;
        dst = dst_v;
        out_dim = kv_out_dim;
        local = global - q_plane - kv_plane;
    }

    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        acc += input[row * in_dim + i] * termite_bf16_to_f32(weight[col * in_dim + i]);
    }
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0];
}

extern "C" __global__ void termite_linear_q8_0_qkv_nobias_f32_tile4(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const float* input,
    const unsigned char* weight_q,
    const unsigned char* weight_k,
    const unsigned char* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    const unsigned int cols = 4u;
    unsigned int q_tiles = (q_out_dim + cols - 1u) / cols;
    unsigned int kv_tiles = (kv_out_dim + cols - 1u) / cols;
    unsigned int tiles_per_row = q_tiles + kv_tiles * 2u;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles_per_row;
    if (row >= rows) return;
    unsigned int tile = global - row * tiles_per_row;

    const unsigned char* weight;
    float* dst;
    unsigned int out_dim;
    unsigned int col_tile;
    if (tile < q_tiles) {
        weight = weight_q;
        dst = dst_q;
        out_dim = q_out_dim;
        col_tile = tile * cols;
    } else if (tile < q_tiles + kv_tiles) {
        weight = weight_k;
        dst = dst_k;
        out_dim = kv_out_dim;
        col_tile = (tile - q_tiles) * cols;
    } else {
        weight = weight_v;
        dst = dst_v;
        out_dim = kv_out_dim;
        col_tile = (tile - q_tiles - kv_tiles) * cols;
    }

    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                acc[c] += x * termite_q8_0_value(bp, lane);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
        }
    }
}

extern "C" __global__ void termite_linear_q4_k_qkv_nobias_f32_tiled(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const float* input,
    const unsigned char* weight_q,
    const unsigned char* weight_k,
    const unsigned char* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    unsigned int q_plane = rows * q_out_dim;
    unsigned int kv_plane = rows * kv_out_dim;
    unsigned int total = q_plane + kv_plane * 2u;
    unsigned int global = blockIdx.x;
    if (global >= total) return;

    const unsigned char* weight;
    float* dst;
    unsigned int out_dim;
    unsigned int local;
    if (global < q_plane) {
        weight = weight_q;
        dst = dst_q;
        out_dim = q_out_dim;
        local = global;
    } else if (global < q_plane + kv_plane) {
        weight = weight_k;
        dst = dst_k;
        out_dim = kv_out_dim;
        local = global - q_plane;
    } else {
        weight = weight_v;
        dst = dst_v;
        out_dim = kv_out_dim;
        local = global - q_plane - kv_plane;
    }

    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0];
}

extern "C" __global__ void termite_linear_q4_k_q4_k_f32_qkv_nobias_tiled(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const float* input,
    const unsigned char* weight_q,
    const unsigned char* weight_k,
    const float* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    unsigned int q_plane = rows * q_out_dim;
    unsigned int kv_plane = rows * kv_out_dim;
    unsigned int total = q_plane + kv_plane * 2u;
    unsigned int global = blockIdx.x;
    if (global >= total) return;

    const unsigned char* qweight = weight_q;
    const unsigned char* kweight = weight_k;
    const float* vweight = weight_v;
    float* dst;
    unsigned int out_dim;
    unsigned int local;
    unsigned int projection;
    if (global < q_plane) {
        dst = dst_q;
        out_dim = q_out_dim;
        local = global;
        projection = 0u;
    } else if (global < q_plane + kv_plane) {
        dst = dst_k;
        out_dim = kv_out_dim;
        local = global - q_plane;
        projection = 1u;
    } else {
        dst = dst_v;
        out_dim = kv_out_dim;
        local = global - q_plane - kv_plane;
        projection = 2u;
    }

    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (projection == 2u) {
        for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
            acc += input[row * in_dim + i] * vweight[col * in_dim + i];
        }
    } else if (tid < 256u) {
        const unsigned char* weight = projection == 0u ? qweight : kweight;
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
    }
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0];
}

extern "C" __global__ void termite_embedding_lookup_q4_k_f32(
    float* dst,
    const unsigned char* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int out_row = idx / dim;
    unsigned int col = idx - out_row * dim;
    unsigned long long src_row = (unsigned long long)ids[out_row];
    unsigned int row_blocks = dim / 256u;
    unsigned int block = col / 256u;
    unsigned int value_index = col - block * 256u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 144ull;
    dst[idx] = termite_q4k_value(bp, value_index);
}

extern "C" __global__ void termite_embedding_lookup_q8_0_f32(
    float* dst,
    const unsigned char* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int out_row = idx / dim;
    unsigned int col = idx - out_row * dim;
    unsigned long long src_row = (unsigned long long)ids[out_row];
    unsigned int row_blocks = dim / 32u;
    unsigned int block = col / 32u;
    unsigned int lane = col - block * 32u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 34ull;
    dst[idx] = termite_q8_0_value(bp, lane);
}

extern "C" __global__ void termite_embedding_lookup_q4_0_f32(
    float* dst,
    const unsigned char* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int out_row = idx / dim;
    unsigned int col = idx - out_row * dim;
    unsigned long long src_row = (unsigned long long)ids[out_row];
    unsigned int row_blocks = dim / 32u;
    unsigned int block = col / 32u;
    unsigned int lane = col - block * 32u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 18ull;
    dst[idx] = termite_q4_0_value(bp, lane);
}

extern "C" __global__ void termite_embedding_lookup_i32_q4_k_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int out_row = idx / dim;
    unsigned int col = idx - out_row * dim;
    unsigned long long src_row = (unsigned long long)((unsigned int)ids[out_row]);
    unsigned int row_blocks = dim / 256u;
    unsigned int block = col / 256u;
    unsigned int value_index = col - block * 256u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 144ull;
    dst[idx] = termite_q4k_value(bp, value_index);
}

extern "C" __global__ void termite_embedding_lookup_i32_q8_0_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int out_row = idx / dim;
    unsigned int col = idx - out_row * dim;
    unsigned long long src_row = (unsigned long long)((unsigned int)ids[out_row]);
    unsigned int row_blocks = dim / 32u;
    unsigned int block = col / 32u;
    unsigned int lane = col - block * 32u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 34ull;
    dst[idx] = termite_q8_0_value(bp, lane);
}

extern "C" __global__ void termite_embedding_lookup_i32_q4_0_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int out_row = idx / dim;
    unsigned int col = idx - out_row * dim;
    unsigned long long src_row = (unsigned long long)((unsigned int)ids[out_row]);
    unsigned int row_blocks = dim / 32u;
    unsigned int block = col / 32u;
    unsigned int lane = col - block * 32u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 18ull;
    dst[idx] = termite_q4_0_value(bp, lane);
}

extern "C" __global__ void termite_deberta_attention_f32(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const float* q_r,
    const float* k_r,
    const long long* mask,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    unsigned int hidden = num_heads * head_dim;
    unsigned int total = batch * seq_len * hidden;
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= total) return;
    unsigned int d = idx % head_dim;
    unsigned int tmp = idx / head_dim;
    unsigned int head = tmp % num_heads;
    tmp /= num_heads;
    unsigned int qi = tmp % seq_len;
    unsigned int b = tmp / seq_len;
    unsigned int head_off = head * head_dim;
    float scale = rsqrtf((float)head_dim * 3.0f);

    float max_score = -3.402823466e+38f;
    for (unsigned int ki = 0; ki < seq_len; ++ki) {
        if (mask[b * seq_len + ki] == 0ll) continue;
        unsigned int rel_idx = qi + seq_len - 1u - ki;
        float score = 0.0f;
        unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
        unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
        unsigned int rel_base = rel_idx * hidden + head_off;
        for (unsigned int j = 0; j < head_dim; ++j) {
            score += q[q_base + j] * k[k_base + j];
            score += q[q_base + j] * k_r[rel_base + j];
            score += q_r[rel_base + j] * k[k_base + j];
        }
        score *= scale;
        max_score = fmaxf(max_score, score);
    }

    float denom = 0.0f;
    float acc = 0.0f;
    for (unsigned int ki = 0; ki < seq_len; ++ki) {
        if (mask[b * seq_len + ki] == 0ll) continue;
        unsigned int rel_idx = qi + seq_len - 1u - ki;
        float score = 0.0f;
        unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
        unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
        unsigned int rel_base = rel_idx * hidden + head_off;
        for (unsigned int j = 0; j < head_dim; ++j) {
            score += q[q_base + j] * k[k_base + j];
            score += q[q_base + j] * k_r[rel_base + j];
            score += q_r[rel_base + j] * k[k_base + j];
        }
        score *= scale;
        float e = expf(score - max_score);
        denom += e;
        acc += e * v[(b * seq_len + ki) * hidden + head_off + d];
    }
    dst[idx] = denom > 0.0f ? acc / denom : 0.0f;
}

extern "C" __global__ void termite_split_last_dim3_f32(
    float* first,
    float* second,
    float* third,
    const float* input,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * dim;
    if (idx >= total) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    unsigned int src = row * dim * 3u + col;
    first[idx] = input[src];
    second[idx] = input[src + dim];
    third[idx] = input[src + dim * 2u];
}
