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

__device__ __forceinline__ void termite_store_half_bytes(unsigned char* dst, float value);
__device__ __forceinline__ float termite_warp_reduce_max_f32(float v);

extern "C" __global__ void termite_fill_f32(float* dst, unsigned int n, float value) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = value;
}

extern "C" __global__ void termite_copy_f32(float* dst, const float* src, unsigned int n) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = src[i];
}

extern "C" __global__ void termite_copy_u8(unsigned char* dst, const unsigned char* src, unsigned int n) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = src[i];
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

extern "C" __global__ void termite_linear_f32_tiled(
    float* dst,
    const float* input,
    const float* weight,
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
        acc += input[row * in_dim + i] * weight[col * in_dim + i];
    }
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[global] = partial[0];
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

extern "C" __global__ void termite_f32_to_i32(
    int* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    dst[idx] = __float2int_rn(input[idx]);
}

// The graph interpreter historically represents i64/u8/bool tensors as f32
// buffers after rounding. Preserve that contract on-device so integer casts
// used by imported training graphs do not force a host round-trip.
extern "C" __global__ void termite_round_f32(
    float* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    dst[idx] = nearbyintf(input[idx]);
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

__device__ __forceinline__ float termite_mtp_load_preproject_weight(
    const void* weight,
    unsigned int index,
    unsigned int dtype
) {
    if (dtype == 0u) return static_cast<const float*>(weight)[index];
    if (dtype == 1u) return __bfloat162float(static_cast<const __nv_bfloat16*>(weight)[index]);
    if (dtype == 2u) return __half2float(static_cast<const half*>(weight)[index]);
    return 0.0f;
}

extern "C" __global__ void termite_gemma4_mtp_preproject_f32(
    float* dst,
    const float* target_embedding,
    const float* activation,
    const void* weight,
    unsigned int backbone_hidden,
    unsigned int draft_hidden,
    unsigned int concat_order,
    unsigned int weight_dtype
) {
    __shared__ float partial[256];
    unsigned int row = blockIdx.x;
    unsigned int tid = threadIdx.x;
    if (row >= draft_hidden || backbone_hidden == 0u) return;

    unsigned int in_dim = backbone_hidden * 2u;
    unsigned int row_base = row * in_dim;
    float sum = 0.0f;
    for (unsigned int i = tid; i < backbone_hidden; i += blockDim.x) {
        float first = target_embedding[i];
        float second = activation[i];
        if (concat_order == 1u) {
            first = activation[i];
            second = target_embedding[i];
        }
        sum += first * termite_mtp_load_preproject_weight(weight, row_base + i, weight_dtype);
        sum += second * termite_mtp_load_preproject_weight(weight, row_base + backbone_hidden + i, weight_dtype);
    }
    partial[tid] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[row] = partial[0];
}

extern "C" __global__ void termite_gemma4_mtp_masked_select_f32(
    unsigned int* dst,
    const float* assistant_hidden,
    const float* logits,
    const void* centroid_weight,
    const float* token_ordering,
    unsigned int hidden_size,
    unsigned int vocab_size,
    unsigned int num_centroids,
    unsigned int top_k,
    unsigned int use_inverse_ordering,
    unsigned int centroid_weight_dtype
) {
    __shared__ float centroid_scores[4096];
    __shared__ float top_scores[128];
    __shared__ unsigned int top_centroids[128];
    __shared__ float block_values[256];
    __shared__ unsigned int block_indices[256];

    if (vocab_size == 0u || hidden_size == 0u || num_centroids == 0u || num_centroids > 4096u || top_k == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }
    if (top_k > 128u) top_k = 128u;
    unsigned int cluster_size = vocab_size / num_centroids;
    if (cluster_size == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }

    for (unsigned int centroid = threadIdx.x; centroid < num_centroids; centroid += blockDim.x) {
        unsigned int row_base = centroid * hidden_size;
        float sum = 0.0f;
        for (unsigned int i = 0u; i < hidden_size; ++i) {
            sum += assistant_hidden[i] * termite_mtp_load_preproject_weight(centroid_weight, row_base + i, centroid_weight_dtype);
        }
        centroid_scores[centroid] = sum;
    }
    __syncthreads();

    if (threadIdx.x == 0u) {
        for (unsigned int i = 0u; i < top_k; ++i) {
            top_scores[i] = -3.402823466e+38f;
            top_centroids[i] = 0u;
        }
        for (unsigned int centroid = 0u; centroid < num_centroids; ++centroid) {
            float score = centroid_scores[centroid];
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

extern "C" __global__ void termite_gemma4_mtp_centroid_scores_hidden_f32(
    float* centroid_scores,
    const float* assistant_hidden,
    const void* centroid_weight,
    unsigned int hidden_size,
    unsigned int num_centroids,
    unsigned int centroid_weight_dtype
) {
    __shared__ float partial[256];
    unsigned int centroid = blockIdx.x;
    unsigned int tid = threadIdx.x;
    if (centroid >= num_centroids || hidden_size == 0u) return;

    unsigned int row_base = centroid * hidden_size;
    float sum = 0.0f;
    for (unsigned int i = tid; i < hidden_size; i += blockDim.x) {
        sum += assistant_hidden[i] * termite_mtp_load_preproject_weight(centroid_weight, row_base + i, centroid_weight_dtype);
    }
    partial[tid] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) centroid_scores[centroid] = partial[0];
}

extern "C" __global__ void termite_gemma4_mtp_centroid_topk_u32(
    unsigned int* top_centroids,
    const float* centroid_scores,
    unsigned int num_centroids,
    unsigned int top_k
) {
    __shared__ float top_scores[128];
    __shared__ unsigned int top_ids[128];
    if (threadIdx.x != 0u) return;
    if (top_k > 128u) top_k = 128u;
    if (top_k == 0u) return;
    for (unsigned int i = 0u; i < top_k; ++i) {
        top_scores[i] = -3.402823466e+38f;
        top_ids[i] = 0u;
    }
    for (unsigned int centroid = 0u; centroid < num_centroids; ++centroid) {
        float score = centroid_scores[centroid];
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
            top_ids[i] = top_ids[i - 1u];
        }
        top_scores[insert_at] = score;
        top_ids[insert_at] = centroid;
    }
    for (unsigned int i = 0u; i < top_k; ++i) top_centroids[i] = top_ids[i];
}

extern "C" __global__ void termite_gemma4_mtp_restricted_lm_head_scores_f32(
    float* partial_values,
    unsigned int* partial_tokens,
    const float* assistant_hidden,
    const void* lm_head_weight,
    const float* token_ordering,
    const unsigned int* top_centroids,
    unsigned int hidden_size,
    unsigned int vocab_size,
    unsigned int num_centroids,
    unsigned int top_k,
    unsigned int lm_head_dtype
) {
    __shared__ float partial[256];
    unsigned int candidate = blockIdx.x;
    unsigned int tid = threadIdx.x;
    unsigned int cluster_size = num_centroids == 0u ? 0u : vocab_size / num_centroids;
    unsigned int candidate_count = top_k * cluster_size;
    if (candidate >= candidate_count || cluster_size == 0u || hidden_size == 0u) return;

    unsigned int top_slot = candidate / cluster_size;
    unsigned int offset = candidate - top_slot * cluster_size;
    unsigned int ordered_pos = top_centroids[top_slot] * cluster_size + offset;
    float token_f = ordered_pos < vocab_size ? token_ordering[ordered_pos] : -1.0f;
    unsigned int token = token_f >= 0.0f ? (unsigned int)token_f : 0u;
    bool valid = token_f >= 0.0f && token < vocab_size;

    float sum = valid ? 0.0f : -3.402823466e+38f;
    if (valid) {
        unsigned int row_base = token * hidden_size;
        for (unsigned int i = tid; i < hidden_size; i += blockDim.x) {
            sum += assistant_hidden[i] * termite_mtp_load_preproject_weight(lm_head_weight, row_base + i, lm_head_dtype);
        }
    }
    partial[tid] = sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) {
        partial_values[candidate] = partial[0];
        partial_tokens[candidate] = token;
    }
}

extern "C" __global__ void termite_gemma4_mtp_reduce_token_scores_f32(
    unsigned int* dst,
    const float* partial_values,
    const unsigned int* partial_tokens,
    unsigned int candidate_count
) {
    __shared__ float block_values[256];
    __shared__ unsigned int block_indices[256];
    unsigned int tid = threadIdx.x;
    float best_score = -3.402823466e+38f;
    unsigned int best_token = 0u;
    for (unsigned int i = tid; i < candidate_count; i += blockDim.x) {
        float score = partial_values[i];
        unsigned int token = partial_tokens[i];
        if (score > best_score || (score == best_score && token < best_token)) {
            best_score = score;
            best_token = token;
        }
    }
    block_values[tid] = best_score;
    block_indices[tid] = best_token;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            float rhs_score = block_values[tid + stride];
            unsigned int rhs_token = block_indices[tid + stride];
            float lhs_score = block_values[tid];
            unsigned int lhs_token = block_indices[tid];
            if (rhs_score > lhs_score || (rhs_score == lhs_score && rhs_token < lhs_token)) {
                block_values[tid] = rhs_score;
                block_indices[tid] = rhs_token;
            }
        }
        __syncthreads();
    }
    if (tid == 0u) dst[0] = block_indices[0];
}

extern "C" __global__ void termite_gemma4_mtp_masked_select_hidden_f32(
    unsigned int* dst,
    const float* assistant_hidden,
    const void* lm_head_weight,
    const void* centroid_weight,
    const float* token_ordering,
    unsigned int hidden_size,
    unsigned int vocab_size,
    unsigned int num_centroids,
    unsigned int top_k,
    unsigned int use_inverse_ordering,
    unsigned int lm_head_dtype,
    unsigned int centroid_weight_dtype
) {
    __shared__ float centroid_scores[4096];
    __shared__ float top_scores[128];
    __shared__ unsigned int top_centroids[128];
    __shared__ float block_values[256];
    __shared__ unsigned int block_indices[256];

    if (vocab_size == 0u || hidden_size == 0u || num_centroids == 0u || num_centroids > 4096u || top_k == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }
    if (top_k > 128u) top_k = 128u;
    unsigned int cluster_size = vocab_size / num_centroids;
    if (cluster_size == 0u) {
        if (threadIdx.x == 0u) dst[0] = 0u;
        return;
    }

    for (unsigned int centroid = threadIdx.x; centroid < num_centroids; centroid += blockDim.x) {
        unsigned int row_base = centroid * hidden_size;
        float sum = 0.0f;
        for (unsigned int i = 0u; i < hidden_size; ++i) {
            sum += assistant_hidden[i] * termite_mtp_load_preproject_weight(centroid_weight, row_base + i, centroid_weight_dtype);
        }
        centroid_scores[centroid] = sum;
    }
    __syncthreads();

    if (threadIdx.x == 0u) {
        for (unsigned int i = 0u; i < top_k; ++i) {
            top_scores[i] = -3.402823466e+38f;
            top_centroids[i] = 0u;
        }
        for (unsigned int centroid = 0u; centroid < num_centroids; ++centroid) {
            float score = centroid_scores[centroid];
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
            unsigned int row_base = token * hidden_size;
            float score = 0.0f;
            for (unsigned int i = 0u; i < hidden_size; ++i) {
                score += assistant_hidden[i] * termite_mtp_load_preproject_weight(lm_head_weight, row_base + i, lm_head_dtype);
            }
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
            unsigned int row_base = token * hidden_size;
            float score = 0.0f;
            for (unsigned int i = 0u; i < hidden_size; ++i) {
                score += assistant_hidden[i] * termite_mtp_load_preproject_weight(lm_head_weight, row_base + i, lm_head_dtype);
            }
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

extern "C" __global__ void termite_rms_norm_f32_bf16(
    float* dst,
    unsigned short* dst_bf16,
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
        unsigned int idx = base + i;
        float value = input[idx] * scale * weight[i];
        dst[idx] = value;
        dst_bf16[idx] = termite_f32_to_bf16(value);
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

extern "C" __global__ void termite_rms_norm_add_f32_bf16(
    float* dst,
    unsigned short* dst_bf16,
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
        float value = input[idx] * norm_scale * weight[i] + residual[idx];
        dst[idx] = value;
        dst_bf16[idx] = termite_f32_to_bf16(value);
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

__device__ __forceinline__ float termite_erf_approx_f32(float x) {
    float sign = x < 0.0f ? -1.0f : 1.0f;
    float ax = fabsf(x);
    float t = 1.0f / (1.0f + 0.3275911f * ax);
    float poly = (((((1.061405429f * t - 1.453152027f) * t) + 1.421413741f) * t - 0.284496736f) * t + 0.254829592f) * t;
    return sign * (1.0f - poly * expf(-(ax * ax)));
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
        out = isfinite(x) ? 0.5f * x * (1.0f + termite_erf_approx_f32(x * 0.7071067811865476f)) : 0.0f;
    } else if (op == 21u) {
        if (!isfinite(x)) {
            out = 0.0f;
        } else {
            float cdf = 0.5f * (1.0f + termite_erf_approx_f32(x * 0.7071067811865476f));
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
            derivative = 0.5f * (1.0f + t) + 0.5f * x * (1.0f - t * t) * 0.7978845608028654f * (1.0f + 0.134145f * x2);
        }
        out = isfinite(derivative) ? y * derivative : 0.0f;
    }
    dst[i] = out;
}

extern "C" __global__ void termite_elementwise_broadcast_f32(
    float* dst,
    const float* a,
    const float* b,
    unsigned int count,
    unsigned int a_rank,
    unsigned int b_rank,
    unsigned int output_rank,
    unsigned int op,
    unsigned int a0, unsigned int a1, unsigned int a2, unsigned int a3,
    unsigned int a4, unsigned int a5, unsigned int a6, unsigned int a7,
    unsigned int b0, unsigned int b1, unsigned int b2, unsigned int b3,
    unsigned int b4, unsigned int b5, unsigned int b6, unsigned int b7,
    unsigned int o0, unsigned int o1, unsigned int o2, unsigned int o3,
    unsigned int o4, unsigned int o5, unsigned int o6, unsigned int o7
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    unsigned int a_dims[8] = { a0, a1, a2, a3, a4, a5, a6, a7 };
    unsigned int b_dims[8] = { b0, b1, b2, b3, b4, b5, b6, b7 };
    unsigned int out_dims[8] = { o0, o1, o2, o3, o4, o5, o6, o7 };
    unsigned int coords[8] = { 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u };
    unsigned int remaining = idx;
    for (int d = (int)output_rank - 1; d >= 0; --d) {
        coords[d] = remaining % out_dims[d];
        remaining /= out_dims[d];
    }
    unsigned int a_idx = 0u;
    for (unsigned int d = 0; d < a_rank; ++d) {
        unsigned int out_d = output_rank - a_rank + d;
        unsigned int coord = a_dims[d] == 1u ? 0u : coords[out_d];
        a_idx = a_idx * a_dims[d] + coord;
    }
    unsigned int b_idx = 0u;
    for (unsigned int d = 0; d < b_rank; ++d) {
        unsigned int out_d = output_rank - b_rank + d;
        unsigned int coord = b_dims[d] == 1u ? 0u : coords[out_d];
        b_idx = b_idx * b_dims[d] + coord;
    }
    float x = a[a_idx];
    float y = b[b_idx];
    if (op == 0u) dst[idx] = x + y;
    else if (op == 1u) dst[idx] = x * y;
    else if (op == 8u) dst[idx] = x - y;
    else if (op == 9u) dst[idx] = x / y;
    else dst[idx] = x < y ? 1.0f : 0.0f;
}

extern "C" __global__ void termite_primitive_where_f32(
    float* output,
    const float* cond,
    const float* on_true,
    const float* on_false,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) output[idx] = cond[idx] != 0.0f ? on_true[idx] : on_false[idx];
}

// Dense dot_general specialization used by training attention gradients.
// Batch dimensions are flattened ahead of the final matrix dimensions:
//   lhs [batch, m, k]
//   rhs [batch, n, k] when rhs_contract_last != 0, otherwise [batch, k, n]
//   out [batch, m, n]
extern "C" __global__ void termite_primitive_batched_dot_f32(
    float* output,
    const float* lhs,
    const float* rhs,
    unsigned int batch_count,
    unsigned int m,
    unsigned int n,
    unsigned int k,
    unsigned int rhs_contract_last
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int output_count = batch_count * m * n;
    if (idx >= output_count) return;

    unsigned int col = idx % n;
    unsigned int row_batch = idx / n;
    unsigned int row = row_batch % m;
    unsigned int batch = row_batch / m;
    unsigned int lhs_base = (batch * m + row) * k;
    float acc = 0.0f;
    if (rhs_contract_last != 0u) {
        unsigned int rhs_base = (batch * n + col) * k;
        for (unsigned int inner = 0; inner < k; ++inner) {
            acc += lhs[lhs_base + inner] * rhs[rhs_base + inner];
        }
    } else {
        unsigned int rhs_base = batch * k * n + col;
        for (unsigned int inner = 0; inner < k; ++inner) {
            acc += lhs[lhs_base + inner] * rhs[rhs_base + inner * n];
        }
    }
    output[idx] = acc;
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

extern "C" __global__ void termite_activation_multiply_slice_last_dim_f32(
    float* dst,
    const float* gate,
    const float* source,
    unsigned int rows,
    unsigned int source_cols,
    unsigned int start,
    unsigned int out_cols,
    unsigned int activation
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * out_cols;
    if (i >= count) return;
    unsigned int row = i / out_cols;
    unsigned int col = i - row * out_cols;
    dst[i] = termite_decoder_activation_f32(gate[i], activation) * source[row * source_cols + start + col];
}

extern "C" __global__ void termite_embedding_lookup_f32(
    float* dst,
    const float* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim,
    float scale
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    long long id = ids[row];
    dst[idx] = weight[(unsigned long long)id * dim + col] * scale;
}

extern "C" __global__ void termite_embedding_lookup_bf16_weight_f32(
    float* dst,
    const unsigned short* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim,
    float scale
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    long long id = ids[row];
    dst[idx] = termite_bf16_to_f32(weight[(unsigned long long)id * dim + col]) * scale;
}

extern "C" __global__ void termite_embedding_lookup_i32_f32(
    float* dst,
    const float* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim,
    float scale
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = total * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    int id = ids[row];
    dst[idx] = weight[(unsigned long long)((unsigned int)id) * dim + col] * scale;
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

extern "C" __global__ void termite_rope_decode_scalars_f32(
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
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) position_offset = decode_scalars[0];
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

extern "C" __global__ void termite_decode_scalars_advance(
    unsigned int* decode_scalars,
    unsigned int position_delta,
    unsigned int query_position_delta,
    unsigned int kv_seq_delta,
    unsigned int total_sequence_delta,
    unsigned int kv_position_delta
) {
    if (blockIdx.x != 0 || threadIdx.x != 0 || decode_scalars == 0) return;
    decode_scalars[0] += position_delta;
    decode_scalars[1] += query_position_delta;
    decode_scalars[2] += kv_seq_delta;
    decode_scalars[3] += total_sequence_delta;
    decode_scalars[4] += kv_position_delta;
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

extern "C" __global__ void termite_rope_scaled_decode_scalars_f32(
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
    float scale,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) position_offset = decode_scalars[0];
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

__device__ __forceinline__ float termite_warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float termite_block_reduce_sum_f32(float value, float* warp_sums) {
    unsigned int lane = threadIdx.x & 31u;
    unsigned int warp = threadIdx.x >> 5;
    unsigned int warp_count = (blockDim.x + 31u) >> 5;
    value = termite_warp_reduce_sum_f32(value);
    if (lane == 0u) warp_sums[warp] = value;
    __syncthreads();
    value = (warp == 0u && lane < warp_count) ? warp_sums[lane] : 0.0f;
    if (warp == 0u) value = termite_warp_reduce_sum_f32(value);
    return value;
}

extern "C" __global__ void termite_gqa_attention_decode_scalars_fast_f32(
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
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }

    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    unsigned int lane = threadIdx.x;
    unsigned int head = blockIdx.x;
    if (batch != 1u ||
        q_seq_len != 1u ||
        mask_len != 0u ||
        bias_mode != 0u ||
        head >= num_heads ||
        head_dim > 512u ||
        (head_dim & 31u) != 0u ||
        blockDim.x < head_dim ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u) return;

    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    unsigned int query_pos = query_position_offset;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int kv_hidden = num_kv_heads * head_dim;
    unsigned int q_base = head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            float score = dot * scale;
            float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}

extern "C" __global__ void termite_gqa_attention_prefill_fast_f32(
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
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;

    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    unsigned int lane = threadIdx.x;
    unsigned int block = blockIdx.x;
    unsigned int total_blocks = batch * q_seq_len * num_heads;
    if (batch != 1u ||
        q_seq_len <= 1u ||
        block >= total_blocks ||
        mask_len != 0u ||
        bias_mode != 0u ||
        head_dim > 512u ||
        (head_dim & 31u) != 0u ||
        blockDim.x < head_dim ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u) return;

    unsigned int head = block % num_heads;
    unsigned int tmp = block / num_heads;
    unsigned int qi = tmp % q_seq_len;
    unsigned int b = tmp / q_seq_len;
    unsigned int query_pos = query_position_offset + qi;

    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int kv_hidden = num_kv_heads * head_dim;
    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            float score = dot * scale;
            float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
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
        kv_position_offset = decode_scalars[4];
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

__device__ __forceinline__ void termite_tq_store_f16_byte(unsigned char* dst, float value, unsigned int byte) {
    unsigned short raw = __half_as_ushort(__float2half_rn(value));
    dst[byte] = byte == 0u ? (unsigned char)(raw & 0xffu) : (unsigned char)(raw >> 8);
}

__device__ __forceinline__ float termite_tq_f16_value(const unsigned char* row, unsigned int value_index) {
    const unsigned char* src = row + value_index * 2u;
    unsigned short raw = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    return __half2float(__ushort_as_half(raw));
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

__device__ float termite_tq_int4_group_scale(const float* src, unsigned int count) {
    float max_abs = 0.0f;
    for (unsigned int d = 0u; d < count; ++d) {
        max_abs = fmaxf(max_abs, fabsf(src[d]));
    }
    return max_abs == 0.0f ? 1.0f : max_abs / 7.0f;
}

__device__ __forceinline__ signed char termite_tq_sign_extend_i4(unsigned char value) {
    unsigned char nibble = value & 0x0fu;
    return (signed char)((nibble & 0x08u) != 0u ? (nibble | 0xf0u) : nibble);
}

__device__ __forceinline__ float termite_tq_value_int4_group(
    const unsigned char* row,
    unsigned int value_index
) {
    const unsigned int group_size = 32u;
    const unsigned int group_bytes = 18u;
    unsigned int group = value_index / group_size;
    unsigned int lane = value_index - group * group_size;
    const unsigned char* group_row = row + group * group_bytes;
    float scale = termite_tq_f16_value(group_row, 0u);
    unsigned char packed = group_row[2u + (lane >> 1)];
    unsigned char nibble = (lane & 1u) == 0u ? (packed & 0x0fu) : ((packed >> 4) & 0x0fu);
    return (float)termite_tq_sign_extend_i4(nibble) * scale;
}

__device__ __forceinline__ float termite_tq_value_at(
    const unsigned char* row,
    unsigned int kv_head,
    unsigned int lane,
    unsigned int head_dim,
    unsigned int value_format
) {
    unsigned int value_index = kv_head * head_dim + lane;
    if (value_format == 0u) return reinterpret_cast<const float*>(row)[value_index];
    if (value_format == 1u) return termite_tq_value_int8_per_head(row, kv_head, lane, head_dim);
    if (value_format == 2u) return termite_tq_f16_value(row, value_index);
    if (value_format == 3u) return termite_tq_value_int4_group(row, value_index);
    return 0.0f;
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
        } else if (format == 2u) {
            unsigned int value_index = byte >> 1;
            if (value_index < row_width) termite_tq_store_f16_byte(dst_row + value_index * 2u, src_row[value_index], byte & 1u);
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
        } else if (value_format == 2u) {
            unsigned int value_index = lane_or_byte >> 1;
            if (value_index < row_width) termite_tq_store_f16_byte(dst_row + value_index * 2u, src_row[value_index], lane_or_byte & 1u);
        } else if (value_format == 3u) {
            const unsigned int group_size = 32u;
            const unsigned int group_bytes = 18u;
            unsigned int group = lane_or_byte / group_bytes;
            unsigned int group_byte = lane_or_byte - group * group_bytes;
            unsigned int value_start = group * group_size;
            if (value_start >= row_width) return;
            unsigned int remaining = row_width - value_start;
            if (remaining > group_size) remaining = group_size;
            const float* src_group = src_row + value_start;
            float scale_f32 = termite_tq_int4_group_scale(src_group, remaining);
            if (group_byte < 2u) {
                termite_tq_store_f16_byte(dst_row + group * group_bytes, scale_f32, group_byte);
            } else {
                unsigned int pair = (group_byte - 2u) * 2u;
                if (pair >= group_size) return;
                float inv_scale = 1.0f / scale_f32;
                float lo_f = pair < remaining ? roundf(src_group[pair] * inv_scale) : 0.0f;
                float hi_f = (pair + 1u) < remaining ? roundf(src_group[pair + 1u] * inv_scale) : 0.0f;
                lo_f = fminf(fmaxf(lo_f, -7.0f), 7.0f);
                hi_f = fminf(fmaxf(hi_f, -7.0f), 7.0f);
                signed char lo_i = (signed char)lo_f;
                signed char hi_i = (signed char)hi_f;
                dst_row[group * group_bytes + group_byte] =
                    ((unsigned char)lo_i & 0x0fu) | (unsigned char)(((unsigned char)hi_i & 0x0fu) << 4);
            }
        }
    }
}

extern "C" __global__ void termite_gqa_attention_decode_turboquant_fast_f32(
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
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }

    const float neg_inf = -3.402823466e+38f;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
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
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_base = head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = neg_inf;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        bool valid = physical_token != 0xffffffffu;
        const unsigned char* k_row = valid ? k + physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int value_index = kv_head * head_dim + lane;
            float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            if (valid) {
                float score = dot * scale;
                float next_max = fmaxf(shared_max_score, score);
                shared_alpha = expf(shared_max_score - next_max);
                shared_beta = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha + shared_beta;
                shared_max_score = next_max;
            } else {
                shared_alpha = 1.0f;
                shared_beta = 0.0f;
            }
        }
        __syncthreads();
        if (lane < head_dim) {
            acc *= shared_alpha;
            if (valid) {
                const unsigned char* v_row = v + physical_token * value_row_bytes;
                float value = termite_tq_value_at(v_row, kv_head, lane, head_dim, value_format);
                acc += shared_beta * value;
            }
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void termite_gqa_attention_prefill_turboquant_fast_f32(
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
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars
) {
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
    (void)decode_scalars;

    const float neg_inf = -3.402823466e+38f;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
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
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = neg_inf;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        bool valid = physical_token != 0xffffffffu;
        const unsigned char* k_row = valid ? k + physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int value_index = kv_head * head_dim + lane;
            float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            if (valid) {
                float score = dot * scale;
                float next_max = fmaxf(shared_max_score, score);
                shared_alpha = expf(shared_max_score - next_max);
                shared_beta = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha + shared_beta;
                shared_max_score = next_max;
            } else {
                shared_alpha = 1.0f;
                shared_beta = 0.0f;
            }
        }
        __syncthreads();
        if (lane < head_dim) {
            acc *= shared_alpha;
            if (valid) {
                const unsigned char* v_row = v + physical_token * value_row_bytes;
                float value = termite_tq_value_at(v_row, kv_head, lane, head_dim, value_format);
                acc += shared_beta * value;
            }
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
}

// Tiled prefill attention over turboquant pages. Each warp owns TWO query
// rows (TILE_M = 16 rows per block, 8 warps) so every K/V element staged
// through shared memory feeds two FMAs. K/V tiles are staged as BF16
// (bit-shift exact to read back), halving shared-memory footprint and
// bank traffic versus F32 tiles. Keys are staged in TILE_N chunks so each
// K/V row is decoded once per block.
// Grid: (num_heads, ceil(q_seq_len / TILE_M)); blockDim.x = 256.
// Dynamic shared memory: tile_n * (head_dim + 2) * 2 bytes, where
// tile_n = 32 for head_dim <= 256 and 16 for head_dim <= 512.
#define TERMITE_TQ_PREFILL_TILE_M 16u

__device__ __forceinline__ float termite_bf16_bits_to_f32(unsigned short bits) {
    return __uint_as_float(((unsigned int)bits) << 16);
}

extern "C" __global__ void termite_gqa_attention_prefill_turboquant_tiled_f32(
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
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars
) {
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

// Tensor-core (wmma f16, 16x16x16) variant of the tiled prefill kernel.
// One block handles 16 query rows for one head; keys stream through shared
// memory in 64-token tiles. Q is staged to shared memory as f16 once per
// block. Per key tile: S = Q*K^T comes from wmma GEMMs (warps 0-3, one
// 16-key n-chunk each), online softmax runs per query row (warp w owns rows
// 2w and 2w+1 with the same masking as the tiled kernel), then O += P*V
// accumulates through wmma into an f32 tile in shared memory, rescaled by
// alpha before each accumulate. f16 tiles are safe here: attention inputs
// are post-norm and P is in [0,1], where f16 carries more mantissa than
// bf16. Requires the >48KB dynamic-smem opt-in on the function (65280 bytes
// at head_dim 256); host code sets CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE.
extern "C" __global__ void termite_gqa_attention_prefill_turboquant_mma_f32(
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
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars
) {
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
    (void)decode_scalars;

    const float neg_inf = -3.402823466e+38f;
    // 64-key tiles for head_dim <= 256; 32-key tiles for the 512-dim global
    // layers keep the f32 output tile within the sm80+ shared-memory budget.
    const unsigned int tile_n = head_dim <= 256u ? 64u : 32u;
    extern __shared__ __align__(32) unsigned char mma_prefill_smem[];
    __shared__ unsigned int tile_phys[64];
    __shared__ float alpha_sh[TERMITE_TQ_PREFILL_TILE_M];

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

    unsigned int head = blockIdx.x;
    unsigned int tile_row_start = blockIdx.y * TERMITE_TQ_PREFILL_TILE_M;
    if (head >= num_heads || tile_row_start >= q_seq_len) return;

    unsigned int tid = threadIdx.x;
    unsigned int warp = tid >> 5;
    unsigned int lane = tid & 31u;

    // Shared-memory partition (base is 16-byte aligned; every section size is
    // a multiple of 16 bytes because head_dim is a multiple of 32).
    const unsigned int kv_pitch = head_dim + 8u;
    const unsigned int sp_pitch = 72u;
    half* q_tile = reinterpret_cast<half*>(mma_prefill_smem);
    half* kv_tile = q_tile + TERMITE_TQ_PREFILL_TILE_M * head_dim;
    float* s_tile = reinterpret_cast<float*>(kv_tile + tile_n * kv_pitch);
    half* p_tile = reinterpret_cast<half*>(s_tile + TERMITE_TQ_PREFILL_TILE_M * sp_pitch);
    float* o_tile = reinterpret_cast<float*>(p_tile + TERMITE_TQ_PREFILL_TILE_M * sp_pitch);

    // Each warp owns two adjacent query rows for softmax state and output.
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

    // Stage the Q tile as f16 (rows past q_seq_len clamp to the last row;
    // their scores are fully masked below) and zero the output accumulator.
    for (unsigned int idx = tid; idx < TERMITE_TQ_PREFILL_TILE_M * head_dim; idx += 256u) {
        unsigned int row = idx / head_dim;
        unsigned int col = idx - row * head_dim;
        unsigned int qi = tile_row_start + row;
        unsigned int qi_clamped = qi < q_seq_len ? qi : (q_seq_len - 1u);
        q_tile[idx] = __float2half(q[qi_clamped * q_hidden + head * head_dim + col]);
        o_tile[idx] = 0.0f;
    }
    __syncthreads();

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

        for (unsigned int idx = tid; idx < tile_n * head_dim; idx += 256u) {
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
            kv_tile[row * kv_pitch + col] = __float2half(key_value);
        }
        __syncthreads();

        // S = Q * K^T on tensor cores: warp w owns keys [w*16, w*16+16).
        // K is stored [key][dim], which is exactly the col-major B fragment
        // for k=dims, n=keys.
        if (warp < (tile_n >> 4)) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> s_acc;
            wmma::fill_fragment(s_acc, 0.0f);
            for (unsigned int kb = 0u; kb < head_dim; kb += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major> b_frag;
                wmma::load_matrix_sync(a_frag, q_tile + kb, head_dim);
                wmma::load_matrix_sync(b_frag, kv_tile + (warp * 16u) * kv_pitch + kb, kv_pitch);
                wmma::mma_sync(s_acc, a_frag, b_frag, s_acc);
            }
            wmma::store_matrix_sync(s_tile + warp * 16u, s_acc, sp_pitch, wmma::mem_row_major);
        }
        __syncthreads();

        // Online softmax per query row; lanes cover keys {lane, lane+32}.
        for (unsigned int sub = 0u; sub < 2u; ++sub) {
            unsigned int row = warp * 2u + sub;
            unsigned int key_start_r = sub == 0u ? key_start0 : key_start1;
            unsigned int key_end_r = sub == 0u ? key_end0 : key_end1;
            float* m_run = sub == 0u ? &m_run0 : &m_run1;
            float* d_run = sub == 0u ? &d_run0 : &d_run1;

            unsigned int ja = lane;
            unsigned int jb = lane + 32u;
            unsigned int kia = n0 + ja;
            unsigned int kib = n0 + jb;
            bool valid_a = ja < n_count && tile_phys[ja] != 0xffffffffu && kia >= key_start_r && kia < key_end_r;
            bool valid_b = jb < n_count && tile_phys[jb] != 0xffffffffu && kib >= key_start_r && kib < key_end_r;
            float score_a = valid_a ? s_tile[row * sp_pitch + ja] * scale : neg_inf;
            float score_b = valid_b ? s_tile[row * sp_pitch + jb] * scale : neg_inf;

            float tile_max = fmaxf(score_a, score_b);
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
                tile_max = fmaxf(tile_max, __shfl_down_sync(0xffffffffu, tile_max, offset));
            }
            tile_max = __shfl_sync(0xffffffffu, tile_max, 0u);

            float alpha = 1.0f;
            float p_a = 0.0f, p_b = 0.0f;
            if (tile_max > neg_inf) {
                float new_max = fmaxf(*m_run, tile_max);
                alpha = *m_run > neg_inf ? expf(*m_run - new_max) : 0.0f;
                p_a = valid_a ? expf(score_a - new_max) : 0.0f;
                p_b = valid_b ? expf(score_b - new_max) : 0.0f;
                *m_run = new_max;
            }
            float sum_p = p_a + p_b;
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
                sum_p += __shfl_down_sync(0xffffffffu, sum_p, offset);
            }
            sum_p = __shfl_sync(0xffffffffu, sum_p, 0u);
            if (tile_max > neg_inf) *d_run = *d_run * alpha + sum_p;

            p_tile[row * sp_pitch + ja] = __float2half(p_a);
            p_tile[row * sp_pitch + jb] = __float2half(p_b);
            if (lane == 0u) alpha_sh[row] = alpha;
        }
        __syncthreads();

        // Rescale the running output by this tile's alpha, then swap the V
        // tile into the K buffer.
        for (unsigned int idx = tid; idx < TERMITE_TQ_PREFILL_TILE_M * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            o_tile[idx] *= alpha_sh[row];
        }
        for (unsigned int idx = tid; idx < tile_n * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            unsigned int col = idx - row * head_dim;
            unsigned int phys = tile_phys[row];
            float value = 0.0f;
            if (phys != 0xffffffffu) {
                const unsigned char* v_row = v + (size_t)phys * value_row_bytes;
                value = termite_tq_value_at(v_row, kv_head, col, head_dim, value_format);
            }
            kv_tile[row * kv_pitch + col] = __float2half(value);
        }
        __syncthreads();

        // O += P * V on tensor cores. Keys past n_count carry p == 0, so all
        // tile_n/16 k-chunks are always safe.
        for (unsigned int nc = warp; nc < (head_dim >> 4); nc += 8u) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> o_acc;
            wmma::load_matrix_sync(o_acc, o_tile + nc * 16u, head_dim, wmma::mem_row_major);
            for (unsigned int kb = 0u; kb < (tile_n >> 4); ++kb) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
                wmma::load_matrix_sync(p_frag, p_tile + kb * 16u, sp_pitch);
                wmma::load_matrix_sync(v_frag, kv_tile + (kb * 16u) * kv_pitch + nc * 16u, kv_pitch);
                wmma::mma_sync(o_acc, p_frag, v_frag, o_acc);
            }
            wmma::store_matrix_sync(o_tile + nc * 16u, o_acc, head_dim, wmma::mem_row_major);
        }
        __syncthreads();
    }

    if (row_valid0) {
        unsigned int out_base = qi0 * q_hidden + head * head_dim;
        float inv_denom = d_run0 > 0.0f ? 1.0f / d_run0 : 0.0f;
        for (unsigned int col = lane; col < head_dim; col += 32u) {
            dst[out_base + col] = o_tile[(warp * 2u) * head_dim + col] * inv_denom;
        }
    }
    if (row_valid1) {
        unsigned int out_base = qi1 * q_hidden + head * head_dim;
        float inv_denom = d_run1 > 0.0f ? 1.0f / d_run1 : 0.0f;
        for (unsigned int col = lane; col < head_dim; col += 32u) {
            dst[out_base + col] = o_tile[(warp * 2u + 1u) * head_dim + col] * inv_denom;
        }
    }
}

// TILE_M=32 variant of the tensor-core prefill kernel (head_dim <= 256,
// sm80+ shared-memory budget: 320*head_dim + 14848 bytes, 96768 at 256).
// Doubling the query tile amortizes the K/V dequant + shared-memory staging
// over twice as many scores: each warp owns one 16x16 wmma tile of the
// 32x64 score matrix (m-chunk = warp/4, n-chunk = warp%4), then four query
// rows for the online softmax, then 2*(head_dim/16) output tiles round-robin
// for O += P*V.
extern "C" __global__ void termite_gqa_attention_prefill_turboquant_mma_m32_f32(
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
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars
) {
    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
    (void)decode_scalars;

    const float neg_inf = -3.402823466e+38f;
    const unsigned int tile_m = 32u;
    const unsigned int tile_n = 64u;
    extern __shared__ __align__(32) unsigned char mma_m32_prefill_smem[];
    __shared__ unsigned int tile_phys[64];
    __shared__ float alpha_sh[32];

    if (batch != 1u ||
        q_seq_len <= 1u ||
        mask_len != 0u ||
        bias_mode != 0u ||
        (format != 0u && format != 2u) ||
        base_key_row_bytes != key_row_bytes ||
        head_dim > 256u ||
        (head_dim & 31u) != 0u ||
        blockDim.x != 256u ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        key_row_bytes == 0u ||
        value_row_bytes == 0u) return;

    unsigned int head = blockIdx.x;
    unsigned int tile_row_start = blockIdx.y * tile_m;
    if (head >= num_heads || tile_row_start >= q_seq_len) return;

    unsigned int tid = threadIdx.x;
    unsigned int warp = tid >> 5;
    unsigned int lane = tid & 31u;

    const unsigned int kv_pitch = head_dim + 8u;
    const unsigned int sp_pitch = 72u;
    half* q_tile = reinterpret_cast<half*>(mma_m32_prefill_smem);
    half* kv_tile = q_tile + tile_m * head_dim;
    float* s_tile = reinterpret_cast<float*>(kv_tile + tile_n * kv_pitch);
    half* p_tile = reinterpret_cast<half*>(s_tile + tile_m * sp_pitch);
    float* o_tile = reinterpret_cast<float*>(p_tile + tile_m * sp_pitch);

    // Each warp owns four adjacent query rows for softmax state and output.
    unsigned int qi_base = tile_row_start + warp * 4u;
    bool row_valid[4];
    unsigned int key_start_r[4];
    unsigned int key_end_r[4];
    float m_run[4];
    float d_run[4];
    for (unsigned int r = 0u; r < 4u; ++r) {
        unsigned int qi = qi_base + r;
        row_valid[r] = qi < q_seq_len;
        unsigned int qi_clamped = row_valid[r] ? qi : (q_seq_len - 1u);
        unsigned int key_start = 0u, key_end = 0u;
        unsigned int query_pos = query_position_offset + qi_clamped;
        if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
            unsigned int visible = query_pos - kv_position_offset + 1u;
            key_end = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u) {
                unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    key_start = window_start_abs - kv_position_offset;
                    if (key_start > key_end) key_start = key_end;
                }
            }
        }
        if (!row_valid[r]) { key_start = 0u; key_end = 0u; }
        key_start_r[r] = key_start;
        key_end_r[r] = key_end;
        m_run[r] = neg_inf;
        d_run[r] = 0.0f;
    }

    // Block-wide key range: causal end grows with qi and the sliding-window
    // start grows with qi, so the union is [start(first row), end(last row)].
    unsigned int qi_last = tile_row_start + tile_m - 1u;
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

    for (unsigned int idx = tid; idx < tile_m * head_dim; idx += 256u) {
        unsigned int row = idx / head_dim;
        unsigned int col = idx - row * head_dim;
        unsigned int qi = tile_row_start + row;
        unsigned int qi_clamped = qi < q_seq_len ? qi : (q_seq_len - 1u);
        q_tile[idx] = __float2half(q[qi_clamped * q_hidden + head * head_dim + col]);
        o_tile[idx] = 0.0f;
    }
    __syncthreads();

    for (unsigned int n0 = block_key_start; n0 < block_key_end; n0 += tile_n) {
        unsigned int n_count = block_key_end - n0;
        if (n_count > tile_n) n_count = tile_n;

        if (tid < tile_n) {
            tile_phys[tid] = tid < n_count
                ? termite_tq_physical_token(n0 + tid, block_table, block_count, page_size_tokens, physical_token_capacity)
                : 0xffffffffu;
        }
        __syncthreads();

        for (unsigned int idx = tid; idx < tile_n * head_dim; idx += 256u) {
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
            kv_tile[row * kv_pitch + col] = __float2half(key_value);
        }
        __syncthreads();

        // S = Q * K^T: warp w owns score tile (m-chunk w/4, n-chunk w%4).
        {
            unsigned int m_chunk = warp >> 2;
            unsigned int n_chunk = warp & 3u;
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> s_acc;
            wmma::fill_fragment(s_acc, 0.0f);
            for (unsigned int kb = 0u; kb < head_dim; kb += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major> b_frag;
                wmma::load_matrix_sync(a_frag, q_tile + (m_chunk * 16u) * head_dim + kb, head_dim);
                wmma::load_matrix_sync(b_frag, kv_tile + (n_chunk * 16u) * kv_pitch + kb, kv_pitch);
                wmma::mma_sync(s_acc, a_frag, b_frag, s_acc);
            }
            wmma::store_matrix_sync(s_tile + (m_chunk * 16u) * sp_pitch + n_chunk * 16u, s_acc, sp_pitch, wmma::mem_row_major);
        }
        __syncthreads();

        // Online softmax per query row; lanes cover keys {lane, lane+32}.
        for (unsigned int sub = 0u; sub < 4u; ++sub) {
            unsigned int row = warp * 4u + sub;
            unsigned int ja = lane;
            unsigned int jb = lane + 32u;
            unsigned int kia = n0 + ja;
            unsigned int kib = n0 + jb;
            bool valid_a = ja < n_count && tile_phys[ja] != 0xffffffffu && kia >= key_start_r[sub] && kia < key_end_r[sub];
            bool valid_b = jb < n_count && tile_phys[jb] != 0xffffffffu && kib >= key_start_r[sub] && kib < key_end_r[sub];
            float score_a = valid_a ? s_tile[row * sp_pitch + ja] * scale : neg_inf;
            float score_b = valid_b ? s_tile[row * sp_pitch + jb] * scale : neg_inf;

            float tile_max = fmaxf(score_a, score_b);
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
                tile_max = fmaxf(tile_max, __shfl_down_sync(0xffffffffu, tile_max, offset));
            }
            tile_max = __shfl_sync(0xffffffffu, tile_max, 0u);

            float alpha = 1.0f;
            float p_a = 0.0f, p_b = 0.0f;
            if (tile_max > neg_inf) {
                float new_max = fmaxf(m_run[sub], tile_max);
                alpha = m_run[sub] > neg_inf ? expf(m_run[sub] - new_max) : 0.0f;
                p_a = valid_a ? expf(score_a - new_max) : 0.0f;
                p_b = valid_b ? expf(score_b - new_max) : 0.0f;
                m_run[sub] = new_max;
            }
            float sum_p = p_a + p_b;
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1) {
                sum_p += __shfl_down_sync(0xffffffffu, sum_p, offset);
            }
            sum_p = __shfl_sync(0xffffffffu, sum_p, 0u);
            if (tile_max > neg_inf) d_run[sub] = d_run[sub] * alpha + sum_p;

            p_tile[row * sp_pitch + ja] = __float2half(p_a);
            p_tile[row * sp_pitch + jb] = __float2half(p_b);
            if (lane == 0u) alpha_sh[row] = alpha;
        }
        __syncthreads();

        for (unsigned int idx = tid; idx < tile_m * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            o_tile[idx] *= alpha_sh[row];
        }
        for (unsigned int idx = tid; idx < tile_n * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            unsigned int col = idx - row * head_dim;
            unsigned int phys = tile_phys[row];
            float value = 0.0f;
            if (phys != 0xffffffffu) {
                const unsigned char* v_row = v + (size_t)phys * value_row_bytes;
                value = termite_tq_value_at(v_row, kv_head, col, head_dim, value_format);
            }
            kv_tile[row * kv_pitch + col] = __float2half(value);
        }
        __syncthreads();

        // O += P * V: 2*(head_dim/16) output tiles round-robin over 8 warps.
        for (unsigned int wt = warp; wt < 2u * (head_dim >> 4); wt += 8u) {
            unsigned int m_chunk = wt / (head_dim >> 4);
            unsigned int n_chunk = wt - m_chunk * (head_dim >> 4);
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> o_acc;
            wmma::load_matrix_sync(o_acc, o_tile + (m_chunk * 16u) * head_dim + n_chunk * 16u, head_dim, wmma::mem_row_major);
            for (unsigned int kb = 0u; kb < 4u; ++kb) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
                wmma::load_matrix_sync(p_frag, p_tile + (m_chunk * 16u) * sp_pitch + kb * 16u, sp_pitch);
                wmma::load_matrix_sync(v_frag, kv_tile + (kb * 16u) * kv_pitch + n_chunk * 16u, kv_pitch);
                wmma::mma_sync(o_acc, p_frag, v_frag, o_acc);
            }
            wmma::store_matrix_sync(o_tile + (m_chunk * 16u) * head_dim + n_chunk * 16u, o_acc, head_dim, wmma::mem_row_major);
        }
        __syncthreads();
    }

    for (unsigned int r = 0u; r < 4u; ++r) {
        if (!row_valid[r]) continue;
        unsigned int out_base = (qi_base + r) * q_hidden + head * head_dim;
        float inv_denom = d_run[r] > 0.0f ? 1.0f / d_run[r] : 0.0f;
        for (unsigned int col = lane; col < head_dim; col += 32u) {
            dst[out_base + col] = o_tile[(warp * 4u + r) * head_dim + col] * inv_denom;
        }
    }
}

extern "C" __global__ void termite_gqa_attention_decode_turboquant_split_stage1_f32(
    float* partial_acc,
    float* partial_meta,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned int* block_table,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int query_position_offset,
    unsigned int kv_position_offset,
    unsigned int sliding_window,
    unsigned int key_row_bytes,
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int format,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars,
    unsigned int chunk_size,
    unsigned int chunk_count
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }

    const float neg_inf = -3.402823466e+38f;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    unsigned int lane = threadIdx.x;
    unsigned int head = blockIdx.x;
    unsigned int chunk = blockIdx.y;
    if ((format != 0u && format != 2u) ||
        head >= num_heads ||
        chunk >= chunk_count ||
        head_dim > 512u ||
        (head_dim & 31u) != 0u ||
        blockDim.x < head_dim ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        key_row_bytes == 0u ||
        value_row_bytes == 0u ||
        chunk_size == 0u) return;

    unsigned int chunk_start = chunk * chunk_size;
    if (chunk_start >= kv_seq_len) return;
    unsigned int chunk_end = chunk_start + chunk_size;

    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    unsigned int query_pos = query_position_offset;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }

    if (chunk_start < key_start) chunk_start = key_start;
    if (chunk_end > key_end) chunk_end = key_end;

    unsigned int partial_index = head * chunk_count + chunk;
    float* acc_out = partial_acc + partial_index * head_dim;
    float* meta_out = partial_meta + partial_index * 2u;
    if (chunk_start >= chunk_end) {
        if (lane < head_dim) acc_out[lane] = 0.0f;
        if (lane == 0u) {
            meta_out[0] = neg_inf;
            meta_out[1] = 0.0f;
        }
        return;
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_base = head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = neg_inf;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = chunk_start; ki < chunk_end; ++ki) {
        unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        bool valid = physical_token != 0xffffffffu;
        const unsigned char* k_row = valid ? k + physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int value_index = kv_head * head_dim + lane;
            float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            if (valid) {
                float score = dot * scale;
                float next_max = fmaxf(shared_max_score, score);
                shared_alpha = expf(shared_max_score - next_max);
                shared_beta = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha + shared_beta;
                shared_max_score = next_max;
            } else {
                shared_alpha = 1.0f;
                shared_beta = 0.0f;
            }
        }
        __syncthreads();
        if (lane < head_dim) {
            acc *= shared_alpha;
            if (valid) {
                const unsigned char* v_row = v + physical_token * value_row_bytes;
                float value = termite_tq_value_at(v_row, kv_head, lane, head_dim, value_format);
                acc += shared_beta * value;
            }
        }
        __syncthreads();
    }

    if (lane < head_dim) acc_out[lane] = acc;
    if (lane == 0u) {
        meta_out[0] = shared_max_score;
        meta_out[1] = shared_denom;
    }
}

extern "C" __global__ void termite_gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32(
    float* partial_acc,
    float* partial_meta,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned int* block_table,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int query_position_offset,
    unsigned int kv_position_offset,
    unsigned int sliding_window,
    unsigned int key_row_bytes,
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int format,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars,
    unsigned int chunk_size,
    unsigned int chunk_count
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }

    const float neg_inf = -3.402823466e+38f;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    unsigned int lane = threadIdx.x;
    unsigned int head = blockIdx.x;
    unsigned int chunk = blockIdx.y;
    if (head >= num_heads ||
        chunk >= chunk_count ||
        head_dim > 512u ||
        (head_dim & 31u) != 0u ||
        blockDim.x < head_dim ||
        num_kv_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        key_row_bytes == 0u ||
        value_row_bytes == 0u ||
        chunk_size == 0u) return;

    unsigned int chunk_start = chunk * chunk_size;
    if (chunk_start >= kv_seq_len) return;
    unsigned int chunk_end = chunk_start + chunk_size;

    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    unsigned int query_pos = query_position_offset;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }

    if (chunk_start < key_start) chunk_start = key_start;
    if (chunk_end > key_end) chunk_end = key_end;

    unsigned int partial_index = head * chunk_count + chunk;
    float* acc_out = partial_acc + partial_index * head_dim;
    float* meta_out = partial_meta + partial_index * 2u;
    if (chunk_start >= chunk_end) {
        if (lane < head_dim) acc_out[lane] = 0.0f;
        if (lane == 0u) {
            meta_out[0] = neg_inf;
            meta_out[1] = 0.0f;
        }
        return;
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_base = head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = neg_inf;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = chunk_start; ki < chunk_end; ++ki) {
        bool valid = ki < physical_token_capacity;
        const unsigned char* k_row = valid ? k + ki * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int value_index = kv_head * head_dim + lane;
            float key_value = termite_tq_decode_polar4_at(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            if (valid) {
                float score = dot * scale;
                float next_max = fmaxf(shared_max_score, score);
                shared_alpha = expf(shared_max_score - next_max);
                shared_beta = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha + shared_beta;
                shared_max_score = next_max;
            } else {
                shared_alpha = 1.0f;
                shared_beta = 0.0f;
            }
        }
        __syncthreads();
        if (lane < head_dim) {
            acc *= shared_alpha;
            if (valid) {
                const unsigned char* v_row = v + ki * value_row_bytes;
                float value = termite_tq_value_int8_per_head(v_row, kv_head, lane, head_dim);
                acc += shared_beta * value;
            }
        }
        __syncthreads();
    }

    if (lane < head_dim) acc_out[lane] = acc;
    if (lane == 0u) {
        meta_out[0] = shared_max_score;
        meta_out[1] = shared_denom;
    }

    (void)block_table;
    (void)block_count;
    (void)page_size_tokens;
    (void)format;
    (void)value_format;
}

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
    unsigned int physical_token_capacity,
    const unsigned int* decode_scalars
) {
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

__device__ __forceinline__ float termite_q4_0_value(const unsigned char* bp, unsigned int lane) {
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

// Dequantizes Q4_0 blocks (18 bytes: f16 scale + 32 packed nibbles) straight
// to BF16 on device, one thread per block. Bit-identical to the host path
// (dequantizeToFloat32 + round-to-nearest-even): both compute (q-8)*d in f32
// and share the same RNE bias trick. Used to build BF16 weight mirrors at
// load time without host dequant or a second PCIe upload.
extern "C" __global__ void termite_dequant_q4_0_bf16(
    unsigned short* dst,
    const unsigned char* src,
    unsigned int block_count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int stride = gridDim.x * blockDim.x;
    for (unsigned int blk = idx; blk < block_count; blk += stride) {
        const unsigned char* bp = src + (size_t)blk * 18u;
        unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
        float d = termite_half_to_float(h);
        unsigned short* out = dst + (size_t)blk * 32u;
        #pragma unroll
        for (unsigned int i = 0u; i < 16u; ++i) {
            unsigned char packed = bp[2u + i];
            out[i] = termite_f32_to_bf16((float)((int)(packed & 0x0fu) - 8) * d);
            out[i + 16u] = termite_f32_to_bf16((float)((int)(packed >> 4) - 8) * d);
        }
    }
}

__device__ __forceinline__ float termite_q4_0_value_nibble(
    const unsigned char* bp,
    unsigned int q_offset,
    unsigned int high_nibble
) {
    unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    float d = termite_half_to_float(h);
    unsigned char packed = bp[q_offset];
    int q = high_nibble != 0u ? (int)(packed >> 4) : (int)(packed & 0x0fu);
    return (float)(q - 8) * d;
}

__device__ __forceinline__ void termite_store_half_bytes(unsigned char* dst, float value) {
    unsigned short raw = __half_as_ushort(__float2half_rn(value));
    dst[0] = (unsigned char)(raw & 0xffu);
    dst[1] = (unsigned char)(raw >> 8);
}

__device__ __forceinline__ float termite_warp_reduce_max_f32(float v) {
    for (int offset = 16; offset > 0; offset >>= 1) {
        v = fmaxf(v, __shfl_down_sync(0xffffffffu, v, offset));
    }
    return __shfl_sync(0xffffffffu, v, 0);
}

__device__ __forceinline__ int termite_pack_i8x4(const signed char* values) {
    unsigned int packed = (unsigned int)(unsigned char)values[0];
    packed |= ((unsigned int)(unsigned char)values[1]) << 8;
    packed |= ((unsigned int)(unsigned char)values[2]) << 16;
    packed |= ((unsigned int)(unsigned char)values[3]) << 24;
    return (int)packed;
}

__device__ __forceinline__ int termite_load_i8x4_aligned(const signed char* values) {
    return *((const int*)values);
}

__device__ __forceinline__ int termite_pack_q4_0_raw_i8x4(const unsigned char* bp, unsigned int base_lane) {
    unsigned int packed = 0u;
    #pragma unroll
    for (unsigned int j = 0u; j < 4u; ++j) {
        unsigned int lane = base_lane + j;
        unsigned char byte = bp[2u + (lane & 15u)];
        unsigned int q = lane < 16u ? (unsigned int)(byte & 0x0fu) : (unsigned int)(byte >> 4);
        packed |= (q & 0xffu) << (j * 8u);
    }
    return (int)packed;
}

__device__ __forceinline__ int termite_pack_q4_0_centered_i8x4(const unsigned char* bp, unsigned int base_lane) {
    unsigned int packed = 0u;
    #pragma unroll
    for (unsigned int j = 0u; j < 4u; ++j) {
        unsigned int lane = base_lane + j;
        unsigned char byte = bp[2u + (lane & 15u)];
        int q = lane < 16u ? (int)(byte & 0x0fu) : (int)(byte >> 4);
        signed char centered = (signed char)(q - 8);
        packed |= ((unsigned int)(unsigned char)centered) << (j * 8u);
    }
    return (int)packed;
}

__device__ __forceinline__ int termite_pack_q4_0_low_centered_i8x4(const unsigned char* bp, unsigned int base_lane) {
    unsigned int packed = 0u;
    #pragma unroll
    for (unsigned int j = 0u; j < 4u; ++j) {
        unsigned char byte = bp[2u + base_lane + j];
        signed char centered = (signed char)((int)(byte & 0x0fu) - 8);
        packed |= ((unsigned int)(unsigned char)centered) << (j * 8u);
    }
    return (int)packed;
}

__device__ __forceinline__ int termite_pack_q4_0_high_centered_i8x4(const unsigned char* bp, unsigned int base_lane) {
    unsigned int packed = 0u;
    #pragma unroll
    for (unsigned int j = 0u; j < 4u; ++j) {
        unsigned char byte = bp[2u + base_lane + j];
        signed char centered = (signed char)((int)(byte >> 4) - 8);
        packed |= ((unsigned int)(unsigned char)centered) << (j * 8u);
    }
    return (int)packed;
}

struct termite_q4_0_centered_i8x4_pair {
    int low;
    int high;
};

__device__ __forceinline__ termite_q4_0_centered_i8x4_pair termite_pack_q4_0_low_high_centered_i8x4(const unsigned char* bp, unsigned int base_lane) {
    const unsigned char* values = bp + 2u + base_lane;
    unsigned int bytes =
        ((unsigned int)values[0]) |
        ((unsigned int)values[1] << 8) |
        ((unsigned int)values[2] << 16) |
        ((unsigned int)values[3] << 24);
    unsigned int low = __vadd4(bytes & 0x0f0f0f0fu, 0xf8f8f8f8u);
    unsigned int high = __vadd4((bytes >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    termite_q4_0_centered_i8x4_pair packed;
    packed.low = (int)low;
    packed.high = (int)high;
    return packed;
}

__device__ __forceinline__ float termite_q4_0_q8_1_partial_mmvq2(
    const unsigned char* bp,
    float q8_d,
    unsigned int iqs,
    int q8_low0,
    int q8_high0,
    int q8_low1,
    int q8_high1
) {
    unsigned short q4_d_h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    float q4_d = termite_half_to_float(q4_d_h);
    unsigned int base0 = iqs * 4u;
    unsigned int base1 = base0 + 4u;
    termite_q4_0_centered_i8x4_pair packed0 = termite_pack_q4_0_low_high_centered_i8x4(bp, base0);
    termite_q4_0_centered_i8x4_pair packed1 = termite_pack_q4_0_low_high_centered_i8x4(bp, base1);
    int sumi = __dp4a(packed0.low, q8_low0, 0);
    sumi = __dp4a(packed0.high, q8_high0, sumi);
    sumi = __dp4a(packed1.low, q8_low1, sumi);
    sumi = __dp4a(packed1.high, q8_high1, sumi);
    return q4_d * q8_d * (float)sumi;
}

extern "C" __global__ void termite_quantize_f32_q8_1_rows(
    unsigned char* dst,
    const float* input,
    unsigned int rows,
    unsigned int in_dim
) {
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int warps_per_block = blockDim.x >> 5;
    unsigned int row_blocks = in_dim / 32u;
    unsigned int q8_block = blockIdx.x * warps_per_block + warp;
    unsigned int total_blocks = rows * row_blocks;
    if (q8_block >= total_blocks) return;

    unsigned int row = q8_block / row_blocks;
    unsigned int block = q8_block - row * row_blocks;
    float x = input[row * in_dim + block * 32u + lane];
    float amax = termite_warp_reduce_max_f32(fabsf(x));
    float d = amax > 0.0f ? amax / 127.0f : 0.0f;
    int q = 0;
    if (d > 0.0f) {
        q = __float2int_rn(x / d);
        q = max(-127, min(127, q));
    }
    unsigned char* bp = dst + q8_block * 36u;
    bp[4u + lane] = (unsigned char)(signed char)q;
    if (lane == 0u) {
        termite_store_half_bytes(bp, d);
        bp[2u] = 0u;
        bp[3u] = 0u;
    }
}

extern "C" __global__ void termite_quantize_gated_f32_q8_1_rows(
    unsigned char* dst,
    const float* gate,
    const float* up,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int activation
) {
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int warps_per_block = blockDim.x >> 5;
    unsigned int row_blocks = in_dim / 32u;
    unsigned int q8_block = blockIdx.x * warps_per_block + warp;
    unsigned int total_blocks = rows * row_blocks;
    if (q8_block >= total_blocks) return;

    unsigned int row = q8_block / row_blocks;
    unsigned int block = q8_block - row * row_blocks;
    unsigned int index = row * in_dim + block * 32u + lane;
    float x = termite_decoder_activation_f32(gate[index], activation) * up[index];
    float amax = termite_warp_reduce_max_f32(fabsf(x));
    float d = amax > 0.0f ? amax / 127.0f : 0.0f;
    int q = 0;
    if (d > 0.0f) {
        q = __float2int_rn(x / d);
        q = max(-127, min(127, q));
    }
    unsigned char* bp = dst + q8_block * 36u;
    bp[4u + lane] = (unsigned char)(signed char)q;
    if (lane == 0u) {
        termite_store_half_bytes(bp, d);
        bp[2u] = 0u;
        bp[3u] = 0u;
    }
}

template <unsigned int COLS>
__device__ __forceinline__ void termite_store_q4_0_cols_warp_sum(
    float* dst,
    unsigned int row,
    unsigned int out_dim,
    unsigned int col_tile,
    const float* acc,
    float* warp_partial,
    unsigned int tid,
    unsigned int lane,
    unsigned int warp
) {
    #pragma unroll
    for (unsigned int c = 0; c < COLS; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c * 8u + warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 8u; ++w) y += warp_partial[c * 8u + w];
                dst[row * out_dim + col] = y;
            }
        }
    }
}

template <unsigned int COLS, unsigned int WARPS>
__device__ __forceinline__ void termite_store_q4_0_cols_warp_sum_warps(
    float* dst,
    unsigned int row,
    unsigned int out_dim,
    unsigned int col_tile,
    const float* acc,
    float* warp_partial,
    unsigned int tid,
    unsigned int lane,
    unsigned int warp
) {
    #pragma unroll
    for (unsigned int c = 0; c < COLS; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < WARPS) warp_partial[c * WARPS + warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < WARPS; ++w) y += warp_partial[c * WARPS + w];
                dst[row * out_dim + col] = y;
            }
        }
    }
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

template <unsigned int COLS>
__device__ void termite_q4_0_tile_cols(
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[COLS][8];
    float acc[COLS];
    #pragma unroll
    for (unsigned int c = 0; c < COLS; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < COLS; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<COLS>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4_0_tile_cols<4u>(dst, input, weight, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_0_f32_tile4_w4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_2048(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 64u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
            acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 4u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 4u; ++w) y += warp_partial[c][w];
            dst[col_tile + c] = y;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_4096(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 128u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
            acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 4u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 4u; ++w) y += warp_partial[c][w];
            dst[col_tile + c] = y;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8];
    float acc[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_rows2(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    const unsigned int rows_per_block = 2u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row0 = blockIdx.y * rows_per_block;
    if (row0 >= rows) return;
    unsigned int row1 = row0 + 1u;
    bool has_row1 = row1 < rows;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[2][4][8];
    float acc0[4];
    float acc1[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        acc0[c] = 0.0f;
        acc1[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp0 = q8_input + (row0 * row_blocks + block) * 36u;
        unsigned short q8_d0_h = (unsigned short)q8_bp0[0] | ((unsigned short)q8_bp0[1] << 8);
        float q8_d0 = termite_half_to_float(q8_d0_h);
        const signed char* q8_values0 = (const signed char*)(q8_bp0 + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0_0 = termite_load_i8x4_aligned(q8_values0 + q8_base0);
        int q8_high0_0 = termite_load_i8x4_aligned(q8_values0 + q8_base0 + 16u);
        int q8_low1_0 = termite_load_i8x4_aligned(q8_values0 + q8_base1);
        int q8_high1_0 = termite_load_i8x4_aligned(q8_values0 + q8_base1 + 16u);

        float q8_d1 = 0.0f;
        int q8_low0_1 = 0;
        int q8_high0_1 = 0;
        int q8_low1_1 = 0;
        int q8_high1_1 = 0;
        if (has_row1) {
            const unsigned char* q8_bp1 = q8_input + (row1 * row_blocks + block) * 36u;
            unsigned short q8_d1_h = (unsigned short)q8_bp1[0] | ((unsigned short)q8_bp1[1] << 8);
            q8_d1 = termite_half_to_float(q8_d1_h);
            const signed char* q8_values1 = (const signed char*)(q8_bp1 + 4u);
            q8_low0_1 = termite_load_i8x4_aligned(q8_values1 + q8_base0);
            q8_high0_1 = termite_load_i8x4_aligned(q8_values1 + q8_base0 + 16u);
            q8_low1_1 = termite_load_i8x4_aligned(q8_values1 + q8_base1);
            q8_high1_1 = termite_load_i8x4_aligned(q8_values1 + q8_base1 + 16u);
        }

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc0[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d0, iqs, q8_low0_0, q8_high0_0, q8_low1_0, q8_high1_0);
                if (has_row1) acc1[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d1, iqs, q8_low0_1, q8_high0_1, q8_low1_1, q8_high1_1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc0[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d0, iqs, q8_low0_0, q8_high0_0, q8_low1_0, q8_high1_0);
                    if (has_row1) acc1[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d1, iqs, q8_low0_1, q8_high0_1, q8_low1_1, q8_high1_1);
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum0 = termite_warp_reduce_sum(acc0[c]);
        float sum1 = termite_warp_reduce_sum(acc1[c]);
        if (lane == 0u && warp < 8u) {
            warp_partial[0][c][warp] = sum0;
            warp_partial[1][c][warp] = sum1;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y0 = 0.0f;
                float y1 = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 8u; ++w) {
                    y0 += warp_partial[0][c][w];
                    y1 += warp_partial[1][c][w];
                }
                dst[row0 * out_dim + col] = y0;
                if (has_row1) dst[row1 * out_dim + col] = y1;
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_rows4(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    const unsigned int rows_per_block = 4u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row_base = blockIdx.y * rows_per_block;
    if (row_base >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4][8];
    bool full_tile = col_tile + 3u < out_dim;
    float acc[4][4];
    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) acc[r][c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        float q8_d[4];
        int q8_low0[4];
        int q8_high0[4];
        int q8_low1[4];
        int q8_high1[4];
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                q8_d[r] = termite_half_to_float(q8_d_h);
                const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                q8_low0[r] = termite_load_i8x4_aligned(q8_values + q8_base0);
                q8_high0[r] = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                q8_low1[r] = termite_load_i8x4_aligned(q8_values + q8_base1);
                q8_high1[r] = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
            } else {
                q8_d[r] = 0.0f;
                q8_low0[r] = 0;
                q8_high0[r] = 0;
                q8_low1[r] = 0;
                q8_high1[r] = 0;
            }
        }

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    if (row_base + r < rows) {
                        acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                    }
                }
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    #pragma unroll
                    for (unsigned int r = 0u; r < rows_per_block; ++r) {
                        if (row_base + r < rows) {
                            acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                        }
                    }
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            float sum = termite_warp_reduce_sum(acc[r][c]);
            if (lane == 0u && warp < 8u) warp_partial[r][c][warp] = sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                #pragma unroll
                for (unsigned int c = 0u; c < cols; ++c) {
                    unsigned int col = col_tile + c;
                    if (col < out_dim) {
                        float y = 0.0f;
                        #pragma unroll
                        for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[r][c][w];
                        dst[row * out_dim + col] = y;
                    }
                }
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_rows8_c4(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    const unsigned int rows_per_block = 8u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row_base = blockIdx.y * rows_per_block;
    if (row_base >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][4][8];
    bool full_tile = col_tile + 3u < out_dim;
    float acc[8][4];
    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) acc[r][c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        float q8_d[8];
        int q8_low0[8];
        int q8_high0[8];
        int q8_low1[8];
        int q8_high1[8];
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                q8_d[r] = termite_half_to_float(q8_d_h);
                const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                q8_low0[r] = termite_load_i8x4_aligned(q8_values + q8_base0);
                q8_high0[r] = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                q8_low1[r] = termite_load_i8x4_aligned(q8_values + q8_base1);
                q8_high1[r] = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
            } else {
                q8_d[r] = 0.0f;
                q8_low0[r] = 0;
                q8_high0[r] = 0;
                q8_low1[r] = 0;
                q8_high1[r] = 0;
            }
        }

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    if (row_base + r < rows) {
                        acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                    }
                }
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    #pragma unroll
                    for (unsigned int r = 0u; r < rows_per_block; ++r) {
                        if (row_base + r < rows) {
                            acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                        }
                    }
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            float sum = termite_warp_reduce_sum(acc[r][c]);
            if (lane == 0u && warp < 8u) warp_partial[r][c][warp] = sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                #pragma unroll
                for (unsigned int c = 0u; c < cols; ++c) {
                    unsigned int col = col_tile + c;
                    if (col < out_dim) {
                        float y = 0.0f;
                        #pragma unroll
                        for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[r][c][w];
                        dst[row * out_dim + col] = y;
                    }
                }
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 320u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
            acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 8u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 8u; ++w) y += warp_partial[c][w];
            dst[col_tile + c] = y;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 320u;
    const unsigned int out_dim = 2560u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row = blockIdx.y;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
            acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 8u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 8u; ++w) y += warp_partial[c][w];
            dst[row * out_dim + col_tile + c] = y;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w10_e4b_down(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 320u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[4][10];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
            acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 10u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 10u; ++w) y += warp_partial[c][w];
            dst[col_tile + c] = y;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile8(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 8u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][4];
    float acc[8];
    bool full_tile = col_tile + 7u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<8u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_f32_tile8(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_q4_0_tile_cols<8u>(dst, input, weight, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_0_activation_slice_last_dim_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* source,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int source_cols,
    unsigned int source_start,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 8u; ++w) y += warp_partial[c][w];
                dst[row * out_dim + col] =
                    termite_decoder_activation_f32(y, activation) *
                    source[row * source_cols + source_start + col];
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_activation_slice_last_dim_f32_tile4_w4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    const float* source,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int source_cols,
    unsigned int source_start,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 4u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 4u; ++w) y += warp_partial[c][w];
                dst[row * out_dim + col] =
                    termite_decoder_activation_f32(y, activation) *
                    source[row * source_cols + source_start + col];
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const float* source,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int source_cols,
    unsigned int source_start,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 4u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (full_tile || col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 4u; ++w) y += warp_partial[c][w];
                dst[row * out_dim + col] =
                    termite_decoder_activation_f32(y, activation) *
                    source[row * source_cols + source_start + col];
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_gated_down_f32_tile4(
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        unsigned int input_idx = row * in_dim + i;
        float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_gated_down_f32_tile4_w4(
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        unsigned int input_idx = row * in_dim + i;
        float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_gated_down_f32_tile8(
    float* dst,
    const float* gate,
    const float* up,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 8u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][8];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        unsigned int input_idx = row * in_dim + i;
        float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_gated_down_f32_tile16(
    float* dst,
    const float* gate,
    const float* up,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 16u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[16][8];
    float acc[16];
    #pragma unroll
    for (unsigned int c = 0; c < 16u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        unsigned int input_idx = row * in_dim + i;
        float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 16u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<16u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
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

__device__ __forceinline__ float termite_q6k_value(const unsigned char* bp, unsigned int value_index) {
    const unsigned char* ql = bp;
    const unsigned char* qh = bp + 128u;
    const unsigned char* scales = bp + 192u;
    unsigned short dh = (unsigned short)bp[208] | ((unsigned short)bp[209] << 8);
    float d = termite_half_to_float(dh);

    unsigned int sub = value_index / 16u;
    unsigned int i = value_index & 15u;
    unsigned int half = sub / 8u;
    unsigned int group = (sub % 8u) / 2u;
    unsigned int l_base = (sub & 1u) * 16u;
    unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    unsigned int qh_off = half * 32u;
    unsigned int qh_shift = group * 2u;
    unsigned int nibble_shift = (group / 2u) * 4u;
    unsigned int l = l_base + i;

    unsigned int low4 = (unsigned int)((ql[ql_off + l] >> nibble_shift) & 0x0fu);
    unsigned int high2 = (unsigned int)((qh[qh_off + l] >> qh_shift) & 0x03u);
    int q = (int)(low4 | (high2 << 4u)) - 32;
    float scale = d * (float)((signed char)scales[sub]);
    return scale * (float)q;
}

__device__ __forceinline__ int termite_q6k_quant_i32(const unsigned char* bp, unsigned int value_index) {
    const unsigned char* ql = bp;
    const unsigned char* qh = bp + 128u;

    unsigned int sub = value_index / 16u;
    unsigned int i = value_index & 15u;
    unsigned int half = sub / 8u;
    unsigned int group = (sub % 8u) / 2u;
    unsigned int l_base = (sub & 1u) * 16u;
    unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    unsigned int qh_off = half * 32u;
    unsigned int qh_shift = group * 2u;
    unsigned int nibble_shift = (group / 2u) * 4u;
    unsigned int l = l_base + i;

    unsigned int low4 = (unsigned int)((ql[ql_off + l] >> nibble_shift) & 0x0fu);
    unsigned int high2 = (unsigned int)((qh[qh_off + l] >> qh_shift) & 0x03u);
    return (int)(low4 | (high2 << 4u)) - 32;
}

__device__ __forceinline__ float termite_q6k_scale_f32(const unsigned char* bp, unsigned int value_index) {
    const unsigned char* scales = bp + 192u;
    unsigned short dh = (unsigned short)bp[208] | ((unsigned short)bp[209] << 8);
    float d = termite_half_to_float(dh);
    unsigned int sub = value_index / 16u;
    return d * (float)((signed char)scales[sub]);
}

__device__ __forceinline__ float termite_q6k_sub_scale_f32(const unsigned char* bp, unsigned int sub) {
    const unsigned char* scales = bp + 192u;
    unsigned short dh = (unsigned short)bp[208] | ((unsigned short)bp[209] << 8);
    float d = termite_half_to_float(dh);
    return d * (float)((signed char)scales[sub]);
}

__device__ __forceinline__ int termite_pack_q6k_i8x4(const unsigned char* bp, unsigned int base_value_index) {
    signed char values[4];
    #pragma unroll
    for (unsigned int i = 0u; i < 4u; ++i) {
        values[i] = (signed char)termite_q6k_quant_i32(bp, base_value_index + i);
    }
    return termite_pack_i8x4(values);
}

__device__ __forceinline__ int termite_pack_q6k_i8x4_sub(const unsigned char* bp, unsigned int sub, unsigned int sub_offset) {
    signed char values[4];
    unsigned int base_value_index = sub * 16u + sub_offset;
    #pragma unroll
    for (unsigned int i = 0u; i < 4u; ++i) {
        values[i] = (signed char)termite_q6k_quant_i32(bp, base_value_index + i);
    }
    return termite_pack_i8x4(values);
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

extern "C" __global__ void termite_linear_q6_k_f32_tile4(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
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
            float x = input[input_idx];
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                    acc[c] += x * termite_q6k_value(bp, tid);
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

extern "C" __global__ void termite_linear_q6_k_gated_down_f32_tile4(
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
                    const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                    acc[c] += x * termite_q6k_value(bp, tid);
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

extern "C" __global__ void termite_linear_q4_0_pair_nobias_f32_tile4(
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_pair_nobias_f32_tile4_w4(
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile4(
    float* dst_a,
    float* dst_b,
    const unsigned char* q8_input,
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_w8(
    float* dst_a,
    float* dst_b,
    const unsigned char* q8_input,
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8];
    float acc[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile8(
    float* dst_a,
    float* dst_b,
    const unsigned char* q8_input,
    const unsigned char* weight_a,
    const unsigned char* weight_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 8u;
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][4];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<8u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_f32_tile4_w4(
    float* dst,
    const float* input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles;
    if (row >= rows) return;
    unsigned int col_tile = (global - row * tiles) * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float gate_partial[4][4];
    __shared__ float up_partial[4][4];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += x * termite_q4_0_value_nibble(gate_bp, q_offset, high_nibble);
                up_acc[c] += x * termite_q4_0_value_nibble(up_bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 4u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 4u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles;
    if (row >= rows) return;
    unsigned int col_tile = (global - row * tiles) * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float gate_partial[4][4];
    __shared__ float up_partial[4][4];
    float gate_acc[4];
    float up_acc[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                    const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                    gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 4u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (full_tile || col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 4u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles;
    if (row >= rows) return;
    unsigned int col_tile = (global - row * tiles) * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    bool full_tile = col_tile + 3u < out_dim;
    __shared__ float gate_partial[4][8];
    __shared__ float up_partial[4][8];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                    const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                    gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 8u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (full_tile || col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 8u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 80u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float gate_partial[4][8];
    __shared__ float up_partial[4][8];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
            const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
            gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 8u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 8u; ++w) {
                gate_y += gate_partial[c][w];
                up_y += up_partial[c][w];
            }
            dst[col_tile + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 80u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float gate_partial[4][5];
    __shared__ float up_partial[4][5];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
            const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
            gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 5u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0; w < 5u; ++w) {
                gate_y += gate_partial[c][w];
                up_y += up_partial[c][w];
            }
            dst[col_tile + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn(
    unsigned char* dst_q8,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int activation
) {
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;
    const unsigned int row_blocks = 80u;
    const unsigned int q8_block_cols = 32u;
    const unsigned int out_row_blocks = 320u;
    unsigned int global_q8_out_block = blockIdx.x;
    unsigned int row = global_q8_out_block / out_row_blocks;
    unsigned int q8_out_block = global_q8_out_block - row * out_row_blocks;
    unsigned int col_block = q8_out_block * q8_block_cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int group = warp / 5u;
    unsigned int group_warp = warp - group * 5u;
    __shared__ float gate_partial[4][4][5];
    __shared__ float up_partial[4][4][5];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            unsigned int local_tid = group_warp * 32u + lane;
            unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
                const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                float q8_d = termite_half_to_float(q8_d_h);
                const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                unsigned int q8_base0 = iqs * 4u;
                unsigned int q8_base1 = q8_base0 + 4u;
                int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
                int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
                int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    unsigned int col = col_tile + c;
                    const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                    const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                    gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
                float up_sum = termite_warp_reduce_sum(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            unsigned int out_group = tid >> 2u;
            unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < 5u; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        float x = activated[lane];
        float amax = termite_warp_reduce_max_f32(fabsf(x));
        float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char* bp = dst_q8 + global_q8_out_block * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            termite_store_half_bytes(bp, d);
            bp[2u] = 0u;
            bp[3u] = 0u;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2(
    unsigned char* dst_q8,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int activation
) {
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;
    const unsigned int rows_per_block = 2u;
    const unsigned int row_blocks = 80u;
    const unsigned int q8_block_cols = 32u;
    const unsigned int out_row_blocks = 320u;
    unsigned int global_pair_block = blockIdx.x;
    unsigned int row_pair = global_pair_block / out_row_blocks;
    unsigned int row0 = row_pair * rows_per_block;
    if (row0 >= rows) return;
    unsigned int row1 = row0 + 1u;
    bool has_row1 = row1 < rows;
    unsigned int q8_out_block = global_pair_block - row_pair * out_row_blocks;
    unsigned int col_block = q8_out_block * q8_block_cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int group = warp / 5u;
    unsigned int group_warp = warp - group * 5u;
    __shared__ float gate_partial[2][4][4][5];
    __shared__ float up_partial[2][4][4][5];
    __shared__ float activated[2][32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            unsigned int local_tid = group_warp * 32u + lane;
            unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc0[4];
            float up_acc0[4];
            float gate_acc1[4];
            float up_acc1[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc0[c] = 0.0f;
                up_acc0[c] = 0.0f;
                gate_acc1[c] = 0.0f;
                up_acc1[c] = 0.0f;
            }

            unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
                const unsigned char* q8_bp0 = q8_input + (row0 * row_blocks + block) * 36u;
                unsigned short q8_d0_h = (unsigned short)q8_bp0[0] | ((unsigned short)q8_bp0[1] << 8);
                float q8_d0 = termite_half_to_float(q8_d0_h);
                const signed char* q8_values0 = (const signed char*)(q8_bp0 + 4u);
                unsigned int q8_base0 = iqs * 4u;
                unsigned int q8_base1 = q8_base0 + 4u;
                int q8_low0_0 = termite_load_i8x4_aligned(q8_values0 + q8_base0);
                int q8_high0_0 = termite_load_i8x4_aligned(q8_values0 + q8_base0 + 16u);
                int q8_low1_0 = termite_load_i8x4_aligned(q8_values0 + q8_base1);
                int q8_high1_0 = termite_load_i8x4_aligned(q8_values0 + q8_base1 + 16u);

                float q8_d1 = 0.0f;
                int q8_low0_1 = 0;
                int q8_high0_1 = 0;
                int q8_low1_1 = 0;
                int q8_high1_1 = 0;
                if (has_row1) {
                    const unsigned char* q8_bp1 = q8_input + (row1 * row_blocks + block) * 36u;
                    unsigned short q8_d1_h = (unsigned short)q8_bp1[0] | ((unsigned short)q8_bp1[1] << 8);
                    q8_d1 = termite_half_to_float(q8_d1_h);
                    const signed char* q8_values1 = (const signed char*)(q8_bp1 + 4u);
                    q8_low0_1 = termite_load_i8x4_aligned(q8_values1 + q8_base0);
                    q8_high0_1 = termite_load_i8x4_aligned(q8_values1 + q8_base0 + 16u);
                    q8_low1_1 = termite_load_i8x4_aligned(q8_values1 + q8_base1);
                    q8_high1_1 = termite_load_i8x4_aligned(q8_values1 + q8_base1 + 16u);
                }

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    unsigned int col = col_tile + c;
                    const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                    const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                    gate_acc0[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d0, iqs, q8_low0_0, q8_high0_0, q8_low1_0, q8_high1_0);
                    up_acc0[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d0, iqs, q8_low0_0, q8_high0_0, q8_low1_0, q8_high1_0);
                    if (has_row1) {
                        gate_acc1[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d1, iqs, q8_low0_1, q8_high0_1, q8_low1_1, q8_high1_1);
                        up_acc1[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d1, iqs, q8_low0_1, q8_high0_1, q8_low1_1, q8_high1_1);
                    }
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                float gate_sum0 = termite_warp_reduce_sum(gate_acc0[c]);
                float up_sum0 = termite_warp_reduce_sum(up_acc0[c]);
                float gate_sum1 = termite_warp_reduce_sum(gate_acc1[c]);
                float up_sum1 = termite_warp_reduce_sum(up_acc1[c]);
                if (lane == 0u) {
                    gate_partial[0][group][c][group_warp] = gate_sum0;
                    up_partial[0][group][c][group_warp] = up_sum0;
                    gate_partial[1][group][c][group_warp] = gate_sum1;
                    up_partial[1][group][c][group_warp] = up_sum1;
                }
            }
        }
        __syncthreads();
        if (tid < 32u) {
            unsigned int out_row = tid >> 4u;
            unsigned int local = tid & 15u;
            if (out_row == 0u || has_row1) {
                unsigned int out_group = local >> 2u;
                unsigned int c = local & 3u;
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 5u; ++w) {
                    gate_y += gate_partial[out_row][out_group][c][w];
                    up_y += up_partial[out_row][out_group][c][w];
                }
                activated[out_row][wave * 16u + out_group * group_cols + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
        __syncthreads();
    }

    if (warp < rows_per_block && (warp == 0u || has_row1)) {
        unsigned int row = row0 + warp;
        float x = activated[warp][lane];
        float amax = termite_warp_reduce_max_f32(fabsf(x));
        float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char* bp = dst_q8 + (row * out_row_blocks + q8_out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            termite_store_half_bytes(bp, d);
            bp[2u] = 0u;
            bp[3u] = 0u;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4(
    unsigned char* dst_q8,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int activation
) {
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;
    const unsigned int rows_per_block = 4u;
    const unsigned int row_blocks = 80u;
    const unsigned int q8_block_cols = 32u;
    const unsigned int out_row_blocks = 320u;
    unsigned int global_group_block = blockIdx.x;
    unsigned int row_group = global_group_block / out_row_blocks;
    unsigned int row_base = row_group * rows_per_block;
    if (row_base >= rows) return;
    unsigned int q8_out_block = global_group_block - row_group * out_row_blocks;
    unsigned int col_block = q8_out_block * q8_block_cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int group = warp / 5u;
    unsigned int group_warp = warp - group * 5u;
    __shared__ float gate_partial[4][4][4][5];
    __shared__ float up_partial[4][4][4][5];
    __shared__ float activated[4][32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            unsigned int local_tid = group_warp * 32u + lane;
            unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4][4];
            float up_acc[4][4];
            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    gate_acc[r][c] = 0.0f;
                    up_acc[r][c] = 0.0f;
                }
            }

            unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
                float q8_d[4];
                int q8_low0[4];
                int q8_high0[4];
                int q8_low1[4];
                int q8_high1[4];
                unsigned int q8_base0 = iqs * 4u;
                unsigned int q8_base1 = q8_base0 + 4u;
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    unsigned int row = row_base + r;
                    if (row < rows) {
                        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                        q8_d[r] = termite_half_to_float(q8_d_h);
                        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                        q8_low0[r] = termite_load_i8x4_aligned(q8_values + q8_base0);
                        q8_high0[r] = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                        q8_low1[r] = termite_load_i8x4_aligned(q8_values + q8_base1);
                        q8_high1[r] = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
                    } else {
                        q8_d[r] = 0.0f;
                        q8_low0[r] = 0;
                        q8_high0[r] = 0;
                        q8_low1[r] = 0;
                        q8_high1[r] = 0;
                    }
                }

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    unsigned int col = col_tile + c;
                    const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                    const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                    #pragma unroll
                    for (unsigned int r = 0u; r < rows_per_block; ++r) {
                        if (row_base + r < rows) {
                            gate_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                            up_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                        }
                    }
                }
            }

            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    float gate_sum = termite_warp_reduce_sum(gate_acc[r][c]);
                    float up_sum = termite_warp_reduce_sum(up_acc[r][c]);
                    if (lane == 0u) {
                        gate_partial[r][group][c][group_warp] = gate_sum;
                        up_partial[r][group][c][group_warp] = up_sum;
                    }
                }
            }
        }
        __syncthreads();
        if (tid < 64u) {
            unsigned int out_row = tid >> 4u;
            unsigned int local = tid & 15u;
            if (row_base + out_row < rows) {
                unsigned int out_group = local >> 2u;
                unsigned int c = local & 3u;
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 5u; ++w) {
                    gate_y += gate_partial[out_row][out_group][c][w];
                    up_y += up_partial[out_row][out_group][c][w];
                }
                activated[out_row][wave * 16u + out_group * group_cols + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
        __syncthreads();
    }

    if (warp < rows_per_block && row_base + warp < rows) {
        unsigned int row = row_base + warp;
        float x = activated[warp][lane];
        float amax = termite_warp_reduce_max_f32(fabsf(x));
        float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char* bp = dst_q8 + (row * out_row_blocks + q8_out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            termite_store_half_bytes(bp, d);
            bp[2u] = 0u;
            bp[3u] = 0u;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2(
    unsigned char* dst_q8,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int activation
) {
    const unsigned int group_cols = 2u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 4u;
    const unsigned int rows_per_block = 8u;
    const unsigned int row_blocks = 80u;
    const unsigned int q8_block_cols = 32u;
    const unsigned int out_row_blocks = 320u;
    unsigned int global_group_block = blockIdx.x;
    unsigned int row_group = global_group_block / out_row_blocks;
    unsigned int row_base = row_group * rows_per_block;
    if (row_base >= rows) return;
    unsigned int q8_out_block = global_group_block - row_group * out_row_blocks;
    unsigned int col_block = q8_out_block * q8_block_cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int group = warp / 5u;
    unsigned int group_warp = warp - group * 5u;
    __shared__ float gate_partial[8][4][2][5];
    __shared__ float up_partial[8][4][2][5];
    __shared__ float activated[8][32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            unsigned int local_tid = group_warp * 32u + lane;
            unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[8][2];
            float up_acc[8][2];
            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    gate_acc[r][c] = 0.0f;
                    up_acc[r][c] = 0.0f;
                }
            }

            unsigned int iqs = (local_tid & 1u) * 2u;
            unsigned int q8_base0 = iqs * 4u;
            unsigned int q8_base1 = q8_base0 + 4u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    unsigned int row = row_base + r;
                    if (row < rows) {
                        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                        float q8_d = termite_half_to_float(q8_d_h);
                        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
                        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
                        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

                        #pragma unroll
                        for (unsigned int c = 0u; c < group_cols; ++c) {
                            unsigned int col = col_tile + c;
                            const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                            const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                            gate_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                            up_acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                        }
                    }
                }
            }

            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    float gate_sum = termite_warp_reduce_sum(gate_acc[r][c]);
                    float up_sum = termite_warp_reduce_sum(up_acc[r][c]);
                    if (lane == 0u) {
                        gate_partial[r][group][c][group_warp] = gate_sum;
                        up_partial[r][group][c][group_warp] = up_sum;
                    }
                }
            }
        }
        __syncthreads();
        if (tid < 64u) {
            unsigned int out_row = tid >> 3u;
            unsigned int local = tid & 7u;
            if (row_base + out_row < rows) {
                unsigned int out_group = local >> 1u;
                unsigned int c = local & 1u;
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 5u; ++w) {
                    gate_y += gate_partial[out_row][out_group][c][w];
                    up_y += up_partial[out_row][out_group][c][w];
                }
                activated[out_row][wave * 8u + out_group * group_cols + c] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
        __syncthreads();
    }

    if (warp < rows_per_block && row_base + warp < rows) {
        unsigned int row = row_base + warp;
        float x = activated[warp][lane];
        float amax = termite_warp_reduce_max_f32(fabsf(x));
        float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char* bp = dst_q8 + (row * out_row_blocks + q8_out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            termite_store_half_bytes(bp, d);
            bp[2u] = 0u;
            bp[3u] = 0u;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1(
    unsigned char* dst_q8,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int activation
) {
    const unsigned int groups_per_wave = 4u;
    const unsigned int warps_per_group = 4u;
    const unsigned int waves = 8u;
    const unsigned int rows_per_block = 16u;
    const unsigned int row_blocks = 80u;
    const unsigned int q8_block_cols = 32u;
    const unsigned int out_row_blocks = 320u;
    unsigned int global_group_block = blockIdx.x;
    unsigned int row_group = global_group_block / out_row_blocks;
    unsigned int row_base = row_group * rows_per_block;
    if (row_base >= rows) return;
    unsigned int q8_out_block = global_group_block - row_group * out_row_blocks;
    unsigned int col_block = q8_out_block * q8_block_cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int group = warp / warps_per_group;
    unsigned int group_warp = warp - group * warps_per_group;
    __shared__ float gate_partial[16][4][4];
    __shared__ float up_partial[16][4][4];
    __shared__ float activated[16][32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            unsigned int local_tid = group_warp * 32u + lane;
            unsigned int col = col_block + wave * groups_per_wave + group;
            float gate_acc[16];
            float up_acc[16];
            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                gate_acc[r] = 0.0f;
                up_acc[r] = 0.0f;
            }

            unsigned int iqs = (local_tid & 1u) * 2u;
            unsigned int q8_base0 = iqs * 4u;
            unsigned int q8_base1 = q8_base0 + 4u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 64u) {
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                unsigned short gate_d_h = (unsigned short)gate_bp[0] | ((unsigned short)gate_bp[1] << 8);
                unsigned short up_d_h = (unsigned short)up_bp[0] | ((unsigned short)up_bp[1] << 8);
                float gate_d = termite_half_to_float(gate_d_h);
                float up_d = termite_half_to_float(up_d_h);
                termite_q4_0_centered_i8x4_pair gate_packed0 = termite_pack_q4_0_low_high_centered_i8x4(gate_bp, q8_base0);
                termite_q4_0_centered_i8x4_pair gate_packed1 = termite_pack_q4_0_low_high_centered_i8x4(gate_bp, q8_base1);
                termite_q4_0_centered_i8x4_pair up_packed0 = termite_pack_q4_0_low_high_centered_i8x4(up_bp, q8_base0);
                termite_q4_0_centered_i8x4_pair up_packed1 = termite_pack_q4_0_low_high_centered_i8x4(up_bp, q8_base1);

                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    unsigned int row = row_base + r;
                    if (row < rows) {
                        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                        float q8_d = termite_half_to_float(q8_d_h);
                        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
                        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
                        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
                        int gate_sumi = __dp4a(gate_packed0.low, q8_low0, 0);
                        gate_sumi = __dp4a(gate_packed0.high, q8_high0, gate_sumi);
                        gate_sumi = __dp4a(gate_packed1.low, q8_low1, gate_sumi);
                        gate_sumi = __dp4a(gate_packed1.high, q8_high1, gate_sumi);
                        int up_sumi = __dp4a(up_packed0.low, q8_low0, 0);
                        up_sumi = __dp4a(up_packed0.high, q8_high0, up_sumi);
                        up_sumi = __dp4a(up_packed1.low, q8_low1, up_sumi);
                        up_sumi = __dp4a(up_packed1.high, q8_high1, up_sumi);
                        gate_acc[r] += gate_d * q8_d * (float)gate_sumi;
                        up_acc[r] += up_d * q8_d * (float)up_sumi;
                    }
                }
            }

            #pragma unroll
            for (unsigned int r = 0u; r < rows_per_block; ++r) {
                float gate_sum = termite_warp_reduce_sum(gate_acc[r]);
                float up_sum = termite_warp_reduce_sum(up_acc[r]);
                if (lane == 0u) {
                    gate_partial[r][group][group_warp] = gate_sum;
                    up_partial[r][group][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 64u) {
            unsigned int out_row = tid >> 2u;
            unsigned int out_group = tid & 3u;
            if (row_base + out_row < rows) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < warps_per_group; ++w) {
                    gate_y += gate_partial[out_row][out_group][w];
                    up_y += up_partial[out_row][out_group][w];
                }
                activated[out_row][wave * 4u + out_group] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
        __syncthreads();
    }

    if (warp < rows_per_block && row_base + warp < rows) {
        unsigned int row = row_base + warp;
        float x = activated[warp][lane];
        float amax = termite_warp_reduce_max_f32(fabsf(x));
        float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char* bp = dst_q8 + (row * out_row_blocks + q8_out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            termite_store_half_bytes(bp, d);
            bp[2u] = 0u;
            bp[3u] = 0u;
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile8(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    const unsigned int cols = 8u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles;
    if (row >= rows) return;
    unsigned int col_tile = (global - row * tiles) * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float gate_partial[8][4];
    __shared__ float up_partial[8][4];
    float gate_acc[8];
    float up_acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < 8u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 4u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 4u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_pair_nobias_f32_tile8(
    float* dst_a,
    float* dst_b,
    const float* input,
    const unsigned char* weight_a,
    const unsigned char* weight_b,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 8u;
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][8];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
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
            unsigned int q_offset = 2u + (lane & 15u);
            unsigned int high_nibble = lane >> 4u;
            #pragma unroll
            for (unsigned int c = 0; c < 4u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
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

extern "C" __global__ void termite_linear_q4_0_argmax_rows_stage1_tile16(
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
    const unsigned int cols = 16u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[16][256];
    float acc[16];
    #pragma unroll
    for (unsigned int c = 0; c < 16u; ++c) acc[c] = 0.0f;

    if (row < rows) {
        for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
            float x = input[row * in_dim + i];
            unsigned int block = i / 32u;
            unsigned int lane = i - block * 32u;
            unsigned int q_offset = 2u + (lane & 15u);
            unsigned int high_nibble = lane >> 4u;
            #pragma unroll
            for (unsigned int c = 0; c < 16u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 16u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 16u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 16u; ++c) {
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

extern "C" __global__ void termite_linear_q6_k_argmax_rows_stage1_tile4(
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
                    const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                    acc[c] += x * termite_q6k_value(bp, tid);
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

extern "C" __global__ void termite_linear_q6_k_argmax_rows_stage1_tile8(
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
    const unsigned int cols = 8u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[8][256];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    if (row < rows) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x = input[row * in_dim + block * 256u + tid];
            #pragma unroll
            for (unsigned int c = 0; c < 8u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                    acc[c] += x * termite_q6k_value(bp, tid);
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 8u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
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

extern "C" __global__ void termite_linear_q6_k_argmax_rows_stage1_tile16(
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
    const unsigned int cols = 16u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 256u;
    __shared__ float partial[16][256];
    float acc[16];
    #pragma unroll
    for (unsigned int c = 0; c < 16u; ++c) acc[c] = 0.0f;

    if (row < rows) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            float x = input[row * in_dim + block * 256u + tid];
            #pragma unroll
            for (unsigned int c = 0; c < 16u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                    acc[c] += x * termite_q6k_value(bp, tid);
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 16u; ++c) partial[c][tid] = acc[c];
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            #pragma unroll
            for (unsigned int c = 0; c < 16u; ++c) partial[c][tid] += partial[c][tid + stride];
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 16u; ++c) {
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

extern "C" __global__ void termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* input_q8_1,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 8u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    bool full_tile = col_tile + 7u < out_dim;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int q8_row_blocks = in_dim / 32u;
    unsigned int task_count = row_blocks * 16u;
    __shared__ float warp_partial[8][8];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    if (row < rows && tid < task_count) {
        unsigned int block = tid / 16u;
        unsigned int sub = tid - block * 16u;
        unsigned int q8_sub_block = sub >> 1u;
        unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = input_q8_1 + (row * q8_row_blocks + block * 8u + q8_sub_block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        int q8_pack0 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 0u);
        int q8_pack1 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 4u);
        int q8_pack2 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 8u);
        int q8_pack3 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 12u);
        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0; c < 8u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                int sumi = 0;
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 0u), q8_pack0, sumi);
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 4u), q8_pack1, sumi);
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 8u), q8_pack2, sumi);
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 12u), q8_pack3, sumi);
                acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0; c < 8u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                    int sumi = 0;
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 0u), q8_pack0, sumi);
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 4u), q8_pack1, sumi);
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 8u), q8_pack2, sumi);
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 12u), q8_pack3, sumi);
                    acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        unsigned int warp_count = blockDim.x >> 5;
        if (row < rows) {
            if (full_tile) {
                #pragma unroll
                for (unsigned int c = 0; c < 8u; ++c) {
                    unsigned int col = col_tile + c;
                    float value = 0.0f;
                    #pragma unroll
                    for (unsigned int w = 0u; w < 8u; ++w) {
                        if (w >= warp_count) continue;
                        value += warp_partial[c][w];
                    }
                    bool suppressed = false;
                    if (suppress_count != 0u) {
                        for (unsigned int j = 0; j < suppress_count; ++j) {
                            int token_id = suppress_token_ids[j];
                            if (token_id >= 0 && (unsigned int)token_id == col) {
                                suppressed = true;
                                break;
                            }
                        }
                    }
                    if (suppressed) continue;
                    if (value > best_value || (value == best_value && col < best_index)) {
                        best_value = value;
                        best_index = col;
                    }
                }
            } else {
                #pragma unroll
                for (unsigned int c = 0; c < 8u; ++c) {
                    unsigned int col = col_tile + c;
                    if (col >= out_dim) continue;
                    float value = 0.0f;
                    #pragma unroll
                    for (unsigned int w = 0u; w < 8u; ++w) {
                        if (w >= warp_count) continue;
                        value += warp_partial[c][w];
                    }
                    bool suppressed = false;
                    if (suppress_count != 0u) {
                        for (unsigned int j = 0; j < suppress_count; ++j) {
                            int token_id = suppress_token_ids[j];
                            if (token_id >= 0 && (unsigned int)token_id == col) {
                                suppressed = true;
                                break;
                            }
                        }
                    }
                    if (suppressed) continue;
                    if (value > best_value || (value == best_value && col < best_index)) {
                        best_value = value;
                        best_index = col;
                    }
                }
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}

extern "C" __global__ void termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* input_q8_1,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    (void)rows;
    (void)in_dim;
    (void)out_dim;
    (void)suppress_count;
    const unsigned int row_blocks = 10u;
    const unsigned int cols = 8u;
    unsigned int global_tile = blockIdx.x;
    unsigned int col_tile = global_tile * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[8][5];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    if (tid < 160u) {
        unsigned int block = tid >> 4u;
        unsigned int sub = tid & 15u;
        unsigned int q8_sub_block = sub >> 1u;
        unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = input_q8_1 + (block * 8u + q8_sub_block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        int q8_pack0 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 0u);
        int q8_pack1 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 4u);
        int q8_pack2 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 8u);
        int q8_pack3 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 12u);
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;
            int sumi = 0;
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 0u), q8_pack0, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 4u), q8_pack1, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 8u), q8_pack2, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 12u), q8_pack3, sumi);
            acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            float value =
                warp_partial[c][0] +
                warp_partial[c][1] +
                warp_partial[c][2] +
                warp_partial[c][3] +
                warp_partial[c][4];
            unsigned int col = col_tile + c;
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}

extern "C" __global__ void termite_linear_q6_k_q8_1_f32_tile8_e4b(
    float* dst,
    const unsigned char* input_q8_1,
    const unsigned char* weight,
    unsigned int out_dim
) {
    const unsigned int row_blocks = 10u;
    const unsigned int cols = 8u;
    unsigned int global_tile = blockIdx.x;
    unsigned int col_tile = global_tile * cols;
    if (col_tile >= out_dim) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[8][5];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    if (tid < 160u) {
        unsigned int block = tid >> 4u;
        unsigned int sub = tid & 15u;
        unsigned int q8_sub_block = sub >> 1u;
        unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = input_q8_1 + (block * 8u + q8_sub_block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        int q8_pack0 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 0u);
        int q8_pack1 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 4u);
        int q8_pack2 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 8u);
        int q8_pack3 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 12u);
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;
            int sumi = 0;
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 0u), q8_pack0, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 4u), q8_pack1, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 8u), q8_pack2, sumi);
            sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 12u), q8_pack3, sumi);
            acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < 8u) {
        unsigned int col = col_tile + tid;
        if (col < out_dim) {
            dst[col] =
                warp_partial[tid][0] +
                warp_partial[tid][1] +
                warp_partial[tid][2] +
                warp_partial[tid][3] +
                warp_partial[tid][4];
        }
    }
}

extern "C" __global__ void termite_argmax_reduce_rows_pairs_f32(
    unsigned int* dst,
    const float* partial_values,
    const unsigned int* partial_indices,
    unsigned int rows,
    unsigned int col_tiles
) {
    __shared__ float warp_best_values[8];
    __shared__ unsigned int warp_best_indices[8];
    unsigned int row = blockIdx.x;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
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
    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        float other_value = __shfl_down_sync(0xffffffffu, best_value, offset);
        unsigned int other_index = __shfl_down_sync(0xffffffffu, best_index, offset);
        if (other_index != 0xffffffffu &&
            (other_value > best_value || (other_value == best_value && other_index < best_index))) {
            best_value = other_value;
            best_index = other_index;
        }
    }
    if (lane == 0u) {
        warp_best_values[warp] = best_value;
        warp_best_indices[warp] = best_index;
    }
    __syncthreads();
    if (tid == 0u && row < rows) {
        float final_value = -3.402823466e+38f;
        unsigned int final_index = 0xffffffffu;
        unsigned int warp_count = blockDim.x >> 5;
        #pragma unroll
        for (unsigned int w = 0u; w < 8u; ++w) {
            if (w >= warp_count) continue;
            float other_value = warp_best_values[w];
            unsigned int other_index = warp_best_indices[w];
            if (other_index != 0xffffffffu &&
                (other_value > final_value || (other_value == final_value && other_index < final_index))) {
                final_value = other_value;
                final_index = other_index;
            }
        }
        dst[row] = final_index == 0xffffffffu ? 0u : final_index;
    }
}

extern "C" __global__ void termite_argmax_reduce_rows_pairs_f32_w16(
    unsigned int* dst,
    const float* partial_values,
    const unsigned int* partial_indices,
    unsigned int rows,
    unsigned int col_tiles
) {
    __shared__ float warp_best_values[16];
    __shared__ unsigned int warp_best_indices[16];
    unsigned int row = blockIdx.x;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
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
    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        float other_value = __shfl_down_sync(0xffffffffu, best_value, offset);
        unsigned int other_index = __shfl_down_sync(0xffffffffu, best_index, offset);
        if (other_index != 0xffffffffu &&
            (other_value > best_value || (other_value == best_value && other_index < best_index))) {
            best_value = other_value;
            best_index = other_index;
        }
    }
    if (lane == 0u) {
        warp_best_values[warp] = best_value;
        warp_best_indices[warp] = best_index;
    }
    __syncthreads();
    if (tid == 0u && row < rows) {
        float final_value = -3.402823466e+38f;
        unsigned int final_index = 0xffffffffu;
        unsigned int warp_count = blockDim.x >> 5;
        #pragma unroll
        for (unsigned int w = 0u; w < 16u; ++w) {
            if (w >= warp_count) continue;
            float other_value = warp_best_values[w];
            unsigned int other_index = warp_best_indices[w];
            if (other_index != 0xffffffffu &&
                (other_value > final_value || (other_value == final_value && other_index < final_index))) {
                final_value = other_value;
                final_index = other_index;
            }
        }
        dst[row] = final_index == 0xffffffffu ? 0u : final_index;
    }
}

extern "C" __global__ void termite_argmax_reduce_pairs_f32(
    unsigned int* dst,
    const float* partial_values,
    const unsigned int* partial_indices,
    unsigned int count
) {
    __shared__ float warp_best_values[8];
    __shared__ unsigned int warp_best_indices[8];
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
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
    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        float other_value = __shfl_down_sync(0xffffffffu, best_value, offset);
        unsigned int other_index = __shfl_down_sync(0xffffffffu, best_index, offset);
        if (other_index != 0xffffffffu &&
            (other_value > best_value || (other_value == best_value && other_index < best_index))) {
            best_value = other_value;
            best_index = other_index;
        }
    }
    if (lane == 0u) {
        warp_best_values[warp] = best_value;
        warp_best_indices[warp] = best_index;
    }
    __syncthreads();
    if (tid == 0u) {
        float final_value = -3.402823466e+38f;
        unsigned int final_index = 0xffffffffu;
        unsigned int warp_count = blockDim.x >> 5;
        #pragma unroll
        for (unsigned int w = 0u; w < 8u; ++w) {
            if (w >= warp_count) continue;
            float other_value = warp_best_values[w];
            unsigned int other_index = warp_best_indices[w];
            if (other_index != 0xffffffffu &&
                (other_value > final_value || (other_value == final_value && other_index < final_index))) {
                final_value = other_value;
                final_index = other_index;
            }
        }
        dst[0] = final_index == 0xffffffffu ? 0u : final_index;
    }
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

extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile4(
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile4_w4(
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile4(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const unsigned char* q8_input,
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<4u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const unsigned char* q8_input,
    const unsigned char* weight_q,
    const unsigned char* weight_k,
    const unsigned char* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    const unsigned int cols = 8u;
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][4];
    float acc[8];
    bool full_tile = col_tile + 7u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<8u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const unsigned char* q8_input,
    const unsigned char* weight_q,
    const unsigned char* weight_k,
    const unsigned char* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    const unsigned int cols = 8u;
    const unsigned int rows_per_block = 4u;
    unsigned int q_tiles = (q_out_dim + cols - 1u) / cols;
    unsigned int kv_tiles = (kv_out_dim + cols - 1u) / cols;
    unsigned int tiles_per_row_group = q_tiles + kv_tiles * 2u;
    unsigned int global = blockIdx.x;
    unsigned int row_group = global / tiles_per_row_group;
    unsigned int row_base = row_group * rows_per_block;
    if (row_base >= rows) return;
    unsigned int tile = global - row_group * tiles_per_row_group;

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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8][4];
    float acc[4][8];
    bool full_tile = col_tile + 7u < out_dim;
    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) acc[r][c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        float q8_d[4];
        int q8_low0[4];
        int q8_high0[4];
        int q8_low1[4];
        int q8_high1[4];
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                q8_d[r] = termite_half_to_float(q8_d_h);
                const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                q8_low0[r] = termite_load_i8x4_aligned(q8_values + q8_base0);
                q8_high0[r] = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                q8_low1[r] = termite_load_i8x4_aligned(q8_values + q8_base1);
                q8_high1[r] = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
            } else {
                q8_d[r] = 0.0f;
                q8_low0[r] = 0;
                q8_high0[r] = 0;
                q8_low1[r] = 0;
                q8_high1[r] = 0;
            }
        }

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    if (row_base + r < rows) {
                        acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                    }
                }
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    #pragma unroll
                    for (unsigned int r = 0u; r < rows_per_block; ++r) {
                        if (row_base + r < rows) {
                            acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                        }
                    }
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            float sum = termite_warp_reduce_sum(acc[r][c]);
            if (lane == 0u && warp < 4u) warp_partial[r][c][warp] = sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                #pragma unroll
                for (unsigned int c = 0u; c < cols; ++c) {
                    unsigned int col = col_tile + c;
                    if (col < out_dim) {
                        float y = 0.0f;
                        #pragma unroll
                        for (unsigned int w = 0u; w < 4u; ++w) y += warp_partial[r][c][w];
                        dst[row * out_dim + col] = y;
                    }
                }
            }
        }
    }
}

extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8(
    float* dst_q,
    float* dst_k,
    float* dst_v,
    const unsigned char* q8_input,
    const unsigned char* weight_q,
    const unsigned char* weight_k,
    const unsigned char* weight_v,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int q_out_dim,
    unsigned int kv_out_dim
) {
    const unsigned int cols = 8u;
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][8];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0 = termite_load_i8x4_aligned(q8_values + q8_base0);
        int q8_high0 = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
        int q8_low1 = termite_load_i8x4_aligned(q8_values + q8_base1);
        int q8_high1 = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<8u, 8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}

extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile8(
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
    const unsigned int cols = 8u;
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
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][8];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum<8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
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
    unsigned int dim,
    float scale
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
    dst[idx] = termite_q4k_value(bp, value_index) * scale;
}

extern "C" __global__ void termite_embedding_lookup_q6_k_f32(
    float* dst,
    const unsigned char* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim,
    float scale
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
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 210ull;
    dst[idx] = termite_q6k_value(bp, value_index) * scale;
}

extern "C" __global__ void termite_embedding_lookup_q8_0_f32(
    float* dst,
    const unsigned char* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim,
    float scale
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
    dst[idx] = termite_q8_0_value(bp, lane) * scale;
}

extern "C" __global__ void termite_embedding_lookup_q4_0_f32(
    float* dst,
    const unsigned char* weight,
    const long long* ids,
    unsigned int total,
    unsigned int dim,
    float scale
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
    unsigned int q_offset = 2u + (lane & 15u);
    unsigned int high_nibble = lane >> 4u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 18ull;
    dst[idx] = termite_q4_0_value_nibble(bp, q_offset, high_nibble) * scale;
}

extern "C" __global__ void termite_embedding_lookup_i32_q4_k_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim,
    float scale
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
    dst[idx] = termite_q4k_value(bp, value_index) * scale;
}

extern "C" __global__ void termite_embedding_lookup_i32_q6_k_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim,
    float scale
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
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 210ull;
    dst[idx] = termite_q6k_value(bp, value_index) * scale;
}

extern "C" __global__ void termite_embedding_add_weighted_i32_q6_k_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    const float* rhs,
    unsigned int total,
    unsigned int dim,
    float lhs_scale,
    float rhs_scale
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
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 210ull;
    dst[idx] = termite_q6k_value(bp, value_index) * lhs_scale + rhs[idx] * rhs_scale;
}

extern "C" __global__ void termite_rms_norm_add_weighted_embedding_i32_q6_k_f32(
    float* dst,
    const float* input,
    const float* norm_weight,
    const unsigned char* embedding_weight,
    const int* ids,
    unsigned int total,
    unsigned int num_groups,
    unsigned int group_dim,
    unsigned int embedding_dim,
    float eps,
    float lhs_scale,
    float rhs_scale
) {
    unsigned int group_row = blockIdx.x;
    unsigned int rows = total * num_groups;
    if (group_row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int token_row = group_row / num_groups;
    unsigned int group = group_row - token_row * num_groups;
    unsigned int base = token_row * embedding_dim + group * group_dim;
    __shared__ float partial[256];
    float sumsq = 0.0f;
    for (unsigned int i = tid; i < group_dim; i += blockDim.x) {
        float x = input[base + i];
        sumsq += x * x;
    }
    partial[tid] = sumsq;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    float norm_scale = rsqrtf(partial[0] / (float)group_dim + eps);
    unsigned long long src_row = (unsigned long long)((unsigned int)ids[token_row]);
    unsigned int row_blocks = embedding_dim / 256u;
    for (unsigned int i = tid; i < group_dim; i += blockDim.x) {
        unsigned int col = group * group_dim + i;
        unsigned int block = col / 256u;
        unsigned int value_index = col - block * 256u;
        const unsigned char* bp = embedding_weight + (src_row * row_blocks + block) * 210ull;
        unsigned int idx = base + i;
        float token_value = termite_q6k_value(bp, value_index);
        float normed_value = input[idx] * norm_scale * norm_weight[i];
        dst[idx] = token_value * lhs_scale + normed_value * rhs_scale;
    }
}

extern "C" __global__ void termite_embedding_lookup_i32_q8_0_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim,
    float scale
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
    dst[idx] = termite_q8_0_value(bp, lane) * scale;
}

extern "C" __global__ void termite_embedding_lookup_i32_q4_0_f32(
    float* dst,
    const unsigned char* weight,
    const int* ids,
    unsigned int total,
    unsigned int dim,
    float scale
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
    unsigned int q_offset = 2u + (lane & 15u);
    unsigned int high_nibble = lane >> 4u;
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 18ull;
    dst[idx] = termite_q4_0_value_nibble(bp, q_offset, high_nibble) * scale;
}

__device__ __forceinline__ bool termite_deberta_attention_key_valid(
    const long long* mask,
    const float* attn_bias,
    unsigned int use_bias,
    unsigned int b,
    unsigned int head,
    unsigned int qi,
    unsigned int ki,
    unsigned int seq_len,
    unsigned int num_heads
) {
    if (use_bias != 0u) {
        unsigned int bias_idx = ((b * num_heads + head) * seq_len + qi) * seq_len + ki;
        return attn_bias[bias_idx] >= -1.0e8f;
    }
    return mask[b * seq_len + ki] != 0ll;
}

__device__ __forceinline__ float termite_deberta_attention_score_f32(
    const float* q,
    const float* k,
    const float* q_r,
    const float* k_r,
    unsigned int b,
    unsigned int qi,
    unsigned int ki,
    unsigned int head,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    unsigned int hidden = num_heads * head_dim;
    unsigned int head_off = head * head_dim;
    unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
    unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
    unsigned int rel_base = (qi + seq_len - 1u - ki) * hidden + head_off;
    float score = 0.0f;
    for (unsigned int d = 0; d < head_dim; ++d) {
        float qd = q[q_base + d];
        float kd = k[k_base + d];
        score += qd * kd + qd * k_r[rel_base + d] + q_r[rel_base + d] * kd;
    }
    return score * rsqrtf((float)head_dim * 3.0f);
}

// One block owns one (batch, head, query) row. Scores are computed once per
// key and shared by every output channel; the legacy kernel recomputed all
// three DeBERTa dot products twice for every channel.
extern "C" __global__ void termite_deberta_attention_f32(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const float* q_r,
    const float* k_r,
    const long long* mask,
    const float* attn_bias,
    unsigned int use_bias,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
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

// Training kernels intentionally use FP32 state.  They share the inference
// stream, so graph outputs, accumulated gradients, clipping and AdamW updates
// remain ordered without host synchronization between operations.
extern "C" __global__ void termite_training_accumulate_f32(
    float* accum,
    const float* grad,
    unsigned int count,
    float scale,
    unsigned int first
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float value = grad[i] * scale;
    accum[i] = first ? value : accum[i] + value;
}

extern "C" __global__ void termite_training_adamw_f32(
    float* weight,
    const float* grad,
    float* m,
    float* v,
    unsigned int count,
    float lr,
    float beta1,
    float beta2,
    float eps,
    float weight_decay,
    float bias_correction1,
    float bias_correction2,
    float grad_scale
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    float g = grad[i] * grad_scale;
    float next_m = beta1 * m[i] + (1.0f - beta1) * g;
    float next_v = beta2 * v[i] + (1.0f - beta2) * g * g;
    m[i] = next_m;
    v[i] = next_v;
    float m_hat = next_m / bias_correction1;
    float v_hat = next_v / bias_correction2;
    float w = weight[i];
    weight[i] = w - lr * (m_hat / (sqrtf(v_hat) + eps) + weight_decay * w);
}

extern "C" __global__ void termite_training_sum_squares_f32(
    float* output,
    const float* input,
    unsigned int count
) {
    __shared__ float partial[256];
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    float value = 0.0f;
    if (i < count) {
        float x = input[i];
        value = x * x;
    }
    partial[threadIdx.x] = value;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (threadIdx.x < stride) partial[threadIdx.x] += partial[threadIdx.x + stride];
        __syncthreads();
    }
    if (threadIdx.x == 0u) atomicAdd(output, partial[0]);
}

extern "C" __global__ void termite_masked_bce_accumulate_f32(
    float* accum,
    const float* logits,
    const float* labels,
    const float* mask,
    unsigned int count,
    float positive_weight,
    float negative_weight
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float m = mask[idx];
    if (m == 0.0f) return;
    float logit = logits[idx];
    float label = labels[idx];
    float label_weight = label * positive_weight + (1.0f - label) * negative_weight;
    float bce = fmaxf(logit, 0.0f) - label * logit + log1pf(expf(-fabsf(logit)));
    atomicAdd(accum, bce * label_weight * m);
    atomicAdd(accum + 1, label_weight * m);
}

extern "C" __global__ void termite_masked_bce_finalize_f32(
    float* accum,
    float eps,
    unsigned int mean_reduction
) {
    if (blockIdx.x == 0 && threadIdx.x == 0 && mean_reduction) {
        accum[0] /= accum[1] + eps;
    }
}

extern "C" __global__ void termite_masked_bce_backward_f32(
    float* output,
    const float* logits,
    const float* labels,
    const float* mask,
    const float* upstream,
    const float* accum,
    unsigned int count,
    float positive_weight,
    float negative_weight,
    float eps,
    unsigned int mean_reduction
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float m = mask[idx];
    if (m == 0.0f) {
        output[idx] = 0.0f;
        return;
    }
    float label = labels[idx];
    float label_weight = label * positive_weight + (1.0f - label) * negative_weight;
    float sigmoid = 1.0f / (1.0f + expf(-logits[idx]));
    float scale = upstream[0];
    if (mean_reduction) scale /= accum[1] + eps;
    output[idx] = scale * label_weight * m * (sigmoid - label);
}

extern "C" __global__ void termite_primitive_reduce_f32(
    float* output,
    const float* input,
    unsigned int input_count,
    unsigned int output_count,
    unsigned int dim0,
    unsigned int dim1,
    unsigned int dim2,
    unsigned int dim3,
    unsigned int dim4,
    unsigned int dim5,
    unsigned int dim6,
    unsigned int dim7,
    unsigned int rank,
    unsigned int reduce_mask,
    unsigned int mode,
    unsigned int reduce_count
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int dims[8] = {dim0, dim1, dim2, dim3, dim4, dim5, dim6, dim7};
    float acc = mode == 1u ? -3.402823466e+38f : 0.0f;
    for (unsigned int reduced_idx = 0; reduced_idx < reduce_count; ++reduced_idx) {
        unsigned int output_remaining = out_idx;
        unsigned int reduced_remaining = reduced_idx;
        unsigned int input_idx = 0u;
        unsigned int input_stride = 1u;
        for (int d = (int)rank - 1; d >= 0; --d) {
            unsigned int coord;
            if ((reduce_mask & (1u << d)) != 0u) {
                coord = reduced_remaining % dims[d];
                reduced_remaining /= dims[d];
            } else {
                coord = output_remaining % dims[d];
                output_remaining /= dims[d];
            }
            input_idx += coord * input_stride;
            input_stride *= dims[d];
        }
        if (input_idx >= input_count) continue;
        float value = input[input_idx];
        acc = mode == 1u ? fmaxf(acc, value) : acc + value;
    }
    if (mode == 2u && reduce_count != 0u) acc /= (float)reduce_count;
    output[out_idx] = acc;
}

extern "C" __global__ void termite_primitive_broadcast_f32(
    float* output,
    const float* input,
    unsigned int output_count,
    unsigned int in0, unsigned int in1, unsigned int in2, unsigned int in3,
    unsigned int in4, unsigned int in5, unsigned int in6, unsigned int in7,
    unsigned int out0, unsigned int out1, unsigned int out2, unsigned int out3,
    unsigned int out4, unsigned int out5, unsigned int out6, unsigned int out7,
    unsigned int axis0, unsigned int axis1, unsigned int axis2, unsigned int axis3,
    unsigned int axis4, unsigned int axis5, unsigned int axis6, unsigned int axis7,
    unsigned int input_rank,
    unsigned int output_rank
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int input_dims[8] = {in0, in1, in2, in3, in4, in5, in6, in7};
    unsigned int output_dims[8] = {out0, out1, out2, out3, out4, out5, out6, out7};
    unsigned int axes[8] = {axis0, axis1, axis2, axis3, axis4, axis5, axis6, axis7};
    unsigned int input_flat = 0u;
    for (unsigned int in_d = 0; in_d < input_rank; ++in_d) {
        unsigned int axis = axes[in_d];
        unsigned int stride = 1u;
        for (unsigned int d = axis + 1u; d < output_rank; ++d) stride *= output_dims[d];
        unsigned int coord = (out_idx / stride) % output_dims[axis];
        if (input_dims[in_d] == 1u) coord = 0u;
        else coord %= input_dims[in_d];
        input_flat = input_flat * input_dims[in_d] + coord;
    }
    output[out_idx] = input[input_flat];
}

extern "C" __global__ void termite_layer_norm_backward_f32(
    float* output,
    const float* input,
    const float* gamma,
    const float* d_y,
    unsigned int rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int input_count = rows * dim;
    if (row >= rows) return;
    unsigned int base = row * dim;
    float mean = 0.0f;
    for (unsigned int j = 0; j < dim; ++j) mean += input[base + j];
    mean /= (float)dim;
    float variance = 0.0f;
    for (unsigned int j = 0; j < dim; ++j) {
        float centered = input[base + j] - mean;
        variance += centered * centered;
    }
    float inv = rsqrtf(variance / (float)dim + eps);
    float sum_dxhat = 0.0f;
    float sum_dxhat_xhat = 0.0f;
    for (unsigned int d = 0; d < dim; ++d) {
        float dxhat = d_y[base + d] * gamma[d];
        float xhat = (input[base + d] - mean) * inv;
        sum_dxhat += dxhat;
        sum_dxhat_xhat += dxhat * xhat;
    }
    for (unsigned int d = 0; d < dim; ++d) {
        float xhat = (input[base + d] - mean) * inv;
        float dxhat = d_y[base + d] * gamma[d];
        output[base + d] = inv * (dxhat - (sum_dxhat + xhat * sum_dxhat_xhat) / (float)dim);
        atomicAdd(output + input_count + d, d_y[base + d] * xhat);
        atomicAdd(output + input_count + dim + d, d_y[base + d]);
    }
}

extern "C" __global__ void termite_primitive_softmax_f32(
    float* output,
    const float* input,
    unsigned int count,
    unsigned int last_dim,
    unsigned int log_softmax
) {
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

extern "C" __global__ void termite_primitive_gather_f32(
    float* output,
    const float* input,
    const float* indices,
    unsigned int output_count,
    unsigned int index_count,
    unsigned int axis_extent,
    unsigned int suffix_size
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int suffix_coord = out_idx % suffix_size;
    unsigned int outer = out_idx / suffix_size;
    unsigned int index_pos = outer % index_count;
    unsigned int prefix = outer / index_count;
    int gather_index = (int)indices[index_pos];
    if (gather_index < 0) gather_index += (int)axis_extent;
    if (gather_index < 0 || (unsigned int)gather_index >= axis_extent) {
        output[out_idx] = NAN;
        return;
    }
    unsigned int input_idx = (prefix * axis_extent + (unsigned int)gather_index) * suffix_size + suffix_coord;
    output[out_idx] = input[input_idx];
}

extern "C" __global__ void termite_primitive_scatter_add_axis0_f32(
    float* output,
    const float* input,
    const float* indices,
    unsigned int rows,
    unsigned int cols,
    unsigned int output_rows
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * cols;
    if (idx >= count) return;
    unsigned int row = idx / cols;
    unsigned int col = idx - row * cols;
    int output_row = (int)indices[row];
    if (output_row < 0) output_row += (int)output_rows;
    if (output_row < 0 || (unsigned int)output_row >= output_rows) return;
    atomicAdd(output + (unsigned int)output_row * cols + col, input[idx]);
}

extern "C" __global__ void termite_primitive_transpose_f32(
    float* output,
    const float* input,
    unsigned int count,
    unsigned int rank,
    unsigned int d0, unsigned int d1, unsigned int d2, unsigned int d3,
    unsigned int d4, unsigned int d5, unsigned int d6, unsigned int d7,
    unsigned int p0, unsigned int p1, unsigned int p2, unsigned int p3,
    unsigned int p4, unsigned int p5, unsigned int p6, unsigned int p7
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= count) return;
    unsigned int dims[8] = { d0, d1, d2, d3, d4, d5, d6, d7 };
    unsigned int perm[8] = { p0, p1, p2, p3, p4, p5, p6, p7 };
    unsigned int input_strides[8];
    unsigned int stride = 1u;
    for (int d = (int)rank - 1; d >= 0; --d) {
        input_strides[d] = stride;
        stride *= dims[d];
    }
    unsigned int remaining = out_idx;
    unsigned int input_idx = 0u;
    for (int out_d = (int)rank - 1; out_d >= 0; --out_d) {
        unsigned int input_axis = perm[out_d];
        unsigned int coord = remaining % dims[input_axis];
        remaining /= dims[input_axis];
        input_idx += coord * input_strides[input_axis];
    }
    output[out_idx] = input[input_idx];
}

// Coalesced rank-2 transpose. A 256-thread block covers one 32x32 tile in
// four 32x8 strips; the padded shared-memory stride avoids bank conflicts.
extern "C" __global__ void termite_primitive_transpose_2d_f32(
    float* output,
    const float* input,
    unsigned int rows,
    unsigned int cols,
    unsigned int tile_cols
) {
    __shared__ float tile[32][33];
    unsigned int tid = threadIdx.x;
    unsigned int tx = tid & 31u;
    unsigned int ty = tid >> 5u;
    unsigned int tile_x = blockIdx.x % tile_cols;
    unsigned int tile_y = blockIdx.x / tile_cols;
    unsigned int input_x = tile_x * 32u + tx;
    unsigned int input_y = tile_y * 32u + ty;

    #pragma unroll
    for (unsigned int offset = 0u; offset < 32u; offset += 8u) {
        if (input_x < cols && input_y + offset < rows) {
            tile[ty + offset][tx] = input[(input_y + offset) * cols + input_x];
        }
    }
    __syncthreads();

    unsigned int output_x = tile_y * 32u + tx;
    unsigned int output_y = tile_x * 32u + ty;
    #pragma unroll
    for (unsigned int offset = 0u; offset < 32u; offset += 8u) {
        if (output_x < rows && output_y + offset < cols) {
            output[(output_y + offset) * rows + output_x] = tile[tx][ty + offset];
        }
    }
}

extern "C" __global__ void termite_primitive_concat_f32(
    float* output,
    const float* a,
    const float* b,
    unsigned int output_count,
    unsigned int a_axis,
    unsigned int b_axis,
    unsigned int inner
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= output_count) return;
    unsigned int inner_coord = idx % inner;
    unsigned int outer_axis = idx / inner;
    unsigned int output_axis = a_axis + b_axis;
    unsigned int axis_coord = outer_axis % output_axis;
    unsigned int outer = outer_axis / output_axis;
    if (axis_coord < a_axis) {
        output[idx] = a[(outer * a_axis + axis_coord) * inner + inner_coord];
    } else {
        unsigned int b_coord = axis_coord - a_axis;
        output[idx] = b[(outer * b_axis + b_coord) * inner + inner_coord];
    }
}

extern "C" __global__ void termite_primitive_slice_f32(
    float* output,
    const float* input,
    unsigned int output_count,
    unsigned int rank,
    unsigned int i0, unsigned int i1, unsigned int i2, unsigned int i3,
    unsigned int i4, unsigned int i5, unsigned int i6, unsigned int i7,
    unsigned int o0, unsigned int o1, unsigned int o2, unsigned int o3,
    unsigned int o4, unsigned int o5, unsigned int o6, unsigned int o7,
    unsigned int s0, unsigned int s1, unsigned int s2, unsigned int s3,
    unsigned int s4, unsigned int s5, unsigned int s6, unsigned int s7,
    unsigned int t0, unsigned int t1, unsigned int t2, unsigned int t3,
    unsigned int t4, unsigned int t5, unsigned int t6, unsigned int t7
) {
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int input_dims[8] = { i0, i1, i2, i3, i4, i5, i6, i7 };
    unsigned int output_dims[8] = { o0, o1, o2, o3, o4, o5, o6, o7 };
    unsigned int starts[8] = { s0, s1, s2, s3, s4, s5, s6, s7 };
    unsigned int strides[8] = { t0, t1, t2, t3, t4, t5, t6, t7 };
    unsigned int coords[8] = { 0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u };
    unsigned int remaining = out_idx;
    for (int d = (int)rank - 1; d >= 0; --d) {
        coords[d] = remaining % output_dims[d];
        remaining /= output_dims[d];
    }
    unsigned int input_idx = 0u;
    for (unsigned int d = 0; d < rank; ++d) {
        unsigned int input_coord = starts[d] + coords[d] * strides[d];
        input_idx = input_idx * input_dims[d] + input_coord;
    }
    output[out_idx] = input[input_idx];
}

__device__ __forceinline__ float termite_deberta_training_score(
    const float* q,
    const float* k,
    const float* q_r,
    const float* k_r,
    unsigned int b,
    unsigned int qi,
    unsigned int ki,
    unsigned int head,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    unsigned int hidden = num_heads * head_dim;
    unsigned int head_off = head * head_dim;
    unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
    unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
    unsigned int rel_base = (qi + seq_len - 1u - ki) * hidden + head_off;
    float value = 0.0f;
    for (unsigned int d = 0; d < head_dim; ++d) {
        float qd = q[q_base + d];
        float kd = k[k_base + d];
        value += qd * kd + qd * k_r[rel_base + d] + q_r[rel_base + d] * kd;
    }
    return value * rsqrtf((float)head_dim * 3.0f);
}

__device__ __forceinline__ float termite_deberta_training_dp(
    const float* v,
    const float* d_out,
    unsigned int b,
    unsigned int qi,
    unsigned int ki,
    unsigned int head,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    unsigned int hidden = num_heads * head_dim;
    unsigned int head_off = head * head_dim;
    unsigned int out_base = (b * seq_len + qi) * hidden + head_off;
    unsigned int v_base = (b * seq_len + ki) * hidden + head_off;
    float value = 0.0f;
    for (unsigned int d = 0; d < head_dim; ++d) value += d_out[out_base + d] * v[v_base + d];
    return value;
}

__device__ __forceinline__ void termite_deberta_training_row_stats(
    const float* q,
    const float* k,
    const float* v,
    const float* q_r,
    const float* k_r,
    const long long* mask,
    const float* attn_bias,
    unsigned int use_bias,
    const float* d_out,
    unsigned int b,
    unsigned int qi,
    unsigned int head,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim,
    float* row_max,
    float* denom,
    float* dot_p_dp
) {
    float max_value = -3.402823466e+38f;
    bool any = false;
    for (unsigned int ki = 0; ki < seq_len; ++ki) {
        if (mask[b * seq_len + ki] == 0ll) continue;
        any = true;
        max_value = fmaxf(max_value, termite_deberta_training_score(q, k, q_r, k_r, b, qi, ki, head, seq_len, num_heads, head_dim));
    }
    float sum = 0.0f;
    float weighted_dp = 0.0f;
    if (any) {
        for (unsigned int ki = 0; ki < seq_len; ++ki) {
            if (mask[b * seq_len + ki] == 0ll) continue;
            float score = termite_deberta_training_score(q, k, q_r, k_r, b, qi, ki, head, seq_len, num_heads, head_dim);
            float e = expf(score - max_value);
            sum += e;
            weighted_dp += e * termite_deberta_training_dp(v, d_out, b, qi, ki, head, seq_len, num_heads, head_dim);
        }
    }
    *row_max = max_value;
    *denom = sum;
    *dot_p_dp = sum > 0.0f ? weighted_dp / sum : 0.0f;
}

__device__ __forceinline__ void termite_deberta_training_pair(
    const float* q,
    const float* k,
    const float* v,
    const float* q_r,
    const float* k_r,
    const float* d_out,
    unsigned int b,
    unsigned int qi,
    unsigned int ki,
    unsigned int head,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim,
    float row_max,
    float denom,
    float dot_p_dp,
    float* probability,
    float* dscore
) {
    if (denom <= 0.0f) {
        *probability = 0.0f;
        *dscore = 0.0f;
        return;
    }
    float score = termite_deberta_training_score(q, k, q_r, k_r, b, qi, ki, head, seq_len, num_heads, head_dim);
    float p = expf(score - row_max) / denom;
    float dp = termite_deberta_training_dp(v, d_out, b, qi, ki, head, seq_len, num_heads, head_dim);
    *probability = p;
    *dscore = p * (dp - dot_p_dp);
}

// Compute softmax probabilities and d(score) once per attention pair. One
// block owns a complete (batch, head, query) row, so reductions stay local and
// deterministic while every packed-gradient output can reuse the result.
extern "C" __global__ void termite_deberta_attention_backward_scores_f32(
    float* probabilities,
    float* d_scores,
    const float* q,
    const float* k,
    const float* v,
    const float* q_r,
    const float* k_r,
    const long long* mask,
    const float* attn_bias,
    unsigned int use_bias,
    const float* d_out,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    unsigned int row = blockIdx.x;
    unsigned int qi = row % seq_len;
    unsigned int tmp = row / seq_len;
    unsigned int head = tmp % num_heads;
    unsigned int b = tmp / num_heads;
    if (b >= batch) return;

    extern __shared__ float scratch[];
    float* scores = scratch;
    float* d_probability = scores + seq_len;
    float* reduction = d_probability + seq_len;
    unsigned int tid = threadIdx.x;

    float local_max = -3.402823466e+38f;
    for (unsigned int ki = tid; ki < seq_len; ki += blockDim.x) {
        if (termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) {
            float score = termite_deberta_training_score(q, k, q_r, k_r, b, qi, ki, head, seq_len, num_heads, head_dim);
            scores[ki] = score;
            d_probability[ki] = termite_deberta_training_dp(v, d_out, b, qi, ki, head, seq_len, num_heads, head_dim);
            local_max = fmaxf(local_max, score);
        } else {
            scores[ki] = -3.402823466e+38f;
            d_probability[ki] = 0.0f;
        }
    }
    reduction[tid] = local_max;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) reduction[tid] = fmaxf(reduction[tid], reduction[tid + stride]);
        __syncthreads();
    }
    float row_max = reduction[0];
    __syncthreads();

    float local_sum = 0.0f;
    for (unsigned int ki = tid; ki < seq_len; ki += blockDim.x) {
        if (termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) {
            float e = expf(scores[ki] - row_max);
            scores[ki] = e;
            local_sum += e;
        }
    }
    reduction[tid] = local_sum;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) reduction[tid] += reduction[tid + stride];
        __syncthreads();
    }
    float denominator = reduction[0];
    __syncthreads();

    float local_weighted_dp = 0.0f;
    for (unsigned int ki = tid; ki < seq_len; ki += blockDim.x) {
        if (termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) local_weighted_dp += scores[ki] * d_probability[ki];
    }
    reduction[tid] = local_weighted_dp;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (tid < stride) reduction[tid] += reduction[tid + stride];
        __syncthreads();
    }
    float dot_p_dp = denominator > 0.0f ? reduction[0] / denominator : 0.0f;
    unsigned int pair_base = row * seq_len;
    for (unsigned int ki = tid; ki < seq_len; ki += blockDim.x) {
        float probability = (denominator > 0.0f && termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) ? scores[ki] / denominator : 0.0f;
        probabilities[pair_base + ki] = probability;
        d_scores[pair_base + ki] = probability * (d_probability[ki] - dot_p_dp);
    }
}

// Deterministic packed VJP. Each output element has a unique writer; the
// expensive score/softmax work above is shared by every gradient segment.
extern "C" __global__ void termite_deberta_attention_backward_f32(
    float* dst,
    const float* probabilities,
    const float* d_scores,
    const float* q,
    const float* k,
    const float* q_r,
    const float* k_r,
    const long long* mask,
    const float* attn_bias,
    unsigned int use_bias,
    const float* d_out,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    unsigned int hidden = num_heads * head_dim;
    unsigned int token_elems = batch * seq_len * hidden;
    unsigned int rel_rows = seq_len * 2u - 1u;
    unsigned int rel_elems = rel_rows * hidden;
    unsigned int packed_elems = token_elems * 3u + rel_elems * 2u;
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= packed_elems) return;
    float scale = rsqrtf((float)head_dim * 3.0f);
    float result = 0.0f;

    if (idx < token_elems * 3u) {
        unsigned int segment = idx / token_elems;
        unsigned int local = idx - segment * token_elems;
        unsigned int d = local % head_dim;
        unsigned int tmp = local / head_dim;
        unsigned int head = tmp % num_heads;
        tmp /= num_heads;
        unsigned int pos = tmp % seq_len;
        unsigned int b = tmp / seq_len;
        unsigned int head_off = head * head_dim;

        if (segment == 0u) {
            unsigned int pair_base = ((b * num_heads + head) * seq_len + pos) * seq_len;
            for (unsigned int ki = 0; ki < seq_len; ++ki) {
                if (!termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, pos, ki, seq_len, num_heads)) continue;
                float ds = d_scores[pair_base + ki];
                unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
                unsigned int rel_base = (pos + seq_len - 1u - ki) * hidden + head_off;
                result += scale * ds * (k[k_base + d] + k_r[rel_base + d]);
            }
        } else {
            unsigned int ki = pos;
            for (unsigned int qi = 0; qi < seq_len; ++qi) {
                if (!termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) continue;
                unsigned int pair_idx = ((b * num_heads + head) * seq_len + qi) * seq_len + ki;
                float p = probabilities[pair_idx];
                float ds = d_scores[pair_idx];
                if (segment == 1u) {
                    unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
                    unsigned int rel_base = (qi + seq_len - 1u - ki) * hidden + head_off;
                    result += scale * ds * (q[q_base + d] + q_r[rel_base + d]);
                } else {
                    unsigned int out_base = (b * seq_len + qi) * hidden + head_off;
                    result += p * d_out[out_base + d];
                }
            }
        }
    } else {
        unsigned int rel_segment_base = token_elems * 3u;
        unsigned int rel_local = idx - rel_segment_base;
        unsigned int segment = rel_local / rel_elems;
        rel_local -= segment * rel_elems;
        unsigned int d = rel_local % head_dim;
        unsigned int tmp = rel_local / head_dim;
        unsigned int head = tmp % num_heads;
        unsigned int rel = tmp / num_heads;
        unsigned int head_off = head * head_dim;
        for (unsigned int b = 0; b < batch; ++b) {
            for (unsigned int qi = 0; qi < seq_len; ++qi) {
                int ki_signed = (int)qi + (int)seq_len - 1 - (int)rel;
                if (ki_signed < 0 || ki_signed >= (int)seq_len) continue;
                unsigned int ki = (unsigned int)ki_signed;
                if (!termite_deberta_attention_key_valid(mask, attn_bias, use_bias, b, head, qi, ki, seq_len, num_heads)) continue;
                unsigned int pair_idx = ((b * num_heads + head) * seq_len + qi) * seq_len + ki;
                float ds = d_scores[pair_idx];
                unsigned int q_base = (b * seq_len + qi) * hidden + head_off;
                unsigned int k_base = (b * seq_len + ki) * hidden + head_off;
                result += scale * ds * (segment == 0u ? k[k_base + d] : q[q_base + d]);
            }
        }
    }
    dst[idx] = result;
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
